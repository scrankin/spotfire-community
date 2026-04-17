"""Helpers for producing Spotfire Binary Data Format (SBDF) files.

Two public entry points:

* :func:`create_sbdf` — a one-shot convenience that takes CSV/iterable input
  and returns the full SBDF file as :class:`bytes`. Suitable for small tables.

* :class:`SbdfStreamingWriter` — a low-level streaming writer that emits SBDF
  bytes incrementally (file header, table slices, table end) so callers can
  pipe the output to an upload target without materialising the full file in
  memory. Suitable for tables that exceed available RAM.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Iterator, Sequence
from enum import IntEnum

from spotfire_community.sbdf._writer import (
    _SID_FILE_HEADER,
    _SID_TABLE_END,
    _SID_TABLE_METADATA,
    _SID_TABLE_SLICE,
    _VT_BOOL,
    _VT_DOUBLE,
    _VT_INT,
    _VT_LONG,
    _VT_STRING,
    _column_slice,
    _i32,
    _infer_type,
    _section,
    _table_metadata,
    csv_to_sbdf as _csv_to_sbdf,
)


class ValueType(IntEnum):
    """SBDF column value types supported by this package."""

    BOOL = _VT_BOOL
    INT = _VT_INT
    LONG = _VT_LONG
    DOUBLE = _VT_DOUBLE
    STRING = _VT_STRING


def infer_types(
    sample_rows: Iterable[Sequence[str]],
    num_cols: int,
) -> list[ValueType]:
    """Infer a ``ValueType`` per column from a sample of stringified rows.

    Args:
        sample_rows: Iterable of rows (each row a sequence of strings). Rows
            shorter than ``num_cols`` are padded with empty strings.
        num_cols: Number of columns in the table.

    Returns:
        A list of ``num_cols`` :class:`ValueType` entries.
    """
    samples = [
        [row[i] if i < len(row) else "" for i in range(num_cols)]
        for row in sample_rows
    ]
    return [
        ValueType(_infer_type([r[c] for r in samples])) for c in range(num_cols)
    ]


class SbdfStreamingWriter:
    """Incremental SBDF writer.

    Emits the SBDF sections (file header, table metadata, table slices, and
    final table-end marker) as separate :class:`bytes` chunks so callers can
    stream the output to an upload target without buffering the full file.

    Example — streaming upload of a Snowflake cursor to a Spotfire library::

        writer = SbdfStreamingWriter(
            headers=["id", "name", "value"],
            column_types=[ValueType.LONG, ValueType.STRING, ValueType.DOUBLE],
        )

        def row_batches() -> Iterator[list[list[str]]]:
            while True:
                rows = cursor.fetchmany(10_000)
                if not rows:
                    break
                yield [[str(v) for v in row] for row in rows]

        library_client.upload_file_streaming(
            data_stream=writer.chunks(row_batches()),
            path="/WiseRock/Customer/data.sbdf",
            item_type=ItemType.SBDF,
        )

    Args:
        headers: Column names, in order.
        column_types: :class:`ValueType` per column. Must be the same length
            as ``headers``. Use :func:`infer_types` to derive these from a
            sample when the types are not known up front.

    Raises:
        ValueError: If ``headers`` and ``column_types`` have different lengths.
    """

    def __init__(
        self,
        headers: Sequence[str],
        column_types: Sequence[ValueType],
    ) -> None:
        if len(headers) != len(column_types):
            raise ValueError(
                f"headers ({len(headers)}) and column_types ({len(column_types)}) "
                "must have the same length"
            )
        self._headers = list(headers)
        self._vtypes = [int(t) for t in column_types]
        self._num_cols = len(headers)
        self._started = False
        self._finished = False

    @property
    def headers(self) -> list[str]:
        return list(self._headers)

    @property
    def column_types(self) -> list[ValueType]:
        return [ValueType(t) for t in self._vtypes]

    def start(self) -> bytes:
        """Emit the file header and table metadata.

        Must be called exactly once, before any :meth:`write_slice` call.

        Returns:
            The bytes of the ``FileHeader`` + ``TableMetadata`` sections.

        Raises:
            RuntimeError: If called more than once.
        """
        if self._started:
            raise RuntimeError("SbdfStreamingWriter.start() called more than once")
        self._started = True
        return (
            _section(_SID_FILE_HEADER)
            + bytes([1, 0])
            + _section(_SID_TABLE_METADATA)
            + _table_metadata(self._headers, self._vtypes)
        )

    def write_slice(self, rows: Sequence[Sequence[str]]) -> bytes:
        """Encode a batch of rows as one SBDF ``TableSlice`` section.

        Args:
            rows: A sequence of stringified rows. Each row must have at least
                as many elements as there are columns; extra elements are
                ignored, missing elements are treated as empty strings.

        Returns:
            The bytes for a single ``TableSlice`` section. May be empty if
            ``rows`` is empty, in which case no section is written.

        Raises:
            RuntimeError: If called before :meth:`start` or after :meth:`finish`.
        """
        if not self._started:
            raise RuntimeError(
                "SbdfStreamingWriter.write_slice() called before start()"
            )
        if self._finished:
            raise RuntimeError(
                "SbdfStreamingWriter.write_slice() called after finish()"
            )
        if not rows:
            return b""
        padded = [
            [row[i] if i < len(row) else "" for i in range(self._num_cols)]
            for row in rows
        ]
        out = bytearray()
        out += _section(_SID_TABLE_SLICE)
        out += _i32(self._num_cols)
        for c in range(self._num_cols):
            out += _column_slice([r[c] for r in padded], self._vtypes[c])
        return bytes(out)

    def finish(self) -> bytes:
        """Emit the ``TableEnd`` marker. Must be called exactly once, last.

        Returns:
            The bytes of the ``TableEnd`` section.

        Raises:
            RuntimeError: If called before :meth:`start` or more than once.
        """
        if not self._started:
            raise RuntimeError(
                "SbdfStreamingWriter.finish() called before start()"
            )
        if self._finished:
            raise RuntimeError(
                "SbdfStreamingWriter.finish() called more than once"
            )
        self._finished = True
        return _section(_SID_TABLE_END)

    def chunks(
        self,
        row_batches: Iterable[Sequence[Sequence[str]]],
    ) -> Iterator[bytes]:
        """Yield SBDF bytes for an entire file, one section at a time.

        Convenience wrapper that calls :meth:`start`, then :meth:`write_slice`
        per batch, then :meth:`finish`. Each yielded chunk is a complete SBDF
        section — safe to forward directly to a chunked upload API.

        Args:
            row_batches: Iterable of row batches. Each batch becomes one
                ``TableSlice``. Empty batches are skipped silently.

        Yields:
            Non-empty :class:`bytes` chunks of SBDF data.
        """
        yield self.start()
        for batch in row_batches:
            slice_bytes = self.write_slice(batch)
            if slice_bytes:
                yield slice_bytes
        yield self.finish()


def create_sbdf(
    data: io.IOBase | Iterable[Sequence[str]],
    chunk_size: int = 10_000,
) -> bytes:
    """Convert tabular data to SBDF bytes (one-shot, in-memory).

    For large tables use :class:`SbdfStreamingWriter` instead.

    Args:
        data: Either a text-mode file-like object (e.g. ``open("f.csv")``,
            ``io.StringIO``) or a ``csv.reader`` / iterable of rows where each
            row is a sequence of strings.  The first row must be a header row.
        chunk_size: Number of rows per SBDF table slice.  Larger values use
            more memory but produce fewer slices.

    Returns:
        The complete SBDF file as a :class:`bytes` object.

    Raises:
        TypeError: If *data* is not a supported type.
        ValueError: If the CSV cannot be parsed or the SBDF cannot be written.
    """
    if isinstance(data, (str, bytes)):
        raise TypeError(
            f"create_sbdf() does not accept {type(data).__name__!r}; "
            "pass a file-like object or csv.reader."
        )

    # Text-mode file-like (io.StringIO, open(...) in text mode, etc.).
    if isinstance(data, io.TextIOBase):
        return _csv_to_sbdf(data.read().encode(), chunk_size)

    # Binary buffered file-like (io.BytesIO, open(..., "rb"), etc.).
    if isinstance(data, io.BufferedIOBase):
        return _csv_to_sbdf(data.read(), chunk_size)

    # Raw binary file-like.
    if isinstance(data, io.RawIOBase):
        raw = data.read()
        return _csv_to_sbdf(raw if raw is not None else b"", chunk_size)

    # Fallback: iterable of rows (e.g. csv.reader).
    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in data:
        writer.writerow(row)
    return _csv_to_sbdf(buf.getvalue().encode(), chunk_size)


__all__ = [
    "SbdfStreamingWriter",
    "ValueType",
    "create_sbdf",
    "infer_types",
]
