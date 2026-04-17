"""Helpers for converting tabular data to Spotfire Binary Data Format (SBDF).

This module exposes :func:`create_sbdf`, which accepts a CSV file-like object,
a ``csv.reader``, or any DataFrame-like object (e.g. ``pandas.DataFrame``) and
returns the raw SBDF bytes — ready to stream directly to the Spotfire Library API.

The conversion is performed by a compiled Rust extension (``spotfire_community._sbdf``)
built with PyO3 and maturin, so no external binary is required.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable, Sequence
from typing import Protocol, runtime_checkable

from spotfire_community._sbdf import csv_to_sbdf as _csv_to_sbdf


@runtime_checkable
class _HasToCsv(Protocol):
    """Structural protocol for DataFrame-like objects with a to_csv() method."""

    def to_csv(self, *, index: bool) -> str: ...


def create_sbdf(
    data: io.IOBase | _HasToCsv | Iterable[Sequence[str]],
    chunk_size: int = 10_000,
) -> bytes:
    """Convert tabular data to Spotfire Binary Data Format (SBDF) bytes.

    Args:
        data: One of:

            * A text-mode file-like object (e.g. ``open("f.csv")``,
              ``io.StringIO``).  The first row must be a header row.
            * A ``csv.reader`` or any iterable of rows where each row is a
              sequence of strings. The first row must be a header row.
            * A ``pandas.DataFrame`` or any object that exposes a
              ``to_csv(index=False) -> str`` method.

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
            "pass a file-like object, csv.reader, or a DataFrame."
        )

    # DataFrame-like: any object with to_csv(index=False) -> str.
    if isinstance(data, _HasToCsv):
        return _csv_to_sbdf(data.to_csv(index=False).encode(), chunk_size)

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


__all__ = ["create_sbdf"]
