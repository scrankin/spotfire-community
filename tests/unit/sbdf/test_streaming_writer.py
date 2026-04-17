"""Unit tests for SbdfStreamingWriter and infer_types helpers."""

from __future__ import annotations

import io

import pytest

from spotfire_community.sbdf import (
    SbdfStreamingWriter,
    ValueType,
    create_sbdf,
    infer_types,
)

_SBDF_MAGIC = b"\xdf\x5b"


def test_value_type_members_distinct() -> None:
    # Each supported SBDF type maps to a distinct underlying int.
    codes = {
        ValueType.BOOL,
        ValueType.INT,
        ValueType.LONG,
        ValueType.DOUBLE,
        ValueType.DATETIME,
        ValueType.DATE,
        ValueType.STRING,
    }
    assert len(codes) == 7


def test_infer_types_matches_csv_path() -> None:
    sample = [["1", "alice", "1.5"], ["2", "bob", "2.5"]]
    types = infer_types(sample, num_cols=3)
    assert types == [ValueType.INT, ValueType.STRING, ValueType.DOUBLE]


def test_infer_types_pads_short_rows() -> None:
    sample = [["1"], ["2", "x"]]  # missing column values should be treated as empty
    types = infer_types(sample, num_cols=3)
    assert types[0] == ValueType.INT
    # col 1: one empty + one "x" → string; col 2: all empty → string
    assert types[1] == ValueType.STRING
    assert types[2] == ValueType.STRING


def test_writer_happy_path_yields_expected_sections() -> None:
    writer = SbdfStreamingWriter(
        headers=["id", "name", "value"],
        column_types=[ValueType.LONG, ValueType.STRING, ValueType.DOUBLE],
    )
    batches = [
        [["1", "Alice", "10.5"], ["2", "Bob", "20.3"]],
        [["3", "Carol", "30.7"]],
    ]
    chunks = list(writer.chunks(iter(batches)))

    # header+metadata, slice #1, slice #2, end marker → 4 chunks.
    assert len(chunks) == 4
    combined = b"".join(chunks)
    assert combined.startswith(_SBDF_MAGIC + bytes([0x01]))  # FileHeader
    assert combined.endswith(_SBDF_MAGIC + bytes([0x05]))  # TableEnd


def test_writer_matches_create_sbdf_byte_for_byte() -> None:
    # Streaming write with explicit column types should produce the same bytes
    # as create_sbdf when given the equivalent CSV and chunk boundaries.
    csv_text = "id,name,value\r\n1,Alice,10.5\r\n2,Bob,20.3\r\n3,Carol,30.7\r\n"
    buffered = create_sbdf(io.BytesIO(csv_text.encode()), chunk_size=2)

    writer = SbdfStreamingWriter(
        headers=["id", "name", "value"],
        column_types=[ValueType.INT, ValueType.STRING, ValueType.DOUBLE],
    )
    streamed = b"".join(
        writer.chunks(
            iter(
                [
                    [["1", "Alice", "10.5"], ["2", "Bob", "20.3"]],
                    [["3", "Carol", "30.7"]],
                ]
            )
        )
    )

    assert streamed == buffered


def test_writer_low_level_methods() -> None:
    writer = SbdfStreamingWriter(headers=["n"], column_types=[ValueType.INT])
    head = writer.start()
    slice_bytes = writer.write_slice([["1"], ["2"]])
    tail = writer.finish()

    assert head.startswith(_SBDF_MAGIC + bytes([0x01]))
    assert slice_bytes.startswith(_SBDF_MAGIC + bytes([0x03]))  # TableSlice
    assert tail == _SBDF_MAGIC + bytes([0x05])


def test_writer_empty_slice_is_skipped() -> None:
    writer = SbdfStreamingWriter(headers=["n"], column_types=[ValueType.INT])
    writer.start()
    assert writer.write_slice([]) == b""


def test_writer_chunks_skips_empty_batches() -> None:
    writer = SbdfStreamingWriter(headers=["n"], column_types=[ValueType.INT])
    chunks = list(writer.chunks(iter([[["1"]], [], [["2"]]])))
    # header, slice-1, slice-2, end = 4 chunks (middle empty batch dropped).
    assert len(chunks) == 4


def test_writer_mismatched_header_and_type_count_raises() -> None:
    with pytest.raises(ValueError, match="same length"):
        SbdfStreamingWriter(
            headers=["a", "b"],
            column_types=[ValueType.INT],
        )


def test_writer_enforces_call_order() -> None:
    writer = SbdfStreamingWriter(headers=["n"], column_types=[ValueType.INT])

    # write_slice before start
    with pytest.raises(RuntimeError, match="before start"):
        writer.write_slice([["1"]])

    # finish before start
    with pytest.raises(RuntimeError, match="before start"):
        writer.finish()

    writer.start()
    with pytest.raises(RuntimeError, match="more than once"):
        writer.start()

    writer.finish()
    with pytest.raises(RuntimeError, match="after finish"):
        writer.write_slice([["1"]])
    with pytest.raises(RuntimeError, match="more than once"):
        writer.finish()


def test_infer_types_detects_date() -> None:
    sample = [["2025-01-01"], ["2025-06-15"], ["2025-12-31"]]
    assert infer_types(sample, num_cols=1) == [ValueType.DATE]


def test_infer_types_detects_datetime() -> None:
    sample = [["2025-01-01T12:34:56"], ["2025-06-15 09:00:00"]]
    assert infer_types(sample, num_cols=1) == [ValueType.DATETIME]


def test_infer_types_falls_back_to_string_when_date_parse_fails() -> None:
    # Some values parse as dates, one does not — whole column becomes string.
    sample = [["2025-01-01"], ["not-a-date"], ["2025-06-15"]]
    assert infer_types(sample, num_cols=1) == [ValueType.STRING]


def test_writer_date_column_roundtrip_math() -> None:
    # Sanity-check the Spotfire epoch math: 0001-01-01 UTC → 0 ms.
    from datetime import datetime, timezone

    from spotfire_community.sbdf._writer import _parse_date_ms

    assert _parse_date_ms("0001-01-01") == 0
    # 2025-01-01 is a known offset; verify against direct datetime math.
    expected = int(
        (
            datetime(2025, 1, 1, tzinfo=timezone.utc)
            - datetime(1, 1, 1, tzinfo=timezone.utc)
        ).total_seconds()
        * 1000
    )
    assert _parse_date_ms("2025-01-01") == expected


def test_writer_date_column_emits_int64_per_row() -> None:
    writer = SbdfStreamingWriter(headers=["d"], column_types=[ValueType.DATE])
    writer.start()
    slice_bytes = writer.write_slice([["2025-01-01"], ["2025-06-15"], ["2025-12-31"]])
    # 3 rows × 8 bytes each for int64 payload.
    # Inspect the column slice: magic+sid(3) + encoding+vtype(2) + i32 count(4)
    # + 24 bytes of date values + i32 props(4) minimum.
    assert len(slice_bytes) >= 3 + 4 + 2 + 4 + 24 + 4


def test_writer_invalid_date_marks_row_invalid() -> None:
    writer = SbdfStreamingWriter(headers=["d"], column_types=[ValueType.DATE])
    writer.start()
    slice_bytes = writer.write_slice([["2025-01-01"], ["not-a-date"], [""]])
    # IsInvalid property must be present — the 2nd and 3rd rows can't parse.
    # Column props count (int32) is non-zero when any row is invalid.
    # The int32 right before the end of the slice is the props count;
    # verify at least one invalid bit is set in the IsInvalid bit array.
    assert b"IsInvalid" in slice_bytes


def test_writer_pads_short_rows_in_slice() -> None:
    writer = SbdfStreamingWriter(
        headers=["a", "b", "c"],
        column_types=[ValueType.STRING, ValueType.STRING, ValueType.STRING],
    )
    writer.start()
    # Short rows should be padded with empty strings — no exception.
    slice_bytes = writer.write_slice([["x"], ["y", "z"]])
    assert len(slice_bytes) > 0
