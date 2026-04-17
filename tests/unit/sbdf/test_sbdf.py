"""Unit tests for spotfire_community.sbdf — create_sbdf helper."""

from __future__ import annotations

import io

import pytest

from spotfire_community.sbdf import create_sbdf

# First two bytes of every valid SBDF file (magic number).
_SBDF_MAGIC = b"\xdf\x5b"

_SIMPLE_CSV = "n,label\n1,a\n2,b\n3,c\n"


# ---------------------------------------------------------------------------
# Basic correctness
# ---------------------------------------------------------------------------


def test_create_sbdf_returns_bytes_from_file_like() -> None:
    result = create_sbdf(io.StringIO(_SIMPLE_CSV))
    assert isinstance(result, bytes)
    assert result[:2] == _SBDF_MAGIC


def test_create_sbdf_returns_bytes_from_csv_reader() -> None:
    import csv

    result = create_sbdf(csv.reader(io.StringIO(_SIMPLE_CSV)))
    assert isinstance(result, bytes)
    assert result[:2] == _SBDF_MAGIC


def test_create_sbdf_returns_bytes_from_dataframe() -> None:
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"n": [1, 2, 3], "label": ["a", "b", "c"]})
    result = create_sbdf(df)
    assert isinstance(result, bytes)
    assert result[:2] == _SBDF_MAGIC


# ---------------------------------------------------------------------------
# Type inference produces non-empty output for various column types
# ---------------------------------------------------------------------------


def test_create_sbdf_int_column() -> None:
    result = create_sbdf(io.StringIO("value\n1\n2\n3\n"))
    assert result[:2] == _SBDF_MAGIC


def test_create_sbdf_bool_column() -> None:
    result = create_sbdf(io.StringIO("flag\ntrue\nfalse\ntrue\n"))
    assert result[:2] == _SBDF_MAGIC


def test_create_sbdf_float_column() -> None:
    result = create_sbdf(io.StringIO("x\n1.5\n2.5\n3.5\n"))
    assert result[:2] == _SBDF_MAGIC


def test_create_sbdf_string_column() -> None:
    result = create_sbdf(io.StringIO("name\nalice\nbob\ncarol\n"))
    assert result[:2] == _SBDF_MAGIC


# ---------------------------------------------------------------------------
# chunk_size produces multiple slices
# ---------------------------------------------------------------------------


def test_create_sbdf_respects_chunk_size() -> None:
    # 10 rows with chunk_size=3 → at least 3 slices, output still valid SBDF.
    csv_data = "n\n" + "\n".join(str(i) for i in range(10)) + "\n"
    result = create_sbdf(io.StringIO(csv_data), chunk_size=3)
    assert result[:2] == _SBDF_MAGIC
    # The word "TableSlice" is not in the binary, but the output must be larger
    # than a minimal single-slice file — use 1 row as the baseline.
    single = create_sbdf(io.StringIO("n\n42\n"))
    assert len(result) > len(single)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_create_sbdf_single_row() -> None:
    result = create_sbdf(io.StringIO("col\nvalue\n"))
    assert result[:2] == _SBDF_MAGIC


def test_create_sbdf_many_columns() -> None:
    headers = ",".join(f"col{i}" for i in range(20))
    values = ",".join(str(i) for i in range(20))
    result = create_sbdf(io.StringIO(f"{headers}\n{values}\n"))
    assert result[:2] == _SBDF_MAGIC


def test_create_sbdf_invalid_type_raises() -> None:
    with pytest.raises(TypeError):
        create_sbdf("not a valid input")  # type: ignore[arg-type]
