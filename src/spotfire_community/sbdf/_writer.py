"""Pure-Python SBDF binary writer (CSV → SBDF)."""

from __future__ import annotations

import csv
import io
import struct
from collections import deque

# Section IDs
_SID_FILE_HEADER = 0x01
_SID_TABLE_METADATA = 0x02
_SID_TABLE_SLICE = 0x03
_SID_COLUMN_SLICE = 0x04
_SID_TABLE_END = 0x05

# Value types
_VT_BOOL = 0x01
_VT_INT = 0x02
_VT_LONG = 0x03
_VT_DOUBLE = 0x05
_VT_STRING = 0x0A
_VT_BINARY = 0x0C

_ENC_PLAIN = 0x01
_ENC_BIT_ARRAY = 0x03

_MAGIC = b"\xdf\x5b"


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------


def _i32(v: int) -> bytes:
    return struct.pack("<i", v)


def _pack7(v: int) -> bytes:
    """7-bit packed (LEB128-style) non-negative integer."""
    out = bytearray()
    for _ in range(5):
        b = v & 0x7F
        v >>= 7
        if v == 0:
            out.append(b)
            break
        out.append(b | 0x80)
    return bytes(out)


def _str_u(s: str) -> bytes:
    """String unpacked: i32(byte_len) + utf-8."""
    enc = s.encode()
    return _i32(len(enc)) + enc


def _str_p(s: str) -> bytes:
    """String packed: pack7(byte_len) + utf-8."""
    enc = s.encode()
    return _pack7(len(enc)) + enc


def _bytes_u(b: bytes) -> bytes:
    """Bytes unpacked: i32(len) + data."""
    return _i32(len(b)) + b


def _section(sid: int) -> bytes:
    return _MAGIC + bytes([sid])


# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------


def _parse_bool(s: str) -> bool | None:
    t = s.strip().lower()
    if t == "true":
        return True
    if t == "false":
        return False
    return None


def _infer_type(sample: list[str]) -> int:
    nz = [v for v in sample if v]
    if not nz:
        return _VT_STRING
    if all(_parse_bool(v) is not None for v in nz):
        return _VT_BOOL
    try:
        parsed = [int(v) for v in nz]
        if all(-(2**31) <= n <= 2**31 - 1 for n in parsed):
            return _VT_INT
        if all(-(2**63) <= n <= 2**63 - 1 for n in parsed):
            return _VT_LONG
    except ValueError:
        pass
    try:
        for v in nz:
            float(v)
        return _VT_DOUBLE
    except ValueError:
        pass
    return _VT_STRING


# ---------------------------------------------------------------------------
# Column slice encoding
# ---------------------------------------------------------------------------


def _bit_array_bytes(flags: list[bool]) -> bytes:
    buf = bytearray((len(flags) + 7) // 8)
    for i, f in enumerate(flags):
        if f:
            buf[i >> 3] |= 0x80 >> (i & 7)
    return bytes(buf)


def _column_slice(values: list[str], vtype: int) -> bytes:
    """Encode one column as a ColumnSlice (section marker + values + props)."""
    invalid: list[bool] = []

    if vtype == _VT_BOOL:
        arr = bytearray()
        for s in values:
            b = _parse_bool(s)
            if b is None:
                invalid.append(True)
                arr.append(0)
            else:
                invalid.append(False)
                arr.append(1 if b else 0)
        payload = _i32(len(values)) + bytes(arr)
        value_bytes = bytes([_ENC_PLAIN, _VT_BOOL]) + payload

    elif vtype == _VT_INT:
        arr = bytearray()
        for s in values:
            try:
                arr += struct.pack("<i", int(s))
                invalid.append(False)
            except (ValueError, struct.error):
                arr += struct.pack("<i", 0)
                invalid.append(True)
        value_bytes = bytes([_ENC_PLAIN, _VT_INT]) + _i32(len(values)) + bytes(arr)

    elif vtype == _VT_LONG:
        arr = bytearray()
        for s in values:
            try:
                arr += struct.pack("<q", int(s))
                invalid.append(False)
            except (ValueError, struct.error):
                arr += struct.pack("<q", 0)
                invalid.append(True)
        value_bytes = bytes([_ENC_PLAIN, _VT_LONG]) + _i32(len(values)) + bytes(arr)

    elif vtype == _VT_DOUBLE:
        arr = bytearray()
        for s in values:
            try:
                arr += struct.pack("<d", float(s))
                invalid.append(False)
            except (ValueError, struct.error):
                arr += struct.pack("<d", 0.0)
                invalid.append(True)
        value_bytes = bytes([_ENC_PLAIN, _VT_DOUBLE]) + _i32(len(values)) + bytes(arr)

    else:  # _VT_STRING
        parts = bytearray()
        for s in values:
            invalid.append(len(s) == 0)
            parts += _str_p(s)
        # count + total_byte_size of packed strings + packed strings
        value_bytes = (
            bytes([_ENC_PLAIN, _VT_STRING])
            + _i32(len(values))
            + _i32(len(parts))
            + bytes(parts)
        )

    has_invalid = any(invalid)
    if has_invalid:
        bit_bytes = _bit_array_bytes(invalid)
        is_invalid = (
            _str_u("IsInvalid")
            + bytes([_ENC_BIT_ARRAY, _VT_BOOL])
            + _i32(len(invalid))
            + bit_bytes
        )
        props = _i32(1) + is_invalid
    else:
        props = _i32(0)

    return _section(_SID_COLUMN_SLICE) + value_bytes + props


# ---------------------------------------------------------------------------
# Table metadata
# ---------------------------------------------------------------------------


def _table_metadata(headers: list[str], vtypes: list[int]) -> bytes:
    num_cols = len(headers)
    out = bytearray()

    # Table-level metadata: [TableColumns = Int(num_cols)]
    out += _i32(1)
    out += _str_u("TableColumns")
    out += bytes([_VT_INT])
    out += bytes([1]) + _i32(num_cols)  # present=1, value
    out += bytes([0])  # no default

    # Column count
    out += _i32(num_cols)

    # Unique column metadata types: Name (String) and DataType (Binary)
    out += _i32(2)
    out += _str_u("Name") + bytes([_VT_STRING]) + bytes([0])
    out += _str_u("DataType") + bytes([_VT_BINARY]) + bytes([0])

    # Per-column: present + value for each metadata type
    for name, vtype in zip(headers, vtypes):
        out += bytes([1]) + _str_u(name)
        out += bytes([1]) + _bytes_u(bytes([vtype]))

    return bytes(out)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def csv_to_sbdf(
    csv_bytes: bytes,
    chunk_size: int = 10_000,
    sample_rows: int = 1_000,
) -> bytes:
    reader = csv.reader(io.StringIO(csv_bytes.decode()))
    headers = next(reader, None)
    if headers is None:
        raise ValueError("CSV has no header row")
    num_cols = len(headers)

    def _pad(row: list[str]) -> list[str]:
        return [row[i] if i < len(row) else "" for i in range(num_cols)]

    # Phase 1: sample rows for type inference
    sampled: list[list[str]] = []
    for _, row in zip(range(sample_rows), reader):
        sampled.append(_pad(row))

    vtypes = [_infer_type([r[c] for r in sampled]) for c in range(num_cols)]

    out = bytearray()
    out += _section(_SID_FILE_HEADER) + bytes([1, 0])
    out += _section(_SID_TABLE_METADATA) + _table_metadata(headers, vtypes)

    # Phase 2: stream table slices from sampled queue + remaining rows
    queue: deque[list[str]] = deque(sampled)

    while True:
        while len(queue) < chunk_size:
            row = next(reader, None)
            if row is None:
                break
            queue.append(_pad(row))

        if not queue:
            break

        take = min(chunk_size, len(queue))
        chunk = [queue.popleft() for _ in range(take)]
        at_eof = take < chunk_size

        out += _section(_SID_TABLE_SLICE)
        out += _i32(num_cols)
        for c in range(num_cols):
            out += _column_slice([r[c] for r in chunk], vtypes[c])

        if at_eof:
            break

    out += _section(_SID_TABLE_END)
    return bytes(out)
