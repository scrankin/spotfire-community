"""Pure-Python SBDF binary writer (CSV → SBDF).

Internal module. Callers should use :mod:`spotfire_community.sbdf`, which
re-exports the high-level API (:func:`create_sbdf`, :class:`SbdfStreamingWriter`,
:class:`ValueType`, :func:`infer_types`).
"""

from __future__ import annotations

import csv
import io
import struct
from collections import deque
from collections.abc import Sequence
from datetime import date, datetime, timezone

# Section IDs
SID_FILE_HEADER = 0x01
SID_TABLE_METADATA = 0x02
SID_TABLE_SLICE = 0x03
SID_COLUMN_SLICE = 0x04
SID_TABLE_END = 0x05

# Value types
VT_BOOL = 0x01
VT_INT = 0x02
VT_LONG = 0x03
VT_DOUBLE = 0x05
VT_DATETIME = 0x06
VT_DATE = 0x07
VT_STRING = 0x0A
VT_BINARY = 0x0C

ENC_PLAIN = 0x01
ENC_BIT_ARRAY = 0x03

MAGIC = b"\xdf\x5b"

# Spotfire SBDF stores Date and DateTime as int64 milliseconds since
# 0001-01-01 00:00:00 UTC. See pod2co/sbdf crate for reference semantics.
_SBDF_EPOCH = datetime(1, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Primitives
# ---------------------------------------------------------------------------


def i32(v: int) -> bytes:
    return struct.pack("<i", v)


def pack7(v: int) -> bytes:
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


def str_u(s: str) -> bytes:
    """String unpacked: i32(byte_len) + utf-8."""
    enc = s.encode()
    return i32(len(enc)) + enc


def str_p(s: str) -> bytes:
    """String packed: pack7(byte_len) + utf-8."""
    enc = s.encode()
    return pack7(len(enc)) + enc


def bytes_u(b: bytes) -> bytes:
    """Bytes unpacked: i32(len) + data."""
    return i32(len(b)) + b


def section(sid: int) -> bytes:
    return MAGIC + bytes([sid])


# ---------------------------------------------------------------------------
# Date/DateTime helpers
# ---------------------------------------------------------------------------


def _timedelta_ms(dt: datetime) -> int:
    """Return milliseconds from the SBDF epoch to *dt* using exact integer math.

    ``timedelta.total_seconds()`` returns a float and can lose precision on
    large deltas; computing from ``days``/``seconds``/``microseconds`` keeps
    the result exact and deterministic.
    """
    delta = dt - _SBDF_EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def _parse_datetime_ms(s: str) -> int | None:
    """Parse *s* as an ISO-8601 datetime; return milliseconds since SBDF epoch.

    Accepts a trailing ``Z`` (UTC) which ``datetime.fromisoformat`` does not
    handle natively on Python 3.10. Naive inputs (no offset) are interpreted
    as UTC. Returns ``None`` if *s* cannot be parsed.
    """
    normalized = s[:-1] + "+00:00" if s.endswith("Z") else s
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return _timedelta_ms(dt)


def _parse_date_ms(s: str) -> int | None:
    """Parse *s* as an ISO date (YYYY-MM-DD); return milliseconds since SBDF epoch.

    Returns ``None`` if *s* cannot be parsed.
    """
    try:
        d = date.fromisoformat(s)
    except ValueError:
        return None
    return _timedelta_ms(datetime(d.year, d.month, d.day, tzinfo=timezone.utc))


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


def infer_type(sample: list[str]) -> int:
    """Pick the most specific SBDF value type that accepts every non-empty value.

    Tries in order: Bool, Int, Long, Double, Date, DateTime, then falls back
    to String. Empty strings are treated as null and do not constrain the type.
    """
    nz = [v for v in sample if v]
    if not nz:
        return VT_STRING
    if all(_parse_bool(v) is not None for v in nz):
        return VT_BOOL
    try:
        parsed = [int(v) for v in nz]
        if all(-(2**31) <= n <= 2**31 - 1 for n in parsed):
            return VT_INT
        if all(-(2**63) <= n <= 2**63 - 1 for n in parsed):
            return VT_LONG
    except ValueError:
        pass
    try:
        for v in nz:
            float(v)
        return VT_DOUBLE
    except ValueError:
        pass
    # Date is checked before DateTime so "YYYY-MM-DD" stays Date rather than
    # promoting to DateTime (datetime.fromisoformat accepts bare dates).
    if all(_parse_date_ms(v) is not None for v in nz) and not any(
        "T" in v or " " in v for v in nz
    ):
        return VT_DATE
    if all(_parse_datetime_ms(v) is not None for v in nz):
        return VT_DATETIME
    return VT_STRING


# ---------------------------------------------------------------------------
# Column slice encoding
# ---------------------------------------------------------------------------


def _bit_array_bytes(flags: list[bool]) -> bytes:
    buf = bytearray((len(flags) + 7) // 8)
    for i, f in enumerate(flags):
        if f:
            buf[i >> 3] |= 0x80 >> (i & 7)
    return bytes(buf)


def column_slice(values: list[str], vtype: int) -> bytes:
    """Encode one column as a ColumnSlice (section marker + values + props).

    Rows that cannot be parsed as *vtype* are encoded with a placeholder value
    and marked in the column's IsInvalid bit array, matching the behaviour of
    the existing Bool/Int/Long/Double branches.
    """
    invalid: list[bool] = []

    if vtype == VT_BOOL:
        arr = bytearray()
        for s in values:
            b = _parse_bool(s)
            if b is None:
                invalid.append(True)
                arr.append(0)
            else:
                invalid.append(False)
                arr.append(1 if b else 0)
        payload = i32(len(values)) + bytes(arr)
        value_bytes = bytes([ENC_PLAIN, VT_BOOL]) + payload

    elif vtype == VT_INT:
        arr = bytearray()
        for s in values:
            try:
                arr += struct.pack("<i", int(s))
                invalid.append(False)
            except (ValueError, struct.error):
                arr += struct.pack("<i", 0)
                invalid.append(True)
        value_bytes = bytes([ENC_PLAIN, VT_INT]) + i32(len(values)) + bytes(arr)

    elif vtype == VT_LONG:
        arr = bytearray()
        for s in values:
            try:
                arr += struct.pack("<q", int(s))
                invalid.append(False)
            except (ValueError, struct.error):
                arr += struct.pack("<q", 0)
                invalid.append(True)
        value_bytes = bytes([ENC_PLAIN, VT_LONG]) + i32(len(values)) + bytes(arr)

    elif vtype == VT_DOUBLE:
        arr = bytearray()
        for s in values:
            try:
                arr += struct.pack("<d", float(s))
                invalid.append(False)
            except (ValueError, struct.error):
                arr += struct.pack("<d", 0.0)
                invalid.append(True)
        value_bytes = bytes([ENC_PLAIN, VT_DOUBLE]) + i32(len(values)) + bytes(arr)

    elif vtype == VT_DATETIME:
        arr = bytearray()
        for s in values:
            ms = _parse_datetime_ms(s) if s else None
            if ms is None:
                invalid.append(True)
                arr += struct.pack("<q", 0)
            else:
                invalid.append(False)
                arr += struct.pack("<q", ms)
        value_bytes = bytes([ENC_PLAIN, VT_DATETIME]) + i32(len(values)) + bytes(arr)

    elif vtype == VT_DATE:
        arr = bytearray()
        for s in values:
            ms = _parse_date_ms(s) if s else None
            if ms is None:
                invalid.append(True)
                arr += struct.pack("<q", 0)
            else:
                invalid.append(False)
                arr += struct.pack("<q", ms)
        value_bytes = bytes([ENC_PLAIN, VT_DATE]) + i32(len(values)) + bytes(arr)

    else:  # VT_STRING
        parts = bytearray()
        for s in values:
            invalid.append(len(s) == 0)
            parts += str_p(s)
        value_bytes = (
            bytes([ENC_PLAIN, VT_STRING])
            + i32(len(values))
            + i32(len(parts))
            + bytes(parts)
        )

    has_invalid = any(invalid)
    if has_invalid:
        bit_bytes = _bit_array_bytes(invalid)
        is_invalid = (
            str_u("IsInvalid")
            + bytes([ENC_BIT_ARRAY, VT_BOOL])
            + i32(len(invalid))
            + bit_bytes
        )
        props = i32(1) + is_invalid
    else:
        props = i32(0)

    return section(SID_COLUMN_SLICE) + value_bytes + props


# ---------------------------------------------------------------------------
# Table metadata
# ---------------------------------------------------------------------------


def table_metadata(headers: Sequence[str], vtypes: Sequence[int]) -> bytes:
    num_cols = len(headers)
    out = bytearray()

    # Table-level metadata: [TableColumns = Int(num_cols)]
    out += i32(1)
    out += str_u("TableColumns")
    out += bytes([VT_INT])
    out += bytes([1]) + i32(num_cols)  # present=1, value
    out += bytes([0])  # no default

    # Column count
    out += i32(num_cols)

    # Unique column metadata types: Name (String) and DataType (Binary)
    out += i32(2)
    out += str_u("Name") + bytes([VT_STRING]) + bytes([0])
    out += str_u("DataType") + bytes([VT_BINARY]) + bytes([0])

    # Per-column: present + value for each metadata type
    for name, vtype in zip(headers, vtypes):
        out += bytes([1]) + str_u(name)
        out += bytes([1]) + bytes_u(bytes([vtype]))

    return bytes(out)


# ---------------------------------------------------------------------------
# Public entry point (used by create_sbdf)
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

    vtypes = [infer_type([r[c] for r in sampled]) for c in range(num_cols)]

    out = bytearray()
    out += section(SID_FILE_HEADER) + bytes([1, 0])
    out += section(SID_TABLE_METADATA) + table_metadata(headers, vtypes)

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

        out += section(SID_TABLE_SLICE)
        out += i32(num_cols)
        for c in range(num_cols):
            out += column_slice([r[c] for r in chunk], vtypes[c])

        if at_eof:
            break

    out += section(SID_TABLE_END)
    return bytes(out)
