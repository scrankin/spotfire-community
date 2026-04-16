"""Helpers for converting tabular data to Spotfire Binary Data Format (SBDF).

This module wraps the ``tabular-to-sbdf`` CLI binary which handles the actual
CSV/Parquet-to-SBDF conversion using the pod2co/sbdf Rust library.
"""

import logging
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path

logger = logging.getLogger(__name__)

CLI_BINARY_NAME = "tabular-to-sbdf"


def _find_binary() -> str:
    """Locate the tabular-to-sbdf binary on PATH or bundled alongside this package.

    Returns:
        The absolute path to the binary.

    Raises:
        FileNotFoundError: If the binary cannot be found.
    """
    # Check bundled location first (sibling bin/ directory)
    bundled = Path(__file__).parent / "bin" / CLI_BINARY_NAME
    if bundled.is_file():
        return str(bundled)

    # Fall back to PATH
    on_path = shutil.which(CLI_BINARY_NAME)
    if on_path is not None:
        return on_path

    raise FileNotFoundError(
        f"'{CLI_BINARY_NAME}' not found. Install it or place the binary "
        f"in {bundled.parent} or on your PATH."
    )


def open_converter(
    input_format: str = "csv",
    chunk_size: int = 10_000,
) -> subprocess.Popen[bytes]:
    """Open the tabular-to-sbdf CLI as a subprocess with stdin/stdout pipes.

    The caller writes tabular data (CSV or Parquet) to ``proc.stdin`` and reads
    SBDF bytes from ``proc.stdout``.

    Args:
        input_format: Input data format — ``"csv"`` or ``"parquet"``.
        chunk_size: Number of rows per SBDF table slice.

    Returns:
        A running subprocess with ``stdin=PIPE`` and ``stdout=PIPE``.
    """
    binary = _find_binary()
    cmd = [binary, "--format", input_format, "--chunk-size", str(chunk_size)]
    logger.info("Starting SBDF converter: %s", " ".join(cmd))

    return subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def read_sbdf_chunks(
    proc: subprocess.Popen[bytes],
    read_size: int = 4 * 1024 * 1024,
) -> Iterator[bytes]:
    """Yield SBDF bytes chunks from the converter's stdout.

    Reads ``read_size`` bytes at a time from the subprocess stdout pipe.
    Suitable for feeding directly into
    :meth:`~spotfire_community.library.client.LibraryClient.upload_file_streaming`.

    Args:
        proc: A running tabular-to-sbdf subprocess (from :func:`open_converter`).
        read_size: Number of bytes to read per iteration (default 4 MB).

    Yields:
        Non-empty bytes chunks of SBDF data.
    """
    assert proc.stdout is not None, "subprocess must be opened with stdout=PIPE"

    while True:
        chunk = proc.stdout.read(read_size)
        if not chunk:
            break
        yield chunk


__all__ = [
    "open_converter",
    "read_sbdf_chunks",
]
