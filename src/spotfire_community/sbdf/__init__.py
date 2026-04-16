"""Helpers for converting tabular data to Spotfire Binary Data Format (SBDF).

This module wraps the ``tabular-to-sbdf`` CLI binary which handles the actual
CSV-to-SBDF conversion using the pod2co/sbdf Rust library.
"""

import logging
import os
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import IO

logger = logging.getLogger(__name__)

CLI_BINARY_NAME = "tabular-to-sbdf"


def _find_binary() -> str:
    """Locate the tabular-to-sbdf binary on PATH or bundled alongside this package.

    On Windows the bundled binary may have a ``.exe`` suffix, which is checked first.

    Returns:
        The absolute path to the binary.

    Raises:
        FileNotFoundError: If the binary cannot be found.
    """
    bundled_dir = Path(__file__).parent / "bin"
    candidates = [bundled_dir / CLI_BINARY_NAME]
    if os.name == "nt":
        candidates.insert(0, bundled_dir / f"{CLI_BINARY_NAME}.exe")

    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)

    on_path = shutil.which(CLI_BINARY_NAME)
    if on_path is not None:
        return on_path

    raise FileNotFoundError(
        f"'{CLI_BINARY_NAME}' not found. Install it or place the binary "
        f"in {bundled_dir} or on your PATH."
    )


def open_converter(
    chunk_size: int = 10_000,
    verbose: bool = False,
    stderr: int | IO[bytes] | None = subprocess.DEVNULL,
) -> subprocess.Popen[bytes]:
    """Open the tabular-to-sbdf CLI as a subprocess with stdin/stdout pipes.

    The caller writes CSV data to ``proc.stdin`` and reads SBDF bytes from
    ``proc.stdout``.

    Args:
        chunk_size: Number of rows per SBDF table slice.
        verbose: If True, pass ``--verbose`` to the CLI to emit progress logs.
        stderr: How to handle the subprocess stderr stream. Defaults to
            ``subprocess.DEVNULL`` to avoid pipe-fill deadlocks; pass
            ``None`` to inherit the parent's stderr, a file handle to
            redirect, or ``subprocess.PIPE`` if you plan to drain it in
            a background thread.

    Returns:
        A running subprocess with ``stdin=PIPE`` and ``stdout=PIPE``.
    """
    binary = _find_binary()
    cmd = [binary, "--format", "csv", "--chunk-size", str(chunk_size)]
    if verbose:
        cmd.append("--verbose")
    logger.info("Starting SBDF converter: %s", " ".join(cmd))

    return subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=stderr,
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
    if proc.stdout is None:
        raise ValueError(
            "read_sbdf_chunks() requires a subprocess opened with "
            "stdout=subprocess.PIPE"
        )

    while True:
        chunk = proc.stdout.read(read_size)
        if not chunk:
            break
        yield chunk


__all__ = [
    "open_converter",
    "read_sbdf_chunks",
]
