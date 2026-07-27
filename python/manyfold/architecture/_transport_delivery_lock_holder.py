"""Minimal supervised process that holds one delivery-journal file lock."""

from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from errno import EACCES, EAGAIN
from pathlib import Path

if os.name == "nt":
    import msvcrt
else:
    import fcntl

_CONTROL_READ_BYTES = 4096
_PROCESS_MARKER = "manyfold-delivery-owner-lock"


def _main(arguments: Sequence[str] | None = None) -> int:
    resolved_arguments = (
        tuple(sys.argv[1:]) if arguments is None else tuple(arguments)
    )
    if len(resolved_arguments) != 4:
        return 2
    raw_path, raw_offset, token, process_marker = resolved_arguments
    if process_marker != _PROCESS_MARKER:
        return 2
    journal_path = Path(raw_path)
    offset = int(raw_offset)
    with journal_path.open("a+b") as journal_file:
        lock_error = _try_lock(journal_file.fileno(), offset)
        identity = os.fstat(journal_file.fileno())
        if lock_error is None:
            status = "ready"
            detail = ""
        elif _is_contention(lock_error):
            status = "owned"
            detail = ""
        else:
            error_number = (
                "none" if lock_error.errno is None else str(lock_error.errno)
            )
            windows_error = getattr(lock_error, "winerror", None)
            status = "error"
            detail = f" errno={error_number} winerror={windows_error}"
        sys.stdout.buffer.write(
            f"{token} {status} {identity.st_dev} {identity.st_ino}{detail}\n".encode(
                "ascii"
            )
        )
        sys.stdout.buffer.flush()
        if lock_error is not None:
            return 3
        while sys.stdin.buffer.read(_CONTROL_READ_BYTES):
            pass
    return 0


def _try_lock(file_descriptor: int, offset: int) -> OSError | None:
    try:
        if os.name == "nt":
            os.lseek(file_descriptor, offset, os.SEEK_SET)
            msvcrt.locking(file_descriptor, msvcrt.LK_NBLCK, 1)
        else:
            fcntl.lockf(
                file_descriptor,
                fcntl.LOCK_EX | fcntl.LOCK_NB,
                1,
                offset,
                os.SEEK_SET,
            )
    except OSError as error:
        return error
    return None


def _is_contention(error: OSError) -> bool:
    if error.errno in {EACCES, EAGAIN}:
        return True
    return os.name == "nt" and getattr(error, "winerror", None) in {32, 33}


if __name__ == "__main__":
    raise SystemExit(_main())
