"""Cross-process locking tied to the opened journal file identity.

One supervised child owns the platform file lock without opening SQLite. Its
memory is bounded by the Python runtime plus a fixed 4 KiB control read, and the
parent closes, terminates, kills, and reaps it under explicit deadlines. The
random token correlates the startup response with this parent; it is not an
authentication or security boundary.
"""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
from pathlib import Path
from queue import Empty, Queue
from threading import Thread
from typing import BinaryIO, final

# DeliveryConfig caps the database below SQLite's 0x40000000 locking region.
# SQLite occupies the next 512 bytes, so this byte is outside both journal
# content and SQLite's locks, including mandatory Windows range locks.
_OWNER_LOCK_OFFSET = (1 << 30) + 512
_LOCK_START_TIMEOUT_SECONDS = 2.0
_LOCK_STOP_TIMEOUT_SECONDS = 2.0
_LOCK_PROCESS_MARKER = "manyfold-delivery-owner-lock"


def _require_single_link(journal_path: Path) -> None:
    if journal_path.exists() and journal_path.stat().st_nlink > 1:
        raise _JournalLockError(
            f"delivery journal {journal_path} is hard-linked"
        )


def _read_status(
    output: BinaryIO,
    results: Queue[tuple[str, ...] | BaseException],
) -> None:
    try:
        payload = output.readline(257)
        if not payload.endswith(b"\n") or len(payload) > 256:
            raise _JournalLockError(
                "delivery journal owner lock returned an invalid status"
            )
        results.put(tuple(payload[:-1].decode("ascii").split(" ")))
    except BaseException as error:
        results.put(error)


def _close_binary_stream(
    stream: BinaryIO,
    errors: list[BaseException],
) -> None:
    file_descriptor: int | None
    try:
        file_descriptor = stream.fileno()
    except (OSError, ValueError):
        file_descriptor = None
    try:
        stream.close()
    except BaseException as error:
        errors.append(error)
        if file_descriptor is not None:
            try:
                os.close(file_descriptor)
            except OSError:
                pass


def _stop_and_reap(
    process: subprocess.Popen[bytes],
) -> list[BaseException]:
    errors: list[BaseException] = []
    if _wait_for_process(process, errors):
        return errors
    try:
        process.terminate()
    except BaseException as error:
        errors.append(error)
    if _wait_for_process(process, errors):
        return errors
    try:
        process.kill()
    except BaseException as error:
        errors.append(error)
    if not _wait_for_process(process, errors):
        errors.append(
            _JournalLockError(
                "delivery journal owner lock did not exit after kill"
            )
        )
    return errors


def _wait_for_process(
    process: subprocess.Popen[bytes],
    errors: list[BaseException],
) -> bool:
    try:
        process.wait(timeout=_LOCK_STOP_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        return False
    except BaseException as error:
        errors.append(error)
        return process.poll() is not None
    return True


@final
class _JournalLockError(RuntimeError):
    pass


@final
class _JournalOwnerLock:
    def __init__(self, journal_path: Path) -> None:
        self._closed = False
        self._journal_file: BinaryIO | None = None
        self._identity: tuple[int, int] | None = None
        self._process: subprocess.Popen[bytes] | None = None
        self._control: BinaryIO | None = None
        try:
            self._journal_file = journal_path.open("a+b")
            opened = os.fstat(self._journal_file.fileno())
            self._identity = (opened.st_dev, opened.st_ino)
            self._start_holder(journal_path)
        except BaseException as error:
            try:
                self._release()
            except BaseException as cleanup_error:
                error.add_note(
                    "delivery journal owner cleanup also failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
            raise

    def path_matches_identity(self, journal_path: Path) -> bool:
        if self._journal_file is None:
            return False
        opened = os.fstat(self._journal_file.fileno())
        current = journal_path.stat()
        return (opened.st_dev, opened.st_ino) == (
            current.st_dev,
            current.st_ino,
        )

    def require_path_identity(self, journal_path: Path) -> None:
        if not self.path_matches_identity(journal_path):
            raise _JournalLockError(
                "delivery journal path changed while ownership was acquired"
            )

    def require_alive(self) -> None:
        if self._process is None or self._process.poll() is not None:
            raise _JournalLockError(
                "delivery journal owner lock exited unexpectedly"
            )

    def is_released(self) -> bool:
        return self._is_fully_released()

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._release()
        finally:
            self._closed = self._is_fully_released()

    def _start_holder(self, journal_path: Path) -> None:
        token = secrets.token_hex(32)
        try:
            self._process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "manyfold.architecture._transport_delivery_lock_holder",
                    str(journal_path),
                    str(_OWNER_LOCK_OFFSET),
                    token,
                    _LOCK_PROCESS_MARKER,
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
            if self._process.stdin is None or self._process.stdout is None:
                raise _JournalLockError(
                    "delivery journal owner lock pipes are unavailable"
                )
            self._control = self._process.stdin
            results: Queue[tuple[str, ...] | BaseException] = Queue(maxsize=1)
            reader = Thread(
                target=_read_status,
                args=(self._process.stdout, results),
                name="manyfold-delivery-owner-lock-start",
            )
            reader.start()
            try:
                status = results.get(timeout=_LOCK_START_TIMEOUT_SECONDS)
            except Empty as error:
                raise _JournalLockError(
                    "delivery journal owner lock did not start in time"
                ) from error
            finally:
                if reader.is_alive():
                    self._process.terminate()
                reader.join(timeout=_LOCK_START_TIMEOUT_SECONDS)
            if isinstance(status, BaseException):
                raise status
            expected_ready = (
                token,
                "ready",
                str(self._identity[0]),
                str(self._identity[1]),
            )
            expected_owned = (
                token,
                "owned",
                str(self._identity[0]),
                str(self._identity[1]),
            )
            if status == expected_owned:
                raise _JournalLockError(
                    f"delivery journal {journal_path} is already owned"
                )
            if status[:4] == (
                token,
                "error",
                str(self._identity[0]),
                str(self._identity[1]),
            ):
                detail = " ".join(status[4:])
                raise _JournalLockError(
                    "delivery journal owner lock helper failed: "
                    f"{detail}"
                )
            if status != expected_ready:
                raise _JournalLockError(
                    "delivery journal owner lock returned an invalid status"
                )
            self._process.stdout.close()
        except (OSError, subprocess.SubprocessError) as error:
            raise _JournalLockError(
                f"could not establish delivery journal ownership: {error}"
            ) from error

    def _release(self) -> None:
        errors: list[BaseException] = []
        if self._control is not None:
            _close_binary_stream(self._control, errors)
            self._control = None
        if self._process is not None:
            process = self._process
            if process.stdout is not None and not process.stdout.closed:
                _close_binary_stream(process.stdout, errors)
            errors.extend(_stop_and_reap(process))
            if process.poll() is not None:
                self._process = None
        if self._journal_file is not None:
            _close_binary_stream(self._journal_file, errors)
            self._journal_file = None
        if self._identity is not None and self._process is None:
            self._identity = None
        if errors:
            first_error = errors[0]
            raise _JournalLockError(
                "could not completely release delivery journal ownership: "
                f"{type(first_error).__name__}: {first_error}"
            ) from first_error

    def _is_fully_released(self) -> bool:
        return (
            self._control is None
            and self._process is None
            and self._journal_file is None
            and self._identity is None
        )
