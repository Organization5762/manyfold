"""Private bounded SQLite journal for durable transport delivery."""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import final

if os.name == "nt":
    import msvcrt
else:
    import fcntl

_JOURNAL_APPLICATION_ID = 0x4D46444C
_JOURNAL_SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
    message_id TEXT PRIMARY KEY,
    frame_kind INTEGER NOT NULL,
    channel TEXT NOT NULL,
    correlation_id TEXT,
    payload BLOB NOT NULL,
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    attempts INTEGER NOT NULL,
    next_attempt_at REAL NOT NULL,
    last_error TEXT,
    size_bytes INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS outbox_due
ON outbox(next_attempt_at, created_at);
CREATE TABLE IF NOT EXISTS inbox (
    message_id TEXT PRIMARY KEY,
    frame_kind INTEGER NOT NULL,
    channel TEXT NOT NULL,
    correlation_id TEXT,
    payload BLOB NOT NULL,
    delivery_attempt INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'acked')),
    created_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    ack_attempts INTEGER NOT NULL,
    next_ack_at REAL NOT NULL,
    ack_confirmed INTEGER NOT NULL CHECK(ack_confirmed IN (0, 1)),
    size_bytes INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS inbox_pending
ON inbox(status, created_at);
CREATE INDEX IF NOT EXISTS inbox_ack_due
ON inbox(status, ack_confirmed, next_ack_at);
"""


def _record_size(
    message_id: str,
    channel: str,
    correlation_id: str | None,
    payload: bytes,
) -> int:
    return (
        len(message_id.encode("utf-8"))
        + len(channel.encode("utf-8"))
        + (0 if correlation_id is None else len(correlation_id.encode("utf-8")))
        + len(payload)
        + 128
    )


class _InboxDisposition(str, Enum):
    NEW = "new"
    PENDING_DUPLICATE = "pending_duplicate"
    ACKED_DUPLICATE = "acked_duplicate"


@dataclass(frozen=True, slots=True)
class _OutboxRecord:
    message_id: str
    frame_kind: int
    channel: str
    correlation_id: str | None
    payload: bytes
    attempts: int


@dataclass(frozen=True, slots=True)
class _InboxRecord:
    message_id: str
    frame_kind: int
    channel: str
    correlation_id: str | None
    payload: bytes
    delivery_attempt: int
    ack_attempts: int = 0


@dataclass(frozen=True, slots=True)
class _JournalStats:
    outbox_items: int
    pending_inbox_items: int
    acked_inbox_items: int
    logical_bytes: int


@dataclass(frozen=True, slots=True)
class _CompactionResult:
    expired_outbox_ids: tuple[str, ...]
    expired_inbox_ids: tuple[str, ...]


class _JournalError(RuntimeError):
    pass


class _JournalFull(_JournalError):
    pass


class _JournalConflict(_JournalError):
    pass


@final
class _DeliveryJournal:
    def __init__(
        self,
        path: Path,
        *,
        max_outbox_items: int,
        max_inbox_items: int,
        max_storage_bytes: int,
    ) -> None:
        self.path = path
        self._max_outbox_items = max_outbox_items
        self._max_inbox_items = max_inbox_items
        self._max_storage_bytes = max_storage_bytes
        self._lock = Lock()
        self._closed = False
        self._owner_lock: _JournalOwnerLock | None = None
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._owner_lock = _JournalOwnerLock(path)
            self._connection = sqlite3.connect(
                path,
                check_same_thread=False,
                isolation_level=None,
                timeout=5.0,
            )
            self._connection.execute("PRAGMA journal_mode=DELETE")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA temp_store=MEMORY")
            self._connection.execute("PRAGMA auto_vacuum=INCREMENTAL")
            application_id = int(
                self._connection.execute("PRAGMA application_id").fetchone()[0]
            )
            schema_version = int(
                self._connection.execute("PRAGMA user_version").fetchone()[0]
            )
            if application_id not in (0, _JOURNAL_APPLICATION_ID):
                raise _JournalError(
                    f"{path} is not a ManyFold delivery journal"
                )
            if schema_version not in (0, _JOURNAL_SCHEMA_VERSION):
                raise _JournalError(
                    "delivery journal schema version "
                    f"{schema_version} is incompatible with "
                    f"{_JOURNAL_SCHEMA_VERSION}"
                )
            page_size = int(
                self._connection.execute("PRAGMA page_size").fetchone()[0]
            )
            max_pages = max_storage_bytes // page_size
            if max_pages < 8:
                raise ValueError(
                    "max_storage_bytes is too small for the SQLite journal"
                )
            self._connection.execute(f"PRAGMA max_page_count={max_pages}")
            self._connection.executescript(_SCHEMA)
            self._connection.execute(
                f"PRAGMA application_id={_JOURNAL_APPLICATION_ID}"
            )
            self._connection.execute(
                f"PRAGMA user_version={_JOURNAL_SCHEMA_VERSION}"
            )
            current_pages = int(
                self._connection.execute("PRAGMA page_count").fetchone()[0]
            )
            if current_pages > max_pages:
                raise ValueError(
                    "existing delivery journal exceeds max_storage_bytes"
                )
        except (OSError, sqlite3.DatabaseError) as error:
            connection = getattr(self, "_connection", None)
            if connection is not None:
                connection.close()
            if self._owner_lock is not None:
                self._owner_lock.close()
            raise _JournalError(
                f"could not open delivery journal {path}: {error}"
            ) from error
        except BaseException:
            connection = getattr(self, "_connection", None)
            if connection is not None:
                connection.close()
            if self._owner_lock is not None:
                self._owner_lock.close()
            raise

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._connection.close()
            finally:
                if self._owner_lock is not None:
                    self._owner_lock.close()

    def insert_outbox(
        self,
        record: _OutboxRecord,
        *,
        created_at: float,
        expires_at: float,
    ) -> bool:
        size_bytes = _record_size(
            record.message_id,
            record.channel,
            record.correlation_id,
            record.payload,
        )
        with self._transaction() as connection:
            existing = connection.execute(
                """
                SELECT frame_kind, channel, correlation_id, payload
                FROM outbox WHERE message_id = ?
                """,
                (record.message_id,),
            ).fetchone()
            if existing is not None:
                if existing != (
                    record.frame_kind,
                    record.channel,
                    record.correlation_id,
                    record.payload,
                ):
                    raise _JournalConflict(
                        f"outbox message_id {record.message_id!r} has different content"
                    )
                return False
            self._require_capacity(
                connection,
                table="outbox",
                item_limit=self._max_outbox_items,
                added_bytes=size_bytes,
            )
            self._execute_write(
                connection,
                """
                INSERT INTO outbox (
                    message_id, frame_kind, channel, correlation_id, payload,
                    created_at, expires_at, attempts, next_attempt_at,
                    last_error, size_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, NULL, ?)
                """,
                (
                    record.message_id,
                    record.frame_kind,
                    record.channel,
                    record.correlation_id,
                    record.payload,
                    created_at,
                    expires_at,
                    created_at,
                    size_bytes,
                ),
            )
            return True

    def due_outbox(self, now: float, *, limit: int) -> tuple[_OutboxRecord, ...]:
        with self._lock:
            self._require_open()
            rows = self._connection.execute(
                """
                SELECT message_id, frame_kind, channel, correlation_id,
                       payload, attempts
                FROM outbox
                WHERE expires_at > ? AND next_attempt_at <= ?
                ORDER BY created_at
                LIMIT ?
                """,
                (now, now, limit),
            ).fetchall()
        return tuple(_OutboxRecord(*row) for row in rows)

    def mark_outbox_attempt(
        self,
        message_id: str,
        *,
        next_attempt_at: float,
        error: str | None,
        increment_attempts: bool,
    ) -> None:
        attempts = "attempts + 1" if increment_attempts else "attempts"
        with self._transaction() as connection:
            self._execute_write(
                connection,
                f"""
                UPDATE outbox
                SET attempts = {attempts}, next_attempt_at = ?, last_error = ?
                WHERE message_id = ?
                """,
                (next_attempt_at, error, message_id),
            )

    def delete_outbox(self, message_id: str) -> bool:
        with self._transaction() as connection:
            cursor = self._execute_write(
                connection,
                "DELETE FROM outbox WHERE message_id = ?",
                (message_id,),
            )
            return cursor.rowcount > 0

    def record_inbox(
        self,
        record: _InboxRecord,
        *,
        created_at: float,
        expires_at: float,
    ) -> _InboxDisposition:
        size_bytes = _record_size(
            record.message_id,
            record.channel,
            record.correlation_id,
            record.payload,
        )
        with self._transaction() as connection:
            existing = connection.execute(
                """
                SELECT frame_kind, channel, correlation_id, payload, status,
                       delivery_attempt
                FROM inbox WHERE message_id = ?
                """,
                (record.message_id,),
            ).fetchone()
            if existing is not None:
                if existing[:4] != (
                    record.frame_kind,
                    record.channel,
                    record.correlation_id,
                    record.payload,
                ):
                    raise _JournalConflict(
                        f"inbox message_id {record.message_id!r} has different content"
                    )
                if existing[4] == "pending" and record.delivery_attempt > existing[5]:
                    self._execute_write(
                        connection,
                        """
                        UPDATE inbox SET delivery_attempt = ?
                        WHERE message_id = ?
                        """,
                        (record.delivery_attempt, record.message_id),
                    )
                return (
                    _InboxDisposition.ACKED_DUPLICATE
                    if existing[4] == "acked"
                    else _InboxDisposition.PENDING_DUPLICATE
                )
            self._require_capacity(
                connection,
                table="inbox",
                item_limit=self._max_inbox_items,
                added_bytes=size_bytes,
            )
            self._execute_write(
                connection,
                """
                INSERT INTO inbox (
                    message_id, frame_kind, channel, correlation_id, payload,
                    delivery_attempt, status, created_at, expires_at,
                    ack_attempts, next_ack_at, ack_confirmed, size_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, 0, ?, 0, ?)
                """,
                (
                    record.message_id,
                    record.frame_kind,
                    record.channel,
                    record.correlation_id,
                    record.payload,
                    record.delivery_attempt,
                    created_at,
                    expires_at,
                    created_at,
                    size_bytes,
                ),
            )
            return _InboxDisposition.NEW

    def pending_inbox(
        self,
        now: float,
        *,
        limit: int,
    ) -> tuple[_InboxRecord, ...]:
        with self._lock:
            self._require_open()
            rows = self._connection.execute(
                """
                SELECT message_id, frame_kind, channel, correlation_id,
                       payload, delivery_attempt, ack_attempts
                FROM inbox
                WHERE status = 'pending' AND expires_at > ?
                ORDER BY created_at
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
        return tuple(_InboxRecord(*row) for row in rows)

    def is_pending_inbox(self, message_id: str, now: float) -> bool:
        with self._lock:
            self._require_open()
            row = self._connection.execute(
                """
                SELECT 1 FROM inbox
                WHERE message_id = ? AND status = 'pending' AND expires_at > ?
                """,
                (message_id, now),
            ).fetchone()
        return row is not None

    def mark_inbox_acked(
        self,
        message_id: str,
        *,
        next_ack_at: float,
    ) -> bool:
        with self._transaction() as connection:
            cursor = self._execute_write(
                connection,
                """
                UPDATE inbox
                SET status = 'acked', next_ack_at = ?, ack_confirmed = 0
                WHERE message_id = ? AND status = 'pending'
                """,
                (next_ack_at, message_id),
            )
            return cursor.rowcount > 0

    def delete_pending_inbox(self, message_id: str) -> bool:
        with self._transaction() as connection:
            cursor = self._execute_write(
                connection,
                "DELETE FROM inbox WHERE message_id = ? AND status = 'pending'",
                (message_id,),
            )
            return cursor.rowcount > 0

    def due_acks(self, now: float, *, limit: int) -> tuple[_InboxRecord, ...]:
        with self._lock:
            self._require_open()
            rows = self._connection.execute(
                """
                SELECT message_id, frame_kind, channel, correlation_id,
                       payload, delivery_attempt, ack_attempts
                FROM inbox
                WHERE status = 'acked' AND ack_confirmed = 0
                  AND expires_at > ? AND next_ack_at <= ?
                ORDER BY next_ack_at
                LIMIT ?
                """,
                (now, now, limit),
            ).fetchall()
        return tuple(_InboxRecord(*row) for row in rows)

    def mark_ack_attempt(
        self,
        message_id: str,
        *,
        next_ack_at: float,
    ) -> None:
        with self._transaction() as connection:
            self._execute_write(
                connection,
                """
                UPDATE inbox
                SET ack_attempts = ack_attempts + 1, next_ack_at = ?
                WHERE message_id = ? AND status = 'acked'
                """,
                (next_ack_at, message_id),
            )

    def schedule_ack_now(self, message_id: str, now: float) -> None:
        with self._transaction() as connection:
            self._execute_write(
                connection,
                """
                UPDATE inbox
                SET next_ack_at = ?, ack_confirmed = 0
                WHERE message_id = ? AND status = 'acked'
                """,
                (now, message_id),
            )

    def confirm_ack(self, message_id: str) -> None:
        with self._transaction() as connection:
            self._execute_write(
                connection,
                """
                UPDATE inbox SET ack_confirmed = 1
                WHERE message_id = ? AND status = 'acked'
                """,
                (message_id,),
            )

    def compact(self, now: float) -> _CompactionResult:
        with self._transaction() as connection:
            expired_outbox_ids = tuple(
                str(row[0])
                for row in connection.execute(
                    "SELECT message_id FROM outbox WHERE expires_at <= ?",
                    (now,),
                ).fetchall()
            )
            expired_inbox_ids = tuple(
                str(row[0])
                for row in connection.execute(
                    "SELECT message_id FROM inbox WHERE expires_at <= ?",
                    (now,),
                ).fetchall()
            )
            self._execute_write(
                connection,
                "DELETE FROM outbox WHERE expires_at <= ?",
                (now,),
            )
            self._execute_write(
                connection,
                "DELETE FROM inbox WHERE expires_at <= ?",
                (now,),
            )
            connection.execute("PRAGMA incremental_vacuum(16)")
            return _CompactionResult(expired_outbox_ids, expired_inbox_ids)

    def stats(self) -> _JournalStats:
        with self._lock:
            self._require_open()
            outbox_items = self._count(self._connection, "outbox")
            pending = int(
                self._connection.execute(
                    "SELECT COUNT(*) FROM inbox WHERE status = 'pending'"
                ).fetchone()[0]
            )
            acked = int(
                self._connection.execute(
                    "SELECT COUNT(*) FROM inbox WHERE status = 'acked'"
                ).fetchone()[0]
            )
            logical_bytes = self._logical_bytes(self._connection)
        return _JournalStats(outbox_items, pending, acked, logical_bytes)

    def _require_capacity(
        self,
        connection: sqlite3.Connection,
        *,
        table: str,
        item_limit: int,
        added_bytes: int,
    ) -> None:
        if self._count(connection, table) >= item_limit:
            raise _JournalFull(f"{table} item limit {item_limit} is full")
        logical_bytes = self._logical_bytes(connection)
        if logical_bytes + added_bytes > self._max_storage_bytes:
            raise _JournalFull(
                "delivery journal logical byte limit "
                f"{self._max_storage_bytes} would be exceeded"
            )

    def _logical_bytes(self, connection: sqlite3.Connection) -> int:
        outbox_bytes = int(
            connection.execute(
                "SELECT COALESCE(SUM(size_bytes), 0) FROM outbox"
            ).fetchone()[0]
        )
        inbox_bytes = int(
            connection.execute(
                "SELECT COALESCE(SUM(size_bytes), 0) FROM inbox"
            ).fetchone()[0]
        )
        return outbox_bytes + inbox_bytes

    def _execute_write(
        self,
        connection: sqlite3.Connection,
        statement: str,
        parameters: tuple[object, ...],
    ) -> sqlite3.Cursor:
        try:
            return connection.execute(statement, parameters)
        except sqlite3.OperationalError as error:
            if "database or disk is full" in str(error).lower():
                raise _JournalFull(
                    f"delivery journal reached {self._max_storage_bytes} bytes"
                ) from error
            raise _JournalError(f"could not write delivery journal: {error}") from error
        except sqlite3.DatabaseError as error:
            raise _JournalError(f"could not write delivery journal: {error}") from error

    def _count(self, connection: sqlite3.Connection, table: str) -> int:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def _require_open(self) -> None:
        if self._closed:
            raise _JournalError("delivery journal is closed")

    def _transaction(self) -> "_JournalTransaction":
        return _JournalTransaction(self)


@final
class _JournalTransaction:
    def __init__(self, journal: _DeliveryJournal) -> None:
        self._journal = journal

    def __enter__(self) -> sqlite3.Connection:
        self._journal._lock.acquire()
        try:
            self._journal._require_open()
            self._journal._connection.execute("BEGIN IMMEDIATE")
        except BaseException:
            self._journal._lock.release()
            raise
        return self._journal._connection

    def __exit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: object,
    ) -> None:
        try:
            if error_type is None:
                self._journal._connection.execute("COMMIT")
            else:
                self._journal._connection.execute("ROLLBACK")
        finally:
            self._journal._lock.release()


@final
class _JournalOwnerLock:
    def __init__(self, journal_path: Path) -> None:
        self._path = journal_path.with_name(f"{journal_path.name}.lock")
        self._file = self._path.open("a+b")
        try:
            if os.name == "nt":
                if self._file.seek(0, os.SEEK_END) == 0:
                    self._file.write(b"\0")
                    self._file.flush()
                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(
                    self._file.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
        except OSError as error:
            self._file.close()
            raise _JournalError(
                f"delivery journal {journal_path} is already owned"
            ) from error

    def close(self) -> None:
        if self._file.closed:
            return
        try:
            if os.name == "nt":
                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._file.close()
