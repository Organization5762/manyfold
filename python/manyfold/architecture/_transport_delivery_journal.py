"""Single-connection SQLite owner for bounded durable delivery state."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from threading import Lock
from time import time
from typing import final

from ._transport_delivery_capacity import (
    _at_watermark,
    _crossed_watermarks,
    _inbox_capacity,
    _journal_stats,
    _logical_bytes,
    _outbox_capacity,
    _project_capacity,
    _require_capacity,
    _topic_stats,
)
from ._transport_delivery_cleanup import (
    _compact_outbox,
    _delete_lifecycle,
    _expire_pending_inbox,
    _latest_outbox_slot,
    _outbox_lifecycle,
    _select_inbox_lifecycle,
)
from ._transport_delivery_journal_errors import (
    _execute_write,
    _JournalConflict,
    _JournalError,
    _JournalFull,
    _translate_sqlite_error,
)
from ._transport_delivery_lock import (
    _JournalLockError,
    _JournalOwnerLock,
    _require_single_link,
)
from ._transport_delivery_outcomes import (
    _confirm_response,
    _mark_response_attempt,
    _OutcomeStorageFull,
    _record_terminal_rejection_with_capacity,
    _schedule_response,
    _TerminalRejectionResult,
)
from ._transport_delivery_policy import (
    DeliveryConfig,
    TopicDeliveryPolicy,
    _bounded_retry_delay,
)
from ._transport_delivery_queries import (
    _due_outbox,
    _due_responses,
    _pending_inbox_batch,
)
from ._transport_delivery_records import (
    _CompactionResult,
    _inbox_logical_bytes,
    _InboxDisposition,
    _InboxInsertResult,
    _InboxRecord,
    _JournalStats,
    _LifecycleRecord,
    _outbox_logical_bytes,
    _OutboxDisposition,
    _OutboxInsertResult,
    _OutboxRecord,
    _OutboxReplayRecord,
    _ReplayCursor,
    _TopicStats,
    _WatermarkCrossing,
)
from ._transport_delivery_recovery import (
    _outbox_replay_batch,
    _RecoveredWatermark,
    _RecoveryCapacityViolation,
    _RecoveryViolation,
    _validate_recovery,
)
from ._transport_delivery_schema import (
    _MAX_MESSAGE_SEQUENCE,
    _initialize_schema,
)

_MESSAGE_ID_SEQUENCE_BLOCK = 1024


@final
class _DeliveryJournal:
    """Own exactly one SQLite connection and serialize every use of it."""

    def __init__(self, config: DeliveryConfig) -> None:
        self.path = config.journal_path
        self._config = config
        self._lock = Lock()
        self._closed = False
        self._connection_closed = False
        self._owner_lock: _JournalOwnerLock | None = None
        self._next_message_sequence = 1
        self._message_sequence_limit = 0
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            _require_single_link(self.path)
            self._owner_lock = _JournalOwnerLock(self.path)
            self._owner_lock.require_path_identity(self.path)
            self._connection = sqlite3.connect(
                self.path,
                check_same_thread=False,
                isolation_level=None,
                timeout=5.0,
            )
            self._owner_lock.require_path_identity(self.path)
            _require_single_link(self.path)
            page_size = int(
                self._connection.execute("PRAGMA page_size").fetchone()[0]
            )
            max_pages = config.max_storage_bytes // page_size
            if max_pages < 8:
                raise ValueError(
                    "max_storage_bytes is too small for the SQLite journal"
                )
            current_pages = int(
                self._connection.execute("PRAGMA page_count").fetchone()[0]
            )
            if current_pages > max_pages:
                raise _JournalFull(
                    "existing delivery journal exceeds max_storage_bytes"
                )
            self._connection.execute(f"PRAGMA max_page_count={max_pages}")
            self._connection.execute("PRAGMA journal_mode=DELETE")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute("PRAGMA temp_store=FILE")
            self._connection.execute("PRAGMA auto_vacuum=INCREMENTAL")
            _initialize_schema(
                self._connection,
                config=config,
                recovery_now=time(),
            )
            self._message_namespace = str(
                self._connection.execute(
                    """
                    SELECT value FROM journal_metadata
                    WHERE key = 'message_namespace'
                    """
                ).fetchone()[0]
            )
            self._owner_lock.require_alive()
        except (OSError, sqlite3.DatabaseError, _JournalLockError) as error:
            self._release_after_failed_open(error)
            raise _JournalError(
                f"could not open delivery journal {self.path}: {error}"
            ) from error
        except BaseException as error:
            self._release_after_failed_open(error)
            raise

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._release_resources("close delivery journal")

    def is_released(self) -> bool:
        with self._lock:
            return self._closed

    def has_open_connection(self) -> bool:
        with self._lock:
            return not self._connection_closed

    def next_message_id(self) -> str:
        with self._transaction() as connection:
            if self._next_message_sequence > self._message_sequence_limit:
                previous_limit = int(
                    connection.execute(
                        """
                        SELECT value FROM journal_metadata
                        WHERE key = 'message_sequence'
                        """
                    ).fetchone()[0]
                )
                if previous_limit >= _MAX_MESSAGE_SEQUENCE:
                    raise _JournalFull(
                        "delivery journal message identifier space is exhausted"
                    )
                next_limit = previous_limit + _MESSAGE_ID_SEQUENCE_BLOCK
                next_limit = min(next_limit, _MAX_MESSAGE_SEQUENCE)
                _execute_write(
                    connection,
                    """
                    UPDATE journal_metadata SET value = ?
                    WHERE key = 'message_sequence'
                    """,
                    (str(next_limit),),
                )
                self._next_message_sequence = previous_limit + 1
                self._message_sequence_limit = next_limit
            sequence = self._next_message_sequence
            self._next_message_sequence += 1
        return f"{self._message_namespace}-{sequence:016x}"

    def validate_recovery(
        self,
        policies: Mapping[str, TopicDeliveryPolicy],
        *,
        max_transport_payload_bytes: int,
        recovery_now: float,
        enforce_bounds: bool = True,
    ) -> tuple[_RecoveredWatermark, ...]:
        """Fail closed on policy drift and return above-watermark topics."""
        with self._lock:
            self._require_open()
            try:
                return _validate_recovery(
                    self._connection,
                    self._config,
                    policies,
                    _journal_stats(self._connection),
                    max_transport_payload_bytes=max_transport_payload_bytes,
                    recovery_now=recovery_now,
                    enforce_bounds=enforce_bounds,
                )
            except _RecoveryCapacityViolation as error:
                raise _JournalFull(
                    str(error),
                    capacity=error.capacity,
                ) from error
            except _RecoveryViolation as error:
                raise _JournalError(
                    f"delivery journal retained data is corrupt: {error}"
                ) from error
            except (
                OverflowError,
                sqlite3.DatabaseError,
                TypeError,
                ValueError,
            ) as error:
                raise _JournalError(
                    "delivery journal retained data could not be decoded: "
                    f"{error}"
                ) from error

    def insert_outbox(
        self,
        record: _OutboxRecord,
        *,
        created_at: float,
        expires_at: float,
        now: float,
        policy: TopicDeliveryPolicy,
    ) -> _OutboxInsertResult:
        size_bytes = _outbox_logical_bytes(record)
        with self._transaction() as connection:
            existing = connection.execute(
                """
                SELECT channel, semantics, source_key, frame_kind,
                       correlation_id, payload
                FROM outbox WHERE message_id = ?
                """,
                (record.message_id,),
            ).fetchone()
            if existing is not None:
                if existing != (
                    record.channel,
                    record.semantics,
                    record.source_key,
                    record.frame_kind,
                    record.correlation_id,
                    record.payload,
                ):
                    raise _JournalConflict(
                        f"outbox message_id {record.message_id!r} has different content"
                    )
                capacity = _outbox_capacity(
                    connection,
                    self._config,
                    record.channel,
                    policy,
                    delta_items=0,
                    delta_bytes=0,
                )
                return _OutboxInsertResult(
                    _OutboxDisposition.DEDUPLICATED,
                    capacity,
                    None,
                    None,
                )
            replaced = _latest_outbox_slot(
                connection,
                record.channel,
                record.source_key,
            )
            delta_items = 1 if replaced is None else 0
            delta_bytes = size_bytes - (
                0 if replaced is None else replaced.size_bytes
            )
            current = _outbox_capacity(
                connection,
                self._config,
                record.channel,
                policy,
                delta_items=0,
                delta_bytes=0,
            )
            projected = _project_capacity(
                current,
                delta_items=delta_items,
                delta_bytes=delta_bytes,
            )
            crossed = _crossed_watermarks(current, projected)
            sweep = (
                self._compact_locked(
                    connection,
                    now,
                    limit=self._config.work_batch_size,
                )
                if _at_watermark(projected)
                else None
            )
            capacity = projected
            if sweep is not None and sweep.affected_items:
                replaced = _latest_outbox_slot(
                    connection,
                    record.channel,
                    record.source_key,
                )
                delta_items = 1 if replaced is None else 0
                delta_bytes = size_bytes - (
                    0 if replaced is None else replaced.size_bytes
                )
                capacity = _project_capacity(
                    _outbox_capacity(
                        connection,
                        self._config,
                        record.channel,
                        policy,
                        delta_items=0,
                        delta_bytes=0,
                    ),
                    delta_items=delta_items,
                    delta_bytes=delta_bytes,
                )
            _require_capacity(capacity)
            if replaced is not None:
                _execute_write(
                    connection,
                    "DELETE FROM outbox WHERE message_id = ?",
                    (replaced.message_id,),
                )
            _execute_write(
                connection,
                """
                INSERT INTO outbox (
                    message_id, channel, semantics, source_key, frame_kind,
                    correlation_id, payload, created_at, expires_at, attempts,
                    max_attempts, next_attempt_at, size_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                """,
                (
                    record.message_id,
                    record.channel,
                    record.semantics,
                    record.source_key,
                    record.frame_kind,
                    record.correlation_id,
                    record.payload,
                    created_at,
                    expires_at,
                    record.max_attempts,
                    created_at,
                    size_bytes,
                ),
            )
            return _OutboxInsertResult(
                (
                    _OutboxDisposition.INSERTED
                    if replaced is None
                    else _OutboxDisposition.COALESCED
                ),
                capacity,
                _WatermarkCrossing(projected, crossed) if crossed else None,
                sweep,
                replaced,
            )

    def outbox_replay_batch(
        self,
        cursor: _ReplayCursor | None,
        *,
        limit: int,
    ) -> tuple[_OutboxReplayRecord, ...]:
        with self._lock:
            self._require_open()
            return _outbox_replay_batch(
                self._connection,
                cursor,
                limit=limit,
            )

    def due_outbox(
        self,
        now: float,
        *,
        limit: int,
    ) -> tuple[_OutboxRecord, ...]:
        with self._lock:
            self._require_open()
            return _due_outbox(self._connection, now, limit=limit)

    def schedule_outbox_retry(
        self,
        message_id: str,
        *,
        next_attempt_at: float,
    ) -> _LifecycleRecord | None:
        with self._transaction() as connection:
            record = _outbox_lifecycle(connection, message_id)
            if record is None:
                return None
            _execute_write(
                connection,
                """
                UPDATE outbox SET next_attempt_at = ?
                WHERE message_id = ?
                """,
                (next_attempt_at, message_id),
            )
            return record

    def schedule_outbox_nack(
        self,
        message_id: str,
        *,
        now: float,
    ) -> _LifecycleRecord | None:
        with self._transaction() as connection:
            record = _outbox_lifecycle(connection, message_id)
            if record is None:
                return None
            delay = _bounded_retry_delay(
                self._config,
                max(1, record.attempts),
            )
            _execute_write(
                connection,
                """
                UPDATE outbox SET next_attempt_at = ?
                WHERE message_id = ?
                """,
                (now + delay, message_id),
            )
            return record

    def mark_outbox_sent(
        self,
        message_id: str,
        *,
        next_attempt_at: float,
    ) -> bool:
        with self._transaction() as connection:
            cursor = _execute_write(
                connection,
                """
                UPDATE outbox
                SET attempts = attempts + 1, next_attempt_at = ?
                WHERE message_id = ? AND attempts < max_attempts
                """,
                (next_attempt_at, message_id),
            )
            return cursor.rowcount > 0

    def delete_outbox(self, message_id: str) -> _LifecycleRecord | None:
        with self._transaction() as connection:
            record = _outbox_lifecycle(connection, message_id)
            if record is None:
                return None
            _execute_write(
                connection,
                "DELETE FROM outbox WHERE message_id = ?",
                (message_id,),
            )
            return record

    def record_inbox(
        self,
        record: _InboxRecord,
        *,
        created_at: float,
        expires_at: float,
        now: float,
        policy: TopicDeliveryPolicy,
    ) -> _InboxInsertResult:
        size_bytes = _inbox_logical_bytes(record)
        with self._transaction() as connection:
            existing = connection.execute(
                """
                SELECT frame_kind, channel, correlation_id, payload, status,
                       delivery_attempt, outcome_reason
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
                status = str(existing[4])
                if status == "pending" and record.delivery_attempt > int(existing[5]):
                    _execute_write(
                        connection,
                        """
                        UPDATE inbox SET delivery_attempt = ?
                        WHERE message_id = ?
                        """,
                        (record.delivery_attempt, record.message_id),
                    )
                disposition = {
                    "pending": _InboxDisposition.PENDING_DUPLICATE,
                    "acked": _InboxDisposition.ACKED_DUPLICATE,
                    "terminal": _InboxDisposition.TERMINAL_DUPLICATE,
                    "expired": _InboxDisposition.EXPIRED_DUPLICATE,
                }[status]
                capacity = _inbox_capacity(
                    connection,
                    self._config,
                    record.channel,
                    policy,
                    delta_items=0,
                    delta_bytes=0,
                )
                return _InboxInsertResult(disposition, capacity, None, None)
            current = _inbox_capacity(
                connection,
                self._config,
                record.channel,
                policy,
                delta_items=0,
                delta_bytes=0,
            )
            projected = _project_capacity(
                current,
                delta_items=1,
                delta_bytes=size_bytes,
            )
            crossed = _crossed_watermarks(current, projected)
            sweep = (
                self._compact_locked(
                    connection,
                    now,
                    limit=self._config.work_batch_size,
                )
                if _at_watermark(projected)
                else None
            )
            capacity = projected
            if sweep is not None and sweep.affected_items:
                capacity = _project_capacity(
                    _inbox_capacity(
                        connection,
                        self._config,
                        record.channel,
                        policy,
                        delta_items=0,
                        delta_bytes=0,
                    ),
                    delta_items=1,
                    delta_bytes=size_bytes,
                )
            _require_capacity(capacity)
            _execute_write(
                connection,
                """
                INSERT INTO inbox (
                    message_id, frame_kind, channel, correlation_id, payload,
                    delivery_attempt, status, created_at, expires_at,
                    ack_attempts, next_ack_at, ack_confirmed, outcome_reason,
                    rejection_only, size_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, 0, ?, 0, NULL, 0, ?)
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
            return _InboxInsertResult(
                _InboxDisposition.NEW,
                capacity,
                _WatermarkCrossing(projected, crossed) if crossed else None,
                sweep,
            )

    def pending_inbox_batch(
        self,
        now: float,
        cursor: _ReplayCursor | None,
        *,
        limit: int,
    ) -> tuple[tuple[_InboxRecord, _ReplayCursor], ...]:
        with self._lock:
            self._require_open()
            return _pending_inbox_batch(
                self._connection,
                now,
                cursor,
                limit=limit,
            )

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

    def mark_inbox_outcome(
        self,
        message_id: str,
        *,
        status: str,
        reason: str | None,
        now: float,
        retention_seconds: float,
    ) -> bool:
        if status not in {"acked", "terminal", "expired"}:
            raise ValueError(f"unsupported durable inbox outcome {status!r}")
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT message_id, frame_kind, channel, correlation_id, payload,
                       delivery_attempt, ack_attempts
                FROM inbox WHERE message_id = ? AND status = 'pending'
                """,
                (message_id,),
            ).fetchone()
            if row is None:
                return False
            record = _InboxRecord(
                str(row[0]),
                int(row[1]),
                str(row[2]),
                None if row[3] is None else str(row[3]),
                bytes(row[4]),
                int(row[5]),
                0,
                status,
                reason,
            )
            size_bytes = _inbox_logical_bytes(record)
            delta = size_bytes - int(
                connection.execute(
                    "SELECT size_bytes FROM inbox WHERE message_id = ?",
                    (message_id,),
                ).fetchone()[0]
            )
            if _logical_bytes(connection) + delta > self._config.max_storage_bytes:
                raise _JournalFull(
                    "durable outcome would exceed max_storage_bytes"
                )
            _execute_write(
                connection,
                """
                UPDATE inbox
                SET status = ?, outcome_reason = ?, expires_at = ?,
                    ack_attempts = 0, next_ack_at = ?, ack_confirmed = 0,
                    size_bytes = ?
                WHERE message_id = ? AND status = 'pending'
                """,
                (
                    status,
                    reason,
                    now + retention_seconds,
                    now,
                    size_bytes,
                    message_id,
                ),
            )
            return True

    def delete_retryable_inbox(self, message_id: str) -> bool:
        with self._transaction() as connection:
            cursor = _execute_write(
                connection,
                "DELETE FROM inbox WHERE message_id = ? AND status = 'pending'",
                (message_id,),
            )
            return cursor.rowcount > 0

    def record_terminal_rejection(
        self,
        record: _InboxRecord,
        *,
        reason: str,
        now: float,
    ) -> _TerminalRejectionResult:
        with self._transaction() as connection:
            try:
                return _record_terminal_rejection_with_capacity(
                    connection,
                    self._config,
                    record,
                    reason=reason,
                    now=now,
                    compact=lambda sweep_now: self._compact_locked(
                        connection,
                        sweep_now,
                        limit=self._config.work_batch_size,
                    ),
                )
            except _OutcomeStorageFull as error:
                raise _JournalFull(str(error)) from error

    def due_responses(
        self,
        now: float,
        *,
        limit: int,
        max_attempts: int,
    ) -> tuple[_InboxRecord, ...]:
        with self._lock:
            self._require_open()
            return _due_responses(
                self._connection,
                now,
                limit=limit,
                max_attempts=max_attempts,
            )

    def mark_response_attempt(
        self,
        message_id: str,
        *,
        next_attempt_at: float,
        max_attempts: int,
    ) -> bool:
        with self._transaction() as connection:
            return _mark_response_attempt(
                connection,
                _execute_write,
                message_id,
                next_attempt_at=next_attempt_at,
                max_attempts=max_attempts,
            )

    def schedule_response_now(
        self,
        message_id: str,
        now: float,
        *,
        max_attempts: int,
    ) -> bool:
        with self._transaction() as connection:
            return _schedule_response(
                connection,
                _execute_write,
                message_id,
                next_attempt_at=now,
                max_attempts=max_attempts,
                only_earlier=True,
            )

    def delay_response(
        self,
        message_id: str,
        *,
        next_attempt_at: float,
        max_attempts: int,
    ) -> bool:
        with self._transaction() as connection:
            return _schedule_response(
                connection,
                _execute_write,
                message_id,
                next_attempt_at=next_attempt_at,
                max_attempts=max_attempts,
                only_earlier=False,
            )

    def confirm_response(self, message_id: str) -> None:
        with self._transaction() as connection:
            _confirm_response(
                connection,
                _execute_write,
                message_id,
            )

    def compact(
        self,
        now: float,
        *,
        limit: int,
    ) -> _CompactionResult:
        with self._transaction() as connection:
            return self._compact_locked(connection, now, limit=limit)

    def compact_outbox(
        self,
        now: float,
        *,
        limit: int,
    ) -> _CompactionResult:
        with self._transaction() as connection:
            return _compact_outbox(connection, now, limit=limit)

    def stats(self) -> _JournalStats:
        with self._lock:
            self._require_open()
            return _journal_stats(self._connection)

    def topic_stats(self, channel: str) -> _TopicStats:
        with self._lock:
            self._require_open()
            return _topic_stats(self._connection, channel)

    def _close_after_rollback_failure(self) -> None:
        self._release_resources("close poisoned delivery journal")

    def _release_resources(self, operation: str) -> None:
        connection_error: BaseException | None = None
        if not self._connection_closed:
            try:
                self._connection.close()
            except BaseException as error:
                connection_error = error
            else:
                self._connection_closed = True
        owner_error: BaseException | None = None
        if self._connection_closed and self._owner_lock is not None:
            try:
                self._owner_lock.close()
            except BaseException as error:
                owner_error = error
            if self._owner_lock.is_released():
                self._owner_lock = None
        self._closed = self._connection_closed and self._owner_lock is None
        if error := connection_error or owner_error:
            raise _JournalError(
                f"could not {operation}: "
                f"{type(error).__name__}: {error}"
            ) from error
        if not self._closed:
            raise _JournalError(
                "delivery journal ownership remains live after close"
            )

    def _compact_locked(
        self,
        connection: sqlite3.Connection,
        now: float,
        *,
        limit: int,
    ) -> _CompactionResult:
        outbox = _compact_outbox(connection, now, limit=limit)
        expired_inbox = _expire_pending_inbox(
            connection,
            now,
            limit=limit,
            retention_seconds=self._config.dedupe_retention_seconds,
            write=_execute_write,
        )
        released_outcomes = _select_inbox_lifecycle(
            connection,
            "status != 'pending' AND expires_at <= ?",
            (now,),
            limit,
        )
        _delete_lifecycle(connection, "inbox", released_outcomes)
        connection.execute("PRAGMA incremental_vacuum(16)")
        return _CompactionResult(
            outbox.expired_outbox,
            expired_inbox,
            outbox.retry_exhausted,
            released_outcomes,
        )

    def _require_open(self) -> None:
        if self._closed:
            raise _JournalError("delivery journal is closed")
        if self._owner_lock is None:
            raise _JournalError("delivery journal ownership is unavailable")
        try:
            self._owner_lock.require_alive()
        except _JournalLockError as error:
            raise _JournalError(
                f"delivery journal ownership was lost: {error}"
            ) from error

    def _transaction(self) -> _JournalTransaction:
        return _JournalTransaction(self)

    def _release_after_failed_open(
        self,
        initiating_error: BaseException,
    ) -> None:
        connection = getattr(self, "_connection", None)
        if connection is not None:
            try:
                connection.close()
            except BaseException as cleanup_error:
                initiating_error.add_note(
                    "delivery journal connection cleanup also failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )
        if self._owner_lock is not None:
            try:
                self._owner_lock.close()
            except BaseException as cleanup_error:
                initiating_error.add_note(
                    "delivery journal ownership cleanup also failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )


@final
class _JournalTransaction:
    def __init__(self, journal: _DeliveryJournal) -> None:
        self._journal = journal

    def __enter__(self) -> sqlite3.Connection:
        self._journal._lock.acquire()
        try:
            self._journal._require_open()
            self._journal._connection.execute("BEGIN IMMEDIATE")
        except sqlite3.DatabaseError as error:
            self._journal._lock.release()
            raise _translate_sqlite_error(
                error,
                operation="begin a transaction in",
            ) from error
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
            if error_type is not None:
                self._rollback(error)
                return
            try:
                self._journal._require_open()
                self._journal._connection.execute("COMMIT")
            except _JournalError as ownership_error:
                self._rollback(ownership_error)
                raise
            except sqlite3.DatabaseError as commit_error:
                self._rollback(commit_error)
                raise _translate_sqlite_error(
                    commit_error,
                    operation="commit a transaction in",
                ) from commit_error
        finally:
            self._journal._lock.release()

    def _rollback(self, original_error: BaseException | None) -> None:
        if not self._journal._connection.in_transaction:
            return
        try:
            self._journal._connection.execute("ROLLBACK")
        except sqlite3.DatabaseError as rollback_error:
            if original_error is not None:
                original_error.add_note(
                    "delivery journal rollback also failed: "
                    f"{rollback_error}"
                )
            try:
                self._journal._close_after_rollback_failure()
            except _JournalError as close_error:
                if original_error is not None:
                    original_error.add_note(str(close_error))
