"""Bounded semantic and capacity preflight for legacy delivery journals."""

from __future__ import annotations

import sqlite3

from ._transport_delivery_cleanup import (
    _compact_outbox,
    _delete_lifecycle,
    _expire_pending_inbox,
    _select_inbox_lifecycle,
)


def _preflight_v1(
    connection: sqlite3.Connection,
    *,
    recovery_batch_size: int,
) -> tuple[int, int]:
    _require_canonical_v1_text(
        connection,
        recovery_batch_size=recovery_batch_size,
    )
    invalid_outbox = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM outbox
            WHERE typeof(message_id) != 'text'
               OR length(trim(message_id)) = 0
               OR length(CAST(message_id AS BLOB)) > 128
               OR typeof(channel) != 'text'
               OR length(trim(channel)) = 0
               OR length(CAST(channel AS BLOB)) > 65535
               OR typeof(frame_kind) != 'integer'
               OR frame_kind NOT IN (1, 2, 3, 4)
               OR (
                    correlation_id IS NOT NULL
                    AND (
                        typeof(correlation_id) != 'text'
                        OR length(trim(correlation_id)) = 0
                        OR length(CAST(correlation_id AS BLOB)) > 65535
                    )
               )
               OR (frame_kind != 1 AND correlation_id IS NULL)
               OR typeof(payload) != 'blob'
               OR typeof(attempts) != 'integer'
               OR attempts < 0 OR attempts > 4294967295
               OR typeof(created_at) NOT IN ('integer', 'real')
               OR typeof(expires_at) NOT IN ('integer', 'real')
               OR typeof(next_attempt_at) NOT IN ('integer', 'real')
               OR created_at > expires_at
               OR abs(created_at) > 1.7976931348623157e308
               OR abs(expires_at) > 1.7976931348623157e308
               OR abs(next_attempt_at) > 1.7976931348623157e308
            """
        ).fetchone()[0]
    )
    invalid_inbox = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM inbox
            WHERE typeof(message_id) != 'text'
               OR length(trim(message_id)) = 0
               OR length(CAST(message_id AS BLOB)) > 128
               OR typeof(channel) != 'text'
               OR length(trim(channel)) = 0
               OR length(CAST(channel AS BLOB)) > 65535
               OR typeof(frame_kind) != 'integer'
               OR frame_kind NOT IN (1, 2, 3, 4)
               OR (
                    correlation_id IS NOT NULL
                    AND (
                        typeof(correlation_id) != 'text'
                        OR length(trim(correlation_id)) = 0
                        OR length(CAST(correlation_id AS BLOB)) > 65535
                    )
               )
               OR (frame_kind != 1 AND correlation_id IS NULL)
               OR typeof(payload) != 'blob'
               OR typeof(delivery_attempt) != 'integer'
               OR delivery_attempt < 1 OR delivery_attempt > 4294967295
               OR typeof(ack_attempts) != 'integer'
               OR ack_attempts < 0 OR ack_attempts > 4294967295
               OR status NOT IN ('pending', 'acked')
               OR (status = 'pending' AND ack_attempts != 0)
               OR typeof(ack_confirmed) != 'integer'
               OR ack_confirmed NOT IN (0, 1)
               OR typeof(created_at) NOT IN ('integer', 'real')
               OR typeof(expires_at) NOT IN ('integer', 'real')
               OR typeof(next_ack_at) NOT IN ('integer', 'real')
               OR created_at > expires_at
               OR abs(created_at) > 1.7976931348623157e308
               OR abs(expires_at) > 1.7976931348623157e308
               OR abs(next_ack_at) > 1.7976931348623157e308
            """
        ).fetchone()[0]
    )
    if invalid_outbox or invalid_inbox:
        raise sqlite3.DatabaseError(
            "legacy delivery journal contains invalid retained row metadata"
        )
    return (
        int(connection.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]),
        int(connection.execute("SELECT COUNT(*) FROM inbox").fetchone()[0]),
    )


def _compact_migrated_v1(
    connection: sqlite3.Connection,
    *,
    recovery_now: float,
    recovery_batch_size: int,
    retained_rows: int,
    dedupe_retention_seconds: float,
) -> None:
    """Apply bounded V2 lifecycle cleanup before committing the migration."""
    max_batches = (
        (retained_rows + recovery_batch_size - 1) // recovery_batch_size
    ) + 1
    for _ in range(max_batches):
        outbox = _compact_outbox(
            connection,
            recovery_now,
            limit=recovery_batch_size,
        )
        expired_inbox = _expire_pending_inbox(
            connection,
            recovery_now,
            limit=recovery_batch_size,
            retention_seconds=dedupe_retention_seconds,
            write=_execute_migration_write,
        )
        released_outcomes = _select_inbox_lifecycle(
            connection,
            "status != 'pending' AND expires_at <= ?",
            (recovery_now,),
            recovery_batch_size,
        )
        _delete_lifecycle(connection, "inbox", released_outcomes)
        affected = (
            len(outbox.expired_outbox)
            + len(outbox.retry_exhausted)
            + len(expired_inbox)
            + len(released_outcomes)
        )
        if affected == 0:
            return
    raise sqlite3.DatabaseError(
        "legacy delivery expiry cleanup exceeded retained-row bound"
    )


def _execute_migration_write(
    connection: sqlite3.Connection,
    statement: str,
    parameters: tuple[object, ...],
) -> sqlite3.Cursor:
    return connection.execute(statement, parameters)


def _require_canonical_v1_text(
    connection: sqlite3.Connection,
    *,
    recovery_batch_size: int,
) -> None:
    for table in ("outbox", "inbox"):
        cursor = connection.execute(
            "SELECT message_id, channel, correlation_id "
            f"FROM {table} ORDER BY rowid"
        )
        while True:
            rows = cursor.fetchmany(recovery_batch_size)
            if not rows:
                break
            for row in rows:
                for value in row:
                    if isinstance(value, str) and value != value.strip():
                        raise sqlite3.DatabaseError(
                            "legacy delivery journal contains "
                            "non-canonical retained text"
                        )
