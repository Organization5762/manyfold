"""Bounded lifecycle selection for an owner-supplied SQLite connection."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable

from ._transport_delivery_records import (
    _CompactionResult,
    _inbox_logical_bytes,
    _InboxRecord,
    _LifecycleRecord,
)

_JournalWrite = Callable[
    [sqlite3.Connection, str, tuple[object, ...]],
    sqlite3.Cursor,
]


def _latest_outbox_slot(
    connection: sqlite3.Connection,
    channel: str,
    source_key: str | None,
) -> _LifecycleRecord | None:
    if source_key is None:
        return None
    row = connection.execute(
        """
        SELECT message_id, channel, source_key, correlation_id, attempts,
               size_bytes
        FROM outbox
        WHERE channel = ? AND source_key = ? AND semantics = 'latest'
        """,
        (channel, source_key),
    ).fetchone()
    return None if row is None else _LifecycleRecord(*row)


def _outbox_lifecycle(
    connection: sqlite3.Connection,
    message_id: str,
) -> _LifecycleRecord | None:
    row = connection.execute(
        """
        SELECT message_id, channel, source_key, correlation_id, attempts,
               size_bytes
        FROM outbox WHERE message_id = ?
        """,
        (message_id,),
    ).fetchone()
    return None if row is None else _LifecycleRecord(*row)


def _select_outbox_lifecycle(
    connection: sqlite3.Connection,
    where: str,
    parameters: tuple[object, ...],
    limit: int,
) -> tuple[_LifecycleRecord, ...]:
    rows = connection.execute(
        f"""
        SELECT message_id, channel, source_key, correlation_id, attempts,
               size_bytes
        FROM outbox WHERE {where}
        ORDER BY expires_at, message_id
        LIMIT ?
        """,
        (*parameters, limit),
    ).fetchall()
    return tuple(_LifecycleRecord(*row) for row in rows)


def _select_inbox_lifecycle(
    connection: sqlite3.Connection,
    where: str,
    parameters: tuple[object, ...],
    limit: int,
) -> tuple[_LifecycleRecord, ...]:
    rows = connection.execute(
        f"""
        SELECT message_id, channel, NULL, correlation_id, delivery_attempt,
               size_bytes
        FROM inbox WHERE {where}
        ORDER BY expires_at, message_id
        LIMIT ?
        """,
        (*parameters, limit),
    ).fetchall()
    return tuple(_LifecycleRecord(*row) for row in rows)


def _delete_lifecycle(
    connection: sqlite3.Connection,
    table: str,
    records: tuple[_LifecycleRecord, ...],
) -> None:
    if records:
        connection.executemany(
            f"DELETE FROM {table} WHERE message_id = ?",
            ((record.message_id,) for record in records),
        )


def _compact_outbox(
    connection: sqlite3.Connection,
    now: float,
    *,
    limit: int,
) -> _CompactionResult:
    expired = _select_outbox_lifecycle(
        connection,
        "expires_at <= ?",
        (now,),
        limit,
    )
    exhausted = _select_outbox_lifecycle(
        connection,
        """
        expires_at > ? AND attempts >= max_attempts
        AND next_attempt_at <= ?
        """,
        (now, now),
        limit,
    )
    _delete_lifecycle(connection, "outbox", expired)
    _delete_lifecycle(connection, "outbox", exhausted)
    return _CompactionResult(
        expired_outbox=expired,
        retry_exhausted=exhausted,
    )


def _expire_pending_inbox(
    connection: sqlite3.Connection,
    now: float,
    *,
    limit: int,
    retention_seconds: float,
    write: _JournalWrite,
) -> tuple[_LifecycleRecord, ...]:
    rows = connection.execute(
        """
        SELECT message_id, frame_kind, channel, correlation_id, payload,
               delivery_attempt, ack_attempts, size_bytes
        FROM inbox
        WHERE status = 'pending' AND expires_at <= ?
        ORDER BY expires_at, message_id
        LIMIT ?
        """,
        (now, limit),
    ).fetchall()
    expired: list[_LifecycleRecord] = []
    for row in rows:
        record = _InboxRecord(
            str(row[0]),
            int(row[1]),
            str(row[2]),
            None if row[3] is None else str(row[3]),
            bytes(row[4]),
            int(row[5]),
            0,
            "expired",
            "receiver inbox expired",
        )
        size_bytes = _inbox_logical_bytes(record)
        write(
            connection,
            """
            UPDATE inbox
            SET status = 'expired', outcome_reason = ?,
                expires_at = ?, ack_attempts = 0, next_ack_at = ?,
                ack_confirmed = 0, size_bytes = ?
            WHERE message_id = ? AND status = 'pending'
            """,
            (
                record.outcome_reason,
                now + retention_seconds,
                now,
                size_bytes,
                record.message_id,
            ),
        )
        expired.append(
            _LifecycleRecord(
                record.message_id,
                record.channel,
                None,
                record.correlation_id,
                record.delivery_attempt,
                int(row[7]),
            )
        )
    return tuple(expired)
