"""Bounded read queries over an owner-supplied delivery connection."""

from __future__ import annotations

import sqlite3

from ._transport_delivery_records import (
    _InboxRecord,
    _OutboxRecord,
    _ReplayCursor,
)


def _due_outbox(
    connection: sqlite3.Connection,
    now: float,
    *,
    limit: int,
) -> tuple[_OutboxRecord, ...]:
    rows = connection.execute(
        """
        SELECT message_id, channel, semantics, source_key, frame_kind,
               correlation_id, payload, attempts, max_attempts
        FROM outbox
        WHERE expires_at > ? AND attempts < max_attempts
          AND next_attempt_at <= ?
        ORDER BY created_at, message_id
        LIMIT ?
        """,
        (now, now, limit),
    ).fetchall()
    return tuple(_OutboxRecord(*row) for row in rows)


def _pending_inbox_batch(
    connection: sqlite3.Connection,
    now: float,
    cursor: _ReplayCursor | None,
    *,
    limit: int,
) -> tuple[tuple[_InboxRecord, _ReplayCursor], ...]:
    created_at = -1.0 if cursor is None else cursor.created_at
    message_id = "" if cursor is None else cursor.message_id
    rows = connection.execute(
        """
        SELECT message_id, frame_kind, channel, correlation_id, payload,
               delivery_attempt, ack_attempts, created_at
        FROM inbox
        WHERE status = 'pending' AND expires_at > ?
          AND (created_at, message_id) > (?, ?)
        ORDER BY created_at, message_id
        LIMIT ?
        """,
        (now, created_at, message_id, limit),
    ).fetchall()
    return tuple(
        (
            _InboxRecord(
                str(row[0]),
                int(row[1]),
                str(row[2]),
                None if row[3] is None else str(row[3]),
                bytes(row[4]),
                int(row[5]),
                int(row[6]),
            ),
            _ReplayCursor(float(row[7]), str(row[0])),
        )
        for row in rows
    )


def _due_responses(
    connection: sqlite3.Connection,
    now: float,
    *,
    limit: int,
    max_attempts: int,
) -> tuple[_InboxRecord, ...]:
    rows = connection.execute(
        """
        SELECT message_id, frame_kind, channel, correlation_id, X'',
               delivery_attempt, ack_attempts, status, outcome_reason
        FROM inbox
        WHERE status != 'pending' AND ack_confirmed = 0
          AND expires_at > ? AND next_ack_at <= ?
          AND ack_attempts < ?
        ORDER BY next_ack_at, message_id
        LIMIT ?
        """,
        (now, now, max_attempts, limit),
    ).fetchall()
    return tuple(_InboxRecord(*row) for row in rows)
