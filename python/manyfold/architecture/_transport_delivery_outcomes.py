"""Durable terminal outcome persistence over an owner-supplied connection."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import final

from ._transport_delivery_capacity import (
    _at_peer_watermark,
    _crossed_peer_watermarks,
    _inbox_peer_capacity,
    _project_peer_capacity,
    _require_capacity,
)
from ._transport_delivery_events import DeliveryCapacity
from ._transport_delivery_policy import DeliveryConfig
from ._transport_delivery_records import (
    _CompactionResult,
    _inbox_logical_bytes,
    _InboxRecord,
    _WatermarkCrossing,
)

_Write = Callable[
    [sqlite3.Connection, str, tuple[object, ...]],
    sqlite3.Cursor,
]


def _mark_response_attempt(
    connection: sqlite3.Connection,
    write: _Write,
    message_id: str,
    *,
    next_attempt_at: float,
    max_attempts: int,
) -> bool:
    write(
        connection,
        """
        UPDATE inbox
        SET ack_attempts = ack_attempts + 1, next_ack_at = ?
        WHERE message_id = ? AND status != 'pending'
          AND ack_confirmed = 0 AND ack_attempts < ?
        """,
        (next_attempt_at, message_id, max_attempts),
    )
    row = connection.execute(
        "SELECT ack_attempts FROM inbox WHERE message_id = ?",
        (message_id,),
    ).fetchone()
    return row is not None and int(row[0]) >= max_attempts


def _schedule_response(
    connection: sqlite3.Connection,
    write: _Write,
    message_id: str,
    *,
    next_attempt_at: float,
    max_attempts: int,
    only_earlier: bool,
) -> bool:
    assignment = (
        "next_ack_at = MIN(next_ack_at, ?), ack_confirmed = 0"
        if only_earlier
        else "next_ack_at = ?"
    )
    confirmation_clause = "" if only_earlier else "AND ack_confirmed = 0"
    cursor = write(
        connection,
        f"""
        UPDATE inbox SET {assignment}
        WHERE message_id = ? AND status != 'pending'
          {confirmation_clause} AND ack_attempts < ?
        """,
        (next_attempt_at, message_id, max_attempts),
    )
    return cursor.rowcount > 0


def _confirm_response(
    connection: sqlite3.Connection,
    write: _Write,
    message_id: str,
) -> None:
    write(
        connection,
        """
        UPDATE inbox SET ack_confirmed = 1
        WHERE message_id = ? AND status != 'pending'
        """,
        (message_id,),
    )


def _terminal_rejection_delta(
    connection: sqlite3.Connection,
    record: _InboxRecord,
) -> tuple[int, int]:
    existing = connection.execute(
        "SELECT 1 FROM inbox WHERE message_id = ?",
        (record.message_id,),
    ).fetchone()
    if existing is not None:
        return 0, 0
    outcome = _terminal_rejection_record(record, "delivery rejected")
    return 1, _inbox_logical_bytes(outcome)


def _terminal_rejection_conflicts(
    connection: sqlite3.Connection,
    record: _InboxRecord,
) -> bool:
    existing = connection.execute(
        """
        SELECT frame_kind, channel, correlation_id, payload, rejection_only
        FROM inbox WHERE message_id = ?
        """,
        (record.message_id,),
    ).fetchone()
    if existing is None or bool(existing[4]):
        return False
    return existing[:4] != (
        record.frame_kind,
        record.channel,
        record.correlation_id,
        record.payload,
    )


def _record_terminal_rejection(
    connection: sqlite3.Connection,
    config: DeliveryConfig,
    record: _InboxRecord,
    *,
    reason: str,
    now: float,
) -> _TerminalRejectionDisposition:
    existing = connection.execute(
        """
        SELECT status, outcome_reason FROM inbox WHERE message_id = ?
        """,
        (record.message_id,),
    ).fetchone()
    try:
        if existing is not None:
            disposition = (
                _TerminalRejectionDisposition.DUPLICATE
                if str(existing[0]) == "terminal"
                and str(existing[1]) == reason
                else _TerminalRejectionDisposition.TRANSITIONED
            )
            connection.execute(
                """
                UPDATE inbox
                SET status = 'terminal', outcome_reason = ?,
                    expires_at = CASE
                        WHEN ? THEN expires_at ELSE ?
                    END,
                    next_ack_at = MIN(next_ack_at, ?),
                    ack_confirmed = CASE
                        WHEN ack_attempts < ? THEN 0 ELSE ack_confirmed
                    END
                WHERE message_id = ?
                """,
                (
                    reason,
                    disposition is _TerminalRejectionDisposition.DUPLICATE,
                    now + config.dedupe_retention_seconds,
                    now,
                    config.max_ack_attempts,
                    record.message_id,
                ),
            )
            return disposition
        outcome = _terminal_rejection_record(record, reason)
        size_bytes = _inbox_logical_bytes(outcome)
        connection.execute(
            """
            INSERT INTO inbox (
                message_id, frame_kind, channel, correlation_id, payload,
                delivery_attempt, status, created_at, expires_at,
                ack_attempts, next_ack_at, ack_confirmed, outcome_reason,
                rejection_only, size_bytes
            ) VALUES (?, ?, ?, ?, ?, ?, 'terminal', ?, ?, 0, ?, 0, ?, 1, ?)
            """,
            (
                outcome.message_id,
                outcome.frame_kind,
                outcome.channel,
                outcome.correlation_id,
                outcome.payload,
                outcome.delivery_attempt,
                now,
                now + config.dedupe_retention_seconds,
                now,
                reason,
                size_bytes,
            ),
        )
        return _TerminalRejectionDisposition.NEW
    except sqlite3.OperationalError as error:
        if "database or disk is full" in str(error).lower():
            raise _OutcomeStorageFull(
                "delivery journal reached its SQLite page limit"
            ) from error
        raise


def _record_terminal_rejection_with_capacity(
    connection: sqlite3.Connection,
    config: DeliveryConfig,
    record: _InboxRecord,
    *,
    reason: str,
    now: float,
    compact: Callable[[float], _CompactionResult],
) -> _TerminalRejectionResult:
    if _terminal_rejection_conflicts(connection, record):
        return _TerminalRejectionResult(
            _TerminalRejectionDisposition.CONFLICT,
            _inbox_peer_capacity(
                connection,
                config,
                delta_items=0,
                delta_bytes=0,
            ),
            None,
            None,
        )
    delta_items, delta_bytes = _terminal_rejection_delta(connection, record)
    current = _inbox_peer_capacity(
        connection,
        config,
        delta_items=0,
        delta_bytes=0,
    )
    projected = _project_peer_capacity(
        current,
        delta_items=delta_items,
        delta_bytes=delta_bytes,
    )
    crossed = _crossed_peer_watermarks(current, projected)
    sweep = compact(now) if _at_peer_watermark(projected) else None
    capacity = projected
    if sweep is not None and sweep.affected_items:
        delta_items, delta_bytes = _terminal_rejection_delta(
            connection,
            record,
        )
        capacity = _project_peer_capacity(
            _inbox_peer_capacity(
                connection,
                config,
                delta_items=0,
                delta_bytes=0,
            ),
            delta_items=delta_items,
            delta_bytes=delta_bytes,
        )
    _require_capacity(capacity)
    disposition = _record_terminal_rejection(
        connection,
        config,
        record,
        reason=reason,
        now=now,
    )
    return _TerminalRejectionResult(
        disposition,
        capacity,
        _WatermarkCrossing(projected, crossed) if crossed else None,
        sweep,
    )


def _terminal_rejection_record(
    record: _InboxRecord,
    reason: str,
) -> _InboxRecord:
    return _InboxRecord(
        record.message_id,
        record.frame_kind,
        record.channel,
        record.correlation_id,
        b"",
        record.delivery_attempt,
        status="terminal",
        outcome_reason=reason,
    )


@final
class _OutcomeStorageFull(RuntimeError):
    pass


@final
class _TerminalRejectionDisposition(str, Enum):
    NEW = "new"
    TRANSITIONED = "transitioned"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


@final
@dataclass(frozen=True, slots=True)
class _TerminalRejectionResult:
    disposition: _TerminalRejectionDisposition
    capacity: DeliveryCapacity
    crossing: _WatermarkCrossing | None
    sweep: _CompactionResult | None
