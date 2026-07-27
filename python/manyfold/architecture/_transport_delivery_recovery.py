"""Bounded recovery validation over an owner-supplied SQLite connection."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from math import isfinite
from typing import final

from ._transport_delivery_capacity import _capacity_error
from ._transport_delivery_events import (
    DeliveryCapacity,
    DeliveryCapacityDimension,
)
from ._transport_delivery_policy import DeliveryConfig, TopicDeliveryPolicy
from ._transport_delivery_protocol import (
    _DELIVERY_HEADER_SIZE,
    _MAX_DELIVERY_ATTEMPT,
    _MAX_MESSAGE_ID_BYTES,
    _MAX_OUTCOME_REASON_BYTES,
    _MAX_TEXT_BYTES,
)
from ._transport_delivery_records import (
    _JournalStats,
    _OutboxReplayRecord,
    _ReplayCursor,
)
from .transport import FrameKind


def _validate_recovery(
    connection: sqlite3.Connection,
    config: DeliveryConfig,
    policies: Mapping[str, TopicDeliveryPolicy],
    stats: _JournalStats,
    *,
    max_transport_payload_bytes: int,
    recovery_now: float,
    enforce_bounds: bool,
) -> tuple[_RecoveredWatermark, ...]:
    if enforce_bounds and stats.outbox_items > config.max_outbox_items:
        raise _RecoveryCapacityViolation(
            "recovered outbox exceeds max_outbox_items"
        )
    if enforce_bounds and (
        stats.retained_inbox_items > config.max_inbox_items
    ):
        raise _RecoveryCapacityViolation(
            "recovered inbox exceeds max_inbox_items"
        )
    if enforce_bounds and stats.logical_bytes > config.max_storage_bytes:
        raise _RecoveryCapacityViolation(
            "recovered journal exceeds max_storage_bytes"
        )
    peer = _peer_watermarks(config, stats) if enforce_bounds else ()
    outbox = _scan_rows(
        connection.execute(
            """
            SELECT channel, semantics, source_key, size_bytes, attempts,
                   max_attempts, created_at, expires_at, next_attempt_at,
                   frame_kind, length(payload), typeof(payload),
                   length(CAST(message_id AS BLOB)),
                   length(CAST(channel AS BLOB)),
                   COALESCE(length(CAST(correlation_id AS BLOB)), 0),
                   correlation_id IS NULL,
                   length(trim(message_id)),
                   length(trim(channel)),
                   CASE WHEN source_key IS NULL
                        THEN NULL ELSE length(trim(source_key)) END,
                   CASE WHEN correlation_id IS NULL
                        THEN NULL ELSE length(trim(correlation_id)) END,
                   typeof(message_id), typeof(channel), typeof(semantics),
                   typeof(source_key), typeof(correlation_id),
                   160
                   + length(CAST(message_id AS BLOB))
                   + length(CAST(channel AS BLOB))
                   + COALESCE(length(CAST(source_key AS BLOB)), 0)
                   + COALESCE(length(CAST(correlation_id AS BLOB)), 0)
                   + length(payload),
                   message_id, channel, source_key, correlation_id,
                   typeof(size_bytes), typeof(frame_kind), typeof(attempts),
                   typeof(max_attempts)
            FROM outbox INDEXED BY outbox_recovery
            ORDER BY channel, created_at, message_id
            """
        ),
        config,
        policies,
        stats,
        side=_RecoveredStoreSide.OUTBOX,
        max_transport_payload_bytes=max_transport_payload_bytes,
        recovery_now=recovery_now,
        enforce_bounds=enforce_bounds,
    )
    inbox = _scan_rows(
        connection.execute(
            """
            SELECT channel, NULL, rejection_only, size_bytes,
                   delivery_attempt, ack_attempts, created_at, expires_at,
                   next_ack_at, status, frame_kind, length(payload),
                   typeof(payload),
                   length(CAST(message_id AS BLOB)),
                   length(CAST(channel AS BLOB)),
                   COALESCE(length(CAST(correlation_id AS BLOB)), 0),
                   correlation_id IS NULL,
                   length(CAST(outcome_reason AS BLOB)),
                   ack_confirmed,
                   length(trim(message_id)),
                   length(trim(channel)),
                   CASE WHEN correlation_id IS NULL
                        THEN NULL ELSE length(trim(correlation_id)) END,
                   CASE WHEN outcome_reason IS NULL
                        THEN NULL ELSE length(trim(outcome_reason)) END,
                   typeof(message_id), typeof(channel),
                   typeof(correlation_id), typeof(outcome_reason),
                   128
                   + length(CAST(message_id AS BLOB))
                   + length(CAST(channel AS BLOB))
                   + COALESCE(length(CAST(correlation_id AS BLOB)), 0)
                   + length(payload),
                   message_id, channel, correlation_id, outcome_reason,
                   typeof(size_bytes), typeof(frame_kind),
                   typeof(delivery_attempt), typeof(ack_attempts),
                   typeof(ack_confirmed), typeof(rejection_only)
            FROM inbox INDEXED BY inbox_recovery
            ORDER BY channel, created_at, message_id
            """
        ),
        config,
        policies,
        stats,
        side=_RecoveredStoreSide.INBOX,
        max_transport_payload_bytes=max_transport_payload_bytes,
        recovery_now=recovery_now,
        enforce_bounds=enforce_bounds,
    )
    return peer + outbox + inbox


def _peer_watermarks(
    config: DeliveryConfig,
    stats: _JournalStats,
) -> tuple[_RecoveredWatermark, ...]:
    recovered: list[_RecoveredWatermark] = []
    for side, items, limit in (
        (
            _RecoveredStoreSide.OUTBOX,
            stats.outbox_items,
            config.max_outbox_items,
        ),
        (
            _RecoveredStoreSide.INBOX,
            stats.retained_inbox_items,
            config.max_inbox_items,
        ),
    ):
        capacity = DeliveryCapacity(
            items,
            limit,
            stats.logical_bytes,
            config.max_storage_bytes,
            0,
            limit,
            0,
            config.max_storage_bytes,
            config.soft_limit_ratio,
            config.soft_limit_ratio,
        )
        if items >= limit * config.soft_limit_ratio:
            recovered.append(
                _RecoveredWatermark(
                    None,
                    side,
                    DeliveryCapacityDimension.PEER_ITEMS,
                    capacity,
                )
            )
    if (
        stats.logical_bytes
        >= config.max_storage_bytes * config.soft_limit_ratio
    ):
        recovered.append(
            _RecoveredWatermark(
                None,
                None,
                DeliveryCapacityDimension.PEER_LOGICAL_BYTES,
                DeliveryCapacity(
                    stats.outbox_items + stats.retained_inbox_items,
                    config.max_outbox_items + config.max_inbox_items,
                    stats.logical_bytes,
                    config.max_storage_bytes,
                    0,
                    1,
                    0,
                    1,
                    config.soft_limit_ratio,
                    config.soft_limit_ratio,
                ),
            )
        )
    return tuple(recovered)


def _outbox_replay_batch(
    connection: sqlite3.Connection,
    cursor: _ReplayCursor | None,
    *,
    limit: int,
) -> tuple[_OutboxReplayRecord, ...]:
    created_at = -1.0 if cursor is None else cursor.created_at
    message_id = "" if cursor is None else cursor.message_id
    rows = connection.execute(
        """
        SELECT message_id, channel, source_key, correlation_id,
               attempts, created_at
        FROM outbox INDEXED BY outbox_replay
        WHERE (created_at, message_id) > (?, ?)
        ORDER BY created_at, message_id
        LIMIT ?
        """,
        (created_at, message_id, limit),
    ).fetchall()
    return tuple(
        _OutboxReplayRecord(
            str(row[0]),
            str(row[1]),
            None if row[2] is None else str(row[2]),
            None if row[3] is None else str(row[3]),
            int(row[4]),
            _ReplayCursor(float(row[5]), str(row[0])),
        )
        for row in rows
    )


def _scan_rows(
    cursor: sqlite3.Cursor,
    config: DeliveryConfig,
    policies: Mapping[str, TopicDeliveryPolicy],
    stats: _JournalStats,
    *,
    side: _RecoveredStoreSide,
    max_transport_payload_bytes: int,
    recovery_now: float,
    enforce_bounds: bool,
) -> tuple[_RecoveredWatermark, ...]:
    watermarks: list[_RecoveredWatermark] = []
    channel: str | None = None
    items = 0
    logical_bytes = 0
    while True:
        batch = cursor.fetchmany(config.recovery_batch_size)
        if not batch:
            break
        for row in batch:
            raw_channel, semantics, source_key, size_bytes = row[:4]
            if (
                not isinstance(raw_channel, str)
                or not raw_channel.strip()
                or raw_channel != raw_channel.strip()
            ):
                raise _RecoveryViolation(
                    "recovered row has an invalid channel"
                )
            next_channel = raw_channel
            policy = policies.get(next_channel)
            if side is _RecoveredStoreSide.OUTBOX:
                _validate_outbox_row(
                    next_channel,
                    _recovery_int(size_bytes, "size_bytes"),
                    row[4:],
                    policy,
                    config,
                    max_transport_payload_bytes,
                    enforce_bounds=enforce_bounds,
                )
            else:
                rejection_flag = _recovery_int(
                    source_key,
                    "rejection_only",
                )
                if rejection_flag not in (0, 1):
                    raise _RecoveryViolation(
                        f"recovered topic {next_channel!r} has invalid "
                        "rejection_only"
                    )
                _validate_inbox_row(
                    next_channel,
                    _recovery_int(size_bytes, "size_bytes"),
                    row[4:],
                    policy,
                    config,
                    rejection_only=bool(rejection_flag),
                    max_transport_payload_bytes=max_transport_payload_bytes,
                    recovery_now=recovery_now,
                    enforce_bounds=enforce_bounds,
                )
                if rejection_flag:
                    continue
            if (
                enforce_bounds
                and channel is not None
                and next_channel != channel
            ):
                _finish_topic(
                    watermarks,
                    config,
                    policies[channel],
                    stats,
                    items,
                    logical_bytes,
                    side=side,
                )
                items = 0
                logical_bytes = 0
            channel = next_channel
            if policy is None and enforce_bounds:
                raise _RecoveryCapacityViolation(
                    f"recovered topic {channel!r} has no explicit policy"
                )
            if side is _RecoveredStoreSide.OUTBOX:
                resolved_semantics = str(semantics)
                if resolved_semantics not in {"append", "latest"}:
                    raise _RecoveryViolation(
                        f"recovered topic {channel!r} semantics conflict"
                    )
                if (resolved_semantics == "latest") != bool(source_key):
                    raise _RecoveryViolation(
                        f"recovered topic {channel!r} has invalid source identity"
                    )
                if (
                    policy is not None
                    and enforce_bounds
                    and resolved_semantics != policy.semantics.value
                ):
                    raise _RecoveryCapacityViolation(
                        f"recovered topic {channel!r} semantics conflict "
                        "with current policy"
                    )
            items += 1
            logical_bytes += _recovery_int(size_bytes, "size_bytes")
    if enforce_bounds and channel is not None:
        _finish_topic(
            watermarks,
            config,
            policies[channel],
            stats,
            items,
            logical_bytes,
            side=side,
        )
    return tuple(watermarks)


def _validate_outbox_row(
    channel: str,
    stored_size: int,
    values: tuple[object, ...],
    policy: TopicDeliveryPolicy | None,
    config: DeliveryConfig,
    max_transport_payload_bytes: int,
    *,
    enforce_bounds: bool,
) -> None:
    (
        attempts,
        max_attempts,
        created_at,
        expires_at,
        next_attempt_at,
        frame_kind,
        payload_size,
        payload_type,
        message_id_size,
        channel_size,
        correlation_size,
        correlation_is_null,
        message_id_trimmed_size,
        channel_trimmed_size,
        source_trimmed_size,
        correlation_trimmed_size,
        message_id_type,
        channel_type,
        semantics_type,
        source_type,
        correlation_type,
        computed_size,
        message_id_text,
        channel_text,
        source_text,
        correlation_text,
        size_type,
        frame_kind_type,
        attempts_type,
        max_attempts_type,
    ) = values
    _validate_integer_storage(
        channel,
        size_bytes=size_type,
        frame_kind=frame_kind_type,
        attempts=attempts_type,
        max_attempts=max_attempts_type,
    )
    _validate_size(
        channel,
        stored_size,
        _recovery_int(computed_size, "computed_size"),
    )
    _validate_required_text(
        channel,
        message_id_trimmed_size,
        channel_trimmed_size,
        correlation_trimmed_size,
    )
    _validate_text_storage(
        channel,
        message_id_type=message_id_type,
        channel_type=channel_type,
        correlation_type=correlation_type,
    )
    _validate_canonical_text(
        channel,
        message_id=message_id_text,
        channel=channel_text,
        source_key=source_text,
        correlation_id=correlation_text,
    )
    if semantics_type != "text":
        raise _RecoveryViolation(
            f"recovered topic {channel!r} semantics is not TEXT"
        )
    if source_type not in {"null", "text"}:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} source_key is not nullable TEXT"
        )
    _validate_wire_metadata(
        channel,
        frame_kind,
        payload_size,
        message_id_size,
        channel_size,
        correlation_size,
        correlation_is_null,
        max_payload_bytes=config.max_message_bytes,
        max_transport_payload_bytes=max_transport_payload_bytes,
        enforce_current_limits=enforce_bounds,
    )
    if payload_type != "blob":
        raise _RecoveryViolation(
            f"recovered topic {channel!r} payload is not a BLOB"
        )
    resolved_attempts = _recovery_int(attempts, "attempts")
    resolved_max_attempts = _recovery_int(max_attempts, "max_attempts")
    if (
        resolved_attempts < 0
        or resolved_max_attempts < 1
        or resolved_attempts > resolved_max_attempts
        or resolved_attempts > _MAX_DELIVERY_ATTEMPT
        or resolved_max_attempts > _MAX_DELIVERY_ATTEMPT
    ):
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has invalid attempt state"
        )
    _validate_timestamps(channel, created_at, expires_at, next_attempt_at)
    if not enforce_bounds:
        return
    if resolved_max_attempts > config.max_delivery_attempts:
        raise _RecoveryCapacityViolation(
            f"recovered topic {channel!r} max_attempts exceeds current config"
        )
    if policy is None:
        if semantics_type != "text" or source_trimmed_size is not None:
            raise _RecoveryViolation(
                f"recovered topic {channel!r} has invalid implicit append state"
            )
        _validate_lifetime(
            channel,
            float(created_at),
            float(expires_at),
            config.message_ttl_seconds,
        )
        return
    if resolved_max_attempts > policy.max_attempts:
        raise _RecoveryCapacityViolation(
            f"recovered topic {channel!r} max_attempts exceeds current policy"
        )
    if (policy.semantics.value == "latest") != (
        source_trimmed_size is not None
        and _recovery_int(source_trimmed_size, "source_key length") > 0
    ):
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has invalid source identity"
        )
    _validate_lifetime(
        channel,
        float(created_at),
        float(expires_at),
        policy.ttl_seconds,
    )


def _validate_inbox_row(
    channel: str,
    stored_size: int,
    values: tuple[object, ...],
    policy: TopicDeliveryPolicy | None,
    config: DeliveryConfig,
    *,
    rejection_only: bool,
    max_transport_payload_bytes: int,
    recovery_now: float,
    enforce_bounds: bool,
) -> None:
    (
        delivery_attempt,
        ack_attempts,
        created_at,
        expires_at,
        next_ack_at,
        status,
        frame_kind,
        payload_size,
        payload_type,
        message_id_size,
        channel_size,
        correlation_size,
        correlation_is_null,
        outcome_reason_size,
        ack_confirmed,
        message_id_trimmed_size,
        channel_trimmed_size,
        correlation_trimmed_size,
        outcome_reason_trimmed_size,
        message_id_type,
        channel_type,
        correlation_type,
        outcome_reason_type,
        computed_size,
        message_id_text,
        channel_text,
        correlation_text,
        outcome_reason_text,
        size_type,
        frame_kind_type,
        delivery_attempt_type,
        ack_attempts_type,
        ack_confirmed_type,
        rejection_only_type,
    ) = values
    _validate_integer_storage(
        channel,
        size_bytes=size_type,
        frame_kind=frame_kind_type,
        delivery_attempt=delivery_attempt_type,
        ack_attempts=ack_attempts_type,
        ack_confirmed=ack_confirmed_type,
        rejection_only=rejection_only_type,
    )
    _validate_size(
        channel,
        stored_size,
        _recovery_int(computed_size, "computed_size"),
    )
    _validate_required_text(
        channel,
        message_id_trimmed_size,
        channel_trimmed_size,
        correlation_trimmed_size,
    )
    _validate_text_storage(
        channel,
        message_id_type=message_id_type,
        channel_type=channel_type,
        correlation_type=correlation_type,
    )
    _validate_canonical_text(
        channel,
        message_id=message_id_text,
        channel=channel_text,
        correlation_id=correlation_text,
        outcome_reason=outcome_reason_text,
    )
    if outcome_reason_type not in {"null", "text"}:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} outcome reason is not nullable TEXT"
        )
    resolved_status = str(status)
    if resolved_status not in {"pending", "acked", "terminal", "expired"}:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has invalid outcome status"
        )
    _validate_wire_metadata(
        channel,
        frame_kind,
        payload_size,
        message_id_size,
        channel_size,
        correlation_size,
        correlation_is_null,
        max_payload_bytes=config.max_message_bytes,
        max_transport_payload_bytes=max_transport_payload_bytes,
        enforce_current_limits=(
            enforce_bounds and resolved_status == "pending"
        ),
    )
    if payload_type != "blob":
        raise _RecoveryViolation(
            f"recovered topic {channel!r} payload is not a BLOB"
        )
    resolved_delivery_attempt = _recovery_int(
        delivery_attempt,
        "delivery_attempt",
    )
    if not 1 <= resolved_delivery_attempt <= _MAX_DELIVERY_ATTEMPT:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has invalid delivery_attempt"
        )
    resolved_ack_attempts = _recovery_int(ack_attempts, "ack_attempts")
    if not 0 <= resolved_ack_attempts <= _MAX_DELIVERY_ATTEMPT:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has invalid ack_attempts"
        )
    if enforce_bounds and resolved_ack_attempts > config.max_ack_attempts:
        raise _RecoveryCapacityViolation(
            f"recovered topic {channel!r} ack_attempts exceeds current policy"
        )
    _validate_timestamps(channel, created_at, expires_at, next_ack_at)
    if (
        outcome_reason_size is not None
        and _recovery_int(
            outcome_reason_size,
            "outcome_reason length",
        )
        > _MAX_OUTCOME_REASON_BYTES
    ):
        raise _RecoveryViolation(
            f"recovered topic {channel!r} outcome reason exceeds wire limit"
        )
    if rejection_only and resolved_status != "terminal":
        raise _RecoveryViolation(
            f"recovered rejection {channel!r} has invalid status"
        )
    resolved_ack_confirmed = _recovery_int(
        ack_confirmed,
        "ack_confirmed",
    )
    if resolved_ack_confirmed not in (0, 1):
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has invalid ack_confirmed"
        )
    has_reason = (
        outcome_reason_trimmed_size is not None
        and _recovery_int(
            outcome_reason_trimmed_size,
            "outcome_reason trimmed length",
        )
        > 0
    )
    if resolved_status == "pending" and (
        has_reason
        or resolved_ack_attempts != 0
        or resolved_ack_confirmed != 0
        or rejection_only
    ):
        raise _RecoveryViolation(
            f"recovered pending topic {channel!r} has outcome state"
        )
    if resolved_status == "acked" and (has_reason or rejection_only):
        raise _RecoveryViolation(
            f"recovered ACK topic {channel!r} has invalid outcome state"
        )
    if resolved_status in {"terminal", "expired"} and not has_reason:
        raise _RecoveryViolation(
            f"recovered {resolved_status} topic {channel!r} lacks a reason"
        )
    if rejection_only and _recovery_int(payload_size, "payload length") != 0:
        raise _RecoveryViolation(
            f"recovered rejection {channel!r} retains a payload"
        )
    if (
        enforce_bounds
        and resolved_status == "pending"
        and policy is not None
    ):
        _validate_lifetime(
            channel,
            float(created_at),
            float(expires_at),
            policy.ttl_seconds,
        )
    if (
        enforce_bounds
        and resolved_status != "pending"
        and float(expires_at)
        > recovery_now + config.dedupe_retention_seconds
    ):
        raise _RecoveryCapacityViolation(
            f"recovered outcome {channel!r} retention exceeds current policy"
        )


def _validate_lifetime(
    channel: str,
    created_at: float,
    expires_at: float,
    limit_seconds: float,
) -> None:
    if created_at > expires_at or expires_at > created_at + limit_seconds:
        raise _RecoveryCapacityViolation(
            f"recovered topic {channel!r} lifetime exceeds current policy"
        )


def _validate_wire_metadata(
    channel: str,
    frame_kind: object,
    payload_size: object,
    message_id_size: object,
    channel_size: object,
    correlation_size: object,
    correlation_is_null: object,
    *,
    max_payload_bytes: int,
    max_transport_payload_bytes: int,
    enforce_current_limits: bool,
) -> None:
    try:
        resolved_frame_kind = FrameKind(int(frame_kind))
    except (TypeError, ValueError) as error:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has invalid frame kind"
        ) from error
    sizes = tuple(
        _recovery_int(value, "wire field length")
        for value in (
            payload_size,
            message_id_size,
            channel_size,
            correlation_size,
        )
    )
    payload_bytes, message_id_bytes, channel_bytes, correlation_bytes = sizes
    if payload_bytes < 0:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has an invalid payload size"
        )
    if enforce_current_limits and payload_bytes > max_payload_bytes:
        raise _RecoveryCapacityViolation(
            f"recovered topic {channel!r} payload exceeds current limit"
        )
    if not 1 <= message_id_bytes <= _MAX_MESSAGE_ID_BYTES:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} message_id exceeds wire limit"
        )
    if not 1 <= channel_bytes <= _MAX_TEXT_BYTES:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} channel exceeds wire limit"
        )
    if not 0 <= correlation_bytes <= _MAX_TEXT_BYTES:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} correlation_id exceeds wire limit"
        )
    has_correlation = not bool(correlation_is_null)
    if has_correlation and correlation_bytes == 0:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has an empty correlation_id"
        )
    if resolved_frame_kind is not FrameKind.PUBSUB and not has_correlation:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} RPC frame lacks a correlation_id"
        )
    encoded_size = (
        _DELIVERY_HEADER_SIZE
        + message_id_bytes
        + channel_bytes
        + correlation_bytes
        + payload_bytes
    )
    if enforce_current_limits and encoded_size > max_transport_payload_bytes:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} DATA frame exceeds transport limit"
        )


def _validate_size(
    channel: str,
    stored_size: int,
    computed_size: int,
) -> None:
    if stored_size != computed_size:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has inconsistent logical size"
        )


def _validate_required_text(
    channel: str,
    message_id_trimmed_size: object,
    channel_trimmed_size: object,
    correlation_trimmed_size: object,
) -> None:
    if _recovery_int(message_id_trimmed_size, "message_id length") < 1:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has a blank message_id"
        )
    if _recovery_int(channel_trimmed_size, "channel length") < 1:
        raise _RecoveryViolation("recovered row has a blank channel")
    if (
        correlation_trimmed_size is not None
        and _recovery_int(
            correlation_trimmed_size,
            "correlation_id length",
        )
        < 1
    ):
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has a blank correlation_id"
        )


def _validate_text_storage(
    channel: str,
    *,
    message_id_type: object,
    channel_type: object,
    correlation_type: object,
) -> None:
    if message_id_type != "text":
        raise _RecoveryViolation(
            f"recovered topic {channel!r} message_id is not TEXT"
        )
    if channel_type != "text":
        raise _RecoveryViolation("recovered channel is not TEXT")
    if correlation_type not in {"null", "text"}:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} correlation_id is not nullable TEXT"
        )


def _validate_canonical_text(
    topic: str,
    **fields: object,
) -> None:
    for field_name, value in fields.items():
        if isinstance(value, str) and value != value.strip():
            raise _RecoveryViolation(
                f"recovered topic {topic!r} has non-canonical {field_name}"
            )


def _validate_integer_storage(
    topic: str,
    **fields: object,
) -> None:
    for field_name, storage_type in fields.items():
        if storage_type != "integer":
            raise _RecoveryViolation(
                f"recovered topic {topic!r} {field_name} is not INTEGER"
            )


def _recovery_int(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _RecoveryViolation(
            f"recovered {field_name} is not an integer"
        )
    return value


def _validate_timestamps(
    channel: str,
    created_at: object,
    expires_at: object,
    next_attempt_at: object,
) -> None:
    try:
        values = tuple(
            float(value)
            for value in (created_at, expires_at, next_attempt_at)
        )
    except (OverflowError, TypeError, ValueError) as error:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has invalid timestamps"
        ) from error
    if not all(isfinite(value) for value in values) or values[0] > values[1]:
        raise _RecoveryViolation(
            f"recovered topic {channel!r} has invalid timestamps"
        )


def _finish_topic(
    watermarks: list[_RecoveredWatermark],
    config: DeliveryConfig,
    policy: TopicDeliveryPolicy,
    stats: _JournalStats,
    items: int,
    logical_bytes: int,
    *,
    side: _RecoveredStoreSide,
) -> None:
    outbound = side is _RecoveredStoreSide.OUTBOX
    capacity = DeliveryCapacity(
        (
            stats.outbox_items
            if outbound
            else stats.retained_inbox_items
        ),
        config.max_outbox_items if outbound else config.max_inbox_items,
        stats.logical_bytes,
        config.max_storage_bytes,
        items,
        policy.max_items if outbound else int(policy.max_inbox_items),
        logical_bytes,
        policy.max_bytes if outbound else int(policy.max_inbox_bytes),
        config.soft_limit_ratio,
        policy.soft_limit_ratio,
    )
    error = _capacity_error(capacity)
    if error is not None:
        raise _RecoveryCapacityViolation(error, capacity=capacity)
    if items >= capacity.topic_item_limit * policy.soft_limit_ratio:
        watermarks.append(
            _RecoveredWatermark(
                policy.topic,
                side,
                DeliveryCapacityDimension.TOPIC_ITEMS,
                capacity,
            )
        )
    if (
        logical_bytes
        >= capacity.topic_byte_limit * policy.soft_limit_ratio
    ):
        watermarks.append(
            _RecoveredWatermark(
                policy.topic,
                side,
                DeliveryCapacityDimension.TOPIC_LOGICAL_BYTES,
                capacity,
            )
        )


class _RecoveryViolation(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        capacity: DeliveryCapacity | None = None,
    ) -> None:
        super().__init__(message)
        self.capacity = capacity


@final
class _RecoveryCapacityViolation(_RecoveryViolation):
    pass


@final
class _RecoveredStoreSide(str, Enum):
    OUTBOX = "outbox"
    INBOX = "inbox"


@final
@dataclass(frozen=True, slots=True)
class _RecoveredWatermark:
    topic: str | None
    side: _RecoveredStoreSide | None
    dimension: DeliveryCapacityDimension
    capacity: DeliveryCapacity
