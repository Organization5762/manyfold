"""Stateless capacity calculations for an owner-supplied SQLite connection."""

from __future__ import annotations

import sqlite3

from ._transport_delivery_events import (
    DeliveryCapacity,
    DeliveryCapacityDimension,
)
from ._transport_delivery_journal_errors import _JournalFull
from ._transport_delivery_policy import DeliveryConfig, TopicDeliveryPolicy
from ._transport_delivery_records import _JournalStats, _TopicStats


def _outbox_capacity(
    connection: sqlite3.Connection,
    config: DeliveryConfig,
    channel: str,
    policy: TopicDeliveryPolicy,
    *,
    delta_items: int,
    delta_bytes: int,
) -> DeliveryCapacity:
    return _capacity(
        connection,
        config,
        table="outbox",
        channel=channel,
        peer_item_limit=config.max_outbox_items,
        topic_item_limit=policy.max_items,
        topic_byte_limit=policy.max_bytes,
        peer_soft_limit_ratio=config.soft_limit_ratio,
        topic_soft_limit_ratio=policy.soft_limit_ratio,
        delta_items=delta_items,
        delta_bytes=delta_bytes,
    )


def _inbox_capacity(
    connection: sqlite3.Connection,
    config: DeliveryConfig,
    channel: str,
    policy: TopicDeliveryPolicy,
    *,
    delta_items: int,
    delta_bytes: int,
) -> DeliveryCapacity:
    return _capacity(
        connection,
        config,
        table="inbox",
        channel=channel,
        peer_item_limit=config.max_inbox_items,
        topic_item_limit=int(policy.max_inbox_items),
        topic_byte_limit=int(policy.max_inbox_bytes),
        peer_soft_limit_ratio=config.soft_limit_ratio,
        topic_soft_limit_ratio=policy.soft_limit_ratio,
        delta_items=delta_items,
        delta_bytes=delta_bytes,
    )


def _inbox_peer_capacity(
    connection: sqlite3.Connection,
    config: DeliveryConfig,
    *,
    delta_items: int,
    delta_bytes: int,
) -> DeliveryCapacity:
    peer_items = int(
        connection.execute("SELECT COUNT(*) FROM inbox").fetchone()[0]
    )
    return DeliveryCapacity(
        peer_items + delta_items,
        config.max_inbox_items,
        _logical_bytes(connection) + delta_bytes,
        config.max_storage_bytes,
        0,
        config.max_inbox_items,
        0,
        config.max_storage_bytes,
        config.soft_limit_ratio,
        config.soft_limit_ratio,
    )


def _project_capacity(
    current: DeliveryCapacity,
    *,
    delta_items: int,
    delta_bytes: int,
) -> DeliveryCapacity:
    """Apply one transaction-local row delta to an exact SQLite snapshot."""
    return DeliveryCapacity(
        current.peer_items + delta_items,
        current.peer_item_limit,
        current.peer_logical_bytes + delta_bytes,
        current.peer_byte_limit,
        current.topic_items + delta_items,
        current.topic_item_limit,
        current.topic_logical_bytes + delta_bytes,
        current.topic_byte_limit,
        current.peer_soft_limit_ratio,
        current.topic_soft_limit_ratio,
    )


def _project_peer_capacity(
    current: DeliveryCapacity,
    *,
    delta_items: int,
    delta_bytes: int,
) -> DeliveryCapacity:
    """Apply a peer-only row delta without inventing a configured topic."""
    return DeliveryCapacity(
        current.peer_items + delta_items,
        current.peer_item_limit,
        current.peer_logical_bytes + delta_bytes,
        current.peer_byte_limit,
        current.topic_items,
        current.topic_item_limit,
        current.topic_logical_bytes,
        current.topic_byte_limit,
        current.peer_soft_limit_ratio,
        current.topic_soft_limit_ratio,
    )


def _capacity_error(capacity: DeliveryCapacity) -> str | None:
    checks = (
        (capacity.peer_items, capacity.peer_item_limit, "peer item"),
        (
            capacity.peer_logical_bytes,
            capacity.peer_byte_limit,
            "peer logical byte",
        ),
        (capacity.topic_items, capacity.topic_item_limit, "topic item"),
        (
            capacity.topic_logical_bytes,
            capacity.topic_byte_limit,
            "topic logical byte",
        ),
    )
    for observed, limit, label in checks:
        if observed > limit:
            return f"delivery {label} limit {limit} would be exceeded"
    return None


def _require_capacity(capacity: DeliveryCapacity) -> None:
    error = _capacity_error(capacity)
    if error is not None:
        raise _JournalFull(error, capacity=capacity)


def _at_watermark(capacity: DeliveryCapacity) -> bool:
    return any(
        observed >= limit * ratio
        for observed, limit, ratio in _capacity_values(capacity)
    )


def _crossed_watermarks(
    current: DeliveryCapacity,
    projected: DeliveryCapacity,
) -> tuple[DeliveryCapacityDimension, ...]:
    return tuple(
        dimension
        for dimension, (before, _, _), (after, limit, ratio) in zip(
            DeliveryCapacityDimension,
            _capacity_values(current),
            _capacity_values(projected),
            strict=True,
        )
        if before < limit * ratio <= after
    )


def _at_peer_watermark(capacity: DeliveryCapacity) -> bool:
    return any(
        observed >= limit * ratio
        for observed, limit, ratio in _capacity_values(capacity)[:2]
    )


def _crossed_peer_watermarks(
    current: DeliveryCapacity,
    projected: DeliveryCapacity,
) -> tuple[DeliveryCapacityDimension, ...]:
    return tuple(
        dimension
        for dimension, (before, _, _), (after, limit, ratio) in zip(
            tuple(DeliveryCapacityDimension)[:2],
            _capacity_values(current)[:2],
            _capacity_values(projected)[:2],
            strict=True,
        )
        if before < limit * ratio <= after
    )


def _logical_bytes(connection: sqlite3.Connection) -> int:
    return sum(
        int(
            connection.execute(
                f"SELECT COALESCE(SUM(size_bytes), 0) FROM {table}"
            ).fetchone()[0]
        )
        for table in ("outbox", "inbox")
    )


def _journal_stats(connection: sqlite3.Connection) -> _JournalStats:
    outbox = connection.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(semantics = 'append'), 0),
               COALESCE(SUM(semantics = 'latest'), 0)
        FROM outbox
        """
    ).fetchone()
    inbox = connection.execute(
        """
        SELECT COALESCE(SUM(status = 'pending'), 0),
               COALESCE(SUM(status = 'acked'), 0),
               COALESCE(SUM(status = 'terminal'), 0),
               COALESCE(SUM(status = 'expired'), 0)
        FROM inbox
        """
    ).fetchone()
    return _JournalStats(
        int(outbox[0]),
        int(outbox[1]),
        int(outbox[2]),
        int(inbox[0]),
        int(inbox[1]),
        int(inbox[2]),
        int(inbox[3]),
        _logical_bytes(connection),
    )


def _topic_stats(
    connection: sqlite3.Connection,
    channel: str,
) -> _TopicStats:
    outbox = connection.execute(
        """
        SELECT COUNT(*),
               COALESCE(SUM(semantics = 'append'), 0),
               COALESCE(SUM(semantics = 'latest'), 0),
               COALESCE(SUM(size_bytes), 0)
        FROM outbox WHERE channel = ?
        """,
        (channel,),
    ).fetchone()
    inbox = connection.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(size_bytes), 0)
        FROM inbox WHERE channel = ?
        """,
        (channel,),
    ).fetchone()
    return _TopicStats(
        channel,
        int(outbox[0]),
        int(outbox[1]),
        int(outbox[2]),
        int(inbox[0]),
        int(outbox[3]) + int(inbox[1]),
    )


def _capacity(
    connection: sqlite3.Connection,
    config: DeliveryConfig,
    *,
    table: str,
    channel: str,
    peer_item_limit: int,
    topic_item_limit: int,
    topic_byte_limit: int,
    peer_soft_limit_ratio: float,
    topic_soft_limit_ratio: float,
    delta_items: int,
    delta_bytes: int,
) -> DeliveryCapacity:
    peer_items = int(
        connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    )
    topic = connection.execute(
        f"""
        SELECT COUNT(*), COALESCE(SUM(size_bytes), 0)
        FROM {table} WHERE channel = ?
        """,
        (channel,),
    ).fetchone()
    return DeliveryCapacity(
        peer_items + delta_items,
        peer_item_limit,
        _logical_bytes(connection) + delta_bytes,
        config.max_storage_bytes,
        int(topic[0]) + delta_items,
        topic_item_limit,
        int(topic[1]) + delta_bytes,
        topic_byte_limit,
        peer_soft_limit_ratio,
        topic_soft_limit_ratio,
    )


def _capacity_values(
    capacity: DeliveryCapacity,
) -> tuple[tuple[int, int, float], ...]:
    return (
        (
            capacity.peer_items,
            capacity.peer_item_limit,
            capacity.peer_soft_limit_ratio,
        ),
        (
            capacity.peer_logical_bytes,
            capacity.peer_byte_limit,
            capacity.peer_soft_limit_ratio,
        ),
        (
            capacity.topic_items,
            capacity.topic_item_limit,
            capacity.topic_soft_limit_ratio,
        ),
        (
            capacity.topic_logical_bytes,
            capacity.topic_byte_limit,
            capacity.topic_soft_limit_ratio,
        ),
    )
