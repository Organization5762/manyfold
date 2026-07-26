"""Private mesh-owned durable topic session."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from hashlib import sha256
from time import monotonic, time
from typing import final

from ._transport_delivery_journal import (
    _DeliveryJournal,
    _InboxDisposition,
    _InboxRecord,
    _JournalFull,
    _OutboxRecord,
)
from ._transport_delivery_protocol import (
    DELIVERY_CHANNEL,
    _decode_delivery_frame,
    _DeliveryFrame,
    _DeliveryOperation,
    _encode_delivery_frame,
)
from .transport import (
    FrameKind,
    TcpTransport,
    TransportClosed,
    TransportMessage,
    TransportQueueFull,
)
from .transport_topics import (
    DurableTopicDiagnostics,
    DurableTopicMode,
    DurableTopicPolicy,
    MeshDurabilityConfig,
)

_BATCH_LIMIT = 32


@dataclass(slots=True)
class _TopicCounters:
    replaced: int = 0
    expired: int = 0
    retried: int = 0
    acknowledged: int = 0
    hard_cap_rejected: int = 0
    recovery_loaded_rows: int = 0


@final
class _MeshDurablePeer:
    def __init__(
        self,
        local_node_id: str,
        peer_node_id: str,
        config: MeshDurabilityConfig,
    ) -> None:
        peer_hash = sha256(peer_node_id.encode("utf-8")).hexdigest()[:24]
        self._config = config
        self._journal = _DeliveryJournal(
            config.journal_directory / local_node_id / f"peer-{peer_hash}.sqlite3",
            max_outbox_items=config.hard_peer_items,
            max_inbox_items=config.hard_peer_items,
            max_storage_bytes=config.hard_peer_bytes,
        )
        self._counters: dict[str, _TopicCounters] = {}
        self._last_compaction = monotonic()
        recovery = self._journal.stats()
        self.recovery_loaded_rows = (
            recovery.outbox_items
            + recovery.pending_inbox_items
            + recovery.acked_inbox_items
        )
        for channel in self._journal.channels():
            stats = self._journal.channel_stats(channel)
            self._topic_counters(channel).recovery_loaded_rows = (
                stats.outbox_items + stats.pending_inbox_items + stats.acked_inbox_items
            )

    def close(self) -> None:
        self._journal.close()

    def enqueue(
        self,
        message: TransportMessage,
        *,
        message_id: str,
        replacement_key: str | None,
        policy: DurableTopicPolicy,
    ) -> None:
        if len(message.payload) > policy.max_message_bytes:
            raise ValueError(
                f"topic {message.channel!r} payload exceeds max_message_bytes "
                f"({len(message.payload)} > {policy.max_message_bytes})"
            )
        self._compact_for_watermark(message.channel, policy)
        record = _OutboxRecord(
            message_id,
            int(message.kind),
            message.channel,
            message.correlation_id,
            message.payload,
            0,
        )
        counters = self._topic_counters(message.channel)
        now = time()
        try:
            if policy.mode is DurableTopicMode.LATEST:
                if replacement_key is None:
                    raise ValueError("latest durable topic requires a replacement key")
                replaced = self._journal.replace_outbox(
                    record,
                    replacement_key=replacement_key,
                    created_at=now,
                    expires_at=now + policy.ttl_seconds,
                    channel_item_limit=policy.hard_pending_items,
                    channel_byte_limit=policy.hard_pending_bytes,
                )
                counters.replaced += int(replaced)
            else:
                self._journal.insert_outbox(
                    record,
                    created_at=now,
                    expires_at=now + policy.ttl_seconds,
                    channel_item_limit=policy.hard_pending_items,
                    channel_byte_limit=policy.hard_pending_bytes,
                )
        except _JournalFull:
            counters.hard_cap_rejected += 1
            raise

    def handle(
        self,
        transport: TcpTransport,
        message: TransportMessage,
        *,
        max_message_bytes: int,
        dedupe_ttl_seconds: float,
    ) -> _DeliveryFrame | None:
        frame = _decode_delivery_frame(
            message,
            max_message_bytes=max_message_bytes,
        )
        if frame.operation is _DeliveryOperation.DATA:
            record = _InboxRecord(
                frame.message_id,
                frame.frame_kind,
                frame.channel,
                frame.correlation_id,
                frame.payload,
                frame.delivery_attempt,
            )
            now = time()
            disposition = self._journal.record_inbox(
                record,
                created_at=now,
                expires_at=now + dedupe_ttl_seconds,
            )
            if disposition is _InboxDisposition.ACKED_DUPLICATE:
                self._journal.schedule_ack_now(frame.message_id, now)
                return None
            return frame
        if frame.operation is _DeliveryOperation.ACK:
            channel = self._outbox_channel(frame.message_id)
            if self._journal.delete_outbox(frame.message_id) and channel is not None:
                self._topic_counters(channel).acknowledged += 1
            self._send_control(transport, _DeliveryOperation.CONFIRM, frame.message_id)
        elif frame.operation is _DeliveryOperation.NACK:
            self._journal.mark_outbox_attempt(
                frame.message_id,
                next_attempt_at=time(),
                error=f"peer NACK: {frame.payload.decode('utf-8', errors='replace')}",
                increment_attempts=False,
            )
        elif frame.operation is _DeliveryOperation.CONFIRM:
            self._journal.confirm_ack(frame.message_id)
        return None

    def recover_pending(self) -> tuple[_DeliveryFrame, ...]:
        records = self._journal.pending_inbox(
            time(),
            limit=self._config.hard_peer_items,
        )
        return tuple(
            _DeliveryFrame(
                _DeliveryOperation.DATA,
                record.message_id,
                record.frame_kind,
                record.delivery_attempt,
                record.channel,
                record.correlation_id,
                record.payload,
            )
            for record in records
        )

    def ack(self, transport: TcpTransport, frame: _DeliveryFrame) -> None:
        self._journal.mark_inbox_acked(frame.message_id, next_ack_at=time())
        self._send_control(transport, _DeliveryOperation.ACK, frame.message_id)

    def nack(
        self,
        transport: TcpTransport,
        frame: _DeliveryFrame,
        reason: str,
    ) -> None:
        self._journal.delete_pending_inbox(frame.message_id)
        self._send_control(
            transport,
            _DeliveryOperation.NACK,
            frame.message_id,
            reason.encode("utf-8")[:65535],
        )

    def tick(self, transport: TcpTransport) -> None:
        self.maintain()
        now = time()
        for record in self._journal.due_outbox(now, limit=_BATCH_LIMIT):
            frame = _encode_delivery_frame(
                _DeliveryOperation.DATA,
                record.message_id,
                frame_kind=record.frame_kind,
                channel=record.channel,
                correlation_id=record.correlation_id,
                payload=record.payload,
                delivery_attempt=record.attempts + 1,
            )
            try:
                transport.send(
                    TransportMessage(FrameKind.PUBSUB, DELIVERY_CHANNEL, frame),
                    timeout=0.01,
                )
            except (TransportClosed, TransportQueueFull) as error:
                self._journal.mark_outbox_attempt(
                    record.message_id,
                    next_attempt_at=now + self._config.retry_initial_seconds,
                    error=f"{type(error).__name__}: {error}",
                    increment_attempts=False,
                )
                continue
            attempt = record.attempts + 1
            self._journal.mark_outbox_attempt(
                record.message_id,
                next_attempt_at=now + self._retry_delay(attempt),
                error=None,
                increment_attempts=True,
            )
            if record.attempts:
                self._topic_counters(record.channel).retried += 1
        for record in self._journal.due_acks(now, limit=_BATCH_LIMIT):
            if self._send_control(
                transport,
                _DeliveryOperation.ACK,
                record.message_id,
            ):
                self._journal.mark_ack_attempt(
                    record.message_id,
                    next_ack_at=now + self._retry_delay(record.ack_attempts + 1),
                )

    def maintain(self) -> None:
        """Expire and compact bounded rows even while the peer is disconnected."""
        now_monotonic = monotonic()
        if (
            now_monotonic - self._last_compaction
            >= self._config.compaction_interval_seconds
        ):
            self._compact()
            self._last_compaction = now_monotonic

    def diagnostics(
        self,
        topic: str,
        policy: DurableTopicPolicy,
    ) -> DurableTopicDiagnostics:
        stats = self._journal.channel_stats(topic)
        counters = self._topic_counters(topic)
        return DurableTopicDiagnostics(
            topic,
            policy.mode,
            stats.outbox_items,
            stats.pending_inbox_items,
            stats.acked_inbox_items,
            stats.logical_bytes,
            counters.replaced,
            counters.expired,
            counters.retried,
            counters.acknowledged,
            counters.hard_cap_rejected,
            counters.recovery_loaded_rows,
        )

    def _compact_for_watermark(
        self,
        topic: str,
        policy: DurableTopicPolicy,
    ) -> None:
        topic_stats = self._journal.channel_stats(topic)
        peer_stats = self._journal.stats()
        if (
            topic_stats.outbox_items >= policy.soft_pending_items
            or topic_stats.logical_bytes >= policy.soft_pending_bytes
            or peer_stats.outbox_items >= self._config.soft_peer_items
            or peer_stats.logical_bytes >= self._config.soft_peer_bytes
        ):
            self._compact()

    def _compact(self) -> None:
        result = self._journal.compact(time())
        for channel, count in Counter(
            result.expired_outbox_channels + result.expired_inbox_channels
        ).items():
            self._topic_counters(channel).expired += count

    def _outbox_channel(self, message_id: str) -> str | None:
        return self._journal.outbox_channel(message_id)

    def _send_control(
        self,
        transport: TcpTransport,
        operation: _DeliveryOperation,
        message_id: str,
        payload: bytes = b"",
    ) -> bool:
        try:
            transport.send(
                TransportMessage(
                    FrameKind.PUBSUB,
                    DELIVERY_CHANNEL,
                    _encode_delivery_frame(
                        operation,
                        message_id,
                        payload=payload,
                    ),
                ),
                timeout=0.01,
            )
        except (TransportClosed, TransportQueueFull):
            return False
        return True

    def _retry_delay(self, attempt: int) -> float:
        try:
            delay = self._config.retry_initial_seconds * (
                self._config.retry_multiplier ** max(attempt - 1, 0)
            )
        except OverflowError:
            return self._config.retry_max_seconds
        return min(delay, self._config.retry_max_seconds)

    def _topic_counters(self, topic: str) -> _TopicCounters:
        counters = self._counters.get(topic)
        if counters is None:
            counters = _TopicCounters()
            self._counters[topic] = counters
        return counters


__all__: list[str] = []
