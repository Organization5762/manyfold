"""Private mesh adapter over the authoritative durable delivery journal."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Protocol, final

from . import _transport_mesh_protocol as _mesh_protocol
from ._transport_mesh_lifecycle import MeshLifecycleKind, MeshLifecycleReason
from .transport import TcpTransport, TransportMessage
from .transport_delivery import (
    DeliveryConfig,
    DeliveryEvent,
    DeliveryEventKind,
    DeliveryHealth,
    DurableDelivery,
    ReceivedDelivery,
)
from .transport_topics import (
    DurableTopicDiagnostics,
    MeshDurabilityConfig,
    MeshTopicPolicy,
)


def _lifecycle_transition(
    event: DeliveryEvent,
) -> tuple[MeshLifecycleKind, MeshLifecycleReason]:
    transitions = {
        DeliveryEventKind.ACKNOWLEDGED: (
            MeshLifecycleKind.DURABLE_ACKED,
            MeshLifecycleReason.ACKNOWLEDGEMENT,
        ),
        DeliveryEventKind.COALESCED: (
            MeshLifecycleKind.DURABLE_COALESCED,
            MeshLifecycleReason.REPLACED,
        ),
        DeliveryEventKind.DEDUPLICATED: (
            MeshLifecycleKind.DURABLE_DROPPED,
            MeshLifecycleReason.DUPLICATE,
        ),
        DeliveryEventKind.DROPPED: (
            MeshLifecycleKind.DURABLE_DROPPED,
            (
                MeshLifecycleReason.CAPACITY
                if event.capacity is not None
                else (
                    MeshLifecycleReason.DELIVERY_ATTEMPTS_EXHAUSTED
                    if event.detail == "retry budget exhausted"
                    else MeshLifecycleReason.ERROR
                )
            ),
        ),
        DeliveryEventKind.DUPLICATE_SUPPRESSED: (
            MeshLifecycleKind.DURABLE_DROPPED,
            MeshLifecycleReason.DUPLICATE,
        ),
        DeliveryEventKind.ENQUEUED: (
            MeshLifecycleKind.DURABLE_ENQUEUED,
            MeshLifecycleReason.LOCAL_PUBLICATION,
        ),
        DeliveryEventKind.EXPIRED: (
            MeshLifecycleKind.DURABLE_EXPIRED,
            MeshLifecycleReason.EXPIRY,
        ),
        DeliveryEventKind.REPLAYED: (
            MeshLifecycleKind.DURABLE_REPLAYED,
            MeshLifecycleReason.RECOVERY,
        ),
        DeliveryEventKind.RETRY_SCHEDULED: (
            MeshLifecycleKind.DURABLE_RETRY,
            MeshLifecycleReason.RETRY_SCHEDULED,
        ),
        DeliveryEventKind.SENT: (
            MeshLifecycleKind.DURABLE_SENT,
            MeshLifecycleReason.LOCAL_PUBLICATION,
        ),
        DeliveryEventKind.SOFT_WATERMARK: (
            MeshLifecycleKind.WATERMARK_CROSSED,
            MeshLifecycleReason.CAPACITY,
        ),
        DeliveryEventKind.SOFT_WATERMARK_RECOVERED: (
            MeshLifecycleKind.WATERMARK_RECOVERED,
            MeshLifecycleReason.CAPACITY,
        ),
    }
    return transitions[event.kind]


def _application_correlation(event: DeliveryEvent) -> str | None:
    try:
        return _mesh_protocol.decode_durable_correlation(
            event.correlation_id
        ).correlation_id
    except ValueError:
        return event.correlation_id


class _LifecycleSink(Protocol):
    def __call__(
        self,
        kind: MeshLifecycleKind,
        reason: MeshLifecycleReason,
        **fields: object,
    ) -> object: ...


@dataclass(slots=True)
class _TopicState:
    coalesced: int = 0
    expired: int = 0
    retried: int = 0
    acknowledged: int = 0
    storage_rejections: int = 0
    recovery_loaded_rows: int = 0


@final
class _MeshDeliveryPeer:
    """Adapt DurableDelivery to one mesh-owned transport receive loop."""

    def __init__(
        self,
        local_node_id: str,
        peer_node_id: str,
        transport: TcpTransport,
        config: MeshDurabilityConfig,
        policies: tuple[MeshTopicPolicy, ...],
        lifecycle_sink: _LifecycleSink,
    ) -> None:
        self._peer_node_id = peer_node_id
        self._emit = lifecycle_sink
        self._states: dict[str, _TopicState] = {}
        journal_policies = tuple(
            policy.journal_policy
            for policy in policies
            if policy.journal_policy is not None
        )
        peer_hash = sha256(peer_node_id.encode("utf-8")).hexdigest()[:24]
        journal_path = (
            config.journal_directory
            / local_node_id
            / f"peer-{peer_hash}.sqlite3"
        )
        journal_path.parent.mkdir(parents=True, exist_ok=True)
        max_message_bytes = max(
            (policy.max_message_bytes for policy in policies),
            default=1,
        )
        self._delivery = DurableDelivery(
            transport,
            DeliveryConfig(
                journal_path=journal_path,
                max_outbox_items=config.hard_peer_items,
                max_inbox_items=config.hard_peer_items,
                max_storage_bytes=config.hard_peer_bytes,
                receive_queue_limit=config.hard_peer_items,
                max_message_bytes=max_message_bytes,
                dedupe_retention_seconds=config.dedupe_retention_seconds,
                retry_initial_seconds=config.retry_initial_seconds,
                retry_multiplier=config.retry_multiplier,
                retry_max_seconds=config.retry_max_seconds,
                topic_policies=journal_policies,
            ),
            owns_receive_loop=False,
            observer=self._observe,
        )

    def close(self) -> None:
        self._delivery.close()

    def send(
        self,
        message: TransportMessage,
        *,
        message_id: str,
        source: str | None,
    ) -> None:
        self._delivery.send(
            message,
            message_id=message_id,
            source=source,
        )

    def handle_transport_message(
        self,
        message: TransportMessage,
    ) -> tuple[ReceivedDelivery, ...]:
        self._delivery.handle_transport_message(message)
        return self.recover_pending()

    def recover_pending(self) -> tuple[ReceivedDelivery, ...]:
        deliveries: list[ReceivedDelivery] = []
        while True:
            try:
                deliveries.append(self._delivery.receive(timeout=0.0))
            except TimeoutError:
                return tuple(deliveries)

    def ack(self, message_id: str) -> None:
        self._delivery.ack(message_id)

    def nack(self, message_id: str, reason: str) -> None:
        self._delivery.nack(message_id, reason=reason)

    def health(self) -> DeliveryHealth:
        return self._delivery.health()

    def diagnostics(
        self,
        topic: str,
        policy: MeshTopicPolicy,
    ) -> DurableTopicDiagnostics:
        state = self._state(topic)
        return DurableTopicDiagnostics(
            topic=topic,
            delivery_class=policy.delivery_class,
            retains_journal_rows=bool(
                self._delivery._retained_topic_rows(topic)
            ),
            outbox_items=self._delivery._retained_topic_outbox_items(topic),
            coalesced=state.coalesced,
            expired=state.expired,
            retried=state.retried,
            acknowledged=state.acknowledged,
            storage_rejections=state.storage_rejections,
            recovery_loaded_rows=state.recovery_loaded_rows,
        )

    def _observe(self, event: DeliveryEvent) -> None:
        state = self._state(event.topic)
        if event.kind is DeliveryEventKind.COALESCED:
            state.coalesced += 1
        elif event.kind is DeliveryEventKind.REPLAYED:
            state.recovery_loaded_rows += 1
        elif event.kind is DeliveryEventKind.ACKNOWLEDGED:
            state.acknowledged += 1
        elif event.kind is DeliveryEventKind.EXPIRED:
            state.expired += 1
        elif event.kind is DeliveryEventKind.RETRY_SCHEDULED:
            state.retried += 1
        elif event.kind is DeliveryEventKind.DROPPED:
            if event.capacity is not None:
                state.storage_rejections += 1
        kind, reason = _lifecycle_transition(event)
        fields: dict[str, object] = {
            "topic": event.topic,
            "peer_node_id": self._peer_node_id,
            "message_id": event.message_id,
            "correlation_id": _application_correlation(event),
            "related_message_id": event.related_message_id,
            "attempt": event.attempt,
            "detail": event.detail,
        }
        if event.capacity is not None:
            fields["item_count"] = event.capacity.topic_items
            fields["byte_count"] = event.capacity.topic_bytes
        self._emit(kind, reason, **fields)
        if event.kind is DeliveryEventKind.DROPPED:
            self._emit(
                MeshLifecycleKind.DELIVERY_FAILED,
                (
                    MeshLifecycleReason.CAPACITY
                    if event.capacity is not None
                    else MeshLifecycleReason.DELIVERY_ATTEMPTS_EXHAUSTED
                ),
                **fields,
            )
        elif event.kind is DeliveryEventKind.EXPIRED and event.terminal:
            self._emit(
                MeshLifecycleKind.DELIVERY_FAILED,
                MeshLifecycleReason.EXPIRY,
                **fields,
            )

    def _state(self, topic: str) -> _TopicState:
        state = self._states.get(topic)
        if state is None:
            state = _TopicState()
            self._states[topic] = state
        return state

__all__: list[str] = []
