"""Bounded multi-peer PubSub routing over concrete TCP transports."""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from queue import Empty, Full, Queue
from tempfile import TemporaryDirectory
from threading import RLock, Thread, current_thread, local
from time import time
from typing import final
from uuid import uuid4

from . import _transport_mesh_protocol as _protocol
from ._transport_mesh_delivery import (
    MeshDeliveryConfig,
    MeshTopicBinding,
    MeshTopicPolicy,
)
from ._transport_mesh_lifecycle import (
    MeshLifecycleEvent,
    MeshLifecycleHealth,
    MeshLifecycleKind,
    MeshLifecycleReason,
    _MeshLifecycleLog,
)
from .pubsub import PubSub, PubSubCallbackSubscription, StreamRow
from .transport import (
    FrameKind,
    LinkHealth,
    LinkState,
    NodeIdentity,
    TcpAddress,
    TcpTransport,
    TransportConfig,
    TransportMessage,
)
from .transport_delivery import (
    DeliveryClosed,
    DeliveryError,
    DeliveryHealth,
    DeliveryStorageFull,
    DurableDelivery,
    ReceivedDelivery,
)

DEFAULT_MAX_PEERS = 32
DEFAULT_MAX_SUBSCRIPTIONS = 4096
DEFAULT_DUPLICATE_WINDOW = 8192
DEFAULT_PUBLICATION_QUEUE_LIMIT = 1024
DEFAULT_LIFECYCLE_EVENT_LIMIT = 4096

_CLOSED_SENTINEL = object()


class MeshError(RuntimeError):
    """Base error for mesh routing and lifecycle failures."""


@final
class MeshClosed(MeshError):
    """Raised when an operation targets a closed mesh."""


@final
class MeshCapacityError(MeshError):
    """Raised before a configured mesh retention bound would be exceeded."""


class MeshRouteError(MeshError):
    """Raised when a publication or control frame cannot be routed."""


class MeshBackpressureError(MeshRouteError):
    """One fanout encountered bounded transport backpressure."""

    def __init__(
        self,
        message_id: str,
        *,
        accepted_peers: Sequence[str],
        rejected_peers: Sequence[str],
    ) -> None:
        self.message_id = message_id
        self.accepted_peers = tuple(accepted_peers)
        self.rejected_peers = tuple(rejected_peers)
        super().__init__(
            f"mesh message {message_id!r} reached peers "
            f"{self.accepted_peers!r} but backpressure rejected "
            f"{self.rejected_peers!r}"
        )


@final
@dataclass(frozen=True, slots=True)
class MeshConfig:
    """Hard memory and fanout limits for one transport mesh."""

    max_peers: int = DEFAULT_MAX_PEERS
    max_subscriptions: int = DEFAULT_MAX_SUBSCRIPTIONS
    duplicate_window: int = DEFAULT_DUPLICATE_WINDOW
    publication_queue_limit: int = DEFAULT_PUBLICATION_QUEUE_LIMIT
    lifecycle_event_limit: int = DEFAULT_LIFECYCLE_EVENT_LIMIT

    def __post_init__(self) -> None:
        _protocol.require_positive_integer(self.max_peers, "max_peers")
        _protocol.require_positive_integer(self.max_subscriptions, "max_subscriptions")
        _protocol.require_positive_integer(self.duplicate_window, "duplicate_window")
        _protocol.require_positive_integer(
            self.publication_queue_limit,
            "publication_queue_limit",
        )
        _protocol.require_positive_integer(
            self.lifecycle_event_limit,
            "lifecycle_event_limit",
        )


@final
@dataclass(frozen=True, slots=True)
class PeerDiscovery:
    """One typed static-discovery entry for a connector-owned peer."""

    node_id: str
    address: TcpAddress
    transport_config: TransportConfig | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "node_id",
            _protocol.require_text(self.node_id, "peer node_id"),
        )
        if not isinstance(self.address, TcpAddress):
            raise ValueError("peer address must be a TcpAddress")
        if self.transport_config is not None and not isinstance(
            self.transport_config,
            TransportConfig,
        ):
            raise ValueError("peer transport_config must be a TransportConfig or None")


@final
@dataclass(frozen=True, slots=True)
class MeshPublication:
    """One locally consumable PubSub publication received by the mesh."""

    topic: str
    payload: bytes
    message_id: str
    correlation_id: str | None
    source_node_id: str
    received_at: float


@final
@dataclass(frozen=True, slots=True)
class MeshPublishResult:
    """Routing result for one accepted local publication."""

    message_id: str
    delivered_locally: bool
    forwarded_peers: tuple[str, ...]


@final
@dataclass(frozen=True, slots=True)
class MeshPeerHealth:
    """Reconnect-aware health and routing state for one registered peer."""

    node_id: str
    source: str
    address: TcpAddress
    link: LinkHealth
    delivery: DeliveryHealth
    interested_topics: tuple[str, ...]
    last_routing_error: str | None


@final
@dataclass(frozen=True, slots=True)
class MeshHealth:
    """Bounded mesh-level lifecycle and routing counters."""

    is_closed: bool
    peer_count: int
    connected_peers: int
    local_subscriptions: int
    remote_subscriptions: int
    publications_queued: int
    recent_publications: int
    duplicate_publications: int
    last_routing_error: str | None


@final
class MeshSubscription:
    """Disposable local interest propagated through the transport mesh."""

    def __init__(
        self,
        mesh: "TransportMesh",
        subscription_id: str,
        topic: str,
    ) -> None:
        self.subscription_id = subscription_id
        self.topic = topic
        self._mesh = mesh
        self._disposed = False
        self._lock = RLock()

    @property
    def is_disposed(self) -> bool:
        """Return whether this handle already withdrew its interest."""
        with self._lock:
            return self._disposed

    def __enter__(self) -> "MeshSubscription":
        return self

    def __exit__(self, *error: object) -> None:
        self.dispose()

    def dispose(self) -> bool:
        """Withdraw this subscription once and propagate unsubscribe."""
        with self._lock:
            if self._disposed:
                return False
            self._mesh._unsubscribe(self.subscription_id)
            self._disposed = True
        return True


@final
class MeshSubscriptionBackpressureError(MeshBackpressureError):
    """Subscription propagation failed while ownership remains with the caller."""

    def __init__(
        self,
        subscription: MeshSubscription,
        *,
        accepted_peers: Sequence[str],
        rejected_peers: Sequence[str],
    ) -> None:
        self.subscription = subscription
        super().__init__(
            subscription.subscription_id,
            accepted_peers=accepted_peers,
            rejected_peers=rejected_peers,
        )


@final
class TransportMesh:
    """Own and route across a bounded set of concrete ``TcpTransport`` links."""

    def __init__(
        self,
        identity: NodeIdentity,
        *,
        connector_config: TransportConfig,
        listener_config: TransportConfig | None = None,
        config: MeshConfig | None = None,
        delivery: MeshDeliveryConfig | None = None,
    ) -> None:
        if not isinstance(identity, NodeIdentity):
            raise ValueError("identity must be a NodeIdentity")
        if not isinstance(connector_config, TransportConfig):
            raise ValueError("connector_config must be a TransportConfig")
        if listener_config is not None and not isinstance(
            listener_config,
            TransportConfig,
        ):
            raise ValueError("listener_config must be a TransportConfig or None")
        if config is not None and not isinstance(config, MeshConfig):
            raise ValueError("config must be a MeshConfig or None")
        if delivery is not None and not isinstance(delivery, MeshDeliveryConfig):
            raise ValueError("delivery must be a MeshDeliveryConfig or None")
        self.identity = identity
        self.connector_config = connector_config
        self.listener_config = listener_config or connector_config
        self.config = config or MeshConfig()
        self.delivery = delivery or MeshDeliveryConfig()
        self._temporary_state = (
            TemporaryDirectory(prefix="manyfold-mesh-")
            if self.delivery.state_directory is None
            else None
        )
        self._state_directory = (
            Path(self._temporary_state.name)
            if self._temporary_state is not None
            else self.delivery.state_directory
        )
        if self._state_directory is None:
            raise RuntimeError("mesh delivery state directory was not resolved")
        self._state_directory.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._lifecycle = _MeshLifecycleLog(
            identity.node_id,
            self.config.lifecycle_event_limit,
        )
        self._closed = False
        self._peers: dict[str, _PeerLink] = {}
        self._peer_reservations: set[str] = set()
        self._local_subscriptions: dict[str, str] = {}
        self._remote_subscriptions: dict[str, _RemoteSubscription] = {}
        self._topic_bindings: dict[str, _TopicBindingState] = {}
        self._seen_order: deque[str] = deque()
        self._seen_ids: set[str] = set()
        self._publications: Queue[MeshPublication | object] = Queue(
            maxsize=self.config.publication_queue_limit
        )
        self._duplicate_publications = 0
        self._last_routing_error: str | None = None
        self._emit(
            MeshLifecycleKind.RUNTIME_STARTING,
            MeshLifecycleReason.STARTUP,
        )
        self._emit(
            MeshLifecycleKind.RUNTIME_READY,
            MeshLifecycleReason.STARTUP,
        )

    def __enter__(self) -> "TransportMesh":
        return self

    def __exit__(self, *error: object) -> None:
        self.close()

    def lifecycle_events(
        self,
        *,
        after_sequence: int = 0,
    ) -> tuple[MeshLifecycleEvent, ...]:
        """Read ordered, bounded, non-durable local lifecycle events."""
        return self._lifecycle.read(after_sequence=after_sequence)

    def lifecycle_health(self) -> MeshLifecycleHealth:
        """Return retention health for the local lifecycle event stream."""
        return self._lifecycle.health()

    def bind_topic(
        self,
        topic: PubSub,
        *,
        policy: MeshTopicPolicy = MeshTopicPolicy.APPEND,
    ) -> MeshTopicBinding:
        """Bind one named ``PubSubTopic`` to durable mesh delivery once."""
        if not isinstance(topic, PubSub):
            raise TypeError("topic must be a PubSub topic")
        if not isinstance(policy, MeshTopicPolicy):
            raise ValueError("policy must be a MeshTopicPolicy")
        resolved_topic = _protocol.require_topic(topic.topic)
        with self._lock:
            self._require_open_locked()
            if self._peers or self._peer_reservations:
                raise MeshRouteError(
                    "durable topics must be bound before mesh peers start"
                )
            if resolved_topic in self._topic_bindings:
                raise MeshRouteError(
                    f"mesh topic {resolved_topic!r} is already bound"
                )
        subscription = self._subscribe(
            resolved_topic,
            subscription_id=self._binding_subscription_id(resolved_topic),
        )
        inbound_context = local()

        def publish(row: StreamRow) -> None:
            if getattr(inbound_context, "active", False):
                return
            payload = row["payload"]
            if not isinstance(payload, bytes | bytearray | memoryview):
                raise MeshRouteError(
                    f"PubSub topic {resolved_topic!r} produced a non-bytes payload"
                )
            correlation_id = self._topic_correlation_id(resolved_topic, row)
            self._publish_bound(
                resolved_topic,
                bytes(payload),
                policy=policy,
                message_id=(
                    None
                    if correlation_id is None
                    else self._correlated_message_id(
                        resolved_topic,
                        correlation_id,
                    )
                ),
                correlation_id=correlation_id,
            )

        callback = topic.subscribe(publish)

        def dispose() -> bool:
            callback_disposed = callback.dispose()
            subscription_disposed = subscription.dispose()
            with self._lock:
                self._topic_bindings.pop(resolved_topic, None)
            return callback_disposed or subscription_disposed

        binding = MeshTopicBinding(topic, policy, dispose)
        with self._lock:
            self._topic_bindings[resolved_topic] = _TopicBindingState(
                topic=topic,
                policy=policy,
                subscription=subscription,
                callback=callback,
                inbound_context=inbound_context,
                binding=binding,
            )
        return binding

    def listen(
        self,
        peer_node_id: str,
        address: TcpAddress | None = None,
        *,
        transport_config: TransportConfig | None = None,
    ) -> TcpAddress:
        """Create and own one reconnecting listener for an expected peer."""
        peer_node_id = _protocol.require_peer_node_id(
            peer_node_id,
            self.identity.node_id,
        )
        config = transport_config or self.listener_config
        if not isinstance(config, TransportConfig):
            raise ValueError("transport_config must be a TransportConfig")
        self._reserve_peer(peer_node_id)
        self._emit(
            MeshLifecycleKind.PEER_CONNECTING,
            MeshLifecycleReason.LISTENER,
            peer_node_id=peer_node_id,
        )
        try:
            transport = TcpTransport.listen(
                self.identity,
                address,
                config=config,
                expected_peer_node_id=peer_node_id,
            )
            self._register_peer(peer_node_id, transport, source="listener")
        except BaseException:
            self._release_reservation(peer_node_id)
            raise
        return transport.address

    def apply_discovery(self, peers: Sequence[PeerDiscovery]) -> None:
        """Replace connector-owned peers with one typed static-discovery snapshot."""
        entries = tuple(peers)
        desired: dict[str, PeerDiscovery] = {}
        for entry in entries:
            if not isinstance(entry, PeerDiscovery):
                raise ValueError("discovery entries must be PeerDiscovery values")
            _protocol.require_peer_node_id(entry.node_id, self.identity.node_id)
            if entry.node_id in desired:
                raise ValueError(f"duplicate discovery peer {entry.node_id!r}")
            desired[entry.node_id] = entry
            self._emit(
                MeshLifecycleKind.PEER_DISCOVERED,
                MeshLifecycleReason.DISCOVERY,
                peer_node_id=entry.node_id,
            )
        with self._lock:
            self._require_open_locked()
            listener_peers = {
                peer_id
                for peer_id, peer in self._peers.items()
                if peer.source == "listener"
            }
            conflict = listener_peers.intersection(desired)
            if conflict:
                raise MeshRouteError(
                    f"discovery peers conflict with listeners {tuple(sorted(conflict))!r}"
                )
            final_count = len(listener_peers) + len(desired)
            if final_count > self.config.max_peers:
                raise MeshCapacityError(
                    f"discovery requires {final_count} peers but max_peers is "
                    f"{self.config.max_peers}"
                )
            current = {
                peer_id: peer
                for peer_id, peer in self._peers.items()
                if peer.source == "discovery"
            }
        for peer_id, peer in current.items():
            entry = desired.get(peer_id)
            if entry is None or entry.address != peer.transport.address:
                self.remove_peer(peer_id)
        for peer_id, entry in desired.items():
            with self._lock:
                existing = self._peers.get(peer_id)
            if existing is not None:
                continue
            self._reserve_peer(peer_id)
            self._emit(
                MeshLifecycleKind.PEER_CONNECTING,
                MeshLifecycleReason.DISCOVERY,
                peer_node_id=peer_id,
            )
            try:
                transport = TcpTransport.connect(
                    self.identity,
                    entry.address,
                    config=entry.transport_config or self.connector_config,
                    expected_peer_node_id=peer_id,
                )
                self._register_peer(peer_id, transport, source="discovery")
            except BaseException:
                self._release_reservation(peer_id)
                raise

    def remove_peer(self, peer_node_id: str) -> bool:
        """Remove one peer, release its routes, and close its owned link."""
        peer_node_id = _protocol.require_text(peer_node_id, "peer node_id")
        with self._lock:
            peer = self._peers.pop(peer_node_id, None)
            if peer is None:
                return False
        peer.delivery.close()
        if peer.reader is not current_thread():
            peer.reader.join(timeout=2.0)
        with self._lock:
            removed = tuple(
                (subscription_id, subscription.topic)
                for subscription_id, subscription in self._remote_subscriptions.items()
                if subscription.next_hop == peer_node_id
            )
            for subscription_id, _ in removed:
                del self._remote_subscriptions[subscription_id]
        self._emit(
            MeshLifecycleKind.PEER_DISCONNECTED,
            MeshLifecycleReason.DISCOVERY,
            peer_node_id=peer_node_id,
        )
        for subscription_id, topic in removed:
            self._fanout_control(
                _protocol.CONTROL_UNSUBSCRIBE,
                _protocol.encode_subscription(subscription_id, topic),
                exclude={peer_node_id},
            )
        self._request_sync(exclude={peer_node_id})
        return True

    def subscribe(self, topic: str) -> MeshSubscription:
        """Register and propagate one bounded local topic subscription."""
        topic = _protocol.require_topic(topic)
        return self._subscribe(topic, subscription_id=uuid4().hex)

    def _subscribe(
        self,
        topic: str,
        *,
        subscription_id: str,
    ) -> MeshSubscription:
        with self._lock:
            self._require_open_locked()
            self._require_subscription_capacity_locked()
            self._local_subscriptions[subscription_id] = topic
        subscription = MeshSubscription(self, subscription_id, topic)
        try:
            self._fanout_control(
                _protocol.CONTROL_SUBSCRIBE,
                _protocol.encode_subscription(subscription_id, topic),
            )
        except MeshBackpressureError as error:
            raise MeshSubscriptionBackpressureError(
                subscription,
                accepted_peers=error.accepted_peers,
                rejected_peers=error.rejected_peers,
            ) from error
        return subscription

    def synchronize(self) -> None:
        """Retry propagation of all subscription state to connected peers."""
        with self._lock:
            self._require_open_locked()
            peer_ids = tuple(self._peers)
        rejected: list[str] = []
        accepted: list[str] = []
        for peer_id in peer_ids:
            try:
                self._synchronize_peer(peer_id)
            except MeshBackpressureError:
                rejected.append(peer_id)
            else:
                accepted.append(peer_id)
        if rejected:
            raise MeshBackpressureError(
                "subscription-sync",
                accepted_peers=accepted,
                rejected_peers=rejected,
            )

    def publish(
        self,
        topic: str,
        payload: bytes | bytearray | memoryview,
        *,
        message_id: str | None = None,
        correlation_id: str | None = None,
    ) -> MeshPublishResult:
        """Publish once to local consumers and interested next-hop peers."""
        topic = _protocol.require_topic(topic)
        payload_bytes = _protocol.require_payload(payload)
        resolved_message_id = (
            uuid4().hex
            if message_id is None
            else _protocol.require_text(message_id, "message_id")
        )
        resolved_correlation_id = (
            None
            if correlation_id is None
            else _protocol.require_text(correlation_id, "correlation_id")
        )
        with self._lock:
            self._require_open_locked()
            if resolved_message_id in self._seen_ids:
                raise MeshRouteError(
                    f"mesh message_id {resolved_message_id!r} was already published"
                )
            local_delivery = topic in self._local_subscriptions.values()
            peer_ids = self._interested_peers_locked(topic)
            if not local_delivery and not peer_ids:
                raise MeshRouteError(f"no mesh subscribers for topic {topic!r}")
            if local_delivery:
                publication = MeshPublication(
                    topic=topic,
                    payload=payload_bytes,
                    message_id=resolved_message_id,
                    correlation_id=resolved_correlation_id,
                    source_node_id=self.identity.node_id,
                    received_at=time(),
                )
                try:
                    self._publications.put_nowait(publication)
                except Full as error:
                    raise MeshBackpressureError(
                        resolved_message_id,
                        accepted_peers=(),
                        rejected_peers=(self.identity.node_id,),
                    ) from error
            self._remember_message_locked(resolved_message_id)
        forwarded = self._fanout_publication(
            topic,
            payload_bytes,
            resolved_message_id,
            source_node_id=self.identity.node_id,
            peer_ids=peer_ids,
            correlation_id=resolved_correlation_id,
        )
        return MeshPublishResult(
            message_id=resolved_message_id,
            delivered_locally=local_delivery,
            forwarded_peers=forwarded,
        )

    def _publish_bound(
        self,
        topic: str,
        payload: bytes,
        *,
        policy: MeshTopicPolicy,
        message_id: str | None = None,
        correlation_id: str | None = None,
    ) -> MeshPublishResult:
        resolved_message_id = uuid4().hex if message_id is None else message_id
        with self._lock:
            self._require_open_locked()
            peer_ids = self._interested_peers_locked(topic)
            self._remember_message_locked(resolved_message_id)
        forwarded = self._fanout_publication(
            topic,
            payload,
            resolved_message_id,
            source_node_id=self.identity.node_id,
            peer_ids=peer_ids,
            policy=policy,
            correlation_id=correlation_id,
        )
        return MeshPublishResult(
            message_id=resolved_message_id,
            delivered_locally=True,
            forwarded_peers=forwarded,
        )

    def receive(self, *, timeout: float | None = None) -> MeshPublication:
        """Receive one locally subscribed publication."""
        _protocol.require_timeout(timeout)
        try:
            publication = self._publications.get(timeout=timeout)
        except Empty as error:
            with self._lock:
                if self._closed:
                    raise MeshClosed("transport mesh is closed") from error
            raise TimeoutError("no mesh publication arrived before timeout") from error
        if publication is _CLOSED_SENTINEL:
            raise MeshClosed("transport mesh is closed")
        if not isinstance(publication, MeshPublication):
            raise MeshRouteError("publication queue contained an invalid value")
        self._publications.task_done()
        return publication

    def peer_health(self) -> tuple[MeshPeerHealth, ...]:
        """Return peer health ordered by node ID."""
        with self._lock:
            peers = tuple(
                (
                    peer_id,
                    peer,
                    tuple(
                        sorted(
                            {
                                subscription.topic
                                for subscription in self._remote_subscriptions.values()
                                if subscription.next_hop == peer_id
                            }
                        )
                    ),
                )
                for peer_id, peer in sorted(self._peers.items())
            )
        return tuple(
            MeshPeerHealth(
                node_id=peer_id,
                source=peer.source,
                address=peer.transport.address,
                link=peer.transport.health(),
                delivery=peer.delivery.health(),
                interested_topics=topics,
                last_routing_error=peer.last_error,
            )
            for peer_id, peer, topics in peers
        )

    def health(self) -> MeshHealth:
        """Return a bounded mesh-level health snapshot."""
        with self._lock:
            connected = sum(
                peer.transport.health().state is LinkState.CONNECTED
                for peer in self._peers.values()
            )
            return MeshHealth(
                is_closed=self._closed,
                peer_count=len(self._peers),
                connected_peers=connected,
                local_subscriptions=len(self._local_subscriptions),
                remote_subscriptions=len(self._remote_subscriptions),
                publications_queued=(
                    0 if self._closed else self._publications.qsize()
                ),
                recent_publications=len(self._seen_ids),
                duplicate_publications=self._duplicate_publications,
                last_routing_error=self._last_routing_error,
            )

    def close(self) -> None:
        """Dispose every link, reader, subscription, and retained payload."""
        with self._lock:
            if self._closed:
                return
            self._emit(
                MeshLifecycleKind.RUNTIME_STOPPING,
                MeshLifecycleReason.SHUTDOWN,
            )
            self._closed = True
            peers = tuple(self._peers.values())
            bindings = tuple(self._topic_bindings.values())
            self._peers.clear()
            self._peer_reservations.clear()
            self._topic_bindings.clear()
            self._local_subscriptions.clear()
            self._remote_subscriptions.clear()
            self._seen_order.clear()
            self._seen_ids.clear()
        for binding in bindings:
            binding.binding.dispose()
        for peer in peers:
            peer.delivery.close(
                graceful_timeout=self.delivery.graceful_shutdown_seconds
            )
        for peer in peers:
            if peer.reader is not current_thread():
                peer.reader.join(timeout=2.0)
        while True:
            try:
                self._publications.get_nowait()
            except Empty:
                break
            self._publications.task_done()
        try:
            self._publications.put_nowait(_CLOSED_SENTINEL)
        except Full:
            pass
        if self._temporary_state is not None:
            self._temporary_state.cleanup()
        self._emit(
            MeshLifecycleKind.RUNTIME_STOPPED,
            MeshLifecycleReason.SHUTDOWN,
        )

    def _reserve_peer(self, peer_node_id: str) -> None:
        with self._lock:
            self._require_open_locked()
            if peer_node_id in self._peers or peer_node_id in self._peer_reservations:
                raise MeshRouteError(f"mesh peer {peer_node_id!r} already exists")
            if (
                len(self._peers) + len(self._peer_reservations)
                >= self.config.max_peers
            ):
                raise MeshCapacityError(
                    f"mesh max_peers limit {self.config.max_peers} is full"
                )
            self._peer_reservations.add(peer_node_id)

    def _release_reservation(self, peer_node_id: str) -> None:
        with self._lock:
            self._peer_reservations.discard(peer_node_id)

    def _register_peer(
        self,
        peer_node_id: str,
        transport: TcpTransport,
        *,
        source: str,
    ) -> None:
        journal_key = sha256(
            (
                f"{self.identity.cluster_id}\0{self.identity.node_id}\0"
                f"{peer_node_id}"
            ).encode()
        ).hexdigest()
        delivery = DurableDelivery(
            transport,
            self.delivery._for_journal(
                self._state_directory / f"peer-{journal_key}.sqlite3",
                transport_max_payload_bytes=transport.config.max_payload_bytes,
            ),
            owns_transport=True,
        )
        reader = Thread(
            target=self._read_peer,
            args=(peer_node_id, delivery),
            name=f"manyfold-mesh-{self.identity.node_id}-{peer_node_id}",
            daemon=True,
        )
        peer = _PeerLink(
            transport=transport,
            delivery=delivery,
            source=source,
            reader=reader,
            last_link_state=LinkState.STARTING,
            last_delivery_health=delivery.health(),
            watermark_crossed=False,
        )
        with self._lock:
            if peer_node_id not in self._peer_reservations:
                delivery.close()
                raise MeshRouteError(
                    f"mesh peer reservation for {peer_node_id!r} was lost"
                )
            self._peer_reservations.remove(peer_node_id)
            self._peers[peer_node_id] = peer
        if peer.last_delivery_health.outbox_items:
            self._emit(
                MeshLifecycleKind.DURABLE_REPLAYED,
                MeshLifecycleReason.RECOVERY,
                peer_node_id=peer_node_id,
                item_count=peer.last_delivery_health.outbox_items,
                byte_count=peer.last_delivery_health.logical_storage_bytes,
            )
        reader.start()

    def _read_peer(
        self,
        peer_node_id: str,
        delivery: DurableDelivery,
    ) -> None:
        transport = delivery.transport
        synchronized_connections = 0
        while True:
            with self._lock:
                peer = self._peers.get(peer_node_id)
                if self._closed or peer is None or peer.transport is not transport:
                    return
            synchronized_connections = self._synchronize_connection(
                peer_node_id,
                transport,
                synchronized_connections,
            )
            self._publish_link_transition(peer_node_id, transport.health().state)
            self._publish_delivery_health(peer_node_id, delivery)
            try:
                received = delivery.receive(timeout=0.1)
            except TimeoutError:
                continue
            except DeliveryClosed:
                return
            synchronized_connections = self._synchronize_connection(
                peer_node_id,
                transport,
                synchronized_connections,
            )
            try:
                self._process_peer_message(peer_node_id, received)
            except MeshCapacityError as error:
                self._record_peer_error(peer_node_id, error)
                delivery.nack(received.message_id, reason=str(error))
                delivery.close()
                return
            except MeshError as error:
                self._record_peer_error(peer_node_id, error)
                delivery.nack(received.message_id, reason=str(error))
            else:
                try:
                    delivery.ack(received.message_id)
                except DeliveryClosed:
                    return
                self._emit(
                    MeshLifecycleKind.DURABLE_ACKED,
                    MeshLifecycleReason.ACKNOWLEDGEMENT,
                    topic=(
                        None
                        if received.message.channel.startswith(
                            _protocol.RESERVED_PREFIX
                        )
                        else received.message.channel
                    ),
                    peer_node_id=peer_node_id,
                    message_id=received.message_id,
                    correlation_id=received.message.correlation_id,
                    attempt=received.delivery_attempt,
                    byte_count=len(received.message.payload),
                )

    def _process_peer_message(
        self,
        peer_node_id: str,
        received: ReceivedDelivery,
    ) -> None:
        message = received.message
        if message.kind is not FrameKind.PUBSUB:
            raise MeshRouteError(
                f"mesh peer {peer_node_id!r} sent non-PubSub frame {message.kind!r}"
            )
        if message.channel == _protocol.CONTROL_SUBSCRIBE:
            subscription_id, topic = self._decode_subscription(message.payload)
            self._accept_remote_subscription(peer_node_id, subscription_id, topic)
            return
        if message.channel == _protocol.CONTROL_UNSUBSCRIBE:
            subscription_id, topic = self._decode_subscription(message.payload)
            self._accept_remote_unsubscribe(peer_node_id, subscription_id, topic)
            return
        if message.channel == _protocol.CONTROL_SYNC:
            self._synchronize_peer(peer_node_id)
            return
        if message.channel.startswith(_protocol.RESERVED_PREFIX):
            raise MeshRouteError(
                f"mesh peer {peer_node_id!r} sent unknown channel {message.channel!r}"
            )
        topic = _protocol.require_topic(message.channel)
        try:
            source_node_id, payload = _protocol.decode_publication(message.payload)
        except ValueError as error:
            raise MeshRouteError(f"publication payload is invalid: {error}") from error
        self._accept_publication(
            peer_node_id,
            source_node_id,
            topic,
            payload,
            received.message_id,
            correlation_id=message.correlation_id,
        )

    def _decode_subscription(self, payload: bytes) -> tuple[str, str]:
        try:
            return _protocol.decode_subscription(payload)
        except (TypeError, ValueError) as error:
            raise MeshRouteError(
                f"subscription control payload is invalid: {error}"
            ) from error

    def _accept_remote_subscription(
        self,
        peer_node_id: str,
        subscription_id: str,
        topic: str,
    ) -> None:
        with self._lock:
            if peer_node_id not in self._peers:
                return
            if subscription_id in self._local_subscriptions:
                return
            existing = self._remote_subscriptions.get(subscription_id)
            if existing is not None:
                return
            self._require_subscription_capacity_locked()
            self._remote_subscriptions[subscription_id] = _RemoteSubscription(
                topic=topic,
                next_hop=peer_node_id,
            )
        self._fanout_control(
            _protocol.CONTROL_SUBSCRIBE,
            _protocol.encode_subscription(subscription_id, topic),
            exclude={peer_node_id},
        )

    def _accept_remote_unsubscribe(
        self,
        peer_node_id: str,
        subscription_id: str,
        topic: str,
    ) -> None:
        with self._lock:
            subscription = self._remote_subscriptions.get(subscription_id)
            if subscription is None or subscription.next_hop != peer_node_id:
                return
            if subscription.topic != topic:
                raise MeshRouteError(
                    f"unsubscribe topic {topic!r} does not match retained "
                    f"topic {subscription.topic!r}"
                )
            del self._remote_subscriptions[subscription_id]
        self._fanout_control(
            _protocol.CONTROL_UNSUBSCRIBE,
            _protocol.encode_subscription(subscription_id, topic),
            exclude={peer_node_id},
        )

    def _accept_publication(
        self,
        peer_node_id: str,
        source_node_id: str,
        topic: str,
        payload: bytes,
        message_id: str,
        *,
        correlation_id: str | None,
    ) -> None:
        with self._lock:
            if message_id in self._seen_ids:
                self._duplicate_publications += 1
                return
            self._remember_message_locked(message_id)
            binding = self._topic_bindings.get(topic)
            binding_subscription_id = (
                None
                if binding is None
                else binding.subscription.subscription_id
            )
            queue_locally = any(
                subscription_topic == topic
                and subscription_id != binding_subscription_id
                for subscription_id, subscription_topic in (
                    self._local_subscriptions.items()
                )
            )
            peer_ids = self._interested_peers_locked(topic, exclude={peer_node_id})
        try:
            self._fanout_publication(
                topic,
                payload,
                message_id,
                source_node_id=source_node_id,
                peer_ids=peer_ids,
                policy=(
                    MeshTopicPolicy.APPEND
                    if binding is None
                    else binding.policy
                ),
                correlation_id=correlation_id,
            )
        except MeshError:
            with self._lock:
                self._forget_message_locked(message_id)
            raise
        if binding is not None:
            binding.inbound_context.active = True
            try:
                binding.topic.publish(payload, key=correlation_id)
            finally:
                binding.inbound_context.active = False
        if queue_locally:
            publication = MeshPublication(
                topic=topic,
                payload=payload,
                message_id=message_id,
                correlation_id=correlation_id,
                source_node_id=source_node_id,
                received_at=time(),
            )
            while True:
                with self._lock:
                    if self._closed:
                        return
                try:
                    self._publications.put(publication, timeout=0.1)
                    break
                except Full:
                    continue

    def _fanout_publication(
        self,
        topic: str,
        payload: bytes,
        message_id: str,
        *,
        source_node_id: str,
        peer_ids: Sequence[str],
        policy: MeshTopicPolicy = MeshTopicPolicy.APPEND,
        correlation_id: str | None = None,
    ) -> tuple[str, ...]:
        message = TransportMessage(
            kind=FrameKind.PUBSUB,
            channel=topic,
            payload=_protocol.encode_publication(source_node_id, payload),
            correlation_id=correlation_id,
        )
        accepted, rejected = self._send_to_peers(
            peer_ids,
            message,
            message_id=message_id,
            topic=topic,
            policy=policy,
        )
        if rejected:
            raise MeshBackpressureError(
                message_id,
                accepted_peers=accepted,
                rejected_peers=rejected,
            )
        return accepted

    def _fanout_control(
        self,
        channel: str,
        payload: bytes,
        *,
        exclude: set[str] | None = None,
    ) -> tuple[str, ...]:
        with self._lock:
            peer_ids = tuple(
                peer_id
                for peer_id in self._peers
                if exclude is None or peer_id not in exclude
            )
        message_id = uuid4().hex
        accepted, rejected = self._send_to_peers(
            peer_ids,
            TransportMessage(
                FrameKind.PUBSUB,
                channel,
                payload,
            ),
            message_id=message_id,
        )
        if rejected:
            raise MeshBackpressureError(
                message_id,
                accepted_peers=accepted,
                rejected_peers=rejected,
            )
        return accepted

    def _send_to_peers(
        self,
        peer_ids: Sequence[str],
        message: TransportMessage,
        *,
        message_id: str,
        topic: str | None = None,
        policy: MeshTopicPolicy = MeshTopicPolicy.APPEND,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        accepted: list[str] = []
        rejected: list[str] = []
        for peer_id in peer_ids:
            with self._lock:
                peer = self._peers.get(peer_id)
            if peer is None:
                rejected.append(peer_id)
                continue
            try:
                peer.delivery.send(message, message_id=message_id)
            except (DeliveryClosed, DeliveryError, DeliveryStorageFull):
                rejected.append(peer_id)
            else:
                accepted.append(peer_id)
                self._emit(
                    MeshLifecycleKind.DURABLE_ENQUEUED,
                    MeshLifecycleReason.LOCAL_PUBLICATION,
                    topic=topic,
                    peer_node_id=peer_id,
                    message_id=message_id,
                    correlation_id=message.correlation_id,
                    item_count=peer.delivery.health().outbox_items,
                    byte_count=len(message.payload),
                    detail=policy.value,
                )
        return tuple(accepted), tuple(rejected)

    def _synchronize_peer(self, peer_node_id: str) -> None:
        with self._lock:
            peer = self._peers.get(peer_node_id)
            if peer is None:
                return
            subscriptions = tuple(self._local_subscriptions.items()) + tuple(
                (
                    subscription_id,
                    subscription.topic,
                )
                for subscription_id, subscription in self._remote_subscriptions.items()
                if subscription.next_hop != peer_node_id
            )
        accepted: list[str] = []
        rejected: list[str] = []
        for subscription_id, topic in subscriptions:
            message = TransportMessage(
                FrameKind.PUBSUB,
                _protocol.CONTROL_SUBSCRIBE,
                _protocol.encode_subscription(subscription_id, topic),
                correlation_id=uuid4().hex,
            )
            sent, failed = self._send_to_peers(
                (peer_node_id,),
                message,
                message_id=message.correlation_id or uuid4().hex,
            )
            accepted.extend(sent)
            rejected.extend(failed)
        if rejected:
            raise MeshBackpressureError(
                "subscription-sync",
                accepted_peers=accepted,
                rejected_peers=rejected,
            )

    def _synchronize_connection(
        self,
        peer_node_id: str,
        transport: TcpTransport,
        synchronized_connections: int,
    ) -> int:
        health = transport.health()
        if (
            health.state is not LinkState.CONNECTED
            or health.connections_established <= synchronized_connections
        ):
            return synchronized_connections
        self._reset_peer_routes(peer_node_id)
        try:
            self._synchronize_peer(peer_node_id)
        except MeshError as error:
            self._record_peer_error(peer_node_id, error)
        return health.connections_established

    def _reset_peer_routes(self, peer_node_id: str) -> None:
        with self._lock:
            removed = tuple(
                (subscription_id, subscription.topic)
                for subscription_id, subscription in self._remote_subscriptions.items()
                if subscription.next_hop == peer_node_id
            )
            for subscription_id, _ in removed:
                del self._remote_subscriptions[subscription_id]
        for subscription_id, topic in removed:
            try:
                self._fanout_control(
                    _protocol.CONTROL_UNSUBSCRIBE,
                    _protocol.encode_subscription(subscription_id, topic),
                    exclude={peer_node_id},
                )
            except MeshBackpressureError as error:
                self._record_error(error)

    def _request_sync(self, *, exclude: set[str]) -> None:
        try:
            self._fanout_control(_protocol.CONTROL_SYNC, b"", exclude=exclude)
        except MeshBackpressureError as error:
            self._record_error(error)

    def _unsubscribe(self, subscription_id: str) -> None:
        with self._lock:
            topic = self._local_subscriptions.get(subscription_id)
            if topic is None or self._closed:
                return
        self._fanout_control(
            _protocol.CONTROL_UNSUBSCRIBE,
            _protocol.encode_subscription(subscription_id, topic),
        )
        with self._lock:
            self._local_subscriptions.pop(subscription_id, None)

    def _interested_peers_locked(
        self,
        topic: str,
        *,
        exclude: set[str] | None = None,
    ) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    subscription.next_hop
                    for subscription in self._remote_subscriptions.values()
                    if subscription.topic == topic
                    and (
                        exclude is None
                        or subscription.next_hop not in exclude
                    )
                }
            )
        )

    def _publish_link_transition(
        self,
        peer_node_id: str,
        state: LinkState,
    ) -> None:
        with self._lock:
            peer = self._peers.get(peer_node_id)
            if peer is None or peer.last_link_state is state:
                return
            previous = peer.last_link_state
            peer.last_link_state = state
        if previous is LinkState.CONNECTED and state is not LinkState.CONNECTED:
            self._emit(
                MeshLifecycleKind.PEER_DISCONNECTED,
                MeshLifecycleReason.LINK_STATE_CHANGED,
                peer_node_id=peer_node_id,
                detail=state.value,
            )
        if state is LinkState.CONNECTED:
            self._emit(
                MeshLifecycleKind.PEER_CONNECTED,
                MeshLifecycleReason.RECONNECT
                if peer.transport.health().connections_established > 1
                else MeshLifecycleReason.LINK_STATE_CHANGED,
                peer_node_id=peer_node_id,
            )
        elif state is LinkState.RECONNECTING:
            self._emit(
                MeshLifecycleKind.PEER_RECONNECTING,
                MeshLifecycleReason.RECONNECT,
                peer_node_id=peer_node_id,
            )

    def _publish_delivery_health(
        self,
        peer_node_id: str,
        delivery: DurableDelivery,
    ) -> None:
        health = delivery.health()
        with self._lock:
            peer = self._peers.get(peer_node_id)
            if peer is None:
                return
            previous = peer.last_delivery_health
            peer.last_delivery_health = health
            crossed = (
                health.outbox_items * 5 >= delivery.config.max_outbox_items * 4
            )
            watermark_changed = crossed != peer.watermark_crossed
            peer.watermark_crossed = crossed
        self._emit_health_delta(
            MeshLifecycleKind.DURABLE_SENT,
            MeshLifecycleReason.LOCAL_PUBLICATION,
            peer_node_id,
            health.frames_sent - previous.frames_sent,
            item_count=health.outbox_items,
        )
        self._emit_health_delta(
            MeshLifecycleKind.DURABLE_RETRY,
            MeshLifecycleReason.RETRY,
            peer_node_id,
            health.retries - previous.retries,
            attempt=health.retries,
            item_count=health.outbox_items,
        )
        self._emit_health_delta(
            MeshLifecycleKind.DURABLE_EXPIRED,
            MeshLifecycleReason.EXPIRY,
            peer_node_id,
            (health.expired_outbox + health.expired_inbox)
            - (previous.expired_outbox + previous.expired_inbox),
            item_count=health.outbox_items + health.pending_inbox_items,
        )
        self._emit_optional_health_delta(
            MeshLifecycleKind.DURABLE_COALESCED,
            MeshLifecycleReason.CAPACITY,
            peer_node_id,
            health,
            previous,
            "coalesced",
        )
        self._emit_optional_health_delta(
            MeshLifecycleKind.DURABLE_DROPPED,
            MeshLifecycleReason.CAPACITY,
            peer_node_id,
            health,
            previous,
            "storage_rejections",
        )
        if health.last_error is not None and health.last_error != previous.last_error:
            self._emit(
                MeshLifecycleKind.DELIVERY_FAILED,
                MeshLifecycleReason.ERROR,
                peer_node_id=peer_node_id,
                item_count=health.outbox_items,
                byte_count=health.logical_storage_bytes,
                detail=health.last_error,
            )
        if watermark_changed:
            self._emit(
                (
                    MeshLifecycleKind.WATERMARK_CROSSED
                    if crossed
                    else MeshLifecycleKind.WATERMARK_RECOVERED
                ),
                MeshLifecycleReason.CAPACITY,
                peer_node_id=peer_node_id,
                item_count=health.outbox_items,
                byte_count=health.logical_storage_bytes,
            )

    def _emit_health_delta(
        self,
        kind: MeshLifecycleKind,
        reason: MeshLifecycleReason,
        peer_node_id: str,
        delta: int,
        *,
        attempt: int | None = None,
        item_count: int | None = None,
    ) -> None:
        if delta <= 0:
            return
        self._emit(
            kind,
            reason,
            peer_node_id=peer_node_id,
            attempt=attempt,
            item_count=item_count if item_count is not None else delta,
        )

    def _emit_optional_health_delta(
        self,
        kind: MeshLifecycleKind,
        reason: MeshLifecycleReason,
        peer_node_id: str,
        health: DeliveryHealth,
        previous: DeliveryHealth,
        field: str,
    ) -> None:
        delta = int(getattr(health, field, 0)) - int(getattr(previous, field, 0))
        self._emit_health_delta(
            kind,
            reason,
            peer_node_id,
            delta,
            item_count=health.outbox_items,
        )

    def _require_subscription_capacity_locked(self) -> None:
        retained = len(self._local_subscriptions) + len(self._remote_subscriptions)
        if retained >= self.config.max_subscriptions:
            raise MeshCapacityError(
                f"mesh max_subscriptions limit {self.config.max_subscriptions} is full"
            )

    def _remember_message_locked(self, message_id: str) -> None:
        if len(self._seen_order) >= self.config.duplicate_window:
            expired = self._seen_order.popleft()
            self._seen_ids.remove(expired)
        self._seen_order.append(message_id)
        self._seen_ids.add(message_id)

    def _forget_message_locked(self, message_id: str) -> None:
        if message_id not in self._seen_ids:
            return
        self._seen_ids.remove(message_id)
        self._seen_order.remove(message_id)

    def _binding_subscription_id(self, topic: str) -> str:
        return sha256(
            (
                f"{self.identity.cluster_id}\0{self.identity.node_id}\0{topic}"
            ).encode()
        ).hexdigest()

    def _topic_correlation_id(
        self,
        topic: str,
        row: StreamRow,
    ) -> str | None:
        message_key = row.get("message_key")
        if message_key is not None:
            return _protocol.require_text(message_key, "PubSub message key")
        if not topic.endswith(".commands"):
            return None
        offset = row.get("offset")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise MeshRouteError(
                f"command topic {topic!r} did not expose a valid topic offset"
            )
        machine_name = topic.removesuffix(".commands")
        return f"{machine_name}:{offset}"

    def _correlated_message_id(self, topic: str, correlation_id: str) -> str:
        return sha256(
            (
                f"{self.identity.cluster_id}\0{self.identity.node_id}\0"
                f"{topic}\0{correlation_id}"
            ).encode()
        ).hexdigest()

    def _require_open_locked(self) -> None:
        if self._closed:
            raise MeshClosed("transport mesh is closed")

    def _record_peer_error(self, peer_node_id: str, error: MeshError) -> None:
        with self._lock:
            peer = self._peers.get(peer_node_id)
            if peer is not None:
                peer.last_error = f"{type(error).__name__}: {error}"
            self._last_routing_error = f"{type(error).__name__}: {error}"

    def _record_error(self, error: MeshError) -> None:
        with self._lock:
            self._last_routing_error = f"{type(error).__name__}: {error}"

    def _emit(
        self,
        kind: MeshLifecycleKind,
        reason: MeshLifecycleReason,
        **fields: object,
    ) -> MeshLifecycleEvent:
        return self._lifecycle.publish(kind, reason, **fields)


@dataclass(slots=True)
class _PeerLink:
    transport: TcpTransport
    delivery: DurableDelivery
    source: str
    reader: Thread
    last_link_state: LinkState
    last_delivery_health: DeliveryHealth
    watermark_crossed: bool
    last_error: str | None = None


@dataclass(frozen=True, slots=True)
class _RemoteSubscription:
    topic: str
    next_hop: str


@dataclass(slots=True)
class _TopicBindingState:
    topic: PubSub
    policy: MeshTopicPolicy
    subscription: MeshSubscription
    callback: PubSubCallbackSubscription
    inbound_context: object
    binding: MeshTopicBinding


__all__ = [
    "DEFAULT_DUPLICATE_WINDOW",
    "DEFAULT_LIFECYCLE_EVENT_LIMIT",
    "DEFAULT_MAX_PEERS",
    "DEFAULT_MAX_SUBSCRIPTIONS",
    "DEFAULT_PUBLICATION_QUEUE_LIMIT",
    "MeshBackpressureError",
    "MeshCapacityError",
    "MeshClosed",
    "MeshConfig",
    "MeshDeliveryConfig",
    "MeshError",
    "MeshHealth",
    "MeshLifecycleEvent",
    "MeshLifecycleHealth",
    "MeshLifecycleKind",
    "MeshLifecycleReason",
    "MeshPeerHealth",
    "MeshPublication",
    "MeshPublishResult",
    "MeshRouteError",
    "MeshSubscription",
    "MeshSubscriptionBackpressureError",
    "MeshTopicBinding",
    "MeshTopicPolicy",
    "PeerDiscovery",
    "TransportMesh",
]
