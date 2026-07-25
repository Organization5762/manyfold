"""Bounded multi-peer PubSub routing over concrete TCP transports."""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from queue import Empty, Full, Queue
from threading import RLock, Thread, current_thread
from time import time
from typing import final
from uuid import uuid4

from . import _transport_mesh_protocol as _protocol
from .transport import (
    FrameKind,
    LinkHealth,
    LinkState,
    NodeIdentity,
    TcpAddress,
    TcpTransport,
    TransportClosed,
    TransportConfig,
    TransportMessage,
    TransportQueueFull,
)

DEFAULT_MAX_PEERS = 32
DEFAULT_MAX_SUBSCRIPTIONS = 4096
DEFAULT_DUPLICATE_WINDOW = 8192
DEFAULT_PUBLICATION_QUEUE_LIMIT = 1024

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

    def __post_init__(self) -> None:
        _protocol.require_positive_integer(self.max_peers, "max_peers")
        _protocol.require_positive_integer(self.max_subscriptions, "max_subscriptions")
        _protocol.require_positive_integer(self.duplicate_window, "duplicate_window")
        _protocol.require_positive_integer(
            self.publication_queue_limit,
            "publication_queue_limit",
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
        self.identity = identity
        self.connector_config = connector_config
        self.listener_config = listener_config or connector_config
        self.config = config or MeshConfig()
        self._lock = RLock()
        self._closed = False
        self._peers: dict[str, _PeerLink] = {}
        self._peer_reservations: set[str] = set()
        self._local_subscriptions: dict[str, str] = {}
        self._remote_subscriptions: dict[str, _RemoteSubscription] = {}
        self._seen_order: deque[str] = deque()
        self._seen_ids: set[str] = set()
        self._publications: Queue[MeshPublication | object] = Queue(
            maxsize=self.config.publication_queue_limit
        )
        self._duplicate_publications = 0
        self._last_routing_error: str | None = None

    def __enter__(self) -> "TransportMesh":
        return self

    def __exit__(self, *error: object) -> None:
        self.close()

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
            removed = tuple(
                (subscription_id, subscription.topic)
                for subscription_id, subscription in self._remote_subscriptions.items()
                if subscription.next_hop == peer_node_id
            )
            for subscription_id, _ in removed:
                del self._remote_subscriptions[subscription_id]
        peer.transport.close()
        if peer.reader is not current_thread():
            peer.reader.join(timeout=2.0)
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
        subscription_id = uuid4().hex
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
    ) -> MeshPublishResult:
        """Publish once to local consumers and interested next-hop peers."""
        topic = _protocol.require_topic(topic)
        payload_bytes = _protocol.require_payload(payload)
        resolved_message_id = (
            uuid4().hex
            if message_id is None
            else _protocol.require_text(message_id, "message_id")
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
        )
        return MeshPublishResult(
            message_id=resolved_message_id,
            delivered_locally=local_delivery,
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
            self._closed = True
            peers = tuple(self._peers.values())
            self._peers.clear()
            self._peer_reservations.clear()
            self._local_subscriptions.clear()
            self._remote_subscriptions.clear()
            self._seen_order.clear()
            self._seen_ids.clear()
        for peer in peers:
            peer.transport.close()
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
        reader = Thread(
            target=self._read_peer,
            args=(peer_node_id, transport),
            name=f"manyfold-mesh-{self.identity.node_id}-{peer_node_id}",
            daemon=True,
        )
        peer = _PeerLink(
            transport=transport,
            source=source,
            reader=reader,
        )
        with self._lock:
            if peer_node_id not in self._peer_reservations:
                transport.close()
                raise MeshRouteError(
                    f"mesh peer reservation for {peer_node_id!r} was lost"
                )
            self._peer_reservations.remove(peer_node_id)
            self._peers[peer_node_id] = peer
        reader.start()

    def _read_peer(self, peer_node_id: str, transport: TcpTransport) -> None:
        synchronized_connections = 0
        while True:
            with self._lock:
                peer = self._peers.get(peer_node_id)
                if self._closed or peer is None or peer.transport is not transport:
                    return
            health = transport.health()
            if (
                health.state is LinkState.CONNECTED
                and health.connections_established > synchronized_connections
            ):
                try:
                    self._synchronize_peer(peer_node_id)
                except MeshError as error:
                    self._record_peer_error(peer_node_id, error)
                synchronized_connections = health.connections_established
            try:
                message = transport.receive(timeout=0.1)
            except TimeoutError:
                continue
            except TransportClosed:
                return
            try:
                self._process_peer_message(peer_node_id, message)
            except MeshCapacityError as error:
                self._record_peer_error(peer_node_id, error)
                transport.close()
                return
            except MeshError as error:
                self._record_peer_error(peer_node_id, error)

    def _process_peer_message(
        self,
        peer_node_id: str,
        message: TransportMessage,
    ) -> None:
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
        if not message.channel.startswith(_protocol.PUBLICATION_PREFIX):
            raise MeshRouteError(
                f"mesh peer {peer_node_id!r} sent unknown channel {message.channel!r}"
            )
        if message.correlation_id is None:
            raise MeshRouteError("mesh publication is missing message_id")
        try:
            source_node_id, topic = _protocol.decode_publication_channel(
                message.channel
            )
        except ValueError as error:
            raise MeshRouteError(f"publication channel is invalid: {error}") from error
        self._accept_publication(
            peer_node_id,
            source_node_id,
            topic,
            message.payload,
            message.correlation_id,
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
    ) -> None:
        with self._lock:
            if message_id in self._seen_ids:
                self._duplicate_publications += 1
                return
            self._remember_message_locked(message_id)
            deliver_locally = topic in self._local_subscriptions.values()
            peer_ids = self._interested_peers_locked(topic, exclude={peer_node_id})
        if deliver_locally:
            publication = MeshPublication(
                topic=topic,
                payload=payload,
                message_id=message_id,
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
        self._fanout_publication(
            topic,
            payload,
            message_id,
            source_node_id=source_node_id,
            peer_ids=peer_ids,
        )

    def _fanout_publication(
        self,
        topic: str,
        payload: bytes,
        message_id: str,
        *,
        source_node_id: str,
        peer_ids: Sequence[str],
    ) -> tuple[str, ...]:
        message = TransportMessage(
            kind=FrameKind.PUBSUB,
            channel=_protocol.encode_publication_channel(source_node_id, topic),
            payload=payload,
            correlation_id=message_id,
        )
        accepted, rejected = self._send_to_peers(peer_ids, message)
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
                correlation_id=message_id,
            ),
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
                peer.transport.send(message)
            except (TransportClosed, TransportQueueFull):
                rejected.append(peer_id)
            else:
                accepted.append(peer_id)
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
            sent, failed = self._send_to_peers((peer_node_id,), message)
            accepted.extend(sent)
            rejected.extend(failed)
        if rejected:
            raise MeshBackpressureError(
                "subscription-sync",
                accepted_peers=accepted,
                rejected_peers=rejected,
            )

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


@dataclass(slots=True)
class _PeerLink:
    transport: TcpTransport
    source: str
    reader: Thread
    last_error: str | None = None


@dataclass(frozen=True, slots=True)
class _RemoteSubscription:
    topic: str
    next_hop: str


__all__ = [
    "DEFAULT_DUPLICATE_WINDOW",
    "DEFAULT_MAX_PEERS",
    "DEFAULT_MAX_SUBSCRIPTIONS",
    "DEFAULT_PUBLICATION_QUEUE_LIMIT",
    "MeshBackpressureError",
    "MeshCapacityError",
    "MeshClosed",
    "MeshConfig",
    "MeshError",
    "MeshHealth",
    "MeshPeerHealth",
    "MeshPublication",
    "MeshPublishResult",
    "MeshRouteError",
    "MeshSubscription",
    "MeshSubscriptionBackpressureError",
    "PeerDiscovery",
    "TransportMesh",
]
