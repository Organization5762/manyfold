from __future__ import annotations

import json
import socket
import time
import unittest
from collections import deque
from dataclasses import dataclass

from manyfold.architecture.discovery import PeerEndpoint
from manyfold.architecture.membership import (
    AuthenticatedPeerSession,
    MembershipConfig,
    MembershipTable,
    MemberState,
)
from manyfold.architecture.swim import (
    HmacDatagramTransport,
    HmacPeerCredentials,
    HmacTransportConfig,
    ReceivedDatagram,
    SwimClosedError,
    SwimConfig,
    SwimMembership,
    SwimPeerSeed,
    UdpDatagramSocket,
)
from manyfold.architecture.transport import NodeIdentity

_CLUSTER_ID = "cluster-a"


class ArchitectureSwimTransportTests(unittest.TestCase):
    def test_hmac_transport_authenticates_sender_and_rejects_replay_and_tamper(
        self,
    ) -> None:
        clock = _ManualClock()
        network = _DeterministicDatagramNetwork(clock)
        endpoint_a = PeerEndpoint("127.0.0.1", 17001)
        endpoint_b = PeerEndpoint("127.0.0.1", 17002)
        transport_a = _transport(network, endpoint_a, "node-a", ("node-b",))
        transport_b = _transport(network, endpoint_b, "node-b", ("node-a",))
        self.addCleanup(transport_a.close)
        self.addCleanup(transport_b.close)
        network.duplicate_next = True

        transport_b.send(endpoint_a, b"hello", local_incarnation=3)
        received = transport_a.receive()

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].payload, b"hello")
        self.assertEqual(received[0].session.identity.node_id, "node-b")
        self.assertEqual(received[0].session.incarnation, 3)
        self.assertEqual(transport_a.stats.dropped_replay, 1)

        captured = network.last_payload
        envelope = json.loads(captured)
        envelope["mac"] = f"{'0' if envelope['mac'][0] != '0' else '1'}{envelope['mac'][1:]}"
        network.inject(
            endpoint_a,
            ReceivedDatagram(
                json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode(),
                endpoint_b,
            ),
        )

        self.assertEqual(transport_a.receive(), ())
        self.assertEqual(transport_a.stats.dropped_authentication, 1)

        foreign_endpoint = PeerEndpoint("127.0.0.1", 17003)
        foreign = HmacDatagramTransport(
            network.open("foreign-node", foreign_endpoint),
            HmacPeerCredentials(
                NodeIdentity("other-cluster", "node-b"),
                foreign_endpoint,
                _key("node-b"),
                {"node-a": _key("node-a")},
            ),
            config=HmacTransportConfig(max_datagram_bytes=4096),
            identifier_source=_SequenceIdentifiers("foreign"),
        )
        self.addCleanup(foreign.close)
        foreign.send(endpoint_a, b"wrong-cluster", local_incarnation=0)

        self.assertEqual(transport_a.receive(), ())
        self.assertEqual(transport_a.stats.dropped_authentication, 2)

        relay_endpoint = PeerEndpoint("127.0.0.1", 17004)
        relayed = HmacDatagramTransport(
            network.open("relayed-node", relay_endpoint),
            HmacPeerCredentials(
                NodeIdentity(_CLUSTER_ID, "node-b"),
                endpoint_b,
                _key("node-b"),
                {"node-a": _key("node-a")},
            ),
            config=HmacTransportConfig(max_datagram_bytes=4096),
            identifier_source=_SequenceIdentifiers("relayed"),
        )
        self.addCleanup(relayed.close)
        relayed.send(endpoint_a, b"signed-endpoint", local_incarnation=3)

        authenticated = transport_a.receive()
        self.assertEqual(len(authenticated), 1)
        self.assertEqual(authenticated[0].session.endpoint, endpoint_b)

    def test_real_udp_transport_round_trip_uses_the_same_authenticator(self) -> None:
        socket_a = UdpDatagramSocket(_available_udp_endpoint())
        socket_b = UdpDatagramSocket(_available_udp_endpoint())
        endpoint_b = socket_b.local_endpoint
        transport_a = HmacDatagramTransport(
            socket_a,
            _credentials("node-a", socket_a.local_endpoint, ("node-b",)),
            identifier_source=_SequenceIdentifiers("wire-a"),
        )
        transport_b = HmacDatagramTransport(
            socket_b,
            _credentials("node-b", socket_b.local_endpoint, ("node-a",)),
            identifier_source=_SequenceIdentifiers("wire-b"),
        )
        self.addCleanup(transport_a.close)
        self.addCleanup(transport_b.close)

        transport_a.send(endpoint_b, b"production-path", local_incarnation=4)

        received = ()
        for _ in range(100):
            received = transport_b.receive()
            if received:
                break
            time.sleep(0.001)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].payload, b"production-path")
        self.assertEqual(received[0].session.identity.node_id, "node-a")
        self.assertEqual(received[0].session.incarnation, 4)


class ArchitectureSwimProtocolTests(unittest.TestCase):
    def test_seed_is_not_a_member_until_credential_is_validated(self) -> None:
        cluster = _Cluster(
            ("node-a", "node-b"),
            authenticated_bootstrap=False,
        )
        self.addCleanup(cluster.close)

        self.assertIsNone(cluster["node-a"].membership.member("node-b"))
        self.assertEqual(cluster["node-a"].stats.seeds, 1)

        cluster.pump(3)

        self.assertEqual(
            cluster["node-a"].membership.member("node-b").state,
            MemberState.ALIVE,
        )
        self.assertEqual(cluster["node-a"].stats.seeds, 0)

    def test_direct_ping_ack_completes_unique_correlated_probe(self) -> None:
        cluster = _Cluster(("node-a", "node-b"))
        self.addCleanup(cluster.close)

        cluster.pump(3)

        stats = cluster["node-a"].stats
        self.assertEqual(stats.direct_probes, 1)
        self.assertEqual(stats.successful_probes, 1)
        self.assertEqual(stats.pending_probes, 0)
        self.assertEqual(
            cluster["node-a"].membership.member("node-b").state,
            MemberState.ALIVE,
        )

    def test_direct_loss_recovers_through_one_bounded_indirect_helper(self) -> None:
        cluster = _Cluster(("node-a", "node-b", "node-c"))
        self.addCleanup(cluster.close)
        cluster.network.block("node-a", "node-b")
        cluster.network.block("node-b", "node-a")

        cluster.tick("node-a")
        cluster.clock.advance(0.21)
        cluster.tick("node-a")
        cluster.pump(5)

        stats = cluster["node-a"].stats
        self.assertEqual(stats.indirect_probes, 1)
        self.assertEqual(stats.successful_probes, 1)
        self.assertEqual(stats.failed_probes, 0)
        self.assertEqual(stats.pending_probes, 0)

    def test_partition_moves_peer_from_suspect_to_dead(self) -> None:
        cluster = _Cluster(("node-a", "node-b"), suspect_seconds=0.5)
        self.addCleanup(cluster.close)
        cluster.network.partition("node-a", "node-b")

        cluster.tick("node-a")
        cluster.clock.advance(0.21)
        cluster.tick("node-a")
        self.assertEqual(
            cluster["node-a"].membership.member("node-b").state,
            MemberState.SUSPECT,
        )

        cluster.clock.advance(0.5)
        cluster.tick("node-a")
        self.assertEqual(
            cluster["node-a"].membership.member("node-b").state,
            MemberState.DEAD,
        )

    def test_delayed_direct_path_can_succeed_indirectly_before_deadline(self) -> None:
        cluster = _Cluster(("node-a", "node-b", "node-c"))
        self.addCleanup(cluster.close)
        cluster.network.delay("node-a", "node-b", 1.0)
        cluster.network.delay("node-b", "node-a", 1.0)

        cluster.tick("node-a")
        cluster.clock.advance(0.21)
        cluster.tick("node-a")
        cluster.pump(5)

        self.assertEqual(cluster["node-a"].stats.successful_probes, 1)
        self.assertEqual(cluster["node-a"].stats.failed_probes, 0)

    def test_suspected_node_self_refutes_and_recovers_after_partition(self) -> None:
        cluster = _Cluster(
            ("node-a", "node-b", "node-c"),
            suspect_seconds=2.0,
        )
        self.addCleanup(cluster.close)
        cluster.network.partition("node-a", "node-b")
        cluster.network.partition("node-b", "node-c")

        cluster.tick("node-a")
        cluster.clock.advance(0.21)
        cluster.tick("node-a")
        cluster.pump(2)
        cluster.clock.advance(0.31)
        cluster.tick("node-a")
        self.assertEqual(
            cluster["node-a"].membership.member("node-b").state,
            MemberState.SUSPECT,
        )

        cluster.network.heal("node-a", "node-b")
        cluster.network.heal("node-b", "node-c")
        cluster.clock.advance(0.5)
        cluster.pump(12)
        cluster.clock.advance(0.5)
        cluster.pump(12)

        self.assertEqual(
            cluster["node-b"].membership.local_record.incarnation,
            1,
        )
        self.assertEqual(
            cluster["node-a"].membership.member("node-b").state,
            MemberState.ALIVE,
        )
        self.assertEqual(
            cluster["node-a"].membership.member("node-b").incarnation,
            1,
        )

    def test_two_node_cluster_reprobes_suspect_for_self_refutation(self) -> None:
        cluster = _Cluster(("node-a", "node-b"), suspect_seconds=2.0)
        self.addCleanup(cluster.close)
        cluster.network.partition("node-a", "node-b")

        cluster.tick("node-a")
        cluster.clock.advance(0.21)
        cluster.tick("node-a")
        self.assertEqual(
            cluster["node-a"].membership.member("node-b").state,
            MemberState.SUSPECT,
        )

        cluster.network.heal("node-a", "node-b")
        cluster.clock.advance(0.79)
        cluster.tick("node-a")
        cluster.tick("node-b")
        cluster.tick("node-a")

        self.assertEqual(
            cluster["node-a"].membership.member("node-b").state,
            MemberState.ALIVE,
        )
        self.assertEqual(
            cluster["node-a"].membership.member("node-b").incarnation,
            1,
        )

    def test_duplicate_request_is_idempotent_and_transport_replay_is_bounded(
        self,
    ) -> None:
        cluster = _Cluster(
            ("node-a", "node-b"),
            replay_limit=8,
            seen_limit=8,
        )
        self.addCleanup(cluster.close)
        cluster.network.duplicate_next = True

        cluster.tick("node-a")
        cluster.pump(3)

        transport_b = cluster.transports["node-b"]
        self.assertEqual(transport_b.stats.dropped_replay, 1)
        self.assertLessEqual(transport_b.stats.replay_messages, 8)
        self.assertLessEqual(cluster["node-b"].stats.seen_requests, 8)

        duplicate_ping = _protocol_message(
            "ping",
            probe_id="same-protocol-request",
            updates=(),
        )
        for _ in range(2):
            cluster.transports["node-a"].send(
                cluster.endpoints["node-b"],
                duplicate_ping,
                local_incarnation=0,
            )
        cluster.tick("node-b")

        self.assertEqual(cluster["node-b"].stats.duplicate_requests, 1)

    def test_explicit_leave_is_disseminated_and_cannot_be_undone_by_stale_alive(
        self,
    ) -> None:
        cluster = _Cluster(("node-a", "node-b"))
        self.addCleanup(cluster.close)

        self.assertTrue(cluster["node-b"].leave())
        cluster.tick("node-a")

        record = cluster["node-a"].membership.member("node-b")
        self.assertEqual(record.state, MemberState.LEFT)
        cluster.transports["node-b"].send(
            cluster.endpoints["node-a"],
            _protocol_message(
                "ping",
                probe_id="stale-after-leave",
                updates=(
                    _wire_update(
                        "node-b",
                        cluster.endpoints["node-b"],
                        incarnation=0,
                        state=MemberState.ALIVE,
                    ),
                ),
            ),
            local_incarnation=0,
        )
        cluster.tick("node-a")

        self.assertEqual(
            cluster["node-a"].membership.member("node-b").state,
            MemberState.LEFT,
        )

    def test_long_running_loss_keeps_every_protocol_collection_hard_bounded(
        self,
    ) -> None:
        cluster = _Cluster(
            ("node-a", "node-b", "node-c"),
            replay_limit=16,
            seen_limit=12,
            pending_limit=2,
            relay_limit=2,
            dissemination_limit=3,
            network_queue_limit=32,
        )
        self.addCleanup(cluster.close)
        cluster.network.partition("node-a", "node-b")

        for _ in range(2_000):
            cluster.tick("node-a")
            cluster.tick("node-c")
            cluster.clock.advance(0.05)
            cluster.tick("node-b")

        for node_id in cluster.node_ids:
            stats = cluster[node_id].stats
            transport_stats = cluster.transports[node_id].stats
            self.assertLessEqual(stats.pending_probes, 2)
            self.assertLessEqual(stats.pending_relays, 2)
            self.assertLessEqual(stats.seen_requests, 12)
            self.assertLessEqual(stats.dissemination_updates, 3)
            self.assertLessEqual(transport_stats.replay_messages, 16)
            self.assertLessEqual(cluster.network.queued(node_id), 32)

    def test_close_cancels_and_releases_owned_state(self) -> None:
        cluster = _Cluster(("node-a", "node-b"))
        engine = cluster["node-a"]
        engine.tick()

        self.assertTrue(engine.close())
        self.assertFalse(engine.close())
        self.assertEqual(engine.stats.pending_probes, 0)
        self.assertEqual(engine.stats.pending_relays, 0)
        self.assertEqual(engine.stats.seen_requests, 0)
        self.assertEqual(engine.stats.dissemination_updates, 0)
        self.assertTrue(engine.membership.is_closed)
        with self.assertRaises(SwimClosedError):
            engine.tick()

        cluster.close()


def _transport(
    network: _DeterministicDatagramNetwork,
    endpoint: PeerEndpoint,
    node_id: str,
    peers: tuple[str, ...],
) -> HmacDatagramTransport:
    return HmacDatagramTransport(
        network.open(node_id, endpoint),
        _credentials(node_id, endpoint, peers),
        config=HmacTransportConfig(max_datagram_bytes=4096),
        identifier_source=_SequenceIdentifiers(f"wire-{node_id}"),
    )


def _credentials(
    node_id: str,
    advertised_endpoint: PeerEndpoint,
    peers: tuple[str, ...],
) -> HmacPeerCredentials:
    return HmacPeerCredentials(
        NodeIdentity(_CLUSTER_ID, node_id),
        advertised_endpoint,
        _key(node_id),
        {peer: _key(peer) for peer in peers},
    )


def _key(node_id: str) -> bytes:
    return node_id.encode().ljust(32, b"-")


def _available_udp_endpoint() -> PeerEndpoint:
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    finally:
        probe.close()
    return PeerEndpoint("127.0.0.1", port)


def _wire_update(
    node_id: str,
    endpoint: PeerEndpoint,
    *,
    incarnation: int,
    state: MemberState,
) -> dict[str, object]:
    return {
        "cluster": _CLUSTER_ID,
        "endpoint": {"host": endpoint.host, "port": endpoint.port},
        "incarnation": incarnation,
        "node": node_id,
        "state": state.value,
    }


def _protocol_message(
    message_type: str,
    *,
    probe_id: str,
    updates: tuple[dict[str, object], ...],
) -> bytes:
    return json.dumps(
        {
            "probe_id": probe_id,
            "type": message_type,
            "updates": updates,
            "version": 1,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


@dataclass(frozen=True)
class _QueuedDatagram:
    deliver_at: float
    datagram: ReceivedDatagram


class _ManualClock:
    def __init__(self) -> None:
        self.current = 0.0

    def now(self) -> float:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += seconds


class _SequenceIdentifiers:
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.sequence = 0

    def next_id(self) -> str:
        self.sequence += 1
        return f"{self.prefix}-{self.sequence}"


class _SortedRandom:
    def sample(self, values: tuple[str, ...] | list[str], count: int) -> tuple[str, ...]:
        return tuple(sorted(values)[:count])


class _DeterministicDatagramNetwork:
    def __init__(
        self,
        clock: _ManualClock,
        *,
        queue_limit: int = 256,
    ) -> None:
        self.clock = clock
        self.queue_limit = queue_limit
        self._node_by_endpoint: dict[PeerEndpoint, str] = {}
        self._inboxes: dict[PeerEndpoint, deque[_QueuedDatagram]] = {}
        self._blocked: set[tuple[str, str]] = set()
        self._delays: dict[tuple[str, str], float] = {}
        self.duplicate_next = False
        self.last_payload = b""

    def open(self, node_id: str, endpoint: PeerEndpoint) -> "_NetworkSocket":
        self._node_by_endpoint[endpoint] = node_id
        self._inboxes[endpoint] = deque(maxlen=self.queue_limit)
        return _NetworkSocket(self, endpoint)

    def route(
        self,
        source: PeerEndpoint,
        destination: PeerEndpoint,
        payload: bytes,
    ) -> None:
        self.last_payload = payload
        source_node = self._node_by_endpoint[source]
        destination_node = self._node_by_endpoint.get(destination)
        if destination_node is None:
            return
        link = (source_node, destination_node)
        if link in self._blocked:
            return
        queued = _QueuedDatagram(
            self.clock.now() + self._delays.get(link, 0.0),
            ReceivedDatagram(payload, source),
        )
        self._inboxes[destination].append(queued)
        if self.duplicate_next:
            self._inboxes[destination].append(queued)
            self.duplicate_next = False

    def receive(
        self,
        endpoint: PeerEndpoint,
        *,
        limit: int,
    ) -> tuple[ReceivedDatagram, ...]:
        inbox = self._inboxes[endpoint]
        ready: list[ReceivedDatagram] = []
        retained: deque[_QueuedDatagram] = deque(maxlen=self.queue_limit)
        while inbox:
            queued = inbox.popleft()
            if queued.deliver_at <= self.clock.now() and len(ready) < limit:
                ready.append(queued.datagram)
            else:
                retained.append(queued)
        inbox.extend(retained)
        return tuple(ready)

    def inject(
        self,
        destination: PeerEndpoint,
        datagram: ReceivedDatagram,
    ) -> None:
        self._inboxes[destination].append(
            _QueuedDatagram(self.clock.now(), datagram)
        )

    def block(self, source_node: str, destination_node: str) -> None:
        self._blocked.add((source_node, destination_node))

    def partition(self, first_node: str, second_node: str) -> None:
        self.block(first_node, second_node)
        self.block(second_node, first_node)

    def heal(self, first_node: str, second_node: str) -> None:
        self._blocked.discard((first_node, second_node))
        self._blocked.discard((second_node, first_node))

    def delay(
        self,
        source_node: str,
        destination_node: str,
        seconds: float,
    ) -> None:
        self._delays[(source_node, destination_node)] = seconds

    def queued(self, node_id: str) -> int:
        endpoint = next(
            endpoint
            for endpoint, registered_node in self._node_by_endpoint.items()
            if registered_node == node_id
        )
        return len(self._inboxes[endpoint])


class _NetworkSocket:
    def __init__(
        self,
        network: _DeterministicDatagramNetwork,
        endpoint: PeerEndpoint,
    ) -> None:
        self.network = network
        self.endpoint = endpoint
        self.closed = False

    def send(self, payload: bytes, endpoint: PeerEndpoint) -> None:
        if self.closed:
            raise RuntimeError("deterministic datagram socket is closed")
        self.network.route(self.endpoint, endpoint, payload)

    def receive(
        self,
        *,
        max_datagrams: int,
        max_bytes: int,
    ) -> tuple[ReceivedDatagram, ...]:
        del max_bytes
        if self.closed:
            raise RuntimeError("deterministic datagram socket is closed")
        return self.network.receive(self.endpoint, limit=max_datagrams)

    def close(self) -> bool:
        if self.closed:
            return False
        self.closed = True
        return True


class _Cluster:
    def __init__(
        self,
        node_ids: tuple[str, ...],
        *,
        suspect_seconds: float = 0.5,
        replay_limit: int = 64,
        seen_limit: int = 64,
        pending_limit: int = 4,
        relay_limit: int = 8,
        dissemination_limit: int = 8,
        network_queue_limit: int = 256,
        authenticated_bootstrap: bool = True,
    ) -> None:
        self.node_ids = node_ids
        self.clock = _ManualClock()
        self.network = _DeterministicDatagramNetwork(
            self.clock,
            queue_limit=network_queue_limit,
        )
        self.endpoints = {
            node_id: PeerEndpoint("127.0.0.1", 18000 + index)
            for index, node_id in enumerate(node_ids)
        }
        self.transports: dict[str, HmacDatagramTransport] = {}
        self.engines: dict[str, SwimMembership] = {}
        for node_id in node_ids:
            transport = HmacDatagramTransport(
                self.network.open(node_id, self.endpoints[node_id]),
                _credentials(
                    node_id,
                    self.endpoints[node_id],
                    tuple(peer for peer in node_ids if peer != node_id),
                ),
                config=HmacTransportConfig(
                    max_datagram_bytes=4096,
                    max_inbound_datagrams=64,
                    max_replay_messages=replay_limit,
                ),
                identifier_source=_SequenceIdentifiers(f"wire-{node_id}"),
            )
            membership = MembershipTable(
                NodeIdentity(_CLUSTER_ID, node_id),
                self.endpoints[node_id],
                config=MembershipConfig(
                    lease_seconds=60,
                    suspect_seconds=suspect_seconds,
                    dead_retention_seconds=2,
                    max_members=max(4, len(node_ids)),
                    max_changes=64,
                ),
                clock=self.clock,
            )
            engine = SwimMembership(
                membership,
                transport,
                config=SwimConfig(
                    probe_interval_seconds=1,
                    ping_timeout_seconds=0.2,
                    indirect_timeout_seconds=0.3,
                    helper_count=1,
                    max_pending_probes=pending_limit,
                    max_pending_relays=relay_limit,
                    max_seen_requests=seen_limit,
                    max_dissemination_updates=dissemination_limit,
                    max_piggyback_updates=4,
                    retransmit_limit=3,
                    max_message_bytes=1200,
                ),
                clock=self.clock,
                random_source=_SortedRandom(),
                identifier_source=_SequenceIdentifiers(f"probe-{node_id}"),
            )
            self.transports[node_id] = transport
            self.engines[node_id] = engine
        for local_node_id, engine in self.engines.items():
            for peer_node_id in node_ids:
                if peer_node_id == local_node_id:
                    continue
                if authenticated_bootstrap:
                    engine.add_peer(
                        AuthenticatedPeerSession(
                            NodeIdentity(_CLUSTER_ID, peer_node_id),
                            self.endpoints[peer_node_id],
                            0,
                        )
                    )
                else:
                    engine.add_seed(
                        SwimPeerSeed(
                            NodeIdentity(_CLUSTER_ID, peer_node_id),
                            self.endpoints[peer_node_id],
                        )
                    )

    def __getitem__(self, node_id: str) -> SwimMembership:
        return self.engines[node_id]

    def tick(self, *node_ids: str) -> None:
        for node_id in node_ids:
            self.engines[node_id].tick()

    def pump(self, rounds: int) -> None:
        for _ in range(rounds):
            for node_id in self.node_ids:
                self.engines[node_id].tick()

    def close(self) -> None:
        for engine in self.engines.values():
            engine.close()


if __name__ == "__main__":
    unittest.main()
