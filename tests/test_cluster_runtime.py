from __future__ import annotations

import socket
import tempfile
import time
import unittest
from collections.abc import Callable
from pathlib import Path

from manyfold.architecture import (
    CompositeDiscovery,
    MembershipConfig,
    MembershipTable,
    MemberState,
    NodeIdentity,
    PeerEndpoint,
    StaticSeedDiscovery,
    TcpAddress,
    TcpTransport,
)
from manyfold.cluster import (
    DevelopmentCluster,
    LocalDevelopmentTransportSecurityProvider,
    NodeConfig,
    NodePhase,
    NodeRuntime,
    NodeStartError,
)


class ClusterRuntimeIntegrationTests(unittest.TestCase):
    def test_single_node_cold_start_runs_real_local_control_plane(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cluster = DevelopmentCluster.create(Path(directory) / "control")
            node = NodeRuntime(
                _node_config(
                    "node-a",
                    _reserve_port(),
                    (),
                    development_cluster=cluster,
                )
            )
            try:
                started = node.start()

                self.assertIs(started, node)
                self.assertEqual(node.phase, NodePhase.READY)
                self.assertIsInstance(node.listener, TcpTransport)
                self.assertIsInstance(node.membership, MembershipTable)
                self.assertEqual(
                    [member.identity.node_id for member in node.snapshot().members],
                    ["node-a"],
                )
                self.assertTrue(
                    all(
                        cluster.process_id(member.node_id) is not None
                        for member in cluster.members
                    )
                )
                self.assertIn(
                    "node-ready-local",
                    {diagnostic.code for diagnostic in node.snapshot().diagnostics},
                )
                self.assertIn(
                    NodePhase.DISCOVERING,
                    {diagnostic.phase for diagnostic in node.snapshot().diagnostics},
                )
            finally:
                node.stop()

            self.assertTrue(
                all(
                    cluster.process_id(member.node_id) is None
                    for member in cluster.members
                )
            )

    def test_second_node_discovers_authenticates_and_joins(self) -> None:
        first_port, second_port = _reserve_ports(2)
        first = NodeRuntime(
            _node_config(
                "node-a",
                first_port,
                (PeerEndpoint("127.0.0.1", second_port),),
            )
        )
        second = NodeRuntime(
            _node_config(
                "node-b",
                second_port,
                (PeerEndpoint("127.0.0.1", first_port),),
            )
        )
        try:
            first.start()
            self.assertEqual(first.phase, NodePhase.DEGRADED)
            self.assertIn(
                "node-degraded",
                {diagnostic.code for diagnostic in first.snapshot().diagnostics},
            )

            second.start()
            first_members = first.wait_for_members(2, timeout=3.0)
            second_members = second.wait_for_members(2, timeout=3.0)

            self.assertEqual(
                {member.identity.node_id for member in first_members},
                {"node-a", "node-b"},
            )
            self.assertEqual(
                {member.identity.node_id for member in second_members},
                {"node-a", "node-b"},
            )
            self.assertTrue(
                _wait_until(
                    lambda: (
                        first.phase is NodePhase.READY
                        and second.phase is NodePhase.READY
                    ),
                    timeout=2.0,
                )
            )
            diagnostic_phases = {
                diagnostic.phase for diagnostic in second.snapshot().diagnostics
            }
            self.assertIn(NodePhase.AUTHENTICATING, diagnostic_phases)
            self.assertIn(NodePhase.JOINING, diagnostic_phases)
        finally:
            second.stop()
            first.stop()

    def test_duplicate_start_and_stop_are_idempotent(self) -> None:
        node = NodeRuntime(_node_config("node-a", _reserve_port(), ()))
        try:
            self.assertIs(node.start(), node)
            listener = node.listener
            membership = node.membership

            self.assertIs(node.start(), node)
            self.assertIs(node.listener, listener)
            self.assertIs(node.membership, membership)
            self.assertTrue(node.stop())
            self.assertFalse(node.stop())
            self.assertEqual(node.snapshot().diagnostics[-1].phase, NodePhase.STOPPED)
        finally:
            node.stop()

    def test_discovered_self_endpoint_still_becomes_local_ready(self) -> None:
        port = _reserve_port()
        node = NodeRuntime(
            _node_config(
                "node-a",
                port,
                (PeerEndpoint("localhost", port),),
            )
        )
        try:
            node.start()

            self.assertEqual(node.phase, NodePhase.READY)
            self.assertEqual(node.peer_transports, ())
            self.assertEqual(len(node.snapshot().members), 1)
        finally:
            node.stop()

    def test_peer_loss_and_recovery_reconcile_without_reinitialization(self) -> None:
        first_port, second_port = _reserve_ports(2)
        first = NodeRuntime(
            _node_config(
                "node-a",
                first_port,
                (PeerEndpoint("127.0.0.1", second_port),),
            )
        )
        second = NodeRuntime(
            _node_config(
                "node-b",
                second_port,
                (PeerEndpoint("127.0.0.1", first_port),),
            )
        )
        try:
            first.start()
            second.start()
            first.wait_for_members(2, timeout=3.0)
            second.wait_for_members(2, timeout=3.0)

            second.stop()
            self.assertTrue(
                _wait_until(
                    lambda: (
                        (member := first.membership.member("node-b")) is not None
                        and member.state is not MemberState.ALIVE
                    ),
                    timeout=3.0,
                )
            )
            self.assertTrue(first.wait_for_phase(NodePhase.DEGRADED, timeout=2.0))

            second.start()
            recovered = first.wait_for_members(2, timeout=4.0)

            self.assertEqual(
                first.membership.member("node-b").state,
                MemberState.ALIVE,
            )
            self.assertEqual(len(recovered), 2)
            self.assertIn(
                "peer-recovered",
                {diagnostic.code for diagnostic in first.snapshot().diagnostics},
            )
        finally:
            second.stop()
            first.stop()

    def test_clean_restart_recreates_owned_runtime_resources(self) -> None:
        node = NodeRuntime(_node_config("node-a", _reserve_port(), ()))
        try:
            node.start()
            first_listener = node.listener
            first_membership = node.membership
            endpoint = node.endpoint
            node.stop()

            node.start()

            self.assertEqual(node.phase, NodePhase.READY)
            self.assertEqual(node.endpoint, endpoint)
            self.assertIsNot(node.listener, first_listener)
            self.assertIsNot(node.membership, first_membership)
            self.assertEqual(len(node.snapshot().members), 1)
        finally:
            node.stop()

    def test_partial_startup_rolls_back_real_listener_and_control_plane(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cluster = DevelopmentCluster.create(Path(directory) / "control")
            blocked_member = cluster.members[0]
            blocked_state = cluster.state_directory(blocked_member.node_id)
            blocked_state.mkdir(parents=True)
            (blocked_state / "identity.json").write_text(
                "{}",
                encoding="utf-8",
            )
            node = NodeRuntime(
                _node_config(
                    "node-a",
                    _reserve_port(),
                    (),
                    development_cluster=cluster,
                )
            )
            try:
                with self.assertRaisesRegex(NodeStartError, "could not start"):
                    node.start()
            finally:
                node.stop()

            self.assertEqual(node.phase, NodePhase.STOPPED)
            self.assertIsNone(node.listener)
            self.assertIsNone(node.membership)
            self.assertTrue(
                all(
                    cluster.process_id(member.node_id) is None
                    for member in cluster.members
                )
            )
            self.assertIn(
                "startup-rolled-back",
                {diagnostic.code for diagnostic in node.snapshot().diagnostics},
            )

    def test_peer_and_diagnostic_retention_are_hard_bounded(self) -> None:
        ports = _reserve_ports(4)
        node = NodeRuntime(
            _node_config(
                "node-a",
                ports[0],
                tuple(PeerEndpoint("127.0.0.1", port) for port in ports[1:]),
                max_peers=2,
                diagnostic_limit=4,
            )
        )
        try:
            node.start()

            self.assertLessEqual(len(node.peer_transports), 2)
            self.assertLessEqual(len(node.snapshot().diagnostics), 4)
        finally:
            node.stop()


def _node_config(
    node_id: str,
    port: int,
    peers: tuple[PeerEndpoint, ...],
    *,
    development_cluster: DevelopmentCluster | None = None,
    max_peers: int = 4,
    diagnostic_limit: int = 32,
) -> NodeConfig:
    return NodeConfig(
        identity=NodeIdentity("test-cluster", node_id),
        listen_address=TcpAddress("127.0.0.1", port),
        discovery=CompositeDiscovery(
            (StaticSeedDiscovery(peers, max_candidates=max_peers),),
            max_candidates=max_peers,
        ),
        transport_security_provider=(LocalDevelopmentTransportSecurityProvider()),
        membership=MembershipConfig(
            lease_seconds=0.3,
            suspect_seconds=0.1,
            dead_retention_seconds=0.2,
            max_members=max_peers + 1,
            max_changes=16,
        ),
        development_cluster=development_cluster,
        max_peers=max_peers,
        diagnostic_limit=diagnostic_limit,
        reconcile_interval_seconds=0.05,
        startup_peer_timeout_seconds=0.1,
        peer_absence_seconds=0.2,
        shutdown_timeout_seconds=2.0,
    )


def _reserve_port() -> int:
    return _reserve_ports(1)[0]


def _reserve_ports(count: int) -> tuple[int, ...]:
    reservations: list[socket.socket] = []
    try:
        for _index in range(count):
            reservation = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            reservation.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            reservation.bind(("127.0.0.1", 0))
            reservations.append(reservation)
        return tuple(int(reservation.getsockname()[1]) for reservation in reservations)
    finally:
        for reservation in reservations:
            reservation.close()


def _wait_until(predicate: Callable[[], bool], *, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


if __name__ == "__main__":
    unittest.main()
