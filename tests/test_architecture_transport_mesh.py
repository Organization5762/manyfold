from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from manyfold.architecture.pubsub import PubSubTopic
from manyfold.architecture.transport import (
    NodeIdentity,
    ReconnectPolicy,
    TcpAddress,
    TransportConfig,
    TransportSecurity,
)
from manyfold.architecture.transport_mesh import (
    MeshBackpressureError,
    MeshCapacityError,
    MeshClosed,
    MeshConfig,
    MeshDeliveryConfig,
    MeshLifecycleKind,
    MeshRouteError,
    MeshSubscriptionBackpressureError,
    MeshTopicPolicy,
    PeerDiscovery,
    TransportMesh,
)


class ArchitectureTransportMeshTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary_directory = tempfile.TemporaryDirectory()
        self._root = Path(self._temporary_directory.name)
        self._meshes: list[TransportMesh] = []

    def tearDown(self) -> None:
        for mesh in reversed(self._meshes):
            mesh.close()
        self._temporary_directory.cleanup()

    def test_bound_topic_survives_three_peer_disconnect_and_restart(self) -> None:
        topic_a = PubSubTopic(
            "navigation",
            namespace="durable-mesh-a",
        )
        topic_c = PubSubTopic(
            "navigation",
            namespace="durable-mesh-c",
        )
        node_a = self._mesh("a", durable_state=True)
        node_b = self._mesh("b", durable_state=True)
        node_c = self._mesh("c", durable_state=True)
        node_a.bind_topic(topic_a, policy=MeshTopicPolicy.APPEND)
        node_c.bind_topic(topic_c, policy=MeshTopicPolicy.APPEND)
        address_b = node_b.listen("a")
        node_a.apply_discovery((PeerDiscovery("b", address_b),))
        address_c = node_c.listen("b")
        node_b.apply_discovery((PeerDiscovery("c", address_c),))
        self.assertTrue(_wait_connected(node_a, node_b, node_c))
        self.assertTrue(
            _wait_for(
                lambda: node_a.health().remote_subscriptions >= 1,
                timeout=2.0,
            )
        )

        node_c.remove_peer("b")
        self.assertTrue(
            _wait_for(
                lambda: node_b.peer_health()[1].link.state.value
                == "reconnecting",
                timeout=1.0,
            )
        )
        self.assertTrue(
            _wait_for(
                lambda: {
                    MeshLifecycleKind.PEER_DISCONNECTED,
                    MeshLifecycleKind.PEER_RECONNECTING,
                }.issubset(
                    {event.kind for event in node_b.lifecycle_events()}
                ),
                timeout=1.0,
            )
        )
        topic_a.publish(b"queued-while-offline")
        self.assertTrue(
            _wait_for(
                lambda: node_b.peer_health()[1].delivery.outbox_items >= 1,
                timeout=2.0,
            )
        )

        node_b.close()
        node_c.close()
        restarted_c = self._mesh("c", durable_state=True, instance_id="c-restarted")
        restarted_c.bind_topic(topic_c, policy=MeshTopicPolicy.APPEND)
        restarted_c.listen("b", address_c)
        restarted_b = self._mesh("b", durable_state=True, instance_id="b-restarted")
        restarted_b.listen("a", address_b)
        restarted_b.apply_discovery((PeerDiscovery("c", address_c),))

        self.assertTrue(_wait_connected(node_a, restarted_b, restarted_c))
        self.assertTrue(
            _wait_for(
                lambda: (
                    topic_c.latest() is not None
                    and topic_c.latest().payload == b"queued-while-offline"
                ),
                timeout=3.0,
            ),
            (
                f"node_b={restarted_b.peer_health()!r}; "
                f"restarted_c={restarted_c.peer_health()!r}; "
                f"latest={topic_c.latest()!r}; "
                f"events={restarted_b.lifecycle_events()!r}"
            ),
        )
        with self.assertRaises(TimeoutError):
            restarted_c.receive(timeout=0.1)
        self.assertTrue(
            _wait_for(
                lambda: restarted_b.peer_health()[1].delivery.outbox_items == 0,
                timeout=2.0,
            )
        )

        kinds = [
            event.kind
            for event in (
                *node_b.lifecycle_events(),
                *restarted_b.lifecycle_events(),
            )
        ]
        self.assertIn(MeshLifecycleKind.PEER_DISCONNECTED, kinds)
        self.assertIn(MeshLifecycleKind.PEER_RECONNECTING, kinds)
        self.assertIn(MeshLifecycleKind.DURABLE_ENQUEUED, kinds)
        self.assertIn(MeshLifecycleKind.DURABLE_REPLAYED, kinds)
        self.assertGreaterEqual(kinds.count(MeshLifecycleKind.PEER_CONNECTED), 4)
        sequences = [
            event.sequence for event in restarted_b.lifecycle_events()
        ]
        self.assertEqual(sequences, sorted(sequences))

    def test_three_peer_restart_suppresses_acknowledged_message_id(self) -> None:
        topic_c = PubSubTopic(
            "commands",
            namespace="durable-dedup-c",
        )
        delivered: list[bytes] = []
        callback = topic_c.subscribe(
            lambda row: delivered.append(bytes(row.payload))
        )
        node_a = self._mesh("dedup-a", durable_state=True)
        node_b = self._mesh("dedup-b", durable_state=True)
        node_c = self._mesh("dedup-c", durable_state=True)
        node_c.bind_topic(topic_c, policy=MeshTopicPolicy.APPEND)
        address_b = node_b.listen("dedup-a")
        node_a.apply_discovery((PeerDiscovery("dedup-b", address_b),))
        address_c = node_c.listen("dedup-b")
        node_b.apply_discovery((PeerDiscovery("dedup-c", address_c),))
        self.assertTrue(_wait_connected(node_a, node_b, node_c))
        self.assertTrue(
            _wait_for(
                lambda: node_a.health().remote_subscriptions == 1,
                timeout=2.0,
            )
        )

        node_a.publish(
            "commands",
            b"execute-once",
            message_id="restart-stable-command",
        )
        self.assertTrue(
            _wait_for(lambda: delivered == [b"execute-once"], timeout=2.0)
        )
        self.assertTrue(
            _wait_for(
                lambda: node_a.peer_health()[0].delivery.outbox_items == 0,
                timeout=2.0,
            )
        )
        for mesh in (node_a, node_b, node_c):
            mesh.close()

        restarted_a = self._mesh(
            "dedup-a",
            durable_state=True,
            instance_id="dedup-a-restarted",
        )
        restarted_b = self._mesh(
            "dedup-b",
            durable_state=True,
            instance_id="dedup-b-restarted",
        )
        restarted_c = self._mesh(
            "dedup-c",
            durable_state=True,
            instance_id="dedup-c-restarted",
        )
        restarted_c.bind_topic(topic_c, policy=MeshTopicPolicy.APPEND)
        restarted_b.listen("dedup-a", address_b)
        restarted_a.apply_discovery((PeerDiscovery("dedup-b", address_b),))
        restarted_c.listen("dedup-b", address_c)
        restarted_b.apply_discovery((PeerDiscovery("dedup-c", address_c),))
        self.assertTrue(_wait_connected(restarted_a, restarted_b, restarted_c))
        self.assertTrue(
            _wait_for(
                lambda: restarted_a.health().remote_subscriptions == 1,
                timeout=2.0,
            )
        )

        restarted_a.publish(
            "commands",
            b"execute-once",
            message_id="restart-stable-command",
        )

        self.assertTrue(
            _wait_for(
                lambda: (
                    restarted_b.peer_health()[0]
                    .delivery.duplicates_suppressed
                    >= 1
                ),
                timeout=2.0,
            )
        )
        time.sleep(0.1)
        self.assertEqual(delivered, [b"execute-once"])
        callback.dispose()

        restarted_c.close()
        self.assertEqual(
            [
                event.kind
                for event in restarted_c.lifecycle_events()[-2:]
            ],
            [
                MeshLifecycleKind.RUNTIME_STOPPING,
                MeshLifecycleKind.RUNTIME_STOPPED,
            ],
        )

    def test_three_node_line_propagates_subscribe_publish_and_unsubscribe(
        self,
    ) -> None:
        node_a, node_b, node_c = self._connected_line()
        subscription = node_c.subscribe("sensor.temperature")
        self.assertTrue(
            _wait_for(
                lambda: (
                    node_a.health().remote_subscriptions == 1
                    and node_b.health().remote_subscriptions == 1
                ),
                timeout=2.0,
            )
        )
        interests = {
            peer.node_id: peer.interested_topics for peer in node_b.peer_health()
        }
        self.assertEqual(interests["a"], ())
        self.assertEqual(interests["c"], ("sensor.temperature",))

        result = node_a.publish(
            "sensor.temperature",
            b"72.4",
            message_id="temperature-1",
        )
        publication = node_c.receive(timeout=2.0)

        self.assertEqual(result.forwarded_peers, ("b",))
        self.assertEqual(publication.topic, "sensor.temperature")
        self.assertEqual(publication.payload, b"72.4")
        self.assertEqual(publication.message_id, "temperature-1")
        self.assertEqual(publication.source_node_id, "a")
        self.assertTrue(subscription.dispose())
        self.assertFalse(subscription.dispose())
        self.assertTrue(
            _wait_for(
                lambda: (
                    node_a.health().remote_subscriptions == 0
                    and node_b.health().remote_subscriptions == 0
                ),
                timeout=2.0,
            )
        )
        with self.assertRaisesRegex(MeshRouteError, "no mesh subscribers"):
            node_a.publish("sensor.temperature", b"late")

    def test_three_node_cycle_bounds_subscription_and_publication_loops(
        self,
    ) -> None:
        node_a, node_b, node_c = self._connected_cycle()
        subscription = node_c.subscribe("events.loop-safe")
        self.assertTrue(
            _wait_for(
                lambda: all(
                    mesh.health().remote_subscriptions == 1
                    for mesh in (node_a, node_b)
                ),
                timeout=2.0,
            )
        )
        time.sleep(0.2)

        result = node_a.publish(
            "events.loop-safe",
            b"once",
            message_id="loop-message",
        )
        publication = node_c.receive(timeout=2.0)

        self.assertEqual(publication.payload, b"once")
        self.assertEqual(publication.source_node_id, "a")
        self.assertGreaterEqual(len(result.forwarded_peers), 1)
        with self.assertRaises(TimeoutError):
            node_c.receive(timeout=0.2)
        with self.assertRaisesRegex(MeshRouteError, "already published"):
            node_c.publish(
                "events.loop-safe",
                b"duplicate",
                message_id="loop-message",
            )
        self.assertLessEqual(node_a.health().remote_subscriptions, 1)
        self.assertLessEqual(node_b.health().remote_subscriptions, 1)
        self.assertLessEqual(node_c.health().remote_subscriptions, 1)
        subscription.dispose()

    def test_static_discovery_update_removes_owned_connector_and_routes(
        self,
    ) -> None:
        node_a, node_b, node_c = self._connected_line()
        subscription = node_c.subscribe("events.discovery")
        self.assertTrue(
            _wait_for(
                lambda: node_a.health().remote_subscriptions == 1,
                timeout=2.0,
            )
        )

        node_a.apply_discovery(())

        self.assertEqual(node_a.health().peer_count, 0)
        self.assertTrue(
            _wait_for(
                lambda: node_b.health().connected_peers == 1,
                timeout=1.0,
            )
        )
        with self.assertRaisesRegex(MeshRouteError, "no mesh subscribers"):
            node_a.publish("events.discovery", b"unroutable")
        subscription.dispose()

    def test_listener_restart_reconnects_and_resynchronizes_interest(self) -> None:
        node_a = self._mesh("a")
        node_b = self._mesh("b")
        address = node_b.listen("a")
        node_a.apply_discovery((PeerDiscovery("b", address),))
        self.assertTrue(_wait_connected(node_a, node_b))
        subscription = node_b.subscribe("events.reconnect")
        self.assertTrue(
            _wait_for(
                lambda: node_a.health().remote_subscriptions == 1,
                timeout=2.0,
            )
        )

        node_b.remove_peer("a")
        self.assertTrue(
            _wait_for(
                lambda: node_a.peer_health()[0].link.state.value == "reconnecting",
                timeout=1.0,
            )
        )
        node_b.listen("a", address)
        self.assertTrue(_wait_connected(node_a, node_b))
        self.assertTrue(
            _wait_for(
                lambda: node_a.health().remote_subscriptions == 1,
                timeout=2.0,
            )
        )

        node_a.publish("events.reconnect", b"restored")

        self.assertEqual(node_b.receive(timeout=2.0).payload, b"restored")
        self.assertGreaterEqual(
            node_a.peer_health()[0].link.connections_established,
            2,
        )
        subscription.dispose()

    def test_new_peer_instance_replaces_stale_subscription_ids(self) -> None:
        node_a = self._mesh("a")
        first_node_b = self._mesh("b", instance_id="b-first")
        address = first_node_b.listen("a")
        node_a.apply_discovery((PeerDiscovery("b", address),))
        self.assertTrue(_wait_connected(node_a, first_node_b))
        first_node_b.subscribe("events.reconnect")
        self.assertTrue(
            _wait_for(
                lambda: node_a.health().remote_subscriptions == 1,
                timeout=2.0,
            )
        )

        first_node_b.close()
        self.assertTrue(
            _wait_for(
                lambda: node_a.peer_health()[0].link.state.value == "reconnecting",
                timeout=1.0,
            )
        )
        second_node_b = self._mesh("b", instance_id="b-second")
        second_node_b.listen("a", address)
        second_node_b.subscribe("events.reconnect")

        self.assertTrue(_wait_connected(node_a, second_node_b))
        self.assertTrue(
            _wait_for(
                lambda: node_a.health().remote_subscriptions == 1,
                timeout=2.0,
            )
        )
        self.assertEqual(
            node_a.peer_health()[0].link.remote_identity.instance_id,
            "b-second",
        )

    def test_peer_and_subscription_limits_fail_before_retention(self) -> None:
        mesh = self._mesh(
            "bounded",
            mesh_config=MeshConfig(
                max_peers=1,
                max_subscriptions=1,
                duplicate_window=2,
                publication_queue_limit=1,
            ),
        )
        mesh.listen("first")
        with self.assertRaisesRegex(MeshCapacityError, "max_peers"):
            mesh.listen("second")
        subscription = mesh.subscribe("one")
        with self.assertRaisesRegex(MeshCapacityError, "max_subscriptions"):
            mesh.subscribe("two")

        self.assertEqual(mesh.health().peer_count, 1)
        self.assertEqual(mesh.health().local_subscriptions, 1)
        subscription.dispose()

    def test_publication_history_and_subscription_churn_stay_bounded(self) -> None:
        mesh = self._mesh(
            "stress",
            mesh_config=MeshConfig(
                max_peers=1,
                max_subscriptions=2,
                duplicate_window=3,
                publication_queue_limit=1,
            ),
        )
        retained = mesh.subscribe("stress.events")
        for index in range(20):
            transient = mesh.subscribe("stress.transient")
            transient.dispose()
            mesh.publish(
                "stress.events",
                str(index).encode(),
                message_id=f"stress-{index}",
            )
            self.assertEqual(mesh.receive(timeout=0.1).payload, str(index).encode())

        self.assertEqual(mesh.health().local_subscriptions, 1)
        self.assertEqual(mesh.health().recent_publications, 3)
        self.assertEqual(mesh.health().publications_queued, 0)
        retained.dispose()

    def test_subscribe_backpressure_rolls_back_unowned_subscription(self) -> None:
        mesh = self._mesh(
            "source",
            outbound_queue_limit=1,
            delivery_config=MeshDeliveryConfig(max_outbox_items=1),
        )
        mesh.listen("offline")
        first = mesh.subscribe("first")

        with self.assertRaises(MeshSubscriptionBackpressureError) as caught:
            mesh.subscribe("second")

        self.assertEqual(caught.exception.subscription.topic, "second")
        self.assertEqual(mesh.health().local_subscriptions, 2)
        with self.assertRaises(MeshBackpressureError):
            first.dispose()

    def test_peer_health_reports_routes_and_routing_errors(self) -> None:
        node_a, node_b, node_c = self._connected_line()
        subscription = node_c.subscribe("events.health")
        self.assertTrue(
            _wait_for(
                lambda: (
                    "events.health"
                    in node_a.peer_health()[0].interested_topics
                ),
                timeout=2.0,
            )
        )

        health = node_a.peer_health()[0]

        self.assertEqual(health.node_id, "b")
        self.assertEqual(health.source, "discovery")
        self.assertEqual(health.link.remote_identity.node_id, "b")
        self.assertEqual(health.interested_topics, ("events.health",))
        self.assertIsNone(health.last_routing_error)
        subscription.dispose()

    def test_close_disposes_links_subscriptions_and_publications(self) -> None:
        mesh = self._mesh("closed")
        subscription = mesh.subscribe("local")
        mesh.publish("local", b"retained")

        mesh.close()

        self.assertTrue(mesh.health().is_closed)
        self.assertEqual(mesh.health().peer_count, 0)
        self.assertEqual(mesh.health().local_subscriptions, 0)
        self.assertEqual(mesh.health().publications_queued, 0)
        self.assertTrue(subscription.dispose())
        self.assertFalse(subscription.dispose())
        with self.assertRaises(MeshClosed):
            mesh.receive(timeout=0.1)
        with self.assertRaises(MeshClosed):
            mesh.subscribe("late")

    def test_mesh_contracts_reject_invalid_configuration_and_routes(self) -> None:
        with self.assertRaisesRegex(ValueError, "max_peers"):
            MeshConfig(max_peers=0)
        with self.assertRaisesRegex(ValueError, "peer node_id"):
            PeerDiscovery("", TcpAddress("127.0.0.1", 1))
        mesh = self._mesh("local")
        with self.assertRaisesRegex(ValueError, "differ"):
            mesh.listen("local")
        with self.assertRaisesRegex(ValueError, "reserved"):
            mesh.subscribe("_manyfold.mesh.private")
        with self.assertRaisesRegex(MeshRouteError, "no mesh subscribers"):
            mesh.publish("unsubscribed", b"value")
        with self.assertRaisesRegex(ValueError, "PeerDiscovery"):
            mesh.apply_discovery((object(),))  # type: ignore[arg-type]

    def _connected_line(
        self,
    ) -> tuple[TransportMesh, TransportMesh, TransportMesh]:
        node_a = self._mesh("a")
        node_b = self._mesh("b")
        node_c = self._mesh("c")
        address_b = node_b.listen("a")
        node_a.apply_discovery((PeerDiscovery("b", address_b),))
        address_c = node_c.listen("b")
        node_b.apply_discovery((PeerDiscovery("c", address_c),))
        self.assertTrue(_wait_connected(node_a, node_b, node_c))
        return node_a, node_b, node_c

    def _connected_cycle(
        self,
    ) -> tuple[TransportMesh, TransportMesh, TransportMesh]:
        node_a, node_b, node_c = self._connected_line()
        address_a = node_a.listen("c")
        node_c.apply_discovery((PeerDiscovery("a", address_a),))
        self.assertTrue(_wait_connected(node_a, node_b, node_c, expected_peers=2))
        return node_a, node_b, node_c

    def _mesh(
        self,
        node_id: str,
        *,
        instance_id: str | None = None,
        outbound_queue_limit: int = 16,
        mesh_config: MeshConfig | None = None,
        delivery_config: MeshDeliveryConfig | None = None,
        durable_state: bool = False,
    ) -> TransportMesh:
        transport_config = _transport_config(
            outbound_queue_limit=outbound_queue_limit
        )
        mesh = TransportMesh(
            NodeIdentity(
                "mesh-tests",
                node_id,
                instance_id or f"{node_id}-instance",
            ),
            connector_config=transport_config,
            listener_config=transport_config,
            config=mesh_config,
            delivery=(
                delivery_config
                or MeshDeliveryConfig(
                    state_directory=(
                        self._root / node_id if durable_state else None
                    ),
                    retry_initial_seconds=0.02,
                    retry_max_seconds=0.1,
                )
            ),
        )
        self._meshes.append(mesh)
        return mesh


def _transport_config(*, outbound_queue_limit: int = 16) -> TransportConfig:
    return TransportConfig(
        security=TransportSecurity.insecure_local_development(),
        outbound_queue_limit=outbound_queue_limit,
        inbound_queue_limit=16,
        max_payload_bytes=4096,
        connect_timeout=0.1,
        handshake_timeout=0.5,
        heartbeat_interval=0.05,
        peer_timeout=0.5,
        reconnect=ReconnectPolicy(
            initial_delay=0.02,
            multiplier=1.5,
            max_delay=0.1,
        ),
    )


def _wait_connected(
    *meshes: TransportMesh,
    expected_peers: int | None = None,
) -> bool:
    return _wait_for(
        lambda: all(
            mesh.health().connected_peers
            == (expected_peers if expected_peers is not None else mesh.health().peer_count)
            for mesh in meshes
        ),
        timeout=2.0,
    )


def _wait_for(predicate: object, *, timeout: float) -> bool:
    if not callable(predicate):
        raise TypeError("predicate must be callable")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return bool(predicate())


if __name__ == "__main__":
    unittest.main()
