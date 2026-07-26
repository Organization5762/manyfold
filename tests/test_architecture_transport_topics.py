from __future__ import annotations

import tempfile
import time
import unittest
from dataclasses import dataclass
from multiprocessing import get_context
from pathlib import Path
from queue import Empty
from threading import enumerate as enumerate_threads

from manyfold.architecture.pubsub import PubSub
from manyfold.architecture.transport import (
    LinkState,
    NodeIdentity,
    ReconnectPolicy,
    TcpAddress,
    TransportConfig,
    TransportSecurity,
)
from manyfold.architecture.transport_mesh import (
    MeshBackpressureError,
    MeshLifecycleKind,
    MeshRouteError,
    PeerDiscovery,
    TransportMesh,
)
from manyfold.architecture.transport_topics import (
    MeshDurabilityConfig,
    MeshTopicPolicy,
    TopicDeliveryClass,
)


class DurableTransportTopicTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self._root = Path(self._temporary.name)
        self._meshes: list[TransportMesh] = []

    def tearDown(self) -> None:
        for mesh in reversed(self._meshes):
            mesh.close()
        self._temporary.cleanup()

    def test_binding_requires_named_schema_and_valid_latest_key(self) -> None:
        mesh = TransportMesh(
            NodeIdentity("cluster", "a", "a-1"),
            connector_config=_transport_config(),
            durability=MeshDurabilityConfig(self._root / "journals"),
        )
        self._meshes.append(mesh)
        with self.assertRaisesRegex(ValueError, "explicit schema"):
            mesh.bind(
                PubSub(topic="untyped", schedule=False),
                policy=MeshTopicPolicy.commands("untyped"),
            )
        typed = PubSub(topic="sensor", schema=_Event, schedule=False)
        with self.assertRaisesRegex(ValueError, "not declared"):
            mesh.bind(
                typed,
                policy=MeshTopicPolicy.latest(
                    "sensor",
                    ttl_seconds=5.0,
                    max_sources=8,
                    max_bytes=1024,
                    key_field="missing",
                ),
            )
        mesh.bind(
            typed,
            policy=MeshTopicPolicy.latest(
                "sensor",
                ttl_seconds=5.0,
                max_sources=8,
                max_bytes=1024,
                key_field="source",
            ),
        )
        with self.assertRaisesRegex(ValueError, "out of bounds"):
            typed.publish_encoded(b"malformed")
        with self.assertRaisesRegex(MeshRouteError, "already bound"):
            mesh.bind(
                typed,
                policy=MeshTopicPolicy.latest(
                    "sensor",
                    ttl_seconds=5.0,
                    max_sources=1,
                    max_bytes=1024,
                ),
            )

    def test_frame_tick_policy_is_one_non_journaled_slot(
        self,
    ) -> None:
        policy = MeshTopicPolicy.live_latest("heart.frame_ticks")
        self.assertEqual(policy.max_sources, 1)
        self.assertFalse(policy.retains_journal_rows)
        self.assertEqual(policy.delivery_class, TopicDeliveryClass.LIVE_LATEST)
        with self.assertRaisesRegex(ValueError, "positive"):
            MeshTopicPolicy.live_latest("heart.frame_ticks", max_sources=0)

    def test_three_peer_append_latest_partition_recovery_and_shutdown(self) -> None:
        mesh_a, topics_a, addresses = self._hub("a", ("b", "c"))
        mesh_b, topics_b = self._spoke("b", "a", addresses["b"])
        mesh_c, topics_c = self._spoke("c", "a", addresses["c"])
        observed_a: list[tuple[str, int]] = []
        observed_b: list[tuple[str, int]] = []
        observed_c: list[tuple[str, int]] = []
        subscriptions = [
            topics_a["navigation"].subscribe(
                lambda row: observed_a.append(("navigation", int(row.value)))
            ),
            topics_b["navigation"].subscribe(
                lambda row: observed_b.append(("navigation", int(row.value)))
            ),
            topics_c["navigation"].subscribe(
                lambda row: observed_c.append(("navigation", int(row.value)))
            ),
            topics_c["rendered"].subscribe(
                lambda row: observed_c.append(("rendered", int(row.value)))
            ),
            topics_c["tick"].subscribe(
                lambda row: observed_c.append(("tick", int(row.value)))
            ),
        ]
        self.addCleanup(
            lambda: [subscription.dispose() for subscription in subscriptions]
        )
        self.assertTrue(
            _wait_for(
                lambda: (
                    mesh_a.health().remote_subscriptions == 8
                    and mesh_b.health().remote_subscriptions == 8
                    and mesh_c.health().remote_subscriptions == 8
                ),
                timeout=3.0,
            )
        )

        topics_b["navigation"].publish(_Event(1, "controller-b"))
        delivered = _wait_for(
            lambda: (
                observed_a.count(("navigation", 1)) == 1
                and observed_c.count(("navigation", 1)) == 1
            ),
            timeout=3.0,
        )
        self.assertTrue(
            delivered,
            (
                observed_a,
                observed_c,
                mesh_a.peer_health(),
                mesh_b.peer_health(),
                mesh_c.peer_health(),
                mesh_a.durable_topic_diagnostics(),
                mesh_b.durable_topic_diagnostics(),
            ),
        )
        time.sleep(0.2)
        self.assertEqual(observed_a.count(("navigation", 1)), 1)
        self.assertEqual(observed_c.count(("navigation", 1)), 1)

        self.assertTrue(mesh_c.remove_peer("a"))
        self.assertIn(
            "rendered",
            next(
                peer for peer in mesh_a.peer_health() if peer.node_id == "c"
            ).interested_topics,
        )
        self.assertTrue(
            _wait_for(
                lambda: next(
                    peer for peer in mesh_a.peer_health() if peer.node_id == "c"
                ).link.state
                is not LinkState.CONNECTED,
                timeout=3.0,
            )
        )
        for value in range(500):
            topics_a["rendered"].publish(_Event(value, "display"))
        for value in range(1000):
            topics_a["tick"].publish(_Event(value, "clock"))
        frame_diagnostics = _diagnostic(mesh_a, "rendered")
        self.assertEqual(frame_diagnostics.outbox_items, 0)
        self.assertFalse(frame_diagnostics.retains_journal_rows)
        self.assertFalse(_diagnostic(mesh_a, "tick").retains_journal_rows)

        mesh_c.apply_discovery((PeerDiscovery("a", addresses["c"]),))
        self.assertTrue(
            _wait_for(
                lambda: (
                    observed_c.count(("rendered", 499)) == 1
                    and observed_c.count(("tick", 999)) == 1
                ),
                timeout=3.0,
            ),
            (
                observed_c,
                mesh_a.peer_health(),
                mesh_c.peer_health(),
                mesh_a.lifecycle_events(),
                mesh_c.lifecycle_events(),
            ),
        )
        self.assertFalse(
            any(
                kind == "rendered" and value < 499
                for kind, value in observed_c
            ),
            observed_c,
        )
        self.assertFalse(
            any(kind == "tick" and value < 999 for kind, value in observed_c),
            observed_c,
        )

        mesh_c.remove_peer("a")
        topics_a["tick"].publish(_Event(1000, "clock"))
        time.sleep(0.12)
        mesh_c.apply_discovery((PeerDiscovery("a", addresses["c"]),))
        self.assertTrue(
            _wait_for(lambda: ("tick", 1000) in observed_c, timeout=3.0),
            (observed_c, mesh_a.peer_health(), mesh_c.peer_health()),
        )

        mesh_a.close()
        mesh_b.close()
        mesh_c.close()
        self.assertFalse(
            any(
                thread.name.startswith("manyfold-mesh-") and thread.is_alive()
                for thread in enumerate_threads()
            )
        )
        self.assertFalse(
            any(
                thread.name.startswith("manyfold-delivery-")
                for thread in enumerate_threads()
            )
        )

    def test_lifecycle_orders_disconnect_retry_reconnect_and_sender_ack(
        self,
    ) -> None:
        mesh_a, topics_a, addresses = self._hub("a", ("b",))
        lifecycle = mesh_a.subscribe_lifecycle()
        self.addCleanup(lifecycle.dispose)
        mesh_b, topics_b = self._spoke("b", "a", addresses["b"])
        observed_navigation: list[int] = []
        observed_sensor: list[int] = []
        subscriptions = (
            topics_b["navigation"].subscribe(
                lambda row: observed_navigation.append(int(row.value))
            ),
            topics_b["sensor"].subscribe(
                lambda row: observed_sensor.append(int(row.value))
            ),
        )
        self.addCleanup(
            lambda: [subscription.dispose() for subscription in subscriptions]
        )
        self.assertTrue(
            _wait_for(
                lambda: mesh_a.health().remote_subscriptions == 4,
                timeout=3.0,
            )
        )

        self.assertTrue(mesh_b.remove_peer("a"))
        self.assertTrue(
            _wait_for(
                lambda: _has_lifecycle(
                    mesh_a,
                    MeshLifecycleKind.PEER_RECONNECTING,
                    peer_node_id="b",
                ),
                timeout=3.0,
            )
        )
        for value in range(3):
            topics_a["navigation"].publish(
                _Event(value, "controller"),
                key=f"navigation-offline-{value}",
            )
        topics_a["sensor"].publish(
            _Event(1, "imu"),
            key="sensor-reading-1",
        )
        topics_a["sensor"].publish(
            _Event(2, "imu"),
            key="sensor-reading-2",
        )
        self.assertEqual(_diagnostic(mesh_a, "navigation").outbox_items, 3)
        self.assertEqual(_diagnostic(mesh_a, "sensor").outbox_items, 1)
        self.assertEqual(_diagnostic(mesh_a, "sensor").coalesced, 1)

        mesh_b.apply_discovery((PeerDiscovery("a", addresses["b"]),))
        self.assertTrue(
            _wait_for(
                lambda: (
                    observed_navigation == [0, 1, 2]
                    and observed_sensor == [2]
                    and _diagnostic(mesh_a, "navigation").outbox_items == 0
                    and _diagnostic(mesh_a, "sensor").outbox_items == 0
                ),
                timeout=4.0,
            ),
            (
                observed_navigation,
                observed_sensor,
                mesh_a.lifecycle_events(),
            ),
        )

        events = mesh_a.lifecycle_events()
        self.assertFalse(
            _diagnostic(mesh_a, "navigation").retains_journal_rows
        )
        self.assertTrue(
            _diagnostic(mesh_b, "navigation").retains_journal_rows
        )
        self.assertEqual(
            tuple(event.sequence for event in events),
            tuple(range(1, len(events) + 1)),
        )
        correlation = "navigation-offline-0"
        correlated = tuple(
            event
            for event in events
            if event.correlation_id == correlation
            and event.peer_node_id == "b"
        )
        self.assertTrue(
            _ordered_kinds(
                correlated,
                (
                    MeshLifecycleKind.DURABLE_ENQUEUED,
                    MeshLifecycleKind.DURABLE_SENT,
                    MeshLifecycleKind.DURABLE_RETRY,
                    MeshLifecycleKind.DURABLE_ACKED,
                ),
            )
        )
        self.assertEqual(
            len({event.message_id for event in correlated}),
            1,
        )
        self.assertTrue(
            _ordered_kinds(
                events,
                (
                    MeshLifecycleKind.PEER_DISCOVERED,
                    MeshLifecycleKind.PEER_CONNECTING,
                    MeshLifecycleKind.PEER_CONNECTED,
                    MeshLifecycleKind.PEER_DISCONNECTED,
                    MeshLifecycleKind.PEER_RECONNECTING,
                    MeshLifecycleKind.PEER_CONNECTED,
                ),
                peer_node_id="b",
            )
        )
        self.assertTrue(
            _ordered_kinds(
                events,
                (
                    MeshLifecycleKind.WATERMARK_CROSSED,
                    MeshLifecycleKind.WATERMARK_RECOVERED,
                ),
            )
        )
        watermark = next(
            event
            for event in events
            if event.kind is MeshLifecycleKind.WATERMARK_CROSSED
        )
        self.assertGreaterEqual(watermark.item_count or 0, 1)
        self.assertGreater(watermark.byte_count or 0, 0)
        self.assertTrue(
            _has_lifecycle(
                mesh_a,
                MeshLifecycleKind.DURABLE_COALESCED,
                topic="sensor",
                correlation_id="sensor-reading-2",
            )
        )
        subscribed_events = lifecycle.drain()
        self.assertEqual(
            tuple(event.sequence for event in subscribed_events),
            tuple(
                range(
                    subscribed_events[0].sequence,
                    subscribed_events[-1].sequence + 1,
                )
            ),
        )

        mesh_a.close()
        final_events = mesh_a.lifecycle_events()
        self.assertTrue(
            _ordered_kinds(
                final_events,
                (
                MeshLifecycleKind.RUNTIME_STOPPING,
                MeshLifecycleKind.RUNTIME_STOPPED,
                ),
            ),
        )
        self.assertIs(
            final_events[-1].kind,
            MeshLifecycleKind.RUNTIME_STOPPED,
        )

    def test_append_hard_cap_rejects_without_exceeding_bound(self) -> None:
        mesh_a, topics_a, addresses = self._hub("a", ("b",))
        mesh_b, _ = self._spoke("b", "a", addresses["b"])
        self.assertTrue(
            _wait_for(
                lambda: mesh_a.health().remote_subscriptions == 4,
                timeout=3.0,
            )
        )
        mesh_b.remove_peer("a")
        self.assertIn(
            "navigation",
            next(
                peer for peer in mesh_a.peer_health() if peer.node_id == "b"
            ).interested_topics,
        )

        for value in range(4):
            topics_a["navigation"].publish(_Event(value, "controller"))
        with self.assertRaisesRegex(MeshBackpressureError, "backpressure"):
            topics_a["navigation"].publish(_Event(5, "controller"))

        diagnostics = _diagnostic(mesh_a, "navigation")
        self.assertEqual(diagnostics.outbox_items, 4)
        self.assertEqual(diagnostics.storage_rejections, 1)
        self.assertEqual(
            diagnostics.delivery_class,
            TopicDeliveryClass.DURABLE_APPEND,
        )

    def test_process_partition_and_restart_loads_append_and_latest_rows(
        self,
    ) -> None:
        context = get_context("spawn")
        output = context.Queue()
        receiver_commands = context.Queue()
        receiver = context.Process(
            target=_receiver_process,
            args=(self._root, None, receiver_commands, output),
        )
        receiver.start()
        self.addCleanup(_stop_process, receiver, receiver_commands)
        address_message = output.get(timeout=5.0)
        self.assertEqual(address_message[0], "receiver-ready")
        address = TcpAddress(address_message[1], address_message[2])

        sender_commands = context.Queue()
        sender = context.Process(
            target=_sender_process,
            args=(self._root, address, sender_commands, output),
        )
        sender.start()
        self.addCleanup(_stop_process, sender, sender_commands)
        self.assertEqual(
            _next_kind(output, "sender-ready", timeout=5.0)[0], "sender-ready"
        )
        sender_commands.put(("navigation", 1))
        self.assertEqual(
            _next_kind(output, "navigation", timeout=5.0),
            ("navigation", 1),
        )
        sender_commands.put(("diagnostics",))
        self.assertEqual(
            _next_kind(output, "diagnostics", timeout=5.0),
            ("diagnostics", 0),
        )

        receiver.terminate()
        receiver.join(timeout=3.0)
        self.assertFalse(receiver.is_alive())
        sender_commands.put(("tick", 50))
        time.sleep(0.12)
        sender_commands.put(("topic-diagnostics", "process.tick"))
        self.assertEqual(
            _next_kind(output, "topic-diagnostics", timeout=5.0)[1],
            0,
        )
        sender_commands.put(("navigation", 2))
        sender_commands.put(("rendered", 77))
        for value in range(100):
            sender_commands.put(("sensor", value))
        sender_commands.put(("close",))
        self.assertEqual(
            _next_kind(output, "sender-closed", timeout=5.0)[0], "sender-closed"
        )
        sender.join(timeout=3.0)
        self.assertFalse(sender.is_alive())

        restarted_receiver_commands = context.Queue()
        restarted_receiver = context.Process(
            target=_receiver_process,
            args=(self._root, address, restarted_receiver_commands, output),
        )
        restarted_receiver.start()
        self.addCleanup(
            _stop_process,
            restarted_receiver,
            restarted_receiver_commands,
        )
        self.assertEqual(
            _next_kind(output, "receiver-ready", timeout=5.0)[0],
            "receiver-ready",
        )
        restarted_sender_commands = context.Queue()
        restarted_sender = context.Process(
            target=_sender_process,
            args=(self._root, address, restarted_sender_commands, output),
        )
        restarted_sender.start()
        self.addCleanup(
            _stop_process,
            restarted_sender,
            restarted_sender_commands,
        )
        sender_ready = _next_kind(output, "sender-ready", timeout=5.0)
        self.assertGreaterEqual(sender_ready[1], 2)
        recovered = {
            _next_kind(output, "navigation", timeout=5.0),
            _next_kind(output, "sensor", timeout=5.0),
        }
        self.assertEqual(recovered, {("navigation", 2), ("sensor", 99)})
        time.sleep(0.2)
        remaining = _drain_output(output)
        self.assertNotIn(("navigation", 2), remaining)
        self.assertNotIn(("tick", 50), remaining)
        self.assertNotIn(("rendered", 77), remaining)
        self.assertFalse(
            any(item[0] == "rendered" and item[1] < 99 for item in remaining)
        )

    def _hub(
        self,
        node_id: str,
        peers: tuple[str, ...],
    ) -> tuple[TransportMesh, dict[str, PubSub], dict[str, object]]:
        mesh, topics = self._mesh(node_id)
        addresses = {peer: mesh.listen(peer) for peer in peers}
        return mesh, topics, addresses

    def _spoke(
        self,
        node_id: str,
        peer_id: str,
        address: TcpAddress,
    ) -> tuple[TransportMesh, dict[str, PubSub]]:
        mesh, topics = self._mesh(node_id)
        mesh.apply_discovery((PeerDiscovery(peer_id, address),))
        return mesh, topics

    def _mesh(self, node_id: str) -> tuple[TransportMesh, dict[str, PubSub]]:
        mesh = TransportMesh(
            NodeIdentity("cluster", node_id, f"{node_id}-1"),
            connector_config=_transport_config(),
            durability=MeshDurabilityConfig(
                self._root / "journals",
                hard_peer_items=8,
                hard_peer_bytes=64 * 1024,
                dedupe_retention_seconds=2.0,
                retry_initial_seconds=0.02,
                retry_multiplier=1.5,
                retry_max_seconds=0.05,
            ),
        )
        self._meshes.append(mesh)
        topics = {
            "navigation": PubSub(
                topic="navigation",
                schema=_Event,
                schedule=False,
            ),
            "rendered": PubSub(topic="rendered", schema=_Event, schedule=False),
            "sensor": PubSub(topic="sensor", schema=_Event, schedule=False),
            "tick": PubSub(topic="tick", schema=_Event, schedule=False),
        }
        mesh.bind(
            topics["navigation"],
            policy=MeshTopicPolicy.commands(
                "navigation",
                ttl_seconds=2.0,
                max_items=4,
                max_bytes=32 * 1024,
                max_message_bytes=4096,
            ),
        )
        mesh.bind(
            topics["rendered"],
            policy=MeshTopicPolicy.live_latest(
                "rendered",
                max_sources=1,
                max_message_bytes=4096,
            ),
        )
        mesh.bind(
            topics["sensor"],
            policy=MeshTopicPolicy.latest(
                "sensor",
                ttl_seconds=2.0,
                max_sources=2,
                max_bytes=32 * 1024,
                max_message_bytes=4096,
                key_field="source",
            ),
        )
        mesh.bind(
            topics["tick"],
            policy=MeshTopicPolicy.live_latest(
                "tick",
                max_sources=1,
                max_message_bytes=4096,
            ),
        )
        return mesh, topics


def _diagnostic(mesh: TransportMesh, topic: str):
    return next(
        diagnostic
        for diagnostic in mesh.durable_topic_diagnostics()
        if diagnostic.topic == topic
    )


def _has_lifecycle(
    mesh: TransportMesh,
    kind: MeshLifecycleKind,
    *,
    topic: str | None = None,
    peer_node_id: str | None = None,
    correlation_id: str | None = None,
) -> bool:
    return any(
        event.kind is kind
        and (topic is None or event.topic == topic)
        and (peer_node_id is None or event.peer_node_id == peer_node_id)
        and (correlation_id is None or event.correlation_id == correlation_id)
        for event in mesh.lifecycle_events()
    )


def _ordered_kinds(
    events,
    kinds: tuple[MeshLifecycleKind, ...],
    *,
    peer_node_id: str | None = None,
) -> bool:
    remaining = iter(kinds)
    expected = next(remaining, None)
    for event in events:
        if peer_node_id is not None and event.peer_node_id != peer_node_id:
            continue
        if event.kind is expected:
            expected = next(remaining, None)
            if expected is None:
                return True
    return False


def _receiver_process(
    root: Path,
    address: TcpAddress | None,
    commands,
    output,
) -> None:
    mesh, topics = _process_mesh(root, "receiver")
    listen_address = mesh.listen("sender", address)
    subscriptions = [
        topics["navigation"].subscribe(
            lambda row: output.put(("navigation", int(row.value)))
        ),
        topics["rendered"].subscribe(
            lambda row: output.put(("rendered", int(row.value)))
        ),
        topics["tick"].subscribe(lambda row: output.put(("tick", int(row.value)))),
        topics["sensor"].subscribe(lambda row: output.put(("sensor", int(row.value)))),
    ]
    output.put(("receiver-ready", listen_address.host, listen_address.port))
    try:
        while True:
            try:
                command = commands.get(timeout=0.05)
            except Empty:
                continue
            if command[0] == "close":
                break
    finally:
        for subscription in subscriptions:
            subscription.dispose()
        mesh.close()
        output.put(
            (
                "receiver-closed",
                sum(
                    thread.name.startswith("manyfold-mesh-")
                    for thread in enumerate_threads()
                ),
            )
        )


def _sender_process(root: Path, address: TcpAddress, commands, output) -> None:
    mesh, topics = _process_mesh(root, "sender")
    mesh.apply_discovery((PeerDiscovery("receiver", address),))
    if not _wait_for(
        lambda: mesh.health().remote_subscriptions == 4,
        timeout=4.0,
    ):
        raise RuntimeError("sender did not recover durable subscriptions")
    recovery_loaded = sum(
        row.recovery_loaded_rows for row in mesh.durable_topic_diagnostics()
    )
    output.put(("sender-ready", recovery_loaded))
    try:
        while True:
            command = commands.get()
            if command[0] == "close":
                break
            if command[0] == "diagnostics":
                if not _wait_for(
                    lambda: (
                        sum(
                            row.outbox_items for row in mesh.durable_topic_diagnostics()
                        )
                        == 0
                    ),
                    timeout=3.0,
                ):
                    raise RuntimeError("sender outbox did not acknowledge")
                output.put(("diagnostics", 0))
                continue
            if command[0] == "topic-diagnostics":
                diagnostics = next(
                    row
                    for row in mesh.durable_topic_diagnostics()
                    if row.topic == command[1]
                )
                output.put(("topic-diagnostics", diagnostics.expired))
                continue
            topics[command[0]].publish(_Event(int(command[1]), "process"))
    finally:
        mesh.close()
        output.put(
            (
                "sender-closed",
                sum(
                    thread.name.startswith("manyfold-mesh-")
                    for thread in enumerate_threads()
                ),
            )
        )


def _process_mesh(root: Path, node_id: str) -> tuple[TransportMesh, dict[str, PubSub]]:
    mesh = TransportMesh(
        NodeIdentity("process-cluster", node_id, f"{node_id}-1"),
        connector_config=_transport_config(),
        durability=MeshDurabilityConfig(
            root / "process-journals",
            hard_peer_items=32,
            hard_peer_bytes=512 * 1024,
            dedupe_retention_seconds=10.0,
            retry_initial_seconds=0.02,
            retry_multiplier=1.5,
            retry_max_seconds=0.05,
        ),
    )
    topics = {
        "navigation": PubSub(
            topic="process.navigation",
            schema=_Event,
            schedule=False,
        ),
        "rendered": PubSub(
            topic="process.rendered",
            schema=_Event,
            schedule=False,
        ),
        "tick": PubSub(
            topic="process.tick",
            schema=_Event,
            schedule=False,
        ),
        "sensor": PubSub(
            topic="process.sensor",
            schema=_Event,
            schedule=False,
        ),
    }
    mesh.bind(
        topics["navigation"],
        policy=MeshTopicPolicy.commands(
            "process.navigation",
            ttl_seconds=10.0,
            max_items=16,
            max_bytes=256 * 1024,
            max_message_bytes=4096,
        ),
    )
    mesh.bind(
        topics["rendered"],
        policy=MeshTopicPolicy.live_latest(
            "process.rendered",
            max_message_bytes=4096,
        ),
    )
    mesh.bind(
        topics["tick"],
        policy=MeshTopicPolicy.live_latest(
            "process.tick",
            max_message_bytes=4096,
        ),
    )
    mesh.bind(
        topics["sensor"],
        policy=MeshTopicPolicy.latest(
            "process.sensor",
            ttl_seconds=5.0,
            max_sources=8,
            max_bytes=256 * 1024,
            key_field="source",
            max_message_bytes=4096,
        ),
    )
    return mesh, topics


def _next_kind(output, kind: str, *, timeout: float):
    deadline = time.monotonic() + timeout
    deferred = []
    while time.monotonic() < deadline:
        try:
            item = output.get(timeout=min(0.1, deadline - time.monotonic()))
        except Empty:
            continue
        if item[0] == kind:
            for deferred_item in deferred:
                output.put(deferred_item)
            return item
        deferred.append(item)
    for deferred_item in deferred:
        output.put(deferred_item)
    raise TimeoutError(f"no {kind!r} process result arrived")


def _drain_output(output) -> list[tuple]:
    items = []
    while True:
        try:
            items.append(output.get_nowait())
        except Empty:
            return items


def _stop_process(process, commands) -> None:
    if not process.is_alive():
        return
    commands.put(("close",))
    process.join(timeout=2.0)
    if process.is_alive():
        process.terminate()
        process.join(timeout=2.0)


def _transport_config() -> TransportConfig:
    return TransportConfig(
        security=TransportSecurity.insecure_local_development(),
        outbound_queue_limit=16,
        inbound_queue_limit=16,
        max_payload_bytes=65536,
        connect_timeout=0.1,
        handshake_timeout=0.5,
        heartbeat_interval=0.05,
        peer_timeout=0.5,
        reconnect=ReconnectPolicy(0.02, 1.5, 0.1),
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


@dataclass(frozen=True)
class _Event:
    value: int
    source: str


if __name__ == "__main__":
    unittest.main()
