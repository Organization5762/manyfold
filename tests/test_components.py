from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.test_support import load_manyfold_package


class ComponentTests(unittest.TestCase):
    def test_file_store_addresses_bytes_by_nested_keyspace_prefix(self) -> None:
        manyfold = load_manyfold_package()

        with tempfile.TemporaryDirectory() as temp_dir:
            store = manyfold.FileStore(temp_dir)
            robot = store.prefix("robots", "r1")
            pose = robot.prefix("pose")

            pose.put("latest", value=b"pose-2")
            pose.put("events", 1, value=b"pose-1")
            pose.put("events", 2, value=b"pose-2")
            reloaded = manyfold.FileStore(temp_dir).prefix("robots", "r1", "pose")

            latest = reloaded.get("latest")
            events = reloaded.scan("events")

        self.assertEqual(latest, b"pose-2")
        self.assertEqual(
            [(entry.key, entry.value) for entry in events],
            [
                (("events", "1"), b"pose-1"),
                (("events", "2"), b"pose-2"),
            ],
        )

    def test_file_store_keeps_special_key_parts_inside_root(self) -> None:
        manyfold = load_manyfold_package()

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "store"
            store = manyfold.FileStore(root)
            keyspace = store.prefix("safe")

            keyspace.put("..", value=b"parent")
            keyspace.put(".", value=b"current")
            keyspace.put("", value=b"empty")
            entries = keyspace.scan()
            outside_path = root.parent / "__value__.bin"

            self.assertFalse(outside_path.exists())
            self.assertEqual(keyspace.get(".."), b"parent")
            self.assertEqual(keyspace.get("."), b"current")
            self.assertEqual(keyspace.get(""), b"empty")
            self.assertEqual(
                [(entry.key, entry.value) for entry in entries],
                [
                    (("",), b"empty"),
                    ((".",), b"current"),
                    (("..",), b"parent"),
                ],
            )

    def test_file_store_rejects_nul_key_parts(self) -> None:
        manyfold = load_manyfold_package()

        with tempfile.TemporaryDirectory() as temp_dir:
            store = manyfold.FileStore(temp_dir)

            with self.assertRaisesRegex(ValueError, "NUL"):
                store.put("bad\x00key", value=b"nope")

    def test_event_log_appends_commits_and_replays_typed_values(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.Schema(
            schema_id="Pose",
            version=1,
            encode=lambda value: f"{value[0]},{value[1]}".encode("ascii"),
            decode=lambda payload: tuple(
                int(part) for part in payload.decode("ascii").split(",", 1)
            ),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            keyspace = manyfold.FileStore(temp_dir).prefix("robots", "r1", "pose_log")
            log = manyfold.EventLog("pose_log", keyspace, schema)
            graph = manyfold.Graph()
            committed: list[tuple[int, int]] = []
            log_subscription = log.install(graph)
            committed_subscription = graph.observe(
                log.output(), replay_latest=False
            ).subscribe(lambda envelope: committed.append(envelope.value))

            graph.publish(log.input(), (1, 10))
            graph.publish(log.input(), (2, 20))
            log_subscription.dispose()
            committed_subscription.dispose()

            reloaded = manyfold.EventLog("pose_log", keyspace, schema)
            replay_graph = manyfold.Graph()
            replayed: list[tuple[int, int]] = []
            replay_subscription = replay_graph.observe(
                reloaded.output(), replay_latest=False
            ).subscribe(lambda envelope: replayed.append(envelope.value))
            records = reloaded.replay(replay_graph)
            replay_subscription.dispose()

        self.assertEqual(committed, [(1, 10), (2, 20)])
        self.assertEqual([record.index for record in records], [1, 2])
        self.assertEqual([record.value for record in records], [(1, 10), (2, 20)])
        self.assertEqual(replayed, [(1, 10), (2, 20)])

    def test_snapshot_store_writes_latest_value_and_publishes_output(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.Schema(
            schema_id="Temperature",
            version=1,
            encode=lambda value: str(value).encode("ascii"),
            decode=lambda payload: int(payload.decode("ascii")),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            keyspace = manyfold.FileStore(temp_dir).prefix("sensors", "ambient")
            snapshot = manyfold.SnapshotStore("ambient_snapshot", keyspace, schema)
            graph = manyfold.Graph()
            latest_values: list[int] = []
            store_subscription = snapshot.install(graph)
            latest_subscription = graph.observe(
                snapshot.output(), replay_latest=False
            ).subscribe(lambda envelope: latest_values.append(envelope.value))

            graph.publish(snapshot.input(), 21)
            graph.publish(snapshot.input(), 24)
            store_subscription.dispose()
            latest_subscription.dispose()

            reloaded = manyfold.SnapshotStore("ambient_snapshot", keyspace, schema)
            replay_graph = manyfold.Graph()
            durable_latest = reloaded.latest()
            published = reloaded.publish_latest(replay_graph)
            latest = replay_graph.latest(reloaded.output())

        self.assertEqual(latest_values, [21, 24])
        self.assertEqual(durable_latest, 24)
        self.assertEqual(published, 24)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value, 24)

    def test_consensus_component_runs_default_leader_election(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        consensus = manyfold.Consensus.install(graph)

        consensus.tick(1)
        self.assertIsNone(consensus.latest_leader())

        consensus.tick(2)
        consensus.propose(1, "set mode=auto")
        consensus.propose(2, "set temp=21")

        self.assertEqual(consensus.latest_leader(), ("node-a", 3, True))
        self.assertEqual(
            consensus.latest_quorum(),
            (3, "node-a", ("node-a", "node-b", "node-c"), True),
        )
        self.assertEqual(
            consensus.latest_log(),
            ((1, "set mode=auto"), (2, "set temp=21")),
        )

    def test_consensus_component_validates_candidate_membership(self) -> None:
        manyfold = load_manyfold_package()

        with self.assertRaisesRegex(ValueError, "candidate_id"):
            manyfold.Consensus(
                manyfold.Graph(),
                nodes=("node-a", "node-b"),
                candidate_id="node-c",
            )

    def test_memory_chip_records_and_resumes_typed_route_values(self) -> None:
        manyfold = load_manyfold_package()
        route = manyfold.route(
            plane=manyfold.Plane.Read,
            layer=manyfold.Layer.Logical,
            owner=manyfold.OwnerName("sensor"),
            family=manyfold.StreamFamily("temperature"),
            stream=manyfold.StreamName("ambient"),
            variant=manyfold.Variant.Meta,
            schema=manyfold.Schema(
                schema_id="Temperature",
                version=1,
                encode=lambda value: str(value).encode("ascii"),
                decode=lambda payload: int(payload.decode("ascii")),
            ),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "memory.jsonl"
            graph = manyfold.Graph()
            memory = manyfold.Memory(path)
            subscription = memory.remember(graph, route, replay_latest=False)
            graph.publish(route, 21, control_epoch=101)
            graph.publish(route, 24, control_epoch=102)
            subscription.dispose()

            resumed_graph = manyfold.Graph()
            records = manyfold.Memory(path).resume(resumed_graph, route)
            latest = resumed_graph.latest(route)

        self.assertEqual([record.value for record in records], [21, 24])
        self.assertEqual([record.control_epoch for record in records], [101, 102])
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value, 24)

    def test_memory_chip_skips_duplicate_observed_events(self) -> None:
        manyfold = load_manyfold_package()
        route = manyfold.route(
            plane=manyfold.Plane.Read,
            layer=manyfold.Layer.Logical,
            owner=manyfold.OwnerName("demo"),
            family=manyfold.StreamFamily("memory"),
            stream=manyfold.StreamName("bytes"),
            variant=manyfold.Variant.Meta,
            schema=manyfold.Schema.bytes("MemoryBytes"),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "memory.jsonl"
            graph = manyfold.Graph()
            graph.publish(route, b"same")
            memory = manyfold.Memory(path)
            first = memory.remember(graph, route)
            second = memory.remember(graph, route)
            first.dispose()
            second.dispose()

            records = memory.records(route)

        self.assertEqual([record.value for record in records], [b"same"])

    def test_memory_chip_records_new_events_after_graph_sequence_restart(self) -> None:
        manyfold = load_manyfold_package()
        route = manyfold.route(
            plane=manyfold.Plane.Read,
            layer=manyfold.Layer.Logical,
            owner=manyfold.OwnerName("demo"),
            family=manyfold.StreamFamily("memory"),
            stream=manyfold.StreamName("bytes"),
            variant=manyfold.Variant.Meta,
            schema=manyfold.Schema.bytes("MemoryBytes"),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "memory.jsonl"

            first_graph = manyfold.Graph()
            first_memory = manyfold.Memory(path)
            first_subscription = first_memory.remember(first_graph, route)
            first_graph.publish(route, b"first", control_epoch=1)
            first_subscription.dispose()

            restarted_graph = manyfold.Graph()
            restarted_memory = manyfold.Memory(path)
            restarted_subscription = restarted_memory.remember(restarted_graph, route)
            restarted_graph.publish(route, b"first")
            restarted_subscription.dispose()

            records = manyfold.Memory(path).records(route)

        self.assertEqual([record.value for record in records], [b"first", b"first"])
        self.assertEqual([record.seq_source for record in records], [1, 1])


if __name__ == "__main__":
    unittest.main()
