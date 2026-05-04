from __future__ import annotations

import json
import tempfile
import threading
import time
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

    def test_file_store_scan_orders_entries_by_decoded_key_parts(self) -> None:
        manyfold = load_manyfold_package()

        with tempfile.TemporaryDirectory() as temp_dir:
            keyspace = manyfold.FileStore(temp_dir).prefix("safe")

            keyspace.put("/", value=b"slash")
            keyspace.put("0", value=b"zero")
            keyspace.put("A", value=b"upper")
            entries = keyspace.scan()

        self.assertEqual(
            [(entry.key, entry.value) for entry in entries],
            [
                (("/",), b"slash"),
                (("0",), b"zero"),
                (("A",), b"upper"),
            ],
        )

    def test_keyspace_keys_orders_entries_without_values(self) -> None:
        manyfold = load_manyfold_package()

        with tempfile.TemporaryDirectory() as temp_dir:
            keyspace = manyfold.FileStore(temp_dir).prefix("safe")

            keyspace.put("/", value=b"slash")
            keyspace.put("0", value=b"zero")
            keyspace.put("A", value=b"upper")
            keys = keyspace.keys()

        self.assertEqual(keys, (("/",), ("0",), ("A",)))

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

    def test_file_store_allows_key_part_matching_value_filename(self) -> None:
        manyfold = load_manyfold_package()

        with tempfile.TemporaryDirectory() as temp_dir:
            keyspace = manyfold.FileStore(temp_dir).prefix("safe")

            keyspace.put("__value__.bin", value=b"nested")
            keyspace.put(value=b"parent")
            entries = keyspace.scan()
            parent = keyspace.get()
            nested = keyspace.get("__value__.bin")

        self.assertEqual(parent, b"parent")
        self.assertEqual(nested, b"nested")
        self.assertEqual(
            [(entry.key, entry.value) for entry in entries],
            [
                ((), b"parent"),
                (("__value__.bin",), b"nested"),
            ],
        )

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

    def test_event_log_append_finds_next_index_without_decoding_history(self) -> None:
        manyfold = load_manyfold_package()
        schema = manyfold.Schema(
            schema_id="Command",
            version=1,
            encode=lambda value: value.encode("ascii"),
            decode=lambda payload: (
                "next"
                if payload == b"next"
                else (_ for _ in ()).throw(AssertionError("decoded legacy record"))
            ),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            keyspace = manyfold.FileStore(temp_dir).prefix("commands")
            keyspace.put("00000000000000000001", value=b"legacy")
            log = manyfold.EventLog("commands", keyspace, schema)
            graph = manyfold.Graph()
            committed: list[str] = []
            log_subscription = log.install(graph)
            committed_subscription = graph.observe(
                log.output(), replay_latest=False
            ).subscribe(lambda envelope: committed.append(envelope.value))

            graph.publish(log.input(), "next")
            log_subscription.dispose()
            committed_subscription.dispose()
            stored_next = keyspace.get("00000000000000000002")

        self.assertEqual(committed, ["next"])
        self.assertEqual(stored_next, b"next")

    def test_event_log_serializes_concurrent_appends(self) -> None:
        manyfold = load_manyfold_package()

        def encode(value: int) -> bytes:
            time.sleep(0.01)
            return str(value).encode("ascii")

        schema = manyfold.Schema(
            schema_id="ConcurrentCommand",
            version=1,
            encode=encode,
            decode=lambda payload: int(payload.decode("ascii")),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            keyspace = manyfold.FileStore(temp_dir).prefix("commands")
            log = manyfold.EventLog("commands", keyspace, schema)
            graph = manyfold.Graph()
            subscription = log.install(graph)
            publishers = tuple(
                threading.Thread(target=graph.publish, args=(log.input(), index))
                for index in range(12)
            )

            for publisher in publishers:
                publisher.start()
            for publisher in publishers:
                publisher.join()
            subscription.dispose()
            records = log.records()

        self.assertEqual([record.index for record in records], list(range(1, 13)))
        self.assertEqual({record.value for record in records}, set(range(12)))

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

    def test_consensus_log_round_trips_multiline_commands(self) -> None:
        manyfold = load_manyfold_package()
        graph = manyfold.Graph()
        consensus = manyfold.Consensus.install(graph)
        command = "set pipe=a|b\nset mode=auto"

        consensus.tick(1)
        consensus.tick(2)
        consensus.propose(1, command)

        self.assertEqual(consensus.latest_log(), ((1, command),))

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
            schema=manyfold.Schema.bytes(name="MemoryBytes"),
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
            schema=manyfold.Schema.bytes(name="MemoryBytes"),
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

    def test_memory_chip_does_not_reappend_resumed_latest_value(self) -> None:
        manyfold = load_manyfold_package()
        route = manyfold.route(
            plane=manyfold.Plane.Read,
            layer=manyfold.Layer.Logical,
            owner=manyfold.OwnerName("demo"),
            family=manyfold.StreamFamily("memory"),
            stream=manyfold.StreamName("bytes"),
            variant=manyfold.Variant.Meta,
            schema=manyfold.Schema.bytes(name="MemoryBytes"),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "memory.jsonl"
            original_graph = manyfold.Graph()
            original_memory = manyfold.Memory(path)
            original_subscription = original_memory.remember(
                original_graph, route, replay_latest=False
            )
            original_graph.publish(route, b"restored", control_epoch=10)
            original_subscription.dispose()

            resumed_graph = manyfold.Graph()
            resumed_memory = manyfold.Memory(path)
            resumed_memory.resume(resumed_graph, route)
            replay_subscription = resumed_memory.remember(resumed_graph, route)
            replay_subscription.dispose()

            records = manyfold.Memory(path).records(route)

        self.assertEqual([record.value for record in records], [b"restored"])
        self.assertEqual([record.control_epoch for record in records], [10])

    def test_memory_chip_seeds_duplicate_filter_from_existing_file(self) -> None:
        manyfold = load_manyfold_package()
        route = manyfold.route(
            plane=manyfold.Plane.Read,
            layer=manyfold.Layer.Logical,
            owner=manyfold.OwnerName("demo"),
            family=manyfold.StreamFamily("memory"),
            stream=manyfold.StreamName("bytes"),
            variant=manyfold.Variant.Meta,
            schema=manyfold.Schema.bytes(name="MemoryBytes"),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "memory.jsonl"
            graph = manyfold.Graph()
            graph.publish(route, b"already-recorded", control_epoch=7)
            first_memory = manyfold.Memory(path)
            first_subscription = first_memory.remember(graph, route)
            first_subscription.dispose()

            second_memory = manyfold.Memory(path)
            second_subscription = second_memory.remember(graph, route)
            second_subscription.dispose()
            records = manyfold.Memory(path).records(route)

        self.assertEqual([record.value for record in records], [b"already-recorded"])
        self.assertEqual([record.control_epoch for record in records], [7])

    def test_memory_chip_reports_corrupt_jsonl_line_with_path_and_line(self) -> None:
        manyfold = load_manyfold_package()

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "memory.jsonl"
            path.write_text(
                json.dumps(
                    {
                        "route": "read/logical/demo/memory/bytes/meta@MemoryBytes.v1",
                        "seq_source": 1,
                        "control_epoch": None,
                        "schema_id": "MemoryBytes",
                        "schema_version": 1,
                        "payload_b64": "",
                    }
                )
                + '\n{"route": ',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                r"memory file .*memory\.jsonl line 2 is not valid JSON",
            ):
                manyfold.Memory(path)

    def test_memory_chip_reports_missing_record_field_with_path_and_line(self) -> None:
        manyfold = load_manyfold_package()

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "memory.jsonl"
            path.write_text(
                json.dumps(
                    {
                        "route": "read/logical/demo/memory/bytes/meta@MemoryBytes.v1",
                        "seq_source": 1,
                        "control_epoch": None,
                        "schema_id": "MemoryBytes",
                        "schema_version": 1,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                r"memory file .*memory\.jsonl line 1 is missing payload_b64",
            ):
                manyfold.Memory(path)

    def test_memory_chip_reports_invalid_record_field_type_with_path_and_line(
        self,
    ) -> None:
        manyfold = load_manyfold_package()

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "memory.jsonl"
            path.write_text(
                json.dumps(
                    {
                        "route": "read/logical/demo/memory/bytes/meta@MemoryBytes.v1",
                        "seq_source": "1",
                        "control_epoch": None,
                        "schema_id": "MemoryBytes",
                        "schema_version": 1,
                        "payload_b64": "",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                r"memory file .*memory\.jsonl line 1 field seq_source "
                r"must be an integer",
            ):
                manyfold.Memory(path)

    def test_memory_chip_reports_invalid_record_payload_base64_with_path_and_line(
        self,
    ) -> None:
        manyfold = load_manyfold_package()

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "memory.jsonl"
            path.write_text(
                json.dumps(
                    {
                        "route": "read/logical/demo/memory/bytes/meta@MemoryBytes.v1",
                        "seq_source": 1,
                        "control_epoch": None,
                        "schema_id": "MemoryBytes",
                        "schema_version": 1,
                        "payload_b64": "not base64!",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError,
                r"memory file .*memory\.jsonl line 1 field payload_b64 "
                r"must be valid base64",
            ):
                manyfold.Memory(path)

    def test_memory_chip_rejects_route_schema_id_mismatch_before_decoding(self) -> None:
        manyfold = load_manyfold_package()
        producer_route = manyfold.route(
            plane=manyfold.Plane.Read,
            layer=manyfold.Layer.Logical,
            owner=manyfold.OwnerName("demo"),
            family=manyfold.StreamFamily("memory"),
            stream=manyfold.StreamName("payload"),
            variant=manyfold.Variant.Meta,
            schema=manyfold.Schema.bytes(name="OriginalBytes"),
        )
        consumer_route = manyfold.route(
            plane=manyfold.Plane.Read,
            layer=manyfold.Layer.Logical,
            owner=manyfold.OwnerName("demo"),
            family=manyfold.StreamFamily("memory"),
            stream=manyfold.StreamName("payload"),
            variant=manyfold.Variant.Meta,
            schema=manyfold.Schema(
                schema_id="OtherPayload",
                version=1,
                encode=lambda value: str(value).encode("ascii"),
                decode=lambda _payload: 0,
            ),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "memory.jsonl"
            graph = manyfold.Graph()
            memory = manyfold.Memory(path)
            subscription = memory.remember(graph, producer_route, replay_latest=False)
            graph.publish(producer_route, b"not-an-int")
            subscription.dispose()

            with self.assertRaisesRegex(
                ValueError,
                r"schema OriginalBytes v1 .* expected OtherPayload v1",
            ):
                manyfold.Memory(path).records(consumer_route)


if __name__ == "__main__":
    unittest.main()
