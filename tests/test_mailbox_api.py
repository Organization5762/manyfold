from __future__ import annotations

import subprocess
import sys
import unittest

from tests.test_support import load_manyfold_graph_module, subprocess_test_env


def load_graph_module():
    return load_manyfold_graph_module()


def run_manyfold_script(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        env=subprocess_test_env(),
        text=True,
    )


class MailboxApiTests(unittest.TestCase):
    def test_native_flow_snapshot_exposes_largest_queue_depth(self) -> None:
        script = """
import manyfold

graph = manyfold.Graph()
producer = manyfold.route(
    plane=manyfold.Plane.Read,
    layer=manyfold.Layer.Logical,
    owner=manyfold.OwnerName("sensor"),
    family=manyfold.StreamFamily("mailbox"),
    stream=manyfold.StreamName("producer"),
    variant=manyfold.Variant.Meta,
    schema=manyfold.Schema.bytes(name="MailboxProducer"),
)
mailbox = graph.mailbox(
    "native-depth",
    manyfold.MailboxDescriptor(capacity=5, overflow_policy="block"),
)
graph.connect(source=producer, sink=mailbox)
snapshot = graph.flow_snapshot(mailbox.ingress)
assert snapshot.largest_queue_depth == 0, snapshot
graph.publish(producer, b"one")
graph.publish(producer, b"two")
graph.publish(producer, b"three")
snapshot = graph.flow_snapshot(mailbox.ingress)
assert snapshot.largest_queue_depth == 3, snapshot
"""
        result = run_manyfold_script(script)

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_mailbox_descriptor_rejects_invalid_options_clearly(self) -> None:
        script = """
import manyfold

cases = (
    (
        {"capacity": 0},
        "capacity must be greater than zero",
    ),
    (
        {"delivery_mode": "fanout"},
        'unsupported delivery_mode "fanout"; expected one of mpsc_serial',
    ),
    (
        {"ordering_policy": "random"},
        'unsupported ordering_policy "random"; expected one of fifo',
    ),
    (
        {"overflow_policy": "panic"},
        'unsupported overflow_policy "panic"; expected one of block',
    ),
)
for kwargs, expected in cases:
    try:
        manyfold.MailboxDescriptor(**kwargs)
    except ValueError as exc:
        assert expected in str(exc), str(exc)
    else:
        raise AssertionError(f"expected ValueError for {kwargs}")
"""
        result = run_manyfold_script(script)

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_mailbox_descriptor_rejects_boolean_capacity(self) -> None:
        script = """
import manyfold

for capacity in (False, True):
    try:
        manyfold.MailboxDescriptor(capacity=capacity)
    except TypeError as exc:
        assert "capacity must be an integer, not bool" in str(exc), str(exc)
    else:
        raise AssertionError(f"expected TypeError for {capacity!r}")
"""
        result = run_manyfold_script(script)

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_graph_mailbox_rejects_blank_and_duplicate_names(self) -> None:
        script = """
import manyfold

graph = manyfold.Graph()
for name in ("", "   "):
    try:
        graph.mailbox(name)
    except ValueError as exc:
        assert "mailbox name must be a non-empty string" in str(exc), str(exc)
    else:
        raise AssertionError(f"expected ValueError for {name!r}")

graph.mailbox("events")
try:
    graph.mailbox("events")
except ValueError as exc:
    assert 'mailbox "events" already exists' in str(exc), str(exc)
else:
    raise AssertionError("expected ValueError for duplicate mailbox")
"""
        result = run_manyfold_script(script)

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_mailbox_snapshot_exposes_semantics_and_delivery_counters(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("events"),
            stream=graph_module.StreamName("raw"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="SensorEvent"),
        )
        sink = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("worker"),
            family=graph_module.StreamFamily("mailbox"),
            stream=graph_module.StreamName("ingest"),
            variant=graph_module.Variant.Request,
            schema=graph_module.Schema.bytes(name="WorkerInbox"),
        )
        graph = graph_module.Graph()
        mailbox = graph.mailbox(
            "worker-inbox",
            graph_module.NativeMailboxDescriptor(
                capacity=2,
                delivery_mode="mpmc_unique",
                ordering_policy="round_robin_by_producer",
                overflow_policy="drop_oldest",
            ),
        )
        graph.connect(source=source, sink=mailbox)
        graph.connect(source=mailbox, sink=sink)

        graph.publish(source, b"one")
        graph.publish(source, b"two")

        snapshot = graph.mailbox_snapshot(mailbox)

        self.assertEqual(snapshot.name, "worker-inbox")
        self.assertEqual(snapshot.capacity, 2)
        self.assertEqual(snapshot.delivery_mode, "mpmc_unique")
        self.assertEqual(snapshot.ordering_policy, "round_robin_by_producer")
        self.assertEqual(snapshot.overflow_policy, "drop_oldest")
        self.assertEqual(snapshot.depth, 0)
        self.assertEqual(snapshot.available_credit, 2)
        self.assertEqual(snapshot.blocked_writes, 0)
        self.assertEqual(snapshot.dropped_messages, 0)
        self.assertEqual(snapshot.coalesced_messages, 0)
        self.assertEqual(snapshot.delivered_messages, 2)

    def test_mailbox_snapshot_tracks_coalesced_latest_updates(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("events"),
            stream=graph_module.StreamName("raw"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="SensorEvent"),
        )
        graph = graph_module.Graph()
        mailbox = graph.mailbox(
            "coalescing-inbox",
            graph_module.NativeMailboxDescriptor(
                capacity=1,
                delivery_mode="mpsc_serial",
                ordering_policy="latest_only",
                overflow_policy="coalesce_latest",
            ),
        )
        graph.connect(source=source, sink=mailbox)

        graph.publish(source, b"one")
        graph.publish(source, b"two")

        snapshot = graph.mailbox_snapshot(mailbox)

        self.assertEqual(snapshot.delivery_mode, "mpsc_serial")
        self.assertEqual(snapshot.ordering_policy, "latest_only")
        self.assertEqual(snapshot.depth, 1)
        self.assertEqual(snapshot.available_credit, 0)
        self.assertEqual(snapshot.dropped_messages, 0)
        self.assertEqual(snapshot.coalesced_messages, 1)
        self.assertEqual(snapshot.delivered_messages, 0)

    def test_mailbox_snapshot_treats_reject_write_as_blocking_overflow(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("events"),
            stream=graph_module.StreamName("raw"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="SensorEvent"),
        )
        graph = graph_module.Graph()
        mailbox = graph.mailbox(
            "rejecting-inbox",
            graph_module.NativeMailboxDescriptor(
                capacity=1,
                overflow_policy="reject_write",
            ),
        )
        graph.connect(source=source, sink=mailbox)

        graph.publish(source, b"one")
        graph.publish(source, b"two")

        snapshot = graph.mailbox_snapshot(mailbox)

        self.assertEqual(snapshot.overflow_policy, "reject_write")
        self.assertEqual(snapshot.depth, 1)
        self.assertEqual(snapshot.available_credit, 0)
        self.assertEqual(snapshot.blocked_writes, 1)
        self.assertEqual(snapshot.dropped_messages, 0)
        self.assertEqual(snapshot.coalesced_messages, 0)

    def test_mailbox_snapshot_tracks_drop_style_overflow_policies(self) -> None:
        graph_module = load_graph_module()

        for overflow_policy in ("drop_newest", "deadline_drop", "spill_to_store"):
            with self.subTest(overflow_policy=overflow_policy):
                source = graph_module.route(
                    plane=graph_module.Plane.Read,
                    layer=graph_module.Layer.Logical,
                    owner=graph_module.OwnerName("sensor"),
                    family=graph_module.StreamFamily("events"),
                    stream=graph_module.StreamName(overflow_policy),
                    variant=graph_module.Variant.Meta,
                    schema=graph_module.Schema.bytes(name="SensorEvent"),
                )
                graph = graph_module.Graph()
                mailbox = graph.mailbox(
                    f"{overflow_policy}-inbox",
                    graph_module.NativeMailboxDescriptor(
                        capacity=1,
                        overflow_policy=overflow_policy,
                    ),
                )
                graph.connect(source=source, sink=mailbox)

                graph.publish(source, b"one")
                graph.publish(source, b"two")

                snapshot = graph.mailbox_snapshot(mailbox)

                self.assertEqual(snapshot.overflow_policy, overflow_policy)
                self.assertEqual(snapshot.depth, 1)
                self.assertEqual(snapshot.available_credit, 0)
                self.assertEqual(snapshot.blocked_writes, 0)
                self.assertEqual(snapshot.dropped_messages, 1)
                self.assertEqual(snapshot.coalesced_messages, 0)
                self.assertEqual(snapshot.delivered_messages, 0)
