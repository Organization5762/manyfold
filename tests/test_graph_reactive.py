from __future__ import annotations

import importlib
import sys
import threading
import time
import unittest
from dataclasses import dataclass

from tests.test_support import load_manyfold_graph_module


def load_graph_module():
    return load_manyfold_graph_module()


def int_schema(graph_module, schema_id: str):
    return graph_module.Schema(
        schema_id=schema_id,
        version=1,
        encode=lambda value: str(value).encode("ascii"),
        decode=lambda payload: int(payload.decode("ascii")),
    )


def str_schema(graph_module, schema_id: str):
    return graph_module.Schema(
        schema_id=schema_id,
        version=1,
        encode=lambda value: value.encode("ascii"),
        decode=lambda payload: payload.decode("ascii"),
    )


class ManualDisposable:
    def dispose(self) -> None:
        pass


class ManualCoalesceScheduler:
    def __init__(self) -> None:
        self.callbacks = []

    def schedule_relative(self, _duetime, action):
        self.callbacks.append(action)
        return ManualDisposable()


class ManualTimer:
    def __init__(self, _interval, function) -> None:
        self.daemon = False
        self.function = function

    def start(self) -> None:
        pass

    def cancel(self) -> None:
        pass


class FailingObservable:
    def __init__(self, message: str) -> None:
        self.message = message

    def subscribe(self, *args, **kwargs):
        del args, kwargs
        raise RuntimeError(self.message)


class GraphReactiveTests(unittest.TestCase):
    def test_producer_kind_exposes_all_runtime_kinds(self) -> None:
        graph_module = load_graph_module()

        def enum_value(value) -> str:
            return getattr(value, "value", str(value))

        route = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Shadow,
            owner=graph_module.OwnerName("lamp"),
            family=graph_module.StreamFamily("brightness"),
            stream=graph_module.StreamName("level"),
            variant=graph_module.Variant.Desired,
            schema=graph_module.Schema.bytes(name="Brightness"),
        )
        graph = graph_module.Graph()

        self.assertEqual(enum_value(graph_module.ProducerKind.Bridge), "bridge")
        self.assertEqual(enum_value(graph_module.ProducerKind.Reconciler), "reconciler")
        self.assertEqual(
            enum_value(graph_module.ProducerKind.LifecycleService),
            "lifecycle_service",
        )
        self.assertEqual(
            enum_value(graph.describe_route(route).identity.producer_ref.kind),
            "reconciler",
        )

    def test_observe_replays_latest_and_pushes_future_writes(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Accel"),
        )
        graph = graph_module.Graph()

        graph.publish(route, b"first")
        observed = []
        subscription = graph.observe(route).subscribe(
            lambda envelope: observed.append(envelope)
        )
        graph.publish(route, b"second")
        subscription.dispose()

        self.assertEqual([item.value for item in observed], [b"first", b"second"])
        self.assertEqual([item.closed.seq_source for item in observed], [1, 2])

    def test_observe_can_skip_latest_replay_and_tracks_live_subscribers(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Gyro"),
        )
        graph = graph_module.Graph()

        graph.publish(route, b"first")
        observed = []
        subscription = graph.observe(route, replay_latest=False).subscribe(
            lambda envelope: observed.append(envelope)
        )

        self.assertEqual(graph.subscribers(route), 1)

        graph.publish(route, b"second")
        subscription.dispose()

        self.assertEqual([item.value for item in observed], [b"second"])
        self.assertEqual(graph.subscribers(route), 0)

    def test_observe_rolls_back_subscriber_tracking_when_latest_replay_fails(
        self,
    ) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("faulty_replay"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="FaultyReplay"),
        )
        graph = graph_module.Graph()

        graph.publish(route, b"first")

        def fail(_envelope) -> None:
            raise RuntimeError("observer rejected replay")

        with self.assertRaisesRegex(RuntimeError, "observer rejected replay"):
            graph.observe(route, subscriber_id="dashboard").subscribe(fail)

        self.assertEqual(graph.subscribers(route), 0)
        self.assertEqual(graph.route_audit(route).active_subscribers, ())

    def test_readable_port_observe_keeps_reentrant_replay_publications(
        self,
    ) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("port_replay"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="PortReplay"),
        )
        graph = graph_module.Graph()
        graph.publish(route, b"first")
        observed_sequences: list[int] = []

        def publish_during_replay(envelope) -> None:
            observed_sequences.append(envelope.seq_source)
            if envelope.seq_source == 1:
                graph.publish(route, b"second")

        subscription = graph._read_port(route).observe().subscribe(publish_during_replay)
        subscription.dispose()

        self.assertEqual(observed_sequences, [1, 2])

    def test_pipe_binds_rx_source_into_writable_port(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("motor"),
            family=graph_module.StreamFamily("speed"),
            stream=graph_module.StreamName("command"),
            variant=graph_module.Variant.Request,
            schema=graph_module.Schema.bytes(name="SpeedCommand"),
        )
        graph = graph_module.Graph()

        graph.pipe(graph_module.rx.from_iterable([b"one", b"two"]), route)
        latest = graph.latest(route)

        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value, b"two")
        self.assertEqual(latest.closed.seq_source, 2)

    def test_any_schema_preserves_local_objects_through_graph_observe(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("window"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.any("RuntimeWindow"),
        )
        graph = graph_module.Graph()
        window = object()
        seen: list[object] = []

        graph.observe(route).subscribe(lambda envelope: seen.append(envelope.value))
        graph.publish(route, window)
        latest = graph.latest(route)

        self.assertIs(seen[0], window)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertIs(latest.value, window)

    def test_observe_pipeline_installs_map_filter_and_callback_nodes(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("numbers"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "RuntimeNumber"),
        )
        graph = graph_module.Graph()
        seen: list[int] = []

        connection = (
            graph.observe(route)
            .map(lambda value: value + 1, name="plus-one")
            .filter(lambda value: value > 2, name="gt-two")
            .callback(seen.append, name="collect")
        )
        graph.publish(route, 1)
        graph.publish(route, 2)

        self.assertEqual(seen, [3])
        self.assertEqual(
            [node.name for node in graph.diagram_nodes()],
            ["plus-one", "gt-two", "collect"],
        )

        connection.remove()
        graph.publish(route, 3)

        self.assertEqual(seen, [3])
        self.assertEqual(list(graph.diagram_nodes()), [])

    def test_transform_pipeline_replays_existing_latest_to_callback(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("latest_number"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "LatestRuntimeNumber"),
        )
        graph = graph_module.Graph()
        seen: list[int] = []

        graph.publish(route, 4)
        connection = graph.observe(route).map(lambda value: value * 2).callback(
            seen.append
        )

        connection.remove()
        self.assertEqual(seen, [8])

    def test_transform_pipeline_replays_existing_latest_through_chained_nodes(
        self,
    ) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("latest_chain_number"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "LatestChainRuntimeNumber"),
        )
        graph = graph_module.Graph()
        seen: list[int] = []

        graph.publish(route, 2)
        connection = (
            graph.observe(route)
            .map(lambda value: value + 3)
            .filter(lambda value: value > 4)
            .map(lambda value: value * 10)
            .callback(seen.append)
        )

        connection.remove()
        self.assertEqual(seen, [50])

    def test_route_pipeline_remove_cleans_up_topology_edge(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("routed_source"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "RoutedSourceNumber"),
        )
        target = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("routed_target"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "RoutedTargetNumber"),
        )
        graph = graph_module.Graph()

        connection = graph.observe(source, replay_latest=False).connect(target)
        self.assertEqual(
            list(graph.topology()),
            [(source.route_ref.display(), target.route_ref.display())],
        )

        connection.remove()

        self.assertEqual(list(graph.topology()), [])

    def test_route_pipeline_disconnects_topology_when_latest_replay_fails(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("failing_route_source"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "FailingRouteSourceNumber"),
        )
        target = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("failing_route_target"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema(
                schema_id="FailingRouteTargetNumber",
                version=1,
                encode=lambda _value: (_ for _ in ()).throw(
                    RuntimeError("target rejected latest")
                ),
                decode=lambda payload: int(payload.decode("ascii")),
            ),
        )
        graph = graph_module.Graph()

        graph.publish(source, 1)

        with self.assertRaisesRegex(RuntimeError, "target rejected latest"):
            graph.observe(source).connect(target)

        self.assertEqual(list(graph.topology()), [])
        self.assertEqual(graph.subscribers(source), 0)

    def test_connect_is_idempotent_for_public_graph_fanout(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("core_source"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="CoreSource"),
        )
        sink = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("core_sink"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="CoreSink"),
        )
        graph = graph_module.Graph()

        graph.connect(source=source, sink=sink)
        graph.connect(source=source, sink=sink)
        graph.publish(source, b"sample")

        latest = graph.latest(sink)

        self.assertEqual(
            list(graph.topology()),
            [(source.route_ref.display(), sink.route_ref.display())],
        )
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value, b"sample")
        self.assertEqual(latest.closed.seq_source, 1)
        self.assertEqual(graph.disconnect(source=source, sink=sink), True)
        self.assertEqual(graph.disconnect(source=source, sink=sink), False)

    def test_callback_pipeline_removes_node_when_latest_replay_fails(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("failing_callback_latest"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "FailingCallbackRuntimeNumber"),
        )
        graph = graph_module.Graph()

        graph.publish(route, 1)

        def fail(_value: int) -> None:
            raise RuntimeError("callback rejected latest")

        with self.assertRaisesRegex(RuntimeError, "callback rejected latest"):
            graph.observe(route).callback(fail, name="failing-callback")

        self.assertEqual(list(graph.diagram_nodes()), [])
        self.assertEqual(graph.subscribers(route), 0)

    def test_transform_pipeline_removes_node_when_latest_replay_fails(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("failing_transform_latest"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "FailingTransformRuntimeNumber"),
        )
        graph = graph_module.Graph()

        graph.publish(route, 1)

        def fail(_value: int) -> int:
            raise RuntimeError("transform rejected latest")

        with self.assertRaisesRegex(RuntimeError, "transform rejected latest"):
            graph.observe(route).map(fail, name="failing-map")

        self.assertEqual(list(graph.diagram_nodes()), [])
        self.assertEqual(graph.subscribers(route), 0)

    def test_registered_pipeline_operation_extends_fluent_route_pipeline(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("signed_numbers"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "SignedRuntimeNumber"),
        )
        graph = graph_module.Graph()
        seen: list[int] = []
        graph.register_pipeline_operation(
            "positive",
            lambda name="positive": graph_module.FilterNode(
                name,
                lambda value: value > 0,
            ),
        )

        graph.observe(route, replay_latest=False).positive().callback(
            seen.append,
            name="collect-positive",
        )
        graph.publish(route, -1)
        graph.publish(route, 5)

        self.assertEqual(seen, [5])

    def test_pipeline_can_place_following_nodes_on_main_thread(self) -> None:
        graph_module = load_graph_module()
        reactive_threads = importlib.import_module("manyfold.reactive_threads")
        reactive_threads.reset_reactive_threading_state_for_tests()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("main_thread_values"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "MainThreadRuntimeNumber"),
        )
        graph = graph_module.Graph()
        seen: list[int] = []
        connection = graph.observe(route, replay_latest=False).on_main_thread().callback(
            seen.append,
            name="collect-main",
        )

        try:
            graph.publish(route, 3)

            self.assertEqual(seen, [])
            self.assertEqual(reactive_threads.drain_frame_thread_queue(), 1)
            self.assertEqual(seen, [3])
            node = next(graph.diagram_nodes())
            self.assertEqual(node.thread_placement.kind, "main")
            self.assertIn("main_thread", graph.render_diagram(group_by=("thread",)))
        finally:
            connection.remove()
            reactive_threads.reset_reactive_threading_state_for_tests()

    def test_pipeline_can_place_following_nodes_on_pooled_thread(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("pooled_values"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "PooledRuntimeNumber"),
        )
        graph = graph_module.Graph()
        main_thread_id = threading.get_ident()
        done = threading.Event()
        seen: list[int] = []
        thread_ids: list[int] = []

        def collect(value: int) -> None:
            thread_ids.append(threading.get_ident())
            seen.append(value)
            done.set()

        connection = graph.observe(route, replay_latest=False).on_pooled_thread().callback(
            collect,
            name="collect-pooled",
        )

        try:
            graph.publish(route, 4)

            self.assertTrue(done.wait(timeout=2), "pooled callback did not run")
            self.assertEqual(seen, [4])
            self.assertNotEqual(thread_ids[0], main_thread_id)
            node = next(graph.diagram_nodes())
            self.assertEqual(node.thread_placement.kind, "pooled")
        finally:
            connection.remove()

    def test_node_isolated_thread_placement_propagates_downstream(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("isolated_values"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "IsolatedRuntimeNumber"),
        )
        graph = graph_module.Graph()
        done = threading.Event()
        map_thread_names: list[str] = []
        seen: list[int] = []

        def transform(value: int) -> int:
            map_thread_names.append(threading.current_thread().name)
            return value + 1

        connection = (
            graph.observe(route, replay_latest=False)
            .on_isolated_thread("exclusive-node")
            .map(
                transform,
                name="exclusive-map",
            )
            .callback(lambda value: (seen.append(value), done.set()), name="collect")
        )

        try:
            graph.publish(route, 4)

            self.assertTrue(done.wait(timeout=2), "isolated callback did not run")
            self.assertEqual(seen, [5])
            self.assertEqual(map_thread_names, ["exclusive-node"])
            map_node = next(
                node for node in graph.diagram_nodes() if node.name == "exclusive-map"
            )
            collect_node = next(
                node for node in graph.diagram_nodes() if node.name == "collect"
            )
            self.assertEqual(map_node.thread_placement.kind, "isolated")
            self.assertEqual(collect_node.thread_placement.kind, "isolated")
            self.assertEqual(collect_node.thread_placement.thread_name, "exclusive-node")
        finally:
            connection.remove()

    def test_pipeline_can_return_to_prior_thread_placement(self) -> None:
        graph_module = load_graph_module()
        reactive_threads = importlib.import_module("manyfold.reactive_threads")
        reactive_threads.reset_reactive_threading_state_for_tests()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("return_thread_values"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "ReturnThreadRuntimeNumber"),
        )
        graph = graph_module.Graph()
        main_thread_id = threading.get_ident()
        pooled_seen = threading.Event()
        callback_seen = threading.Event()
        thread_ids: dict[str, int] = {}
        values: list[int] = []

        def pooled_step(value: int) -> int:
            thread_ids["pooled"] = threading.get_ident()
            pooled_seen.set()
            return value + 1

        def main_step(value: int) -> int:
            thread_ids["main"] = threading.get_ident()
            return value + 1

        def collect(value: int) -> None:
            thread_ids["callback"] = threading.get_ident()
            values.append(value)
            callback_seen.set()

        connection = (
            graph.observe(route, replay_latest=False)
            .on_pooled_thread()
            .map(pooled_step, name="pooled-step")
            .on_main_thread()
            .map(main_step, name="main-step")
            .return_to_prior_thread()
            .callback(collect, name="collect-prior")
        )

        try:
            graph.publish(route, 10)
            self.assertTrue(pooled_seen.wait(timeout=2), "pooled step did not run")
            self.assertEqual(values, [])
            self.assertEqual(reactive_threads.drain_frame_thread_queue(), 1)
            self.assertTrue(callback_seen.wait(timeout=2), "callback did not run")

            self.assertEqual(values, [12])
            self.assertNotEqual(thread_ids["pooled"], main_thread_id)
            self.assertEqual(thread_ids["main"], main_thread_id)
            self.assertNotEqual(thread_ids["callback"], main_thread_id)
            placements = {
                node.name: node.thread_placement
                for node in graph.diagram_nodes()
            }
            self.assertEqual(placements["pooled-step"].kind, "pooled")
            self.assertEqual(placements["main-step"].kind, "main")
            self.assertEqual(placements["collect-prior"].kind, "pooled")
        finally:
            connection.remove()
            reactive_threads.reset_reactive_threading_state_for_tests()

    def test_coalesce_latest_node_emits_latest_value_on_completion(self) -> None:
        graph_module = load_graph_module()
        node = graph_module.CoalesceLatestNode(
            name="coalesce",
            window_ms=1000,
            stream_name="numbers",
        )
        seen: list[int] = []
        source = graph_module.Subject()

        node.observable(source).subscribe(seen.append)
        source.on_next(1)
        source.on_next(2)
        source.on_next(3)
        source.on_completed()

        self.assertEqual(seen, [3])

    def test_coalesce_latest_ignores_stale_timer_callbacks(self) -> None:
        graph_module = load_graph_module()
        scheduler = ManualCoalesceScheduler()
        original_scheduler = graph_module.reactive_threads.coalesce_scheduler
        original_timer = graph_module.Timer
        graph_module.reactive_threads.coalesce_scheduler = lambda: scheduler
        graph_module.Timer = ManualTimer
        node = graph_module.CoalesceLatestNode(
            name="coalesce",
            window_ms=1000,
            stream_name="numbers",
        )
        seen: list[int] = []
        source = graph_module.Subject()

        try:
            node.observable(source).subscribe(seen.append)
            source.on_next(1)
            source.on_next(2)

            scheduler.callbacks[0]()
            self.assertEqual(seen, [])

            scheduler.callbacks[1]()
            self.assertEqual(seen, [2])
        finally:
            graph_module.reactive_threads.coalesce_scheduler = original_scheduler
            graph_module.Timer = original_timer

    def test_observe_pipeline_coalesces_latest_route_value(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("coalesce_numbers"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "CoalesceRuntimeNumber"),
        )
        graph = graph_module.Graph()
        seen: list[int] = []

        connection = graph.observe(route, replay_latest=False).coalesce_latest(
            window_ms=100,
            name="coalesce",
            stream_name="numbers",
        ).callback(seen.append)
        try:
            graph.publish(route, 1)
            graph.publish(route, 2)
            deadline = time.monotonic() + 2.0
            while not seen and time.monotonic() < deadline:
                time.sleep(0.01)

            self.assertEqual(seen, [2])
            self.assertEqual(
                [node.name for node in graph.diagram_nodes()],
                ["coalesce", "callback-1"],
            )
        finally:
            connection.remove()

    def test_coalesce_latest_preserves_main_thread_placement_downstream(self) -> None:
        graph_module = load_graph_module()
        reactive_threads = importlib.import_module("manyfold.reactive_threads")
        reactive_threads.reset_reactive_threading_state_for_tests()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("main_thread_coalesced_values"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "MainThreadCoalescedRuntimeNumber"),
        )
        graph = graph_module.Graph()
        seen: list[int] = []
        connection = (
            graph.observe(route, replay_latest=False)
            .on_main_thread()
            .coalesce_latest(window_ms=1, name="coalesce-main")
            .callback(seen.append, name="collect-main-coalesced")
        )

        try:
            graph.publish(route, 4)
            self.assertEqual(seen, [])
            self.assertEqual(reactive_threads.drain_frame_thread_queue(), 1)
            time.sleep(0.05)

            self.assertEqual(seen, [])
            self.assertEqual(reactive_threads.drain_frame_thread_queue(), 1)
            self.assertEqual(seen, [4])
            placements = {
                node.name: node.thread_placement
                for node in graph.diagram_nodes()
            }
            self.assertEqual(placements["coalesce-main"].kind, "main")
            self.assertEqual(placements["collect-main-coalesced"].kind, "main")
        finally:
            connection.remove()
            reactive_threads.reset_reactive_threading_state_for_tests()

    def test_instrument_stream_logs_periodic_delivery_stats(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.Subject()
        seen: list[int] = []

        with self.assertLogs("manyfold.graph", level="DEBUG") as logs:
            subscription = graph_module.instrument_stream(
                source,
                stream_name="numbers",
                log_interval_ms=1,
            ).subscribe(seen.append)
            try:
                source.on_next(1)
                source.on_next(2)
                time.sleep(0.05)
            finally:
                subscription.dispose()

        self.assertEqual(seen, [1, 2])
        self.assertTrue(
            any(
                "Stream stats for numbers events=2 subscribers=1 interval_ms=1"
                in message
                for message in logs.output
            )
        )

    def test_observe_pipeline_installs_logging_node(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart"),
            family=graph_module.StreamFamily("runtime"),
            stream=graph_module.StreamName("logged_numbers"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "LoggedRuntimeNumber"),
        )
        graph = graph_module.Graph()
        seen: list[int] = []

        connection = graph.observe(route, replay_latest=False).log(
            interval_ms=1,
            name="log-values",
            stream_name="numbers",
        ).callback(seen.append)
        try:
            graph.publish(route, 7)

            self.assertEqual(seen, [7])
            self.assertEqual(
                [node.name for node in graph.diagram_nodes()],
                ["log-values", "callback-1"],
            )
        finally:
            connection.remove()

    def test_read_then_write_next_epoch_step_installs_shared_write_stream(self) -> None:
        graph_module = load_graph_module()
        write_request = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("motor"),
            family=graph_module.StreamFamily("speed"),
            stream=graph_module.StreamName("pid"),
            variant=graph_module.Variant.Request,
            schema=graph_module.Schema.bytes(name="SpeedPid"),
        )
        step = graph_module.ReadThenWriteNextEpochStep.map(
            name="Step",
            read=graph_module.rx.from_iterable([b"one", b"two"]),
            output=write_request,
            transform=lambda payload: payload.upper(),
        )
        graph = graph_module.Graph()
        mirrored = []
        step.write.subscribe(lambda payload: mirrored.append(payload))
        graph.install(step)

        latest = graph.latest(write_request)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(mirrored, [b"ONE", b"TWO"])
        self.assertEqual(latest.value, b"TWO")
        self.assertEqual(latest.closed.seq_source, 2)

    def test_plan_join_exposes_repartition_nodes(self) -> None:
        graph_module = load_graph_module()
        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("left"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Accel"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("right"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Gyro"),
        )
        graph = graph_module.Graph()

        plan = graph.plan_join(
            "imu_fusion",
            graph_module.JoinInput(left, partition_key_semantics="device_id"),
            graph_module.JoinInput(
                right,
                partition_key_semantics="axis_id",
                deterministic_rekey=True,
            ),
        )

        self.assertEqual(plan.join_class, "repartition")
        self.assertEqual(len(plan.visible_nodes), 2)
        self.assertEqual(graph.explain_join("imu_fusion").join_class, "repartition")
        topology = list(graph.topology())
        self.assertEqual(len(topology), 2)

    def test_topology_edges_are_reported_in_stable_display_order(self) -> None:
        graph_module = load_graph_module()

        def route(owner: str, stream: str):
            return graph_module.route(
                plane=graph_module.Plane.Read,
                layer=graph_module.Layer.Logical,
                owner=graph_module.OwnerName(owner),
                family=graph_module.StreamFamily("topology"),
                stream=graph_module.StreamName(stream),
                variant=graph_module.Variant.Meta,
                schema=graph_module.Schema.bytes(name="StableTopology"),
            )

        graph = graph_module.Graph()
        left_source = route("zeta", "source")
        left_sink = route("zeta", "sink")
        right_source = route("alpha", "source")
        right_sink = route("alpha", "sink")

        graph.connect(source=left_source, sink=left_sink)
        graph.connect(source=right_source, sink=right_sink)

        self.assertEqual(
            list(graph.topology()),
            [
                (right_source.display(), right_sink.display()),
                (left_source.display(), left_sink.display()),
            ],
        )

    def test_plan_join_uses_lookup_when_right_side_is_materialized_view(self) -> None:
        graph_module = load_graph_module()
        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Accel"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("catalog"),
            family=graph_module.StreamFamily("device"),
            stream=graph_module.StreamName("calibration"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.bytes(name="Calibration"),
        )
        graph = graph_module.Graph()

        plan = graph.plan_join(
            "imu_lookup",
            graph_module.JoinInput(left, partition_key_semantics="device_id"),
            graph_module.JoinInput(
                right,
                partition_key_semantics="calibration_id",
                materialized_view=True,
            ),
        )

        self.assertEqual(plan.join_class, "lookup")
        self.assertEqual(plan.state_budget, "right_materialized_view")
        self.assertEqual(plan.taint_implications, ("snapshot_consistency_required",))
        self.assertEqual(plan.visible_nodes, ())
        self.assertEqual(list(graph.topology()), [])

    def test_plan_join_uses_broadcast_mirror_when_side_is_marked_eligible(self) -> None:
        graph_module = load_graph_module()
        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Accel"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("dashboard"),
            family=graph_module.StreamFamily("device"),
            stream=graph_module.StreamName("selection"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Selection"),
        )
        graph = graph_module.Graph()

        plan = graph.plan_join(
            "broadcast_overlay",
            graph_module.JoinInput(left, partition_key_semantics="device_id"),
            graph_module.JoinInput(
                right,
                partition_key_semantics="selection_id",
                deterministic_rekey=False,
                broadcast_mirror_eligible=True,
            ),
        )

        self.assertEqual(plan.join_class, "broadcast_mirror")
        self.assertEqual(plan.state_budget, "mirror_memory")
        self.assertEqual(plan.taint_implications, ("order_insensitive_broadcast",))
        self.assertEqual(plan.visible_nodes, ())
        self.assertEqual(list(graph.topology()), [])

    def test_plan_join_rejects_incompatible_cross_partition_join_without_strategy(
        self,
    ) -> None:
        graph_module = load_graph_module()
        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("left"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Accel"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("right"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Gyro"),
        )
        graph = graph_module.Graph()

        with self.assertRaisesRegex(
            ValueError,
            "join partition keys are incompatible without repartition or broadcast",
        ):
            graph.plan_join(
                "illegal_join",
                graph_module.JoinInput(
                    left,
                    partition_key_semantics="device_id",
                    deterministic_rekey=False,
                ),
                graph_module.JoinInput(
                    right,
                    partition_key_semantics="axis_id",
                    deterministic_rekey=False,
                ),
            )

    def test_stateful_map_publishes_running_state_transforms(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("counter"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("raw"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "CounterValue"),
        )
        output = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("counter"),
            family=graph_module.StreamFamily("state"),
            stream=graph_module.StreamName("running_total"),
            variant=graph_module.Variant.State,
            schema=int_schema(graph_module, "RunningTotal"),
        )
        graph = graph_module.Graph()
        observed = []
        graph.observe(output).subscribe(
            lambda envelope: observed.append(envelope.value)
        )

        graph.stateful_map(
            source,
            initial_state=0,
            step=lambda state, value: (state + value, state + value),
            output=output,
        )
        graph.publish(source, 2)
        graph.publish(source, 3)

        self.assertEqual(observed, [2, 5])
        latest = graph.latest(output)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value, 5)

    def test_stateful_map_accepts_raw_route_refs_as_bytes(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("counter"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("raw"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="CounterBytes"),
        )
        output = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("counter"),
            family=graph_module.StreamFamily("state"),
            stream=graph_module.StreamName("running_total"),
            variant=graph_module.Variant.State,
            schema=int_schema(graph_module, "RunningTotal"),
        )
        graph = graph_module.Graph()
        observed = []
        graph.observe(output).subscribe(
            lambda envelope: observed.append(envelope.value)
        )

        graph.stateful_map(
            source.route_ref,
            initial_state=0,
            step=lambda state, value: (
                state + int(value.decode("ascii")),
                state + int(value.decode("ascii")),
            ),
            output=output,
        )
        graph.publish(source, b"2")
        graph.publish(source, b"3")

        self.assertEqual(observed, [2, 5])

    def test_stateful_map_preserves_source_taints_on_output(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Ephemeral,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("entropy"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="EntropyBytes"),
        )
        output = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("entropy_state"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.bytes(name="EntropyState"),
        )
        graph = graph_module.Graph()

        subscription = graph.stateful_map(
            source,
            initial_state=0,
            step=lambda state, value: (state + 1, value),
            output=output,
        )
        graph.publish(source, b"nonce-1")
        subscription.dispose()

        latest = graph.latest(output)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(
            tuple(
                taint.value_id
                for taint in latest.closed.taints
                if getattr(taint.domain, "as_str", lambda: taint.domain)()
                == "determinism"
            ),
            ("DET_NONREPLAYABLE",),
        )

    def test_stateful_map_deduplicates_matching_source_and_output_taints(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Ephemeral,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("entropy"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="EntropyBytes"),
        )
        output = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Ephemeral,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("entropy_state"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.bytes(name="EntropyState"),
        )
        graph = graph_module.Graph()

        subscription = graph.stateful_map(
            source,
            initial_state=0,
            step=lambda state, value: (state + 1, value),
            output=output,
        )
        graph.publish(source, b"nonce-1")
        subscription.dispose()

        latest = graph.latest(output)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(
            tuple(
                taint.value_id
                for taint in latest.closed.taints
                if getattr(taint.domain, "as_str", lambda: taint.domain)()
                == "determinism"
            ),
            ("DET_NONREPLAYABLE",),
        )

    def test_filter_replays_latest_and_emits_future_matching_values(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("frame_meta"),
            variant=graph_module.Variant.Meta,
            schema=str_schema(graph_module, "LidarFrameMeta"),
        )
        graph = graph_module.Graph()

        graph.publish(source, "frame-1:drop")
        graph.publish(source, "frame-2:open")

        matched: list[str] = []
        subscription = graph.filter(
            source,
            predicate=lambda frame: frame.endswith(":open"),
        ).subscribe(matched.append)

        graph.publish(source, "frame-3:drop")
        graph.publish(source, "frame-4:open")
        subscription.dispose()

        self.assertEqual(matched, ["frame-2:open", "frame-4:open"])

    def test_filter_subscribes_before_replaying_latest_value(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("reentrant_frame"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "ReentrantFrame"),
        )
        graph = graph_module.Graph()
        graph.publish(source, 1)
        matched: list[int] = []

        def receive(value: int) -> None:
            matched.append(value)
            if value == 1:
                graph.publish(source, 2)

        subscription = graph.filter(
            source,
            predicate=lambda value: value > 0,
        ).subscribe(receive)
        subscription.dispose()

        self.assertEqual(matched, [1, 2])

    def test_filter_accepts_raw_route_refs_as_bytes(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("uart"),
            stream=graph_module.StreamName("line"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="UartLine"),
        )
        graph = graph_module.Graph()
        matched: list[bytes] = []

        subscription = graph.filter(
            source.route_ref,
            predicate=lambda payload: payload.startswith(b"ok:"),
        ).subscribe(matched.append)

        graph.publish(source, b"skip")
        graph.publish(source, b"ok:1")
        graph.publish(source, b"ok:2")
        subscription.dispose()

        self.assertEqual(matched, [b"ok:1", b"ok:2"])

    def test_window_replays_latest_and_tracks_sliding_values(self) -> None:
        graph_module = load_graph_module()

        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Temperature"),
        )
        graph = graph_module.Graph()
        graph.publish(route, 20)

        windows = []
        subscription = graph.window(route, size=2).subscribe(
            lambda items: windows.append(items)
        )
        graph.publish(route, 21)
        graph.publish(route, 22)
        subscription.dispose()

        self.assertEqual(windows, [[20], [20, 21], [21, 22]])

    def test_window_partitions_buffers_by_key(self) -> None:
        graph_module = load_graph_module()

        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("axes"),
            variant=graph_module.Variant.Meta,
            schema=str_schema(graph_module, "AxisReading"),
        )
        graph = graph_module.Graph()

        windows = []
        subscription = graph.window(
            route,
            size=2,
            partition_by=lambda value: value.split(":")[0],
        ).subscribe(windows.append)
        graph.publish(route, "left:20")
        graph.publish(route, "right:40")
        graph.publish(route, "left:21")
        graph.publish(route, "right:41")
        subscription.dispose()

        self.assertEqual(
            windows,
            [
                ["left:20"],
                ["right:40"],
                ["left:20", "left:21"],
                ["right:40", "right:41"],
            ],
        )

    def test_window_accepts_raw_route_refs_and_emits_bytes(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="TemperatureBytes"),
        )
        graph = graph_module.Graph()
        graph.publish(route, b"20")

        windows = []
        subscription = graph.window(route.route_ref, size=2).subscribe(
            lambda items: windows.append(items)
        )
        graph.publish(route, b"21")
        graph.publish(route, b"22")
        subscription.dispose()

        self.assertEqual(windows, [[b"20"], [b"20", b"21"], [b"21", b"22"]])

    def test_window_rejects_non_positive_size(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Temperature"),
        )
        graph = graph_module.Graph()

        with self.assertRaisesRegex(ValueError, "window size must be positive"):
            graph.window(route, size=0)

    def test_window_isolates_buffer_state_per_subscription(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Temperature"),
        )
        graph = graph_module.Graph()
        graph.publish(route, 20)

        first_windows = []
        first = graph.window(route, size=2).subscribe(
            lambda items: first_windows.append(items)
        )
        graph.publish(route, 21)
        first.dispose()

        second_windows = []
        second = graph.window(route, size=2).subscribe(
            lambda items: second_windows.append(items)
        )
        graph.publish(route, 22)
        second.dispose()

        self.assertEqual(first_windows, [[20], [20, 21]])
        self.assertEqual(second_windows, [[21], [21, 22]])

    def test_window_aggregate_replays_latest_and_emits_rolling_sums(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Temperature"),
        )
        graph = graph_module.Graph()
        graph.publish(route, 20)

        aggregates = []
        subscription = graph.window_aggregate(
            route,
            size=2,
            aggregate=sum,
        ).subscribe(aggregates.append)
        graph.publish(route, 21)
        graph.publish(route, 22)
        subscription.dispose()

        self.assertEqual(aggregates, [20, 41, 43])

    def test_window_aggregate_partitions_by_key(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("axes"),
            variant=graph_module.Variant.Meta,
            schema=str_schema(graph_module, "AxisReading"),
        )
        graph = graph_module.Graph()

        aggregates = []
        subscription = graph.window_aggregate(
            route,
            size=2,
            aggregate=lambda items: "|".join(items),
            partition_by=lambda value: value.split(":")[0],
        ).subscribe(aggregates.append)
        graph.publish(route, "left:20")
        graph.publish(route, "right:40")
        graph.publish(route, "left:21")
        subscription.dispose()

        self.assertEqual(
            aggregates,
            ["left:20", "right:40", "left:20|left:21"],
        )

    def test_window_aggregate_accepts_raw_route_refs(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="TemperatureBytes"),
        )
        graph = graph_module.Graph()

        aggregates = []
        subscription = graph.window_aggregate(
            route.route_ref,
            size=2,
            aggregate=lambda items: b"|".join(items),
        ).subscribe(aggregates.append)
        graph.publish(route, b"20")
        graph.publish(route, b"21")
        subscription.dispose()

        self.assertEqual(aggregates, [b"20", b"20|21"])

    def test_window_aggregate_isolates_buffer_state_per_subscription(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Temperature"),
        )
        graph = graph_module.Graph()
        graph.publish(route, 20)

        first_aggregates = []
        first = graph.window_aggregate(route, size=2, aggregate=sum).subscribe(
            first_aggregates.append
        )
        graph.publish(route, 21)
        first.dispose()

        second_aggregates = []
        second = graph.window_aggregate(route, size=2, aggregate=sum).subscribe(
            second_aggregates.append
        )
        graph.publish(route, 22)
        second.dispose()

        self.assertEqual(first_aggregates, [20, 41])
        self.assertEqual(second_aggregates, [21, 43])

    def test_window_emits_buffer_only_when_explicit_trigger_advances(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Temperature"),
        )
        trigger = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("watermark"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="WatermarkTick"),
        )
        graph = graph_module.Graph()

        windows = []
        subscription = graph.window(route, size=2, trigger=trigger).subscribe(
            windows.append
        )
        graph.publish(route, 20)
        graph.publish(route, 21)
        self.assertEqual(windows, [])
        graph.publish(trigger, b"tick-1")
        graph.publish(route, 22)
        graph.publish(trigger, b"tick-2")
        subscription.dispose()

        self.assertEqual(windows, [[20, 21], [21, 22]])

    def test_window_disposes_source_when_trigger_subscription_fails(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("trigger_setup_temperature"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "TriggerSetupTemperature"),
        )
        trigger = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("trigger_setup_watermark"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="TriggerSetupWatermark"),
        )
        graph = graph_module.Graph()
        original_observe = graph.observe

        def observe_with_trigger_failure(route_ref, *args, **kwargs):
            if route_ref == trigger:
                return FailingObservable("trigger subscription failed")
            return original_observe(route_ref, *args, **kwargs)

        graph.observe = observe_with_trigger_failure  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "trigger subscription failed"):
            graph.window(route, size=2, trigger=trigger).subscribe(lambda _items: None)

        self.assertEqual(graph.subscribers(route), 0)

    def test_window_aggregate_supports_explicit_trigger_policy(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Temperature"),
        )
        trigger = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("watermark"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="WatermarkTick"),
        )
        graph = graph_module.Graph()
        graph.publish(route, 20)
        graph.publish(trigger, b"tick-0")

        aggregates = []
        subscription = graph.window_aggregate(
            route,
            size=2,
            aggregate=sum,
            trigger=trigger,
        ).subscribe(aggregates.append)
        graph.publish(route, 21)
        self.assertEqual(aggregates, [20])
        graph.publish(route, 22)
        graph.publish(trigger, b"tick-1")
        graph.publish(route, 23)
        graph.publish(trigger, b"tick-2")
        subscription.dispose()

        self.assertEqual(aggregates, [20, 43, 45])

    def test_window_by_time_uses_control_epoch_as_event_time(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Temperature"),
        )
        graph = graph_module.Graph()

        windows = []
        subscription = graph.window_by_time(route, width=3).subscribe(windows.append)
        graph.publish(route, 20, control_epoch=10)
        graph.publish(route, 21, control_epoch=11)
        graph.publish(route, 22, control_epoch=13)
        subscription.dispose()

        self.assertEqual(windows, [[20], [20, 21], [21, 22]])

    def test_window_by_time_partitions_event_time_buffers_by_key(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("axes"),
            variant=graph_module.Variant.Meta,
            schema=str_schema(graph_module, "AxisReading"),
        )
        watermark = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("watermark"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "WatermarkTick"),
        )
        graph = graph_module.Graph()

        windows = []
        subscription = graph.window_by_time(
            route,
            width=2,
            watermark=watermark,
            partition_by=lambda value: value.split(":")[0],
            event_time=lambda value: int(value.split(":")[1]),
            watermark_time=lambda value: value,
        ).subscribe(windows.append)
        graph.publish(route, "left:1")
        graph.publish(route, "right:1")
        graph.publish(route, "left:2")
        graph.publish(route, "right:3")
        graph.publish(watermark, 2)
        graph.publish(watermark, 3)
        subscription.dispose()

        self.assertEqual(
            windows,
            [
                ["left:1", "left:2"],
                ["right:1"],
                ["left:2"],
                ["right:3"],
            ],
        )

    def test_window_by_time_supports_watermark_and_grace_for_late_data(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Temperature"),
        )
        watermark = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("watermark"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="WatermarkTick"),
        )
        graph = graph_module.Graph()

        windows = []
        subscription = graph.window_by_time(
            route,
            width=3,
            watermark=watermark,
            grace=1,
        ).subscribe(windows.append)
        graph.publish(route, 20, control_epoch=10)
        graph.publish(route, 22, control_epoch=12)
        graph.publish(watermark, b"tick-12", control_epoch=12)
        graph.publish(route, 21, control_epoch=11)
        graph.publish(watermark, b"tick-13", control_epoch=13)
        graph.publish(route, 19, control_epoch=9)
        graph.publish(watermark, b"tick-14", control_epoch=14)
        subscription.dispose()

        self.assertEqual(windows, [[20, 22], [21, 22], [22]])

    def test_window_by_time_retains_watermark_before_source_data(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Temperature"),
        )
        watermark = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("watermark"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="WatermarkTick"),
        )
        graph = graph_module.Graph()

        windows = []
        subscription = graph.window_by_time(
            route,
            width=3,
            watermark=watermark,
        ).subscribe(windows.append)
        graph.publish(watermark, b"tick-12", control_epoch=12)
        graph.publish(route, 19, control_epoch=9)
        graph.publish(route, 22, control_epoch=12)
        graph.publish(watermark, b"tick-12", control_epoch=12)
        subscription.dispose()

        self.assertEqual(windows, [[22]])

    def test_window_by_time_rejects_invalid_width_and_grace(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Temperature"),
        )
        graph = graph_module.Graph()

        with self.assertRaisesRegex(ValueError, "window width must be positive"):
            graph.window_by_time(route, width=0)
        with self.assertRaisesRegex(
            ValueError, "window grace must be non-negative"
        ):
            graph.window_by_time(route, width=2, grace=-1)

    def test_window_aggregate_by_time_emits_watermark_aggregates(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("temperature"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Temperature"),
        )
        watermark = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("watermark"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="WatermarkTick"),
        )
        graph = graph_module.Graph()

        aggregates = []
        subscription = graph.window_aggregate_by_time(
            route,
            width=3,
            watermark=watermark,
            aggregate=sum,
        ).subscribe(aggregates.append)
        graph.publish(route, 20, control_epoch=10)
        graph.publish(route, 21, control_epoch=11)
        graph.publish(watermark, b"tick-11", control_epoch=11)
        graph.publish(route, 22, control_epoch=12)
        graph.publish(watermark, b"tick-12", control_epoch=12)
        subscription.dispose()

        self.assertEqual(aggregates, [41, 63])

    def test_window_aggregate_by_time_partitions_by_key(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("axes"),
            variant=graph_module.Variant.Meta,
            schema=str_schema(graph_module, "AxisReading"),
        )
        watermark = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("watermark"),
            variant=graph_module.Variant.Event,
            schema=int_schema(graph_module, "WatermarkTick"),
        )
        graph = graph_module.Graph()

        aggregates = []
        subscription = graph.window_aggregate_by_time(
            route,
            width=2,
            watermark=watermark,
            partition_by=lambda value: value.split(":")[0],
            event_time=lambda value: int(value.split(":")[1]),
            watermark_time=lambda value: value,
            aggregate=lambda items: "|".join(items),
        ).subscribe(aggregates.append)
        graph.publish(route, "left:1")
        graph.publish(route, "right:1")
        graph.publish(route, "left:2")
        graph.publish(route, "right:3")
        graph.publish(watermark, 2)
        graph.publish(watermark, 3)
        subscription.dispose()

        self.assertEqual(
            aggregates,
            ["left:1|left:2", "right:1", "left:2", "right:3"],
        )

    def test_join_latest_uses_current_latest_then_future_updates(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Gyro"),
        )
        graph = graph_module.Graph()
        graph.publish(left, 2)
        graph.publish(right, 10)

        joined = []
        subscription = graph.join_latest(
            left, right, combine=lambda a, b: a + b
        ).subscribe(joined.append)
        graph.publish(left, 3)
        graph.publish(right, 11)
        subscription.dispose()

        self.assertEqual(joined, [12, 13, 14])

    def test_join_latest_disposes_left_when_right_subscription_fails(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("left_setup_failure"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "LeftSetupFailure"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("right_setup_failure"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "RightSetupFailure"),
        )
        graph = graph_module.Graph()
        original_observe = graph.observe

        def observe_with_right_failure(route_ref, *args, **kwargs):
            if route_ref == right:
                return FailingObservable("right subscription failed")
            return original_observe(route_ref, *args, **kwargs)

        graph.observe = observe_with_right_failure  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "right subscription failed"):
            graph.join_latest(left, right, combine=lambda a, b: a + b).subscribe(
                lambda _value: None
            )

        self.assertEqual(graph.subscribers(left), 0)

    def test_join_latest_accepts_raw_route_refs_and_combines_bytes(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="AccelBytes"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="GyroBytes"),
        )
        graph = graph_module.Graph()
        graph.publish(left, b"2")
        graph.publish(right, b"10")

        joined = []
        subscription = graph.join_latest(
            left.route_ref,
            right.route_ref,
            combine=lambda a, b: int(a.decode("ascii")) + int(b.decode("ascii")),
        ).subscribe(joined.append)
        graph.publish(left, b"3")
        graph.publish(right, b"11")
        subscription.dispose()

        self.assertEqual(joined, [12, 13, 14])

    def test_join_latest_supports_mixed_typed_and_raw_sources(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="GyroBytes"),
        )
        graph = graph_module.Graph()
        graph.publish(left, 2)
        graph.publish(right, b"10")

        joined = []
        subscription = graph.join_latest(
            left,
            right.route_ref,
            combine=lambda a, b: a + int(b.decode("ascii")),
        ).subscribe(joined.append)
        graph.publish(left, 3)
        graph.publish(right, b"11")
        subscription.dispose()

        self.assertEqual(joined, [12, 13, 14])

    def test_join_latest_treats_none_as_present_latest_value(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("runtime"),
            family=graph_module.StreamFamily("optional"),
            stream=graph_module.StreamName("left"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.any("OptionalLeft"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("runtime"),
            family=graph_module.StreamFamily("optional"),
            stream=graph_module.StreamName("right"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.any("OptionalRight"),
        )
        graph = graph_module.Graph()
        graph.publish(left, None)
        graph.publish(right, "ready")

        joined = []
        subscription = graph.join_latest(
            left, right, combine=lambda a, b: (a, b)
        ).subscribe(joined.append)
        graph.publish(right, "updated")
        subscription.dispose()

        self.assertEqual(joined, [(None, "ready"), (None, "updated")])

    def test_interval_join_emits_pairs_within_bounded_sequence_distance(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Gyro"),
        )
        graph = graph_module.Graph()

        joined = []
        subscription = graph.interval_join(
            left,
            right,
            within=1,
            combine=lambda accel, gyro: (accel, gyro),
        ).subscribe(joined.append)
        graph.publish(left, 100)
        graph.publish(right, 7)
        graph.publish(left, 101)
        graph.publish(right, 8)
        graph.publish(right, 9)
        subscription.dispose()

        self.assertEqual(joined, [(100, 7), (101, 7), (100, 8), (101, 8), (101, 9)])

    def test_interval_join_accepts_raw_route_refs_and_rejects_negative_distance(
        self,
    ) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="AccelBytes"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="GyroBytes"),
        )
        graph = graph_module.Graph()

        with self.assertRaisesRegex(
            ValueError, "interval join distance must be non-negative"
        ):
            graph.interval_join(
                left.route_ref, right.route_ref, within=-1, combine=lambda a, b: (a, b)
            )

        joined = []
        subscription = graph.interval_join(
            left.route_ref,
            right.route_ref,
            within=0,
            combine=lambda accel, gyro: int(accel.decode("ascii"))
            + int(gyro.decode("ascii")),
        ).subscribe(joined.append)
        graph.publish(left, b"2")
        graph.publish(right, b"10")
        graph.publish(left, b"3")
        graph.publish(right, b"11")
        subscription.dispose()

        self.assertEqual(joined, [12, 14])

    def test_interval_join_disposes_left_when_right_subscription_fails(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("interval_left_setup_failure"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "IntervalLeftSetupFailure"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("interval_right_setup_failure"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "IntervalRightSetupFailure"),
        )
        graph = graph_module.Graph()
        original_observe = graph.observe

        def observe_with_right_failure(route_ref, *args, **kwargs):
            if route_ref == right:
                return FailingObservable("right subscription failed")
            return original_observe(route_ref, *args, **kwargs)

        graph.observe = observe_with_right_failure  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "right subscription failed"):
            graph.interval_join(
                left,
                right,
                within=1,
                combine=lambda accel, gyro: (accel, gyro),
            ).subscribe(lambda _value: None)

        self.assertEqual(graph.subscribers(left), 0)

    def test_join_latest_isolates_latest_state_per_subscription(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Gyro"),
        )
        graph = graph_module.Graph()
        graph.publish(left, 2)
        graph.publish(right, 10)

        first_joined = []
        first = graph.join_latest(left, right, combine=lambda a, b: a + b).subscribe(
            first_joined.append
        )
        graph.publish(left, 3)
        first.dispose()

        second_joined = []
        second = graph.join_latest(left, right, combine=lambda a, b: a + b).subscribe(
            second_joined.append
        )
        graph.publish(right, 11)
        second.dispose()

        self.assertEqual(first_joined, [12, 13])
        self.assertEqual(second_joined, [13, 14])

    def test_join_latest_subscribes_before_replaying_latest_values(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("reentrant_accel"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "ReentrantAccel"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("reentrant_gyro"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "ReentrantGyro"),
        )
        graph = graph_module.Graph()
        graph.publish(left, 1)
        graph.publish(right, 10)
        joined: list[int] = []

        def receive(value: int) -> None:
            joined.append(value)
            if value == 11:
                graph.publish(left, 2)

        subscription = graph.join_latest(
            left,
            right,
            combine=lambda accel, gyro: accel + gyro,
        ).subscribe(receive)
        subscription.dispose()

        self.assertEqual(joined, [11, 12])

    def test_lookup_join_emits_only_when_left_side_arrives(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        right_state = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("catalog"),
            family=graph_module.StreamFamily("device"),
            stream=graph_module.StreamName("calibration"),
            variant=graph_module.Variant.State,
            schema=int_schema(graph_module, "Calibration"),
        )
        graph = graph_module.Graph()

        joined = []
        subscription = graph.lookup_join(
            left,
            right_state,
            combine=lambda accel, calibration: accel + calibration,
        ).subscribe(joined.append)

        graph.publish(left, 1)
        graph.publish(right_state, 10)
        graph.publish(left, 2)
        graph.publish(right_state, 11)
        graph.publish(left, 3)
        subscription.dispose()

        self.assertEqual(joined, [12, 14])

    def test_lookup_join_supports_raw_route_refs(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="AccelBytes"),
        )
        right_state = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("catalog"),
            family=graph_module.StreamFamily("device"),
            stream=graph_module.StreamName("calibration"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.bytes(name="CalibrationBytes"),
        )
        graph = graph_module.Graph()

        joined = []
        subscription = graph.lookup_join(
            left.route_ref,
            right_state.route_ref,
            combine=lambda accel, calibration: int(accel.decode("ascii"))
            + int(calibration.decode("ascii")),
        ).subscribe(joined.append)

        graph.publish(right_state, b"10")
        graph.publish(left, b"2")
        graph.publish(right_state, b"11")
        graph.publish(left, b"3")
        subscription.dispose()

        self.assertEqual(joined, [12, 14])

    def test_lookup_join_treats_none_as_present_state_value(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("runtime"),
            family=graph_module.StreamFamily("optional"),
            stream=graph_module.StreamName("event"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.any("OptionalEvent"),
        )
        right_state = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("runtime"),
            family=graph_module.StreamFamily("optional"),
            stream=graph_module.StreamName("state"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.any("OptionalState"),
        )
        graph = graph_module.Graph()

        joined = []
        subscription = graph.lookup_join(
            left, right_state, combine=lambda event, state: (event, state)
        ).subscribe(joined.append)
        graph.publish(left, "ignored")
        graph.publish(right_state, None)
        graph.publish(left, "joined")
        subscription.dispose()

        self.assertEqual(joined, [("joined", None)])

    def test_lookup_join_replays_latest_state_per_subscription(self) -> None:
        graph_module = load_graph_module()

        left = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        right_state = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("catalog"),
            family=graph_module.StreamFamily("device"),
            stream=graph_module.StreamName("calibration"),
            variant=graph_module.Variant.State,
            schema=int_schema(graph_module, "Calibration"),
        )
        graph = graph_module.Graph()
        graph.publish(right_state, 10)

        first_joined = []
        first = graph.lookup_join(
            left, right_state, combine=lambda accel, calibration: accel + calibration
        ).subscribe(first_joined.append)
        graph.publish(left, 2)
        first.dispose()

        graph.publish(right_state, 11)

        second_joined = []
        second = graph.lookup_join(
            left, right_state, combine=lambda accel, calibration: accel + calibration
        ).subscribe(second_joined.append)
        graph.publish(left, 3)
        second.dispose()

        self.assertEqual(first_joined, [12])
        self.assertEqual(second_joined, [14])

    def test_capacitor_coalesces_fast_source_updates_until_demand_arrives(
        self,
    ) -> None:
        graph_module = load_graph_module()

        source_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        sampled = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel_sampled"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        demand = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("drain"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="DrainTick"),
        )
        graph = graph_module.Graph()
        graph.capacitor(
            source=graph_module.source(source_route),
            sink=graph_module.sink(sampled),
            demand=demand,
        )

        emitted = []
        subscription = graph.observe(sampled, replay_latest=False).subscribe(
            lambda envelope: emitted.append(envelope.value)
        )

        graph.publish(source_route, 1)
        graph.publish(source_route, 2)
        graph.publish(demand, b"tick-1")
        graph.publish(source_route, 3)
        graph.publish(source_route, 4)
        graph.publish(demand, b"tick-2")
        subscription.dispose()

        self.assertEqual(emitted, [2, 4])

    def test_capacitor_discharges_latest_value_on_demand(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        sampled = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel_sampled"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        demand = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("drain"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="DrainTick"),
        )
        graph = graph_module.Graph()

        capacitor = graph.capacitor(
            source=source,
            sink=sampled,
            capacity=1,
            demand=demand,
        )
        emitted = []
        sub = graph.observe(sampled, replay_latest=False).subscribe(emitted.append)

        graph.publish(source, 1, control_epoch=10)
        graph.publish(source, 2, control_epoch=20)
        graph.publish(demand, b"tick", control_epoch=21)
        sub.dispose()

        self.assertEqual(capacitor.capacity, 1)
        self.assertEqual([item.value for item in emitted], [2])
        self.assertEqual([item.closed.control_epoch for item in emitted], [20])

    def test_capacitor_disposes_source_when_demand_subscription_fails(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("capacitor_source_setup_failure"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "CapacitorSourceSetupFailure"),
        )
        sampled = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("capacitor_sampled_setup_failure"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "CapacitorSampledSetupFailure"),
        )
        demand = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("capacitor_demand_setup_failure"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="CapacitorDemandSetupFailure"),
        )
        graph = graph_module.Graph()
        original_observe = graph.observe

        def observe_with_demand_failure(route_ref, *args, **kwargs):
            if route_ref == demand:
                return FailingObservable("demand subscription failed")
            return original_observe(route_ref, *args, **kwargs)

        graph.observe = observe_with_demand_failure  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "demand subscription failed"):
            graph.capacitor(source=source, sink=sampled, demand=demand)

        self.assertEqual(graph.subscribers(source), 0)

    def test_source_and_sink_wrappers_work_across_core_graph_calls(self) -> None:
        graph_module = load_graph_module()

        source_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("flow"),
            stream=graph_module.StreamName("source"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "SourceValue"),
        )
        sink_route = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("processor"),
            family=graph_module.StreamFamily("flow"),
            stream=graph_module.StreamName("sink"),
            variant=graph_module.Variant.State,
            schema=int_schema(graph_module, "SinkValue"),
        )
        source = graph_module.source(source_route)
        sink = graph_module.sink(sink_route)
        graph = graph_module.Graph()

        graph.connect(source=source, sink=sink)
        graph.publish(source, 3)
        observed = []
        sub = graph.observe(source).subscribe(
            lambda envelope: observed.append(envelope.value)
        )
        graph.publish(source, 5)
        sub.dispose()

        latest = graph.latest(source)
        edge = graph.describe_edge(source=source, sink=sink)

        self.assertEqual(observed, [3, 5])
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value, 5)
        self.assertEqual(edge.credit_class, "default")

    def test_stub_graph_orders_catalog_topology_and_credit_snapshots(self) -> None:
        graph_module = load_graph_module()
        z_source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("zeta"),
            family=graph_module.StreamFamily("flow"),
            stream=graph_module.StreamName("source"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "ZetaValue"),
        )
        z_sink = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("zeta"),
            family=graph_module.StreamFamily("flow"),
            stream=graph_module.StreamName("sink"),
            variant=graph_module.Variant.State,
            schema=int_schema(graph_module, "ZetaValue"),
        )
        a_source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("alpha"),
            family=graph_module.StreamFamily("flow"),
            stream=graph_module.StreamName("source"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "AlphaValue"),
        )
        a_sink = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("alpha"),
            family=graph_module.StreamFamily("flow"),
            stream=graph_module.StreamName("sink"),
            variant=graph_module.Variant.State,
            schema=int_schema(graph_module, "AlphaValue"),
        )
        graph = graph_module.Graph()

        graph.connect(source=z_source, sink=z_sink)
        graph.connect(source=a_source, sink=a_sink)

        catalog_displays = [route.display() for route in graph.catalog()]
        expected_route_displays = tuple(
            route.display() for route in (z_source, z_sink, a_source, a_sink)
        )
        self.assertEqual(catalog_displays, sorted(catalog_displays))
        for route_display in expected_route_displays:
            self.assertIn(route_display, catalog_displays)
        self.assertEqual(
            list(graph.topology()),
            [
                (a_source.display(), a_sink.display()),
                (z_source.display(), z_sink.display()),
            ],
        )
        credit_displays = [
            snapshot.route_display for snapshot in graph.credit_snapshot()
        ]
        self.assertEqual(credit_displays, sorted(credit_displays))
        for route_display in expected_route_displays:
            self.assertIn(route_display, credit_displays)

    def test_flow_wiring_requires_keyword_source_and_sink(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("flow"),
            stream=graph_module.StreamName("source"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "SourceValue"),
        )
        sink = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("processor"),
            family=graph_module.StreamFamily("flow"),
            stream=graph_module.StreamName("sink"),
            variant=graph_module.Variant.State,
            schema=int_schema(graph_module, "SinkValue"),
        )
        graph = graph_module.Graph()

        with self.assertRaises(TypeError):
            graph.connect(source, sink)
        with self.assertRaises(TypeError):
            graph.describe_edge(source, sink)
        with self.assertRaises(TypeError):
            graph.capacitor(source, sink)
        with self.assertRaises(TypeError):
            graph.resistor(source, sink)

    def test_diagram_renders_topology_as_mermaid(self) -> None:
        graph_module = load_graph_module()

        source_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("heart.switch"),
            family=graph_module.StreamFamily("peripheral"),
            stream=graph_module.StreamName("serial"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="SwitchSerial"),
        )
        sink_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heart.switch"),
            family=graph_module.StreamFamily("peripheral"),
            stream=graph_module.StreamName("state"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.bytes(name="SwitchState"),
        )
        graph = graph_module.Graph()
        graph.connect(source=source_route, sink=sink_route)

        diagram = graph.diagram(group_by=("layer", "owner"))

        self.assertIn("flowchart LR", diagram)
        self.assertIn('["raw / heart.switch"]', diagram)
        self.assertIn('["logical / heart.switch"]', diagram)
        self.assertIn('["peripheral.serial<br/>meta"]', diagram)
        self.assertIn('["peripheral.state<br/>state"]', diagram)
        self.assertIn("-->", diagram)

    def test_empty_diagram_is_still_renderable(self) -> None:
        graph_module = load_graph_module()
        graph = graph_module.Graph()

        self.assertEqual(
            graph.render_diagram(),
            "flowchart LR\n  %% graph has no topology edges",
        )

    def test_diagram_renders_registered_node_without_edges(self) -> None:
        graph_module = load_graph_module()
        graph = graph_module.Graph()

        graph.register_diagram_node("planner", group="control")

        diagram = graph.render_diagram()

        self.assertIn("flowchart LR", diagram)
        self.assertIn('subgraph g0["control"]', diagram)
        self.assertIn('["planner"]', diagram)
        self.assertNotIn("graph has no topology edges", diagram)

    def test_registered_diagram_node_metadata_is_key_sorted(self) -> None:
        graph_module = load_graph_module()
        graph = graph_module.Graph()

        node = graph.register_diagram_node(
            "planner",
            metadata={"zeta": "last", "alpha": "first"},
        )

        self.assertEqual(
            node.metadata,
            (("alpha", "first"), ("zeta", "last")),
        )

    def test_context_collects_scoped_route_inputs_and_outputs(self) -> None:
        graph_module = load_graph_module()
        command = graph_module.route(
            plane=graph_module.Plane.Write,
            owner="board",
            family="control",
            stream="command",
            variant=graph_module.Variant.Request,
            schema=str_schema(graph_module, "BoardCommand"),
        )
        reading = graph_module.route(
            owner="board",
            family="sensor",
            stream="reading",
            schema=int_schema(graph_module, "BoardReading"),
        )
        outside = graph_module.route(
            owner="board",
            family="sensor",
            stream="outside",
            schema=int_schema(graph_module, "OutsideReading"),
        )
        graph = graph_module.Graph()

        with graph.context(name="board") as board:
            graph.observe(command, replay_latest=False)
            graph.publish(reading, 7)

        graph.publish(outside, 9)

        node = board.node
        metadata = dict(node.metadata)
        self.assertEqual(node.name, "board")
        self.assertEqual(node.input_routes, (command.display(),))
        self.assertEqual(node.output_routes, (reading.display(),))
        self.assertEqual(metadata["context"], "true")
        self.assertEqual(metadata["context_path"], "board")

    def test_nested_context_links_child_part_to_parent_context(self) -> None:
        graph_module = load_graph_module()
        raw_accel = graph_module.route(
            owner="imu",
            family="accelerometer",
            stream="raw",
            schema=int_schema(graph_module, "RawAccel"),
        )
        filtered_accel = graph_module.route(
            owner="imu",
            family="accelerometer",
            stream="filtered",
            schema=int_schema(graph_module, "FilteredAccel"),
        )
        graph = graph_module.Graph()

        with graph.context(name="board"):
            with graph.context(name="accelerometer") as accelerometer:
                graph.connect(source=raw_accel, sink=filtered_accel)

        nodes = {node.name: node for node in graph.diagram_nodes()}
        accelerometer_metadata = dict(accelerometer.node.metadata)

        self.assertEqual(nodes["board"].input_routes, (raw_accel.display(),))
        self.assertEqual(nodes["board"].output_routes, (filtered_accel.display(),))
        self.assertEqual(nodes["accelerometer"].input_routes, (raw_accel.display(),))
        self.assertEqual(
            nodes["accelerometer"].output_routes,
            (filtered_accel.display(),),
        )
        self.assertEqual(accelerometer_metadata["context_parent"], "board")
        self.assertEqual(
            accelerometer_metadata["context_path"],
            "board/accelerometer",
        )
        diagram = graph.render_diagram()
        self.assertIn('["board"]', diagram)
        self.assertIn('["accelerometer"]', diagram)

    def test_diagram_rejects_unknown_group_fields(self) -> None:
        graph_module = load_graph_module()
        graph = graph_module.Graph()

        with self.assertRaises(ValueError):
            graph.diagram(group_by=("rack",))

    def test_resistor_gates_values_with_gate_predicate(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("frame_meta"),
            variant=graph_module.Variant.Meta,
            schema=str_schema(graph_module, "FrameMeta"),
        )
        selected = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("selected_frame"),
            variant=graph_module.Variant.Meta,
            schema=str_schema(graph_module, "FrameMeta"),
        )
        graph = graph_module.Graph()

        graph.resistor(
            source=source,
            sink=selected,
            gate=lambda value: value.endswith(":open"),
        )
        emitted = []
        sub = graph.observe(selected, replay_latest=False).subscribe(emitted.append)

        graph.publish(source, "frame-1:drop")
        graph.publish(source, "frame-2:open")
        sub.dispose()

        self.assertEqual([item.value for item in emitted], ["frame-2:open"])

    def test_resistor_disposes_source_when_release_subscription_fails(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("resistor_source_setup_failure"),
            variant=graph_module.Variant.Meta,
            schema=str_schema(graph_module, "ResistorSourceSetupFailure"),
        )
        selected = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("resistor_selected_setup_failure"),
            variant=graph_module.Variant.Meta,
            schema=str_schema(graph_module, "ResistorSelectedSetupFailure"),
        )
        release = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("resistor_release_setup_failure"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="ResistorReleaseSetupFailure"),
        )
        graph = graph_module.Graph()
        original_observe = graph.observe

        def observe_with_release_failure(route_ref, *args, **kwargs):
            if route_ref == release:
                return FailingObservable("release subscription failed")
            return original_observe(route_ref, *args, **kwargs)

        graph.observe = observe_with_release_failure  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "release subscription failed"):
            graph.resistor(source=source, sink=selected, release=release)

        self.assertEqual(graph.subscribers(source), 0)

    def test_watchdog_emits_timeout_after_missed_ticks_and_resets(self) -> None:
        graph_module = load_graph_module()

        heartbeat = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("raft"),
            family=graph_module.StreamFamily("leader"),
            stream=graph_module.StreamName("heartbeat_seen"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.bytes(name="Heartbeat"),
        )
        clock = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("raft"),
            family=graph_module.StreamFamily("clock"),
            stream=graph_module.StreamName("election_tick"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="Tick"),
        )
        timeout = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("raft"),
            family=graph_module.StreamFamily("election"),
            stream=graph_module.StreamName("timeout"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="Timeout"),
        )
        graph = graph_module.Graph()

        graph.watchdog(
            reset_by=heartbeat,
            output=timeout,
            after=2,
            clock=clock,
        )
        emitted = []
        sub = graph.observe(timeout, replay_latest=False).subscribe(emitted.append)

        graph.publish(clock, b"tick-1", control_epoch=1)
        graph.publish(clock, b"tick-2", control_epoch=2)
        graph.publish(clock, b"tick-3", control_epoch=3)
        graph.publish(heartbeat, b"ok", control_epoch=3)
        graph.publish(clock, b"tick-4", control_epoch=4)
        graph.publish(clock, b"tick-5", control_epoch=5)
        sub.dispose()

        self.assertEqual([item.value for item in emitted], [b"timeout", b"timeout"])
        self.assertEqual([item.closed.control_epoch for item in emitted], [2, 5])

    def test_watchdog_disposes_reset_when_clock_subscription_fails(self) -> None:
        graph_module = load_graph_module()

        heartbeat = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("raft"),
            family=graph_module.StreamFamily("leader"),
            stream=graph_module.StreamName("watchdog_reset_setup_failure"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.bytes(name="WatchdogResetSetupFailure"),
        )
        clock = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("raft"),
            family=graph_module.StreamFamily("clock"),
            stream=graph_module.StreamName("watchdog_clock_setup_failure"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="WatchdogClockSetupFailure"),
        )
        timeout = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("raft"),
            family=graph_module.StreamFamily("election"),
            stream=graph_module.StreamName("watchdog_timeout_setup_failure"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="WatchdogTimeoutSetupFailure"),
        )
        graph = graph_module.Graph()
        original_observe = graph.observe

        def observe_with_clock_failure(route_ref, *args, **kwargs):
            if route_ref == clock:
                return FailingObservable("clock subscription failed")
            return original_observe(route_ref, *args, **kwargs)

        graph.observe = observe_with_clock_failure  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "clock subscription failed"):
            graph.watchdog(
                reset_by=heartbeat,
                output=timeout,
                after=2,
                clock=clock,
            )

        self.assertEqual(graph.subscribers(heartbeat), 0)

    def test_capacitor_demand_supports_raw_route_refs(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="AccelBytes"),
        )
        sampled = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel_sampled"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="AccelBytes"),
        )
        demand = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("drain"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="DrainTick"),
        )
        graph = graph_module.Graph()
        graph.capacitor(
            source=source.route_ref,
            sink=sampled.route_ref,
            demand=demand.route_ref,
        )

        emitted = []
        subscription = graph.observe(sampled.route_ref, replay_latest=False).subscribe(
            emitted.append
        )

        graph.publish(source, b"one")
        graph.publish(source, b"two")
        graph.publish(demand, b"tick")
        subscription.dispose()

        self.assertEqual(
            [item.payload_ref.inline_bytes for item in emitted],
            [b"two"],
        )

    def test_capacitor_demand_replays_latest_source_for_new_sink(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        first_sampled = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("first_sampled"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        second_sampled = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("second_sampled"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "Accel"),
        )
        demand = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("scheduler"),
            family=graph_module.StreamFamily("tick"),
            stream=graph_module.StreamName("drain"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="DrainTick"),
        )
        graph = graph_module.Graph()

        first_emitted = []
        graph.capacitor(source=source, sink=first_sampled, demand=demand)
        first = graph.observe(first_sampled, replay_latest=False).subscribe(
            lambda envelope: first_emitted.append(envelope.value)
        )
        graph.publish(source, 1)
        graph.publish(demand, b"tick-1")
        first.dispose()

        second_emitted = []
        graph.capacitor(source=source, sink=second_sampled, demand=demand)
        second = graph.observe(second_sampled, replay_latest=False).subscribe(
            lambda envelope: second_emitted.append(envelope.value)
        )
        graph.publish(source, 2)
        graph.publish(demand, b"tick-2")
        second.dispose()

        self.assertEqual(first_emitted, [1])
        self.assertEqual(second_emitted, [2])

    def test_materialize_decodes_raw_source_into_typed_state_route(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("counter"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("raw"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="CounterBytes"),
        )
        state_route = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("counter"),
            family=graph_module.StreamFamily("state"),
            stream=graph_module.StreamName("latest"),
            variant=graph_module.Variant.State,
            schema=int_schema(graph_module, "CounterValue"),
        )
        graph = graph_module.Graph()
        observed = []
        graph.observe(state_route).subscribe(
            lambda envelope: observed.append(envelope.value)
        )

        subscription = graph.materialize(source.route_ref, state_route=state_route)
        graph.publish(source, b"7")
        graph.publish(source, b"8")
        subscription.dispose()

        self.assertEqual(observed, [7, 8])
        latest = graph.latest(state_route)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value, 8)

    def test_materialize_encodes_typed_source_into_raw_state_route(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("counter"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("typed"),
            variant=graph_module.Variant.Meta,
            schema=int_schema(graph_module, "CounterValue"),
        )
        state_route = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Raw,
            owner=graph_module.OwnerName("counter"),
            family=graph_module.StreamFamily("state"),
            stream=graph_module.StreamName("latest_bytes"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.bytes(name="CounterBytes"),
        )
        graph = graph_module.Graph()
        observed = []
        graph.observe(state_route).subscribe(
            lambda envelope: observed.append(envelope.value)
        )

        subscription = graph.materialize(source, state_route=state_route.route_ref)
        graph.publish(source, 7)
        graph.publish(source, 8)
        subscription.dispose()

        self.assertEqual(observed, [b"7", b"8"])
        latest = graph.latest(state_route)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value, b"8")

    def test_materialize_preserves_source_taints_on_state_route(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Ephemeral,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("entropy"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="EntropyBytes"),
        )
        state_route = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("latest_entropy"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.bytes(name="EntropyState"),
        )
        graph = graph_module.Graph()

        subscription = graph.materialize(source, state_route=state_route)
        graph.publish(source, b"nonce-1")
        subscription.dispose()

        latest = graph.latest(state_route)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(
            tuple(
                taint.value_id
                for taint in latest.closed.taints
                if getattr(taint.domain, "as_str", lambda: taint.domain)()
                == "determinism"
            ),
            ("DET_NONREPLAYABLE",),
        )

    def test_materialize_deduplicates_matching_source_and_state_route_taints(
        self,
    ) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Ephemeral,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("entropy"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="EntropyBytes"),
        )
        state_route = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Ephemeral,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("latest_entropy"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.bytes(name="EntropyState"),
        )
        graph = graph_module.Graph()

        subscription = graph.materialize(source, state_route=state_route)
        graph.publish(source, b"nonce-1")
        subscription.dispose()

        latest = graph.latest(state_route)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(
            tuple(
                taint.value_id
                for taint in latest.closed.taints
                if getattr(taint.domain, "as_str", lambda: taint.domain)()
                == "determinism"
            ),
            ("DET_NONREPLAYABLE",),
        )

    def test_repair_taints_clears_time_unknown_with_explicit_proof(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("raw"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="ImuBytes"),
        )
        repaired = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("time_repaired"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="ImuBytes"),
        )
        graph = graph_module.Graph()

        subscription = graph.repair_taints(
            source,
            output=repaired,
            repair=graph_module.TaintRepair(
                domain=graph_module.TaintDomain.Time,
                cleared=("TIME_UNKNOWN",),
                added=(
                    graph_module.TaintMark(
                        graph_module.TaintDomain.Time,
                        "TIME_REPAIRED",
                        "clock_repair:model=v1",
                    ),
                ),
                proof="clock repair model v1 aligned the device clock",
            ),
        )
        envelope = graph.publish(source, b"frame-1")
        envelope.closed.taints.append(
            graph_module.TaintMark(
                graph_module.TaintDomain.Time,
                "TIME_UNKNOWN",
                "device_clock_missing",
            )
        )
        subscription.dispose()

        latest = graph.latest(repaired)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(
            tuple(
                (
                    getattr(taint.domain, "as_str", lambda: taint.domain)(),
                    taint.value_id,
                )
                for taint in latest.closed.taints
            ),
            (("time", "TIME_REPAIRED"),),
        )
        self.assertTrue(
            any(
                event.event_type == "repair"
                and "clock repair model v1 aligned the device clock" in event.detail
                for event in graph.audit(repaired)
            )
        )

    def test_repair_taints_rejects_absorbing_determinism_clear(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Ephemeral,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("entropy"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="EntropyBytes"),
        )
        repaired = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("entropy_repaired"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="EntropyBytes"),
        )
        graph = graph_module.Graph()

        subscription = graph.repair_taints(
            source,
            output=repaired,
            repair=graph_module.TaintRepair(
                domain=graph_module.TaintDomain.Determinism,
                cleared=("DET_NONREPLAYABLE",),
                proof="impossible proof",
            ),
        )
        with self.assertRaisesRegex(
            ValueError, "cannot clear absorbing taint DET_NONREPLAYABLE"
        ):
            graph.publish(source, b"nonce-1")
        subscription.dispose()

    def test_taint_repair_requires_proof_when_clearing_marks(self) -> None:
        graph_module = load_graph_module()

        with self.assertRaisesRegex(
            ValueError, "taint repairs that clear marks require a proof"
        ):
            graph_module.TaintRepair(
                domain=graph_module.TaintDomain.Time,
                cleared=("TIME_UNKNOWN",),
            )

    def test_query_taints_reports_stream_bounds_and_event_marks(self) -> None:
        graph_module = load_graph_module()

        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Ephemeral,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("entropy"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="EntropyBytes"),
        )
        graph = graph_module.Graph()

        graph.publish(route, b"nonce-1")
        response = graph.query(
            graph_module.QueryRequest(command="taints", route=route),
        )

        self.assertEqual(
            response.items,
            (
                "stream:determinism:DET_NONREPLAYABLE:read.ephemeral.session.trace.entropy.event.v1",
                "event:1:determinism:DET_NONREPLAYABLE:read.ephemeral.session.trace.entropy.event.v1",
            ),
        )

    def test_query_taints_includes_repair_notes(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("raw"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="ImuBytes"),
        )
        repaired = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("time_repaired"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="ImuBytes"),
        )
        graph = graph_module.Graph()

        subscription = graph.repair_taints(
            source,
            output=repaired,
            repair=graph_module.TaintRepair(
                domain=graph_module.TaintDomain.Time,
                cleared=("TIME_UNKNOWN",),
                added=(
                    graph_module.TaintMark(
                        graph_module.TaintDomain.Time,
                        "TIME_REPAIRED",
                        "clock_repair:model=v1",
                    ),
                ),
                proof="clock repair model v1 aligned the device clock",
            ),
        )
        envelope = graph.publish(source, b"frame-1")
        envelope.closed.taints.append(
            graph_module.TaintMark(
                graph_module.TaintDomain.Time,
                "TIME_UNKNOWN",
                "device_clock_missing",
            )
        )
        response = graph.query(
            graph_module.QueryRequest(command="taints", route=repaired),
        )
        subscription.dispose()

        self.assertEqual(
            response.items,
            (
                "stream:time:TIME_REPAIRED:clock_repair:model=v1",
                "event:1:time:TIME_REPAIRED:clock_repair:model=v1",
                "repair:time:clock repair model v1 aligned the device clock",
            ),
        )

    def test_query_replay_subscribers_and_writers_report_live_route_state(self) -> None:
        graph_module = load_graph_module()

        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Accel"),
        )
        graph = graph_module.Graph()
        graph.publish(
            route,
            b"first",
            producer=graph_module.ProducerRef("sensor-a", "device"),
        )
        graph.publish(route, b"second")
        graph.export_route(route)
        graph.grant_access(
            graph_module.CapabilityGrant(
                principal_id="dashboard",
                route=route,
                metadata_read=True,
                replay_read=True,
            )
        )

        subscription = graph.observe(route, replay_latest=False).subscribe(lambda _: None)

        replay = graph.query(
            graph_module.QueryRequest(command="replay", route=route),
            requester_id="dashboard",
        )
        subscribers = graph.query(
            graph_module.QueryRequest(command="subscribers", route=route),
            requester_id="dashboard",
        )
        writers = graph.query(
            graph_module.QueryRequest(command="writers", route=route),
            requester_id="dashboard",
        )
        subscription.dispose()

        self.assertEqual(replay.items, ("1", "2"))
        self.assertEqual(subscribers.items, ("1",))
        self.assertEqual(writers.items, ("python", "sensor-a"))

    def test_materialize_preserves_lineage_causality_and_parent_event(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=str_schema(graph_module, "Accel"),
        )
        state_route = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("topology"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel_latest"),
            variant=graph_module.Variant.State,
            schema=str_schema(graph_module, "AccelState"),
        )
        graph = graph_module.Graph()

        subscription = graph.materialize(source, state_route=state_route)
        source_event = graph.publish(
            source,
            "frame-1",
            trace_id="trace-7",
            causality_id="boot-chain",
            correlation_id="request-7",
        )
        subscription.dispose()

        records = tuple(graph.lineage(state_route))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].trace_id, "trace-7")
        self.assertEqual(records[0].causality_id, "boot-chain")
        self.assertEqual(records[0].correlation_id, "request-7")
        self.assertEqual(
            tuple(parent.display() for parent in records[0].parent_events),
            (f"{source.display()}@{source_event.closed.seq_source}",),
        )

    def test_query_trace_filters_by_trace_id(self) -> None:
        graph_module = load_graph_module()

        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro"),
            variant=graph_module.Variant.Meta,
            schema=str_schema(graph_module, "Gyro"),
        )
        state_route = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("topology"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro_latest"),
            variant=graph_module.Variant.State,
            schema=str_schema(graph_module, "GyroState"),
        )
        graph = graph_module.Graph()
        graph.grant_access(
            graph_module.CapabilityGrant(
                principal_id="auditor",
                route=source,
                debug_read=True,
                graph_validation=True,
            )
        )

        subscription = graph.materialize(source, state_route=state_route)
        graph.publish(
            source,
            "frame-1",
            trace_id="trace-gyro-9",
            causality_id="gyro-chain",
            correlation_id="request-9",
        )
        subscription.dispose()

        response = graph.query(
            graph_module.QueryRequest(
                command="trace",
                lineage_trace_id="trace-gyro-9",
            ),
            requester_id="auditor",
        )

        self.assertEqual(len(response.items), 2)
        self.assertTrue(
            any(
                item.startswith(
                    f"{source.display()}@1|trace=trace-gyro-9|causality=gyro-chain"
                )
                for item in response.items
            )
        )
        self.assertTrue(
            any(
                f"|parents={source.display()}@1" in item for item in response.items
            )
        )

    def test_lineage_filters_by_correlation_id(self) -> None:
        graph_module = load_graph_module()

        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("mag"),
            variant=graph_module.Variant.Meta,
            schema=str_schema(graph_module, "Mag"),
        )
        graph = graph_module.Graph()

        graph.publish(route, "frame-1", correlation_id="request-11")
        graph.publish(route, "frame-2", correlation_id="request-12")

        records = tuple(graph.lineage(correlation_id="request-11"))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].correlation_id, "request-11")

    def test_configure_retention_latest_only_limits_replay_and_descriptor(self) -> None:
        graph_module = load_graph_module()

        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Accel"),
        )
        graph = graph_module.Graph()
        graph.configure_retention(
            route,
            graph_module.RouteRetentionPolicy(
                latest_replay_policy="latest_only",
                replay_window="latest",
                history_limit=1,
            ),
        )

        graph.publish(route, b"first")
        graph.publish(route, b"second")

        descriptor = graph.describe_route(route)
        replay = tuple(envelope.seq_source for envelope in graph.replay(route))

        self.assertEqual(descriptor.retention.latest_replay_policy, "latest_only")
        self.assertEqual(descriptor.retention.replay_window, "latest")
        self.assertEqual(replay, (2,))

    def test_ephemeral_routes_default_to_non_replayable_retention(self) -> None:
        graph_module = load_graph_module()

        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Ephemeral,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("entropy"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="EntropyBytes"),
        )
        graph = graph_module.Graph()

        graph.publish(route, b"nonce-1")
        graph.publish(route, b"nonce-2")

        descriptor = graph.describe_route(route)
        replay = tuple(graph.replay(route))

        self.assertEqual(descriptor.retention.latest_replay_policy, "none")
        self.assertEqual(descriptor.retention.replay_window, "none")
        self.assertEqual(replay, ())

    def test_configure_retention_bounded_history_trims_stored_replay(self) -> None:
        graph_module = load_graph_module()

        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Gyro"),
        )
        graph = graph_module.Graph()
        graph.configure_retention(
            route,
            graph_module.RouteRetentionPolicy(
                latest_replay_policy="bounded_history",
                replay_window="last_2",
                history_limit=2,
            ),
        )

        graph.publish(route, b"one")
        graph.publish(route, b"two")
        graph.publish(route, b"three")

        replay = tuple(envelope.seq_source for envelope in graph.replay(route))
        descriptor = graph.describe_route(route)

        self.assertEqual(replay, (2, 3))
        self.assertEqual(descriptor.retention.latest_replay_policy, "bounded_history")
        self.assertEqual(descriptor.retention.replay_window, "last_2")

    def test_route_retention_policy_rejects_invalid_replay_policy_and_history(
        self,
    ) -> None:
        graph_module = load_graph_module()

        with self.assertRaisesRegex(
            ValueError, "latest_replay_policy must be one of"
        ):
            graph_module.RouteRetentionPolicy(latest_replay_policy="forever")
        with self.assertRaisesRegex(
            ValueError, "history_limit must be positive when provided"
        ):
            graph_module.RouteRetentionPolicy(
                latest_replay_policy="latest_only",
                history_limit=0,
            )
        with self.assertRaisesRegex(
            ValueError, "payload_retention_policy must be one of"
        ):
            graph_module.RouteRetentionPolicy(
                latest_replay_policy="latest_only",
                payload_retention_policy="forever_cache",
            )

    def test_query_plane_streams_and_capabilities(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Accel"),
        )
        graph = graph_module.Graph()
        graph.publish(route, b"sample")
        graph.export_route(route)
        graph.grant_access(
            graph_module.CapabilityGrant(
                principal_id="dashboard",
                route=route,
                metadata_read=True,
                replay_read=True,
                debug_read=True,
            )
        )

        response = graph.query(
            graph_module.QueryRequest(command="latest", route=route),
            requester_id="dashboard",
        )

        self.assertEqual(response.command, "latest")
        self.assertTrue(response.items)
        service = graph.query_service()
        self.assertIsNotNone(graph.latest(service.request))
        self.assertIsNotNone(graph.latest(service.response))
        debug_routes = list(graph.debug_routes())
        self.assertTrue(debug_routes)
        with self.assertRaises(PermissionError):
            graph.query(
                graph_module.QueryRequest(command="open_payload", route=route),
                requester_id="dashboard",
            )

    def test_exported_route_allows_metadata_queries_without_explicit_grant(
        self,
    ) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Accel"),
        )
        graph = graph_module.Graph()
        graph.publish(route, b"sample")
        graph.export_route(route)

        latest = graph.query(
            graph_module.QueryRequest(command="latest", route=route),
            requester_id="observer",
        )
        described = graph.query(
            graph_module.QueryRequest(command="describe_route", route=route),
            requester_id="observer",
        )

        self.assertEqual(latest.items[0], route.display())
        self.assertEqual(described.items[0], route.display())
        with self.assertRaises(PermissionError):
            graph.query(
                graph_module.QueryRequest(command="replay", route=route),
                requester_id="observer",
            )

    def test_register_middleware_link_and_mesh_primitive(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Accel"),
        )
        sink = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("dashboard"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel_copy"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="Accel"),
        )
        graph = graph_module.Graph()

        middleware = graph.add_middleware(
            graph_module.Middleware(
                name="validate_accel",
                kind="validation",
                attachment_scope="route",
                target=source.display(),
            )
        )
        link = graph.register_link(
            graph_module.Link(
                name="tcp0",
                link_class="TcpStreamLink",
                capabilities=graph_module.LinkCapabilities(
                    ordered=True,
                    reliable=True,
                    authenticated=True,
                ),
            )
        )
        primitive = graph.add_mesh_primitive(
            graph_module.MeshPrimitive(
                name="bridge_to_dashboard",
                kind="bridge",
                sources=(source,),
                destinations=(sink,),
                link_name=link.name,
                ordering_policy="source-priority",
            )
        )

        self.assertEqual(middleware.kind, "validation")
        self.assertEqual(list(graph.links())[0].name, "tcp0")
        self.assertEqual(primitive.kind, "bridge")
        self.assertEqual(len(list(graph.middleware())), 1)
        self.assertEqual(len(list(graph.mesh_primitives())), 1)

    def test_publish_lazy_defers_payload_open_until_demand(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Bulk,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("frame"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="LidarFrame"),
        )
        graph = graph_module.Graph()
        opens = 0

        def open_payload():
            nonlocal opens
            opens += 1
            return b"point-cloud"

        graph.publish_lazy(
            route,
            graph_module.LazyPayloadSource(open=open_payload, logical_length_bytes=11),
        )

        closed = graph.latest(route.route_ref)
        self.assertIsNotNone(closed)
        self.assertEqual(opens, 0)
        initial_snapshot = graph.payload_demand_snapshot(route)
        self.assertEqual(initial_snapshot.metadata_events, 1)
        self.assertEqual(initial_snapshot.payload_open_requests, 0)
        self.assertEqual(initial_snapshot.lazy_source_opens, 0)
        self.assertEqual(initial_snapshot.unopened_lazy_payloads, 1)

        graph.export_route(route)
        graph.grant_access(
            graph_module.CapabilityGrant(
                principal_id="inspector",
                route=route,
                metadata_read=True,
                payload_open=True,
                debug_read=True,
            )
        )
        response = graph.query(
            graph_module.QueryRequest(command="open_payload", route=route),
            requester_id="inspector",
        )

        self.assertEqual(response.items, ("point-cloud",))
        self.assertEqual(opens, 1)
        self.assertIn(
            "payload_open", [event.event_type for event in graph.audit(route)]
        )
        snapshot = graph.payload_demand_snapshot(route)
        self.assertEqual(snapshot.payload_open_requests, 1)
        self.assertEqual(snapshot.lazy_source_opens, 1)
        self.assertEqual(snapshot.materialized_payload_bytes, 11)
        self.assertEqual(snapshot.cache_hits, 0)
        self.assertEqual(snapshot.unopened_lazy_payloads, 0)

    def test_payload_demand_query_reports_lazy_open_accounting(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Bulk,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("frame"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="LidarFrame"),
        )
        graph = graph_module.Graph()

        graph.publish_lazy(
            route,
            graph_module.LazyPayloadSource(
                open=lambda: b"point-cloud", logical_length_bytes=11
            ),
        )
        graph.export_route(route)
        graph.grant_access(
            graph_module.CapabilityGrant(
                principal_id="inspector",
                route=route,
                metadata_read=True,
                payload_open=True,
            )
        )

        graph.open_payload(route)
        graph.open_payload(route)
        response = graph.query(
            graph_module.QueryRequest(command="payload_demand", route=route),
            requester_id="inspector",
        )

        self.assertEqual(
            response.items,
            (
                route.display(),
                "1",
                "2",
                "2",
                "22",
                "0",
                "0",
            ),
        )

    def test_payload_demand_snapshot_reopens_external_store_payloads_per_route(
        self,
    ) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Bulk,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("frame"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="LidarFrame"),
        )
        graph = graph_module.Graph()

        graph.publish_lazy(
            route,
            graph_module.LazyPayloadSource(
                open=lambda: b"frame-1", logical_length_bytes=7
            ),
        )
        graph.publish_lazy(
            route,
            graph_module.LazyPayloadSource(
                open=lambda: b"frame-2", logical_length_bytes=7
            ),
        )

        self.assertEqual(graph.open_payload(route), b"frame-2")
        self.assertEqual(graph.open_payload(route), b"frame-2")

        snapshot = graph.payload_demand_snapshot(route)

        self.assertEqual(snapshot.metadata_events, 2)
        self.assertEqual(snapshot.payload_open_requests, 2)
        self.assertEqual(snapshot.lazy_source_opens, 2)
        self.assertEqual(snapshot.materialized_payload_bytes, 14)
        self.assertEqual(snapshot.cache_hits, 0)
        self.assertEqual(snapshot.unopened_lazy_payloads, 1)

    def test_payload_demand_snapshot_caches_separate_store_payloads(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("camera"),
            family=graph_module.StreamFamily("frame"),
            stream=graph_module.StreamName("preview"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="PreviewFrame"),
        )
        graph = graph_module.Graph()
        opens = 0

        def open_payload():
            nonlocal opens
            opens += 1
            return b"preview"

        graph.publish_lazy(
            route,
            graph_module.LazyPayloadSource(open=open_payload, logical_length_bytes=7),
        )

        self.assertEqual(graph.open_payload(route), b"preview")
        self.assertEqual(graph.open_payload(route), b"preview")

        snapshot = graph.payload_demand_snapshot(route)

        self.assertEqual(opens, 1)
        self.assertEqual(snapshot.lazy_source_opens, 1)
        self.assertEqual(snapshot.materialized_payload_bytes, 7)
        self.assertEqual(snapshot.cache_hits, 1)
        self.assertEqual(snapshot.unopened_lazy_payloads, 0)

    def test_open_payload_preserves_specific_empty_inline_envelope(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("camera"),
            family=graph_module.StreamFamily("frame"),
            stream=graph_module.StreamName("preview"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="PreviewFrame"),
        )
        graph = graph_module.Graph()

        first = graph.publish(route, b"")
        graph.publish(route, b"next")

        self.assertEqual(graph.open_payload(first.closed), b"")
        self.assertEqual(graph.open_payload(route), b"next")

    def test_non_replayable_payload_retention_purges_old_lazy_sources(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Ephemeral,
            owner=graph_module.OwnerName("session"),
            family=graph_module.StreamFamily("trace"),
            stream=graph_module.StreamName("blob"),
            variant=graph_module.Variant.Event,
            schema=graph_module.Schema.bytes(name="TraceBlob"),
        )
        graph = graph_module.Graph()

        graph.publish_lazy(
            route,
            graph_module.LazyPayloadSource(open=lambda: b"blob-1", logical_length_bytes=6),
        )
        first = graph.latest(route.route_ref)
        self.assertIsNotNone(first)
        graph.publish_lazy(
            route,
            graph_module.LazyPayloadSource(open=lambda: b"blob-2", logical_length_bytes=6),
        )

        snapshot = graph.payload_demand_snapshot(route)

        assert first is not None
        self.assertIsNone(graph.open_payload(first))
        self.assertEqual(graph.open_payload(route), b"blob-2")
        self.assertEqual(snapshot.unopened_lazy_payloads, 1)

    def test_exported_route_allows_payload_demand_query_without_explicit_grant(
        self,
    ) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Bulk,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("frame"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="LidarFrame"),
        )
        graph = graph_module.Graph()

        graph.publish_lazy(
            route,
            graph_module.LazyPayloadSource(
                open=lambda: b"point-cloud", logical_length_bytes=11
            ),
        )
        graph.export_route(route)

        response = graph.query(
            graph_module.QueryRequest(command="payload_demand", route=route),
            requester_id="observer",
        )

        self.assertEqual(
            response.items,
            (
                route.display(),
                "1",
                "0",
                "0",
                "0",
                "0",
                "1",
            ),
        )

    def test_watermark_snapshot_uses_control_epoch_for_write_routes(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("motor"),
            family=graph_module.StreamFamily("speed"),
            stream=graph_module.StreamName("target"),
            variant=graph_module.Variant.Request,
            schema=graph_module.Schema.bytes(name="MotorTarget"),
        )
        graph = graph_module.Graph()

        graph.publish(route, b"500", control_epoch=7)
        snapshot = graph.watermark_snapshot(route)

        self.assertEqual(snapshot.route_display, route.display())
        self.assertEqual(snapshot.clock_domain, "control_epoch")
        self.assertEqual(snapshot.event_time_policy, "control_epoch_or_ingest")
        self.assertEqual(snapshot.latest_seq_source, 1)
        self.assertEqual(snapshot.latest_control_epoch, 7)
        self.assertEqual(snapshot.current_watermark, 7)

    def test_watermark_query_reports_bulk_route_progress(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Bulk,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("frame"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="LidarFrame"),
        )
        graph = graph_module.Graph()

        graph.publish_lazy(
            route,
            graph_module.LazyPayloadSource(
                open=lambda: b"point-cloud", logical_length_bytes=11
            ),
        )
        graph.export_route(route)
        response = graph.query(
            graph_module.QueryRequest(command="watermark", route=route),
            requester_id="observer",
        )

        self.assertEqual(
            response.items,
            (
                route.display(),
                "unpartitioned",
                "monotonic",
                "ingest",
                "recommended",
                "1",
                "None",
                "1",
            ),
        )

    def test_watermark_query_without_route_lists_registered_route_summaries(
        self,
    ) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Bulk,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("frame"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="LidarFrame"),
        )
        sink = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("motor"),
            family=graph_module.StreamFamily("speed"),
            stream=graph_module.StreamName("target"),
            variant=graph_module.Variant.Request,
            schema=graph_module.Schema.bytes(name="MotorTarget"),
        )
        graph = graph_module.Graph()

        graph.publish_lazy(
            source,
            graph_module.LazyPayloadSource(
                open=lambda: b"point-cloud", logical_length_bytes=11
            ),
        )
        graph.publish(sink, b"500", control_epoch=3)

        response = graph.query(graph_module.QueryRequest(command="watermark"))

        self.assertTrue(
            any(
                item.startswith(f"{source.display()}|1|1|None")
                for item in response.items
            )
        )
        self.assertTrue(
            any(item.startswith(f"{sink.display()}|3|1|3") for item in response.items)
        )

    def test_describe_route_exposes_rfc_descriptor_buckets(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Bulk,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("frame"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="LidarFrame"),
        )
        graph = graph_module.Graph()

        descriptor = graph.describe_route(route)

        self.assertEqual(descriptor.identity.route_ref.display(), route.display())
        self.assertEqual(descriptor.schema.payload_open_policy, "lazy_external")
        self.assertEqual(descriptor.flow.credit_class, "bulk_payload")
        self.assertEqual(
            descriptor.retention.payload_retention_policy, "external_store"
        )
        self.assertEqual(
            descriptor.environment.transport_preferences, ("memory", "bulk_link")
        )

    def test_describe_edge_composes_flow_policies_in_rfc_order(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("telemetry"),
            stream=graph_module.StreamName("source"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="SourceTelemetry"),
        )
        sink = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("processor"),
            family=graph_module.StreamFamily("telemetry"),
            stream=graph_module.StreamName("sink"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="SinkTelemetry"),
        )
        graph = graph_module.Graph()
        graph.configure_flow_defaults(
            graph_module.FlowPolicy(
                backpressure_policy="graph_default",
                credit_class="graph_credit",
                mailbox_policy="graph_mailbox",
                async_boundary_kind="graph_inline",
                overflow_policy="graph_overflow",
            )
        )
        graph.configure_source_flow(
            source,
            graph_module.FlowPolicy(
                backpressure_policy="source_push",
                credit_class="source_credit",
                async_boundary_kind="source_async",
            ),
        )
        graph.configure_sink_flow(
            sink,
            graph_module.FlowPolicy(
                backpressure_policy="sink_pull",
                overflow_policy="sink_block",
            ),
        )
        graph.connect(
            source=source,
            sink=sink,
            flow_policy=graph_module.FlowPolicy(credit_class="edge_credit"),
        )

        descriptor = graph.describe_edge(source=source, sink=sink)

        self.assertEqual(descriptor.backpressure_policy, "sink_pull")
        self.assertEqual(descriptor.credit_class, "edge_credit")
        self.assertEqual(descriptor.mailbox_policy, "graph_mailbox")
        self.assertEqual(descriptor.async_boundary_kind, "source_async")
        self.assertEqual(descriptor.overflow_policy, "sink_block")

    def test_describe_edge_uses_mailbox_flow_defaults(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("mailbox"),
            stream=graph_module.StreamName("producer"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="MailboxProducer"),
        )
        graph = graph_module.Graph()
        mailbox = graph.mailbox(
            "bridge",
            graph_module.NativeMailboxDescriptor(
                capacity=4, overflow_policy="drop_oldest"
            ),
        )
        graph.connect(source=source, sink=mailbox)

        descriptor = graph.describe_edge(source=source, sink=mailbox)

        self.assertEqual(descriptor.backpressure_policy, "propagate")
        self.assertEqual(descriptor.credit_class, "mailbox")
        self.assertEqual(descriptor.mailbox_policy, "queue")
        self.assertEqual(descriptor.async_boundary_kind, "mailbox")
        self.assertEqual(descriptor.overflow_policy, "drop_oldest")

    def test_mailbox_snapshot_tracks_depth_and_drop_oldest_overflow(self) -> None:
        graph_module = load_graph_module()
        producer_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("mailbox"),
            stream=graph_module.StreamName("producer"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="MailboxProducer"),
        )
        graph = graph_module.Graph()
        mailbox = graph.mailbox(
            "bridge",
            graph_module.NativeMailboxDescriptor(
                capacity=2, overflow_policy="drop_oldest"
            ),
        )
        graph.connect(source=producer_route, sink=mailbox)

        graph.publish(producer_route, b"one")
        graph.publish(producer_route, b"two")
        graph.publish(producer_route, b"three")

        snapshot = graph.mailbox_snapshot(mailbox)
        flow = graph.flow_snapshot(mailbox.ingress)

        self.assertEqual(snapshot.depth, 2)
        self.assertEqual(snapshot.available_credit, 0)
        self.assertEqual(snapshot.dropped_messages, 1)
        self.assertEqual(snapshot.overflow_policy, "drop_oldest")
        self.assertEqual(flow.available, 0)
        self.assertEqual(flow.dropped_messages, 1)
        self.assertEqual(flow.backpressure_policy, "propagate")

    def test_flow_snapshot_accepts_native_ports_and_reports_queue_capacity(
        self,
    ) -> None:
        graph_module = load_graph_module()
        producer_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("mailbox"),
            stream=graph_module.StreamName("producer"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="MailboxProducer"),
        )
        graph = graph_module.Graph()
        mailbox = graph.mailbox(
            "capacity_bridge",
            graph_module.NativeMailboxDescriptor(capacity=3, overflow_policy="block"),
        )
        graph.connect(source=producer_route, sink=mailbox)
        graph.publish(producer_route, b"one")
        graph.publish(producer_route, b"two")
        graph.publish(producer_route, b"three")
        graph.publish(producer_route, b"four")

        ingress = graph.flow_snapshot(mailbox.ingress)
        egress = graph.flow_snapshot(mailbox.egress)

        self.assertEqual(
            ingress.route_display, mailbox.ingress.describe().route_display
        )
        self.assertEqual(egress.route_display, mailbox.egress.describe().route_display)
        self.assertEqual(ingress.available, 0)
        self.assertEqual(ingress.blocked_senders, 1)
        self.assertEqual(ingress.largest_queue_depth, 3)
        self.assertEqual(egress.available, 0)
        self.assertEqual(egress.blocked_senders, 1)
        self.assertEqual(egress.largest_queue_depth, 3)

    def test_mailbox_snapshot_reports_blocked_writes_for_blocking_mailbox(self) -> None:
        graph_module = load_graph_module()
        producer_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("mailbox"),
            stream=graph_module.StreamName("producer"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="MailboxProducer"),
        )
        graph = graph_module.Graph()
        mailbox = graph.mailbox(
            "blocked_bridge",
            graph_module.NativeMailboxDescriptor(capacity=1, overflow_policy="block"),
        )
        graph.connect(source=producer_route, sink=mailbox)

        graph.publish(producer_route, b"one")
        graph.publish(producer_route, b"two")

        snapshot = graph.mailbox_snapshot(mailbox)

        self.assertEqual(snapshot.depth, 1)
        self.assertEqual(snapshot.available_credit, 0)
        self.assertEqual(snapshot.blocked_writes, 1)
        self.assertEqual(snapshot.dropped_messages, 0)

    def test_mailbox_snapshot_coalesces_latest_without_counting_drops(self) -> None:
        graph_module = load_graph_module()
        producer_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("mailbox"),
            stream=graph_module.StreamName("producer"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="MailboxProducer"),
        )
        graph = graph_module.Graph()
        mailbox = graph.mailbox(
            "coalesced_bridge",
            graph_module.NativeMailboxDescriptor(
                capacity=1,
                overflow_policy="coalesce_latest",
            ),
        )
        graph.connect(source=producer_route, sink=mailbox)

        graph.publish(producer_route, b"one")
        graph.publish(producer_route, b"two")

        snapshot = graph.mailbox_snapshot(mailbox)

        self.assertEqual(snapshot.capacity, 1)
        self.assertEqual(snapshot.depth, 1)
        self.assertEqual(snapshot.available_credit, 0)
        self.assertEqual(snapshot.blocked_writes, 0)
        self.assertEqual(snapshot.dropped_messages, 0)
        self.assertEqual(tuple(mailbox.egress.meta()), ())

    def test_flow_snapshot_accepts_registered_typed_routes(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("temperature"),
            stream=graph_module.StreamName("ambient"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="AmbientTemperature"),
        )
        graph = graph_module.Graph()

        graph.publish(route, b"24")

        snapshot = graph.flow_snapshot(route)

        self.assertEqual(snapshot.route_display, route.display())
        self.assertEqual(snapshot.available, 9223372036854775807)
        self.assertEqual(snapshot.blocked_senders, 0)
        self.assertEqual(snapshot.dropped_messages, 0)
        self.assertEqual(snapshot.backpressure_policy, "propagate")

    def test_flow_snapshot_rejects_unregistered_routes(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("temperature"),
            stream=graph_module.StreamName("ambient"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="AmbientTemperature"),
        )
        graph = graph_module.Graph()

        with self.assertRaisesRegex(KeyError, route.display()):
            graph.flow_snapshot(route)

    def test_credit_snapshot_query_returns_route_flow_details(self) -> None:
        graph_module = load_graph_module()
        producer_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("mailbox"),
            stream=graph_module.StreamName("producer"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="MailboxProducer"),
        )
        graph = graph_module.Graph()
        mailbox = graph.mailbox(
            "query_bridge",
            graph_module.NativeMailboxDescriptor(capacity=1, overflow_policy="block"),
        )
        ingress_route = mailbox.ingress._route
        graph.connect(source=producer_route, sink=mailbox)
        graph.publish(producer_route, b"one")
        graph.publish(producer_route, b"two")

        graph.export_route(ingress_route)
        graph.grant_access(
            graph_module.CapabilityGrant(
                principal_id="dashboard",
                route=ingress_route,
                metadata_read=True,
            )
        )
        response = graph.query(
            graph_module.QueryRequest(command="credit_snapshot", route=ingress_route),
            requester_id="dashboard",
        )

        self.assertEqual(response.items[1], "0")
        self.assertEqual(response.items[2], "1")
        self.assertEqual(response.items[3], "0")
        self.assertEqual(response.items[4], "propagate")

    def test_credit_snapshot_query_without_route_lists_registered_flow_summaries(
        self,
    ) -> None:
        graph_module = load_graph_module()
        producer_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("mailbox"),
            stream=graph_module.StreamName("producer"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="MailboxProducer"),
        )
        graph = graph_module.Graph()
        mailbox = graph.mailbox(
            "summary_bridge",
            graph_module.NativeMailboxDescriptor(capacity=1, overflow_policy="block"),
        )
        graph.connect(source=producer_route, sink=mailbox)
        graph.publish(producer_route, b"one")
        graph.publish(producer_route, b"two")

        response = graph.query(graph_module.QueryRequest(command="credit_snapshot"))

        self.assertTrue(
            any(
                item.startswith(f"{mailbox.ingress.describe().route_display}|0|1|0")
                for item in response.items
            )
        )
        self.assertTrue(
            any(
                item.startswith(f"{producer_route.display()}|9223372036854775807|0|0")
                for item in response.items
            )
        )

    def test_mailbox_snapshot_uses_default_descriptor_when_none_is_provided(
        self,
    ) -> None:
        graph_module = load_graph_module()
        producer_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("mailbox"),
            stream=graph_module.StreamName("producer"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="MailboxProducer"),
        )
        graph = graph_module.Graph()
        mailbox = graph.mailbox("default_bridge")
        graph.connect(source=producer_route, sink=mailbox)
        graph.publish(producer_route, b"one")

        snapshot = graph.mailbox_snapshot(mailbox)

        self.assertEqual(snapshot.name, "default_bridge")
        self.assertEqual(snapshot.capacity, 128)
        self.assertEqual(snapshot.overflow_policy, "block")
        self.assertEqual(snapshot.depth, 1)
        self.assertEqual(snapshot.available_credit, 127)

    def test_mailbox_forwarding_drains_queue_and_restores_credit_to_capacity(
        self,
    ) -> None:
        graph_module = load_graph_module()
        producer_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("mailbox"),
            stream=graph_module.StreamName("producer"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="MailboxProducer"),
        )
        consumer_route = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("consumer"),
            family=graph_module.StreamFamily("mailbox"),
            stream=graph_module.StreamName("consumer"),
            variant=graph_module.Variant.Request,
            schema=graph_module.Schema.bytes(name="MailboxConsumer"),
        )
        graph = graph_module.Graph()
        mailbox = graph.mailbox(
            "forwarding_bridge",
            graph_module.NativeMailboxDescriptor(
                capacity=5,
                overflow_policy="drop_oldest",
            ),
        )
        graph.connect(source=producer_route, sink=mailbox)
        graph.connect(source=mailbox, sink=consumer_route)

        graph.publish(producer_route, b"one")
        graph.publish(producer_route, b"two")

        snapshot = graph.mailbox_snapshot(mailbox)
        flow = graph.flow_snapshot(mailbox.ingress)
        latest = graph.latest(consumer_route)

        self.assertEqual(snapshot.depth, 0)
        self.assertEqual(snapshot.available_credit, 5)
        self.assertEqual(snapshot.blocked_writes, 0)
        self.assertEqual(snapshot.dropped_messages, 0)
        self.assertEqual(flow.available, 5)
        self.assertEqual(flow.largest_queue_depth, 1)
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value, b"two")
        self.assertEqual(latest.closed.seq_source, 2)

    def test_mailbox_snapshot_rejects_mailboxes_from_other_graphs(self) -> None:
        graph_module = load_graph_module()
        producer_route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("sensor"),
            family=graph_module.StreamFamily("mailbox"),
            stream=graph_module.StreamName("producer"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="MailboxProducer"),
        )
        first_graph = graph_module.Graph()
        mailbox = first_graph.mailbox("foreign_bridge")
        first_graph.connect(source=producer_route, sink=mailbox)
        first_graph.publish(producer_route, b"one")

        second_graph = graph_module.Graph()

        with self.assertRaisesRegex(KeyError, "unknown mailbox foreign_bridge"):
            second_graph.mailbox_snapshot(mailbox)

    def test_publish_guarded_releases_on_epoch_and_ack(self) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("light"),
            graph_module.StreamFamily("brightness"),
            graph_module.StreamName("level"),
            graph_module.Schema.bytes(name="Brightness"),
        )
        graph = graph_module.Graph()

        graph.publish_guarded(
            binding,
            b"80",
            not_before_epoch=2,
            wait_for_ack=binding.ack,
        )

        self.assertEqual(graph.run_scheduler(epoch=1), ())

        graph.reconcile_write_binding(binding.request, ack=b"ok")
        self.assertEqual(graph.run_scheduler(epoch=1), ())

        released = graph.run_scheduler(epoch=2)
        self.assertEqual(len(released), 1)
        shadow = graph.shadow_state(binding.request)
        self.assertTrue(shadow.pending_write)
        self.assertIsNotNone(shadow.desired)

    def test_publish_guarded_drops_expired_write_and_emits_audit_event(self) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("fan"),
            graph_module.StreamFamily("speed"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="FanSpeed"),
        )
        graph = graph_module.Graph()

        graph.publish_guarded(
            binding,
            b"1200",
            not_before_epoch=3,
            expires_at_epoch=2,
        )

        self.assertEqual(graph.run_scheduler(epoch=2), ())
        self.assertEqual(graph.run_scheduler(epoch=3), ())
        self.assertIsNone(graph.latest(binding.request))
        self.assertIn(
            "expired guarded write",
            " ".join(event.detail for event in graph.audit(binding.request)),
        )

    def test_publish_guarded_retries_until_ack_with_typed_policy(self) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("light"),
            graph_module.StreamFamily("brightness"),
            graph_module.StreamName("level"),
            graph_module.Schema.bytes(name="Brightness"),
        )
        graph = graph_module.Graph()

        graph.publish_guarded(
            binding,
            b"80",
            retry_policy=graph_module.RetryPolicy.fixed_backoff(
                max_attempts=3, backoff_epochs=1
            ),
        )

        first = graph.run_scheduler(epoch=1)
        self.assertEqual(len(first), 1)
        self.assertEqual(graph.latest(binding.request).seq_source, 1)

        self.assertEqual(graph.run_scheduler(epoch=2), ())

        second = graph.run_scheduler(epoch=3)
        self.assertEqual(len(second), 1)
        self.assertEqual(graph.latest(binding.request).seq_source, 2)

        graph.reconcile_write_binding(binding.request, ack=b"ok")
        self.assertEqual(graph.run_scheduler(epoch=4), ())
        self.assertEqual(graph.latest(binding.request).seq_source, 2)
        self.assertIn(
            "acknowledged guarded write",
            " ".join(event.detail for event in graph.audit(binding.request)),
        )

    def test_publish_guarded_exhausts_retry_budget_without_ack(self) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("pump"),
            graph_module.StreamFamily("flow"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="PumpFlow"),
        )
        graph = graph_module.Graph()

        graph.publish_guarded(
            binding,
            b"12",
            retry_policy=graph_module.RetryPolicy.immediate(max_attempts=2),
        )

        self.assertEqual(len(graph.run_scheduler(epoch=1)), 1)
        self.assertEqual(len(graph.run_scheduler(epoch=2)), 1)
        self.assertEqual(graph.run_scheduler(epoch=3), ())
        self.assertEqual(graph.latest(binding.request).seq_source, 2)
        self.assertIn(
            "exhausted guarded write retries",
            " ".join(event.detail for event in graph.audit(binding.request)),
        )

    def test_publish_guarded_ignores_stale_ack_baseline_until_new_ack_arrives(
        self,
    ) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("pump"),
            graph_module.StreamFamily("flow"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="PumpFlow"),
        )
        graph = graph_module.Graph()

        graph.reconcile_write_binding(binding, ack=b"old")
        graph.publish_guarded(
            binding,
            b"12",
            retry_policy=graph_module.RetryPolicy.immediate(max_attempts=3),
        )

        self.assertEqual(len(graph.run_scheduler(epoch=1)), 1)
        self.assertEqual(len(graph.run_scheduler(epoch=2)), 1)

        graph.reconcile_write_binding(binding, ack=b"new")
        self.assertEqual(graph.run_scheduler(epoch=3), ())
        self.assertEqual(graph.latest(binding.request).seq_source, 2)
        self.assertIn(
            "acknowledged guarded write",
            " ".join(event.detail for event in graph.audit(binding.request)),
        )

    def test_publish_guarded_retries_plain_route_until_explicit_ack_route_advances(
        self,
    ) -> None:
        graph_module = load_graph_module()
        request = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heater"),
            family=graph_module.StreamFamily("temperature"),
            stream=graph_module.StreamName("target"),
            variant=graph_module.Variant.Request,
            schema=graph_module.Schema.bytes(name="TemperatureTarget"),
        )
        ack = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Shadow,
            owner=graph_module.OwnerName("heater"),
            family=graph_module.StreamFamily("temperature"),
            stream=graph_module.StreamName("target"),
            variant=graph_module.Variant.Ack,
            schema=graph_module.Schema.bytes(name="TemperatureTargetAck"),
        )
        graph = graph_module.Graph()

        graph.publish_guarded(
            request,
            b"22",
            retry_policy=graph_module.RetryPolicy.immediate(max_attempts=3),
            ack_route=ack,
        )

        self.assertEqual(len(graph.run_scheduler(epoch=1)), 1)
        self.assertEqual(len(graph.run_scheduler(epoch=2)), 1)

        graph.publish(ack, b"ok")

        self.assertEqual(graph.run_scheduler(epoch=3), ())
        self.assertEqual(graph.latest(request).closed.seq_source, 2)
        self.assertIn(
            "acknowledged guarded write",
            " ".join(event.detail for event in graph.audit(request)),
        )

    def test_publish_guarded_accepts_typed_ack_route_baselines(self) -> None:
        graph_module = load_graph_module()
        request = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heater"),
            family=graph_module.StreamFamily("temperature"),
            stream=graph_module.StreamName("typed_target"),
            variant=graph_module.Variant.Request,
            schema=graph_module.Schema.bytes(name="TypedTemperatureTarget"),
        )
        ack = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Shadow,
            owner=graph_module.OwnerName("heater"),
            family=graph_module.StreamFamily("temperature"),
            stream=graph_module.StreamName("typed_target"),
            variant=graph_module.Variant.Ack,
            schema=graph_module.Schema.bytes(name="TypedTemperatureTargetAck"),
        )
        graph = graph_module.Graph()

        graph.publish(ack, b"old")
        graph.publish_guarded(
            request.route_ref,
            b"22",
            retry_policy=graph_module.RetryPolicy.immediate(max_attempts=3),
            ack_route=ack,
        )

        self.assertEqual(len(graph.run_scheduler(epoch=1)), 1)
        self.assertEqual(len(graph.run_scheduler(epoch=2)), 1)

        graph.publish(ack, b"new")

        self.assertEqual(graph.run_scheduler(epoch=3), ())
        self.assertEqual(graph.latest(request).closed.seq_source, 2)
        self.assertIn(
            "acknowledged guarded write",
            " ".join(event.detail for event in graph.audit(request)),
        )

    def test_scheduler_snapshot_reports_guarded_write_state_and_retry_gates(
        self,
    ) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("pump"),
            graph_module.StreamFamily("flow"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="PumpFlow"),
        )
        graph = graph_module.Graph()

        graph.publish_guarded(
            binding,
            b"12",
            not_before_epoch=3,
            retry_policy=graph_module.RetryPolicy.fixed_backoff(
                max_attempts=3, backoff_epochs=2
            ),
        )

        initial = tuple(graph.scheduler_snapshot(binding.request))
        self.assertEqual(len(initial), 1)
        self.assertEqual(initial[0].route_display, binding.request.display())
        self.assertEqual(initial[0].not_before_epoch, 3)
        self.assertTrue(initial[0].ack_observed)
        self.assertIsNone(initial[0].next_retry_epoch)
        self.assertFalse(initial[0].ready_now)

        graph.run_scheduler(epoch=3)
        after_first_attempt = tuple(graph.scheduler_snapshot(binding.request))
        self.assertEqual(len(after_first_attempt), 1)
        self.assertEqual(after_first_attempt[0].attempt_count, 1)
        self.assertFalse(after_first_attempt[0].ack_observed)
        self.assertEqual(after_first_attempt[0].next_retry_epoch, 6)
        self.assertFalse(after_first_attempt[0].ready_now)

        graph.run_scheduler(epoch=6)
        after_second_attempt = tuple(graph.scheduler_snapshot(binding.request))
        self.assertEqual(len(after_second_attempt), 1)
        self.assertEqual(after_second_attempt[0].attempt_count, 2)
        self.assertEqual(after_second_attempt[0].next_retry_epoch, 9)
        self.assertFalse(after_second_attempt[0].ready_now)

        graph.reconcile_write_binding(binding.request, ack=b"ok")
        after_ack = tuple(graph.scheduler_snapshot(binding.request))
        self.assertEqual(len(after_ack), 1)
        self.assertTrue(after_ack[0].ack_observed)
        self.assertFalse(after_ack[0].ready_now)

        graph.run_scheduler(epoch=7)
        self.assertEqual(tuple(graph.scheduler_snapshot(binding.request)), ())

    def test_scheduler_snapshot_orders_pending_writes_by_route_display(self) -> None:
        graph_module = load_graph_module()
        alpha = graph_module.WriteBindings.logical(
            graph_module.OwnerName("alpha"),
            graph_module.StreamFamily("flow"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="PumpFlow"),
        )
        zeta = graph_module.WriteBindings.logical(
            graph_module.OwnerName("zeta"),
            graph_module.StreamFamily("flow"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="PumpFlow"),
        )
        graph = graph_module.Graph()

        graph.publish_guarded(zeta, b"9", not_before_epoch=3)
        graph.publish_guarded(alpha, b"1", not_before_epoch=2)

        self.assertEqual(
            [snapshot.route_display for snapshot in graph.scheduler_snapshot()],
            [alpha.request.display(), zeta.request.display()],
        )

    def test_publish_guarded_retry_policy_requires_ack_route(self) -> None:
        graph_module = load_graph_module()
        request = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("heater"),
            family=graph_module.StreamFamily("temperature"),
            stream=graph_module.StreamName("target"),
            variant=graph_module.Variant.Request,
            schema=graph_module.Schema.bytes(name="TemperatureTarget"),
        )
        graph = graph_module.Graph()

        with self.assertRaisesRegex(ValueError, "retry_policy requires an ack_route"):
            graph.publish_guarded(
                request,
                b"22",
                retry_policy=graph_module.RetryPolicy.never(),
            )

    def test_retry_policy_rejects_non_positive_attempts_and_negative_backoff(
        self,
    ) -> None:
        graph_module = load_graph_module()

        with self.assertRaisesRegex(ValueError, "max_attempts must be positive"):
            graph_module.RetryPolicy(max_attempts=0)
        with self.assertRaisesRegex(ValueError, "backoff_epochs must be non-negative"):
            graph_module.RetryPolicy(max_attempts=1, backoff_epochs=-1)

    def test_shadow_state_tracks_pending_and_reconciliation(self) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("counter"),
            graph_module.StreamFamily("counter"),
            graph_module.StreamName("value"),
            graph_module.Schema.bytes(name="CounterValue"),
        )
        graph = graph_module.Graph()

        graph.publish(binding, b"41")
        initial = graph.shadow_state(binding.request)
        self.assertTrue(initial.pending_write)
        self.assertIsNotNone(initial.desired)
        self.assertIsNone(initial.effective)
        self.assertEqual(initial.coherence_taints, ("COHERENCE_WRITE_PENDING",))

        reconciled = graph.reconcile_write_binding(
            binding.request,
            reported=b"41",
            effective=b"41",
            ack=b"ok",
        )
        self.assertFalse(reconciled.pending_write)
        self.assertIsNotNone(reconciled.reported)
        self.assertIsNotNone(reconciled.effective)
        self.assertIsNotNone(reconciled.ack)
        self.assertEqual(reconciled.coherence_taints, ("COHERENCE_STABLE",))

        graph.export_route(binding.request)
        graph.grant_access(
            graph_module.CapabilityGrant(
                principal_id="dashboard",
                route=binding.request,
                metadata_read=True,
            )
        )
        response = graph.query(
            graph_module.QueryRequest(command="shadow", route=binding.request),
            requester_id="dashboard",
        )
        self.assertIn("stable", response.items)
        self.assertIn("COHERENCE_STABLE", response.items)

    def test_publish_write_binding_mirrors_desired_route_and_returns_request_envelope(
        self,
    ) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("counter"),
            graph_module.StreamFamily("loop"),
            graph_module.StreamName("count"),
            graph_module.Schema.bytes(name="CounterValue"),
        )
        graph = graph_module.Graph()

        request_envelope = graph.publish(binding, b"2")
        desired = graph.latest(binding.desired)
        request = graph.latest(binding.request)

        self.assertEqual(request_envelope.route.display(), binding.request.display())
        self.assertIsNotNone(desired)
        self.assertIsNotNone(request)
        assert desired is not None
        assert request is not None
        self.assertEqual(desired.payload_ref.inline_bytes, b"2")
        self.assertEqual(desired.seq_source, 1)
        self.assertEqual(request.payload_ref.inline_bytes, b"2")
        self.assertEqual(request.seq_source, 1)
        self.assertIn(
            "COHERENCE_WRITE_PENDING",
            tuple(taint.value_id for taint in request.taints),
        )
        self.assertEqual(graph.shadow_state(binding).coherence_taints, ("COHERENCE_WRITE_PENDING",))

    def test_route_audit_summarizes_write_binding_scope(self) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("light"),
            graph_module.StreamFamily("brightness"),
            graph_module.StreamName("level"),
            graph_module.Schema.bytes(name="Brightness"),
        )
        graph = graph_module.Graph()

        subscription = graph.observe(
            binding.effective,
            subscriber_id="dashboard",
        ).subscribe(lambda _: None)
        try:
            graph.publish(binding, b"80")
            graph.reconcile_write_binding(
                binding,
                reported=b"75",
                effective=b"75",
                ack=b"ok",
                producer=graph_module.ProducerRef("device-adapter", "device"),
            )

            snapshot = graph.route_audit(binding.effective)

            self.assertEqual(snapshot.route_display, binding.effective.display())
            self.assertEqual(
                snapshot.scope_routes,
                (
                    binding.request.display(),
                    binding.desired.display(),
                    binding.reported.display(),
                    binding.effective.display(),
                    binding.ack.display(),
                ),
            )
            self.assertEqual(
                snapshot.recent_producers, ("device-adapter", "python")
            )
            self.assertEqual(snapshot.active_subscribers, ("dashboard",))
            self.assertEqual(
                snapshot.related_write_requests,
                (f"{binding.request.display()}@1",),
            )
            self.assertTrue(
                any(
                    event.startswith("write:published")
                    for event in snapshot.recent_debug_events
                )
            )
        finally:
            subscription.dispose()

        self.assertEqual(graph.route_audit(binding.effective).active_subscribers, ())

    def test_route_audit_preserves_binding_scope_event_chronology(self) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("pump"),
            graph_module.StreamFamily("pressure"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="PressureTarget"),
        )
        graph = graph_module.Graph()

        graph.publish(binding, b"80")
        graph.publish(binding.effective, b"72")
        graph.publish(binding.reported, b"74")
        graph.publish(binding.ack, b"ok")

        self.assertEqual(
            graph.route_audit(binding.request).recent_debug_events,
            (
                f"write:published {binding.request.display()}",
                f"write:published {binding.effective.display()}",
                f"write:published {binding.reported.display()}",
                f"write:published {binding.ack.display()}",
            ),
        )

    def test_route_audit_includes_lifecycle_event_and_health_routes(self) -> None:
        graph_module = load_graph_module()
        graph = graph_module.Graph()
        lifecycle = graph.lifecycle(
            graph_module.OwnerName("device"),
            graph_module.StreamFamily("imu_left"),
            intent_schema=graph_module.Schema.bytes(name="LifecycleIntent"),
            observation_schema=graph_module.Schema.bytes(name="LifecycleObservation"),
            health_schema=graph_module.Schema.bytes(name="LifecycleHealth"),
        )

        graph.publish(lifecycle, b"CONNECTED")
        graph.publish(lifecycle.event, b"retrying")
        graph.publish(lifecycle.health, b"healthy")
        snapshot = graph.route_audit(lifecycle.event)

        self.assertEqual(snapshot.route_display, lifecycle.event.display())
        self.assertEqual(
            snapshot.scope_routes,
            (
                lifecycle.request.display(),
                lifecycle.desired.display(),
                lifecycle.reported.display(),
                lifecycle.effective.display(),
                lifecycle.event.display(),
                lifecycle.health.display(),
            ),
        )
        self.assertEqual(
            snapshot.related_write_requests,
            (f"{lifecycle.request.display()}@1",),
        )

    def test_route_audit_query_returns_scope_and_subscriber_details(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Bulk,
            owner=graph_module.OwnerName("lidar"),
            family=graph_module.StreamFamily("scan"),
            stream=graph_module.StreamName("frame"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes(name="LidarFrame"),
        )
        graph = graph_module.Graph()

        subscription = graph.observe(route, subscriber_id="inspector").subscribe(
            lambda _: None
        )
        try:
            graph.publish_lazy(
                route,
                graph_module.LazyPayloadSource(
                    open=lambda: b"point-cloud",
                    logical_length_bytes=11,
                ),
            )
            graph.open_payload(route)
            graph.export_route(route)
            graph.grant_access(
                graph_module.CapabilityGrant(
                    principal_id="auditor",
                    route=route,
                    metadata_read=True,
                    debug_read=True,
                )
            )

            response = graph.query(
                graph_module.QueryRequest(command="route_audit", route=route),
                requester_id="auditor",
            )

            self.assertEqual(response.items[0], route.display())
            self.assertIn(f"scope={route.display()}", response.items)
            self.assertIn("producers=python", response.items)
            self.assertIn("subscribers=inspector", response.items)
            self.assertTrue(
                any("payload_open:opened payload" in item for item in response.items)
            )
        finally:
            subscription.dispose()

    def test_scheduler_query_reports_pending_guarded_write_state(self) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("fan"),
            graph_module.StreamFamily("speed"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="FanSpeed"),
        )
        graph = graph_module.Graph()

        graph.publish_guarded(binding, b"1200", not_before_epoch=4)

        graph.export_route(binding.request)
        graph.grant_access(
            graph_module.CapabilityGrant(
                principal_id="dashboard",
                route=binding.request,
                metadata_read=True,
            )
        )
        graph.grant_access(
            graph_module.CapabilityGrant(
                principal_id="auditor",
                route=binding.request,
                metadata_read=True,
                graph_validation=True,
            )
        )

        scoped = graph.query(
            graph_module.QueryRequest(command="scheduler", route=binding.request),
            requester_id="dashboard",
        )
        self.assertEqual(
            scoped.items,
            (f"{binding.request.display()}|0|0|False|True|4|None",),
        )

        global_view = graph.query(
            graph_module.QueryRequest(command="scheduler"),
            requester_id="auditor",
        )
        self.assertEqual(global_view.items, scoped.items)

    def test_shadow_state_marks_stale_reported_when_device_has_not_caught_up(
        self,
    ) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("heater"),
            graph_module.StreamFamily("temperature"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="TemperatureTarget"),
        )
        graph = graph_module.Graph()

        graph.publish(binding, b"22")
        shadow = graph.reconcile_write_binding(binding, reported=b"20", effective=b"20")

        self.assertTrue(shadow.pending_write)
        self.assertEqual(
            shadow.coherence_taints,
            ("COHERENCE_WRITE_PENDING", "COHERENCE_STALE_REPORTED"),
        )

    def test_shadow_state_marks_echo_unmatched_when_effective_differs_from_reported(
        self,
    ) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("light"),
            graph_module.StreamFamily("brightness"),
            graph_module.StreamName("level"),
            graph_module.Schema.bytes(name="Brightness"),
        )
        graph = graph_module.Graph()

        graph.publish(binding, b"80")
        shadow = graph.reconcile_write_binding(binding, reported=b"70", effective=b"65")

        self.assertTrue(shadow.pending_write)
        self.assertEqual(
            shadow.coherence_taints,
            (
                "COHERENCE_WRITE_PENDING",
                "COHERENCE_STALE_REPORTED",
                "COHERENCE_ECHO_UNMATCHED",
            ),
        )

    def test_reconcile_write_binding_rejects_ack_without_ack_route(self) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("heater"),
            graph_module.StreamFamily("temperature"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="TemperatureTarget"),
        )
        graph = graph_module.Graph()

        unacked_binding = graph_module.WriteBinding(
            request=binding.request,
            desired=binding.desired,
            reported=binding.reported,
            effective=binding.effective,
            ack=None,
        )

        with self.assertRaisesRegex(ValueError, "does not define an ack route"):
            graph.reconcile_write_binding(unacked_binding, ack=b"ok")

    def test_validate_graph_reports_write_request_without_shadow_binding(self) -> None:
        graph_module = load_graph_module()
        request = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("light"),
            family=graph_module.StreamFamily("brightness"),
            stream=graph_module.StreamName("set"),
            variant=graph_module.Variant.Request,
            schema=graph_module.Schema.bytes(name="Brightness"),
        )
        graph = graph_module.Graph()

        graph.register_port(request)
        issues = list(graph.validate_graph())

        self.assertTrue(any("lacks a shadow binding" in issue for issue in issues))

    def test_validate_graph_rejects_unsafe_write_feedback_loop(self) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("motor"),
            graph_module.StreamFamily("speed"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="MotorSpeed"),
        )
        graph = graph_module.Graph()

        graph.publish(binding, b"500")
        graph.connect(source=binding.effective, sink=binding.request)
        issues = list(graph.validate_graph())

        self.assertTrue(any("Unsafe write-back loop" in issue for issue in issues))

    def test_validate_graph_allows_write_feedback_loop_with_internal_boundary(
        self,
    ) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("motor"),
            graph_module.StreamFamily("speed"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="MotorSpeed"),
        )
        bridge = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("boundary"),
            family=graph_module.StreamFamily("loop"),
            stream=graph_module.StreamName("delay"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.bytes(name="MotorSpeed"),
        )
        graph = graph_module.Graph()

        graph.publish(binding, b"500")
        graph.connect(source=binding.effective, sink=bridge)
        graph.connect(source=bridge, sink=binding.request)
        issues = list(graph.validate_graph())

        self.assertFalse(any("Unsafe write-back loop" in issue for issue in issues))

    def test_validate_graph_reports_unprotected_feedback_path_when_boundary_exists(
        self,
    ) -> None:
        graph_module = load_graph_module()
        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("motor"),
            graph_module.StreamFamily("speed"),
            graph_module.StreamName("target"),
            graph_module.Schema.bytes(name="MotorSpeed"),
        )
        bridge = graph_module.route(
            plane=graph_module.Plane.State,
            layer=graph_module.Layer.Internal,
            owner=graph_module.OwnerName("boundary"),
            family=graph_module.StreamFamily("loop"),
            stream=graph_module.StreamName("delay"),
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.bytes(name="MotorSpeed"),
        )
        graph = graph_module.Graph()

        graph.publish(binding, b"500")
        graph.connect(source=binding.effective, sink=bridge)
        graph.connect(source=bridge, sink=binding.request)
        graph.connect(source=binding.effective, sink=binding.request)
        issues = list(graph.validate_graph())

        self.assertTrue(any("Unsafe write-back loop" in issue for issue in issues))

    def test_route_accepts_wrapped_identity_namespace_and_schema_class(self) -> None:
        graph_module = load_graph_module()
        primitives = sys.modules["manyfold.primitives"]

        @dataclass(frozen=True)
        class FakeProto:
            payload: bytes

            def SerializeToString(self) -> bytes:
                return self.payload

            @staticmethod
            def FromString(payload: bytes) -> "FakeProto":
                return FakeProto(payload)

        raw_route = primitives.route(
            namespace=primitives.RouteNamespace(
                plane=graph_module.Plane.Write,
                layer=graph_module.Layer.Raw,
            ),
            identity=primitives.RouteIdentity.of(
                owner="led",
                family="pwm",
                stream="duty_cycle",
                variant=graph_module.Variant.Request,
            ),
            schema=bytes,
            schema_id="PwmDutyCycle",
        )
        proto_route = primitives.route(
            namespace=primitives.RouteNamespace(
                plane=graph_module.Plane.Read,
                layer=graph_module.Layer.Logical,
            ),
            identity=primitives.RouteIdentity.of(
                owner="imu",
                family="sensor",
                stream="accel",
                variant=graph_module.Variant.Meta,
            ),
            schema=FakeProto,
        )

        self.assertEqual(raw_route.display(), "write.raw.led.pwm.duty_cycle.request.v1")
        self.assertEqual(raw_route.schema.schema_id, "PwmDutyCycle")
        self.assertEqual(proto_route.schema.schema_id, "FakeProto")
        self.assertEqual(proto_route.schema.encode(FakeProto(b"x")), b"x")

    def test_route_defaults_to_basic_read_logical_meta_with_string_identity(
        self,
    ) -> None:
        graph_module = load_graph_module()
        primitives = sys.modules["manyfold.primitives"]

        route = primitives.route(
            owner="sensor",
            family="environment",
            stream="temperature",
            schema=graph_module.Schema.bytes(name="Temperature"),
        )

        self.assertEqual(
            route.display(),
            "read.logical.sensor.environment.temperature.meta.v1",
        )
        self.assertIs(route.plane, graph_module.Plane.Read)
        self.assertIs(route.layer, graph_module.Layer.Logical)
        self.assertIs(route.variant, graph_module.Variant.Meta)

    def test_typed_route_builds_derivative_route_from_existing_context(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            owner="sensor",
            family="environment",
            stream="temperature",
            schema=graph_module.Schema.float(name="Temperature"),
        )

        derivative = source.derivative_route(
            stream="average_temperature",
            schema=graph_module.Schema.float(name="AverageTemperature"),
        )

        self.assertEqual(
            derivative.display(),
            "read.logical.sensor.environment.average_temperature.meta.v1",
        )
        self.assertEqual(derivative.schema.schema_id, "AverageTemperature")
        self.assertIs(derivative.owner, source.owner)
        self.assertIs(derivative.family, source.family)
        self.assertIs(derivative.plane, source.plane)
        self.assertIs(derivative.layer, source.layer)
        self.assertIs(derivative.variant, source.variant)

    def test_typed_route_derivative_route_accepts_context_overrides(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            owner="sensor",
            family="environment",
            stream="temperature",
            schema=graph_module.Schema.float(name="Temperature"),
        )

        derivative = source.derivative_route(
            owner="analytics",
            family="rollup",
            stream="average_temperature",
            variant=graph_module.Variant.State,
            schema=graph_module.Schema.float(name="AverageTemperature"),
        )

        self.assertEqual(
            derivative.display(),
            "read.logical.analytics.rollup.average_temperature.state.v1",
        )

    def test_bytes_schema_requires_name_keyword(self) -> None:
        graph_module = load_graph_module()

        schema = graph_module.Schema.bytes(name="Temperature")

        self.assertEqual(schema.schema_id, "Temperature")
        with self.assertRaises(TypeError):
            graph_module.Schema.bytes("Temperature")

    def test_float_schema_round_trips_ascii_float_values(self) -> None:
        graph_module = load_graph_module()

        schema = graph_module.Schema.float(name="Temperature")

        self.assertEqual(schema.schema_id, "Temperature")
        self.assertEqual(schema.decode(schema.encode(72.4)), 72.4)

    def test_route_pipeline_moving_average_publishes_to_route(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            owner="sensor",
            family="environment",
            stream="temperature",
            schema=graph_module.Schema.float(name="Temperature"),
        )
        average = graph_module.route(
            owner="sensor",
            family="environment",
            stream="average_temperature",
            schema=graph_module.Schema.float(name="AverageTemperature"),
        )
        graph = graph_module.Graph()

        connection = graph.observe(source, replay_latest=False).moving_average(
            window_size=3
        ).connect(average)
        graph.publish(source, 72.4)
        graph.publish(source, 72.9)
        graph.publish(source, 73.7)

        latest = graph.latest(average)
        assert latest is not None
        node = next(graph.diagram_nodes())
        metadata = dict(node.metadata)
        self.assertAlmostEqual(latest.value, 73.0)
        self.assertEqual(latest.closed.seq_source, 6)
        self.assertEqual(node.name, "moving-average-1")
        self.assertEqual(
            node.input_routes,
            ("read.logical.sensor.environment.temperature.meta.v1",),
        )
        self.assertEqual(
            node.output_routes,
            ("read.internal.manyfold.graph.pipeline.moving-average-1-1.event.v1",),
        )
        self.assertEqual(metadata["statistic"], "moving_average")
        self.assertEqual(metadata["storage"], "sliding_capacitor")
        self.assertEqual(metadata["window_size"], "3")
        connection.dispose()
        self.assertEqual(tuple(graph.diagram_nodes()), ())

    def test_route_pipeline_moving_average_rejects_non_positive_window(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            owner="sensor",
            family="environment",
            stream="temperature",
            schema=graph_module.Schema.float(name="Temperature"),
        )
        graph = graph_module.Graph()

        with self.assertRaisesRegex(ValueError, "average window size must be positive"):
            graph.observe(source).moving_average(window_size=0)

    def test_route_preserves_existing_schema_version_without_override(self) -> None:
        graph_module = load_graph_module()
        primitives = sys.modules["manyfold.primitives"]

        route = primitives.route(
            namespace=primitives.RouteNamespace(
                plane=graph_module.Plane.Write,
                layer=graph_module.Layer.Logical,
            ),
            identity=primitives.RouteIdentity.of(
                owner="led",
                family="pwm",
                stream="duty_cycle",
                variant=graph_module.Variant.Request,
            ),
            schema=graph_module.Schema.bytes(name="PwmDutyCycle", version=7),
        )

        self.assertEqual(route.display(), "write.logical.led.pwm.duty_cycle.request.v7")
        self.assertEqual(route.schema.version, 7)

    def test_route_preserves_existing_schema_version_when_overriding_schema_id_only(
        self,
    ) -> None:
        graph_module = load_graph_module()
        primitives = sys.modules["manyfold.primitives"]

        route = primitives.route(
            namespace=primitives.RouteNamespace(
                plane=graph_module.Plane.Write,
                layer=graph_module.Layer.Logical,
            ),
            identity=primitives.RouteIdentity.of(
                owner="led",
                family="pwm",
                stream="duty_cycle",
                variant=graph_module.Variant.Request,
            ),
            schema=graph_module.Schema.bytes(name="OldPwmDutyCycle", version=7),
            schema_id="PwmDutyCycle",
        )

        self.assertEqual(route.display(), "write.logical.led.pwm.duty_cycle.request.v7")
        self.assertEqual(route.schema.schema_id, "PwmDutyCycle")
        self.assertEqual(route.schema.version, 7)

    def test_route_applies_schema_id_and_version_overrides_together(self) -> None:
        graph_module = load_graph_module()
        primitives = sys.modules["manyfold.primitives"]

        route = primitives.route(
            namespace=primitives.RouteNamespace(
                plane=graph_module.Plane.Write,
                layer=graph_module.Layer.Logical,
            ),
            identity=primitives.RouteIdentity.of(
                owner="led",
                family="pwm",
                stream="duty_cycle",
                variant=graph_module.Variant.Request,
            ),
            schema=graph_module.Schema.bytes(name="OldPwmDutyCycle", version=7),
            schema_id="PwmDutyCycle",
            version=8,
        )

        self.assertEqual(route.display(), "write.logical.led.pwm.duty_cycle.request.v8")
        self.assertEqual(route.schema.schema_id, "PwmDutyCycle")
        self.assertEqual(route.schema.version, 8)

    def test_load_graph_module_reloads_primitives_cleanly(self) -> None:
        load_graph_module()
        setattr(sys.modules["manyfold.primitives"], "SENTINEL", object())

        load_graph_module()

        self.assertNotIn("SENTINEL", vars(sys.modules["manyfold.primitives"]))

    def test_route_rejects_mixed_wrapped_and_unwrapped_arguments(self) -> None:
        graph_module = load_graph_module()
        primitives = sys.modules["manyfold.primitives"]

        with self.assertRaisesRegex(ValueError, "namespace or plane/layer"):
            primitives.route(
                plane=graph_module.Plane.Read,
                layer=graph_module.Layer.Logical,
                namespace=primitives.RouteNamespace(
                    plane=graph_module.Plane.Read,
                    layer=graph_module.Layer.Logical,
                ),
                identity=primitives.RouteIdentity.of(
                    owner="imu",
                    family="sensor",
                    stream="accel",
                    variant=graph_module.Variant.Meta,
                ),
                schema=bytes,
                schema_id="ImuReading",
            )

        with self.assertRaisesRegex(
            ValueError, "identity or owner/family/stream/variant"
        ):
            primitives.route(
                namespace=primitives.RouteNamespace(
                    plane=graph_module.Plane.Read,
                    layer=graph_module.Layer.Logical,
                ),
                owner=primitives.OwnerName("imu"),
                family=primitives.StreamFamily("sensor"),
                stream=primitives.StreamName("accel"),
                variant=graph_module.Variant.Meta,
                identity=primitives.RouteIdentity.of(
                    owner="imu",
                    family="sensor",
                    stream="accel",
                    variant=graph_module.Variant.Meta,
                ),
                schema=bytes,
                schema_id="ImuReading",
            )

    def test_write_bindings_logical_builds_shadow_routes_and_ack_schema(self) -> None:
        graph_module = load_graph_module()

        binding = graph_module.WriteBindings.logical(
            graph_module.OwnerName("light"),
            graph_module.StreamFamily("brightness"),
            graph_module.StreamName("level"),
            graph_module.Schema.bytes(name="Brightness", version=3),
        )

        self.assertEqual(
            binding.request.display(), "write.logical.light.brightness.level.request.v3"
        )
        self.assertEqual(
            binding.desired.display(), "write.shadow.light.brightness.level.desired.v3"
        )
        self.assertEqual(
            binding.reported.display(),
            "write.shadow.light.brightness.level.reported.v3",
        )
        self.assertEqual(
            binding.effective.display(),
            "write.shadow.light.brightness.level.effective.v3",
        )
        self.assertEqual(
            binding.ack.display(), "write.shadow.light.brightness.level.ack.v3"
        )
        assert binding.ack is not None
        self.assertEqual(binding.ack.schema.schema_id, "BrightnessAck")

    def test_write_bindings_lifecycle_builds_rfc_route_family(self) -> None:
        graph_module = load_graph_module()

        binding = graph_module.WriteBindings.lifecycle(
            graph_module.OwnerName("device"),
            graph_module.StreamFamily("imu_left"),
            intent_schema=graph_module.Schema.bytes(name="LifecycleIntent", version=2),
            observation_schema=graph_module.Schema.bytes(
                name="LifecycleObservation", version=2
            ),
            ack_schema=graph_module.Schema.bytes(name="LifecycleAck", version=2),
            health_schema=graph_module.Schema.bytes(name="LifecycleHealth", version=2),
        )

        self.assertEqual(
            binding.request.display(), "write.raw.device.imu_left.lifecycle.request.v2"
        )
        self.assertEqual(
            binding.desired.display(), "write.shadow.device.imu_left.lifecycle.desired.v2"
        )
        self.assertEqual(
            binding.reported.display(),
            "write.shadow.device.imu_left.lifecycle.reported.v2",
        )
        self.assertEqual(
            binding.effective.display(),
            "write.shadow.device.imu_left.lifecycle.effective.v2",
        )
        self.assertEqual(
            binding.event.display(), "read.internal.device.imu_left.lifecycle.event.v2"
        )
        assert binding.ack is not None
        self.assertEqual(
            binding.ack.display(), "write.shadow.device.imu_left.lifecycle.ack.v2"
        )
        assert binding.health is not None
        self.assertEqual(
            binding.health.display(),
            "read.internal.device.imu_left.lifecycle.health.v2",
        )

    def test_graph_lifecycle_registers_binding_and_supports_reconciliation(self) -> None:
        graph_module = load_graph_module()
        graph = graph_module.Graph()
        lifecycle = graph.lifecycle(
            graph_module.OwnerName("device"),
            graph_module.StreamFamily("radio"),
            intent_schema=graph_module.Schema.bytes(name="LifecycleIntent"),
            observation_schema=graph_module.Schema.bytes(name="LifecycleObservation"),
            ack_schema=graph_module.Schema.bytes(name="LifecycleAck"),
        )

        graph.publish(lifecycle, b"DISCOVERABLE")
        shadow = graph.reconcile_write_binding(
            lifecycle,
            reported=b"DISCOVERABLE",
            effective=b"DISCOVERABLE",
            ack=b"ok",
        )

        self.assertEqual(shadow.coherence_taints, ("COHERENCE_STABLE",))
        self.assertEqual(graph.shadow_state(lifecycle.request).ack.payload_ref.inline_bytes, b"ok")


if __name__ == "__main__":
    unittest.main()
