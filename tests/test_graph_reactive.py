from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "python" / "manyfold" / "graph.py"
PRIMITIVES_MODULE_PATH = REPO_ROOT / "python" / "manyfold" / "primitives.py"


def install_reactivex_stub() -> None:
    if (
        "reactivex" in sys.modules
        and "reactivex.subject" in sys.modules
        and "reactivex.operators" in sys.modules
    ):
        return

    class Disposable:
        def __init__(self, dispose=None):
            self._dispose = dispose or (lambda: None)

        def dispose(self) -> None:
            self._dispose()

    class _CallbackObserver:
        def __init__(self, on_next):
            self.on_next = on_next

        def on_error(self, error):
            raise error

        def on_completed(self):
            return None

    class Observable:
        def __init__(self, subscribe):
            self._subscribe = subscribe

        def subscribe(self, observer=None, scheduler=None):
            if callable(observer) and not hasattr(observer, "on_next"):
                observer = _CallbackObserver(observer)
            return self._subscribe(observer, scheduler)

        def pipe(self, *transforms):
            observable = self
            for transform in transforms:
                observable = transform(observable)
            return observable

    class Subject:
        def __init__(self):
            self._observers = []

        def subscribe(self, observer=None, scheduler=None):
            if callable(observer) and not hasattr(observer, "on_next"):
                observer = _CallbackObserver(observer)
            self._observers.append(observer)

            def unsubscribe() -> None:
                if observer in self._observers:
                    self._observers.remove(observer)

            return Disposable(unsubscribe)

        def on_next(self, value) -> None:
            for observer in list(self._observers):
                observer.on_next(value)

    def create(subscribe):
        return Observable(subscribe)

    def from_iterable(items):
        def subscribe(observer=None, scheduler=None):
            for item in items:
                observer.on_next(item)
            return Disposable()

        return Observable(subscribe)

    def op_map(mapper):
        def transform(source):
            def subscribe(observer=None, scheduler=None):
                return source.subscribe(
                    lambda item: observer.on_next(mapper(item)),
                    scheduler=scheduler,
                )

            return Observable(subscribe)

        return transform

    def op_filter(predicate):
        def transform(source):
            def subscribe(observer=None, scheduler=None):
                return source.subscribe(
                    lambda item: observer.on_next(item) if predicate(item) else None,
                    scheduler=scheduler,
                )

            return Observable(subscribe)

        return transform

    def op_share():
        return lambda source: source

    class ConnectableObservable(Observable):
        def __init__(self, source):
            self._source = source
            self._observers = []
            super().__init__(self._subscribe_connectable)

        def _subscribe_connectable(self, observer=None, scheduler=None):
            if callable(observer) and not hasattr(observer, "on_next"):
                observer = _CallbackObserver(observer)
            self._observers.append(observer)

            def unsubscribe() -> None:
                if observer in self._observers:
                    self._observers.remove(observer)

            return Disposable(unsubscribe)

        def connect(self):
            return self._source.subscribe(
                lambda item: [observer.on_next(item) for observer in list(self._observers)]
            )

    def op_publish():
        def transform(source):
            return ConnectableObservable(source)

        return transform

    rx_module = types.ModuleType("reactivex")
    rx_module.Observable = Observable
    rx_module.create = create
    rx_module.from_iterable = from_iterable
    subject_module = types.ModuleType("reactivex.subject")
    subject_module.Subject = Subject
    ops_module = types.ModuleType("reactivex.operators")
    ops_module.filter = op_filter
    ops_module.map = op_map
    ops_module.publish = op_publish
    ops_module.share = op_share
    sys.modules["reactivex"] = rx_module
    sys.modules["reactivex.subject"] = subject_module
    sys.modules["reactivex.operators"] = ops_module


def install_manyfold_rust_stub() -> None:
    if "manyfold._manyfold_rust" in sys.modules:
        return

    rust_module = types.ModuleType("manyfold._manyfold_rust")

    class EnumValue(str):
        def __repr__(self) -> str:
            return self

    class Plane:
        Read = EnumValue("read")
        Write = EnumValue("write")
        State = EnumValue("state")
        Query = EnumValue("query")
        Debug = EnumValue("debug")

    class Layer:
        Raw = EnumValue("raw")
        Logical = EnumValue("logical")
        Shadow = EnumValue("shadow")
        Bulk = EnumValue("bulk")
        Internal = EnumValue("internal")
        Ephemeral = EnumValue("ephemeral")

    class Variant:
        Meta = EnumValue("meta")
        Payload = EnumValue("payload")
        Request = EnumValue("request")
        Desired = EnumValue("desired")
        Reported = EnumValue("reported")
        Effective = EnumValue("effective")
        Ack = EnumValue("ack")
        State = EnumValue("state")
        QueryRequest = EnumValue("query_request")
        QueryResponse = EnumValue("query_response")
        Event = EnumValue("event")
        Health = EnumValue("health")

    @dataclass(frozen=True)
    class NamespaceRef:
        plane: str
        layer: str
        owner: str

    @dataclass(frozen=True)
    class SchemaRef:
        schema_id: str
        version: int

    @dataclass(frozen=True)
    class RouteRef:
        namespace: NamespaceRef
        family: str
        stream: str
        variant: str
        schema: SchemaRef

        def display(self) -> str:
            return (
                f"{self.namespace.plane}.{self.namespace.layer}.{self.namespace.owner}."
                f"{self.family}.{self.stream}.v{self.schema.version}"
            )

    @dataclass
    class PayloadRef:
        payload_id: str
        logical_length_bytes: int
        codec_id: str
        inline_bytes: bytes

    @dataclass
    class ClosedEnvelope:
        route: RouteRef
        payload_ref: PayloadRef
        seq_source: int

    @dataclass(frozen=True)
    class ProducerRef:
        producer_id: str
        kind: str = "application"

    @dataclass(frozen=True)
    class PortDescriptor:
        route_display: str
        human_description: str = ""
        payload_open_policy: str = "lazy"
        backpressure_policy: str = "propagate"
        debug_enabled: bool = True

    class ReadablePort:
        def __init__(self, graph, route):
            self._graph = graph
            self._route = route

        def meta(self):
            latest = self.latest()
            return [] if latest is None else [latest]

        def open(self):
            return []

        def latest(self):
            return self._graph._latest.get(self._route.display())

        def describe(self):
            return PortDescriptor(route_display=self._route.display())

    class WritablePort:
        def __init__(self, graph, route):
            self._graph = graph
            self._route = route

        def write(self, payload, producer=None, control_epoch=None):
            seq_source = self._graph._sequence.get(self._route.display(), 0) + 1
            self._graph._sequence[self._route.display()] = seq_source
            envelope = ClosedEnvelope(
                route=self._route,
                payload_ref=PayloadRef(
                    payload_id=f"{self._route.display()}:{seq_source}",
                    logical_length_bytes=len(payload),
                    codec_id="identity",
                    inline_bytes=bytes(payload),
                ),
                seq_source=seq_source,
            )
            self._graph._latest[self._route.display()] = envelope
            return envelope

        def describe(self):
            return PortDescriptor(route_display=self._route.display())

    @dataclass
    class WriteBinding:
        request: RouteRef
        desired: RouteRef
        reported: RouteRef
        effective: RouteRef
        ack: Optional[RouteRef] = None

    @dataclass
    class MailboxDescriptor:
        capacity: int = 128

    @dataclass
    class Mailbox:
        name: str

    @dataclass
    class ControlLoop:
        name: str
        read_routes: list[RouteRef]
        write_request: RouteRef
        epoch: int = 0

    class Graph:
        def __init__(self):
            self._latest = {}
            self._sequence = {}
            self._loops = {}
            self._catalog = {}
            self._edges = []

        def register_port(self, route):
            self._catalog[route.display()] = route
            return route

        def read(self, route):
            self.register_port(route)
            return ReadablePort(self, route)

        def writable_port(self, route):
            self.register_port(route)
            return WritablePort(self, route)

        def register_binding(self, name, binding):
            return binding

        def mailbox(self, name, descriptor=None):
            return Mailbox(name=name)

        def connect(self, source, sink):
            self.register_port(source)
            self.register_port(sink)
            self._edges.append((source.display(), sink.display()))
            return None

        def install(self, control_loop):
            self._loops[control_loop.name] = control_loop

        def tick_control_loop(self, name):
            loop = self._loops[name]
            loop.epoch += 1
            envelope = ClosedEnvelope(
                route=loop.write_request,
                payload_ref=PayloadRef(
                    payload_id=f"{loop.name}:{loop.epoch}",
                    logical_length_bytes=0,
                    codec_id="identity",
                    inline_bytes=b"",
                ),
                seq_source=loop.epoch,
            )
            self._latest[loop.write_request.display()] = envelope
            return envelope

        def catalog(self):
            return list(self._catalog.values())

        def describe_route(self, route):
            return PortDescriptor(route_display=route.display())

        def latest(self, route):
            return self._latest.get(route.display())

        def topology(self):
            return list(self._edges)

        def validate_graph(self):
            return []

    rust_module.ControlLoop = ControlLoop
    rust_module.ClosedEnvelope = ClosedEnvelope
    rust_module.Graph = Graph
    rust_module.Layer = Layer
    rust_module.Mailbox = Mailbox
    rust_module.MailboxDescriptor = MailboxDescriptor
    rust_module.NamespaceRef = NamespaceRef
    rust_module.PortDescriptor = PortDescriptor
    rust_module.Plane = Plane
    rust_module.ProducerRef = ProducerRef
    rust_module.ReadablePort = ReadablePort
    rust_module.RouteRef = RouteRef
    rust_module.SchemaRef = SchemaRef
    rust_module.Variant = Variant
    rust_module.WritablePort = WritablePort
    rust_module.WriteBinding = WriteBinding
    sys.modules["manyfold._manyfold_rust"] = rust_module


def load_graph_module():
    install_reactivex_stub()
    install_manyfold_rust_stub()
    package = types.ModuleType("manyfold")
    package.__path__ = [str(REPO_ROOT / "python" / "manyfold")]
    sys.modules["manyfold"] = package
    primitives_spec = importlib.util.spec_from_file_location("manyfold.primitives", PRIMITIVES_MODULE_PATH)
    primitives_module = importlib.util.module_from_spec(primitives_spec)
    assert primitives_spec is not None and primitives_spec.loader is not None
    sys.modules[primitives_spec.name] = primitives_module
    primitives_spec.loader.exec_module(primitives_module)
    spec = importlib.util.spec_from_file_location("manyfold.graph", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class GraphReactiveTests(unittest.TestCase):
    def test_observe_replays_latest_and_pushes_future_writes(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes("Accel"),
        )
        graph = graph_module.Graph()

        graph.publish(route, b"first")
        observed = []
        subscription = graph.observe(route).subscribe(lambda envelope: observed.append(envelope))
        graph.publish(route, b"second")
        subscription.dispose()

        self.assertEqual([item.value for item in observed], [b"first", b"second"])
        self.assertEqual([item.closed.seq_source for item in observed], [1, 2])

    def test_pipe_binds_rx_source_into_writable_port(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("motor"),
            family=graph_module.StreamFamily("speed"),
            stream=graph_module.StreamName("command"),
            variant=graph_module.Variant.Request,
            schema=graph_module.Schema.bytes("SpeedCommand"),
        )
        graph = graph_module.Graph()

        graph.pipe(graph_module.rx.from_iterable([b"one", b"two"]), route)
        latest = graph.latest(route)

        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest.value, b"two")
        self.assertEqual(latest.closed.seq_source, 2)

    def test_read_then_write_next_epoch_step_installs_shared_write_stream(self) -> None:
        graph_module = load_graph_module()
        write_request = graph_module.route(
            plane=graph_module.Plane.Write,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("motor"),
            family=graph_module.StreamFamily("speed"),
            stream=graph_module.StreamName("pid"),
            variant=graph_module.Variant.Request,
            schema=graph_module.Schema.bytes("SpeedPid"),
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
            schema=graph_module.Schema.bytes("Accel"),
        )
        right = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("right"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("gyro"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes("Gyro"),
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

    def test_query_plane_streams_and_capabilities(self) -> None:
        graph_module = load_graph_module()
        route = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes("Accel"),
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

    def test_register_middleware_link_and_mesh_primitive(self) -> None:
        graph_module = load_graph_module()
        source = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("imu"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes("Accel"),
        )
        sink = graph_module.route(
            plane=graph_module.Plane.Read,
            layer=graph_module.Layer.Logical,
            owner=graph_module.OwnerName("dashboard"),
            family=graph_module.StreamFamily("sensor"),
            stream=graph_module.StreamName("accel_copy"),
            variant=graph_module.Variant.Meta,
            schema=graph_module.Schema.bytes("Accel"),
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


if __name__ == "__main__":
    unittest.main()
