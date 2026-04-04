from __future__ import annotations

import importlib
import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIR = REPO_ROOT / "python" / "manyfold"


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

    def op_distinct():
        def transform(source):
            seen = []

            def subscribe(observer=None, scheduler=None):
                def on_next(item):
                    if item not in seen:
                        seen.append(item)
                        observer.on_next(item)

                return source.subscribe(on_next, scheduler=scheduler)

            return Observable(subscribe)

        return transform

    def op_publish():
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
    ops_module.distinct = op_distinct
    ops_module.publish = op_publish
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

    class ProducerKind:
        Device = EnumValue("device")
        FirmwareAgent = EnumValue("firmware_agent")
        Transform = EnumValue("transform")
        ControlLoop = EnumValue("control_loop")
        Mailbox = EnumValue("mailbox")
        QueryService = EnumValue("query_service")
        Application = EnumValue("application")

    class TaintDomain:
        Time = EnumValue("time")
        Order = EnumValue("order")
        Delivery = EnumValue("delivery")
        Determinism = EnumValue("determinism")
        Scheduling = EnumValue("scheduling")
        Trust = EnumValue("trust")
        Coherence = EnumValue("coherence")

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
        logical_length_bytes: int = 0
        codec_id: str = "identity"
        inline_bytes: bytes = b""

    @dataclass
    class ProducerRef:
        producer_id: str
        kind: str

    @dataclass
    class RuntimeRef:
        runtime_id: str

    @dataclass
    class ClockDomainRef:
        clock_domain_id: str

    @dataclass
    class TaintMark:
        domain: str
        value_id: str
        origin_id: str

    @dataclass
    class ScheduleGuard:
        expires_at_epoch: Optional[int] = None

        @staticmethod
        def not_before_epoch(epoch: int) -> "ScheduleGuard":
            return ScheduleGuard(expires_at_epoch=epoch)

        @staticmethod
        def wait_for_ack(route: RouteRef) -> "ScheduleGuard":
            return ScheduleGuard()

    @dataclass
    class ClosedEnvelope:
        route: RouteRef
        payload_ref: PayloadRef
        seq_source: int
        producer: ProducerRef = None
        emitter: RuntimeRef = None
        control_epoch: Optional[int] = None
        taints: list[TaintMark] = None
        guards: list[ScheduleGuard] = None

        def __post_init__(self) -> None:
            if self.producer is None:
                self.producer = ProducerRef("python", ProducerKind.Application)
            if self.emitter is None:
                self.emitter = RuntimeRef("runtime:stub")
            if self.taints is None:
                self.taints = []
            if self.guards is None:
                self.guards = []

    @dataclass
    class OpenedEnvelope:
        closed: ClosedEnvelope
        payload: bytes

    @dataclass
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
            latest = self.latest()
            if latest is None:
                return []
            return [OpenedEnvelope(closed=latest, payload=latest.payload_ref.inline_bytes)]

        def latest(self):
            return self._graph._latest.get(self._route)

        def describe(self):
            return PortDescriptor(route_display=self._route.display())

    class WritablePort:
        def __init__(self, graph, route):
            self._graph = graph
            self._route = route

        def write(self, payload, producer=None, control_epoch=None):
            seq_source = self._graph._sequence.get(self._route, 0) + 1
            self._graph._sequence[self._route] = seq_source
            envelope = ClosedEnvelope(
                route=self._route,
                payload_ref=PayloadRef(
                    payload_id=f"{self._route.display()}:{seq_source}",
                    logical_length_bytes=len(payload),
                    codec_id="identity",
                    inline_bytes=bytes(payload),
                ),
                seq_source=seq_source,
                producer=producer,
                control_epoch=control_epoch,
            )
            self._graph._latest[self._route] = envelope
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
        delivery_mode: str = "mpsc_serial"
        ordering_policy: str = "fifo"
        overflow_policy: str = "block"

    @dataclass
    class Mailbox:
        name: str
        ingress: WritablePort
        egress: ReadablePort

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
            self._edges = []

        def register_port(self, route):
            return route

        def read(self, route):
            return ReadablePort(self, route)

        def writable_port(self, route):
            return WritablePort(self, route)

        def register_binding(self, name, binding):
            return binding

        def mailbox(self, name, descriptor=None):
            ingress_route = RouteRef(
                namespace=NamespaceRef(plane=Plane.Write, layer=Layer.Internal, owner=name),
                family="mailbox",
                stream=name,
                variant=Variant.Request,
                schema=SchemaRef("MailboxIngress", 1),
            )
            egress_route = RouteRef(
                namespace=NamespaceRef(plane=Plane.Read, layer=Layer.Internal, owner=name),
                family="mailbox",
                stream=name,
                variant=Variant.Meta,
                schema=SchemaRef("MailboxEgress", 1),
            )
            return Mailbox(
                name=name,
                ingress=WritablePort(self, ingress_route),
                egress=ReadablePort(self, egress_route),
            )

        def connect(self, source, sink):
            if hasattr(source, "egress"):
                source = source.egress._route
            if hasattr(sink, "ingress"):
                sink = sink.ingress._route
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
            self._latest[loop.write_request] = envelope
            return envelope

        def catalog(self):
            return []

        def describe_route(self, route):
            return PortDescriptor(route_display=route.display())

        def latest(self, route):
            return self._latest.get(route)

        def topology(self):
            return list(self._edges)

        def validate_graph(self):
            return []

    rust_module.ClockDomainRef = ClockDomainRef
    rust_module.ClosedEnvelope = ClosedEnvelope
    rust_module.ControlLoop = ControlLoop
    rust_module.Graph = Graph
    rust_module.Layer = Layer
    rust_module.Mailbox = Mailbox
    rust_module.MailboxDescriptor = MailboxDescriptor
    rust_module.NamespaceRef = NamespaceRef
    rust_module.OpenedEnvelope = OpenedEnvelope
    rust_module.PayloadRef = PayloadRef
    rust_module.Plane = Plane
    rust_module.PortDescriptor = PortDescriptor
    rust_module.ProducerKind = ProducerKind
    rust_module.ProducerRef = ProducerRef
    rust_module.ReadablePort = ReadablePort
    rust_module.RouteRef = RouteRef
    rust_module.RuntimeRef = RuntimeRef
    rust_module.ScheduleGuard = ScheduleGuard
    rust_module.SchemaRef = SchemaRef
    rust_module.TaintDomain = TaintDomain
    rust_module.TaintMark = TaintMark
    rust_module.Variant = Variant
    rust_module.WritablePort = WritablePort
    rust_module.WriteBinding = WriteBinding
    rust_module.bridge_version = lambda: "stub"
    sys.modules["manyfold._manyfold_rust"] = rust_module


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_manyfold_package():
    install_reactivex_stub()
    install_manyfold_rust_stub()
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    package = types.ModuleType("manyfold")
    package.__path__ = [str(PACKAGE_DIR)]
    sys.modules["manyfold"] = package

    primitives = _load_module("manyfold.primitives", PACKAGE_DIR / "primitives.py")
    graph = _load_module("manyfold.graph", PACKAGE_DIR / "graph.py")
    embedded = _load_module("manyfold.embedded", PACKAGE_DIR / "embedded.py")
    rust = sys.modules["manyfold._manyfold_rust"]

    exports = {
        "ClockDomainRef": rust.ClockDomainRef,
        "ClosedEnvelope": rust.ClosedEnvelope,
        "ControlLoop": rust.ControlLoop,
        "ControlLoops": graph.ControlLoops,
        "EmbeddedBulkSensor": embedded.EmbeddedBulkSensor,
        "EmbeddedDeviceProfile": embedded.EmbeddedDeviceProfile,
        "EmbeddedRuntimeRules": embedded.EmbeddedRuntimeRules,
        "EmbeddedScalarSensor": embedded.EmbeddedScalarSensor,
        "FirmwareAgentProfile": embedded.FirmwareAgentProfile,
        "Graph": graph.Graph,
        "Layer": rust.Layer,
        "Mailbox": rust.Mailbox,
        "MailboxDescriptor": rust.MailboxDescriptor,
        "NamespaceRef": rust.NamespaceRef,
        "OpenedEnvelope": rust.OpenedEnvelope,
        "OwnerName": primitives.OwnerName,
        "PayloadRef": rust.PayloadRef,
        "Plane": rust.Plane,
        "PortDescriptor": rust.PortDescriptor,
        "ProducerKind": rust.ProducerKind,
        "ProducerRef": rust.ProducerRef,
        "ReadablePort": rust.ReadablePort,
        "ReadThenWriteNextEpochStep": primitives.ReadThenWriteNextEpochStep,
        "RouteRef": rust.RouteRef,
        "RuntimeRef": rust.RuntimeRef,
        "ScheduleGuard": rust.ScheduleGuard,
        "Schema": primitives.Schema,
        "SchemaRef": rust.SchemaRef,
        "StreamFamily": primitives.StreamFamily,
        "StreamName": primitives.StreamName,
        "TaintDomain": rust.TaintDomain,
        "TaintMark": rust.TaintMark,
        "TypedEnvelope": primitives.TypedEnvelope,
        "TypedRoute": primitives.TypedRoute,
        "Variant": rust.Variant,
        "WritablePort": rust.WritablePort,
        "WriteBinding": rust.WriteBinding,
        "WriteBindings": graph.WriteBindings,
        "bridge_version": rust.bridge_version,
        "route": primitives.route,
    }
    for name, value in exports.items():
        setattr(package, name, value)

    reference_examples = _load_module("manyfold.reference_examples", PACKAGE_DIR / "reference_examples.py")
    package.REFERENCE_EXAMPLE_SUITE = reference_examples.REFERENCE_EXAMPLE_SUITE
    package.ReferenceExample = reference_examples.ReferenceExample
    package.implemented_reference_examples = reference_examples.implemented_reference_examples
    package.reference_example_suite = reference_examples.reference_example_suite
    return package


def load_example_module(name: str):
    load_manyfold_package()
    full_name = f"examples.{name}"
    if full_name in sys.modules:
        del sys.modules[full_name]
    return importlib.import_module(full_name)
