from __future__ import annotations

import importlib
import importlib.util
import os
import re
import struct
import sys
import threading
import time
import types
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_DIR = REPO_ROOT / "python" / "manyfold"
PYTHON_ROOT = REPO_ROOT / "python"
MODULES_TO_RESET = (
    "manyfold",
    "manyfold.primitives",
    "manyfold.graph",
    "manyfold.embedded",
    "manyfold.components",
    "manyfold.lego_catalog",
    "manyfold.sensor_io",
    "manyfold.reference_examples",
    "manyfold.streams",
    "manyfold.datastream_threads",
    "manyfold.stats",
)
_MISSING_MODULE = object()
UV_BIN_DIR = Path(os.environ.get("UV_BIN_DIR", Path.home() / ".local" / "bin"))


def subprocess_test_env() -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = _pythonpath_with_repo_python_first(env.get("PYTHONPATH"))
    if (UV_BIN_DIR / "uv").exists():
        env["PATH"] = f"{UV_BIN_DIR}{os.pathsep}{env.get('PATH', '')}"
    env.setdefault("UV_CACHE_DIR", str(REPO_ROOT / ".cache" / "uv"))
    return env


def install_manyfold_rust_stub() -> None:
    if "manyfold._manyfold_rust" in sys.modules:
        return

    rust_module = types.ModuleType("manyfold._manyfold_rust")

    def parse_sql_statement(sql: str) -> dict[str, str]:
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        stripped = sql.strip()
        statements = tuple(part.strip() for part in stripped.split(";") if part.strip())
        if len(statements) != 1:
            raise ValueError("SQL planner requires exactly one statement")
        stripped = statements[0]
        match = re.match(r"\A([A-Za-z]+)\b", stripped)
        if match is None:
            raise ValueError("invalid SQL: expected statement")
        keyword = match.group(1).lower()
        if re.match(r"\ASELECT\s+FROM\b", stripped, re.IGNORECASE):
            raise ValueError("invalid SQL: incomplete statement")
        if keyword == "select":
            return {"kind": "select", "sql": stripped}
        if keyword == "insert" and re.search(r"\bVALUES\b", stripped, re.IGNORECASE):
            return {"kind": "insert", "sql": stripped}
        if keyword == "update" and re.search(r"\bSET\b", stripped, re.IGNORECASE):
            return {"kind": "update", "sql": stripped}
        if keyword == "delete" and re.search(r"\bFROM\b", stripped, re.IGNORECASE):
            return {"kind": "delete", "sql": stripped}
        if keyword in {"select", "insert", "update", "delete"}:
            raise ValueError("invalid SQL: incomplete statement")
        return {"kind": "unsupported", "sql": stripped}

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
        Bridge = EnumValue("bridge")
        Reconciler = EnumValue("reconciler")
        LifecycleService = EnumValue("lifecycle_service")

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
                f"{self.family}.{self.stream}.{self.variant}.v{self.schema.version}"
            )

    @dataclass(frozen=True)
    class PayloadRef:
        payload_id: str
        logical_length_bytes: int = 0
        codec_id: str = "identity"
        inline_bytes: bytes = b""

    @dataclass(frozen=True)
    class ProducerRef:
        producer_id: str
        kind: str

    @dataclass(frozen=True)
    class RuntimeRef:
        runtime_id: str

    @dataclass(frozen=True)
    class ClockDomainRef:
        clock_domain_id: str

    class ManyFoldLockLease:
        def __init__(
            self,
            lock: "ManyFoldLock",
            *,
            owner: str | None = None,
        ) -> None:
            self._lock = lock
            self.lock_name = lock.name
            self.owner = owner
            self.is_released = False

        def __enter__(self) -> "ManyFoldLockLease":
            return self

        def __exit__(self, *_exc: object) -> None:
            self.release()

        def release(self) -> None:
            if self.is_released:
                return
            self.is_released = True
            self._lock._release()

    class ManyFoldLock:
        _registry: dict[str, "ManyFoldLock"] = {}
        _registry_lock = threading.Lock()

        def __init__(self, name: str) -> None:
            self.name = name.strip()
            self.path = f"stub://manyfold-lock/{self.name}"
            self._lock = threading.Lock()

        @classmethod
        def for_resource(cls, name: str) -> "ManyFoldLock":
            normalized = name.strip()
            with cls._registry_lock:
                lock = cls._registry.get(normalized)
                if lock is None:
                    lock = cls(normalized)
                    cls._registry[normalized] = lock
                return lock

        def take(
            self,
            *,
            owner: str | None = None,
            blocking: bool = True,
        ) -> ManyFoldLockLease:
            if not self._lock.acquire(blocking=blocking):
                raise RuntimeError(f"lock {self.name!r} is already held")
            return ManyFoldLockLease(self, owner=owner)

        def _release(self) -> None:
            self._lock.release()

    @dataclass(frozen=True)
    class TaintMark:
        domain: str
        value_id: str
        origin_id: str

    @dataclass(frozen=True)
    class ScheduleGuard:
        expires_at_epoch: Optional[int] = None

        @staticmethod
        def not_before_epoch(epoch: int) -> "ScheduleGuard":
            return ScheduleGuard(expires_at_epoch=epoch)

        @staticmethod
        def wait_for_ack(route: RouteRef) -> "ScheduleGuard":
            return ScheduleGuard()

    @dataclass(frozen=True)
    class ClosedEnvelope:
        route: RouteRef
        payload_ref: PayloadRef
        seq_source: int
        producer: ProducerRef = None
        emitter: RuntimeRef = None
        control_epoch: Optional[int] = None
        taints: tuple[TaintMark, ...] = ()
        guards: tuple[ScheduleGuard, ...] = ()

        def __post_init__(self) -> None:
            if self.producer is None:
                object.__setattr__(
                    self, "producer", ProducerRef("python", ProducerKind.Application)
                )
            if self.emitter is None:
                object.__setattr__(self, "emitter", RuntimeRef("runtime:stub"))
            object.__setattr__(self, "taints", tuple(self.taints or ()))
            object.__setattr__(self, "guards", tuple(self.guards or ()))

        @property
        def payload_id(self) -> str:
            return self.payload_ref.payload_id

        @property
        def has_inline_payload(self) -> bool:
            return bool(self.payload_ref.inline_bytes)

        @property
        def inline_payload(self) -> bytes:
            return self.payload_ref.inline_bytes

        def with_taints(self, taints) -> "ClosedEnvelope":
            return ClosedEnvelope(
                route=self.route,
                payload_ref=self.payload_ref,
                seq_source=self.seq_source,
                producer=self.producer,
                emitter=self.emitter,
                control_epoch=self.control_epoch,
                taints=tuple(taints),
                guards=self.guards,
            )

        def close(self) -> "ClosedEnvelope":
            return self

    @dataclass(frozen=True)
    class OpenedEnvelope:
        closed: ClosedEnvelope
        payload: bytes

        def close(self) -> ClosedEnvelope:
            return self.closed

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
            return [
                OpenedEnvelope(closed=latest, payload=latest.payload_ref.inline_bytes)
            ]

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
            taints = []
            if (
                self._route.namespace.plane == Plane.Write
                and self._route.variant == Variant.Request
            ):
                taints.append(
                    TaintMark(
                        TaintDomain.Coherence,
                        "COHERENCE_WRITE_PENDING",
                        self._route.display(),
                    )
                )
            if self._route.namespace.layer == Layer.Ephemeral:
                taints.append(
                    TaintMark(
                        TaintDomain.Determinism,
                        "DET_NONREPLAYABLE",
                        self._route.display(),
                    )
                )
            if control_epoch is not None:
                taints.append(
                    TaintMark(
                        TaintDomain.Scheduling,
                        "SCHED_READY",
                        self._route.display(),
                    )
                )
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
                taints=taints,
            )
            self._graph._latest[self._route] = envelope
            self._graph._record_native_envelope(envelope)
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
    class CreditSnapshot:
        route_display: str
        credit_class: str = "default"
        available: int = 2**63 - 1
        blocked_senders: int = 0
        dropped_messages: int = 0
        largest_queue_depth: int = 0

    @dataclass
    class RetentionSnapshot:
        route_display: str
        latest_seq_source: Optional[int]
        metadata_event_count: int
        replay_count: int
        payload_count: int
        lineage_count: int
        trace_index_count: int
        causality_index_count: int
        correlation_index_count: int
        history_limit: Optional[int]

    @dataclass
    class MailboxDescriptor:
        capacity: int = 128
        delivery_mode: str = "mpsc_serial"
        ordering_policy: str = "fifo"
        overflow_policy: str = "block"

        def __post_init__(self) -> None:
            if isinstance(self.capacity, bool):
                raise TypeError("capacity must be an integer, not bool")
            if self.capacity <= 0:
                raise ValueError("capacity must be greater than zero")

    @dataclass
    class Mailbox:
        name: str
        ingress: WritablePort
        egress: ReadablePort
        descriptor: "MailboxDescriptor"
        queue: list[tuple[bytes, Optional[ProducerRef], Optional[int]]]
        blocked_writes: int = 0
        dropped_messages: int = 0
        coalesced_messages: int = 0
        delivered_messages: int = 0
        largest_queue_depth: int = 0

        def depth(self):
            return len(self.queue)

        def available_credit(self):
            return max(self.descriptor.capacity - len(self.queue), 0)

    @dataclass
    class ControlLoop:
        name: str
        read_routes: list[RouteRef]
        write_request: RouteRef
        epoch: int = 0

    @dataclass
    class NoLineageMaterializerDropProfile:
        source_route: RouteRef
        target_route: RouteRef
        materialize_generation: int

    class Graph:
        def __init__(self):
            self._latest = {}
            self._history = {}
            self._retention_limits = {}
            self._sequence = {}
            self._loops = {}
            self._edges = []
            self._materialize_targets_by_source = {}
            self._materialize_generation = 0
            self._bindings = {}
            self._catalog = {}
            self._mailboxes = {}

        def register_port(self, route):
            self._catalog[route.display()] = route
            limit = 8
            if route.namespace.layer == Layer.Ephemeral:
                limit = 0
            elif route.namespace.layer == Layer.Internal:
                limit = 1
            self._retention_limits.setdefault(
                route,
                limit,
            )
            return route

        def configure_retention(
            self,
            route,
            latest_replay_policy,
            durability_class,
            replay_window,
            payload_retention_policy,
            history_limit=None,
        ):
            del durability_class, replay_window, payload_retention_policy
            limit = history_limit
            if latest_replay_policy == "none":
                limit = 0
            elif latest_replay_policy == "latest_only":
                limit = 1
            elif latest_replay_policy == "bounded_history" and limit is None:
                limit = 8
            self._retention_limits[route] = limit
            self._trim_retention(route)

        def read(self, route):
            self.register_port(route)
            return ReadablePort(self, route)

        def writable_port(self, route):
            self.register_port(route)
            return WritablePort(self, route)

        def register_binding(self, name, binding):
            self._bindings[binding.request] = binding
            self.register_port(binding.request)
            self.register_port(binding.desired)
            self.register_port(binding.reported)
            self.register_port(binding.effective)
            if binding.ack is not None:
                self.register_port(binding.ack)
            return binding

        def emit(self, route, payload, producer=None, control_epoch=None):
            emitted = [
                self.writable_port(route).write(
                    payload, producer=producer, control_epoch=control_epoch
                )
            ]
            binding = self._bindings.get(route)
            if binding is not None:
                emitted.append(
                    self.writable_port(binding.desired).write(
                        payload,
                        producer=producer,
                        control_epoch=control_epoch,
                    )
                )
            fanout = []
            for envelope in tuple(emitted):
                fanout.extend(self._fanout(envelope))
            emitted.extend(fanout)
            return emitted

        def emit_single_if_unrouted(
            self, route, payload, producer=None, control_epoch=None
        ):
            if self._bindings.get(route) is not None:
                return None
            if any(source == route.display() for source, _ in self._edges):
                return None
            if any(
                mailbox.ingress._route == route for mailbox in self._mailboxes.values()
            ):
                return None
            return self.writable_port(route).write(
                payload,
                producer=producer,
                control_epoch=control_epoch,
            )

        def emit_single_if_unrouted_drop(
            self, route, payload, producer=None, control_epoch=None
        ):
            return (
                self.emit_single_if_unrouted(
                    route,
                    payload,
                    producer=producer,
                    control_epoch=control_epoch,
                )
                is not None
            )

        def emit_single_if_unrouted_and_materializer_drop(
            self,
            route,
            target_route,
            payload,
            producer=None,
            control_epoch=None,
        ):
            if target_route not in self._materialize_targets_by_source.get(route, ()):
                return False
            envelope = self.emit_single_if_unrouted(
                route,
                payload,
                producer=producer,
                control_epoch=control_epoch,
            )
            if envelope is None:
                return False
            self._materialize_bytes_from_source(envelope, target_route)
            return True

        def emit_single_if_unrouted_and_materializer_drop_python(
            self,
            route,
            target_route,
            payload,
        ):
            return self.emit_single_if_unrouted_and_materializer_drop(
                route,
                target_route,
                payload,
            )

        def compile_no_lineage_materializer_drop_profile(
            self,
            route,
            target_route,
        ):
            if target_route not in self._materialize_targets_by_source.get(route, ()):
                raise RuntimeError("materializer profile route pair is not registered")
            return NoLineageMaterializerDropProfile(
                source_route=route,
                target_route=target_route,
                materialize_generation=self._materialize_generation,
            )

        def release_no_lineage_materializer_drop_profile(self, profile):
            del profile

        def emit_no_lineage_materializer_drop_profile_python(self, profile, payload):
            if profile.materialize_generation != self._materialize_generation:
                return self.emit_single_if_unrouted_and_materializer_drop_python(
                    profile.source_route,
                    profile.target_route,
                    payload,
                )
            envelope = self.emit_single_if_unrouted(
                profile.source_route,
                payload,
            )
            if envelope is None:
                return False
            self._materialize_bytes_from_source(envelope, profile.target_route)
            return True

        def emit_single_if_unrouted_with_lineage_no_parents(
            self,
            route,
            payload,
            producer=None,
            control_epoch=None,
            trace_id=None,
            causality_id=None,
            correlation_id=None,
        ):
            envelope = self.emit_single_if_unrouted(
                route,
                payload,
                producer=producer,
                control_epoch=control_epoch,
            )
            if envelope is None:
                return None
            del trace_id, causality_id, correlation_id
            return envelope

        def emit_single_if_unrouted_with_lineage_no_parents_and_materializers(
            self,
            route,
            payload,
            producer=None,
            control_epoch=None,
            trace_id=None,
            causality_id=None,
            correlation_id=None,
        ):
            envelope = self.emit_single_if_unrouted_with_lineage_no_parents(
                route,
                payload,
                producer=producer,
                control_epoch=control_epoch,
                trace_id=trace_id,
                causality_id=causality_id,
                correlation_id=correlation_id,
            )
            if envelope is None:
                return None
            emitted = [envelope]
            for target in tuple(self._materialize_targets_by_source.get(route, ())):
                emitted.append(self._materialize_bytes_from_source(envelope, target))
            return emitted

        def emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop(
            self,
            route,
            payload,
            producer=None,
            control_epoch=None,
            trace_id=None,
            causality_id=None,
            correlation_id=None,
        ):
            emitted = (
                self.emit_single_if_unrouted_with_lineage_no_parents_and_materializers(
                    route,
                    payload,
                    producer=producer,
                    control_epoch=control_epoch,
                    trace_id=trace_id,
                    causality_id=causality_id,
                    correlation_id=correlation_id,
                )
            )
            return emitted is not None

        def materialize_bytes_one_parent(
            self,
            source_route,
            source_seq_source,
            target_route,
            producer=None,
        ):
            source = self._event_envelope(source_route, source_seq_source)
            if source is None:
                return None
            return self._materialize_bytes_from_source(source, target_route, producer)

        def register_materialize_bytes(self, source_route, target_route):
            self.register_port(source_route)
            self.register_port(target_route)
            targets = self._materialize_targets_by_source.setdefault(source_route, [])
            if target_route in targets:
                return False
            targets.append(target_route)
            self._materialize_generation += 1
            return True

        def unregister_materialize_bytes(self, source_route, target_route):
            targets = self._materialize_targets_by_source.get(source_route)
            if targets is None or target_route not in targets:
                return False
            targets.remove(target_route)
            if not targets:
                self._materialize_targets_by_source.pop(source_route, None)
            self._materialize_generation += 1
            return True

        def mailbox(self, name, descriptor=None):
            descriptor = descriptor or MailboxDescriptor()
            ingress_route = RouteRef(
                namespace=NamespaceRef(
                    plane=Plane.Write, layer=Layer.Internal, owner=name
                ),
                family="mailbox",
                stream=name,
                variant=Variant.Request,
                schema=SchemaRef("MailboxIngress", 1),
            )
            egress_route = RouteRef(
                namespace=NamespaceRef(
                    plane=Plane.Read, layer=Layer.Internal, owner=name
                ),
                family="mailbox",
                stream=name,
                variant=Variant.Meta,
                schema=SchemaRef("MailboxEgress", 1),
            )
            self.register_port(ingress_route)
            self.register_port(egress_route)
            mailbox = Mailbox(
                name=name,
                ingress=WritablePort(self, ingress_route),
                egress=ReadablePort(self, egress_route),
                descriptor=descriptor,
                queue=[],
            )
            self._mailboxes[name] = mailbox
            return mailbox

        def connect(self, source, sink):
            if hasattr(source, "egress"):
                source = source.egress._route
            if hasattr(sink, "ingress"):
                sink = sink.ingress._route
            self.register_port(source)
            self.register_port(sink)
            edge = (source.display(), sink.display())
            if edge in self._edges:
                return False
            self._edges.append(edge)
            return True

        def disconnect(self, source, sink):
            if hasattr(source, "egress"):
                source = source.egress._route
            if hasattr(sink, "ingress"):
                sink = sink.ingress._route
            edge = (source.display(), sink.display())
            if edge not in self._edges:
                return False
            self._edges.remove(edge)
            return True

        def install(self, control_loop):
            if control_loop.name in self._loops:
                raise ValueError(
                    f"control loop {control_loop.name!r} is already installed"
                )
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
            return sorted(self._catalog.values(), key=lambda route: route.display())

        def describe_route(self, route):
            return PortDescriptor(route_display=route.display())

        def latest(self, route):
            return self._latest.get(route)

        def replay(self, route):
            return list(self._history.get(route, ()))

        def retained_payload_count(self, route):
            return len(self._history.get(route, ()))

        def payload_by_id(self, payload_id):
            for envelope in self._latest.values():
                if envelope.payload_id == payload_id:
                    return bytes(envelope.inline_payload)
            for history in self._history.values():
                for envelope in history:
                    if envelope.payload_id == payload_id:
                        return bytes(envelope.inline_payload)
            return None

        def retention_snapshot(self, route=None):
            routes = [route] if route is not None else self.catalog()
            snapshots = []
            for route_ref in routes:
                route_display = route_ref.display()
                latest_seq_source = getattr(
                    self._latest.get(route_ref), "seq_source", None
                )
                snapshots.append(
                    RetentionSnapshot(
                        route_display=route_display,
                        latest_seq_source=latest_seq_source,
                        metadata_event_count=latest_seq_source or 0,
                        replay_count=len(self._history.get(route_ref, ())),
                        payload_count=self.retained_payload_count(route_ref),
                        lineage_count=0,
                        trace_index_count=0,
                        causality_index_count=0,
                        correlation_index_count=0,
                        history_limit=self._retention_limits.get(route_ref),
                    )
                )
            return snapshots

        def retention_violations(self):
            violations = []
            retained = set()
            for route, history in self._history.items():
                route_display = route.display()
                retained.update(
                    (route_display, envelope.seq_source) for envelope in history
                )
                latest = self._latest.get(route)
                if latest is not None:
                    retained.add((route_display, latest.seq_source))
            return sorted(violations)

        def topology(self):
            return sorted(self._edges)

        def validate_graph(self):
            return []

        def credit_snapshot(self):
            snapshots = []
            for route in self.catalog():
                mailbox = None
                for candidate in self._mailboxes.values():
                    if route in (candidate.ingress._route, candidate.egress._route):
                        mailbox = candidate
                        break
                if mailbox is None:
                    snapshots.append(CreditSnapshot(route_display=route.display()))
                else:
                    snapshots.append(
                        CreditSnapshot(
                            route_display=route.display(),
                            available=mailbox.available_credit(),
                            blocked_senders=mailbox.blocked_writes,
                            dropped_messages=mailbox.dropped_messages,
                            largest_queue_depth=mailbox.largest_queue_depth,
                        )
                    )
            return sorted(snapshots, key=lambda snapshot: snapshot.route_display)

        def _record_native_envelope(self, envelope):
            history = self._history.setdefault(envelope.route, [])
            history.append(envelope)
            self._trim_retention(envelope.route)

        def _trim_retention(self, route):
            limit = self._retention_limits.get(route)
            if limit is None:
                return
            history = self._history.get(route)
            if history is not None and len(history) > limit:
                del history[: len(history) - limit]

        def _event_envelope(self, route, seq_source):
            for envelope in self._history.get(route, ()):
                if envelope.seq_source == seq_source:
                    return envelope
            latest = self._latest.get(route)
            if latest is not None and latest.seq_source == seq_source:
                return latest
            return None

        def _materialize_bytes_from_source(self, source, target, producer=None):
            envelope = self.writable_port(target).write(
                source.payload_ref.inline_bytes,
                producer=producer,
                control_epoch=source.control_epoch,
            )
            return envelope

        def _fanout(self, envelope):
            emitted = []
            for source_display, sink_display in self._edges:
                if source_display != envelope.route.display():
                    continue
                sink = self._catalog[sink_display]
                mailbox = self._mailbox_for_ingress(sink)
                if mailbox is not None:
                    emitted.extend(
                        self._enqueue_mailbox(
                            mailbox,
                            envelope.payload_ref.inline_bytes,
                            envelope.producer,
                            envelope.control_epoch,
                        )
                    )
                    continue
                forwarded = self.writable_port(sink).write(
                    envelope.payload_ref.inline_bytes,
                    producer=envelope.producer,
                    control_epoch=envelope.control_epoch,
                )
                emitted.append(forwarded)
                emitted.extend(self._fanout(forwarded))
            return emitted

        def _mailbox_for_ingress(self, route):
            for mailbox in self._mailboxes.values():
                if mailbox.ingress._route == route:
                    return mailbox
            return None

        def _enqueue_mailbox(self, mailbox, payload, producer, control_epoch):
            emitted = []
            if len(mailbox.queue) >= mailbox.descriptor.capacity:
                if mailbox.descriptor.overflow_policy in {"block", "reject_write"}:
                    mailbox.blocked_writes += 1
                    return emitted
                if mailbox.descriptor.overflow_policy in {
                    "drop_newest",
                    "deadline_drop",
                    "spill_to_store",
                }:
                    mailbox.dropped_messages += 1
                    return emitted
                if mailbox.descriptor.overflow_policy == "drop_oldest":
                    mailbox.queue.pop(0)
                    mailbox.dropped_messages += 1
                elif (
                    mailbox.descriptor.overflow_policy == "coalesce_latest"
                    and mailbox.queue
                ):
                    mailbox.queue[-1] = (bytes(payload), producer, control_epoch)
                    mailbox.coalesced_messages += 1
                    return self._drain_mailbox(mailbox)
            mailbox.queue.append((bytes(payload), producer, control_epoch))
            mailbox.largest_queue_depth = max(
                mailbox.largest_queue_depth, len(mailbox.queue)
            )
            return self._drain_mailbox(mailbox)

        def _drain_mailbox(self, mailbox):
            if not any(
                source == mailbox.egress._route.display() for source, _ in self._edges
            ):
                return []
            emitted = []
            while mailbox.queue:
                payload, producer, control_epoch = mailbox.queue.pop(0)
                mailbox.delivered_messages += 1
                egress_envelope = self.writable_port(mailbox.egress._route).write(
                    payload,
                    producer=producer,
                    control_epoch=control_epoch,
                )
                emitted.append(egress_envelope)
                emitted.extend(self._fanout(egress_envelope))
            return emitted

    @dataclass(frozen=True)
    class ArchitecturePad:
        name: str
        topic: str
        direction: str = "internal"

    @dataclass(frozen=True)
    class ArchitectureRelay:
        name: str
        source: str
        target: str

    @dataclass(frozen=True)
    class ArchitectureVia:
        name: str
        source: str
        target: str
        boundary: str

    @dataclass(frozen=True)
    class ArchitectureGround:
        name: str
        reason: str

    @dataclass(frozen=True)
    class ArchitectureProbe:
        name: str
        target: str

    @dataclass(frozen=True)
    class ArchitectureRegulator:
        name: str
        policy: str
        limit: int | None = None

    @dataclass(frozen=True)
    class ArchitectureCapacitor:
        name: str
        source: str
        target: str
        capacity: int = 1
        location: str | None = None

    @dataclass(frozen=True)
    class ArchitectureResistor:
        name: str
        boundary: str
        policy: str
        limit: int | None = None

    @dataclass(frozen=True)
    class ClockCalibrationSample:
        observed_ns: int
        reference_ns: int
        temperature_c: float | None = None

    @dataclass(frozen=True)
    class FlatBufferField:
        name: str
        index: int
        field_type: str

    @dataclass(frozen=True)
    class FlatBufferTable:
        name: str
        fields: list[FlatBufferField]

    class SystemTimeProvider:
        def now_ns(self):
            return time.time_ns()

    @dataclass(frozen=True)
    class NtpTimeProvider:
        server: str
        port: int = 123
        timeout_ms: int = 1000

        def now_ns(self):
            return time.time_ns()

    class MonotonicLogicalClock:
        def __init__(self):
            self._value = 0

        def tick(self):
            self._value += 1
            return self._value

    class Clock(MonotonicLogicalClock):
        pass

    @dataclass(frozen=True)
    class CalibratedClock:
        samples: list[ClockCalibrationSample]

        def now_ns(self):
            if not self.samples:
                raise ValueError("calibrated clock requires observations")
            sample = self.samples[0]
            return time.time_ns() + sample.reference_ns - sample.observed_ns

    @dataclass(frozen=True)
    class PubSubMessage:
        topic: str
        payload: bytes
        offset: int = 0

    @dataclass(frozen=True)
    class PubSubSubscription:
        name: str
        topic: str

    @dataclass(frozen=True)
    class PubSubDelivery:
        topic: str
        offset: int
        delivered_to: list[str]

        @property
        def subscriber_count(self):
            return len(self.delivered_to)

    class InMemoryPubSub:
        def __init__(self, *, retained_messages=1024):
            self.retained_messages = retained_messages
            self._messages = []
            self._subscriptions = {}
            self._positions = {}

        @property
        def message_count(self):
            return len(self._messages)

        @property
        def subscriber_count(self):
            return len(self._subscriptions)

        def subscribe(self, topic, *, name=None, replay_from_beginning=False):
            subscription_name = name or topic
            self._subscriptions[subscription_name] = topic
            self._positions[subscription_name] = (
                0 if replay_from_beginning else len(self._messages)
            )
            return PubSubSubscription(subscription_name, topic)

        def publish(self, topic, payload):
            offset = len(self._messages)
            message = PubSubMessage(topic, bytes(payload), offset)
            self._messages.append(message)
            delivered_to = [
                name
                for name, subscribed_topic in self._subscriptions.items()
                if subscribed_topic in {topic, "*"}
            ]
            return PubSubDelivery(topic, offset, delivered_to)

        def poll(self, subscription, *, max_messages=None):
            topic = self._subscriptions[subscription]
            start = self._positions[subscription]
            messages = [
                message
                for message in self._messages[start:]
                if topic in {message.topic, "*"}
            ]
            if max_messages is not None:
                messages = messages[:max_messages]
            self._positions[subscription] = len(self._messages)
            return messages

        def latest(self, topic=None):
            for message in reversed(self._messages):
                if topic is None or message.topic == topic:
                    return message
            return None

        def topic_offsets(self):
            return {message.topic: message.offset for message in self._messages}

    @dataclass(frozen=True)
    class DataStreamRecord:
        pad_name: str | None
        topic: str
        payload: bytes
        offset: int
        process_sequence: int
        event_time: int
        key: str | None = None

        @property
        def seq_source(self):
            return self.offset + 1

    class DataStreamProcessor:
        def __init__(self):
            self._records = []
            self._process_sequence = 0

        def ingest(self, message, *, event_time=None, key=None):
            self._process_sequence += 1
            record = DataStreamRecord(
                None,
                message.topic,
                message.payload,
                message.offset,
                self._process_sequence,
                self._process_sequence if event_time is None else event_time,
                key,
            )
            self._records.append(record)
            return record

        def ingest_many(self, messages):
            return [self.ingest(message) for message in messages]

        def query(self, sql, parameters=None):
            latest = self.latest(
                (parameters or {}).get("topic", "sensor.environment.temperature")
            )
            if latest is None:
                return []
            temperature_f = None
            unit = None
            if latest.payload:
                try:
                    temperature_f, unit = _decode_temperature_flatbuffer(latest.payload)
                except (IndexError, UnicodeDecodeError, ValueError):
                    pass
            return [
                {
                    "pad_name": latest.pad_name,
                    "seq_source": latest.seq_source,
                    "temperature_f": temperature_f,
                    "unit": unit,
                    "payload": latest.payload,
                }
            ]

        def query_one(self, sql, parameters=None):
            rows = self.query(sql, parameters)
            return rows[0] if rows else None

        def latest(self, topic):
            for record in reversed(self._records):
                if record.topic == topic:
                    return record
            return None

    class PubSubRuntime:
        def __init__(self, *, name="pubsub", retained_messages=1024):
            self.pubsub = InMemoryPubSub(retained_messages=retained_messages)
            self.subscription = self.pubsub.subscribe(
                "*", name=f"{name}.stream_processor"
            )
            self.processor = DataStreamProcessor()
            self.metadata = {}
            self.records = []

        def register_flatbuffer_pad(self, pad_name, table):
            return None

        def publish(self, topic, payload, *, pad_name=None, event_time=None, key=None):
            delivery = self.pubsub.publish(topic, payload)
            self.metadata[(topic, delivery.offset)] = (event_time, key, pad_name)
            return delivery

        def drain(self):
            records = []
            for message in self.pubsub.poll(self.subscription.name):
                event_time, key, pad_name = self.metadata.pop(
                    (message.topic, message.offset),
                    (None, None, None),
                )
                record = self.processor.ingest(message, event_time=event_time, key=key)
                board_record = DataStreamRecord(
                    pad_name,
                    record.topic,
                    record.payload,
                    record.offset,
                    record.process_sequence,
                    record.event_time,
                    record.key,
                )
                self.records.append(board_record)
                records.append(board_record)
            return records

        def latest(self, topic):
            self.drain()
            for record in reversed(self.records):
                if record.topic == topic:
                    return record
            return None

        def query(self, sql, parameters=None):
            self.drain()
            topic = (parameters or {}).get("topic", "sensor.environment.temperature")
            for record in reversed(self.records):
                if record.topic == topic:
                    temperature_f = None
                    unit = None
                    if record.payload:
                        try:
                            temperature_f, unit = _decode_temperature_flatbuffer(
                                record.payload
                            )
                        except (IndexError, UnicodeDecodeError, ValueError):
                            pass
                    return [
                        {
                            "pad_name": record.pad_name,
                            "seq_source": record.seq_source,
                            "temperature_f": temperature_f,
                            "unit": unit,
                            "payload": record.payload,
                        }
                    ]
            return []

        def query_one(self, sql, parameters=None):
            rows = self.query(sql, parameters)
            return rows[0] if rows else None

    rust_module.PubSubRuntime = PubSubRuntime
    rust_module.ArchitectureCapacitor = ArchitectureCapacitor
    rust_module.ArchitectureGround = ArchitectureGround
    rust_module.ArchitecturePad = ArchitecturePad
    rust_module.ArchitectureProbe = ArchitectureProbe
    rust_module.ArchitectureRegulator = ArchitectureRegulator
    rust_module.ArchitectureRelay = ArchitectureRelay
    rust_module.ArchitectureResistor = ArchitectureResistor
    rust_module.ArchitectureVia = ArchitectureVia
    rust_module.CalibratedClock = CalibratedClock
    rust_module.Clock = Clock
    rust_module.ClockCalibrationSample = ClockCalibrationSample
    rust_module.ClockDomainRef = ClockDomainRef
    rust_module.ClosedEnvelope = ClosedEnvelope
    rust_module.ControlLoop = ControlLoop
    rust_module.CreditSnapshot = CreditSnapshot
    rust_module.DataStreamProcessor = DataStreamProcessor
    rust_module.DataStreamRecord = DataStreamRecord
    rust_module.FlatBufferField = FlatBufferField
    rust_module.FlatBufferTable = FlatBufferTable
    rust_module.Graph = Graph
    rust_module.InMemoryPubSub = InMemoryPubSub
    rust_module.Layer = Layer
    rust_module.Mailbox = Mailbox
    rust_module.MailboxDescriptor = MailboxDescriptor
    rust_module.ManyFoldLock = ManyFoldLock
    rust_module.ManyFoldLockLease = ManyFoldLockLease
    rust_module.NamespaceRef = NamespaceRef
    rust_module.NoLineageMaterializerDropProfile = NoLineageMaterializerDropProfile
    rust_module.MonotonicLogicalClock = MonotonicLogicalClock
    rust_module.NtpTimeProvider = NtpTimeProvider
    rust_module.OpenedEnvelope = OpenedEnvelope
    rust_module.PayloadRef = PayloadRef
    rust_module.Plane = Plane
    rust_module.PortDescriptor = PortDescriptor
    rust_module.ProducerKind = ProducerKind
    rust_module.ProducerRef = ProducerRef
    rust_module.PubSubDelivery = PubSubDelivery
    rust_module.PubSubMessage = PubSubMessage
    rust_module.PubSubSubscription = PubSubSubscription
    rust_module.ReadablePort = ReadablePort
    rust_module.RetentionSnapshot = RetentionSnapshot
    rust_module.RouteRef = RouteRef
    rust_module.RuntimeRef = RuntimeRef
    rust_module.ScheduleGuard = ScheduleGuard
    rust_module.SchemaRef = SchemaRef
    rust_module.SystemTimeProvider = SystemTimeProvider
    rust_module.TaintDomain = TaintDomain
    rust_module.TaintMark = TaintMark
    rust_module.Variant = Variant
    rust_module.WritablePort = WritablePort
    rust_module.WriteBinding = WriteBinding
    rust_module.bridge_version = lambda: "stub"
    rust_module.parse_sql_statement = parse_sql_statement
    sys.modules["manyfold._manyfold_rust"] = rust_module


def reset_test_modules() -> None:
    for module_name in MODULES_TO_RESET:
        sys.modules.pop(module_name, None)
    for module_name in tuple(sys.modules):
        if module_name.startswith("examples."):
            sys.modules.pop(module_name, None)


def load_manyfold_package():
    reset_test_modules()
    install_manyfold_rust_stub()
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    package = types.ModuleType("manyfold")
    package.__path__ = [str(PACKAGE_DIR)]
    sys.modules["manyfold"] = package

    primitives = _load_module("manyfold.primitives", PACKAGE_DIR / "primitives.py")
    graph = _load_module("manyfold.graph", PACKAGE_DIR / "graph.py")
    embedded = _load_module("manyfold.embedded", PACKAGE_DIR / "embedded.py")
    components = _load_module("manyfold.components", PACKAGE_DIR / "components.py")
    lego_catalog = _load_module(
        "manyfold.lego_catalog", PACKAGE_DIR / "lego_catalog.py"
    )
    sensor_io = _load_module("manyfold.sensor_io", PACKAGE_DIR / "sensor_io.py")
    stats = sys.modules["manyfold.stats"]
    rust = sys.modules["manyfold._manyfold_rust"]

    exports = {
        "all_legos": lego_catalog.all_legos,
        "Average": stats.Average,
        "BackoffPolicy": sensor_io.BackoffPolicy,
        "BoundedRingBuffer": sensor_io.BoundedRingBuffer,
        "ChangeFilter": sensor_io.ChangeFilter,
        "Clock": sensor_io.Clock,
        "DetectionNode": sensor_io.DetectionNode,
        "DetectionNodeHandle": sensor_io.DetectionNodeHandle,
        "DiagramNode": graph.DiagramNode,
        "ClockDomainRef": rust.ClockDomainRef,
        "ClosedEnvelope": rust.ClosedEnvelope,
        "Consensus": components.Consensus,
        "ConsensusRoutes": components.ConsensusRoutes,
        "ControlLoop": rust.ControlLoop,
        "ControlLoops": graph.ControlLoops,
        "CorrelationTracingStore": graph.CorrelationTracingStore,
        "CreditSnapshot": rust.CreditSnapshot,
        "RetentionSnapshot": rust.RetentionSnapshot,
        "EmbeddedBulkSensor": embedded.EmbeddedBulkSensor,
        "EmbeddedDeviceProfile": embedded.EmbeddedDeviceProfile,
        "EmbeddedRuntimeRules": embedded.EmbeddedRuntimeRules,
        "EmbeddedScalarSensor": embedded.EmbeddedScalarSensor,
        "EventLog": components.EventLog,
        "EventLogRecord": components.EventLogRecord,
        "EventLogRoutes": components.EventLogRoutes,
        "FileStore": components.FileStore,
        "FirmwareAgentProfile": embedded.FirmwareAgentProfile,
        "Capacitor": graph.Capacitor,
        "CallbackObservable": graph.CallbackObservable,
        "CallbackNode": graph.CallbackNode,
        "CallbackSubscription": graph.CallbackSubscription,
        "CoalesceLatestNode": graph.CoalesceLatestNode,
        "CombineLatestNode": graph.CombineLatestNode,
        "CompositeSubscription": graph.CompositeSubscription,
        "ConstantNode": graph.ConstantNode,
        "Contract": primitives.Contract,
        "DelimitedMessageBuffer": sensor_io.DelimitedMessageBuffer,
        "DoubleBuffer": sensor_io.DoubleBuffer,
        "DuplexSensorPeripheral": sensor_io.DuplexSensorPeripheral,
        "EmptyNode": graph.EmptyNode,
        "EventStream": graph.EventStream,
        "FilterNode": graph.FilterNode,
        "FlowPolicy": graph.FlowPolicy,
        "FlowSnapshot": graph.FlowSnapshot,
        "FrameAssembler": sensor_io.FrameAssembler,
        "Graph": graph.Graph,
        "GraphConnection": graph.GraphConnection,
        "GraphContext": graph.GraphContext,
        "GraphManifest": graph.GraphManifest,
        "GraphAccessNode": sensor_io.GraphAccessNode,
        "HealthStatus": sensor_io.HealthStatus,
        "Interval": graph.Interval,
        "IntervalNode": graph.IntervalNode,
        "JoinInput": graph.JoinInput,
        "JsonEventDecoder": sensor_io.JsonEventDecoder,
        "Keyspace": components.Keyspace,
        "Layer": rust.Layer,
        "LazyPayloadSource": graph.LazyPayloadSource,
        "LifecycleBinding": graph.LifecycleBinding,
        "LineageRecord": graph.LineageRecord,
        "LoggingNode": graph.LoggingNode,
        "LocalDurableSpool": sensor_io.LocalDurableSpool,
        "LocalSensorSource": sensor_io.LocalSensorSource,
        "Lego": lego_catalog.Lego,
        "Mailbox": rust.Mailbox,
        "MailboxDescriptor": rust.MailboxDescriptor,
        "MailboxSnapshot": graph.MailboxSnapshot,
        "MainThreadNode": graph.MainThreadNode,
        "ManifestDebugRoute": graph.ManifestDebugRoute,
        "ManifestDiagramNode": graph.ManifestDiagramNode,
        "ManifestEdge": graph.ManifestEdge,
        "ManifestLink": graph.ManifestLink,
        "ManifestMeshPrimitive": graph.ManifestMeshPrimitive,
        "ManifestQueryService": graph.ManifestQueryService,
        "ManifestRoute": graph.ManifestRoute,
        "ManifestWriteBinding": graph.ManifestWriteBinding,
        "MapNode": graph.MapNode,
        "ManualClock": sensor_io.ManualClock,
        "ManagedGraphNode": sensor_io.ManagedGraphNode,
        "ManagedGraphNodeHandle": sensor_io.ManagedGraphNodeHandle,
        "ManagedRunLoop": sensor_io.ManagedRunLoop,
        "ManagedRunLoopHandle": sensor_io.ManagedRunLoopHandle,
        "Memory": components.Memory,
        "MemoryRecord": components.MemoryRecord,
        "MergeNode": graph.MergeNode,
        "NamespaceRef": rust.NamespaceRef,
        "NativeCorrelationTracingStore": graph.NativeCorrelationTracingStore,
        "NodeThreadPlacement": graph.NodeThreadPlacement,
        "NoopCorrelationTracingStore": graph.NoopCorrelationTracingStore,
        "NoopSubscription": graph.NoopSubscription,
        "OpenedEnvelope": rust.OpenedEnvelope,
        "OwnerName": primitives.OwnerName,
        "PayloadDemandSnapshot": graph.PayloadDemandSnapshot,
        "PayloadRef": rust.PayloadRef,
        "PeripheralAdapter": sensor_io.PeripheralAdapter,
        "PeripheralAdapterHandle": sensor_io.PeripheralAdapterHandle,
        "Plane": rust.Plane,
        "PortDescriptor": rust.PortDescriptor,
        "ProcessEndpoint": graph.ProcessEndpoint,
        "ProcessRpcAuditEntry": graph.ProcessRpcAuditEntry,
        "ProcessRpcFailure": graph.ProcessRpcFailure,
        "ProcessRpcRequest": graph.ProcessRpcRequest,
        "ProcessRpcResponse": graph.ProcessRpcResponse,
        "ProcessRpcRoutes": graph.ProcessRpcRoutes,
        "ProcessRpcTransport": graph.ProcessRpcTransport,
        "ProducerKind": rust.ProducerKind,
        "ProducerRef": rust.ProducerRef,
        "ReadablePort": rust.ReadablePort,
        "ReadThenWriteNextEpochStep": primitives.ReadThenWriteNextEpochStep,
        "RateMatchedSensor": sensor_io.RateMatchedSensor,
        "ReactiveSensorHandle": sensor_io.ReactiveSensorHandle,
        "ReactiveSensorSource": sensor_io.ReactiveSensorSource,
        "Resistor": graph.Resistor,
        "RetryLoop": sensor_io.RetryLoop,
        "RetryPolicy": graph.RetryPolicy,
        "RouteIdentity": primitives.RouteIdentity,
        "RouteNamespace": primitives.RouteNamespace,
        "RouteScope": primitives.RouteScope,
        "RouteRef": rust.RouteRef,
        "RouteAuditSnapshot": graph.RouteAuditSnapshot,
        "RoutePipeline": graph.RoutePipeline,
        "RouteRetentionPolicy": graph.RouteRetentionPolicy,
        "RuntimeRef": rust.RuntimeRef,
        "ScheduledWriteSnapshot": graph.ScheduledWriteSnapshot,
        "ScheduleGuard": rust.ScheduleGuard,
        "Schema": primitives.Schema,
        "SchemaRef": rust.SchemaRef,
        "SequenceCounter": sensor_io.SequenceCounter,
        "SensorBackoffPolicy": sensor_io.BackoffPolicy,
        "SensorDebugEnvelope": sensor_io.SensorDebugEnvelope,
        "SensorDebugStage": sensor_io.SensorDebugStage,
        "SensorDebugTap": sensor_io.SensorDebugTap,
        "SensorEvent": sensor_io.SensorEvent,
        "SensorFrame": sensor_io.SensorFrame,
        "SensorHealthHandle": sensor_io.SensorHealthHandle,
        "SensorHealthWatchdog": sensor_io.SensorHealthWatchdog,
        "SensorIdentity": sensor_io.SensorIdentity,
        "SensorLocation": sensor_io.SensorLocation,
        "SensorRetryPolicy": sensor_io.RetryPolicy,
        "SensorSample": sensor_io.SensorSample,
        "SensorSourceHandle": sensor_io.SensorSourceHandle,
        "SensorTag": sensor_io.SensorTag,
        "ShadowSnapshot": graph.ShadowSnapshot,
        "Sink": primitives.Sink,
        "SnapshotStore": components.SnapshotStore,
        "SnapshotStoreRoutes": components.SnapshotStoreRoutes,
        "Source": primitives.Source,
        "StopToken": sensor_io.StopToken,
        "StoreEntry": components.StoreEntry,
        "StreamNode": graph.StreamNode,
        "StreamFamily": primitives.StreamFamily,
        "StreamName": primitives.StreamName,
        "TaintDomain": rust.TaintDomain,
        "TaintMark": rust.TaintMark,
        "TaintRepair": graph.TaintRepair,
        "TypedEnvelope": primitives.TypedEnvelope,
        "TypedRoute": primitives.TypedRoute,
        "Variant": rust.Variant,
        "WatermarkSnapshot": graph.WatermarkSnapshot,
        "Watchdog": graph.Watchdog,
        "WritablePort": rust.WritablePort,
        "WriteBinding": rust.WriteBinding,
        "WriteBindings": graph.WriteBindings,
        "bridge_version": rust.bridge_version,
        "parse_sql_statement": rust.parse_sql_statement,
        "dependency_closure_of": lego_catalog.dependency_closure_of,
        "dependencies_of": lego_catalog.dependencies_of,
        "dependents_of": lego_catalog.dependents_of,
        "drain_main_thread_queue": sys.modules[
            "manyfold.datastream_threads"
        ].drain_main_thread_queue,
        "get_lego": lego_catalog.get_lego,
        "health_status_schema": sensor_io.health_status_schema,
        "instrument_stream": graph.instrument_stream,
        "logical_routes": primitives.logical_routes,
        "route": primitives.route,
        "legos_by_layer": lego_catalog.legos_by_layer,
        "legos_by_role": lego_catalog.legos_by_role,
        "sensor_event_schema": sensor_io.sensor_event_schema,
        "sensor_sample_schema": sensor_io.sensor_sample_schema,
        "shutdown_signal": sys.modules["manyfold.datastream_threads"].shutdown_signal,
        "sink": primitives.sink,
        "source": primitives.source,
        "SystemClock": sensor_io.SystemClock,
        "ThresholdFilter": sensor_io.ThresholdFilter,
        "Timer": graph.Timer,
        "xor_checksum": sensor_io.xor_checksum,
    }
    for name, value in exports.items():
        setattr(package, name, value)

    reference_examples = _load_module(
        "manyfold.reference_examples", PACKAGE_DIR / "reference_examples.py"
    )
    package.REFERENCE_EXAMPLE_SUITE = reference_examples.REFERENCE_EXAMPLE_SUITE
    package.ReferenceExample = reference_examples.ReferenceExample
    package.__all__ = tuple(
        sorted(
            (
                *exports.keys(),
                "REFERENCE_EXAMPLE_SUITE",
                "ReferenceExample",
                "implemented_reference_examples",
                "reference_example_suite",
            )
        )
    )
    package.implemented_reference_examples = (
        reference_examples.implemented_reference_examples
    )
    package.reference_example_suite = reference_examples.reference_example_suite
    return package


def load_example_module(name: str):
    load_manyfold_package()
    full_name = f"examples.{name}"
    if full_name in sys.modules:
        del sys.modules[full_name]
    return importlib.import_module(full_name)


def load_manyfold_graph_module():
    load_manyfold_package()
    return sys.modules["manyfold.graph"]


def _decode_temperature_flatbuffer(payload: bytes) -> tuple[float, str]:
    root_offset = struct.unpack_from("<I", payload, 0)[0]
    temperature = struct.unpack_from("<f", payload, root_offset + 4)[0]
    string_offset_position = root_offset + 8
    string_position = (
        string_offset_position
        + struct.unpack_from(
            "<I",
            payload,
            string_offset_position,
        )[0]
    )
    unit_length = struct.unpack_from("<I", payload, string_position)[0]
    unit_start = string_position + 4
    unit = payload[unit_start : unit_start + unit_length].decode("utf-8")
    return temperature, unit


def _module_available(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except (ImportError, ValueError):
        return module_name in sys.modules


def _pythonpath_with_repo_python_first(current_pythonpath: str | None) -> str:
    python_root = str(PYTHON_ROOT)
    if not current_pythonpath:
        return python_root
    resolved_python_root = PYTHON_ROOT.resolve()
    existing_paths = _unique_nonempty_paths(
        path
        for path in current_pythonpath.split(os.pathsep)
        if _resolve_pythonpath_entry(path) != resolved_python_root
    )
    return os.pathsep.join((python_root, *existing_paths))


def _resolve_pythonpath_entry(path: str) -> Path:
    entry = Path(path)
    if not entry.is_absolute():
        entry = REPO_ROOT / entry
    return entry.resolve()


def _unique_nonempty_paths(paths: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    unique_paths: list[str] = []
    for path in paths:
        if not path or path in seen:
            continue
        seen.add(path)
        unique_paths.append(path)
    return tuple(unique_paths)


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError(f"could not load module spec for {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    previous_module = sys.modules.get(name, _MISSING_MODULE)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        if previous_module is _MISSING_MODULE:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous_module
        raise
    return module
