"""High-level Python helpers matching the RFC examples."""

from __future__ import annotations

from dataclasses import dataclass

from ._manyfold_rust import ControlLoop as NativeControlLoop
from ._manyfold_rust import Graph as NativeGraph
from ._manyfold_rust import NamespaceRef
from ._manyfold_rust import Plane
from ._manyfold_rust import RouteRef
from ._manyfold_rust import SchemaRef
from ._manyfold_rust import Variant
from ._manyfold_rust import WriteBinding
from ._manyfold_rust import Layer


def route(
    *,
    plane: Plane,
    layer: Layer,
    owner: str,
    family: str,
    stream: str,
    variant: Variant,
    schema_id: str,
    version: int = 1,
) -> RouteRef:
    return RouteRef(
        NamespaceRef(plane=plane, layer=layer, owner=owner),
        family=family,
        stream=stream,
        variant=variant,
        schema=SchemaRef(schema_id=schema_id, version=version),
    )


@dataclass(frozen=True)
class WriteBindings:
    """Factories for common shadow-route write binding layouts."""

    @staticmethod
    def logical(owner: str, family: str, stream: str, schema_id: str, version: int = 1) -> WriteBinding:
        request = route(
            plane=Plane.Write,
            layer=Layer.Logical,
            owner=owner,
            family=family,
            stream=stream,
            variant=Variant.Request,
            schema_id=schema_id,
            version=version,
        )
        desired = route(
            plane=Plane.Write,
            layer=Layer.Shadow,
            owner=owner,
            family=family,
            stream=stream,
            variant=Variant.Desired,
            schema_id=schema_id,
            version=version,
        )
        reported = route(
            plane=Plane.Write,
            layer=Layer.Shadow,
            owner=owner,
            family=family,
            stream=stream,
            variant=Variant.Reported,
            schema_id=schema_id,
            version=version,
        )
        effective = route(
            plane=Plane.Write,
            layer=Layer.Shadow,
            owner=owner,
            family=family,
            stream=stream,
            variant=Variant.Effective,
            schema_id=schema_id,
            version=version,
        )
        ack = route(
            plane=Plane.Write,
            layer=Layer.Shadow,
            owner=owner,
            family=family,
            stream=stream,
            variant=Variant.Ack,
            schema_id=f"{schema_id}Ack",
            version=version,
        )
        return WriteBinding(request=request, desired=desired, reported=reported, effective=effective, ack=ack)


@dataclass(frozen=True)
class ControlLoops:
    """Factories for narrow RFC-shaped control loop stubs."""

    @staticmethod
    def with_routes(name: str, *, read_routes: list[RouteRef], write_request: RouteRef) -> NativeControlLoop:
        return NativeControlLoop(name=name, read_routes=read_routes, write_request=write_request)

    @staticmethod
    def speed_pid(*, read_state: RouteRef, read_feedback: RouteRef, write_request: RouteRef) -> NativeControlLoop:
        return ControlLoops.with_routes(
            "SpeedPid",
            read_routes=[read_state, read_feedback],
            write_request=write_request,
        )

    @staticmethod
    def counter_accumulate(*, read_state: RouteRef, write_request: RouteRef) -> NativeControlLoop:
        return ControlLoops.with_routes(
            "CounterAccumulate",
            read_routes=[read_state],
            write_request=write_request,
        )


class Graph:
    """Thin ergonomic wrapper over the native graph."""

    def __init__(self) -> None:
        self._graph = NativeGraph()

    def register_port(self, route_ref: RouteRef) -> RouteRef:
        return self._graph.register_port(route_ref)

    def read(self, route_ref: RouteRef):
        return self._graph.read(route_ref)

    def write(self, target):
        if isinstance(target, WriteBinding):
            return self._graph.register_binding(target.request.display(), target)
        return self._graph.writable_port(target)

    def mailbox(self, name: str, descriptor=None):
        return self._graph.mailbox(name, descriptor)

    def connect(self, source, sink) -> None:
        self._graph.connect(source, sink)

    def install(self, control_loop: NativeControlLoop) -> None:
        self._graph.install(control_loop)

    def tick_control_loop(self, name: str):
        return self._graph.tick_control_loop(name)

    def catalog(self) -> list[RouteRef]:
        return self._graph.catalog()

    def describe_route(self, route_ref: RouteRef):
        return self._graph.describe_route(route_ref)

    def latest(self, route_ref: RouteRef):
        return self._graph.latest(route_ref)

    def topology(self) -> list[tuple[str, str]]:
        return self._graph.topology()

    def validate_graph(self) -> list[str]:
        return self._graph.validate_graph()
