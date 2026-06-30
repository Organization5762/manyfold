"""Private publish-path support for :mod:`manyfold.graph`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from manyfold._manyfold_rust import Graph as NativeGraph, RouteRef
from manyfold.primitives import TypedRoute


@dataclass(frozen=True)
class NativeEmitCalls:
    """Native graph emit methods used by Python publish paths."""

    emit: Callable[..., Any] | None
    emit_single_if_unrouted: Callable[..., Any] | None
    emit_single_if_unrouted_drop: Callable[..., Any] | None
    emit_single_if_unrouted_and_materializer_drop: Callable[..., Any] | None
    emit_single_if_unrouted_and_materializer_drop_python: Callable[..., Any] | None
    emit_single_if_unrouted_with_lineage_no_parents: Callable[..., Any] | None
    emit_single_if_unrouted_with_lineage_no_parents_and_materializers: (
        Callable[..., Any] | None
    )
    emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop: (
        Callable[..., Any] | None
    )
    emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop_python: (
        Callable[..., Any] | None
    )
    emit_single_if_unrouted_with_lineage_ids_no_parents_and_materializers_drop_python: (
        Callable[..., Any] | None
    )
    emit_single_if_unrouted_with_lineage_ids_no_parents_and_materializer_drop_python: (
        Callable[..., Any] | None
    )
    compile_no_lineage_materializer_drop_profile: Callable[..., Any] | None
    release_no_lineage_materializer_drop_profile: Callable[..., Any] | None
    emit_no_lineage_materializer_drop_profile_python: Callable[..., Any] | None
    materialize_bytes_one_parent: Callable[..., Any] | None
    register_materialize_bytes: Callable[..., Any] | None
    unregister_materialize_bytes: Callable[..., Any] | None
    payload_by_id: Callable[..., Any] | None

    @classmethod
    def from_native_graph(cls, graph: NativeGraph) -> NativeEmitCalls:
        """Collect optional native emit methods once per graph reset."""
        return cls(
            emit=getattr(graph, "emit", None),
            emit_single_if_unrouted=getattr(graph, "emit_single_if_unrouted", None),
            emit_single_if_unrouted_drop=getattr(
                graph,
                "emit_single_if_unrouted_drop",
                None,
            ),
            emit_single_if_unrouted_and_materializer_drop=getattr(
                graph,
                "emit_single_if_unrouted_and_materializer_drop",
                None,
            ),
            emit_single_if_unrouted_and_materializer_drop_python=getattr(
                graph,
                "emit_single_if_unrouted_and_materializer_drop_python",
                None,
            ),
            emit_single_if_unrouted_with_lineage_no_parents=getattr(
                graph,
                "emit_single_if_unrouted_with_lineage_no_parents",
                None,
            ),
            emit_single_if_unrouted_with_lineage_no_parents_and_materializers=getattr(
                graph,
                "emit_single_if_unrouted_with_lineage_no_parents_and_materializers",
                None,
            ),
            emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop=getattr(
                graph,
                "emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop",
                None,
            ),
            emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop_python=getattr(
                graph,
                "emit_single_if_unrouted_with_lineage_no_parents_and_materializers_drop_python",
                None,
            ),
            emit_single_if_unrouted_with_lineage_ids_no_parents_and_materializers_drop_python=getattr(
                graph,
                "emit_single_if_unrouted_with_lineage_ids_no_parents_and_materializers_drop_python",
                None,
            ),
            emit_single_if_unrouted_with_lineage_ids_no_parents_and_materializer_drop_python=getattr(
                graph,
                "emit_single_if_unrouted_with_lineage_ids_no_parents_and_materializer_drop_python",
                None,
            ),
            compile_no_lineage_materializer_drop_profile=getattr(
                graph,
                "compile_no_lineage_materializer_drop_profile",
                None,
            ),
            release_no_lineage_materializer_drop_profile=getattr(
                graph,
                "release_no_lineage_materializer_drop_profile",
                None,
            ),
            emit_no_lineage_materializer_drop_profile_python=getattr(
                graph,
                "emit_no_lineage_materializer_drop_profile_python",
                None,
            ),
            materialize_bytes_one_parent=getattr(
                graph,
                "materialize_bytes_one_parent",
                None,
            ),
            register_materialize_bytes=getattr(
                graph,
                "register_materialize_bytes",
                None,
            ),
            unregister_materialize_bytes=getattr(
                graph,
                "unregister_materialize_bytes",
                None,
            ),
            payload_by_id=getattr(graph, "payload_by_id", None),
        )


@dataclass(frozen=True)
class TypedPublishTarget:
    """Resolved typed target data used by publish paths."""

    route: TypedRoute[Any]
    native_route: RouteRef
    route_key: str
    is_process_local: bool
    is_bytes_passthrough: bool
    uses_sparse_native_retention: bool

    @property
    def runtime_flags(self) -> tuple[bool, bool, bool]:
        """Return the cacheable runtime flags for this target."""
        return (
            self.is_process_local,
            self.is_bytes_passthrough,
            self.uses_sparse_native_retention,
        )
