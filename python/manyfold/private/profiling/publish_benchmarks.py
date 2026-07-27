"""Repeatable benchmarks for the Python :class:`manyfold.graph.Graph` publish path."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Literal

from manyfold import _manyfold_rust
from manyfold._repo_paths import REPO_ROOT
from manyfold.graph import Graph, RouteRetentionPolicy, SubscriptionLike
from manyfold.primitives import (
    Layer,
    Plane,
    Schema,
    TypedRoute,
    Variant,
    _any_schema_value_count,
    _release_known_any_schema_value,
    route,
)

PublishWorkload = Literal[
    "materializer_fanout_nowait",
    "materializer_unobserved_nowait",
    "process_local_nowait",
    "raw_route_publish",
    "retained_history_publish",
    "sparse_drop_nowait",
    "subscriber_delivery_nowait",
    "typed_bytes_publish",
    "typed_encoded_nowait",
    "typed_encoded_publish",
]

WORKLOADS: tuple[PublishWorkload, ...] = (
    "materializer_fanout_nowait",
    "materializer_unobserved_nowait",
    "process_local_nowait",
    "raw_route_publish",
    "retained_history_publish",
    "sparse_drop_nowait",
    "subscriber_delivery_nowait",
    "typed_bytes_publish",
    "typed_encoded_nowait",
    "typed_encoded_publish",
)


def run_publish_benchmarks(
    workloads: Sequence[PublishWorkload] = WORKLOADS,
    *,
    iterations: int = 100_000,
    require_clean: bool = True,
    runs: int = 7,
    warmup_iterations: int = 10_000,
) -> dict[str, object]:
    """Run selected publish workloads and return a reusable evidence artifact."""

    _require_positive_int(iterations, "iterations")
    _require_positive_int(runs, "runs")
    _require_positive_int(warmup_iterations, "warmup_iterations")
    selected = tuple(workloads)
    if not selected:
        raise ValueError("at least one publish workload is required")
    unsupported = tuple(workload for workload in selected if workload not in WORKLOADS)
    if unsupported:
        raise ValueError(f"unsupported publish workload: {unsupported[0]}")
    provenance = _repository_provenance()
    if require_clean and provenance["dirty"]:
        raise RuntimeError(
            "formal publish benchmark requires a clean git worktree; "
            "commit or remove local changes first"
        )
    return {
        "environment": _environment(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "iterations": iterations,
        "provenance": provenance,
        "runs": runs,
        "schema_version": 2,
        "warmup_iterations": warmup_iterations,
        "workloads": tuple(
            _run_repeated(
                workload,
                iterations=iterations,
                runs=runs,
                warmup_iterations=warmup_iterations,
            )
            for workload in selected
        ),
    }


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "workloads",
        nargs="*",
        choices=WORKLOADS,
        help="workloads to run; defaults to the full publish matrix",
    )
    parser.add_argument("--iterations", type=int, default=100_000)
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--warmup-iterations", type=int, default=10_000)
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--allow-dirty", action="store_true")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args(argv)
    if args.list:
        sys.stdout.write("\n".join(WORKLOADS) + "\n")
        return 0
    result = run_publish_benchmarks(
        args.workloads or WORKLOADS,
        iterations=args.iterations,
        require_clean=not args.allow_dirty,
        runs=args.runs,
        warmup_iterations=args.warmup_iterations,
    )
    output = json.dumps(result, indent=2, sort_keys=True)
    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(output + "\n", encoding="utf-8")
    sys.stdout.write(output + "\n")
    return 0


def _run_repeated(
    workload: PublishWorkload,
    *,
    iterations: int,
    runs: int,
    warmup_iterations: int,
) -> dict[str, object]:
    setup = _workload_setup(workload)
    _run_once(
        setup,
        iterations=max(1, min(iterations, 16)),
        warmup_iterations=max(1, min(warmup_iterations, 16)),
    )
    cold_event_us: list[float] = []
    end_to_end_seconds: list[float] = []
    final_states: list[dict[str, int | bool]] = []
    steady_event_us: list[float] = []
    for _ in range(runs):
        result = _run_once(
            setup,
            iterations=iterations,
            warmup_iterations=warmup_iterations,
        )
        cold_event_us.append(result["cold_event_us"])
        end_to_end_seconds.append(result["end_to_end_seconds"])
        final_states.append(result["final_state"])
        steady_event_us.append(result["steady_event_us"])
    first_state = final_states[0]
    for run_index, state in enumerate(final_states[1:], start=2):
        if state != first_state:
            raise RuntimeError(
                f"publish benchmark {workload!r} final state changed in run "
                f"{run_index}: expected {first_state!r}, observed {state!r}"
            )
    return {
        "per_route_first_publish": _timing_summary(cold_event_us),
        "end_to_end_seconds": _timing_summary(end_to_end_seconds),
        "final_state": first_state,
        "run_final_states": tuple(final_states),
        "steady_publish": _timing_summary(steady_event_us),
        "workload": workload,
    }


def _run_once(
    setup: Callable[[], _WorkloadSession],
    *,
    iterations: int,
    warmup_iterations: int,
) -> dict[str, Any]:
    end_to_end_started = time.perf_counter()
    session = setup()
    total_events = 1 + warmup_iterations + iterations
    try:
        cold_started = time.perf_counter()
        session.publish_one()
        cold_event_us = (time.perf_counter() - cold_started) * 1_000_000.0
        for _ in range(warmup_iterations):
            session.publish_one()
        steady_started = time.perf_counter()
        for _ in range(iterations):
            session.publish_one()
        steady_event_us = (
            (time.perf_counter() - steady_started) * 1_000_000.0 / iterations
        )
        final_state = session.final_state(total_events)
    finally:
        disposal_state = session.dispose()
    final_state.update(disposal_state)
    return {
        "cold_event_us": cold_event_us,
        "end_to_end_seconds": time.perf_counter() - end_to_end_started,
        "final_state": final_state,
        "steady_event_us": steady_event_us,
    }


def _setup_raw_route_publish() -> _WorkloadSession:
    graph = Graph()
    target = _benchmark_route("raw_route", Schema.bytes(name="PublishRawRoute"))
    payload = b"x" * 16
    return _session(
        graph,
        publish_one=lambda: graph.publish(target.route_ref, payload),
        final_state=lambda _events: {
            "latest_payload_bytes": len(_require_latest_bytes(graph, target)),
            "retained_history": _retained_history(graph, target),
        },
    )


def _setup_typed_bytes_publish() -> _WorkloadSession:
    graph = Graph()
    target = _benchmark_route("typed_bytes", Schema.bytes(name="PublishTypedBytes"))
    payload = b"x" * 16
    return _session(
        graph,
        publish_one=lambda: graph.publish(target, payload),
        final_state=lambda _events: {
            "latest_payload_bytes": len(_require_latest_value(graph, target)),
            "retained_history": _retained_history(graph, target),
        },
    )


def _setup_typed_encoded_publish() -> _WorkloadSession:
    graph = Graph()
    target = _benchmark_route("typed_encoded", Schema.float(name="PublishFloat"))
    return _session(
        graph,
        publish_one=lambda: graph.publish(target, 1.25),
        final_state=lambda _events: {
            "decoded_latest": _require_latest_value(graph, target) == 1.25,
            "retained_history": _retained_history(graph, target),
        },
    )


def _setup_typed_encoded_nowait() -> _WorkloadSession:
    graph = Graph()
    target = _benchmark_route(
        "typed_encoded_nowait",
        Schema.float(name="PublishFloatNowait"),
    )
    return _session(
        graph,
        publish_one=lambda: graph.publish_nowait(target, 1.25),
        final_state=lambda _events: {
            "decoded_latest": _require_latest_value(graph, target) == 1.25,
            "retained_history": _retained_history(graph, target),
        },
    )


def _setup_process_local_nowait() -> _WorkloadSession:
    start_count = _any_schema_value_count()
    graph = Graph()
    target = _benchmark_route("process_local", Schema.any("PublishProcessLocal"))
    payload = object()
    return _session(
        graph,
        publish_one=lambda: graph.publish_nowait(target, payload),
        final_state=lambda _events: {
            "latest_preserves_identity": _require_latest_value(graph, target)
            is payload,
            "retained_process_local_values": _any_schema_value_count()
            - start_count,
            "retained_history": _retained_history(graph, target),
        },
        process_local_value_baseline=start_count,
    )


def _setup_sparse_drop_nowait() -> _WorkloadSession:
    graph = Graph()
    target = _benchmark_route("sparse_drop", Schema.bytes(name="PublishSparseDrop"))
    payload = b"x" * 16
    return _session(
        graph,
        publish_one=lambda: graph.publish_nowait(target, payload),
        final_state=lambda events: {
            "accepted_events": _latest_sequence(graph, target),
            "all_events_accepted": _latest_sequence(graph, target) == events,
            "latest_payload_bytes": len(_require_latest_value(graph, target)),
            "python_materialized_payloads": len(graph._materialized_payloads),
            "retained_history": _retained_history(graph, target),
        },
    )


def _setup_subscriber_delivery_nowait() -> _WorkloadSession:
    graph = Graph()
    target = _benchmark_route(
        "subscriber_delivery",
        Schema.bytes(name="PublishSubscriberDelivery"),
    )
    observed = 0

    def on_next(_envelope: object) -> None:
        nonlocal observed
        observed += 1

    subscription = graph.observe(target, replay_latest=False).subscribe(on_next)
    return _session(
        graph,
        publish_one=lambda: graph.publish_nowait(target, b"x" * 16),
        final_state=lambda events: {
            "all_events_delivered_inline": observed == events,
            "delivered": observed,
            "retained_history": _retained_history(graph, target),
            "subscribers": graph.subscribers(target),
        },
        disposables=(subscription,),
        after_dispose=lambda: {
            "subscribers_after_dispose": graph.subscribers(target)
        },
    )


def _setup_materializer_fanout_nowait() -> _WorkloadSession:
    graph = Graph()
    source = _benchmark_route(
        "materializer_fanout_source",
        Schema.bytes(name="PublishMaterializerFanoutSource"),
    )
    state_route = _benchmark_route(
        "materializer_fanout_state",
        Schema.bytes(name="PublishMaterializerFanoutState"),
        plane=Plane.State,
        variant=Variant.State,
    )
    observed = 0

    def on_next(_envelope: object) -> None:
        nonlocal observed
        observed += 1

    state_subscription = graph.observe(
        state_route,
        replay_latest=False,
    ).subscribe(on_next)
    materializer = graph.materialize(source, state_route=state_route)
    return _session(
        graph,
        publish_one=lambda: graph.publish_nowait(source, b"x" * 16),
        final_state=lambda events: {
            "all_events_delivered_inline": observed == events,
            "delivered": observed,
            "source_accepted_events": _latest_sequence(graph, source),
            "source_history": _retained_history(graph, source),
            "state_accepted_events": _latest_sequence(graph, state_route),
            "state_history": _retained_history(graph, state_route),
        },
        disposables=(materializer, state_subscription),
        after_dispose=lambda: {
            "subscribers_after_dispose": graph.subscribers(state_route)
        },
    )


def _setup_materializer_unobserved_nowait() -> _WorkloadSession:
    graph = Graph()
    source = _benchmark_route(
        "materializer_drop_source",
        Schema.bytes(name="PublishMaterializerDropSource"),
    )
    state_route = _benchmark_route(
        "materializer_drop_state",
        Schema.bytes(name="PublishMaterializerDropState"),
        plane=Plane.State,
        variant=Variant.State,
    )
    materializer = graph.materialize(source, state_route=state_route)
    return _session(
        graph,
        publish_one=lambda: graph.publish_nowait(source, b"x" * 16),
        final_state=lambda events: {
            "all_source_events_accepted": _latest_sequence(graph, source) == events,
            "all_state_events_accepted": _latest_sequence(graph, state_route) == events,
            "python_materialized_payloads": len(graph._materialized_payloads),
            "source_accepted_events": _latest_sequence(graph, source),
            "source_history": _retained_history(graph, source),
            "state_accepted_events": _latest_sequence(graph, state_route),
            "state_history": _retained_history(graph, state_route),
        },
        disposables=(materializer,),
    )


def _setup_retained_history_publish() -> _WorkloadSession:
    graph = Graph()
    target = _benchmark_route(
        "retained_history",
        Schema.bytes(name="PublishRetainedHistory"),
    )
    graph.configure_retention(
        target,
        RouteRetentionPolicy(
            latest_replay_policy="bounded_history",
            replay_window="last_32",
            history_limit=32,
        ),
    )
    return _session(
        graph,
        publish_one=lambda: graph.publish(target, b"x" * 16),
        final_state=lambda events: {
            "accepted_events": _latest_sequence(graph, target),
            "all_events_accepted": _latest_sequence(graph, target) == events,
            "retained_history": _retained_history(graph, target),
        },
    )


def _session(
    graph: Graph,
    *,
    publish_one: Callable[[], object],
    final_state: Callable[[int], dict[str, int | bool]],
    disposables: tuple[SubscriptionLike, ...] = (),
    after_dispose: Callable[[], dict[str, int | bool]] = lambda: {},
    process_local_value_baseline: int | None = None,
) -> _WorkloadSession:
    def dispose() -> dict[str, int | bool]:
        process_local_values = (
            ()
            if process_local_value_baseline is None
            else tuple(graph._materialized_payloads.values())
        )
        disposal_error: BaseException | None = None
        state: dict[str, int | bool] = {}
        try:
            for disposable in disposables:
                try:
                    disposable.dispose()
                except BaseException as error:
                    if disposal_error is None:
                        disposal_error = error
            try:
                graph.dispose()
            except BaseException as error:
                if disposal_error is None:
                    disposal_error = error
            state.update(
                {
                    "graph_owned_subscriptions_after_dispose": len(
                        graph._subscriptions
                    ),
                    "python_materialized_payloads_after_dispose": len(
                        graph._materialized_payloads
                    ),
                }
            )
            try:
                state.update(after_dispose())
            except BaseException as error:
                if disposal_error is None:
                    disposal_error = error
        finally:
            if process_local_value_baseline is not None:
                state["released_by_graph"] = (
                    _any_schema_value_count() == process_local_value_baseline
                )
            for payload in process_local_values:
                _release_known_any_schema_value(payload)
            if process_local_value_baseline is not None:
                state["released_by_benchmark_cleanup"] = (
                    _any_schema_value_count() == process_local_value_baseline
                )
        if disposal_error is not None:
            raise disposal_error
        return state

    return _WorkloadSession(
        publish_one=publish_one,
        final_state=final_state,
        dispose=dispose,
    )


def _timing_summary(values: Sequence[float]) -> dict[str, object]:
    average = statistics.fmean(values)
    stdev = statistics.pstdev(values)
    return {
        "average": average,
        "maximum": max(values),
        "minimum": min(values),
        "relative_stdev_percent": 0.0 if average == 0.0 else stdev / average * 100.0,
        "runs": tuple(values),
        "stdev": stdev,
    }


def _retained_history(graph: Graph, target: TypedRoute[Any]) -> int:
    return next(graph.retention_snapshot(target)).replay_count


def _require_latest_bytes(graph: Graph, target: TypedRoute[Any]) -> bytes:
    latest = graph.open_payload(target.route_ref)
    if latest is None:
        raise RuntimeError(f"raw route {target.display()} has no byte payload")
    return latest


def _require_latest_value(graph: Graph, target: TypedRoute[Any]) -> Any:
    latest = graph.latest(target)
    if latest is None:
        raise RuntimeError(f"typed route {target.display()} has no latest envelope")
    return latest.value


def _latest_sequence(graph: Graph, target: TypedRoute[Any]) -> int:
    latest = graph.latest(target)
    if latest is None:
        raise RuntimeError(f"typed route {target.display()} has no latest envelope")
    return latest.closed.seq_source


def _benchmark_route(
    stream: str,
    schema: Schema[Any],
    *,
    plane: Plane = Plane.Read,
    variant: Variant = Variant.Event,
) -> TypedRoute[Any]:
    return route(
        plane=plane,
        layer=Layer.Logical,
        owner="publish_benchmark",
        family="decision_table",
        stream=stream,
        variant=variant,
        schema=schema,
    )


def _workload_setup(workload: PublishWorkload) -> Callable[[], _WorkloadSession]:
    setups: dict[PublishWorkload, Callable[[], _WorkloadSession]] = {
        "materializer_fanout_nowait": _setup_materializer_fanout_nowait,
        "materializer_unobserved_nowait": _setup_materializer_unobserved_nowait,
        "process_local_nowait": _setup_process_local_nowait,
        "raw_route_publish": _setup_raw_route_publish,
        "retained_history_publish": _setup_retained_history_publish,
        "sparse_drop_nowait": _setup_sparse_drop_nowait,
        "subscriber_delivery_nowait": _setup_subscriber_delivery_nowait,
        "typed_bytes_publish": _setup_typed_bytes_publish,
        "typed_encoded_nowait": _setup_typed_encoded_nowait,
        "typed_encoded_publish": _setup_typed_encoded_publish,
    }
    return setups[workload]


def _environment() -> dict[str, object]:
    return {
        "cargo": _command_version(("cargo", "--version")),
        "cpu_count": os.cpu_count(),
        "cpu_model": _cpu_model(),
        "implementation": platform.python_implementation(),
        "machine": platform.machine(),
        "manyfold_version": _manyfold_version(),
        "native_bridge_version": _manyfold_rust.bridge_version(),
        "native_module": str(Path(_manyfold_rust.__file__).resolve()),
        "native_module_sha256": _file_sha256(Path(_manyfold_rust.__file__)),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "python_build": platform.python_build(),
        "python_compiler": platform.python_compiler(),
        "python_executable": str(Path(sys.executable).resolve()),
        "rustc": _command_version(("rustc", "--version")),
    }


def _repository_provenance() -> dict[str, object]:
    revision = _git_output(("rev-parse", "HEAD"))
    status = tuple(
        line
        for line in _git_output(
            ("status", "--short", "--untracked-files=all"),
        ).splitlines()
        if line
    )
    return {
        "dirty": bool(status),
        "revision": revision,
        "status": status,
        "tree": _git_output(("rev-parse", "HEAD^{tree}")),
    }


def _cpu_model() -> str:
    if sys.platform == "darwin":
        model = _command_version(("sysctl", "-n", "machdep.cpu.brand_string"))
        if model != "unavailable":
            return model
    return platform.processor() or platform.machine()


def _command_version(command: tuple[str, ...]) -> str:
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return "unavailable"
    return result.stdout.strip()


def _git_output(arguments: tuple[str, ...]) -> str:
    result = subprocess.run(
        ("git", *arguments),
        check=True,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    return result.stdout.strip()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _manyfold_version() -> str:
    try:
        return version("manyfold")
    except PackageNotFoundError:
        return "unknown"


def _require_positive_int(value: int, field: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0:
        raise ValueError(f"{field} must be positive")


@dataclass(frozen=True, slots=True)
class _WorkloadSession:
    publish_one: Callable[[], object]
    final_state: Callable[[int], dict[str, int | bool]]
    dispose: Callable[[], dict[str, int | bool]]


if __name__ == "__main__":
    raise SystemExit(_main())
