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
from dataclasses import dataclass, field
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
    _has_known_any_schema_value,
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
MAX_FIRST_PUBLISH_BATCH_SIZE = 16
MIN_FORMAL_FIRST_PUBLISHES_PER_RUN = 512


def run_publish_benchmarks(
    workloads: Sequence[PublishWorkload] = WORKLOADS,
    *,
    first_publish_batch_size: int = 16,
    first_publish_batches: int = 64,
    iterations: int = 100_000,
    require_clean: bool = True,
    runs: int = 7,
    warmup_iterations: int = 10_000,
) -> dict[str, object]:
    """Run selected publish workloads and return a reusable evidence artifact."""

    _require_positive_int(first_publish_batch_size, "first_publish_batch_size")
    _require_positive_int(first_publish_batches, "first_publish_batches")
    _require_batch_size(first_publish_batch_size)
    _require_positive_int(iterations, "iterations")
    _require_positive_int(runs, "runs")
    _require_positive_int(warmup_iterations, "warmup_iterations")
    selected = tuple(workloads)
    if not selected:
        raise ValueError("at least one publish workload is required")
    unsupported = tuple(workload for workload in selected if workload not in WORKLOADS)
    if unsupported:
        raise ValueError(f"unsupported publish workload: {unsupported[0]}")
    if len(set(selected)) != len(selected):
        duplicate = next(
            workload
            for index, workload in enumerate(selected)
            if workload in selected[:index]
        )
        raise ValueError(f"publish workloads must be unique; duplicate: {duplicate}")
    first_publishes_per_run = first_publish_batch_size * first_publish_batches
    if (
        require_clean
        and first_publishes_per_run < MIN_FORMAL_FIRST_PUBLISHES_PER_RUN
    ):
        raise ValueError(
            "formal publish benchmark requires at least "
            f"{MIN_FORMAL_FIRST_PUBLISHES_PER_RUN} first publishes per run; "
            f"observed {first_publishes_per_run}"
        )
    provenance = _repository_provenance()
    if require_clean and provenance["dirty"]:
        raise RuntimeError(
            "formal publish benchmark requires a clean git worktree; "
            "commit or remove local changes first"
        )
    setups = {workload: _workload_setup(workload) for workload in selected}
    for setup in setups.values():
        _run_once(
            setup,
            iterations=max(1, min(iterations, 16)),
            warmup_iterations=max(1, min(warmup_iterations, 16)),
        )
    measurements = {
        workload: _WorkloadMeasurements() for workload in selected
    }
    run_workload_orders: list[tuple[PublishWorkload, ...]] = []
    for run_index in range(runs):
        run_order = _rotated_workloads(selected, run_index)
        run_workload_orders.append(run_order)
        for workload in run_order:
            first_publish_result = _run_first_publish_batches(
                setups[workload],
                batch_size=first_publish_batch_size,
                batches=first_publish_batches,
            )
            result = _run_once(
                setups[workload],
                iterations=iterations,
                warmup_iterations=warmup_iterations,
            )
            measurements[workload].append(first_publish_result, result)
    return {
        "environment": _environment(),
        "first_publish_batch_size": first_publish_batch_size,
        "first_publish_batches": first_publish_batches,
        "first_publishes_per_run": first_publishes_per_run,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "iterations": iterations,
        "provenance": provenance,
        "run_workload_orders": tuple(run_workload_orders),
        "runs": runs,
        "schema_version": 4,
        "warmup_iterations": warmup_iterations,
        "workloads": tuple(
            _summarize_workload(
                workload,
                measurements[workload],
                batch_size=first_publish_batch_size,
                batches=first_publish_batches,
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
    parser.add_argument("--first-publish-batch-size", type=int, default=16)
    parser.add_argument("--first-publish-batches", type=int, default=64)
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
        first_publish_batch_size=args.first_publish_batch_size,
        first_publish_batches=args.first_publish_batches,
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


def _summarize_workload(
    workload: PublishWorkload,
    measurements: _WorkloadMeasurements,
    *,
    batch_size: int,
    batches: int,
) -> dict[str, object]:
    first_state = _require_equal_states(
        workload,
        "steady",
        measurements.final_states,
    )
    first_publish_state = _require_equal_states(
        workload,
        "per-route first-publish",
        measurements.first_publish_final_states,
    )
    return {
        "end_to_end_seconds": _timing_summary(measurements.end_to_end_seconds),
        "final_state": first_state,
        "per_route_first_publish": {
            **_timing_summary(measurements.first_publish_event_us),
            "batch_size": batch_size,
            "batches_per_run": batches,
            "publishes_per_run": batch_size * batches,
            "raw_batch_means_us": tuple(
                measurements.first_publish_batch_means_us
            ),
            "raw_batch_process_local_value_deltas": tuple(
                measurements.first_publish_batch_process_local_value_deltas
            ),
            "raw_batch_timed_duration_us": tuple(
                measurements.first_publish_batch_timed_duration_us
            ),
            "run_final_state": first_publish_state,
            "run_final_states": tuple(
                measurements.first_publish_final_states
            ),
            "run_total_timed_duration_us": tuple(
                measurements.first_publish_total_timed_duration_us
            ),
            "run_verified_sessions": tuple(
                measurements.first_publish_verified_sessions
            ),
        },
        "run_final_states": tuple(measurements.final_states),
        "steady_publish": _timing_summary(measurements.steady_event_us),
        "workload": workload,
    }


def _rotated_workloads(
    workloads: tuple[PublishWorkload, ...],
    run_index: int,
) -> tuple[PublishWorkload, ...]:
    offset = run_index % len(workloads)
    return workloads[offset:] + workloads[:offset]


def _run_first_publish_batches(
    setup: Callable[[], _WorkloadSession],
    *,
    batch_size: int,
    batches: int,
) -> dict[str, Any]:
    _require_positive_int(batch_size, "batch_size")
    _require_positive_int(batches, "batches")
    _require_batch_size(batch_size)
    batch_means_us: list[float] = []
    batch_process_local_value_deltas: list[int] = []
    batch_timed_duration_us: list[float] = []
    batch_final_states: list[dict[str, int | bool]] = []
    verified_sessions = 0
    for _ in range(batches):
        result = _run_first_publish_batch(setup, batch_size=batch_size)
        batch_means_us.append(result["average_event_us"])
        batch_process_local_value_deltas.append(
            result["process_local_value_delta"]
        )
        batch_timed_duration_us.append(result["timed_duration_us"])
        batch_final_states.append(result["final_state"])
        verified_sessions += result["verified_sessions"]
    first_state = _require_equal_states(
        "fresh route",
        "per-route first-publish batch",
        batch_final_states,
    )
    total_timed_duration_us = sum(batch_timed_duration_us)
    total_publishes = batch_size * batches
    return {
        "average_event_us": total_timed_duration_us / total_publishes,
        "batch_means_us": tuple(batch_means_us),
        "batch_process_local_value_deltas": tuple(
            batch_process_local_value_deltas
        ),
        "batch_timed_duration_us": tuple(batch_timed_duration_us),
        "final_state": first_state,
        "total_timed_duration_us": total_timed_duration_us,
        "verified_sessions": verified_sessions,
    }


def _run_first_publish_batch(
    setup: Callable[[], _WorkloadSession],
    *,
    batch_size: int,
) -> dict[str, Any]:
    process_local_baseline = _any_schema_value_count()
    sessions: list[_WorkloadSession] = []
    final_states: list[dict[str, int | bool]] = []
    error: BaseException | None = None
    process_local_value_delta = 0
    timed_duration_us = 0.0
    try:
        for _ in range(batch_size):
            sessions.append(setup())
        started_ns = time.perf_counter_ns()
        for session in sessions:
            session.publish_one()
        timed_duration_us = (time.perf_counter_ns() - started_ns) / 1_000.0
        for session in sessions:
            final_states.append(session.final_state(1))
        process_local_value_delta = (
            _any_schema_value_count() - process_local_baseline
        )
    except BaseException as caught:
        error = caught
    for index, session in enumerate(sessions):
        try:
            disposal_state = session.dispose()
        except BaseException as caught:
            if error is None:
                error = caught
            continue
        if index < len(final_states):
            final_states[index].update(disposal_state)
    if _any_schema_value_count() != process_local_baseline and error is None:
        error = RuntimeError(
            "first-publish batch did not restore process-local payload ownership: "
            f"expected {process_local_baseline}, observed {_any_schema_value_count()}"
        )
    if error is not None:
        raise error
    final_state = _require_equal_states(
        "fresh route",
        "per-route first-publish session",
        final_states,
    )
    return {
        "average_event_us": timed_duration_us / batch_size,
        "final_state": final_state,
        "process_local_value_delta": process_local_value_delta,
        "timed_duration_us": timed_duration_us,
        "verified_sessions": len(final_states),
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
        session.publish_one()
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
    graph = Graph()
    target = _benchmark_route("process_local", Schema.any("PublishProcessLocal"))
    payload = object()
    return _session(
        graph,
        publish_one=lambda: graph.publish_nowait(target, payload),
        final_state=lambda _events: {
            "latest_preserves_identity": _require_latest_value(graph, target)
            is payload,
            "retained_process_local_values": len(graph._materialized_payloads),
            "retained_history": _retained_history(graph, target),
        },
        tracks_process_local_values=True,
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
    tracks_process_local_values: bool = False,
) -> _WorkloadSession:
    def dispose() -> dict[str, int | bool]:
        process_local_values = (
            ()
            if not tracks_process_local_values
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
            if tracks_process_local_values:
                state["released_by_graph"] = all(
                    not _has_known_any_schema_value(payload)
                    for payload in process_local_values
                )
            for payload in process_local_values:
                _release_known_any_schema_value(payload)
            if tracks_process_local_values:
                state["released_by_benchmark_cleanup"] = all(
                    not _has_known_any_schema_value(payload)
                    for payload in process_local_values
                )
        if disposal_error is not None:
            raise disposal_error
        return state

    return _WorkloadSession(
        publish_one=publish_one,
        final_state=final_state,
        dispose=dispose,
    )


def _require_equal_states(
    workload: str,
    measurement: str,
    states: Sequence[dict[str, int | bool]],
) -> dict[str, int | bool]:
    first_state = states[0]
    for run_index, state in enumerate(states[1:], start=2):
        if state != first_state:
            raise RuntimeError(
                f"publish benchmark {workload!r} {measurement} final state changed "
                f"in run {run_index}: expected {first_state!r}, observed {state!r}"
            )
    return first_state


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


def _require_batch_size(batch_size: int) -> None:
    if batch_size > MAX_FIRST_PUBLISH_BATCH_SIZE:
        raise ValueError(
            "first-publish batch size must not exceed "
            f"{MAX_FIRST_PUBLISH_BATCH_SIZE}; observed {batch_size}"
        )


@dataclass(frozen=True, slots=True)
class _WorkloadSession:
    publish_one: Callable[[], object]
    final_state: Callable[[int], dict[str, int | bool]]
    dispose: Callable[[], dict[str, int | bool]]


@dataclass(slots=True)
class _WorkloadMeasurements:
    end_to_end_seconds: list[float] = field(default_factory=list)
    final_states: list[dict[str, int | bool]] = field(default_factory=list)
    first_publish_batch_means_us: list[tuple[float, ...]] = field(
        default_factory=list
    )
    first_publish_batch_process_local_value_deltas: list[tuple[int, ...]] = field(
        default_factory=list
    )
    first_publish_batch_timed_duration_us: list[tuple[float, ...]] = field(
        default_factory=list
    )
    first_publish_event_us: list[float] = field(default_factory=list)
    first_publish_final_states: list[dict[str, int | bool]] = field(
        default_factory=list
    )
    first_publish_total_timed_duration_us: list[float] = field(
        default_factory=list
    )
    first_publish_verified_sessions: list[int] = field(default_factory=list)
    steady_event_us: list[float] = field(default_factory=list)

    def append(
        self,
        first_publish_result: dict[str, Any],
        steady_result: dict[str, Any],
    ) -> None:
        self.end_to_end_seconds.append(steady_result["end_to_end_seconds"])
        self.final_states.append(steady_result["final_state"])
        self.first_publish_batch_means_us.append(
            first_publish_result["batch_means_us"]
        )
        self.first_publish_batch_process_local_value_deltas.append(
            first_publish_result["batch_process_local_value_deltas"]
        )
        self.first_publish_batch_timed_duration_us.append(
            first_publish_result["batch_timed_duration_us"]
        )
        self.first_publish_event_us.append(
            first_publish_result["average_event_us"]
        )
        self.first_publish_final_states.append(first_publish_result["final_state"])
        self.first_publish_total_timed_duration_us.append(
            first_publish_result["total_timed_duration_us"]
        )
        self.first_publish_verified_sessions.append(
            first_publish_result["verified_sessions"]
        )
        self.steady_event_us.append(steady_result["steady_event_us"])


if __name__ == "__main__":
    raise SystemExit(_main())
