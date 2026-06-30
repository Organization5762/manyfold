"""Profileable workloads for core Manyfold runtime subparts."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal

from manyfold.graph import Graph, ProcessEndpoint, ProcessRpcTransport
from manyfold.primitives import Layer, Plane, Schema, Variant, route

RuntimeWorkload = Literal["publish", "observe", "rpc"]
RunMode = Literal["standard", "parallel"]

WORKLOADS: tuple[RuntimeWorkload, ...] = ("publish", "observe", "rpc")


def run_runtime_benchmark(
    workload: RuntimeWorkload,
    *,
    iterations: int = 1_000,
    runs: int = 1,
    mode: RunMode = "standard",
    callers: int = 4,
    max_average_event_us: float | None = None,
    max_elapsed_seconds: float | None = None,
) -> dict[str, object]:
    """Run one core runtime workload and return JSON-ready timing data."""

    _require_positive_int(iterations, "iterations")
    _require_positive_int(runs, "runs")
    _require_positive_float_or_none(max_average_event_us, "max_average_event_us")
    _require_positive_float_or_none(max_elapsed_seconds, "max_elapsed_seconds")
    if workload not in WORKLOADS:
        raise ValueError(f"unsupported runtime workload: {workload}")
    if mode not in ("standard", "parallel"):
        raise ValueError("runtime benchmark mode must be standard or parallel")
    runner = _workload_runner(workload)
    timed = _run_repeated(
        lambda: _run_parallel(runner, iterations=iterations, callers=callers)
        if mode == "parallel"
        else runner(iterations),
        events=iterations,
        runs=runs,
    )
    result = {
        "workload": workload,
        "mode": mode,
        **timed,
    }
    return _with_gates(
        result,
        max_average_event_us=max_average_event_us,
        max_elapsed_seconds=max_elapsed_seconds,
    )


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workload", nargs="?", choices=WORKLOADS)
    parser.add_argument("--iterations", type=int, default=1_000)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--callers", type=int, default=4)
    parser.add_argument(
        "--output-json",
        type=Path,
        help="write the benchmark result JSON to this path as a reusable artifact",
    )
    parser.add_argument(
        "--max-average-event-us",
        type=float,
        help="fail if average microseconds per event exceeds this value",
    )
    parser.add_argument(
        "--max-elapsed-seconds",
        type=float,
        help="fail if mean elapsed seconds exceeds this value",
    )
    parser.add_argument(
        "--mode",
        choices=("standard", "parallel"),
        default="standard",
    )
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args(argv)
    if args.list:
        print("\n".join(WORKLOADS))
        return 0
    if args.workload is None:
        parser.error("workload is required unless --list is passed")
    result = run_runtime_benchmark(
        args.workload,
        iterations=args.iterations,
        runs=args.runs,
        mode=args.mode,
        callers=args.callers,
        max_average_event_us=args.max_average_event_us,
        max_elapsed_seconds=args.max_elapsed_seconds,
    )
    output = json.dumps(result, sort_keys=True)
    if args.output_json is not None:
        _write_json_artifact(args.output_json, output)
    sys.stdout.write(output + "\n")
    return 0 if result["passed"] else 1


def _publish_workload(iterations: int) -> dict[str, int]:
    graph = Graph()
    target = _benchmark_route("publish")
    payload = b"x" * 16
    for _ in range(iterations):
        graph.publish_nowait(target, payload)
    latest = graph.latest(target)
    return {
        "events": iterations,
        "latest_payload_bytes": len(latest.value) if latest is not None else 0,
    }


def _observe_workload(iterations: int) -> dict[str, int]:
    graph = Graph()
    target = _benchmark_route("observe")
    observed = 0

    def _on_next(_envelope: object) -> None:
        nonlocal observed
        observed += 1

    subscription = graph.observe(target).subscribe(_on_next)
    try:
        for _ in range(iterations):
            graph.publish_nowait(target, b"x")
    finally:
        subscription.dispose()
    return {
        "events": iterations,
        "observed": observed,
    }


def _rpc_workload(iterations: int) -> dict[str, int]:
    graph = Graph()
    caller = ProcessEndpoint("runtime-benchmark-caller")
    worker = ProcessEndpoint("runtime-benchmark-worker")
    server = ProcessRpcTransport.install(graph, worker, audit_limit=iterations)
    client = ProcessRpcTransport.attach(graph, server.routes, caller)
    try:
        server.register("benchmark", "echo", lambda request: request.payload)
        for index in range(iterations):
            client.call(
                target=worker,
                service="benchmark",
                method="echo",
                payload=index,
                correlation_id=f"call-{index}",
            )
        audit = next(graph.retention_snapshot(server.routes.audit_log))
        return {
            "events": iterations,
            "rpc_calls": iterations,
            "retained_audit_payloads": audit.payload_count,
        }
    finally:
        client.dispose()
        server.dispose()


def _run_parallel(
    runner: Callable[[int], dict[str, int]],
    *,
    iterations: int,
    callers: int,
) -> dict[str, int]:
    _require_positive_int(callers, "callers")
    shard_sizes = _event_shards(events=iterations, shards=callers)
    with ThreadPoolExecutor(max_workers=callers) as executor:
        results = tuple(executor.map(runner, shard_sizes))
    totals: dict[str, int] = {
        "callers": callers,
        "events": iterations,
    }
    for result in results:
        for key, value in result.items():
            if key == "events":
                continue
            totals[key] = totals.get(key, 0) + value
    return totals


def _run_repeated(
    workload: Callable[[], dict[str, int]],
    *,
    events: int,
    runs: int,
) -> dict[str, object]:
    _require_positive_int(events, "events")
    _require_positive_int(runs, "runs")
    durations: list[float] = []
    latest: dict[str, int] | None = None
    for _ in range(runs):
        started = time.perf_counter()
        latest = workload()
        durations.append(time.perf_counter() - started)
    if latest is None:
        raise RuntimeError("runtime benchmark produced no result")
    mean_seconds = statistics.mean(durations)
    return {
        **latest,
        "average_event_us": mean_seconds * 1_000_000 / events,
        "elapsed_seconds": mean_seconds,
        "max_seconds": max(durations),
        "mean_seconds": mean_seconds,
        "min_seconds": min(durations),
        "runs": runs,
        "stdev_seconds": statistics.pstdev(durations),
    }


def _benchmark_route(stream: str):
    return route(
        plane=Plane.Write,
        layer=Layer.Logical,
        owner="runtime_benchmark",
        family="subpart",
        stream=stream,
        variant=Variant.Request,
        schema=Schema.bytes(name=f"RuntimeBenchmark{stream.title()}"),
    )


def _event_shards(*, events: int, shards: int) -> tuple[int, ...]:
    base, extra = divmod(events, shards)
    return tuple(base + (1 if index < extra else 0) for index in range(shards))


def _require_positive_int(value: int, field: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0:
        raise ValueError(f"{field} must be positive")


def _require_positive_float_or_none(value: float | None, field: str) -> None:
    if value is None:
        return
    if not isinstance(value, (float, int)) or isinstance(value, bool):
        raise TypeError(f"{field} must be a number or None")
    if value <= 0:
        raise ValueError(f"{field} must be positive")


def _with_gates(
    result: dict[str, object],
    *,
    max_average_event_us: float | None,
    max_elapsed_seconds: float | None,
) -> dict[str, object]:
    gates = []
    if max_average_event_us is not None:
        gates.append(
            _upper_bound_gate(
                result,
                name="max_average_event_us",
                field="average_event_us",
                limit=max_average_event_us,
            )
        )
    if max_elapsed_seconds is not None:
        gates.append(
            _upper_bound_gate(
                result,
                name="max_elapsed_seconds",
                field="elapsed_seconds",
                limit=max_elapsed_seconds,
            )
        )
    return {
        **result,
        "gates": tuple(gates),
        "passed": all(gate["passed"] for gate in gates),
    }


def _upper_bound_gate(
    result: dict[str, object],
    *,
    name: str,
    field: str,
    limit: float,
) -> dict[str, object]:
    observed = result.get(field)
    if not isinstance(observed, (float, int)) or isinstance(observed, bool):
        raise TypeError(f"{field} must be numeric to evaluate {name}")
    return {
        "field": field,
        "limit": float(limit),
        "name": name,
        "observed": float(observed),
        "passed": observed <= limit,
    }


def _write_json_artifact(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload + "\n", encoding="utf-8")


def _workload_runner(workload: RuntimeWorkload) -> Callable[[int], dict[str, int]]:
    if workload == "publish":
        return _publish_workload
    if workload == "observe":
        return _observe_workload
    if workload == "rpc":
        return _rpc_workload
    raise ValueError(f"unsupported runtime workload: {workload}")


if __name__ == "__main__":
    raise SystemExit(_main())
