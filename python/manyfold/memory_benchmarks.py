from __future__ import annotations

import argparse
import gc
import json
import os
import resource
import signal
import subprocess
import sys
import tempfile
import time
import tracemalloc
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

from manyfold.graph import (
    Graph,
    NativeCorrelationTracingStore,
    NativeLineageTracingStore,
    NoopCorrelationTracingStore,
    NoopLineageTracingStore,
    RouteRetentionPolicy,
    TaintDomain,
    TaintMark,
)
from manyfold.primitives import (
    Layer,
    OwnerName,
    Plane,
    Schema,
    StreamFamily,
    StreamName,
    Variant,
    route,
)

_FdReader = Callable[[int], int | None]
_SmapsReader = Callable[[int], "_ProcessMemory | None"]
_ProcessRow = tuple[int, int, int]
_RssSampleIndex = int | slice
_BENCHMARK_TRACE_FILENAME = str(Path(__file__).resolve())
_TRACE_EXCLUDED_SUFFIXES = (
    "/fnmatch.py",
    "/subprocess.py",
    "/tracemalloc.py",
)


def run_probe(
    iterations: int,
    history_limit: int,
    sample_every: int,
    *,
    churn_subscriptions: bool = False,
    churn_taints: bool = False,
    live_observers: int = 0,
    lineage_retention_policy: str = "none",
    payload_bytes: int = len(b"frame"),
    payload_schema: str = "bytes",
    publish_mode: str = "publish",
    unrelated_edges: int = 0,
    materialize_state: bool = False,
    metadata_mode: str = "unique",
    correlation_store: str = "noop",
    lineage_store: str = "native",
) -> tuple[_ProbeSample, ...]:
    return _run_worker_loop(
        iterations,
        history_limit,
        sample_every,
        churn_subscriptions=churn_subscriptions,
        churn_taints=churn_taints,
        live_observers=live_observers,
        lineage_retention_policy=lineage_retention_policy,
        payload_bytes=payload_bytes,
        payload_schema=payload_schema,
        publish_mode=publish_mode,
        unrelated_edges=unrelated_edges,
        materialize_state=materialize_state,
        metadata_mode=metadata_mode,
        correlation_store=correlation_store,
        lineage_store=lineage_store,
    )


def _build_stream(payload_schema: str = "bytes"):
    if payload_schema == "any":
        schema = Schema.any("MemoryProbeAny")
    elif payload_schema == "bytes":
        schema = Schema.bytes(name="MemoryProbeBytes")
    else:
        raise ValueError("payload_schema must be one of 'bytes' or 'any'")
    return route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner=OwnerName("heart"),
        family=StreamFamily("runtime"),
        stream=StreamName("memory_probe"),
        variant=Variant.Event,
        schema=schema,
    )


def _build_state_stream(payload_schema: str = "bytes"):
    if payload_schema == "any":
        schema = Schema.any("MemoryProbeStateAny")
    elif payload_schema == "bytes":
        schema = Schema.bytes(name="MemoryProbeStateBytes")
    else:
        raise ValueError("payload_schema must be one of 'bytes' or 'any'")
    return route(
        plane=Plane.State,
        layer=Layer.Logical,
        owner=OwnerName("heart"),
        family=StreamFamily("runtime"),
        stream=StreamName("memory_probe_state"),
        variant=Variant.State,
        schema=schema,
    )


def _probe_payload(index: int, payload_schema: str) -> object:
    if payload_schema == "any":
        return {"frame": index}
    return b"frame"


def _build_unrelated_stream(index: int, role: str):
    return route(
        plane=Plane.Read,
        layer=Layer.Logical,
        owner=OwnerName("topology"),
        family=StreamFamily("unrelated"),
        stream=StreamName(f"{role}_{index:05}"),
        variant=Variant.Event,
        schema=Schema.bytes(name="UnrelatedTopologyBytes"),
    )


def _install_unrelated_edges(graph: Graph, count: int) -> None:
    for index in range(count):
        graph.connect(
            source=_build_unrelated_stream(index, "source"),
            sink=_build_unrelated_stream(index, "sink"),
        )


def _current_rss_kib(pid: int) -> int:
    result = subprocess.run(
        ("ps", "-o", "rss=", "-p", str(pid)),
        check=True,
        capture_output=True,
        text=True,
    )
    return int(result.stdout.strip())


def _process_rows() -> tuple[_ProcessRow, ...]:
    result = subprocess.run(
        ("ps", "-axo", "pid=,ppid=,rss="),
        check=True,
        capture_output=True,
        text=True,
    )
    return _parse_process_rows(result.stdout)


def _parse_process_rows(text: str) -> tuple[_ProcessRow, ...]:
    rows: list[_ProcessRow] = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            rows.append((int(parts[0]), int(parts[1]), int(parts[2])))
        except ValueError:
            continue
    return tuple(rows)


def _descendant_pids(root_pid: int, rows: tuple[_ProcessRow, ...]) -> frozenset[int]:
    children_by_parent: dict[int, list[int]] = {}
    known_pids: set[int] = set()
    for pid, ppid, _rss_kib in rows:
        known_pids.add(pid)
        children_by_parent.setdefault(ppid, []).append(pid)
    if root_pid not in known_pids:
        raise ValueError(f"process {root_pid} not found")
    descendants: set[int] = set()
    pending = [root_pid]
    while pending:
        pid = pending.pop()
        if pid in descendants:
            continue
        descendants.add(pid)
        pending.extend(children_by_parent.get(pid, ()))
    return frozenset(descendants)


def _process_tree_rss_from_rows(root_pid: int, rows: tuple[_ProcessRow, ...]) -> int:
    pids = _descendant_pids(root_pid, rows)
    return sum(rss_kib for pid, _ppid, rss_kib in rows if pid in pids)


def _process_tree_rss_kib(root_pid: int) -> int:
    return _process_tree_rss_from_rows(root_pid, _process_rows())


def _parse_smaps_rollup(text: str) -> _ProcessMemory:
    values: dict[str, int] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        key = parts[0].rstrip(":")
        if key not in {"Anonymous", "Private_Clean", "Private_Dirty", "Pss", "Rss"}:
            continue
        try:
            values[key] = int(parts[1])
        except ValueError:
            continue
    return _ProcessMemory(
        rss_kib=values.get("Rss"),
        pss_kib=values.get("Pss"),
        private_kib=values.get("Private_Clean", 0) + values.get("Private_Dirty", 0),
        anonymous_kib=values.get("Anonymous"),
    )


def _process_smaps_rollup(pid: int) -> _ProcessMemory | None:
    try:
        return _parse_smaps_rollup(
            Path(f"/proc/{pid}/smaps_rollup").read_text(encoding="utf-8")
        )
    except OSError:
        return None


def _process_tree_memory_from_rows(
    root_pid: int,
    rows: tuple[_ProcessRow, ...],
    read_memory: _SmapsReader,
) -> _ProcessMemory | None:
    pids = _descendant_pids(root_pid, rows)
    total = _ProcessMemory()
    observed = False
    for pid in pids:
        memory = read_memory(pid)
        if memory is None:
            continue
        observed = True
        total = total.add(memory)
    return total if observed else None


def _process_tree_memory(pid: int) -> _ProcessMemory | None:
    return _process_tree_memory_from_rows(pid, _process_rows(), _process_smaps_rollup)


def _process_fd_count(pid: int) -> int | None:
    try:
        return sum(1 for _entry in Path(f"/proc/{pid}/fd").iterdir())
    except OSError:
        return None


def _process_tree_fd_count_from_rows(
    root_pid: int,
    rows: tuple[_ProcessRow, ...],
    read_count: _FdReader,
) -> int | None:
    pids = _descendant_pids(root_pid, rows)
    total = 0
    observed = False
    for pid in pids:
        count = read_count(pid)
        if count is None:
            continue
        observed = True
        total += count
    return total if observed else None


def _process_tree_fd_count(pid: int) -> int | None:
    return _process_tree_fd_count_from_rows(pid, _process_rows(), _process_fd_count)


def _external_rss_kib(pid: int, scope: str) -> int:
    if scope == "process":
        return _current_rss_kib(pid)
    if scope != "tree":
        raise ValueError("external RSS scope must be 'process' or 'tree'")
    try:
        return _process_tree_rss_kib(pid)
    except (subprocess.CalledProcessError, ValueError):
        return _current_rss_kib(pid)


def _external_process_memory(pid: int, scope: str) -> _ProcessMemory | None:
    if scope == "process":
        return _process_smaps_rollup(pid)
    if scope != "tree":
        raise ValueError("external RSS scope must be 'process' or 'tree'")
    try:
        return _process_tree_memory(pid)
    except (subprocess.CalledProcessError, ValueError):
        return _process_smaps_rollup(pid)


def _external_fd_count(pid: int, scope: str) -> int | None:
    if scope == "process":
        return _process_fd_count(pid)
    if scope != "tree":
        raise ValueError("external RSS scope must be 'process' or 'tree'")
    try:
        return _process_tree_fd_count(pid)
    except (subprocess.CalledProcessError, ValueError):
        return _process_fd_count(pid)


def _rss_native() -> int:
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def _resource_usage() -> _UsageSnapshot:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return _UsageSnapshot(
        cpu_seconds=usage.ru_utime + usage.ru_stime,
        input_blocks=int(usage.ru_inblock),
        output_blocks=int(usage.ru_oublock),
    )


def _child_resource_usage() -> _UsageSnapshot:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return _UsageSnapshot(
        cpu_seconds=usage.ru_utime + usage.ru_stime,
        input_blocks=int(usage.ru_inblock),
        output_blocks=int(usage.ru_oublock),
    )


def _usage_delta(
    baseline: _UsageSnapshot,
    current: _UsageSnapshot,
) -> _UsageSnapshot:
    return _UsageSnapshot(
        cpu_seconds=max(0.0, current.cpu_seconds - baseline.cpu_seconds),
        input_blocks=max(0, current.input_blocks - baseline.input_blocks),
        output_blocks=max(0, current.output_blocks - baseline.output_blocks),
    )


def _resource_usage_delta(baseline: _UsageSnapshot) -> _UsageSnapshot:
    return _usage_delta(baseline, _resource_usage())


def _sample(
    graph: Graph,
    route_ref,
    step: int,
    elapsed_seconds: float,
    interval_event_us: float,
    usage_baseline: _UsageSnapshot,
) -> _ProbeSample:
    gc.collect()
    current, peak = _runtime_traced_memory()
    retention = next(graph.retention_snapshot(route_ref))
    violations = tuple(graph.retention_violations())
    if violations:
        raise SystemExit(
            "retention invariant violations: " + "; ".join(violations)
        )
    usage_delta = _resource_usage_delta(usage_baseline)
    return _ProbeSample(
        step=step,
        replay_count=retention.replay_count,
        lineage_count=retention.lineage_count,
        correlation_index_count=retention.correlation_index_count,
        payload_count=retention.payload_count,
        violation_count=len(violations),
        subscriber_metadata_count=_subscriber_metadata_count(graph),
        taint_metadata_count=_taint_metadata_count(graph),
        materialized_payload_count=_materialized_payload_count(graph),
        traced_current_bytes=current,
        traced_peak_bytes=peak,
        current_rss_kib=_current_rss_kib(os.getpid()),
        max_rss_native=_rss_native(),
        elapsed_seconds=elapsed_seconds,
        average_event_us=(elapsed_seconds * 1_000_000.0 / step if step else 0.0),
        interval_event_us=interval_event_us,
        cpu_seconds=usage_delta.cpu_seconds,
        input_blocks=usage_delta.input_blocks,
        output_blocks=usage_delta.output_blocks,
    )


def _run_worker_loop(
    iterations: int,
    history_limit: int,
    sample_every: int,
    *,
    churn_subscriptions: bool,
    churn_taints: bool,
    live_observers: int,
    lineage_retention_policy: str,
    payload_bytes: int,
    payload_schema: str,
    publish_mode: str,
    unrelated_edges: int,
    materialize_state: bool,
    metadata_mode: str,
    correlation_store: str,
    lineage_store: str,
) -> tuple[_ProbeSample, ...]:
    if publish_mode not in ("publish", "nowait"):
        raise ValueError("publish_mode must be one of 'publish' or 'nowait'")
    if metadata_mode not in ("none", "static", "unique", "unique-all"):
        raise ValueError(
            "metadata_mode must be one of 'none', 'static', 'unique', or 'unique-all'"
        )
    if lineage_store not in ("native", "noop"):
        raise ValueError("lineage_store must be one of 'native' or 'noop'")
    if correlation_store not in ("native", "noop"):
        raise ValueError("correlation_store must be one of 'native' or 'noop'")
    if payload_bytes <= 0:
        raise ValueError("payload_bytes must be positive")
    stream = _build_stream(payload_schema)
    graph = Graph()
    if lineage_store == "noop":
        graph.attach(NoopLineageTracingStore())
    else:
        graph.attach(NativeLineageTracingStore())
    if correlation_store == "native":
        graph.attach(NativeCorrelationTracingStore())
    else:
        graph.attach(NoopCorrelationTracingStore())
    _install_unrelated_edges(graph, unrelated_edges)
    graph.configure_retention(
        stream,
        RouteRetentionPolicy(
            latest_replay_policy="bounded_history",
            replay_window=f"last_{history_limit}",
            history_limit=history_limit,
            lineage_retention_policy=lineage_retention_policy,
        ),
    )
    sample_route = stream
    materialize_subscription = None
    if materialize_state:
        sample_route = _build_state_stream(payload_schema)
        graph.configure_retention(
            sample_route,
            RouteRetentionPolicy(
                latest_replay_policy="bounded_history",
                replay_window=f"last_{history_limit}",
                history_limit=history_limit,
                lineage_retention_policy=lineage_retention_policy,
            ),
        )
        materialize_subscription = graph.materialize(stream, state_route=sample_route)
    live_subscriptions = tuple(
        graph.observe(
            sample_route,
            replay_latest=False,
            subscriber_id=f"live-subscriber-{index}",
        ).subscribe(lambda _envelope: None)
        for index in range(live_observers)
    )
    tracemalloc.start()
    usage_baseline = _resource_usage()
    start = time.monotonic()
    last_sample_step = 0
    last_sample_elapsed = 0.0
    samples: list[_ProbeSample] = []
    completed = False
    static_payload = (
        bytes(index % 251 for index in range(payload_bytes))
        if payload_schema == "bytes"
        else None
    )
    metadata_disabled = lineage_store == "noop" or metadata_mode == "none"
    static_metadata = (
        ("memory-probe", "memory-probe-chain", "memory-probe")
        if metadata_mode == "static" and not metadata_disabled
        else None
    )
    publish_nowait = graph.publish_nowait
    publish = graph.publish
    try:
        for index in range(iterations):
            payload = (
                static_payload
                if static_payload is not None
                else _probe_payload(index, payload_schema)
            )
            if static_metadata is not None:
                trace_id, causality_id, correlation_id = static_metadata
            elif metadata_mode == "unique":
                trace_id = "memory-probe"
                causality_id = "memory-probe-chain"
                correlation_id = f"probe-{index:020}"
            elif not metadata_disabled:
                trace_id = f"memory-probe-{index:020}"
                causality_id = f"memory-probe-chain-{index:020}"
                correlation_id = f"probe-{index:020}"
            if churn_subscriptions:
                subscription = graph.observe(
                    stream,
                    replay_latest=False,
                    subscriber_id=f"probe-subscriber-{index % 16}",
                ).subscribe(lambda _envelope: None)
                subscription.dispose()
                subscription.dispose()
            if publish_mode == "nowait":
                if metadata_disabled:
                    publish_nowait(stream, payload)
                else:
                    publish_nowait(
                        stream,
                        payload,
                        trace_id=trace_id,
                        causality_id=causality_id,
                        correlation_id=correlation_id,
                    )
            else:
                if metadata_disabled:
                    publish(stream, payload)
                else:
                    publish(
                        stream,
                        payload,
                        trace_id=trace_id,
                        causality_id=causality_id,
                        correlation_id=correlation_id,
                    )
            if churn_taints:
                latest = graph.latest(stream)
                if latest is None:
                    raise SystemExit("latest envelope missing while churning taints")
                latest_closed = latest.closed
                taint = TaintMark(
                    TaintDomain.Time,
                    f"PROBE_DYNAMIC_TIME_{index}",
                    stream.display(),
                )
                updated = graph._closed_with_taints(
                    latest_closed,
                    (*getattr(latest_closed, "taints", ()), taint),
                )
                graph._replace_recorded_envelope(updated)
                graph._remember_stream_taints(stream, updated)
            step = index + 1
            if step == history_limit or step % sample_every == 0 or step == iterations:
                elapsed_seconds = time.monotonic() - start
                interval_events = step - last_sample_step
                interval_elapsed = elapsed_seconds - last_sample_elapsed
                sample = _sample(
                    graph,
                    sample_route,
                    step,
                    elapsed_seconds,
                    (
                        interval_elapsed * 1_000_000.0 / interval_events
                        if interval_events > 0
                        else 0.0
                    ),
                    usage_baseline,
                )
                samples.append(sample)
                print(_format_probe_sample(sample), flush=True)
                last_sample_step = step
                last_sample_elapsed = elapsed_seconds
        completed = True
    finally:
        if materialize_subscription is not None:
            materialize_subscription.dispose()
        for subscription in live_subscriptions:
            subscription.dispose()
        if completed:
            retained_subscriber_metadata = _subscriber_metadata_count(graph)
            if retained_subscriber_metadata != 0:
                raise SystemExit(
                    "disposed subscriptions retained subscriber metadata "
                    f"{retained_subscriber_metadata} entries"
                )
    return tuple(samples)


def _run_graph_churn_loop(
    iterations: int,
    history_limit: int,
    sample_every: int,
) -> tuple[_ProbeSample, ...]:
    stream = _build_stream()
    tracemalloc.start()
    usage_baseline = _resource_usage()
    start = time.monotonic()
    last_sample_step = 0
    last_sample_elapsed = 0.0
    samples: list[_ProbeSample] = []
    for index in range(iterations):
        graph = Graph()
        graph.configure_retention(
            stream,
            RouteRetentionPolicy(
                latest_replay_policy="bounded_history",
                replay_window=f"last_{history_limit}",
                history_limit=history_limit,
            ),
        )
        subscription = graph.observe(
            stream,
            replay_latest=False,
            subscriber_id=f"graph-churn-{index % 16}",
        ).subscribe(lambda _envelope: None)
        graph.publish(
            stream,
            b"frame",
            trace_id="graph-churn",
            causality_id="graph-churn-chain",
            correlation_id=f"graph-{index:020}",
        )
        graph.dispose()
        subscription.dispose()
        step = index + 1
        if step == history_limit or step % sample_every == 0 or step == iterations:
            elapsed_seconds = time.monotonic() - start
            interval_events = step - last_sample_step
            interval_elapsed = elapsed_seconds - last_sample_elapsed
            sample = _empty_graph_sample(
                step,
                elapsed_seconds,
                (
                    interval_elapsed * 1_000_000.0 / interval_events
                    if interval_events > 0
                    else 0.0
                ),
                usage_baseline,
            )
            samples.append(sample)
            print(_format_probe_sample(sample), flush=True)
            last_sample_step = step
            last_sample_elapsed = elapsed_seconds
    return tuple(samples)


def _empty_graph_sample(
    step: int,
    elapsed_seconds: float,
    interval_event_us: float,
    usage_baseline: _UsageSnapshot,
) -> _ProbeSample:
    gc.collect()
    current, peak = _runtime_traced_memory()
    usage_delta = _resource_usage_delta(usage_baseline)
    return _ProbeSample(
        step=step,
        replay_count=0,
        lineage_count=0,
        correlation_index_count=0,
        payload_count=0,
        violation_count=0,
        subscriber_metadata_count=0,
        taint_metadata_count=0,
        materialized_payload_count=0,
        traced_current_bytes=current,
        traced_peak_bytes=peak,
        current_rss_kib=_current_rss_kib(os.getpid()),
        max_rss_native=_rss_native(),
        elapsed_seconds=elapsed_seconds,
        average_event_us=(elapsed_seconds * 1_000_000.0 / step if step else 0.0),
        interval_event_us=interval_event_us,
        cpu_seconds=usage_delta.cpu_seconds,
        input_blocks=usage_delta.input_blocks,
        output_blocks=usage_delta.output_blocks,
    )


def _format_probe_sample(sample: _ProbeSample) -> str:
    return (
        "step={step} replay={replay} lineage={lineage} "
        "correlation_index={correlation_index} payloads={payloads} "
        "violations={violations} subscriber_metadata={subscriber_metadata} "
        "taint_metadata={taint_metadata} "
        "materialized_payloads={materialized_payloads} "
        "traced_current={current} traced_peak={peak} "
        "current_rss_kib={current_rss} max_rss_native={max_rss} "
        "elapsed_seconds={elapsed:.3f} cpu_seconds={cpu:.3f} "
        "average_event_us={average_event_us:.3f} "
        "interval_event_us={interval_event_us:.3f} "
        "input_blocks={input_blocks} output_blocks={output_blocks}"
    ).format(
        step=sample.step,
        replay=sample.replay_count,
        lineage=sample.lineage_count,
        correlation_index=sample.correlation_index_count,
        payloads=sample.payload_count,
        violations=sample.violation_count,
        subscriber_metadata=sample.subscriber_metadata_count,
        taint_metadata=sample.taint_metadata_count,
        materialized_payloads=sample.materialized_payload_count,
        current=sample.traced_current_bytes,
        peak=sample.traced_peak_bytes,
        current_rss=sample.current_rss_kib,
        max_rss=sample.max_rss_native,
        elapsed=sample.elapsed_seconds,
        cpu=sample.cpu_seconds,
        average_event_us=sample.average_event_us,
        interval_event_us=sample.interval_event_us,
        input_blocks=sample.input_blocks,
        output_blocks=sample.output_blocks,
    )


def _runtime_traced_memory() -> tuple[int, int]:
    _current, peak = tracemalloc.get_traced_memory()
    snapshot = tracemalloc.take_snapshot()
    current = sum(
        stat.size
        for stat in snapshot.statistics("filename")
        if not _trace_filename_is_benchmark_overhead(stat.traceback[0].filename)
    )
    return current, peak


def _trace_filename_is_benchmark_overhead(filename: str) -> bool:
    if filename == "<frozen abc>" or filename == _BENCHMARK_TRACE_FILENAME:
        return True
    return (
        filename.endswith(_TRACE_EXCLUDED_SUFFIXES)
        or "/re/" in filename
    )


def _parse_probe_sample(line: str) -> _ProbeSample | None:
    if not line.startswith("step="):
        return None
    values = dict(item.split("=", 1) for item in line.split())
    return _ProbeSample(
        step=int(values["step"]),
        replay_count=int(values["replay"]),
        lineage_count=int(values["lineage"]),
        correlation_index_count=int(values.get("correlation_index", "0")),
        payload_count=int(values["payloads"]),
        violation_count=int(values["violations"]),
        subscriber_metadata_count=int(values["subscriber_metadata"]),
        taint_metadata_count=int(values["taint_metadata"]),
        materialized_payload_count=int(values.get("materialized_payloads", "0")),
        traced_current_bytes=int(values["traced_current"]),
        traced_peak_bytes=int(values["traced_peak"]),
        current_rss_kib=int(values["current_rss_kib"]),
        max_rss_native=int(values["max_rss_native"]),
        elapsed_seconds=float(values.get("elapsed_seconds", "0")),
        cpu_seconds=float(values.get("cpu_seconds", "0")),
        average_event_us=float(values.get("average_event_us", "0")),
        interval_event_us=float(values.get("interval_event_us", "0")),
        input_blocks=int(values.get("input_blocks", "0")),
        output_blocks=int(values.get("output_blocks", "0")),
    )


def _check_retained_counts(samples: tuple[_ProbeSample, ...], history_limit: int) -> None:
    _check_retained_counts_for_lineage(samples, history_limit, history_limit, 0, 0)


def _check_retained_counts_for_lineage(
    samples: tuple[_ProbeSample, ...],
    history_limit: int,
    expected_lineage_count: int,
    expected_correlation_index_count: int,
    expected_subscriber_metadata_count: int,
) -> None:
    final = samples[-1]
    if final.replay_count != history_limit:
        raise SystemExit(f"replay retained {final.replay_count}, expected {history_limit}")
    if final.lineage_count != expected_lineage_count:
        raise SystemExit(
            f"lineage retained {final.lineage_count}, expected {expected_lineage_count}"
        )
    if final.correlation_index_count != expected_correlation_index_count:
        raise SystemExit(
            "correlation index retained "
            f"{final.correlation_index_count}, expected "
            f"{expected_correlation_index_count}"
        )
    if final.payload_count != history_limit:
        raise SystemExit(f"payloads retained {final.payload_count}, expected {history_limit}")
    if final.violation_count != 0:
        raise SystemExit(f"retention violations reported: {final.violation_count}")
    if final.subscriber_metadata_count != expected_subscriber_metadata_count:
        raise SystemExit(
            "subscriber metadata retained "
            f"{final.subscriber_metadata_count} entries; expected "
            f"{expected_subscriber_metadata_count}"
        )
    taint_metadata_max = max(sample.taint_metadata_count for sample in samples)
    if taint_metadata_max > history_limit:
        raise SystemExit(
            "taint metadata retained "
            f"{taint_metadata_max} entries beyond history limit {history_limit}"
        )


def _check_disposed_graph_counts(samples: tuple[_ProbeSample, ...]) -> None:
    for sample in samples:
        retained = (
            sample.replay_count
            + sample.lineage_count
            + sample.payload_count
            + sample.violation_count
            + sample.subscriber_metadata_count
            + sample.taint_metadata_count
        )
        if retained != 0:
            raise SystemExit(
                f"disposed graph sample at step {sample.step} retained {retained} entries"
            )


def _check_elapsed_seconds(
    samples: tuple[_ProbeSample, ...],
    *,
    max_seconds: float,
) -> None:
    final = samples[-1]
    if final.elapsed_seconds > max_seconds:
        raise SystemExit(
            "benchmark elapsed time "
            f"{final.elapsed_seconds:.3f}s exceeds {max_seconds:.3f}s"
        )


def _check_cpu_seconds(
    samples: tuple[_ProbeSample, ...],
    *,
    max_seconds: float,
) -> None:
    final = samples[-1]
    if final.cpu_seconds > max_seconds:
        raise SystemExit(
            "benchmark CPU time "
            f"{final.cpu_seconds:.3f}s exceeds {max_seconds:.3f}s"
        )


def _check_average_event_latency(
    samples: tuple[_ProbeSample, ...],
    *,
    max_us: float,
) -> None:
    final = samples[-1]
    if final.average_event_us > max_us:
        raise SystemExit(
            "benchmark average event latency "
            f"{final.average_event_us:.3f}us exceeds {max_us:.3f}us"
        )


def _check_interval_event_latency(
    samples: tuple[_ProbeSample, ...],
    *,
    max_us: float,
) -> None:
    slowest = max(samples, key=lambda sample: sample.interval_event_us)
    if slowest.interval_event_us > max_us:
        raise SystemExit(
            "benchmark sampled interval event latency "
            f"{slowest.interval_event_us:.3f}us at step {slowest.step} "
            f"exceeds {max_us:.3f}us"
        )


def _check_block_io(
    samples: tuple[_ProbeSample, ...],
    *,
    max_input_blocks: int | None,
    max_output_blocks: int | None,
) -> None:
    final = samples[-1]
    if max_input_blocks is not None and final.input_blocks > max_input_blocks:
        raise SystemExit(
            "benchmark block input "
            f"{final.input_blocks} blocks exceeds {max_input_blocks}"
        )
    if max_output_blocks is not None and final.output_blocks > max_output_blocks:
        raise SystemExit(
            "benchmark block output "
            f"{final.output_blocks} blocks exceeds {max_output_blocks}"
        )


def _check_external_elapsed_seconds(
    elapsed_seconds: float,
    *,
    max_seconds: float,
) -> None:
    if elapsed_seconds > max_seconds:
        raise SystemExit(
            "external command elapsed time "
            f"{elapsed_seconds:.3f}s exceeds {max_seconds:.3f}s"
        )


def _check_external_min_elapsed_seconds(
    elapsed_seconds: float,
    *,
    min_seconds: float,
) -> None:
    if elapsed_seconds < min_seconds:
        raise SystemExit(
            "external command elapsed time "
            f"{elapsed_seconds:.3f}s is below required {min_seconds:.3f}s"
        )


def _check_external_min_samples(
    samples: tuple["_RssSample", ...],
    *,
    min_samples: int,
) -> None:
    if len(samples) < min_samples:
        raise SystemExit(
            "external command collected "
            f"{len(samples)} samples, below required {min_samples}"
        )


def _check_external_cpu_seconds(
    usage: _UsageSnapshot,
    *,
    max_seconds: float,
) -> None:
    if usage.cpu_seconds > max_seconds:
        raise SystemExit(
            "external command CPU time "
            f"{usage.cpu_seconds:.3f}s exceeds {max_seconds:.3f}s"
        )


def _check_external_block_io(
    usage: _UsageSnapshot,
    *,
    max_input_blocks: int | None,
    max_output_blocks: int | None,
) -> None:
    if max_input_blocks is not None and usage.input_blocks > max_input_blocks:
        raise SystemExit(
            "external command block input "
            f"{usage.input_blocks} blocks exceeds {max_input_blocks}"
        )
    if max_output_blocks is not None and usage.output_blocks > max_output_blocks:
        raise SystemExit(
            "external command block output "
            f"{usage.output_blocks} blocks exceeds {max_output_blocks}"
        )


def _check_materialized_payload_count(
    samples: tuple[_ProbeSample, ...],
    *,
    max_count: int,
) -> None:
    final = samples[-1]
    if final.materialized_payload_count > max_count:
        raise SystemExit(
            "benchmark materialized Python payloads "
            f"{final.materialized_payload_count} exceeds {max_count}"
        )


def _subscriber_metadata_count(graph: Graph) -> int:
    counts = getattr(graph, "_subscriber_count", {})
    subscribers = getattr(graph, "_route_subscribers", {})
    subscriber_refs = getattr(graph, "_route_subscriber_refs", {})
    subjects = getattr(graph, "_subjects", {})
    direct_subscribers = getattr(graph, "_direct_envelope_subscribers", {})
    direct_snapshots = getattr(graph, "_direct_envelope_snapshots", {})
    native_materializers = getattr(graph, "_native_materialize_edges_by_source", {})
    return (
        len(counts)
        + len(subscribers)
        + sum(len(route_subscribers) for route_subscribers in subscribers.values())
        + len(subscriber_refs)
        + sum(len(refs) for refs in subscriber_refs.values())
        + len(subjects)
        + len(direct_subscribers)
        + sum(len(refs) for refs in direct_subscribers.values())
        + len(direct_snapshots)
        + len(native_materializers)
        + sum(len(targets) for targets in native_materializers.values())
    )


def _taint_metadata_count(graph: Graph) -> int:
    return sum(
        len(taints)
        for taints in getattr(graph, "_stream_taint_upper_bounds", {}).values()
    )


def _materialized_payload_count(graph: Graph) -> int:
    return len(getattr(graph, "_materialized_payloads", {}))


def _optional_kib(value: int | None) -> str:
    return "unavailable" if value is None else str(value)


def _optional_count(value: int | None) -> str:
    return "unavailable" if value is None else str(value)


def _sum_optional_kib(left: int | None, right: int | None) -> int | None:
    if left is None:
        return right
    if right is None:
        return left
    return left + right


def _check_external_rss_plateau(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    plateau_kib: int,
) -> None:
    if not samples:
        raise SystemExit("no external RSS samples collected")
    first_steady_index = max(0, int(len(samples) * warmup_fraction))
    steady = samples[first_steady_index:]
    if len(steady) < 2:
        return
    rss_values = tuple(sample.current_rss_kib for sample in steady)
    rss_range = max(rss_values) - min(rss_values)
    if rss_range > plateau_kib:
        raise SystemExit(
            "external RSS did not plateau: steady-state range "
            f"{rss_range} KiB exceeds {plateau_kib} KiB"
        )


def _final_tail_plateau(
    samples: tuple[_RssSample, ...],
    *,
    plateau: int,
    value: Callable[[_RssSample], int | None],
) -> dict[str, float | int | None]:
    observed = tuple(
        (sample.elapsed_seconds, sample_value)
        for sample in samples
        if (sample_value := value(sample)) is not None
    )
    if not observed:
        return {"range": None, "sample_count": 0, "seconds": 0.0}
    first_index = len(observed) - 1
    minimum = observed[first_index][1]
    maximum = minimum
    while first_index > 0:
        previous_value = observed[first_index - 1][1]
        next_minimum = min(minimum, previous_value)
        next_maximum = max(maximum, previous_value)
        if next_maximum - next_minimum > plateau:
            break
        first_index -= 1
        minimum = next_minimum
        maximum = next_maximum
    tail = observed[first_index:]
    seconds = tail[-1][0] - tail[0][0] if len(tail) >= 2 else 0.0
    return {
        "range": maximum - minimum,
        "sample_count": len(tail),
        "seconds": seconds,
    }


def _check_external_tail_plateau(
    samples: tuple[_RssSample, ...],
    *,
    plateau: int,
    min_seconds: float,
    min_samples: int,
    label: str,
    value: Callable[[_RssSample], int | None],
) -> None:
    tail = _final_tail_plateau(samples, plateau=plateau, value=value)
    if tail["range"] is None:
        raise SystemExit(f"no external {label} samples collected")
    if tail["range"] > plateau:
        raise SystemExit(
            f"external {label} final tail range {tail['range']} exceeds {plateau}"
        )
    if tail["seconds"] < min_seconds:
        raise SystemExit(
            f"external {label} final tail plateau lasted "
            f"{tail['seconds']:.3f}s, below required {min_seconds:.3f}s"
        )
    if tail["sample_count"] < min_samples:
        raise SystemExit(
            f"external {label} final tail plateau collected "
            f"{tail['sample_count']} samples, below required {min_samples}"
        )


def _check_external_memory_plateau(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    plateau_kib: int,
    label: str,
    value: Callable[[_RssSample], int | None],
) -> None:
    if not samples:
        raise SystemExit(f"no external {label} samples collected")
    first_steady_index = max(0, int(len(samples) * warmup_fraction))
    steady = samples[first_steady_index:]
    values = tuple(sample_value for sample in steady if (sample_value := value(sample)) is not None)
    if not values:
        raise SystemExit(f"no external {label} samples collected")
    if len(values) < 2:
        return
    value_range = max(values) - min(values)
    if value_range > plateau_kib:
        raise SystemExit(
            f"external {label} did not plateau: steady-state range "
            f"{value_range} KiB exceeds {plateau_kib} KiB"
        )


def _check_external_count_plateau(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    plateau_count: int,
    label: str,
    value: Callable[[_RssSample], int | None],
) -> None:
    if not samples:
        raise SystemExit(f"no external {label} samples collected")
    first_steady_index = max(0, int(len(samples) * warmup_fraction))
    steady = samples[first_steady_index:]
    values = tuple(sample_value for sample in steady if (sample_value := value(sample)) is not None)
    if not values:
        raise SystemExit(f"no external {label} samples collected")
    if len(values) < 2:
        return
    value_range = max(values) - min(values)
    if value_range > plateau_count:
        raise SystemExit(
            f"external {label} did not plateau: steady-state range "
            f"{value_range} exceeds {plateau_count}"
        )


def _projected_rss_growth_over_seconds(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    project_seconds: float,
) -> int:
    first_steady_index = max(0, int(len(samples) * warmup_fraction))
    steady = samples[first_steady_index:]
    if len(steady) < 2:
        return 0
    first = steady[0]
    last = steady[-1]
    elapsed = last.elapsed_seconds - first.elapsed_seconds
    if elapsed <= 0:
        return 0
    growth = last.current_rss_kib - first.current_rss_kib
    if growth <= 0:
        return 0
    return int((growth / elapsed) * project_seconds)


def _projected_external_growth_over_seconds(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    project_seconds: float,
    value: Callable[[_RssSample], int | None],
) -> int:
    first_steady_index = max(0, int(len(samples) * warmup_fraction))
    steady = tuple(
        (sample, sample_value)
        for sample in samples[first_steady_index:]
        if (sample_value := value(sample)) is not None
    )
    if len(steady) < 2:
        return 0
    first, first_value = steady[0]
    last, last_value = steady[-1]
    elapsed = last.elapsed_seconds - first.elapsed_seconds
    if elapsed <= 0:
        return 0
    growth = last_value - first_value
    if growth <= 0:
        return 0
    return int((growth / elapsed) * project_seconds)


def _projected_rss_segment_growth_over_seconds(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    project_seconds: float,
) -> int:
    first_steady_index = max(0, int(len(samples) * warmup_fraction))
    steady = samples[first_steady_index:]
    if len(steady) < 2:
        return 0
    projected = 0
    for first, last in zip(steady, steady[1:]):
        elapsed = last.elapsed_seconds - first.elapsed_seconds
        if elapsed <= 0:
            continue
        growth = last.current_rss_kib - first.current_rss_kib
        if growth > 0:
            projected = max(projected, int((growth / elapsed) * project_seconds))
    return projected


def _projected_external_segment_growth_over_seconds(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    project_seconds: float,
    value: Callable[[_RssSample], int | None],
) -> int:
    first_steady_index = max(0, int(len(samples) * warmup_fraction))
    steady = tuple(
        (sample, sample_value)
        for sample in samples[first_steady_index:]
        if (sample_value := value(sample)) is not None
    )
    if len(steady) < 2:
        return 0
    projected = 0
    for first, last in zip(steady, steady[1:]):
        first_sample, first_value = first
        last_sample, last_value = last
        elapsed = last_sample.elapsed_seconds - first_sample.elapsed_seconds
        if elapsed <= 0:
            continue
        growth = last_value - first_value
        if growth > 0:
            projected = max(projected, int((growth / elapsed) * project_seconds))
    return projected


def _check_external_projected_rss_growth(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    project_seconds: float,
    limit_kib: int,
) -> None:
    projected = _projected_rss_growth_over_seconds(
        samples,
        warmup_fraction=warmup_fraction,
        project_seconds=project_seconds,
    )
    if projected > limit_kib:
        raise SystemExit(
            "external RSS projects to "
            f"{projected} KiB over {project_seconds:.3f}s, exceeding "
            f"{limit_kib} KiB"
        )


def _check_external_projected_memory_growth(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    project_seconds: float,
    limit_kib: int,
    label: str,
    value: Callable[[_RssSample], int | None],
) -> None:
    if not any(value(sample) is not None for sample in samples):
        raise SystemExit(f"no external {label} samples collected")
    projected = _projected_external_growth_over_seconds(
        samples,
        warmup_fraction=warmup_fraction,
        project_seconds=project_seconds,
        value=value,
    )
    if projected > limit_kib:
        raise SystemExit(
            f"external {label} projects to "
            f"{projected} KiB over {project_seconds:.3f}s, exceeding "
            f"{limit_kib} KiB"
        )


def _check_external_projected_count_growth(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    project_seconds: float,
    limit_count: int,
    label: str,
    value: Callable[[_RssSample], int | None],
) -> None:
    if not any(value(sample) is not None for sample in samples):
        raise SystemExit(f"no external {label} samples collected")
    projected = _projected_external_growth_over_seconds(
        samples,
        warmup_fraction=warmup_fraction,
        project_seconds=project_seconds,
        value=value,
    )
    if projected > limit_count:
        raise SystemExit(
            f"external {label} projects to "
            f"{projected} over {project_seconds:.3f}s, exceeding "
            f"{limit_count}"
        )


def _check_external_segment_projected_rss_growth(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    project_seconds: float,
    limit_kib: int,
) -> None:
    projected = _projected_rss_segment_growth_over_seconds(
        samples,
        warmup_fraction=warmup_fraction,
        project_seconds=project_seconds,
    )
    if projected > limit_kib:
        raise SystemExit(
            "external RSS segment projects to "
            f"{projected} KiB over {project_seconds:.3f}s, exceeding "
            f"{limit_kib} KiB"
        )


def _check_external_segment_projected_memory_growth(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    project_seconds: float,
    limit_kib: int,
    label: str,
    value: Callable[[_RssSample], int | None],
) -> None:
    if not any(value(sample) is not None for sample in samples):
        raise SystemExit(f"no external {label} samples collected")
    projected = _projected_external_segment_growth_over_seconds(
        samples,
        warmup_fraction=warmup_fraction,
        project_seconds=project_seconds,
        value=value,
    )
    if projected > limit_kib:
        raise SystemExit(
            f"external {label} segment projects to "
            f"{projected} KiB over {project_seconds:.3f}s, exceeding "
            f"{limit_kib} KiB"
        )


def _check_external_segment_projected_count_growth(
    samples: tuple[_RssSample, ...],
    *,
    warmup_fraction: float,
    project_seconds: float,
    limit_count: int,
    label: str,
    value: Callable[[_RssSample], int | None],
) -> None:
    if not any(value(sample) is not None for sample in samples):
        raise SystemExit(f"no external {label} samples collected")
    projected = _projected_external_segment_growth_over_seconds(
        samples,
        warmup_fraction=warmup_fraction,
        project_seconds=project_seconds,
        value=value,
    )
    if projected > limit_count:
        raise SystemExit(
            f"external {label} segment projects to "
            f"{projected} over {project_seconds:.3f}s, exceeding "
            f"{limit_count}"
        )


def _check_external_named_memory_gates(
    samples: tuple[_RssSample, ...],
    args: argparse.Namespace,
    *,
    attr_prefix: str,
    label: str,
    value: Callable[[_RssSample], int | None],
) -> None:
    plateau_kib = getattr(args, f"external_{attr_prefix}_plateau_kib", None)
    projected_growth_kib = getattr(
        args,
        f"external_{attr_prefix}_projected_growth_kib",
        None,
    )
    segment_projected_growth_kib = getattr(
        args,
        f"external_{attr_prefix}_segment_projected_growth_kib",
        None,
    )
    if plateau_kib is not None:
        _check_external_memory_plateau(
            samples,
            warmup_fraction=args.rss_warmup_fraction,
            plateau_kib=plateau_kib,
            label=label,
            value=value,
        )
    if projected_growth_kib is not None:
        _check_external_projected_memory_growth(
            samples,
            warmup_fraction=args.rss_warmup_fraction,
            project_seconds=args.external_project_seconds,
            limit_kib=projected_growth_kib,
            label=label,
            value=value,
        )
    if segment_projected_growth_kib is not None:
        _check_external_segment_projected_memory_growth(
            samples,
            warmup_fraction=args.rss_warmup_fraction,
            project_seconds=args.external_project_seconds,
            limit_kib=segment_projected_growth_kib,
            label=label,
            value=value,
        )


def _check_external_named_count_gates(
    samples: tuple[_RssSample, ...],
    args: argparse.Namespace,
    *,
    attr_prefix: str,
    label: str,
    value: Callable[[_RssSample], int | None],
) -> None:
    plateau_count = getattr(args, f"external_{attr_prefix}_plateau_count", None)
    projected_growth_count = getattr(
        args,
        f"external_{attr_prefix}_projected_growth_count",
        None,
    )
    segment_projected_growth_count = getattr(
        args,
        f"external_{attr_prefix}_segment_projected_growth_count",
        None,
    )
    if plateau_count is not None:
        _check_external_count_plateau(
            samples,
            warmup_fraction=args.rss_warmup_fraction,
            plateau_count=plateau_count,
            label=label,
            value=value,
        )
    if projected_growth_count is not None:
        _check_external_projected_count_growth(
            samples,
            warmup_fraction=args.rss_warmup_fraction,
            project_seconds=args.external_project_seconds,
            limit_count=projected_growth_count,
            label=label,
            value=value,
        )
    if segment_projected_growth_count is not None:
        _check_external_segment_projected_count_growth(
            samples,
            warmup_fraction=args.rss_warmup_fraction,
            project_seconds=args.external_project_seconds,
            limit_count=segment_projected_growth_count,
            label=label,
            value=value,
        )


def _check_traced_current_plateau(
    samples: tuple[_ProbeSample, ...],
    *,
    warmup_fraction: float,
    plateau_bytes: int,
) -> None:
    first_steady_index = max(0, int(len(samples) * warmup_fraction))
    steady = samples[first_steady_index:]
    if len(steady) < 2:
        return
    traced_values = tuple(sample.traced_current_bytes for sample in steady)
    traced_range = max(traced_values) - min(traced_values)
    if traced_range > plateau_bytes:
        raise SystemExit(
            "tracemalloc current bytes did not plateau: steady-state range "
            f"{traced_range} exceeds {plateau_bytes}"
        )


def _projected_growth_over_events(
    samples: tuple[_ProbeSample, ...],
    *,
    warmup_fraction: float,
    project_events: int,
    value: Callable[[_ProbeSample], int],
) -> int:
    first_steady_index = max(0, int(len(samples) * warmup_fraction))
    steady = samples[first_steady_index:]
    if len(steady) < 2:
        return 0
    first = steady[0]
    last = steady[-1]
    event_span = last.step - first.step
    if event_span <= 0:
        return 0
    growth = value(last) - value(first)
    if growth <= 0:
        return 0
    return int((growth / event_span) * project_events)


def _projected_growth_over_event_segments(
    samples: tuple[_ProbeSample, ...],
    *,
    warmup_fraction: float,
    project_events: int,
    value: Callable[[_ProbeSample], int],
) -> int:
    first_steady_index = max(0, int(len(samples) * warmup_fraction))
    steady = samples[first_steady_index:]
    if len(steady) < 2:
        return 0
    projected = 0
    for first, last in zip(steady, steady[1:]):
        event_span = last.step - first.step
        if event_span <= 0:
            continue
        growth = value(last) - value(first)
        if growth > 0:
            projected = max(projected, int((growth / event_span) * project_events))
    return projected


def _check_projected_traced_growth(
    samples: tuple[_ProbeSample, ...],
    *,
    warmup_fraction: float,
    project_events: int,
    limit_bytes: int,
) -> None:
    projected = _projected_growth_over_events(
        samples,
        warmup_fraction=warmup_fraction,
        project_events=project_events,
        value=lambda sample: sample.traced_current_bytes,
    )
    if projected > limit_bytes:
        raise SystemExit(
            "tracemalloc current bytes project to "
            f"{projected} bytes over {project_events} events, exceeding "
            f"{limit_bytes}"
        )


def _check_segment_projected_traced_growth(
    samples: tuple[_ProbeSample, ...],
    *,
    warmup_fraction: float,
    project_events: int,
    limit_bytes: int,
) -> None:
    projected = _projected_growth_over_event_segments(
        samples,
        warmup_fraction=warmup_fraction,
        project_events=project_events,
        value=lambda sample: sample.traced_current_bytes,
    )
    if projected > limit_bytes:
        raise SystemExit(
            "tracemalloc current byte segment projects to "
            f"{projected} bytes over {project_events} events, exceeding "
            f"{limit_bytes}"
        )


def _check_projected_rss_growth(
    samples: tuple[_ProbeSample, ...],
    *,
    warmup_fraction: float,
    project_events: int,
    limit_kib: int,
) -> None:
    projected = _projected_growth_over_events(
        samples,
        warmup_fraction=warmup_fraction,
        project_events=project_events,
        value=lambda sample: sample.current_rss_kib,
    )
    if projected > limit_kib:
        raise SystemExit(
            "current RSS projects to "
            f"{projected} KiB over {project_events} events, exceeding "
            f"{limit_kib} KiB"
        )


def _check_segment_projected_rss_growth(
    samples: tuple[_ProbeSample, ...],
    *,
    warmup_fraction: float,
    project_events: int,
    limit_kib: int,
) -> None:
    projected = _projected_growth_over_event_segments(
        samples,
        warmup_fraction=warmup_fraction,
        project_events=project_events,
        value=lambda sample: sample.current_rss_kib,
    )
    if projected > limit_kib:
        raise SystemExit(
            "current RSS segment projects to "
            f"{projected} KiB over {project_events} events, exceeding "
            f"{limit_kib} KiB"
        )


def _final_probe_tail_plateau(
    samples: tuple[_ProbeSample, ...],
    *,
    plateau: int,
    value: Callable[[_ProbeSample], int],
) -> dict[str, int | None]:
    if not samples:
        return {"range": None, "sample_count": 0}
    first_index = len(samples) - 1
    minimum = value(samples[first_index])
    maximum = minimum
    while first_index > 0:
        previous_value = value(samples[first_index - 1])
        next_minimum = min(minimum, previous_value)
        next_maximum = max(maximum, previous_value)
        if next_maximum - next_minimum > plateau:
            break
        first_index -= 1
        minimum = next_minimum
        maximum = next_maximum
    return {"range": maximum - minimum, "sample_count": len(samples) - first_index}


def _check_probe_tail_plateau(
    samples: tuple[_ProbeSample, ...],
    *,
    plateau: int,
    min_samples: int,
    label: str,
    value: Callable[[_ProbeSample], int],
) -> None:
    tail = _final_probe_tail_plateau(samples, plateau=plateau, value=value)
    if tail["range"] is None:
        raise SystemExit(f"no {label} samples collected")
    if tail["sample_count"] < min_samples:
        raise SystemExit(
            f"{label} final tail plateau collected "
            f"{tail['sample_count']} samples, below required {min_samples}"
        )


def _run_external_monitor(args: argparse.Namespace) -> tuple[_ProbeSample, ...]:
    command = (
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--iterations",
        str(args.iterations),
        "--history-limit",
        str(args.history_limit),
        "--sample-every",
        str(args.sample_every),
        "--lineage-retention",
        args.lineage_retention,
        "--lineage-store",
        args.lineage_store,
        "--correlation-store",
        args.correlation_store,
        "--payload-bytes",
        str(args.payload_bytes),
        "--payload-schema",
        args.payload_schema,
        "--publish-mode",
        args.publish_mode,
        "--metadata-mode",
        args.metadata_mode,
    )
    if args.unrelated_edges:
        command = (*command, "--unrelated-edges", str(args.unrelated_edges))
    if args.materialize_state:
        command = (*command, "--materialize-state")
    if args.live_observers:
        command = (*command, "--live-observers", str(args.live_observers))
    if args.churn_subscriptions:
        command = (*command, "--churn-subscriptions")
    if args.churn_taints:
        command = (*command, "--churn-taints")
    if args.churn_graphs:
        command = (*command, "--churn-graphs")
    with tempfile.TemporaryFile(mode="w+") as stdout_file:
        with tempfile.TemporaryFile(mode="w+") as stderr_file:
            process = subprocess.Popen(
                command,
                stdout=stdout_file,
                stderr=stderr_file,
                text=True,
            )
            rss_samples = _RssSampleSeries()
            start = time.monotonic()
            while process.poll() is None:
                try:
                    rss_samples.append(
                        elapsed_seconds=time.monotonic() - start,
                        current_rss_kib=_current_rss_kib(process.pid),
                    )
                except (subprocess.CalledProcessError, ValueError):
                    pass
                time.sleep(args.monitor_interval)
            process.wait()
            stdout_file.seek(0)
            stderr_file.seek(0)
            stdout = stdout_file.read()
            stderr = stderr_file.read()
    if process.returncode != 0:
        raise SystemExit(stderr or f"worker failed with exit code {process.returncode}")
    probe_samples = tuple(
        sample
        for line in stdout.splitlines()
        if (sample := _parse_probe_sample(line)) is not None
    )
    for line in stdout.splitlines():
        print(line)
    for sample in rss_samples:
        print(
            "external_rss elapsed={elapsed:.3f} current_rss_kib={rss}".format(
                elapsed=sample.elapsed_seconds,
                rss=sample.current_rss_kib,
            )
        )
    if args.check:
        if args.churn_graphs:
            _check_disposed_graph_counts(probe_samples)
        else:
            expected_lineage = (
                args.history_limit
                if (
                    args.lineage_retention == "retained"
                    and args.lineage_store == "native"
                )
                else 0
            )
            _check_retained_counts_for_lineage(
                probe_samples,
                args.history_limit,
                expected_lineage,
                _expected_correlation_index_count(
                    args.history_limit,
                    lineage_retention=args.lineage_retention,
                    lineage_store=args.lineage_store,
                    correlation_store=args.correlation_store,
                    metadata_mode=args.metadata_mode,
                ),
                _expected_subscriber_metadata_count(
                    args.live_observers,
                    materialize_state=args.materialize_state,
                ),
            )
        _check_traced_current_plateau(
            probe_samples,
            warmup_fraction=args.traced_warmup_fraction,
            plateau_bytes=args.traced_plateau_bytes,
        )
        if args.traced_projected_growth_bytes is not None:
            _check_projected_traced_growth(
                probe_samples,
                warmup_fraction=args.traced_warmup_fraction,
                project_events=args.project_events,
                limit_bytes=args.traced_projected_growth_bytes,
            )
        if args.traced_segment_projected_growth_bytes is not None:
            _check_segment_projected_traced_growth(
                probe_samples,
                warmup_fraction=args.traced_warmup_fraction,
                project_events=args.project_events,
                limit_bytes=args.traced_segment_projected_growth_bytes,
            )
        if args.rss_projected_growth_kib is not None:
            _check_projected_rss_growth(
                probe_samples,
                warmup_fraction=args.rss_warmup_fraction,
                project_events=args.project_events,
                limit_kib=args.rss_projected_growth_kib,
            )
        if args.rss_segment_projected_growth_kib is not None:
            _check_segment_projected_rss_growth(
                probe_samples,
                warmup_fraction=args.rss_warmup_fraction,
                project_events=args.project_events,
                limit_kib=args.rss_segment_projected_growth_kib,
            )
        if args.rss_tail_plateau_kib is not None:
            _check_probe_tail_plateau(
                probe_samples,
                plateau=args.rss_tail_plateau_kib,
                min_samples=args.rss_tail_min_samples,
                label="current RSS",
                value=lambda sample: sample.current_rss_kib,
            )
        if args.max_elapsed_seconds is not None:
            _check_elapsed_seconds(
                probe_samples,
                max_seconds=args.max_elapsed_seconds,
            )
        if args.max_cpu_seconds is not None:
            _check_cpu_seconds(
                probe_samples,
                max_seconds=args.max_cpu_seconds,
            )
        if args.max_average_event_us is not None:
            _check_average_event_latency(
                probe_samples,
                max_us=args.max_average_event_us,
            )
        if args.max_interval_event_us is not None:
            _check_interval_event_latency(
                probe_samples,
                max_us=args.max_interval_event_us,
            )
        _check_block_io(
            probe_samples,
            max_input_blocks=args.max_disk_input_blocks,
            max_output_blocks=args.max_disk_output_blocks,
        )
        if args.max_materialized_payloads is not None:
            _check_materialized_payload_count(
                probe_samples,
                max_count=args.max_materialized_payloads,
            )
        _check_external_rss_plateau(
            rss_samples,
            warmup_fraction=args.rss_warmup_fraction,
            plateau_kib=args.rss_plateau_kib,
        )
    return probe_samples


def _external_command_from_args(args: argparse.Namespace) -> tuple[str, ...]:
    command = tuple(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise SystemExit("--external-command requires a command after --")
    return command


def _external_popen_kwargs(terminate_scope: str) -> dict[str, bool]:
    if terminate_scope == "tree" and os.name == "posix":
        return {"start_new_session": True}
    return {}


def _external_popen_options(
    args: argparse.Namespace,
    terminate_scope: str,
) -> dict[str, object]:
    options = dict(_external_popen_kwargs(terminate_scope))
    external_cwd = getattr(args, "external_cwd", None)
    if external_cwd is not None:
        options["cwd"] = external_cwd
    external_env = getattr(args, "external_env", None)
    if external_env is not None:
        options["env"] = external_env
    return options


def _terminate_external_process(
    process: subprocess.Popen[object],
    *,
    scope: str,
    sig: signal.Signals,
) -> None:
    if scope == "tree" and os.name == "posix":
        try:
            os.killpg(process.pid, sig)
            return
        except ProcessLookupError:
            return
        except PermissionError:
            pass
    if sig == signal.SIGKILL:
        process.kill()
    else:
        process.terminate()


def _stop_external_process(
    process: subprocess.Popen[object],
    *,
    scope: str,
    timeout_seconds: float,
) -> None:
    _terminate_external_process(process, scope=scope, sig=signal.SIGTERM)
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        _terminate_external_process(process, scope=scope, sig=signal.SIGKILL)
        process.wait()


def _run_external_command_monitor(args: argparse.Namespace) -> tuple[_RssSample, ...]:
    command = _external_command_from_args(args)
    usage_baseline = _child_resource_usage()
    terminate_scope = getattr(args, "external_terminate_scope", "tree")
    process = subprocess.Popen(command, **_external_popen_options(args, terminate_scope))
    rss_samples = _RssSampleSeries()
    start = time.monotonic()
    terminated_by_monitor = False
    try:
        while process.poll() is None:
            elapsed = time.monotonic() - start
            try:
                scope = getattr(args, "external_rss_scope", "tree")
                memory = _external_process_memory(process.pid, scope)
                fd_count = _external_fd_count(process.pid, scope)
                rss_samples.append(
                    elapsed_seconds=elapsed,
                    current_rss_kib=_external_rss_kib(process.pid, scope),
                    pss_kib=None if memory is None else memory.pss_kib,
                    private_kib=None if memory is None else memory.private_kib,
                    anonymous_kib=None if memory is None else memory.anonymous_kib,
                    fd_count=fd_count,
                )
            except (subprocess.CalledProcessError, ValueError):
                pass
            if (
                args.external_duration_seconds is not None
                and elapsed >= args.external_duration_seconds
            ):
                terminated_by_monitor = True
                _stop_external_process(
                    process,
                    scope=terminate_scope,
                    timeout_seconds=args.external_shutdown_timeout_seconds,
                )
                break
            time.sleep(args.monitor_interval)
    finally:
        if process.poll() is None:
            _stop_external_process(
                process,
                scope=terminate_scope,
                timeout_seconds=args.external_shutdown_timeout_seconds,
            )
    elapsed = time.monotonic() - start
    usage_delta = _usage_delta(usage_baseline, _child_resource_usage())
    samples = rss_samples
    output_json = getattr(args, "external_output_json", None)
    if output_json is not None:
        _write_external_monitor_summary(
            Path(output_json),
            command=command,
            check_enabled=args.check,
            gate_args=args,
            metadata=getattr(args, "external_metadata", None),
            samples=samples,
            output_max_samples=getattr(args, "external_output_max_samples", 512),
            elapsed_seconds=elapsed,
            project_seconds=args.external_project_seconds,
            warmup_fraction=args.rss_warmup_fraction,
            usage=usage_delta,
            returncode=process.returncode,
            terminated_by_monitor=terminated_by_monitor,
        )
    for sample in rss_samples:
        print(
            "external_rss elapsed={elapsed:.3f} current_rss_kib={rss} "
            "pss_kib={pss} private_kib={private} anonymous_kib={anonymous} "
            "fd_count={fds}".format(
                elapsed=sample.elapsed_seconds,
                rss=sample.current_rss_kib,
                pss=_optional_kib(sample.pss_kib),
                private=_optional_kib(sample.private_kib),
                anonymous=_optional_kib(sample.anonymous_kib),
                fds=_optional_count(sample.fd_count),
            )
        )
    print(
        "external_command_status returncode={returncode} elapsed_seconds={elapsed:.3f} "
        "cpu_seconds={cpu:.3f} input_blocks={input_blocks} "
        "output_blocks={output_blocks} terminated_by_monitor={terminated}".format(
            returncode=process.returncode,
            elapsed=elapsed,
            cpu=usage_delta.cpu_seconds,
            input_blocks=usage_delta.input_blocks,
            output_blocks=usage_delta.output_blocks,
            terminated=terminated_by_monitor,
        )
    )
    if process.returncode not in (0, None) and not terminated_by_monitor:
        raise SystemExit(f"external command failed with exit code {process.returncode}")
    if args.check:
        if getattr(args, "external_min_samples", None) is not None:
            _check_external_min_samples(
                samples,
                min_samples=args.external_min_samples,
            )
        _check_external_rss_plateau(
            samples,
            warmup_fraction=args.rss_warmup_fraction,
            plateau_kib=args.rss_plateau_kib,
        )
        if args.external_rss_projected_growth_kib is not None:
            _check_external_projected_rss_growth(
                samples,
                warmup_fraction=args.rss_warmup_fraction,
                project_seconds=args.external_project_seconds,
                limit_kib=args.external_rss_projected_growth_kib,
            )
        if args.external_rss_segment_projected_growth_kib is not None:
            _check_external_segment_projected_rss_growth(
                samples,
                warmup_fraction=args.rss_warmup_fraction,
                project_seconds=args.external_project_seconds,
                limit_kib=args.external_rss_segment_projected_growth_kib,
            )
        if getattr(args, "external_rss_tail_plateau_kib", None) is not None:
            _check_external_tail_plateau(
                samples,
                plateau=args.external_rss_tail_plateau_kib,
                min_seconds=getattr(args, "external_tail_min_seconds", 0.0),
                min_samples=getattr(args, "external_tail_min_samples", 1),
                label="RSS",
                value=lambda sample: sample.current_rss_kib,
            )
        _check_external_named_memory_gates(
            samples,
            args,
            attr_prefix="pss",
            label="PSS",
            value=lambda sample: sample.pss_kib,
        )
        _check_external_named_memory_gates(
            samples,
            args,
            attr_prefix="private",
            label="private memory",
            value=lambda sample: sample.private_kib,
        )
        _check_external_named_memory_gates(
            samples,
            args,
            attr_prefix="anonymous",
            label="anonymous memory",
            value=lambda sample: sample.anonymous_kib,
        )
        _check_external_named_count_gates(
            samples,
            args,
            attr_prefix="fd",
            label="file descriptors",
            value=lambda sample: sample.fd_count,
        )
        if args.max_elapsed_seconds is not None:
            _check_external_elapsed_seconds(
                elapsed,
                max_seconds=args.max_elapsed_seconds,
            )
        if getattr(args, "external_min_elapsed_seconds", None) is not None:
            _check_external_min_elapsed_seconds(
                elapsed,
                min_seconds=args.external_min_elapsed_seconds,
            )
        if args.max_cpu_seconds is not None:
            _check_external_cpu_seconds(
                usage_delta,
                max_seconds=args.max_cpu_seconds,
            )
        _check_external_block_io(
            usage_delta,
            max_input_blocks=args.max_disk_input_blocks,
            max_output_blocks=args.max_disk_output_blocks,
        )
    return samples


def _write_external_monitor_summary(
    path: Path,
    *,
    command: tuple[str, ...],
    check_enabled: bool = False,
    gate_args: argparse.Namespace | None = None,
    samples: tuple["_RssSample", ...],
    elapsed_seconds: float,
    project_seconds: float,
    warmup_fraction: float,
    usage: "_UsageSnapshot",
    returncode: int | None,
    terminated_by_monitor: bool,
    metadata: dict[str, object] | None = None,
    output_max_samples: int | None = 512,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    metrics = _external_monitor_metric_summary(
        samples,
        warmup_fraction=warmup_fraction,
        project_seconds=project_seconds,
    )
    gates = _external_monitor_status_gates(
        returncode=returncode,
        terminated_by_monitor=terminated_by_monitor,
    )
    if check_enabled and gate_args is not None:
        gates.extend(
            _external_monitor_gate_summary(
                samples,
                metrics,
                gate_args,
                elapsed_seconds=elapsed_seconds,
                usage=usage,
            )
        )
    failed_gates = tuple(gate["name"] for gate in gates if not gate["passed"])
    unavailable_gates = tuple(gate["name"] for gate in gates if not gate["available"])
    retained_samples = _retained_external_samples(samples, output_max_samples)
    summary = {
        "command": list(command),
        "status": {
            "returncode": returncode,
            "elapsed_seconds": elapsed_seconds,
            "cpu_seconds": usage.cpu_seconds,
            "input_blocks": usage.input_blocks,
            "output_blocks": usage.output_blocks,
            "terminated_by_monitor": terminated_by_monitor,
        },
        "checks_enabled": check_enabled,
        "failed_gates": list(failed_gates),
        "gates": gates,
        "metrics": metrics,
        "metadata": dict(metadata or {}),
        "passed": not failed_gates,
        "retained_sample_count": len(retained_samples),
        "sample_count": len(samples),
        "sample_retention_limit": output_max_samples,
        "samples": [_rss_sample_dict(sample) for sample in retained_samples],
        "unavailable_gates": list(unavailable_gates),
    }
    path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _external_monitor_metric_summary(
    samples: tuple["_RssSample", ...],
    *,
    warmup_fraction: float,
    project_seconds: float,
) -> dict[str, dict[str, float | int | None]]:
    return {
        "rss_kib": _external_metric_summary(
            samples,
            warmup_fraction=warmup_fraction,
            project_seconds=project_seconds,
            value=lambda sample: sample.current_rss_kib,
        ),
        "pss_kib": _external_metric_summary(
            samples,
            warmup_fraction=warmup_fraction,
            project_seconds=project_seconds,
            value=lambda sample: sample.pss_kib,
        ),
        "private_kib": _external_metric_summary(
            samples,
            warmup_fraction=warmup_fraction,
            project_seconds=project_seconds,
            value=lambda sample: sample.private_kib,
        ),
        "anonymous_kib": _external_metric_summary(
            samples,
            warmup_fraction=warmup_fraction,
            project_seconds=project_seconds,
            value=lambda sample: sample.anonymous_kib,
        ),
        "fd_count": _external_metric_summary(
            samples,
            warmup_fraction=warmup_fraction,
            project_seconds=project_seconds,
            value=lambda sample: sample.fd_count,
        ),
    }


def _retained_external_samples(
    samples: tuple["_RssSample", ...],
    max_samples: int | None,
) -> tuple["_RssSample", ...]:
    if max_samples is None:
        return samples
    if max_samples < 0:
        raise ValueError("external output max samples must be non-negative")
    if max_samples == 0:
        return ()
    return samples[-max_samples:]


def _external_monitor_gate_summary(
    samples: tuple["_RssSample", ...],
    metrics: dict[str, dict[str, float | int | None]],
    args: argparse.Namespace,
    *,
    elapsed_seconds: float,
    usage: "_UsageSnapshot",
) -> list[dict[str, float | int | str | bool | None]]:
    gates: list[dict[str, float | int | str | bool | None]] = []
    min_samples = getattr(args, "external_min_samples", None)
    if min_samples is not None:
        gates.append(_count_gate("external_min_samples", len(samples), min_samples, "min"))
    gates.append(
        _metric_gate(
            "external_rss_plateau_kib",
            "rss_kib",
            "steady_range",
            metrics,
            getattr(args, "rss_plateau_kib"),
        )
    )
    _append_optional_metric_gate(
        gates,
        "external_rss_projected_growth_kib",
        "rss_kib",
        "projected_growth",
        metrics,
        getattr(args, "external_rss_projected_growth_kib", None),
    )
    _append_optional_metric_gate(
        gates,
        "external_rss_segment_projected_growth_kib",
        "rss_kib",
        "segment_projected_growth",
        metrics,
        getattr(args, "external_rss_segment_projected_growth_kib", None),
    )
    rss_tail_plateau_kib = getattr(args, "external_rss_tail_plateau_kib", None)
    if rss_tail_plateau_kib is not None:
        tail = _final_tail_plateau(
            samples,
            plateau=rss_tail_plateau_kib,
            value=lambda sample: sample.current_rss_kib,
        )
        gates.append(
            _optional_value_gate(
                "external_rss_tail_plateau_kib",
                tail["range"],
                rss_tail_plateau_kib,
                "max",
            )
        )
        gates.append(
            _optional_value_gate(
                "external_rss_tail_plateau_seconds",
                tail["seconds"],
                getattr(args, "external_tail_min_seconds", 0.0),
                "min",
            )
        )
        gates.append(
            _optional_value_gate(
                "external_rss_tail_plateau_samples",
                tail["sample_count"],
                getattr(args, "external_tail_min_samples", 1),
                "min",
            )
        )
    _append_named_memory_gates(gates, "pss", "pss_kib", metrics, args)
    _append_named_memory_gates(gates, "private", "private_kib", metrics, args)
    _append_named_memory_gates(gates, "anonymous", "anonymous_kib", metrics, args)
    _append_named_count_gates(gates, "fd", "fd_count", metrics, args)
    _append_optional_count_gate(
        gates,
        "external_min_elapsed_seconds",
        elapsed_seconds,
        getattr(args, "external_min_elapsed_seconds", None),
        "min",
    )
    _append_optional_count_gate(
        gates,
        "max_elapsed_seconds",
        elapsed_seconds,
        getattr(args, "max_elapsed_seconds", None),
        "max",
    )
    _append_optional_count_gate(
        gates,
        "max_cpu_seconds",
        usage.cpu_seconds,
        getattr(args, "max_cpu_seconds", None),
        "max",
    )
    _append_optional_count_gate(
        gates,
        "max_disk_input_blocks",
        usage.input_blocks,
        getattr(args, "max_disk_input_blocks", None),
        "max",
    )
    _append_optional_count_gate(
        gates,
        "max_disk_output_blocks",
        usage.output_blocks,
        getattr(args, "max_disk_output_blocks", None),
        "max",
    )
    return gates


def _external_monitor_status_gates(
    *,
    returncode: int | None,
    terminated_by_monitor: bool,
) -> list[dict[str, float | int | str | bool | None]]:
    passed = terminated_by_monitor or returncode in (0, None)
    return [
        {
            "name": "external_command_status",
            "metric": "returncode",
            "field": "value",
            "mode": "zero_or_monitor_terminated",
            "observed": returncode,
            "limit": 0,
            "available": True,
            "passed": passed,
        }
    ]


def _append_named_memory_gates(
    gates: list[dict[str, float | int | str | bool | None]],
    attr_prefix: str,
    metric_name: str,
    metrics: dict[str, dict[str, float | int | None]],
    args: argparse.Namespace,
) -> None:
    _append_optional_metric_gate(
        gates,
        f"external_{attr_prefix}_plateau_kib",
        metric_name,
        "steady_range",
        metrics,
        getattr(args, f"external_{attr_prefix}_plateau_kib", None),
    )
    _append_optional_metric_gate(
        gates,
        f"external_{attr_prefix}_projected_growth_kib",
        metric_name,
        "projected_growth",
        metrics,
        getattr(args, f"external_{attr_prefix}_projected_growth_kib", None),
    )
    _append_optional_metric_gate(
        gates,
        f"external_{attr_prefix}_segment_projected_growth_kib",
        metric_name,
        "segment_projected_growth",
        metrics,
        getattr(args, f"external_{attr_prefix}_segment_projected_growth_kib", None),
    )


def _append_named_count_gates(
    gates: list[dict[str, float | int | str | bool | None]],
    attr_prefix: str,
    metric_name: str,
    metrics: dict[str, dict[str, float | int | None]],
    args: argparse.Namespace,
) -> None:
    _append_optional_metric_gate(
        gates,
        f"external_{attr_prefix}_plateau_count",
        metric_name,
        "steady_range",
        metrics,
        getattr(args, f"external_{attr_prefix}_plateau_count", None),
    )
    _append_optional_metric_gate(
        gates,
        f"external_{attr_prefix}_projected_growth_count",
        metric_name,
        "projected_growth",
        metrics,
        getattr(args, f"external_{attr_prefix}_projected_growth_count", None),
    )
    _append_optional_metric_gate(
        gates,
        f"external_{attr_prefix}_segment_projected_growth_count",
        metric_name,
        "segment_projected_growth",
        metrics,
        getattr(args, f"external_{attr_prefix}_segment_projected_growth_count", None),
    )


def _append_optional_metric_gate(
    gates: list[dict[str, float | int | str | bool | None]],
    name: str,
    metric: str,
    field: str,
    metrics: dict[str, dict[str, float | int | None]],
    limit: float | int | None,
) -> None:
    if limit is None:
        return
    gates.append(_metric_gate(name, metric, field, metrics, limit))


def _append_optional_count_gate(
    gates: list[dict[str, float | int | str | bool | None]],
    name: str,
    observed: float | int,
    limit: float | int | None,
    mode: str,
) -> None:
    if limit is None:
        return
    gates.append(_count_gate(name, observed, limit, mode))


def _metric_gate(
    name: str,
    metric: str,
    field: str,
    metrics: dict[str, dict[str, float | int | None]],
    limit: float | int,
) -> dict[str, float | int | str | bool | None]:
    observed = metrics[metric][field]
    return {
        "name": name,
        "metric": metric,
        "field": field,
        "mode": "max",
        "observed": observed,
        "limit": limit,
        "available": observed is not None,
        "passed": observed is not None and observed <= limit,
    }


def _count_gate(
    name: str,
    observed: float | int,
    limit: float | int,
    mode: str,
) -> dict[str, float | int | str | bool | None]:
    passed = observed >= limit if mode == "min" else observed <= limit
    return {
        "name": name,
        "metric": name,
        "field": "value",
        "mode": mode,
        "observed": observed,
        "limit": limit,
        "available": True,
        "passed": passed,
    }


def _optional_value_gate(
    name: str,
    observed: float | int | None,
    limit: float | int,
    mode: str,
) -> dict[str, float | int | str | bool | None]:
    passed = (
        False
        if observed is None
        else observed >= limit
        if mode == "min"
        else observed <= limit
    )
    return {
        "name": name,
        "metric": name,
        "field": "value",
        "mode": mode,
        "observed": observed,
        "limit": limit,
        "available": observed is not None,
        "passed": passed,
    }


def _external_metric_summary(
    samples: tuple["_RssSample", ...],
    *,
    warmup_fraction: float,
    project_seconds: float,
    value: Callable[["_RssSample"], int | None],
) -> dict[str, float | int | None]:
    first_steady_index = max(0, int(len(samples) * warmup_fraction))
    steady_values = tuple(
        sample_value
        for sample in samples[first_steady_index:]
        if (sample_value := value(sample)) is not None
    )
    if not steady_values:
        return {
            "sample_count": len(samples),
            "steady_sample_count": 0,
            "steady_min": None,
            "steady_max": None,
            "steady_range": None,
            "project_seconds": project_seconds,
            "projected_growth": None,
            "segment_projected_growth": None,
        }
    return {
        "sample_count": len(samples),
        "steady_sample_count": len(steady_values),
        "steady_min": min(steady_values),
        "steady_max": max(steady_values),
        "steady_range": max(steady_values) - min(steady_values),
        "project_seconds": project_seconds,
        "projected_growth": _projected_external_growth_over_seconds(
            samples,
            warmup_fraction=warmup_fraction,
            project_seconds=project_seconds,
            value=value,
        ),
        "segment_projected_growth": _projected_external_segment_growth_over_seconds(
            samples,
            warmup_fraction=warmup_fraction,
            project_seconds=project_seconds,
            value=value,
        ),
    }


def _rss_sample_dict(sample: "_RssSample") -> dict[str, int | float | None]:
    return {
        "elapsed_seconds": sample.elapsed_seconds,
        "current_rss_kib": sample.current_rss_kib,
        "pss_kib": sample.pss_kib,
        "private_kib": sample.private_kib,
        "anonymous_kib": sample.anonymous_kib,
        "fd_count": sample.fd_count,
    }


def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=50_000)
    parser.add_argument("--history-limit", type=int, default=8)
    parser.add_argument("--sample-every", type=int, default=10_000)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--external-monitor", action="store_true")
    parser.add_argument("--external-command", action="store_true")
    parser.add_argument("--external-duration-seconds", type=float)
    parser.add_argument("--external-output-json", type=Path)
    parser.add_argument("--external-output-max-samples", type=int, default=512)
    parser.add_argument("--external-min-elapsed-seconds", type=float)
    parser.add_argument("--external-min-samples", type=int)
    parser.add_argument("--external-project-seconds", type=float, default=86_400.0)
    parser.add_argument("--external-rss-projected-growth-kib", type=int)
    parser.add_argument("--external-rss-segment-projected-growth-kib", type=int)
    parser.add_argument("--external-rss-tail-plateau-kib", type=int)
    parser.add_argument("--external-tail-min-seconds", type=float, default=0.0)
    parser.add_argument("--external-tail-min-samples", type=int, default=1)
    parser.add_argument("--external-pss-plateau-kib", type=int)
    parser.add_argument("--external-pss-projected-growth-kib", type=int)
    parser.add_argument("--external-pss-segment-projected-growth-kib", type=int)
    parser.add_argument("--external-private-plateau-kib", type=int)
    parser.add_argument("--external-private-projected-growth-kib", type=int)
    parser.add_argument("--external-private-segment-projected-growth-kib", type=int)
    parser.add_argument("--external-anonymous-plateau-kib", type=int)
    parser.add_argument("--external-anonymous-projected-growth-kib", type=int)
    parser.add_argument("--external-anonymous-segment-projected-growth-kib", type=int)
    parser.add_argument("--external-fd-plateau-count", type=int)
    parser.add_argument("--external-fd-projected-growth-count", type=int)
    parser.add_argument("--external-fd-segment-projected-growth-count", type=int)
    parser.add_argument("--external-rss-scope", choices=("process", "tree"), default="tree")
    parser.add_argument("--external-shutdown-timeout-seconds", type=float, default=5.0)
    parser.add_argument(
        "--external-terminate-scope",
        choices=("process", "tree"),
        default="tree",
    )
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--churn-subscriptions", action="store_true")
    parser.add_argument("--churn-taints", action="store_true")
    parser.add_argument("--churn-graphs", action="store_true")
    parser.add_argument("--live-observers", type=int, default=0)
    parser.add_argument("--lineage-retention", choices=("none", "retained"), default="none")
    parser.add_argument("--lineage-store", choices=("native", "noop"), default="native")
    parser.add_argument("--correlation-store", choices=("native", "noop"), default="noop")
    parser.add_argument("--payload-bytes", type=int, default=len(b"frame"))
    parser.add_argument("--payload-schema", choices=("bytes", "any"), default="bytes")
    parser.add_argument("--publish-mode", choices=("publish", "nowait"), default="publish")
    parser.add_argument(
        "--metadata-mode",
        choices=("none", "static", "unique", "unique-all"),
        default="unique",
    )
    parser.add_argument("--unrelated-edges", type=int, default=0)
    parser.add_argument("--materialize-state", action="store_true")
    parser.add_argument("--monitor-interval", type=float, default=0.5)
    parser.add_argument("--rss-plateau-kib", type=int, default=512)
    parser.add_argument("--rss-tail-plateau-kib", type=int)
    parser.add_argument("--rss-tail-min-samples", type=int, default=1)
    parser.add_argument("--rss-warmup-fraction", type=float, default=0.5)
    parser.add_argument("--traced-plateau-bytes", type=int, default=16_384)
    parser.add_argument("--traced-warmup-fraction", type=float, default=0.5)
    parser.add_argument("--project-events", type=int, default=1_000_000_000)
    parser.add_argument("--traced-projected-growth-bytes", type=int)
    parser.add_argument("--traced-segment-projected-growth-bytes", type=int)
    parser.add_argument("--rss-projected-growth-kib", type=int)
    parser.add_argument("--rss-segment-projected-growth-kib", type=int)
    parser.add_argument("--max-elapsed-seconds", type=float)
    parser.add_argument("--max-cpu-seconds", type=float)
    parser.add_argument("--max-average-event-us", type=float)
    parser.add_argument("--max-interval-event-us", type=float)
    parser.add_argument("--max-disk-input-blocks", type=int)
    parser.add_argument("--max-disk-output-blocks", type=int)
    parser.add_argument("--max-materialized-payloads", type=int)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.external_command:
        _run_external_command_monitor(args)
        return
    if args.external_monitor:
        _run_external_monitor(args)
        return
    if args.churn_graphs:
        samples = _run_graph_churn_loop(
            args.iterations,
            args.history_limit,
            args.sample_every,
        )
    else:
        samples = _run_worker_loop(
            args.iterations,
            args.history_limit,
            args.sample_every,
            churn_subscriptions=args.churn_subscriptions,
            churn_taints=args.churn_taints,
            live_observers=args.live_observers,
            lineage_retention_policy=args.lineage_retention,
            lineage_store=args.lineage_store,
            payload_bytes=args.payload_bytes,
            payload_schema=args.payload_schema,
            publish_mode=args.publish_mode,
            unrelated_edges=args.unrelated_edges,
            materialize_state=args.materialize_state,
            metadata_mode=args.metadata_mode,
            correlation_store=args.correlation_store,
        )
    if args.check:
        if args.churn_graphs:
            _check_disposed_graph_counts(samples)
        else:
            expected_lineage = (
                args.history_limit
                if (
                    args.lineage_retention == "retained"
                    and args.lineage_store == "native"
                )
                else 0
            )
            _check_retained_counts_for_lineage(
                samples,
                args.history_limit,
                expected_lineage,
                _expected_correlation_index_count(
                    args.history_limit,
                    lineage_retention=args.lineage_retention,
                    lineage_store=args.lineage_store,
                    correlation_store=args.correlation_store,
                    metadata_mode=args.metadata_mode,
                ),
                _expected_subscriber_metadata_count(
                    args.live_observers,
                    materialize_state=args.materialize_state,
                ),
            )
        _check_traced_current_plateau(
            samples,
            warmup_fraction=args.traced_warmup_fraction,
            plateau_bytes=args.traced_plateau_bytes,
        )
        if args.traced_projected_growth_bytes is not None:
            _check_projected_traced_growth(
                samples,
                warmup_fraction=args.traced_warmup_fraction,
                project_events=args.project_events,
                limit_bytes=args.traced_projected_growth_bytes,
            )
        if args.traced_segment_projected_growth_bytes is not None:
            _check_segment_projected_traced_growth(
                samples,
                warmup_fraction=args.traced_warmup_fraction,
                project_events=args.project_events,
                limit_bytes=args.traced_segment_projected_growth_bytes,
            )
        if args.rss_projected_growth_kib is not None:
            _check_projected_rss_growth(
                samples,
                warmup_fraction=args.rss_warmup_fraction,
                project_events=args.project_events,
                limit_kib=args.rss_projected_growth_kib,
            )
        if args.rss_segment_projected_growth_kib is not None:
            _check_segment_projected_rss_growth(
                samples,
                warmup_fraction=args.rss_warmup_fraction,
                project_events=args.project_events,
                limit_kib=args.rss_segment_projected_growth_kib,
            )
        if args.rss_tail_plateau_kib is not None:
            _check_probe_tail_plateau(
                samples,
                plateau=args.rss_tail_plateau_kib,
                min_samples=args.rss_tail_min_samples,
                label="current RSS",
                value=lambda sample: sample.current_rss_kib,
            )
        if args.max_elapsed_seconds is not None:
            _check_elapsed_seconds(samples, max_seconds=args.max_elapsed_seconds)
        if args.max_cpu_seconds is not None:
            _check_cpu_seconds(samples, max_seconds=args.max_cpu_seconds)
        if args.max_average_event_us is not None:
            _check_average_event_latency(
                samples,
                max_us=args.max_average_event_us,
            )
        if args.max_interval_event_us is not None:
            _check_interval_event_latency(
                samples,
                max_us=args.max_interval_event_us,
            )
        _check_block_io(
            samples,
            max_input_blocks=args.max_disk_input_blocks,
            max_output_blocks=args.max_disk_output_blocks,
        )
        if args.max_materialized_payloads is not None:
            _check_materialized_payload_count(
                samples,
                max_count=args.max_materialized_payloads,
            )


def _expected_subscriber_metadata_count(
    live_observers: int,
    *,
    materialize_state: bool = False,
) -> int:
    materialize_metadata = 7 if materialize_state else 0
    if live_observers <= 0:
        return materialize_metadata
    return materialize_metadata + 4 + (live_observers * 2)


def _expected_correlation_index_count(
    history_limit: int,
    *,
    lineage_retention: str,
    lineage_store: str,
    correlation_store: str,
    metadata_mode: str,
) -> int:
    if (
        lineage_retention != "retained"
        or lineage_store != "native"
        or correlation_store != "native"
        or metadata_mode == "none"
    ):
        return 0
    return history_limit


@dataclass(frozen=True, slots=True)
class _ProbeSample:
    step: int
    replay_count: int
    lineage_count: int
    correlation_index_count: int
    payload_count: int
    violation_count: int
    subscriber_metadata_count: int
    taint_metadata_count: int
    materialized_payload_count: int
    traced_current_bytes: int
    traced_peak_bytes: int
    current_rss_kib: int
    max_rss_native: int
    elapsed_seconds: float
    cpu_seconds: float
    average_event_us: float
    interval_event_us: float
    input_blocks: int
    output_blocks: int


@dataclass(frozen=True, slots=True)
class _ProcessMemory:
    rss_kib: int | None = None
    pss_kib: int | None = None
    private_kib: int | None = None
    anonymous_kib: int | None = None

    def add(self, other: "_ProcessMemory") -> "_ProcessMemory":
        return _ProcessMemory(
            rss_kib=_sum_optional_kib(self.rss_kib, other.rss_kib),
            pss_kib=_sum_optional_kib(self.pss_kib, other.pss_kib),
            private_kib=_sum_optional_kib(self.private_kib, other.private_kib),
            anonymous_kib=_sum_optional_kib(
                self.anonymous_kib,
                other.anonymous_kib,
            ),
        )


@dataclass(frozen=True, slots=True)
class _RssSample:
    elapsed_seconds: float
    current_rss_kib: int
    pss_kib: int | None = None
    private_kib: int | None = None
    anonymous_kib: int | None = None
    fd_count: int | None = None


class _RssSampleSeries(Sequence[_RssSample]):
    """Compact monitor sample storage; materializes rows only for consumers."""

    __slots__ = (
        "_anonymous_kib",
        "_current_rss_kib",
        "_elapsed_seconds",
        "_fd_count",
        "_private_kib",
        "_pss_kib",
    )

    def __init__(self) -> None:
        self._elapsed_seconds: list[float] = []
        self._current_rss_kib: list[int] = []
        self._pss_kib: list[int | None] = []
        self._private_kib: list[int | None] = []
        self._anonymous_kib: list[int | None] = []
        self._fd_count: list[int | None] = []

    def __len__(self) -> int:
        return len(self._elapsed_seconds)

    def __iter__(self) -> Iterator[_RssSample]:
        for index in range(len(self)):
            yield self._sample_at(index)

    def __getitem__(self, index: _RssSampleIndex) -> _RssSample | tuple[_RssSample, ...]:
        if isinstance(index, slice):
            return tuple(self._sample_at(item) for item in range(*index.indices(len(self))))
        return self._sample_at(index)

    def append(
        self,
        *,
        elapsed_seconds: float,
        current_rss_kib: int,
        pss_kib: int | None = None,
        private_kib: int | None = None,
        anonymous_kib: int | None = None,
        fd_count: int | None = None,
    ) -> None:
        self._elapsed_seconds.append(elapsed_seconds)
        self._current_rss_kib.append(current_rss_kib)
        self._pss_kib.append(pss_kib)
        self._private_kib.append(private_kib)
        self._anonymous_kib.append(anonymous_kib)
        self._fd_count.append(fd_count)

    def _sample_at(self, index: int) -> _RssSample:
        return _RssSample(
            elapsed_seconds=self._elapsed_seconds[index],
            current_rss_kib=self._current_rss_kib[index],
            pss_kib=self._pss_kib[index],
            private_kib=self._private_kib[index],
            anonymous_kib=self._anonymous_kib[index],
            fd_count=self._fd_count[index],
        )


@dataclass(frozen=True, slots=True)
class _UsageSnapshot:
    cpu_seconds: float
    input_blocks: int
    output_blocks: int


if __name__ == "__main__":
    _main()
