"""Compare durable delivery sends through the public transport boundary."""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import platform
import socket
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path
from time import perf_counter_ns

import manyfold._manyfold_rust as _manyfold_rust
import manyfold.architecture.transport_delivery as _transport_delivery_module
from manyfold.architecture.transport import (
    FrameKind,
    NodeIdentity,
    TcpAddress,
    TcpTransport,
    TransportConfig,
    TransportMessage,
    TransportSecurity,
)
from manyfold.architecture.transport_delivery import DeliveryConfig, DurableDelivery


def _unused_address() -> TcpAddress:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        host, port = probe.getsockname()[:2]
        return TcpAddress(str(host), int(port))
    finally:
        probe.close()


def _one_run(
    root: Path,
    *,
    iterations: int,
    payload_bytes: int,
    run_index: int,
) -> dict[str, float | int]:
    transport = TcpTransport.listen(
        NodeIdentity("cluster", "sender", f"bench-{run_index}"),
        _unused_address(),
        config=TransportConfig(
            security=TransportSecurity.insecure_local_development(),
            outbound_queue_limit=16,
            inbound_queue_limit=16,
            max_payload_bytes=65536,
            connect_timeout=0.01,
            handshake_timeout=0.1,
            heartbeat_interval=0.05,
            peer_timeout=0.5,
        ),
    )
    delivery = DurableDelivery(
        transport,
        DeliveryConfig(
            root / f"run-{run_index}.sqlite3",
            max_outbox_items=max(4096, iterations * 4),
            max_inbox_items=1,
            max_storage_bytes=max(
                4 * 1024 * 1024,
                iterations * (payload_bytes + 512),
            ),
            receive_queue_limit=1,
            max_message_bytes=4096,
            message_ttl_seconds=60.0,
            dedupe_retention_seconds=60.0,
            retry_initial_seconds=0.1,
            retry_multiplier=2.0,
            retry_max_seconds=5.0,
        ),
    )
    payload = b"x" * payload_bytes
    started = perf_counter_ns()
    try:
        for index in range(iterations):
            delivery.send(
                TransportMessage(FrameKind.PUBSUB, "benchmark.topic", payload),
                message_id=f"message-{index}",
            )
        elapsed_ns = perf_counter_ns() - started
        health = delivery.health()
        database_file_bytes = delivery.config.journal_path.stat().st_size
    finally:
        delivery.close()
        transport.close()
    if health.outbox_items != iterations:
        raise RuntimeError(
            f"retained {health.outbox_items} rows; expected {iterations}"
        )
    return {
        "elapsed_seconds": elapsed_ns / 1_000_000_000,
        "operations_per_second": iterations * 1_000_000_000 / elapsed_ns,
        "retained_items": health.outbox_items,
        "logical_storage_bytes": health.logical_storage_bytes,
        "database_file_bytes": database_file_bytes,
    }


def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=_positive_integer, default=1000)
    parser.add_argument("--payload-bytes", type=_positive_integer, default=256)
    parser.add_argument("--warmup-runs", type=_positive_integer, default=1)
    parser.add_argument("--measured-runs", type=_positive_integer, default=5)
    arguments = parser.parse_args()
    with tempfile.TemporaryDirectory() as temporary_directory:
        root = Path(temporary_directory)
        for warmup_index in range(arguments.warmup_runs):
            _one_run(
                root,
                iterations=arguments.iterations,
                payload_bytes=arguments.payload_bytes,
                run_index=warmup_index,
            )
        runs = [
            _one_run(
                root,
                iterations=arguments.iterations,
                payload_bytes=arguments.payload_bytes,
                run_index=arguments.warmup_runs + index,
            )
            for index in range(arguments.measured_runs)
        ]
    operations_per_second = [
        float(run["operations_per_second"]) for run in runs
    ]
    print(
        json.dumps(
            {
                "environment": _environment(),
                "iterations": arguments.iterations,
                "payload_bytes": arguments.payload_bytes,
                "warmup_runs": arguments.warmup_runs,
                "measured_runs": arguments.measured_runs,
                "runs": runs,
                "operations_per_second_mean": statistics.mean(
                    operations_per_second
                ),
                "operations_per_second_stdev": (
                    statistics.stdev(operations_per_second)
                    if len(operations_per_second) > 1
                    else 0.0
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )


def _environment() -> dict[str, str | int | None]:
    delivery_file = Path(inspect.getfile(_transport_delivery_module)).resolve()
    native_file = Path(inspect.getfile(_manyfold_rust)).resolve()
    repo_root = _repo_root_for(delivery_file)
    return {
        "git_revision": _git_revision(repo_root),
        "machine": platform.machine(),
        "manyfold_transport_delivery": str(delivery_file),
        "native_extension": str(native_file),
        "native_extension_sha256": _sha256(native_file),
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python": sys.version.split()[0],
        "python_executable": sys.executable,
        "repo_root": str(repo_root) if repo_root is not None else None,
    }


def _repo_root_for(path: Path) -> Path | None:
    for candidate in (path, *path.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def _git_revision(repo_root: Path | None) -> str | None:
    if repo_root is None:
        return None
    completed = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
        timeout=5.0,
    )
    return completed.stdout.strip()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while True:
            chunk = file.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


if __name__ == "__main__":
    _main()
