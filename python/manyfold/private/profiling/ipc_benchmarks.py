"""Measure local process IPC round-trip overhead for worker compatibility."""

from __future__ import annotations

import argparse
import json
import struct
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

_LENGTH = struct.Struct("<I")


def run_python_baseline(*, iterations: int, payload_bytes: int) -> IpcBenchmarkResult:
    """Measure Python framing work without crossing a process boundary."""
    _validate_positive_int("iterations", iterations)
    _validate_positive_int("payload_bytes", payload_bytes)
    payload = bytes((index % 251 for index in range(payload_bytes)))
    frame = _LENGTH.pack(len(payload)) + payload
    checksum = 0
    start = time.perf_counter()
    for _ in range(iterations):
        size = _LENGTH.unpack(frame[: _LENGTH.size])[0]
        response = frame[_LENGTH.size : _LENGTH.size + size]
        checksum ^= response[-1]
    return _result("python_framing", iterations, payload_bytes, start, checksum)


def run_rust_process_pipe(
    *,
    worker: Path,
    worker_args: Sequence[str] = (),
    iterations: int,
    payload_bytes: int,
) -> IpcBenchmarkResult:
    """Measure Python round-trips through a Rust child process over pipes."""
    _validate_positive_int("iterations", iterations)
    _validate_positive_int("payload_bytes", payload_bytes)
    if not worker.exists():
        raise FileNotFoundError(f"worker does not exist: {worker}")
    payload = bytes((index % 251 for index in range(payload_bytes)))
    header = _LENGTH.pack(len(payload))
    checksum = 0
    process = subprocess.Popen(
        [str(worker), *worker_args],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.stdin is None or process.stdout is None:
        process.kill()
        raise RuntimeError("worker pipes were not created")
    stderr = ""
    try:
        start = time.perf_counter()
        for _ in range(iterations):
            process.stdin.write(header)
            process.stdin.write(payload)
            process.stdin.flush()
            echoed_header = process.stdout.read(_LENGTH.size)
            if len(echoed_header) != _LENGTH.size:
                raise RuntimeError("worker closed before response header")
            response_size = _LENGTH.unpack(echoed_header)[0]
            response = process.stdout.read(response_size)
            if response != payload:
                raise RuntimeError("worker returned an unexpected payload")
            checksum ^= response[-1]
        result = _result("python_to_rust_process_pipe", iterations, payload_bytes, start, checksum)
    finally:
        if process.stdin is not None:
            process.stdin.close()
        process.wait(timeout=5)
        if process.stderr is not None:
            stderr = process.stderr.read().decode("utf-8", errors="replace")
            process.stderr.close()
        if process.stdout is not None:
            process.stdout.close()
    if process.returncode != 0:
        raise RuntimeError(f"worker exited with {process.returncode}: {stderr}")
    return result


@dataclass(frozen=True)
class IpcBenchmarkResult:
    """Summary of one IPC benchmark workload."""

    workload: str
    iterations: int
    payload_bytes: int
    elapsed_seconds: float
    average_roundtrip_us: float
    roundtrips_per_second: float
    checksum: int

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serializable result record."""
        return {
            "workload": self.workload,
            "iterations": self.iterations,
            "payload_bytes": self.payload_bytes,
            "elapsed_seconds": self.elapsed_seconds,
            "average_roundtrip_us": self.average_roundtrip_us,
            "roundtrips_per_second": self.roundtrips_per_second,
            "checksum": self.checksum,
        }


def _main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=10000)
    parser.add_argument("--payload-bytes", type=int, default=64)
    args = parser.parse_args(argv)

    results = [
        run_python_baseline(
            iterations=args.iterations,
            payload_bytes=args.payload_bytes,
        ),
        run_rust_process_pipe(
            worker=args.worker,
            worker_args=(),
            iterations=args.iterations,
            payload_bytes=args.payload_bytes,
        ),
    ]
    json.dump([result.as_dict() for result in results], sys.stdout, indent=2)
    sys.stdout.write("\n")


def _result(
    workload: str,
    iterations: int,
    payload_bytes: int,
    start: float,
    checksum: int,
) -> IpcBenchmarkResult:
    elapsed_seconds = time.perf_counter() - start
    return IpcBenchmarkResult(
        workload=workload,
        iterations=iterations,
        payload_bytes=payload_bytes,
        elapsed_seconds=elapsed_seconds,
        average_roundtrip_us=elapsed_seconds / iterations * 1_000_000.0,
        roundtrips_per_second=iterations / elapsed_seconds,
        checksum=checksum,
    )


def _validate_positive_int(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")


__all__ = [
    "IpcBenchmarkResult",
    "run_python_baseline",
    "run_rust_process_pipe",
]


if __name__ == "__main__":
    _main()
