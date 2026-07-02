# IPC Worker Benchmark

This benchmark measures the compatibility cost of moving a Manyfold client API
call across a process boundary into a Rust worker. It is intentionally smaller
than the real worker protocol: the Rust child process reads a 4-byte
little-endian payload length, reads that many bytes, and echoes the same frame
back to the caller.

## What It Measures

`manyfold-ipc-benchmark` emits JSON for two workloads:

- `python_framing`: Python length-prefix encode/decode work without crossing a
  process boundary. This is the local framing baseline.
- `python_to_rust_process_pipe`: Python sends one request to a Rust child
  process over stdin and waits for one response on stdout. This measures
  unbatched local process round-trip overhead for the smallest useful worker
  proxy shape.

The benchmark does not measure SQL, PubSub routing, serialization beyond the
length prefix, Electron preload overhead, sockets, shared memory, network IO, or
WASM host calls. Add those as separate workloads when the production transport
is selected.

## Baseline

Environment:

- Date: 2026-07-02.
- Host: local macOS Codex worktree.
- Rust build: `cargo build --release --bin ipc_echo_worker`.
- Python command:
  `uv run manyfold-ipc-benchmark --worker target/release/ipc_echo_worker`.
- Workload: 50,000 unbatched request/response round trips.

| Payload | Python Framing us | Python -> Rust Process Pipe us | Round Trips / Second |
| ---: | ---: | ---: | ---: |
| 16 B | 0.097 | 7.277 | 137,411 |
| 64 B | 0.096 | 7.299 | 137,008 |
| 1 KiB | 0.121 | 8.034 | 124,468 |
| 4 KiB | 0.175 | 8.469 | 118,084 |
| 16 KiB | 0.288 | 10.588 | 94,451 |

For context, the native in-process Rust PubSub benchmark on the same worktree
reported about `0.12 us` for publish/latest and `0.64 us` for poll with
batch-size 1 at 64-byte payloads. Unbatched local process IPC is therefore a
single-digit microsecond absolute cost, but a large multiplier over the
in-process hot path.

## Interpretation

The compatibility path is reasonable for control-plane calls, UI events, locks,
clocks, and modest PubSub traffic. It should not be the default shape for
per-frame, per-sample, or high-TPS data streams unless the caller batches,
coalesces, or keeps the hot path in-process.

WASM/Electron still needs a separate measurement. This worktree did not have
`node`, `deno`, `bun`, or `wasmtime` available, so no JavaScript-hosted number
was collected. Expect Electron to add at least one host bridge hop on top of the
Rust process pipe cost.

## Commands

Build the Rust echo worker:

```sh
cargo build --release --bin ipc_echo_worker
```

Run the focused Python-to-Rust pipe benchmark:

```sh
uv run manyfold-ipc-benchmark --worker target/release/ipc_echo_worker --iterations 50000 --payload-bytes 64
```

Run a small payload sweep:

```sh
for size in 16 64 1024 4096 16384; do
  uv run manyfold-ipc-benchmark --worker target/release/ipc_echo_worker --iterations 50000 --payload-bytes "$size"
done
```
