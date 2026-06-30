# Dormant Runtime Benchmark

This benchmark tracks the cost of starting one controller instance, admitting or
spawning runtime capacity under that controller, and then waking that capacity.
It is a native Rust benchmark because the runtime substrate is a Rust program;
Python is a client surface over that substrate, not the source of truth for
idle-process cost.

## What It Measures

`dormant_runtime_benchmark` has two modes:

- `slots`: builds Rust in-process runtime objects with dormant slots. Waking a
  slot claims one dormant slot, binds a small fingerprintable role, and returns
  it to the dormant pool. This measures the intended hot-path bookkeeping cost
  before IO, graph routing, or process scheduling.
- `processes`: starts already-running Rust child programs, lets them settle, and
  wakes them by sending one byte over stdin. This measures the smallest useful
  proxy for a process-to-process control-plane wake: scheduler wake, pipe write,
  child read, child exit, and parent wait.

Cold start and wake are intentionally separate. `construction_seconds` is the
cost for the controller to create or admit dormant capacity. `average_wake_us`
is the cost to use already-created capacity.

## Wake Semantics

A dormant runtime slot is usable when the controller can send a role/config
assignment to a live Manyfold runtime. In the slot benchmark that is a mutable
in-process claim and bind. In the process benchmark that is a control message to
an already-running child process.

Manyfold should not have a magic default warm-pool size. The baseline starts
from one controller instance. Extra capacity is requested explicitly with
`--capacity-instances`, and production policy should derive that bound from an
outside constraint such as memory budget, CPU headroom, tenant isolation,
availability target, or placement policy.

The Python substrate example models this as `ManyFoldController`: it tracks the
known runtime instances, maintains a small dormant buffer, and refuses to spawn
past a hard total-process ceiling. The controller owns this policy; individual
runtimes only expose the narrow operation of spawning more dormant process
slots on their node.

The production version should replace the benchmark stdin byte with a typed
control stream, usually Unix domain sockets for local runtime management or TCP
for node-to-node management. The semantic shape is the same:

1. Start or select the controller instance.
2. Admit existing capacity or spawn more instances up to the configured bound.
3. Send a typed assignment: role, graph inputs, graph outputs, lifecycle policy,
   and required resources.
4. The runtime binds streams and reports readiness.
5. If the assignment fails, the control plane retries another runtime or reports
   the readiness failure.

The benchmark does not measure full graph routing, durability, TCP ingress,
PyO3, or application initialization. Those should be added as separate workload
benchmarks when those surfaces become material.

## Baseline

Environment:

- Date: 2026-06-29.
- Host: `Darwin Lapis 24.4.0 arm64`.
- Build: `cargo build --release --bin dormant_runtime_benchmark`.
- Runs: median of 5.
- Slots workload: 0.5 seconds idle, 1,000,000 polls, 1,000,000 wakes.
- Process workload: 0.5 seconds idle before wake.
- RSS source: `ps -o rss= -p` fallback on macOS.

| Shape | Construction Seconds | Idle CPU Percent | Average Poll us | Average Wake us | RSS KiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 controller, 1 capacity instance, 10 slots | 0.000005916 | 0.002574 | 0.001328 | 0.003039 | 1296 |
| 1 controller, 10 capacity instances, 100 slots | 0.000012500 | 0.001998 | 0.001597 | 0.000722 | 1312 |

| Shape | Construction Seconds | Child CPU Percent | Average Wake us | Child RSS Total KiB | Child RSS Per Process KiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 controller, 1 spawned instance | 0.000150292 | 0.521691 | 171.125000 | 1216 | 1216.000 |
| 1 controller, 10 spawned instances | 0.001063500 | 0.496335 | 64.162500 | 12224 | 1222.400 |

Interpretation:

- A slot is effectively free to keep around at this scale. The measured wake is
  bookkeeping against an already-live runtime object.
- A separate dormant process costs about 1.2 MiB RSS on this host and wakes via
  the minimal control-plane proxy in less than 0.2 ms for the one-process case.
- Ten separate dormant processes cost about 12 MiB RSS on this host. That is an
  explicit capacity benchmark, not a default runtime promise.

## Regression Gates

Run the focused gate before changing dormant-runtime semantics:

```sh
cargo run --release --bin dormant_runtime_benchmark -- --mode slots --capacity-instances 10 --dormant-processes 10 --idle-seconds 0.5 --poll-iterations 1000000 --wake-iterations 1000000 --max-average-wake-us 0.05
cargo run --release --bin dormant_runtime_benchmark -- --mode processes --capacity-instances 10 --idle-seconds 0.5 --max-average-wake-us 5000 --max-child-rss-per-process-kib 4096
```

Use broader gates only after adding real control streams, readiness reporting,
or graph binding. Keep those as separate named workloads so regressions show
which layer got slower.
