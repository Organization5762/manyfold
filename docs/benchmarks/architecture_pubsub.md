# Architecture PubSub Benchmark

This benchmark tracks the Rust-backed `InMemoryPubSub` primitive exposed through
`manyfold.architecture.pubsub`. It measures the architecture module directly,
not the Python wrapper, sockets, graph routing, disk durability, or network IO.

## What It Measures

`architecture_pubsub_benchmark` emits JSON for these workloads:

- `publish`: append retained messages with no subscribers.
- `poll`: publish and drain a named subscription in fixed-size batches.
- `wildcard_fanout`: publish to wildcard subscribers and report delivery fanout.
- `replay`: subscribe from the retained beginning and drain retained history.
- `latest`: repeatedly read the latest retained message for matching topics.
- `retention`: publish past the retention bound and verify the retained set
  stays capped.

Use these as hot-path architecture gates. Add separate benchmarks for Python
wrapper overhead, socket ingress, graph routing, durable storage, or replicated
delivery when those layers become part of the claim.

## Baseline

Environment:

- Date: 2026-06-30.
- Host: `Darwin Lapis 24.4.0 arm64`.
- Build: `cargo run --release --bin architecture_pubsub_benchmark`.
- Workload: 100,000 iterations, 4,096 retained messages, 16 subscribers,
  32-message poll batches, 64-byte payloads, 8 topics.

| Workload | Average Operation us | Final Messages | Final Subscribers |
| --- | ---: | ---: | ---: |
| publish | 0.102790 | 4096 | 0 |
| poll | 0.329273 | 4096 | 1 |
| wildcard_fanout | 0.847722 | 4096 | 16 |
| replay | 277.989562 | 4096 | 16 |
| latest | 0.111062 | 4096 | 0 |
| retention | 0.099188 | 4096 | 0 |

`replay` is reported per replay subscription, not per retained message; each
operation drains the retained history for one new subscriber.

## Regression Gate

Run this focused gate before changing `src/architecture/pubsub.rs`:

```sh
cargo run --release --bin architecture_pubsub_benchmark -- --workload all --iterations 100000 --retained-messages 4096 --subscribers 16 --batch-size 32 --payload-bytes 64 --topic-count 8 --max-average-operation-us 1000
```

The gate is intentionally loose at first because this is the first architecture
benchmark. Tighten per-workload gates after collecting CI baselines on stable
hardware.
