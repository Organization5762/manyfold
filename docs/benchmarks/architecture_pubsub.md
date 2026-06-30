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
- Checked-in baseline:
  `src/benchmarks/baseline/architecture_pubsub.json`.
- Workload: 100,000 iterations, 4,096 retained messages, 16 subscribers,
  32-message poll batches, 64-byte payloads, 8 topics.

| Workload | Average Operation us | Final Messages | Final Subscribers |
| --- | ---: | ---: | ---: |
| publish | 0.096278 | 4096 | 0 |
| poll | 0.334627 | 4096 | 1 |
| wildcard_fanout | 0.874094 | 4096 | 16 |
| replay | 279.481750 | 4096 | 16 |
| latest | 0.113537 | 4096 | 0 |
| retention | 0.101683 | 4096 | 0 |

`replay` is reported per replay subscription, not per retained message; each
operation drains the retained history for one new subscriber.

## Regression Gate

Run this focused gate before changing `src/architecture/pubsub.rs`:

```sh
cargo run --release --bin architecture_pubsub_benchmark -- --workload all --iterations 100000 --retained-messages 4096 --subscribers 16 --batch-size 32 --payload-bytes 64 --topic-count 8 --check-baseline
```

By default, `--check-baseline` reads the sibling baseline file for this
benchmark and allows a 20% latency regression per workload. It also rejects
shape drift and final retained/subscriber count drift. Use
`--max-regression-percent N` to temporarily widen or tighten the threshold.

Refresh the checked-in baseline only when the measured change is intentional:

```sh
cargo run --release --bin architecture_pubsub_benchmark -- --workload all --iterations 100000 --retained-messages 4096 --subscribers 16 --batch-size 32 --payload-bytes 64 --topic-count 8 --write-baseline
```
