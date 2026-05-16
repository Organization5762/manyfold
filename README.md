# manyfold

<p align="center">
  <img src="docs/assets/manyfold-topology-graph.png" alt="A schematic logical board with overlapping graph regions and circuit-style routes" width="640">
</p>

Manyfold is a component library for execution graphs.

It helps make graph-shaped programs easier to build, inspect, and explain.
Routes, schemas, buffers, demand, time, payload access, writes, taints, and
lineage are modeled as graph concerns instead of being hidden in callback code
or queue configuration.

Think of it as a logical board: routes are traces, ports are pads, components
shape execution, and overlapping regions show where ownership, policy, and data
flow meet.

This repository is an RFC-stage Python package with a PyO3/Rust extension. It
is not a production runtime yet, but the package is runnable and the examples
exercise the supported surface.

## Start Fast

```sh
uv sync
uv run python examples/simple_latest.py
uv run python -m unittest tests.test_examples
```

## A Small Graph in Motion

### Publish Values

```python
from manyfold import Graph, Schema, route

graph = Graph()
temperature = route(
    owner="sensor",
    family="environment",
    stream="temperature",
    schema=Schema.bytes(name="Temperature"),
)

graph.publish(temperature, b"72.4F")
graph.publish(temperature, b"72.9F")
latest = graph.latest(temperature)
assert latest is not None
print(f"latest #{latest.closed.seq_source}: {latest.value!r}")
```

Output:

```text
latest #2: b'72.9F'
```

The fields are the parts of the graph name:

- `owner` is the component or subsystem responsible for the signal.
- `family` groups related streams.
- `stream` names this specific signal.
- `schema` says how payloads are encoded and decoded.

Basic routes default to read/logical/meta, so the first example stays focused
on the moving signal. Pass explicit `plane`, `layer`, or `variant` when that
role matters.

### Stats: Compute Values

```python
temperature = route(
    owner="sensor",
    family="environment",
    stream="temperature",
    schema=Schema.float(name="Temperature"),
)
average_temperature = temperature.derivative_route(
    stream="average_temperature",
    schema=Schema.float(name="AverageTemperature"),
)

subscription = graph.observe(temperature, replay_latest=False).moving_average(
    window_size=3
).connect(average_temperature)
for reading in (72.4, 72.9, 73.7):
    graph.publish(temperature, reading)
subscription.dispose()

latest_average = graph.latest(average_temperature)
assert latest_average is not None
print(f"average: {latest_average.value:.1f}F")

node = next(
    node
    for node in graph.diagram_nodes()
    if dict(node.metadata).get("statistic") == "moving_average"
)
print(dict(node.metadata))
```

Output:

```text
average: 73.0F
{'statistic': 'moving_average', 'storage': 'sliding_capacitor', 'window_size': '3'}
```

The shape is the same: computed values are just values published to another
typed route. The moving average also renders as a graph-visible node backed by
a sliding capacitor, so derived state and operational inspection stay in the
same vocabulary.

### Model Consensus

```python
from manyfold import Consensus

consensus = Consensus.install(graph, nodes=("node-a", "node-b"))
consensus.tick(1)
consensus.tick(2)
consensus.propose(1, "set mode=auto")
consensus.propose(2, "set temp=21")

print(consensus.latest_leader())
print(consensus.latest_log())
```

Output:

```text
('node-a', 3, True)
((1, 'set mode=auto'), (2, 'set temp=21'))
```

The consensus component uses Raft-shaped leader election and replicated-log
concepts from Diego Ongaro and John Ousterhout's
[“In Search of an Understandable Consensus Algorithm”](https://www.usenix.org/conference/atc14/technical-sessions/presentation/ongaro)
(USENIX ATC 2014).

## Read Next

- [Onboarding](docs/onboarding.md): repo setup, first commands, and where to look first.
- [Using Manyfold](docs/using_manyfold.md): routes, graphs, observation, flow components, and examples.
- [Performance](docs/performance.md): how to represent performance concerns as graph concerns.
- [Distributed systems catalog](docs/distributed_systems_lego_catalog.md): higher-level component ideas.
- [v1.0.0 Vision](docs/v1_0_0_vision.md): a far-off but reachable target for distributed graph computing and primitive building.
- [Wiregraph RFC](docs/rfc/wiregraph_rfc_rev2.md): the larger design target.

## What It Models

- Typed routes for logical signals.
- Replayable latest-value reads and Rx-style observation.
- Graph-visible node thread placement for main, background, pooled, or isolated
  execution.
- Graph-visible capacitors, resistors, watchdogs, mailboxes, windows, and joins.
- Explicit demand, retention, lazy payload access, and write-shadow state.
- Lineage, taints, route audit snapshots, and topology queries.
- Local file-backed stores and a small consensus component scaffold.

The public Python surface is intentionally narrow at the top level. Advanced
helpers live under `manyfold.graph`, and the examples are the best way to see
which parts are supported today.

## Examples

<!-- manyfold:featured-examples:start -->
The `examples/` directory is organized as a short path through the mental
model. Start with a route, derive values, add explicit demand, then move
into joins, watermarks, planning, consensus, and taint-aware runtime behavior. The supported
examples are validated by the regular `unittest` run so they do not drift
away from the API.

**Start here: publish changing state and read the latest value**
- [examples/simple_latest.py](examples/simple_latest.py): Smallest changing-signal publish/read-back example.

**Layer computation: publish derived values**
- [examples/average_temperature.py](examples/average_temperature.py): Compute and publish a rolling average from temperature samples.

**Control the flow: make downstream demand visible**
- [examples/rate_matched_sensor.py](examples/rate_matched_sensor.py): A one-slot capacitor coalesces bursty reads behind explicit demand.

**Fuse streams: coordinate independent sensors**
- [examples/imu_fusion_join.py](examples/imu_fusion_join.py): Capacitors stage accelerometer and gyro streams before an event-time join.

**Reason in time: release data by watermark progress**
- [examples/rolling_window_aggregate.py](examples/rolling_window_aggregate.py): A capacitor discharges samples behind explicit event-time watermarks.

**Scale the graph: plan repartition work explicitly**
- [examples/cross_partition_join.py](examples/cross_partition_join.py): A repartition join with skew metrics and planner output.

**Capstone: wire a Raft-shaped consensus component**
- [examples/raft_demo.py](examples/raft_demo.py): The Consensus component wires Raft election defaults from graph primitives.

**Audit the hard parts: mark nondeterminism on purpose**
- [examples/ephemeral_entropy_stream.py](examples/ephemeral_entropy_stream.py): Per-request entropy derivation that taints determinism explicitly.

More involved operator, query, transport, mesh, and security coverage stays
in [tests/test_graph_reactive.py](tests/test_graph_reactive.py), with archived exploratory scripts kept
under [examples/archived/](examples/archived/). The example manifest, README featured-example
list, and RFC reference suite all derive from the shared example catalog,
so supported versus archived status lives in one place.
<!-- manyfold:featured-examples:end -->

## Verify

Use `uv run` for Python commands.

```sh
cargo test
uv run ruff check
uv run python -m unittest discover -s tests -p 'test_*.py'
uv run python -m manyfold.rfc_checklist_gen --check
uv run manyfold-example-catalog --check
uv run manyfold-example-catalog --list reference
uv run python -m examples.catalog --check-manifest
uv run python -m examples.catalog --check-readme
```

## Repo Map

- `python/manyfold/`: Python wrapper API.
- `src/`: Rust in-memory runtime and PyO3 extension.
- `examples/`: runnable examples covered by tests.
- `tests/`: `unittest` suite.
- `docs/`: onboarding, usage, performance notes, release notes, and RFC docs.
