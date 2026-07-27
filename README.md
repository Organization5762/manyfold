# manyfold

<p align="center">
  <img src="docs/assets/manyfold-topology-graph.png" alt="A schematic logical board with overlapping graph regions and circuit-style routes" width="640">
</p>

Manyfold is a Python component library for programs that become easier to
understand when their data flow is explicit.

Start with a typed stream. If the program grows, promote its routing,
computation, buffering, demand, and execution policy into an inspectable graph.
The same values remain visible from application code instead of disappearing
behind callbacks and queues.

This repository is an RFC-stage Python package with a PyO3/Rust extension. It
is runnable, but it is not a production runtime yet. The supported examples are
tested with the package.

## Start with a changing value

```sh
uv sync
```

```python
from dataclasses import dataclass

from manyfold.architecture import PubSub


@dataclass(frozen=True)
class Temperature:
    degrees: float
    unit: str


temperature = PubSub(
    topic="sensor.environment.temperature",
    schema=Temperature,
)

temperature.publish(Temperature(degrees=72.4, unit="F"))
temperature.publish(Temperature(degrees=72.9, unit="F"))

latest = temperature.latest()
if latest is None:
    raise RuntimeError("temperature stream is empty")

print(f"latest #{latest.seq_source}: {latest.degrees}{latest.unit}")
```

Output:

```text
latest #2: 72.9F
```

That is the complete publish/read path. You do not need to write SQL and call
`latest()` for the same value. `latest()` is the concise choice when you want
current state; SQL is available when the question is more specific.

The explicit topic gives the stream a stable application identity. Its dotted
parts read from broad ownership to a specific signal: `sensor` owns the
`environment` family, which contains `temperature`. For local experiments,
`PubSub()` can create an ephemeral topic and infer a dataclass or Pydantic
schema from the first value.

## Choose the surface that matches the problem

| If you want to… | Start with… | Why |
| --- | --- | --- |
| Publish changing state and read its current value | `PubSub.publish()` and `latest()` | Smallest typed stream surface |
| Filter, aggregate, or inspect retained history | `PubSub` query helpers or `query()` | The retained stream is exposed as a SQL relation |
| React to new values | `PubSub.subscribe()`, `map()`, and `filter()` | Disposable live callbacks without building a graph |
| Make computation or flow policy inspectable | `Graph`, typed routes, and graph components | Routing, demand, storage, and execution become explicit |
| Adapt a device or outside system | Architecture interfaces backed by `PubSub` | Lifecycle, schema, data, and errors share one queryable stream |

These surfaces are a progression, not a checklist. Use only the layer that
answers the problem you have.

## Ask a stream a specific question

Use built-in helpers for common operations such as `latest()`, `average()`,
`where()`, and `take()`. Use SQL when you need a custom projection, filter,
window, or join:

```python
warm_readings = temperature.query(
    """
    SELECT offset + 1 AS sequence, degrees, unit
    FROM stream
    WHERE degrees >= :minimum
    ORDER BY offset
    """,
    {"minimum": 72.5},
)
print([(row.sequence, row.degrees, row.unit) for row in warm_readings])
```

Output:

```text
[(2, 72.9, 'F')]
```

The `stream` relation is scoped to this `PubSub` topic. Retention is bounded by
`retained_messages`, so a query describes retained stream state rather than an
unbounded event archive. Structured schema fields become typed SQL columns;
the encoded value remains available as `payload`.

## Make behavior part of the graph

Callbacks are enough when reacting to a value is the whole job. Use `Graph`
when the connection itself matters: when someone should be able to inspect
where a value came from, how it was transformed, where it runs, or what bounds
its flow.

```python
from manyfold import Graph, Schema, route

graph = Graph()
temperature = route(
    owner="sensor",
    family="environment",
    stream="temperature",
    schema=Schema.float(name="Temperature"),
)
average = temperature.derivative_route(
    stream="average",
    schema=Schema.float(name="AverageTemperature"),
)

subscription = (
    graph.observe(temperature, replay_latest=False)
    .moving_average(window_size=3)
    .connect(average)
)

for reading in (72.4, 72.9, 73.7):
    graph.publish(temperature, reading)

latest_average = graph.latest(average)
subscription.dispose()
if latest_average is None:
    raise RuntimeError("average route is empty")

print(f"average: {latest_average.value:.1f}F")
```

Output:

```text
average: 73.0F
```

The moving average is now a graph-visible node between two typed routes.
Manyfold uses the same model for bounded capacitors, demand gates, mailboxes,
joins, windows, watchdogs, write shadows, thread placement, and other execution
decisions. Add those components when the behavior must be operated or
explained—not merely because they exist.

## Bring outside state into the same model

Architecture interfaces normalize an external source's lifecycle, schema,
data, and errors into a `PubSub` stream. For example,
`BluetoothControllerInterface` keeps controller connect/disconnect cycles beside
sampled state, while `SerialBusInterface` keeps bus discovery and schema changes
beside frames.

This is useful when downstream code needs to answer both “what is the latest
reading?” and “was the device connected with the expected schema?” without
maintaining a separate state convention. See
[Using Manyfold](docs/using_manyfold.md#interfaces) for the complete examples.

## The mental model

Think of a Manyfold graph as a logical board:

- routes are named, typed traces;
- components transform or control flow;
- ports show the boundary of a part;
- the manifest and runtime snapshots expose what is connected and retained.

The public `manyfold` namespace covers the common graph API. Application streams
and interface adapters live under `manyfold.architecture`; lower-level topology
descriptions live under `manyfold.architecture.native`; advanced graph helpers
live under `manyfold.graph`.

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

## Go deeper

- [Using Manyfold](docs/using_manyfold.md) develops the stream and graph APIs.
- [Performance](docs/performance.md) explains how flow and performance concerns
  become graph components.
- [Onboarding](docs/onboarding.md) covers repository setup and contribution.
- [Wiregraph RFC](docs/rfc/wiregraph_rfc_rev2.md) describes the larger design
  target.

## Verify

Use `uv run` for Python commands.

```sh
cargo test
uv run ruff check
uv run python -m unittest discover -s tests -p 'test_*.py'
uv run python -m manyfold.rfc_checklist_gen --check
uv run manyfold-example-catalog --check
uv run python -m examples.catalog --check-readme
```

## Repo Map

- `python/manyfold/`: Python wrapper API.
- `src/`: Rust in-memory runtime and PyO3 extension.
- `examples/`: runnable examples covered by tests.
- `tests/`: `unittest` suite.
- `docs/`: onboarding, usage, performance notes, release notes, and RFC docs.
