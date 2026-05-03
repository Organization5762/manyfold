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

Smallest useful shape:

```python
from manyfold import Graph, Layer, OwnerName, Plane, Schema, StreamFamily, StreamName, Variant, route

graph = Graph()
temperature = route(
    plane=Plane.Read,
    layer=Layer.Logical,
    owner=OwnerName("sensor"),
    family=StreamFamily("environment"),
    stream=StreamName("temperature"),
    variant=Variant.Meta,
    schema=Schema.bytes("Temperature"),
)

graph.publish(temperature, b"72.4F")
latest = graph.latest(temperature)
assert latest is not None
print(latest.value)
```

## Read Next

- [Onboarding](docs/onboarding.md): repo setup, first commands, and where to look first.
- [Using Manyfold](docs/using_manyfold.md): routes, graphs, observation, flow components, and examples.
- [Performance](docs/performance.md): how to represent performance concerns as graph concerns.
- [Distributed systems catalog](docs/distributed_systems_lego_catalog.md): higher-level component ideas.
- [Wiregraph RFC](docs/rfc/wiregraph_rfc_rev2.md): the larger design target.

## What It Models

- Typed routes for logical signals.
- Replayable latest-value reads and Rx-style observation.
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
model. Start with a route, add explicit demand, then move into joins,
watermarks, planning, and taint-aware runtime behavior. The supported
examples are validated by the regular `unittest` run so they do not drift
away from the API.

**Start here: create one typed route and read it back**
- `examples/simple_latest.py`: Smallest publish/read-back example.

**Control the flow: make downstream demand visible**
- `examples/rate_matched_sensor.py`: A one-slot capacitor coalesces bursty reads behind explicit demand.

**Fuse streams: coordinate independent sensors**
- `examples/imu_fusion_join.py`: Capacitors stage accelerometer and gyro streams before an event-time join.

**Reason in time: release data by watermark progress**
- `examples/rolling_window_aggregate.py`: A capacitor discharges samples behind explicit event-time watermarks.

**Scale the graph: plan repartition work explicitly**
- `examples/cross_partition_join.py`: A repartition join with skew metrics and planner output.

**Audit the hard parts: mark nondeterminism on purpose**
- `examples/ephemeral_entropy_stream.py`: Per-request entropy derivation that taints determinism explicitly.

More involved operator, query, transport, mesh, and security coverage stays
in `tests/test_graph_reactive.py`, with archived exploratory scripts kept
under `examples/archived/`. The example manifest, README featured-example
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
uv run python -m examples.catalog --check-readme
```

## Repo Map

- `python/manyfold/`: Python wrapper API.
- `src/`: Rust in-memory runtime and PyO3 extension.
- `examples/`: runnable examples covered by tests.
- `tests/`: `unittest` suite.
- `docs/`: onboarding, usage, performance notes, release notes, and RFC docs.
