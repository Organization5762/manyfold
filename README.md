# manyfold

<p align="center">
  <img src="docs/assets/manyfold-topology-graph.png" alt="A schematic logical board with overlapping graph regions and circuit-style routes" width="640">
</p>

Manyfold is a component library for execution graphs.

Its goal is to make graphs more usable and more grokkable: application signals,
runtime state, flow control, time, retention, writes, payload access, and audit
history should be representable as graph concerns instead of being hidden in
callbacks, queue names, retry loops, and side-channel logs.

The concrete mental model is a logical board. Building a Manyfold graph should
feel as direct as laying out a circuit board: routes are traces, ports are pads,
mailboxes and buffers are components, planes and layers separate concerns, and
overlapping graph regions make shared ownership, policy, and topology visible.
The point is not to draw a prettier pipeline. The point is to preserve enough
structure that an execution graph can be queried, shaped, replayed, audited,
and explained.

This repository contains an RFC-stage implementation scaffold for the
`docs/rfc/wiregraph_rfc_rev2.md` Manyfold RFC. It is not a production runtime
yet, but the API direction is concrete enough to show the shape of the library.

## Logical Boards

A normal event-driven program often flattens its design into strings and
callbacks: topic names, handler chains, queue settings, retry policies, and log
correlation ids. Those are all graph facts, but they usually live outside the
graph.

Manyfold pulls those facts into the graph. A `TypedRoute` names a signal with
ownership, stream identity, variant, plane, layer, and schema. Components such
as capacitors, resistors, watchdogs, mailboxes, windows, joins, stores, write
bindings, taints, and lineage records then make execution concerns visible at
the same level as nodes and edges.

That is where the board metaphor matters. A circuit board is concrete because
you can point at traces, pads, vias, planes, and components. Manyfold aims for
the same concreteness in software execution graphs. A logical board should let
you point at a route, see what owns it, inspect what pressure exists on it,
follow how a write becomes effective state, and explain why an output exists.

## Graphs With Depth

A graph laid onto a topology is not really flat. Nodes that are grouped together
occupy a region. Regions can overlap. Routes can cross boundaries. Some edges
belong to application state, some to transport, some to storage, some to audit,
and some to control policy.

Manyfold treats that depth as part of the model. The Z axis is the structure
created by grouping, layering, ownership, and operational policy:

- a route can sit on an application layer, device layer, transport plane, or audit plane;
- a capacitor can lift downstream pressure into visible bounded storage;
- a write binding can separate request, shadow, reported, and effective state into related strata;
- a lineage query can cut across those strata and explain how an output came to exist.

Data-flow concerns become graph concerns. That is the core design rule.

## Start Here: A Flow Story

Start with the smallest possible story. A device, model, service, or UI control
has one thing to say. You give that signal a route. The route is not just a
string topic; it carries ownership, stream identity, variant, plane, layer, and
payload schema. When a producer publishes a value, the graph closes an envelope
around it. When a consumer asks for the latest value, the schema decodes it back
into the Python type the application expects.

That first step is deliberately boring. It should feel like saying, "here is a
temperature value" or "here is the desired brightness." The important shift is
that the graph now knows what the value is, where it belongs, and how to decode
it. From there, the interesting control points become explicit instead of being
buried in callback code.

Now imagine the consumer cannot keep up. A bursty sensor can publish ten values
while the downstream display, planner, or actuator only wants one fresh value at
a time. Add a capacitor. The capacitor is graph-visible bounded storage: it can
coalesce, hold capacity, and create demand only when it has room. The program no
longer has to pretend that "subscribe faster" is a flow-control strategy. The
route can say, in the graph, "I have room for one more useful value."

Next, imagine the value is expensive or risky to open. A LiDAR scan, camera
frame, debug trace, encrypted blob, or model artifact may have cheap metadata and
large payload bytes. Route the metadata first, gate it with a resistor, and only
open the payload route when some policy selects it. The graph can expose payload
demand separately from metadata observation, which is exactly the distinction a
real edge system needs when bandwidth, memory, privacy, or battery life matters.

Then add time. Watermarks make event-time progress explicit. A rolling window
can buffer samples until the graph has seen enough time advance to release a
meaningful aggregate. This is the difference between "sleep for a bit and hope
the data arrived" and "release this window when the input stream has proven it
has moved past the boundary."

Finally, add writes. A write request is not the same thing as reported device
state, and neither is the same thing as the effective value the application
should trust. Manyfold models that difference directly with request, shadow,
reported, and effective routes. A control loop can read from one route, derive a
write for the next epoch, and expose the shared write stream so observers see the
same values the graph sees.

That is the mental model behind the examples: create a route, publish a value,
shape demand, gate release, join streams, inspect time, and make writes
auditable.

## Current Surface

The current scaffold focuses on:

- typed namespace, route, and schema identity objects;
- explicit `ReadablePort`, `WritablePort`, `WriteBinding`, and `Mailbox` surfaces;
- graph-visible capacitor, resistor, and watchdog flow primitives;
- mailbox credit, overflow, and queue inspection helpers;
- guarded write scheduling with typed retry and backoff policies;
- explicit write-shadow reconciliation with RFC-shaped coherence taints;
- lifecycle bindings as write/shadow bundles with auditable event and optional health routes;
- demand-driven rate matching for bursty streams;
- watermark-aware event-time rolling windows and aggregations;
- stream-table lookup joins and bounded streaming interval joins;
- route-level payload-demand accounting for lazy bulk payloads;
- per-route replay and retention policy overrides;
- taint-repair operators plus stream-bound taint and repair-note inspection;
- event-lineage inspection by route, trace id, causality id, or correlation id;
- route-local audit snapshots for producers, subscribers, writes, and repairs;
- catalog, latest-value, topology, validation, scheduler, and flow query helpers;
- object-first Python routes and shared-stream `ReadThenWriteNextEpochStep` composition;
- embedded device profile helpers for scalar and bulk sensors;
- a named reference example suite that tracks the RFC examples.

The intended Python wrapper surface is narrow:

- build a `TypedRoute` with `OwnerName`, `StreamFamily`, `StreamName`, and `Schema`
- build a `ReadThenWriteNextEpochStep` from a read stream and an output route
- `graph.latest(route)` for snapshot reads
- `graph.observe(route)` for Rx subscriptions
- `graph.publish(route, payload)` for writes
- `graph.pipe(source, route)` for wiring an Rx source into a route
- `graph.install(step)` for attaching a `ReadThenWriteNextEpochStep`
- `graph.run_control_loop(name)` for advancing a control loop once
- `source(route)` and `sink(route)` for naming signal roles without adding runtime nodes
- `graph.capacitor(source=..., sink=..., capacity=..., demand=..., immediate=...)` for active bounded storage that creates demand until full
- `graph.resistor(source=..., sink=..., gate=..., release=...)` for explicit pass-through, gated, or release-pulsed flow shaping
- `graph.watchdog(reset_by=..., output=..., after=..., clock=...)` for missing-flow detection such as Raft election timeouts
- `graph.flow_snapshot(route_or_port)` for the current route credit view
- `graph.describe_edge(source=..., sink=...)` for RFC-ordered edge flow descriptor composition
- `graph.configure_flow_defaults(FlowPolicy(...))` for graph-wide edge flow defaults
- `graph.configure_source_flow(route, FlowPolicy(...))` for source-side edge defaults
- `graph.configure_sink_flow(route, FlowPolicy(...))` for sink-side edge requirements
- `graph.scheduler_snapshot(route=None)` for queued guarded-write state and retry gates
- `graph.mailbox_snapshot(mailbox)` for mailbox depth/overflow inspection
- `graph.lifecycle(owner, family, intent_schema=...)` for RFC-shaped device/runtime lifecycle bindings
- `graph.configure_retention(route, RouteRetentionPolicy(...))` for explicit replay/retention semantics
- `graph.route_audit(route)` for route-local producer/subscriber/write/taint audit summaries
- `graph.lineage(route=None, causality_id=..., correlation_id=...)` for retained causality/correlation inspection
- `graph.filter(source, predicate=...)` for explicit typed metadata/value filtering
- `graph.window(source, size=..., trigger=..., partition_by=...)` for rolling windows with optional trigger- and partition-scoped release
- `graph.window_aggregate(source, size=..., aggregate=..., trigger=..., partition_by=...)` for rolling window aggregations with explicit trigger and partition policy
- `graph.window_by_time(source, width=..., watermark=..., partition_by=...)` for event-time windows driven by explicit watermark progress with per-partition buffers
- `graph.window_aggregate_by_time(source, width=..., aggregate=..., watermark=..., partition_by=...)` for watermark-aware event-time aggregations with partition-scoped state
- `graph.lookup_join(left, right_state, combine=...)` for stream-table joins against materialized state
- `graph.interval_join(left, right, within=..., combine=...)` for bounded streaming joins
- `FileStore(root).prefix(...)` for FoundationDB-style byte keyspaces on local files
- `EventLog(name, keyspace, schema)` for typed append/committed flow over a byte keyspace
- `SnapshotStore(name, keyspace, schema)` for typed latest-value state over a byte keyspace
- `Consensus.install(graph, ...)` for a default Raft-style leader-election component built from capacitors, resistors, and watchdogs
- `Memory(path).remember(graph, route)` and `Memory(path).resume(graph, route)` for disk-backed route memory

These route inputs are object-based rather than ad hoc strings. `Schema` also
owns payload encoding/decoding, so `latest(route)` and `observe(route)` can
return typed values instead of raw payload bytes.

`ReadThenWriteNextEpochStep` lives in the primary primitives module because it is
becoming a composition unit: it has one required input stream (`read`), one
required output route (`output`), and one shared derived stream (`write`). Any
subscriber to `write` observes the same emitted values that the graph sees when
the step is installed and started.

## Use Cases Worth Building

**Embedded sensor gateways.** Manyfold is a natural fit for UART, SPI, BLE, CAN,
and serial-adjacent systems where raw physical signals need to become logical
application streams. A raw temperature sensor can feed a smoothed logical route.
A battery monitor can publish cheap metadata frequently while storing bulk
diagnostics behind lazy payload demand. A firmware bridge can declare mailbox
capacity and overflow behavior instead of hiding those decisions inside a driver
thread.

**Robotics and perception.** Sensor fusion is mostly a story about time,
identity, and pressure. IMU streams, wheel odometry, camera frames, LiDAR
metadata, pose estimates, and planner outputs all want separate route identities
but shared lineage. Capacitors can stage fast producers. Event-time joins can
bind accelerometer and gyro samples within a bounded interval. Lazy payload
opening can keep full frames cold until a planner, debugger, or training capture
actually asks for them.

**Industrial control and actuator shadows.** Desired, reported, and effective
state are different facts. Treating them as separate routes makes brightness
control, motor targets, valve positions, HVAC setpoints, and calibration changes
much easier to reason about. The current write-binding and lifecycle surfaces
point toward systems where every command can be traced from request through
guarded scheduling to reported reconciliation.

**Realtime dashboards.** UI state is often a graph already: live counters,
health indicators, replayable status, operator commands, and debug panels are
all different views of the same flow. Manyfold's `latest` and `observe` calls
cover the common dashboard path, while route audit and lineage queries make it
possible to answer "why did the screen change?" without reconstructing history
from unrelated logs.

**Edge AI and selective inference.** Model pipelines often need to separate
metadata routing from expensive tensor or media payloads. A camera can publish
frame metadata, a policy route can decide which frames deserve inference, and a
payload route can open only the selected bytes. Taint marks can label
nondeterministic or privacy-sensitive transforms, while lineage records preserve
which model input produced which output.

**Distributed coordination.** The default consensus component is intentionally
small, but it shows why graph-visible primitives are useful for coordination.
Election ticks, watchdog timeouts, request-vote messages, heartbeats, quorum
state, and replicated log entries can be named routes instead of hidden channels.
That makes the coordination mechanism inspectable and testable as a graph.

**Streaming analytics without hidden magic.** Windowed aggregates, lookup joins,
interval joins, repartition plans, skew metrics, and retention policies are all
places where a streaming system can surprise its users. Manyfold's approach is
to surface those decisions as descriptors and snapshots. The goal is not just to
produce a result, but to preserve enough structure to explain the cost and
correctness envelope of that result.

**Security, audit, and compliance flows.** Some data is encrypted. Some payloads
should be visible only to principals with explicit grants. Some values are
nondeterministic, repaired, redacted, or derived from sensitive inputs. By
tracking capabilities, taints, repair notes, and lineage as graph concepts, this
approach gives a future implementation a place to enforce policy and a query
surface to explain enforcement decisions.

The common thread across those use cases is that flow control, payload access,
write intent, time, joins, and auditability are all first-class. That lets a
small script grow toward a real runtime without changing the shape of the
program every time another operational concern appears.

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

## Project Layout

- `pyproject.toml`: maturin/PyO3 Python packaging entrypoint
- `Cargo.toml`: Rust crate for the native core and Python extension
- `src/`: Rust in-memory runtime, typed refs, descriptors, envelopes, mailboxes, queries, and control-loop stubs
- `python/manyfold/`: Python-facing ergonomic wrapper layer
- `python/manyfold/primitives.py`: primary nouns and verbs for the Python API
- `python/manyfold/components.py`: convenient components built from graph primitives
- `python/manyfold/embedded.py`: RFC 21 embedded device profile helpers and validation
- `python/manyfold/reference_examples.py`: RFC 23 reference example suite registry
- `examples/`: executable API examples that are also covered by the test suite

## Best Practices

When extending this repository, prefer a narrow, explicit, well-documented API
over a broad convenience surface.

- Write tests for every meaningful behavior change. Keep the smallest,
  easiest-to-understand examples close to the usage they demonstrate in
  `examples/` and mirror them with straightforward assertions in
  `tests/test_examples.py`. Put more complex integration, reactive, and
  repository-level coverage in the rest of the `tests/` directory.
- Write extensive docstrings and supporting documentation for public modules,
  classes, and functions. If a section of code is non-obvious, add a concise
  comment that explains the invariant, constraint, or design reason behind it.
- Always add types. Prefer signatures, return types, and data shapes that make
  the code self-describing, and keep pushing the API toward something a new
  reader can grok quickly without tracing through multiple layers of code.
- Only elevate essential concepts into the primary API. Keep helper functions
  and intermediate building blocks semi-private by default, and use a leading
  underscore liberally for methods and functions that support the implementation
  but should not become part of the stable surface area.

## Verification

Use `cargo test` for native verification.

Use `uv sync` to provision the Python environment and build the extension into
the local `.venv`. Then run Python verification with `uv run`.

Typical Python commands:

- `uv run python -m unittest discover -s tests -p 'test_*.py'`
- `uv run python -m manyfold.rfc_checklist_gen --check`
- `uv run manyfold-example-catalog --check`
- `uv run manyfold-example-catalog --list reference`
- `uv run python -m examples.catalog --check-manifest`
- `uv run python -m examples.catalog --check-readme`

Generate PyO3 `.pyi` stubs with `cargo run --features stub-gen --bin stub_gen`.
If the default interpreter is older than Python 3.10, set `PYO3_PYTHON` to a
3.10+ interpreter first, for example
`PYO3_PYTHON=/opt/homebrew/Cellar/python@3.14/3.14.3_1/Frameworks/Python.framework/Versions/3.14/bin/python3.14 cargo run --features stub-gen --bin stub_gen`.
Regenerate the RFC implementation checklist with
`uv run manyfold-rfc-checklist` (or `uv run python -m manyfold.rfc_checklist_gen`).

The Python package now targets Python 3.10+ because `reactivex==5.0.0a2`
requires it.

## Publishing

The package is configured for PyPI as `manyfold` using maturin and PyO3.
Release builds are handled by `.github/workflows/pypi.yml`, which publishes
through PyPI Trusted Publishing. The workflow builds an sdist plus Linux,
macOS Intel, macOS Apple Silicon, and Windows wheels.

Before publishing a release:

- make sure `version` matches in `pyproject.toml` and `Cargo.toml`;
- run `cargo test`;
- run `uv run python -m unittest discover -s tests -p 'test_*.py'`;
- run `uv run --with build python -m build`;
- run `uv run --with twine twine check dist/*`.

For the first PyPI release, create a pending Trusted Publisher in PyPI:

- PyPI project name: `manyfold`
- Owner: `Organization5762`
- Repository: `manyfold`
- Workflow filename: `pypi.yml`
- Environment name: `pypi`

Then publish the GitHub release:

```sh
git tag v0.1.0
git push origin v0.1.0
```

Create a GitHub release from `v0.1.0` and publish it. The release event runs
the PyPI workflow. You can also start the same workflow manually from GitHub
Actions with `workflow_dispatch` after the pending publisher exists.
