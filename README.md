# manyfold

This repository now contains a first-pass implementation scaffold for the
`docs/rfc/wiregraph_rfc_rev2.md` Manyfold RFC.

## Layout

- `pyproject.toml`: maturin/PyO3 Python packaging entrypoint
- `Cargo.toml`: Rust crate for the native core and Python extension
- `src/`: Rust in-memory runtime, typed refs, descriptors, envelopes, mailboxes, queries, and control-loop stubs
- `python/manyfold/`: Python-facing ergonomic wrapper layer
- `python/manyfold/primitives.py`: primary nouns and verbs for the Python API
- `python/manyfold/components.py`: convenient components built from graph primitives
- `python/manyfold/embedded.py`: RFC 21 embedded device profile helpers and validation
- `python/manyfold/reference_examples.py`: RFC 23 reference example suite registry
- `examples/`: executable API examples that are also covered by the test suite

## Status

This is an RFC stub implementation, not a production runtime. The current code focuses on:

- typed namespace/route/schema identity objects,
- explicit `ReadablePort`, `WritablePort`, `WriteBinding`, and `Mailbox` surfaces,
- descriptor and envelope scaffolding,
- graph-visible capacitor, resistor, and watchdog flow primitives,
- mailbox credit, overflow, and queue inspection helpers,
- guarded write scheduling with typed retry/backoff policies,
- explicit write-shadow reconciliation with RFC-shaped coherence taints,
- lifecycle bindings as specialized write/shadow bundles with auditable event and optional health routes,
- explicit demand-driven rate matching for bursty streams,
- watermark-aware event-time rolling windows and aggregations,
- stream-table lookup joins against materialized state views,
- route-level payload-demand accounting plus route-level payload-store retention semantics for lazy bulk payloads,
- explicit per-route replay/retention policy overrides in the Python layer,
- explicit taint-repair operators plus stream-bound taint and repair-note query inspection,
- explicit event-lineage inspection by route, trace id, causality id, or correlation id,
- route-local audit snapshots that summarize producers, subscribers, related write requests, and taint repairs,
- catalog/latest/topology/validation query helpers,
- a minimal `ControlLoop` epoch stub,
- Python object-first routes and shared-stream `ReadThenWriteNextEpochStep` composition,
- embedded device profile helpers for scalar and bulk sensors,
- a named reference example suite that tracks the RFC examples and runs the supported subset,
- Python bindings via PyO3 in the same layout as the referenced project style.

## API Design Rules

The repository follows four implementation rules:

- write both very small examples and deeper behavioral tests;
- prefer extensive docstrings, targeted comments for non-obvious logic, and README-level guidance;
- add types aggressively and shape APIs around understandable objects rather than stringly calls;
- keep the top-level `manyfold` namespace narrow, with advanced helpers living under `manyfold.graph` and helper internals prefixed with `_`.

The intended Python wrapper surface is deliberately narrow:

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

## Why This Approach Is Useful

Manyfold is trying to make the moving parts of reactive systems visible without
making application code feel like a distributed-systems paper. A normal
event-driven program often hides the hardest questions in callbacks, queue
names, string topics, and retry loops: who owns this signal, what schema is it,
is the latest value replayable, what happens when the consumer is slower than
the producer, did this write request take effect, and can we explain why this
output exists?

The current approach turns those questions into graph-shaped objects. A
`TypedRoute` names a signal with an owner, family, stream, variant, plane, layer,
and schema. `publish`, `latest`, `observe`, and `pipe` give the small everyday
API. Capacitors, resistors, watchdogs, mailboxes, windows, joins, retention
policies, write shadows, taints, and lineage records then add behavior in places
where the graph can inspect it.

That is the sales pitch: instead of building a clever pipeline that becomes
opaque as soon as it works, you build a flow that can be queried, audited,
paused, shaped, replayed, and explained. The implementation in this repository
is still an RFC scaffold, but the API direction is already concrete enough to
show what kinds of systems this style can support.

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
