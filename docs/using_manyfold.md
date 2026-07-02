# Using Manyfold

Manyfold gives ordinary Python code a graph-shaped runtime surface. The small
daily API is:

- name a signal with a typed route;
- publish values to that route;
- read the latest value or observe changes;
- add graph-visible components when execution behavior matters.

## Routes

A route is more than a topic string. It records where a signal belongs.

```python
from manyfold import Graph, Schema, route

graph = Graph()
temperature = route(
    owner="sensor",
    family="environment",
    stream="temperature",
    schema=Schema.bytes(name="Temperature"),
)
```

Basic routes default to the common read/logical/meta shape. The route still
carries ownership, stream identity, plane, layer, variant, and schema; those
fields are what make the graph inspectable later. Pass explicit `plane`,
`layer`, or `variant` only when the route needs a less common role.

For a first route, read the fields as:

- `owner`: the component or subsystem responsible for the signal.
- `family`: a group for related streams.
- `stream`: this specific signal inside the family.
- `schema`: the encode/decode contract for values on the route.

## Publish and Read

```python
from dataclasses import dataclass

from manyfold.architecture import PubSub


@dataclass(frozen=True)
class Temperature:
    degrees: float
    unit: str


temperature = PubSub()

temperature.publish(Temperature(degrees=72.4, unit="F"))
temperature.publish(Temperature(degrees=72.9, unit="F"))
latest = temperature.latest()
latest_row = temperature.query_one(
    """
    SELECT pad_name, offset + 1 AS seq_source, degrees, unit
    FROM stream
    ORDER BY event_time DESC, process_sequence DESC
    LIMIT 1
    """
)
if latest is not None and latest_row is not None:
    print(f"latest #{latest_row['seq_source']}: {latest.degrees}{latest.unit}")
```

Output:

```text
latest #2: 72.9F
```

`PubSub` is a stream backed by PubSub delivery and a Rust SQL
stream processor, so `latest()` is an ordinary ordered `query_one(...)` under
the hood. If no topic is provided, the stream gets an ephemeral UUID5 topic.
`latest()` returns a row object, so SQL-backed fields can be read as
`latest.degrees` or `latest["degrees"]`. If no schema is provided, the first
structured model publish fixes the stream schema lazily. The stream materializes
logical schema fields into the SQL table. The current runtime encodes model
values as FlatBuffer bytes in `payload`.
Pydantic models work through their `model_fields` and `model_dump()` surface;
dataclasses work through type annotations.
Callers may still pass generated FlatBuffer bytes, builder `Output()`, or table
objects. The queue assigns a default `event_time` when callers omit it, and
`key` defaults to `None`.

Manyfold-native architecture elements remain available from
`manyfold.architecture.native` for lower-level topology descriptions, but the
stream API is the primary application surface. Behavior-heavy substrates such
as PubSub stay backed by the runtime implementation.

## Observability

Metrics and logs use the same architecture shape as application data. The
Rust runtime owns the observability records, a destination owns PubSub topics,
and the current process attaches as a Manyfold worker. The default metric
destination emits OpenTelemetry-compatible histogram records without requiring
an OpenTelemetry package in-process:

```python
from manyfold.architecture import default_metrics_destination

metrics = default_metrics_destination()
metrics.record_histogram(
    "pubsub.publish.duration",
    0.004,
    unit="s",
    attributes={"topic": "heart.input"},
)
```

The default log destination writes ordinary process logs to a rotating local
file and can collect recent lines into a PubSub log topic. Rotation defaults to
100 MB per file with 5 backup files:

```python
import logging

from manyfold.architecture import default_log_destination

logs = default_log_destination()
handler = logs.configure_root_logger()
logging.getLogger("heart").info("renderer started")
handler.flush()
records = logs.collect_local_logs(limit=1)
print("renderer started" in records[0].body)
```

Output:

```text
True
```

## Observe

```python
subscription = graph.observe(temperature, replay_latest=False).subscribe(
    lambda envelope: print(envelope.value)
)

graph.publish(temperature, b"72.5F")
subscription.dispose()
```

Output:

```text
b'72.5F'
```

Observation is useful for application callbacks. When a callback starts to hide
state, pressure, policy, or replay behavior, model that behavior as a graph
component instead.

Pipelines can also make thread placement graph-visible. A placement applies to
everything downstream in that fluent chain until another placement is selected
or restored:

```python
subscription = (
    graph.observe(temperature, replay_latest=False)
    .on_pooled_thread()
    .map(lambda value: value.removeprefix("raw:"), name="parse")
    .on_main_thread()
    .map(lambda value: value.strip(), name="trim")
    .return_to_prior_thread()
    .callback(print, name="print-temperature")
)
```

Use `.on_main_thread()` for work that must run on the frame thread,
`.on_background_thread()` or `.on_pooled_thread()` for shared worker pools, and
`.on_isolated_thread()` for a node that must be the only work on its thread.
Use `.return_to_prior_thread()` after a scoped operation to restore the previous
explicit placement for following nodes.

## Contexts

Use `Graph.context(name=...)` when a set of routes and nodes should be exposed
as one graph-visible part:

```python
from manyfold import Graph, Plane, Schema, Variant, route

graph = Graph()
command = route(
    plane=Plane.Write,
    owner="board",
    family="control",
    stream="command",
    variant=Variant.Request,
    schema=Schema.bytes(name="BoardCommand"),
)
reading = route(
    owner="board",
    family="sensor",
    stream="reading",
    schema=Schema.bytes(name="BoardReading"),
)

with graph.context(name="board") as board:
    graph.observe(command, replay_latest=False)
    graph.publish(reading, b"72.4F")

print(board.input_routes)
print(board.output_routes)
```

Output:

```text
('write.logical.board.control.command.request.v1',)
('read.logical.board.sensor.reading.meta.v1',)
```

Nested contexts link child parts to their parents. Routes observed or connected
as sources become inputs; routes published, registered as read/state ports, or
connected as sinks become outputs. Diagram nodes registered inside a context
are linked to that context in rendered diagrams.

## Stats

Stats layer onto a stream pipeline and publish derived values like any other
route:

```python
temperature_value = route(
    owner="sensor",
    family="environment",
    stream="temperature_value",
    schema=Schema.float(name="Temperature"),
)
average_temperature = temperature_value.derivative_route(
    stream="average_temperature",
    schema=Schema.float(name="AverageTemperature"),
)

graph.observe(temperature_value, replay_latest=False).moving_average(
    window_size=3
).connect(average_temperature)
```

`moving_average` renders as a graph-visible node with metadata such as
`statistic="moving_average"`, `storage="sliding_capacitor"`, and the configured
`window_size`.

## Add Execution Components

A capacitor makes downstream demand explicit:

```python
graph.capacitor(
    source=source_route,
    sink=sampled_route,
    capacity=1,
    demand=demand_route,
)
```

A resistor gates or releases flow:

```python
graph.resistor(
    source=metadata_route,
    sink=selected_route,
    gate=policy_route,
)
```

Windows and joins can be written directly against stream SQL relations:

```sql
SELECT *
FROM stream
WHERE event_time BETWEEN :watermark - :width + 1 AND :watermark;

SELECT left_payload, right_payload, message_key
FROM joined_stream;

WITH latest_state AS (
  SELECT payload
  FROM right_stream
  ORDER BY event_time DESC, process_sequence DESC
  LIMIT 1
)
SELECT left_stream.payload, latest_state.payload
FROM left_stream
CROSS JOIN latest_state
```

Use `stream.query(...)` for SQL scoped to one PubSub stream and
`stream.query_one(...)` when the SQL must return at most one row. The queue
assigns a default `event_time` when callers do not provide one; distributed
queue implementations own that ordering contract.

## Inspect

Manyfold is useful when the runtime can answer operational questions:

```python
graph.flow_snapshot(route_or_port)
graph.scheduler_snapshot(route=None)
graph.mailbox_snapshot(mailbox)
graph.route_audit(route)
```

Use these surfaces when debugging behavior that would otherwise live in logs,
queue dashboards, or ad hoc counters.

## Manifest

Use `Graph.manifest()` when you need a reviewable snapshot of graph identity,
routes, edges, runtime nodes, middleware, links, and mesh primitives:

```python
graph.connect(source=temperature, sink=average_temperature)
manifest = graph.manifest()

print(manifest.manifest_version)
print(manifest.edges[0].source.display())
print(manifest.edges[0].sink.display())
```

Output:

```text
manyfold.graph.manifest.v0
read.logical.sensor.environment.temperature.meta.v1
read.logical.sensor.environment.average_temperature.meta.v1
```

The Python manifest keeps routes, edges, descriptors, links, and mesh
primitives as objects. Use `Graph.manifest_json()` only when the snapshot should
cross the serialization boundary: committed, diffed, or attached to a bug
report.

## Example Path

Start here:

```sh
uv run python examples/simple_latest.py
uv run python examples/rate_matched_sensor.py
uv run python examples/imu_fusion_join.py
uv run python examples/rolling_window_aggregate.py
```

Then use the catalog:

```sh
uv run manyfold-example-catalog --list reference
```
