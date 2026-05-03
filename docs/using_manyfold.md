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
graph.publish(temperature, b"72.4F")
graph.publish(temperature, b"72.9F")
latest = graph.latest(temperature)

if latest is not None:
    print(f"latest #{latest.closed.seq_source}: {latest.value!r}")
```

Output:

```text
latest #2: b'72.9F'
```

`Schema` owns encoding and decoding, so reads and observations can return typed
payloads rather than raw transport bytes.

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
everything downstream in that fluent chain until another placement is selected:

```python
subscription = (
    graph.observe(temperature, replay_latest=False)
    .on_main_thread()
    .map(lambda value: value.strip(), name="trim")
    .on_pooled_thread()
    .callback(print, name="print-temperature")
)
```

Use `.on_main_thread()` for work that must run on the frame thread,
`.on_background_thread()` or `.on_pooled_thread()` for shared worker pools, and
`.on_isolated_thread()` for a node that must be the only work on its thread.

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
average_temperature = route(
    owner="sensor",
    family="environment",
    stream="average_temperature",
    schema=Schema.float(name="AverageTemperature"),
)

graph.observe(temperature_value, replay_latest=False).moving_average(
    window_size=3
).connect(average_temperature)
```

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

Windows and joins make time and coordination visible:

```python
graph.window_by_time(source, width=1.0, watermark=watermark_route)
graph.lookup_join(left_stream, right_state, combine=combine_values)
```

## Inspect

Manyfold is useful when the runtime can answer operational questions:

```python
graph.flow_snapshot(route_or_port)
graph.scheduler_snapshot(route=None)
graph.mailbox_snapshot(mailbox)
graph.route_audit(route)
graph.lineage(route=None, causality_id=...)
```

Use these surfaces when debugging behavior that would otherwise live in logs,
queue dashboards, or ad hoc counters.

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
