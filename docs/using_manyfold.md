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
```

The route carries ownership, stream identity, plane, layer, variant, and schema.
Those fields are what make the graph inspectable later.

## Publish and Read

```python
graph.publish(temperature, b"72.4F")
latest = graph.latest(temperature)

if latest is not None:
    print(latest.value)
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

Observation is useful for application callbacks. When a callback starts to hide
state, pressure, policy, or replay behavior, model that behavior as a graph
component instead.

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

