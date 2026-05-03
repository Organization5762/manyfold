# Performance

Manyfold's performance stance is simple: if a data-flow concern affects runtime
behavior, make it a graph concern.

That means pressure, buffering, replay, payload loading, scheduling, retention,
and audit cost should be represented by components and policies that the graph
can inspect.

## Model Pressure Explicitly

Use capacitors when a producer can outpace a consumer.

- Set capacity deliberately.
- Decide whether old values should coalesce or queue.
- Drive release through demand routes.
- Inspect the route credit view with `graph.flow_snapshot(...)`.

Avoid hiding pressure in a faster callback or an unbounded list.

## Gate Expensive Work

Use resistors and lazy payload routes when values are expensive to open.

- Route cheap metadata first.
- Gate full payload access behind policy.
- Keep large bytes in a payload store until selected.
- Track payload demand separately from metadata observation.

This is the right shape for camera frames, LiDAR scans, traces, artifacts,
encrypted blobs, and model inputs.

## Bound Time and State

Use windows, watermarks, and retention policies when streams accumulate state.

- Prefer event-time watermarks over sleeps.
- Partition windows when one stream contains many independent keys.
- Configure route retention intentionally.
- Keep replay policy explicit so latest-value reads do not become hidden memory.

## Keep Writes Auditable

Treat request, shadow, reported, and effective state as separate facts.

- Request routes carry desired action.
- Shadow routes track intended device/runtime state.
- Reported routes reflect what the target says happened.
- Effective routes capture what the application should trust.

This separation makes performance and correctness easier to debug because retry,
backoff, reconciliation, and stale state are visible in the graph.

## Support Performance Work

When adding or optimizing behavior:

1. Add the smallest example that shows the execution shape.
2. Add a behavioral test that exercises the real code path.
3. Add a snapshot/query surface if the behavior needs runtime inspection.
4. Keep route and component APIs object-shaped and typed.
5. Run the focused Python test, then broaden to the full suite when shared behavior changes.

Useful commands:

```sh
uv run python -m unittest tests.test_graph_reactive
uv run python -m unittest tests.test_components
uv run python -m unittest discover -s tests -p 'test_*.py'
cargo test
```

## Performance Checklist

- Is backpressure represented in the graph?
- Is expensive payload loading demand-driven?
- Is retention bounded and explicit?
- Can a route audit explain producers and subscribers?
- Can lineage explain why an output exists?
- Does the test use actual graph behavior instead of a stub?

