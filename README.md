# manyfold

This repository now contains a first-pass implementation scaffold for the
`docs/rfc/wiregraph_rfc_rev2.md` Manyfold RFC.

## Layout

- `pyproject.toml`: maturin/PyO3 Python packaging entrypoint
- `Cargo.toml`: Rust crate for the native core and Python extension
- `src/`: Rust in-memory runtime, typed refs, descriptors, envelopes, mailboxes, queries, and control-loop stubs
- `python/manyfold/`: Python-facing ergonomic wrapper layer
- `python/manyfold/primitives.py`: primary nouns and verbs for the Python API
- `examples/`: executable API examples that are also covered by the test suite
- `proto/manyfold/v1/wiregraph.proto`: extracted protobuf schema scaffold from the RFC appendix

## Status

This is an RFC stub implementation, not a production runtime. The current code focuses on:

- typed namespace/route/schema identity objects,
- explicit `ReadablePort`, `WritablePort`, `WriteBinding`, and `Mailbox` surfaces,
- descriptor and envelope scaffolding,
- catalog/latest/topology/validation query helpers,
- a minimal `ControlLoop` epoch stub,
- Python object-first routes and shared-stream `ReadThenWriteNextEpochStep` composition,
- Python bindings via PyO3 in the same layout as the referenced project style.

The intended Python wrapper surface is deliberately narrow:

- build a `TypedRoute` with `OwnerName`, `StreamFamily`, `StreamName`, and `Schema`
- build a `ReadThenWriteNextEpochStep` from a read stream and an output route
- `graph.latest(route)` for snapshot reads
- `graph.observe(route)` for Rx subscriptions
- `graph.publish(route, payload)` for writes
- `graph.pipe(source, route)` for wiring an Rx source into a route
- `graph.install(step)` for attaching a `ReadThenWriteNextEpochStep`
- `graph.run_control_loop(name)` for advancing a control loop once

These route inputs are object-based rather than ad hoc strings. `Schema` also
owns payload encoding/decoding, so `latest(route)` and `observe(route)` can
return typed values instead of raw payload bytes.

`ReadThenWriteNextEpochStep` lives in the primary primitives module because it is
becoming a composition unit: it has one required input stream (`read`), one
required output route (`output`), and one shared derived stream (`write`). Any
subscriber to `write` observes the same emitted values that the graph sees when
the step is installed and started.

The `examples/` directory demonstrates these calls directly, and the examples are
validated by the regular `unittest` run so they do not drift away from the API.

## Best Practices

When extending this repository, prefer a narrow, explicit, well-documented API
over a broad convenience surface.

- Write tests for every meaningful behavior change. Keep the smallest,
  easiest-to-understand examples close to the usage they demonstrate in
  `examples/` and mirror them with straightforward assertions in
  `tests/test_examples.py`. Put more complex integration, reactive, and
  repository-level coverage in the rest of the `tests/` directory, where
  `tests/test_graph_reactive.py` and `tests/test_rfc_checklist_gen.py` show the
  expected level of depth.
- Write extensive docstrings and supporting documentation for public modules,
  classes, and functions. If a section of code is non-obvious, add a concise
  comment that explains the invariant, constraint, or design reason behind it
  instead of leaving future readers to infer intent from implementation details.
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

Generate PyO3 `.pyi` stubs with `cargo run --features stub-gen --bin stub_gen`.
If the default interpreter is older than Python 3.10, set `PYO3_PYTHON` to a
3.10+ interpreter first, for example
`PYO3_PYTHON=/opt/homebrew/Cellar/python@3.14/3.14.3_1/Frameworks/Python.framework/Versions/3.14/bin/python3.14 cargo run --features stub-gen --bin stub_gen`.
Regenerate the RFC implementation checklist with
`uv run manyfold-rfc-checklist` (or `uv run python -m manyfold.rfc_checklist_gen`).

The Python package now targets Python 3.10+ because `reactivex==5.0.0a2`
requires it.
