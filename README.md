# manyfold

This repository now contains a first-pass implementation scaffold for the
`docs/rfc/wiregraph_rfc_rev2.md` Manyfold RFC.

## Layout

- `pyproject.toml`: maturin/PyO3 Python packaging entrypoint
- `Cargo.toml`: Rust crate for the native core and Python extension
- `src/`: Rust in-memory runtime, typed refs, descriptors, envelopes, mailboxes, queries, and control-loop stubs
- `python/manyfold/`: Python-facing ergonomic wrapper layer
- `proto/manyfold/v1/wiregraph.proto`: extracted protobuf schema scaffold from the RFC appendix

## Status

This is an RFC stub implementation, not a production runtime. The current code focuses on:

- typed namespace/route/schema identity objects,
- explicit `ReadablePort`, `WritablePort`, `WriteBinding`, and `Mailbox` surfaces,
- descriptor and envelope scaffolding,
- catalog/latest/topology/validation query helpers,
- a minimal `ControlLoop` epoch stub,
- Python bindings via PyO3 in the same layout as the referenced project style.

## Verification

Use `cargo test` for native verification.

Regenerate Python stubs and the RFC implementation checklist with
`manyfold-stub-gen` (or `python -m manyfold.stub_gen`).

The Python package now targets Python 3.10+ because `reactivex==5.0.0a2`
requires it.
