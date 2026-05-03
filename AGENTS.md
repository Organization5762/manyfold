# Agent Startup Guide

This repository is a `uv`-managed Python package with a PyO3/Rust extension.
Assume a fresh Codex worktree has no activated virtualenv and may be on a
detached `HEAD`; that is normal and not a blocker.

## First Commands

Run these before deciding how to test or edit:

```sh
git status --short --branch
uv sync
```

Use `uv run ...` for Python commands. Do not call bare `pytest`, `python`, or
package entrypoints and then report that they are missing from `PATH`; the
project's Python tools live in the `uv` environment and the test suite uses
`unittest`.

## Verification

Use the smallest command that covers the changed surface:

```sh
cargo test
uv run python -m unittest discover -s tests -p 'test_*.py'
uv run python -m manyfold.rfc_checklist_gen --check
uv run manyfold-example-catalog --check
uv run manyfold-example-catalog --list reference
uv run python -m examples.catalog --check-manifest
uv run python -m examples.catalog --check-readme
```

For a focused Python test file, use:

```sh
uv run python -m unittest tests.test_components
```

For a focused test case, use:

```sh
uv run python -m unittest tests.test_components.ComponentTests.test_file_store_addresses_bytes_by_nested_keyspace_prefix
```

## Repo Shape

- `pyproject.toml` and `uv.lock` define the Python package and environment.
- `.python-version` pins the local Python preference to `3.12`.
- `Cargo.toml` and `Cargo.lock` define the native Rust extension.
- `python/manyfold/` contains the Python wrapper API.
- `src/` contains the Rust core and PyO3 API.
- `examples/` contains executable API examples.
- `tests/` contains the `unittest` suite.

## Working Rules

- Prefer existing object-shaped API patterns over stringly helpers.
- Add or update tests for every meaningful behavior change.
- Keep public API documentation and docstrings current when behavior changes.
- Keep the top-level `manyfold` namespace narrow; advanced helpers belong under
  `manyfold.graph` or in semi-private helpers.
- Treat README verification commands as the source of truth when they conflict
  with memory or assumptions.
- If you discover a new setup trap, missing command, or repeated source of agent
  confusion, update this file in the same change. Keep it short, concrete, and
  startup-oriented.
