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

If `uv` is not on `PATH`, use `/Users/lampe/.local/bin/uv` before changing tests
or scripts to work around the shell environment.

After editing `src/` Rust extension code, or after pulling/rebasing onto new
`src/` changes, run `uv sync --reinstall-package manyfold` before Python tests
that exercise the native module.

If the default `uv` cache is blocked by sandbox permissions, set
`UV_CACHE_DIR=.cache/uv` for `uv sync` and verification commands before
retrying.

If `git fetch` or commit operations fail because the worktree gitdir is not
writable, make a fresh clone under the writable automation workspace and do the
release work there instead of forcing the broken worktree.

## Verification

Use the smallest command that covers the changed surface:

```sh
cargo test
uv run ruff check
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
- Keep Python APIs object-shaped: use typed refs, dataclasses, and attributes
  instead of string keys, path strings, or dict-shaped primary surfaces.
- Add or update tests for every meaningful behavior change.
- Keep public API documentation and docstrings current when behavior changes.
- When documentation examples print or otherwise produce output, show a sample
  output block next to the example. Link to example files with Markdown links
  instead of only wrapping paths in code ticks.
- Keep the top-level `manyfold` namespace narrow; advanced helpers belong under
  `manyfold.graph` or in semi-private helpers.
- Organize files in this order after module docstrings and imports where
  dependencies allow: type aliases, constants, public functions, public classes,
  private functions, then private classes. Within classes, keep public methods
  before private methods, with dunder lifecycle hooks placed where idiomatic.
  Keep runtime-derived constants, type aliases, and base classes before
  declarations that require them at import time. Keep files under about 1000
  lines of code.
- Avoid stubs unless absolutely necessary. Prefer runners that exercise actual
  code paths instead of replacing behavior.
- Do not use `assert` for runtime invariants in `python/manyfold`; raise
  explicit exceptions so optimized Python keeps the checks.
- Keep code condensed, typed, and documented. Use docstrings for public surface
  area and add short comments around key logic when they make the surrounding
  code easier to interpret.
- Treat README verification commands as the source of truth when they conflict
  with memory or assumptions.
- If you discover a new setup trap, missing command, or repeated source of agent
  confusion, update this file in the same change. Keep it short, concrete, and
  startup-oriented.
