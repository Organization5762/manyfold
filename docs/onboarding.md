# Onboarding

Manyfold is a `uv`-managed Python package backed by a PyO3/Rust extension. A
fresh checkout does not need an activated virtualenv.

## First Commands

Run these from the repository root:

```sh
git status --short --branch
uv sync
```

Use `uv run ...` for Python entrypoints:

```sh
uv run python examples/simple_latest.py
uv run python -m unittest tests.test_examples
```

## First 15 Minutes

1. Read the short README for the project shape.
2. Run `examples/simple_latest.py` to see publish/read-back.
3. Run `examples/rate_matched_sensor.py` to see explicit demand.
4. Open [Using Manyfold](using_manyfold.md) for the object model.
5. Open [Performance](performance.md) before adding buffers, retention, payloads, or scheduling.

## Repo Map

- `python/manyfold/`: Python-facing API and graph helpers.
- `src/`: Rust runtime core and PyO3 bindings.
- `examples/`: small runnable stories for supported behavior.
- `tests/`: `unittest` coverage for Python behavior and examples.
- `docs/rfc/`: design target and implementation checklist.
- `docs/releases/`: release notes.

## Working Rules

- Prefer object-shaped routes and schemas over stringly helpers.
- Keep the top-level `manyfold` namespace narrow.
- Add or update tests when behavior changes.
- Keep README short; put deeper explanations in `docs/`.
- Treat README verification commands as source of truth when commands drift.

## Focused Verification

Use the smallest check that covers your change:

```sh
uv run python -m unittest tests.test_examples
uv run python -m unittest tests.test_graph_reactive
uv run python -m examples.catalog --check-readme
uv run manyfold-example-catalog --check
uv run python -m manyfold.rfc_checklist_gen --check
cargo test
uv run ruff check
```

