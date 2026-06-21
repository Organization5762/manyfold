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
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
uv run ruff check
uv run python -m unittest discover -s tests -p 'test_*.py'
uv run python -m manyfold.rfc_checklist_gen --check
uv run manyfold-example-catalog --check
uv run manyfold-example-catalog --list reference
uv run python -m examples.catalog --check-manifest
uv run python -m examples.catalog --check-readme
```

For focused Python tests, use:

```sh
uv run python -m unittest tests.test_components
uv run python -m unittest tests.test_components.ComponentTests.test_file_store_addresses_bytes_by_nested_keyspace_prefix
```

For memory and profiler changes, also run the focused benchmark gates that match
the touched path. Representative V1 gates:

```sh
cargo run --release --bin memory_retention_benchmark -- --iterations 1000000 --sample-every 100000 --history-limit 8 --live-plateau-bytes 0 --peak-plateau-bytes 0 --rss-tail-plateau-kib 512 --rss-tail-min-samples 3 --project-events 1000000000 --projected-live-growth-bytes 0 --projected-live-segment-growth-bytes 0 --projected-peak-growth-bytes 0 --projected-peak-segment-growth-bytes 0 --max-elapsed-seconds 30 --max-cpu-seconds 30 --max-average-event-us 100 --max-interval-event-us 100 --max-disk-input-blocks 0 --max-disk-output-blocks 0 --warmup-samples 1 --check-invariants-every 10000 --lineage-retention none --metadata-mode none --materialize-state
cargo run --release --bin memory_retention_benchmark -- --iterations 1000000 --sample-every 100000 --history-limit 8 --live-plateau-bytes 0 --peak-plateau-bytes 0 --rss-tail-plateau-kib 512 --rss-tail-min-samples 3 --project-events 1000000000 --projected-live-growth-bytes 0 --projected-live-segment-growth-bytes 0 --projected-peak-growth-bytes 0 --projected-peak-segment-growth-bytes 0 --max-elapsed-seconds 30 --max-cpu-seconds 30 --max-average-event-us 100 --max-interval-event-us 100 --max-disk-input-blocks 0 --max-disk-output-blocks 0 --warmup-samples 1 --check-invariants-every 10000 --lineage-retention retained --lineage-store noop --metadata-mode static --materialize-state
uv run manyfold-memory-benchmark --iterations 75000 --sample-every 25000 --history-limit 8 --lineage-retention retained --lineage-store noop --materialize-state --publish-mode nowait --metadata-mode static --payload-schema bytes --check --max-materialized-payloads 0 --traced-plateau-bytes 65536 --traced-projected-growth-bytes 134217728 --traced-segment-projected-growth-bytes 134217728 --rss-tail-plateau-kib 512 --rss-tail-min-samples 3 --max-cpu-seconds 10 --max-average-event-us 500 --max-interval-event-us 1000 --max-disk-input-blocks 0 --max-disk-output-blocks 0
```

For long-haul native allocator proof runs, use the same stress tool at target
event volume:

```sh
cargo run --release --bin memory_retention_benchmark -- --iterations 1000000000 --sample-every 100000000 --history-limit 8 --live-plateau-bytes 0 --rss-tail-plateau-kib 512 --rss-tail-min-samples 3 --warmup-samples 2 --check-invariants-every 100000 --lineage-retention none
cargo run --release --bin memory_retention_benchmark -- --iterations 1000000000 --sample-every 100000000 --history-limit 8 --live-plateau-bytes 0 --rss-tail-plateau-kib 512 --rss-tail-min-samples 3 --warmup-samples 2 --check-invariants-every 100000 --lineage-retention retained
```

For Heart-on-device proof, run the real target command with tree-scoped external
monitoring and private-memory gates. Linux targets should expose PSS/private/
anonymous memory from `smaps_rollup`; keep RSS as a fallback signal, not the only
acceptance criterion:

```sh
uv run manyfold-heart-benchmark --heart-root /path/to/heart --totem-command totem run --configuration lib_2026 --duration-seconds 3600 --strict-device-memory-gates --external-min-elapsed-seconds 300 --external-min-samples 30 --external-output-max-samples 500 --external-rss-scope tree --output-json heart-lib_2026-monitor.json --external-pss-projected-growth-kib 0 --external-pss-segment-projected-growth-kib 0 --external-private-projected-growth-kib 0 --external-private-segment-projected-growth-kib 0 --external-anonymous-projected-growth-kib 0 --external-anonymous-segment-projected-growth-kib 0 --external-fd-plateau-count 0 --external-fd-segment-projected-growth-count 0
uv run manyfold-monitor-verify heart-lib_2026-monitor.json --min-samples 30 --require-command-fragment lib_2026 --require-metric pss --require-metric private --require-metric anonymous --require-metric fd --require-sample-field pss --require-sample-field private --require-sample-field anonymous --require-sample-field fd --require-gate-limit external_pss_projected_growth_kib=0 --require-gate-limit external_pss_segment_projected_growth_kib=0 --require-gate-limit external_private_projected_growth_kib=0 --require-gate-limit external_private_segment_projected_growth_kib=0 --require-gate-limit external_anonymous_projected_growth_kib=0 --require-gate-limit external_anonymous_segment_projected_growth_kib=0 --require-gate-limit external_fd_plateau_count=0 --require-gate-limit external_fd_segment_projected_growth_count=0
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
  declarations that require them at import time. Keep module metadata such as
  `__all__` near the bottom instead of in the import preamble. Keep files under
  about 1000 lines of code.
- Use private `_main` functions for script and console entrypoints instead of
  public `main` functions.
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

## Memory Lifecycle

- Treat memory leaks as release blockers. Manyfold is a long-running runtime;
  code that grows without a clear bound, owner, and disposal path is incorrect
  even when tests pass functionally.
- Every retained object must have an explicit lifecycle. When adding a cache,
  queue, history, audit log, subscription, timer, callback, payload handle,
  lineage index, route note, native allocation, or global registry entry, define
  who owns it, when it is released, and how repeated events prove it stays
  bounded.
- Runtime memory is bounded by default. Any in-memory collection in a hot path
  must use a hard maximum (`deque(maxlen=...)`, retention policy, backpressure
  limit, or explicit eviction), be scoped to static graph topology, or document
  why unbounded growth is safe.
- Lineage retention is opt-in. Default graph routes should run sparse with
  `lineage_retention_policy="none"`; enable `retained` only for explicit
  debugging/introspection paths and prove the retained indexes stay bounded.
- Never add append-only `list`/`dict` structures to event, timer, observer,
  debug, audit, payload, or scheduler paths. Prefer bounded collections and
  remove all secondary indexes when the primary record expires.
- Pair every encoded or registered payload with a release path. In particular,
  process-local object stores such as `Schema.any()` must delete values when the
  corresponding envelope leaves retention.
- Pair every subscription, timer, thread, callback queue, and native handle with
  disposal. Disposal must clear pending callbacks and release captured payloads;
  shutdown hooks are not a substitute for per-owner cleanup.
- Coalesce high-rate handoff paths by default. Frame-thread delivery, timers,
  sensors, and debug streams should not allocate one durable callback, envelope,
  native write, or metadata record per tick unless retention is explicitly
  bounded and tested.
- When adding or changing retained runtime state, add a stress test that drives
  repeated events past the bound and asserts the relevant counts stay flat after
  expiry/disposal.
- Validate suspicious memory behavior with runtime probes, not intuition. Use
  RSS/PSS/anonymous memory, `tracemalloc`, GC object counts, and domain-specific
  counters to distinguish true live retention from allocator high-water marks.
- Rust memory leaks are merge blockers. CI must run the native benchmark under
  jemalloc final leak profiling with `prof_leak:true`, `lg_prof_sample:0`, and
  `prof_final:true`; upload final heap profiles when jemalloc reports leaks.
- Native profiler smoke runs should use release-like binaries with line-table
  debug info, frame pointers, and v0 Rust symbol mangling so DHAT, heaptrack,
  perf, Cachegrind, Callgrind, Massif, and Coz reports resolve to useful code.
- Record validation commands for memory/runtime changes, including focused
  stress probes and the full test suite when practical.

## Recent Validation

- 2026-06-21: Final V1 merge-readiness pass for memory/runtime work:
  `/Users/lampe/.local/bin/uv sync`, `cargo fmt --check`, `cargo clippy
  --all-targets --all-features -- -D warnings`, `cargo test`, `uv sync
  --reinstall-package manyfold`, full `uv run ruff check`, full `uv run python
  -m unittest discover -s tests -p 'test_*.py'` (971 tests), RFC checklist,
  example catalog checks, a 75,000-event Python Heart noop-lineage nowait
  benchmark artifact verification, a 300,000-event native sparse release
  benchmark artifact verification, generated-artifact scan, and `git diff
  --check`.
- 2026-06-21: PR #257 CI follow-up fixed Ubuntu stable Clippy's derivable
  default warnings for native store modes, taught jemalloc verification to parse
  the `Leak approximation summary` format emitted by current jemalloc, and kept
  profiler/leak wrapper latency limits separate from normal benchmark latency
  gates. CI allows only the observed two-object, 128 KiB jemalloc/runtime
  exit-time baseline while native benchmark artifacts still require flat live
  allocation counts. The jemalloc wrapper gets a 90-second CPU/elapsed budget
  because full `lg_prof_sample:0` final leak profiling is much slower than the
  normal strict 30-second benchmark gates. Ran `cargo fmt --check`, Clippy with
  all targets/features and `-D warnings`, `cargo test`, `uv sync
  --reinstall-package manyfold`, focused Ruff, `uv run python -m unittest
  tests.test_jemalloc_leak_check tests.test_project_metadata`, and `git diff
  --check`.
- 2026-06-21: Strengthened Heart external monitor artifact verification with
  exact gate limit/mode assertions so device proof artifacts must preserve
  zero-growth PSS/private/anonymous/fd gates. Ran focused Ruff, `uv run python
  -m unittest tests.test_monitor_artifacts tests.test_project_metadata
  tests.test_heart_benchmarks`, a temporary `uv run manyfold-monitor-verify`
  CLI smoke with required gate limits/modes, and `git diff --check`.
- 2026-06-21: Strengthened benchmark artifact verification with monotonic
  progress checks for `step`, `elapsed_seconds`, and `cpu_seconds` so reordered
  or regressing memory logs cannot pass plateau gates. Ran focused Ruff, `uv run
  python -m unittest tests.test_benchmark_artifacts`, a 2,000-event
  `manyfold-memory-benchmark` smoke verified by `manyfold-benchmark-log-verify`,
  and `git diff --check`.
- 2026-06-21: V1 PR-readiness validation after CI pinning and AGENTS cleanup:
  `/Users/lampe/.local/bin/uv sync`, `cargo fmt --check`, `cargo clippy
  --all-targets --all-features -- -D warnings`, `cargo test`, `uv sync
  --reinstall-package manyfold`, full `uv run ruff check`, full `uv run python
  -m unittest discover -s tests -p 'test_*.py'` (961 tests), RFC checklist,
  example catalog checks, native sparse and Heart-style noop-lineage plateau
  gates through 1,000,000 events, Python Heart-style noop-lineage nowait plateau
  gate through 75,000 events, local `/Users/lampe/code/heart` focused `lib_2026`
  integration (68 tests), and `git diff --check`.
- 2026-06-21: Added `manyfold-native-profiler-verify` so required CI DHAT and
  heaptrack runs are independently checked from `summary.json` plus raw profiler
  artifacts. CI now verifies both the sparse and Heart materialized-state
  profiler summaries. Ran focused Ruff, `uv run python -m unittest
  tests.test_native_profilers tests.test_project_metadata`, and an installed
  CLI smoke against a synthetic DHAT summary/artifact directory.
- 2026-06-21: Added `manyfold-heart-benchmark --strict-device-memory-gates` for
  real totem proof runs. The shorthand requires zero projected PSS/private/
  anonymous growth and fd plateau/growth gates unless explicitly relaxed, so
  Linux device runs must prove `smaps_rollup` and fd-count evidence instead of
  relying on RSS alone. Ran focused Ruff and `uv run python -m unittest
  tests.test_heart_benchmarks tests.test_project_metadata`.
- 2026-06-21: CI benchmark log verification now requires native
  `live_allocated_bytes` and `peak_allocated_bytes` to fully plateau across the
  final three retained native benchmark samples, in addition to retained counts
  and RSS tail checks. Ran focused Ruff, `uv run python -m unittest
  tests.test_benchmark_artifacts tests.test_project_metadata`, and a real
  300,000-event native sparse benchmark whose saved log passed
  `manyfold-benchmark-log-verify` with allocator and RSS tail plateau checks.
- 2026-06-21: Expanded `manyfold-benchmark-log-verify` CI coverage to the
  remaining Heart-shaped native/Python benchmark logs: retained materialized
  state, large payloads, high-sequence lineage, process-local payloads,
  unrelated topology, and subscription churn. Ran focused Ruff,
  `uv run python -m unittest tests.test_project_metadata
  tests.test_benchmark_artifacts`, plus local artifact-verifier smokes for a
  300,000-event native Heart retained-materialized run and a 75,000-event Python
  process-local nowait run.
- 2026-06-21: `manyfold-benchmark-log-verify` now supports numeric upper-bound
  checks via `--require-final-max` and `--require-numeric-max`; CI uses them to
  verify saved benchmark logs include CPU seconds, average/interval latency, and
  zero block-I/O evidence. Ran focused Ruff, `uv run python -m unittest
  tests.test_benchmark_artifacts tests.test_project_metadata`, and a real
  75,000-event Python Heart noop-lineage nowait benchmark whose saved log passed
  retained-count, RSS-tail, CPU, latency, and block-I/O artifact checks.
- 2026-06-21: CI benchmark artifact verification now also caps final elapsed
  wall-clock time with `--require-final-max elapsed_seconds=30`, so saved logs
  prove benchmarks did not silently slow down while satisfying memory gates. Ran
  focused Ruff, `uv run python -m unittest tests.test_benchmark_artifacts
  tests.test_project_metadata`, a real 75,000-event Python Heart noop-lineage
  nowait benchmark, and a 300,000-event native sparse release benchmark whose
  saved logs passed retained-count/allocator, RSS-tail, CPU, elapsed-time,
  latency, and block-I/O artifact checks.
- 2026-06-21: V1 memory-readiness work added native sparse/retained lineage
  stores, explicit lineage/correlation attachment modes, jemalloc leak-check
  artifact verification, native profiler wrappers, Python/native plateau gates,
  benchmark log verification, and Heart wrapper provenance checks. Local
  validation covered `cargo fmt --check`, Clippy, `cargo test`, `uv sync
  --reinstall-package manyfold`, Ruff, full unittest discovery, RFC checklist,
  example catalog checks, representative native and Python memory plateau
  benchmarks, and `git diff --check`.
- 2026-06-21: Local `/Users/lampe/code/heart` focused integration passed with
  this worktree on `PYTHONPATH` for `lib_2026` configuration, peripheral
  runtime, multi-scene lifecycle, event streams, gamepad controller, input debug
  tap, navigation profile, and peripheral input bus tests. Full rendered Heart
  probes under SDL dummy can crash on OpenGL scenes; use focused headless tests
  locally and real display/OpenGL-capable device runs for long-haul proof.
- 2026-06-21: Local DHAT/heaptrack/jemalloc proof can skip or fail on macOS when
  those tools are not installed. Linux CI is responsible for required profiler
  artifacts and jemalloc leak-summary evidence. Retried totem probing from the
  skills workspace; `michael@totem4.local` still failed DNS resolution locally,
  so real Heart-on-totem profiling remains pending until a device is reachable.
