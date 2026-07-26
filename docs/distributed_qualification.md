# Distributed Startup And Recovery Qualification

This release-candidate gate exercises shipped discovery, authenticated
membership and transport, the persistent development Raft cluster, machine
signer lifecycle, and Heart consumer behavior with real processes, sockets,
state, signals, and a force-installed candidate wheel.

## Exact release gate

Produce `signer.json`, `mesh.json`, and `coordination.json` from a clean Heart
checkout using its `.venv` directly after wheel installation:

```sh
uv pip install --python .venv/bin/python --force-reinstall --no-deps /path/to/manyfold_candidate.whl
.venv/bin/python scripts/qualify_manyfold_signer.py --signer-executable .venv/bin/manyfold-machine-signer --enrollment-executable .venv/bin/manyfold-enrollment --output /path/to/heart-artifacts/signer.json
HEART_MANYFOLD_TEST_ARTIFACT=/path/to/heart-artifacts/mesh.json .venv/bin/pytest -n 0 tests/runtime/test_manyfold_node_mesh.py::test_real_process_mesh_story -q
.venv/bin/python scripts/verify_manyfold_world_coordination.py --output /path/to/heart-artifacts/coordination.json
```

Do not use `uv run` after installing the wheel: synchronization can replace the
candidate with Heart's lockfile pin.

Then run exactly:

```sh
uv run manyfold-distributed-qualification --profile release --heart-artifact-dir /path/to/heart-artifacts --output-dir artifacts/distributed-qualification
```

The command is a release gate: any `fail` or `blocked` result returns nonzero.
It atomically writes `summary.json`, child logs and state, copies each Heart
artifact, records artifact digests, and hashes the exact wheel named by Heart's
installation provenance. Use `--diagnostic-only` only for investigation.

## Matrix

The 22 required stories cover:

- first boot, three-process convergence, overlapping cold start, duplicate
  identities, leader kill, process restart, unavailable quorum, state damage,
  partition healing, and deterministic shutdown;
- malformed/stale DNS and mDNS candidates plus stale membership incarnations;
- authenticated transport, short-lived certificate renewal/expiry, machine
  identity persistence, two clients sharing one signer, unauthorized and
  unavailable clients, signer restart, rotation/revocation, bounded IPC, and
  proof that clients do not open durable private keys;
- Heart navigation, one low-rate sensor topic, signer bootstrap/renewal, and
  Raft/RPC world/device state through leader failure; and
- explicit proof that frame ticks, rendered frames, debug input, microphone
  samples, navigation events, and sensor samples remain outside durable
  delivery and Raft.

The optional soak is bounded to 10-30 minutes and at most 512 samples:

```sh
uv run manyfold-distributed-qualification --profile soak --soak-seconds 600 --heart-artifact-dir /path/to/heart-artifacts --output-dir artifacts/distributed-soak
```

It measures commit latency, convergence, RSS, thread count, file descriptors,
and Raft apply backlog while enforcing fixed resource limits. Normal CI does
not run the soak.

Heart's repository-wide 15 Semgrep findings and 142 mypy error lines are known
pre-existing baselines. They are not signer, mesh, or world regressions and are
not described as green.
