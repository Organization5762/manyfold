# Persistent coordinator development cluster

ManyFold's cluster coordinator is a persistent, three-member Raft control
plane. It uses [PySyncObj 0.3.15](https://pypi.org/project/pysyncobj/) for the
Raft protocol, election, replication, quorum, journal, and snapshot behavior.
ManyFold adds stable process configuration, identity-bound state directories, a
durable SQLite applied log, a bounded HTTP API, leader discovery, and the local
process harness.

This coordinator is separate from `manyfold.components.Consensus`. That graph
component remains useful as Raft-shaped example wiring, but it is not a
distributed consensus implementation.

## Run the one-host cluster

From the repository root:

```sh
uv run python -m manyfold.cluster.dev_cluster \
  --root .manyfold-dev-cluster
```

The command starts three child processes, waits for a quorum-backed leader, and
prints the generated identities and ports:

```json
{
  "leader": "node-2",
  "members": [
    {
      "api_port": 53101,
      "host": "127.0.0.1",
      "node_id": "node-1",
      "pid": 41001,
      "raft_port": 53100,
      "state_directory": "/workspace/.manyfold-dev-cluster/nodes/node-1"
    }
  ],
  "root": "/workspace/.manyfold-dev-cluster"
}
```

The other two member records have the same shape with distinct process IDs,
state directories, Raft ports, and API ports. Press Ctrl-C for an orderly
shutdown. Run the same command again to reuse the identities and durable state.

Each node directory contains:

```text
nodes/node-1/
├── committed.sqlite3
├── identity.json
├── node.log
├── raft.journal
└── raft.snapshot
```

`raft.snapshot` appears after log compaction. The identity file prevents a
state directory from being opened as a different member or cluster.

## Coordinator API

Each process serves a small HTTP/JSON API on its generated API port:

- `GET /v1/status` reports identity, role, leader discovery, quorum, term,
  Raft commit/apply indexes, and the locally applied control-log sequence.
- `GET /v1/log?after=0&limit=100` reads a bounded page from the local durable
  committed control log.
- `POST /v1/commands` submits one bounded JSON control-plane command.

A command body is:

```json
{
  "command_id": "deployment/42",
  "kind": "deployment.set",
  "payload": {
    "replicas": 3
  }
}
```

The leader returns `201 Created` only after the command has been committed and
applied:

```json
{
  "command_id": "deployment/42",
  "kind": "deployment.set",
  "payload": {
    "replicas": 3
  },
  "sequence": 1
}
```

A follower with a known leader returns `307 Temporary Redirect`, including both
the `Location` header and a JSON leader/member discovery body. A node without a
known leader returns `503 Service Unavailable` and the fixed member list.
Clients must tolerate the leader changing after discovery.

Command IDs and kinds are restricted tokens. Payloads must be finite JSON
objects no larger than 64 KiB. Reusing a committed command ID with identical
content is idempotent; reusing it with different content is rejected.

## Scope and bounded resources

Raft is only for low-rate control-plane mutations such as deployment,
configuration, ownership, and topology decisions. Hot PubSub frames, sensor
samples, payload envelopes, and debug streams must never be submitted to the
coordinator. Those stay on ManyFold's data plane.

The development node uses:

- one PySyncObj tick thread;
- a 128-command Raft submission queue;
- a single-request HTTP server with a 16-connection listen backlog;
- request bodies capped at the command limit plus JSON framing;
- log reads capped at 1,000 records;
- one SQLite transaction per applied command; and
- deterministic HTTP close, Raft destroy, thread join, and child-process wait.

Raft journal and SQLite control-log growth are durable state rather than runtime
queues. Raft snapshots compact its protocol journal. The SQLite control log is
intentionally retained in full for audit and recovery in this development
cluster.

## Recovery proof

Run the focused process test:

```sh
uv run python -m unittest tests.test_cluster_processes
```

Expected output:

```text
.
----------------------------------------------------------------------
Ran 1 test in ...s

OK
```

The test starts three real coordinator processes, verifies follower redirect,
commits a command, kills the elected leader, waits for the surviving quorum to
elect a new leader, commits another command, restarts the killed member against
its original state directory, and verifies that all three durable logs contain
the same ordered commands.

## Production limitations

This is a real persistent Raft cluster and a development harness, not yet a
production deployment system:

- membership is fixed at three nodes and a Raft identity is bound to its
  configured address;
- the harness supports one IPv4/DNS host and allocates ephemeral local ports;
- the HTTP and Raft transports have no authentication, authorization, or TLS;
- replacing a lost state directory, dynamic membership, and cross-host
  bootstrap are not implemented;
- crash recovery is process-kill tested, but filesystem corruption, sudden
  power loss, disk-full behavior, and forced snapshot-install recovery still
  need fault-injection coverage;
- the single-request HTTP surface favors bounded behavior and clear semantics
  over control-plane throughput; and
- operational metrics, alerts, backup, restore, and rolling-upgrade procedures
  remain release-integration work.
