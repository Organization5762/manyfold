# Persistent coordinator development cluster

ManyFold's cluster coordinator is a persistent, static N-member Raft control
plane. It uses [PySyncObj 0.3.15](https://pypi.org/project/pysyncobj/) for the
Raft protocol, election, replication, quorum, journal, and snapshot behavior.
ManyFold adds explicit address-bound identities, composable network protocols,
identity-bound state directories, a durable SQLite applied log, a bounded HTTP
API, leader discovery, fault injection, and the local process harness.

This coordinator is separate from `manyfold.components.Consensus`. That graph
component remains useful as Raft-shaped example wiring, but it is not a
distributed consensus implementation.

## Run the one-host cluster

From the repository root:

```sh
uv run manyfold-cluster \
  --root .manyfold-dev-cluster \
  --nodes 5
```

The harness accepts 1–9 local members. The command starts five child processes,
waits for a quorum-backed leader, and prints the generated identities and ports:

The development harness only binds loopback hosts. Its HTTP control API is not
authenticated and cannot be exposed on a LAN or tailnet.

```json
{
  "leader": "node-2",
  "members": [
    {
      "api_port": 53101,
      "host": "127.0.0.1",
      "node_id": "node-1",
      "pid": 41001,
      "raft_identity": "127.0.0.1:53100",
      "raft_port": 53100,
      "state_directory": "/workspace/.manyfold-dev-cluster/nodes/node-1"
    }
  ],
  "network": {
    "layers": [],
    "raft_transport": "tcp"
  },
  "node_count": 5,
  "root": "/workspace/.manyfold-dev-cluster"
}
```

The other member records have the same shape with distinct process IDs, state
directories, address-bound Raft identities, Raft ports, and API ports. Press
Ctrl-C for an orderly shutdown. Run the same command with the same node count
and network options to reuse the identities and durable state.

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

## Network protocols

`NetworkProtocolConfig` independently records the base Raft transport and its
ordered layers. The built-in stack uses PySyncObj TCP. Embedders can inject a
different `RaftNetworkProtocol` into `PersistentRaftCoordinator`; the adapter
provides the node identity class and transport factory without changing
consensus, storage, or the HTTP control plane.

The development fault stack composes marker-controlled disconnects around TCP:

```sh
uv run manyfold-cluster \
  --root .manyfold-fault-cluster \
  --nodes 5 \
  --disconnect-faults
```

Sample configuration:

```json
{
  "layers": [
    "disconnect_faults"
  ],
  "raft_transport": "tcp"
}
```

`DevelopmentCluster.disconnect_node()` closes and removes all Raft peer
connections while leaving the coordinator process and HTTP status API alive.
`reconnect_node()` removes the marker and re-adds the same peers. This tests a
real transport partition without privileged firewall rules or a process kill.

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

Each development node uses:

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

## Recovery and fault proofs

Run the focused process and fault tests:

```sh
uv run python -m unittest \
  tests.test_cluster_processes \
  tests.test_cluster_faults
```

Expected output:

```text
...
----------------------------------------------------------------------
Ran 3 tests in ...s

OK
```

The test starts three real coordinator processes, verifies follower redirect,
commits a command, kills the elected leader, waits for the surviving quorum to
elect a new leader, commits another command, restarts the killed member against
its original state directory, and verifies that all three durable logs contain
the same ordered commands.

The fault suite also starts five real processes, disconnects two followers
without changing their PIDs, commits through the remaining three-node quorum,
reconnects the followers, and verifies catch-up. Separate disk faults overwrite
the SQLite control log and Raft journal headers; restart must fail fast with the
exact corrupt path rather than silently resetting state.

## Production limitations

This is a real persistent Raft cluster and a development harness, not yet a
production deployment system:

- membership is static after bootstrap, and each Raft identity remains bound to
  its configured address;
- the bounded harness supports 1–9 processes on one IPv4/DNS host and allocates
  ephemeral local ports;
- TCP is the only built-in Raft base transport and HTTP is the only built-in
  control API, although the Raft protocol adapter is injectable;
- the HTTP and Raft transports have no authentication, authorization, or TLS;
- replacing a lost state directory, dynamic membership, and cross-host
  bootstrap are not implemented;
- crash recovery and targeted control-log/journal corruption are tested, but
  broader filesystem corruption, sudden power loss, disk-full behavior, repair,
  and forced snapshot-install recovery still need fault-injection coverage;
- the single-request HTTP surface favors bounded behavior and clear semantics
  over control-plane throughput; and
- operational metrics, alerts, backup, restore, and rolling-upgrade procedures
  remain release-integration work.
