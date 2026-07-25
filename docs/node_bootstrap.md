# Node bootstrap

`manyfold.cluster.NodeRuntime` is the client-side release-candidate bootstrap
for one ManyFold node. One `NodeConfig` supplies the existing architecture
objects, a process-scoped `TransportSecurityProvider`, and hard bounds; one
`start()` call acquires short-lived transport security, binds the node, starts
its optional development control plane, discovers endpoints, authenticates
transport sessions, joins authenticated peers to membership, and begins
continuous reconciliation.

The runtime coordinates these concrete objects rather than replacing them:

- `NodeIdentity` remains the transport identity.
- `CompositeDiscovery` composes `StaticSeedDiscovery`, `DnsDiscovery`, and
  `MdnsDiscovery`.
- `TransportSecurityProvider` acquires per-process transport configuration from
  an externally owned signer client.
- `TcpTransport` owns the listener and bounded reconnecting peer links created
  from that process configuration.
- `MembershipTable` owns bounded authenticated member state.
- `DevelopmentCluster` remains the optional fixed three-process local control
  plane.

All five are available directly on the running `NodeRuntime`.

## Start a local node

The command-line workflow defaults to explicit loopback-only development
transport and starts a persistent local control plane:

```sh
uv run manyfold node start \
  --cluster-id development \
  --node-id node-a \
  --listen-port 7443
```

It prints the bound identity, endpoint, current phase, local membership, and
bounded actionable diagnostics before waiting for Ctrl-C:

```json
{
  "cluster_id": "development",
  "endpoint": {
    "host": "127.0.0.1",
    "port": 7443
  },
  "node_id": "node-a",
  "phase": "ready"
}
```

The real output also includes the random process `instance_id`, complete member
records, and retained diagnostics. Add peers through repeatable static or DNS
seeds, and opt into local-link mDNS browsing when the deployment advertises
ManyFold DNS-SD services:

```sh
uv run manyfold node start \
  --cluster-id development \
  --node-id node-b \
  --listen-port 7444 \
  --peer 127.0.0.1:7443 \
  --dns-seed node-c.tailnet-name.ts.net:7443 \
  --mdns
```

The CLI is a thin adapter: it parses endpoints, constructs `NodeConfig` with
`LocalDevelopmentTransportSecurityProvider`, calls `NodeRuntime.start()`,
prints `NodeSnapshot`, and calls `stop()` on termination. The local provider is
stateless and creates keyless loopback-only configuration. Secure deployments
inject their machine-signer client through the typed API.

## Typed API

```python
from manyfold.architecture import (
    CompositeDiscovery,
    MembershipConfig,
    NodeIdentity,
    PeerEndpoint,
    StaticSeedDiscovery,
    TcpAddress,
)
from manyfold.cluster import (
    DevelopmentCluster,
    LocalDevelopmentTransportSecurityProvider,
    NodeConfig,
    NodeRuntime,
)

config = NodeConfig(
    identity=NodeIdentity("development", "node-a"),
    listen_address=TcpAddress("127.0.0.1", 7443),
    discovery=CompositeDiscovery(
        (
            StaticSeedDiscovery(
                (PeerEndpoint("127.0.0.1", 7444),)
            ),
        )
    ),
    transport_security_provider=LocalDevelopmentTransportSecurityProvider(),
    membership=MembershipConfig(max_members=33),
    development_cluster=DevelopmentCluster.create(
        ".manyfold-node/node-a/control"
    ),
    max_peers=32,
)

node = NodeRuntime(config)
try:
    node.start()
    print(node.snapshot().phase.value)
finally:
    node.stop()
```

Sample output when the configured peer is already listening:

```text
ready
```

An empty `CompositeDiscovery(())` reaches `ready` as a local-only node.
`start()` returns the same running object on duplicate calls. `stop()` returns
`True` only for the first stop, releases resources in reverse ownership order,
and leaves the runtime restartable.

## Secure signer integration

Secure `NodeRuntime` configurations require a `TransportSecurityProvider`.
Implement that narrow protocol in the signer client owned by the secure
enrollment package:

```python
def acquire(
    identity: NodeIdentity,
    *,
    timeout_seconds: float,
    minimum_lifetime_seconds: float,
) -> ProcessTransportSecurity:
    ...
```

The response contains listener and connector `TransportConfig` objects plus an
absolute credential expiration. Secure responses must use mutual TLS and must
remain valid for `minimum_lifetime_seconds`. The provider should communicate
with one externally managed machine-local signer shared by all local ManyFold
processes.

`NodeRuntime` neither reads long-lived private-key files nor starts, stops,
unlocks, or persists the signer. It does not call a provider close method.
Stopping a node only releases that process's transports and drops its
short-lived response. A second local process may independently acquire another
response through the same signer client or service.

## Phases and diagnostics

`NodePhase` makes initialization and ongoing health explicit:

| Phase | Meaning |
| --- | --- |
| `signer_unavailable` | The shared machine signer could not be reached before the configured timeout. |
| `signer_locked` | The signer requires local unlock or enrollment before issuance. |
| `credential_expired` | Issued credentials are expired or do not meet the minimum remaining lifetime. |
| `discovering` | A bounded pass is collecting untrusted endpoints. |
| `authenticating` | A bounded `TcpTransport` is handshaking or reconnecting. |
| `joining` | A transport-authenticated identity is renewing membership. |
| `ready` | The node is local-only with no candidates, or all retained candidates are joined. |
| `degraded` | A discovery source or retained peer is unavailable; reconciliation continues. |
| `stopped` | Owned transports, membership, timers, and control-plane processes are disposed. |

`NodeSnapshot.diagnostics` is a hard-bounded sequence of `NodeDiagnostic`
values. Each diagnostic includes a stable code, severity, phase, message,
suggested action, and optional peer endpoint. `NodeConfig` also bounds peers,
membership records and history, transport queues and frame sizes, discovery
candidates, signer acquisition and minimum lifetime, reconcile timing, peer
absence, and shutdown waiting.

Startup acquires signer security before constructing owned resources. Signer,
listener, membership, or development-cluster failure records its actionable
phase, closes every resource already acquired, and returns the runtime to
`stopped`. The single supervisor owns reconciliation; transport retry delays
are capped by each connector's existing `ReconnectPolicy`.

## Current boundary

This API bootstraps and monitors authenticated links; it does not turn the
process-local PubSub implementation into a replicated mesh.

- Mutual TLS authenticates production peer identity. The explicit loopback
  development provider validates claimed cluster and node fields but does not
  credential-authenticate them.
- The bootstrap defines the signer-client contract but implements no signer
  daemon, key database, enrollment store, unlock flow, or duplicated credential
  persistence.
- Credentials are checked during startup. Automatic live certificate rotation
  is not implemented; deployments must issue credentials sized for the process
  lifetime or restart cleanly to reacquire them.
- The development cluster is a fixed, local three-process Raft control plane.
  Bootstrap does not add dynamic Raft membership or join one node's local
  harness to another node's harness.
- `TcpTransport` is a single-peer link. The runtime creates at most
  `max_peers` outbound links and one listener; it does not propagate PubSub
  subscriptions, route messages across peers, or provide durable delivery.
- Membership renewal uses the authenticated transport identity with
  incarnation `0` because the current transport handshake does not exchange a
  monotonic membership incarnation. Process-instance changes remain visible in
  `NodeIdentity.instance_id` and diagnostics.
- mDNS browsing discovers advertised services but this runtime does not publish
  a DNS-SD record. Deployments must provide an advertiser or static/DNS seeds.
