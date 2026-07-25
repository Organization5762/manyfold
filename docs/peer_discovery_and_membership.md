# Peer discovery and membership

Manyfold's architecture layer separates finding an address from trusting a
member. Discovery produces `PeerCandidate` values containing only an endpoint,
a source label, and an optional TLS/server name. It never supplies a cluster ID
or node ID.

The connection path is:

1. Compose static, ordinary DNS, and mDNS/DNS-SD discovery sources.
2. Connect to an untrusted candidate.
3. Authenticate the peer with the deployment's transport and credentials.
4. Validate the credential-bound cluster and node identity.
5. Construct `AuthenticatedPeerSession` and pass it to `MembershipTable`, or
   hand the established authenticated-session transport to `SwimMembership`.

`AuthenticatedPeerSession` is therefore a trust-boundary value, not an
authentication mechanism. The membership table rejects sessions from another
cluster and sessions claiming the local node ID. The included HMAC datagram
transport constructs this value only after validating a configured peer key.

See [Secure node enrollment](secure_node_enrollment.md) for the closed-by-default
CA/token bootstrap that authenticates a discovered candidate through
`TcpTransport` before this value may be constructed.

## Discovery

The discovery API lives in `manyfold.architecture.discovery`; its primary
types are also available from the shared `manyfold.architecture` facade.

```python
from manyfold.architecture.discovery import (
    CompositeDiscovery,
    DnsDiscovery,
    DnsSeed,
    MdnsDiscovery,
    PeerEndpoint,
    StaticSeedDiscovery,
)

discovery = CompositeDiscovery(
    (
        StaticSeedDiscovery((PeerEndpoint("10.0.0.12", 7443),)),
        DnsDiscovery(
            (
                DnsSeed("manyfold-a.tailnet-name.ts.net", 7443),
                DnsSeed("manyfold-b.internal.example", 7443),
            )
        ),
        MdnsDiscovery(service_type="_manyfold._tcp.local."),
    ),
    max_candidates=64,
)

report = discovery.discover()
```

Static seeds are used as written. `DnsDiscovery` uses the system resolver, so
ordinary DNS, split DNS, `/etc/hosts`, and tailnet MagicDNS can all supply
reachable addresses. DNS and MagicDNS are reachability sources only; their
answers do not authorize cluster membership.

`MdnsDiscovery` performs a bounded DNS-SD browse through the maintained
`zeroconf` package, including its IPv4/IPv6 parsing and socket lifecycle.
Manyfold cancels each browser and closes its Zeroconf runtime before returning.
mDNS is link-local: it does not discover peers across routed networks, VLANs,
or a tailnet. Deployments using a platform-specific browser can inject it
through the narrow `DnsSdResolver` protocol.

Every built-in discovery pass returns a bounded `DiscoveryReport`. Composite
discovery deduplicates endpoints, preserves partial successes, and reports
source failures without treating them as identity failures. Configured DNS
seeds, composite sources, candidates, mDNS services, and accumulated failures
all have explicit hard limits.

## Membership

The membership API lives in `manyfold.architecture.membership`.

```python
from manyfold.architecture import (
    AuthenticatedPeerSession,
    MembershipConfig,
    MembershipTable,
    NodeIdentity,
    PeerEndpoint,
)

membership = MembershipTable(
    NodeIdentity("production", "node-a"),
    PeerEndpoint("10.0.0.11", 7443),
    local_incarnation=7,
    config=MembershipConfig(
        lease_seconds=15,
        suspect_seconds=5,
        dead_retention_seconds=300,
        max_members=256,
        max_changes=256,
    ),
)

# Construct this only after the transport authenticates both identity fields.
session = AuthenticatedPeerSession(
    NodeIdentity("production", "node-b"),
    PeerEndpoint("10.0.0.12", 7443),
    incarnation=3,
)
membership.heartbeat(session)
```

Remote members move through `alive`, `suspect`, `dead`, and `left` states:

- An authenticated heartbeat admits a member or renews its bounded lease.
- `expire()` moves an expired lease to `suspect`, then moves expired suspicion
  to `dead`.
- `mark_suspect()` and `mark_dead()` let an external failure detector apply
  probe results only to the matching incarnation.
- `leave_peer()` records an authenticated explicit leave. `leave_local()` emits
  the local leave and stops accepting updates.
- A newer incarnation supersedes older state, including `dead` or `left`.
  Stale updates are ignored. At the same incarnation, an authenticated
  heartbeat can refute suspicion or death but cannot undo explicit leave.
- Remote dead and left records are evicted after `dead_retention_seconds`. The
  local leave remains available for dissemination until the table is closed.

Time is supplied by `MonotonicClock`; production defaults to
`SystemMonotonicClock`. The table creates no timer or worker thread. The owning
runtime drives `expire()` from its scheduler, which makes lifecycle and
deterministic testing explicit.

`SwimMembership` uses `observe_authenticated_session()` and `apply_update()`
instead of lease renewal. Directly authenticated observations admit a sender
without creating a second lease failure detector. Incarnation and state
precedence reject stale gossip; at one incarnation, `left` outranks `dead`,
which outranks `suspect`, which outranks `alive`. A local suspect or dead claim
advances the local incarnation above the claim and disseminates `alive`.

Both retained structures have hard bounds. `max_members` caps live and retained
terminal records. `max_changes` caps the state-change feed used by a future
dissemination backend. A reader that falls behind receives
`MembershipHistoryGap` and must take a fresh `snapshot()`. Repeated healthy
heartbeats renew a lease without appending change events.

Call `close()` or use the table as a context manager to clear all records and
change events. Calls after disposal fail with `MembershipClosedError`.

## SWIM failure detection

The SWIM API lives in `manyfold.architecture.swim` and is not re-exported from
the shared package facade.

```python
import os

from manyfold.architecture.discovery import PeerEndpoint
from manyfold.architecture.membership import (
    MembershipConfig,
    MembershipTable,
)
from manyfold.architecture.transport import NodeIdentity
from manyfold.architecture.swim import (
    HmacDatagramTransport,
    HmacPeerCredentials,
    SwimConfig,
    SwimMembership,
    SwimPeerSeed,
    UdpDatagramSocket,
)

local_identity = NodeIdentity("production", "node-a")
node_a_key = bytes.fromhex(os.environ["MANYFOLD_NODE_A_HMAC_KEY_HEX"])
node_b_key = bytes.fromhex(os.environ["MANYFOLD_NODE_B_HMAC_KEY_HEX"])
membership = MembershipTable(
    local_identity,
    PeerEndpoint("10.0.0.11", 7443),
    local_incarnation=7,
    config=MembershipConfig(max_members=256, max_changes=256),
)
transport = HmacDatagramTransport(
    UdpDatagramSocket(PeerEndpoint("0.0.0.0", 7443)),
    HmacPeerCredentials(
        local_identity=local_identity,
        advertised_endpoint=PeerEndpoint("10.0.0.11", 7443),
        local_key=node_a_key,
        peer_keys={"node-b": node_b_key},
        max_peers=256,
    ),
)
swim = SwimMembership(
    membership,
    transport,
    config=SwimConfig(
        helper_count=3,
        max_pending_probes=32,
        max_pending_relays=64,
        max_seeds=256,
    ),
)
swim.add_seed(
    SwimPeerSeed(
        NodeIdentity("production", "node-b"),
        PeerEndpoint("10.0.0.12", 7443),
        incarnation=3,
    )
)
```

A seed is only a bounded probe target. It does not appear in the membership
snapshot until a datagram from the expected node ID passes transport credential
validation. When a deployment already has an established authenticated session,
it can call `add_peer(AuthenticatedPeerSession(...))` directly.

The owner performs one randomized direct probe per interval. Every probe has a
unique opaque correlation ID and an absolute direct deadline. A direct timeout
selects at most `helper_count` distinct live helpers and sends `ping_req`; each
helper owns a bounded relay correlation until the indirect deadline. A matching
direct or helper ACK completes the probe. Final failure marks only the probed
incarnation suspect, and expiry promotes that incarnation to dead.

Membership changes are piggybacked on ordinary protocol messages. At most
`max_piggyback_updates` fit in one bounded protocol payload, each subject keeps
only its newest incarnation/state, and an entry is removed after
`retransmit_limit` successful sends. The queue itself is capped by
`max_dissemination_updates`. Explicit `leave()` sends a signed leave directly to
each currently live peer, cancels pending work, and stops participation.

`tick()` performs a bounded receive pass, expires relay/probe deadlines, applies
membership deadlines, and starts at most one new probe. It creates no thread or
timer. The owning runtime must call it often enough to meet the configured
timeouts. `close()` closes the owned socket/transport, clears replay,
correlation, seed, request, and dissemination state, and closes the membership
table.

## Authentication and transport guarantees

`HmacDatagramTransport` is the production UDP implementation. Its canonical
envelope binds the cluster ID, sender node ID, sender incarnation, random
message ID, advertised sender endpoint, and payload under HMAC-SHA-256. ACKs use
the signed advertised endpoint rather than the unauthenticated UDP source
address. The transport accepts only configured node IDs, uses constant-time MAC
comparison, rejects malformed/oversized envelopes, and retains a hard-bounded
replay window. Keys must contain at least 32 bytes and must come from an
approved secret store; discovery data never supplies keys.

The precise trust claim is symmetric credential possession. Any process holding
a node's verification key can impersonate that node. HMAC authenticates and
integrity-protects datagrams but does not encrypt them. Deployments needing
confidentiality, asymmetric identity, hardware-bound credentials, or online key
rotation should implement the narrow `SwimMessageTransport` over their
established secure sessions. Such an implementation must validate cluster and
node identity before producing `AuthenticatedDatagram`; source addresses,
DNS/mDNS names, and caller-provided JSON are not authentication. It must also
bound payload size and the number of datagrams returned by each `receive()`
call, because `SwimMembership` takes that transport-level backpressure as part
of the interface contract.

Replay retention is deliberately finite. A bit-for-bit replay inside the
configured window is rejected; an ancient authenticated message can be seen
again after eviction, where incarnation/state precedence and idempotent probe
handling limit its effect. Deployments that require durable anti-replay across
long outages or restarts need a transport with persisted sequence state.

## Operational constraints

- Persist the local incarnation and increase it before rejoining after a
  process restart. Reusing an old incarnation can leave the node dominated by a
  retained suspect, dead, or left update.
- Configure `max_message_bytes` so the authenticated envelope remains below the
  deployment's datagram MTU. The defaults cap SWIM payloads at 512 bytes and
  authenticated datagrams at 1200 bytes.
- `max_members`, credential entries, seeds, inbound datagrams per tick, replay
  IDs, active probes, relays, seen request correlations, dissemination entries,
  piggyback fan-out, and helper fan-out all have explicit hard limits.
- UDP delivery and leave delivery remain best effort. Suspicion can be false
  during partitions; SWIM supplies failure detection, not consensus or fencing.
- The current backend has no adaptive suspicion timeout, awareness score,
  persistent replay sequence, key rotation protocol, or encrypted transport.
