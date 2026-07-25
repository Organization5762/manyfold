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
5. Construct `AuthenticatedPeerSession` and pass it to `MembershipTable`.

`AuthenticatedPeerSession` is therefore a trust-boundary value, not an
authentication mechanism. The membership table rejects sessions from another
cluster and sessions claiming the local node ID.

## Discovery

The discovery API lives in `manyfold.architecture.discovery` and is deliberately
not re-exported from the shared package facades yet.

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

Both retained structures have hard bounds. `max_members` caps live and retained
terminal records. `max_changes` caps the state-change feed used by a future
dissemination backend. A reader that falls behind receives
`MembershipHistoryGap` and must take a fresh `snapshot()`. Repeated healthy
heartbeats renew a lease without appending change events.

Call `close()` or use the table as a context manager to clear all records and
change events. Calls after disposal fail with `MembershipClosedError`.

## Failure detection boundary

This release does not claim a SWIM implementation. Correct direct and indirect
probing needs an authenticated datagram/session transport, per-probe correlation
and deadlines, bounded helper selection, replay protection, and network-partition
tests. Those transport guarantees do not exist in this architecture layer yet.

`heartbeat()`, `mark_suspect()`, `mark_dead()`, incarnation checks, snapshots,
and the bounded `changes_since()` feed are the narrow state interface for a
later proven SWIM backend. The change feed is not gossip dissemination by
itself.
