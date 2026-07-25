# WASM client initialization

Manyfold's WASM package exposes the same identity and trust concepts as the
Python distributed runtime without claiming browser-only capabilities it does
not have. `ManyfoldClient` owns a bounded in-process PubSub runtime and delegates
host work through `HostCapabilities`.

## Initialization contract

`ClientConfig` requires a `NodeIdentity` with cluster, node, and process-instance
identity. It also owns the default callback placement, bounded static peer
endpoints, PubSub retention, and maximum peer count.

Static peers and host discovery results are endpoints, not identities. The host
enrollment callback must authenticate a candidate and return the
credential-bound `NodeIdentity`. Manyfold rejects another cluster, the local
node ID, malformed results, and unauthenticated results. Any such failure starts
the local runtime in `degraded` state without admitting the candidate.

Enrollment is the host trust boundary. A host may either perform the complete
purpose-specific enrollment operation itself or install
`setEnrollmentCredentialIssuer(...)`. The issuer receives structured identity
and candidate fields plus the fixed `manyfold.peer-enrollment.v1` purpose; it
does not receive arbitrary bytes to sign. Its opaque credential is accepted
only when it repeats that purpose, is at most 16 KiB, and expires within five
minutes, then it is passed to `setEnrollment(...)`. Signer unavailability,
malformed credentials, and expired credentials skip that candidate and produce
bounded `degraded` diagnostics without retaining or printing the token.

The lifecycle is:

1. Construct `NodeIdentity`, `ClientConfig`, and `HostCapabilities`.
   Install host callbacks before constructing the client; the client takes an
   owned capability snapshot.
2. Subscribe to status with `onStatus(...)` when lifecycle reporting is needed.
3. Call and await `start()`.
4. Create client-owned PubSub topics with `client.pubsub(...)`.
5. Call and await `shutdown()` once. Shutdown permanently disposes the client.

Calling `start()` while startup is active or after it reaches `ready` or
`degraded` fails. `cancelStart()` requests cancellation; host discovery and
enrollment callbacks receive an `isCancelled()` function so they can stop their
own work promptly. A cancelled pass returns to `stopped` and may be retried.

All retained collections are bounded by `maxPeers`, PubSub retention, callback
queue limits, or the hard 128-entry degraded-diagnostic limit. Shutdown clears
authenticated peers, status callbacks, host callbacks, credential issuers,
worker-spawn hooks, and retained PubSub state. Existing PubSub handles then fail
with a shutdown error.

## Host differences

| Host | Built into WASM | Host injection |
| --- | --- | --- |
| Browser | In-process PubSub, identity validation, lifecycle, static peers, readiness/degraded status, main-event-loop callbacks | Application discovery, complete enrollment or short-lived enrollment credentials, optional real callback scheduler |
| Electron | Same WASM core | Renderer/main-process bridge for discovery, enrollment, and optional machine-local credential issuance; optional native worker spawning, callback scheduling, cleanup |
| Desktop/Node | Same WASM core | Native discovery such as mDNS or DNS, authenticated TCP/session enrollment, optional machine-local credential issuance, native worker spawning, callback scheduling, cleanup |

The WASM module never performs mDNS, opens arbitrary TCP sockets, or spawns a
native process. It exposes neither a machine-signer socket nor arbitrary
`sign(bytes)`. Browser hosts cannot register a native worker spawner.
Electron/desktop code may bridge its injected, purpose-specific credential
issuer to a machine-local signer, but that socket remains owned by the host.
`CallbackPlacement.spawnedThread(...)` requires a host scheduler and does not
fall back to `setTimeout`.

## Trust boundary

Python's `PeerCandidate` intentionally carries no cluster or node identity, and
its `AuthenticatedPeerSession` is created only after transport authentication.
The WASM contract preserves that split:

- `PeerEndpoint` is an untrusted host/port/server-name candidate.
- `setDiscovery(...)` returns only candidates.
- `setEnrollmentCredentialIssuer(...)` may provide a purpose-bound, short-lived
  opaque credential without exposing a signing primitive.
- `setEnrollment(...)` performs host transport and credential work, using the
  issued credential when one is configured.
- `authenticatedPeers()` exposes only identities returned by a successful
  enrollment callback and validated against the local cluster.

Manyfold does not infer authentication from DNS, mDNS, a static address, or a
host callback merely completing.

## Native-host work still required

A production Electron or desktop host must still provide:

- mDNS/DNS/static discovery appropriate to its network.
- mutual-TLS or equivalent authenticated sessions that bind credentials to the
  returned cluster and node identity.
- a purpose-specific enrollment implementation or credential issuer. Desktop
  and Electron hosts may bridge to a protected machine signer socket; browser
  hosts must receive enrollment or short-lived credentials from their host.
- actual PubSub frame routing and subscription propagation across enrolled
  sessions; initialization currently records authenticated peers but keeps the
  WASM PubSub store process-local.
- certificate issuance, rotation, revocation, authorization, retry,
  deduplication, deadlines, and deployment-specific observability.
- a native worker process manager when the application uses that capability.

The runnable package example is
[`scripts/wasm_npm/examples/client_initialization.cjs`](../scripts/wasm_npm/examples/client_initialization.cjs).
