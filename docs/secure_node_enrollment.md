# Secure node enrollment

Manyfold production TCP links start closed. Discovery returns addresses only;
it never creates a cluster, authorizes a node, or creates membership. A peer
becomes an `AuthenticatedPeerSession` candidate only after `TcpTransport`
completes mTLS, validates the pinned CA, and binds the certificate URI to the
existing `NodeIdentity(cluster_id, node_id)` handshake.

## Trust bootstrap

Create the authority once on a trusted machine:

```sh
uv run manyfold-enrollment initialize \
  --state-dir ./cluster-authority \
  --cluster-id production-a \
  --node-id coordinator \
  --server-name coordinator.tailnet.example
```

Output:

```text
{
  "enrollment_token": "<redacted one-time token>",
  "status": {
    "is_authority": true,
    "is_enrolled": true
  }
}
```

The command creates a P-256 CA, node key, certificate, empty CRL, and durable
cluster/node identity. It does not admit any other node. The token expires
after ten minutes, is accepted once, and contains the CA certificate plus its
SHA-256 pin. Send the token through a secret channel independent of discovery.
On an isolated tailnet, the tailnet's authenticated channel can carry the
token and serialized `EnrollmentRequest`; on an untrusted LAN, use a separate
authenticated channel or an offline transfer. Possession of the bearer token
is authorization to enroll one new node.

Manyfold uses the maintained `cryptography` package for X.509, CSR, CRL, and
P-256 operations. Python's standard library can consume certificates through
`ssl` but cannot issue or inspect this lifecycle. A local ASN.1/X.509
implementation would create substantially more security and maintenance
ownership than this pinned, widely reviewed dependency.

For two state directories available to one trusted operator:

```sh
uv run manyfold-machine-signer start \
  --state-dir ./cluster-authority \
  --socket ./cluster-authority/signer.sock

uv run manyfold-enrollment enroll \
  --authority-socket ./cluster-authority/signer.sock \
  --state-dir ./worker-1 \
  --node-id worker-1 \
  --server-name worker-1.tailnet.example \
  --token-file ./worker-1-enrollment.token
```

The token file must be owned by the current UID with mode `0600`; remove it
after successful enrollment. `--token` remains available for controlled API
testing, but production automation should use `--token-file` so the bearer
secret never appears in an argument vector or process listing.

Network enrollment services use the same three explicit operations:
`NodeIdentityStore.prepare()`, authority
`MachineSignerClient.issue_certificate()`, and node `import_enrollment()`.
`EnrollmentRequest.to_json()` carries no token or private key;
`EnrollmentBundle.to_json()` carries only public certificates and the CRL. Keep
the authority key off worker nodes and verify the token's CA pin before
importing a bundle. After the initializer's first token, issue another through
the running authority signer:

```sh
uv run manyfold-machine-signer issue-token \
  --socket ./cluster-authority/signer.sock
```

## TcpTransport integration

The enrolled `NodeIdentityStore` belongs to one machine-local signer service,
not to each Manyfold application process:

```sh
uv run manyfold-machine-signer start \
  --state-dir ./worker-1 \
  --socket ./worker-1/signer.sock \
  --allowed-uid "$(id -u)" \
  --max-clients 16 \
  --max-audit-entries 256 \
  --credential-ttl-seconds 300
```

Health and rotation use the typed service API. Member rotation exchanges only a
CSR, the current certificate, and a domain-bound current-key proof between the
member and authority signers:

```sh
uv run manyfold-machine-signer status --socket ./worker-1/signer.sock
uv run manyfold-machine-signer rotate \
  --socket ./worker-1/signer.sock \
  --authority-socket ./cluster-authority/signer.sock
```

The new private key is generated and activated inside the member signer. Neither
the client nor the authority receives it. Authority self-rotation may omit
`--authority-socket`.

The signer directory and socket are `0700`/`0600`. POSIX peer credentials must
match the configured UID allowlist, only one process can hold the signer lock,
requests and concurrent clients are bounded, and audit history is a bounded
ring. A stale socket from a crash is removed only after the new process acquires
the instance lock. On Windows, deployment requires an equivalent named-pipe
host with an explicit owner-only ACL; the POSIX implementation fails closed
rather than falling back to TCP.

`--allowed-uid` is repeatable. Omitting it permits only the signer process UID.
The credential TTL is constrained to 2–3600 seconds; 300 seconds is the
production default, while the short end exists for qualification and tightly
controlled deployments.

Application processes use the shared client:

```python
from manyfold.architecture import (
    MachineSignerClient,
    NodeIdentity,
    TcpAddress,
    TcpTransport,
    TransportConfig,
)

identity = NodeIdentity("production-a", "worker-1")
signer = MachineSignerClient("./worker-1/signer.sock", identity)
readiness = signer.ensure_process_credentials(
    max_attempts=3,
    retry_delay_seconds=0.05,
)
transport = TcpTransport.connect(
    signer.identity,
    TcpAddress("coordinator.tailnet.example", 7443),
    config=TransportConfig(
        security=signer.transport_security(
            server_side=False,
            server_hostname="coordinator.tailnet.example",
        )
    ),
    expected_peer_node_id="coordinator",
)
```

`credential_status()` is side-effect free and returns a typed
`ProcessCredentialStatus` with `state`, `issued_at`, `expires_at`, `generation`,
`serial_number`, `is_usable`, and `last_error`. The serial proves separate local
processes received distinct leaf credentials even when both are on generation
one. `ensure_process_credentials()` performs at most five caller-selected
attempts; `renew_process_credentials()` forces the same bounded path. States
distinguish empty startup, ready, renewal due, renewal failed while an older
credential is still usable, unavailable, expired, and closed. Once expired,
context creation and ensure fail closed.

Python `SSLContext` cannot delegate its TLS CertificateVerify operation to an
external signing service. The signer therefore issues a process-generated leaf
key and certificate valid for five minutes by default. The durable machine key
never leaves the signer; the process key becomes renewal-due after 80% of its
issued lifetime and is deleted when `MachineSignerClient.close()` runs. Theft
exposure is bounded by the configured remaining leaf lifetime (300 seconds by
default), while signer unavailability prevents startup or renewal but does not
interrupt an already established TLS session. Callers should alert on
`RENEWAL_FAILED` before the current credential expires.

The connector verifies the DNS/IP server name, CA chain, and
`manyfold://identity/<cluster>/<node>` certificate URI. The listener uses
`signer.transport_security(server_side=True)` and requires a client
certificate. `TransportSecurity` asks the signer client for a new context on every
connection, so rotation and CRL updates affect reconnects without terminating
an established connection.

## Renewal, rotation, and revocation

Rotate before the 24-hour machine certificate expires:

```sh
uv run manyfold-machine-signer rotate \
  --socket ./worker-1/signer.sock \
  --authority-socket ./cluster-authority/signer.sock
```

The new CSR is authorized by the current node key. Activation uses a complete
generation directory and one atomic active-generation pointer. The current and
previous generations are retained; older active certificate records are
revoked when the configured overlap bound is reached.

Revoke a node at the authority:

```sh
uv run manyfold-machine-signer revoke \
  --socket ./cluster-authority/signer.sock \
  --node-id worker-1
```

Revocation stops the affected signer from issuing or renewing process
credentials. Already issued process leaves remain usable for at most five
minutes; existing TLS sessions are not forcibly closed. Root CRL distribution
remains an operator/service responsibility for immediate rejection of the
machine intermediate on remote reconnects. Incident response must distribute
the signed CRL and close live transports when five-minute expiry is not fast
enough.

Times are stored in UTC seconds. Certificates start two minutes before
issuance, and token/certificate validation allows two minutes of bounded clock
skew. Operators should still synchronize clocks. Signed CRL numbers prevent a
stale list from rolling revocation state backward. Token, enrolled-node,
revocation, active-certificate, credential-generation, client, request, and
audit state all have hard bounds; `EnrollmentPolicy.max_nodes` defaults to
1,024.

## Persistence and permissions

Identity directories must be mode `0700`; managed files are `0600`. Writes use
a same-directory temporary file, file `fsync`, atomic replacement, and parent
directory `fsync`. Incomplete temporary files and generation directories are
never selected by the active pointer. State paths must be local filesystems
with ordinary POSIX atomic-rename and advisory-lock semantics.

Back up the authority directory securely. Losing a node directory loses that
node identity; losing the authority key prevents new issuance. Do not copy the
authority key to members.

The active machine private key path is
`STATE_DIR/generations/<active UUID>/node.key`; `STATE_DIR/active.json` selects
that generation atomically. Only an authority also has `STATE_DIR/ca.key`.
These paths are service state, not application configuration. Client processes
receive only a temporary short-lived key under an owner-only system temporary
directory, and `MachineSignerClient` exposes no durable-key path or bytes.

## Local development

Local cleartext development remains a separate, explicit mode:

```python
TransportSecurity.insecure_local_development()
```

`TcpTransport` enforces loopback addresses for this mode. It creates no CA,
token, certificate, or production trust, and must not be used on a LAN or
tailnet. Development convenience therefore cannot silently weaken an enrolled
production store.
