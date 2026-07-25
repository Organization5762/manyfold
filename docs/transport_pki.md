# Transport PKI lifecycle

`manyfold.architecture.transport_pki` turns certificate files into the explicit
mutual-TLS security policy required by `TcpTransport`. It keeps issuance outside
the runtime while enforcing the parts a transport process owns:

- CA-verified client and server certificates with `CERT_REQUIRED`;
- client hostname verification;
- TLS 1.3 by default, with TLS 1.2 as the oldest permitted override;
- optional OpenSSL certificate-revocation-list checking;
- POSIX private keys inaccessible to group and other users;
- stable file snapshots so partially rotated material is never installed; and
- last-known-good context retention when a reload fails.

The certificate still needs the identity URI required by the transport:
`manyfold://identity/<percent-encoded-cluster>/<percent-encoded-node>`.

## Load contexts from files

```python
from pathlib import Path

from manyfold.architecture.transport import TransportConfig
from manyfold.architecture.transport_pki import MutualTlsFiles

server_files = MutualTlsFiles(
    ca_certificate=Path("/run/manyfold/ca.pem"),
    certificate=Path("/run/manyfold/server.pem"),
    private_key=Path("/run/manyfold/server.key"),
    crl=Path("/run/manyfold/ca.crl"),
)
server_config = TransportConfig(security=server_files.server_security())
```

Use `client_security(server_hostname)` for a connector. The hostname is passed
to the TLS stack independently of the Manyfold node identity.

## Rotate without replacing transport objects

`TlsSecurityReloader` exposes a stable provider-backed `TransportSecurity`.
`TcpTransport` resolves that provider for every new connection, so a successful
reload applies automatically after reconnect while established TLS sessions
continue with the context that created them.

```python
from manyfold.architecture.transport_pki import TlsSecurityReloader

reloader = TlsSecurityReloader.for_server(server_files)
server_config = TransportConfig(security=reloader.security)

# Call after the certificate manager atomically replaces its files.
if reloader.reload_if_changed():
    print(reloader.health().material_generation)
```

Sample output after the first rotation:

```text
2
```

A failed reload raises `TlsMaterialError`, records the cause in
`TlsReloadHealth.last_error`, and retains the previous verified context.
Operators should alert on that error and retry after the certificate manager
finishes an atomic rotation. Call `close()` to drop the reloader's context
reference and reject future reloads.

Certificate issuance, secure key delivery, revocation-list publication, and the
decision to force existing sessions to reconnect remain deployment operations.
The runtime consumes those results without becoming a certificate authority.
