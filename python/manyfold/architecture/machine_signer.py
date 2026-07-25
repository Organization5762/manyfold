"""Machine-local ownership boundary for Manyfold identity private keys."""

from __future__ import annotations

import base64
import contextlib
import datetime as dt
import fcntl
import hashlib
import ipaddress
import json
import os
import socket
import ssl
import stat
import struct
import tempfile
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Final, final

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from .enrollment import (
    EnrollmentBundle,
    EnrollmentRequest,
    EnrollmentToken,
    NodeIdentityStore,
    RotationRequest,
    _active_material,
    _authority,
    _identity_uri,
    _key_pem,
    _now,
    _read_secure,
)
from .transport import NodeIdentity, TransportSecurity

_PROCESS_CREDENTIAL_TTL_SECONDS: Final = 5 * 60
_MIN_PROCESS_CREDENTIAL_TTL_SECONDS: Final = 2
_MAX_PROCESS_CREDENTIAL_TTL_SECONDS: Final = 60 * 60
_MAX_MESSAGE_BYTES: Final = 64 * 1024
_CHALLENGE_DOMAIN: Final = b"manyfold:transport-identity-challenge:v1\0"
_ENROLLMENT_DOMAIN: Final = b"manyfold:enrollment-proof:v1\0"


@final
@dataclass(frozen=True, slots=True)
class MachineSignerHealth:
    """Actionable bounded service-health snapshot."""

    running: bool
    socket_path: Path
    machine_identity: NodeIdentity
    public_key_sha256: str
    credential_ttl_seconds: int
    active_clients: int
    accepted_requests: int
    rejected_requests: int
    audit_entries: int
    last_error: str | None


class ProcessCredentialState(str, Enum):
    """Observable process-credential lifecycle state."""

    EMPTY = "empty"
    READY = "ready"
    RENEWAL_DUE = "renewal_due"
    RENEWAL_FAILED = "renewal_failed"
    EXPIRED = "expired"
    UNAVAILABLE = "unavailable"
    CLOSED = "closed"


@final
@dataclass(frozen=True, slots=True)
class ProcessCredentialStatus:
    """Typed readiness and expiry snapshot for one signer client."""

    state: ProcessCredentialState
    issued_at: dt.datetime | None
    expires_at: dt.datetime | None
    generation: int
    serial_number: int | None
    is_usable: bool
    last_error: str | None


@final
class MachineSignerService:
    """One bounded Unix-socket signer and short-lived credential issuer."""

    def __init__(
        self,
        store: NodeIdentityStore,
        socket_path: str | Path,
        *,
        allowed_uids: frozenset[int] | None = None,
        max_clients: int = 16,
        max_audit_entries: int = 256,
        credential_ttl_seconds: int = _PROCESS_CREDENTIAL_TTL_SECONDS,
    ) -> None:
        if os.name != "posix":
            raise NotImplementedError(
                "MachineSignerService requires a Windows named-pipe/ACL host"
            )
        if max_clients < 1 or max_audit_entries < 1:
            raise ValueError("signer limits must be positive")
        if not (
            _MIN_PROCESS_CREDENTIAL_TTL_SECONDS
            <= credential_ttl_seconds
            <= _MAX_PROCESS_CREDENTIAL_TTL_SECONDS
        ):
            raise ValueError("credential_ttl_seconds must be between 2 and 3600")
        self._store = store
        self._socket_path = Path(socket_path)
        self._allowed_uids = (
            frozenset({os.getuid()}) if allowed_uids is None else allowed_uids
        )
        self._max_clients = max_clients
        self._credential_ttl_seconds = credential_ttl_seconds
        self._audit: deque[dict[str, object]] = deque(maxlen=max_audit_entries)
        self._slots = threading.BoundedSemaphore(max_clients)
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._listener: socket.socket | None = None
        self._instance_lock_fd: int | None = None
        self._thread: threading.Thread | None = None
        self._connections: set[socket.socket] = set()
        self._client_threads: set[threading.Thread] = set()
        self._active_clients = 0
        self._accepted_requests = 0
        self._rejected_requests = 0
        self._last_error: str | None = None

    def start(self) -> None:
        """Start once; repeated calls are idempotent."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._socket_path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            _require_private_directory(self._socket_path.parent)
            lock_path = self._socket_path.with_suffix(".lock")
            lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            os.chmod(lock_path, 0o600)
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                os.close(lock_fd)
                raise RuntimeError(
                    f"another machine signer owns {self._socket_path}"
                ) from None
            self._instance_lock_fd = lock_fd
            listener: socket.socket | None = None
            try:
                with contextlib.suppress(FileNotFoundError):
                    self._socket_path.unlink()
                listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                listener.bind(str(self._socket_path))
                os.chmod(self._socket_path, 0o600)
                listener.listen(self._max_clients)
                listener.settimeout(0.2)
                self._listener = listener
                self._stop.clear()
                self._thread = threading.Thread(
                    target=self._serve,
                    name="manyfold-machine-signer",
                    daemon=True,
                )
                self._thread.start()
            except BaseException:
                self._listener = None
                self._thread = None
                self._instance_lock_fd = None
                if listener is not None:
                    listener.close()
                with contextlib.suppress(FileNotFoundError):
                    self._socket_path.unlink()
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
                raise

    def stop(self) -> None:
        """Stop once; repeated calls are idempotent and clear the socket."""
        with self._lock:
            thread = self._thread
            listener = self._listener
            self._thread = None
            self._listener = None
            connections = tuple(self._connections)
            client_threads = tuple(self._client_threads)
            lock_fd = self._instance_lock_fd
            self._instance_lock_fd = None
            self._stop.set()
        if listener is not None:
            listener.close()
        for connection in connections:
            connection.close()
        if thread is not None:
            thread.join(timeout=2)
        deadline = time.monotonic() + 2
        for client_thread in client_threads:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            client_thread.join(timeout=remaining)
        with contextlib.suppress(FileNotFoundError):
            self._socket_path.unlink()
        if lock_fd is not None:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def health(self) -> MachineSignerHealth:
        """Return service state without exposing private material."""
        key, _ = _active_material(self._store.root, self._store._state)
        public_key = key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        with self._lock:
            return MachineSignerHealth(
                running=self._thread is not None and self._thread.is_alive(),
                socket_path=self._socket_path,
                machine_identity=self._store.identity,
                public_key_sha256=hashlib.sha256(public_key).hexdigest(),
                credential_ttl_seconds=self._credential_ttl_seconds,
                active_clients=self._active_clients,
                accepted_requests=self._accepted_requests,
                rejected_requests=self._rejected_requests,
                audit_entries=len(self._audit),
                last_error=self._last_error,
            )

    def __enter__(self) -> MachineSignerService:
        self.start()
        return self

    def __exit__(self, *error: object) -> None:
        self.stop()

    def _serve(self) -> None:
        while not self._stop.is_set():
            listener = self._listener
            if listener is None:
                return
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError as error:
                if not self._stop.is_set():
                    self._record_error(error)
                return
            if not self._slots.acquire(blocking=False):
                connection.close()
                self._reject("client limit reached")
                continue
            connection.settimeout(2)
            client_thread = threading.Thread(
                target=self._handle,
                args=(connection,),
                daemon=True,
            )
            with self._lock:
                self._connections.add(connection)
                self._client_threads.add(client_thread)
            client_thread.start()

    def _handle(self, connection: socket.socket) -> None:
        with self._lock:
            self._active_clients += 1
        try:
            uid = _peer_uid(connection)
            if uid not in self._allowed_uids:
                raise PermissionError(f"local uid {uid} is not authorized")
            request = _receive(connection)
            operation = request.get("operation")
            response = self._dispatch(operation, request)
            _send(connection, {"ok": True, "result": response})
            with self._lock:
                self._accepted_requests += 1
                self._audit.append({"operation": operation, "uid": uid})
        except Exception as error:
            error_text = f"{type(error).__name__}: {error}"
            self._reject(error_text)
            with contextlib.suppress(OSError):
                _send(connection, {"error": error_text, "ok": False})
        finally:
            connection.close()
            with self._lock:
                self._active_clients -= 1
                self._connections.discard(connection)
                self._client_threads.discard(threading.current_thread())
            self._slots.release()

    def _dispatch(self, operation: object, request: dict[str, Any]) -> object:
        if operation == "status":
            health = self.health()
            return {
                "accepted_requests": health.accepted_requests,
                "active_clients": health.active_clients,
                "audit_entries": health.audit_entries,
                "cluster_id": health.machine_identity.cluster_id,
                "credential_ttl_seconds": health.credential_ttl_seconds,
                "last_error": health.last_error,
                "node_id": health.machine_identity.node_id,
                "public_key_sha256": health.public_key_sha256,
                "rejected_requests": health.rejected_requests,
                "running": health.running,
            }
        if operation == "process_credentials":
            return self._process_credentials(
                _text(request.get("instance_id"), "instance_id")
            )
        if operation == "transport_challenge":
            return self._typed_signature(
                _CHALLENGE_DOMAIN,
                _decoded(request.get("nonce"), "nonce", limit=64),
            )
        if operation == "enrollment_proof":
            return self._typed_signature(
                _ENROLLMENT_DOMAIN,
                _decoded(request.get("csr_sha256"), "csr_sha256", limit=32),
            )
        if operation == "issue_token":
            return {"token": self._store.issue_token().encode()}
        if operation == "issue_certificate":
            token = EnrollmentToken.decode(_text(request.get("token"), "token"))
            enrollment = EnrollmentRequest.from_json(
                _text(request.get("request"), "request")
            )
            return {"bundle": self._store.issue(token, enrollment).to_json()}
        if operation == "prepare_rotation":
            return {"request": self._store._prepare_rotation().to_json()}
        if operation == "issue_rotation":
            rotation = RotationRequest.from_json(
                _text(request.get("request"), "request")
            )
            return {"bundle": self._store._issue_rotation_request(rotation).to_json()}
        if operation == "import_rotation":
            bundle = EnrollmentBundle.from_json(_text(request.get("bundle"), "bundle"))
            self._store._import_rotation(bundle)
            return {"rotated": True}
        if operation == "revoke":
            return {
                "revoked": self._store.revoke(_text(request.get("node_id"), "node_id"))
            }
        raise ValueError(f"unsupported signer operation {operation!r}")

    def _process_credentials(self, instance_id: str) -> dict[str, object]:
        if self._store.is_authority:
            node = _authority(self._store.root)["nodes"].get(
                self._store.identity.node_id
            )
            if node is not None and node["revoked"]:
                raise PermissionError("machine identity is revoked")
        machine_key, machine_certificate = _active_material(
            self._store.root,
            self._store._state,
        )
        process_key = ec.generate_private_key(ec.SECP256R1())
        now = _now(None)
        identity = self._store.identity
        names: list[x509.GeneralName] = [
            x509.UniformResourceIdentifier(_identity_uri(identity))
        ]
        for name in self._store._state["server_names"]:
            try:
                names.append(x509.IPAddress(ipaddress.ip_address(name)))
            except ValueError:
                names.append(x509.DNSName(name))
        certificate = (
            x509.CertificateBuilder()
            .subject_name(
                x509.Name(
                    [
                        x509.NameAttribute(
                            NameOID.COMMON_NAME, f"{identity.node_id}:{instance_id}"
                        )
                    ]
                )
            )
            .issuer_name(machine_certificate.subject)
            .public_key(process_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(seconds=30))
            .not_valid_after(now + dt.timedelta(seconds=self._credential_ttl_seconds))
            .add_extension(
                x509.BasicConstraints(ca=False, path_length=None), critical=True
            )
            .add_extension(
                x509.ExtendedKeyUsage(
                    (ExtendedKeyUsageOID.CLIENT_AUTH, ExtendedKeyUsageOID.SERVER_AUTH)
                ),
                critical=True,
            )
            .add_extension(
                x509.SubjectAlternativeName(names),
                critical=False,
            )
            .sign(machine_key, hashes.SHA256())
        )
        return {
            "ca_pem": _read_secure(self._store.root / "ca.pem").decode(),
            "certificate_chain_pem": (
                certificate.public_bytes(serialization.Encoding.PEM)
                + machine_certificate.public_bytes(serialization.Encoding.PEM)
            ).decode(),
            "issued_at": int(now.timestamp()),
            "expires_at": int(certificate.not_valid_after_utc.timestamp()),
            "renew_after": int(now.timestamp())
            + max(1, self._credential_ttl_seconds * 4 // 5),
            "serial_number": certificate.serial_number,
            "key_pem": _key_pem(process_key).decode(),
        }

    def _typed_signature(self, domain: bytes, payload: bytes) -> dict[str, str]:
        key, _ = _active_material(self._store.root, self._store._state)
        signature = key.sign(domain + payload, ec.ECDSA(hashes.SHA256()))
        return {"signature": base64.urlsafe_b64encode(signature).decode()}

    def _reject(self, error: str) -> None:
        with self._lock:
            self._rejected_requests += 1
            self._last_error = error
            self._audit.append({"error": error, "operation": "rejected"})

    def _record_error(self, error: BaseException) -> None:
        with self._lock:
            self._last_error = f"{type(error).__name__}: {error}"


@final
class MachineSignerClient:
    """Typed signer client and reloadable short-lived TLS context provider."""

    def __init__(
        self,
        socket_path: str | Path,
        identity: NodeIdentity,
        *,
        clock: Callable[[], dt.datetime] | None = None,
    ) -> None:
        self._socket_path = Path(socket_path)
        self._identity = identity
        self._clock = _utc_now if clock is None else clock
        if not callable(self._clock):
            raise TypeError("clock must be callable")
        self._temporary = tempfile.TemporaryDirectory(
            prefix="manyfold-process-identity-"
        )
        os.chmod(self._temporary.name, 0o700)
        self._credential: dict[str, object] | None = None
        self._credential_generation = 0
        self._credential_last_error: str | None = None
        self._closed = False
        self._lock = threading.Lock()

    @property
    def identity(self) -> NodeIdentity:
        """Return the process instance bound to machine cluster/node identity."""
        return self._identity

    def status(self) -> dict[str, object]:
        """Read machine identity and public-key status."""
        return self._request("status")

    def credential_status(self) -> ProcessCredentialStatus:
        """Return process credential readiness without causing issuance."""
        with self._lock:
            return self._credential_status_locked(_timestamp_now(self._clock))

    def ensure_process_credentials(
        self,
        *,
        force_renewal: bool = False,
        max_attempts: int = 3,
        retry_delay_seconds: float = 0.05,
    ) -> ProcessCredentialStatus:
        """Ensure usable short-lived credentials with bounded signer retries."""
        if not 1 <= max_attempts <= 5:
            raise ValueError("max_attempts must be between 1 and 5")
        if not 0 <= retry_delay_seconds <= 1:
            raise ValueError("retry_delay_seconds must be between 0 and 1")
        with self._lock:
            if self._closed:
                raise RuntimeError("machine signer client is closed")
            now = _timestamp_now(self._clock)
            status = self._credential_status_locked(now)
            if not force_renewal and status.state is ProcessCredentialState.READY:
                return status
            error: BaseException | None = None
            for attempt in range(max_attempts):
                try:
                    credential = self._request(
                        "process_credentials",
                        instance_id=self._identity.instance_id,
                    )
                    issued_at = int(credential["issued_at"])
                    expires_at = int(credential["expires_at"])
                    renew_after = int(credential["renew_after"])
                    serial_number = int(credential["serial_number"])
                    if (
                        issued_at > now + 30
                        or not issued_at < renew_after < expires_at
                        or expires_at <= now
                        or serial_number < 1
                    ):
                        raise RuntimeError(
                            "signer returned an invalid process credential window"
                        )
                    self._credential = credential
                    self._credential_generation += 1
                    self._credential_last_error = None
                    return self._credential_status_locked(now)
                except (
                    ConnectionError,
                    OSError,
                    PermissionError,
                    RuntimeError,
                    TimeoutError,
                    ValueError,
                ) as caught:
                    error = caught
                    self._credential_last_error = f"{type(caught).__name__}: {caught}"
                    if attempt + 1 < max_attempts:
                        time.sleep(retry_delay_seconds)
            status = self._credential_status_locked(_timestamp_now(self._clock))
            if status.is_usable:
                return status
            raise RuntimeError(
                "machine signer could not provide an unexpired process credential: "
                f"{self._credential_last_error}"
            ) from error

    def renew_process_credentials(
        self,
        *,
        max_attempts: int = 3,
        retry_delay_seconds: float = 0.05,
    ) -> ProcessCredentialStatus:
        """Force a bounded renewal attempt and report the resulting lifecycle."""
        return self.ensure_process_credentials(
            force_renewal=True,
            max_attempts=max_attempts,
            retry_delay_seconds=retry_delay_seconds,
        )

    def transport_identity_challenge(self, nonce: bytes) -> bytes:
        """Sign one fixed-size transport challenge in its dedicated domain."""
        if len(nonce) != 64:
            raise ValueError("transport challenge nonce must be exactly 64 bytes")
        result = self._request(
            "transport_challenge",
            nonce=base64.urlsafe_b64encode(nonce).decode(),
        )
        return base64.urlsafe_b64decode(str(result["signature"]))

    def enrollment_proof(self, csr_sha256: bytes) -> bytes:
        """Sign one CSR digest in the dedicated enrollment-proof domain."""
        if len(csr_sha256) != 32:
            raise ValueError("CSR SHA-256 digest must be exactly 32 bytes")
        result = self._request(
            "enrollment_proof",
            csr_sha256=base64.urlsafe_b64encode(csr_sha256).decode(),
        )
        return base64.urlsafe_b64decode(str(result["signature"]))

    def rotate(self, authority: MachineSignerClient | None = None) -> None:
        """Rotate through typed member/authority signer calls without key export."""
        authority_client = self if authority is None else authority
        prepared = self._request("prepare_rotation")
        request = RotationRequest.from_json(str(prepared["request"]))
        issued = authority_client._request(
            "issue_rotation",
            request=request.to_json(),
        )
        bundle = EnrollmentBundle.from_json(str(issued["bundle"]))
        self._request("import_rotation", bundle=bundle.to_json())
        with self._lock:
            self._credential = None

    def issue_token(self) -> EnrollmentToken:
        """Issue one authority-owned enrollment token without reading the CA key."""
        result = self._request("issue_token")
        return EnrollmentToken.decode(str(result["token"]))

    def issue_certificate(
        self,
        token: EnrollmentToken,
        request: EnrollmentRequest,
    ) -> EnrollmentBundle:
        """Issue one typed enrollment bundle through an authority signer."""
        result = self._request(
            "issue_certificate",
            token=token.encode(),
            request=request.to_json(),
        )
        return EnrollmentBundle.from_json(str(result["bundle"]))

    def revoke(self, node_id: str) -> int:
        """Ask an authority signer to revoke one enrolled machine identity."""
        result = self._request("revoke", node_id=node_id)
        if node_id == self._identity.node_id:
            with self._lock:
                self._credential = None
        return int(result["revoked"])

    def transport_security(
        self,
        *,
        server_side: bool,
        server_hostname: str | None = None,
    ) -> TransportSecurity:
        """Create transport security backed by renewable process credentials."""
        if server_side and server_hostname is not None:
            raise ValueError("listener security does not use server_hostname")
        if not server_side and server_hostname is None:
            raise ValueError("connector security requires server_hostname")
        return TransportSecurity.mutual_tls_provider(
            lambda: self.ssl_context(server_side=server_side),
            server_hostname=server_hostname,
        )

    def ssl_context(self, *, server_side: bool) -> ssl.SSLContext:
        """Issue or renew a five-minute process credential, then build TLS."""
        credential = self._credentials()
        root = Path(self._temporary.name)
        key = root / "process.key"
        chain = root / "process.pem"
        ca = root / "ca.pem"
        _private_write(key, str(credential["key_pem"]).encode())
        _private_write(chain, str(credential["certificate_chain_pem"]).encode())
        _private_write(ca, str(credential["ca_pem"]).encode())
        context = ssl.SSLContext(
            ssl.PROTOCOL_TLS_SERVER if server_side else ssl.PROTOCOL_TLS_CLIENT
        )
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = not server_side
        context.load_verify_locations(cafile=ca)
        context.load_cert_chain(chain, key)
        return context

    def close(self) -> None:
        """Remove short-lived process key material."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._credential = None
        self._temporary.cleanup()

    def _credentials(self) -> dict[str, object]:
        self.ensure_process_credentials()
        with self._lock:
            if self._credential is None:
                raise RuntimeError("process credential is unavailable")
            return self._credential

    def _credential_status_locked(self, now: int) -> ProcessCredentialStatus:
        if self._closed:
            state = ProcessCredentialState.CLOSED
        elif self._credential is None:
            state = (
                ProcessCredentialState.UNAVAILABLE
                if self._credential_last_error is not None
                else ProcessCredentialState.EMPTY
            )
        else:
            expires_at = int(self._credential["expires_at"])
            if expires_at <= now:
                state = ProcessCredentialState.EXPIRED
            elif self._credential_last_error is not None:
                state = ProcessCredentialState.RENEWAL_FAILED
            elif int(self._credential["renew_after"]) <= now:
                state = ProcessCredentialState.RENEWAL_DUE
            else:
                state = ProcessCredentialState.READY
        issued_at = (
            None
            if self._credential is None
            else dt.datetime.fromtimestamp(
                int(self._credential["issued_at"]),
                dt.timezone.utc,
            )
        )
        expires_at_value = (
            None
            if self._credential is None
            else dt.datetime.fromtimestamp(
                int(self._credential["expires_at"]),
                dt.timezone.utc,
            )
        )
        return ProcessCredentialStatus(
            state=state,
            issued_at=issued_at,
            expires_at=expires_at_value,
            generation=self._credential_generation,
            serial_number=(
                None
                if self._credential is None
                else int(self._credential["serial_number"])
            ),
            is_usable=(
                self._credential is not None
                and int(self._credential["expires_at"]) > now
                and not self._closed
            ),
            last_error=self._credential_last_error,
        )

    def _request(self, operation: str, **payload: object) -> dict[str, object]:
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            connection.settimeout(2)
            connection.connect(str(self._socket_path))
            _send(connection, {"operation": operation, **payload})
            response = _receive(connection)
        finally:
            connection.close()
        if not response.get("ok"):
            raise PermissionError(str(response.get("error", "signer request rejected")))
        result = response.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("signer returned a malformed result")
        return result


def _peer_uid(connection: socket.socket) -> int:
    if hasattr(connection, "getpeereid"):
        return int(connection.getpeereid()[0])
    if hasattr(socket, "SO_PEERCRED"):
        credentials = connection.getsockopt(
            socket.SOL_SOCKET,
            socket.SO_PEERCRED,
            struct.calcsize("3i"),
        )
        return int(struct.unpack("3i", credentials)[1])
    return os.getuid()


def _send(connection: socket.socket, value: object) -> None:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    if len(payload) > _MAX_MESSAGE_BYTES:
        raise ValueError("signer response exceeds bounded message size")
    connection.sendall(struct.pack("!I", len(payload)) + payload)


def _receive(connection: socket.socket) -> dict[str, Any]:
    header = _read_exact(connection, 4)
    size = struct.unpack("!I", header)[0]
    if size > _MAX_MESSAGE_BYTES:
        raise ValueError("signer request exceeds bounded message size")
    value = json.loads(_read_exact(connection, size))
    if not isinstance(value, dict):
        raise ValueError("signer message must be a JSON object")
    return value


def _read_exact(connection: socket.socket, size: int) -> bytes:
    value = bytearray()
    while len(value) < size:
        chunk = connection.recv(size - len(value))
        if not chunk:
            raise ConnectionError("signer connection closed mid-message")
        value.extend(chunk)
    return bytes(value)


def _private_write(path: Path, value: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(value)


def _decoded(value: object, name: str, *, limit: int) -> bytes:
    encoded = _text(value, name)
    payload = base64.b64decode(
        encoded + "=" * (-len(encoded) % 4),
        altchars=b"-_",
        validate=True,
    )
    if len(payload) != limit:
        raise ValueError(f"{name} must decode to exactly {limit} bytes")
    return payload


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be non-empty text")
    return value


def _require_private_directory(path: Path) -> None:
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o077:
        raise PermissionError(f"signer socket directory must have mode 0700: {path}")
    if metadata.st_uid != os.getuid():
        raise PermissionError(f"signer socket directory has another owner: {path}")


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _timestamp_now(clock: Callable[[], dt.datetime]) -> int:
    current = clock()
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("client clock must include a timezone")
    return int(current.timestamp())


__all__ = (
    "MachineSignerClient",
    "MachineSignerHealth",
    "MachineSignerService",
    "ProcessCredentialState",
    "ProcessCredentialStatus",
)
