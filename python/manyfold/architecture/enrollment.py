"""Durable, closed-by-default enrollment for Manyfold TCP node identities."""

from __future__ import annotations

import base64
import contextlib
import datetime as dt
import fcntl
import hashlib
import hmac
import ipaddress
import json
import os
import secrets
import shutil
import ssl
import stat
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, final
from urllib.parse import quote, unquote

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from .transport import NodeIdentity

_UTC: Final = dt.timezone.utc
_TOKEN_TTL: Final = dt.timedelta(minutes=10)
_CERTIFICATE_TTL: Final = dt.timedelta(hours=24)
_CLOCK_SKEW: Final = dt.timedelta(minutes=2)
_CA_TTL: Final = dt.timedelta(days=3650)
_MAX_TOKENS: Final = 64
_MAX_NODES: Final = 1024
_MAX_REVOCATIONS: Final = 256
_MAX_ACTIVE_CERTIFICATES: Final = 4
_GENERATION_LIMIT: Final = 2
_SCHEMA: Final = 1


@final
@dataclass(frozen=True, slots=True)
class EnrollmentPolicy:
    """Bounded authority retention and credential lifetimes."""

    token_ttl: dt.timedelta = _TOKEN_TTL
    certificate_ttl: dt.timedelta = _CERTIFICATE_TTL
    clock_skew: dt.timedelta = _CLOCK_SKEW
    max_tokens: int = _MAX_TOKENS
    max_nodes: int = _MAX_NODES
    max_revocations: int = _MAX_REVOCATIONS
    max_active_certificates: int = _MAX_ACTIVE_CERTIFICATES

    def __post_init__(self) -> None:
        if self.token_ttl <= dt.timedelta():
            raise ValueError("token_ttl must be positive")
        if self.certificate_ttl <= dt.timedelta():
            raise ValueError("certificate_ttl must be positive")
        if self.clock_skew < dt.timedelta():
            raise ValueError("clock_skew must not be negative")
        if self.max_tokens < 1 or self.max_nodes < 1 or self.max_revocations < 1:
            raise ValueError("retained-state limits must be positive")
        if self.max_active_certificates < 2:
            raise ValueError("max_active_certificates must be at least 2")


@final
@dataclass(frozen=True, slots=True)
class EnrollmentToken:
    """Short-lived one-time bearer proof carrying the pinned trust root."""

    cluster_id: str
    token_id: str
    secret: str
    issued_at: dt.datetime
    expires_at: dt.datetime
    ca_pem: str
    ca_sha256: str

    def __repr__(self) -> str:
        return (
            "EnrollmentToken("
            f"cluster_id={self.cluster_id!r}, token_id={self.token_id!r}, "
            "secret='<redacted>', "
            f"expires_at={self.expires_at!r}, ca_sha256={self.ca_sha256!r})"
        )

    def encode(self) -> str:
        """Encode the secret for an operator-controlled out-of-band channel."""
        return _b64(
            _json_bytes(
                {
                    "ca_pem": self.ca_pem,
                    "ca_sha256": self.ca_sha256,
                    "cluster_id": self.cluster_id,
                    "expires_at": _timestamp(self.expires_at),
                    "issued_at": _timestamp(self.issued_at),
                    "secret": self.secret,
                    "token_id": self.token_id,
                    "version": _SCHEMA,
                }
            )
        )

    @classmethod
    def decode(cls, encoded: str) -> EnrollmentToken:
        """Decode and validate a token before using its trust-root pin."""
        try:
            payload = json.loads(_unb64(encoded))
            if payload["version"] != _SCHEMA:
                raise ValueError("unsupported version")
            token = cls(
                cluster_id=_text(payload["cluster_id"], "cluster_id"),
                token_id=_uuid(payload["token_id"], "token_id"),
                secret=_text(payload["secret"], "secret"),
                issued_at=_from_timestamp(payload["issued_at"]),
                expires_at=_from_timestamp(payload["expires_at"]),
                ca_pem=_text(payload["ca_pem"], "ca_pem"),
                ca_sha256=_sha256_text(payload["ca_sha256"]),
            )
        except (KeyError, TypeError, json.JSONDecodeError, ValueError) as error:
            raise ValueError(f"invalid enrollment token: {error}") from error
        ca = x509.load_pem_x509_certificate(token.ca_pem.encode("ascii"))
        if _fingerprint(ca) != token.ca_sha256:
            raise ValueError("invalid enrollment token: CA pin mismatch")
        if token.expires_at <= token.issued_at:
            raise ValueError("invalid enrollment token: expiry precedes issuance")
        return token


@final
@dataclass(frozen=True, slots=True)
class EnrollmentRequest:
    """CSR for the existing ``NodeIdentity`` cluster/node contract."""

    request_id: str
    identity: NodeIdentity
    csr_pem: str
    server_names: tuple[str, ...]

    def to_json(self) -> str:
        """Serialize the non-secret request for a network or offline exchange."""
        return _json_bytes(
            {
                "cluster_id": self.identity.cluster_id,
                "csr_pem": self.csr_pem,
                "node_id": self.identity.node_id,
                "request_id": self.request_id,
                "server_names": list(self.server_names),
                "version": _SCHEMA,
            }
        ).decode()

    @classmethod
    def from_json(cls, document: str) -> EnrollmentRequest:
        """Load a request received independently of its bearer token."""
        value = _document(document, "enrollment request")
        return cls(
            _uuid(value["request_id"], "request_id"),
            NodeIdentity(
                _text(value["cluster_id"], "cluster_id"),
                _text(value["node_id"], "node_id"),
            ),
            _text(value["csr_pem"], "csr_pem"),
            _server_names(value["server_names"]),
        )


@final
@dataclass(frozen=True, slots=True)
class EnrollmentBundle:
    """Issued node certificate plus current CA revocation material."""

    request_id: str
    identity: NodeIdentity
    certificate_pem: str
    ca_pem: str
    crl_pem: str

    def to_json(self) -> str:
        """Serialize the public issuance result for crash-safe node import."""
        return _json_bytes(
            {
                "ca_pem": self.ca_pem,
                "certificate_pem": self.certificate_pem,
                "cluster_id": self.identity.cluster_id,
                "crl_pem": self.crl_pem,
                "node_id": self.identity.node_id,
                "request_id": self.request_id,
                "version": _SCHEMA,
            }
        ).decode()

    @classmethod
    def from_json(cls, document: str) -> EnrollmentBundle:
        """Load a serialized issuance result."""
        value = _document(document, "enrollment bundle")
        return cls(
            _uuid(value["request_id"], "request_id"),
            NodeIdentity(
                _text(value["cluster_id"], "cluster_id"),
                _text(value["node_id"], "node_id"),
            ),
            _text(value["certificate_pem"], "certificate_pem"),
            _text(value["ca_pem"], "ca_pem"),
            _text(value["crl_pem"], "crl_pem"),
        )


@final
@dataclass(frozen=True, slots=True)
class RotationRequest:
    """Public CSR and current-key proof for signer-to-signer rotation."""

    request_id: str
    identity: NodeIdentity
    csr_pem: str
    current_certificate_pem: str
    proof: str
    server_names: tuple[str, ...]

    def to_json(self) -> str:
        """Serialize a rotation request without either machine private key."""
        return _json_bytes(
            {
                "cluster_id": self.identity.cluster_id,
                "csr_pem": self.csr_pem,
                "current_certificate_pem": self.current_certificate_pem,
                "node_id": self.identity.node_id,
                "proof": self.proof,
                "request_id": self.request_id,
                "server_names": list(self.server_names),
                "version": _SCHEMA,
            }
        ).decode()

    @classmethod
    def from_json(cls, document: str) -> RotationRequest:
        """Load a typed request received from another signer service."""
        value = _document(document, "rotation request")
        return cls(
            _uuid(value["request_id"], "request_id"),
            NodeIdentity(
                _text(value["cluster_id"], "cluster_id"),
                _text(value["node_id"], "node_id"),
            ),
            _text(value["csr_pem"], "csr_pem"),
            _text(value["current_certificate_pem"], "current_certificate_pem"),
            _text(value["proof"], "proof"),
            _server_names(value["server_names"]),
        )


@final
@dataclass(frozen=True, slots=True)
class EnrollmentStatus:
    """Non-secret persisted identity status."""

    root: Path
    identity: NodeIdentity
    is_authority: bool
    is_enrolled: bool
    certificate_serial: int | None
    certificate_not_after: dt.datetime | None
    ca_sha256: str


@final
class NodeIdentityStore:
    """Own durable ``NodeIdentity`` keys and reloadable transport security."""

    def __init__(self, root: Path, state: dict[str, Any]) -> None:
        self._root = root
        self._state = state

    @classmethod
    def initialize(
        cls,
        root: str | Path,
        *,
        node_id: str,
        cluster_id: str | None = None,
        server_names: Sequence[str] = ("localhost",),
        policy: EnrollmentPolicy = EnrollmentPolicy(),
        now: dt.datetime | None = None,
    ) -> tuple[NodeIdentityStore, EnrollmentToken]:
        """Create a closed cluster authority and its first enrolled node."""
        current = _now(now)
        path = _new_root(root)
        identity = NodeIdentity(cluster_id or str(uuid.uuid4()), node_id)
        names = _server_names(server_names)
        ca_key = ec.generate_private_key(ec.SECP256R1())
        ca = _ca_certificate(ca_key, identity.cluster_id, current, policy.clock_skew)
        node_key = ec.generate_private_key(ec.SECP256R1())
        certificate = _sign_csr(
            ca_key,
            ca,
            _csr(node_key, identity, names),
            current,
            policy,
        )
        state = _identity_state(identity, True, names, policy)
        authority = {
            "crl_number": 0,
            "nodes": {
                identity.node_id: {
                    "certificates": [_certificate_record(certificate)],
                    "revoked": False,
                }
            },
            "revocations": {},
            "schema": _SCHEMA,
            "tokens": {},
        }
        with _lock(path):
            _atomic_json(path / "identity.json", state)
            _atomic(path / "ca.pem", ca.public_bytes(serialization.Encoding.PEM))
            _atomic(path / "ca.key", _key_pem(ca_key))
            _atomic_json(path / "authority.json", authority)
            _write_crl(path, authority, ca_key, ca, current)
            _write_trust_bundle(path)
            _install_generation(path, node_key, certificate)
        store = cls.open(path)
        return store, store.issue_token(now=current)

    @classmethod
    def prepare(
        cls,
        root: str | Path,
        *,
        node_id: str,
        token: EnrollmentToken | str,
        server_names: Sequence[str] = ("localhost",),
        now: dt.datetime | None = None,
    ) -> tuple[NodeIdentityStore, EnrollmentRequest]:
        """Persist a new node key and CSR without trusting a discovery result."""
        proof = _token(token)
        _token_window(proof, _now(now), _CLOCK_SKEW)
        path = _new_root(root)
        identity = NodeIdentity(proof.cluster_id, node_id)
        names = _server_names(server_names)
        key = ec.generate_private_key(ec.SECP256R1())
        request = EnrollmentRequest(
            str(uuid.uuid4()),
            identity,
            _csr(key, identity, names)
            .public_bytes(serialization.Encoding.PEM)
            .decode("ascii"),
            names,
        )
        with _lock(path):
            _atomic_json(
                path / "identity.json",
                _identity_state(identity, False, names, EnrollmentPolicy()),
            )
            _atomic(path / "ca.pem", proof.ca_pem.encode("ascii"))
            _atomic(path / "pending.key", _key_pem(key))
        return cls.open(path, require_enrolled=False), request

    @classmethod
    def open(
        cls,
        root: str | Path,
        *,
        require_enrolled: bool = True,
    ) -> NodeIdentityStore:
        """Open strict-permission state, ignoring incomplete temporary writes."""
        path = Path(root)
        _secure_directory(path)
        state = _read_json(path / "identity.json")
        _validate_state(state)
        _load_ca(path, state)
        if require_enrolled:
            _active_material(path, state)
        return cls(path, state)

    @property
    def root(self) -> Path:
        """Return the state directory."""
        return self._root

    @property
    def identity(self) -> NodeIdentity:
        """Return the durable cluster/node identity with a fresh process instance."""
        return NodeIdentity(self._state["cluster_id"], self._state["node_id"])

    @property
    def is_authority(self) -> bool:
        """Report whether this store owns the CA signing key."""
        return self._state["is_authority"]

    def status(self) -> EnrollmentStatus:
        """Return redacted status."""
        certificate = None
        try:
            certificate = _active_certificate(self._root, self._state)
        except FileNotFoundError:
            pass
        return EnrollmentStatus(
            self._root,
            self.identity,
            self.is_authority,
            certificate is not None,
            None if certificate is None else certificate.serial_number,
            None if certificate is None else certificate.not_valid_after_utc,
            _fingerprint(_load_ca(self._root, self._state)),
        )

    def issue_token(
        self,
        *,
        now: dt.datetime | None = None,
    ) -> EnrollmentToken:
        """Issue one short-lived, one-time proof without opening membership."""
        self._require_authority()
        current = _now(now)
        policy = _policy(self._state)
        ca = _load_ca(self._root, self._state)
        token = EnrollmentToken(
            self._state["cluster_id"],
            str(uuid.uuid4()),
            _b64(secrets.token_bytes(32)),
            current,
            current + policy.token_ttl,
            ca.public_bytes(serialization.Encoding.PEM).decode("ascii"),
            _fingerprint(ca),
        )
        with _lock(self._root):
            authority = _authority(self._root)
            _prune(authority, current, policy.clock_skew)
            if len(authority["tokens"]) >= policy.max_tokens:
                raise RuntimeError("retained enrollment-token limit reached")
            authority["tokens"][token.token_id] = {
                "consumed": False,
                "expires_at": _timestamp(token.expires_at),
                "issued_at": _timestamp(token.issued_at),
                "secret_sha256": _secret_digest(token.secret),
            }
            _atomic_json(self._root / "authority.json", authority)
        return token

    def issue(
        self,
        token: EnrollmentToken | str,
        request: EnrollmentRequest,
        *,
        now: dt.datetime | None = None,
    ) -> EnrollmentBundle:
        """Consume a proof and issue a certificate bound to ``NodeIdentity``."""
        self._require_authority()
        proof = _token(token)
        current = _now(now)
        policy = _policy(self._state)
        if request.identity.cluster_id != self._state["cluster_id"]:
            raise PermissionError("enrollment request belongs to another cluster")
        ca_key, ca = _authority_material(self._root, self._state)
        if proof.cluster_id != self._state[
            "cluster_id"
        ] or proof.ca_sha256 != _fingerprint(ca):
            raise PermissionError("enrollment token pins another cluster")
        csr = _validated_csr(request)
        with _lock(self._root):
            authority = _authority(self._root)
            _prune(authority, current, policy.clock_skew)
            if request.identity.node_id in authority["nodes"]:
                raise PermissionError("node identity already exists")
            if len(authority["nodes"]) >= policy.max_nodes:
                raise RuntimeError("enrolled-node limit reached")
            record = authority["tokens"].get(proof.token_id)
            _consume_token(record, proof, current, policy.clock_skew)
            _atomic_json(self._root / "authority.json", authority)
            certificate = _sign_csr(ca_key, ca, csr, current, policy)
            authority = _authority(self._root)
            authority["nodes"][request.identity.node_id] = {
                "certificates": [_certificate_record(certificate)],
                "revoked": False,
            }
            _atomic_json(self._root / "authority.json", authority)
        return _bundle(
            request.request_id, request.identity, certificate, ca, self._root
        )

    def import_enrollment(
        self,
        token: EnrollmentToken | str,
        bundle: EnrollmentBundle,
        *,
        now: dt.datetime | None = None,
    ) -> None:
        """Atomically activate an issued certificate matching the pending key."""
        proof = _token(token)
        if proof.ca_sha256 != _fingerprint(_load_ca(self._root, self._state)):
            raise PermissionError("bundle does not match the pinned trust root")
        self._import_bundle(bundle, self._root / "pending.key", _now(now))
        (self._root / "pending.key").unlink()

    def enroll(
        self,
        root: str | Path,
        *,
        node_id: str,
        token: EnrollmentToken | str,
        server_names: Sequence[str] = ("localhost",),
        now: dt.datetime | None = None,
    ) -> NodeIdentityStore:
        """Run prepare/issue/import when authority and new node are local."""
        node, request = self.prepare(
            root,
            node_id=node_id,
            token=token,
            server_names=server_names,
            now=now,
        )
        node.import_enrollment(token, self.issue(token, request, now=now), now=now)
        return self.open(root)

    def revoke(
        self,
        node_id: str,
        *,
        now: dt.datetime | None = None,
    ) -> int:
        """Revoke all active certificates for one enrolled node."""
        self._require_authority()
        current = _now(now)
        policy = _policy(self._state)
        ca_key, ca = _authority_material(self._root, self._state)
        with _lock(self._root):
            authority = _authority(self._root)
            _prune(authority, current, policy.clock_skew)
            node = authority["nodes"].get(_text(node_id, "node_id"))
            if node is None:
                raise KeyError(f"unknown node_id {node_id!r}")
            added = 0
            for record in node["certificates"]:
                if record["serial"] not in authority["revocations"]:
                    _revoke_record(authority, record, current, policy)
                    added += 1
            node["revoked"] = True
            authority["crl_number"] += 1
            _atomic_json(self._root / "authority.json", authority)
            _write_crl(self._root, authority, ca_key, ca, current)
        return added

    def import_crl(
        self,
        crl_pem: str,
        *,
        now: dt.datetime | None = None,
    ) -> None:
        """Import a CA-signed revocation update."""
        crl = x509.load_pem_x509_crl(crl_pem.encode("ascii"))
        _validate_crl_update(self._root, self._state, crl, _now(now))
        _atomic(self._root / "crl.pem", crl_pem.encode("ascii"))
        _write_trust_bundle(self._root)

    def retained_state_counts(self) -> dict[str, int]:
        """Expose bounded durable counts for operations and regression tests."""
        authority = _authority(self._root) if self.is_authority else None
        generations = self._root / "generations"
        return {
            "generations": len(
                tuple(
                    path
                    for path in generations.iterdir()
                    if path.is_dir() and not path.name.startswith(".")
                )
            ),
            "nodes": 0 if authority is None else len(authority["nodes"]),
            "revocations": 0 if authority is None else len(authority["revocations"]),
            "tokens": 0 if authority is None else len(authority["tokens"]),
        }

    def _ssl_context(self, *, server_side: bool) -> ssl.SSLContext:
        """Build internal root-revocation test context; apps use MachineSignerClient."""
        key_path, certificate_path = _active_paths(self._root)
        context = ssl.SSLContext(
            ssl.PROTOCOL_TLS_SERVER if server_side else ssl.PROTOCOL_TLS_CLIENT
        )
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = not server_side
        context.load_verify_locations(cafile=self._root / "trust.pem")
        context.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
        context.load_cert_chain(str(certificate_path), str(key_path))
        return context

    def _prepare_rotation(self) -> RotationRequest:
        """Persist one bounded pending rotation for signer-service orchestration."""
        pending_key = self._root / "pending-rotation.key"
        pending_request = self._root / "pending-rotation.json"
        with _lock(self._root):
            if pending_request.exists() and pending_key.exists():
                return RotationRequest.from_json(_read_secure(pending_request).decode())
            with contextlib.suppress(FileNotFoundError):
                pending_request.unlink()
            with contextlib.suppress(FileNotFoundError):
                pending_key.unlink()
            old_key, old_certificate = _active_material(self._root, self._state)
            new_key = ec.generate_private_key(ec.SECP256R1())
            request_id = str(uuid.uuid4())
            server_names = tuple(self._state["server_names"])
            csr_pem = (
                _csr(new_key, self.identity, server_names)
                .public_bytes(serialization.Encoding.PEM)
                .decode("ascii")
                .strip()
            )
            request = RotationRequest(
                request_id,
                self.identity,
                csr_pem,
                old_certificate.public_bytes(serialization.Encoding.PEM).decode(),
                _b64(
                    old_key.sign(
                        _rotation_payload(
                            request_id,
                            old_certificate.serial_number,
                            csr_pem,
                        ),
                        ec.ECDSA(hashes.SHA256()),
                    )
                ),
                server_names,
            )
            _atomic(pending_key, _key_pem(new_key))
            _atomic(pending_request, request.to_json().encode())
            return request

    def _issue_rotation_request(
        self,
        request: RotationRequest,
        *,
        now: dt.datetime | None = None,
    ) -> EnrollmentBundle:
        """Authorize another signer's typed current-key rotation proof."""
        return self._issue_rotation(
            request.request_id,
            request.identity,
            request.csr_pem,
            x509.load_pem_x509_certificate(
                request.current_certificate_pem.encode("ascii")
            ),
            request.proof,
            request.server_names,
            now=_now(now),
        )

    def _import_rotation(
        self,
        bundle: EnrollmentBundle,
        *,
        now: dt.datetime | None = None,
    ) -> None:
        """Activate the matching pending key and clear bounded pending state."""
        pending_key = self._root / "pending-rotation.key"
        pending_request = self._root / "pending-rotation.json"
        request = RotationRequest.from_json(_read_secure(pending_request).decode())
        if bundle.request_id != request.request_id:
            raise PermissionError("rotation bundle does not match pending request")
        self._import_bundle(bundle, pending_key, _now(now))
        pending_key.unlink()
        pending_request.unlink()
        _fsync(self._root)

    def _issue_rotation(
        self,
        request_id: str,
        identity: NodeIdentity,
        csr_pem: str,
        current_certificate: x509.Certificate,
        proof: str,
        server_names: tuple[str, ...],
        *,
        now: dt.datetime,
    ) -> EnrollmentBundle:
        self._require_authority()
        policy = _policy(self._state)
        ca_key, ca = _authority_material(self._root, self._state)
        _verify_certificate(current_certificate, ca)
        if _certificate_identity(current_certificate) != (
            identity.cluster_id,
            identity.node_id,
        ):
            raise PermissionError("rotation certificate does not bind NodeIdentity")
        public_key = current_certificate.public_key()
        if not isinstance(public_key, ec.EllipticCurvePublicKey):
            raise PermissionError("unsupported rotation key")
        public_key.verify(
            _unb64(proof),
            _rotation_payload(request_id, current_certificate.serial_number, csr_pem),
            ec.ECDSA(hashes.SHA256()),
        )
        request = EnrollmentRequest(
            request_id,
            identity,
            csr_pem,
            server_names,
        )
        csr = _validated_csr(request)
        with _lock(self._root):
            authority = _authority(self._root)
            _prune(authority, now, policy.clock_skew)
            node = authority["nodes"].get(identity.node_id)
            if node is None or node["revoked"]:
                raise PermissionError("rotation node is unknown or revoked")
            serials = {record["serial"] for record in node["certificates"]}
            if str(current_certificate.serial_number) not in serials:
                raise PermissionError("rotation certificate is not active")
            certificate = _sign_csr(ca_key, ca, csr, now, policy)
            node["certificates"].append(_certificate_record(certificate))
            while len(node["certificates"]) > policy.max_active_certificates:
                _revoke_record(authority, node["certificates"].pop(0), now, policy)
            authority["crl_number"] += 1
            _atomic_json(self._root / "authority.json", authority)
            _write_crl(self._root, authority, ca_key, ca, now)
        return _bundle(request_id, identity, certificate, ca, self._root)

    def _import_bundle(
        self,
        bundle: EnrollmentBundle,
        key_path: Path,
        now: dt.datetime,
    ) -> None:
        expected = (self._state["cluster_id"], self._state["node_id"])
        if (bundle.identity.cluster_id, bundle.identity.node_id) != expected:
            raise PermissionError("certificate bundle belongs to another NodeIdentity")
        ca = _load_ca(self._root, self._state)
        if _fingerprint(
            x509.load_pem_x509_certificate(bundle.ca_pem.encode())
        ) != _fingerprint(ca):
            raise PermissionError("certificate bundle uses another trust root")
        key = _load_key(key_path)
        certificate = x509.load_pem_x509_certificate(bundle.certificate_pem.encode())
        _verify_certificate(certificate, ca)
        if _certificate_identity(certificate) != expected:
            raise PermissionError("certificate does not bind the durable NodeIdentity")
        if _public_bytes(key.public_key()) != _public_bytes(certificate.public_key()):
            raise PermissionError("certificate does not match the pending private key")
        policy = _policy(self._state)
        if now < certificate.not_valid_before_utc - policy.clock_skew:
            raise PermissionError("certificate is not valid yet")
        if now > certificate.not_valid_after_utc + policy.clock_skew:
            raise PermissionError("certificate has expired")
        crl = x509.load_pem_x509_crl(bundle.crl_pem.encode())
        _validate_crl_update(self._root, self._state, crl, now)
        with _lock(self._root):
            _atomic(self._root / "crl.pem", bundle.crl_pem.encode())
            _write_trust_bundle(self._root)
            _install_generation(self._root, key, certificate)

    def _require_authority(self) -> None:
        if not self.is_authority:
            raise PermissionError("node does not own the cluster authority")


def _identity_state(
    identity: NodeIdentity,
    is_authority: bool,
    server_names: tuple[str, ...],
    policy: EnrollmentPolicy,
) -> dict[str, Any]:
    return {
        "cluster_id": identity.cluster_id,
        "is_authority": is_authority,
        "node_id": identity.node_id,
        "policy": {
            "certificate_ttl": int(policy.certificate_ttl.total_seconds()),
            "clock_skew": int(policy.clock_skew.total_seconds()),
            "max_active_certificates": policy.max_active_certificates,
            "max_nodes": policy.max_nodes,
            "max_revocations": policy.max_revocations,
            "max_tokens": policy.max_tokens,
            "token_ttl": int(policy.token_ttl.total_seconds()),
        },
        "schema": _SCHEMA,
        "server_names": list(server_names),
    }


def _policy(state: dict[str, Any]) -> EnrollmentPolicy:
    value = state["policy"]
    return EnrollmentPolicy(
        token_ttl=dt.timedelta(seconds=value["token_ttl"]),
        certificate_ttl=dt.timedelta(seconds=value["certificate_ttl"]),
        clock_skew=dt.timedelta(seconds=value["clock_skew"]),
        max_tokens=value["max_tokens"],
        max_nodes=value["max_nodes"],
        max_revocations=value["max_revocations"],
        max_active_certificates=value["max_active_certificates"],
    )


def _validate_state(state: dict[str, Any]) -> None:
    try:
        if state["schema"] != _SCHEMA:
            raise ValueError("unsupported schema")
        NodeIdentity(state["cluster_id"], state["node_id"])
        if type(state["is_authority"]) is not bool:
            raise ValueError("is_authority must be boolean")
        _server_names(state["server_names"])
        _policy(state)
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"invalid node identity state: {error}") from error


def _ca_certificate(
    key: ec.EllipticCurvePrivateKey,
    cluster_id: str,
    now: dt.datetime,
    skew: dt.timedelta,
) -> x509.Certificate:
    name = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, f"Manyfold {cluster_id}")]
    )
    return (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - skew)
        .not_valid_after(now + _CA_TTL)
        .add_extension(x509.BasicConstraints(ca=True, path_length=1), critical=True)
        .add_extension(
            x509.KeyUsage(True, False, False, False, False, True, True, False, False),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )


def _csr(
    key: ec.EllipticCurvePrivateKey,
    identity: NodeIdentity,
    server_names: tuple[str, ...],
) -> x509.CertificateSigningRequest:
    names: list[x509.GeneralName] = [
        x509.UniformResourceIdentifier(_identity_uri(identity))
    ]
    for name in server_names:
        try:
            names.append(x509.IPAddress(ipaddress.ip_address(name)))
        except ValueError:
            names.append(x509.DNSName(name))
    return (
        x509.CertificateSigningRequestBuilder()
        .subject_name(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, identity.node_id)])
        )
        .add_extension(x509.SubjectAlternativeName(names), critical=False)
        .sign(key, hashes.SHA256())
    )


def _validated_csr(request: EnrollmentRequest) -> x509.CertificateSigningRequest:
    csr = x509.load_pem_x509_csr(request.csr_pem.encode("ascii"))
    if not csr.is_signature_valid:
        raise PermissionError("CSR signature is invalid")
    names = csr.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    uris = names.get_values_for_type(x509.UniformResourceIdentifier)
    if uris != [_identity_uri(request.identity)]:
        raise PermissionError("CSR does not bind the requested NodeIdentity")
    actual_names = set(names.get_values_for_type(x509.DNSName))
    actual_names.update(
        str(value) for value in names.get_values_for_type(x509.IPAddress)
    )
    if actual_names != set(request.server_names):
        raise PermissionError("CSR server names do not match the request")
    return csr


def _sign_csr(
    ca_key: ec.EllipticCurvePrivateKey,
    ca: x509.Certificate,
    csr: x509.CertificateSigningRequest,
    now: dt.datetime,
    policy: EnrollmentPolicy,
) -> x509.Certificate:
    builder = (
        x509.CertificateBuilder()
        .subject_name(csr.subject)
        .issuer_name(ca.subject)
        .public_key(csr.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - policy.clock_skew)
        .not_valid_after(now + policy.certificate_ttl)
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(True, False, False, False, False, True, False, False, False),
            critical=True,
        )
    )
    for extension in csr.extensions:
        builder = builder.add_extension(extension.value, extension.critical)
    return builder.sign(ca_key, hashes.SHA256())


def _certificate_identity(certificate: x509.Certificate) -> tuple[str, str]:
    names = certificate.extensions.get_extension_for_class(
        x509.SubjectAlternativeName
    ).value
    uris = names.get_values_for_type(x509.UniformResourceIdentifier)
    if len(uris) != 1 or not uris[0].startswith("manyfold://identity/"):
        raise PermissionError("certificate lacks one Manyfold NodeIdentity URI")
    encoded = uris[0].removeprefix("manyfold://identity/").split("/")
    if len(encoded) != 2:
        raise PermissionError("certificate NodeIdentity URI is malformed")
    return unquote(encoded[0]), unquote(encoded[1])


def _identity_uri(identity: NodeIdentity) -> str:
    return (
        f"manyfold://identity/{quote(identity.cluster_id, safe='')}/"
        f"{quote(identity.node_id, safe='')}"
    )


def _verify_certificate(certificate: x509.Certificate, ca: x509.Certificate) -> None:
    public_key = ca.public_key()
    if not isinstance(public_key, ec.EllipticCurvePublicKey):
        raise PermissionError("unsupported CA key")
    public_key.verify(
        certificate.signature,
        certificate.tbs_certificate_bytes,
        ec.ECDSA(certificate.signature_hash_algorithm),
    )


def _write_crl(
    root: Path,
    authority: dict[str, Any],
    ca_key: ec.EllipticCurvePrivateKey,
    ca: x509.Certificate,
    now: dt.datetime,
) -> None:
    builder = (
        x509.CertificateRevocationListBuilder()
        .issuer_name(ca.subject)
        .last_update(now)
        .next_update(now + _CERTIFICATE_TTL)
        .add_extension(x509.CRLNumber(authority["crl_number"]), critical=False)
    )
    for serial, record in sorted(authority["revocations"].items()):
        revoked = (
            x509.RevokedCertificateBuilder()
            .serial_number(int(serial))
            .revocation_date(_from_timestamp(record["revoked_at"]))
            .build()
        )
        builder = builder.add_revoked_certificate(revoked)
    _atomic(
        root / "crl.pem",
        builder.sign(ca_key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM),
    )
    _write_trust_bundle(root)


def _write_trust_bundle(root: Path) -> None:
    _atomic(
        root / "trust.pem",
        _read_secure(root / "ca.pem") + _read_secure(root / "crl.pem"),
    )


def _verify_crl(crl: x509.CertificateRevocationList, ca: x509.Certificate) -> None:
    public_key = ca.public_key()
    if (
        not isinstance(public_key, ec.EllipticCurvePublicKey)
        or crl.issuer != ca.subject
    ):
        raise PermissionError("revocation list does not match the cluster CA")
    public_key.verify(
        crl.signature,
        crl.tbs_certlist_bytes,
        ec.ECDSA(crl.signature_hash_algorithm),
    )


def _validate_crl_update(
    root: Path,
    state: dict[str, Any],
    crl: x509.CertificateRevocationList,
    now: dt.datetime,
) -> None:
    _verify_crl(crl, _load_ca(root, state))
    policy = _policy(state)
    if crl.last_update_utc > now + policy.clock_skew:
        raise PermissionError("revocation list is not valid yet")
    if crl.next_update_utc < now - policy.clock_skew:
        raise PermissionError("revocation list has expired")
    number = _crl_number(crl)
    current_path = root / "crl.pem"
    if not current_path.exists():
        return
    current = x509.load_pem_x509_crl(_read_secure(current_path))
    current_number = _crl_number(current)
    if number < current_number:
        raise PermissionError("revocation list rollback was rejected")
    if number == current_number and crl.fingerprint(
        hashes.SHA256()
    ) != current.fingerprint(hashes.SHA256()):
        raise PermissionError("revocation list number was reused")


def _crl_number(crl: x509.CertificateRevocationList) -> int:
    try:
        return crl.extensions.get_extension_for_class(x509.CRLNumber).value.crl_number
    except x509.ExtensionNotFound as error:
        raise PermissionError("revocation list lacks a CRL number") from error


def _revoke_record(
    authority: dict[str, Any],
    record: dict[str, Any],
    now: dt.datetime,
    policy: EnrollmentPolicy,
) -> None:
    if len(authority["revocations"]) >= policy.max_revocations:
        raise RuntimeError("retained revocation limit reached")
    authority["revocations"][record["serial"]] = {
        "expires_at": record["expires_at"],
        "revoked_at": _timestamp(now),
    }


def _prune(authority: dict[str, Any], now: dt.datetime, skew: dt.timedelta) -> None:
    for field in ("tokens", "revocations"):
        authority[field] = {
            key: record
            for key, record in authority[field].items()
            if now <= _from_timestamp(record["expires_at"]) + skew
        }
    for node in authority["nodes"].values():
        node["certificates"] = [
            record
            for record in node["certificates"]
            if now <= _from_timestamp(record["expires_at"]) + skew
        ]


def _consume_token(
    record: dict[str, Any] | None,
    token: EnrollmentToken,
    now: dt.datetime,
    skew: dt.timedelta,
) -> None:
    if record is None:
        raise PermissionError("enrollment token is unknown or expired")
    if record["consumed"]:
        raise PermissionError("enrollment token was already consumed")
    if not hmac.compare_digest(record["secret_sha256"], _secret_digest(token.secret)):
        raise PermissionError("enrollment token proof is invalid")
    _token_window(token, now, skew)
    record["consumed"] = True


def _token_window(token: EnrollmentToken, now: dt.datetime, skew: dt.timedelta) -> None:
    if now < token.issued_at - skew:
        raise PermissionError("enrollment token is not valid yet")
    if now > token.expires_at + skew:
        raise PermissionError("enrollment token has expired")


def _bundle(
    request_id: str,
    identity: NodeIdentity,
    certificate: x509.Certificate,
    ca: x509.Certificate,
    root: Path,
) -> EnrollmentBundle:
    return EnrollmentBundle(
        request_id,
        NodeIdentity(identity.cluster_id, identity.node_id, identity.instance_id),
        certificate.public_bytes(serialization.Encoding.PEM).decode("ascii"),
        ca.public_bytes(serialization.Encoding.PEM).decode("ascii"),
        _read_secure(root / "crl.pem").decode("ascii"),
    )


def _install_generation(
    root: Path,
    key: ec.EllipticCurvePrivateKey,
    certificate: x509.Certificate,
) -> None:
    directory = root / "generations"
    directory.mkdir(mode=0o700, exist_ok=True)
    generation = str(uuid.uuid4())
    pending = directory / f".pending-{generation}"
    pending.mkdir(mode=0o700)
    try:
        _atomic(pending / "node.key", _key_pem(key))
        _atomic(
            pending / "node.pem",
            certificate.public_bytes(serialization.Encoding.PEM),
        )
        os.replace(pending, directory / generation)
        _fsync(directory)
        previous = None
        active = root / "active.json"
        if active.exists():
            previous = _read_json(active)["generation"]
        _atomic_json(active, {"generation": generation, "previous": previous})
        keep = {generation, previous}
        for path in directory.iterdir():
            if path.is_dir() and path.name not in keep:
                shutil.rmtree(path)
        _fsync(directory)
    finally:
        if pending.exists():
            shutil.rmtree(pending)


def _active_paths(root: Path) -> tuple[Path, Path]:
    generation = _uuid(_read_json(root / "active.json")["generation"], "generation")
    directory = root / "generations" / generation
    _secure_directory(directory)
    return directory / "node.key", directory / "node.pem"


def _active_material(
    root: Path,
    state: dict[str, Any],
) -> tuple[ec.EllipticCurvePrivateKey, x509.Certificate]:
    key_path, _ = _active_paths(root)
    key = _load_key(key_path)
    certificate = _active_certificate(root, state)
    if _public_bytes(key.public_key()) != _public_bytes(certificate.public_key()):
        raise PermissionError("active certificate does not match identity key")
    return key, certificate


def _active_certificate(
    root: Path,
    state: dict[str, Any],
) -> x509.Certificate:
    _, certificate_path = _active_paths(root)
    certificate = x509.load_pem_x509_certificate(_read_secure(certificate_path))
    _verify_certificate(certificate, _load_ca(root, state))
    if _certificate_identity(certificate) != (state["cluster_id"], state["node_id"]):
        raise PermissionError("active certificate does not bind durable NodeIdentity")
    return certificate


def _load_ca(root: Path, state: dict[str, Any]) -> x509.Certificate:
    ca = x509.load_pem_x509_certificate(_read_secure(root / "ca.pem"))
    _verify_certificate(ca, ca)
    common_names = ca.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    if (
        len(common_names) != 1
        or common_names[0].value != f"Manyfold {state['cluster_id']}"
    ):
        raise PermissionError("trust root does not match durable cluster_id")
    return ca


def _authority_material(
    root: Path,
    state: dict[str, Any],
) -> tuple[ec.EllipticCurvePrivateKey, x509.Certificate]:
    key = _load_key(root / "ca.key")
    ca = _load_ca(root, state)
    if _public_bytes(key.public_key()) != _public_bytes(ca.public_key()):
        raise PermissionError("authority key does not match trust root")
    return key, ca


def _authority(root: Path) -> dict[str, Any]:
    value = _read_json(root / "authority.json")
    if (
        value.get("schema") != _SCHEMA
        or type(value.get("crl_number")) is not int
        or value["crl_number"] < 0
    ):
        raise ValueError("invalid authority state")
    return value


def _certificate_record(certificate: x509.Certificate) -> dict[str, Any]:
    return {
        "expires_at": _timestamp(certificate.not_valid_after_utc),
        "serial": str(certificate.serial_number),
    }


def _rotation_payload(request_id: str, serial: int, csr_pem: str) -> bytes:
    return _json_bytes(
        {
            "csr_sha256": hashlib.sha256(csr_pem.encode()).hexdigest(),
            "request_id": request_id,
            "serial": str(serial),
        }
    )


def _key_pem(key: ec.EllipticCurvePrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _load_key(path: Path) -> ec.EllipticCurvePrivateKey:
    key = serialization.load_pem_private_key(_read_secure(path), password=None)
    if not isinstance(key, ec.EllipticCurvePrivateKey):
        raise PermissionError("identity key must be an EC private key")
    return key


def _public_bytes(key: Any) -> bytes:
    return key.public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def _new_root(root: str | Path) -> Path:
    path = Path(root)
    if path.is_symlink():
        raise PermissionError(f"identity directory must not be a symlink: {path}")
    if path.exists() and any(path.iterdir()):
        raise FileExistsError(f"identity directory is not empty: {path}")
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chmod(path, 0o700)
    _secure_directory(path)
    return path


def _secure_directory(path: Path) -> None:
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o077:
        raise PermissionError(f"identity directory must have mode 0700: {path}")
    if hasattr(os, "getuid") and metadata.st_uid != os.getuid():
        raise PermissionError(f"identity directory has another owner: {path}")


def _read_secure(path: Path) -> bytes:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077:
        raise PermissionError(f"identity file must have mode 0600: {path}")
    if hasattr(os, "getuid") and metadata.st_uid != os.getuid():
        raise PermissionError(f"identity file has another owner: {path}")
    return path.read_bytes()


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(_read_secure(path))
    if not isinstance(value, dict):
        raise ValueError(f"state must be a JSON object: {path}")
    return value


def _atomic_json(path: Path, value: object) -> None:
    _atomic(path, _json_bytes(value) + b"\n")


def _atomic(path: Path, value: bytes) -> None:
    temporary = path.with_name(f".{path.name}.tmp-{uuid.uuid4()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync(path.parent)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def _fsync(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextlib.contextmanager
def _lock(root: Path) -> Iterator[None]:
    path = root / ".identity.lock"
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        os.chmod(path, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _server_names(values: Sequence[str]) -> tuple[str, ...]:
    names = tuple(dict.fromkeys(_text(value, "server_name") for value in values))
    if not names:
        raise ValueError("at least one server_name is required")
    return names


def _document(document: str, name: str) -> dict[str, Any]:
    try:
        value = json.loads(document)
        if not isinstance(value, dict) or value["version"] != _SCHEMA:
            raise ValueError("unsupported or malformed document")
        return value
    except (KeyError, TypeError, json.JSONDecodeError, ValueError) as error:
        raise ValueError(f"invalid {name}: {error}") from error


def _token(value: EnrollmentToken | str) -> EnrollmentToken:
    return (
        value if isinstance(value, EnrollmentToken) else EnrollmentToken.decode(value)
    )


def _fingerprint(certificate: x509.Certificate) -> str:
    return certificate.fingerprint(hashes.SHA256()).hex()


def _secret_digest(secret: str) -> str:
    return hashlib.sha256(secret.encode("ascii")).hexdigest()


def _sha256_text(value: object) -> str:
    text = _text(value, "ca_sha256")
    if len(text) != 64 or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise ValueError("ca_sha256 must be lowercase hexadecimal")
    return text


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be non-empty text")
    return value.strip()


def _uuid(value: object, name: str) -> str:
    text = _text(value, name)
    if str(uuid.UUID(text)) != text:
        raise ValueError(f"{name} must be a canonical UUID")
    return text


def _now(value: dt.datetime | None) -> dt.datetime:
    current = dt.datetime.now(_UTC) if value is None else value
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("time must include a timezone")
    return current.astimezone(_UTC).replace(microsecond=0)


def _timestamp(value: dt.datetime) -> int:
    return int(_now(value).timestamp())


def _from_timestamp(value: object) -> dt.datetime:
    if type(value) is not int:
        raise ValueError("timestamp must be an integer")
    return dt.datetime.fromtimestamp(value, _UTC)


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _unb64(value: str) -> bytes:
    return base64.b64decode(
        _text(value, "base64url") + "=" * (-len(value) % 4),
        altchars=b"-_",
        validate=True,
    )


__all__ = (
    "EnrollmentBundle",
    "EnrollmentPolicy",
    "EnrollmentRequest",
    "EnrollmentStatus",
    "EnrollmentToken",
    "NodeIdentityStore",
    "RotationRequest",
)
