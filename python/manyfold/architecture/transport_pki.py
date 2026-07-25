"""Filesystem-backed mutual-TLS lifecycle for cross-process transport."""

from __future__ import annotations

import os
import ssl
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from threading import Lock
from time import time

from .transport import TransportSecurity

PrivateKeyPassword = Callable[[], str | bytes]


class TlsMaterialError(RuntimeError):
    """Raised when TLS material cannot produce a stable verified context."""


class TlsReloaderClosed(TlsMaterialError):
    """Raised when a closed reloader is asked for security material."""


class TlsEndpointRole(str, Enum):
    """TLS role whose context is managed by a reloader."""

    SERVER = "server"
    CLIENT = "client"


@dataclass(frozen=True, slots=True)
class MutualTlsFiles:
    """Certificate files and policy used to construct mutual-TLS contexts."""

    ca_certificate: Path
    certificate: Path
    private_key: Path
    crl: Path | None = None
    minimum_version: ssl.TLSVersion = ssl.TLSVersion.TLSv1_3
    private_key_password: PrivateKeyPassword | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        for field_name in ("ca_certificate", "certificate", "private_key"):
            object.__setattr__(
                self,
                field_name,
                _require_path(getattr(self, field_name), field_name),
            )
        if self.crl is not None:
            object.__setattr__(self, "crl", _require_path(self.crl, "crl"))
        if not isinstance(self.minimum_version, ssl.TLSVersion):
            raise ValueError("minimum_version must be an ssl.TLSVersion")
        if self.minimum_version < ssl.TLSVersion.TLSv1_2:
            raise ValueError("minimum_version must be TLSv1_2 or newer")
        if self.private_key_password is not None and not callable(
            self.private_key_password
        ):
            raise TypeError("private_key_password must be callable")

    def server_security(self) -> TransportSecurity:
        """Load a server context that requires trusted client certificates."""
        before = self.snapshot()
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self._configure_shared_context(context)
        context.load_cert_chain(
            certfile=self.certificate,
            keyfile=self.private_key,
            password=self.private_key_password,
        )
        context.check_hostname = False
        context.verify_mode = ssl.CERT_REQUIRED
        self._require_stable_snapshot(before)
        return TransportSecurity.mutual_tls(context)

    def client_security(self, server_hostname: str) -> TransportSecurity:
        """Load a client context that verifies the server and presents a cert."""
        server_hostname = _require_text(server_hostname, "server_hostname")
        before = self.snapshot()
        context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=self.ca_certificate,
        )
        self._configure_shared_context(context, ca_already_loaded=True)
        context.load_cert_chain(
            certfile=self.certificate,
            keyfile=self.private_key,
            password=self.private_key_password,
        )
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        self._require_stable_snapshot(before)
        return TransportSecurity.mutual_tls(
            context,
            server_hostname=server_hostname,
        )

    def snapshot(self) -> "TlsMaterialSnapshot":
        """Return a stable metadata fingerprint for all configured files."""
        _require_private_key_permissions(self.private_key)
        paths = tuple(
            path
            for path in (
                self.ca_certificate,
                self.certificate,
                self.private_key,
                self.crl,
            )
            if path is not None
        )
        return TlsMaterialSnapshot(
            files=tuple(_file_identity(path) for path in paths),
        )

    def _configure_shared_context(
        self,
        context: ssl.SSLContext,
        *,
        ca_already_loaded: bool = False,
    ) -> None:
        context.minimum_version = self.minimum_version
        if not ca_already_loaded:
            context.load_verify_locations(cafile=self.ca_certificate)
        if self.crl is not None:
            context.load_verify_locations(cafile=self.crl)
            crl_check = getattr(ssl, "VERIFY_CRL_CHECK_CHAIN", None)
            if crl_check is None:
                raise TlsMaterialError(
                    "this Python/OpenSSL build cannot enforce certificate revocation"
                )
            context.verify_flags |= crl_check

    def _require_stable_snapshot(self, before: "TlsMaterialSnapshot") -> None:
        after = self.snapshot()
        if after != before:
            raise TlsMaterialError(
                "TLS material changed while its SSLContext was being constructed"
            )


@dataclass(frozen=True, slots=True)
class TlsMaterialSnapshot:
    """Bounded metadata fingerprint used to detect certificate rotation."""

    files: tuple[tuple[str, int, int, int], ...]


@dataclass(frozen=True, slots=True)
class TlsReloadHealth:
    """Immutable health snapshot for one TLS material reloader."""

    generation: int
    material_generation: int
    role: TlsEndpointRole
    snapshot: TlsMaterialSnapshot
    changed_at: float
    is_closed: bool
    last_error: str | None


class TlsSecurityReloader:
    """Reload changed certificate files while retaining last-known-good trust."""

    def __init__(
        self,
        *,
        files: MutualTlsFiles,
        role: TlsEndpointRole,
        server_hostname: str | None = None,
    ) -> None:
        if not isinstance(files, MutualTlsFiles):
            raise ValueError("files must be MutualTlsFiles")
        if not isinstance(role, TlsEndpointRole):
            raise ValueError("role must be a TlsEndpointRole")
        if role is TlsEndpointRole.CLIENT:
            server_hostname = _require_text(server_hostname, "server_hostname")
        elif server_hostname is not None:
            raise ValueError("server reloaders do not use server_hostname")
        self.files = files
        self.role = role
        self.server_hostname = server_hostname
        self._lock = Lock()
        self._generation = 0
        self._material_generation = 0
        self._changed_at = time()
        self._is_closed = False
        self._last_error: str | None = None
        self._snapshot = files.snapshot()
        self._loaded_security: TransportSecurity | None = self._load_security()
        self._transport_security = TransportSecurity.mutual_tls_provider(
            self._current_context,
            server_hostname=self.server_hostname,
        )
        self._material_generation = 1

    @classmethod
    def for_server(cls, files: MutualTlsFiles) -> "TlsSecurityReloader":
        """Create a reloader for listener-side mutual TLS."""
        return cls(files=files, role=TlsEndpointRole.SERVER)

    @classmethod
    def for_client(
        cls,
        files: MutualTlsFiles,
        *,
        server_hostname: str,
    ) -> "TlsSecurityReloader":
        """Create a reloader for connector-side mutual TLS."""
        return cls(
            files=files,
            role=TlsEndpointRole.CLIENT,
            server_hostname=server_hostname,
        )

    def __enter__(self) -> "TlsSecurityReloader":
        return self

    def __exit__(self, *error: object) -> None:
        self.close()

    @property
    def security(self) -> TransportSecurity:
        """Return the current last-known-good immutable security policy."""
        with self._lock:
            if self._is_closed:
                raise TlsReloaderClosed("TLS security reloader is closed")
            return self._transport_security

    def health(self) -> TlsReloadHealth:
        """Return current material generation and reload error state."""
        with self._lock:
            return self._health_locked()

    def reload_if_changed(self) -> bool:
        """Reload atomically changed files, preserving old trust on failure."""
        with self._lock:
            if self._is_closed:
                raise TlsReloaderClosed("TLS security reloader is closed")
            try:
                observed = self.files.snapshot()
                if observed == self._snapshot:
                    return False
                replacement = self._load_security()
                stable = self.files.snapshot()
                if stable != observed:
                    raise TlsMaterialError(
                        "TLS material changed during reload; retry after rotation settles"
                    )
            except (OSError, ssl.SSLError, TlsMaterialError) as error:
                self._last_error = f"{type(error).__name__}: {error}"
                self._generation += 1
                self._changed_at = time()
                raise TlsMaterialError(
                    "failed to reload TLS material; retained last-known-good context"
                ) from error
            self._loaded_security = replacement
            self._snapshot = stable
            self._material_generation += 1
            self._last_error = None
            self._generation += 1
            self._changed_at = time()
            return True

    def close(self) -> None:
        """Drop the managed context reference and reject future reloads."""
        with self._lock:
            if self._is_closed:
                return
            self._is_closed = True
            self._loaded_security = None
            self._generation += 1
            self._changed_at = time()

    def _load_security(self) -> TransportSecurity:
        if self.role is TlsEndpointRole.SERVER:
            return self.files.server_security()
        if self.server_hostname is None:
            raise TlsMaterialError("client TLS reloader has no server_hostname")
        return self.files.client_security(self.server_hostname)

    def _current_context(self) -> ssl.SSLContext:
        with self._lock:
            if self._is_closed:
                raise TlsReloaderClosed("TLS security reloader is closed")
            if self._loaded_security is None:
                raise TlsMaterialError("TLS security reloader has no valid context")
            return self._loaded_security.resolve_ssl_context()

    def _health_locked(self) -> TlsReloadHealth:
        return TlsReloadHealth(
            generation=self._generation,
            material_generation=self._material_generation,
            role=self.role,
            snapshot=self._snapshot,
            changed_at=self._changed_at,
            is_closed=self._is_closed,
            last_error=self._last_error,
        )


def _require_path(value: Path, field_name: str) -> Path:
    if not isinstance(value, Path):
        raise TypeError(f"{field_name} must be a pathlib.Path")
    return value.expanduser().resolve()


def _require_text(value: str | None, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_private_key_permissions(path: Path) -> None:
    stat_result = path.stat()
    if not path.is_file():
        raise TlsMaterialError(f"private key is not a regular file: {path}")
    if os.name == "posix" and stat_result.st_mode & 0o077:
        raise PermissionError(
            f"private key permissions must exclude group/other access: {path}"
        )


def _file_identity(path: Path) -> tuple[str, int, int, int]:
    stat_result = path.stat()
    if not path.is_file():
        raise TlsMaterialError(f"TLS material is not a regular file: {path}")
    return (
        str(path),
        stat_result.st_ino,
        stat_result.st_size,
        stat_result.st_mtime_ns,
    )


__all__ = [
    "MutualTlsFiles",
    "PrivateKeyPassword",
    "TlsEndpointRole",
    "TlsMaterialError",
    "TlsMaterialSnapshot",
    "TlsReloadHealth",
    "TlsReloaderClosed",
    "TlsSecurityReloader",
]
