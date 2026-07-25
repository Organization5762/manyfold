"""Untrusted peer endpoint discovery for distributed Manyfold runtimes."""

from __future__ import annotations

import ipaddress
import socket
from collections.abc import Sequence
from dataclasses import dataclass
from threading import Event, Lock
from typing import Protocol, TypeVar, final

from zeroconf import IPVersion, ServiceBrowser, ServiceListener, Zeroconf

_T = TypeVar("_T")

DEFAULT_DISCOVERY_LIMIT = 128
DEFAULT_DISCOVERY_SOURCE_LIMIT = 128
DEFAULT_MDNS_SERVICE = "_manyfold._tcp.local."
DEFAULT_MDNS_TIMEOUT_SECONDS = 1.0


class PeerDiscovery(Protocol):
    """Source of untrusted peer endpoint candidates."""

    @property
    def source_name(self) -> str: ...

    def discover(self) -> "DiscoveryReport": ...


class AddressResolver(Protocol):
    """Resolve an ordinary DNS name to IP addresses."""

    def resolve(self, hostname: str) -> Sequence[str]: ...


class DnsSdResolver(Protocol):
    """Resolve DNS-SD service instances visible on the local link."""

    def resolve(
        self,
        service_type: str,
        *,
        timeout_seconds: float,
    ) -> Sequence["DnsSdService"]: ...


@final
@dataclass(frozen=True)
class PeerEndpoint:
    """Network endpoint that has not established a trusted peer identity."""

    host: str
    port: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "host", _require_text(self.host, "endpoint host"))
        object.__setattr__(self, "port", _require_port(self.port))


@final
@dataclass(frozen=True)
class PeerCandidate:
    """Untrusted endpoint emitted by one discovery source.

    Cluster and node identity are intentionally absent. A transport must
    authenticate the endpoint before passing it to membership.
    """

    endpoint: PeerEndpoint
    source: str
    server_name: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "source", _require_text(self.source, "source"))
        if self.server_name is not None:
            object.__setattr__(
                self,
                "server_name",
                _require_text(self.server_name, "server_name"),
            )


@final
@dataclass(frozen=True)
class DiscoveryFailure:
    """Failure from one discovery source or seed."""

    source: str
    message: str


@final
@dataclass(frozen=True)
class DiscoveryReport:
    """Bounded candidates and non-fatal source failures from one discovery pass."""

    candidates: tuple[PeerCandidate, ...] = ()
    failures: tuple[DiscoveryFailure, ...] = ()


@final
@dataclass(frozen=True)
class DnsSeed:
    """Ordinary DNS seed suitable for unicast DNS and tailnet MagicDNS."""

    hostname: str
    port: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "hostname", _require_text(self.hostname, "hostname"))
        object.__setattr__(self, "port", _require_port(self.port))


@final
@dataclass(frozen=True)
class DnsSdService:
    """Resolved DNS-SD service instance."""

    instance: str
    target: str
    port: int
    addresses: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "instance", _require_text(self.instance, "instance"))
        object.__setattr__(self, "target", _require_text(self.target, "target"))
        object.__setattr__(self, "port", _require_port(self.port))
        object.__setattr__(
            self,
            "addresses",
            tuple(_require_text(address, "service address") for address in self.addresses),
        )


@final
class CompositeDiscovery:
    """Combine discovery sources with endpoint deduplication and a hard limit."""

    def __init__(
        self,
        sources: Sequence[PeerDiscovery],
        *,
        max_candidates: int = DEFAULT_DISCOVERY_LIMIT,
        max_failures: int = DEFAULT_DISCOVERY_LIMIT,
        max_sources: int = DEFAULT_DISCOVERY_SOURCE_LIMIT,
    ) -> None:
        self._sources = _bounded_sequence(
            sources,
            limit=_require_positive_int(max_sources, "max_sources"),
            field="sources",
        )
        self._max_candidates = _require_positive_int(
            max_candidates,
            "max_candidates",
        )
        self._max_failures = _require_positive_int(max_failures, "max_failures")

    @property
    def source_name(self) -> str:
        """Return the stable composite source label."""
        return "composite"

    def discover(self) -> DiscoveryReport:
        """Run every source and return bounded, endpoint-deduplicated candidates."""
        candidates: list[PeerCandidate] = []
        failures: list[DiscoveryFailure] = []
        seen: set[PeerEndpoint] = set()
        for source in self._sources:
            try:
                report = source.discover()
            except Exception as error:
                if len(failures) < self._max_failures:
                    failures.append(
                        DiscoveryFailure(
                            source=source.source_name,
                            message=f"{type(error).__name__}: {error}",
                        )
                    )
                continue
            remaining_failures = self._max_failures - len(failures)
            failures.extend(report.failures[:remaining_failures])
            for candidate in report.candidates:
                if candidate.endpoint in seen:
                    continue
                seen.add(candidate.endpoint)
                candidates.append(candidate)
                if len(candidates) == self._max_candidates:
                    return DiscoveryReport(tuple(candidates), tuple(failures))
        return DiscoveryReport(tuple(candidates), tuple(failures))


@final
class StaticSeedDiscovery:
    """Return a fixed, bounded set of untrusted endpoints."""

    def __init__(
        self,
        endpoints: Sequence[PeerEndpoint],
        *,
        max_candidates: int = DEFAULT_DISCOVERY_LIMIT,
    ) -> None:
        self._endpoints = _deduplicate_endpoints(
            endpoints,
            limit=_require_positive_int(max_candidates, "max_candidates"),
        )

    @property
    def source_name(self) -> str:
        """Return the stable static-seed source label."""
        return "static"

    def discover(self) -> DiscoveryReport:
        """Return configured endpoints without assigning peer identities."""
        return DiscoveryReport(
            tuple(
                PeerCandidate(endpoint=endpoint, source=self.source_name)
                for endpoint in self._endpoints
            )
        )


@final
class SystemAddressResolver:
    """Ordinary system resolver backed by ``socket.getaddrinfo``."""

    def resolve(self, hostname: str) -> Sequence[str]:
        """Resolve A and AAAA addresses using the host's DNS configuration."""
        hostname = _require_text(hostname, "hostname")
        records = socket.getaddrinfo(
            hostname,
            None,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
        )
        addresses = {
            _canonical_ip(record[4][0])
            for record in records
            if record[0] in (socket.AF_INET, socket.AF_INET6)
        }
        return tuple(sorted(addresses))


@final
class DnsDiscovery:
    """Resolve ordinary DNS seeds, including tailnet MagicDNS names.

    DNS proves reachability only. Returned candidates remain untrusted.
    """

    def __init__(
        self,
        seeds: Sequence[DnsSeed],
        *,
        resolver: AddressResolver | None = None,
        max_candidates: int = DEFAULT_DISCOVERY_LIMIT,
        max_seeds: int = DEFAULT_DISCOVERY_SOURCE_LIMIT,
    ) -> None:
        self._seeds = _bounded_sequence(
            seeds,
            limit=_require_positive_int(max_seeds, "max_seeds"),
            field="seeds",
        )
        self._resolver = resolver or SystemAddressResolver()
        self._max_candidates = _require_positive_int(
            max_candidates,
            "max_candidates",
        )

    @property
    def source_name(self) -> str:
        """Return the stable ordinary-DNS source label."""
        return "dns"

    def discover(self) -> DiscoveryReport:
        """Resolve configured hostnames into bounded untrusted endpoints."""
        candidates: list[PeerCandidate] = []
        failures: list[DiscoveryFailure] = []
        seen: set[PeerEndpoint] = set()
        for seed in self._seeds:
            try:
                addresses = self._resolver.resolve(seed.hostname)
            except (OSError, ValueError) as error:
                failures.append(
                    DiscoveryFailure(
                        source=f"dns:{seed.hostname}",
                        message=f"{type(error).__name__}: {error}",
                    )
                )
                continue
            for address in addresses:
                try:
                    endpoint = PeerEndpoint(_canonical_ip(address), seed.port)
                except (TypeError, ValueError) as error:
                    failures.append(
                        DiscoveryFailure(
                            source=f"dns:{seed.hostname}",
                            message=(
                                f"invalid resolved address {address!r}: "
                                f"{type(error).__name__}: {error}"
                            ),
                        )
                    )
                    continue
                if endpoint in seen:
                    continue
                seen.add(endpoint)
                candidates.append(
                    PeerCandidate(
                        endpoint=endpoint,
                        source=f"dns:{seed.hostname}",
                        server_name=seed.hostname,
                    )
                )
                if len(candidates) == self._max_candidates:
                    return DiscoveryReport(tuple(candidates), tuple(failures))
        return DiscoveryReport(tuple(candidates), tuple(failures))


@final
class SystemMdnsResolver:
    """DNS-SD resolver backed by the maintained ``zeroconf`` package."""

    def __init__(self, *, max_services: int = DEFAULT_DISCOVERY_LIMIT) -> None:
        self._max_services = _require_positive_int(max_services, "max_services")

    def resolve(
        self,
        service_type: str,
        *,
        timeout_seconds: float,
    ) -> Sequence[DnsSdService]:
        """Browse one service type with bounded collection and socket lifecycle."""
        service_type = _canonical_dns_name(service_type)
        timeout_seconds = _require_positive_number(
            timeout_seconds,
            "timeout_seconds",
        )
        collector = _ServiceCollector(max_services=self._max_services)
        zeroconf = Zeroconf(ip_version=IPVersion.All)
        browser: ServiceBrowser | None = None
        try:
            browser = ServiceBrowser(
                zeroconf,
                service_type,
                listener=collector,
            )
            collector.wait(timeout_seconds)
            browser.cancel()
            browser = None

            services: list[DnsSdService] = []
            for name in collector.names():
                info = zeroconf.get_service_info(service_type, name, timeout=1)
                if info is None or info.server is None:
                    continue
                services.append(
                    DnsSdService(
                        instance=name,
                        target=info.server,
                        port=info.port,
                        addresses=tuple(
                            sorted(info.parsed_addresses(IPVersion.All))
                        ),
                    )
                )
            return tuple(services)
        finally:
            if browser is not None:
                browser.cancel()
            zeroconf.close()


@final
class MdnsDiscovery:
    """Discover DNS-SD services visible on the current local link."""

    def __init__(
        self,
        *,
        service_type: str = DEFAULT_MDNS_SERVICE,
        resolver: DnsSdResolver | None = None,
        timeout_seconds: float = DEFAULT_MDNS_TIMEOUT_SECONDS,
        max_candidates: int = DEFAULT_DISCOVERY_LIMIT,
    ) -> None:
        self._service_type = _canonical_dns_name(service_type)
        self._resolver = resolver or SystemMdnsResolver()
        self._timeout_seconds = _require_positive_number(
            timeout_seconds,
            "timeout_seconds",
        )
        self._max_candidates = _require_positive_int(
            max_candidates,
            "max_candidates",
        )

    @property
    def source_name(self) -> str:
        """Return the stable mDNS source label."""
        return "mdns"

    def discover(self) -> DiscoveryReport:
        """Resolve local DNS-SD services into bounded untrusted endpoints."""
        try:
            services = self._resolver.resolve(
                self._service_type,
                timeout_seconds=self._timeout_seconds,
            )
        except Exception as error:
            return DiscoveryReport(
                failures=(
                    DiscoveryFailure(
                        source=f"mdns:{self._service_type}",
                        message=f"{type(error).__name__}: {error}",
                    ),
                )
            )

        candidates: list[PeerCandidate] = []
        failures: list[DiscoveryFailure] = []
        seen: set[PeerEndpoint] = set()
        for service in services:
            hosts = service.addresses or (service.target,)
            for host in hosts:
                try:
                    endpoint_host = _canonical_ip(host) if service.addresses else host
                    endpoint = PeerEndpoint(endpoint_host, service.port)
                except (TypeError, ValueError) as error:
                    failures.append(
                        DiscoveryFailure(
                            source=f"mdns:{service.instance}",
                            message=(
                                f"invalid resolved address {host!r}: "
                                f"{type(error).__name__}: {error}"
                            ),
                        )
                    )
                    continue
                if endpoint in seen:
                    continue
                seen.add(endpoint)
                candidates.append(
                    PeerCandidate(
                        endpoint=endpoint,
                        source=f"mdns:{service.instance}",
                        server_name=service.target,
                    )
                )
                if len(candidates) == self._max_candidates:
                    return DiscoveryReport(tuple(candidates), tuple(failures))
        return DiscoveryReport(tuple(candidates), tuple(failures))


def _canonical_dns_name(name: str) -> str:
    return f"{_require_text(name, 'DNS name').rstrip('.')}."


def _canonical_ip(address: str) -> str:
    address = _require_text(address, "IP address")
    zone_index = address.find("%")
    if zone_index >= 0:
        host, zone = address[:zone_index], address[zone_index:]
        return f"{ipaddress.ip_address(host).compressed}{zone}"
    return ipaddress.ip_address(address).compressed


def _bounded_sequence(
    values: Sequence[_T],
    *,
    limit: int,
    field: str,
) -> tuple[_T, ...]:
    if len(values) > limit:
        raise ValueError(f"{field} must contain at most {limit} values")
    return tuple(values)


def _deduplicate_endpoints(
    endpoints: Sequence[PeerEndpoint],
    *,
    limit: int,
) -> tuple[PeerEndpoint, ...]:
    result: list[PeerEndpoint] = []
    seen: set[PeerEndpoint] = set()
    for endpoint in endpoints:
        if not isinstance(endpoint, PeerEndpoint):
            raise ValueError("static endpoints must be PeerEndpoint values")
        if endpoint in seen:
            continue
        seen.add(endpoint)
        result.append(endpoint)
        if len(result) == limit:
            break
    return tuple(result)


def _require_port(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("endpoint port must be an integer")
    if not 1 <= value <= 65535:
        raise ValueError("endpoint port must be between 1 and 65535")
    return value


def _require_positive_int(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _require_positive_number(value: float, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise ValueError(f"{field} must be a positive number")
    return float(value)


def _require_text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


@final
class _ServiceCollector(ServiceListener):
    def __init__(self, *, max_services: int) -> None:
        self._max_services = max_services
        self._names: set[str] = set()
        self._lock = Lock()
        self._wait = Event()

    def add_service(self, zeroconf: Zeroconf, type_: str, name: str) -> None:
        del zeroconf, type_
        with self._lock:
            if len(self._names) < self._max_services:
                self._names.add(name)
                if len(self._names) == self._max_services:
                    self._wait.set()

    def remove_service(self, zeroconf: Zeroconf, type_: str, name: str) -> None:
        del zeroconf, type_
        with self._lock:
            self._names.discard(name)

    def update_service(self, zeroconf: Zeroconf, type_: str, name: str) -> None:
        self.add_service(zeroconf, type_, name)

    def wait(self, timeout_seconds: float) -> None:
        self._wait.wait(timeout_seconds)

    def names(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._names, key=str.casefold))


__all__ = [
    "AddressResolver",
    "CompositeDiscovery",
    "DEFAULT_DISCOVERY_LIMIT",
    "DEFAULT_DISCOVERY_SOURCE_LIMIT",
    "DEFAULT_MDNS_SERVICE",
    "DEFAULT_MDNS_TIMEOUT_SECONDS",
    "DiscoveryFailure",
    "DiscoveryReport",
    "DnsDiscovery",
    "DnsSdResolver",
    "DnsSdService",
    "DnsSeed",
    "MdnsDiscovery",
    "PeerCandidate",
    "PeerDiscovery",
    "PeerEndpoint",
    "StaticSeedDiscovery",
    "SystemAddressResolver",
    "SystemMdnsResolver",
]
