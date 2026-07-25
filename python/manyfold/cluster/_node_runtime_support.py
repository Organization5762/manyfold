"""Private transport helpers for the node bootstrap runtime."""

from __future__ import annotations

import ipaddress
from dataclasses import replace

from manyfold.architecture.discovery import PeerCandidate, PeerEndpoint
from manyfold.architecture.transport import (
    TransportConfig,
    TransportSecurityMode,
)


def _connector_config(
    config: TransportConfig,
    candidate: PeerCandidate,
) -> TransportConfig:
    security = config.security
    if (
        security.mode is TransportSecurityMode.MUTUAL_TLS
        and candidate.server_name is not None
        and security.server_hostname != candidate.server_name
    ):
        security = replace(security, server_hostname=candidate.server_name)
        return replace(config, security=security)
    return config


def _is_local_candidate(
    candidate: PeerEndpoint,
    local: PeerEndpoint,
) -> bool:
    if candidate.port != local.port:
        return False
    if candidate.host.casefold() == local.host.casefold():
        return True
    return _is_loopback(candidate.host) and _is_loopback(local.host)


def _is_loopback(host: str) -> bool:
    if host.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host.split("%", 1)[0]).is_loopback
    except ValueError:
        return False
