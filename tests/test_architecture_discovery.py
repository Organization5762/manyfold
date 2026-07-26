from __future__ import annotations

import socket
import unittest

from manyfold.architecture import (
    CompositeDiscovery,
    DiscoveryReport,
    DnsDiscovery,
    DnsSdService,
    DnsSeed,
    MdnsDiscovery,
    PeerCandidate,
    PeerEndpoint,
    StaticSeedDiscovery,
)
from manyfold.architecture.discovery import DiscoveryFailure


class ArchitectureDiscoveryTests(unittest.TestCase):
    def test_static_candidates_are_untrusted_endpoints_without_identity(self) -> None:
        endpoint = PeerEndpoint("node-a.example", 7443)
        discovery = StaticSeedDiscovery((endpoint, endpoint))

        report = discovery.discover()

        self.assertEqual(
            report.candidates,
            (PeerCandidate(endpoint=endpoint, source="static"),),
        )
        self.assertFalse(hasattr(report.candidates[0], "cluster_id"))
        self.assertFalse(hasattr(report.candidates[0], "node_id"))

    def test_system_dns_resolves_localhost_as_untrusted_reachability(self) -> None:
        report = DnsDiscovery((DnsSeed("localhost", 7443),)).discover()

        self.assertFalse(report.failures)
        self.assertTrue(report.candidates)
        self.assertTrue(
            all(candidate.server_name == "localhost" for candidate in report.candidates)
        )
        for candidate in report.candidates:
            socket.inet_pton(
                socket.AF_INET6 if ":" in candidate.endpoint.host else socket.AF_INET,
                candidate.endpoint.host,
            )

    def test_dns_skips_malformed_resolver_address_and_keeps_valid_candidate(
        self,
    ) -> None:
        report = DnsDiscovery(
            (DnsSeed("node-a.example", 7443),),
            resolver=_ResolvedAddresses(("not-an-ip", "192.0.2.10")),
        ).discover()

        self.assertEqual(
            tuple(candidate.endpoint.host for candidate in report.candidates),
            ("192.0.2.10",),
        )
        self.assertEqual(len(report.failures), 1)
        self.assertIn("invalid resolved address", report.failures[0].message)

    def test_dns_bounds_malformed_resolver_failures(self) -> None:
        report = DnsDiscovery(
            (DnsSeed("node-a.example", 7443),),
            resolver=_ResolvedAddresses(tuple(f"invalid-{index}" for index in range(20))),
            max_failures=3,
        ).discover()

        self.assertFalse(report.candidates)
        self.assertEqual(len(report.failures), 3)

    def test_composite_discovery_deduplicates_and_bounds_candidates(self) -> None:
        first = PeerEndpoint("10.0.0.1", 7443)
        second = PeerEndpoint("10.0.0.2", 7443)
        discovery = CompositeDiscovery(
            (
                StaticSeedDiscovery((first,)),
                StaticSeedDiscovery((first, second)),
            ),
            max_candidates=2,
        )

        report = discovery.discover()

        self.assertEqual(
            tuple(candidate.endpoint for candidate in report.candidates),
            (first, second),
        )

    def test_mdns_discovery_uses_dns_sd_target_for_session_server_name(self) -> None:
        resolver = _ResolvedDnsSd(
            (
                DnsSdService(
                    instance="node-a._manyfold._tcp.local.",
                    target="node-a.local.",
                    port=7443,
                    addresses=("192.0.2.10", "2001:db8::10"),
                ),
            )
        )
        discovery = MdnsDiscovery(resolver=resolver)

        report = discovery.discover()

        self.assertEqual(
            tuple(candidate.endpoint.host for candidate in report.candidates),
            ("192.0.2.10", "2001:db8::10"),
        )
        self.assertTrue(
            all(
                candidate.server_name == "node-a.local."
                for candidate in report.candidates
            )
        )
        self.assertEqual(resolver.calls, 1)

    def test_mdns_skips_malformed_service_address_and_keeps_valid_candidate(
        self,
    ) -> None:
        report = MdnsDiscovery(
            resolver=_ResolvedDnsSd(
                (
                    DnsSdService(
                        instance="node-a._manyfold._tcp.local.",
                        target="node-a.local.",
                        port=7443,
                        addresses=("not-an-ip", "192.0.2.10"),
                    ),
                )
            )
        ).discover()

        self.assertEqual(
            tuple(candidate.endpoint.host for candidate in report.candidates),
            ("192.0.2.10",),
        )
        self.assertEqual(len(report.failures), 1)
        self.assertIn("invalid resolved address", report.failures[0].message)

    def test_mdns_bounds_malformed_service_failures(self) -> None:
        report = MdnsDiscovery(
            resolver=_ResolvedDnsSd(
                (
                    DnsSdService(
                        instance="node-a._manyfold._tcp.local.",
                        target="node-a.local.",
                        port=7443,
                        addresses=tuple(f"invalid-{index}" for index in range(20)),
                    ),
                )
            ),
            max_failures=3,
        ).discover()

        self.assertFalse(report.candidates)
        self.assertEqual(len(report.failures), 3)

    def test_composite_reports_one_source_failure_and_continues(self) -> None:
        endpoint = PeerEndpoint("10.0.0.1", 7443)
        discovery = CompositeDiscovery(
            (_BrokenDiscovery(), StaticSeedDiscovery((endpoint,)))
        )

        report = discovery.discover()

        self.assertEqual(
            tuple(candidate.endpoint for candidate in report.candidates),
            (endpoint,),
        )
        self.assertEqual(len(report.failures), 1)
        self.assertEqual(report.failures[0].source, "broken")
        self.assertIn("resolver unavailable", report.failures[0].message)

    def test_composite_bounds_sources_and_accumulated_failures(self) -> None:
        with self.assertRaisesRegex(ValueError, "sources.*at most 2"):
            CompositeDiscovery(
                (_BrokenDiscovery(), _BrokenDiscovery(), _BrokenDiscovery()),
                max_sources=2,
            )

        report = CompositeDiscovery(
            (_ManyFailuresDiscovery(),),
            max_failures=3,
        ).discover()

        self.assertEqual(len(report.failures), 3)

    def test_dns_discovery_bounds_retained_seed_configuration(self) -> None:
        seeds = tuple(DnsSeed(f"node-{index}.example", 7443) for index in range(3))

        with self.assertRaisesRegex(ValueError, "seeds.*at most 2"):
            DnsDiscovery(seeds, max_seeds=2)


class _ResolvedDnsSd:
    def __init__(self, services: tuple[DnsSdService, ...]) -> None:
        self.services = services
        self.calls = 0

    def resolve(
        self,
        service_type: str,
        *,
        timeout_seconds: float,
    ) -> tuple[DnsSdService, ...]:
        self.calls += 1
        if service_type != "_manyfold._tcp.local.":
            raise ValueError(f"unexpected service type: {service_type}")
        if timeout_seconds <= 0:
            raise ValueError("timeout must be positive")
        return self.services


class _ResolvedAddresses:
    def __init__(self, addresses: tuple[str, ...]) -> None:
        self.addresses = addresses

    def resolve(self, hostname: str) -> tuple[str, ...]:
        if not hostname:
            raise ValueError("hostname must not be empty")
        return self.addresses


class _BrokenDiscovery:
    @property
    def source_name(self) -> str:
        return "broken"

    def discover(self) -> DiscoveryReport:
        raise OSError("resolver unavailable")


class _ManyFailuresDiscovery:
    @property
    def source_name(self) -> str:
        return "many-failures"

    def discover(self) -> DiscoveryReport:
        return DiscoveryReport(
            failures=tuple(
                DiscoveryFailure(source=self.source_name, message=str(index))
                for index in range(100)
            )
        )


if __name__ == "__main__":
    unittest.main()
