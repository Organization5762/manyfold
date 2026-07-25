"""Command-line workflows for ManyFold."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import threading
from pathlib import Path

from manyfold.architecture import (
    CompositeDiscovery,
    DnsDiscovery,
    DnsSeed,
    MdnsDiscovery,
    MembershipConfig,
    NodeIdentity,
    PeerEndpoint,
    StaticSeedDiscovery,
    TcpAddress,
)
from manyfold.cluster import (
    DevelopmentCluster,
    LocalDevelopmentTransportSecurityProvider,
    NodeConfig,
    NodeRuntime,
)

DEFAULT_NODE_PORT = 7443


def _parse_args(arguments: tuple[str, ...] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    node = commands.add_parser("node", help="manage one ManyFold node")
    node_commands = node.add_subparsers(dest="node_command", required=True)
    start = node_commands.add_parser(
        "start",
        help="start and continuously reconcile one node",
    )
    start.add_argument("--cluster-id", default="development")
    start.add_argument("--node-id", required=True)
    start.add_argument("--listen-host", default="127.0.0.1")
    start.add_argument("--listen-port", type=int, default=DEFAULT_NODE_PORT)
    start.add_argument(
        "--peer",
        action="append",
        default=[],
        metavar="HOST:PORT",
        help="static peer endpoint; may be repeated",
    )
    start.add_argument(
        "--dns-seed",
        action="append",
        default=[],
        metavar="HOST:PORT",
        help="ordinary DNS or MagicDNS seed; may be repeated",
    )
    start.add_argument(
        "--mdns",
        action="store_true",
        help="also browse the local _manyfold._tcp mDNS service",
    )
    start.add_argument(
        "--state-root",
        type=Path,
        default=None,
        help="development control-plane state root",
    )
    start.add_argument(
        "--without-development-cluster",
        action="store_true",
        help="do not start the local three-process development control plane",
    )
    start.add_argument("--max-peers", type=int, default=32)
    start.add_argument("--diagnostic-limit", type=int, default=128)
    start.add_argument("--reconcile-interval", type=float, default=1.0)
    start.add_argument("--startup-peer-timeout", type=float, default=2.0)
    return parser.parse_args(arguments)


def _main(arguments: tuple[str, ...] | None = None) -> None:
    args = _parse_args(arguments)
    if args.command == "node" and args.node_command == "start":
        _start_node(args)
        return
    raise RuntimeError(f"unsupported command: {args.command!r}")


def _start_node(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    static_endpoints = tuple(_parse_endpoint(value) for value in args.peer)
    dns_seeds = tuple(
        DnsSeed(endpoint.host, endpoint.port)
        for endpoint in (_parse_endpoint(value) for value in args.dns_seed)
    )
    sources = []
    if static_endpoints:
        sources.append(StaticSeedDiscovery(static_endpoints))
    if dns_seeds:
        sources.append(DnsDiscovery(dns_seeds))
    if args.mdns:
        sources.append(MdnsDiscovery())

    identity = NodeIdentity(args.cluster_id, args.node_id)
    state_root = (
        args.state_root
        if args.state_root is not None
        else Path(".manyfold-node") / args.node_id / "control"
    )
    development_cluster = (
        None
        if args.without_development_cluster
        else DevelopmentCluster.create(state_root)
    )
    node = NodeRuntime(
        NodeConfig(
            identity=identity,
            listen_address=TcpAddress(args.listen_host, args.listen_port),
            discovery=CompositeDiscovery(tuple(sources), max_candidates=args.max_peers),
            transport_security_provider=(LocalDevelopmentTransportSecurityProvider()),
            membership=MembershipConfig(max_members=args.max_peers + 1),
            development_cluster=development_cluster,
            max_peers=args.max_peers,
            diagnostic_limit=args.diagnostic_limit,
            reconcile_interval_seconds=args.reconcile_interval,
            startup_peer_timeout_seconds=args.startup_peer_timeout,
        )
    )
    stop_event = threading.Event()
    previous_handlers = {
        signal_number: signal.getsignal(signal_number)
        for signal_number in (signal.SIGINT, signal.SIGTERM)
    }
    for signal_number in previous_handlers:
        signal.signal(signal_number, lambda _number, _frame: stop_event.set())

    try:
        node.start()
        print(json.dumps(_summary(node), indent=2, sort_keys=True), flush=True)
        stop_event.wait()
    finally:
        node.stop()
        for signal_number, previous_handler in previous_handlers.items():
            signal.signal(signal_number, previous_handler)


def _summary(node: NodeRuntime) -> dict[str, object]:
    snapshot = node.snapshot()
    endpoint = snapshot.endpoint
    return {
        "cluster_id": snapshot.identity.cluster_id,
        "node_id": snapshot.identity.node_id,
        "instance_id": snapshot.identity.instance_id,
        "phase": snapshot.phase.value,
        "credential_expires_at_epoch_seconds": (
            snapshot.credential_expires_at_epoch_seconds
        ),
        "endpoint": (
            None if endpoint is None else {"host": endpoint.host, "port": endpoint.port}
        ),
        "members": [
            {
                "node_id": member.identity.node_id,
                "instance_id": member.identity.instance_id,
                "state": member.state.value,
                "host": member.endpoint.host,
                "port": member.endpoint.port,
            }
            for member in snapshot.members
        ],
        "diagnostics": [
            {
                "sequence": diagnostic.sequence,
                "phase": diagnostic.phase.value,
                "severity": diagnostic.severity.value,
                "code": diagnostic.code,
                "message": diagnostic.message,
                "action": diagnostic.action,
            }
            for diagnostic in snapshot.diagnostics
        ],
    }


def _parse_endpoint(value: str) -> PeerEndpoint:
    value = value.strip()
    if value.startswith("["):
        close = value.find("]")
        if close < 0 or close + 1 >= len(value) or value[close + 1] != ":":
            raise argparse.ArgumentTypeError(f"endpoint {value!r} must be [IPv6]:PORT")
        host = value[1:close]
        port_text = value[close + 2 :]
    else:
        host, separator, port_text = value.rpartition(":")
        if not separator or ":" in host:
            raise argparse.ArgumentTypeError(
                f"endpoint {value!r} must be HOST:PORT or [IPv6]:PORT"
            )
    try:
        port = int(port_text)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"endpoint {value!r} has a non-integer port"
        ) from error
    return PeerEndpoint(host, port)


if __name__ == "__main__":
    _main()
