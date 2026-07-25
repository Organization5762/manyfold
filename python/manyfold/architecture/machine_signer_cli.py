"""Run and inspect the machine-local Manyfold signer service."""

from __future__ import annotations

import argparse
import json
import signal
import threading
from collections.abc import Sequence
from pathlib import Path

from .enrollment import NodeIdentityStore
from .machine_signer import MachineSignerClient, MachineSignerService
from .transport import NodeIdentity


def _main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "start":
        stop = threading.Event()
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        service = MachineSignerService(
            NodeIdentityStore.open(args.state_dir),
            args.socket,
            allowed_uids=(
                None if args.allowed_uid is None else frozenset(args.allowed_uid)
            ),
            max_clients=args.max_clients,
            max_audit_entries=args.max_audit_entries,
            credential_ttl_seconds=args.credential_ttl_seconds,
        )
        service.start()
        try:
            stop.wait()
        finally:
            service.stop()
        return 0
    client = MachineSignerClient(args.socket, NodeIdentity("status", "status"))
    try:
        if args.command == "status":
            print(json.dumps(client.status(), indent=2, sort_keys=True))
        elif args.command == "issue-token":
            token = client.issue_token()
            print(
                json.dumps(
                    {
                        "enrollment_token": token.encode(),
                        "expires_at": token.expires_at.isoformat(),
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        elif args.command == "revoke":
            print(
                json.dumps(
                    {
                        "node_id": args.node_id,
                        "revoked_certificates": client.revoke(args.node_id),
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        elif args.command == "rotate":
            authority = MachineSignerClient(
                args.authority_socket or args.socket,
                NodeIdentity("status", "status"),
            )
            try:
                client.rotate(authority)
            finally:
                authority.close()
            print(json.dumps(client.status(), indent=2, sort_keys=True))
        else:
            raise RuntimeError(f"unsupported command {args.command!r}")
    finally:
        client.close()
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("--state-dir", type=Path, required=True)
    start.add_argument("--socket", type=Path, required=True)
    start.add_argument("--allowed-uid", action="append", type=int)
    start.add_argument("--max-clients", type=_positive_int, default=16)
    start.add_argument("--max-audit-entries", type=_positive_int, default=256)
    start.add_argument(
        "--credential-ttl-seconds",
        type=_credential_ttl,
        default=300,
    )
    for name in ("status", "issue-token", "revoke", "rotate"):
        command = commands.add_parser(name)
        command.add_argument("--socket", type=Path, required=True)
        if name == "revoke":
            command.add_argument("--node-id", required=True)
        if name == "rotate":
            command.add_argument("--authority-socket", type=Path)
    return parser


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _credential_ttl(value: str) -> int:
    parsed = int(value)
    if not 2 <= parsed <= 3600:
        raise argparse.ArgumentTypeError("value must be between 2 and 3600")
    return parsed


if __name__ == "__main__":
    raise SystemExit(_main())
