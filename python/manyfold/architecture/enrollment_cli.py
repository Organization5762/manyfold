"""Manage Manyfold node enrollment and transport credentials."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import sys
from collections.abc import Sequence
from enum import Enum
from pathlib import Path
from typing import Any

from .enrollment import EnrollmentToken, NodeIdentityStore, _read_secure
from .machine_signer import MachineSignerClient
from .transport import NodeIdentity


def _main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "initialize":
            store, token = NodeIdentityStore.initialize(
                args.state_dir,
                cluster_id=args.cluster_id,
                node_id=args.node_id,
                server_names=args.server_name,
            )
            _print({"enrollment_token": token.encode(), "status": _status(store)})
        elif args.command == "enroll":
            encoded_token = (
                args.token
                if args.token_file is None
                else _read_secure(args.token_file).decode("ascii").strip()
            )
            token = EnrollmentToken.decode(encoded_token)
            store, request = NodeIdentityStore.prepare(
                args.state_dir,
                node_id=args.node_id,
                token=token,
                server_names=args.server_name,
            )
            authority = MachineSignerClient(
                args.authority_socket,
                NodeIdentity("enrollment", "enrollment"),
            )
            try:
                store.import_enrollment(
                    token,
                    authority.issue_certificate(token, request),
                )
            finally:
                authority.close()
            _print(_status(store))
        elif args.command == "status":
            _print(
                _status(
                    NodeIdentityStore.open(
                        args.state_dir,
                        require_enrolled=False,
                    )
                )
            )
        else:
            raise RuntimeError(f"unsupported command {args.command!r}")
    except (
        KeyError,
        OSError,
        PermissionError,
        RuntimeError,
        ValueError,
    ) as error:
        print(f"manyfold-enrollment: {error}", file=sys.stderr)
        return 2
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    initialize = commands.add_parser("initialize")
    initialize.add_argument("--state-dir", type=Path, required=True)
    initialize.add_argument("--cluster-id")
    initialize.add_argument("--node-id", required=True)
    initialize.add_argument("--server-name", action="append", default=["localhost"])

    enroll = commands.add_parser("enroll")
    enroll.add_argument("--authority-socket", type=Path, required=True)
    enroll.add_argument("--state-dir", type=Path, required=True)
    enroll.add_argument("--node-id", required=True)
    enroll.add_argument("--server-name", action="append", default=["localhost"])
    token_source = enroll.add_mutually_exclusive_group(required=True)
    token_source.add_argument("--token")
    token_source.add_argument("--token-file", type=Path)

    status = commands.add_parser("status")
    status.add_argument("--state-dir", type=Path, required=True)

    return parser


def _status(store: NodeIdentityStore) -> dict[str, Any]:
    return _json_value(dataclasses.asdict(store.status()))


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def _print(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(_main())
