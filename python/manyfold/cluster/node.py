"""Run one persistent ManyFold coordinator process."""

from __future__ import annotations

import argparse
import json
import logging
import signal
import threading
from functools import partial
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, final
from urllib.parse import parse_qs, urlsplit

from .consensus import (
    MAX_COMMAND_BYTES,
    ClusterConfig,
    ControlCommand,
    CoordinatorUnavailableError,
    NotLeaderError,
    PersistentRaftCoordinator,
)

MAX_HTTP_BODY_BYTES = MAX_COMMAND_BYTES + 4096
HTTP_BACKLOG_SIZE = 16
_LOG = logging.getLogger(__name__)


def serve_node(
    config: ClusterConfig,
    node_id: str,
    state_directory: str | Path,
    stop_event: threading.Event | None = None,
) -> None:
    """Serve one coordinator until signalled or ``stop_event`` is set."""
    member = config.member(node_id)
    coordinator = PersistentRaftCoordinator(config, node_id, state_directory)
    handler = partial(_CoordinatorRequestHandler, coordinator=coordinator)
    try:
        server = _BoundedHTTPServer((member.host, member.api_port), handler)
    except Exception:
        coordinator.close()
        raise
    server.timeout = 0.2
    requested_stop = stop_event if stop_event is not None else threading.Event()

    previous_handlers: dict[int, Any] = {}
    if threading.current_thread() is threading.main_thread():
        for signal_number in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signal_number] = signal.getsignal(signal_number)
            signal.signal(
                signal_number,
                lambda _number, _frame: requested_stop.set(),
            )

    _LOG.info(
        "coordinator node=%s raft=%s api=%s state=%s",
        member.node_id,
        member.raft_address,
        member.api_address,
        coordinator.state_directory,
    )
    try:
        while not requested_stop.is_set():
            server.handle_request()
    finally:
        server.server_close()
        coordinator.close()
        for signal_number, previous_handler in previous_handlers.items():
            signal.signal(signal_number, previous_handler)


def _single_int_parameter(
    query: dict[str, list[str]],
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    values = query.get(name)
    if values is None:
        return default
    if len(values) != 1:
        raise ValueError(f"query parameter {name!r} must occur once")
    try:
        value = int(values[0])
    except ValueError as error:
        raise ValueError(f"query parameter {name!r} must be an integer") from error
    if not minimum <= value <= maximum:
        raise ValueError(
            f"query parameter {name!r} must be from {minimum} through {maximum}"
        )
    return value


def _parse_args(arguments: tuple[str, ...] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    return parser.parse_args(arguments)


def _main(arguments: tuple[str, ...] | None = None) -> None:
    args = _parse_args(arguments)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    serve_node(
        ClusterConfig.load(args.config),
        args.node_id,
        args.state_dir,
    )


@final
class _BoundedHTTPServer(HTTPServer):
    allow_reuse_address = True
    request_queue_size = HTTP_BACKLOG_SIZE


@final
class _CoordinatorRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "ManyFoldCoordinator/1"

    def __init__(
        self,
        *args: Any,
        coordinator: PersistentRaftCoordinator,
        **kwargs: Any,
    ) -> None:
        self._coordinator = coordinator
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:
        """Serve status and bounded local committed-log reads."""
        parsed = urlsplit(self.path)
        try:
            if parsed.path == "/v1/status":
                self._write_json(
                    HTTPStatus.OK,
                    {
                        **self._coordinator.status().to_dict(),
                        "member_count": len(self._coordinator.config.members),
                        "members": [
                            member.to_dict()
                            for member in self._coordinator.config.members
                        ],
                        "network": self._coordinator.config.network.to_dict(),
                    },
                )
                return
            if parsed.path == "/v1/log":
                query = parse_qs(parsed.query, keep_blank_values=True)
                after_sequence = _single_int_parameter(
                    query,
                    "after",
                    default=0,
                    minimum=0,
                    maximum=2**63 - 1,
                )
                limit = _single_int_parameter(
                    query,
                    "limit",
                    default=100,
                    minimum=1,
                    maximum=1000,
                )
                commands = self._coordinator.read_log(
                    after_sequence=after_sequence,
                    limit=limit,
                )
                self._write_json(
                    HTTPStatus.OK,
                    {
                        "commands": [command.to_dict() for command in commands],
                        "next_after": (
                            commands[-1].sequence if commands else after_sequence
                        ),
                    },
                )
                return
            self._write_error(
                HTTPStatus.NOT_FOUND,
                "not_found",
                f"unknown coordinator endpoint {parsed.path!r}",
            )
        except ValueError as error:
            self._write_error(HTTPStatus.BAD_REQUEST, "invalid_request", str(error))
        except Exception:
            _LOG.exception("coordinator GET failed path=%s", self.path)
            self._write_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "internal_error",
                "coordinator request failed",
            )

    def do_POST(self) -> None:
        """Commit one bounded control-plane command or redirect to the leader."""
        parsed = urlsplit(self.path)
        if parsed.path != "/v1/commands":
            self._write_error(
                HTTPStatus.NOT_FOUND,
                "not_found",
                f"unknown coordinator endpoint {parsed.path!r}",
            )
            return
        try:
            value = self._read_json_body()
            command = ControlCommand.from_json(value)
            committed = self._coordinator.commit(command)
        except NotLeaderError as error:
            self._write_not_leader(error)
            return
        except CoordinatorUnavailableError as error:
            self._write_error(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "raft_unavailable",
                str(error),
                headers={"Retry-After": "1"},
            )
            return
        except ValueError as error:
            self._write_error(HTTPStatus.BAD_REQUEST, "invalid_command", str(error))
            return
        except Exception:
            _LOG.exception("coordinator POST failed path=%s", self.path)
            self._write_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "internal_error",
                "coordinator request failed",
            )
            return
        self._write_json(HTTPStatus.CREATED, committed.to_dict())

    def log_message(self, format: str, *args: Any) -> None:
        """Route access logs through the node logger."""
        _LOG.info("%s - %s", self.address_string(), format % args)

    def _read_json_body(self) -> object:
        content_length_value = self.headers.get("Content-Length")
        if content_length_value is None:
            raise ValueError("Content-Length is required")
        try:
            content_length = int(content_length_value)
        except ValueError as error:
            raise ValueError("Content-Length must be an integer") from error
        if content_length < 0:
            raise ValueError("Content-Length must not be negative")
        if content_length > MAX_HTTP_BODY_BYTES:
            self.close_connection = True
            raise ValueError(
                f"request body is {content_length} bytes; maximum is "
                f"{MAX_HTTP_BODY_BYTES}"
            )
        body = self.rfile.read(content_length)
        try:
            return json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"request body must be UTF-8 JSON: {error}") from error

    def _write_not_leader(self, error: NotLeaderError) -> None:
        leader = error.leader
        response = {
            "error": "not_leader",
            "message": str(error),
            "leader": leader.to_dict() if leader is not None else None,
            "members": [
                member.to_dict() for member in self._coordinator.config.members
            ],
            "network": self._coordinator.config.network.to_dict(),
        }
        if leader is None:
            self._write_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                response,
                headers={"Retry-After": "1"},
            )
            return
        self._write_json(
            HTTPStatus.TEMPORARY_REDIRECT,
            response,
            headers={"Location": f"{leader.api_url}/v1/commands"},
        )

    def _write_error(
        self,
        status: HTTPStatus,
        code: str,
        message: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._write_json(
            status,
            {"error": code, "message": message},
            headers=headers,
        )

    def _write_json(
        self,
        status: HTTPStatus,
        value: object,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(
            value,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        for name, header_value in (headers or {}).items():
            self.send_header(name, header_value)
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True


if __name__ == "__main__":
    _main()
