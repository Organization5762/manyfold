"""Run a persistent three-process ManyFold coordinator cluster on one host."""

from __future__ import annotations

import argparse
import http.client
import json
import logging
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import IO, final

from .consensus import ClusterConfig, MemberConfig

DEFAULT_HOST = "127.0.0.1"
DEFAULT_START_TIMEOUT_SECONDS = 15.0
DEFAULT_OPERATION_TIMEOUT_SECONDS = 10.0
_LOG = logging.getLogger(__name__)


@final
@dataclass(frozen=True)
class HttpResponse:
    """One raw coordinator API response without automatic redirect following."""

    status: int
    headers: dict[str, str]
    body: object


@final
class DevelopmentCluster:
    """Own three coordinator subprocesses with isolated durable state."""

    def __init__(self, root: str | Path, config: ClusterConfig) -> None:
        self.root = Path(root).resolve()
        self.config = config
        self.config_path = self.root / "cluster.json"
        self._processes: dict[str, _NodeProcess] = {}

    @classmethod
    def create(
        cls,
        root: str | Path,
        *,
        host: str = DEFAULT_HOST,
    ) -> DevelopmentCluster:
        """Create or reopen a durable one-host three-member configuration."""
        _require_loopback_host(host)
        cluster_root = Path(root).resolve()
        cluster_root.mkdir(parents=True, exist_ok=True)
        config_path = cluster_root / "cluster.json"
        if config_path.exists():
            config = ClusterConfig.load(config_path)
            configured_hosts = {member.host for member in config.members}
            if configured_hosts != {host}:
                raise ValueError(
                    f"existing cluster uses hosts {sorted(configured_hosts)!r}, "
                    f"not requested host {host!r}"
                )
        else:
            ports = _reserve_ports(6, host)
            config = ClusterConfig(
                tuple(
                    MemberConfig(
                        node_id=f"node-{index + 1}",
                        host=host,
                        raft_port=ports[index * 2],
                        api_port=ports[index * 2 + 1],
                    )
                    for index in range(3)
                )
            )
            config.save(config_path)
        return cls(cluster_root, config)

    @property
    def members(self) -> tuple[MemberConfig, ...]:
        """Return the fixed members in deterministic node order."""
        return self.config.members

    def state_directory(self, node_id: str) -> Path:
        """Return one member's durable, identity-bound state directory."""
        self.config.member(node_id)
        return self.root / "nodes" / node_id

    def start(self) -> None:
        """Start all members, failing if any process exits during election."""
        try:
            for member in self.members:
                self.start_node(member.node_id)
            self.wait_for_leader(timeout_seconds=DEFAULT_START_TIMEOUT_SECONDS)
        except Exception:
            self.stop()
            raise

    def start_node(self, node_id: str) -> int:
        """Start or restart one member against its existing durable state."""
        member = self.config.member(node_id)
        existing = self._processes.get(node_id)
        if existing is not None and existing.process.poll() is None:
            return existing.process.pid
        if existing is not None:
            existing.close_log()

        state_directory = self.state_directory(node_id)
        state_directory.mkdir(parents=True, exist_ok=True)
        log_path = state_directory / "node.log"
        log_stream = log_path.open("ab", buffering=0)
        command = (
            sys.executable,
            "-m",
            "manyfold.cluster.node",
            "--config",
            str(self.config_path),
            "--node-id",
            member.node_id,
            "--state-dir",
            str(state_directory),
        )
        try:
            process = subprocess.Popen(
                command,
                cwd=self.root,
                stdin=subprocess.DEVNULL,
                stdout=log_stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except Exception:
            log_stream.close()
            raise
        self._processes[node_id] = _NodeProcess(
            member=member,
            process=process,
            log_path=log_path,
            log_stream=log_stream,
        )
        return process.pid

    def stop(self) -> None:
        """Terminate every member and wait for deterministic Raft shutdown."""
        for member in reversed(self.members):
            self.stop_node(member.node_id)

    def stop_node(self, node_id: str, *, timeout_seconds: float = 5.0) -> None:
        """Terminate one member, escalating to a kill only after a deadline."""
        node_process = self._processes.get(node_id)
        if node_process is None:
            return
        process = node_process.process
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=timeout_seconds)
        node_process.close_log()
        del self._processes[node_id]

    def kill_node(self, node_id: str) -> None:
        """Abruptly kill one member to exercise crash recovery."""
        node_process = self._require_running_process(node_id)
        node_process.process.kill()
        node_process.process.wait(timeout=5.0)
        node_process.close_log()
        del self._processes[node_id]

    def process_id(self, node_id: str) -> int | None:
        """Return a running member PID, or ``None``."""
        node_process = self._processes.get(node_id)
        if node_process is None or node_process.process.poll() is not None:
            return None
        return node_process.process.pid

    def status(self, node_id: str) -> dict[str, object]:
        """Read one member's local Raft status and discovery metadata."""
        response = self.request(node_id, "GET", "/v1/status")
        if response.status != 200 or not isinstance(response.body, dict):
            raise RuntimeError(
                f"status request to {node_id!r} returned "
                f"{response.status}: {response.body!r}"
            )
        return response.body

    def read_log(self, node_id: str) -> tuple[dict[str, object], ...]:
        """Read one member's locally applied committed control log."""
        response = self.request(node_id, "GET", "/v1/log?limit=1000")
        if response.status != 200 or not isinstance(response.body, dict):
            raise RuntimeError(
                f"log request to {node_id!r} returned "
                f"{response.status}: {response.body!r}"
            )
        commands = response.body.get("commands")
        if not isinstance(commands, list) or not all(
            isinstance(command, dict) for command in commands
        ):
            raise RuntimeError(
                f"log request to {node_id!r} returned invalid commands {commands!r}"
            )
        return tuple(commands)

    def request(
        self,
        node_id: str,
        method: str,
        path: str,
        body: object | None = None,
        *,
        timeout_seconds: float = 2.0,
    ) -> HttpResponse:
        """Issue one API request without following a follower redirect."""
        member = self.config.member(node_id)
        headers: dict[str, str] = {}
        encoded_body: bytes | None = None
        if body is not None:
            encoded_body = json.dumps(
                body,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(encoded_body))
        connection = http.client.HTTPConnection(
            member.host,
            member.api_port,
            timeout=timeout_seconds,
        )
        try:
            connection.request(method, path, body=encoded_body, headers=headers)
            response = connection.getresponse()
            response_body = response.read()
            response_headers = {
                name.lower(): value for name, value in response.getheaders()
            }
        finally:
            connection.close()
        try:
            decoded: object = json.loads(response_body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RuntimeError(
                f"{node_id!r} returned non-JSON HTTP {response.status}"
            ) from error
        return HttpResponse(response.status, response_headers, decoded)

    def wait_for_leader(
        self,
        *,
        timeout_seconds: float = DEFAULT_OPERATION_TIMEOUT_SECONDS,
        excluded_node_ids: frozenset[str] = frozenset(),
    ) -> str:
        """Wait for exactly one live, quorum-backed leader and return its ID."""
        deadline = time.monotonic() + timeout_seconds
        last_statuses: dict[str, object] = {}
        while time.monotonic() < deadline:
            leaders: list[str] = []
            last_statuses = {}
            for member in self.members:
                if member.node_id in excluded_node_ids:
                    continue
                node_process = self._processes.get(member.node_id)
                if node_process is None:
                    continue
                return_code = node_process.process.poll()
                if return_code is not None:
                    raise RuntimeError(
                        f"coordinator {member.node_id!r} exited with "
                        f"status {return_code}; see {node_process.log_path}"
                    )
                try:
                    status = self.status(member.node_id)
                except (ConnectionError, OSError, RuntimeError):
                    continue
                last_statuses[member.node_id] = status
                if status.get("role") == "leader" and status.get("has_quorum") is True:
                    leaders.append(member.node_id)
            if len(leaders) == 1:
                return leaders[0]
            time.sleep(0.05)
        raise TimeoutError(
            f"cluster did not elect one quorum-backed leader within "
            f"{timeout_seconds} seconds; statuses={last_statuses!r}"
        )

    def commit(
        self,
        kind: str,
        payload: dict[str, object],
        *,
        command_id: str | None = None,
        timeout_seconds: float = DEFAULT_OPERATION_TIMEOUT_SECONDS,
    ) -> dict[str, object]:
        """Discover the current leader and commit one control command."""
        identifier = command_id if command_id is not None else uuid.uuid4().hex
        request_body = {
            "command_id": identifier,
            "kind": kind,
            "payload": payload,
        }
        deadline = time.monotonic() + timeout_seconds
        target: str | None = None
        last_response: HttpResponse | None = None
        while time.monotonic() < deadline:
            if target is None or self.process_id(target) is None:
                try:
                    target = self.wait_for_leader(
                        timeout_seconds=max(0.1, deadline - time.monotonic())
                    )
                except TimeoutError:
                    break
            try:
                response = self.request(
                    target,
                    "POST",
                    "/v1/commands",
                    request_body,
                )
            except (ConnectionError, OSError):
                target = None
                time.sleep(0.05)
                continue
            last_response = response
            if response.status == 201 and isinstance(response.body, dict):
                return response.body
            if response.status == 307 and isinstance(response.body, dict):
                leader = response.body.get("leader")
                target = (
                    leader.get("node_id")
                    if isinstance(leader, dict)
                    and isinstance(leader.get("node_id"), str)
                    else None
                )
                continue
            if response.status == 503:
                target = None
                time.sleep(0.05)
                continue
            raise RuntimeError(
                f"command commit returned HTTP {response.status}: {response.body!r}"
            )
        raise TimeoutError(
            f"control command {identifier!r} did not commit within "
            f"{timeout_seconds} seconds; last_response={last_response!r}"
        )

    def wait_for_log_length(
        self,
        node_id: str,
        length: int,
        *,
        timeout_seconds: float = DEFAULT_OPERATION_TIMEOUT_SECONDS,
    ) -> tuple[dict[str, object], ...]:
        """Wait for one member to apply at least ``length`` commands."""
        if isinstance(length, bool) or not isinstance(length, int) or length < 0:
            raise ValueError("length must be a non-negative integer")
        deadline = time.monotonic() + timeout_seconds
        last_log: tuple[dict[str, object], ...] = ()
        while time.monotonic() < deadline:
            try:
                last_log = self.read_log(node_id)
            except (ConnectionError, OSError, RuntimeError):
                time.sleep(0.05)
                continue
            if len(last_log) >= length:
                return last_log
            time.sleep(0.05)
        raise TimeoutError(
            f"coordinator {node_id!r} did not apply {length} commands within "
            f"{timeout_seconds} seconds; observed={last_log!r}"
        )

    def __enter__(self) -> DevelopmentCluster:
        self.start()
        return self

    def __exit__(
        self,
        exception_type: object,
        exception: object,
        traceback: object,
    ) -> None:
        self.stop()

    def _require_running_process(self, node_id: str) -> _NodeProcess:
        self.config.member(node_id)
        node_process = self._processes.get(node_id)
        if node_process is None or node_process.process.poll() is not None:
            raise RuntimeError(f"coordinator {node_id!r} is not running")
        return node_process


def _reserve_ports(count: int, host: str) -> tuple[int, ...]:
    reservations: list[socket.socket] = []
    try:
        for _index in range(count):
            reservation = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            reservation.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            reservation.bind((host, 0))
            reservations.append(reservation)
        return tuple(int(reservation.getsockname()[1]) for reservation in reservations)
    finally:
        for reservation in reservations:
            reservation.close()


def _require_loopback_host(host: str) -> None:
    if host not in {"127.0.0.1", "localhost"}:
        raise ValueError(
            "development cluster host must be loopback because its HTTP API "
            "is not authenticated"
        )


def _parse_args(arguments: tuple[str, ...] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(".manyfold-dev-cluster"),
        help="durable cluster state root (default: .manyfold-dev-cluster)",
    )
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=f"one-host bind address (default: {DEFAULT_HOST})",
    )
    return parser.parse_args(arguments)


def _main(arguments: tuple[str, ...] | None = None) -> None:
    args = _parse_args(arguments)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    cluster = DevelopmentCluster.create(args.root, host=args.host)
    stop_event = threading.Event()
    previous_handlers = {
        signal_number: signal.getsignal(signal_number)
        for signal_number in (signal.SIGINT, signal.SIGTERM)
    }
    for signal_number in previous_handlers:
        signal.signal(
            signal_number,
            lambda _number, _frame: stop_event.set(),
        )

    try:
        cluster.start()
        leader = cluster.wait_for_leader()
        summary = {
            "leader": leader,
            "root": str(cluster.root),
            "members": [
                {
                    **member.to_dict(),
                    "pid": cluster.process_id(member.node_id),
                    "state_directory": str(cluster.state_directory(member.node_id)),
                }
                for member in cluster.members
            ],
        }
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
        while not stop_event.wait(0.5):
            for member in cluster.members:
                node_process = cluster._processes[member.node_id]
                return_code = node_process.process.poll()
                if return_code is not None:
                    raise RuntimeError(
                        f"coordinator {member.node_id!r} exited with "
                        f"status {return_code}; see {node_process.log_path}"
                    )
    finally:
        cluster.stop()
        for signal_number, previous_handler in previous_handlers.items():
            signal.signal(signal_number, previous_handler)


@final
@dataclass
class _NodeProcess:
    member: MemberConfig
    process: subprocess.Popen[bytes]
    log_path: Path
    log_stream: IO[bytes]

    def close_log(self) -> None:
        if not self.log_stream.closed:
            self.log_stream.close()


if __name__ == "__main__":
    _main()
