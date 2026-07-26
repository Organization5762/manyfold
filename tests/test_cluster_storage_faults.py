from __future__ import annotations

import errno
import pickle
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Literal

from manyfold.cluster import (
    ClusterConfig,
    CommittedCommand,
    ControlCommand,
    MemberConfig,
    PersistentRaftCoordinator,
)
from manyfold.cluster.consensus import CoordinatorUnavailableError


class ClusterStorageFaultTests(unittest.TestCase):
    def test_enospc_before_control_log_write_recovers_exactly_once(self) -> None:
        self._assert_fault_recovery("before_write", errno.ENOSPC)

    def test_interruption_before_durable_commit_recovers_exactly_once(self) -> None:
        self._assert_fault_recovery("before_commit", errno.EIO)

    def test_runtime_boundary_is_excluded_from_raft_snapshot_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            boundary = _UnserializableWriteBoundary()
            coordinator = PersistentRaftCoordinator(
                _single_node_config(),
                "node-1",
                directory,
                durable_write_boundary=boundary,
            )
            try:
                control_log = getattr(coordinator, "_control_log")
                snapshot = control_log._serialize()
                control_log._deserialize(pickle.loads(pickle.dumps(snapshot)))
                self.assertIs(
                    getattr(control_log, "_durable_write_boundary"),
                    boundary,
                )
            finally:
                coordinator.close()

    def _assert_fault_recovery(
        self,
        stage: Literal["before_write", "before_commit"],
        error_number: int,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = _single_node_config()
            fault = _OneShotWriteFault(stage, error_number)
            coordinator = PersistentRaftCoordinator(
                config,
                "node-1",
                directory,
                durable_write_boundary=fault,
            )
            try:
                _wait_for_leader(coordinator)
                with self.assertRaisesRegex(
                    CoordinatorUnavailableError,
                    "outcome may be unknown",
                ):
                    coordinator.commit(
                        ControlCommand(
                            "faulted-command",
                            "qualification.storage",
                            {"stage": stage},
                        ),
                        timeout_seconds=2,
                    )
                applied = _wait_for_log_length(coordinator, 1)
                self.assertEqual(
                    [command.command_id for command in applied],
                    ["faulted-command"],
                )
                self.assertEqual(fault.calls, [stage])
            finally:
                coordinator.close()

            reopened = PersistentRaftCoordinator(
                config,
                "node-1",
                directory,
            )
            try:
                _wait_for_leader(reopened)
                self.assertEqual(
                    [command.command_id for command in reopened.read_log()],
                    ["faulted-command"],
                )
                next_command = reopened.commit(
                    ControlCommand(
                        "after-recovery",
                        "qualification.storage",
                        {"recovered": True},
                    )
                )
                self.assertEqual(next_command.sequence, 2)
            finally:
                reopened.close()


def _single_node_config() -> ClusterConfig:
    raft_port, api_port = _reserve_ports(2)
    return ClusterConfig(
        (
            MemberConfig(
                "node-1",
                "127.0.0.1",
                raft_port,
                api_port,
            ),
        )
    )


def _reserve_ports(count: int) -> tuple[int, ...]:
    sockets: list[socket.socket] = []
    try:
        for _ in range(count):
            stream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            stream.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            stream.bind(("127.0.0.1", 0))
            sockets.append(stream)
        return tuple(int(stream.getsockname()[1]) for stream in sockets)
    finally:
        for stream in sockets:
            stream.close()


def _wait_for_leader(coordinator: PersistentRaftCoordinator) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if coordinator.status().role == "leader":
            return
        time.sleep(0.05)
    raise TimeoutError("single-node coordinator did not become leader")


def _wait_for_log_length(
    coordinator: PersistentRaftCoordinator,
    length: int,
) -> tuple[CommittedCommand, ...]:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        commands = coordinator.read_log()
        if len(commands) >= length:
            return commands
        time.sleep(0.05)
    raise TimeoutError(f"control log did not reach {length} entries")


class _OneShotWriteFault:
    def __init__(
        self,
        stage: Literal["before_write", "before_commit"],
        error_number: int,
    ) -> None:
        self.stage = stage
        self.error_number = error_number
        self.calls: list[str] = []

    def before_write(self, path: Path) -> None:
        self._raise_once("before_write", path)

    def before_commit(self, path: Path) -> None:
        self._raise_once("before_commit", path)

    def _raise_once(self, stage: str, path: Path) -> None:
        if stage != self.stage or self.calls:
            return
        self.calls.append(stage)
        raise OSError(
            self.error_number,
            f"injected {stage} failure at {path}",
        )


class _UnserializableWriteBoundary:
    def __init__(self) -> None:
        self.lock = threading.Lock()

    def before_write(self, path: Path) -> None:
        del path

    def before_commit(self, path: Path) -> None:
        del path


if __name__ == "__main__":
    unittest.main()
