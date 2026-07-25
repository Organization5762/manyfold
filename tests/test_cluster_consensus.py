from __future__ import annotations

import math
import tempfile
import unittest
from pathlib import Path

from manyfold.cluster import (
    MAX_COMMAND_BYTES,
    ClusterConfig,
    ControlCommand,
    DevelopmentCluster,
    MemberConfig,
)


class ClusterConfigTests(unittest.TestCase):
    def test_development_cluster_rejects_non_loopback_http_binding(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "must be loopback"):
                DevelopmentCluster.create(directory, host="0.0.0.0")

    def test_cluster_config_round_trip_preserves_distinct_identities(self) -> None:
        config = _cluster_config()

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cluster.json"
            config.save(path)
            loaded = ClusterConfig.load(path)

        self.assertEqual(loaded, config)
        self.assertEqual(
            {member.node_id for member in loaded.members},
            {"node-1", "node-2", "node-3"},
        )
        self.assertEqual(
            len(
                {
                    port
                    for member in loaded.members
                    for port in (member.raft_port, member.api_port)
                }
            ),
            6,
        )

    def test_cluster_config_rejects_shared_one_host_port(self) -> None:
        members = list(_cluster_config().members)
        members[2] = MemberConfig("node-3", "127.0.0.1", 21004, 21006)

        with self.assertRaisesRegex(ValueError, "ports must be distinct"):
            ClusterConfig(tuple(members))


class ControlCommandTests(unittest.TestCase):
    def test_control_command_canonicalizes_json_payload(self) -> None:
        command = ControlCommand.from_json(
            {
                "command_id": "deploy/2026-07-25",
                "kind": "deployment.set",
                "payload": {"replicas": 3, "enabled": True},
            }
        )

        self.assertEqual(
            command.payload_json,
            '{"enabled":true,"replicas":3}',
        )

    def test_control_command_rejects_non_finite_json(self) -> None:
        with self.assertRaisesRegex(ValueError, "finite JSON"):
            ControlCommand(
                command_id="bad-number",
                kind="configuration.set",
                payload={"threshold": math.nan},
            )

    def test_control_command_rejects_payload_over_hard_limit(self) -> None:
        with self.assertRaisesRegex(ValueError, "maximum"):
            ControlCommand(
                command_id="oversized",
                kind="configuration.set",
                payload={"value": "x" * MAX_COMMAND_BYTES},
            )


def _cluster_config() -> ClusterConfig:
    return ClusterConfig(
        (
            MemberConfig("node-1", "127.0.0.1", 21001, 21002),
            MemberConfig("node-2", "127.0.0.1", 21003, 21004),
            MemberConfig("node-3", "127.0.0.1", 21005, 21006),
        )
    )
