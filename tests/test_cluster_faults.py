from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from manyfold.cluster.dev_cluster import (
    CONTROL_LOG_FAULT_TARGET,
    RAFT_JOURNAL_FAULT_TARGET,
    DevelopmentCluster,
)
from manyfold.cluster.network import (
    DISCONNECT_FAULT_LAYER,
    NetworkProtocolConfig,
)


class ClusterNetworkFaultIntegrationTests(unittest.TestCase):
    def test_five_node_quorum_commits_through_live_disconnect_and_recovers(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cluster = DevelopmentCluster.create(
                Path(directory) / "cluster",
                node_count=5,
                network=NetworkProtocolConfig(
                    layers=(DISCONNECT_FAULT_LAYER,),
                ),
            )

            with cluster:
                node_ids = [member.node_id for member in cluster.members]
                process_ids = {
                    node_id: cluster.process_id(node_id) for node_id in node_ids
                }
                leader = cluster.wait_for_leader()
                disconnected = [node_id for node_id in node_ids if node_id != leader][
                    :2
                ]

                first = cluster.commit(
                    "topology.set",
                    {"generation": 1},
                    command_id="before-disconnect",
                )
                self.assertEqual(first["sequence"], 1)
                for node_id in node_ids:
                    cluster.wait_for_log_length(node_id, 1)

                for node_id in disconnected:
                    cluster.disconnect_node(node_id)
                    _wait_for_quorum_state(cluster, node_id, has_quorum=False)

                second = cluster.commit(
                    "topology.set",
                    {"generation": 2},
                    command_id="during-disconnect",
                )
                self.assertEqual(second["sequence"], 2)
                for node_id in set(node_ids) - set(disconnected):
                    cluster.wait_for_log_length(node_id, 2)

                for node_id in disconnected:
                    cluster.reconnect_node(node_id)
                    cluster.wait_for_log_length(node_id, 2)

                for node_id in node_ids:
                    self.assertEqual(cluster.process_id(node_id), process_ids[node_id])
                    self.assertEqual(
                        [
                            command["command_id"]
                            for command in cluster.read_log(node_id)
                        ],
                        ["before-disconnect", "during-disconnect"],
                    )


class ClusterDiskFaultIntegrationTests(unittest.TestCase):
    def test_corrupt_control_log_and_raft_journal_fail_fast(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cluster = DevelopmentCluster.create(Path(directory) / "cluster")

            with cluster:
                leader = cluster.wait_for_leader()
                followers = [
                    member.node_id
                    for member in cluster.members
                    if member.node_id != leader
                ]
                cluster.commit(
                    "configuration.set",
                    {"revision": 1},
                    command_id="seed-state",
                )
                for node_id in followers:
                    cluster.wait_for_log_length(node_id, 1)

                targets = (
                    (followers[0], CONTROL_LOG_FAULT_TARGET, "committed.sqlite3"),
                    (followers[1], RAFT_JOURNAL_FAULT_TARGET, "raft.journal"),
                )
                for node_id, target, filename in targets:
                    with self.subTest(target=target):
                        cluster.stop_node(node_id)
                        corrupted_path = cluster.corrupt_state(node_id, target)
                        self.assertEqual(corrupted_path.name, filename)
                        cluster.start_node(node_id)
                        return_code = cluster.wait_for_node_exit(node_id)

                        self.assertNotEqual(return_code, 0)
                        log_text = (
                            cluster.state_directory(node_id) / "node.log"
                        ).read_text(encoding="utf-8")
                        self.assertIn("coordinator state is corrupt", log_text)
                        self.assertIn(filename, log_text)


def _wait_for_quorum_state(
    cluster: DevelopmentCluster,
    node_id: str,
    *,
    has_quorum: bool,
    timeout_seconds: float = 5.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_status: dict[str, object] | None = None
    while time.monotonic() < deadline:
        last_status = cluster.status(node_id)
        if last_status.get("has_quorum") is has_quorum:
            return
        time.sleep(0.05)
    raise TimeoutError(
        f"coordinator {node_id!r} quorum did not become {has_quorum}; "
        f"last_status={last_status!r}"
    )
