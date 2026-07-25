from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from manyfold.cluster import DevelopmentCluster


class ClusterProcessIntegrationTests(unittest.TestCase):
    def test_quorum_commits_after_leader_kill_and_restarted_member_catches_up(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            cluster = DevelopmentCluster.create(Path(directory) / "cluster")

            with cluster:
                node_ids = [member.node_id for member in cluster.members]
                process_ids = {
                    node_id: cluster.process_id(node_id) for node_id in node_ids
                }
                all_ports = {
                    port
                    for member in cluster.members
                    for port in (member.raft_port, member.api_port)
                }

                self.assertEqual(len(set(process_ids.values())), 3)
                self.assertNotIn(None, process_ids.values())
                self.assertEqual(len(all_ports), 6)
                self.assertEqual(
                    len({cluster.state_directory(node_id) for node_id in node_ids}),
                    3,
                )

                first_leader = cluster.wait_for_leader()
                follower = next(
                    node_id for node_id in node_ids if node_id != first_leader
                )
                redirect = cluster.request(
                    follower,
                    "POST",
                    "/v1/commands",
                    {
                        "command_id": "command-before-kill",
                        "kind": "runtime.mode.set",
                        "payload": {"mode": "maintenance"},
                    },
                )

                self.assertEqual(redirect.status, 307)
                self.assertIsInstance(redirect.body, dict)
                assert isinstance(redirect.body, dict)
                redirect_leader = redirect.body["leader"]
                self.assertIsInstance(redirect_leader, dict)
                assert isinstance(redirect_leader, dict)
                self.assertEqual(redirect_leader["node_id"], first_leader)
                self.assertEqual(
                    redirect.headers["location"],
                    f"{cluster.config.member(first_leader).api_url}/v1/commands",
                )

                first_command = cluster.commit(
                    "runtime.mode.set",
                    {"mode": "maintenance"},
                    command_id="command-before-kill",
                )
                self.assertEqual(first_command["sequence"], 1)
                for node_id in node_ids:
                    cluster.wait_for_log_length(node_id, 1)

                first_leader_pid = cluster.process_id(first_leader)
                cluster.kill_node(first_leader)
                self.assertIsNone(cluster.process_id(first_leader))

                second_leader = cluster.wait_for_leader(
                    excluded_node_ids=frozenset({first_leader})
                )
                self.assertNotEqual(second_leader, first_leader)
                second_command = cluster.commit(
                    "runtime.mode.set",
                    {"mode": "active"},
                    command_id="command-during-recovery",
                )
                self.assertEqual(second_command["sequence"], 2)

                restarted_pid = cluster.start_node(first_leader)
                self.assertNotEqual(restarted_pid, first_leader_pid)
                recovered_log = cluster.wait_for_log_length(first_leader, 2)
                expected_ids = [
                    "command-before-kill",
                    "command-during-recovery",
                ]
                self.assertEqual(
                    [command["command_id"] for command in recovered_log],
                    expected_ids,
                )

                duplicate = cluster.commit(
                    "runtime.mode.set",
                    {"mode": "maintenance"},
                    command_id="command-before-kill",
                )
                self.assertEqual(duplicate["sequence"], 1)

                for node_id in node_ids:
                    applied_log = cluster.wait_for_log_length(node_id, 2)
                    self.assertEqual(
                        [command["sequence"] for command in applied_log],
                        [1, 2],
                    )
                    self.assertEqual(
                        [command["command_id"] for command in applied_log],
                        expected_ids,
                    )
                    status = cluster.status(node_id)
                    self.assertEqual(status["node_id"], node_id)
                    self.assertGreaterEqual(status["control_log_sequence"], 2)

                restarted_state = cluster.state_directory(first_leader)
                self.assertTrue((restarted_state / "identity.json").is_file())
                self.assertTrue((restarted_state / "raft.journal").is_file())
                self.assertTrue((restarted_state / "committed.sqlite3").is_file())

            for node_id in node_ids:
                self.assertIsNone(cluster.process_id(node_id))
