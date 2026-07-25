from __future__ import annotations

import json
import subprocess
import sys
import time
import unittest
from pathlib import Path

from manyfold.cli import _parse_endpoint

from tests.test_support import subprocess_test_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class CliIntegrationTests(unittest.TestCase):
    def test_node_start_runs_typed_runtime_until_termination(self) -> None:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "manyfold.cli",
                "node",
                "start",
                "--cluster-id",
                "cli-test",
                "--node-id",
                "node-a",
                "--listen-port",
                "0",
                "--without-development-cluster",
                "--startup-peer-timeout",
                "0",
            ],
            cwd=PROJECT_ROOT,
            env=subprocess_test_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            time.sleep(0.5)
            process.terminate()
            stdout, stderr = process.communicate(timeout=5.0)
        except BaseException:
            process.kill()
            process.wait(timeout=5.0)
            raise

        self.assertEqual(process.returncode, 0, stderr)
        summary = json.loads(stdout)
        self.assertEqual(summary["cluster_id"], "cli-test")
        self.assertEqual(summary["node_id"], "node-a")
        self.assertEqual(summary["phase"], "ready")
        self.assertGreater(summary["endpoint"]["port"], 0)

    def test_endpoint_parser_supports_names_and_bracketed_ipv6(self) -> None:
        self.assertEqual(
            _parse_endpoint("node-a.local:7443").host,
            "node-a.local",
        )
        self.assertEqual(_parse_endpoint("[::1]:7443").host, "::1")


if __name__ == "__main__":
    unittest.main()
