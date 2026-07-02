from __future__ import annotations

import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from manyfold.private.profiling import ipc_benchmarks


class IpcBenchmarkTests(unittest.TestCase):
    def test_python_baseline_reports_roundtrip_metrics(self) -> None:
        result = ipc_benchmarks.run_python_baseline(
            iterations=10,
            payload_bytes=8,
        )

        self.assertEqual(result.workload, "python_framing")
        self.assertEqual(result.iterations, 10)
        self.assertEqual(result.payload_bytes, 8)
        self.assertGreater(result.average_roundtrip_us, 0.0)
        self.assertGreater(result.roundtrips_per_second, 0.0)

    def test_process_pipe_uses_length_prefixed_worker_protocol(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            worker = Path(directory) / "echo_worker.py"
            worker.write_text(
                textwrap.dedent(
                    """
                    import struct
                    import sys

                    length = struct.Struct("<I")
                    while True:
                        header = sys.stdin.buffer.read(length.size)
                        if not header:
                            break
                        size = length.unpack(header)[0]
                        payload = sys.stdin.buffer.read(size)
                        sys.stdout.buffer.write(header)
                        sys.stdout.buffer.write(payload)
                        sys.stdout.buffer.flush()
                    """
                ).lstrip()
            )

            result = ipc_benchmarks.run_rust_process_pipe(
                worker=Path(sys.executable),
                worker_args=(str(worker),),
                iterations=3,
                payload_bytes=16,
            )

        self.assertEqual(result.workload, "python_to_rust_process_pipe")
        self.assertEqual(result.payload_bytes, 16)

    def test_process_pipe_rejects_missing_worker(self) -> None:
        with self.assertRaises(FileNotFoundError):
            ipc_benchmarks.run_rust_process_pipe(
                worker=Path("missing-worker"),
                iterations=1,
                payload_bytes=1,
            )


if __name__ == "__main__":
    unittest.main()
