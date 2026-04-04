from __future__ import annotations

import unittest

from examples.observe_publish import run_example as run_observe_publish_example
from examples.pipe_route import run_example as run_pipe_route_example
from examples.read_then_write_next_epoch_step import run_example as run_read_then_write_next_epoch_step_example
from examples.write_binding import run_example as run_write_binding_example


class ExampleTests(unittest.TestCase):
    def test_observe_publish_example(self) -> None:
        result = run_observe_publish_example()

        self.assertEqual(result["observed_payloads"], (b"first", b"second"))
        self.assertEqual(result["latest_payload"], b"second")
        self.assertEqual(result["latest_seq"], 2)

    def test_pipe_route_example(self) -> None:
        result = run_pipe_route_example()

        self.assertEqual(result["latest_payload"], b"fast")
        self.assertEqual(result["latest_seq"], 2)

    def test_read_then_write_next_epoch_step_example(self) -> None:
        result = run_read_then_write_next_epoch_step_example()

        self.assertEqual(result["mirrored_writes"], (b"SLOW", b"FAST"))
        self.assertEqual(result["latest_payload"], b"FAST")
        self.assertEqual(result["latest_seq"], 2)

    def test_write_binding_example(self) -> None:
        result = run_write_binding_example()

        self.assertEqual(result["request_payload"], b"42")
        self.assertEqual(result["desired_payload"], b"42")


if __name__ == "__main__":
    unittest.main()
