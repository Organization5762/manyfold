from __future__ import annotations

import unittest

from test_support import load_example_module


class ExampleTests(unittest.TestCase):
    def test_simple_latest_example(self) -> None:
        result = load_example_module("simple_latest").run_example()

        self.assertEqual(result["latest_payload"], b"hello")
        self.assertEqual(result["latest_seq"], 1)

    def test_observe_publish_example(self) -> None:
        result = load_example_module("observe_publish").run_example()

        self.assertEqual(result["observed_payloads"], (b"first", b"second"))
        self.assertEqual(result["latest_payload"], b"second")
        self.assertEqual(result["latest_seq"], 2)

    def test_pipe_route_example(self) -> None:
        result = load_example_module("pipe_route").run_example()

        self.assertEqual(result["latest_payload"], b"fast")
        self.assertEqual(result["latest_seq"], 2)

    def test_read_then_write_next_epoch_step_example(self) -> None:
        result = load_example_module("read_then_write_next_epoch_step").run_example()

        self.assertEqual(result["mirrored_writes"], (b"SLOW", b"FAST"))
        self.assertEqual(result["latest_payload"], b"FAST")
        self.assertEqual(result["latest_seq"], 2)

    def test_write_binding_example(self) -> None:
        result = load_example_module("write_binding").run_example()

        self.assertEqual(result["request_payload"], b"42")
        self.assertEqual(result["desired_payload"], b"42")

    def test_uart_temperature_sensor_example(self) -> None:
        result = load_example_module("uart_temperature_sensor").run_example()

        self.assertEqual(result["raw_latest"], 24)
        self.assertEqual(result["smoothed_latest"], 24)
        self.assertEqual(result["profile_issues"], ())

    def test_lazy_lidar_payload_example(self) -> None:
        result = load_example_module("lazy_lidar_payload").run_example()

        self.assertEqual(result["selected_frame"], b"frame-2-points")
        self.assertEqual(result["metadata_count"], 2)
        self.assertEqual(result["profile_issues"], ())

    def test_closed_counter_loop_example(self) -> None:
        result = load_example_module("closed_counter_loop").run_example()

        self.assertEqual(result["desired_latest"], b"2")
        self.assertEqual(result["effective_latest"], b"2")
        self.assertEqual(result["ack_latest"], b"ok")

    def test_brightness_control_example(self) -> None:
        result = load_example_module("brightness_control").run_example()

        self.assertEqual(result["pwm_latest"], b"\xff")
        self.assertEqual(result["pwm_seq"], 3)

    def test_mailbox_bridge_example(self) -> None:
        result = load_example_module("mailbox_bridge").run_example()

        self.assertEqual(result["capacity"], 4)
        self.assertEqual(result["overflow_policy"], "drop_oldest")
        self.assertEqual(len(result["topology_edges"]), 2)

    def test_broadcast_mirror_example(self) -> None:
        result = load_example_module("broadcast_mirror").run_example()

        self.assertEqual(result["mirror_a"], (b"v1", b"v2"))
        self.assertEqual(result["mirror_b"], (b"v1", b"v2"))


if __name__ == "__main__":
    unittest.main()
