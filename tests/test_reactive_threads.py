from __future__ import annotations

import importlib
import os
import unittest
from unittest.mock import patch

from tests.test_support import load_manyfold_package


class ReactiveThreadsTests(unittest.TestCase):
    def setUp(self) -> None:
        load_manyfold_package()
        self.reactive_threads = importlib.import_module("manyfold.reactive_threads")
        self.rx = importlib.import_module("manyfold._rx")
        self.ops = importlib.import_module("manyfold._rx.operators")
        self.reactive_threads.reset_reactive_threading_state_for_tests()

    def tearDown(self) -> None:
        self.reactive_threads.reset_reactive_threading_state_for_tests()

    def test_shared_schedulers_record_configured_worker_counts(self) -> None:
        env = {
            "MANYFOLD_RX_BACKGROUND_MAX_WORKERS": "7",
            "MANYFOLD_RX_BLOCKING_IO_MAX_WORKERS": "3",
            "MANYFOLD_RX_INPUT_MAX_WORKERS": "5",
        }
        with patch.dict(os.environ, env, clear=False):
            self.reactive_threads.background_scheduler()
            self.reactive_threads.blocking_io_scheduler()
            self.reactive_threads.input_scheduler()

        self.assertEqual(
            self.reactive_threads.scheduler_diagnostics(),
            {
                "background_max_workers": 7,
                "blocking_io_max_workers": 3,
                "input_max_workers": 5,
            },
        )

    def test_shared_schedulers_accept_heart_env_names_during_migration(self) -> None:
        with patch.dict(
            os.environ,
            {
                "HEART_RX_BACKGROUND_MAX_WORKERS": "6",
                "HEART_RX_BLOCKING_IO_MAX_WORKERS": "4",
                "HEART_RX_INPUT_MAX_WORKERS": "2",
            },
            clear=False,
        ):
            self.reactive_threads.background_scheduler()
            self.reactive_threads.blocking_io_scheduler()
            self.reactive_threads.input_scheduler()

        self.assertEqual(
            self.reactive_threads.scheduler_diagnostics(),
            {
                "background_max_workers": 6,
                "blocking_io_max_workers": 4,
                "input_max_workers": 2,
            },
        )

    def test_deliver_on_frame_thread_queues_until_drained(self) -> None:
        source = self.rx.Subject()
        values: list[int] = []

        self.reactive_threads.deliver_on_frame_thread(source).subscribe(values.append)
        source.on_next(1)
        source.on_next(2)

        self.assertEqual(values, [])
        self.assertFalse(self.reactive_threads.on_frame_thread())
        self.assertEqual(self.reactive_threads.drain_frame_thread_queue(max_items=1), 1)
        self.assertEqual(values, [1])
        self.assertTrue(self.reactive_threads.on_frame_thread())
        self.assertEqual(self.reactive_threads.drain_frame_thread_queue(), 1)
        self.assertEqual(values, [1, 2])

        latency = self.reactive_threads.delivery_latency_snapshot()
        self.assertEqual(
            latency[self.reactive_threads.FRAME_THREAD_LATENCY_STREAM].count,
            2,
        )

    def test_latency_snapshot_reports_percentiles_from_sorted_samples(self) -> None:
        recorder = self.reactive_threads._LatencyRecorder()

        for delay_s in (0.005, 0.001, 0.010, 0.002):
            recorder.record("stream", delay_s)

        stats = recorder.snapshot()["stream"]

        self.assertEqual(stats.count, 4)
        self.assertEqual(stats.p50_ms, 2.0)
        self.assertEqual(stats.p95_ms, 10.0)
        self.assertEqual(stats.p99_ms, 10.0)
        self.assertEqual(stats.max_ms, 10.0)

    def test_disposed_frame_thread_delivery_drops_queued_callbacks(self) -> None:
        source = self.rx.Subject()
        values: list[int] = []

        subscription = self.reactive_threads.deliver_on_frame_thread(source).subscribe(
            values.append
        )
        source.on_next(1)
        subscription.dispose()

        self.assertEqual(self.reactive_threads.drain_frame_thread_queue(), 1)
        self.assertEqual(values, [])

    def test_pipe_helpers_apply_operators_and_share_streams(self) -> None:
        background_values: list[int] = []
        self.reactive_threads.pipe_in_background(
            self.rx.from_iterable([1, 2, 3]),
            self.ops.map(lambda value: value * 2),
        ).subscribe(background_values.append)

        main_thread_values: list[int] = []
        source = self.rx.Subject()
        self.reactive_threads.pipe_in_main_thread(
            source,
            self.ops.map(lambda value: value + 10),
        ).subscribe(main_thread_values.append)
        source.on_next(1)
        source.on_next(2)
        self.reactive_threads.drain_frame_thread_queue()

        self.assertEqual(background_values, [2, 4, 6])
        self.assertEqual(main_thread_values, [11, 12])

    def test_share_sequence_exposes_iterable_as_observable(self) -> None:
        values: list[str] = []

        self.reactive_threads.share_sequence(["a", "b"]).subscribe(values.append)

        self.assertEqual(values, ["a", "b"])


if __name__ == "__main__":
    unittest.main()
