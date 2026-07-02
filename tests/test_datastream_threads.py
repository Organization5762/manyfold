from __future__ import annotations

import importlib
import os
import unittest
from datetime import timedelta
from threading import Event, get_ident
from unittest.mock import patch

from tests.test_support import load_manyfold_package


class DataStreamThreadsTests(unittest.TestCase):
    def setUp(self) -> None:
        load_manyfold_package()
        self.datastream_threads = importlib.import_module("manyfold.datastream_threads")
        self.streams = importlib.import_module("manyfold.streams")
        self.datastream_threads.reset_datastream_delivery_for_tests()

    def tearDown(self) -> None:
        self.datastream_threads.reset_datastream_delivery_for_tests()

    def test_datastream_threads_exports_are_tuple_shaped(self) -> None:
        self.assertIsInstance(self.datastream_threads.__all__, tuple)
        self.assertEqual(
            self.datastream_threads.__all__,
            tuple(sorted(self.datastream_threads.__all__)),
        )
        self.assertEqual(
            len(self.datastream_threads.__all__),
            len(set(self.datastream_threads.__all__)),
        )
        for name in self.datastream_threads.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(self.datastream_threads, name))

    def test_shared_schedulers_record_configured_worker_counts(self) -> None:
        env = {
            "MANYFOLD_DATASTREAM_BACKGROUND_MAX_WORKERS": "7",
            "MANYFOLD_DATASTREAM_BACKGROUND_PRIORITY_QUEUE_LIMIT": "17",
            "MANYFOLD_DATASTREAM_MAIN_THREAD_QUEUE_LIMIT": "11",
            "MANYFOLD_DATASTREAM_BLOCKING_IO_MAX_WORKERS": "3",
            "MANYFOLD_DATASTREAM_INPUT_MAX_WORKERS": "5",
        }
        with patch.dict(os.environ, env, clear=False):
            self.datastream_threads.reset_datastream_delivery_for_tests()
            self.datastream_threads.background_scheduler()
            self.datastream_threads.blocking_io_scheduler()
            self.datastream_threads.input_scheduler()

            self.assertEqual(
                self.datastream_threads._BACKGROUND_SCHEDULER.max_workers,
                7,
            )
            self.assertEqual(
                self.datastream_threads._BACKGROUND_PRIORITY_QUEUE.maxsize,
                17,
            )
            self.assertEqual(
                self.datastream_threads._BLOCKING_IO_SCHEDULER.max_workers,
                3,
            )
            self.assertEqual(self.datastream_threads._MAIN_THREAD_QUEUE_LIMIT, 11)
            self.assertEqual(self.datastream_threads._INPUT_SCHEDULER.max_workers, 5)

    def test_shared_schedulers_accept_heart_env_names_during_migration(self) -> None:
        env = {
            "HEART_DATASTREAM_BACKGROUND_MAX_WORKERS": "6",
            "HEART_DATASTREAM_BLOCKING_IO_MAX_WORKERS": "4",
            "HEART_DATASTREAM_MAIN_THREAD_QUEUE_LIMIT": "13",
            "HEART_DATASTREAM_INPUT_MAX_WORKERS": "2",
        }
        with patch.dict(os.environ, env, clear=False):
            self.datastream_threads.reset_datastream_delivery_for_tests()
            self.datastream_threads.background_scheduler()
            self.datastream_threads.blocking_io_scheduler()
            self.datastream_threads.input_scheduler()

            self.assertEqual(
                self.datastream_threads._BACKGROUND_SCHEDULER.max_workers,
                6,
            )
            self.assertEqual(
                self.datastream_threads._BLOCKING_IO_SCHEDULER.max_workers,
                4,
            )
            self.assertEqual(self.datastream_threads._MAIN_THREAD_QUEUE_LIMIT, 13)
            self.assertEqual(self.datastream_threads._INPUT_SCHEDULER.max_workers, 2)

    def test_legacy_scheduler_env_errors_name_legacy_variable(self) -> None:
        with patch.dict(
            os.environ,
            {"HEART_DATASTREAM_BACKGROUND_MAX_WORKERS": "many"},
            clear=False,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "HEART_DATASTREAM_BACKGROUND_MAX_WORKERS must be an integer",
            ):
                self.datastream_threads.background_scheduler()

    def test_legacy_scheduler_env_minimum_errors_name_legacy_variable(self) -> None:
        with patch.dict(
            os.environ,
            {"HEART_DATASTREAM_BACKGROUND_MAX_WORKERS": "0"},
            clear=False,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "HEART_DATASTREAM_BACKGROUND_MAX_WORKERS must be at least 1",
            ):
                self.datastream_threads.background_scheduler()

    def test_thread_helpers_reject_invalid_thread_names(self) -> None:
        for name in ("", " ", 7):
            with self.subTest(helper="create_default_thread_factory", name=name):
                with self.assertRaisesRegex(ValueError, "thread name"):
                    self.datastream_threads.create_default_thread_factory(name)

            with self.subTest(helper="interval_in_background", name=name):
                with self.assertRaisesRegex(ValueError, "thread name"):
                    self.datastream_threads.interval_in_background(
                        timedelta(milliseconds=1),
                        name=name,
                    )

    def test_delivery_helpers_reject_invalid_observables(self) -> None:
        source = object()
        cases = (
            lambda: self.datastream_threads.deliver_on_main_thread(source),
            lambda: self.datastream_threads.deliver_on_background(source),
        )

        for build in cases:
            with self.subTest(build=build):
                with self.assertRaisesRegex(ValueError, "source must be an Observable"):
                    build()

    def test_thread_name_validation_trims_outer_whitespace(self) -> None:
        thread_factory = self.datastream_threads.create_default_thread_factory(
            " worker-stream "
        )

        thread = thread_factory(lambda: None)

        self.assertEqual(thread.name, "worker-stream")

    def test_interval_in_background_rejects_invalid_periods(self) -> None:
        for period, message in (
            (0.001, "period must be a timedelta"),
            (timedelta(0), "period must be positive"),
            (timedelta(microseconds=-1), "period must be positive"),
        ):
            with self.subTest(period=period):
                with self.assertRaisesRegex(ValueError, message):
                    self.datastream_threads.interval_in_background(period)

    def test_deliver_on_main_thread_coalesces_until_drained(self) -> None:
        source = self.streams.Subject()
        values: list[int] = []

        self.datastream_threads.deliver_on_main_thread(source).subscribe(values.append)
        source.on_next(1)
        source.on_next(2)

        self.assertEqual(values, [])
        self.assertFalse(self.datastream_threads.on_main_thread())
        self.assertEqual(
            self.datastream_threads.drain_main_thread_queue(max_items=1), 1
        )
        self.assertEqual(values, [2])
        self.assertTrue(self.datastream_threads.on_main_thread())
        self.assertEqual(self.datastream_threads.drain_main_thread_queue(), 0)

    def test_drain_main_thread_queue_zero_limit_leaves_queue_pending(self) -> None:
        source = self.streams.Subject()
        values: list[int] = []

        self.datastream_threads.deliver_on_main_thread(source).subscribe(values.append)
        source.on_next(1)

        self.assertEqual(
            self.datastream_threads.drain_main_thread_queue(max_items=0), 0
        )
        self.assertEqual(values, [])
        self.assertEqual(self.datastream_threads.drain_main_thread_queue(), 1)
        self.assertEqual(values, [1])

    def test_main_thread_queue_rejects_work_after_limit(self) -> None:
        first = self.streams.Subject()
        second = self.streams.Subject()
        first_values: list[int] = []
        second_values: list[int] = []
        errors: list[Exception] = []
        old_limit = self.datastream_threads._MAIN_THREAD_QUEUE_LIMIT
        self.datastream_threads._MAIN_THREAD_QUEUE_LIMIT = 1
        try:
            first_subscription = self.datastream_threads.deliver_on_main_thread(
                first
            ).subscribe(first_values.append, on_error=errors.append)
            second_subscription = self.datastream_threads.deliver_on_main_thread(
                second
            ).subscribe(second_values.append, on_error=errors.append)
            try:
                first.on_next(1)
                second.on_next(2)

                self.assertEqual(len(self.datastream_threads._MAIN_THREAD_QUEUE), 1)
                self.assertEqual(len(errors), 1)
                self.assertRegex(str(errors[0]), "main thread queue is full")
                self.assertEqual(self.datastream_threads.drain_main_thread_queue(), 1)
                self.assertEqual(first_values, [1])
                self.assertEqual(second_values, [])
            finally:
                first_subscription.dispose()
                second_subscription.dispose()
        finally:
            self.datastream_threads._MAIN_THREAD_QUEUE_LIMIT = old_limit
            self.datastream_threads.reset_datastream_delivery_for_tests()

    def test_drain_main_thread_queue_rejects_non_integer_limit(self) -> None:
        for max_items in (True, 1.5, "1"):
            with self.subTest(max_items=max_items):
                with self.assertRaisesRegex(
                    ValueError,
                    "max_items must be an integer or None",
                ):
                    self.datastream_threads.drain_main_thread_queue(max_items=max_items)

    def test_drain_main_thread_queue_rejects_negative_limit(self) -> None:
        with self.assertRaisesRegex(ValueError, "max_items must not be negative"):
            self.datastream_threads.drain_main_thread_queue(max_items=-1)

    def test_reset_datastream_delivery_resets_schedulers_queues_and_intervals(
        self,
    ) -> None:
        previous_signal = self.datastream_threads._interval_reset_signal
        self.datastream_threads.background_scheduler()
        self.datastream_threads.blocking_io_scheduler()
        self.datastream_threads.input_scheduler()
        self.datastream_threads.deliver_on_main_thread(
            self.streams.from_iterable([1])
        ).subscribe(lambda value: None)

        self.datastream_threads.reset_datastream_delivery_for_tests()

        self.assertIsNot(
            self.datastream_threads._interval_reset_signal, previous_signal
        )
        self.assertIsNone(self.datastream_threads._BACKGROUND_SCHEDULER.scheduler)
        self.assertIsNone(self.datastream_threads._BLOCKING_IO_SCHEDULER.scheduler)
        self.assertIsNone(self.datastream_threads._INPUT_SCHEDULER.scheduler)
        self.assertEqual(len(self.datastream_threads._MAIN_THREAD_QUEUE), 0)
        self.assertIsNone(self.datastream_threads._MAIN_THREAD_IDENT)

    def test_disposed_main_thread_delivery_drops_queued_callbacks(self) -> None:
        source = self.streams.Subject()
        values: list[int] = []

        subscription = self.datastream_threads.deliver_on_main_thread(source).subscribe(
            values.append
        )
        source.on_next(1)
        subscription.dispose()

        self.assertEqual(self.datastream_threads.drain_main_thread_queue(), 1)
        self.assertEqual(values, [])

    def test_deliver_on_background_delivers_off_caller_thread(self) -> None:
        source = self.streams.Subject()
        caller_thread = get_ident()
        delivered = Event()
        values: list[tuple[int, int]] = []

        subscription = self.datastream_threads.deliver_on_background(source).subscribe(
            lambda value: (values.append((value, get_ident())), delivered.set())
        )
        try:
            source.on_next(3)

            self.assertTrue(delivered.wait(timeout=1.0))
            self.assertEqual(values[0][0], 3)
            self.assertNotEqual(values[0][1], caller_thread)
        finally:
            subscription.dispose()

    def test_deliver_on_background_coalesces_until_worker_runs(self) -> None:
        source = self.streams.Subject()
        started = Event()
        release = Event()
        delivered = Event()
        values: list[int] = []

        self.datastream_threads._enqueue_background_priority_task(
            lambda: (started.set(), release.wait(timeout=1.0)),
            priority=self.datastream_threads.DataStreamPriority.LOW,
        )
        self.assertTrue(started.wait(timeout=1.0))

        subscription = self.datastream_threads.deliver_on_background(
            source,
            priority=self.datastream_threads.DataStreamPriority.LOW,
        ).subscribe(lambda value: (values.append(value), delivered.set()))
        try:
            source.on_next(1)
            source.on_next(2)
            release.set()

            self.assertTrue(delivered.wait(timeout=1.0))
            self.assertEqual(values, [2])
        finally:
            subscription.dispose()

    def test_background_priority_worker_prefers_higher_priority_pending_work(
        self,
    ) -> None:
        started = Event()
        release = Event()
        completed = Event()
        values: list[str] = []

        def record(value: str) -> None:
            values.append(value)
            if len(values) == 3:
                completed.set()

        self.datastream_threads._enqueue_background_priority_task(
            lambda: (started.set(), release.wait(timeout=1.0), record("low-blocking")),
            priority=self.datastream_threads.DataStreamPriority.LOW,
        )
        self.assertTrue(started.wait(timeout=1.0))
        self.datastream_threads._enqueue_background_priority_task(
            lambda: record("low-pending"),
            priority=self.datastream_threads.DataStreamPriority.LOW,
        )
        self.datastream_threads._enqueue_background_priority_task(
            lambda: record("high-pending"),
            priority=self.datastream_threads.DataStreamPriority.HIGH,
        )
        release.set()

        self.assertTrue(completed.wait(timeout=1.0))
        self.assertEqual(values, ["low-blocking", "high-pending", "low-pending"])

    def test_deliver_on_background_can_promote_pending_work_with_dynamic_priority(
        self,
    ) -> None:
        source = self.streams.Subject()
        active = False
        started = Event()
        release = Event()
        completed = Event()
        values: list[str] = []

        def current_priority() -> object:
            if active:
                return self.datastream_threads.DataStreamPriority.HIGH
            return self.datastream_threads.DataStreamPriority.LOW

        def record(value: str) -> None:
            values.append(value)
            if len(values) == 3:
                completed.set()

        self.datastream_threads._enqueue_background_priority_task(
            lambda: (started.set(), release.wait(timeout=1.0), record("low-blocking")),
            priority=self.datastream_threads.DataStreamPriority.LOW,
        )
        self.assertTrue(started.wait(timeout=1.0))

        subscription = self.datastream_threads.deliver_on_background(
            source,
            priority=current_priority,
        ).subscribe(lambda value: record(f"dynamic-{value}"))
        try:
            source.on_next(1)
            active = True
            source.on_next(2)
            self.datastream_threads._enqueue_background_priority_task(
                lambda: record("low-pending"),
                priority=self.datastream_threads.DataStreamPriority.LOW,
            )
            release.set()

            self.assertTrue(completed.wait(timeout=1.0))
            self.assertEqual(values, ["low-blocking", "dynamic-2", "low-pending"])
        finally:
            subscription.dispose()

    def test_deliver_on_background_rejects_invalid_dynamic_priority_result(
        self,
    ) -> None:
        source = self.streams.Subject()
        errors: list[Exception] = []

        self.datastream_threads.deliver_on_background(
            source,
            priority=lambda: "urgent",
        ).subscribe(on_error=errors.append)
        source.on_next(1)

        self.assertEqual(len(errors), 1)
        self.assertRegex(str(errors[0]), "priority must be a DataStreamPriority")

    def test_deliver_on_background_rejects_invalid_priority(self) -> None:
        source = self.streams.Subject()

        for priority in (True, 7, "low"):
            with self.subTest(priority=priority):
                with self.assertRaisesRegex(
                    ValueError,
                    "priority must be a DataStreamPriority",
                ):
                    self.datastream_threads.deliver_on_background(
                        source,
                        priority=priority,
                    )


if __name__ == "__main__":
    unittest.main()
