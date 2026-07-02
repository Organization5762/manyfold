from __future__ import annotations

import unittest
from threading import Event, get_ident

from manyfold.architecture import (
    CallbackDelivery,
    CallbackPlacement,
    drain_main_thread_callbacks,
    on_main_callback_thread,
)
from manyfold.architecture.callbacks import reset_callback_delivery_for_tests


class ArchitectureCallbackDeliveryTests(unittest.TestCase):
    def tearDown(self) -> None:
        reset_callback_delivery_for_tests()

    def test_inline_delivery_runs_callback_immediately(self) -> None:
        delivery = CallbackDelivery(CallbackPlacement.inline())
        values: list[int] = []

        self.assertTrue(delivery.deliver(values.append, 3))

        self.assertEqual(values, [3])

    def test_main_thread_delivery_runs_when_drained(self) -> None:
        delivery = CallbackDelivery(CallbackPlacement.main_thread())
        values: list[int] = []

        self.assertTrue(delivery.deliver(values.append, 1))
        self.assertTrue(delivery.deliver(values.append, 2))
        self.assertEqual(values, [])
        self.assertFalse(on_main_callback_thread())

        self.assertEqual(drain_main_thread_callbacks(max_items=1), 1)
        self.assertEqual(values, [1])
        self.assertTrue(on_main_callback_thread())
        self.assertEqual(drain_main_thread_callbacks(), 1)
        self.assertEqual(values, [1, 2])

    def test_main_thread_delivery_reports_full_queue(self) -> None:
        delivery = CallbackDelivery(CallbackPlacement.main_thread(queue_limit=1))

        self.assertTrue(delivery.submit(lambda: None))
        self.assertFalse(delivery.submit(lambda: None))

    def test_spawned_thread_delivery_runs_off_caller_thread(self) -> None:
        caller_thread = get_ident()
        delivered = Event()
        values: list[tuple[int, int]] = []
        delivery = CallbackDelivery(CallbackPlacement.spawned_thread("callbacks"))
        try:
            self.assertTrue(
                delivery.deliver(
                    lambda value: (
                        values.append((value, get_ident())),
                        delivered.set(),
                    ),
                    7,
                )
            )

            self.assertTrue(delivered.wait(timeout=1.0))
            self.assertEqual(values[0][0], 7)
            self.assertNotEqual(values[0][1], caller_thread)
        finally:
            delivery.close()

    def test_delivery_rejects_invalid_inputs(self) -> None:
        with self.assertRaisesRegex(ValueError, "thread name"):
            CallbackPlacement.spawned_thread(" ")
        with self.assertRaisesRegex(ValueError, "queue_limit"):
            CallbackPlacement.main_thread(queue_limit=0)
        with self.assertRaisesRegex(TypeError, "callback"):
            CallbackDelivery().submit(object())  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
