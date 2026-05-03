from __future__ import annotations

import importlib
import unittest

from tests.test_support import load_manyfold_package


class RxFacadeTests(unittest.TestCase):
    def test_private_facade_exposes_core_stream_primitives(self) -> None:
        load_manyfold_package()
        rx = importlib.import_module("manyfold._rx")
        ops = importlib.import_module("manyfold._rx.operators")
        values: list[int] = []

        subscription = rx.from_iterable([1, 2, 3]).pipe(
            ops.map(lambda value: value + 1),
        ).subscribe(values.append)

        self.assertTrue(callable(getattr(subscription, "dispose", None)))
        self.assertEqual(values, [2, 3, 4])

    def test_private_facade_exposes_subjects_and_schedulers(self) -> None:
        load_manyfold_package()
        scheduler_module = importlib.import_module("manyfold._rx.scheduler")
        subject_module = importlib.import_module("manyfold._rx.subject")
        behavior_module = importlib.import_module("manyfold._rx.subject.behaviorsubject")
        behavior_subject = subject_module.BehaviorSubject

        subject = behavior_subject(1)
        values: list[int] = []

        subject.subscribe(values.append)
        subject.on_next(2)

        self.assertEqual(values, [1, 2])
        self.assertIs(behavior_module.BehaviorSubject, behavior_subject)
        self.assertIsInstance(
            scheduler_module.TimeoutScheduler(),
            scheduler_module.TimeoutScheduler,
        )

    def test_private_facade_exposes_marble_testing(self) -> None:
        load_manyfold_package()
        marble_module = importlib.import_module("manyfold._rx.testing.marbles")
        with marble_module.marbles_testing() as (start, cold, _hot, exp):
            source = cold("--1-2-|")
            actual = start(source)

        self.assertEqual(actual, exp("--1-2-|"))

    def test_public_rx_facade_is_not_available(self) -> None:
        load_manyfold_package()

        with self.assertRaises(ModuleNotFoundError):
            importlib.import_module("manyfold.rx")


if __name__ == "__main__":
    unittest.main()
