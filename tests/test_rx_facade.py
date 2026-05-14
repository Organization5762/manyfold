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

        subscription = (
            rx.from_iterable([1, 2, 3])
            .pipe(
                ops.map(lambda value: value + 1),
            )
            .subscribe(values.append)
        )

        self.assertTrue(callable(getattr(subscription, "dispose", None)))
        self.assertEqual(values, [2, 3, 4])

    def test_private_facade_exposes_subjects_and_schedulers(self) -> None:
        load_manyfold_package()
        scheduler_module = importlib.import_module("manyfold._rx.scheduler")
        subject_module = importlib.import_module("manyfold._rx.subject")
        behavior_module = importlib.import_module(
            "manyfold._rx.subject.behaviorsubject"
        )
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

    def test_private_marble_facade_exports_only_testing_helpers(self) -> None:
        load_manyfold_package()
        marble_module = importlib.import_module("manyfold._rx.testing.marbles")

        self.assertEqual(
            marble_module.__all__,
            ("MarblesContext", "marbles_testing", "messages_to_records"),
        )
        self.assertNotIn("warn", marble_module.__all__)
        self.assertNotIn("typing", marble_module.__all__)

    def test_private_operator_facade_deduplicates_upstream_exports(self) -> None:
        load_manyfold_package()
        operators_module = importlib.import_module("manyfold._rx.operators")
        upstream_module = importlib.import_module("reactivex.operators")

        self.assertEqual(
            operators_module.__all__,
            tuple(sorted(set(upstream_module.__all__))),
        )
        self.assertEqual(
            len(operators_module.__all__), len(set(operators_module.__all__))
        )
        self.assertEqual(operators_module.zip_with_list, upstream_module.zip_with_list)

    def test_private_operator_facade_does_not_leak_upstream_module(self) -> None:
        load_manyfold_package()
        operators_module = importlib.import_module("manyfold._rx.operators")

        self.assertNotIn("_operators", operators_module.__dict__)

    def test_private_marble_facade_does_not_leak_runtime_imports(self) -> None:
        load_manyfold_package()
        marble_module = importlib.import_module("manyfold._rx.testing.marbles")

        self.assertNotIn("datetime", marble_module.__dict__)
        self.assertNotIn("typing", marble_module.__dict__)
        self.assertNotIn("warn", marble_module.__dict__)

    def test_private_testing_facade_exports_only_rx_test_primitives(self) -> None:
        load_manyfold_package()
        testing_module = importlib.import_module("manyfold._rx.testing")

        self.assertEqual(
            testing_module.__all__,
            (
                "MockDisposable",
                "OnErrorPredicate",
                "OnNextPredicate",
                "ReactiveTest",
                "Recorded",
                "TestScheduler",
            ),
        )
        self.assertNotIn("annotations", testing_module.__dict__)
        self.assertNotIn("coldobservable", testing_module.__all__)
        self.assertNotIn("hotobservable", testing_module.__all__)
        self.assertNotIn("is_prime", testing_module.__all__)

    def test_private_subject_submodules_export_only_subject_classes(self) -> None:
        load_manyfold_package()
        expected_exports = {
            "manyfold._rx.subject": (
                "AsyncSubject",
                "BehaviorSubject",
                "ReplaySubject",
                "Subject",
            ),
            "manyfold._rx.subject.asyncsubject": ("AsyncSubject",),
            "manyfold._rx.subject.behaviorsubject": ("BehaviorSubject",),
            "manyfold._rx.subject.replaysubject": ("ReplaySubject",),
            "manyfold._rx.subject.subject": ("Subject",),
        }

        for module_name, names in expected_exports.items():
            with self.subTest(module=module_name):
                module = importlib.import_module(module_name)

                self.assertEqual(module.__all__, names)
                self.assertNotIn("typing", module.__all__)
                self.assertNotIn("abc", module.__all__)
                self.assertNotIn("TypeVar", module.__all__)

    def test_private_rx_facade_exports_intentional_snapshot(self) -> None:
        load_manyfold_package()
        rx_module = importlib.import_module("manyfold._rx")

        self.assertEqual(
            rx_module.__all__,
            (
                "ConnectableObservable",
                "GroupedObservable",
                "Notification",
                "Observable",
                "Observer",
                "Subject",
                "abc",
                "amb",
                "case",
                "catch",
                "catch_with_iterable",
                "combine_latest",
                "compose",
                "concat",
                "concat_with_iterable",
                "create",
                "defer",
                "empty",
                "fork_join",
                "from_callable",
                "from_callback",
                "from_future",
                "from_iterable",
                "from_marbles",
                "generate",
                "generate_with_relative_time",
                "if_then",
                "interval",
                "just",
                "merge",
                "never",
                "of",
                "on_error_resume_next",
                "operators",
                "pipe",
                "range",
                "repeat_value",
                "return_value",
                "start",
                "start_async",
                "throw",
                "timer",
                "to_async",
                "typing",
                "using",
                "with_latest_from",
                "zip",
            ),
        )
        self.assertNotIn("annotations", rx_module.__all__)

    def test_private_rx_facade_submodules_are_stable_on_first_import(self) -> None:
        load_manyfold_package()
        rx_module = importlib.import_module("manyfold._rx")

        self.assertEqual(rx_module.operators.__name__, "manyfold._rx.operators")
        self.assertEqual(rx_module.typing.__name__, "manyfold._rx.typing")
        self.assertIs(
            rx_module.operators,
            importlib.import_module("manyfold._rx.operators"),
        )
        self.assertIs(
            rx_module.typing,
            importlib.import_module("manyfold._rx.typing"),
        )

    def test_private_facade_modules_publish_stable_exports(self) -> None:
        load_manyfold_package()
        expected_exports = {
            "manyfold._rx": ("Observable", "Subject", "from_iterable"),
            "manyfold._rx.abc": ("ObservableBase", "SchedulerBase", "SubjectBase"),
            "manyfold._rx.disposable": ("Disposable", "SerialDisposable"),
            "manyfold._rx.operators": ("map",),
            "manyfold._rx.scheduler": ("TimeoutScheduler",),
            "manyfold._rx.subject": ("BehaviorSubject", "Subject"),
            "manyfold._rx.subject.behaviorsubject": ("BehaviorSubject",),
            "manyfold._rx.subject.replaysubject": ("ReplaySubject",),
            "manyfold._rx.subject.subject": ("Subject",),
            "manyfold._rx.testing": ("ReactiveTest", "TestScheduler"),
            "manyfold._rx.testing.marbles": ("marbles_testing",),
            "manyfold._rx.typing": ("Mapper", "Predicate", "Subscription"),
        }

        for module_name, names in expected_exports.items():
            with self.subTest(module=module_name):
                module = importlib.import_module(module_name)

                self.assertEqual(module.__all__, tuple(sorted(module.__all__)))
                self.assertTrue(
                    all(not name.startswith("_") for name in module.__all__)
                )
                for name in names:
                    self.assertIn(name, module.__all__)
                    self.assertIs(getattr(module, name), module.__dict__[name])

    def test_public_rx_facade_is_not_available(self) -> None:
        load_manyfold_package()

        with self.assertRaises(ModuleNotFoundError):
            importlib.import_module("manyfold.rx")

    def test_core_modules_use_private_facade(self) -> None:
        load_manyfold_package()
        graph_module = importlib.import_module("manyfold.graph")
        primitives_module = importlib.import_module("manyfold.primitives")
        rx_module = importlib.import_module("manyfold._rx")
        subject_module = importlib.import_module("manyfold._rx.subject")

        self.assertIs(graph_module.rx, rx_module)
        self.assertIs(primitives_module.Observable, rx_module.Observable)
        self.assertNotIn("reactivex", graph_module.__dict__)
        self.assertIs(rx_module.Subject, subject_module.Subject)
        self.assertIs(graph_module.Subject, subject_module.Subject)


if __name__ == "__main__":
    unittest.main()
