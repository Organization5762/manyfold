from __future__ import annotations

import sys
import unittest

from tests.test_support import load_manyfold_package


class LegoCatalogTests(unittest.TestCase):
    def test_lego_catalog_exports_intentional_surface(self) -> None:
        manyfold = load_manyfold_package()
        lego_catalog = sys.modules["manyfold.lego_catalog"]

        self.assertEqual(
            lego_catalog.__all__,
            (
                "Lego",
                "all_legos",
                "dependencies_of",
                "dependency_closure_of",
                "dependents_of",
                "get_lego",
                "legos_by_layer",
                "legos_by_role",
            ),
        )
        for name in lego_catalog.__all__:
            with self.subTest(name=name):
                self.assertIs(getattr(lego_catalog, name), getattr(manyfold, name))

    def test_catalog_dependencies_reference_known_legos(self) -> None:
        manyfold = load_manyfold_package()
        known = {lego.name for lego in manyfold.all_legos()}

        for lego in manyfold.all_legos():
            self.assertLessEqual(set(lego.requires), known, lego.name)

    def test_catalog_names_are_unique(self) -> None:
        manyfold = load_manyfold_package()
        lego_catalog = sys.modules["manyfold.lego_catalog"]

        names = [lego.name for lego in manyfold.all_legos()]

        self.assertEqual(lego_catalog._duplicate_names(names), ())
        self.assertEqual(
            lego_catalog._duplicate_names(["Key", "Bytes", "Bytes", "Key"]),
            ("Bytes", "Key"),
        )

    def test_catalog_dependencies_are_unique_per_lego(self) -> None:
        manyfold = load_manyfold_package()
        lego_catalog = sys.modules["manyfold.lego_catalog"]

        self.assertEqual(
            lego_catalog._duplicate_requirements_by_lego(manyfold.all_legos()), ()
        )
        self.assertEqual(
            lego_catalog._duplicate_requirements_by_lego(
                (
                    manyfold.Lego(
                        "Bridge",
                        "communication",
                        "local",
                        "Connects routes.",
                        ("Pipe",),
                    ),
                    manyfold.Lego(
                        "Pipe",
                        "communication",
                        "local",
                        "Moves events.",
                        ("RouteRef", "Schema", "RouteRef"),
                    ),
                )
            ),
            ("Pipe: RouteRef",),
        )

    def test_lego_constructor_rejects_blank_identity_text(self) -> None:
        manyfold = load_manyfold_package()

        for kwargs, message in (
            ({"name": " "}, "name must be a non-empty string"),
            ({"role": ""}, "role must be a non-empty string"),
            ({"layer": "\t"}, "layer must be a non-empty string"),
            ({"contract": ""}, "contract must be a non-empty string"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.Lego(
                        **{
                            "name": "Widget",
                            "role": "compute",
                            "layer": "local",
                            "contract": "Does one thing.",
                            **kwargs,
                        }
                    )

    def test_lego_constructor_rejects_malformed_text_tuples(self) -> None:
        manyfold = load_manyfold_package()

        for kwargs, message in (
            ({"requires": ["Clock"]}, "requires must be a tuple of strings"),
            ({"provides": (" ",)}, "provides must contain non-empty strings"),
            ({"knobs": (1,)}, "knobs must contain non-empty strings"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(ValueError, message):
                    manyfold.Lego(
                        **{
                            "name": "Widget",
                            "role": "compute",
                            "layer": "local",
                            "contract": "Does one thing.",
                            **kwargs,
                        }
                    )

    def test_consensus_dependencies_are_lowered_components(self) -> None:
        manyfold = load_manyfold_package()

        dependencies = {lego.name for lego in manyfold.dependencies_of("Consensus")}

        self.assertGreaterEqual(
            dependencies,
            {
                "EventLog",
                "SnapshotStore",
                "Membership",
                "Quorum",
                "LeaderElection",
                "Term",
                "Timer",
                "Transport",
            },
        )

    def test_retry_loop_dependencies_keep_retry_low_level(self) -> None:
        manyfold = load_manyfold_package()

        dependencies = [lego.name for lego in manyfold.dependencies_of("RetryLoop")]

        self.assertEqual(
            dependencies,
            ["RetryPolicy", "BackoffPolicy", "Timeout", "Cancellation"],
        )

    def test_local_sensor_source_depends_on_sequence_counter(self) -> None:
        manyfold = load_manyfold_package()

        dependencies = {
            lego.name for lego in manyfold.dependencies_of("LocalSensorSource")
        }

        self.assertIn("SequenceCounter", dependencies)
        self.assertIn("SensorSample", dependencies)

    def test_role_and_layer_queries_are_sorted(self) -> None:
        manyfold = load_manyfold_package()

        persistence = manyfold.legos_by_role("persistence")
        durable = manyfold.legos_by_layer("durable")

        self.assertEqual(
            [lego.name for lego in persistence],
            sorted(lego.name for lego in persistence),
        )
        self.assertEqual(
            [lego.name for lego in durable],
            sorted(lego.name for lego in durable),
        )

    def test_dependents_report_direct_users(self) -> None:
        manyfold = load_manyfold_package()

        dependents = [lego.name for lego in manyfold.dependents_of("EventLog")]

        self.assertIn("Consensus", dependents)
        self.assertIn("DurableQueue", dependents)
        self.assertIn("Workflow", dependents)
        self.assertEqual(dependents, sorted(dependents))

    def test_dependency_closure_reports_transitive_build_order(self) -> None:
        manyfold = load_manyfold_package()

        closure = [lego.name for lego in manyfold.dependency_closure_of("Workflow")]

        self.assertEqual(len(closure), len(set(closure)))
        self.assertIn("ByteStore", closure)
        self.assertIn("DurableQueue", closure)
        self.assertIn("RetryLoop", closure)
        self.assertNotIn("Workflow", closure)
        self.assertLess(closure.index("ByteStore"), closure.index("EventLog"))
        self.assertLess(closure.index("EventLog"), closure.index("DurableQueue"))

    def test_dependency_closure_accepts_leaf_legos(self) -> None:
        manyfold = load_manyfold_package()

        self.assertEqual(manyfold.dependency_closure_of("Bytes"), ())

    def test_catalog_queries_reject_malformed_lookup_text(self) -> None:
        manyfold = load_manyfold_package()

        for lookup, value, message in (
            (manyfold.get_lego, (""), "name must be a non-empty string"),
            (manyfold.dependencies_of, (" "), "name must be a non-empty string"),
            (manyfold.dependency_closure_of, (None), "name must be a non-empty string"),
            (manyfold.dependents_of, (1), "name must be a non-empty string"),
            (manyfold.legos_by_role, ("\t"), "role must be a non-empty string"),
            (manyfold.legos_by_layer, (b"local"), "layer must be a non-empty string"),
        ):
            with self.subTest(lookup=lookup.__name__, value=value):
                with self.assertRaisesRegex(ValueError, message):
                    lookup(value)


if __name__ == "__main__":
    unittest.main()
