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
                    manyfold.Lego("Bridge", "communication", "local", "", ("Pipe",)),
                    manyfold.Lego(
                        "Pipe",
                        "communication",
                        "local",
                        "",
                        ("RouteRef", "Schema", "RouteRef"),
                    ),
                )
            ),
            ("Pipe: RouteRef",),
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


if __name__ == "__main__":
    unittest.main()
