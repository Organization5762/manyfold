from __future__ import annotations

import unittest

from test_support import load_manyfold_package


class ReferenceExampleSuiteTests(unittest.TestCase):
    def test_reference_example_suite_declares_all_rfc_examples(self) -> None:
        manyfold = load_manyfold_package()
        suite = manyfold.reference_example_suite()

        self.assertEqual(tuple(example.number for example in suite), tuple(range(1, 11)))
        self.assertEqual(sum(example.implemented for example in suite), 6)

    def test_implemented_reference_examples_run(self) -> None:
        manyfold = load_manyfold_package()
        for example in manyfold.implemented_reference_examples():
            with self.subTest(example=example.title):
                self.assertIsNotNone(example.runner)
                example.runner()


if __name__ == "__main__":
    unittest.main()
