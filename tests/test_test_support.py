from __future__ import annotations

import os
import unittest
from unittest import mock

from tests import test_support


class TestSupportTests(unittest.TestCase):
    def test_subprocess_pythonpath_prepends_repo_python_root(self) -> None:
        self.assertEqual(
            test_support._pythonpath_with_repo_python_first(None),
            str(test_support.PYTHON_ROOT),
        )

    def test_subprocess_pythonpath_removes_duplicate_repo_python_root(self) -> None:
        python_root = str(test_support.PYTHON_ROOT)
        current_pythonpath = os.pathsep.join(
            ("/tmp/first", python_root, "/tmp/second", python_root)
        )

        pythonpath = test_support._pythonpath_with_repo_python_first(current_pythonpath)

        self.assertEqual(
            pythonpath,
            os.pathsep.join((python_root, "/tmp/first", "/tmp/second")),
        )

    def test_subprocess_pythonpath_deduplicates_inherited_paths(self) -> None:
        python_root = str(test_support.PYTHON_ROOT)
        current_pythonpath = os.pathsep.join(
            ("/tmp/first", "", "/tmp/second", "/tmp/first")
        )

        pythonpath = test_support._pythonpath_with_repo_python_first(current_pythonpath)

        self.assertEqual(
            pythonpath,
            os.pathsep.join((python_root, "/tmp/first", "/tmp/second")),
        )

    def test_subprocess_pythonpath_removes_relative_repo_python_root(self) -> None:
        python_root = str(test_support.PYTHON_ROOT)
        relative_python_root = os.path.relpath(
            test_support.PYTHON_ROOT,
            start=test_support.REPO_ROOT,
        )
        current_pythonpath = os.pathsep.join((relative_python_root, "/tmp/project"))

        pythonpath = test_support._pythonpath_with_repo_python_first(current_pythonpath)

        self.assertEqual(pythonpath, os.pathsep.join((python_root, "/tmp/project")))

    def test_subprocess_test_env_sets_stable_pythonpath_once(self) -> None:
        python_root = str(test_support.PYTHON_ROOT)
        current_pythonpath = os.pathsep.join(
            (python_root, "/tmp/project", "/tmp/project")
        )
        with mock.patch.dict(
            os.environ,
            {"PYTHONPATH": current_pythonpath},
            clear=False,
        ):
            env = test_support.subprocess_test_env()

        self.assertEqual(env["PYTHONPATH"], os.pathsep.join((python_root, "/tmp/project")))
        self.assertEqual(env["PYTHONPATH"].split(os.pathsep).count(python_root), 1)
        self.assertEqual(env["PYTHONPATH"].split(os.pathsep).count("/tmp/project"), 1)

    def test_module_available_uses_import_specs_for_installed_modules(self) -> None:
        with mock.patch("importlib.util.find_spec", return_value=object()) as find_spec:
            self.assertTrue(test_support._module_available("reactivex"))

        find_spec.assert_called_once_with("reactivex")

    def test_module_available_falls_back_to_loaded_modules_for_specless_stubs(
        self,
    ) -> None:
        with (
            mock.patch("importlib.util.find_spec", side_effect=ValueError),
            mock.patch.dict("sys.modules", {"reactivex": object()}),
        ):
            self.assertTrue(test_support._module_available("reactivex"))

    def test_reactivex_availability_checks_required_facade_modules(self) -> None:
        checked: list[str] = []

        def available(module_name: str) -> bool:
            checked.append(module_name)
            return True

        with mock.patch.object(test_support, "_module_available", side_effect=available):
            self.assertTrue(test_support._reactivex_available())

        self.assertEqual(
            checked,
            [
                "reactivex",
                "reactivex.operators",
                "reactivex.subject",
            ],
        )


if __name__ == "__main__":
    unittest.main()
