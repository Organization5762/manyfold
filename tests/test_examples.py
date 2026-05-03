from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from examples import (
    ARCHIVED_EXAMPLE_ENTRIES,
    ARCHIVED_EXAMPLE_MODULES,
    EXAMPLE_CATALOG,
    EXAMPLE_CATALOG_BY_MODULE,
    README_EXAMPLE_ENTRIES,
    README_EXAMPLE_MODULES,
    README_FEATURED_EXAMPLES_END,
    README_FEATURED_EXAMPLES_START,
    REFERENCE_EXAMPLE_ENTRIES,
    REFERENCE_EXAMPLE_ENTRY_BY_NUMBER,
    REFERENCE_EXAMPLE_GAPS,
    REFERENCE_EXAMPLE_IMPLEMENTED_COUNT,
    REFERENCE_EXAMPLE_NUMBERS,
    REFERENCE_EXAMPLE_PROGRESS_DETAIL,
    SUPPORTED_EXAMPLE_ENTRIES,
    SUPPORTED_EXAMPLE_MODULES,
    ExampleCatalogEntry,
    ReferenceExampleGap,
    catalog_entry,
    reference_example_metadata,
)
from examples._catalog import (
    _discover_manifestable_modules,
    render_readme_featured_examples,
    sync_readme_featured_examples,
)
from examples._exports import CATALOG_EXPORTS
from examples._shared import example_route, int_schema, sibling_route
from tests.test_support import (
    load_example_module,
    load_manyfold_package,
    subprocess_test_env,
)


class ExampleTests(unittest.TestCase):
    def test_repo_path_helpers_install_python_and_repo_roots_once(self) -> None:
        module_path = Path(__file__).resolve().parents[1] / "python" / "manyfold" / "_repo_paths.py"
        spec = importlib.util.spec_from_file_location("manyfold_repo_paths_test", module_path)
        module = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        python_dir = str(module.PYTHON_DIR)
        repo_root = str(module.REPO_ROOT)
        original_sys_path = list(sys.path)
        try:
            sys.path = [path for path in sys.path if path not in {python_dir, repo_root}]

            module.ensure_repo_import_paths()
            module.ensure_repo_import_paths()

            self.assertEqual(sys.path[0], repo_root)
            self.assertEqual(sys.path[1], python_dir)
            self.assertEqual(sum(path == repo_root for path in sys.path), 1)
            self.assertEqual(sum(path == python_dir for path in sys.path), 1)
        finally:
            sys.path = original_sys_path

    def test_readme_featured_examples_block_matches_catalog_renderer(self) -> None:
        readme_path = Path(__file__).resolve().parents[1] / "README.md"
        readme = readme_path.read_text(encoding="utf-8")
        start = readme.index(README_FEATURED_EXAMPLES_START)
        end = readme.index(README_FEATURED_EXAMPLES_END)
        block = readme[start + len(README_FEATURED_EXAMPLES_START) : end].strip()

        self.assertEqual(block, render_readme_featured_examples())
        self.assertEqual(sync_readme_featured_examples(readme), readme)

    def test_readme_example_references_match_supported_manifest(self) -> None:
        readme_path = Path(__file__).resolve().parents[1] / "README.md"
        readme = readme_path.read_text(encoding="utf-8")

        referenced_modules = tuple(
            match.group(1)
            for match in re.finditer(
                r"\[examples/([a-z0-9][a-z0-9_]*)\.py\]\(examples/\1\.py\)",
                readme,
            )
        )

        self.assertEqual(
            referenced_modules,
            README_EXAMPLE_MODULES,
        )
        self.assertTrue(set(referenced_modules).issubset(SUPPORTED_EXAMPLE_MODULES))
        self.assertIn("[examples/archived/](examples/archived/)", readme)
        self.assertIn("shared example catalog", readme)

    def test_examples_package_re_exports_catalog_symbols_and_types(self) -> None:
        examples_package = __import__("examples")

        self.assertEqual(
            tuple(examples_package.__all__),
            (
                "ARCHIVED_EXAMPLE_ENTRIES",
                "ARCHIVED_EXAMPLE_MODULES",
                "EXAMPLE_CATALOG",
                "EXAMPLE_CATALOG_BY_MODULE",
                "ExampleCatalogEntry",
                "README_EXAMPLE_ENTRIES",
                "README_FEATURED_EXAMPLES_END",
                "README_FEATURED_EXAMPLES_START",
                "README_EXAMPLE_MODULES",
                "REFERENCE_EXAMPLE_ENTRIES",
                "REFERENCE_EXAMPLE_ENTRY_BY_NUMBER",
                "REFERENCE_EXAMPLE_GAPS",
                "REFERENCE_EXAMPLE_GAP_BY_NUMBER",
                "REFERENCE_EXAMPLE_IMPLEMENTED_COUNT",
                "REFERENCE_EXAMPLE_NUMBERS",
                "REFERENCE_EXAMPLE_PROGRESS_DETAIL",
                "ReferenceExampleGap",
                "SUPPORTED_EXAMPLE_ENTRIES",
                "SUPPORTED_EXAMPLE_MODULES",
                "catalog_entry",
                "render_readme_featured_examples",
                "reference_example_metadata",
                "sync_readme_featured_examples",
            ),
        )
        self.assertIs(examples_package.EXAMPLE_CATALOG, EXAMPLE_CATALOG)
        self.assertIs(examples_package.ExampleCatalogEntry, ExampleCatalogEntry)
        self.assertIs(examples_package.ReferenceExampleGap, ReferenceExampleGap)

    def test_catalog_exports_manifest_matches_examples_package_and_catalog_module(
        self,
    ) -> None:
        import examples._catalog as catalog_module

        examples_package = __import__("examples")

        self.assertEqual(tuple(CATALOG_EXPORTS), tuple(examples_package.__all__))
        self.assertEqual(tuple(CATALOG_EXPORTS), tuple(catalog_module.__all__))

    def test_examples_package_lazy_exports_cache_dir_and_errors(self) -> None:
        examples_package = __import__("examples")

        self.assertIn("catalog_entry", dir(examples_package))
        self.assertIn("sync_readme_featured_examples", dir(examples_package))

        examples_package.__dict__.pop("catalog_entry", None)
        loaded = getattr(examples_package, "catalog_entry")
        current_catalog_entry = __import__(
            "examples._catalog", fromlist=["catalog_entry"]
        ).catalog_entry

        self.assertEqual(
            loaded("brightness_control"),
            catalog_entry("brightness_control"),
        )
        self.assertIs(examples_package.__dict__["catalog_entry"], loaded)
        self.assertEqual(
            current_catalog_entry("brightness_control").module_name,
            loaded("brightness_control").module_name,
        )

        with self.assertRaisesRegex(
            AttributeError,
            r"module 'examples' has no attribute 'missing_symbol'",
        ):
            getattr(examples_package, "missing_symbol")

    def test_examples_package_dir_exposes_all_lazy_exports(self) -> None:
        examples_package = __import__("examples")

        exported_names = set(dir(examples_package))

        self.assertTrue(set(examples_package.__all__).issubset(exported_names))

    def test_catalog_cli_check_readme_runs_without_runpy_warning(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "examples.catalog",
                "--check-readme",
            ],
            check=False,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[1],
            env=subprocess_test_env(),
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        if sys.version_info < (3, 14):
            self.assertNotIn("RuntimeWarning", result.stderr)

    def test_catalog_cli_check_manifest_runs_without_runpy_warning(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "examples.catalog",
                "--check-manifest",
            ],
            check=False,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[1],
            env=subprocess_test_env(),
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        if sys.version_info < (3, 14):
            self.assertNotIn("RuntimeWarning", result.stderr)

    def test_catalog_cli_check_runs_without_runpy_warning(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "examples.catalog",
                "--check",
            ],
            check=False,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[1],
            env=subprocess_test_env(),
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        if sys.version_info < (3, 14):
            self.assertNotIn("RuntimeWarning", result.stderr)

    def test_catalog_cli_list_modes_print_expected_manifests(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]

        for mode, expected in (
            ("supported", SUPPORTED_EXAMPLE_MODULES),
            ("archived", ARCHIVED_EXAMPLE_MODULES),
            ("reference", tuple(entry.module_name for entry in REFERENCE_EXAMPLE_ENTRIES)),
            ("readme", README_EXAMPLE_MODULES),
        ):
            with self.subTest(mode=mode):
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "examples.catalog",
                        "--list",
                        mode,
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    cwd=repo_root,
                    env=subprocess_test_env(),
                )

                self.assertEqual(result.returncode, 0, msg=result.stderr)
                self.assertEqual(tuple(result.stdout.splitlines()), expected)
                self.assertEqual(result.stderr, "")

    def test_manyfold_example_catalog_script_check_runs(self) -> None:
        result = subprocess.run(
            [
                "uv",
                "run",
                "manyfold-example-catalog",
                "--check",
            ],
            check=False,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[1],
            env=subprocess_test_env(),
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_manyfold_example_catalog_script_list_reference_runs(self) -> None:
        result = subprocess.run(
            [
                "uv",
                "run",
                "manyfold-example-catalog",
                "--list",
                "reference",
            ],
            check=False,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[1],
            env=subprocess_test_env(),
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(
            tuple(result.stdout.splitlines()),
            tuple(entry.module_name for entry in REFERENCE_EXAMPLE_ENTRIES),
        )

    def test_manyfold_example_catalog_module_check_runs(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "manyfold_example_catalog",
                "--check",
            ],
            check=False,
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[1],
            env=subprocess_test_env(),
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_repo_root_manyfold_example_catalog_wrapper_delegates_main(self) -> None:
        module_path = Path(__file__).resolve().parents[1] / "manyfold_example_catalog.py"
        spec = importlib.util.spec_from_file_location(
            "repo_root_manyfold_example_catalog_test",
            module_path,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        sys.modules[spec.name] = module

        fake_impl = type(sys)("_manyfold_example_catalog_impl")
        recorded: list[list[str] | None] = []

        def fake_main(passed_argv: list[str] | None = None) -> int:
            recorded.append(passed_argv)
            return 29

        fake_impl.main = fake_main

        original_spec_from_file_location = importlib.util.spec_from_file_location

        def fake_spec_from_file_location(name: str, location, *args, **kwargs):
            if name == "_manyfold_example_catalog_impl":
                fake_spec = mock.Mock()
                fake_spec.loader = mock.Mock()
                fake_spec.loader.exec_module.side_effect = lambda target: target.__dict__.update(
                    fake_impl.__dict__
                )
                return fake_spec
            return original_spec_from_file_location(name, location, *args, **kwargs)

        with mock.patch(
            "importlib.util.spec_from_file_location",
            side_effect=fake_spec_from_file_location,
        ):
            spec.loader.exec_module(module)

        argv = ["--check"]
        self.assertEqual(module.main(argv), 29)
        self.assertEqual(recorded, [argv])
        self.assertEqual(module.__all__, ["main"])

    def test_repo_root_manyfold_example_catalog_wrapper_uses_python_impl_path(
        self,
    ) -> None:
        module_path = Path(__file__).resolve().parents[1] / "manyfold_example_catalog.py"
        spec = importlib.util.spec_from_file_location(
            "repo_root_manyfold_example_catalog_impl_path_test",
            module_path,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        sys.modules[spec.name] = module

        captured: list[tuple[str, object]] = []
        original_spec_from_file_location = importlib.util.spec_from_file_location

        def fake_spec_from_file_location(name: str, location, *args, **kwargs):
            captured.append((name, location))
            if name == "_manyfold_example_catalog_impl":
                fake_spec = mock.Mock()
                fake_spec.loader = mock.Mock()
                fake_spec.loader.exec_module.side_effect = (
                    lambda target: target.__dict__.update({"main": lambda argv=None: 0})
                )
                return fake_spec
            return original_spec_from_file_location(name, location, *args, **kwargs)

        with mock.patch(
            "importlib.util.spec_from_file_location",
            side_effect=fake_spec_from_file_location,
        ):
            spec.loader.exec_module(module)

        self.assertIn(
            ("_manyfold_example_catalog_impl", module.PYTHON_DIR / "manyfold_example_catalog.py"),
            captured,
        )
        self.assertEqual(module.REPO_ROOT, module.MODULE_PATH.parent)

    def test_manyfold_example_catalog_wrapper_delegates_argv_and_adds_repo_root(
        self,
    ) -> None:
        module_path = (
            Path(__file__).resolve().parents[1] / "python" / "manyfold_example_catalog.py"
        )
        spec = importlib.util.spec_from_file_location(
            "manyfold_example_catalog_test", module_path
        )
        module = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        repo_root = str(module.REPO_ROOT)
        python_dir = str(module.PYTHON_DIR)
        argv = ["--check-readme"]

        fake_catalog = type(sys)("examples.catalog")
        recorded: list[list[str] | None] = []

        def fake_main(passed_argv: list[str] | None = None) -> int:
            recorded.append(passed_argv)
            return 17

        fake_catalog.main = fake_main

        original_sys_path = list(sys.path)
        try:
            sys.path = [
                path for path in sys.path if path not in {repo_root, python_dir}
            ]
            with mock.patch.dict(sys.modules, {"examples.catalog": fake_catalog}):
                self.assertEqual(module.main(argv), 17)

                self.assertEqual(recorded, [argv])
                self.assertEqual(sys.path[:2], [repo_root, python_dir])
        finally:
            sys.path = original_sys_path

    def test_manyfold_example_catalog_wrapper_preserves_none_argv(self) -> None:
        module_path = (
            Path(__file__).resolve().parents[1] / "python" / "manyfold_example_catalog.py"
        )
        spec = importlib.util.spec_from_file_location(
            "manyfold_example_catalog_none_argv_test",
            module_path,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        sys.modules[spec.name] = module

        fake_manyfold = type(sys)("manyfold")
        fake_manyfold.__path__ = []  # type: ignore[attr-defined]
        fake_repo_paths = type(sys)("manyfold._repo_paths")
        fake_repo_paths.ensure_repo_import_paths = lambda: None

        with mock.patch.dict(
            sys.modules,
            {
                "manyfold": fake_manyfold,
                "manyfold._repo_paths": fake_repo_paths,
            },
        ):
            spec.loader.exec_module(module)

        fake_catalog = type(sys)("examples.catalog")
        recorded: list[list[str] | None] = []

        def fake_main(passed_argv: list[str] | None = None) -> int:
            recorded.append(passed_argv)
            return 23

        fake_catalog.main = fake_main

        with mock.patch.dict(sys.modules, {"examples.catalog": fake_catalog}):
            self.assertEqual(module.main(), 23)

        self.assertEqual(recorded, [None])

    def test_manyfold_example_catalog_file_import_uses_repo_path_fallback(
        self,
    ) -> None:
        module_path = (
            Path(__file__).resolve().parents[1] / "python" / "manyfold_example_catalog.py"
        )
        spec = importlib.util.spec_from_file_location(
            "manyfold_example_catalog_file_test",
            module_path,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        sys.modules[spec.name] = module

        calls: list[str] = []
        fake_manyfold = type(sys)("manyfold")
        fake_manyfold.__path__ = []  # type: ignore[attr-defined]
        fake_repo_paths = type(sys)("manyfold._repo_paths")
        fake_repo_paths.ensure_repo_import_paths = lambda: calls.append("called")

        with mock.patch.dict(
            sys.modules,
            {
                "manyfold": fake_manyfold,
                "manyfold._repo_paths": fake_repo_paths,
            },
        ):
            spec.loader.exec_module(module)

        self.assertEqual(calls, ["called"])
        self.assertEqual(module.REPO_ROOT, module.PYTHON_DIR.parent)

    def test_repo_path_helpers_prepend_expected_import_roots_once(self) -> None:
        module_path = (
            Path(__file__).resolve().parents[1]
            / "python"
            / "manyfold"
            / "_repo_paths.py"
        )
        spec = importlib.util.spec_from_file_location("manyfold_repo_paths_test", module_path)
        module = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        repo_root = str(module.REPO_ROOT)
        python_dir = str(module.PYTHON_DIR)
        original_sys_path = list(sys.path)
        try:
            sys.path = [
                path for path in original_sys_path if path not in {repo_root, python_dir}
            ]

            module.ensure_package_dir_on_path()
            module.ensure_package_dir_on_path()
            self.assertEqual(sys.path[0], python_dir)
            self.assertEqual(sys.path.count(python_dir), 1)

            module.ensure_repo_root_on_path()
            module.ensure_repo_root_on_path()
            self.assertEqual(sys.path[:2], [repo_root, python_dir])
            self.assertEqual(sys.path.count(repo_root), 1)

            sys.path = [
                path for path in original_sys_path if path not in {repo_root, python_dir}
            ]
            module.ensure_repo_import_paths()
            module.ensure_repo_import_paths()
            self.assertEqual(sys.path[:2], [repo_root, python_dir])
            self.assertEqual(sys.path.count(repo_root), 1)
            self.assertEqual(sys.path.count(python_dir), 1)
        finally:
            sys.path = original_sys_path

    def test_repo_path_helpers_load_module_from_path_executes_module(self) -> None:
        module_path = (
            Path(__file__).resolve().parents[1]
            / "python"
            / "manyfold"
            / "_repo_paths.py"
        )
        spec = importlib.util.spec_from_file_location("manyfold_repo_paths_test", module_path)
        module = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        loaded = module.load_module_from_path(
            "examples_catalog_exports_test",
            Path(__file__).resolve().parents[1] / "examples" / "_exports.py",
        )

        self.assertIs(sys.modules["examples_catalog_exports_test"], loaded)
        self.assertEqual(loaded.CATALOG_EXPORTS[0], "ARCHIVED_EXAMPLE_ENTRIES")
        self.assertIn("sync_readme_featured_examples", loaded.CATALOG_EXPORTS)

    def test_repo_path_helpers_remove_failed_module_loads(self) -> None:
        module_path = (
            Path(__file__).resolve().parents[1]
            / "python"
            / "manyfold"
            / "_repo_paths.py"
        )
        spec = importlib.util.spec_from_file_location("manyfold_repo_paths_test", module_path)
        module = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        with tempfile.TemporaryDirectory() as temp_dir:
            broken_module = Path(temp_dir) / "broken_module.py"
            broken_module.write_text("raise RuntimeError('boom')\n", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "boom"):
                module.load_module_from_path("manyfold_broken_load_test", broken_module)

        self.assertNotIn("manyfold_broken_load_test", sys.modules)

    def test_catalog_main_check_and_write_modes_follow_generated_output(self) -> None:
        import examples._catalog as catalog_module

        generated = render_readme_featured_examples()
        original_path = catalog_module._README_PATH
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                readme_path = Path(temp_dir) / "README.md"
                readme_path.write_text(
                    "\n".join(
                        (
                            "# Scratch README",
                            README_FEATURED_EXAMPLES_START,
                            "stale generated block",
                            README_FEATURED_EXAMPLES_END,
                            "tail",
                        )
                    ),
                    encoding="utf-8",
                )
                catalog_module._README_PATH = readme_path

                self.assertEqual(catalog_module.main(["--check-manifest"]), 0)
                self.assertEqual(catalog_module.main(["--check"]), 1)
                self.assertEqual(catalog_module.main(["--check-readme"]), 1)
                self.assertEqual(catalog_module.main([]), 0)
                self.assertEqual(
                    readme_path.read_text(encoding="utf-8"),
                    "\n".join(
                        (
                            "# Scratch README",
                            README_FEATURED_EXAMPLES_START,
                            generated,
                            README_FEATURED_EXAMPLES_END,
                            "tail",
                        )
                    ),
                )
                self.assertEqual(catalog_module.main(["--check"]), 0)
                self.assertEqual(catalog_module.main(["--check-readme"]), 0)
        finally:
            catalog_module._README_PATH = original_path

    def test_readme_example_manifest_uses_supported_non_archived_entries(self) -> None:
        expected_entries = tuple(
            sorted(
                (
                    entry
                    for entry in SUPPORTED_EXAMPLE_ENTRIES
                    if entry.readme_order is not None
                ),
                key=lambda entry: entry.readme_order if entry.readme_order is not None else -1,
            )
        )
        self.assertEqual(README_EXAMPLE_ENTRIES, expected_entries)
        self.assertTrue(README_EXAMPLE_ENTRIES)
        self.assertTrue(
            all(not entry.archived for entry in README_EXAMPLE_ENTRIES)
        )

    def test_catalog_lookup_helpers_return_shared_metadata_objects(self) -> None:
        brightness = catalog_entry("brightness_control")
        raft_demo = reference_example_metadata(9)
        lidar = reference_example_metadata(3)

        self.assertIs(brightness, EXAMPLE_CATALOG_BY_MODULE["brightness_control"])
        self.assertIs(lidar, REFERENCE_EXAMPLE_ENTRY_BY_NUMBER[3])
        self.assertIs(raft_demo, REFERENCE_EXAMPLE_ENTRY_BY_NUMBER[9])
        self.assertEqual(brightness.reference_number, 5)
        self.assertEqual(raft_demo.reference_title, "Raft demo")

    def test_catalog_entries_preserve_recent_reference_metadata(self) -> None:
        cross_partition = catalog_entry("cross_partition_join")
        entropy = catalog_entry("ephemeral_entropy_stream")
        rolling = catalog_entry("rolling_window_aggregate")

        self.assertEqual(cross_partition.readme_order, 6)
        self.assertEqual(cross_partition.reference_number, 7)
        self.assertEqual(cross_partition.reference_title, "Cross-partition join")
        self.assertEqual(entropy.readme_order, 8)
        self.assertEqual(entropy.reference_number, 10)
        self.assertEqual(entropy.reference_title, "Ephemeral entropy stream")
        self.assertEqual(rolling.readme_order, 5)
        self.assertFalse(rolling.is_reference_example)

    def test_catalog_entries_expose_shared_import_and_file_paths(self) -> None:
        brightness = catalog_entry("brightness_control")
        archived = catalog_entry("archived.windowed_join")

        self.assertEqual(brightness.import_path, "examples.brightness_control")
        self.assertTrue(brightness.file_path.samefile(Path("examples/brightness_control.py")))
        self.assertEqual(archived.import_path, "examples.archived.windowed_join")
        self.assertTrue(
            archived.file_path.samefile(Path("examples/archived/windowed_join.py"))
        )

    def test_catalog_lookup_helpers_raise_clear_errors_for_unknown_values(self) -> None:
        with self.assertRaisesRegex(KeyError, "unknown example module"):
            catalog_entry("missing_example")
        with self.assertRaisesRegex(KeyError, "unknown RFC reference example"):
            reference_example_metadata(99)

    def test_internal_entry_manifests_partition_catalog(self) -> None:
        self.assertEqual(
            SUPPORTED_EXAMPLE_ENTRIES + ARCHIVED_EXAMPLE_ENTRIES, EXAMPLE_CATALOG
        )

    def test_example_catalog_drives_supported_and_archived_manifests(self) -> None:
        self.assertEqual(
            SUPPORTED_EXAMPLE_MODULES + ARCHIVED_EXAMPLE_MODULES,
            tuple(entry.module_name for entry in EXAMPLE_CATALOG),
        )
        self.assertTrue(
            set(SUPPORTED_EXAMPLE_MODULES).isdisjoint(ARCHIVED_EXAMPLE_MODULES)
        )
        self.assertEqual(
            SUPPORTED_EXAMPLE_MODULES,
            tuple(entry.module_name for entry in SUPPORTED_EXAMPLE_ENTRIES),
        )
        self.assertEqual(
            ARCHIVED_EXAMPLE_MODULES,
            tuple(entry.module_name for entry in ARCHIVED_EXAMPLE_ENTRIES),
        )

    def test_example_module_manifest_distinguishes_supported_and_archived(self) -> None:
        self.assertIn("imu_fusion_join", SUPPORTED_EXAMPLE_MODULES)
        self.assertIn("rate_matched_sensor", SUPPORTED_EXAMPLE_MODULES)
        self.assertIn("rolling_window_aggregate", SUPPORTED_EXAMPLE_MODULES)
        self.assertIn("cross_partition_join", SUPPORTED_EXAMPLE_MODULES)
        self.assertIn("raft_demo", SUPPORTED_EXAMPLE_MODULES)
        self.assertIn("ephemeral_entropy_stream", SUPPORTED_EXAMPLE_MODULES)
        self.assertEqual(ARCHIVED_EXAMPLE_MODULES, ("archived.windowed_join",))

    def test_supported_example_manifest_matches_filesystem_modules(self) -> None:
        examples_dir = Path(__file__).resolve().parents[1] / "examples"
        discovered = sorted(
            path.stem
            for path in examples_dir.glob("*.py")
            if path.stem not in {"__init__", "_catalog", "_exports", "_shared", "catalog"}
        )

        self.assertEqual(tuple(discovered), tuple(sorted(SUPPORTED_EXAMPLE_MODULES)))

    def test_archived_example_manifest_matches_filesystem_modules(self) -> None:
        archived_dir = Path(__file__).resolve().parents[1] / "examples" / "archived"
        discovered = sorted(
            path.relative_to(archived_dir).with_suffix("").as_posix()
            for path in archived_dir.glob("*.py")
            if path.stem != "__init__"
        )

        self.assertEqual(
            tuple(f"archived.{name}" for name in discovered),
            tuple(sorted(ARCHIVED_EXAMPLE_MODULES)),
        )

    def test_catalog_discovery_matches_supported_and_archived_manifests(self) -> None:
        self.assertCountEqual(
            _discover_manifestable_modules(),
            (*SUPPORTED_EXAMPLE_MODULES, *ARCHIVED_EXAMPLE_MODULES),
        )

    def test_catalog_discovery_orders_supported_modules_before_archived_modules(
        self,
    ) -> None:
        discovered = _discover_manifestable_modules()
        archived_start = next(
            index
            for index, module_name in enumerate(discovered)
            if module_name.startswith("archived.")
        )

        self.assertEqual(
            discovered[:archived_start],
            tuple(sorted(SUPPORTED_EXAMPLE_MODULES)),
        )
        self.assertEqual(
            discovered[archived_start:],
            tuple(sorted(ARCHIVED_EXAMPLE_MODULES)),
        )

    def test_readme_featured_examples_renderer_uses_manifest_order_only(self) -> None:
        rendered = render_readme_featured_examples().splitlines()

        bullet_lines = tuple(
            line.removeprefix("- [examples/").split(".py]", 1)[0].replace("/", ".")
            for line in rendered
            if line.startswith("- [examples/")
        )

        self.assertEqual(bullet_lines, README_EXAMPLE_MODULES)
        self.assertNotIn("archived.windowed_join", bullet_lines)
        self.assertNotIn("observe_publish", bullet_lines)

    def test_catalog_validation_rejects_partial_reference_metadata(self) -> None:
        import examples._catalog as catalog_module

        original_catalog = catalog_module.EXAMPLE_CATALOG
        try:
            catalog_module.EXAMPLE_CATALOG = (
                ExampleCatalogEntry(
                    "simple_latest",
                    "Broken partial reference metadata.",
                    reference_number=99,
                ),
            )

            with self.assertRaisesRegex(
                ValueError,
                "must define reference number and title together",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module.EXAMPLE_CATALOG = original_catalog

    def test_catalog_validation_rejects_duplicate_module_names(self) -> None:
        import examples._catalog as catalog_module

        original_catalog = catalog_module.EXAMPLE_CATALOG
        duplicate = EXAMPLE_CATALOG[0]
        try:
            catalog_module.EXAMPLE_CATALOG = (*EXAMPLE_CATALOG, duplicate)

            with self.assertRaisesRegex(
                ValueError,
                "example catalog contains duplicate module names",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module.EXAMPLE_CATALOG = original_catalog

    def test_catalog_validation_rejects_non_contiguous_reference_numbers(self) -> None:
        import examples._catalog as catalog_module

        original_catalog = catalog_module.EXAMPLE_CATALOG
        try:
            catalog_module.EXAMPLE_CATALOG = tuple(
                replace(entry, reference_number=11)
                if entry.module_name == "ephemeral_entropy_stream"
                else entry
                for entry in original_catalog
            )

            with self.assertRaisesRegex(
                ValueError,
                "reference example numbers must be contiguous starting at 1",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module.EXAMPLE_CATALOG = original_catalog

    def test_catalog_validation_rejects_archived_reference_examples(self) -> None:
        import examples._catalog as catalog_module

        original_catalog = catalog_module.EXAMPLE_CATALOG
        try:
            catalog_module.EXAMPLE_CATALOG = (
                ExampleCatalogEntry(
                    "archived.windowed_join",
                    "Broken archived reference example.",
                    archived=True,
                    reference_number=11,
                    reference_title="Broken archived example",
                ),
            )

            with self.assertRaisesRegex(
                ValueError,
                "cannot also be a reference example",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module.EXAMPLE_CATALOG = original_catalog

    def test_catalog_validation_rejects_archived_readme_featured_examples(
        self,
    ) -> None:
        import examples._catalog as catalog_module

        original_catalog = catalog_module.EXAMPLE_CATALOG
        try:
            catalog_module.EXAMPLE_CATALOG = (
                ExampleCatalogEntry(
                    "archived.windowed_join",
                    "Broken archived README example.",
                    archived=True,
                    readme_order=1,
                ),
            )

            with self.assertRaisesRegex(
                ValueError,
                "cannot be featured in the README",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module.EXAMPLE_CATALOG = original_catalog

    def test_catalog_validation_rejects_duplicate_readme_orders(self) -> None:
        import examples._catalog as catalog_module

        original_catalog = catalog_module.EXAMPLE_CATALOG
        try:
            catalog_module.EXAMPLE_CATALOG = tuple(
                replace(entry, readme_order=1)
                if entry.module_name == "rate_matched_sensor"
                else entry
                for entry in original_catalog
            )

            with self.assertRaisesRegex(
                ValueError,
                "duplicate readme_order values",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module.EXAMPLE_CATALOG = original_catalog

    def test_catalog_validation_rejects_non_positive_readme_orders(self) -> None:
        import examples._catalog as catalog_module

        original_catalog = catalog_module.EXAMPLE_CATALOG
        try:
            catalog_module.EXAMPLE_CATALOG = tuple(
                replace(entry, readme_order=0)
                if entry.module_name == "simple_latest"
                else entry
                for entry in original_catalog
            )

            with self.assertRaisesRegex(
                ValueError,
                "must use positive readme_order values",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module.EXAMPLE_CATALOG = original_catalog

    def test_catalog_validation_rejects_non_contiguous_readme_orders(self) -> None:
        import examples._catalog as catalog_module

        original_catalog = catalog_module.EXAMPLE_CATALOG
        try:
            catalog_module.EXAMPLE_CATALOG = tuple(
                replace(entry, readme_order=10)
                if entry.module_name == "ephemeral_entropy_stream"
                else entry
                for entry in original_catalog
            )

            with self.assertRaisesRegex(
                ValueError,
                "must use contiguous readme_order values",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module.EXAMPLE_CATALOG = original_catalog

    def test_catalog_validation_rejects_reference_gap_overlap(self) -> None:
        import examples._catalog as catalog_module

        original_gaps = catalog_module.REFERENCE_EXAMPLE_GAPS
        try:
            catalog_module.REFERENCE_EXAMPLE_GAPS = (
                ReferenceExampleGap(
                    reference_number=1,
                    reference_title="Duplicate gap",
                    summary="Broken overlap with implemented example.",
                ),
            )

            with self.assertRaisesRegex(
                ValueError,
                "reference example numbers overlap with gaps: 1",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module.REFERENCE_EXAMPLE_GAPS = original_gaps

    def test_catalog_validation_rejects_manifest_drift_from_discovered_modules(self) -> None:
        import examples._catalog as catalog_module

        original_discover = catalog_module._discover_manifestable_modules
        try:
            catalog_module._discover_manifestable_modules = lambda: (
                *SUPPORTED_EXAMPLE_MODULES,
                *ARCHIVED_EXAMPLE_MODULES,
                "future_example",
            )

            with self.assertRaisesRegex(
                ValueError,
                "example files missing from catalog: future_example",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module._discover_manifestable_modules = original_discover

    def test_catalog_validation_rejects_manifest_entries_missing_from_discovery(
        self,
    ) -> None:
        import examples._catalog as catalog_module

        original_discover = catalog_module._discover_manifestable_modules
        try:
            catalog_module._discover_manifestable_modules = lambda: tuple(
                module_name
                for module_name in (*SUPPORTED_EXAMPLE_MODULES, *ARCHIVED_EXAMPLE_MODULES)
                if module_name != "simple_latest"
            )

            with self.assertRaisesRegex(
                ValueError,
                "example catalog entries missing files: simple_latest",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module._discover_manifestable_modules = original_discover

    def test_catalog_validation_rejects_duplicate_reference_gap_numbers(self) -> None:
        import examples._catalog as catalog_module

        original_gaps = catalog_module.REFERENCE_EXAMPLE_GAPS
        try:
            catalog_module.REFERENCE_EXAMPLE_GAPS = (
                ReferenceExampleGap(
                    reference_number=9,
                    reference_title="Raft demo",
                    summary="Existing gap.",
                ),
                ReferenceExampleGap(
                    reference_number=9,
                    reference_title="Duplicate Raft demo",
                    summary="Duplicate gap entry.",
                ),
            )

            with self.assertRaisesRegex(
                ValueError,
                "reference example gaps contain duplicate RFC example numbers",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module.REFERENCE_EXAMPLE_GAPS = original_gaps

    def test_catalog_validation_rejects_gap_overlap_with_implemented_reference_example(
        self,
    ) -> None:
        import examples._catalog as catalog_module

        original_gaps = catalog_module.REFERENCE_EXAMPLE_GAPS
        try:
            catalog_module.REFERENCE_EXAMPLE_GAPS = (
                ReferenceExampleGap(
                    reference_number=3,
                    reference_title="Lazy LiDAR payload",
                    summary="Incorrectly duplicated as a gap.",
                ),
            )

            with self.assertRaisesRegex(
                ValueError,
                "reference example numbers overlap with gaps: 3",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module.REFERENCE_EXAMPLE_GAPS = original_gaps

    def test_catalog_validation_rejects_entry_without_backing_file(self) -> None:
        import examples._catalog as catalog_module

        original_catalog = catalog_module.EXAMPLE_CATALOG
        try:
            catalog_module.EXAMPLE_CATALOG = (
                *original_catalog,
                ExampleCatalogEntry(
                    "missing_example_file",
                    "Catalog entry without a matching example module.",
                ),
            )

            with self.assertRaisesRegex(
                ValueError,
                "does not map to an existing example file",
            ):
                catalog_module._validate_catalog()
        finally:
            catalog_module.EXAMPLE_CATALOG = original_catalog

    def test_reference_catalog_numbers_are_unique_and_complete(self) -> None:
        referenced_numbers = sorted(
            entry.reference_number
            for entry in EXAMPLE_CATALOG
            if entry.reference_number is not None
        )

        self.assertEqual(referenced_numbers, list(range(1, 11)))
        self.assertEqual(REFERENCE_EXAMPLE_NUMBERS, tuple(range(1, 11)))
        self.assertEqual(REFERENCE_EXAMPLE_IMPLEMENTED_COUNT, 10)
        self.assertEqual(REFERENCE_EXAMPLE_GAPS, ())
        self.assertEqual(
            REFERENCE_EXAMPLE_PROGRESS_DETAIL,
            "The repository now ships a named RFC reference example suite, with "
            "runnable coverage for all 10 examples.",
        )

    def test_reference_catalog_entries_keep_complete_reference_metadata(self) -> None:
        for entry in EXAMPLE_CATALOG:
            with self.subTest(module=entry.module_name):
                self.assertEqual(
                    entry.is_reference_example,
                    entry.reference_number is not None
                    and entry.reference_title is not None,
                )

    def test_example_catalog_keeps_module_names_unique(self) -> None:
        module_names = tuple(entry.module_name for entry in EXAMPLE_CATALOG)

        self.assertEqual(len(module_names), len(set(module_names)))

    def test_shared_example_route_helpers_preserve_and_override_typed_segments(
        self,
    ) -> None:
        manyfold = load_manyfold_package()
        base = example_route(
            plane=manyfold.Plane.Read,
            layer=manyfold.Layer.Logical,
            owner="sensor",
            family="temperature",
            stream="ambient",
            variant=manyfold.Variant.Meta,
            schema=int_schema("Temperature"),
        )

        sibling = sibling_route(
            base,
            plane=manyfold.Plane.State,
            layer=manyfold.Layer.Internal,
            owner="scheduler",
            stream="latest",
        )

        self.assertEqual(base.owner.value, "sensor")
        self.assertEqual(base.family.value, "temperature")
        self.assertEqual(base.stream.value, "ambient")
        self.assertEqual(base.schema.schema_id, "Temperature")
        self.assertEqual(sibling.plane, manyfold.Plane.State)
        self.assertEqual(sibling.layer, manyfold.Layer.Internal)
        self.assertEqual(sibling.owner.value, "scheduler")
        self.assertEqual(sibling.family.value, "temperature")
        self.assertEqual(sibling.stream.value, "latest")
        self.assertIs(sibling.schema, base.schema)

    def test_shared_int_schema_round_trips_ascii_values_and_version(self) -> None:
        schema = int_schema("Temperature", version=3)

        self.assertEqual(schema.schema_id, "Temperature")
        self.assertEqual(schema.version, 3)
        self.assertEqual(schema.encode(42), b"42")
        self.assertEqual(schema.decode(b"42"), 42)

    def test_sync_readme_featured_examples_rejects_missing_or_reversed_markers(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "README featured-example markers are missing or out of order",
        ):
            sync_readme_featured_examples("README without generated markers")

        with self.assertRaisesRegex(
            ValueError,
            "README featured-example markers are missing or out of order",
        ):
            sync_readme_featured_examples(
                f"{README_FEATURED_EXAMPLES_END}\n...\n{README_FEATURED_EXAMPLES_START}"
            )

    def test_sync_readme_featured_examples_rejects_duplicate_markers(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "README featured-example markers must appear exactly once",
        ):
            sync_readme_featured_examples(
                "\n".join(
                    (
                        README_FEATURED_EXAMPLES_START,
                        "first block",
                        README_FEATURED_EXAMPLES_END,
                        README_FEATURED_EXAMPLES_START,
                        "second block",
                        README_FEATURED_EXAMPLES_END,
                    )
                )
            )

        with self.assertRaisesRegex(
            ValueError,
            "README featured-example markers must appear exactly once",
        ):
            sync_readme_featured_examples(
                "\n".join(
                    (
                        README_FEATURED_EXAMPLES_START,
                        "generated block",
                        README_FEATURED_EXAMPLES_END,
                        README_FEATURED_EXAMPLES_END,
                    )
                )
            )

    def test_manifested_examples_export_run_example(self) -> None:
        for name in (*SUPPORTED_EXAMPLE_MODULES, *ARCHIVED_EXAMPLE_MODULES):
            with self.subTest(name=name):
                module = load_example_module(name)
                self.assertTrue(callable(getattr(module, "run_example", None)))

    def test_load_example_module_reloads_cleanly(self) -> None:
        load_example_module("cross_partition_join")
        setattr(sys.modules["examples.cross_partition_join"], "SENTINEL", object())

        load_example_module("cross_partition_join")

        self.assertNotIn("SENTINEL", vars(sys.modules["examples.cross_partition_join"]))

    def test_simple_latest_example(self) -> None:
        result = load_example_module("simple_latest").run_example()

        self.assertEqual(result["latest_payload"], b"72.9F")
        self.assertEqual(result["latest_seq"], 2)

    def test_average_temperature_example(self) -> None:
        result = load_example_module("average_temperature").run_example()

        self.assertEqual(result["samples"], (b"72.4F", b"72.9F", b"73.7F"))
        self.assertEqual(result["averages"], (b"72.4F", b"72.7F", b"73.0F"))
        self.assertEqual(result["latest_average"], b"73.0F")
        self.assertEqual(result["latest_seq"], 3)

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

    def test_imu_fusion_join_example(self) -> None:
        result = load_example_module("imu_fusion_join").run_example()

        self.assertEqual(
            result["fused_pairs"], ((100, 7), (101, 7), (100, 8), (101, 8))
        )
        self.assertEqual(result["latest_pose"], (101, 8))

    def test_lazy_lidar_payload_example(self) -> None:
        result = load_example_module("lazy_lidar_payload").run_example()

        self.assertEqual(result["selected_frame"], b"frame-2-points")
        self.assertEqual(result["metadata_count"], 2)
        self.assertEqual(result["matched_frames"], ("frame-2:open",))
        self.assertEqual(result["payload_open_requests"], 1)
        self.assertEqual(result["lazy_source_opens"], 1)
        self.assertEqual(result["unopened_lazy_payloads"], 1)
        self.assertEqual(result["profile_issues"], ())

    def test_archived_windowed_join_example(self) -> None:
        result = load_example_module("archived.windowed_join").run_example()

        self.assertEqual(result["rolling_windows"], ((1,), (1, 2)))
        self.assertEqual(result["joined_values"], (11, 12, 13))

    def test_closed_counter_loop_example(self) -> None:
        result = load_example_module("closed_counter_loop").run_example()

        self.assertEqual(result["desired_latest"], b"2")
        self.assertEqual(result["effective_latest"], b"2")
        self.assertEqual(result["ack_latest"], b"ok")
        self.assertEqual(result["coherence_taints"], ("COHERENCE_STABLE",))

    def test_brightness_control_example(self) -> None:
        result = load_example_module("brightness_control").run_example()

        self.assertEqual(result["pwm_latest"], b"\xff")
        self.assertEqual(result["pwm_seq"], 3)

    def test_mailbox_bridge_example(self) -> None:
        result = load_example_module("mailbox_bridge").run_example()

        self.assertEqual(result["capacity"], 4)
        self.assertEqual(result["available_credit"], 4)
        self.assertEqual(result["depth"], 0)
        self.assertEqual(result["dropped_messages"], 0)
        self.assertEqual(result["overflow_policy"], "drop_oldest")
        self.assertEqual(
            result["topology_edges"],
            (
                (
                    "read.logical.sensor.mailbox.producer.meta.v1",
                    "write.internal.bridge.mailbox.bridge.request.v1",
                ),
                (
                    "read.internal.bridge.mailbox.bridge.meta.v1",
                    "write.logical.consumer.mailbox.consumer.request.v1",
                ),
            ),
        )

    def test_broadcast_mirror_example(self) -> None:
        result = load_example_module("broadcast_mirror").run_example()

        self.assertEqual(result["mirror_a"], (b"v1", b"v2"))
        self.assertEqual(result["mirror_b"], (b"v1", b"v2"))

    def test_rate_matched_sensor_example(self) -> None:
        result = load_example_module("rate_matched_sensor").run_example()

        self.assertEqual(result["emitted_values"], (11, 13))

    def test_cross_partition_join_example(self) -> None:
        result = load_example_module("cross_partition_join").run_example()

        self.assertEqual(result["join_class"], "repartition")
        self.assertEqual(
            result["visible_nodes"],
            (
                "state.internal.imu_fusion.join.left_repartition.state.v1",
                "state.internal.imu_fusion.join.right_repartition.state.v1",
            ),
        )
        self.assertEqual(
            result["topology_edges"],
            (
                (
                    "read.logical.imu_left.sensor.accel.meta.v1",
                    "state.internal.imu_fusion.join.left_repartition.state.v1",
                ),
                (
                    "read.logical.imu_right.sensor.gyro.meta.v1",
                    "state.internal.imu_fusion.join.right_repartition.state.v1",
                ),
            ),
        )
        self.assertEqual(result["taint_implications"], ("deterministic_rekey",))

    def test_raft_demo_example(self) -> None:
        result = load_example_module("raft_demo").run_example()

        self.assertEqual(result["leader_state"], ("node-a", 3, True))
        self.assertEqual(
            result["quorum_state"],
            (3, "node-a", ("node-a", "node-b"), True),
        )
        self.assertEqual(
            result["replicated_log"],
            ((1, "set mode=auto"), (2, "set temp=21")),
        )

    def test_rolling_window_aggregate_example(self) -> None:
        result = load_example_module("rolling_window_aggregate").run_example()

        self.assertEqual(result["rolling_sums"], (20, 41, 63, 66))

    def test_ephemeral_entropy_stream_example(self) -> None:
        result = load_example_module("ephemeral_entropy_stream").run_example()

        self.assertEqual(result["latest_payload"], b"nonce-1")
        self.assertEqual(result["replay_count"], 0)
        self.assertEqual(result["determinism_taints"], ("DET_NONREPLAYABLE",))
        self.assertEqual(result["latest_replay_policy"], "none")


if __name__ == "__main__":
    unittest.main()
