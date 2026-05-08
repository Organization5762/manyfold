from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from examples import REFERENCE_EXAMPLE_PROGRESS_DETAIL
from tests.test_support import subprocess_test_env

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "python" / "manyfold" / "rfc_checklist_gen.py"


def load_generator():
    spec = importlib.util.spec_from_file_location(
        "manyfold_rfc_checklist_gen", MODULE_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class RfcChecklistGenTests(unittest.TestCase):
    def test_generator_file_import_uses_repo_path_fallback(self) -> None:
        spec = importlib.util.spec_from_file_location(
            "manyfold_rfc_checklist_gen_file_test",
            MODULE_PATH,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        sys.modules[spec.name] = module

        calls: list[str] = []
        fake_manyfold = types.ModuleType("manyfold")
        fake_manyfold.__path__ = []  # type: ignore[attr-defined]
        fake_repo_paths = types.ModuleType("manyfold._repo_paths")
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
        self.assertEqual(module.CHECKLIST_STATUS["23"][1], REFERENCE_EXAMPLE_PROGRESS_DETAIL)

    def test_parse_helpers_ignore_appendix_headings_and_unknown_appendix_items(
        self,
    ) -> None:
        generator = load_generator()

        self.assertIsNone(
            generator._parse_section_heading("## Appendix F. Acceptance Criteria")
        )
        self.assertIsNone(
            generator._parse_section_heading("## Notes. Not a numbered section")
        )
        section = generator._parse_section_heading("## 6. Appendix-aware vocabulary")
        assert section is not None
        self.assertEqual(section.title, "Appendix-aware vocabulary")
        self.assertIsNone(generator._parse_appendix_item("- [ ] Unknown appendix item"))
        self.assertIsNone(
            generator._parse_appendix_item("- [?] Query plane modeled as streams")
        )
        self.assertIsNone(
            generator._parse_appendix_item("- [ ]Query plane modeled as streams")
        )

    def test_parse_appendix_item_accepts_known_checkbox_states(self) -> None:
        generator = load_generator()

        open_item = generator._parse_appendix_item(
            "- [ ] Retry, filtering, backpressure, overflow, and rate matching"
        )
        complete_item = generator._parse_appendix_item(
            "- [x] Query plane modeled as streams"
        )

        assert open_item is not None
        assert complete_item is not None
        self.assertEqual(open_item.checkbox, " ")
        self.assertEqual(complete_item.checkbox, "x")

    def test_recent_operator_and_flow_control_statuses_are_descriptive(self) -> None:
        generator = load_generator()

        self.assertIn("rate matching now exist", generator.CHECKLIST_STATUS["13"][1])
        self.assertIn(
            "trigger-driven rolling window aggregation",
            generator.CHECKLIST_STATUS["14"][1],
        )
        self.assertIn("lookup join", generator.CHECKLIST_STATUS["14"][1])
        self.assertIn(
            "route credit snapshots",
            generator.APPENDIX_STATUS[
                "Retry, filtering, backpressure, overflow, and rate matching"
            ][1],
        )
        self.assertIn(
            "bounded interval joins",
            generator.APPENDIX_STATUS["Windows, aggregations, and streaming joins"][1],
        )
        self.assertIn(
            "lookup joins",
            generator.APPENDIX_STATUS["Windows, aggregations, and streaming joins"][1],
        )
        self.assertEqual(
            generator.CHECKLIST_STATUS["23"][1],
            REFERENCE_EXAMPLE_PROGRESS_DETAIL,
        )

    def test_parsed_rfc_sections_include_recent_partial_progress_details(self) -> None:
        generator = load_generator()

        sections, appendix_items = generator.parse_rfc_sections()
        sections_by_number = {section.number: section for section in sections}
        appendix_by_title = {item.title: item for item in appendix_items}

        self.assertEqual(sections_by_number["13"].checkbox, " ")
        self.assertIn("queue depth", sections_by_number["13"].detail)
        self.assertEqual(sections_by_number["14"].checkbox, " ")
        self.assertIn("latest-join", sections_by_number["14"].detail)
        self.assertIn("lookup join", sections_by_number["14"].detail)
        self.assertEqual(
            appendix_by_title[
                "Retry, filtering, backpressure, overflow, and rate matching"
            ].checkbox,
            " ",
        )
        self.assertIn(
            "demand-driven rate matching",
            appendix_by_title[
                "Retry, filtering, backpressure, overflow, and rate matching"
            ].detail,
        )
        self.assertEqual(
            appendix_by_title["Windows, aggregations, and streaming joins"].checkbox,
            " ",
        )
        self.assertIn(
            "trigger-driven rolling window aggregation",
            appendix_by_title["Windows, aggregations, and streaming joins"].detail,
        )
        self.assertIn(
            "lookup joins",
            appendix_by_title["Windows, aggregations, and streaming joins"].detail,
        )

    def test_checklist_output_matches_repository(self) -> None:
        generator = load_generator()
        sections, appendix_items = generator.parse_rfc_sections()
        rendered = generator.render_checklist(sections, appendix_items)
        self.assertEqual(
            rendered,
            (REPO_ROOT / "docs" / "rfc" / "implementation_checklist.md").read_text(),
        )

    def test_checklist_main_check_and_write_modes_follow_generated_output(self) -> None:
        generator = load_generator()
        sections, appendix_items = generator.parse_rfc_sections()
        rendered = generator.render_checklist(sections, appendix_items)

        with tempfile.TemporaryDirectory() as temp_dir:
            checklist_path = Path(temp_dir) / "implementation_checklist.md"
            original_path = generator.CHECKLIST_PATH
            try:
                generator.CHECKLIST_PATH = checklist_path

                self.assertEqual(generator.main(["--check"]), 1)

                self.assertEqual(generator.main([]), 0)
                self.assertEqual(checklist_path.read_text(), rendered)

                self.assertEqual(generator.main(["--check"]), 0)
            finally:
                generator.CHECKLIST_PATH = original_path

    def test_checklist_write_mode_skips_unchanged_file(self) -> None:
        generator = load_generator()
        sections, appendix_items = generator.parse_rfc_sections()
        rendered = generator.render_checklist(sections, appendix_items)
        test_case = self

        class TrackingChecklistPath:
            writes = 0

            def exists(self) -> bool:
                return True

            def read_text(self, *, encoding: str) -> str:
                test_case.assertEqual(encoding, "utf-8")
                return rendered

            def write_text(self, content: str, *, encoding: str) -> int:
                test_case.assertEqual(content, rendered)
                test_case.assertEqual(encoding, "utf-8")
                self.writes += 1
                return len(content)

        checklist_path = TrackingChecklistPath()

        self.assertFalse(generator._write_if_changed(checklist_path, rendered))
        self.assertEqual(checklist_path.writes, 0)

    def test_checklist_write_mode_updates_changed_file(self) -> None:
        generator = load_generator()
        sections, appendix_items = generator.parse_rfc_sections()
        rendered = generator.render_checklist(sections, appendix_items)
        test_case = self

        class TrackingChecklistPath:
            writes = 0
            content = "stale checklist"

            def exists(self) -> bool:
                return True

            def read_text(self, *, encoding: str) -> str:
                test_case.assertEqual(encoding, "utf-8")
                return self.content

            def write_text(self, content: str, *, encoding: str) -> int:
                test_case.assertEqual(encoding, "utf-8")
                self.writes += 1
                self.content = content
                return len(content)

        checklist_path = TrackingChecklistPath()

        self.assertTrue(generator._write_if_changed(checklist_path, rendered))
        self.assertEqual(checklist_path.writes, 1)
        self.assertEqual(checklist_path.content, rendered)

    def test_script_entrypoint_check_runs(self) -> None:
        result = subprocess.run(
            [
                "uv",
                "run",
                "manyfold-rfc-checklist",
                "--check",
            ],
            check=False,
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            env=subprocess_test_env(),
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)


if __name__ == "__main__":
    unittest.main()
