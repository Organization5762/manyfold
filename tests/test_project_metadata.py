from __future__ import annotations

import re
import unittest
from pathlib import Path

import tomllib

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
PACKAGE_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+")


def _dependency_name(requirement: str) -> str:
    match = PACKAGE_NAME_RE.match(requirement)
    if match is None:
        raise AssertionError(f"cannot parse dependency name from {requirement!r}")
    return match.group(0).lower()


class ProjectMetadataTests(unittest.TestCase):
    def test_pyproject_metadata_lists_stay_sorted(self) -> None:
        with PYPROJECT_PATH.open("rb") as file:
            metadata = tomllib.load(file)

        project = metadata["project"]
        dependency_groups = metadata["dependency-groups"]
        keywords = tuple(project["keywords"])
        dependencies = tuple(project["dependencies"])
        dev_dependencies = tuple(dependency_groups["dev"])
        script_names = tuple(project["scripts"])

        self.assertEqual(keywords, tuple(sorted(keywords)))
        self.assertEqual(
            dependencies,
            tuple(sorted(dependencies, key=_dependency_name)),
        )
        self.assertEqual(
            dev_dependencies,
            tuple(sorted(dev_dependencies, key=_dependency_name)),
        )
        self.assertEqual(script_names, tuple(sorted(script_names)))
