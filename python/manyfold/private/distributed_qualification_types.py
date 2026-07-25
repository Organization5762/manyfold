"""Result contract shared by distributed qualification scenarios."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ScenarioStatus = Literal["pass", "fail", "blocked"]


def result(
    name: str,
    passed: bool,
    guarantee: str,
    *,
    evidence: dict[str, object],
) -> ScenarioResult:
    return ScenarioResult(
        name,
        "pass" if passed else "fail",
        guarantee if passed else f"Required outcome was not observed: {guarantee}",
        guarantee if passed else "",
        evidence,
    )


def failed(name: str, detail: str, error: Exception) -> ScenarioResult:
    return ScenarioResult(
        name,
        "fail",
        f"{detail}: {type(error).__name__}: {error}",
        "",
        {"error_type": type(error).__name__, "error": str(error)},
    )


def blocked(name: str, detail: str, **evidence: object) -> ScenarioResult:
    return ScenarioResult(name, "blocked", detail, "", evidence)


@dataclass(frozen=True)
class ScenarioResult:
    """One machine-readable qualification outcome."""

    name: str
    status: ScenarioStatus
    detail: str
    guarantee: str
    evidence: dict[str, object]
