from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

LEAK_SUMMARY_RE = re.compile(
    r"<jemalloc>:\s+Leak(?: approximation)? summary:\s+~?(?P<bytes>\d+)\s+"
    r"bytes?,\s+~?(?P<objects>\d+)\s+objects?,\s+(?:>=\s*)?"
    r"(?P<contexts>\d+)\s+contexts?"
)
DEFAULT_MALLOC_CONF = "prof_leak:true,lg_prof_sample:0,prof_final:true"


def run_leak_check(
    command: tuple[str, ...],
    *,
    malloc_conf: str = DEFAULT_MALLOC_CONF,
    libjemalloc: str | None = None,
    output_dir: Path | None = None,
    leak_bytes_threshold: int = 0,
    leak_objects_threshold: int = 0,
) -> LeakCheckResult:
    """Run a command under jemalloc's final leak profiler and parse the result."""
    if output_dir is not None:
        _reset_output_dir(output_dir)
    env = os.environ.copy()
    env["MALLOC_CONF"] = malloc_conf
    env[_preload_env_name()] = _preload_value(
        existing=env.get(_preload_env_name()),
        libjemalloc=libjemalloc or _find_jemalloc_library(),
    )
    if output_dir is not None:
        env["MALLOC_CONF"] = _append_conf(
            env["MALLOC_CONF"],
            f"prof_prefix:{output_dir / 'jeprof'}",
        )
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    observed_summary = _parse_leak_summary(completed.stderr)
    failing_summary = observed_summary
    if failing_summary is not None and not failing_summary.exceeds(
        bytes_threshold=leak_bytes_threshold,
        objects_threshold=leak_objects_threshold,
    ):
        failing_summary = None
    result = LeakCheckResult(
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        leak_summary=failing_summary,
        observed_leak_summary=observed_summary,
    )
    if output_dir is not None:
        _write_summary(
            output_dir,
            command=command,
            malloc_conf=env["MALLOC_CONF"],
            result=result,
            leak_bytes_threshold=leak_bytes_threshold,
            leak_objects_threshold=leak_objects_threshold,
        )
    return result


def verify_jemalloc_summary(
    path: Path,
    *,
    required_command_fragments: tuple[str, ...] = (),
) -> dict[str, object]:
    """Return a parsed jemalloc summary after enforcing leak evidence checks."""
    summary = json.loads(path.read_text(encoding="utf-8"))
    errors = _jemalloc_summary_errors(
        summary,
        required_command_fragments=required_command_fragments,
    )
    if errors:
        raise SystemExit("; ".join(errors))
    return summary


class LeakCheckResult:
    def __init__(
        self,
        *,
        returncode: int,
        stdout: str,
        stderr: str,
        leak_summary: LeakSummary | None,
        observed_leak_summary: LeakSummary | None,
    ) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.leak_summary = leak_summary
        self.observed_leak_summary = observed_leak_summary

    def as_dict(self) -> dict[str, object]:
        return {
            "failing_leak_summary": _summary_dict(self.leak_summary),
            "observed_leak_summary": _summary_dict(self.observed_leak_summary),
            "passed": (
                self.returncode == 0
                and self.observed_leak_summary is not None
                and self.leak_summary is None
            ),
            "returncode": self.returncode,
        }

    def format(self) -> str:
        summary = self.observed_leak_summary
        if summary is None:
            return (
                "jemalloc_leak_check passed=false "
                f"returncode={self.returncode} leak_summary=unavailable"
            )
        return (
            "jemalloc_leak_check "
            f"passed={str(self.as_dict()['passed']).lower()} "
            f"returncode={self.returncode} "
            f"bytes={summary.bytes} objects={summary.objects} "
            f"contexts={summary.contexts}"
        )


class LeakSummary:
    def __init__(self, *, bytes: int, objects: int, contexts: int) -> None:
        self.bytes = bytes
        self.objects = objects
        self.contexts = contexts

    def exceeds(self, *, bytes_threshold: int, objects_threshold: int) -> bool:
        return self.bytes > bytes_threshold or self.objects > objects_threshold

    def as_dict(self) -> dict[str, int]:
        return {
            "bytes": self.bytes,
            "contexts": self.contexts,
            "objects": self.objects,
        }


def _main() -> None:
    args = _parse_args()
    command = tuple(args.command)
    if not command:
        raise SystemExit("jemalloc leak check requires a command after --")
    result = run_leak_check(
        command,
        malloc_conf=args.malloc_conf,
        libjemalloc=args.libjemalloc,
        output_dir=args.output_dir,
        leak_bytes_threshold=args.leak_bytes_threshold,
        leak_objects_threshold=args.leak_objects_threshold,
    )
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    if result.returncode != 0:
        raise SystemExit(result.returncode)
    if args.require_leak_summary and result.observed_leak_summary is None:
        raise SystemExit(
            "jemalloc leak check failed: no leak summary was emitted; "
            "verify jemalloc was preloaded and prof_leak/prof_final are active"
        )
    if result.leak_summary is not None:
        raise SystemExit(
            "jemalloc leak check failed: "
            f"{result.leak_summary.bytes} bytes, "
            f"{result.leak_summary.objects} objects, "
            f"{result.leak_summary.contexts} contexts retained at process exit"
        )
    print(result.format())


def _verify_main() -> None:
    args = _parse_verify_args()
    summary = verify_jemalloc_summary(
        args.summary,
        required_command_fragments=tuple(args.require_command_fragment or ()),
    )
    observed = summary["observed_leak_summary"]
    print(
        "jemalloc_summary passed=true returncode={returncode} "
        "bytes={bytes} objects={objects} contexts={contexts}".format(
            returncode=summary["returncode"],
            bytes=observed["bytes"],
            objects=observed["objects"],
            contexts=observed["contexts"],
        )
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a command under jemalloc final leak profiling."
    )
    parser.add_argument(
        "--malloc-conf",
        default=DEFAULT_MALLOC_CONF,
        help="jemalloc MALLOC_CONF value used for the checked command.",
    )
    parser.add_argument(
        "--libjemalloc",
        help="Path to libjemalloc. Defaults to a platform-specific search.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("jemalloc-leak-results"),
        help="Directory for jemalloc final heap profile files.",
    )
    parser.add_argument(
        "--leak-bytes-threshold",
        type=int,
        default=0,
        help="Allowed bytes in jemalloc's leak summary before failing.",
    )
    parser.add_argument(
        "--leak-objects-threshold",
        type=int,
        default=0,
        help="Allowed objects in jemalloc's leak summary before failing.",
    )
    parser.add_argument(
        "--allow-missing-leak-summary",
        dest="require_leak_summary",
        action="store_false",
        default=True,
        help=(
            "Do not fail when the wrapped command exits without a jemalloc leak "
            "summary. Intended only for local diagnostics."
        ),
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    return args


def _parse_verify_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify a Manyfold jemalloc leak-check summary JSON artifact."
    )
    parser.add_argument("summary", type=Path)
    parser.add_argument(
        "--require-command-fragment",
        action="append",
        help="Require this string to appear in the recorded command.",
    )
    return parser.parse_args()


def _find_jemalloc_library() -> str:
    candidates = (
        os.environ.get("JEMALLOC_PATH"),
        "/usr/lib/x86_64-linux-gnu/libjemalloc.so.2",
        "/usr/lib/aarch64-linux-gnu/libjemalloc.so.2",
        "/usr/lib64/libjemalloc.so.2",
        "/usr/lib/libjemalloc.so.2",
        "/opt/homebrew/lib/libjemalloc.dylib",
        "/usr/local/lib/libjemalloc.dylib",
    )
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    located = shutil.which("libjemalloc.so.2")
    if located is not None:
        return located
    raise SystemExit(
        "libjemalloc was not found; install jemalloc or pass --libjemalloc"
    )


def _preload_env_name() -> str:
    if sys.platform == "darwin":
        return "DYLD_INSERT_LIBRARIES"
    return "LD_PRELOAD"


def _preload_value(*, existing: str | None, libjemalloc: str) -> str:
    if not existing:
        return libjemalloc
    separator = ":" if sys.platform == "darwin" else " "
    return f"{libjemalloc}{separator}{existing}"


def _append_conf(current: str, addition: str) -> str:
    if not current:
        return addition
    return f"{current},{addition}"


def _reset_output_dir(output_dir: Path) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


def _write_summary(
    output_dir: Path,
    *,
    command: tuple[str, ...],
    malloc_conf: str,
    result: LeakCheckResult,
    leak_bytes_threshold: int,
    leak_objects_threshold: int,
) -> None:
    summary = {
        "command": list(command),
        "leak_bytes_threshold": leak_bytes_threshold,
        "leak_objects_threshold": leak_objects_threshold,
        "malloc_conf": malloc_conf,
        **result.as_dict(),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _jemalloc_summary_errors(
    summary: dict[str, object],
    *,
    required_command_fragments: tuple[str, ...],
) -> tuple[str, ...]:
    errors: list[str] = []
    if summary.get("passed") is not True:
        errors.append("summary did not pass")
    if summary.get("returncode") != 0:
        errors.append(f"returncode was {summary.get('returncode')!r}")
    observed = summary.get("observed_leak_summary")
    if not isinstance(observed, dict):
        errors.append("observed leak summary missing")
    else:
        for field in ("bytes", "contexts", "objects"):
            if not isinstance(observed.get(field), int):
                errors.append(f"observed leak summary field missing: {field}")
    if summary.get("failing_leak_summary") is not None:
        errors.append("failing leak summary present")
    command = " ".join(str(part) for part in summary.get("command", ()))
    for fragment in required_command_fragments:
        if fragment not in command:
            errors.append(f"command missing required fragment {fragment!r}")
    return tuple(errors)


def _summary_dict(summary: LeakSummary | None) -> dict[str, int] | None:
    if summary is None:
        return None
    return summary.as_dict()


def _parse_leak_summary(stderr: str) -> LeakSummary | None:
    match = LEAK_SUMMARY_RE.search(stderr)
    if match is None:
        return None
    return LeakSummary(
        bytes=int(match.group("bytes")),
        objects=int(match.group("objects")),
        contexts=int(match.group("contexts")),
    )
