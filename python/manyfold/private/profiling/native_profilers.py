from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from pathlib import Path

SUPPORTED_PROFILERS = (
    "cachegrind",
    "callgrind",
    "coz",
    "dhat",
    "heaptrack",
    "massif",
    "perf",
)
DEFAULT_PROFILERS = ("dhat", "heaptrack", "perf", "coz")
DEFAULT_BENCHMARK_COMMAND = (
    "./target/profiling/memory_retention_benchmark",
    "--iterations",
    "20000",
    "--sample-every",
    "10000",
    "--history-limit",
    "8",
    "--live-plateau-bytes",
    "0",
    "--warmup-samples",
    "1",
    "--check-invariants-every",
    "1000",
)


def run_profiles(
    command: tuple[str, ...],
    *,
    profilers: tuple[str, ...] = DEFAULT_PROFILERS,
    output_dir: Path = Path("native-profile-results"),
    skip_missing: bool = False,
    continue_on_error: bool = False,
) -> tuple[ProfilerResult, ...]:
    """Run native profilers over a command and collect profile artifacts."""
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[ProfilerResult] = []
    for profiler in _expand_profilers(profilers):
        profile_dir = output_dir / profiler
        _reset_profile_dir(profile_dir)
        try:
            profile_command = _profile_command(profiler, command, profile_dir)
        except _MissingProfilerTool as exc:
            status = "skipped" if skip_missing else "failed"
            result = ProfilerResult(
                profiler=profiler,
                status=status,
                elapsed_seconds=0.0,
                returncode=127,
                detail=str(exc),
            )
            results.append(result)
            if not skip_missing and not continue_on_error:
                break
            continue

        started = time.monotonic()
        completed = subprocess.run(
            profile_command.command,
            check=False,
            capture_output=True,
            text=True,
        )
        elapsed_seconds = time.monotonic() - started
        _write_text(profile_dir / "stdout.txt", completed.stdout)
        _write_text(profile_dir / "stderr.txt", completed.stderr)
        post_detail = _run_post_processors(profile_command, profile_dir)
        missing_outputs = _missing_expected_outputs(profile_command)
        status = (
            "ok"
            if completed.returncode == 0 and not missing_outputs
            else "failed"
        )
        detail_parts = []
        if post_detail:
            detail_parts.append(post_detail)
        if missing_outputs:
            detail_parts.append(
                "missing expected profile output: " + ", ".join(missing_outputs)
            )
        detail = "; ".join(detail_parts) or "profile captured"
        if completed.returncode != 0:
            detail = f"profile command exited {completed.returncode}: {detail}"
        result = ProfilerResult(
            profiler=profiler,
            status=status,
            elapsed_seconds=elapsed_seconds,
            returncode=completed.returncode,
            detail=detail,
        )
        results.append(result)
        if completed.returncode != 0 and not continue_on_error:
            break
    _write_summary(output_dir, command, tuple(results))
    return tuple(results)


def verify_profiler_summary(
    path: Path,
    *,
    required_profilers: tuple[str, ...] = (),
    required_command_fragments: tuple[str, ...] = (),
    allow_skipped: bool = False,
) -> dict[str, object]:
    """Return a parsed profiler summary after enforcing artifact evidence."""
    summary = json.loads(path.read_text(encoding="utf-8"))
    errors = _profiler_summary_errors(
        summary,
        output_dir=path.parent,
        required_profilers=required_profilers,
        required_command_fragments=required_command_fragments,
        allow_skipped=allow_skipped,
    )
    if errors:
        raise SystemExit("; ".join(errors))
    return summary


class ProfilerResult:
    def __init__(
        self,
        *,
        profiler: str,
        status: str,
        elapsed_seconds: float,
        returncode: int,
        detail: str,
    ) -> None:
        self.profiler = profiler
        self.status = status
        self.elapsed_seconds = elapsed_seconds
        self.returncode = returncode
        self.detail = detail

    def format(self) -> str:
        return (
            f"profiler={self.profiler} status={self.status} "
            f"returncode={self.returncode} "
            f"elapsed_seconds={self.elapsed_seconds:.3f} detail={self.detail}"
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "profiler": self.profiler,
            "status": self.status,
            "elapsed_seconds": self.elapsed_seconds,
            "returncode": self.returncode,
            "detail": self.detail,
        }


def _main(argv: tuple[str, ...] | None = None) -> None:
    args = _parse_args(argv)
    if args.list:
        for profiler in SUPPORTED_PROFILERS:
            print(profiler)
        return
    command = tuple(args.command) if args.command else DEFAULT_BENCHMARK_COMMAND
    results = run_profiles(
        command,
        profilers=tuple(args.profiler or DEFAULT_PROFILERS),
        output_dir=args.output_dir,
        skip_missing=args.skip_missing,
        continue_on_error=args.continue_on_error,
    )
    failed = False
    for result in results:
        print(result.format())
        failed = failed or result.status == "failed"
    if failed and not args.allow_failures:
        raise SystemExit(1)


def _verify_main(argv: tuple[str, ...] | None = None) -> None:
    args = _parse_verify_args(argv)
    summary = verify_profiler_summary(
        args.summary,
        required_profilers=tuple(args.require_profiler or ()),
        required_command_fragments=tuple(args.require_command_fragment or ()),
        allow_skipped=args.allow_skipped,
    )
    results = tuple(summary["results"])
    ok_count = sum(1 for result in results if result["status"] == "ok")
    print(
        "native_profiler_summary passed=true "
        f"results={len(results)} ok={ok_count}"
    )


def _parse_args(argv: tuple[str, ...] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run native profiling tools against the Manyfold benchmark."
    )
    parser.add_argument(
        "--profiler",
        action="append",
        choices=(*SUPPORTED_PROFILERS, "all"),
        help="Profiler to run. May be repeated. Defaults to a fast useful subset.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("native-profile-results"),
        help="Directory where profiler artifacts are written.",
    )
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Skip profilers whose command-line tools are not installed.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Run remaining profilers after a profiler command fails.",
    )
    parser.add_argument(
        "--allow-failures",
        action="store_true",
        help="Exit successfully even if one or more profiler commands fail.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List supported profiler names and exit.",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    return args


def _parse_verify_args(argv: tuple[str, ...] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify a Manyfold native profiler summary JSON artifact."
    )
    parser.add_argument("summary", type=Path)
    parser.add_argument(
        "--require-profiler",
        action="append",
        choices=SUPPORTED_PROFILERS,
        help="Require this profiler to be present, successful, and artifact-backed.",
    )
    parser.add_argument(
        "--require-command-fragment",
        action="append",
        help="Require this string to appear in the recorded profiled command.",
    )
    parser.add_argument(
        "--allow-skipped",
        action="store_true",
        help="Allow skipped profiler results. Failed results are never accepted.",
    )
    return parser.parse_args(argv)


def _expand_profilers(profilers: tuple[str, ...]) -> tuple[str, ...]:
    expanded: list[str] = []
    for profiler in profilers:
        if profiler == "all":
            expanded.extend(SUPPORTED_PROFILERS)
        else:
            expanded.append(profiler)
    return tuple(dict.fromkeys(expanded))


def _profile_command(
    profiler: str,
    command: tuple[str, ...],
    output_dir: Path,
) -> _ProfilerCommand:
    if profiler == "cachegrind":
        return _valgrind_command(
            profiler,
            command,
            output_dir,
            "--tool=cachegrind",
            f"--cachegrind-out-file={output_dir / 'cachegrind.out'}",
            (("cg_annotate", output_dir / "cachegrind.out", output_dir / "cachegrind.txt"),),
        )
    if profiler == "callgrind":
        return _valgrind_command(
            profiler,
            command,
            output_dir,
            "--tool=callgrind",
            f"--callgrind-out-file={output_dir / 'callgrind.out'}",
            (
                (
                    "callgrind_annotate",
                    output_dir / "callgrind.out",
                    output_dir / "callgrind.txt",
                ),
            ),
        )
    if profiler == "coz":
        _require_tool("coz")
        return _ProfilerCommand(
            command=(
                "coz",
                "run",
                "-b",
                "MAIN",
                "--end-to-end",
                "-o",
                str(output_dir / "profile.coz"),
                "---",
                *command,
            ),
            post_processors=(
                (
                    "coz",
                    "plot",
                    "--text",
                    "-i",
                    output_dir / "profile.coz",
                    output_dir / "profile.txt",
                ),
            ),
            expected_outputs=(output_dir / "profile.coz",),
        )
    if profiler == "dhat":
        return _valgrind_command(
            profiler,
            command,
            output_dir,
            "--tool=dhat",
            f"--dhat-out-file={output_dir / 'dhat.out'}",
            (),
        )
    if profiler == "heaptrack":
        _require_tool("heaptrack")
        return _ProfilerCommand(
            command=("heaptrack", "-o", str(output_dir / "heaptrack"), *command),
            post_processors=(
                (
                    "heaptrack_print",
                    output_dir / "heaptrack",
                    output_dir / "heaptrack.txt",
                ),
            ),
            expected_outputs=(("glob", output_dir, "heaptrack*"),),
        )
    if profiler == "massif":
        return _valgrind_command(
            profiler,
            command,
            output_dir,
            "--tool=massif",
            f"--massif-out-file={output_dir / 'massif.out'}",
            (("ms_print", output_dir / "massif.out", output_dir / "massif.txt"),),
        )
    if profiler == "perf":
        _require_tool("perf")
        data_file = output_dir / "perf.data"
        return _ProfilerCommand(
            command=(
                "perf",
                "record",
                "-e",
                "cpu-clock",
                "-F",
                "99",
                "-g",
                "-o",
                str(data_file),
                "--",
                *command,
            ),
            post_processors=(
                ("perf", "report", "--stdio", "-i", data_file, output_dir / "perf-report.txt"),
                ("perf", "script", "-i", data_file, output_dir / "perf-script.txt"),
            ),
            expected_outputs=(data_file,),
        )
    raise ValueError(f"unsupported profiler: {profiler}")


def _valgrind_command(
    profiler: str,
    command: tuple[str, ...],
    output_dir: Path,
    tool_arg: str,
    output_arg: str,
    post_processors: tuple[tuple[object, ...], ...],
) -> _ProfilerCommand:
    _require_tool("valgrind")
    log_file = output_dir / f"{profiler}.log"
    return _ProfilerCommand(
        command=("valgrind", tool_arg, f"--log-file={log_file}", output_arg, *command),
        post_processors=post_processors,
        expected_outputs=(log_file, Path(output_arg.split("=", 1)[1])),
    )


def _run_post_processors(
    profile_command: _ProfilerCommand,
    output_dir: Path,
) -> str:
    details: list[str] = []
    for post_processor in profile_command.post_processors:
        tool = str(post_processor[0])
        output_path = Path(post_processor[-1])
        if shutil.which(tool) is None:
            details.append(f"{tool} not found; raw profile retained")
            continue
        command = tuple(str(part) for part in post_processor[:-1])
        if tool == "heaptrack_print":
            command = _heaptrack_print_command(command, output_dir)
            if not command:
                details.append("heaptrack output not found")
                continue
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        _write_text(output_path, completed.stdout + completed.stderr)
        if completed.returncode != 0:
            details.append(f"{tool} exited {completed.returncode}")
        else:
            details.append(f"{output_path.name} written")
    return "; ".join(details)


def _heaptrack_print_command(
    command: tuple[str, ...],
    output_dir: Path,
) -> tuple[str, ...]:
    matches = sorted(
        path
        for path in output_dir.glob("heaptrack*")
        if path.is_file() and path.name != "heaptrack.txt"
    )
    if not matches:
        return ()
    return (*command[:1], str(matches[-1]))


def _missing_expected_outputs(profile_command: "_ProfilerCommand") -> tuple[str, ...]:
    missing: list[str] = []
    for expected_output in profile_command.expected_outputs:
        if isinstance(expected_output, Path):
            if not expected_output.exists():
                missing.append(str(expected_output))
            continue
        kind, directory, pattern = expected_output
        if kind != "glob":
            raise ValueError(f"unsupported expected output kind: {kind}")
        matches = tuple(
            path
            for path in Path(directory).glob(str(pattern))
            if path.is_file()
            and path.name not in {"heaptrack.txt", "stderr.txt", "stdout.txt"}
        )
        if not matches:
            missing.append(str(Path(directory) / str(pattern)))
    return tuple(missing)


def _reset_profile_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _require_tool(tool: str) -> None:
    if shutil.which(tool) is None:
        raise _MissingProfilerTool(f"{tool} is not installed")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_summary(
    output_dir: Path,
    command: tuple[str, ...],
    results: tuple[ProfilerResult, ...],
) -> None:
    summary = {
        "command": list(command),
        "results": [result.as_dict() for result in results],
    }
    _write_text(
        output_dir / "summary.json",
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
    )


def _profiler_summary_errors(
    summary: dict[str, object],
    *,
    output_dir: Path,
    required_profilers: tuple[str, ...],
    required_command_fragments: tuple[str, ...],
    allow_skipped: bool,
) -> tuple[str, ...]:
    errors: list[str] = []
    command = summary.get("command")
    if not isinstance(command, list) or not all(
        isinstance(part, str) for part in command
    ):
        errors.append("command missing or invalid")
        command_text = ""
    else:
        command_text = " ".join(command)
    for fragment in required_command_fragments:
        if fragment not in command_text:
            errors.append(f"command missing required fragment {fragment!r}")

    results = summary.get("results")
    if not isinstance(results, list) or not results:
        errors.append("results missing or empty")
        return tuple(errors)

    seen: set[str] = set()
    result_by_profiler: dict[str, dict[str, object]] = {}
    for raw_result in results:
        if not isinstance(raw_result, dict):
            errors.append("profiler result must be an object")
            continue
        profiler = raw_result.get("profiler")
        if not isinstance(profiler, str):
            errors.append("profiler result missing profiler name")
            continue
        if profiler in seen:
            errors.append(f"duplicate profiler result: {profiler}")
        seen.add(profiler)
        result_by_profiler[profiler] = raw_result
        errors.extend(
            _profiler_result_errors(
                raw_result,
                output_dir=output_dir,
                allow_skipped=allow_skipped,
            )
        )

    for profiler in required_profilers:
        result = result_by_profiler.get(profiler)
        if result is None:
            errors.append(f"required profiler missing: {profiler}")
            continue
        if result.get("status") != "ok":
            errors.append(f"required profiler did not pass: {profiler}")
    return tuple(errors)


def _profiler_result_errors(
    result: dict[str, object],
    *,
    output_dir: Path,
    allow_skipped: bool,
) -> tuple[str, ...]:
    errors: list[str] = []
    profiler = result.get("profiler")
    status = result.get("status")
    returncode = result.get("returncode")
    elapsed_seconds = result.get("elapsed_seconds")
    if profiler not in SUPPORTED_PROFILERS:
        errors.append(f"unsupported profiler in summary: {profiler!r}")
        return tuple(errors)
    if status == "failed":
        errors.append(f"profiler failed: {profiler}")
    elif status == "skipped":
        if not allow_skipped:
            errors.append(f"profiler skipped: {profiler}")
    elif status != "ok":
        errors.append(f"profiler has invalid status: {profiler}={status!r}")
    if status == "ok" and returncode != 0:
        errors.append(f"profiler ok with nonzero returncode: {profiler}")
    if not isinstance(elapsed_seconds, int | float) or elapsed_seconds < 0:
        errors.append(f"profiler elapsed_seconds invalid: {profiler}")
    if status == "ok":
        errors.extend(_profiler_artifact_errors(str(profiler), output_dir))
    return tuple(errors)


def _profiler_artifact_errors(profiler: str, output_dir: Path) -> tuple[str, ...]:
    profile_dir = output_dir / profiler
    required_files = {
        "cachegrind": ("cachegrind.log", "cachegrind.out"),
        "callgrind": ("callgrind.log", "callgrind.out"),
        "coz": ("profile.coz",),
        "dhat": ("dhat.log", "dhat.out"),
        "massif": ("massif.log", "massif.out"),
        "perf": ("perf.data",),
    }
    errors: list[str] = []
    for name in ("stdout.txt", "stderr.txt", *required_files.get(profiler, ())):
        if not (profile_dir / name).is_file():
            errors.append(f"profiler artifact missing: {profile_dir / name}")
    if profiler == "heaptrack":
        matches = tuple(
            path
            for path in profile_dir.glob("heaptrack*")
            if path.is_file()
            and path.name not in {"heaptrack.txt", "stderr.txt", "stdout.txt"}
        )
        if not matches:
            errors.append(f"profiler artifact missing: {profile_dir / 'heaptrack*'}")
    return tuple(errors)


class _ProfilerCommand:
    def __init__(
        self,
        *,
        command: tuple[object, ...],
        post_processors: tuple[tuple[object, ...], ...],
        expected_outputs: tuple[object, ...],
    ) -> None:
        self.command = tuple(str(part) for part in command)
        self.post_processors = post_processors
        self.expected_outputs = expected_outputs


class _MissingProfilerTool(RuntimeError):
    pass
