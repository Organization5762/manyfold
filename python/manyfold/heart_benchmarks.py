from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path
from typing import Sequence

from manyfold import memory_benchmarks, monitor_artifacts

DEFAULT_CONFIGURATION = "lib_2026"
DEFAULT_EXTERNAL_RSS_PROJECTED_GROWTH_KIB = 0
DEFAULT_EXTERNAL_RSS_SEGMENT_PROJECTED_GROWTH_KIB = 0
DEFAULT_EXTERNAL_RSS_TAIL_PLATEAU_KIB = 1024
DEFAULT_EXTERNAL_OUTPUT_MAX_SAMPLES = 512
DEFAULT_EXTERNAL_TAIL_MIN_SAMPLES = 5
DEFAULT_EXTERNAL_TAIL_MIN_SECONDS = 8.0
DEFAULT_RSS_WARMUP_FRACTION = 0.75
STRICT_DEVICE_GROWTH_LIMIT = 0
DEFAULT_HEART_ROOT = Path.home() / "code" / "heart"
DEFAULT_UV = shutil.which("uv") or str(Path.home() / ".local" / "bin" / "uv")
DEFAULT_TOTEM_COMMAND = (DEFAULT_UV, "run", "totem", "run")
DEFAULT_VERIFY_PYTHON_COMMAND = (DEFAULT_UV, "run", "python")
LOCAL_MANYFOLD_PYTHON = Path(__file__).resolve().parents[1]
STRICT_DEVICE_REQUIRED_FIELDS = ("pss", "private", "anonymous", "fd")
STRICT_DEVICE_REQUIRED_GATE_LIMITS = {
    "external_anonymous_projected_growth_kib": 0,
    "external_anonymous_segment_projected_growth_kib": 0,
    "external_fd_plateau_count": 0,
    "external_fd_segment_projected_growth_count": 0,
    "external_private_projected_growth_kib": 0,
    "external_private_segment_projected_growth_kib": 0,
    "external_pss_projected_growth_kib": 0,
    "external_pss_segment_projected_growth_kib": 0,
}
STRICT_DEVICE_REQUIRED_GATE_MODES = {
    gate: "max" for gate in STRICT_DEVICE_REQUIRED_GATE_LIMITS
}


def run_heart_benchmark(args: argparse.Namespace) -> object:
    """Run Heart under the external memory monitor."""
    monitor_args = _monitor_args(args)
    if args.verify_local_manyfold:
        monitor_args.external_metadata = _verify_local_manyfold_import(
            monitor_args,
            tuple(args.verify_python_command),
            args.local_manyfold_python.expanduser(),
        )
    print(
        "heart_benchmark cwd={cwd} command={command} duration_seconds={duration}".format(
            cwd=monitor_args.external_cwd,
            command=" ".join(monitor_args.command),
            duration=monitor_args.external_duration_seconds,
        ),
        flush=True,
    )
    return memory_benchmarks._run_external_command_monitor(monitor_args)


def verify_heart_monitor_artifact(
    path: Path,
    *,
    configuration: str = DEFAULT_CONFIGURATION,
    min_samples: int = 30,
) -> dict[str, object]:
    """Verify a Heart device monitor artifact against strict memory gates."""
    return monitor_artifacts.verify_monitor_artifact(
        path,
        min_samples=min_samples,
        required_command_fragments=(configuration,),
        required_gate_limits=STRICT_DEVICE_REQUIRED_GATE_LIMITS,
        required_gate_modes=STRICT_DEVICE_REQUIRED_GATE_MODES,
        required_metrics=STRICT_DEVICE_REQUIRED_FIELDS,
        required_sample_fields=STRICT_DEVICE_REQUIRED_FIELDS,
    )


def _main(argv: Sequence[str] | None = None) -> None:
    run_heart_benchmark(_parse_args(argv))


def _verify_main(argv: Sequence[str] | None = None) -> None:
    args = _parse_verify_args(argv)
    artifact = verify_heart_monitor_artifact(
        args.artifact,
        configuration=args.configuration,
        min_samples=args.min_samples,
    )
    print(
        "heart_monitor_artifact passed=true samples={samples}".format(
            samples=_artifact_sample_count(artifact),
        )
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Heart's totem command under Manyfold's external memory monitor."
        )
    )
    parser.add_argument(
        "--heart-root",
        type=Path,
        default=DEFAULT_HEART_ROOT,
        help="Path to the Heart checkout. Defaults to ~/code/heart.",
    )
    parser.add_argument(
        "--configuration",
        default=DEFAULT_CONFIGURATION,
        help="Heart totem configuration to run.",
    )
    parser.add_argument(
        "--totem-command",
        nargs="+",
        default=DEFAULT_TOTEM_COMMAND,
        help="Command prefix used before --configuration.",
    )
    parser.add_argument(
        "--local-manyfold-python",
        type=Path,
        default=LOCAL_MANYFOLD_PYTHON,
        help="Manyfold Python source path prepended to PYTHONPATH.",
    )
    parser.add_argument(
        "--extra-pythonpath",
        action="append",
        help="Additional PYTHONPATH entries inserted before the inherited value.",
    )
    parser.add_argument(
        "--verify-python-command",
        nargs="+",
        default=DEFAULT_VERIFY_PYTHON_COMMAND,
        help=(
            "Command used for the preflight import check. Defaults to "
            "`uv run python` from the Heart checkout."
        ),
    )
    parser.add_argument(
        "--verify-local-manyfold",
        dest="verify_local_manyfold",
        action="store_true",
        default=True,
        help="Import Manyfold from the Heart environment before launching totem.",
    )
    parser.add_argument(
        "--no-verify-local-manyfold",
        dest="verify_local_manyfold",
        action="store_false",
        help="Skip the preflight import check.",
    )
    parser.add_argument("--duration-seconds", type=float, default=300.0)
    parser.add_argument(
        "--output-json",
        type=Path,
        help="Write external monitor samples and command status to this JSON file.",
    )
    parser.add_argument(
        "--external-output-max-samples",
        type=int,
        default=DEFAULT_EXTERNAL_OUTPUT_MAX_SAMPLES,
        help="Maximum sample rows retained in the JSON artifact.",
    )
    parser.add_argument("--external-min-elapsed-seconds", type=float, default=60.0)
    parser.add_argument("--external-min-samples", type=int, default=10)
    parser.add_argument("--external-project-seconds", type=float, default=86_400.0)
    parser.add_argument(
        "--external-rss-projected-growth-kib",
        type=int,
        default=DEFAULT_EXTERNAL_RSS_PROJECTED_GROWTH_KIB,
        help=(
            "Maximum projected RSS growth over --external-project-seconds. "
            "Defaults to 0 KiB for Heart."
        ),
    )
    parser.add_argument(
        "--external-rss-segment-projected-growth-kib",
        type=int,
        default=DEFAULT_EXTERNAL_RSS_SEGMENT_PROJECTED_GROWTH_KIB,
        help=(
            "Maximum projected RSS growth for any steady-state sample segment. "
            "Defaults to 0 KiB for Heart."
        ),
    )
    parser.add_argument(
        "--external-rss-tail-plateau-kib",
        type=int,
        default=DEFAULT_EXTERNAL_RSS_TAIL_PLATEAU_KIB,
        help=(
            "Maximum RSS range accepted for the final steady tail. "
            "Defaults to 1024 KiB for Heart."
        ),
    )
    parser.add_argument(
        "--external-tail-min-seconds",
        type=float,
        default=DEFAULT_EXTERNAL_TAIL_MIN_SECONDS,
    )
    parser.add_argument(
        "--external-tail-min-samples",
        type=int,
        default=DEFAULT_EXTERNAL_TAIL_MIN_SAMPLES,
    )
    parser.add_argument("--external-pss-plateau-kib", type=int)
    parser.add_argument("--external-pss-projected-growth-kib", type=int)
    parser.add_argument("--external-pss-segment-projected-growth-kib", type=int)
    parser.add_argument("--external-private-plateau-kib", type=int)
    parser.add_argument("--external-private-projected-growth-kib", type=int)
    parser.add_argument("--external-private-segment-projected-growth-kib", type=int)
    parser.add_argument("--external-anonymous-plateau-kib", type=int)
    parser.add_argument("--external-anonymous-projected-growth-kib", type=int)
    parser.add_argument("--external-anonymous-segment-projected-growth-kib", type=int)
    parser.add_argument("--external-fd-plateau-count", type=int)
    parser.add_argument("--external-fd-projected-growth-count", type=int)
    parser.add_argument("--external-fd-segment-projected-growth-count", type=int)
    parser.add_argument(
        "--strict-device-memory-gates",
        action="store_true",
        help=(
            "Require Linux device-only PSS, private, anonymous, and fd gates "
            "with zero projected growth unless each gate is explicitly set."
        ),
    )
    parser.add_argument(
        "--external-rss-scope",
        choices=("process", "tree"),
        default="tree",
    )
    parser.add_argument(
        "--external-terminate-scope",
        choices=("process", "tree"),
        default="tree",
    )
    parser.add_argument("--external-shutdown-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--monitor-interval", type=float, default=1.0)
    parser.add_argument("--rss-plateau-kib", type=int, default=1024)
    parser.add_argument(
        "--rss-warmup-fraction",
        type=float,
        default=DEFAULT_RSS_WARMUP_FRACTION,
    )
    parser.add_argument("--max-elapsed-seconds", type=float)
    parser.add_argument("--max-cpu-seconds", type=float)
    parser.add_argument("--max-disk-input-blocks", type=int)
    parser.add_argument("--max-disk-output-blocks", type=int)
    parser.add_argument("--check", dest="check", action="store_true", default=True)
    parser.add_argument("--no-check", dest="check", action="store_false")
    return parser.parse_args(argv)


def _parse_verify_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify a Heart monitor artifact against strict device gates."
    )
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--configuration", default=DEFAULT_CONFIGURATION)
    parser.add_argument("--min-samples", type=int, default=30)
    return parser.parse_args(argv)


def _monitor_args(args: argparse.Namespace) -> argparse.Namespace:
    heart_root = args.heart_root.expanduser()
    strict_gates = _strict_device_gate_defaults(args)
    return argparse.Namespace(
        check=args.check,
        command=_totem_command(tuple(args.totem_command), args.configuration),
        external_anonymous_plateau_kib=strict_gates["anonymous_plateau_kib"],
        external_anonymous_projected_growth_kib=(
            strict_gates["anonymous_projected_growth_kib"]
        ),
        external_anonymous_segment_projected_growth_kib=(
            strict_gates["anonymous_segment_projected_growth_kib"]
        ),
        external_cwd=str(heart_root),
        external_duration_seconds=args.duration_seconds,
        external_env=_heart_env(
            heart_root,
            manyfold_python=args.local_manyfold_python.expanduser(),
            extra_pythonpath=tuple(args.extra_pythonpath or ()),
        ),
        external_output_json=args.output_json,
        external_output_max_samples=args.external_output_max_samples,
        external_fd_plateau_count=strict_gates["fd_plateau_count"],
        external_fd_projected_growth_count=strict_gates["fd_projected_growth_count"],
        external_fd_segment_projected_growth_count=(
            strict_gates["fd_segment_projected_growth_count"]
        ),
        external_min_elapsed_seconds=args.external_min_elapsed_seconds,
        external_min_samples=args.external_min_samples,
        external_private_plateau_kib=strict_gates["private_plateau_kib"],
        external_private_projected_growth_kib=(
            strict_gates["private_projected_growth_kib"]
        ),
        external_private_segment_projected_growth_kib=(
            strict_gates["private_segment_projected_growth_kib"]
        ),
        external_project_seconds=args.external_project_seconds,
        external_pss_plateau_kib=strict_gates["pss_plateau_kib"],
        external_pss_projected_growth_kib=strict_gates["pss_projected_growth_kib"],
        external_pss_segment_projected_growth_kib=(
            strict_gates["pss_segment_projected_growth_kib"]
        ),
        external_rss_projected_growth_kib=args.external_rss_projected_growth_kib,
        external_rss_scope=args.external_rss_scope,
        external_rss_segment_projected_growth_kib=(
            args.external_rss_segment_projected_growth_kib
        ),
        external_rss_tail_plateau_kib=args.external_rss_tail_plateau_kib,
        external_tail_min_samples=args.external_tail_min_samples,
        external_tail_min_seconds=args.external_tail_min_seconds,
        external_shutdown_timeout_seconds=args.external_shutdown_timeout_seconds,
        external_terminate_scope=args.external_terminate_scope,
        max_cpu_seconds=args.max_cpu_seconds,
        max_disk_input_blocks=args.max_disk_input_blocks,
        max_disk_output_blocks=args.max_disk_output_blocks,
        max_elapsed_seconds=args.max_elapsed_seconds,
        monitor_interval=args.monitor_interval,
        rss_plateau_kib=args.rss_plateau_kib,
        rss_warmup_fraction=args.rss_warmup_fraction,
    )


def _strict_device_gate_defaults(args: argparse.Namespace) -> dict[str, int | None]:
    defaults = {
        "anonymous_plateau_kib": args.external_anonymous_plateau_kib,
        "anonymous_projected_growth_kib": args.external_anonymous_projected_growth_kib,
        "anonymous_segment_projected_growth_kib": (
            args.external_anonymous_segment_projected_growth_kib
        ),
        "fd_plateau_count": args.external_fd_plateau_count,
        "fd_projected_growth_count": args.external_fd_projected_growth_count,
        "fd_segment_projected_growth_count": (
            args.external_fd_segment_projected_growth_count
        ),
        "private_plateau_kib": args.external_private_plateau_kib,
        "private_projected_growth_kib": args.external_private_projected_growth_kib,
        "private_segment_projected_growth_kib": (
            args.external_private_segment_projected_growth_kib
        ),
        "pss_plateau_kib": args.external_pss_plateau_kib,
        "pss_projected_growth_kib": args.external_pss_projected_growth_kib,
        "pss_segment_projected_growth_kib": (
            args.external_pss_segment_projected_growth_kib
        ),
    }
    if not args.strict_device_memory_gates:
        return defaults
    strict_defaults = {
        "anonymous_projected_growth_kib",
        "anonymous_segment_projected_growth_kib",
        "fd_plateau_count",
        "fd_projected_growth_count",
        "fd_segment_projected_growth_count",
        "private_projected_growth_kib",
        "private_segment_projected_growth_kib",
        "pss_projected_growth_kib",
        "pss_segment_projected_growth_kib",
    }
    return {
        key: (
            STRICT_DEVICE_GROWTH_LIMIT
            if key in strict_defaults and value is None
            else value
        )
        for key, value in defaults.items()
    }


def _totem_command(command_prefix: tuple[str, ...], configuration: str) -> tuple[str, ...]:
    if not command_prefix:
        raise SystemExit("--totem-command requires at least one token")
    if not configuration:
        raise SystemExit("--configuration must be non-empty")
    return (*command_prefix, "--configuration", configuration)


def _verify_local_manyfold_import(
    monitor_args: argparse.Namespace,
    python_command: tuple[str, ...],
    expected_python_path: Path,
) -> dict[str, str]:
    if not python_command:
        raise SystemExit("--verify-python-command requires at least one token")
    expected_prefix = str(expected_python_path)
    script = (
        "import manyfold\n"
        "source = manyfold.__file__ or ''\n"
        "if not source.startswith({expected_prefix!r}):\n"
        "    raise SystemExit(\n"
        "        'Manyfold import did not use expected source: '\n"
        "        + source\n"
        "    )\n"
        "print('manyfold_import source=' + source)\n"
        "print('manyfold_bridge_version=' + str(manyfold.bridge_version()))\n"
    ).format(expected_prefix=expected_prefix)
    try:
        result = subprocess.run(
            (*python_command, "-c", script),
            check=True,
            capture_output=True,
            cwd=monitor_args.external_cwd,
            env=monitor_args.external_env,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        raise SystemExit(error.stderr or error.stdout or str(error)) from error
    print(result.stdout, end="", flush=True)
    return _manyfold_preflight_metadata(result.stdout)


def _manyfold_preflight_metadata(stdout: str) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for line in stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator and key in ("manyfold_import source", "manyfold_bridge_version"):
            metadata[
                "manyfold_source"
                if key == "manyfold_import source"
                else "manyfold_bridge_version"
            ] = value
    missing = {"manyfold_source", "manyfold_bridge_version"} - metadata.keys()
    if missing:
        raise SystemExit(
            "Manyfold preflight output missing metadata: " + ", ".join(sorted(missing))
        )
    return metadata


def _artifact_sample_count(artifact: dict[str, object]) -> int:
    sample_count = artifact.get("sample_count")
    if isinstance(sample_count, int):
        return sample_count
    samples = artifact.get("samples", ())
    if isinstance(samples, list | tuple):
        return len(samples)
    return 0


def _heart_env(
    heart_root: Path,
    *,
    manyfold_python: Path,
    extra_pythonpath: tuple[str, ...],
) -> dict[str, str]:
    env = os.environ.copy()
    pythonpath_parts = [
        str(manyfold_python),
        str(heart_root / "src"),
        *extra_pythonpath,
    ]
    inherited_pythonpath = env.get("PYTHONPATH")
    if inherited_pythonpath:
        pythonpath_parts.append(inherited_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
    return env


if __name__ == "__main__":
    _main()
