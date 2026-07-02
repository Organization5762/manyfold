"""Build the generated npm package for Manyfold's WASM worker adapter."""

from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import subprocess

import tomllib

DEFAULT_OUT_DIR = pathlib.Path("dist/npm/manyfold")
DEFAULT_PACKAGE_NAME = "@organization5762/manyfold"
DEFAULT_WASM_PACK_TARGETS = ("bundler", "web", "nodejs")


def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=pathlib.Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--package-name", default=DEFAULT_PACKAGE_NAME)
    parser.add_argument(
        "--wasm-pack-target",
        action="append",
        choices=DEFAULT_WASM_PACK_TARGETS,
        dest="wasm_pack_targets",
        help=(
            "wasm-pack target to include. Repeat to select multiple targets. "
            "Defaults to bundler, web, and nodejs."
        ),
    )
    args = parser.parse_args()

    root = pathlib.Path(__file__).resolve().parents[1]
    out_dir = args.out_dir
    if not out_dir.is_absolute():
        out_dir = root / out_dir
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True)

    wasm_pack_targets = tuple(args.wasm_pack_targets or DEFAULT_WASM_PACK_TARGETS)
    for target in wasm_pack_targets:
        _build_wasm_pack_target(root, out_dir / target, target)

    cargo = tomllib.loads((root / "Cargo.toml").read_text())
    pyproject = tomllib.loads((root / "pyproject.toml").read_text())
    cargo_version = cargo["package"]["version"]
    python_version = pyproject["project"]["version"]
    if cargo_version != python_version:
        raise SystemExit(
            "Cargo.toml package.version "
            f"{cargo_version!r} must match pyproject.toml project.version "
            f"{python_version!r}"
        )

    module_name = cargo["lib"]["name"]
    package = _package_metadata(
        cargo=cargo,
        package_name=args.package_name,
        version=cargo_version,
        module_name=module_name,
        wasm_pack_targets=wasm_pack_targets,
    )
    package_path = out_dir / "package.json"
    package_path.write_text(json.dumps(package, indent=2, sort_keys=True) + "\n")
    shutil.copyfile(root / "LICENSE", out_dir / "LICENSE")
    shutil.copyfile(root / "scripts" / "wasm_npm" / "README.md", out_dir / "README.md")
    print(
        "built npm package "
        f"{args.package_name}@{cargo_version} in {out_dir} "
        f"with wasm-pack targets {', '.join(wasm_pack_targets)}"
    )


def _build_wasm_pack_target(root: pathlib.Path, out_dir: pathlib.Path, target: str) -> None:
    _run(
        [
            "wasm-pack",
            "build",
            str(root),
            "--target",
            target,
            "--release",
            "--out-dir",
            str(out_dir),
            "--",
            "--no-default-features",
            "--features",
            "wasm",
        ]
    )
    generated_package = out_dir / "package.json"
    if generated_package.exists():
        generated_package.unlink()
    for generated_metadata in ("LICENSE", "README.md", ".gitignore"):
        metadata_path = out_dir / generated_metadata
        if metadata_path.exists():
            metadata_path.unlink()


def _package_metadata(
    *,
    cargo: dict[str, object],
    package_name: str,
    version: str,
    module_name: str,
    wasm_pack_targets: tuple[str, ...],
) -> dict[str, object]:
    exports: dict[str, object] = {
        "./package.json": "./package.json",
    }
    if "bundler" in wasm_pack_targets:
        exports["."] = _esm_export("bundler", module_name)
    elif "web" in wasm_pack_targets:
        exports["."] = _esm_export("web", module_name)
    elif "nodejs" in wasm_pack_targets:
        exports["."] = _node_export("nodejs", module_name)
    for target in wasm_pack_targets:
        exports[f"./{target}"] = (
            _node_export(target, module_name)
            if target == "nodejs"
            else _esm_export(target, module_name)
        )

    return {
        "name": package_name,
        "version": version,
        "description": "Manyfold WebAssembly PubSub runtime adapter.",
        "license": cargo["package"]["license"],
        "homepage": cargo["package"]["homepage"],
        "repository": {
            "type": "git",
            "url": f"git+{cargo['package']['repository']}.git",
            "directory": ".",
        },
        "keywords": [
            "manyfold",
            "wasm",
            "webassembly",
            "worker",
            "pubsub",
        ],
        "main": f"./nodejs/{module_name}.js"
        if "nodejs" in wasm_pack_targets
        else f"./{wasm_pack_targets[0]}/{module_name}.js",
        "module": f"./bundler/{module_name}.js"
        if "bundler" in wasm_pack_targets
        else f"./{wasm_pack_targets[0]}/{module_name}.js",
        "types": f"./{wasm_pack_targets[0]}/{module_name}.d.ts",
        "exports": exports,
        "sideEffects": False,
        "files": [
            "bundler/**",
            "web/**",
            "nodejs/**",
            "LICENSE",
            "README.md",
            "package.json",
        ],
    }


def _esm_export(target: str, module_name: str) -> dict[str, str]:
    return {
        "types": f"./{target}/{module_name}.d.ts",
        "import": f"./{target}/{module_name}.js",
        "default": f"./{target}/{module_name}.js",
    }


def _node_export(target: str, module_name: str) -> dict[str, str]:
    return {
        "types": f"./{target}/{module_name}.d.ts",
        "require": f"./{target}/{module_name}.js",
        "default": f"./{target}/{module_name}.js",
    }


def _run(command: list[str]) -> None:
    try:
        subprocess.run(command, check=True)
    except FileNotFoundError as error:
        raise SystemExit(
            "wasm-pack is required to build the npm package; install it with "
            "`cargo install wasm-pack --locked` or run the npm publish workflow"
        ) from error
    except subprocess.CalledProcessError as error:
        raise SystemExit(error.returncode) from error


if __name__ == "__main__":
    _main()
