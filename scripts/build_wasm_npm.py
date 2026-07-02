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


def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=pathlib.Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--package-name", default=DEFAULT_PACKAGE_NAME)
    args = parser.parse_args()

    root = pathlib.Path(__file__).resolve().parents[1]
    out_dir = args.out_dir
    if not out_dir.is_absolute():
        out_dir = root / out_dir
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.parent.mkdir(parents=True, exist_ok=True)

    _run(
        [
            "wasm-pack",
            "build",
            str(root),
            "--target",
            "bundler",
            "--release",
            "--out-dir",
            str(out_dir),
            "--",
            "--no-default-features",
            "--features",
            "wasm",
        ]
    )

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

    package_path = out_dir / "package.json"
    package = json.loads(package_path.read_text())
    package.update(
        {
            "name": args.package_name,
            "version": cargo_version,
            "description": "Manyfold worker proxy WebAssembly adapter.",
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
            "sideEffects": False,
            "files": [
                "*.js",
                "*.d.ts",
                "*.wasm",
                "LICENSE",
                "README.md",
                "package.json",
            ],
        }
    )
    package_path.write_text(json.dumps(package, indent=2, sort_keys=True) + "\n")
    shutil.copyfile(root / "LICENSE", out_dir / "LICENSE")
    shutil.copyfile(root / "npm" / "manyfold" / "README.md", out_dir / "README.md")
    print(f"built npm package {args.package_name}@{cargo_version} in {out_dir}")


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
