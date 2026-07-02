# WASM npm Publishing

Manyfold publishes its WebAssembly worker adapter as a generated npm package.
The source of truth stays in the Rust crate; release automation builds the npm
package with `wasm-pack`, verifies `npm pack --dry-run`, uploads the generated
package as a workflow artifact, and submits it to npm staged publishing from the
GitHub Release workflow.

## Package

Default package name:

```text
@organization5762/manyfold
```

The package is generated into:

```text
dist/npm/manyfold
```

Build it locally with:

```sh
python scripts/build_wasm_npm.py
```

This requires `wasm-pack`:

```sh
cargo install wasm-pack --locked
```

## GitHub and npm Setup

Prefer npm Trusted Publishing over a long-lived `NPM_TOKEN`.

1. Create or claim the npm package name and scope.
2. In npm package settings, add a GitHub Actions trusted publisher.
3. Configure the trusted publisher with:
   - Repository owner: `Organization5762`
   - Repository name: `manyfold`
   - Workflow file: `.github/workflows/npm.yml`
   - Environment: `npm`
   - Allowed action: `npm stage publish`
4. In GitHub repository settings, create the `npm` environment. Add required
   reviewers if the npm release should require manual approval.
5. Stage a package by creating a GitHub Release after the version has been
   updated in both `Cargo.toml` and `pyproject.toml`.
6. Review and approve the staged package on npmjs.com or with the npm CLI.

The workflow uses `permissions: id-token: write`, Node 24, the npm registry URL,
and `npm stage publish --provenance`, which are the pieces npm needs for
trusted publishing, staged publishing, and provenance.

For a first package publish, npm may require an initial manual publish or package
creation before trusted publishing settings are available. After that, the
GitHub workflow should own releases.
