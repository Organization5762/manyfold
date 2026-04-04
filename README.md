# manyfold

## PyPI publishing

This repository includes a GitHub Actions workflow at
`.github/workflows/pypi.yml` that builds the project and publishes it to PyPI
when a GitHub release is published. The same workflow can also be run manually
with `workflow_dispatch`.

The workflow uses PyPI trusted publishing via GitHub OIDC. Before the publish
job can succeed, set up the following:

1. Add Python packaging metadata to the repository such as `pyproject.toml` or
   `setup.py`, so `python -m build` can produce files in `dist/`.
2. Create the project on PyPI.
3. In PyPI, add this GitHub repository as a trusted publisher for the workflow
   file `.github/workflows/pypi.yml`.
4. In GitHub, create an environment named `pypi`. Add protection rules there if
   you want manual approval before publishing.

Once those prerequisites are in place, publishing a GitHub release will trigger
the workflow and upload the built distributions to PyPI.
