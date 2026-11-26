#!/usr/bin/env python3
"""Print one release's notes: its CHANGELOG section and the fixed install, client and verify blocks (#428, #1773, #1804).

  python3 scripts/release/release_notes.py 0.0.4 CHANGELOG.md > notes.md

The release workflow passes the CHANGELOG of the tested commit, so the notes describe what
was built, not what was merged after it. A release without a section still gets the install
block and one line that says no notes were written.
"""
from __future__ import annotations

import sys
from pathlib import Path

FALLBACK = "No notes were written for this release."

INSTALL = """\
## Install
```console
$ kubectl apply --server-side --force-conflicts -f https://github.com/stardelt/stardelt/releases/download/v{version}/install.yaml
$ curl -fsSLO https://github.com/stardelt/stardelt/releases/download/v{version}/platform-minimal.yaml
$ kubectl apply -f platform-minimal.yaml      # after you set your domain in it
```
`platform-minimal.yaml` turns on the lakehouse, pipelines and dashboards; set `notebooks: true` under
`spec.pillars` for notebooks. This release was proven on one node with 8 vCPU and 16 GB of memory.
An upgrade applies the new `install.yaml` the same way; a release upgrades the one before it (N−1).
stardelt is `0.0.x`: there is no API stability promise before `0.1.0`.
"""

CLIENT = """\
## Client
The command and the notebook library come from this release:
```console
$ pip install "stardelt[jupyter] @ https://github.com/stardelt/stardelt/releases/download/v{version}/stardelt-{version}-py3-none-any.whl"
```
Use the client of the same release as your Nova (`v{version}`). A client of another release warns
once per command and names the line to install; it fails only on an operation your release does not
serve. An install of the retired `0.4.x` line sorts above `0.0.x` for pip: upgrade it with
`pip install --force-reinstall` and the line above.
"""

VERIFY = """\
## Verify
Every image of this release is signed by the release workflow of `stardelt/stardelt`:
```console
$ cosign verify ghcr.io/stardelt/nova:v{version} \\
    --certificate-identity 'https://github.com/stardelt/stardelt/.github/workflows/release-publish.yml@refs/heads/main' \\
    --certificate-oidc-issuer https://token.actions.githubusercontent.com
```
The same works for `stardelt-operator`, `spark`, `superset`, `airflow`, `jupyter-singleuser` and
`stardelt-cli`. Each image carries its software bill of materials (SPDX):
```console
$ docker buildx imagetools inspect ghcr.io/stardelt/nova:v{version} --format '{{{{ json .SBOM }}}}'
```
"""


def section(changelog: str, version: str) -> str:
    """The `## v<version>` section of `changelog`, heading included, or "".

    The section ends at the next line that starts with `## ` (that line is not part of it).
    """
    heading = f"## v{version}"
    lines = changelog.splitlines()
    start = None
    for index, line in enumerate(lines):
        if line.startswith(heading) and line[len(heading) : len(heading) + 1] in ("", " ", "—", "-", "("):
            start = index
            break
    if start is None:
        return ""
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if lines[index].startswith("## "):
            end = index
            break
    return "\n".join(lines[start:end]).strip("\n")


def notes(version: str, changelog: str) -> str:
    """The release body: the changelog section (or the fallback line) and the install block."""
    version = version.removeprefix("v")
    found = section(changelog, version)
    head = found if found else FALLBACK
    return f"{head}\n\n{INSTALL.format(version=version)}\n{CLIENT.format(version=version)}\n{VERIFY.format(version=version)}"


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 2:
        print("usage: release_notes.py 0.0.N CHANGELOG.md", file=sys.stderr)
        return 2
    version, changelog_path = args
    changelog = ""
    try:
        changelog = Path(changelog_path).read_text()
    except FileNotFoundError:
        pass
    sys.stdout.write(notes(version, changelog))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
