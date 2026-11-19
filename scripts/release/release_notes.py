#!/usr/bin/env python3
"""Print one release's notes: its CHANGELOG section and the fixed install block (#428).

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
An upgrade applies the new `install.yaml` the same way; a release upgrades the one before it (N−1).
stardelt is `0.0.x`: there is no API stability promise before `0.1.0`.
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
    return f"{head}\n\n{INSTALL.format(version=version)}"


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
