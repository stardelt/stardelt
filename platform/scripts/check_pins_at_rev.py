#!/usr/bin/env python3
"""Report, for one commit M, which pins are the builds of commits at or before M (#1703 spike).

Two rules are reported side by side and neither fails the process:
  D1  the pin resolves to some `sha-<short>` tag of a commit <= M
  D2  the pin is the build of the newest commit <= M that changed the app's inputs
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_pin_freshness import APPS, parse_pins, pin_text, resolve_digest  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
ROOT = REPO.parent

# The app -> build-input pathspec table (mirrors the workflows' `paths:` and
# platform/scripts/changed-images.sh).
INPUTS = {
    "nova": ["nova", ".github/workflows/nova-image.yml"],
    "stardelt-operator": ["operator", ".github/workflows/operator-image.yml"],
    "spark": ["platform/images/spark"],
    "superset": ["platform/images/superset"],
    "jupyter": ["platform/images/jupyter", "python", "demos"],
    "airflow": ["demos", ".github/workflows/demos-ci.yml"],
}


def _git(*args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(ROOT), *args], capture_output=True, text=True
    )
    return proc.stdout if proc.returncode == 0 else ""


def builds(app: str, rev: str, limit: int) -> list[str]:
    """The commits at or before `rev` that changed `app`'s inputs, newest first."""
    out = _git("log", f"--format=%H", f"-{limit}", rev, "--", *INPUTS[app])
    return out.split()


def short(commit: str) -> str:
    """The 7-character form the workflows' `sha-` tags carry (`type=sha,format=short`)."""
    out = _git("rev-parse", "--short=7", commit).strip()
    return out or commit[:7]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rev", default="origin/main", help="the commit M to report (default: origin/main)")
    parser.add_argument("--limit", type=int, default=20, help="how many builds back to look for the pin's source")
    args = parser.parse_args(argv)

    if _git("status", "--porcelain").strip():
        print("! the working tree is not clean; the manifests judged are the rev's", file=sys.stderr)

    for app, manifest, kind, tag in APPS:
        pinned = parse_pins(pin_text(REPO, manifest, args.rev)[0], kind)
        commits = builds(app, args.rev, args.limit)

        pin = pinned[0][:15] if pinned else "none"  # "sha256:" + 8 hex, like check_pin_freshness._short
        newest = short(commits[0]) if commits else "none"
        newest_digest, _ = resolve_digest(app, f"sha-{newest}") if commits else (None, "")
        tags = "ok" if newest_digest else "missing"
        newest_match = "yes" if newest_digest and newest_digest in pinned else "no"

        source = ""
        for commit in commits:
            digest, _ = resolve_digest(app, f"sha-{short(commit)}")
            if digest and digest in pinned:
                source = short(commit)
                break

        print(
            f"{app}\tpin={pin}\tnewest={newest}\tnewest_match={newest_match}"
            f"\tsource={source or 'none'}\ttags={tags}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
