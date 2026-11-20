#!/usr/bin/env python3
"""Did a fresh node pass at this commit? (stardelt/stardelt#620, slice of #2)

A release is cut at a commit that a fresh node has proven: `platform/scripts/e2e-fresh-node.sh`
writes `$HOME/.cache/stardelt-e2e/<run id>/report.txt`, whose `commit:` line names the commit
the run tested and whose last line says `e2e-fresh-node: PASS (…)`. This script reads those
reports and answers with an exit code, so `scripts/release/cut.sh` can refuse a commit that no
run has passed at.

    python3 scripts/release/check_fresh_node.py <commit> [--dir DIR]

Exit codes: 0 a report passed at <commit>, 1 no report did (or DIR does not exist).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

DEFAULT_DIR = Path.home() / ".cache" / "stardelt-e2e"


def reports(directory: Path) -> list[Path]:
    """Every `DIR/*/report.txt`, in a stable order."""
    try:
        children = sorted(directory.iterdir())
    except OSError:
        return []
    return [child / "report.txt" for child in children if (child / "report.txt").is_file()]


def lines_of(report: Path) -> list[str]:
    """The report's lines, or none when it cannot be read."""
    try:
        return report.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []


def passed_at(report: Path, commit: str) -> bool:
    """True when this report names `commit` exactly and ends in a PASS line."""
    lines = lines_of(report)
    if f"commit: {commit}" not in lines:
        return False
    for line in reversed(lines):
        if line.strip():
            return line.startswith("e2e-fresh-node: PASS")
    return False


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Check that a fresh-node run passed at this commit.",
        usage="check_fresh_node.py <commit> [--dir DIR]",
    )
    parser.add_argument("commit", help="the full 40-character commit SHA")
    parser.add_argument(
        "--dir",
        default=str(DEFAULT_DIR),
        help=f"where the runs write their reports (default: {DEFAULT_DIR})",
    )
    args = parser.parse_args(argv)
    directory = Path(args.dir).expanduser()

    for report in reports(directory):
        if passed_at(report, args.commit):
            print(f"✓ fresh-node run {report.parent.name} passed at {args.commit[:12]}")
            return 0

    print(
        f"✗ no passing fresh-node run for {args.commit[:12]} in {directory} — run: "
        f"YES=1 FRESH_DOMAIN=<domain> bash platform/scripts/e2e-fresh-node.sh up",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
