#!/usr/bin/env python3
"""The operator's golden values are compiled into its image, so a pin that predates a read from
`crates/stardelt-operator/values/` renders stale defaults."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# `python3 scripts/check_golden_values.py` puts `scripts/` on sys.path, not its parent: the shared
# helpers live in the `scripts` package, so the platform directory has to be importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.check_golden_image import (  # noqa: E402
    _git,
    _read,
    _short,
    operator_pin,
    resolve_revision,
)

REPO = Path(__file__).resolve().parents[1]
OPERATOR_REPO = "stardelt/stardelt-operator"
OPERATOR_VALUES = "helm-values/operator.yaml"
GOLDEN_DIR = "crates/stardelt-operator/values"
GOLDEN_EXTRA = ("crates/stardelt-operator/src/values.rs",)


def golden_paths(operator_dir: str, revision: str) -> tuple[list[str], str]:
    """The golden value files at `revision`, plus the ones that are not under `GOLDEN_DIR`.

    `([], error)` when the revision cannot be listed.
    """
    if not operator_dir or not revision:
        return [], "no operator checkout or revision to list"
    proc = _git(operator_dir, "ls-tree", "-r", "--name-only", revision, "--", GOLDEN_DIR)
    if proc.returncode != 0:
        return [], proc.stderr.strip() or "cannot list %s at %s" % (GOLDEN_DIR, revision)
    paths = sorted(path for path in proc.stdout.splitlines() if path.strip())
    return paths + list(GOLDEN_EXTRA), ""


def at_revision(operator_dir: str, revision: str, path: str) -> str:
    """The content of `path` at `revision`, or `""` when it cannot be read."""
    if not operator_dir or not revision or not path:
        return ""
    show = _git(operator_dir, "show", "%s:./%s" % (revision, path))
    return show.stdout if show.returncode == 0 else ""


def compare(
    operator_digest: str, revision: str, mismatched: list[str], total: int
) -> tuple[list[str], int]:
    """One `✓`/`✗` line and the exit code (0 all match, 1 some differ, 3 unreadable input)."""
    missing = []
    if not operator_digest:
        missing.append("the operator pin in " + OPERATOR_VALUES)
    if not revision:
        missing.append("the operator image's revision label")
    if total <= 0:
        missing.append("the operator's golden files")
    if missing:
        return ["✗ could not read " + ", ".join(missing)], 3

    if not mismatched:
        return [
            "✓ operator goldens: %d files match the pinned image %s (revision %s)"
            % (total, _short(operator_digest), revision[:7])
        ], 0
    return [
        "✗ operator goldens: %d of %d differ from the pinned image %s (revision %s): %s — "
        "the operator image build of this commit is pending or failed; "
        "re-run the rollup after it lands"
        % (
            len(mismatched),
            total,
            _short(operator_digest),
            revision[:7],
            ", ".join(mismatched),
        )
    ], 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--operator-digest",
        default=None,
        help="the operator image digest to judge (default: the pin in " + OPERATOR_VALUES + ")",
    )
    parser.add_argument(
        "--operator-dir",
        default=None,
        help="the operator checkout (default: $OPERATOR_DIR, else ../operator)",
    )
    args = parser.parse_args(argv)

    operator_dir = (
        args.operator_dir or os.environ.get("OPERATOR_DIR") or str(REPO.parent / "operator")
    )

    operator_digest = (args.operator_digest or "").strip()
    if not operator_digest:
        pins = operator_pin(_read(REPO / OPERATOR_VALUES))
        operator_digest = pins[0] if pins else ""
    elif not operator_digest.startswith("sha256:"):
        operator_digest = "sha256:" + operator_digest

    if not operator_digest:
        lines, code = compare("", "", [], 0)
        for line in lines:
            print(line)
        return code

    revision, err = resolve_revision(OPERATOR_REPO, operator_digest)
    if err:
        print("✗ operator %s: %s" % (_short(operator_digest), err))
        return 3

    paths, err = golden_paths(operator_dir, revision)
    if err:
        print("✗ operator %s revision %s: %s" % (_short(operator_digest), revision[:7], err))
        return 3

    # A file missing from the working tree reads as `""`, so it counts as a mismatch.
    mismatched = [
        path for path in paths if at_revision(operator_dir, revision, path) != _read(Path(operator_dir) / path)
    ]

    lines, code = compare(operator_digest, revision, mismatched, len(paths))
    for line in lines:
        print(line)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
