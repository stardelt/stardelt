#!/usr/bin/env python3
"""Guard against a new copy of a platform file in the operator, and against drift in the copies
that must stay equal (internal#545 finding 1).

The operator embeds some platform scripts with `include_str!`, and its golden `values/` are a
copy of `helm-values/`. Release night showed what happens when such a pair drifts apart. Every
known copy is a row of `platform/twins.tsv`; this script finds the copies that have no row and
compares the ones marked `exact`. It removes no copy: which one is canonical is internal#545 M1.
"""

from __future__ import annotations

import difflib
import sys
from pathlib import Path

ROWS_PATH = "platform/twins.tsv"
OPERATOR_SRC = "operator/crates/stardelt-operator/src"
OPERATOR_VALUES = "operator/crates/stardelt-operator/values"
PLATFORM_VALUES = "platform/helm-values"
KINDS = ("exact", "drift")
# The copy header a copied file carries instead of the platform file's first lines.
COPY_HEADER = ("# Copy of", "# embedded by", "# injected", "# Edit the platform")


def read_rows(root: Path) -> tuple[list[tuple[str, str, str]], list[str]]:
    """The `(kind, operator path, platform path)` rows, plus a line per malformed one."""
    rows: list[tuple[str, str, str]] = []
    failures: list[str] = []
    path = root / ROWS_PATH
    if not path.is_file():
        return rows, failures
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) != 3 or fields[0].strip() not in KINDS:
            failures.append(
                "✗ bad row: %r — expected exact|drift<TAB>operator path<TAB>platform path" % line
            )
            continue
        rows.append((fields[0].strip(), fields[1].strip(), fields[2].strip()))
    return rows, failures


def body_lines(text: str) -> list[str]:
    """`text` without the leading copy header: the comment lines at the top of the file, after an
    optional shebang, whose first line starts with one of `COPY_HEADER`."""
    lines = text.splitlines()
    start = 1 if lines and lines[0].startswith("#!") else 0
    index = start
    while (
        index < len(lines)
        and lines[index].startswith("#")
        and not lines[index].startswith("#!")
    ):
        index += 1
    if index > start and lines[start].startswith(COPY_HEADER):
        return lines[:start] + lines[index:]
    return lines


def differing_lines(left: list[str], right: list[str]) -> int:
    """How many lines of the two versions differ (removed plus added)."""
    diff = list(difflib.unified_diff(left, right, "operator", "platform", lineterm=""))
    return sum(1 for line in diff[2:] if line[:1] in ("+", "-"))


def named_path(line: str) -> str:
    """The platform path a `# Copy of …` line names, e.g. `stardelt-platform/scripts/x.py (…)`."""
    named = line[len("# Copy of"):].strip()
    for index, char in enumerate(named):
        if char in " \t,":
            named = named[:index]
            break
    if named.startswith("stardelt-platform/"):
        named = "platform/" + named[len("stardelt-platform/"):]
    return named


def found_copies(root: Path) -> list[tuple[str, str]]:
    """`(operator path, platform path)` of every copy the tree contains."""
    copies: list[tuple[str, str]] = []
    values = root / OPERATOR_VALUES
    if values.is_dir():
        for path in sorted(values.glob("*.yaml")):
            platform_path = "%s/%s" % (PLATFORM_VALUES, path.name)
            if (root / platform_path).is_file():
                copies.append(("%s/%s" % (OPERATOR_VALUES, path.name), platform_path))
    src = root / OPERATOR_SRC
    if src.is_dir():
        for path in sorted(src.iterdir()):
            if not path.is_file():
                continue
            try:
                head = path.read_text(encoding="utf-8").splitlines()[:5]
            except UnicodeDecodeError:
                continue
            for line in head:
                if line.startswith("# Copy of"):
                    copies.append(("%s/%s" % (OPERATOR_SRC, path.name), named_path(line)))
                    break
    return copies


def check(root: Path) -> tuple[list[str], int]:
    """One report line per problem, or the `✓` summary line; plus the exit code."""
    rows, lines = read_rows(root)
    listed = {(operator_path, platform_path) for _, operator_path, platform_path in rows}
    for kind, operator_path, platform_path in rows:
        operator_file = root / operator_path
        platform_file = root / platform_path
        if not operator_file.is_file() or not platform_file.is_file():
            lines.append(
                "✗ stale row: %s ↔ %s — one of the two files is gone; drop the row"
                % (operator_path, platform_path)
            )
            continue
        if kind != "exact":
            continue
        left = body_lines(operator_file.read_text(encoding="utf-8"))
        right = platform_file.read_text(encoding="utf-8").splitlines()
        if left != right:
            lines.append(
                "✗ drifted: %s differs from %s in %d line(s) — re-copy the platform file"
                % (operator_path, platform_path, differing_lines(left, right))
            )
    for operator_path, platform_path in found_copies(root):
        if (operator_path, platform_path) not in listed:
            lines.append(
                "✗ new copy: %s ↔ %s — embed the platform file instead, or add a row (internal#545 M1)"
                % (operator_path, platform_path)
            )
    if lines:
        return lines, 1
    exact = sum(1 for kind, _, _ in rows if kind == "exact")
    drift = sum(1 for kind, _, _ in rows if kind == "drift")
    return ["✓ twins: %d exact, %d drift, no new copy" % (exact, drift)], 0


def main(argv=None, root=None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    directory = Path(root) if root is not None else Path(__file__).resolve().parents[2]
    lines, code = check(directory)
    for line in lines:
        print(line)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
