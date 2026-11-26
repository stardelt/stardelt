"""dbt's own result, in words: what a `dbt` task's log says after the run (`#112`).

run_results.json holds one node per entry: `unique_id` (`model.shop.daily_revenue`,
`test.shop.revenue_is_not_negative`), `status` (`success`/`pass`/`fail`/`error`/`warn`/
`skipped`) and, on a failure, the one-line `message`. The summary never changes dbt's exit
code: the entrypoint prints it and exits with dbt's own status.
"""

import json
import os
import sys

# Kinds of node, in the order a reader cares about; any other kind follows alphabetically.
_KIND_ORDER = ("model", "seed", "snapshot", "test")

# Folded status labels, in report order: key, singular, plural, and the dbt statuses it covers.
# `passed` and `failed` read the same either way; a status this table does not name keeps its name.
_STATUS_ORDER = (
    ("passed", "passed", "passed", ("success", "pass")),
    ("failed", "failed", "failed", ("fail",)),
    ("error", "error", "errors", ("error",)),
    ("warning", "warning", "warnings", ("warn", "warning")),
    ("skipped", "skipped", "skipped", ("skipped",)),
)


def _count(noun: str, count: int) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _kinds(results: list[dict]) -> str:
    counts: dict[str, int] = {}
    for result in results:
        kind = str(result.get("unique_id", "")).split(".", 1)[0] or "unknown"
        counts[kind] = counts.get(kind, 0) + 1
    known = [kind for kind in _KIND_ORDER if counts.get(kind)]
    extra = sorted(kind for kind in counts if kind not in _KIND_ORDER)
    parts = [_count(kind, counts[kind]) for kind in known + extra]
    return f"dbt: {_count('node', len(results))} — {', '.join(parts)}"


def _fold(status: str) -> tuple[str, str, str]:
    """Return `(key, singular, plural)` for a dbt status."""
    for key, singular, plural, statuses in _STATUS_ORDER:
        if status in statuses:
            return key, singular, plural
    return status, status, status + "s"


def _label(singular: str, plural: str, count: int) -> str:
    return f"{count} {singular if count == 1 else plural}"


def _statuses(results: list[dict]) -> str:
    folded: dict[str, tuple[str, str, int]] = {}
    for result in results:
        key, singular, plural = _fold(str(result.get("status", "unknown")))
        if key not in folded:
            folded[key] = (singular, plural, 0)
        folded[key] = (singular, plural, folded[key][2] + 1)
    parts = []
    for key, _, _, _ in _STATUS_ORDER:
        if key in folded:
            singular, plural, count = folded.pop(key)
            parts.append(_label(singular, plural, count))
    for singular, plural, count in folded.values():
        parts.append(_label(singular, plural, count))
    return "dbt: " + ", ".join(parts)


def _message(result: dict) -> str:
    message = str(result.get("message") or "").strip().splitlines()
    return message[0] if message else "no message"


def _tables(manifest: dict | None) -> dict[str, str]:
    """`{unique_id: table}` for every node the manifest gives a relation, quotes stripped."""
    tables: dict[str, str] = {}
    for unique_id, node in ((manifest or {}).get("nodes") or {}).items():
        relation = node.get("relation_name")
        if relation:
            tables[unique_id] = str(relation).replace('"', "")
    return tables


def _named(result: dict, nodes: dict, tables: dict[str, str]) -> str:
    """A test's unique_id, and the tables it guards when the manifest names them."""
    unique_id = str(result.get("unique_id", ""))
    node = nodes.get(unique_id) or {}
    guarded = [
        tables[name]
        for name in (node.get("depends_on") or {}).get("nodes") or []
        if name in tables
    ]
    return f"{unique_id} ({', '.join(guarded)})" if guarded else unique_id


def summarize(document: dict, manifest: dict | None = None) -> list[str]:
    """The lines a `dbt` task's log shows for one run_results.json document."""
    results = document.get("results") or []
    if not results:
        return ["dbt: ran 0 nodes"]
    lines = [_kinds(results), _statuses(results)]
    nodes = (manifest or {}).get("nodes") or {}
    tables = _tables(manifest)
    # Failures first, then warnings, each group in `results` order: a reader stops at the reason
    # the task failed and only then looks at what merely warned.
    for result in results:
        if str(result.get("status", "")) in ("fail", "error"):
            lines.append(f"dbt: ✗ {_named(result, nodes, tables)} — {_message(result)}")
    for result in results:
        if str(result.get("status", "")) in ("warn", "warning"):
            lines.append(f"dbt: ! {_named(result, nodes, tables)} — {_message(result)}")
    return lines


def _sibling(path: str, name: str) -> dict | None:
    """The JSON file next to `path`, or None when it is missing or unreadable."""
    try:
        with open(os.path.join(os.path.dirname(path), name), encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def main(argv: list[str]) -> int:
    """Print the summary for the file named on the command line; always return 0.

    `argv` is the script's `sys.argv` (`["run_summary.py", "<path>"]`) from `__main__` and the
    bare `["<path>"]` in the tests: the path is the last word either way.
    """
    path = argv[-1] if argv else ""
    try:
        with open(path, encoding="utf-8") as handle:
            document = json.load(handle)
    except (OSError, ValueError):
        # A run that failed before its first node wrote no run_results.json at all; the task's
        # exit code already says so, and the summary must not turn it into a different one.
        print(f"dbt: no run_results.json at {path}")
        return 0
    for line in summarize(document, _sibling(path, "manifest.json")):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
