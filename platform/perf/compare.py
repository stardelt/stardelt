#!/usr/bin/env python3
"""Compare the latest perf-smoke run with the committed baseline (stardelt/stardelt#1522).

`make perf-smoke` writes one JSON per run to `~/.cache/stardelt-perf/<timestamp>.json`
(stardelt/stardelt#1515). This script reads the newest of them and prints one line per
statement next to `perf/perf-baseline.json`, the run a person committed on purpose.

The comparison is advisory: a statement is printed as `SLOW` but the run still exits 0.
`PERF_STRICT=1` turns that into a failed run (exit 1). The dev node is noisy — the first
`scan` run of the baseline was 1007 ms and the next two 458/464 ms — so the number compared
is `p50` and the default threshold is 3x, not 1x.

    python3 perf/compare.py
    python3 perf/compare.py --latest /path/to/run.json
    PERF_STRICT=1 PERF_MAX_RATIO=2.0 python3 perf/compare.py

Standard library only, like the `python3` blocks inside `scripts/perf-smoke.sh`.
"""

import argparse
import json
import os
import sys
from pathlib import Path

DEFAULT_MAX_RATIO = 3.0
LATEST_DIR = Path.home() / ".cache" / "stardelt-perf"
BASELINE = Path(__file__).resolve().parents[1] / "perf" / "perf-baseline.json"


def baseline() -> Path:
    """The committed reference document."""
    return BASELINE


def latest() -> Path:
    """The newest run in `$HOME/.cache/stardelt-perf`; the file name is the timestamp."""
    if not LATEST_DIR.is_dir():
        raise FileNotFoundError("no run directory at %s" % LATEST_DIR)
    candidates = sorted(path for path in LATEST_DIR.glob("*.json") if path.is_file())
    if not candidates:
        raise FileNotFoundError("no *.json run in %s" % LATEST_DIR)
    return candidates[-1]


def p50(statement):
    """The median of one statement document, or None when it is zero or absent."""
    value = statement.get("p50") if isinstance(statement, dict) else statement
    if not isinstance(value, (int, float)) or value <= 0:
        return None
    return value


def ratio(latest_p50, baseline_p50):
    """latest / baseline rounded to one decimal, or None when either side has no p50."""
    latest_value = p50(latest_p50)
    baseline_value = p50(baseline_p50)
    if latest_value is None or baseline_value is None:
        return None
    return round(latest_value / baseline_value, 1)


def verdict(latest_statement, baseline_statement, max_ratio):
    """`ok`, `slow` or `missing` for one statement; never `ok` without a p50 on both sides."""
    value = ratio(latest_statement, baseline_statement)
    if value is None:
        return "missing"
    return "slow" if value > max_ratio else "ok"


def _load(path: Path, kind: str) -> dict:
    if not path.is_file():
        raise FileNotFoundError("%s document not found: %s" % (kind, path))
    with open(path) as handle:
        return json.load(handle)


def _statements(document: dict) -> list:
    statements = document.get("statements")
    return statements if isinstance(statements, list) else []


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare the latest perf-smoke run with perf/perf-baseline.json")
    parser.add_argument("--baseline", type=Path, default=baseline(),
                        help="the committed reference (default: %(default)s)")
    parser.add_argument("--latest", type=Path, default=None,
                        help="the run to compare (default: newest in %s)" % LATEST_DIR)
    args = parser.parse_args(argv)

    try:
        latest_path = args.latest if args.latest is not None else latest()
    except FileNotFoundError as error:
        print("perf-compare: %s" % error)
        return 1

    try:
        baseline_document = _load(args.baseline, "baseline")
        latest_document = _load(latest_path, "latest")
    except (FileNotFoundError, ValueError) as error:
        print("perf-compare: %s" % error)
        return 1

    max_ratio = DEFAULT_MAX_RATIO
    try:
        max_ratio = float(os.environ.get("PERF_MAX_RATIO", DEFAULT_MAX_RATIO))
    except ValueError:
        print("perf-compare: PERF_MAX_RATIO is not a number: %s"
              % os.environ.get("PERF_MAX_RATIO"))
        return 1
    strict = os.environ.get("PERF_STRICT") == "1"

    baseline_statements = {s.get("id"): s for s in _statements(baseline_document)}
    latest_statements = {s.get("id"): s for s in _statements(latest_document)}

    compared = 0
    slow = []
    for statement_id, baseline_statement in baseline_statements.items():
        latest_statement = latest_statements.get(statement_id)
        if latest_statement is None:
            print("%-10s only in baseline" % statement_id)
            continue
        compared += 1
        outcome = verdict(latest_statement, baseline_statement, max_ratio)
        value = ratio(latest_statement, baseline_statement)
        shown_ratio = "-" if value is None else "%.1f" % value
        print("%-10s baseline=%dms  latest=%dms  ratio=%sx  %s" % (
            statement_id,
            p50(baseline_statement) or 0,
            p50(latest_statement) or 0,
            shown_ratio,
            {"ok": "ok", "slow": "SLOW", "missing": "missing"}[outcome]))
        if outcome == "slow":
            slow.append(statement_id)

    for statement_id in latest_statements:
        if statement_id not in baseline_statements:
            print("%-10s only in latest" % statement_id)

    print("perf-compare: %d statement(s) compared, %d slow (max ratio %sx)"
          % (compared, len(slow), max_ratio))
    if strict and slow:
        print("perf-compare: FAIL %s slower than %sx the baseline"
              % (", ".join(slow), max_ratio))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
