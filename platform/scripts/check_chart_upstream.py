#!/usr/bin/env python3
"""Every pinned chart in chart/Chart.yaml still exists in its upstream Helm repository.

Read-only and offline-testable: fetch() is injected in the tests. Exit 0 = every pinned
version is published; 1 = at least one is missing; 3 = at least one repository could not be
read (UNVERIFIED, not a pass). A deprecated chart is reported and does not fail: #1558
decided to stay on Superset's chart for 0.0.7 and see the deprecation each release.

The parity half (the Makefile, Chart.yaml and the values agree) is
scripts/check-version-parity.sh; this is the upstream half.
"""

from __future__ import annotations

import argparse
import sys
import urllib.request
from functools import partial
from pathlib import Path

import yaml

INDEX = "index.yaml"
TIMEOUT = 20
CHART = Path("chart/Chart.yaml")


def index_url(repository):
    """The Helm index URL of one repository (`<repository>/index.yaml`)."""
    return repository.rstrip("/") + "/" + INDEX


def dependencies(path=CHART):
    """[(name, repository, version)] from a chart's Chart.yaml, in file order."""
    chart = yaml.safe_load(Path(path).read_text())
    return [
        (d["name"], d["repository"], str(d["version"]))
        for d in chart.get("dependencies") or []
    ]


def evaluate(name, version, entries):
    """One row dict: name, version, present, newest, deprecated (bool), app_version.

    `entries` is a Helm index's `entries` mapping (chart name -> its releases, newest first).
    """
    releases = (entries or {}).get(name) or []
    newest = releases[0].get("version") if releases else None
    pinned = next(
        (e for e in releases if str(e.get("version")) == str(version)), None
    )
    return {
        "name": name,
        "version": version,
        "present": pinned is not None,
        "newest": str(newest) if newest is not None else None,
        "deprecated": bool(pinned.get("deprecated")) if pinned else False,
        "app_version": pinned.get("appVersion") if pinned else None,
    }


def fetch(url, timeout=TIMEOUT):
    """The index text; raises OSError on any network or HTTP failure."""
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8")


def rows_for(deps, fetch=fetch):
    """One row per dependency; a fetch failure becomes {"name", "error"}."""
    rows = []
    for name, repository, version in deps:
        url = index_url(repository)
        try:
            index = yaml.safe_load(fetch(url)) or {}
        except Exception as error:  # OSError, HTTP errors, a broken YAML index
            first = str(error).splitlines()[0] if str(error) else type(error).__name__
            rows.append({"name": name, "version": version, "url": url,
                         "error": first or type(error).__name__})
            continue
        row = evaluate(name, version, index.get("entries") or {})
        row["url"] = url
        rows.append(row)
    return rows


def exit_code(rows):
    """1 when any row is missing; 3 when any row failed to fetch and none is missing; else 0."""
    if any("error" not in row and not row["present"] for row in rows):
        return 1
    if any("error" in row for row in rows):
        return 3
    return 0


def report(rows):
    """The ✓/!/?/✗ lines and the summary; returns nothing."""
    for row in rows:
        name, version = row["name"], row["version"]
        newest = row.get("newest") or "none"
        if "error" in row:
            print("? {} {} UNVERIFIED: {}".format(name, version, row["error"]))
        elif not row["present"]:
            print("✗ {} {} is not in {} (newest {})".format(
                name, version, row.get("url", "its Helm repository"), newest))
        elif row["deprecated"]:
            print("! {} {} is deprecated upstream (newest {})".format(
                name, version, newest))
        else:
            print("✓ {} {} published (newest {})".format(name, version, newest))
    total = len(rows)
    published = sum(1 for row in rows if row.get("present"))
    deprecated = sum(1 for row in rows if row.get("present") and row["deprecated"])
    missing = sum(1 for row in rows if "error" not in row and not row["present"])
    unreadable = sum(1 for row in rows if "error" in row)
    if missing:
        print("check-chart-upstream: FAIL ({} missing of {})".format(missing, total))
    elif unreadable:
        print("check-chart-upstream: UNVERIFIED ({} of {} not readable)".format(
            unreadable, total))
    else:
        print("check-chart-upstream: PASS ({} of {} published; {} deprecated)".format(
            published, total, deprecated))


def main(argv=None, fetch=fetch):
    """Parse --chart NAME (optional) and --timeout; print the report; return exit_code()."""
    parser = argparse.ArgumentParser(
        prog="check-chart-upstream",
        description="Every pinned chart in chart/Chart.yaml is still published upstream.")
    parser.add_argument("--chart", help="only this dependency (exact chart name)")
    parser.add_argument("--timeout", type=int, default=TIMEOUT,
                        help="seconds per index request (default {})".format(TIMEOUT))
    args = parser.parse_args(argv)

    deps = dependencies()
    if args.chart is not None:
        deps = [dep for dep in deps if dep[0] == args.chart]
        if not deps:
            parser.error("--chart {}: no such dependency in {}".format(args.chart, CHART))

    rows = rows_for(deps, fetch=partial(fetch, timeout=args.timeout))
    report(rows)
    return exit_code(rows)


if __name__ == "__main__":
    raise SystemExit(main())
