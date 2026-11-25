#!/usr/bin/env python3
"""One table per perf workload, newest run of each — the release's and the Sizing page's numbers.

Five harnesses write five differently shaped JSON files into
`${PERF_OUT_DIR:-$HOME/.cache/stardelt-perf}`: `scripts/perf-smoke.sh` (`<UTC>.json`),
`perf/pipelines.py`, `perf/ingest.py`, `perf/spark.sh` and `perf/highwater.py`. This reads the
newest file of each kind and prints one `## <source>` block per workload, so a person preparing a
release copies one table instead of retyping five files. `--json PATH` writes the combined
document; that JSON is what a release copies into stardelt-internal.

    python3 perf/report.py
    python3 perf/report.py --json /tmp/perf-report.json
    python3 perf/report.py --self-test

A source with no file is `no run in <dir>` and is not an error on its own; when no source has a
file at all the command exits 1. Standard library only, like the other harnesses.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone

SOURCES = ("smoke", "pipelines", "ingest", "spark", "highwater")

# `scripts/perf-smoke.sh` writes `<UTC>.json` (and `<UTC>-2.json` on a repeat within one second).
SMOKE_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z(?:-\d+)?\.json$")

TIMESTAMP_LENGTH = 20  # len("2026-11-25T00:12:34Z")

DEFAULT_DIR = "~/.cache/stardelt-perf"


def _load(path: str | None) -> dict | None:
    """The JSON object at `path`, or None when it is absent or not an object."""
    if not path:
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            document = json.load(handle)
    except (OSError, ValueError):
        return None
    return document if isinstance(document, dict) else None


def _stamp(entry: str, name: str) -> str:
    """The `<UTC>` part of a run's file name, without the `.json` or the `-2` suffix."""
    stem = entry[: -len(".json")]
    if name != "smoke" and stem.startswith(name + "-"):
        stem = stem[len(name) + 1 :]
    return stem[:TIMESTAMP_LENGTH]


def newest(out_dir, name):
    """The newest file for one source: `<UTC>.json` for smoke (a top-level `statements` key),
    `<name>-<UTC>.json` for the rest. None when there is none; ties break on mtime, then name."""
    if name not in SOURCES:
        raise ValueError("unknown perf source: %s" % name)
    directory = os.path.expanduser(str(out_dir))
    try:
        entries = os.listdir(directory)
    except OSError:
        return None
    best_key = None
    best_path = None
    for entry in entries:
        if not entry.endswith(".json"):
            continue
        path = os.path.join(directory, entry)
        if not os.path.isfile(path):
            continue
        if name == "smoke":
            if not SMOKE_NAME.match(entry):
                continue
            document = _load(path)
            if document is None or not isinstance(document.get("statements"), list):
                continue
        elif not entry.startswith(name + "-"):
            continue
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        key = (_stamp(entry, name), mtime, entry)
        if best_key is None or key > best_key:
            best_key, best_path = key, path
    return best_path


def _rows(document: dict, key: str) -> list:
    """The list of row objects under `key`, skipping anything that is not an object."""
    value = document.get(key)
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def read_smoke(document):
    """{"timestamp", "person", "repeats", "rows": [{"id", "runs", "p50", "p95", "rows", "state"}]}"""
    return {
        "timestamp": document.get("timestamp"),
        "person": document.get("person"),
        "repeats": document.get("repeats"),
        "rows": [
            {
                "id": row.get("id"),
                "runs": len(row.get("runs") or []),
                "p50": row.get("p50"),
                "p95": row.get("p95"),
                "rows": row.get("rows"),
                "state": row.get("state"),
            }
            for row in _rows(document, "statements")
        ],
    }


def read_pipelines(document):
    """{"timestamp", "person", "pipelines": [...], "rows": [{"level", "pipelines", "runs", "ok",
    "p50", "p95", "max"}]}. Accepts the pre-#1533 `pipeline` string."""
    pipelines = document.get("pipelines")
    if not isinstance(pipelines, list):
        pipeline = document.get("pipeline")
        pipelines = [pipeline] if pipeline else []
    rows = []
    for level in _rows(document, "levels"):
        names = level.get("pipelines")
        rows.append(
            {
                "level": level.get("level"),
                "pipelines": names if isinstance(names, list) else pipelines,
                "runs": level.get("runs"),
                "ok": level.get("ok"),
                "p50": level.get("p50"),
                "p95": level.get("p95"),
                "max": level.get("max"),
            }
        )
    return {
        "timestamp": document.get("timestamp"),
        "person": document.get("person"),
        "pipelines": pipelines,
        "rows": rows,
    }


def read_ingest(document):
    """{"timestamp", "table", "rows": [{"rows", "batches", "count", "seconds", "rowsPerSecond",
    "p50"}]}"""
    return {
        "timestamp": document.get("timestamp"),
        "table": document.get("table"),
        "rows": [
            {
                "rows": level.get("rows"),
                "batches": level.get("batches"),
                "count": level.get("count"),
                "seconds": level.get("seconds"),
                "rowsPerSecond": level.get("rowsPerSecond"),
                "p50": level.get("p50"),
            }
            for level in _rows(document, "levels")
        ],
    }


def read_spark(document):
    """{"timestamp", "server", "rows": <input rows>, "rowsPerSecond", "operations": [{"id", "ms",
    "rows", "ok"}]}"""
    return {
        "timestamp": document.get("timestamp"),
        "server": document.get("server"),
        "rows": document.get("rows"),
        "rowsPerSecond": document.get("rowsPerSecond"),
        "operations": [
            {
                "id": operation.get("id"),
                "ms": operation.get("ms"),
                "rows": operation.get("rows"),
                "ok": operation.get("ok"),
            }
            for operation in _rows(document, "operations")
        ],
    }


def read_highwater(document):
    """{"timestamp", "namespace", "samples", "childExit", "rows": [{"pod", "peakCpuM",
    "peakMemoryMiB", "restarts", "oomKilled"}]} — the top five by memory."""
    pods = [
        {
            "pod": pod.get("pod"),
            "peakCpuM": pod.get("peakCpuM"),
            "peakMemoryMiB": pod.get("peakMemoryMiB"),
            "restarts": pod.get("restarts"),
            "oomKilled": pod.get("oomKilled"),
        }
        for pod in _rows(document, "pods")
    ]
    pods.sort(key=lambda row: (-(row.get("peakMemoryMiB") or 0), str(row.get("pod"))))
    return {
        "timestamp": document.get("timestamp"),
        "namespace": document.get("namespace"),
        "samples": document.get("samples"),
        "childExit": document.get("childExit"),
        "rows": pods[:5],
    }


READERS = {
    "smoke": read_smoke,
    "pipelines": read_pipelines,
    "ingest": read_ingest,
    "spark": read_spark,
    "highwater": read_highwater,
}


def report(out_dir):
    """{"generatedAt", "dir", "sources": {name: the reader's dict or None}}"""
    directory = os.path.expanduser(str(out_dir))
    sources = {}
    for name in SOURCES:
        document = _load(newest(directory, name))
        sources[name] = READERS[name](document) if document is not None else None
    return {
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dir": directory,
        "sources": sources,
    }


def _ms(value) -> str:
    """A stored millisecond number: `476ms`, `-` when it is None (a failed run has no p50)."""
    return "-" if value is None else "%dms" % value


def _seconds_from_ms(value) -> str:
    """A stored millisecond number as the writer's own seconds line: `0.5s`."""
    return "-" if value is None else "%.1fs" % (value / 1000.0)


def _seconds(value) -> str:
    """A stored seconds value with one decimal: `0.5s`."""
    return "-" if value is None else "%.1fs" % value


def _plain(value) -> str:
    return "-" if value is None else str(value)


def _names(value) -> str:
    if isinstance(value, list) and value:
        return ", ".join(str(name) for name in value)
    return "-"


def _block_smoke(source: dict) -> list[str]:
    lines = [
        "## smoke  %s  person=%s  repeats=%s"
        % (_plain(source.get("timestamp")), _plain(source.get("person")),
           _plain(source.get("repeats")))
    ]
    for row in source.get("rows") or []:
        lines.append(
            "%-12s runs=%s  p50=%s  p95=%s  rows=%s  %s"
            % (
                _plain(row.get("id")),
                _plain(row.get("runs")),
                _ms(row.get("p50")),
                _ms(row.get("p95")),
                _plain(row.get("rows")),
                _plain(row.get("state")),
            )
        )
    return lines


def _block_pipelines(source: dict) -> list[str]:
    lines = [
        "## pipelines  %s  person=%s"
        % (_plain(source.get("timestamp")), _plain(source.get("person")))
    ]
    for row in source.get("rows") or []:
        lines.append(
            "level %s  runs=%s  ok=%s  p50=%s  p95=%s  max=%s  %s"
            % (
                _plain(row.get("level")),
                _plain(row.get("runs")),
                _plain(row.get("ok")),
                _seconds_from_ms(row.get("p50")),
                _seconds_from_ms(row.get("p95")),
                _seconds_from_ms(row.get("max")),
                _names(row.get("pipelines")),
            )
        )
    return lines


def _block_ingest(source: dict) -> list[str]:
    lines = ["## ingest  %s  table=%s" % (_plain(source.get("timestamp")),
                                          _plain(source.get("table")))]
    for row in source.get("rows") or []:
        lines.append(
            "rows=%s  batches=%s  count=%s  seconds=%s  rowsPerSecond=%s  p50=%s"
            % (
                _plain(row.get("rows")),
                _plain(row.get("batches")),
                _plain(row.get("count")),
                _seconds(row.get("seconds")),
                _plain(row.get("rowsPerSecond")),
                _ms(row.get("p50")),
            )
        )
    return lines


def _block_spark(source: dict) -> list[str]:
    lines = [
        "## spark  %s  server=%s  rows=%s"
        % (_plain(source.get("timestamp")), _plain(source.get("server")),
           _plain(source.get("rows")))
    ]
    for operation in source.get("operations") or []:
        rows = operation.get("rows")
        counted = "-" if rows is None else "%s row%s" % (rows, "" if rows == 1 else "s")
        line = "%s  %s  %s" % (_plain(operation.get("id")), _ms(operation.get("ms")), counted)
        if operation.get("ok") is False:
            line += "  FAILED"
        lines.append(line)
    return lines


def _block_highwater(source: dict) -> list[str]:
    lines = [
        "## highwater  %s  namespace=%s  samples=%s"
        % (
            _plain(source.get("timestamp")),
            _plain(source.get("namespace")),
            _plain(source.get("samples")),
        )
    ]
    for row in (source.get("rows") or [])[:5]:
        lines.append(
            "%s  %sm  %sMi%s"
            % (
                _plain(row.get("pod")),
                _plain(row.get("peakCpuM")),
                _plain(row.get("peakMemoryMiB")),
                " (!)" if (row.get("restarts") or 0) > 0 or row.get("oomKilled") else "",
            )
        )
    return lines


BLOCKS = {
    "smoke": _block_smoke,
    "pipelines": _block_pipelines,
    "ingest": _block_ingest,
    "spark": _block_spark,
    "highwater": _block_highwater,
}


def render(result):
    """The text: one `## <source>` block per source that has a file, `no run in <dir>` for an
    absent one."""
    directory = result.get("dir") or ""
    lines = []
    for name in SOURCES:
        source = (result.get("sources") or {}).get(name)
        if source is None:
            lines.append("## %s: no run in %s" % (name, directory))
        else:
            lines.extend(BLOCKS[name](source))
    return "\n".join(lines)


def self_test() -> int:
    """Five small fixture documents through report() and render(): the newest of each kind is
    found, the pre-#1533 pipelines document still prints, the highwater block stops at five and
    marks an OOM, an absent number is `-`, and an empty directory gives five None sources."""
    stamp = "2026-11-25T00:12:34Z"
    with tempfile.TemporaryDirectory() as tmp:
        fixtures = {
            "%s.json" % stamp: {
                "timestamp": stamp,
                "person": "demo",
                "repeats": 3,
                "statements": [
                    {"id": "count", "runs": [10, 11, 12], "p50": 11, "p95": 12, "rows": 1,
                     "state": "SUCCEEDED"},
                ],
            },
            "pipelines-%s.json" % stamp: {
                "timestamp": stamp,
                "person": "demo",
                "pipeline": "shop-1-bronze",
                "levels": [
                    {"level": 1, "runs": 1, "ok": 1, "errors": 0, "p50": 500, "p95": 500,
                     "max": 500, "states": ["success"]},
                    {"level": 2, "runs": 2, "ok": 0, "errors": 2, "p50": None, "p95": None,
                     "max": None, "states": ["failed", "failed"]},
                ],
            },
            "ingest-%s.json" % stamp: {
                "timestamp": stamp,
                "table": "playground.scratch.perf_ingest",
                "levels": [
                    {"level": 1, "rows": 10000, "ok": 1, "batches": 1, "count": 10000,
                     "seconds": 0.476, "rowsPerSecond": 21008, "p50": 476},
                ],
            },
            "spark-%s.json" % stamp: {
                "timestamp": stamp,
                "server": "spark-connect.stardelt.svc.cluster.local:15002",
                "rows": 5000000,
                "rowsPerSecond": 2370,
                "operations": [
                    {"id": "session", "ms": 1, "rows": 1, "ok": True},
                    {"id": "join", "ms": 1637, "rows": 100000, "ok": False},
                ],
            },
            "highwater-%s.json" % stamp: {
                "timestamp": stamp,
                "namespace": "stardelt",
                "samples": 12,
                "childExit": 0,
                "pods": [
                    {"pod": "p%d" % i, "peakCpuM": 100 * i, "peakMemoryMiB": 1000 - 10 * i,
                     "restarts": 0, "oomKilled": 0}
                    for i in range(7)
                ],
            },
        }
        # The OOM pod is the largest, so it survives the top five.
        fixtures["highwater-%s.json" % stamp]["pods"].append(
            {"pod": "oom-pod", "peakCpuM": 50, "peakMemoryMiB": 4096, "restarts": 2,
             "oomKilled": 1}
        )
        for name, document in fixtures.items():
            with open(os.path.join(tmp, name), "w", encoding="utf-8") as handle:
                json.dump(document, handle)

        result = report(tmp)
        found = [name for name in SOURCES if result["sources"][name] is not None]
        assert found == list(SOURCES), "sources found: %s" % found

        pipelines = result["sources"]["pipelines"]
        assert pipelines["rows"][0]["pipelines"] == ["shop-1-bronze"], pipelines

        highwater = result["sources"]["highwater"]
        assert len(highwater["rows"]) == 5, highwater
        assert highwater["rows"][0]["pod"] == "oom-pod", highwater

        text = render(result)
        assert "(!)" in text, text
        assert "p50=-" in text, text
        assert text.count("## ") == 5, text

        with tempfile.TemporaryDirectory() as empty:
            blank = report(empty)
        assert all(blank["sources"][name] is None for name in SOURCES), blank

        print("self-test: PASS (%d source(s), %d fixture(s))" % (len(found), len(fixtures)))
        return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="One table per perf workload, newest run of each (#53)")
    parser.add_argument("--dir", default=os.environ.get("PERF_OUT_DIR") or DEFAULT_DIR,
                        help="where the runs are (default: %(default)s)")
    parser.add_argument("--json", metavar="PATH", help="write the combined document")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if args.self_test:
        return self_test()

    result = report(args.dir)
    if not any(source is not None for source in result["sources"].values()):
        print("perf-report: FAIL no perf run in %s — run make perf-smoke (or another workload) first"
              % result["dir"])
        return 1

    print(render(result))
    if args.json:
        path = os.path.expanduser(args.json)
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2)
            handle.write("\n")
        print("perf-report: JSON at %s" % path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
