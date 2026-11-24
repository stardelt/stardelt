#!/usr/bin/env python3
"""Peak CPU and memory per pod while one workload runs (#53, decision 3).

Sizing needs a **high-water mark**, not an instant: the Spark driver's peak falls between
two `kubectl top` samples, and an `OOMKilled` only shows in a pod's `lastState`. This wraps
a child command (`perf/pipelines.py` by default), samples `kubectl top pods` every
`PERF_SAMPLE` seconds until the child exits, then takes one final sample and the pods'
`restartCount`/`lastState`. Standard library only.

Each run writes one JSON to `${PERF_OUT_DIR:-$HOME/.cache/stardelt-perf}/highwater-<UTC>.json`
with `{"timestamp", "namespace", "samples", "startedAt", "childExit", "pods": [...]}`; each
pod row is `{"pod", "peakCpuM", "peakMemoryMiB", "restarts", "oomKilled"}`. The numbers feed
the Sizing page (#53 decision 4); see perf/README.md.

`python3 perf/highwater.py -- <command…>` measures a run;
`python3 perf/highwater.py --self-test` proves the parsing with canned text, no cluster.
"""

from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

DEFAULT_SAMPLE_SECONDS = 2.0

MI = 1024 * 1024


def _mebibytes(text: str) -> int:
    """'363Mi' -> 363, '1Gi' -> 1024, '512Ki' -> 1 (rounded up), '1048576' -> 1."""
    value = text.strip()
    for suffix, factor in (("Ti", 1024 * 1024), ("Gi", 1024), ("Mi", 1), ("Ki", 1 / 1024)):
        if value.endswith(suffix):
            return int(math.ceil(int(value[: -len(suffix)]) * factor))
    return int(math.ceil(int(value) / MI))


def _millicores(text: str) -> int:
    """'6m' -> 6, '1' -> 1000, '500000n' -> 1. A byte suffix is read as MiB (rounded up)."""
    value = text.strip()
    if value.endswith("m"):
        return int(value[:-1])
    if value.endswith("n"):
        return int(math.ceil(int(value[:-1]) / 1_000_000))
    if value.endswith("u"):
        return int(math.ceil(int(value[:-1]) / 1_000))
    if value.endswith(("Ki", "Mi", "Gi", "Ti")):
        return _mebibytes(value)
    return int(value) * 1000


def parse_top(text: str) -> dict[str, tuple[int, int]]:
    """`kubectl top pods --no-headers` -> {pod: (cpu millicores, memory MiB)}.

    '6m' -> 6, '1' -> 1000; '363Mi' -> 363, '1Gi' -> 1024, '512Ki' -> 1 (rounded up);
    a row with '<unknown>' is skipped, and a trailing namespace column is ignored.
    """
    pods: dict[str, tuple[int, int]] = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        name, raw_cpu, raw_memory = fields[0], fields[1], fields[2]
        if "<unknown>" in (raw_cpu, raw_memory):
            continue
        try:
            pods[name] = (_millicores(raw_cpu), _mebibytes(raw_memory))
        except ValueError:
            # An unparsable row is no sample, not a zero.
            continue
    return pods


def summarize(
    samples: list[dict[str, tuple[int, int]]],
    restarts: dict[str, dict[str, int]] | None = None,
) -> list[dict]:
    """One row per pod: {"pod", "peakCpuM", "peakMemoryMiB", "restarts", "oomKilled"}.

    Peak is the maximum over the samples; a pod that disappears keeps its last row.
    Sorted by peak memory, descending.
    """
    peaks: dict[str, tuple[int, int]] = {}
    for snapshot in samples:
        for pod, (cpu, memory) in snapshot.items():
            last = peaks.get(pod)
            if last is None:
                peaks[pod] = (cpu, memory)
            else:
                peaks[pod] = (max(last[0], cpu), max(last[1], memory))
    counted = restarts or {}
    rows = []
    for pod, (cpu, memory) in peaks.items():
        seen = counted.get(pod) or {}
        rows.append(
            {
                "pod": pod,
                "peakCpuM": cpu,
                "peakMemoryMiB": memory,
                "restarts": int(seen.get("restarts", 0)),
                "oomKilled": 1 if seen.get("oomKilled") else 0,
            }
        )
    rows.sort(key=lambda row: (-row["peakMemoryMiB"], row["pod"]))
    return rows


def _kubectl(args: list[str], timeout: float = 30) -> str | None:
    """Run kubectl and return stdout; None when it fails for any reason (never raises)."""
    try:
        result = subprocess.run(["kubectl", *args], capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return result.stdout


def sample(namespace: str) -> dict[str, tuple[int, int]]:
    """Run `kubectl -n NS top pods --no-headers` and parse it; {} when kubectl fails."""
    output = _kubectl(["-n", namespace, "top", "pods", "--no-headers"])
    if output is None:
        return {}
    return parse_top(output)


def restarts_of(namespace: str) -> dict[str, dict[str, int]]:
    """`kubectl -n NS get pods -o json` -> {pod: {"restarts": sum(restartCount),
    "oomKilled": 1|0}} from lastState.terminated.reason == 'OOMKilled'."""
    output = _kubectl(["-n", namespace, "get", "pods", "-o", "json"], timeout=60)
    if output is None:
        return {}
    try:
        listing = json.loads(output)
    except ValueError:
        return {}
    counted: dict[str, dict[str, int]] = {}
    for item in (listing or {}).get("items") or []:
        name = ((item.get("metadata") or {}).get("name")) or ""
        if not name:
            continue
        statuses = (item.get("status") or {}).get("containerStatuses") or []
        total = 0
        oom = 0
        for status in statuses:
            total += int(status.get("restartCount") or 0)
            terminated = ((status.get("lastState") or {}).get("terminated")) or {}
            if terminated.get("reason") == "OOMKilled":
                oom = 1
        counted[name] = {"restarts": total, "oomKilled": oom}
    return counted


def _print_table(rows: list[dict]) -> None:
    print("%-4s  %-48s  %7s  %8s  %8s  %s" % ("peak", "pod", "CPU(m)", "MEM(MiB)", "restarts", "OOM"))
    for row in rows:
        print(
            "%-4d  %-48s  %7d  %8d  %8d  %s"
            % (
                row["peakMemoryMiB"],
                row["pod"],
                row["peakCpuM"],
                row["peakMemoryMiB"],
                row["restarts"],
                "yes" if row["oomKilled"] else "no",
            )
        )


def _output_path(out_dir: str, timestamp: str) -> str:
    path = os.path.join(out_dir, "highwater-%s.json" % timestamp)
    suffix = 2
    while os.path.exists(path):
        path = os.path.join(out_dir, "highwater-%s-%d.json" % (timestamp, suffix))
        suffix += 1
    return path


def self_test() -> int:
    """Canned `top` text and a canned pod list through parse_top/summarize; 0 on success."""
    canned = [
        (
            "spark-connect-6d9f8c9f7f-abcde   12m   1590Mi\n"
            "trino-coordinator-55c95f5bdd-26mwc   120m   812Mi\n"
        ),
        (
            "spark-connect-6d9f8c9f7f-abcde   40m   1200Mi\n"
            "trino-coordinator-55c95f5bdd-26mwc   80m   700Mi\n"
        ),
        # The last sample is the smallest: the peak must survive it.
        (
            "spark-connect-6d9f8c9f7f-abcde   3m   900Mi\n"
            "trino-coordinator-55c95f5bdd-26mwc   130m   640Mi\n"
        ),
    ]
    samples = [parse_top(text) for text in canned]
    assert samples[0]["spark-connect-6d9f8c9f7f-abcde"] == (12, 1590), samples[0]
    assert samples[0]["trino-coordinator-55c95f5bdd-26mwc"] == (120, 812), samples[0]

    canned_restarts = {
        "spark-connect-6d9f8c9f7f-abcde": {"restarts": 0, "oomKilled": 0},
        "trino-coordinator-55c95f5bdd-26mwc": {"restarts": 2, "oomKilled": 1},
    }
    rows = summarize(samples, canned_restarts)
    assert [row["pod"] for row in rows] == [
        "spark-connect-6d9f8c9f7f-abcde",
        "trino-coordinator-55c95f5bdd-26mwc",
    ], rows
    spark = rows[0]
    assert spark["peakCpuM"] == 40 and spark["peakMemoryMiB"] == 1590, spark
    trino = rows[1]
    assert trino["peakCpuM"] == 130 and trino["peakMemoryMiB"] == 812, trino
    assert trino["restarts"] == 2 and trino["oomKilled"] == 1, trino
    print("highwater self-test: PASS (%d pods, %d samples)" % (len(rows), len(samples)))
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()
    if "--" not in argv:
        raise SystemExit("usage: highwater.py -- <command…>")
    child_command = argv[argv.index("--") + 1 :]
    if not child_command:
        raise SystemExit("usage: highwater.py -- <command…>")

    namespace = os.environ.get("PERF_NAMESPACE") or "stardelt"
    out_dir = os.environ.get("PERF_OUT_DIR") or os.path.expanduser("~/.cache/stardelt-perf")
    try:
        interval = float(os.environ.get("PERF_SAMPLE") or DEFAULT_SAMPLE_SECONDS)
    except ValueError:
        raise SystemExit("PERF_SAMPLE must be a number of seconds") from None
    if interval <= 0:
        raise SystemExit("PERF_SAMPLE must be positive")

    started = datetime.now(timezone.utc)
    samples: list[dict[str, tuple[int, int]]] = []
    child = subprocess.Popen(child_command)
    next_at = time.monotonic()
    while child.poll() is None:
        snapshot = sample(namespace)
        if snapshot:
            samples.append(snapshot)
        next_at += interval
        delay = next_at - time.monotonic()
        if delay > 0:
            time.sleep(min(delay, interval))
    final = sample(namespace)
    if final:
        samples.append(final)
    counted = restarts_of(namespace)
    rows = summarize(samples, counted)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = _output_path(out_dir, timestamp)
    os.makedirs(out_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "timestamp": timestamp,
                "namespace": namespace,
                "samples": len(samples),
                "startedAt": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "childExit": child.returncode,
                "pods": rows,
            },
            fh,
            indent=2,
        )
        fh.write("\n")

    _print_table(rows)
    print("highwater: %d pods, %d samples, JSON at %s" % (len(rows), len(samples), path))
    return child.returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
