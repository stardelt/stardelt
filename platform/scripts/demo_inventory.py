#!/usr/bin/env python3
"""Read-only inventory of what a visitor sees on the public demo (stardelt#79 point 2).

Prints three Markdown tables — Pipelines, Stream apps and Spark clusters — with the owner,
schedule, viewers and last run of every object in one namespace. It only reads: three
`kubectl get -o json` calls, no Secret, no applied change. The removal decision for the
demo bundle is taken from this table, so the output is not edited by hand.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys

DASH = "—"


def kubectl(kind, namespace):
    """The `items` of `kubectl get <kind> -o json`; a failed call ends the script."""
    result = subprocess.run(
        ["kubectl", "-n", namespace, "get", kind, "-o", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        first = next((line for line in result.stderr.strip().splitlines() if line), "")
        print(f"demo-inventory: kubectl get {kind} failed: {first}", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout).get("items") or []


def _cell(value):
    """One Markdown table cell; an absent value renders as a dash."""
    text = DASH if value in (None, "") else str(value)
    return text.replace("|", "\\|")


def _name(obj):
    return obj.get("metadata", {}).get("name", "")


def _spec(obj):
    return obj.get("spec") or {}


def _status(obj):
    return obj.get("status") or {}


def _viewers(obj):
    viewers = _spec(obj).get("viewers") or []
    return ", ".join(viewers) if viewers else DASH


def _table(header, rows):
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for row in rows:
        lines.append("| " + " | ".join(_cell(value) for value in row) + " |")
    return lines


def _pipeline_row(obj):
    spec = _spec(obj)
    schedule = spec.get("schedule")
    if schedule:
        cron = schedule.get("cron", "")
        when = f"{cron} {schedule.get('timezone') or 'UTC'}"
    else:
        when = "on demand"
    runs = _status(obj).get("runs") or []
    if runs:
        last = runs[-1]
        last_run = f"{last.get('state', '')} {last.get('startedAt', '')}".strip()
    else:
        last_run = "never"
    tasks = ", ".join(task.get("kind", "?") for task in spec.get("tasks") or [])
    owner = spec.get("ownerGroup") or DASH
    return [_name(obj), owner, when, _viewers(obj), last_run, tasks]


def _stream_row(obj):
    status = _status(obj)
    return [
        _name(obj),
        _spec(obj).get("ownerGroup") or DASH,
        _viewers(obj),
        status.get("phase") or DASH,
        status.get("lastCheckpointAt") or DASH,
    ]


def _cluster_row(obj):
    spec = _spec(obj)
    owner = spec.get("owner") or {}
    who = owner.get("group") or owner.get("user") or DASH
    return [_name(obj), who, _viewers(obj), _status(obj).get("phase") or DASH]


def render(pipelines, streams, clusters) -> str:
    """The three tables and the total line; pure, so the tests drive it directly."""
    by_name = lambda obj: _name(obj)  # noqa: E731 — one key for three sorts
    lines = ["## Pipelines"]
    lines += _table(
        ["pipeline", "owner", "schedule", "viewers", "last run", "tasks"],
        [_pipeline_row(obj) for obj in sorted(pipelines, key=by_name)],
    )
    lines += ["", "## Stream apps"]
    lines += _table(
        ["stream", "owner", "viewers", "phase", "last checkpoint"],
        [_stream_row(obj) for obj in sorted(streams, key=by_name)],
    )
    lines += ["", "## Spark clusters"]
    lines += _table(
        ["cluster", "owner", "viewers", "phase"],
        [_cluster_row(obj) for obj in sorted(clusters, key=by_name)],
    )
    lines += [
        "",
        f"Total: {len(pipelines)} pipelines, {len(streams)} streams, {len(clusters)} clusters.",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only demo inventory (stardelt#79).")
    parser.add_argument(
        "--namespace",
        default=os.environ.get("NAMESPACE", "stardelt"),
        help="namespace to read (default: NAMESPACE, else stardelt)",
    )
    args = parser.parse_args()
    pipelines = kubectl("pipelines.stardelt.io", args.namespace)
    streams = kubectl("streamapps.stardelt.io", args.namespace)
    clusters = kubectl("sparkclusters.stardelt.io", args.namespace)
    print(render(pipelines, streams, clusters))
    return 0


if __name__ == "__main__":
    sys.exit(main())
