#!/usr/bin/env python3
"""Read-only inventory of what a visitor sees on the public demo (stardelt#79 point 2).

Prints three Markdown tables — Pipelines, Stream apps and Spark clusters — with the owner,
schedule, viewers and last run of every object in one namespace. It only reads: three
`kubectl get -o json` calls, no Secret, no applied change. The removal decision for the
demo bundle is taken from this table, so the output is not edited by hand.

The `last run` column comes from Nova — the source a visitor's Pipelines page reads — in one of
three ways (`--runs`):

  nova (default)  a signed-in `stardelt` CLI; when there is no login, the public demo person's
                  token answers instead (the same way `scripts/verify-demo-visitor.sh` reads it).
  demo            only the public demo person's token: no CLI and no login needed, which is what
                  the build plane can run. The password `demo` is PUBLIC ON PURPOSE
                  (`scripts/public-demo.sh` creates that person and prints it), so nothing here
                  prints the token and the two temp files it uses are mode 0600 and removed on
                  every path.
  crd             the CRD's `status.runs`, which nothing writes, so every row answers `never`.

`--check` turns the tables into the convergence list of stardelt#79 and exits 1 on a violation,
including a last-run source that could not answer at all.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

DASH = "—"
NOVA_DEFAULT = "https://alpha.dev.stardelt.io"
# The realm `verify-demo-visitor.sh` signs `demo` into; `ISSUER` overrides it, as there.
ISSUER_DEFAULT = "https://id.alpha.dev.stardelt.io/realms/stardelt"
GROUP = "visitors"

# The twelve one-statement pipelines the shop bundle replaced, verbatim from
# `scripts/demo-shop.sh`; stage 2 of that script deletes exactly this list.
LEGACY = (
    "shop-01-bronze", "shop-02-silver", "shop-03-gold", "shop-04-customers", "shop-05-products", "shop-06-lines",
    "shop-07-stg-cust", "shop-08-stg-prod", "shop-09-stg-lines", "shop-10-revenue", "shop-11-ltv", "shop-12-mix",
)

# The command that converges one kind of demo object. A demo-owned cluster today is the
# sandbox `make install-spark-demo` applies (`manifests/spark-demo-sandbox.yaml`); a
# `demo-cluster` target is only added with the first demo-owned SparkCluster in the
# stardelt namespace (#79).
FIX = {"pipeline": "demo-shop", "stream": "demo-stream", "cluster": "install-spark-demo"}


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


def cli_path():
    """The `stardelt` CLI on PATH, else the venv row 0b of `public-demo-check` accepts."""
    found = shutil.which("stardelt")
    if found:
        return found
    fallback = Path(
        os.environ.get("STARDELT_CLI_VENV", "~/.local/venvs/stardelt-cli")
    ).expanduser() / "bin" / "stardelt"
    return str(fallback) if os.access(fallback, os.X_OK) else None


def nova_last_runs(cli, host, namespace):
    """`{pipeline name: lastRun}` from Nova's Pipelines page; `None` when the CLI cannot answer.

    Nova's `PipelineView.lastRun` is `{state, startedAt, …}` or `null`; the CLI prints the
    `PipelineListResponse` as JSON. Only the named namespace's rows are kept.
    """
    result = subprocess.run(
        [cli, "--host", host, "api", "pipelines_list"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        first = next((line for line in result.stderr.strip().splitlines() if line), "")
        print(f"demo-inventory: the last-run column needs a signed-in CLI ({first})", file=sys.stderr)
        return None
    try:
        rows = json.loads(result.stdout).get("pipelines") or []
    except ValueError as err:
        print(f"demo-inventory: the last-run column needs a signed-in CLI ({err})", file=sys.stderr)
        return None
    return {
        row["name"]: row.get("lastRun")
        for row in rows
        if row.get("namespace") == namespace
    }


def demo_token(issuer):
    """The public demo person's token, or `None` when the sign-in fails.

    `scripts/mint-user-token.py` reads the password from a file, never argv, and never prints the
    token; the password here is `demo`, public on purpose (`scripts/public-demo.sh`). Both temp
    files are mode 0600 and removed on every path.
    """
    password_file = token_file = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as handle:
            password_file = handle.name
            handle.write("demo\n")
        os.chmod(password_file, 0o600)
        fd, token_file = tempfile.mkstemp()
        os.close(fd)
        result = subprocess.run(
            [sys.executable, str(Path(__file__).with_name("mint-user-token.py"))],
            env={
                **os.environ,
                "ISSUER": issuer,
                "USER": "demo",
                "PASSWORD_FILE": password_file,
                "TOKEN_FILE": token_file,
            },
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            first = next((line for line in result.stderr.strip().splitlines() if line), "")
            print(f"demo-inventory: cannot mint the demo token ({first})", file=sys.stderr)
            return None
        return Path(token_file).read_text().strip()
    finally:
        for path in (password_file, token_file):
            if path and os.path.exists(path):
                os.unlink(path)


def runs_from_payload(payload, namespace):
    """`{pipeline name: lastRun}` of one namespace from a `PipelineListResponse`; pure."""
    return {
        row["name"]: row.get("lastRun")
        for row in payload.get("pipelines") or []
        if row.get("namespace") == namespace
    }


def nova_runs_from_demo(nova, namespace, issuer):
    """`{pipeline name: lastRun}` read as the public demo person; `None` on any failure."""
    token = demo_token(issuer)
    if token is None:
        return None
    # The token is written and never read back: it stays out of every line this prints.
    request = urllib.request.Request(
        f"{nova}/api/pipelines", headers={"Authorization": f"Bearer {token}"}
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return runs_from_payload(payload, namespace)
    except Exception as err:  # noqa: BLE001 — any failure is one line, like the CLI path
        first = next((line for line in str(err).strip().splitlines() if line), "")
        print(f"demo-inventory: cannot read Nova's pipelines as demo ({first})", file=sys.stderr)
        return None


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


def _pipeline_row(obj, runs):
    name = _name(obj)
    spec = _spec(obj)
    schedule = spec.get("schedule")
    if schedule:
        cron = schedule.get("cron", "")
        when = f"{cron} {schedule.get('timezone') or 'UTC'}"
    else:
        when = "on demand"
    if name in runs:
        last = runs[name]
        last_run = f"{last.get('state', '')} {last.get('startedAt') or ''}".strip() if last else "never"
    else:
        # The CRD has a pipeline Nova does not list (a resource that is not loaded): fall back
        # to the CRD's own runs, which nothing writes.
        runs_in_crd = _status(obj).get("runs") or []
        if runs_in_crd:
            last = runs_in_crd[-1]
            last_run = f"{last.get('state', '')} {last.get('startedAt', '')}".strip()
        else:
            last_run = "never"
    tasks = ", ".join(task.get("kind", "?") for task in spec.get("tasks") or [])
    owner = spec.get("ownerGroup") or DASH
    return [name, owner, when, _viewers(obj), last_run, tasks]


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


def render(pipelines, streams, clusters, runs=None) -> str:
    """The three tables and the total line; pure, so the tests drive it directly."""
    by_name = lambda obj: _name(obj)  # noqa: E731 — one key for three sorts
    runs = runs or {}
    lines = ["## Pipelines"]
    lines += _table(
        ["pipeline", "owner", "schedule", "viewers", "last run", "tasks"],
        [_pipeline_row(obj, runs) for obj in sorted(pipelines, key=by_name)],
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


def _owner(obj, kind):
    """The group that owns an object: `spec.ownerGroup`, or `spec.owner.group` for a cluster."""
    if kind == "cluster":
        return (_spec(obj).get("owner") or {}).get("group")
    return _spec(obj).get("ownerGroup")


def _demo_objects(pipelines, streams, clusters):
    """Every object the demo group owns, in kind order."""
    return [
        (kind, obj)
        for kind, objects in (("pipeline", pipelines), ("stream", streams), ("cluster", clusters))
        for obj in sorted(objects, key=_name)
        if _owner(obj, kind) == "demo"
    ]


def check(pipelines, streams, clusters, runs) -> list[str]:
    """The convergence list of stardelt#79; pure. `runs` is `None` for `--runs crd`."""
    problems = []
    names = {_name(obj) for obj in pipelines}
    for name in LEGACY:
        if name in names:
            problems.append(
                f"the legacy pipeline {name} is still there — run make demo-shop YES=1"
            )
    demo = _demo_objects(pipelines, streams, clusters)
    for kind, obj in demo:
        if GROUP not in (_spec(obj).get("viewers") or []):
            problems.append(
                f"{kind} {_name(obj)} does not name {GROUP} in its viewers"
                f" — run make {FIX[kind]} YES=1"
            )
    for kind, obj in demo:
        if kind == "pipeline" and not _spec(obj).get("schedule"):
            problems.append(
                f"pipeline {_name(obj)} is on demand; a visitor cannot see a next run"
            )
    if runs is not None:
        for kind, obj in demo:
            if kind != "pipeline" or not _spec(obj).get("schedule"):
                continue
            last = runs.get(_name(obj))
            state = (last or {}).get("state")
            if state != "success":
                problems.append(f"pipeline {_name(obj)}'s last run is {state or 'never'}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only demo inventory (stardelt#79).")
    parser.add_argument(
        "--namespace",
        default=os.environ.get("NAMESPACE", "stardelt"),
        help="namespace to read (default: NAMESPACE, else stardelt)",
    )
    parser.add_argument(
        "--nova",
        default=os.environ.get("STARDELT_NOVA_URL", NOVA_DEFAULT),
        help="Nova's URL for the last-run column (default: STARDELT_NOVA_URL, else the lab)",
    )
    parser.add_argument(
        "--runs",
        choices=("nova", "crd", "demo"),
        default="nova",
        help=(
            "where the last run comes from: nova (the signed-in CLI, the visitor's source), "
            "demo (the public demo person's token; needs no CLI login) or crd; default: nova"
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 unless the demo converged: no legacy pipeline, viewers, a schedule, a green last run",
    )
    args = parser.parse_args()

    runs = None
    if args.runs in ("nova", "demo"):
        issuer = os.environ.get("ISSUER", ISSUER_DEFAULT)
    if args.runs == "nova":
        cli = cli_path()
        if cli is None:
            print(
                "demo-inventory: the stardelt CLI is not installed — see make public-demo-check 0b",
                file=sys.stderr,
            )
        else:
            runs = nova_last_runs(cli, args.nova, args.namespace)
        if runs is None:
            # The build plane has no login: the public demo person's token answers the same
            # question, from the same Nova endpoint the visitor's page reads.
            fallback = nova_runs_from_demo(args.nova, args.namespace, issuer)
            if fallback is not None:
                print(
                    "! no CLI sign-in; the last-run column comes from the public demo person",
                    file=sys.stderr,
                )
                runs = fallback
        if runs is None and args.check:
            return 1
    elif args.runs == "demo":
        runs = nova_runs_from_demo(args.nova, args.namespace, issuer)
        if runs is None and args.check:
            return 1

    pipelines = kubectl("pipelines.stardelt.io", args.namespace)
    streams = kubectl("streamapps.stardelt.io", args.namespace)
    clusters = kubectl("sparkclusters.stardelt.io", args.namespace)
    print(render(pipelines, streams, clusters, runs))
    if not args.check:
        return 0

    problems = check(pipelines, streams, clusters, runs)
    for problem in problems:
        print(f"✗ {problem}")
    if problems:
        print(f"demo-inventory: check FAILED ({len(problems)} problem(s))")
        code = 1
    else:
        print(f"demo-inventory: check OK ({len(_demo_objects(pipelines, streams, clusters))} demo objects)")
        code = 0
    if args.runs == "crd":
        print("demo-inventory: the last-run rule was skipped (--runs crd)")
    return code


if __name__ == "__main__":
    sys.exit(main())
