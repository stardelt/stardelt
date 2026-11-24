#!/usr/bin/env python3
"""N pipeline runs started at once: start-to-terminal p50/p95 as one person (#53, decision 1(e)).

What a team feels when its schedules fire together: N runs of one pipeline started at the
same moment through Nova, timed with `time.monotonic()` from the `POST …/runs` to the poll
that saw `success`/`failed` — client wall-clock, not Airflow's own `durationSeconds`.
Standard library only. The numbers feed the Sizing page (#53 decision 4); see perf/README.md.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Barrier, Lock, Thread


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"   # platform/scripts

# Nova refuses more than ten starts per user per minute (429), so a level never exceeds this.
MAX_LEVEL = 10
TERMINAL = ("success", "failed")
IN_FLIGHT = ("queued", "running")
DEMO_PASSWORD = "demo"  # public on purpose: scripts/public-demo.sh creates that person


class RunStartError(Exception):
    """`POST …/runs` answered a status that is not a started run."""

    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"POST runs answered {status}: {body}")
        self.status = status
        self.body = body


def percentile(values: list[int], percent: float) -> int | None:
    """Nearest-rank percentile (`baseline.py:85` has the same rule); None on an empty list."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(percent * len(ordered) / 100))
    return ordered[min(rank, len(ordered)) - 1]


def summarize(milliseconds: list[int]) -> dict[str, object]:
    """{"runs": n, "p50": ms, "p95": ms, "max": ms} over the finished runs."""
    return {
        "runs": len(milliseconds),
        "p50": percentile(milliseconds, 50),
        "p95": percentile(milliseconds, 95),
        "max": max(milliseconds) if milliseconds else None,
    }


def parse_levels(raw: str) -> list[int]:
    """PERF_LEVELS="1,2,4" -> [1, 2, 4]; "0" -> []; anything else non-positive or > 10 is a SystemExit."""
    value = raw.strip()
    if value == "0":
        return []
    try:
        levels = [int(part) for part in value.split(",")]
    except ValueError:
        raise SystemExit("PERF_LEVELS must be a comma-separated list of 1..%d" % MAX_LEVEL) from None
    # A level above ten would answer 429 from Nova's starts-per-minute limit, not a measurement.
    if any(level < 1 or level > MAX_LEVEL for level in levels):
        raise SystemExit("PERF_LEVELS must be a comma-separated list of 1..%d" % MAX_LEVEL)
    return levels


def mint_token(nova: str, issuer: str, person: str, password_file: str) -> str:
    """Run scripts/mint-user-token.py with the env `perf-smoke.sh` uses and return the token text."""
    with tempfile.TemporaryDirectory() as tmp:
        token_file = os.path.join(tmp, "token")
        env = dict(os.environ)
        env.update(
            {
                "ISSUER": issuer,
                "USER": person,
                "PASSWORD_FILE": str(password_file),
                "TOKEN_FILE": token_file,
            }
        )
        env.pop("ADMIN_PASSWORD_GRANT", None)
        result = subprocess.run(
            [sys.executable, str(SCRIPTS / "mint-user-token.py")],
            env=env,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            lines = (result.stderr or result.stdout).strip().splitlines()
            raise SystemExit(
                "cannot mint a token for %s: %s" % (person, lines[0] if lines else "no output")
            )
        with open(token_file, encoding="utf-8") as fh:
            return fh.read().strip()


def _get_json(url: str, token: str, timeout: float = 60) -> dict[str, object]:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def start_run(nova: str, token: str, namespace: str, name: str) -> str:
    """POST the run; return its runId, or raise RunStartError(status, body[0:300])."""
    url = f"{nova}/api/pipelines/{namespace}/{name}/runs"
    request = urllib.request.Request(
        url,
        data=b"{}",
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            status = response.status
            body = response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        # A refused run (409/429) is a failed start, not a crash.
        raise RunStartError(exc.code, exc.read().decode("utf-8", "replace")[:300]) from None
    try:
        run_id = (json.loads(body) or {}).get("runId")
    except ValueError:
        run_id = None
    if not run_id:
        raise RunStartError(status, body[:300])
    return str(run_id)


def wait_run(
    nova: str,
    token: str,
    namespace: str,
    name: str,
    run_id: str,
    timeout: float,
    poll: float,
) -> tuple[str, int | None]:
    """Poll until `success`/`failed`; return (state, milliseconds).  A state that is neither
    terminal nor queued/running is a failure, not a success."""
    url = f"{nova}/api/pipelines/{namespace}/{name}/runs/{run_id}"
    started = time.monotonic()
    deadline = started + timeout
    state = "unknown"
    while True:
        try:
            answer = _get_json(url, token)
        except urllib.error.HTTPError as exc:
            return f"http-{exc.code}", _milliseconds(started)
        except urllib.error.URLError:
            return "http-error", _milliseconds(started)
        state = str(((answer.get("run") or {}) if isinstance(answer, dict) else {}).get("state") or "unknown")
        if state in TERMINAL:
            return state, _milliseconds(started)
        if state not in IN_FLIGHT:
            # Airflow never reports anything else; an unknown state is a failure of the run.
            return state, _milliseconds(started)
        if time.monotonic() >= deadline:
            return "timeout", _milliseconds(started)
        time.sleep(poll)


def _milliseconds(started: float) -> int:
    return int(round((time.monotonic() - started) * 1000))


def _seconds(milliseconds: int | None) -> str:
    return "-" if milliseconds is None else "%.1fs" % (milliseconds / 1000.0)


def measure_level(
    nova: str,
    token: str,
    namespace: str,
    name: str,
    level: int,
    timeout: float,
    poll: float,
) -> dict[str, object]:
    """One level: start N runs at the same moment, wait for all, return the dict the JSON records."""
    barrier = Barrier(level)
    lock = Lock()
    outcomes: list[tuple[str, int | None]] = []

    def one() -> None:
        barrier.wait()
        try:
            run_id = start_run(nova, token, namespace, name)
        except RunStartError as exc:
            with lock:
                outcomes.append((f"start-{exc.status}", None))
            return
        state, milliseconds = wait_run(nova, token, namespace, name, run_id, timeout, poll)
        with lock:
            outcomes.append((state, milliseconds))

    threads = [Thread(target=one) for _ in range(level)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    states = [state for state, _ in outcomes]
    finished = [ms for state, ms in outcomes if state == "success" and ms is not None]
    summary = summarize(finished)
    row: dict[str, object] = {
        "level": level,
        "runs": level,
        "ok": len(finished),
        "errors": len(outcomes) - len(finished),
        "p50": summary["p50"],
        "p95": summary["p95"],
        "max": summary["max"],
        "states": states,
    }
    print(
        "level %d  runs=%d  ok=%d  p50=%s  p95=%s  max=%s"
        % (level, level, row["ok"], _seconds(row["p50"]), _seconds(row["p95"]), _seconds(row["max"]))
    )
    return row


def write_json(path: str, payload: dict[str, object]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")


def _output_path(out_dir: str, timestamp: str) -> str:
    path = os.path.join(out_dir, "pipelines-%s.json" % timestamp)
    suffix = 2
    while os.path.exists(path):
        path = os.path.join(out_dir, "pipelines-%s-%d.json" % (timestamp, suffix))
        suffix += 1
    return path


class _StubHandler(BaseHTTPRequestHandler):
    """The stub Nova of `self_test()`: POST starts a run, GET reports it."""

    lock = Lock()
    started = 0
    polls: dict[str, int] = {}
    fail_all = False

    def do_POST(self):  # noqa: N802
        with type(self).lock:
            type(self).started += 1
            run_id = "r-%d" % type(self).started
        self._send({"runId": run_id})

    def do_GET(self):  # noqa: N802
        run_id = self.path.rstrip("/").rsplit("/", 1)[-1]
        with type(self).lock:
            count = type(self).polls.get(run_id, 0) + 1
            type(self).polls[run_id] = count
        if type(self).fail_all:
            state = "failed"
        elif count > 2:
            state = "success"
        else:
            state = "running"
        self._send({"run": {"state": state}, "tasks": []})

    def _send(self, body: dict[str, object]) -> None:
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


def self_test() -> int:
    """A stub http.server: POST answers {"runId": "r-<n>"}, GET answers
    `running` twice then `success`; also one level whose run answers
    `failed`.  Proves the start, the poll, the timing and the error count
    without a cluster.  Exit 0 prints `self-test: PASS (… lines)`."""
    server = HTTPServer(("127.0.0.1", 0), _StubHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        nova = "http://127.0.0.1:%d" % server.server_address[1]
        one = measure_level(nova, "test-token", "stardelt", "shop-1-bronze", 1, 10, 0.01)
        assert one["runs"] == 1 and one["ok"] == 1, f"level 1 did not succeed: {one}"
        assert one["errors"] == 0, f"level 1 counted an error: {one}"
        assert one["states"] == ["success"], f"level 1 states: {one}"
        assert one["p50"] == one["p95"] == one["max"], f"one run's p50/p95/max differ: {one}"
        assert isinstance(one["p50"], int) and one["p50"] >= 0, f"timing missing: {one}"

        _StubHandler.fail_all = True
        two = measure_level(nova, "test-token", "stardelt", "shop-1-bronze", 2, 10, 0.01)
        assert two["runs"] == 2 and two["ok"] == 0, f"level 2 counted a success: {two}"
        assert two["errors"] == 2, f"level 2 error count: {two}"
        assert two["states"] == ["failed", "failed"], f"level 2 states: {two}"
        assert two["p50"] is None and two["p95"] is None and two["max"] is None, (
            f"a failed run produced a latency: {two}"
        )

        assert _StubHandler.started == 3, f"expected 3 starts: {_StubHandler.started}"
        # Two `running` answers then `success` per successful run; a failed run is terminal at once.
        polled = sum(_StubHandler.polls.values())
        assert polled == 3 + 1 + 1, f"expected 5 polls: {polled}"
        print("self-test: PASS (%d level(s), %d run(s))" % (2, 3))
        return 0
    finally:
        server.shutdown()
        _StubHandler.started = 0
        _StubHandler.polls = {}
        _StubHandler.fail_all = False


def main() -> int:
    parser = argparse.ArgumentParser(description="N pipeline runs started at once")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()

    nova = (os.environ.get("STARDELT_NOVA_URL") or "https://alpha.dev.stardelt.io").rstrip("/")
    issuer = (os.environ.get("ISSUER") or "https://id.alpha.dev.stardelt.io/realms/stardelt").rstrip("/")
    person = os.environ.get("PERF_PERSON") or "demo"
    namespace = os.environ.get("PERF_NAMESPACE") or "stardelt"
    name = os.environ.get("PERF_PIPELINE") or "shop-1-bronze"
    levels = parse_levels(os.environ.get("PERF_LEVELS") or "1,2")
    timeout = float(os.environ.get("PERF_TIMEOUT") or "900")
    poll = float(os.environ.get("PERF_POLL") or "2")
    out_dir = os.environ.get("PERF_OUT_DIR") or os.path.expanduser("~/.cache/stardelt-perf")
    if not levels:
        raise SystemExit("PERF_LEVELS lists no level to run")
    if poll <= 0 or timeout <= 0:
        raise SystemExit("PERF_POLL and PERF_TIMEOUT must be positive")

    password_file = tempfile.NamedTemporaryFile("w", suffix=".password", delete=False)
    try:
        os.chmod(password_file.name, 0o600)
        password_file.write(DEMO_PASSWORD + "\n")
        password_file.close()
        token = mint_token(nova, issuer, person, password_file.name)
    finally:
        try:
            os.unlink(password_file.name)
        except OSError:
            pass

    rows = [
        measure_level(nova, token, namespace, name, level, timeout, poll) for level in levels
    ]
    total = sum(int(row["runs"]) for row in rows)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = _output_path(out_dir, timestamp)
    write_json(
        path,
        {
            "timestamp": timestamp,
            "nova": nova,
            "person": person,
            "pipeline": name,
            "levels": rows,
        },
    )

    for row in rows:
        if not row["errors"]:
            continue
        failed = next(state for state in row["states"] if state != "success")
        print(
            "perf-pipelines: FAIL level %d: %d run(s) did not succeed (%s)"
            % (row["level"], row["errors"], failed)
        )
        return 1
    print("perf-pipelines: PASS (%d run(s) as %s on %s)" % (total, person, name))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
