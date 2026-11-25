#!/usr/bin/env python3
"""N statements in flight through Nova as the visitor: the third Sizing number (#53, decision 1(a)).

Neither `perf/baseline.py` (straight against Trino with an administrator token) nor
`scripts/perf-smoke.sh` (one statement at a time) measures the path a person feels: N statements
in flight **through Nova as `demo`**, so the token check, the statement API and the engine queue
are all inside the measurement. One level = `repeats` rounds; in a round `level` threads start at
the same instant and each submits `perf-smoke.sh`'s `cpu` statement, timed with
`time.monotonic()` from the `POST /api/statements` to the poll that saw a terminal state —
client wall-clock, like `perf-smoke.sh`, not Trino's own queue time. Standard library only.
The number feeds the Sizing page (#53 decision 4); see perf/README.md.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from threading import Barrier, Lock, Thread

from pipelines import mint_token, summarize, write_json  # platform/perf is sys.path[0] here
from pipelines import parse_levels as _parse_levels


DEFAULT_LEVELS = "1,2,4,8"
IN_FLIGHT = ("PENDING", "RUNNING")
DEMO_PASSWORD = "demo"  # public on purpose: scripts/public-demo.sh creates that person
NAME = "cpu"  # the row of perf-smoke.sh this statement is
# perf-smoke.sh's `cpu` row: needs no table and takes about 0.5 s alone.
STATEMENT = (
    "SELECT count(*), sum(cast(mod(a.x * b.y, 1000000) AS bigint)) "
    "FROM UNNEST(sequence(1, 1000)) AS a(x) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(y)"
)


def parse_levels(raw: str) -> list[int]:
    """`pipelines.parse_levels`, with the empty string (an unset `PERF_LEVELS`) meaning the default."""
    return _parse_levels(raw.strip() or DEFAULT_LEVELS)


def _milliseconds(started: float) -> int:
    return int(round((time.monotonic() - started) * 1000))


def _request(url: str, token: str, method: str = "GET", body: bytes | None = None):
    headers = {"Authorization": f"Bearer {token}"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    return urllib.request.Request(url, data=body, method=method, headers=headers)


def submit(nova: str, token: str, sql: str, timeout: float) -> tuple[dict, int]:
    """POST /api/statements, poll `?waitSeconds=10` until terminal; return (view, milliseconds).

    The milliseconds are `time.monotonic()` from the POST to the answer that was terminal.  A
    refused submit (`429`), a transport failure and a timeout come back as a view with an
    `http-<code>`/`error`/`timeout` state, so a level counts them instead of aborting the run.
    """
    started = time.monotonic()
    data = json.dumps({"sql": sql}).encode()
    try:
        with urllib.request.urlopen(_request(f"{nova}/api/statements", token, "POST", data), timeout=timeout) as response:
            view = json.load(response)
    except urllib.error.HTTPError as exc:
        return {
            "statementId": None,
            "state": "http-%d" % exc.code,
            "error": exc.read().decode("utf-8", "replace")[:300],
        }, _milliseconds(started)
    except (urllib.error.URLError, OSError) as exc:
        return {"statementId": None, "state": "error", "error": str(exc)[:300]}, _milliseconds(started)
    except ValueError:
        return {"statementId": None, "state": "error", "error": "the submit answer is not JSON"}, _milliseconds(started)
    if not isinstance(view, dict):
        return {"statementId": None, "state": "error", "error": "the submit answer is not an object"}, _milliseconds(started)

    statement_id = str(view.get("statementId") or "")
    deadline = started + timeout
    while str(view.get("state")) in IN_FLIGHT:
        if time.monotonic() >= deadline:
            return dict(view, state="timeout", error="still %s after %ss" % (view.get("state"), timeout)), _milliseconds(started)
        try:
            url = f"{nova}/api/statements/{statement_id}?waitSeconds=10"
            with urllib.request.urlopen(_request(url, token), timeout=timeout) as response:
                polled = json.load(response)
        except urllib.error.HTTPError as exc:
            return dict(view, state="http-%d" % exc.code, error=exc.read().decode("utf-8", "replace")[:300]), _milliseconds(started)
        except (urllib.error.URLError, OSError) as exc:
            return dict(view, state="error", error=str(exc)[:300]), _milliseconds(started)
        except ValueError:
            return dict(view, state="error", error="the poll answer is not JSON"), _milliseconds(started)
        if not isinstance(polled, dict):
            return dict(view, state="error", error="the poll answer is not an object"), _milliseconds(started)
        view = polled
    return view, _milliseconds(started)


def free(nova: str, token: str, statement_id: str) -> tuple[bool, str]:
    """DELETE /api/statements/{id}: a person holds at most ten statements.

    Best effort and reported, never raised: the statement is this run's, and a level that
    failed because the DELETE answered 404 is still a measurement.
    """
    try:
        with urllib.request.urlopen(_request(f"{nova}/api/statements/{statement_id}", token, "DELETE"), timeout=60) as response:
            status = response.status
    except urllib.error.HTTPError as exc:
        return False, "DELETE /api/statements/%s answered %d" % (statement_id, exc.code)
    except (urllib.error.URLError, OSError) as exc:
        return False, "DELETE /api/statements/%s: %s" % (statement_id, str(exc)[:300])
    return status in (200, 204), "" if status in (200, 204) else "DELETE answered %d" % status


def one_statement(nova: str, token: str, timeout: float) -> tuple[str, int | None]:
    """The measured unit of a level: (state, milliseconds over the successful run only)."""
    view, milliseconds = submit(nova, token, STATEMENT, timeout)
    state = str(view.get("state"))
    statement_id = str(view.get("statementId") or "")
    if statement_id:
        freed, error = free(nova, token, statement_id)
        if not freed:
            print("perf-concurrency: %s" % error)
    if state != "SUCCEEDED":
        return state, None
    return state, milliseconds


def measure_round(nova: str, token: str, level: int, timeout: float) -> list[tuple[str, int | None]]:
    """One round: `level` threads start at the same instant and wait for all of them."""
    barrier = Barrier(level)
    lock = Lock()
    outcomes: list[tuple[str, int | None] | None] = [None] * level

    def one(index: int) -> None:
        barrier.wait()
        try:
            outcome = one_statement(nova, token, timeout)
        except Exception as exc:  # noqa: BLE001 - an unexpected failure is an error row, not a crash
            outcome = ("error", None)
            print("perf-concurrency: %s" % str(exc)[:300])
        with lock:
            outcomes[index] = outcome

    threads = [Thread(target=one, args=(index,)) for index in range(level)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return [outcome or ("error", None) for outcome in outcomes]


def level_row(level: int, outcomes: list[tuple[str, int | None]]) -> dict[str, object]:
    """The JSON row of one level: `runs` over every round, p50/p95/max over the successes."""
    finished = [milliseconds for state, milliseconds in outcomes if state == "SUCCEEDED" and milliseconds is not None]
    summary = summarize(finished)
    return {
        "level": level,
        "runs": len(outcomes),
        "p50": summary["p50"],
        "p95": summary["p95"],
        "max": summary["max"],
        "errors": len(outcomes) - len(finished),
    }


def _line(row: dict[str, object]) -> str:
    """`level 4  p50=480ms p95=610ms max=640ms errors=0`; `-` when every run of the level failed."""

    def ms(value: object) -> str:
        return "-" if value is None else "%dms" % value

    return "level %s  p50=%s p95=%s max=%s errors=%s" % (
        row["level"],
        ms(row["p50"]),
        ms(row["p95"]),
        ms(row["max"]),
        row["errors"],
    )


def measure_level(nova: str, token: str, level: int, repeats: int, timeout: float) -> dict[str, object]:
    """`repeats` rounds of `level` statements at once; print the level's line and return its row."""
    outcomes: list[tuple[str, int | None]] = []
    for _ in range(repeats):
        outcomes.extend(measure_round(nova, token, level, timeout))
    row = level_row(level, outcomes)
    print(_line(row))
    return row


def document(
    timestamp: str,
    nova: str,
    person: str,
    repeats: int,
    levels: list[dict[str, object]],
) -> dict[str, object]:
    """The JSON the run writes; `note` says what the number is and what it is not."""
    return {
        "timestamp": timestamp,
        "nova": nova,
        "person": person,
        "statement": NAME,
        "repeats": repeats,
        "levels": levels,
        "note": "client wall-clock per statement through Nova; all levels are one signed-in person",
    }


def _output_path(out_dir: str, timestamp: str) -> str:
    path = os.path.join(out_dir, "concurrency-%s.json" % timestamp)
    suffix = 2
    while os.path.exists(path):
        path = os.path.join(out_dir, "concurrency-%s-%d.json" % (timestamp, suffix))
        suffix += 1
    return path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="N statements in flight through Nova as the visitor")
    parser.add_argument("--nova", default=os.environ.get("STARDELT_NOVA_URL") or "https://alpha.dev.stardelt.io",
                        help="Nova's URL (default: $STARDELT_NOVA_URL or the lab)")
    parser.add_argument("--issuer", default=os.environ.get("ISSUER") or "https://id.alpha.dev.stardelt.io/realms/stardelt",
                        help="token issuer (default: $ISSUER or the lab's realm)")
    parser.add_argument("--person", default=os.environ.get("PERF_PERSON") or "demo",
                        help="the person every level is measured as (default: $PERF_PERSON or demo)")
    parser.add_argument("--levels", default=os.environ.get("PERF_LEVELS") or DEFAULT_LEVELS,
                        help="statements in flight, comma separated (default: $PERF_LEVELS or %(default)s)")
    parser.add_argument("--repeats", default=os.environ.get("PERF_REPEATS") or "5",
                        help="rounds per level (default: $PERF_REPEATS or 5)")
    parser.add_argument("--out-dir", default=os.environ.get("PERF_OUT_DIR") or "~/.cache/stardelt-perf",
                        help="where the JSON goes (default: $PERF_OUT_DIR or ~/.cache/stardelt-perf)")
    args = parser.parse_args(argv)

    nova = str(args.nova).rstrip("/")
    issuer = str(args.issuer).rstrip("/")
    person = str(args.person)
    levels = parse_levels(str(args.levels))
    try:
        repeats = int(str(args.repeats))
    except ValueError:
        raise SystemExit("PERF_REPEATS must be a whole number of rounds") from None
    if repeats < 1:
        raise SystemExit("PERF_REPEATS must be at least 1")
    if not levels:
        raise SystemExit("PERF_LEVELS lists no level to run")
    try:
        timeout = float(os.environ.get("PERF_TIMEOUT") or "180")
    except ValueError:
        raise SystemExit("PERF_TIMEOUT must be a number of seconds") from None
    if timeout <= 0:
        raise SystemExit("PERF_TIMEOUT must be positive")
    out_dir = os.path.expanduser(str(args.out_dir))

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

    print("perf-concurrency: %s, %s, %d round(s)" % (person, NAME, repeats))
    rows = [measure_level(nova, token, level, repeats, timeout) for level in levels]

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = _output_path(out_dir, timestamp)
    write_json(path, document(timestamp, nova, person, repeats, rows))
    print("perf-concurrency: JSON at %s" % path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
