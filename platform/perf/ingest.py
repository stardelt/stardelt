#!/usr/bin/env python3
"""Rows per second into Iceberg through the interactive path — the Sizing page's ingest column (#53).

A `%sql` task in a pipeline runs on Trino, so the Trino write path **is** the product's ingest
path. One level = R rows inserted into one Iceberg table as `demo`, wall-clock from the first
submitted `INSERT` to the last answer, reported as rows per second. Standard library only.
The number feeds the Sizing page (#53 decision 4); see perf/README.md.

Three facts shape the loop, each measured on the lab 2026-11-25:

- `sequence()` refuses more than 10 000 entries (`INVALID_FUNCTION_ARGUMENT`), so a level of R
  rows is `ceil(R / 10000)` statements of at most 10 000 rows each.
- A person holds at most ten statements in Nova (`429` on the eleventh), so every submitted
  statement is freed with `DELETE /api/statements/{id}` before the next one.
- `demo` may `CREATE TABLE`, `INSERT` and `DELETE FROM` in `playground.scratch` but may not
  `DROP TABLE`: the table is created once and left empty for `demo-reset`.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import tempfile
import time
import urllib.error
import urllib.request
from contextlib import redirect_stdout
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Lock, Thread

from pipelines import mint_token, percentile, write_json  # platform/perf is sys.path[0] here


MAX_BATCH = 10000  # Trino's sequence() cap, measured 2026-11-25
TERMINAL = ("SUCCEEDED", "FAILED", "CANCELED")
IN_FLIGHT = ("PENDING", "RUNNING")
DEMO_PASSWORD = "demo"  # public on purpose: scripts/public-demo.sh creates that person


def batches_for(rows: int) -> list[int]:
    """[10000, 10000, 5000] for 25000: each batch ≤ MAX_BATCH, the last one carries the rest."""
    if rows <= 0:
        raise SystemExit("PERF_ROWS must list positive row counts (got %r)" % (rows,))
    batches = []
    left = int(rows)
    while left > 0:
        batches.append(min(MAX_BATCH, left))
        left -= batches[-1]
    return batches


def rows_per_second(rows: int, seconds: float) -> int:
    """int(rows / seconds); SystemExit when seconds is 0 or negative."""
    if seconds <= 0:
        raise SystemExit("cannot divide %d rows by %r seconds" % (rows, seconds))
    return int(rows / seconds)


def _request(url: str, token: str, method: str = "GET", body: bytes | None = None):
    headers = {"Authorization": f"Bearer {token}"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    return urllib.request.Request(url, data=body, method=method, headers=headers)


def submit(nova: str, token: str, sql: str, timeout: float = 180) -> dict:
    """POST /api/statements; poll until terminal; return the last StatementView. `error` is a str.

    A transport failure is returned as a view with an `http-<code>`/`error` state rather than
    raised, so the caller can still free whatever the submit created.
    """
    data = json.dumps({"sql": sql, "waitSeconds": int(timeout)}).encode()
    try:
        with urllib.request.urlopen(_request(f"{nova}/api/statements", token, "POST", data), timeout=timeout) as response:
            view = json.load(response)
    except urllib.error.HTTPError as exc:
        return {
            "statementId": None,
            "state": "http-%d" % exc.code,
            "error": exc.read().decode("utf-8", "replace")[:300],
        }
    except (urllib.error.URLError, OSError) as exc:
        return {"statementId": None, "state": "error", "error": str(exc)[:300]}
    except ValueError:
        return {"statementId": None, "state": "error", "error": "the submit answer is not JSON"}
    if not isinstance(view, dict):
        return {"statementId": None, "state": "error", "error": "the submit answer is not an object"}

    statement_id = str(view.get("statementId") or "")
    deadline = time.monotonic() + timeout
    while str(view.get("state")) in IN_FLIGHT:
        if time.monotonic() >= deadline:
            return dict(view, state="timeout", error="still %s after %ss" % (view.get("state"), timeout))
        try:
            url = f"{nova}/api/statements/{statement_id}?waitSeconds=10"
            with urllib.request.urlopen(_request(url, token), timeout=timeout) as response:
                polled = json.load(response)
        except urllib.error.HTTPError as exc:
            return dict(view, state="http-%d" % exc.code, error=exc.read().decode("utf-8", "replace")[:300])
        except (urllib.error.URLError, OSError) as exc:
            return dict(view, state="error", error=str(exc)[:300])
        except ValueError:
            return dict(view, state="error", error="the poll answer is not JSON")
        if not isinstance(polled, dict):
            return dict(view, state="error", error="the poll answer is not an object")
        view = polled
    return view


def free(nova: str, token: str, statement_id: str) -> None:
    """DELETE /api/statements/{id} (204). A held statement blocks the person at ten."""
    if not statement_id:
        raise SystemExit("free: the statement has no statementId")
    try:
        with urllib.request.urlopen(_request(f"{nova}/api/statements/{statement_id}", token, "DELETE"), timeout=60) as response:
            status = response.status
    except urllib.error.HTTPError as exc:
        raise SystemExit("DELETE /api/statements/%s answered %d" % (statement_id, exc.code)) from None
    if status not in (200, 204):
        raise SystemExit("DELETE /api/statements/%s answered %d" % (statement_id, status))


def execute(nova: str, token: str, sql: str) -> dict:
    """submit + free + SystemExit when the state is not SUCCEEDED (the message carries the error)."""
    view = submit(nova, token, sql)
    statement_id = str(view.get("statementId") or "")
    if statement_id:
        free(nova, token, statement_id)
    if str(view.get("state")) != "SUCCEEDED":
        raise SystemExit("%s answered %s: %s" % (_short(sql), view.get("state"), view.get("error")))
    return view


def _short(sql: str, limit: int = 60) -> str:
    """The first words of a statement, for an error line."""
    flat = " ".join(sql.split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _first_value(view: dict) -> object:
    """The single cell of a one-row answer, or None."""
    rows = view.get("rows")
    if isinstance(rows, list) and rows and isinstance(rows[0], list) and rows[0]:
        return rows[0][0]
    return None


def run_level(nova: str, token: str, table: str, rows: int) -> dict:
    """DELETE the table's rows, time `batches_for(rows)` inserts, verify count(*), DELETE again.

    The level's seconds run from the first insert's `POST` to the last insert's terminal answer
    (`time.monotonic()`), so the reset and the count are outside it.  Prints one line and raises
    SystemExit when a statement failed or the count is not `rows`.
    """
    batches = batches_for(rows)
    execute(nova, token, "DELETE FROM %s" % table)

    durations: list[int] = []
    errors: list[str] = []
    offset = 0
    started = time.monotonic()
    for size in batches:
        sql = "INSERT INTO %s SELECT i + %d AS i FROM UNNEST(sequence(1, %d)) AS t(i)" % (table, offset, size)
        begin = time.monotonic()
        view = submit(nova, token, sql)
        durations.append(int(round((time.monotonic() - begin) * 1000)))
        statement_id = str(view.get("statementId") or "")
        if str(view.get("state")) != "SUCCEEDED":
            errors.append("%s: %s" % (view.get("state"), view.get("error")))
        if statement_id:
            try:
                free(nova, token, statement_id)
            except SystemExit as exc:
                errors.append(str(exc))
        offset += size
    seconds = max(round(time.monotonic() - started, 3), 0.001)

    counted = _first_value(execute(nova, token, "SELECT count(*) FROM %s" % table))
    try:
        count = int(counted)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        count = -1
    # The table is left empty: `demo` may not DROP it, and demo-reset only clears the schema.
    execute(nova, token, "DELETE FROM %s" % table)

    ok = 1 if not errors and count == rows else 0
    row = {
        "rows": rows,
        "ok": ok,
        "batches": len(batches),
        "count": count,
        "seconds": seconds,
        "rowsPerSecond": rows_per_second(rows, seconds),
        "p50": percentile(durations, 50),
    }
    print(
        "rows=%d  ok=%d  batches=%d  count=%d  p50=%sms  rows/s=%d"
        % (row["rows"], row["ok"], row["batches"], row["count"], row["p50"], row["rowsPerSecond"])
    )
    if errors:
        raise SystemExit("%d statement(s) failed (%s)" % (len(errors), errors[0]))
    if count != rows:
        raise SystemExit("count is %d, expected %d" % (count, rows))
    return row


def _output_path(out_dir: str, timestamp: str) -> str:
    path = os.path.join(out_dir, "ingest-%s.json" % timestamp)
    suffix = 2
    while os.path.exists(path):
        path = os.path.join(out_dir, "ingest-%s-%d.json" % (timestamp, suffix))
        suffix += 1
    return path


class _StubHandler(BaseHTTPRequestHandler):
    """The stub Nova of `self_test()`: POST runs a statement, GET reports it, DELETE frees it."""

    lock = Lock()
    submitted = 0
    inserted = 0
    freed: list[str] = []
    sql_of: dict[str, str] = {}
    fail_inserts = False

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            body = {}
        sql = str((body or {}).get("sql") or "")
        with type(self).lock:
            type(self).submitted += 1
            statement_id = "s-%d" % type(self).submitted
            type(self).sql_of[statement_id] = sql
            lower = sql.strip().lower()
            rows = []
            state = "SUCCEEDED"
            error = None
            if lower.startswith("insert"):
                if type(self).fail_inserts:
                    state = "FAILED"
                    error = "INVALID_FUNCTION_ARGUMENT: stub refuses the insert"
                else:
                    type(self).inserted += _inserted_rows(sql)
            elif lower.startswith("delete"):
                type(self).inserted = 0
            elif lower.startswith("select count(*)"):
                rows = [[type(self).inserted]]
            self._send(
                200,
                {
                    "statementId": statement_id,
                    "state": state,
                    "error": error,
                    "columns": [],
                    "rowCount": len(rows),
                    "chunks": 1,
                    "truncated": False,
                    "rows": rows,
                },
            )

    def do_GET(self):  # noqa: N802
        statement_id = self.path.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
        with type(self).lock:
            sql = type(self).sql_of.get(statement_id, "")
            rows = [[type(self).inserted]] if sql.lower().startswith("select count(*)") else []
        self._send(
            200,
            {
                "statementId": statement_id,
                "state": "SUCCEEDED",
                "error": None,
                "columns": [],
                "rowCount": len(rows),
                "chunks": 1,
                "truncated": False,
                "rows": rows,
            },
        )

    def do_DELETE(self):  # noqa: N802
        statement_id = self.path.rstrip("/").rsplit("/", 1)[-1]
        with type(self).lock:
            type(self).freed.append(statement_id)
        self.send_response(204)
        self.end_headers()

    def _send(self, status: int, body: dict[str, object]) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


def _inserted_rows(sql: str) -> int:
    """The `sequence(1, N)` of a stub INSERT: the rows it claims to have written."""
    match = re.search(r"sequence\(1,\s*(\d+)\)", sql)
    return int(match.group(1)) if match else 0


def self_test() -> int:
    """A stub http.server: POST runs a statement, GET reports it, DELETE frees it.

    Level 1 is one batch, level 2 is two (`15000` -> `[10000, 5000]`), so the batching, the
    time arithmetic, the count check and the free-after-each-statement are proven without a
    cluster; a canned FAILED insert proves the exit.  Exit 0 prints
    `self-test: PASS (2 level(s), 3 statements)`.
    """
    server = HTTPServer(("127.0.0.1", 0), _StubHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        nova = "http://127.0.0.1:%d" % server.server_address[1]
        table = "playground.scratch.perf_ingest"

        one = run_level(nova, "test-token", table, 10000)
        assert one["ok"] == 1 and one["count"] == 10000, one
        assert one["batches"] == 1, one
        assert one["seconds"] > 0, one
        assert one["rowsPerSecond"] == rows_per_second(10000, one["seconds"]), one
        assert isinstance(one["p50"], int) and one["p50"] >= 0, one

        captured = io.StringIO()
        with redirect_stdout(captured):
            two = run_level(nova, "test-token", table, 15000)
        assert two["ok"] == 1 and two["count"] == 15000, two
        assert two["batches"] == 2, two
        assert "rows=15000" in captured.getvalue(), captured.getvalue()

        # Three INSERTs (1 + 2 batches); every statement was freed exactly once.
        assert _StubHandler.inserted == 0, _StubHandler.inserted  # the last level emptied the table
        freed = list(_StubHandler.freed)
        assert sorted(freed) == sorted(_StubHandler.sql_of), (
            "a statement was not freed: %d statements, %d frees" % (len(_StubHandler.sql_of), len(freed))
        )
        assert len(freed) == len(set(freed)), "a statement was freed twice: %s" % freed

        _StubHandler.fail_inserts = True
        try:
            with redirect_stdout(io.StringIO()):
                run_level(nova, "test-token", table, 10000)
        except SystemExit as exc:
            assert "statement(s) failed" in str(exc), exc
        else:
            raise AssertionError("a FAILED insert did not raise SystemExit")

        print("self-test: PASS (%d level(s), %d statements)" % (2, 3))
        return 0
    finally:
        server.shutdown()
        _StubHandler.submitted = 0
        _StubHandler.inserted = 0
        _StubHandler.freed = []
        _StubHandler.sql_of = {}
        _StubHandler.fail_inserts = False


def parse_rows(raw: str) -> list[int]:
    """PERF_ROWS="10000,50000" -> [10000, 50000]; anything non-positive is a SystemExit."""
    try:
        rows = [int(part) for part in raw.split(",")]
    except ValueError:
        raise SystemExit("PERF_ROWS must be a comma-separated list of positive row counts") from None
    if not rows or any(row <= 0 for row in rows):
        raise SystemExit("PERF_ROWS must be a comma-separated list of positive row counts")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Rows per second into Iceberg as demo")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()

    nova = (os.environ.get("STARDELT_NOVA_URL") or "https://alpha.dev.stardelt.io").rstrip("/")
    issuer = (os.environ.get("ISSUER") or "https://id.alpha.dev.stardelt.io/realms/stardelt").rstrip("/")
    person = os.environ.get("PERF_PERSON") or "demo"
    table = os.environ.get("PERF_TABLE") or "playground.scratch.perf_ingest"
    sizes = parse_rows(os.environ.get("PERF_ROWS") or "10000,50000")
    try:
        timeout = float(os.environ.get("PERF_TIMEOUT") or "180")
    except ValueError:
        raise SystemExit("PERF_TIMEOUT must be a number of seconds") from None
    if timeout <= 0:
        raise SystemExit("PERF_TIMEOUT must be positive")
    out_dir = os.environ.get("PERF_OUT_DIR") or os.path.expanduser("~/.cache/stardelt-perf")

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

    # `demo` may CREATE but not DROP, so the table is created once and emptied by every level.
    execute(nova, token, "CREATE TABLE IF NOT EXISTS %s (i BIGINT)" % table)

    levels = []
    for index, rows in enumerate(sizes):
        try:
            row = run_level(nova, token, table, rows)
        except SystemExit as exc:
            print("perf-ingest: FAIL level %d: %s" % (index + 1, exc))
            return 1
        levels.append({"level": index + 1, **row})

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = _output_path(out_dir, timestamp)
    write_json(
        path,
        {
            "timestamp": timestamp,
            "nova": nova,
            "person": person,
            "table": table,
            "levels": levels,
        },
    )

    total = sum(int(level["rows"]) for level in levels)
    seconds = sum(float(level["seconds"]) for level in levels)
    print(
        "perf-ingest: PASS (%d rows in %.1f s, %d rows/s as %s into %s)"
        % (total, seconds, rows_per_second(total, seconds), person, table)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
