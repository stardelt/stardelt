"""Tests for stardelt.evals: ask, run read-only, post the verdict (#125 slice 3)."""

import io
import types

import pytest

from stardelt import cli, evals
from tests.test_api import FakeResponse, FakeSession

EVAL = {"id": 7, "ownerGroup": "analytics", "question": "how many orders?", "expected": {"kind": "value", "value": 3}, "createdAt": "2026-11-27T00:00:00.000Z"}


class FakeNova:
    def __init__(self, evals, answers, runs=None):
        self.evals, self.answers, self.calls, self.runs = evals, answers, [], runs or {}

    def call(self, operation, body=None, path=None, query=None):
        self.calls.append((operation, body, path, query))
        if operation == "catalog_eval_list":
            return self.evals
        if operation == "ask_answer":
            return self.answers.pop(0)
        if operation == "eval_run_create":
            return {"id": len(self.calls), "passed": "error" not in (body or {}), "reason": (body or {}).get("error")}
        raise AssertionError(f"unexpected {operation}")


def result(rows, truncated=False):
    return types.SimpleNamespace(rows=rows, truncated=truncated)


def test_an_answer_that_asks_well_is_posted_with_its_rows(monkeypatch):
    nova = FakeNova([EVAL], [{"model": "m", "endpoint": "e", "sql": "select 3", "sentence": "three"}])
    monkeypatch.setattr(evals.statements, "run", lambda client, sql: result([[3]]))
    body = evals.ask_and_run(nova, EVAL, "gold")
    assert body == {"model": "m", "endpoint": "e", "sql": "select 3", "rows": [[3]]}
    assert nova.calls[0] == ("ask_answer", {"question": "how many orders?"}, None, {"catalog": "gold"})


def test_an_answer_without_sql_is_posted_as_an_error(monkeypatch):
    nova = FakeNova([EVAL], [{"model": "m", "endpoint": "e", "sql": None, "sentence": "I found no table"}])
    monkeypatch.setattr(evals.statements, "run", lambda client, sql: pytest.fail("nothing to run"))
    body = evals.ask_and_run(nova, EVAL, "gold")
    assert body["error"] == "I found no table"
    assert "rows" not in body and "sql" not in body


def test_a_truncated_answer_never_passes(monkeypatch):
    nova = FakeNova([EVAL], [{"model": "m", "endpoint": "e", "sql": "select * from t"}])
    monkeypatch.setattr(evals.statements, "run", lambda client, sql: result([[1]], truncated=True))
    assert evals.ask_and_run(nova, EVAL, "gold")["error"] == "the answer was truncated"


def test_run_returns_the_rows_and_the_verdict(monkeypatch):
    failing = {**EVAL, "id": 8, "question": "how many refunds?"}
    nova = FakeNova(
        [EVAL, failing],
        [{"model": "m", "endpoint": "e", "sql": "select 3"}, {"model": "m", "endpoint": "e", "sql": None, "sentence": "no table"}],
    )
    monkeypatch.setattr(evals.statements, "run", lambda client, sql: result([[3]]))
    pairs = evals.run(nova, "gold")
    assert [(ev["id"], row["passed"]) for ev, row in pairs] == [(7, True), (8, False)]


def _cli_session(*responses):
    return FakeSession(*responses)


def test_the_cli_exits_one_when_an_eval_fails(monkeypatch):
    done = {"statementId": "q1", "state": "SUCCEEDED", "columns": [{"name": "n", "type": "bigint"}], "rows": [[3]], "chunks": 1, "truncated": False}
    session = _cli_session(
        FakeResponse(200, [EVAL, {**EVAL, "id": 8, "question": "how many refunds?"}]),
        FakeResponse(200, {"model": "m", "endpoint": "e", "sql": "select 3", "sentence": "three"}),
        FakeResponse(200, done),
        FakeResponse(204, None),
        FakeResponse(201, {"id": 1, "evalId": 7, "passed": True}),
        FakeResponse(200, {"model": "m", "endpoint": "e", "sql": None, "sentence": "no table"}),
        FakeResponse(201, {"id": 2, "evalId": 8, "passed": False, "reason": "no table"}),
    )
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(
        ["evals", "run", "--catalog", "gold"],
        environ={"STARDELT_NOVA_URL": "https://nova.example", "STARDELT_TOKEN": "t"},
        stdout=out,
        stderr=err,
        session=session,
    )
    assert code == 1, err.getvalue()
    assert "PASS" in out.getvalue() and "FAIL" in out.getvalue()
    assert [call[2]["json"]["question"] for call in session.calls if call[1].endswith("/api/ask")] == [
        "how many orders?",
        "how many refunds?",
    ]
    assert session.calls[1][2]["params"] == {"catalog": "gold"}
