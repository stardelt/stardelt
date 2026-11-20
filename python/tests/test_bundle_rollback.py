"""``stardelt bundle rollback``: replay exactly what one recorded deployment applied. No network: a fake session."""

import io
import json

from stardelt import cli
from tests.test_api import CANARY, FakeResponse, FakeSession

ENV = {"STARDELT_NOVA_URL": "https://nova.example", "STARDELT_TOKEN": CANARY}
CREATE = {"ok": True, "action": "create", "errors": [], "changes": [], "nextRuns": []}
REFUSED = {
    "ok": False,
    "action": "create",
    "errors": [{"message": "you are not a member of the group sales"}],
    "changes": [],
    "nextRuns": [],
}
LOAD = {"name": "load", "ownerGroup": "sales", "sql": "select 1"}
NIGHTLY = {"name": "nightly", "ownerGroup": "sales", "sql": "select 2"}
ROW = {
    "id": 3,
    "bundle": "sales-etl",
    "ownerGroup": "sales",
    "target": "prod",
    "status": "applied",
    "revision": "abc1234",
    "repository": "https://example/repo",
    "resources": [LOAD],
}


def run(argv, *responses):
    out, err = io.StringIO(), io.StringIO()
    session = FakeSession(*responses)
    code = cli.main(argv, environ=ENV, stdout=out, stderr=err, session=session, stdin=io.StringIO(""))
    return code, out.getvalue(), err.getvalue(), session


def calls(session):
    return [(method, url.removeprefix("https://nova.example")) for method, url, _ in session.calls]


def test_rollback_replays_the_stored_documents_and_records_itself():
    code, out, err, session = run(
        ["bundle", "rollback", "--deployment", "3", "--yes"],
        FakeResponse(payload=ROW), FakeResponse(payload=CREATE),
        FakeResponse(payload={"pipelines": []}), FakeResponse(201, {"name": "load"}),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 9}),
    )
    assert (code, err) == (0, "")
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "created", "name": "load"},
        {"action": "recorded", "id": 9},
    ]
    record = session.calls[-1][2]["json"]
    assert (record["bundle"], record["ownerGroup"], record["target"], record["revision"]) == ("sales-etl", "sales", "prod", "abc1234")
    assert record["status"] == "applied"
    assert record["resources"] == [LOAD]
    assert record["previousDeploymentId"] == 3
    assert calls(session) == [
        ("GET", "/api/deployments/3"), ("POST", "/api/pipelines/validate"),
        ("GET", "/api/pipelines"), ("POST", "/api/pipelines"),
        ("GET", "/api/pipelines"), ("POST", "/api/deployments"),
    ]


def test_the_preview_checks_every_document_and_writes_nothing():
    code, out, err, session = run(
        ["bundle", "rollback", "--deployment", "3"], FakeResponse(payload=ROW), FakeResponse(payload=CREATE)
    )
    assert code == 0
    assert [json.loads(line) for line in out.splitlines()] == [{"action": "would-create", "name": "load"}]
    assert err == "stardelt: nothing was written; add --yes to replay deployment 3\n"
    written = [f"{method} {url}" for method, url in calls(session)]
    assert "POST /api/pipelines" not in written
    assert "POST /api/pipelines/validate" in written
    assert "POST /api/deployments" not in written


def test_a_failed_deploy_has_nothing_to_replay():
    code, out, err, session = run(
        ["bundle", "rollback", "--deployment", "3", "--yes"], FakeResponse(payload={**ROW, "status": "failed"})
    )
    assert (code, out) == (2, "")
    assert "deployment 3 is failed: there is nothing to roll back to" in err
    assert len(session.calls) == 1, "nothing is checked and nothing is written"


def test_a_deployment_nova_does_not_know_is_one_sentence():
    code, out, err, session = run(
        ["bundle", "rollback", "--deployment", "77", "--yes"],
        FakeResponse(404, {"error": "no deployment 77 among the ones you may see"}),
    )
    assert (code, out) == (2, "")
    assert "no deployment 77 among the ones you may see" in err
    assert len(session.calls) == 1


def test_a_refusal_stops_at_the_first_document_and_records_the_attempt_as_failed():
    code, out, err, session = run(
        ["bundle", "rollback", "--deployment", "3", "--yes"],
        FakeResponse(payload={**ROW, "resources": [LOAD, NIGHTLY]}), FakeResponse(payload=REFUSED),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 10}),
    )
    assert code == 2
    assert "load: you are not a member of the group sales" in err
    assert "POST /api/pipelines" not in [f"{method} {url}" for method, url in calls(session)]
    record = session.calls[-1][2]["json"]
    assert record["status"] == "failed"
    assert record["resources"] == [LOAD, NIGHTLY]
