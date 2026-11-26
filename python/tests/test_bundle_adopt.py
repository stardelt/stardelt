"""``stardelt bundle plan --adopt`` and ``deploy --adopt``: taking over a pipeline made in Nova (#1888).

No network: a fake session stands in for Nova's answers, in the order the commands ask them.
"""

import io
import json

from stardelt import cli
from tests.test_api import CANARY, FakeResponse, FakeSession
from tests.test_bundle import make

ENV = {"STARDELT_NOVA_URL": "https://nova.example", "STARDELT_TOKEN": CANARY}
ME = FakeResponse(payload={"user": "alice"})
CREATE = {"ok": True, "action": "create", "errors": [], "changes": [], "nextRuns": []}
ADOPT = {"ok": True, "action": "update", "errors": [], "changes": ["statement"], "nextRuns": [], "adopt": True}
PLAN_OK = {"ok": True, "errors": [], "warnings": [], "changes": [], "files": [], "planId": "abc"}
NAMED = "load was made in Nova, not by a bundle; pass --adopt load to take it over"


def run(argv, *responses):
    out, err = io.StringIO(), io.StringIO()
    session = FakeSession(*responses)
    code = cli.main(argv, environ=ENV, stdout=out, stderr=err, session=session, stdin=io.StringIO(""))
    return code, out.getvalue(), err.getvalue(), session


def calls(session):
    return [(method, url.removeprefix("https://nova.example")) for method, url, _ in session.calls]


def test_plan_refuses_a_pipeline_made_in_nova_and_writes_nothing(tmp_path):
    make(tmp_path)
    code, _, err, session = run(
        ["bundle", "plan", str(tmp_path), "-t", "prod"], ME, FakeResponse(payload=ADOPT), FakeResponse(payload=CREATE)
    )
    assert code == 2
    assert err == f"stardelt: prod: {NAMED}\n"
    assert calls(session) == [
        ("GET", "/api/me"), ("POST", "/api/pipelines/validate"), ("POST", "/api/pipelines/validate")
    ], "a plan only asks Nova"


def test_adopt_takes_over_a_pipeline_made_in_nova_and_the_plan_says_adopted(tmp_path):
    make(tmp_path)
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod", "--adopt", "load"],
        ME, FakeResponse(payload=ADOPT), FakeResponse(payload=CREATE), FakeResponse(payload=PLAN_OK),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt", "ownerGroup": "sales"}]}),
        FakeResponse(payload={"name": "load"}),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt", "ownerGroup": "sales"}]}),
        FakeResponse(201, {"name": "nightly"}),
        FakeResponse(payload={"pipelines": [
            {"name": "load", "namespace": "stardelt"}, {"name": "nightly", "namespace": "stardelt"},
        ]}),
        FakeResponse(201, {"id": 11, "bundle": "sales-etl", "target": "prod", "status": "applied"}),
    )
    assert (code, err) == (0, "")
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "updated", "name": "load", "kind": "Pipeline"},
        {"action": "created", "name": "nightly", "kind": "Pipeline"},
        {"action": "recorded", "id": 11},
    ]
    stamped = next(call for call in session.calls if call[0] == "PUT" and call[1].endswith("/pipelines/stardelt/load"))
    assert stamped[2]["json"]["provenance"]["bundle"] == "sales-etl", "the adopt stamps the pipeline as the bundle's"

    # The same adopt in a plan: the row says `adopted`.
    code, out, err, _ = run(
        ["bundle", "plan", str(tmp_path), "-t", "prod", "--adopt", "load", "--format", "markdown"],
        ME, FakeResponse(payload=ADOPT), FakeResponse(payload=CREATE), FakeResponse(payload=[]),
    )
    assert (code, err) == (0, "")
    assert "| would-update | Pipeline | load | statement; adopted |" in out


def test_adopt_of_a_name_the_bundle_would_not_adopt_is_refused(tmp_path):
    make(tmp_path)
    code, _, err, session = run(
        ["bundle", "plan", str(tmp_path), "-t", "prod", "--adopt", "nightly"],
        ME, FakeResponse(payload=ADOPT), FakeResponse(payload=CREATE),
    )
    assert code == 2
    assert "--adopt nightly: nothing in this bundle would adopt that pipeline" in err
    assert len(calls(session)) == 3, "still only the checks: nothing was written"
