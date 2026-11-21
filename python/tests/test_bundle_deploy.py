"""``stardelt bundle plan`` and ``deploy`` (stardelt.bundle.render and deploy). No network: a fake session stands in."""

import hashlib
import io
import json
import shutil
import subprocess

import pytest

from stardelt import api, bundle, cli
from tests.test_api import CANARY, FakeResponse, FakeSession
from tests.test_bundle import LOAD as LOAD_YAML
from tests.test_bundle import MANIFEST, STREAM, make

ENV = {"STARDELT_NOVA_URL": "https://nova.example", "STARDELT_TOKEN": CANARY}
ME = FakeResponse(payload={"user": "alice"})
CREATE = {"ok": True, "action": "create", "errors": [], "changes": [], "nextRuns": ["2026-11-02T03:00:00+01:00"]}
UPDATE = {"ok": True, "action": "update", "errors": [], "changes": ["statement"], "nextRuns": []}
STREAM_OK = {"ok": True, "errors": []}
PLAN_OK = {"ok": True, "errors": [], "warnings": [], "changes": [], "files": [], "planId": "abc"}
NO_STREAMS = {"namespace": "stardelt", "streams": []}
NO_CATALOGS = {"catalogs": [], "default": "gold"}
LOAD = {
    "name": "load",
    "ownerGroup": "sales",
    "sql": "select 1",
    "schedule": {"cron": "0 3 * * *", "timezone": "Europe/Berlin"},
}


def run(argv, *responses):
    out, err = io.StringIO(), io.StringIO()
    session = FakeSession(*responses)
    code = cli.main(argv, environ=ENV, stdout=out, stderr=err, session=session, stdin=io.StringIO(""))
    return code, out.getvalue(), err.getvalue(), session


def calls(session):
    return [(method, url.removeprefix("https://nova.example")) for method, url, _ in session.calls]


def test_a_production_target_is_rendered_as_written_with_the_sql_file_read_in(tmp_path):
    problems, mode, specs = bundle.render(make(tmp_path), "prod", "alice")
    assert (problems, mode) == ([], "production")
    assert specs == [LOAD, {"name": "nightly", "ownerGroup": "sales", "sql": "select * from sales.demo.trips"}]


def test_a_development_target_deploys_under_the_persons_name_without_a_schedule(tmp_path):
    problems, mode, specs = bundle.render(make(tmp_path), "dev", "Alice O'Neil")
    assert (problems, mode) == ([], "development")
    assert [spec["name"] for spec in specs] == ["dev-alice-o-neil-load", "dev-alice-o-neil-nightly"]
    assert all("schedule" not in spec for spec in specs)
    assert specs[1]["sql"] == "select * from sales_dev.demo.trips"
    problems, _, specs = bundle.render(make(tmp_path), "dev", None)
    assert specs == [] and "sign in first" in problems[0][1]


def test_a_development_name_that_is_too_long_is_refused_and_a_notebook_file_stays_a_file(tmp_path):
    long = "name: " + "a" * 33 + "\nownerGroup: sales\nsql: select 1\n"
    problems, _, specs = bundle.render(make(tmp_path, resources={"long.yaml": long}), "dev", "alice")
    assert specs == [] and "longer than 40 characters" in problems[0][1]
    notebook = "name: nb\nownerGroup: sales\nnotebook: {file: notebooks/explore.ipynb}\n"
    problems, _, specs = bundle.render(make(tmp_path, resources={"nb.yaml": notebook}), "prod", "alice")
    assert problems == [] and {spec["name"]: spec.get("notebook") for spec in specs}["nb"] == {"file": "notebooks/explore.ipynb"}


def test_plan_asks_nova_about_every_pipeline_and_writes_nothing(tmp_path):
    make(tmp_path)
    code, out, err, session = run(
        ["bundle", "plan", str(tmp_path), "-t", "prod"], ME, FakeResponse(payload=CREATE), FakeResponse(payload=UPDATE)
    )
    assert (code, err) == (0, "")
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "would-create", "name": "load", "nextRuns": ["2026-11-02T03:00:00+01:00"], "kind": "Pipeline"},
        {"action": "would-update", "name": "nightly", "changes": ["statement"], "kind": "Pipeline"},
    ]
    assert calls(session) == [("GET", "/api/me"), ("POST", "/api/pipelines/validate"), ("POST", "/api/pipelines/validate")]
    assert session.calls[1][2]["json"] == {**LOAD, "provenance": {"bundle": "sales-etl", "target": "prod"}}, (
        "outside a Git checkout the stamp names the bundle and the target and no commit"
    )


def test_plan_format_markdown_is_a_table_a_pull_request_can_carry(tmp_path):
    make(tmp_path)
    code, out, err, session = run(
        ["bundle", "plan", str(tmp_path), "-t", "prod", "--format", "markdown"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=UPDATE), FakeResponse(payload=[]),
    )
    assert (code, err) == (0, "")
    files = bundle._digests(tmp_path)
    assert out == (
        "### Bundle plan · `prod`\n\n"
        "| Action | Kind | Name | Notes |\n"
        "|---|---|---|---|\n"
        "| would-create | Pipeline | load | next: 2026-11-02T03:00:00+01:00 |\n"
        "| would-update | Pipeline | nightly | statement |\n"
        "\n### Bundle files · `prod`\n\n"
        "| File | sha256 | State |\n"
        "|---|---|---|\n"
        + "".join(f"| {file['path']} | {file['sha256'][:12]} | new |\n" for file in files)
    )
    assert calls(session)[-1] == ("GET", "/api/deployments")
    assert session.calls[-1][2]["params"] == {"bundle": "sales-etl", "target": "prod"}


def test_plan_markdown_escapes_a_pipe_and_a_line_break_and_has_one_row_per_result():
    text = bundle.plan_markdown("prod", [
        {"action": "would-update", "name": "a|b", "changes": ["schedule"], "modified": True},
        {"action": "not-recorded", "name": "x", "reason": "one\ntwo"},
    ])
    assert text.splitlines()[4] == "| would-update | — | a\\|b | schedule; changed since deploy |"
    assert text.splitlines()[5] == "| not-recorded | — | x | one two |"


def test_plan_of_a_development_target_sends_the_prefixed_name_and_no_schedule(tmp_path):
    make(tmp_path)
    code, _, _, session = run(
        ["bundle", "plan", str(tmp_path), "-t", "dev"], ME, FakeResponse(payload=NO_CATALOGS), FakeResponse(payload=CREATE), FakeResponse(payload=CREATE)
    )
    assert code == 0
    assert session.calls[2][2]["json"] == {
        "name": "dev-alice-load",
        "ownerGroup": "sales",
        "sql": "select 1",
        "provenance": {"bundle": "sales-etl", "target": "dev"},
    }


def test_deploy_creates_or_updates_after_every_pipeline_passed(tmp_path):
    make(tmp_path)
    existing = {"pipelines": [{"name": "nightly", "namespace": "stardelt", "ownerGroup": "sales"}]}
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=UPDATE), FakeResponse(payload=PLAN_OK),
        FakeResponse(payload={"pipelines": []}), FakeResponse(201, {"name": "load"}),
        FakeResponse(payload=existing), FakeResponse(payload={"name": "nightly"}),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}, {"name": "nightly", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 7, "bundle": "sales-etl", "target": "prod", "status": "applied"}),
    )
    assert (code, err) == (0, "")
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "created", "name": "load", "kind": "Pipeline"},
        {"action": "updated", "name": "nightly", "kind": "Pipeline"},
        {"action": "recorded", "id": 7},
    ]
    assert calls(session) == [
        ("GET", "/api/me"), ("POST", "/api/pipelines/validate"), ("POST", "/api/pipelines/validate"),
        ("POST", "/api/deployments/plan"),
        ("GET", "/api/pipelines"), ("POST", "/api/pipelines"),
        ("GET", "/api/pipelines"), ("PUT", "/api/pipelines/stardelt/nightly"),
        ("GET", "/api/pipelines"), ("POST", "/api/deployments"),
    ]


def test_a_write_that_fails_reports_the_sentence_and_what_was_already_written(tmp_path):
    make(tmp_path)
    existing = {"pipelines": [{"name": "nightly", "namespace": "stardelt", "ownerGroup": "sales"}]}
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=UPDATE), FakeResponse(payload=PLAN_OK),
        FakeResponse(payload={"pipelines": []}), FakeResponse(201, {"name": "load"}),
        FakeResponse(payload=existing), FakeResponse(503, {"error": "the scheduler is not reachable"}),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 9, "bundle": "sales-etl", "target": "prod", "status": "failed"}),
    )
    assert code == 2
    assert err == "stardelt: prod: nightly: the scheduler is not reachable (HTTP 503)\n"
    assert "Traceback" not in err
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "created", "name": "load", "kind": "Pipeline"},
        {"action": "recorded", "id": 9},
    ]


def test_a_failed_deploy_whose_record_is_refused_still_exits_2_with_its_own_sentence(tmp_path):
    make(tmp_path)
    existing = {"pipelines": [{"name": "nightly", "namespace": "stardelt", "ownerGroup": "sales"}]}
    code, out, err, _ = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=UPDATE), FakeResponse(payload=PLAN_OK),
        FakeResponse(payload={"pipelines": []}), FakeResponse(201, {"name": "load"}),
        FakeResponse(payload=existing), FakeResponse(503, {"error": "the scheduler is not reachable"}),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}]}),
        FakeResponse(503, {"error": "database is down"}),
    )
    assert code == 2
    assert err == "stardelt: prod: nightly: the scheduler is not reachable (HTTP 503)\n"
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "created", "name": "load", "kind": "Pipeline"},
        {"action": "not-recorded", "reason": "database is down (HTTP 503)"},
    ]


def test_one_refusal_means_nothing_is_written_and_the_sentence_is_shown(tmp_path):
    make(tmp_path)
    refused = {"ok": False, "errors": [{"message": "you are not a member of the group sales"}], "changes": [], "nextRuns": []}
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"], ME, FakeResponse(payload=refused), FakeResponse(payload=UPDATE)
    )
    assert code == 2
    assert err == "stardelt: prod: load: you are not a member of the group sales\n"
    assert [json.loads(line)["name"] for line in out.splitlines()] == ["nightly"], "what passed is still reported"
    assert [method for method, _ in calls(session)] == ["GET", "POST", "POST"], "no create, no update"


def test_a_refused_deploy_records_nothing(tmp_path):
    make(tmp_path)
    refused = {"ok": False, "errors": [{"message": "you are not a member of the group sales"}], "changes": [], "nextRuns": []}
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"], ME, FakeResponse(payload=refused), FakeResponse(payload=UPDATE)
    )
    assert code == 2 and "load: you are not a member of the group sales" in err
    assert "POST /api/deployments" not in [f"{method} {url}" for method, url in calls(session)]


def test_deploy_dry_run_is_a_plan_and_a_bundle_problem_stops_before_any_request(tmp_path):
    make(tmp_path)
    code, out, _, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod", "--dry-run"], ME, FakeResponse(payload=CREATE), FakeResponse(payload=CREATE)
    )
    assert code == 0 and out.count("would-create") == 2 and len(session.calls) == 3
    code, out, err, session = run(["bundle", "plan", str(tmp_path), "-t", "nope"], ME)
    assert (code, out) == (2, "") and "no target named 'nope'" in err


def git(path, *args):
    done = subprocess.run(
        ["git", "-c", "user.email=a@example.com", "-c", "user.name=t", "-C", str(path), *args],
        capture_output=True, text=True, check=True,
    )
    return done.stdout.strip()


@pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")
def test_a_production_target_refuses_a_dirty_tree_and_a_development_target_records_it(tmp_path):
    make(tmp_path)
    git(tmp_path, "init", "-q")
    git(tmp_path, "add", "-A")
    git(tmp_path, "commit", "-q", "-m", "bundle")
    commit = git(tmp_path, "rev-parse", "HEAD")
    prod_answers = (ME, FakeResponse(payload=CREATE), FakeResponse(payload=CREATE))
    dev_answers = (ME, FakeResponse(payload=NO_CATALOGS), FakeResponse(payload=CREATE), FakeResponse(payload=CREATE))

    _, _, _, session = run(["bundle", "plan", str(tmp_path), "-t", "prod"], *prod_answers)
    assert session.calls[1][2]["json"]["provenance"] == {"bundle": "sales-etl", "target": "prod", "revision": commit}

    (tmp_path / "sql" / "load.sql").write_text("select 2")
    code, out, err, session = run(["bundle", "plan", str(tmp_path), "-t", "prod"], *prod_answers)
    assert (code, out) == (2, "")
    assert "uncommitted changes" in err
    assert len(session.calls) == 1, "only me(); no pipeline is checked"

    code, out, err, session = run(["bundle", "plan", str(tmp_path), "-t", "dev"], *dev_answers)
    assert (code, err) == (0, "")
    assert session.calls[2][2]["json"]["provenance"] == {"bundle": "sales-etl", "target": "dev", "revision": commit + "-dirty"}
    assert len(commit) == 40 and set(commit) <= set("0123456789abcdef")


@pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")
def test_an_untracked_file_outside_the_bundle_does_not_make_it_dirty(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    make(bundle_dir)
    git(tmp_path, "init", "-q")
    git(tmp_path, "add", "-A")
    git(tmp_path, "commit", "-q", "-m", "bundle")
    commit = git(tmp_path, "rev-parse", "HEAD")
    (tmp_path / ".wt-x").mkdir()
    (tmp_path / ".wt-x" / "f").write_text("")
    prod_answers = (ME, FakeResponse(payload=CREATE), FakeResponse(payload=CREATE))

    code, out, err, session = run(["bundle", "plan", str(bundle_dir), "-t", "prod"], *prod_answers)
    assert (code, err) == (0, "") and out
    assert session.calls[1][2]["json"]["provenance"] == {"bundle": "sales-etl", "target": "prod", "revision": commit}

    (bundle_dir / "new.sql").write_text("select 1")
    code, out, err, session = run(["bundle", "plan", str(bundle_dir), "-t", "prod"], *prod_answers)
    assert (code, out) == (2, "")
    assert "uncommitted changes" in err


def test_the_stamp_names_the_bundle_and_the_target_and_leaves_the_commit_out_without_git(tmp_path):
    make(tmp_path)
    assert bundle.provenance(tmp_path, "dev") == {"bundle": "sales-etl", "target": "dev"}


def stamped(name, bundle_name="sales-etl", target="prod"):
    return {"name": name, "namespace": "stardelt", "ownerGroup": "sales", "deployment": {"bundle": bundle_name, "target": target}}


LISTING = {
    "pipelines": [
        stamped("load"),
        stamped("nightly"),
        stamped("old"),
        stamped("old-in-dev", target="dev"),
        stamped("theirs", bundle_name="other-bundle"),
        {"name": "by-hand", "namespace": "stardelt", "ownerGroup": "sales"},
    ]
}


def test_plan_with_prune_lists_only_what_this_bundle_target_deployed_and_no_longer_names(tmp_path):
    make(tmp_path)
    code, out, err, session = run(
        ["bundle", "plan", str(tmp_path), "-t", "prod", "--prune"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=UPDATE), FakeResponse(payload=NO_STREAMS),
        FakeResponse(payload=LISTING),
    )
    assert (code, err) == (0, "")
    assert [json.loads(line)["action"] + " " + json.loads(line)["name"] for line in out.splitlines()] == [
        "would-create load", "would-update nightly", "would-delete old",
    ]
    assert [method for method, _ in calls(session)] == ["GET", "POST", "POST", "GET", "GET"], "nothing is deleted by a plan"


def test_deploy_with_prune_removes_them_after_everything_else_succeeded(tmp_path):
    make(tmp_path)
    existing = {"pipelines": [{"name": "nightly", "namespace": "stardelt", "ownerGroup": "sales"}]}
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod", "--prune"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=UPDATE), FakeResponse(payload=NO_STREAMS),
        FakeResponse(payload=LISTING), FakeResponse(payload=PLAN_OK),
        FakeResponse(payload={"pipelines": []}), FakeResponse(201, {"name": "load"}),
        FakeResponse(payload=existing), FakeResponse(payload={"name": "nightly"}),
        FakeResponse(204),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}, {"name": "nightly", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 7, "bundle": "sales-etl", "target": "prod", "status": "applied"}),
    )
    assert (code, err) == (0, "")
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "created", "name": "load", "kind": "Pipeline"},
        {"action": "updated", "name": "nightly", "kind": "Pipeline"},
        {"action": "deleted", "name": "old", "kind": "Pipeline"},
        {"action": "recorded", "id": 7},
    ]
    assert calls(session)[-3] == ("DELETE", "/api/pipelines/stardelt/old")
    assert [method for method, _ in calls(session)].count("DELETE") == 1


def test_a_refused_prune_delete_reports_the_sentence_and_keeps_the_earlier_rows(tmp_path):
    make(tmp_path)
    existing = {"pipelines": [{"name": "nightly", "namespace": "stardelt", "ownerGroup": "sales"}]}
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod", "--prune"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=UPDATE), FakeResponse(payload=NO_STREAMS),
        FakeResponse(payload=LISTING), FakeResponse(payload=PLAN_OK),
        FakeResponse(payload={"pipelines": []}), FakeResponse(201, {"name": "load"}),
        FakeResponse(payload=existing), FakeResponse(payload={"name": "nightly"}),
        FakeResponse(503, {"error": "the scheduler is not reachable"}),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 9, "bundle": "sales-etl", "target": "prod", "status": "failed"}),
    )
    assert code == 2
    assert err == "stardelt: prod: old: the scheduler is not reachable (HTTP 503)\n"
    assert "Traceback" not in err
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "created", "name": "load", "kind": "Pipeline"},
        {"action": "updated", "name": "nightly", "kind": "Pipeline"},
        {"action": "recorded", "id": 9},
    ]
    recorded = session.calls[-1][2]["json"]
    assert recorded["status"] == "failed"
    assert recorded["pipelines"] == [{"namespace": "stardelt", "name": "load", "action": "created"}]


def test_the_record_keeps_the_rendered_pipeline_documents_so_a_rollback_can_replay_them(tmp_path):
    make(tmp_path)
    stamp = {"bundle": "sales-etl", "revision": None, "repository": None}
    specs = [
        {"name": "load", "ownerGroup": "sales", "sql": "select 1"},
        {"name": "nightly", "ownerGroup": "sales", "sql": "select * from sales.demo.trips"},
    ]
    session = FakeSession(FakeResponse(201, {"id": 7}))
    nova = api.Client(base_url="https://nova.example", token=CANARY, session=session)
    assert bundle._record(nova, stamp, tmp_path, "prod", "applied", [], specs) == {"action": "recorded", "id": 7}
    assert session.calls[0][2]["json"]["resources"] == specs, "the record keeps exactly what was applied"
    digests = session.calls[0][2]["json"]["files"]
    paths = [entry["path"] for entry in digests]
    assert paths == sorted(paths), "the digests are sorted by path"
    assert len(paths) == 4
    assert paths[0].endswith("explore.ipynb") and paths[-1].endswith("load.sql")
    assert sum(1 for path in paths if path.endswith((".yaml", ".sql"))) == 3
    assert all(len(entry["sha256"]) == 64 for entry in digests)
    assert next(entry for entry in digests if entry["path"].endswith("load.sql"))["sha256"] == hashlib.sha256(b"select 1").hexdigest()
    session = FakeSession(FakeResponse(201, {"id": 8}))
    nova = api.Client(base_url="https://nova.example", token=CANARY, session=session)
    bundle._record(nova, stamp, tmp_path, "prod", "failed", [])
    assert session.calls[0][2]["json"]["resources"] == [], "a failed deploy keeps no rendered documents"
    assert [entry["path"] for entry in session.calls[0][2]["json"]["files"]] == paths, "a failed deploy still says which content it read"


def test_a_development_target_prunes_only_the_callers_own_pipelines(tmp_path):
    make(tmp_path)
    listing = {
        "pipelines": [
            stamped("dev-alice-load", target="dev"),
            stamped("dev-alice-old", target="dev"),
            stamped("dev-bob-old", target="dev"),
        ]
    }
    code, out, _, _ = run(
        ["bundle", "plan", str(tmp_path), "-t", "dev", "--prune"],
        ME, FakeResponse(payload=NO_CATALOGS), FakeResponse(payload=CREATE), FakeResponse(payload=CREATE), FakeResponse(payload=NO_STREAMS),
        FakeResponse(payload=listing),
    )
    assert code == 0
    assert [json.loads(line)["name"] for line in out.splitlines() if "would-delete" in line] == ["dev-alice-old"]


def test_a_refusal_means_nothing_is_deleted_either(tmp_path):
    make(tmp_path)
    refused = {"ok": False, "errors": [{"message": "no"}], "changes": [], "nextRuns": []}
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod", "--prune"],
        ME, FakeResponse(payload=refused), FakeResponse(payload=UPDATE), FakeResponse(payload=NO_STREAMS),
        FakeResponse(payload=LISTING),
    )
    assert code == 2 and "load: no" in err
    assert [method for method, _ in calls(session)] == ["GET", "POST", "POST", "GET", "GET"], "no write, no delete"


def with_stream(tmp_path, manifest=None):
    """A bundle with one pipeline and one stream app: ``resources/load.yaml`` and ``resources/streams.yaml``."""
    resources = {"load.yaml": LOAD_YAML, "streams.yaml": STREAM}
    return make(tmp_path, manifest=MANIFEST if manifest is None else manifest, resources=resources)


MODIFIED = {
    "namespace": "stardelt",
    "streams": [
        {"name": "measurements", "namespace": "stardelt", "deployment": {"bundle": "sales-etl", "target": "prod", "modified": True}},
    ],
}


def test_plan_lists_a_stream_as_would_create_and_writes_nothing(tmp_path):
    with_stream(tmp_path)
    code, out, err, session = run(
        ["bundle", "plan", str(tmp_path), "-t", "prod"], ME, FakeResponse(payload=CREATE),
        FakeResponse(payload=NO_STREAMS), FakeResponse(payload=STREAM_OK),
    )
    assert (code, err) == (0, "")
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "would-create", "name": "load", "nextRuns": ["2026-11-02T03:00:00+01:00"], "kind": "Pipeline"},
        {"action": "would-create", "name": "measurements", "kind": "StreamApp"},
    ]
    assert calls(session) == [
        ("GET", "/api/me"), ("POST", "/api/pipelines/validate"),
        ("GET", "/api/streams"), ("POST", "/api/streams/validate"),
    ], "no write"
    sent = session.calls[3][2]["json"]
    assert sent == {
        "name": "measurements",
        "ownerGroup": "sales",
        "source": {"topic": "pegelonline.measurements"},
        "sink": {"catalog": "sales", "table": "bronze.measurements", "mode": "merge", "mergeKeys": ["station"]},
        "provenance": {"bundle": "sales-etl", "target": "prod"},
    }


def test_plan_lists_a_stream_of_this_bundle_target_as_would_update(tmp_path):
    with_stream(tmp_path)
    listed = {"namespace": "stardelt", "streams": [
        {"name": "measurements", "namespace": "stardelt", "deployment": {"bundle": "sales-etl", "target": "prod"}},
    ]}
    code, out, _, _ = run(
        ["bundle", "plan", str(tmp_path), "-t", "prod"], ME, FakeResponse(payload=CREATE),
        FakeResponse(payload=listed), FakeResponse(payload=STREAM_OK),
    )
    assert code == 0
    assert [json.loads(line) for line in out.splitlines()][-1] == {"action": "would-update", "name": "measurements", "kind": "StreamApp"}


def test_a_refused_stream_stops_the_command_before_anything_is_written(tmp_path):
    with_stream(tmp_path)
    refused = {"ok": False, "errors": [{"message": "sink.catalog is 1 to 63 lower-case letters, digits and dashes"}]}
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"], ME, FakeResponse(payload=CREATE),
        FakeResponse(payload=NO_STREAMS), FakeResponse(payload=refused),
    )
    assert code == 2
    assert err == "stardelt: prod: measurements: sink.catalog is 1 to 63 lower-case letters, digits and dashes\n"
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "would-create", "name": "load", "nextRuns": ["2026-11-02T03:00:00+01:00"], "kind": "Pipeline"},
    ], "what passed is still reported"
    assert [method for method, _ in calls(session)] == ["GET", "POST", "GET", "POST"], "no create, no update"


def test_deploy_puts_the_stream_with_its_stamp(tmp_path):
    with_stream(tmp_path)
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=NO_STREAMS), FakeResponse(payload=STREAM_OK),
        FakeResponse(payload=PLAN_OK),
        FakeResponse(payload={"pipelines": []}), FakeResponse(201, {"name": "load"}),
        FakeResponse(payload={"name": "measurements"}),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 7, "bundle": "sales-etl", "target": "prod", "status": "applied"}),
    )
    assert (code, err) == (0, "")
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "created", "name": "load", "kind": "Pipeline"},
        {"action": "created", "name": "measurements", "kind": "StreamApp"},
        {"action": "recorded", "id": 7},
    ]
    method, path, kwargs = session.calls[-3]
    assert (method, path) == ("PUT", "https://nova.example/api/streams/stardelt/measurements")
    assert kwargs["json"]["name"] == "measurements"
    assert kwargs["json"]["provenance"] == {"bundle": "sales-etl", "target": "prod"}, "no commit outside a checkout"


def test_a_deploy_with_prune_removes_a_stale_stream_app(tmp_path):
    with_stream(tmp_path)
    listed = {"namespace": "stardelt", "streams": [
        {"name": "measurements", "namespace": "stardelt",
         "deployment": {"bundle": "sales-etl", "target": "prod"}},
        {"name": "old-stream", "namespace": "stardelt",
         "deployment": {"bundle": "sales-etl", "target": "prod"}},
    ]}
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod", "--prune"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=listed), FakeResponse(payload=STREAM_OK),
        FakeResponse(payload={"pipelines": []}), FakeResponse(payload=PLAN_OK), FakeResponse(payload={"pipelines": []}),
        FakeResponse(201, {"name": "load"}),
        FakeResponse(201, {"name": "measurements"}), FakeResponse(204),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 11, "bundle": "sales-etl", "target": "prod", "status": "applied"}),
    )
    assert (code, err) == (0, "")
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "created", "name": "load", "kind": "Pipeline"},
        {"action": "updated", "name": "measurements", "kind": "StreamApp"},
        {"action": "deleted", "name": "old-stream", "kind": "StreamApp"},
        {"action": "recorded", "id": 11},
    ]
    assert ("DELETE", "/api/streams/stardelt/old-stream") in calls(session)
    assert [method for method, _ in calls(session)].count("DELETE") == 1


def test_a_hand_changed_stream_is_refused_by_a_drift_block_target_unless_accepted(tmp_path):
    manifest = MANIFEST.replace("runAs: pipeline-sales}", "runAs: pipeline-sales, drift: block}")
    with_stream(tmp_path, manifest=manifest)
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"], ME, FakeResponse(payload=CREATE),
        FakeResponse(payload=MODIFIED), FakeResponse(payload=STREAM_OK),
    )
    assert code == 2
    assert "measurements was changed after its last deploy and this target says drift: block" in err
    assert [method for method, _ in calls(session)] == ["GET", "POST", "GET", "POST"], "no write"

    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod", "--accept-drift"], ME, FakeResponse(payload=CREATE),
        FakeResponse(payload=MODIFIED), FakeResponse(payload=STREAM_OK), FakeResponse(payload=PLAN_OK),
        FakeResponse(payload={"pipelines": []}), FakeResponse(201, {"name": "load"}),
        FakeResponse(payload={"name": "measurements"}),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 7, "bundle": "sales-etl", "target": "prod", "status": "applied"}),
    )
    assert (code, err) == (0, "")
    assert calls(session)[-3] == ("PUT", "/api/streams/stardelt/measurements")
    assert {"action": "updated", "name": "measurements", "kind": "StreamApp"} in [json.loads(line) for line in out.splitlines()]


def test_a_deploy_that_cannot_be_recorded_still_succeeds_and_says_so(tmp_path):
    make(tmp_path)
    existing = {"pipelines": [{"name": "nightly", "namespace": "stardelt", "ownerGroup": "sales"}]}
    code, out, err, _ = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=UPDATE), FakeResponse(payload=PLAN_OK),
        FakeResponse(payload={"pipelines": []}), FakeResponse(201, {"name": "load"}),
        FakeResponse(payload=existing), FakeResponse(payload={"name": "nightly"}),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}, {"name": "nightly", "namespace": "stardelt"}]}),
        FakeResponse(503, {"error": "this needs Nova's database, and this installation has none; ask your administrator"}),
    )
    assert (code, err) == (0, "")
    assert [json.loads(line) for line in out.splitlines()][-1] == {
        "action": "not-recorded",
        "reason": "this needs Nova's database, and this installation has none; ask your administrator (HTTP 503)",
    }


def test_the_deploy_asks_nova_for_the_plan_and_the_record_names_it(tmp_path):
    make(tmp_path)
    existing = {"pipelines": [{"name": "nightly", "namespace": "stardelt", "ownerGroup": "sales"}]}
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=UPDATE), FakeResponse(payload=PLAN_OK),
        FakeResponse(payload={"pipelines": []}), FakeResponse(201, {"name": "load"}),
        FakeResponse(payload=existing), FakeResponse(payload={"name": "nightly"}),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}, {"name": "nightly", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 7, "bundle": "sales-etl", "target": "prod", "status": "applied"}),
    )
    assert (code, err) == (0, "")
    plan_call = next(call for call in session.calls if call[1].endswith("/api/deployments/plan"))
    assert plan_call[0] == "POST"
    sent = plan_call[2]["json"]
    assert sent["bundle"] == "sales-etl" and sent["ownerGroup"] == "sales" and sent["target"] == "prod"
    assert sent["files"] == bundle._digests(tmp_path), "the plan names the same files the record will"
    stamp = {"bundle": "sales-etl", "target": "prod"}
    assert sent["resources"] == [
        {**LOAD, "provenance": stamp},
        {"name": "nightly", "ownerGroup": "sales", "sql": "select * from sales.demo.trips", "provenance": stamp},
    ], "the plan's set is the rendered set, in the order the deploy writes it"
    create = next(call for call in session.calls if call[0] == "POST" and call[1].endswith("/api/pipelines"))
    update = next(call for call in session.calls if call[0] == "PUT" and "/api/pipelines/" in call[1])
    assert create[2]["json"] == sent["resources"][0], "the plan checked exactly the document the deploy creates"
    assert update[2]["json"] == {k: v for k, v in sent["resources"][1].items() if k not in ("name", "ownerGroup")}
    assert session.calls[-1][2]["json"]["planId"] == "abc"


def test_a_plan_that_refuses_writes_nothing_and_its_sentence_is_the_problem(tmp_path):
    make(tmp_path)
    refused = {
        "ok": False, "errors": [{"path": "resources[0] (gold)", "message": "you are not a member of the group sales"}],
        "changes": [], "warnings": [], "files": [], "planId": "",
    }
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=UPDATE), FakeResponse(payload=refused),
    )
    assert code == 2
    assert err == "stardelt: prod: resources[0] (gold): you are not a member of the group sales\n"
    assert out == ""
    writes = [
        f"{method} {url.removeprefix('https://nova.example')}"
        for method, url, kwargs in session.calls
        if method in ("POST", "PUT", "DELETE") and "/api/pipelines" in url and "/validate" not in url
        or (method == "POST" and url.endswith("/api/deployments"))
    ]
    assert writes == [], "no create, no update, no record"
    assert calls(session) == [
        ("GET", "/api/me"), ("POST", "/api/pipelines/validate"), ("POST", "/api/pipelines/validate"),
        ("POST", "/api/deployments/plan"),
    ]


def test_a_nova_without_the_plan_route_still_deploys_and_records_no_plan(tmp_path, capsys):
    make(tmp_path)
    existing = {"pipelines": [{"name": "nightly", "namespace": "stardelt", "ownerGroup": "sales"}]}
    code, out, err, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=UPDATE), FakeResponse(404, {"error": "not found"}),
        FakeResponse(payload={"pipelines": []}), FakeResponse(201, {"name": "load"}),
        FakeResponse(payload=existing), FakeResponse(payload={"name": "nightly"}),
        FakeResponse(payload={"pipelines": [{"name": "load", "namespace": "stardelt"}, {"name": "nightly", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 7, "bundle": "sales-etl", "target": "prod", "status": "applied"}),
    )
    assert (code, err) == (0, "")
    assert "does not answer a deployment plan" in capsys.readouterr().err
    assert [json.loads(line) for line in out.splitlines()] == [
        {"action": "created", "name": "load", "kind": "Pipeline"},
        {"action": "updated", "name": "nightly", "kind": "Pipeline"},
        {"action": "recorded", "id": 7},
    ]
    assert session.calls[-1][2]["json"]["planId"] is None


def test_validate_rejects_nine_viewers(tmp_path):
    nine = ", ".join(f"g{n}" for n in range(9))
    wide = f"name: wide\nviewers: [{nine}]\nsql: select 1\n"
    problems = bundle.validate(make(tmp_path, resources={"wide.yaml": wide}), "prod")[0]
    assert "viewers must be a list of at most 8 group names" in " | ".join(message for _, message in problems)


def test_validate_rejects_a_viewer_that_is_not_a_group_name(tmp_path):
    bad = "name: bad\nviewers: [visitors, 'not a group']\nsql: select 1\n"
    problems = bundle.validate(make(tmp_path, resources={"bad.yaml": bad}), "prod")[0]
    assert "viewers must be a list of at most 8 group names" in " | ".join(message for _, message in problems)


def test_a_pipeline_with_viewers_is_sent_to_create(tmp_path):
    viewers = "name: shared\nviewers: [visitors]\nsql: select 1\nschedule: {cron: '0 3 * * *', timezone: Europe/Berlin}\n"
    make(tmp_path, resources={"shared.yaml": viewers},
         files={"sql/load.sql": "select 1", "notebooks/explore.ipynb": "{}"})
    code, _, _, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"],
        ME, FakeResponse(payload=CREATE), FakeResponse(payload=PLAN_OK), FakeResponse(payload={"pipelines": []}),
        FakeResponse(201, {"name": "shared"}),
        FakeResponse(payload={"pipelines": [{"name": "shared", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 7, "bundle": "sales-etl", "target": "prod", "status": "applied"}),
    )
    assert code == 0
    create = next(call for call in session.calls if call[0] == "POST" and call[1].endswith("/api/pipelines"))
    assert create[2]["json"]["viewers"] == ["visitors"], "the deploy sends the file's viewers to create"


def test_a_pipeline_with_viewers_publishes_the_notebook_with_them(tmp_path):
    explore = "name: explore\nviewers: [visitors]\nnotebook: {file: notebooks/explore.ipynb}\n"
    make(tmp_path, resources={"explore.yaml": explore},
         files={"sql/load.sql": "select 1", "notebooks/explore.ipynb": json.dumps({"nbformat": 4, "cells": []})})
    new = FakeResponse(payload={"name": "explore.ipynb", "group": "sales", "objectVersion": "", "unchanged": False})
    stored = FakeResponse(payload={"name": "explore.ipynb", "group": "sales", "objectVersion": "v8", "unchanged": False})
    code, _, _, session = run(
        ["bundle", "deploy", str(tmp_path), "-t", "prod"],
        ME, new, stored, FakeResponse(payload=CREATE), FakeResponse(payload=PLAN_OK), FakeResponse(payload={"pipelines": []}), FakeResponse(payload={}),
        FakeResponse(payload={"pipelines": [{"name": "explore", "namespace": "stardelt"}]}),
        FakeResponse(201, {"id": 7, "bundle": "sales-etl", "target": "prod", "status": "applied"}),
    )
    assert code == 0
    publish = [call for call in session.calls if call[0] == "PUT" and "/api/notebooks/" in call[1]][-1]
    assert publish[2]["headers"]["X-Stardelt-Viewers"] == '["visitors"]'


PRODUCTION = {"catalogs": [{"name": "sales", "id": "1", "environment": "production", "canWrite": True}], "default": "sales"}


def test_a_development_target_may_not_name_a_production_catalog(tmp_path):
    manifest = MANIFEST.replace(
        "dev:  {mode: development, variables: {catalog: sales_dev}}",
        "dev:  {mode: development, variables: {catalog: sales}}",
    )
    make(tmp_path, manifest=manifest)
    code, out, err, session = run(
        ["bundle", "plan", str(tmp_path), "-t", "dev"], ME, FakeResponse(payload=PRODUCTION)
    )
    assert (code, out) == (2, "")
    assert "a development target may not name the production catalog 'sales'" in err
    assert calls(session) == [("GET", "/api/me"), ("GET", "/api/catalog/catalogs")], "nothing is checked or written"


def test_a_production_target_may_name_a_production_catalog(tmp_path):
    make(tmp_path)
    code, _, _, session = run(
        ["bundle", "plan", str(tmp_path), "-t", "prod"], ME, FakeResponse(payload=CREATE),
        FakeResponse(payload=CREATE),
    )
    assert code == 0
    assert ("GET", "/api/catalog/catalogs") not in calls(session)
