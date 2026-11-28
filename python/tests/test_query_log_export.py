"""``stardelt query-log export``: page the administrator's export into JSON lines. No network: a fake session stands in."""

import io
import json
from urllib.parse import urlencode

from stardelt import cli
from tests.test_api import CANARY, FakeResponse, FakeSession

ENV = {"STARDELT_NOVA_URL": "https://nova.example", "STARDELT_TOKEN": CANARY}


def run(argv, *responses):
    out, err = io.StringIO(), io.StringIO()
    session = FakeSession(*responses)
    code = cli.main(argv, environ=ENV, stdout=out, stderr=err, session=session, stdin=io.StringIO(""))
    return code, out.getvalue(), err.getvalue(), session


def urls(session):
    """Each call as the real client sends it: the path plus its params in the client's own order."""
    out = []
    for _, url, kwargs in session.calls:
        path = url.removeprefix("https://nova.example")
        params = kwargs.get("params")
        out.append(f"{path}?{urlencode(params)}" if params else path)
    return out


def test_two_pages_are_written_as_json_lines(tmp_path):
    path = tmp_path / "query-log.jsonl"
    code, out, err, session = run(
        ["query-log", "export", "--out", str(path), "--limit", "2"],
        FakeResponse(payload={"rows": [{"id": "a"}, {"id": "b"}], "nextCursor": "C1"}),
        FakeResponse(payload={"rows": [{"id": "c"}], "nextCursor": None}),
    )
    assert code == 0, err
    assert urls(session) == [
        "/api/admin/query-log/export?limit=2",
        "/api/admin/query-log/export?cursor=C1&limit=2",
    ]
    lines = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["id"] for line in lines] == ["a", "b", "c"]
    assert json.loads(out) == {"rows": 3, "nextCursor": None}


def test_a_refused_second_page_names_the_cursor_and_keeps_the_first_page(tmp_path):
    path = tmp_path / "query-log.jsonl"
    code, out, err, session = run(
        ["query-log", "export", "--out", str(path), "--limit", "2"],
        FakeResponse(payload={"rows": [{"id": "a"}], "nextCursor": "C1"}),
        FakeResponse(status=403, payload={"error": "this view is for platform administrators"}),
    )
    assert code == 1
    assert f"1 rows were written to {path}; continue with --cursor C1" in err
    assert "this view is for platform administrators" in err
    assert out == ""
    assert [json.loads(line)["id"] for line in path.read_text(encoding="utf-8").splitlines()] == [
        "a",
    ]


def test_a_cursor_continues_in_the_same_file(tmp_path):
    path = tmp_path / "query-log.jsonl"
    code, out, err, session = run(
        ["query-log", "export", "--out", str(path), "--limit", "1"],
        FakeResponse(payload={"rows": [{"id": "a"}], "nextCursor": "C1"}),
        FakeResponse(status=403, payload={"error": "this view is for platform administrators"}),
    )
    assert code == 1
    assert "1 rows were written to " + str(path) in err

    code, out, err, session = run(
        ["query-log", "export", "--out", str(path), "--cursor", "C1"],
        FakeResponse(payload={"rows": [{"id": "b"}], "nextCursor": None}),
    )
    assert code == 0, err
    assert json.loads(out) == {"rows": 1, "nextCursor": None}
    assert [json.loads(line)["id"] for line in path.read_text(encoding="utf-8").splitlines()] == [
        "a",
        "b",
    ]


def test_append_adds_rows_to_an_existing_file(tmp_path):
    path = tmp_path / "query-log.jsonl"
    path.write_text(json.dumps({"id": "old"}) + "\n", encoding="utf-8")
    code, out, err, session = run(
        ["query-log", "export", "--out", str(path), "--append"],
        FakeResponse(payload={"rows": [{"id": "new"}], "nextCursor": None}),
    )
    assert code == 0, err
    assert json.loads(out) == {"rows": 1, "nextCursor": None}
    assert [json.loads(line)["id"] for line in path.read_text(encoding="utf-8").splitlines()] == [
        "old",
        "new",
    ]
