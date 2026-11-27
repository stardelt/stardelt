"""Tests for `stardelt doctor` (stardelt.doctor). No network: a fake session stands in."""

import io

import requests

import stardelt
from stardelt import cli

from tests.test_api import CANARY, FakeResponse, FakeSession


class Health(FakeResponse):
    def __init__(self, version):
        super().__init__(200, {"status": "ok", "db": "up"})
        self.headers = {"x-stardelt-version": version}


ME = {"user": "ana", "auth_mode": "oidc", "warehouse": "main"}

ENDPOINT = {
    "name": "default",
    "model": "qwen3-4b",
    "location": "on-premise",
    "purposes": ["ask", "agents"],
    "reachable": True,
    "toolCalling": True,
    "jsonOutput": True,
}


def run(environ, *responses, session=None):
    out, err = io.StringIO(), io.StringIO()
    session = session or FakeSession(*responses)
    code = cli.main(["doctor"], environ=environ, stdout=out, stderr=err, session=session)
    return code, out.getvalue(), err.getvalue(), session


def token_env(tmp_path):
    return {"STARDELT_NOVA_URL": "https://nova.example", "STARDELT_TOKEN": CANARY, "HOME": str(tmp_path)}


def test_all_checks_pass_with_a_token_and_the_same_release(tmp_path):
    code, out, err, session = run(
        token_env(tmp_path),
        Health(stardelt.__version__),
        FakeResponse(payload=ME),
        FakeResponse(payload=[ENDPOINT]),
    )
    assert code == 0
    assert "✓ identity: ana (oidc), warehouse main" in out
    assert "✓ assistant: default (qwen3-4b, on-premise · prompts stay here)" in out
    assert session.calls[-1][1].endswith("/api/model-endpoints")
    assert out.endswith("all checks passed\n")
    assert "Authorization" not in (session.calls[0][2].get("headers") or {})


def test_the_token_is_never_printed(tmp_path):
    _, out, _, _ = run(
        token_env(tmp_path),
        Health(stardelt.__version__),
        FakeResponse(payload=ME),
        FakeResponse(payload=[ENDPOINT]),
    )
    assert CANARY not in out


def test_no_host_stops_at_the_host_line(tmp_path):
    code, out, _, session = run({"HOME": str(tmp_path)})
    assert code == 1
    assert "✗ host: none" in out
    assert session.calls == []


def test_another_release_is_a_warning_not_a_failure(tmp_path):
    code, out, _, _ = run(
        token_env(tmp_path),
        Health("0.0.1"),
        FakeResponse(payload=ME),
        FakeResponse(payload=[ENDPOINT]),
    )
    assert code == 0
    assert "! nova: answers, release 0.0.1" in out


def test_nova_that_does_not_answer_fails_and_skips_identity(tmp_path):
    class Unreachable(FakeSession):
        def request(self, method, url, **kwargs):
            raise requests.ConnectionError("boom")

    code, out, _, _ = run(token_env(tmp_path), session=Unreachable())
    assert code == 1
    assert "does not answer (ConnectionError)" in out
    assert "- identity: not checked" in out


def test_not_signed_in_names_login(tmp_path):
    code, out, _, session = run(
        {"STARDELT_NOVA_URL": "https://nova.example", "HOME": str(tmp_path)},
        Health(stardelt.__version__),
    )
    assert code == 1
    assert "✗ sign-in: none — run `stardelt login`" in out
    assert len(session.calls) == 1


def test_a_refused_token_names_login(tmp_path):
    code, out, _, _ = run(
        token_env(tmp_path), Health(stardelt.__version__), FakeResponse(401, {"error": "invalid token"})
    )
    assert code == 1
    assert "Nova refused the sign-in (401)" in out


def test_no_model_endpoint_is_a_warning_not_a_failure(tmp_path):
    code, out, _, _ = run(
        token_env(tmp_path),
        Health(stardelt.__version__),
        FakeResponse(payload=ME),
        FakeResponse(payload=[]),
    )
    assert code == 0
    assert "! assistant: off — no ModelEndpoint answers Ask" in out
    assert out.endswith("all checks passed\n")


def test_an_endpoint_ask_would_refuse_is_not_the_assistant(tmp_path):
    code, out, _, _ = run(
        token_env(tmp_path),
        Health(stardelt.__version__),
        FakeResponse(payload=ME),
        FakeResponse(payload=[dict(ENDPOINT, reachable=False)]),
    )
    assert code == 0
    assert "! assistant: off" in out


def test_a_cloud_endpoint_says_prompts_leave_the_cluster(tmp_path):
    _, out, _, _ = run(
        token_env(tmp_path),
        Health(stardelt.__version__),
        FakeResponse(payload=ME),
        FakeResponse(payload=[dict(ENDPOINT, location="cloud")]),
    )
    assert "✓ assistant: default (qwen3-4b, cloud · prompts leave your cluster)" in out
