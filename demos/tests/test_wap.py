import re

import httpx
import pytest
from stardelt_pipelines import wap
from stardelt_pipelines.wap import WapError

BASE = "https://catalog.test"
TABLE = "gold.shop.t"
BRANCH = "audit_0123456789abcdef"


def client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_a_branch_name_is_deterministic_and_legal():
    first = wap.branch_name("manual__2026-11-21T05:00:00+00:00")
    assert re.match(r"^audit_[0-9a-f]{16}$", first)
    assert first == wap.branch_name("manual__2026-11-21T05:00:00+00:00")
    assert first != wap.branch_name("scheduled__2026-11-21T05:00:00+00:00")


def test_the_table_placeholder_becomes_the_trino_form():
    assert wap.resolve_table(
        "SELECT count(*) FROM ${table} WHERE id < 0", "gold.shop.t", 42
    ) == "SELECT count(*) FROM gold.shop.t FOR VERSION AS OF 42 WHERE id < 0"


def test_a_table_needs_three_parts():
    with pytest.raises(WapError) as error:
        wap.table_parts("gold.t")
    assert "`gold.t` is not catalog.schema.table" in str(error.value)
    assert wap.table_parts("gold.shop.t") == ("gold", "shop", "t")


def test_create_branch_posts_the_ref_update():
    seen = {}

    def handler(request):
        seen["method"] = request.method
        seen["path"] = request.url.path
        seen["body"] = request.content.decode()
        return httpx.Response(200, json={})

    with client(handler) as http:
        wap.create_branch(http, BASE, TABLE, BRANCH, 7)
    assert (seen["method"], seen["path"]) == (
        "POST",
        "/catalog/v1/gold/namespaces/shop/tables/t",
    )
    assert seen["body"] == (
        '{"requirements":[],"updates":[{"action":"set-snapshot-ref",'
        f'"ref-name":"{BRANCH}","snapshot-id":7,"type":"branch",'
        '"max-ref-age-ms":604800000}]}'
    )


def test_publish_reads_the_branch_and_asserts_main():
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path, request.content.decode()))
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "refs": {BRANCH: {"snapshot-id": 8, "type": "branch"}},
                    }
                },
            )
        return httpx.Response(200, json={})

    with client(handler) as http:
        wap.publish(http, BASE, TABLE, BRANCH, 6)
    assert calls[0][0] == "GET"
    assert calls[0][1] == "/catalog/v1/gold/namespaces/shop/tables/t"
    assert calls[1][0] == "POST"
    assert calls[1][2] == (
        '{"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":6}],'
        '"updates":[{"action":"set-snapshot-ref","ref-name":"main","snapshot-id":8,'
        '"type":"branch"}]}'
    )


def test_a_stale_assert_is_the_rerun_sentence():
    def handler(request):
        if request.method == "GET":
            return httpx.Response(200, json={"metadata": {"refs": {BRANCH: {"snapshot-id": 8}}}})
        return httpx.Response(409, json={"error": {"message": "conflict"}})

    with client(handler) as http, pytest.raises(WapError) as error:
        wap.publish(http, BASE, TABLE, BRANCH, 6)
    assert "main moved during the run" in str(error.value)


def test_a_refused_commit_shows_the_catalogs_sentence():
    def handler(request):
        return httpx.Response(422, json={"error": {"message": "missing field 'action'"}})

    with client(handler) as http, pytest.raises(WapError) as error:
        wap.commit(http, BASE, TABLE, [], [{"ref-name": BRANCH}])
    assert "missing field 'action'" in str(error.value)


def test_drop_branch_posts_the_ref_removal():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["body"] = request.content.decode()
        return httpx.Response(200, json={})

    with client(handler) as http:
        wap.drop_branch(http, BASE, TABLE, BRANCH)
    assert seen["path"] == "/catalog/v1/gold/namespaces/shop/tables/t"
    assert seen["body"] == (
        '{"requirements":[],"updates":[{"action":"remove-snapshot-ref","ref-name":"%s"}]}'
        % BRANCH
    )
