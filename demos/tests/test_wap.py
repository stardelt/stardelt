import re

import httpx
import pytest
from stardelt_pipelines import wap
from stardelt_pipelines.wap import WapError

BASE = "https://catalog.test"
TABLE = "gold.shop.t"
BRANCH = "audit_0123456789abcdef"
# `defaults.prefix` of warehouse `gold`, measured on the lab 2026-11-21.
PREFIX = "0002beca-d524-11f1-a60a-0b41f9b1e44b"


def client(handler):
    def answer(request):
        if request.url.path == "/catalog/v1/config":
            return httpx.Response(200, json={"defaults": {"prefix": PREFIX}})
        return handler(request)

    return httpx.Client(transport=httpx.MockTransport(answer))


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
        f"/catalog/v1/{PREFIX}/namespaces/shop/tables/t",
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
    assert calls[0][1] == f"/catalog/v1/{PREFIX}/namespaces/shop/tables/t"
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
    assert seen["path"] == f"/catalog/v1/{PREFIX}/namespaces/shop/tables/t"
    assert seen["body"] == (
        '{"requirements":[],"updates":[{"action":"remove-snapshot-ref","ref-name":"%s"}]}'
        % BRANCH
    )


class FakeCatalog:
    """A catalog that records every `(method, url, json)` and answers the script, in order."""

    def __init__(self, answers):
        self.answers = list(answers)
        self.calls = []

    def _answer(self, method, url, json=None):
        self.calls.append((method, url, json))
        status, body = self.answers.pop(0)
        return httpx.Response(status, json=body)

    def get(self, url):
        if "/catalog/v1/config" in url:
            return httpx.Response(200, json={"defaults": {"prefix": PREFIX}})
        return self._answer("GET", url)

    def post(self, url, json=None):
        return self._answer("POST", url, json)


def refs_answer(snapshot_id):
    return (200, {"metadata": {"refs": {"main": {"snapshot-id": snapshot_id}}}})


def test_prepare_creates_each_branch_from_main():
    catalog = FakeCatalog(
        [
            refs_answer(10),
            (200, {}),
            (200, {}),
            refs_answer(20),
            (200, {}),
            (200, {}),
        ]
    )
    main = wap.prepare(catalog, BASE, ["gold.shop.t", "gold.shop.u"], "audit_ab")
    assert main == {"gold.shop.t": 10, "gold.shop.u": 20}
    assert [call[0] for call in catalog.calls] == [
        "GET",
        "POST",
        "POST",
        "GET",
        "POST",
        "POST",
    ]
    for index, (name, snapshot) in enumerate((("t", 10), ("u", 20))):
        get, properties, branch = catalog.calls[index * 3 : index * 3 + 3]
        assert get[1] == f"{BASE}/catalog/v1/{PREFIX}/namespaces/shop/tables/{name}?snapshots=all"
        assert properties[2] == {
            "requirements": [],
            "updates": [{"action": "set-properties", "updates": {"write.wap.enabled": "true"}}],
        }
        assert branch[2]["updates"] == [
            {
                "action": "set-snapshot-ref",
                "ref-name": "audit_ab",
                "snapshot-id": snapshot,
                "type": "branch",
                "max-ref-age-ms": wap.FAILED_BRANCH_TTL_MS,
            }
        ]


def test_branch_snapshot_names_a_missing_branch():
    catalog = FakeCatalog([refs_answer(10)])
    with pytest.raises(WapError) as error:
        wap.branch_snapshot(catalog, BASE, TABLE, "audit_x")
    assert str(error.value) == "`gold.shop.t` has no branch `audit_x`"


def test_enable_branch_writes_posts_the_property():
    catalog = FakeCatalog([(200, {})])
    wap.enable_branch_writes(catalog, BASE, TABLE)
    (method, url, body) = catalog.calls[0]
    assert (method, url) == ("POST", f"{BASE}/catalog/v1/{PREFIX}/namespaces/shop/tables/t")
    assert body == {
        "requirements": [],
        "updates": [{"action": "set-properties", "updates": {"write.wap.enabled": "true"}}],
    }


def test_the_catalog_name_reaches_the_catalog_as_its_prefix():
    seen = []

    def handler(request):  # raw transport: this test wants to see the config GET too
        query = "?" + request.url.query.decode() if request.url.query else ""
        seen.append((request.method, request.url.path + query))
        if request.url.path == "/catalog/v1/config":
            return httpx.Response(200, json={"defaults": {"prefix": PREFIX}})
        return httpx.Response(200, json={})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        wap.create_branch(http, BASE, TABLE, BRANCH, 7)
    assert seen[0] == ("GET", "/catalog/v1/config?warehouse=gold")
    assert seen[1] == ("POST", f"/catalog/v1/{PREFIX}/namespaces/shop/tables/t")


def test_a_warehouse_without_a_prefix_is_named():
    def handler(request):  # raw transport: `client()` would answer the config GET with a prefix
        return httpx.Response(200, json={})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(WapError) as error:
            wap.main_snapshot(http, BASE, TABLE)
    assert "the catalog answered no prefix for warehouse `gold`" in str(error.value)
