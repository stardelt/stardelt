"""`stardelt bundle context`: the guides a bundle's `context:` block sets on its tables (#124 slice 9)."""

import io

from stardelt import api, bundle, cli

MANIFEST = """
bundle: {name: sales-etl, ownerGroup: sales}
variables:
  catalog: {default: sales_dev}
  unused: {default: x}
include: [resources/*.yaml]
files: [sql/*.sql, notebooks/*.ipynb]
requires: {connections: [crm-db]}
targets:
  dev:  {mode: development, variables: {catalog: sales_dev}}
  prod: {mode: production, variables: {catalog: sales}, runAs: pipeline-sales}
"""
LOAD = "name: load\nownerGroup: sales\nsqlFile: sql/load.sql\nschedule: {cron: '0 3 * * *', timezone: Europe/Berlin}\n"
NIGHTLY = "name: nightly\nsql: select * from ${var.catalog}.demo.trips\n"


class Nova:
    """A Nova that answers `table_documentation_get` from a dict and records every call."""

    def __init__(self, guides):
        self.guides = guides  # {"catalog.ns.tbl": guide or None}
        self.owners = {}  # {"catalog.ns.tbl": owner or None}
        self.calls = []

    def call(self, op, path=None, query=None, body=None, headers=None):
        self.calls.append((op, path, query, body))
        if op == "table_documentation_get":
            key = f"{query['catalog']}.{path['ns']}.{path['tbl']}"
            if self.guides.get("refuse-get") == key:
                raise api.StardeltApiError(403, "only the table's owner may read it")
            return {"guide": self.guides.get(key), "owner": self.owners.get(key)}
        if op == "table_documentation_update":
            key = f"{query['catalog']}.{path['ns']}.{path['tbl']}"
            self.guides[key] = body["guide"]
            return None
        raise AssertionError(op)

    def updates(self):
        return [call for call in self.calls if call[0] == "table_documentation_update"]


def make(tmp_path, tables=None, manifest=MANIFEST):
    """A full bundle (it passes `bundle.validate`) whose `context:` block names `tables`."""
    tables = tables if tables is not None else [
        {"table": "bronze.shop.customers", "guide": "How orders join customers."},
    ]
    block = "\ncontext:\n  tables:\n"
    for entry in tables:
        block += f"    - table: {entry['table']}\n      guide: {entry['guide']!r}\n"
        for key, value in entry.items():
            if key not in ("table", "guide"):
                block += f"      {key}: {value}\n"
    (tmp_path / "bundle.yaml").write_text(manifest + block, encoding="utf-8")
    (tmp_path / "resources").mkdir(exist_ok=True)
    for name, text in {"load.yaml": LOAD, "nightly.yaml": NIGHTLY}.items():
        (tmp_path / "resources" / name).write_text(text)
    for name, text in {"sql/load.sql": "select 1", "notebooks/explore.ipynb": "{}"}.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return tmp_path


def test_a_matching_guide_answers_unchanged_and_writes_nothing(tmp_path):
    directory = make(tmp_path)
    nova = Nova({"bronze.shop.customers": "How orders join customers."})
    problems, rows = bundle.context(nova, directory)
    assert problems == []
    assert rows == [{"table": "bronze.shop.customers", "action": "unchanged"}]
    assert nova.updates() == []


def test_a_differing_guide_under_check_answers_would_set_and_writes_nothing(tmp_path):
    directory = make(tmp_path)
    nova = Nova({"bronze.shop.customers": "old words"})
    problems, rows = bundle.context(nova, directory, check=True)
    assert problems == []
    assert rows == [{"table": "bronze.shop.customers", "action": "would-set"}]
    assert nova.updates() == []


def test_the_command_exits_1_on_drift_under_check(tmp_path, monkeypatch):
    directory = make(tmp_path)
    nova = Nova({"bronze.shop.customers": "old words"})
    monkeypatch.setattr(api, "Client", lambda **kwargs: nova)
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(
        ["bundle", "context", str(directory), "--check"],
        environ={"STARDELT_NOVA_URL": "https://nova.example", "STARDELT_TOKEN": "t"},
        stdout=out, stderr=err, session=object(),
    )
    assert code == 1
    assert out.getvalue() == "bronze.shop.customers  would-set\n"
    assert nova.updates() == []


def test_applying_claims_an_ownerless_table_with_the_bundles_group(tmp_path):
    directory = make(tmp_path)
    nova = Nova({})
    problems, rows = bundle.context(nova, directory)
    assert problems == []
    assert rows == [{"table": "bronze.shop.customers", "action": "set"}]
    assert nova.updates() == [
        (
            "table_documentation_update",
            {"ns": "shop", "tbl": "customers"},
            {"catalog": "bronze"},
            {"guide": "How orders join customers.", "owner": "sales"},
        )
    ]


def test_an_owned_table_is_written_without_an_owner(tmp_path):
    directory = make(tmp_path)
    nova = Nova({})
    nova.owners["bronze.shop.customers"] = "shop-owners"
    problems, rows = bundle.context(nova, directory)
    assert problems == []
    assert rows == [{"table": "bronze.shop.customers", "action": "set"}]
    assert nova.updates() == [
        (
            "table_documentation_update",
            {"ns": "shop", "tbl": "customers"},
            {"catalog": "bronze"},
            {"guide": "How orders join customers."},
        )
    ]


def test_two_rows_write_only_the_second(tmp_path):
    directory = make(tmp_path, tables=[
        {"table": "bronze.shop.customers", "guide": "same"},
        {"table": "bronze.shop.orders", "guide": "new words"},
    ])
    nova = Nova({"bronze.shop.customers": "same"})
    problems, rows = bundle.context(nova, directory)
    assert problems == []
    assert rows == [
        {"table": "bronze.shop.customers", "action": "unchanged"},
        {"table": "bronze.shop.orders", "action": "set"},
    ]
    assert [call[1] for call in nova.updates()] == [{"ns": "shop", "tbl": "orders"}]


def test_a_refused_read_is_a_problem_and_the_other_rows_are_still_visited(tmp_path):
    directory = make(tmp_path, tables=[
        {"table": "bronze.shop.customers", "guide": "words"},
        {"table": "bronze.shop.orders", "guide": "new words"},
    ])
    nova = Nova({"refuse-get": "bronze.shop.customers"})
    problems, rows = bundle.context(nova, directory)
    assert [where for where, _ in problems] == ["bronze.shop.customers"]
    assert "only the table's owner may read it" in problems[0][1]
    assert rows == [{"table": "bronze.shop.orders", "action": "set"}]


def test_validate_accepts_the_full_manifest_with_the_context_block(tmp_path):
    problems, warnings, resolved = bundle.validate(make(tmp_path))
    assert problems == []
    assert warnings == ["variable 'unused' is declared but no pipeline file uses it"]
    assert resolved == {"dev": ("development", ["load", "nightly"]), "prod": ("production", ["load", "nightly"])}


def said(problems):
    return " | ".join(f"{where}: {message}" for where, message in problems)


def test_validate_refuses_a_duplicate_table_an_empty_guide_and_an_unknown_key(tmp_path):
    problems = bundle.validate(make(tmp_path, tables=[
        {"table": "bronze.shop.customers", "guide": "one"},
        {"table": "bronze.shop.customers", "guide": "two"},
        {"table": "bronze.shop.orders", "guide": "  "},
        {"table": "bronze.shop.loads", "guide": "x", "colour": "red"},
    ]))[0]
    text = said(problems)
    assert "bronze.shop.customers is named twice" in text
    assert "context.tables[2].guide: must be a non-empty string" in text
    assert "context.tables[3]: unknown key 'colour' (known: table, guide)" in text


def test_validate_refuses_a_table_name_without_three_parts(tmp_path):
    problems = bundle.validate(make(tmp_path, tables=[
        {"table": "shop.customers", "guide": "words"},
    ]))[0]
    assert "context.tables[0].table: must be catalog.namespace.table" in said(problems)


def test_a_bundle_without_a_context_block_is_refused_by_the_command(tmp_path):
    make(tmp_path)
    (tmp_path / "bundle.yaml").write_text(MANIFEST, encoding="utf-8")
    problems, rows = bundle.context(Nova({}), tmp_path)
    assert problems == [("context", "the bundle has no `context:` block")]
    assert rows == []
