"""``bundle.schema.json``: the editor's view of ``bundle.yaml``, kept in step with ``stardelt.bundle``."""

import json
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

from stardelt import bundle

SCHEMA = Path(bundle.__file__).with_name("bundle.schema.json")
ROOT = Path(__file__).resolve().parents[2]


def schema():
    return json.loads(SCHEMA.read_text(encoding="utf-8"))


def errors(document):
    return list(Draft202012Validator(schema()).iter_errors(document))


def test_the_schema_matches_the_code():
    doc = schema()
    Draft202012Validator.check_schema(doc)
    assert set(doc["properties"]) == bundle.KEYS
    target = doc["properties"]["targets"]["additionalProperties"]["properties"]
    assert target["mode"]["enum"] == list(bundle.MODES)
    assert target["drift"]["enum"] == list(bundle.DRIFT)
    assert doc["properties"]["files"]["maxItems"] == bundle.MAX_FILES
    placed = [
        (bundle.NAME.pattern, doc["properties"]["bundle"]["properties"]["name"]["pattern"]),
        (bundle.GROUP.pattern, doc["properties"]["bundle"]["properties"]["ownerGroup"]["pattern"]),
        (bundle.VARIABLE.pattern, doc["properties"]["variables"]["propertyNames"]["pattern"]),
        (bundle.CONNECTION.pattern, doc["properties"]["requires"]["properties"]["connections"]["items"]["pattern"]),
        (bundle.CATALOG.pattern, doc["properties"]["migrations"]["properties"]["catalog"]["pattern"]),
    ]
    for pattern, in_schema in placed:
        assert pattern == in_schema, pattern


def test_every_example_bundle_validates():
    examples = sorted(ROOT.glob("demos/examples/*/bundle.yaml"))
    assert examples, "the demo bundles are the schema's examples"
    for path in examples:
        assert errors(yaml.safe_load(path.read_text(encoding="utf-8"))) == [], path


def test_init_writes_a_bundle_the_schema_accepts(tmp_path):
    bundle.init(tmp_path / "shop", "shop-etl", "shop")
    assert errors(yaml.safe_load((tmp_path / "shop" / "bundle.yaml").read_text(encoding="utf-8"))) == []


def test_an_unknown_key_is_refused():
    top = errors(
        {
            "bundle": {"name": "shop-etl", "ownerGroup": "shop"},
            "include": ["x.yaml"],
            "targets": {"prod": {"mode": "production", "runAs": "pipeline-shop"}},
            "targts": {"prod": {"mode": "production", "runAs": "pipeline-shop"}},
        }
    )
    assert len(top) == 1, [error.message for error in top]
    assert list(top[0].absolute_path) == []
    inside = errors(
        {
            "bundle": {"name": "shop-etl", "ownerGroup": "shop"},
            "include": ["x.yaml"],
            "targets": {
                "prod": {"mode": "production", "drift": "block", "runAs": "pipeline-shop", "schedual": "0 3 * * *"}
            },
        }
    )
    assert len(inside) == 1, [error.message for error in inside]
    assert "'schedual' was unexpected" in inside[0].message
    assert list(inside[0].absolute_path)[-1] == "prod", "jsonschema names the object that holds the extra key"
