"""The shop example (examples/shop): its seeds, its generator and its layers agree with each other. No dbt, no cluster."""

import csv
import re
import subprocess
from pathlib import Path

import yaml

SHOP = Path(__file__).resolve().parents[1] / "examples" / "shop"


def rows(name):
    with open(SHOP / "seeds" / name, newline="") as handle:
        return list(csv.DictReader(handle))


def test_the_seeds_are_small_and_have_the_ids_the_generator_draws_from():
    customers, products = rows("customers.csv"), rows("products.csv")
    assert [int(r["id"]) for r in customers] == list(range(1, 201))
    assert [int(r["id"]) for r in products] == list(range(1, 31))
    assert all(re.fullmatch(r"\d+\.\d\d", r["unit_price"]) and float(r["unit_price"]) > 0 for r in products)
    assert {r["segment"] for r in customers} == {"retail", "business", "enterprise"}
    assert sum(p.stat().st_size for p in (SHOP / "seeds").glob("*.csv")) < 20_000


def test_the_generator_draws_customers_and_products_from_exactly_the_ids_that_exist():
    sql = (SHOP / "models" / "bronze" / "order_lines.sql").read_text()
    found = re.findall(r"mod\(mod\(from_big_endian_64\(xxhash64\(to_utf8\('(\w)'[^)]*\)\)\)\), (\d+)\) \+ (\d+), (\d+)\)", sql)
    by_letter = {letter: (int(a), int(b), int(c)) for letter, a, b, c in found}
    assert by_letter["c"] == (200, 200, 200) and by_letter["p"] == (30, 30, 30)
    assert by_letter["q"] == (5, 5, 5) and by_letter["d"] == (365, 365, 365)


def test_the_layers_are_the_catalogs_and_the_scale_is_a_multiple_of_a_thousand():
    project = yaml.safe_load((SHOP / "dbt_project.yml").read_text())
    models = project["models"]["shop"]
    assert {layer: models[layer]["+database"] for layer in ("bronze", "staging", "marts")} == {"bronze": "bronze", "staging": "silver", "marts": "gold"}
    assert project["seeds"]["shop"]["+database"] == "bronze"
    assert project["vars"]["scale"] % 1000 == 0 and project["vars"]["scale"] >= 1000


def test_every_model_is_described_and_tested():
    described = {m["name"]: m for m in yaml.safe_load((SHOP / "models" / "schema.yml").read_text())["models"]}
    on_disk = {p.stem for p in (SHOP / "models").rglob("*.sql")}
    assert set(described) == on_disk
    for name, model in described.items():
        assert model.get("description"), name
        assert any(column.get("data_tests") for column in model["columns"]), name


def test_the_profile_reads_the_token_from_the_environment_and_holds_no_secret():
    text = (SHOP / "profiles.yml").read_text()
    assert "env_var('DBT_TRINO_JWT'" in text
    assert "eyJ" not in text and "Bearer" not in text


def demo_command(*arguments):
    done = subprocess.run(["make", "-n", "-C", str(SHOP), "demo", *arguments], capture_output=True, text=True, check=True)
    return done.stdout


def test_changing_the_scale_is_a_full_refresh_because_the_gold_table_is_incremental():
    assert "--full-refresh" not in demo_command()
    assert "--full-refresh" not in demo_command("SCALE=1000000")
    command = demo_command("SCALE=1000000", "FULL=1")
    assert "--full-refresh" in command and "scale: 1000000" in command
    readme = (SHOP / "README.md").read_text()
    assert "make demo SCALE=1000000 FULL=1" in readme


def test_the_shop_overrides_the_relation_listing_without_the_system_catalog():
    macro = (SHOP / "macros" / "list_relations_without_caching.sql").read_text()
    assert "macro trino__list_relations_without_caching(relation)" in macro
    assert "system." not in macro, "the override exists so dbt reads no system.* catalog"
    assert "materialized_view" not in macro, "the shop has no materialized views to detect"
    project = yaml.safe_load((SHOP / "dbt_project.yml").read_text())
    assert "macros" in project["macro-paths"]


def test_the_staging_views_run_as_the_querying_identity():
    project = yaml.safe_load((SHOP / "dbt_project.yml").read_text())
    staging = project["models"]["shop"]["staging"]
    assert staging["+materialized"] == "view"
    assert staging["+view_security"] == "invoker", (
        "Trino writes trino.run-as-owner for a SECURITY DEFINER view, and Lakekeeper refuses "
        "that property without a trusted engine; the staging views are internal to this project"
    )


def test_the_incremental_temp_relation_keeps_dbts_default_and_the_units_test_is_a_marts_test():
    project = yaml.safe_load((SHOP / "dbt_project.yml").read_text())
    assert "+views_enabled" not in project["models"], (
        "dbt's documented default is a view for the incremental temp relation; Trino is the "
        "catalog's trusted engine now (stardelt#393, #486, #487), so the #441 workaround is gone"
    )
    test = (SHOP / "tests" / "units_reconcile.sql").read_text()
    assert "ref('order_lines_enriched')" in test, (
        "the units test must ref only marts models, or eager indirect selection runs it in "
        "the tag:load batch, where a fresh catalog has no gold.shop_dbt yet"
    )
    assert "ref('stg_order_lines')" not in test


def test_every_dbt_batch_tag_selects_a_resource_and_the_seeds_come_first():
    project = yaml.safe_load((SHOP / "dbt_project.yml").read_text())
    tags = set()
    for layer in project["models"]["shop"].values():
        tags.update(layer.get("+tags") or [])
    tags.update(project["seeds"]["shop"].get("+tags") or [])
    for snapshot in project.get("snapshots", {}).values():
        tags.update(snapshot.get("+tags") or [])
    pipeline = yaml.safe_load((SHOP / "pipeline-dbt.yaml").read_text())
    batches = yaml.safe_load(pipeline["spec"]["tasks"][0]["config"]["batches"])
    assert batches[0] == "tag:seed", "the seeds must be built before anything reads bronze"
    unknown = [b for b in batches if b.removeprefix("tag:") not in tags]
    assert not unknown, f"batches select tags no resource carries: {unknown}"
    assert set(tags) <= {b.removeprefix("tag:") for b in batches}


def test_the_dbt_pipeline_is_readable_by_visitors():
    pipeline = yaml.safe_load((SHOP / "pipeline-dbt.yaml").read_text())
    assert pipeline["spec"]["viewers"] == ["visitors"]
