"""Bundles (stardelt.bundle) and ``stardelt bundle validate``: offline, no network, no sign-in."""

import io
import os
import shutil
import subprocess

import pytest
import yaml

from stardelt import bundle, cli

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
STREAM = (
    "kind: StreamApp\nname: measurements\nownerGroup: sales\n"
    "source: {topic: pegelonline.measurements}\n"
    'sink: {catalog: "${var.catalog}", table: bronze.measurements, mode: merge, mergeKeys: [station]}\n'
)


def make(tmp_path, manifest=MANIFEST, resources=None, files=None):
    (tmp_path / "bundle.yaml").write_text(manifest)
    (tmp_path / "resources").mkdir(exist_ok=True)
    for name, text in (resources if resources is not None else {"load.yaml": LOAD, "nightly.yaml": NIGHTLY}).items():
        (tmp_path / "resources" / name).write_text(text)
    for name, text in (files if files is not None else {"sql/load.sql": "select 1", "notebooks/explore.ipynb": "{}"}).items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return tmp_path


def said(problems):
    return " | ".join(f"{where}: {message}" for where, message in problems)


def test_a_good_bundle_resolves_the_pipelines_of_every_target(tmp_path):
    problems, warnings, resolved = bundle.validate(make(tmp_path))
    assert problems == []
    assert resolved == {"dev": ("development", ["load", "nightly"]), "prod": ("production", ["load", "nightly"])}
    assert warnings == ["variable 'unused' is declared but no pipeline file uses it"]


def test_the_command_prints_one_line_per_target_without_a_host_or_a_sign_in(tmp_path):
    make(tmp_path)
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(["bundle", "validate", str(tmp_path), "-t", "prod"], environ={}, stdout=out, stderr=err)
    assert (code, out.getvalue()) == (0, "prod (production): load, nightly\n")
    assert "warning" in err.getvalue()


def test_the_command_exits_2_with_one_sentence_per_problem(tmp_path):
    make(tmp_path, manifest=MANIFEST.replace("sales-etl", "X"))
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(["bundle", "validate", str(tmp_path)], environ={}, stdout=out, stderr=err)
    assert (code, out.getvalue()) == (2, "")
    assert "stardelt: bundle.name: must be 3 to 40 characters" in err.getvalue()


def test_unknown_keys_and_a_missing_manifest_are_problems(tmp_path):
    assert "no bundle.yaml here" in said(bundle.validate(tmp_path)[0])
    make(tmp_path, manifest=MANIFEST + "colour: red\n")
    assert "unknown key 'colour'" in said(bundle.validate(tmp_path)[0])


def test_a_variable_must_be_declared_and_have_a_value_in_every_target(tmp_path):
    manifest = MANIFEST.replace("  catalog: {default: sales_dev}\n", "  catalog: {}\n").replace("dev:  {mode: development, variables: {catalog: sales_dev}}", "dev:  {mode: development}")
    problems = said(bundle.validate(make(tmp_path, manifest=manifest))[0])
    assert "nightly.yaml (dev): ${var.catalog} has no value" in problems
    manifest = MANIFEST.replace("variables: {catalog: sales}, runAs", "variables: {catalog: sales, other: 1}, runAs")
    assert "variable 'other' is not declared" in said(bundle.validate(make(tmp_path, manifest=manifest))[0])


def test_a_production_target_needs_a_run_as_principal_of_the_owner_group(tmp_path):
    for run_as in ("", ", runAs: pipeline-other", ", runAs: someone"):
        manifest = MANIFEST.replace(", runAs: pipeline-sales", run_as)
        assert "a production target needs runAs: pipeline-sales" in said(bundle.validate(make(tmp_path, manifest=manifest))[0]), run_as
    manifest = MANIFEST.replace("runAs: pipeline-sales", "runAs: pipeline-sales-prod")
    assert bundle.validate(make(tmp_path, manifest=manifest))[0] == []


def test_every_pipeline_file_must_be_a_pipeline_with_one_source_inside_the_bundle(tmp_path):
    resources = {"a.yaml": "sql: select 1\n", "b.yaml": "name: b\nsql: select 1\nsqlFile: sql/load.sql\n", "c.yaml": "name: c\nsqlFile: sql/missing.sql\n",
                 "d.yaml": "name: d\nnotebook: {file: notebooks/none.ipynb}\n", "e.yaml": "name: e\nownerGroup: other\nsql: select 1\n"}
    problems = said(bundle.validate(make(tmp_path, resources=resources), "dev")[0])
    assert "a.yaml (dev): the file must be one JSON or YAML object with a \"name\"" in problems
    assert "b.yaml (dev): give exactly one of sql, sqlFile or notebook" in problems
    assert "c.yaml (dev): sqlFile 'sql/missing.sql' is not among the bundle's files" in problems
    assert "d.yaml (dev): notebook file 'notebooks/none.ipynb' is not among the bundle's files" in problems
    assert "e.yaml (dev): ownerGroup 'other' is not the bundle's group 'sales'" in problems


def test_two_files_with_one_pipeline_name_and_a_pattern_that_matches_nothing_are_problems(tmp_path):
    problems = said(bundle.validate(make(tmp_path, resources={"a.yaml": NIGHTLY, "b.yaml": NIGHTLY}), "dev")[0])
    assert "two pipeline files define 'nightly'" in problems
    problems = said(bundle.validate(make(tmp_path, manifest=MANIFEST.replace("notebooks/*.ipynb", "notebooks/*.nope")))[0])
    assert "notebooks/*.nope: matches no file" in problems


def test_a_stream_file_validates_and_is_not_a_pipeline_name(tmp_path):
    problems, warnings, resolved = bundle.validate(make(tmp_path, resources={"load.yaml": LOAD, "nightly.yaml": NIGHTLY, "streams.yaml": STREAM}))
    assert problems == []
    assert resolved["prod"] == ("production", ["load", "nightly"]), "a stream app is not a pipeline name"
    assert warnings == ["variable 'unused' is declared but no pipeline file uses it"]


def test_a_stream_file_that_is_a_pipeline_or_names_another_group_is_refused(tmp_path):
    for extra, sentence in (
        ("sql: select 1\n", "a StreamApp is not a pipeline: remove sql"),
        ("publish: wap\n", "a StreamApp is not a pipeline: remove publish"),
        ("sink: {catalog: c, table: no-dot}\n", "sink.table is schema.table"),
    ):
        stream = f"kind: StreamApp\nname: measurements\nsource: {{topic: t}}\nsink: {{catalog: c, table: bronze.m}}\n{extra}"
        problems = said(bundle.validate(make(tmp_path, resources={"streams.yaml": stream}), "dev")[0])
        assert sentence in problems, extra


def test_an_unknown_kind_is_refused(tmp_path):
    problems = said(bundle.validate(make(tmp_path, resources={"a.yaml": "kind: Topic\nname: t\n"}), "dev")[0])
    assert "a.yaml (dev): kind must be one of Pipeline, StreamApp" in problems


def test_render_streams_gives_the_bodies_streams_put_takes(tmp_path):
    problems, mode, specs = bundle.render_streams(
        make(tmp_path, resources={"load.yaml": LOAD, "nightly.yaml": NIGHTLY, "streams.yaml": STREAM}), "dev", "alice"
    )
    assert (problems, mode) == ([], "development")
    assert specs == [
        {
            "name": "dev-alice-measurements",
            "ownerGroup": "sales",
            "source": {"topic": "pegelonline.measurements"},
            "sink": {"catalog": "sales_dev", "table": "bronze.measurements", "mode": "merge", "mergeKeys": ["station"]},
        }
    ]


def test_paths_must_stay_inside_the_bundle_and_links_and_big_files_are_refused(tmp_path):
    problems = said(bundle.validate(make(tmp_path, manifest=MANIFEST.replace("sql/*.sql", "../*.sql")))[0])
    assert "a pattern must stay inside the bundle" in problems
    make(tmp_path)
    outside = tmp_path.parent / "outside.sql"
    outside.write_text("select 2")
    os.symlink(outside, tmp_path / "sql" / "link.sql")
    assert "sql/link.sql: a symbolic link is not allowed" in said(bundle.validate(tmp_path)[0])
    os.unlink(tmp_path / "sql" / "link.sql")
    (tmp_path / "sql" / "big.sql").write_bytes(b"x" * (bundle.MAX_BYTES + 1))
    assert "sql/big.sql: larger than 5 MiB" in said(bundle.validate(tmp_path)[0])


def test_a_target_filter_and_an_unknown_target(tmp_path):
    make(tmp_path)
    problems, _, resolved = bundle.validate(tmp_path, "dev")
    assert problems == [] and list(resolved) == ["dev"]
    assert "no target named 'nope' (targets: dev, prod)" in said(bundle.validate(tmp_path, "nope")[0])


def test_init_writes_a_bundle_that_validates(tmp_path):
    written = bundle.init(tmp_path / "shop", "shop-etl", "shop")
    assert written == ["bundle.yaml", "resources/daily.yaml", "sql/daily.sql"]
    problems, warnings, resolved = bundle.validate(tmp_path / "shop")
    assert (problems, warnings) == ([], [])
    assert resolved == {"dev": ("development", ["daily"]), "prod": ("production", ["daily"])}
    assert "runAs: pipeline-shop" in (tmp_path / "shop" / "bundle.yaml").read_text()
    assert "timezone: UTC" in (tmp_path / "shop" / "resources" / "daily.yaml").read_text(), "Nova needs both fields"


def test_an_unknown_target_key_is_refused(tmp_path):
    manifest = MANIFEST.replace(
        "prod: {mode: production, variables: {catalog: sales}, runAs: pipeline-sales}",
        'prod: {mode: production, variables: {catalog: sales}, runAs: pipeline-sales, schedual: "0 3 * * *"}',
    )
    problems, _, _ = bundle.validate(make(tmp_path, manifest=manifest))
    assert [where for where, _ in problems] == ["targets.prod"]
    assert problems[0][1].startswith("unknown key 'schedual'")


def test_a_schedule_needs_both_cron_and_timezone_because_nova_refuses_it_otherwise(tmp_path):
    sentence = "schedule needs both cron and timezone (an IANA zone such as Europe/Berlin)"
    for schedule in ("{cron: '0 3 * * *'}", "{timezone: UTC}", "{cron: '', timezone: UTC}", "'0 3 * * *'", "{cron: 3, timezone: UTC}"):
        resources = {"load.yaml": f"name: load\nownerGroup: sales\nsqlFile: sql/load.sql\nschedule: {schedule}\n"}
        problems, _, _ = bundle.validate(make(tmp_path, resources=resources))
        assert sentence in said(problems), schedule
        assert "load.yaml (dev)" in said(problems) and "load.yaml (prod)" in said(problems), schedule
    resources = {"load.yaml": "name: load\nownerGroup: sales\nsqlFile: sql/load.sql\n"}
    assert bundle.validate(make(tmp_path, resources=resources))[0] == [], "a pipeline without a schedule runs on demand"


def test_init_refuses_a_bad_name_a_bad_group_and_an_existing_bundle_and_writes_nothing(tmp_path):
    for name, group, said in (("X", "shop", "the name must be"), ("shop-etl", "", "the group must be")):
        try:
            bundle.init(tmp_path, name, group)
        except ValueError as err:
            assert said in str(err)
        else:
            raise AssertionError("expected a ValueError")
    assert list(tmp_path.iterdir()) == []
    bundle.init(tmp_path, "shop-etl", "shop")
    try:
        bundle.init(tmp_path, "shop-etl", "shop")
    except ValueError as err:
        assert "already exists" in str(err)
    else:
        raise AssertionError("expected a ValueError")


def test_the_init_command_says_what_it_wrote_and_exits_2_on_a_mistake(tmp_path):
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(["bundle", "init", str(tmp_path / "b"), "--name", "shop-etl", "--group", "shop"], environ={}, stdout=out, stderr=err)
    assert (code, out.getvalue()) == (0, "") and "wrote bundle.yaml, resources/daily.yaml, sql/daily.sql" in err.getvalue()
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(["bundle", "init", str(tmp_path / "b"), "--name", "shop-etl", "--group", "shop"], environ={}, stdout=out, stderr=err)
    assert code == 2 and "already exists" in err.getvalue()


TASK_FILES = {
    "sql/schema.sql": "create table gold.schema (id bigint);\n",
    "sql/customers.sql": "select 1\n",
    "notebooks/explore.ipynb": "{}",
}


def task_bundle(directory, tasks, extra=""):
    """A bundle whose one resource file gives ``tasks`` (one YAML block entry per string)."""
    resource = "name: shop-gold\n" + extra + "tasks:\n" + "".join(f"  - {task}\n" for task in tasks)
    return make(directory, resources={"gold.yaml": resource}, files=dict(TASK_FILES))


def test_a_pipeline_may_carry_several_tasks(tmp_path):
    task_bundle(tmp_path, ["{name: schema, sqlFile: sql/schema.sql}", "{name: customers, sqlFile: sql/customers.sql, dependsOn: [schema]}"])
    assert bundle.validate(tmp_path)[0] == []
    problems, mode, specs = bundle.render(tmp_path, "prod", "alice")
    assert (problems, mode) == ([], "production")
    assert specs == [
        {
            "name": "shop-gold",
            "tasks": [
                {"name": "schema", "sql": "create table gold.schema (id bigint);\n"},
                {"name": "customers", "sql": "select 1\n", "dependsOn": ["schema"]},
            ],
            "ownerGroup": "sales",
        }
    ]


def test_a_dbt_task_validates_and_renders(tmp_path):
    """`dbt` (#112 slice 1b): a task source that names a project already in the tool image."""
    task_bundle(tmp_path, ["{name: build, dbt: {project: shop, select: 'tag:marts', threads: 4}}"])
    assert bundle.validate(tmp_path, "prod")[0] == []
    problems, mode, specs = bundle.render(tmp_path, "prod", "alice")
    assert (problems, mode) == ([], "production")
    assert specs[0]["tasks"][0]["dbt"] == {"project": "shop", "select": "tag:marts", "threads": 4}
    assert specs[0]["tasks"] == [{"name": "build", "dbt": {"project": "shop", "select": "tag:marts", "threads": 4}}]
    assert specs[0]["ownerGroup"] == "sales"


def test_a_quality_task_validates_and_renders(tmp_path):
    """`quality` (#114 S1): the SQL assertions the loader runs, written in the bundle."""
    task_bundle(
        tmp_path,
        ["{name: gate, quality: {checks: [{name: no_null_ids, sql: 'SELECT 1 WHERE 1=0', severity: warn}]}}"],
    )
    assert bundle.validate(tmp_path, "prod")[0] == []
    problems, mode, specs = bundle.render(tmp_path, "prod", "alice")
    assert (problems, mode) == ([], "production")
    assert specs[0]["tasks"] == [
        {"name": "gate", "quality": {"checks": [{"name": "no_null_ids", "sql": "SELECT 1 WHERE 1=0", "severity": "warn"}]}}
    ]
    assert specs[0]["ownerGroup"] == "sales"


NOTES = "notebooks/explore.ipynb"  # the notebook fixture TASK_FILES already writes


def test_a_wap_check_may_name_the_audit_branch(tmp_path):
    """`${table}` is the branch the run audits (#114); the loader resolves it since #858."""
    task_bundle(
        tmp_path,
        [
            "{name: build, notebook: {file: " + NOTES + "}, writes: [gold.shop.daily]}",
            "{name: gate, quality: {checks: [{name: no_null_ids, sql: 'SELECT 1 FROM ${table}'}]}}",
        ],
        extra="publish: wap\n",
    )
    assert bundle.validate(tmp_path, "prod")[0] == []


def test_a_table_placeholder_needs_wap_and_one_written_table(tmp_path):
    direct = tmp_path / "direct"
    direct.mkdir()
    task_bundle(direct, ["{name: gate, quality: {checks: [{name: c, sql: 'SELECT 1 FROM ${table}'}]}}"])
    assert "a check that names ${table} needs publish: wap in the pipeline" in said(bundle.validate(direct)[0])

    two = tmp_path / "two"
    two.mkdir()
    task_bundle(
        two,
        [
            "{name: build, notebook: {file: " + NOTES + "}, writes: [gold.shop.a, gold.shop.b]}",
            "{name: gate, quality: {checks: [{name: c, sql: 'SELECT 1 FROM ${table}'}]}}",
        ],
        extra="publish: wap\n",
    )
    assert "${table} names the audit branch of one table, and this pipeline writes 2" in said(bundle.validate(two)[0])


def test_quality_task_rules_are_reported(tmp_path):
    needs_checks = "a quality task needs checks (a list of 1 to 50 SQL assertions)"
    needs_name = "a check needs a name (1 to 63 letters, digits, dashes or underscores, starting with a letter or a digit)"
    many = "[" + ", ".join(f"{{name: c{index}, sql: 'select 1'}}" for index in range(51)) + "]"
    rows = [
        ("{name: x, quality: {checks: []}}", needs_checks),
        (f"{{name: x, quality: {{checks: {many}}}}}", needs_checks),
        ("{name: x, quality: yes}", "quality must be a mapping with checks"),
        ("{name: x, quality: {checks: [{name: '-bad', sql: 'select 1'}]}}", needs_name),
        (
            "{name: x, quality: {checks: [{name: c, sql: 'select 1'}, {name: c, sql: 'select 2'}]}}",
            "the check name is used twice",
        ),
        ("{name: x, quality: {checks: [{name: c, sql: ''}]}}", "a check's sql must be a non-empty string of at most 20000 characters"),
        (
            "{name: x, quality: {checks: [{name: c, sql: '" + "x" * 21000 + "'}]}}",
            "a check's sql must be a non-empty string of at most 20000 characters",
        ),
        ("{name: x, quality: {checks: [{name: c, sql: 'select 1', severity: fatal}]}}", "a check's severity must be error or warn"),
        (
            "{name: x, quality: {checks: [{name: c, sql: 'select 1', model: x}]}}",
            "a check accepts only name, sql and severity (found `model`)",
        ),
    ]
    for index, (task, expected) in enumerate(rows):
        directory = tmp_path / f"row{index}"
        directory.mkdir()
        task_bundle(directory, [task])
        problems = bundle.validate(directory)[0]
        assert problems[0][0] == "resources/gold.yaml (dev), task 'x'", problems
        assert expected in said(problems), problems


def test_a_quality_task_cannot_also_give_sql(tmp_path):
    task_bundle(tmp_path, ["{name: gate, sql: select 1, quality: {checks: [{name: c, sql: 'select 2'}]}}"])
    assert "task 'gate': give exactly one of sql, sqlFile, notebook, dbt, quality or spark-pipeline" in said(bundle.validate(tmp_path)[0])


def test_a_spark_pipeline_task_validates_and_renders(tmp_path):
    """`spark-pipeline` (#80 S1b): a declared graph in a bundle directory, rendered as the body Nova takes."""
    make(
        tmp_path,
        resources={
            "gold.yaml": "name: shop-gold\ntasks:\n"
            "  - {name: build, spark-pipeline: {project: spark/shop, refresh: full}, writes: [gold.shop.trips]}\n"
        },
        files={
            **TASK_FILES,
            "spark/shop/spark-pipeline.yml": "name: shop\nconfiguration:\n  spark.sql.catalog.gold.type: rest\n",
        },
    )
    assert bundle.validate(tmp_path, "prod")[0] == []
    problems, mode, specs = bundle.render(tmp_path, "prod", "alice")
    assert (problems, mode) == ([], "production")
    assert specs[0]["tasks"] == [
        {
            "name": "build",
            "sparkPipeline": {
                "project": "spark/shop",
                "refresh": "full",
                "files": {"spark-pipeline.yml": "name: shop\nconfiguration:\n  spark.sql.catalog.gold.type: rest\n"},
            },
            "writes": ["gold.shop.trips"],
        }
    ]
    assert specs[0]["ownerGroup"] == "sales"


def test_a_spark_pipeline_task_carries_its_graph_files(tmp_path):
    """`spark-pipeline` (#80 S2b): every file of the graph rides the task, `spark-pipeline.yml` at the root."""
    make(
        tmp_path,
        resources={
            "gold.yaml": "name: shop-gold\ntasks:\n"
            "  - {name: build, spark-pipeline: {project: spark/shop}, writes: [gold.shop.trips]}\n"
        },
        files={
            **TASK_FILES,
            "spark/shop/spark-pipeline.yml": "name: shop\nconfiguration:\n  spark.sql.catalog.gold.type: rest\n",
            "spark/shop/sql/trips.sql": "select 1\n",
        },
    )
    problems, _, specs = bundle.render(tmp_path, "prod", "alice")
    assert problems == []
    assert specs[0]["tasks"][0]["sparkPipeline"]["files"] == {
        "spark-pipeline.yml": "name: shop\nconfiguration:\n  spark.sql.catalog.gold.type: rest\n",
        "sql/trips.sql": "select 1\n",
    }


def test_a_graph_file_symlink_is_refused(tmp_path):
    make(
        tmp_path,
        resources={
            "gold.yaml": "name: shop-gold\ntasks:\n"
            "  - {name: build, spark-pipeline: {project: spark/shop}, writes: [gold.shop.trips]}\n"
        },
        files={
            **TASK_FILES,
            "spark/shop/spark-pipeline.yml": "name: shop\nconfiguration:\n  spark.sql.catalog.gold.type: rest\n",
        },
    )
    (tmp_path / "secret.txt").write_text("x")
    (tmp_path / "spark/shop/linked.sql").symlink_to(tmp_path / "secret.txt")
    problems, mode, specs = bundle.render(tmp_path, "prod", "alice")
    assert specs == []
    assert "a symbolic link is not allowed: linked.sql" in said(problems)


def test_a_graph_file_that_is_not_utf8_text_is_refused(tmp_path):
    make(
        tmp_path,
        resources={
            "gold.yaml": "name: shop-gold\ntasks:\n"
            "  - {name: build, spark-pipeline: {project: spark/shop}, writes: [gold.shop.trips]}\n"
        },
        files={
            **TASK_FILES,
            "spark/shop/spark-pipeline.yml": "name: shop\nconfiguration:\n  spark.sql.catalog.gold.type: rest\n",
        },
    )
    (tmp_path / "spark/shop/sql").mkdir()
    (tmp_path / "spark/shop/sql/trips.sql").write_bytes(b"select \x00 1")
    problems, mode, specs = bundle.render(tmp_path, "prod", "alice")
    assert specs == []
    assert problems == [
        ("resources/gold.yaml (prod) (build)", "the graph file sql/trips.sql is not UTF-8 text")
    ]
    assert said(problems).endswith("the graph file sql/trips.sql is not UTF-8 text")


def test_a_spark_pipeline_task_needs_its_project_directory(tmp_path):
    make(
        tmp_path,
        resources={"gold.yaml": "name: shop-gold\ntasks:\n  - {name: build, spark-pipeline: {project: spark/missing}}\n"},
        files=dict(TASK_FILES),
    )
    problems = bundle.validate(tmp_path)[0]
    assert problems[0][0] == "resources/gold.yaml (dev), task 'build'"
    assert "project 'spark/missing' has no spark-pipeline.yml" in said(problems)


def test_a_spark_pipeline_task_project_stays_inside_the_bundle(tmp_path):
    make(
        tmp_path,
        resources={"gold.yaml": "name: shop-gold\ntasks:\n  - {name: build, spark-pipeline: {project: sql/../spark/shop}}\n"},
        files={**TASK_FILES, "spark/shop/spark-pipeline.yml": "name: shop\n"},
    )
    problems = bundle.validate(tmp_path)[0]
    assert problems[0][0] == "resources/gold.yaml (dev), task 'build'"
    assert "project must be a relative directory inside the bundle" in said(problems)


def test_a_spark_pipeline_yml_may_not_set_the_warehouse_keys(tmp_path):
    make(
        tmp_path,
        resources={"gold.yaml": "name: shop-gold\ntasks:\n  - {name: build, spark-pipeline: {project: spark/shop}}\n"},
        files={**TASK_FILES, "spark/shop/spark-pipeline.yml": "name: shop\nstorage: warehouse\n"},
    )
    problems = bundle.validate(tmp_path)[0]
    assert problems[0][0] == "resources/gold.yaml (dev), task 'build'"
    assert (
        "spark-pipeline.yml must not set catalog, database or storage: stardelt sets them at run time (#80)"
        in said(problems)
    )


def test_a_spark_pipeline_libraries_entry_needs_a_glob(tmp_path):
    """The CLI reads `{glob: {include: …}}`; a bare string passed every check and died in the pod (#1006)."""
    make(
        tmp_path,
        resources={
            "gold.yaml": "name: shop-gold\ntasks:\n"
            "  - {name: build, spark-pipeline: {project: spark/shop}, writes: [gold.shop.trips]}\n"
        },
        files={
            **TASK_FILES,
            "spark/shop/spark-pipeline.yml": "name: shop\nlibraries: [tables.sql]\n"
            "configuration:\n  spark.sql.catalog.gold.type: rest\n",
        },
    )
    problems = bundle.validate(tmp_path)[0]
    assert problems[0][0] == "resources/gold.yaml (dev), task 'build'"
    assert "libraries entries need glob.include" in said(problems)


@pytest.mark.parametrize(
    "include, refused",
    [
        ("*.sql", True),
        ("sql/*.sql", True),
        ("sql/**/*", True),
        ("sql/", True),
        ("**", True),
        ("/", True),
        ("sql/range_ten.sql", False),
        ("sql/**", False),
    ],
)
def test_a_spark_pipeline_libraries_pattern_must_be_a_file_or_a_folder(tmp_path, include, refused):
    """Spark's CLI refuses `*.sql`; the bundle must refuse it first (#1031)."""
    make(
        tmp_path,
        resources={
            "gold.yaml": "name: shop-gold\ntasks:\n"
            "  - {name: build, spark-pipeline: {project: spark/shop}, writes: [gold.shop.trips]}\n"
        },
        files={
            **TASK_FILES,
            # the quotes matter: a YAML plain scalar may not start with `*` (an alias)
            "spark/shop/spark-pipeline.yml": "name: shop\nlibraries:\n"
            f'  - glob:\n      include: "{include}"\n'
            "configuration:\n  spark.sql.catalog.gold.type: rest\n",
        },
    )
    problems = bundle.validate(tmp_path)[0]
    if refused:
        assert problems, f"{include!r} passed validation"
        assert "must be a file path or a folder ending in /**" in said(problems)
        assert include in said(problems)
    else:
        assert not [p for p in problems if "glob.include" in p[1]]


@pytest.mark.parametrize("graph", ["# nothing yet\n", "[]\n", "- name: x\n"])
def test_a_spark_pipeline_yml_that_is_not_a_mapping_is_refused(tmp_path, graph):
    """A comment-only, empty or list graph is a problem, not an AttributeError (#972)."""
    make(
        tmp_path,
        resources={
            "gold.yaml": "name: shop-gold\ntasks:\n"
            "  - {name: build, spark-pipeline: {project: spark/shop}, writes: [gold.shop.trips]}\n"
        },
        files={**TASK_FILES, "spark/shop/spark-pipeline.yml": graph},
    )
    problems = bundle.validate(tmp_path)[0]
    assert problems[0][0].endswith("task 'build'")
    assert "spark-pipeline.yml must be a mapping" in said(problems)


def test_a_spark_pipeline_yml_that_is_a_mapping_but_names_no_catalog_still_says_so(tmp_path):
    make(
        tmp_path,
        resources={
            "gold.yaml": "name: shop-gold\ntasks:\n"
            "  - {name: build, spark-pipeline: {project: spark/shop}, writes: [gold.shop.trips]}\n"
        },
        files={**TASK_FILES, "spark/shop/spark-pipeline.yml": "name: shop\n"},
    )
    problems = bundle.validate(tmp_path)[0]
    assert "spark-pipeline.yml has no `spark.sql.catalog.gold.type` under configuration" in said(problems)


def test_a_spark_pipeline_task_needs_writes_for_its_target(tmp_path):
    make(
        tmp_path,
        resources={"gold.yaml": "name: shop-gold\ntasks:\n  - {name: build, spark-pipeline: {project: spark/shop}}\n"},
        files={**TASK_FILES, "spark/shop/spark-pipeline.yml": "name: shop\nconfiguration:\n  spark.sql.catalog.gold.type: rest\n"},
    )
    problems = bundle.validate(tmp_path)[0]
    assert problems[0][0] == "resources/gold.yaml (dev), task 'build'"
    assert "a spark-pipeline task needs writes: the catalog and schema the graph writes, as catalog.schema.table" in said(problems)


def test_a_spark_pipeline_task_has_one_catalog_and_schema(tmp_path):
    make(
        tmp_path,
        resources={
            "gold.yaml": "name: shop-gold\ntasks:\n"
            "  - {name: build, spark-pipeline: {project: spark/shop}, writes: [gold.shop.trips, silver.shop.trips]}\n"
        },
        files={**TASK_FILES, "spark/shop/spark-pipeline.yml": "name: shop\nconfiguration:\n  spark.sql.catalog.gold.type: rest\n"},
    )
    problems = bundle.validate(tmp_path)[0]
    assert "a spark-pipeline task writes into one catalog/schema, not gold.shop, silver.shop" in said(problems)


def test_a_spark_pipeline_yml_names_the_catalog_its_writes_use(tmp_path):
    make(
        tmp_path,
        resources={"gold.yaml": "name: shop-gold\ntasks:\n  - {name: build, spark-pipeline: {project: spark/shop}, writes: [gold.shop.trips]}\n"},
        files={**TASK_FILES, "spark/shop/spark-pipeline.yml": "name: shop\nconfiguration:\n  spark.sql.catalog.silver.type: rest\n"},
    )
    problems = bundle.validate(tmp_path)[0]
    assert "spark-pipeline.yml has no `spark.sql.catalog.gold.type` under configuration: the graph's session would not know catalog gold" in said(problems)


def test_a_wap_pipeline_with_a_check_and_a_spark_writer_validates_and_renders(tmp_path):
    """`publish: wap` (#114, #797): a quality check and a Spark task that names the tables it writes."""
    make(
        tmp_path,
        resources={
            "gold.yaml": "name: shop-gold\npublish: wap\ntasks:\n"
            "  - {name: build, spark-pipeline: {project: spark/shop}, writes: [gold.shop.daily]}\n"
            "  - {name: gate, quality: {checks: [{name: no_null_ids, sql: 'SELECT 1 WHERE 1=0'}]}}\n"
        },
        files={
            **TASK_FILES,
            "spark/shop/spark-pipeline.yml": "name: shop\nconfiguration:\n  spark.sql.catalog.gold.type: rest\n",
        },
    )
    assert bundle.validate(tmp_path, "prod")[0] == []
    problems, mode, specs = bundle.render(tmp_path, "prod", "alice")
    assert (problems, mode) == ([], "production")
    assert specs[0]["publish"] == "wap"
    assert specs[0]["tasks"][0]["writes"] == ["gold.shop.daily"]


def test_publish_wap_needs_a_check_and_a_writer(tmp_path):
    sentence = (
        "publish: wap needs a task list with a quality task that has a check "
        "and a spark-pipeline or notebook task with writes"
    )
    rows = [
        # no quality task at all
        "  - {name: build, spark-pipeline: {project: spark/shop}, writes: [gold.shop.daily]}\n",
        # a quality task without a check
        "  - {name: build, spark-pipeline: {project: spark/shop}, writes: [gold.shop.daily]}\n"
        "  - {name: gate, quality: {checks: []}}\n",
        # a check but no task that writes
        "  - {name: gate, quality: {checks: [{name: c, sql: 'select 1'}]}}\n",
    ]
    for index, tasks in enumerate(rows):
        directory = tmp_path / f"row{index}"
        directory.mkdir()
        make(
            directory,
            resources={"gold.yaml": "name: shop-gold\npublish: wap\ntasks:\n" + tasks},
            files={**TASK_FILES, "spark/shop/spark-pipeline.yml": "name: shop\n"},
        )
        assert sentence in said(bundle.validate(directory)[0]), tasks


def test_publish_must_be_direct_or_wap(tmp_path):
    task_bundle(tmp_path, ["{name: build, sql: select 1}"], extra="publish: maybe\n")
    assert "resources/gold.yaml (dev): publish must be direct or wap" in said(bundle.validate(tmp_path)[0])
    only = tmp_path / "direct"
    only.mkdir()
    task_bundle(only, ["{name: build, sql: select 1}"], extra="publish: direct\n")
    assert bundle.validate(only, "prod")[0] == []


def test_writes_are_up_to_16_catalog_schema_tables_of_a_spark_or_notebook_task(tmp_path):
    bad_table = tmp_path / "bad-table"
    bad_table.mkdir()
    task_bundle(bad_table, ["{name: build, notebook: {file: notebooks/explore.ipynb}, writes: [gold.shop]}"])
    assert "writes must be a list of at most 16 tables, each catalog.schema.table" in said(bundle.validate(bad_table)[0])

    too_many = tmp_path / "too-many"
    too_many.mkdir()
    task_bundle(
        too_many,
        [
            "{name: build, notebook: {file: notebooks/explore.ipynb}, writes: ["
            + ", ".join(f"gold.shop.t{index}" for index in range(17))
            + "]}"
        ],
    )
    assert "writes must be a list of at most 16 tables, each catalog.schema.table" in said(bundle.validate(too_many)[0])

    sql_task = tmp_path / "sql-task"
    sql_task.mkdir()
    task_bundle(sql_task, ["{name: build, sql: select 1, writes: [gold.shop.daily]}"])
    assert "only a spark-pipeline or notebook task declares writes" in said(bundle.validate(sql_task)[0])

    notebook = tmp_path / "notebook"
    notebook.mkdir()
    task_bundle(notebook, ["{name: build, notebook: {file: notebooks/explore.ipynb}, writes: [gold.shop.daily]}"])
    assert bundle.validate(notebook, "prod")[0] == []


def test_dbt_task_rules_are_reported(tmp_path):
    needs_select = "a dbt task needs exactly one of select (one dbt selector) and batches (a list of 1 to 8 selectors, run in order)"
    rows = [
        (["{name: x, dbt: {project: Dbt, select: tag:marts}}"], "a dbt task needs project (a directory under /opt/dbt/projects in the tool image)"),
        (["{name: x, dbt: {project: shop, select: tag:marts, batches: [tag:marts]}}"], needs_select),
        (["{name: x, dbt: {project: shop}}"], needs_select),
        (["{name: x, dbt: {project: shop, select: tag:marts, vars: {when: 3}}}"], "dbt vars must be a mapping of string values"),
        (["{name: x, dbt: {project: shop, select: tag:marts, model: x}}"], "dbt tasks accept only project, select, batches, threads and vars (found `model`)"),
    ]
    for index, (tasks, expected) in enumerate(rows):
        directory = tmp_path / f"row{index}"
        directory.mkdir()
        task_bundle(directory, tasks)
        problems = bundle.validate(directory)[0]
        assert problems[0][0] == "resources/gold.yaml (dev), task 'x'"
        assert expected in said(problems), problems


def test_a_top_level_dbt_is_refused_with_the_task_sentence(tmp_path):
    task_bundle(tmp_path, ["{name: build, sql: select 1}"], extra="dbt: {project: shop, select: tag:marts}\n")
    assert "a dbt run is a task: put it under tasks: with a name" in said(bundle.validate(tmp_path)[0])


def test_a_task_problem_names_the_file_and_the_task(tmp_path):
    task_bundle(tmp_path, ["{name: customer_ltv, sql: select 1, sqlFile: sql/schema.sql}"])
    problems, _, _ = bundle.validate(tmp_path, "prod")
    assert problems[0][0] == "resources/gold.yaml (prod), task 'customer_ltv'"
    assert problems[0][1] == "give exactly one of sql, sqlFile, notebook, dbt, quality or spark-pipeline"


def test_task_rules_are_reported(tmp_path):
    rows = [
        (["{name: x, sql: select 1, sqlFile: sql/schema.sql}"], "task 'x': give exactly one of sql, sqlFile, notebook, dbt, quality or spark-pipeline"),
        (["{name: x, sql: select 1}", "{name: x, sql: select 2}"], "the name is used twice"),
        (["{name: x, sql: select 1, dependsOn: [nope]}"], "dependsOn names 'nope'"),
        (["{name: a, sql: select 1, dependsOn: [b]}", "{name: b, sql: select 2, dependsOn: [a]}"], "dependsOn is a cycle"),
        (["{name: x, sqlFile: sql/nope.sql}"], "is not among the bundle's files"),
        ([f"{{name: t{index}, sql: select 1}}" for index in range(31)], "at most 30 tasks"),
        (
            [*(f"{{name: dep{index}, sql: select 1}}" for index in range(9)), "{name: x, sql: select 1, dependsOn: [dep0, dep1, dep2, dep3, dep4, dep5, dep6, dep7, dep8]}"],
            "at most 8 dependsOn entries",
        ),
    ]
    for index, (tasks, expected) in enumerate(rows):
        directory = tmp_path / f"row{index}"
        directory.mkdir()
        task_bundle(directory, tasks)
        problems = said(bundle.validate(directory)[0])
        assert expected in problems, problems


def test_a_pipeline_cannot_give_both_tasks_and_one_source(tmp_path):
    task_bundle(tmp_path, ["{name: x, sql: select 1}"], extra="sql: select 1\n")
    assert "give tasks, or one of sql, sqlFile or notebook — not both" in said(bundle.validate(tmp_path)[0])


def test_a_pipeline_description_validates_and_round_trips(tmp_path):
    """`spec.description` (#327): the validator leaves the key alone, and `generate` keeps it.

    The bundle has no allowlist for pipeline keys, so a description needs no code here; a test
    proves it instead.
    """
    described = "name: load\ndescription: Loads the shop's bronze layer\nsqlFile: sql/load.sql\n"
    (tmp_path / "validated").mkdir()
    assert bundle.validate(make(tmp_path / "validated", resources={"load.yaml": described}))[0] == []

    class Nova:
        """The one call `generate` makes: the pipeline as Nova returns it."""

        def call(self, method, path=None):
            assert (method, path) == ("pipelines_spec", {"namespace": "stardelt", "name": "daily"})
            return {
                "managedBy": "nova",
                "editable": True,
                "document": {
                    "name": "daily",
                    "ownerGroup": "sales",
                    "description": "Loads the shop's bronze layer",
                    "sql": "select 1",
                },
            }

    directory = tmp_path / "round-trip"
    directory.mkdir()
    bundle.generate(Nova(), make(directory), "stardelt", "daily")
    text = (directory / "resources" / "daily.yaml").read_text()
    assert yaml.safe_load(text)["description"] == "Loads the shop's bronze layer"
    assert "description" in text


def test_generate_writes_a_dbt_task_list(tmp_path):
    class Nova:
        """The one call `generate` makes: a task list with a statement, a dependency and a dbt run."""

        def call(self, method, path=None):
            assert (method, path) == ("pipelines_spec", {"namespace": "stardelt", "name": "daily"})
            return {
                "managedBy": "nova",
                "editable": False,
                "document": {
                    "name": "daily",
                    "ownerGroup": "sales",
                    "tasks": [
                        {"name": "load", "sql": "select 1"},
                        {"name": "build", "dependsOn": ["load"], "dbt": {"project": "shop", "batches": ["tag:seed"], "threads": 4}},
                    ],
                },
            }

    directory = tmp_path / "task-list"
    directory.mkdir()
    bundle.generate(Nova(), make(directory), "stardelt", "daily")
    written = yaml.safe_load((directory / "resources" / "daily.yaml").read_text())
    assert written["tasks"][1]["dbt"] == {"project": "shop", "batches": ["tag:seed"], "threads": 4}
    assert (directory / "sql" / "daily-load.sql").read_text() == "select 1\n"
    assert bundle.validate(directory)[0] == []


def test_generate_refuses_a_notebook_task_list(tmp_path):
    class Nova:
        """The one call `generate` makes: a task list with a notebook task."""

        def call(self, method, path=None):
            assert (method, path) == ("pipelines_spec", {"namespace": "stardelt", "name": "daily"})
            return {
                "managedBy": "nova",
                "editable": False,
                "document": {
                    "name": "daily",
                    "ownerGroup": "sales",
                    "tasks": [{"name": "nb", "notebook": {"name": "a.ipynb", "objectVersion": "v1"}}],
                },
            }

    directory = tmp_path / "notebook-task"
    directory.mkdir()
    with pytest.raises(bundle.GenerateError, match="notebook task"):
        bundle.generate(Nova(), make(directory), "stardelt", "daily")


def _git(directory, *args):
    subprocess.run(["git", "-C", str(directory), *args], check=True, capture_output=True, text=True)


def test_source_of_a_committed_file(tmp_path):
    if not shutil.which("git"):
        pytest.skip("git is not installed")
    _git(tmp_path, "init")
    notebook = tmp_path / "notes" / "a.ipynb"
    notebook.parent.mkdir()
    notebook.write_text("{}")
    _git(tmp_path, "remote", "add", "origin", "git@github.com:acme/repo.git")
    _git(tmp_path, "-c", "user.name=t", "-c", "user.email=t@t", "add", "-A")
    _git(tmp_path, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-m", "one")
    commit = subprocess.run(["git", "-C", str(tmp_path), "rev-parse", "HEAD"], capture_output=True, text=True, check=True).stdout.strip()

    assert bundle.source_of(notebook) == {
        "repository": "https://github.com/acme/repo",
        "commit": commit,
        "path": "notes/a.ipynb",
        "clean": True,
    }

    notebook.write_text("{} ")
    assert bundle.source_of(notebook)["clean"] is False


def test_source_of_outside_a_repository_is_none(tmp_path):
    if not shutil.which("git"):
        pytest.skip("git is not installed")
    directory = tmp_path / "plain"
    directory.mkdir()
    (directory / "a.ipynb").write_text("{}")
    assert bundle.source_of(directory / "a.ipynb") is None


def test_a_notebook_pipeline_keeps_its_connection_when_the_version_is_pinned():
    spec = {"name": "load", "notebook": {"file": "notebooks/explore.ipynb", "connection": "rnacentral", "params": '{"a": "1"}'}}
    pinned = bundle._pinned(spec, {"objectVersion": "0f1e2d3c"})
    assert pinned["notebook"] == {"name": "explore.ipynb", "objectVersion": "0f1e2d3c", "params": '{"a": "1"}', "connection": "rnacentral"}
    assert bundle._pinned({"name": "load", "notebook": {"file": "notebooks/explore.ipynb"}}, {"objectVersion": "0f1e2d3c"})["notebook"] == {
        "name": "explore.ipynb",
        "objectVersion": "0f1e2d3c",
    }


def test_a_notebook_connection_must_be_a_connection_name(tmp_path):
    files = {"sql/load.sql": "select 1", "notebooks/explore.ipynb": "{}"}
    for connection in ("rnacentral", "crm-db-2"):
        problems = bundle.validate(make(tmp_path, resources={"load.yaml": f"name: load\nnotebook: {{file: notebooks/explore.ipynb, connection: {connection}}}\n"}, files=dict(files)))[0]
        assert problems == [], connection
    for connection in ("RNAcentral", "a_b"):
        resources = {"load.yaml": f"name: load\nnotebook: {{file: notebooks/explore.ipynb, connection: {connection}}}\n"}
        problems = bundle.validate(make(tmp_path, resources=resources, files=dict(files)))[0]
        assert any("notebook.connection is a Connection name" in message for _, message in problems), problems


def test_a_password_in_a_bundle_file_is_a_warning_and_not_a_problem(tmp_path):
    make(tmp_path, files={
        "sql/load.sql": "select 1",  # created by the test (new)
        "notebooks/explore.ipynb": "{}",  # created by the test (new)
        "sql/leak.sql": "select * from x where password = 'hunter2hunter2'",  # created by the test (new)
    })
    problems, warnings, _ = bundle.validate(tmp_path)
    assert problems == []
    assert warnings == [
        "sql/leak.sql: looks like it holds password; a bundle file is delivered to every stage, so keep it in a Connection",
        "variable 'unused' is declared but no pipeline file uses it",
    ]


def test_a_private_key_in_a_notebook_is_a_warning(tmp_path):
    make(tmp_path, files={
        "sql/load.sql": "select 1",  # created by the test (new)
        "notebooks/explore.ipynb": '{"cells": [{"source": "-----BEGIN RSA PRIVATE KEY-----"}]}',  # created by the test (new)
    })
    problems, warnings, _ = bundle.validate(tmp_path)
    assert problems == []
    assert warnings == [
        "notebooks/explore.ipynb: looks like it holds a private key; a bundle file is delivered to every stage, so keep it in a Connection",
        "variable 'unused' is declared but no pipeline file uses it",
    ]


def test_the_command_prints_the_credential_warning_and_exits_zero(tmp_path):
    make(tmp_path, files={
        "sql/load.sql": "select 1",  # created by the test (new)
        "notebooks/explore.ipynb": "{}",  # created by the test (new)
        "sql/leak.sql": "api_key = 'abcdefgh1234'",  # created by the test (new)
    })
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(["bundle", "validate", str(tmp_path)], environ={}, stdout=out, stderr=err)
    assert code == 0
    assert "stardelt: warning: sql/leak.sql: looks like it holds api_key" in err.getvalue()
