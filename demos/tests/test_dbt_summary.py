"""The `dbt` task's log summary: models built, tests passed, and the failing node named (#112).

The image file is not an importable package, so it is loaded by path — no new test dependency.
"""
import importlib.util
import json
from pathlib import Path

PATH = Path(__file__).resolve().parents[1] / "images" / "dbt" / "run_summary.py"
spec = importlib.util.spec_from_file_location("run_summary", PATH)
run_summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run_summary)


def passing() -> dict:
    return {
        "results": [
            {"unique_id": "model.shop.daily_revenue", "status": "success"},
            {"unique_id": "seed.shop.customers", "status": "success"},
            {"unique_id": "test.shop.revenue_is_not_negative", "status": "pass"},
        ]
    }


def test_all_passing():
    """Three nodes, all good: the counts, then one status line and nothing to name."""
    assert run_summary.summarize(passing()) == [
        "dbt: 3 nodes — 1 model, 1 seed, 1 test",
        "dbt: 3 passed",
    ]


def test_warnings_and_failure_are_named():
    """One warning and one failure: the failing node first, each with dbt's one-line reason.

    The ticket wrote the status line as `3 passed, 1 failed, 2 warnings` and its prose as "two
    warnings", but the fixture it gives adds one `warn` entry to 5 nodes — its own rule (one
    status per entry) can only print `1 warning`, as its live example does (`33 passed, 1
    failed, 1 warning` for 35 nodes).
    """
    document = passing()
    document["results"].extend(
        [
            {
                "unique_id": "test.shop.units_reconcile",
                "status": "warn",
                "message": "Got 2 results, configured to warn if != 0",
            },
            {
                "unique_id": "test.shop.unique_customers_id",
                "status": "fail",
                "message": "Got 1 result, configured to fail if != 0",
            },
        ]
    )
    assert run_summary.summarize(document) == [
        "dbt: 5 nodes — 1 model, 1 seed, 3 tests",
        "dbt: 3 passed, 1 failed, 1 warning",
        "dbt: ✗ test.shop.unique_customers_id — Got 1 result, configured to fail if != 0",
        "dbt: ! test.shop.units_reconcile — Got 2 results, configured to warn if != 0",
    ]


def test_missing_file_says_so_and_succeeds(capsys):
    """A run that failed before its first node: the line, and the summary itself exits 0."""
    assert run_summary.main(["/nonexistent/run_results.json"]) == 0
    assert (
        capsys.readouterr().out
        == "dbt: no run_results.json at /nonexistent/run_results.json\n"
    )


def manifest() -> dict:
    """The run's manifest: the model's table, and the test that depends on it."""
    return {
        "nodes": {
            "model.shop.customers": {
                "unique_id": "model.shop.customers",
                "relation_name": '"bronze"."shop_dbt"."customers"',
            },
            "test.shop.unique_customers_id": {
                "unique_id": "test.shop.unique_customers_id",
                "depends_on": {"nodes": ["model.shop.customers"]},
            },
        }
    }


def failing() -> dict:
    document = passing()
    document["results"].append(
        {
            "unique_id": "test.shop.unique_customers_id",
            "status": "fail",
            "message": "Got 1 result, configured to fail if != 0",
        }
    )
    return document


def test_a_failing_test_names_the_table_it_guards():
    """With the manifest, the ✗ line carries the table the test protects, unquoted."""
    assert run_summary.summarize(failing(), manifest()) == [
        "dbt: 4 nodes — 1 model, 1 seed, 2 tests",
        "dbt: 3 passed, 1 failed",
        "dbt: ✗ test.shop.unique_customers_id (bronze.shop_dbt.customers) — Got 1 result, configured to fail if != 0",
    ]


def test_a_missing_manifest_keeps_the_plain_line():
    """Without a manifest the ✗ line is the one #112 ships today."""
    assert run_summary.summarize(failing(), None)[2] == (
        "dbt: ✗ test.shop.unique_customers_id — Got 1 result, configured to fail if != 0"
    )


def test_main_reads_the_manifest_beside_the_run_results(tmp_path, capsys):
    (tmp_path / "manifest.json").write_text(json.dumps(manifest()))
    (tmp_path / "run_results.json").write_text(json.dumps(failing()))
    assert run_summary.main([str(tmp_path / "run_results.json")]) == 0
    assert "dbt: ✗ test.shop.unique_customers_id (bronze.shop_dbt.customers)" in capsys.readouterr().out


def test_main_without_a_manifest_prints_the_plain_line(tmp_path, capsys):
    (tmp_path / "run_results.json").write_text(json.dumps(failing()))
    assert run_summary.main([str(tmp_path / "run_results.json")]) == 0
    assert "dbt: ✗ test.shop.unique_customers_id — Got 1 result" in capsys.readouterr().out
