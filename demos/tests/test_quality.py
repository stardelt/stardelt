import logging

import pytest
from stardelt_pipelines.plan import PlanError, plan
from stardelt_pipelines.quality import Check, evaluate


def resource(**spec):
    return {"metadata": {"name": "daily"}, "spec": spec}


def quality(name="assert", checks=None, **extra):
    task = {"name": name, "kind": "quality"}
    if checks is not None:
        task["checks"] = checks
    task.update(extra)
    return task


def check(name, sql="SELECT 1 WHERE 1=0", **extra):
    item = {"name": name, "sql": sql}
    item.update(extra)
    return item


def test_a_quality_task_carries_its_checks():
    p = plan(
        resource(
            tasks=[
                quality(
                    checks=[
                        check("a", "SELECT 1 WHERE 1=0"),
                        check("b", "SELECT 1", severity="warn"),
                    ]
                )
            ]
        ),
        "t",
    )
    (gate,) = p.tasks
    assert gate.kind == "quality"
    assert gate.checks == (
        Check("a", "SELECT 1 WHERE 1=0"),
        Check("b", "SELECT 1", "warn"),
    )


def test_quality_task_without_checks_is_refused():
    with pytest.raises(PlanError) as error:
        plan(resource(tasks=[quality()]), "t")
    message = str(error.value)
    assert "assert" in message
    assert "needs checks (a list of 1 to 50 SQL assertions)" in message


def test_51_checks_are_refused():
    with pytest.raises(PlanError) as error:
        plan(
            resource(tasks=[quality(checks=[check(f"c{i}") for i in range(51)])]),
            "t",
        )
    assert "needs checks (a list of 1 to 50 SQL assertions)" in str(error.value)


def test_a_duplicate_check_name_is_refused():
    with pytest.raises(PlanError) as error:
        plan(resource(tasks=[quality(checks=[check("a"), check("a")])]), "t")
    assert str(error.value) == "check name 'a' is used twice in quality task 'assert'"


def test_a_check_without_sql_is_refused():
    with pytest.raises(PlanError) as error:
        plan(resource(tasks=[quality(checks=[{"name": "a"}])]), "t")
    assert str(error.value) == (
        "check 'a' in quality task 'assert': sql must be a non-empty string of at most 20000 "
        "characters"
    )


def test_an_unknown_severity_is_refused():
    with pytest.raises(PlanError) as error:
        plan(resource(tasks=[quality(checks=[check("a", severity="fatal")])]), "t")
    assert str(error.value) == (
        "check 'a' in quality task 'assert': severity must be error or warn (found `fatal`)"
    )


def test_an_unknown_key_is_refused():
    with pytest.raises(PlanError) as error:
        plan(resource(tasks=[quality(checks=[check("a", rows=3)])]), "t")
    assert str(error.value) == (
        "check 'a' in quality task 'assert' accepts only name, sql and severity (found `rows`)"
    )


def test_the_audit_table_placeholder_is_refused():
    with pytest.raises(PlanError) as error:
        plan(
            resource(tasks=[quality(checks=[check("a", "SELECT * FROM ${table}")])]),
            "t",
        )
    assert str(error.value) == (
        "check 'a' in quality task 'assert' uses ${table}: that placeholder is the audit branch "
        "(publish: wap), which has no runtime yet"
    )


def test_a_quality_task_cannot_name_a_connection():
    with pytest.raises(PlanError) as error:
        plan(resource(tasks=[quality(checks=[check("a")], connection="crm-db")]), "t")
    assert "cannot name a connection" in str(error.value)


# --- the gate: an `error` check fails the task, a `warn` check only says so ---------------------


def test_only_error_checks_with_rows_fail_the_gate(caplog):
    # The stub is keyed by `Check.name`: each check's sql is that name.
    checks = [
        Check("a", "a"),
        Check("b", "b", "warn"),
        Check("c", "c"),
    ]
    rows = {"a": 0, "b": 2, "c": 1}
    with caplog.at_level(logging.INFO, logger="stardelt_pipelines.quality"):
        failed = evaluate(checks, lambda sql: rows[sql])
    assert failed == [("c", 1)]
    lines = [record.getMessage() for record in caplog.records]
    assert "check a: error, 0 row(s)" in lines
    assert "check b: warn, 2 row(s)" in lines
    assert "check c: error, 1 row(s)" in lines


def test_a_passing_gate_returns_no_failures():
    assert evaluate([Check("a", "SELECT 1")], lambda sql: 0) == []
