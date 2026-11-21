import logging

import pytest
from stardelt_pipelines.plan import WAP_NO_RUNTIME, PlanError, plan, refuse_unrunnable
from stardelt_pipelines.quality import Check, evaluate


def resource(**spec):
    return {"metadata": {"name": "daily"}, "spec": spec}


def principal(name="pipeline-analytics", group="analytics"):
    """A ready `ServicePrincipal` of the namespace, as the controller fills its status."""
    return {
        "metadata": {"name": name},
        "spec": {"ownerGroup": group},
        "status": {"ready": True, "secretName": f"sp-{name}"},
    }


def sql_task(name="load"):
    return {"name": name, "kind": "sql", "sql": "SELECT 1"}


def notebook_task(name="report", writes=None):
    task = {
        "name": name,
        "kind": "notebook",
        "config": {"notebook": "report.ipynb", "objectVersion": "v1"},
    }
    if writes is not None:
        task["writes"] = writes
    return task


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


def test_the_audit_table_placeholder_is_refused_without_wap():
    with pytest.raises(PlanError) as error:
        plan(
            resource(tasks=[quality(checks=[check("a", "SELECT * FROM ${table}")])]),
            "t",
        )
    assert str(error.value) == (
        "check 'a' in quality task 'assert' uses ${table}, which needs publish: wap in the "
        "pipeline"
    )


def test_a_wap_plan_carries_publish_and_the_audit_table_placeholder():
    p = plan(
        resource(
            publish="wap",
            ownerGroup="analytics",
            runAs="pipeline-analytics",
            tasks=[
                notebook_task(writes=["gold.shop.t"]),
                quality(checks=[check("a", "SELECT count(*) FROM ${table}")]),
            ],
        ),
        "t",
        {"pipeline-analytics": principal()},
    )
    assert p.publish == "wap"
    assert p.tasks[0].writes == ("gold.shop.t",)
    assert p.tasks[1].checks[0].sql == "SELECT count(*) FROM ${table}"


def test_a_wap_plan_without_a_writing_task_is_refused():
    with pytest.raises(PlanError) as error:
        plan(
            resource(
                publish="wap",
                tasks=[sql_task()],
            ),
            "t",
        )
    assert str(error.value) == "publish: wap needs a task that writes at least one table"


def test_a_wap_plan_is_unrunnable_until_the_loader_wires_it():
    p = plan(
        resource(
            publish="wap",
            ownerGroup="analytics",
            runAs="pipeline-analytics",
            tasks=[notebook_task(writes=["gold.shop.t"])],
        ),
        "t",
        {"pipeline-analytics": principal()},
    )
    with pytest.raises(PlanError) as error:
        refuse_unrunnable(p)
    assert str(error.value) == WAP_NO_RUNTIME


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
