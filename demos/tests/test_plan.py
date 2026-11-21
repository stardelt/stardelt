import json

import pytest
from stardelt_pipelines.plan import (
    PlanError,
    PrincipalRef,
    parse_duration,
    plan,
    refuse_unrunnable,
)


def resource(**spec):
    return {"metadata": {"name": "daily"}, "spec": spec}


def sql(name, *deps):
    task = {"name": name, "kind": "sql", "sql": "SELECT 1"}
    if deps:
        task["dependsOn"] = list(deps)
    return task


def principal(name="pipeline-analytics", group="analytics", ready=True, secret="auto", message=None):
    """A `ServicePrincipal` as the API server returns it: the controller fills `status`."""
    if secret == "auto":
        secret = f"sp-{name}"
    status = {"ready": ready}
    if secret is not None:
        status["secretName"] = secret
    if message is not None:
        status["conditions"] = [
            {"type": "Ready", "status": "True" if ready else "False", "message": message}
        ]
    return {"metadata": {"name": name}, "spec": {"ownerGroup": group}, "status": status}


def test_a_minimal_pipeline_gets_the_defaults():
    p = plan(resource(tasks=[sql("a")]), "team")
    assert p.dag_id == "team__daily"
    assert p.cron is None and p.timezone == "UTC"
    assert p.retries == 0 and p.retry_delay_s == 60
    assert p.timeout_s is None and p.max_active_runs == 1
    assert p.run_as is None and p.params == {}


def test_every_field_is_carried():
    p = plan(
        resource(
            tasks=[sql("a"), sql("b", "a")],
            schedule={"cron": "0 3 * * *", "timezone": "Europe/Berlin"},
            ownerGroup="analytics",
            runAs="pipeline-analytics",
            params={"days": "1"},
            retries={"attempts": 2, "backoff": "5m"},
            timeout="2h",
            concurrency="Allow",
        ),
        "team",
        {"pipeline-analytics": principal()},
    )
    assert (p.cron, p.timezone) == ("0 3 * * *", "Europe/Berlin")
    assert (p.retries, p.retry_delay_s, p.timeout_s) == (2, 300, 7200)
    assert p.max_active_runs == 16 and p.owner_group == "analytics"
    assert p.run_as == "pipeline-analytics" and p.principal.name == "pipeline-analytics"
    assert p.params == {"days": "1"}
    assert [t.depends_on for t in p.tasks] == [(), ("a",)]


def test_a_diamond_is_fine():
    p = plan(resource(tasks=[sql("a"), sql("b", "a"), sql("c", "a"), sql("d", "b", "c")]), "t")
    assert len(p.tasks) == 4


@pytest.mark.parametrize(
    "tasks, message",
    [
        ([], "no tasks"),
        ([sql("a"), sql("a")], "used twice"),
        ([sql("a", "ghost")], "ghost"),
        ([sql("a", "a")], "depends on `a`"),
        ([sql("a", "c"), sql("b", "a"), sql("c", "b")], "circle"),
        ([{"name": "a", "kind": "sql"}], "needs a sql statement"),
        ([{"kind": "sql", "sql": "x"}], "no name"),
    ],
)
def test_broken_shapes_are_plan_errors(tasks, message):
    with pytest.raises(PlanError, match=message):
        plan(resource(tasks=tasks), "t")


def test_a_resource_without_a_name_is_rejected():
    with pytest.raises(PlanError, match="no name"):
        plan({"spec": {"tasks": [sql("a")]}}, "t")


@pytest.mark.parametrize("kind", ["python"])
def test_a_kind_without_a_runtime_is_refused(kind):
    # The API accepts these kinds ahead of their runtimes; the plan must refuse them so the
    # loader skips the DAG at parse time instead of failing when the schedule fires.
    with pytest.raises(PlanError, match="has no runtime yet"):
        plan(resource(tasks=[{"name": "a", "kind": kind}]), "t")


@pytest.mark.parametrize("text, seconds", [("30s", 30), ("5m", 300), ("2h", 7200), ("1d", 86400)])
def test_durations(text, seconds):
    assert parse_duration(text, None) == seconds


def test_duration_defaults_and_errors():
    assert parse_duration(None, 60) == 60 and parse_duration(None, None) is None
    for bad in ("5", "m5", "5 m", "1.5h", ""):
        with pytest.raises(PlanError):
            parse_duration(bad, None)


# --- the principal: a pipeline with an owner runs as that group's principal ---------------------


def test_plan_without_owner_or_runas_uses_the_default():
    p = plan(resource(tasks=[sql("a")]), "team", {"pipeline-analytics": principal()})
    assert p.owner_group is None
    assert p.principal is None


def test_plan_with_owner_and_no_runas_is_refused():
    with pytest.raises(PlanError) as error:
        plan(resource(tasks=[sql("a")], ownerGroup="analytics"), "team")
    assert str(error.value) == (
        "a pipeline with an owner needs runAs: the principal of its group (pipeline-analytics)"
    )


def test_plan_with_runas_and_no_owner_is_refused():
    with pytest.raises(PlanError) as error:
        plan(
            resource(tasks=[sql("a")], runAs="pipeline-analytics"),
            "team",
            {"pipeline-analytics": principal()},
        )
    assert str(error.value) == "runAs needs ownerGroup: say which group owns this pipeline"


def test_plan_with_unknown_principal_is_refused():
    with pytest.raises(PlanError) as error:
        plan(
            resource(tasks=[sql("a")], ownerGroup="analytics", runAs="pipeline-analytics"),
            "team",
            {"pipeline-sales": principal("pipeline-sales", "sales")},
        )
    assert str(error.value) == "runAs pipeline-analytics is not a ServicePrincipal of this namespace"


def test_plan_refuses_a_principal_of_another_group():
    with pytest.raises(PlanError) as error:
        plan(
            resource(tasks=[sql("a")], ownerGroup="analytics", runAs="pipeline-analytics"),
            "team",
            {"pipeline-analytics": principal(group="sales")},
        )
    message = str(error.value)
    assert message == (
        "runAs pipeline-analytics belongs to the group sales, this pipeline is owned by analytics"
    )
    assert "sales" in message and "analytics" in message


def test_plan_refuses_a_principal_that_is_not_ready():
    with pytest.raises(PlanError) as error:
        plan(
            resource(tasks=[sql("a")], ownerGroup="analytics", runAs="pipeline-analytics"),
            "team",
            {"pipeline-analytics": principal(ready=False, message="the client does not exist yet")},
        )
    assert str(error.value) == (
        "runAs pipeline-analytics is not ready: the client does not exist yet"
    )


def test_plan_refuses_a_principal_whose_status_is_empty():
    controller_has_not_run = {
        "metadata": {"name": "pipeline-analytics"},
        "spec": {"ownerGroup": "analytics"},
    }
    with pytest.raises(PlanError) as error:
        plan(
            resource(tasks=[sql("a")], ownerGroup="analytics", runAs="pipeline-analytics"),
            "team",
            {"pipeline-analytics": controller_has_not_run},
        )
    assert str(error.value) == "runAs pipeline-analytics is not ready: no status yet"


def test_plan_refuses_a_principal_without_a_secret_name():
    with pytest.raises(PlanError) as error:
        plan(
            resource(tasks=[sql("a")], ownerGroup="analytics", runAs="pipeline-analytics"),
            "team",
            {"pipeline-analytics": principal(secret=None)},
        )
    assert str(error.value) == "runAs pipeline-analytics has no credentials Secret yet"


def test_plan_accepts_the_group_principal():
    p = plan(
        resource(tasks=[sql("a")], ownerGroup="analytics", runAs="pipeline-analytics"),
        "team",
        {"pipeline-analytics": principal()},
    )
    assert p.principal == PrincipalRef("pipeline-analytics", "sp-pipeline-analytics")
    assert (p.owner_group, p.run_as) == ("analytics", "pipeline-analytics")


def test_the_default_name_is_not_a_group_principal():
    # The shared default principal has no group. Even a resource of that name must not let a
    # pipeline claim it: a group's pipeline names that group's principal.
    with pytest.raises(PlanError) as error:
        plan(
            resource(tasks=[sql("a")], ownerGroup="analytics", runAs="pipeline-runner"),
            "team",
            {"pipeline-runner": principal("pipeline-runner", "analytics")},
        )
    assert str(error.value) == "runAs pipeline-runner is not a ServicePrincipal of this namespace"



# --- the notebook task: a published object version, run as the group's principal ----------------


def notebook(
    name="report", file="Report - daily.ipynb", version="version-1", params=None, **extra
):
    config = {"notebook": file, "objectVersion": version}
    if params is not None:
        config["params"] = params
    task = {"name": name, "kind": "notebook", "config": config}
    task.update(extra)
    return task


def owned(*tasks, **spec):
    """A pipeline with the group principal the notebook task needs."""
    spec.setdefault("ownerGroup", "analytics")
    spec.setdefault("runAs", "pipeline-analytics")
    return resource(tasks=list(tasks), **spec), "team", {"pipeline-analytics": principal()}


def test_plan_accepts_a_notebook_task():
    p = plan(
        *owned(
            sql("prepare"),
            notebook("report", params='{"days": "3"}', dependsOn=["prepare"]),
            params={"region": "eu"},
        )
    )
    assert p.name == "daily"
    prepare, report = p.tasks
    assert prepare.notebook is None and prepare.object_version is None
    assert prepare.notebook_params == ()
    assert report.notebook == "Report - daily.ipynb"
    assert report.object_version == "version-1"
    assert report.notebook_params == (("days", "3"),)
    assert report.depends_on == ("prepare",)
    assert (p.owner_group, p.principal.name) == ("analytics", "pipeline-analytics")


@pytest.mark.parametrize(
    "config",
    [
        None,
        {},
        {"notebook": "report.ipynb"},
        {"objectVersion": "version-1"},
        {"notebook": "report.txt", "objectVersion": "version-1"},
        {"notebook": "/report.ipynb", "objectVersion": "version-1"},
        {"notebook": "report.ipynb", "objectVersion": ""},
        {"notebook": "report.ipynb", "objectVersion": "two words"},
        {"notebook": "report.ipynb", "objectVersion": "v" * 129},
    ],
)
def test_plan_refuses_missing_file_or_version(config):
    task = {"name": "report", "kind": "notebook"}
    if config is not None:
        task["config"] = config
    with pytest.raises(PlanError) as error:
        plan(*owned(task))
    assert str(error.value) == (
        "a notebook task needs config.notebook (a .ipynb file name) and config.objectVersion "
        "(the published version)"
    )


def test_refuses_extra_config_keys():
    task = {
        "name": "report",
        "kind": "notebook",
        "config": {
            "notebook": "report.ipynb",
            "objectVersion": "version-1",
            "path": "sales/report.ipynb",
        },
    }
    with pytest.raises(PlanError) as error:
        plan(*owned(task))
    assert str(error.value) == (
        "notebook tasks accept only notebook, objectVersion and params (found `path`)"
    )


@pytest.mark.parametrize(
    "params",
    [
        "not json",
        '["days"]',
        '{"days": 3}',
        '{"days": null}',
        json.dumps({f"p{index}": "1" for index in range(33)}),
    ],
)
def test_refuses_bad_params(params):
    with pytest.raises(PlanError) as error:
        plan(*owned(notebook(params=params)))
    assert str(error.value) == (
        "notebook params must be a JSON object string of string values, at most 32 keys"
    )


def test_refuses_notebook_without_owner_group():
    with pytest.raises(PlanError) as error:
        plan(resource(tasks=[notebook()]), "team")
    assert str(error.value) == (
        "a notebook task runs as the group principal of its pipeline: set ownerGroup and runAs"
    )


# --- the connection: a notebook task's credentials for an outside system ------------------------


def connection(**spec):
    """A `Connection` resource as the API server returns it."""
    status = spec.pop("status", {"ready": True})
    spec.setdefault("scope", "group")
    spec.setdefault("ownerGroup", "analytics")
    spec.setdefault("type", "postgresql")
    spec.setdefault("config", {"host": "db"})
    spec.setdefault("secret", {"keys": ["user", "password"]})
    name = spec.pop("name", "crm-db")
    return name, {"metadata": {"name": name}, "spec": spec, "status": status}


def test_a_notebook_task_receives_its_connections_keys():
    name, crm = connection()
    p = plan(
        *owned(notebook("report", connection=name)),
        connections={name: crm},
    )
    (report,) = p.tasks
    assert report.connection == "crm-db"
    assert report.connection_keys == ("user", "password")


def test_a_camel_case_key_is_allowed_it_is_upper_cased():
    name, crm = connection(secret={"keys": ["clientId", "clientSecret"]})
    p = plan(
        *owned(notebook("report", connection=name)),
        connections={name: crm},
    )
    (report,) = p.tasks
    assert report.connection_keys == ("clientId", "clientSecret")


def test_a_connection_without_secret_keys_needs_no_synced_secret():
    name, kafka = connection(name="bus", secret=None, status={"ready": False})
    p = plan(
        *owned(notebook("report", connection=name)),
        connections={name: kafka},
    )
    (report,) = p.tasks
    assert report.connection_keys == ()


@pytest.mark.parametrize(
    "task, connections, message",
    [
        (lambda: notebook("report", connection="ghost"), {}, "does not exist"),
        (
            lambda: notebook("report", connection="personal-db"),
            dict([connection(name="personal-db", scope="personal")]),
            "is personal",
        ),
        (
            lambda: notebook("report", connection="sales-db"),
            dict([connection(name="sales-db", scope="group", ownerGroup="sales")]),
            "belongs to group",
        ),
        (
            lambda: notebook("report", connection="shared-db"),
            dict([connection(name="shared-db", scope="managed", usableBy=["sales"])]),
            "does not list group",
        ),
        (
            lambda: dict(sql("load"), connection="crm-db"),
            dict([connection()]),
            "cannot name a connection",
        ),
        (
            lambda: notebook("report", connection="crm-db"),
            dict(
                [
                    connection(
                        status={
                            "ready": False,
                            "conditions": [
                                {
                                    "type": "Ready",
                                    "reason": "ExternalSecretsNotInstalled",
                                    "message": "External Secrets Operator is not installed",
                                }
                            ],
                        }
                    )
                ]
            ),
            "has no Kubernetes Secret yet",
        ),
        (
            lambda: notebook("report", connection="crm-db"),
            dict([connection(secret={"keys": ["user", "api-key"]})]),
            "cannot become an environment variable",
        ),
    ],
)
def test_a_connection_must_exist_and_belong_to_the_pipelines_group(task, connections, message):
    with pytest.raises(PlanError, match=message):
        plan(*owned(task()), connections=connections)


# --- the spark-pipeline task: the graph rides a pod and stardelt owns catalog and database ------

GRAPH = """\
name: graph
configuration:
  spark.sql.catalog.playground: org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.playground.type: rest
libraries:
  - glob:
      include: sql/**
"""


def spark(name="build", files=None, writes=("playground.scratch.range_ten",), **extra):
    if files is None:
        files = {"spark-pipeline.yml": GRAPH, "sql/range_ten.sql": "SELECT 1"}
    task = {"name": name, "kind": "spark-pipeline", "files": files, "writes": list(writes)}
    task.update(extra)
    return task


def test_plan_accepts_a_spark_pipeline_task():
    p = plan(
        *owned(spark(refresh="full", fullRefresh=["range_ten"]))
    )
    (build,) = p.tasks
    assert build.kind == "spark-pipeline"
    assert [path for path, _ in build.graph] == ["spark-pipeline.yml", "sql/range_ten.sql"]
    assert dict(build.graph)["spark-pipeline.yml"] == GRAPH
    assert build.refresh == "full"
    assert build.full_refresh == ("range_ten",)
    assert build.writes == ("playground.scratch.range_ten",)


def test_plan_refuses_a_libraries_entry_without_a_glob():
    graph = (
        "name: graph\n"
        "libraries: [sql/range_ten.sql]\n"
        "configuration:\n  spark.sql.catalog.playground.type: rest\n"
    )
    with pytest.raises(PlanError, match="libraries entries need glob.include"):
        plan(
            *owned(
                spark(
                    files={"spark-pipeline.yml": graph, "sql/range_ten.sql": "SELECT 1"}
                )
            )
        )


def test_a_spark_pipeline_graph_without_writes_is_refused():
    with pytest.raises(PlanError) as error:
        plan(*owned(spark(writes=())))
    assert str(error.value) == (
        "spark-pipeline task `build` needs writes: the catalog and schema the graph writes, as "
        "catalog.schema.table — stardelt sets the graph's catalog and database from it"
    )


def test_a_spark_pipeline_graph_with_two_targets_is_refused():
    with pytest.raises(PlanError) as error:
        plan(*owned(spark(writes=("playground.scratch.a", "playground.other.b"))))
    assert str(error.value) == (
        "spark-pipeline task `build` writes into more than one catalog/schema "
        "(playground.other, playground.scratch): a graph has one target; split the task"
    )


def test_a_spark_pipeline_graph_must_not_set_the_owned_keys():
    files = {
        "spark-pipeline.yml": GRAPH + "catalog: playground\n",
        "sql/range_ten.sql": "SELECT 1",
    }
    with pytest.raises(PlanError) as error:
        plan(*owned(spark(files=files)))
    assert str(error.value) == (
        "spark-pipeline task `build`: spark-pipeline.yml must not set catalog, database or "
        "storage — the task's writes and the group's bucket set them at run time (#80)"
    )


def test_a_spark_pipeline_graph_needs_the_catalog_block():
    files = {
        "spark-pipeline.yml": "name: graph\nconfiguration:\n  spark.remote: sc://x\n",
        "sql/range_ten.sql": "SELECT 1",
    }
    with pytest.raises(PlanError) as error:
        plan(*owned(spark(files=files)))
    assert str(error.value) == (
        "spark-pipeline task `build`: catalog `playground` has no "
        "`spark.sql.catalog.playground.type` under `configuration:` — the Connect session would "
        "not know the catalog"
    )


def test_publish_wap_accepts_a_spark_pipeline_task():
    refuse_unrunnable(plan(*owned(spark(), publish="wap")))
    # The fixture cannot give a spark-pipeline task no writes (the plan refuses that itself), so
    # the "no writer" refusal is shown on a sql task: it still refuses.
    with pytest.raises(PlanError) as error:
        plan(*owned(sql("a"), publish="wap"))
    assert str(error.value) == "publish: wap needs a task that writes at least one table"
