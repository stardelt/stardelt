"""A `dbt` task: its config in the plan, the batches the plan mints, and its pod's building blocks.

`dbt_arguments` and `pod_env` import no Airflow, so the whole file runs in the plain pytest job.
The operator itself is only built by `pipeline_loader`, which needs the Kubernetes provider.
"""
import pytest
from stardelt_pipelines.dbt import (
    PROJECTS_DIR,
    TRINO_CA_FILE,
    dbt_arguments,
    pod_env,
    project_dir,
)
from stardelt_pipelines.plan import PlanError, plan


def resource(**spec):
    return {"metadata": {"name": "shop-dbt"}, "spec": spec}


def principal():
    """`pipeline-demo`, the demo group's `ServicePrincipal`, as the API server returns it."""
    return {
        "metadata": {"name": "pipeline-demo"},
        "spec": {"ownerGroup": "demo"},
        "status": {"ready": True, "secretName": "sp-pipeline-demo"},
    }


def dbt(name="build", *, deps=None, **config):
    task = {"name": name, "kind": "dbt", "config": {"project": "shop", **config}}
    if deps is not None:
        task["dependsOn"] = list(deps)
    return task


def sql(name, *deps):
    task = {"name": name, "kind": "sql", "sql": "SELECT 1"}
    if deps:
        task["dependsOn"] = list(deps)
    return task


def planned(*tasks, **spec):
    """The plan of a `demo` pipeline that runs as `pipeline-demo`."""
    return plan(
        resource(tasks=list(tasks), ownerGroup="demo", runAs="pipeline-demo", **spec),
        "stardelt",
        {"pipeline-demo": principal()},
    )


def test_a_dbt_task_plans_as_one_task_with_its_selector():
    p = planned(dbt(select="tag:load", threads="6", vars='{"scale": "200000"}'))
    assert [t.name for t in p.tasks] == ["build"]
    task = p.tasks[0]
    assert (task.kind, task.dbt_project) == ("dbt", "shop")
    assert task.dbt_select == ("tag:load",)
    assert (task.dbt_threads, task.dbt_vars) == ("6", '{"scale": "200000"}')
    # The defaults of the config: four threads, no vars.
    other = planned(dbt(select="tag:load")).tasks[0]
    assert (other.dbt_threads, other.dbt_vars) == ("4", None)


def test_a_dbt_task_needs_a_principal():
    with pytest.raises(PlanError, match="ownerGroup and runAs"):
        plan(resource(tasks=[dbt(select="tag:load")]), "stardelt")
    with pytest.raises(PlanError, match="ownerGroup and runAs"):
        # `ownerGroup` without `runAs` is not enough: the pod mints a token from the principal.
        plan(
            resource(tasks=[dbt(select="tag:load")], ownerGroup="demo"),
            "stardelt",
            {"pipeline-demo": principal()},
        )


def test_an_unknown_config_key_is_refused():
    with pytest.raises(PlanError, match="found `sql`"):
        planned({"name": "build", "kind": "dbt", "config": {"sql": "select 1"}})
    with pytest.raises(PlanError, match="found `nope`"):
        planned(dbt(select="tag:load", nope="x"))


def test_batches_expand_into_a_chain_and_dependents_follow_the_last():
    p = planned(
        sql("load"),
        dbt(deps=["load"], batches='["tag:load","tag:staging","tag:marts"]'),
        sql("next", "build"),
    )
    by_name = {t.name: t for t in p.tasks}
    assert [t.name for t in p.tasks] == ["load", "build-1", "build-2", "build-3", "next"]
    # One selector per batch, in order, the project and the threads on each.
    assert [by_name[f"build-{i}"].dbt_select for i in (1, 2, 3)] == [
        ("tag:load",),
        ("tag:staging",),
        ("tag:marts",),
    ]
    assert {by_name[f"build-{i}"].dbt_project for i in (1, 2, 3)} == {"shop"}
    # A chain: the first batch carries what the task declared, each later one its predecessor.
    assert by_name["build-1"].depends_on == ("load",)
    assert by_name["build-2"].depends_on == ("build-1",)
    assert by_name["build-3"].depends_on == ("build-2",)
    # A task that depended on the batched name waits for the last batch.
    assert by_name["next"].depends_on == ("build-3",)


def test_project_names_and_threads_are_bounded():
    for project in ("Shop", "../shop", "shop/x", "shop_x", ""):
        with pytest.raises(PlanError, match="config.project"):
            planned(dbt(select="tag:load", project=project))
    for threads in ("0", "17", "x", "-1", " 4"):
        with pytest.raises(PlanError, match="threads"):
            planned(dbt(select="tag:load", threads=threads))
    # The bounds themselves are accepted.
    assert planned(dbt(select="tag:load", threads="16")).tasks[0].dbt_threads == "16"
    assert planned(dbt(select="tag:load", threads="1")).tasks[0].dbt_threads == "1"


def test_the_arguments_name_the_project_and_the_selector():
    assert dbt_arguments(project="shop", select="tag:load", threads="4", vars_text=None) == [
        "build",
        "--project-dir",
        f"{PROJECTS_DIR}/shop",
        "--profiles-dir",
        f"{PROJECTS_DIR}/shop",
        "--select",
        "tag:load",
        "--threads",
        "4",
    ]
    assert dbt_arguments(
        project="shop", select="tag:marts", threads="6", vars_text='{"scale": "200000"}'
    )[-2:] == ["--vars", '{"scale": "200000"}']
    # A name that is not one directory is refused, never normalized.
    with pytest.raises(ValueError, match="project name"):
        project_dir("../shop")


def test_the_pod_environment_has_no_literal_credential(monkeypatch):
    canary = "canary-client-secret-value"
    monkeypatch.setenv("PIPELINE_TRINO_URL", "https://trino:8443")
    monkeypatch.setenv("PIPELINE_TOKEN_URL", "https://id.example/realms/stardelt/token")
    # A literal value in the loader's environment must never reach the pod's `value`.
    monkeypatch.setenv("PIPELINE_CLIENT_SECRET", canary)

    env = pod_env(principal_secret="sp-pipeline-demo")
    by_name = {item.name: item for item in env}

    assert by_name["PIPELINE_TRINO_URL"].value == "https://trino:8443"
    assert by_name["PIPELINE_TOKEN_URL"].value == "https://id.example/realms/stardelt/token"
    assert by_name["PIPELINE_TRINO_CA_FILE"].value == TRINO_CA_FILE
    for name, key in (
        ("PIPELINE_CLIENT_ID", "client-id"),
        ("PIPELINE_CLIENT_SECRET", "client-secret"),
    ):
        assert by_name[name].value is None
        assert by_name[name].value_from.secret_key_ref.name == "sp-pipeline-demo"
        assert by_name[name].value_from.secret_key_ref.key == key
    assert canary not in [item.value for item in env if item.value]


def test_a_missing_engine_url_fails_with_its_name(monkeypatch):
    monkeypatch.delenv("PIPELINE_TRINO_URL", raising=False)
    monkeypatch.setenv("PIPELINE_TOKEN_URL", "https://id.example/realms/stardelt/token")
    with pytest.raises(ValueError, match="^PIPELINE_TRINO_URL is not set$"):
        pod_env(principal_secret="sp-pipeline-demo")
