"""The pure parts of a `spark-pipeline` task: the graph's key map, its pod's environment and runner.

The pod's environment needs the Kubernetes client models (the loader image and the dev cluster have
them; the plain pytest job may not), so the tests that need them skip when `kubernetes` is absent.
"""
import json

import pytest

pytest.importorskip("kubernetes")

from kubernetes.client import models as k8s  # noqa: E402
from stardelt_pipelines import spark_pipeline  # noqa: E402
from stardelt_pipelines.spark_pipeline import (  # noqa: E402
    SPEC_PATH,
    checkpoint_uri,
    config_map_name,
    connect_remote,
    key_paths,
    pod_env,
    steps,
)


def test_key_paths_are_the_operators_path_sorted_flat_keys():
    paths = key_paths(
        {"range_ten.sql": "", "spark-pipeline.yml": "", "notes.md": ""}
    )
    assert paths == (("f00", "notes.md"), ("f01", "range_ten.sql"), ("f02", "spark-pipeline.yml"))


def test_config_map_name_is_the_operators():
    assert config_map_name("shop", "build") == "spark-project-shop-build"


def test_checkpoint_uri_lives_under_the_groups_bucket():
    assert (
        checkpoint_uri("lakehouse", "demo", "shop", "build")
        == "s3a://lakehouse/pipelines/demo/shop/build/checkpoints"
    )


def test_the_pod_speaks_to_the_owner_groups_cluster_by_default(monkeypatch):
    # #493 point 6: the shared server is no longer the default; the group's SparkCluster is.
    monkeypatch.delenv(spark_pipeline.REMOTE_ENV, raising=False)
    variables = env(monkeypatch, remote=connect_remote("demo"))
    assert (
        variables["SPARK_REMOTE"].value
        == "sc://spc-demo-server.stardelt.svc.cluster.local:15002"
    )


def test_pipeline_spark_remote_still_overrides_the_group_cluster(monkeypatch):
    monkeypatch.setenv(spark_pipeline.REMOTE_ENV, "sc://elsewhere:15002")
    variables = env(monkeypatch, remote=connect_remote("demo"))
    assert variables["SPARK_REMOTE"].value == "sc://elsewhere:15002"


def test_the_runner_is_valid_python():
    compile(spark_pipeline.RUNNER, "<runner>", "exec")


def values(**overrides):
    base = dict(
        paths=(("f00", "spark-pipeline.yml"),),
        remote="sc://spark-connect:15002",
        catalog="playground",
        database="scratch",
        storage="s3a://lakehouse/pipelines/demo/shop/build/checkpoints",
        steps=steps(SPEC_PATH, None, ()),
        wap_branch=None,
    )
    base.update(overrides)
    return base


def env(monkeypatch, **overrides):
    return {var.name: var for var in pod_env(**values(**overrides))}


def test_the_dry_run_runs_before_the_graph():
    assert steps(SPEC_PATH, None, ()) == (
        ("dry-run", "--spec", SPEC_PATH),
        ("run", "--spec", SPEC_PATH),
    )


def test_a_full_refresh_carries_all():
    assert steps(SPEC_PATH, "full", ())[1] == (
        "run",
        "--spec",
        SPEC_PATH,
        "--full-refresh-all",
    )


def test_full_refresh_datasets_are_comma_separated():
    assert steps(SPEC_PATH, None, ("orders", "customers"))[1][-2:] == (
        "--full-refresh",
        "orders,customers",
    )


def test_the_runner_runs_the_steps_it_is_given():
    # The dry run is the first entry of SPARK_PROJECT_STEPS (`steps()`), which the pod reads from
    # its environment; the runner holds no `dry-run` literal, so the order is pinned by
    # test_the_dry_run_runs_before_the_graph and this pins that the runner loops over the steps.
    assert "SPARK_PROJECT_STEPS" in spark_pipeline.RUNNER
    assert 'spark-pipelines", *step' in spark_pipeline.RUNNER


def test_the_pod_carries_no_client_credentials(monkeypatch):
    variables = env(monkeypatch)
    for name in ("PIPELINE_TOKEN_URL", "PIPELINE_CLIENT_ID", "PIPELINE_CLIENT_SECRET"):
        assert name not in variables, name


def test_the_runner_reads_the_projected_catalog_token():
    assert "TOKEN_DIR" in spark_pipeline.RUNNER
    assert "/var/run/secrets/stardelt-catalog/token" in spark_pipeline.RUNNER
    assert "PIPELINE_CLIENT_SECRET" not in spark_pipeline.RUNNER


def test_the_graph_map_is_the_json_pair_list(monkeypatch):
    variables = env(monkeypatch, paths=(("f00", "notes.md"), ("f01", "spark-pipeline.yml")))
    assert json.loads(variables["SPARK_PROJECT_PATHS"].value) == [
        ["f00", "notes.md"],
        ["f01", "spark-pipeline.yml"],
    ]


def test_the_runner_runs_on_the_images_interpreter():
    # `platform/images/spark/Dockerfile` installs PyYAML into /opt/python313 (PYSPARK_PYTHON);
    # the Ubuntu `python3` on PATH has no yaml, so the runner must not ask for it (#969).
    assert spark_pipeline.PYTHON == "/opt/python313/bin/python3"


def test_pod_env_carries_the_wap_branch_only_when_asked(monkeypatch):
    assert "SPARK_WAP_BRANCH" not in env(monkeypatch, wap_branch=None)

    variables = env(monkeypatch, wap_branch="audit_ab")
    branches = [name for name in variables if name == "SPARK_WAP_BRANCH"]
    assert len(branches) == 1
    assert variables["SPARK_WAP_BRANCH"].value == "audit_ab"


def test_the_runner_sets_spark_wap_branch_from_the_env():
    # The runner cannot run in the unit environment; `test_the_runner_is_valid_python` keeps it
    # compiling, and this pins the key #991 Q1 measured as the only working place for it.
    assert 'configuration["spark.wap.branch"] = branch' in spark_pipeline.RUNNER
