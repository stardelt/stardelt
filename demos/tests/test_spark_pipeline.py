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
    checkpoint_uri,
    config_map_name,
    key_paths,
    pod_env,
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


def test_the_runner_is_valid_python():
    compile(spark_pipeline.RUNNER, "<runner>", "exec")


def values(**overrides):
    base = dict(
        paths=(("f00", "spark-pipeline.yml"),),
        remote="sc://spark-connect:15002",
        catalog="playground",
        database="scratch",
        storage="s3a://lakehouse/pipelines/demo/shop/build/checkpoints",
        refresh=None,
        full_refresh=(),
        principal_secret="sp-pipeline-demo",
    )
    base.update(overrides)
    return base


def env(monkeypatch, **overrides):
    monkeypatch.setenv("PIPELINE_TOKEN_URL", "https://id.example/token")
    return {var.name: var for var in pod_env(**values(**overrides))}


def test_a_full_refresh_carries_all(monkeypatch):
    assert env(monkeypatch, refresh="full")["SPARK_REFRESH"].value == "all"


def test_full_refresh_datasets_are_comma_separated(monkeypatch):
    assert (
        env(monkeypatch, full_refresh=("a", "b"))["SPARK_REFRESH"].value == "a,b"
    )


def test_the_principal_credentials_are_secret_refs_not_values(monkeypatch):
    variables = env(monkeypatch)
    for name, key in (
        ("PIPELINE_CLIENT_ID", "client-id"),
        ("PIPELINE_CLIENT_SECRET", "client-secret"),
    ):
        variable = variables[name]
        assert variable.value is None
        assert isinstance(variable.value_from, k8s.V1EnvVarSource)
        assert variable.value_from.secret_key_ref.name == "sp-pipeline-demo"
        assert variable.value_from.secret_key_ref.key == key


def test_the_graph_map_is_the_json_pair_list(monkeypatch):
    variables = env(monkeypatch, paths=(("f00", "notes.md"), ("f01", "spark-pipeline.yml")))
    assert json.loads(variables["SPARK_PROJECT_PATHS"].value) == [
        ["f00", "notes.md"],
        ["f01", "spark-pipeline.yml"],
    ]
