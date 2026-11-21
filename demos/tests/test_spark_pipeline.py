"""The pure parts of a `spark-pipeline` task: the ConfigMap key map, the pod's environment and the
runner. Nothing here needs Airflow or a cluster.
"""
import json

import pytest
from kubernetes.client import models as k8s
from stardelt_pipelines import spark_pipeline
from stardelt_pipelines.spark_pipeline import (
    checkpoint_uri,
    config_map_name,
    key_paths,
    pod_env,
)


def test_key_paths_follow_the_operators_path_order():
    paths = key_paths(
        {"range_ten.sql": "", "spark-pipeline.yml": "", "notes.md": ""}
    )
    assert paths == (
        ("f00", "notes.md"),
        ("f01", "range_ten.sql"),
        ("f02", "spark-pipeline.yml"),
    )


def test_config_map_name_is_the_operators():
    assert config_map_name("shop", "build") == "spark-project-shop-build"


def test_checkpoint_uri_lives_under_the_groups_bucket():
    assert (
        checkpoint_uri("lakehouse", "demo", "shop", "build")
        == "s3a://lakehouse/pipelines/demo/shop/build/checkpoints"
    )


def test_the_runner_is_valid_python():
    assert compile(spark_pipeline.RUNNER, "<runner>", "exec")


def test_pod_env_carries_the_paths_and_the_three_owned_keys(monkeypatch):
    monkeypatch.setenv("PIPELINE_TOKEN_URL", "https://id.example/token")
    env = pod_env(
        paths=(("f00", "spark-pipeline.yml"), ("f01", "sql/range_ten.sql")),
        remote="sc://spark-connect.example:15002",
        catalog="playground",
        database="scratch",
        storage="s3a://lakehouse/pipelines/demo/shop/build/checkpoints",
        refresh=None,
        full_refresh=(),
        principal_secret="sp-pipeline-demo",
    )
    values = {item.name: item.value for item in env if item.value is not None}
    assert json.loads(values["SPARK_PROJECT_PATHS"]) == [
        ["f00", "spark-pipeline.yml"],
        ["f01", "sql/range_ten.sql"],
    ]
    assert values["SPARK_CATALOG"] == "playground"
    assert values["SPARK_DATABASE"] == "scratch"
    assert values["SPARK_REMOTE"] == "sc://spark-connect.example:15002"
    assert values["PIPELINE_TOKEN_URL"] == "https://id.example/token"


def test_pod_env_maps_a_full_refresh_to_all(monkeypatch):
    monkeypatch.setenv("PIPELINE_TOKEN_URL", "https://id.example/token")
    env = pod_env(
        paths=(),
        remote="sc://spark-connect.example:15002",
        catalog="playground",
        database="scratch",
        storage="s3a://lakehouse/pipelines/demo/shop/build/checkpoints",
        refresh="full",
        full_refresh=(),
        principal_secret="sp-pipeline-demo",
    )
    values = {item.name: item.value for item in env}
    assert values["SPARK_REFRESH"] == "all"


def test_pod_env_names_the_full_refresh_datasets(monkeypatch):
    monkeypatch.setenv("PIPELINE_TOKEN_URL", "https://id.example/token")
    env = pod_env(
        paths=(),
        remote="sc://spark-connect.example:15002",
        catalog="playground",
        database="scratch",
        storage="s3a://lakehouse/pipelines/demo/shop/build/checkpoints",
        refresh=None,
        full_refresh=("a", "b"),
        principal_secret="sp-pipeline-demo",
    )
    values = {item.name: item.value for item in env}
    assert values["SPARK_REFRESH"] == "a,b"


def test_the_principal_credentials_are_secret_references(monkeypatch):
    monkeypatch.setenv("PIPELINE_TOKEN_URL", "https://id.example/token")
    env = {
        item.name: item
        for item in pod_env(
            paths=(),
            remote="sc://spark-connect.example:15002",
            catalog="playground",
            database="scratch",
            storage="s3a://lakehouse/pipelines/demo/shop/build/checkpoints",
            refresh=None,
            full_refresh=(),
            principal_secret="sp-pipeline-demo",
        )
    }
    for name, key in (
        ("PIPELINE_CLIENT_ID", "client-id"),
        ("PIPELINE_CLIENT_SECRET", "client-secret"),
    ):
        item = env[name]
        assert item.value is None
        source = item.value_from
        assert isinstance(source, k8s.V1EnvVarSource)
        reference = source.secret_key_ref
        assert reference.name == "sp-pipeline-demo"
        assert reference.key == key


def test_pod_env_needs_the_token_url():
    with pytest.raises(ValueError, match="PIPELINE_TOKEN_URL is not set"):
        pod_env(
            paths=(),
            remote="sc://spark-connect.example:15002",
            catalog="playground",
            database="scratch",
            storage="s3a://lakehouse/pipelines/demo/shop/build/checkpoints",
            refresh=None,
            full_refresh=(),
            principal_secret="sp-pipeline-demo",
        )
