"""Tests for `stardelt setup vscode` (stardelt.vscode_setup). They write into a temporary data directory."""

import json
import sys
import tomllib
from pathlib import Path

import pytest

from stardelt import login, vscode_setup

NOVA = "https://alpha.dev.stardelt.io"
KERNEL = Path("kernels") / "stardelt-alpha-dev-stardelt-io" / "kernel.json"


def env(tmp_path, **extra):
    return {"JUPYTER_DATA_DIR": str(tmp_path), **extra}


def test_it_writes_a_kernelspec_that_starts_the_stardelt_kernel(tmp_path):
    path = vscode_setup.install(env(tmp_path), host=NOVA)
    assert path == tmp_path / KERNEL and path.exists()
    spec = json.loads(path.read_text())
    assert spec["argv"][0] == sys.executable
    assert spec["argv"][1:4] == ["-m", "stardelt.kernel", "-f"]
    assert spec["argv"][4] == "{connection_file}"
    assert spec["display_name"] == "stardelt — alpha.dev.stardelt.io"
    assert spec["env"]["SPARK_REMOTE"] == "sc://spark.alpha.dev.stardelt.io:443/;use_ssl=true;token=__TOKEN__"


def test_it_takes_the_host_from_the_stored_sign_in(tmp_path):
    environ = env(tmp_path, HOME=str(tmp_path / "home"))
    login.save(
        environ,
        {"access_token": "stored-token", "issuer": "https://id.example", "host": "https://nova.example"},
    )
    path = vscode_setup.install(environ)
    assert path == tmp_path / "kernels" / "stardelt-nova-example" / "kernel.json"
    spec = json.loads(path.read_text())
    assert spec["display_name"] == "stardelt — nova.example"
    assert spec["env"]["STARDELT_NOVA_URL"] == "https://nova.example"
    assert spec["env"]["SPARK_REMOTE"] == "sc://spark.example:443/;use_ssl=true;token=__TOKEN__"


def test_an_unknown_host_is_a_value_error_naming_both_ways(tmp_path):
    with pytest.raises(ValueError) as caught:
        vscode_setup.install(env(tmp_path))
    assert "--host https://<nova> login" in str(caught.value)


def test_the_spark_host_can_be_overridden(tmp_path):
    path = vscode_setup.install(env(tmp_path), host=NOVA, spark_host="spark.elsewhere.example")
    spec = json.loads(path.read_text())
    assert spec["env"]["SPARK_REMOTE"] == "sc://spark.elsewhere.example:443/;use_ssl=true;token=__TOKEN__"


def test_the_extra_declares_ipykernel():
    project = Path(__file__).resolve().parents[1] / "pyproject.toml"
    data = tomllib.loads(project.read_text())
    assert "ipykernel" in data["project"]["optional-dependencies"]["jupyter"]
