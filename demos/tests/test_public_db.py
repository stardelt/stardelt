"""The public-db bundle (examples/public-db): its notebook is a real nbformat document, its schema comes first and its pipeline pins the connection."""
import json
from pathlib import Path

import yaml

PUBLIC_DB = Path(__file__).resolve().parents[1] / "examples" / "public-db"


def _pipeline():
    return yaml.safe_load((PUBLIC_DB / "pipelines" / "public-db-load.yaml").read_text())


def _tasks():
    return {task["name"]: task for task in _pipeline()["tasks"]}


def test_the_bundle_is_one_pipeline_with_a_notebook_and_the_connection():
    manifest = yaml.safe_load((PUBLIC_DB / "bundle.yaml").read_text())
    assert manifest["bundle"]["ownerGroup"] == "demo"
    assert manifest["requires"]["connections"] == ["rnacentral"]
    assert manifest["targets"]["prod"]["runAs"] == "pipeline-demo"
    assert "sql/schema.sql" in manifest["files"]
    run = _tasks()["run"]
    assert run["notebook"]["connection"] == "rnacentral"
    assert run["notebook"]["file"] == "notebooks/load_rnacentral.ipynb"


def test_the_pipeline_creates_its_schema_before_the_notebook_writes():
    """A table can only be created in a schema that exists (#1636): the first task creates it."""
    tasks = _tasks()
    assert list(tasks) == ["schema", "run"], "the schema task runs first"
    statement = (PUBLIC_DB / tasks["schema"]["sqlFile"]).read_text().lower()
    assert "create schema if not exists bronze.public_db" in statement
    assert tasks["run"]["dependsOn"] == ["schema"]


def test_the_pipeline_is_readable_by_visitors():
    assert _pipeline()["viewers"] == ["visitors"]


def test_the_notebook_is_nbformat_4_and_reads_the_connection_from_the_environment():
    notebook = json.loads((PUBLIC_DB / "notebooks" / "load_rnacentral.ipynb").read_text())
    assert notebook["nbformat"] == 4
    code = "".join("".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code")
    assert 'os.environ["CONN_RNACENTRAL_USER"]' in code
    assert 'os.environ["CONN_RNACENTRAL_PASSWORD"]' in code
    assert "NWDMCE5xdipIjRrp" not in code, "the password belongs to the Connection's Secret, never to a file"
