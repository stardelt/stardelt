"""The public-db bundle (examples/public-db): its notebook is a real nbformat document and its pipeline pins the connection."""
import json
from pathlib import Path

import yaml

PUBLIC_DB = Path(__file__).resolve().parents[1] / "examples" / "public-db"


def test_the_bundle_is_one_pipeline_with_a_notebook_and_the_connection():
    manifest = yaml.safe_load((PUBLIC_DB / "bundle.yaml").read_text())
    assert manifest["bundle"]["ownerGroup"] == "demo"
    assert manifest["requires"]["connections"] == ["rnacentral"]
    assert manifest["targets"]["prod"]["runAs"] == "pipeline-demo"
    pipeline = yaml.safe_load((PUBLIC_DB / "pipelines" / "public-db-load.yaml").read_text())
    assert pipeline["notebook"]["connection"] == "rnacentral"
    assert pipeline["notebook"]["file"] == "notebooks/load_rnacentral.ipynb"


def test_the_pipeline_is_readable_by_visitors():
    pipeline = yaml.safe_load((PUBLIC_DB / "pipelines" / "public-db-load.yaml").read_text())
    assert pipeline["viewers"] == ["visitors"]


def test_the_notebook_is_nbformat_4_and_reads_the_connection_from_the_environment():
    notebook = json.loads((PUBLIC_DB / "notebooks" / "load_rnacentral.ipynb").read_text())
    assert notebook["nbformat"] == 4
    code = "".join("".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code")
    assert 'os.environ["CONN_RNACENTRAL_USER"]' in code
    assert 'os.environ["CONN_RNACENTRAL_PASSWORD"]' in code
    assert "NWDMCE5xdipIjRrp" not in code, "the password belongs to the Connection's Secret, never to a file"


def test_notebook_creates_its_schema_first():
    notebook = json.loads((PUBLIC_DB / "notebooks" / "load_rnacentral.ipynb").read_text())
    cells = ["".join(cell["source"]) for cell in notebook["cells"]]
    schema_cell = next(i for i, text in enumerate(cells) if "create schema if not exists bronze.public_db" in text.lower())
    first_write = next(i for i, text in enumerate(cells) if "write_df(" in text)
    assert schema_cell < first_write
