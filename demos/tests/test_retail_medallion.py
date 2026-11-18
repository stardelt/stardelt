"""The retail-medallion demo (examples/retail-medallion) names the notebooks it ships and only native-helper notebooks.

No cluster and no stardelt package: the Pipeline and the two notebooks are read and checked.
"""

import json
import pathlib
import re

import yaml

EXAMPLES = pathlib.Path(__file__).resolve().parent.parent / "examples"
DEMO = EXAMPLES / "retail-medallion"
PIPELINE = DEMO / "pipeline.yaml"

VERSION = re.compile(r"^[0-9a-f]{32}$")


def pipeline():
    return yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))


def notebooks():
    return [DEMO / "retail_spark_ingest.ipynb", DEMO / "retail_gold.ipynb"]


def test_pipeline_names_the_committed_notebooks():
    tasks = pipeline()["spec"]["tasks"]
    assert sorted(task["name"] for task in tasks) == ["gold", "ingest"]
    for task in tasks:
        if task["kind"] != "notebook":
            continue
        assert (DEMO / task["config"]["notebook"]).is_file()
        assert VERSION.match(task["config"]["objectVersion"])


def test_the_demo_pipeline_is_not_scheduled():
    assert "schedule" not in pipeline()["spec"]


def test_notebooks_are_nbformat_4_and_use_the_native_helper():
    for path in notebooks():
        notebook = json.loads(path.read_text(encoding="utf-8"))
        assert notebook["nbformat"] == 4
        source = "\n".join(
            "".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"
        )
        assert "stardelt.spark()" in source
        assert "import stardelt" in source
