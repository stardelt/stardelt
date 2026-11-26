"""The PEGELONLINE demo pipeline publishes the versioned view after the table it reads."""

import pathlib

import yaml

EXAMPLE = pathlib.Path(__file__).resolve().parent.parent / "examples" / "pegelonline"


def tasks():
    return yaml.safe_load((EXAMPLE / "pipeline.yaml").read_text(encoding="utf-8"))["spec"]["tasks"]


def test_the_view_is_created_and_commented_after_the_gold_table():
    by_name = {task["name"]: task for task in tasks()}
    assert by_name["view"]["dependsOn"] == ["gold-latest"]
    assert by_name["view-comment"]["dependsOn"] == ["view"]
    assert "CREATE OR REPLACE VIEW shared.gold.pegelonline_levels_v1" in by_name["view"]["sql"]
    assert "COMMENT ON VIEW shared.gold.pegelonline_levels_v1" in by_name["view-comment"]["sql"]


def test_every_dependency_names_a_task_of_this_pipeline():
    names = {task["name"] for task in tasks()}
    for task in tasks():
        for dependency in task.get("dependsOn", []):
            assert dependency in names, (task["name"], dependency)
