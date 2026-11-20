"""Unit tests for scripts/demo_inventory.py (stardelt#79 point 2, slice 1).

Standard library only, like the script: `python3 -m unittest discover -s scripts/tests`
runs them in CI. `render()` is pure, so every test drives canned `kubectl get ... -o json`
`items` and no cluster is touched.
"""
import importlib.util
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "demo_inventory.py"
_spec = importlib.util.spec_from_file_location("demo_inventory", SCRIPT)
demo_inventory = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(demo_inventory)

HEADER = "| pipeline | owner | schedule | viewers | last run | tasks |"


def pipeline(name, owner_group="demo", schedule=None, viewers=None, runs=None, tasks=None):
    spec = {
        "tasks": tasks if tasks is not None else [{"name": "load", "kind": "sql"}],
    }
    if owner_group is not None:
        spec["ownerGroup"] = owner_group
    if schedule is not None:
        spec["schedule"] = schedule
    if viewers is not None:
        spec["viewers"] = viewers
    return {
        "metadata": {"name": name, "namespace": "stardelt"},
        "spec": spec,
        "status": {"runs": runs if runs is not None else []},
    }


def stream(name, owner_group="demo", viewers=None, phase="Running", checkpoint="2026-11-19T10:00:00Z"):
    spec = {"ownerGroup": owner_group} if owner_group is not None else {}
    if viewers is not None:
        spec["viewers"] = viewers
    status = {"phase": phase}
    if checkpoint is not None:
        status["lastCheckpointAt"] = checkpoint
    return {"metadata": {"name": name}, "spec": spec, "status": status}


def cluster(name, group="demo", user=None, viewers=None, phase="Running"):
    owner = {}
    if group is not None:
        owner["group"] = group
    if user is not None:
        owner["user"] = user
    spec = {"owner": owner, "runtime": "spark-4.0-py3.12"}
    if viewers is not None:
        spec["viewers"] = viewers
    return {"metadata": {"name": name}, "spec": spec, "status": {"phase": phase}}


class RenderTest(unittest.TestCase):
    def test_pipeline_rows_schedule_never_and_order(self):
        """When: a scheduled pipeline with one run and an on-demand one. Then: one row each,
        the run in `last run`, `never` for the empty list, sorted by name."""
        scheduled = pipeline(
            "shop-nightly",
            schedule={"cron": "0 3 * * *", "timezone": "Europe/Berlin"},
            viewers=["analysts"],
            runs=[{"id": "r1", "state": "succeeded", "startedAt": "2026-11-19T02:00:00Z"}],
            tasks=[{"name": "seed", "kind": "dbt"}, {"name": "load", "kind": "sql"}],
        )
        on_demand = pipeline("shop-ontap", schedule=None)
        # Input out of order: the table sorts it.
        text = demo_inventory.render([scheduled, on_demand], [], [])

        self.assertEqual(text.splitlines()[0], "## Pipelines")
        self.assertEqual(text.splitlines()[1], HEADER)
        rows = [line for line in text.splitlines() if line.startswith("| shop-")]
        self.assertEqual(
            rows,
            [
                "| shop-nightly | demo | 0 3 * * * Europe/Berlin | analysts | "
                "succeeded 2026-11-19T02:00:00Z | dbt, sql |",
                "| shop-ontap | demo | on demand | — | never | sql |",
            ],
        )
        self.assertTrue(text.splitlines()[-1].startswith("Total: 2 pipelines, 0 streams, 0 clusters."))

    def test_stream_and_cluster_rows(self):
        """When: one stream with a checkpoint and one cluster owned by a group. Then: the
        checkpoint and the owner group land in their cells and the counts are right."""
        text = demo_inventory.render(
            [],
            [stream("orders", viewers=["analysts"], phase="Running")],
            [cluster("lab", viewers=["analysts"], phase="Stopped")],
        )
        self.assertIn("| stream | owner | viewers | phase | last checkpoint |", text)
        self.assertIn("| orders | demo | analysts | Running | 2026-11-19T10:00:00Z |", text)
        self.assertIn("| cluster | owner | viewers | phase |", text)
        self.assertIn("| lab | demo | analysts | Stopped |", text)
        self.assertTrue(text.endswith("Total: 0 pipelines, 1 streams, 1 clusters."))

    def test_missing_owner_and_checkpoint_render_a_dash(self):
        """When: no `ownerGroup`, no cluster owner group (a user instead), no checkpoint.
        Then: every empty cell is `—`, never `None`."""
        text = demo_inventory.render(
            [pipeline("anon", owner_group=None)],
            [stream("silent", owner_group=None, checkpoint=None)],
            [cluster("solo", group=None, user="maddin")],
        )
        self.assertIn("| anon | — | on demand | — | never | sql |", text)
        self.assertIn("| silent | — | — | Running | — |", text)
        self.assertIn("| solo | maddin | — | Running |", text)
        self.assertNotIn("None", text)


if __name__ == "__main__":
    unittest.main()
