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

    def test_last_run_comes_from_nova_not_the_crd(self):
        """When: the CRD's `status.runs` is empty and Nova's row says success. Then: the
        Nova run is the `last run` cell, not `never`."""
        obj = pipeline("shop-dbt", schedule={"cron": "0 4 * * *", "timezone": "Europe/Berlin"})
        text = demo_inventory.render(
            [obj],
            [],
            [],
            {"shop-dbt": {"state": "success", "startedAt": "2026-11-19T20:18:00Z"}},
        )
        self.assertIn(
            "| shop-dbt | demo | 0 4 * * * Europe/Berlin | — | success 2026-11-19T20:18:00Z | sql |",
            text,
        )


class CheckTest(unittest.TestCase):
    def test_check_names_a_legacy_pipeline(self):
        """When: `shop-01-bronze` survived the bundle deploy. Then: check names it and the fix."""
        problems = demo_inventory.check(
            [pipeline("shop-01-bronze", schedule=None)], [], [], {}
        )
        self.assertIn(
            "the legacy pipeline shop-01-bronze is still there — run make demo-shop YES=1",
            problems,
        )

    def test_check_names_a_demo_object_without_visitors(self):
        """When: a demo-owned pipeline, stream and cluster lack `visitors`. Then: one message
        each, with the kind word and the command that converges it."""
        problems = demo_inventory.check(
            [pipeline("shop-1-bronze", schedule={"cron": "0 2 * * *"}, viewers=["analysts"])],
            [stream("demo-pegelonline", viewers=[])],
            [cluster("demo-lab", viewers=["analysts"])],
            {"shop-1-bronze": {"state": "success", "startedAt": "2026-11-19T20:18:00Z"}},
        )
        self.assertIn(
            "pipeline shop-1-bronze does not name visitors in its viewers — run make demo-shop YES=1",
            problems,
        )
        self.assertIn(
            "stream demo-pegelonline does not name visitors in its viewers — run make demo-stream YES=1",
            problems,
        )
        self.assertIn(
            "cluster demo-lab does not name visitors in its viewers — run make demo-cluster YES=1",
            problems,
        )

    def test_check_names_an_on_demand_demo_pipeline(self):
        """When: a demo-owned pipeline carries no schedule. Then: check names it."""
        problems = demo_inventory.check(
            [pipeline("shop-ontap", schedule=None, viewers=["visitors"])], [], [], {}
        )
        self.assertIn(
            "pipeline shop-ontap is on demand; a visitor cannot see a next run", problems
        )

    def test_check_names_a_pipeline_that_never_ran(self):
        """When: a scheduled demo pipeline has no last run in Nova. Then: check names `never`."""
        problems = demo_inventory.check(
            [
                pipeline(
                    "shop-dbt",
                    schedule={"cron": "0 4 * * *"},
                    viewers=["visitors"],
                )
            ],
            [],
            [],
            {"shop-dbt": None},
        )
        self.assertIn("pipeline shop-dbt's last run is never", problems)

    def test_check_names_a_failed_last_run(self):
        """When: Nova's last run of a scheduled demo pipeline failed. Then: check names it."""
        problems = demo_inventory.check(
            [pipeline("shop-dbt", schedule={"cron": "0 4 * * *"}, viewers=["visitors"])],
            [],
            [],
            {"shop-dbt": {"state": "failed", "startedAt": "2026-11-19T20:18:00Z"}},
        )
        self.assertIn("pipeline shop-dbt's last run is failed", problems)

    def test_check_names_a_stream_without_visitors(self):
        """When: the demo's stream names no viewers. Then: check names it with `demo-stream`."""
        problems = demo_inventory.check([], [stream("demo-pegelonline", viewers=None)], [], {})
        self.assertIn(
            "stream demo-pegelonline does not name visitors in its viewers"
            " — run make demo-stream YES=1",
            problems,
        )

    def test_check_ignores_another_group(self):
        """When: another group owns the objects. Then: check is silent — it asserts only demo."""
        problems = demo_inventory.check(
            [pipeline("retail-medallion", owner_group="retail-analytics", schedule=None)],
            [stream("mobility", owner_group="mobility-analytics")],
            [cluster("other", group="retail-analytics")],
            {},
        )
        self.assertEqual(problems, [])

    def test_check_accepts_a_green_converged_state(self):
        """When: the bundle's three scheduled, visible pipelines ran green in Nova. Then: []."""
        schedules = {
            "shop-1-bronze": {"cron": "0 2 * * *", "timezone": "Europe/Berlin"},
            "shop-2-silver": {"cron": "30 2 * * *", "timezone": "Europe/Berlin"},
            "shop-3-gold": {"cron": "0 3 * * *", "timezone": "Europe/Berlin"},
        }
        pipelines = [
            pipeline(name, schedule=schedule, viewers=["visitors"])
            for name, schedule in schedules.items()
        ]
        runs = {
            name: {"state": "success", "startedAt": "2026-11-19T20:18:00Z"}
            for name in schedules
        }
        self.assertEqual(demo_inventory.check(pipelines, [], [], runs), [])


if __name__ == "__main__":
    unittest.main()
