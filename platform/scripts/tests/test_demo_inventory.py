"""Unit tests for scripts/demo_inventory.py (stardelt#79 point 2, slice 1).

Standard library only, like the script: `python3 -m unittest discover -s scripts/tests`
runs them in CI. `render()` is pure, so every test drives canned `kubectl get ... -o json`
`items` and no cluster is touched.
"""
import importlib.util
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "demo_inventory.py"
_spec = importlib.util.spec_from_file_location("demo_inventory", SCRIPT)
demo_inventory = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(demo_inventory)

HEADER = "| pipeline | owner | schedule | viewers | last run | tasks |"


def pipeline(
    name,
    owner_group="demo",
    schedule=None,
    viewers=None,
    runs=None,
    tasks=None,
    description="Loads the demo's seed data.",
    labels=None,
):
    spec = {
        "tasks": tasks if tasks is not None else [{"name": "load", "kind": "sql"}],
    }
    if owner_group is not None:
        spec["ownerGroup"] = owner_group
    if schedule is not None:
        spec["schedule"] = schedule
    if viewers is not None:
        spec["viewers"] = viewers
    if description is not None:
        spec["description"] = description
    return {
        "metadata": {"name": name, "namespace": "stardelt", "labels": labels or {}},
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


def connection(name, viewers=None, scope="managed"):
    spec = {"scope": scope, "type": "postgresql", "usableBy": ["demo"]}
    if viewers is not None:
        spec["viewers"] = viewers
    return {"metadata": {"name": name}, "spec": spec}


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


class RunsFromPayloadTest(unittest.TestCase):
    def test_runs_only_from_the_named_namespace(self):
        """When: Nova lists a run pipeline, one with `lastRun: null` and one in another
        namespace. Then: the dictionary has the two named rows (the second mapped to `None`)
        and nothing from the other namespace."""
        payload = {
            "pipelines": [
                {"name": "shop-dbt", "namespace": "stardelt", "lastRun": {"state": "success"}},
                {"name": "shop-1-bronze", "namespace": "stardelt", "lastRun": None},
                {"name": "retail-medallion", "namespace": "retail-analytics", "lastRun": {"state": "success"}},
            ]
        }
        self.assertEqual(
            demo_inventory.runs_from_payload(payload, "stardelt"),
            {"shop-dbt": {"state": "success"}, "shop-1-bronze": None},
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

    def test_check_names_a_leftover_check_pipeline(self):
        problems = demo_inventory.check(
            [pipeline("verify-wap", labels={"stardelt.io/check": "verify-wap"})], [], [], {}
        )
        assert problems == [
            "the release check's pipeline verify-wap is still on the demo (age unknown) — "
            "kubectl delete pipeline verify-wap -n stardelt"
        ]

    def test_a_live_check_pipeline_is_not_a_leftover(self):
        """When: a check-labelled pipeline was created five minutes ago and is still running.
        Then: check stays silent — the delete command would break the run (#1336)."""
        now = datetime.now(timezone.utc)
        obj = pipeline("verify-wap", labels={"stardelt.io/check": "verify-wap"})
        obj["metadata"]["creationTimestamp"] = (now - timedelta(minutes=5)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        self.assertEqual(demo_inventory.check([obj], [], [], {}, now=now), [])

    def test_an_old_check_pipeline_is_still_a_leftover(self):
        """When: a check-labelled pipeline was created two hours ago — its run is dead. Then:
        check names it, with its age, and the delete command."""
        now = datetime.now(timezone.utc)
        obj = pipeline("verify-wap", labels={"stardelt.io/check": "verify-wap"})
        obj["metadata"]["creationTimestamp"] = (now - timedelta(hours=2)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        self.assertEqual(
            demo_inventory.check([obj], [], [], {}, now=now),
            [
                "the release check's pipeline verify-wap is still on the demo (120 min old) — "
                "kubectl delete pipeline verify-wap -n stardelt"
            ],
        )

    def test_the_total_excludes_check_pipelines(self):
        """When: one demo pipeline and one check-labelled pipeline share the namespace. Then:
        the total counts only the demo one, though both keep their row."""
        text = demo_inventory.render(
            [
                pipeline("shop-1-bronze"),
                pipeline("verify-wap", labels={"stardelt.io/check": "verify-wap"}),
            ],
            [],
            [],
        )
        self.assertIn("| verify-wap |", text)
        self.assertTrue(text.endswith("Total: 1 pipelines, 0 streams, 0 clusters."))

    def test_check_does_not_count_a_check_pipeline_as_demo_content(self):
        # No viewers, no description and no schedule: fine for a check's object, never reported.
        problems = demo_inventory.check(
            [pipeline("verify-wap", viewers=None, description=None, labels={"stardelt.io/check": "verify-wap"})],
            [],
            [],
            {},
        )
        assert [p for p in problems if "viewers" in p or "description" in p or "on demand" in p] == []

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

    def test_check_names_a_demo_pipeline_without_a_description(self):
        problems = demo_inventory.check(
            [
                pipeline(
                    "shop-dbt",
                    schedule={"cron": "0 4 * * *", "timezone": "Europe/Berlin"},
                    viewers=["visitors"],
                    description=None,
                )
            ],
            [],
            [],
            {"shop-dbt": {"state": "success"}},
        )
        self.assertEqual(
            problems,
            ["pipeline shop-dbt has no one-line description — run make demo-shop YES=1"],
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

    def test_check_names_an_empty_streams_page(self):
        """When: no stream names `visitors` but a connection does. Then: check names the
        streams page and the command that fills it."""
        problems = demo_inventory.check(
            [], [], [], {}, [connection("rnacentral", viewers=["visitors"])]
        )
        self.assertIn(
            "no stream is visible to visitors: the visitors' streams page is empty"
            " — run make demo-stream YES=1",
            problems,
        )

    def test_check_names_an_empty_compute_page(self):
        """When: a stream and a connection name `visitors` but no cluster does. Then: check
        names the Compute page and the command that fills it."""
        problems = demo_inventory.check(
            [],
            [stream("demo-pegelonline", viewers=["visitors"])],
            [],
            {},
            [connection("rnacentral", viewers=["visitors"])],
        )
        self.assertIn(
            "no cluster is visible to visitors: the visitors' compute page is empty"
            " — run make demo-cluster",
            problems,
        )

    def test_check_names_an_empty_connections_page(self):
        """When: no connection names `visitors` but a stream does. Then: check names the
        connections page and the command that fills it."""
        problems = demo_inventory.check(
            [], [stream("demo-pegelonline", viewers=["visitors"])], [], {}, []
        )
        self.assertIn(
            "no connection is visible to visitors: the visitors' connections page is empty"
            " — run make demo-connection YES=1",
            problems,
        )

    def test_check_without_connections_keeps_the_old_behaviour(self):
        """When: a caller does not pass the connection list. Then: check stays silent — the
        page rule is not suddenly failing for it."""
        self.assertEqual(demo_inventory.check([], [], [], {}), [])


if __name__ == "__main__":
    unittest.main()
