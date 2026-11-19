"""Unit tests for scripts/spark-confmap-janitor.sh's liveness rules (platform#1585, stardelt#215).

A long-running StreamApp creates one `spark-exec-<16 hex>-conf-map` when it starts and its
executor keeps using it for the app's whole life, so a janitor that deletes by age alone
removes a map a live executor mounts: a restart of that executor then fails with
`configmap ... not found` (measured on the lab 2026-11-16, see the issue). The shared Connect
server leaves neither a mount nor a `spark-app-selector` label while its executors are scaled
to zero, so its map is attributed by the driver pod that predates it (stardelt#215).

The janitor must keep a map any pod mounts, a map of an app with a live pod, and a map a live
Spark driver could have created; it must delete nothing at all when it cannot list the pods,
and `JANITOR_DRY_RUN=1` must delete nothing.

Standard library only: `kubectl` is replaced by a shell shim first on PATH, so no cluster is
touched and no credential exists anywhere in the test.
"""

import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "spark-confmap-janitor.sh"

SHIM = """#!/usr/bin/env bash
# Fake kubectl: `get configmap(s)` prints FAKE_CONFIGMAPS, `get pods` prints FAKE_PODS and exits
# FAKE_PODS_EXIT, `delete configmap NAME` appends the name to FAKE_DELETED.
set -uo pipefail
args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n | --namespace) shift 2 ;;
    -o | --output) shift 2 ;;
    --ignore-not-found) shift ;;
    *) args+=("$1"); shift ;;
  esac
done
verb="${args[0]-}"
resource="${args[1]-}"
case "$verb $resource" in
  "get configmap" | "get configmaps")
    [ -n "${FAKE_CONFIGMAPS:-}" ] || { echo "fake kubectl: no configmap fixture" >&2; exit 2; }
    printf '%s' "$(cat "$FAKE_CONFIGMAPS")"
    printf '\\n'
    ;;
  "get pod" | "get pods")
    [ -n "${FAKE_PODS:-}" ] || { echo "fake kubectl: no pod fixture" >&2; exit 2; }
    printf '%s' "$(cat "$FAKE_PODS")"
    printf '\\n'
    exit "${FAKE_PODS_EXIT:-0}"
    ;;
  "delete configmap")
    printf '%s\\n' "${args[2]-}" >> "$FAKE_DELETED"
    ;;
  *) echo "fake kubectl: unsupported invocation: $verb $resource" >&2; exit 2 ;;
esac
"""

APP_A = "spark-aaaaaaaaaaaaaaaa"
APP_B = "spark-bbbbbbbbbbbbbbbb"
SPARK_IMAGE = "ghcr.io/stardelt/spark:dev@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"


def stamp(hours_ago=0, **delta):
    """RFC3339 timestamp `hours_ago` hours in the past, shifted by `delta`."""
    moment = datetime.now(timezone.utc) - timedelta(hours=hours_ago) + timedelta(**delta)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


class Janitor(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
        self.configmaps = self.dir / "configmaps.txt"
        self.pods = self.dir / "pods.txt"
        self.deleted = self.dir / "deleted.txt"
        # The `|` rows the janitor's jsonpath produces: name|creationTimestamp|spark-app-selector.
        self.old = stamp(2)
        self.configmaps.write_text(
            "\n".join(
                f"{name}|{when}|{app_id}"
                for name, when, app_id in (
                    ("spark-exec-aaaaaaaaaaaaaaaa-conf-map", self.old, APP_A),
                    ("spark-exec-bbbbbbbbbbbbbbbb-conf-map", stamp(4), APP_B),
                    ("spark-exec-cccccccccccccccc-conf-map", stamp(0), ""),
                )
            )
            + "\n"
        )

    def run_script(self, pods="", pods_exit=0, dry_run=False):
        self.pods.write_text(pods)
        env = dict(
            os.environ,
            PATH=f"{self.dir}:{os.environ['PATH']}",
            NAMESPACE="stardelt",
            JANITOR_MAX_AGE_SECONDS="3600",
            FAKE_CONFIGMAPS=str(self.configmaps),
            FAKE_PODS=str(self.pods),
            FAKE_PODS_EXIT=str(pods_exit),
            FAKE_DELETED=str(self.deleted),
        )
        if dry_run:
            env["JANITOR_DRY_RUN"] = "1"
        else:
            env.pop("JANITOR_DRY_RUN", None)
        return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=60)

    @staticmethod
    def pod_row(name="pod-1", started="", app_id="", mounted="", images="busybox", deleting=""):
        """One row of `name|deletionTimestamp|startTime|spark-app-selector|volumes|images`."""
        mounts = "".join(f"{cm} " for cm in mounted)
        imgs = "".join(f"{image} " for image in images)
        return f"{name}|{deleting}|{started}|{app_id}|{mounts}|{imgs}\n"

    def test_a_map_a_pod_mounts_is_kept_whatever_its_age(self):
        done = self.run_script(
            pods=self.pod_row(mounted=["spark-exec-aaaaaaaaaaaaaaaa-conf-map"])
        )
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("keep   spark-exec-aaaaaaaaaaaaaaaa-conf-map (referenced by a pod)", done.stdout)
        self.assertIn("delete spark-exec-bbbbbbbbbbbbbbbb-conf-map", done.stdout)
        self.assertIn("keep   spark-exec-cccccccccccccccc-conf-map", done.stdout)
        self.assertEqual(self.deleted.read_text().split(), ["spark-exec-bbbbbbbbbbbbbbbb-conf-map"],
                         "only the unreferenced, too-old map is deleted")

    def test_a_failed_pod_list_deletes_nothing(self):
        done = self.run_script(
            pods=self.pod_row(mounted=["spark-exec-aaaaaaaaaaaaaaaa-conf-map"]), pods_exit=1
        )
        self.assertEqual(done.returncode, 1)
        self.assertIn("refusing to delete anything", done.stderr)
        self.assertFalse(self.deleted.exists(), "no ConfigMap is deleted when the pod list fails")

    def test_with_no_pods_the_old_behaviour_is_unchanged(self):
        done = self.run_script(pods="")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(
            self.deleted.read_text().split(),
            ["spark-exec-aaaaaaaaaaaaaaaa-conf-map", "spark-exec-bbbbbbbbbbbbbbbb-conf-map"],
        )
        self.assertIn("keep   spark-exec-cccccccccccccccc-conf-map", done.stdout)
        self.assertIn("deleted 2 ConfigMap(s)", done.stdout)

    def test_a_map_created_by_a_live_driver_is_kept(self):
        # The shared Connect server: no spark-app-selector label, no mount, driver started a
        # minute before it created the map.
        done = self.run_script(
            pods=self.pod_row(
                name="spark-connect-6747bd6d4b-tlfxb",
                started=stamp(2, minutes=-1),
                images=[SPARK_IMAGE],
            )
        )
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn(
            "keep   spark-exec-aaaaaaaaaaaaaaaa-conf-map (created by a live spark driver)", done.stdout
        )
        self.assertNotIn("spark-exec-aaaaaaaaaaaaaaaa-conf-map", self.deleted.read_text().split(),
                         "the driver's map is not deleted")

    def test_a_map_of_a_live_app_is_kept(self):
        done = self.run_script(pods=self.pod_row(app_id=APP_A))
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn(
            f"keep   spark-exec-aaaaaaaaaaaaaaaa-conf-map (app {APP_A} has a live pod)", done.stdout
        )
        self.assertNotIn("spark-exec-aaaaaaaaaaaaaaaa-conf-map", self.deleted.read_text().split())

    def test_an_old_map_of_a_dead_app_is_deleted(self):
        # The driver restarted three hours after the map was created: it cannot be the map's
        # creator, so the map is a leftover.
        done = self.run_script(
            pods=self.pod_row(started=stamp(2, hours=3), images=[SPARK_IMAGE])
        )
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("delete spark-exec-aaaaaaaaaaaaaaaa-conf-map", done.stdout)
        self.assertIn("spark-exec-aaaaaaaaaaaaaaaa-conf-map", self.deleted.read_text().split())

    def test_dry_run_deletes_nothing(self):
        done = self.run_script(pods="", dry_run=True)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("would delete spark-exec-aaaaaaaaaaaaaaaa-conf-map", done.stdout)
        self.assertIn("would delete 2 ConfigMap(s)", done.stdout)
        self.assertFalse(self.deleted.exists(), "a dry run deletes nothing")


if __name__ == "__main__":
    unittest.main()
