"""Unit tests for scripts/verify-cronjobs.sh (platform#1586).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl` is replaced by a shell shim first on `PATH`; it answers
`get cronjobs -o json` and `get jobs -o json` from fixture files, so no cluster is touched and
the failure case (a newest Job that failed) is proven without breaking a live CronJob.
"""
import json
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-cronjobs.sh"

SHIM = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl: records every call, then answers `get cronjobs`/`get jobs` from the fixtures.
# FAKE_JOBS_FAIL=1 makes `get jobs` exit 1, the API-error case.
printf '%s\\n' "$*" >> "$FAKE_CALLS"
args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n | --namespace) shift 2 ;;
    -o | --output) shift 2 ;;
    *) args+=("$1"); shift ;;
  esac
done
verb="${args[0]-}"
resource="${args[1]-}"
case "$verb" in
  get) ;;
  *) echo "fake kubectl: unsupported verb: $verb" >&2; exit 2 ;;
esac
case "$resource" in
  namespace) ;;
  cronjobs) cat "$FAKE_CRONJOBS" ;;
  jobs) [ -z "${FAKE_JOBS_FAIL:-}" ] || exit 1; cat "$FAKE_JOBS" ;;
  *) echo "fake kubectl: unexpected resource: $resource" >&2; exit 2 ;;
esac
"""


def cronjob(name, suspend=False):
    return {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": {"name": name, "namespace": "stardelt"},
        "spec": {"schedule": "*/10 * * * *", "suspend": suspend},
    }


def job(name, owner, created, active=0, succeeded=0, failed=0):
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": "stardelt",
            "creationTimestamp": created,
            "ownerReferences": [
                {"apiVersion": "batch/v1", "kind": "CronJob", "name": owner, "uid": "uid-" + owner},
            ],
        },
        "status": {
            "active": active,
            "succeeded": succeeded,
            "failed": failed,
            "conditions": [],
        },
    }


class VerifyCronJobs(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
        self.calls = self.dir / "calls"
        self.cronjobs = self.dir / "cronjobs.json"
        self.jobs = self.dir / "jobs.json"

    def run_script(self, cronjobs, jobs, **extra):
        self.cronjobs.write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": cronjobs}))
        self.jobs.write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": jobs}))
        env = dict(
            os.environ,
            PATH=f"{self.dir}:{os.environ['PATH']}",
            NAMESPACE="stardelt",
            FAKE_CALLS=str(self.calls),
            FAKE_CRONJOBS=str(self.cronjobs),
            FAKE_JOBS=str(self.jobs),
        )
        env.update(extra)
        return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=60)

    def test_a_succeeded_newest_job_is_green(self):
        done = self.run_script(
            [cronjob("access-sync")],
            [job("access-sync-29914038", "access-sync", "2026-11-16T15:34:00Z", succeeded=1)],
        )
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("✓ access-sync", done.stdout)
        self.assertIn("access-sync-29914038 succeeded", done.stdout)
        self.assertIn("PASS (1 CronJobs: every newest Job succeeded, is running, or is suspended)", done.stdout)

    def test_a_failed_newest_job_fails_with_the_job_name_and_the_logs_hint(self):
        done = self.run_script(
            [cronjob("spark-confmap-janitor")],
            [job("spark-confmap-janitor-29914020", "spark-confmap-janitor",
                 "2026-11-16T15:00:00Z", failed=3)],
        )
        self.assertEqual(done.returncode, 1)
        self.assertIn("✗ spark-confmap-janitor", done.stdout)
        self.assertIn("spark-confmap-janitor-29914020 failed (3 attempt(s))", done.stdout)
        self.assertIn("hint: kubectl -n stardelt logs job/spark-confmap-janitor-29914020 --tail=20", done.stdout)
        self.assertIn("FAIL (1 CronJob(s) with a failed newest Job)", done.stdout)
        self.assertNotIn("✓", done.stdout)

    def test_a_suspended_cronjob_and_one_without_a_job_are_not_failures(self):
        done = self.run_script(
            [cronjob("paused", suspend=True), cronjob("fresh")],
            [],
        )
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("– paused", done.stdout)
        self.assertIn("suspended", done.stdout)
        self.assertIn("– fresh", done.stdout)
        self.assertIn("no Job yet", done.stdout)
        self.assertIn("PASS (2 CronJobs", done.stdout)

    def test_a_running_newest_job_is_not_a_failure(self):
        done = self.run_script(
            [cronjob("lakehouse-backup")],
            [job("lakehouse-backup-29913315", "lakehouse-backup", "2026-11-16T03:15:00Z", active=1)],
        )
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("… lakehouse-backup", done.stdout)
        self.assertIn("lakehouse-backup-29913315 is running", done.stdout)
        self.assertIn("PASS (1 CronJobs", done.stdout)

    def test_a_jobs_list_that_cannot_be_read_is_unverified(self):
        done = self.run_script(
            [cronjob("access-sync")],
            [job("access-sync-29914038", "access-sync", "2026-11-16T15:34:00Z", succeeded=1)],
            FAKE_JOBS_FAIL="1",
        )
        self.assertEqual(done.returncode, 3)
        self.assertIn("UNVERIFIED", done.stdout)
        self.assertNotIn("PASS", done.stdout)

    def test_the_newest_job_is_the_signal_not_any_failure(self):
        done = self.run_script(
            [cronjob("ddns")],
            [
                job("ddns-29914020", "ddns", "2026-11-16T14:20:00Z", failed=2),
                job("ddns-29914050", "ddns", "2026-11-16T15:30:00Z", succeeded=1),
            ],
        )
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("✓ ddns", done.stdout)
        self.assertIn("ddns-29914050 succeeded", done.stdout)
        self.assertNotIn("✗", done.stdout)
        self.assertIn("PASS (1 CronJobs", done.stdout)

    def test_a_job_that_is_not_owned_by_the_cronjob_is_ignored(self):
        orphan = job("demo-reset-verify", "some-other-cronjob", "2026-11-16T15:40:00Z", failed=4)
        done = self.run_script(
            [cronjob("demo-reset")],
            [job("demo-reset-29913300", "demo-reset", "2026-11-16T03:00:00Z", succeeded=1), orphan],
        )
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("demo-reset-29913300 succeeded", done.stdout)
        self.assertNotIn("demo-reset-verify", done.stdout)

    def test_the_script_only_reads(self):
        self.run_script(
            [cronjob("access-sync")],
            [job("access-sync-29914038", "access-sync", "2026-11-16T15:34:00Z", succeeded=1)],
        )
        calls = [line for line in self.calls.read_text().splitlines() if line.strip()]
        self.assertTrue(calls, "the shim saw no kubectl call")
        for call in calls:
            self.assertIn(" get ", " " + call + " ")
            for write in ("apply", "create", "delete", "patch", "replace", "edit", "label",
                          "annotate", "scale", "rollout", "run", "exec", "cordon", "drain"):
                self.assertNotIn(" %s " % write, " " + call + " ")


@unittest.skipUnless(shutil.which("make"), "make is not installed")
class VerifyCronJobsTarget(unittest.TestCase):
    def test_the_make_target_runs_the_script(self):
        done = subprocess.run(["make", "-n", "verify-cronjobs", "NAMESPACE=stardelt"], cwd=REPO,
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("NAMESPACE=stardelt bash scripts/verify-cronjobs.sh", done.stdout)


if __name__ == "__main__":
    unittest.main()
