"""The listing pod's contract in scripts/verify-backups-all.sh (stardelt/stardelt#589).

Standard library only, like the other tests here. `kubectl` is a shell shim first on PATH: it
serves one PVC and one CronJob, records the pods it was asked to create and delete, and answers
`wait` and `logs` from the environment, so the test drives the three output shapes the script must
tell apart:

  A. `logs` empty                      -> a failed attempt, retried, then "answered nothing";
  B. `logs` has `NOW` but no `SOURCE`  -> the same;
  C. `logs` has `NOW` and one `SOURCE` per requested source -> PASS.

No cluster is touched.
"""
import json
import os
import shutil
import stat
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-backups-all.sh"

SHIM = """#!/usr/bin/env bash
set -u
# Fake kubectl. `-n NS get pvc|cronjob -o json` serves canned files, every other `get` fails (the
# CNPG kinds and the off-site Secret are absent). `run NAME ...` records the attempt, `wait` exits
# with FAKE_WAIT_EXIT, `logs` prints the file FAKE_LOGS, `describe` prints an events line.
shift 2
verb="${1-}"
case "$verb" in
  get)
    case "${2-}" in
      pvc) cat "$FAKE_DIR/pvc.json" ;;
      cronjob) cat "$FAKE_DIR/cronjob.json" ;;
      *) exit 1 ;;
    esac
    ;;
  run)
    echo "$2" >> "$FAKE_DIR/run-names"
    ;;
  wait)
    exit "${FAKE_WAIT_EXIT:-0}"
    ;;
  logs)
    cat "$FAKE_LOGS"
    ;;
  describe)
    echo "Events:"
    echo "  Normal  Created  container s3ls"
    echo "  Warning BackOff  container s3ls is waiting to start"
    ;;
  delete)
    echo "$3" >> "$FAKE_DIR/deleted"
    ;;
esac
exit 0
"""


class ListingPodContract(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="backups-listing-test-"))
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
        (self.dir / "pvc.json").write_text(
            json.dumps({"items": [{"metadata": {"name": "data-airflow-postgresql-0"}}]})
        )
        (self.dir / "cronjob.json").write_text(
            json.dumps({"items": [{"metadata": {"name": "airflow-pg-dump"}, "status": {}}]})
        )
        self.logs = self.dir / "logs.txt"
        self.now = int(time.time())

    def run_script(self, logs, wait_exit=0, attempts="2"):
        self.logs.write_text(logs)
        env = dict(
            os.environ,
            PATH=f"{self.dir}{os.pathsep}{os.environ['PATH']}",
            NAMESPACE="stardelt",
            FAKE_DIR=str(self.dir),
            FAKE_LOGS=str(self.logs),
            FAKE_WAIT_EXIT=str(wait_exit),
            LIST_ATTEMPTS=str(attempts),
        )
        return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=120)

    def names(self):
        path = self.dir / "run-names"
        return path.read_text().split() if path.exists() else []

    def deleted(self):
        path = self.dir / "deleted"
        return path.read_text().split() if path.exists() else []

    def test_a_pod_that_answers_nothing_is_retried_and_said_to_have_answered_nothing(self):
        done = self.run_script("")
        self.assertEqual(done.returncode, 3, done.stdout + done.stderr)
        self.assertEqual(len(self.names()), 2, "LIST_ATTEMPTS=2 means two attempts")
        self.assertIn("the listing pod answered nothing (attempt 1 of 2); its whole output:", done.stderr)
        self.assertIn("the listing pod answered nothing (attempt 2 of 2); its whole output:", done.stderr)
        self.assertIn("(missing: no NOW line; no SOURCE line for pgdump/airflow)", done.stderr)
        self.assertIn("the listing pod answered nothing in 2 attempt(s); this backup was NOT checked", done.stderr)
        self.assertNotIn("the listing pod failed (attempt", done.stderr)
        self.assertIn("verify-backups-all: UNVERIFIED", done.stderr)

    def test_a_pod_that_prints_only_its_clock_is_retried_too(self):
        done = self.run_script(f"NOW {self.now}\n")
        self.assertEqual(done.returncode, 3, done.stdout + done.stderr)
        self.assertEqual(len(self.names()), 2)
        self.assertIn("(missing: no SOURCE line for pgdump/airflow)", done.stderr)
        self.assertIn("answered nothing in 2 attempt(s)", done.stderr)
        self.assertNotIn("the listing pod failed (attempt", done.stderr)

    def test_a_complete_answer_passes_on_the_first_attempt_and_the_pod_is_deleted(self):
        done = self.run_script(f"NOW {self.now}\nSOURCE pgdump/airflow {self.now - 3600} pgdump/airflow/x.sql.gz\n")
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("airflow-postgresql-0", done.stdout)
        self.assertIn("PASS", done.stdout)
        self.assertIn("verify-backups-all: PASS", done.stdout)
        self.assertEqual(len(self.names()), 1)
        self.assertEqual(self.deleted(), self.names(), "every attempt deletes its pod, success included")
        self.assertNotIn("answered nothing", done.stderr)
        self.assertNotIn("the listing pod failed (attempt", done.stderr)

    def test_a_wait_that_fails_still_says_failed_and_shows_the_events(self):
        done = self.run_script("", wait_exit=1, attempts="1")
        self.assertEqual(done.returncode, 3, done.stdout + done.stderr)
        self.assertIn("the listing pod failed (attempt 1 of 1); its whole output:", done.stderr)
        self.assertIn("the listing pod failed 1 time(s), its output is printed above", done.stderr)
        self.assertIn("Warning BackOff", done.stderr, "the pod's events are captured before the pod is deleted")
        self.assertNotIn("answered nothing", done.stderr)
        self.assertEqual(self.deleted(), self.names())


if __name__ == "__main__":
    unittest.main()
