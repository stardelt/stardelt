"""Unit tests for scripts/verify-restarts.sh (platform#1700).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl` is replaced by a shell shim first on `PATH`; it answers
`get pods -o json` from a fixture file, so no cluster is touched and the failure case (a pod that
is not Ready) is proven without breaking a live pod.
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
SCRIPT = REPO / "scripts" / "verify-restarts.sh"

SHIM = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl: records every call, then answers `get pods` from the fixture.
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
  pods) cat "$FAKE_PODS" ;;
  *) echo "fake kubectl: unexpected resource: $resource" >&2; exit 2 ;;
esac
"""


def pod(name, owner, owner_kind="ReplicaSet", ready=True, restarts=0, terminated=None):
    status = {
        "conditions": [
            {"type": "Ready", "status": "True" if ready else "False"},
            {"type": "ContainersReady", "status": "True" if ready else "False"},
        ],
        "containerStatuses": [
            {"name": "c", "restartCount": restarts, "ready": ready,
             "state": {"running": {}}, "lastState": {}},
        ],
    }
    if terminated:
        status["containerStatuses"][0]["lastState"] = {"terminated": terminated}
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": "stardelt",
            "ownerReferences": [
                {"apiVersion": "apps/v1", "kind": owner_kind, "name": owner, "uid": "uid-" + owner},
            ],
        },
        "status": status,
    }


def deployment_pods(name, replicas=1, restarts=0, ready=True):
    return [
        pod("%s-6df9-%s" % (name, chr(ord("a") + i)), "%s-6df9c8d5b" % name,
            ready=ready, restarts=restarts)
        for i in range(replicas)
    ]


class VerifyRestarts(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
        self.calls = self.dir / "calls"
        self.pods = self.dir / "pods.json"

    def run_script(self, pods, **extra):
        self.pods.write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": pods}))
        env = dict(
            os.environ,
            PATH="%s:%s" % (self.dir, os.environ["PATH"]),
            NAMESPACE="stardelt",
            FAKE_CALLS=str(self.calls),
            FAKE_PODS=str(self.pods),
        )
        env.update(extra)
        return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True,
                              timeout=60)

    def test_a_healthy_deployment_with_restarts_is_green_and_the_count_is_printed(self):
        done = self.run_script(
            deployment_pods("stardelt-operator", replicas=2, restarts=1)
            + deployment_pods("nova", replicas=1),
            ROWS="stardelt-operator,nova",
        )
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("! stardelt-operator", done.stdout)
        self.assertIn("2 pod(s) ready, 2 restart(s) in total", done.stdout)
        self.assertIn("✓ nova", done.stdout)
        self.assertIn("verify-restarts: PASS (2 Deployment(s)", done.stdout)
        self.assertNotIn("✗", done.stdout)

    def test_a_pod_that_is_not_ready_fails_with_a_cross(self):
        done = self.run_script(
            deployment_pods("nova", replicas=1, ready=False),
            ROWS="nova",
        )
        self.assertEqual(done.returncode, 1)
        line = [ln for ln in done.stdout.splitlines() if "nova" in ln][0]
        self.assertTrue(line.startswith("✗"), line)
        self.assertIn("0 of 1 pod(s) ready", line)
        self.assertIn("verify-restarts: FAIL (1 Deployment(s) with a not-Ready pod)", done.stdout)

    def test_an_empty_pod_list_is_unverified(self):
        done = self.run_script([], ROWS="nova")
        self.assertEqual(done.returncode, 3)
        self.assertIn("verify-restarts: UNVERIFIED", done.stdout)
        self.assertNotIn("PASS", done.stdout)
        self.assertNotIn("FAIL", done.stdout)

    def test_a_listed_deployment_without_pods_is_an_honest_dash(self):
        done = self.run_script(deployment_pods("nova", replicas=1), ROWS="nova,hub")
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("– hub", done.stdout)
        self.assertIn("not installed", done.stdout)
        self.assertNotIn("✗", done.stdout)
        self.assertIn("verify-restarts: PASS (1 of 2 Deployment(s)", done.stdout)

    def test_the_newest_termination_is_printed(self):
        pods = [
            pod("user-scheduler-7d-x", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": "2026-11-17T13:52:57Z"}),
            pod("user-scheduler-7d-y", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": "2026-11-17T13:40:00Z"}),
        ]
        done = self.run_script(pods, ROWS="user-scheduler")
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("last exit Error exit=1 at 2026-11-17T13:52:57Z", done.stdout)
        self.assertNotIn("2026-11-17T13:40:00Z", done.stdout)

    def test_the_statefulset_pod_is_not_matched_by_the_name_prefix(self):
        pods = deployment_pods("nova", replicas=1) + [
            pod("nova-pg-1", "nova-pg", owner_kind="StatefulSet"),
        ]
        done = self.run_script(pods, ROWS="nova")
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("1 pod(s) ready, 0 restart(s) in total", done.stdout)
        self.assertNotIn("nova-pg", done.stdout)

    def test_the_default_rows_include_the_deployments_that_restart(self):
        done = self.run_script(deployment_pods("nova", replicas=1))
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("– cnpg-cloudnative-pg", done.stdout)
        self.assertIn("– barman-cloud-plugin-barman-cloud", done.stdout)
        self.assertIn("– lakekeeper", done.stdout)
        self.assertIn("verify-restarts: PASS (1 of 11 Deployment(s)", done.stdout)

    def test_the_script_only_reads(self):
        self.run_script(deployment_pods("nova", replicas=1), ROWS="nova")
        calls = [line for line in self.calls.read_text().splitlines() if line.strip()]
        self.assertEqual(len(calls), 1, calls)
        self.assertIn(" get pods", " " + calls[0] + " ")
        for write in ("apply", "create", "delete", "patch", "replace", "edit", "label",
                      "annotate", "scale", "rollout", "run", "exec", "cordon", "drain"):
            self.assertNotIn(" %s " % write, " " + calls[0] + " ")


@unittest.skipUnless(shutil.which("make"), "make is not installed")
class VerifyRestartsTarget(unittest.TestCase):
    def test_the_make_target_runs_the_script(self):
        done = subprocess.run(["make", "-n", "verify-restarts", "NAMESPACE=stardelt"], cwd=REPO,
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("bash scripts/verify-restarts.sh", done.stdout)

    def test_step_verify_runs_the_script(self):
        done = subprocess.run(["make", "-n", "step-verify", "NAMESPACE=stardelt"], cwd=REPO,
                              capture_output=True, text=True, timeout=120)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("verify-restarts", done.stdout)


if __name__ == "__main__":
    unittest.main()
