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
from datetime import datetime, timedelta, timezone
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


def seconds_ago(seconds):
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


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

    def test_a_crash_inside_the_window_fails_with_a_cross(self):
        pods = [
            pod("user-scheduler-7d-x", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": seconds_ago(60)}),
        ]
        done = self.run_script(pods, ROWS="user-scheduler")
        self.assertEqual(done.returncode, 1)
        line = [ln for ln in done.stdout.splitlines() if "user-scheduler" in ln][0]
        self.assertTrue(line.startswith("✗"), line)
        self.assertIn("Error exit=1", line)
        self.assertIn("verify-restarts: FAIL (1 Deployment(s) had a container crash within 3600s)",
                      done.stdout)

    def test_the_nodes_own_restart_inside_the_window_is_never_fatal(self):
        pods = [
            pod("user-scheduler-7d-x", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Unknown", "exitCode": 255,
                            "finishedAt": seconds_ago(60)}),
        ]
        done = self.run_script(pods, ROWS="user-scheduler")
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        line = [ln for ln in done.stdout.splitlines() if "user-scheduler" in ln][0]
        self.assertTrue(line.startswith("!"), line)
        self.assertIn("verify-restarts: PASS", done.stdout)

    def test_a_container_still_restarting_on_its_own_is_named_and_fails(self):
        pods = [
            pod("nova-6df9-x", "nova-6df9c8d5b", restarts=3,
                terminated={"reason": "OOMKilled", "exitCode": 137,
                            "finishedAt": seconds_ago(30)}),
            pod("nova-6df9-y", "nova-6df9c8d5b", restarts=2,
                terminated={"reason": "OOMKilled", "exitCode": 137,
                            "finishedAt": seconds_ago(20)}),
        ]
        done = self.run_script(pods, ROWS="nova")
        self.assertEqual(done.returncode, 1)
        line = [ln for ln in done.stdout.splitlines() if "nova" in ln][0]
        self.assertTrue(line.startswith("✗"), line)
        self.assertIn("2 crash(es) within 3600s: OOMKilled exit=137", line)
        self.assertIn("read kubectl -n stardelt logs nova-6df9-x --previous", line)

    def test_a_crash_older_than_the_window_is_printed_and_never_fails(self):
        pods = [
            pod("user-scheduler-7d-x", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": seconds_ago(2 * 3600)}),
        ]
        done = self.run_script(pods, ROWS="user-scheduler", RESTART_MAX_AGE_SECONDS="3600")
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        line = [ln for ln in done.stdout.splitlines() if "user-scheduler" in ln][0]
        self.assertTrue(line.startswith("!"), line)
        self.assertIn("last exit Error exit=1", line)
        self.assertNotIn("crash(es) within", line)
        self.assertIn("verify-restarts: PASS", done.stdout)

    def test_the_window_is_read_from_restart_max_age_seconds(self):
        pods = [
            pod("user-scheduler-7d-x", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": seconds_ago(120)}),
        ]
        done = self.run_script(pods, ROWS="user-scheduler", RESTART_MAX_AGE_SECONDS="60")
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("verify-restarts: PASS", done.stdout)

    def test_an_error_exit_at_the_node_stall_is_not_fatal(self):
        pods = deployment_pods("nova") + [
            pod("nova-6df9-x", "nova-6df9c8d5b", restarts=2,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": seconds_ago(60)}),
            pod("user-scheduler-7d-z", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Unknown", "exitCode": 255,
                            "finishedAt": seconds_ago(55)}),
        ]
        done = self.run_script(pods, ROWS="nova")
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        line = [ln for ln in done.stdout.splitlines() if "nova" in ln][0]
        self.assertTrue(line.startswith("!"), line)
        self.assertIn("node-caused exit(s)", line)
        self.assertIn("verify-restarts: PASS", done.stdout)
        self.assertNotIn("✗", done.stdout)

    def test_an_error_exit_far_from_the_node_stall_still_fails(self):
        pods = deployment_pods("nova") + [
            pod("nova-6df9-x", "nova-6df9c8d5b", restarts=2,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": seconds_ago(600)}),
            pod("user-scheduler-7d-z", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Unknown", "exitCode": 255,
                            "finishedAt": seconds_ago(60)}),
        ]
        done = self.run_script(pods, ROWS="nova")
        self.assertEqual(done.returncode, 1)
        line = [ln for ln in done.stdout.splitlines() if "nova" in ln][0]
        self.assertTrue(line.startswith("✗"), line)
        self.assertIn("verify-restarts: FAIL (1 Deployment(s) had a container crash within 3600s)",
                      done.stdout)

    def test_an_oom_killed_exit_is_never_node_caused(self):
        pods = deployment_pods("nova") + [
            pod("nova-6df9-x", "nova-6df9c8d5b", restarts=2,
                terminated={"reason": "OOMKilled", "exitCode": 137,
                            "finishedAt": seconds_ago(60)}),
            pod("user-scheduler-7d-z", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Unknown", "exitCode": 255,
                            "finishedAt": seconds_ago(55)}),
        ]
        done = self.run_script(pods, ROWS="nova")
        self.assertEqual(done.returncode, 1)
        line = [ln for ln in done.stdout.splitlines() if "nova" in ln][0]
        self.assertTrue(line.startswith("✗"), line)
        self.assertIn("OOMKilled exit=137", line)

    def test_the_node_stall_window_is_read_from_the_environment(self):
        pods = deployment_pods("nova") + [
            pod("nova-6df9-x", "nova-6df9c8d5b", restarts=2,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": seconds_ago(60)}),
            pod("user-scheduler-7d-z", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Unknown", "exitCode": 255,
                            "finishedAt": seconds_ago(55)}),
        ]
        done = self.run_script(pods, ROWS="nova", RESTART_NODE_STALL_WINDOW="2")
        self.assertEqual(done.returncode, 1)

    def test_error_exits_of_several_controllers_in_one_window_are_the_cluster_stall(self):
        pods = [
            pod("user-scheduler-7d-x", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": seconds_ago(60)}),
            pod("spark-operator-controller-7d-x", "spark-operator-controller-5c6d7f8b9",
                restarts=1,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": seconds_ago(54)}),
            pod("cnpg-cloudnative-pg-7d-x", "cnpg-cloudnative-pg-5c6d7f8b9", restarts=1,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": seconds_ago(48)}),
        ]
        rows = "user-scheduler,spark-operator-controller,cnpg-cloudnative-pg"
        done = self.run_script(pods, ROWS=rows)
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        lines = [ln for ln in done.stdout.splitlines() if "pod(s) ready" in ln]
        self.assertEqual(len(lines), 3, done.stdout)
        for line in lines:
            self.assertTrue(line.startswith("!"), line)
            self.assertIn("cluster-wide exit(s)", line)
            self.assertIn("controllers lost their leases", line)
        self.assertIn("verify-restarts: PASS", done.stdout)
        self.assertNotIn("✗", done.stdout)

    def test_an_old_node_stall_does_not_relabel_a_cluster_stall(self):
        """The lab keeps old exit=255 exits, so `stalls` is never empty (#267).

        A row whose own exit is far outside the 120 s node window is a cluster stall even when an
        Unknown/255 exit exists somewhere in the namespace — the sentence must not quote 600 s as
        a distance to a 120 s window.
        """
        pods = [
            pod("user-scheduler-7d-x", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": seconds_ago(60)}),
            pod("spark-operator-controller-7d-x", "spark-operator-controller-5c6d7f8b9",
                restarts=1,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": seconds_ago(54)}),
            pod("stardelt-operator-7d-x", "stardelt-operator-5c6d7f8b9", restarts=1,
                terminated={"reason": "Unknown", "exitCode": 255,
                            "finishedAt": seconds_ago(600)}),
        ]
        rows = "user-scheduler,spark-operator-controller,stardelt-operator"
        done = self.run_script(pods, ROWS=rows)
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        lines = [ln for ln in done.stdout.splitlines() if "pod(s) ready" in ln]
        self.assertEqual(len(lines), 3, done.stdout)
        for name in ("user-scheduler", "spark-operator-controller"):
            line = [ln for ln in lines if name in ln][0]
            self.assertTrue(line.startswith("!"), line)
            self.assertIn("cluster-wide exit(s)", line)
            self.assertNotIn("of an unknown exit=255", line)
        self.assertIn("verify-restarts: PASS", done.stdout)

    def test_one_controller_still_fails_on_its_own_error_exit(self):
        pods = [
            pod("user-scheduler-7d-x", "user-scheduler-5c6d7f8b9", restarts=1,
                terminated={"reason": "Error", "exitCode": 1,
                            "finishedAt": seconds_ago(60)}),
        ]
        done = self.run_script(pods, ROWS="user-scheduler")
        self.assertEqual(done.returncode, 1)
        line = [ln for ln in done.stdout.splitlines() if "user-scheduler" in ln][0]
        self.assertTrue(line.startswith("✗"), line)
        self.assertIn("1 crash(es) within 3600s: Error exit=1", line)
        self.assertIn("verify-restarts: FAIL (1 Deployment(s) had a container crash within 3600s)",
                      done.stdout)

    def test_the_statefulset_pod_is_not_matched_by_the_name_prefix(self):
        pods = deployment_pods("nova", replicas=1) + [
            pod("nova-pg-1", "nova-pg", owner_kind="StatefulSet"),
        ]
        done = self.run_script(pods, ROWS="nova")
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("1 pod(s) ready, 0 restart(s) in total", done.stdout)
        self.assertNotIn("nova-pg", done.stdout)

    def test_a_spark_connect_restart_is_reported(self):
        pods = [
            pod("spark-connect-649c65f694-zw4md", "spark-connect-649c65f694", restarts=0),
            pod("spark-connect-auth-5f9975b8f6-2thdc", "spark-connect-auth-5f9975b8f6", restarts=6),
        ]
        done = self.run_script(pods)
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("✓ spark-connect ", done.stdout)
        self.assertIn("! spark-connect-auth", done.stdout)
        self.assertIn("1 pod(s) ready, 6 restart(s) in total", done.stdout)
        self.assertNotIn("2 pod(s) ready", done.stdout)

    def test_the_default_rows_include_the_deployments_that_restart(self):
        done = self.run_script(deployment_pods("nova", replicas=1))
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("– cnpg-cloudnative-pg", done.stdout)
        self.assertIn("– barman-cloud-plugin-barman-cloud", done.stdout)
        self.assertIn("– lakekeeper", done.stdout)
        self.assertIn("– spark-connect", done.stdout)
        self.assertIn("– spark-connect-auth", done.stdout)
        self.assertIn("verify-restarts: PASS (1 of 13 Deployment(s)", done.stdout)

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
