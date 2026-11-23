"""A pod that finishes between the scan's list and its per-pod read (stardelt-platform#1525), and
the CronJob pod templates the scan admits too (#1363).

`scripts/verify-pod-security.sh` lists the Running pods and then reads each one's spec: two API
calls. A pipeline task pod or a CronJob pod can finish in between, and under `set -euo pipefail`
the second call's NotFound aborted the whole scan, turning a green floor into exit 1. The scan now
skips the vanished pod and keeps it out of the `of N` total. It re-admits each CronJob's pod
template the same way, in its own counters.

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl` is a Python shim first on PATH, so no cluster is touched.
"""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "verify-pod-security.sh"

# The platform namespace's labels a healthy install carries: `enforce` is `baseline` because
# Lakekeeper's `opa` sidecar does not pass `restricted` in chart 0.14.0 (#1095), while `audit` and
# `warn` stay `restricted`.
NAMESPACE_LABELS = json.dumps(
    {
        "app.kubernetes.io/part-of": "stardelt",
        "pod-security.kubernetes.io/enforce": "baseline",
        "pod-security.kubernetes.io/enforce-version": "latest",
        "pod-security.kubernetes.io/audit": "restricted",
        "pod-security.kubernetes.io/audit-version": "latest",
        "pod-security.kubernetes.io/warn": "restricted",
        "pod-security.kubernetes.io/warn-version": "latest",
    }
)

# The shim's default namespace label document, injected into its source.
SHIM = '''#!/usr/bin/env python3
"""Fake kubectl: seven Running pods, p7 is gone when its spec is read, and CronJobs from env.

`CRONJOBS` is a comma-separated list of `name:ok` / `name:refuse`; `CRONJOB_GONE` and
`CRONJOB_ERROR` name a CronJob whose read answers NotFound / a server error.
"""
import json
import os
import sys

NAMESPACE_LABELS = """__NAMESPACE_LABELS__"""

argv = sys.argv[1:]
namespace = ""
rest = []
i = 0
while i < len(argv):
    if argv[i] in ("-n", "--namespace"):
        namespace = argv[i + 1]
        i += 2
        continue
    rest.append(argv[i])
    i += 1
verb = rest[0] if rest else ""


def pod(name, labels=None):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "stardelt", "labels": labels or {"app": "demo"}},
        "spec": {
            "containers": [
                {
                    "name": "c",
                    "image": "busybox",
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                        "runAsNonRoot": True,
                    },
                }
            ]
        },
        "status": {"phase": "Running"},
    }


def cronjobs():
    out = []
    for entry in filter(None, os.environ.get("CRONJOBS", "").split(",")):
        name, _, verdict = entry.partition(":")
        labels = {"probe_refuse": "true"} if verdict == "refuse" else {"app": "cron"}
        template = pod(name, labels)
        out.append(
            {
                "apiVersion": "batch/v1",
                "kind": "CronJob",
                "metadata": {"name": name, "namespace": "stardelt", "labels": {}},
                "spec": {"jobTemplate": {"spec": {"template": template}}},
            }
        )
    return out


if verb == "get":
    kind = rest[1] if len(rest) > 1 else ""
    if kind == "namespace":
        name = rest[2] if len(rest) > 2 else ""
        if name == "stardelt" and any(a.startswith("jsonpath") for a in rest):
            # The platform namespace's own Pod Security labels (#1096 question 5).
            print(os.environ.get("NAMESPACE_LABELS", NAMESPACE_LABELS))
            sys.exit(0)
        # The probe namespace does not exist yet; after the create the script never asks again.
        sys.exit(1)
    if kind == "pods":
        print(json.dumps({"apiVersion": "v1", "kind": "PodList",
                          "items": [pod("p%d" % n) for n in range(1, 8)]}))
        sys.exit(0)
    if kind == "pod":
        name = rest[2]
        if name == "p7":
            print('Error from server (NotFound): pods "p7" not found', file=sys.stderr)
            sys.exit(1)
        print(json.dumps(pod(name)))
        sys.exit(0)
    if kind == "cronjobs":
        print(json.dumps({"apiVersion": "batch/v1", "kind": "CronJobList",
                          "items": cronjobs()}))
        sys.exit(0)
    if kind == "cronjob":
        name = rest[2]
        if name == os.environ.get("CRONJOB_GONE", ""):
            print('Error from server (NotFound): cronjobs.batch "%s" not found' % name,
                  file=sys.stderr)
            sys.exit(1)
        if name == os.environ.get("CRONJOB_ERROR", ""):
            print('Error from server (Timeout): the server was unable to return a response',
                  file=sys.stderr)
            sys.exit(1)
        for cj in cronjobs():
            if cj["metadata"]["name"] == name:
                print(json.dumps(cj))
                sys.exit(0)
        print('Error from server (NotFound): cronjobs.batch "%s" not found' % name,
              file=sys.stderr)
        sys.exit(1)
if verb == "create" and "--dry-run=server" in rest:
    # The dry run is the verdict: it admits a Pod object and refuses anything else, as the API
    # server does. A vanished pod leaves the scan with an empty file, which the server refuses.
    path = rest[rest.index("-f") + 1] if "-f" in rest else ""
    try:
        body = json.loads(open(path).read())
    except (OSError, ValueError):
        body = {}
    if body.get("kind") != "Pod" or not body.get("metadata", {}).get("name"):
        print("error: no objects passed to create", file=sys.stderr)
        sys.exit(1)
    if (body.get("metadata", {}).get("labels") or {}).get("probe_refuse") == "true":
        print('Error from server (Forbidden): pods "%s" is forbidden: violates PodSecurity '
              '"baseline:latest": allowPrivilegeEscalation != false (container "c" must set '
              'securityContext.allowPrivilegeEscalation=false)'
              % body["metadata"]["name"], file=sys.stderr)
        sys.exit(1)
    sys.exit(0)
if verb in ("create", "label", "delete"):
    sys.exit(0)
print("unexpected kubectl call: %s" % " ".join(argv), file=sys.stderr)
sys.exit(2)
'''


class PodGoneMidScan(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        shim = Path(self.tmp.name) / "kubectl"
        shim.write_text(SHIM.replace("__NAMESPACE_LABELS__", NAMESPACE_LABELS))
        shim.chmod(0o755)

    def run_script(self, *arguments, labels=None, env_extra=None):
        env = dict(
            os.environ,
            PATH=f"{self.tmp.name}:{os.environ['PATH']}",
            NS="stardelt",
            POD_SECURITY_PROBE_NS="stardelt-pss-probe-test",
            TMPDIR=self.tmp.name,
        )
        if labels is not None:
            env["NAMESPACE_LABELS"] = labels
        env.update(env_extra or {})
        return subprocess.run(
            ["bash", str(SCRIPT), *arguments], env=env, capture_output=True, text=True, timeout=120
        )

    def test_a_pod_that_finishes_mid_scan_is_skipped(self):
        result = self.run_script("--level", "baseline", "--allow-namespace-level", "baseline")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("– p7: gone before the probe (skipped)", result.stdout)
        self.assertTrue(
            result.stdout.strip().endswith(
                "PASS (6 admitted by baseline: 6 of 6 pod(s) and 0 of 0 CronJob template(s), "
                "0 known exceptions)"
            ),
            result.stdout,
        )

    def test_the_namespace_label_is_reported_as_a_known_exception(self):
        result = self.run_script("--level", "baseline", "--allow-namespace-level", "baseline")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(
            "– namespace stardelt enforces baseline (allowed by --allow-namespace-level; "
            "audit and warn restricted)",
            result.stdout,
        )

    def test_a_privileged_namespace_label_fails(self):
        labels = json.loads(NAMESPACE_LABELS)
        labels["pod-security.kubernetes.io/enforce"] = "privileged"
        result = self.run_script("--level", "baseline", labels=json.dumps(labels))
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("✗ namespace stardelt:", result.stdout)


class CronJobTemplates(unittest.TestCase):
    """#1363: the pod templates of the namespace's CronJobs are admitted too, in their own
    counters, so the 25 nightly pods (#1362) are audited rather than hand-read."""

    setUp = PodGoneMidScan.setUp
    run_script = PodGoneMidScan.run_script

    def test_a_compliant_template_is_admitted_and_counted(self):
        result = self.run_script(
            "--level", "baseline", "--allow-namespace-level", "baseline",
            env_extra={"CRONJOBS": "good:ok"},
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(
            result.stdout.strip().endswith(
                "PASS (7 admitted by baseline: 6 of 6 pod(s) and 1 of 1 CronJob template(s), "
                "0 known exceptions)"
            ),
            result.stdout,
        )

    def test_a_refused_template_fails_the_run(self):
        result = self.run_script(
            "--level", "baseline", "--allow-namespace-level", "baseline",
            env_extra={"CRONJOBS": "good:ok,bad:refuse"},
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("✗ cronjob/bad: violates PodSecurity", result.stdout)
        self.assertIn(
            "FAIL (7 of 8 admitted: 6 of 6 pod(s), 1 of 2 CronJob template(s); "
            "1 refused, 0 known exceptions)",
            result.stdout,
        )

    def test_an_allow_pattern_exempts_a_template_as_cronjob_name(self):
        result = self.run_script(
            "--level", "baseline", "--allow-namespace-level", "baseline",
            "--allow", "^cronjob/bad$",
            env_extra={"CRONJOBS": "bad:refuse"},
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("– cronjob/bad: known exception (^cronjob/bad$)", result.stdout)
        self.assertTrue(
            result.stdout.strip().endswith(
                "PASS (6 admitted by baseline: 6 of 6 pod(s) and 0 of 1 CronJob template(s), "
                "1 known exception)"
            ),
            result.stdout,
        )

    def test_a_template_gone_mid_scan_is_skipped(self):
        result = self.run_script(
            "--level", "baseline", "--allow-namespace-level", "baseline",
            env_extra={"CRONJOBS": "gone:ok", "CRONJOB_GONE": "gone"},
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("– cronjob/gone: gone before the probe (skipped)", result.stdout)
        self.assertIn("0 of 0 CronJob template(s)", result.stdout)

    def test_a_template_read_error_fails_the_run(self):
        result = self.run_script(
            "--level", "baseline", "--allow-namespace-level", "baseline",
            env_extra={"CRONJOBS": "slow:ok", "CRONJOB_ERROR": "slow"},
        )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("✗ cannot read cronjob/slow: Error from server (Timeout)", result.stdout)

    def test_a_namespace_without_cronjobs_still_passes(self):
        result = self.run_script("--level", "baseline", "--allow-namespace-level", "baseline")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("0 of 0 CronJob template(s)", result.stdout)


if __name__ == "__main__":
    unittest.main()
