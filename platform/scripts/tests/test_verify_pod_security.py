"""A pod that finishes between the scan's list and its per-pod read (stardelt-platform#1525).

`scripts/verify-pod-security.sh` lists the Running pods and then reads each one's spec: two API
calls. A pipeline task pod or a CronJob pod can finish in between, and under `set -euo pipefail`
the second call's NotFound aborted the whole scan, turning a green floor into exit 1. The scan now
skips the vanished pod and keeps it out of the `of N` total.

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
"""Fake kubectl: seven Running pods, and p7 is gone when its spec is read."""
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


def pod(name):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "stardelt", "labels": {"app": "demo"}},
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

    def run_script(self, *arguments, labels=None):
        env = dict(
            os.environ,
            PATH=f"{self.tmp.name}:{os.environ['PATH']}",
            NS="stardelt",
            POD_SECURITY_PROBE_NS="stardelt-pss-probe-test",
            TMPDIR=self.tmp.name,
        )
        if labels is not None:
            env["NAMESPACE_LABELS"] = labels
        return subprocess.run(
            ["bash", str(SCRIPT), *arguments], env=env, capture_output=True, text=True, timeout=120
        )

    def test_a_pod_that_finishes_mid_scan_is_skipped(self):
        result = self.run_script("--level", "baseline", "--allow-namespace-level", "baseline")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("– p7: gone before the probe (skipped)", result.stdout)
        self.assertTrue(
            result.stdout.strip().endswith(
                "PASS (6 of 6 pods admitted by baseline, 0 known exceptions)"
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


if __name__ == "__main__":
    unittest.main()
