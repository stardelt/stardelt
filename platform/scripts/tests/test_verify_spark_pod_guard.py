"""The three answers to the hostNetwork case of scripts/verify-spark-pod-guard.sh (platform#1489).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl` is replaced by a Python shim that is first on PATH: it admits or denies
what the script applies and prints the answer the test's mode asks for. No cluster is touched.
"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "verify-spark-pod-guard.sh"

SHIM = '''#!/usr/bin/env python3
"""Fake kubectl for the pod-guard verifier: the answer to the hostNetwork probe is FAKE_MODE."""
import os
import sys

argv = sys.argv[1:]
if argv[:1] == ["-n"]:  # every call names the namespace first
    argv = argv[2:]
mode = os.environ.get("FAKE_MODE", "pss")

if argv[:2] == ["get", "validatingadmissionpolicy"]:
    sys.exit(0)
if argv[:2] == ["delete", "configmap"]:
    prefix = "spark-connect"
    for arg in argv:
        if "spark-stream-driver" in arg:
            prefix = "spark-stream"
        elif "spark-batch-driver" in arg:
            prefix = "spark-batch"
    print(f"{prefix}-configmaps denied")
    sys.exit(1)
if argv[:1] == ["apply"]:
    joined = " ".join(argv)
    prefix = "spark-connect"
    if "spark-stream-driver" in joined:
        prefix = "spark-stream"
    elif "spark-batch-driver" in joined:
        prefix = "spark-batch"
    if "fixtures/spark-executor-pod.yaml" in joined:
        sys.exit(0)
    stdin = sys.stdin.read()
    if "hostNetwork: true" in stdin:
        if mode == "pss":
            print('Error from server (Forbidden): error when creating "STDIN": pods '
                  '"spark-guard-probe-host-network" is forbidden: violates PodSecurity '
                  '"baseline:latest": host namespaces (hostNetwork=true)')
            sys.exit(1)
        if mode == "admitted":
            sys.exit(0)
        print("Error from server (Forbidden): something else")
        sys.exit(1)
    if "name: spark-guard-probe" in stdin:
        print(f"{prefix}-executor-pods denied by the policy")
        sys.exit(1)
    if "keycloak-realm-probe" in stdin:
        print(f"{prefix}-configmaps denied by the policy")
        sys.exit(1)
    if "-conf-map" in stdin:
        sys.exit(0)
sys.exit(2)
'''


class HostNetworkCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        shim = Path(self.tmp.name) / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(0o755)

    def run_script(self, mode):
        env = {**os.environ, "PATH": f"{self.tmp.name}:/usr/bin:/bin", "NS": "stardelt",
               "FAKE_MODE": mode}
        return subprocess.run(["bash", str(SCRIPT)], cwd=REPO_ROOT, env=env,
                              capture_output=True, text=True, timeout=60)

    def test_a_pod_security_refusal_of_the_host_network_probe_passes_and_says_so(self):
        done = self.run_script("pss")
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("✓ deny-host-network.yaml refused by Pod Security (hostNetwork) before the policy",
                      done.stdout)
        self.assertIn("PASS (15 cases: 11 denied, 4 admitted)", done.stdout)

    def test_an_admitted_host_network_probe_fails(self):
        done = self.run_script("admitted")
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("✗ deny-host-network.yaml was admitted", done.stdout)

    def test_a_refusal_by_neither_the_policy_nor_pod_security_fails(self):
        done = self.run_script("other")
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn(
            "✗ deny-host-network.yaml was refused, but by neither spark-connect-executor-pods nor Pod Security",
            done.stdout,
        )


if __name__ == "__main__":
    unittest.main()
