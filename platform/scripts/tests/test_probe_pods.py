"""Tests for scripts/probe-pod-spec.py and the probe pods that use it (platform#1656).

The namespace enforces `baseline` today and `restricted` when `make verify-pod-security`
runs; under `restricted` a probe without a non-root UID, RuntimeDefault seccomp, no
privilege escalation and `capabilities.drop: ["ALL"]` is refused. These tests pin the Pod
document the helper prints and the fact that every `kubectl run` in the rewired scripts
builds its pod from it. No cluster is needed.
"""

import json
import os
import re
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
HELPER = os.path.join(ROOT, "scripts", "probe-pod-spec.py")
PINNED = "curlimages/curl:8.11.1@sha256:c1fe1679c34d9784c1b0d1e5f62ac0a79fca01fb6377cdd33e90473c6f9f9a69"

# A later ticket adds the remaining scripts; this list is the contract for now.
REWIRED = (
    "scripts/verify-metrics.sh",
    "scripts/verify-seaweedfs-ports.sh",
    "scripts/verify-catalog-permissions.sh",
    "scripts/verify-trino-tls.sh",
    "scripts/verify-spark-cluster.sh",
    "scripts/verify-backups.sh",
)


def run(args, stdin=None):
    return subprocess.run(
        [sys.executable, HELPER] + list(args),
        input=stdin, capture_output=True, text=True, cwd=ROOT,
    )


class ProbePodSpecTest(unittest.TestCase):
    def test_generate_prints_the_restricted_floor(self):
        result = run(["--name", "metrics-scrape-probe", "--image", PINNED,
                      "--command", "sleep 600", "--label", "app.kubernetes.io/name=prometheus"])
        self.assertEqual(result.returncode, 0, result.stderr)
        pod = json.loads(result.stdout)
        spec = pod["spec"]
        self.assertEqual((pod["kind"], pod["metadata"]["name"]), ("Pod", "metrics-scrape-probe"))
        self.assertEqual(pod["metadata"]["labels"], {"app.kubernetes.io/name": "prometheus"})
        self.assertFalse(spec["automountServiceAccountToken"])
        self.assertEqual(spec["restartPolicy"], "Never")
        self.assertIs(spec["securityContext"]["runAsNonRoot"], True)
        self.assertEqual(spec["securityContext"]["runAsUser"], 65534)
        self.assertEqual(spec["securityContext"]["runAsGroup"], 65534)
        self.assertEqual(spec["securityContext"]["seccompProfile"], {"type": "RuntimeDefault"})
        container = spec["containers"][0]
        self.assertEqual(container["image"], PINNED)
        self.assertEqual(container["command"], ["sh", "-c", "exec sleep 600"])
        self.assertIs(container["securityContext"]["allowPrivilegeEscalation"], False)
        self.assertEqual(container["securityContext"]["capabilities"]["drop"], ["ALL"])

    def test_a_floating_tag_is_refused(self):
        result = run(["--name", "p", "--image", "busybox:1.36", "--command", "sleep 1"])
        self.assertEqual(result.returncode, 2, result.stdout)
        self.assertIn("probe-pod-spec: busybox:1.36 is a floating tag", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_merge_keeps_what_the_caller_set(self):
        document = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "s3ls"},
            "spec": {
                "securityContext": {"runAsUser": 185},
                "containers": [{
                    "name": "s3ls",
                    "image": PINNED,
                    "command": ["sh", "-ec", "aws s3 ls"],
                    "env": [{"name": "HOME", "value": "/tmp"}],
                }],
            },
        }
        result = run(["--merge"], stdin=json.dumps(document))
        self.assertEqual(result.returncode, 0, result.stderr)
        pod = json.loads(result.stdout)
        spec = pod["spec"]
        self.assertEqual(spec["securityContext"]["runAsUser"], 185)
        self.assertIs(spec["securityContext"]["runAsNonRoot"], True)
        self.assertEqual(spec["securityContext"]["seccompProfile"], {"type": "RuntimeDefault"})
        self.assertFalse(spec["automountServiceAccountToken"])
        container = spec["containers"][0]
        self.assertEqual(container["command"], ["sh", "-ec", "aws s3 ls"])
        self.assertEqual(container["env"], [{"name": "HOME", "value": "/tmp"}])
        self.assertIs(container["securityContext"]["allowPrivilegeEscalation"], False)
        self.assertEqual(container["securityContext"]["capabilities"]["drop"], ["ALL"])

    def test_every_probe_pod_is_built_by_the_helper(self):
        problems = []
        for relative in REWIRED:
            with open(os.path.join(ROOT, relative), encoding="utf-8") as handle:
                text = handle.read()
            # A trailing `\` continues the command; the flags may be on the next line. A `kubectl`
            # that is not creating a pod (`get`, `exec`, …) never has ` run ` before a shell
            # operator, so `|;&` in between keeps it out.
            for number, line in enumerate(text.replace("\\\n", " ").splitlines(), start=1):
                if not re.search(r"\bkubectl\b[^\n|;&]*\srun\s", line):
                    continue
                for flag in ("--overrides", "probe-pod-spec.py"):
                    if flag not in line:
                        problems.append("%s:%d %s is missing %s" % (relative, number, line.strip(), flag))
        self.assertEqual(problems, [], "\n".join(problems))


if __name__ == "__main__":
    unittest.main()
