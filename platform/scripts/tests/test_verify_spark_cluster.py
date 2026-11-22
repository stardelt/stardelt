"""The SparkCluster verifier names every thing it claims to prove (platform#1487).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests`
runs them in CI, and `python3 -m pytest scripts/tests/test_verify_spark_cluster.py -q` runs the
same tests. No cluster is touched — the script is read, and `bash -n` parses it.

The point of each assertion here is its mutation: drop the trap, drop the CR delete in the
cleanup, drop one of the nine checks or raise the size, and exactly one test fails.
"""

import re
import subprocess
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "verify-spark-cluster.sh"

# The nine checks: the words a reader sees on the ✓/✗ line, and the thing the script looks at.
# Losing either one is the mutation these tests exist for — a check that no longer names what it
# proves, or one that prints the right words without asking the cluster.
CHECKS = [
    ("the phase is Running",
     r'if \[ "\$phase" = Running \]'),
    ("the endpoint starts with sc:// and ends with :15002",
     r"'sc://'\*':15002'"),
    (r"the server pod \$POD is Ready",
     r"containerStatuses\[\*\]\.ready"),
    ("the server pod's serviceAccountName is the owner group's driver",
     r"\.spec\.serviceAccountName"),
    ("ServiceAccount, Role and RoleBinding spark-cluster-driver exist, and",
     r"for kind in serviceaccount role rolebinding"),
    ("policies spark-cluster-executor-pods and spark-cluster-configmaps exist, each with a binding",
     r"for policy in spark-cluster-executor-pods spark-cluster-configmaps"),
    (r"NetworkPolicy \$NP exists",
     r'if kubectl -n "\$NS" get networkpolicy "\$NP"'),
    ("the server pod's log has no 'PKIX path building failed'",
     r"grep -c 'PKIX path building failed'"),
    ("a pod labelled spark-role=executor reaches the driver on 7078",
     r"spark-role=executor"),
]


class SparkClusterVerifier(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = SCRIPT.read_text()

    def test_it_parses_and_is_executable(self):
        done = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertTrue(SCRIPT.stat().st_mode & 0o111, "the script is not executable")

    def test_the_cleanup_runs_on_every_exit_path(self):
        self.assertIn("trap cleanup EXIT", self.text)
        self.assertIn("cleanup()", self.text)

    def test_the_cleanup_deletes_the_cluster_it_created(self):
        cleanup = self.text.split("cleanup()", 1)[1].split("\n}\n", 1)[0]
        self.assertRegex(
            cleanup,
            r'kubectl -n "\$NS" delete sparkcluster "\$NAME"',
            "the cleanup must delete the SparkCluster itself, not only its pods",
        )
        self.assertIn("--ignore-not-found", cleanup)
        self.assertRegex(cleanup, r"SECONDS \+ 120", "the cleanup waits up to 2 min for the pods")

    def test_the_cleanup_waits_for_the_objects_the_cr_owned(self):
        cleanup = self.text.split("cleanup()", 1)[1].split("\n}\n", 1)[0]
        self.assertIn("gone", cleanup, "the cleanup must wait, not only delete")

    def test_the_wait_does_not_pipe_kubectl_into_grep_q(self):
        # `grep -q` closes the pipe at the first match, kubectl dies of SIGPIPE and `pipefail`
        # makes that 141 the pipeline's status, so the negated pipeline answers "gone" while the
        # pod is still Terminating (measured 2026-11-15: the pod outlived the script by a minute).
        code = "\n".join(line for line in self.text.splitlines()
                         if not line.lstrip().startswith("#"))
        self.assertIsNone(
            re.search(r"kubectl[^\n]*\|\s*grep -q", code),
            "a `kubectl … | grep -q` pipeline under pipefail reports 'gone' on SIGPIPE",
        )

    def test_it_names_all_checks(self):
        missing = []
        for label, subject in CHECKS:
            if not re.search(label, self.text):
                missing.append(f"label: {label}")
            if not re.search(subject, self.text):
                missing.append(f"what it looks at: {subject}")
        self.assertEqual(missing, [], "these assertions are not in the script")

    def test_the_cleanup_deletes_the_probe_pod(self):
        cleanup = self.text.split("cleanup()", 1)[1].split("\n}\n", 1)[0]
        self.assertIn('delete pod "$EXEC"', cleanup)

    def test_the_cluster_is_small(self):
        self.assertIn("size: small", self.text)
        self.assertNotRegex(self.text, r"size:\s*(medium|large)")


if __name__ == "__main__":
    unittest.main()
