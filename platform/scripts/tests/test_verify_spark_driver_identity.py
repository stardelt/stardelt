"""The Spark driver-identity verifier names every thing it claims to prove (#1119).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests`
runs them in CI, and `python3 -m pytest scripts/tests/test_verify_spark_driver_identity.py -q`
runs the same tests. No cluster is touched — the script is read, and `bash -n` parses it.

The point of each assertion here is its mutation: drop one of the five claims or the command
behind it, add a `/catalog/v1/config` call to the code (the header may name it to say why it is
never called), or drop the trap, and exactly one test fails.
"""

import re
import subprocess
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "verify-spark-driver-identity.sh"

# The five claims: the words a reader sees on the ✓/✗ line, and the command behind it. Losing
# either one is the mutation these tests exist for — a check that no longer names what it proves,
# or one that prints the right words without asking the cluster.
CLAIMS = [
    ("status.driverServiceAccount is",
     r'\$driver" = "\$sa"'),
    ("status.workloadIdentity is",
     r'\$workload" = "\$want"'),
    ("is bound to the shared spark-cluster-driver Role",
     r'get rolebinding "\$sa" -o jsonpath'),
    ("ServiceAccount .* exists",
     r'get sa "\$sa" -o json'),
    ("the catalog accepts the driver's projected audience",
     r'create token "\$sa" --audience="\$AUDIENCE"'),
]


class SparkDriverIdentityVerifier(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = SCRIPT.read_text()

    def test_it_parses_and_is_executable(self):
        done = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertTrue(SCRIPT.stat().st_mode & 0o111, "the script is not executable")

    def test_it_forwards_the_catalog_and_defaults_to_port_18088(self):
        self.assertIn("port-forward svc/lakekeeper", self.text)
        self.assertIn('PORT="${PORT:-18088}"', self.text)

    def test_it_names_all_claims_with_the_command_behind_them(self):
        missing = []
        for label, subject in CLAIMS:
            if not re.search(label, self.text):
                missing.append(f"label: {label}")
            if not re.search(subject, self.text):
                missing.append(f"what it looks at: {subject}")
        self.assertEqual(missing, [], "these assertions are not in the script")

    def test_the_catalog_surface_is_read_only(self):
        """`/catalog/v1/config` creates the principal (#538); a check must not call it.

        The header names that path to say why it is never called, so the search skips comment
        lines — exactly as `test_verify_workload_identity.py` does.
        """
        self.assertIn("/management/v1/whoami", self.text)
        for number, line in enumerate(self.text.splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            for forbidden in ("catalog/v1/config", "-X DELETE", "-X PUT", "-X POST", "-X PATCH",
                              "--request", "--data"):
                self.assertNotIn(forbidden, line,
                                 f"line {number}: {forbidden} writes or provisions a principal")
            if "curl" in line:
                self.assertIsNone(re.search(r"(^|\s)-d(\s|=)", line),
                                  f"line {number}: -d sends a body to the catalog")
            if "/management/" in line:
                self.assertIn("/management/v1/whoami", line,
                              f"line {number}: only whoami is read-only in /management")

    def test_every_exit_3_path_exists(self):
        self.assertIn("unverified \"$cmd not found\"", self.text)
        self.assertIn('unverified "port $PORT is already in use', self.text)
        self.assertIn('unverified "no group-owned SparkCluster in $NS"', self.text)

    def test_a_failed_parse_is_unverified_not_fail(self):
        """Exit 3 promises "could not see the subject"; a traceback exits 1 (#1132 item 1)."""
        self.assertIn('if ! rows="$(printf', self.text)
        self.assertIn('unverified "cannot parse the SparkCluster list in $NS"', self.text)
        self.assertIn('if ! sa_uid="$(printf', self.text)
        self.assertIn('unverified "cannot parse ServiceAccount $sa"', self.text)

    def test_an_empty_minted_token_is_named_not_a_401(self):
        """An empty token file is a Bearer with no token: the 401 names the wrong cause (#1132)."""
        self.assertIn('[ ! -s "$token_file" ]', self.text)
        self.assertIn("the API server minted an empty token for ServiceAccount", self.text)

    def test_the_rolebinding_claim_checks_the_kind_and_the_subject(self):
        """A binding named after the ServiceAccount but bound to another subject passed (#1132)."""
        self.assertIn(".roleRef.kind", self.text)
        self.assertIn(".roleRef.name", self.text)
        self.assertIn("ServiceAccount/$sa", self.text)

    def test_the_cleanup_kills_the_forward_and_removes_the_tmp_dir(self):
        self.assertIn("trap cleanup EXIT", self.text)
        cleanup = self.text.split("cleanup()", 1)[1].split("\n}\n", 1)[0]
        self.assertIn('kill "$pf_pid"', cleanup)
        self.assertIn('rm -rf "$tmp"', cleanup)


if __name__ == "__main__":
    unittest.main()
