"""The Spark driver-identity verifier names every thing it claims to prove (#1119).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests`
runs them in CI, and `python3 -m pytest scripts/tests/test_verify_spark_driver_identity.py -q`
runs the same tests. No cluster and no credential are needed: the claims are read as text, and
`Behaviour` runs the script against `kubectl`/`curl` shims placed first on `PATH` — the shape
`test_verify_workload_identity.py` uses (#1144).

The point of each assertion here is its mutation: drop one of the five claims or the command
behind it, add a `/catalog/v1/config` call to the code (the header may name it to say why it is
never called), drop the trap, or drop a FAIL/UNVERIFIED message, and exactly one test fails.
"""

import json
import os
import re
import shutil
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "verify-spark-driver-identity.sh"
BASH = shutil.which("bash") or "/bin/bash"

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


KUBECTL = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl: the cluster list from FAKE_CLUSTERS, the ServiceAccount uid from FAKE_SA_UID,
# the binding from FAKE_BINDING, an empty token from FAKE_TOKEN. No cluster is touched.
case " $* " in
  *" get sparkclusters "*)
    printf '%s' "${FAKE_CLUSTERS}" ;;
  *" get sa "*)
    if [ -n "${FAKE_SA_MISSING:-}" ]; then
      echo "Error from server (NotFound): serviceaccounts \\"$FAKE_SA_MISSING\\" not found" >&2
      exit 1
    fi
    printf '{"metadata":{"uid":"%s"}}' "${FAKE_SA_UID:-sa-uid-visitors}" ;;
  *" get rolebinding "*)
    printf '%s' "${FAKE_BINDING:-Role|spark-cluster-driver|ServiceAccount/spark-cluster-driver-visitors }" ;;
  *" create token "*)
    printf '%s' "${FAKE_TOKEN-fake-driver-token}" ;;
  *" port-forward "*)
    exec sleep 300 ;;
  *) : ;;
esac
exit 0
"""

CURL = """#!/usr/bin/env bash
set -euo pipefail
out=/dev/null
header=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    -H) header="${2#@}"; shift 2 ;;
    -m|--max-time|-w) shift 2 ;;
    *) shift ;;
  esac
done
code=401
body='{}'
if [ -n "$header" ] && [ -s "$header" ]; then
  code="${FAKE_WHOAMI_CODE:-404}"
  body="${FAKE_WHOAMI_BODY:-{\\"error\\":{\\"code\\":\\"UserNotFound\\"}}}"
fi
if [ "$out" != /dev/null ]; then
  printf '%s' "$body" > "$out"
fi
printf '%s' "$code"
"""


def cluster(name="demo-spark", group="visitors", driver=None, workload=None):
    """One group-owned SparkCluster as the verifier reads it."""
    sa = f"spark-cluster-driver-{group}"
    return {
        "metadata": {"name": name},
        "spec": {"owner": {"group": group}},
        "status": {
            "driverServiceAccount": driver if driver is not None else sa,
            "workloadIdentity": workload if workload is not None else f"kubernetes~sa-uid-{group}",
        },
    }


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Behaviour(unittest.TestCase):
    """The script runs against shims: exit codes 0/1/3 and the token rule are measured."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        for name, text in (("kubectl", KUBECTL), ("curl", CURL)):
            shim = self.dir / name
            shim.write_text(text.replace("\n   ", "\n"))
            shim.chmod(0o755)
        self.port = free_port()

    def run_script(self, clusters=None, **extra):
        if clusters is None:
            clusters = [cluster()]
        env = dict(
            os.environ,
            PATH=f"{self.dir}:{os.environ['PATH']}",
            NS="stardelt",
            PORT=str(self.port),
            TMPDIR=str(self.dir),
            FAKE_CLUSTERS=json.dumps({"items": clusters}),
        )
        env.update(extra)
        return subprocess.run([BASH, str(SCRIPT)], env=env, capture_output=True, text=True)

    def test_all_green_passes_and_prints_no_token(self):
        result = self.run_script()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("✓ demo-spark: ServiceAccount spark-cluster-driver-visitors exists", result.stdout)
        self.assertIn(
            "✓ demo-spark: the driver's token authenticates (the principal is not provisioned yet)",
            result.stdout,
        )
        self.assertIn("verify-spark-driver-identity: PASS (1 group-owned SparkCluster(s))", result.stdout)
        self.assertNotIn("fake-driver-token", result.stdout)
        self.assertNotIn("fake-driver-token", result.stderr)
        accepted = self.run_script(FAKE_WHOAMI_CODE="200")
        self.assertEqual(0, accepted.returncode, accepted.stdout + accepted.stderr)
        self.assertIn("✓ demo-spark: the catalog accepts the driver's projected audience", accepted.stdout)

    def test_a_missing_serviceaccount_is_a_fail(self):
        result = self.run_script(FAKE_SA_MISSING="spark-cluster-driver-visitors")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(
            "FAIL (demo-spark: ServiceAccount spark-cluster-driver-visitors is missing — roll the operator (#1100))",
            result.stdout,
        )

    def test_a_foreign_rolebinding_subject_is_a_fail(self):
        result = self.run_script(
            FAKE_BINDING="Role|spark-cluster-driver|ServiceAccount/spark-cluster-driver-other "
        )
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn("ServiceAccount/spark-cluster-driver-visitors", result.stdout)
        self.assertIn("FAIL (demo-spark: the RoleBinding of spark-cluster-driver-visitors", result.stdout)

    def test_an_empty_minted_token_names_itself(self):
        result = self.run_script(FAKE_TOKEN="")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(
            "FAIL (demo-spark: the API server minted an empty token for ServiceAccount spark-cluster-driver-visitors)",
            result.stdout,
        )

    def test_a_broken_cluster_list_is_unverified(self):
        result = self.run_script(FAKE_CLUSTERS="not json")
        self.assertEqual(3, result.returncode, result.stdout + result.stderr)
        self.assertIn("UNVERIFIED (cannot parse the SparkCluster list in stardelt)", result.stdout)

    def test_a_401_from_the_catalog_is_a_fail(self):
        result = self.run_script(FAKE_WHOAMI_CODE="401")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(
            "FAIL (demo-spark: the catalog refuses the driver's ServiceAccount token (HTTP 401) — the workload audience is off)",
            result.stdout,
        )


if __name__ == "__main__":
    unittest.main()
