"""Unit tests for scripts/verify-workload-identity.sh (stardelt#492, #650, #684).

The gap the tests close: `make verify-workload-identity` is a release check whose whole
subject is invisible until a workload tries to sign in — the ServiceAccount `sp-<name>` of
every ServicePrincipal, its no-automount rule, the ownerReference, and
`status.workloadIdentity: kubernetes~<SA uid>`. A check that read the wrong status as PASS
would turn an old operator, a deleted ServiceAccount or an automounting one into "the chain
holds". The script must also stay read-only: it never calls `/catalog/v1/config` (that call
provisions the principal) and never prints a token.

Standard library only, like the other tests here: `python3 -m unittest discover -s
scripts/tests -v` runs them in CI. The first three tests read the script as text; the rest
put shell shims for `kubectl` and `curl` first on `PATH`, so no cluster is touched and no
token exists.
"""
import json
import os
import shutil
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
SCRIPT = PLATFORM / "scripts" / "verify-workload-identity.sh"
BASH = shutil.which("bash") or "/bin/bash"

KUBECTL = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl. The principals come from FAKE_PRINCIPALS (a JSON array), the ServiceAccount
# from FAKE_SA_* variables, and the token is named after the ServiceAccount so the curl shim
# can answer without seeing a real credential.
name=""
case " $* " in
  *" get serviceprincipals "*)
    if [ -n "${FAKE_PRINCIPALS:-}" ]; then
      printf '%s' "$FAKE_PRINCIPALS"
    else
      printf '{"items":[]}'
    fi
    ;;
  *" port-forward "*)
    exec sleep 300
    ;;
  *" get sa "*)
    for arg in "$@"; do
      case "$arg" in sp-*) name="${arg#sp-}" ;; esac
    done
    if [ "${FAKE_NO_SA:-}" = "$name" ]; then
      echo "Error from server (NotFound): serviceaccounts \\"sp-$name\\" not found" >&2
      exit 1
    fi
    python3 - "$name" <<'PY'
import json, os, sys
name = sys.argv[1]
automount = os.environ.get("FAKE_AUTOMOUNT", "false")
doc = {
    "metadata": {
        "name": "sp-" + name,
        "uid": os.environ.get("FAKE_SA_UID", "sa-uid-" + name),
        "labels": {"stardelt.io/service-principal": os.environ.get("FAKE_SA_LABEL", name)},
        "ownerReferences": [{
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "ServicePrincipal",
            "name": os.environ.get("FAKE_OWNER_NAME", name),
            "controller": os.environ.get("FAKE_OWNER_CONTROLLER", "true") == "true",
            "uid": os.environ.get("FAKE_OWNER_UID", "uid-" + name),
        }],
    },
}
if automount != "absent":
    doc["automountServiceAccountToken"] = automount == "true"
print(json.dumps(doc))
PY
    ;;
  *" create token "*)
    for arg in "$@"; do
      case "$arg" in sp-*) name="${arg#sp-}" ;; esac
    done
    printf 'fake-%s-token' "$name"
    ;;
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


def principal(name: str, workload: str) -> dict:
    return {
        "metadata": {"name": name, "uid": "uid-" + name},
        "spec": {"ownerGroup": "visitors"},
        "status": {"workloadIdentity": workload},
    }


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ScriptText(unittest.TestCase):
    """The read-only rules the script must keep, read from its text."""

    @classmethod
    def setUpClass(cls):
        cls.text = SCRIPT.read_text()

    def test_it_never_provisions_the_principal(self):
        """`/catalog/v1/config` creates the principal (#538); a check must not call it."""
        for number, line in enumerate(self.text.splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            self.assertNotIn("catalog/v1/config", line, "line %d" % number)
            self.assertNotIn("catalog/v1%2Fconfig", line, "line %d" % number)

    def test_it_never_prints_a_token(self):
        """The only `Bearer` line writes a mode-0600 header file, never stdout."""
        bearers = [line for line in self.text.splitlines() if "Bearer" in line]
        self.assertTrue(bearers, "the script never sets an Authorization header")
        for line in bearers:
            self.assertNotIn("echo", line, line)
            self.assertIn(">", line, line)

    def test_it_reads_the_chain_it_guards(self):
        for needle in (
            "workloadIdentity",
            "automountServiceAccountToken",
            "stardelt.io/service-principal",
            "kubernetes~",
        ):
            self.assertIn(needle, self.text)

    def test_it_stops_the_port_forward_and_deletes_nothing(self):
        self.assertIn("port-forward", self.text)
        self.assertIn("kill \"$pf_pid\"", self.text)
        self.assertNotIn("delete serviceaccount", self.text.lower())
        self.assertNotIn("delete serviceprincipal", self.text.lower())
        for line in self.text.splitlines():
            if line.lstrip().startswith("#"):
                continue
            self.assertNotIn(" delete ", line, line)
            self.assertNotRegex(line, r"create token .*delete", line)


class Behaviour(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        for name, text in (("kubectl", KUBECTL), ("curl", CURL)):
            shim = self.dir / name
            shim.write_text(text)
            shim.chmod(0o755)
        self.port = free_port()

    def run_script(self, principals=None, **extra):
        if principals is None:
            principals = [principal("pipeline-demo", "kubernetes~sa-uid-pipeline-demo")]
        env = dict(
            os.environ,
            PATH=f"{self.dir}:{os.environ['PATH']}",
            NS="stardelt",
            PORT=str(self.port),
            TMPDIR=str(self.dir),
            FAKE_PRINCIPALS=json.dumps({"items": principals}),
        )
        env.update(extra)
        return subprocess.run(
            [BASH, str(SCRIPT)], env=env, capture_output=True, text=True
        )

    def test_all_green_passes(self):
        result = self.run_script()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn(
            "✓ pipeline-demo: kubernetes~sa-uid-p… matches the ServiceAccount",
            result.stdout,
        )
        self.assertIn(
            "✓ pipeline-demo: the workload token authenticates (the principal is not provisioned yet)",
            result.stdout,
        )
        self.assertIn("verify-workload-identity: PASS (1 ServicePrincipal(s))", result.stdout)
        self.assertNotIn("FAIL", result.stdout)
        self.assertNotIn("fake-pipeline-demo-token", result.stdout)

    def test_a_200_whoami_is_a_pass(self):
        result = self.run_script(FAKE_WHOAMI_CODE="200")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("✓ pipeline-demo: the workload token signs in", result.stdout)

    def test_a_missing_serviceaccount_is_a_fail(self):
        result = self.run_script(FAKE_NO_SA="pipeline-demo")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(
            "FAIL (pipeline-demo: ServiceAccount sp-pipeline-demo is missing — roll the operator)",
            result.stdout,
        )
        self.assertIn("verify-workload-identity: FAIL", result.stdout)
        self.assertNotIn("PASS", result.stdout)

    def test_an_automounting_serviceaccount_is_a_fail(self):
        """Absent is a FAIL too: the API server may automount it on the next pod."""
        result = self.run_script(FAKE_AUTOMOUNT="true")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn("sp-pipeline-demo automounts its token", result.stdout)

    def test_an_absent_automount_field_is_a_fail(self):
        result = self.run_script(FAKE_AUTOMOUNT="absent")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn("automounts its token (automountServiceAccountToken=null)", result.stdout)

    def test_a_foreign_status_identity_is_a_fail(self):
        result = self.run_script(
            principals=[principal("pipeline-demo", "kubernetes~0000-other-sa")]
        )
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(
            "FAIL (pipeline-demo: status.workloadIdentity=kubernetes~0000-other-sa "
            "does not match the ServiceAccount uid (kubernetes~sa-uid-pipeline-demo)",
            result.stdout,
        )

    def test_a_label_mismatch_is_a_fail(self):
        result = self.run_script(FAKE_SA_LABEL="other")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(
            "FAIL (pipeline-demo: ServiceAccount sp-pipeline-demo is not labelled "
            "stardelt.io/service-principal=pipeline-demo (got \"other\"))",
            result.stdout,
        )

    def test_a_missing_owner_reference_is_a_fail(self):
        result = self.run_script(FAKE_OWNER_NAME="other")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(
            "FAIL (pipeline-demo: ServiceAccount sp-pipeline-demo has no ownerReference "
            "to ServicePrincipal pipeline-demo)",
            result.stdout,
        )

    def test_a_non_controller_owner_reference_is_a_fail(self):
        result = self.run_script(FAKE_OWNER_CONTROLLER="false")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(
            "FAIL (pipeline-demo: ServiceAccount sp-pipeline-demo ownerReference "
            "to ServicePrincipal pipeline-demo is not the controller)",
            result.stdout,
        )

    def test_a_foreign_owner_uid_is_a_fail(self):
        """The ownerReference must name the principal's uid, not the SA's."""
        result = self.run_script(FAKE_OWNER_UID="sa-uid-pipeline-demo")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(
            "FAIL (pipeline-demo: ServiceAccount sp-pipeline-demo is owned by "
            "ServicePrincipal uid \"sa-uid-pipeline-demo\", not uid-pipeline-demo",
            result.stdout,
        )

    def test_an_unset_status_identity_is_a_fail(self):
        result = self.run_script(principals=[principal("pipeline-demo", "")])
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn("status.workloadIdentity=<unset>", result.stdout)

    def test_a_401_from_the_catalog_is_a_fail(self):
        result = self.run_script(FAKE_WHOAMI_CODE="401")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(
            "FAIL (pipeline-demo: the catalog refuses its ServiceAccount token (HTTP 401))",
            result.stdout,
        )

    def test_a_404_without_user_not_found_is_a_fail(self):
        result = self.run_script(FAKE_WHOAMI_BODY='{"error":{"code":"Other"}}')
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn("404 without UserNotFound", result.stdout)

    def test_no_serviceprincipal_is_unverified(self):
        result = self.run_script(principals=[])
        self.assertEqual(3, result.returncode, result.stdout + result.stderr)
        self.assertIn("UNVERIFIED (no ServicePrincipal in stardelt)", result.stdout)

    def test_a_busy_port_is_unverified(self):
        """The negative case the ticket asks for: no port-forward, no pass."""
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", self.port))
            sock.listen(1)
            result = self.run_script()
        self.assertEqual(3, result.returncode, result.stdout + result.stderr)
        self.assertIn("UNVERIFIED (port", result.stdout)
        self.assertNotIn("PASS", result.stdout)

    def test_no_kubectl_is_unverified(self):
        empty = self.dir / "empty"
        empty.mkdir()
        env = dict(os.environ, PATH=str(empty), NS="stardelt", PORT=str(self.port),
                   TMPDIR=str(self.dir))
        result = subprocess.run(
            [BASH, str(SCRIPT)], env=env, capture_output=True, text=True
        )
        self.assertEqual(3, result.returncode, result.stdout + result.stderr)
        self.assertIn("UNVERIFIED (kubectl not found)", result.stdout)


if __name__ == "__main__":
    unittest.main()
