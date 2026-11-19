"""Unit tests for scripts/verify-spark-demo-security.sh (stardelt/stardelt#311).

The gap the check closes: `verify-spark-demo-query` proves the demo sandbox runs a job and
`verify-spark-demo-sandbox` judges what it contains; neither measures what a visitor's own code
can do inside a session (stardelt/stardelt#201's security-test list).

Standard library only, like the other tests here: `python3 -m unittest discover -s
scripts/tests -v` runs them in CI. `kubectl` and `python3` are bash shims first on PATH, so no
cluster and no network are touched: the sandbox exists in every case, the pod's phase and its
log come from FAKE_PHASE and FAKE_LOG, and the judged lines are the ones the test writes. That
is the point of the four cases — the verdict must follow the log's five lines, not the pod
phase alone (the stardelt/stardelt#282 lesson).
"""
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-spark-demo-security.sh"

KUBECTL = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl: the sandbox exists, the identity ConfigMap names an issuer, the S3 Secret names
# a bucket, the client pod is created and its phase is FAKE_PHASE; `logs` prints FAKE_LOG.
case " $* " in
  *" get deployment spark-demo "*) exit 0 ;;
  *" get configmap stardelt-identity "*) printf '%s' 'https://id.test/realms/stardelt'; exit 0 ;;
  *" get secret stardelt-s3-creds "*)
    printf '%s' "lakehouse" | base64 | tr -d '\\n'; exit 0 ;;
  *" get pod "*" -o jsonpath="*) printf '%s' "${FAKE_PHASE:-Unknown}"; exit 0 ;;
  *" get pod "*" -o json "*) printf '{"status":{"phase":"%s"}}' "${FAKE_PHASE:-Unknown}"; exit 0 ;;
  *" wait "*) [ "${FAKE_PHASE:-}" = "Succeeded" ] && exit 0 || exit 1 ;;
  *" logs "*) cat "${FAKE_LOG:?}"; exit 0 ;;
esac
exit 0
"""

PYTHON = """#!/usr/bin/env bash
set -euo pipefail
# The token minting is the only python3 call this script makes that would touch the network;
# the rest (the phase decode) is passed through to the real interpreter.
if [ "${1:-}" = "scripts/mint-user-token.py" ]; then
  printf '%s' "fake-token" > "${TOKEN_FILE:?}"
  chmod 600 "${TOKEN_FILE}"
  echo "minted: segments=3 aud=openbao,nova,spark-connect,lakekeeper,trino azp=stardelt-dev preferred_username=demo"
  exit 0
fi
exec "${REAL_PYTHON3:?}" "$@"
"""

REFUSED_LOG = """\
catalog=granted:loaded
catalog=refused: (org.apache.iceberg.exceptions.RESTException) Error occurred while processing GET request
vended=ok 1 row(s)
files=refused: (java.nio.file.AccessDeniedException) s3a://lakehouse/gold: No AWS Credentials provided
token=absent
api=refused: ConnectionRefusedError(111, 'Connection refused')
udf=ok (refused: OSError(101, 'Network is unreachable'))
"""


class SparkDemoSecurity(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        bindir = self.dir / "bin"
        bindir.mkdir()
        for name, body in (("kubectl", KUBECTL), ("python3", PYTHON)):
            shim = bindir / name
            shim.write_text(body)
            shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
        self.bindir = bindir
        self.log = self.dir / "log.txt"

    def run_script(self, log, phase="Succeeded", **extra):
        self.log.write_text(log)
        env = dict(
            os.environ,
            PATH=f"{self.bindir}:{os.environ['PATH']}",
            NAMESPACE="test",
            TMPDIR=str(self.dir),
            FAKE_PHASE=phase,
            FAKE_LOG=str(self.log),
            REAL_PYTHON3=shutil.which("python3") or "/usr/bin/python3",
        )
        env.update(extra)
        return subprocess.run(
            ["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=60
        )

    def test_the_five_probes_are_a_pass(self):
        result = self.run_script(REFUSED_LOG)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("PASS (5 of 5)", result.stdout)
        self.assertIn("retail is refused", result.stdout)

    def test_a_failed_probe_is_a_fail(self):
        log = REFUSED_LOG.replace("files=refused: (java.nio.file.AccessDeniedException) "
                                  "s3a://lakehouse/gold: No AWS Credentials provided",
                                  "files=ADMITTED 3")
        result = self.run_script(log)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("FAIL", result.stdout)
        self.assertIn("files=ADMITTED 3", result.stdout)

    def test_a_missing_vended_credential_is_a_fail(self):
        log = REFUSED_LOG.replace(
            "vended=ok 1 row(s)",
            "vended=refused: (org.apache.iceberg.exceptions.ForbiddenException) "
            "Forbidden: s3.access-key-id is not set",
        )
        result = self.run_script(log)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("FAIL", result.stdout)
        self.assertIn("(make demo-sts YES=1)", result.stdout)

    def test_a_pending_pod_is_unverified(self):
        result = self.run_script(REFUSED_LOG, phase="Pending")
        self.assertEqual(result.returncode, 3, result.stdout + result.stderr)
        self.assertIn("UNVERIFIED", result.stdout)


if __name__ == "__main__":
    unittest.main()
