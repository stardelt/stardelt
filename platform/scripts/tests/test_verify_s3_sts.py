"""Unit tests for scripts/verify-s3-sts.sh (stardelt#282).

The gap the tests close: the script used to wait only for `Succeeded`, so a probe pod that
reached `Failed` — the gateway is up but does not speak STS — was reported as UNVERIFIED
instead of FAIL. A terminal phase is a judged run; only a pod that reached neither phase is
UNVERIFIED.

Standard library only, like the other tests here: `python3 -m unittest discover -s
scripts/tests -v` runs them in CI. `kubectl` is a shell shim first on PATH that answers the
Secret probe, the apply, the phase read and the logs from environment variables, so no
cluster is touched.
"""
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-s3-sts.sh"

SHIM = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl: the Secret probe and the apply succeed, the phase read prints FAKE_PHASE,
# the logs read prints FAKE_LOGS and the delete succeeds. Nothing else is ever called.
case " $* " in
  *" get pod "*) printf '%s' "${FAKE_PHASE:-Succeeded}" ;;
  *" logs "*) printf '%s\\n' "${FAKE_LOGS:-}" ;;
  *) : ;;
esac
exit 0
"""

ALL_PASS = "pass identity-put\npass session-in\npass session-out"


class VerifyS3Sts(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)

    def run_script(self, **extra):
        env = dict(
            os.environ,
            PATH=f"{self.dir}:{os.environ['PATH']}",
            NS="stardelt",
            TMPDIR=str(self.dir),
        )
        env.pop("FAKE_PHASE", None)
        env.pop("FAKE_LOGS", None)
        env.update(extra)
        return subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=str(REPO),
            env=env,
            capture_output=True,
            text=True,
        )

    def test_a_failed_probe_pod_is_a_fail(self):
        # The AWS CLI could not assume the role, so the pod printed one fail line and exited 1.
        result = self.run_script(FAKE_PHASE="Failed", FAKE_LOGS="pass identity-put\nfail assume")
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn("FAIL (1 of 3)", result.stdout)
        self.assertNotIn("UNVERIFIED", result.stdout)

    def test_a_pending_pod_is_unverified(self):
        result = self.run_script(FAKE_PHASE="Pending", S3_STS_WAIT_SECONDS="0")
        self.assertEqual(3, result.returncode, result.stdout + result.stderr)
        self.assertIn("UNVERIFIED", result.stdout)

    def test_a_succeeded_pod_with_three_passes_is_a_pass(self):
        result = self.run_script(FAKE_PHASE="Succeeded", FAKE_LOGS=ALL_PASS)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("PASS (3 of 3)", result.stdout)


if __name__ == "__main__":
    unittest.main()
