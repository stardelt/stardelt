"""scripts/verify-pins.sh reads the three helper CronJobs, not only the Deployments (platform#1567).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl` is a shell shim first on PATH that answers `get cronjob <name>` from the
environment and records every call, so no cluster is touched.

Why: platform#1563 pinned two helper images by digest in three manifests, and at 12:00Z the live
CronJobs still ran the bare tags while `make verify-pins` answered PASS. Nothing read those
manifests, so a pin that was never applied stayed invisible to every check.
"""

import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-pins.sh"

AWS_CLI = "amazon/aws-cli:2.17.0"
AWS_CLI_PINNED = (
    "amazon/aws-cli:2.17.0"
    "@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73"
)
PYTHON_PINNED = (
    "python:3.13.16-slim-bookworm"
    "@sha256:73aa5faee130c60da030e105ec971ccf57473fbd22ce73bd6fc9ba99a40b838d"
)

ROWS = "access-sync,demo-reset,lakehouse-backup"

SHIM = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl: every call is appended to FAKE_LOG and answered from FAKE_IMAGES_<name>.
echo "$*" >>"$FAKE_LOG"
args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n | --namespace) shift 2 ;;
    *) args+=("$1"); shift ;;
  esac
done
if [ "${args[0]-}" = get ] && [ "${args[1]-}" = cronjob ]; then
  name="$(printf '%s' "${args[2]-}" | tr '-' '_' | tr '[:lower:]' '[:upper:]')"
  var="FAKE_IMAGES_${name}"
  value="${!var-}"
  if [ -z "$value" ]; then
    exit 1
  fi
  printf '%s\\n' "$value"
  exit 0
fi
exit 0
"""


class VerifyPins(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
        self.log = self.dir / "kubectl.log"
        self.log.write_text("")

    def run_script(self, images):
        """images maps CronJob name -> list of image strings the cluster reports."""
        env = dict(
            os.environ,
            PATH=f"{self.dir}:{os.environ['PATH']}",
            NAMESPACE="stardelt",
            ROWS=ROWS,
            FAKE_LOG=str(self.log),
        )
        for name, values in images.items():
            env["FAKE_IMAGES_" + name.upper().replace("-", "_")] = "\n".join(values)
        return subprocess.run(
            ["bash", str(SCRIPT)], cwd=REPO, env=env, capture_output=True, text=True, timeout=60
        )

    def calls(self):
        return [line for line in self.log.read_text().splitlines() if line]

    def assert_read_only(self):
        self.assertTrue(self.calls(), "the shim recorded nothing")
        for call in self.calls():
            verb = call.split()
            for mutating in ("apply", "set", "patch", "delete", "create", "edit", "scale"):
                self.assertNotIn(mutating, verb, call)

    def test_pinned_images_pass(self):
        done = self.run_script(
            {
                "access-sync": [PYTHON_PINNED, PYTHON_PINNED],
                "demo-reset": [PYTHON_PINNED],
                "lakehouse-backup": [AWS_CLI_PINNED],
            }
        )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("PASS", done.stdout)
        self.assertIn(
            "✓ access-sync        2 image(s) match manifests/access-sync.yaml", done.stdout
        )
        self.assertIn(
            "✓ demo-reset         1 image(s) match manifests/demo-reset.yaml", done.stdout
        )
        self.assertIn(
            "✓ lakehouse-backup   1 image(s) match manifests/object-store-backup.yaml",
            done.stdout,
        )
        self.assertNotIn("✗", done.stdout)
        self.assert_read_only()

    def test_a_manifest_pin_that_was_never_applied_fails(self):
        done = self.run_script(
            {
                "access-sync": [PYTHON_PINNED, PYTHON_PINNED],
                "demo-reset": [PYTHON_PINNED],
                # The pre-#1563 bare tag: the manifest was pinned, the CronJob was not applied.
                "lakehouse-backup": [AWS_CLI],
            }
        )
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("FAIL", done.stdout + done.stderr)
        self.assertIn(AWS_CLI, done.stdout)
        self.assertIn("kubectl apply -f manifests/object-store-backup.yaml", done.stdout)
        self.assert_read_only()


if __name__ == "__main__":
    unittest.main()
