"""Tests for scripts/roll-cnpg-image.sh (platform#1619).

Standard library only, like the other tests here. `kubectl` is replaced by a shell shim that is
first on PATH: it answers the four reads the script makes and flips the answer after the force
delete, so the drift path is exercised without a cluster.
"""

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "roll-cnpg-image.sh"

WANT = "ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie@sha256:" + "0" * 64
LIVE = "ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie"

SHIM = """#!/usr/bin/env bash
set -u
echo "$*" >> "$FAKE_LOG"
case "$*" in
  *"get clusters.postgresql.cnpg.io"*"jsonpath={.spec.imageName}"*)
    [ -n "${FAKE_MISSING:-}" ] && exit 0
    printf '%s' "$FAKE_WANT"; exit 0 ;;
  *"get clusters.postgresql.cnpg.io"*"status.conditions"*)
    [ -e "$FAKE_ROLLED" ] && printf 'True' || printf 'False'; exit 0 ;;
  *"get pods -l"*)
    printf '%s-1' "$FAKE_CLUSTER"; exit 0 ;;
  *"get pod "*"jsonpath={.spec.containers"*)
    if [ -e "$FAKE_ROLLED" ]; then printf '%s' "$FAKE_WANT"; else printf '%s' "$FAKE_LIVE"; fi
    exit 0 ;;
  *"delete pod "*"--grace-period=0 --force"*)
    touch "$FAKE_ROLLED"; exit 0 ;;
esac
echo "unexpected kubectl: $*" >&2
exit 1
"""


class RollCnpgImageTest(unittest.TestCase):
    def run_script(self, *, live, want=WANT, missing=False, cluster="keycloak-pg"):
        temp = Path(tempfile.mkdtemp())
        shim = temp / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        log = temp / "calls.log"
        log.write_text("")
        env = {
            **os.environ,
            "PATH": f"{temp}{os.pathsep}{os.environ.get('PATH', '')}",
            "FAKE_LOG": str(log),
            "FAKE_ROLLED": str(temp / "rolled"),
            "FAKE_WANT": want,
            "FAKE_LIVE": live,
            "FAKE_CLUSTER": cluster,
            "CLUSTERS": cluster,
            "NAMESPACE": "stardelt",
            "POLL_SECONDS": "0",
            "TIMEOUT": "10",
        }
        if missing:
            env["FAKE_MISSING"] = "1"
        done = subprocess.run(
            ["bash", str(SCRIPT)], capture_output=True, text=True, env=env, cwd=REPO
        )
        return done, log.read_text()

    def test_a_healthy_cluster_is_left_alone(self):
        done, calls = self.run_script(live=WANT)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("✓", done.stdout)
        self.assertIn("PASS", done.stdout)
        self.assertNotIn("delete pod", calls)

    def test_a_drifted_primary_is_force_deleted_and_converges(self):
        done, calls = self.run_script(live=LIVE)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertEqual(calls.count("delete pod keycloak-pg-1 --grace-period=0 --force"), 1, calls)
        self.assertIn("Ready", done.stdout)

    def test_a_missing_cluster_is_unverified_not_a_failure(self):
        done, _ = self.run_script(live=LIVE, missing=True)
        self.assertEqual(done.returncode, 3, done.stdout + done.stderr)
        self.assertIn("no Cluster", done.stdout)
        self.assertIn("UNVERIFIED", done.stderr)


if __name__ == "__main__":
    unittest.main()
