"""Tests for the CNPG age signal of scripts/verify-backups-all.sh (platform#1500).

A barman-cloud-plugin cluster leaves `status.lastSuccessfulBackup` at the plugin-migration
date and records each backup as a Backup object plus the LastBackupSucceeded condition.
`kubectl` is replaced by a shell shim first on PATH that serves canned JSON files.
"""
import json
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-backups-all.sh"

SHIM = """#!/usr/bin/env bash
set -u
shift 2
[ "${1-}" = get ] || exit 1
case "${2-}" in
  pvc) cat "$FAKE_DIR/pvc.json" ;;
  clusters.postgresql.cnpg.io) cat "$FAKE_DIR/clusters.json" ;;
  cronjob) cat "$FAKE_DIR/cronjob.json" ;;
  scheduledbackup) cat "$FAKE_DIR/scheduledbackup.json" ;;
  objectstore) cat "$FAKE_DIR/objectstore.json" ;;
  backups.postgresql.cnpg.io) cat "$FAKE_DIR/backups.json" ;;
  *) exit 1 ;;
esac
"""


def ts(hours_ago):
    return (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")


class CnpgAge(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
        (self.dir / "pvc.json").write_text(json.dumps({"items": [{"metadata": {"name": "keycloak-pg-1"}}]}))
        (self.dir / "cronjob.json").write_text(json.dumps({"items": []}))
        (self.dir / "scheduledbackup.json").write_text(
            json.dumps({"items": [{"metadata": {"name": "keycloak-pg-daily"}}]})
        )
        (self.dir / "objectstore.json").write_text(json.dumps({"items": [{
            "metadata": {"name": "keycloak-pg-backups"},
            "spec": {"configuration": {"destinationPath": "s3://backups/keycloak-pg"}},
        }]}))

    def cluster(self, last_successful_backup, condition_time):
        return {"items": [{
            "metadata": {"name": "keycloak-pg"},
            "spec": {"plugins": [{"enabled": True, "isWALArchiver": True,
                                  "name": "barman-cloud.cloudnative-pg.io",
                                  "parameters": {"barmanObjectName": "keycloak-pg-backups"}}]},
            "status": {"lastSuccessfulBackup": last_successful_backup,
                       "conditions": [{"type": "LastBackupSucceeded", "status": "True",
                                       "lastTransitionTime": condition_time}]},
        }]}

    def backups(self, started_at):
        items = [] if started_at is None else [{
            "metadata": {"name": "keycloak-pg-daily-20261115024500"},
            "spec": {"cluster": {"name": "keycloak-pg"}},
            "status": {"phase": "completed", "startedAt": started_at},
        }]
        return {"items": items}

    def run_script(self, cluster, backups):
        (self.dir / "clusters.json").write_text(json.dumps(cluster))
        (self.dir / "backups.json").write_text(json.dumps(backups))
        env = dict(os.environ, PATH=f"{self.dir}:{os.environ['PATH']}", NAMESPACE="stardelt",
                   FAKE_DIR=str(self.dir))
        return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=60)

    def test_a_fresh_plugin_backup_passes_even_when_the_cluster_field_is_the_migration_date(self):
        done = self.run_script(self.cluster("2026-11-13T02:45:18Z", ts(19)), self.backups(ts(19)))
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("PASS", done.stdout)

    def test_a_stale_plugin_backup_fails_even_when_the_cluster_field_is_fresh(self):
        done = self.run_script(self.cluster(ts(1), ts(1)), self.backups(ts(72)))
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("older than MAX_AGE_HOURS=36", done.stderr)

    def test_no_completed_backup_falls_back_to_the_condition_then_fails_when_that_is_old(self):
        fresh = self.run_script(self.cluster("", ts(5)), self.backups(None))
        self.assertEqual(fresh.returncode, 0, fresh.stdout + fresh.stderr)
        old = self.run_script(self.cluster("", ts(60)), self.backups(None))
        self.assertEqual(old.returncode, 1, old.stdout + old.stderr)
