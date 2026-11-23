"""Tests for the offsite-backup CronJob's pod security context (platform#1369).

Standard library + PyYAML: `python3 -m unittest scripts.tests.test_offsite_backup_pod_security -v`.

The rclone image has no non-root user, so the pod has to choose a non-root UID itself to
satisfy Pod Security `restricted` (the namespace enforces it after platform#1362). The image
has no writable home for that UID, so the container needs HOME and XDG_CACHE_HOME: rclone
resolves its config and cache under them.
"""
import unittest
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
MANIFEST = REPO / "manifests" / "offsite-backup.yaml"

POD_SECURITY_CONTEXT = {
    "runAsNonRoot": True,
    "runAsUser": 65534,
    "runAsGroup": 65534,
    "seccompProfile": {"type": "RuntimeDefault"},
}
POD_CONTAINER_SECURITY_CONTEXT = {
    "allowPrivilegeEscalation": False,
    "capabilities": {"drop": ["ALL"]},
}


def load_cronjob():
    with MANIFEST.open() as handle:
        for document in yaml.safe_load_all(handle):
            if document and document.get("kind") == "CronJob":
                if document["metadata"]["name"] == "offsite-backup":
                    return document
    raise AssertionError("CronJob offsite-backup not found in the manifest")


def pod_spec():
    return load_cronjob()["spec"]["jobTemplate"]["spec"]["template"]["spec"]


def rclone_container():
    for container in pod_spec()["containers"]:
        if container["name"] == "rclone":
            return container
    raise AssertionError("container rclone not found in the CronJob's pod template")


class TestOffsiteBackupPodSecurity(unittest.TestCase):
    def test_the_pod_declares_run_as_non_root(self):
        self.assertEqual(pod_spec()["securityContext"], POD_SECURITY_CONTEXT)

    def test_the_container_declares_the_restricted_fields(self):
        self.assertEqual(
            rclone_container()["securityContext"], POD_CONTAINER_SECURITY_CONTEXT
        )

    def test_the_container_has_a_writable_home(self):
        env = rclone_container()["env"]
        self.assertIn({"name": "HOME", "value": "/tmp"}, env)
        self.assertIn({"name": "XDG_CACHE_HOME", "value": "/tmp/.cache"}, env)


if __name__ == "__main__":
    unittest.main()
