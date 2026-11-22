"""Unit tests for scripts/check_helm_release.py (#1126)."""

import json
import pathlib
import tempfile
import unittest

from scripts.check_helm_release import main, pending

REPO = pathlib.Path(__file__).resolve().parents[2]


def status(state, revision=157):
    return json.dumps({"name": "stardelt-operator", "version": revision, "info": {"status": state}})


class PendingReleaseTest(unittest.TestCase):
    def test_a_pending_upgrade_is_stuck_and_names_its_revision(self):
        self.assertEqual(pending(status("pending-upgrade")), ("pending-upgrade", 157))

    def test_every_pending_state_is_stuck(self):
        for state in ("pending-install", "pending-rollback", "pending-uninstall"):
            self.assertIsNotNone(pending(status(state)), state)

    def test_a_deployed_release_is_not_stuck(self):
        self.assertIsNone(pending(status("deployed")))

    def test_garbage_is_not_a_stuck_release(self):
        self.assertIsNone(pending("not json"))

    def test_main_fails_with_the_recovery_line(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            handle.write(status("pending-upgrade"))
            name = handle.name
        self.assertEqual(main(["check_helm_release.py", name]), 1)

    def test_pin_image_runs_the_preflight(self):
        script = (REPO / "scripts" / "pin-image.sh").read_text(encoding="utf-8")
        self.assertIn("check_helm_release.py", script)
