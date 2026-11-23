"""make verify-wap tells a lab with no room from a WAP defect (#1185).

The gate row failed at 14:26Z only because the build pod never scheduled
(`Insufficient cpu`), and passed on the same commit at 15:09Z. These tests keep the verdict
honest: the check reads the scheduler's FailedScheduling event and answers UNVERIFIED.

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests`
runs them; no cluster and no credential are needed.
"""

import re
import shutil
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-wap.sh"
BASH = shutil.which("bash") or "/bin/bash"


class VerifyWapUnscheduled(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = SCRIPT.read_text()

    def test_it_parses_and_is_executable(self):
        done = subprocess.run([BASH, "-n", str(SCRIPT)], capture_output=True, text=True)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertTrue(SCRIPT.stat().st_mode & 0o111, "the script is not executable")

    def test_it_reads_the_failed_scheduling_event(self):
        self.assertIn("--field-selector reason=FailedScheduling", self.text)
        self.assertIn('--arg prefix "^$1-"', self.text)
        self.assertIn(".eventTime // .lastTimestamp // .firstTimestamp", self.text)

    def test_it_answers_unverified_not_fail(self):
        self.assertIn(
            "verify-wap: UNVERIFIED (the $3 pod never scheduled on the lab",
            self.text,
        )

    def test_both_wap_runs_ask_the_question(self):
        calls = re.findall(r'^no_room "\$DAG" "\$RUN" build$', self.text, re.M)
        self.assertEqual(len(calls), 2, "run 2 and run 3 each call no_room")

    def test_the_metadata_read_retries_through_a_fresh_forward(self):
        # #1383: the local forward dies under load; that is not a refusal by the platform.
        self.assertIn(
            "forward_lk || { echo \"  the catalog forward could not be re-established\"",
            self.text,
        )

    def test_a_failed_read_is_unverified_not_a_refusal(self):
        # A table that was never read is UNVERIFIED, not a WAP defect (measured 12:09Z gate).
        self.assertIn("verify-wap: UNVERIFIED (the metadata of", self.text)
        statuses = re.findall(r'\|\| read[23]=\$\?', self.text)
        self.assertEqual(len(statuses), 2, "run 2 and run 3 each keep the read status")

    def test_the_refusal_still_checks_the_branch_and_the_ttl(self):
        # The refusal row is unchanged: one audit branch, kept for the 7-day TTL.
        self.assertIn('audit_count="$(printf', self.text)
        self.assertIn('[ "$audit_count" = "1" ]', self.text)
        self.assertIn('audit_ttl="$(jq -r --arg b "$audit3"', self.text)
        self.assertIn('[ "$audit_ttl" = "604800000" ]', self.text)


if __name__ == "__main__":
    unittest.main()
