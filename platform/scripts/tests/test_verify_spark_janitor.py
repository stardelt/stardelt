"""The three answers of the judge in scripts/verify-spark-janitor.sh (stardelt#249).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. The script is run in judge-only mode (`JANITOR_VERIFY_LOG`), so no cluster and no
kubectl are touched: each fixture is one janitor log, and the test asserts the verdict the script
prints for it.
"""

import os
import subprocess
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "verify-spark-janitor.sh"
FIXTURES = REPO_ROOT / "scripts" / "tests" / "fixtures"


def judge(fixture):
    return subprocess.run(
        ["bash", "scripts/verify-spark-janitor.sh"],
        cwd=REPO_ROOT,
        env={**os.environ, "JANITOR_VERIFY_LOG": str(FIXTURES / fixture)},
        capture_output=True,
        text=True,
        timeout=60,
    )


class JudgeTest(unittest.TestCase):
    def test_keep_log_passes(self):
        done = judge("janitor-keep.log")
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("✓ live driver", done.stdout)
        self.assertIn("verify-spark-janitor: PASS (3 of 3)", done.stdout)

    def test_would_delete_log_fails(self):
        done = judge("janitor-would-delete.log")
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("would delete spark-exec-0f1e2d3c4b5a6978-conf-map 3600s", done.stdout)
        self.assertIn("verify-spark-janitor: FAIL (2 of 3)", done.stdout)

    def test_empty_log_fails(self):
        done = judge("janitor-empty.log")
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("✗ map present", done.stdout)
        self.assertIn("verify-spark-janitor: FAIL (2 of 3)", done.stdout)


if __name__ == "__main__":
    unittest.main()
