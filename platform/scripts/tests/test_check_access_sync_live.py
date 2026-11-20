"""Unit tests for scripts/check_access_sync_live.py (stardelt/stardelt#697).

Standard library only, like the script: `python3 -m unittest discover -s scripts/tests`
runs them in CI. `verdict()` is pure, so no cluster is touched.
"""

import importlib.util
import re
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "check_access_sync_live.py"
_spec = importlib.util.spec_from_file_location("check_access_sync_live", SCRIPT)
check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check)


class CheckAccessSyncLiveTest(unittest.TestCase):
    def test_equal_scripts_pass(self):
        code, text = check.verdict(b"x\n", b"x\n")
        self.assertEqual(code, 0)
        self.assertTrue(text.startswith("✓ access-sync script:"), text)

    def test_a_changed_script_is_a_failure(self):
        code, text = check.verdict(b"x\n", b"y\n")
        self.assertEqual(code, 1)
        self.assertTrue(text.endswith("— run make access-sync"), text)

    def test_the_digest_is_short(self):
        _, text = check.verdict(b"x\n", b"y\n")
        self.assertIn("sha256:", text)
        self.assertIsNone(re.search(r"[0-9a-f]{64}", text), text)


if __name__ == "__main__":
    unittest.main()
