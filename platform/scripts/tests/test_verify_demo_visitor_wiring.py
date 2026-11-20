"""Wiring tests for `make verify-demo-visitor` (stardelt/stardelt#527).

The script itself is measured on the lab; these two tests are the part CI can prove on a
machine with no cluster and no network: the Makefile target exists, and the script it runs is
executable and parses. Standard library only, like the other tests here.
"""
import os
import subprocess
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]
MAKEFILE = PLATFORM / "Makefile"
SCRIPT = PLATFORM / "scripts" / "verify-demo-visitor.sh"


class VerifyDemoVisitorWiring(unittest.TestCase):
    def test_the_make_target_exists_and_runs_the_script(self):
        text = MAKEFILE.read_text()
        self.assertIn("\nverify-demo-visitor: _context", text)
        self.assertIn("bash scripts/verify-demo-visitor.sh", text)

    def test_the_script_is_executable_and_parses(self):
        self.assertTrue(SCRIPT.is_file(), f"{SCRIPT} does not exist")
        self.assertTrue(os.access(SCRIPT, os.X_OK), f"{SCRIPT} is not executable")
        result = subprocess.run(
            ["bash", "-n", str(SCRIPT)], capture_output=True, text=True, timeout=30
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
