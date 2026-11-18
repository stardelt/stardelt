"""verify-crds skips when there is no operator checkout (platform#1717).

`make step-verify` ends every upgrade and runs `verify-crds`; a one-repository checkout (the
quickstart clones only stardelt-platform) has no ../stardelt-operator, and that is a skip, not a
failed upgrade. A checkout without chart/crds still fails.
"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-crds.sh"


class VerifyCrdsSkip(unittest.TestCase):
    def test_a_missing_checkout_skips_without_kubectl(self):
        env = dict(os.environ, OPERATOR_DIR="/nonexistent-stardelt-operator")
        env.pop("KUBECONFIG", None)
        done = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn(
            "verify-crds: SKIP (no operator checkout at /nonexistent-stardelt-operator; set OPERATOR_DIR)",
            done.stdout,
        )

    def test_an_empty_checkout_still_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = dict(os.environ, OPERATOR_DIR=tmp)
            done = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)
        self.assertEqual(done.returncode, 1)
        self.assertIn("no operator CRDs at", done.stderr)


if __name__ == "__main__":
    unittest.main()
