"""Unit tests for scripts/verify_secret_annotations.py (stardelt/stardelt#544).

Standard library only, like the neighbours: `kubectl` is a shell shim first on PATH, so no
cluster is touched and no credential exists anywhere. The live half is `make
verify-secret-annotations` on a cluster.
"""
import json
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify_secret_annotations.py"
KEY = "kubectl.kubernetes.io/last-applied-configuration"

SHIM = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl: `get secrets -o json` answers FAKE_SECRETS (a file), or fails with FAKE_FAIL=1.
if [ "${FAKE_FAIL:-0}" = 1 ]; then
  echo "error: the server is unreachable" >&2
  exit 1
fi
cat "$FAKE_SECRETS"
"""


def secret(name, annotation=None):
    metadata = {"name": name}
    if annotation is not None:
        metadata["annotations"] = {KEY: annotation}
    return {"metadata": metadata}


class VerifySecretAnnotationsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.bin = self.dir / "bin"
        self.bin.mkdir()
        shim = self.bin / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        self.secrets = self.dir / "secrets.json"

    def run_script(self, items, fail=False):
        self.secrets.write_text(json.dumps({"items": items}))
        env = dict(os.environ)
        env["PATH"] = f"{self.bin}:{env['PATH']}"
        env["NAMESPACE"] = "stardelt"
        env["FAKE_SECRETS"] = str(self.secrets)
        env["FAKE_FAIL"] = "1" if fail else "0"
        return subprocess.run(
            ["python3", str(SCRIPT)], capture_output=True, text=True, env=env
        )

    def test_clean_secret_passes(self):
        done = self.run_script([secret("sp-clean")])
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertEqual(
            done.stdout.strip(),
            "verify-secret-annotations: PASS (1 Secret(s), no credential in the annotation)",
        )

    def test_credential_in_annotation_fails_with_the_fix(self):
        done = self.run_script(
            [
                secret("sp-clean"),
                secret("sp-pipeline-demo", '{"data":{"client-secret":"c2VjcmV0"}}'),
            ]
        )
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("sp-pipeline-demo carries a credential in", done.stdout)
        self.assertIn(
            f"kubectl -n stardelt annotate secret sp-pipeline-demo {KEY}-", done.stdout
        )
        self.assertNotIn("sp-clean carries", done.stdout)
        self.assertEqual(
            done.stdout.strip().splitlines()[-1],
            "verify-secret-annotations: FAIL (1 of 2 Secret(s))",
        )

    def test_kubectl_failure_exits_two(self):
        done = self.run_script([], fail=True)
        self.assertEqual(done.returncode, 2, done.stdout + done.stderr)
        self.assertIn("kubectl cannot read the Secrets", done.stdout)


if __name__ == "__main__":
    unittest.main()
