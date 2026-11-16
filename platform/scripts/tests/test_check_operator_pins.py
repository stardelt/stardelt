"""scripts/check-operator-pins.sh reads the digest each row means (platform#1634).

The notebook row used to take the first `sha256:` of helm-values/jupyterhub.yaml — the hub's pin
since #1608 — and compared it with the operator's notebook image, so it was red on main for a pin
that never moved. A fake operator checkout (a plain directory: no `origin/main`, so the script
falls back to it) proves the row reads the `tag: "dev@sha256:` line.
"""

import re
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "check-operator-pins.sh"

# The operator's file as operator main has it: only the notebook image (values/jupyterhub.yaml:149).
OPERATOR_FILES = {
    "crates/stardelt-operator/src/provision.rs": None,  # copied from the platform row below
    "crates/stardelt-operator/values/airflow.yaml": None,
    "crates/stardelt-operator/values/spark.yaml": None,
    "crates/stardelt-operator/values/superset.yaml": None,
    "crates/stardelt-operator/values/jupyterhub.yaml": None,
}
PLATFORM_SIDE = {
    "crates/stardelt-operator/src/provision.rs": "manifests/nova-deployment.yaml",
    "crates/stardelt-operator/values/airflow.yaml": "helm-values/airflow.yaml",
    "crates/stardelt-operator/values/spark.yaml": "manifests/spark.yaml",
    "crates/stardelt-operator/values/superset.yaml": "helm-values/superset.yaml",
}


def notebook_digest():
    text = (REPO / "helm-values" / "jupyterhub.yaml").read_text()
    line = next(l for l in text.splitlines() if re.search(r'^\s+tag: "dev@sha256:', l))
    return re.search(r"sha256:[0-9a-f]{64}", line).group(0)


class CheckOperatorPins(unittest.TestCase):
    def test_the_notebook_row_reads_the_notebook_pin(self):
        with tempfile.TemporaryDirectory() as tmp:
            op = Path(tmp)
            for rel, platform in PLATFORM_SIDE.items():
                target = op / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text((REPO / platform).read_text())
            (op / "crates/stardelt-operator/values/jupyterhub.yaml").write_text(
                "singleuser:\n  image:\n    name: ghcr.io/stardelt/jupyter-singleuser\n"
                f'    tag: "dev@{notebook_digest()}"\n'
            )
            done = subprocess.run(
                ["bash", str(SCRIPT)],
                cwd=REPO,
                env={"OPERATOR_DIR": str(op), "PATH": "/usr/local/bin:/usr/bin:/bin"},
                capture_output=True,
                text=True,
                timeout=60,
            )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn(f"✓ notebook both install paths run {notebook_digest()[:15]}…", done.stdout)
