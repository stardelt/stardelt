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


def first_digest(rel):
    """The first `sha256:` in a platform file — what a row without a pattern reads."""
    return re.search(r"sha256:[0-9a-f]{64}", (REPO / rel).read_text()).group(0)


# The operator's copies of the chart-default images (operator#817/#818): same digests, copied here so
# a pin bump needs no test edit. `crates/stardelt-operator/src/kafka.rs` has no platform-side twin;
# its BROKER_IMAGE is built from the broker digest of manifests/kafka.yaml below.
OPERATOR_MIRROR = {
    "crates/stardelt-operator/values/external-secrets.yaml": "helm-values/external-secrets.yaml",
    "crates/stardelt-operator/values/spark-operator.yaml": "helm-values/spark-operator.yaml",
    "crates/stardelt-operator/values/kafka.yaml": "helm-values/strimzi.yaml",
}


def fake_operator(root):
    """A plain directory operator checkout: no `origin/main`, so the script reads its files."""
    op = Path(root)
    for rel, platform in PLATFORM_SIDE.items():
        target = op / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text((REPO / platform).read_text())
    (op / "crates/stardelt-operator/values/jupyterhub.yaml").write_text(
        "singleuser:\n  image:\n    name: ghcr.io/stardelt/jupyter-singleuser\n"
        f'    tag: "dev@{notebook_digest()}"\n'
    )
    for rel, platform in OPERATOR_MIRROR.items():
        target = op / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text((REPO / platform).read_text())
    (op / "crates/stardelt-operator/src/kafka.rs").write_text(
        "pub const BROKER_IMAGE: &str = "
        f'"quay.io/strimzi/kafka:1.3.1-kafka-4.3.2@{first_digest("manifests/kafka.yaml")}";\n'
        "pub const OPERATOR_IMAGE: &str = "
        f'"quay.io/strimzi/operator:1.3.1@{first_digest("helm-values/strimzi.yaml")}";\n'
    )
    return op


class CheckOperatorPins(unittest.TestCase):
    def run_script(self, op):
        return subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=REPO,
            env={"OPERATOR_DIR": str(op), "PATH": "/usr/local/bin:/usr/bin:/bin"},
            capture_output=True,
            text=True,
            timeout=60,
        )

    def test_the_notebook_row_reads_the_notebook_pin(self):
        with tempfile.TemporaryDirectory() as tmp:
            done = self.run_script(fake_operator(tmp))
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn(f"✓ notebook both install paths run {notebook_digest()[:15]}…", done.stdout)

    def test_the_chart_default_rows_read_their_own_digests(self):
        with tempfile.TemporaryDirectory() as tmp:
            done = self.run_script(fake_operator(tmp))
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        for name, rel in (
            ("external-secrets", "helm-values/external-secrets.yaml"),
            ("spark-operator", "helm-values/spark-operator.yaml"),
            ("strimzi", "helm-values/strimzi.yaml"),
            ("kafka-broker", "manifests/kafka.yaml"),
        ):
            self.assertIn(f"✓ {name:<7} both install paths run {first_digest(rel)[:15]}…", done.stdout)
        self.assertIn("check-operator-pins: PASS", done.stdout)
