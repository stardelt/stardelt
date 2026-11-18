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
# platform#1640's set rows need every digest of the file, so `fake_operator` overwrites this entry
# with the platform file below; the notebook row's pattern is what still picks the right line.
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


# The set rows (platform#1640) read every digest of these files, so the fake checkout needs each one;
# they are the operator's copies of the same values, so the platform file is what it holds.
SHARED_FILES = {
    "crates/stardelt-operator/values/airflow.yaml": "helm-values/airflow.yaml",
    "crates/stardelt-operator/values/barman-cloud.yaml": "helm-values/barman-cloud.yaml",
    "crates/stardelt-operator/values/cnpg.yaml": "helm-values/cnpg.yaml",
    "crates/stardelt-operator/values/external-secrets.yaml": "helm-values/external-secrets.yaml",
    "crates/stardelt-operator/values/jupyterhub.yaml": "helm-values/jupyterhub.yaml",
    "crates/stardelt-operator/values/kafka.yaml": "helm-values/strimzi.yaml",
    "crates/stardelt-operator/values/openbao.yaml": "helm-values/openbao.yaml",
    "crates/stardelt-operator/values/openfga.yaml": "helm-values/openfga.yaml",
    "crates/stardelt-operator/values/seaweedfs.yaml": "helm-values/seaweedfs.yaml",
    "crates/stardelt-operator/values/spark-operator.yaml": "helm-values/spark-operator.yaml",
    "crates/stardelt-operator/values/superset.yaml": "helm-values/superset.yaml",
    "crates/stardelt-operator/values/trino.yaml": "helm-values/trino.yaml",
    "crates/stardelt-operator/values/trino-identity.yaml": "helm-values/trino-identity.yaml",
}

# The operator's lakekeeper values run `authz.backend: allowall`, so only the catalog is pinned
# there; the platform file's OPA bridge and IdP digests are not part of that row.
LAKEKEEPER_TAG = re.compile(r'tag: "?(v0\.15\.0@sha256:[0-9a-f]{64})"?', re.M)


def fake_operator(root):
    """A plain directory operator checkout: no `origin/main`, so the script reads its files."""
    op = Path(root)
    for rel, platform in PLATFORM_SIDE.items():
        target = op / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text((REPO / platform).read_text())
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
    for rel, platform in SHARED_FILES.items():
        target = op / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text((REPO / platform).read_text())
    lakekeeper = op / "crates/stardelt-operator/values/lakekeeper.yaml"
    lakekeeper.write_text(
        "catalog:\n  image:\n"
        f'    tag: "{LAKEKEEPER_TAG.search((REPO / "helm-values/lakekeeper.yaml").read_text()).group(1)}"\n'
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

    def test_the_run_names_the_operator_revision_it_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            op = fake_operator(tmp)
            subprocess.run(["git", "init", "-q", str(op)], check=True)
            subprocess.run(["git", "-C", str(op), "add", "-A"], check=True)
            subprocess.run(["git", "-C", str(op), "-c", "user.name=t", "-c", "user.email=t@example.com",
                            "commit", "-qm", "operator main"], check=True)
            subprocess.run(["git", "-C", str(op), "update-ref", "refs/remotes/origin/main", "HEAD"],
                           check=True)
            sha = subprocess.run(["git", "-C", str(op), "rev-parse", "--short=7", "HEAD"],
                                 capture_output=True, text=True, check=True).stdout.strip()
            done = self.run_script(op)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn(f"check-operator-pins: reading {op} origin/main {sha} (", done.stdout)

    def test_a_failure_names_the_revision_and_the_fetch(self):
        with tempfile.TemporaryDirectory() as tmp:
            op = fake_operator(tmp)
            # One operator file with a different digest: at least one row must fail.
            path = op / "crates/stardelt-operator/values/superset.yaml"
            path.write_text(path.read_text().replace("sha256:", "sha256:0", 1))
            done = self.run_script(op)
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("check-operator-pins: FAIL (read", done.stderr)
        self.assertIn(f"git -C {op} fetch", done.stderr)
