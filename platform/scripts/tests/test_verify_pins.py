"""scripts/verify-pins.sh reads the three helper CronJobs, not only the Deployments (platform#1567).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl` is a shell shim first on PATH that answers `get cronjob <name>` from the
environment and records every call, so no cluster is touched.

Why: platform#1563 pinned two helper images by digest in three manifests, and at 12:00Z the live
CronJobs still ran the bare tags while `make verify-pins` answered PASS. Nothing read those
manifests, so a pin that was never applied stayed invisible to every check.
"""

import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-pins.sh"

AWS_CLI = "amazon/aws-cli:2.17.0"
AWS_CLI_PINNED = (
    "amazon/aws-cli:2.17.0"
    "@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73"
)
PYTHON_PINNED = (
    "python:3.13.16-slim-bookworm"
    "@sha256:73aa5faee130c60da030e105ec971ccf57473fbd22ce73bd6fc9ba99a40b838d"
)
ALPINE_PINNED = (
    "alpine/k8s:1.37.1"
    "@sha256:712dc5385d1c1a3f83b747fa9e40f64d433a97f72ff42f9755ff7dd1f62846e4"
)
KEYCLOAK_PINNED = (
    "quay.io/keycloak/keycloak:26.8.2"
    "@sha256:c68e286cacd87b942ac1939a0a1481461109d6acd3a57dc496969a319a76bbf0"
)
KEYCLOAK = "quay.io/keycloak/keycloak:26.8.2"

ROWS = (
    "access-sync,demo-reset,lakehouse-backup,openbao-group-policies,"
    "spark-confmap-janitor,keycloak"
)
CORE_ROWS = "lakekeeper,trino,seaweedfs-s3,openfga,airflow-statsd"

LAKEKEEPER_PINNED = (
    "quay.io/lakekeeper/catalog:v0.15.0"
    "@sha256:b02a882df48042020c473a79b980a643fcc883a6f9c21b74c5b7cc8eabeec9fe"
)
TRINO_PINNED = (
    "trinodb/trino:484"
    "@sha256:2704eb53935af5e582d63d6033e9bf99d20c325e9b0e30b1016ef59a4773979f"
)
SEAWEEDFS_PINNED = (
    "chrislusf/seaweedfs:4.25"
    "@sha256:c42a5268ca13fcb65e0fae925886b107f4bf294d8db15e1be5509d55104eb509"
)
OPENFGA_PINNED = (
    "openfga/openfga:v1.22.1"
    "@sha256:b852071314f1fda1194d341a6ebac0a4f619b3cd87a6626d2ccc7048af4ec671"
)
STATSD_PINNED = (
    "quay.io/prometheus/statsd-exporter:v0.30.0"
    "@sha256:378cb79c4ac7d6941e5ed71b1f3cd6f4707cf42c72c104ffe81fa4d9026ef659"
)
STATSD = "quay.io/prometheus/statsd-exporter:v0.30.0"

SHIM = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl: every call is appended to FAKE_LOG and answered from FAKE_IMAGES_<name>.
echo "$*" >>"$FAKE_LOG"
args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n | --namespace) shift 2 ;;
    *) args+=("$1"); shift ;;
  esac
done
if [ "${args[0]-}" = get ] && [ "${args[1]-}" = cronjob ]; then
  name="$(printf '%s' "${args[2]-}" | tr '-' '_' | tr '[:lower:]' '[:upper:]')"
  var="FAKE_IMAGES_${name}"
  value="${!var-}"
  if [ -z "$value" ]; then
    exit 1
  fi
  printf '%s\\n' "$value"
  exit 0
fi
if [ "${args[0]-}" = get ] && [ "${args[1]-}" = deploy ]; then
  name="$(printf '%s' "${args[2]-}" | tr '-' '_' | tr '[:lower:]' '[:upper:]')"
  var="FAKE_DEPLOY_${name}"
  value="${!var-}"
  if [ -z "$value" ]; then
    exit 1
  fi
  printf '%s\\n' "$value"
  exit 0
fi
exit 0
"""


class VerifyPins(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
        self.log = self.dir / "kubectl.log"
        self.log.write_text("")

    def run_script(self, images, deploys=None, rows: str = ROWS):
        """images maps CronJob name -> list of image strings the cluster reports.

        deploys maps Deployment name -> the single image string the cluster reports.
        """
        env = dict(
            os.environ,
            PATH=f"{self.dir}:{os.environ['PATH']}",
            NAMESPACE="stardelt",
            ROWS=rows,
            FAKE_LOG=str(self.log),
        )
        for name, values in images.items():
            env["FAKE_IMAGES_" + name.upper().replace("-", "_")] = "\n".join(values)
        for name, value in (deploys or {}).items():
            env["FAKE_DEPLOY_" + name.upper().replace("-", "_")] = value
        return subprocess.run(
            ["bash", str(SCRIPT)], cwd=REPO, env=env, capture_output=True, text=True, timeout=60
        )

    def calls(self):
        return [line for line in self.log.read_text().splitlines() if line]

    def assert_read_only(self):
        self.assertTrue(self.calls(), "the shim recorded nothing")
        for call in self.calls():
            verb = call.split()
            for mutating in ("apply", "set", "patch", "delete", "create", "edit", "scale"):
                self.assertNotIn(mutating, verb, call)

    def test_pinned_images_pass(self):
        done = self.run_script(
            {
                "access-sync": [PYTHON_PINNED, PYTHON_PINNED],
                "demo-reset": [PYTHON_PINNED],
                "lakehouse-backup": [AWS_CLI_PINNED],
                "openbao-group-policies": [ALPINE_PINNED],
                "spark-confmap-janitor": [ALPINE_PINNED],
            },
            deploys={"keycloak": KEYCLOAK_PINNED},
        )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("PASS", done.stdout)
        self.assertIn(
            "✓ access-sync        2 image(s) match manifests/access-sync.yaml", done.stdout
        )
        self.assertIn(
            "✓ demo-reset         1 image(s) match manifests/demo-reset.yaml", done.stdout
        )
        self.assertIn(
            "✓ lakehouse-backup   1 image(s) match manifests/object-store-backup.yaml",
            done.stdout,
        )
        self.assertIn("1 image(s) match manifests/openbao-group-policies.yaml", done.stdout)
        self.assertIn("1 image(s) match manifests/spark-confmap-janitor.yaml", done.stdout)
        self.assertIn("matches manifests/keycloak.yaml", done.stdout)
        self.assertNotIn("✗", done.stdout)
        self.assert_read_only()

    def test_a_manifest_pin_that_was_never_applied_fails(self):
        done = self.run_script(
            {
                "access-sync": [PYTHON_PINNED, PYTHON_PINNED],
                "demo-reset": [PYTHON_PINNED],
                # The pre-#1563 bare tag: the manifest was pinned, the CronJob was not applied.
                "lakehouse-backup": [AWS_CLI],
                "openbao-group-policies": [ALPINE_PINNED],
                "spark-confmap-janitor": [ALPINE_PINNED],
            },
            deploys={"keycloak": KEYCLOAK_PINNED},
        )
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("FAIL", done.stdout + done.stderr)
        self.assertIn(AWS_CLI, done.stdout)
        self.assertIn("kubectl apply -f manifests/object-store-backup.yaml", done.stdout)
        self.assert_read_only()

    def test_keycloak_left_on_the_bare_tag_fails(self):
        """A keycloak Deployment that was never applied: quay.io is not ghcr.io/stardelt, so the
        image kind cannot read it and pin-image.sh has no keycloak app to send a reader to."""
        done = self.run_script(
            {
                "access-sync": [PYTHON_PINNED, PYTHON_PINNED],
                "demo-reset": [PYTHON_PINNED],
                "lakehouse-backup": [AWS_CLI_PINNED],
                "openbao-group-policies": [ALPINE_PINNED],
                "spark-confmap-janitor": [ALPINE_PINNED],
            },
            deploys={"keycloak": KEYCLOAK},
        )
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("FAIL", done.stdout + done.stderr)
        self.assertIn("kubectl apply -f manifests/keycloak.yaml", done.stdout)
        self.assert_read_only()

    def _core_deploys(self):
        return {
            "lakekeeper": LAKEKEEPER_PINNED,
            "trino_coordinator": TRINO_PINNED,
            "seaweedfs_s3": SEAWEEDFS_PINNED,
            "openfga": OPENFGA_PINNED,
            "airflow_statsd": STATSD_PINNED,
        }

    def test_core_service_rows_read_their_own_tag(self):
        """The five core services are pinned by digest in helm-values (platform#1587, #1594); a
        tag: row names the tag it means, because lakekeeper's file also carries the OPA sidecar."""
        done = self.run_script({}, deploys=self._core_deploys(), rows=CORE_ROWS)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("verify-pins: PASS", done.stdout)
        self.assertIn("matches helm-values/lakekeeper.yaml", done.stdout)
        self.assertIn("matches helm-values/trino.yaml", done.stdout)
        self.assertIn("matches helm-values/seaweedfs.yaml", done.stdout)
        self.assertIn("matches helm-values/openfga.yaml", done.stdout)
        self.assertIn("matches helm-values/airflow.yaml", done.stdout)
        self.assertNotIn("✗", done.stdout)
        self.assert_read_only()

    def test_statsd_left_on_the_bare_tag_names_its_rollout(self):
        """If the #1594 airflow rollout is skipped, its Deployment runs the bare tag, and the
        failing row must name `make step-airflow`, not a pin-image.sh app that does not exist."""
        deploys = self._core_deploys()
        deploys["airflow_statsd"] = STATSD
        done = self.run_script({}, deploys=deploys, rows=CORE_ROWS)
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("FAIL", done.stdout + done.stderr)
        self.assertIn("make step-airflow", done.stdout)
        self.assert_read_only()


if __name__ == "__main__":
    unittest.main()
