"""scripts/verify-pins.sh reads the three helper CronJobs, not only the Deployments (platform#1567).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl` is a shell shim first on PATH that answers `get cronjob <name>` from the
environment and records every call, so no cluster is touched.

Why: platform#1563 pinned two helper images by digest in three manifests, and at 12:00Z the live
CronJobs still ran the bare tags while `make verify-pins` answered PASS. Nothing read those
manifests, so a pin that was never applied stayed invisible to every check.
"""

import base64
import os
import re
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-pins.sh"

# The real values file: the hub image (4.4.5@…) comes before the spawn image (dev@…), which
# is exactly the order that made the unanchored `hub` regex read the wrong digest (#1605).
JUPYTER_VALUES = (REPO / "helm-values" / "jupyterhub.yaml").read_text()
SPAWN_DIGEST = re.search(r'tag:\s*"dev@(sha256:[0-9a-f]{64})"', JUPYTER_VALUES).group(1)
HUB_DIGEST = re.search(r'tag:\s*"4\.4\.5@(sha256:[0-9a-f]{64})"', JUPYTER_VALUES).group(1)

# The CNPG operator, the barman-cloud plugin and Reloader: three Deployments pinned in #1615 and
# read by no row until #1616. The digests come from the values files, as the notebook ones do, so
# a pin change does not need a test edit and a stale literal cannot pass.
CNPG_VALUES = (REPO / "helm-values" / "cnpg.yaml").read_text()
BARMAN_VALUES = (REPO / "helm-values" / "barman-cloud.yaml").read_text()
RELOADER_VALUES = (REPO / "helm-values" / "reloader.yaml").read_text()
CNPG_DIGEST = re.search(r'tag:\s*"1\.30\.1@(sha256:[0-9a-f]{64})"', CNPG_VALUES).group(1)
BARMAN_DIGEST = re.search(r'tag:\s*"v0\.16\.0@(sha256:[0-9a-f]{64})"', BARMAN_VALUES).group(1)
RELOADER_DIGEST = re.search(r'tag:\s*"v1\.4\.24@(sha256:[0-9a-f]{64})"', RELOADER_VALUES).group(1)
CNPG_ROWS = "cnpg-operator,barman-cloud,reloader"

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

OPENBAO_PINNED = "quay.io/openbao/openbao:2.7.2" "@sha256:5046911075e137e154ce49ab051502f4ec7cf367d484ae4a4dce665d9b90561f"
SUPERSET_PG_PINNED = "docker.io/bitnamilegacy/postgresql:14.17.0-debian-12-r3" "@sha256:769d9d755a41e845d00c8eda9bd2c0945ee9c437d64cc215db7a4bd05cd17f17"
SUPERSET_REDIS_PINNED = "docker.io/bitnamilegacy/redis:7.0.10-debian-11-r4" "@sha256:224a79826b42869bdc72a70933efd840c5a5f10a70caafca68e57be6901e36fb"
AIRFLOW_PG_PINNED = "docker.io/bitnamilegacy/postgresql:16.1.0-debian-11-r15" "@sha256:29e3dd0e7e7a740eabdbae6f82673507d180701a719bbdb6d6308a58cf723e64"
PAUSE_PINNED = "registry.k8s.io/pause:3.10.1" "@sha256:278fb9dbcca9518083ad1e11276933a2e96f23de604a3a08cc3c80002767d24c"
HUB_PINNED = "quay.io/jupyterhub/k8s-hub:4.4.5" "@sha256:6ce210488f5598e2dd5130d82e84d9f74b72538b66895861240bd84926ec50bd"
PROXY_PINNED = "quay.io/jupyterhub/configurable-http-proxy:5.4.0" "@sha256:7977d796d372642aa9eb962ff36590b3019f8f24493fcf9f1d07d2c81e8e6080"
SCHEDULER_PINNED = "registry.k8s.io/kube-scheduler:v1.30.14" "@sha256:74a5cf9cfa9fcc2246f68c650f1f5c7add20da2145f13800fd97ba1d69fc06c8"
CHART_ROWS = (
    "superset-postgresql,superset-redis-master,airflow-postgresql,openbao,user-placeholder,"
    "continuous-image-puller,hub-image,proxy,user-scheduler"
)

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
if [ "${args[0]-}" = get ] && [ "${args[1]-}" = secret ]; then
  printf '%s' "${FAKE_SECRET_HUB-}"
  exit 0
fi
if [ "${args[0]-}" = get ] && [ "${args[1]-}" = sts ]; then
  name="$(printf '%s' "${args[2]-}" | tr '-' '_' | tr '[:lower:]' '[:upper:]')"
  var="FAKE_STS_${name}"
  value="${!var-}"
  if [ -z "$value" ]; then
    exit 1
  fi
  printf '%s\\n' "$value"
  exit 0
fi
if [ "${args[0]-}" = get ] && [ "${args[1]-}" = ds ]; then
  name="$(printf '%s' "${args[2]-}" | tr '-' '_' | tr '[:lower:]' '[:upper:]')"
  var="FAKE_DS_${name}"
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

    def run_script(
        self,
        images,
        deploys=None,
        rows: str = ROWS,
        secret_hub: str | None = None,
        statefulsets=None,
        daemonsets=None,
    ):
        """images maps CronJob name -> list of image strings the cluster reports.

        deploys maps Deployment name -> the single image string the cluster reports.
        secret_hub is the plain values.yaml the Secret `hub` carries; the shim base64-encodes it.
        statefulsets and daemonsets map StatefulSet/DaemonSet name -> that single image string.
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
        if secret_hub is not None:
            env["FAKE_SECRET_HUB"] = base64.b64encode(secret_hub.encode()).decode()
        for name, value in (statefulsets or {}).items():
            env["FAKE_STS_" + name.upper().replace("-", "_")] = value
        for name, value in (daemonsets or {}).items():
            env["FAKE_DS_" + name.upper().replace("-", "_")] = value
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

    def test_notebook_row_reads_the_spawn_image_not_the_hub_image(self):
        """#1605/#1608 pinned the chart's own hub image above the singleuser tag; the unanchored
        `hub` regex then read the hub digest and the row could never pass."""
        done = self.run_script(
            {}, rows="notebook", secret_hub=f"singleuser:\n  image:\n    tag: dev@{SPAWN_DIGEST}\n"
        )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("✓ notebook", done.stdout)
        self.assertIn(SPAWN_DIGEST[:19], done.stdout)
        self.assertNotIn(HUB_DIGEST[:19], done.stdout)

    def test_a_spawn_image_that_is_not_the_pin_fails(self):
        other = "sha256:" + "0" * 64
        done = self.run_script(
            {}, rows="notebook", secret_hub=f"singleuser:\n  image:\n    tag: dev@{other}\n"
        )
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("FAIL", done.stdout + done.stderr)
        self.assertIn("make install-notebooks", done.stdout)

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

    def test_chart_images_pass(self):
        """#1604 and #1605 pinned the chart-default images by digest: five StatefulSets, the
        continuous-image-puller DaemonSet and the three JupyterHub Deployments. Nothing read
        them, so a rollback to a floating tag stayed green (the #1594 class, one step on)."""
        done = self.run_script(
            {},
            deploys={"hub": HUB_PINNED, "proxy": PROXY_PINNED, "user-scheduler": SCHEDULER_PINNED},
            statefulsets={
                "openbao": OPENBAO_PINNED,
                "superset-postgresql": SUPERSET_PG_PINNED,
                "superset-redis-master": SUPERSET_REDIS_PINNED,
                "airflow-postgresql": AIRFLOW_PG_PINNED,
                "user-placeholder": PAUSE_PINNED,
            },
            daemonsets={"continuous-image-puller": PAUSE_PINNED},
            rows=CHART_ROWS,
        )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("verify-pins: PASS", done.stdout)
        self.assertIn("matches helm-values/superset.yaml", done.stdout)
        self.assertIn("matches helm-values/airflow.yaml", done.stdout)
        self.assertIn("matches helm-values/openbao.yaml", done.stdout)
        self.assertIn("matches helm-values/jupyterhub.yaml", done.stdout)
        # One ✓ per row: four files, nine rows.
        self.assertEqual(done.stdout.count("✓"), 9, done.stdout)
        self.assertNotIn("✗", done.stdout)
        self.assert_read_only()

    def test_openbao_left_on_the_bare_tag_fails(self):
        """The OpenBao StatefulSet before the #1604 rollout runs the bare tag; its row must fail
        and name `make openbao`, not a pin-image.sh app that does not exist."""
        done = self.run_script(
            {},
            deploys={"hub": HUB_PINNED, "proxy": PROXY_PINNED, "user-scheduler": SCHEDULER_PINNED},
            statefulsets={
                "openbao": "quay.io/openbao/openbao:2.7.2",
                "superset-postgresql": SUPERSET_PG_PINNED,
                "superset-redis-master": SUPERSET_REDIS_PINNED,
                "airflow-postgresql": AIRFLOW_PG_PINNED,
                "user-placeholder": PAUSE_PINNED,
            },
            daemonsets={"continuous-image-puller": PAUSE_PINNED},
            rows=CHART_ROWS,
        )
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("FAIL", done.stdout + done.stderr)
        self.assertIn("make openbao", done.stdout)

    def test_cnpg_family_rows_read_their_deployments(self):
        done = self.run_script(
            {},
            deploys={
                "cnpg-cloudnative-pg": "ghcr.io/cloudnative-pg/cloudnative-pg:1.30.1@" + CNPG_DIGEST,
                "barman-cloud-plugin-barman-cloud": "ghcr.io/cloudnative-pg/plugin-barman-cloud:v0.16.0@" + BARMAN_DIGEST,
                "reloader-reloader": "ghcr.io/stakater/reloader:v1.4.24@" + RELOADER_DIGEST,
            },
            rows=CNPG_ROWS,
        )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("PASS", done.stdout)
        self.assertIn("✓ cnpg-operator", done.stdout)
        self.assertIn("✓ barman-cloud", done.stdout)
        self.assertIn("✓ reloader", done.stdout)
        self.assertNotIn("✗", done.stdout)
        self.assert_read_only()

    def test_reloader_left_on_the_bare_tag_fails_and_names_the_rollout(self):
        done = self.run_script(
            {},
            deploys={
                "cnpg-cloudnative-pg": "ghcr.io/cloudnative-pg/cloudnative-pg:1.30.1@" + CNPG_DIGEST,
                "barman-cloud-plugin-barman-cloud": "ghcr.io/cloudnative-pg/plugin-barman-cloud:v0.16.0@" + BARMAN_DIGEST,
                "reloader-reloader": "ghcr.io/stakater/reloader:v1.4.24",
            },
            rows=CNPG_ROWS,
        )
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("✗ reloader", done.stdout)
        self.assertIn("make step-reloader", done.stdout)


if __name__ == "__main__":
    unittest.main()
