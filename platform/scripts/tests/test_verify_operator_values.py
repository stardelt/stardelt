"""Tests for scripts/verify-operator-values.sh re-reading a component once (platform#781), for its
digest loop, which compares every digest of the shared values files with the running operator
(platform#1641), and for the pod it probes — the one that carries the pinned digest (platform#192).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl` is replaced by a shell shim that is first on PATH: `get deploy` succeeds
and `exec … values <component> full …` prints a fixture document, so no cluster is touched. The
fixture documents are built here and carry the same image digests the script reads from
`helm-values/`, so a pin bump does not break the test. The pods the shim lists come from
`$FAKE_DIR/pods`: one line per pod, `<name> <image…>`.
"""
import os
import re
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-operator-values.sh"

SHIM = """#!/usr/bin/env bash
set -u
# Fake kubectl: `-n NS get deploy stardelt-operator` succeeds and the pods it selects come from
# $FAKE_DIR/pods (one `<name> <image…>` line each); `-n NS exec POD -- stardelt-operator values
# <component> full [--identity DOMAIN]` prints $FAKE_DIR/<component>-<mode>.yaml and logs the call.
# When FAKE_TRUNCATE asks for it, the trino identity document is cut short: a `kubectl exec` stream
# can lose its tail, and the script must re-read before it reports a miss.
case "${3-}" in
  get)
    case "${*}" in
      *" get pods "*) cat "$FAKE_DIR/pods" ;;
      *" -o go-template="*) printf 'app=stardelt-operator,' ;;
    esac
    exit 0
    ;;
  exec)
    pod="${4-}"
    component="${8-}"
    mode=plain
    if [ "${10-}" = "--identity" ]; then
      mode=identity
    fi
    printf '%s\\n' "$pod" >> "$FAKE_DIR/exec-pods.log"
    printf '%s %s\\n' "$component" "$mode" >> "$FAKE_KUBECTL_LOG"
    file="$FAKE_DIR/$component-$mode.yaml"
    truncate=0
    if [ "$component" = trino ] && [ "$mode" = identity ]; then
      calls="$(grep -c '^trino identity$' "$FAKE_KUBECTL_LOG")"
      case "${FAKE_TRUNCATE:-none}" in
        first) [ "$calls" -eq 1 ] && truncate=1 ;;
        always) truncate=1 ;;
      esac
    fi
    if [ "$truncate" = 1 ]; then
      head -n -20 "$file"
    else
      cat "$file"
    fi
    exit 0
    ;;
esac
exit 1
"""


def digest_of(path, pattern=None):
    """The digest the script reads: the named line, else the first `digest:` line, else the first match."""
    text = (REPO / path).read_text()
    if pattern:
        line = next(l for l in text.splitlines() if re.search(pattern, l))
        return re.search(r"sha256:[0-9a-f]{64}", line).group(0)
    match = re.search(r"^\s+digest: sha256:[0-9a-f]{64}", text, re.M) or re.search(
        r"sha256:[0-9a-f]{64}", text
    )
    if not match:
        raise AssertionError(f"no digest in {path}")
    return re.search(r"sha256:[0-9a-f]{64}", match.group(0)).group(0)


ESO = digest_of("helm-values/external-secrets.yaml")
SPARK_OPERATOR = digest_of("helm-values/spark-operator.yaml")
STRIMZI = digest_of("helm-values/strimzi.yaml")

# Mirrors `shared_digests` in scripts/verify-operator-values.sh: component, values file, and the line
# pattern when a file carries an image the operator path does not render (lakekeeper's OPA bridge
# belongs to the platform install only, platform#1641).
SHARED = [
    ("trino", "helm-values/trino.yaml", None),
    ("lakekeeper", "helm-values/lakekeeper.yaml", r"v0\.15\.0@sha256:"),
    ("seaweedfs", "helm-values/seaweedfs.yaml", None),
    ("openbao", "helm-values/openbao.yaml", None),
    ("openfga", "helm-values/openfga.yaml", None),
    ("airflow", "helm-values/airflow.yaml", None),
    ("jupyterhub", "helm-values/jupyterhub.yaml", None),
    ("superset", "helm-values/superset.yaml", None),
    ("external-secrets", "helm-values/external-secrets.yaml", None),
    ("spark-operator", "helm-values/spark-operator.yaml", None),
    ("kafka", "helm-values/strimzi.yaml", None),
    ("cnpg", "helm-values/cnpg.yaml", None),
    ("barman-cloud", "helm-values/barman-cloud.yaml", None),
]

IDP_GATE = digest_of("helm-values/trino-identity.yaml")

# The operator pin the script compares a pod's image against: helm-values/operator.yaml's
# `tag: dev@sha256:…` line, the one scripts/check_golden_image.py reads.
OPERATOR_PIN_RE = re.compile(r'^\s*tag:\s*"?dev@(sha256:[0-9a-f]{64})"?\s*$', re.M)


def operator_pin():
    match = OPERATOR_PIN_RE.search((REPO / "helm-values" / "operator.yaml").read_text())
    assert match, "no operator pin in helm-values/operator.yaml"
    return match.group(1)


def pod_line(name, digest):
    """One line of the shim's pod listing: the name, then the container images."""
    return f"{name} ghcr.io/stardelt/stardelt-operator:dev@{digest}\n"


def script_digests(path, pattern=None):
    """Every digest the script's loop expects from `path`, sorted, exactly as it reads them."""
    text = (REPO / path).read_text()
    if pattern:
        text = "\n".join(line for line in text.splitlines() if re.search(pattern, line))
    return sorted(set(re.findall(r"sha256:[0-9a-f]{64}", text)))


def fixtures():
    loader = digest_of("helm-values/airflow.yaml")
    notebook = digest_of("helm-values/jupyterhub.yaml", r'^\s+tag: "dev@sha256:')
    superset = digest_of("helm-values/superset.yaml")
    eso = digest_of("helm-values/external-secrets.yaml")
    spark_operator = digest_of("helm-values/spark-operator.yaml")
    strimzi = digest_of("helm-values/strimzi.yaml")
    # The trino identity document keeps the three needles the truncated read must still satisfy
    # before it misses `passwordAuthSecret` in the removed tail.
    trino_identity = "\n".join(
        [
            "apiVersion: v1",
            "kind: ConfigMap",
            "metadata:",
            "  name: trino",
            "data:",
            "  server:",
            "    https:",
            "      port: 8443",
            "    authenticationType: PASSWORD,OAUTH2",
            "  accessControl:",
            '    "original_user": "superset"',
        ]
        + [f"    catalog.probe.{i}: value" for i in range(25)]
        + [
            "  password: trino-password-db",
            "    passwordAuthSecret: trino-password-db",
            f"  IdP gate image: trinodb/trino:484@{IDP_GATE}",
        ]
    )
    docs = {
        "airflow-plain.yaml": "\n".join(
            [
                "apiVersion: v1",
                "kind: ConfigMap",
                "data:",
                "  serviceAccount:",
                "    name: stardelt-nova-operator",
                "  loader:",
                f"    image: ghcr.io/stardelt/airflow-loader:dev@{loader}",
            ]
        )
        + "\n",
        "airflow-identity.yaml": "data:\n  oidc:\n    secretName: stardelt-oidc-clients\n",
        "jupyterhub-plain.yaml": "\n".join(
            [
                "storage:",
                "  capacity: 2Gi",
                "singleuser:",
                f"  image: ghcr.io/stardelt/notebook:dev@{notebook}",
            ]
        )
        + "\n",
        "jupyterhub-identity.yaml": "singleuser:\n  oidc:\n    secretName: stardelt-oidc-clients\n",
        "superset-plain.yaml": "image:\n  repository: ghcr.io/stardelt/superset\n"
        f"  tag: dev@{superset}\n",
        "superset-identity.yaml": "extraConfig:\n  trino_uri: trino.example.test:443\n",
        "seaweedfs-plain.yaml": "\n".join(
            [
                "apiVersion: v1",
                "kind: ConfigMap",
                "volume:",
                "  name: data",
                "  size: 20Gi",
                "  storageClassName: local-path",
                "s3:",
                "  enabled: true",
                "  existingConfigSecret: seaweedfs-s3-config",
                "  extraArgs:",
                "    - -iam.config=/etc/sw/seaweedfs_s3_config",
            ]
        )
        + "\n",
        "trino-plain.yaml": "data:\n  catalog: {}\n",
        "trino-identity.yaml": trino_identity + "\n",
        "external-secrets-plain.yaml": f'image:\n  tag: "v2.15.0@{eso}"\n',
        "spark-operator-plain.yaml": f'controller:\n  replicas: 1\nimage:\n  tag: "2.5.2@{spark_operator}"\n',
        "kafka-plain.yaml": f'image:\n  tag: "1.3.1@{strimzi}"\n'
        f'topicOperator:\n  image:\n    tag: "1.3.1@{strimzi}"\n',
    }
    # Every digest the loop probes, in the document `values <component> full` prints for it.
    for component, path, pattern in SHARED:
        name = f"{component}-plain.yaml"
        docs[name] = docs.get(name, "") + "".join(
            f"  image: ghcr.io/example/{component}@{digest}\n"
            for digest in script_digests(path, pattern)
        )
    return docs


class TestVerifyOperatorValues(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
        for name, text in fixtures().items():
            (self.dir / name).write_text(text)
        (self.dir / "pods").write_text(pod_line("stardelt-operator-7d9c-new", operator_pin()))
        self.log = self.dir / "kubectl.log"

    def run_script(self, truncate="none"):
        env = dict(
            os.environ,
            PATH=f"{self.dir}:{os.environ['PATH']}",
            NAMESPACE="stardelt",
            FAKE_DIR=str(self.dir),
            FAKE_KUBECTL_LOG=str(self.log),
            FAKE_TRUNCATE=truncate,
        )
        env.pop("BASE_DOMAIN", None)
        return subprocess.run(
            ["bash", str(SCRIPT)], cwd=REPO, env=env, capture_output=True, text=True, timeout=60
        )

    def calls(self):
        return self.log.read_text().splitlines() if self.log.exists() else []

    def probed_pods(self):
        path = self.dir / "exec-pods.log"
        return path.read_text().splitlines() if path.exists() else []

    def test_a_full_document_passes_every_probe_without_a_retry(self):
        done = self.run_script()
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("verify-operator-values: PASS (46 probes)", done.stdout)
        self.assertNotIn("re-reading values once", done.stderr)
        self.assertEqual(
            len(self.calls()), 24, "seven plain reads, four with --identity, thirteen digest reads"
        )

    def test_a_first_read_that_misses_the_tail_is_re_read_and_passes(self):
        done = self.run_script("first")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertIn("verify-operator-values: PASS (46 probes)", done.stdout)
        self.assertIn("re-reading values once", done.stderr)
        self.assertEqual(self.calls().count("trino identity"), 2)

    def test_a_second_read_that_also_misses_fails_once(self):
        done = self.run_script("always")
        self.assertEqual(done.returncode, 1, done.stderr)
        self.assertIn("verify-operator-values: FAIL (2 of 46 probes)", done.stdout)
        self.assertEqual((done.stdout + done.stderr).count("trino identity: password file"), 1)
        self.assertIn("is not in the running operator", done.stderr)
        self.assertEqual(self.calls().count("trino identity"), 2)

    def test_a_kafka_operator_pin_the_operator_does_not_carry_fails(self):
        (self.dir / "kafka-plain.yaml").write_text(
            f'image:\n  tag: "1.3.1@sha256:{"0" * 64}"\n'
        )
        done = self.run_script()
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("verify-operator-values: FAIL", done.stdout)
        self.assertIn("kafka: cluster operator image", done.stderr)

    def test_a_shared_digest_the_operator_does_not_carry_fails_and_names_it(self):
        """The loop of platform#1641: one absent digest of cnpg's values file fails on its own row."""
        absent = script_digests("helm-values/cnpg.yaml")[0]
        (self.dir / "cnpg-plain.yaml").write_text("image:\n  name: nothing\n")
        done = self.run_script()
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn(f"✗ cnpg {absent[7:22]}…: `{absent}` is not in the running operator", done.stderr)
        self.assertIn("verify-operator-values: FAIL (1 of 46 probes)", done.stdout)

    def test_the_pod_running_the_pinned_digest_is_the_one_probed(self):
        """platform#192: two old pods stay Ready while they terminate; the pinned one answers."""
        old = "sha256:" + "a" * 64
        pin = operator_pin()
        (self.dir / "pods").write_text(
            pod_line("stardelt-operator-old-1", old)
            + pod_line("stardelt-operator-old-2", old)
            + pod_line("stardelt-operator-pinned", pin)
        )
        done = self.run_script()
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("verify-operator-values: PASS (46 probes)", done.stdout)
        self.assertIn(
            f"✓ values             probed stardelt-operator-pinned ({pin[:15]}…)", done.stdout
        )
        self.assertEqual(set(self.probed_pods()), {"stardelt-operator-pinned"})
        self.assertEqual(len(self.calls()), 24)

    def test_no_pod_runs_the_pinned_digest_proves_nothing_and_exits_3(self):
        """A not-yet-rolled-out pin is UNVERIFIED, never FAIL: nothing has been disproved."""
        (self.dir / "pods").write_text(pod_line("stardelt-operator-old-1", "sha256:" + "b" * 64))
        done = self.run_script()
        self.assertEqual(done.returncode, 3, done.stdout + done.stderr)
        self.assertIn(
            f"UNVERIFIED (no pod runs the pinned digest {operator_pin()})", done.stderr
        )
        self.assertNotIn("FAIL", done.stdout + done.stderr)
        self.assertEqual(self.calls(), [])
        self.assertEqual(self.probed_pods(), [])

    def test_an_empty_pod_list_proves_nothing_and_exits_3(self):
        (self.dir / "pods").write_text("")
        done = self.run_script()
        self.assertEqual(done.returncode, 3, done.stdout + done.stderr)
        self.assertIn("UNVERIFIED (no pod runs the pinned digest", done.stderr)
        self.assertNotIn("FAIL", done.stdout + done.stderr)


if __name__ == "__main__":
    unittest.main()
