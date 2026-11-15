"""The pin script: which deployment it targets and which image form it writes.

``scripts/pin-image.sh`` resolves a tag to a digest and pins the deployment that runs
it. Two things about it are easy to get wrong and expensive when they are: the
deployment an app actually runs in (Spark runs in ``spark-connect``, not ``spark``), and
the form of the reference it writes — the manifests carry ``repo:dev@sha256:…``, and a
live Deployment written as ``repo@sha256:…`` disagrees with them, so the next
``kubectl apply`` flips the pod spec and rolls the pods for nothing (platform#414).

The script is exercised against a fake ``kubectl`` and a fake ``curl``, the pattern the
other script tests use.
"""

import os
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "pin-image.sh"

DIGEST = "sha256:" + "ab" * 32
OTHER_DIGEST = "sha256:" + "cd" * 32

FAKE_KUBECTL = '''#!/usr/bin/env python3
import os
import sys

args = sys.argv[1:]
log = os.environ.get("FAKE_LOG")
if log:
    with open(log, "a") as handle:
        handle.write(" ".join(args) + "\\n")

if "set" in args and "image" in args:
    print([a for a in args if "=" in a and not a.startswith("-")][-1])
    sys.exit(0)

if "rollout" in args:
    print('deployment "app" successfully rolled out')
    sys.exit(0)

if "get" in args and "deploy" in args:
    name = args[args.index("deploy") + 1]
    missing = [m for m in os.environ.get("FAKE_MISSING", "").split(",") if m]
    if name in missing:
        print(
            'Error from server (NotFound): deployments.apps "%s" not found' % name,
            file=sys.stderr,
        )
        sys.exit(1)
    if any("].image}" in a for a in args):
        print(os.environ.get("FAKE_CURRENT_IMAGE", ""))
        sys.exit(0)
    print(os.environ.get("FAKE_CONTAINER", "app"))
    sys.exit(0)

if "get" in args and "configmap" in args:
    print("configmap/stardelt-spark-runtimes")
    sys.exit(0)

if "apply" in args:
    payload = sys.stdin.read()
    if log:
        with open(log, "a") as handle:
            handle.write("apply " + payload.replace("\\n", " ") + "\\n")
    print("configmap/stardelt-spark-runtimes created")
    sys.exit(0)

print("fake kubectl: unsupported: " + " ".join(args), file=sys.stderr)
sys.exit(96)
'''

FAKE_CURL = '''#!/usr/bin/env python3
import os
import sys

args = " ".join(sys.argv[1:])
if "/v2/" in args:
    print("docker-content-digest: " + os.environ.get("FAKE_DIGEST", "sha256:" + "00" * 32))
else:
    print('{"token": "fake-token"}')
'''


class PinImageTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="pin-image-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        for name, body in (("kubectl", FAKE_KUBECTL), ("curl", FAKE_CURL)):
            path = self.bin / name
            path.write_text(body)
            path.chmod(0o755)
        self.log = self.tmp / "kubectl.log"
        self.work = self.tmp / "work"
        (self.work / "manifests").mkdir(parents=True)
        self.manifest = self.work / "manifests" / "nova-deployment.yaml"
        self.manifest.write_text(
            "    containers:\n"
            "      - name: app\n"
            "        image: ghcr.io/stardelt/nova:dev@sha256:" + "00" * 32 + "\n"
        )

    def run_pin(self, app="nova", current="", digest=DIGEST, missing="", env=None):
        env = dict(os.environ, **(env or {}))
        env["PATH"] = str(self.bin) + os.pathsep + env["PATH"]
        env["FAKE_LOG"] = str(self.log)
        env["FAKE_DIGEST"] = digest
        env["FAKE_CURRENT_IMAGE"] = current
        env["FAKE_MISSING"] = missing
        return subprocess.run(
            ["bash", str(SCRIPT), app],
            cwd=self.work,
            env=env,
            capture_output=True,
            text=True,
        )

    def calls(self):
        return self.log.read_text() if self.log.exists() else ""

    def test_spark_targets_the_deployment_it_actually_runs_in(self):
        self.run_pin("spark", current="ghcr.io/stardelt/spark:dev@sha256:" + "00" * 32)
        calls = self.calls()
        self.assertIn("deploy spark-connect", calls)
        self.assertNotIn("deploy spark ", calls)
        self.assertIn("set image deploy/spark-connect", calls)

    def test_the_reference_keeps_the_tag_like_the_manifests(self):
        self.run_pin("nova", current="ghcr.io/stardelt/nova:dev@sha256:" + "00" * 32)
        self.assertIn("app=ghcr.io/stardelt/nova:dev@" + DIGEST, self.calls())
        # the manifest carries the same form, so the next apply is a no-op
        self.assertIn("ghcr.io/stardelt/nova:dev@" + DIGEST, self.manifest.read_text())

    def test_a_tagless_current_image_is_already_pinned(self):
        proc = self.run_pin("nova", current="ghcr.io/stardelt/nova@" + DIGEST)
        self.assertIn("already pinned", proc.stdout)
        self.assertNotIn("set image", self.calls())

    def test_the_same_digest_with_the_tag_is_already_pinned(self):
        proc = self.run_pin("nova", current="ghcr.io/stardelt/nova:dev@" + DIGEST)
        self.assertIn("already pinned", proc.stdout)
        self.assertNotIn("set image", self.calls())

    def test_a_different_digest_is_set_in_the_readable_form(self):
        proc = self.run_pin("nova", current="ghcr.io/stardelt/nova:dev@sha256:" + "00" * 32)
        self.assertNotIn("already pinned", proc.stdout)
        self.assertIn("app=ghcr.io/stardelt/nova:dev@" + DIGEST, self.calls())

    def test_a_missing_deployment_fails_loudly(self):
        proc = self.run_pin("nova", missing="nova")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("NotFound", proc.stderr)

    def spark_fixtures(self):
        """The two files a spark pin writes: the manifest and the runtime catalogue."""
        zero = "00" * 32
        manifest = self.work / "manifests" / "spark.yaml"
        manifest.write_text(
            "    containers:\n"
            "      - name: app\n"
            "        image: ghcr.io/stardelt/spark:dev@sha256:" + zero + "\n"
            "        args:\n"
            "          - spark.kubernetes.container.image "
            "ghcr.io/stardelt/spark:dev@sha256:" + zero + "\n"
        )
        catalogue = self.work / "chart" / "files" / "spark-runtimes.json"
        catalogue.parent.mkdir(parents=True, exist_ok=True)
        catalogue.write_text(
            '{"spark-4.1-py3.11": {"image": "ghcr.io/stardelt/spark:dev@sha256:'
            + zero
            + '"}}\n'
        )
        return manifest, catalogue

    def apply_payloads(self):
        return [
            line[len("apply ") :]
            for line in self.calls().splitlines()
            if line.startswith("apply ")
        ]

    def test_a_spark_pin_applies_the_runtime_catalogue_configmap(self):
        _, catalogue = self.spark_fixtures()
        self.run_pin("spark", current="ghcr.io/stardelt/spark:dev@sha256:" + "00" * 32)
        calls = self.calls()
        self.assertIn("get configmap stardelt-spark-runtimes", calls)
        payloads = self.apply_payloads()
        self.assertEqual(len(payloads), 1, calls)
        document = json.loads(payloads[0])
        self.assertEqual(document["kind"], "ConfigMap")
        self.assertEqual(document["metadata"]["name"], "stardelt-spark-runtimes")
        self.assertTrue(
            json.loads(document["data"]["spark-4.1-py3.11"])["image"].endswith(
                "@" + DIGEST
            )
        )
        # the cluster half comes after the file was written, not instead of it
        self.assertIn(DIGEST, catalogue.read_text())
        self.assertGreater(calls.index("apply "), calls.rindex("set image"))

    def test_a_spark_pin_without_a_cluster_writes_no_configmap(self):
        _, catalogue = self.spark_fixtures()
        self.run_pin(
            "spark",
            current="ghcr.io/stardelt/spark:dev@sha256:" + "00" * 32,
            env={"WRITE_MANIFEST_ONLY": "1"},
        )
        self.assertEqual(self.apply_payloads(), [], self.calls())
        self.assertEqual(self.calls().count("get configmap"), 0, self.calls())
        self.assertIn(DIGEST, catalogue.read_text())


if __name__ == "__main__":
    unittest.main()
