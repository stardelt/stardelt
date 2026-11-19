"""The gateway's one configuration document, written by two install paths (stardelt#256).

Secret `seaweedfs-s3-config` is written twice — by `scripts/s3-secrets.sh` (`make install`,
`make install-core`) and by `chart/templates/s3-credentials.yaml` (`make chart-install`) — and
the SeaweedFS S3 gateway reads the SAME key twice: with `-config` for the identities, and with
`-iam.config` for the STS/IAM half (`sts.signingKey`, `policy`, `policies`, `roles`). If the two
writers drift apart, one install path silently produces a gateway without STS; if the s3
Deployment loses `-iam.config`, the IAM half is stored and never loaded.

`test_document_shape` reads the document out of the script without a cluster. The other two
need Helm and the sub-charts fetched into `chart/charts/*.tgz` (gitignored;
`helm dependency update chart`); the `lint` job runs `unittest discover` and has neither, so the
module skips itself there, while the `helm-lint` job has both.
"""

import json
import pathlib
import shutil
import subprocess
import unittest

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "s3-secrets.sh"
HELM = shutil.which("helm")

ACCESS_KEY = "AK"
SECRET_KEY = "SK"
STS_SIGNING_KEY = "SIGN"

ROLE_ARN = "arn:aws:iam::000000000000:role/stardelt"
IAM_CONFIG_ARG = "-iam.config=/etc/sw/seaweedfs_s3_config"


def missing_subcharts():
    """Sub-chart names `chart/Chart.yaml` declares but `chart/charts/` lacks."""
    with open(REPO_ROOT / "chart" / "Chart.yaml", encoding="utf-8") as handle:
        deps = yaml.safe_load(handle).get("dependencies") or []
    present = [p.name for p in (REPO_ROOT / "chart" / "charts").glob("*.tgz")]
    return [d["name"] for d in deps
            if not any(p.startswith(d["name"] + "-") for p in present)]


def seaweedfs_chart():
    """The fetched seaweedfs sub-chart tarball, the values `helm-values/seaweedfs.yaml` target."""
    found = sorted((REPO_ROOT / "chart" / "charts").glob("seaweedfs-*.tgz"))
    return found[-1] if found else None


def script_document():
    """The document `scripts/s3-secrets.sh` writes, printed without a cluster."""
    out = subprocess.run(
        ["bash", str(SCRIPT), "--print-config", ACCESS_KEY, SECRET_KEY, STS_SIGNING_KEY],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if out.returncode != 0:
        raise AssertionError(f"s3-secrets.sh --print-config failed: {out.stderr[-2000:]}")
    return json.loads(out.stdout)


class DocumentShapeTest(unittest.TestCase):
    def test_document_shape(self):
        doc = script_document()
        self.assertEqual(
            set(doc), {"identities", "sts", "policy", "policies", "roles"},
            "the document carries the identity half and the IAM half",
        )
        self.assertEqual(doc["sts"]["signingKey"], STS_SIGNING_KEY)
        self.assertEqual(doc["identities"][0]["actions"], ["Admin", "Read", "Write"],
                         "the platform identity half is unchanged")
        self.assertEqual(doc["roles"][0]["roleArn"], ROLE_ARN)


class ChartAgreementTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if HELM is None:
            raise unittest.SkipTest("helm is not on PATH")
        missing = missing_subcharts()
        if missing:
            print(f"skip: chart/charts/ lacks {', '.join(missing)} "
                  "(run `helm dependency update chart`)")
            raise unittest.SkipTest(f"chart dependencies not fetched: {', '.join(missing)}")

    def test_chart_and_script_agree(self):
        """The chart's document and the script's are the same JSON, key for key."""
        out = subprocess.run(
            [HELM, "template", "stardelt-platform", "chart",
             "--show-only", "templates/s3-credentials.yaml",
             "--set", f"s3Credentials.accessKey={ACCESS_KEY}",
             "--set", f"s3Credentials.secretKey={SECRET_KEY}",
             "--set", f"s3Credentials.stsSigningKey={STS_SIGNING_KEY}"],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        docs = [d for d in yaml.safe_load_all(out.stdout) if d]
        config = next(d for d in docs if d["metadata"]["name"] == "seaweedfs-s3-config")
        chart_doc = json.loads(config["stringData"]["seaweedfs_s3_config"])
        self.assertEqual(chart_doc, script_document(),
                         "scripts/s3-secrets.sh and chart/templates/s3-credentials.yaml "
                         "must write the same document")
        self.assertEqual(config["stringData"]["seaweedfs_s3_config"],
                         json.dumps(chart_doc, separators=(",", ":")),
                         "the Secret carries a JSON document, not YAML")

    def test_extra_args_rendered(self):
        """`-iam.config` reaches the s3 Deployment's command, next to the identities' `-config`."""
        chart = seaweedfs_chart()
        if chart is None:
            raise unittest.SkipTest("chart/charts/seaweedfs-*.tgz is not fetched")

        out = subprocess.run(
            [HELM, "template", "seaweedfs", str(chart),
             "--namespace", "stardelt", "-f", "helm-values/seaweedfs.yaml"],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        deployment = next(
            d for d in yaml.safe_load_all(out.stdout)
            if d and d.get("kind") == "Deployment" and d["metadata"]["name"] == "seaweedfs-s3"
        )
        command = deployment["spec"]["template"]["spec"]["containers"][0]["command"]
        # The chart's own entrypoint is one `sh -ec` script that `exec`s the gateway; the
        # flags live in it, one per line, each ending in a backslash.
        script = command[-1]
        self.assertIn("-config=/etc/sw/seaweedfs_s3_config", script,
                      "the identities half must still be passed")
        self.assertIn(IAM_CONFIG_ARG, script,
                      "the IAM half is stored in the Secret; without this flag it is never read")
        self.assertLess(script.index("-config=/etc/sw/seaweedfs_s3_config"),
                        script.index(IAM_CONFIG_ARG))


if __name__ == "__main__":
    unittest.main()
