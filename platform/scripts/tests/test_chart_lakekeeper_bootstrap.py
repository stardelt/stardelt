"""The chart's lakekeeper-bootstrap Job passes Pod Security `restricted` (platform#1375).

`platform/chart/templates/lakekeeper-bootstrap.yaml` is the chart twin of
`platform/manifests/lakekeeper-bootstrap.yaml`. platform#1367 fixed that manifest
(and the operator's `bootstrap_job`) but not this copy, so a chart install in a
namespace that enforces `restricted` lost the `warehouse` bootstrap to a refused
Job: the pod carried no pod-level `securityContext` at all.

The image is pinned by digest too: every other copy of the bootstrap Job carries
`curlimages/curl:8.10.1@sha256:d9b4541e…`, and a floating tag in a chart value
would drift on the next pull.

Needs Helm and the sub-charts fetched into `chart/charts/*.tgz` (gitignored;
`helm dependency update chart`) for the render test. The `lint` job runs
`unittest discover` as well and has no Helm, so that test skips itself there; the
`helm-lint` job runs it after its dependency step and has both.
"""

import pathlib
import re
import shutil
import subprocess
import unittest

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

# The `lint` job runs `unittest discover` without Helm; the `helm-lint` job has it.
HELM = shutil.which("helm")

IMAGE = re.compile(r"^curlimages/curl:8\.10\.1@sha256:[0-9a-f]{64}$")

POD_SECURITY_CONTEXT = {
    "runAsNonRoot": True,
    "runAsUser": 65534,
    "runAsGroup": 65534,
    "seccompProfile": {"type": "RuntimeDefault"},
}

CONTAINER_SECURITY_CONTEXT = {
    "allowPrivilegeEscalation": False,
    "capabilities": {"drop": ["ALL"]},
    "seccompProfile": {"type": "RuntimeDefault"},
}


def missing_subcharts():
    """Sub-chart names `chart/Chart.yaml` declares but `chart/charts/` lacks."""
    with open(REPO_ROOT / "chart" / "Chart.yaml", encoding="utf-8") as handle:
        deps = yaml.safe_load(handle).get("dependencies") or []
    present = [p.name for p in (REPO_ROOT / "chart" / "charts").glob("*.tgz")]
    return [d["name"] for d in deps
            if not any(p.startswith(d["name"] + "-") for p in present)]


class ChartLakekeeperBootstrapTest(unittest.TestCase):
    def test_the_image_is_pinned_by_digest(self):
        with open(REPO_ROOT / "chart" / "values.yaml", encoding="utf-8") as handle:
            values = yaml.safe_load(handle)
        image = values["lakekeeperBootstrap"]["image"]
        self.assertRegex(image, IMAGE)

    def test_the_rendered_pod_is_restricted(self):
        if HELM is None:
            raise unittest.SkipTest("helm is not on PATH")
        missing = missing_subcharts()
        if missing:
            print(f"skip: chart/charts/ lacks {', '.join(missing)} "
                  "(run `helm dependency update chart`)")
            raise unittest.SkipTest(f"chart dependencies not fetched: {', '.join(missing)}")

        out = subprocess.run([HELM, "template", "stardelt", "chart", "-n", "stardelt"],
                             cwd=REPO_ROOT, capture_output=True, text=True)
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])

        jobs = [doc for doc in yaml.safe_load_all(out.stdout)
                if doc and doc.get("kind") == "Job"
                and doc.get("metadata", {}).get("name") == "lakekeeper-bootstrap"]
        self.assertEqual(len(jobs), 1, "expected one lakekeeper-bootstrap Job")

        pod_spec = jobs[0]["spec"]["template"]["spec"]
        self.assertEqual(pod_spec["securityContext"], POD_SECURITY_CONTEXT)

        containers = [c for c in pod_spec["containers"] if c["name"] == "bootstrap"]
        self.assertEqual(len(containers), 1, "expected one `bootstrap` container")
        self.assertEqual(containers[0]["securityContext"], CONTAINER_SECURITY_CONTEXT)


if __name__ == "__main__":
    unittest.main()
