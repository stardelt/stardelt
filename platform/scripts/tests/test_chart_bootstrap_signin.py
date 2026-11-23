"""The chart's lakekeeper-bootstrap Job is the manifest Job (platform#1379).

`platform/chart/templates/lakekeeper-bootstrap.yaml` carried a stale copy of the
bootstrap script: it called `GET /management/v1/info` with no Authorization
header, so on a chart install where the catalog authenticates (identity.mode
`platform` or `external`) Lakekeeper answered 401, the Job exhausted
`backoffLimit` and no `warehouse` was created. The manifest's copy signs in as
`svc-catalog-bootstrap` and the operator's `bootstrap_job` does too; the chart
was the third copy and the only one that could drift silently.

This module renders both documents and compares the container and the pod's
volumes field by field, so a change to one without the other fails CI. The chart
defaults produce the manifest's Secret names, so the comparison is exact.

Needs Helm and the sub-charts fetched into `chart/charts/*.tgz` (gitignored;
`helm dependency update chart`). The `lint` job runs `unittest discover` as well
and does not fetch them, so the module skips itself there; the `helm-lint` job
runs it after its dependency step and has both.
"""

import pathlib
import shutil
import subprocess
import unittest

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

# The `lint` job runs `unittest discover` without Helm; the `helm-lint` job has it.
HELM = shutil.which("helm")

JOB = "lakekeeper-bootstrap"


def missing_subcharts():
    """Sub-chart names `chart/Chart.yaml` declares but `chart/charts/` lacks."""
    with open(REPO_ROOT / "chart" / "Chart.yaml", encoding="utf-8") as handle:
        deps = yaml.safe_load(handle).get("dependencies") or []
    present = [p.name for p in (REPO_ROOT / "chart" / "charts").glob("*.tgz")]
    return [d["name"] for d in deps
            if not any(p.startswith(d["name"] + "-") for p in present)]


def job_from(documents):
    jobs = [doc for doc in documents
            if isinstance(doc, dict) and doc.get("kind") == "Job"
            and doc.get("metadata", {}).get("name") == JOB]
    if len(jobs) != 1:
        raise AssertionError(f"expected one {JOB} Job, found {len(jobs)}")
    return jobs[0]


class ChartBootstrapSigninTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if HELM is None:
            raise unittest.SkipTest("helm is not on PATH")
        missing = missing_subcharts()
        if missing:
            print(f"skip: chart/charts/ lacks {', '.join(missing)} "
                  "(run `helm dependency update chart`)")
            raise unittest.SkipTest(f"chart dependencies not fetched: {', '.join(missing)}")

        out = subprocess.run([HELM, "template", "stardelt", "chart", "-n", "stardelt"],
                             cwd=REPO_ROOT, capture_output=True, text=True)
        if out.returncode != 0:
            raise AssertionError(out.stderr[-2000:])
        cls.chart_job = job_from(yaml.safe_load_all(out.stdout))
        with open(REPO_ROOT / "manifests" / "lakekeeper-bootstrap.yaml",
                  encoding="utf-8") as handle:
            cls.manifest_job = job_from(yaml.safe_load_all(handle))

    def test_the_chart_job_is_the_manifest_job(self):
        chart_pod = self.chart_job["spec"]["template"]["spec"]
        manifest_pod = self.manifest_job["spec"]["template"]["spec"]

        chart_containers = [c for c in chart_pod["containers"] if c["name"] == "bootstrap"]
        manifest_containers = [c for c in manifest_pod["containers"] if c["name"] == "bootstrap"]
        self.assertEqual(len(chart_containers), 1, "expected one `bootstrap` container")
        self.assertEqual(len(manifest_containers), 1, "expected one `bootstrap` container")
        chart_bootstrap = chart_containers[0]
        manifest_bootstrap = manifest_containers[0]

        self.assertEqual(chart_bootstrap["command"], manifest_bootstrap["command"])
        self.assertEqual(chart_bootstrap["args"], manifest_bootstrap["args"],
                         "the chart's bootstrap script drifted from the manifest's")
        self.assertEqual(chart_bootstrap["env"], manifest_bootstrap["env"])
        self.assertEqual(chart_bootstrap["volumeMounts"],
                         manifest_bootstrap["volumeMounts"])
        self.assertEqual(chart_pod["volumes"], manifest_pod["volumes"])

        # The client-credentials exchange is the point of the copy: a later edit
        # must not quietly turn it back into an anonymous call.
        script = chart_bootstrap["args"][0]
        self.assertIn("--data-urlencode client_secret@", script)
        self.assertIn("-H @", script)


if __name__ == "__main__":
    unittest.main()
