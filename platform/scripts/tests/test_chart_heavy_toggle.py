"""Superset and Airflow cannot both be enabled in one release (platform#1645).

The Superset chart's bundled Redis and the Airflow chart define
`redis.serviceAccountName` with different signatures, so a render that enables
both resolves one chart's helper against the other's call site and fails inside
a vendored sub-chart with `wrong type for value`. `chart/values.yaml` and
`docs/install.md` document the limitation, but a reader who misses them gets an
error that names neither key, neither component nor the workaround.

A parent-chart `fail` template cannot refuse it: the sub-chart's templates are
rendered (and the collision resolved) before any umbrella-chart template runs.
`chart/values.schema.json` is validated before anything renders, so it refuses
the pair and names `airflow.enabled`.

Needs Helm and the sub-charts fetched into `chart/charts/*.tgz` (gitignored;
`helm dependency update chart`). The `lint` job runs `unittest discover` as
well and does not fetch them, so the module skips itself there; the
`helm-lint` job runs it after its dependency step and has both.
"""

import pathlib
import shutil
import subprocess
import unittest

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

# The `lint` job runs `unittest discover` without Helm; the `helm-lint` job has it.
HELM = shutil.which("helm")

COLLISION = "wrong type for value"


def missing_subcharts():
    """Sub-chart names `chart/Chart.yaml` declares but `chart/charts/` lacks."""
    with open(REPO_ROOT / "chart" / "Chart.yaml", encoding="utf-8") as handle:
        deps = yaml.safe_load(handle).get("dependencies") or []
    present = [p.name for p in (REPO_ROOT / "chart" / "charts").glob("*.tgz")]
    return [d["name"] for d in deps
            if not any(p.startswith(d["name"] + "-") for p in present)]


def render(*settings):
    """Render the umbrella chart with the given `--set` arguments."""
    return subprocess.run([HELM, "template", "stardelt", "chart", "-n", "stardelt",
                           *settings],
                          cwd=REPO_ROOT, capture_output=True, text=True)


class HeavyToggleTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if HELM is None:
            raise unittest.SkipTest("helm is not on PATH")
        missing = missing_subcharts()
        if missing:
            print(f"skip: chart/charts/ lacks {', '.join(missing)} "
                  "(run `helm dependency update chart`)")
            raise unittest.SkipTest(f"chart dependencies not fetched: {', '.join(missing)}")

    def test_both_enabled_refuses_with_the_key(self):
        out = render("--set", "superset.enabled=true", "--set", "airflow.enabled=true")
        self.assertEqual(out.returncode, 1, out.stderr[-2000:])
        # Helm <= 3.18 spells the refused key `airflow.enabled`; 3.19 and newer
        # use the JSON pointer `/airflow/enabled`. Both must name the key.
        self.assertRegex(out.stderr, r"airflow[./]enabled")
        self.assertNotIn(COLLISION, out.stderr,
                         "the values schema must refuse the pair before anything renders")

    def test_superset_alone_renders(self):
        out = render("--set", "superset.enabled=true")
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])

    def test_airflow_alone_renders(self):
        out = render("--set", "airflow.enabled=true")
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])


if __name__ == "__main__":
    unittest.main()
