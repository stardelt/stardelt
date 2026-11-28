"""`step-airflow` deletes the two non-hook Jobs before the Helm upgrade (#2373).

On 2026-11-28 the loader roll failed: the triggerer Deployment timed out, `--atomic`
rolled back, and patching the completed `airflow-create-user` and
`airflow-run-airflow-migrations` Jobs failed because a Job's pod template is
immutable. Helm left the release `failed`. Deleting the Jobs before the upgrade
lets Helm create them again.
"""
import pathlib
import re
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
MAKEFILE = PLATFORM / "Makefile"
JOBS = ("airflow-create-user", "airflow-run-airflow-migrations")


def step_airflow_lines():
    """The lines of the `step-airflow` target, up to the next target."""
    lines = MAKEFILE.read_text().splitlines()
    start = next(
        index
        for index, line in enumerate(lines)
        if line.startswith("step-airflow:")
    )
    for line in lines[start + 1 :]:
        if re.match(r"^[a-z][a-z0-9-]*:", line):
            break
        yield line


class StepAirflowJobs(unittest.TestCase):
    def test_both_jobs_are_deleted_before_the_upgrade(self):
        lines = list(step_airflow_lines())
        delete = [
            index
            for index, line in enumerate(lines)
            if "kubectl -n $(NAMESPACE) delete job" in line
        ]
        self.assertEqual(1, len(delete), "step-airflow must delete the two Jobs once")
        line = lines[delete[0]]
        for job in JOBS:
            self.assertIn(job, line)
        self.assertIn("--ignore-not-found", line)
        upgrade = [
            index
            for index, line in enumerate(lines)
            if "helm upgrade --install airflow" in line
        ]
        self.assertTrue(upgrade, "step-airflow must still upgrade the release")
        self.assertLess(delete[0], upgrade[0])


if __name__ == "__main__":
    unittest.main()
