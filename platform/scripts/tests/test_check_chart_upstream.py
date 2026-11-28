"""Unit tests for scripts/check_chart_upstream.py (stardelt/stardelt#2239).

Standard library only; discovered by `python3 -m unittest discover -s scripts/tests`.
The network is never needed: every fetch is injected.
"""

import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import yaml

from scripts.check_chart_upstream import (
    dependencies,
    evaluate,
    exit_code,
    index_url,
    main,
    report,
    rows_for,
)

REPO = Path(__file__).resolve().parents[2]

INDEX = """
entries:
  superset:
    - version: 0.22.8
      appVersion: 6.1.0
      deprecated: true
  trino:
    - version: 1.45.0
    - version: 1.44.0
"""


def entries():
    return yaml.safe_load(INDEX)["entries"]


class EvaluateTest(unittest.TestCase):
    def test_a_pinned_deprecated_chart_is_present_and_reported(self):
        row = evaluate("superset", "0.22.8", entries())
        self.assertTrue(row["present"])
        self.assertTrue(row["deprecated"])
        self.assertEqual(row["newest"], "0.22.8")
        self.assertEqual(row["app_version"], "6.1.0")

    def test_a_published_chart_is_not_deprecated(self):
        row = evaluate("trino", "1.45.0", entries())
        self.assertTrue(row["present"])
        self.assertFalse(row["deprecated"])
        self.assertEqual(row["newest"], "1.45.0")

    def test_a_chart_absent_from_the_index_is_missing(self):
        row = evaluate("airflow", "1.22.0", {})
        self.assertFalse(row["present"])
        self.assertIsNone(row["newest"])
        self.assertFalse(row["deprecated"])

    def test_a_pin_the_index_no_longer_carries_is_missing(self):
        row = evaluate("trino", "9.9.9", entries())
        self.assertFalse(row["present"])
        self.assertEqual(row["newest"], "1.45.0")


class RowsTest(unittest.TestCase):
    def test_a_fetch_failure_becomes_an_error_row(self):
        def broken(url, timeout=20):
            raise OSError("name resolution failed\nsecond line")

        rows = rows_for([("trino", "https://example.invalid/charts", "1.45.0")],
                        fetch=broken)
        self.assertEqual(len(rows), 1)
        self.assertIn("error", rows[0])
        self.assertEqual(rows[0]["error"], "name resolution failed")
        self.assertEqual(rows[0]["url"], "https://example.invalid/charts/index.yaml")

    def test_the_index_url_has_one_slash(self):
        self.assertEqual(index_url("https://apache.github.io/superset/"),
                         "https://apache.github.io/superset/index.yaml")
        self.assertEqual(index_url("https://airflow.apache.org"),
                         "https://airflow.apache.org/index.yaml")


class ExitCodeTest(unittest.TestCase):
    def test_zero_when_every_row_is_published(self):
        rows = [evaluate("trino", "1.45.0", entries()),
                evaluate("superset", "0.22.8", entries())]
        self.assertEqual(exit_code(rows), 0)

    def test_one_when_a_row_is_missing(self):
        rows = [evaluate("trino", "1.45.0", entries()),
                evaluate("airflow", "1.22.0", {})]
        self.assertEqual(exit_code(rows), 1)

    def test_three_when_a_repository_could_not_be_read(self):
        rows = [{"name": "trino", "version": "1.45.0", "error": "timeout"}]
        self.assertEqual(exit_code(rows), 3)

    def test_missing_outranks_unreadable(self):
        rows = [{"name": "trino", "version": "1.45.0", "error": "timeout"},
                evaluate("airflow", "1.22.0", {})]
        self.assertEqual(exit_code(rows), 1)


class ReportTest(unittest.TestCase):
    def test_the_three_shapes_and_the_summary(self):
        missing = evaluate("airflow", "1.22.0", {})
        missing["url"] = "u"
        rows = [evaluate("trino", "1.45.0", entries()),
                evaluate("superset", "0.22.8", entries()),
                missing,
                {"name": "lakekeeper", "version": "0.14.0", "url": "u", "error": "boom"}]
        out = io.StringIO()
        with redirect_stdout(out):
            report(rows)
        text = out.getvalue()
        self.assertIn("✓ trino 1.45.0 published (newest 1.45.0)", text)
        self.assertIn("! superset 0.22.8 is deprecated upstream (newest 0.22.8)", text)
        self.assertIn("✗ airflow 1.22.0 is not in u (newest none)", text)
        self.assertIn("? lakekeeper 0.14.0 UNVERIFIED: boom", text)
        # A missing row outranks an unreadable one, exactly as exit_code does.
        self.assertIn("check-chart-upstream: FAIL (1 missing of 4)", text)

    def test_a_deprecation_does_not_change_the_summary(self):
        rows = [evaluate("superset", "0.22.8", entries())]
        out = io.StringIO()
        with redirect_stdout(out):
            report(rows)
        self.assertIn("check-chart-upstream: PASS (1 of 1 published; 1 deprecated)",
                      out.getvalue())


class MainTest(unittest.TestCase):
    def test_an_unreachable_repository_answers_three_and_says_unverified(self):
        def broken(url, timeout=20):
            raise OSError("no network")

        out = io.StringIO()
        with redirect_stdout(out):
            code = main(["--chart", "trino"], fetch=broken)
        self.assertEqual(code, 3)
        self.assertIn("UNVERIFIED", out.getvalue())
        self.assertIn("check-chart-upstream: UNVERIFIED (1 of 1 not readable)", out.getvalue())

    def test_an_unknown_chart_is_a_usage_error(self):
        with redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as raised:
            main(["--chart", "nope"], fetch=lambda url, timeout=20: "")
        self.assertEqual(raised.exception.code, 2)


class DependenciesTest(unittest.TestCase):
    @unittest.skipUnless((REPO / "chart/Chart.yaml").is_file(),
                         "run from platform/ (chart/Chart.yaml is relative)")
    def test_the_eight_pinned_charts(self):
        deps = dependencies("chart/Chart.yaml")
        self.assertEqual([name for name, _, _ in deps], [
            "cloudnative-pg", "plugin-barman-cloud", "seaweedfs", "lakekeeper",
            "trino", "superset", "airflow", "jupyterhub",
        ])
        self.assertEqual(deps[5], ("superset", "https://apache.github.io/superset",
                                   "0.22.8"))


if __name__ == "__main__":
    unittest.main()
