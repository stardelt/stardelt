"""Unit tests for scripts/check_twins.py (#1791, internal#545 finding 1).

Standard library only; discovered by `python3 -m unittest discover -s scripts/tests`.
"""

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.check_twins import main

HEADER = "# kind\toperator path\tplatform path\n"
OPERATOR_SRC = "operator/crates/stardelt-operator/src"
OPERATOR_VALUES = "operator/crates/stardelt-operator/values"
PLATFORM_VALUES = "platform/helm-values"
SCRIPT = "platform/scripts/superset-group-roles.py"
SCRIPT_COPY = OPERATOR_SRC + "/superset_group_roles.py"
BODY = '#!/usr/bin/env python3\n"""Create a role per group."""\nprint("hi")\n'


def write(root: Path, path: str, text: str) -> None:
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def run(root: Path) -> tuple:
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        code = main(root=root)
    return code, out.getvalue()


class CheckTwinsTest(unittest.TestCase):
    def test_known_rows_and_equal_copies_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(
                root,
                "platform/twins.tsv",
                HEADER
                + "exact\t%s\t%s\n" % (SCRIPT_COPY, SCRIPT)
                + "drift\t%s/airflow.yaml\t%s/airflow.yaml\n" % (OPERATOR_VALUES, PLATFORM_VALUES),
            )
            write(root, SCRIPT, BODY)
            write(
                root,
                SCRIPT_COPY,
                "# Copy of platform/scripts/superset-group-roles.py (#494), embedded by\n"
                "# Edit the platform file first, then re-copy.\n" + BODY,
            )
            write(root, OPERATOR_VALUES + "/airflow.yaml", "a: 1\n")
            write(root, PLATFORM_VALUES + "/airflow.yaml", "a: 2\n")

            code, out = run(root)

            self.assertEqual(code, 0, out)
            self.assertIn("✓ twins: 1 exact, 1 drift, no new copy", out)

    def test_an_exact_copy_that_drifted_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root, "platform/twins.tsv", HEADER + "exact\t%s\t%s\n" % (SCRIPT_COPY, SCRIPT))
            write(root, SCRIPT, BODY)
            write(root, SCRIPT_COPY, "# Copy of platform/scripts/superset-group-roles.py\n" + BODY + 'print("new")\n')

            code, out = run(root)

            self.assertEqual(code, 1, out)
            self.assertIn("drifted", out)

    def test_a_new_values_copy_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root, "platform/twins.tsv", HEADER)
            write(root, OPERATOR_VALUES + "/trino.yaml", "a: 1\n")
            write(root, PLATFORM_VALUES + "/trino.yaml", "a: 2\n")

            code, out = run(root)

            self.assertEqual(code, 1, out)
            self.assertIn("new copy", out)
            self.assertIn("operator/crates/stardelt-operator/values/trino.yaml", out)

    def test_a_new_script_copy_is_found_by_its_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root, "platform/twins.tsv", HEADER)
            write(root, SCRIPT, BODY)
            write(
                root,
                SCRIPT_COPY,
                "# Copy of stardelt-platform/scripts/superset-group-roles.py (#494),\n"
                "# embedded by crates/stardelt-operator/src/superset.rs.\n" + BODY,
            )

            code, out = run(root)

            self.assertEqual(code, 1, out)
            self.assertIn("new copy", out)
            self.assertIn(SCRIPT, out)

    def test_a_new_script_copy_without_a_header_is_found_by_its_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(root, "platform/twins.tsv", HEADER)
            write(root, "platform/scripts/access-sync-job.py", BODY)
            write(root, OPERATOR_SRC + "/access_sync_job.py", BODY)

            code, out = run(root)

            self.assertEqual(code, 1, out)
            self.assertIn("new copy", out)
            self.assertIn("platform/scripts/access-sync-job.py", out)

    def test_a_header_less_copy_with_its_row_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            row = "\t".join(
                [
                    "exact",
                    OPERATOR_SRC + "/access_sync_job.py",
                    "platform/scripts/access-sync-job.py",
                ]
            )
            write(root, "platform/twins.tsv", HEADER + row + "\n")
            write(root, "platform/scripts/access-sync-job.py", BODY)
            write(root, OPERATOR_SRC + "/access_sync_job.py", BODY)

            code, out = run(root)

            self.assertEqual(code, 0, out)
            self.assertIn("✓ twins: 1 exact, 0 drift, no new copy", out)

    def test_a_row_whose_file_is_gone_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write(
                root,
                "platform/twins.tsv",
                HEADER + "exact\t%s\t%s\n" % (OPERATOR_VALUES + "/gone.yaml", PLATFORM_VALUES + "/gone.yaml"),
            )

            code, out = run(root)

            self.assertEqual(code, 1, out)
            self.assertIn("stale row", out)


if __name__ == "__main__":
    unittest.main()
