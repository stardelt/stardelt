"""Unit tests for scripts/check_golden_values.py (#516, the #330 sequencing trap).

Standard library only; discovered by `python3 -m unittest discover -s scripts/tests`.
"""

import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.check_golden_values import at_revision, compare, golden_paths

DIGEST_A = "sha256:" + "a" * 64
REVISION = "b" * 40


def _operator_checkout() -> bool:
    """`../operator` is a git checkout (the sibling folder, or the monorepo's folder)."""
    if not Path("../operator").is_dir():
        return False
    proc = subprocess.run(
        ["git", "-C", "../operator", "rev-parse", "--is-inside-work-tree"],
        capture_output=True,
        text=True,
    )
    return proc.returncode == 0 and proc.stdout.strip() == "true"


class CompareTest(unittest.TestCase):
    def test_a_match_names_the_count_and_the_revision(self):
        lines, code = compare(DIGEST_A, REVISION, [], 21)
        self.assertEqual(code, 0)
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("✓"))
        self.assertIn("21 files match", lines[0])
        self.assertIn("revision bbbbbbb", lines[0])
        self.assertIn("aaaaaaaa…", lines[0])

    def test_a_mismatch_names_every_file(self):
        mismatched = [
            "crates/stardelt-operator/values/airflow.yaml",
            "crates/stardelt-operator/values/jupyterhub.yaml",
        ]
        lines, code = compare(DIGEST_A, REVISION, mismatched, 21)
        self.assertEqual(code, 1)
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("✗"))
        self.assertIn("2 of 21 differ", lines[0])
        for path in mismatched:
            self.assertIn(path, lines[0])
        self.assertIn("re-run the rollup after it lands", lines[0])

    def test_three_when_the_image_is_unreadable(self):
        lines, code = compare("", "", [], 0)
        self.assertEqual(code, 3)
        self.assertTrue(lines[0].startswith("✗"))


class AtRevisionTest(unittest.TestCase):
    def test_at_revision_reads_an_older_commit(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            path = "crates/stardelt-operator/values/airflow.yaml"
            golden = repo / path
            golden.parent.mkdir(parents=True)
            golden.write_text("a")
            for command in (["git", "init", "-q", "-b", "main"], ["git", "add", "-A"], [
                "git",
                "-c",
                "user.email=test@example.com",
                "-c",
                "user.name=test",
                "commit",
                "-q",
                "-m",
                "a",
            ]):
                subprocess.run(command, cwd=repo, check=True, capture_output=True)
            first = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=repo, check=True,
                capture_output=True, text=True,
            ).stdout.strip()
            golden.write_text("b")
            subprocess.run(
                ["git", "-c", "user.email=test@example.com", "-c", "user.name=test",
                 "commit", "-q", "-am", "b"],
                cwd=repo, check=True, capture_output=True,
            )
            self.assertEqual(at_revision(str(repo), first, path), "a")
            self.assertEqual(golden.read_text(), "b")


class GoldenPathsTest(unittest.TestCase):
    @unittest.skipUnless(_operator_checkout(), "../operator is not a git checkout")
    def test_the_real_operator_tree_has_golden_files(self):
        paths, err = golden_paths("../operator", "HEAD")
        self.assertEqual(err, "")
        self.assertTrue(paths)
        for path in paths:
            self.assertTrue(path.startswith("crates/stardelt-operator/"), path)


if __name__ == "__main__":
    unittest.main()
