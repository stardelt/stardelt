"""Unit tests for scripts/check_pin_freshness.py (stardelt-platform#556).

Standard library only; discovered by `python3 -m unittest discover -s scripts/tests`.
"""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.check_pin_freshness import (
    compare,
    exit_code,
    parse_pins,
    pin_text,
)

REPO = Path(__file__).resolve().parents[2]


class ParsePinsTest(unittest.TestCase):
    def test_parse_operator_tag(self):
        text = (REPO / "helm-values/operator.yaml").read_text()
        pins = parse_pins(text, "tag")
        self.assertEqual(len(pins), 1)
        self.assertTrue(pins[0].startswith("sha256:"))
        self.assertEqual(len(pins[0]), len("sha256:") + 64)

        self.assertEqual(parse_pins("image:\n  repository: x\n  tag: dev\n", "tag"), [])

    def test_parse_superset_quoted_tag(self):
        text = (REPO / "helm-values/superset.yaml").read_text()
        pins = parse_pins(text, "tag")
        self.assertEqual(len(pins), 1)
        self.assertTrue(pins[0].startswith("sha256:"))
        self.assertEqual(len(pins[0]), len("sha256:") + 64)

    def test_parse_unquoted_tag(self):
        digest = "sha256:" + "a" * 64
        self.assertEqual(parse_pins("  tag: dev@{}\n".format(digest), "tag"), [digest])

    def test_parse_two_spark_pins(self):
        digest = "sha256:" + "a" * 64
        text = (
            "    spark.kubernetes.container.image ghcr.io/stardelt/spark:dev@{d}\n"
            "    spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID x:y\n"
            "          image: ghcr.io/stardelt/spark:dev@{d}\n"
            "          image: ghcr.io/stardelt/other:dev@{d}\n"
        ).format(d=digest)
        pins = parse_pins(text, "image")
        self.assertEqual(pins, [digest, digest])

    def test_parse_airflow_digest(self):
        text = (REPO / "helm-values/airflow.yaml").read_text()
        pins = parse_pins(text, "digest")
        self.assertEqual(len(pins), 1)
        self.assertTrue(pins[0].startswith("sha256:"))
        self.assertEqual(len(pins[0]), len("sha256:") + 64)

        digest = "sha256:" + "a" * 64
        self.assertEqual(parse_pins("  digest: {}\n".format(digest), "digest"), [digest])
        # Exactly one bare digest line: none or two are "no pin".
        self.assertEqual(parse_pins("  tag: demo\n", "digest"), [])
        self.assertEqual(
            parse_pins(
                "  digest: {}\n  digest: {}\n".format(digest, digest), "digest"
            ),
            [],
        )

    def test_parse_chart_notebook_tag(self):
        text = (REPO / "chart/values.yaml").read_text()
        pins = parse_pins(text, "tag")
        self.assertEqual(len(pins), 1)
        self.assertEqual(len(pins[0]), len("sha256:") + 64)


class CompareTest(unittest.TestCase):
    def test_compare_reports_a_stale_pin(self):
        lines = compare(
            "stardelt-operator",
            ["sha256:aa…"],
            "sha256:bb…",
            "helm-values/operator.yaml",
        )
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("✗"))
        self.assertIn("pin-image.sh stardelt-operator", lines[0])
        self.assertEqual(exit_code(lines), 1)

    def test_compare_is_silent_when_fresh(self):
        digest = "sha256:" + "c" * 64
        lines = compare("nova", [digest], digest, "manifests/nova-deployment.yaml")
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("✓"))
        self.assertEqual(exit_code(lines), 0)

    def test_compare_prints_the_pin_tag(self):
        digest = "sha256:" + "d" * 64
        lines = compare("airflow", [digest], digest, "helm-values/airflow.yaml", "demo")
        self.assertEqual(lines, ["✓ airflow demo@dddddddd… matches helm-values/airflow.yaml"])
        stale = compare(
            "airflow", ["sha256:" + "e" * 64], digest, "helm-values/airflow.yaml", "demo"
        )
        self.assertIn("demo resolves to dddddddd…", stale[0])
        self.assertNotIn(" dev@", stale[0])


class PinTextTest(unittest.TestCase):
    """A pin is read from a commit, not from whatever a rollout left in the tree."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.write("a")
        self.git("init", "-q")
        self.git("add", "pins.yaml")
        self.git("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "pin")

    def write(self, hex_digit):
        (self.dir / "pins.yaml").write_text(f"tag: dev@sha256:{hex_digit * 64}\n")

    def git(self, *args):
        return subprocess.run(
            ["git", "-C", str(self.dir), *args], capture_output=True, text=True, check=True
        ).stdout.strip()

    def test_a_clean_tree_without_a_remote_reads_head_and_says_so(self):
        text, notes = pin_text(self.dir, "pins.yaml")
        self.assertIn("a" * 64, text)
        self.assertEqual(len(notes), 1, notes)
        self.assertIn("origin/main does not resolve here, judging HEAD", notes[0])

    def test_a_dirty_file_is_named_and_the_commit_wins(self):
        self.write("b")
        text, notes = pin_text(self.dir, "pins.yaml")
        self.assertIn("a" * 64, text)
        self.assertNotIn("b" * 64, text)
        self.assertTrue(any("differs in the working tree; judging HEAD" in n for n in notes), notes)

    def test_a_file_in_a_subdirectory_is_read_from_the_commit(self):
        # An un-prefixed `git show origin/main:pins.yaml` finds the root file; the pin under
        # platform/ must come from its own commit, and the dirty root file must not be judged.
        (self.dir / "platform").mkdir()
        (self.dir / "platform/pins.yaml").write_text(f"tag: dev@sha256:{'d' * 64}\n")
        self.git("add", "platform/pins.yaml")
        self.git(
            "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "in a folder"
        )
        self.git("update-ref", "refs/remotes/origin/main", "HEAD")
        self.write("e")
        text, notes = pin_text(self.dir / "platform", "pins.yaml")
        self.assertIn("d" * 64, text)
        self.assertNotIn("a" * 64, text)
        self.assertNotIn("e" * 64, text)
        self.assertEqual(notes, [])

    def test_origin_main_wins_over_a_different_head(self):
        first = self.git("rev-parse", "HEAD")
        self.write("c")
        self.git("add", "pins.yaml")
        self.git("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "head moves")
        self.git("update-ref", "refs/remotes/origin/main", first)
        text, notes = pin_text(self.dir, "pins.yaml")
        self.assertIn("a" * 64, text)
        self.assertNotIn("c" * 64, text)
        self.assertTrue(any("judging origin/main" in n for n in notes), notes)


    def test_an_explicit_rev_wins_over_origin_main(self):
        first = self.git("rev-parse", "HEAD")
        self.write("c")
        self.git("add", "pins.yaml")
        self.git(
            "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "head moves"
        )
        self.git("update-ref", "refs/remotes/origin/main", "HEAD")
        text, notes = pin_text(self.dir, "pins.yaml", first)
        self.assertIn("a" * 64, text)
        self.assertNotIn("c" * 64, text)
        self.assertTrue(any("judging " + first[:7] in n for n in notes), notes)

    def test_an_explicit_rev_with_a_clean_tree_prints_no_notes(self):
        text, notes = pin_text(self.dir, "pins.yaml", "HEAD")
        self.assertIn("a" * 64, text)
        self.assertEqual(notes, [])

    def test_an_unresolvable_rev_says_so_and_judges_the_working_tree(self):
        self.write("f")
        text, notes = pin_text(self.dir, "pins.yaml", "no-such-ref")
        self.assertIn("f" * 64, text)
        self.assertTrue(any("does not resolve, judging the working tree" in n for n in notes), notes)


if __name__ == "__main__":
    unittest.main()
