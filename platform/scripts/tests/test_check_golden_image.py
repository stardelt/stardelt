"""Unit tests for scripts/check_golden_image.py (stardelt-platform#1711).

Standard library only; discovered by `python3 -m unittest discover -s scripts/tests`.
"""

import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.check_golden_image import (
    compare,
    compare_source,
    nova_image_at,
    nova_image_of,
    nova_pin,
    operator_pin,
    revision_of_config,
)

REPO = Path(__file__).resolve().parents[2]

DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64


class OperatorPinTest(unittest.TestCase):
    def test_real_operator_values_has_one_pin(self):
        pins = operator_pin((REPO / "helm-values/operator.yaml").read_text())
        self.assertEqual(len(pins), 1)
        self.assertEqual(len(pins[0]), len("sha256:") + 64)
        self.assertTrue(pins[0].startswith("sha256:"))

    def test_second_tag_line_is_not_the_operator_pin(self):
        text = (
            "image:\n"
            "  repository: ghcr.io/stardelt/stardelt-operator\n"
            "  tag: dev@{a}\n"
            "bundled:\n"
            "  tag: v0.1.0\n"
            "  nested:\n"
            "    tag: 16.4\n"
        ).format(a=DIGEST_A)
        self.assertEqual(operator_pin(text), [DIGEST_A])

    def test_no_dev_tag_is_no_pin(self):
        self.assertEqual(operator_pin("image:\n  tag: dev\n"), [])


class NovaPinTest(unittest.TestCase):
    def test_real_nova_manifest_has_one_pin(self):
        pins = nova_pin((REPO / "manifests/nova-deployment.yaml").read_text())
        self.assertEqual(len(pins), 1)
        self.assertEqual(len(pins[0]), len("sha256:") + 64)
        self.assertTrue(pins[0].startswith("sha256:"))

    def test_other_image_lines_are_not_the_nova_pin(self):
        text = (
            "          image: ghcr.io/stardelt/spark:dev@{a}\n"
            "          image: ghcr.io/stardelt/nova:dev@{b}\n"
        ).format(a=DIGEST_A, b=DIGEST_B)
        self.assertEqual(nova_pin(text), [DIGEST_B])


class NovaImageOfTest(unittest.TestCase):
    def test_constant_next_to_another_digest(self):
        text = (
            "pub const OTHER_IMAGE: &str = \"ghcr.io/stardelt/other:dev@{a}\";\n"
            'pub const NOVA_IMAGE: &str = "ghcr.io/stardelt/nova:dev@{b}";\n'
        ).format(a=DIGEST_A, b=DIGEST_B)
        self.assertEqual(nova_image_of(text), DIGEST_B)

    def test_missing_constant_is_empty(self):
        self.assertEqual(nova_image_of('pub const OTHER: &str = "x";\n'), "")


class RevisionOfConfigTest(unittest.TestCase):
    def test_label_is_read(self):
        blob = (
            '{"config": {"Labels": {"org.opencontainers.image.revision": "%s"}}}' % ("c" * 40)
        )
        self.assertEqual(revision_of_config(blob), "c" * 40)

    def test_missing_labels_are_empty(self):
        self.assertEqual(revision_of_config('{"config": {}}'), "")
        self.assertEqual(revision_of_config("not json"), "")


class CompareTest(unittest.TestCase):
    def test_equal_digests_pass(self):
        lines, code = compare(DIGEST_A, "b" * 40, DIGEST_A, DIGEST_A)
        self.assertEqual(code, 0)
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("✓"))
        self.assertIn("revision bbbbbbb", lines[0])

    def test_mismatch_fails_and_names_both_digests(self):
        lines, code = compare(DIGEST_A, "b" * 40, DIGEST_B, DIGEST_A)
        self.assertEqual(code, 1)
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("✗"))
        self.assertIn("bbbbbbbb…", lines[0])
        self.assertIn("aaaaaaaa…", lines[0])

    def test_empty_input_is_unreadable(self):
        lines, code = compare("", "b" * 40, DIGEST_A, DIGEST_A)
        self.assertEqual(code, 3)
        self.assertTrue(lines[0].startswith("✗"))


class CompareSourceTest(unittest.TestCase):
    def test_local_source_with_this_tree_s_pin_passes(self):
        lines, code = compare_source(DIGEST_A, DIGEST_A)
        self.assertEqual(code, 0)
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("✓"))
        self.assertIn("aaaaaaaa…", lines[0])

    def test_a_pin_the_source_does_not_carry_fails_and_names_both_digests(self):
        lines, code = compare_source(DIGEST_A, DIGEST_B)
        self.assertEqual(code, 1)
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith("✗"))
        self.assertIn("aaaaaaaa…", lines[0])
        self.assertIn("bbbbbbbb…", lines[0])


class NovaImageAtTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.repo = Path(self._tmp.name)
        provision = self.repo / "crates/stardelt-operator/src/provision.rs"
        provision.parent.mkdir(parents=True)
        provision.write_text(
            'pub const NOVA_IMAGE: &str = "ghcr.io/stardelt/nova:dev@%s";\n' % DIGEST_A
        )
        for command in (
            ["git", "init", "-q", "-b", "main"],
            ["git", "add", "-A"],
            [
                "git",
                "-c",
                "user.email=test@example.com",
                "-c",
                "user.name=test",
                "commit",
                "-q",
                "-m",
                "fixture",
            ],
        ):
            subprocess.run(command, cwd=self.repo, check=True, capture_output=True)
        self.revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=self.repo,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def test_reads_the_constant_at_a_revision(self):
        self.assertEqual(nova_image_at(str(self.repo), self.revision), (DIGEST_A, ""))

    def test_unknown_revision_returns_an_error_and_does_not_raise(self):
        digest, err = nova_image_at(str(self.repo), "0" * 40)
        self.assertEqual(digest, "")
        self.assertTrue(err)


if __name__ == "__main__":
    unittest.main()
