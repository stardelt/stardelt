"""Tests for scripts/release/release_notes.py (#428)."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import release_notes  # noqa: E402

FALLBACK = release_notes.FALLBACK

TWO_SECTIONS = """\
# Changelog

## v0.0.4
- the release workflow publishes without a person
- a second change

## v0.0.3
- an older change
"""


class SectionTest(unittest.TestCase):
    def test_a_section_in_the_middle_of_two(self) -> None:
        text = release_notes.notes("0.0.4", TWO_SECTIONS)
        self.assertIn("## v0.0.4\n- the release workflow publishes without a person\n- a second change", text)
        self.assertNotIn("an older change", text)
        self.assertIn("releases/download/v0.0.4/install.yaml", text)

    def test_the_last_section_of_a_file(self) -> None:
        text = release_notes.notes("0.0.3", TWO_SECTIONS)
        self.assertIn("## v0.0.3\n- an older change", text)
        self.assertNotIn("a second change", text)
        self.assertIn("releases/download/v0.0.3/install.yaml", text)

    def test_a_missing_section_prints_the_fallback_and_the_install_block(self) -> None:
        text = release_notes.notes("0.0.9", TWO_SECTIONS)
        self.assertIn(FALLBACK, text)
        self.assertNotIn("## v0.0.9", text)
        self.assertIn("## Install", text)
        self.assertIn("kubectl apply --server-side --force-conflicts", text)
        self.assertIn("releases/download/v0.0.9/platform-minimal.yaml", text)


class RepositoryChangelogTest(unittest.TestCase):
    def test_the_root_changelog_carries_the_0_0_4_section(self) -> None:
        root = Path(__file__).resolve().parents[3]
        text = (root / "CHANGELOG.md").read_text()
        self.assertTrue(text.startswith("# Changelog"))
        self.assertNotIn(release_notes.FALLBACK, release_notes.notes("0.0.4", text))
        section = release_notes.section(text, "0.0.4")
        for heading in ("### Breaking", "### Highlights", "### Upgrade"):
            self.assertIn(heading, section)
        self.assertIn("spark-4.1-py3.13", section)

    def test_no_older_release_section_is_invented(self) -> None:
        root = Path(__file__).resolve().parents[3]
        text = (root / "CHANGELOG.md").read_text()
        self.assertNotIn("## v0.0.3", text, "there is no v0.0.3 release; do not invent one")


if __name__ == "__main__":
    unittest.main()
