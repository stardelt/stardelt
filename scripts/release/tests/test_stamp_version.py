#!/usr/bin/env python3
"""Tests for scripts/release/stamp-version.py (stardelt/stardelt#421).

Each test copies the files the script's RULES name into a temporary directory and runs the
script there with --root, so the checkout is never written to.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / "scripts" / "release" / "stamp-version.py"

# The 11 files the RULES in stamp-version.py name, with their folders.
FILES = [
    "python/pyproject.toml",
    "python/stardelt/__init__.py",
    "python/jupyterlab/package.json",
    "operator/Cargo.toml",
    "operator/Cargo.lock",
    "operator/chart/Chart.yaml",
    "nova/Cargo.toml",
    "nova/Cargo.lock",
    "nova/frontend/package.json",
    "nova/frontend/package-lock.json",
    "platform/chart/Chart.yaml",
]


def run(root: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root), *args],
        capture_output=True,
        text=True,
    )


class StampVersionTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        for rel in FILES:
            dst = self.root / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REPO / rel, dst)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_stamp_then_check_passes(self) -> None:
        stamp = run(self.root, "0.0.7")
        self.assertEqual(stamp.returncode, 0, stamp.stderr)
        check = run(self.root, "--check", "0.0.7")
        self.assertEqual(check.returncode, 0, check.stdout + check.stderr)
        ticks = [line for line in check.stdout.splitlines() if line.startswith("✓")]
        self.assertEqual(len(ticks), 14, check.stdout)

    def test_check_reports_every_mismatch(self) -> None:
        check = run(self.root, "--check", "0.0.7")
        self.assertEqual(check.returncode, 1, check.stdout + check.stderr)
        self.assertIn("python/pyproject.toml", check.stdout)
        self.assertIn("operator/chart/Chart.yaml", check.stdout)

    def test_refuses_a_non_0_0_version(self) -> None:
        before = {rel: (self.root / rel).read_bytes() for rel in FILES}
        refused = run(self.root, "0.1.0")
        self.assertEqual(refused.returncode, 2, refused.stdout + refused.stderr)
        after = {rel: (self.root / rel).read_bytes() for rel in FILES}
        self.assertEqual(before, after)

    def test_app_version_has_the_v_prefix(self) -> None:
        stamp = run(self.root, "0.0.7")
        self.assertEqual(stamp.returncode, 0, stamp.stderr)
        chart = (self.root / "platform/chart/Chart.yaml").read_text()
        self.assertIn('appVersion: "v0.0.7"', chart)
        self.assertIn("version: 0.0.7", chart)
        self.assertIn("    version: 0.29.1", chart)


if __name__ == "__main__":
    unittest.main()
