"""Tests for scripts/release/check_fresh_node.py (#620)."""
from __future__ import annotations

import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import check_fresh_node  # noqa: E402

SHA = "0123456789abcdef0123456789abcdef01234567"
PASS_LINE = "e2e-fresh-node: PASS (one server, created and deleted in 1s)"


class CheckFreshNodeTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)

    def write_report(self, run: str, commit: str, last: str) -> None:
        report = self.dir / run
        report.mkdir(parents=True, exist_ok=True)
        (report / "report.txt").write_text(
            f"commit: {commit}\n"
            "node type: cpx42 @ nbg1\n"
            "base domain: <none>\n"
            f"{last}\n",
            encoding="utf-8",
        )

    def run_main(self, argv: list[str]) -> tuple[int, str, str]:
        """main()'s exit code and what it printed, so the test output stays the test output."""
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = check_fresh_node.main(argv)
        return rc, out.getvalue(), err.getvalue()

    def test_a_passing_report_at_the_commit_passes(self) -> None:
        self.write_report("r1", SHA, PASS_LINE)
        rc, out, _err = self.run_main([SHA, "--dir", str(self.dir)])
        self.assertEqual(rc, 0)
        self.assertIn(f"✓ fresh-node run r1 passed at {SHA[:12]}", out)

    def test_a_failed_report_does_not_pass(self) -> None:
        self.write_report("r1", SHA, "e2e-fresh-node: FAIL (see apply and /tmp/run)")
        rc, _out, err = self.run_main([SHA, "--dir", str(self.dir)])
        self.assertEqual(rc, 1)
        self.assertIn(f"✗ no passing fresh-node run for {SHA[:12]}", err)

    def test_another_commit_does_not_pass(self) -> None:
        self.write_report("r1", "a" * 40, PASS_LINE)
        rc, _out, _err = self.run_main(["b" * 40, "--dir", str(self.dir)])
        self.assertEqual(rc, 1)

    def test_a_dirty_run_does_not_pass(self) -> None:
        self.write_report("r1", f"{SHA}-dirty", PASS_LINE)
        rc, _out, _err = self.run_main([SHA, "--dir", str(self.dir)])
        self.assertEqual(rc, 1)

    def test_a_missing_directory_is_a_plain_failure(self) -> None:
        missing = self.dir / "does-not-exist"
        rc, _out, err = self.run_main([SHA, "--dir", str(missing)])
        self.assertEqual(rc, 1)
        self.assertIn(str(missing), err)


if __name__ == "__main__":
    unittest.main()
