"""The comparison verdict, without a file system (stardelt/stardelt#1522)."""

import sys
import unittest
from pathlib import Path

# `perf` is a namespace directory here, and Linux ships a `perf` extension module in
# dist-packages that wins the name; import the script by its own directory instead.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from compare import ratio, verdict  # noqa: E402


class VerdictTest(unittest.TestCase):
    def test_double_the_baseline_is_ok_under_three(self):
        self.assertEqual(verdict(928, 464, 3.0), "ok")
        self.assertEqual(ratio(928, 464), 2.0)

    def test_four_times_the_baseline_is_slow(self):
        self.assertEqual(verdict(1856, 464, 3.0), "slow")
        self.assertEqual(ratio(1856, 464), 4.0)

    def test_missing_p50_is_missing_not_zero(self):
        self.assertEqual(verdict(None, 464, 3.0), "missing")
        self.assertEqual(verdict(0, 464, 3.0), "missing")
        self.assertIsNone(ratio(0, 464))


if __name__ == "__main__":
    unittest.main()
