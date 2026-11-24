"""N pipeline runs at once: the arithmetic and the stub proof (#1533, stardelt/stardelt#53)."""

import subprocess
import sys
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]

# `perf` is a namespace directory here, and Linux ships a `perf` extension module in
# dist-packages that wins the name; import the script by its own directory instead.
sys.path.insert(0, str(PLATFORM / "perf"))

from pipelines import parse_levels, percentile, summarize  # noqa: E402


class PercentileTest(unittest.TestCase):
    def test_percentile_nearest_rank(self):
        # Nearest rank: the ceil(percent * n / 100)-th value of the sorted list.
        self.assertEqual(percentile([7], 50), 7)
        self.assertEqual(percentile([7], 95), 7)
        self.assertEqual(percentile([2, 1], 50), 1)
        self.assertEqual(percentile([2, 1], 95), 2)
        ten = list(range(1, 11))
        self.assertEqual(percentile(ten, 50), 5)
        self.assertEqual(percentile(ten, 95), 10)
        self.assertIsNone(percentile([], 95))

    def test_summarize_marks_the_slow_run(self):
        summary = summarize([10, 20, 400])
        self.assertEqual(summary, {"runs": 3, "p50": 20, "p95": 400, "max": 400})
        self.assertEqual(summarize([420]), {"runs": 1, "p50": 420, "p95": 420, "max": 420})
        self.assertEqual(summarize([]), {"runs": 0, "p50": None, "p95": None, "max": None})


class ParseLevelsTest(unittest.TestCase):
    def test_parse_levels_rejects_zero_and_eleven(self):
        self.assertEqual(parse_levels("1,2"), [1, 2])
        self.assertEqual(parse_levels(" 1 , 10 "), [1, 10])
        self.assertEqual(parse_levels("0"), [])
        for raw in ("0,2", "-1", "11", "1,,2", "two"):
            with self.assertRaises(SystemExit, msg=raw):
                parse_levels(raw)


class SelfTestTest(unittest.TestCase):
    def test_self_test_passes(self):
        result = subprocess.run(
            [sys.executable, "perf/pipelines.py", "--self-test"],
            cwd=str(PLATFORM),
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("self-test: PASS", result.stdout)


if __name__ == "__main__":
    unittest.main()
