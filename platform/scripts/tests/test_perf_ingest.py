"""Ingest rows per second: the batching, the arithmetic and the stub proof (#1548, stardelt/stardelt#53)."""

import subprocess
import sys
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]

# `perf` is a namespace directory here, and Linux ships a `perf` extension module in
# dist-packages that wins the name; import the script by its own directory instead.
sys.path.insert(0, str(PLATFORM / "perf"))

from ingest import batches_for, rows_per_second  # noqa: E402


class BatchesTest(unittest.TestCase):
    def test_batches_never_exceed_the_sequence_cap(self):
        self.assertEqual(batches_for(10000), [10000])
        self.assertEqual(batches_for(25000), [10000, 10000, 5000])
        self.assertEqual(batches_for(10001), [10000, 1])
        for rows in (0, -1):
            with self.assertRaises(SystemExit, msg=rows):
                batches_for(rows)


class RateTest(unittest.TestCase):
    def test_rows_per_second_rounds_down(self):
        self.assertEqual(rows_per_second(21008, 1.0), 21008)
        self.assertEqual(rows_per_second(1000, 0.3), 3333)
        with self.assertRaises(SystemExit):
            rows_per_second(10, 0)


class SelfTestTest(unittest.TestCase):
    def test_self_test_passes(self):
        result = subprocess.run([sys.executable, "perf/ingest.py", "--self-test"],
                                cwd=str(PLATFORM), capture_output=True, text=True, timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("self-test: PASS", result.stdout)


if __name__ == "__main__":
    unittest.main()
