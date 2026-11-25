"""The concurrency harness's row shaping, without a cluster (stardelt/stardelt#1568)."""

import sys
import unittest
from pathlib import Path

# `perf` is a namespace directory here, and Linux ships a `perf` extension module in
# dist-packages that wins the name; import the script by its own directory instead.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from concurrency import DEFAULT_LEVELS, _line, document, level_row, parse_levels  # noqa: E402
from pipelines import summarize  # noqa: E402


class ConcurrencyTest(unittest.TestCase):
    def test_an_empty_summary_has_no_percentile_and_renders_as_dash(self):
        self.assertIsNone(summarize([])["p50"])
        self.assertIn("p50=-", _line(level_row(4, [("failed", None)] * 4)))

    def test_a_level_counts_its_error_and_times_only_the_successes(self):
        row = level_row(1, [("SUCCEEDED", 100), ("SUCCEEDED", 300), ("http-429", None)])
        self.assertEqual(row["runs"], 3)
        self.assertEqual(row["errors"], 1)
        self.assertEqual(row["p50"], 100)
        self.assertEqual(row["max"], 300)

    def test_levels_parse_and_an_unset_list_is_the_default(self):
        self.assertEqual(parse_levels("1,2,4"), [1, 2, 4])
        self.assertEqual(parse_levels(""), parse_levels(DEFAULT_LEVELS))

    def test_the_document_carries_timestamp_person_statement_and_levels(self):
        levels = [level_row(1, [("SUCCEEDED", 480)])]
        document_ = document("2026-11-25T00:12:34Z", "https://alpha.dev.stardelt.io", "demo", 5,
                             levels)
        self.assertEqual(document_["timestamp"], "2026-11-25T00:12:34Z")
        self.assertEqual(document_["person"], "demo")
        self.assertEqual(document_["statement"], "cpu")
        self.assertEqual(document_["levels"], levels)
        self.assertIn("one signed-in person", document_["note"])


if __name__ == "__main__":
    unittest.main()
