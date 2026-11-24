"""Peak CPU/memory per pod: the parsing and the canned proof (#1538, stardelt/stardelt#53)."""

import sys
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]

# `perf` is a namespace directory here, and Linux ships a `perf` extension module in
# dist-packages that wins the name; import the script by its own directory instead.
sys.path.insert(0, str(PLATFORM / "perf"))

from highwater import parse_top, self_test, summarize  # noqa: E402

CANNED = """\
spark-connect-6d9f8c9f7f-abcde      6m      363Mi
trino-coordinator-55c95f5bdd-26mwc  1       2Gi
airflow-scheduler-7944b58999-nhj5h  512Ki   1Ki
airflow-api-server-75586c4b67-tb5vk  <unknown>   <unknown>
airflow-postgresql-0                 41m     67Mi   stardelt
"""


class ParseTopTest(unittest.TestCase):
    def test_parse_top_reads_millicores_and_mebibytes(self):
        pods = parse_top(CANNED)
        self.assertEqual(pods["spark-connect-6d9f8c9f7f-abcde"], (6, 363))
        self.assertEqual(pods["trino-coordinator-55c95f5bdd-26mwc"], (1000, 2048))
        # '512Ki' / '1Ki' round up to the next unit, never to zero.
        self.assertEqual(pods["airflow-scheduler-7944b58999-nhj5h"], (1, 1))
        # '<unknown>' is no sample at all.
        self.assertNotIn("airflow-api-server-75586c4b67-tb5vk", pods)
        # A trailing namespace column does not shift the numbers.
        self.assertEqual(pods["airflow-postgresql-0"], (41, 67))


class SummarizeTest(unittest.TestCase):
    def test_summarize_takes_the_peak_not_the_last(self):
        samples = [
            {"trino-coordinator-55c95f5bdd-26mwc": (120, 812)},
            {"trino-coordinator-55c95f5bdd-26mwc": (40, 300)},
        ]
        self.assertEqual(
            summarize(samples),
            [
                {
                    "pod": "trino-coordinator-55c95f5bdd-26mwc",
                    "peakCpuM": 120,
                    "peakMemoryMiB": 812,
                    "restarts": 0,
                    "oomKilled": 0,
                }
            ],
        )

    def test_summarize_keeps_a_disappeared_pod(self):
        samples = [
            {"spark-connect-6d9f8c9f7f-abcde": (12, 1590)},
            {"trino-coordinator-55c95f5bdd-26mwc": (120, 812)},
        ]
        rows = summarize(samples, {"spark-connect-6d9f8c9f7f-abcde": {"restarts": 1, "oomKilled": 1}})
        # Memory decides the order, so the pod that vanished still heads the table.
        self.assertEqual(
            [row["pod"] for row in rows],
            ["spark-connect-6d9f8c9f7f-abcde", "trino-coordinator-55c95f5bdd-26mwc"],
        )
        spark = rows[0]
        self.assertEqual((spark["peakCpuM"], spark["peakMemoryMiB"]), (12, 1590))
        self.assertEqual((spark["restarts"], spark["oomKilled"]), (1, 1))
        self.assertEqual((rows[1]["restarts"], rows[1]["oomKilled"]), (0, 0))


class SelfTestTest(unittest.TestCase):
    def test_self_test_passes(self):
        self.assertEqual(self_test(), 0)


if __name__ == "__main__":
    unittest.main()
