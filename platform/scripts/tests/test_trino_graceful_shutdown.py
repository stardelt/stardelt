"""A Trino worker's pod grace outlasts the drain Trino runs itself (platform#1205).

Trino 484's worker shutdown is two phases of `shutdown.grace-period` plus the task time
(measured 2026-11-22: 2 x 120 s + tasks ~= 245 s), so a pod grace at the chart's minimum of
2 x the grace period let the kubelet kill the container in the second phase. Both values
files are read as text: the indentation around the two keys is stable.
"""

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FILES = {
    "platform": ROOT / "helm-values" / "trino.yaml",
    "operator twin": ROOT.parent
    / "operator"
    / "crates"
    / "stardelt-operator"
    / "values"
    / "trino.yaml",
}

POD_GRACE_RE = re.compile(r"^\s*terminationGracePeriodSeconds:\s*(\d+)\s*$", re.MULTILINE)
GRACE_RE = re.compile(r"^\s*gracePeriodSeconds:\s*(\d+)\s*$", re.MULTILINE)

# The chart's minimum is 2 x the grace period; the headroom is what leaves the task phase
# its ~50 s, and a later edit back to the minimum must fail here.
MIN_HEADROOM = 30


class TrinoGracefulShutdownTest(unittest.TestCase):
    def read(self, name):
        return FILES[name].read_text(encoding="utf-8")

    def numbers(self, name):
        text = self.read(name)
        pod = POD_GRACE_RE.search(text)
        grace = GRACE_RE.search(text)
        self.assertIsNotNone(pod, f"{name}: no terminationGracePeriodSeconds")
        self.assertIsNotNone(grace, f"{name}: no gracePeriodSeconds")
        return int(pod.group(1)), int(grace.group(1))

    def test_both_values_files_give_the_worker_300_seconds_and_120_s_of_grace(self):
        for name in FILES:
            pod, grace = self.numbers(name)
            self.assertEqual(pod, 300, name)
            self.assertEqual(grace, 120, name)

    def test_the_pod_grace_leaves_the_task_phase_headroom(self):
        for name in FILES:
            pod, grace = self.numbers(name)
            self.assertGreaterEqual(
                pod,
                2 * grace + MIN_HEADROOM,
                f"{name}: {pod}s is the chart's minimum {2 * grace}s plus nothing — "
                "the kubelet kills the worker in its second shutdown phase",
            )

    def test_the_comment_names_the_two_phase_arithmetic(self):
        for name in FILES:
            text = self.read(name)
            self.assertIn(
                "2 x `shutdown.grace-period`",
                text,
                f"{name}: the comment must state that the worker's drain is "
                "2 x shutdown.grace-period plus task time",
            )


if __name__ == "__main__":
    unittest.main()
