"""One table per perf workload, newest run of each (#1549, stardelt/stardelt#53)."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

PLATFORM = Path(__file__).resolve().parents[2]

# `perf` is a namespace directory here, and Linux ships a `perf` extension module in
# dist-packages that wins the name; import the script by its own directory instead.
sys.path.insert(0, str(PLATFORM / "perf"))

from report import newest, render, report  # noqa: E402


class NewestTest(unittest.TestCase):
    def test_newest_picks_by_timestamp_then_mtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            older = root / "pipelines-2026-11-24T00:00:00Z.json"
            newer = root / "pipelines-2026-11-25T00:00:00Z.json"
            older.write_text(json.dumps({"timestamp": "2026-11-24T00:00:00Z"}))
            newer.write_text(json.dumps({"timestamp": "2026-11-25T00:00:00Z"}))
            # The older name stays older even when its mtime is the freshest.
            os.utime(older, (1_800_000_000, 1_800_000_000))
            os.utime(newer, (1_700_000_000, 1_700_000_000))
            self.assertEqual(newest(tmp, "pipelines"), str(newer))

            # Same timestamp: the later mtime wins.
            tie = root / "pipelines-2026-11-25T00:00:00Z-2.json"
            tie.write_text(json.dumps({"timestamp": "2026-11-25T00:00:00Z"}))
            os.utime(newer, (1_900_000_000, 1_900_000_000))
            os.utime(tie, (1_950_000_000, 1_950_000_000))
            self.assertEqual(newest(tmp, "pipelines"), str(tie))

            # A `<UTC>.json` without `statements` is not a smoke run.
            stray = root / "2026-11-26T00:00:00Z.json"
            stray.write_text(json.dumps({"timestamp": "2026-11-26T00:00:00Z"}))
            self.assertIsNone(newest(tmp, "smoke"))

            self.assertIsNone(newest(str(root / "missing"), "pipelines"))


class RenderTest(unittest.TestCase):
    def test_the_pre_1533_pipelines_document_still_prints(self):
        with tempfile.TemporaryDirectory() as tmp:
            document = {
                "timestamp": "2026-11-25T00:11:31Z",
                "person": "demo",
                "pipeline": "shop-1-bronze",
                "levels": [
                    {"level": 1, "runs": 1, "ok": 1, "errors": 0, "p50": 1500, "p95": 1500,
                     "max": 1500, "states": ["success"]},
                    {"level": 2, "runs": 2, "ok": 0, "errors": 2, "p50": None, "p95": None,
                     "max": None, "states": ["failed", "failed"]},
                ],
            }
            (Path(tmp) / "pipelines-2026-11-25T00:11:31Z.json").write_text(json.dumps(document))
            text = render(report(tmp))
            self.assertIn("## pipelines  2026-11-25T00:11:31Z  person=demo", text)
            self.assertIn("level 1  runs=1  ok=1  p50=1.5s  p95=1.5s  max=1.5s  shop-1-bronze", text)
            self.assertIn("p50=-", text)
            self.assertIn("## smoke: no run in %s" % tmp, text)

    def test_the_highwater_block_stops_at_five_and_marks_an_oom(self):
        with tempfile.TemporaryDirectory() as tmp:
            pods = [
                {"pod": "pod-%d" % i, "peakCpuM": 10 * i, "peakMemoryMiB": 1000 - 10 * i,
                 "restarts": 0, "oomKilled": 0}
                for i in range(6)
            ]
            pods.append({"pod": "oom-pod", "peakCpuM": 69, "peakMemoryMiB": 4096,
                         "restarts": 2, "oomKilled": 1})
            document = {"timestamp": "2026-11-24T23:22:17Z", "namespace": "stardelt",
                        "samples": 30, "childExit": 0, "pods": pods}
            (Path(tmp) / "highwater-2026-11-24T23:22:17Z.json").write_text(json.dumps(document))
            text = render(report(tmp))
            self.assertIn("## highwater  2026-11-24T23:22:17Z  namespace=stardelt  samples=30", text)
            self.assertIn("oom-pod  69m  4096Mi (!)", text)
            shown = [line for line in text.splitlines() if line.startswith("pod-") or
                     line.startswith("oom-pod")]
            self.assertEqual(len(shown), 5, text)
            self.assertIn("pod-0  0m  1000Mi", shown)
            self.assertNotIn("pod-5", "\n".join(shown))


class SelfTestTest(unittest.TestCase):
    def test_self_test_passes(self):
        result = subprocess.run(
            [sys.executable, "perf/report.py", "--self-test"],
            cwd=str(PLATFORM),
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("self-test: PASS", result.stdout)


if __name__ == "__main__":
    unittest.main()
