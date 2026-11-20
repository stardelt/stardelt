"""Unit tests for scripts/verify-operator-quiet.sh (stardelt/stardelt#453, #274 rule 7).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl` is replaced by a shell shim first on `PATH`; it answers from files the
test writes (`objects.txt`, `top.txt`, `metrics.txt`) and from a counter for the kubelet stats
summary, so no cluster is touched and both the failure case (a resourceVersion moves) and the
honest "proved nothing" case (an empty metrics endpoint) are proven without breaking a live object.

Every case runs with `WINDOW=0`: the script sleeps zero seconds and reports the rows it measured.
"""

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-operator-quiet.sh"

# Fake kubectl. Records every call, then answers:
#   - `get pods -l app=stardelt-operator` -> the pod names in $FAKE_DIR/pods.txt
#   - `get <kind> -o jsonpath=...`        -> objects.txt, and from the 2nd call on
#                                            objects_second.txt when the test wrote one
#   - `get pod <name> -o jsonpath=...`    -> the single node name `node-1`
#   - `get --raw <url>`                   -> metrics.txt when the URL names `pods/`,
#                                            and metrics_second.txt once the stats summary
#                                            has been read twice (that is after the script's
#                                            sleep), so a counter can move inside the window;
#                                            otherwise a stats summary that grows 1 KiB a call
#   - `top pods ...`                      -> top.txt
SHIM = """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "${1-}" >> "$FAKE_DIR/kubectl_calls.log"
printf '%s\\n' "$*" >> "$FAKE_DIR/kubectl_args.log"
dir="$FAKE_DIR"

if [ "${1-}" = "top" ]; then
  cat "$dir/top.txt"
  exit 0
fi

args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n | --namespace | -o | --output | -l | --selector) shift 2 ;;
    *) args+=("$1"); shift ;;
  esac
done
verb="${args[0]-}"
command_rest="${args[1]-}"

if [ "$command_rest" = "--raw" ]; then
  # `kubectl get --raw <url>`
  url="${args[2]-}"
  case "$url" in
    *pods/*)
      if [ -f "$dir/metrics_second.txt" ] && [ "$(cat "$dir/stats_calls" 2>/dev/null || echo 0)" -ge 2 ]; then
        cat "$dir/metrics_second.txt"
      else
        cat "$dir/metrics.txt"
      fi
      ;;
    *stats/summary)
      n=$(( $(cat "$dir/stats_calls" 2>/dev/null || echo 0) + 1 ))
      echo "$n" > "$dir/stats_calls"
      printf '{"pods":[{"podRef":{"name":"%s"},"network":{"txBytes":%d,"rxBytes":0}}]}' \\
        "$(cat "$dir/pod.txt")" "$((1024 * n))"
      ;;
    *)
      echo "fake kubectl: unexpected raw url $url" >&2
      exit 2
      ;;
  esac
  exit 0
fi

[ "$verb" = "get" ] || { echo "fake kubectl: unsupported verb $verb" >&2; exit 2; }
case "$command_rest" in
  pods) cat "$dir/pods.txt" ;;
  pod) echo node-1 ;;
  *)
    n=$(( $(cat "$dir/objects_calls" 2>/dev/null || echo 0) + 1 ))
    echo "$n" > "$dir/objects_calls"
    if [ -f "$dir/objects_second.txt" ] && [ "$n" -gt 1 ]; then
      cat "$dir/objects_second.txt"
    else
      cat "$dir/objects.txt"
    fi
    ;;
esac
"""


class OperatorQuietTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.bin = self.dir / "bin"
        self.bin.mkdir()
        shim = self.bin / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        (self.dir / "pod.txt").write_text("operator-a")
        (self.dir / "pods.txt").write_text("operator-a")
        (self.dir / "objects.txt").write_text("bronze=1234\n")
        (self.dir / "top.txt").write_text("operator-a   1m   6Mi\n")
        (self.dir / "metrics.txt").write_text(
            '# HELP stardelt_operator_reconciles_total Reconciles since start, by controller and result.\n'
            '# TYPE stardelt_operator_reconciles_total counter\n'
            'stardelt_operator_reconciles_total{controller="catalog",result="ok"} 15\n'
            '# HELP stardelt_operator_status_writes_total Status patches written, by controller.\n'
            '# TYPE stardelt_operator_status_writes_total counter\n'
            'stardelt_operator_status_writes_total{controller="catalog"} 7\n'
        )

    def tearDown(self):
        self._tmp.cleanup()

    def run_script(self):
        return subprocess.run(
            ["bash", str(SCRIPT)],
            cwd=REPO,
            env={
                **os.environ,
                "PATH": f"{self.bin}{os.pathsep}{os.environ.get('PATH', '')}",
                "FAKE_DIR": str(self.dir),
                "NS": "stardelt",
                "WINDOW": "0",
            },
            capture_output=True,
            text=True,
        )

    def test_an_idle_operator_passes(self):
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout.rstrip("\n").splitlines()[-1], "verify-operator-quiet: PASS")
        self.assertIn("resources unchanged", result.stdout)
        # The stats summary grows 1024 B per call: 4.0 KB/s at WINDOW=0 (an effective 1 s).
        self.assertNotIn("✗", result.stdout)

    def test_the_pod_list_asks_for_running_pods_only(self):
        # An evicted pod (phase Failed) has no kubelet stats summary, so it is never listed
        # as an operator pod: only a running pod can be measured (#754).
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        calls = (self.dir / "kubectl_args.log").read_text().splitlines()
        self.assertTrue(
            any("get pods" in line and "--field-selector status.phase=Running" in line for line in calls),
            calls,
        )

    def test_no_running_pod_is_unverified(self):
        (self.dir / "pods.txt").write_text("")
        result = self.run_script()
        self.assertEqual(result.returncode, 3, result.stdout + result.stderr)
        self.assertIn(
            "UNVERIFIED (no pod matches app=stardelt-operator in stardelt)", result.stdout
        )
        self.assertNotIn("PASS", result.stdout)

    def test_a_status_write_fails_and_names_the_object(self):
        (self.dir / "objects_second.txt").write_text("bronze=1235\n")
        result = self.run_script()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("catalog/bronze: resourceVersion 1234 → 1235", result.stdout)
        self.assertEqual(result.stdout.rstrip("\n").splitlines()[-1].split(" ")[1], "FAIL")

    def test_a_status_write_counter_moving_fails_and_names_the_count(self):
        (self.dir / "metrics_second.txt").write_text(
            'stardelt_operator_leader 1\n'
            '# TYPE stardelt_operator_reconciles_total counter\n'
            'stardelt_operator_reconciles_total{controller="catalog",result="ok"} 15\n'
            '# TYPE stardelt_operator_status_writes_total counter\n'
            'stardelt_operator_status_writes_total{controller="catalog"} 9\n'
        )
        result = self.run_script()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("✗ the operator wrote 2 status(es) while nothing changed", result.stdout)
        self.assertEqual(result.stdout.rstrip("\n").splitlines()[-1].split(" ")[1], "FAIL")

    def test_an_unreachable_metrics_endpoint_is_unverified(self):
        (self.dir / "metrics.txt").write_text("")
        result = self.run_script()
        self.assertEqual(result.returncode, 3, result.stdout + result.stderr)
        self.assertIn("UNVERIFIED", result.stdout)

    def test_a_help_only_metrics_page_reads_as_zero_not_unverified(self):
        (self.dir / "metrics.txt").write_text(
            "# HELP stardelt_operator_reconciles_total Reconciles since start, by controller and result.\n"
            "# TYPE stardelt_operator_reconciles_total counter\n"
            "# HELP stardelt_operator_status_writes_total Status patches written, by controller.\n"
            "# TYPE stardelt_operator_status_writes_total counter\n"
        )
        result = self.run_script()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("✓ 0 reconciles", result.stdout)
        self.assertIn("✓ no status write (counter 0 → 0)", result.stdout)
        self.assertNotIn("UNVERIFIED", result.stdout)


if __name__ == "__main__":
    unittest.main()
