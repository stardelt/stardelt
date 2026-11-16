"""scripts/verify-warehouse-scale.sh tells a normal one-tick lag from a real disagreement (platform#1551).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl` is a shell shim first on PATH that answers `get sqlwarehouses -o json` and
`get deploy trino-worker -o json` from the environment and records every call, so no cluster is
touched.

Three writers keep a class's scale state and they do not tick together: Nova writes the annotations
every 60 s and the access-sync job writes status.scale every 2 min. So a status that is *behind* its
annotation is lag, and only a status that is *ahead* of it — or one that still names a stamp the
object no longer carries — is the bug the check exists for.
"""

import json
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "verify-warehouse-scale.sh"

STAMP = "2026-11-16T08:00:00Z"
LATER = "2026-11-16T08:01:00Z"

SHIM = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl: every call is appended to FAKE_LOG and answered from FAKE_WAREHOUSES / FAKE_WORKER.
echo "$*" >>"$FAKE_LOG"
args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n | --namespace) shift 2 ;;
    *) args+=("$1"); shift ;;
  esac
done
if [ "${args[0]-}" = get ] && [ "${args[1]-}" = sqlwarehouses ]; then
  printf '%s\\n' "$FAKE_WAREHOUSES"
  exit 0
fi
if [ "${args[0]-}" = get ] && [ "${args[1]-}" = deploy ] && [ "${args[2]-}" = trino-worker ]; then
  printf '%s\\n' "${FAKE_WORKER:?no worker fixture}"
  exit 0
fi
exit 0
"""


def warehouses(annotations, last_query_at, in_flight=0):
    """One Ready class named demo; the worker Deployment below targets one replica."""
    status_scale = {
        "phase": "Running",
        "workers": 1,
        "queriesInFlight": in_flight,
    }
    if last_query_at is not None:
        status_scale["lastQueryAt"] = last_query_at
    return {
        "items": [
            {
                "metadata": {"name": "demo", "annotations": annotations},
                "status": {"phase": "Ready", "scale": status_scale},
            }
        ]
    }


WORKER = {"spec": {"replicas": 1}}


class VerifyWarehouseScale(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
        self.log = self.dir / "kubectl.log"
        self.log.write_text("")

    def run_script(self, warehouse, worker=WORKER):
        env = dict(
            os.environ,
            PATH=f"{self.dir}:{os.environ['PATH']}",
            NS="stardelt",
            FAKE_LOG=str(self.log),
            FAKE_WAREHOUSES=json.dumps(warehouse),
            FAKE_WORKER=json.dumps(worker),
        )
        env.pop("NAMESPACE", None)
        return subprocess.run(
            ["bash", str(SCRIPT)], cwd=REPO, env=env, capture_output=True, text=True, timeout=60
        )

    def calls(self):
        return [line for line in self.log.read_text().splitlines() if line]

    def test_agrees(self):
        done = self.run_script(
            warehouses(
                {"stardelt.io/last-query-at": STAMP, "stardelt.io/queries-in-flight": "0"},
                STAMP,
            )
        )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("PASS", done.stdout)
        self.assertIn("✓ demo:", done.stdout)
        self.assertNotIn("· demo:", done.stdout)
        self.assertNotIn("✗", done.stdout)
        # The script stays read-only: it only ever asks.
        self.assertTrue(self.calls(), "the shim recorded nothing")
        for call in self.calls():
            verb = call.split()[2:]  # calls are recorded as "-n <ns> <verb> …"
            self.assertEqual(verb[0], "get", call)
            for mutating in ("delete", "patch", "scale", "create", "apply", "edit"):
                self.assertNotIn(mutating, verb, call)

    def test_annotation_ahead_is_pending(self):
        done = self.run_script(
            warehouses(
                {"stardelt.io/last-query-at": LATER, "stardelt.io/queries-in-flight": "0"},
                STAMP,
            )
        )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("PASS", done.stdout)
        self.assertIn("· demo:", done.stdout)
        self.assertNotIn("✗ demo", done.stdout)

    def test_removed_stamp_fails(self):
        done = self.run_script(warehouses({"stardelt.io/queries-in-flight": "0"}, STAMP))
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("FAIL", done.stdout)
        self.assertIn("✗ demo", done.stdout)
        self.assertIn("a removed stamp must clear", done.stdout)

    def test_in_flight_mismatch_is_pending(self):
        done = self.run_script(
            warehouses(
                {"stardelt.io/last-query-at": STAMP, "stardelt.io/queries-in-flight": "2"},
                STAMP,
                in_flight=0,
            )
        )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("PASS", done.stdout)
        self.assertIn("· demo:", done.stdout)
        self.assertNotIn("✗", done.stdout)


if __name__ == "__main__":
    unittest.main()
