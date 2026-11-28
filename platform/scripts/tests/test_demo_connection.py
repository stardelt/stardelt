"""Unit tests for scripts/demo-connection.sh (stardelt/stardelt#2346): the Connection it applies names `visitors`.

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `kubectl` is a shell shim first on PATH that records every call, so no cluster is
touched and no credential exists anywhere in the test. The live half is the maintainer's
`make demo-connection YES=1`; what can be decided offline is decided here: the plan line and the
manifest the script applies.
"""
import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "demo-connection.sh"

SHIM = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl: every call is appended to FAKE_LOG; the stdin of `apply -f -` is appended to
# FAKE_APPLIES (every apply, so the test finds the Connection among the store's objects);
# `get secret openbao-init` answers a base64 root token, `get connection` answers Ready.
echo "$*" >>"$FAKE_LOG"
args=()
while [ "$#" -gt 0 ]; do
  case "$1" in
    -n | --namespace) shift 2 ;;
    *) args+=("$1"); shift ;;
  esac
done
case "${args[0]-} ${args[1]-}" in
  "get secret") printf 'dG9rZW4=' ;;
  "get connection") printf 'true' ;;
esac
if [ "${args[0]-}" = exec ] && printf '%s' "$*" | grep -q 'auth list'; then
  printf '{"kubernetes/":{}}'
fi
if [ "${args[0]-}" = apply ]; then
  cat >>"$FAKE_APPLIES"
  printf '\\n---\\n' >>"$FAKE_APPLIES"
elif [ "${args[0]-}" = exec ]; then
  cat >/dev/null 2>&1 || true
fi
exit 0
"""


class DemoConnection(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
        self.log = self.dir / "kubectl.log"
        self.log.write_text("")
        self.applies = self.dir / "applies.yaml"
        self.applies.write_text("")

    def run_script(self, **extra):
        env = dict(
            os.environ,
            PATH=f"{self.dir}:{os.environ['PATH']}",
            NAMESPACE="stardelt",
            FAKE_LOG=str(self.log),
            FAKE_APPLIES=str(self.applies),
        )
        env.pop("YES", None)
        env.update(extra)
        return subprocess.run(
            ["bash", str(SCRIPT)],
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
            stdin=subprocess.DEVNULL,
        )

    def test_the_plan_names_the_viewers(self):
        result = self.run_script()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("viewers visitors", result.stdout)

    def test_the_applied_connection_is_visible_to_visitors(self):
        result = self.run_script(YES="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        applied = self.applies.read_text()
        self.assertIn("usableBy: [visitors, demo]", applied)
        self.assertIn("viewers: [visitors]", applied)


if __name__ == "__main__":
    unittest.main()
