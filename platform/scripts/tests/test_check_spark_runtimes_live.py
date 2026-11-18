"""Unit tests for the `--live` mode of scripts/check-spark-runtimes.sh (stardelt-platform#1517).

The gap the mode closes: `make install-spark` and `pin-image.sh` write
configmap/stardelt-spark-runtimes, and nothing read it back, so a hand `kubectl apply`
or a skipped pin step left the operator starting an older Spark image unnoticed.

Standard library only, like the other tests here: `python3 -m unittest discover -s
scripts/tests -v` runs them in CI. `kubectl` is a shell shim first on PATH that answers
the ConfigMap read from a fixture, so no cluster is touched and the negative cases (a
changed sparkConf value, an absent ConfigMap) need no change to the running cluster.
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
SCRIPT = REPO / "scripts" / "check-spark-runtimes.sh"
CATALOGUE = REPO / "chart" / "files" / "spark-runtimes.json"
RUNTIME = "spark-4.1-py3.11"

SHIM = """#!/usr/bin/env bash
set -euo pipefail
# Fake kubectl: `get configmap stardelt-spark-runtimes -o json` prints FAKE_CONFIGMAP;
# the existence probe (no -o json) succeeds; FAKE_MISSING names a namespace whose
# ConfigMap is absent. Nothing else is ever called by the script.
namespace=""
previous=""
for arg in "$@"; do
  if [ "$previous" = "-n" ] || [ "$previous" = "--namespace" ]; then
    namespace="$arg"
  fi
  previous="$arg"
done
if [ -n "${FAKE_MISSING:-}" ] && [ "$namespace" = "${FAKE_MISSING}" ]; then
  exit 1
fi
case " $* " in
  *" get configmap "*) wants_configmap=1 ;;
  *) wants_configmap=0 ;;
esac
case " $* " in
  *" -o json "*|*" --output=json "*|*" -o=json "*) wants_json=1 ;;
  *) wants_json=0 ;;
esac
if [ "$wants_configmap" = 1 ] && [ "$wants_json" = 1 ]; then
  cat "${FAKE_CONFIGMAP:?}"
fi
exit 0
"""


class SparkRuntimesLive(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        shim = self.dir / "kubectl"
        shim.write_text(SHIM)
        shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
        self.configmap = self.dir / "configmap.json"

    def catalogue_object(self, shuffle_partitions=None):
        with CATALOGUE.open() as handle:
            catalogue = json.load(handle)
        self.assertIn(RUNTIME, catalogue, "the catalogue no longer carries the runtime this test names")
        entry = catalogue[RUNTIME]
        if shuffle_partitions is not None:
            entry = dict(entry)
            entry["sparkConf"] = dict(entry["sparkConf"])
            entry["sparkConf"]["spark.sql.shuffle.partitions"] = shuffle_partitions
        return entry

    def write_configmap(self, entry):
        # A ConfigMap whose single data value is the catalogue entry rendered as a string,
        # which is what pin-image.sh and install-spark apply.
        self.configmap.write_text(
            json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"name": "stardelt-spark-runtimes"},
                    "data": {RUNTIME: json.dumps(entry)},
                },
                indent=2,
            )
        )

    def run_script(self, *arguments, **extra):
        env = dict(
            os.environ,
            PATH=f"{self.dir}:{os.environ['PATH']}",
            NAMESPACE="test",
            FAKE_CONFIGMAP=str(self.configmap),
            TMPDIR=str(self.dir),
        )
        env.pop("FAKE_MISSING", None)
        env.update(extra)
        return subprocess.run(
            ["bash", str(SCRIPT), *arguments], env=env, capture_output=True, text=True, timeout=60
        )

    def test_live_configmap_matches_the_catalogue(self):
        self.write_configmap(self.catalogue_object())
        result = self.run_script("--live")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("live ConfigMap matches the catalogue", result.stdout)
        # The file-vs-file checks still run and keep their OK line.
        self.assertIn("check-spark-runtimes: OK (spark-4.1-py3.11, Spark 4.1.4, image ", result.stdout)

    def test_live_configmap_differs_names_the_changed_key(self):
        self.write_configmap(self.catalogue_object(shuffle_partitions="16"))
        result = self.run_script("--live")
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("the live ConfigMap differs", result.stderr)
        self.assertIn("spark.sql.shuffle.partitions", result.stderr)


if __name__ == "__main__":
    unittest.main()
