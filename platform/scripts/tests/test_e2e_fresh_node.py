"""scripts/e2e-fresh-node.sh: the one-command fresh-node run (stardelt/stardelt#613).

Standard library only, like the other tests here. The script is never allowed to reach Hetzner
in this test: a fake `hetzner-k3s` (and `hcloud`, `helm`, `kubectl`, `make`) comes first on
PATH, a fake wrapper stands in for `dev/hetzner-k3s/fresh-node.sh` through FRESH_NODE_SH, and
the `.fresh-*.yaml` files live in a temporary FRESH_DIR — never in the checkout.
"""

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "e2e-fresh-node.sh"

# exit 0 and touch nothing: the tool is on PATH, that is all preflight asks for.
FAKE_TOOL = """#!/usr/bin/env bash
exit 0
"""

# A created server leaves this marker behind; no test may ever see it.
FAKE_HETZNER_K3S = """#!/usr/bin/env bash
: > "$HOME/hetzner-k3s.called"
exit 0
"""

# `hcloud server list` answers with one header line, then whatever the test asked for.
FAKE_HCLOUD = """#!/usr/bin/env bash
if [ "${1:-}" = "server" ] && [ "${2:-}" = "list" ]; then
  echo "NAME IPV4 IPV6 PRIVATE-NET LOCATION"
  if [ -n "${FAKE_HCLOUD_LIST:-}" ]; then
    printf '%s\\n' "$FAKE_HCLOUD_LIST"
  fi
  exit 0
fi
exit 0
"""

# The stand-in for dev/hetzner-k3s/fresh-node.sh: records the name it was asked to delete.
FAKE_FRESH_NODE = """#!/usr/bin/env bash
echo "${FRESH_NAME:-}" >> "$HOME/calls"
exit 0
"""


class E2EFreshNode(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.home = self.tmp / "home"
        self.home.mkdir()
        self.bindir = self.tmp / "bin"
        self.bindir.mkdir()
        self.fresh_dir = self.tmp / "fresh"
        self.fresh_dir.mkdir()
        self.fake_fresh_node = self.tmp / "fake-fresh-node.sh"

        for name, body in (
            ("hetzner-k3s", FAKE_HETZNER_K3S),
            ("hcloud", FAKE_HCLOUD),
            ("helm", FAKE_TOOL),
            ("kubectl", FAKE_TOOL),
            ("make", FAKE_TOOL),
        ):
            self._write(self.bindir / name, body)
        self._write(self.fake_fresh_node, FAKE_FRESH_NODE)

    def _write(self, path, body):
        path.write_text(body)
        path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    def run_script(self, *args, **env):
        base = {
            "PATH": str(self.bindir) + os.pathsep + os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(self.home),
            "TMPDIR": str(self.tmp),
            "FRESH_NODE_SH": str(self.fake_fresh_node),
            "FRESH_DIR": str(self.fresh_dir),
            "E2E_RUN_ID": "testid",
        }
        base.update(env)
        return subprocess.run(
            ["bash", str(SCRIPT), *args],
            capture_output=True,
            text=True,
            env=base,
        )

    def test_dry_run_prints_the_plan_and_creates_nothing(self):
        done = self.run_script("up", DRY_RUN="1", YES="1")
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn(
            "would create stardelt-fresh-e2e-testid (cpx42 @ nbg1), install the operator, "
            "apply platform-minimal.yaml for <domain>, verify, delete",
            done.stdout,
        )
        self.assertEqual(len(done.stdout.strip().splitlines()), 1, done.stdout)
        self.assertFalse((self.home / "hetzner-k3s.called").exists(), "a server was created")
        self.assertFalse((self.home / ".cache").exists(), "the run directory was created")

    def test_without_yes_it_refuses_before_creating_a_server(self):
        done = self.run_script("up", YES="", DRY_RUN="")
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("re-run with YES=1", done.stderr)
        self.assertFalse((self.home / "hetzner-k3s.called").exists(), "a server was created")

    def test_down_deletes_every_run_this_checkout_started(self):
        (self.fresh_dir / ".fresh-stardelt-fresh-a.yaml").write_text("cluster_name: stardelt-fresh-a\n")
        (self.fresh_dir / ".fresh-stardelt-fresh-b.yaml").write_text("cluster_name: stardelt-fresh-b\n")
        done = self.run_script("down")
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        calls = (self.home / "calls").read_text().split()
        self.assertEqual(calls, ["stardelt-fresh-a", "stardelt-fresh-b"], done.stdout)
        self.assertEqual(list(self.fresh_dir.glob(".fresh-*.yaml")), [], "a config file was left behind")
        self.assertIn("no stardelt-fresh-* server is left", done.stdout)

    def test_down_reports_a_server_hetzner_still_lists(self):
        done = self.run_script("down", FAKE_HCLOUD_LIST="stardelt-fresh-gone 10.0.0.1 nbg1")
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        self.assertIn("stardelt-fresh-gone", done.stdout)
        self.assertIn("hcloud server delete stardelt-fresh-gone", done.stdout)
        self.assertIn("these servers are left — delete them in the Hetzner console", done.stderr)


if __name__ == "__main__":
    unittest.main()
