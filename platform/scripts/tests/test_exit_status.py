"""scripts/exit-status.sh: the 0.0.4 exit list printed as one row per line (stardelt/stardelt#401).

Standard library only, like the other tests here. `gh` and `make` are shell shims first on PATH:
the `gh` one answers the issue states and the P0 count from the environment, the `make` one stands
in for `make verify-release`. Line 6 reads the repository's own manifests, so the scan is the real
one (they are all digests).
"""

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "exit-status.sh"

FAKE_GH = """#!/usr/bin/env python3
import os, sys
args = sys.argv[1:]
if os.environ.get("FAKE_GH_BROKEN") == "1":
    sys.exit(1)
if "auth" in args:
    sys.exit(0)
if "view" in args:
    print(os.environ.get("FAKE_ISSUE_STATE", "CLOSED"))
    sys.exit(0)
if "list" in args:
    if "length" in args:
        print(os.environ.get("FAKE_P0_COUNT", "0"))
    else:
        for n in os.environ.get("FAKE_P0_NUMBERS", "").split():
            print(n)
    sys.exit(0)
sys.exit(0)
"""

FAKE_MAKE = """#!/usr/bin/env bash
echo "✓ verify-pins 1s"
echo "commit abc1234 · 19/19 passed · 0 unverified · logs in /tmp/verify-release.test"
exit ${FAKE_MAKE_EXIT:-0}
"""


class ExitStatus(unittest.TestCase):
    def _run(self, extra_env=None):
        tmp = Path(tempfile.mkdtemp(prefix="exit-status-test-"))
        self.addCleanup(lambda: subprocess.run(["rm", "-rf", str(tmp)]))
        bindir = tmp / "bin"
        bindir.mkdir()
        for name, body in (("gh", FAKE_GH), ("make", FAKE_MAKE)):
            path = bindir / name
            path.write_text(body)
            path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        env = dict(os.environ)
        env["PATH"] = str(bindir) + os.pathsep + env["PATH"]
        env["GH"] = "gh"
        env.setdefault("TMPDIR", str(tmp))
        env.update(extra_env or {})
        return subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, env=env)

    def test_every_exit_line_is_printed(self):
        done = self._run()
        rows = [line for line in done.stdout.splitlines() if line[:1] in ("✓", "✗", "·", "?")]
        self.assertEqual(len(rows), 6, done.stdout)
        for number, row in enumerate(rows, start=1):
            self.assertTrue(row.startswith(("✓ ", "✗ ", "· ", "? ")), row)
            self.assertEqual(row[2:].split(" ", 1)[0], str(number), row)
        self.assertIn("MANUAL", rows[0], done.stdout)

    def test_an_open_p0_fails_the_script(self):
        done = self._run({"FAKE_P0_COUNT": "3", "FAKE_P0_NUMBERS": "16 48 75"})
        self.assertEqual(done.returncode, 1, done.stdout + done.stderr)
        rows = [line for line in done.stdout.splitlines() if line.startswith(("✓ ", "✗ ", "· ", "? "))]
        row3 = [row for row in rows if row[2:].split(" ", 1)[0] == "3"]
        self.assertEqual(len(row3), 1, done.stdout)
        self.assertIn("✗", row3[0])
        self.assertIn("3", row3[0])
        self.assertIn("#16", row3[0])

    def test_all_green_exits_zero(self):
        done = self._run({"FAKE_ISSUE_STATE": "CLOSED", "FAKE_P0_COUNT": "0", "FAKE_MAKE_EXIT": "0"})
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        self.assertIn("0 fail", done.stdout)
        self.assertIn("✓ 6  every image is pinned by digest", done.stdout)

    def test_an_unreachable_gh_is_unverified(self):
        done = self._run({"FAKE_GH_BROKEN": "1"})
        self.assertEqual(done.returncode, 3, done.stdout + done.stderr)
        self.assertIn("UNVERIFIED", done.stdout)

    def test_the_makefile_target_exists_and_is_phony(self):
        makefile = (REPO / "Makefile").read_text()
        self.assertIn("\nexit-status: _context", makefile)
        phony = [line for line in makefile.splitlines() if line.startswith(".PHONY:")]
        self.assertTrue(any("exit-status" in line.split() for line in phony), "exit-status not in .PHONY")


if __name__ == "__main__":
    unittest.main()
