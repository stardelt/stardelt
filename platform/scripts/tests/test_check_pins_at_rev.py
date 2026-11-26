"""Unit tests for scripts/check_pins_at_rev.py --gate (stardelt/stardelt#1750).

No git and no registry: every name the report reads is monkeypatched, so the tests pin the
rule D1 ("each pin is the build of a commit at or before M") and never the network.
"""

import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from scripts import check_pins_at_rev

APPS = [("nova", "m.yaml", "kind", "dev"), ("airflow", "a.yaml", "kind", "demo")]
PIN_TEXT = lambda repo, manifest, rev: ("text-" + manifest, None)  # noqa: E731
PARSE_PINS = lambda text, kind: [{"text-m.yaml": "sha256:aaa", "text-a.yaml": "sha256:bbb"}[text]]  # noqa: E731
BUILDS = lambda app, rev, limit: ["c2" * 20, "c1" * 20]  # noqa: E731
SHORT = lambda c: c[:7]  # noqa: E731
GIT = lambda *a: ""  # noqa: E731


def run(argv, resolve_digest):
    """`main(argv)` with the module's git and registry names replaced; returns (code, out, err)."""
    out, err = io.StringIO(), io.StringIO()
    with patch.object(check_pins_at_rev, "APPS", APPS), patch.object(
        check_pins_at_rev, "pin_text", PIN_TEXT
    ), patch.object(check_pins_at_rev, "parse_pins", PARSE_PINS), patch.object(
        check_pins_at_rev, "builds", BUILDS
    ), patch.object(
        check_pins_at_rev, "short", SHORT
    ), patch.object(
        check_pins_at_rev, "_git", GIT
    ), patch.object(
        check_pins_at_rev, "resolve_digest", resolve_digest
    ), redirect_stdout(out), redirect_stderr(err):
        code = check_pins_at_rev.main(argv)
    return code, out.getvalue(), err.getvalue()


class GateTest(unittest.TestCase):
    def test_the_gate_passes_when_every_pin_is_a_build_at_or_before_the_rev(self):
        def resolve_digest(app, tag):
            if app == "nova" and tag == "sha-c2c2c2c":
                return "sha256:aaa", ""
            if app == "airflow" and tag == "sha-c1c1c1c":
                return "sha256:bbb", ""
            return None, ""

        code, out, err = run(["--rev", "M", "--gate"], resolve_digest)

        self.assertEqual(code, 0)
        self.assertIn("PASS", out)
        self.assertEqual([line for line in err.splitlines() if line.startswith("note: ")],
                         ["note: airflow is pinned to the build of c1c1c1c; the newest build at or before M is c2c2c2c (D2, advisory)"])

    def test_the_gate_fails_and_names_the_app_without_a_proven_build(self):
        def resolve_digest(app, tag):
            if app == "nova" and tag == "sha-c2c2c2c":
                return "sha256:aaa", ""
            return None, ""

        code, out, err = run(["--rev", "M", "--gate"], resolve_digest)

        self.assertEqual(code, 1)
        self.assertNotIn("PASS", out)
        self.assertIn("FAIL", err)
        self.assertIn("airflow", err)

    def test_without_the_gate_the_report_never_fails(self):
        def resolve_digest(app, tag):
            if app == "nova" and tag == "sha-c2c2c2c":
                return "sha256:aaa", ""
            return None, ""

        code, out, err = run(["--rev", "M"], resolve_digest)

        self.assertEqual(code, 0)
        self.assertIn("airflow", out)
        self.assertNotIn("airflow", err)


if __name__ == "__main__":
    unittest.main()
