"""Tests for scripts/sync-operator-chart.sh (platform#1491).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests -v`
runs them in CI. `helm` is replaced by a Python shim that is first on PATH and records its argv in
`$TMP/helm-argv`; the operator chart is an empty `Chart.yaml` in a temp dir. No cluster is touched.

Why: `scripts/pin-image.sh stardelt-operator` moves the operator's image and its CRDs, but the chart
— ClusterRole, NetworkPolicies, values — is applied only by `helm upgrade --install`
(`make install-operator`). A chart-only fix was pinned, rolled out and stayed dark until a manual
`helm upgrade` (operator#781's `deletecollection` rule, platform#1491).
"""
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "sync-operator-chart.sh"
PIN_IMAGE = REPO / "scripts" / "pin-image.sh"

DIGEST = "sha256:" + "a" * 64
CALL = 'bash scripts/sync-operator-chart.sh "$digest" "$NS"'
CRD_APPLY = 'kubectl apply --server-side --force-conflicts -f "$CRDS_DIR"'

HELM_SHIM = """#!/usr/bin/env python3
import os, sys
with open(os.path.join(os.environ["TMP"], "helm-argv"), "w") as f:
    f.write("\\n".join(sys.argv[1:]))
print("Release \\"stardelt-operator\\" has been upgraded. Happy Helming!")
print("STATUS: deployed")
"""

# The dirs that hold `dirname` but (on a normal runner) not `helm`: /usr/local/bin and a user's
# local bin are where helm lands, so they stay out. If helm ever moves into these, the test skips
# itself with a printed line instead of running a real upgrade.
SYSTEM_PATH = os.pathsep.join(("/usr/bin", "/bin"))


class SyncOperatorChartTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.bin = self.dir / "bin"
        self.bin.mkdir()
        helm = self.bin / "helm"
        helm.write_text(HELM_SHIM)
        helm.chmod(0o755)
        self.operator = self.dir / "operator"
        (self.operator / "chart").mkdir(parents=True)
        (self.operator / "chart" / "Chart.yaml").write_text(
            "apiVersion: v2\nname: stardelt-operator\nversion: 0.1.0\n"
        )
        self.argv_file = self.dir / "helm-argv"

    def tearDown(self):
        self.tmp.cleanup()

    def env(self, path=None, operator=None):
        return {
            **os.environ,
            "PATH": path if path is not None else f"{self.bin}:{os.environ['PATH']}",
            "OPERATOR_DIR": str(operator if operator is not None else self.operator),
            "TMP": str(self.dir),
        }

    def run_script(self, *args, path=None, operator=None):
        return subprocess.run(
            ["bash", "scripts/sync-operator-chart.sh", *args],
            cwd=REPO,
            env=self.env(path, operator),
            capture_output=True,
            text=True,
        )

    def argv(self):
        self.assertTrue(self.argv_file.exists(), "the fake helm recorded no argv")
        return self.argv_file.read_text().splitlines()

    def assertSequence(self, argv, *expected):
        for first, second in zip(expected, expected[1:]):
            self.assertIn(first, argv)
            self.assertIn(second, argv)
            self.assertLess(
                argv.index(first), argv.index(second), f"{first!r} must precede {second!r}"
            )

    def git(self, *args):
        return subprocess.run(
            ["git", "-C", str(self.operator), *args], capture_output=True, text=True, check=True
        ).stdout.strip()

    def make_git_chart(self):
        """Turn self.operator into a git checkout; return its first commit's sha."""
        self.git("init", "-q")
        self.git("add", "chart/Chart.yaml")
        self.git("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "chart")
        return self.git("rev-parse", "HEAD")

    def test_the_rendered_chart_names_its_revision(self):
        first = self.make_git_chart()
        result = self.run_script(DIGEST)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("› chart from", result.stdout)
        self.assertIn(first[:7], result.stdout)

    def test_a_checkout_behind_origin_main_is_named_before_it_renders(self):
        first = self.make_git_chart()
        (self.operator / "chart" / "Chart.yaml").write_text(
            "apiVersion: v2\nname: stardelt-operator\nversion: 0.1.0\n# moved\n"
        )
        self.git("add", "chart/Chart.yaml")
        self.git("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "newer")
        newest = self.git("rev-parse", "HEAD")
        # A stale checkout: HEAD on the older commit, origin/main on the newer one.
        self.git("reset", "-q", "--hard", first)
        self.git("update-ref", "refs/remotes/origin/main", newest)
        result = self.run_script(DIGEST)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("1 commit(s) behind origin/main", result.stderr)
        self.assertIn(first[:7], result.stderr)

    def test_renders_the_chart_at_the_digest(self):
        result = self.run_script(DIGEST, "stardelt")
        self.assertEqual(result.returncode, 0, result.stderr)
        argv = self.argv()
        self.assertSequence(argv, "upgrade", "--install", "stardelt-operator")
        self.assertSequence(argv, "--namespace", "stardelt")
        self.assertSequence(argv, "-f", "helm-values/operator.yaml")
        self.assertSequence(argv, "--set-string", f"image.tag=dev@{DIGEST}")
        self.assertSequence(argv, "--set", "imagePullSecrets[0].name=ghcr-creds")
        self.assertIn("--wait", argv)
        self.assertTrue(
            result.stdout.rstrip().endswith(
                f"› chart stardelt-operator rendered at dev@{DIGEST[:19]}…"
            ),
            result.stdout,
        )

    def test_a_tag_is_refused(self):
        result = self.run_script("dev@" + DIGEST)
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.argv_file.exists(), "the chart was rendered for a tag")
        self.assertIn("usage:", result.stderr)

    def test_a_short_digest_is_refused(self):
        result = self.run_script("sha256:" + "a" * 8)
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.argv_file.exists(), "the chart was rendered for a short digest")

    def test_no_helm_is_a_note_not_a_failure(self):
        if subprocess.run(
            ["sh", "-c", f"PATH={SYSTEM_PATH} command -v helm"],
            capture_output=True,
        ).returncode == 0:
            self.skipTest("helm is on the system PATH here; cannot test its absence")
        result = self.run_script(DIGEST, "stardelt", path=SYSTEM_PATH)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("helm not found", result.stdout)
        self.assertEqual(result.stderr, "")

    def test_no_chart_is_a_note_not_a_failure(self):
        # pin-image.sh also runs on machines without the operator checkout; a missing chart is a
        # printed note, not a failed pin.
        result = self.run_script(DIGEST, "stardelt", operator=self.dir / "nothing-here")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no operator chart", result.stdout)
        self.assertEqual(result.stderr, "")
        self.assertFalse(self.argv_file.exists(), "helm ran without a chart")

    def test_pin_image_calls_it_last(self):
        text = PIN_IMAGE.read_text()
        self.assertEqual(
            text.count(CALL), 1, f"{PIN_IMAGE} must call the helper exactly once"
        )
        call_at = text.index(CALL)
        crd_at = text.index(CRD_APPLY)
        self.assertGreater(
            call_at, crd_at, "the chart must be rendered after the CRDs are applied"
        )
        lines = text.splitlines()
        call_line = next(i for i, line in enumerate(lines) if CALL in line)
        condition = next(
            line
            for line in reversed(lines[:call_line])
            if line.lstrip().startswith("if ")
        )
        for needle in ("SYNC_CHART:-1", "stardelt-operator", "WRITE_MANIFEST_ONLY:-0"):
            self.assertIn(needle, condition, f"{needle} missing from: {condition}")


if __name__ == "__main__":
    unittest.main()
