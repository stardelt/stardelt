"""The namespace step labels the install's namespace, not a literal `stardelt` (platform#1456)."""

import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def render(namespace=None):
    args = ["make", "-n"]
    if namespace:
        args.append("NAMESPACE=" + namespace)
    args.append("step-namespace")
    return subprocess.run(args, cwd=REPO, capture_output=True, text=True, check=True).stdout


class NamespaceStep(unittest.TestCase):
    def test_the_render_names_the_install_namespace(self):
        # `make -n` prints the recipe without running it; the recipe is a sed pipeline, so the
        # rendered name appears in the command (`/  name: demo/`), not in a file.
        self.assertIn("name: demo", render("demo"))
        self.assertNotEqual(render("demo"), render())

    def test_the_manifest_carries_the_floor(self):
        text = (REPO / "manifests" / "namespace.yaml").read_text(encoding="utf-8")
        for label in (
            "pod-security.kubernetes.io/enforce: restricted",
            "pod-security.kubernetes.io/audit: restricted",
            "pod-security.kubernetes.io/warn: restricted",
        ):
            self.assertIn(label, text)
