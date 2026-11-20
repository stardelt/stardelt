"""The release workflow and the fresh-node gate (stardelt/stardelt#623).

`scripts/release/cut.sh` refuses a commit no fresh node passed at (#620). That report lives on
the machine that ran the run (#48) and never on a hosted runner, so `.github/workflows/release.yml`
passes `NO_FRESH_NODE_CHECK=1` to its cut step. These tests keep that line and the reason beside
it, without a YAML parser (the job installs none).
"""
from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
CUT = ROOT / "scripts" / "release" / "cut.sh"
NAME = "      - name: Check the commit and generate releases/"


class ReleaseWorkflowGateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.text = WORKFLOW.read_text(encoding="utf-8")

    def step(self) -> str:
        """The cut step: from its `- name:` line to the next step at the same indentation."""
        start = self.text.index(NAME)
        rest = self.text[start + len(NAME):]
        end = rest.find("\n      - ")
        return self.text[start:] if end == -1 else self.text[start:start + len(NAME) + end]

    def test_the_cut_step_skips_the_gate_a_hosted_runner_cannot_satisfy(self) -> None:
        step = self.step()
        self.assertIn('NO_FRESH_NODE_CHECK: "1"', step)
        self.assertIn("cut.sh", step)
        self.assertIn("#48", step, "the reason the gate is off here is written beside the line")
        self.assertIn("hosted runner", step)

    def test_the_gate_stays_where_the_run_happened(self) -> None:
        cut = CUT.read_text(encoding="utf-8")
        self.assertIn('if [[ "${NO_FRESH_NODE_CHECK:-0}" != 1 ]]', cut)
        self.assertIn("check_fresh_node.py", cut)

    def test_the_workflow_header_names_the_order(self) -> None:
        self.assertIn("installs that install.yaml before the real cut", self.text)


if __name__ == "__main__":
    unittest.main()
