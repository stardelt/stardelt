"""The catalog accepts workload ServiceAccount tokens on both install paths (stardelt#492)."""

import re
import unittest
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
PLATFORM_VALUES = REPO_ROOT / "platform" / "helm-values" / "lakekeeper.yaml"
OPERATOR_VALUES = REPO_ROOT / "operator" / "crates" / "stardelt-operator" / "values" / "lakekeeper.yaml"
SPIKE = REPO_ROOT / "platform" / "scripts" / "spike-k8s-auth.sh"

EXPECTED = {
    "enabled": True,
    "audience": "stardelt-catalog-workload",
    "createClusterRoleBinding": True,
}


class LakekeeperKubernetesAuthTest(unittest.TestCase):
    def test_the_platform_path_authenticates_service_account_tokens(self):
        values = yaml.safe_load(PLATFORM_VALUES.read_text())
        self.assertEqual(values["auth"]["k8s"], EXPECTED)

    def test_the_operator_twin_matches(self):
        values = yaml.safe_load(OPERATOR_VALUES.read_text())
        self.assertEqual(values["auth"]["k8s"], EXPECTED)

    def test_the_audience_matches_the_spike_that_measured_it(self):
        match = re.search(r"^AUDIENCE=(\S+)$", SPIKE.read_text(), re.M)
        self.assertIsNotNone(match, "spike-k8s-auth.sh has no AUDIENCE=")
        self.assertEqual(match.group(1), EXPECTED["audience"])


if __name__ == "__main__":
    unittest.main()
