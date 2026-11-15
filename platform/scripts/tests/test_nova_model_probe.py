"""The model probe's re-check interval has an install path (operator#609 slice c2)."""
import pathlib
import unittest

import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]


def nova_container():
    manifests = REPO / "manifests" / "nova-deployment.yaml"
    for document in yaml.safe_load_all(manifests.read_text()):
        if document and document.get("kind") == "Deployment":
            return document["spec"]["template"]["spec"]["containers"][0]
    raise AssertionError("no Deployment in manifests/nova-deployment.yaml")


class ProbeInterval(unittest.TestCase):
    def test_the_manifest_sets_a_whole_number_of_seconds_in_range(self):
        env = {entry["name"]: entry for entry in nova_container()["env"]}
        value = env["NOVA_MODEL_PROBE_SECONDS"]["value"]
        self.assertTrue(value.isdigit(), value)
        self.assertGreaterEqual(int(value), 300)
        self.assertLessEqual(int(value), 604800)

    def test_the_chart_value_matches_the_manifest(self):
        values = yaml.safe_load((REPO / "chart" / "values.yaml").read_text())
        env = {entry["name"]: entry for entry in nova_container()["env"]}
        self.assertEqual(int(env["NOVA_MODEL_PROBE_SECONDS"]["value"]),
                         values["nova"]["modelProbeSeconds"])

    def test_the_template_renders_the_value(self):
        template = (REPO / "chart" / "templates" / "nova.yaml").read_text()
        self.assertIn("NOVA_MODEL_PROBE_SECONDS", template)
        self.assertIn(".Values.nova.modelProbeSeconds", template)
