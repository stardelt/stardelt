"""The model-call retention has an install path (nova#890 slice 1)."""
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


class ModelCallRetention(unittest.TestCase):
    def test_the_manifest_sets_the_default(self):
        env = {entry["name"]: entry for entry in nova_container()["env"]}
        value = env["NOVA_MODEL_CALL_RETENTION_DAYS"]["value"]
        self.assertEqual(value, "30")
        self.assertGreaterEqual(int(value), 0)

    def test_the_chart_default_is_empty(self):
        values = yaml.safe_load((REPO / "chart" / "values.yaml").read_text())
        self.assertEqual(values["nova"]["modelCallRetentionDays"], "")
        # The comment above the value must explain that `0` is a value, not "unset".
        lines = (REPO / "chart" / "values.yaml").read_text().splitlines()
        index = next(i for i, line in enumerate(lines)
                     if line.strip().startswith("modelCallRetentionDays:"))
        comment = [line for line in lines[:index]
                   if line.strip().startswith("#")][-3:]
        self.assertIn("0", "\n".join(comment))

    def test_the_template_renders_only_when_set(self):
        template = (REPO / "chart" / "templates" / "nova.yaml").read_text()
        self.assertIn("NOVA_MODEL_CALL_RETENTION_DAYS", template)
        self.assertIn(".Values.nova.modelCallRetentionDays", template)
        self.assertIn(
            'ne (.Values.nova.modelCallRetentionDays | toString) ""', template)
