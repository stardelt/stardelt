"""Nova's model endpoint keys are projected by the kubelet (operator#609 slice c1).

The umbrella chart is the operator-free install path. Nova must never read a Secret through the
API: for every entry of `nova.modelKeys` the chart projects the key of the Connection's synced
Secret `conn-<connection>` into the pod at `/var/run/stardelt/models/<endpoint>/key`, exactly as
the operator does. A rotated key therefore arrives without a restart, and an install without
model endpoints renders neither the volume nor the mount.
"""
import pathlib
import unittest

import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
CHART = REPO / "chart"


class ModelKeys(unittest.TestCase):
    def test_the_value_defaults_to_no_projection(self):
        values = yaml.safe_load((CHART / "values.yaml").read_text())
        self.assertEqual(values["nova"]["modelKeys"], [])

    def test_the_template_projects_each_key_under_its_endpoint(self):
        template = (CHART / "templates" / "nova.yaml").read_text()
        for needle in (
            "{{- with .Values.nova.modelKeys }}",
            "name: model-keys",
            "mountPath: /var/run/stardelt/models",
            "defaultMode: 0440",
            'name: {{ printf "conn-%s" .connection | quote }}',
            'path: {{ printf "%s/key" .endpoint | quote }}',
            "optional: true",
            "fsGroup: 65532",
        ):
            self.assertIn(needle, template, needle)


if __name__ == "__main__":
    unittest.main()
