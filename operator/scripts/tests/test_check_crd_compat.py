#!/usr/bin/env python3
"""Tests for `check_crd_compat.breaking_changes` — no git, two in-memory CRDs.

Run from `operator/`: python3 -m unittest discover -s scripts/tests -v
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import check_crd_compat  # noqa: E402


def crd(properties, required=None):
    """A one-version (v1alpha1) CRD of kind Volume with `spec` carrying `properties`."""
    spec = {"properties": properties}
    if required is not None:
        spec["required"] = required
    return {
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {"name": "volumes.stardelt.io"},
        "spec": {
            "group": "stardelt.io",
            "names": {"kind": "Volume", "plural": "volumes", "singular": "volume"},
            "scope": "Namespaced",
            "versions": [
                {
                    "name": "v1alpha1",
                    "served": True,
                    "storage": True,
                    "schema": {
                        "openAPIV3Schema": {
                            "description": "A governed folder of files.",
                            "properties": {"spec": spec},
                            "required": ["spec"],
                        }
                    },
                }
            ],
        },
    }


def changes(base, head):
    return check_crd_compat.breaking_changes(
        {"stardelt.io_volumes.yaml": base}, {"stardelt.io_volumes.yaml": head}
    )


class BreakingChanges(unittest.TestCase):
    def test_identical_is_compatible(self):
        doc = crd({"catalog": {"type": "string"}, "name": {"type": "string"}})
        self.assertEqual(changes(doc, doc), [])

    def test_removed_property(self):
        base = crd({"catalog": {"type": "string"}, "grants": {"type": "array"}})
        head = crd({"catalog": {"type": "string"}})
        self.assertEqual(changes(base, head), ["Volume.spec.grants: removed"])

    def test_retyped_property(self):
        base = crd({"catalog": {"type": "string"}})
        head = crd({"catalog": {"type": "integer"}})
        self.assertEqual(changes(base, head), ["Volume.spec.catalog: type string → integer"])

    def test_newly_required_without_default(self):
        base = crd({"catalog": {"type": "string"}}, required=["catalog"])
        head = crd(
            {"catalog": {"type": "string"}, "owner": {"type": "string"}},
            required=["catalog", "owner"],
        )
        self.assertEqual(changes(base, head), ["Volume.spec.owner: newly required"])

    def test_required_with_default_is_compatible(self):
        base = crd({"catalog": {"type": "string"}}, required=["catalog"])
        head = crd(
            {"catalog": {"type": "string"}, "owner": {"type": "string", "default": "platform"}},
            required=["catalog", "owner"],
        )
        self.assertEqual(changes(base, head), [])

    def test_added_optional_property_is_compatible(self):
        base = crd({"catalog": {"type": "string"}})
        head = crd({"catalog": {"type": "string"}, "owner": {"type": "string"}})
        self.assertEqual(changes(base, head), [])

    def test_removed_array_item_property(self):
        base = crd(
            {
                "grants": {
                    "type": "array",
                    "items": {
                        "properties": {
                            "group": {"type": "string"},
                            "verbs": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["group"],
                    },
                }
            }
        )
        head = crd(
            {
                "grants": {
                    "type": "array",
                    "items": {
                        "properties": {"verbs": {"type": "array", "items": {"type": "string"}}},
                    },
                }
            }
        )
        self.assertEqual(changes(base, head), ["Volume.spec.grants[].group: removed"])


if __name__ == "__main__":
    unittest.main()
