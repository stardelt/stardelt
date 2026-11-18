"""The restore proof pins the image its source cluster archives with (platform#1512).

A physical restore cannot cross a PostgreSQL major version, so
`manifests/cnpg-restore-proof.yaml` must name the same `spec.imageName` as
`manifests/cnpg-postgres.yaml`: a bump of one and not the other is valid YAML,
green CI, and a restore that fails only when someone needs it.
"""

import pathlib
import unittest

import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
SOURCE = "cnpg-postgres.yaml"
RESTORE = "cnpg-restore-proof.yaml"
PREFIX = "ghcr.io/cloudnative-pg/postgresql:"


def cluster_image(name):
    """The `spec.imageName` of the single `kind: Cluster` document in a manifest."""
    path = REPO / "manifests" / name
    clusters = [
        document
        for document in yaml.safe_load_all(path.read_text())
        if document and document.get("kind") == "Cluster"
    ]
    if len(clusters) != 1:
        raise AssertionError(f"{name} holds {len(clusters)} Cluster documents, expected 1")
    return clusters[0]["spec"].get("imageName")


class RestoreProofImage(unittest.TestCase):
    def test_the_restore_proof_names_the_source_image(self):
        source_image = cluster_image(SOURCE)
        restore_image = cluster_image(RESTORE)
        self.assertEqual(
            restore_image,
            source_image,
            f"restore proof pins {restore_image}, the catalog cluster pins {source_image}",
        )

    def test_both_name_an_image(self):
        for name in (SOURCE, RESTORE):
            image = cluster_image(name)
            self.assertIsInstance(image, str, name)
            self.assertTrue(image, f"{name} names no spec.imageName")
            self.assertTrue(
                image.startswith(PREFIX),
                f"{name} pins {image}, not a {PREFIX}* image",
            )


if __name__ == "__main__":
    unittest.main()
