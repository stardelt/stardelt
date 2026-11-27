#!/usr/bin/env python3
"""Tests for scripts/release/generate.py (stardelt/stardelt#423).

The pure parts only: the chart list, the CLI digest, the pin grouping and the versions.yaml
text. No network, no helm.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts" / "release"))

import generate  # noqa: E402 — the script is not a package


class ChartsTest(unittest.TestCase):
    def test_keys_are_lowercased_with_dashes(self) -> None:
        text = (
            "CNPG_VERSION       := 0.29.1\n"
            "PLUGIN_BARMAN_CLOUD_VERSION := 0.9.0\n"
            "OPENFGA_VERSION    ?= 0.4.1\n"
        )
        self.assertEqual(
            generate.charts(text),
            [("cnpg", "0.29.1"), ("plugin-barman-cloud", "0.9.0"), ("openfga", "0.4.1")],
        )


class CliDigestTest(unittest.TestCase):
    LINE = (
        'pub const IMAGE: &str = "ghcr.io/stardelt/stardelt-cli:v0.0.4@'
        'sha256:47e12f49fdf810042fdc74d77a813a186941861c3e00ea4faa97c792c98fb81e";\n'
    )

    def test_one_line(self) -> None:
        self.assertEqual(
            generate.cli_digest(self.LINE),
            "sha256:47e12f49fdf810042fdc74d77a813a186941861c3e00ea4faa97c792c98fb81e",
        )

    def test_no_match_exits(self) -> None:
        with self.assertRaises(SystemExit):
            generate.cli_digest("pub const IMAGE: &str = \"ghcr.io/other\";\n")


class DbtDigestTest(unittest.TestCase):
    LINE = (
        "  - name: PIPELINE_DBT_IMAGE\n"
        "    value: ghcr.io/stardelt/dbt:demo@sha256:" + "d" * 64 + "\n"
    )

    def test_one_line(self) -> None:
        self.assertEqual(generate.dbt_digest(self.LINE), "sha256:" + "d" * 64)

    def test_no_match_exits(self) -> None:
        with self.assertRaises(SystemExit):
            generate.dbt_digest("  - name: PIPELINE_DBT_IMAGE\n    value: ghcr.io/stardelt/dbt:demo\n")


class OneDigestPerAppTest(unittest.TestCase):
    SHA_A = "sha256:" + "a" * 64
    SHA_B = "sha256:" + "b" * 64

    def test_jupyter_is_renamed(self) -> None:
        images = generate.one_digest_per_app({"jupyter": [self.SHA_A, self.SHA_A]})
        self.assertEqual(images, {"jupyter-singleuser": self.SHA_A})

    def test_disagreeing_pins_exit(self) -> None:
        with self.assertRaises(SystemExit):
            generate.one_digest_per_app({"spark": [self.SHA_A, self.SHA_B]})


class RegistryTest(unittest.TestCase):
    def test_ghcr_and_quay_split(self) -> None:
        self.assertEqual(
            generate.registry("ghcr.io/stardelt/nova"),
            (
                "https://ghcr.io/token?scope=repository:stardelt/nova:pull&service=ghcr.io",
                "https://ghcr.io/v2/stardelt/nova",
            ),
        )
        self.assertEqual(
            generate.registry("quay.io/keycloak/keycloak"),
            (
                "https://quay.io/v2/auth?service=quay.io&scope=repository:keycloak/keycloak:pull",
                "https://quay.io/v2/keycloak/keycloak",
            ),
        )

    def test_an_unknown_registry_is_refused(self) -> None:
        with self.assertRaises(SystemExit):
            generate.registry("docker.io/library/busybox")


class UpstreamPinsTest(unittest.TestCase):
    KEYCLOAK_SHA = "c68e286cacd87b942ac1939a0a1481461109d6acd3a57dc496969a319a76bbf0"
    POSTGRES_SHA = "0f29b435fb501ee534cd0c555d122a6b8e90de477de8e8381c82c5e10d9a9de4"
    KEYCLOAK = (
        'pub const IMAGE: &str = "quay.io/keycloak/keycloak:26.8.2@sha256:'
        + KEYCLOAK_SHA
        + '";\n'
        'pub const THEME: &str = "stardelt";\n'
        'pub const IMAGE_UNPINNED: &str = "quay.io/keycloak/keycloak:26.8.2";\n'
    )
    PG_DUMP = (
        'pub const SUPERSET_PG_IMAGE: &str = '
        '"ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie@sha256:' + POSTGRES_SHA + '";\n'
        'pub const AIRFLOW_PG_IMAGE: &str = '
        '"ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie@sha256:' + POSTGRES_SHA + '";\n'
    )

    def test_the_three_upstream_constants_are_read(self) -> None:
        pins = generate.upstream_pins(self.KEYCLOAK, self.PG_DUMP)
        self.assertEqual(
            list(pins), ["keycloak", "postgresql-superset", "postgresql-airflow"]
        )
        self.assertEqual(
            pins["keycloak"],
            ("quay.io/keycloak/keycloak", "sha256:" + self.KEYCLOAK_SHA),
        )
        self.assertEqual(
            pins["postgresql-superset"],
            ("ghcr.io/cloudnative-pg/postgresql", "sha256:" + self.POSTGRES_SHA),
        )
        self.assertEqual(
            pins["postgresql-airflow"],
            ("ghcr.io/cloudnative-pg/postgresql", "sha256:" + self.POSTGRES_SHA),
        )

    def test_a_missing_constant_exits(self) -> None:
        with self.assertRaises(SystemExit):
            generate.upstream_pins(self.KEYCLOAK, "")


class VersionsYamlTest(unittest.TestCase):
    def test_header_images_and_version(self) -> None:
        images = {
            image: ("sha256:" + "c" * 64, "abc123") for image in generate.IMAGE_ORDER
        }
        text = generate.versions_yaml(
            "0.0.9",
            "0" * 40,
            images,
            [("cnpg", "0.29.1")],
            {"stardelt": "0.4.1", "stardelt-jupyterlab": "0.2.0"},
        )
        self.assertTrue(text.startswith("# Generated by scripts/release/generate.py"))
        self.assertIn("version: v0.0.9", text)
        self.assertIn('stardelt-cli: {digest: "sha256:', text)

    def test_an_upstream_image_carries_the_marker(self) -> None:
        images = {
            image: ("sha256:" + "c" * 64, "abc123") for image in generate.IMAGE_ORDER
        }
        text = generate.versions_yaml(
            "0.0.9",
            "0" * 40,
            images,
            [("cnpg", "0.29.1")],
            {"stardelt": "0.4.1", "stardelt-jupyterlab": "0.2.0"},
            upstream=frozenset({"keycloak"}),
        )
        self.assertIn(
            'keycloak: {digest: "sha256:' + "c" * 64 + '", revision: "abc123", source: upstream}',
            text,
        )
        self.assertIn(
            'nova: {digest: "sha256:' + "c" * 64 + '", revision: "abc123"}',
            text,
        )


if __name__ == "__main__":
    unittest.main()
