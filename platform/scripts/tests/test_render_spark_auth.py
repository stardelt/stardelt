"""`scripts/render_spark_auth.py`: the Spark front follows the identity contract (stardelt#507).

The front's manifest carries placeholders, and the values come from ConfigMap
`stardelt-identity`: `platform` and `external` installs must render different issuers, JWKS
URLs, audiences and group claims from the same file. Both fixtures below are the ConfigMaps
the two paths write.
"""

import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
RENDERER = REPO_ROOT / "scripts" / "render_spark_auth.py"
MANIFEST = REPO_ROOT / "manifests" / "spark-connect-auth.yaml"

PLATFORM = {
    "issuerUrl": "https://id.lab.example/realms/stardelt",
    "tokenEndpoint": "https://id.lab.example/realms/stardelt/protocol/openid-connect/token",
    "jwksUri": "http://keycloak.stardelt.svc.cluster.local:8080/realms/stardelt"
               "/protocol/openid-connect/certs",
    "groupClaim": "groups",
    "adminGroup": "stardelt-admins",
    "authMode": "oidc",
}

EXTERNAL = {
    "issuerUrl": "https://login.example.com/tenant/v2.0",
    "tokenEndpoint": "https://login.example.com/tenant/v2.0/token",
    "jwksUri": "https://login.example.com/tenant/v2.0/keys",
    "apiAudience": "api://stardelt",
    "groupClaim": "roles",
    "adminGroup": "platform-admins",
    "authMode": "oidc",
}


def render(data):
    """Run the renderer on a ConfigMap holding `data`; return the completed process."""
    with tempfile.TemporaryDirectory() as scratch:
        identity = pathlib.Path(scratch) / "identity.json"
        identity.write_text(
            json.dumps({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "stardelt-identity", "namespace": "stardelt"},
                "data": data,
            }),
            encoding="utf-8",
        )
        return subprocess.run(
            [sys.executable, str(RENDERER), "--identity", str(identity),
             "--manifest", str(MANIFEST), "--namespace", "stardelt",
             "--base-domain", "lab.example"],
            capture_output=True, text=True,
        )


class RenderSparkAuthTest(unittest.TestCase):
    def test_platform_mode_renders_the_in_cluster_keycloak(self):
        out = render(PLATFORM)
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        text = out.stdout
        for expected in [
            "issuer: https://id.lab.example/realms/stardelt",
            'audiences: ["spark-connect"]',
            "uri: http://keycloak.stardelt.svc.cluster.local:8080/realms/stardelt"
            "/protocol/openid-connect/certs",
            "cluster: keycloak",
            "path: [{ key: jwt_payload }, { key: groups }]",
            'string_match: { exact: "stardelt-users" }',
            'string_match: { exact: "stardelt-admins" }',
            'hosts: ["spark.lab.example"]',
        ]:
            self.assertIn(expected, text)

    def test_external_mode_renders_the_customer_provider(self):
        out = render(EXTERNAL)
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        text = out.stdout
        for expected in [
            "issuer: https://login.example.com/tenant/v2.0",
            'audiences: ["api://stardelt"]',
            "uri: https://login.example.com/tenant/v2.0/keys",
            "cluster: jwks",
            "path: [{ key: jwt_payload }, { key: roles }]",
            'string_match: { exact: "platform-admins" }',
            "sni: login.example.com",
            "address: login.example.com, port_value: 443",
        ]:
            self.assertIn(expected, text)
        for absent in ("id.lab.example", "realms/stardelt", "stardelt-admins"):
            self.assertNotIn(absent, text)

    def test_no_placeholder_survives_the_render(self):
        for data in (PLATFORM, EXTERNAL):
            out = render(data)
            self.assertEqual(out.returncode, 0, out.stderr[-2000:])
            leftovers = [line for line in out.stdout.splitlines() if "__" in line]
            self.assertEqual(leftovers, [], f"placeholders left: {leftovers}")

    def test_defaults_fill_in_a_contract_without_the_optional_keys(self):
        out = render({
            "issuerUrl": "https://id.lab.example/realms/stardelt",
            "tokenEndpoint": "https://id.lab.example/realms/stardelt"
                             "/protocol/openid-connect/token",
        })
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        text = out.stdout
        for expected in [
            'audiences: ["spark-connect"]',
            "uri: http://keycloak.stardelt.svc.cluster.local:8080/realms/stardelt"
            "/protocol/openid-connect/certs",
            "cluster: keycloak",
            "path: [{ key: jwt_payload }, { key: groups }]",
            'string_match: { exact: "stardelt-admins" }',
        ]:
            self.assertIn(expected, text)


if __name__ == "__main__":
    unittest.main()
