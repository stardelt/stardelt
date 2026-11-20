"""The identity contract's endpoints: `tokenEndpoint` and `jwksUri` (platform#1084, #611, #507).

Every component that talks to the IdP with client credentials needs the OAuth2 token
endpoint, and every component that validates a token without discovery needs the JWKS URL.
If each one concatenated `<issuer>/protocol/openid-connect/token` itself, an external provider
(whose endpoints are not under the issuer) would break every consumer at once. So the chart
resolves them once and publishes them as `tokenEndpoint` and `jwksUri` in ConfigMap
`stardelt-identity`: derived from the Keycloak for `identity.mode: platform`, given as
`identity.tokenEndpoint` / `identity.jwksUri` for `external` — where a render without either
must fail rather than publish an empty key. An external install also publishes the audience it
issues tokens for (`apiAudience`), because the actor's audience is not Keycloak's
`spark-connect`.

Needs Helm and the sub-charts fetched into `chart/charts/*.tgz` (gitignored;
`helm dependency update chart`). The `lint` job runs `unittest discover` as
well and does not fetch them, so the module skips itself there; the
`helm-lint` job runs it after its dependency step and has both.
"""

import pathlib
import shutil
import subprocess
import unittest

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

# The `lint` job runs `unittest discover` without Helm; the `helm-lint` job has it.
HELM = shutil.which("helm")


def missing_subcharts():
    """Sub-chart names `chart/Chart.yaml` declares but `chart/charts/` lacks."""
    with open(REPO_ROOT / "chart" / "Chart.yaml", encoding="utf-8") as handle:
        deps = yaml.safe_load(handle).get("dependencies") or []
    present = [p.name for p in (REPO_ROOT / "chart" / "charts").glob("*.tgz")]
    return [d["name"] for d in deps
            if not any(p.startswith(d["name"] + "-") for p in present)]


def render(*settings):
    """Render `templates/identity.yaml` and return the completed process."""
    cmd = [HELM, "template", "stardelt", "chart", "-n", "stardelt",
           "--show-only", "templates/identity.yaml", *settings]
    return subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)


def contract(out):
    """The `stardelt-identity` ConfigMap's data from a successful render."""
    for doc in yaml.safe_load_all(out.stdout):
        if doc and doc.get("kind") == "ConfigMap" and doc["metadata"]["name"] == "stardelt-identity":
            return doc["data"]
    raise AssertionError(f"no stardelt-identity ConfigMap in the render: {out.stdout[-2000:]}")


EXTERNAL = (
    "--set", "identity.mode=external",
    "--set", "identity.issuerUrl=https://login.example.com/tenant/v2.0",
    "--set", "identity.tokenEndpoint=https://login.example.com/tenant/v2.0/token",
    "--set", "identity.jwksUri=https://login.example.com/tenant/v2.0/keys",
    "--set", "identity.apiAudience=api://stardelt",
)


class IdentityContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if HELM is None:
            raise unittest.SkipTest("helm is not on PATH")
        missing = missing_subcharts()
        if missing:
            print(f"skip: chart/charts/ lacks {', '.join(missing)} "
                  "(run `helm dependency update chart`)")
            raise unittest.SkipTest(f"chart dependencies not fetched: {', '.join(missing)}")

    def test_platform_mode_derives_the_keycloak_token_endpoint(self):
        out = render("--set", "identity.mode=platform", "--set", "platform.baseDomain=example.com")
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        self.assertEqual(
            contract(out)["tokenEndpoint"],
            "https://id.example.com/realms/stardelt/protocol/openid-connect/token")

    def test_platform_mode_derives_the_in_cluster_jwks_url(self):
        out = render("--set", "identity.mode=platform", "--set", "platform.baseDomain=example.com")
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        self.assertEqual(
            contract(out)["jwksUri"],
            "http://keycloak.stardelt.svc.cluster.local:8080/realms/stardelt"
            "/protocol/openid-connect/certs")
        self.assertNotIn("apiAudience", contract(out))

    def test_external_mode_passes_the_token_endpoint_through(self):
        out = render(*EXTERNAL)
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        self.assertEqual(contract(out)["tokenEndpoint"],
                         "https://login.example.com/tenant/v2.0/token")

    def test_external_mode_publishes_the_jwks_url_and_the_audience(self):
        out = render(*EXTERNAL)
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        self.assertEqual(contract(out)["jwksUri"], "https://login.example.com/tenant/v2.0/keys")
        self.assertEqual(contract(out)["apiAudience"], "api://stardelt")

    def test_external_mode_without_a_token_endpoint_fails_the_render(self):
        out = render("--set", "identity.mode=external",
                     "--set", "identity.issuerUrl=https://idp.example.com",
                     "--set", "identity.jwksUri=https://idp.example.com/keys",
                     "--set", "identity.apiAudience=api://stardelt")
        self.assertNotEqual(out.returncode, 0, "external mode without tokenEndpoint must fail")
        self.assertIn("identity.mode=external needs identity.tokenEndpoint",
                      out.stdout + out.stderr)

    def test_external_mode_without_a_jwks_uri_fails_the_render(self):
        out = render("--set", "identity.mode=external",
                     "--set", "identity.issuerUrl=https://idp.example.com",
                     "--set", "identity.tokenEndpoint=https://idp.example.com/token",
                     "--set", "identity.apiAudience=api://stardelt")
        self.assertNotEqual(out.returncode, 0, "external mode without jwksUri must fail")
        self.assertIn("identity.mode=external needs identity.jwksUri",
                      out.stdout + out.stderr)

    def test_external_mode_without_an_api_audience_fails_the_render(self):
        out = render("--set", "identity.mode=external",
                     "--set", "identity.issuerUrl=https://idp.example.com",
                     "--set", "identity.tokenEndpoint=https://idp.example.com/token",
                     "--set", "identity.jwksUri=https://idp.example.com/keys")
        self.assertNotEqual(out.returncode, 0, "external mode without apiAudience must fail")
        self.assertIn("identity.mode=external needs identity.apiAudience",
                      out.stdout + out.stderr)


if __name__ == "__main__":
    unittest.main()
