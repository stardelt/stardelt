"""The chart's LDAP federation speaks the transport it is given (#1556).

`platform/chart/templates/keycloak.yaml` rendered `ldap://<host>:<port>` for every
directory, so an LDAPS directory on 636 got a plaintext connection and the realm
import failed. The operator's `ldap_components` has the same `identity.ldap.tls`
switch (`none`, `starttls`, `ldaps`); this module renders the chart's realm per
value and checks the provider's `config.connectionUrl` and, for StartTLS, the
`startTls` list Keycloak reads as a string list.

Needs Helm and the sub-charts fetched into `chart/charts/*.tgz` (gitignored;
`helm dependency update chart`). The `lint` job runs `unittest discover` without
them, so the module skips itself there; `helm-lint` has both.
"""

import json
import pathlib
import shutil
import subprocess
import unittest

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

HELM = shutil.which("helm")

HOST = "dir.example.com"
USERS_DN = "ou=people,dc=example,dc=org"
GROUP_DN = "ou=groups,dc=example,dc=org"


def missing_subcharts():
    """Sub-chart names `chart/Chart.yaml` declares but `chart/charts/` lacks."""
    with open(REPO_ROOT / "chart" / "Chart.yaml", encoding="utf-8") as handle:
        deps = yaml.safe_load(handle).get("dependencies") or []
    present = [p.name for p in (REPO_ROOT / "chart" / "charts").glob("*.tgz")]
    return [d["name"] for d in deps
            if not any(p.startswith(d["name"] + "-") for p in present)]


def realm_config(extra):
    """The `stardelt-realm.json` of the rendered `keycloak-realm` ConfigMap."""
    command = [HELM, "template", "stardelt", "chart", "-n", "stardelt",
               "--set", "identity.mode=platform",
               "--set", "platform.baseDomain=example.com",
               "--set", f"identity.keycloak.ldap.host={HOST}",
               "--set", f"identity.keycloak.ldap.usersDn={USERS_DN}",
               "--set", f"identity.keycloak.ldap.groupDn={GROUP_DN}",
               "--set", "identity.keycloak.ldap.bindSecretName=corp-directory"]
    for item in extra:
        command += ["--set", item]
    out = subprocess.run(command, cwd=REPO_ROOT, capture_output=True, text=True)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-2000:])
    for doc in yaml.safe_load_all(out.stdout):
        if (isinstance(doc, dict) and doc.get("kind") == "ConfigMap"
                and doc.get("metadata", {}).get("name") == "keycloak-realm"):
            return json.loads(doc["data"]["stardelt-realm.json"])
    raise AssertionError("no keycloak-realm ConfigMap in the rendered chart")


def ldap_config(realm):
    providers = realm["components"]["org.keycloak.storage.UserStorageProvider"]
    return providers[0]["config"]


def render_failure(extra):
    """The stderr of a `helm template` run that must fail."""
    command = [HELM, "template", "stardelt", "chart", "-n", "stardelt",
               "--set", "identity.mode=platform",
               "--set", "platform.baseDomain=example.com",
               "--set", f"identity.keycloak.ldap.host={HOST}",
               "--set", f"identity.keycloak.ldap.usersDn={USERS_DN}",
               "--set", f"identity.keycloak.ldap.groupDn={GROUP_DN}",
               "--set", "identity.keycloak.ldap.bindSecretName=corp-directory"]
    for item in extra:
        command += ["--set", item]
    out = subprocess.run(command, cwd=REPO_ROOT, capture_output=True, text=True)
    assert out.returncode != 0, "the chart accepted an invalid directory"
    return out.stderr


class ChartKeycloakLdapTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if HELM is None:
            raise unittest.SkipTest("helm is not on PATH")
        missing = missing_subcharts()
        if missing:
            print(f"skip: chart/charts/ lacks {', '.join(missing)} "
                  "(run `helm dependency update chart`)")
            raise unittest.SkipTest(f"chart dependencies not fetched: {', '.join(missing)}")

    def test_ldaps_uses_the_ldaps_scheme(self):
        config = ldap_config(realm_config([
            "identity.keycloak.ldap.tls=ldaps",
            "identity.keycloak.ldap.port=636",
        ]))
        self.assertEqual(config["connectionUrl"], ["ldaps://dir.example.com:636"])
        self.assertNotIn("startTls", config, "ldaps carries no startTls key")

    def test_starttls_sets_the_keycloak_flag(self):
        config = ldap_config(realm_config([
            "identity.keycloak.ldap.tls=starttls",
            "identity.keycloak.ldap.port=389",
        ]))
        self.assertEqual(config["connectionUrl"], ["ldap://dir.example.com:389"])
        self.assertEqual(config["startTls"], ["true"])

    def test_none_is_plaintext(self):
        config = ldap_config(realm_config([
            "identity.keycloak.ldap.tls=none",
            "identity.keycloak.ldap.port=389",
        ]))
        self.assertEqual(config["connectionUrl"], ["ldap://dir.example.com:389"])
        self.assertNotIn("startTls", config, "plaintext carries no startTls key")

    def test_an_unknown_tls_is_refused(self):
        stderr = render_failure(["identity.keycloak.ldap.tls=startls"])
        self.assertIn(
            'identity.keycloak.ldap.tls must be none, starttls or ldaps, got "startls"',
            stderr)

    def test_ldaps_needs_port_636(self):
        stderr = render_failure([
            "identity.keycloak.ldap.tls=ldaps",
            "identity.keycloak.ldap.port=389",
        ])
        self.assertIn(
            "identity.keycloak.ldap.port must be 636 when tls is ldaps, got 389",
            stderr)

    def test_port_636_needs_ldaps(self):
        stderr = render_failure([
            "identity.keycloak.ldap.tls=starttls",
            "identity.keycloak.ldap.port=636",
        ])
        self.assertIn(
            'identity.keycloak.ldap.port 636 needs tls: ldaps, got "starttls"',
            stderr)


if __name__ == "__main__":
    unittest.main()
