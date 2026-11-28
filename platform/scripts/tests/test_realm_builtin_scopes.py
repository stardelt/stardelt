"""The shipped realm carries Keycloak's built-in OIDC client scopes.
An import that names `clientScopes` gets only the scopes it names: without `profile`, `email`,
`roles`, `web-origins`, `acr` and `basic` a fresh realm refuses every browser sign-in
(`Invalid scopes: openid profile email`) and no token carries `preferred_username`
(the lab restore after stardelt/stardelt#2323).
"""
import json
import pathlib
import unittest
REALM = pathlib.Path(__file__).resolve().parents[2] / "chart" / "files" / "keycloak-realm.json"
DEFAULT = ["profile", "email", "roles", "web-origins", "acr", "basic"]
OPTIONAL = ["offline_access", "address", "phone", "microprofile-jwt", "organization"]
class RealmBuiltinScopesTest(unittest.TestCase):
 def setUp(self):
     self.realm = json.loads(REALM.read_text())
     self.scopes = {s["name"]: s for s in self.realm["clientScopes"]}
 def test_every_default_and_optional_scope_is_defined(self):
     for name in DEFAULT + OPTIONAL:
         if name == "offline_access":
             continue  # Keycloak creates offline_access with every realm
         self.assertIn(name, self.scopes, f"client scope {name} is missing from the realm")
 def test_the_realm_defaults_name_them(self):
     self.assertEqual(self.realm["defaultDefaultClientScopes"], DEFAULT)
     self.assertEqual(self.realm["defaultOptionalClientScopes"], OPTIONAL)
 def test_profile_maps_preferred_username_and_basic_maps_sub(self):
     claims = lambda s: {m["config"].get("claim.name") for m in self.scopes[s]["protocolMappers"]}
     self.assertIn("preferred_username", claims("profile"))
     self.assertIn("oidc-sub-mapper", {m["protocolMapper"] for m in self.scopes["basic"]["protocolMappers"]})
if __name__ == "__main__":
 unittest.main()
