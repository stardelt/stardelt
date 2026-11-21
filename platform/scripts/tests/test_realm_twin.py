"""The two realm templates are the same file (#908).

`platform/scripts/identity-bootstrap.sh` applies the chart file to the `keycloak-realm` ConfigMap;
the operator embeds `operator/crates/stardelt-operator/src/keycloak_realm.json` with `include_str!`
because its image builds with `context: operator` and cannot read `platform/` at compile time.
A change to one without the other ships two different realms, so this test keeps them byte-equal.
"""
import pathlib
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
PLATFORM_REALM = REPO_ROOT / "chart" / "files" / "keycloak-realm.json"
OPERATOR_REALM = (
    REPO_ROOT.parent
    / "operator"
    / "crates"
    / "stardelt-operator"
    / "src"
    / "keycloak_realm.json"
)


class RealmTwinTest(unittest.TestCase):
    def test_the_two_realm_files_are_byte_identical(self):
        self.assertTrue(PLATFORM_REALM.is_file(), f"{PLATFORM_REALM} is missing")
        self.assertTrue(OPERATOR_REALM.is_file(), f"{OPERATOR_REALM} is missing")
        self.assertEqual(
            PLATFORM_REALM.read_bytes(),
            OPERATOR_REALM.read_bytes(),
            f"{PLATFORM_REALM} and {OPERATOR_REALM} differ: they must stay byte-identical",
        )


if __name__ == "__main__":
    unittest.main()
