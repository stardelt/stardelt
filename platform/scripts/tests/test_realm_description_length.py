"""Every `description` in the imported realm fits Keycloak's `varchar(255)` (platform#1774).

CLIENT/ROLE/GROUP.DESCRIPTION are varchar(255) in Keycloak; a longer one crash-loops the first
import (#293 run).
"""
import json
import pathlib
import unittest

REPO = pathlib.Path(__file__).resolve().parents[2]
REALM = REPO / "chart" / "files" / "keycloak-realm.json"
LIMIT = 255


def descriptions(node, path="$"):
    """Yield `(path, value)` for every `description` string in the tree."""
    if isinstance(node, dict):
        for key, value in node.items():
            child = f"{path}.{key}"
            if key == "description" and isinstance(value, str):
                yield child, value
            yield from descriptions(value, child)
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from descriptions(value, f"{path}[{index}]")


class RealmDescriptionLength(unittest.TestCase):
    def test_descriptions_fit_varchar_255(self):
        realm = json.loads(REALM.read_text(encoding="utf-8"))
        too_long = [
            f"{path} is {len(value)} characters: {value!r}"
            for path, value in descriptions(realm)
            if len(value) > LIMIT
        ]
        self.assertEqual([], too_long, "description exceeds varchar(255)")


if __name__ == "__main__":
    unittest.main()
