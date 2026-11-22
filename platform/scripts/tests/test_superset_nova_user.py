"""Nova's Superset service account: the role is Gamma plus the dashboard writes and the datasets (#1216).

`superset-nova-user.py` runs in the Superset init Job and creates the user Nova imports dashboard
bundles with. The spike #1210 measured that `Gamma` alone exports nothing: the role also needs
`can_write`/`can_delete`/`can_export` on Dashboard and `datasource_access` on the datasets of every
Trino database. The pure parts of the script are tested here without Superset.
"""

import importlib.util
import pathlib
import sys
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "superset-nova-user.py"
spec = importlib.util.spec_from_file_location("superset_nova_user", SCRIPT)
nova = importlib.util.module_from_spec(spec)
spec.loader.exec_module(nova)


class TrinoDatabasesTest(unittest.TestCase):
    def test_a_trino_uri_is_recognised_whatever_its_case_and_anything_else_is_not(self):
        self.assertTrue(nova.is_trino("trino://superset@trino:8080/warehouse"))
        self.assertTrue(nova.is_trino("Trino://superset@trino:8080/mobility"))
        self.assertFalse(nova.is_trino("postgresql://user@db:5432/app"))
        self.assertFalse(nova.is_trino(None))
        self.assertFalse(nova.is_trino(""))

    def test_the_script_can_be_loaded_without_superset(self):
        self.assertFalse([name for name in sys.modules if name == "superset" or name.startswith("superset.")])


class GrantsTest(unittest.TestCase):
    def test_every_named_dataset_is_granted_and_a_dataset_without_a_name_is_skipped(self):
        self.assertEqual(
            nova.grants(["x", None, ""]),
            {("datasource_access", "x")},
        )


class ConvergeTest(unittest.TestCase):
    def test_a_missing_datasource_grant_is_added_and_drift_is_removed(self):
        wanted = {("can_read", "Dashboard")}
        granted = nova.grants(["x"])
        current = wanted | {("can_write", "Database"), ("all_datasource_access", "all_datasource_access")}
        self.assertEqual(
            nova.converge(current, wanted, granted),
            ([("datasource_access", "x")],
             [("all_datasource_access", "all_datasource_access"), ("can_write", "Database")]),
        )


if __name__ == "__main__":
    unittest.main()
