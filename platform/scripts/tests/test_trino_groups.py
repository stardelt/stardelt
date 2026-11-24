"""Unit tests for the Trino group file reader (`scripts/lib/trino_groups.py`, #1502).

Standard library only, like the module: `python3 -m unittest discover -s scripts/tests`
runs them in CI. No cluster and no token: the file's lines are strings, and the rule
under test is that a person is found by the token's `sub` whether Trino's
`principal-field` is `sub` or `username`.
"""
import importlib.util
import unittest
from pathlib import Path

MODULE = Path(__file__).resolve().parents[1] / "lib" / "trino_groups.py"
_spec = importlib.util.spec_from_file_location("trino_groups", MODULE)
trino_groups = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(trino_groups)

DEMO_SUB = "5549059e-d41f-4706-ba9b-d1b4c5948492"
FILE = """stardelt-admins:1a3e139b-fd98-4127-93db-bdfaca5a24b3
visitors:5549059e-d41f-4706-ba9b-d1b4c5948492,69a2befa-2080-4aaa-b0b5-6e2db5bfdbc9
wh-demo:5549059e-d41f-4706-ba9b-d1b4c5948492,69a2befa-2080-4aaa-b0b5-6e2db5bfdbc9
"""


class GroupsFor(unittest.TestCase):
    def test_the_sub_finds_every_group_it_appears_in(self):
        self.assertEqual(trino_groups.groups_for(FILE.splitlines(), DEMO_SUB),
                         {"visitors", "wh-demo"})

    def test_a_line_for_another_principal_is_not_this_persons_group(self):
        self.assertEqual(trino_groups.groups_for(FILE.splitlines(),
                                                 "1a3e139b-fd98-4127-93db-bdfaca5a24b3"),
                         {"stardelt-admins"})

    def test_an_unknown_sub_has_no_groups(self):
        self.assertEqual(trino_groups.groups_for(FILE.splitlines(),
                                                 "00000000-0000-0000-0000-000000000000"), set())

    def test_a_sub_that_is_only_a_prefix_of_a_member_is_not_a_member(self):
        self.assertEqual(trino_groups.groups_for(["visitors:5549059e-d41f"], "5549059e"), set())


if __name__ == "__main__":
    unittest.main()
