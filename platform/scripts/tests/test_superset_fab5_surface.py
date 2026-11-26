"""The Superset init scripts call only Flask-AppBuilder 5 security-manager attributes (#1521).

Superset 6.1.0 ships Flask-AppBuilder 5.0.2, which removed `BaseSecurityManager.get_session`.
The three platform scripts and the operator's two copies run inside the chart's `superset-init-db`
hook, so one attribute that does not exist there fails the whole `helm upgrade` and rolls Superset
back (measured on the lab, 2026-11-24 03:37Z). This test pins the surface they may use.
"""

import pathlib
import re
import unittest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1]

# The operator embeds its own copies of two init scripts (values.rs, include_str!): the D44 twins.
OPERATOR_SRC = SCRIPTS.parents[1] / "operator" / "crates" / "stardelt-operator" / "src"

# The SQLAlchemy security manager in Flask-AppBuilder v5.0.2, the version Superset 6.1.0 pins.
FAB5_SECURITY_MANAGER = {
    "find_role", "add_role", "find_permission_view_menu", "add_permission_role",
    "del_permission_role", "find_user", "add_user",
}


class SecurityManagerSurfaceTest(unittest.TestCase):
    def test_the_init_scripts_use_only_attributes_flask_appbuilder_5_has(self):
        used = {}
        paths = [SCRIPTS / n for n in ("superset-analyst-role.py", "superset-nova-user.py",
                                       "superset-group-roles.py")]
        paths += [OPERATOR_SRC / n for n in ("superset_analyst_role.py", "superset_group_roles.py")]
        for path in paths:
            self.assertTrue(path.is_file(), path)
            text = path.read_text()
            for attribute in re.findall(r"\bsm\.([A-Za-z_]+)", text):
                used.setdefault(attribute, set()).add(path.name)
        missing = sorted(set(used) - FAB5_SECURITY_MANAGER)
        self.assertEqual(missing, [], "not in Flask-AppBuilder 5.0.2: %s" % missing)
