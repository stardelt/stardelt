#!/usr/bin/env python3
"""Create/refresh Nova's Superset service account for dashboard bundles (stardelt#1216, #115).

Nova calls Superset's export/import API as `stardelt-nova`, not as the human `admin`.
The spike (stardelt#1210) measured that the role needs `Gamma`'s permission set plus
`can_write`/`can_delete`/`can_export` on Dashboard (the import, the overwrite and the
prune), and `datasource_access` on the datasets of every Trino database (any catalog,
not only `shared`) — without that a service user sees no dashboard and the export
answers 404. `DASHBOARD_RBAC` is off, so `dashboard.roles` grants nothing and the
datasource grant is what lets the user export a dashboard it does not own.

The password comes from `STARDELT_SUPERSET_NOVA_PASSWORD`, which the chart reads from
Secret `superset-nova` (scripts/app-secrets.sh). An install that never ran that
script has no variable and the init Job still finishes, printing a skip line.

Idempotent: it converges the role to exactly that permission set (drift is removed),
keeps an existing user's password and prints one line. Run it inside the Superset app
environment:

    kubectl -n stardelt exec deploy/superset -- python /app/configs/superset-nova-user.py
"""

DASHBOARD = {("can_write", "Dashboard"), ("can_delete", "Dashboard"), ("can_export", "Dashboard")}

USER = "stardelt-nova"
ROLE = "stardelt-nova"


def is_trino(uri):
    """Whether a Superset database URI is one of the platform's Trino databases."""
    return str(uri or "").lower().startswith("trino://")


def grants(tables):
    """The `datasource_access` pairs for each dataset name, skipping one without a name."""
    return {("datasource_access", perm) for perm in tables if perm}


def converge(current, wanted, granted):
    """What to add to and what to remove from a role that holds `current`: exactly `wanted` plus `granted`."""
    keep = wanted | granted
    return sorted(keep - current), sorted(current - keep)


def main():
    import os

    password = os.environ.get("STARDELT_SUPERSET_NOVA_PASSWORD", "")
    if not password:
        print("user %s: skipped (no STARDELT_SUPERSET_NOVA_PASSWORD)" % USER)
        return

    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset import db, security_manager as sm
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database

        trino = [d for d in db.session.query(Database).order_by(Database.id) if is_trino(d.sqlalchemy_uri)]
        tables = db.session.query(SqlaTable).filter(SqlaTable.database_id.in_([d.id for d in trino])).all()
        names = lambda r: {(p.permission.name, p.view_menu.name) for p in r.permissions}
        wanted = names(sm.find_role("Gamma")) | DASHBOARD
        role = sm.find_role(ROLE) or sm.add_role(ROLE)
        add, remove = converge(names(role), wanted, grants([t.perm for t in tables]))
        for n, v in add:
            pv = sm.find_permission_view_menu(n, v)
            if pv:
                sm.add_permission_role(role, pv)
        for n, v in remove:
            sm.del_permission_role(role, sm.find_permission_view_menu(n, v))   # drift: the role is exactly this set
        sm.get_session.commit()

        user = sm.find_user(username=USER)
        if user:
            user.roles = [role]
            user.active = True
            state = "exists"                                   # the password stays as it is
        else:
            sm.add_user(USER, "stardelt", "Nova", "nova@stardelt.local", role, password)
            state = "created"
        sm.get_session.commit()
        print("user %s: %s, role %s: %d permissions, %d dataset(s)"
              % (USER, state, ROLE, len(role.permissions), len(tables)))


if __name__ == "__main__":
    main()
