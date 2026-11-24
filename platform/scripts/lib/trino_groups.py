"""The Trino group file: one `group:sub,sub,...` line per group.

`scripts/access-sync-job.py` renders it into ConfigMap `trino-group-file` and Trino's
`file` group provider reads it (platform#125). Its members are Keycloak `sub` values
whatever claim Trino uses as the engine user (`principal-field` in
`helm-values/trino-identity.yaml`), so every reader looks a person up by the sub.
"""


def groups_for(lines, sub):
    """The names of the groups whose member list contains `sub`; empty when none does."""
    found = set()
    if not sub:
        return found
    for line in lines:
        name, _, members = line.strip().partition(":")
        if sub in members.split(","):
            found.add(name)
    return found
