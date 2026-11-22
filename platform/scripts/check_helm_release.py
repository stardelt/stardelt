"""Refuse to touch a Helm release that an interrupted upgrade left `pending-*` (#1126).

A release in `pending-upgrade`, `pending-install`, `pending-rollback` or `pending-uninstall`
refuses every later operation ("another operation (install/upgrade/rollback) is in progress"),
so a roll must stop before it moves a Deployment that the release then cannot record.
"""

import json
import sys

PENDING = ("pending-install", "pending-upgrade", "pending-rollback", "pending-uninstall")


def pending(helm_status_json):
    """`(status, revision)` when the release is stuck, else None."""
    try:
        doc = json.loads(helm_status_json or "{}")
    except json.JSONDecodeError:
        return None
    status = ((doc.get("info") or {}).get("status") or "").strip()
    if status not in PENDING:
        return None
    return status, int(doc.get("version") or 0)


def main(argv):
    """`check_helm_release.py <helm-status-json-file>`: exit 1 with the recovery line."""
    if len(argv) != 2:
        print("usage: check_helm_release.py <helm-status-json>", file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as handle:
        stuck = pending(handle.read())
    if stuck is None:
        return 0
    status, revision = stuck
    print(
        "check-helm-release: the release is %s at revision %d — an earlier upgrade was "
        "interrupted, and every later one is refused until it is cleared:\n"
        "  helm -n <namespace> history <release>\n"
        "  helm -n <namespace> rollback <release> <last deployed revision>" % (status, revision),
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
