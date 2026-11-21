"""Write-Audit-Publish on Iceberg branches (#114; shapes measured in #820)."""
from __future__ import annotations

import hashlib
import re

BRANCH_PREFIX = "audit_"
# A failed run keeps its branch for 7 days (#114 decision 5); a passed run drops it.
FAILED_BRANCH_TTL_MS = 7 * 24 * 60 * 60 * 1000
# The CRD's `writes` pattern: catalog.schema.table, three parts, no spaces or dots inside.
TABLE = re.compile(r"^[^ .]+[.][^ .]+[.][^ .]+$")


class WapError(RuntimeError):
    """The catalog refused a WAP commit; the message is the sentence the run shows."""


def branch_name(run_id: str) -> str:
    """`audit_<16 hex of sha256(run_id)>`: deterministic, and legal whatever the run id carries."""
    return BRANCH_PREFIX + hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16]


def resolve_table(sql: str, table: str, snapshot_id: int) -> str:
    """`${table}` becomes the Trino form #820 measured: `<table> FOR VERSION AS OF <id>`."""
    return sql.replace("${table}", f"{table} FOR VERSION AS OF {int(snapshot_id)}")


def table_parts(table: str) -> tuple[str, str, str]:
    """`gold.shop.t` -> (`gold`, `shop`, `t`), or `WapError` naming the value."""
    if not TABLE.match(table):
        raise WapError(f"`{table}` is not catalog.schema.table")
    catalog, schema, name = table.split(".")
    return catalog, schema, name


def table_url(base_url: str, table: str) -> str:
    """The Iceberg REST path of one table, `{base}/catalog/v1/{catalog}/namespaces/{schema}/tables/{name}`."""
    catalog, schema, name = table_parts(table)
    return f"{base_url.rstrip('/')}/catalog/v1/{catalog}/namespaces/{schema}/tables/{name}"


def refs(client, base_url: str, table: str) -> dict:
    """The table's `metadata.refs` (one GET with `?snapshots=all`)."""
    response = client.get(table_url(base_url, table) + "?snapshots=all")
    if response.status_code >= 400:
        raise WapError(f"the catalog could not load `{table}` ({response.status_code})")
    return (response.json().get("metadata") or {}).get("refs") or {}


def create_branch(client, base_url: str, table: str, branch: str, snapshot_id: int) -> None:
    """Point a new branch ref at `snapshot_id`; the 7-day TTL is the failed run's cleanup."""
    commit(
        client,
        base_url,
        table,
        [],
        [{
            "action": "set-snapshot-ref",
            "ref-name": branch,
            "snapshot-id": int(snapshot_id),
            "type": "branch",
            "max-ref-age-ms": FAILED_BRANCH_TTL_MS,
        }],
    )


def publish(client, base_url: str, table: str, branch: str, expected_main: int) -> None:
    """Move `main` to the branch's snapshot, only while `main` is still `expected_main`."""
    branch_snapshot = (refs(client, base_url, table).get(branch) or {}).get("snapshot-id")
    if branch_snapshot is None:
        raise WapError(f"`{table}` has no branch `{branch}` to publish")
    commit(
        client,
        base_url,
        table,
        [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": int(expected_main)}],
        [{
            "action": "set-snapshot-ref",
            "ref-name": "main",
            "snapshot-id": int(branch_snapshot),
            "type": "branch",
        }],
    )


def drop_branch(client, base_url: str, table: str, branch: str) -> None:
    """Delete the branch ref; the branch's snapshots stay until the table's expiry runs."""
    commit(
        client,
        base_url,
        table,
        [],
        [{"action": "remove-snapshot-ref", "ref-name": branch}],
    )


def commit(client, base_url: str, table: str, requirements: list, updates: list) -> dict:
    """One catalog commit; `409` is the "main moved" rerun sentence, any other 4xx the catalog's own."""
    response = client.post(
        table_url(base_url, table),
        json={"requirements": requirements, "updates": updates},
    )
    if response.status_code == 409:
        raise WapError(
            "main moved during the run: another writer committed while the audit branch was "
            "written; rerun, and this run publishes nothing"
        )
    if response.status_code >= 400:
        message = ""
        try:
            message = (response.json().get("error") or {}).get("message") or ""
        except ValueError:
            message = response.text[:200]
        raise WapError(f"the catalog refused the commit ({response.status_code}): {message}")
    return response.json()
