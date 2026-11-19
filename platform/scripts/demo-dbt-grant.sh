#!/usr/bin/env bash
# Let the demo's dbt run drop and rename its own tables: Lakekeeper `modify` for pipeline-demo (#373).
#
# The shop project materializes tables in bronze and gold and views in silver, all in the schema
# `shop_dbt`. dbt's table materialization builds `<model>__dbt_tmp` and then drops (or renames)
# the previous table; since platform#1130 a `write` grant is Lakekeeper `write`, and dropping or
# renaming needs `modify`, which only administrators hold. Measured on the dev cluster on
# 2026-11-19 (#373): without this grant the run ends in
#   Access Denied: Cannot drop table bronze.shop_dbt.order_lines__dbt_tmp
#
# Scope: a NAMESPACE-level `modify` on `shop_dbt` exists and is honoured (measured: with it on
# bronze and silver the same run dropped the temp table and renamed it into place), but
# Lakekeeper 0.15 cannot resolve a namespace name to its id over the management API:
# `GET /catalog/v1/{warehouse}/namespaces/shop_dbt` answers `"namespace-uuid": null`, there is no
# namespace list endpoint, and every assignment endpoint rejects anything but a UUID
# (lakekeeper#1898). A script therefore cannot name a schema, and it could only reach schemas
# that already exist — `gold.shop_dbt` does not before the first `tag:marts` run, so the second
# run would fail on exactly the drop this grant is for. The grant is therefore warehouse-scoped
# on bronze, silver and gold: `modify` reaches every schema and table in a warehouse, and the
# demo owns all three (spec.owner `demo` in manifests/public-demo-catalogs.yaml) — the same
# scope `demo-reset-grant.sh` uses for playground.
#
# The grant goes to the user `oidc~<subject>` of `sp-pipeline-demo`, never to the `demo` role:
# access-sync deletes every warehouse grant of a role it manages when the Catalog does not
# declare it, while a grant to another subject is left alone (it reads as `foreign` in the
# Catalog's Drift condition).
#
#   make demo-dbt-grant                       grant it on bronze, silver and gold (needs YES=1)
#   bash scripts/demo-dbt-grant.sh --check    read only; exit 1 when one of them is missing
#
# Copied from scripts/demo-reset-grant.sh: the temp directory and trap, the port-forward and the
# token helper. Secrets never reach argv or stdout: the token lives in a 0600 temp directory and
# curl reads it from a file (`-H @file`); the principal is named in the output, its `sub` never
# printed.
set -euo pipefail

NAMESPACE=${NAMESPACE:-stardelt}
BASE=http://127.0.0.1:18181
WAREHOUSES=(bronze silver gold)
PRINCIPAL=service-account-sp-pipeline-demo
YES="${YES:-0}"

ACTION=grant
case "${1:-}" in
  --check) ACTION=check ;;
  "") ;;
  *) printf 'usage: %s [--check]\n' "$0" >&2; exit 2 ;;
esac

if [ "$ACTION" = grant ] && [ "$YES" != 1 ]; then
  printf 'would: grant Lakekeeper modify to %s on the warehouses %s (drop and rename of the shop dbt tables)\n' \
    "$PRINCIPAL" "${WAREHOUSES[*]}"
  printf 'nothing was changed — re-run with YES=1\n'
  exit 1
fi

TMP=$(mktemp -d)
chmod 700 "$TMP"
PF_PID=""
cleanup() {
  if [ -n "$PF_PID" ]; then kill "$PF_PID" 2>/dev/null || true; fi
  rm -rf "$TMP"
}
trap cleanup EXIT

ok()   { printf '  ✓ %s\n' "$*"; }
info() { printf '  · %s\n' "$*"; }
die()  { printf '  ✗ %s\n' "$*" >&2; exit 1; }

# --- the dbt principal and the three warehouses -------------------------------
SUBJECT="$(kubectl -n "$NAMESPACE" get secret sp-pipeline-demo -o jsonpath='{.data.subject}' | base64 -d)"
[ -n "$SUBJECT" ] || die "Secret sp-pipeline-demo has no subject"
declare -A WAREHOUSE_ID=()
for w in "${WAREHOUSES[@]}"; do
  id="$(kubectl -n "$NAMESPACE" get catalog "$w" \
    -o jsonpath='{.status.conditions[?(@.type=="CatalogReady")].message}' | awk '{print $NF}')"
  case "$id" in *-*-*-*-*) ;; *) die "catalog $w has no Lakekeeper warehouse id";; esac
  WAREHOUSE_ID[$w]=$id
done

token() { # token <client suffix> -> $TMP/token-<name>, $TMP/hdr-<name>
  local name=$1 id secret url scope cfg
  id=$(kubectl -n "$NAMESPACE" get secret "svc-catalog-$name" -o jsonpath='{.data.client-id}' | base64 -d)
  secret=$(kubectl -n "$NAMESPACE" get secret "svc-catalog-$name" -o jsonpath='{.data.client-secret}' | base64 -d)
  url=$(kubectl -n "$NAMESPACE" get secret "svc-catalog-$name" -o jsonpath='{.data.token-url}' | base64 -d)
  scope=$(kubectl -n "$NAMESPACE" get secret "svc-catalog-$name" -o jsonpath='{.data.scope}' 2>/dev/null | base64 -d 2>/dev/null || true)
  [ -n "${scope:-}" ] || scope=openid
  cfg=$TMP/curl-$name.cfg
  {
    printf 'url = "%s"\n' "$url"
    printf 'data-urlencode = "grant_type=client_credentials"\n'
    printf 'data-urlencode = "client_id=%s"\n' "$id"
    printf 'data-urlencode = "client_secret=%s"\n' "$secret"
    printf 'data-urlencode = "scope=%s"\n' "$scope"
    printf 'silent\nshow-error\n'
  } > "$cfg"
  chmod 600 "$cfg"
  curl -K "$cfg" > "$TMP/token-$name.json" \
    || die "token request failed for svc-catalog-$name"
  python3 - "$TMP/token-$name.json" "$TMP/token-$name" <<'PY'
import json, sys
token = json.load(open(sys.argv[1]))["access_token"]
open(sys.argv[2], "w").write(token)
PY
  [ -s "$TMP/token-$name" ] || die "empty token for svc-catalog-$name"
  { printf 'Authorization: Bearer '; cat "$TMP/token-$name"; printf '\n'; } > "$TMP/hdr-$name"
  chmod 600 "$TMP/hdr-$name"
}

# --- c. port-forward and current state ---------------------------------------
kubectl -n "$NAMESPACE" port-forward svc/lakekeeper 18181:8181 >/dev/null 2>"$TMP/pf.log" &
PF_PID=$!
for _ in $(seq 1 40); do
  curl -sf "$BASE/health" >/dev/null 2>&1 && break
  sleep 0.5
done
curl -sf "$BASE/health" >/dev/null 2>&1 || die "port-forward to Lakekeeper failed"

# The bridge's token is the one that reads and writes the assignment list.
token bridge

has_modify() { # has_modify <assignments.json> — prints yes when the principal holds modify
  python3 - "$1" "$SUBJECT" <<'PY'
import json, sys
assignments = json.load(open(sys.argv[1])).get("assignments", [])
subject = "oidc~" + sys.argv[2]
print("yes" if any(a.get("type") == "modify" and a.get("user") == subject
                   for a in assignments) else "no")
PY
}

grant_one() { # grant_one <warehouse name>
  local w=$1 id=${WAREHOUSE_ID[$1]} assignments
  assignments=$TMP/warehouse-$w-assignments.json
  curl -sS -f -H @"$TMP/hdr-bridge" \
    "$BASE/management/v1/permissions/warehouse/$id/assignments" > "$assignments" \
    || die "Lakekeeper refused the assignment list of $w"
  if [ "$(has_modify "$assignments")" = yes ]; then
    if [ "$ACTION" = check ]; then
      ok "modify on warehouse $w ($id) for $PRINCIPAL"
    else
      printf '› warehouse %s: unchanged — modify already granted\n' "$w"
    fi
    return 0
  fi

  if [ "$ACTION" = check ]; then
    printf 'MISSING  modify on warehouse %s (%s) for %s — run: make demo-dbt-grant YES=1\n' \
      "$w" "$id" "$PRINCIPAL" >&2
    return 1
  fi

  # One assignment on the warehouse reaches every schema and table under it, and nothing
  # outside it. The subject goes in a 0600 file, never on the command line.
  local payload=$TMP/grant-$w.json code
  python3 - "$SUBJECT" > "$payload" <<'PY'
import json, sys
json.dump({"writes": [{"type": "modify", "user": "oidc~" + sys.argv[1]}], "deletes": []}, sys.stdout)
PY
  chmod 600 "$payload"
  code=$(curl -sS -o "$TMP/grant-$w-response.json" -w '%{http_code}' \
    -H @"$TMP/hdr-bridge" -H 'Content-Type: application/json' \
    --data @"$payload" \
    "$BASE/management/v1/permissions/warehouse/$id/assignments")
  case "$code" in
    200|204) ;;
    *) die "Lakekeeper answered HTTP $code to the grant on $w — nothing was changed" ;;
  esac
  printf '› warehouse %s (%s): modify granted to %s\n' "$w" "$id" "$PRINCIPAL"
}

missing=0
for w in "${WAREHOUSES[@]}"; do
  grant_one "$w" || missing=$((missing + 1))
done

if [ "$ACTION" = check ] && [ "$missing" -gt 0 ]; then
  info "$missing of ${#WAREHOUSES[@]} warehouses without modify for the shop's dbt run"
  exit 1
fi
