#!/usr/bin/env bash
# Prove what a signed-in person may read in Trino's `system` catalog (platform#416).
#
#   NAMESPACE=stardelt TRINO_URL=https://trino.alpha.dev.stardelt.io \
#     bash scripts/verify-trino-system-catalog.sh
#
# `system` is read-only for everyone but administrators (decision on #366, option A):
# dbt-trino reads `system.metadata.materialized_views` before every run, so `SELECT` is
# allowed; `CALL` and the runtime tables stay denied, and `tpch`/`tpcds` stay closed.
#
# Four checks, all with a real token for the public demo person (`demo`/`demo`):
#   1. read      `select count(*) from system.metadata.materialized_views` -> a number;
#   2. runtime   `select count(*) from system.runtime.tasks` -> Access Denied;
#   3. CALL      `call system.runtime.kill_query(...)` -> Access Denied (`read-only` is
#                SELECT only);
#   4. filter    `system.metadata.table_comments` for a catalog `demo` may not read ->
#                `0`, so `read-only` did not become "read every catalog's metadata".
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NAMESPACE:-stardelt}"
TRINO_URL="${TRINO_URL:-https://trino.alpha.dev.stardelt.io}"

for tool in kubectl python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "verify-trino-system-catalog: $tool not found" >&2; exit 1; }
done

umask 077
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

fail() { echo "verify-trino-system-catalog: $*" >&2; exit 1; }

# run <sql> -> {"rows": [...], "error": "<message>"|null} on stdout. The token is read
# from the file, never argv, and the poll requests carry no credentials (the nextUri is
# the capability, as in verify-trino-auth.sh).
run() {
  python3 - "$1" "$tmp/t" "$TRINO_URL" <<'PY'
import json, sys, urllib.error, urllib.request
sql, token_file, trino = sys.argv[1], sys.argv[2], sys.argv[3].rstrip("/")
token = open(token_file).read().strip()

def step(url, data=None):
    headers = {} if data is None else {"Authorization": "Bearer " + token,
                                       "Content-Type": "text/plain"}
    req = urllib.request.Request(url, data=data, headers=headers,
                                 method="POST" if data is not None else "GET")
    try:
        with urllib.request.urlopen(req) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            return json.loads(raw)
        except ValueError:
            return {"error": {"message": "HTTP %s: %s" % (e.code, raw[:200])}}

rows, error = [], None
body = step(trino + "/v1/statement", sql.encode())
while True:
    if body.get("error"):
        error = body["error"].get("message") or json.dumps(body["error"])
        break
    if not body.get("id") and not body.get("nextUri"):
        error = "no query id: " + json.dumps(body)[:200]
        break
    rows.extend(body.get("data") or [])
    if not body.get("nextUri"):
        break
    body = step(body["nextUri"])
print(json.dumps({"rows": rows, "error": error}))
PY
}

# value <json> -> the scalar of the single count row, or "" when the run failed.
value() {
  python3 -c 'import json,sys
d=json.load(sys.stdin)
print(d["rows"][0][0] if d["rows"] and d["rows"][0] else "")' <<<"$1"
}
# error_of <json> -> the error message, or "".
error_of() {
  python3 -c 'import json,sys
print(json.load(sys.stdin).get("error") or "")' <<<"$1"
}

echo "› minting a token for the public demo person"
kubectl -n "$NAMESPACE" get secret stardelt-oidc-clients -o jsonpath='{.data.trino}' | base64 -d > "$tmp/trino-secret"
[ -s "$tmp/trino-secret" ] || fail "stardelt-oidc-clients has no 'trino' key — run scripts/identity-bootstrap.sh"
ISSUER="$(kubectl -n "$NAMESPACE" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}')"
[ -n "$ISSUER" ] || fail "ConfigMap stardelt-identity has no issuerUrl — run 'make install-identity'"
ISSUER="$ISSUER" TRINO_URL="$TRINO_URL" USER=demo PASSWORD=demo \
  CLIENT_SECRET_FILE="$tmp/trino-secret" MINT_ONLY=1 TOKEN_FILE="$tmp/t" \
  python3 scripts/verify-trino-auth.py >/dev/null
[ -s "$tmp/t" ] || fail "no token was minted for 'demo'"

passed=0

echo "› 1/4 read: system.metadata.materialized_views"
out="$(run 'select count(*) from system.metadata.materialized_views')"
err="$(error_of "$out")"; n="$(value "$out")"
[ -z "$err" ] || fail "1/4 read denied: $err"
printf '%s' "$n" | grep -Eq '^[0-9]+$' || fail "1/4 expected a count, got: $out"
echo "  materialized_views: $n"
passed=$((passed + 1))

echo "› 2/4 runtime table stays denied: system.runtime.tasks"
out="$(run 'select count(*) from system.runtime.tasks')"
err="$(error_of "$out")"
printf '%s' "$err" | grep -q 'Access Denied' || fail "2/4 expected Access Denied, got: ${err:-rows $(value "$out")}"
echo "  runtime.tasks: Access Denied"
passed=$((passed + 1))

echo "› 3/4 CALL stays denied: system.runtime.kill_query"
out="$(run "call system.runtime.kill_query(query_id => 'x', message => 'x')")"
err="$(error_of "$out")"
printf '%s' "$err" | grep -q 'Access Denied' || fail "3/4 expected Access Denied, got: ${err:-rows $(value "$out")}"
echo "  kill_query: Access Denied"
passed=$((passed + 1))

echo "› 4/4 metadata of another catalog is filtered: table_comments where catalog_name = 'mobility'"
out="$(run "select count(*) from system.metadata.table_comments where catalog_name = 'mobility'")"
err="$(error_of "$out")"; n="$(value "$out")"
[ -z "$err" ] || fail "4/4 query failed: $err"
[ "$n" = "0" ] || fail "4/4 expected 0 rows for the mobility catalog, got: $n"
echo "  table_comments(mobility): 0"
passed=$((passed + 1))

echo "verify-trino-system-catalog: $passed/4 checks passed"
