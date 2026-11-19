#!/usr/bin/env bash
# Prove Trino is Lakekeeper's trusted engine, and that nothing else is (#393, #486).
#
#   NAMESPACE=stardelt bash scripts/verify-trusted-engine.sh
#   make verify-trusted-engine
#
# Trino creates views with `SECURITY DEFINER` by default and stores their owner in the
# reserved property `trino.run-as-owner`. Lakekeeper 0.15 reserves that property and answers
# `403 ProtectedPropertyModification` to every caller that is not a configured trusted
# engine, so a DEFINER view — dbt's default, and what a pipeline's temp relation wants —
# fails without the setting. The trust is per audience, and only `svc-catalog-trino` carries
# the audience `lakekeeper-trino` (make sync-catalog-clients, helm-values/lakekeeper.yaml).
#
# Three checks:
#   1. create   as the public demo person through Trino: `create or replace view
#               playground.scratch.te_probe` (DEFINER, the default) succeeds;
#   2. read     `select x from playground.scratch.te_probe` returns 1;
#   3. negative `svc-catalog-nova`'s own client-credentials token cannot write the owner
#               property: `POST /catalog/v1/<playground>/namespaces/scratch/views` with
#               `properties["trino.run-as-owner"]` answers `403
#               ProtectedPropertyModification` — the audience is what makes Trino trusted,
#               not the catalog API.
#
# The probe is NOT dropped: `Drop` is `modify` in Lakekeeper 0.15, and the public demo
# person holds browse+select+write+create on `playground`, not `modify` (#486). The check
# reuses the one name with `create or replace`, so it never piles up, and the nightly
# `make demo-reset` purges `playground.scratch`.
#
# Nothing is printed that is not a status: the tokens live in mode-0600 files under a
# temporary directory, and the trap removes them.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NAMESPACE:-stardelt}"
TRINO_URL="${TRINO_URL:-https://trino.alpha.dev.stardelt.io}"
LK_PORT="${LK_PORT:-18186}"
WAREHOUSE=playground
VIEW=te_probe
NOVA_VIEW=te_probe_nova

for tool in kubectl curl python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "verify-trusted-engine: $tool not found" >&2; exit 1; }
done

umask 077
tmp="$(mktemp -d)"
LK_PID=""

fail() { echo "verify-trusted-engine: $*" >&2; exit 1; }

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
        failure = body["error"]
        error = failure.get("message") or json.dumps(failure)
        # A catalog refusal arrives as "Failed to create view 'x'"; the reason (the
        # catalog's own message, e.g. ProtectedPropertyModification) is the cause.
        cause = ((failure.get("failureInfo") or {}).get("cause") or {}).get("message")
        if cause and cause not in error:
            error += ": " + cause
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

# value <json> -> the scalar of the first row, or "" when there is none.
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

# The probe stays behind on purpose (see the header); the port-forward and every token
# file under $tmp go.
# shellcheck disable=SC2329  # invoked by the trap below
cleanup() {
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

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

echo "› 1/3 a SECURITY DEFINER view is created through Trino: playground.scratch.$VIEW"
out="$(run "create or replace view playground.scratch.$VIEW as select 1 as x")"
err="$(error_of "$out")"
[ -z "$err" ] || fail "1/3 create view refused: $err"
echo "  create or replace view: ok"
passed=$((passed + 1))

echo "› 2/3 the view reads back: playground.scratch.$VIEW"
out="$(run "select x from playground.scratch.$VIEW")"
err="$(error_of "$out")"; n="$(value "$out")"
[ -z "$err" ] || fail "2/3 read failed: $err"
[ "$n" = "1" ] || fail "2/3 expected 1, got: ${n:-<no rows>}"
echo "  $VIEW.x: 1"
passed=$((passed + 1))

echo "› 3/3 a non-trusted client cannot write the owner property: svc-catalog-nova"
kubectl -n "$NAMESPACE" get secret svc-catalog-nova -o jsonpath='{.data.client-id}' | base64 -d > "$tmp/nova-id"
kubectl -n "$NAMESPACE" get secret svc-catalog-nova -o jsonpath='{.data.client-secret}' | base64 -d > "$tmp/nova-secret"
[ -s "$tmp/nova-id" ] && [ -s "$tmp/nova-secret" ] || fail "Secret $NAMESPACE/svc-catalog-nova is missing — run 'make sync-catalog-clients'"
curl -fsS -X POST "${ISSUER%/}/protocol/openid-connect/token" \
  --data-urlencode grant_type=client_credentials \
  --data-urlencode client_id@"$tmp/nova-id" \
  --data-urlencode client_secret@"$tmp/nova-secret" \
  -o "$tmp/nova-token.json" || fail "the realm's token endpoint refused svc-catalog-nova"
python3 - "$tmp/nova-token.json" "$tmp/nova-hdr" <<'PY'
import json, sys
token = json.load(open(sys.argv[1]))["access_token"]
open(sys.argv[2], "w").write("Authorization: Bearer " + token + "\n")
PY

# The catalog's REST API on 8181, through a port-forward: the same path the spike scripts
# and `make verify-catalog-permissions` use (the NetworkPolicy admits the pod's own
# callers; a port-forward arrives from the node, which the policy leaves alone).
kubectl -n "$NAMESPACE" port-forward svc/lakekeeper "$LK_PORT:8181" >/dev/null 2>&1 &
LK_PID=$!
LK="http://127.0.0.1:$LK_PORT"
code=000
for _ in $(seq 1 30); do
  code="$(curl -s -o /dev/null -m 2 -w '%{http_code}' "$LK/management/v1/info" 2>/dev/null)" || code=000
  [ "$code" != "000" ] && break
  sleep 1
done
[ "$code" != "000" ] || fail "the catalog did not answer on 127.0.0.1:$LK_PORT within 30s"

curl -sS -H @"$tmp/nova-hdr" "$LK/management/v1/warehouse" -o "$tmp/warehouses.json" \
  || fail "listing warehouses failed"
ID="$(python3 - "$tmp/warehouses.json" "$WAREHOUSE" <<'PY'
import json, sys
body = json.load(open(sys.argv[1]))
warehouses = body.get("warehouses", []) if isinstance(body, dict) else body
print(next((w["id"] for w in warehouses if w.get("name") == sys.argv[2]), ""))
PY
)"
[ -n "$ID" ] || fail "no warehouse named '$WAREHOUSE' (run 'make catalogs')"

python3 - "$tmp/view.json" "$NOVA_VIEW" "$WAREHOUSE" <<'PY'
import json, sys, time
body = {
    "name": sys.argv[2],
    "schema": {"type": "struct", "schema-id": 0,
               "fields": [{"id": 1, "name": "x", "required": False, "type": "int"}]},
    "view-version": {
        "version-id": 1,
        "timestamp-ms": int(time.time() * 1000),
        "schema-id": 0,
        "summary": {"engine-name": "verify-trusted-engine"},
        "representations": [{"type": "sql", "sql": "select 1 as x", "dialect": "trino"}],
        "default-catalog": sys.argv[3],
        "default-namespace": ["scratch"],
    },
    "properties": {"trino.run-as-owner": "verify-trusted-engine"},
}
json.dump(body, open(sys.argv[1], "w"))
PY

code="$(curl -sS -H @"$tmp/nova-hdr" -H 'Content-Type: application/json' -X POST \
  --data @"$tmp/view.json" "$LK/catalog/v1/$ID/namespaces/scratch/views" \
  -o "$tmp/view-out.json" -w '%{http_code}')" || code=000
type="$(python3 -c 'import json,sys
print((json.load(open(sys.argv[1])).get("error") or {}).get("type") or "")' "$tmp/view-out.json" 2>/dev/null || true)"
if [ "$code" != "403" ] || [ "$type" != "ProtectedPropertyModification" ]; then
  # The refusal did not happen, so the view may exist now: remove it before failing.
  curl -sS -H @"$tmp/nova-hdr" -X DELETE \
    "$LK/catalog/v1/$ID/namespaces/scratch/views/$NOVA_VIEW?purgeRequested=true" -o /dev/null 2>/dev/null || true
  fail "3/3 expected 403 ProtectedPropertyModification, got ${code}${type:+ $type}"
fi
echo "  svc-catalog-nova: 403 ProtectedPropertyModification"
passed=$((passed + 1))

echo "verify-trusted-engine: PASS ($passed/3)"
