#!/usr/bin/env bash
# Prove the deployed OPA bridge loaded #1490's policy document and answers both
# entry points Trino will call (stardelt/stardelt#1501).
#
#   make verify-bridge-policy        # or: bash scripts/verify-bridge-policy.sh
#
# `test-bridge-policies.sh` proves the rendered bundle in CI, against a fresh
# render. This proves the *running* bridge: OPA reads its bundle when it starts,
# so a `helm upgrade` that only changes the `lakekeeper-opa-policies` ConfigMap
# leaves the old bundle in memory — the unit harness is green while
# `data.stardelt.row_filters` is not defined at all (measured 2026-11-24: merged
# 01:19Z, the live bridge still served the 16-file bundle until `make
# step-lakekeeper` at 01:43Z). Four rows:
#   1. the ConfigMap carries `stardelt.policy.rego` and `stardelt.front.rego`;
#   2. `GetRowFilters` answers `{"result":[]}` — the entry point exists and no
#      rule means no filter (a partial set with no members is *defined*);
#   3. `GetColumnMask` has **no** `result` — undefined means no mask. `[]` is
#      refused by the plugin ("Failed to deserialize OPA policy response", F2 of
#      the row-filter spike);
#   4. another table answers `[]` too: an empty document is not a blanket filter.
#
# The bodies are the ones Trino sends, as captured by the spike and built in
# scripts/tests/test_bridge_policies.py (`row_filter_body`/`column_mask_body`).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NAMESPACE:-stardelt}"

for tool in kubectl curl python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "verify-bridge-policy: $tool not found" >&2; exit 1; }
done

bad() { echo "✗ $1: $2"; exit 1; }

# --- the bridge, through a port-forward (the shape catalog-authz-bootstrap.sh uses) --------
port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()')"
base="http://localhost:$port"
log="$(mktemp)"
trap 'rm -f "$log"' EXIT

kubectl -n "$NAMESPACE" port-forward svc/lakekeeper-opa "$port":8282 >/dev/null 2>"$log" &
pf_pid=$!
trap 'kill "$pf_pid" 2>/dev/null || true; rm -f "$log"' EXIT

health=""
for _ in $(seq 1 30); do
  health="$(curl -fsS "$base/health" 2>/dev/null || true)"
  [ -n "$health" ] && break
  sleep 0.5
done
[ -n "$health" ] || bad "port-forward" "svc/lakekeeper-opa in $NAMESPACE did not answer /health within 15 s ($(tail -n 1 "$log" 2>/dev/null))"

ask() { # ask <entry point> <file with the input body> -> the response body, exit 1 on a non-200
  local code
  code="$(curl -sS -o "$log.body" -w '%{http_code}' -X POST "$base/v1/data/$1" \
    -H 'Content-Type: application/json' --data-binary @"$2")" || true
  [ "$code" = "200" ] || bad "$1" "HTTP $code: $(cut -c1-300 "$log.body")"
  cat "$log.body"
}

# --- row 1: the ConfigMap carries the policy document and the entry points ----------------
data="$(kubectl -n "$NAMESPACE" get configmap lakekeeper-opa-policies -o jsonpath='{.data}')" \
  || bad "policy document" "the ConfigMap lakekeeper-opa-policies is not in $NAMESPACE"
files="$(printf '%s' "$data" | python3 -c '
import json, sys
data = json.load(sys.stdin)
missing = [n for n in ("stardelt.policy.rego", "stardelt.front.rego") if n not in data]
print("missing " + " ".join(missing) if missing else len(data))
')" || bad "policy document" "the ConfigMap data is not JSON"
case "$files" in
  missing*) bad "policy document" "lakekeeper-opa-policies has no ${files#missing } — run make step-lakekeeper" ;;
esac
echo "✓ policy document: stardelt.policy.rego and stardelt.front.rego are in lakekeeper-opa-policies ($files files)"

# --- row 2: GetRowFilters is defined and answers [] --------------------------------------
cat >"$log.row2" <<'JSON'
{"input":{"context":{"identity":{"user":"verify","groups":["wh-verify"]}},"action":{"operation":"GetRowFilters","resource":{"table":{"catalogName":"bronze","schemaName":"shop","tableName":"customers"}}}}}
JSON
answer="$(ask stardelt/row_filters "$log.row2")"
verdict="$(printf '%s' "$answer" | python3 -c '
import json, sys
body = json.loads(sys.stdin.read())
if "result" not in body:
    print("the bridge did not load the policy document — run make step-lakekeeper")
elif body["result"] != []:
    print("expected [] (no rule = no filter), got " + json.dumps(body["result"])[:200])
else:
    print("ok")
')" || bad row_filters "the answer is not JSON: $(printf '%s' "$answer" | cut -c1-300)"
[ "$verdict" = "ok" ] || bad row_filters "$verdict"
echo "✓ row_filters: [] (no rule = no filter)"

# --- row 3: GetColumnMask stays undefined ------------------------------------------------
cat >"$log.row3" <<'JSON'
{"input":{"context":{"identity":{"user":"verify","groups":["wh-verify"]},"softwareStack":{"trinoVersion":"484"},"properties":{}},"action":{"operation":"GetColumnMask","resource":{"column":{"catalogName":"bronze","schemaName":"shop","tableName":"customers","columnName":"name","columnType":"varchar"}}}}}
JSON
answer="$(ask stardelt/column_masks "$log.row3")"
verdict="$(printf '%s' "$answer" | python3 -c '
import json, sys
body = json.loads(sys.stdin.read())
if "result" not in body:
    print("ok")
elif body["result"] == []:
    print("the plugin refuses an empty array — undefined is \"no mask\" (Trino answers "
          "\"GENERIC_INTERNAL_ERROR: Failed to deserialize OPA policy response\")")
else:
    print("expected no result (undefined = no mask), got " + json.dumps(body)[:200])
')" || bad column_masks "the answer is not JSON: $(printf '%s' "$answer" | cut -c1-300)"
[ "$verdict" = "ok" ] || bad column_masks "$verdict"
echo "✓ column_masks: undefined (no rule = no mask)"

# --- row 4: another table is not a blanket filter ----------------------------------------
cat >"$log.row4" <<'JSON'
{"input":{"context":{"identity":{"user":"verify","groups":["wh-verify"]}},"action":{"operation":"GetRowFilters","resource":{"table":{"catalogName":"gold","schemaName":"other","tableName":"t"}}}}}
JSON
answer="$(ask stardelt/row_filters "$log.row4")"
verdict="$(printf '%s' "$answer" | python3 -c '
import json, sys
body = json.loads(sys.stdin.read())
if "result" not in body:
    print("the bridge did not load the policy document — run make step-lakekeeper")
elif body["result"] != []:
    print("expected [] (the empty document is not a blanket filter), got " + json.dumps(body["result"])[:200])
else:
    print("ok")
')" || bad "another table" "the answer is not JSON: $(printf '%s' "$answer" | cut -c1-300)"
[ "$verdict" = "ok" ] || bad "another table" "$verdict"
echo "✓ another table: [] (the empty document is not a blanket filter)"

echo "verify-bridge-policy: PASS (4 of 4)"
