#!/usr/bin/env bash
# Evals end to end as the public visitor (stardelt/stardelt#2129).
#
#   make verify-evals
#   bash scripts/verify-evals.sh          (STARDELT_NOVA_URL=…, ISSUER=… to override)
#
# The 0.0.9 exit promises that evals gate every model, guide or schema change. `verify-release`
# had rows for the CLI, for Ask and for the product path, but nothing proved the eval store, the
# comparison or the delete route still work. This row asks the API only: no model and no cluster
# object, so a release can run it without an endpoint.
#
#   create   POST /api/catalog/evals       a value eval for bronze.shop.customers = 200
#   pass     POST /api/evals/<id>/runs     rows [[200]] passes
#   fail     POST /api/evals/<id>/runs     rows [[199]] fails with the page's sentence
#   runs     GET  /api/evals/<id>/runs     two runs, the failing one first
#   scoped   GET  /api/catalog/evals       playground lists it, bronze does not
#   delete   DELETE /api/evals/<id>        204
#   gone     GET  /api/evals/<id>/runs     404 {"error":"no such eval"}
#
# `demo` is a member of `visitors` (`GET /api/me`), and only `playground` is writable for that
# group, so the eval's owner group is `visitors`, not `demo`. The password `demo` is PUBLIC ON
# PURPOSE (scripts/public-demo.sh creates that person); the password and token live in two
# `mktemp` files (0600) and nothing here prints the token. The check deletes its eval on every
# exit, so a second run starts from a new id and leaves nothing behind.
#
# Exit 0 = PASS (7 of 7), 1 = FAIL (every row still runs; each ✗ line names why), 3 = UNVERIFIED
# (curl, python3 or jq is missing, or no Nova URL is set). A host that refuses the token is a
# failed row, not an unverified run: the check proved something is wrong.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit

NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"; NOVA="${NOVA%/}"
ISSUER="${ISSUER:-https://id.alpha.dev.stardelt.io/realms/stardelt}"; ISSUER="${ISSUER%/}"
PERSON=demo
CLIENT_ID=stardelt-dev

for tool in curl python3 jq; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "verify-evals: UNVERIFIED ($tool is not installed)"
    exit 3
  fi
done
if [ -z "$NOVA" ]; then
  echo "verify-evals: UNVERIFIED (no Nova URL is set)"
  exit 3
fi

umask 077
tmp="$(mktemp -d "${TMPDIR:-/tmp}/verify-evals.XXXXXX")" || exit 1
EVAL_ID=""
trap 'rm -rf "$tmp"' EXIT
trap 'exit 130' INT TERM

PASSWORD_FILE="$tmp/password"
TOKEN_FILE="$tmp/token"
BODY_FILE="$tmp/body"
: > "$PASSWORD_FILE"
: > "$TOKEN_FILE"
: > "$BODY_FILE"
chmod 600 "$PASSWORD_FILE" "$TOKEN_FILE" "$BODY_FILE"
# `mint-user-token.py` strips the trailing newline; the realm compares the exact string.
printf 'demo\n' > "$PASSWORD_FILE"

PASSED=0
FAILED=0
RAN=0
ok()  { printf '✓ %-14s %s\n' "$1" "$2"; PASSED=$((PASSED + 1)); RAN=$((RAN + 1)); }
bad() {
  printf '✗ %-14s %s\n' "$1" "$2"
  FAILED=$((FAILED + 1))
  case "$2" in "not run"*) ;; *) RAN=$((RAN + 1)) ;; esac
}
first_line() { printf '%s\n' "$1" | head -n 1 | cut -c1-200; }

# The eval the check leaves behind must not survive a failed row or a signal: delete it on exit
# while it still exists. The id is cleared as soon as `delete` succeeds, so this never repeats it.
delete_eval_on_exit() {
  if [ -n "$EVAL_ID" ]; then
    curl -sS -o /dev/null -X DELETE -H "Authorization: Bearer $(cat "$TOKEN_FILE" 2>/dev/null)" \
      "$NOVA/api/evals/$EVAL_ID" >/dev/null 2>&1 || true
  fi
}
trap 'delete_eval_on_exit; rm -rf "$tmp"' EXIT

# call <out-file> <curl args…>: the request as demo, printing only the status code.
call() {
  local out="$1"; shift
  curl -sS -o "$out" -w '%{http_code}' \
    -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
    -H 'content-type: application/json' "$@"
}

# assert <file> <python assertions using d>: the body must satisfy them.
assert_body() {
  python3 - "$1" <<PY
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
$2
PY
}

# ---- the token: minted once on the public client the CLI signs in with -----------------------

TOKEN_OK=0
if ! ISSUER="$ISSUER" USER="$PERSON" PASSWORD_FILE="$PASSWORD_FILE" TOKEN_FILE="$TOKEN_FILE" \
       CLIENT_ID="$CLIENT_ID" python3 scripts/mint-user-token.py >"$tmp/mint.log" 2>&1; then
  bad create "cannot mint a token for $PERSON: $(first_line "$(cat "$tmp/mint.log")")"
elif [ ! -s "$TOKEN_FILE" ]; then
  bad create "minting a token for $PERSON wrote no token"
else
  TOKEN_OK=1
fi

# ---- create: the visitor stores a value eval in the catalog it may write ---------------------

row_create() {
  local status
  status="$(call "$BODY_FILE" -X POST "$NOVA/api/catalog/evals?catalog=playground" \
    -d '{"ownerGroup":"visitors","question":"verify-evals: how many customers does bronze.shop.customers hold?","expected":{"kind":"value","value":200}}')"
  if [ "$status" != 201 ]; then
    bad create "$status, not 201: $(first_line "$(cat "$BODY_FILE")")"
    return
  fi
  if ! assert_body "$BODY_FILE" '
assert d.get("ownerGroup") == "visitors", d.get("ownerGroup")
assert d.get("expected") == {"kind": "value", "value": 200}, d.get("expected")
assert isinstance(d.get("id"), int), d.get("id")
' 2>"$tmp/create.err"; then
    bad create "the stored eval is not a visitors value eval: $(first_line "$(cat "$tmp/create.err")")"
    return
  fi
  EVAL_ID="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$BODY_FILE")"
  if [ -z "$EVAL_ID" ]; then
    bad create "the stored eval carries no id: $(first_line "$(cat "$BODY_FILE")")"
    return
  fi
  ok create "201, eval $EVAL_ID owned by visitors"
}

# ---- pass / fail: the API compares the posted rows with the expected value --------------------

row_pass() {
  local status
  status="$(call "$BODY_FILE" -X POST "$NOVA/api/evals/$EVAL_ID/runs" \
    -d '{"rows":[[200]],"model":"verify-evals","endpoint":"none"}')"
  if [ "$status" != 201 ]; then
    bad pass "$status, not 201: $(first_line "$(cat "$BODY_FILE")")"
    return
  fi
  if ! assert_body "$BODY_FILE" '
assert d.get("passed") is True, d.get("passed")
assert d.get("reason") is None, d.get("reason")
' 2>"$tmp/pass.err"; then
    bad pass "the matching answer did not pass: $(first_line "$(cat "$tmp/pass.err")")"
    return
  fi
  ok pass "201, passed"
}

row_fail() {
  local status reason="the answer's rows differ from the expected rows"
  status="$(call "$BODY_FILE" -X POST "$NOVA/api/evals/$EVAL_ID/runs" \
    -d '{"rows":[[199]],"model":"verify-evals","endpoint":"none"}')"
  if [ "$status" != 201 ]; then
    bad fail "$status, not 201: $(first_line "$(cat "$BODY_FILE")")"
    return
  fi
  if ! assert_body "$BODY_FILE" "
assert d.get('passed') is False, d.get('passed')
assert d.get('reason') == \"$reason\", d.get('reason')
" 2>"$tmp/fail.err"; then
    bad fail "the differing answer did not fail with the page's sentence: $(first_line "$(cat "$tmp/fail.err")")"
    return
  fi
  ok fail "201, \"$reason\""
}

# ---- runs: both verdicts are stored, newest first ---------------------------------------------

row_runs() {
  local status
  status="$(call "$BODY_FILE" -X GET "$NOVA/api/evals/$EVAL_ID/runs")"
  if [ "$status" != 200 ]; then
    bad runs "$status, not 200: $(first_line "$(cat "$BODY_FILE")")"
    return
  fi
  if ! assert_body "$BODY_FILE" '
assert isinstance(d, list), type(d).__name__
assert len(d) == 2, len(d)
assert d[0].get("passed") is False, d[0].get("passed")
assert d[1].get("passed") is True, d[1].get("passed")
assert d[0].get("id") > d[1].get("id"), (d[0].get("id"), d[1].get("id"))
' 2>"$tmp/runs.err"; then
    bad runs "the two runs are not stored failing first: $(first_line "$(cat "$tmp/runs.err")")"
    return
  fi
  ok runs "200, 2 runs, newest first"
}

# ---- scoped: the eval belongs to playground, not to another catalog ---------------------------

row_scoped() {
  local status_playground status_bronze
  status_playground="$(call "$BODY_FILE" -X GET "$NOVA/api/catalog/evals?catalog=playground")"
  if [ "$status_playground" != 200 ]; then
    bad scoped "playground answered $status_playground, not 200: $(first_line "$(cat "$BODY_FILE")")"
    return
  fi
  cp "$BODY_FILE" "$tmp/playground"
  status_bronze="$(call "$BODY_FILE" -X GET "$NOVA/api/catalog/evals?catalog=bronze")"
  if [ "$status_bronze" != 200 ]; then
    bad scoped "bronze answered $status_bronze, not 200: $(first_line "$(cat "$BODY_FILE")")"
    return
  fi
  cp "$BODY_FILE" "$tmp/bronze"
  if ! assert_body "$tmp/playground" "
ids = [row.get('id') for row in d]
assert $EVAL_ID in ids, ids
" 2>"$tmp/scoped.err"; then
    bad scoped "playground does not list eval $EVAL_ID: $(first_line "$(cat "$tmp/scoped.err")")"
    return
  fi
  if ! assert_body "$tmp/bronze" "
ids = [row.get('id') for row in d]
assert $EVAL_ID not in ids, ids
" 2>"$tmp/scoped.err"; then
    bad scoped "bronze lists eval $EVAL_ID, which belongs to playground: $(first_line "$(cat "$tmp/scoped.err")")"
    return
  fi
  ok scoped "playground lists it, bronze does not"
}

# ---- delete / gone: removal works and the eval is really gone ---------------------------------

SAVED_ID=""

row_delete() {
  local status
  SAVED_ID="$EVAL_ID"
  status="$(call "$BODY_FILE" -X DELETE "$NOVA/api/evals/$EVAL_ID")"
  if [ "$status" != 204 ]; then
    bad delete "$status, not 204: $(first_line "$(cat "$BODY_FILE")")"
    return
  fi
  EVAL_ID=""
  ok delete "204"
}

row_gone() {
  local status
  if [ -z "$SAVED_ID" ]; then
    bad gone "not run: no eval was created"
    return
  fi
  status="$(call "$BODY_FILE" -X GET "$NOVA/api/evals/$SAVED_ID/runs")"
  if [ "$status" != 404 ]; then
    bad gone "$status, not 404: $(first_line "$(cat "$BODY_FILE")")"
    return
  fi
  if ! assert_body "$BODY_FILE" '
assert d.get("error") == "no such eval", d.get("error")
' 2>"$tmp/gone.err"; then
    bad gone "the deleted eval answers something else: $(first_line "$(cat "$tmp/gone.err")")"
    return
  fi
  ok gone "404"
}

# ---- the seven rows, in the order the header names them ---------------------------------------

if [ "$TOKEN_OK" != 1 ]; then
  for row in pass fail runs scoped delete gone; do bad "$row" "not run: sign-in minted no token"; done
else
  row_create
  if [ -z "$EVAL_ID" ] && [ -z "$SAVED_ID" ]; then
    # No id means create stored nothing: the rows that need one cannot run. `gone` still runs and
    # says why it did not. A delete failure keeps EVAL_ID, so those rows still run.
    for row in pass fail runs scoped delete; do bad "$row" "not run: create stored no eval"; done
    row_gone
  else
    row_pass
    row_fail
    row_runs
    row_scoped
    row_delete
    row_gone
  fi
fi

if [ "$PASSED" -eq 7 ] && [ "$FAILED" -eq 0 ]; then
  echo "verify-evals: PASS (7 of 7)"
  exit 0
fi
echo "verify-evals: FAIL ($RAN of 7)"
exit 1
