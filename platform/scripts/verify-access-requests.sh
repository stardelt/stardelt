#!/usr/bin/env bash
# The visitor's access-request API, live as the person `demo` (stardelt/stardelt#489 slice 1,
# #1576).
#
#   make verify-access-requests        (STARDELT_NOVA_URL=…, ISSUER=… to override)
#   bash scripts/verify-access-requests.sh
#
# Asking for catalog access is the only flow a visitor drives without an administrator, and
# `nova/backend/src/access_requests.rs` is the route table behind it. Its unit tests prove the
# sentences against a stub store and `verify-contract` proves the four routes are served; this
# row proves Nova, its database and a realm token agree:
#
#   create         POST   /api/access-requests        201, state open, the caller as requester
#   read           GET    /api/access-requests/{id}   200, the same row
#   withdraw       DELETE /api/access-requests/{id}   200, state withdrawn
#   withdraw-again DELETE /api/access-requests/{id}   409, "this request was withdrawn"
#   refuse-group   POST   with group `demo`           403, "you are not a member of group demo"
#   refuse-manage  POST   with verbs `["manage"]`     400, the catalog owner's verb sentence
#   foreign        GET    /api/access-requests/{id}   404, "not found"
#
# The refusals are the sentences the route answers with, quoted verbatim: a translation or a
# reworded message fails the row.
#
# The password is `demo` and it is PUBLIC ON PURPOSE: `scripts/public-demo.sh` creates that
# person and prints it, so it protects nothing. The password and the token live in `mktemp`
# files (0600, removed on every exit) and nothing here prints the token. The token comes from
# `scripts/mint-user-token.py` on the public client `stardelt-dev`, exactly as `verify-cli.sh`
# mints it.
#
# A withdrawn request is history and cannot be deleted; every run leaves one `withdrawn` row
# in `demo`'s own list (capped at 50, and this check does not read it), and a withdrawn
# request changes no grant. An open request is withdrawn on any exit, so a failure before row
# 3 leaves nothing behind.
#
# Exit 0 = PASS (7 of 7), 1 = FAIL (the rows that passed are in the last line; every row still
# runs, and each ✗ line names why).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"; NOVA="${NOVA%/}"
ISSUER="${ISSUER:-https://id.alpha.dev.stardelt.io/realms/stardelt}"; ISSUER="${ISSUER%/}"
TOTAL_ROWS=7
REASON="the release check asks for catalog access"

umask 077
tmp="$(mktemp -d "${TMPDIR:-/tmp}/verify-access-requests.XXXXXX")"
PASSWORD_FILE="$tmp/password"
TOKEN_FILE="$tmp/token"
BODY_FILE="$tmp/body"
CHECKER="$tmp/check.py"
: > "$PASSWORD_FILE"
: > "$TOKEN_FILE"
: > "$BODY_FILE"
chmod 600 "$PASSWORD_FILE" "$TOKEN_FILE" "$BODY_FILE"
# `mint-user-token.py` strips the trailing newline; the realm compares the exact string.
printf 'demo\n' > "$PASSWORD_FILE"

# The id of the request `create` made, while it is still open: the cleanup withdraws it once.
ID=""
cleanup() {
  if [ -n "$ID" ] && [ -s "$TOKEN_FILE" ]; then
    curl -sS -o /dev/null -X DELETE \
      -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
      "$NOVA/api/access-requests/$ID" || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

for tool in curl python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "verify-access-requests: FAIL $tool not found"; exit 1; }
done

# One field out of the answer in $BODY_FILE: a list as Python repr (`['select']`), `!missing`
# when the body has no such key, `!not JSON: …` when it is not a JSON object at all.
cat > "$CHECKER" <<'PY'
import json
import sys

path, name = sys.argv[1], sys.argv[2]
try:
    with open(path, encoding="utf-8") as handle:
        body = json.load(handle)
except (OSError, ValueError) as exc:
    print("!not JSON: %s" % exc)
    raise SystemExit(0)
if not isinstance(body, dict):
    print("!not a JSON object")
    raise SystemExit(0)
value = body.get(name)
if value is None:
    print("!missing")
elif isinstance(value, list):
    print(repr(value))
elif isinstance(value, bool):
    print("true" if value else "false")
else:
    print(value)
PY

PASSED=0
FAILED=0
ok()  { printf '✓ %-14s %s\n' "$1" "$2"; PASSED=$((PASSED + 1)); }
bad() { printf '✗ %-14s %s\n' "$1" "$2"; FAILED=$((FAILED + 1)); }
first_line() { printf '%s\n' "$1" | head -n 1 | cut -c1-200; }
field() { python3 "$CHECKER" "$BODY_FILE" "$1"; }
answer() { first_line "$(cat "$BODY_FILE")"; }

# api <method> <path> [json]: the status in $STATUS, the body in $BODY_FILE. A curl that does
# not answer is 000, so the row fails on its status instead of on a missing file.
STATUS=000
api() {
  local method="$1" path="$2" data="${3:-}"
  local args=(-sS -o "$BODY_FILE" -w '%{http_code}' -X "$method"
              -H "Authorization: Bearer $(cat "$TOKEN_FILE")")
  if [ -n "$data" ]; then
    args+=(-H 'Content-Type: application/json' --data "$data")
  fi
  if ! STATUS="$(curl "${args[@]}" "$NOVA$path")"; then
    STATUS=000
  fi
}

# ---- create: the visitor asks, and the stored row is theirs -----------------------------------

row_create() {
  api POST /api/access-requests \
    "{\"group\":\"visitors\",\"catalog\":\"bronze\",\"verbs\":[\"select\"],\"reason\":\"$REASON\"}"
  if [ "$STATUS" != 201 ]; then
    bad create "POST answered $STATUS, not 201: $(answer)"
    return
  fi
  local id state requester group catalog verbs
  id="$(field id)"
  state="$(field state)"
  requester="$(field requesterName)"
  group="$(field groupName)"
  catalog="$(field catalog)"
  verbs="$(field verbs)"
  if [ "$state" != open ]; then
    bad create "the new request's state is '$state', not open"
  elif [ "$requester" != demo ]; then
    bad create "the new request's requesterName is '$requester', not demo"
  elif [ "$group" != visitors ]; then
    bad create "the new request's groupName is '$group', not visitors"
  elif [ "$catalog" != bronze ]; then
    bad create "the new request's catalog is '$catalog', not bronze"
  elif [ "$verbs" != "['select']" ]; then
    bad create "the new request's verbs are $verbs, not ['select']"
  elif ! printf '%s' "$id" | grep -qx '[0-9][0-9]*'; then
    bad create "the new request carries no numeric id ('$id')"
  else
    ID="$id"
    ok create "201 id $id open visitors/bronze ['select'] requester demo"
  fi
}

# ---- read: the requester reads the row back ---------------------------------------------------

row_read() {
  if [ -z "$ID" ]; then
    bad read "not run: the create row stored no id"
    return
  fi
  api GET "/api/access-requests/$ID"
  if [ "$STATUS" != 200 ]; then
    bad read "GET answered $STATUS, not 200: $(answer)"
    return
  fi
  local id
  id="$(field id)"
  if [ "$id" != "$ID" ]; then
    bad read "GET answered id '$id', not $ID"
  else
    ok read "200 the same row"
  fi
}

# ---- withdraw: the open request becomes withdrawn, and only once ------------------------------

row_withdraw() {
  if [ -z "$ID" ]; then
    bad withdraw "not run: the create row stored no id"
    return
  fi
  api DELETE "/api/access-requests/$ID"
  if [ "$STATUS" != 200 ]; then
    bad withdraw "DELETE answered $STATUS, not 200: $(answer)"
    return
  fi
  local state
  state="$(field state)"
  if [ "$state" != withdrawn ]; then
    bad withdraw "the withdrawn request's state is '$state', not withdrawn"
  else
    ok withdraw "200 state withdrawn"
  fi
}

row_withdraw_again() {
  if [ -z "$ID" ]; then
    bad withdraw-again "not run: the create row stored no id"
    return
  fi
  api DELETE "/api/access-requests/$ID"
  local error
  error="$(field error)"
  if [ "$STATUS" != 409 ]; then
    bad withdraw-again "the second DELETE answered $STATUS, not 409: $(answer)"
  elif [ "$error" != "this request was withdrawn" ]; then
    bad withdraw-again "the second DELETE answered '$error', not 'this request was withdrawn'"
  else
    ok withdraw-again "409 this request was withdrawn"
  fi
  # The row is history now; the cleanup has nothing left to withdraw.
  ID=""
}

# ---- the refusals: a group the visitor is not in, the owner's verb, a foreign id --------------

row_refuse_group() {
  api POST /api/access-requests \
    "{\"group\":\"demo\",\"catalog\":\"bronze\",\"verbs\":[\"select\"],\"reason\":\"$REASON\"}"
  local error
  error="$(field error)"
  if [ "$STATUS" != 403 ]; then
    bad refuse-group "a group the visitor is not in answered $STATUS, not 403: $(answer)"
  elif [ "$error" != "you are not a member of group demo" ]; then
    bad refuse-group "the answer is '$error', not 'you are not a member of group demo'"
  else
    ok refuse-group "403 you are not a member of group demo"
  fi
}

row_refuse_manage() {
  api POST /api/access-requests \
    "{\"group\":\"visitors\",\"catalog\":\"bronze\",\"verbs\":[\"manage\"],\"reason\":\"$REASON\"}"
  local error
  error="$(field error)"
  if [ "$STATUS" != 400 ]; then
    bad refuse-manage "requesting manage answered $STATUS, not 400: $(answer)"
  elif [ "$error" != "manage belongs to the catalog's owner; request browse, select, write or create" ]; then
    bad refuse-manage "the answer is '$error', not the catalog-owner sentence"
  else
    ok refuse-manage "400 manage belongs to the catalog's owner; request browse, select, write or create"
  fi
}

row_foreign() {
  api GET /api/access-requests/999999999
  local error
  error="$(field error)"
  if [ "$STATUS" != 404 ]; then
    bad foreign "an unknown id answered $STATUS, not 404: $(answer)"
  elif [ "$error" != "not found" ]; then
    bad foreign "an unknown id answered '$error', not 'not found'"
  else
    ok foreign "404 not found"
  fi
}

# ---- the token: minted once, on the public client the visitor signs in with -------------------

if ! ISSUER="$ISSUER" USER=demo PASSWORD_FILE="$PASSWORD_FILE" TOKEN_FILE="$TOKEN_FILE" \
       CLIENT_ID=stardelt-dev python3 scripts/mint-user-token.py >"$tmp/mint.log" 2>&1 \
   || [ ! -s "$TOKEN_FILE" ]; then
  why="not run: cannot mint a token for demo: $(first_line "$(cat "$tmp/mint.log")")"
  for name in create read withdraw withdraw-again refuse-group refuse-manage foreign; do
    bad "$name" "$why"
  done
else
  row_create
  row_read
  row_withdraw
  row_withdraw_again
  row_refuse_group
  row_refuse_manage
  row_foreign
fi

if [ "$PASSED" -eq "$TOTAL_ROWS" ] && [ "$FAILED" -eq 0 ]; then
  echo "verify-access-requests: PASS ($TOTAL_ROWS of $TOTAL_ROWS)"
else
  echo "verify-access-requests: FAIL ($PASSED of $TOTAL_ROWS)"
  exit 1
fi
