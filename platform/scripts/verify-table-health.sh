#!/usr/bin/env bash
# The freshness promise round-trips, live as the person `demo` (stardelt/stardelt#402 slice 1,
# #1582). The live half of the table-health API that #1594 asks for.
#
#   make verify-table-health        (STARDELT_NOVA_URL=…, ISSUER=… to override)
#   bash scripts/verify-table-health.sh
#
# Nova's unit tests prove the freshness answer against a stub catalog, and the read proves
# `canEdit` against the interim rule. This row proves the promise survives the trip through
# Trino, Lakekeeper and back on the public demo, as a visitor who owns the table's group:
#
#   create       POST   /api/statements                  the probe table exists
#   status-grey  GET    …/health?catalog=playground      grey, and canEdit is true
#   refuse-bad   PUT    {"by":"25:00",…}                 400, "the deadline is a 24-hour clock time like 07:00"
#   set          PUT    {"by":"<now + 2m>",…}            200, green, promise.by echoes, dueAt is set
#   persist      GET    …/health?catalog=playground      green, and promise.by is still the sent text
#   remove       PUT    {"by":null}                      200, promise null, grey
#
# `demo` cannot drop its own table (measured 2026-11-25: `DROP TABLE` answers PERMISSION_DENIED),
# so the check never drops it: it reuses one fixed name with `CREATE TABLE IF NOT EXISTS`.
# `playground.scratch.verify_health_probe` is therefore left behind, empty, and the nightly reset
# (`scripts/demo-reset-job.py`, 03:00 UTC) drops every playground table and recreates `scratch`,
# so the residue disappears within a day. No promise is left behind: the `remove` row deletes it
# and a `trap` on EXIT repeats the deletion when the run fails earlier.
#
# Why `now + 2 minutes` is green at any hour: the most recent deadline at or before `now` is
# then yesterday's wall clock, and the newest write (today's `CREATE`, or an earlier run today)
# is after it.
#
# The password is `demo` and it is PUBLIC ON PURPOSE (`scripts/public-demo.sh` prints it, so it
# protects nothing). The password and the token live in `mktemp` files (0600, removed on every
# exit) and nothing here prints the token. The token comes from `scripts/mint-user-token.py` on
# the public client `stardelt-dev`, exactly as `verify-access-requests.sh` mints it.
#
# Exit 0 = PASS (6 of 6), 1 = FAIL (the rows that passed are in the last line; every row still
# runs, and each ✗ line names why).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"; NOVA="${NOVA%/}"
ISSUER="${ISSUER:-https://id.alpha.dev.stardelt.io/realms/stardelt}"; ISSUER="${ISSUER%/}"
TOTAL_ROWS=6
TABLE=playground.scratch.verify_health_probe
HEALTH_PATH="/api/catalog/tables/scratch/verify_health_probe/health?catalog=playground"

umask 077
tmp="$(mktemp -d)"
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

cleanup() {
  if [ -s "$TOKEN_FILE" ]; then
    curl -sS -o /dev/null -X PUT \
      -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
      -H 'Content-Type: application/json' \
      --data '{"by":null}' "$NOVA$HEALTH_PATH" || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

for tool in curl python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "verify-table-health: FAIL $tool not found"; exit 1; }
done

# One field out of the answer in $BODY_FILE; a dotted name walks into the object (`promise.by`).
# `null` for a JSON null, `!missing` when the body has no such key, `!not JSON: …` when it is
# not a JSON object at all.
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
value = body
for part in name.split("."):
    if not isinstance(value, dict) or part not in value:
        print("!missing")
        raise SystemExit(0)
    value = value[part]
if value is None:
    print("null")
elif isinstance(value, list):
    print(repr(value))
elif isinstance(value, bool):
    print("true" if value else "false")
else:
    print(value)
PY

PASSED=0
FAILED=0
ok()  { printf '✓ %-12s %s\n' "$1" "$2"; PASSED=$((PASSED + 1)); }
bad() { printf '✗ %-12s %s\n' "$1" "$2"; FAILED=$((FAILED + 1)); }
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

# ---- create: the probe table exists; `demo` can add to `scratch` but cannot drop -----------------

row_create() {
  api POST /api/statements \
    "{\"sql\":\"CREATE TABLE IF NOT EXISTS $TABLE (id bigint)\",\"waitSeconds\":45}"
  if [ "$STATUS" != 200 ]; then
    bad create "POST /api/statements answered $STATUS, not 200: $(answer)"
    return
  fi
  local state
  state="$(field state)"
  if [ "$state" != SUCCEEDED ]; then
    bad create "the CREATE statement's state is '$state', not SUCCEEDED: $(answer)"
  else
    ok create "the statement SUCCEEDED"
  fi
}

# ---- status-grey: a table with no promise is grey, and its owner group may set one ------------

row_status_grey() {
  api GET "$HEALTH_PATH"
  if [ "$STATUS" != 200 ]; then
    bad status-grey "GET answered $STATUS, not 200: $(answer)"
    return
  fi
  local status promise canedit
  status="$(field status)"
  promise="$(field promise)"
  canedit="$(field canEdit)"
  if [ "$status" != grey ]; then
    bad status-grey "the status is '$status', not grey: $(answer)"
  elif [ "$promise" != null ]; then
    bad status-grey "the promise is '$promise', not null: $(answer)"
  elif [ "$canedit" != true ]; then
    bad status-grey "canEdit is '$canedit', not true: $(answer)"
  else
    ok status-grey "200 status grey, promise null, canEdit true"
  fi
}

# ---- refuse-bad: a deadline that is not a 24-hour clock time is the route's sentence -----------

row_refuse_bad() {
  api PUT "$HEALTH_PATH" '{"by":"25:00","timezone":"UTC","graceMinutes":0}'
  local error
  error="$(field error)"
  if [ "$STATUS" != 400 ]; then
    bad refuse-bad "'25:00' answered $STATUS, not 400: $(answer)"
  elif [ "$error" != "the deadline is a 24-hour clock time like 07:00" ]; then
    bad refuse-bad "the answer is '$error', not the route's deadline sentence"
  else
    ok refuse-bad "400 the deadline is a 24-hour clock time like 07:00"
  fi
}

# ---- set: the promise is written, and the answer to the write already agrees --------------------

BY=""
row_set() {
  BY="$(date -u -d '+2 minutes' +%H:%M)"
  api PUT "$HEALTH_PATH" "{\"by\":\"$BY\",\"timezone\":\"UTC\",\"graceMinutes\":0}"
  if [ "$STATUS" != 200 ]; then
    bad set "the PUT answered $STATUS, not 200: $(answer)"
    return
  fi
  local status promise_by due
  status="$(field status)"
  promise_by="$(field promise.by)"
  due="$(field dueAt)"
  if [ "$status" != green ]; then
    bad set "by $BY answered status '$status', not green: $(answer)"
  elif [ "$promise_by" != "$BY" ]; then
    bad set "promise.by is '$promise_by', not the sent '$BY': $(answer)"
  elif [ -z "$due" ] || [ "$due" = null ] || [ "${due#!}" != "$due" ]; then
    bad set "dueAt is '$due', not a timestamp: $(answer)"
  else
    ok set "200 green promise.by $BY dueAt $due"
  fi
}

# ---- persist: the read a page makes sees the same promise --------------------------------------

row_persist() {
  api GET "$HEALTH_PATH"
  if [ "$STATUS" != 200 ]; then
    bad persist "GET answered $STATUS, not 200: $(answer)"
    return
  fi
  local status promise_by
  status="$(field status)"
  promise_by="$(field promise.by)"
  if [ "$status" != green ]; then
    bad persist "the read status is '$status', not green: $(answer)"
  elif [ "$promise_by" != "$BY" ]; then
    bad persist "the read promise.by is '$promise_by', not the sent '$BY': $(answer)"
  else
    ok persist "200 green, promise.by still $BY"
  fi
}

# ---- remove: `by: null` deletes the promise and the table is grey again ------------------------

row_remove() {
  api PUT "$HEALTH_PATH" '{"by":null}'
  if [ "$STATUS" != 200 ]; then
    bad remove "{\"by\":null} answered $STATUS, not 200: $(answer)"
    return
  fi
  local promise status
  promise="$(field promise)"
  status="$(field status)"
  if [ "$promise" != null ]; then
    bad remove "the promise after the removal is '$promise', not null: $(answer)"
  elif [ "$status" != grey ]; then
    bad remove "the status after the removal is '$status', not grey: $(answer)"
  else
    ok remove "200 promise null, status grey"
  fi
}

# ---- the token: minted once, on the public client the visitor signs in with --------------------

if ! ISSUER="$ISSUER" USER=demo PASSWORD_FILE="$PASSWORD_FILE" TOKEN_FILE="$TOKEN_FILE" \
       CLIENT_ID=stardelt-dev python3 scripts/mint-user-token.py >"$tmp/mint.log" 2>&1 \
   || [ ! -s "$TOKEN_FILE" ]; then
  why="not run: cannot mint a token for demo: $(first_line "$(cat "$tmp/mint.log")")"
  for name in create status-grey refuse-bad set persist remove; do
    bad "$name" "$why"
  done
else
  row_create
  row_status_grey
  row_refuse_bad
  row_set
  row_persist
  row_remove
fi

if [ "$PASSED" -eq "$TOTAL_ROWS" ] && [ "$FAILED" -eq 0 ]; then
  echo "verify-table-health: PASS ($TOTAL_ROWS of $TOTAL_ROWS)"
else
  echo "verify-table-health: FAIL ($PASSED of $TOTAL_ROWS)"
  exit 1
fi
