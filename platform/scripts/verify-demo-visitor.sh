#!/usr/bin/env bash
# The public demo's visitor view, measured through Nova's API as the person `demo`
# (stardelt/stardelt#527 — the acceptance of #79 as one command).
#
#   make verify-demo-visitor      (STARDELT_NOVA_URL=…, ISSUER=… to override)
#
# #79 was verified by hand, one page at a time. This is the same measurement, read-only:
#
#   me           GET /api/me             user `demo`, `shared_account: true`
#   groups       the minted token        `preferred_username: demo`, `groups: [visitors]` exactly
#   pipelines    GET /api/pipelines      at least one row, every `ownerGroup` `demo`
#   dbt          GET /api/pipelines/stardelt/shop-dbt  200, a task of kind `dbt`
#   streams      GET /api/streams        every `owner_group` `demo`
#   connections  GET /api/connections    every `usableBy` names `visitors`, no secret-looking config key
#   destinations GET /api/destinations   every `owner_group` `demo`
#   compute      GET /api/compute        a `Running` cluster owned by `visitors` (the sandbox)
#   notebooks    GET /api/notebooks      every `group` `demo`; empty only prints a note
#   lineage      GET /api/lineage/read-tables   a non-empty `tables`
#
# The password is `demo` and it is PUBLIC ON PURPOSE: scripts/public-demo.sh creates that person
# and prints it, so it protects nothing. It is the handoff's one allowed credential. The password
# and the token live in two `mktemp` files (0600, removed on every exit) and nothing here prints
# the token. Every request is a read: nothing on the cluster changes.
#
# PASS (exit 0) = every row held. FAIL (exit 1) = the first row that did not, named in the last
# line — a check that passes before the demo is deployed proves nothing.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"
NOVA="${NOVA%/}"
ISSUER="${ISSUER:-https://id.alpha.dev.stardelt.io/realms/stardelt}"
ISSUER="${ISSUER%/}"

PASSWORD_FILE="$(mktemp)"
TOKEN_FILE="$(mktemp)"
BODY_FILE="$(mktemp)"
chmod 600 "$PASSWORD_FILE" "$TOKEN_FILE" "$BODY_FILE"
# `mint-user-token.py` strips the trailing newline; the realm compares the exact string.
printf 'demo\n' > "$PASSWORD_FILE"

CURRENT_ROW=startup
cleanup() { rm -f "$PASSWORD_FILE" "$TOKEN_FILE" "$BODY_FILE"; }
trap cleanup EXIT
trap 'exit 130' INT TERM

fail() { # fail <reason>: the first breaking row, by its id, in the last line.
  echo "✗ $1"
  echo "verify-demo-visitor: FAIL ($CURRENT_ROW)"
  exit 1
}

for tool in curl python3; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool not found"
done

# Sign in: the visitor's own token through the device grant on the public client `stardelt-dev`.
if ! out="$(ISSUER="$ISSUER" USER=demo PASSWORD_FILE="$PASSWORD_FILE" TOKEN_FILE="$TOKEN_FILE" \
      python3 scripts/mint-user-token.py 2>&1)"; then
  fail "cannot mint a token for demo: $(printf '%s\n' "$out" | head -n 1)"
fi
printf '%s\n' "$out"

# Every API call writes its body to one file: a check parses it right away, so no call's answer
# outlives the next call. A curl that does not answer is a row failure, never a silent empty body.
api() { # api <path>
  if ! curl -fsS -H "Authorization: Bearer $(cat "$TOKEN_FILE")" "$NOVA$1" >"$BODY_FILE"; then
    fail "$1 did not answer"
  fi
}

# check <id> <function>: run one row. The function's first line is `OK <what held>` or
# `FAIL <why not>`; any further line is a note printed before the row's ✓. A FAIL ends the
# script on the first row it can prove.
rows=0
names=""
check() {
  CURRENT_ROW="$1"
  local out verdict notes
  out="$("$2")"
  verdict="$(printf '%s\n' "$out" | head -n 1)"
  notes="$(printf '%s\n' "$out" | tail -n +2)"
  case "$verdict" in
    "OK "*) : ;;
    "FAIL "*) fail "${verdict#FAIL }" ;;
    *) fail "the $1 row answered neither OK nor FAIL: $verdict" ;;
  esac
  [ -z "$notes" ] || printf '%s\n' "$notes"
  rows=$((rows + 1))
  names="${names:+$names, }$1"
  printf '✓ %s: %s\n' "$1" "${verdict#OK }"
}

check_me() {
  api /api/me
  python3 - "$BODY_FILE" <<'PY'
import json, sys

me = json.load(open(sys.argv[1]))
if me.get("user") != "demo":
    print("FAIL /api/me answers user %r, not demo" % (me.get("user"),))
elif me.get("shared_account") is not True:
    print("FAIL /api/me does not mark demo as a shared account (shared_account=%r)"
          % (me.get("shared_account"),))
else:
    print("OK user demo, shared account")
PY
}

check_groups() {
  python3 - "$TOKEN_FILE" <<'PY'
import base64, json, sys

token = open(sys.argv[1]).read().strip()
parts = token.split(".")
if len(parts) != 3:
    print("FAIL the minted token is not a compact JWS (%d segment(s))" % len(parts))
    raise SystemExit
# Padded to a multiple of 4, as base64url needs; the payload is printed as claims, never as a token.
payload = parts[1] + "=" * (-len(parts[1]) % 4)
claims = json.loads(base64.urlsafe_b64decode(payload))
user = claims.get("preferred_username")
groups = claims.get("groups")
if user != "demo":
    print("FAIL the token's preferred_username is %r, not demo" % (user,))
elif groups != ["visitors"]:
    print("FAIL demo's groups are %s, not visitors only" % (json.dumps(groups),))
else:
    print("OK the token names demo in visitors only")
    print("visitor: demo, groups: visitors")
PY
}

check_pipelines() {
  api /api/pipelines
  python3 - "$BODY_FILE" <<'PY'
import json, sys

rows = json.load(open(sys.argv[1])).get("pipelines") or []
if not rows:
    print("FAIL no pipeline is visible to the visitor")
    raise SystemExit
for row in rows:
    owner = row.get("ownerGroup")
    if owner != "demo":
        print("FAIL pipeline %s is visible to visitors but owned by %s"
              % (row.get("name"), owner))
        break
else:
    print("OK %d pipeline(s), all owned by demo" % len(rows))
PY
}

check_dbt() {
  # A 404 is the row this check exists for: shop-dbt without `viewers: [visitors]` is invisible.
  if ! curl -fsS -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
      "$NOVA/api/pipelines/stardelt/shop-dbt" >"$BODY_FILE"; then
    echo "FAIL shop-dbt is not visible to the visitor (it needs viewers=[visitors])"
    return 0
  fi
  python3 - "$BODY_FILE" <<'PY'
import json, sys

pipeline = json.load(open(sys.argv[1]))
kinds = [task.get("kind") or "?" for task in pipeline.get("tasks") or []]
if "dbt" not in kinds:
    print("FAIL shop-dbt is visible but its tasks are %s, not a dbt task"
          % (", ".join(kinds) or "none"))
else:
    print("OK visible, a dbt task")
PY
}

check_streams() {
  api /api/streams
  python3 - "$BODY_FILE" <<'PY'
import json, sys

streams = json.load(open(sys.argv[1])).get("streams") or []
for row in streams:
    owner = row.get("owner_group")
    if owner != "demo":
        print("FAIL stream %s is visible to visitors but owned by %s"
              % (row.get("name"), owner))
        break
else:
    print("OK %d stream(s), all owned by demo" % len(streams))
PY
}

check_connections() {
  api /api/connections
  python3 - "$BODY_FILE" <<'PY'
import json, re, sys

mask = re.compile(r"password|secret|token", re.I)
rows = json.load(open(sys.argv[1])).get("connections") or []
for row in rows:
    if "visitors" not in (row.get("usableBy") or []):
        print("FAIL connection %s is visible to visitors but not usable by them (usableBy=%s)"
              % (row.get("name"), json.dumps(row.get("usableBy") or [])))
        break
    leaked = sorted(key for key in (row.get("config") or {}) if mask.search(key))
    if leaked:
        print("FAIL connection %s carries a secret-looking config key: %s"
              % (row.get("name"), ", ".join(leaked)))
        break
else:
    print("OK %d connection(s), usable by visitors, no value" % len(rows))
PY
}

check_destinations() {
  api /api/destinations
  python3 - "$BODY_FILE" <<'PY'
import json, sys

rows = json.load(open(sys.argv[1])) or []
for row in rows:
    owner = row.get("owner_group")
    if owner != "demo":
        print("FAIL destination %s is visible to visitors but owned by %s"
              % (row.get("name"), owner))
        break
else:
    print("OK %d destination(s), all owned by demo" % len(rows))
PY
}

check_compute() {
  api /api/compute
  python3 - "$BODY_FILE" <<'PY'
import json, sys

clusters = json.load(open(sys.argv[1])).get("clusters") or []
for cluster in clusters:
    owner = (cluster.get("owner") or {}).get("group")
    if cluster.get("phase") == "Running" and owner == "visitors":
        print("OK the demo sandbox is Running for visitors")
        break
else:
    seen = ", ".join("%s(%s, %s)" % (c.get("name"), c.get("phase"),
                                    (c.get("owner") or {}).get("group") or "-")
                     for c in clusters) or "none"
    print("FAIL the demo sandbox is not running (clusters: %s)" % seen)
PY
}

check_notebooks() {
  api /api/notebooks
  python3 - "$BODY_FILE" <<'PY'
import json, sys

rows = json.load(open(sys.argv[1])) or []
for row in rows:
    group = row.get("group")
    if group != "demo":
        print("FAIL notebook %s is visible to visitors but in group %s"
              % (row.get("name"), group))
        break
else:
    # The demo's publish is a later slice: an empty list is correct today, not a failure.
    if rows:
        print("OK %d notebook(s), all in group demo" % len(rows))
    else:
        print("OK 0 notebook(s), none published yet")
        print("note: no notebook is published yet")
PY
}

check_lineage() {
  api /api/lineage/read-tables
  python3 - "$BODY_FILE" <<'PY'
import json, sys

tables = json.load(open(sys.argv[1])).get("tables") or []
if not tables:
    print("FAIL the Lineage page opens empty for the visitor")
else:
    print("OK %d table(s) read by the visitor" % len(tables))
PY
}

check me check_me
check groups check_groups
check pipelines check_pipelines
check shop-dbt check_dbt
check streams check_streams
check connections check_connections
check destinations check_destinations
check compute check_compute
check notebooks check_notebooks
check lineage check_lineage

echo "verify-demo-visitor: PASS ($rows rows: $names)"
