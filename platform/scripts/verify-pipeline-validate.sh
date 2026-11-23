#!/usr/bin/env bash
# The document an agent writes is checked, and nothing is created (stardelt/stardelt#1444, #98).
#
#   make verify-pipeline-validate
#   STARDELT_NOVA_URL=https://stardelt.example.com ISSUER=https://id.stardelt.example.com/realms/stardelt \
#     PERSON=demo OWNER_GROUP=visitors bash scripts/verify-pipeline-validate.sh
#
# `POST /api/pipelines/validate` (nova/backend/src/pipelines.rs, `validate_pipeline`) answers what a
# create of this document would do, without doing it: a refusal is a `200` with `ok: false`, which
# is the answer the CLI turns into `bundle plan`. It has eight unit tests and nothing on an install
# runs it, so a release can ship with the route answering 404, with a rule that no longer refuses,
# or with a body that writes a pipeline, and the gate stays green.
#
#   sign-in          the token `mint-user-token.py` mints for `$PERSON` on the public client
#   accepted         the document an agent writes: 200, `ok: true`, `action: "create"`, no errors
#   refused/cron     the same body plus a cron faster than one run per five minutes: 200, `ok:
#                    false`, one error whose message is `at most one run every 5 minutes`
#   refused/cycle    a task list whose two tasks depend on each other: 200, `ok: false`, an error
#                    whose message says `task a is part of a dependency cycle`
#   nothing-written  GET /api/pipelines/stardelt/$NAME: 404. Any other status means a validate
#                    wrote a pipeline, which is the whole point of the route
#
# The person and the owner group are the check's inputs: PERSON (default `demo`) and OWNER_GROUP
# (default `visitors`, the demo person's own group; the group's principal `pipeline-visitors` must
# be Ready). The password is `demo` and it is PUBLIC ON PURPOSE: `scripts/public-demo.sh` creates
# that person and prints it, so it protects nothing. The password, the token and every body live in
# `mktemp` files (0600, in a 0700 directory, removed on every exit) and nothing here prints the
# token. No request writes: the three bodies only ask, and the fourth call reads.
#
# PASS (exit 0) = all five rows held. FAIL (exit 1) = at least one row did not, and the last line
# names the first failing row — a check that passes before the route is deployed proves nothing.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"
NOVA="${NOVA%/}"
ISSUER="${ISSUER:-https://id.${NOVA#*://}/realms/stardelt}"
ISSUER="${ISSUER%/}"

PERSON="${PERSON:-demo}"
OWNER_GROUP="${OWNER_GROUP:-visitors}"
NAME="verify-validate-$(date +%s)"
PIPELINES_NS=stardelt

umask 077
TMP="$(mktemp -d "${TMPDIR:-/tmp}/verify-pipeline-validate.XXXXXX")"
PASSWORD_FILE="$TMP/password"
TOKEN_FILE="$TMP/token"
ACCEPTED_FILE="$TMP/accepted.json"
CRON_FILE="$TMP/cron.json"
CYCLE_FILE="$TMP/cycle.json"
BODY_FILE="$TMP/body"
ERROR_FILE="$TMP/error"

CURRENT_ROW=startup
trap 'rm -rf "$TMP"' EXIT
trap 'exit 130' INT TERM

fail() { # fail <reason>: a row that could not even be asked; nothing after it is proof.
  echo "✗ $1"
  echo "verify-pipeline-validate: FAIL ($CURRENT_ROW)"
  exit 1
}

for tool in curl python3; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool not found"
done

PASSED=0
FAILED=0
FIRST_FAILED=
ok() { printf '✓ %-16s %s\n' "$1" "$2"; PASSED=$((PASSED + 1)); }
bad() { # bad <row> <reason>: one ✗ line; the row is remembered as the first failure.
  printf '✗ %-16s %s\n' "$1" "$2"
  FAILED=$((FAILED + 1))
  [ -n "$FIRST_FAILED" ] || FIRST_FAILED="$1"
}

# request <method> <path> <out-file> [request-body-file]: the status code on stdout; the answer in
# out-file, curl's own message in $ERROR_FILE. A host that never answered prints `000`.
request() {
  local method="$1" path="$2" out="$3" body="${4:-}" code
  local args=(--silent --show-error --connect-timeout 10 --max-time 30
              -X "$method" -o "$out" -w '%{http_code}'
              -H "Authorization: Bearer $(cat "$TOKEN_FILE")")
  if [ -n "$body" ]; then
    args+=(-H 'content-type: application/json' --data-binary "@$body")
  fi
  code="$(curl "${args[@]}" "$NOVA$path" 2>"$ERROR_FILE")" || code=000
  [ -n "$code" ] || code=000
  printf '%s' "$code"
}

# describe <status>: the status, or `unreachable` with curl's own message.
describe() {
  if [ "$1" = "000" ] || [ -z "$1" ]; then
    printf 'unreachable (%s)' "$(tr -d '\r' <"$ERROR_FILE" 2>/dev/null | tr '\n' ' ' | sed -e 's/[[:space:]]*$//')"
  else
    printf '%s' "$1"
  fi
}

# ---- sign-in: the person's own token on the public client -----------------------------------

CURRENT_ROW=sign-in
if ! printf 'demo\n' >"$PASSWORD_FILE" || [ ! -s "$PASSWORD_FILE" ]; then
  bad sign-in "cannot write the password file"
elif ! ISSUER="$ISSUER" USER="$PERSON" PASSWORD_FILE="$PASSWORD_FILE" TOKEN_FILE="$TOKEN_FILE" \
       python3 scripts/mint-user-token.py >"$TMP/mint.log" 2>&1; then
  bad sign-in "minting a token for $PERSON failed: $(tail -n 1 "$TMP/mint.log" | cut -c1-200)"
elif [ ! -s "$TOKEN_FILE" ]; then
  bad sign-in "minting a token for $PERSON wrote no token"
else
  ok sign-in "$PERSON"
fi

if [ ! -s "$TOKEN_FILE" ]; then
  bad accepted "not run: sign-in minted no token"
  bad refused/cron "not run: sign-in minted no token"
  bad refused/cycle "not run: sign-in minted no token"
  bad nothing-written "not run: sign-in minted no token"
else

  # ---- accepted: the document an agent writes -----------------------------------------------

  CURRENT_ROW=accepted
  printf '{"name":"%s","ownerGroup":"%s","sql":"SELECT 1 AS one"}\n' "$NAME" "$OWNER_GROUP" \
    >"$ACCEPTED_FILE"
  status="$(request POST /api/pipelines/validate "$BODY_FILE" "$ACCEPTED_FILE")"
  if [ "$status" = "200" ]; then
    if detail="$(python3 - "$BODY_FILE" <<'PY'
import json, sys

try:
    doc = json.load(open(sys.argv[1]))
except ValueError as exc:
    print("the answer is not JSON: %s" % exc)
    sys.exit(1)
if not isinstance(doc, dict):
    print("the answer is not an object")
    sys.exit(1)
if doc.get("ok") is not True:
    print("ok=%s errors=%s" % (json.dumps(doc.get("ok")), json.dumps(doc.get("errors"))))
    sys.exit(1)
if doc.get("action") != "create":
    print("action=%s" % json.dumps(doc.get("action")))
    sys.exit(1)
if doc.get("errors") != []:
    print("errors=%s" % json.dumps(doc.get("errors")))
    sys.exit(1)
print("ok=true action=create")
PY
)"; then
      ok accepted "$detail"
    else
      bad accepted "200, $detail (want ok=true action=create errors=[])"
    fi
  else
    bad accepted "POST /api/pipelines/validate → $(describe "$status") (want 200 ok=true action=create)"
  fi

  # ---- refused/cron: a schedule faster than one run per five minutes -----------------------

  CURRENT_ROW=refused/cron
  printf '{"name":"%s","ownerGroup":"%s","sql":"SELECT 1 AS one","schedule":{"cron":"* * * * *","timezone":"UTC"}}\n' \
    "$NAME" "$OWNER_GROUP" >"$CRON_FILE"
  status="$(request POST /api/pipelines/validate "$BODY_FILE" "$CRON_FILE")"
  if [ "$status" = "200" ]; then
    if detail="$(python3 - "$BODY_FILE" <<'PY'
import json, sys

try:
    doc = json.load(open(sys.argv[1]))
except ValueError as exc:
    print("the answer is not JSON: %s" % exc)
    sys.exit(1)
if not isinstance(doc, dict):
    print("the answer is not an object")
    sys.exit(1)
if doc.get("ok") is not False:
    print("ok=%s" % json.dumps(doc.get("ok")))
    sys.exit(1)
errors = doc.get("errors")
if not isinstance(errors, list) or len(errors) != 1 or not isinstance(errors[0], dict):
    print("errors=%s (want one error)" % json.dumps(errors))
    sys.exit(1)
message = errors[0].get("message")
if message != "at most one run every 5 minutes":
    print("message=%s" % json.dumps(message))
    sys.exit(1)
print(message)
PY
)"; then
      ok refused/cron "$detail"
    else
      bad refused/cron "200, $detail (want ok=false, one error: at most one run every 5 minutes)"
    fi
  else
    bad refused/cron "POST /api/pipelines/validate → $(describe "$status") (want 200 ok=false)"
  fi

  # ---- refused/cycle: two tasks that depend on each other ----------------------------------

  CURRENT_ROW=refused/cycle
  printf '{"name":"%s","ownerGroup":"%s","tasks":[{"name":"a","sql":"SELECT * FROM b","dependsOn":["b"]},{"name":"b","sql":"SELECT * FROM a","dependsOn":["a"]}]}\n' \
    "$NAME" "$OWNER_GROUP" >"$CYCLE_FILE"
  status="$(request POST /api/pipelines/validate "$BODY_FILE" "$CYCLE_FILE")"
  if [ "$status" = "200" ]; then
    if detail="$(python3 - "$BODY_FILE" <<'PY'
import json, sys

try:
    doc = json.load(open(sys.argv[1]))
except ValueError as exc:
    print("the answer is not JSON: %s" % exc)
    sys.exit(1)
if not isinstance(doc, dict):
    print("the answer is not an object")
    sys.exit(1)
if doc.get("ok") is not False:
    print("ok=%s" % json.dumps(doc.get("ok")))
    sys.exit(1)
errors = doc.get("errors")
if not isinstance(errors, list) or not errors:
    print("no error in the answer")
    sys.exit(1)
messages = [e.get("message") for e in errors if isinstance(e, dict) and isinstance(e.get("message"), str)]
# The live text wraps the task name in backticks; compare on the sentence without them.
wanted = "task a is part of a dependency cycle"
for message in messages:
    if wanted in message.replace("`", ""):
        print(message)
        break
else:
    print("errors=%s" % json.dumps(errors))
    sys.exit(1)
PY
)"; then
      ok refused/cycle "$detail"
    else
      bad refused/cycle "200, $detail (want ok=false, an error containing: task a is part of a dependency cycle)"
    fi
  else
    bad refused/cycle "POST /api/pipelines/validate → $(describe "$status") (want 200 ok=false)"
  fi

  # ---- nothing-written: the three bodies above created nothing -----------------------------

  CURRENT_ROW=nothing-written
  status="$(request GET "/api/pipelines/$PIPELINES_NS/$NAME" "$BODY_FILE")"
  if [ "$status" = "404" ]; then
    ok nothing-written "GET /api/pipelines/$PIPELINES_NS/$NAME → 404"
  else
    bad nothing-written "GET /api/pipelines/$PIPELINES_NS/$NAME → $(describe "$status") (want 404: a validate wrote a pipeline)"
  fi

fi

# ---- verdict -------------------------------------------------------------------------------

if [ "$PASSED" -eq 5 ] && [ "$FAILED" -eq 0 ]; then
  echo "verify-pipeline-validate: PASS (5 of 5)"
  exit 0
fi
echo "verify-pipeline-validate: FAIL (${FIRST_FAILED:-setup}, $PASSED of 5)"
exit 1
