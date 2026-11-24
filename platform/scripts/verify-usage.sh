#!/usr/bin/env bash
# The usage answer a 0.0.6 install must give (stardelt/stardelt#1508, #1488/#1489).
#
#   make verify-usage             (STARDELT_NOVA_URL=…, ISSUER=… to override)
#   bash scripts/verify-usage.sh
#
# The 0.0.6 exit says an install "shows administrators usage and cost per group". The usage
# route is the answer to that sentence, and this walks it as the visitor `demo`, whose token
# carries `groups=['visitors']`:
#
#   anonymous   GET /api/query-log/usage          401: without a token there is no usage
#   scope=all   GET …?scope=all                   403: the all scope is the administrator's
#   by=group    GET …?by=group                    200: the group row, the total, eight fields
#   showback    price and every estimatedCost     off (both null) or a rate (both a number)
#   total       the items add up to the total     a person in two groups is in each row
#
# A missing group join drops `visitors`; a broken total falls below the items; an inverted
# showback pairs `null` with a number. Each is a row here.
#
# The password is `demo` and it is PUBLIC ON PURPOSE: scripts/public-demo.sh creates that
# person and prints it, so it protects nothing. The password and the token live in `mktemp`
# files (0600, removed on every exit) and nothing here prints the token. Every request is a
# read; nothing is created.
#
# PASS (exit 0) = every row held. FAIL (exit 1) = the first row that did not, named in the
# last line — a check that passes before the usage route answers proves nothing.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"
NOVA="${NOVA%/}"
ISSUER="${ISSUER:-https://id.alpha.dev.stardelt.io/realms/stardelt}"
ISSUER="${ISSUER%/}"
PERSON=demo
TOTAL_ROWS=5

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

passed=0
row() { printf '✓ %s\n' "$1"; passed=$((passed + 1)); }

fail() { # fail <row> <what differed>
  printf '✗ %s: %s\n' "$1" "$2"
  echo "verify-usage: FAIL ($CURRENT_ROW)"
  exit 1
}

for tool in curl python3; do
  command -v "$tool" >/dev/null 2>&1 || fail startup "$tool not found"
done

# Every API call writes its body to BODY_FILE and its status to CODE: a check parses the body
# right away, so no answer outlives the next call. A curl that does not answer is a row
# failure (CODE=000), never a silent empty body.
CODE=000
api() { # api <path>
  CODE="$(curl -sS -o "$BODY_FILE" -w '%{http_code}' \
    -H "Authorization: Bearer $(cat "$TOKEN_FILE")" "$NOVA$1")" || CODE=000
}

row_sign_in() {
  local out
  if ! out="$(ISSUER="$ISSUER" USER="$PERSON" PASSWORD_FILE="$PASSWORD_FILE" TOKEN_FILE="$TOKEN_FILE" \
        python3 scripts/mint-user-token.py 2>&1)"; then
    fail sign-in "cannot mint a token for $PERSON: $(printf '%s\n' "$out" | head -n 1)"
  fi
}

row_anonymous() {
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' "$NOVA/api/query-log/usage")" || code=000
  [ "$code" = 401 ] || fail anonymous "answered $code, not 401"
  row "anonymous: 401"
}

row_scope_all() {
  local code answer
  code="$(curl -sS -o "$BODY_FILE" -w '%{http_code}' \
    -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
    "$NOVA/api/query-log/usage?scope=all")" || code=000
  [ "$code" = 403 ] || fail "scope=all as $PERSON" "answered $code, not 403"
  answer="$(python3 - "$BODY_FILE" <<'PY' || true
import json, sys

try:
    body = json.load(open(sys.argv[1]))
except ValueError as exc:
    print("DIFF the 403 body is not JSON: %s" % exc)
    raise SystemExit
error = body.get("error") if isinstance(body, dict) else None
if not isinstance(error, str) or "all scope is for administrators" not in error:
    print("DIFF the 403 names no administrator scope (%r)" % (error,))
else:
    print("OK %s" % error)
PY
)"
  case "$answer" in
    "OK "*) row "scope=all as $PERSON: 403 (${answer#OK })" ;;
    *) fail "scope=all as $PERSON" "${answer#DIFF }" ;;
  esac
}

# The answer `demo`'s token must give: the `visitors` group it carries, and the totals row.
row_by_group() {
  local answer rows
  api "/api/query-log/usage?by=group"
  [ "$CODE" = 200 ] || fail by=group "answered $CODE, not 200"
  answer="$(python3 - "$BODY_FILE" <<'PY' || true
import json, sys

FIELDS = ("key", "queries", "failed", "cpuMs", "wallMs", "inputBytes", "writtenBytes",
          "estimatedCost")

try:
    page = json.load(open(sys.argv[1]))
except ValueError as exc:
    print("DIFF the usage answer is not JSON: %s" % exc)
    raise SystemExit
if not isinstance(page, dict):
    print("DIFF the usage answer is not an object")
    raise SystemExit
if page.get("by") != "group":
    print("DIFF by is %r" % (page.get("by"),))
    raise SystemExit
total = page.get("total")
if not isinstance(total, dict) or total.get("key") != "total":
    print("DIFF the total row's key is %r, not 'total'"
          % (total.get("key") if isinstance(total, dict) else total,))
    raise SystemExit
items = page.get("items")
if not isinstance(items, list) or not items:
    print("DIFF items is %s" % ("not a list" if not isinstance(items, list) else "empty"))
    raise SystemExit
for item in items:
    missing = [field for field in FIELDS if field not in item]
    if missing:
        print("DIFF an item is missing %s" % ", ".join(missing))
        raise SystemExit
# The token's own groups claim, measured 2026-11-24: `demo` is in `visitors`.
if not any(item.get("key") == "visitors" for item in items):
    print("DIFF no item is the visitor's group 'visitors' (keys: %s)"
          % ", ".join(repr(item.get("key")) for item in items))
    raise SystemExit
print("OK %d" % len(items))
PY
)"
  case "$answer" in
    "OK "*) ;;
    *) fail by=group "${answer#DIFF }" ;;
  esac
  rows="${answer#OK }"
  if [ "$rows" = 1 ]; then rows="one row"; else rows="$rows rows"; fi
  row "by=group: $rows, key visitors, total key total, 8 fields"
}

# Both states of showback: no price means no estimate anywhere, and a price means a number
# everywhere. `null` beside a number is the inverted shape a reader would trust wrongly.
row_showback() {
  local answer
  api "/api/query-log/usage?by=group"
  [ "$CODE" = 200 ] || fail showback "answered $CODE, not 200"
  answer="$(python3 - "$BODY_FILE" <<'PY' || true
import json, sys

try:
    page = json.load(open(sys.argv[1]))
except ValueError as exc:
    print("DIFF the usage answer is not JSON: %s" % exc)
    raise SystemExit
items = page.get("items")
total = page.get("total")
if not isinstance(items, list) or not isinstance(total, dict):
    print("DIFF the usage answer carries no items and total")
    raise SystemExit
estimates = [item.get("estimatedCost") for item in items] + [total.get("estimatedCost")]
price = page.get("price")


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


if price is None:
    numbered = [value for value in estimates if value is not None]
    if numbered:
        print("DIFF price is null but an estimatedCost is %r" % (numbered[0],))
    else:
        print("OK price null and every estimatedCost null")
elif isinstance(price, dict):
    if not number(price.get("cpuHourPrice")):
        print("DIFF price names no cpuHourPrice (%r)" % (price.get("cpuHourPrice"),))
    elif not isinstance(price.get("currency"), str) or not price.get("currency"):
        print("DIFF price names no currency (%r)" % (price.get("currency"),))
    elif not all(number(value) for value in estimates):
        print("DIFF price is set but an estimatedCost is not a number (%s)"
              % ", ".join(repr(value) for value in estimates))
    else:
        print("OK price %s %s per CPU-hour and every estimatedCost a number"
              % (price.get("cpuHourPrice"), price.get("currency")))
else:
    print("DIFF price is %r, neither null nor a rate object" % (price,))
PY
)"
  case "$answer" in
    "OK "*) row "showback: ${answer#OK }" ;;
    *) fail showback "${answer#DIFF }" ;;
  esac
}

# A person in two groups appears in each group row, so the items can exceed the total and can
# never fall below it: a lower sum means the group join lost a query.
row_total() {
  local answer
  api "/api/query-log/usage?by=group"
  [ "$CODE" = 200 ] || fail total "answered $CODE, not 200"
  answer="$(python3 - "$BODY_FILE" <<'PY' || true
import json, sys

try:
    page = json.load(open(sys.argv[1]))
except ValueError as exc:
    print("DIFF the usage answer is not JSON: %s" % exc)
    raise SystemExit
items = page.get("items")
total = page.get("total")
if not isinstance(items, list) or not isinstance(total, dict):
    print("DIFF the usage answer carries no items and total")
    raise SystemExit
counts = [item.get("queries") for item in items]
if not all(isinstance(count, int) and not isinstance(count, bool) for count in counts):
    print("DIFF an item's queries is not an integer (%s)" % ", ".join(repr(c) for c in counts))
    raise SystemExit
count = total.get("queries")
if not isinstance(count, int) or isinstance(count, bool):
    print("DIFF total.queries is %r, not an integer" % (count,))
    raise SystemExit
if sum(counts) < count:
    print("DIFF the items add up to %d, below the total %d" % (sum(counts), count))
else:
    print("OK %d = %d" % (sum(counts), count))
PY
)"
  case "$answer" in
    "OK "*) row "total: items add up to at least the total (${answer#OK })" ;;
    *) fail total "${answer#DIFF }" ;;
  esac
}

CURRENT_ROW=sign-in
row_sign_in
CURRENT_ROW=anonymous
row_anonymous
CURRENT_ROW=scope=all
row_scope_all
CURRENT_ROW=by=group
row_by_group
CURRENT_ROW=showback
row_showback
CURRENT_ROW=total
row_total

echo "verify-usage: PASS ($passed of $TOTAL_ROWS)"
