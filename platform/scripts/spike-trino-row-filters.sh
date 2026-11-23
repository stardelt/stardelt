#!/usr/bin/env bash
# Spike for stardelt/stardelt#1485 (design #50): can Trino's OPA plugin apply **row filters** and
# **column masks**, and what exactly does it send and expect through our bridge?
#
#   cd platform && make spike-trino-row-filters
#
# The plugin is the only place in #50's plan where one rule can reach Nova's SQL editor, a
# notebook's `stardelt.sql()` and Superset's impersonated queries at once. Nothing here ships:
# the two values files are patched in place for the measurement and restored in the trap, and
# both releases are redeployed so the cluster ends where it started.
#
# Seven lines, printed from the running objects (the `…` values are what the spike measures):
#   F1 whether Trino calls the two URIs, with which operation and which body, and how often
#      (per query, not per session)
#   F2 what the empty answer does: `[]` is "no filter"; for a column mask `[]` is rejected and
#      the entry point must stay undefined (the same 200 rows as before the spike)
#   F3 a row filter keyed on one user: 33 rows instead of 200 as that user
#   F4 a column mask on `name`: the value is the replacement expression as that user
#   F5 the other principal: `superset` is denied the demo warehouse before a filter is ever
#      considered, and the bridge answers `[]` for it — recorded as measured
#   F6 a malformed filter expression: the engine's sentence, which #50's editor has to show
#   F7 which of #50's paths the rule reaches: Nova's statement API and Superset's impersonation
#   CLEANUP the two URIs are gone from the live config, 200 rows as `demo`, `verify-trino-auth`
#      PASS — the pre-spike behaviour is back
#
# The rule is keyed on the string Trino puts in `input.context.identity.user` (the Keycloak
# `sub`, measured with `SELECT current_user`), never on a server name: the same rule then
# applies to the impersonated Superset path (#310), because impersonation changes that identity.
#
# The only cluster change is the two properties, the two Rego entry points and a DEBUG logger;
# the trap restores all of it. No token, password or Secret value is printed: only ids, counts
# and the engine's sentences.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NAMESPACE:-stardelt}"
TRINO_URL="${TRINO_URL:-https://trino.alpha.dev.stardelt.io}"
BASE_DOMAIN="${BASE_DOMAIN:-alpha.dev.stardelt.io}"
NOVA_URL="${NOVA_URL:-https://$BASE_DOMAIN}"
SPIKE_USER="${SPIKE_USER:-demo}"
SPIKE_PASSWORD="${SPIKE_PASSWORD:-demo}"
TABLE=bronze.shop.customers

TRINO_VALUES=helm-values/trino-identity.yaml
LAKEKEEPER_VALUES=helm-values/lakekeeper.yaml

fail() { echo "spike-trino-row-filters: $*" >&2; exit 1; }

for tool in kubectl helm python3 curl git; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool not found"
done

# The install rules (AGENTS.md): never upgrade two releases from a checkout that is behind
# origin/main — its pins are old and the upgrade rewrites the live Deployments to them.
git rev-parse --git-dir >/dev/null 2>&1 || fail "not inside a git checkout"
git fetch -q origin || fail "cannot reach origin"
behind="$(git rev-list --count HEAD..origin/main)"
[ "$behind" = 0 ] || fail "this checkout is $behind commit(s) behind origin/main — pull first"

# The values files are patched by this script; they must not carry anyone's work when it starts.
git diff --quiet -- "$TRINO_VALUES" "$LAKEKEEPER_VALUES" \
  || fail "$TRINO_VALUES or $LAKEKEEPER_VALUES has local changes — commit or stash them first"

umask 077
tmp="$(mktemp -d)"
deployed=0
f6=""

# ---------------------------------------------------------------------------
# Revert. Every exit path: the files go back to HEAD, both releases are redeployed, and the
# CLEANUP line proves the pre-spike behaviour is back. A spike that leaves the cluster
# modified has not answered anything usable.
# ---------------------------------------------------------------------------
# shellcheck disable=SC2329  # called by the EXIT trap below
cleanup() {
  rc=$?
  trap - EXIT
  set +e
  echo "› revert: restore $TRINO_VALUES and $LAKEKEEPER_VALUES, redeploy both releases"
  git checkout -- "$TRINO_VALUES" "$LAKEKEEPER_VALUES"
  if [ "$deployed" = 1 ]; then
    make step-lakekeeper >"$tmp/revert-lakekeeper.log" 2>&1 \
      || { echo "  step-lakekeeper failed:"; tail -n 15 "$tmp/revert-lakekeeper.log"; }
    make step-trino >"$tmp/revert-trino.log" 2>&1 \
      || { echo "  step-trino failed:"; tail -n 15 "$tmp/revert-trino.log"; }
  fi

  local rf count verify
  rf="$(kubectl -n "$NAMESPACE" get configmap trino-coordinator \
        -o jsonpath='{.data.access-control-opa\.properties}' 2>/dev/null | grep -c 'row-filters' || true)"
  mint_trino_token
  count="$(count_as "$tmp/demo-token")"
  if make verify-trino-auth TRINO_VERIFY_USER="$SPIKE_USER" TRINO_VERIFY_USER_PASSWORD="$SPIKE_PASSWORD" \
      >"$tmp/verify-trino-auth.log" 2>&1; then
    verify=PASS
  else
    verify=FAIL
    tail -n 8 "$tmp/verify-trino-auth.log"
  fi

  [ -z "$f6" ] || echo "$f6"
  echo "CLEANUP row_filters_called=$([ "${rf:-0}" -eq 0 ] && echo no || echo yes) count=${count:-unknown} verify_trino_auth=$verify"
  echo "spike-trino-row-filters: DONE"
  rm -rf "$tmp"
  exit "$rc"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Clients. Each prints exactly one line: `rows=<json>` or `error=<the engine's sentence>`.
# ---------------------------------------------------------------------------
trino_sql() { # trino_sql <token-file> <sql>
  python3 - "$1" "$2" "$TRINO_URL" <<'PY'
import json, sys, urllib.error, urllib.request

token_file, sql, base = sys.argv[1], sys.argv[2], sys.argv[3].rstrip("/")
token = open(token_file, encoding="utf-8").read().strip()


def post(url, data=None):
    request = urllib.request.Request(
        url, data=data,
        headers={"Authorization": "Bearer " + token, "X-Trino-Source": "spike-trino-row-filters"})
    with urllib.request.urlopen(request, timeout=180) as response:
        return json.load(response)


try:
    body = post(base + "/v1/statement", sql.encode())
    rows = list(body.get("data") or [])
    while body.get("nextUri"):
        if body.get("error"):
            break
        body = post(body["nextUri"])
        rows.extend(body.get("data") or [])
    if body.get("error"):
        print("error=" + str(body["error"].get("message") or body["error"]).replace("\n", " "))
    else:
        print("rows=" + json.dumps(rows))
except urllib.error.HTTPError as e:
    print("error=HTTP %s %s" % (e.code, e.read().decode("utf-8", "replace")[:300].replace("\n", " ")))
PY
}

superset_sql() { # superset_sql <impersonated-user|""> <sql> — through Superset's own engine
  local out line
  out="$(kubectl -n "$NAMESPACE" exec -i deploy/superset -- env \
    SUPERSET_SQL="$2" SUPERSET_IMPERSONATE="$1" python - <<'PY' 2>&1
import json, os

from types import SimpleNamespace

from superset.app import create_app

app = create_app()
sql = os.environ["SUPERSET_SQL"]
who = os.environ.get("SUPERSET_IMPERSONATE") or ""


def run():
    from sqlalchemy import text

    from superset import db
    from superset.models.core import Database

    database = db.session.query(Database).filter_by(database_name="Trino Warehouse").one()
    with database.get_sqla_engine() as engine:
        with engine.connect() as connection:
            return [list(row) for row in connection.execute(text(sql))]


try:
    if who:
        # Superset's impersonation reads the signed-in user from `g.user` (utils.get_username);
        # setting it here drives the same `get_sqla_engine()` a SQL Lab query would.
        with app.test_request_context("/"):
            from flask import g

            g.user = SimpleNamespace(username=who, id=0)
            print("rows=" + json.dumps(run()))
    else:
        print("rows=" + json.dumps(run()))
except Exception as e:  # noqa: BLE001 — the sentence is the measurement
    print("error=" + type(e).__name__ + ": " + str(e)[:300].replace("\n", " "))
PY
)"
  line="$(printf '%s\n' "$out" | grep -E '^(rows|error)=' | tail -n 1 || true)"
  if [ -n "$line" ]; then
    printf '%s\n' "$line"
  else
    printf 'error=the Superset probe printed no result: %s\n' "$(printf '%s' "$out" | tail -n 1 | cut -c1-200)"
  fi
}

nova_sql() { # nova_sql <token-file> <sql> — Nova's statement API, impersonating the caller
  python3 - "$1" "$2" "$NOVA_URL" <<'PY'
import json, sys, time, urllib.error, urllib.request

token_file, sql, base = sys.argv[1], sys.argv[2], sys.argv[3].rstrip("/")
token = open(token_file, encoding="utf-8").read().strip()


def call(method, url, body=None):
    request = urllib.request.Request(
        url, data=body, method=method,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


try:
    state = call("POST", base + "/api/statements", json.dumps({"sql": sql, "waitSeconds": 25}).encode())
    deadline = time.time() + 90
    while state.get("state") not in ("SUCCEEDED", "FAILED", "CANCELED") and time.time() < deadline:
        state = call("GET", "%s/api/statements/%s?waitSeconds=20" % (base, state.get("statementId")))
    if state.get("state") == "SUCCEEDED":
        print("rows=" + json.dumps(state.get("rows") or []))
    else:
        print("error=%s: %s" % (state.get("state"), str(state.get("error") or "")[:300].replace("\n", " ")))
except urllib.error.HTTPError as e:
    print("error=HTTP %s %s" % (e.code, e.read().decode("utf-8", "replace")[:300].replace("\n", " ")))
PY
}

rows_of() { sed -n 's/^rows=//p'; }

# A Keycloak access token for the measured person, through the device grant on client `trino`
# (the same flow verify-trino-auth.sh's check 3 uses). It lives minutes, so it is re-minted
# before every group of measurements. Nothing prints it.
mint_trino_token() {
  kubectl -n "$NAMESPACE" get secret stardelt-oidc-clients -o jsonpath='{.data.trino}' | base64 -d >"$tmp/trino-client"
  ISSUER="$(kubectl -n "$NAMESPACE" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}')" \
  TRINO_URL="$TRINO_URL" USER="$SPIKE_USER" PASSWORD="$SPIKE_PASSWORD" \
  CLIENT_SECRET_FILE="$tmp/trino-client" MINT_ONLY=1 TOKEN_FILE="$tmp/demo-token" \
    python3 scripts/verify-trino-auth.py >"$tmp/mint-trino.log" 2>&1 \
    || { cat "$tmp/mint-trino.log"; fail "could not mint a Trino token for $SPIKE_USER"; }
}

# The same person's token with the `nova` audience — Nova accepts only its own client id, and
# its login is the browser code flow (the realm requires PKCE S256), so this drives the
# authorize form the way the browser does and reads the code off the redirect.
mint_nova_token() {
  kubectl -n "$NAMESPACE" get secret stardelt-oidc-clients -o jsonpath='{.data.nova}' | base64 -d >"$tmp/nova-client"
  printf '%s' "$SPIKE_PASSWORD" >"$tmp/demo-pw"
  ISSUER="$(kubectl -n "$NAMESPACE" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}')" \
  CLIENT_SECRET_FILE="$tmp/nova-client" PASSWORD_FILE="$tmp/demo-pw" USER="$SPIKE_USER" \
  REDIRECT_URI="$NOVA_URL/api/auth/callback" TOKEN_FILE="$tmp/nova-token" \
    python3 "$tmp/mint-nova-token.py" >"$tmp/mint-nova.log" 2>&1 \
    || { cat "$tmp/mint-nova.log"; fail "could not mint a Nova token for $SPIKE_USER"; }
}

count_as() { # count_as <token-file> -> the row count of $TABLE as that token's person
  local line
  line="$(trino_sql "$1" "SELECT count(*) FROM $TABLE")"
  case "$line" in
    rows=*) python3 -c 'import json,sys; rows=json.loads(sys.argv[1]); print(rows[0][0] if rows else "no-rows")' "${line#rows=}" ;;
    *) printf '%s' "${line#error=}" ;;
  esac
}

# ---------------------------------------------------------------------------
# The temporary configuration. Both functions edit the values files in place; the trap
# restores them from git.
# ---------------------------------------------------------------------------
patch_trino_values() {
  python3 - "$TRINO_VALUES" <<'PY'
import sys

path = sys.argv[1]
text = open(path, encoding="utf-8").read()
anchor = ("      opa.policy.batched-uri="
          "http://lakekeeper-opa.stardelt.svc.cluster.local:8282/v1/data/stardelt/batch\n")
assert text.count(anchor) == 1, "the OPA properties block moved — fix this spike before it misleads"
added = (
    "      opa.policy.row-filters-uri=http://lakekeeper-opa.stardelt.svc.cluster.local:8282/v1/data/stardelt/row_filters\n"
    "      opa.policy.column-masking-uri=http://lakekeeper-opa.stardelt.svc.cluster.local:8282/v1/data/stardelt/column_masks\n"
    "      # F1 needs the bodies Trino sends; the plugin writes them at DEBUG (OpaHttpClient).\n"
    "      opa.log-requests=true\n")
text = text.replace(anchor, anchor + added)
text += ("\n# Spike #1485 (temporary): the OPA client logs the request bodies F1 records.\n"
         "additionalLogProperties:\n"
         "  - io.trino.plugin.opa.OpaHttpClient=DEBUG\n")
open(path, "w", encoding="utf-8").write(text)
PY
}

append_rego() { # append_rego <file holding the rego lines; six spaces of indent are added>
  python3 - "$LAKEKEEPER_VALUES" "$1" <<'PY'
import sys

values, block_path = sys.argv[1], sys.argv[2]
lines = open(values, encoding="utf-8").read().split("\n")
start = next(i for i, line in enumerate(lines) if line.strip().startswith("stardelt.front.rego:"))
end = start + 1
while end < len(lines) and (not lines[end].strip() or lines[end].startswith("      ")):
    end += 1
block = ["      " + line if line else "" for line in
         open(block_path, encoding="utf-8").read().rstrip("\n").split("\n")]
lines[end:end] = ["      # --- spike #1485 (temporary; the trap restores this file) ---"] + block
open(values, "w", encoding="utf-8").write("\n".join(lines))
PY
}

# Puts a Rego block (with @@USER@@) at the end of `stardelt.front.rego` and redeploys the bridge
# only (the Trino properties do not change after the first stage).
set_rego() { # set_rego <block-text>
  git checkout -- "$LAKEKEEPER_VALUES"
  printf '%s\n' "$1" | sed "s/@@USER@@/$DEMO_USER/g" >"$tmp/rego-block"
  append_rego "$tmp/rego-block"
  echo "› redeploy the OPA bridge: $(printf '%s\n' "$1" | head -n 1 | cut -c1-90)"
  deployed=1
  make step-lakekeeper >"$tmp/step-lakekeeper.log" 2>&1 \
    || { tail -n 20 "$tmp/step-lakekeeper.log"; fail "step-lakekeeper failed"; }
}

deploy_trino() {
  echo "› deploy Trino with both URIs"
  deployed=1
  make step-trino >"$tmp/step-trino.log" 2>&1 \
    || { tail -n 20 "$tmp/step-trino.log"; fail "step-trino failed"; }
  kubectl -n "$NAMESPACE" rollout status deployment/trino-coordinator --timeout=10m >/dev/null \
    || fail "the Trino coordinator did not roll out"
}

# The coordinator's DEBUG lines for the OPA plugin, ANSI colours stripped.
opa_requests() { # opa_requests <since, e.g. 180s> | opa_requests <RFC3339 mark>
  if [[ "${1:-}" == *T* ]]; then
    kubectl -n "$NAMESPACE" logs deployment/trino-coordinator --since-time="$1" 2>&1 \
      | sed -e 's/\x1b\[[0-9;]*m//g'
  else
    kubectl -n "$NAMESPACE" logs deployment/trino-coordinator --since="${1:-180s}" 2>&1 \
      | sed -e 's/\x1b\[[0-9;]*m//g'
  fi
}

# The bridge's own answer for one user string, asked the way Trino asks it (F5, F7).
bridge_answer() { # bridge_answer <endpoint> <user>
  kubectl -n "$NAMESPACE" exec deploy/trino-coordinator -c trino-coordinator -- \
    curl -sS -X POST -d "{\"input\":{\"context\":{\"identity\":{\"user\":\"$2\"}}}}" \
      "http://lakekeeper-opa.stardelt.svc.cluster.local:8282/v1/data/stardelt/$1"
}

# ---------------------------------------------------------------------------
# 0. Pre-flight: the person, the table and the untouched answer.
# ---------------------------------------------------------------------------
mint_trino_token
baseline="$(count_as "$tmp/demo-token")"
[ "$baseline" = 200 ] || fail "pre-spike $TABLE holds $baseline rows for $SPIKE_USER, expected 200"
DEMO_USER="$(trino_sql "$tmp/demo-token" "SELECT current_user" | rows_of \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)[0][0])')"
[ -n "$DEMO_USER" ] || fail "could not read current_user"
echo "› $SPIKE_USER is '$DEMO_USER' to Trino; $TABLE has 200 rows before the spike"

cat >"$tmp/mint-nova-token.py" <<'PY'
import base64, hashlib, http.cookiejar, json, os, re, sys, time, urllib.error, urllib.parse, urllib.request

ISSUER = os.environ["ISSUER"].rstrip("/")
SECRET = open(os.environ["CLIENT_SECRET_FILE"], encoding="utf-8").read().strip()
PASSWORD = open(os.environ["PASSWORD_FILE"], encoding="utf-8").read().strip()
USER = os.environ["USER"]
REDIRECT = os.environ["REDIRECT_URI"]
OUT = os.environ["TOKEN_FILE"]

verifier = base64.urlsafe_b64encode(os.urandom(48)).decode().rstrip("=")
challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).decode().rstrip("=")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


jar = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar), NoRedirect())
FORM = re.compile(r"<form[^>]*>.*?</form>", re.S)
INPUT = re.compile(r'name="([^"]+)" value="([^"]*)"')
ACTION = re.compile(r'action="([^"]+)"')


def fetch(url, data=None, tries=6):
    for attempt in range(tries):
        try:
            try:
                with opener.open(urllib.request.Request(url, data=data), timeout=30) as response:
                    return response.geturl(), response.headers.get("Location"), response.read().decode()
            except urllib.error.HTTPError as error:
                return error.geturl(), error.headers.get("Location"), error.read().decode()
        except urllib.error.URLError:
            if attempt + 1 == tries:
                raise
            time.sleep(3)


query = urllib.parse.urlencode({
    "response_type": "code", "client_id": "nova", "redirect_uri": REDIRECT, "scope": "openid",
    "state": "spike-1485", "code_challenge": challenge, "code_challenge_method": "S256"})
url, _, html = fetch(ISSUER + "/protocol/openid-connect/auth?" + query)
form = FORM.search(html)
if not form:
    sys.exit("no login form on the authorize page")
action = ACTION.search(form.group(0)).group(1).replace("&amp;", "&")
origin = "https://" + urllib.parse.urlsplit(url).netloc
action_url = origin + action if action.startswith("/") else action
fields = dict(INPUT.findall(form.group(0)))
body = urllib.parse.urlencode({
    **{k: v for k, v in fields.items() if k in ("execution", "tab_id", "session_code", "client_id")},
    "username": USER, "password": PASSWORD}).encode()
_, location, html = fetch(action_url, data=body)
if not location or "code=" not in location:
    sys.exit("login did not produce a code: " + str(location or html[:200]))
code = urllib.parse.parse_qs(urllib.parse.urlsplit(location).query)["code"][0]
body = urllib.parse.urlencode({
    "grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT,
    "client_id": "nova", "client_secret": SECRET, "code_verifier": verifier}).encode()
with urllib.request.urlopen(
        urllib.request.Request(ISSUER + "/protocol/openid-connect/token", data=body), timeout=30) as response:
    token = json.load(response)["access_token"]
with open(OUT, "w", encoding="utf-8") as handle:
    handle.write(token)
PY

# ---------------------------------------------------------------------------
# 1. Both URIs, both entry points empty — the answer the ticket names first.
#    `column_masks := []` is measured too, because the documented shapes differ.
# ---------------------------------------------------------------------------
patch_trino_values
set_rego "$(cat <<'REGO'
row_filters := []

column_masks := []
REGO
)"
deploy_trino

mint_trino_token
empty_arrays="$(trino_sql "$tmp/demo-token" "SELECT count(*) FROM $TABLE")"
case "$empty_arrays" in
  rows=*) empty_arrays_fact="accepted" ;;
  *) empty_arrays_fact="rejected:$(printf '%s' "${empty_arrays#error=}" | cut -c1-140)" ;;
esac

set_rego "$(cat <<'REGO'
row_filters := []

# `column_masks` is deliberately undefined: `[]` is not an accepted answer for a mask
# (see F2), an absent entry point is.
REGO
)"
mint_trino_token
f2_count="$(count_as "$tmp/demo-token")"
[ "$f2_count" = 200 ] || fail "the empty answer changed the result ($f2_count rows, expected 200)"

# ---------------------------------------------------------------------------
# 2. F1: what Trino sends. Two queries, so "per query" is measured, not assumed.
# ---------------------------------------------------------------------------
echo "› capture the OPA requests of two queries"
mark="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
sleep 2
first="$(trino_sql "$tmp/demo-token" "SELECT * FROM $TABLE LIMIT 3")"
sleep 1
second="$(trino_sql "$tmp/demo-token" "SELECT name, country FROM $TABLE LIMIT 2")"
case "$first$second" in
  *error=*) fail "a capture query failed: ${first#error=} ${second#error=}" ;;
esac
sleep 3
logs="$(opa_requests "$mark")"
# Only this person's requests: the cluster is shared and another query would inflate the count.
mine="$(printf '%s\n' "$logs" | grep -F "\"user\":\"$DEMO_USER\"")"
row_calls="$(printf '%s\n' "$mine" | grep -c 'operation":"GetRowFilters"' || true)"
mask_calls="$(printf '%s\n' "$mine" | grep -c 'operation":"GetColumnMask"' || true)"
row_path="$(printf '%s\n' "$mine" | grep 'operation":"GetRowFilters"' | tail -n 1 \
  | sed -n 's/.*Sending OPA request to URI "\([^"]*\)".*/\1/p')"
row_body="$(printf '%s\n' "$mine" | grep 'operation":"GetRowFilters"' | tail -n 1 \
  | sed -n 's/.*request body = \(.*\) ; request headers.*/\1/p')"
mask_body="$(printf '%s\n' "$mine" | grep 'operation":"GetColumnMask"' | tail -n 1 \
  | sed -n 's/.*request body = \(.*\) ; request headers.*/\1/p')"
if [ "$row_calls" -gt 0 ]; then called=yes; else called=no; fi
echo "F1 row_filters_called=$called operation=GetRowFilters calls_for_2_queries=$row_calls path=${row_path:-none} row_filter_body=${row_body:-none}"
echo "F1b column_masking_called=$([ "$mask_calls" -gt 0 ] && echo yes || echo no) operation=GetColumnMask calls_for_2_queries=$mask_calls column_mask_body=$(printf '%s' "${mask_body:-none}" | cut -c1-260)"
echo "F2 empty_rule_count=$f2_count empty_column_mask_array=$empty_arrays_fact"

# ---------------------------------------------------------------------------
# 3. F3: one row filter, keyed on the measured user string.
# ---------------------------------------------------------------------------
set_rego "$(cat <<'REGO'
row_filters := [{"expression": "country = 'de'"}] if {
  _user == "@@USER@@"
}

row_filters := [] if {
  _user != "@@USER@@"
}
REGO
)"
mint_trino_token
f3_line="$(trino_sql "$tmp/demo-token" "SELECT count(*) FROM $TABLE")"
f3="$(printf '%s' "$f3_line" | sed -n 's/^rows=//p' \
  | python3 -c 'import json,sys; rows=json.load(sys.stdin); print(rows[0][0] if rows else "no-rows")')"
f3_rows="$(trino_sql "$tmp/demo-token" "SELECT name, country FROM $TABLE ORDER BY id LIMIT 2" | sed -n 's/^rows=//p')"
echo "F3 filtered_count=$f3 first_rows=$f3_rows"

# ---------------------------------------------------------------------------
# 4. F4 and F5: a column mask on `name`; the service account is untouched.
# ---------------------------------------------------------------------------
set_rego "$(cat <<'REGO'
row_filters := [{"expression": "country = 'de'"}] if {
  _user == "@@USER@@"
}

row_filters := [] if {
  _user != "@@USER@@"
}

column_masks := {"expression": "'masked'"} if {
  _user == "@@USER@@"
  input.action.resource.column.columnName == "name"
}
REGO
)"
mint_trino_token
masked_line="$(trino_sql "$tmp/demo-token" "SELECT name, country FROM $TABLE ORDER BY id LIMIT 2")"
masked="$(printf '%s' "$masked_line" | sed -n 's/^rows=//p')"
f4="$(printf '%s' "$masked" \
  | python3 -c 'import json,sys
rows=json.load(sys.stdin)
print(rows[0][0] if rows else "no-rows")' 2>/dev/null || printf '%s' "${masked_line#error=}")"
echo "F4 masked_name=$f4 masked_rows=$masked"

superset_pw="$(kubectl -n "$NAMESPACE" get secret trino-password-db -o jsonpath='{.data.superset-password}' | base64 -d)"
finish_statement() { python3 - "$1" <<'PY'
import json, sys, urllib.request
body = json.loads(sys.argv[1])
rows = list(body.get("data") or [])
while body.get("nextUri") and not body.get("error"):
    with urllib.request.urlopen(urllib.request.Request(body["nextUri"]), timeout=60) as r:
        body = json.load(r)
    rows.extend(body.get("data") or [])
if body.get("error"):
    print("error:" + str(body["error"].get("message"))[:160])
elif rows:
    print(rows[0][0])
else:
    print("no-rows")
PY
}
as_superset() { # as_superset <sql> — the `superset` service account, no impersonation
  curl -sS -u "superset:$superset_pw" -X POST "$TRINO_URL/v1/statement" \
    -H 'X-Trino-Source: spike-trino-row-filters' --data "$1"
}
f5_id_line="$(as_superset "SELECT count(*) FROM $TABLE")"
f5="$(finish_statement "$f5_id_line")"
f5_names="$(finish_statement "$(as_superset "SELECT min(name) FROM $TABLE")")"
# The `superset` service account is not a member of the demo warehouse's group: Trino's own
# rules deny the catalog before a row filter is ever considered, so "the other user still sees
# 200" cannot be measured on this table. What can be measured is the policy's answer for the
# same user string, asked the way Trino asks it.
f5_policy="$(bridge_answer row_filters superset)"
echo "F5 other_user_count=$f5 other_user=superset first_name=$f5_names other_user_policy_answer=$f5_policy query_id=$(printf '%s' "$f5_id_line" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("id") or "none")' 2>/dev/null || echo none)"

# ---------------------------------------------------------------------------
# 5. F7: the two paths #50 cares about — measured while the rule is in place.
# ---------------------------------------------------------------------------
mint_trino_token
mint_nova_token
nova_line="$(nova_sql "$tmp/nova-token" "SELECT count(*) FROM $TABLE")"
nova_fact="$(printf '%s' "$nova_line" | sed -n 's/^rows=//p' \
  | python3 -c 'import json,sys; rows=json.load(sys.stdin); print(rows[0][0] if rows else "no-rows")' 2>/dev/null \
  || printf 'error:%s' "$(printf '%s' "$nova_line" | sed -n 's/^error=//p' | cut -c1-120)")"
impersonated_line="$(superset_sql "$DEMO_USER" "SELECT count(*) FROM $TABLE")"
superset_fact="$(printf '%s' "$impersonated_line" | sed -n 's/^rows=//p' \
  | python3 -c 'import json,sys; rows=json.load(sys.stdin); print(rows[0][0] if rows else "no-rows")' 2>/dev/null \
  || printf 'error:%s' "$(printf '%s' "$impersonated_line" | sed -n 's/^error=//p' | cut -c1-120)")"
echo "F7 nova_path=$nova_fact superset_impersonation_path=$superset_fact spark_and_iceberg_path=not-measured (they never pass Trino, #84)"

# ---------------------------------------------------------------------------
# 6. F6: the sentence a malformed filter produces. Printed by the trap, in the
#    contract's order, once the queries above have used the valid rule.
# ---------------------------------------------------------------------------
set_rego "$(cat <<'REGO'
row_filters := [{"expression": "country ="}] if {
  _user == "@@USER@@"
}

row_filters := [] if {
  _user != "@@USER@@"
}
REGO
)"
mint_trino_token
broken="$(trino_sql "$tmp/demo-token" "SELECT count(*) FROM $TABLE")"
case "$broken" in
  rows=*) f6="F6 malformed_error=none (the engine accepted a truncated expression: ${broken#rows=})" ;;
  *) f6="F6 malformed_error=$(printf '%s' "${broken#error=}" | cut -c1-400)" ;;
esac

exit 0
