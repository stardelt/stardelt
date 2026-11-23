#!/usr/bin/env bash
# Converge the live Keycloak realm onto the one object that lets Nova mint an engine token:
# `standard.token.exchange.enabled=true` on the confidential client `nova`
# (stardelt/stardelt#1453, measured by #1445/#1442).
#
#   make sync-mcp-exchange
#   REALM_URL=https://id.example.com/realms/stardelt NAMESPACE=stardelt bash scripts/keycloak-sync-mcp-exchange.sh
#
# The design (`design/agent-access.md`, "Downstream calls") says Nova exchanges a person's MCP
# token for a `trino`-audience token with RFC 8693 token exchange. Keycloak 26 keeps standard
# token exchange **per client**: the realm enables it on no client, so the exchange answers
# `Standard token exchange is not enabled for the requested client` and the engine token cannot
# be minted. The flag belongs on `nova` — the **requester** (the actor), not the subject client
# `stardelt-mcp` — and only a confidential client with a secret can be the actor; the public
# `stardelt-mcp` cannot.
#
# Why a script: `--import-realm` creates the realm on a fresh install only (the realm import is
# the fresh-install twin of this check). This one reads the live realm, sets the attribute when it
# drifted and proves the result end to end. A `PUT` of the client representation **without**
# `standard.token.exchange.enabled` leaves the stored value in place (measured, three times), so
# turning it off means writing `"false"`; this script only ever writes `"true"`.
#
# The rows, printed as ✓/✗ (the pattern of `scripts/verify-mcp.sh`):
#   attribute  the client attribute is `true` on `nova` (read back after the write)
#   exchange   POST the token endpoint as `nova` with the person's MCP token as subject → 200
#              with an `access_token`
#   claims     the exchanged token's `aud` contains `trino` and `exp` is in the future
#   trino      POST /v1/statement through a port-forward with the exchanged token and
#              `SELECT 1` → 200 with a JSON `id`
#
# Exit 0 = PASS (4 of 4), 1 = FAIL (one ✗ row and every row still ran), 3 = UNVERIFIED (a tool is
# missing: nothing was asked, nothing proved).
#
# It prints no token, client secret or password. The actor's credentials go through a mode-0600
# `curl -K` config file and the subject token through `--data-urlencode name@file`, so none of
# them reaches argv or stdout; every file lives in a mode-0700 temp dir that the trap removes,
# together with the two port-forwards.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NAMESPACE="${NAMESPACE:-stardelt}"
ACTOR="${ACTOR:-nova}"
MCP_CLIENT="${MCP_CLIENT:-stardelt-mcp}"
AUDIENCE="${AUDIENCE:-trino}"
MASTER_SECRET="${MASTER_SECRET:-keycloak-admin}"
REALM_URL="${REALM_URL:-}"
REALM="${REALM:-stardelt}"

CURL=(curl --fail-with-body --silent --show-error --max-time 30)

for tool in kubectl curl python3 base64 jq; do
  command -v "$tool" >/dev/null 2>&1 \
    || { echo "keycloak-sync-mcp-exchange: UNVERIFIED (missing tool: $tool)"; exit 3; }
done

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/mcp-exchange.XXXXXX")" || exit 3
chmod 700 "$tmp"
kc_pid=""
trino_pid=""
cleanup() {
  if [ -n "$kc_pid" ]; then kill "$kc_pid" >/dev/null 2>&1 || true; fi
  if [ -n "$trino_pid" ]; then kill "$trino_pid" >/dev/null 2>&1 || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

fail() { echo "keycloak-sync-mcp-exchange: $*" >&2; exit 1; }

PASSED=0
FAILED=0
ok()  { printf '✓ %-10s %s\n' "$1" "$2"; PASSED=$((PASSED + 1)); }
bad() { printf '✗ %-10s %s\n' "$1" "$2"; FAILED=$((FAILED + 1)); }

free_port() {
  local port="$1"
  while (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; do port=$((port + 1)); done
  printf '%s' "$port"
}

# 1. Reach the admin API on loopback, through the Service.
kc_port="$(free_port 18230)"
kubectl -n "$NAMESPACE" port-forward svc/keycloak "$kc_port:8080" >"$tmp/keycloak-pf.log" 2>&1 &
kc_pid=$!
ready=""
for _ in $(seq 1 30); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$kc_port") 2>/dev/null; then ready=1; break; fi
  sleep 0.5
done
[ -n "$ready" ] || fail "port-forward to svc/keycloak never came up — is Keycloak installed in namespace $NAMESPACE?"
KC="http://127.0.0.1:$kc_port"

# Keycloak in production mode answers on its public hostname (KC_HOSTNAME), so a request to
# 127.0.0.1 has to say which host it is meant for.
strip_scheme() { printf '%s' "$1" | sed -E 's|https?://||; s|/.*||'; }
KC_HOST="$(strip_scheme "$(kubectl -n "$NAMESPACE" get configmap keycloak-env -o jsonpath='{.data.KC_HOSTNAME}' 2>/dev/null || true)")"
[ -n "$KC_HOST" ] || fail "ConfigMap keycloak-env has no KC_HOSTNAME — run 'make install-identity'"
HEAD=(-H "X-Forwarded-Host: $KC_HOST" -H 'X-Forwarded-Proto: https')

# 2. Master-realm token. Retry: the Service accepts a connection while Keycloak is booting.
kubectl -n "$NAMESPACE" get secret "$MASTER_SECRET" -o jsonpath='{.data.password}' | base64 -d > "$tmp/master-pw" \
  || fail "cannot read Secret $MASTER_SECRET — run 'make install-identity'"
master_token=""
for _ in $(seq 1 20); do
  master_token="$(curl --silent --show-error --max-time 30 "${HEAD[@]}" \
    -X POST "$KC/realms/master/protocol/openid-connect/token" \
    --data-urlencode "username=admin" --data-urlencode "password@$tmp/master-pw" \
    --data-urlencode "client_id=admin-cli" --data-urlencode "grant_type=password" 2>/dev/null \
    | python3 -c 'import sys,json
try: print(json.load(sys.stdin).get("access_token") or "")
except Exception: print("")' 2>/dev/null || true)"
  [ -n "$master_token" ] && break
  sleep 2
done
[ -n "$master_token" ] || fail "the master realm refused the $MASTER_SECRET credential — is Keycloak reachable through svc/keycloak?"
AUTH=(-H "Authorization: Bearer $master_token")
API="$KC/admin/realms/$REALM"

status=""
for _ in $(seq 1 30); do
  status="$(curl --silent --show-error --max-time 30 -o /dev/null -w '%{http_code}' "${HEAD[@]}" "${AUTH[@]}" "$API" || true)"
  [ "$status" = 200 ] && break
  sleep 2
done
[ "$status" = 200 ] || fail "realm $REALM does not exist after 60s — check 'kubectl -n $NAMESPACE logs deploy/keycloak' for the import"

client_id_of() {
  "${CURL[@]}" "${HEAD[@]}" "${AUTH[@]}" "$API/clients?clientId=$1" \
    | python3 -c 'import sys,json
clients = json.load(sys.stdin)
print(clients[0]["id"] if clients else "")'
}

# 3. The actor `nova`. Read the stored representation, set the attribute when it drifted, PUT the
#    full representation back (an omitted attribute would keep the old value), read it back.
actor_uuid="$(client_id_of "$ACTOR")"
[ -n "$actor_uuid" ] || fail "realm $REALM has no client $ACTOR — run 'make install-identity' first"
"${CURL[@]}" "${HEAD[@]}" "${AUTH[@]}" "$API/clients/$actor_uuid" > "$tmp/actor-client.json"
state="$(python3 - "$tmp/actor-client.json" "$tmp/actor-merged.json" <<'PY'
import json, sys
current = json.load(open(sys.argv[1]))
attrs = current.setdefault("attributes", {})
if attrs.get("standard.token.exchange.enabled") == "true":
    print("unchanged")
else:
    attrs["standard.token.exchange.enabled"] = "true"
    json.dump(current, open(sys.argv[2], "w"))
    print("changed")
PY
)"
if [ "$state" = "changed" ]; then
  "${CURL[@]}" "${HEAD[@]}" "${AUTH[@]}" -H 'Content-Type: application/json' \
    -X PUT --data @"$tmp/actor-merged.json" "$API/clients/$actor_uuid" >/dev/null \
    || fail "Keycloak refused the attribute update for client $ACTOR"
fi
echo "› client $ACTOR: standard.token.exchange.enabled=true ($state)"

"${CURL[@]}" "${HEAD[@]}" "${AUTH[@]}" "$API/clients/$actor_uuid" > "$tmp/actor-after.json"
stored="$(python3 -c 'import json,sys
print((json.load(open(sys.argv[1])).get("attributes") or {}).get("standard.token.exchange.enabled", ""))' "$tmp/actor-after.json")"
[ "$stored" = "true" ] || fail "the attribute did not stick"

# 4. The subject client's audience. The exchange only succeeds when `nova` is in the subject
#    token's `aud`; the mapper that adds it is `make sync-mcp-client` (#1452).
mcp_uuid="$(client_id_of "$MCP_CLIENT")"
no_audience="the MCP token has no nova audience — run 'make sync-mcp-client' (stardelt/stardelt#1452)"
[ -n "$mcp_uuid" ] || fail "$no_audience"
"${CURL[@]}" "${HEAD[@]}" "${AUTH[@]}" "$API/clients/$mcp_uuid" > "$tmp/mcp-client.json"
python3 - "$tmp/mcp-client.json" "$ACTOR" <<'PY' || fail "$no_audience"
import json, sys
client = json.load(open(sys.argv[1]))
actor = sys.argv[2]
mappers = client.get("protocolMappers") or []
if not any((m.get("config") or {}).get("included.client.audience") == actor for m in mappers):
    sys.exit(1)
PY

# 5. The realm's public issuer: ConfigMap stardelt-identity `issuerUrl`, unless REALM_URL is set.
if [ -z "$REALM_URL" ]; then
  REALM_URL="$(kubectl -n "$NAMESPACE" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}' 2>/dev/null || true)"
fi
REALM_URL="${REALM_URL%/}"
[ -n "$REALM_URL" ] || fail "ConfigMap stardelt-identity has no issuerUrl — run 'make install-identity' first, or set REALM_URL"

# --- attribute --------------------------------------------------------------------------
ok attribute "actor $ACTOR carries standard.token.exchange.enabled=true"

# --- the subject token ------------------------------------------------------------------
printf 'demo\n' > "$tmp/password"
chmod 600 "$tmp/password"
subject_ok=""
if ISSUER="$REALM_URL" USER="${SUBJECT_USER:-demo}" PASSWORD_FILE="$tmp/password" TOKEN_FILE="$tmp/subject" \
     CLIENT_ID="$MCP_CLIENT" python3 scripts/mint-user-token.py > "$tmp/mint.log" 2>&1 \
   && [ -s "$tmp/subject" ]; then
  chmod 600 "$tmp/subject"
  subject_ok=1
else
  subject_error="$(head -n 1 "$tmp/mint.log" 2>/dev/null || true)"
fi

# --- the actor's credentials ------------------------------------------------------------
actor_ok=""
if kubectl -n "$NAMESPACE" get secret stardelt-oidc-clients -o jsonpath="{.data.$ACTOR}" 2>/dev/null \
     | base64 -d > "$tmp/actor-secret" && [ -s "$tmp/actor-secret" ]; then
  chmod 600 "$tmp/actor-secret"
  {
    printf 'url = "%s/protocol/openid-connect/token"\n' "$REALM_URL"
    printf 'user = "%s:%s"\n' "$ACTOR" "$(cat "$tmp/actor-secret")"
  } > "$tmp/actor.conf"
  chmod 600 "$tmp/actor.conf"
  actor_ok=1
fi

# --- exchange ---------------------------------------------------------------------------
exchanged=""
if [ -z "$subject_ok" ]; then
  bad exchange "the person's MCP token could not be minted: ${subject_error:-mint-user-token.py failed}"
elif [ -z "$actor_ok" ]; then
  bad exchange "Secret stardelt-oidc-clients key $ACTOR is not readable"
else
  status="$(curl -sS -K "$tmp/actor.conf" \
    -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \
    --data-urlencode subject_token@"$tmp/subject" \
    -d subject_token_type=urn:ietf:params:oauth:token-type:access_token \
    -d requested_token_type=urn:ietf:params:oauth:token-type:access_token \
    -d "audience=$AUDIENCE" \
    -o "$tmp/exchange.json" -w '%{http_code}' 2>/dev/null)" || status="000"
  [ -n "$status" ] || status="000"
  jq -r '.access_token // empty' "$tmp/exchange.json" > "$tmp/exchanged" 2>/dev/null || true
  if [ -s "$tmp/exchanged" ] && [ "$status" = "200" ]; then
    chmod 600 "$tmp/exchanged"
    exchanged=1
    ok exchange "the realm answered 200 with an access_token"
  else
    error="$(jq -r '.error // empty' "$tmp/exchange.json" 2>/dev/null || true)"
    description="$(jq -r '.error_description // empty' "$tmp/exchange.json" 2>/dev/null || true)"
    bad exchange "status=${status} error=${error:-none} error_description=${description:-none}"
  fi
fi

# --- claims -----------------------------------------------------------------------------
if [ -z "$exchanged" ]; then
  bad claims "no exchanged token to read"
elif claims="$(python3 - "$tmp/exchanged" <<'PY'
import base64, json, sys, time

token = open(sys.argv[1]).read().strip()
parts = token.split(".")
if len(parts) != 3:
    print("segments=%d (not a compact JWS)" % len(parts))
    raise SystemExit(1)
payload = parts[1] + "=" * (-len(parts[1]) % 4)
try:
    claims = json.loads(base64.urlsafe_b64decode(payload))
except ValueError:
    print("payload is not JSON")
    raise SystemExit(1)
aud = claims.get("aud")
if isinstance(aud, list):
    aud = ",".join(str(a) for a in aud)
exp = claims.get("exp")
problems = []
if "trino" not in (aud or "").split(","):
    problems.append("aud=%s" % (aud,))
if not isinstance(exp, int) or exp <= int(time.time()):
    problems.append("exp=%s (not in the future)" % (exp,))
scope = claims.get("scope") or "-"
if problems:
    print(", ".join(problems) + " scope=%s" % scope)
    raise SystemExit(1)
print("aud=%s exp=%s scope=%s" % (aud, exp, scope))
PY
)"; then
  ok claims "$claims"
else
  bad claims "$claims"
fi

# --- trino ------------------------------------------------------------------------------
if [ -z "$exchanged" ]; then
  bad trino "no exchanged token to send"
else
  trino_port="$(free_port 18443)"
  kubectl -n "$NAMESPACE" port-forward svc/trino "$trino_port:8443" > "$tmp/trino-pf.log" 2>&1 &
  trino_pid=$!
  ready=""
  for _ in $(seq 1 40); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$trino_port") 2>/dev/null; then ready=1; break; fi
    sleep 0.5
  done
  if [ -z "$ready" ]; then
    bad trino "the port-forward to svc/trino did not open in 20 s"
  else
    trino_status="$(curl -sk -X POST "https://127.0.0.1:$trino_port/v1/statement" \
      -H "Authorization: Bearer $(cat "$tmp/exchanged")" \
      -H 'Content-Type: text/plain' -H "X-Trino-User: ${SUBJECT_USER:-demo}" -H 'X-Trino-Source: stardelt-mcp' \
      -H "X-Trino-Client-Tags: $ACTOR" --data 'SELECT 1' \
      -o "$tmp/statement" -w '%{http_code}' 2>/dev/null)" || trino_status="000"
    [ -n "$trino_status" ] || trino_status="000"
    id="$(jq -r '.id // empty' "$tmp/statement" 2>/dev/null || true)"
    if [ "$trino_status" = "200" ] && [ -n "$id" ]; then
      ok trino "200 with a statement id"
    else
      body="$(head -c 120 "$tmp/statement" 2>/dev/null | tr '\n' ' ')"
      bad trino "status=${trino_status} body=${body:-empty}"
    fi
  fi
fi

# --- verdict ----------------------------------------------------------------------------
if [ "$PASSED" -ne 4 ] || [ "$FAILED" -ne 0 ]; then
  echo "FAIL ($PASSED of 4)"
  exit 1
fi
echo "PASS (4 of 4)"
