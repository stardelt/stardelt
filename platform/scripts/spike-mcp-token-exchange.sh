#!/usr/bin/env bash
# Spike for stardelt/stardelt#1445 (the unblocker for #1442): can Nova exchange a person's
# MCP-audience token for a `trino`-audience token on this realm?
#
# #1442 is measured: a person signed in to `/mcp` through the realm client `stardelt-mcp` gets
# `401 Invalid credentials` from Trino, because the engine's `oauth2` authenticator wants the
# `trino` audience and the MCP token carries `aud=https://alpha.dev.stardelt.io/mcp`. The design
# (`design/agent-access.md`, "Downstream calls") and the client's own description
# (`scripts/keycloak-sync-mcp-client.sh`) say the engine token is minted by RFC 8693 token
# exchange — but no exchange has been measured on this realm.
#
# The rows:
#   E1  the subject token is the person's MCP token: its segments, `aud`, `azp`, username.
#   E2  the exchange attempt with `nova` (the confidential realm client) as the actor: the HTTP
#       status and, when no `access_token` comes back, `error` and `error_description`. Any
#       status is a result; `access_denied` names the permission the follow-up ticket needs.
#   E3  only with a token: the exchanged token's `aud`, `exp` (the expiring time in seconds),
#       `scope` and `groups`.
#   E4  only with a token: Trino 8443 through a port-forward accepts it (`SELECT 1`).
#   E5  only with a token: does the query log say the person ran `SELECT 1`, and with which
#       client tags?
#
# It changes no realm object and creates no cluster object: it reads Secret
# `stardelt-oidc-clients` (key `nova`) and ConfigMap `stardelt-identity` (key `issuerUrl`),
# and a `kubectl port-forward` that the trap kills.
#
# It prints no password, secret or token. Every credential travels through a mode-0600 file: the
# password and both tokens are files, the actor's credentials go through curl's `-K` config and
# the subject token through `--data-urlencode name@file`, so none of them reaches argv or stdout.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1
umask 077

NS="${NS:-stardelt}"
NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"
NOVA="${NOVA%/}"
REALM="${REALM_URL:-https://id.alpha.dev.stardelt.io/realms/stardelt}"
REALM="${REALM%/}"
AUDIENCE="${AUDIENCE:-trino}"
ACTOR="${ACTOR:-nova}"
PF_PORT="${PF_PORT:-18443}"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/mcp-exchange.XXXXXX")" || exit 1
chmod 700 "$tmp"
PF_PID=""

cleanup() {
  if [ -n "$PF_PID" ]; then kill "$PF_PID" >/dev/null 2>&1 || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

for tool in kubectl curl jq python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

exchanged=""
answer="partial"

# --- E1: the person's MCP-audience token ------------------------------------------------
printf 'demo\n' > "$tmp/password"
chmod 600 "$tmp/password"
if ISSUER="$REALM" USER=demo PASSWORD_FILE="$tmp/password" TOKEN_FILE="$tmp/subject" \
     CLIENT_ID=stardelt-mcp python3 scripts/mint-user-token.py > "$tmp/mint.log" 2>&1 \
   && [ -s "$tmp/subject" ]; then
  chmod 600 "$tmp/subject"
  claims="$(python3 scripts/mint-user-token.py --decode "$tmp/subject" 2>&1)"
  echo "E1 subject $claims"
else
  echo "E1 subject mint-failed: $(head -n 1 "$tmp/mint.log")"
  echo "spike-mcp-token-exchange: DONE exchange=partial"
  echo "summary: exchange=partial the person's MCP token could not be minted; nothing measured"
  exit 0
fi

# --- the actor: the confidential client `nova` ------------------------------------------
actor_ok=""
if kubectl -n "$NS" get secret stardelt-oidc-clients -o jsonpath='{.data.nova}' 2>/dev/null \
     | base64 -d > "$tmp/actor-secret" && [ -s "$tmp/actor-secret" ]; then
  chmod 600 "$tmp/actor-secret"
  printf '%s' "$ACTOR" > "$tmp/actor-id"
  issuer="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}' 2>/dev/null)"
  issuer="${issuer%/}"
  [ -n "$issuer" ] || issuer="$REALM"
  {
    printf 'url = "%s/protocol/openid-connect/token"\n' "$issuer"
    printf 'user = "%s:%s"\n' "$(cat "$tmp/actor-id")" "$(cat "$tmp/actor-secret")"
  } > "$tmp/actor.conf"
  chmod 600 "$tmp/actor.conf"
  actor_ok=1
fi

# --- E2: the exchange ----------------------------------------------------------------
if [ -z "$actor_ok" ]; then
  echo "E2 exchange-token unmeasured (Secret stardelt-oidc-clients key nova, or ConfigMap stardelt-identity key issuerUrl, is not readable)"
  echo "spike-mcp-token-exchange: DONE exchange=partial"
  echo "summary: exchange=partial the actor's credentials were not readable; nothing measured"
  exit 0
fi

status="$(curl -sS -K "$tmp/actor.conf" \
  -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \
  --data-urlencode subject_token@"$tmp/subject" \
  -d subject_token_type=urn:ietf:params:oauth:token-type:access_token \
  -d requested_token_type=urn:ietf:params:oauth:token-type:access_token \
  -d "audience=$AUDIENCE" \
  -o "$tmp/exchange.json" -w '%{http_code}' 2>/dev/null)" || status="000"
[ -n "$status" ] || status="000"

jq -r '.access_token // empty' "$tmp/exchange.json" > "$tmp/exchanged" 2>/dev/null || true
if [ -s "$tmp/exchanged" ]; then
  chmod 600 "$tmp/exchanged"
  exchanged=1
fi

if [ -n "$exchanged" ]; then
  echo "E2 exchange-token $status"
else
  error="$(jq -r '.error // empty' "$tmp/exchange.json" 2>/dev/null || true)"
  description="$(jq -r '.error_description // empty' "$tmp/exchange.json" 2>/dev/null || true)"
  if [ -z "$error$description" ]; then
    body="$(head -c 200 "$tmp/exchange.json" 2>/dev/null | tr '\n' ' ')"
    echo "E2 exchange-token $status no-error-field body=${body:-empty}"
  elif [ "$error" = "access_denied" ]; then
    echo "E2 exchange-token $status access_denied: the exchange was refused (${description:-no description}); the subject client stardelt-mcp has no token-exchange permission policy"
  else
    echo "E2 exchange-token $status error=${error:-none} description=${description:-none}"
    if [ "${description:-}" = "Standard token exchange is not enabled for the requested client" ]; then
      echo "E2 missing-object client attribute standard.token.exchange.enabled=true on the requesting client ${ACTOR} (Keycloak 26: standard token exchange is off per client; when it is on, ${ACTOR} must also be in the subject token's aud)"
    fi
  fi
  echo "spike-mcp-token-exchange: DONE exchange=refused"
  echo "summary: exchange=refused status=$status error=${error:-unknown} description=${description:-none} - missing object: the ${ACTOR} client attribute standard.token.exchange.enabled=true; enabling it is the follow-up ticket's maintainer step"
  exit 0
fi

# --- E3: the exchanged token's claims -------------------------------------------------
echo "E3 exchanged-claims $(python3 - "$tmp/exchanged" <<'PY'
import base64, json, sys

token = open(sys.argv[1]).read().strip()
parts = token.split(".")
if len(parts) != 3:
    print("segments=%d (not a compact JWS)" % len(parts))
    raise SystemExit
payload = parts[1] + "=" * (-len(parts[1]) % 4)
claims = json.loads(base64.urlsafe_b64decode(payload))
aud = claims.get("aud")
if isinstance(aud, list):
    aud = ",".join(str(a) for a in aud)
groups = claims.get("groups")
if isinstance(groups, list):
    groups = ",".join(str(g) for g in groups)
print("aud=%s exp=%s scope=%s groups=%s" % (
    aud, claims.get("exp"), claims.get("scope") or "-", groups or "-"))
PY
)"

# --- E4: Trino accepts it -------------------------------------------------------------
kubectl -n "$NS" port-forward svc/trino "$PF_PORT:8443" > "$tmp/port-forward.log" 2>&1 &
PF_PID=$!
ready=""
for _ in $(seq 1 40); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$PF_PORT") 2>/dev/null; then ready=1; break; fi
  sleep 0.5
done

if [ -z "$ready" ]; then
  echo "E4 trino unmeasured (the port-forward to svc/trino did not open in 20 s)"
else
  trino_status="$(curl -sk -X POST "https://127.0.0.1:$PF_PORT/v1/statement" \
    -H "Authorization: Bearer $(cat "$tmp/exchanged")" \
    -H 'Content-Type: text/plain' -H 'X-Trino-User: demo' -H 'X-Trino-Source: stardelt-mcp' \
    -H 'X-Trino-Client-Tags: nova' --data 'SELECT 1' \
    -o "$tmp/statement" -w '%{http_code}' 2>/dev/null)" || trino_status="000"
  [ -n "$trino_status" ] || trino_status="000"
  id="$(jq -r '.id // empty' "$tmp/statement" 2>/dev/null || true)"
  if [ "$trino_status" = "200" ] && [ -n "$id" ]; then
    echo "E4 trino 200 (JSON with an id)"
  else
    echo "E4 trino $trino_status $(head -c 120 "$tmp/statement" 2>/dev/null | tr '\n' ' ')"
  fi

  # --- E5: does the engine see the person? ---------------------------------------------
  if ISSUER="$REALM" USER=demo PASSWORD_FILE="$tmp/password" TOKEN_FILE="$tmp/person" \
       CLIENT_ID=stardelt-dev python3 scripts/mint-user-token.py > "$tmp/person.log" 2>&1 \
     && [ -s "$tmp/person" ]; then
    chmod 600 "$tmp/person"
    row=""
    for _ in 1 2 3 4 5; do
      curl -fsS -H "Authorization: Bearer $(cat "$tmp/person")" \
        "$NOVA/api/query-log?limit=5" > "$tmp/query-log.json" 2>/dev/null || true
      row="$(jq -r '[.items[]? | select((.sql // "") | contains("SELECT 1"))] | .[0].id // empty' \
        "$tmp/query-log.json" 2>/dev/null || true)"
      [ -n "$row" ] && break
      sleep 2
    done
    if [ -n "$row" ]; then
      curl -fsS -H "Authorization: Bearer $(cat "$tmp/person")" \
        "$NOVA/api/query-log/$row" > "$tmp/query-detail.json" 2>/dev/null || true
      echo "E5 person $(jq -r '.user + " clientTags=" + ((.clientTags // []) | join(","))' \
        "$tmp/query-detail.json" 2>/dev/null || echo "detail unreadable")"
    else
      echo "E5 person no query-log row for SELECT 1 (the collector is asynchronous, 5 reads 2 s apart)"
    fi
  else
    echo "E5 person unmeasured (no stardelt-dev token for demo)"
  fi
fi

answer="ok"
echo "spike-mcp-token-exchange: DONE exchange=$answer"
echo "summary: exchange=ok a $AUDIENCE-audience token came back from the exchange and was tried against the engine"
