#!/usr/bin/env bash
# Spike for stardelt/stardelt#1700 (slice 0 of #1642): can a Nova-issued personal token act as
# its person downstream — does the refresh-token option (a) work, and is the RFC 8693 exchange
# (option b) available for a `stardelt-dev` person token?
#
# #1642 has to choose how a Nova-issued token becomes a real person downstream. Two facts decide
# it and neither was measured:
#
#   R1  the device grant of the CLI's client `stardelt-dev` asks `openid offline_access`: does the
#       token response carry a `refresh_token`, and what are `expires_in` / `refresh_expires_in`?
#   E1  the refresh_token grant from a plain script, no browser: the HTTP status (expected 200).
#   E2  the refreshed access token's `aud`, `azp`, `preferred_username`, `groups`
#       (expected `azp=stardelt-dev`, `nova` in `aud`, `groups=visitors`).
#   E3  `GET ${NOVA}/api/me` with the refreshed token: the status and the `name` (expected 200).
#   E4  RFC 8693 with the refreshed token as the subject and `nova` as the actor, `audience=trino`
#       — the same request `spike-mcp-token-exchange.sh`'s E2 builds. Either answer is a result:
#       200 makes option (b) available for a person token, `403 access_denied` makes it not.
#   E5  the refresh token is a real, revocable credential: after RFC 7009 revocation E1 answers
#       `400 invalid_grant` (the way `python/stardelt/login.py::revoke` revokes).
#   E6  the control: an opaque `sdp_…` secret is useless downstream — `/api/me` answers 401.
#
# It changes no realm object, no route and no product code: it reads Secret `stardelt-oidc-clients`
# (key `nova`) for the actor and nothing else.
#
# The person is `demo`, the public visitor: its password is printed by `scripts/public-demo.sh`
# and hardcoded by `scripts/verify-product-path.sh`, so it protects nothing. (The ticket named a
# Secret `public-demo-person`; no such Secret exists on the lab — see the #1700 comment.)
#
# It prints no password, secret or token: the password and every token travel through mode-0600
# files in a 0700 `mktemp -d`, the actor's credentials go through curl's `-K` config and every
# token in a request body through `--data-urlencode name@file`. Output is status codes, claim
# names and numbers of seconds; a credential in a log line would be a defect.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1
umask 077

NS="${NS:-stardelt}"
NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"
NOVA="${NOVA%/}"
REALM="${REALM_URL:-https://id.alpha.dev.stardelt.io/realms/stardelt}"
REALM="${REALM%/}"
PERSON="${PERSON:-demo}"
PASSWORD="${PASSWORD:-demo}"
CLIENT="${CLIENT_ID:-stardelt-dev}"
ACTOR="${ACTOR:-nova}"
AUDIENCE="${AUDIENCE:-trino}"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/pat-downstream.XXXXXX")" || exit 1
chmod 700 "$tmp"

cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

for tool in curl jq python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

partial_row() { # partial_row <row> <reason>: "not tested" is a result, never a guess
  echo "$1 not-tested: $2"
  partial=1
}
partial=""

# --- R1: the device grant, offline_access first ----------------------------------------------
printf '%s' "$PASSWORD" > "$tmp/password"
chmod 600 "$tmp/password"

mint() { # mint <scope>: the device grant for $PERSON; the token and its response land in $tmp
  rm -f "$tmp/access" "$tmp/access.response"
  ISSUER="$REALM" USER="$PERSON" PASSWORD_FILE="$tmp/password" TOKEN_FILE="$tmp/access" \
    CLIENT_ID="$CLIENT" SCOPE="$1" python3 scripts/mint-user-token.py > "$tmp/mint.log" 2>&1 \
    && [ -s "$tmp/access" ] && [ -s "$tmp/access.response" ]
}

offline="accepted"
if ! mint "openid offline_access"; then
  # Keycloak's answer when it will not issue an offline token. The realm grants the built-in
  # role `offline_access` to nobody today, so this is the expected path and is itself a result.
  offline="refused: $(tr '\n' ' ' < "$tmp/mint.log" | sed 's/  */ /g' | cut -c1-160)"
  if ! mint "${SCOPE:-openid}"; then
    partial_row R1 "the device grant minted no token even with scope ${SCOPE:-openid}: $(head -n 1 "$tmp/mint.log")"
    echo "spike-pat-downstream: DONE answer=partial"
    echo "summary: answer=partial nothing was minted, nothing measured"
    exit 0
  fi
  offline="$offline; fallback-scope=${SCOPE:-openid}"
fi
chmod 600 "$tmp/access.response"

refresh_present="$(jq -r 'if ((.refresh_token // "") | length) > 0 then "present" else "absent" end' \
  "$tmp/access.response" 2>/dev/null || echo "unreadable")"
ttl="$(jq -r '(.expires_in // "-") | tostring' "$tmp/access.response" 2>/dev/null || echo -)"
refresh_ttl="$(jq -r '(.refresh_expires_in // "-") | tostring' "$tmp/access.response" 2>/dev/null || echo -)"
echo "R1 offline-token=$offline refresh-token=$refresh_present expires_in=$ttl refresh_expires_in=$refresh_ttl"

# --- E1: the refresh_token grant from a script -----------------------------------------------
jq -j '.refresh_token // empty' "$tmp/access.response" > "$tmp/refresh" 2>/dev/null || true
if [ ! -s "$tmp/refresh" ]; then
  partial_row E1 "no refresh_token in the device grant's answer (scope offline_access was not granted)"
  partial_row E2 "no refreshed token"
  partial_row E3 "no refreshed token"
  partial_row E4 "no refreshed token"
  partial_row E5 "no refresh token to revoke"
else
  chmod 600 "$tmp/refresh"
  status="$(curl -sS --max-time 30 -X POST "$REALM/protocol/openid-connect/token" \
    -d grant_type=refresh_token -d "client_id=$CLIENT" \
    --data-urlencode refresh_token@"$tmp/refresh" \
    -o "$tmp/refresh-answer" -w '%{http_code}' 2>/dev/null)" || status="000"
  [ -n "$status" ] || status="000"
  jq -j '.access_token // empty' "$tmp/refresh-answer" > "$tmp/refreshed" 2>/dev/null || true
  echo "E1 refresh=$status error=$(jq -r '.error // "-"' "$tmp/refresh-answer" 2>/dev/null || echo -)"
  [ -s "$tmp/refreshed" ] && chmod 600 "$tmp/refreshed"

  # --- E2: what the refreshed token says -------------------------------------------------------
  if [ -s "$tmp/refreshed" ]; then
    e2="$(python3 - "$tmp/refreshed" <<'PY'
import base64, json, sys

token = open(sys.argv[1]).read().strip()
parts = token.split(".")
if len(parts) != 3:
    print("segments=%d (not a compact JWS)" % len(parts))
    raise SystemExit
claims = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))


def joined(value, empty):
    if isinstance(value, list):
        return ",".join(str(item) for item in value) or empty
    return str(value) if value else empty


print("aud=%s azp=%s preferred_username=%s groups=%s" % (
    joined(claims.get("aud"), "-"), claims.get("azp") or "-",
    claims.get("preferred_username") or "-", joined(claims.get("groups"), "-")))
PY
)"
    echo "E2 $e2"

    # --- E3: Nova accepts the refreshed token -----------------------------------------------
    me_status="$(curl -sS --max-time 30 -H "Authorization: Bearer $(cat "$tmp/refreshed")" \
      "$NOVA/api/me" -o "$tmp/me" -w '%{http_code}' 2>/dev/null)" || me_status="000"
    [ -n "$me_status" ] || me_status="000"
    echo "E3 me=$me_status name=$(jq -r '.name // .user // "-"' "$tmp/me" 2>/dev/null || echo -)"

    # --- E4: RFC 8693 with `nova` as the actor ----------------------------------------------
    actor_ok=""
    if kubectl -n "$NS" get secret stardelt-oidc-clients -o jsonpath='{.data.nova}' 2>/dev/null \
         | base64 -d > "$tmp/actor-secret" && [ -s "$tmp/actor-secret" ]; then
      chmod 600 "$tmp/actor-secret"
      issuer="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}' 2>/dev/null)"
      issuer="${issuer%/}"
      [ -n "$issuer" ] || issuer="$REALM"
      {
        printf 'url = "%s/protocol/openid-connect/token"\n' "$issuer"
        printf 'user = "%s:%s"\n' "$ACTOR" "$(cat "$tmp/actor-secret")"
      } > "$tmp/actor.conf"
      chmod 600 "$tmp/actor.conf"
      actor_ok=1
    fi
    if [ -z "$actor_ok" ]; then
      partial_row E4 "Secret stardelt-oidc-clients key $ACTOR is not readable, so there is no actor"
    else
      ex_status="$(curl -sS --max-time 30 -K "$tmp/actor.conf" \
        -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \
        --data-urlencode subject_token@"$tmp/refreshed" \
        -d subject_token_type=urn:ietf:params:oauth:token-type:access_token \
        -d requested_token_type=urn:ietf:params:oauth:token-type:access_token \
        -d "audience=$AUDIENCE" \
        -o "$tmp/exchange" -w '%{http_code}' 2>/dev/null)" || ex_status="000"
      [ -n "$ex_status" ] || ex_status="000"
      if jq -e '.access_token' "$tmp/exchange" >/dev/null 2>&1; then
        jq -j '.access_token' "$tmp/exchange" > "$tmp/exchanged"
        chmod 600 "$tmp/exchanged"
        aud="$(python3 - "$tmp/exchanged" <<'PY'
import base64, json, sys

parts = open(sys.argv[1]).read().strip().split(".")
if len(parts) != 3:
    print("-")
    raise SystemExit
aud = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4))).get("aud")
if isinstance(aud, list):
    aud = ",".join(str(item) for item in aud)
print(aud or "-")
PY
)"
        echo "E4 exchange=$ex_status token=yes aud=$aud error=-"
      else
        echo "E4 exchange=$ex_status token=no error=$(jq -r '.error // "-"' "$tmp/exchange" 2>/dev/null || echo -) description=$(jq -r '.error_description // "-"' "$tmp/exchange" 2>/dev/null || echo -)"
      fi
    fi
  else
    partial_row E2 "the refresh answered $status: $(jq -r '.error // "-"' "$tmp/refresh-answer" 2>/dev/null || echo -)"
    partial_row E3 "no refreshed token"
    partial_row E4 "no refreshed token"
  fi

  # --- E5: revocation makes it a real credential -------------------------------------------
  revoke_status="$(curl -sS --max-time 30 -X POST "$REALM/protocol/openid-connect/revoke" \
    -d "client_id=$CLIENT" -d token_type_hint=refresh_token \
    --data-urlencode token@"$tmp/refresh" \
    -o "$tmp/revoke" -w '%{http_code}' 2>/dev/null)" || revoke_status="000"
  [ -n "$revoke_status" ] || revoke_status="000"
  after_status="$(curl -sS --max-time 30 -X POST "$REALM/protocol/openid-connect/token" \
    -d grant_type=refresh_token -d "client_id=$CLIENT" \
    --data-urlencode refresh_token@"$tmp/refresh" \
    -o "$tmp/after-revoke" -w '%{http_code}' 2>/dev/null)" || after_status="000"
  [ -n "$after_status" ] || after_status="000"
  echo "E5 revoke=$revoke_status refresh-after-revoke=$after_status error=$(jq -r '.error // "-"' "$tmp/after-revoke" 2>/dev/null || echo -)"
fi

# --- E6: the control, an opaque secret is useless downstream ---------------------------------
opaque_status="$(curl -sS --max-time 30 -H 'Authorization: Bearer sdp_deadbeef' \
  "$NOVA/api/me" -o "$tmp/opaque" -w '%{http_code}' 2>/dev/null)" || opaque_status="000"
[ -n "$opaque_status" ] || opaque_status="000"
echo "E6 opaque-token=$opaque_status"

if [ -n "$partial" ]; then
  echo "spike-pat-downstream: DONE answer=partial"
  echo "summary: answer=partial some rows are not tested; the lines above name them"
  exit 0
fi
echo "spike-pat-downstream: DONE answer=complete"
echo "summary: answer=complete every row answered"
