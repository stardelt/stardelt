#!/usr/bin/env bash
# Converge the live Keycloak realm's GitHub Actions identity provider (platform#57).
#
#   NAMESPACE=stardelt make sync-github-idp
#
# Why a script: `--import-realm` runs on a fresh realm only, so a realm that already
# existed when the template gained `identityProviders` has no `github-actions`
# provider. The clients a DeployStage renders authenticate with
# `clientAuthenticatorType: federated-jwt` and `jwt.credential.issuer: github-actions`
# (operator/crates/stardelt-operator/src/deploy_stage_keycloak.rs); without the
# provider the realm does not know that issuer and the CI sign-in fails.
#
# The desired body is read from chart/files/keycloak-realm.json, the same entry
# `--import-realm` creates on a fresh realm (platform/scripts/tests/test_realm_twin.py
# keeps it identical to the operator's copy), never a third copy. Idempotent: a
# present provider is compared field by field and written only when it drifted.
#
# Reaches Keycloak through svc/keycloak (port-forward) because the public
# `/admin` API is closed; the master-realm password travels only through a
# mode-0600 temp file, never argv or stdout.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NAMESPACE:-stardelt}"
REALM="${REALM:-stardelt}"
MASTER_SECRET="${MASTER_SECRET:-keycloak-admin}"
IDP_ALIAS="github-actions"

CURL=(curl --fail-with-body --silent --show-error --max-time 30)
CURL_SOFT=(curl --silent --show-error --max-time 30)

for tool in kubectl curl python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "keycloak-sync-github-idp: $tool not found" >&2; exit 1; }
done

umask 077
tmp="$(mktemp -d)"
pf_pid=""
cleanup() {
  if [ -n "$pf_pid" ]; then kill "$pf_pid" 2>/dev/null || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT

fail() { echo "keycloak-sync-github-idp: $*" >&2; exit 1; }

# 1. Reach the admin API on loopback, through the Service.
local_port=18250
while (exec 3<>"/dev/tcp/127.0.0.1/$local_port") 2>/dev/null; do
  local_port=$((local_port + 1))
done
kubectl -n "$NAMESPACE" port-forward svc/keycloak "$local_port:8080" >"$tmp/port-forward.log" 2>&1 &
pf_pid=$!
ready=""
for _ in $(seq 1 30); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$local_port") 2>/dev/null; then ready=1; break; fi
  sleep 0.5
done
[ -n "$ready" ] || fail "port-forward to svc/keycloak never came up — is Keycloak installed in namespace $NAMESPACE?"
KC="http://127.0.0.1:$local_port"

# Keycloak in production mode answers on its public hostname (KC_HOSTNAME), so a
# request to 127.0.0.1 has to say which host it is meant for.
strip_scheme() { printf '%s' "$1" | sed -E 's|https?://||; s|/.*||'; }
KC_HOST="$(strip_scheme "$(kubectl -n "$NAMESPACE" get configmap keycloak-env -o jsonpath='{.data.KC_HOSTNAME}' 2>/dev/null || true)")"
[ -n "$KC_HOST" ] || fail "ConfigMap keycloak-env has no KC_HOSTNAME — run 'make install-identity'"
HEAD=(-H "X-Forwarded-Host: $KC_HOST" -H 'X-Forwarded-Proto: https')

# 2. Master-realm token. Retry: the Service can accept a connection while Keycloak
#    is still finishing its boot.
kubectl -n "$NAMESPACE" get secret "$MASTER_SECRET" -o jsonpath='{.data.password}' | base64 -d > "$tmp/master-pw" \
  || fail "cannot read Secret $MASTER_SECRET — run 'make install-identity'"
master_token=""
for _ in $(seq 1 20); do
  master_token="$("${CURL_SOFT[@]}" "${HEAD[@]}" -X POST "$KC/realms/master/protocol/openid-connect/token" \
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
  status="$("${CURL_SOFT[@]}" -o /dev/null -w '%{http_code}' "${HEAD[@]}" "${AUTH[@]}" "$API" || true)"
  [ "$status" = 200 ] && break
  sleep 2
done
[ "$status" = 200 ] || fail "realm $REALM does not exist after 60s — check 'kubectl -n $NAMESPACE logs deploy/keycloak' for the import"

# 3. The desired body: the template's `github-actions` entry, and nothing else.
desired="$tmp/github-actions.json"
python3 - "$desired" <<'PY'
import json, pathlib, sys
realm = json.loads(pathlib.Path("chart/files/keycloak-realm.json").read_text())
entries = [i for i in realm.get("identityProviders", []) if i.get("alias") == "github-actions"]
if len(entries) != 1:
    raise SystemExit(
        "keycloak-sync-github-idp: chart/files/keycloak-realm.json must carry exactly one "
        f"github-actions identity provider (found {len(entries)})"
    )
pathlib.Path(sys.argv[1]).write_text(json.dumps(entries[0], indent=1))
PY

# 4. Converge: missing is created, a drifted entry is restored, an equal one is
#    left alone. Keycloak adds fields of its own (`internalId`), so only the
#    fields the template owns are compared.
code="$("${CURL_SOFT[@]}" -o "$tmp/live.json" -w '%{http_code}' "${HEAD[@]}" "${AUTH[@]}" \
  "$API/identity-provider/instances/$IDP_ALIAS" || true)"
case "$code" in
  404)
    "${CURL[@]}" "${HEAD[@]}" "${AUTH[@]}" -H 'Content-Type: application/json' -X POST \
      --data @"$desired" "$API/identity-provider/instances" >/dev/null \
      || fail "Keycloak refused to create identity provider $IDP_ALIAS"
    change=created ;;
  200)
    change="$(python3 - "$tmp/live.json" "$desired" <<'PY'
import json, sys
live = json.load(open(sys.argv[1]))
want = json.load(open(sys.argv[2]))
# Keycloak never returns a stored secret: it answers `**********`. `clientSecret`
# is therefore compared for presence only, the way the sibling sync scripts skip
# the keys Keycloak adds to a stored body.
want_config = {k: v for k, v in (want.get("config") or {}).items() if k != "clientSecret"}
same = (live.get("providerId") == want["providerId"]
        and bool(live.get("enabled")) == bool(want["enabled"])
        and bool(live.get("hideOnLogin")) == bool(want["hideOnLogin"])
        and all((live.get("config") or {}).get(k) == v for k, v in want_config.items()))
print("unchanged" if same else "changed")
PY
)"
    if [ "$change" = changed ]; then
      "${CURL[@]}" "${HEAD[@]}" "${AUTH[@]}" -H 'Content-Type: application/json' -X PUT \
        --data @"$desired" "$API/identity-provider/instances/$IDP_ALIAS" >/dev/null \
        || fail "Keycloak refused the update of identity provider $IDP_ALIAS"
      change=updated
    else
      change=unchanged
    fi ;;
  *)
    fail "GET $API/identity-provider/instances/$IDP_ALIAS answered HTTP $code" ;;
esac

# 5. Read the instance back and report what the realm now holds.
"${CURL[@]}" "${HEAD[@]}" "${AUTH[@]}" "$API/identity-provider/instances/$IDP_ALIAS" > "$tmp/readback.json"
python3 - "$tmp/readback.json" "$change" <<'PY' || fail "the read-back of identity provider $IDP_ALIAS is incomplete"
import json, sys
idp = json.load(open(sys.argv[1]))
config = idp.get("config") or {}
for key in ("issuer", "clientId"):
    if not config.get(key):
        sys.exit("config has no %s" % key)
print("keycloak-sync-github-idp: github-actions change=%s enabled=%s hideOnLogin=%s issuer=%s clientId=%s"
      % (sys.argv[2], "true" if idp.get("enabled") else "false",
         "true" if idp.get("hideOnLogin") else "false",
         config["issuer"], config["clientId"]))
PY

# 6. Negative proof: an alias that does not exist answers 404, so the 200 above is
#    the provider and not a catch-all.
absent="$("${CURL_SOFT[@]}" -o /dev/null -w '%{http_code}' "${HEAD[@]}" "${AUTH[@]}" \
  "$API/identity-provider/instances/no-such-idp" || true)"
[ "$absent" = 404 ] || fail "absent identity provider answered HTTP $absent, expected 404"
echo "keycloak-sync-github-idp: absent-idp answers 404"
