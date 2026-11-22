#!/usr/bin/env bash
# A DeployStage is four facts that must agree, and nothing checked the agreement live (#57
# slice 7):
#
#   1. the object's spec.subject and spec.audience (kind DeployStage, operator crd.rs);
#   2. the Keycloak client the operator renders, stardelt-deploy-<stage>, with
#      clientAuthenticatorType federated-jwt (deploy_stage_keycloak.rs::client_payload);
#   3. attributes["jwt.credential.issuer"] = github-actions and
#      attributes["jwt.credential.sub"] = spec.subject — the CI subject the client accepts;
#   4. the realm's github-actions identity provider (3 names it) and one aud-<audience>
#      mapper carrying the stage's audience.
#
# A drift in any of them becomes a CI job that cannot sign in, with no check saying which of
# the four is wrong. This reads each one off the live realm after the operator rendered it, and
# fails with one sentence per fact.
#
#   NAMESPACE=stardelt bash scripts/verify-deploy-stage.sh
#
# Creates one DeployStage named `verify-stage` (override with STAGE), asserts the facts and
# deletes it again on every exit path; no other object is touched. Reaches the admin API through
# svc/keycloak (port-forward); the master credential travels only through a mode-0600 file.
#
# Exit codes: 0 PASS (7 of 7), 1 FAIL, 3 UNVERIFIED (a tool, the CRD or the realm admin API is
# absent — nothing was measured).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NS="${NS:-${NAMESPACE:-stardelt}}"
REALM="${REALM:-stardelt}"
STAGE="${STAGE:-verify-stage}"
AUDIENCE="${AUDIENCE:-stardelt-deploy}"
TIMEOUT="${TIMEOUT:-120}"
MASTER_SECRET="${MASTER_SECRET:-keycloak-admin}"
SUBJECT="${SUBJECT:-repo:stardelt@1/stardelt@1:ref:refs/heads/verify}"
CLIENT_ID="stardelt-deploy-${STAGE}"

CURL=(curl --fail-with-body --silent --show-error --max-time 30)
CURL_SOFT=(curl --silent --show-error --max-time 30)

for tool in kubectl curl python3 base64; do
  command -v "$tool" >/dev/null 2>&1 \
    || { echo "verify-deploy-stage: UNVERIFIED ($tool not found)"; exit 3; }
done

kubectl get crd deploystages.stardelt.io >/dev/null 2>&1 \
  || { echo "verify-deploy-stage: UNVERIFIED (the DeployStage CRD is not installed)"; exit 3; }

umask 077
tmp="$(mktemp -d)"
pf_pid=""
KC=""
API=""
AUTH=()
applied=no
cleaned=no

# The trap is the contract: the stage and the client it owns are gone after every exit path,
# including a FAIL and a signal. `client-deleted` is read from the realm, not assumed.
cleanup() {
  [ "$cleaned" = no ] || return 0
  cleaned=yes
  kubectl -n "$NS" delete deploystage "$STAGE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "$applied" = yes ]; then
    local deleted=no answer="" count=""
    local deadline=$((SECONDS + TIMEOUT))
    while :; do
      answer="$("${CURL_SOFT[@]}" "${HEAD[@]}" "${AUTH[@]}" "$API/clients?clientId=$CLIENT_ID" 2>/dev/null \
        | python3 -c 'import json,sys
try: print(len(json.load(sys.stdin)))
except Exception: print("")' 2>/dev/null || true)"
      count="$answer"
      if [ "$count" = 0 ]; then deleted=yes; break; fi
      [ "$SECONDS" -lt "$deadline" ] || break
      sleep 1
    done
    echo "cleanup stage-deleted=yes client-deleted=$deleted"
  fi
  if [ -n "$pf_pid" ]; then kill "$pf_pid" 2>/dev/null || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

fail() { echo "$*"; exit 1; }
unverified() { echo "verify-deploy-stage: UNVERIFIED ($*)"; exit 3; }

# 1. The admin API on loopback, through the Service — the public ingress publishes no /admin.
local_port=18260
while (exec 3<>"/dev/tcp/127.0.0.1/$local_port") 2>/dev/null; do
  local_port=$((local_port + 1))
done
kubectl -n "$NS" port-forward svc/keycloak "$local_port:8080" >"$tmp/port-forward.log" 2>&1 &
pf_pid=$!
ready=""
for _ in $(seq 1 30); do
  if (exec 3<>"/dev/tcp/127.0.0.1/$local_port") 2>/dev/null; then ready=1; break; fi
  sleep 0.5
done
[ -n "$ready" ] || unverified "the realm admin API did not answer"
KC="http://127.0.0.1:$local_port"

# Keycloak in production mode answers on its public hostname, so a request to 127.0.0.1 has to
# say which host it is meant for.
strip_scheme() { printf '%s' "$1" | sed -E 's|https?://||; s|/.*||'; }
KC_HOST="$(strip_scheme "$(kubectl -n "$NS" get configmap keycloak-env -o jsonpath='{.data.KC_HOSTNAME}' 2>/dev/null || true)")"
[ -n "$KC_HOST" ] || unverified "the realm admin API did not answer"
HEAD=(-H "X-Forwarded-Host: $KC_HOST" -H 'X-Forwarded-Proto: https')

# 2. The master-realm token. Retried: the Service accepts a connection while Keycloak boots.
kubectl -n "$NS" get secret "$MASTER_SECRET" -o jsonpath='{.data.password}' > "$tmp/master-pw.b64" 2>/dev/null \
  || unverified "the realm admin API did not answer"
base64 -d < "$tmp/master-pw.b64" > "$tmp/master-pw" 2>/dev/null \
  || unverified "the realm admin API did not answer"
token=""
for _ in $(seq 1 20); do
  token="$("${CURL_SOFT[@]}" "${HEAD[@]}" -X POST "$KC/realms/master/protocol/openid-connect/token" \
    --data-urlencode "username=admin" --data-urlencode "password@$tmp/master-pw" \
    --data-urlencode "client_id=admin-cli" --data-urlencode "grant_type=password" 2>/dev/null \
    | python3 -c 'import sys,json
try: print(json.load(sys.stdin).get("access_token") or "")
except Exception: print("")' 2>/dev/null || true)"
  [ -n "$token" ] && break
  sleep 2
done
[ -n "$token" ] || unverified "the realm admin API did not answer"
AUTH=(-H "Authorization: Bearer $token")
API="$KC/admin/realms/$REALM"
status=""
for _ in $(seq 1 30); do
  status="$("${CURL_SOFT[@]}" -o /dev/null -w '%{http_code}' "${HEAD[@]}" "${AUTH[@]}" "$API" || true)"
  [ "$status" = 200 ] && break
  sleep 2
done
[ "$status" = 200 ] || unverified "the realm admin API did not answer"

# 3. One stage of our own. The subject is the shape the CRD's pattern takes with GitHub's
#    immutable ids (repo:<owner>@<id>/<name>@<id>:ref:refs/heads/<branch>).
kubectl -n "$NS" apply -f - >/dev/null <<EOF
apiVersion: stardelt.io/v1alpha1
kind: DeployStage
metadata:
  name: ${STAGE}
  namespace: ${NS}
spec:
  subject: ${SUBJECT}
  audience: ${AUDIENCE}
EOF
applied=yes

# 4. The client the operator renders. Asked of the realm once a second, because the render is
#    an API call the reconcile makes after admission returned.
answer=""
count=0
uuid=""
deadline=$((SECONDS + TIMEOUT))
while :; do
  answer="$("${CURL_SOFT[@]}" "${HEAD[@]}" "${AUTH[@]}" "$API/clients?clientId=$CLIENT_ID" 2>/dev/null \
    | python3 -c 'import json,sys
try: clients = json.load(sys.stdin)
except Exception: clients = []
print(len(clients), clients[0].get("id", "") if clients else "")' 2>/dev/null || true)"
  count="${answer%% *}"
  uuid="${answer#* }"
  count="${count:-0}"
  uuid="${uuid:-}"
  [ "$count" -gt 0 ] && break
  [ "$SECONDS" -lt "$deadline" ] || break
  sleep 1
done

if [ "$count" -gt 1 ]; then
  fail "FAIL two clients answer clientId=$CLIENT_ID"
fi
if [ "$count" -eq 0 ]; then
  echo "the stage reports: $(kubectl -n "$NS" get deploystage "$STAGE" -o jsonpath='{.status.conditions}' 2>/dev/null || true)"
  fail "FAIL the operator did not render client $CLIENT_ID"
fi

"${CURL[@]}" "${HEAD[@]}" "${AUTH[@]}" "$API/clients/$uuid" > "$tmp/client.json" \
  || fail "FAIL Keycloak did not answer for client $CLIENT_ID"
subject="$(kubectl -n "$NS" get deploystage "$STAGE" -o jsonpath='{.spec.subject}' 2>/dev/null || true)"
idp_status="$("${CURL_SOFT[@]}" -o "$tmp/idp.json" -w '%{http_code}' "${HEAD[@]}" "${AUTH[@]}" \
  "$API/identity-provider/instances/github-actions" 2>/dev/null || true)"

# 5. The seven facts, one PASS/FAIL line each. The python block prints `check<TAB>verdict<TAB>
#    sentence`; the shell only formats and counts.
python3 - "$tmp/client.json" "$subject" "$AUDIENCE" "$idp_status" "$tmp/idp.json" > "$tmp/checks" <<'PY'
import json, sys

client = json.load(open(sys.argv[1]))
subject, audience, idp_status = sys.argv[2], sys.argv[3], sys.argv[4]
idp = {}
if idp_status == "200":
    try:
        idp = json.load(open(sys.argv[5]))
    except (OSError, ValueError):
        idp = {}
attrs = client.get("attributes") or {}
none = "(none)"


def row(check, ok, good, bad):
    print("\t".join([check, "PASS" if ok else "FAIL", good if ok else bad]))


auth = client.get("clientAuthenticatorType") or none
row("auth", auth == "federated-jwt",
    "client %s uses federated-jwt" % client.get("clientId", none),
    "the client does not use federated-jwt authentication")

issuer = attrs.get("jwt.credential.issuer") or none
row("issuer", issuer == "github-actions",
    "jwt.credential.issuer is github-actions",
    "the client names issuer %s, not github-actions" % issuer)

pinned = attrs.get("jwt.credential.sub") or none
row("subject", pinned == subject,
    "jwt.credential.sub is %s" % subject,
    "the client pins subject %s, the stage says %s" % (pinned, subject or none))

mapper = None
for entry in client.get("protocolMappers") or []:
    config = entry.get("config") or {}
    if (entry.get("protocolMapper") == "oidc-audience-mapper"
            and config.get("included.custom.audience") == audience):
        mapper = entry
        break
row("audience", mapper is not None,
    "%s carries %s" % ((mapper or {}).get("name") or "aud-" + audience, audience),
    "no audience mapper carries %s" % audience)

row("idp", idp_status == "200",
    "the realm has the github-actions identity provider",
    "the realm has no github-actions identity provider - run make sync-github-idp (#988)")

row("idp-enabled", bool(idp.get("enabled")),
    "the github-actions identity provider is enabled, so Keycloak validates an assertion",
    "the github-actions identity provider is disabled - a CI assertion is refused")

row("service-account", client.get("serviceAccountsEnabled") is True,
    "client %s may retrieve a service account, so client_credentials is accepted" % client.get("clientId", none),
    "the client cannot retrieve a service account - Keycloak answers unauthorized_client before it reads the assertion")
PY

passed=0
total=0
while IFS=$'\t' read -r check verdict sentence; do
  printf '%s %-10s %s\n' "$verdict" "$check" "$sentence"
  total=$((total + 1))
  if [ "$verdict" = PASS ]; then passed=$((passed + 1)); fi
done < "$tmp/checks"
[ "$total" -eq 7 ] || fail "FAIL the check block answered $total of 7 rows"

# The FAIL branch exits first on purpose: a trailing unconditional `exit` makes ShellCheck
# (0.11 SC2329) call the trap-only `cleanup` unreachable.
[ "$passed" -eq 7 ] || { echo "verify-deploy-stage: FAIL ($passed of 7)"; exit 1; }
echo "verify-deploy-stage: PASS (7 of 7)"
