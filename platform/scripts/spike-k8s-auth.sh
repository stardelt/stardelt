#!/usr/bin/env bash
# Spike for stardelt/stardelt#538 (#492 question 1): does a projected Kubernetes ServiceAccount
# token sign in to the catalog, which principal does it become, and does a role grant reach it?
#
# The design (#492) has workloads authenticate with Kubernetes ServiceAccount tokens and people
# with OIDC. This run measures the six facts every later slice depends on:
#
#   K0  before the flip: the OIDC client still signs in, and its principal id (`oidc~…`).
#   K1  the flip: `system:auth-delegator` for the catalog's ServiceAccount, plus
#       `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` and its audience, and how long the
#       single replica takes to come back. OIDC is unchanged after it (the same id as K0).
#   K2  a ServiceAccount token with the audience `stardelt-catalog-workload` signs in: its
#       principal id and name, and what an unprivileged principal may read of a warehouse.
#       Measured 2026-11-20: the first `whoami` answers 404 `UserNotFound` naming the id
#       `kubernetes~<uid>`; the first *config* call creates the user
#       (`last-updated-with: config-call-creation`), and every call after it answers 200.
#   K3  the tokens the catalog must refuse: the API server's audience, the OIDC audience
#       (`lakekeeper`), a deleted ServiceAccount — and an OIDC token stays an OIDC principal.
#   K4  a role grant reaches the new principal: a warehouse `describe` through a role. The
#       catalog's authorisation cache hides the grant for a few seconds (measured: 404 at once,
#       200 after 15 s), so the check polls; the line prints the wait.
#   K5  the principal's id form: what #492's access-sync change has to write.
#   K6  cleanup on every exit path, and OIDC signs in again afterwards.
#
# The only lab change is K1; `cleanup` undoes it on EXIT/INT/TERM (the ClusterRoleBinding, the
# two env vars and the namespace), and restores nothing that did not land. The run creates its
# own role (`spike-k8s-auth`, not an access-sync managed role) and deletes it again.
#
# It prints no token or secret: every credential travels through a mode-0600 file (`-H @file`),
# never through argv or stdout. Status codes, ids, names and numbers of seconds are the output;
# a credential in a log line is a defect.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
LK_PORT="${LK_PORT:-18181}"
AUDIENCE=stardelt-catalog-workload
OIDC_AUDIENCE=lakekeeper
SA_NS=stardelt-spike-k8s-auth
SA=spike-workload
GONE=spike-gone
CRB=spike-lakekeeper-tokenreview
ROLE=spike-k8s-auth

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/k8s-auth.XXXXXX")"
LK_PID=""
role_id=""
sa_id=""
oidc_id=""
flipped=no
crb_created=no
ns_created=no

# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  local code started
  if [ -n "$role_id" ] && [ -s "$tmp/auth" ]; then
    code="$(mgmt DELETE "/management/v1/role/$role_id")"
    echo "K6 delete-role=$code"
  fi
  if [ -n "$sa_id" ] && [ -s "$tmp/auth" ]; then
    code="$(mgmt DELETE "/management/v1/user/$sa_id")"
    echo "K6 delete-user=$code"
  fi
  if [ "$ns_created" = "yes" ]; then
    if kubectl delete namespace "$SA_NS" --wait=false >/dev/null 2>&1; then
      echo "K6 delete-namespace=ok"
    else
      echo "K6 delete-namespace=fail"
    fi
  fi
  if [ "$crb_created" = "yes" ]; then
    if kubectl delete clusterrolebinding "$CRB" >/dev/null 2>&1; then
      echo "K6 delete-clusterrolebinding=ok"
    else
      echo "K6 delete-clusterrolebinding=fail"
    fi
  fi
  if [ "$flipped" = "yes" ]; then
    if kubectl -n "$NS" set env deploy/lakekeeper \
        LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION- \
        LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE- >/dev/null 2>&1; then
      started="$(date +%s)"
      if kubectl -n "$NS" rollout status deploy/lakekeeper --timeout=180s >/dev/null 2>&1; then
        echo "K6 restore-env=ok rollout_s=$(( $(date +%s) - started ))"
      else
        echo "K6 restore-env=ok rollout=timeout"
      fi
    else
      echo "K6 restore-env=fail"
    fi
    forward_lk || true
    echo "K6 oidc whoami=$(mgmt GET /management/v1/whoami) id=$(jq -r '.id // "none"' "$tmp/out.json" 2>/dev/null || echo none)"
  fi
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

for tool in kubectl curl jq; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

# --- port-forward -------------------------------------------------------------------------
LK="http://127.0.0.1:$LK_PORT"

up() { # url -> 0 when anything but a transport failure answered
  local code
  code="$(curl -s -o /dev/null -m 2 -w '%{http_code}' "$1" 2>/dev/null)" || code="000"
  [ "${code:-000}" != "000" ]
}

forward_lk() {
  local _
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  kubectl -n "$NS" port-forward svc/lakekeeper "$LK_PORT:8181" >/dev/null 2>&1 &
  LK_PID=$!
  for _ in $(seq 1 30); do
    if up "$LK/management/v1/info"; then return 0; fi
    sleep 1
  done
  return 1
}

if ! forward_lk; then
  echo "lakekeeper did not answer within 30s" >&2
  exit 1
fi

# --- management auth ---------------------------------------------------------------------
bootstrap_auth() {
  local issuer body
  kubectl -n "$NS" get secret svc-catalog-bootstrap -o jsonpath='{.data.client-id}' | base64 -d > "$tmp/client-id"
  kubectl -n "$NS" get secret svc-catalog-bootstrap -o jsonpath='{.data.client-secret}' | base64 -d > "$tmp/client-secret"
  issuer="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}')"
  [ -n "$issuer" ] || { echo "ConfigMap stardelt-identity has no issuerUrl" >&2; exit 1; }
  if ! curl -fsS -X POST "${issuer%/}/protocol/openid-connect/token" \
      --data-urlencode grant_type=client_credentials \
      --data-urlencode client_id@"$tmp/client-id" \
      --data-urlencode client_secret@"$tmp/client-secret" \
      -o "$tmp/token.json" 2>/dev/null; then
    body="$(jq -r '.error // "no body"' "$tmp/token.json" 2>/dev/null || echo "no body")"
    echo "the realm's token endpoint refused svc-catalog-bootstrap: $body" >&2
    exit 1
  fi
  printf 'Authorization: Bearer %s\n' "$(jq -r .access_token "$tmp/token.json")" > "$tmp/auth"
}

mgmt() { # <method> <path> [data-file] -> status code, body in $tmp/out.json
  local method="$1" path="$2" data="${3-}" code refreshed=no reopened=no
  while :; do
    if [ -n "$data" ]; then
      code="$(curl -sS -H @"$tmp/auth" -H 'Content-Type: application/json' -X "$method" \
        -d @"$data" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    else
      code="$(curl -sS -H @"$tmp/auth" -X "$method" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    fi
    # Keycloak's client-credentials token lives 5 minutes; a long run must re-mint, not fail.
    if [ "$code" = "401" ] && [ "$refreshed" = "no" ]; then refreshed=yes; bootstrap_auth; continue; fi
    if [ "$code" = "000" ] && [ "$reopened" = "no" ]; then reopened=yes; forward_lk && continue; fi
    break
  done
  echo "${code:-000}"
}

# A call with any Bearer header file (the ServiceAccount tokens, and the OIDC token of K3c).
bearer() { # <method> <auth-file> <path> -> code, body in $tmp/out.json
  local method="$1" auth="$2" path="$3" code reopened=no
  while :; do
    code="$(curl -sS -H @"$auth" -X "$method" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    if [ "$code" = "000" ] && [ "$reopened" = "no" ]; then reopened=yes; forward_lk && continue; fi
    break
  done
  echo "${code:-000}"
}

# `kubectl create token` writes the token to stdout only; it never reaches argv of curl.
mint_sa() { # <serviceaccount> <audience|none> <out-file>
  if [ "$2" = "none" ]; then
    kubectl -n "$SA_NS" create token "$1" --duration 10m > "$3"
  else
    kubectl -n "$SA_NS" create token "$1" --audience "$2" --duration 10m > "$3"
  fi
  chmod 600 "$3"
  printf 'Authorization: Bearer %s\n' "$(tr -d '\n' < "$3")" > "$3.auth"
  chmod 600 "$3.auth"
}

# --- K0: before the flip -----------------------------------------------------------------
bootstrap_auth
code="$(mgmt GET /management/v1/whoami)"
oidc_id="$(jq -r '.id // "none"' "$tmp/out.json" 2>/dev/null || echo none)"
echo "K0 oidc whoami=$code id=$oidc_id"

# --- K1: the flip ------------------------------------------------------------------------
started="$(date +%s)"
if kubectl create clusterrolebinding "$CRB" \
    --clusterrole=system:auth-delegator \
    --serviceaccount="$NS:lakekeeper" >/dev/null; then
  crb_created=yes
fi
if kubectl -n "$NS" set env deploy/lakekeeper \
    LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION=true \
    LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE="$AUDIENCE" >/dev/null; then
  flipped=yes
fi
rollout=ok
if ! kubectl -n "$NS" rollout status deploy/lakekeeper --timeout=180s >/dev/null; then
  rollout=fail
fi
forward_lk || true
echo "K1 flip=$rollout rollout_s=$(( $(date +%s) - started ))"
if [ "$flipped" != "yes" ] || [ "$rollout" != "ok" ]; then
  echo "the catalog did not take the Kubernetes authentication settings; K2 cannot be measured" >&2
  exit 1
fi
code="$(mgmt GET /management/v1/whoami)"
echo "K1 oidc whoami=$code id=$(jq -r '.id // "none"' "$tmp/out.json" 2>/dev/null || echo none) (K0 id=$oidc_id)"

# --- K2: a ServiceAccount token signs in -------------------------------------------------
if kubectl create namespace "$SA_NS" >/dev/null; then ns_created=yes; fi
kubectl -n "$SA_NS" create serviceaccount "$SA" >/dev/null
mint_sa "$SA" "$AUDIENCE" "$tmp/sa-token"
code="$(bearer GET "$tmp/sa-token.auth" /management/v1/whoami)"
sa_id="$(jq -r '.id // "none"' "$tmp/out.json" 2>/dev/null || echo none)"
sa_name="$(jq -r '.name // "-"' "$tmp/out.json" 2>/dev/null || echo -)"
sa_error="$(jq -r '.error.type // empty' "$tmp/out.json" 2>/dev/null || true)"
echo "K2 sa whoami=$code id=$sa_id name=$sa_name${sa_error:+ error=$sa_error}"
if [ "$sa_id" = "none" ]; then
  # The catalog's own refusal names the principal it would have looked up.
  sa_id="$(sed -nE 's/.*User with id ([^ "]+) not found.*/\1/p' "$tmp/out.json" | head -n1)"
  echo "K2 sa principal-id=${sa_id:-none} (from whoami's UserNotFound)"
fi

code="$(bearer GET "$tmp/sa-token.auth" '/catalog/v1/config?warehouse=playground')"
echo "K2 sa config-before-grant=$code"

# The config call is the one that provisions the user (measured 2026-11-20), so the same
# token is now a known principal.
code="$(bearer GET "$tmp/sa-token.auth" /management/v1/whoami)"
if [ "$sa_id" = "none" ] || [ -z "$sa_id" ]; then
  sa_id="$(jq -r '.id // "none"' "$tmp/out.json" 2>/dev/null || echo none)"
fi
echo "K2 sa whoami-after-config=$code id=$(jq -r '.id // "none"' "$tmp/out.json" 2>/dev/null || echo none) name=$(jq -r '.name // "-"' "$tmp/out.json" 2>/dev/null || echo -) user-type=$(jq -r '."user-type" // "-"' "$tmp/out.json" 2>/dev/null || echo -)"
if [ "$sa_id" = "none" ] || [ -z "$sa_id" ]; then
  echo "the catalog did not name the ServiceAccount principal; K4's grant cannot be written" >&2
  exit 1
fi

# --- K3: tokens the catalog must refuse --------------------------------------------------
mint_sa "$SA" none "$tmp/api-token"
echo "K3a aud=api-server $(bearer GET "$tmp/api-token.auth" /management/v1/whoami)"

mint_sa "$SA" "$OIDC_AUDIENCE" "$tmp/oidc-aud-token"
echo "K3b aud=$OIDC_AUDIENCE $(bearer GET "$tmp/oidc-aud-token.auth" /management/v1/whoami)"

# The OIDC path must keep its own principal: an OIDC token is never read as a Kubernetes one.
code="$(mgmt GET /management/v1/whoami)"
echo "K3c oidc-token=$code id=$(jq -r '.id // "none"' "$tmp/out.json" 2>/dev/null || echo none)"

kubectl -n "$SA_NS" create serviceaccount "$GONE" >/dev/null
mint_sa "$GONE" "$AUDIENCE" "$tmp/gone-token"
kubectl -n "$SA_NS" delete serviceaccount "$GONE" >/dev/null
sleep 5
echo "K3d deleted-sa $(bearer GET "$tmp/gone-token.auth" /management/v1/whoami)"

# --- K4: a grant reaches the new principal ----------------------------------------------
# The shapes are access-sync's: a role, then an `assignee` write, then a warehouse grant. The
# role's description does not start with access-sync's `stardelt group ` prefix, so the sync
# will not touch it while this run holds it.
jq -n --arg n "$ROLE" '{name: $n, description: "spike #538"}' > "$tmp/role.json"
code="$(mgmt POST /management/v1/role "$tmp/role.json")"
role_id="$(jq -r '.id // empty' "$tmp/out.json" 2>/dev/null || true)"
echo "K4 role-create=$code id=$role_id"
if [ -z "$role_id" ]; then
  echo "the catalog did not create the role; K4's grant cannot be written" >&2
  exit 1
fi

jq -n --arg u "$sa_id" '{writes: [{type: "assignee", user: $u}], deletes: []}' > "$tmp/assignee.json"
echo "K4 role-assignee=$(mgmt POST "/management/v1/permissions/role/$role_id/assignments" "$tmp/assignee.json")"

code="$(mgmt GET /management/v1/warehouse)"
wid="$(jq -r --arg w playground '
  if type == "array" then .[] elif has("warehouses") then .warehouses[] else empty end
  | select(.name == $w) | .id' "$tmp/out.json" 2>/dev/null | head -n1)"
echo "K4 warehouse-id=$wid (http $code)"
if [ -z "$wid" ] || [ "$wid" = "null" ]; then
  echo "no warehouse named playground; K4's grant cannot be written" >&2
  exit 1
fi

jq -n --arg r "$role_id" '{writes: [{type: "describe", role: $r}], deletes: []}' > "$tmp/grant.json"
echo "K4 warehouse-grant=$(mgmt POST "/management/v1/permissions/warehouse/$wid/assignments" "$tmp/grant.json")"

# The grant is not visible at once: the catalog keeps an authorisation decision cache (measured
# 2026-11-20: the same call answered 404 immediately and 200 after 15 s). Poll instead of
# reporting the cache's first answer as the grant's.
started="$(date +%s)"
waited=0
code="$(bearer GET "$tmp/sa-token.auth" '/catalog/v1/config?warehouse=playground')"
first="$code"
while [ "$code" != "200" ] && [ "$waited" -lt 60 ]; do
  sleep 3
  waited=$(( waited + 3 ))
  code="$(bearer GET "$tmp/sa-token.auth" '/catalog/v1/config?warehouse=playground')"
done
echo "K4 sa config-after-grant=$code after=${waited}s (first=$first)"
# --- K5: the principal's id form ---------------------------------------------------------
form="$(printf '%s' "$sa_id" | sed -E 's/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/<uuid>/g')"
has_ns=no; has_name=no
case "$sa_id" in *"$SA_NS"*) has_ns=yes ;; esac
case "$sa_id" in *"$SA"*) has_name=yes ;; esac
uid_only=no
[ "$has_ns$has_name" = "nono" ] && uid_only=yes
echo "K5 id-form=$form contains-namespace=$has_ns contains-name=$has_name uid-only=$uid_only"

# --- K6 runs from `cleanup` on EXIT ------------------------------------------------------
exit 0
