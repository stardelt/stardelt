#!/usr/bin/env bash
# Prove the catalog accepts workload ServiceAccount tokens and refuses the other two
# audiences (stardelt/stardelt#492, #562; the audience split was measured in #538).
#
#   make verify-catalog-workload-auth
#
# #562's change is two chart values plus a ClusterRoleBinding, and nothing checked them on a
# running cluster: they are invisible until a workload tries to sign in. The dangerous half
# is the audience split. A bearer token whose audience is the right one authenticates; the
# OIDC audience `lakekeeper` on the same ServiceAccount token must stay 401, and so must the
# API server's own audience. An edit that drops `audience:` authenticates everything; one
# that drops `enabled:` is invisible until a workload fails.
#
# Five checks, in this order:
#   1. Secret `lakekeeper-config-envs` carries
#      LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION=true and
#      LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE=$AUDIENCE (the chart renders both into
#      that Secret; a plain Deployment env var would not be visible here).
#   2. ClusterRoleBinding `stardelt:lakekeeper-token-review` binds `system:auth-delegator`.
#      The chart creates it only inside `auth.k8s`, so a wrong value shows up exactly here.
#   3. A token with the workload audience answers `whoami` 404 `UserNotFound`: authentication
#      ran, the principal is not provisioned yet. A 401 means the workload login is off.
#   4. The OIDC audience `lakekeeper` on the same ServiceAccount is 401.
#   5. The API server's audience (no `--audience`) is 401.
#
# READ-ONLY IN THE CATALOG. This script NEVER calls `/catalog/v1/config` — that call
# *provisions* the principal (`last-updated-with: config-call-creation`, measured in #538),
# and a verify target that creates a user on every release run is not a check. `whoami`
# alone creates nothing: a second run of the same token stays 404. No `/management` write
# endpoint is called either.
#
# The only cluster writes are the throwaway ServiceAccount `$SA` and its short-lived tokens;
# the ServiceAccount is deleted on every exit path (a leftover SA is a leftover credential)
# and no token, header or Secret value is ever printed.
#
# Environment:
#   NS        namespace, default stardelt
#   PORT      local port for the port-forward, default 18086 (verify-airflow-runs uses 18081,
#             verify-airflow-actions 18080)
#   AUDIENCE  the workload audience, default stardelt-catalog-workload
#   SA        the throwaway ServiceAccount, default verify-catalog-workload
#
# Exit 0 = PASS, 1 = FAIL, 3 = UNVERIFIED (could not see the subject: no kubectl/curl/python3,
# the port in use, no Secret `lakekeeper-config-envs` = an install without a catalog, no
# port-forward).
set -euo pipefail

NS="${NS:-stardelt}"
PORT="${PORT:-18086}"
AUDIENCE="${AUDIENCE:-stardelt-catalog-workload}"
SA="${SA:-verify-catalog-workload}"

# The tokens live for the length of one run. 10m is the API server's minimum: a TokenRequest
# with expirationSeconds 300 is refused with "may not specify a duration less than 10 minutes"
# (measured 2026-11-20 on the lab, kubernetes TokenRequest validation). The ServiceAccount is
# deleted in the trap below, so the token is unusable as soon as this script exits.
TOKEN_DURATION=10m

# The OIDC audience the catalog is configured with (helm-values/lakekeeper.yaml,
# LAKEKEEPER__OPENID_AUDIENCE). A token with it must NOT authenticate as a workload.
OIDC_AUDIENCE=lakekeeper

CRB='stardelt:lakekeeper-token-review'
SECRET=lakekeeper-config-envs

unverified() { echo "UNVERIFIED ($*)"; exit 3; }
fail() { echo "verify-catalog-workload-auth: FAIL"; echo "FAIL ($*)"; exit 1; }
pass() { printf '✓ %s\n' "$*"; }

for cmd in kubectl curl python3; do
  command -v "$cmd" >/dev/null 2>&1 || unverified "$cmd not found"
done

umask 077  # the token header file is mode 0600
tmp="$(mktemp -d "${TMPDIR:-/tmp}/verify-catalog-workload-auth.XXXXXX")"
pf_pid=""
sa_created=no
# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  local code=0
  if [ "$sa_created" = yes ]; then
    kubectl -n "$NS" delete serviceaccount "$SA" --ignore-not-found >/dev/null 2>&1 || code=$?
    if [ "$code" -ne 0 ]; then
      echo "· could not delete ServiceAccount $SA/$NS — delete it by hand" >&2
    fi
  fi
  if [ -n "$pf_pid" ]; then
    kill "$pf_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM
rm -f "$tmp/hdr" "$tmp/body"

# ---------------------------------------------------------------------------
# 0. The port must be free. `kubectl port-forward` prints `address already in use` and dies
#    while anything else holds the port, and the checks below would then talk to that other
#    service. This path runs before any cluster call.
# ---------------------------------------------------------------------------
if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  unverified "port $PORT is already in use — stop the other forward or set PORT"
fi

# ---------------------------------------------------------------------------
# 1. The two chart values, as the catalog receives them. The values are decoded with
#    python3 and nothing but these two keys is printed. A missing Secret is not a failure
#    of the platform: an install without a catalog has nothing to check.
# ---------------------------------------------------------------------------
if ! secret="$(kubectl -n "$NS" get secret "$SECRET" -o json 2>&1)"; then
  case "$secret" in
    *NotFound*) unverified "no Secret $SECRET (an install without a catalog)" ;;
    *) unverified "cannot read Secret $SECRET: $secret" ;;
  esac
fi

value_of() { # key -> the decoded value, empty when absent
  printf '%s' "$secret" | python3 -c '
import base64, json, sys
data = json.load(sys.stdin).get("data") or {}
raw = data.get(sys.argv[1])
sys.stdout.write(base64.b64decode(raw).decode() if raw else "")
' "$1"
}

enabled="$(value_of LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION)" \
  || unverified "cannot decode Secret $SECRET"
audience="$(value_of LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE)" \
  || unverified "cannot decode Secret $SECRET"
unset secret

if [ "$enabled" != "true" ]; then
  fail "LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION=${enabled:-<unset>} (expected true) — the chart sets it from auth.k8s.enabled, so fix helm-values/lakekeeper.yaml and run make step-lakekeeper"
fi
if [ "$audience" != "$AUDIENCE" ]; then
  fail "LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE=${audience:-<unset>} (expected $AUDIENCE) — fix helm-values/lakekeeper.yaml and run make step-lakekeeper"
fi
pass "the catalog enables Kubernetes authentication (audience $AUDIENCE)"

# ---------------------------------------------------------------------------
# 2. The binding the chart creates inside auth.k8s. Without it the catalog cannot call
#    TokenReview, so every workload token would be 401 with the two values still right.
# ---------------------------------------------------------------------------
if ! role="$(kubectl get clusterrolebinding "$CRB" -o jsonpath='{.roleRef.name}' 2>&1)"; then
  case "$role" in
    *NotFound*) fail "ClusterRoleBinding $CRB is missing — the chart creates it only with auth.k8s enabled (make step-lakekeeper)" ;;
    *) unverified "cannot read ClusterRoleBinding $CRB: $role" ;;
  esac
fi
[ "$role" = "system:auth-delegator" ] \
  || fail "ClusterRoleBinding $CRB grants '${role:-<empty>}', expected system:auth-delegator"
pass "ClusterRoleBinding $CRB grants system:auth-delegator"

# ---------------------------------------------------------------------------
# 3. Port-forward the catalog. The API is only reachable in-cluster; the forward is killed on
#    every exit path and its stderr is kept so a forward that died says why. Any answer but a
#    connection error means the server is up (whoami without a token is a 401).
# ---------------------------------------------------------------------------
pf_log="$tmp/port-forward.log"
kubectl -n "$NS" port-forward svc/lakekeeper "$PORT":8181 >/dev/null 2>"$pf_log" &
pf_pid=$!

base="http://127.0.0.1:$PORT"
up=0
code=""
for _ in $(seq 1 20); do
  kill -0 "$pf_pid" 2>/dev/null || unverified "port-forward exited: $(tail -n 1 "$pf_log")"
  code="$(curl -s -o /dev/null -m 2 -w '%{http_code}' "$base/management/v1/whoami" 2>/dev/null)" || code="000"
  if [ "${code:-000}" != "000" ]; then up=1; break; fi
  sleep 0.5
done
[ "$up" -eq 1 ] || unverified "$base/management/v1/whoami did not answer within 10s"

# ---------------------------------------------------------------------------
# 4. A throwaway ServiceAccount and three tokens. The token travels in a mode-0600 header
#    file, never on the command line or in the output.
# ---------------------------------------------------------------------------
if ! kubectl -n "$NS" create serviceaccount "$SA" >/dev/null 2>"$tmp/sa.err"; then
  if ! grep -q AlreadyExists "$tmp/sa.err"; then
    unverified "cannot create ServiceAccount $SA: $(tail -n 1 "$tmp/sa.err")"
  fi
fi
sa_created=yes

# whoami with one token: prints the HTTP status, leaves the body in $tmp/body.
whoami() {
  printf 'Authorization: Bearer %s\n' "$1" > "$tmp/hdr"
  local got
  got="$(curl -s -o "$tmp/body" -m 10 -w '%{http_code}' -H @"$tmp/hdr" \
    "$base/management/v1/whoami" 2>/dev/null)" || got="000"
  rm -f "$tmp/hdr"
  printf '%s' "${got:-000}"
}

token_for() { # audience-or-empty -> a token, on stdout only
  if [ -n "$1" ]; then
    kubectl -n "$NS" create token "$SA" --audience="$1" --duration="$TOKEN_DURATION"
  else
    kubectl -n "$NS" create token "$SA" --duration="$TOKEN_DURATION"
  fi
}

# 4a. The workload audience authenticates: 404 UserNotFound, because whoami provisions nothing.
token="$(token_for "$AUDIENCE")" || unverified "cannot mint a token for ServiceAccount $SA"
[ -n "$token" ] || unverified "the API server minted an empty token for ServiceAccount $SA"
code="$(whoami "$token")"
unset token
case "$code" in
  404)
    grep -q 'UserNotFound' "$tmp/body" \
      || fail "a workload-audience ServiceAccount token answered 404 without UserNotFound in the body"
    pass "a workload-audience ServiceAccount token authenticates (whoami 404 UserNotFound before any grant)"
    ;;
  401)
    fail "a workload-audience ServiceAccount token answered 401 — 401 means the workload login is off (LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION or system:auth-delegator); run make step-lakekeeper"
    ;;
  000) unverified "the port-forward died while checking the workload audience" ;;
  *) fail "a workload-audience ServiceAccount token answered HTTP $code, expected 404 UserNotFound" ;;
esac

# 4b. The OIDC audience on a ServiceAccount token is not a workload audience.
token="$(token_for "$OIDC_AUDIENCE")" || unverified "cannot mint a token with audience $OIDC_AUDIENCE"
code="$(whoami "$token")"
unset token
[ "$code" = "401" ] \
  || fail "the OIDC audience $OIDC_AUDIENCE on a ServiceAccount token answered HTTP $code, expected 401"
pass "the OIDC audience $OIDC_AUDIENCE on a ServiceAccount token is refused (401)"

# 4c. The API server's own audience is not a workload audience either.
token="$(token_for '')" || unverified "cannot mint a token with the API server audience"
code="$(whoami "$token")"
unset token
[ "$code" = "401" ] \
  || fail "the API-server audience on a ServiceAccount token answered HTTP $code, expected 401"
pass "the API-server audience is refused (401)"

echo "verify-catalog-workload-auth: PASS"
exit 0
