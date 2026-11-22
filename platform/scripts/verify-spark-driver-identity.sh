#!/usr/bin/env bash
# Prove the live driver-identity chain of every group-owned SparkCluster (#493, #1100, #1101, #1102).
#
#   make verify-spark-driver-identity
#
# #1100 gives a SparkCluster whose `spec.owner.group` is set a driver ServiceAccount
# `spark-cluster-driver-<group>`, bound to the shared `spark-cluster-driver` Role; #1101 adds
# `kubernetes~<ServiceAccount uid>` to the owner group's catalog role; #1102 records both in
# `status.driverServiceAccount` and `status.workloadIdentity`. Nothing checked the chain, and a
# link that breaks changes no condition — a Spark job is simply refused. Four kinds of breakage
# are invisible until then:
#   - the ServiceAccount is missing (an operator that was not rolled, or a deleted SA);
#   - the ServiceAccount is not bound to the shared `spark-cluster-driver` Role, so the driver
#     cannot create its executors;
#   - `status.driverServiceAccount` / `status.workloadIdentity` name another identity (an
#     operator older than #1102, or a status written by a different ServiceAccount), so the
#     catalog principal and the ServiceAccount disagree;
#   - the catalog refuses the workload audience for that ServiceAccount.
#
# READ-ONLY IN THE CATALOG. This script NEVER calls `/catalog/v1/config` — that call
# *provisions* the principal (`last-updated-with: config-call-creation`, #538) — and no
# `/management` write endpoint. `whoami` alone creates nothing. The only cluster writes are the
# short-lived ServiceAccount tokens of step 5, in the script's own `tmp` directory; no
# ServiceAccount, SparkCluster or Secret is created, changed or deleted, and no token, header
# or Secret value is ever printed.
#
# Environment:
#   NS        namespace, default stardelt
#   PORT      local port for the port-forward, default 18088 (verify-workload-identity uses
#             18087, verify-catalog-workload-auth 18086, verify-airflow-runs 18081,
#             verify-airflow-actions 18080)
#   AUDIENCE  the workload audience, default stardelt-catalog-workload
#
# Exit 0 = PASS, 1 = FAIL, 3 = UNVERIFIED (could not see the subject: no kubectl/curl/python3,
# the port in use, no port-forward, no group-owned SparkCluster in $NS).
set -euo pipefail

NS="${NS:-stardelt}"
PORT="${PORT:-18088}"
AUDIENCE="${AUDIENCE:-stardelt-catalog-workload}"

TOKEN_DURATION=10m

unverified() { echo "UNVERIFIED ($*)"; exit 3; }

failures=0
fail() {
  failures=$((failures + 1))
  printf 'FAIL (%s)\n' "$*"
}
pass() { printf '✓ %s\n' "$*"; }

for cmd in kubectl curl python3; do
  command -v "$cmd" >/dev/null 2>&1 || unverified "$cmd not found"
done

umask 077  # the token and header files are mode 0600
tmp="$(mktemp -d "${TMPDIR:-/tmp}/verify-spark-driver-identity.XXXXXX")"
pf_pid=""
# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  if [ -n "$pf_pid" ]; then
    kill "$pf_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# 0. The port must be free. `kubectl port-forward` prints `address already in use` and dies
#    while anything else holds the port, and the checks below would then talk to that other
#    service. This path runs before any cluster call.
# ---------------------------------------------------------------------------
if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  unverified "port $PORT is already in use — stop the other forward or set PORT"
fi

# ---------------------------------------------------------------------------
# 1. Every group-owned SparkCluster, read-only. Nothing but name, the owner group and the two
#    status fields is taken from the object, so no Secret reference reaches a file.
# ---------------------------------------------------------------------------
if ! clusters="$(kubectl -n "$NS" get sparkclusters -o json 2>"$tmp/sc.err")"; then
  unverified "cannot list SparkClusters in $NS: $(tail -n 1 "$tmp/sc.err")"
fi
rows="$(printf '%s' "$clusters" | python3 -c '
import json, sys
for item in (json.load(sys.stdin).get("items") or []):
    meta = item.get("metadata") or {}
    spec = item.get("spec") or {}
    status = item.get("status") or {}
    group = ((spec.get("owner") or {}).get("group")) or ""
    if not group:
        continue
    print("|".join((
        meta.get("name") or "",
        group,
        status.get("driverServiceAccount") or "",
        status.get("workloadIdentity") or "",
    )))
')"
unset clusters

[ -n "$rows" ] || unverified "no group-owned SparkCluster in $NS"

# ---------------------------------------------------------------------------
# 2. Port-forward the catalog once, before the loop, so a forward that cannot start is
#    UNVERIFIED rather than a FAIL against every cluster. The forward is killed in the trap;
#    its stderr is kept so a forward that died says why.
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

# whoami with one token: prints the HTTP status, leaves the body in $tmp/body.
whoami() { # token -> status on stdout only
  printf 'Authorization: Bearer %s\n' "$1" > "$tmp/hdr"
  local got
  got="$(curl -s -o "$tmp/body" -m 10 -w '%{http_code}' -H @"$tmp/hdr" \
    "$base/management/v1/whoami" 2>/dev/null)" || got="000"
  rm -f "$tmp/hdr"
  printf '%s' "${got:-000}"
}

# ---------------------------------------------------------------------------
# 3. For every cluster: the driver ServiceAccount `spark-cluster-driver-<group>` exists and is
#    bound to the shared `spark-cluster-driver` Role; the status names that ServiceAccount and
#    its uid (#1102); the catalog accepts its projected audience (#1101, #538).
# ---------------------------------------------------------------------------
total=0
while IFS='|' read -r name group driver workload; do
  [ -n "$name" ] || continue
  total=$((total + 1))
  sa="spark-cluster-driver-$group"

  if ! sa_json="$(kubectl -n "$NS" get sa "$sa" -o json 2>"$tmp/sa.err")"; then
    if grep -q NotFound "$tmp/sa.err"; then
      fail "$name: ServiceAccount $sa is missing — roll the operator (#1100)"
    else
      unverified "cannot read ServiceAccount $sa: $(tail -n 1 "$tmp/sa.err")"
    fi
    continue
  fi
  pass "$name: ServiceAccount $sa exists"

  role="$(kubectl -n "$NS" get rolebinding "$sa" -o jsonpath='{.roleRef.name}' 2>/dev/null || true)"
  if [ "$role" = spark-cluster-driver ]; then
    pass "$name: $sa is bound to the shared spark-cluster-driver Role"
  else
    fail "$name: the RoleBinding of $sa names '${role:-nothing}' (want spark-cluster-driver) — the driver cannot create its executors"
  fi

  if [ "$driver" = "$sa" ]; then
    pass "$name: status.driverServiceAccount is $sa"
  else
    fail "$name: status.driverServiceAccount is '${driver:-<unset>}' (want $sa) — an operator older than #1102, or a status that names another identity"
  fi

  sa_uid="$(printf '%s' "$sa_json" | python3 -c '
import json, sys
sys.stdout.write(str((json.load(sys.stdin).get("metadata") or {}).get("uid") or ""))
')"
  [ -n "$sa_uid" ] || unverified "ServiceAccount $sa has no uid"
  want="kubernetes~$sa_uid"
  if [ "$workload" = "$want" ]; then
    pass "$name: status.workloadIdentity is ${want:0:19}…"
  else
    fail "$name: status.workloadIdentity is '${workload:-<unset>}' (want $want) — the status and the ServiceAccount disagree"
  fi

  token_file="$tmp/$name.token"
  if ! kubectl -n "$NS" create token "$sa" --audience="$AUDIENCE" \
      --duration="$TOKEN_DURATION" >"$token_file" 2>"$tmp/token.err"; then
    fail "$name: cannot mint a token for ServiceAccount $sa: $(tail -n 1 "$tmp/token.err")"
    continue
  fi
  token="$(cat "$token_file")"
  code="$(whoami "$token")"
  unset token
  case "$code" in
    200) pass "$name: the catalog accepts the driver's projected audience" ;;
    404) if grep -q 'UserNotFound' "$tmp/body"; then
           pass "$name: the driver's token authenticates (the principal is not provisioned yet)"
         else
           fail "$name: whoami answered 404 without UserNotFound in the body"
         fi ;;
    401) fail "$name: the catalog refuses the driver's ServiceAccount token (HTTP 401) — the workload audience is off" ;;
    000) fail "$name: no answer from the catalog (the port-forward died)" ;;
    *)   fail "$name: the catalog answered HTTP $code for the driver's ServiceAccount token" ;;
  esac
  rm -f "$token_file"
done <<<"$rows"

# ---------------------------------------------------------------------------
# 4. Result.
# ---------------------------------------------------------------------------
if [ "$failures" -ne 0 ]; then
  echo "verify-spark-driver-identity: FAIL ($failures check(s) failed, $total group-owned SparkCluster(s))"
  exit 1
fi
echo "verify-spark-driver-identity: PASS ($total group-owned SparkCluster(s))"
exit 0
