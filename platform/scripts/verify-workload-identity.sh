#!/usr/bin/env bash
# Prove the live workload-identity chain of every ServicePrincipal (#492, #650, #684).
#
#   make verify-workload-identity
#
# #650 gives every ServicePrincipal a ServiceAccount `sp-<name>` and records
# `status.workloadIdentity: kubernetes~<ServiceAccount uid>`; #684 adds that id to the owner
# group's catalog role. Nothing checked the chain, and #650's own rule — "a failure here must
# not change `ready`" — means a broken ServiceAccount surfaces in no condition either. Four
# kinds of breakage are invisible until a run fails:
#   - the ServiceAccount is missing (an operator that was not rolled, or a deleted SA);
#   - the SA automounts its token, so a workload gets a credential nobody asked for;
#   - `status.workloadIdentity` is not the SA's uid (an old operator, or a status written by
#     a different SA), so the catalog principal and the SA disagree;
#   - the catalog refuses the workload audience for that SA.
#
# READ-ONLY IN THE CATALOG. This script NEVER calls `/catalog/v1/config` — that call
# *provisions* the principal (`last-updated-with: config-call-creation`, #538) — and no
# `/management` write endpoint. `whoami` alone creates nothing. The only cluster writes are
# the short-lived ServiceAccount tokens of step 4, in the script's own `tmp` directory; no
# ServiceAccount, ServicePrincipal or Secret is created, changed or deleted, and no token,
# header or Secret value is ever printed.
#
# Environment:
#   NS        namespace, default stardelt
#   PORT      local port for the port-forward, default 18087 (verify-catalog-workload-auth
#             uses 18086, verify-airflow-runs 18081, verify-airflow-actions 18080)
#   AUDIENCE  the workload audience, default stardelt-catalog-workload
#
# Exit 0 = PASS, 1 = FAIL, 3 = UNVERIFIED (could not see the subject: no kubectl/curl/python3,
# the port in use, no port-forward, no ServicePrincipal in $NS).
set -euo pipefail

NS="${NS:-stardelt}"
PORT="${PORT:-18087}"
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
tmp="$(mktemp -d "${TMPDIR:-/tmp}/verify-workload-identity.XXXXXX")"
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
# 1. Every ServicePrincipal, read-only. Nothing but name, uid, status.workloadIdentity and
#    spec.ownerGroup is taken from the object, so a Secret reference never reaches a file.
# ---------------------------------------------------------------------------
if ! principals="$(kubectl -n "$NS" get serviceprincipals -o json 2>&1)"; then
  unverified "cannot read ServicePrincipals in $NS: $principals"
fi
if ! rows="$(printf '%s' "$principals" | python3 -c '
import json, sys
items = json.load(sys.stdin).get("items") or []
for item in items:
    meta = item.get("metadata") or {}
    status = item.get("status") or {}
    spec = item.get("spec") or {}
    print("|".join((
        meta.get("name") or "",
        meta.get("uid") or "",
        status.get("workloadIdentity") or "",
        spec.get("ownerGroup") or "",
    )))
')"; then
  unverified "cannot parse the ServicePrincipal list in $NS"
fi
unset principals

if [ -z "$rows" ]; then
  unverified "no ServicePrincipal in $NS"
fi

# ---------------------------------------------------------------------------
# 2. Port-forward the catalog once, before the loop, so a forward that cannot start is
#    UNVERIFIED rather than a FAIL against every principal. The forward is killed in the
#    trap; its stderr is kept so a forward that died says why.
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
# 3. For every principal: ServiceAccount `sp-<name>` exists, mounts no token, is owned by the
#    principal, and status.workloadIdentity is its uid.
# ---------------------------------------------------------------------------
total=0
checked=""
# shellcheck disable=SC2034  # ownerGroup is read so step 2 prints the whole chain; the ticket asserts nothing on it
while IFS='|' read -r name uid workload owner_group; do
  [ -n "$name" ] || continue
  total=$((total + 1))
  sa="sp-$name"

  if ! sa_json="$(kubectl -n "$NS" get sa "$sa" -o json 2>"$tmp/sa.err")"; then
    if grep -q NotFound "$tmp/sa.err"; then
      fail "$name: ServiceAccount $sa is missing — roll the operator"
    else
      unverified "cannot read ServiceAccount $sa: $(tail -n 1 "$tmp/sa.err")"
    fi
    continue
  fi

  if ! problem="$(printf '%s' "$sa_json" | python3 -c '
import json, sys
name, uid = sys.argv[1], sys.argv[2]
doc = json.load(sys.stdin)
meta = doc.get("metadata") or {}
if doc.get("automountServiceAccountToken") is not False:
    print("ServiceAccount sp-%s automounts its token (automountServiceAccountToken=%s) — the API server mounts a token into every pod that uses it; set it to false"
          % (name, json.dumps(doc.get("automountServiceAccountToken"))))
    raise SystemExit(0)
if (meta.get("labels") or {}).get("stardelt.io/service-principal") != name:
    print("ServiceAccount sp-%s is not labelled stardelt.io/service-principal=%s (got %s)"
          % (name, name, json.dumps((meta.get("labels") or {}).get("stardelt.io/service-principal"))))
    raise SystemExit(0)
owners = [
    ref for ref in (meta.get("ownerReferences") or [])
    if ref.get("kind") == "ServicePrincipal" and ref.get("name") == name
]
if not owners:
    print("ServiceAccount sp-%s has no ownerReference to ServicePrincipal %s" % (name, name))
    raise SystemExit(0)
owner = owners[0]
if owner.get("controller") is not True:
    print("ServiceAccount sp-%s ownerReference to ServicePrincipal %s is not the controller" % (name, name))
    raise SystemExit(0)
if owner.get("uid") != uid:
    print("ServiceAccount sp-%s is owned by ServicePrincipal uid %s, not %s — a deleted and recreated principal owns this ServiceAccount"
          % (name, json.dumps(owner.get("uid")), uid))
    raise SystemExit(0)
' "$name" "$uid")"; then
    unverified "cannot inspect ServiceAccount $sa"
  fi
  if [ -n "$problem" ]; then
    fail "$name: $problem"
    continue
  fi

  if ! sa_uid="$(printf '%s' "$sa_json" | python3 -c '
import json, sys
sys.stdout.write(str((json.load(sys.stdin).get("metadata") or {}).get("uid") or ""))
')"; then
    unverified "cannot read the uid of ServiceAccount $sa"
  fi
  [ -n "$sa_uid" ] || unverified "ServiceAccount $sa has no uid"

  want="kubernetes~$sa_uid"
  if [ "$workload" != "$want" ]; then
    fail "$name: status.workloadIdentity=${workload:-<unset>} does not match the ServiceAccount uid ($want) — an old operator, or a status written by a different ServiceAccount"
    continue
  fi
  pass "$name: ${want:0:19}… matches the ServiceAccount"

  checked="$checked$name"$'\n'
done <<<"$rows"

# ---------------------------------------------------------------------------
# 4. The catalog accepts each principal's token. The one dangerous outcome is a 401, which
#    means the workload login is off; a 404 UserNotFound means authentication ran and the
#    principal is not provisioned yet (the grant is exercised elsewhere). No write endpoint
#    is called, so a release check never creates a user.
# ---------------------------------------------------------------------------
if [ -n "$checked" ]; then
  while IFS= read -r name; do
    [ -n "$name" ] || continue
    sa="sp-$name"
    token_file="$tmp/$name.token"

    if ! kubectl -n "$NS" create token "$sa" --audience="$AUDIENCE" \
      --duration="$TOKEN_DURATION" >"$token_file" 2>"$tmp/token.err"; then
      fail "$name: cannot mint a token for ServiceAccount $sa: $(tail -n 1 "$tmp/token.err")"
      continue
    fi
    if [ ! -s "$token_file" ]; then
      fail "$name: the API server minted an empty token for ServiceAccount $sa"
      continue
    fi
    token="$(cat "$token_file")"
    code="$(whoami "$token")"
    unset token
    case "$code" in
      200)
        pass "$name: the workload token signs in"
        ;;
      404)
        if grep -q 'UserNotFound' "$tmp/body"; then
          pass "$name: the workload token authenticates (the principal is not provisioned yet)"
        else
          fail "$name: the workload token answered 404 without UserNotFound in the body"
        fi
        ;;
      401)
        fail "$name: the catalog refuses its ServiceAccount token (HTTP 401)"
        ;;
      000)
        fail "$name: no answer from the catalog (the port-forward died)"
        ;;
      *)
        fail "$name: the catalog answered HTTP $code for its ServiceAccount token"
        ;;
    esac
    rm -f "$token_file"
  done <<<"$checked"
fi

# ---------------------------------------------------------------------------
# 5. Result.
# ---------------------------------------------------------------------------
if [ "$failures" -ne 0 ]; then
  echo "verify-workload-identity: FAIL ($failures check(s) failed, $total ServicePrincipal(s))"
  exit 1
fi
echo "verify-workload-identity: PASS ($total ServicePrincipal(s))"
exit 0
