#!/usr/bin/env bash
# The `journey` phase of the fresh-node run (#1806): the checks a person still does by hand in
# `person_notes()`.
#
#   scripts/journey-fresh-node.sh
#   JOURNEY_WHEEL=/path/to/stardelt-0.0.4-py3-none-any.whl scripts/journey-fresh-node.sh
#
# J1 identity: a `ServicePrincipal` comes Ready, its client-credentials token is accepted by
#   Trino, and `select current_user` answers the principal's `status.subject` — a machine
#   identity works on this install. The principal is named `journey` in the group every realm
#   has (`stardelt-users`); it is deleted on exit unless KEEP=1.
#   Nova is not used: a ServicePrincipal's token carries `aud=trino,lakekeeper`, and Nova
#   refuses it with `401 invalid token: wrong audience` (measured 2026-11-26).
# J2 client: the release's own wheel installs into a throwaway venv and `stardelt --version`
#   equals the release. Needs a wheel: `JOURNEY_WHEEL`, or `RELEASE_DIR` whose `versions.yaml`
#   names the version (the wheel is then that version's GitHub release asset).
#
# Exit codes: 0 everything passed, 1 a check failed, 3 nothing could be checked (no kubectl,
# no identity provider, no wheel). Needs: kubectl, curl, python3 (venv).
set -euo pipefail

NAMESPACE="${NAMESPACE:-stardelt}"
PLATFORM_INSTANCE="${PLATFORM_INSTANCE:-cluster}"
PLATFORM_DIR="${PLATFORM_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PRINCIPAL="${PRINCIPAL:-journey}"
GROUP="${GROUP:-stardelt-users}"
KEEP="${KEEP:-0}"

status=0
ok() { printf '✓ %s %s\n' "$1" "$2"; }
bad() { printf '✗ %s %s\n' "$1" "$2" >&2; status=1; }
# `?` means "cannot be checked": it only turns a clean run into UNVERIFIED, never hides a FAIL.
unknown() {
  printf '? %s %s\n' "$1" "$2" >&2
  if [ "$status" -eq 0 ]; then
    status=3
  fi
}

tmp="$(mktemp -d)"
chmod 700 "$tmp"
forward_pid=""
cleanup() {
  if [ -n "$forward_pid" ] && kill -0 "$forward_pid" 2>/dev/null; then
    kill "$forward_pid" 2>/dev/null || true
  fi
  if [ "$KEEP" != 1 ] && command -v kubectl >/dev/null 2>&1; then
    kubectl -n "$NAMESPACE" delete "serviceprincipal/${PRINCIPAL}" --ignore-not-found >/dev/null 2>&1 || true
    make -C "$PLATFORM_DIR" delete-principal NAME="$PRINCIPAL" YES=1 >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT

# --- J1: a machine identity ---------------------------------------------------------------
phase_identity() {
  local endpoint trino_url subject secret client_id_file secret_file token_file rows
  if ! command -v kubectl >/dev/null 2>&1 || ! command -v curl >/dev/null 2>&1; then
    unknown identity '"kubectl and curl are needed"'
    return 0
  fi
  endpoint="$(kubectl -n "$NAMESPACE" get configmap stardelt-identity -o jsonpath='{.data.tokenEndpoint}' 2>/dev/null || true)"
  trino_url="$(kubectl -n "$NAMESPACE" get configmap stardelt-identity -o jsonpath='{.data.trinoUrl}' 2>/dev/null || true)"
  if [ -z "$endpoint" ]; then
    unknown identity '"this instance has no identity provider (ConfigMap stardelt-identity has no tokenEndpoint)"'
    return 0
  fi

  if ! kubectl -n "$NAMESPACE" apply -f - >/dev/null 2>"$tmp/apply.err" <<EOF
apiVersion: stardelt.io/v1alpha1
kind: ServicePrincipal
metadata:
  name: ${PRINCIPAL}
spec:
  ownerGroup: ${GROUP}
EOF
  then
    bad identity "\"the ServicePrincipal was refused: $(head -c 200 "$tmp/apply.err")\""
    return 0
  fi
  # A ServicePrincipal is not self-provisioning: the Keycloak client and its Secret are created
  # by `make sync-principal` (operator/crates/stardelt-operator/src/principal.rs:153), and the
  # operator then converges the CR onto them.
  if ! make -C "$PLATFORM_DIR" sync-principal NAME="$PRINCIPAL" GROUP="$GROUP" >"$tmp/sync.log" 2>&1; then
    bad identity "\"make sync-principal failed: $(tail -c 300 "$tmp/sync.log")\""
    return 0
  fi
  if ! kubectl -n "$NAMESPACE" wait --for=condition=Ready "serviceprincipal/${PRINCIPAL}" --timeout=120s >/dev/null 2>&1; then
    bad identity "\"serviceprincipal/${PRINCIPAL} is not Ready: $(kubectl -n "$NAMESPACE" get "serviceprincipal/${PRINCIPAL}" -o jsonpath='{.status.conditions[0].message}' 2>/dev/null)\""
    return 0
  fi
  subject="$(kubectl -n "$NAMESPACE" get "serviceprincipal/${PRINCIPAL}" -o jsonpath='{.status.subject}' 2>/dev/null || true)"
  secret="$(kubectl -n "$NAMESPACE" get "serviceprincipal/${PRINCIPAL}" -o jsonpath='{.status.secretName}' 2>/dev/null || true)"
  if [ -z "$subject" ] || [ -z "$secret" ]; then
    bad identity '"the principal is Ready but carries no subject or secretName"'
    return 0
  fi

  client_id_file="$tmp/client-id"
  secret_file="$tmp/client-secret"
  token_file="$tmp/token.json"
  kubectl -n "$NAMESPACE" get "secret/${secret}" -o jsonpath='{.data.client-id}' | base64 -d >"$client_id_file"
  kubectl -n "$NAMESPACE" get "secret/${secret}" -o jsonpath='{.data.client-secret}' | base64 -d >"$secret_file"
  chmod 600 "$client_id_file" "$secret_file"
  if ! curl -sS -o "$token_file" --max-time 20 -X POST "$endpoint" \
    --data-urlencode "grant_type=client_credentials" \
    --data-urlencode "client_id@${client_id_file}" \
    --data-urlencode "client_secret@${secret_file}"; then
    bad identity '"the token endpoint did not answer"'
    return 0
  fi
  python3 - "$token_file" >"$tmp/access-token" <<'PY'
import json, sys
answer = json.load(open(sys.argv[1]))
if "access_token" not in answer:
    raise SystemExit("no access_token: %s" % answer.get("error", "no answer"))
sys.stdout.write(answer["access_token"])
PY

  kubectl -n "$NAMESPACE" port-forward svc/trino 18443:8443 >/dev/null 2>&1 &
  forward_pid=$!
  local attempt=0
  while [ "$attempt" -lt 30 ]; do
    if curl -sk --max-time 3 -o /dev/null https://127.0.0.1:18443/v1/info; then
      break
    fi
    if ! kill -0 "$forward_pid" 2>/dev/null; then
      bad identity '"port-forward to svc/trino exited immediately"'
      return 0
    fi
    sleep 0.5
    attempt=$((attempt + 1))
  done

  if ! rows="$(python3 - "$tmp/access-token" <<'PY'
import json, ssl, sys, time, urllib.request

token = open(sys.argv[1]).read().strip()
context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE


def call(url, body=None):
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Authorization": "Bearer " + token, "Content-Type": "text/plain"},
    )
    with urllib.request.urlopen(request, context=context, timeout=20) as answer:
        return json.load(answer)


state = call("https://127.0.0.1:18443/v1/statement", b"select current_user")
for _ in range(60):
    if state.get("data"):
        print(state["data"][0][0])
        break
    if state.get("error"):
        raise SystemExit("the engine refused: %s" % state["error"].get("message"))
    next_uri = state.get("nextUri")
    if not next_uri:
        raise SystemExit("no rows and no nextUri")
    time.sleep(0.3)
    state = call(next_uri)
else:
    raise SystemExit("the statement did not finish")
PY
  )"; then
    bad identity '"the Trino statement did not answer"'
    return 0
  fi

  if [ "$rows" = "$subject" ]; then
    ok identity "ServicePrincipal ${PRINCIPAL} reached Trino as its own subject"
  else
    bad identity "\"select current_user answered ${rows}, not the principal's subject ${subject}\""
  fi
}

# --- J2: the released client --------------------------------------------------------------
phase_client() {
  local version wheel venv got
  if [ -n "${JOURNEY_WHEEL:-}" ]; then
    wheel="$JOURNEY_WHEEL"
    version="$(basename "$wheel" | sed -n 's/^stardelt-\([0-9][0-9.]*\)-py3-none-any\.whl$/\1/p')"
  elif [ -n "${RELEASE_DIR:-}" ] && [ -f "${RELEASE_DIR}/versions.yaml" ]; then
    version="$(sed -n 's/^  stardelt: \([0-9][0-9.]*\)$/\1/p' "${RELEASE_DIR}/versions.yaml" | head -1)"
    wheel="https://github.com/stardelt/stardelt/releases/download/v${version}/stardelt-${version}-py3-none-any.whl"
  else
    unknown client '"no release wheel: pass JOURNEY_WHEEL, or RELEASE_DIR with versions.yaml"'
    return 0
  fi
  if [ -z "$version" ] || [ -z "$wheel" ]; then
    unknown client '"the release version could not be read from versions.yaml"'
    return 0
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    unknown client '"python3 is needed for the wheel check"'
    return 0
  fi
  venv="$tmp/venv"
  if ! python3 -m venv "$venv" >/dev/null 2>&1; then
    unknown client '"python3 -m venv is not available"'
    return 0
  fi
  if ! "$venv/bin/pip" install --quiet --disable-pip-version-check "$wheel" >"$tmp/pip.log" 2>&1; then
    bad client "\"pip install ${wheel} failed: $(tail -c 200 "$tmp/pip.log")\""
    return 0
  fi
  got="$("$venv/bin/stardelt" --version 2>&1 || true)"
  if [ "$got" = "stardelt ${version}" ]; then
    ok client "$got"
  else
    bad client "\"stardelt --version answered '${got}', not 'stardelt ${version}'\""
  fi
}

# --- main ---------------------------------------------------------------------------------
case "${1:-all}" in
  all | identity)
    phase_identity
    [ "${1:-all}" = identity ] || phase_client
    ;;
  client) phase_client ;;
  *)
    echo "journey-fresh-node: unknown phase '$1' (all, identity, client)" >&2
    exit 2
    ;;
esac

echo
case "$status" in
  0) echo "journey-fresh-node: PASS" ;;
  1) echo "journey-fresh-node: FAIL (a check above is short)" >&2 ;;
  *) echo "journey-fresh-node: UNVERIFIED (a check could not be made)" >&2 ;;
esac
exit "$status"
