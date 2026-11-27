#!/usr/bin/env bash
# The `journey` phase of the fresh-node run (#1806): the checks a person still does by hand in
# `person_notes()`.
#
#   scripts/journey-fresh-node.sh
#   JOURNEY_WHEEL=/path/to/stardelt-0.0.4-py3-none-any.whl scripts/journey-fresh-node.sh
#
# J1 identity: the operator creates a `ServicePrincipal`'s client and Secret; it comes Ready,
#   its client-credentials token is accepted by Trino, and `select current_user` answers the
#   principal's `status.subject` — a machine identity works on this install. The principal is
#   named `journey` in the group every realm has (`stardelt-users`); it is deleted on exit
#   unless KEEP=1.
#   Nova is not used: a ServicePrincipal's token carries `aud=trino,lakekeeper`, and Nova
#   refuses it with `401 invalid token: wrong audience` (measured 2026-11-26).
# J2 client: the release's own wheel installs into a throwaway venv and `stardelt --version`
#   equals the release. Needs a wheel: `JOURNEY_WHEEL`, or `RELEASE_DIR` whose `versions.yaml`
#   names the version (the wheel is then that version's GitHub release asset).
# J3 write: the run applies `Catalog/journey`, writes three rows through Trino and deletes the
#   catalog on exit.
# X1 exit-read: the three rows are read back with plain pyiceberg, straight from the S3 gateway —
#   no catalog, no stardelt component in the path (#1806 X1).
# J4 refusal: a second client (`svc-journey-outsider`, no group) is refused
#   `journey.e2e.exit_test`; it is deleted on exit.
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
s3_forward_pid=""
# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  if [ -n "$forward_pid" ] && kill -0 "$forward_pid" 2>/dev/null; then
    kill "$forward_pid" 2>/dev/null || true
  fi
  if [ -n "$s3_forward_pid" ] && kill -0 "$s3_forward_pid" 2>/dev/null; then
    kill "$s3_forward_pid" 2>/dev/null || true
  fi
  if [ "$KEEP" != 1 ] && command -v kubectl >/dev/null 2>&1; then
    kubectl -n "$NAMESPACE" delete "serviceprincipal/${PRINCIPAL}" --ignore-not-found >/dev/null 2>&1 || true
    kubectl -n "$NAMESPACE" delete "serviceprincipal/pipeline-${GROUP}" --ignore-not-found >/dev/null 2>&1 || true
    kubectl -n "$NAMESPACE" delete catalog/journey --ignore-not-found >/dev/null 2>&1 || true
    NAMESPACE="$NAMESPACE" KIND=service NAME="journey-outsider" YES=1 \
      bash "$PLATFORM_DIR/scripts/keycloak-sync-principal.sh" --delete >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT

# trino_sql <token-file> <sql>: run one statement with that token and print the first column
# of the first row. Needs phase_identity's port-forward. Non-zero on an error.
trino_sql() {
  python3 - "$1" "$2" <<'PY'
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


state = call("https://127.0.0.1:18443/v1/statement", sys.argv[2].encode())
for _ in range(60):
    if state.get("data"):
        print(state["data"][0][0])
        break
    if state.get("error"):
        raise SystemExit("the engine refused: %s" % state["error"].get("message"))
    next_uri = state.get("nextUri")
    if not next_uri:
        if (state.get("stats") or {}).get("state") == "FINISHED":
            break  # DDL/DML answer no rows: nothing to print, not an error
        raise SystemExit("no rows and no nextUri")
    time.sleep(0.3)
    state = call(next_uri)
else:
    raise SystemExit("the statement did not finish")
PY
}

# --- J1: a machine identity ---------------------------------------------------------------
phase_identity() {
  local endpoint subject secret client_id_file secret_file token_file rows
  if ! command -v kubectl >/dev/null 2>&1 || ! command -v curl >/dev/null 2>&1; then
    unknown identity '"kubectl and curl are needed"'
    return 0
  fi
  endpoint="$(kubectl -n "$NAMESPACE" get configmap stardelt-identity -o jsonpath='{.data.tokenEndpoint}' 2>/dev/null || true)"
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

  if ! rows="$(trino_sql "$tmp/access-token" 'select current_user')"; then
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

# --- J3: a write --------------------------------------------------------------------------
phase_write() {
  local schema table rows
  if ! command -v kubectl >/dev/null 2>&1 || [ ! -s "$tmp/access-token" ]; then
    unknown write '"the write needs J1 (kubectl and the principal token)"'
    return 0
  fi
  # The catalog's maintenance job runs as `pipeline-<owner group>`; without it the Catalog
  # stays not-CatalogReady (`MaintenanceReady=False`). Same shape as demo-access.sh.
  if ! kubectl -n "$NAMESPACE" apply -f - >/dev/null 2>"$tmp/principal.err" <<EOF
apiVersion: stardelt.io/v1alpha1
kind: ServicePrincipal
metadata:
  name: pipeline-${GROUP}
spec:
  ownerGroup: ${GROUP}
  purpose: pipeline
  audiences: [trino, lakekeeper]
EOF
  then
    bad write "\"the maintenance principal was refused: $(head -c 200 "$tmp/principal.err")\""
    return 0
  fi
  if ! kubectl -n "$NAMESPACE" wait --for=condition=Ready "serviceprincipal/pipeline-${GROUP}" --timeout=120s >/dev/null 2>&1; then
    bad write "\"serviceprincipal/pipeline-${GROUP} is not Ready: $(kubectl -n "$NAMESPACE" get "serviceprincipal/pipeline-${GROUP}" -o jsonpath='{.status.conditions[0].message}' 2>/dev/null)\""
    return 0
  fi
  if ! kubectl -n "$NAMESPACE" apply -f - >/dev/null 2>"$tmp/catalog.err" <<EOF
apiVersion: stardelt.io/v1alpha1
kind: Catalog
metadata:
  name: journey
spec:
  displayName: Journey
  description: The fresh-node run's write proof; the run deletes it again.
  lakehouseRef: {name: main}
  owner: stardelt-users
  grants:
    - {group: stardelt-users, verb: browse}
    - {group: stardelt-users, verb: select}
    - {group: stardelt-users, verb: write}
    - {group: stardelt-users, verb: create}
EOF
  then
    bad write "\"the Catalog was refused: $(head -c 200 "$tmp/catalog.err")\""
    return 0
  fi
  if ! kubectl -n "$NAMESPACE" wait --for=condition=CatalogReady catalog/journey --timeout=180s >/dev/null 2>&1; then
    bad write "\"catalog/journey is not CatalogReady: $(kubectl -n "$NAMESPACE" get catalog/journey -o jsonpath='{range .status.conditions[*]}{.type}={.status} {.message}{"\n"}{end}' 2>/dev/null)\""
    return 0
  fi
  if ! kubectl -n "$NAMESPACE" wait --for=condition=GrantsReady catalog/journey --timeout=180s >/dev/null 2>&1; then
    bad write "\"catalog/journey is not GrantsReady: $(kubectl -n "$NAMESPACE" get catalog/journey -o jsonpath='{range .status.conditions[*]}{.type}={.status} {.message}{"\n"}{end}' 2>/dev/null)\""
    return 0
  fi
  schema='create schema if not exists journey.e2e'
  table='create table if not exists journey.e2e.exit_test (id bigint)'
  if ! trino_sql "$tmp/access-token" "$schema" >/dev/null; then
    bad write '"create schema journey.e2e did not answer"'
    return 0
  fi
  if ! trino_sql "$tmp/access-token" "$table" >/dev/null; then
    bad write '"create table journey.e2e.exit_test did not answer"'
    return 0
  fi
  if ! trino_sql "$tmp/access-token" 'delete from journey.e2e.exit_test' >/dev/null; then
    bad write '"delete from journey.e2e.exit_test did not answer"'
    return 0
  fi
  if ! trino_sql "$tmp/access-token" 'insert into journey.e2e.exit_test (id) values (1), (2), (3)' >/dev/null; then
    bad write '"insert into journey.e2e.exit_test did not answer"'
    return 0
  fi
  if ! rows="$(trino_sql "$tmp/access-token" 'select count(*) from journey.e2e.exit_test')"; then
    rows=""
  fi
  if [ "$rows" = 3 ]; then
    ok write "three rows in journey.e2e.exit_test"
  else
    bad write "\"count(*) answered ${rows:-nothing}, not 3\""
  fi
}

# --- X1: the way out ----------------------------------------------------------------------
phase_exit_read() {
  local venv
  if ! command -v kubectl >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1; then
    unknown exit-read '"kubectl and python3 are needed"'
    return 0
  fi
  venv="$tmp/iceberg-venv"
  if ! python3 -m venv "$venv" >/dev/null 2>&1; then
    unknown exit-read '"python3 -m venv is not available"'
    return 0
  fi
  if ! "$venv/bin/pip" install --quiet --disable-pip-version-check "pyiceberg[pyarrow]" boto3 >"$tmp/iceberg-pip.log" 2>&1; then
    bad exit-read "\"pip install pyiceberg[pyarrow] boto3 failed: $(tail -c 200 "$tmp/iceberg-pip.log")\""
    return 0
  fi
  kubectl -n "$NAMESPACE" get secret stardelt-s3-creds -o jsonpath='{.data.access-key}' | base64 -d >"$tmp/s3-access"
  kubectl -n "$NAMESPACE" get secret stardelt-s3-creds -o jsonpath='{.data.secret-key}' | base64 -d >"$tmp/s3-secret"
  chmod 600 "$tmp/s3-access" "$tmp/s3-secret"
  kubectl -n "$NAMESPACE" port-forward svc/seaweedfs-s3 18333:8333 >/dev/null 2>&1 &
  s3_forward_pid=$!
  local attempt=0
  while [ "$attempt" -lt 30 ]; do
    if curl -sS --max-time 3 -o /dev/null http://127.0.0.1:18333/; then
      break
    fi
    if ! kill -0 "$s3_forward_pid" 2>/dev/null; then
      bad exit-read '"port-forward to svc/seaweedfs-s3 exited immediately"'
      return 0
    fi
    sleep 0.5
    attempt=$((attempt + 1))
  done
  local line
  if ! line="$("$venv/bin/python" "$PLATFORM_DIR/scripts/journey_read.py" \
    --endpoint http://127.0.0.1:18333 --bucket lakehouse --needle /e2e/exit_test/metadata/ \
    --access-key-file "$tmp/s3-access" --secret-key-file "$tmp/s3-secret" --rows 3)"; then
    bad exit-read "\"the table could not be read without stardelt: ${line:-no answer}\""
    return 0
  fi
  ok exit-read "$line"
}

# --- J4: a refusal ------------------------------------------------------------------------
phase_refusal() {
  local outsider="journey-outsider" token_file
  if ! command -v kubectl >/dev/null 2>&1 || [ ! -s "$tmp/access-token" ]; then
    unknown refuse '"the refusal needs J1 (kubectl and the port-forward)"'
    return 0
  fi
  if ! NAMESPACE="$NAMESPACE" KIND=service NAME="$outsider" AUDIENCES=trino,lakekeeper \
    bash "$PLATFORM_DIR/scripts/keycloak-sync-principal.sh" >"$tmp/outsider-sync.log" 2>&1; then
    bad refuse "\"the second client could not be created: $(tail -c 300 "$tmp/outsider-sync.log")\""
    return 0
  fi
  token_file="$tmp/outsider-token"
  kubectl -n "$NAMESPACE" get secret "svc-${outsider}" -o jsonpath='{.data.client-id}' | base64 -d >"$tmp/outsider-client-id"
  kubectl -n "$NAMESPACE" get secret "svc-${outsider}" -o jsonpath='{.data.client-secret}' | base64 -d >"$tmp/outsider-client-secret"
  kubectl -n "$NAMESPACE" get secret "svc-${outsider}" -o jsonpath='{.data.token-url}' | base64 -d >"$tmp/outsider-token-url"
  chmod 600 "$tmp"/outsider-client-id "$tmp"/outsider-client-secret "$tmp"/outsider-token-url
  local endpoint
  endpoint="$(cat "$tmp/outsider-token-url")"
  if ! curl -sS -o "$tmp/outsider-token.json" --max-time 20 -X POST "$endpoint" \
    --data-urlencode "grant_type=client_credentials" \
    --data-urlencode "client_id@$tmp/outsider-client-id" \
    --data-urlencode "client_secret@$tmp/outsider-client-secret"; then
    bad refuse '"the token endpoint did not answer for the second client"'
    return 0
  fi
  python3 - "$tmp/outsider-token.json" >"$token_file" <<'PY'
import json, sys
answer = json.load(open(sys.argv[1]))
if "access_token" not in answer:
    raise SystemExit("no access_token: %s" % answer.get("error", "no answer"))
sys.stdout.write(answer["access_token"])
PY
  local answer rc=0
  answer="$(trino_sql "$token_file" 'select count(*) from journey.e2e.exit_test' 2>&1)" || rc=$?
  if [ "$rc" -eq 0 ]; then
    bad refuse "\"the second client read journey.e2e.exit_test (${answer:-no answer}): the grant boundary is open\""
    return 0
  fi
  case "$answer" in
    *"Access Denied"*|*"Cannot access catalog"*|*"not authorized"*)
      ok refuse "svc-${outsider} (no group) was refused journey.e2e.exit_test" ;;
    *)
      bad refuse "\"the second client failed for another reason: ${answer:-no answer}\"" ;;
  esac
}

# --- main ---------------------------------------------------------------------------------
case "${1:-all}" in
  all | identity)
    phase_identity
    if [ "${1:-all}" = all ]; then
      phase_client
      phase_write
      phase_exit_read
      phase_refusal
    fi
    ;;
  client) phase_client ;;
  write) phase_write ;;
  exit-read) phase_exit_read ;;
  refusal) phase_refusal ;;
  *)
    echo "journey-fresh-node: unknown phase '$1' (all, identity, client, write, exit-read, refusal)" >&2
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
