#!/usr/bin/env bash
# Spike for stardelt/stardelt#820 (#114's WAP slices b–e, after #744, #796, #807): does
# Write-Audit-Publish work on Iceberg branches through Lakekeeper, and with which reader?
#
# #114 decided WAP as four steps: the run writes to a branch `audit_<run id>`, the `quality`
# task checks that branch, Nova moves `main` with the Iceberg REST commit `set-snapshot-ref`
# plus `assert-ref-snapshot-id`, and a failed run leaves the branch behind. Three of the four
# are unmeasured. This run measures them on two scratch tables, `playground.spike_wap.t`
# (`sts-enabled: false`) and `gold.spike_wap.t` (`sts-enabled: true`):
#
#   Q1  the branch write: which of the three Spark forms writes into a branch through the
#       REST catalog — `spark.wap.branch`, the `branch` write option, or the branch-qualified
#       table name `t.branch_audit_spike` — and what the branch ref's snapshot id does. The
#       pod runs with `spark.sql.extensions` set, as the platform's Spark does
#       (manifests/spark.yaml); without it every branch write is refused, which the first
#       run of this script measured (`Must enable Iceberg extensions …`). The two switches
#       are measured once more after `write.wap.enabled=true` is set on the table: that
#       property is what makes them write the branch instead of main.
#   Q2  isolation: `main` keeps the snapshot it had, the new data files exist only on the
#       branch, and a plain SELECT on `main` does not see the new rows.
#   Q3  the check on the branch: can Trino (`sql` today) and Spark read the branch — by
#       snapshot id and by branch-qualified name? This decides whether a `quality` task needs
#       a new runtime.
#   Q4  the publish: the commit body #114 describes, the corrected `action` form the catalog
#       actually takes, and the answer to a stale `assert-ref-snapshot-id` ("main moved").
#   Q5  the drop: a branch ref with `max-ref-age-ms` (7 days) as the failed run leaves it,
#       and the status of deleting the branch.
#   Q6  STS: the same branch write on `gold`, and whether `LoadTableResult.config` carries a
#       vended `s3.access-key-id`/`s3.session-token` for the branch as it does for `main`.
#
# The catalog identity in the pod is the workload's *projected* ServiceAccount token, read per
# request by `io.stardelt.iceberg.TokenFileAuthManager` (#811): the WAP writer a runner will be.
# Its audience is the catalog's workload audience (`stardelt-catalog-workload`, #562/helm-values),
# not the OIDC audience `lakekeeper`, which the catalog refuses by design (#492); the script
# measures both audiences first so the line is evidence, not assumption.
#
# Storage credentials in the pod are the platform's S3 Secret (#769's W3b): `playground` vends
# nothing (`sts-enabled: false`), so a write there can only use them; on `gold` the vended
# credential of the delegated load takes precedence, which is the visitor-table shape.
#
# The lab, all reverted in `cleanup` on EXIT/INT/TERM:
#   - namespace `stardelt-spike-wap` with ServiceAccount `spike-wap` and a copy of Secret
#     `stardelt-s3-creds` (deleted with the namespace);
#   - one `assignee` on the existing `demo` role for `kubernetes~<sa uid>`, re-applied every
#     10 s while the pod runs and deleted again: access-sync removes an assignee that no
#     ServicePrincipal backs (measured 2026-11-21, #769). `demo` holds `write` on `playground`
#     and on `gold`; `visitors` holds it on `playground` only;
#   - one *additive* NetworkPolicy `spike-wap-allow` in the catalog's namespace (the
#     `lakekeeper-ingress` policy admits named callers only, #769); deleted on every exit path;
#   - schema `spike_wap` and table `t` in `playground` and in `gold`, the branch refs this run
#     created, and the branches' snapshots. A table that already exists stops the run.
#
# It prints no token, key or secret: names, status codes, snapshot ids, row and file counts
# only. A token travels in a mode-0600 file (`-H @file`) or inside the pod.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
LK_PORT="${LK_PORT:-18181}"
AUDIENCE="${AUDIENCE:-stardelt-catalog-workload}"
OIDC_AUDIENCE="${OIDC_AUDIENCE:-lakekeeper}"
SA_NS=stardelt-spike-wap
SA=spike-wap
POD=spike-wap
NETPOL=spike-wap-allow
PLAY=playground
GOLD=gold
SCHEMA=spike_wap
TABLE=t
BRANCH=audit_spike
TTL_BRANCH=audit_spike_ttl
TTL_MS="${TTL_MS:-604800000}"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/wap-branch.XXXXXX")"
LK_PID=""
SA_UID=""
DEMO_ROLE=""
GRANT_PID=""
WID_PLAY=""
WID_GOLD=""
ns_created=no
netpol_created=no
assigned=no
schema_created_play=no
schema_created_gold=no

# "<warehouse-id> <schema>" of every table this run created; `cleanup` touches nothing else.
TABLES=()
# Catalog branches this run created, as "<warehouse-id> <branch>".
BRANCHES=()

# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  local code pairs="" entry wid branch schema
  if [ -n "$GRANT_PID" ]; then
    rm -f "$tmp/grant-loop"
    wait "$GRANT_PID" 2>/dev/null || true
  fi
  if [ "$assigned" = "yes" ] && [ -n "$DEMO_ROLE" ] && [ -s "$tmp/auth" ]; then
    jq -n --arg u "kubernetes~$SA_UID" \
      '{writes: [], deletes: [{type: "assignee", user: $u}]}' > "$tmp/unassign.json"
    echo "L cleanup role-unassign=$(mgmt POST "/management/v1/permissions/role/$DEMO_ROLE/assignments" "$tmp/unassign.json")"
  fi
  if [ -s "$tmp/auth" ]; then
    # The branches first: a branch keeps a snapshot alive, and the table drop purges anyway.
    for entry in ${BRANCHES[@]+"${BRANCHES[@]}"}; do
      read -r wid branch <<< "$entry"
      code="$(remove_ref "$wid" "$SCHEMA" "$TABLE" "$branch" 2>/dev/null || echo 000)"
      pairs="$pairs $branch=$code"
    done
    if [ -n "$pairs" ]; then echo "L cleanup delete-branch=$(echo "$pairs" | sed 's/^ //; s/ /,/g')"; fi
    for entry in ${TABLES[@]+"${TABLES[@]}"}; do
      read -r wid schema <<< "$entry"
      # The catalog calls the two warehouses by name; the id in TABLES is the management id.
      code="$(drop_table "$wid" "$schema" "$TABLE" 2>/dev/null || echo 000)"
      echo "L cleanup drop-table=$([ "$wid" = "$WID_PLAY" ] && echo "$PLAY" || echo "$GOLD").$schema.$TABLE code=$code"
    done
    if [ "$schema_created_play" = "yes" ]; then
      code="$(delete_schema "$WID_PLAY" "$SCHEMA" 2>/dev/null || echo 000)"
      echo "L cleanup delete-schema=$PLAY.$SCHEMA code=$code"
    fi
    if [ "$schema_created_gold" = "yes" ]; then
      code="$(delete_schema "$WID_GOLD" "$SCHEMA" 2>/dev/null || echo 000)"
      echo "L cleanup delete-schema=$GOLD.$SCHEMA code=$code"
    fi
  fi
  if [ "$netpol_created" = "yes" ]; then
    if kubectl -n "$NS" delete networkpolicy "$NETPOL" --ignore-not-found >/dev/null 2>&1; then
      echo "L cleanup delete-networkpolicy=ok"
    else
      echo "L cleanup delete-networkpolicy=fail"
    fi
  fi
  if [ "$ns_created" = "yes" ]; then
    if kubectl delete namespace "$SA_NS" --timeout=120s >/dev/null 2>&1; then
      echo "L cleanup delete-namespace=ok"
    else
      echo "L cleanup delete-namespace=fail"
    fi
  fi
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT
# A signal must end the script; the EXIT trap above still runs `cleanup` on every path.
trap 'exit 130' INT
trap 'exit 143' TERM

for tool in kubectl curl jq python3 base64; do
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

# A call with any Bearer header file (the run's ServiceAccount tokens).
bearer() { # <method> <auth-file> <path> -> code, body in $tmp/out.json
  local method="$1" auth="$2" path="$3" code reopened=no
  while :; do
    code="$(curl -sS -H @"$auth" -X "$method" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    if [ "$code" = "000" ] && [ "$reopened" = "no" ]; then reopened=yes; forward_lk && continue; fi
    break
  done
  echo "${code:-000}"
}

# The delegated load: the header is what makes Lakekeeper put vended credentials in `config`.
load_table() { # <warehouse-id> <namespace> <table> [query] -> code, body in $tmp/out.json
  local code reopened=no
  while :; do
    code="$(curl -sS -H @"$tmp/auth" -H 'X-Iceberg-Access-Delegation: vended-credentials' \
      "$LK/catalog/v1/$1/namespaces/$2/tables/$3${4-}" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
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

# shellcheck disable=SC2329  # called from the trap
drop_table() { # <warehouse-id> <namespace> <table> -> status code (purged)
  local code refreshed=no
  while :; do
    code="$(curl -sS -H @"$tmp/auth" -X DELETE \
      "$LK/catalog/v1/$1/namespaces/$2/tables/$3?purgeRequested=true" -o /dev/null -w '%{http_code}')" || code="000"
    if [ "$code" = "401" ] && [ "$refreshed" = "no" ]; then refreshed=yes; bootstrap_auth; continue; fi
    break
  done
  echo "${code:-000}"
}

# shellcheck disable=SC2329  # called from the trap
delete_schema() { # <warehouse-id> <namespace> -> status code
  local code refreshed=no
  while :; do
    code="$(curl -sS -H @"$tmp/auth" -X DELETE \
      "$LK/catalog/v1/$1/namespaces/$2" -o /dev/null -w '%{http_code}')" || code="000"
    if [ "$code" = "401" ] && [ "$refreshed" = "no" ]; then refreshed=yes; bootstrap_auth; continue; fi
    break
  done
  echo "${code:-000}"
}

# shellcheck disable=SC2329  # called from the trap
remove_ref() { # <warehouse-id> <namespace> <table> <ref> -> status code
  jq -n --arg ref "$4" \
    '{requirements: [], updates: [{action: "remove-snapshot-ref", "ref-name": $ref}]}' \
    > "$tmp/remove-ref.json"
  mgmt POST "/catalog/v1/$1/namespaces/$2/tables/$3" "$tmp/remove-ref.json"
}

# --- the workload's identity -------------------------------------------------------------
bootstrap_auth
if kubectl create namespace "$SA_NS" >/dev/null 2>&1; then ns_created=yes; fi
if [ "$ns_created" != "yes" ]; then
  echo "namespace $SA_NS already exists; not deleting something this run did not create" >&2
  exit 1
fi
kubectl label namespace "$SA_NS" \
  pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/warn=restricted >/dev/null
kubectl -n "$SA_NS" create serviceaccount "$SA" >/dev/null
SA_UID="$(kubectl -n "$SA_NS" get sa "$SA" -o jsonpath='{.metadata.uid}')"
echo "L principal=kubernetes~$SA_UID"

# The platform's own S3 Secret, copied for the pod: `playground` vends nothing (#769's W3b).
if kubectl -n "$NS" get secret stardelt-s3-creds >/dev/null 2>&1; then
  kubectl -n "$NS" get secret stardelt-s3-creds -o json \
    | jq --arg ns "$SA_NS" '{apiVersion: "v1", kind: "Secret", type: "Opaque",
        metadata: {name: "stardelt-s3-creds", namespace: $ns}, data: .data}' \
    | kubectl apply -f - >/dev/null
  echo "L s3-secret=copied (the storage half, as every Spark run gets it)"
else
  echo "L s3-secret=missing"
fi

# The audience question first: the catalog's workload audience authenticates, the OIDC one
# must not (#492). A 401 from `lakekeeper` is the documented split, not a defect.
mint_sa "$SA" none "$tmp/sa-oidc"
oc="$(bearer GET "$tmp/sa-oidc.auth" "/catalog/v1/config?warehouse=$PLAY")"
mint_sa "$SA" "$AUDIENCE" "$tmp/sa-token"
ac="$(bearer GET "$tmp/sa-token.auth" "/catalog/v1/config?warehouse=$PLAY")"
echo "L audience=$OIDC_AUDIENCE http=$oc (refused by design) audience=$AUDIENCE http=$ac"

# The first *config* call creates the catalog user (#538); its answer may still be 404 because
# the new principal holds no grant yet, but `whoami` afterwards names it.
code="$(bearer GET "$tmp/sa-token.auth" /management/v1/whoami)"
id="$(jq -r '.id // "none"' "$tmp/out.json" 2>/dev/null || echo none)"
echo "L principal-whoami=$code id-matches=$([ "$id" = "kubernetes~$SA_UID" ] && echo yes || echo no)"

code="$(mgmt GET /management/v1/role)"
DEMO_ROLE="$(jq -r '.roles[]? | select(.name == "demo") | .id' "$tmp/out.json" 2>/dev/null | head -n1)"
if [ -z "$DEMO_ROLE" ]; then
  echo "the catalog has no role 'demo'; the write cannot be granted" >&2
  exit 1
fi
jq -n --arg u "kubernetes~$SA_UID" '{writes: [{type: "assignee", user: $u}], deletes: []}' > "$tmp/assign.json"
assign_code="$(mgmt POST "/management/v1/permissions/role/$DEMO_ROLE/assignments" "$tmp/assign.json")"
if [ "$assign_code" = "204" ] || [ "$assign_code" = "200" ]; then assigned=yes; fi
echo "L demo-role=$DEMO_ROLE assign=$assign_code"

# access-sync reconciles the catalog roles every two minutes and removes an assignee that is
# not backed by a ServicePrincipal (#769, measured 2026-11-21). The product path is a
# ServicePrincipal's owner-group role (#650/#684); for this measurement the run keeps the one
# assignee applied until the pod has finished, and `cleanup` deletes it.
keep_grant() {
  local code
  while [ -f "$tmp/grant-loop" ]; do
    code="$(curl -sS -H @"$tmp/auth" -H 'Content-Type: application/json' -X POST \
      -d @"$tmp/assign.json" "$LK/management/v1/permissions/role/$DEMO_ROLE/assignments" \
      -o "$tmp/loop.json" -w '%{http_code}')" || code="000"
    if [ "$code" = "401" ]; then bootstrap_auth; fi
    sleep 5
  done
}
touch "$tmp/grant-loop"
keep_grant &
GRANT_PID=$!
echo "L grant-keeper=started (access-sync removes a hand-made assignee within one cycle)"

mgmt GET /management/v1/warehouse >/dev/null
WID_PLAY="$(jq -r '(.warehouses // .)[] | select(.name == "'"$PLAY"'") | .id' "$tmp/out.json" 2>/dev/null | head -n1)"
WID_GOLD="$(jq -r '(.warehouses // .)[] | select(.name == "'"$GOLD"'") | .id' "$tmp/out.json" 2>/dev/null | head -n1)"
echo "L warehouse=$PLAY id=${WID_PLAY:-none} $GOLD id=${WID_GOLD:-none}"
if [ -z "$WID_PLAY" ] || [ -z "$WID_GOLD" ] || [ "$WID_PLAY" = "null" ] || [ "$WID_GOLD" = "null" ]; then
  echo "warehouse $PLAY or $GOLD is missing; the run cannot write" >&2
  exit 1
fi

# The catalog's authorisation cache answers 404 for 3-15 s after a grant change (#538).
sleep 20
code="$(bearer GET "$tmp/sa-token.auth" "/catalog/v1/config?warehouse=$PLAY")"
if [ "$code" != "200" ]; then
  sleep 15
fi
echo "L grant-reached=$(bearer GET "$tmp/sa-token.auth" "/catalog/v1/config?warehouse=$PLAY")"

# --- the scratch schema and one table per warehouse ---------------------------------------
# A table that already exists stops the run: `cleanup` would purge another run's data.
for pair in "$WID_PLAY:$PLAY" "$WID_GOLD:$GOLD"; do
  wid="${pair%%:*}"
  name="${pair#*:}"
  code="$(mgmt GET "/catalog/v1/$wid/namespaces/$SCHEMA/tables/$TABLE")"
  echo "L table-before=$name.$SCHEMA.$TABLE http=$code"
  if [ "$code" != "404" ]; then
    echo "table $name.$SCHEMA.$TABLE answered $code, not 404; refusing to touch it" >&2
    exit 1
  fi
done

create_schema() { # <warehouse-id> -> status code
  jq -n --arg ns "$SCHEMA" '{namespace: [$ns], properties: {}}' > "$tmp/namespace.json"
  mgmt POST "/catalog/v1/$1/namespaces" "$tmp/namespace.json"
}

create_table() { # <warehouse-id> -> status code
  jq -n --arg t "$TABLE" '{
      name: $t,
      schema: {"type": "struct", "schema-id": 0, fields: [
        {"id": 1, "name": "id", "required": false, "type": "int"},
        {"id": 2, "name": "note", "required": false, "type": "string"}]},
      "partition-spec": {"spec-id": 0, fields: []},
      "write-order": {"order-id": 0, fields: []},
      properties: {}}' > "$tmp/table.json"
  mgmt POST "/catalog/v1/$1/namespaces/$SCHEMA/tables" "$tmp/table.json"
}

for pair in "$WID_PLAY:$PLAY" "$WID_GOLD:$GOLD"; do
  wid="${pair%%:*}"
  name="${pair#*:}"
  code="$(create_schema "$wid")"
  echo "L schema-create=$name.$SCHEMA code=$code"
  if [ "$code" = "200" ] || [ "$code" = "204" ]; then
    if [ "$name" = "$PLAY" ]; then schema_created_play=yes; else schema_created_gold=yes; fi
  elif [ "$code" != "409" ]; then
    echo "cannot create schema $name.$SCHEMA: $code $(jq -c '.error.message' "$tmp/out.json" 2>/dev/null || true)" >&2
    exit 1
  fi
  code="$(create_table "$wid")"
  echo "L table-create=$name.$SCHEMA.$TABLE code=$code"
  if [ "$code" != "200" ]; then
    echo "cannot create table $name.$SCHEMA.$TABLE: $code $(jq -c '.error.message' "$tmp/out.json" 2>/dev/null || true)" >&2
    exit 1
  fi
  TABLES+=("$wid $SCHEMA")
done

# --- the additive NetworkPolicy (see the header) -----------------------------------------
if kubectl -n "$NS" get networkpolicy "$NETPOL" >/dev/null 2>&1; then
  echo "NetworkPolicy $NETPOL already exists; not overwriting something this run did not create" >&2
  exit 1
fi
if ! kubectl -n "$NS" apply -f - >/dev/null 2>&1 <<EOF
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: ${NETPOL}
  namespace: ${NS}
  labels: { app.kubernetes.io/part-of: stardelt }
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: lakekeeper
      app.kubernetes.io/component: catalog
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: ${SA_NS}
          podSelector:
            matchLabels:
              app.kubernetes.io/name: ${POD}
      ports:
        - port: 8181
          protocol: TCP
EOF
then
  echo "the additive NetworkPolicy could not be created; the pod cannot reach the catalog" >&2
  exit 1
fi
netpol_created=yes

# --- the pod ------------------------------------------------------------------------------
image="$(awk '/^ *image: ghcr\.io\/stardelt\/spark:/{print $2; exit}' manifests/spark.yaml)"
if [ -z "$image" ]; then
  echo "manifests/spark.yaml names no ghcr.io/stardelt/spark image" >&2
  exit 1
fi
echo "L image=$image"

cat > "$tmp/run.py" <<PY
"""Spike #820: WAP on Iceberg branches from Spark — the write, the read, the isolation."""
import os
import time

CATALOG = os.environ.get("CATALOG_URI", "http://lakekeeper.stardelt.svc.cluster.local:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://seaweedfs-s3.stardelt.svc.cluster.local:8333")
TOKEN_FILE = os.environ.get("TOKEN_FILE", "/var/run/secrets/stardelt-catalog/token")
BRANCH = os.environ.get("BRANCH", "$BRANCH")
PLAY = "$PLAY"
GOLD = "$GOLD"
SCHEMA = "$SCHEMA"
TABLE = "$TABLE"

PLAY_TABLE = ".".join((PLAY, SCHEMA, TABLE))
GOLD_TABLE = ".".join((GOLD, SCHEMA, TABLE))


def out(line):
    print(line, flush=True)


def first_line(exc):
    lines = [line.strip() for line in str(exc).splitlines() if line.strip()]
    # Py4J wraps a Java exception as "An error occurred while calling ..." and puts the real
    # message on the next line, beginning with ':'. Take that one when it is there.
    for line in lines:
        if line.startswith(": "):
            return line[2:].strip()[:240]
    return (lines[0] if lines else type(exc).__name__)[:240]


def wait_token():
    for _ in range(60):
        try:
            with open(TOKEN_FILE) as handle:
                value = handle.read().strip()
            if value:
                return value
        except OSError:
            pass
        time.sleep(1)
    raise SystemExit("the projected token file never appeared")


wait_token()
from pyspark.sql import SparkSession

session = SparkSession.builder.appName("wap-branch-spike").getOrCreate()


def configure(name):
    prefix = "spark.sql.catalog." + name
    values = {
        "": "org.apache.iceberg.spark.SparkCatalog",
        ".type": "rest",
        ".uri": CATALOG + "/catalog",
        ".warehouse": name,
        ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        ".s3.endpoint": S3_ENDPOINT,
        ".s3.region": "us-east-1",
        ".s3.path-style-access": "true",
        ".header.X-Iceberg-Access-Delegation": "vended-credentials",
        ".rest.auth.type": "io.stardelt.iceberg.TokenFileAuthManager",
        ".rest.auth.token-file": TOKEN_FILE,
    }
    # The platform's own S3 Secret: playground has sts-enabled=false and vends nothing, so a
    # write there can only use these; on gold the vended credential takes precedence.
    if os.environ.get("S3_ACCESS_KEY"):
        values[".s3.access-key-id"] = os.environ["S3_ACCESS_KEY"]
        values[".s3.secret-access-key"] = os.environ.get("S3_SECRET_KEY", "")
    for key, value in values.items():
        session.conf.set(prefix + key, value)


configure(PLAY)
configure(GOLD)


def with_retry(action, attempts=12, delay=5):
    """Retry a statement the hand-made grant can lose for one access-sync cycle (#769)."""
    last = ""
    for _ in range(attempts):
        try:
            action()
            return "ok"
        except Exception as exc:
            last = first_line(exc)
            if not any(word in last for word in ("Forbidden", "NotAuthorized", "does not exist",
                                                "NotFoundException", "NoSuchWarehouse")):
                return "refused: " + last
            time.sleep(delay)
    return "refused: " + last


def refs(table):
    return {row["name"]: row["snapshot_id"]
            for row in session.sql("SELECT name, snapshot_id FROM " + table + ".refs").collect()}


def branch_snapshot(table):
    return refs(table).get(BRANCH)


def rows_at(table, snapshot):
    if snapshot is None:
        return "n/a"
    try:
        return str(session.sql(
            "SELECT count(*) AS c FROM " + table + " VERSION AS OF " + str(snapshot)
        ).collect()[0]["c"])
    except Exception as exc:
        return "error(" + first_line(exc)[:80] + ")"


def files_at(table, snapshot=None):
    statement = "SELECT file_path FROM " + table + ".files"
    if snapshot is not None:
        statement += " VERSION AS OF " + str(snapshot)
    try:
        return {row["file_path"] for row in session.sql(statement).collect()}
    except Exception as exc:
        return first_line(exc)[:160]


def attempt(tag, table, action):
    before = branch_snapshot(table)
    main_before = refs(table).get("main")
    error = ""
    try:
        action()
    except Exception as exc:
        error = first_line(exc)
    after = branch_snapshot(table)
    main_after = refs(table).get("main")
    wrote = after is not None and after != before
    out("%s: branch-before=%s branch-after=%s wrote=%s main-before=%s main-after=%s "
        "main-wrote=%s rows-on-branch=%s rows-on-main=%d%s" % (
            tag,
            "none" if before is None else before,
            "none" if after is None else after,
            "yes" if wrote else "no",
            main_before,
            main_after,
            "yes" if main_after != main_before else "no",
            rows_at(table, after),
            session.table(table).count(),
            (" error=" + error) if error else ""))
    return wrote


def form_table_qualified(table):
    session.sql("INSERT INTO " + table + ".branch_" + BRANCH + " VALUES (11, 'table-qualified')")


def form_write_option(table):
    (session.createDataFrame([(12, "write-option")], "id int, note string")
     .writeTo(table).option("branch", BRANCH).append())


def form_wap_conf(table):
    session.conf.set("spark.wap.branch", BRANCH)
    try:
        session.sql("INSERT INTO " + table + " VALUES (13, 'wap-conf')")
    finally:
        session.conf.unset("spark.wap.branch")


def negative_absent_branch(table):
    missing = BRANCH + "_absent"
    error = ""
    try:
        session.sql("INSERT INTO " + table + ".branch_" + missing + " VALUES (99, 'absent')")
    except Exception as exc:
        error = first_line(exc)
    after = refs(table).get(missing)
    out("Q1d: branch=%s wrote=%s error=%s" % (
        missing, "yes" if after is not None else "no", error or "none"))


def branch_forms(tag_prefix, table):
    working = []
    # spark.wap.branch runs first: it is the one form that creates a missing branch.
    if attempt(tag_prefix + "c", table, lambda: form_wap_conf(table)):
        working.append("spark.wap.branch")
    if attempt(tag_prefix + "b", table, lambda: form_write_option(table)):
        working.append("write-option")
    if branch_snapshot(table) is None:
        # A branch-qualified write does not create the branch; make it the way a runner would
        # and measure the write itself.
        try:
            session.sql("ALTER TABLE " + table + " CREATE BRANCH " + BRANCH)
            out("L branch-created-by=alter-table %s" % table)
        except Exception as exc:
            out("L branch-create=%s refused: %s" % (table, first_line(exc)))
    if attempt(tag_prefix + "a", table, lambda: form_table_qualified(table)):
        working.append("table-qualified")
    return working


# --- Q1: the branch write on playground ---------------------------------------------------
out("L catalog-preflight=" + with_retry(
    lambda: session.sql("SELECT count(*) FROM " + PLAY_TABLE).collect()))
out("L main-write=%s = %s" % (
    PLAY_TABLE, with_retry(lambda: session.sql("INSERT INTO " + PLAY_TABLE + " VALUES (0, 'main')"))))
out("L main-ref-before=%s" % refs(PLAY_TABLE).get("main"))

negative_absent_branch(PLAY_TABLE)
working = branch_forms("Q1", PLAY_TABLE)

# The table property a WAP table carries (Iceberg's write.wap.enabled): without it the
# branch option and the WAP conf commit to main, silently. With it, measure the same two
# forms again.
try:
    session.sql("ALTER TABLE " + PLAY_TABLE + " SET TBLPROPERTIES ('write.wap.enabled'='true')")
    out("L table-property=write.wap.enabled:true " + PLAY_TABLE)
except Exception as exc:
    out("L table-property=%s refused: %s" % (PLAY_TABLE, first_line(exc)))
if attempt("Q1e", PLAY_TABLE, lambda: form_write_option(PLAY_TABLE)):
    working.append("write-option(write.wap.enabled)")
if attempt("Q1f", PLAY_TABLE, lambda: form_wap_conf(PLAY_TABLE)):
    working.append("spark.wap.branch(write.wap.enabled)")
out("Q1: forms=table-qualified,write-option,spark.wap.branch working=%s branch=%s branch-snapshot=%s "
    "(the option and the conf only write the branch with write.wap.enabled=true)" % (
        ",".join(working) if working else "none", BRANCH, branch_snapshot(PLAY_TABLE)))

# --- Q2: isolation ------------------------------------------------------------------------
branch = branch_snapshot(PLAY_TABLE)
main_rows = session.table(PLAY_TABLE).count()
try:
    branch_rows = str(session.table(PLAY_TABLE + ".branch_" + BRANCH).count())
except Exception as exc:
    branch_rows = "error(" + first_line(exc)[:80] + ")"
main_files = files_at(PLAY_TABLE)
branch_files = files_at(PLAY_TABLE, branch) if branch is not None else "none"
if isinstance(main_files, set) and isinstance(branch_files, set):
    branch_only = str(len(branch_files - main_files))
    main_only = str(len(main_files - branch_files))
else:
    branch_only = "n/a"
    main_only = "n/a"
misplaced = session.sql(
    "SELECT count(*) AS c FROM " + PLAY_TABLE + " WHERE id = 11").collect()[0]["c"]
out("Q2: spark main-snapshot=%s main-rows=%d branch-snapshot=%s branch-rows=%s "
    "branch-only-files=%s main-only-files=%s branch-row-11-visible-on-main=%d" % (
        refs(PLAY_TABLE).get("main"), main_rows, branch, branch_rows, branch_only, main_only,
        misplaced))

# --- Q3: Spark reads the branch -----------------------------------------------------------
try:
    table_form = str(session.table(PLAY_TABLE + ".branch_" + BRANCH).count())
except Exception as exc:
    table_form = "refused(" + first_line(exc)[:100] + ")"
try:
    version_form = str(session.sql(
        "SELECT count(*) AS c FROM " + PLAY_TABLE + " VERSION AS OF " + str(branch)
    ).collect()[0]["c"])
except Exception as exc:
    version_form = "refused(" + first_line(exc)[:100] + ")"
out("Q3: spark-table-form=%s spark-version-as-of=%s" % (table_form, version_form))

# --- Q6: the same on gold (sts-enabled: true) --------------------------------------------
out("L main-write=%s = %s" % (
    GOLD_TABLE, with_retry(lambda: session.sql("INSERT INTO " + GOLD_TABLE + " VALUES (0, 'main')"))))
out("L gold-main-ref-before=%s" % refs(GOLD_TABLE).get("main"))
gold_working = branch_forms("Q6", GOLD_TABLE)
out("Q6: gold forms=table-qualified,write-option,spark.wap.branch working=%s branch=%s branch-snapshot=%s" % (
    ",".join(gold_working) if gold_working else "none", BRANCH, branch_snapshot(GOLD_TABLE)))

session.stop()
PY

kubectl -n "$SA_NS" create configmap "$POD-script" \
  --from-file=run.py="$tmp/run.py" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

if kubectl -n "$SA_NS" apply -f - >/dev/null 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${SA_NS}
  labels:
    app.kubernetes.io/name: ${POD}
spec:
  restartPolicy: Never
  serviceAccountName: ${SA}
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 185
    runAsGroup: 185
    fsGroup: 185
    seccompProfile: { type: RuntimeDefault }
  containers:
    - name: workload
      image: ${image}
      command: ["/opt/spark/bin/spark-submit"]
      args:
        - --master
        - local[1]
        - --conf
        - spark.ui.enabled=false
        - --conf
        - spark.sql.shuffle.partitions=2
        - --conf
        - spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
        - /opt/spike/run.py
      env:
        - { name: HOME, value: /tmp }
        - { name: TOKEN_FILE, value: /var/run/secrets/stardelt-catalog/token }
        - { name: CATALOG_URI, value: "http://lakekeeper.${NS}.svc.cluster.local:8181" }
        - { name: S3_ENDPOINT, value: "http://seaweedfs-s3.${NS}.svc.cluster.local:8333" }
        - { name: BRANCH, value: "${BRANCH}" }
        - name: S3_ACCESS_KEY
          valueFrom: { secretKeyRef: { name: stardelt-s3-creds, key: access-key, optional: true } }
        - name: S3_SECRET_KEY
          valueFrom: { secretKeyRef: { name: stardelt-s3-creds, key: secret-key, optional: true } }
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: { drop: ["ALL"] }
      resources:
        requests: { cpu: 500m, memory: 1Gi }
        limits: { cpu: 2, memory: 3Gi }
      volumeMounts:
        - { name: catalog-token, mountPath: /var/run/secrets/stardelt-catalog, readOnly: true }
        - { name: script, mountPath: /opt/spike, readOnly: true }
  volumes:
    - name: catalog-token
      projected:
        sources:
          - serviceAccountToken:
              audience: ${AUDIENCE}
              expirationSeconds: 600
              path: token
    - name: script
      configMap: { name: ${POD}-script }
EOF
then
  started_s="$(date +%s)"
  # A loop rather than `kubectl wait`: a Failed pod (or a deleted one) must be reported at
  # once, not after the full timeout.
  phase=""
  missing=0
  while :; do
    phase="$(kubectl -n "$SA_NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    case "$phase" in Succeeded|Failed) break ;; esac
    if [ -z "$phase" ]; then
      missing=$(( missing + 1 ))
      if [ "$missing" -ge 6 ]; then break; fi
    else
      missing=0
    fi
    if [ $(( $(date +%s) - started_s )) -gt 1200 ]; then break; fi
    sleep 5
  done
  if [ "$phase" = "Succeeded" ]; then
    echo "L pod=succeeded after_s=$(( $(date +%s) - started_s ))"
  else
    echo "L pod=failed phase=${phase:-Missing} after_s=$(( $(date +%s) - started_s ))"
  fi
  logs="$(kubectl -n "$SA_NS" logs "$POD" 2>&1 || true)"
  printf '%s\n' "$logs" | grep -E '^(Q[0-9]|L )' > "$tmp/pod.txt" || true
  grep -E '^L ' "$tmp/pod.txt" || true
  grep -E '^Q' "$tmp/pod.txt" > "$tmp/q.txt" || true
  if ! grep -q '^Q1:' "$tmp/q.txt"; then
    echo "--- the pod did not reach Q1; last lines:" >&2
    printf '%s\n' "$logs" | grep -v -E '^[0-9]{2}/[0-9]{2}/[0-9]{2} ' | tail -n 20 >&2 || true
    exit 1
  fi
else
  echo "the workload pod could not be created" >&2
  exit 1
fi

# --- Q2 (catalog side): the refs and the branch snapshot, through the REST catalog ---------
code="$(mgmt GET "/catalog/v1/$WID_PLAY/namespaces/$SCHEMA/tables/$TABLE?snapshots=all")"
refs_json="$(jq -c '.metadata.refs // {}' "$tmp/out.json" 2>/dev/null || echo '{}')"
main_now="$(jq -r '.metadata.refs.main."snapshot-id" // "none"' "$tmp/out.json" 2>/dev/null || echo none)"
branch_now="$(jq -r --arg b "$BRANCH" '.metadata.refs[$b]."snapshot-id" // "none"' "$tmp/out.json" 2>/dev/null || echo none)"
if [ "$branch_now" = "none" ]; then
  added="n/a"
else
  added="$(jq -r --argjson snap "$branch_now" '
    [(.metadata.snapshots // [])[] | select(."snapshot-id" == $snap)][0].summary["added-data-files"] // "n/a"' \
    "$tmp/out.json" 2>/dev/null || echo n/a)"
fi
echo "Q2: catalog http=$code main-ref=$main_now refs=$refs_json branch-added-data-files=$added" >> "$tmp/q.txt"

# The branches the pod created, for `cleanup`; a branch that never appeared is not touched.
if [ "$branch_now" != "none" ]; then BRANCHES+=("$WID_PLAY $BRANCH"); fi
mgmt GET "/catalog/v1/$WID_GOLD/namespaces/$SCHEMA/tables/$TABLE?snapshots=all" >/dev/null
gold_branch="$(jq -r --arg b "$BRANCH" '.metadata.refs[$b]."snapshot-id" // "none"' "$tmp/out.json" 2>/dev/null || echo none)"
if [ "$gold_branch" != "none" ]; then BRANCHES+=("$WID_GOLD $BRANCH"); fi

# --- Q3 (Trino): can the `quality` task's runtime read the branch? ------------------------
public_url="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.publicUrl}' 2>/dev/null || true)"
TRINO_URL="${TRINO_URL:-https://trino.${public_url#https://}}"
[ -n "${TRINO_URL#https://trino.}" ] || TRINO_URL="https://trino.alpha.dev.stardelt.io"

cat > "$tmp/trino.py" <<'PY'
import base64, json, sys, urllib.error, urllib.request

url, mode, secret_file, sql = sys.argv[1:5]
headers = {"Content-Type": "text/plain"}
if mode == "basic":
    password = open(secret_file).read().strip()
    headers["Authorization"] = "Basic " + base64.b64encode(("superset:" + password).encode()).decode()
else:
    headers["Authorization"] = "Bearer " + open(secret_file).read().strip()


def call(endpoint, data=None):
    request = urllib.request.Request(endpoint, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, {"error": {"message": error.read().decode()[:300]}}


code, body = call(url.rstrip("/") + "/v1/statement", sql.encode())
rows = list(body.get("data") or [])
failure = body.get("error")
while body.get("nextUri"):
    code, body = call(body["nextUri"])
    rows.extend(body.get("data") or [])
    failure = failure or body.get("error")
print(json.dumps({"code": code, "rows": rows, "error": failure}))
PY

trino() { # <bearer> <secret-file> <sql> -> json line
  python3 "$tmp/trino.py" "$TRINO_URL" "$1" "$2" "$3"
}

mint_demo() {
  local issuer
  issuer="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}')"
  ISSUER="$issuer" USER=demo PASSWORD_FILE="$tmp/password" TOKEN_FILE="$tmp/demo-token" \
    python3 scripts/mint-user-token.py >/dev/null
}

printf '%s' "${DEMO_PASSWORD:-demo}" > "$tmp/password"
mint_demo 2>/dev/null || true
trino_version="$(trino bearer "$tmp/demo-token" 'SELECT version()' 2>/dev/null || true)"
if [ -n "$trino_version" ] && [ "$(jq -r '.code // 0' <<<"$trino_version")" = "401" ]; then
  mint_demo 2>/dev/null || true
  trino_version="$(trino bearer "$tmp/demo-token" 'SELECT version()' 2>/dev/null || true)"
fi

if [ -n "$trino_version" ] && [ -z "$(jq -r '.error // empty' <<<"$trino_version")" ]; then
  version="$(jq -r '.rows[0][0] // "unknown"' <<<"$trino_version")"
  if [ "$branch_now" != "none" ]; then
    by_version="$(trino bearer "$tmp/demo-token" \
      "SELECT count(*) FROM $PLAY.$SCHEMA.$TABLE FOR VERSION AS OF $branch_now")"
    by_version_rows="$(jq -r '.rows[0][0] // "refused"' <<<"$by_version")"
    by_version_error="$(jq -r '.error.message // empty' <<<"$by_version" | head -c 160)"
    by_name="$(trino bearer "$tmp/demo-token" \
      "SELECT count(*) FROM $PLAY.$SCHEMA.$TABLE.branch_$BRANCH")"
    by_name_rows="$(jq -r '.rows[0][0] // "refused"' <<<"$by_name")"
    by_name_error="$(jq -r '.error.message // empty' <<<"$by_name" | head -c 160)"
    {
      echo "Q3: trino-version=$version version-as-of-rows=$by_version_rows$([ -n "$by_version_error" ] && echo " error=$by_version_error")"
      echo "Q3: trino-table-branch-form-rows=$by_name_rows$([ -n "$by_name_error" ] && echo " error=$by_name_error")"
    } >> "$tmp/q.txt"
  else
    echo "Q3: trino-version=$version version-as-of=not-testable (no branch ref)" >> "$tmp/q.txt"
  fi
else
  echo "Q3: trino=not-testable (no statement answered: $(jq -r '.error.message // "no answer"' <<<"${trino_version:-{\}}" 2>/dev/null | head -c 160))" >> "$tmp/q.txt"
fi

# --- Q4: the publish ---------------------------------------------------------------------
# `main` as the run found it is the assert; the branch's snapshot is what `main` becomes.
mgmt GET "/catalog/v1/$WID_PLAY/namespaces/$SCHEMA/tables/$TABLE?snapshots=all" >/dev/null
main_req="$(jq -r '.metadata.refs.main."snapshot-id" // empty' "$tmp/out.json")"
branch_snap="$(jq -r --arg b "$BRANCH" '.metadata.refs[$b]."snapshot-id" // empty' "$tmp/out.json")"
if [ -z "$main_req" ] || [ -z "$branch_snap" ]; then
  echo "Q4: not-testable (main-ref=${main_req:-none} branch-ref=${branch_snap:-none})" >> "$tmp/q.txt"
else
  # The body #114 describes, exactly: the duplicate `type` key is part of the question.
  printf '%s\n' \
    "{\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":$main_req}],\"updates\":[{\"type\":\"set-snapshot-ref\",\"ref-name\":\"main\",\"snapshot-id\":$branch_snap,\"type\":\"branch\"}]}" \
    > "$tmp/publish-ticket.json"
  code="$(mgmt POST "/catalog/v1/$WID_PLAY/namespaces/$SCHEMA/tables/$TABLE" "$tmp/publish-ticket.json")"
  err="$(jq -c '.error // empty | {type: (.type // "none"), message: ((.message // "no message") | .[0:160])}' \
    "$tmp/out.json" 2>/dev/null || true)"
  if [ -z "$err" ]; then err="raw=$(head -c 200 "$tmp/out.json" | tr -d '\n')"; fi
  echo "Q4: ticket-body status=$code error=$err" >> "$tmp/q.txt"

  jq -n --argjson main "$main_req" --argjson branch "$branch_snap" '{
      requirements: [{type: "assert-ref-snapshot-id", ref: "main", "snapshot-id": $main}],
      updates: [{action: "set-snapshot-ref", "ref-name": "main", "snapshot-id": $branch, type: "branch"}]}' \
    > "$tmp/publish.json"
  code="$(mgmt POST "/catalog/v1/$WID_PLAY/namespaces/$SCHEMA/tables/$TABLE" "$tmp/publish.json")"
  new_main="$(jq -r '.metadata.refs.main."snapshot-id" // "none"' "$tmp/out.json" 2>/dev/null || echo none)"
  echo "Q4: publish action-form status=$code main-ref=$new_main (expected $branch_snap)" >> "$tmp/q.txt"

  # The stale assert: the value `main` had one commit ago. That answer is slice c's sentence.
  code="$(mgmt POST "/catalog/v1/$WID_PLAY/namespaces/$SCHEMA/tables/$TABLE" "$tmp/publish.json")"
  err="$(jq -c '.error // . | {type: (.type // .code // "none"), message: ((.message // "no message") | .[0:200])}' \
    "$tmp/out.json" 2>/dev/null || true)"
  echo "Q4b: stale-assert status=$code error=$err" >> "$tmp/q.txt"
fi

# --- Q5: the ref a failed run leaves behind, and the drop --------------------------------
if [ -n "${branch_snap:-}" ]; then
  jq -n --argjson snap "$branch_snap" --argjson ttl "$TTL_MS" --arg ref "$TTL_BRANCH" '{
      requirements: [],
      updates: [{action: "set-snapshot-ref", "ref-name": $ref, "snapshot-id": $snap,
                 type: "branch", "max-ref-age-ms": $ttl}]}' > "$tmp/ttl.json"
  code="$(mgmt POST "/catalog/v1/$WID_PLAY/namespaces/$SCHEMA/tables/$TABLE" "$tmp/ttl.json")"
  mgmt GET "/catalog/v1/$WID_PLAY/namespaces/$SCHEMA/tables/$TABLE?snapshots=all" >/dev/null
  ttl_value="$(jq -r --arg b "$TTL_BRANCH" '.metadata.refs[$b]["max-ref-age-ms"] // "absent"' "$tmp/out.json" 2>/dev/null || echo absent)"
  jq -n --arg ref "$TTL_BRANCH" \
    '{requirements: [], updates: [{action: "remove-snapshot-ref", "ref-name": $ref}]}' > "$tmp/drop-ref.json"
  drop_code="$(mgmt POST "/catalog/v1/$WID_PLAY/namespaces/$SCHEMA/tables/$TABLE" "$tmp/drop-ref.json")"
  mgmt GET "/catalog/v1/$WID_PLAY/namespaces/$SCHEMA/tables/$TABLE?snapshots=all" >/dev/null
  gone="$(jq -r --arg b "$TTL_BRANCH" 'if .metadata.refs[$b] then "no" else "yes" end' "$tmp/out.json" 2>/dev/null || echo unknown)"
  echo "Q5: ttl-branch=$TTL_BRANCH create=$code max-ref-age-ms=$ttl_value delete=$drop_code gone=$gone" >> "$tmp/q.txt"
else
  echo "Q5: not-testable (no branch ref)" >> "$tmp/q.txt"
fi

# --- Q6 (catalog side): the vended config for main and for the branch --------------------
main_load="$(load_table "$WID_GOLD" "$SCHEMA" "$TABLE" '?snapshots=all')"
main_ak="$(jq -r 'if (.config // {}) | has("s3.access-key-id") then "yes" else "no" end' "$tmp/out.json" 2>/dev/null || echo unknown)"
main_tok="$(jq -r 'if (.config // {}) | has("s3.session-token") then "yes" else "no" end' "$tmp/out.json" 2>/dev/null || echo unknown)"
branch_load="$(load_table "$WID_GOLD" "$SCHEMA" "$TABLE" "?ref=$BRANCH")"
branch_ak="$(jq -r 'if (.config // {}) | has("s3.access-key-id") then "yes" else "no" end' "$tmp/out.json" 2>/dev/null || echo unknown)"
branch_tok="$(jq -r 'if (.config // {}) | has("s3.session-token") then "yes" else "no" end' "$tmp/out.json" 2>/dev/null || echo unknown)"
bogus_load="$(load_table "$WID_GOLD" "$SCHEMA" "$TABLE" '?ref=does_not_exist')"
echo "Q6: load main http=$main_load access-key-id=$main_ak session-token=$main_tok; ref=$BRANCH http=$branch_load access-key-id=$branch_ak session-token=$branch_tok; ref=does_not_exist http=$bogus_load (same body => the parameter is ignored)" \
  >> "$tmp/q.txt"

# --- the answers, in order, then the lab lines -------------------------------------------
sort -s -V "$tmp/q.txt"

exit 0
