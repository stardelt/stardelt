#!/usr/bin/env bash
# Spike for stardelt/stardelt#991 (#114/#80 S2c-4): does a `spark-pipeline` graph write an
# Iceberg branch, and does the spec's `configuration:` carry the branch key to the server
# session?
#
# #820 measured the branch write for a plain `INSERT` from a local Spark
# (`spike-wap-branch.sh`): `spark.wap.branch` writes the branch only on a table with
# `write.wap.enabled=true`, and without that property the same call writes `main` silently.
# #957 Q2 measured that a spec's `configuration:` reaches the server session as dynamic keys,
# and is refused for static ones (`CANNOT_MODIFY_STATIC_CONFIG`). A `spark-pipeline` graph
# writes `CREATE MATERIALIZED VIEW` / `CREATE OR REPLACE TABLE`, not `INSERT`, and its key
# would arrive from `configuration:` — the combination nobody has measured. Four answers, one
# script, one pod, one table:
#
#   Q1  the key is accepted from the spec's `configuration:`: `spark.wap.branch: audit_spike`
#       in the pod's `/tmp` copy, beside `spark.remote` and the catalog block. `accepted` when
#       the CLI starts and the run begins; `refused <sentence>` when the server answers
#       `CANNOT_MODIFY_STATIC_CONFIG` or similar. A refusal ends the measurement.
#   Q2  a materialized view writes the branch, with `write.wap.enabled=true`. "The graph
#       writes the branch" means `main` did not move **and** the branch's snapshot did.
#   Q3  the control: without the table property the same graph writes `main`, silently.
#   Q4  what a check would see on the branch: the new branch snapshot's `added-records`, the
#       snapshot a `quality` task reads with `FOR VERSION AS OF <id>` (wap.py::resolve_table).
#   Q5  the publish (`assert-ref-snapshot-id` + `set-snapshot-ref`) moves `main`, and a second
#       run of the same graph against the new `main` completes.
#
# A refusal is a result, not a script failure: it prints its `L` line, cleans up and exits 0,
# as `spike-wap-branch.sh` does. The `L` lines print in the order the lab allows — Q3 drops
# Q2's table, so it is written and measured last.
#
# The lab, all reverted in `cleanup` on EXIT/INT/TERM:
#   - namespace `gold.spike_wap_graph` (created here; the REST catalog does not cascade on
#     DROP, so its tables are deleted first, then the namespace) and the `audit_spike` branch;
#   - ConfigMap `spike-wap-graph` and pod `spike-wap-graph` in `stardelt`;
#   - the token files the pod mints (they live in the pod's `/tmp`, and in the build plane's
#     mode-0600 scratch directory, which is removed).
# The graph is the one the ticket names, `CREATE MATERIALIZED VIEW range_ten AS SELECT id FROM
# range(10);`. The empty table Q2 and Q3 branch from comes from a provisioning copy of the same
# graph with `WHERE false`; a materialized view is replaced by the next run.
#
# The engine is the platform's own Spark Connect server, `spark-connect` in `stardelt`: the
# values a group's `SparkCluster` runs (`platform/manifests/spark.yaml`,
# `operator/crates/stardelt-operator/values/spark.yaml`) set `spark.sql.extensions`, and
# without them Spark refuses every branch write ("Must enable Iceberg extensions…", #820 Q1 —
# measured again here on the demo sandbox's `local[2]` engine, which has none). The pod
# carries the labels of a `spark-pipeline` task pod (`app.kubernetes.io/name:
# pipeline-spark-pipeline`): `spark-connect-ingress` admits it on 15002 and
# `pipeline-notebook-egress` gives it DNS, the server and the realm's public token endpoint
# (#977). A spike of the WAP wiring must not measure a refusal that only the lab's demo engine
# has.
#
# The identity is the principal `sp-pipeline-demo` (#957 Q3): its client-credentials token is
# minted inside the pod for the run and again by the build plane for the catalog calls. No
# token, key or secret value is printed; every pod line that leaves is redacted first.
#
# The branch name is the fixed `audit_spike`: the loader computes `audit_<16 hex of
# sha256(run id)>` (`demos/dags/stardelt_pipelines/wap.py::branch_name`) and this spike has no
# run id.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
CM_GRAPH=spike-wap-graph
POD=spike-wap-graph
CATALOG=gold
SCHEMA=spike_wap_graph
TABLE=range_ten
BRANCH=audit_spike
BUCKET=lakehouse
GROUP="${GROUP:-demo}"
PRINCIPAL_SECRET="sp-pipeline-$GROUP"
CONNECT_HOST="spark-connect.$NS.svc.cluster.local"
CONNECT_URL="sc://$CONNECT_HOST:15002"
RUN_TIMEOUT="${RUN_TIMEOUT:-300}"
TTL_MS="${TTL_MS:-604800000}"
LK_PORT="${LK_PORT:-18181}"
LK="http://127.0.0.1:$LK_PORT"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/wap-spark.XXXXXX")"
LK_PID=""
PREFIX=""
schema_created=no
cm_created=no
pod_created=no
schema_deleted=no
cm_deleted=no
pod_deleted=no

L() { printf 'L %s\n' "$*"; }

# One line out of multi-line text.
text() { tr '\n' ' ' | tr -s ' ' | cut -c1-300; }

# The first `name=value` line of a pod answer.
field() { sed -n "s/^$1=//p" <<<"$2" | head -n1; }

up() { # url -> true when anything but a transport failure answered
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

# The build plane's own catalog identity: the principal's client-credentials token, in a
# mode-0600 file. The pod mints its own; neither is printed.
mint_catalog_token() { # <out-file>
  local url
  url="$(kubectl -n "$NS" get cm stardelt-identity -o jsonpath='{.data.tokenEndpoint}')"
  [ -n "$url" ] || { echo "ConfigMap stardelt-identity has no tokenEndpoint" >&2; return 1; }
  curl -fsS -X POST "$url" \
    --data-urlencode grant_type=client_credentials \
    --data-urlencode client_id@"$tmp/client-id" \
    --data-urlencode client_secret@"$tmp/client-secret" \
    -o "$tmp/token.json" 2>/dev/null || { echo "the realm refused $PRINCIPAL_SECRET" >&2; return 1; }
  jq -r .access_token "$tmp/token.json" > "$1"
  chmod 600 "$1"
}

auth_ready() {
  mint_catalog_token "$tmp/token"
  printf 'Authorization: Bearer %s\n' "$(cat "$tmp/token")" > "$tmp/auth"
  chmod 600 "$tmp/auth"
}

catalog() { # <method> <path> [data-file] -> status code, body in $tmp/out.json
  local method="$1" path="$2" data="${3-}" code
  if [ -n "$data" ]; then
    code="$(curl -sS -H @"$tmp/auth" -H 'Content-Type: application/json' -X "$method" \
      -d @"$data" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
  else
    code="$(curl -sS -H @"$tmp/auth" -X "$method" "$LK$path" -o "$tmp/out.json" \
      -w '%{http_code}')" || code="000"
  fi
  echo "${code:-000}"
}

# One table read with every snapshot; the refs and the snapshot summaries come from one body.
table_get() { # -> status code, body in $tmp/out.json
  catalog GET "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables/$TABLE?snapshots=all"
}

# The Iceberg REST commit of one table (the shape wap.py::commit posts).
table_commit() { # <json-file> -> status code, body in $tmp/out.json
  catalog POST "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables/$TABLE" "$1"
}

ref_of() { # <ref> -> snapshot id of that ref, or `none`, from $tmp/out.json
  jq -r --arg n "$1" '.metadata.refs[$n]."snapshot-id" // "none"' "$tmp/out.json" 2>/dev/null \
    || echo none
}

set_properties() { # <json object of updates> -> status code
  jq -n --argjson updates "$1" \
    '{requirements: [], updates: [{action: "set-properties", updates: $updates}]}' \
    > "$tmp/set-properties.json"
  table_commit "$tmp/set-properties.json"
}

create_branch() { # <snapshot id> -> status code
  jq -n --arg ref "$BRANCH" --argjson snap "$1" --argjson ttl "$TTL_MS" '{
      requirements: [],
      updates: [{action: "set-snapshot-ref", "ref-name": $ref, "snapshot-id": $snap,
                 type: "branch", "max-ref-age-ms": $ttl}]}' > "$tmp/branch.json"
  table_commit "$tmp/branch.json"
}

# shellcheck disable=SC2329  # called from the trap
drop_schema() { # fresh authorisation, delete every table, then the namespace
  local name code tables_ok=yes
  if ! forward_lk; then
    echo "lakekeeper did not answer; the schema stays" >&2
    return 1
  fi
  auth_ready
  PREFIX="$(curl -sS -H @"$tmp/auth" "$LK/catalog/v1/config?warehouse=$CATALOG" \
    | jq -r '.defaults.prefix // empty' 2>/dev/null || true)"
  [ -n "$PREFIX" ] || return 1
  code="$(catalog GET "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables")"
  if [ "$code" = "200" ]; then
    for name in $(jq -r '.identifiers[]?.name' "$tmp/out.json" 2>/dev/null); do
      code="$(catalog DELETE "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables/$name?purgeRequested=true")"
      L "cleanup drop-table=$SCHEMA.$name code=$code"
      [ "$code" = "204" ] || [ "$code" = "200" ] || tables_ok=no
    done
    [ "$tables_ok" = "yes" ] || return 1
  fi
  code="$(catalog DELETE "/catalog/v1/$PREFIX/namespaces/$SCHEMA")"
  L "cleanup drop-schema=$CATALOG.$SCHEMA code=$code"
  [ "$code" = "204" ] || [ "$code" = "200" ]
}

# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  set +e
  if [ "$schema_created" = "yes" ]; then
    if drop_schema; then schema_deleted=yes; fi
  fi
  if [ "$pod_created" = "yes" ]; then
    if kubectl -n "$NS" delete pod "$POD" --ignore-not-found --timeout=150s >/dev/null 2>&1; then
      pod_deleted=yes
    fi
    if [ "$(kubectl -n "$NS" get pod "$POD" --ignore-not-found -o name 2>/dev/null | wc -l)" = "0" ]; then
      pod_deleted=yes
    fi
  fi
  if [ "$cm_created" = "yes" ]; then
    if kubectl -n "$NS" delete configmap "$CM_GRAPH" --ignore-not-found >/dev/null 2>&1; then
      cm_deleted=yes
    fi
  fi
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  rm -f "$tmp/token" "$tmp/auth" "$tmp/client-id" "$tmp/client-secret" "$tmp/token.json"
  L "cleanup schema-deleted=$schema_deleted cm-deleted=$cm_deleted pod-deleted=$pod_deleted"
  rm -rf "$tmp"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for tool in kubectl curl jq python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

# --- the pinned image, the engine, the identity endpoint ------------------------------------
image="$(awk '/^ *image: ghcr\.io\/stardelt\/spark:/{print $2; exit}' manifests/spark.yaml)"
if [ -z "$image" ]; then
  echo "manifests/spark.yaml names no ghcr.io/stardelt/spark image" >&2
  exit 1
fi
L "image=$image"

engine_pod="$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=spark-connect \
  -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}' 2>/dev/null || true)"
if [ -z "$engine_pod" ]; then
  echo "no Running spark-connect pod in $NS" >&2
  exit 1
fi
L "engine=$engine_pod connect=$CONNECT_URL"

token_url="$(kubectl -n "$NS" get cm stardelt-identity -o jsonpath='{.data.tokenEndpoint}')"
[ -n "$token_url" ] || { echo "ConfigMap stardelt-identity has no tokenEndpoint" >&2; exit 1; }
kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" -o jsonpath='{.data.client-id}' \
  | base64 -d > "$tmp/client-id" || { echo "Secret $PRINCIPAL_SECRET has no client-id" >&2; exit 1; }
kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" -o jsonpath='{.data.client-secret}' \
  | base64 -d > "$tmp/client-secret" || { echo "Secret $PRINCIPAL_SECRET has no client-secret" >&2; exit 1; }
chmod 600 "$tmp"/*
L "identity=$PRINCIPAL_SECRET keys=client-id,client-secret (secretKeyRef in the pod)"

if ! forward_lk; then
  echo "lakekeeper did not answer within 30s" >&2
  exit 1
fi
auth_ready
PREFIX="$(curl -sS -H @"$tmp/auth" "$LK/catalog/v1/config?warehouse=$CATALOG" \
  | jq -r '.defaults.prefix // empty' 2>/dev/null || true)"
[ -n "$PREFIX" ] || { echo "the catalog config named no warehouse prefix" >&2; exit 1; }
code="$(catalog GET "/catalog/v1/$PREFIX/namespaces/$SCHEMA")"
if [ "$code" = "200" ]; then
  echo "namespace $SCHEMA already exists; refusing to touch someone's data" >&2
  exit 1
fi
printf '{"namespace":["%s"],"properties":{}}' "$SCHEMA" > "$tmp/namespace.json"
code="$(catalog POST "/catalog/v1/$PREFIX/namespaces" "$tmp/namespace.json")"
if [ "$code" = "200" ] || [ "$code" = "204" ]; then schema_created=yes; fi
L "schema-create=$CATALOG.$SCHEMA code=$code"

# --- the two graphs (one ConfigMap) --------------------------------------------------------
# `provision` makes the empty table the branch is taken from; `graph` is the ticket's
# measurement graph, whose spec carries `spark.wap.branch`. Two files rather than one so the
# provisioning run does not create the branch itself (#820: `spark.wap.branch` creates a
# missing branch).
spec() { # <wap key: yes|no> -> a spec on stdout
  cat <<EOF
name: spike-wap-graph
# The catalog's own storage is s3://<catalog>/<namespace-uuid>/: "gold" is the catalog and
# prefix, not a bucket.
storage: s3a://$BUCKET/pipelines/spike-wap-graph/checkpoints
catalog: $CATALOG
database: $SCHEMA
configuration:
  spark.remote: "$CONNECT_URL"
$(if [ "$1" = "yes" ]; then echo "  spark.wap.branch: \"$BRANCH\""; fi)
  spark.sql.catalog.$CATALOG: org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.$CATALOG.type: rest
  spark.sql.catalog.$CATALOG.rest-catalog-purge: "true"
  spark.sql.catalog.$CATALOG.uri: http://lakekeeper.$NS.svc.cluster.local:8181/catalog
  spark.sql.catalog.$CATALOG.warehouse: $CATALOG
  # <<token>>
  spark.sql.catalog.$CATALOG.io-impl: org.apache.iceberg.aws.s3.S3FileIO
  spark.sql.catalog.$CATALOG.header.X-Iceberg-Access-Delegation: vended-credentials
  spark.sql.catalog.$CATALOG.s3.endpoint: http://seaweedfs-s3.$NS.svc.cluster.local:8333
  spark.sql.catalog.$CATALOG.s3.region: us-east-1
  spark.sql.catalog.$CATALOG.s3.path-style-access: "true"
libraries:
  - glob:
      include: sql/**
EOF
}

mkdir -p "$tmp/graph/sql" "$tmp/provision/sql"
spec yes > "$tmp/graph/spark-pipeline.yml"
spec no > "$tmp/provision/spark-pipeline.yml"
cat > "$tmp/graph/sql/range_ten.sql" <<'EOF'
CREATE MATERIALIZED VIEW range_ten AS SELECT id FROM range(10);
EOF
cat > "$tmp/provision/sql/range_ten.sql" <<'EOF'
CREATE MATERIALIZED VIEW range_ten AS SELECT id FROM range(10) WHERE false;
EOF

kubectl -n "$NS" create configmap "$CM_GRAPH" \
  --from-file=graph.yml="$tmp/graph/spark-pipeline.yml" \
  --from-file=graph_sql.sql="$tmp/graph/sql/range_ten.sql" \
  --from-file=provision.yml="$tmp/provision/spark-pipeline.yml" \
  --from-file=provision_sql.sql="$tmp/provision/sql/range_ten.sql" \
  --dry-run=client -o yaml 2>/dev/null \
  | kubectl apply -f - >/dev/null && cm_created=yes
L "graph=ConfigMap $CM_GRAPH keys=4 (graph.yml, graph_sql.sql, provision.yml, provision_sql.sql); the sql/ tree mounts through items[].path"

# --- the pod ------------------------------------------------------------------------------
# `app.kubernetes.io/name: pipeline-spark-pipeline` is the task pod's own label: both
# `spark-connect-ingress` (platform/manifests/spark.yaml) and `pipeline-notebook-egress`
# select it (#977). The principal's Secret reaches the pod through `secretKeyRef` only.
cat > "$tmp/pod.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  namespace: $NS
  labels:
    app.kubernetes.io/name: pipeline-spark-pipeline
    app.kubernetes.io/part-of: stardelt
    stardelt.io/spike: s991
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  imagePullSecrets:
    - name: ghcr-creds
  securityContext:
    runAsNonRoot: true
    runAsUser: 185
    runAsGroup: 185
    fsGroup: 185
    seccompProfile: { type: RuntimeDefault }
  containers:
    - name: cli
      image: $image
      imagePullPolicy: IfNotPresent
      command: ["sleep", "7200"]
      env:
        - { name: HOME, value: /tmp }
        - { name: SPARK_HOME, value: /opt/spark }
        - { name: PIPELINE_TOKEN_URL, value: "$token_url" }
        - name: PIPELINE_CLIENT_ID
          valueFrom:
            secretKeyRef: { name: $PRINCIPAL_SECRET, key: client-id }
        - name: PIPELINE_CLIENT_SECRET
          valueFrom:
            secretKeyRef: { name: $PRINCIPAL_SECRET, key: client-secret }
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: { drop: ["ALL"] }
      resources:
        requests: { cpu: 250m, memory: 1Gi }
        limits: { cpu: "2", memory: 3Gi }
      volumeMounts:
        - { name: graph, mountPath: /opt/graph, readOnly: true }
        - { name: tmp, mountPath: /tmp }
  volumes:
    - name: graph
      configMap:
        name: $CM_GRAPH
        items:
          - { key: graph.yml, path: graph/spark-pipeline.yml }
          - { key: graph_sql.sql, path: graph/sql/range_ten.sql }
          - { key: provision.yml, path: provision/spark-pipeline.yml }
          - { key: provision_sql.sql, path: provision/sql/range_ten.sql }
    - name: tmp
      emptyDir: {}
EOF
if kubectl apply -f "$tmp/pod.yaml" >/dev/null 2>&1; then pod_created=yes; fi
if ! kubectl -n "$NS" wait --for=condition=Ready "pod/$POD" --timeout=300s >/dev/null 2>&1; then
  echo "pod $POD did not become ready" >&2
  kubectl -n "$NS" describe pod "$POD" 2>/dev/null | tail -n 20 >&2 || true
  exit 1
fi
L "pod=$POD labels=app.kubernetes.io/name=pipeline-spark-pipeline (the task pod's shape)"
L "versions $(kubectl -n "$NS" exec "$POD" -- bash -c 'sed -n 1p /opt/spark/RELEASE; ls /opt/spark/jars | grep -oE "spark-pipelines_2.13-[0-9.]+" | head -n1' 2>&1 | text)"

# --- the in-pod run script ----------------------------------------------------------------
# One script per run, piped to `bash -s` with `<tree> <tag> <timeout>`: mint a token (0600),
# copy the graph tree, write the token into the spec's `/tmp` copy, build the client conf
# (without `spark.master` and `spark.connect.grpc.binding.*`, which the server owns) and run
# the CLI. Everything it prints is redacted, so no token reaches this transcript.
cat > "$tmp/run.sh" <<'INNER'
set -euo pipefail
umask 077
tree="$1"; tag="$2"; limit="$3"
work=/tmp/s991
dir="$work/run-$tag"
rm -rf "$dir"; mkdir -p "$dir"
cp -rL "/opt/graph/$tree/." "$dir"/
chmod -R u+w "$dir"

# A pod of the pinned image ships no /opt/spark/conf at all (the sandbox's file comes from its
# Deployment's ConfigMap); when a conf dir is present, the keys the server owns are dropped.
rm -rf "$work/conf"; mkdir -p "$work/conf"
if [ -d /opt/spark/conf ]; then
  cp -rL /opt/spark/conf/. "$work/conf"/
  if [ -f "$work/conf/spark-defaults.conf" ]; then
    { grep -vE '^[[:space:]]*(spark\.master|spark\.connect\.grpc\.binding\.)' \
        "$work/conf/spark-defaults.conf" || true; } > "$work/conf/conf.tmp"
    mv "$work/conf/conf.tmp" "$work/conf/spark-defaults.conf"
  fi
fi

# The engine's CA is in the image and is not the realm's; the client-credentials POST goes to
# the realm's own endpoint (#957's mint, unchanged).
python3 - "$work/token" <<'PY'
import json, os, ssl, sys, urllib.parse, urllib.request
for name in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
    os.environ.pop(name, None)
body = urllib.parse.urlencode(
    {
        "grant_type": "client_credentials",
        "client_id": os.environ["PIPELINE_CLIENT_ID"],
        "client_secret": os.environ["PIPELINE_CLIENT_SECRET"],
    }
).encode()
request = urllib.request.Request(
    os.environ["PIPELINE_TOKEN_URL"],
    data=body,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
)
with urllib.request.urlopen(request, timeout=30, context=ssl.create_default_context()) as response:
    payload = json.load(response)
handle = os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.write(handle, payload["access_token"].encode())
os.close(handle)
print("token-minted expires_in=%s" % payload.get("expires_in", "unknown"))
PY

python3 - "$dir/spark-pipeline.yml" "$work/token" <<'PY'
import os, sys
path, token_path = sys.argv[1], sys.argv[2]
token = open(token_path).read().strip()
out = []
for line in open(path).read().splitlines():
    if line.strip() == "# <<token>>":
        out.append('  spark.sql.catalog.gold.token: "%s"' % token)
        continue
    out.append(line)
open(path, "w", newline="\n").write("\n".join(out) + "\n")
PY

echo "spec-wap-branch=$(grep -c '^  spark\.wap\.branch:' "$dir/spark-pipeline.yml" || true) token-in-spec=present"
start="$(date +%s%3N)"
set +e
SPARK_CONF_DIR="$work/conf" timeout "$limit" \
  /opt/spark/bin/spark-pipelines run --spec "$dir/spark-pipeline.yml" > "$work/$tag.log" 2>&1
code=$?
set -e
end="$(date +%s%3N)"
echo "run exit=$code ms=$((end-start))"

# The answers, from the log; the token is replaced before anything is printed.
python3 - "$work/$tag.log" "$work/token" <<'PY'
import sys
log = open(sys.argv[1], errors="replace").read()
try:
    token = open(sys.argv[2]).read().strip()
except OSError:
    token = ""
if token:
    log = log.replace(token, "<token>")
lines = [line.strip() for line in log.splitlines()]
static = [
    line for line in lines
    if "CANNOT_MODIFY_STATIC_CONFIG" in line or "Cannot modify the value of a static config" in line
]
started = any(word in line for line in lines for word in
              ("Creating Spark session", "Loading pipeline spec", "Run is COMPLETED"))
completed = any("Run is COMPLETED" in line for line in lines)
print("flows=%d" % sum(1 for line in lines if "has COMPLETED" in line))
print("started=%s" % ("yes" if started else "no"))
print("completed=%s" % ("yes" if completed else "no"))
print("static=%s" % (static[0][:240] if static else "none"))
interesting = (
    "Run is COMPLETED", "has COMPLETED", "CANNOT_MODIFY_STATIC_CONFIG",
    "Not authorized", "Unauthorized", "401", "TokenExpired", "expired",
    "Exception", "Error:", "Error ",
)
hits = [line for line in lines if any(word in line for word in interesting)]
for line in hits[:6]:
    print("note: " + line[:240])
plain = [line for line in lines if line and not line.startswith("at ")]
print("last: " + (plain[-1][:240] if plain else ""))
PY
INNER
chmod 600 "$tmp/run.sh"

pod_run() { # <tree> <tag> [timeout]
  local tree="$1" tag="$2" limit="${3-$RUN_TIMEOUT}"
  kubectl -n "$NS" exec -i "$POD" -- bash -s -- "$tree" "$tag" "$limit" < "$tmp/run.sh"
}

# --- Q2 setup: the empty table, the property, the branch ----------------------------------
prov="$(pod_run provision provision-q2 2>&1 || true)"
L "q2-provision $(grep -E '^(run|flows) ' <<<"$prov" | tr '\n' ' ' | text)"
if [ "$(field flows "$prov")" != "1" ]; then
  L "q2-setup=refused last=\"$(field last "$prov" | text)\""
  exit 0
fi
code="$(table_get)"
main_before="$(ref_of main)"
if [ "$code" != "200" ] || [ "$main_before" = "none" ]; then
  L "q2-setup=failed table-read=$code main=$main_before"
  exit 0
fi
code="$(set_properties '{"write.wap.enabled": "true"}')"
L "q2-write-wap-enabled code=$code"
code="$(create_branch "$main_before")"
L "q2-branch-create ref=$BRANCH snapshot=$main_before code=$code"
code="$(table_get)"
branch_before="$(ref_of "$BRANCH")"
L "q2-before main=$main_before branch=$branch_before table-read=$code"

# --- Q2: the graph run --------------------------------------------------------------------
q2="$(pod_run graph q2 2>&1 || true)"
code="$(table_get)"
main_after="$(ref_of main)"
branch_after="$(ref_of "$BRANCH")"
q2_wrote_main=no
[ "$main_after" != "$main_before" ] && q2_wrote_main=yes
q2_wrote_branch=no
[ "$branch_after" != "$branch_before" ] && q2_wrote_branch=yes
# Q1: the same run, whose spec carries `spark.wap.branch` in `configuration:` — the answer
# the refusal would end the measurement with is printed first.
q1_static="$(field static "$q2")"
if [ -n "$q1_static" ] && [ "$q1_static" != "none" ]; then
  L "q1-config refused $(printf '%s' "$q1_static" | text)"
elif [ "$(field started "$q2")" = "yes" ] || [ "$(field flows "$q2" | head -n1)" != "0" ]; then
  L "q1-config accepted"
else
  L "q1-config unknown run-exit=$(field run "$q2" | text) last=\"$(field last "$q2" | text)\""
fi
L "q2-main-before=$main_before q2-main-after=$main_after q2-main-wrote=$q2_wrote_main"
L "q2-branch-before=$branch_before q2-branch-after=$branch_after q2-branch-wrote=$q2_wrote_branch"
L "q2-run $(grep -E '^run ' <<<"$q2") flows=$(field flows "$q2") completed=$(field completed "$q2")"
printf '%s\n' "$q2" | grep -E '^(note|last):' | sed 's/^/  · /' || true

# --- Q4: the branch snapshot a `quality` task would read ----------------------------------
if [ "$q2_wrote_branch" = "yes" ] && [ "$q2_wrote_main" = "no" ]; then
  rows="$(jq -r --argjson snap "$branch_after" '
    [(.metadata.snapshots // [])[] | select(."snapshot-id" == $snap)][0].summary["added-records"]
    // "absent"' "$tmp/out.json" 2>/dev/null || echo absent)"
  L "q4-branch-snapshot=$branch_after q4-branch-rows=$rows"
else
  L "q4-branch-snapshot=none q4-branch-rows=n/a (Q2 did not write the branch)"
fi

# --- Q5: the publish, then a second run ---------------------------------------------------
jq -n --argjson main "$main_before" --argjson snap "$branch_after" '{
    requirements: [{type: "assert-ref-snapshot-id", ref: "main", "snapshot-id": $main}],
    updates: [{action: "set-snapshot-ref", "ref-name": "main", "snapshot-id": $snap,
               type: "branch"}]}' > "$tmp/publish.json"
code="$(table_commit "$tmp/publish.json")"
published="$(ref_of main)"
L "q5-published main-before=$main_before main-after=$published moved=$([ "$published" = "$branch_after" ] && echo yes || echo no) code=$code"

code="$(create_branch "$published")"
L "q5-branch-at-new-main ref=$BRANCH snapshot=$published code=$code"
q5="$(pod_run graph q5 2>&1 || true)"
if [ "$(field completed "$q5")" = "yes" ]; then
  L "q5-second-run=COMPLETED flows=$(field flows "$q5")"
else
  L "q5-second-run=failed flows=$(field flows "$q5") last=\"$(field last "$q5" | text)\""
fi

# --- Q3: the control — no `write.wap.enabled`, the same graph -----------------------------
code="$(catalog DELETE "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables/$TABLE?purgeRequested=true")"
L "q3-table-drop code=$code"
prov="$(pod_run provision provision-q3 2>&1 || true)"
L "q3-provision $(grep -E '^(run|flows) ' <<<"$prov" | tr '\n' ' ' | text)"
code="$(table_get)"
q3_main_before="$(ref_of main)"
if [ "$code" = "200" ] && [ "$q3_main_before" != "none" ]; then
  code="$(create_branch "$q3_main_before")"
  L "q3-branch-create ref=$BRANCH snapshot=$q3_main_before code=$code"
  code="$(table_get)"
  q3_branch_before="$(ref_of "$BRANCH")"
  q3="$(pod_run graph q3 2>&1 || true)"
  code="$(table_get)"
  q3_main_after="$(ref_of main)"
  q3_branch_after="$(ref_of "$BRANCH")"
  L "q3-main-before=$q3_main_before q3-main-after=$q3_main_after q3-wrote-main=$([ "$q3_main_after" != "$q3_main_before" ] && echo yes || echo no)"
  L "q3-branch-before=$q3_branch_before q3-branch-after=$q3_branch_after q3-branch-wrote=$([ "$q3_branch_after" != "$q3_branch_before" ] && echo yes || echo no)"
  L "q3-run $(grep -E '^run ' <<<"$q3") flows=$(field flows "$q3") completed=$(field completed "$q3")"
else
  L "q3-main-before=none q3-main-after=none q3-wrote-main=n/a (the provisioning run failed)"
  L "q3-branch-before=none q3-branch-after=none q3-branch-wrote=n/a"
fi

exit 0
