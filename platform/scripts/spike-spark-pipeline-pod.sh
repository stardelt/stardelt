#!/usr/bin/env bash
# Spike for stardelt/stardelt#894 (#80 S2a): where a `spark-pipeline` run gets its engine and
# where its files come from.
#
# #731 (S0) measured the `spark-pipelines` CLI **inside the demo sandbox pod** (local files,
# local Spark, loopback to the pod's own Connect server). A `spark-pipeline` task of the
# platform runs in a pod of its own, like the `notebook` task's
# (`demos/dags/stardelt_pipelines/notebook.py`), and carries neither. This run measures the
# facts S2b needs, each with its own log line:
#
#   Q1  the invocation. The 4.1.4 CLI has no `--remote` (only `--spec`, `--refresh`,
#       `--full-refresh`, `--full-refresh-all`), and the pod's ConfigMap sets
#       `spark.master local[2]`, so the CLI dies at submission ("Remote cannot be specified
#       with master and/or deploy mode"). With a `SPARK_CONF_DIR` copy without
#       `spark.master`:
#         - `spark.remote: sc://127.0.0.1:15002` in the spec's own `configuration:` makes the
#           run go through the running Connect server (`spark.sql.extensions` in that block is
#           refused with `CANNOT_MODIFY_STATIC_CONFIG`, which is the proof that the session is
#           the server's and not a local one);
#         - without a remote the CLI starts a Spark of its own in the client JVM. It works if
#           the client's Connect port is moved (the server already owns 15002).
#   Q2  a second pod, the loader's shape: a pod in `stardelt` on the image pinned in
#       `manifests/spark.yaml`, the graph from a ConfigMap, no token and no Secret. The
#       sandbox's NetworkPolicy refuses the Connect Service in 0.0 s, and the CLI then waits
#       with no error at all at "Creating Spark session..." (90 s timeout). With the visitor
#       labels the channel opens and the catalog refuses (`Missing Authorization Header`).
#   Q3  the transport. The CLI writes **no file** beside the spec, so a read-only ConfigMap
#       mount carries a batch graph. A ConfigMap key cannot contain `/`, so the `sql/` tree
#       mounts through `items[].path`. A ten-file graph is ~1.1 kB, far below the 1 MiB limit.
#   Q4  `dry-run` from the pod: wall time, exit code, and the sentence for a missing table.
#
# The graph is #731's shape: two `CREATE MATERIALIZED VIEW IF NOT EXISTS` SQL files over
# `range(1)`, `libraries: sql/**`, catalog `playground`, schema `spike_s2`.
#
# The catalog identity in the sandbox is #731's: a client-credentials token for
# `svc-catalog-spark` (`aud: lakekeeper`) plus the platform S3 key from Secret
# `stardelt-s3-creds` (playground vends nothing, `sts-enabled: false`). Both go into the spec
# written inside the sandbox pod's `/tmp` with mode 0600 and are never printed; the token lives
# five minutes, so it is minted again before every run.
#
# The lab, all reverted in `cleanup` on EXIT/INT/TERM:
#   - schema `playground.spike_s2` (created here; the REST catalog does not cascade on DROP,
#     so the tables are deleted first, then the namespace);
#   - ConfigMaps `spike-s2-graph` and `spike-s2-ten`, pods `spike-s2-second` and
#     `spike-s2-notebook` in `stardelt`;
#   - the sandbox pod's `/tmp/s2-*` scratch this run created.
# The sandbox Deployment stays as it is.
#
# It prints no token, key or secret: names, status codes, timings, row values only.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
DEMO_NS="${DEMO_NS:-stardelt-demo-spark}"
CM_GRAPH=spike-s2-graph
CM_TEN=spike-s2-ten
POD=spike-s2-second
POD_VISITOR=spike-s2-notebook
SCHEMA=spike_s2
CATALOG=playground
BUCKET=lakehouse
LK_PORT="${LK_PORT:-18181}"
LK="http://127.0.0.1:$LK_PORT"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/s2-pod.XXXXXX")"
LK_PID=""
PREFIX=""
sandbox_pod=""
schema_created=no
cm_created=no
cm_ten_created=no
pods_created=no
sandbox_scratch=no

cm_deleted=no
pods_deleted=no
schema_deleted=no

L() { printf 'L %s\n' "$*"; }

# --- catalog helpers ----------------------------------------------------------------------
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

# A client-credentials token for svc-catalog-spark (#731's identity), in a 0600 file.
mint_catalog_token() { # <out-file>
  local issuer
  issuer="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}')"
  [ -n "$issuer" ] || { echo "ConfigMap stardelt-identity has no issuerUrl" >&2; exit 1; }
  if ! curl -fsS -X POST "${issuer%/}/protocol/openid-connect/token" \
      --data-urlencode grant_type=client_credentials \
      --data-urlencode client_id@"$tmp/client-id" \
      --data-urlencode client_secret@"$tmp/client-secret" \
      -o "$tmp/token.json" 2>/dev/null; then
    echo "the realm refused svc-catalog-spark's client credentials" >&2
    exit 1
  fi
  jq -r .access_token "$tmp/token.json" > "$1"
  chmod 600 "$1"
}

auth_ready() { # mint a token and turn it into a header file for the catalog calls
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

# --- sandbox helpers ----------------------------------------------------------------------
sandbox_sh() { kubectl -n "$DEMO_NS" exec "$sandbox_pod" -- bash -lc "$1"; }

put_in_sandbox() { # <remote-path> <local-file>
  kubectl -n "$DEMO_NS" exec -i "$sandbox_pod" -- \
    bash -lc "umask 077; mkdir -p \"\$(dirname '$1')\"; cat > '$1'" < "$2"
}

# A millisecond wall time around a command whose output goes to <log>; sets RUN_EXIT, RUN_MS.
RUN_EXIT=0
RUN_MS=0
timed() { # <log> <command...>
  local log="$1" start end
  shift
  start="$(date +%s%3N)"
  set +e
  "$@" > "$log" 2>&1
  RUN_EXIT=$?
  set -e
  end="$(date +%s%3N)"
  RUN_MS=$(( end - start ))
}

# The spec: `configuration:` with the catalog and, optionally, the remote. `python3` reads the
# token and the S3 keys from the environment, so no value ever reaches an argv.
write_spec() { # <remote-url|local> <storage> <out> [static] [credentials]
  local remote="$1" storage="$2" out="$3" static="${4-no}" creds="${5-yes}"
  SPEC_REMOTE="$remote" SPEC_STORAGE="$storage" SPEC_STATIC="$static" SPEC_CREDS="$creds" \
  SPEC_CATALOG="$CATALOG" SPEC_SCHEMA="$SCHEMA" \
  TOKEN="$(cat "$tmp/token")" S3_AK="$(cat "$tmp/ak")" S3_SK="$(cat "$tmp/sk")" \
    python3 - > "$out" <<'PY'
import os

lines = [
    "name: spike-s2",
    f"storage: {os.environ['SPEC_STORAGE']}",
    f"catalog: {os.environ['SPEC_CATALOG']}",
    f"database: {os.environ['SPEC_SCHEMA']}",
    "configuration:",
]
if os.environ["SPEC_REMOTE"] != "local":
    lines.append(f'  spark.remote: "{os.environ["SPEC_REMOTE"]}"')
if os.environ["SPEC_STATIC"] == "yes":
    lines.append(
        "  spark.sql.extensions: "
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
lines += [
    "  spark.sql.catalog.playground: org.apache.iceberg.spark.SparkCatalog",
    "  spark.sql.catalog.playground.type: rest",
    "  spark.sql.catalog.playground.uri: "
    "http://lakekeeper.stardelt.svc.cluster.local:8181/catalog",
    "  spark.sql.catalog.playground.warehouse: playground",
]
if os.environ["SPEC_CREDS"] == "yes":
    lines += [
        f'  spark.sql.catalog.playground.token: "{os.environ["TOKEN"]}"',
        "  spark.sql.catalog.playground.io-impl: org.apache.iceberg.aws.s3.S3FileIO",
        "  spark.sql.catalog.playground.s3.endpoint: "
        "http://seaweedfs-s3.stardelt.svc.cluster.local:8333",
        "  spark.sql.catalog.playground.s3.region: us-east-1",
        '  spark.sql.catalog.playground.s3.path-style-access: "true"',
        f'  spark.sql.catalog.playground.s3.access-key-id: "{os.environ["S3_AK"]}"',
        f'  spark.sql.catalog.playground.s3.secret-access-key: "{os.environ["S3_SK"]}"',
    ]
lines += [
    "libraries:",
    "  - glob:",
    "      include: sql/**",
    "",
]
print("\n".join(lines))
PY
  chmod 600 "$out"
}

write_graph() { # <dir> [<mv_two SQL>]
  local dir="$1"
  local second="${2-CREATE MATERIALIZED VIEW IF NOT EXISTS mv_two AS SELECT id + 1 AS id FROM mv_one;}"
  mkdir -p "$dir/sql"
  printf 'CREATE MATERIALIZED VIEW IF NOT EXISTS mv_one AS SELECT id FROM range(1);\n' \
    > "$dir/sql/mv_one.sql"
  printf '%s\n' "$second" > "$dir/sql/mv_two.sql"
}

# --- cleanup ------------------------------------------------------------------------------
# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  set +e
  if [ "$schema_created" = "yes" ]; then
    if drop_schema; then schema_deleted=yes; fi
  fi
  if [ "$pods_created" = "yes" ]; then
    if kubectl -n "$NS" delete pod "$POD" "$POD_VISITOR" --ignore-not-found \
        --timeout=60s >/dev/null 2>&1; then pods_deleted=yes; fi
  fi
  if [ "$cm_created" = "yes" ] || [ "$cm_ten_created" = "yes" ]; then
    if kubectl -n "$NS" delete configmap "$CM_GRAPH" "$CM_TEN" --ignore-not-found \
        >/dev/null 2>&1; then cm_deleted=yes; fi
  fi
  if [ "$sandbox_scratch" = "yes" ] && [ -n "$sandbox_pod" ]; then
    # Only the names this script made.
    if sandbox_sh 'chmod -R u+w /tmp/s2-graph /tmp/s2-graph-local /tmp/s2-graph-missing /tmp/s2-graph-static /tmp/s2-conf /tmp/s2-conf-local /tmp/s2-storage 2>/dev/null; rm -rf /tmp/s2-graph /tmp/s2-graph-local /tmp/s2-graph-missing /tmp/s2-graph-static /tmp/s2-conf /tmp/s2-conf-local /tmp/s2-storage /tmp/s2-remote.log /tmp/s2-local.log /tmp/s2-stock.log /tmp/s2-static.log /tmp/s2-dry-good.log /tmp/s2-dry-missing.log' >/dev/null 2>&1; then
      L "cleanup sandbox-scratch=removed"
    else
      L "cleanup sandbox-scratch=failed"
    fi
  fi
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  L "cleanup schema-deleted=$schema_deleted cm-deleted=$cm_deleted pods-deleted=$pods_deleted"
  rm -rf "$tmp"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for tool in kubectl curl jq python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

# --- the pinned image, the sandbox, the catalog identity ----------------------------------
image="$(awk '/^ *image: ghcr\.io\/stardelt\/spark:/{print $2; exit}' manifests/spark.yaml)"
if [ -z "$image" ]; then
  echo "manifests/spark.yaml names no ghcr.io/stardelt/spark image" >&2
  exit 1
fi
L "image=$image"

sandbox_pod="$(kubectl -n "$DEMO_NS" get pod -l app.kubernetes.io/name=spark-demo \
  -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}' 2>/dev/null || true)"
sandbox_ip="$(kubectl -n "$DEMO_NS" get svc spark-demo -o jsonpath='{.spec.clusterIP}' \
  2>/dev/null || true)"
if [ -z "$sandbox_pod" ]; then
  echo "no Running spark-demo pod in $DEMO_NS" >&2
  exit 1
fi
L "sandbox=$sandbox_pod cluster-ip=$sandbox_ip"

kubectl -n "$NS" get secret svc-catalog-spark -o jsonpath='{.data.client-id}' \
  | base64 -d > "$tmp/client-id"
kubectl -n "$NS" get secret svc-catalog-spark -o jsonpath='{.data.client-secret}' \
  | base64 -d > "$tmp/client-secret"
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.access-key}' \
  | base64 -d > "$tmp/ak"
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.secret-key}' \
  | base64 -d > "$tmp/sk"
chmod 600 "$tmp"/*
L "identity=svc-catalog-spark (aud lakekeeper) + Secret stardelt-s3-creds (as #731)"

if ! forward_lk; then
  echo "lakekeeper did not answer within 30s" >&2
  exit 1
fi
auth_ready
PREFIX="$(curl -sS -H @"$tmp/auth" "$LK/catalog/v1/config?warehouse=$CATALOG" \
  | jq -r '.defaults.prefix // empty' 2>/dev/null || true)"
[ -n "$PREFIX" ] || { echo "the catalog config named no warehouse prefix" >&2; exit 1; }
code="$(catalog GET "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables")"
if [ "$code" = "200" ]; then
  echo "schema $SCHEMA already holds tables; refusing to touch someone's data" >&2
  exit 1
fi
printf '{"namespace":["%s"],"properties":{}}' "$SCHEMA" > "$tmp/namespace.json"
code="$(catalog POST "/catalog/v1/$PREFIX/namespaces" "$tmp/namespace.json")"
if [ "$code" = "200" ] || [ "$code" = "204" ]; then
  schema_created=yes
fi
L "schema-create=$CATALOG.$SCHEMA code=$code"

# --- the sandbox's Spark configuration ---------------------------------------------------
# The stock file sets `spark.master local[2]`, which conflicts with the remote a run names.
sandbox_scratch=yes
sandbox_sh 'rm -rf /tmp/s2-graph /tmp/s2-graph-local /tmp/s2-graph-missing /tmp/s2-graph-static /tmp/s2-conf /tmp/s2-conf-local /tmp/s2-storage /tmp/s2-remote.log /tmp/s2-local.log /tmp/s2-stock.log /tmp/s2-static.log /tmp/s2-dry-good.log /tmp/s2-dry-missing.log
  mkdir -p /tmp/s2-conf /tmp/s2-conf-local
  grep -v "^ *spark.master" /opt/spark/conf/spark-defaults.conf \
    > /tmp/s2-conf/spark-defaults.conf
  # The local shape runs a Spark of its own in the client JVM; the server owns 15002.
  { cat /tmp/s2-conf/spark-defaults.conf
    echo "spark.connect.grpc.binding.address 127.0.0.1"
    echo "spark.connect.grpc.binding.port 15099"; } \
    > /tmp/s2-conf-local/spark-defaults.conf' >/dev/null
L "conf=1 stock (spark.master local[2]) 2 without master 3 without master and client binding 127.0.0.1:15099"

# --- Q1: the invocation ------------------------------------------------------------------
auth_ready
write_graph "$tmp/remote"
write_spec "sc://127.0.0.1:15002" "s3://$BUCKET/spike-s2/checkpoints" "$tmp/remote.yml"
put_in_sandbox /tmp/s2-graph/spark-pipeline.yml "$tmp/remote.yml"
put_in_sandbox /tmp/s2-graph/sql/mv_one.sql "$tmp/remote/sql/mv_one.sql"
put_in_sandbox /tmp/s2-graph/sql/mv_two.sql "$tmp/remote/sql/mv_two.sql"

# 1a. the pod's own configuration: the CLI dies at submission.
timed "$tmp/stock.log" kubectl -n "$DEMO_NS" exec "$sandbox_pod" -- bash -lc \
  'cd /tmp/s2-graph && /opt/spark/bin/spark-pipelines run --spec spark-pipeline.yml'
stock_first="$(head -n1 "$tmp/stock.log")"
stock_last="$(grep -v '^	at ' "$tmp/stock.log" | sed -n '2p')"
L "connect-submit-conflict=exit=$RUN_EXIT ms=$RUN_MS first=\"$stock_first\" last=\"$stock_last\""

# 1b. a static config in the spec's `configuration:` is refused over a remote session: the
#     proof that the run is the server's.
write_graph "$tmp/static"
write_spec "sc://127.0.0.1:15002" "s3://$BUCKET/spike-s2/checkpoints" "$tmp/static.yml" yes
put_in_sandbox /tmp/s2-graph-static/spark-pipeline.yml "$tmp/static.yml"
put_in_sandbox /tmp/s2-graph-static/sql/mv_one.sql "$tmp/static/sql/mv_one.sql"
put_in_sandbox /tmp/s2-graph-static/sql/mv_two.sql "$tmp/static/sql/mv_two.sql"
timed "$tmp/static.log" kubectl -n "$DEMO_NS" exec "$sandbox_pod" -- bash -lc \
  'cd /tmp/s2-graph-static && SPARK_CONF_DIR=/tmp/s2-conf /opt/spark/bin/spark-pipelines dry-run --spec spark-pipeline.yml'
static_err="$(grep -o 'CANNOT_MODIFY_STATIC_CONFIG.*' "$tmp/static.log" | head -n1 | cut -c1-140 || true)"
L "connect=remote (spark.remote in the spec's configuration); static-config-refused=$([ -n "$static_err" ] && echo yes || echo no)"
L "connect-reaches-server=$static_err"

# 1c. the real remote run; a fresh token because it lives five minutes.
auth_ready
write_spec "sc://127.0.0.1:15002" "s3://$BUCKET/spike-s2/checkpoints" "$tmp/remote.yml"
put_in_sandbox /tmp/s2-graph/spark-pipeline.yml "$tmp/remote.yml"
timed "$tmp/remote.log" kubectl -n "$DEMO_NS" exec "$sandbox_pod" -- bash -lc \
  'cd /tmp/s2-graph && SPARK_CONF_DIR=/tmp/s2-conf /opt/spark/bin/spark-pipelines run --spec spark-pipeline.yml'
remote_completed="$(grep -c 'has COMPLETED' "$tmp/remote.log" || true)"
L "connect-run=exit=$RUN_EXIT ms=$RUN_MS flows-completed=$remote_completed"
grep -E 'Loading pipeline spec|Run is COMPLETED' "$tmp/remote.log" | sed 's/^/connect-run-log: /' || true

# The read-back is a Connect session of its own, so it says the server holds the views.
if [ "$RUN_EXIT" != "0" ]; then
  L "connect-run-read=not-tested (the run did not complete)"
else
  timed "$tmp/read.log" kubectl -n "$DEMO_NS" exec -i "$sandbox_pod" -- \
    env PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    /opt/python313/bin/python3 -c '
import yaml
from pyspark.sql import SparkSession
spec = yaml.safe_load(open("/tmp/s2-graph/spark-pipeline.yml"))
builder = SparkSession.builder
for key, value in spec["configuration"].items():
    builder = builder.config(key, str(value))
session = builder.create()
for name in ("mv_one", "mv_two"):
    rows = session.table("playground.spike_s2." + name).collect()
    print("read %s=%s" % (name, [row[0] for row in rows]))
session.stop()'
  read_rows="$(grep '^read ' "$tmp/read.log" | sed 's/^read //' | paste -sd' ' -)"
  L "connect-run-read=$read_rows"
fi

# 1d. the other invocation: no remote, so the CLI starts a Spark of its own.
auth_ready
write_graph "$tmp/local"
write_spec "local" "file:///tmp/s2-storage" "$tmp/local.yml"
put_in_sandbox /tmp/s2-graph-local/spark-pipeline.yml "$tmp/local.yml"
put_in_sandbox /tmp/s2-graph-local/sql/mv_one.sql "$tmp/local/sql/mv_one.sql"
put_in_sandbox /tmp/s2-graph-local/sql/mv_two.sql "$tmp/local/sql/mv_two.sql"
timed "$tmp/local.log" kubectl -n "$DEMO_NS" exec "$sandbox_pod" -- bash -lc \
  'cd /tmp/s2-graph-local && SPARK_CONF_DIR=/tmp/s2-conf-local /opt/spark/bin/spark-pipelines run --spec spark-pipeline.yml'
local_spark="$(grep -c "Using Spark's default log4j profile" "$tmp/local.log" || true)"
local_completed="$(grep -c 'has COMPLETED' "$tmp/local.log" || true)"
L "connect-local=exit=$RUN_EXIT ms=$RUN_MS own-sparkcontext=$local_spark flows-completed=$local_completed"
L "connect-local-note=without a remote the CLI starts its own Spark; the server owns 15002, so the client's Connect port must move (s2-conf-local: 15099)"

# --- Q2: a second pod (the loader's shape) -----------------------------------------------
auth_ready
# No token and no S3 key: this pod measures reachability and the CLI, not identity.
write_spec "sc://spark-demo.$DEMO_NS.svc.cluster.local:15002" \
  "s3://$BUCKET/spike-s2/checkpoints" "$tmp/second.yml" no no
# The ConfigMap carries neither. A key cannot contain '/', so the sql tree is projected with
# items[].path.
kubectl -n "$NS" create configmap "$CM_GRAPH" \
  --from-file=spark-pipeline.yml="$tmp/second.yml" \
  --from-file=mv_one.sql="$tmp/remote/sql/mv_one.sql" \
  --from-file=mv_two.sql="$tmp/remote/sql/mv_two.sql" \
  --dry-run=client -o yaml 2>/dev/null \
  | kubectl apply -f - >/dev/null && cm_created=yes

pod_manifest() { # <name> <extra label lines>
  cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $1
  namespace: $NS
  labels:
    app.kubernetes.io/name: spike-s2
    app.kubernetes.io/part-of: stardelt
$2
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 185
    runAsGroup: 185
    fsGroup: 185
    seccompProfile: { type: RuntimeDefault }
  containers:
    - name: cli
      image: $image
      command: ["sleep", "3600"]
      env:
        - { name: HOME, value: /tmp }
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
          - { key: spark-pipeline.yml, path: spark-pipeline.yml }
          - { key: mv_one.sql, path: sql/mv_one.sql }
          - { key: mv_two.sql, path: sql/mv_two.sql }
    - name: tmp
      emptyDir: {}
EOF
}

{
  pod_manifest "$POD" ""
  echo "---"
  # The sandbox's ingress policy admits pods labelled as the visitor notebook
  # (manifests/spark-demo-sandbox.yaml). This pod carries the labels only to tell the
  # NetworkPolicy apart from the server; it holds no token and no Secret.
  pod_manifest "$POD_VISITOR" '    component: singleuser-server
    stardelt.io/group.visitors: "true"'
} > "$tmp/pods.yaml"
if kubectl apply -f "$tmp/pods.yaml" >/dev/null 2>&1; then pods_created=yes; fi
for name in "$POD" "$POD_VISITOR"; do
  if ! kubectl -n "$NS" wait --for=condition=Ready "pod/$name" --timeout=240s >/dev/null 2>&1; then
    echo "pod $name did not become ready" >&2
    exit 1
  fi
done

# 2a. the channel, without any catalog: a TCP connect from each pod.
tcp_probe='
python3 - <<PY
import socket, time
targets = [("spark-demo.stardelt-demo-spark.svc.cluster.local", 15002),
           ("lakekeeper.stardelt.svc.cluster.local", 8181)]
for host, port in targets:
    started = time.time()
    try:
        socket.create_connection((host, port), timeout=5).close()
        print("tcp %s:%d open %.2fs" % (host, port, time.time() - started))
    except Exception as error:
        print("tcp %s:%d FAIL %s %.2fs" % (host, port, type(error).__name__, time.time() - started))
PY'
plain_tcp="$(kubectl -n "$NS" exec "$POD" -- bash -lc "$tcp_probe" 2>&1 | paste -sd' ' -)"
visitor_tcp="$(kubectl -n "$NS" exec "$POD_VISITOR" -- bash -lc "$tcp_probe" 2>&1 | paste -sd' ' -)"
sandbox_tcp="$(sandbox_sh "
python3 - <<PY
import socket, time
targets = [('127.0.0.1', 15002), ('$sandbox_ip', 15002),
           ('spark-demo.$DEMO_NS.svc.cluster.local', 15002),
           ('lakekeeper.$NS.svc.cluster.local', 8181)]
for host, port in targets:
    started = time.time()
    try:
        socket.create_connection((host, port), timeout=5).close()
        print('tcp %s:%d open %.2fs' % (host, port, time.time() - started))
    except Exception as error:
        print('tcp %s:%d FAIL %s %.2fs' % (host, port, type(error).__name__, time.time() - started))
PY" 2>&1 | paste -sd' ' -)"
L "second-pod=no-visitor-labels: $plain_tcp"
L "second-pod=visitor-labels: $visitor_tcp"
L "sandbox-own-address: $sandbox_tcp"

# 2b. the CLI from a loader-shaped pod: a refused channel produces no error at all.
timed "$tmp/second-cli.log" kubectl -n "$NS" exec "$POD" -- bash -lc \
  'cd /opt/graph && timeout 90 /opt/spark/bin/spark-pipelines run --spec spark-pipeline.yml'
second_last="$(grep -v '^	at ' "$tmp/second-cli.log" | grep -v 'command terminated' | tail -n1)"
L "second-pod-cli=exit=$RUN_EXIT ms=$RUN_MS last=\"$second_last\" (124 is the 90 s timeout; no error line)"
timed "$tmp/visitor-cli.log" kubectl -n "$NS" exec "$POD_VISITOR" -- bash -lc \
  'cd /opt/graph && timeout 120 /opt/spark/bin/spark-pipelines run --spec spark-pipeline.yml'
visitor_err="$(grep -o 'Not authorized: [^"]*' "$tmp/visitor-cli.log" | head -n1 || true)"
L "second-pod-cli=visitor-labels: exit=$RUN_EXIT ms=$RUN_MS error=\"$visitor_err\""
L "second-pod-answer=the Connect ingress is the client pod's only network dependency: the client pod cannot open a TCP connection to the catalog, yet the run's catalog call answered 401, so the catalog and S3 are the engine pod's business; the sandbox's NetworkPolicy admits only the visitor labels"

# --- Q3: the transport -------------------------------------------------------------------
# The read-only run from the visitor-labels pod: the CLI writes nothing beside the spec.
timed "$tmp/ro.log" kubectl -n "$NS" exec "$POD_VISITOR" -- bash -lc \
  'cd /opt/graph && timeout 120 /opt/spark/bin/spark-pipelines run --spec spark-pipeline.yml'
writes_ro="$(kubectl -n "$NS" exec "$POD_VISITOR" -- bash -lc \
  'find /opt/graph -type f | grep -v -E "(spark-pipeline|mv_one|mv_two)" | wc -l' 2>/dev/null || echo unknown)"
# The writable emptyDir copy of the same graph: the same answer, so the mount is not it.
timed "$tmp/writable.log" kubectl -n "$NS" exec "$POD_VISITOR" -- bash -lc \
  'rm -rf /tmp/graph && cp -L -r /opt/graph /tmp/graph && cd /tmp/graph &&
   timeout 120 /opt/spark/bin/spark-pipelines run --spec spark-pipeline.yml'
writable_err="$(grep -o 'Not authorized: [^"]*' "$tmp/writable.log" | head -n1 || true)"
writable_writes="$(kubectl -n "$NS" exec "$POD_VISITOR" -- bash -lc \
  'find /tmp/graph -type f | grep -v -E "(spark-pipeline|mv_one|mv_two)" | wc -l' 2>/dev/null || echo unknown)"
# A ConfigMap key cannot contain '/', so the sql/ tree only mounts through items[].path.
key_error="$(kubectl -n "$NS" create configmap spike-s2-keyprobe \
  --from-file="sql/mv_one.sql=$tmp/remote/sql/mv_one.sql" --dry-run=client -o yaml 2>&1 \
  | head -n1 || true)"
# A ten-file graph: the data size and the object size against the 1 MiB limit.
ten="$tmp/ten"
mkdir -p "$ten"
for n in $(seq 1 10); do
  printf "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_%02d AS\nSELECT id, 'row-%02d' AS label FROM range(100);\n" \
    "$n" "$n" > "$ten/mv_$n.sql"
done
ten_args=()
for f in "$ten"/mv_*.sql; do ten_args+=( --from-file="$(basename "$f")=$f" ); done
kubectl -n "$NS" create configmap "$CM_TEN" "${ten_args[@]}" --dry-run=client -o yaml \
  | kubectl apply -f - >/dev/null && cm_ten_created=yes
ten_json="$(kubectl -n "$NS" get configmap "$CM_TEN" -o json)"
ten_keys="$(jq '.data | keys | length' <<<"$ten_json")"
ten_data="$(jq '.data | to_entries | map(.value | length) | add' <<<"$ten_json")"
ten_obj="$(wc -c <<<"$ten_json" | tr -d ' ')"
L "transport=no-writes: files the CLI added beside the spec on the read-only mount=$writes_ro; writable copy exit=$RUN_EXIT added=$writable_writes error=\"$writable_err\""
L "transport-cm-key=$key_error"
L "transport-ten-file-cm keys=$ten_keys data-bytes=$ten_data object-bytes=$ten_obj (limit 1 MiB)"

# --- Q4: dry-run from the pod ------------------------------------------------------------
# In the sandbox, where the channel and the identity are #731's; the loader-shaped pod cannot
# reach either (Q2), so its dry-run is the wait measured above.
auth_ready
write_graph "$tmp/missing" 'CREATE MATERIALIZED VIEW IF NOT EXISTS mv_two AS SELECT id FROM mv_missing;'
write_spec "sc://127.0.0.1:15002" "s3://$BUCKET/spike-s2/checkpoints" "$tmp/missing.yml"
put_in_sandbox /tmp/s2-graph-missing/spark-pipeline.yml "$tmp/missing.yml"
put_in_sandbox /tmp/s2-graph-missing/sql/mv_one.sql "$tmp/missing/sql/mv_one.sql"
put_in_sandbox /tmp/s2-graph-missing/sql/mv_two.sql "$tmp/missing/sql/mv_two.sql"
write_spec "sc://127.0.0.1:15002" "s3://$BUCKET/spike-s2/checkpoints" "$tmp/remote.yml"
put_in_sandbox /tmp/s2-graph/spark-pipeline.yml "$tmp/remote.yml"

timed "$tmp/dry-good.log" kubectl -n "$DEMO_NS" exec "$sandbox_pod" -- bash -lc \
  'cd /tmp/s2-graph && SPARK_CONF_DIR=/tmp/s2-conf /opt/spark/bin/spark-pipelines dry-run --spec spark-pipeline.yml'
L "dry-run=exit=$RUN_EXIT ms=$RUN_MS"
dry_good_seconds=$(( RUN_MS / 1000 ))
timed "$tmp/dry-missing.log" kubectl -n "$DEMO_NS" exec "$sandbox_pod" -- bash -lc \
  'cd /tmp/s2-graph-missing && SPARK_CONF_DIR=/tmp/s2-conf /opt/spark/bin/spark-pipelines dry-run --spec spark-pipeline.yml'
missing_line="$(grep -m1 'TABLE_OR_VIEW_NOT_FOUND' "$tmp/dry-missing.log" | sed 's/^Error: //' || true)"
missing_flow="$(grep -m1 'Failed to resolve flow' "$tmp/dry-missing.log" | sed 's/.*: //' || true)"
L "dry-run-seconds=$dry_good_seconds (exit 0); missing-table exit=$RUN_EXIT in $(( RUN_MS / 1000 ))s"
L "dry-run-missing-table=$missing_line flow=$missing_flow"

# The same dry-run from the loader-shaped pod with the read-only graph.
timed "$tmp/visitor-dry.log" kubectl -n "$NS" exec "$POD_VISITOR" -- bash -lc \
  'cd /opt/graph && timeout 120 /opt/spark/bin/spark-pipelines dry-run --spec spark-pipeline.yml'
visitor_dry_err="$(grep -o 'Not authorized: [^"]*' "$tmp/visitor-dry.log" | head -n1 || true)"
L "dry-run-second-pod=exit=$RUN_EXIT ms=$RUN_MS error=\"$visitor_dry_err\""

exit 0
