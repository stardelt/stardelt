#!/usr/bin/env bash
# Spike for stardelt/stardelt#957 (#80 S2c-0): a `spark-pipeline` run as the group principal.
#
# #894 measured where a run gets its files and which invocation reaches an engine (a
# `spark.remote` in the spec's `configuration:`, a `SPARK_CONF_DIR` copy without
# `spark.master`/`spark.connect.grpc.binding.*`, a read-only ConfigMap). It deliberately held
# no identity. This run adds the principal and measures the six facts S2c-2 (the run pod) must
# not guess, each on its own `L` line:
#
#   Q1  the pod's shape. A pod in `stardelt` on the image pinned in `manifests/spark.yaml`, the
#       graph from a read-only ConfigMap, the sandbox's visitor labels (`component=singleuser-server`
#       and `stardelt.io/group.visitors=true`, both of which its NetworkPolicy selects), the
#       principal's Secret only through `secretKeyRef`. Without the labels the Connect Service
#       refuses the TCP connect; with them the CLI reaches "Run is COMPLETED".
#   Q2  which keys of the spec's `configuration:` the *server* applies: the
#       `spark.sql.catalog.<name>.*` block (session), `spark.remote` (client, the CLI's own
#       connection selector), and the static keys Spark refuses (`spark.sql.extensions`,
#       `spark.master`) with `CANNOT_MODIFY_STATIC_CONFIG`.
#   Q3  the identity: `demo`'s client-credentials token in `spark.sql.catalog.gold.token` reads
#       `gold.shop.daily_revenue`; the same graph with the token line omitted pastes Lakekeeper's
#       refusal.
#   Q4  the write and the vended credential: a `CREATE MATERIALIZED VIEW ... AS SELECT ... FROM
#       range(...)` into `gold.spike_s2c` with **no** S3 key in the spec, through
#       `header.X-Iceberg-Access-Delegation=vended-credentials`.
#   Q5  the token's life: the catalog reads it on every REST call, so an expired token is refused
#       mid-graph, and a second run needs a freshly minted one. The token is minted once at the
#       start and reused at the end once it has lived out `expires_in`, so the script does not
#       sit idle for five minutes.
#   Q6  the silent refusal: `spark.remote: sc://127.0.0.1:1` and the CLI's wall time and output,
#       then the library's TCP pre-check (`_reachable`, `python/stardelt/__init__.py`) turning it
#       into one sentence in under 5 s.
#
# The graph is a ConfigMap (read-only in the pod) with two datasets: `read_daily` (counts
# `gold.shop.daily_revenue`) and `range_ten` (`SELECT id FROM range(10)`), both written into
# `gold.spike_s2c`. The `token` line is **not** in the ConfigMap: the pod mints the token (the
# same client-credentials POST as `demos/images/dbt/stardelt-dbt`, with the engine's CA
# removed) and writes it into its own copy of the spec under `/tmp` (0600). No token, key or
# secret value is printed: every excerpt that leaves the pod is redacted first.
#
# The pod runs one `bash -s` script per measurement, so one refusal does not hide the next
# answer; each of those scripts is the one script the ticket names (mint the token, copy the
# graph, write the catalog block into the spec, build the conf copy, run the CLI).
#
# The lab, all reverted in `cleanup` on EXIT/INT/TERM:
#   - namespace `spike_s2c` of catalog `gold` (created here; the REST catalog does not cascade
#     on DROP, so the tables are deleted first, then the namespace);
#   - ConfigMap `spike-s2c-graph`, pods `spike-s2c-principal` and `spike-s2c-plain` in `stardelt`;
#   - the token file the build plane minted for its catalogue calls.
# The sandbox Deployment, its pod and its old `Error` ReplicaSet pod are left alone.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
DEMO_NS="${DEMO_NS:-stardelt-demo-spark}"
CM_GRAPH=spike-s2c-graph
POD=spike-s2c-principal
POD_PLAIN=spike-s2c-plain
SCHEMA=spike_s2c
CATALOG=gold
BUCKET=lakehouse
GROUP="${GROUP:-demo}"
PRINCIPAL_SECRET="sp-pipeline-$GROUP"
CONNECT_HOST="spark-demo.$DEMO_NS.svc.cluster.local"
CONNECT_URL="sc://$CONNECT_HOST:15002"
CLOSED_URL="sc://127.0.0.1:1"
RUN_TIMEOUT="${RUN_TIMEOUT:-240}"
CLOSED_TIMEOUT="${CLOSED_TIMEOUT:-60}"
LK_PORT="${LK_PORT:-18181}"
LK="http://127.0.0.1:$LK_PORT"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/s2c-principal.XXXXXX")"
LK_PID=""
PREFIX=""
schema_created=no
cm_created=no
pods_created=no
schema_deleted=no
cm_deleted=no
pods_deleted=no

L() { printf 'L %s\n' "$*"; }

# One line out of multi-line text.
text() { tr '\n' ' ' | tr -s ' ' | cut -c1-300; }

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

# The catalogue token of the build plane's own checks (never of the run): the `demo` principal's
# client credentials go into 0600 files, the token into another; none of them is printed.
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
  if [ "$pods_created" = "yes" ]; then
    if kubectl -n "$NS" delete pod "$POD" "$POD_PLAIN" --ignore-not-found \
        --timeout=150s >/dev/null 2>&1; then pods_deleted=yes; fi
    if [ "$(kubectl -n "$NS" get pod "$POD" "$POD_PLAIN" --ignore-not-found \
        -o name 2>/dev/null | wc -l)" = "0" ]; then pods_deleted=yes; fi
  fi
  if [ "$cm_created" = "yes" ]; then
    if kubectl -n "$NS" delete configmap "$CM_GRAPH" --ignore-not-found \
        >/dev/null 2>&1; then cm_deleted=yes; fi
  fi
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  rm -f "$tmp/token" "$tmp/auth" "$tmp/client-id" "$tmp/client-secret" "$tmp/token.json"
  L "cleanup schema-dropped=$schema_deleted cm-deleted=$cm_deleted pod-deleted=$pods_deleted token-file-removed=$([ ! -e "$tmp/token" ] && echo yes || echo no)"
  rm -rf "$tmp"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for tool in kubectl curl jq python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

# --- the pinned image, the sandbox, the identity endpoint ---------------------------------
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
L "sandbox=$sandbox_pod connect=$sandbox_ip:15002 url=$CONNECT_URL"

token_url="$(kubectl -n "$NS" get cm stardelt-identity -o jsonpath='{.data.tokenEndpoint}')"
[ -n "$token_url" ] || { echo "ConfigMap stardelt-identity has no tokenEndpoint" >&2; exit 1; }
idx=0
for key in client-id client-secret; do
  idx=$(( idx + 1 ))
  kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" -o jsonpath="{.data.$key}" \
    | base64 -d > "$tmp/key$idx" || { echo "Secret $PRINCIPAL_SECRET has no $key" >&2; exit 1; }
done
cp "$tmp/key1" "$tmp/client-id"
cp "$tmp/key2" "$tmp/client-secret"
rm -f "$tmp/key1" "$tmp/key2"
chmod 600 "$tmp"/*
L "identity=$PRINCIPAL_SECRET keys=client-id,client-secret (secretKeyRef in the pod; never read by the pod's script)"

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
  echo "namespace $SCHEMA already holds tables; refusing to touch someone's data" >&2
  exit 1
fi
printf '{"namespace":["%s"],"properties":{}}' "$SCHEMA" > "$tmp/namespace.json"
code="$(catalog POST "/catalog/v1/$PREFIX/namespaces" "$tmp/namespace.json")"
if [ "$code" = "200" ] || [ "$code" = "204" ]; then schema_created=yes; fi
L "schema-create=$CATALOG.$SCHEMA code=$code"

# --- the graph (ConfigMap; the token line is the placeholder the pod fills) ---------------
mkdir -p "$tmp/graph/sql"
cat > "$tmp/graph/spark-pipeline.yml" <<EOF
name: spike-s2c
# The catalogue's own storage is s3://lakehouse/<catalog>/<namespace-uuid>/ (measured on
# gold.shop.daily_revenue): gold is the catalog and prefix, not a bucket.
storage: s3a://$BUCKET/pipelines/spike-s2c/checkpoints
catalog: $CATALOG
database: $SCHEMA
configuration:
  spark.remote: "$CONNECT_URL"
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
cat > "$tmp/graph/sql/read_daily.sql" <<'EOF'
CREATE MATERIALIZED VIEW IF NOT EXISTS read_daily AS
SELECT count(*) AS n FROM gold.shop.daily_revenue;
EOF
cat > "$tmp/graph/sql/range_ten.sql" <<'EOF'
CREATE MATERIALIZED VIEW IF NOT EXISTS range_ten AS
SELECT id, 'row-' || id AS label FROM range(10);
EOF

kubectl -n "$NS" create configmap "$CM_GRAPH" \
  --from-file=spark-pipeline.yml="$tmp/graph/spark-pipeline.yml" \
  --from-file=read_daily.sql="$tmp/graph/sql/read_daily.sql" \
  --from-file=range_ten.sql="$tmp/graph/sql/range_ten.sql" \
  --dry-run=client -o yaml 2>/dev/null \
  | kubectl apply -f - >/dev/null && cm_created=yes
L "graph=ConfigMap $CM_GRAPH keys=3 (spark-pipeline.yml, read_daily.sql, range_ten.sql); the sql/ tree mounts through items[].path"

# --- the pods -----------------------------------------------------------------------------
# `POD` carries the two labels the sandbox's ingress NetworkPolicy selects; `POD_PLAIN` carries
# neither, so the TCP probe says what the labels do. Both hold the graph read-only, no S3 key.
pod_manifest() { # <name> <labels|empty>
  cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $1
  namespace: $NS
  labels:
    app.kubernetes.io/name: spike-s2c
    app.kubernetes.io/part-of: stardelt
    stardelt.io/spike: s2c-principal
$2
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
          - { key: spark-pipeline.yml, path: spark-pipeline.yml }
          - { key: read_daily.sql, path: sql/read_daily.sql }
          - { key: range_ten.sql, path: sql/range_ten.sql }
    - name: tmp
      emptyDir: {}
EOF
}

{
  pod_manifest "$POD" '    component: singleuser-server
    stardelt.io/group.visitors: "true"'
  echo "---"
  pod_manifest "$POD_PLAIN" ""
} > "$tmp/pods.yaml"
if kubectl apply -f "$tmp/pods.yaml" >/dev/null 2>&1; then pods_created=yes; fi
for name in "$POD" "$POD_PLAIN"; do
  if ! kubectl -n "$NS" wait --for=condition=Ready "pod/$name" --timeout=300s >/dev/null 2>&1; then
    echo "pod $name did not become ready" >&2
    kubectl -n "$NS" describe pod "$name" 2>/dev/null | tail -n 20 >&2 || true
    exit 1
  fi
done
L "pods=$POD (component=singleuser-server, stardelt.io/group.visitors=true) $POD_PLAIN (no labels)"
L "versions $(kubectl -n "$NS" exec "$POD" -- bash -c 'sed -n 1p /opt/spark/RELEASE; ls /opt/spark/jars | grep -oE "spark-pipelines_2.13-[0-9.]+" | head -n1' 2>&1 | text)"

# --- the in-pod run script ----------------------------------------------------------------
# One script per measurement, piped to `bash -s` with `<tag> <mode> <remote> <static-key> <timeout>`:
# mint the token, copy the graph, write the catalog block (the token from a file it wrote, 0600),
# copy $SPARK_HOME/conf without `spark.master` and `spark.connect.grpc.binding.*`, run the CLI.
# Everything it prints is redacted (`<token>`), so no token reaches the build plane's transcript.
cat > "$tmp/run.sh" <<'INNER'
set -euo pipefail
umask 077
tag="$1"; mode="$2"; remote="$3"; static_key="$4"; limit="$5"
work=/tmp/s2c
dir="$work/run-$tag"
rm -rf "$dir"; mkdir -p "$dir"
cp -rL /opt/graph/. "$dir"/
chmod -R u+w "$dir"

# The client's conf copy. A pod of the pinned image ships no /opt/spark/conf at all (the sandbox's
# file comes from its Deployment's ConfigMap), so there is nothing to strip there; when a conf dir
# is present, the keys the server owns (`spark.master`, `spark.connect.grpc.binding.*`) are dropped.
rm -rf "$work/conf"; mkdir -p "$work/conf"
stock=/opt/spark/conf
conf_present=no; dropped_master=na; dropped_binding=na
if [ -d "$stock" ]; then
  cp -rL "$stock/." "$work/conf"/
  conf_present=yes; dropped_master=no; dropped_binding=no
  if [ -f "$work/conf/spark-defaults.conf" ]; then
    if grep -qE '^[[:space:]]*spark\.master' "$work/conf/spark-defaults.conf"; then dropped_master=yes; fi
    if grep -qE '^[[:space:]]*spark\.connect\.grpc\.binding\.' "$work/conf/spark-defaults.conf"; then dropped_binding=yes; fi
    { grep -vE '^[[:space:]]*(spark\.master|spark\.connect\.grpc\.binding\.)' \
        "$work/conf/spark-defaults.conf" || true; } > "$work/conf/conf.tmp"
    mv "$work/conf/conf.tmp" "$work/conf/spark-defaults.conf"
  fi
fi
echo "conf-dir=$([ "$conf_present" = yes ] && echo present || echo absent) dropped-master=$dropped_master dropped-binding=$dropped_binding"

mint() { # <file>
  python3 - "$1" <<'PY'
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
if "expires_in" in payload:
    open(sys.argv[1] + "-expires", "w").write(str(payload["expires_in"]))
print("token expires_in=%s" % payload.get("expires_in", "unknown"))
PY
}

case "$mode" in
  token|static-extensions|static-master|closed-port|re-token) mint "$work/token" ;;
  no-token) rm -f "$work/token" ;;  invalid)
    # A token the catalog no longer accepts: the minted one with one character of its signature
    # changed. The realm's client-credentials token lives `expires_in` (measured 28800 s here), so
    # a real expiry is out of a spike's reach; the catalog's answer to a token it refuses is the
    # sentence a run gets after an expiry, and it also says the token is read on every REST call.
    # The catalog itself is not reachable from this pod (its ingress admits no spike pod), so the
    # build plane asks it, with the same refused token, from its port-forward.
    echo "token-age=$(( $(date +%s) - $(cat "$work/token-old-at") ))s"
    python3 - /tmp/s2c/token-old "$work/token" <<'PY'
import os, sys
token = open(sys.argv[1]).read().strip()
bad = token[:-1] + ("A" if token[-1] != "A" else "B")
handle = os.open(sys.argv[2], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.write(handle, bad.encode())
os.close(handle)
print("token-invalid signature-changed-at=%d" % len(token))
PY
    ;;
  *) echo "unknown mode $mode" >&2; exit 2 ;;
esac

# The spec's own copy: the placeholder becomes the minted token, `spark.remote` stays, and the
# static key a Q2 run proves refused is added under `configuration:`.
python3 - "$dir/spark-pipeline.yml" "$remote" "$static_key" "$work/token" <<'PY'
import os, sys
path, remote, static_key, token_path = sys.argv[1:5]
token = ""
if os.path.exists(token_path):
    token = open(token_path).read().strip()
out = []
for line in open(path).read().splitlines():
    stripped = line.strip()
    if stripped == "# <<token>>":
        if token:
            out.append('  spark.sql.catalog.gold.token: "%s"' % token)
        continue
    if stripped.startswith("spark.remote:"):
        out.append('  spark.remote: "%s"' % remote)
        continue
    out.append(line)
    if stripped == "configuration:" and static_key:
        key, value = static_key.split("=", 1)
        out.append('  %s: "%s"' % (key, value))
open(path, "w", newline="\n").write("\n".join(out) + "\n")
PY

run_cli() { # <log>
  local start end code
  start="$(date +%s%3N)"
  set +e
  SPARK_CONF_DIR="$work/conf" timeout "$limit" \
    /opt/spark/bin/spark-pipelines run --spec "$dir/spark-pipeline.yml" > "$1" 2>&1
  code=$?
  set -e
  end="$(date +%s%3N)"
  echo "run exit=$code ms=$((end-start))"
}

summarise() { # <log>
  python3 - "$1" "$work/token" <<'PY'
import sys
log = open(sys.argv[1], errors="replace").read()
try:
    token = open(sys.argv[2]).read().strip()
except OSError:
    token = ""
if token:
    log = log.replace(token, "<token>")
lines = [line.strip() for line in log.splitlines()]
interesting = (
    "Run is COMPLETED", "has COMPLETED", "CANNOT_MODIFY_STATIC_CONFIG",
    "Not authorized", "Unauthorized", "401", "TokenExpired", "expired",
    "Creating Spark session", "Exception", "Error:",
)
hits = [line for line in lines if any(word in line for word in interesting)]
print("flows=%d" % sum(1 for line in lines if "has COMPLETED" in line))
for line in hits[:6]:
    print("note: " + line[:240])
plain = [line for line in lines if line and not line.startswith("at ")]
print("last: " + (plain[-1][:240] if plain else ""))
PY
}

readback() { # a fresh session from the same configuration: what the server committed
  PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    /opt/python313/bin/python3 - "$dir/spark-pipeline.yml" <<'PY'
import sys
config = {}
inside = False
for line in open(sys.argv[1]).read().splitlines():
    if line.rstrip() == "configuration:":
        inside = True
        continue
    if inside:
        if not line.startswith("  "):
            break
        key, _, value = line.strip().partition(":")
        config[key.strip()] = value.strip().strip('"')
from pyspark.sql import SparkSession
builder = SparkSession.builder.remote(config.pop("spark.remote")).appName("spike-s2c-readback")
for key, value in config.items():
    builder = builder.config(key, value)
session = builder.create()
for name in ("read_daily", "range_ten"):
    print("read gold.spike_s2c.%s rows=%d" % (name, session.table("gold.spike_s2c." + name).count()))
session.stop()
PY
}

case "$mode" in
  token|re-token)
    echo "spec-keys=$(grep -c '^  spark\.' "$dir/spark-pipeline.yml") token-in-spec=$([ -s "$work/token" ] && echo present || echo absent) s3-key-in-spec=$(grep -cE 's3\.(access-key-id|secret-access-key)' "$dir/spark-pipeline.yml" || true)"
    run_cli "$work/$tag.log"
    summarise "$work/$tag.log"
    if grep -q "Run is COMPLETED" "$work/$tag.log"; then
      if readback > "$work/readback.log" 2>&1; then
        grep '^read ' "$work/readback.log" || true
      else
        echo "readback=refused $(tail -n1 "$work/readback.log" | cut -c1-200)"
      fi
    fi
    ;;
  *)
    run_cli "$work/$tag.log"
    summarise "$work/$tag.log"
    ;;
esac
INNER
chmod 600 "$tmp/run.sh"

pod_run() { # <tag> <mode> [remote] [static key=value] [timeout]
  local tag="$1" mode="$2" remote="${3-$CONNECT_URL}" static="${4-}" limit="${5-$RUN_TIMEOUT}"
  kubectl -n "$NS" exec -i "$POD" -- bash -s -- "$tag" "$mode" "$remote" "$static" "$limit" \
    < "$tmp/run.sh"
}

tcp_probe() { # <pod>
  kubectl -n "$NS" exec -i "$1" -- python3 - <<PY
import socket, time
started = time.time()
try:
    socket.create_connection(("$CONNECT_HOST", 15002), timeout=5).close()
    print("tcp open %.2fs" % (time.time() - started))
except Exception as error:
    print("tcp FAIL %s %.2fs" % (type(error).__name__, time.time() - started))
PY
}

# --- Q1: the labels and the channel ------------------------------------------------------
plain_tcp="$(tcp_probe "$POD_PLAIN" 2>&1 || true)"
visitor_tcp="$(tcp_probe "$POD" 2>&1 || true)"
L "q1-tcp no-visitor-labels=\"$(printf '%s' "$plain_tcp" | text)\" visitor-labels=\"$(printf '%s' "$visitor_tcp" | text)\""

# A token minted now, kept in the pod for Q5; it lives `expires_in`, and the other measurements
# use up that time, so Q5 does not wait for it.
old_token="$(kubectl -n "$NS" exec -i "$POD" -- bash -s <<'PRE' 2>&1 || true
set -euo pipefail
umask 077
mkdir -p /tmp/s2c
python3 - /tmp/s2c/token-old <<'PY'
import json, os, ssl, sys, time, urllib.parse, urllib.request
for name in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
    os.environ.pop(name, None)
body = urllib.parse.urlencode(
    {"grant_type": "client_credentials", "client_id": os.environ["PIPELINE_CLIENT_ID"],
     "client_secret": os.environ["PIPELINE_CLIENT_SECRET"]}
).encode()
request = urllib.request.Request(
    os.environ["PIPELINE_TOKEN_URL"], data=body,
    headers={"Content-Type": "application/x-www-form-urlencoded"})
with urllib.request.urlopen(request, timeout=30, context=ssl.create_default_context()) as response:
    payload = json.load(response)
handle = os.open(sys.argv[1], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.write(handle, payload["access_token"].encode())
os.close(handle)
open(sys.argv[1] + "-expires", "w").write(str(payload.get("expires_in", 300)))
open("/tmp/s2c/token-old-at", "w").write(str(int(time.time())))
print("expires_in=%s" % payload.get("expires_in", "unknown"))
PY
PRE
)"
L "q5-token-minted ttl=$(printf '%s' "$old_token" | sed 's/^expires_in=//' | text)"

# --- Q1/Q2/Q3/Q4: the main run (the server's session, the principal) ----------------------
main="$(pod_run main token)"
L "q1-conf $(grep '^conf-dir' <<<"$main" | text)"
L "q1-run $(grep '^run ' <<<"$main" | sed 's/^run //' | text)"
L "q1-run-flows=$(grep '^flows=' <<<"$main" | sed 's/^flows=//' | text)"
L "q2-spec $(grep '^spec-keys' <<<"$main" | sed 's/^spec-keys=//' | text) (catalog keys applied by the server: both flows COMPLETED)"
grep -E '^(note|last|read|readback)' <<<"$main" | sed 's/^/  · /' || true
L "q3-read $(grep '^read gold.spike_s2c.read_daily' <<<"$main" | sed 's/^read //' | text)"
L "q4-write $(grep '^read gold.spike_s2c.range_ten' <<<"$main" | sed 's/^read //' | text)"
L "q5-token-ttl=$(grep '^token expires_in' <<<"$main" | sed 's/^token //' | text)"

# The catalogue agrees the two views exist (status only; no token in the output).
code="$(catalog GET "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables")"
tables="$(jq -r '[.identifiers[]?.name] | join(",")' "$tmp/out.json" 2>/dev/null || true)"
L "q4-catalog $CATALOG.$SCHEMA code=$code tables=$tables"

# --- Q3: the same graph without the token line -------------------------------------------
# The namespace is emptied first: a graph whose two views already exist would only read them, and
# `gold` is a public demo catalog (visitors read anonymously), so a no-token run must be made to
# write to get the catalog's refusal.
code="$(catalog GET "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables")"
if [ "$code" = "200" ]; then
  for name in $(jq -r '.identifiers[]?.name' "$tmp/out.json" 2>/dev/null); do
    code="$(catalog DELETE "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables/$name?purgeRequested=true")"
    L "q3-reset drop-table=$SCHEMA.$name code=$code"
  done
fi
notoken="$(pod_run no-token no-token)"
L "q3-no-token $(grep '^run ' <<<"$notoken" | sed 's/^run //' | text)"
L "q3-no-token-answer=\"$(grep -E '^note:' <<<"$notoken" | sed 's/^note: //' | grep -v 'Creating Spark session' | paste -sd' | ' - | text)\""

# --- Q2: the static keys the server refuses ----------------------------------------------
ext="$(pod_run static-extensions static-extensions "$CONNECT_URL" \
  "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")"
L "q2-static-extensions $(grep '^run ' <<<"$ext" | sed 's/^run //' | text)"
L "q2-static-extensions-answer=\"$(grep -E '^note:' <<<"$ext" | sed 's/^note: //' | grep -v 'Creating Spark session' | paste -sd' | ' - | text)\""
master="$(pod_run static-master static-master "$CONNECT_URL" "spark.master=local[2]")"
L "q2-static-master $(grep '^run ' <<<"$master" | sed 's/^run //' | text)"
L "q2-static-master-answer=\"$(grep -E '^note:' <<<"$master" | sed 's/^note: //' | grep -v 'Creating Spark session' | paste -sd' | ' - | text)\""

# --- Q5: the token's life ----------------------------------------------------------------
# The catalog's answer to a token it no longer accepts, asked from the build plane (the client pod
# is not admitted by the catalog's ingress). One character of the signature is changed, so no
# value is stored beyond the header file this script deletes in `cleanup`.
bad_token="$(cat "$tmp/token")"
if [ "${bad_token: -1}" = "A" ]; then
  bad_token="${bad_token%?}B"
else
  bad_token="${bad_token%?}A"
fi
printf 'Authorization: Bearer %s\n' "$bad_token" > "$tmp/auth-bad"
chmod 600 "$tmp/auth-bad"
bad_code="$(curl -sS -H @"$tmp/auth-bad" "$LK/catalog/v1/config?warehouse=$CATALOG" \
  -o "$tmp/bad.json" -w '%{http_code}' 2>/dev/null || echo 000)"
bad_type="$(jq -r '.error.type // empty' "$tmp/bad.json" 2>/dev/null || true)"
bad_message="$(jq -r '.error.message // empty' "$tmp/bad.json" 2>/dev/null || true)"
unset bad_token
L "q5-catalog-refusal status=$bad_code type=$bad_type message=$(printf '%s' "$bad_message" | text)"
invalid="$(pod_run invalid invalid "$CONNECT_URL" "" 180 2>&1 || true)"
L "q5-token-age $(grep -E '^token-age' <<<"$invalid" | text)"
L "q5-invalid-run $(grep '^run ' <<<"$invalid" | sed 's/^run //' | text)"
L "q5-invalid-run-answer=\"$(grep -E '^note:' <<<"$invalid" | sed 's/^note: //' | grep -v 'Creating Spark session' | paste -sd' | ' - | text)\""
again="$(pod_run re-token re-token)"
L "q5-second-run $(grep '^run ' <<<"$again" | sed 's/^run //' | text)"
L "q5-second-run-flows=$(grep '^flows=' <<<"$again" | sed 's/^flows=//' | text)"

# --- Q6: the closed port and the pre-check -----------------------------------------------
closed="$(pod_run closed closed-port "$CLOSED_URL" "" "$CLOSED_TIMEOUT" 2>&1 || true)"
L "q6-closed-port $(grep '^run ' <<<"$closed" | sed 's/^run //' | text)"
L "q6-closed-port-last=\"$(grep '^last:' <<<"$closed" | sed 's/^last: //' | text)\""
precheck="$(kubectl -n "$NS" exec -i "$POD" -- python3 - <<'PRE' 2>&1 || true
import socket, time
from urllib.parse import urlsplit


# `stardelt._session`'s pre-check, verbatim (`python/stardelt/__init__.py`, `_reachable`): the
# image carries no `stardelt` package, so the function body is reproduced here, not imported.
def reachable(remote, timeout=3.0):
    parts = urlsplit(remote.replace("sc://", "http://", 1).split(";", 1)[0])
    host, port = parts.hostname, parts.port or 15002
    try:
        socket.create_connection((host, port), timeout=timeout).close()
    except OSError as exc:
        raise RuntimeError(
            f"Spark is not reachable at {host}:{port} ({type(exc).__name__}). "
            "Either the cluster is stopped or this notebook may not use Spark. %sql still works."
        ) from None


started = time.time()
try:
    reachable("sc://127.0.0.1:1")
    print("precheck reached (unexpected) ms=%d" % ((time.time() - started) * 1000))
except RuntimeError as error:
    print("precheck refused ms=%d sentence=\"%s\"" % ((time.time() - started) * 1000, error))
PRE
)"
L "q6-precheck $(printf '%s' "$precheck" | text)"
L "q6-stardelt-import $(kubectl -n "$NS" exec "$POD" -- python3 -c 'import stardelt' 2>&1 | grep -m1 'ModuleNotFoundError' | text)"

exit 0
