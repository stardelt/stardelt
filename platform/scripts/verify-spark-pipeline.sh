#!/usr/bin/env bash
# One `spark-pipeline` task pod, end to end (stardelt/stardelt#990, #80 S2c-3).
#
#   make verify-spark-pipeline           (NS=… the platform namespace, TIMEOUT=… the pod wait)
#
# Why: `spark-pipeline` has no live gate. #969 made the loader run one and measured it by hand,
# and #986 then had to fix a bug the hand measurement missed (the image's Ubuntu `python3` has
# no PyYAML, so the runner died on `import yaml`; the interpreter with PyYAML is
# `/opt/python313/bin/python3`). The demo gate added in #979 covers the *sandbox*; nothing covers
# the **task pod** — its image, its labels, its admission to the Connect server, its principal's
# token and the vended credential its write uses. A regression in any of those is silent until
# someone runs a pipeline by hand.
#
# This check proves, in one run:
#   run    a pod of the image the shared Connect server runs starts its `spark-pipelines run`
#          and the graph completes against that server;
#   flows  at least one flow of the graph reports COMPLETED;
#   table  Lakekeeper's REST catalog answers 200 for the table, with a main snapshot — the
#          write reached the catalog through the principal's token and the vended credential
#          (the spec carries no S3 key at all);
#   rows   a fresh session from the same configuration counts 10 rows, so the commit is readable.
#   credential  the pod carries one projected ServiceAccount token for the catalog audience and
#               no Secret reference; the token is the principal's ServiceAccount
#
# Exit 0 = PASS (all four), 1 = FAIL (one of the four did not hold), 3 = UNVERIFIED (no kubectl,
# the server or the principal is not installed, the pod never started or never reached the
# server — nothing about the task pod was measured then).
#
# Two traps this script carries:
#   - `umask 077`: the principal's client credentials and every token go into mode-0600 files
#     under $HOME, and none of them is printed (the log excerpts it shows are redacted first);
#   - `cleanup` runs on EXIT, INT and TERM: the scratch schema, the graph ConfigMap, the pod and
#     the port-forward are removed whatever happened.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-${NAMESPACE:-stardelt}}"
CATALOG="${CATALOG:-gold}"
SCHEMA="${SCHEMA:-verify_spark_pipeline}"
TABLE="${TABLE:-range_ten}"
GROUP="${GROUP:-demo}"
PRINCIPAL_SECRET="${PRINCIPAL_SECRET:-sp-pipeline-$GROUP}"
PRINCIPAL_SA="${PRINCIPAL_SA:-sp-pipeline-$GROUP}"
CONNECT_URL="${CONNECT_URL:-sc://spark-connect.$NS.svc.cluster.local:15002}"
BUCKET="${BUCKET:-lakehouse}"
CM="${CM:-verify-spark-pipeline-graph}"
POD="${POD:-verify-spark-pipeline}"
LK_PORT="${LK_PORT:-18183}"
TIMEOUT="${TIMEOUT:-300}"
LK="http://127.0.0.1:$LK_PORT"

scratch="${TMPDIR:-$HOME/.cache}"
mkdir -p "$scratch"
tmp="$(mktemp -d "$scratch/verify-spark-pipeline.XXXXXX")"
LK_PID=""
PREFIX=""
schema_created=no
cm_created=no
pod_created=no
finished=no

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

# The `demo` principal's client credentials go into 0600 files, the token into another; none of
# them is printed. This token is the *build plane's* for the catalog calls around the run; the
# pod mints its own, so the task's write is measured with a token it obtained itself.
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
      [ "$code" = "204" ] || [ "$code" = "200" ] || tables_ok=no
    done
    [ "$tables_ok" = "yes" ] || return 1
  fi
  code="$(catalog DELETE "/catalog/v1/$PREFIX/namespaces/$SCHEMA")"
  # The REST catalog does not cascade on DROP, so the tables above go first; a namespace that
  # still holds one stays and this answers 404/409.
  [ "$code" = "204" ] || [ "$code" = "200" ]
}

# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  set +e
  local schema_deleted=no cm_deleted=no pod_deleted=no
  if [ "$schema_created" = "yes" ]; then
    if drop_schema; then schema_deleted=yes; fi
  else
    schema_deleted=yes
  fi
  if [ "$cm_created" = "yes" ]; then
    if kubectl -n "$NS" delete configmap "$CM" --ignore-not-found >/dev/null 2>&1; then cm_deleted=yes; fi
  else
    cm_deleted=yes
  fi
  if [ "$pod_created" = "yes" ]; then
    if kubectl -n "$NS" delete pod "$POD" --ignore-not-found --timeout=150s >/dev/null 2>&1; then pod_deleted=yes; fi
    if [ "$(kubectl -n "$NS" get pod "$POD" --ignore-not-found -o name 2>/dev/null | wc -l)" = "0" ]; then pod_deleted=yes; fi
  else
    pod_deleted=yes
  fi
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  rm -f "$tmp/token" "$tmp/token.json" "$tmp/auth" "$tmp/client-id" "$tmp/client-secret"
  rm -rf "$tmp"
  echo "cleanup schema-deleted=$schema_deleted cm-deleted=$cm_deleted pod-deleted=$pod_deleted"
}
trap '{ [ "$finished" = yes ] || cleanup; }' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for tool in kubectl curl jq python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "verify-spark-pipeline: UNVERIFIED ($tool not found)"; exit 3; }
done

# --- the shared server, the principal, the identity endpoint ------------------------------
if ! kubectl -n "$NS" get deploy spark-connect >/dev/null 2>&1; then
  echo "verify-spark-pipeline: UNVERIFIED (the shared Connect server is not installed)"
  exit 3
fi
if ! kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" >/dev/null 2>&1; then
  echo "verify-spark-pipeline: UNVERIFIED (the demo principal is not installed — make demo-access)"
  exit 3
fi
token_url="$(kubectl -n "$NS" get cm stardelt-identity -o jsonpath='{.data.tokenEndpoint}' 2>/dev/null || true)"
if [ -z "$token_url" ]; then
  echo "verify-spark-pipeline: UNVERIFIED (ConfigMap stardelt-identity has no tokenEndpoint — make install-identity)"
  exit 3
fi

# --- step 2: the image under test comes from the cluster, not a literal -------------------
IMAGE="$(kubectl -n "$NS" get deploy spark-connect -o jsonpath='{.spec.template.spec.containers[0].image}')"
case "$IMAGE" in
  *@sha256:*) : ;;
  *) echo "verify-spark-pipeline: UNVERIFIED (spark-connect runs $IMAGE, no digest)"; exit 3 ;;
esac
echo "image=$IMAGE"

# The principal's keys, for the build plane's own catalogue calls; the pod reads them itself
# through `secretKeyRef`.
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

if ! forward_lk; then
  echo "verify-spark-pipeline: UNVERIFIED (lakekeeper did not answer within 30s)"
  exit 3
fi
auth_ready
PREFIX="$(curl -sS -H @"$tmp/auth" "$LK/catalog/v1/config?warehouse=$CATALOG" \
  | jq -r '.defaults.prefix // empty' 2>/dev/null || true)"
[ -n "$PREFIX" ] || { echo "verify-spark-pipeline: UNVERIFIED (the catalog config named no warehouse prefix)"; exit 3; }
code="$(catalog GET "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables")"
if [ "$code" = "200" ]; then
  echo "verify-spark-pipeline: FAIL (namespace $SCHEMA already holds tables; refusing to touch someone's data)"
  exit 1
fi
printf '{"namespace":["%s"],"properties":{}}' "$SCHEMA" > "$tmp/namespace.json"
code="$(catalog POST "/catalog/v1/$PREFIX/namespaces" "$tmp/namespace.json")"
if [ "$code" = "200" ] || [ "$code" = "204" ]; then schema_created=yes; fi
[ "$schema_created" = "yes" ] || { echo "verify-spark-pipeline: FAIL (the catalog refused namespace $SCHEMA, code $code)"; exit 1; }

# --- step 3: the graph (ConfigMap, projected into /opt/graph) -----------------------------
cat > "$tmp/spark-pipeline.yml" <<EOF
name: verify-spark-pipeline
libraries:
  - glob:
      include: tables.sql
EOF
cat > "$tmp/tables.sql" <<EOF
CREATE MATERIALIZED VIEW $TABLE AS SELECT id FROM range(10);
EOF

kubectl -n "$NS" create configmap "$CM" \
  --from-file=spark-pipeline.yml="$tmp/spark-pipeline.yml" \
  --from-file=tables.sql="$tmp/tables.sql" \
  --dry-run=client -o yaml 2>/dev/null \
  | kubectl apply -f - >/dev/null && cm_created=yes
[ "$cm_created" = "yes" ] || { echo "verify-spark-pipeline: FAIL (could not create ConfigMap $CM)"; exit 1; }

# --- step 4: the task pod, on the loader's shape -----------------------------------------
# The pod's runner is the loader's (`demos/dags/stardelt_pipelines/spark_pipeline.py::RUNNER`):
# copy the graph under its paths, write the keys stardelt owns and the eleven catalog keys,
# mint the principal's token into the spec under /tmp, run the CLI on an empty `SPARK_CONF_DIR`.
# It prints the CLI's output redacted, then one fresh session's row count.
cat > "$tmp/runner.py" <<'PY'
import glob
import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

GRAPH = Path("/opt/graph")
WORK = Path("/tmp/verify-spark-pipeline")
CATALOG = os.environ["SPARK_CATALOG"]
DATABASE = os.environ["SPARK_DATABASE"]
TABLE = os.environ["SPARK_TABLE"]

work = WORK / "graph"
work.mkdir(parents=True, exist_ok=True)
for name in ("spark-pipeline.yml", "tables.sql"):
    (work / name).write_bytes((GRAPH / name).read_bytes())

spec = work / "spark-pipeline.yml"
document = yaml.safe_load(spec.read_text())
document["catalog"] = CATALOG
document["database"] = DATABASE
document["storage"] = os.environ["SPARK_STORAGE"]
configuration = {
    "spark.remote": os.environ["SPARK_REMOTE"],
    f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{CATALOG}.type": "rest",
    f"spark.sql.catalog.{CATALOG}.rest-catalog-purge": "true",
    f"spark.sql.catalog.{CATALOG}.uri": os.environ["CATALOG_URI"],
    f"spark.sql.catalog.{CATALOG}.warehouse": CATALOG,
    f"spark.sql.catalog.{CATALOG}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    f"spark.sql.catalog.{CATALOG}.header.X-Iceberg-Access-Delegation": "vended-credentials",
    f"spark.sql.catalog.{CATALOG}.s3.endpoint": os.environ["S3_ENDPOINT"],
    f"spark.sql.catalog.{CATALOG}.s3.region": "us-east-1",
    f"spark.sql.catalog.{CATALOG}.s3.path-style-access": "true",
}

# The engine's own CA variables would make urllib distrust the realm's certificate; the spike
# dropped them the same way (measured #957).
for name in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
    os.environ.pop(name, None)
token = Path("/var/run/secrets/stardelt-catalog/token").read_text().strip()
configuration[f"spark.sql.catalog.{CATALOG}.token"] = token
document["configuration"] = configuration
spec.write_text(yaml.safe_dump(document, sort_keys=False))
s3_keys = sum(
    1
    for key in configuration
    if key.startswith(f"spark.sql.catalog.{CATALOG}.s3.")
    and key.endswith(("access-key-id", "secret-access-key"))
)
print(f"s3-key-in-spec={s3_keys}")

conf = WORK / "conf"
conf.mkdir(parents=True, exist_ok=True)
env = dict(os.environ, SPARK_CONF_DIR=str(conf), HOME="/tmp", SPARK_HOME="/opt/spark")
print(f"running spark-pipelines run against {configuration['spark.remote']}", flush=True)
result = subprocess.run(
    ["/opt/spark/bin/spark-pipelines", "run", "--spec", str(spec)],
    cwd=str(work),
    env=env,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
)
print(result.stdout.replace(token, "<token>"))
print(f"credential=projected token-bytes={len(token)}")
print(f"run exit={result.returncode}")

READBACK = r"""
import glob, json, sys
sys.path.insert(0, "/opt/spark/python")
sys.path.insert(0, sorted(glob.glob("/opt/spark/python/lib/py4j-*-src.zip"))[-1])
from pyspark.sql import SparkSession
plan = json.load(open(sys.argv[1]))
builder = SparkSession.builder.remote(plan["remote"]).appName("verify-spark-pipeline-readback")
for key, value in plan["configuration"].items():
    builder = builder.config(key, value)
session = builder.create()
try:
    print("rows=%d" % session.table(plan["table"]).count())
finally:
    session.stop()
"""

if result.returncode == 0 and "Run is COMPLETED" in result.stdout:
    plan = WORK / "readback.json"
    handle = os.open(plan, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.write(
        handle,
        json.dumps(
            {
                "remote": configuration["spark.remote"],
                "configuration": {
                    key: value
                    for key, value in configuration.items()
                    if key != "spark.remote"
                },
                "table": f"{CATALOG}.{DATABASE}.{TABLE}",
            }
        ).encode(),
    )
    os.close(handle)
    readback = subprocess.run(
        [sys.executable, "-c", READBACK, str(plan)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(readback.stdout.replace(token, "<token>"))
    if readback.returncode != 0:
        print("readback exit=%d" % readback.returncode)
raise SystemExit(result.returncode)
PY

runner_json="$(python3 -c 'import json,sys; print(json.dumps(open(sys.argv[1]).read()))' "$tmp/runner.py")"

# The two labels `spark-connect-ingress` selects: `app.kubernetes.io/name=pipeline-spark-pipeline`
# and the owner group, with no `stardelt.io/group.visitors`. `component=singleuser-server` is the
# notebook's pair and is not added. A visitors label is not admitted: the policy drops the packet
# silently, so the client hangs in its gRPC connect until this check's timeout (measured #990).
pod_manifest() {
  cat <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  namespace: $NS
  labels:
    app.kubernetes.io/name: pipeline-spark-pipeline
    app.kubernetes.io/part-of: stardelt
    stardelt.io/group.$GROUP: "true"
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  serviceAccountName: $PRINCIPAL_SA
  imagePullSecrets:
    - name: ghcr-creds
  securityContext:
    runAsNonRoot: true
    runAsUser: 185
    runAsGroup: 185
    fsGroup: 185
    seccompProfile: { type: RuntimeDefault }
  containers:
    - name: runner
      image: $IMAGE
      imagePullPolicy: IfNotPresent
      command: ["/opt/python313/bin/python3", "-c", $runner_json]
      env:
        - { name: HOME, value: /tmp }
        - { name: SPARK_HOME, value: /opt/spark }
        - { name: SPARK_CATALOG, value: "$CATALOG" }
        - { name: SPARK_DATABASE, value: "$SCHEMA" }
        - { name: SPARK_TABLE, value: "$TABLE" }
        - { name: SPARK_STORAGE, value: "s3a://$BUCKET/pipelines/$GROUP/verify-spark-pipeline/$TABLE/checkpoints" }
        - { name: SPARK_REMOTE, value: "$CONNECT_URL" }
        - { name: CATALOG_URI, value: "http://lakekeeper.$NS.svc.cluster.local:8181/catalog" }
        - { name: S3_ENDPOINT, value: "http://seaweedfs-s3.$NS.svc.cluster.local:8333" }
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: { drop: ["ALL"] }
      resources:
        requests: { cpu: 250m, memory: 1Gi }
        limits: { cpu: "2", memory: 3Gi }
      volumeMounts:
        - { name: graph, mountPath: /opt/graph, readOnly: true }
        - { name: tmp, mountPath: /tmp }
        - { name: catalog-token, mountPath: /var/run/secrets/stardelt-catalog, readOnly: true }
  volumes:
    - name: graph
      configMap:
        name: $CM
        items:
          - { key: spark-pipeline.yml, path: spark-pipeline.yml }
          - { key: tables.sql, path: tables.sql }
    - name: tmp
      emptyDir: {}
    - name: catalog-token
      projected:
        sources:
          - serviceAccountToken:
              audience: stardelt-catalog-workload
              expirationSeconds: 3600
              path: token
EOF
}

# A pod left by an interrupted run would give this one its old log; it is this check's own name.
kubectl -n "$NS" delete pod "$POD" --ignore-not-found --timeout=120s >/dev/null 2>&1 || true
pod_manifest > "$tmp/pod.yaml"
if grep -q 'secretKeyRef' "$tmp/pod.yaml"; then
  echo "verify-spark-pipeline: FAIL (the pod carries a Secret reference)"
  exit 1
fi
if kubectl apply -f "$tmp/pod.yaml" >/dev/null 2>&1; then pod_created=yes; fi
[ "$pod_created" = "yes" ] || { echo "verify-spark-pipeline: FAIL (the API server refused pod $POD)"; exit 1; }

phase=""
reason=""
deadline=$(( SECONDS + TIMEOUT ))
while [ "$SECONDS" -lt "$deadline" ]; do
  phase="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  case "$phase" in
    Succeeded|Failed) break ;;
  esac
  reason="$(kubectl -n "$NS" get pod "$POD" \
    -o jsonpath='{.status.containerStatuses[0].state.waiting.reason}' 2>/dev/null || true)"
  case "$reason" in
    ErrImagePull|ImagePullBackOff|InvalidImageName|CreateContainerConfigError|CreateContainerError) break ;;
  esac
  sleep 3
done

if [ "$phase" != "Succeeded" ] && [ "$phase" != "Failed" ]; then
  early="$(kubectl -n "$NS" logs "$POD" --tail=20 2>&1 | tail -n 5 | text || true)"
  if [ -z "$phase" ] || [ -n "$reason" ]; then
    detail="$(kubectl -n "$NS" describe pod "$POD" 2>/dev/null | grep -m1 -E 'Warning|Error|Failed|reason' | text || true)"
    echo "verify-spark-pipeline: UNVERIFIED (the task pod never started: phase=${phase:-absent} reason=${reason:-none} ${detail})"
  else
    echo "verify-spark-pipeline: UNVERIFIED (the task pod did not finish within ${TIMEOUT}s and did not reach the Connect server at $CONNECT_URL: phase=$phase last=\"$early\")"
  fi
  exit 3
fi

logs="$(kubectl -n "$NS" logs "$POD" --tail=400 2>&1 || true)"

# A pod that never reached the server is not a wrong graph (#957 Q6): a stopped server, a
# refused connection and a refused NetworkPolicy all look like this from inside the client.
unreachable="$(printf '%s\n' "$logs" \
  | grep -m1 -E 'UNAVAILABLE|Connection refused|Cannot assign requested address|Name or service not known|Temporary failure in name resolution|Network is unreachable' || true)"
if [ -z "$(printf '%s\n' "$logs" | grep -F 'Run is COMPLETED' || true)" ] && [ -n "$unreachable" ]; then
  echo "verify-spark-pipeline: UNVERIFIED (the task pod did not reach the Connect server: $(printf '%s' "$unreachable" | text))"
  exit 3
fi

bad=""
# PASS 1 — the run.
if printf '%s\n' "$logs" | grep -qF 'Run is COMPLETED'; then
  printf 'PASS %-10s%s\n' run "spark-pipelines run is COMPLETED"
else
  bad="$bad run"
fi

# PASS 2 — the flows.
flows="$(printf '%s\n' "$logs" | grep -cF 'has COMPLETED' || true)"
if [ "${flows:-0}" -ge 1 ]; then
  printf 'PASS %-10s%s\n' flows "$flows flow(s) completed"
else
  bad="$bad flows"
fi

# PASS 3 — the catalog holds the table with a main snapshot (the principal's token and the
# vended credential did the write; the spec carries no S3 key).
table_code="$(catalog GET "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables/$TABLE")"
snapshot="$(jq -r '.metadata.refs.main["snapshot-id"] // empty' "$tmp/out.json" 2>/dev/null || true)"
if [ "$table_code" = "200" ] && [ -n "$snapshot" ]; then
  printf 'PASS %-10s%s\n' table "$CATALOG.$SCHEMA.$TABLE has main snapshot $snapshot"
else
  bad="$bad table"
fi

# PASS 4 — a fresh session reads the commit back.
if printf '%s\n' "$logs" | grep -qx 'rows=10'; then
  printf 'PASS %-10s%s\n' rows "count(*) = 10 through a fresh session"
else
  bad="$bad rows"
fi

# PASS 5 — the pod held only the projected catalog token.
if printf '%s\n' "$logs" | grep -q '^credential=projected'; then
  printf 'PASS %-12s%s\n' credential "one projected catalog token, no Secret in the pod"
else
  bad="$bad credential"
fi

if printf '%s\n' "$logs" | grep -q '^s3-key-in-spec=[1-9]'; then
  bad="$bad s3-key"
fi

if [ -n "$bad" ]; then
  printf '%s\n' "$logs" | tail -n 40
fi

cleanup
finished=yes

if [ -n "$bad" ]; then
  echo "verify-spark-pipeline: FAIL (did not hold:$bad)"
  exit 1
fi
echo "verify-spark-pipeline: PASS (5 of 5)"
exit 0
