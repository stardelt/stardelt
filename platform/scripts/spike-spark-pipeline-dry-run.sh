#!/usr/bin/env bash
# Spike for stardelt/stardelt#1236 (#80 point 3): the `spark-pipelines dry-run` before a deploy.
#
#   make spike-spark-pipeline-dry-run      (NS=… the platform namespace, TIMEOUT=… the pod wait)
#
# Why: the loader already runs `spark-pipelines dry-run` before `spark-pipelines run` in the task
# pod (`demos/dags/stardelt_pipelines/spark_pipeline.py::steps`, #969). #80 point 3 wants the same
# verdict in `POST /api/pipelines/validate` and in `stardelt bundle plan`, before a deploy. That
# needs two facts nobody has measured: how long a dry run takes on a realistic graph (DR1) and how
# good its errors are — does it name the file, the dataset and the table, does it catch the
# mistakes a person makes (DR2–DR5), and does it need the catalog at all (DR6)?
#
# One pod of the image the shared Connect server runs, six CLI invocations, one `DRn ` line each.
# The graph trees come from ConfigMap `spike-dry-run-graph` as flat keys (`dr1--sql--m01.sql`);
# the pod's runner copies them under their paths (the loader's own shape).
#
#   DR1  ten chained materialized views over `range(0, 1000)`; `duration=`, `ok=`.
#   DR2  a view selecting a column that does not exist on its source dataset: does the error name
#        the file and the dataset?
#   DR3  a cycle (a from b, b from a): is it caught?
#   DR4  a view selecting from a table that does not exist: is it caught, and named?
#   DR5  a graph the dry run accepts whose run then fails (`CAST('not-a-number' AS INT)` is
#        analysis-clean and fails at execution): is "dry run passed" a strong signal?
#   DR6  the same clean graph against a catalog URI nothing listens on: does the dry run need the
#        catalog (could it run in `bundle plan` on a laptop)?
#
# The answer to DR5 decides how much weight a validate route may put on the dry run; DR6 decides
# which of the three carriers (Nova's validate pod, the loader, `bundle plan` locally) can host it.
#
# Exit 0 = PASS (six `DRn ` lines), 1 = a line is missing or contradicts the script, 3 =
# UNVERIFIED (no kubectl, the server or the principal is not installed, the pod never started or
# did not end `Succeeded` — nothing about the dry run was measured then). A dry run that fails is
# a measurement, not a script failure: DR2–DR6 print `caught=no` and the script still passes.
#
# Two traps this script carries:
#   - `umask 077`: the principal's client credentials and the minted token live in mode-0600 files
#     under $HOME and in the pod's /tmp; the pod redacts the token before anything leaves;
#   - `cleanup` runs on EXIT, INT and TERM: the scratch schema, the ConfigMap, the pod and the
#     port-forward are removed whatever happened. The schema is dropped from inside the pod first
#     (through the Connect server — the task pod's egress policy has no route to the catalog);
#     `cleanup` repeats it from the build plane if that failed.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-${NAMESPACE:-stardelt}}"
POD="${POD:-spike-dry-run}"
CM="${CM:-spike-dry-run-graph}"
CATALOG="${CATALOG:-playground}"
DATABASE="${DATABASE:-spike_dry_run}"
GROUP="${GROUP:-demo}"
PRINCIPAL_SECRET="${PRINCIPAL_SECRET:-sp-pipeline-$GROUP}"
CONNECT_URL="${CONNECT_URL:-sc://spark-connect.$NS.svc.cluster.local:15002}"
CATALOG_URI="${CATALOG_URI:-http://lakekeeper.$NS.svc.cluster.local:8181/catalog}"
S3_ENDPOINT="${S3_ENDPOINT:-http://seaweedfs-s3.$NS.svc.cluster.local:8333}"
BUCKET="${BUCKET:-lakehouse}"
# The port DR6 points at: nothing in the pod listens there, so a catalog call is refused at once
# rather than hanging into the CLI's timeout.
DEAD_CATALOG_URI="${DEAD_CATALOG_URI:-http://127.0.0.1:9/catalog}"
LK_PORT="${LK_PORT:-18185}"
LK="http://127.0.0.1:$LK_PORT"
TIMEOUT="${TIMEOUT:-1800}"

scratch="${TMPDIR:-$HOME/.cache}"
mkdir -p "$scratch"
tmp="$(mktemp -d "$scratch/spike-dry-run.XXXXXX")"
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
  code="$(catalog GET "/catalog/v1/$PREFIX/namespaces/$DATABASE/tables")"
  if [ "$code" = "200" ]; then
    for name in $(jq -r '.identifiers[]?.name' "$tmp/out.json" 2>/dev/null); do
      code="$(catalog DELETE "/catalog/v1/$PREFIX/namespaces/$DATABASE/tables/$name?purgeRequested=true")"
      [ "$code" = "204" ] || [ "$code" = "200" ] || tables_ok=no
    done
    [ "$tables_ok" = "yes" ] || return 1
  fi
  code="$(catalog DELETE "/catalog/v1/$PREFIX/namespaces/$DATABASE")"
  [ "$code" = "204" ] || [ "$code" = "200" ] || [ "$code" = "404" ]
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
  echo "cleanup schema-deleted=$schema_deleted cm-deleted=$cm_deleted pod-deleted=$pod_deleted" >&2
}
trap '{ [ "$finished" = yes ] || cleanup; }' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for tool in kubectl curl jq python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "spike-spark-pipeline-dry-run: UNVERIFIED ($tool not found)"; exit 3; }
done

# --- the server, the principal, the identity endpoint -------------------------------------
if ! kubectl -n "$NS" get deploy spark-connect >/dev/null 2>&1; then
  echo "spike-spark-pipeline-dry-run: UNVERIFIED (the shared Connect server is not installed)"
  exit 3
fi
if ! kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" >/dev/null 2>&1; then
  echo "spike-spark-pipeline-dry-run: UNVERIFIED (the demo principal is not installed — make demo-access)"
  exit 3
fi
token_url="$(kubectl -n "$NS" get cm stardelt-identity -o jsonpath='{.data.tokenEndpoint}' 2>/dev/null || true)"
if [ -z "$token_url" ]; then
  echo "spike-spark-pipeline-dry-run: UNVERIFIED (ConfigMap stardelt-identity has no tokenEndpoint — make install-identity)"
  exit 3
fi

# The image under test comes from the cluster, not a literal: the dry run must run in the image
# the pipeline task pod runs, and that is the image the shared server runs (`spark-pipelines` is
# part of the Spark distribution; the notebook image has no Spark).
IMAGE="$(kubectl -n "$NS" get deploy spark-connect -o jsonpath='{.spec.template.spec.containers[0].image}')"
case "$IMAGE" in
  *@sha256:*) : ;;
  *) echo "spike-spark-pipeline-dry-run: UNVERIFIED (spark-connect runs $IMAGE, no digest)"; exit 3 ;;
esac
echo "image=$IMAGE"

kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" -o jsonpath='{.data.client-id}' \
  | base64 -d > "$tmp/client-id" || { echo "Secret $PRINCIPAL_SECRET has no client-id" >&2; exit 1; }
kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" -o jsonpath='{.data.client-secret}' \
  | base64 -d > "$tmp/client-secret" || { echo "Secret $PRINCIPAL_SECRET has no client-secret" >&2; exit 1; }
chmod 600 "$tmp"/*

if ! forward_lk; then
  echo "spike-spark-pipeline-dry-run: UNVERIFIED (lakekeeper did not answer within 30s)"
  exit 3
fi
auth_ready
PREFIX="$(curl -sS -H @"$tmp/auth" "$LK/catalog/v1/config?warehouse=$CATALOG" \
  | jq -r '.defaults.prefix // empty' 2>/dev/null || true)"
[ -n "$PREFIX" ] || { echo "spike-spark-pipeline-dry-run: UNVERIFIED (the catalog config named no warehouse prefix for $CATALOG)"; exit 3; }

# The scratch schema is this measurement's own; a table in it means an earlier run left data, and
# this script does not delete someone's tables to start.
code="$(catalog GET "/catalog/v1/$PREFIX/namespaces/$DATABASE")"
if [ "$code" = "200" ]; then
  code="$(catalog GET "/catalog/v1/$PREFIX/namespaces/$DATABASE/tables")"
  if [ "$code" = "200" ] && [ "$(jq -r '.identifiers | length' "$tmp/out.json" 2>/dev/null || echo 0)" != "0" ]; then
    echo "spike-spark-pipeline-dry-run: FAIL ($CATALOG.$DATABASE already holds tables; refusing to touch someone's data)"
    exit 1
  fi
else
  printf '{"namespace":["%s"],"properties":{}}' "$DATABASE" > "$tmp/namespace.json"
  code="$(catalog POST "/catalog/v1/$PREFIX/namespaces" "$tmp/namespace.json")"
  if [ "$code" = "200" ] || [ "$code" = "204" ]; then schema_created=yes; fi
  [ "$schema_created" = "yes" ] || { echo "spike-spark-pipeline-dry-run: FAIL (the catalog refused namespace $CATALOG.$DATABASE, code $code)"; exit 1; }
fi
echo "schema=$CATALOG.$DATABASE"

# --- the six graphs (one ConfigMap, flat keys) --------------------------------------------
# Every graph is generated here, so the SQL a person would get wrong is the SQL a person writes.
mkdir -p "$tmp/graphs"
previous=""
for index in 01 02 03 04 05 06 07 08 09 10; do
  if [ -z "$previous" ]; then
    printf 'CREATE MATERIALIZED VIEW mv_%s AS SELECT id FROM range(0, 1000);\n' "$index" \
      > "$tmp/graphs/dr1--sql--mv$index.sql"
  else
    printf 'CREATE MATERIALIZED VIEW mv_%s AS SELECT id + 1 AS id FROM mv_%s;\n' "$index" "$previous" \
      > "$tmp/graphs/dr1--sql--mv$index.sql"
  fi
  previous="$index"
done
# DR2: a column the source dataset in the same graph does not have.
printf 'CREATE MATERIALIZED VIEW dr2_base AS SELECT id FROM range(10);\n' > "$tmp/graphs/dr2--sql--01_base.sql"
printf 'CREATE MATERIALIZED VIEW dr2_view AS SELECT no_such_column FROM dr2_base;\n' > "$tmp/graphs/dr2--sql--02_view.sql"
# DR3: a cycle.
printf 'CREATE MATERIALIZED VIEW cyc_a AS SELECT id FROM cyc_b;\n' > "$tmp/graphs/dr3--sql--a.sql"
printf 'CREATE MATERIALIZED VIEW cyc_b AS SELECT id FROM cyc_a;\n' > "$tmp/graphs/dr3--sql--b.sql"
# DR4: a table that does not exist, in the spec's own catalog and database.
printf 'CREATE MATERIALIZED VIEW dr4_view AS SELECT * FROM %s.%s.no_such_table;\n' "$CATALOG" "$DATABASE" \
  > "$tmp/graphs/dr4--sql--view.sql"
# DR5: analysis-clean, fails when it executes (a string is not a number).
printf "CREATE MATERIALIZED VIEW dr5_view AS SELECT CAST('not-a-number' AS INT) AS broken FROM range(10);\n" \
  > "$tmp/graphs/dr5--sql--view.sql"

# --- the in-pod runner --------------------------------------------------------------------
# One program, six CLI invocations. It owns the spec it writes (catalog, database, storage, the
# catalog block and the token), so no key of the principal is in the ConfigMap, and it drops the
# scratch schema through the Connect server last — the task pod's egress policy reaches the server
# and the realm but not the catalog.
cat > "$tmp/graphs/runner.py" <<'PY'
import json
import os
import ssl
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

GRAPH = Path("/opt/graph")
WORK = Path("/tmp/spike-dry-run")
CLI = "/opt/spark/bin/spark-pipelines"
CATALOG = os.environ["SPARK_CATALOG"]
DATABASE = os.environ["SPARK_DATABASE"]
REMOTE = os.environ["SPARK_REMOTE"]
CATALOG_URI = os.environ["CATALOG_URI"]
DEAD_CATALOG_URI = os.environ["DEAD_CATALOG_URI"]
S3_ENDPOINT = os.environ["S3_ENDPOINT"]
STORAGE = os.environ["SPARK_STORAGE"]
LIMIT = int(os.environ.get("SPIKE_CLI_TIMEOUT", "600"))


def text(value, width=200):
    return " ".join(str(value).split())[:width]


def mint():
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
    with urllib.request.urlopen(request, timeout=30, context=ssl.create_default_context()) as answer:
        return json.load(answer)["access_token"]


TOKEN = mint()


def redact(value):
    return value.replace(TOKEN, "<token>") if TOKEN else value


def configuration(catalog_uri):
    return {
        "spark.remote": REMOTE,
        f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG}.type": "rest",
        f"spark.sql.catalog.{CATALOG}.rest-catalog-purge": "true",
        f"spark.sql.catalog.{CATALOG}.uri": catalog_uri,
        f"spark.sql.catalog.{CATALOG}.warehouse": CATALOG,
        f"spark.sql.catalog.{CATALOG}.token": TOKEN,
        f"spark.sql.catalog.{CATALOG}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{CATALOG}.header.X-Iceberg-Access-Delegation": "vended-credentials",
        f"spark.sql.catalog.{CATALOG}.s3.endpoint": S3_ENDPOINT,
        f"spark.sql.catalog.{CATALOG}.s3.region": "us-east-1",
        f"spark.sql.catalog.{CATALOG}.s3.path-style-access": "true",
    }


def tree(tag):
    """The graph tree for <tag>, copied out of the read-only ConfigMap so the spec can sit in it."""
    out = WORK / "graph" / tag
    for source in sorted((GRAPH / tag).rglob("*")):
        if not source.is_file():
            continue
        target = out / source.relative_to(GRAPH / tag)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
    return out


def spec(directory, catalog_uri=CATALOG_URI):
    """The spec stardelt owns: catalog, database, storage and the catalog block."""
    document = {
        "name": "spike-dry-run-" + directory.name,
        "storage": STORAGE,
        "catalog": CATALOG,
        "database": DATABASE,
        "configuration": configuration(catalog_uri),
        # `sql/**` is the folder form the CLI's glob accepts; a bare `sql/` matches the directory
        # and no file (`pyspark/pipelines/cli.py::validate_patch_glob_pattern`).
        "libraries": [{"glob": {"include": "sql/**"}}],
    }
    path = directory / "spark-pipeline.yml"
    import yaml

    path.write_text(yaml.safe_dump(document, sort_keys=False))
    return path


def cli(spec_path, step):
    conf = WORK / "conf"
    conf.mkdir(parents=True, exist_ok=True)
    env = dict(
        os.environ,
        SPARK_CONF_DIR=str(conf),
        HOME="/tmp",
        SPARK_HOME="/opt/spark",
        PYTHONUNBUFFERED="1",
        PYSPARK_PYTHON=sys.executable,
    )
    started = time.monotonic()
    try:
        result = subprocess.run(
            [CLI, step, "--spec", str(spec_path)],
            cwd=str(spec_path.parent),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=LIMIT,
        )
        code, log = result.returncode, result.stdout
    except subprocess.TimeoutExpired as expired:
        code, log = 124, (expired.output or "") + "\n<timeout %ss>" % LIMIT
    seconds = int(round(time.monotonic() - started))
    return code, seconds, redact(log)


def lines(log):
    return [line.rstrip() for line in log.splitlines() if line.strip()]


def errors(log):
    """The failure lines of a run, without the Python stack frames around them."""
    plain = [
        line
        for line in lines(log)
        if not line.lstrip().startswith(("at ", "File ", "self.", "...", "- "))
    ]
    found = [
        line
        for line in plain
        if any(
            word in line
            for word in (
                "Error",
                "Exception",
                "Cannot",
                "cannot",
                "Caused by",
                "refused",
                "UNAVAILABLE",
            )
        )
    ]
    return found or plain[-3:]


def evidence(log, width=240, count=3):
    return [text(line, width) for line in errors(log)[:count]]


def mentions(log, needle, width=240):
    """The lines of a run's output that carry <needle> (where the file or dataset shows up)."""
    return [text(line, width) for line in lines(log) if needle in line][:2]


def say(*parts):
    print(" ".join(parts), flush=True)


def run(tag):
    directory = tree(tag)
    return directory, spec(directory)


try:
    # DR1 — ten datasets, the duration a pre-deploy check would pay.
    directory, path = run("dr1")
    code, seconds, log = cli(path, "dry-run")
    say(f"DR1 duration={seconds}s datasets=10 ok={'yes' if code == 0 else 'no'}")
    if code != 0:
        for line in evidence(log):
            say("  ·", text(line, 240))

    # DR2 — a missing column: which of file and dataset does the error name?
    directory, path = run("dr2")
    code, seconds, log = cli(path, "dry-run")
    failure = " ".join(errors(log))
    say(
        "DR2"
        f" caught={'yes' if code != 0 else 'no'}"
        f" names-file={'yes' if '02_view.sql' in log else 'no'}"
        f" names-dataset={'yes' if 'dr2_view' in log else 'no'}"
        f" error-names-file={'yes' if '02_view.sql' in failure else 'no'}"
        f" error-names-dataset={'yes' if 'dr2_view' in failure else 'no'}"
    )
    for line in evidence(log) + mentions(log, "02_view.sql"):
        say("  ·", text(line, 240))

    # DR3 — a cycle.
    directory, path = run("dr3")
    code, seconds, log = cli(path, "dry-run")
    say(f"DR3 caught={'yes' if code != 0 else 'no'}")
    for line in evidence(log):
        say("  ·", text(line, 240))

    # DR4 — a missing table.
    directory, path = run("dr4")
    code, seconds, log = cli(path, "dry-run")
    failure = " ".join(errors(log))
    say(
        "DR4"
        f" caught={'yes' if code != 0 else 'no'}"
        f" names-table={'yes' if 'no_such_table' in log else 'no'}"
        f" error-names-table={'yes' if 'no_such_table' in failure else 'no'}"
    )
    for line in evidence(log):
        say("  ·", text(line, 240))

    # DR5 — the dry run accepts the graph and the run then fails: how strong is the signal?
    directory, path = run("dr5")
    dry_code, _, dry_log = cli(path, "dry-run")
    if dry_code != 0:
        say("DR5 dry=caught run=skipped")
        for line in evidence(dry_log):
            say("  ·", text(line, 240))
    else:
        run_code, _, run_log = cli(path, "run")
        say(f"DR5 dry=ok run={'ok' if run_code == 0 else 'failed'}")
        if run_code != 0:
            for line in evidence(run_log):
                say("  ·", text(line, 240))

    # DR6 — the same clean graph against a catalog URI nothing listens on.
    directory = tree("dr1")
    path = spec(directory, DEAD_CATALOG_URI)
    code, seconds, log = cli(path, "dry-run")
    say(f"DR6 fails={'yes' if code != 0 else 'no'} seconds={seconds}")
    for line in evidence(log, count=6):
        say("  ·", text(line, 240))
finally:
    # The scratch schema goes from inside the pod, through the Connect server (see the header).
    try:
        sys.path.insert(0, "/opt/spark/python")
        import glob

        sys.path.insert(0, sorted(glob.glob("/opt/spark/python/lib/py4j-*-src.zip"))[-1])
        from pyspark.sql import SparkSession

        builder = SparkSession.builder.remote(REMOTE).appName("spike-dry-run-cleanup")
        for key, value in configuration(CATALOG_URI).items():
            if key != "spark.remote":
                builder = builder.config(key, value)
        session = builder.create()
        try:
            tables = [row[1] for row in session.sql(f"SHOW TABLES IN {CATALOG}.{DATABASE}").collect()]
            for table in tables:
                session.sql(f"DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.{table} PURGE")
            session.sql(f"DROP NAMESPACE IF EXISTS {CATALOG}.{DATABASE} CASCADE")
            say(f"cleanup drop-schema={CATALOG}.{DATABASE} tables-dropped={len(tables)}")
        finally:
            session.stop()
    except Exception as failure:  # the build plane repeats the drop in this script's trap
        say("cleanup drop-schema=failed", text(failure, 200))
PY

# --- the pod (the task pod's shape, its own labels) ---------------------------------------
# ConfigMap keys may carry alphanumerics and `-._` only, so `dr1--sql--mv01.sql` is stored as
# `dr1__sql__mv01_sql`; `SPIKE_PATHS` tells the pod where each key belongs (the loader's shape).
args=()
items=""
for file in "$tmp/graphs"/*; do
  [ -f "$file" ] || continue
  name="$(basename "$file")"
  key="$(printf '%s' "$name" | tr '.-' '__')"
  case "$name" in
    runner.py) path="runner.py" ;;
    *--sql--*) path="${name/--sql--//sql/}" ;;
    *) path="$name" ;;
  esac
  args+=("--from-file=$key=$file")
  items="$items          - { key: $key, path: $path }
"
done

kubectl -n "$NS" create configmap "$CM" "${args[@]}" \
  --dry-run=client -o yaml 2>/dev/null \
  | kubectl apply -f - >/dev/null && cm_created=yes
[ "$cm_created" = "yes" ] || { echo "spike-spark-pipeline-dry-run: FAIL (could not create ConfigMap $CM)"; exit 1; }
echo "graph=ConfigMap $CM keys=${#args[@]}"

cat > "$tmp/pod.yaml" <<EOF
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
      command: ["/opt/python313/bin/python3", "/opt/graph/runner.py"]
      env:
        - { name: HOME, value: /tmp }
        - { name: SPARK_HOME, value: /opt/spark }
        - { name: PYTHONUNBUFFERED, value: "1" }
        - { name: SPARK_CATALOG, value: "$CATALOG" }
        - { name: SPARK_DATABASE, value: "$DATABASE" }
        - { name: SPARK_STORAGE, value: "s3a://$BUCKET/pipelines/$GROUP/spike-dry-run/$DATABASE/checkpoints" }
        - { name: SPARK_REMOTE, value: "$CONNECT_URL" }
        - { name: CATALOG_URI, value: "$CATALOG_URI" }
        - { name: DEAD_CATALOG_URI, value: "$DEAD_CATALOG_URI" }
        - { name: S3_ENDPOINT, value: "$S3_ENDPOINT" }
        - { name: SPIKE_CLI_TIMEOUT, value: "600" }
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
        name: $CM
        items:
$items    - name: tmp
      emptyDir: {}
EOF

# A pod left by an interrupted run would give this one its old log; it is this spike's own name.
kubectl -n "$NS" delete pod "$POD" --ignore-not-found --timeout=120s >/dev/null 2>&1 || true
if kubectl apply -f "$tmp/pod.yaml" >/dev/null 2>&1; then pod_created=yes; fi
[ "$pod_created" = "yes" ] || { echo "spike-spark-pipeline-dry-run: FAIL (the API server refused pod $POD)"; exit 1; }

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

logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
printf '%s\n' "$logs"

if [ "$phase" != "Succeeded" ]; then
  printf '%s\n' "$logs" | tail -n 20
  if [ -z "$phase" ] || [ -n "$reason" ]; then
    detail="$(kubectl -n "$NS" describe pod "$POD" 2>/dev/null | grep -m1 -E 'Warning|Error|Failed|reason' | text || true)"
    echo "spike-spark-pipeline-dry-run: UNVERIFIED (the pod never started: phase=${phase:-absent} reason=${reason:-none} ${detail})"
  else
    echo "spike-spark-pipeline-dry-run: UNVERIFIED (the pod ended phase=$phase, not Succeeded; see the last 20 lines above)"
  fi
  exit 3
fi

rows="$(printf '%s\n' "$logs" | grep -cE '^DR[1-6] ' || true)"
if [ "${rows:-0}" -ne 6 ]; then
  echo "spike-spark-pipeline-dry-run: FAIL (${rows:-0} of 6 DR rows printed)"
  exit 1
fi
echo "spike-spark-pipeline-dry-run: PASS (6 rows)"
finished=yes
cleanup
exit 0
