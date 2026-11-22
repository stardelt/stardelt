#!/usr/bin/env bash
# Write-Audit-Publish, end to end (stardelt/stardelt#114, #1033).
#
#   make verify-wap                      (NS=… the platform namespace, TIMEOUT=… the run wait)
#
# Why: `publish: wap` has no release check. #114's slices landed and #1025 measured the three
# runs by hand once; `verify-release` had no row for it. A regression in the loader's
# `demos/dags/stardelt_pipelines/wap.py`, in the operator's `SPARK_WAP_BRANCH` wiring, in the
# `quality` task or in the catalog publish stays invisible until someone runs a demo by hand.
#
# This check proves, in three DAG runs of one temporary pipeline:
#   direct   a `publish: direct` run creates the table with a `main` snapshot;
#   publish  a `publish: wap` run whose check passes moves `main` and drops the audit branch;
#   refusal  a `publish: wap` run whose check finds rows moves nothing and keeps one
#            `audit_<16 hex>` branch with the 7-day TTL;
#   cleanup  the Pipeline and the table are gone (the configmap the operator rendered too).
#
# Exit 0 = PASS (all four), 1 = FAIL (a row did not hold), 3 = UNVERIFIED (no kubectl, no
# scheduler, no `sp-pipeline-demo` Secret, a loader too old for the warehouse prefix, or the DAG
# never appeared — nothing about WAP was measured then).
#
# Traps this script carries:
#   - `umask 077`: the principal's client credentials and every token go into mode-0600 files
#     under $HOME, and none of them is printed;
#   - `cleanup` runs on EXIT, INT and TERM: the Pipeline, the table and the port-forward are
#     removed whatever happened. The `scratch` schema is the demo's and stays; no other table is
#     touched;
#   - `airflow tasks logs` does not exist in Airflow 3.2 (#1025), so every assertion reads the
#     run state, the per-task states and the catalog's `metadata.refs` — never a task log.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-${NAMESPACE:-stardelt}}"
PIPELINE="${PIPELINE:-verify-wap}"
DAG="${DAG:-stardelt__$PIPELINE}"
CATALOG="${CATALOG:-playground}"
SCHEMA="${SCHEMA:-scratch}"
TABLE_NAME="${TABLE_NAME:-wap_gate}"
TABLE="$CATALOG.$SCHEMA.$TABLE_NAME"
GROUP="${GROUP:-demo}"
PRINCIPAL_SECRET="${PRINCIPAL_SECRET:-sp-pipeline-$GROUP}"
LK_PORT="${LK_PORT:-18184}"
TIMEOUT="${TIMEOUT:-900}"
LK="http://127.0.0.1:$LK_PORT"

scratch="${TMPDIR:-$HOME/.cache}"
mkdir -p "$scratch"
tmp="$(mktemp -d "$scratch/verify-wap.XXXXXX")"
LK_PID=""
PREFIX=""
pipeline_deleted=no
table_deleted=no
finished=no
bad=""

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
# them is printed. This token is the build plane's for the catalog calls around the runs; the
# task pods mint their own through the same principal.
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

# shellcheck disable=SC2329  # called from the trap and from step 6
cleanup() {
  set +e
  local code
  if kubectl -n "$NS" delete pipeline "$PIPELINE" --ignore-not-found --timeout=60s >/dev/null 2>&1; then
    pipeline_deleted=yes
  fi
  if [ -n "$PREFIX" ]; then
    if up "$LK/management/v1/info" || forward_lk; then
      auth_ready >/dev/null 2>&1 || true
      # 404 is fine: a second run's cleanup finds the table already gone.
      code="$(catalog DELETE "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables/$TABLE_NAME?purgeRequested=true")"
      case "$code" in
        204|200|404) table_deleted=yes ;;
      esac
    fi
  fi
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  rm -f "$tmp/token" "$tmp/token.json" "$tmp/auth" "$tmp/client-id" "$tmp/client-secret"
  rm -rf "$tmp"
}
trap '{ [ "$finished" = yes ] || cleanup; }' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for tool in kubectl curl jq base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "verify-wap: UNVERIFIED ($tool not found)"; exit 3; }
done

# --- the scheduler, the principal, the loader ---------------------------------------------
if ! kubectl -n "$NS" get deploy airflow-scheduler >/dev/null 2>&1; then
  echo "verify-wap: UNVERIFIED (the Airflow scheduler is not installed)"
  exit 3
fi
if ! kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" >/dev/null 2>&1; then
  echo "verify-wap: UNVERIFIED (the demo principal is not installed — make demo-access)"
  exit 3
fi
token_url="$(kubectl -n "$NS" get cm stardelt-identity -o jsonpath='{.data.tokenEndpoint}' 2>/dev/null || true)"
if [ -z "$token_url" ]; then
  echo "verify-wap: UNVERIFIED (ConfigMap stardelt-identity has no tokenEndpoint — make install-identity)"
  exit 3
fi
# #1007: the loader resolves a warehouse's prefix before it builds a table route. Without it the
# audit branch cannot be created on `playground` and nothing about WAP can be measured.
if ! kubectl -n "$NS" exec deploy/airflow-scheduler -c scheduler -- \
    grep -q 'def warehouse_prefix' /opt/airflow/dags/stardelt_pipelines/wap.py >/dev/null 2>&1; then
  echo "verify-wap: UNVERIFIED (the running loader predates the warehouse-prefix fix — roll the airflow pin out)"
  exit 3
fi

# The principal's keys, for the build plane's own catalogue calls; the pods read them themselves
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
  echo "verify-wap: UNVERIFIED (lakekeeper did not answer within 30s)"
  exit 3
fi
auth_ready
PREFIX="$(curl -sS -H @"$tmp/auth" "$LK/catalog/v1/config?warehouse=$CATALOG" \
  | jq -r '.defaults.prefix // empty' 2>/dev/null || true)"
[ -n "$PREFIX" ] || { echo "verify-wap: UNVERIFIED (the catalog config named no prefix for $CATALOG)"; exit 3; }

# --- the Airflow CLI, on the scheduler ----------------------------------------------------
af() { kubectl -n "$NS" exec deploy/airflow-scheduler -c scheduler -- airflow "$@" 2>/dev/null; }

# `dags list-runs -o plain` is space-aligned (dag_id run_id state …); `tasks states-for-dag-run`
# puts a `|` between its columns. Both are parsed from the plain output, never from logs.
newest_run_id() { # <dag> -> the newest run's id (list-runs is newest first)
  af dags list-runs "$1" -o plain | awk -v dag="$1" '$1==dag {print $2; exit}'
}

run_state() { # <dag> <run> -> the run's state, or empty while it is not there yet
  af dags list-runs "$1" -o plain | awk -v dag="$1" -v run="$2" '$1==dag && $2==run {print $3; exit}'
}

task_state() { # <dag> <run> <task> -> the task's state, or "-"
  af tasks states-for-dag-run "$1" "$2" \
    | awk -F'[|]' -v dag="$1" -v task="$3" '{a=$1; gsub(/[[:space:]]/,"",a); if (a!=dag) next;
        t=$3; gsub(/^[[:space:]]+|[[:space:]]+$/,"",t); if (t!=task) next;
        s=$4; gsub(/^[[:space:]]+|[[:space:]]+$/,"",s); print s; exit}'
}

# `pipeline_loader.py` reads the Pipeline resources at parse time, and Airflow re-parses a file
# on its own cadence (a `dag_dir_list_interval` of five minutes by default). A release check
# cannot wait that long, so it asks for the parse: `dags reserialize` rebuilds the DagBag in this
# process and writes the serialized DAGs the trigger reads. The signal that it happened is the
# DAG's own `last_parsed_time`.
parse_time() { # <dag> -> "YYYY-MM-DD HH:MM:SS…" of the last parse, or empty
  af dags details "$1" -o plain \
    | awk '$1=="last_parsed_time" {print $2" "$3; exit}'
}

reparse() { # <dag> -> 0 when the DAG was parsed after this call started
  local before after _ deadline
  before="$(parse_time "$1")"
  af dags reserialize >/dev/null 2>&1 || true
  deadline=$(( SECONDS + 60 ))
  while [ "$SECONDS" -lt "$deadline" ]; do
    after="$(parse_time "$1")"
    if [ -n "$after" ] && [ "$after" != "$before" ]; then return 0; fi
    sleep 2
  done
  # One natural re-parse may still be on its way.
  deadline=$(( SECONDS + 360 ))
  while [ "$SECONDS" -lt "$deadline" ]; do
    after="$(parse_time "$1")"
    if [ -n "$after" ] && [ "$after" != "$before" ]; then return 0; fi
    sleep 10
  done
  return 1
}

wait_dag() { # <dag> -> 0 when the loader has loaded it
  local _
  for _ in $(seq 1 12); do
    if af dags list -o plain | grep -qE "^${DAG}[[:space:]]"; then return 0; fi
    af dags reserialize >/dev/null 2>&1 || true
    sleep 5
  done
  af dags list -o plain | grep -qE "^${DAG}[[:space:]]"
}

wait_run() { # <dag> <run> -> RUN_STATE; 1 when it did not settle within TIMEOUT
  local deadline=$(( SECONDS + TIMEOUT )) state=""
  while [ "$SECONDS" -lt "$deadline" ]; do
    state="$(run_state "$1" "$2")"
    case "$state" in
      success|failed) RUN_STATE="$state"; return 0 ;;
    esac
    sleep 10
  done
  RUN_STATE="${state:-unknown}"
  return 1
}

# A pod that never scheduled is the lab's capacity, not a WAP defect (#1185). Kubernetes keeps
# these events for about an hour; the `.eventTime` field is the one this cluster fills in.
unscheduled_pod() { # <task-name> -> the FailedScheduling message for a pod of that task, or empty
  local since
  since="$(date -u -d '-20 min' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || true)"
  [ -n "$since" ] || return 0
  kubectl -n "$NS" get events --field-selector reason=FailedScheduling -o json 2>/dev/null \
    | jq -r --arg prefix "^$1-" --arg since "$since" \
        '.items[]
         | select(.involvedObject.kind == "Pod")
         | select(.involvedObject.name | test($prefix))
         | select(((.eventTime // .lastTimestamp // .firstTimestamp) // "") >= $since)
         | .message' 2>/dev/null | head -n 1
}

# Exit 3 (nothing measured) when a task failed and its pod never got scheduled: a retry on a
# quieter node is what decides WAP, not this run.
no_room() { # <dag> <run> <task>
  [ "$(task_state "$1" "$2" "$3")" = "failed" ] || return 0
  local why
  why="$(unscheduled_pod "$3")"
  [ -n "$why" ] || return 0
  echo "verify-wap: UNVERIFIED (the $3 pod never scheduled on the lab — $why; nothing about WAP was measured, retry when the node has CPU free)" >&2
  exit 3
}

trigger_run() { # <dag> -> sets RUN; 1 when the newest run did not appear
  local before after _ deadline=$(( SECONDS + 120 ))
  before="$(newest_run_id "$1")"
  af dags trigger "$1" >/dev/null || { echo "the scheduler refused to trigger $1" >&2; return 1; }
  while [ "$SECONDS" -lt "$deadline" ]; do
    after="$(newest_run_id "$1")"
    if [ -n "$after" ] && [ "$after" != "$before" ]; then RUN="$after"; return 0; fi
    sleep 5
  done
  return 1
}

# --- the pipeline document: runs differ in `publish:` and the check's sql only ------------
pipeline_document() { # <publish> <check-sql>
  cat <<EOF
apiVersion: stardelt.io/v1alpha1
kind: Pipeline
metadata:
  name: $PIPELINE
  namespace: $NS
spec:
  ownerGroup: $GROUP
  description: WAP release check (temporary; platform/scripts/verify-wap.sh)
  runAs: pipeline-demo
  publish: $1
  tasks:
    - name: build
      kind: spark-pipeline
      project: graph
      files:
        spark-pipeline.yml: |
          name: $PIPELINE
          configuration:
            spark.sql.catalog.$CATALOG: org.apache.iceberg.spark.SparkCatalog
            spark.sql.catalog.$CATALOG.type: rest
            spark.sql.catalog.$CATALOG.uri: http://lakekeeper.$NS.svc.cluster.local:8181/catalog
            spark.sql.catalog.$CATALOG.warehouse: $CATALOG
            spark.sql.catalog.$CATALOG.io-impl: org.apache.iceberg.aws.s3.S3FileIO
            spark.sql.catalog.$CATALOG.s3.endpoint: http://seaweedfs-s3.$NS.svc.cluster.local:8333
            spark.sql.catalog.$CATALOG.s3.region: us-east-1
            spark.sql.catalog.$CATALOG.s3.path-style-access: "true"
          libraries:
            - glob:
                include: "sql/**"
        sql/wap_gate.sql: |
          CREATE MATERIALIZED VIEW $TABLE_NAME AS SELECT explode(sequence(1, 10)) AS id;
      writes: [$TABLE]
    - name: check
      kind: quality
      dependsOn: [build]
      checks:
        - name: ids_present
          sql: "$2"
EOF
}

# The `quality` task refuses `${table}` outside `publish: wap` (a direct run has no audit branch
# to read), so the direct run names the table itself.
DIRECT_SQL="SELECT id FROM $TABLE WHERE id IS NULL"
# shellcheck disable=SC2016  # `${table}` is the loader's placeholder, resolved by the check task
PASS_SQL='SELECT id FROM ${table} WHERE id IS NULL'
# shellcheck disable=SC2016  # as above, and the 10 rows make the check fail
FAIL_SQL='SELECT id FROM ${table} WHERE id IS NOT NULL'

# `main`'s snapshot id, or empty; also writes the refs to $tmp/refs.json.
main_snapshot() {
  local code
  rm -f "$tmp/refs.json"
  code="$(catalog GET "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables/$TABLE_NAME?snapshots=all")"
  [ "$code" = "200" ] || return 1
  cp "$tmp/out.json" "$tmp/refs.json"
  jq -r '.metadata.refs.main["snapshot-id"] // empty' "$tmp/refs.json" 2>/dev/null || true
}

# --- start clean: a leftover pipeline or table would collide with this run ----------------
kubectl -n "$NS" delete pipeline "$PIPELINE" --ignore-not-found >/dev/null 2>&1 || true
catalog DELETE "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables/$TABLE_NAME?purgeRequested=true" >/dev/null 2>&1 || true

# --- run 1: publish: direct ---------------------------------------------------------------
echo "run 1: publish: direct" >&2
pipeline_document direct "$DIRECT_SQL" > "$tmp/pipeline.yaml"
applied=no
if kubectl -n "$NS" apply -f "$tmp/pipeline.yaml" >/dev/null 2>&1; then applied=yes; fi
[ "$applied" = "yes" ] || { echo "verify-wap: FAIL (the API server refused the Pipeline $PIPELINE)"; exit 1; }

if ! wait_dag "$DAG"; then
  echo "verify-wap: UNVERIFIED (the DAG $DAG never appeared; the loader refused the Pipeline)"
  af dags list-import-errors -o plain 2>/dev/null | grep -iE "pipeline|$DAG" | text >&2 || true
  kubectl -n "$NS" logs deploy/airflow-dag-processor --tail=100 2>&1 \
    | grep -i "pipeline" | tail -n 3 | text >&2 || true
  exit 3
fi

# A parse that predates this apply would trigger the DAG a previous session left loaded.
reparse "$DAG" || { echo "verify-wap: UNVERIFIED (the loader did not re-parse $DAG after the update)"; exit 3; }
if ! trigger_run "$DAG"; then
  echo "verify-wap: UNVERIFIED (the DAG $DAG was not triggered)"
  exit 3
fi
if ! wait_run "$DAG" "$RUN"; then
  echo "verify-wap: UNVERIFIED (run 1 ($RUN) did not finish within ${TIMEOUT}s, state=${RUN_STATE})"
  exit 3
fi
S1="$(main_snapshot || true)"
if [ "$RUN_STATE" = "success" ] && [ -n "$S1" ]; then
  echo "PASS direct"
  echo "  run $RUN success, main snapshot-id $S1" >&2
else
  echo "FAIL direct"
  echo "  run $RUN state=$RUN_STATE main-snapshot=${S1:-none} tasks: $(af tasks states-for-dag-run "$DAG" "$RUN" | grep -E "^$DAG" | awk -F'[|]' '{t=$3;gsub(/ /,"",t);s=$4;gsub(/ /,"",s);printf "%s=%s ",t,s}')" >&2
  bad="$bad direct"
fi

# --- run 2: publish: wap, the check passes ------------------------------------------------
echo "run 2: publish: wap (check passes)" >&2
pipeline_document wap "$PASS_SQL" > "$tmp/pipeline.yaml"
kubectl -n "$NS" apply -f "$tmp/pipeline.yaml" >/dev/null \
  || { echo "verify-wap: FAIL (the API server refused the Pipeline update for run 2)"; exit 1; }
reparse "$DAG" || { echo "verify-wap: UNVERIFIED (the loader did not re-parse $DAG after the update)"; exit 3; }
if ! trigger_run "$DAG"; then
  echo "verify-wap: UNVERIFIED (the DAG $DAG was not triggered for run 2)"
  exit 3
fi
if ! wait_run "$DAG" "$RUN"; then
  echo "verify-wap: UNVERIFIED (run 2 ($RUN) did not finish within ${TIMEOUT}s, state=${RUN_STATE})"
  exit 3
fi
no_room "$DAG" "$RUN" build
tasks=""
for task in wap-prepare build check wap-publish; do
  tasks="$tasks $task=$(task_state "$DAG" "$RUN" "$task")"
done
echo "  run $RUN state=$RUN_STATE$tasks" >&2
S2="$(main_snapshot || true)"
audit2="$(jq -r '(.metadata.refs // {}) | keys[] | select(startswith("audit_"))' "$tmp/refs.json" 2>/dev/null || true)"
if [ "$RUN_STATE" = "success" ] \
   && [ "$(task_state "$DAG" "$RUN" wap-prepare)" = "success" ] \
   && [ "$(task_state "$DAG" "$RUN" build)" = "success" ] \
   && [ "$(task_state "$DAG" "$RUN" check)" = "success" ] \
   && [ "$(task_state "$DAG" "$RUN" wap-publish)" = "success" ] \
   && [ -n "$S2" ] && [ "$S2" != "${S1:-}" ] && [ -z "$audit2" ]; then
  echo "PASS publish"
  echo "  main moved ${S1:-none} -> $S2, no audit branch left" >&2
else
  echo "FAIL publish"
  echo "  run $RUN state=$RUN_STATE main${S2:+=}${S2:-none} audit=${audit2:-none}" >&2
  bad="$bad publish"
fi

# --- run 3: publish: wap, the check finds rows — nothing may be published --------------------
echo "run 3: publish: wap (check finds rows)" >&2
pipeline_document wap "$FAIL_SQL" > "$tmp/pipeline.yaml"
kubectl -n "$NS" apply -f "$tmp/pipeline.yaml" >/dev/null \
  || { echo "verify-wap: FAIL (the API server refused the Pipeline update for run 3)"; exit 1; }
reparse "$DAG" || { echo "verify-wap: UNVERIFIED (the loader did not re-parse $DAG after the update)"; exit 3; }
if ! trigger_run "$DAG"; then
  echo "verify-wap: UNVERIFIED (the DAG $DAG was not triggered for run 3)"
  exit 3
fi
if ! wait_run "$DAG" "$RUN"; then
  echo "verify-wap: UNVERIFIED (run 3 ($RUN) did not finish within ${TIMEOUT}s, state=${RUN_STATE})"
  exit 3
fi
no_room "$DAG" "$RUN" build
check_state="$(task_state "$DAG" "$RUN" check)"
publish_state="$(task_state "$DAG" "$RUN" wap-publish)"
S3="$(main_snapshot || true)"
audit3="$(jq -r '(.metadata.refs // {}) | keys[] | select(startswith("audit_"))' "$tmp/refs.json" 2>/dev/null || true)"
audit_count="$(printf '%s' "$audit3" | grep -c . || true)"
audit_ttl=""
if [ "$audit_count" = "1" ]; then
  audit_ttl="$(jq -r --arg b "$audit3" '.metadata.refs[$b]["max-ref-age-ms"] // empty' "$tmp/refs.json" 2>/dev/null || true)"
fi
echo "  run $RUN state=$RUN_STATE check=$check_state wap-publish=${publish_state:--} main=${S3:-none} audit=${audit3:-none} ttl=${audit_ttl:-none}" >&2
if [ "$RUN_STATE" = "failed" ] && [ "$check_state" = "failed" ] \
   && [ "$publish_state" != "success" ] \
   && [ -n "$S3" ] && [ "$S3" = "${S2:-}" ] \
   && [ "$audit_count" = "1" ] \
   && printf '%s' "$audit3" | grep -qE '^audit_[0-9a-f]{16}$' \
   && [ "$audit_ttl" = "604800000" ]; then
  echo "PASS refusal"
  echo "  main stayed $S3, $audit3 kept for 7 days" >&2
else
  echo "FAIL refusal"
  bad="$bad refusal"
fi

# --- step 6: cleanup ----------------------------------------------------------------------
cleanup
cm_left=""
for _ in $(seq 1 12); do
  cm_left="$(kubectl -n "$NS" get configmap -o name 2>/dev/null | grep -c "spark-project-$PIPELINE" || true)"
  [ "${cm_left:-0}" = "0" ] && break
  sleep 5
done
if [ "$pipeline_deleted" = "yes" ] && [ "$table_deleted" = "yes" ] && [ "${cm_left:-0}" = "0" ]; then
  echo "PASS cleanup"
  echo "  the Pipeline, $TABLE and its graph configmap are gone; the scratch schema stays" >&2
else
  echo "FAIL cleanup"
  echo "  pipeline-deleted=$pipeline_deleted table-deleted=$table_deleted cm-left=${cm_left:-?}" >&2
  bad="$bad cleanup"
fi
finished=yes

if [ -n "${bad:-}" ]; then
  echo "verify-wap: FAIL (did not hold:$bad)"
  exit 1
fi
echo "verify-wap: PASS (4 of 4)"
exit 0
