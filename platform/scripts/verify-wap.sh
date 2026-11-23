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
#   - `cleanup` runs on EXIT, INT and TERM: the table and both port-forwards are removed whatever
#     happened. A run still in flight keeps its Pipeline (`stardelt.io/check: verify-wap`), so
#     a task parked in `up_for_reschedule` can still be re-queued; `make demo-inventory-check`
#     reports that leftover with its delete command (#1279). The `scratch` schema is the demo's
#     and stays; no other table is touched;
#   - one table per run (`wap_gate_<UTC timestamp>`, #1319): a killed run's table and refs are
#     never measured again; a `wap_gate*` table whose Pipeline is gone is dropped at the start,
#     and a drop that fails is UNVERIFIED instead of a silent `|| true`;
#   - two copies at once are safe (#1329): the Pipeline, DAG, table and graph ConfigMap carry a
#     per-run id with random digits, the port-forward listens on a port of its own, and the
#     start sweep never drops a table whose Pipeline still exists. Only the label is shared.
#   - `airflow tasks logs` does not exist in Airflow 3.2 (#1025), so every assertion reads the
#     run state, the per-task states and the catalog's `metadata.refs` — never a task log. A
#     failing row asks the API v2 for the task's log afterwards, only to say why (#1335).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-${NAMESPACE:-stardelt}}"
# One id per run (#1329): UTC seconds plus 4 random hex digits. The clock alone collides when
# two copies start in the same second (measured: identical names, both runs failed). Lower-case
# hex and digits are legal in a Kubernetes name and in an unquoted SQL identifier.
RUN_SUFFIX="${RUN_SUFFIX:-$(date -u +%Y%m%d%H%M%S)$(od -An -N2 -tx1 /dev/urandom | tr -d ' \n')}"
PIPELINE="${PIPELINE:-verify-wap-$RUN_SUFFIX}"
DAG="${DAG:-stardelt__$PIPELINE}"
CATALOG="${CATALOG:-playground}"
SCHEMA="${SCHEMA:-scratch}"
# One table per run (#1319): a run that was killed half-way leaves a table, and its refs,
# that the next run must never measure. Leftovers of that shape are swept at the start.
TABLE_NAME="${TABLE_NAME:-wap_gate_$RUN_SUFFIX}"
TABLE="$CATALOG.$SCHEMA.$TABLE_NAME"
GROUP="${GROUP:-demo}"
PRINCIPAL_SECRET="${PRINCIPAL_SECRET:-sp-pipeline-$GROUP}"
LK_PORT="${LK_PORT:-}"
# The Airflow API server forward, opened only when a row fails (#1335).
AF_PORT="${AF_PORT:-18082}"
TIMEOUT="${TIMEOUT:-900}"
LK=""
AF="http://127.0.0.1:$AF_PORT"

scratch="${TMPDIR:-$HOME/.cache}"
mkdir -p "$scratch"
tmp="$(mktemp -d "$scratch/verify-wap.XXXXXX")"
LK_PID=""
AF_PID=""
AF_TOKEN=""
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

forward_lk() { # sets LK to this run's own port-forward; 1 when none answered within 30 s
  local _ port
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  kubectl -n "$NS" port-forward svc/lakekeeper "$LK_PORT:8181" > "$tmp/port-forward.out" 2>&1 &
  LK_PID=$!
  for _ in $(seq 1 30); do
    port="$(grep -oE '127\.0\.0\.1:[0-9]+' "$tmp/port-forward.out" | head -n 1 | cut -d: -f2)"
    if [ -n "$port" ]; then
      LK="http://127.0.0.1:$port"
      if up "$LK/management/v1/info"; then return 0; fi
    fi
    kill -0 "$LK_PID" 2>/dev/null || return 1   # the forward died: never borrow another listener
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
  local code state=""
  # #1279: deleting the Pipeline drops its DAG, and a task instance parked in
  # `up_for_reschedule` is only re-queued once the DAG is back. A run that has not settled keeps
  # its Pipeline; it carries `stardelt.io/check: verify-wap`, so `make demo-inventory-check`
  # reports it as a leftover together with the delete command (#1274).
  if [ -n "${RUN:-}" ]; then state="$(run_state "$DAG" "$RUN" 2>/dev/null || true)"; fi
  case "$state" in
    running|queued)
      echo "verify-wap: left the Pipeline $PIPELINE in place — run $RUN is still $state (delete it later with: kubectl -n $NS delete pipeline $PIPELINE)" >&2
      ;;
    *)
      if kubectl -n "$NS" delete pipeline "$PIPELINE" --ignore-not-found --timeout=60s >/dev/null 2>&1; then
        pipeline_deleted=yes
      fi
      ;;
  esac
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
  if [ -n "$AF_PID" ]; then kill "$AF_PID" >/dev/null 2>&1 || true; fi
  AF_TOKEN=""
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

# --- the failing task's log, through the Airflow API v2 (#1335) ---------------------------
# A red row printed `build=upstream_failed` and the refs, but not the sentence the failing task
# raised — the one fact a reader needs (#1332), and `airflow tasks logs` does not exist in
# Airflow 3.2. The API has `…/taskInstances/{task_id}/logs/{try_number}`; the service user and
# the bearer-config curl are `verify-airflow-runs.sh`'s. Forward and sign-in start lazily, so a
# passing run opens neither.
af_forward() { # 0 when svc/airflow-api-server answers on $AF_PORT
  local _ code
  [ -n "$AF_PID" ] && return 0
  kubectl -n "$NS" port-forward svc/airflow-api-server "$AF_PORT:8080" >/dev/null 2>&1 &
  AF_PID=$!
  for _ in $(seq 1 20); do
    code="$(curl -s -o /dev/null -m 2 -w '%{http_code}' "$AF/api/v2/version" 2>/dev/null)" || code="000"
    [ "${code:-000}" = "200" ] && return 0
    kill -0 "$AF_PID" 2>/dev/null || return 1   # the forward died: never borrow another listener
    sleep 1
  done
  return 1
}

af_sign_in() { # sets AF_TOKEN from Secret airflow-nova-api; the values are never printed
  local secret user pass body raw code
  secret="$(kubectl -n "$NS" get secret airflow-nova-api -o json 2>/dev/null)" || return 1
  user="$(printf '%s' "$secret" | jq -r '.data.username // empty' 2>/dev/null | base64 -d 2>/dev/null)" || return 1
  pass="$(printf '%s' "$secret" | jq -r '.data.password // empty' 2>/dev/null | base64 -d 2>/dev/null)" || return 1
  [ -n "$user" ] && [ -n "$pass" ] || return 1
  body="$(AF_USER="$user" AF_PASS="$pass" jq -n '{username: env.AF_USER, password: env.AF_PASS}' 2>/dev/null)" || return 1
  unset user pass
  raw="$(printf '%s' "$body" | curl -s -m 5 -H 'Content-Type: application/json' \
    --data-binary @- -w '\n%{http_code}' "$AF/auth/token" 2>/dev/null)" || return 1
  unset body
  code="${raw##*$'\n'}"
  [ "$code" = "201" ] || return 1
  AF_TOKEN="$(printf '%s' "${raw%$'\n'*}" | jq -r '.access_token // empty' 2>/dev/null)" || return 1
  [ -n "$AF_TOKEN" ]
}

task_log() { # <dag> <run> <task> -> last error lines of attempt 1 on stderr, or nothing
  local raw lines
  af_forward || return 0
  [ -n "$AF_TOKEN" ] || af_sign_in || return 0
  # `-f`: a 404 (no log for this attempt) or any other refusal prints nothing; the bearer
  # travels in a curl config on a pipe, never on the command line. A log that cannot be read is
  # never a failure of the check, so this returns 0 whatever happened.
  raw="$(curl -sf -m 10 -K <(printf 'header = "Authorization: Bearer %s"\n' "$AF_TOKEN") \
    "$AF/api/v2/dags/$1/dagRuns/$2/taskInstances/$3/logs/1" 2>/dev/null || true)"
  [ -n "$raw" ] || return 0
  # The endpoint answers `{"content":[{"event":"…"}]}`, the whole log on one line (measured
  # 2026-11-23 on 3.2), so the log is unfolded to one line per entry and per raised exception
  # before it is filtered; anything else is filtered as it came.
  lines="$(printf '%s' "$raw" | jq -r 'if type == "object" and (.content | type) == "array" then
      (.content[] | (.event // empty | tostring)),
      (.content[] | .error_detail[]? | "\(.exc_type // "error"): \(.exc_value // "")")
    else . end' 2>/dev/null)" || lines="$raw"
  lines="$(printf '%s\n' "$lines" | grep -E 'Error|Exception|Traceback|refused|failed|WapError' \
    | tail -n 8 | sed -E 's/(Bearer|token=)[A-Za-z0-9._~+\/-]+/\1<redacted>/g' || true)"
  # Printed rather than piped by the caller: a pipeline would run this in a subshell, and the
  # port-forward it started (and the token) would outlive it — `cleanup` must be able to kill it.
  if [ -n "$lines" ]; then printf '%s\n' "$lines" | sed 's/^/    /' >&2; fi
  return 0
}

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
  local deadline=$(( SECONDS + TIMEOUT )) state="" polls=0
  while [ "$SECONDS" -lt "$deadline" ]; do
    state="$(run_state "$1" "$2")"
    case "$state" in
      success|failed) RUN_STATE="$state"; return 0 ;;
    esac
    polls=$((polls + 1))
    # #1279: a task instance parked in `up_for_reschedule` sat 17 minutes until a second
    # `make verify-wap` re-registered the Pipeline — the re-parse is what re-queued it. Ask for
    # one every minute instead of waiting on `dag_dir_list_interval` (five minutes by default).
    if [ $((polls % 6)) -eq 0 ]; then af dags reserialize >/dev/null 2>&1 || true; fi
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
  labels:
    stardelt.io/check: verify-wap
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
code="$(catalog GET "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables")"
if [ "$code" != "200" ]; then
  echo "verify-wap: UNVERIFIED (could not list $CATALOG.$SCHEMA: HTTP $code)"
  exit 3
fi
leftovers="$(jq -r '.identifiers[]?.name | select(startswith("wap_gate"))' "$tmp/out.json")"
for name in $leftovers; do
  suffix="${name#wap_gate}"; suffix="${suffix#_}"
  owner="verify-wap${suffix:+-$suffix}"
  if kubectl -n "$NS" get pipeline "$owner" >/dev/null 2>&1; then
    echo "  kept $CATALOG.$SCHEMA.$name: pipeline $owner is still there (another copy, or a leftover demo-inventory-check reports)" >&2
    continue
  fi
  code="$(catalog DELETE "/catalog/v1/$PREFIX/namespaces/$SCHEMA/tables/$name?purgeRequested=true")"
  case "$code" in
    200|204|404) echo "  dropped the leftover table $CATALOG.$SCHEMA.$name" >&2 ;;
    *) echo "verify-wap: UNVERIFIED (the leftover table $CATALOG.$SCHEMA.$name could not be dropped: HTTP $code)"; exit 3 ;;
  esac
done

own_branch() { # <run-id> -> audit_<16 hex>, the same name as wap.branch_name (wap.py:18-20)
  printf 'audit_%s' "$(printf '%s' "$1" | sha256sum | cut -c1-16)"
}
refs_line() { # every ref of the last main_snapshot read: name, type, snapshot, TTL
  jq -r '(.metadata.refs // {}) | to_entries[] | "    \(.key) type=\(.value.type) snapshot=\(.value["snapshot-id"]) ttl=\(.value["max-ref-age-ms"] // "none")"' \
    "$tmp/refs.json" 2>/dev/null || true
}

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
  for t in wap-prepare build check wap-publish; do
    if [ "$(task_state "$DAG" "$RUN" "$t")" = "failed" ]; then
      echo "  $t said:" >&2
      task_log "$DAG" "$RUN" "$t"
    fi
  done
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
  echo "  the run's own branch is $(own_branch "$RUN"); the table's refs:" >&2
  refs_line >&2
  for t in wap-prepare build check wap-publish; do
    if [ "$(task_state "$DAG" "$RUN" "$t")" = "failed" ]; then
      echo "  $t said:" >&2
      task_log "$DAG" "$RUN" "$t"
    fi
  done
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
   && [ "$audit3" = "$(own_branch "$RUN")" ] \
   && [ "$audit_ttl" = "604800000" ]; then
  echo "PASS refusal"
  echo "  main stayed $S3, $audit3 kept for 7 days" >&2
else
  echo "FAIL refusal"
  echo "  the run's own branch is $(own_branch "$RUN"); the table's refs:" >&2
  refs_line >&2
  for t in wap-prepare build check wap-publish; do
    if [ "$(task_state "$DAG" "$RUN" "$t")" = "failed" ]; then
      echo "  $t said:" >&2
      task_log "$DAG" "$RUN" "$t"
    fi
  done
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
