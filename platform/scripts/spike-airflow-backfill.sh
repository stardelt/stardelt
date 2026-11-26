#!/usr/bin/env bash
# Spike for stardelt/stardelt#1876 (points 3 and 4 of #117): Airflow 3's backfill API and the
# "missed schedule" of a `catchup=False` + `CronTriggerTimetable` DAG.
#
#   make spike-airflow-backfill      (NS=… PORT=… DAG=… TIMEOUT=… DOWN_SECONDS=…)
#
# Why: #117 wants "run for a range" (a backfill of at most 31 dates) and an answer for the 03:00
# run of a night the scheduler was down. Nova cannot take either from the design: Airflow 3.2
# ships its own backfill API (if it works with Nova's credential and a `CronTriggerTimetable`,
# Nova starts it and reports on it; otherwise Nova triggers the dates one by one and owns the
# state), and nothing documents whether `catchup=False` creates nothing or one late run.
#
# Rows, `key=value`, one or more `B<n>` lines each:
#   B1  the role of the credential Nova signs in with: `GET /api/v2/users` on the public API and,
#       because 3.2 has no such route, the `ab_user`/`ab_user_role` tables through the api-server.
#   B2  `POST /api/v2/backfills/dry_run`: which `logical_date`s a past range yields (with conf and
#       without), and what a range that already has runs answers (`reprocess_behavior` none,
#       failed, completed).
#   B3  `POST /api/v2/backfills` on that range: the id, what `GET /api/v2/backfills/{id}` reports
#       (`created_at`, `completed_at`, and whether it names the runs), and the DAG runs it made
#       (`run_type`, `run_id`, `logical_date`, `max_active_runs` against the DAG's own bound).
#   B4  the refusals: the same call on a **paused** DAG (accepted, but its runs stay queued), a
#       second backfill while one is open (409), `max_active_runs=1` while a run is queued, and
#       what `PUT …/cancel` does to the queued and running runs.
#   B5  the missed schedule: a scratch `Pipeline` on `*/2 * * * *` Europe/Berlin, one `sql` task,
#       the scheduler scaled to 0 for `DOWN_SECONDS` (default 300 = two slots and a half), then
#       back: which runs appear, with which `logical_date`/`run_type`, and how long after the
#       scheduler returned.
#   B6  `dag_run_conf` through a backfill: is it carried verbatim in the created run's `conf`.
#   C1  the conclusion for each of the two questions, with the recommendation.
#
# Every range this script backfills is computed *before* the earliest `logical_date` the DAG
# already has (B2 asks exactly two days, B4 three days), so a re-run measures the same thing and
# never replays a date twice. The scratch `Pipeline`'s `sql: select 1` writes nothing.
#
# The `logical_date`s are the DAG's timetable's, not a local midnight: the same `0 4 * * *`
# Berlin DAG answers 02:00Z for an October range and 03:00Z for a November one (DST). Take the
# dates from the dry run, never compute them from the cron.
#
# Exit 0 = PASS (all six rows printed at least one line), 1 = FAIL (a row is missing), 3 =
# UNVERIFIED (no kubectl, the port-forward, the sign-in or the backfill API is absent — nothing
# was measured).
#
# It leaves the cluster as it found it: the trap deletes the scratch `Pipeline`, scales
# `airflow-scheduler` back to the replica count it started with, restores the pause state of
# `DAG`, kills the port-forward and removes the scratch directory — including after a `set -e`
# failure. What stays behind is metadata the API cannot delete: the runs a backfill created (no
# delete route, only cancel — B4 records what cancel does) and the scratch DAG's row and runs,
# which is why the scratch name carries the time.
#
# No secret is printed: the password goes from Secret `airflow-nova-api` into a 0600 file and
# into the login body on stdin; the token lives in a variable only, never in a line, a file name
# or a `-v` output.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-${NAMESPACE:-stardelt}}"
PORT="${PORT:-18086}"
BASE="http://127.0.0.1:$PORT"
DAG="${DAG:-stardelt__shop-dbt}"
# A DAG's metadata outlives its Pipeline: the name carries the time, so every run measures a DAG
# that has no history of its own.
PIPELINE="${PIPELINE:-spike-airflow-backfill}-$(date -u +%H%M%S)"
SCRATCH_DAG="stardelt__$PIPELINE"
SECRET="${SECRET:-airflow-nova-api}"
USER_NAME="${USER_NAME:-nova-api}"
APISERVER="${APISERVER:-airflow-api-server}"
SCHEDULER="${SCHEDULER:-airflow-scheduler}"
TIMEOUT="${TIMEOUT:-300}"          # a row's wait: the DAG, a run, a backfill
BACKFILL_TIMEOUT="${BACKFILL_TIMEOUT:-900}"   # B3: a dbt backfill with retries
DOWN_SECONDS="${DOWN_SECONDS:-300}"
CONTEXT="${CONTEXT:-}"

scratch="${TMPDIR:-$HOME/.cache}"
mkdir -p "$scratch"
tmp="$(mktemp -d "$scratch/spike-airflow-backfill.XXXXXX")"
PF_PID=""
TOKEN=""
FROM=""
TO=""
pause_changed=no
dag_was_paused=""
B5_LATE=""
B5_WINDOW=""
B5_MISSING=""
B5_SLOTS=""
B5_CREATED=""
pipeline_applied=no
sched_replicas=""
declare -A rows=()
missing=""
unverified=""

kc() { # kubectl, with the pinned context when one is given
  if [ -n "$CONTEXT" ]; then kubectl --context "$CONTEXT" "$@"; else kubectl "$@"; fi
}

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  # The pause state lives in Airflow, not in Kubernetes: restore it over the port-forward, before
  # the forward is killed.
  if [ "$pause_changed" = yes ] && [ -n "$dag_was_paused" ] && [ -n "$TOKEN" ]; then
    printf '{"is_paused":%s}' "$dag_was_paused" > "$tmp/unpause.json"
    api PATCH "/api/v2/dags/$DAG" "$tmp/unpause-out.json" "$tmp/unpause.json" >/dev/null 2>&1 || true
  fi
  if [ -n "$PF_PID" ]; then kill "$PF_PID" >/dev/null 2>&1 || true; fi
  if [ "$pipeline_applied" = yes ]; then
    kc -n "$NS" delete pipeline "$PIPELINE" --ignore-not-found >/dev/null 2>&1 || true
  fi
  if [ -n "$sched_replicas" ]; then
    kc -n "$NS" scale deploy "$SCHEDULER" --replicas="$sched_replicas" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp"
  exit "$rc"
}
trap cleanup EXIT INT TERM

row() { # B<n> key=value … — remembers which rows were printed and prints the body compactly
  local n="$1"; shift
  rows["$n"]=1
  printf '%s %s\n' "$n" "$*"
}

body() { # file -> the one-line, length-capped body for a row
  jq -c . "$1" 2>/dev/null | cut -c1-400 || printf '(unreadable)'
}

api() { # method path out [in-file] -> prints the HTTP status, writes the body to `out`
  local m="$1" p="$2" out="$3" in="${4:-}"
  if [ -n "$in" ]; then
    curl -s -m 60 -o "$out" -w '%{http_code}' -X "$m" "$BASE$p" \
      -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' --data-binary "@$in" \
      2>/dev/null || printf '000'
  else
    curl -s -m 60 -o "$out" -w '%{http_code}' -X "$m" "$BASE$p" \
      -H "Authorization: Bearer $TOKEN" 2>/dev/null || printf '000'
  fi
}

now_s() { date -u +%s; }
stamp() { date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ; }

# --- preflight -------------------------------------------------------------------------------
for tool in kubectl curl jq base64 date; do
  command -v "$tool" >/dev/null 2>&1 || unverified="${unverified:+$unverified, }no $tool"
done
if [ -n "$unverified" ]; then
  echo "spike-airflow-backfill: UNVERIFIED ($unverified)"
  exit 3
fi
sched_replicas="$(kc -n "$NS" get deploy "$SCHEDULER" -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
if [ -z "$sched_replicas" ]; then
  echo "spike-airflow-backfill: UNVERIFIED (deploy/$SCHEDULER not found in $NS)"
  unverified="deploy/$SCHEDULER not found in $NS"
  exit 3
fi

# One port-forward for every call; killed by the trap. The port must be free and must answer
# with Airflow's version document: another process on 127.0.0.1 would answer for it.
if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | grep -q ":$PORT "; then
  echo "spike-airflow-backfill: UNVERIFIED (127.0.0.1:$PORT is taken; pass PORT=<free port>)"
  unverified="port $PORT is taken by another process"
  exit 3
fi
# `kubectl` itself, not the kc() wrapper: $! must be the process the trap has to kill.
if [ -n "$CONTEXT" ]; then
  kubectl --context "$CONTEXT" -n "$NS" port-forward "svc/$APISERVER" "$PORT:8080" \
    >"$tmp/pf.log" 2>&1 &
else
  kubectl -n "$NS" port-forward "svc/$APISERVER" "$PORT:8080" >"$tmp/pf.log" 2>&1 &
fi
PF_PID=$!
up=no
for _ in $(seq 1 40); do
  curl -s -o "$tmp/version.json" -m 2 "$BASE/api/v2/version" 2>/dev/null || true
  if [ -n "$(jq -r '.version // empty' "$tmp/version.json" 2>/dev/null || true)" ]; then up=yes; break; fi
  sleep 0.5
done
if [ "$up" != yes ]; then
  echo "spike-airflow-backfill: UNVERIFIED (127.0.0.1:$PORT does not answer as Airflow; pass PORT=<free port>)"
  unverified="port $PORT does not answer as Airflow"
  exit 3
fi
echo "forward svc/$APISERVER $BASE version=$(jq -r '.version' "$tmp/version.json")"

# The backfill API has to exist in this build; every row would be UNVERIFIED without it.
curl -s -m 10 -o "$tmp/openapi.json" "$BASE/openapi.json" || true
if ! jq -e '.paths["/api/v2/backfills"] and .paths["/api/v2/backfills/dry_run"]' \
    "$tmp/openapi.json" >/dev/null 2>&1; then
  echo "spike-airflow-backfill: UNVERIFIED (/openapi.json does not advertise /api/v2/backfills)"
  unverified="/openapi.json advertises no /api/v2/backfills"
  exit 3
fi

# Sign in as Nova does: POST /auth/token with the Secret's password; the token stays in $TOKEN.
kc -n "$NS" get secret "$SECRET" -o jsonpath='{.data.password}' | base64 -d > "$tmp/pw"
jq -n --arg u "$USER_NAME" --rawfile p "$tmp/pw" '{username:$u,password:$p}' > "$tmp/login.json"
code="$(curl -s -o "$tmp/token.json" -m 30 -w '%{http_code}' -X POST "$BASE/auth/token" \
  -H 'Content-Type: application/json' --data-binary "@$tmp/login.json" 2>/dev/null || printf '000')"
TOKEN="$(jq -r '.access_token // empty' "$tmp/token.json" 2>/dev/null || true)"
rm -f "$tmp/pw"
if [ -z "$TOKEN" ] || [ "$code" != "200" ] && [ "$code" != "201" ]; then
  echo "spike-airflow-backfill: UNVERIFIED (POST /auth/token as $USER_NAME answered $code)"
  unverified="POST /auth/token answered $code"
  exit 3
fi
echo "auth user=$USER_NAME status=$code token=kept"

# --- helpers used by the rows ----------------------------------------------------------------
runs_json() { # out — every run of $DAG
  api GET "/api/v2/dags/$DAG/dagRuns?limit=1000" "$1" >/dev/null || printf '000'
}

# The two-day window for a range with no runs: the DAG's earliest logical_date minus three days.
empty_range() {
  local min day
  runs_json "$tmp/runs-all.json" >/dev/null || true
  min="$(jq -r '[.dag_runs[]? | .logical_date // empty] | min // empty' "$tmp/runs-all.json" 2>/dev/null || true)"
  if [ -z "$min" ]; then
    FROM="2026-06-10T00:00:00Z"; TO="2026-06-11T23:59:59Z"; return 0
  fi
  day="${min%%T*}"
  FROM="$(date -u -d "$day -3 days" +%Y-%m-%dT00:00:00Z)"
  TO="$(date -u -d "$day -2 days" +%Y-%m-%dT23:59:59Z)"
}

# Three free slots before the earliest run: B4's `max_active_runs=1` needs a run to queue behind.
three_day_range() {
  local min day
  runs_json "$tmp/runs-all.json" >/dev/null || true
  min="$(jq -r '[.dag_runs[]? | .logical_date // empty] | min // empty' "$tmp/runs-all.json" 2>/dev/null || true)"
  if [ -z "$min" ]; then
    FROM="2026-06-08T00:00:00Z"; TO="2026-06-10T23:59:59Z"; return 0
  fi
  day="${min%%T*}"
  FROM="$(date -u -d "$day -4 days" +%Y-%m-%dT00:00:00Z)"
  TO="$(date -u -d "$day -2 days" +%Y-%m-%dT23:59:59Z)"
}

backfill_body() { # out [extra json object] — the body for a range backfill of $DAG
  local out="$1" extra="${2:-}"
  [ -n "$extra" ] || extra='{}'
  jq -n --arg d "$DAG" --arg f "$FROM" --arg t "$TO" --argjson x "$extra" \
    '{dag_id:$d, from_date:$f, to_date:$t} + $x' > "$out"
}

scratch_runs() { # out — every run of the scratch DAG; prints the HTTP status
  api GET "/api/v2/dags/$SCRATCH_DAG/dagRuns?limit=100" "$1"
}

close_open_backfills() { # row-tag — Airflow allows one open backfill per DAG; close leftovers
  local tag="$1" oid code
  api GET "/api/v2/backfills?dag_id=$DAG&limit=100" "$tmp/backfills-open.json" >/dev/null || true
  for oid in $(jq -r '.backfills[]? | select(.completed_at == null) | .id' "$tmp/backfills-open.json" 2>/dev/null || true); do
    code="$(api PUT "/api/v2/backfills/$oid/cancel" "$tmp/cancel-open.json")"
    row "$tag" open_backfill "id=$oid" "cancel_status=$code"
  done
}

# --- B1: the role of Nova's credential --------------------------------------------------------
b1() {
  local code roles manager
  code="$(api GET /api/v2/users "$tmp/b1-users.json")"
  row B1 public_api=/api/v2/users "status=$code" "body=$(body "$tmp/b1-users.json")"
  cat > "$tmp/b1.py" <<'PY'
from airflow import settings
from airflow.utils.session import create_session
from sqlalchemy import text
with create_session() as session:
    rows = session.execute(
        text(
            "select r.name from ab_user_role ur "
            "join ab_user u on u.id = ur.user_id "
            "join ab_role r on r.id = ur.role_id "
            "where u.username = :user order by r.name"
        ),
        {"user": "nova-api"},
    ).fetchall()
print("roles=" + (",".join(r[0] for r in rows) or "(none)"))
print("auth_manager=" + str(settings.conf.get("core", "auth_manager")))
PY
  roles="$(kc -n "$NS" exec "deploy/$APISERVER" -c api-server -i -- python - \
    < "$tmp/b1.py" 2>/dev/null | grep -E '^(roles|auth_manager)=' | tr '\n' ' ' || true)"
  row B1 user="$USER_NAME" source=ab_user_role "$roles"
}

# --- B2: what the dry run answers ------------------------------------------------------------
b2() {
  local code latest from2 to2
  empty_range
  backfill_body "$tmp/b2.json"
  code="$(api POST /api/v2/backfills/dry_run "$tmp/b2-out.json" "$tmp/b2.json")"
  row B2 dry_run "range=$FROM..$TO" conf=none "status=$code" \
    "total_entries=$(jq -r '.total_entries // "-"' "$tmp/b2-out.json" 2>/dev/null || printf '-')" \
    "logical_dates=$(jq -r '[.backfills[]?.logical_date] | join(",")' "$tmp/b2-out.json" 2>/dev/null || printf '-')" \
    "partition_keys=$(jq -r '[.backfills[]?.partition_key] | map(select(. != null)) | if length == 0 then "none" else join(",") end' "$tmp/b2-out.json" 2>/dev/null || printf '-')"

  backfill_body "$tmp/b2.json" '{"dag_run_conf":{"runDate":"2026-11-24","params":{"scale":"1000"}}}'
  code="$(api POST /api/v2/backfills/dry_run "$tmp/b2-out.json" "$tmp/b2.json")"
  row B2 dry_run "range=$FROM..$TO" conf=given "status=$code" \
    "total_entries=$(jq -r '.total_entries // "-"' "$tmp/b2-out.json" 2>/dev/null || printf '-')" \
    "logical_dates=$(jq -r '[.backfills[]?.logical_date] | join(",")' "$tmp/b2-out.json" 2>/dev/null || printf '-')" \
    "needs_conf=no"

  # A range that already has runs: what does each reprocess_behavior answer?
  latest="$(jq -r '[.dag_runs[]? | .logical_date // empty] | max // empty' "$tmp/runs-all.json" 2>/dev/null || true)"
  if [ -n "$latest" ]; then
    from2="$(date -u -d "${latest%%T*} -1 day" +%Y-%m-%dT00:00:00Z)"
    to2="${latest%%T*}T23:59:59Z"
    for behavior in none failed completed; do
      jq -n --arg d "$DAG" --arg f "$from2" --arg t "$to2" --arg b "$behavior" \
        '{dag_id:$d, from_date:$f, to_date:$t, reprocess_behavior:$b}' > "$tmp/b2.json"
      code="$(api POST /api/v2/backfills/dry_run "$tmp/b2-out.json" "$tmp/b2.json")"
      row B2 dry_run "range=$from2..$to2" "covered=yes" "reprocess=$behavior" "status=$code" \
        "total_entries=$(jq -r '.total_entries // "-"' "$tmp/b2-out.json" 2>/dev/null || printf '-')"
    done
  fi
}

# --- B3: a real backfill ---------------------------------------------------------------------
b3() {
  local code id start completed waited runs
  close_open_backfills B3
  empty_range
  backfill_body "$tmp/b3.json"
  code="$(api POST /api/v2/backfills "$tmp/b3-out.json" "$tmp/b3.json")"
  id="$(jq -r '.id // empty' "$tmp/b3-out.json" 2>/dev/null || true)"
  row B3 create "range=$FROM..$TO" "status=$code" "backfill_id=${id:-none}" \
    "is_paused=$(jq -r '.is_paused | tostring' "$tmp/b3-out.json" 2>/dev/null || printf '-')" \
    "max_active_runs=$(jq -r '.max_active_runs // "-"' "$tmp/b3-out.json" 2>/dev/null || printf '-')" \
    "response_keys=$(jq -r 'keys | join(",")' "$tmp/b3-out.json" 2>/dev/null || printf '-')"
  [ -z "$id" ] && return 0

  start="$(now_s)"; waited=0; completed=""
  while :; do
    api GET "/api/v2/backfills/$id" "$tmp/b3-get.json" >/dev/null || true
    completed="$(jq -r '.completed_at // empty' "$tmp/b3-get.json" 2>/dev/null || true)"
    waited=$(( $(now_s) - start ))
    [ -n "$completed" ] && break
    [ "$waited" -ge "$BACKFILL_TIMEOUT" ] && break
    sleep 5
  done
  row B3 get "backfill_id=$id" \
    "created_at=$(jq -r '.created_at // "-"' "$tmp/b3-get.json" 2>/dev/null || printf '-')" \
    "completed_at=${completed:-null}" "waited_s=$waited" \
    "is_paused=$(jq -r '.is_paused | tostring' "$tmp/b3-get.json" 2>/dev/null || printf '-')" \
    "names_runs=$(jq -r 'if has("dag_run_ids") or has("dag_runs") then "yes" else "no" end' "$tmp/b3-get.json" 2>/dev/null || printf '-')"

  runs_json "$tmp/b3-runs.json" >/dev/null || true
  runs="$(jq -r --arg f "$FROM" --arg t "$TO" '
    [.dag_runs[]? | select(.run_type == "backfill")
     | select(.logical_date != null and .logical_date >= $f and .logical_date <= $t)] | length' \
    "$tmp/b3-runs.json" 2>/dev/null || printf '-')"
  row B3 runs "count=$runs" "run_type=backfill"
  jq -r --arg f "$FROM" --arg t "$TO" '
    .dag_runs[]? | select(.run_type == "backfill")
    | select(.logical_date != null and .logical_date >= $f and .logical_date <= $t)
    | "B3 run id=\(.dag_run_id) run_type=\(.run_type) logical_date=\(.logical_date) state=\(.state) queued_at=\(.queued_at) start_date=\(.start_date)"' \
    "$tmp/b3-runs.json" 2>/dev/null | while read -r line; do rows[B3]=1; printf '%s\n' "$line"; done
}

# --- B4: what Airflow refuses, and what cancel does ------------------------------------------
b4() {
  local code id states a_from a_to
  # Airflow allows one open backfill per DAG; start from a clean slate so this row measures what
  # it says it measures (a second create while one is open is its own row below).
  close_open_backfills B4

  api GET "/api/v2/dags/$DAG" "$tmp/b4-dag.json" >/dev/null || true
  dag_was_paused="$(jq -r '.is_paused' "$tmp/b4-dag.json" 2>/dev/null || printf false)"
  printf '{"is_paused":true}' > "$tmp/b4-pause.json"
  pause_changed=yes
  code="$(api PATCH "/api/v2/dags/$DAG" "$tmp/b4-patch.json" "$tmp/b4-pause.json")"
  row B4 paused_dag "was_paused=$dag_was_paused" "patch_status=$code" \
    "is_paused_now=$(jq -r '.is_paused | tostring' "$tmp/b4-patch.json" 2>/dev/null || printf '-')"

  empty_range
  backfill_body "$tmp/b4.json"
  code="$(api POST /api/v2/backfills "$tmp/b4-out.json" "$tmp/b4.json")"
  id="$(jq -r '.id // empty' "$tmp/b4-out.json" 2>/dev/null || true)"
  row B4 paused_create "range=$FROM..$TO" "status=$code" \
    "refused=$([ "$code" = 200 ] && echo no || echo yes)" \
    "body=$(body "$tmp/b4-out.json")"

  sleep 20
  runs_json "$tmp/b4-runs.json" >/dev/null || true
  states="$(jq -r --arg f "$FROM" --arg t "$TO" '
    [.dag_runs[]? | select(.logical_date != null and .logical_date >= $f and .logical_date <= $t)
     | "\(.run_type):\(.state)"] | join(",")' "$tmp/b4-runs.json" 2>/dev/null || printf '-')"
  row B4 paused_runs "after_s=20" "dag_paused=yes" "runs=$states"
  a_from="$FROM"; a_to="$TO"

  # A second backfill while the first is open: one per DAG.
  empty_range
  backfill_body "$tmp/b4.json"
  code="$(api POST /api/v2/backfills "$tmp/b4-out2.json" "$tmp/b4.json")"
  row B4 second_create "range=$FROM..$TO" "open_backfill=$id" "status=$code" \
    "refused=$([ "$code" = 409 ] && echo yes || echo no)" "body=$(body "$tmp/b4-out2.json")"

  # Cancel while every run is still queued: what happens to them?
  code="$(api PUT "/api/v2/backfills/$id/cancel" "$tmp/b4-cancel.json")"
  row B4 cancel "backfill_id=$id" "status=$code" \
    "completed_at=$(jq -r '.completed_at // "null"' "$tmp/b4-cancel.json" 2>/dev/null || printf '-')" \
    "is_paused=$(jq -r '.is_paused | tostring' "$tmp/b4-cancel.json" 2>/dev/null || printf '-')"
  sleep 15
  runs_json "$tmp/b4-runs.json" >/dev/null || true
  states="$(jq -r --arg f "$a_from" --arg t "$a_to" '
    [.dag_runs[]? | select(.logical_date != null and .logical_date >= $f and .logical_date <= $t)
     | "\(.logical_date)=\(.state)"] | join(",")' "$tmp/b4-runs.json" 2>/dev/null || printf '-')"
  row B4 after_cancel "after_s=15" "runs=$states" \
    "runs_still_there=$(jq -r --arg f "$a_from" --arg t "$a_to" '
      [.dag_runs[]? | select(.logical_date != null and .logical_date >= $f and .logical_date <= $t)] | length' \
      "$tmp/b4-runs.json" 2>/dev/null || printf '-')"

  printf '{"is_paused":%s}' "$dag_was_paused" > "$tmp/b4-pause.json"
  pause_changed=no
  code="$(api PATCH "/api/v2/dags/$DAG" "$tmp/b4-patch.json" "$tmp/b4-pause.json")"
  row B4 unpaused "status=$code" "is_paused=$(jq -r '.is_paused | tostring' "$tmp/b4-patch.json" 2>/dev/null || printf '-')"

  # max_active_runs=1 over three dates: one runs, the others wait behind it.
  three_day_range
  backfill_body "$tmp/b4.json" '{"max_active_runs":1}'
  code="$(api POST /api/v2/backfills "$tmp/b4-out.json" "$tmp/b4.json")"
  id="$(jq -r '.id // empty' "$tmp/b4-out.json" 2>/dev/null || true)"
  row B4 max_active_runs_1 "range=$FROM..$TO" "status=$code" "backfill_id=${id:-none}" \
    "max_active_runs=$(jq -r '.max_active_runs | tostring' "$tmp/b4-out.json" 2>/dev/null || printf '-')" \
    "body=$(body "$tmp/b4-out.json")"
  [ -z "$id" ] && return 0
  sleep 25
  runs_json "$tmp/b4-runs.json" >/dev/null || true
  states="$(jq -r --arg f "$FROM" --arg t "$TO" '
    [.dag_runs[]? | select(.logical_date != null and .logical_date >= $f and .logical_date <= $t)
     | "\(.logical_date)=\(.state)"] | join(",")' "$tmp/b4-runs.json" 2>/dev/null || printf '-')"
  row B4 queued "after_s=25" "runs=$states"

  code="$(api PUT "/api/v2/backfills/$id/cancel" "$tmp/b4-cancel.json")"
  row B4 cancel_max_active_runs_1 "backfill_id=$id" "status=$code" \
    "completed_at=$(jq -r '.completed_at // "null"' "$tmp/b4-cancel.json" 2>/dev/null || printf '-')"
  sleep 15
  runs_json "$tmp/b4-runs.json" >/dev/null || true
  states="$(jq -r --arg f "$FROM" --arg t "$TO" '
    [.dag_runs[]? | select(.logical_date != null and .logical_date >= $f and .logical_date <= $t)
     | "\(.logical_date)=\(.state)"] | join(",")' "$tmp/b4-runs.json" 2>/dev/null || printf '-')"
  row B4 after_cancel_max_active_runs_1 "after_s=15" "runs=$states"
}

# --- B5: the missed schedule -----------------------------------------------------------------
b5() {
  local code appeared started first_logical down up states late first_after down_epoch up_epoch waited
  cat > "$tmp/pipeline.yaml" <<YAML
apiVersion: stardelt.io/v1alpha1
kind: Pipeline
metadata:
  name: $PIPELINE
  namespace: $NS
  labels:
    stardelt.io/owner-group: demo
spec:
  engine: airflow
  ownerGroup: demo
  runAs: pipeline-demo
  concurrency: Forbid
  retries:
    attempts: 1
    backoff: 1m
  schedule:
    cron: "*/2 * * * *"
    timezone: Europe/Berlin
  tasks:
    - kind: sql
      name: ping
      sql: select 1
YAML
  pipeline_applied=yes
  kc -n "$NS" apply -f "$tmp/pipeline.yaml" >/dev/null || {
    row B5 apply "status=failed"; return 0; }
  row B5 apply "pipeline=$PIPELINE" "dag=$SCRATCH_DAG" "status=applied"

  # The dag-processor's interval is ~30 s: wait for the DAG to appear.
  started="$(now_s)"; appeared=no; waited=0
  while :; do
    code="$(api GET "/api/v2/dags/$SCRATCH_DAG" "$tmp/b5-dag.json")"
    [ "$code" = 200 ] && { appeared=yes; break; }
    waited=$(( $(now_s) - started ))
    [ "$waited" -ge "$TIMEOUT" ] && break
    sleep 5
  done
  row B5 dag "dag=$SCRATCH_DAG" "appeared=$appeared" "appeared_after_s=$waited"
  [ "$appeared" = yes ] || return 0

  # One successful run: */2 gives a slot every two minutes.
  started="$(now_s)"; first_logical=""; waited=0
  while :; do
    scratch_runs "$tmp/b5-runs.json" >/dev/null || true
    first_logical="$(jq -r '[.dag_runs[]? | select(.state == "success") | .logical_date] | min // empty' \
      "$tmp/b5-runs.json" 2>/dev/null || true)"
    [ -n "$first_logical" ] && break
    waited=$(( $(now_s) - started ))
    [ "$waited" -ge "$TIMEOUT" ] && break
    sleep 5
  done
  if [ -z "$first_logical" ]; then
    row B5 first_run "state=success" "found=no" "waited_s=$waited"
    return 0
  fi
  row B5 first_run "logical_date=$first_logical" "state=success" \
    "run_id=$(jq -r --arg l "$first_logical" '[.dag_runs[]? | select(.logical_date == $l)][0].dag_run_id // "-"' "$tmp/b5-runs.json" 2>/dev/null || printf '-')"

  # Down: the scheduler is the only thing that creates runs; two and a half slots pass.
  down="$(stamp "$(now_s)")"
  down_epoch="$(now_s)"
  kc -n "$NS" scale deploy "$SCHEDULER" --replicas=0 >/dev/null
  sleep 10
  row B5 scheduler_down "at=$down" "replicas=0" "down_seconds=$DOWN_SECONDS" \
    "ready=$(kc -n "$NS" get deploy "$SCHEDULER" -o json 2>/dev/null | jq -r '.status.readyReplicas // 0')"
  sleep "$DOWN_SECONDS"
  kc -n "$NS" scale deploy "$SCHEDULER" --replicas="$sched_replicas" >/dev/null
  up="$(stamp "$(now_s)")"
  up_epoch="$(now_s)"
  kc -n "$NS" rollout status "deploy/$SCHEDULER" --timeout=180s >/dev/null 2>&1 || true
  row B5 scheduler_up "at=$up" "replicas=$sched_replicas" \
    "ready=$(kc -n "$NS" get deploy "$SCHEDULER" -o json 2>/dev/null | jq -r '.status.readyReplicas // 0')"

  # Does anything appear after the return, and after how long?
  first_after=""
  while :; do
    scratch_runs "$tmp/b5-runs.json" >/dev/null || true
    first_after="$(jq -r --arg l "$first_logical" '[.dag_runs[]? | .logical_date // empty | select(. > $l)] | min // empty' \
      "$tmp/b5-runs.json" 2>/dev/null || true)"
    [ -n "$first_after" ] && break
    waited=$(( $(now_s) - up_epoch ))
    [ "$waited" -ge 300 ] && break
    sleep 5
  done
  row B5 runs_after_up "found=$([ -n "$first_after" ] && echo yes || echo no)" \
    "first_logical_date=${first_after:-none}" \
    "delay_s=$(( $(now_s) - up_epoch ))"
  jq -r --arg l "$first_logical" '
    .dag_runs[]? | select(.logical_date != null and .logical_date > $l)
    | "B5 run id=\(.dag_run_id) run_type=\(.run_type) logical_date=\(.logical_date) run_after=\(.run_after) queued_at=\(.queued_at) state=\(.state)"' \
    "$tmp/b5-runs.json" 2>/dev/null | while read -r line; do rows[B5]=1; printf '%s\n' "$line"; done

  # Slots whose logical_date falls inside the outage: created late, or never?
  states="$(jq -r --arg d "$down" --arg u "$up" '
    [.dag_runs[]? | select(.logical_date != null and .logical_date >= $d and .logical_date <= $u)
     | "\(.logical_date)=\(.state)"] | join(",")' "$tmp/b5-runs.json" 2>/dev/null || true)"
  late="$(jq -r --arg d "$down" --arg u "$up" '
    [.dag_runs[]? | select(.logical_date != null and .logical_date >= $d and .logical_date <= $u)
     | select(.queued_at != null and .queued_at > $u)] | length' "$tmp/b5-runs.json" 2>/dev/null || printf 0)"
  row B5 missed_slots "window=$down..$up" "runs_in_window=${states:-none}" \
    "created_after_return=$late"

  # The slots between the last run before the outage and the first one after it: how many
  # passed, how many got a run, how many are never created.
  last_before="$(jq -r --arg d "$down" '[.dag_runs[]? | .logical_date // empty | select(. <= $d)] | max // empty' \
    "$tmp/b5-runs.json" 2>/dev/null || true)"
  gap_slots=0; gap_created=0
  if [ -n "$last_before" ] && [ -n "$first_after" ]; then
    gap_slots=$(( ( $(date -u -d "$first_after" +%s) - $(date -u -d "$last_before" +%s) ) / 120 ))
    gap_created="$(jq -r --arg l "$last_before" --arg f "$first_after" '
      [.dag_runs[]? | .logical_date // empty | select(. > $l and . <= $f)] | length' \
      "$tmp/b5-runs.json" 2>/dev/null || printf 0)"
  fi
  row B5 gap "last_run_before_down=${last_before:-none}" "first_run_after_up=${first_after:-none}" \
    "slots_in_gap=$gap_slots" "runs_created=$gap_created" "missing=$((gap_slots - gap_created))"

  kc -n "$NS" delete pipeline "$PIPELINE" --ignore-not-found >/dev/null 2>&1 || true
  pipeline_applied=no
  row B5 scratch_removed "pipeline=$PIPELINE" \
    "left=$(kc -n "$NS" get pipelines.stardelt.io 2>/dev/null | grep -c "$PIPELINE" || true)"
  B5_LATE="$late"
  B5_WINDOW="$states"
  B5_MISSING="$((gap_slots - gap_created))"
  B5_SLOTS="$gap_slots"
  B5_CREATED="$gap_created"
}

# --- B6: dag_run_conf through a backfill -----------------------------------------------------
b6() {
  local code id rid conf sent verbatim
  close_open_backfills B6
  empty_range
  jq -n --arg d "$DAG" --arg f "$FROM" --arg t "$TO" \
    '{dag_id:$d, from_date:$f, to_date:$t,
      dag_run_conf:{runDate:"2026-11-24", params:{scale:"1000"}}}' > "$tmp/b6.json"
  code="$(api POST /api/v2/backfills "$tmp/b6-out.json" "$tmp/b6.json")"
  id="$(jq -r '.id // empty' "$tmp/b6-out.json" 2>/dev/null || true)"
  row B6 create "range=$FROM..$TO" "status=$code" "backfill_id=${id:-none}" \
    "dag_run_conf=$(jq -c '.dag_run_conf' "$tmp/b6-out.json" 2>/dev/null || printf '-')"
  [ -z "$id" ] && return 0

  sleep 10
  runs_json "$tmp/b6-runs.json" >/dev/null || true
  rid="$(jq -r --arg f "$FROM" --arg t "$TO" '
    [.dag_runs[]? | select(.logical_date != null and .logical_date >= $f and .logical_date <= $t)
     | select(.run_type == "backfill")][0].dag_run_id // empty' "$tmp/b6-runs.json" 2>/dev/null || true)"
  if [ -n "$rid" ]; then
    local enc; enc="$(jq -rn --arg v "$rid" '$v | @uri')"
    api GET "/api/v2/dags/$DAG/dagRuns/$enc" "$tmp/b6-run.json" >/dev/null || true
    # Airflow answers the object with its own key order: compare the canonical form.
    conf="$(jq -S -c '.conf // {}' "$tmp/b6-run.json" 2>/dev/null || printf '{}')"
    sent="$(jq -S -c '.dag_run_conf' "$tmp/b6-out.json" 2>/dev/null || printf '{}')"
    verbatim=no
    [ "$conf" = "$sent" ] && verbatim=yes
    row B6 run "id=$rid" "conf=$conf" "sent=$sent" "verbatim=$verbatim"
  else
    row B6 run "id=none" "conf=-" "verbatim=no"
  fi
  # Do not let the demo run dbt with the spike's conf: cancel the backfill it belongs to.
  code="$(api PUT "/api/v2/backfills/$id/cancel" "$tmp/b6-cancel.json")"
  row B6 cancel "backfill_id=$id" "status=$code" \
    "completed_at=$(jq -r '.completed_at // "null"' "$tmp/b6-cancel.json" 2>/dev/null || printf '-')"
}

# --- rows ------------------------------------------------------------------------------------
b1
b2
b3
b4
b5
b6

# --- C1: the conclusions ---------------------------------------------------------------------
B3_RUNS="$(runs_json "$tmp/c1-runs.json" >/dev/null; jq -r '[.dag_runs[]? | select(.run_type=="backfill")] | length' "$tmp/c1-runs.json" 2>/dev/null || printf '-')"
row C1 backfill="start Airflow's own backfill (POST /api/v2/backfills), not N triggers: the credential can, the dry run gives the dates first, and the runs carry run_type=backfill" \
  "poll=GET /api/v2/backfills/{id} for completed_at (it names no runs) and GET /api/v2/dags/{dag}/dagRuns, filtering run_type==backfill (run_id 'backfill__<logical_date>')" \
  "limits=one open backfill per DAG (409 on a second), cancel marks its queued runs failed, reprocess_behavior=completed replays dates that already ran" \
  "backfill_runs_seen=$B3_RUNS"
if [ "${B5_CREATED:-0}" = 0 ]; then
  row C1 missed="catchup=False + CronTriggerTimetable: Airflow created no run for any of the ${B5_SLOTS:-?} slots of the outage" \
    "reading=the page shows Missed for every slot that passed"
elif [ "${B5_MISSING:-0}" = 0 ]; then
  row C1 missed="catchup=False + CronTriggerTimetable: every slot of the outage got a run late (${B5_CREATED} of ${B5_SLOTS})" \
    "reading=the page shows each run and its own logical_date"
else
  row C1 missed="catchup=False + CronTriggerTimetable: only ${B5_CREATED} of the ${B5_SLOTS:-?} slots of the outage got a run, ${B5_MISSING} got none" \
    "created=${B5_WINDOW:-unknown}" \
    "reading=the page shows the late run with its own logical_date and Missed for the ${B5_MISSING} skipped slots"
fi

# --- verdict ---------------------------------------------------------------------------------
for n in B1 B2 B3 B4 B5 B6; do
  [ -n "${rows[$n]:-}" ] || missing="${missing:+$missing }$n"
done
if [ -n "$unverified" ]; then
  echo "spike-airflow-backfill: UNVERIFIED ($unverified)"
  exit 3
fi
if [ -n "$missing" ]; then
  echo "spike-airflow-backfill: FAIL (missing rows: $missing)"
  exit 1
fi
echo "spike-airflow-backfill: PASS (6 rows)"
