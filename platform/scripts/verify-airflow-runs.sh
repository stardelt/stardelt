#!/usr/bin/env bash
# Prove every unpaused pipeline ran: for each DAG the platform schedules, the newest DAG run
# and its state. `make verify-cronjobs` reads Kubernetes Jobs and `verify-streaming` reads
# StreamApps; a pipeline run is an Airflow DAG run, and nothing read its outcome — so the
# demo's retail-medallion pipeline failed every scheduled run from 2026-11-13 and no check
# said so (platform#1713). Measured 2026-11-17T16:2xZ, newest run per DAG:
#
#   2026-11-17T04:00 failed scheduled     stardelt__retail-medallion
#   2026-11-16T13:35 failed manual
#   2026-11-16T04:00 failed scheduled
#   2026-11-15T09:36 failed manual
#   2026-11-15T04:00 failed scheduled
#   2026-11-14T04:00 failed scheduled
#   2026-11-13T04:00 failed scheduled
#   2026-11-12T04:00 success scheduled    the last good run
#
#   NS=stardelt bash scripts/verify-airflow-runs.sh
#
# Environment:
#   NS      namespace, default stardelt
#   PORT    local port for the port-forward, default 18081 (verify-airflow-actions uses 18080)
#   SECRET  Secret holding the service user's password, default airflow-nova-api
#
# Read-only: two GETs per unpaused DAG, nothing created, paused, patched or deleted. It signs
# in the way Nova does (POST /auth/token -> 201) and reads the same API v2 endpoints.
#
# A stale success is printed, never fatal: scheduling that stopped is a different check. A
# paused DAG, a DAG with no timetable (no schedule — a manual-only pipeline such as
# retail-medallion since platform#1713), a DAG with no run, and a run that is running or
# queued are printed with `·` and never fail this check.
#
# Exit 0 = PASS, 1 = FAIL (an unpaused DAG's newest run failed), 3 = UNVERIFIED (could not
# see the subject: no kubectl/curl/python3, no Secret, the port already in use, no
# port-forward, an answer that is not the Airflow API, no DAG list).
set -euo pipefail

NS="${NS:-stardelt}"
PORT="${PORT:-18081}"
SECRET="${SECRET:-airflow-nova-api}"

unverified() { echo "UNVERIFIED ($*)"; exit 3; }
fail() { echo "FAIL ($*)"; exit 1; }

for cmd in kubectl curl python3; do
  command -v "$cmd" >/dev/null 2>&1 || unverified "$cmd not found"
done

# ---------------------------------------------------------------------------
# 0. The port must be free. `kubectl port-forward` prints `address already in
#    use` and dies while anything else holds the port, and the sign-in below
#    then talks to that other service — on 2026-11-17 a stale Nova forward on
#    18081 answered `405 Method Not Allowed` and the check called a healthy
#    Airflow broken. The busy-port path runs before any cluster call.
# ---------------------------------------------------------------------------
if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  unverified "port $PORT is already in use — stop the other forward or set PORT"
fi

# ---------------------------------------------------------------------------
# 1. The service user's credential. A missing Secret is not a failure of the
#    platform: an install without Pipelines has no service user to check.
#    The values are used and never printed.
# ---------------------------------------------------------------------------
if ! secret="$(kubectl -n "$NS" get secret "$SECRET" -o json 2>&1)"; then
  case "$secret" in
    *NotFound*) unverified "no Secret $SECRET" ;;
    *) unverified "cannot read Secret $SECRET: $secret" ;;
  esac
fi
username="$(printf '%s' "$secret" | python3 -c 'import base64,json,sys; print(base64.b64decode(json.load(sys.stdin)["data"]["username"]).decode())')" \
  || unverified "Secret $SECRET has no username"
password="$(printf '%s' "$secret" | python3 -c 'import base64,json,sys; print(base64.b64decode(json.load(sys.stdin)["data"]["password"]).decode())')" \
  || unverified "Secret $SECRET has no password"
unset secret

# ---------------------------------------------------------------------------
# 2. Port-forward the API server. The API is only reachable in-cluster; the
#    forward is killed on every exit path, and its stderr is kept so a forward
#    that died says why instead of leaving the sign-in to whatever answers.
# ---------------------------------------------------------------------------
pf_log="$(mktemp)"
kubectl -n "$NS" port-forward svc/airflow-api-server "$PORT":8080 >/dev/null 2>"$pf_log" &
PF_PID=$!
cleanup() { kill "$PF_PID" 2>/dev/null || true; rm -f "$pf_log"; }
trap cleanup EXIT

# /api/v2/version is both liveness and identity: a 200 says this is the Airflow
# API, not another service that happens to hold the port (`/health` answers 404
# here, so it only proved that something accepted the TCP connection).
base="http://127.0.0.1:$PORT"
up=0
version_code=""
for _ in $(seq 1 20); do
  kill -0 "$PF_PID" 2>/dev/null || unverified "port-forward exited: $(tail -n 1 "$pf_log")"
  version_code="$(curl -s -o /dev/null -w '%{http_code}' "$base/api/v2/version" || true)"
  if [ "$version_code" = "200" ]; then up=1; break; fi
  sleep 0.5
done
[ "$up" -eq 1 ] || unverified "$base is not the Airflow API (GET /api/v2/version answered $version_code)"

# ---------------------------------------------------------------------------
# 3. Sign in as the service user. 401/403 is the #476/#680 shape: the user does
#    not exist or the Secret's password is not its password.
# ---------------------------------------------------------------------------
token_body="$(AIRFLOW_USER="$username" AIRFLOW_PASS="$password" python3 -c \
  'import json,os; print(json.dumps({"username": os.environ["AIRFLOW_USER"], "password": os.environ["AIRFLOW_PASS"]}))')"
unset username password

token_raw="$(printf '%s' "$token_body" | curl -s -H 'Content-Type: application/json' \
  --data-binary @- -w '\n%{http_code}' "$base/auth/token")" || fail "cannot reach $base/auth/token"
unset token_body
token_code="${token_raw##*$'\n'}"
token_payload="${token_raw%$'\n'*}"

case "$token_code" in
  201) ;;
  401|403) fail "nova-api cannot sign in to Airflow — check Secret $SECRET" ;;
  *) fail "sign-in returned HTTP $token_code, expected 201" ;;
esac

token="$(printf '%s' "$token_payload" | python3 -c \
  'import json,sys; print(json.load(sys.stdin)["access_token"])')" \
  || fail "sign-in answered $token_code without an access_token"
[ -n "$token" ] || fail "sign-in answered 201 with an empty access_token"
unset token_raw token_payload

# The bearer token travels in a curl config on a pipe, never on the command
# line, so it does not show up in the process list.
api_curl() {
  curl -s -K <(printf 'header = "Authorization: Bearer %s"\n' "$token") "$@"
}

# ---------------------------------------------------------------------------
# 4. List the DAGs. 403 is the #680 shape: `nova-api` lost its role (or the
#    role is empty), which every manifest looks correct while.
# ---------------------------------------------------------------------------
dags_raw="$(api_curl -w '\n%{http_code}' "$base/api/v2/dags?limit=200")" \
  || fail "cannot reach $base/api/v2/dags"
dags_code="${dags_raw##*$'\n'}"
dags_payload="${dags_raw%$'\n'*}"

case "$dags_code" in
  200) ;;
  401|403) fail "nova-api cannot list DAGs — role stardelt-nova-operator is missing or empty" ;;
  *) fail "listing DAGs returned HTTP $dags_code" ;;
esac

dag_rows="$(printf '%s' "$dags_payload" | python3 -c '
import json, sys
dags = json.load(sys.stdin).get("dags") or []
for dag in dags:
    print("%s\t%s\t%s" % (dag.get("dag_id"), "true" if dag.get("is_paused") else "false",
                          dag.get("timetable_summary") or ""))
')" || unverified "cannot read the DAG list"
unset dags_raw dags_payload

[ -n "$dag_rows" ] || unverified "Airflow lists no DAGs in $NS"

failed_ids=()
dags_seen=0
paused_count=0
unscheduled_count=0

while IFS=$'\t' read -r dag is_paused timetable; do
  [ -n "$dag" ] || continue
  dags_seen=$((dags_seen + 1))

  if [ "$is_paused" = "true" ]; then
    paused_count=$((paused_count + 1))
    printf '%s %-32s paused\n' '·' "$dag"
    continue
  fi

  if [ -z "$timetable" ]; then
    unscheduled_count=$((unscheduled_count + 1))
    printf '%s %-32s %s\n' '·' "$dag" "unscheduled"
    continue
  fi

  runs_raw="$(api_curl -w '\n%{http_code}' \
    "$base/api/v2/dags/$dag/dagRuns?order_by=-run_after&limit=1")" \
    || fail "cannot reach $base/api/v2/dags/$dag/dagRuns"
  runs_code="${runs_raw##*$'\n'}"
  runs_payload="${runs_raw%$'\n'*}"

  case "$runs_code" in
    200) ;;
    401|403) fail "nova-api cannot read the runs of DAG $dag — role stardelt-nova-operator is missing or empty" ;;
    *) fail "listing runs of DAG $dag returned HTTP $runs_code" ;;
  esac

  # Columns: state, run_after (formatted), age — one python call, so the date
  # arithmetic and the JSON stay in the same place as the rest of the script.
  run_row="$(printf '%s' "$runs_payload" | python3 -c '
import datetime, json, sys

runs = json.load(sys.stdin).get("dag_runs") or []
if not runs:
    print("no runs\t\t")
    sys.exit(0)

run = runs[0]
state = run.get("state") or "unknown"
run_after = run.get("run_after") or run.get("logical_date") or ""
stamp = ""
age = ""
if run_after:
    try:
        when = datetime.datetime.fromisoformat(run_after.replace("Z", "+00:00"))
    except ValueError:
        stamp = run_after
    else:
        stamp = when.strftime("%Y-%m-%dT%H:%MZ")
        delta = datetime.datetime.now(datetime.timezone.utc) - when
        minutes = int(delta.total_seconds() // 60)
        if minutes < 0:
            age = "in the future"
        elif minutes < 60:
            age = "%dm ago" % minutes
        elif minutes < 60 * 24:
            age = "%dh ago" % (minutes // 60)
        else:
            age = "%dd ago" % (minutes // (60 * 24))
print("%s\t%s\t%s" % (state, stamp, age))
')" || unverified "cannot read the runs of DAG $dag"
  IFS=$'\t' read -r state stamp age <<< "$run_row"

  case "$state" in
    success)
      printf '%s %-32s %-8s %-18s %s\n' '✓' "$dag" "$state" "$stamp" "$age"
      ;;
    failed|upstream_failed)
      printf '%s %-32s %-8s %-18s %s\n' '✗' "$dag" "$state" "$stamp" "$age"
      failed_ids+=("$dag")
      ;;
    *)
      printf '%s %-32s %-8s %-18s %s\n' '·' "$dag" "$state" "$stamp" "$age"
      ;;
  esac
done < <(printf '%s\n' "$dag_rows")

if [ "${#failed_ids[@]}" -gt 0 ]; then
  fail "${#failed_ids[@]} DAG run(s) failed: ${failed_ids[*]}"
fi

echo "PASS ($dags_seen DAG(s): every unpaused and scheduled DAG's newest run succeeded, $paused_count paused, $unscheduled_count unscheduled)"
