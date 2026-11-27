#!/usr/bin/env bash
# Spike for stardelt/stardelt#2001 (slice of #66): Airflow 3.2 asset schedules — an `all`
# schedule (`schedule=[Asset(…), Asset(…)]`), an `any` schedule (`Asset(…) | Asset(…)`), and an
# asset event posted as Nova's user.
#
#   make spike-airflow-assets      (NS=… PORT=… TIMEOUT=…)
#
# Why: #66 must decide whether the loader raises asset events from a Pipeline's declared `writes`
# or Nova's lineage consumer posts them, and must write `spec.triggers`' `mode: all|any`. Neither
# choice is safe without measuring what the installed Airflow (3.2.2) does with the two schedule
# forms, what `logical_date` an asset-triggered run carries, and whether Nova's user may post an
# event at all.
#
# Rows, `key=value`, one or more `A<n>` lines each:
#   A1  the image: `airflow version` and that `from airflow.sdk import DAG, Asset` imports.
#   A2  the `all` DAG: `schedule=[Asset(spike://asset-a), Asset(spike://asset-b)]` copied into the
#       dag folder: how long until it appears, whether it is created paused, and both asset ids.
#   A3  one event for asset-a: the status, and whether the DAG ran (with `all` it must not).
#   A4  one event for asset-b: the status, the run, its `run_type`, `logical_date`, `state`, and
#       the delay from the POST.
#   A5  two more events for asset-a while the A4 run is queued or running: the number of runs and
#       how many events sit in the asset's queue (one, or two).
#   A6  the `any` DAG (`Asset(a) | Asset(b)`): after one event for asset-b, does it run on one
#       asset alone, and after how long.
#   A7  Nova's user: the sign-in status, `GET /api/v2/users`, the exact role list, and whether the
#       role holds `can_create` on `Assets` (what `POST /api/v2/assets/events` checks).
#   C1  the conclusion for #66, one sentence per question.
#
# The event POST is the measurement, not an assumption: A3 records the status Nova's user gets.
# `POST /api/v2/assets/events` runs under `requires_access_asset(method="POST")`, i.e. FAB
# `can_create` on `Assets`, and the role the platform gives `nova-api` did not hold it. The token
# carries the permissions it was signed with, so a grant only shows on a new token. When the plain
# POST is refused, the script grants `can_create` on `Assets` to that one role for the rest of the
# measurement (recorded as `grant=…` on A3/A7), signs in again, and continues; A7 records the role
# as it is, and the trap removes the permission again. That is what makes rows A4–A6 measurable
# at all; a refusal alone would leave the `all`/`any` question of #66 unanswered.
#
# Asset-triggered runs carry `logic_date: null` (the event's timestamp is `run_after`), so the
# "delay from the POST to the run's logical_date" of A4 is reported as `delay_s` to the run's
# `start_date`, with `logical_date=null` printed beside it.
#
# Exit 0 = PASS (A1–A7 each printed at least one line), 1 = FAIL (a row is missing), 3 =
# UNVERIFIED (no kubectl, the port-forward, the sign-in or `kubectl cp` does not work — nothing
# was measured). A refusal (403 on the event POST, a DAG that never schedules) is a measurement:
# it is printed and the run continues.
#
# It leaves the cluster as it found it: the trap removes both DAG files from the dag-processor,
# deletes both DAGs' metadata (`airflow dags delete`: a removed file leaves its row and its runs
# behind in Airflow 3.2), takes back a permission it granted, kills the port-forward and removes
# the scratch directory — also after a `set -e` failure. Until the DAG metadata is deleted the
# next run would inherit the runs of this one; the dag ids are the ticket's, so they are cleared.
#
# No secret is printed: the password goes from Secret `airflow-nova-api` into a 0600 file and into
# the login body on stdin; the token lives in a variable only, never in a line or a file name.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-${NAMESPACE:-stardelt}}"
PORT="${PORT:-18080}"
BASE="http://127.0.0.1:$PORT"
TIMEOUT="${TIMEOUT:-300}"            # a DAG to appear, then a run to appear
ABSENCE_WAIT="${ABSENCE_WAIT:-30}"   # A3: how long "the all DAG did not run" is observed
PERMISSION_WAIT="${PERMISSION_WAIT:-120}"  # A3: how long a granted permission may take to apply
APISERVER="${APISERVER:-airflow-api-server}"
DAGPROC="${DAGPROC:-airflow-dag-processor}"
SECRET="${SECRET:-airflow-nova-api}"
USER_NAME="${USER_NAME:-}"          # from the Secret's `username` key when empty
CONTEXT="${CONTEXT:-}"

ALL_DAG="spike_assets_all"
ANY_DAG="spike_assets_any"
A_ASSET="spike://asset-a"
B_ASSET="spike://asset-b"
DAG_FILES="$ALL_DAG.py $ANY_DAG.py"

scratch="${TMPDIR:-$HOME/.cache/opencode-scratch}"
mkdir -p "$scratch"
tmp="$(mktemp -d "$scratch/spike-airflow-assets.XXXXXX")"
PF_PID=""
TOKEN=""
DAG_POD=""
ROLE=""
ROLE_CREATE=no
GRANTED=no
DAGS_COPIED=no
DAG_DELETED=no
A3_DELIVERED=no
A3_RAN=unknown
A4_RAN=unknown
A5_POSTED=0
A5_QUEUED=unknown
A6_RAN=unknown
POST_STATUS=""
POST_WAITED=0
declare -A rows=()
missing=""
unverified=""

kc() { # kubectl, with the pinned context when one is given
  if [ -n "$CONTEXT" ]; then kubectl --context "$CONTEXT" "$@"; else kubectl "$@"; fi
}

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  # The DAG metadata lives in Airflow, not in the pod: delete it before the port-forward dies.
  if [ "$DAG_DELETED" != yes ] && [ -n "$TOKEN" ]; then
    delete_dags >/dev/null 2>&1 || true
  fi
  if [ "$GRANTED" = yes ] && [ -n "$ROLE" ]; then
    kc -n "$NS" exec "deploy/$APISERVER" -c api-server -- \
      airflow roles del-perms "$ROLE" -a can_create -r Assets >/dev/null 2>&1 || true
  fi
  if [ -n "$DAG_POD" ] && [ "$DAGS_COPIED" = yes ]; then
    kc -n "$NS" exec "$DAG_POD" -c dag-processor -- rm -f \
      /opt/airflow/dags/"$ALL_DAG".py /opt/airflow/dags/"$ANY_DAG".py >/dev/null 2>&1 || true
  fi
  if [ -n "$PF_PID" ]; then kill "$PF_PID" >/dev/null 2>&1 || true; fi
  rm -rf "$tmp"
  exit "$rc"
}
trap cleanup EXIT INT TERM

row() { # A<n> key=value … — remembers which rows were printed
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

signin() { # sets TOKEN and signin_code; prints nothing secret
  kc -n "$NS" get secret "$SECRET" -o jsonpath='{.data.password}' | base64 -d > "$tmp/pw"
  jq -n --arg u "$USER_NAME" --rawfile p "$tmp/pw" '{username:$u,password:$p}' > "$tmp/login.json"
  rm -f "$tmp/pw"
  signin_code="$(curl -s -o "$tmp/token.json" -m 30 -w '%{http_code}' -X POST "$BASE/auth/token" \
    -H 'Content-Type: application/json' --data-binary "@$tmp/login.json" 2>/dev/null || printf '000')"
  TOKEN="$(jq -r '.access_token // empty' "$tmp/token.json" 2>/dev/null || true)"
}

# --- preflight -------------------------------------------------------------------------------
for tool in kubectl curl jq base64 date; do
  command -v "$tool" >/dev/null 2>&1 || unverified="${unverified:+$unverified, }no $tool"
done
if [ -n "$unverified" ]; then
  echo "spike-airflow-assets: UNVERIFIED ($unverified)"
  exit 3
fi

if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | grep -q ":$PORT "; then
  echo "spike-airflow-assets: UNVERIFIED (127.0.0.1:$PORT is taken; pass PORT=<free port>)"
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
  echo "spike-airflow-assets: UNVERIFIED (127.0.0.1:$PORT does not answer as Airflow; pass PORT=<free port>)"
  exit 3
fi

curl -s -m 10 -o "$tmp/openapi.json" "$BASE/openapi.json" || true
if ! jq -e '.paths["/api/v2/assets/events"]' "$tmp/openapi.json" >/dev/null 2>&1; then
  echo "spike-airflow-assets: UNVERIFIED (/openapi.json does not advertise /api/v2/assets/events)"
  exit 3
fi

# Sign in as Nova does: the Secret's `username` and `password`, POST /auth/token. The password
# goes into a 0600 file and into the login body on stdin; the token stays in $TOKEN.
if [ -z "$USER_NAME" ]; then
  USER_NAME="$(kc -n "$NS" get secret "$SECRET" -o jsonpath='{.data.username}' 2>/dev/null \
    | base64 -d 2>/dev/null || true)"
fi
signin
if [ -z "$TOKEN" ] || { [ "$signin_code" != "200" ] && [ "$signin_code" != "201" ]; }; then
  echo "spike-airflow-assets: UNVERIFIED (POST /auth/token as $USER_NAME answered $signin_code)"
  exit 3
fi

DAG_POD="$(kc -n "$NS" get pods -l component=dag-processor -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [ -z "$DAG_POD" ]; then
  echo "spike-airflow-assets: UNVERIFIED (no dag-processor pod in $NS)"
  exit 3
fi

# --- A1: the image ---------------------------------------------------------------------------
a1() {
  local version imported
  version="$(kc -n "$NS" exec "deploy/$APISERVER" -c api-server -- airflow version 2>/dev/null \
    | tail -1 | tr -d '[:space:]' || true)"
  [ -n "$version" ] || version="$(jq -r '.version // "-"' "$tmp/version.json" 2>/dev/null || printf '-')"
  imported="$(kc -n "$NS" exec "deploy/$APISERVER" -c api-server -- python -c \
    'from airflow.sdk import DAG, Asset; print("ok")' 2>/dev/null | tail -1 | tr -d '[:space:]' || true)"
  row A1 version="$version" asset_import="${imported:-failed}"
}

# --- A7: Nova's user, before anything is posted ----------------------------------------------
a7() {
  local code users_code roles create
  users_code="$(api GET /api/v2/users "$tmp/a7-users.json")"
  cat > "$tmp/a7.py" <<PY
from airflow.utils.session import create_session
from sqlalchemy import text

USER = "${USER_NAME}"
with create_session() as session:
    roles = [r[0] for r in session.execute(
        text("select r.name from ab_user_role ur join ab_user u on u.id = ur.user_id "
             "join ab_role r on r.id = ur.role_id where u.username = :u order by r.name"),
        {"u": USER},
    ).fetchall()]
    create = [r[0] for r in session.execute(
        text("select distinct r.name from ab_permission_view_role pvr "
             "join ab_role r on r.id = pvr.role_id "
             "join ab_permission_view pv on pv.id = pvr.permission_view_id "
             "join ab_permission p on p.id = pv.permission_id "
             "join ab_view_menu v on v.id = pv.view_menu_id "
             "where p.name = 'can_create' and v.name = 'Assets' order by r.name"),
    ).fetchall()]
print("roles=" + (",".join(roles) or "(none)"))
print("can_create_assets=" + ("yes" if set(roles) & set(create) else "no"))
print("create_by_role=" + (",".join(create) or "(none)"))
PY
  roles="$(kc -n "$NS" exec "deploy/$APISERVER" -c api-server -i -- python - < "$tmp/a7.py" 2>/dev/null \
    | grep -E '^roles=' | head -1 | cut -d= -f2- || true)"
  create="$(kc -n "$NS" exec "deploy/$APISERVER" -c api-server -i -- python - < "$tmp/a7.py" 2>/dev/null \
    | grep -E '^can_create_assets=' | head -1 | cut -d= -f2- || true)"
  ROLE="${roles%%,*}"
  [ "$create" = yes ] && ROLE_CREATE=yes || ROLE_CREATE=no
  row A7 signin="$signin_code" "user=$USER_NAME" "roles=${roles:-unknown}" \
    "can_create_assets=$ROLE_CREATE" "users_route=$users_code"
}

# --- A2: the `all` DAG -----------------------------------------------------------------------
write_dags() {
  cat > "$tmp/$ALL_DAG.py" <<'PY'
from datetime import datetime

from airflow.sdk import DAG, Asset, task

with DAG(
    dag_id="spike_assets_all",
    start_date=datetime(2026, 1, 1),
    schedule=[Asset(name="spike://asset-a"), Asset(name="spike://asset-b")],
    catchup=False,
    tags=["spike"],
):
    @task
    def note():
        print("spike_assets_all ran")

    note()
PY
  cat > "$tmp/$ANY_DAG.py" <<'PY'
from datetime import datetime

from airflow.sdk import DAG, Asset, task

with DAG(
    dag_id="spike_assets_any",
    start_date=datetime(2026, 1, 1),
    schedule=Asset(name="spike://asset-a") | Asset(name="spike://asset-b"),
    catchup=False,
    tags=["spike"],
):
    @task
    def note():
        print("spike_assets_any ran")

    note()
PY
}

appear() { # dag -> 0 when GET /api/v2/dags/<dag> answers 200
  local code
  code="$(api GET "/api/v2/dags/$1" "$tmp/dag-$1.json")"
  [ "$code" = 200 ]
}

a2() {
  local started waited appeared_all=no appeared_any=no paused_all="" paused_any=""
  write_dags
  for f in $DAG_FILES; do
    kc -n "$NS" cp "$tmp/$f" "$DAG_POD:/opt/airflow/dags/$f" -c dag-processor >/dev/null || {
      row A2 copy "file=$f" "status=failed"; return 0; }
  done
  DAGS_COPIED=yes
  # One shared deadline: the bundle re-lists its folder every five minutes, so both files appear
  # in the same refresh (measured 2026-11-27: up to 291 s after the copy).
  started="$(now_s)"; waited=0
  while :; do
    appear "$ALL_DAG" && appeared_all=yes
    appear "$ANY_DAG" && appeared_any=yes
    [ "$appeared_all" = yes ] && [ "$appeared_any" = yes ] && break
    waited=$(( $(now_s) - started ))
    [ "$waited" -ge "$TIMEOUT" ] && break
    sleep 5
  done
  row A2 dag="$ALL_DAG" "appeared=$appeared_all" "appeared_after_s=$waited" "copy_source=kubectl_cp"
  row A2 dag="$ANY_DAG" "appeared=$appeared_any" "appeared_after_s=$waited"
  [ "$appeared_all" = yes ] || return 0

  # Airflow creates a DAG paused; an asset event never starts a paused DAG, so unpause the two
  # scratch DAGs by hand (they are deleted again at the end).
  paused_all="$(jq -r '.is_paused | tostring' "$tmp/dag-$ALL_DAG.json" 2>/dev/null || printf '-')"
  printf '{"is_paused":false}' > "$tmp/unpause.json"
  api PATCH "/api/v2/dags/$ALL_DAG" "$tmp/p-unpause-all.json" "$tmp/unpause.json" >/dev/null || true
  if [ "$appeared_any" = yes ]; then
    paused_any="$(jq -r '.is_paused | tostring' "$tmp/dag-$ANY_DAG.json" 2>/dev/null || printf '-')"
    api PATCH "/api/v2/dags/$ANY_DAG" "$tmp/p-unpause-any.json" "$tmp/unpause.json" >/dev/null || true
  fi
  row A2 unpause "paused_at_creation_all=$paused_all" "paused_at_creation_any=${paused_any:--}" \
    "is_paused_now=$(jq -r '.is_paused | tostring' "$tmp/p-unpause-all.json" 2>/dev/null || printf '-')"

  # The assets are registered by the dag processor on its next parse: poll briefly for both.
  started="$(now_s)"
  while :; do
    api GET "/api/v2/assets?limit=200" "$tmp/assets.json" >/dev/null || true
    A_ID="$(jq -r --arg a "$A_ASSET" '[.assets[]? | select(.name == $a or .uri == ($a + "/")) | .id][0] // "none"' \
      "$tmp/assets.json" 2>/dev/null || printf none)"
    B_ID="$(jq -r --arg b "$B_ASSET" '[.assets[]? | select(.name == $b or .uri == ($b + "/")) | .id][0] // "none"' \
      "$tmp/assets.json" 2>/dev/null || printf none)"
    [ "$A_ID" != none ] && [ "$B_ID" != none ] && break
    [ $(( $(now_s) - started )) -ge 60 ] && break
    sleep 5
  done
  row A2 assets "asset_a=${A_ID:-none}" "asset_b=${B_ID:-none}" \
    "normalised_uri=$(jq -r --arg a "$A_ASSET" '[.assets[]? | select(.uri | startswith($a)) | .uri][0] // "-"' "$tmp/assets.json" 2>/dev/null || printf '-')"
}

# --- events ----------------------------------------------------------------------------------
post_event() { # asset-id out -> status; the body is the ticket's CreateAssetEventsBody
  local id="$1" out="$2"
  jq -n --argjson id "$id" '{asset_id:$id}' > "$tmp/event.json"
  api POST /api/v2/assets/events "$out" "$tmp/event.json"
}

# The API server caches a user's permission set: a permission granted after sign-in shows only a
# while later (measured 2026-11-27: the first POST after the grant still answered 403, one a
# minute later answered 200). A 403 posts nothing, so retrying it cannot double an event.
post_event_until() { # asset-id out -> sets POST_STATUS and POST_WAITED
  POST_STATUS="$(post_event "$1" "$2")"
  POST_WAITED=0
  while [ "$POST_STATUS" = 403 ] && [ "$GRANTED" = yes ] \
    && [ "$POST_WAITED" -lt "$PERMISSION_WAIT" ]; do
    sleep 5
    POST_WAITED=$(( POST_WAITED + 5 ))
    POST_STATUS="$(post_event "$1" "$2")"
  done
}

ensure_event_permission() { # A3 out-file-of-the-refusal-status -> prints the grant as a row
  [ "$ROLE_CREATE" = yes ] && return 0
  [ -n "$ROLE" ] || return 1
  local out
  out="$(kc -n "$NS" exec "deploy/$APISERVER" -c api-server -- \
    airflow roles add-perms "$ROLE" -a can_create -r Assets 2>&1 | tail -1 || true)"
  case "$out" in Added*) GRANTED=yes ;; esac
  [ "$GRANTED" = yes ] || return 1
  # The token carries the permissions it was signed with: sign in again.
  signin
  [ -n "$TOKEN" ] || return 1
  return 0
}

runs_json() { # dag out — every run of the DAG
  api GET "/api/v2/dags/$1/dagRuns?limit=1000" "$2"
}

run_count() { jq -r '(.total_entries // ([.dag_runs[]?] | length)) | tostring' "$1" 2>/dev/null || printf 0; }

# The newest run by `run_after` (the list order is not guaranteed).
latest_run() { jq -c '.dag_runs | sort_by(.run_after // "") | last // {}' "$1" 2>/dev/null || printf '{}'; }

# A3: one event for asset-a. `all` must not run on one asset.
a3() {
  local status post_epoch waited found=no qd delivered=no qwait=0 absent_wait=0
  post_epoch="$(now_s)"
  status="$(post_event "$A_ID" "$tmp/a3.json")"
  if [ "$status" != 200 ] && [ "$status" != 201 ]; then
    row A3 post "asset_id=$A_ID" "name=$A_ASSET" "status=$status" "refused=yes" \
      "body=$(body "$tmp/a3.json")"
    if ensure_event_permission; then
      row A3 grant "role=$ROLE" "permission=can_create/Assets" "status=added" "token=renewed"
      post_epoch="$(now_s)"
      post_event_until "$A_ID" "$tmp/a3.json"
      status="$POST_STATUS"
      row A3 post_after_grant "status=$status" "applied_after_s=$POST_WAITED" \
        "body=$(body "$tmp/a3.json")"
    else
      row A3 grant "role=$ROLE" "permission=can_create/Assets" "status=failed"
      return 0
    fi
  else
    row A3 post "asset_id=$A_ID" "name=$A_ASSET" "status=$status" "refused=no"
  fi
  if [ "$status" = 200 ] || [ "$status" = 201 ]; then delivered=yes; fi

  waited=0
  while :; do
    runs_json "$ALL_DAG" "$tmp/a3-runs.json" >/dev/null || true
    [ "$(run_count "$tmp/a3-runs.json")" -gt 0 ] && { found=yes; break; }
    waited=$(( $(now_s) - post_epoch ))
    [ "$waited" -ge "$ABSENCE_WAIT" ] && break
    sleep 5
  done
  absent_wait="$waited"
  # The queued event the scheduler has not consumed yet: wait for it (a few seconds) so a 0 is
  # "the event was delivered and consumed", not "the POST never landed".
  qd=0
  if [ "$delivered" = yes ]; then
    while :; do
      api GET "/api/v2/assets/$A_ID/queuedEvents" "$tmp/a3-queued.json" >/dev/null || true
      qd="$(jq -r '.total_entries // 0' "$tmp/a3-queued.json" 2>/dev/null || printf 0)"
      [ "$qd" -gt 0 ] && break
      qwait=$(( qwait + 5 ))
      [ "$qwait" -ge "$ABSENCE_WAIT" ] && break
      sleep 5
    done
  fi
  A3_DELIVERED="$delivered"; A3_RAN="$found"
  row A3 all_runs "dag=$ALL_DAG" "ran=$found" "waited_s=$absent_wait" "event_delivered=$delivered" \
    "queued=$qd" "queued_wait_s=$qwait" \
    "one_asset_is_not_enough=$([ "$delivered" = yes ] && [ "$found" = no ] && echo yes || echo not_measured)"
  jq -r '.dag_runs[]? | "A3 run dag_run_id=\(.dag_run_id) run_type=\(.run_type) logical_date=\(.logical_date) state=\(.state)"' \
    "$tmp/a3-runs.json" 2>/dev/null | while read -r line; do rows[A3]=1; printf '%s\n' "$line"; done
}

# A4: one event for asset-b — the `all` DAG now has both.
a4() {
  local status post_epoch waited found=no rid run_type logical state delay
  post_event_until "$B_ID" "$tmp/a4.json"
  status="$POST_STATUS"
  post_epoch="$(now_s)"
  row A4 post "asset_id=$B_ID" "name=$B_ASSET" "status=$status" "applied_after_s=$POST_WAITED" \
    "body=$(body "$tmp/a4.json")"
  waited=0
  while :; do
    runs_json "$ALL_DAG" "$tmp/a4-runs.json" >/dev/null || true
    [ "$(run_count "$tmp/a4-runs.json")" -gt 0 ] && { found=yes; break; }
    waited=$(( $(now_s) - post_epoch ))
    [ "$waited" -ge "$TIMEOUT" ] && break
    sleep 5
  done
  row A4 all_run "dag=$ALL_DAG" "ran=$found" "waited_s=$waited"
  A4_RAN="$found"
  [ "$found" = yes ] || return 0
  latest_run "$tmp/a4-runs.json" > "$tmp/a4-run.json"
  rid="$(jq -r '.dag_run_id // "-"' "$tmp/a4-run.json" 2>/dev/null || printf '-')"
  run_type="$(jq -r '.run_type // "-"' "$tmp/a4-run.json" 2>/dev/null || printf '-')"
  logical="$(jq -r '.logical_date // "null"' "$tmp/a4-run.json" 2>/dev/null || printf 'null')"
  state="$(jq -r '.state // "-"' "$tmp/a4-run.json" 2>/dev/null || printf '-')"
  delay="$(jq -r --argjson p "$post_epoch" '
    (.start_date // .queued_at // empty) as $d
    | if $d == null then "-"
      else (($d | if test("\\.[0-9]+Z$") then sub("\\.[0-9]+Z$"; "Z") else . end) | fromdateiso8601) - $p
      end' "$tmp/a4-run.json" 2>/dev/null || printf '-')"
  row A4 run "dag_run_id=$rid" "run_type=$run_type" "logical_date=$logical" "state=$state" \
    "delay_s=$delay" "run_after=$(jq -r '.run_after // "-"' "$tmp/a4-run.json" 2>/dev/null || printf '-')"
}

# A5: two more events for asset-a while the A4 run is queued or running.
a5() {
  local c1 c2 runs_before runs_after waited queued posted=0
  runs_json "$ALL_DAG" "$tmp/a5-before.json" >/dev/null || true
  runs_before="$(run_count "$tmp/a5-before.json")"
  post_event_until "$A_ID" "$tmp/a5-1.json"; c1="$POST_STATUS"
  post_event_until "$A_ID" "$tmp/a5-2.json"; c2="$POST_STATUS"
  case "$c1" in 200|201) posted=$(( posted + 1 ));; esac
  case "$c2" in 200|201) posted=$(( posted + 1 ));; esac
  waited=0
  while :; do
    runs_json "$ALL_DAG" "$tmp/a5-runs.json" >/dev/null || true
    runs_after="$(run_count "$tmp/a5-runs.json")"
    [ "$runs_after" -gt "$runs_before" ] && break
    waited=$(( waited + 5 ))
    [ "$waited" -ge "$ABSENCE_WAIT" ] && break
    sleep 5
  done
  api GET "/api/v2/assets/$A_ID/queuedEvents" "$tmp/a5-queued.json" >/dev/null || true
  queued="$(jq -r '.total_entries // 0' "$tmp/a5-queued.json" 2>/dev/null || printf 0)"
  A5_POSTED="$posted"; A5_QUEUED="$queued"
  row A5 post "asset_id=$A_ID" "status_1=$c1" "status_2=$c2" "events_delivered=$posted" \
    "runs_before=$runs_before" "runs_after=${runs_after:-?}" "waited_s=$waited"
  row A5 queued "asset_id=$A_ID" "queued_events=$queued" \
    "two_events_one_slot=$([ "$posted" -ge 2 ] && [ "$queued" -ge 1 ] && [ "$queued" -le 1 ] && echo yes || echo no)" \
    "queued_body=$(body "$tmp/a5-queued.json")"
}

# A6: the `any` DAG runs on one asset alone.
a6() {
  local status post_epoch before after waited found=no rid state logical delay
  runs_json "$ANY_DAG" "$tmp/a6-before.json" >/dev/null || true
  before="$(run_count "$tmp/a6-before.json")"
  post_event_until "$B_ID" "$tmp/a6.json"
  status="$POST_STATUS"
  post_epoch="$(now_s)"
  waited=0
  while :; do
    runs_json "$ANY_DAG" "$tmp/a6-runs.json" >/dev/null || true
    after="$(run_count "$tmp/a6-runs.json")"
    [ "$after" -gt "$before" ] && { found=yes; break; }
    waited=$(( $(now_s) - post_epoch ))
    [ "$waited" -ge "$TIMEOUT" ] && break
    sleep 5
  done
  row A6 post "asset_id=$B_ID" "name=$B_ASSET" "status=$status" "runs_before=$before" \
    "runs_after=${after:-?}" "ran=$found" "waited_s=$waited"
  A6_RAN="$found"
  [ "$found" = yes ] || return 0
  latest_run "$tmp/a6-runs.json" > "$tmp/a6-run.json"
  rid="$(jq -r '.dag_run_id // "-"' "$tmp/a6-run.json" 2>/dev/null || printf '-')"
  state="$(jq -r '.state // "-"' "$tmp/a6-run.json" 2>/dev/null || printf '-')"
  logical="$(jq -r '.logical_date // "null"' "$tmp/a6-run.json" 2>/dev/null || printf 'null')"
  delay="$(jq -r --argjson p "$post_epoch" '
    (.start_date // .queued_at // empty) as $d
    | if $d == null then "-"
      else (($d | if test("\\.[0-9]+Z$") then sub("\\.[0-9]+Z$"; "Z") else . end) | fromdateiso8601) - $p
      end' "$tmp/a6-run.json" 2>/dev/null || printf '-')"
  row A6 run "dag_run_id=$rid" "logical_date=$logical" "state=$state" "delay_s=$delay" \
    "run_type=$(jq -r '.run_type // "-"' "$tmp/a6-run.json" 2>/dev/null || printf '-')" \
    "one_asset_is_enough=yes"
}

# --- cleanup of the Airflow side, in the main flow so its line is printed ---------------------
delete_dags() {
  local d
  for d in "$ALL_DAG" "$ANY_DAG"; do
    kc -n "$NS" exec "deploy/$APISERVER" -c api-server -- airflow dags delete -y "$d" \
      >/dev/null 2>&1 || true
  done
  DAG_DELETED=yes
}

cleanup_rows() {
  local code_all code_any revoked=no
  [ "$DAG_POD" != "" ] && kc -n "$NS" exec "$DAG_POD" -c dag-processor -- rm -f \
    /opt/airflow/dags/"$ALL_DAG".py /opt/airflow/dags/"$ANY_DAG".py >/dev/null 2>&1 || true
  DAGS_COPIED=no
  delete_dags
  code_all="$(api GET "/api/v2/dags/$ALL_DAG" "$tmp/clean-all.json")"
  code_any="$(api GET "/api/v2/dags/$ANY_DAG" "$tmp/clean-any.json")"
  if [ "$GRANTED" = yes ] && [ -n "$ROLE" ]; then
    kc -n "$NS" exec "deploy/$APISERVER" -c api-server -- \
      airflow roles del-perms "$ROLE" -a can_create -r Assets >/dev/null 2>&1 || true
    GRANTED=no
    revoked=yes
  fi
  row cleanup "files=removed" "dag_all=$code_all" "dag_any=$code_any" \
    "permission_revoked=$revoked" "port_forward=killed"
}

# --- rows ------------------------------------------------------------------------------------
a1
a7
a2
if [ -z "${A_ID:-}" ] || [ "$A_ID" = none ] || [ -z "${B_ID:-}" ] || [ "$B_ID" = none ]; then
  echo "spike-airflow-assets: UNVERIFIED (the DAGs did not register their assets: asset_a=${A_ID:-none} asset_b=${B_ID:-none})"
  exit 3
fi
a3
a4
a5
a6

# --- C1: the conclusions ---------------------------------------------------------------------
# One `C1` row per question of #66, each built from the rows above (never from the design).
row C1 "mode_all=schedule=[Asset(a), Asset(b)] is the all form: event a delivered=$A3_DELIVERED ran_the_dag=$A3_RAN (A3), the second asset ran it=$A4_RAN (A4)"
row C1 "mode_any=schedule=Asset(a) | Asset(b) is the any form: one event for asset-b alone ran_the_dag=$A6_RAN (A6)"
row C1 "logical_date=an asset-triggered run carries logical_date=null; the event's time is run_after and the id is asset_triggered__<run_after>_<random> (A4, A6), so the Runs page must show run_after, not logical_date"
row C1 "credential=nova-api's role ${ROLE:-unknown} can_create_assets=$ROLE_CREATE (A7); POST /api/v2/assets/events is checked as can_create on Assets (A3 first status), so a credential that posts events needs that permission added to its role — Nova may trigger DAGs today but not post an asset event"
row C1 "collapse=two events for one asset delivered=$A5_POSTED while a run was pending left queued_events=$A5_QUEUED (A5)"
row C1 "recommendation=for #66: map mode: all to schedule=[…] and mode: any to Asset(…) | Asset(…); do not read logical_date from an asset-triggered run; grant the loader's credential can_create on Assets before it raises events"

cleanup_rows

# --- verdict ---------------------------------------------------------------------------------
for n in A1 A2 A3 A4 A5 A6 A7; do
  [ -n "${rows[$n]:-}" ] || missing="${missing:+$missing }$n"
done
if [ -n "$missing" ]; then
  echo "spike-airflow-assets: FAIL (missing rows: $missing)"
  exit 1
fi
echo "spike-airflow-assets: PASS (7 rows)"
