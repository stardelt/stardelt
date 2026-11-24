#!/usr/bin/env bash
# The interactive path's latency, measured as the visitor `demo` (stardelt/stardelt#1515, #53).
#
#   make perf-smoke              (STARDELT_NOVA_URL=…, ISSUER=… to override)
#   bash scripts/perf-smoke.sh
#
# The number a person feels when they click: sign in, `POST /api/statements`, poll, cancel.
# It records **client wall-clock** through Nova's statement API. `StatementView`
# (`nova/openapi/nova.json`) carries no engine timing yet, so the JSON says so in `note`;
# engine stats are a later slot of #53.
#
# One JSON per run at `~/.cache/stardelt-perf/<YYYY-MM-DDTHH:MM:SSZ>.json`, plus a table on
# stdout. Nothing is written to the cluster: every statement is the person's own and is
# cancelled before the next, so the slot is free again.
#
# Five fixed statements that need no data and no catalog: the lab has no `tpch`/`tpcds`
# catalog (`SHOW CATALOGS` as `demo` answers bronze, gold, playground, silver, system,
# measured 2026-11-24 03:06Z). `scan` reads the demo's bronze table; `cpu`, `shuffle` and
# `nodepage` need no table at all; `metadata` describes it. `sequence(1, 1000000)` is refused
# (`result of sequence function must not have more than 10000 entries`), so the cross join
# below is what produces 10^6 rows.
#
# The password is `demo` and it is PUBLIC ON PURPOSE: `scripts/public-demo.sh` creates that
# person and prints it, so it protects nothing. The password and the token live in two
# `mktemp` files (0600, removed on every exit) and nothing here prints the token.
#
# PASS (exit 0) = every run of every statement reached `SUCCEEDED`. FAIL (exit 1) = the first
# run that did not, named in the last line; a statement that does not reach `SUCCEEDED` is a
# failure of the run, not a data point.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"
NOVA="${NOVA%/}"
ISSUER="${ISSUER:-https://id.alpha.dev.stardelt.io/realms/stardelt}"
ISSUER="${ISSUER%/}"
PERSON="${PERF_PERSON:-demo}"
REPEATS="${PERF_REPEATS:-3}"
OUT_DIR="$HOME/.cache/stardelt-perf"

# id|sql|expected rows. The expected count is a record check, not a gate: a statement that
# answers another number of rows is printed as a warning and still measured.
STATEMENTS=(
  'scan|SELECT count(*) FROM bronze.shop.customers|1'
  'cpu|SELECT count(*), sum(cast(mod(a.x * b.y, 1000000) AS bigint)) FROM UNNEST(sequence(1, 1000)) AS a(x) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(y)|1'
  'shuffle|SELECT count(*) FROM (SELECT mod(a.x * b.y, 100000) AS k, count(*) AS c FROM UNNEST(sequence(1, 1000)) AS a(x) CROSS JOIN UNNEST(sequence(1, 1000)) AS b(y) GROUP BY 1) s|1'
  'nodepage|SELECT count(*) FROM system.runtime.nodes|1'
  'metadata|DESCRIBE bronze.shop.customers|5'
)

PASSWORD_FILE="$(mktemp)"
TOKEN_FILE="$(mktemp)"
BODY_FILE="$(mktemp)"
RESULTS_FILE="$(mktemp)"
chmod 600 "$PASSWORD_FILE" "$TOKEN_FILE" "$BODY_FILE" "$RESULTS_FILE"
printf 'demo\n' > "$PASSWORD_FILE"

cleanup() { rm -f "$PASSWORD_FILE" "$TOKEN_FILE" "$BODY_FILE" "$RESULTS_FILE"; }
trap cleanup EXIT
trap 'exit 130' INT TERM

FAILURE=""

for tool in curl python3 date; do
  command -v "$tool" >/dev/null 2>&1 || { echo "perf-smoke: FAIL $tool not found"; exit 1; }
done

# A curl that does not answer is a failed run, never a silent empty body.
api_get() { # api_get <path>
  curl -fsS -H "Authorization: Bearer $(cat "$TOKEN_FILE")" "$NOVA$1" >"$BODY_FILE"
}

# The statement is freed on every path, so the person's slot is not left occupied.
cancel() { # cancel <statement id>
  curl -fsS -X DELETE -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
    "$NOVA/api/statements/$1" >/dev/null
}

view_field() { # view_field <field>: one scalar out of BODY_FILE's StatementView, "" when absent
  python3 - "$BODY_FILE" "$1" <<'PY'
import json, sys

try:
    value = json.load(open(sys.argv[1])).get(sys.argv[2])
except (ValueError, OSError):
    value = None
print("" if value is None else value)
PY
}

view_rows() { # view_rows: the number of elements in BODY_FILE's `rows` list
  python3 - "$BODY_FILE" <<'PY'
import json, sys

try:
    rows = json.load(open(sys.argv[1])).get("rows")
except (ValueError, OSError):
    rows = None
print(len(rows) if isinstance(rows, list) else 0)
PY
}

# One run of one statement. Sets MS (milliseconds of the submit plus every poll, the cancel
# excluded), ROWS and STATE; returns 1 with MEASURE_ERROR set when the run did not succeed.
MS=""
ROWS=0
STATE="?"
MEASURE_ERROR=""
measure() { # measure <sql>
  local sql="$1" id state start end body
  MS=""
  ROWS=0
  STATE="?"
  MEASURE_ERROR=""

  body="$(python3 -c 'import json,sys; print(json.dumps({"sql": sys.argv[1], "waitSeconds": 60}))' "$sql")"
  start="$(date +%s%N)"
  if ! curl -fsS -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
      -X POST -H 'Content-Type: application/json' -d "$body" \
      "$NOVA/api/statements" >"$BODY_FILE"; then
    MEASURE_ERROR="POST /api/statements did not answer"
    return 1
  fi
  id="$(view_field statementId)"
  if [ -z "$id" ]; then
    MEASURE_ERROR="the submit answer carries no statementId"
    return 1
  fi
  state="$(view_field state)"

  # Twelve polls of ten seconds: PENDING and RUNNING are not failures yet, a statement that
  # is still one of them after two minutes is.
  for _ in $(seq 1 12); do
    case "$state" in
      PENDING|RUNNING) ;;
      *) break ;;
    esac
    if ! api_get "/api/statements/$id?waitSeconds=10"; then
      cancel "$id" || true
      MEASURE_ERROR="GET /api/statements/$id did not answer (statement $id)"
      return 1
    fi
    state="$(view_field state)"
  done
  end="$(date +%s%N)"

  MS=$(( (end - start) / 1000000 ))
  ROWS="$(view_rows)"
  STATE="${state:-?}"

  if ! cancel "$id"; then
    MEASURE_ERROR="cannot cancel statement $id"
    return 1
  fi
  case "$STATE" in
    SUCCEEDED) return 0 ;;
    PENDING|RUNNING)
      MEASURE_ERROR="statement $id is still $STATE after 12 polls"
      return 1
      ;;
    *)
      MEASURE_ERROR="statement $id is $STATE, not SUCCEEDED"
      return 1
      ;;
  esac
}

# --- sign in ------------------------------------------------------------------------------
if ! out="$(ISSUER="$ISSUER" USER="$PERSON" PASSWORD_FILE="$PASSWORD_FILE" TOKEN_FILE="$TOKEN_FILE" \
      python3 scripts/mint-user-token.py 2>&1)"; then
  echo "perf-smoke: FAIL cannot mint a token for $PERSON: $(printf '%s\n' "$out" | head -n 1)"
  exit 1
fi

# --- measure ------------------------------------------------------------------------------
# One tab-separated line per statement: id, state, rows, the run values, the sql.
: > "$RESULTS_FILE"
for spec in "${STATEMENTS[@]}"; do
  ID="${spec%%|*}"
  rest="${spec#*|}"
  SQL="${rest%|*}"
  EXPECTED="${rest##*|}"
  MS_LIST=""
  ROWS_LAST=0
  STATE_LAST="?"

  for repeat in $(seq 1 "$REPEATS"); do
    [ "$repeat" -eq 1 ] || sleep 1
    if ! measure "$SQL"; then
      FAILURE="$ID run $repeat: $MEASURE_ERROR"
      ROWS_LAST="$ROWS"
      STATE_LAST="$STATE"
      [ -z "$MS" ] || MS_LIST="${MS_LIST:+$MS_LIST,}$MS"
      printf '%s\t%s\t%s\t%s\t%s\n' "$ID" "$STATE_LAST" "$ROWS_LAST" "$MS_LIST" "$SQL" >> "$RESULTS_FILE"
      break 2
    fi
    ROWS_LAST="$ROWS"
    STATE_LAST="$STATE"
    MS_LIST="${MS_LIST:+$MS_LIST,}$MS"
    if [ "$ROWS" != "$EXPECTED" ]; then
      echo "perf-smoke: WARN $ID answered $ROWS row(s), expected $EXPECTED" >&2
    fi
  done
  printf '%s\t%s\t%s\t%s\t%s\n' "$ID" "$STATE_LAST" "$ROWS_LAST" "$MS_LIST" "$SQL" >> "$RESULTS_FILE"
done

# --- write the baseline --------------------------------------------------------------------
mkdir -p "$OUT_DIR"
python3 - "$RESULTS_FILE" "$OUT_DIR" "$NOVA" "$PERSON" "$REPEATS" <<'PY'
import datetime
import json
import os
import sys

results, out_dir, nova, person, repeats = sys.argv[1:6]
NOTE = "client wall-clock through Nova's statement API; no engine stats in StatementView yet"


def nearest_rank(values, percent):
    """The nearest-rank percentile: the ceil(percent * n / 100)-th value of the sorted list."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, -(-percent * len(ordered) // 100))
    return ordered[min(rank, len(ordered)) - 1]


statements = []
with open(results) as handle:
    for line in handle:
        line = line.rstrip("\n")
        if not line:
            continue
        statement_id, state, rows, ms_list, sql = line.split("\t")
        runs = [int(value) for value in ms_list.split(",") if value]
        statements.append({
            "id": statement_id,
            "sql": sql,
            "runs": runs,
            "p50": nearest_rank(runs, 50),
            "p95": nearest_rank(runs, 95),
            "rows": int(rows),
            "state": state,
        })

timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
path = os.path.join(out_dir, timestamp + ".json")
suffix = 2
while os.path.exists(path):
    path = os.path.join(out_dir, "%s-%d.json" % (timestamp, suffix))
    suffix += 1

document = {
    "timestamp": timestamp,
    "nova": nova,
    "person": person,
    "repeats": int(repeats),
    "statements": statements,
    "note": NOTE,
}
with open(path, "w") as handle:
    json.dump(document, handle, indent=2)
    handle.write("\n")

for statement in statements:
    print("%-10s runs=%d  p50=%sms  p95=%sms  rows=%d" % (
        statement["id"], len(statement["runs"]),
        statement["p50"], statement["p95"], statement["rows"]))
print("perf-smoke: baseline written to %s" % path)
PY

if [ -n "$FAILURE" ]; then
  echo "perf-smoke: FAIL $FAILURE"
  exit 1
fi
echo "perf-smoke: PASS ($REPEATS repeat(s) per statement as $PERSON)"
