#!/usr/bin/env bash
# Spike for stardelt/stardelt#2010 (#32, M-10): the materialized-view spike (#1986) measured that
# Trino 484 refuses `CREATE MATERIALIZED VIEW` on the Iceberg REST catalog the platform ships
# (`NOT_SUPPORTED`). #32's acceleration decision therefore needs the second candidate measured: a
# table rebuilt by a scheduled statement — `CREATE OR REPLACE TABLE … AS SELECT`, or `DELETE` +
# `INSERT` — from a Pipeline `sql` task, which runs as `pipeline-<group>`.
#
#   cd platform && make spike-refresh-table
#
# The questions #32 needs before it is sliced: what a rebuild costs, whether a reader ever sees a
# half-built table, and which grants the person and the principal need.
#
# Seven lines, printed from the running cluster (the `<…>` values are what the spike measures):
#   R1 setup       the scratch schema, the source table and its three rows, two days
#   R2 first build `CREATE TABLE … AS SELECT` as `demo`: rows, the aggregate, wall-clock ms
#   R3 stale       one more source row the built table does not see: both numbers
#   R4 replacement `CREATE OR REPLACE TABLE … AS SELECT` as `demo`: `refreshed` and the new
#                  aggregate, or the engine's own sentence when it refuses
#   R5 delete+insert the fallback when R4 is refused: `DELETE` + `INSERT` timed, or the sentence
#   R6 pipeline    the same `CREATE OR REPLACE` as the demo group's pipeline principal
#                  (`sp-pipeline-demo`), which is what a scheduled `sql` task's `runAs` is
#   CLEANUP        daily, events and the schema are gone: `playground` is as it was
#
# Every statement a person makes goes through the catalog as `demo` (bearer token), never as a
# service account: the grants are part of the answer. The only cluster change is the scratch
# schema, and the trap drops it on every exit path. `playground` grants `write` and `create` to
# `demo`/`visitors` but no `delete`, so the three drops as `demo` can be refused — then the CLEANUP
# line names that sentence and the demo group's pipeline principal (`sp-pipeline-demo`) drops the
# tables and the namespace through the Lakekeeper catalog API. The line says which path cleaned up.
#
# No token, password or Secret value is printed: only row counts, status codes and the engine's
# sentences.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
CATALOG=playground
SCHEMA=spike_refresh
GROUP=demo
DEMO_PASSWORD="${DEMO_PASSWORD:-demo}"
TRINO_URL="${TRINO_URL:-https://trino.alpha.dev.stardelt.io}"
PRINCIPAL_SECRET="${PRINCIPAL_SECRET:-sp-pipeline-$GROUP}"
LK_PORT="${LK_PORT:-18188}"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/spike-refresh.XXXXXX")"
cleaned=no

for tool in kubectl python3 curl jq; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

# --- Trino statement API -----------------------------------------------------------------
# One statement, rows as JSON: `{"code":200,"rows":[[..]],"error":null,"stats":{...}}`. The
# `trino.py`/`trino()`/`mint_demo()` block is the one in `spike-lakekeeper-sts-writer.sh`, with
# the statement's stats added so M6 can say how much of the source a refresh read.
cat > "$tmp/trino.py" <<'PY'
import base64, json, sys, urllib.error, urllib.request

url, mode, secret_file, sql = sys.argv[1:5]
headers = {"Content-Type": "text/plain"}
if mode == "basic":
    password = open(secret_file).read().strip()
    headers["Authorization"] = "Basic " + base64.b64encode(("superset:" + password).encode()).decode()
else:
    headers["Authorization"] = "Bearer " + open(secret_file).read().strip()


def call(endpoint, data=None):
    request = urllib.request.Request(endpoint, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, {"error": {"message": error.read().decode()[:300]}}


code, body = call(url.rstrip("/") + "/v1/statement", sql.encode())
rows = list(body.get("data") or [])
failure = body.get("error")
while body.get("nextUri"):
    code, body = call(body["nextUri"])
    rows.extend(body.get("data") or [])
    failure = failure or body.get("error")
stats = body.get("stats") or {}
print(json.dumps({"code": code, "rows": rows, "error": failure,
                  "stats": {key: stats.get(key) for key in (
                      "processedInputPositions", "processedInputDataSize",
                      "elapsedTime", "state")}}))
PY

trino() { # <basic|bearer> <secret-file> <sql> -> json line
  python3 "$tmp/trino.py" "$TRINO_URL" "$1" "$2" "$3"
}

mint_demo() {
  local issuer
  issuer="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}')"
  ISSUER="$issuer" USER=demo PASSWORD_FILE="$tmp/password" TOKEN_FILE="$tmp/demo-token" \
    python3 scripts/mint-user-token.py >/dev/null
}

code_of() { python3 -c 'import json,sys; print(json.loads(sys.argv[1]).get("code", 0))' "$1"; }

# A bearer statement, re-minted once if the access token outlived Keycloak's lifespan.
trino_bearer() { # <sql> -> json line
  local out
  out="$(trino bearer "$tmp/demo-token" "$1")"
  if [ "$(code_of "$out")" = "401" ] && mint_demo 2>/dev/null; then
    out="$(trino bearer "$tmp/demo-token" "$1")"
  fi
  printf '%s' "$out"
}

# The engine's sentence for a failed statement: `errorName: message`, one line.
sentence() { # <json> -> one line
  python3 - "$1" <<'PY'
import json, sys
body = json.loads(sys.argv[1])
error = body.get("error")
if not error:
    print("")
    raise SystemExit
if isinstance(error, dict):
    name = error.get("errorName") or error.get("type") or "ERROR"
    message = str(error.get("message") or error).replace("\n", " ")
else:
    name, message = "ERROR", str(error)
print(("%s: %s" % (name, message))[:400])
PY
}

failed() { [ -n "$(sentence "$1")" ]; }

# Wall-clock milliseconds of one statement: prints the json line, writes the duration to a file.
timed() { # <sql> <duration-file> -> json line
  local start end out
  start="$(date +%s%N)"
  out="$(trino_bearer "$1")"
  end="$(date +%s%N)"
  echo $(( (end - start) / 1000000 )) > "$2"
  printf '%s' "$out"
}

rows_of() { python3 -c 'import json,sys; print(json.dumps(json.loads(sys.argv[1]).get("rows") or []))' "$1"; }

# --- dropping the scratch schema ---------------------------------------------------------
# The demo person holds `write` and `create` on `playground`, not `delete` (measured): the three
# drops can be refused. This is the fallback: the demo group's pipeline principal drops every
# table and the namespace through the Lakekeeper catalog API (`umask 077`, files in $tmp).
drop_schema_privileged() {
  local pid="" port="$LK_PORT" prefix code name ok=yes
  kubectl -n "$NS" port-forward svc/lakekeeper "$port:8181" >/dev/null 2>&1 &
  pid=$!
  for _ in $(seq 1 30); do
    if curl -s -o /dev/null -m 2 "http://127.0.0.1:$port/management/v1/info"; then break; fi
    sleep 1
  done
  kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" -o jsonpath='{.data.client-id}' \
    | base64 -d > "$tmp/priv-client-id" 2>/dev/null || ok=no
  kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" -o jsonpath='{.data.client-secret}' \
    | base64 -d > "$tmp/priv-client-secret" 2>/dev/null || ok=no
  if [ "$ok" = yes ]; then
    local url
    url="$(kubectl -n "$NS" get cm stardelt-identity -o jsonpath='{.data.tokenEndpoint}')"
    if [ -n "$url" ]; then
      curl -fsS -X POST "$url" \
        --data-urlencode grant_type=client_credentials \
        --data-urlencode client_id@"$tmp/priv-client-id" \
        --data-urlencode client_secret@"$tmp/priv-client-secret" -o "$tmp/priv-token.json" 2>/dev/null \
        || ok=no
    else
      ok=no
    fi
  fi
  if [ "$ok" = yes ]; then
    printf 'Authorization: Bearer %s\n' "$(jq -r .access_token "$tmp/priv-token.json")" > "$tmp/priv-auth"
    prefix="$(curl -sS -H @"$tmp/priv-auth" "http://127.0.0.1:$port/catalog/v1/config?warehouse=$CATALOG" \
      | jq -r '.defaults.prefix // empty' 2>/dev/null || true)"
  fi
  if [ -n "${prefix:-}" ]; then
    for name in $(curl -sS -H @"$tmp/priv-auth" \
        "http://127.0.0.1:$port/catalog/v1/$prefix/namespaces/$SCHEMA/tables" \
        | jq -r '.identifiers[]?.name' 2>/dev/null); do
      code="$(curl -sS -H @"$tmp/priv-auth" -X DELETE \
        "http://127.0.0.1:$port/catalog/v1/$prefix/namespaces/$SCHEMA/tables/$name?purgeRequested=true" \
        -o /dev/null -w '%{http_code}' || echo 000)"
      [ "$code" = 204 ] || [ "$code" = 200 ] || ok=no
    done
    code="$(curl -sS -H @"$tmp/priv-auth" -X DELETE \
      "http://127.0.0.1:$port/catalog/v1/$prefix/namespaces/$SCHEMA" -o /dev/null -w '%{http_code}' || echo 000)"
    [ "$code" = 204 ] || [ "$code" = 200 ] || [ "$code" = 404 ] || ok=no
  else
    ok=no
  fi
  [ -z "$pid" ] || kill "$pid" >/dev/null 2>&1 || true
  [ "$ok" = yes ]
}

# --- the demo group's pipeline principal -------------------------------------------------
# `sp-pipeline-demo` is the identity a scheduled `sql` task runs as (`runAs: pipeline-<group>`).
# Its token is minted with the client-credentials grant, the same block `drop_schema_privileged`
# uses; nothing prints it.
mint_principal() {
  local ok=yes url
  kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" -o jsonpath='{.data.client-id}' \
    | base64 -d > "$tmp/priv-client-id" 2>/dev/null || ok=no
  kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" -o jsonpath='{.data.client-secret}' \
    | base64 -d > "$tmp/priv-client-secret" 2>/dev/null || ok=no
  url="$(kubectl -n "$NS" get cm stardelt-identity -o jsonpath='{.data.tokenEndpoint}' 2>/dev/null || true)"
  [ -n "$url" ] || ok=no
  if [ "$ok" = yes ]; then
    curl -fsS -X POST "$url" \
      --data-urlencode grant_type=client_credentials \
      --data-urlencode client_id@"$tmp/priv-client-id" \
      --data-urlencode client_secret@"$tmp/priv-client-secret" -o "$tmp/priv-token.json" 2>/dev/null \
      || ok=no
  fi
  [ "$ok" = yes ] || return 1
  jq -r .access_token "$tmp/priv-token.json" > "$tmp/priv-token"
  chmod 600 "$tmp/priv-token"
}

trino_principal() { # <sql> -> json line
  trino bearer "$tmp/priv-token" "$1"
}

# Wall-clock milliseconds of one statement as the pipeline principal.
timed_principal() { # <sql> <duration-file> -> json line
  local start end out
  start="$(date +%s%N)"
  out="$(trino_principal "$1")"
  end="$(date +%s%N)"
  echo $(( (end - start) / 1000000 )) > "$2"
  printf '%s' "$out"
}

# One scalar cell of a `SELECT`.
scalar() { # <json> -> one line
  python3 -c 'import json,sys; rows=json.loads(sys.argv[1]).get("rows") or []; print(rows[0][0] if rows else "no-row")' "$1"
}

# The three drops as the same person that created them; then the fallback when they are refused.
# Prints the CLEANUP line, once per run (the trap calls it on a failed R row).
drop_all() {
  local sentences="" out sql
  for sql in \
    "DROP TABLE IF EXISTS $CATALOG.$SCHEMA.daily" \
    "DROP TABLE IF EXISTS $CATALOG.$SCHEMA.events" \
    "DROP SCHEMA IF EXISTS $CATALOG.$SCHEMA"
  do
    out="$(trino_bearer "$sql")"
    if failed "$out"; then sentences="${sentences}${sentences:+; }$(sentence "$out")"; fi
  done
  if [ -z "$sentences" ]; then
    echo "CLEANUP: daily, events and $SCHEMA dropped as $GROUP"
  elif drop_schema_privileged; then
    echo "CLEANUP: daily, events and $SCHEMA dropped by $PRINCIPAL_SECRET (as $GROUP: $sentences)"
  else
    echo "CLEANUP: daily, events and $SCHEMA NOT dropped — as $GROUP: $sentences; $PRINCIPAL_SECRET could not drop $CATALOG.$SCHEMA"
  fi
}

# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  local code=$?
  trap - EXIT INT TERM
  set +e
  if [ "$cleaned" = no ] && [ -s "$tmp/demo-token" ]; then drop_all; fi
  rm -rf "$tmp"
  exit "$code"
}
trap cleanup EXIT INT TERM

# --- R1 setup ----------------------------------------------------------------------------
printf '%s' "$DEMO_PASSWORD" > "$tmp/password"
mint_demo

out="$(trino_bearer "CREATE SCHEMA IF NOT EXISTS $CATALOG.$SCHEMA")"
s1="$( [ -z "$(sentence "$out")" ] && echo ok || echo "refused($(sentence "$out"))")"
out="$(trino_bearer "CREATE TABLE IF NOT EXISTS $CATALOG.$SCHEMA.events (day date, n bigint)")"
s2="$( [ -z "$(sentence "$out")" ] && echo ok || echo "refused($(sentence "$out"))")"
out="$(trino_bearer "INSERT INTO $CATALOG.$SCHEMA.events VALUES
  (DATE '2026-01-01', 1), (DATE '2026-01-01', 2), (DATE '2026-01-02', 4)")"
s3="$( [ -z "$(sentence "$out")" ] && echo ok || echo "refused($(sentence "$out"))")"
case "$s1/$s2/$s3" in
  ok/ok/ok) : ;;
  *) echo "R1: setup failed schema=$s1 table=$s2 insert=$s3"; exit 1 ;;
esac
rows="$(scalar "$(trino_bearer "SELECT count(*) FROM $CATALOG.$SCHEMA.events")")"
days="$(scalar "$(trino_bearer "SELECT count(DISTINCT day) FROM $CATALOG.$SCHEMA.events")")"
echo "R1: setup ok — $rows rows, $days days"

# --- R2 first build ----------------------------------------------------------------------
out="$(timed "CREATE TABLE $CATALOG.$SCHEMA.daily AS
  SELECT day, sum(n) AS total FROM $CATALOG.$SCHEMA.events GROUP BY day" "$tmp/r2-ms")"
if failed "$out"; then
  echo "R2: first build refused: $(sentence "$out")"
  exit 1
fi
rows="$(scalar "$(trino_bearer "SELECT count(*) FROM $CATALOG.$SCHEMA.daily")")"
total="$(scalar "$(trino_bearer "SELECT total FROM $CATALOG.$SCHEMA.daily WHERE day = DATE '2026-01-01'")")"
echo "R2: first build ok — $rows rows, 2026-01-01 total $total, $(cat "$tmp/r2-ms") ms"

# --- R3 stale ----------------------------------------------------------------------------
out="$(trino_bearer "INSERT INTO $CATALOG.$SCHEMA.events VALUES (DATE '2026-01-01', 5)")"
if failed "$out"; then echo "R3: stale refused: $(sentence "$out")"; exit 1; fi
source_rows="$(scalar "$(trino_bearer "SELECT count(*) FROM $CATALOG.$SCHEMA.events")")"
source_total="$(scalar "$(trino_bearer "SELECT sum(n) FROM $CATALOG.$SCHEMA.events WHERE day = DATE '2026-01-01'")")"
table_total="$(scalar "$(trino_bearer "SELECT total FROM $CATALOG.$SCHEMA.daily WHERE day = DATE '2026-01-01'")")"
echo "R3: stale — source $source_rows rows, 2026-01-01 total $source_total, table still $table_total"

# --- R4 replacement as the person --------------------------------------------------------
rebuild_sql="CREATE OR REPLACE TABLE $CATALOG.$SCHEMA.daily AS
  SELECT day, sum(n) AS total FROM $CATALOG.$SCHEMA.events GROUP BY day"
out="$(timed "$rebuild_sql" "$tmp/r4-ms")"
r4_ok=no
if failed "$out"; then
  echo "R4: demo CREATE OR REPLACE — refused: $(sentence "$out")"
else
  r4_ok=yes
  total="$(scalar "$(trino_bearer "SELECT total FROM $CATALOG.$SCHEMA.daily WHERE day = DATE '2026-01-01'")")"
  echo "R4: demo CREATE OR REPLACE — refreshed, 2026-01-01 total $total, $(cat "$tmp/r4-ms") ms"
fi

# --- R5 delete + insert, the fallback ----------------------------------------------------
if [ "$r4_ok" = yes ]; then
  echo "R5: demo DELETE + INSERT — not run, R4 refreshed"
else
  out="$(timed "DELETE FROM $CATALOG.$SCHEMA.daily WHERE day = DATE '2026-01-01'" "$tmp/r5-delete-ms")"
  if failed "$out"; then
    echo "R5: demo DELETE + INSERT — refused: $(sentence "$out")"
  else
    out2="$(timed "INSERT INTO $CATALOG.$SCHEMA.daily
      SELECT day, sum(n) AS total FROM $CATALOG.$SCHEMA.events
      WHERE day = DATE '2026-01-01' GROUP BY day" "$tmp/r5-insert-ms")"
    if failed "$out2"; then
      echo "R5: demo DELETE + INSERT — delete ok, insert refused: $(sentence "$out2")"
    else
      total="$(scalar "$(trino_bearer "SELECT total FROM $CATALOG.$SCHEMA.daily WHERE day = DATE '2026-01-01'")")"
      echo "R5: demo DELETE + INSERT — refreshed, 2026-01-01 total $total, delete $(cat "$tmp/r5-delete-ms") ms + insert $(cat "$tmp/r5-insert-ms") ms"
    fi
  fi
fi

# --- R6 the pipeline principal -----------------------------------------------------------
if mint_principal; then
  out="$(timed_principal "$rebuild_sql" "$tmp/r6-ms")"
  if failed "$out"; then
    echo "R6: pipeline-demo CREATE OR REPLACE — refused: $(sentence "$out")"
  else
    total="$(scalar "$(trino_principal "SELECT total FROM $CATALOG.$SCHEMA.daily WHERE day = DATE '2026-01-01'")")"
    echo "R6: pipeline-demo CREATE OR REPLACE — refreshed, 2026-01-01 total $total, $(cat "$tmp/r6-ms") ms"
  fi
else
  echo "R6: pipeline-demo CREATE OR REPLACE — not run, Secret $PRINCIPAL_SECRET or the token endpoint is missing"
fi

# --- CLEANUP -----------------------------------------------------------------------------
drop_all
cleaned=yes
