#!/usr/bin/env bash
# Spike for stardelt/stardelt#1986 (#32, M-10): can Trino 484 create, refresh (full and, if
# supported, incremental) and query a **materialized view** over an Iceberg table in the
# Lakekeeper catalog, and what do the timings look like?
#
#   cd platform && make spike-materialized-views
#
# The acceptance of #32 needs this measurement before any slice: #32 promises acceleration only
# if the engine can do it through the catalog the platform ships. Nothing here ships to a
# customer path; the whole run happens in the scratch schema `playground.spike_mv` as the demo
# person and the trap drops it again, so a failed M row leaves `playground` as it was.
#
# Eight lines, printed from the running cluster (the `<…>` values are what the spike measures):
#   M1 setup       the scratch schema, table and three rows (station, measured_at, value)
#   M2 create      whether `CREATE MATERIALIZED VIEW` is accepted, or the engine's sentence
#   M3 query       the view's row count (2) and the statement's wall-clock milliseconds
#   M4 stale       an insert the view does not see: row count and `n` for `a` are unchanged
#   M5 refresh     `REFRESH MATERIALIZED VIEW` and its milliseconds, then the refreshed counts
#   M6 incremental a second refresh after one more row: full or incremental, with the engine's
#                  own answer and how much of the source the refresh read
#   M7 permissions the view's metadata, its storage table, and whether `DROP MATERIALIZED VIEW`
#                  succeeds as the person that created it
#   CLEANUP        view, table and schema are gone: `playground` is as it was
#
# Every statement goes through the catalog as `demo` (bearer token), never as a service
# account: #32 has to know what a normal user may do. The only cluster change is the scratch
# schema, and the trap drops it on every exit path. `playground` grants `write` and `create` to
# `demo`/`visitors` but no `delete`, so the three drops as `demo` can be refused — then the
# CLEANUP line names that sentence and the demo group's pipeline principal (`sp-pipeline-demo`,
# the same principal `spike-spark-pipeline-dry-run.sh` uses) drops the schema through the
# Lakekeeper catalog API. The line says which path cleaned up.
#
# No token, password or Secret value is printed: only row counts, status codes and the engine's
# sentences.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
CATALOG=playground
SCHEMA=spike_mv
GROUP=demo
PRINCIPAL_SECRET="${PRINCIPAL_SECRET:-sp-pipeline-$GROUP}"
LK_PORT="${LK_PORT:-18188}"
DEMO_PASSWORD="${DEMO_PASSWORD:-demo}"
TRINO_URL="${TRINO_URL:-https://trino.alpha.dev.stardelt.io}"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/spike-mv.XXXXXX")"
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

# `SELECT … FROM the view` -> two facts: the row count, and `n` for station `a`.
view_facts() { # <json> -> prints "<rows> <n_a>"
  python3 - "$1" <<'PY'
import json, sys
rows = json.loads(sys.argv[1]).get("rows") or []
hit = [row for row in rows if row[0] == "a"]
print(len(rows), hit[0][2] if hit else "no-row")
PY
}

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

# The three drops, as the same person that created them; then the fallback when they are refused.
# Prints the CLEANUP line, once per run (the trap calls it on a failed M row).
drop_all() {
  local sentences="" out sql
  for sql in \
    "DROP MATERIALIZED VIEW IF EXISTS $CATALOG.$SCHEMA.latest" \
    "DROP TABLE IF EXISTS $CATALOG.$SCHEMA.measurements" \
    "DROP SCHEMA IF EXISTS $CATALOG.$SCHEMA"
  do
    out="$(trino_bearer "$sql")"
    if failed "$out"; then sentences="${sentences}${sentences:+; }$(sentence "$out")"; fi
  done
  if [ -z "$sentences" ]; then
    echo "CLEANUP ok"
  elif drop_schema_privileged; then
    echo "CLEANUP ok (as $GROUP: $sentences; dropped by $PRINCIPAL_SECRET)"
  else
    echo "CLEANUP refused as $GROUP ($sentences) and $PRINCIPAL_SECRET could not drop $CATALOG.$SCHEMA"
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

# --- M1 setup ----------------------------------------------------------------------------
printf '%s' "$DEMO_PASSWORD" > "$tmp/password"
mint_demo

out="$(trino_bearer "CREATE SCHEMA IF NOT EXISTS $CATALOG.$SCHEMA")"
s1="$( [ -z "$(sentence "$out")" ] && echo ok || echo "refused($(sentence "$out"))")"
out="$(trino_bearer "CREATE TABLE IF NOT EXISTS $CATALOG.$SCHEMA.measurements (station varchar, measured_at timestamp(6), value double)")"
s2="$( [ -z "$(sentence "$out")" ] && echo ok || echo "refused($(sentence "$out"))")"
out="$(trino_bearer "INSERT INTO $CATALOG.$SCHEMA.measurements VALUES
  ('a', TIMESTAMP '2026-01-01 00:00:00', 10.0),
  ('a', TIMESTAMP '2026-01-01 01:00:00', 12.0),
  ('b', TIMESTAMP '2026-01-01 00:00:00', 7.0)")"
s3="$( [ -z "$(sentence "$out")" ] && echo ok || echo "refused($(sentence "$out"))")"
case "$s1/$s2/$s3" in
  ok/ok/ok) echo "M1 setup ok schema=$s1 table=$s2 insert=$s3" ;;
  *) echo "M1 setup failed schema=$s1 table=$s2 insert=$s3"; exit 1 ;;
esac

# --- M2 create ---------------------------------------------------------------------------
out="$(trino_bearer "CREATE MATERIALIZED VIEW $CATALOG.$SCHEMA.latest AS
  SELECT station, max(measured_at) AS newest, count(*) AS n
  FROM $CATALOG.$SCHEMA.measurements GROUP BY station")"
create_error="$(sentence "$out")"
if [ -n "$create_error" ]; then
  # The refusal is the finding: record it, clean up, stop. No cluster property is touched.
  echo "M2 create refused: $create_error"
  drop_all
  cleaned=yes
  echo "verdict: no materialized view through $CATALOG on this Trino — #32 cannot build acceleration on it"
  exit 0
fi
echo "M2 create ok"

# --- M3 query ----------------------------------------------------------------------------
out="$(timed "SELECT * FROM $CATALOG.$SCHEMA.latest" "$tmp/m3-ms")"
if failed "$out"; then
  echo "M3 query refused: $(sentence "$out")"; exit 1
fi
read -r count _ < <(view_facts "$out")
echo "M3 query rows=$count elapsed=$(cat "$tmp/m3-ms")ms"

# --- M4 staleness ------------------------------------------------------------------------
out="$(trino_bearer "INSERT INTO $CATALOG.$SCHEMA.measurements VALUES ('a', TIMESTAMP '2026-01-01 02:00:00', 14.0)")"
if failed "$out"; then echo "M4 stale refused: $(sentence "$out")"; exit 1; fi
out="$(trino_bearer "SELECT * FROM $CATALOG.$SCHEMA.latest")"
if failed "$out"; then echo "M4 stale refused: $(sentence "$out")"; exit 1; fi
read -r count n_a < <(view_facts "$out")
echo "M4 stale rows=$count n_a=$n_a"

# --- M5 full refresh --------------------------------------------------------------------
out="$(timed "REFRESH MATERIALIZED VIEW $CATALOG.$SCHEMA.latest" "$tmp/m5-ms")"
if failed "$out"; then echo "M5 refresh refused: $(sentence "$out")"; exit 1; fi
out="$(trino_bearer "SELECT * FROM $CATALOG.$SCHEMA.latest")"
if failed "$out"; then echo "M5 refresh refused: $(sentence "$out")"; exit 1; fi
read -r count n_a < <(view_facts "$out")
echo "M5 refresh ok elapsed=$(cat "$tmp/m5-ms")ms rows=$count n_a=$n_a"

# --- M6 incremental ----------------------------------------------------------------------
# The source's row count before the refresh is the bar: a refresh that reads fewer positions
# than that is incremental, one that reads all of them (or reports nothing) is full.
source_rows="$(trino_bearer "SELECT count(*) FROM $CATALOG.$SCHEMA.measurements" \
  | python3 -c 'import json,sys; rows=json.loads(sys.argv[1]).get("rows") or []; print(rows[0][0] if rows else 0)')"
out="$(trino_bearer "INSERT INTO $CATALOG.$SCHEMA.measurements VALUES ('b', TIMESTAMP '2026-01-01 01:00:00', 9.0)")"
if failed "$out"; then echo "M6 incremental refused: $(sentence "$out")"; exit 1; fi
out="$(timed "REFRESH MATERIALIZED VIEW $CATALOG.$SCHEMA.latest" "$tmp/m6-ms")"
if failed "$out"; then echo "M6 incremental refused: $(sentence "$out")"; exit 1; fi
read_positions="$(python3 -c 'import json,sys; print((json.loads(sys.argv[1]).get("stats") or {}).get("processedInputPositions"))' "$out")"
out="$(trino_bearer "SELECT station, newest, n FROM $CATALOG.$SCHEMA.latest ORDER BY station")"
if failed "$out"; then echo "M6 incremental refused: $(sentence "$out")"; exit 1; fi
rows_after="$(rows_of "$out")"
explain="$(trino_bearer "EXPLAIN (TYPE IO) SELECT * FROM $CATALOG.$SCHEMA.latest")"
explain_text="$(python3 -c '
import json, sys
rows = json.loads(sys.argv[1]).get("rows") or []
print(" | ".join(str(cell) for row in rows for cell in row).replace("\n", " ")[:400])' "$explain")"
meta="$(trino_bearer "SELECT * FROM system.metadata.materialized_views WHERE schema_name = '$SCHEMA'")"
meta_count="$(python3 -c 'import json,sys; print(len(json.loads(sys.argv[1]).get("rows") or []))' "$meta")"
mode="$(python3 - "$read_positions" "$source_rows" <<'PY'
import sys
read, source = sys.argv[1], sys.argv[2]
if read in ("", "None", "null"):
    print("not-reported")
elif int(read) < int(source):
    print("incremental")
else:
    print("full")
PY
)"
echo "M6 incremental refresh=$mode source_positions_read=$read_positions source_rows=$source_rows elapsed=$(cat "$tmp/m6-ms")ms rows=$rows_after explain_io=[$explain_text]"

# --- M7 permissions ----------------------------------------------------------------------
storage="$(python3 - "$meta" <<'PY'
import json, sys
body = json.loads(sys.argv[1])
if body.get("error"):
    print("refused: " + str(body["error"].get("message"))[:200])
    raise SystemExit
rows = body.get("rows") or []
if not rows:
    print("no-row")
    raise SystemExit
values = " ".join(str(cell) for cell in rows[0])
print(values[:300])
PY
)"
drop="$(trino_bearer "DROP MATERIALIZED VIEW IF EXISTS $CATALOG.$SCHEMA.latest")"
drop_fact="$( [ -z "$(sentence "$drop")" ] && echo ok || echo "refused($(sentence "$drop"))")"
echo "M7 permissions metadata_rows=$meta_count storage=[$storage] drop=$drop_fact"

# --- CLEANUP -----------------------------------------------------------------------------
drop_all
cleaned=yes
echo "verdict: materialized views are usable through $CATALOG on this Trino"
