#!/usr/bin/env bash
# Spike for stardelt/stardelt#2096 (spike for #41, the feature store): #41 wants point-in-time
# correct joins for features — "offline-first, no online store until asked" — and its acceptance
# test ("define one feature view on a demo table and run a point-in-time join") has never been run
# on this platform. Before anyone chooses a component (or none) the engine the platform pins must
# be measured: does it join correctly, and what does the join cost.
#
#   cd platform && make spike-point-in-time-join
#
# A plain `JOIN … ON f.valid_from <= e.day` gives the classic trap: two rows for an event that
# spans two versions, and no row at all for an event older than every version. The spike prints
# that join and the two documented `NEAREST` forms, so the design note can show why the join is
# not a `JOIN`.
#
# Seven lines, printed from the running cluster (the `<…>` values are what the spike measures):
#   R1 setup          the scratch schema, three feature versions and three events
#   R2 the naive join `JOIN … ON valid_from <= day`: the wrong shape (a duplicate, a missing
#                     event), or the engine's own sentence when it refuses
#   R3 LEFT NEAREST   `LEFT JOIN NEAREST ( … MATCH valid_from <= e.day ) ON TRUE`: one feature
#                     row per event, the version in force on the event's day, or the sentence
#   R4 CROSS NEAREST  the second documented form, `CROSS JOIN NEAREST ( … )`: same rows as R3?
#   R5 10 000 events  R3's query over a table a notebook really joins: 10 000 rows, wall-clock ms
#   R6 pipeline       the same query as the demo group's pipeline principal (`sp-pipeline-demo`),
#                     which is what a scheduled `sql` task runs as
#   CLEANUP           events_big, events, features and the schema are gone: `playground` as it was
#
# Every statement a person makes goes through the catalog as `demo` (bearer token), never as a
# service account: the grants are part of the answer. The only cluster change is the scratch
# schema, and the trap drops it on every exit path. `playground` grants `write` and `create` to
# `demo`/`visitors` but no `delete`, so the drops as `demo` can be refused — then the demo group's
# pipeline principal (`sp-pipeline-demo`) drops the tables and the namespace through the Lakekeeper
# catalog API. The CLEANUP line says which path cleaned up.
#
# No token, password or Secret value is printed: only row counts, wall-clock milliseconds and the
# engine's sentences.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
CATALOG=playground
SCHEMA=spike_pit
GROUP=demo
DEMO_PASSWORD="${DEMO_PASSWORD:-demo}"
TRINO_URL="${TRINO_URL:-https://trino.alpha.dev.stardelt.io}"
PRINCIPAL_SECRET="${PRINCIPAL_SECRET:-sp-pipeline-$GROUP}"
LK_PORT="${LK_PORT:-18188}"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/spike-point-in-time.XXXXXX")"
cleaned=no

for tool in kubectl python3 curl jq; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

# --- Trino statement API -----------------------------------------------------------------
# One statement, rows as JSON: `{"code":200,"rows":[[..]],"error":null,"stats":{...}}`. The
# `trino.py`/`trino()`/`mint_demo()` block is the one in `spike-refresh-table.sh`.
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

# stdin, not argv: a 10 000-row result overflows the kernel's argument list (measured in R5).
code_of() { printf '%s' "$1" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("code", 0))'; }

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
# The demo person holds `write` and `create` on `playground`, not `delete` (measured): the drops
# can be refused. This is the fallback: the demo group's pipeline principal drops every table and
# the namespace through the Lakekeeper catalog API (`umask 077`, files in $tmp).
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

# The four drops as the same person that created them; then the fallback when they are refused.
# Prints the CLEANUP line, once per run (the trap calls it on a failed R row).
drop_all() {
  local sentences="" out sql
  for sql in \
    "DROP TABLE IF EXISTS $CATALOG.$SCHEMA.events_big" \
    "DROP TABLE IF EXISTS $CATALOG.$SCHEMA.events" \
    "DROP TABLE IF EXISTS $CATALOG.$SCHEMA.features" \
    "DROP SCHEMA IF EXISTS $CATALOG.$SCHEMA"
  do
    out="$(trino_bearer "$sql")"
    if failed "$out"; then sentences="${sentences}${sentences:+; }$(sentence "$out")"; fi
  done
  if [ -z "$sentences" ]; then
    echo "CLEANUP: events_big, events, features and $SCHEMA dropped as $GROUP"
  elif drop_schema_privileged; then
    echo "CLEANUP: events_big, events, features and $SCHEMA dropped by $PRINCIPAL_SECRET (as $GROUP: $sentences)"
  else
    echo "CLEANUP: events_big, events, features and $SCHEMA NOT dropped — as $GROUP: $sentences; $PRINCIPAL_SECRET could not drop $CATALOG.$SCHEMA"
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
out="$(trino_bearer "CREATE TABLE IF NOT EXISTS $CATALOG.$SCHEMA.features
  (entity varchar, valid_from date, label varchar)")"
s2="$( [ -z "$(sentence "$out")" ] && echo ok || echo "refused($(sentence "$out"))")"
out="$(trino_bearer "INSERT INTO $CATALOG.$SCHEMA.features VALUES
  ('a', DATE '2026-01-01', 'bronze'), ('a', DATE '2026-03-01', 'silver'), ('b', DATE '2026-02-01', 'bronze')")"
s3="$( [ -z "$(sentence "$out")" ] && echo ok || echo "refused($(sentence "$out"))")"
out="$(trino_bearer "CREATE TABLE IF NOT EXISTS $CATALOG.$SCHEMA.events
  (entity varchar, day date, amount bigint)")"
s4="$( [ -z "$(sentence "$out")" ] && echo ok || echo "refused($(sentence "$out"))")"
out="$(trino_bearer "INSERT INTO $CATALOG.$SCHEMA.events VALUES
  ('a', DATE '2026-01-15', 10), ('a', DATE '2026-04-01', 20), ('b', DATE '2026-01-15', 30)")"
s5="$( [ -z "$(sentence "$out")" ] && echo ok || echo "refused($(sentence "$out"))")"
case "$s1/$s2/$s3/$s4/$s5" in
  ok/ok/ok/ok/ok) : ;;
  *) echo "R1: setup failed schema=$s1 features=$s2 feature-rows=$s3 events=$s4 event-rows=$s5"; exit 1 ;;
esac
features="$(scalar "$(trino_bearer "SELECT count(*) FROM $CATALOG.$SCHEMA.features")")"
events="$(scalar "$(trino_bearer "SELECT count(*) FROM $CATALOG.$SCHEMA.events")")"
echo "R1: setup ok — $features features, $events events"

# --- R2 the naive join -------------------------------------------------------------------
r2_sql="SELECT e.entity, e.day, f.valid_from, f.label
FROM $CATALOG.$SCHEMA.events e
JOIN $CATALOG.$SCHEMA.features f
  ON f.entity = e.entity AND f.valid_from <= e.day
ORDER BY e.entity, e.day"
out="$(timed "$r2_sql" "$tmp/r2-ms")"
if failed "$out"; then
  echo "R2: naive join — refused: $(sentence "$out")"
  exit 1
fi
r2_rows="$(rows_of "$out")"
r2_rows_n="$(python3 -c 'import json,sys; print(len(json.loads(sys.argv[1])))' "$r2_rows")"
r2_verdict="$(python3 - "$r2_rows" <<'PY'
import json, sys
rows = json.loads(sys.argv[1])
dup = sum(1 for r in rows if r[0] == "a" and r[1] == "2026-04-01")
have_b = any(r[0] == "b" and r[1] == "2026-01-15" for r in rows)
if len(rows) == 3 and dup == 2 and not have_b:
    print("wrong: a duplicate and a missing event")
else:
    print("unexpected rows: " + "; ".join("/".join(str(c) for c in r) for r in rows))
PY
)"
echo "R2: naive join — $r2_rows_n rows: a/2026-04-01 twice, b/2026-01-15 missing — $r2_verdict"

# --- R3 LEFT JOIN NEAREST ----------------------------------------------------------------
r3_sql="SELECT e.entity, e.day, e.amount, label
FROM $CATALOG.$SCHEMA.events e
LEFT JOIN NEAREST (
    FROM $CATALOG.$SCHEMA.features
    WHERE entity = e.entity
    MATCH valid_from <= e.day
) ON TRUE
ORDER BY e.entity, e.day"
out="$(timed "$r3_sql" "$tmp/r3-ms")"
if failed "$out"; then
  echo "R3: LEFT JOIN NEAREST — refused: $(sentence "$out")"
  exit 1
fi
r3_rows="$(rows_of "$out")"
r3_rows_n="$(python3 -c 'import json,sys; print(len(json.loads(sys.argv[1])))' "$r3_rows")"
r3_verdict="$(python3 - "$r3_rows" <<'PY'
import json, sys
rows = json.loads(sys.argv[1])
want = {("a", "2026-01-15"): "bronze", ("a", "2026-04-01"): "silver", ("b", "2026-01-15"): None}
seen = {}
ok = len(rows) == 3
for r in rows:
    key = (r[0], r[1])
    label = r[3] if len(r) > 3 else None
    if label == "":
        label = None
    if key in seen or key not in want or want[key] != label:
        ok = False
    seen[key] = label


def show(key):
    label = seen.get(key)
    return "%s/%s %s" % (key[0], key[1], label if label is not None else "—")


order = [("a", "2026-01-15"), ("a", "2026-04-01"), ("b", "2026-01-15")]
if ok and set(seen) == set(want):
    print("correct: one row per event (%s)" % ", ".join(show(k) for k in order))
else:
    print("rows: " + "; ".join("/".join(str(c) for c in r) for r in rows))
PY
)"
echo "R3: LEFT JOIN NEAREST — $r3_rows_n rows, $r3_verdict, $(cat "$tmp/r3-ms") ms"

# --- R4 CROSS JOIN NEAREST ---------------------------------------------------------------
r4_sql="SELECT e.entity, e.day, e.amount, label
FROM $CATALOG.$SCHEMA.events e
CROSS JOIN NEAREST (
    FROM $CATALOG.$SCHEMA.features
    WHERE entity = e.entity
    MATCH valid_from <= e.day
)
ORDER BY e.entity, e.day"
out="$(timed "$r4_sql" "$tmp/r4-ms")"
if failed "$out"; then
  echo "R4: CROSS JOIN NEAREST — refused: $(sentence "$out")"
else
  r4_rows="$(rows_of "$out")"
  r4_verdict="$(python3 - "$r3_rows" "$r4_rows" <<'PY'
import json, sys
left = json.loads(sys.argv[1])
right = json.loads(sys.argv[2])
same = sorted(json.dumps(r) for r in left) == sorted(json.dumps(r) for r in right)
if same:
    print("same as R3")
else:
    print("differs from R3: " + "; ".join("/".join(str(c) for c in r) for r in right))
PY
)"
  echo "R4: CROSS JOIN NEAREST — $r4_verdict"
fi

# --- R5 10 000 events --------------------------------------------------------------------
out="$(trino_bearer "CREATE TABLE $CATALOG.$SCHEMA.events_big AS
SELECT CASE WHEN element % 2 = 0 THEN 'a' ELSE 'b' END AS entity,
       date_add('day', element % 120, DATE '2026-01-01') AS day,
       element AS amount
FROM UNNEST(sequence(1, 10000)) AS t(element)")"
if failed "$out"; then
  echo "R5: 10 000 events — setup refused: $(sentence "$out")"
else
  r5_sql="SELECT e.entity, e.day, e.amount, label
FROM $CATALOG.$SCHEMA.events_big e
LEFT JOIN NEAREST (
    FROM $CATALOG.$SCHEMA.features
    WHERE entity = e.entity
    MATCH valid_from <= e.day
) ON TRUE
ORDER BY e.entity, e.day"
  out="$(timed "$r5_sql" "$tmp/r5-ms")"
  # 10 000 rows overflow the kernel's argv limit, so this result is read from a file, not an
  # argument (the harness's `sentence`/`rows_of` pass json on the command line).
  printf '%s' "$out" > "$tmp/r5.json"
  r5="$(python3 - "$tmp/r5.json" <<'PY'
import json, sys
body = json.load(open(sys.argv[1]))
error = body.get("error")
if error:
    name = error.get("errorName") or error.get("type") or "ERROR"
    print("refused: %s: %s" % (name, str(error.get("message") or error).replace("\n", " ")[:400]))
else:
    print("%d rows" % len(body.get("rows") or []))
PY
)"
  echo "R5: 10 000 events — $r5, $(cat "$tmp/r5-ms") ms"
fi

# --- R6 the pipeline principal -----------------------------------------------------------
if mint_principal; then
  out="$(timed_principal "$r3_sql" "$tmp/r6-ms")"
  if failed "$out"; then
    echo "R6: pipeline-demo — refused: $(sentence "$out")"
  else
    rows="$(python3 -c 'import json,sys; print(len(json.loads(sys.argv[1])))' "$(rows_of "$out")")"
    echo "R6: pipeline-demo — $rows rows, ok"
  fi
else
  echo "R6: pipeline-demo — not run, Secret $PRINCIPAL_SECRET or the token endpoint is missing"
fi

# --- CLEANUP -----------------------------------------------------------------------------
drop_all
cleaned=yes
