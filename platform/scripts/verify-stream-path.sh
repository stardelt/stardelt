#!/usr/bin/env bash
# One Kafka message becomes one sink row, end to end (stardelt/stardelt#2006).
#
#   make verify-stream-path              (NS=… the platform namespace, TIMEOUT=… the row wait)
#
# Why: `make verify-streaming` proves the broker, the Spark Operator's watch and each StreamApp's
# phase — not that a stream turns a message into a row. On 2026-11-27 the demo's
# `demo-pegelonline` was `Running` while its sink table held an empty snapshot and its topic's
# offset stood still across a 20 s reading. A phase is not a data path.
#
# This check builds its own topic, StreamApp and Iceberg table, sends exactly one message, waits
# for it in the table, and removes all three:
#   streamapp  the operator brings the StreamApp to `Running`;
#   topic      the operator's own KafkaTopic answers `Ready` with `True`;
#   message    one JSON line through the broker becomes one row in the sink table.
#
# Exit 0 = PASS (the message became a row), 1 = FAIL (a step proved something false, or the
# scratch objects could not be removed), 3 = UNVERIFIED (no kubectl/curl/jq/python3, no
# principal Secret, no identity endpoint or no broker — nothing about the path was measured then).
#
# Traps this script carries:
#   - `umask 077`: the principal's client credentials and the token go into mode-0600 files under
#     the temp dir, and none of them is printed;
#   - `cleanup` runs on every exit path: the StreamApp, its KafkaTopic, the table and the schema
#     are removed whatever happened; a failed removal prints `CLEANUP FAILED <what>` and makes
#     the run exit 1 even after a PASS;
#   - two copies at once are safe (#1329): the StreamApp, topic and schema carry a per-run id, so
#     one copy never measures or deletes another's objects.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-${NAMESPACE:-stardelt}}"
GROUP="${GROUP:-demo}"
PRINCIPAL_SECRET="${PRINCIPAL_SECRET:-sp-pipeline-$GROUP}"
TRINO_URL="${TRINO_URL:-https://trino.alpha.dev.stardelt.io}"
TIMEOUT="${TIMEOUT:-180}"
RUN="verify-stream-path-$(date -u +%Y%m%dt%H%M%S)-$RANDOM"
SCHEMA="verify_stream_path_$RANDOM"
CATALOG=gold

scratch="${TMPDIR:-$HOME/.cache}"
mkdir -p "$scratch"
tmp="$(mktemp -d "$scratch/verify-stream-path.XXXXXX")"

BROKER=""
cleanup_failed=no

# One line out of multi-line text.
text() { tr '\n' ' ' | tr -s ' ' | cut -c1-300; }

# shellcheck disable=SC2329  # called from the trap below
cleanup() {
  set +e
  if ! kubectl -n "$NS" delete streamapp "$RUN" --ignore-not-found --wait=true --timeout=120s >/dev/null; then
    echo "CLEANUP FAILED streamapp $RUN" >&2
    cleanup_failed=yes
  fi
  if ! kubectl -n "$NS" delete kafkatopic "$RUN" --ignore-not-found --wait=true --timeout=120s >/dev/null 2>&1; then
    echo "CLEANUP FAILED kafkatopic $RUN" >&2
    cleanup_failed=yes
  fi
  # The sink is the principal's own; dropping it needs this run's token, never another's.
  if [ -s "$tmp/token" ]; then
    if [ "$(jq -r '.code' <<< "$(trino "DROP TABLE IF EXISTS $CATALOG.$SCHEMA.messages")" 2>/dev/null)" != "200" ]; then
      echo "CLEANUP FAILED table $CATALOG.$SCHEMA.messages" >&2
      cleanup_failed=yes
    fi
    if [ "$(jq -r '.code' <<< "$(trino "DROP SCHEMA IF EXISTS $CATALOG.$SCHEMA")" 2>/dev/null)" != "200" ]; then
      echo "CLEANUP FAILED schema $CATALOG.$SCHEMA" >&2
      cleanup_failed=yes
    fi
  fi
  rm -f "$tmp/token" "$tmp/token.json" "$tmp/client-id" "$tmp/client-secret"
  rm -rf "$tmp"
}

# shellcheck disable=SC2329  # registered with `trap` below
on_exit() {
  local code=$?
  trap - EXIT INT TERM
  set +e
  cleanup
  if [ "$cleanup_failed" = yes ] && [ "$code" -eq 0 ]; then code=1; fi
  exit "$code"
}
trap on_exit EXIT INT TERM

# --- preconditions -------------------------------------------------------------------------
for tool in kubectl curl jq python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "? $tool is not installed"; exit 3; }
done

if ! kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" >/dev/null 2>&1; then
  echo "? Secret $PRINCIPAL_SECRET is not installed — make demo-access"
  exit 3
fi
if [ -z "$(kubectl -n "$NS" get cm stardelt-identity -o jsonpath='{.data.tokenEndpoint}' 2>/dev/null || true)" ]; then
  echo "? ConfigMap stardelt-identity has no tokenEndpoint — make install-identity"
  exit 3
fi
BROKER="$(kubectl -n "$NS" get pods -l strimzi.io/name=kafka-kafka -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [ -z "$BROKER" ]; then
  echo "? no broker pod (strimzi.io/name=kafka-kafka) — make install"
  exit 3
fi

# --- the principal's client credentials, and its token -------------------------------------
# The values go into 0600 files and the token into another; none of them is printed.
kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" -o jsonpath='{.data.client-id}' | base64 -d > "$tmp/client-id"
kubectl -n "$NS" get secret "$PRINCIPAL_SECRET" -o jsonpath='{.data.client-secret}' | base64 -d > "$tmp/client-secret"
chmod 600 "$tmp/client-id" "$tmp/client-secret"
[ -s "$tmp/client-id" ] && [ -s "$tmp/client-secret" ] \
  || { echo "? Secret $PRINCIPAL_SECRET has no client-id/client-secret"; exit 3; }

token_url="$(kubectl -n "$NS" get cm stardelt-identity -o jsonpath='{.data.tokenEndpoint}')"
code="$(curl -sS -o "$tmp/token.json" -w '%{http_code}' -X POST "$token_url" \
  --data-urlencode grant_type=client_credentials \
  --data-urlencode client_id@"$tmp/client-id" \
  --data-urlencode client_secret@"$tmp/client-secret" 2>/dev/null)" || code="000"
if [ "$code" = "000" ]; then
  echo "? the identity endpoint did not answer — nothing was measured"
  exit 3
fi
[ "$code" = "200" ] \
  || { echo "? the realm refused $PRINCIPAL_SECRET (HTTP $code: $(jq -r '.error // "no error field"' "$tmp/token.json" 2>/dev/null | cut -c1-80))"; exit 3; }
jq -r .access_token "$tmp/token.json" > "$tmp/token"
chmod 600 "$tmp/token"
[ -s "$tmp/token" ] && [ "$(cat "$tmp/token")" != "null" ] \
  || { echo "? the realm answered without an access_token"; exit 3; }

# --- the Trino statement API ---------------------------------------------------------------
# One statement, rows as JSON: `{"code":200,"rows":[[..]],"error":null}`. `nextUri` is followed
# until it is absent; a transport failure is code 0, an engine error carries its message.
cat > "$tmp/trino.py" <<'PY'
import json, sys, urllib.error, urllib.request

url, token_file, sql = sys.argv[1:4]
headers = {
    "Content-Type": "text/plain",
    "Authorization": "Bearer " + open(token_file).read().strip(),
}


def call(endpoint, data=None):
    request = urllib.request.Request(endpoint, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, {"error": {"message": error.read().decode()[:300]}}
    except (urllib.error.URLError, OSError) as error:
        return 0, {"error": {"message": "unreachable: %s" % error}}


code, body = call(url.rstrip("/") + "/v1/statement", sql.encode())
rows = list(body.get("data") or [])
failure = body.get("error")
while body.get("nextUri"):
    code, body = call(body["nextUri"])
    rows.extend(body.get("data") or [])
    failure = failure or body.get("error")
print(json.dumps({"code": code, "rows": rows, "error": failure}))
PY

trino() { # <sql> -> json line
  python3 "$tmp/trino.py" "$TRINO_URL" "$tmp/token" "$1"
}

engine_error() { # <json> -> the engine's sentence, or empty
  jq -c '.error.message // .error // empty' <<< "$1" 2>/dev/null | text
}

echo "› principal: $PRINCIPAL_SECRET (token ok)"

# --- the scratch sink, as the principal ----------------------------------------------------
echo "› schema: $CATALOG.$SCHEMA"
out="$(trino "CREATE SCHEMA IF NOT EXISTS $CATALOG.$SCHEMA")"
if [ "$(jq -r '.code' <<< "$out")" != "200" ] || [ "$(jq -r '.error // empty' <<< "$out")" != "" ]; then
  echo "✗ CREATE SCHEMA refused: $(engine_error "$out")"
  echo "FAIL (the scratch schema could not be created)"
  exit 1
fi

echo "› sink: $CATALOG.$SCHEMA.messages (id bigint, word varchar)"
out="$(trino "CREATE TABLE $CATALOG.$SCHEMA.messages (id bigint, word varchar)")"
if [ "$(jq -r '.code' <<< "$out")" != "200" ] || [ "$(jq -r '.error // empty' <<< "$out")" != "" ]; then
  echo "✗ CREATE TABLE refused: $(engine_error "$out")"
  echo "FAIL (the scratch table could not be created)"
  exit 1
fi

# --- the StreamApp; the operator creates its KafkaTopic ------------------------------------
# `runAs` is derived from `ownerGroup` (`resolve_run_as` in stream_app.rs), and the topic comes
# from the platform's own bus because `source.connectionRef` is absent.
cat > "$tmp/streamapp.yaml" <<EOF
apiVersion: stardelt.io/v1alpha1
kind: StreamApp
metadata:
  name: $RUN
  namespace: $NS
  labels:
    stardelt.io/check: verify-stream-path
spec:
  ownerGroup: $GROUP
  source:
    topic: $RUN
    format: json
  sink:
    catalog: $CATALOG
    table: $SCHEMA.messages
    mode: append
  restartPolicy: on-failure
EOF

kubectl -n "$NS" apply -f "$tmp/streamapp.yaml" >/dev/null 2>&1 \
  || { echo "✗ the API server refused the StreamApp $RUN"; echo "FAIL (the stream was never created)"; exit 1; }

# --- wait: the operator brings the StreamApp to Running ------------------------------------
phase=""
deadline=$(( SECONDS + 120 ))
while [ "$SECONDS" -lt "$deadline" ]; do
  phase="$(kubectl -n "$NS" get streamapp "$RUN" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  case "$phase" in
    Running) break ;;
    Failed)
      echo "✗ streamapp $RUN Failed: $(kubectl -n "$NS" get streamapp "$RUN" -o jsonpath='{.status.lastError}' 2>/dev/null | text)"
      echo "FAIL (the operator could not start the stream)"
      exit 1
      ;;
  esac
  sleep 5
done
if [ "$phase" != "Running" ]; then
  echo "✗ streamapp $RUN did not reach Running within 120s (phase=${phase:-none}): $(kubectl -n "$NS" get streamapp "$RUN" -o jsonpath='{.status.lastError}' 2>/dev/null | text)"
  echo "FAIL (the stream never reported Running)"
  exit 1
fi
echo "✓ streamapp $RUN Running"

# --- wait: the operator's own topic is Ready -----------------------------------------------
ready=""
deadline=$(( SECONDS + 120 ))
while [ "$SECONDS" -lt "$deadline" ]; do
  ready="$(kubectl -n "$NS" get kafkatopic "$RUN" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  [ "$ready" = "True" ] && break
  sleep 3
done
if [ "$ready" != "True" ]; then
  echo "✗ kafkatopic $RUN is not Ready (status=${ready:-none})"
  echo "FAIL (the stream's topic never became ready)"
  exit 1
fi
echo "✓ kafkatopic Ready"

# --- one message, through the broker pod --------------------------------------------------
printf '%s\n' '{"id":1,"word":"hello"}' | kubectl -n "$NS" exec -i "$BROKER" -- \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$RUN" >/dev/null 2>&1 \
  || { echo "✗ the broker refused the message on $RUN"; echo "FAIL (no message was produced)"; exit 1; }
echo "✓ message sent (id=1 word=hello)"

# --- wait: the row appears in the sink -----------------------------------------------------
rows="?"
started=$SECONDS
deadline=$(( SECONDS + TIMEOUT ))
while [ "$SECONDS" -lt "$deadline" ]; do
  out="$(trino "SELECT count(*) FROM $CATALOG.$SCHEMA.messages WHERE word = 'hello'")"
  if [ "$(jq -r '.code' <<< "$out")" = "200" ] && [ "$(jq -r '.error // empty' <<< "$out")" = "" ]; then
    rows="$(jq -r '.rows[0][0] // 0' <<< "$out")"
    [ "$rows" -ge 1 ] 2>/dev/null && break
  fi
  sleep 5
done
if [ "$(jq -r '.code' <<< "$out")" = "0" ]; then
  echo "? the Trino endpoint did not answer ($TRINO_URL) — the row was never measured"
  exit 3
fi
if [ "$rows" -ge 1 ] 2>/dev/null; then
  echo "✓ sink row: $rows"
  echo "PASS (1 message -> 1 row in $(( SECONDS - started )) s)"
  exit 0
fi
echo "✗ no row in $CATALOG.$SCHEMA.messages after ${TIMEOUT}s (rows=${rows}, streamapp phase=$(kubectl -n "$NS" get streamapp "$RUN" -o jsonpath='{.status.phase}' 2>/dev/null || true)): $(kubectl -n "$NS" get streamapp "$RUN" -o jsonpath='{.status.lastError}' 2>/dev/null | text)"
echo "FAIL (the message never reached the sink)"
exit 1
