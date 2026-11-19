#!/usr/bin/env bash
# Spike for stardelt/stardelt#309 (slice a of #201, Q3 of #75): the blast radius of
# `sts-enabled` on one warehouse — every client of `gold` sees what the flip changes.
#
# #259 measured that Lakekeeper vends a table-scoped credential for `demo` on `gold`, and
# restored the profile. This one flips `gold`, keeps it flipped for one run, and measures in
# that window: a writer's vended credential (Q20), Trino's reads and writes (Q21), the shared
# Spark server (Q22), the catalog's own maintenance (Q23), then the restore (Q24).
#
# The harness is the one of `scripts/spike-lakekeeper-vending.sh` (#292), reused: same
# bootstrap, management calls, profile flip/restore, `s3sig`, port-forwards, 0700 temp dir and
# `cleanup` trap. It writes one warehouse's storage profile and restores it on every exit path.
#
# Traps the first version fell into, measured on the lab 2026-11-19 (the ticket's letter is
# kept, the extra measurement is marked):
#   * a warehouse whose storage profile has `sts-enabled=false` vends nothing: the delegated
#     config of a table is byte-for-byte the plain one (`s3.endpoint`, `s3.region`,
#     `s3.path-style-access`, `region`, `client.region`, `scan-planning-mode` — no
#     `s3.access-key-id`, no `s3.session-token`). Vending needs the flip on the warehouse the
#     client reads through, and the ticket flips only `gold`.
#   * so Q20 as written (on `playground`, before the flip) can only report `vended=no`. The
#     writable-or-read-only question is therefore also measured on `gold` — the one warehouse
#     this run flips, where the **group** `demo` holds `write` on it — and printed as
#     `Q20-gold:`. That line does not match the `Q2[0-4]:` count of the ticket's verify.
#     (The **person** `demo` is in the group `visitors` only and reads `gold`; `Q20-gold`
#     therefore measured a *reader's* credential. The writer's case is #424, measured on
#     `playground` by `scripts/spike-lakekeeper-sts-writer.sh`.)
#   * the service account `superset` sees catalog `shared` only, so the Q21 read it names is
#     refused ("Cannot access catalog gold"); the read is repeated as `demo` and the refusal is
#     printed, rather than silently substituting a principal.
#   * `demo` may not create a table in `gold` through Trino ("Access Denied: Cannot create
#     table") although the warehouse assignments carry `create`, so `Q20-gold` uses an existing
#     table (default `shop.daily_revenue`) instead of one this script creates.
#   * `demo` may not drop either: the demo Catalog grants `write` and `create`, not `delete`.
#     The cleanup drops this run's tables through the catalog as the bootstrap service account.
#
# Every answer is one `Q<n>:` line. The script prints status codes, key names and key prefixes
# only: a credential in a log line is a defect. Tokens and passwords travel through mode-0600
# files (`-H @file`, `-K file`), never through argv.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
WAREHOUSE="${WAREHOUSE:-gold}"
SCRATCH_NS="${SCRATCH_NS:-scratch}"
WRITER_TABLE="${WRITER_TABLE:-sts_probe}"
SPARK_TABLE="${SPARK_TABLE:-spark_probe}"
LK_PORT="${LK_PORT:-18181}"
S3_PORT="${S3_PORT:-18333}"
ROLE_ARN="${ROLE_ARN:-arn:aws:iam::000000000000:role/stardelt}"
DEMO_PASSWORD="${DEMO_PASSWORD:-demo}"
POD=sts-blast-client
JOB=sts-blast-maint-gold

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/lk-sts-blast.XXXXXX")"
LK_PID=""
S3_PID=""
ID=""
PLAY_ID=""
BUCKET=""
REGION=""
TRINO_URL=""

# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  local code
  # The tables this run created, dropped through the catalog by the bootstrap service account.
  # `demo` cannot do it: the demo Catalog grants `write` and `create`, not `delete`, so
  # `DROP TABLE playground.scratch.sts_probe` answers "Access Denied: Cannot drop table"
  # (measured 2026-11-19).
  if [ -s "$tmp/auth" ] && [ -n "$PLAY_ID" ]; then
    drop_table "$PLAY_ID" "$SCRATCH_NS" "$WRITER_TABLE" >/dev/null
    drop_table "$PLAY_ID" "$SCRATCH_NS" "$SPARK_TABLE" >/dev/null
    [ -z "$ID" ] || drop_table "$ID" "$SCRATCH_NS" "$WRITER_TABLE" >/dev/null
  fi
  kubectl -n "$NS" delete job "$JOB" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl -n "$NS" delete secret "$POD-token" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NS" delete configmap "$POD-script" --ignore-not-found >/dev/null 2>&1 || true
  if [ -f "$tmp/flipped" ] && [ -n "$ID" ] && [ -s "$tmp/profile.json" ] && [ -s "$tmp/auth" ]; then
    code="$(restore_profile 2>/dev/null || echo 000)"
    echo "trap: restored the storage profile of $WAREHOUSE => ${code:-000}"
  fi
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  if [ -n "$S3_PID" ]; then kill "$S3_PID" >/dev/null 2>&1 || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

for tool in kubectl curl jq python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

# --- inputs ------------------------------------------------------------------------------
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.bucket}' | base64 -d > "$tmp/bucket"
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.region}' | base64 -d > "$tmp/region"
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.access-key}' | base64 -d > "$tmp/access-key"
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.secret-key}' | base64 -d > "$tmp/secret-key"
kubectl -n "$NS" get secret trino-password-db -o jsonpath='{.data.superset-password}' | base64 -d > "$tmp/superset-pw"
BUCKET="$(tr -d '\n' < "$tmp/bucket")"
REGION="$(tr -d '\n' < "$tmp/region")"

public_url="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.publicUrl}' 2>/dev/null || true)"
TRINO_URL="${TRINO_URL:-https://trino.${public_url#https://}}"
[ -n "${TRINO_URL#https://trino.}" ] || TRINO_URL="https://trino.alpha.dev.stardelt.io"

# --- Trino statement API -----------------------------------------------------------------
# One statement, rows as JSON: `{"code":200,"rows":[[..]],"error":null}`. `basic` is the
# service account `superset`, `bearer` the minted realm user — the same call, one header.
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
print(json.dumps({"code": code, "rows": rows, "error": failure}))
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

# A bearer statement, re-minted once if the access token outlived Keycloak's lifespan.
trino_bearer() { # <sql> -> json line
  local out
  out="$(trino bearer "$tmp/demo-token" "$1")"
  if [ "$(jq -r '.code // 0' <<<"$out")" = "401" ] && mint_demo 2>/dev/null; then
    out="$(trino bearer "$tmp/demo-token" "$1")"
  fi
  printf '%s' "$out"
}

# --- port-forwards -----------------------------------------------------------------------
kubectl -n "$NS" port-forward svc/lakekeeper "$LK_PORT:8181" >/dev/null 2>&1 &
LK_PID=$!
kubectl -n "$NS" port-forward svc/seaweedfs-s3 "$S3_PORT:8333" >/dev/null 2>&1 &
S3_PID=$!
LK="http://127.0.0.1:$LK_PORT"
EP="http://127.0.0.1:$S3_PORT"

up() { # url -> 0 when anything but a transport failure answered
  local code
  code="$(curl -s -o /dev/null -m 2 -w '%{http_code}' "$1" 2>/dev/null)" || code="000"
  [ "${code:-000}" != "000" ]
}
ready=no
for _ in $(seq 1 30); do
  if up "$LK/management/v1/info" && up "$EP/"; then ready=yes; break; fi
  sleep 1
done
[ "$ready" = "yes" ] || { echo "lakekeeper or the S3 gateway did not answer within 30s" >&2; exit 1; }

# --- management auth ---------------------------------------------------------------------
bootstrap_auth() {
  local issuer body
  kubectl -n "$NS" get secret svc-catalog-bootstrap -o jsonpath='{.data.client-id}' | base64 -d > "$tmp/client-id"
  kubectl -n "$NS" get secret svc-catalog-bootstrap -o jsonpath='{.data.client-secret}' | base64 -d > "$tmp/client-secret"
  issuer="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}')"
  [ -n "$issuer" ] || { echo "ConfigMap stardelt-identity has no issuerUrl" >&2; exit 1; }
  if ! curl -fsS -X POST "${issuer%/}/protocol/openid-connect/token" \
      --data-urlencode grant_type=client_credentials \
      --data-urlencode client_id@"$tmp/client-id" \
      --data-urlencode client_secret@"$tmp/client-secret" \
      -o "$tmp/token.json" 2>/dev/null; then
    body="$(jq -r '.error // "no body"' "$tmp/token.json" 2>/dev/null || echo "no body")"
    echo "the realm's token endpoint refused svc-catalog-bootstrap: $body" >&2
    exit 1
  fi
  printf 'Authorization: Bearer %s\n' "$(jq -r .access_token "$tmp/token.json")" > "$tmp/auth"
}

mgmt() { # <method> <path> [data-file] -> status code, body in $tmp/out.json
  local method="$1" path="$2" data="${3-}" code refreshed=no
  while :; do
    if [ -n "$data" ]; then
      code="$(curl -sS -H @"$tmp/auth" -H 'Content-Type: application/json' -X "$method" \
        -d @"$data" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    else
      code="$(curl -sS -H @"$tmp/auth" -X "$method" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    fi
    # Keycloak's client-credentials token lives 5 minutes; a long run must re-mint, not fail.
    if [ "$code" = "401" ] && [ "$refreshed" = "no" ]; then refreshed=yes; bootstrap_auth; continue; fi
    break
  done
  echo "${code:-000}"
}

post_profile() { # <body-file> -> status code
  mgmt POST "/management/v1/warehouse/$ID/storage" "$1"
}

# shellcheck disable=SC2329  # called by cleanup, which the trap calls
drop_table() { # <warehouse-id> <namespace> <table> -> status code (purged)
  local code refreshed=no
  while :; do
    code="$(curl -sS -H @"$tmp/auth" -X DELETE \
      "$LK/catalog/v1/$1/namespaces/$2/tables/$3?purgeRequested=true" -o /dev/null -w '%{http_code}')" || code="000"
    if [ "$code" = "401" ] && [ "$refreshed" = "no" ]; then refreshed=yes; bootstrap_auth; continue; fi
    break
  done
  echo "${code:-000}"
}

restore_profile() {
  jq -n --slurpfile p "$tmp/profile.json" \
        --rawfile ak "$tmp/access-key" --rawfile sk "$tmp/secret-key" \
    '{"storage-profile": $p[0],
      "storage-credential": {type: "s3", "credential-type": "access-key",
        "access-key-id": ($ak | rtrimstr("\n")), "secret-access-key": ($sk | rtrimstr("\n"))}}' \
    > "$tmp/restore.json"
  post_profile "$tmp/restore.json"
}

bootstrap_auth

# --- the two ids, and the profile the restore puts back -----------------------------------
code="$(mgmt GET /management/v1/warehouse)"
ID="$(jq -r --arg w "$WAREHOUSE" '
  if type == "array" then .[] elif has("warehouses") then .warehouses[] else empty end
  | select(.name == $w) | .id' "$tmp/out.json" | head -n1)"
PLAY_ID="$(jq -r '
  if type == "array" then .[] elif has("warehouses") then .warehouses[] else empty end
  | select(.name == "playground") | .id' "$tmp/out.json" | head -n1)"
if [ -z "$ID" ] || [ "$ID" = "null" ] || [ -z "$PLAY_ID" ] || [ "$PLAY_ID" = "null" ]; then
  echo "no warehouses named $WAREHOUSE and playground (http $code)" >&2
  exit 1
fi
code="$(mgmt GET "/management/v1/warehouse/$ID")"
jq '.["storage-profile"]' "$tmp/out.json" > "$tmp/profile.json"
STS_BEFORE="$(jq -r '.["sts-enabled"]' "$tmp/profile.json")"

# --- the demo token and the shared S3 request helper -------------------------------------
printf '%s' "$DEMO_PASSWORD" > "$tmp/password"
mint_demo
printf 'Authorization: Bearer %s\n' "$(cat "$tmp/demo-token")" > "$tmp/demo-auth"
printf 'X-Iceberg-Access-Delegation: vended-credentials\n' > "$tmp/deleg-header"

catalog() { # <path> [extra-header-file] -> code, body in $tmp/out.json
  local path="$1" hdr="${2-}" code
  if [ -n "$hdr" ]; then
    code="$(curl -sS -H @"$tmp/demo-auth" -H @"$hdr" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
  else
    code="$(curl -sS -H @"$tmp/demo-auth" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
  fi
  echo "${code:-000}"
}

s3sig() { # <method> <key> [body] -> status code, with the triple in $tmp/s3-curl.conf
  local method="$1" key="$2" body="${3-}" code
  if [ -n "$body" ]; then
    code="$(curl -sS -K "$tmp/s3-curl.conf" -o /dev/null -w '%{http_code}' --aws-sigv4 "aws:amz:$REGION:s3" \
      -X "$method" --data-binary "$body" "$EP/$BUCKET/$key")" || code="000"
  else
    code="$(curl -sS -K "$tmp/s3-curl.conf" -o /dev/null -w '%{http_code}' --aws-sigv4 "aws:amz:$REGION:s3" \
      -X "$method" "$EP/$BUCKET/$key")" || code="000"
  fi
  echo "${code:-000}"
}

vended_conf() { # <config.json> -> writes $tmp/s3-curl.conf, prints yes/no
  jq -r '.["s3.access-key-id"] // empty' "$1" > "$tmp/v-ak"
  jq -r '.["s3.secret-access-key"] // empty' "$1" > "$tmp/v-sk"
  jq -r '.["s3.session-token"] // empty' "$1" > "$tmp/v-tok"
  if [ -s "$tmp/v-tok" ]; then
    {
      printf 'user = "%s:%s"\n' "$(cat "$tmp/v-ak")" "$(cat "$tmp/v-sk")"
      printf 'header = "x-amz-security-token: %s"\n' "$(cat "$tmp/v-tok")"
    } > "$tmp/s3-curl.conf"
    echo yes
  else
    : > "$tmp/s3-curl.conf"
    echo no
  fi
}

# --- question 20: a writer's credential, before any flip ------------------------------------------
schema="$(trino_bearer "CREATE SCHEMA IF NOT EXISTS playground.$SCRATCH_NS")"
[ "$(jq -r '.error // empty' <<<"$schema")" = "" ] \
  || { echo "refused: CREATE SCHEMA as demo => $(jq -r '.code' <<<"$schema") $(jq -c '.error' <<<"$schema")" >&2; exit 1; }
made="$(trino_bearer "CREATE TABLE IF NOT EXISTS playground.$SCRATCH_NS.$WRITER_TABLE AS SELECT 1 AS x")"
[ "$(jq -r '.error // empty' <<<"$made")" = "" ] \
  || { echo "refused: CREATE TABLE as demo => $(jq -r '.code' <<<"$made") $(jq -c '.error' <<<"$made")" >&2; exit 1; }

code="$(catalog "/catalog/v1/$PLAY_ID/namespaces/$SCRATCH_NS/tables/$WRITER_TABLE" "$tmp/deleg-header")"
if [ "$code" != "200" ]; then
  echo "refused: table load as demo => $code $(jq -c '.error.message' "$tmp/out.json" 2>/dev/null || true)" >&2
  exit 1
fi
LOCATION="$(jq -r '.metadata.location' "$tmp/out.json")"
jq '.config' "$tmp/out.json" > "$tmp/q20-config.json"
vended="$(vended_conf "$tmp/q20-config.json")"
put="na"
writer="not-vended"
if [ "$vended" = "yes" ]; then
  KEY_IN="${LOCATION#s3://"$BUCKET"/}"
  put="$(s3sig PUT "$KEY_IN/sts-probe.txt" 'spike-309')"
  case "$put" in
    200) writer=writable; s3sig DELETE "$KEY_IN/sts-probe.txt" >/dev/null || true ;;
    403) writer=read-only ;;
    *) writer="put-$put" ;;
  esac
fi
echo "Q20: vended=$vended writer=$writer (put=$put)"

# --- question 21: Trino while $WAREHOUSE is STS-enabled ------------------------------------------
if [ "$STS_BEFORE" = "true" ]; then
  : # the trap must not restore a profile this run did not change
else
  jq -n --slurpfile p "$tmp/profile.json" \
        --rawfile ak "$tmp/access-key" --rawfile sk "$tmp/secret-key" \
        --arg ep "http://seaweedfs-s3.$NS.svc.cluster.local:8333" --arg role "$ROLE_ARN" \
    '{"storage-profile": ($p[0] + {"sts-enabled": true, "sts-role-arn": $role,
                                    "sts-endpoint": $ep, "sts-session-policy-principal": false}),
      "storage-credential": {type: "s3", "credential-type": "access-key",
        "access-key-id": ($ak | rtrimstr("\n")), "secret-access-key": ($sk | rtrimstr("\n"))}}' \
    > "$tmp/sts-profile.json"
  code="$(post_profile "$tmp/sts-profile.json")"
  # The flag exists only once a write landed: a failure before or during the flip restores nothing.
  if [ "$code" = "200" ]; then touch "$tmp/flipped"; fi
  if [ "$code" != "200" ]; then
    echo "refused: storage update => $code $(jq -c '{message: .error.message, type: .error.type}' "$tmp/out.json" 2>/dev/null || head -c 200 "$tmp/out.json")" >&2
    exit 1
  fi
fi

# The ticket names `superset` for the read. Measured 2026-11-19: that service account sees
# only catalog `shared` (`SHOW CATALOGS` => shared), so it cannot read `$WAREHOUSE` with or
# without a flip. The read is therefore repeated as `demo`, the realm user that does hold
# `select` on the catalog, and the refusal of the first leg is printed rather than hidden.
gold="$(trino basic "$tmp/superset-pw" "SELECT count(*) FROM $WAREHOUSE.shop.daily_revenue")"
gold_error="$(jq -r '.error.message // empty' <<<"$gold")"
if [ -n "$gold_error" ]; then
  echo "refused: the superset read of $WAREHOUSE => $(jq -r '.code' <<<"$gold") $gold_error" >&2
  gold="$(trino_bearer "SELECT count(*) FROM $WAREHOUSE.shop.daily_revenue")"
  gold_error="$(jq -r '.error.message // empty' <<<"$gold")"
  [ -z "$gold_error" ] || echo "refused: the demo read of $WAREHOUSE => $(jq -r '.code' <<<"$gold") $gold_error" >&2
fi
gold_rows="$(jq -r '.rows[0][0] // "-"' <<<"$gold")"
demo_write="$(trino_bearer "CREATE OR REPLACE TABLE playground.$SCRATCH_NS.$WRITER_TABLE AS SELECT 2 AS x")"
demo_write_code="$(jq -r '.code' <<<"$demo_write")"
demo_write_error="$(jq -r '.error.message // empty' <<<"$demo_write")"
if [ -n "$demo_write_error" ]; then
  echo "refused: Trino refused the demo write => $demo_write_code $demo_write_error" >&2
fi
echo "Q21: trino-gold-rows=$gold_rows demo-trino-write=$demo_write_code"

# --- Q20-gold: the same writer question, on the warehouse this run flips ------------------
# Not part of the ticket's verify: `playground` is never flipped here, so only `$WAREHOUSE`
# can vend. The **group** `demo` holds `write` on it (Catalog `$WAREHOUSE`, .spec.grants); the
# **person** `demo` is in the group `visitors`, which reads `$WAREHOUSE`, so this load is a
# reader's load and `Q20-gold` answers a reader's credential — the writer's case is #424.
# The table is one that already exists: `demo` may not create a schema or table in
# `$WAREHOUSE` through Trino even though Lakekeeper's warehouse assignments carry `create`
# (measured 2026-11-19: `CREATE SCHEMA gold.scratch` => Access Denied, while the same call on
# `playground` answers 200). The session policy is computed from the warehouse's grants, not
# from who created the table, so an existing table carries the same answer.
vend_ns="${VEND_NAMESPACE:-shop}"
vend_table="${VEND_TABLE:-daily_revenue}"
code="$(catalog "/catalog/v1/$ID/namespaces/$vend_ns/tables/$vend_table" "$tmp/deleg-header")"
if [ "$code" != "200" ]; then
  echo "Q20-gold: table load => $code" >&2
else
  LOCATION="$(jq -r '.metadata.location' "$tmp/out.json")"
  jq '.config' "$tmp/out.json" > "$tmp/q20-gold-config.json"
  vended="$(vended_conf "$tmp/q20-gold-config.json")"
  put="na"
  writer="not-vended"
  if [ "$vended" = "yes" ]; then
    KEY_IN="${LOCATION#s3://"$BUCKET"/}"
    put="$(s3sig PUT "$KEY_IN/sts-probe.txt" 'spike-309')"
    case "$put" in
      200) writer=writable; s3sig DELETE "$KEY_IN/sts-probe.txt" >/dev/null || true ;;
      403) writer=read-only ;;
      *) writer="put-$put" ;;
    esac
  fi
  echo "Q20-gold: vended=$vended writer=$writer (put=$put, $vend_ns.$vend_table)"
fi

# --- question 22: the shared Spark server ---------------------------------------------------------
client_digest="$(grep -m1 -E '^[[:space:]]*tag:[[:space:]]*"dev@sha256:[0-9a-f]{64}"' helm-values/jupyterhub.yaml \
  | sed -E 's/.*"dev@(sha256:[0-9a-f]{64})".*/\1/')"
# One print for every path: the answer line exists exactly once in this file.
report_q22() { # <gold-rows> <playground-write>
  echo "Q22: gold-rows=$1 playground-write=$2"
}
if [ -z "$client_digest" ]; then
  echo "not tested: helm-values/jupyterhub.yaml names no dev@sha256 notebook image" >&2
  report_q22 - not-tested
else
  kubectl -n "$NS" create secret generic "$POD-token" \
    --from-file=token="$tmp/demo-token" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

  cat > "$tmp/client.py" <<'PY'
import os

from pyspark.sql import SparkSession

REMOTE = os.environ["SPARK_REMOTE"]
TOKEN = os.environ["TRINO_TOKEN"]
session = SparkSession.builder.remote(REMOTE).appName("sts-blast").create()


def catalog(name):
    """The catalog block `python/stardelt/__init__.py` writes, plus the delegation header."""
    setting = "spark.sql.catalog." + name
    entries = [
        ("", "org.apache.iceberg.spark.SparkCatalog"),
        (".type", "rest"),
        (".rest-catalog-purge", "true"),
        (".uri", "http://lakekeeper.stardelt.svc.cluster.local:8181/catalog"),
        (".warehouse", name),
        (".token", TOKEN),
        (".header.X-Iceberg-Access-Delegation", "vended-credentials"),
        (".io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
        (".s3.endpoint", "http://seaweedfs-s3.stardelt.svc.cluster.local:8333"),
        (".s3.region", "us-east-1"),
        (".s3.path-style-access", "true"),
    ]
    for key, value in entries:
        session.conf.set(setting + key, value)


catalog("gold")
try:
    print("gold-rows=%d" % session.table("gold.shop.daily_revenue").count(), flush=True)
except Exception as error:  # the message is the measurement
    print("gold-rows=- (%s)" % str(error).strip().splitlines()[0][:200], flush=True)

catalog("playground")
try:
    session.sql("CREATE NAMESPACE IF NOT EXISTS playground.scratch").collect()
    session.sql("SELECT 1 AS x").writeTo("playground.scratch.spark_probe").createOrReplace()
    print("playground-write=ok", flush=True)
except Exception as error:  # the message is the measurement
    print("playground-write=fail (%s)" % str(error).strip().splitlines()[0][:200], flush=True)
finally:
    try:
        session.stop()
    except Exception:
        pass
PY

  kubectl -n "$NS" create configmap "$POD-script" --from-file=client.py="$tmp/client.py" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null

  # The labels are the two NetworkPolicies' contract: `component: singleuser-server` +
  # `app`/`release` jupyterhub is `singleuser` (DNS, 15002 to spark-connect) and, without
  # `stardelt.io/group.visitors`, `spark-connect-ingress` admits it as a member.
  kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true
  if ! kubectl -n "$NS" apply -f - >/dev/null 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${NS}
  labels:
    app: jupyterhub
    release: jupyterhub
    component: singleuser-server
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  containers:
    - name: client
      image: ghcr.io/stardelt/jupyter-singleuser:dev@${client_digest}
      command: ["python3", "/opt/check/client.py"]
      env:
        - name: HOME
          value: /tmp
        - name: SPARK_REMOTE
          value: sc://spark-connect.${NS}.svc.cluster.local:15002
        - name: TRINO_TOKEN
          valueFrom:
            secretKeyRef: { name: ${POD}-token, key: token }
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
      resources:
        requests: { cpu: 100m, memory: 512Mi }
        limits: { cpu: 1, memory: 2Gi }
      volumeMounts:
        - name: script
          mountPath: /opt/check
          readOnly: true
  volumes:
    - name: script
      configMap:
        name: ${POD}-script
EOF
  then
    echo "not tested: the client pod could not be created" >&2
    report_q22 - not-tested
  else
    if kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$POD" --timeout=300s >/dev/null 2>&1; then
      logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
    else
      logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
      echo "the client pod did not finish within 300s ($(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo Unknown))" >&2
    fi
    gold_rows_line="$(printf '%s\n' "$logs" | grep -m1 '^gold-rows=' || true)"
    write_line="$(printf '%s\n' "$logs" | grep -m1 '^playground-write=' || true)"
    [ -n "$gold_rows_line" ] || gold_rows_line="gold-rows=- (no line in the log)"
    [ -n "$write_line" ] || write_line="playground-write=fail (no line in the log)"
    gold_rows="${gold_rows_line#gold-rows=}"
    case "$gold_rows" in *"("*) printf '%s\n' "$gold_rows_line" >&2 ;; esac
    case "$write_line" in playground-write=ok) write=ok ;; *) write=fail; printf '%s\n' "$write_line" >&2 ;; esac
    report_q22 "${gold_rows%% *}" "$write"
  fi
fi

# --- question 23: the catalog's own maintenance ---------------------------------------------------
maint=Succeeded
kubectl -n "$NS" delete job "$JOB" --ignore-not-found >/dev/null 2>&1 || true
if kubectl -n "$NS" create job --from=cronjob/maint-"$WAREHOUSE" "$JOB" >/dev/null 2>&1; then
  if kubectl -n "$NS" wait --for=condition=complete "job/$JOB" --timeout=300s >/dev/null 2>&1; then
    kubectl -n "$NS" logs "job/$JOB" --tail=3 2>&1 || true
  else
    maint="$(kubectl -n "$NS" get job "$JOB" -o jsonpath='{.status.conditions[*].type}' 2>/dev/null || echo Unknown)"
    maint="${maint:-not-complete}"
    kubectl -n "$NS" logs "job/$JOB" --tail=3 2>&1 || true
  fi
  kubectl -n "$NS" delete job "$JOB" --ignore-not-found >/dev/null 2>&1 || true
else
  maint="job-not-created"
fi
echo "Q23: maint-$WAREHOUSE=$maint"

# --- question 24: the restore holds ---------------------------------------------------------------
q24="restore-failed"
code="$(restore_profile)"
if [ "$code" = "200" ]; then
  rm -f "$tmp/flipped"
  mgmt GET "/management/v1/warehouse/$ID" >/dev/null
  STS_RESTORED="$(jq -r '.["storage-profile"]["sts-enabled"]' "$tmp/out.json")"
  code_read="$(catalog "/catalog/v1/$PLAY_ID/namespaces/$SCRATCH_NS/tables/$WRITER_TABLE")"
  echo "Q24: restore => sts-enabled=$STS_RESTORED, table read => $code_read"
  q24=restored
  [ "$STS_RESTORED" = "false" ] && [ "$code_read" = "200" ] || q24="restored-mismatch"
else
  echo "refused: restore => $code $(jq -c '{message: .error.message}' "$tmp/out.json" 2>/dev/null || head -c 200 "$tmp/out.json")" >&2
fi

case "$q24" in
  restored) exit 0 ;;
  *) exit 1 ;;
esac
