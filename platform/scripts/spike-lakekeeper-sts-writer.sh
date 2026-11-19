#!/usr/bin/env bash
# Spike for stardelt/stardelt#424 (Q3 of #75, after #309): what does a *writer* get from a
# vended credential?
#
# #309 printed `Q20-gold: vended=yes writer=read-only (put=403)` and the ticket for #424 shows
# why that line does not answer the question: that run loaded a table as the *person* `demo`,
# who is in the group `visitors` only and holds `select` on `gold`, while the `write` grant on
# `gold` belongs to the *group* `demo`. A reader got a read-only credential — consistent with
# the catalog deriving the scope from the caller's rights.
#
#   W1  the caller is a writer: `visitors` hold `write` on `playground` (the catalog, not the
#       person `demo`), so a table loaded there as `demo` is a writer's load.
#   W2  two tables are created inside `playground.scratch` through Trino as `demo`.
#   W3  `playground`'s storage profile is flipped to `sts-enabled=true` (W3 prints the code).
#   W4  table a is loaded with `X-Iceberg-Access-Delegation: vended-credentials`, table b
#       without it; the vendored credential of table a is used at the S3 gateway for a PUT, a
#       GET and a DELETE inside table a's prefix, and for a PUT inside table b's (the sibling).
#   W5  a multipart upload inside table a's prefix with the same credential (create, two parts,
#       complete, delete).
#   W6  the lifetime keys of the delegated config (names only: whether an expiry and a refresh
#       endpoint are present, and the warehouse's validity setting).
#   W7  a Spark session in the demo sandbox — which holds no static key — writes a table in
#       `playground.scratch` through the catalog and reads it back.
#   W8  the storage profile is restored and the warehouse's flag read back.
#
# This script writes one warehouse's storage profile (`playground`) and restores it on every
# exit path: `cleanup` runs on EXIT/INT/TERM and restores from `$tmp/profile.json` whenever a
# flip landed. `gold` and the other warehouses are not touched, and no pod outside this run's
# client is restarted.
#
# It prints no credential: the bucket keys and the visitor's token travel through mode-0600
# files (`-H @file`, `-K file`), never through argv or stdout. Status codes, key names and the
# length of a prefix are the output; a credential in a log line is a defect.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
WAREHOUSE=playground
LK_PORT="${LK_PORT:-18181}"
S3_PORT="${S3_PORT:-18333}"
ROLE_ARN="${ROLE_ARN:-arn:aws:iam::000000000000:role/stardelt}"
DEMO_PASSWORD="${DEMO_PASSWORD:-demo}"
POD=sts-writer-client
SCRATCH_NS=scratch
DEMO_SPARK_NS="${DEMO_SPARK_NS:-stardelt-demo-spark}"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/lk-sts-writer.XXXXXX")"
LK_PID=""
S3_PID=""
ID=""
BUCKET=""
REGION=""
TRINO_URL=""

# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  local code
  # The tables this run created, dropped through the catalog by the bootstrap service account:
  # the demo Catalog grants `write` and `create`, not `delete`.
  if [ -s "$tmp/auth" ] && [ -n "$ID" ]; then
    drop_table "$ID" "$SCRATCH_NS" sts_writer_a >/dev/null
    drop_table "$ID" "$SCRATCH_NS" sts_writer_b >/dev/null
    drop_table "$ID" "$SCRATCH_NS" sts_writer_spark >/dev/null
  fi
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

# --- W1: the caller is a writer -----------------------------------------------------------
verbs="$(kubectl -n "$NS" get catalog "$WAREHOUSE" -o json 2>/dev/null \
  | jq -r '[.spec.grants[] | select(.group == "visitors") | .verb] | join(",")' 2>/dev/null || true)"
echo "W1: visitors verbs on $WAREHOUSE = ${verbs:-<none>}"
case ",$verbs," in
  *,write,*) : ;;
  *) echo "W1: visitors hold no write on $WAREHOUSE" >&2; exit 1 ;;
esac

# --- the warehouse id, and the profile the restore puts back ------------------------------
code="$(mgmt GET /management/v1/warehouse)"
ID="$(jq -r --arg w "$WAREHOUSE" '
  if type == "array" then .[] elif has("warehouses") then .warehouses[] else empty end
  | select(.name == $w) | .id' "$tmp/out.json" | head -n1)"
if [ -z "$ID" ] || [ "$ID" = "null" ]; then
  echo "no warehouse named $WAREHOUSE (http $code)" >&2
  exit 1
fi

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

# --- W2: two tables, as the writer, before the flip ---------------------------------------
# `demo` may not create a schema or table in `gold` through Trino (measured 2026-11-19 in
# #309) but does hold `create` + `write` on `playground`, so both statements answer 200 here.
for statement in \
  "CREATE SCHEMA IF NOT EXISTS $WAREHOUSE.$SCRATCH_NS" \
  "CREATE TABLE IF NOT EXISTS $WAREHOUSE.$SCRATCH_NS.sts_writer_a AS SELECT 1 AS x" \
  "CREATE TABLE IF NOT EXISTS $WAREHOUSE.$SCRATCH_NS.sts_writer_b AS SELECT 1 AS x"
do
  out="$(trino_bearer "$statement")"
  if [ -n "$(jq -r '.error // empty' <<<"$out")" ]; then
    echo "refused: $statement => $(jq -r '.code' <<<"$out") $(jq -c '.error' <<<"$out")" >&2
    exit 1
  fi
done

# --- W3: flip $WAREHOUSE -----------------------------------------------------------------
code="$(mgmt GET "/management/v1/warehouse/$ID")"
jq '.["storage-profile"]' "$tmp/out.json" > "$tmp/profile.json"
STS_BEFORE="$(jq -r '.["sts-enabled"]' "$tmp/profile.json")"

if [ "$STS_BEFORE" = "true" ]; then
  # Already flipped when the run started: nothing to change, nothing for the trap to restore.
  echo "W3: flip => already-enabled (sts-enabled was true before this run)" >&2
  code=200
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
fi
echo "W3: flip => $code"
if [ "$code" != "200" ]; then
  echo "refused: storage update => $code $(jq -c '{message: .error.message, type: .error.type}' "$tmp/out.json" 2>/dev/null || head -c 200 "$tmp/out.json")" >&2
  exit 1
fi

# --- W4: the writer's credential at the S3 level -----------------------------------------
code="$(catalog "/catalog/v1/$ID/namespaces/$SCRATCH_NS/tables/sts_writer_a" "$tmp/deleg-header")"
if [ "$code" != "200" ]; then
  echo "refused: table a load as demo => $code $(jq -c '.error.message' "$tmp/out.json" 2>/dev/null || true)" >&2
  exit 1
fi
KEY_A="$(jq -r '.metadata.location' "$tmp/out.json")"
KEY_A="${KEY_A#s3://"$BUCKET"/}"
jq '.config' "$tmp/out.json" > "$tmp/w4-a-config.json"
vended="$(vended_conf "$tmp/w4-a-config.json")"

put="na"; get="na"; del="na"; sibling="na"
if [ "$vended" = "no" ]; then
  echo "W4: vended=no"
else
  code="$(catalog "/catalog/v1/$ID/namespaces/$SCRATCH_NS/tables/sts_writer_b")"
  if [ "$code" != "200" ]; then
    echo "refused: table b load as demo => $code $(jq -c '.error.message' "$tmp/out.json" 2>/dev/null || true)" >&2
    exit 1
  fi
  KEY_B="$(jq -r '.metadata.location' "$tmp/out.json")"
  KEY_B="${KEY_B#s3://"$BUCKET"/}"
  # Table a's credential again: table b's load overwrote `$tmp/s3-curl.conf`.
  vended_conf "$tmp/w4-a-config.json" >/dev/null

  put="$(s3sig PUT "$KEY_A/w4-probe.txt" w4)"
  get="$(s3sig GET "$KEY_A/w4-probe.txt")"
  del="$(s3sig DELETE "$KEY_A/w4-probe.txt")"
  sibling="$(s3sig PUT "$KEY_B/w4-probe.txt" w4)"
  echo "W4: put=$put get=$get delete=$del sibling-put=$sibling"
fi

# --- W5: a multipart upload inside table a's prefix --------------------------------------
if [ "$vended" = "no" ]; then
  echo "W5: create/part1/part2/complete/delete = na na na na na"
else
  vended_conf "$tmp/w4-a-config.json" >/dev/null
  create="$(curl -sS -K "$tmp/s3-curl.conf" -o "$tmp/w5-create.xml" -w '%{http_code}' \
    --aws-sigv4 "aws:amz:$REGION:s3" -X POST "$EP/$BUCKET/$KEY_A/w5.bin?uploads=")" || create="000"
  part1=na; part2=na; complete=na; del5=na
  if [ "$create" != "200" ]; then
    echo "W5: create refused => $(sed -n 's:.*<Code>\([^<]*\)</Code>.*:\1:p' "$tmp/w5-create.xml" | head -n1)" >&2
  else
    UPLOAD_ID="$(sed -n 's:.*<UploadId>\(.*\)</UploadId>.*:\1:p' "$tmp/w5-create.xml")"
    head -c 5242880 /dev/zero > "$tmp/part1"
    part1="$(curl -sS -K "$tmp/s3-curl.conf" -o /dev/null -D "$tmp/h1" -w '%{http_code}' \
      --aws-sigv4 "aws:amz:$REGION:s3" -X PUT --data-binary @"$tmp/part1" \
      "$EP/$BUCKET/$KEY_A/w5.bin?partNumber=1&uploadId=$UPLOAD_ID")" || part1="000"
    ETAG1="$(grep -i '^etag:' "$tmp/h1" | cut -d' ' -f2 | tr -d '\r')"
    head -c 1024 /dev/zero > "$tmp/part2"
    part2="$(curl -sS -K "$tmp/s3-curl.conf" -o /dev/null -D "$tmp/h2" -w '%{http_code}' \
      --aws-sigv4 "aws:amz:$REGION:s3" -X PUT --data-binary @"$tmp/part2" \
      "$EP/$BUCKET/$KEY_A/w5.bin?partNumber=2&uploadId=$UPLOAD_ID")" || part2="000"
    ETAG2="$(grep -i '^etag:' "$tmp/h2" | cut -d' ' -f2 | tr -d '\r')"
    printf '<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>' \
      "$ETAG1" "$ETAG2" > "$tmp/complete.xml"
    complete="$(curl -sS -K "$tmp/s3-curl.conf" -o /dev/null -w '%{http_code}' \
      --aws-sigv4 "aws:amz:$REGION:s3" -X POST --data-binary @"$tmp/complete.xml" \
      "$EP/$BUCKET/$KEY_A/w5.bin?uploadId=$UPLOAD_ID")" || complete="000"
  fi
  del5="$(s3sig DELETE "$KEY_A/w5.bin")"
  echo "W5: create/part1/part2/complete/delete = $create $part1 $part2 $complete $del5"
fi

# --- W6: lifetime keys (names only) ------------------------------------------------------
expires="$(jq 'has("s3.session-token-expires-at-ms")' "$tmp/w4-a-config.json")"
refresh="$(jq '[keys[] | select(test("refresh"))] | length > 0' "$tmp/w4-a-config.json")"
validity="$(jq -r '.["sts-token-validity-seconds"] // "default"' "$tmp/profile.json")"
echo "W6: expires-at=$expires refresh-endpoint=$refresh validity=$validity"

# --- W7: Spark writes with no key --------------------------------------------------------
sandbox=no
if kubectl -n "$DEMO_SPARK_NS" get deploy spark-demo >/dev/null 2>&1; then sandbox=yes; fi
if [ "$sandbox" = "no" ]; then
  echo "W7: sandbox not installed"
else
  client_digest="$(grep -m1 -E '^[[:space:]]*tag:[[:space:]]*"dev@sha256:[0-9a-f]{64}"' helm-values/jupyterhub.yaml \
    | sed -E 's/.*"dev@(sha256:[0-9a-f]{64})".*/\1/')"
  if [ -z "$client_digest" ]; then
    echo "W7: sandbox not installed (helm-values/jupyterhub.yaml names no dev@sha256 notebook image)" >&2
  else
    kubectl -n "$NS" create secret generic "$POD-token" \
      --from-file=token="$tmp/demo-token" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

    cat > "$tmp/client.py" <<'PY'
import os

from pyspark.sql import SparkSession

SPARK_HOST = os.environ["SPARK_HOST"]
TOKEN = os.environ["TRINO_TOKEN"]
session = SparkSession.builder.remote("sc://" + SPARK_HOST).appName("sts-writer").create()


def first_line(exc):
    lines = [l.strip() for l in str(exc).splitlines() if l.strip()]
    return (lines[0] if lines else type(exc).__name__)[:240]


# Only `playground` is configured: the write goes through the one catalog whose visitors hold
# `write`, with the demo token and the delegation header, as python/stardelt sets a catalog.
name = "playground"
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

try:
    session.createDataFrame([(1,), (2,)], "x int").writeTo("playground.scratch.sts_writer_spark").createOrReplace()
    print("W7: sandbox-write=ok rows=%d" % session.table("playground.scratch.sts_writer_spark").count(), flush=True)
except Exception as exc:  # the message is the measurement
    print("W7: sandbox-write=refused: " + first_line(exc), flush=True)
finally:
    try:
        session.stop()
    except Exception:
        pass
PY

    kubectl -n "$NS" create configmap "$POD-script" --from-file=client.py="$tmp/client.py" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null

    # The labels are the two NetworkPolicies' contract, as in `verify-spark-demo-security.sh`:
    # `component: singleuser-server` + `app`/`release` jupyterhub is a `singleuser` pod (DNS and
    # 15002), and the sandbox's own ingress policy admits only a pod that carries
    # `stardelt.io/group.visitors: "true"`. Without that label the connection is dropped
    # silently and the job hangs — measured 2026-11-19 on this run.
    kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true
    if ! kubectl -n "$NS" apply -f - >/dev/null 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${NS}
  labels:
    app.kubernetes.io/name: spike-lakekeeper-sts-writer
    app: jupyterhub
    release: jupyterhub
    component: singleuser-server
    stardelt.io/group.visitors: "true"
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
        - name: SPARK_HOST
          value: spark-demo.${DEMO_SPARK_NS}.svc.cluster.local:15002
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
      echo "W7: sandbox-write=refused: the client pod could not be created" >&2
    else
      if kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$POD" --timeout=300s >/dev/null 2>&1; then
        logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
      else
        logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
        echo "the client pod did not finish within 300s ($(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo Unknown))" >&2
      fi
      line="$(printf '%s\n' "$logs" | grep -m1 '^W7:' || true)"
      [ -n "$line" ] || line="W7: sandbox-write=refused: no line in the log"
      printf '%s\n' "$line"
    fi
  fi
fi

# --- W8: restore -------------------------------------------------------------------------
code="$(restore_profile)"
if [ "$code" = "200" ]; then
  rm -f "$tmp/flipped"
  mgmt GET "/management/v1/warehouse/$ID" >/dev/null
  STS_RESTORED="$(jq -r '.["storage-profile"]["sts-enabled"]' "$tmp/out.json")"
  echo "W8: restore => sts-enabled=$STS_RESTORED"
else
  echo "refused: restore => $code $(jq -c '{message: .error.message}' "$tmp/out.json" 2>/dev/null || head -c 200 "$tmp/out.json")" >&2
  exit 1
fi

exit 0
