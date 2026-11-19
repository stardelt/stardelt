#!/usr/bin/env bash
# Spike for stardelt/stardelt#490 (renewal of #75): what keeps a long job writing?
#
# The sources say how renewal should work (#488 §1.2-1.4); nothing of it is measured. This run
# measures the seven facts a long job depends on, with the shapes the product uses:
#
#   V1  `playground`'s storage profile takes `sts-token-validity-seconds: 900` (a long job's
#       credential has to be short enough to expire inside a run, and long enough to measure).
#   V2  where the lifetime keys live: keys of `.config` and of
#       `.["storage-credentials"][0].config` from `loadTable`, and the vended credential's
#       `expires-in` in seconds (names only, never values).
#   V3  the refresh endpoint answers: `…/tables/{t}/credentials` with the caller's own token
#       (expected 200) and with a token the catalog cannot accept (expected 401).
#   V4  a Spark write that commits after the vended credential's 900 s: does Iceberg refresh it,
#       and does the refresh need a *live* catalog token? Counts the requests the catalog saw.
#   V5  which renewal grant the realm accepts for a pipeline principal: client credentials and
#       the RFC 8693 token exchange Iceberg sends (codes and the `error` field only).
#   V6  a renewed channel keeps the Spark Connect session: `session_id=…` in a new client
#       channel, then read a temp view the first channel created.
#   V7  the storage profile is restored and the warehouse's flags are read back.
#
# This script writes one warehouse's storage profile (`playground`) and restores it on every
# exit path: `cleanup` runs on EXIT/INT/TERM and restores from `$tmp/profile.json` whenever a
# flip landed. `gold` and the other warehouses are not touched, and no deployment is restarted.
#
# It prints no token, key, session token or secret: every credential travels through mode-0600
# files (`-H @file`, `-K file`, Secret keys), never through argv or stdout. Status codes, key
# names, counts and numbers of seconds are the output; a credential in a log line is a defect.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
WAREHOUSE=playground
LK_PORT="${LK_PORT:-18181}"
S3_PORT="${S3_PORT:-18333}"
ROLE_ARN="${ROLE_ARN:-arn:aws:iam::000000000000:role/stardelt}"
DEMO_PASSWORD="${DEMO_PASSWORD:-demo}"
POD=renewal-client
SCRATCH_NS=scratch
DEMO_SPARK_NS="${DEMO_SPARK_NS:-stardelt-demo-spark}"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/renewal.XXXXXX")"
LK_PID=""
S3_PID=""
ID=""
TRINO_URL=""

# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  local code
  # The table this run created, dropped through the catalog by the bootstrap service account:
  # the demo Catalog grants `write` and `create`, not `delete`.
  if [ -s "$tmp/auth" ] && [ -n "$ID" ]; then
    drop_table "$ID" "$SCRATCH_NS" renew_a >/dev/null
  fi
  kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl -n "$NS" delete pod "$POD-connect" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl -n "$NS" delete secret "$POD-token" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NS" delete configmap "$POD-script" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NS" delete configmap "$POD-connect-script" --ignore-not-found >/dev/null 2>&1 || true
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
# The bucket and the region are not read: this run makes no signed S3 request of its own (the
# writer script's W4/W5 do), and only the flip body needs the warehouse's key pair.
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.access-key}' | base64 -d > "$tmp/access-key"
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.secret-key}' | base64 -d > "$tmp/secret-key"
kubectl -n "$NS" get secret trino-password-db -o jsonpath='{.data.superset-password}' | base64 -d > "$tmp/superset-pw"

public_url="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.publicUrl}' 2>/dev/null || true)"
TRINO_URL="${TRINO_URL:-https://trino.${public_url#https://}}"
[ -n "${TRINO_URL#https://trino.}" ] || TRINO_URL="https://trino.alpha.dev.stardelt.io"
DOMAIN="${DOMAIN:-${public_url#https://}}"
[ -n "$DOMAIN" ] || DOMAIN=alpha.dev.stardelt.io

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
LK="http://127.0.0.1:$LK_PORT"
EP="http://127.0.0.1:$S3_PORT"

up() { # url -> 0 when anything but a transport failure answered
  local code
  code="$(curl -s -o /dev/null -m 2 -w '%{http_code}' "$1" 2>/dev/null)" || code="000"
  [ "${code:-000}" != "000" ]
}

# A forward that outlives a 17-minute wait is gone when the run comes back to it (measured
# 2026-11-19: the V4 wait outlived the forward, and the restore answered 000). Every caller
# lifts it again on a transport failure instead of reporting a failed run.
forward_lk() {
  local _
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  kubectl -n "$NS" port-forward svc/lakekeeper "$LK_PORT:8181" >/dev/null 2>&1 &
  LK_PID=$!
  for _ in $(seq 1 30); do
    if up "$LK/management/v1/info"; then return 0; fi
    sleep 1
  done
  return 1
}

forward_s3() {
  local _
  if [ -n "$S3_PID" ]; then kill "$S3_PID" >/dev/null 2>&1 || true; fi
  kubectl -n "$NS" port-forward svc/seaweedfs-s3 "$S3_PORT:8333" >/dev/null 2>&1 &
  S3_PID=$!
  for _ in $(seq 1 30); do
    if up "$EP/"; then return 0; fi
    sleep 1
  done
  return 1
}

if ! forward_lk || ! forward_s3; then
  echo "lakekeeper or the S3 gateway did not answer within 30s" >&2
  exit 1
fi

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
  local method="$1" path="$2" data="${3-}" code refreshed=no reopened=no
  while :; do
    if [ -n "$data" ]; then
      code="$(curl -sS -H @"$tmp/auth" -H 'Content-Type: application/json' -X "$method" \
        -d @"$data" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    else
      code="$(curl -sS -H @"$tmp/auth" -X "$method" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    fi
    # Keycloak's client-credentials token lives 5 minutes; a long run must re-mint, not fail.
    if [ "$code" = "401" ] && [ "$refreshed" = "no" ]; then refreshed=yes; bootstrap_auth; continue; fi
    if [ "$code" = "000" ] && [ "$reopened" = "no" ]; then reopened=yes; forward_lk && continue; fi
    break
  done
  echo "${code:-000}"
}

post_profile() { # <body-file> -> status code
  mgmt POST "/management/v1/warehouse/$ID/storage" "$1"
}

# shellcheck disable=SC2329  # called by cleanup, which the trap calls
drop_table() { # <warehouse-id> <namespace> <table> -> status code (purged)
  local code refreshed=no reopened=no
  while :; do
    code="$(curl -sS -H @"$tmp/auth" -X DELETE \
      "$LK/catalog/v1/$1/namespaces/$2/tables/$3?purgeRequested=true" -o /dev/null -w '%{http_code}')" || code="000"
    if [ "$code" = "401" ] && [ "$refreshed" = "no" ]; then refreshed=yes; bootstrap_auth; continue; fi
    if [ "$code" = "000" ] && [ "$reopened" = "no" ]; then reopened=yes; forward_lk && continue; fi
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

# --- the demo token and the shared request helpers ---------------------------------------
printf '%s' "$DEMO_PASSWORD" > "$tmp/password"
mint_demo
printf 'Authorization: Bearer %s\n' "$(cat "$tmp/demo-token")" > "$tmp/demo-auth"
printf 'X-Iceberg-Access-Delegation: vended-credentials\n' > "$tmp/deleg-header"

catalog() { # <path> [extra-header-file] -> code, body in $tmp/out.json
  local path="$1" hdr="${2-}" code reopened=no
  while :; do
    if [ -n "$hdr" ]; then
      code="$(curl -sS -H @"$tmp/demo-auth" -H @"$hdr" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    else
      code="$(curl -sS -H @"$tmp/demo-auth" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    fi
    if [ "$code" = "000" ] && [ "$reopened" = "no" ]; then reopened=yes; forward_lk && continue; fi
    break
  done
  echo "${code:-000}"
}

# --- step 2: the table the run writes into, as the writer, before the flip ----------------
for statement in \
  "CREATE SCHEMA IF NOT EXISTS $WAREHOUSE.$SCRATCH_NS" \
  "CREATE TABLE IF NOT EXISTS $WAREHOUSE.$SCRATCH_NS.renew_a AS SELECT 1 AS x"
do
  out="$(trino_bearer "$statement")"
  if [ -n "$(jq -r '.error // empty' <<<"$out")" ]; then
    echo "refused: $statement => $(jq -r '.code' <<<"$out") $(jq -c '.error' <<<"$out")" >&2
    exit 1
  fi
done

# --- V1: flip with a short validity -------------------------------------------------------
code="$(mgmt GET "/management/v1/warehouse/$ID")"
jq '.["storage-profile"]' "$tmp/out.json" > "$tmp/profile.json"
STS_BEFORE="$(jq -r '.["sts-enabled"]' "$tmp/profile.json")"
VALIDITY_BEFORE="$(jq -r '.["sts-token-validity-seconds"] // "default"' "$tmp/profile.json")"

# The flip body is the writer script's, plus the validity. A profile that already has STS on
# keeps its `sts-*` values: only the validity is changed, so the restore is one field away.
jq -n --slurpfile p "$tmp/profile.json" \
      --rawfile ak "$tmp/access-key" --rawfile sk "$tmp/secret-key" \
      --arg ep "http://seaweedfs-s3.$NS.svc.cluster.local:8333" --arg role "$ROLE_ARN" \
  '{"storage-profile": ($p[0]
      + (if ($p[0]["sts-enabled"] // false) then {} else
           {"sts-enabled": true, "sts-role-arn": $role, "sts-endpoint": $ep,
            "sts-session-policy-principal": false} end)
      + {"sts-token-validity-seconds": 900}),
    "storage-credential": {type: "s3", "credential-type": "access-key",
      "access-key-id": ($ak | rtrimstr("\n")), "secret-access-key": ($sk | rtrimstr("\n"))}}' \
  > "$tmp/sts-profile.json"
code="$(post_profile "$tmp/sts-profile.json")"
# The flag exists only once a write landed: a failure before or during the flip restores nothing.
if [ "$code" = "200" ]; then touch "$tmp/flipped"; fi
echo "V1: flip(validity=900) => $code"
if [ "$code" != "200" ]; then
  echo "refused: storage update => $code $(jq -c '{message: .error.message, type: .error.type}' "$tmp/out.json" 2>/dev/null || head -c 200 "$tmp/out.json")" >&2
  exit 1
fi

# --- V2: where the lifetime keys are ------------------------------------------------------
code="$(catalog "/catalog/v1/$ID/namespaces/$SCRATCH_NS/tables/renew_a" "$tmp/deleg-header")"
if [ "$code" != "200" ]; then
  echo "refused: table load as demo => $code $(jq -c '.error.message' "$tmp/out.json" 2>/dev/null || true)" >&2
  exit 1
fi
cp "$tmp/out.json" "$tmp/renew-a.json"
config_keys="$(jq -r '[.config | keys[]] | sort | join(",")' "$tmp/renew-a.json")"
sc_keys="$(jq -r '[.["storage-credentials"][0].config | keys[]] | sort | join(",")' "$tmp/renew-a.json")"
expires_in="$(jq -r '
  .["storage-credentials"][0].config["s3.session-token-expires-at-ms"] as $ms
  | if $ms == null then "none" else ((($ms | tonumber) / 1000) - now | round | tostring) end' "$tmp/renew-a.json")"
echo "V2: config-keys=[$config_keys] sc-keys=[$sc_keys] expires-in=$expires_in"

# --- V3: the refresh endpoint answers -----------------------------------------------------
code="$(catalog "/catalog/v1/$ID/namespaces/$SCRATCH_NS/tables/renew_a/credentials")"
keys="$(jq -r '[.["storage-credentials"][0].config | keys[]] | sort | join(",")' "$tmp/out.json")"
# The same path with a token the catalog cannot accept: the negative case of the refresh.
printf 'Authorization: Bearer invalid\n' > "$tmp/bad-auth"
bad="$(curl -sS -H @"$tmp/bad-auth" "$LK/catalog/v1/$ID/namespaces/$SCRATCH_NS/tables/renew_a/credentials" \
  -o "$tmp/v3-bad.json" -w '%{http_code}')" || bad="000"
echo "V3: load-credentials=$code keys=[$keys] bad-token=$bad"

# --- V4: a Spark write after the vended credential expired --------------------------------
sandbox=no
if kubectl -n "$DEMO_SPARK_NS" get deploy spark-demo >/dev/null 2>&1; then sandbox=yes; fi
if [ "$sandbox" = "no" ]; then
  echo "V4: write-after-expiry=skipped (the demo sandbox is not installed)"
else
  client_digest="$(grep -m1 -E '^[[:space:]]*tag:[[:space:]]*"dev@sha256:[0-9a-f]{64}"' helm-values/jupyterhub.yaml \
    | sed -E 's/.*"dev@(sha256:[0-9a-f]{64})".*/\1/')"
  if [ -z "$client_digest" ]; then
    echo "V4: write-after-expiry=skipped (helm-values/jupyterhub.yaml names no dev@sha256 notebook image)"
  else
    kubectl -n "$NS" create secret generic "$POD-token" \
      --from-file=token="$tmp/demo-token" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

    cat > "$tmp/client.py" <<'PY'
import base64
import json
import os
import time

from pyspark.sql import SparkSession

SPARK_HOST = os.environ["SPARK_HOST"]
TOKEN = os.environ["TRINO_TOKEN"]
session = SparkSession.builder.remote("sc://" + SPARK_HOST).appName("renewal").create()


def first_line(exc):
    """The first line that names the cause, then the deepest `Caused by:` — never a traceback."""
    lines = [l.strip() for l in str(exc).splitlines() if l.strip()]
    named = [l for l in lines if any(k in l.lower() for k in ("credential", "token", "403", "expired"))]
    first = named[0] if named else (lines[0] if lines else type(exc).__name__)
    causes = [l for l in lines if l.startswith("Caused by:")]
    if causes and causes[-1] != first:
        return (first + " | " + causes[-1])[:300]
    return first[:300]


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

segment = TOKEN.split(".")[1]
exp = json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))).get("exp", 0)
print("V4: token-left-at-start=%d" % (exp - time.time()), flush=True)
session.table("playground.scratch.renew_a").count()


def slow(it):
    # Longer than the 900 s the warehouse hands out: the commit below needs a fresh credential.
    time.sleep(960)
    for pdf in it:
        yield pdf


try:
    session.range(0, 2, 1, 1).toDF("x").mapInPandas(slow, "x long") \
        .writeTo("playground.scratch.renew_a").append()
    print("V4: write-after-expiry=ok rows=%d" % session.table("playground.scratch.renew_a").count(), flush=True)
except Exception as exc:  # the message is the measurement
    print("V4: write-after-expiry=refused: " + first_line(exc), flush=True)
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
    # `stardelt.io/group.visitors: "true"`.
    calls_before="$(kubectl -n "$NS" logs deploy/lakekeeper -c lakekeeper --since=30m 2>/dev/null \
      | { grep -c '/tables/renew%5Fa/credentials' || true; })"
    calls_before="${calls_before:-0}"
    kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true
    if ! kubectl -n "$NS" apply -f - >/dev/null 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${NS}
  labels:
    app.kubernetes.io/name: spike-renewal
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
      echo "V4: write-after-expiry=refused: the client pod could not be created"
    else
      # 960 s of sleep, the write, and the session's own start-up: 25 minutes.
      if kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$POD" --timeout=1500s >/dev/null 2>&1; then
        logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
      else
        logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
        echo "the client pod did not finish within 1500s ($(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo Unknown))" >&2
      fi
      printf '%s\n' "$logs" | grep -E '^V4:' || echo "V4: write-after-expiry=refused: no line in the log"
    fi
    # The requests to the refresh endpoint the catalog logged during the run: the delta, so
    # V3's own probe of the same path does not count as a refresh. A *successful*
    # `loadCredentials` leaves no log line (only a refusal does), so this counts failures.
    calls_after="$(kubectl -n "$NS" logs deploy/lakekeeper -c lakekeeper --since=30m 2>/dev/null \
      | { grep -c '/tables/renew%5Fa/credentials' || true; })"
    calls_after="${calls_after:-0}"
    echo "V4: credentials-calls=$((calls_after - calls_before))"
  fi
fi

# --- V5: which renewal grant the realm accepts for a pipeline principal -------------------
# Names only from the live driver: the client's secret is read into a mode-0600 file and never
# printed, and the client id stays in a file that curl's `-K` config reads.
pod_env() { # <pod> <env-name> -> the value, `secret:<name>/<key>`, or `missing`
  kubectl -n "$NS" get pod "$1" -o json | jq -r --arg n "$2" '
    [.spec.containers[].env[]? | select(.name == $n)][0]
    | if . == null then "missing"
      elif .value != null then .value
      elif .valueFrom.secretKeyRef != null then "secret:" + .valueFrom.secretKeyRef.name + "/" + .valueFrom.secretKeyRef.key
      else "other" end'
}

read_secret_ref() { # <secret:<name>/<key>> <out-file> -> 0 when the key was read
  local ref="${1#secret:}" name key
  name="${ref%%/*}"
  key="${ref##*/}"
  [ -n "$name" ] && [ -n "$key" ] || return 1
  kubectl -n "$NS" get secret "$name" -o jsonpath="{.data.$key}" | base64 -d > "$2"
}

if ! kubectl -n "$NS" get pod pegelonline-driver >/dev/null 2>&1; then
  echo "V5: client-credentials=skipped token-exchange=skipped (no pegelonline-driver pod)"
else
  id_ref="$(pod_env pegelonline-driver CATALOG_CLIENT_ID)"
  secret_ref="$(pod_env pegelonline-driver CATALOG_CLIENT_SECRET)"
  token_url="$(pod_env pegelonline-driver CATALOG_TOKEN_URL)"
  if ! read_secret_ref "$id_ref" "$tmp/pipeline-client-id" \
     || ! read_secret_ref "$secret_ref" "$tmp/pipeline-client-secret" \
     || [ -z "$token_url" ] || [ "$token_url" = "missing" ]; then
    echo "V5: client-credentials=skipped token-exchange=skipped (the driver's catalog env is not readable: id=$id_ref secret=$secret_ref)"
  else
    # `-K` keeps the client's credentials out of argv; `-d`/`--data-urlencode name@file` keeps
    # the subject token out of it too.
    {
      printf 'url = "%s"\n' "$token_url"
      printf 'user = "%s:%s"\n' "$(cat "$tmp/pipeline-client-id")" "$(cat "$tmp/pipeline-client-secret")"
    } > "$tmp/cc.conf"
    cc="$(curl -sS -K "$tmp/cc.conf" -d grant_type=client_credentials -d scope=openid \
      -o "$tmp/cc.json" -w '%{http_code}')" || cc="000"
    jq -r '.access_token // empty' "$tmp/cc.json" > "$tmp/cc-token"
    tx="skipped"
    tx_note="no access token to exchange"
    if [ -s "$tmp/cc-token" ]; then
      tx="$(curl -sS -K "$tmp/cc.conf" \
        -d grant_type=urn:ietf:params:oauth:grant-type:token-exchange \
        --data-urlencode subject_token@"$tmp/cc-token" \
        -d subject_token_type=urn:ietf:params:oauth:token-type:access_token \
        -d scope=openid -o "$tmp/tx.json" -w '%{http_code}')" || tx="000"
      if jq -e 'has("access_token")' "$tmp/tx.json" >/dev/null 2>&1; then
        tx_note="ok"
      else
        tx_note="$(jq -r '.error // "no access_token, no error"' "$tmp/tx.json" 2>/dev/null || echo "no body")"
      fi
    fi
    echo "V5: client-credentials=$cc token-exchange=$tx $tx_note (client ${secret_ref%%/*})"
  fi
fi

# --- V6: a renewed channel keeps the Spark Connect session --------------------------------
SPARK_USER="${SPARK_USER:-analyst}"
issuer="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}')"
if ! kubectl -n "$NS" get secret dev-seed-users -o jsonpath="{.data.$SPARK_USER}" 2>/dev/null \
    | base64 -d > "$tmp/spark-pw" || [ ! -s "$tmp/spark-pw" ]; then
  echo "V6: reattach=skipped (Secret dev-seed-users has no key $SPARK_USER)"
else
  chmod 600 "$tmp/spark-pw"
  member="yes"
  for i in 1 2; do
    if ! ISSUER="$issuer" USER="$SPARK_USER" PASSWORD_FILE="$tmp/spark-pw" TOKEN_FILE="$tmp/member$i" \
        python3 scripts/mint-user-token.py >/dev/null 2>&1; then member=no; fi
  done
  client_digest="$(grep -m1 -E '^[[:space:]]*tag:[[:space:]]*"dev@sha256:[0-9a-f]{64}"' helm-values/jupyterhub.yaml \
    | sed -E 's/.*"dev@(sha256:[0-9a-f]{64})".*/\1/')"
  if [ "$member" = "no" ] || [ -z "$client_digest" ]; then
    echo "V6: reattach=skipped (could not mint two member tokens, or no dev@sha256 notebook image)"
  else
    cat > "$tmp/connect.py" <<'PY'
import os

from pyspark.sql import SparkSession

BASE = os.environ["BASE_DOMAIN"]
T1 = os.environ["MEMBER_TOKEN"]
T2 = os.environ["MEMBER_TOKEN_2"]


def first_line(exc):
    lines = [l.strip() for l in str(exc).splitlines() if l.strip()]
    picked = [l for l in lines if "StatusCode." in l or "details =" in l]
    return (", ".join(picked) if picked else (lines[0] if lines else type(exc).__name__))[:240]


try:
    # `.create()`, not `.getOrCreate()`: the builder's getOrCreate returns the process's active
    # session (pyspark/sql/connect/session.py), so it would keep s1's channel and measure nothing.
    s1 = SparkSession.builder.remote(
        "sc://spark.%s:443/;use_ssl=true;user_id=renewal;token=%s" % (BASE, T1)).create()
    s1.range(5).createOrReplaceTempView("renew_v")
    sid = s1.session_id
    s2 = SparkSession.builder.remote(
        "sc://spark.%s:443/;use_ssl=true;user_id=renewal;session_id=%s;token=%s" % (BASE, sid, T2)).create()
    print("V6: reattach rows=%d same-session=%s" % (s2.table("renew_v").count(), s2.session_id == sid), flush=True)
except Exception as exc:  # the error text is the measurement
    print("V6: reattach refused: " + first_line(exc), flush=True)
PY

    kubectl -n "$NS" create configmap "$POD-connect-script" --from-file=connect.py="$tmp/connect.py" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null
    # One Secret for both client pods: the demo token for V4, the two member tokens for V6.
    kubectl -n "$NS" create secret generic "$POD-token" \
      --from-file=token="$tmp/demo-token" --from-file=t1="$tmp/member1" --from-file=t2="$tmp/member2" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null

    # The pod carries `app.kubernetes.io/name` and nothing else: no NetworkPolicy selects it, so
    # it has the egress and DNS the public host needs — the shape `verify-spark-auth.sh` measured.
    kubectl -n "$NS" delete pod "$POD-connect" --ignore-not-found >/dev/null 2>&1 || true
    if ! kubectl -n "$NS" apply -f - >/dev/null 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}-connect
  namespace: ${NS}
  labels:
    app.kubernetes.io/name: spike-renewal-connect
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  containers:
    - name: client
      image: ghcr.io/stardelt/jupyter-singleuser:dev@${client_digest}
      command: ["python3", "/opt/check/connect.py"]
      env:
        - name: HOME
          value: /tmp
        - name: BASE_DOMAIN
          value: ${DOMAIN}
        - name: MEMBER_TOKEN
          valueFrom:
            secretKeyRef: { name: ${POD}-token, key: t1 }
        - name: MEMBER_TOKEN_2
          valueFrom:
            secretKeyRef: { name: ${POD}-token, key: t2 }
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
      resources:
        requests: { cpu: 100m, memory: 256Mi }
        limits: { cpu: 1, memory: 1Gi }
      volumeMounts:
        - name: script
          mountPath: /opt/check
          readOnly: true
  volumes:
    - name: script
      configMap:
        name: ${POD}-connect-script
EOF
    then
      echo "V6: reattach refused: the client pod could not be created"
    else
      if kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$POD-connect" --timeout=300s >/dev/null 2>&1; then
        logs="$(kubectl -n "$NS" logs "$POD-connect" 2>&1 || true)"
      else
        logs="$(kubectl -n "$NS" logs "$POD-connect" 2>&1 || true)"
        echo "the reattach pod did not finish within 300s ($(kubectl -n "$NS" get pod "$POD-connect" -o jsonpath='{.status.phase}' 2>/dev/null || echo Unknown))" >&2
      fi
      printf '%s\n' "$logs" | grep -m1 -E '^V6:' || echo "V6: reattach refused: no line in the log"
    fi
  fi
fi

# --- V7: restore -------------------------------------------------------------------------
code="$(restore_profile)"
if [ "$code" = "200" ]; then
  rm -f "$tmp/flipped"
  mgmt GET "/management/v1/warehouse/$ID" >/dev/null
  echo "V7: restore => sts-enabled=$(jq -r '.["storage-profile"]["sts-enabled"]' "$tmp/out.json") validity=$(jq -r '.["storage-profile"]["sts-token-validity-seconds"] // "default"' "$tmp/out.json") (before: sts-enabled=$STS_BEFORE validity=$VALIDITY_BEFORE)"
else
  echo "refused: restore => $code $(jq -c '{message: .error.message}' "$tmp/out.json" 2>/dev/null || head -c 200 "$tmp/out.json")" >&2
  exit 1
fi

exit 0
