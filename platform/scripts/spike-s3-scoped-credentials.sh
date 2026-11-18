#!/usr/bin/env bash
# Spike for stardelt#135 (Q3 of stardelt#75): measure the shipped SeaweedFS S3 gateway.
#
#   Q1  can one static identity be restricted to one bucket?
#   Q2  does the gateway answer an STS AssumeRole at all, and can such a session be
#       restricted to one bucket?
#   Q3  what does the platform ask Lakekeeper for today (read-only: two files, one grep,
#       plus a read-only kubectl lookup of the lab's Catalogs when a cluster is reachable)?
#
# Every answer is one `Q<n>: ...` line on stdout; detail lines start with `  - `. A
# "not supported" / "absent" is a successful measurement: the script exits 0 either way.
# No secret is printed, nothing is written to a cluster, and the container and temp dir
# are removed on exit (including SIGINT).
#
# Docker is required; the image and digest are the ones the lab runs.
set -euo pipefail

IMAGE="chrislusf/seaweedfs:4.25@sha256:c42a5268ca13fcb65e0fae925886b107f4bf294d8db15e1be5509d55104eb509"
NAME="s3-scope-spike"
PORT="18333"
ENDPOINT="http://127.0.0.1:${PORT}"
# Throwaway credentials of the spike only; never a real key pair.
FULL_AK="SPIKEFULLACCESS0001"
FULL_SK="spike-full-secret-key-000000000001"
SCOPED_AK="SPIKESCOPEDACCESS01"
SCOPED_SK="spike-scoped-secret-key-000000001"
SIGNING_KEY="$(printf 'spike-signing-key-0000000000000001' | base64 -w0)"
ROLE_ARN="arn:aws:iam::000000000000:role/spike"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/s3-scope-spike.XXXXXX")"
body="$tmp/body"

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

# --- the gateway ------------------------------------------------------------------------
write_s3_config() { # <scoped identity JSON object>
  cat > "$tmp/s3.json" <<EOF
{"identities":[{"name":"full","credentials":[{"accessKey":"$FULL_AK","secretKey":"$FULL_SK"}],"actions":["Admin","Read","Write"]},$1]}
EOF
}

start_gateway() { # [extra `server` arguments]
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --rm --name "$NAME" -p "${PORT}:8333" -v "$tmp:/spike" \
    "$IMAGE" server -dir=/data -s3 -s3.port=8333 -s3.config=/spike/s3.json "$@" >/dev/null
  local code
  for _ in $(seq 1 60); do
    # The root answers 403 without credentials: any HTTP code means the gateway is up.
    code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "$ENDPOINT/" 2>/dev/null)" || code="000"
    if [ "${code:-000}" != "000" ]; then return 0; fi
    sleep 1
  done
  echo "the gateway did not answer within 60s" >&2
  exit 1
}

# s3 <ak> <sk> <method> <path> [data] -> status code; the body lands in $body
s3() {
  local ak="$1" sk="$2" method="$3" path="$4" data="${5-}" code
  if [ -n "$data" ]; then
    code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:s3" \
      --user "$ak:$sk" -X "$method" --data-binary "$data" "$ENDPOINT$path" 2>/dev/null)" || code="000"
  else
    code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:s3" \
      --user "$ak:$sk" -X "$method" "$ENDPOINT$path" 2>/dev/null)" || code="000"
  fi
  echo "${code:-000}"
}

create_buckets() {
  echo "  - buckets as full: PUT /spike-a $(s3 "$FULL_AK" "$FULL_SK" PUT /spike-a), PUT /spike-b $(s3 "$FULL_AK" "$FULL_SK" PUT /spike-b)"
}

# scoped_put_get -> "put-a get-a put-b" for the scoped identity
scoped_put_get() {
  printf '%s %s %s' \
    "$(s3 "$SCOPED_AK" "$SCOPED_SK" PUT /spike-a/hello.txt 'hello from spike-a')" \
    "$(s3 "$SCOPED_AK" "$SCOPED_SK" GET /spike-a/hello.txt)" \
    "$(s3 "$SCOPED_AK" "$SCOPED_SK" PUT /spike-b/denied.txt 'not allowed')"
}

# --- Q1: bucket-scoped static identity --------------------------------------------------
echo "Q1: measuring scoped static identity"

write_s3_config '{"name":"scoped-a","credentials":[{"accessKey":"'"$SCOPED_AK"'","secretKey":"'"$SCOPED_SK"'"}],"actions":["Read:spike-a","Write:spike-a"]}'
start_gateway
create_buckets
formA="$(scoped_put_get)"
echo "  - Read:<bucket>  (actions [Read:spike-a, Write:spike-a]): put-a/get-a/put-b = $formA"

write_s3_config '{"name":"scoped-a","credentials":[{"accessKey":"'"$SCOPED_AK"'","secretKey":"'"$SCOPED_SK"'"}],"actions":["Read","Write"],"buckets":["spike-a"]}'
start_gateway
create_buckets
formB="$(scoped_put_get)"
echo "  - buckets[]      (actions [Read, Write], buckets [spike-a]): put-a/get-a/put-b = $formB"

q1="not supported"
case "$formA" in "200 200 403") q1="supported: Read:<bucket>" ;; esac
case "$formB" in
  "200 200 403") if [ "$q1" = "not supported" ]; then q1="supported: buckets[]"; fi ;;
esac
echo "Q1: scoped identities $q1"

# --- Q2: STS AssumeRole -----------------------------------------------------------------
sts_probe() { # <label> <curl args...> -> prints "label => code", body in $body
  local label="$1" code
  shift
  code="$(curl -sS -o "$body" -w '%{http_code}' "$@" 2>/dev/null)" || code="000"
  echo "  - $label => ${code:-000} $(head -n 1 "$body" 2>/dev/null | cut -c1-140)"
  echo "${code:-000}" > "$tmp/last-code"
}
sts_signed() { # <ak> <sk> <method> <path> [data] -> code
  local ak="$1" sk="$2" method="$3" path="$4" data="${5-}" code
  if [ -n "$data" ]; then
    code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:sts" \
      --user "$ak:$sk" -X "$method" --data-urlencode "$data" "$ENDPOINT$path" 2>/dev/null)" || code="000"
  else
    code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:sts" \
      --user "$ak:$sk" -X "$method" "$ENDPOINT$path" 2>/dev/null)" || code="000"
  fi
  echo "${code:-000}"
}
creds() { # reads access key, secret and session token out of $body; values are never printed
  S_AK="$(sed -n 's:.*<AccessKeyId>\([^<]*\)</AccessKeyId>.*:\1:p' "$body" | head -n1)"
  S_SK="$(sed -n 's:.*<SecretAccessKey>\([^<]*\)</SecretAccessKey>.*:\1:p' "$body" | head -n1)"
  S_TOK="$(sed -n 's:.*<SessionToken>\([^<]*\)</SessionToken>.*:\1:p' "$body" | head -n1)"
}
session_s3() { # <method> <path> [data] -> code, signed with the session in S_AK/S_SK/S_TOK
  local method="$1" path="$2" data="${3-}" code
  if [ -n "$data" ]; then
    code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:s3" \
      --user "$S_AK:$S_SK" -H "x-amz-security-token: $S_TOK" -X "$method" \
      --data-binary "$data" "$ENDPOINT$path" 2>/dev/null)" || code="000"
  else
    code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:s3" \
      --user "$S_AK:$S_SK" -H "x-amz-security-token: $S_TOK" -X "$method" \
      "$ENDPOINT$path" 2>/dev/null)" || code="000"
  fi
  echo "${code:-000}"
}

session_codes() { # put-a get-a put-b get-b for the session in S_AK/S_SK/S_TOK
  printf '%s %s %s %s' \
    "$(session_s3 PUT /spike-a/session.txt 'session')" \
    "$(session_s3 GET /spike-a/session.txt)" \
    "$(session_s3 PUT /spike-b/session.txt 'session')" \
    "$(session_s3 GET /spike-b/session.txt)"
}

echo "Q2: probing the STS surface, first with the config the lab ships (identities only)"
write_s3_config '{"name":"unused","credentials":[{"accessKey":"SPIKEUNUSED0001","secretKey":"spike-unused-secret-key-0000000001"}],"actions":["Read","Write"]}'
start_gateway
sts_probe "unsigned GET /?Action=AssumeRole" "$ENDPOINT/?Action=AssumeRole&Version=2011-06-15"
sts_probe "unsigned GET /sts?Action=AssumeRole" "$ENDPOINT/sts?Action=AssumeRole&Version=2011-06-15"
sts_probe "unsigned GET /?Action=GetSessionToken" "$ENDPOINT/?Action=GetSessionToken&Version=2011-06-15"
lab_signed="$(sts_signed "$FULL_AK" "$FULL_SK" POST "/?Action=AssumeRole&Version=2011-06-15&RoleArn=$ROLE_ARN&RoleSessionName=spike")"
echo "  - signed POST /?Action=AssumeRole => $lab_signed $(head -n 1 "$body" | cut -c1-140)"
echo "  - gateway log: $(docker logs "$NAME" 2>&1 | grep -c 'Failed to load IAM configuration') × 'Failed to load IAM configuration' (no sts.signingKey)"

echo "Q2: same probe with -s3.iam.config (sts.signingKey, a role, a policy)"
cat > "$tmp/iam.json" <<EOF
{"sts":{"signingKey":"$SIGNING_KEY"},"policy":{"storeType":"memory","defaultEffect":"Deny"},
 "policies":[{"name":"spike-a","document":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::spike-a","arn:aws:s3:::spike-a/*"]}]}}],
 "roles":[{"roleName":"spike","roleArn":"$ROLE_ARN","trustPolicy":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["sts:AssumeRole"]}]},"attachedPolicies":["spike-a"]}]}
EOF
start_gateway "-s3.iam.config=/spike/iam.json"
create_buckets
iam_signed="$(sts_signed "$FULL_AK" "$FULL_SK" POST "/?Action=AssumeRole&Version=2011-06-15&RoleArn=$ROLE_ARN&RoleSessionName=spike")"
echo "  - signed POST /?Action=AssumeRole (query parameters) => $iam_signed"
creds
plain_role="not tested: AssumeRole returned no credentials"
if [ -n "$S_AK" ] && [ -n "$S_SK" ] && [ -n "$S_TOK" ]; then
  plain_role="$(session_codes)"
  echo "  - with the role's own policy (spike-a only): put-a get-a put-b get-b = $plain_role"
fi

# The AWS shape that scopes a session: an inline session policy on AssumeRole.
SESSION_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::spike-a","arn:aws:s3:::spike-a/*"]}]}'
body="$tmp/body"
policy_code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:sts" --user "$FULL_AK:$FULL_SK" \
  -X POST -d "Action=AssumeRole&Version=2011-06-15&RoleArn=$ROLE_ARN&RoleSessionName=spike" \
  --data-urlencode "Policy=$SESSION_POLICY" "$ENDPOINT/" 2>/dev/null)" || policy_code="000"
echo "  - signed POST / with an inline session Policy => ${policy_code:-000}"
creds
scoped_session="not tested: AssumeRole with a session Policy returned no credentials"
if [ -n "$S_AK" ] && [ -n "$S_SK" ] && [ -n "$S_TOK" ]; then
  scoped_session="$(session_codes)"
  echo "  - with the session policy (spike-a only): put-a get-a put-b get-b = $scoped_session"
fi

case "$scoped_session" in
  "200 200 403 403") q2="present: POST /?Action=AssumeRole, SigV4-signed, with -s3.iam.config (sts.signingKey + role); a session Policy scopes the session to one bucket" ;;
  *)
    if [ "${iam_signed:-000}" = "200" ]; then
      q2="present: POST /?Action=AssumeRole, SigV4-signed, with -s3.iam.config (sts.signingKey + role); bucket scoping of the session not shown"
    elif [ "${lab_signed:-000}" = "200" ]; then
      q2="present: POST /?Action=AssumeRole, SigV4-signed, with the identities-only config"
    else
      q2="absent"
    fi ;;
esac
echo "Q2: sts $q2"

# --- Q3: the Lakekeeper side (read-only) -------------------------------------------------
CHART_LK="$(awk '/^ *- name: lakekeeper$/{f=1;next} f&&/version:/{print $2;exit}' "$ROOT/platform/chart/Chart.yaml")"
BOOTSTRAP="operator/crates/stardelt-operator/src/lakekeeper_bootstrap.sh"
WH_LINE="$(grep -n 'warehouse-name' "$ROOT/$BOOTSTRAP" | head -n1 | cut -d: -f1)"
CRED_LINE="$(grep -n 'storage-credential' "$ROOT/$BOOTSTRAP" | head -n1 | cut -d: -f1)"
CRD_LINE="$(grep -n 'credential_secret_ref' "$ROOT/operator/crates/stardelt-operator/src/crd.rs" | head -n1 | cut -d: -f1)"
CONSUMERS="$(grep -rl 'credential_secret_ref' --include='*.rs' "$ROOT/operator" | grep -cv '/src/crd.rs$' || true)"
echo "  - lakekeeper chart $CHART_LK"
echo "  - one warehouse 'warehouse' with one storage credential: $BOOTSTRAP:$WH_LINE (credential at :$CRED_LINE)"
echo "  - Catalog.spec.storage.credentialSecretRef is declared (operator/crates/stardelt-operator/src/crd.rs:$CRD_LINE) and read by $CONSUMERS Rust file(s)"
if command -v kubectl >/dev/null 2>&1 && kubectl get catalogs -A >/dev/null 2>&1; then
  echo "  - lab: $(kubectl get catalogs -A --no-headers | wc -l | tr -d ' ') Catalogs, $(kubectl get catalogs -A -o json | grep -c '"credentialSecretRef"' || true) set a per-catalog credential, lakehouses: $(kubectl get lakehouses -A --no-headers 2>/dev/null | wc -l | tr -d ' ')"
else
  echo "  - lab: not tested (no reachable cluster)"
fi
echo "Q3: storage one credential for every catalog ($BOOTSTRAP:$WH_LINE)"
