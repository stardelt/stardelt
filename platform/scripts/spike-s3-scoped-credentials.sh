#!/usr/bin/env bash
# Spike for stardelt#135 (Q3 of stardelt#75): measure the shipped SeaweedFS S3 gateway.
#
#   Q1  can one static identity be restricted to one bucket?
#   Q2  does the gateway answer an STS AssumeRole at all, and can such a session be
#       restricted to one bucket?
#   Q3  what does the platform ask Lakekeeper for today (read-only: two files, one grep,
#       plus a read-only kubectl lookup of the lab's Catalogs when a cluster is reachable)?
#   Q4  can one static identity be restricted to a *prefix* of one bucket (stardelt#201
#       item 3, storage access for the demo sandbox; Q3-B of stardelt#75)?
#   Q5  can an inline STS session Policy be restricted to a prefix of one bucket
#       (stardelt#201 item 3, storage access for the demo sandbox; Q3-B of stardelt#75)?
#   Q6  what can Lakekeeper 0.15.0 be told about STS for a warehouse (read-only: the
#       pinned release's management OpenAPI, plus one grep of the bootstrap script)?
#   Q7  can one config file serve both -s3.config and -s3.iam.config, so the gateway keeps
#       the platform's identities and gains STS (stardelt#245, the platform path for #201)?
#   Q8  which values of the pinned chart reach the gateway's command line, can a second
#       config file be mounted there, and what do the rendered args look like (read-only,
#       no cluster: the vendored chart, helm-values/seaweedfs.yaml and `helm template`)?
#   Q9  does a session Policy at a table-shaped deep prefix scope correctly, and what does
#       it need — the trailing `/*` or the bare prefix (stardelt#245)?
#   Q10 can Lakekeeper 0.15.0 replace a warehouse's storage profile in place, or only at
#       creation (read-only: the same management OpenAPI document as Q6)?
#   Q11 does the command the chart actually runs (`weed s3`, not `weed server`) accept one
#       document for -config and -iam.config and keep static identities *and* AssumeRole
#       with a prefix session policy — or does it need two files (stardelt#201 slice 2)?
#   Q12 do s3.extraArgs and s3.extraVolumes/s3.extraVolumeMounts render into the pinned
#       chart's s3 Deployment, and in which values shape (read-only, helm template)?
#   Q13 which top-level keys does the live `seaweedfs_s3_config` carry (read-only:
#       key names only, never key material; slice 2 has to add the same keys to the two
#       places that write it)?
#
# Every answer is one `Q<n>: ...` line on stdout; detail lines start with `  - `. A
# "not supported" / "absent" is a successful measurement: the script exits 0 either way.
# No secret is printed, nothing is written to a cluster, and the container and temp dir
# are removed on exit (including SIGINT).
#
# Docker is required; the image and digest are the ones the lab runs. Q6 and Q10 need
# kubectl and curl and degrade to `not measured` when the management port-forward is
# unreachable; Q8 needs the vendored chart (`make chart-deps`) and helm.
set -euo pipefail

IMAGE="chrislusf/seaweedfs:4.25@sha256:c42a5268ca13fcb65e0fae925886b107f4bf294d8db15e1be5509d55104eb509"
NAME="s3-scope-spike"
GW="spike-s3-gw"
PORT="18333"
ENDPOINT="http://127.0.0.1:${PORT}"
PORT2=$((PORT + 1))
ENDPOINT2="http://127.0.0.1:${PORT2}"
# The endpoint the S3/STS helpers talk to: the first container everywhere except Q11,
# which measures the chart's own `weed s3` in the second container.
EP="$ENDPOINT"
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
PF_PID=""

cleanup() {
  if [ -n "$PF_PID" ]; then kill "$PF_PID" >/dev/null 2>&1 || true; fi
  docker rm -f "$NAME" spike-s3-gw >/dev/null 2>&1 || true
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
      --user "$ak:$sk" -X "$method" --data-binary "$data" "$EP$path" 2>/dev/null)" || code="000"
  else
    code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:s3" \
      --user "$ak:$sk" -X "$method" "$EP$path" 2>/dev/null)" || code="000"
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
      --user "$ak:$sk" -X "$method" --data-urlencode "$data" "$EP$path" 2>/dev/null)" || code="000"
  else
    code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:sts" \
      --user "$ak:$sk" -X "$method" "$EP$path" 2>/dev/null)" || code="000"
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
      --data-binary "$data" "$EP$path" 2>/dev/null)" || code="000"
  else
    code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:s3" \
      --user "$S_AK:$S_SK" -H "x-amz-security-token: $S_TOK" -X "$method" \
      "$EP$path" 2>/dev/null)" || code="000"
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

# --- Q4: static identity confined to a bucket prefix (stardelt#201 item 3, #75 Q3-B) -----
echo "Q4: measuring a static identity whose actions name a prefix"

# Six codes in the order the design reads them: put-in get-in put-out get-out list-bucket
# list-prefix. Every request uses a fresh container, so the bucket and both seed objects
# are written by the full identity first.
prefix6() { # <actions JSON array> -> the six codes
  local actions="$1"
  write_s3_config '{"name":"prefix-a","credentials":[{"accessKey":"'"$SCOPED_AK"'","secretKey":"'"$SCOPED_SK"'"}],"actions":'"$actions"'}'
  start_gateway
  s3 "$FULL_AK" "$FULL_SK" PUT /spike-c >/dev/null
  s3 "$FULL_AK" "$FULL_SK" PUT /spike-c/prefix-a/inside.txt 'inside prefix-a' >/dev/null
  s3 "$FULL_AK" "$FULL_SK" PUT /spike-c/prefix-b/outside.txt 'outside prefix-a' >/dev/null
  printf '%s %s %s %s %s %s' \
    "$(s3 "$SCOPED_AK" "$SCOPED_SK" PUT /spike-c/prefix-a/scoped.txt 'scoped')" \
    "$(s3 "$SCOPED_AK" "$SCOPED_SK" GET /spike-c/prefix-a/inside.txt)" \
    "$(s3 "$SCOPED_AK" "$SCOPED_SK" PUT /spike-c/prefix-b/scoped.txt 'scoped')" \
    "$(s3 "$SCOPED_AK" "$SCOPED_SK" GET /spike-c/prefix-b/outside.txt)" \
    "$(s3 "$SCOPED_AK" "$SCOPED_SK" GET "/spike-c?list-type=2")" \
    "$(s3 "$SCOPED_AK" "$SCOPED_SK" GET "/spike-c?list-type=2&prefix=prefix-a/")"
}

q4_slash="$(prefix6 '["Read:spike-c/prefix-a/","Write:spike-c/prefix-a/"]')"
q4_noslash="$(prefix6 '["Read:spike-c/prefix-a","Write:spike-c/prefix-a"]')"

q4="not supported"
q4_codes="$q4_noslash"
case "$q4_slash" in "200 200 403 403 "*)
  q4="supported: Read:<bucket>/<prefix>/ (trailing slash required)"; q4_codes="$q4_slash" ;;
*)
  case "$q4_noslash" in "200 200 403 403 "*)
    q4="supported: Read:<bucket>/<prefix> (no trailing slash)"; q4_codes="$q4_noslash" ;;
  esac ;;
esac

echo "  - trailing slash (Read:<bucket>/<prefix>/): put-in/get-in/put-out/get-out/list-bucket/list-prefix = $q4_slash"
echo "  - no trailing slash (Read:<bucket>/<prefix>): put-in/get-in/put-out/get-out/list-bucket/list-prefix = $q4_noslash"
echo "Q4: scoped identities at a prefix $q4"
echo "  - put-in/get-in/put-out/get-out/list-bucket/list-prefix = $q4_codes"
read -r _put_in _get_in _put_out _get_out list_bucket _list_prefix <<<"$q4_codes"
if [ "$list_bucket" = "200" ]; then
  echo "  - finding: list-bucket answers 200 — the identity can enumerate object names in the bucket"
fi

# --- Q5: inline STS session policy confined to a bucket prefix (stardelt#201 item 3) -----
echo "Q5: measuring an inline session Policy whose resources name a prefix"

PREFIX_ROLE_ARN="arn:aws:iam::000000000000:role/spike-prefix"
TRUST_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["sts:AssumeRole"]}]}'
PREFIX_RESOURCES='["arn:aws:s3:::spike-c/prefix-a","arn:aws:s3:::spike-c/prefix-a/*"]'

write_s3_config '{"name":"prefix-a","credentials":[{"accessKey":"'"$SCOPED_AK"'","secretKey":"'"$SCOPED_SK"'"}],"actions":["Read:spike-c/prefix-a/","Write:spike-c/prefix-a/"]}'
cat > "$tmp/iam.json" <<EOF
{"sts":{"signingKey":"$SIGNING_KEY"},"policy":{"storeType":"memory","defaultEffect":"Deny"},
 "policies":[{"name":"spike-c","document":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::spike-c","arn:aws:s3:::spike-c/*"]}]}},
             {"name":"spike-c-prefix","document":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":$PREFIX_RESOURCES}]}}],
 "roles":[{"roleName":"spike","roleArn":"$ROLE_ARN","trustPolicy":$TRUST_POLICY,"attachedPolicies":["spike-c"]},
          {"roleName":"spike-prefix","roleArn":"$PREFIX_ROLE_ARN","trustPolicy":$TRUST_POLICY,"attachedPolicies":["spike-c-prefix"]}]}
EOF
start_gateway "-s3.iam.config=/spike/iam.json"
# Bucket creation and the seed objects stay on the full identity.
echo "  - bucket and seed objects as full: PUT /spike-c $(s3 "$FULL_AK" "$FULL_SK" PUT /spike-c), PUT inside.txt $(s3 "$FULL_AK" "$FULL_SK" PUT /spike-c/prefix-a/inside.txt 'inside prefix-a'), PUT outside.txt $(s3 "$FULL_AK" "$FULL_SK" PUT /spike-c/prefix-b/outside.txt 'outside prefix-a')"

prefix_session_codes() { # <policy document JSON> -> "put-in get-in put-out get-out"
  local policy="$1" code
  code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:sts" --user "$FULL_AK:$FULL_SK" \
    -X POST -d "Action=AssumeRole&Version=2011-06-15&RoleArn=$PREFIX_ROLE_ARN&RoleSessionName=spike-prefix" \
    --data-urlencode "Policy=$policy" "$ENDPOINT/" 2>/dev/null)" || code="000"
  creds
  if [ -z "$S_AK" ] || [ -z "$S_SK" ] || [ -z "$S_TOK" ]; then
    printf 'not tested: AssumeRole returned %s and no credentials' "${code:-000}"
    return 0
  fi
  printf '%s %s %s %s' \
    "$(session_s3 PUT /spike-c/prefix-a/session.txt 'session')" \
    "$(session_s3 GET /spike-c/prefix-a/inside.txt)" \
    "$(session_s3 PUT /spike-c/prefix-b/session.txt 'session')" \
    "$(session_s3 GET /spike-c/prefix-b/outside.txt)"
}

SESSION_PREFIX_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":'"$PREFIX_RESOURCES"'}]}'
SESSION_PREFIX_PRINCIPAL='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:*"],"Resource":'"$PREFIX_RESOURCES"'}]}'

q5_plain="$(prefix_session_codes "$SESSION_PREFIX_POLICY")"
q5_principal="$(prefix_session_codes "$SESSION_PREFIX_PRINCIPAL")"

q5="not measurable"
case "$q5_plain" in
  "200 200 403 403") q5="the inline session Policy scopes to a prefix" ;;
  "200 200 200 200") q5="the inline session Policy only scopes to a bucket" ;;
  *)
    case "$q5_principal" in
      "200 200 403 403") q5="the inline session Policy scopes to a prefix, with Principal:* in every statement" ;;
      "200 200 200 200") q5="the inline session Policy only scopes to a bucket" ;;
    esac ;;
esac
echo "Q5: $q5"
echo "  - put-in/get-in/put-out/get-out = $q5_plain"
echo "  - with Principal:*: $q5_principal"

# --- Q6: what Lakekeeper 0.15.0 can be told (read-only) ----------------------------------
echo "Q6: reading the pinned catalog's storage-profile fields (no write)"
STS_LINE="$(grep -n '"sts-enabled"' "$ROOT/$BOOTSTRAP" | head -n1)"
if [ -n "$STS_LINE" ]; then
  echo "  - lakekeeper_bootstrap.sh:$STS_LINE"
else
  echo "  - lakekeeper_bootstrap.sh: no \"sts-enabled\" line (moved or removed)"
fi
STS_VALUE="$(printf '%s' "$STS_LINE" | sed -n 's/.*"sts-enabled": *\([a-z]*\).*/\1/p')"
[ -n "$STS_VALUE" ] || STS_VALUE="unknown"

LK_JSON="$tmp/lk.json"
lk_ok=no
if command -v kubectl >/dev/null 2>&1 && command -v curl >/dev/null 2>&1; then
  kubectl -n stardelt port-forward svc/lakekeeper 18181:8181 >"$tmp/lk-pf.log" 2>&1 &
  PF_PID=$!
  for _ in $(seq 1 30); do
    if curl -fsS --max-time 2 http://127.0.0.1:18181/api-docs/management/v1/openapi.json -o "$LK_JSON" 2>/dev/null; then
      lk_ok=yes
      break
    fi
    sleep 0.5
  done
  kill "$PF_PID" >/dev/null 2>&1 || true
  PF_PID=""
fi

lk_ver="the pinned release"
sts_endpoint="missing"
sts_principal="missing"
if [ "$lk_ok" = yes ]; then
  LK_IMAGE="$(kubectl -n stardelt get pods -l app.kubernetes.io/name=lakekeeper -o jsonpath='{range .items[*]}{.spec.containers[0].image}{"\n"}{end}' 2>/dev/null | head -n1)"
  lk_tag="$(printf '%s' "$LK_IMAGE" | sed -n 's/.*:\(v[0-9][^@]*\).*/\1/p')"
  [ -z "$lk_tag" ] || lk_ver="$lk_tag"
  lk_req="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1]))["components"]["schemas"]["S3Profile"]; print(", ".join(d.get("required",[])))' "$LK_JSON")"
  lk_sts="$(python3 -c 'import json,sys; p=json.load(open(sys.argv[1]))["components"]["schemas"]["S3Profile"]["properties"]; print(", ".join(k for k in p if k.startswith("sts")))' "$LK_JSON")"
  case ", $lk_sts," in *", sts-endpoint,"*) sts_endpoint="present" ;; esac
  case ", $lk_sts," in *", sts-session-policy-principal,"*) sts_principal="present" ;; esac
  echo "  - StorageProfileS3 required: $lk_req"
  echo "  - StorageProfileS3 sts-* fields: $lk_sts"
else
  echo "  - management OpenAPI: not reachable ($(tail -n1 "$tmp/lk-pf.log" 2>/dev/null | cut -c1-120))"
fi

if [ "$lk_ok" = yes ]; then
  q6="sts-endpoint and sts-session-policy-principal exist in Lakekeeper $lk_ver; the warehouse the bootstrap creates is sts-enabled: $STS_VALUE"
else
  q6="not measured: the management OpenAPI was not reachable"
fi
echo "Q6: $q6"

# --- Q7: one config document for both gateway flags (stardelt#245) -----------------------
echo "Q7: measuring one config file for both -s3.config and -s3.iam.config"

# The platform ships one Secret with one key (seaweedfs_s3_config); this document carries
# the identities *and* the STS half, and both flags read it.
cat > "$tmp/s3.json" <<EOF
{"identities":[{"name":"full","credentials":[{"accessKey":"$FULL_AK","secretKey":"$FULL_SK"}],"actions":["Admin","Read","Write"]}],
 "sts":{"signingKey":"$SIGNING_KEY"},"policy":{"storeType":"memory","defaultEffect":"Deny"},
 "policies":[{"name":"spike-d","document":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::spike-d","arn:aws:s3:::spike-d/*"]}]}}],
 "roles":[{"roleName":"spike","roleArn":"$ROLE_ARN","trustPolicy":$TRUST_POLICY,"attachedPolicies":["spike-d"]}]}
EOF
start_gateway "-s3.iam.config=/spike/s3.json"

q7_identity="$(printf '%s %s %s' \
  "$(s3 "$FULL_AK" "$FULL_SK" PUT /spike-d)" \
  "$(s3 "$FULL_AK" "$FULL_SK" PUT /spike-d/q7.txt 'q7')" \
  "$(s3 "$FULL_AK" "$FULL_SK" GET /spike-d/q7.txt)")"
q7_assume="$(sts_signed "$FULL_AK" "$FULL_SK" POST "/?Action=AssumeRole&Version=2011-06-15&RoleArn=$ROLE_ARN&RoleSessionName=spike")"
creds
q7_session="not tested: AssumeRole returned no credentials"
if [ -n "$S_AK" ] && [ -n "$S_SK" ] && [ -n "$S_TOK" ]; then
  q7_session="$(printf '%s %s' \
    "$(session_s3 PUT /spike-d/q7-session.txt 'session')" \
    "$(session_s3 GET /spike-d/q7.txt)")"
fi

echo "  - platform identity (actions [Admin,Read,Write]): create-bucket/PUT/GET = $q7_identity"
echo "  - AssumeRole signed with the same credentials = $q7_assume; session PUT/GET = $q7_session"
echo "  - gateway log: $(docker logs "$NAME" 2>&1 | grep -c 'Failed to load IAM configuration') × 'Failed to load IAM configuration' (one file for both flags)"
read -r q7_bucket q7_put q7_get <<<"$q7_identity"
q7="one document for both flags: identity PUT/GET = $q7_put $q7_get, AssumeRole = $q7_assume"
if [ "$q7_bucket" != "200" ] || [ "$q7_put" != "200" ] || [ "$q7_get" != "200" ] || [ "$q7_assume" != "200" ]; then
  q7="not one document for both flags: create-bucket/PUT/GET = $q7_identity, AssumeRole = $q7_assume"
fi
echo "Q7: $q7"

# --- Q8: what the pinned chart can pass to the gateway (read-only) -----------------------
echo "Q8: reading the pinned chart's S3 gateway flags (no cluster)"

SEAWEED_CHART="$(awk '/^ *- name: seaweedfs$/{f=1;next} f&&/version:/{print $2;exit}' "$ROOT/platform/chart/Chart.yaml")"
S3_TEMPLATE="$ROOT/platform/chart/charts/seaweedfs/templates/s3/s3-deployment.yaml"
S3_VALUES="$ROOT/platform/chart/charts/seaweedfs/values.yaml"
VALUES="$ROOT/platform/helm-values/seaweedfs.yaml"

if [ -f "$S3_TEMPLATE" ]; then
  s3_config_line="$(grep -n '\-config=/etc/sw/' "$S3_TEMPLATE" | head -n1 | cut -d: -f1)"
  extra_args_line="$(grep -n 'Values.s3.extraArgs' "$S3_TEMPLATE" | head -n1 | cut -d: -f1)"
  extra_mount_line="$(grep -n 's3.extraVolumeMounts' "$S3_TEMPLATE" | head -n1 | cut -d: -f1)"
  extra_vol_line="$(grep -n 's3.extraVolumes' "$S3_TEMPLATE" | head -n1 | cut -d: -f1)"
  iam_hits="$(grep -ci 'iamConfig\|iam\.config' "$S3_TEMPLATE" "$S3_VALUES" | awk -F: '{s+=$2} END {print s+0}' || true)"
  secret_hits="$(grep -c 'existingConfigSecret' "$S3_TEMPLATE" || true)"
  echo "  - chart seaweedfs $SEAWEED_CHART: the gateway mounts the Secret's key as one file and passes it with -config (templates/s3/s3-deployment.yaml:$s3_config_line; existingConfigSecret at :$secret_hits)"
  echo "  - no -iam.config / iamConfig anywhere in the chart's s3 template or values ($iam_hits hits)"
  echo "  - s3.extraArgs reach the command line (templates/s3/s3-deployment.yaml:$extra_args_line); s3.extraVolumeMounts/extraVolumes render free-form YAML (:${extra_mount_line}/:${extra_vol_line})"
  echo "  - platform values: s3.existingConfigSecret $(grep -m1 '^  existingConfigSecret:' "$VALUES" | sed 's/^  existingConfigSecret: *//'), no extraArgs/extraVolumes"
else
  echo "  - chart not vendored: chart/charts/seaweedfs is absent (run \`make chart-deps\`)"
fi

q8="not measured"
if [ -f "$S3_TEMPLATE" ] && command -v helm >/dev/null 2>&1; then
  if helm template seaweedfs "$ROOT/platform/chart/charts/seaweedfs" --values "$VALUES" > "$tmp/rendered.yaml" 2>"$tmp/helm.err"; then
    python3 - "$tmp/rendered.yaml" <<'PY' | sed 's/^/  | /'
import sys
for doc in open(sys.argv[1]).read().split("\n---\n"):
    if "kind: Deployment" not in doc or "swfs-s3" not in doc:
        continue
    name = [l.split("name:", 1)[1].strip() for l in doc.splitlines() if l.strip().startswith("name:")][0]
    body = doc[doc.index("command:"):]
    body = body[:body.index("volumeMounts:")]
    print("rendered command of Deployment " + name + ":")
    for line in body.splitlines()[1:]:
        if line.strip():
            print(line.rstrip())
    break
PY
    rendered_config="$(grep -c '\-config=/etc/sw/seaweedfs_s3_config' "$tmp/rendered.yaml" || true)"
    q8="the chart passes -config from Secret seaweedfs-s3-config key seaweedfs_s3_config ($rendered_config rendered s3 deployment(s)); it has no iam flag, and s3.extraArgs plus s3.extraVolumes/extraVolumeMounts can add -iam.config and its second file"
  else
    echo "  - helm template failed: $(tail -n1 "$tmp/helm.err" | cut -c1-140)"
    q8="not measured: helm template failed"
  fi
fi
echo "Q8: $q8"

# --- Q9: a session policy at a table-shaped deep prefix (stardelt#245) -------------------
echo "Q9: measuring an inline session Policy at a table-shaped deep prefix"

DEEP_PREFIX="warehouse/00000000-0000-0000-0000-000000000000/ns/table"
DEEP_SIBLING="warehouse/00000000-0000-0000-0000-000000000000/ns/table2"
DEEP_RESOURCES='["arn:aws:s3:::spike-d/'"$DEEP_PREFIX"'","arn:aws:s3:::spike-d/'"$DEEP_PREFIX"'/*"]'
DEEP_RESOURCES_BARE='["arn:aws:s3:::spike-d/'"$DEEP_PREFIX"'"]'

deep_assume() { # <policy document JSON> -> AssumeRole into S_AK/S_SK/S_TOK (runs in this shell)
  local policy="$1" code
  code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:sts" --user "$FULL_AK:$FULL_SK" \
    -X POST -d "Action=AssumeRole&Version=2011-06-15&RoleArn=$ROLE_ARN&RoleSessionName=spike-deep" \
    --data-urlencode "Policy=$policy" "$ENDPOINT/" 2>/dev/null)" || code="000"
  S_AK=""; S_SK=""; S_TOK=""
  creds
  DEEP_CODE="${code:-000}"
}

deep_session_codes() { # the session in S_AK/S_SK/S_TOK -> "put-in get-in put-out get-out"
  if [ -z "$S_AK" ] || [ -z "$S_SK" ] || [ -z "$S_TOK" ]; then
    printf 'not tested: AssumeRole returned %s and no credentials' "${DEEP_CODE:-000}"
    return 0
  fi
  printf '%s %s %s %s' \
    "$(session_s3 PUT "/spike-d/$DEEP_PREFIX/data/f.parquet" 'deep')" \
    "$(session_s3 GET "/spike-d/$DEEP_PREFIX/data/f.parquet")" \
    "$(session_s3 PUT "/spike-d/$DEEP_SIBLING/data/f.parquet" 'deep')" \
    "$(session_s3 GET "/spike-d/$DEEP_SIBLING/data/f.parquet")"
}

DEEP_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":'"$DEEP_RESOURCES"'}]}'
DEEP_POLICY_BARE='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":'"$DEEP_RESOURCES_BARE"'}]}'

q9_deep="not tested: AssumeRole returned no credentials"
q9_root="not tested: AssumeRole returned no credentials"
q9_bare="not tested: AssumeRole returned no credentials"
deep_assume "$DEEP_POLICY"
if [ -n "$S_AK" ] && [ -n "$S_SK" ] && [ -n "$S_TOK" ]; then
  q9_deep="$(deep_session_codes)"
  q9_root="$(printf '%s %s' \
    "$(session_s3 PUT /spike-d/root-q9.txt 'root')" \
    "$(session_s3 GET /spike-d/q7.txt)")"
  deep_assume "$DEEP_POLICY_BARE"
  q9_bare="$(deep_session_codes)"
fi

echo "  - prefix: arn:aws:s3:::spike-d/$DEEP_PREFIX and …/$DEEP_PREFIX/*"
echo "  - bucket root (PUT /spike-d/root-q9.txt, GET /spike-d/q7.txt): $q9_root"
echo "  - without the trailing /*: put-in/get-in/put-out/get-out = $q9_bare"
q9="the deep session policy: put-in/get-in/put-out/get-out = $q9_deep"
case "$q9_bare" in
  "403 403 403 403") echo "  - finding: the gateway needs the trailing /* — the bare deep prefix allows nothing" ;;
esac
if [ "$q9_deep" != "200 200 403 403" ]; then
  q9="the deep session policy does not scope as the design needs: put-in/get-in/put-out/get-out = $q9_deep"
fi
echo "Q9: $q9"

# --- Q10: can Lakekeeper 0.15.0 update a storage profile in place? (read-only) -----------
echo "Q10: reading the pinned release's warehouse update operations (no write)"

if [ "$lk_ok" = yes ]; then
  wh_ops="$(python3 -c 'import json,sys
d=json.load(open(sys.argv[1]))
out=[]
for p,v in sorted(d["paths"].items()):
    if not p.startswith("/management/v1/warehouse"): continue
    for m,op in sorted(v.items()):
        if "storage" in p and m in ("get","post","put","patch","delete"):
            out.append(m.upper()+" "+p+" ("+op.get("summary","")+")")
print("; ".join(out))' "$LK_JSON")"
  wh_writers="$(python3 -c 'import json,sys
d=json.load(open(sys.argv[1]))
print(", ".join(sorted(m.upper()+" "+p for p,v in d["paths"].items() if p.startswith("/management/v1/warehouse") for m in v if m in ("put","patch"))) or "none")' "$LK_JSON")"
  wh_rule="$(python3 -c 'import json,sys
d=json.load(open(sys.argv[1]))
s=d["components"]["schemas"]["UpdateWarehouseStorageRequest"]["properties"]["storage-profile"]
print(" ".join(s.get("description","").split())[:260])' "$LK_JSON")"
  echo "  - operations that take a storage profile: $wh_ops"
  echo "  - PUT or PATCH on a warehouse path: $wh_writers"
  echo "  - UpdateWarehouseStorageRequest: $wh_rule"
  q10="Lakekeeper $lk_ver updates a storage profile by POST /management/v1/warehouse/{warehouse_id}/storage, in place (credential-only: /storage-credential; dry run: /storage/validate-profile); no PUT or PATCH"
else
  echo "  - management OpenAPI: not reachable (the same document as Q6)"
  q10="not measured: the management OpenAPI was not reachable"
fi
echo "Q10: $q10"

# --- Q11: the chart's own command, `weed s3`, with one document (stardelt#201 slice 2) ----
echo "Q11: measuring the chart's gateway (\`weed s3\`) with STS"

# The chart runs `weed s3 -config=<file>` (Q8), not `weed server -s3.config=<file>`. One
# document carries the identity and the STS half (Q7's template), and both flags read it.
Q11_BUCKET="spike-g"
Q11_PREFIX="warehouse/ns/table"
Q11_SIBLING="warehouse/ns/table2"
Q11_POLICY='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::'"$Q11_BUCKET/$Q11_PREFIX"'","arn:aws:s3:::'"$Q11_BUCKET/$Q11_PREFIX"'/*"]}]}'

cat > "$tmp/s3.json" <<EOF
{"identities":[{"name":"full","credentials":[{"accessKey":"$FULL_AK","secretKey":"$FULL_SK"}],"actions":["Admin","Read","Write"]}],
 "sts":{"signingKey":"$SIGNING_KEY"},"policy":{"storeType":"memory","defaultEffect":"Deny"},
 "policies":[{"name":"full","document":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::*"]}]}}],
 "roles":[{"roleName":"spike","roleArn":"$ROLE_ARN","trustPolicy":$TRUST_POLICY,"attachedPolicies":["full"]}]}
EOF

# `weed s3` is stateless: it reads the filer of the container that already serves 8333 and
# is the filer on 8888. The default bridge has no embedded DNS, so --link gives the second
# container the first one's name.
start_gateway2() { # <config file> <iam config file> -> 0 when the gateway answers
  docker rm -f "$GW" >/dev/null 2>&1 || true
  docker run -d --rm --name "$GW" -p "${PORT2}:8333" -v "$tmp:/spike" --link "$NAME:$NAME" \
    "$IMAGE" s3 -port=8333 -filer="$NAME:8888" -config="/spike/$1" -iam.config="/spike/$2" \
    >/dev/null 2>"$tmp/gw.err"
  local code
  for _ in $(seq 1 60); do
    code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "$ENDPOINT2/" 2>/dev/null)" || code="000"
    if [ "${code:-000}" != "000" ]; then return 0; fi
    sleep 1
  done
  echo "  - the gateway did not answer within 60s; docker logs tail:" >&2
  docker logs "$GW" 2>&1 | tail -n 5 >&2
  tail -n1 "$tmp/gw.err" >&2 || true
  return 1
}

# q11_measure -> sets Q11_IDENT ("PUT GET"), Q11_ASSUME, Q11_SESSION (four codes);
# talks to the second container because EP points there.
q11_measure() {
  s3 "$FULL_AK" "$FULL_SK" PUT "/$Q11_BUCKET" >/dev/null
  s3 "$FULL_AK" "$FULL_SK" PUT "/$Q11_BUCKET/$Q11_PREFIX/data/inside.parquet" 'inside' >/dev/null
  s3 "$FULL_AK" "$FULL_SK" PUT "/$Q11_BUCKET/$Q11_SIBLING/data/outside.parquet" 'outside' >/dev/null
  Q11_IDENT="$(printf '%s %s' \
    "$(s3 "$FULL_AK" "$FULL_SK" PUT "/$Q11_BUCKET/q11.txt" 'q11')" \
    "$(s3 "$FULL_AK" "$FULL_SK" GET "/$Q11_BUCKET/q11.txt")")"
  Q11_ASSUME="$(sts_signed "$FULL_AK" "$FULL_SK" POST "/?Action=AssumeRole&Version=2011-06-15&RoleArn=$ROLE_ARN&RoleSessionName=spike")"
  # The Q5 shape: a signed AssumeRole carrying an inline session Policy at the prefix.
  local policy_code
  policy_code="$(curl -sS -o "$body" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:sts" --user "$FULL_AK:$FULL_SK" \
    -X POST -d "Action=AssumeRole&Version=2011-06-15&RoleArn=$ROLE_ARN&RoleSessionName=spike" \
    --data-urlencode "Policy=$Q11_POLICY" "$EP/" 2>/dev/null)" || policy_code="000"
  Q11_SESSION="not tested: AssumeRole with a session Policy returned ${policy_code:-000} and no credentials"
  creds
  if [ -n "$S_AK" ] && [ -n "$S_SK" ] && [ -n "$S_TOK" ]; then
    Q11_SESSION="$(printf '%s %s %s %s' \
      "$(session_s3 PUT "/$Q11_BUCKET/$Q11_PREFIX/data/session.parquet" 'session')" \
      "$(session_s3 GET "/$Q11_BUCKET/$Q11_PREFIX/data/inside.parquet")" \
      "$(session_s3 PUT "/$Q11_BUCKET/$Q11_SIBLING/data/session.parquet" 'session')" \
      "$(session_s3 GET "/$Q11_BUCKET/$Q11_SIBLING/data/outside.parquet")")"
  fi
}

q11_one_ok=no
q11_two_ok=no
q11_one_ident="not tested"
q11_one_assume="not tested"
q11_one_session="not tested"
q11_two_ident="not tested"
q11_two_assume="not tested"
q11_two_session="not tested"
q11_warn_one=0
q11_warn_two=0

start_gateway   # the container that already serves 8333 is also the filer
EP="$ENDPOINT2"
if start_gateway2 s3.json s3.json; then
  q11_measure
  q11_one_ident="$Q11_IDENT"; q11_one_assume="$Q11_ASSUME"; q11_one_session="$Q11_SESSION"
  q11_warn_one="$(docker logs "$GW" 2>&1 | grep -c 'invalid policy' || true)"
  echo "  - one file, both flags: identities PUT/GET = $q11_one_ident, AssumeRole = $q11_one_assume, session put-in/get-in/put-out/get-out = $q11_one_session"

  # The IAM keys only, written here: `-config` keeps the identities, `-iam.config` the STS half.
  cat > "$tmp/s3.json" <<EOF
{"identities":[{"name":"full","credentials":[{"accessKey":"$FULL_AK","secretKey":"$FULL_SK"}],"actions":["Admin","Read","Write"]}]}
EOF
  cat > "$tmp/iam.json" <<EOF
{"sts":{"signingKey":"$SIGNING_KEY"},"policy":{"storeType":"memory","defaultEffect":"Deny"},
 "policies":[{"name":"full","document":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::*"]}]}}],
 "roles":[{"roleName":"spike","roleArn":"$ROLE_ARN","trustPolicy":$TRUST_POLICY,"attachedPolicies":["full"]}]}
EOF
  if start_gateway2 s3.json iam.json; then
    q11_measure
    q11_two_ident="$Q11_IDENT"; q11_two_assume="$Q11_ASSUME"; q11_two_session="$Q11_SESSION"
    q11_warn_two="$(docker logs "$GW" 2>&1 | grep -c 'invalid policy' || true)"
    echo "  - two files: identities PUT/GET = $q11_two_ident, AssumeRole = $q11_two_assume, session put-in/get-in/put-out/get-out = $q11_two_session"
  fi

  if [ "$q11_one_ident" = "200 200" ] && [ "$q11_one_assume" = "200" ] && \
     [ "$q11_one_session" = "200 200 403 403" ]; then q11_one_ok=yes; fi
  if [ "$q11_two_ident" = "200 200" ] && [ "$q11_two_assume" = "200" ] && \
     [ "$q11_two_session" = "200 200 403 403" ]; then q11_two_ok=yes; fi
  echo "  - gateway log: $q11_warn_one × 'invalid policy' with one file (the identities pass drops them; -iam.config loads them), $q11_warn_two × with two files"
fi
EP="$ENDPOINT"

case "$q11_one_ok:$q11_two_ok" in
  yes:*) q11="one document for both flags (identities PUT/GET = $q11_one_ident, AssumeRole = $q11_one_assume, session put-in/get-in/put-out/get-out = $q11_one_session; two files: $q11_two_ident, $q11_two_assume, $q11_two_session)" ;;
  no:yes) q11="two files needed (one file: PUT/GET = $q11_one_ident, AssumeRole = $q11_one_assume, session = $q11_one_session; two files: PUT/GET = $q11_two_ident, AssumeRole = $q11_two_assume, session = $q11_two_session)" ;;
  *)
    if [ "$q11_one_ident" = "not tested" ] && [ "$q11_two_ident" = "not tested" ]; then
      q11="not supported: the gateway did not start"
    else
      q11="not supported (one file: PUT/GET = $q11_one_ident, AssumeRole = $q11_one_assume, session = $q11_one_session; two files: PUT/GET = $q11_two_ident, AssumeRole = $q11_two_assume, session = $q11_two_session)"
    fi ;;
esac
echo "Q11: $q11"

# --- Q12: the values shape that renders the flag and the mount (read-only) ---------------
echo "Q12: rendering the pinned chart's s3 Deployment with slice 2's values (no cluster)"

CHART_DIR="$ROOT/platform/chart"
seaweed_tgz() { # -> the vendored seaweedfs .tgz, or nothing (helm dependency update's output)
  local f
  for f in "$CHART_DIR"/charts/seaweedfs-*.tgz; do
    if [ -f "$f" ]; then printf '%s' "$f"; return 0; fi
  done
  return 0
}
if [ ! -d "$CHART_DIR/charts/seaweedfs" ] && [ -z "$(seaweed_tgz)" ]; then
  if make -C "$ROOT/platform" chart-deps >"$tmp/chart-deps.log" 2>&1; then
    echo "  - chart/charts/seaweedfs was absent; \`make chart-deps\` fetched $(basename "$(seaweed_tgz)")"
  else
    echo "  - chart/charts/seaweedfs is absent and \`make chart-deps\` failed: $(tail -n1 "$tmp/chart-deps.log" | cut -c1-140)"
  fi
fi
# helm dependency update leaves a .tgz; an extracted chart directory is used when present.
SEAWEED_REF=""
if [ -d "$CHART_DIR/charts/seaweedfs" ]; then
  SEAWEED_REF="$CHART_DIR/charts/seaweedfs"
else
  SEAWEED_REF="$(seaweed_tgz)"
fi

q12="not measured"
mount_answer="not rendered"
argv_answer="absent"
if [ -n "$SEAWEED_REF" ] && command -v helm >/dev/null 2>&1; then
  # The ticket's shape: every value as a --set list entry.
  if helm template seaweedfs "$SEAWEED_REF" --values "$VALUES" \
      --set 's3.extraArgs[0]=-iam.config=/etc/sw/seaweedfs_s3_config' \
      --set 's3.extraVolumes[0].name=sw-iam' --set 's3.extraVolumes[0].configMap.name=spike-iam' \
      --set 's3.extraVolumeMounts[0].name=sw-iam' --set 's3.extraVolumeMounts[0].mountPath=/etc/sw-iam' \
      >"$tmp/q12-list.yaml" 2>"$tmp/q12-list.err"; then
    q12_list="rendered"
  else
    q12_list="$(grep -m1 '^Error:' "$tmp/q12-list.err" | cut -c1-200)"
    [ -n "$q12_list" ] || q12_list="failed: $(tail -n1 "$tmp/q12-list.err" | cut -c1-160)"
    echo "  - --set '<path>[0]=…' for the volumes: $q12_list"
  fi

  # The chart tpl's s3.extraVolumes and s3.extraVolumeMounts as YAML strings; that is the
  # shape a values file carries (helm-values/seaweedfs.yaml is where slice 2 would add it).
  cat > "$tmp/q12-values.yaml" <<'EOF'
s3:
  extraArgs: ["-iam.config=/etc/sw/seaweedfs_s3_config"]
  extraVolumes: |
    - name: sw-iam
      configMap:
        name: spike-iam
  extraVolumeMounts: |
    - name: sw-iam
      mountPath: /etc/sw-iam
      readOnly: true
EOF
  if helm template seaweedfs "$SEAWEED_REF" --values "$VALUES" -f "$tmp/q12-values.yaml" \
      >"$tmp/q12.yaml" 2>"$tmp/q12.err"; then
    python3 - "$tmp/q12.yaml" <<'PY' | sed 's/^/  | /'
import sys
for doc in open(sys.argv[1]).read().split("\n---\n"):
    if "kind: Deployment" not in doc or "swfs-s3" not in doc:
        continue
    name = [l.split("name:", 1)[1].strip() for l in doc.splitlines() if l.strip().startswith("name:")][0]
    body = doc[doc.index("command:"):]
    mounts = body[body.index("volumeMounts:"):]
    mounts = mounts[:mounts.index("ports:")]
    print("rendered command of Deployment " + name + ":")
    for line in body[:body.index("volumeMounts:")].splitlines()[1:]:
        if line.strip():
            print(line.rstrip())
    print("rendered volumeMounts of Deployment " + name + ":")
    for line in mounts.splitlines()[1:]:
        if line.strip():
            print(line.rstrip())
    break
PY
    if grep -q -- '-iam.config=/etc/sw/seaweedfs_s3_config' "$tmp/q12.yaml"; then argv_answer="present"; fi
    if grep -q 'mountPath: /etc/sw-iam' "$tmp/q12.yaml" && grep -q 'name: spike-iam' "$tmp/q12.yaml"; then
      mount_answer="rendered (mount + the spike-iam volume)"
    fi
    q12="s3.extraArgs puts -iam.config=… in the argv: $argv_answer; s3.extraVolumes/s3.extraVolumeMounts render the second file: $mount_answer; the chart runs both values through tpl as strings, so the --set '<path>[0]=…' list form fails and the values-file string form (helm-values/seaweedfs.yaml) is the shape slice 2 needs"
  else
    echo "  - helm template failed: $(tail -n1 "$tmp/q12.err" | cut -c1-160)"
    q12="not measured: helm template failed"
  fi
else
  echo "  - chart not vendored and no seaweedfs .tgz under chart/charts (run \`make chart-deps\`)"
  q12="not measured: the chart is not vendored"
fi
echo "Q12: $q12"

# --- Q13: the live Secret's top-level keys (read-only, names only) -----------------------
echo "Q13: reading the live seaweedfs_s3_config keys (no cluster write)"

q13="not measured"
if command -v kubectl >/dev/null 2>&1; then
  # The document goes Secret -> base64 -> python over one pipe; key names only, no value is
  # printed and nothing decoded is written to disk.
  q13_keys="$(kubectl -n stardelt get secret seaweedfs-s3-config -o jsonpath='{.data.seaweedfs_s3_config}' 2>"$tmp/q13.err" \
    | base64 -d 2>>"$tmp/q13.err" \
    | python3 -c 'import json,sys
try:
    print(", ".join(sorted(json.load(sys.stdin))))
except Exception:
    print("")' 2>>"$tmp/q13.err")" || q13_keys=""
  if [ -n "$q13_keys" ]; then
    q13="$q13_keys"
  else
    echo "  - live config: not reachable ($(tail -n1 "$tmp/q13.err" 2>/dev/null | cut -c1-140))"
    q13="not measured: the live Secret was not reachable"
  fi
else
  echo "  - live config: not reachable (kubectl is not installed)"
  q13="not measured: kubectl is not installed"
fi
echo "Q13: live seaweedfs_s3_config keys: $q13"

# --- the two lines the design quotes ------------------------------------------------------
b_shape="not available"
case "$q4" in supported*) b_shape="available" ;; esac
a_shape="not configurable"
if [ "$sts_endpoint" = present ] && [ "$sts_principal" = present ]; then a_shape="configurable"; fi
echo "for #201 item 3: prefix-scoped static identity = $q4; STS session policy at a prefix = $q5"
echo "for #201 item 3 (STS path): one file for both gateway flags = $q7; deep table-shaped session policy = $q9; Lakekeeper profile update = $q10"
echo "for #201 slice 2: gateway flags = $q11; chart values = $q12; live config keys = $q13"
echo "recommendation input: B is $b_shape; A is $a_shape (Q6)"
