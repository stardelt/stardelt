#!/usr/bin/env bash
# Spike for stardelt/stardelt#201 item 3 and stardelt/stardelt#259 (Q14-Q19 of the storage path):
# does Lakekeeper 0.15.0 vend a table-scoped credential through the platform's S3 gateway (STS),
# and does that session stop at the table's prefix?
#
# It measures; it writes to one warehouse's storage profile and restores it. Every answer is one
# `Q<n>: ...` line on stdout. The script prints status codes, key names and prefixes only: a
# credential in a log line is a defect. Secret and token values travel through mode-0600 files
# (`-H @file`, `-K file`), never through argv, and the temp dir is removed on every exit path.
#
# Deviations from the ticket text, each measured on 2026-11-19 (see the ticket's evidence comment):
#   - POST /management/v1/warehouse/<id>/storage keeps the storage credential only when the body
#     carries `storage-credential`; without it the profile is read as credential-less and the
#     update is refused with 400 "Access Denied" (the profile's own validation write). So both
#     POSTs send the access key from Secret `stardelt-s3-creds`, read into temp files.
#   - The catalog prefix is the warehouse *id* (a UUID), not its name: `/catalog/v1/<name>/...`
#     answers 400 WarehouseIdIsNotUUID on this release.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
WAREHOUSE="${WAREHOUSE:-gold}"
NAMESPACE_IN_CATALOG="${NAMESPACE_IN_CATALOG:-shop}"
LK_PORT="${LK_PORT:-18181}"
S3_PORT="${S3_PORT:-18333}"
ROLE_ARN="${ROLE_ARN:-arn:aws:iam::000000000000:role/stardelt}"
DEMO_PASSWORD="${DEMO_PASSWORD:-demo}"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/lk-vending-spike.XXXXXX")"
LK_PID=""
S3_PID=""
ID=""
BUCKET=""
REGION=""

# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  local code
  if [ -f "$tmp/flipped" ] && [ -n "$ID" ] && [ -s "$tmp/profile.json" ] && [ -s "$tmp/auth" ]; then
    code="$(restore_profile 2>/dev/null || echo 000)"
    echo "trap: restored the storage profile of $WAREHOUSE => ${code:-000}"
  fi
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  if [ -n "$S3_PID" ]; then kill "$S3_PID" >/dev/null 2>&1 || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

for tool in kubectl curl jq python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

# --- inputs ------------------------------------------------------------------------------
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.bucket}' | base64 -d > "$tmp/bucket"
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.region}' | base64 -d > "$tmp/region"
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.access-key}' | base64 -d > "$tmp/access-key"
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.secret-key}' | base64 -d > "$tmp/secret-key"
BUCKET="$(tr -d '\n' < "$tmp/bucket")"
REGION="$(tr -d '\n' < "$tmp/region")"

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

# --- Q14: what the live warehouse says ----------------------------------------------------
code="$(mgmt GET /management/v1/warehouse)"
list="$tmp/out.json"
ID="$(jq -r --arg w "$WAREHOUSE" '
  if type == "array" then .[] elif has("warehouses") then .warehouses[] else empty end
  | select(.name == $w) | .id' "$list" | head -n1)"
if [ -z "$ID" ] || [ "$ID" = "null" ]; then
  echo "Q14: shape => $(jq -c 'if type == "object" then keys else type end' "$list")" >&2
  echo "Q14: no warehouse named $WAREHOUSE in the list (http $code)" >&2
  exit 1
fi
code="$(mgmt GET "/management/v1/warehouse/$ID")"
jq '.["storage-profile"]' "$tmp/out.json" > "$tmp/profile.json"
STS_BEFORE="$(jq -r '.["sts-enabled"]' "$tmp/profile.json")"
KEY_PREFIX="$(jq -r '.["key-prefix"] // "-"' "$tmp/profile.json")"
echo "Q14: $WAREHOUSE id=${ID:0:8}… key-prefix=$KEY_PREFIX sts-enabled=$STS_BEFORE (http $code)"

# --- Q15: flipping it is accepted ---------------------------------------------------------
q15="already-true"
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
  # The flag exists only once a write landed: a failure before or during Q15 restores nothing.
  if [ "$code" = "200" ]; then touch "$tmp/flipped"; fi
  if [ "$code" != "200" ]; then
    echo "Q15: storage update => $code $(jq -c '{message: .error.message, type: .error.type}' "$tmp/out.json" 2>/dev/null || head -c 200 "$tmp/out.json")" >&2
    exit 1
  fi
  mgmt GET "/management/v1/warehouse/$ID" >/dev/null
  STS_AFTER="$(jq -r '.["storage-profile"]["sts-enabled"]' "$tmp/out.json")"
  q15="flipped"
  echo "Q15: storage update => $code, sts-enabled=$STS_AFTER"
fi

# --- Q16: a visitor's token reads the catalog ---------------------------------------------
printf '%s' "$DEMO_PASSWORD" > "$tmp/password"
ISSUER="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}')"
ISSUER="$ISSUER" USER=demo PASSWORD_FILE="$tmp/password" TOKEN_FILE="$tmp/demo-token" \
  python3 scripts/mint-user-token.py >/dev/null
printf 'Authorization: Bearer %s\n' "$(cat "$tmp/demo-token")" > "$tmp/demo-auth"

catalog() { # <path> [extra-header-file] -> code, body in $tmp/out.json
  local path="$1" hdr="${2-}" code
  if [ -n "$hdr" ]; then
    code="$(curl -sS -H @"$tmp/demo-auth" -H @"$hdr" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
  else
    code="$(curl -sS -H @"$tmp/demo-auth" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
  fi
  echo "${code:-000}"
}

code_ns="$(catalog "/catalog/v1/$ID/namespaces")"
NS_COUNT="$(jq '[.namespaces[]?] | length' "$tmp/out.json")"
code_tbl="$(catalog "/catalog/v1/$ID/namespaces/$NAMESPACE_IN_CATALOG/tables")"
if [ "$code_ns" != "200" ] || [ "$code_tbl" != "200" ]; then
  echo "Q16: namespaces => $code_ns, tables => $code_tbl $(jq -c '{message: .error.message, type: .error.type}' "$tmp/out.json" 2>/dev/null || head -c 200 "$tmp/out.json")" >&2
  exit 1
fi
jq -r '.identifiers[]?.name' "$tmp/out.json" > "$tmp/tables"
TABLE_COUNT="$(wc -l < "$tmp/tables" | tr -d ' ')"
TABLES_PREVIEW="$(head -n 3 "$tmp/tables" | paste -sd, - | sed 's/,/, /g')"
echo "Q16: namespaces => $code_ns ($NS_COUNT), tables => $code_tbl ($TABLE_COUNT: $TABLES_PREVIEW)"

# --- Q17: vending is real, and only when asked -------------------------------------------
TABLE="${TABLE:-$(head -n1 "$tmp/tables")}"
[ -n "$TABLE" ] || { echo "Q17: the catalog lists no table in $NAMESPACE_IN_CATALOG" >&2; exit 1; }
printf 'X-Iceberg-Access-Delegation: vended-credentials\n' > "$tmp/deleg-header"

PLAIN_PATH="/catalog/v1/$ID/namespaces/$NAMESPACE_IN_CATALOG/tables/$TABLE"
code_plain="$(catalog "$PLAIN_PATH")"
jq '.config' "$tmp/out.json" > "$tmp/plain-config.json"
code_deleg="$(catalog "$PLAIN_PATH" "$tmp/deleg-header")"
jq '.config' "$tmp/out.json" > "$tmp/deleg-config.json"
jq -r '.metadata.location' "$tmp/out.json" > "$tmp/location"
KEYS_PLAIN="$(jq -r 'if . == null then "-" else (keys | join(",")) end' "$tmp/plain-config.json")"
KEYS_DELEG="$(jq -r 'if . == null then "-" else (keys | join(",")) end' "$tmp/deleg-config.json")"
vended=no
jq -e 'has("s3.session-token")' "$tmp/deleg-config.json" >/dev/null 2>&1 && vended=yes
echo "Q17: plain config keys = [$KEYS_PLAIN]; delegated config keys = [$KEYS_DELEG]; vended=$vended (http $code_plain/$code_deleg)"

# --- Q18: the session scopes to the table ------------------------------------------------
LOCATION="$(cat "$tmp/location")"
KEY_IN="${LOCATION#s3://"$BUCKET"/}"
SIBLING="$(sed -n 2p "$tmp/tables")"
if [ -z "$SIBLING" ] || [ "$KEY_IN" = "$LOCATION" ]; then
  echo "Q18: only one table (or a location outside $BUCKET) — scope not measurable" >&2
  q18="not tested"
else
  jq -r '.["s3.access-key-id"] // empty' "$tmp/deleg-config.json" > "$tmp/v-ak"
  jq -r '.["s3.secret-access-key"] // empty' "$tmp/deleg-config.json" > "$tmp/v-sk"
  jq -r '.["s3.session-token"] // empty' "$tmp/deleg-config.json" > "$tmp/v-tok"

  # The vended values reach curl through a config file, so no credential is in argv.
  {
    printf 'user = "%s:%s"\n' "$(cat "$tmp/v-ak")" "$(cat "$tmp/v-sk")"
    printf 'header = "x-amz-security-token: %s"\n' "$(cat "$tmp/v-tok")"
  } > "$tmp/s3-curl.conf"

  s3sig() { # <method> <key> [body] -> status code
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

  # One object the table already owns, so the read is a real GetObject (a missing key answers 404,
  # which cannot tell a denial from an absence).
  OWN_KEY="$(curl -sS -K "$tmp/s3-curl.conf" --aws-sigv4 "aws:amz:$REGION:s3" \
    "$EP/$BUCKET?list-type=2&prefix=$KEY_IN/" 2>/dev/null \
    | grep -o '<Key>[^<]*' | head -n1 | cut -c6- || true)"

  in_put="$(s3sig PUT "$KEY_IN/spike-259-in.txt" 'spike-259')"
  if [ -n "$OWN_KEY" ]; then in_get="$(s3sig GET "$OWN_KEY")"; else in_get="not tested"; fi
  # The sibling table's own prefix, taken from the catalog, not guessed from ours.
  catalog "/catalog/v1/$ID/namespaces/$NAMESPACE_IN_CATALOG/tables/$SIBLING" "$tmp/deleg-header" >/dev/null
  SIB_LOCATION="$(jq -r '.metadata.location' "$tmp/out.json")"
  KEY_OUT="${SIB_LOCATION#s3://"$BUCKET"/}"
  sib_put="$(s3sig PUT "$KEY_OUT/spike-259-out.txt" 'spike-259')"
  if [ "$KEY_OUT" = "$SIB_LOCATION" ]; then sib_get="not tested"; else sib_get="$(s3sig GET "$KEY_OUT/spike-259-out.txt")"; fi

  # Nothing was written when the session is read-only; remove whatever it did allow.
  s3sig DELETE "$KEY_IN/spike-259-in.txt" >/dev/null || true

  case "$in_put $in_get $sib_put $sib_get" in
    "200 200 403 403") q18="scoped=yes" ;;
    "403 200 403 403") q18="scoped=read-only" ;;
    *) q18="scoped=no" ;;
  esac
  echo "Q18: in put/get = $in_put $in_get, sibling put/get = $sib_put $sib_get"
  echo "$q18"
fi

# --- Q19: the restore holds ---------------------------------------------------------------
code="$(restore_profile)"
if [ "$code" = "200" ]; then
  rm -f "$tmp/flipped"
  mgmt GET "/management/v1/warehouse/$ID" >/dev/null
  STS_RESTORED="$(jq -r '.["storage-profile"]["sts-enabled"]' "$tmp/out.json")"
  code_read="$(catalog "$PLAIN_PATH")"
  echo "Q19: restore => sts-enabled=$STS_RESTORED, table read => $code_read"
  q19="restored"
  [ "$code_read" = "200" ] || q19="restore-read-$code_read"
else
  echo "Q19: restore => $code $(jq -c '{message: .error.message}' "$tmp/out.json" 2>/dev/null || head -c 200 "$tmp/out.json")" >&2
  q19="restore-failed"
fi

# --- summary -----------------------------------------------------------------------------
echo "q14=ok q15=$q15 q16=read q17=vended:$vended q18=$q18 q19=$q19"
case "$q19" in
  restored) exit 0 ;;
  *) exit 1 ;;
esac
