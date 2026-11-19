#!/usr/bin/env bash
# The demo's storage credentials, in one idempotent command (stardelt/stardelt#316, #201 slice b).
#
#   make demo-sts-check                 # read-only: does the warehouse vend credentials?
#   make demo-sts YES=1                 # flip WAREHOUSE (default gold) to sts-enabled=true
#   make demo-sts-off YES=1             # the rollback: sts-enabled=false
#   bash scripts/demo-sts.sh --off      # the same, without make
#
#   make demo-sts-visitors-check        # read-only: do bronze, silver and gold vend?
#   make demo-sts-visitors YES=1        # the visitor set of #201: bronze, silver and gold
#   make demo-sts-visitors-off YES=1    # the rollback of the above
#
# `--visitors` is `--check|--on|--off` for each of `VISITOR_WAREHOUSES` (`bronze silver gold`),
# one warehouse at a time; `playground` is written by Trino and is not part of it.
#
# The demo sandbox (`manifests/spark-demo-sandbox.yaml`) carries no storage credential by
# design: no `AWS_*`, no Secret. A visitor's `spark` session can therefore compute, but it
# cannot read a data file. With `sts-enabled=true` on a warehouse Lakekeeper vends a
# table-scoped credential on *every* load of a table — measured for `gold` in #259 and #309
# (Q17: the plain and the delegated table load carry the same vended keys). The catalog's
# grant decides the scope: the credential covers the one table's prefix and is read-only, so
# `writeTo("playground…")` from the sandbox stays impossible (that is a product decision on
# #201, not this switch).
#
# `--off` is the rollback: the same storage profile with `sts-enabled=false`. It changes one
# warehouse, nothing else; Trino, the shared Spark server and maintenance were measured
# unaffected by the flip (#309).
#
# No credential is printed: the bucket's access key and secret are written to mode-0600 files
# and travel only in the request body, exactly as in `scripts/spike-lakekeeper-sts-blast.sh`.
#
# Env: NAMESPACE (stardelt), WAREHOUSE (gold), YES (0), LK_PORT (18181), ROLE_ARN.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NAMESPACE:-stardelt}"
WAREHOUSE="${WAREHOUSE:-gold}"
VISITOR_WAREHOUSES=(bronze silver gold)
YES="${YES:-0}"
LK_PORT="${LK_PORT:-18181}"
ROLE_ARN="${ROLE_ARN:-arn:aws:iam::000000000000:role/stardelt}"

mode=check
case "${1:-}" in
  --check) mode=check ;;
  --on) mode=on ;;
  --off) mode=off ;;
  --visitors) mode=visitors; visitors_mode=--check ;;
  --visitors-on) mode=visitors; visitors_mode=--on ;;
  --visitors-off) mode=visitors; visitors_mode=--off ;;
  "") ;;
  *) echo "demo-sts: unexpected argument '$1' (accepted: --check, --on, --off, --visitors, --visitors-on, --visitors-off)" >&2; exit 2 ;;
esac

tmp="$(mktemp -d "${TMPDIR:-/tmp}/demo-sts.XXXXXX")"
LK_PID=""
ID=""
# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

for tool in kubectl curl jq python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

# The visitor set of #201: the same path, once per warehouse, so a single flip stays as it was.
if [ "$mode" = "visitors" ]; then
  rc=0
  for w in "${VISITOR_WAREHOUSES[@]}"; do
    WAREHOUSE="$w" YES="$YES" bash "${BASH_SOURCE[0]}" "$visitors_mode" || rc=1
  done
  exit "$rc"
fi

# --- port-forward ------------------------------------------------------------------------
kubectl -n "$NS" port-forward svc/lakekeeper "$LK_PORT:8181" >/dev/null 2>&1 &
LK_PID=$!
LK="http://127.0.0.1:$LK_PORT"

up() { # url -> 0 when anything but a transport failure answered
  local code
  code="$(curl -s -o /dev/null -m 2 -w '%{http_code}' "$1" 2>/dev/null)" || code="000"
  [ "${code:-000}" != "000" ]
}
ready=no
for _ in $(seq 1 30); do
  if up "$LK/management/v1/info"; then ready=yes; break; fi
  sleep 1
done
[ "$ready" = "yes" ] || { echo "lakekeeper did not answer within 30s" >&2; exit 1; }

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

bootstrap_auth

# --- the warehouse and the profile a flip builds on --------------------------------------
code="$(mgmt GET /management/v1/warehouse)"
ID="$(jq -r --arg w "$WAREHOUSE" '
  if type == "array" then .[] elif has("warehouses") then .warehouses[] else empty end
  | select(.name == $w) | .id' "$tmp/out.json" | head -n1)"
if [ -z "$ID" ] || [ "$ID" = "null" ]; then
  echo "no warehouse named $WAREHOUSE (http $code)" >&2
  exit 1
fi
code="$(mgmt GET "/management/v1/warehouse/$ID")"
if [ "$code" != "200" ]; then
  echo "the warehouse $WAREHOUSE could not be read (http $code)" >&2
  exit 1
fi
jq '.["storage-profile"]' "$tmp/out.json" > "$tmp/profile.json"
sts="$(jq -r '.["sts-enabled"] // false' "$tmp/profile.json")"

if [ "$mode" = "check" ]; then
  if [ "$sts" = "true" ]; then
    echo "demo-sts: $WAREHOUSE sts-enabled=true"
    exit 0
  fi
  echo "demo-sts: $WAREHOUSE sts-enabled=false (make demo-sts YES=1)"
  exit 1
fi

if [ "$mode" = "on" ]; then want=true; else want=false; fi
if [ "$YES" != "1" ]; then
  echo "nothing was changed — re-run with YES=1"
  exit 1
fi

if [ "$sts" = "$want" ]; then
  echo "demo-sts: $WAREHOUSE sts-enabled=$want (unchanged)"
  exit 0
fi

# The credential the profile already carries: mode-0600 files, never argv, never a log line.
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.access-key}' | base64 -d > "$tmp/access-key"
kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.secret-key}' | base64 -d > "$tmp/secret-key"

jq -n --slurpfile p "$tmp/profile.json" \
      --rawfile ak "$tmp/access-key" --rawfile sk "$tmp/secret-key" \
      --arg ep "http://seaweedfs-s3.$NS.svc.cluster.local:8333" --arg role "$ROLE_ARN" \
      --argjson on "$want" \
  '{"storage-profile": ($p[0] + {"sts-enabled": $on, "sts-role-arn": $role,
                                 "sts-endpoint": $ep, "sts-session-policy-principal": false}),
    "storage-credential": {type: "s3", "credential-type": "access-key",
      "access-key-id": ($ak | rtrimstr("\n")), "secret-access-key": ($sk | rtrimstr("\n"))}}' \
  > "$tmp/flip.json"

code="$(post_profile "$tmp/flip.json")"
if [ "$code" != "200" ]; then
  echo "demo-sts: the storage update of $WAREHOUSE was refused => $code $(jq -r '.error.message // .message // empty' "$tmp/out.json" 2>/dev/null || head -c 200 "$tmp/out.json")" >&2
  exit 1
fi
echo "demo-sts: $WAREHOUSE sts-enabled=$want"
