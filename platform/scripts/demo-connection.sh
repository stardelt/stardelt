#!/usr/bin/env bash
# The demo's first `Connection`, in one idempotent command (stardelt/stardelt#349, #79 point 3).
#
#   make demo-connection YES=1      # store openbao-managed, the secret path, the Connection
#   make demo-connection-off YES=1  # the rollback: the Connection and its secret path
#   bash scripts/demo-connection.sh # the plan, and exit 1
#
# The demo connects to RNAcentral's public PostgreSQL (`hh-pgsql-public.ebi.ac.uk`, database
# `pfmegrnargs`): a read-only account over CC0 data. Its credential is PUBLIC ON PURPOSE —
# RNAcentral publishes it on its own help page ("Public Postgres database"), the same way this
# repository publishes `demo`/`demo` — so it protects nothing and is meant to protect nothing.
#
# In order, and idempotently:
#
#   1. the External Secrets store `openbao-managed` (scripts/eso-openbao-store.sh managed);
#   2. the secret `secret/connections/managed/rnacentral` (keys `user`, `password`);
#   3. the `Connection` rnacentral, scope `managed`, `usableBy: [visitors]`.
#
# `managed` means the platform's own work uses it, never a person's notebook: a pipeline of
# the group `visitors` may use it, while `stardelt.connection()` in a notebook refuses a
# managed connection by design (decision D23).
#
# Tokens reach the store only through stdin of `kubectl exec`, never through argv or a log
# line, and a secret value is written and never read back. Rollback:
# `bash scripts/demo-connection.sh --off YES=1`.
#
# Env: NAMESPACE (stardelt), YES (0).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NAMESPACE:-stardelt}"
YES="${YES:-0}"
POD="${POD:-openbao-0}"
CONTAINER="${CONTAINER:-openbao}"
INIT_SECRET=openbao-init

NAME=rnacentral
SECRET_PATH="secret/connections/managed/$NAME"
HOST=hh-pgsql-public.ebi.ac.uk
PORT=5432
DATABASE=pfmegrnargs
USER=reader
# Public on purpose: RNAcentral's published read-only account.
PASSWORD=NWDMCE5xdipIjRrp
GROUP=visitors

fail() { echo "demo-connection: $*" >&2; exit 1; }

mode=on
case "${1:-}" in
  --off) mode=off ;;
  "") ;;
  *) fail "unexpected argument '$1' (accepted: --off)" ;;
esac

for tool in kubectl python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool not found"
done

if [ "$YES" != 1 ]; then
  if [ "$mode" = off ]; then
    echo "would: delete Connection $NAME and the secret path $SECRET_PATH (metadata and every version)"
  else
    echo "would: ensure the External Secrets store openbao-managed; write $SECRET_PATH (user, password); apply Connection $NAME (managed, usableBy $GROUP)"
  fi
  echo "nothing was changed — re-run with YES=1"
  exit 1
fi

K=(kubectl -n "$NAMESPACE")

# bao_root <args...>: the root token is sent as the first line of stdin and exported inside the pod;
# anything the caller pipes in follows it. Copied from scripts/openbao-connections.sh.
bao_root() {
  { printf '%s\n' "$ROOT_TOKEN"; cat; } | "${K[@]}" exec -i "$POD" -c "$CONTAINER" -- \
    sh -c 'IFS= read -r BAO_TOKEN; export BAO_TOKEN BAO_ADDR=http://127.0.0.1:8200; exec bao "$@"' sh "$@"
}

ROOT_TOKEN="$("${K[@]}" get secret "$INIT_SECRET" -o jsonpath='{.data.root-token}' 2>/dev/null | base64 -d)" \
  || true
[ -n "${ROOT_TOKEN:-}" ] || fail "Secret $INIT_SECRET has no root token: run make openbao"

manifest() {
  cat <<EOF
apiVersion: stardelt.io/v1alpha1
kind: Connection
metadata:
  name: $NAME
  namespace: $NAMESPACE
  labels: { stardelt.io/managed-by: public-demo }
spec:
  scope: managed
  usableBy: [$GROUP]
  type: postgresql
  config:
    host: $HOST
    port: "$PORT"
    database: $DATABASE
  secret:
    keys: [user, password]
EOF
}

if [ "$mode" = off ]; then
  "${K[@]}" delete connection "$NAME" --ignore-not-found
  if bao_root kv metadata get "$SECRET_PATH" < /dev/null >/dev/null 2>&1; then
    bao_root kv metadata delete "$SECRET_PATH" < /dev/null >/dev/null
    echo "› secret $SECRET_PATH: deleted (every version)"
  else
    echo "› secret $SECRET_PATH: not found"
  fi
  echo "demo-connection: Connection $NAME and its secret path are gone"
  exit 0
fi

# 1. The store the Connection's ExternalSecret names.
store_out="$(bash scripts/eso-openbao-store.sh managed 2>&1)" \
  || fail "scripts/eso-openbao-store.sh managed failed: $store_out"
printf '%s\n' "$store_out" | awk '/^› store openbao-managed: /'

# 2. The secret values, written and never read back: `user` is a public name, the password
# travels only as the stdin of the exec (`password=-` reads its value from stdin).
printf '%s' "$PASSWORD" | bao_root kv put "$SECRET_PATH" user="$USER" password=- >/dev/null
echo "› secret $SECRET_PATH: written"

# 3. The Connection; the operator turns it into the ExternalSecret of the Secret conn-rnacentral.
manifest | "${K[@]}" apply -f - >/dev/null

# 4. Ready means the Secret exists and holds every key in spec.secret.keys.
ready=""
for _ in $(seq 1 60); do
  ready="$("${K[@]}" get connection "$NAME" -o jsonpath='{.status.ready}' 2>/dev/null || true)"
  [ "$ready" = true ] && break
  sleep 2
done
if [ "$ready" != true ]; then
  condition="$("${K[@]}" get connection "$NAME" \
    -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.reason}: {.message}{end}' 2>/dev/null || true)"
  fail "Connection $NAME is not Ready after 120 s (${condition:-no Ready condition})"
fi

echo "demo-connection: $NAME is Ready (Secret conn-$NAME holds password, user)"
