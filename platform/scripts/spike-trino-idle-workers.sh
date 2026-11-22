#!/usr/bin/env bash
# What a query does while no Trino worker runs (stardelt/stardelt#1198, #30 step A).
#
#   make spike-trino-idle      (STARDELT_NOVA_URL=…, ISSUER=…, NS=… to override)
#
# Step A of #30 asks for the measurement the auto-stop design rests on: scale the worker
# Deployment to 0, submit a query, scale the worker back up when the coordinator reports a
# queued query, and record the wait until the query runs, whether it succeeds, and the idle
# memory of the cluster.
#
# This script does exactly that, through the same statement API a person uses, and prints six
# rows:
#
#   idle        replicas and the worker pod's memory before anything changes
#   scaled      the Deployment at 0 and no ready replica
#   queued      the submit answers 202; the state observed at 5 s, 15 s and 30 s
#   restored    the worker scaled back to 1 at the 30 s mark
#   result      the state the statement ends in, the elapsed seconds and rows (or the error)
#   idle again  the memory of the worker pod once it runs again
#
# The sign-in is `demo` with the password `demo`, which is PUBLIC ON PURPOSE:
# `scripts/public-demo.sh` creates that person and prints it, so it protects nothing. The
# password and the token live in two `mktemp` files (0600) and are removed on every exit;
# nothing here prints the token. Every exit path scales `trino-worker` back to 1.
#
# Exit 0 = the measurement ran and printed every row. Exit 1 = a step failed (the line names it).
# Exit 3 = UNVERIFIED: a tool is missing, or the statement neither finished nor failed in 300 s.
# An answered statement whose state is not what the design expected is a measurement, not a
# failure: it is printed and the script carries on.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"
NOVA="${NOVA%/}"
ISSUER="${ISSUER:-https://id.alpha.dev.stardelt.io/realms/stardelt}"
ISSUER="${ISSUER%/}"
NS="${NS:-stardelt}"

PASSWORD_FILE="$(mktemp)"
TOKEN_FILE="$(mktemp)"
BODY_FILE="$(mktemp)"
ERR_FILE="$(mktemp)"
chmod 600 "$PASSWORD_FILE" "$TOKEN_FILE" "$BODY_FILE" "$ERR_FILE"
# `mint-user-token.py` strips the trailing newline; the realm compares the exact string.
printf 'demo\n' > "$PASSWORD_FILE"

# Set right before the Deployment is scaled to 0; the trap then puts it back on every exit.
worker_scaled=0
cleanup() {
  if [ "$worker_scaled" = 1 ]; then
    kubectl -n "$NS" scale deployment trino-worker --replicas=1 >/dev/null 2>&1 || true
  fi
  rm -f "$PASSWORD_FILE" "$TOKEN_FILE" "$BODY_FILE" "$ERR_FILE"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

fail() { # fail <reason>: the step that broke, in the last line.
  echo "FAIL $1"
  echo "spike-trino-idle-workers: FAIL"
  exit 1
}

unverified() { # unverified <reason>: nothing was learned, so this is not a result.
  echo "UNVERIFIED ($1)"
  exit 3
}

now() { date +%s; }

first_line() { head -n 1 "$1" 2>/dev/null || true; }

for tool in kubectl curl python3; do
  command -v "$tool" >/dev/null 2>&1 || unverified "$tool not found"
done

# The token is in an argument here, so the helpers never echo their own command line.
auth_header() { printf 'Authorization: Bearer %s' "$(cat "$TOKEN_FILE")"; }

# api <label> <curl args…>: the body to $BODY_FILE, the HTTP code on stdout.
# `label` is what is printed on a failure (never the args: they carry the token).
api() {
  local label="$1" code
  shift
  if ! code="$(curl -sS -o "$BODY_FILE" -w '%{http_code}' "$@" 2>"$ERR_FILE")"; then
    echo "FAIL curl $label — $(first_line "$ERR_FILE")" >&2
    return 1
  fi
  printf '%s' "$code"
}

# field <name>: one top-level field of $BODY_FILE as text (empty when absent or not JSON).
field() {
  python3 - "$BODY_FILE" "$1" <<'PY'
import json, sys

try:
    doc = json.load(open(sys.argv[1]))
except ValueError:
    raise SystemExit
value = doc.get(sys.argv[2])
if value is not None:
    print(value if not isinstance(value, (int, float)) else int(value))
PY
}

# worker_memory: the MEMORY column of the `trino-worker-*` row of `kubectl top pod`.
worker_memory() {
  local out
  if ! out="$(kubectl -n "$NS" top pod -l app.kubernetes.io/component=worker --no-headers 2>"$ERR_FILE")"; then
    echo "FAIL kubectl top pod -l app.kubernetes.io/component=worker — $(first_line "$ERR_FILE")" >&2
    return 1
  fi
  printf '%s\n' "$out" | awk '$1 ~ /^trino-worker-/ {print $3; exit}'
}

# Row 0: sign in as the demo person.
if ! out="$(ISSUER="$ISSUER" USER=demo PASSWORD_FILE="$PASSWORD_FILE" TOKEN_FILE="$TOKEN_FILE" \
      python3 scripts/mint-user-token.py 2>&1)"; then
  fail "cannot mint a token for demo: $(printf '%s\n' "$out" | head -n 1)"
fi

# Row 1 `idle`: what the cluster costs before anything changes.
replicas="$(kubectl -n "$NS" get deploy trino-worker -o jsonpath='{.spec.replicas}' 2>"$ERR_FILE")" \
  || fail "kubectl get deploy trino-worker — $(first_line "$ERR_FILE")"
memory="$(worker_memory)" || fail "cannot read the worker's memory"
echo "idle replicas=${replicas:-unknown} worker-memory=${memory:-none}"

# Row 2 `scaled`: no worker runs, and none is ready.
worker_scaled=1
kubectl -n "$NS" scale deployment trino-worker --replicas=0 >/dev/null 2>"$ERR_FILE" \
  || fail "kubectl scale deployment trino-worker --replicas=0 — $(first_line "$ERR_FILE")"
scaled_at="$(now)"
while :; do
  ready="$(kubectl -n "$NS" get deploy trino-worker -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
  case "$ready" in
    "" | 0) break ;;
  esac
  [ "$(( $(now) - scaled_at ))" -ge 120 ] \
    && fail "the Deployment still reports a ready replica after 120 s"
  sleep 2
done
echo "scaled ready=0 after $(( $(now) - scaled_at ))s"

# Row 3 `queued`: submit one query and watch it wait for the worker.
submitted_at="$(now)"
code="$(api 'POST /api/statements' -X POST \
    -H "$(auth_header)" -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT count(*) AS n FROM bronze.shop.customers","waitSeconds":0}' \
    "$NOVA/api/statements")" || fail "POST /api/statements did not answer"
[ "$code" = 202 ] || fail "POST /api/statements answered $code, not 202: $(head -c 300 "$BODY_FILE")"
statement_id="$(field statementId)"
[ -n "$statement_id" ] || fail "the submit answer carries no statementId: $(head -c 300 "$BODY_FILE")"
state="$(field state)"

statement_state() { # statement_state: the statement's state now (?waitSeconds=0).
  local code
  code="$(api 'GET /api/statements/<id>' -H "$(auth_header)" \
      "$NOVA/api/statements/$statement_id?waitSeconds=0")" \
    || fail "GET /api/statements/<id> did not answer"
  [ "$code" = 200 ] || fail "GET /api/statements/<id> answered $code: $(head -c 300 "$BODY_FILE")"
  field state
}

for mark in 5 15 30; do
  while [ "$(( $(now) - submitted_at ))" -lt "$mark" ]; do sleep 2; done
  state="$(statement_state)"
  echo "queued state=$state at ${mark}s (statementId=$statement_id)"
done

# Row 4 `restored`: the worker runs again, at the 30 s mark.
kubectl -n "$NS" scale deployment trino-worker --replicas=1 >/dev/null 2>"$ERR_FILE" \
  || fail "kubectl scale deployment trino-worker --replicas=1 — $(first_line "$ERR_FILE")"
echo "restored"

# Row 5 `result`: the statement ends, or nothing was learned.
deadline="$(( submitted_at + 300 ))"
while :; do
  state="$(statement_state)"
  case "$state" in
    SUCCEEDED | FAILED | CANCELED) break ;;
  esac
  [ "$(now)" -ge "$deadline" ] \
    && unverified "the statement neither finished nor failed in 300 s"
  sleep 2
done
elapsed="$(( $(now) - submitted_at ))"
case "$state" in
  SUCCEEDED) echo "result state=$state elapsed=${elapsed}s rowCount=$(field rowCount)" ;;
  *) echo "result state=$state elapsed=${elapsed}s error=$(field error)" ;;
esac

# Row 6 `idle again`: the worker's memory once it serves again.
deadline="$(( $(now) + 120 ))"
while :; do
  memory="$(worker_memory)" || fail "cannot read the worker's memory again"
  [ -n "$memory" ] && break
  [ "$(now)" -ge "$deadline" ] && break
  sleep 2
done
echo "idle again worker-memory=${memory:-none}"

echo "spike-trino-idle-workers: done"
