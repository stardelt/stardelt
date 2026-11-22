#!/usr/bin/env bash
# Prove the identity Nova imports dashboard bundles with: the Secret, the converged Superset
# user and its role, and one real export as that user (stardelt#1216, slice a of #115).
#
#   make verify-superset-nova-identity      (NS=… PORT=… optional)
#
# #115 decision 5 has Nova call Superset's import API with a service account. This checks what
# the live install actually has, not only that the init Job ran: Secret `superset-nova` exists,
# the user can sign in, its role carries `datasource_access` on the Trino datasets, its
# dashboard list is non-empty (a fresh install's is not, which is UNVERIFIED, not a failure)
# and the export of one dashboard answers 200 with a ZIP that has a `dashboards/` entry — the
# spike #1210 measured that a role without the dataset grants exports nothing (404).
#
# READ-ONLY. Nothing is created in Superset or on the cluster: GETs only, one port-forward
# (killed by the trap) and one exec into the running Superset pod to read the role.
#
#   preflight          kubectl, curl, jq, python3; the Superset Deployment; the local port
#   secret             Secret `superset-nova`: non-empty username and password (never the value)
#   login              POST /api/v1/security/login as that user; the Bearer header in a 0600 file
#   role               in the pod: `stardelt-nova`'s permission count and its dataset grants
#   dashboards         GET /api/v1/dashboard/: the count
#   export             GET /api/v1/dashboard/export/: status, bytes and the `dashboards/` entry
#   cleanup            nothing created; the port-forward is gone
#
# Exit 0 = PASS, 1 = FAIL, 3 = UNVERIFIED (a tool, the Secret or the Deployment is missing, the
# instance has no dashboard yet, or the port-forward does not answer).
#
# No secret is printed: the username and the password go from the Secret into 0600 files and
# the JSON login body is built from them by python3; the access token goes into a 0600 header
# file (`curl -H @file`) and the export into a 0600 file.
set -euo pipefail

NS="${NS:-stardelt}"
PORT="${PORT:-8088}"
BASE="http://127.0.0.1:$PORT"
DEPLOY="${DEPLOY:-superset}"
SECRET="${SECRET:-superset-nova}"
USER_NAME="${USER_NAME:-stardelt-nova}"
ROLE="${ROLE:-stardelt-nova}"
CONTEXT="${CONTEXT:-}"

umask 077
tmp="$(mktemp -d "${TMPDIR:-/tmp}/verify-superset-nova.XXXXXX")"
PF_PID=""
FAILURES=0
UNVERIFIED=""

row() { printf '%-14s %s\n' "$1" "$2"; }
fail() { row "$1" "FAIL $2"; FAILURES=$((FAILURES + 1)); }

kc() { # kubectl, with the pinned context when one is given
  if [ -n "$CONTEXT" ]; then kubectl --context "$CONTEXT" "$@"; else kubectl "$@"; fi
}

# shellcheck disable=SC2329  # called from the trap and from give_up
cleanup() {
  set +e
  if [ -n "$PF_PID" ]; then
    kill "$PF_PID" >/dev/null 2>&1
    wait "$PF_PID" >/dev/null 2>&1
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

give_up() { # <reason>: the verdict and exit 3, the trap cleans up
  UNVERIFIED="$1"
  echo "verify-superset-nova-identity: UNVERIFIED ($1)"
  exit 3
}

# --- preflight ------------------------------------------------------------------------------
missing=""
for tool in kubectl curl jq python3; do
  command -v "$tool" >/dev/null 2>&1 || missing="$missing $tool"
done
if [ -n "$missing" ]; then
  row preflight "unverified missing:$missing"
  give_up "missing:$missing"
fi
if ! kc -n "$NS" get "deploy/$DEPLOY" >/dev/null 2>&1; then
  row preflight "unverified no Deployment $NS/$DEPLOY"
  give_up "the Superset Deployment is not installed"
fi
if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  row preflight "unverified port $PORT is already in use — stop the other forward or set PORT"
  give_up "port $PORT is already in use"
fi
row preflight "ok kubectl curl jq python3 $DEPLOY"

# --- secret ---------------------------------------------------------------------------------
if ! kc -n "$NS" get secret "$SECRET" >/dev/null 2>&1; then
  row secret "unverified no Secret $NS/$SECRET (run make step-app-secrets)"
  give_up "the $SECRET Secret is not installed"
fi
kc -n "$NS" get secret "$SECRET" -o jsonpath='{.data.username}' | base64 -d > "$tmp/user"
kc -n "$NS" get secret "$SECRET" -o jsonpath='{.data.password}' | base64 -d > "$tmp/pass"
chmod 600 "$tmp/user" "$tmp/pass"
user="$(cat "$tmp/user")"
pass_bytes="$(wc -c < "$tmp/pass" | tr -d ' ')"
if [ -z "$user" ] || [ "$pass_bytes" -eq 0 ]; then
  row secret "unverified Secret $NS/$SECRET has an empty username or password"
  give_up "the $SECRET Secret has an empty username or password"
fi
row secret "ok username=$user password=<$pass_bytes bytes>"

# --- login ----------------------------------------------------------------------------------
# not through kc: `$!` must be the kubectl process the trap kills
if [ -n "$CONTEXT" ]; then
  kubectl --context "$CONTEXT" -n "$NS" port-forward "svc/$DEPLOY" "$PORT:8088" >"$tmp/pf.log" 2>&1 &
else
  kubectl -n "$NS" port-forward "svc/$DEPLOY" "$PORT:8088" >"$tmp/pf.log" 2>&1 &
fi
PF_PID=$!
up=0
for _ in $(seq 1 30); do
  kill -0 "$PF_PID" 2>/dev/null || give_up "port-forward exited: $(tail -n 1 "$tmp/pf.log")"
  code="$(curl -s -o /dev/null --max-time 2 -w '%{http_code}' "$BASE/health" 2>/dev/null)" || code=000
  [ "${code:-000}" != "000" ] && { up=1; break; }
  sleep 1
done
[ "$up" -eq 1 ] || give_up "superset did not answer on $PORT within 30s"

python3 -c '
import json, sys
print(json.dumps({"username": open(sys.argv[1]).read().strip(),
                  "password": open(sys.argv[2]).read().strip(),
                  "provider": "db", "refresh": True}))' "$tmp/user" "$tmp/pass" > "$tmp/login.json"
code="$(curl -sS --max-time 30 -c "$tmp/cookies" -H 'Content-Type: application/json' \
    -X POST --data @"$tmp/login.json" -o "$tmp/login.out" -w '%{http_code}' \
    "$BASE/api/v1/security/login" 2>/dev/null)" || code=000
rm -f "$tmp/login.json"
token_len="$(python3 -c 'import json,sys; print(len((json.load(open(sys.argv[1])) or {}).get("access_token") or ""))' \
    "$tmp/login.out" 2>/dev/null || echo 0)"
if [ "${code:-000}" != 200 ] || [ "$token_len" -eq 0 ]; then
  row login "FAIL $code (no access_token): is the user converged? run make step-superset"
  FAILURES=$((FAILURES + 1))
  echo "verify-superset-nova-identity: FAIL ($FAILURES check(s) failed)"
  exit 1
fi
python3 -c '
import json, sys
print("Authorization: Bearer " + json.load(open(sys.argv[1]))["access_token"])' "$tmp/login.out" > "$tmp/auth"
chmod 600 "$tmp/auth" "$tmp/cookies"
rm -f "$tmp/login.out"
row login "ok 200"

# --- role -----------------------------------------------------------------------------------
cat > "$tmp/role.py" <<'PY'
import sys

from superset.app import create_app

app = create_app()
with app.app_context():
    from superset import security_manager as sm

    role = sm.find_role(sys.argv[1])
    if role is None:
        print("role=%s state=missing" % sys.argv[1])
    else:
        perms = {(p.permission.name, p.view_menu.name) for p in role.permissions}
        datasets = {p for p in perms if p[0] == "datasource_access"}
        print("role=%s permissions=%d datasets=%d" % (role.name, len(perms), len(datasets)))
PY
role_out="$(kc -n "$NS" exec "deploy/$DEPLOY" -- python -c "$(cat "$tmp/role.py")" "$ROLE" 2>/dev/null \
  | sed '/^Loaded your LOCAL configuration/d' | tail -1 || true)"
role_perms="$(sed -n 's/.*permissions=\([0-9]*\).*/\1/p' <<<"$role_out")"
role_datasets="$(sed -n 's/.*datasets=\([0-9]*\).*/\1/p' <<<"$role_out")"
if [ -z "$role_perms" ] || [ -z "$role_datasets" ] || [ "$role_datasets" -eq 0 ]; then
  fail role "role $ROLE: ${role_out:-no output} — it must exist and hold datasource_access on the Trino datasets"
else
  row role "ok role $ROLE: $role_perms permissions, $role_datasets dataset(s)"
fi

# --- dashboards -----------------------------------------------------------------------------
dashboards() { # -> http code on stdout, body in $tmp/dash.json
  local got
  got="$(curl -gsS --max-time 30 -b "$tmp/cookies" -c "$tmp/cookies" -H @"$tmp/auth" \
      "$BASE/api/v1/dashboard/?q=(page_size:200)" -o "$tmp/dash.json" -w '%{http_code}' 2>/dev/null)" || got=000
  printf '%s' "${got:-000}"
}

code="$(dashboards)"
count="$(jq -r '.count // empty' "$tmp/dash.json" 2>/dev/null || true)"
if [ "$code" != 200 ] || [ -z "$count" ]; then
  fail dashboards "the dashboard list answered $code"
  count=0
else
  row dashboards "ok $count"
fi
dash_id="$(jq -r '.result[0].id // empty' "$tmp/dash.json" 2>/dev/null || true)"

# --- export ---------------------------------------------------------------------------------
if [ -z "$dash_id" ]; then
  row export "unverified the instance has no dashboard to export (a fresh install has none)"
  UNVERIFIED="no dashboard on the instance"
else
  code="$(curl -gsS --max-time 60 -b "$tmp/cookies" -c "$tmp/cookies" -H @"$tmp/auth" \
      "$BASE/api/v1/dashboard/export/?q=[$dash_id]" -o "$tmp/export.zip" -w '%{http_code}' 2>/dev/null)" || code=000
  bytes="$(wc -c < "$tmp/export.zip" 2>/dev/null | tr -d ' ' || echo 0)"
  has_dash=no
  if python3 -c 'import sys, zipfile; sys.exit(0 if any("/dashboards/" in n or n.startswith("dashboards/")
              for n in zipfile.ZipFile(sys.argv[1]).namelist()) else 1)' "$tmp/export.zip" 2>/dev/null; then
    has_dash=yes
  fi
  if [ "$code" = 200 ] && [ "$bytes" -gt 0 ] && [ "$has_dash" = yes ]; then
    row export "ok $code $bytes bytes dashboards/: $has_dash"
  else
    fail export "$code $bytes bytes dashboards/: $has_dash"
  fi
fi

# --- cleanup --------------------------------------------------------------------------------
if [ -n "$PF_PID" ]; then
  kill "$PF_PID" >/dev/null 2>&1 || true
  wait "$PF_PID" >/dev/null 2>&1 || true
fi
if [ -n "$PF_PID" ] && kill -0 "$PF_PID" 2>/dev/null; then
  fail cleanup "the port-forward to $PORT is still running"
else
  row cleanup "ok nothing created"
fi

# --- verdict --------------------------------------------------------------------------------
if [ "$FAILURES" -ne 0 ]; then
  echo "verify-superset-nova-identity: FAIL ($FAILURES check(s) failed)"
  exit 1
fi
if [ -n "$UNVERIFIED" ]; then
  echo "verify-superset-nova-identity: UNVERIFIED ($UNVERIFIED)"
  exit 3
fi
echo "verify-superset-nova-identity: PASS"
