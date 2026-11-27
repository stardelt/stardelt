#!/usr/bin/env bash
# Spike for stardelt/stardelt#40 (MLflow 3 as a plug): how a person signs in to an MLflow
# tracking server the platform would ship, and how a pipeline's principal reaches it.
#
#   cd platform && make spike-mlflow-auth
#
# MLflow has no native OIDC. Its own auth is HTTP basic (`--app-name basic-auth`); OIDC needs
# the Apache-2.0 plugin `mlflow-oidc-auth` (`--app-name oidc-auth`). #40's connection type is
# `mlflow` with a `token` secret (`MLFLOW_TRACKING_TOKEN`), so the spike also measures whether
# each app accepts a bearer token at all.
#
# Five lines, one per question (the `<…>` values are what this script measures):
#   R1 basic auth, no credentials   the status of the experiments search; 401 is the pass, 200
#                                   is option 3's defect, a redirect to a login page a finding
#   R2 basic auth, with credentials the mlflow client's exit status, the run it wrote and the
#                                   row count the API sees; the admin account and the bearer
#                                   story (MLflow 3.18 ships no default admin password)
#   R3 the OIDC plugin's authorize  what GET / answers, and the redirect the plugin builds from
#                                   the realm's discovery document: host, client_id, PKCE, scope
#   R4 the callback and the claim   the callback path the plugin actually serves, the group
#                                   claim's default, which the operator's client needs
#   R5 footprint and licence        the two packages' licences and the virtualenv's size
#
# The measurement runs in a throwaway virtualenv on this host: it installs nothing into the
# cluster, creates no bucket, namespace or Keycloak client, and prints no token, password or
# Secret value (the admin password is generated per run and never echoed). The trap kills both
# servers and removes the virtualenv and the temp dir on every exit path.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

PORT1="${PORT1:-5501}"
PORT2="${PORT2:-5502}"
ISSUER="${OIDC_ISSUER:-https://id.alpha.dev.stardelt.io/realms/stardelt}"
DISCOVERY="${OIDC_DISCOVERY_URL:-$ISSUER/.well-known/openid-configuration}"
CLIENT_ID="${OIDC_CLIENT_ID:-mlflow-spike}"

TD="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/spike-mlflow-auth.XXXXXX")"
pid1=""
pid2=""
# Each server runs in its own process group (`set -m`), so the trap kills the worker too.
set -m

cleanup() {
  rc=$?
  trap - EXIT INT TERM
  for pid in "$pid1" "$pid2"; do
    [ -n "$pid" ] && kill -- -"$pid" 2>/dev/null || true
  done
  sleep 1
  for pid in "$pid1" "$pid2"; do
    [ -n "$pid" ] && kill -9 -- -"$pid" 2>/dev/null || true
  done
  rm -rf "$TD"
  exit "$rc"
}
trap cleanup EXIT INT TERM

for tool in python3 curl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

# A server already on the port would answer the health check below and be measured by mistake.
for port in "$PORT1" "$PORT2"; do
  if curl -s -o /dev/null --max-time 2 "http://127.0.0.1:$port/health"; then
    echo "port $port is already serving — stop it or set PORT1/PORT2" >&2
    exit 1
  fi
done

# --- R0 the install ----------------------------------------------------------------------
# `mlflow[auth]` rather than `mlflow`: with MLflow 3.18 the basic-auth app refuses to start
# without Flask-WTF ("The MLflow basic auth app requires the Flask-WTF package … `pip install
# mlflow[auth]`"). The extra is part of the same pinned version.
python3 -m venv "$TD/venv"
"$TD/venv/bin/pip" install --quiet --upgrade pip
"$TD/venv/bin/pip" install --quiet "mlflow[auth]==3.18.0" "mlflow-oidc-auth==11.5.0"
"$TD/venv/bin/python" -c 'import mlflow, mlflow_oidc_auth; print("R0: mlflow", mlflow.__version__, "plugin", getattr(mlflow_oidc_auth, "__version__", "?"))'
export MLFLOW_DISABLE_AGENT_HINT=1

wait_health() {
  local port="$1" code
  for _ in $(seq 1 60); do
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:$port/health" 2>/dev/null || true)"
    [ "$code" = 200 ] && return 0
    sleep 1
  done
  return 1
}

# --- R1 and R2: basic auth ---------------------------------------------------------------
# MLflow 3.18 ships no default admin password; the server names MLFLOW_AUTH_ADMIN_PASSWORD in
# its own refusal. A random per-run value is set, never printed.
ADMIN_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
# The app writes `basic_auth.db` into the current directory: the subshell makes it $TD's, so no
# leftover lands in the checkout and the admin is re-created per run.
( cd "$TD" && exec env \
    MLFLOW_FLASK_SERVER_SECRET_KEY="$(python3 -c 'import secrets; print(secrets.token_hex(32))')" \
    MLFLOW_AUTH_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
    "$TD/venv/bin/mlflow" server --host 127.0.0.1 --port "$PORT1" \
      --backend-store-uri "sqlite:///$TD/mlflow.db" --artifacts-destination "$TD/artifacts" \
      --app-name basic-auth ) >"$TD/basic.log" 2>&1 &
pid1=$!

BASE1="http://127.0.0.1:$PORT1"
if wait_health "$PORT1"; then
  r1="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$BASE1/api/2.0/mlflow/experiments/search")"
  case "$r1" in
    401) r1_verdict="401 without credentials (a pass)" ;;
    200) r1_verdict="200 WITHOUT credentials — the app does not protect the API (option 3's defect)" ;;
    3*)  r1_verdict="$r1 redirect without credentials (a login page, not the API's answer)" ;;
    *)   r1_verdict="$r1 without credentials" ;;
  esac
  echo "R1: basic auth ($PORT1) experiments/search $r1_verdict"

  rc=0
  MLFLOW_TRACKING_URI="$BASE1" MLFLOW_TRACKING_USERNAME=admin \
  MLFLOW_TRACKING_PASSWORD="$ADMIN_PASSWORD" \
    "$TD/venv/bin/python" - >"$TD/run.out" 2>"$TD/run.err" <<'PY' || rc=$?
import mlflow
mlflow.set_experiment("spike-mlflow-auth")
with mlflow.start_run() as run:
    mlflow.log_metric("answer", 42)
    print(run.info.run_id)
PY
  run_id="$(grep -E '^[0-9a-f]{32}$' "$TD/run.out" 2>/dev/null | tail -n 1 || true)"
  exp_id="$(curl -sS --max-time 10 -u "admin:$ADMIN_PASSWORD" \
    --get --data-urlencode 'experiment_name=spike-mlflow-auth' \
    "$BASE1/api/2.0/mlflow/experiments/get-by-name" \
    | python3 -c 'import json,sys
try:
    print(json.load(sys.stdin).get("experiment", {}).get("experiment_id", ""))
except Exception:
    print("")')"
  runs="?"
  if [ -n "$exp_id" ]; then
    runs="$(curl -sS --max-time 10 -u "admin:$ADMIN_PASSWORD" -X POST \
      -H 'content-type: application/json' -d "{\"experiment_ids\":[\"$exp_id\"]}" \
      "$BASE1/api/2.0/mlflow/runs/search" \
      | python3 -c 'import json,sys
try:
    print(len(json.load(sys.stdin).get("runs", [])))
except Exception:
    print("?")')"
  fi
  bearer="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
    -H 'Authorization: Bearer spike-token' "$BASE1/api/2.0/mlflow/experiments/search")"
  r2="no default admin password in 3.18 (MLFLOW_AUTH_ADMIN_PASSWORD set per run)"
  if [ "$rc" = 0 ]; then
    r2="run written with user/password rc=0 (${run_id:0:12}…), runs/search sees ${runs} run(s); $r2; a Bearer token instead of user/password → $bearer"
  else
    r2="run refused rc=$rc ($(tail -n 1 "$TD/run.err" 2>/dev/null | cut -c1-140)); $r2; runs/search sees ${runs} run(s)"
  fi
  echo "R2: basic auth with credentials — $r2"
else
  echo "R1: basic auth ($PORT1) — the server did not answer /health in 60 s: $(tail -n 5 "$TD/basic.log" | tr '\n' ' ' | cut -c1-200)"
  echo "R2: basic auth with credentials — not run, the server did not start"
fi

# --- R3 and R4: the OIDC plugin ----------------------------------------------------------
kill -- -"$pid1" 2>/dev/null || true
wait "$pid1" 2>/dev/null || true
pid1=""

( cd "$TD" && exec env \
    MLFLOW_FLASK_SERVER_SECRET_KEY="$(python3 -c 'import secrets; print(secrets.token_hex(32))')" \
    OIDC_DISCOVERY_URL="$DISCOVERY" \
    OIDC_CLIENT_ID="$CLIENT_ID" \
    OIDC_PUBLIC_CLIENT=true \
    OIDC_REDIRECT_URI="http://127.0.0.1:$PORT2/oidc/callback" \
    OIDC_ISSUER="$ISSUER" \
    "$TD/venv/bin/mlflow" server --host 127.0.0.1 --port "$PORT2" \
      --backend-store-uri "sqlite:///$TD/mlflow-oidc.db" --artifacts-destination "$TD/artifacts" \
      --app-name oidc-auth ) >"$TD/oidc.log" 2>&1 &
pid2=$!

BASE2="http://127.0.0.1:$PORT2"
if wait_health "$PORT2"; then
  root="$(curl -sS -o /dev/null -w '%{http_code} %{redirect_url}' --max-time 10 "$BASE2/")"
  root="${root% }"
  login="$(curl -sS -o /dev/null -w '%{http_code} %{redirect_url}' --max-time 10 "$BASE2/login")"
  login_code="${login%% *}"
  login_url="${login#* }"
  flags=""
  for pair in "client_id=$CLIENT_ID" 'response_type=code' 'code_challenge_method=S256' 'scope=openid'; do
    case "$login_url" in
      *"$pair"*) flags="$flags $pair" ;;
      *) flags="$flags MISSING:$pair" ;;
    esac
  done
  secret_note=""
  grep -q 'SECRET_KEY is not configured' "$TD/oidc.log" && secret_note="; plugin warns SECRET_KEY is unset (sessions per process, invalid across replicas)"
  echo "R3: oidc-auth ($PORT2) GET / → $root; the browser entry GET /login → $login_code ${login_url:0:120} (${flags# })$secret_note"
  if [ "$login_code" != 302 ] || [ -z "$login_url" ]; then
    echo "R3: oidc-auth did not build an authorize redirect — last five log lines:" >&2
    tail -n 5 "$TD/oidc.log" >&2
  fi

  auth_py="$("$TD/venv/bin/python" -c 'import mlflow_oidc_auth.routers.auth as a; print(a.__file__)')"
  cfg_py="$("$TD/venv/bin/python" -c 'import mlflow_oidc_auth.config as c; print(c.__file__)')"
  callback_const="$(grep -m1 '^CALLBACK' "$auth_py" | tr -d ' ')"
  groups_line="$(grep -m1 'OIDC_GROUPS_ATTRIBUTE' "$cfg_py" | sed 's/^ *//')"
  served="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$BASE2/callback")"
  ticket_cb="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$BASE2/oidc/callback")"
  redirect_uri="$(printf '%s' "$login_url" | sed -n 's/.*redirect_uri=\([^&]*\).*/\1/p')"
  echo "R4: the plugin's route is $callback_const (GET /callback → $served, a state error; the ticket's /oidc/callback → $ticket_cb, not routed); the authorize redirect it builds names redirect_uri=$redirect_uri; group claim default: $groups_line"
else
  echo "R3: oidc-auth ($PORT2) — the server did not answer /health in 60 s: $(tail -n 5 "$TD/oidc.log" | tr '\n' ' ' | cut -c1-200)"
  echo "R4: callback and group claim — not run, the plugin did not start"
fi

# --- R5 footprint and licence ------------------------------------------------------------
licences="$("$TD/venv/bin/python" - <<'PY'
import importlib.metadata as m
import pathlib

def licence(name, version):
    d = m.distribution(name)
    md = d.metadata
    expr = md.get("License-Expression")
    if expr:
        return f"{name} {version} {expr} (License-Expression)"
    field = (md.get("License") or "").strip()
    lic_dir = pathlib.Path(d._path) / "licenses"
    files = sorted(lic_dir.glob("LICENSE*")) if lic_dir.is_dir() else []
    text = files[0].read_text(errors="replace") if files else ""
    if "Apache License" in text:
        return f"{name} {version} Apache-2.0 ({files[0].name}: Apache License 2.0)"
    if "MIT License" in text:
        return f"{name} {version} MIT ({files[0].name})"
    return f"{name} {version} licence field {field[:60]!r}, no LICENSE file read"

print(licence("mlflow", m.version("mlflow")) + "; " + licence("mlflow-oidc-auth", m.version("mlflow-oidc-auth")))
PY
)"
size="$(du -sh "$TD/venv" 2>/dev/null | cut -f1)"
echo "R5: $licences; virtualenv $size"
