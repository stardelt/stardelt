#!/usr/bin/env bash
# Which Superset user Nova acts as for dashboard bundles, which role it needs, and where its
# password lives (stardelt/stardelt#1210, slice a of #115).
#
#   make spike-superset-nova-identity      (NS=… PORT=… optional)
#
# Why: #115 decision 5 says Nova calls Superset's import API with its service account. The spike
# #1200 (PR #1207) measured the export/import API with the human admin. This measures the
# identity: can a dedicated user be created idempotently, does it see a dashboard it does not
# own, does the flow work from inside the cluster, and where does its password live.
#
# Every row reports what the instance *has* afterwards, not only the HTTP status. The instance
# is left as it was found: the trap deletes the user, the role, every dashboard titled
# `spike-nova-`, the charts the run created, the scratch pod and its Secret, the port-forward
# and the scratch directory — including after a `set -e` failure.
#
#   preflight          kubectl, curl, python3, jq, unzip; the Superset Deployment and `superset-admin`
#   forward            kubectl port-forward svc/superset (killed by the trap)
#   list               the starting counts (dashboards, charts, datasets) and the demo dashboard
#   admin-login        the `superset-admin` Secret, a cookie jar, the access token in a file
#   role-create        question 1: no REST endpoint creates a user or role in 5.0.0 (404, absent
#                      from /api/v1/_openapi); the role is converged in the pod instead
#   role-idempotent    the same converge a second time: the same permission count
#   user-create        the user `stardelt-nova`, created in the pod, twice
#   user-login         POST /api/v1/security/login as the service user
#   export-as-service  question 1: with `Gamma` only the list is empty and the export answers
#                      404; with the role's `datasource_access` it answers 200 and a ZIP with
#                      `dashboards/`
#   export-shop        question 2: the demo dashboard, owned by admin, exported as the service
#                      user, with its datasets and `databases/`
#   in-cluster         question 3: login/CSRF/export through `http://superset:8088` from a
#                      scratch pod, the session cookie's flags and the POST result
#   import-overwrite   question 5: the Shop archive on a dashboard the user does not own
#                      (refused), then a copy imported and re-imported with `overwrite=true`
#   cleanup            the user and the role are gone; the counts are back to the start
#   verdict            PASS (every row ok) / rows answered no / UNVERIFIED
#
# Exit 0 = done, 1 = a row answered "no", 3 = UNVERIFIED (a tool, the Deployment or the
# `superset-admin` Secret is missing, the port-forward does not answer, or a row that could not
# be measured). Nothing persistent is created: no Secret, no Deployment, no values change.
#
# No secret is printed: the admin password goes from the Secret into a 0600 file and from there
# into a JSON body on stdin; the service password is generated into a 0600 file and travels the
# same way — and into the scratch pod only through a `spike-nova-login` Secret's env, never
# argv. The access tokens go into 0600 header files (`curl -H @file`), the session cookies into
# 0600 jars. No database URI is printed either: the bundle's `databases/` file is reported by
# key, and the URI only as scheme and whether it carries a password.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-${NAMESPACE:-stardelt}}"
PORT="${PORT:-18088}"
BASE="http://127.0.0.1:$PORT"
DEPLOY="${DEPLOY:-superset}"
SECRET="${SECRET:-superset-admin}"
ROLE="${ROLE:-stardelt-nova}"
SVC_USER="${SVC_USER:-stardelt-nova}"
CLUSTER_URL="${CLUSTER_URL:-http://superset:8088}"
POD="${POD:-spike-nova-shell}"
POD_NAME="$POD-$(date -u +%H%M%S)"
POD_SECRET="${POD_SECRET:-spike-nova-login}"
CONTEXT="${CONTEXT:-}"
CURL_IMAGE="${CURL_IMAGE:-curlimages/curl:8.10.1}"

scratch="${TMPDIR:-$HOME/.cache}"
mkdir -p "$scratch"
tmp="$(mktemp -d "$scratch/spike-superset-nova.XXXXXX")"
ts="$(date -u +%Y%m%dT%H%M%SZ)"

PF_PID=""
forwarded=no
finished=no
failed=0
unverified=""
csrf=""
svc_csrf=""
start_dash=""
start_charts=""
start_datasets=""
max_chart=""

row() { printf '%-22s %s\n' "$1" "$2"; }
ok() { echo "✓ $*"; }
no() { echo "✗ $*"; failed=1; }
untested() { echo "UNVERIFIED ($1)"; unverified="$1"; }

kc() { # kubectl, with the pinned context when one is given
  if [ -n "$CONTEXT" ]; then kubectl --context "$CONTEXT" "$@"; else kubectl "$@"; fi
}

# --- in-pod helper --------------------------------------------------------------------------
# Passed to `python -c` (script text only, never a secret) with a JSON document on stdin: the
# idempotent shapes of `superset-analyst-role.py` (a role converged to an exact permission set)
# and of Superset's own create-user. Only stdin and stdout cross the pod boundary.
cat > "$tmp/pod.py" <<'PY'
import json, sys

cmd = sys.argv[1]
spec = json.load(sys.stdin)

from superset.app import create_app

app = create_app()
with app.app_context():
    from superset import db, security_manager as sm

    def names(role):
        return {(p.permission.name, p.view_menu.name) for p in role.permissions}

    if cmd == "role":
        from superset.models.dashboard import Dashboard
        dash = next((d for d in db.session.query(Dashboard).order_by(Dashboard.id)
                     if d.dashboard_title == spec["dashboard"]), None)
        if dash is None:
            print("no dashboard titled %r" % spec["dashboard"])
            raise SystemExit(0)
        grants, databases = set(), set()
        for chart in dash.slices:
            ds = chart.datasource
            if ds is None or getattr(ds, "database", None) is None:
                continue
            grants.add(("datasource_access", ds.perm))
            databases.add(ds.database.perm)
        want = names(sm.find_role("Gamma")) | grants
        role = sm.find_role(spec["role"]) or sm.add_role(spec["role"])
        before = len(names(role))
        have = names(role)
        for perm, view in sorted(want - have):
            pv = sm.find_permission_view_menu(perm, view)
            if pv is None:
                print("missing-permview %s|%s" % (perm, view))
                continue
            sm.add_permission_role(role, pv)
        for perm, view in sorted(have - want):
            pv = sm.find_permission_view_menu(perm, view)
            if pv is not None:
                sm.del_permission_role(role, pv)
        db.session.commit()
        print("role=%s before=%d after=%d grants=%d databases=%d gamma=%d"
              % (role.name, before, len(names(role)), len(grants), len(databases),
                 len(names(sm.find_role("Gamma")))))
        print("databases:" + ";".join(sorted(databases)))

    elif cmd == "user":
        existing = sm.find_user(username=spec["user"])
        if existing:
            print("user=%s state=exists roles=%s"
                  % (spec["user"], ",".join(r.name for r in existing.roles)))
        else:
            role = sm.find_role(spec["role"])
            if role is None:
                print("role=%s state=missing" % spec["role"])
                raise SystemExit(0)
            sm.add_user(spec["user"], "Nova", "Superset", spec["user"] + "@svc.stardelt.local",
                        role, spec["password"])
            db.session.commit()
            print("user=%s state=created roles=%s" % (spec["user"], spec["role"]))

    elif cmd == "roles":  # exactly these roles for the user
        user = sm.find_user(username=spec["user"])
        user.roles = [sm.find_role(n) for n in spec["roles"]]
        db.session.commit()
        print("user=%s roles=%s" % (spec["user"], ",".join(r.name for r in user.roles)))

    elif cmd == "check":
        user = sm.find_user(username=spec["user"])
        role = sm.find_role(spec["role"])
        print("user=%s role=%s" % (user.username if user else "none", role.name if role else "none"))

    elif cmd == "drop":
        user = sm.find_user(username=spec["user"])
        if user:
            db.session.delete(user)
            db.session.commit()
        role = sm.find_role(spec["role"])
        if role:
            role.permissions = []
            db.session.commit()
            db.session.delete(role)
            db.session.commit()
        print("user=%s role=%s" % (sm.find_user(username=spec["user"]) and "present" or "none",
                                   sm.find_role(spec["role"]) and "present" or "none"))
PY

pod_py() { # <command>; the JSON goes in on stdin
  kc -n "$NS" exec -i "deploy/$DEPLOY" -c "$DEPLOY" -- python -c "$(cat "$tmp/pod.py")" "$1" 2>/dev/null \
    | sed '/^Loaded your LOCAL configuration/d' 
}

# --- scratch state --------------------------------------------------------------------------
: > "$tmp/created-dash"
: > "$tmp/created-charts"

# shellcheck disable=SC2329  # called from the trap, from give_up and from the cleanup row
cleanup() {
  set +e
  if [ -s "$tmp/created-dash" ]; then
    bulk_delete dashboard "$(cat "$tmp/created-dash")" >/dev/null 2>&1
  fi
  if [ -s "$tmp/created-charts" ]; then
    bulk_delete chart "$(cat "$tmp/created-charts")" >/dev/null 2>&1
  fi
  pod_py drop <<< "{\"user\":\"$SVC_USER\",\"role\":\"$ROLE\"}" >/dev/null 2>&1
  kc -n "$NS" delete pod "$POD_NAME" --ignore-not-found --timeout=120s >/dev/null 2>&1
  kc -n "$NS" delete secret "$POD_SECRET" --ignore-not-found >/dev/null 2>&1
  set -e
}

finish() {
  set +e
  if [ "$forwarded" = yes ] && [ -n "$PF_PID" ]; then
    kill "$PF_PID" >/dev/null 2>&1
  fi
  rm -rf "$tmp"
  finished=yes
}

trap '{ [ "$finished" = yes ] || { cleanup; finish; }; }' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

give_up() { # <unverified reason>: the row, the verdict, the cleanup, exit 3
  untested "$1"
  echo "spike-superset-nova-identity: UNVERIFIED ($1)"
  cleanup
  finish
  exit 3
}

# --- HTTP -----------------------------------------------------------------------------------
get() { # <jar> <auth file> <path with query> <out file> -> status code
  local code
  code="$(curl -gsS --max-time 30 -b "$1" -c "$1" -H @"$2" "$BASE$3" -o "$4" \
      -w '%{http_code}' 2>/dev/null)" || code=000
  echo "${code:-000}"
}

post_form() { # <jar> <auth> <csrf> <url path> <form args…> -> status code, body in $tmp/body.json
  local jar="$1" auth="$2" token="$3" url="$4"
  shift 4
  local args=() field code
  for field in "$@"; do args+=(-F "$field"); done
  code="$(curl -sS -g --max-time 60 -b "$jar" -c "$jar" -H @"$auth" \
      -H "X-CSRFToken: $token" -H "Referer: $BASE/" "${args[@]}" "$BASE$url" \
      -o "$tmp/body.json" -w '%{http_code}' 2>/dev/null)" || code=000
  echo "${code:-000}"
}

bulk_delete() { # <api type> <comma separated ids> -> status code (5.0.0 has only this route)
  local code
  [ -n "$2" ] || { echo 000; return; }
  code="$(curl -gsS --max-time 30 -b "$tmp/cookies" -c "$tmp/cookies" -H @"$tmp/auth" \
      -H "X-CSRFToken: $csrf" -H "Referer: $BASE/" -X DELETE "$BASE/api/v1/$1/?q=[$2]" \
      -o "$tmp/delete.json" -w '%{http_code}' 2>/dev/null)" || code=000
  echo "${code:-000}"
}

body_line() { head -c 200 "$tmp/body.json" | tr -d '\r\n' | cut -c1-160; }

# The reason of a failed import is a WARNING or a traceback line in the Superset log; the time
# window keeps an earlier run's line out of the answer.
import_reason() { # <since, e.g. 3m>
  kc -n "$NS" logs "deploy/$DEPLOY" --since="${1:-3m}" 2>/dev/null \
    | grep -E 'Import Error:|ImportFailedError:|SupersetException:' | tail -1 \
    | sed -E 's/^.*(Import Error:|ImportFailedError:|SupersetException:) ?//' | cut -c1-300 || true
}

json_csrf() { # <jar> <auth> <out token file> -> status code
  local code
  code="$(get "$1" "$2" /api/v1/security/csrf_token/ "$3.json")"
  jq -r '.result // empty' "$3.json" 2>/dev/null > "$3" || true
  echo "$code"
}

login_jar() { # <username file> <password file> <jar> <auth out> -> status code
  python3 -c '
import json, sys
print(json.dumps({"username": open(sys.argv[1]).read().strip(),
                  "password": open(sys.argv[2]).read().strip(),
                  "provider": "db", "refresh": True}))' "$1" "$2" > "$tmp/login.json"
  local code
  code="$(curl -sS --max-time 30 -c "$3" -H 'Content-Type: application/json' \
      -X POST --data @"$tmp/login.json" -o "$tmp/login.out" -w '%{http_code}' \
      "$BASE/api/v1/security/login" 2>/dev/null)" || code=000
  rm -f "$tmp/login.json"
  printf 'Authorization: Bearer %s\n' "$(jq -r '.access_token // empty' "$tmp/login.out")" > "$4"
  chmod 600 "$3" "$4"
  echo "${code:-000}"
}

# --- bundle builder -------------------------------------------------------------------------
# A copy of an export with a fresh dashboard uuid, slug and title and fresh chart uuids (the
# dashboard's `position` carries the chart uuids, so they are replaced there too). Datasets and
# `databases/` keep their files: their uuids are the target's, so the importer returns the
# existing rows instead of creating new ones. Prints the new dashboard uuid.
cat > "$tmp/copy.py" <<'PY'
import os, re, sys, uuid, zipfile

src, out, title, top = sys.argv[1:5]
slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
files = sorted(os.path.relpath(os.path.join(r, n), src).replace(os.sep, "/")
               for r, _, ns in os.walk(src) for n in ns)
texts = {f: open(os.path.join(src, f), encoding="utf-8").read() for f in files}


def value(text, key):
    m = re.search(r"^" + key + r": *(\S+) *$", text, re.M)
    return m.group(1) if m else None


def set_value(text, key, new):
    return re.sub(r"^" + key + r":.*$", key + ": " + new, text, count=1, flags=re.M)


charts = {}
for rel in [f for f in files if f.startswith("charts/")]:
    old = value(texts[rel], "uuid")
    charts[old] = str(uuid.uuid4())
    texts[rel] = set_value(texts[rel], "uuid", charts[old])
dash_uuid = None
for rel in [f for f in files if f.startswith("dashboards/")]:
    text = texts[rel]
    for old, new in charts.items():
        text = text.replace(old, new)
    dash_uuid = str(uuid.uuid4())
    text = set_value(text, "dashboard_title", title)
    text = set_value(text, "slug", slug)
    text = set_value(text, "uuid", dash_uuid)
    texts[rel] = text
with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as bundle:
    for rel, text in texts.items():
        bundle.writestr(top + "/" + rel, text)
print(dash_uuid)
PY

# --- preflight ------------------------------------------------------------------------------
missing=""
for tool in kubectl curl python3 jq unzip; do
  command -v "$tool" >/dev/null 2>&1 || missing="$missing $tool"
done
if [ -n "$missing" ]; then
  row preflight "missing:$missing"
  untested "missing:$missing"
  echo "spike-superset-nova-identity: UNVERIFIED"
  exit 3
fi
row preflight "kubectl curl python3 jq unzip"

if ! kc -n "$NS" get "deploy/$DEPLOY" >/dev/null 2>&1; then
  row forward "no Deployment $NS/$DEPLOY"
  unverified="the Superset Deployment is not installed"
  echo "spike-superset-nova-identity: UNVERIFIED"
  exit 3
fi
if ! kc -n "$NS" get secret "$SECRET" >/dev/null 2>&1; then
  row admin-login "no Secret $NS/$SECRET"
  unverified="the $SECRET Secret is not installed"
  echo "spike-superset-nova-identity: UNVERIFIED"
  exit 3
fi

# --- forward --------------------------------------------------------------------------------
answers() {
  local code
  code="$(curl -s -o /dev/null --max-time 2 -w '%{http_code}' "$BASE/health" 2>/dev/null)" || code=000
  [ "${code:-000}" != "000" ]
}
if answers; then
  row forward "already listening on $PORT (reused, not killed by the trap)"
else
  # not through kc: `$!` must be the kubectl process the trap kills, not a subshell that
  # leaves the forward running behind it
  if [ -n "$CONTEXT" ]; then
    kubectl --context "$CONTEXT" -n "$NS" port-forward "svc/$DEPLOY" "$PORT:8088" >"$tmp/pf.log" 2>&1 &
  else
    kubectl -n "$NS" port-forward "svc/$DEPLOY" "$PORT:8088" >"$tmp/pf.log" 2>&1 &
  fi
  PF_PID=$!
  forwarded=yes
  for _ in $(seq 1 30); do
    answers && break
    sleep 1
  done
  answers || { row forward "no answer on $PORT within 30s"; give_up "superset did not answer through the port-forward"; }
  row forward "svc/$DEPLOY $PORT:8088"
fi

# --- admin-login ----------------------------------------------------------------------------
kc -n "$NS" get secret "$SECRET" -o jsonpath='{.data.username}' | base64 -d > "$tmp/admin-user"
kc -n "$NS" get secret "$SECRET" -o jsonpath='{.data.password}' | base64 -d > "$tmp/admin-pass"
chmod 600 "$tmp/admin-user" "$tmp/admin-pass"
code="$(login_jar "$tmp/admin-user" "$tmp/admin-pass" "$tmp/cookies" "$tmp/auth")"
rm -f "$tmp/admin-pass"
if [ "$code" != 200 ] || [ ! -s "$tmp/auth" ]; then
  row admin-login "$code (no access_token)"
  give_up "the admin login answered $code"
fi
row admin-login "200 (access_token in a 0600 file, cookie jar in a 0600 file)"
code="$(json_csrf "$tmp/cookies" "$tmp/auth" "$tmp/csrf")"
csrf="$(cat "$tmp/csrf")"
[ "$code" = 200 ] && [ -n "$csrf" ] || give_up "no CSRF token for the admin session ($code)"

# --- list -----------------------------------------------------------------------------------
code="$(get "$tmp/cookies" "$tmp/auth" '/api/v1/dashboard/?q=(page_size:200)' "$tmp/dash.json")"
[ "$code" = 200 ] || give_up "the dashboard list answered $code"
start_dash="$(jq -r '.count' "$tmp/dash.json")"
code="$(get "$tmp/cookies" "$tmp/auth" '/api/v1/chart/?q=(page_size:200)' "$tmp/chart.json")"
[ "$code" = 200 ] || give_up "the chart list answered $code"
start_charts="$(jq -r '.count' "$tmp/chart.json")"
max_chart="$(jq -r '[.result[].id]|max // 0' "$tmp/chart.json")"
code="$(get "$tmp/cookies" "$tmp/auth" '/api/v1/dataset/?q=(page_size:200)' "$tmp/dataset.json")"
[ "$code" = 200 ] || give_up "the dataset list answered $code"
start_datasets="$(jq -r '.count' "$tmp/dataset.json")"

shop_title="$(jq -r '.result[]?|select(.dashboard_title|test("^Shop"))|.dashboard_title' "$tmp/dash.json" | head -1)"
shop_id="$(jq -r '.result[]?|select(.dashboard_title|test("^Shop"))|.id' "$tmp/dash.json" | head -1)"
[ -n "$shop_id" ] || give_up "the instance has no dashboard titled Shop"
shop_owners="$(jq -r --arg id "$shop_id" '.result[]?|select((.id|tostring)==$id)|[(.owners//[])|.[]|(.username // ((.first_name // "") + " " + (.last_name // "")))]|join(",")' "$tmp/dash.json")"
row list "dashboards=$start_dash charts=$start_charts datasets=$start_datasets; demo: id=$shop_id owners=${shop_owners:-unlisted}"

# --- role-create (question 1) ---------------------------------------------------------------
# The REST API of 5.0.0 has no role or user endpoint: `/api/v1/security/roles/` and
# `/api/v1/security/users/` answer 404 and appear in no `paths` of /api/v1/_openapi. FAB's
# RoleApi/UserApi exist in the package but the Superset app never registers them, so a user is
# created the way `superset-analyst-role.py` already does its role: in the pod, through the
# security manager, converged (re-running is a no-op).
code="$(get "$tmp/cookies" "$tmp/auth" '/api/v1/_openapi' "$tmp/openapi.json")"
paths="$(jq -r '[.paths|keys[]|select(test("security/(roles|users)"))]|length' "$tmp/openapi.json" 2>/dev/null || echo unreadable)"
roles_rest="$(curl -sS --max-time 30 -b "$tmp/cookies" -H @"$tmp/auth" \
    -H "X-CSRFToken: $csrf" -H "Referer: $BASE/" -X POST -H 'Content-Type: application/json' \
    --data '{"name":"spike-nova-probe"}' -o /dev/null -w '%{http_code}' \
    "$BASE/api/v1/security/roles/" 2>/dev/null)" || roles_rest=000
users_rest="$(curl -sS --max-time 30 -b "$tmp/cookies" -H @"$tmp/auth" \
    -H "X-CSRFToken: $csrf" -H "Referer: $BASE/" -X POST -H 'Content-Type: application/json' \
    --data '{"first_name":"spike","last_name":"probe","username":"spike-nova-probe","password":"x"}' \
    -o /dev/null -w '%{http_code}' "$BASE/api/v1/security/users/" 2>/dev/null)" || users_rest=000
row role-create "REST POST /security/roles/ $roles_rest, /security/users/ $users_rest; _openapi paths matching security/(roles|users): $paths"
if [ "$roles_rest" = 404 ] && [ "$users_rest" = 404 ] && [ "$paths" = 0 ]; then
  ok "no REST endpoint creates a Superset user or role in 5.0.0: both answer 404 and neither is in _openapi"
else
  no "the REST role/user endpoints answered $roles_rest/$users_rest ($paths paths): re-read question 1"
fi

role_out="$(printf '{"role":"%s","dashboard":"%s"}' "$ROLE" "$shop_title" | pod_py role || true)"
echo "  ${role_out:-no output}"
role_after="$(sed -n 's/.*after=\([0-9]*\).*/\1/p' <<<"$role_out")"
granted_n="$(sed -n 's/.*grants=\([0-9]*\).*/\1/p' <<<"$role_out")"
row role-converge "role=$ROLE converged in the pod: perms=$role_after (was $(sed -n 's/.*before=\([0-9]*\).*/\1/p' <<<"$role_out")), datasource grants of '$shop_title': $granted_n"
if [ -z "$role_after" ] || [ "$granted_n" = 0 ] || [[ "$role_out" == *missing-permview* ]]; then
  no "the role did not converge ($role_out)"
else
  ok "the role $ROLE holds Gamma's $(sed -n 's/.*gamma=\([0-9]*\).*/\1/p' <<<"$role_out") permissions plus $granted_n datasource grants"
fi
sed -n 's/^databases://p' <<<"$role_out" | tr ';' '\n' | sed '/^$/d' | sed 's/^/  database: /' | head -10

role_out2="$(printf '{"role":"%s","dashboard":"%s"}' "$ROLE" "$shop_title" | pod_py role || true)"
role_after2="$(sed -n 's/.*after=\([0-9]*\).*/\1/p' <<<"$role_out2")"
row role-idempotent "second converge: before=$(sed -n 's/.*before=\([0-9]*\).*/\1/p' <<<"$role_out2") after=$role_after2"
if [ -n "$role_after2" ] && [ "$role_after" = "$role_after2" ]; then
  ok "a second converge leaves the same $role_after2 permissions"
else
  no "the second converge changed the role ($role_after then $role_after2)"
fi

# --- user-create ----------------------------------------------------------------------------
# no trailing newline: the file becomes the Secret's value, and a newline inside the JSON
# login body makes Superset answer 400
printf '%s' "$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')" > "$tmp/pass"
chmod 600 "$tmp/pass"
user_out="$(printf '{"user":"%s","role":"%s","password":"%s"}' "$SVC_USER" "$ROLE" "$(cat "$tmp/pass")" | pod_py user || true)"
user_again="$(printf '{"user":"%s","role":"%s","password":"%s"}' "$SVC_USER" "$ROLE" "$(cat "$tmp/pass")" | pod_py user || true)"
row user-create "first: ${user_out:-no output}; second: ${user_again:-no output}"
if [[ "$user_out" == *state=created* ]] && [[ "$user_again" == *state=exists* ]]; then
  ok "the user $SVC_USER is created idempotently (a second run reports it exists and keeps the password)"
else
  no "the user was not created idempotently ($user_out / $user_again)"
fi

# --- user-login -----------------------------------------------------------------------------
printf '%s\n' "$SVC_USER" > "$tmp/svc-user"
code="$(login_jar "$tmp/svc-user" "$tmp/pass" "$tmp/svc-cookies" "$tmp/svc-auth")"
row user-login "$code (access_token in a 0600 file)"
if [ "$code" = 200 ] && [ -s "$tmp/svc-auth" ]; then
  ok "POST /api/v1/security/login as $SVC_USER answers 200"
  code="$(json_csrf "$tmp/svc-cookies" "$tmp/svc-auth" "$tmp/svc-csrf")"
  svc_csrf="$(cat "$tmp/svc-csrf")"
else
  no "the service user's login answered $code"
fi

# --- export-as-service (question 1) ---------------------------------------------------------
# First with `Gamma` only: the starting point of a fresh Superset user.
printf '{"user":"%s","roles":["Gamma"]}' "$SVC_USER" | pod_py roles >/dev/null 2>&1 || true
code="$(get "$tmp/svc-cookies" "$tmp/svc-auth" '/api/v1/dashboard/?q=(page_size:200)' "$tmp/svc-dash.json")"
list_gamma="$(jq -r '.count' "$tmp/svc-dash.json" 2>/dev/null || echo unreadable)"
gamma_export="$(curl -gsS --max-time 30 -b "$tmp/svc-cookies" -c "$tmp/svc-cookies" -H @"$tmp/svc-auth" \
    "$BASE/api/v1/dashboard/export/?q=[$shop_id]" -o "$tmp/svc-gamma.zip" -w '%{http_code}' 2>/dev/null)" || gamma_export=000
row export-as-service "Gamma only: list=$list_gamma export=$gamma_export $(head -c 60 "$tmp/svc-gamma.zip" 2>/dev/null | tr -dc '[:print:]')"

# Then with the role: Gamma's set plus the dashboard's datasource grants.
printf '{"user":"%s","roles":["Gamma","%s"]}' "$SVC_USER" "$ROLE" | pod_py roles >/dev/null 2>&1 || true
code="$(get "$tmp/svc-cookies" "$tmp/svc-auth" '/api/v1/dashboard/?q=(page_size:200)' "$tmp/svc-dash.json")"
list_role="$(jq -r '.count' "$tmp/svc-dash.json" 2>/dev/null || echo unreadable)"
code="$(curl -gsS --max-time 30 -b "$tmp/svc-cookies" -c "$tmp/svc-cookies" -H @"$tmp/svc-auth" \
    "$BASE/api/v1/dashboard/export/?q=[$shop_id]" -o "$tmp/svc.zip" -w '%{http_code}' 2>/dev/null)" || code=000
size="$(wc -c < "$tmp/svc.zip" 2>/dev/null || echo 0)"
if python3 -c 'import zipfile,sys; sys.exit(0 if any("/dashboards/" in n for n in zipfile.ZipFile(sys.argv[1]).namelist()) else 1)' "$tmp/svc.zip" 2>/dev/null; then has_dash=yes; else has_dash=no; fi
row export-as-service "with $ROLE: list=$list_role export=$code $size bytes, dashboards/ in the ZIP: $has_dash"
if [ "$code" = 200 ] && [ "$has_dash" = yes ] && [ "$size" -gt 0 ] && [ "$gamma_export" = 404 ]; then
  ok "Gamma alone sees no dashboard and the export answers 404; the same user with $ROLE gets a ZIP with dashboards/"
elif [ "$code" = 200 ] && [ "$has_dash" = yes ]; then
  ok "the export with $ROLE answers 200 with dashboards/ (Gamma alone: list=$list_gamma export=$gamma_export)"
else
  no "the export as $SVC_USER answered $code ($size bytes, dashboards/: $has_dash)"
fi

# --- export-shop (question 2) ---------------------------------------------------------------
mkdir -p "$tmp/ex" "$tmp/src"
unzip -q "$tmp/svc.zip" -d "$tmp/ex" 2>/dev/null || true
top_dir="$(find "$tmp/ex" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -1)"
[ -n "$top_dir" ] && cp -r "$top_dir/." "$tmp/src/" 2>/dev/null || true
files="$(find "$tmp/src" -type f 2>/dev/null | wc -l | tr -d ' ')"
datasets_n="$(find "$tmp/src/datasets" -name '*.yaml' 2>/dev/null | wc -l | tr -d ' ')"
db_n="$(find "$tmp/src/databases" -name '*.yaml' 2>/dev/null | wc -l | tr -d ' ')"
row export-shop "'$shop_title' (id=$shop_id, owners=${shop_owners:-unlisted}) as $SVC_USER: files=$files datasets=$datasets_n databases=$db_n"
if [ "$files" -gt 0 ] && [ "$datasets_n" -gt 0 ] && [ "$db_n" -gt 0 ]; then
  ok "a service user that is not an owner exports the dashboard with its datasets and databases/"
else
  no "the export of '$shop_title' carried $files files ($datasets_n datasets, $db_n databases)"
fi
db_file="$(find "$tmp/src/databases" -name '*.yaml' 2>/dev/null | head -1)"
if [ -n "$db_file" ]; then
  python3 - "$db_file" <<'PY'
import sys
text = open(sys.argv[1], encoding="utf-8").read()
uri = next((l.split(": ", 1)[1] for l in text.splitlines() if l.startswith("sqlalchemy_uri")), "")
scheme = uri.split("://")[0] if "://" in uri else "none"
rest = uri.split("://", 1)[1] if "://" in uri else ""
userinfo = rest.split("/", 1)[0] if "@" in rest.split("/", 1)[0] else ""
userinfo = userinfo.rsplit("@", 1)[0] if "@" in userinfo else ""
keys = [l.split(":", 1)[0].strip() for l in text.splitlines() if l and not l.startswith(" ")]
print("  databases/ keys: %s" % ",".join(keys))
print("  uri scheme=%s userinfo=%s password-in-uri=%s"
      % (scheme, "yes" if userinfo else "no", "yes" if ":" in userinfo else "no"))
PY
fi

# --- in-cluster (question 3) ----------------------------------------------------------------
# a pod or Secret a killed run left behind would answer in place of this run's
kc -n "$NS" delete pod "$POD_NAME" --ignore-not-found --timeout=120s >/dev/null 2>&1 || true
kc -n "$NS" delete secret "$POD_SECRET" --ignore-not-found --timeout=60s >/dev/null 2>&1 || true
kc -n "$NS" create secret generic "$POD_SECRET" --from-file=password="$tmp/pass" >/dev/null 2>&1 || true
kc -n "$NS" run "$POD_NAME" --image="$CURL_IMAGE" --restart=Never --quiet \
  --overrides "{\"spec\":{\"containers\":[{\"name\":\"$POD_NAME\",\"image\":\"$CURL_IMAGE\",\"stdin\":true,\"command\":[\"sleep\",\"600\"],\"securityContext\":{\"runAsNonRoot\":true,\"runAsUser\":65534,\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]},\"seccompProfile\":{\"type\":\"RuntimeDefault\"}},\"env\":[{\"name\":\"SUPERSET_PASSWORD\",\"valueFrom\":{\"secretKeyRef\":{\"name\":\"$POD_SECRET\",\"key\":\"password\"}}}]}]}}" >/dev/null 2>&1 || true
if kc -n "$NS" wait --for=condition=Ready "pod/$POD_NAME" --timeout=180s >/dev/null 2>&1; then
  cat > "$tmp/in-cluster.sh" <<'SH'
set -u
B="$1"
d=$(mktemp -d); cd "$d"
printf 'password-bytes=%s\n' "${#SUPERSET_PASSWORD}"
login=$(curl -sS -w '\n%{http_code}' -H 'Content-Type: application/json' -X POST \
  --data "{\"username\":\"$SUPERSET_USER\",\"password\":\"$SUPERSET_PASSWORD\",\"provider\":\"db\",\"refresh\":true}" \
  "$B/api/v1/security/login")
login_code=$(printf '%s' "$login" | tail -1)
tok=$(printf '%s' "$login" | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
printf 'login=%s token-bytes=%s\n' "$login_code" "${#tok}"
printf 'Authorization: Bearer %s\n' "$tok" > auth
code=$(curl -sS -D h -H @auth -o csrf.json -w '%{http_code}' "$B/api/v1/security/csrf_token/")
flags=$(sed -n 's/^[Ss]et-[Cc]ookie: session=[^;]*;\(.*\)$/\1/p' h | tr -d '\r' | sed 's/^ *//')
session=$(sed -n 's/^[Ss]et-[Cc]ookie: session=\([^;]*\).*/\1/p' h)
csrf=$(sed -n 's/.*"result":"\([^"]*\)".*/\1/p' csrf.json)
printf 'csrf=%s csrf-bytes=%s\n' "$code" "${#csrf}"
printf 'cookie-flags=%s\n' "${flags:-none}"
code=$(curl -gsS -H @auth -o exp.zip -w '%{http_code}' "$B/api/v1/dashboard/export/?q=[$SUPERSET_DASHBOARD]")
printf 'export=%s bytes=%s\n' "$code" "$(wc -c < exp.zip 2>/dev/null || echo 0)"
# curl does not store a Secure cookie received over http from a non-localhost name, so this
# POST carries no session and Superset redirects it to the login page.
code=$(curl -sS -H @auth -H "X-CSRFToken: $csrf" -H "Referer: $B/" -D h1 \
  -F "formData=@exp.zip" -F 'overwrite=true' -o imp1.json -w '%{http_code}' "$B/api/v1/dashboard/import/")
printf 'post-without-session=%s location=%s\n' "$code" "$(sed -n 's/^[Ll]ocation: *//p' h1 | tr -d '\r')"
# the same POST with the session cookie the server set, injected by hand: it reaches the app
code=$(curl -sS -H @auth -H "Cookie: session=$session" -H "X-CSRFToken: $csrf" -H "Referer: $B/" \
  -F "formData=@exp.zip" -F 'overwrite=true' -o imp2.json -w '%{http_code}' "$B/api/v1/dashboard/import/")
printf 'post-with-session=%s body=%s\n' "$code" "$(head -c 60 imp2.json | tr -d '\r\n')"
SH
  in_out="$(kc -n "$NS" exec -i "$POD_NAME" -- env SUPERSET_USER="$SVC_USER" SUPERSET_DASHBOARD="$shop_id" \
      sh -s "$CLUSTER_URL" < "$tmp/in-cluster.sh" 2>/dev/null || true)"
  while IFS= read -r in_line; do echo "  $in_line"; done <<< "${in_out:-  (no output)}"
  c_login="$(sed -n 's/^login=\([0-9]*\).*/\1/p' <<<"$in_out")"
  c_csrf="$(sed -n 's/^csrf=\([0-9]*\).*/\1/p' <<<"$in_out")"
  c_export="$(sed -n 's/^export=\([0-9]*\).*/\1/p' <<<"$in_out")"
  c_flags="$(sed -n 's/^cookie-flags=//p' <<<"$in_out")"
  c_post1="$(sed -n 's/^post-without-session=\([0-9]*\).*/\1/p' <<<"$in_out")"
  c_post2="$(sed -n 's/^post-with-session=\([0-9]*\).*/\1/p' <<<"$in_out")"
  c_loc="$(sed -n 's/^post-without-session=[0-9]* location=//p' <<<"$in_out")"
  row in-cluster "$CLUSTER_URL: login=$c_login csrf=$c_csrf export=$c_export, cookie '${c_flags:-none}'; POST without the session $c_post1${c_loc:+ -> $c_loc}, with it $c_post2"
  if [ "$c_login" = 200 ] && [ "$c_csrf" = 200 ] && [ "$c_export" = 200 ] && [ "$c_post1" = 302 ] && [ -n "$c_post2" ] && [ "$c_post2" != 302 ]; then
    ok "GETs work from a pod with the bearer token; the CSRF POST needs the Secure session cookie, which a client does not present over plain http to a cluster name"
  else
    no "the in-cluster flow differed (csrf=$c_csrf export=$c_export post=$c_post1/$c_post2, cookie '${c_flags:-none}')"
  fi
else
  row in-cluster "no Ready pod ($CURL_IMAGE)"
  unverified="the scratch pod did not start"
fi

# --- import-overwrite (question 5) ----------------------------------------------------------
copy_title="spike-nova-$ts"
python3 "$tmp/copy.py" "$tmp/src" "$tmp/copy.zip" "$copy_title" "spike_nova_$ts" >/dev/null
shop_code="$(post_form "$tmp/svc-cookies" "$tmp/svc-auth" "$svc_csrf" /api/v1/dashboard/import/ \
    "formData=@$tmp/svc.zip" 'overwrite=true')"
sleep 2
shop_reason="$(import_reason 3m)"
row import-overwrite "the Shop archive on its existing dashboard: $shop_code ${shop_reason:-no reason in the log}"
code_copy="$(post_form "$tmp/svc-cookies" "$tmp/svc-auth" "$svc_csrf" /api/v1/dashboard/import/ \
    "formData=@$tmp/copy.zip" 'overwrite=true')"
copy_body="$(body_line)"
sleep 2
code_again="$(post_form "$tmp/svc-cookies" "$tmp/svc-auth" "$svc_csrf" /api/v1/dashboard/import/ \
    "formData=@$tmp/copy.zip" 'overwrite=true')"
again_body="$(body_line)"
code="$(get "$tmp/svc-cookies" "$tmp/svc-auth" '/api/v1/dashboard/?q=(page_size:200)' "$tmp/svc-dash2.json")"
copy_id="$(jq -r --arg t "$copy_title" '.result[]?|select(.dashboard_title==$t)|.id' "$tmp/svc-dash2.json" | head -1)"
copy_ids="$(jq -r --arg t "$copy_title" '[.result[]?|select(.dashboard_title==$t)]|length' "$tmp/svc-dash2.json")"
row import-overwrite "the copy '$copy_title': first $code_copy $copy_body, again $code_again $again_body, dashboards with the title: $copy_ids, id=$copy_id"
[ -n "$copy_id" ] && printf '%s' "$copy_id" > "$tmp/created-dash"
# the charts the copy brought are new uuids: remember every chart id above the starting maximum
code="$(get "$tmp/cookies" "$tmp/auth" '/api/v1/chart/?q=(page_size:200)' "$tmp/chart2.json")"
jq -r --argjson max "$max_chart" '[.result[]|select(.id > $max)]|map(.id)|join(",")' "$tmp/chart2.json" \
  > "$tmp/created-charts"
if [ "$code_copy" = 200 ] && [ "$code_again" = 200 ] && [ -n "$copy_id" ] && [ "$copy_ids" = 1 ]; then
  ok "an import as $SVC_USER keeps one dashboard ($copy_id); a second import with overwrite=true answers 200 with the same id"
else
  no "the copy import answered $code_copy/$code_again ($copy_ids dashboards, id '${copy_id:-none}')"
fi
case "$shop_reason" in
  *permissions*overwrite*) ok "the Shop archive is refused on a dashboard $SVC_USER does not own: $shop_reason" ;;
  *) no "the Shop archive was not refused for ownership (row: '${shop_reason:-none}'): re-read question 5" ;;
esac

# --- cleanup --------------------------------------------------------------------------------
cleanup
code="$(get "$tmp/cookies" "$tmp/auth" '/api/v1/dashboard/?q=(page_size:200)' "$tmp/dash3.json")"
now_dash="$(jq -r '.count' "$tmp/dash3.json")"
code="$(get "$tmp/cookies" "$tmp/auth" '/api/v1/chart/?q=(page_size:200)' "$tmp/chart3.json")"
now_charts="$(jq -r '.count' "$tmp/chart3.json")"
code="$(get "$tmp/cookies" "$tmp/auth" '/api/v1/dataset/?q=(page_size:200)' "$tmp/dataset3.json")"
now_datasets="$(jq -r '.count' "$tmp/dataset3.json")"
left="$(jq -r '[.result[]?|select(.dashboard_title|startswith("spike-nova-"))]|length' "$tmp/dash3.json")"
idents="$(printf '{"user":"%s","role":"%s"}' "$SVC_USER" "$ROLE" | pod_py check || true)"
row cleanup "user/role: ${idents:-unreadable}; dashboards $now_dash/$start_dash (spike-nova- left: $left), charts $now_charts/$start_charts, datasets $now_datasets/$start_datasets"
if [[ "$idents" == *"user=none role=none"* ]] && [ "$now_dash" = "$start_dash" ] && [ "$left" = 0 ] \
    && [ "$now_charts" = "$start_charts" ] && [ "$now_datasets" = "$start_datasets" ]; then
  ok "the user, the role and every object this run created are gone; the instance is back to its starting state"
else
  no "the instance is not back to its starting state (${idents:-unreadable}, dashboards $now_dash vs $start_dash, spike-nova- left $left, charts $now_charts vs $start_charts, datasets $now_datasets vs $start_datasets)"
fi
kc -n "$NS" delete secret "$POD_SECRET" --ignore-not-found >/dev/null 2>&1 || true

# --- verdict --------------------------------------------------------------------------------
if [ -n "$unverified" ]; then
  echo "spike-superset-nova-identity: UNVERIFIED ($unverified)"
  finish
  exit 3
fi
if [ "$failed" -ne 0 ]; then
  echo "spike-superset-nova-identity: rows answered no — see the ✗ lines above"
  finish
  exit 1
fi
finish
echo "spike-superset-nova-identity: PASS"
