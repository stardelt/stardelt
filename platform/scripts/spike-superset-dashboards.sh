#!/usr/bin/env bash
# Superset's dashboard export/import API, measured end to end (stardelt/stardelt#1200, #115).
#
#   make spike-superset-dashboards        (NS=… PORT=… required)
#
# Why: #115 slice a writes Nova code against Superset's export/import shape. The export half was
# measured on the lab (one timestamped top directory; `datasets/*/*.yaml` carries `database_uuid`,
# not a name; `databases/` holds the connection URI). The import half was not: whether an archive
# without `databases/` carries its datasets, what a `database_uuid` absent from the target does,
# what a repeated import does, and which field decides identity.
#
# This measures the five questions of #1200 in one run. Every row reports what the instance *has*
# afterwards, not only the HTTP status: a `200` that leaves the dashboard without charts is the
# failure mode that matters to slice a.
#
#   preflight            kubectl, curl, python3, unzip, jq, uuidgen
#   forward              kubectl port-forward svc/superset (killed by the trap)
#   login                the `superset-admin` Secret, a cookie jar, the access token in a file
#   csrf                 the CSRF token for a POST (a POST without it, or without the cookie, fails)
#   list                 the starting count and titles
#   export               GET /api/v1/dashboard/export/ of the first dashboard
#   unpack               the file list, whether `databases/` is there
#   datasets             every dataset's `database_uuid`
#   databases            every target database's name and uuid
#   minimal-import       metadata.yaml + one dashboard, the smallest archive that imports
#   no-databases-import  question 1: the real export without `databases/`
#   missing-database     question 2: one dataset pointing at a uuid the target does not have
#   dataset-uuid         what a dataset file with a new uuid does to an existing row
#   reimport             question 3: the same archive again, and with `overwrite=true`
#   slug                 question 4: same title with a fresh uuid; then an existing slug
#   cleanup              every dashboard titled `spike-` gone, the count back to the start
#   verdict              ✓ every row measured / ✗ a row answered "no" / UNVERIFIED
#
# Exit 0 = done, 1 = a row answered "no", 3 = UNVERIFIED (no tool, no Superset Deployment, no
# `superset-admin` Secret, or a row that could not be measured — nothing about the import was
# measured then). The instance is left as it was found: the trap deletes every dashboard titled
# `spike-` and every chart/dataset the run created (their uuids were not in the instance before),
# kills the port-forward and removes the scratch.
#
# No secret is printed: the admin password goes from the Secret into a 0600 file and from there
# into a JSON body on stdin, the access token into a 0600 header file (`curl -H @file`), the
# session cookie into a 0600 jar. No database URI is printed either — `databases/` is used only
# to map a name to a uuid, and the "unchanged" check compares digests, not values.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-${NAMESPACE:-stardelt}}"
PORT="${PORT:-18088}"
BASE="http://127.0.0.1:$PORT"
DEPLOY="${DEPLOY:-superset}"
SECRET="${SECRET:-superset-admin}"

scratch="${TMPDIR:-$HOME/.cache}"
mkdir -p "$scratch"
tmp="$(mktemp -d "$scratch/spike-superset.XXXXXX")"
ts="$(date -u +%Y%m%dT%H%M%SZ)"

PF_PID=""
forwarded=no
finished=no
failed=0
unverified=""
csrf=""
start_count=""
code=""
body=""
reason=""
start_charts=""
start_datasets=""

row() { printf '%-22s %s\n' "$1" "$2"; }
ok() { echo "✓ $*"; }
no() { echo "✗ $*"; failed=1; }
untested() { echo "UNVERIFIED ($1)"; unverified="$1"; }

# --- bundle builder ------------------------------------------------------------------------
# Rewrites an unpacked export into an import bundle: optionally fresh uuids for the datasets
# (`--fresh-datasets`), the charts and the dashboard (`--fresh-charts`), every dataset's
# `database_uuid` mapped by database name to the target's own uuid, optionally without
# `databases/`. Datasets keep their export uuid by default: an import that keeps the uuid gets the
# existing row back, one with a fresh uuid rewrites the row of the same table (measured, the
# `dataset-uuid` row). It never writes a database URI into the bundle: `--add-database` copies a
# database's own file, which the caller fetched from the target.
cat > "$tmp/bundle.py" <<'PY'
import json, os, re, sys, uuid, zipfile


def parse(argv):
    o = {"src": None, "out": None, "top": None, "db_map": None, "adds": [],
         "drop": False, "fresh": False, "fresh_datasets": False, "title": None,
         "slug": None, "dash_uuid": None, "break_table": None, "summary": None}
    keys = {"--src": "src", "--out": "out", "--top": "top", "--db-map": "db_map",
            "--title": "title", "--slug": "slug", "--dashboard-uuid": "dash_uuid",
            "--break-dataset": "break_table", "--summary": "summary"}
    i = 1
    while i < len(argv):
        a = argv[i]
        if a in keys:
            o[keys[a]] = argv[i + 1]
            i += 2
        elif a == "--add-database":
            o["adds"].append(argv[i + 1])
            i += 2
        elif a == "--drop-databases":
            o["drop"] = True
            i += 1
        elif a == "--fresh-charts":
            o["fresh"] = True
            i += 1
        elif a == "--fresh-datasets":
            o["fresh_datasets"] = True
            i += 1
        else:
            raise SystemExit("bundle.py: unknown argument %s" % a)
    return o


def top_value(text, key):
    m = re.search(r"^" + key + r": *(\S+) *$", text, re.M)
    return m.group(1) if m else None


def set_top(text, key, value):
    return re.sub(r"^" + key + r":.*$", key + ": " + value, text, count=1, flags=re.M)


def stem(rel):
    return os.path.basename(rel).rsplit(".", 1)[0]


def main(argv):
    o = parse(argv)
    dbmap = {}
    if o["db_map"] and os.path.exists(o["db_map"]):
        for line in open(o["db_map"]):
            parts = line.rstrip("\n").split("\t")
            if len(parts) == 2 and parts[1]:
                dbmap[parts[0]] = parts[1]

    files = []
    for root, _, names in os.walk(o["src"]):
        for name in names:
            files.append(os.path.relpath(os.path.join(root, name), o["src"]).replace(os.sep, "/"))
    files.sort()
    texts = {}
    for rel in files:
        with open(os.path.join(o["src"], rel), encoding="utf-8") as fh:
            texts[rel] = fh.read()

    ds_map, charts, datasets = {}, [], []
    for rel in [f for f in files if f.startswith("datasets/")]:
        text = texts[rel]
        old = top_value(text, "uuid")
        new = str(uuid.uuid4()) if o["fresh_datasets"] else old
        ds_map[old] = new
        db_old = top_value(text, "database_uuid")
        if o["break_table"] and stem(rel) == o["break_table"]:
            db_new = str(uuid.uuid4())
        else:
            db_new = dbmap.get(db_old, db_old)
        if db_new != db_old:
            text = set_top(text, "database_uuid", db_new)
        if new != old:
            text = set_top(text, "uuid", new)
        texts[rel] = text
        datasets.append({"file": rel, "table": stem(rel), "uuid": new, "database_uuid": db_new})

    ch_map = {}
    for rel in [f for f in files if f.startswith("charts/")]:
        text = texts[rel]
        old = top_value(text, "uuid")
        new = str(uuid.uuid4()) if o["fresh"] else old
        ch_map[old] = new
        ds_uuid = top_value(text, "dataset_uuid")
        if ds_uuid in ds_map and ds_map[ds_uuid] != ds_uuid:
            text = set_top(text, "dataset_uuid", ds_map[ds_uuid])
        if new != old:
            text = set_top(text, "uuid", new)
        texts[rel] = text
        charts.append({"file": rel, "uuid": new})

    dashboard = {}
    for rel in [f for f in files if f.startswith("dashboards/")]:
        text = texts[rel]
        old = top_value(text, "uuid")
        if o["dash_uuid"]:
            new = o["dash_uuid"]
        elif o["fresh"]:
            new = str(uuid.uuid4())
        else:
            new = old
        # the position carries each chart's uuid; a fresh chart uuid must move there too
        for before, after in ch_map.items():
            if before != after:
                text = text.replace(before, after)
        if o["title"]:
            text = set_top(text, "dashboard_title", o["title"])
        if o["slug"]:
            text = set_top(text, "slug", o["slug"])
        if new != old:
            text = set_top(text, "uuid", new)
        texts[rel] = text
        dashboard = {"file": rel, "uuid": new,
                     "title": o["title"] or top_value(text, "dashboard_title"),
                     "slug": o["slug"] or top_value(text, "slug")}

    with zipfile.ZipFile(o["out"], "w", zipfile.ZIP_DEFLATED) as bundle:
        for rel, text in texts.items():
            if o["drop"] and rel.startswith("databases/"):
                continue
            bundle.writestr(o["top"] + "/" + rel, text)
        for spec in o["adds"]:
            name, path = spec.split("=", 1)
            with open(path, encoding="utf-8") as fh:
                bundle.writestr(o["top"] + "/databases/" + name + ".yaml", fh.read())

    if o["summary"]:
        with open(o["summary"], "w", encoding="utf-8") as fh:
            json.dump({"datasets": datasets, "charts": charts, "dashboard": dashboard}, fh)


main(sys.argv)
PY

# --- scratch state -------------------------------------------------------------------------
: > "$tmp/before-uuids"
: > "$tmp/created-uuids"

# ids_of <uuid list file> <live list json> -> the live ids, comma separated
ids_of() {
  python3 -c '
import json, sys
want = set(open(sys.argv[1]).read().split())
rows = json.load(open(sys.argv[2]))["result"]
print(",".join(str(r["id"]) for r in rows if r["uuid"] in want))' "$1" "$2"
}

# shellcheck disable=SC2329  # called from the trap and from the cleanup row
cleanup() {
  set +e
  local ids
  grep '^chart ' "$tmp/created-uuids" 2>/dev/null | cut -d' ' -f2 | sort -u > "$tmp/created-charts"
  grep '^dataset ' "$tmp/created-uuids" 2>/dev/null | cut -d' ' -f2 | sort -u > "$tmp/created-datasets"
  # dashboards first: their charts and datasets are only deletable once nothing references them
  grep '^dashboard ' "$tmp/created-uuids" 2>/dev/null | cut -d' ' -f2 | sort -u > "$tmp/created-dashboards"
  get '/api/v1/dashboard/?q=(page_size:200)' "$tmp/cleanup-dash.json" >/dev/null 2>&1
  ids="$( { jq -r '.result[]?|select(.dashboard_title|startswith("spike"))|.id' \
             "$tmp/cleanup-dash.json" 2>/dev/null
           ids_of "$tmp/created-dashboards" "$tmp/cleanup-dash.json" 2>/dev/null | tr ',' '\n'
         } | sed '/^$/d' | sort -u | paste -sd, - )"
  [ -n "$ids" ] && bulk_delete dashboard "$ids" >/dev/null 2>&1
  get '/api/v1/chart/?q=(page_size:200)' "$tmp/cleanup-chart.json" >/dev/null 2>&1
  ids="$(ids_of "$tmp/created-charts" "$tmp/cleanup-chart.json" 2>/dev/null)"
  [ -n "$ids" ] && bulk_delete chart "$ids" >/dev/null 2>&1
  get '/api/v1/dataset/?q=(page_size:200)' "$tmp/cleanup-dataset.json" >/dev/null 2>&1
  ids="$(ids_of "$tmp/created-datasets" "$tmp/cleanup-dataset.json" 2>/dev/null)"
  [ -n "$ids" ] && bulk_delete dataset "$ids" >/dev/null 2>&1
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

# --- HTTP ----------------------------------------------------------------------------------
get() { # <path with query> <out file> -> status code
  local code
  code="$(curl -gsS --max-time 30 -b "$tmp/cookies" -c "$tmp/cookies" \
      -H @"$tmp/auth" "$BASE$1" -o "$2" -w '%{http_code}' 2>/dev/null)" || code=000
  echo "${code:-000}"
}

bulk_delete() { # <api type> <comma separated ids> -> status code
  # Superset 5.0.0 exposes only the bulk route (`DELETE /api/v1/<type>/?q=[…]`); the single
  # `DELETE /api/v1/<type>/<id>/` answers 404 (measured 2026-11-22).
  local code
  [ -n "$2" ] || { echo 000; return; }
  code="$(curl -gsS --max-time 30 -b "$tmp/cookies" -c "$tmp/cookies" \
      -H @"$tmp/auth" -H "X-CSRFToken: $csrf" -H "Referer: $BASE/" -X DELETE \
      "$BASE/api/v1/$1/?q=[$2]" -o "$tmp/delete.json" -w '%{http_code}' 2>/dev/null)" || code=000
  echo "${code:-000}"
}

import_zip() { # <zip> <query|-> [form field…] -> status code, body in $tmp/body.json
  local zip="$1" query="$2"
  shift 2
  local url="$BASE/api/v1/dashboard/import/" code
  [ "$query" = - ] || url="$url?$query"
  local args=(-F "formData=@$zip")
  local field
  for field in "$@"; do args+=(-F "$field"); done
  code="$(curl -sS -g --max-time 30 -b "$tmp/cookies" -c "$tmp/cookies" \
      -H @"$tmp/auth" -H "X-CSRFToken: $csrf" -H "Referer: $BASE/" \
      "${args[@]}" "$url" -o "$tmp/body.json" -w '%{http_code}' 2>/dev/null)" || code=000
  echo "${code:-000}"
}

body_line() { head -c 200 "$tmp/body.json" | tr -d '\r\n' | cut -c1-160; }

# Superset answers a validation failure with a 500 page; the reason is a WARNING in its log.
# The count before the POST keeps a stale line of an earlier import out of the answer.
error_lines() {
  kubectl -n "$NS" logs "deploy/$DEPLOY" --since=10m 2>/dev/null \
    | grep -c 'Import Error' || true
}

import_reason() { # <Import Error lines before the POST> -> the new line, empty when none
  sleep 2
  kubectl -n "$NS" logs "deploy/$DEPLOY" --since=10m 2>/dev/null \
    | grep 'Import Error' | tail -n "+$(( $1 + 1 ))" | tail -1 \
    | sed 's/^.*Import Error: //' | cut -c1-300 || true
}

# Sets $code, $body and $reason for one import. Everything goes through here so the log line and
# the response body belong to this POST, not to the one before it.
import_with_reason() { # <zip> <query|-> [form field…]
  local before
  before="$(error_lines)"
  code="$(import_zip "$@")"
  body="$(body_line)"
  reason="$(import_reason "$before")"
}

count_present() { # <summary.json> <charts|datasets> <live.json> -> how many uuids the live list has
  python3 -c '
import json, sys
summary, kind, live = sys.argv[1:4]
want = [x["uuid"] for x in json.load(open(summary))[kind]]
have = {r["uuid"] for r in json.load(open(live))["result"]}
print(sum(1 for u in want if u in have))' "$1" "$2" "$3"
}

record_created() { # <summary.json>: remember the uuids the instance did not have before
  python3 -c '
import json, sys
summary, before, out = sys.argv[1:4]
known = set(open(before).read().splitlines())
data = json.load(open(summary))
rows = ["chart " + x["uuid"] for x in data["charts"]]
rows += ["dataset " + x["uuid"] for x in data["datasets"]]
rows.append("dashboard " + data["dashboard"]["uuid"])
with open(out, "a") as fh:
    for row in rows:
        if row not in known:
            fh.write(row + "\n")' "$1" "$tmp/before-uuids" "$tmp/created-uuids"
}

live_charts() { get '/api/v1/chart/?q=(page_size:200)' "$tmp/charts.json" >/dev/null; }
live_datasets() { get '/api/v1/dataset/?q=(page_size:200)' "$tmp/datasets.json" >/dev/null; }
live_dashboards() { get '/api/v1/dashboard/?q=(page_size:200)' "$tmp/dashboards.json" >/dev/null; }

dash_uuid_of() { # <slug|title> -> the dashboard's uuid, empty when absent
  live_dashboards
  jq -r --arg s "$1" '.result[]?|select(.slug==$s)|.uuid' "$tmp/dashboards.json" | head -1
}
dash_id_of() { # <slug> -> the dashboard's id, empty when absent
  live_dashboards
  jq -r --arg s "$1" '.result[]?|select(.slug==$s)|.id' "$tmp/dashboards.json" | head -1
}
dash_charts() { # <id> -> how many charts are linked to the dashboard
  get "/api/v1/dashboard/$1/charts" "$tmp/dash-charts.json" >/dev/null
  jq -r '(.count // (.result|length)) // 0' "$tmp/dash-charts.json" 2>/dev/null || echo 0
}

# --- preflight -----------------------------------------------------------------------------
missing=""
for tool in kubectl curl python3 unzip jq uuidgen; do
  command -v "$tool" >/dev/null 2>&1 || missing="$missing $tool"
done
if [ -n "$missing" ]; then
  row preflight "missing:$missing"
  untested "missing:$missing"
  echo "spike-superset-dashboards: UNVERIFIED"
  finish
  exit 3
fi
row preflight "kubectl curl python3 unzip jq uuidgen"

if ! kubectl -n "$NS" get "deploy/$DEPLOY" >/dev/null 2>&1; then
  row forward "no Deployment $NS/$DEPLOY"
  untested "the Superset Deployment is not installed"
  echo "spike-superset-dashboards: UNVERIFIED"
  finish
  exit 3
fi
if ! kubectl -n "$NS" get secret "$SECRET" >/dev/null 2>&1; then
  row login "no Secret $NS/$SECRET"
  untested "the $SECRET Secret is not installed"
  echo "spike-superset-dashboards: UNVERIFIED"
  finish
  exit 3
fi

# --- forward -------------------------------------------------------------------------------
answers() { # true when a port-forward (anybody's) answers on $PORT
  local code
  code="$(curl -s -o /dev/null --max-time 2 -w '%{http_code}' "$BASE/health" 2>/dev/null)" || code=000
  [ "${code:-000}" != "000" ]
}
if answers; then
  row forward "already listening on $PORT (reused, not killed by the trap)"
else
  kubectl -n "$NS" port-forward "svc/$DEPLOY" "$PORT:8088" >"$tmp/pf.log" 2>&1 &
  PF_PID=$!
  forwarded=yes
  for _ in $(seq 1 30); do
    answers && break
    sleep 1
  done
  if ! answers; then
    row forward "no answer on $PORT within 30s"
    untested "superset did not answer through the port-forward"
    echo "spike-superset-dashboards: UNVERIFIED"
    finish
    exit 3
  fi
  row forward "svc/$DEPLOY $PORT:8088"
fi

# --- login ---------------------------------------------------------------------------------
kubectl -n "$NS" get secret "$SECRET" -o jsonpath='{.data.username}' | base64 -d > "$tmp/user"
kubectl -n "$NS" get secret "$SECRET" -o jsonpath='{.data.password}' | base64 -d > "$tmp/pass"
chmod 600 "$tmp/user" "$tmp/pass"
python3 -c '
import json, sys
print(json.dumps({"username": open(sys.argv[1]).read().strip(),
                  "password": open(sys.argv[2]).read().strip(),
                  "provider": "db", "refresh": True}))' "$tmp/user" "$tmp/pass" > "$tmp/login.json"
chmod 600 "$tmp/login.json"
code="$(curl -sS --max-time 30 -c "$tmp/cookies" -H 'Content-Type: application/json' \
    -X POST --data @"$tmp/login.json" -o "$tmp/login.out" -w '%{http_code}' \
    "$BASE/api/v1/security/login" 2>/dev/null)" || code=000
rm -f "$tmp/user" "$tmp/pass" "$tmp/login.json"
[ "$code" = 200 ] || {
  row login "$code $(body_line)"
  untested "the admin login answered $code"
  echo "spike-superset-dashboards: UNVERIFIED"
  finish
  exit 3
}
printf 'Authorization: Bearer %s\n' "$(jq -r '.access_token // empty' "$tmp/login.out")" > "$tmp/auth"
chmod 600 "$tmp/auth"
chmod 600 "$tmp/cookies"
[ -s "$tmp/auth" ] || {
  row login "the login response carried no access_token"
  untested "no access_token"
  echo "spike-superset-dashboards: UNVERIFIED"
  finish
  exit 3
}
row login "200 (access_token in a 0600 file, cookie jar in a 0600 file)"

# --- csrf ----------------------------------------------------------------------------------
code="$(get /api/v1/security/csrf_token/ "$tmp/csrf.json")"
csrf="$(jq -r '.result // empty' "$tmp/csrf.json" 2>/dev/null || true)"
if [ "$code" != 200 ] || [ -z "$csrf" ]; then
  row csrf "$code (no CSRF token)"
  untested "no CSRF token (the POSTs of this run cannot work)"
  echo "spike-superset-dashboards: UNVERIFIED"
  finish
  exit 3
fi
if grep -q 'session' "$tmp/cookies"; then cookie=yes; else cookie=no; fi
row csrf "200 (session cookie in the jar: $cookie)"

# --- list ----------------------------------------------------------------------------------
code="$(get '/api/v1/dashboard/?q=(page_size:100)' "$tmp/dashboards.json")"
[ "$code" = 200 ] || {
  row list "GET /api/v1/dashboard/ answered $code"
  untested "the dashboard list answered $code"
  echo "spike-superset-dashboards: UNVERIFIED"
  finish
  exit 3
}
start_count="$(jq -r '.count' "$tmp/dashboards.json")"
# The script's own leftovers (`spike-`) are not part of the instance's starting state: a second
# run must start from the same number as the first, whatever a killed run left behind.
start_own="$(jq -r '[.result[]?|select(.dashboard_title|startswith("spike-"))]|length' "$tmp/dashboards.json")"
start_real=$((start_count - start_own))
row list "count=$start_count (of which spike-: $start_own)"
echo "list count=$start_count"
jq -r '.result[]|"  id=\(.id) \(.dashboard_title)"' "$tmp/dashboards.json"

first_id="$(jq -r '.result[]?|select(.dashboard_title|startswith("spike-")|not)|.id' "$tmp/dashboards.json" | head -1)"
first_title="$(jq -r --arg id "$first_id" '.result[]?|select((.id|tostring)==$id)|.dashboard_title' "$tmp/dashboards.json" | head -1)"
[ -n "$first_id" ] || {
  row export "no dashboard to export"
  untested "the instance has no dashboard to export"
  echo "spike-superset-dashboards: UNVERIFIED"
  finish
  exit 3
}

# The snapshot of what exists before any import; cleanup deletes only what is new.
live_charts
live_datasets
start_charts="$(jq -r '.count' "$tmp/charts.json")"
start_datasets="$(jq -r '.count' "$tmp/datasets.json")"
{
  jq -r '.result[]?|"chart " + .uuid' "$tmp/charts.json"
  jq -r '.result[]?|"dataset " + .uuid' "$tmp/datasets.json"
} > "$tmp/before-uuids"

# --- export --------------------------------------------------------------------------------
code="$(curl -gsS --max-time 30 -b "$tmp/cookies" -c "$tmp/cookies" -H @"$tmp/auth" \
    "$BASE/api/v1/dashboard/export/?q=[$first_id]" -o "$tmp/export.zip" -w '%{http_code}' \
    2>/dev/null)" || code=000
size="$(wc -c < "$tmp/export.zip" 2>/dev/null || echo 0)"
[ "$code" = 200 ] || {
  row export "id=$first_id $code"
  untested "the export answered $code"
  echo "spike-superset-dashboards: UNVERIFIED"
  finish
  exit 3
}
row export "id=$first_id ($first_title) $code $size bytes"

# --- unpack --------------------------------------------------------------------------------
mkdir -p "$tmp/ex" "$tmp/src"
unzip -q "$tmp/export.zip" -d "$tmp/ex"
top_dir="$(find "$tmp/ex" -mindepth 1 -maxdepth 1 -type d | head -1)"
if [ -z "$top_dir" ]; then
  row unpack "the export has no top directory"
  untested "the export has no top directory"
  echo "spike-superset-dashboards: UNVERIFIED"
  finish
  exit 3
fi
cp -r "$top_dir/." "$tmp/src/"
file_count="$(find "$tmp/src" -type f | wc -l | tr -d ' ')"
if [ -d "$tmp/src/databases" ]; then has_dbs=yes; else has_dbs=no; fi
row unpack "$file_count files under $(basename "$top_dir")/ ; databases/ present: $has_dbs"
find "$tmp/src" -type f | sed "s|$tmp/src/|  |" | sort

# --- datasets ------------------------------------------------------------------------------
echo "datasets:"
for file in "$tmp/src"/datasets/*/*.yaml; do
  [ -f "$file" ] || continue
  db_uuid="$(sed -n 's/^database_uuid: *//p' "$file" | head -1)"
  echo "  $(basename "$file" .yaml) -> $db_uuid"
done
dataset_count=0
if [ -d "$tmp/src/datasets" ]; then
  dataset_count="$(find "$tmp/src/datasets" -name '*.yaml' | wc -l | tr -d ' ')"
fi
row datasets "$dataset_count dataset(s)"

# --- databases -----------------------------------------------------------------------------
code="$(get '/api/v1/database/?q=(page_size:200)' "$tmp/dbs.json")"
[ "$code" = 200 ] || {
  row databases "GET /api/v1/database/ answered $code"
  untested "the database list answered $code"
  echo "spike-superset-dashboards: UNVERIFIED"
  finish
  exit 3
}
db_count="$(jq -r '.count' "$tmp/dbs.json")"
echo "databases ($db_count):"
jq -r '.result[]|"  \(.database_name) \(.uuid)"' "$tmp/dbs.json" | sort
row databases "count=$db_count"

# name -> export uuid (from the bundle's own databases/ files) -> target uuid (by same name)
echo "mapping (database name, export uuid, target uuid):" > "$tmp/dbmap-note"
: > "$tmp/dbmap.tsv"
for file in "$tmp/src"/databases/*.yaml; do
  [ -f "$file" ] || continue
  name="$(sed -n 's/^database_name: *//p' "$file" | head -1 | sed -E "s/^'(.*)'$/\1/")"
  export_uuid="$(sed -n 's/^uuid: *//p' "$file" | head -1)"
  target_uuid="$(jq -r --arg n "$name" '.result[]?|select(.database_name==$n)|.uuid' \
                   "$tmp/dbs.json" | head -1)"
  printf '%s\t%s\n' "$export_uuid" "${target_uuid:-}" >> "$tmp/dbmap.tsv"
  printf '  %s %s -> %s\n' "$name" "$export_uuid" "${target_uuid:-MISSING}" >> "$tmp/dbmap-note"
done
sed -n '2,$p' "$tmp/dbmap-note" | head -20
# the exported datasets' databases must each exist in the target by name
missing_names=0
while read -r line; do
  case "$line" in
    *"-> MISSING"*) missing_names=$((missing_names + 1)) ;;
  esac
done < "$tmp/dbmap-note"
echo "  databases of the export absent from the target by name: $missing_names"

# --- minimal-import ------------------------------------------------------------------------
min_title="spike-minimal-$ts"
python3 - "$tmp/minimal.zip" "spike_minimal_$ts" "$min_title" "$(uuidgen | tr '[:upper:]' '[:lower:]')" <<'PY'
import datetime, sys, zipfile
out, top, title, uid = sys.argv[1:5]
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
meta = "version: 1.0.0\ntype: Dashboard\ntimestamp: '%s'\n" % now
dash = """dashboard_title: {t}
slug: {t}
published: false
uuid: {u}
position:
  DASHBOARD_VERSION_KEY: v2
  ROOT_ID:
    children:
    - GRID_ID
    id: ROOT_ID
    type: ROOT
  GRID_ID:
    children: []
    id: GRID_ID
    parents:
    - ROOT_ID
    type: GRID
  HEADER_ID:
    id: HEADER_ID
    meta:
      text: {t}
    type: HEADER
version: 1.0.0
""".format(t=title, u=uid)
with zipfile.ZipFile(out, "w") as bundle:
    bundle.writestr(top + "/metadata.yaml", meta)
    bundle.writestr(top + "/dashboards/" + title + ".yaml", dash)
PY
import_with_reason "$tmp/minimal.zip" -
if [ "$code" = 200 ]; then
  row minimal-import "$code $body"
  ok "minimal import 200 {\"message\":\"OK\"}"
else
  row minimal-import "$code $body"
  no "minimal import answered $code ${reason:-no reason in the log}"
fi

# --- no-databases-import (question 1) ------------------------------------------------------
nodb_title="spike-nodb-$ts"
python3 "$tmp/bundle.py" --src "$tmp/src" --out "$tmp/nodb.zip" --top "spike_nodb_$ts" \
  --drop-databases --fresh-charts --title "spike-nodb-$ts" --slug "spike-nodb-$ts" \
  --db-map "$tmp/dbmap.tsv" --summary "$tmp/nodb.json"
record_created "$tmp/nodb.json"
import_with_reason "$tmp/nodb.zip" -
want_charts="$(jq -r '.charts|length' "$tmp/nodb.json")"
if [ "$code" = 200 ]; then
  live_charts
  charts_in="$(count_present "$tmp/nodb.json" charts "$tmp/charts.json")"
  nodb_id="$(dash_id_of "$nodb_title")"
  if [ -n "$nodb_id" ]; then kept="id=$nodb_id charts-linked=$(dash_charts "$nodb_id")/$want_charts"; else kept="NO dashboard"; fi
  row no-databases-import "$code; dashboard: $kept; new charts $charts_in/$want_charts"
  if [ -z "$nodb_id" ] || [ "$charts_in" != "$want_charts" ]; then
    ok "archive without databases/ answers 200 but drops its datasets and charts (dashboard imported)"
  else
    ok "archive without databases/ carries all its datasets and charts"
  fi
  echo "  import reason: ${reason:-none}"
else
  row no-databases-import "$code $body"
  ok "archive without databases/ is refused: $code ${reason:-no reason in the log}"
fi

# The way slice a could still import without storing a URI: the target's own database file, read
# from the target at deploy time. Its uuid is the target's, so the importer returns the existing
# row and the URI is not rewritten (`overwrite=false`).
db_file_name=""
db_file_path=""
if [ -n "$(jq -r '.result[0].database_name // empty' "$tmp/dbs.json")" ]; then
  ref_export_uuid="$(sed -n 's/^database_uuid: *//p' "$tmp/src"/datasets/*/*.yaml | head -1)"
  ref_id="$(jq -r --arg u "$ref_export_uuid" '.result[]?|select(.uuid==$u)|.id' "$tmp/dbs.json" | head -1)"
  if [ -n "$ref_id" ]; then
    uri_before="$(jq -r --arg id "$ref_id" '.result[]?|select(.id==($id|tonumber))|.sqlalchemy_uri // empty' "$tmp/dbs.json" | sha256sum | cut -d' ' -f1)"
    code="$(curl -gsS --max-time 30 -b "$tmp/cookies" -c "$tmp/cookies" -H @"$tmp/auth" \
        "$BASE/api/v1/database/export/?q=[$ref_id]" -o "$tmp/db.zip" -w '%{http_code}' 2>/dev/null)" || code=000
    if [ "$code" = 200 ]; then
      mkdir -p "$tmp/dbex"
      unzip -q "$tmp/db.zip" -d "$tmp/dbex"
      db_file_path="$(find "$tmp/dbex" -path '*/databases/*.yaml' | head -1)"
      db_file_name="$(basename "$db_file_path" .yaml)"
      cp "$db_file_path" "$tmp/target-db.yaml"
      python3 "$tmp/bundle.py" --src "$tmp/src" --out "$tmp/synth.zip" --top "spike_synth_$ts" \
        --drop-databases --fresh-charts --title "spike-synth-$ts" --slug "spike-synth-$ts" \
        --db-map "$tmp/dbmap.tsv" --add-database "$db_file_name=$tmp/target-db.yaml" \
        --summary "$tmp/synth.json"
      record_created "$tmp/synth.json"
      import_with_reason "$tmp/synth.zip" -
      if [ "$code" = 200 ]; then
        live_charts
        charts_in="$(count_present "$tmp/synth.json" charts "$tmp/charts.json")"
        want_charts="$(jq -r '.charts|length' "$tmp/synth.json")"
        synth_id="$(dash_id_of "spike-synth-$ts")"
        linked="none"
        [ -n "$synth_id" ] && linked="$(dash_charts "$synth_id")"
        row "no-databases+synth" "$code; dashboard: id=${synth_id:-NONE} charts-linked=$linked/$want_charts; new charts $charts_in/$want_charts"
        get '/api/v1/database/?q=(page_size:200)' "$tmp/dbs2.json" >/dev/null
        uri_after="$(jq -r --arg id "$ref_id" '.result[]?|select(.id==($id|tonumber))|.sqlalchemy_uri // empty' "$tmp/dbs2.json" | sha256sum | cut -d' ' -f1)"
        if [ "$uri_before" = "$uri_after" ]; then
          ok "a synthesized databases/ file leaves the target database unchanged (URI digest equal)"
        else
          no "the target database's URI changed during the synthesized import"
        fi
      else
        row "no-databases+synth" "$code $body"
        no "synthesized databases/ import answered $code ${reason:-no reason in the log}"
      fi
    else
      row "no-databases+synth" "database export answered $code"
      no "could not read the target database's own export"
    fi
  fi
fi

# --- missing-database (question 2) ---------------------------------------------------------
# The other datasets must be mappable, so this bundle carries the target's own database file and
# breaks exactly one dataset: the difference to the row above is that dataset alone.
broken_table="$(jq -r '.datasets[0].table' "$tmp/nodb.json")"
missing_args=()
if [ -n "$db_file_name" ] && [ -f "$tmp/target-db.yaml" ]; then
  missing_args=(--add-database "$db_file_name=$tmp/target-db.yaml")
fi
python3 "$tmp/bundle.py" --src "$tmp/src" --out "$tmp/missing.zip" --top "spike_missing_$ts" \
  --drop-databases --fresh-charts --title "spike-missing-$ts" --slug "spike-missing-$ts" \
  --db-map "$tmp/dbmap.tsv" "${missing_args[@]}" \
  --break-dataset "$broken_table" --summary "$tmp/missing.json"
record_created "$tmp/missing.json"
import_with_reason "$tmp/missing.zip" -
live_charts
charts_in="$(count_present "$tmp/missing.json" charts "$tmp/charts.json")"
want_charts="$(jq -r '.charts|length' "$tmp/missing.json")"
missing_id="$(dash_id_of "spike-missing-$ts")"
linked="none"
[ -n "$missing_id" ] && linked="$(dash_charts "$missing_id")"
row missing-database "$code; dashboard: id=${missing_id:-NONE} charts-linked=$linked/$want_charts; new charts $charts_in/$want_charts; reason: ${reason:-none}"
if [ -n "$reason" ]; then
  ok "a dataset with an unknown database_uuid fails the import: $reason"
else
  ok "a dataset with an unknown database_uuid is dropped silently ($linked/$want_charts charts, $code, no message)"
fi

# --- dataset-uuid: what an import does to a dataset row the target already has ---------------
# Slice a ships `datasets/` in the bundle, so this decides whether a promotion may rewrite the
# target's dataset rows. Measured against the target's own table: one import with a fresh uuid,
# then one with the file's own uuid to put the row back exactly as it was.
if [ -d "$tmp/src/datasets" ]; then
  probe_file="$(find "$tmp/src/datasets" -name '*.yaml' | head -1)"
  probe_table="$(basename "$probe_file" .yaml)"
  probe_db="$(find "$tmp/src/databases" -name '*.yaml' | head -1)"
  probe_fresh="$(uuidgen | tr '[:upper:]' '[:lower:]')"
  build_ds() { # <dataset yaml> <database yaml> <uuid|-> <out zip>
    python3 -c '
import datetime, re, sys, zipfile
dataset, database, uid, out = sys.argv[1:5]
text = open(dataset).read()
if uid != "-":
    text = re.sub(r"^uuid: .*$", "uuid: " + uid, text, count=1, flags=re.M)
with zipfile.ZipFile(out, "w") as bundle:
    bundle.writestr("spike_ds/metadata.yaml",
                    "version: 1.0.0\ntype: SqlaTable\ntimestamp: \x27%s\x27\n"
                    % datetime.datetime.now(datetime.timezone.utc).isoformat())
    bundle.writestr("spike_ds/databases/%s" % database.split("/")[-1], open(database).read())
    bundle.writestr("spike_ds/datasets/%s/%s" % (database.split("/")[-2], dataset.split("/")[-1]), text)
' "$1" "$2" "$3" "$4"
  }
  build_ds "$probe_file" "$probe_db" "$probe_fresh" "$tmp/ds-fresh.zip"
  build_ds "$probe_file" "$probe_db" - "$tmp/ds-back.zip"
  live_datasets
  before_id="$(jq -r --arg t "$probe_table" '.result[]?|select(.table_name==$t)|.id' "$tmp/datasets.json" | head -1)"
  before_uuid="$(jq -r --arg t "$probe_table" '.result[]?|select(.table_name==$t)|.uuid' "$tmp/datasets.json" | head -1)"
  before_total="$(jq -r '.count' "$tmp/datasets.json")"
  before_lines="$(error_lines)"
  code="$(curl -sS -g --max-time 60 -b "$tmp/cookies" -c "$tmp/cookies" -H @"$tmp/auth" \
      -H "X-CSRFToken: $csrf" -H "Referer: $BASE/" -F "formData=@$tmp/ds-fresh.zip" \
      -o "$tmp/body.json" -w '%{http_code}' "$BASE/api/v1/dataset/import/" 2>/dev/null)" || code=000
  body="$(body_line)"
  reason="$(import_reason "$before_lines")"
  live_datasets
  after_id="$(jq -r --arg t "$probe_table" '.result[]?|select(.table_name==$t)|.id' "$tmp/datasets.json" | head -1)"
  after_uuid="$(jq -r --arg t "$probe_table" '.result[]?|select(.table_name==$t)|.uuid' "$tmp/datasets.json" | head -1)"
  after_total="$(jq -r '.count' "$tmp/datasets.json")"
  row dataset-uuid "$code; $probe_table id $before_id->$after_id, uuid $before_uuid->$after_uuid, rows $before_total->$after_total"
  if [ "$after_total" = "$before_total" ] && [ "$after_id" = "$before_id" ] && [ "$after_uuid" = "$probe_fresh" ]; then
    ok "a dataset file with a new uuid rewrites the target's row in place (same id, no second row)"
  else
    ok "a dataset file with a new uuid did not rewrite the row in place (see the row)"
  fi
  # put the uuid back: the same file with its own uuid is the row the target had
  code="$(curl -sS -g --max-time 60 -b "$tmp/cookies" -c "$tmp/cookies" -H @"$tmp/auth" \
      -H "X-CSRFToken: $csrf" -H "Referer: $BASE/" -F "formData=@$tmp/ds-back.zip" \
      -o "$tmp/body.json" -w '%{http_code}' "$BASE/api/v1/dataset/import/" 2>/dev/null)" || code=000
  live_datasets
  restored="$(jq -r --arg t "$probe_table" '.result[]?|select(.table_name==$t)|.uuid' "$tmp/datasets.json" | head -1)"
  if [ "$code" = 200 ] && [ "$restored" = "$before_uuid" ]; then
    ok "the dataset row is back to $before_uuid"
  else
    no "the dataset row was not restored ($restored, import answered $code)"
  fi
else
  row dataset-uuid "UNVERIFIED (the export carried no datasets/)"
  unverified="the dataset-uuid row"
fi

# --- reimport (question 3) -----------------------------------------------------------------
reference_zip="$tmp/synth.zip"
reference_title="spike-synth-$ts"
if [ ! -f "$reference_zip" ] || [ -z "$(dash_id_of "$reference_title")" ]; then
  reference_zip="$tmp/nodb.zip"
  reference_title="$nodb_title"
fi
id_before="$(dash_id_of "$reference_title")"
import_with_reason "$reference_zip" -
code_plain="$code"
reason_plain="$reason"
import_with_reason "$reference_zip" 'overwrite=true'
code_query="$code"
reason_query="$reason"
import_with_reason "$reference_zip" - 'overwrite=true'
code_form="$code"
body_form="$body"
id_after="$(dash_id_of "$reference_title")"
row reimport "plain: $code_plain; ?overwrite=true: $code_query; form overwrite=true: $code_form $body_form; id before=$id_before after=$id_after"
echo "  plain reason: ${reason_plain:-none}"
echo "  query reason: ${reason_query:-none}"
if [ "$code_form" = 200 ] && [ "$id_before" = "$id_after" ] && [ -n "$id_before" ]; then
  ok "reimport needs the form field overwrite=true and keeps the id ($id_before)"
else
  no "reimport with the form field did not keep the dashboard (before=$id_before after=$id_after)"
fi

# --- slug (question 4) ---------------------------------------------------------------------
slug_args=()
if [ -n "$db_file_name" ] && [ -f "$tmp/target-db.yaml" ]; then
  slug_args=(--add-database "$db_file_name=$tmp/target-db.yaml")
fi
# (a) the dashboard keeps its title and gets the readable slug a per-target plan wants
slug_new="spike-slug-$ts"
python3 "$tmp/bundle.py" --src "$tmp/src" --out "$tmp/sametitle.zip" --top "spike_sametitle_$ts" \
  --drop-databases --fresh-charts --slug "$slug_new" \
  --db-map "$tmp/dbmap.tsv" "${slug_args[@]}" --summary "$tmp/sametitle.json"
record_created "$tmp/sametitle.json"
bundle_uuid="$(jq -r '.dashboard.uuid' "$tmp/sametitle.json")"
import_with_reason "$tmp/sametitle.zip" -
live_dashboards
same_id="$(dash_id_of "$slug_new")"
titles_same="$(jq -r --arg t "$first_title" '[.result[]?|select(.dashboard_title==$t)]|length' \
                 "$tmp/dashboards.json")"
imported_uuid="$(jq -r --arg id "${same_id:-none}" '.result[]?|select((.id|tostring)==$id)|.uuid' \
                   "$tmp/dashboards.json" | head -1)"
row slug "(a) same title, new slug $slug_new: $code id=${same_id:-NONE} uuid=$imported_uuid dashboards-with-the-title=$titles_same"
# identity: the imported dashboard carries the bundle's uuid, and a second import with the same
# uuid is the one that "already exists" — that is the field the importer keys on.
if [ -n "$imported_uuid" ] && [ "$imported_uuid" = "$bundle_uuid" ]; then
  ok "identity is the uuid: the imported dashboard carries the bundle's uuid ($bundle_uuid)"
else
  no "the imported dashboard's uuid ($imported_uuid) is not the bundle's ($bundle_uuid)"
fi

# (b) a fresh uuid on a slug that already exists: the slug is not what the importer keys on
existing_slug="$(jq -r --arg t "$first_title" '.result[]?|select(.dashboard_title==$t)|.slug' \
                   "$tmp/dashboards.json" | head -1)"
if [ -n "$existing_slug" ]; then
  python3 "$tmp/bundle.py" --src "$tmp/src" --out "$tmp/dupslug.zip" --top "spike_dupslug_$ts" \
    --drop-databases --fresh-charts --slug "$existing_slug" \
    --db-map "$tmp/dbmap.tsv" --summary "$tmp/dupslug.json"
  record_created "$tmp/dupslug.json"
  import_with_reason "$tmp/dupslug.zip" -
  dup_uuid="$(jq -r '.dashboard.uuid' "$tmp/dupslug.json")"
  live_dashboards
  dup_id="$(jq -r --arg u "$dup_uuid" '.result[]?|select(.uuid==$u)|.id' "$tmp/dashboards.json" | head -1)"
  dup_slugs="$(jq -r --arg s "$existing_slug" '[.result[]?|select(.slug==$s)]|length' \
                 "$tmp/dashboards.json")"
  row "slug(b)" "(b) fresh uuid, existing slug '$existing_slug': $code id=${dup_id:-NONE} (the first one: ${same_id:-NONE}); dashboards-with-that-slug=$dup_slugs${reason:+; reason: $reason}"
  if [ -n "$dup_id" ] && [ "$dup_id" = "$same_id" ]; then
    ok "the slug is an identity key too: a new uuid on an existing slug overwrites that dashboard (id kept, uuid replaced)"
  else
    ok "a new uuid on an existing slug created a second dashboard (id $dup_id next to $same_id)"
  fi
else
  row "slug(b)" "UNVERIFIED (the first slug import left no dashboard)"
  unverified="second slug case"
fi

# --- cleanup -------------------------------------------------------------------------------
cleanup
live_dashboards
live_charts
live_datasets
now_count="$(jq -r '.count' "$tmp/dashboards.json")"
now_charts="$(jq -r '.count' "$tmp/charts.json")"
now_datasets="$(jq -r '.count' "$tmp/datasets.json")"
left="$(jq -r '[.result[]?|select(.dashboard_title|startswith("spike-"))]|length' "$tmp/dashboards.json")"
now_real=$((now_count - left))
row cleanup "dashboards $now_count (non-spike $now_real, start $start_real), spike- left: $left, charts $now_charts/$start_charts, datasets $now_datasets/$start_datasets"
if [ "$now_real" = "$start_real" ] && [ "$left" = 0 ] \
    && [ "$now_charts" = "$start_charts" ] && [ "$now_datasets" = "$start_datasets" ]; then
  ok "the instance is back to its starting state"
else
  no "the instance is not back to its starting state (non-spike $now_real vs $start_real, spike- left $left, charts $now_charts vs $start_charts, datasets $now_datasets vs $start_datasets)"
fi

# --- verdict -------------------------------------------------------------------------------
if [ -n "$unverified" ]; then
  echo "spike-superset-dashboards: UNVERIFIED ($unverified)"
  finish
  exit 3
fi
if [ "$failed" -ne 0 ]; then
  echo "spike-superset-dashboards: rows answered no — see the ✗ lines above"
  finish
  exit 1
fi
finish
echo "spike-superset-dashboards: done"
