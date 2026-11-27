#!/usr/bin/env bash
# The PEGELONLINE worked example, live: the applied Pipeline, its three quality checks, the view
# (stardelt/stardelt#1995, #647). `make verify-pegelonline`, or `bash scripts/verify-pegelonline.sh`.
#
# Why: the example has no release row, so `make verify-release` passed while the pipeline's
# `checks` task failed on the lab for three consecutive runs (2026-11-27, #1992). This check
# reads the Pipeline the *cluster* carries — not the file in the repository — runs each of its
# checks as the pipeline's own principal, and counts the view:
#   pipeline  the applied Pipeline is named and has its tasks;
#   check …   every quality check returns 0 rows (a row is a violation);
#   view      shared.gold.pegelonline_levels_v1 holds rows.
# The unit guard proves the applied Pipeline is the one #1992 fixed: a green repository file is
# not a green cluster.
#
# Exit 0 = PASS, 1 = FAIL (a check or the view proved something false, or the applied Pipeline
# predates #1992), 3 = UNVERIFIED (the Pipeline, the Secret, the identity endpoint or the engine
# is absent — nothing about the example was measured then).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-${NAMESPACE:-stardelt}}"
PIPELINE="${PIPELINE:-pegelonline-hourly}"
SECRET="${SECRET:-sp-pipeline-stardelt-users}"
TRINO_URL="${TRINO_URL:-https://trino.alpha.dev.stardelt.io}"
VIEW="shared.gold.pegelonline_levels_v1"

for tool in kubectl curl jq python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "? $tool is not installed"; exit 3; }
done

scratch="${TMPDIR:-$HOME/.cache}"
mkdir -p "$scratch"
tmp="$(mktemp -d "$scratch/verify-pegelonline.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

# --- the applied Pipeline -----------------------------------------------------------------
if ! kubectl -n "$NS" get pipeline "$PIPELINE" -o json > "$tmp/pipeline.json" 2>/dev/null; then
  echo "? the PEGELONLINE Pipeline is not applied — run demos/examples/pegelonline/README.md"
  exit 3
fi

python3 - "$tmp/pipeline.json" > "$tmp/pipeline.txt" <<'PY'
import json, sys

document = json.load(open(sys.argv[1]))
spec = document.get("spec") or {}
tasks = spec.get("tasks") or []
print("tasks\t%d" % len(tasks))
print("name\t%s" % (document.get("metadata") or {}).get("name", ""))
for task in tasks:
    if task.get("name") != "checks":
        continue
    for check in task.get("checks") or []:
        print("check\t%s\t%s" % (check.get("name", ""), json.dumps(check.get("sql", ""))))
PY

TASKS="$(awk -F'\t' '$1 == "tasks" { print $2 }' "$tmp/pipeline.txt")"
NAME="$(awk -F'\t' '$1 == "name" { print $2 }' "$tmp/pipeline.txt")"

echo "✓ pipeline: ${NAME:-$PIPELINE}, ${TASKS:-0} tasks"

# The applied `value_in_range` must name the unit #1992 added; the repository file being green
# says nothing about what the cluster runs.
RANGE_SQL="$(awk -F'\t' '$1 == "check" && $2 == "value_in_range" { print $3 }' "$tmp/pipeline.txt")"
if ! python3 -c 'import json,sys; sys.exit(0 if "unit = '\''cm'\''" in json.loads(sys.argv[1]) else 1)' \
     "${RANGE_SQL:-null}" 2>/dev/null; then
  echo "✗ the applied Pipeline predates stardelt/stardelt#1992 — kubectl apply -f demos/examples/pegelonline/pipeline.yaml"
  echo "FAIL (the applied Pipeline is not the fixed one)"
  exit 1
fi

# --- the principal's client credentials ---------------------------------------------------
if ! kubectl -n "$NS" get secret "$SECRET" >/dev/null 2>&1; then
  echo "? Secret $SECRET is not installed — the PEGELONLINE example is not set up on this cluster"
  exit 3
fi
kubectl -n "$NS" get secret "$SECRET" -o jsonpath='{.data.client-id}' | base64 -d > "$tmp/client-id"
kubectl -n "$NS" get secret "$SECRET" -o jsonpath='{.data.client-secret}' | base64 -d > "$tmp/client-secret"
chmod 600 "$tmp/client-id" "$tmp/client-secret"
[ -s "$tmp/client-id" ] && [ -s "$tmp/client-secret" ] \
  || { echo "? Secret $SECRET has no client-id/client-secret"; exit 3; }

ISSUER="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}' 2>/dev/null || true)"
[ -n "$ISSUER" ] \
  || { echo "? ConfigMap stardelt-identity has no issuerUrl — run make install-identity"; exit 3; }

code="$(curl -sS -o "$tmp/token.json" -w '%{http_code}' -X POST \
  "$ISSUER/protocol/openid-connect/token" \
  --data-urlencode grant_type=client_credentials \
  --data-urlencode client_id@"$tmp/client-id" \
  --data-urlencode client_secret@"$tmp/client-secret" 2>/dev/null)" || code="000"
if [ "$code" = "000" ]; then
  echo "? the identity endpoint did not answer — nothing was measured"
  exit 3
fi
[ "$code" = "200" ] \
  || { echo "✗ the realm refused $SECRET (HTTP $code: $(jq -r '.error // "no error field"' "$tmp/token.json" 2>/dev/null | cut -c1-80))"; echo "FAIL (no token as the pipeline principal)"; exit 1; }
jq -r .access_token "$tmp/token.json" > "$tmp/token"
chmod 600 "$tmp/token"
[ -s "$tmp/token" ] && [ "$(cat "$tmp/token")" != "null" ] \
  || { echo "✗ the realm answered without an access_token"; echo "FAIL (no token as the pipeline principal)"; exit 1; }

# --- the Trino statement API --------------------------------------------------------------
# One statement, rows as JSON: `{"code":200,"rows":[[..]],"error":null}`. `nextUri` is followed
# until it is absent; a transport failure is code 0, an engine error carries its message.
cat > "$tmp/trino.py" <<'PY'
import json, sys, urllib.error, urllib.request

url, token_file, sql = sys.argv[1:4]
headers = {
    "Content-Type": "text/plain",
    "Authorization": "Bearer " + open(token_file).read().strip(),
}


def call(endpoint, data=None):
    request = urllib.request.Request(endpoint, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, {"error": {"message": error.read().decode()[:300]}}
    except (urllib.error.URLError, OSError) as error:
        return 0, {"error": {"message": "unreachable: %s" % error}}


code, body = call(url.rstrip("/") + "/v1/statement", sql.encode())
rows = list(body.get("data") or [])
failure = body.get("error")
while body.get("nextUri"):
    code, body = call(body["nextUri"])
    rows.extend(body.get("data") or [])
    failure = failure or body.get("error")
print(json.dumps({"code": code, "rows": rows, "error": failure}))
PY

trino() { # <sql> -> json line
  python3 "$tmp/trino.py" "$TRINO_URL" "$tmp/token" "$1"
}

probe="$(trino 'SELECT 1')"
if [ "$(jq -r '.code' <<<"$probe")" = "0" ]; then
  echo "? the Trino endpoint did not answer ($TRINO_URL)"
  exit 3
fi
if [ "$(jq -r '.error // empty' <<<"$probe")" != "" ] && [ "$(jq -r '.code' <<<"$probe")" != "200" ]; then
  echo "? the Trino endpoint refused the principal's probe: $(jq -c '.error.message // .error' <<<"$probe" | cut -c1-120)"
  exit 3
fi

# --- the live checks -----------------------------------------------------------------------
failures=0
total=1
while IFS=$'\t' read -r kind name sql; do
  [ "$kind" = "check" ] || continue
  total=$((total + 1))
  out="$(trino "$(python3 -c 'import json,sys; print(json.loads(sys.argv[1]))' "$sql")")"
  if [ "$(jq -r '.code' <<<"$out")" != "200" ] || [ "$(jq -r '.error // empty' <<<"$out")" != "" ]; then
    echo "✗ check $name: the statement failed: $(jq -c '.error.message // .error' <<<"$out" | cut -c1-160)"
    failures=$((failures + 1))
    continue
  fi
  rows="$(jq -r '.rows | length' <<<"$out")"
  if [ "$rows" = "0" ]; then
    case "$name" in
      value_in_range) echo "✓ check $name: 0 rows (unit-aware)" ;;
      *) echo "✓ check $name: 0 rows" ;;
    esac
  else
    echo "✗ check $name: $rows row(s) — first: $(jq -c '.rows[0]' <<<"$out" | cut -c1-200)"
    failures=$((failures + 1))
  fi
done < "$tmp/pipeline.txt"

# --- the view ------------------------------------------------------------------------------
total=$((total + 1))
out="$(trino "SELECT count(*) FROM $VIEW")"
if [ "$(jq -r '.code' <<<"$out")" = "200" ] && [ "$(jq -r '.error // empty' <<<"$out")" = "" ]; then
  views="$(jq -r '.rows[0][0]' <<<"$out")"
  if [ "$views" -gt 0 ] 2>/dev/null; then
    echo "✓ view: $VIEW has $views rows"
  else
    echo "✗ view: $VIEW has ${views:-no} rows — run the DAG once (airflow dags trigger stardelt__pegelonline-hourly)"
    failures=$((failures + 1))
  fi
else
  echo "✗ view: $VIEW is not queryable: $(jq -c '.error.message // .error' <<<"$out" | cut -c1-160)"
  failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
  echo "FAIL ($((total - failures)) of $total)"
  exit 1
fi
echo "PASS ($total of $total)"
exit 0
