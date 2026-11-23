#!/usr/bin/env bash
# The visitor's whole path, measured through Nova's API as the person `demo`
# (stardelt/stardelt#929, #98 item 4 — the read half and the visitor's run half).
#
#   make verify-product-path              (STARDELT_NOVA_URL=…, ISSUER=… to override)
#   make verify-product-path-create       the create half (#98 item 4)
#   bash scripts/verify-product-path.sh [--create]
#
# `--create` acts as the person `product-path`, a member of the group `demo`. It reads that
# person's password from Secret `product-path-user` at runtime, never prints it, and deletes
# the pipeline it creates on every exit.
#
# `verify-demo-visitor.sh` (#527) checks one page at a time. This walks the path a person
# actually takes, as the visitor, without a write and without a new identity:
#
#   sign-in    the minted token              `demo` on the public client `stardelt-dev`
#   search     GET /api/catalog/search?q=shop   a match for bronze.shop.customers
#   table      GET /api/catalog/tables/shop/customers?catalog=bronze
#                                            `metadata-location` and the Iceberg metadata
#   query      POST /api/statements          `SELECT count(*) … FROM bronze.shop.customers`
#              GET /api/statements/{id}      SUCCEEDED, one column `customers`, rows > 0
#              DELETE /api/statements/{id}   the person's statement slot is free again
#   query-log  GET /api/query-log?limit=10   an entry whose `sql` names the statement
#   run        POST /api/pipelines/{ns}/{name}/runs            a viewer starts shop-1-bronze
#              GET  …/runs/{run_id}                            the run reaches `success`
#   log        GET  …/runs/{run_id}/tasks/{task_id}/logs       the task's log lines, not an empty page
#
# The failures that reach a person are at the joins: a search hit whose metadata 404s in the
# other catalog, a statement that submits and never finishes, a query log that never sees it.
#
# The password is `demo` and it is PUBLIC ON PURPOSE: scripts/public-demo.sh creates that person
# and prints it, so it protects nothing. The password and the token live in two `mktemp` files
# (0600, removed on every exit) and nothing here prints the token. Every request is a read
# except the statement, its cancel and one manual run of `shop-1-bronze`; a run writes the
# demo's own bronze tables, as the demo's **Run now** does, and creates and deletes nothing.
#
# PASS (exit 0) = every row held. FAIL (exit 1) = the first row that did not, named in the last
# line — a check that passes before the demo is deployed proves nothing.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

CREATE=0
case "${1:-}" in
  "") ;;
  --create) CREATE=1 ;;
  *) echo "usage: verify-product-path.sh [--create]" >&2; exit 2 ;;
esac

NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"
NOVA="${NOVA%/}"
ISSUER="${ISSUER:-https://id.alpha.dev.stardelt.io/realms/stardelt}"
ISSUER="${ISSUER%/}"

PIPELINES_NS=stardelt
if [ "$CREATE" = 1 ]; then
  PERSON=product-path
  PIPELINE="product-path-$(date +%s)"
  RUN_PIPELINE="$PIPELINE"
else
  PERSON=demo
  RUN_PIPELINE=shop-1-bronze
fi

PASSWORD_FILE="$(mktemp)"
TOKEN_FILE="$(mktemp)"
BODY_FILE="$(mktemp)"
RUN_FILE="$(mktemp)"
chmod 600 "$PASSWORD_FILE" "$TOKEN_FILE" "$BODY_FILE" "$RUN_FILE"
# `mint-user-token.py` strips the trailing newline; the realm compares the exact string.
if [ "$CREATE" = 1 ]; then
  command -v kubectl >/dev/null 2>&1 || { echo "verify-product-path: FAIL (create) kubectl not found: --create reads Secret product-path-user"; exit 1; }
  if ! kubectl -n stardelt get secret product-path-user -o jsonpath='{.data.password}' 2>/dev/null | base64 -d >"$PASSWORD_FILE" \
     || [ ! -s "$PASSWORD_FILE" ]; then
    echo "verify-product-path: FAIL (create) cannot read Secret product-path-user: run make public-demo YES=1"
    exit 1
  fi
else
  printf 'demo\n' > "$PASSWORD_FILE"
fi

CURRENT_ROW=startup
cleanup() {
  if [ "$CREATE" = 1 ] && [ -s "$TOKEN_FILE" ]; then
    curl -fsS -X DELETE -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
      "$NOVA/api/pipelines/$PIPELINES_NS/$PIPELINE" >/dev/null || true
  fi
  rm -f "$PASSWORD_FILE" "$TOKEN_FILE" "$BODY_FILE" "$RUN_FILE"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

fail() { # fail <reason>: the first breaking row, by its id, in the last line.
  echo "✗ $1"
  echo "verify-product-path: FAIL ($CURRENT_ROW)"
  exit 1
}

for tool in curl python3; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool not found"
done

# Every API call writes its body to one file: a check parses it right away, so no call's answer
# outlives the next call. A curl that does not answer is a row failure, never a silent empty body.
api() { # api <path>
  if ! curl -fsS -H "Authorization: Bearer $(cat "$TOKEN_FILE")" "$NOVA$1" >"$BODY_FILE"; then
    fail "$1 did not answer"
  fi
}

row() { # row <id> <function>: one line of output, `✓ …`, or the first FAIL ends the script.
  CURRENT_ROW="$1"
  local out
  out="$("$2")" || true
  [ -n "$out" ] || fail "the $1 row printed nothing"
  case "$out" in
    "✓ "*) printf '%s\n' "$out" ;;
    "FAIL "*) fail "${out#FAIL }" ;;
    *) fail "${out#FAIL }" ;;
  esac
}

row_sign_in() {
  if ! out="$(ISSUER="$ISSUER" USER="$PERSON" PASSWORD_FILE="$PASSWORD_FILE" TOKEN_FILE="$TOKEN_FILE" \
        python3 scripts/mint-user-token.py 2>&1)"; then
    printf 'FAIL cannot mint a token for %s: %s\n' "$PERSON" "$(printf '%s\n' "$out" | head -n 1)"
    return 1
  fi
  echo "✓ sign-in $PERSON"
}

row_search() {
  api "/api/catalog/search?q=shop&limit=20"
  local verdict
  verdict="$(python3 - "$BODY_FILE" <<'PY' || true
import json, sys

try:
    page = json.load(open(sys.argv[1]))
except ValueError as exc:
    print("FAIL the search answer is not JSON: %s" % exc)
    raise SystemExit
matches = page.get("matches")
if not isinstance(matches, list):
    print("FAIL the search answer carries no matches list")
    raise SystemExit
wanted = {"catalog": "bronze", "namespace": "shop", "table": "customers"}
for match in matches:
    # `catalog`, `namespace` and `table` are required on every match: a partial one is a bug.
    if any(not isinstance(match.get(key), str) or not match.get(key) for key in wanted):
        print("FAIL a search match is not a {catalog, namespace, table} triple: %s"
              % json.dumps(match))
        break
    if all(match.get(key) == value for key, value in wanted.items()):
        print("✓ search shop → bronze.shop.customers")
        break
else:
    seen = ", ".join("%s.%s.%s" % (m.get("catalog"), m.get("namespace"), m.get("table"))
                     for m in matches) or "none"
    print("FAIL no match is bronze.shop.customers (matches: %s)" % seen)
PY
)"
  printf '%s\n' "$verdict"
  case "$verdict" in "✓ "*) return 0 ;; *) return 1 ;; esac
}

row_table() {
  api "/api/catalog/tables/shop/customers?catalog=bronze"
  local verdict
  verdict="$(python3 - "$BODY_FILE" <<'PY' || true
import json, sys

try:
    document = json.load(open(sys.argv[1]))
except ValueError as exc:
    print("FAIL the table answer is not JSON: %s" % exc)
    raise SystemExit
location = document.get("metadata-location")
if not isinstance(location, str) or not location:
    print("FAIL bronze.shop.customers carries no metadata-location (%r)" % (location,))
    raise SystemExit
metadata = document.get("metadata")
if not isinstance(metadata, dict):
    print("FAIL bronze.shop.customers carries no metadata document")
    raise SystemExit
if "current-schema-id" not in metadata:
    print("FAIL bronze.shop.customers' metadata has no current-schema-id")
    raise SystemExit
for stored in ("config", "storage-credentials"):
    if stored in document:
        print("FAIL the table answer carries %r: a storage credential must not reach a browser or an agent" % (stored,))
        raise SystemExit
print("✓ table bronze.shop.customers")
PY
)"
  printf '%s\n' "$verdict"
  case "$verdict" in "✓ "*) return 0 ;; *) return 1 ;; esac
}

statement_state() { # statement_state: the `state` of BODY_FILE's statement view, `?` when none.
  python3 - "$BODY_FILE" <<'PY'
import json, sys

try:
    print(json.load(open(sys.argv[1])).get("state") or "?")
except ValueError:
    print("?")
PY
}

row_query() {
  local id state verdict i
  # The person's own statement: one number out of a table the visitor may read.
  if ! curl -fsS -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
      -X POST -H 'Content-Type: application/json' --data @- "$NOVA/api/statements" >"$BODY_FILE" <<'JSON'
{"sql": "SELECT count(*) AS customers FROM bronze.shop.customers", "waitSeconds": 30}
JSON
  then
    echo "FAIL POST /api/statements did not answer"
    return 1
  fi
  id="$(python3 - "$BODY_FILE" <<'PY'
import json, sys

try:
    print(json.load(open(sys.argv[1])).get("statementId") or "")
except ValueError:
    print("")
PY
)"
  if [ -z "$id" ]; then
    echo "FAIL the statement's answer carries no statementId"
    return 1
  fi

  # Six polls of ten seconds: PENDING and RUNNING are not failures yet, a statement that is
  # still one of them after a minute is.
  state=PENDING
  for i in 1 2 3 4 5 6; do
    if ! curl -fsS -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
        "$NOVA/api/statements/$id?waitSeconds=10" >"$BODY_FILE"; then
      curl -fsS -X DELETE -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
        "$NOVA/api/statements/$id" >/dev/null || true
      echo "FAIL GET /api/statements/$id did not answer"
      return 1
    fi
    state="$(statement_state)"
    case "$state" in
      PENDING|RUNNING) sleep 1 ;;
      *) break ;;
    esac
  done
  case "$state" in
    SUCCEEDED) : ;;
    *)
      curl -fsS -X DELETE -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
        "$NOVA/api/statements/$id" >/dev/null || true
      echo "FAIL the statement is $state, not SUCCEEDED"
      return 1
      ;;
  esac

  verdict="$(python3 - "$BODY_FILE" <<'PY'
import json, sys

view = json.load(open(sys.argv[1]))
error = view.get("error")
if error:
    print("FAIL the statement failed: %s" % (error,))
    raise SystemExit
columns = view.get("columns") or []
if not columns or columns[0].get("name") != "customers":
    print("FAIL the statement's first column is %r, not customers"
          % ((columns[0].get("name") if columns else None),))
    raise SystemExit
rows = view.get("rows") or []
value = rows[0][0] if rows and rows[0] else None
if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
    print("FAIL the statement's count is %r, not a positive integer" % (value,))
else:
    print("✓ query count(*) = %d" % value)
PY
)" || true
  # The cancel runs on every outcome so the person's statement slot is free again.
  if ! curl -fsS -X DELETE -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
      "$NOVA/api/statements/$id" >/dev/null; then
    echo "FAIL cannot cancel statement $id"
    return 1
  fi
  [ -n "$verdict" ] || verdict="FAIL the statement's answer is not JSON"
  printf '%s\n' "$verdict"
  case "$verdict" in "✓ "*) return 0 ;; *) return 1 ;; esac
}

row_query_log() {
  local verdict i
  # The collector is asynchronous: five reads two seconds apart, then the row fails by name.
  for i in 1 2 3 4 5; do
    api "/api/query-log?limit=10"
    verdict="$(python3 - "$BODY_FILE" <<'PY' || true
import json, sys

try:
    page = json.load(open(sys.argv[1]))
except ValueError as exc:
    print("FAIL the query log is not JSON: %s" % exc)
    raise SystemExit
items = page.get("items")
if not isinstance(items, list):
    print("FAIL the query log page carries no items list")
    raise SystemExit
for item in items:
    if "FROM bronze.shop.customers" in (item.get("sql") or ""):
        print("✓ query-log entry")
        break
else:
    print("FAIL the query log does not name the statement")
PY
)"
    case "$verdict" in
      "✓ "*) printf '%s\n' "$verdict"; return 0 ;;
    esac
    [ "$i" -eq 5 ] || sleep 2
  done
  [ -n "$verdict" ] || verdict="FAIL the query log is not JSON"
  printf '%s\n' "$verdict"
  return 1
}

row_pipeline_run() {
  local verdict
  if ! curl -fsS -X POST -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
      -H 'Content-Type: application/json' -d '{}' \
      "$NOVA/api/pipelines/$PIPELINES_NS/$RUN_PIPELINE/runs" >"$BODY_FILE"; then
    echo "FAIL POST /api/pipelines/$PIPELINES_NS/$RUN_PIPELINE/runs did not answer: a viewer may run $RUN_PIPELINE"
    return 1
  fi
  verdict="$(python3 - "$BODY_FILE" "$RUN_FILE" <<'PY'
import json, sys

answer = json.load(open(sys.argv[1]))
run_id = answer.get("runId")
if not run_id:
    print("FAIL the run answer carries no runId")
    raise SystemExit
open(sys.argv[2], "w").write(run_id + "\n")
print("✓ run started as the visitor (%s)" % run_id)
PY
)" || true
  [ -n "$verdict" ] || verdict="FAIL the run answer is not JSON"
  printf '%s\n' "$verdict"
  case "$verdict" in "✓ "*) return 0 ;; *) return 1 ;; esac
}

row_pipeline_log() {
  local run i state task verdict
  run="$(tr -d '\n' < "$RUN_FILE")"
  # shop-1-bronze finished in ~35 s on 2026-11-22; 180 s is the budget for a loaded host.
  for i in $(seq 1 36); do
    api "/api/pipelines/$PIPELINES_NS/$RUN_PIPELINE/runs/$run"
    state="$(python3 -c 'import json,sys; print((json.load(open(sys.argv[1])).get("run") or {}).get("state"))' "$BODY_FILE")"
    case "$state" in
      success) break ;;
      failed|error)
        echo "FAIL the run is $state"
        return 1
        ;;
    esac
    [ "$i" -eq 36 ] && { echo "FAIL the run is still $state after 180 s"; return 1; }
    sleep 5
  done
  task="$(python3 -c 'import json,sys; print(((json.load(open(sys.argv[1])).get("tasks") or [{}])[0]).get("taskId") or "")' "$BODY_FILE")"
  if [ -z "$task" ]; then
    echo "FAIL the run names no task"
    return 1
  fi
  if ! curl -fsS -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
      "$NOVA/api/pipelines/$PIPELINES_NS/$RUN_PIPELINE/runs/$run/tasks/$task/logs" >"$BODY_FILE"; then
    echo "FAIL the log route did not answer for the viewer"
    return 1
  fi
  verdict="$(python3 - "$BODY_FILE" <<'PY'
import json, sys

page = json.load(open(sys.argv[1]))
lines = page.get("lines") or []
text = " ".join((line.get("text") or "") for line in lines)
if not lines or not text.strip():
    print("FAIL the task log is empty")
else:
    print("✓ run succeeded and its log read (%d lines)" % len(lines))
PY
)" || true
  [ -n "$verdict" ] || verdict="FAIL the log answer is not JSON"
  printf '%s\n' "$verdict"
  case "$verdict" in "✓ "*) return 0 ;; *) return 1 ;; esac
}

row_pipeline_create() {
  local verdict
  if ! curl -fsS -X POST -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"$PIPELINE\",\"ownerGroup\":\"demo\",\"sql\":\"SELECT 1 AS one\",\"schedule\":{\"cron\":\"0 5 * * *\",\"timezone\":\"UTC\"}}" \
      "$NOVA/api/pipelines" >"$BODY_FILE"; then
    echo "FAIL POST /api/pipelines did not answer for $PERSON"
    return 1
  fi
  verdict="$(python3 - "$BODY_FILE" "$PIPELINE" <<'PY'
import json, sys

try:
    view = json.load(open(sys.argv[1]))
except ValueError:
    print("FAIL the create answer is not JSON")
    raise SystemExit
if view.get("name") != sys.argv[2]:
    print("FAIL the created pipeline is %r, not %s" % (view.get("name"), sys.argv[2]))
elif view.get("ownerGroup") != "demo":
    print("FAIL the created pipeline is owned by %r, not demo" % (view.get("ownerGroup"),))
elif view.get("schedule") != "0 5 * * * (UTC)":
    print("FAIL the created pipeline's schedule is %r" % (view.get("schedule"),))
elif view.get("canEdit") is not True:
    print("FAIL the creator cannot edit its own pipeline")
else:
    print("✓ create %s (ownerGroup demo, 0 5 * * * (UTC))" % sys.argv[2])
PY
)" || true
  [ -n "$verdict" ] || verdict="FAIL the create answer is not JSON"
  printf '%s\n' "$verdict"
  case "$verdict" in "✓ "*) return 0 ;; *) return 1 ;; esac
}

row_pipeline_schedule() {
  local i verdict
  for i in $(seq 1 12); do
    api "/api/pipelines/$PIPELINES_NS/$PIPELINE"
    verdict="$(python3 - "$BODY_FILE" <<'PY'
import json, sys

try:
    view = json.load(open(sys.argv[1]))
except ValueError:
    print("FAIL the pipeline view is not JSON")
    raise SystemExit
next_run = view.get("nextRun")
if isinstance(next_run, str) and next_run:
    print("✓ schedule reached the scheduler (next run %s)" % next_run)
elif view.get("loaded") is True:
    print("FAIL the scheduler loaded the pipeline but computed no next run")
else:
    print("WAIT")
PY
)"
    case "$verdict" in
      "✓ "*) printf '%s\n' "$verdict"; return 0 ;;
      WAIT) [ "$i" -eq 12 ] && break; sleep 5 ;;
      *) printf '%s\n' "$verdict"; return 1 ;;
    esac
  done
  echo "FAIL the pipeline is not loaded after 60 s, so the cron reached no scheduler"
  return 1
}

row_pipeline_delete() {
  local code verdict
  code="$(curl -sS -o "$BODY_FILE" -w '%{http_code}' -X DELETE \
    -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
    "$NOVA/api/pipelines/$PIPELINES_NS/$PIPELINE")" || {
    echo "FAIL DELETE /api/pipelines/$PIPELINES_NS/$PIPELINE did not answer"
    return 1
  }
  if [ "$code" != 204 ]; then
    echo "FAIL DELETE answered $code, not 204"
    return 1
  fi
  code="$(curl -sS -o "$BODY_FILE" -w '%{http_code}' -H "Authorization: Bearer $(cat "$TOKEN_FILE")" \
    "$NOVA/api/pipelines/$PIPELINES_NS/$PIPELINE")"
  if [ "$code" != 404 ]; then
    echo "FAIL the deleted pipeline still answers $code"
    return 1
  fi
  echo "✓ delete $PIPELINE (then 404)"
}

row sign-in row_sign_in
if [ "$CREATE" = 1 ]; then
  row create row_pipeline_create
  row schedule row_pipeline_schedule
  row run row_pipeline_run
  row log row_pipeline_log
  row delete row_pipeline_delete
else
  row search row_search
  row table row_table
  row query row_query
  row query-log row_query_log
  row run row_pipeline_run
  row log row_pipeline_log
fi

if [ "$CREATE" = 1 ]; then
  echo "verify-product-path --create: PASS"
else
  echo "verify-product-path: PASS"
fi
