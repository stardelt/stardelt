#!/usr/bin/env bash
# A signed-in MCP client lists the shipped tools and the platform answers one of them
# (stardelt/stardelt#88, #17; ticket #1443).
#
#   make verify-mcp-tools
#   MCP_USER=demo MCP_CLIENT=stardelt-mcp NOVA_HOST=https://stardelt.example.com bash scripts/verify-mcp-tools.sh
#
# `verify-mcp.sh` (#1438) proves the unsigned half: the discovery metadata and the 401 challenge.
# This proves the signed-in half, which is the 0.0.5 exit criterion: a person's MCP token
# (audience `<host>/mcp`, client `stardelt-mcp`, scope `stardelt:read`) is accepted, the tool
# list is the shipped one, and a tool answer comes back. Without it a release can ship with
# `/mcp` answering 401 to every signed-in call while the gate stays green.
#
#   sign-in        the token `mint-user-token.py` mints for `$MCP_USER` on `$MCP_CLIENT`: its
#                  `azp` is `$MCP_CLIENT`, `$HOST/mcp` is among its audiences, and the only
#                  other audience is `nova` (the exchange actor #1442/#1455 needs)
#   tools/list     POST $HOST/mcp {"jsonrpc":"2.0","id":1,"method":"tools/list"} → 200 and the
#                  names carry `platform_status`, `sql_read`, `pipeline_start` and `table_health`
#   platform_status tools/call `platform_status` → 200, `status` is `ok`, `db` is `up|disabled`
#   write-scope    tools/call `pipeline_start` → 403 `insufficient_scope` asking for
#                  `stardelt:write`, and no run is started: the `stardelt-mcp` token a device
#                  sign-in mints must not carry the optional write scope
#
#   sql_read       tools/call `sql_read` {"sql":"SELECT 1 AS one"} → one row `[[1]]`, column `one`
#   sql_explain    tools/call `sql_explain` → at least one plan row, column `Query Plan`
#   catalog_search tools/call `catalog_search` {"query":"customers","catalog":"bronze"} →
#                  `bronze.shop.customers` among the matches
#   table_health   tools/call `table_health` on bronze.shop.customers → one of grey, green, amber, red
#   query-log      the agent's statement is attributed to the person, source `stardelt-mcp`
#
# These call the engine through the token exchange #1442/#1455 added: the MCP token is never
# forwarded to Trino.
#
# The person and the client are the check's inputs: MCP_USER (default `demo`) and MCP_CLIENT
# (default `stardelt-mcp`). The client's token is the one to use — the public demo client
# `stardelt-dev` carries `aud` for nova/trino/lakekeeper and `/mcp` refuses it with
# `401 invalid token: wrong audience`. The password is MCP_PASSWORD_FILE when set, else the
# public demo password (scripts/public-demo.sh prints it: it protects nothing) in a 0600 file.
# The token lives in that temp dir (0700, removed on every exit) and is never printed.
#
# The host: NOVA_HOST, else https://$BASE_DOMAIN, else the lab's https://alpha.dev.stardelt.io —
# verify-release.sh calls this script directly and names neither, as verify-product-path.sh
# defaults too. The realm is https://id.<host>/realms/stardelt; REALM_URL overrides it for an
# install whose IdP is not at id.<host>. Nothing here is Kubernetes-specific: the requests go to
# the public host and the realm, so the check runs from a laptop as well as from the lab.
#
# Exit 0 = PASS, 1 = FAIL (one ✗ row, and every row still ran), 3 = UNVERIFIED (no usable host,
# or curl/python3 missing: nothing was asked, so nothing was proved).
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

MCP_USER="${MCP_USER:-demo}"
MCP_CLIENT="${MCP_CLIENT:-stardelt-mcp}"

BASE_DOMAIN="${BASE_DOMAIN:-}"
HOST="${NOVA_HOST:-${BASE_DOMAIN:+https://${BASE_DOMAIN}}}"
HOST="${HOST:-https://alpha.dev.stardelt.io}"
HOST="${HOST%/}"
REALM_URL="${REALM_URL:-https://id.${HOST#*://}/realms/stardelt}"
REALM_URL="${REALM_URL%/}"

case "$HOST" in
  http://*|https://*) ;;
  *)
    echo "verify-mcp-tools: UNVERIFIED (no host — '$HOST' is not an http(s) URL; set NOVA_HOST=https://<host>)"
    exit 3
    ;;
esac
command -v curl >/dev/null 2>&1 || { echo "verify-mcp-tools: UNVERIFIED (curl not found)"; exit 3; }
command -v python3 >/dev/null 2>&1 || { echo "verify-mcp-tools: UNVERIFIED (python3 not found)"; exit 3; }

umask 077
tmp="$(mktemp -d "${TMPDIR:-/tmp}/verify-mcp-tools.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

PASSED=0
FAILED=0
ok()  { printf '✓ %-16s %s\n' "$1" "$2"; PASSED=$((PASSED + 1)); }
bad() { printf '✗ %-16s %s\n' "$1" "$2"; FAILED=$((FAILED + 1)); }

# fetch <method> <url> [json body]: the status code on stdout; headers in $tmp/headers, body in
# $tmp/body, curl's own message in $tmp/err. A host that never answered prints `000`. The bearer
# header travels in a curl config file, never on the command line.
fetch() {
  local method="$1" url="$2" body="${3:-}"
  local args=(--silent --show-error --connect-timeout 10 --max-time 30
              -X "$method" -D "$tmp/headers" -o "$tmp/body" -w '%{http_code}')
  : > "$tmp/headers"
  : > "$tmp/body"
  if [ -n "$body" ]; then
    printf '%s' "$body" > "$tmp/request.json"
    args+=(-H 'content-type: application/json' --data-binary "@$tmp/request.json")
  fi
  if [ -s "$tmp/auth.curlrc" ]; then
    args+=(-K "$tmp/auth.curlrc")
  fi
  curl "${args[@]}" "$url" 2>"$tmp/err" || true
}

# curl_message: curl's own failure in one line, in parentheses, or nothing.
curl_message() {
  local msg
  msg="$(tr -d '\r' < "$tmp/err" 2>/dev/null | tr '\n' ' ' | sed -e 's/[[:space:]]*$//')"
  if [ -n "$msg" ]; then printf ' (%s)' "$msg"; fi
}

# describe <status>: the status, or `unreachable` for curl's 000.
describe() {
  if [ "$1" = "000" ] || [ -z "$1" ]; then
    printf 'unreachable%s' "$(curl_message)"
  else
    printf '%s' "$1"
  fi
}

# error_of: the body's top-level `error` in one line, or nothing.
error_of() {
  python3 -c '
import json, sys
try:
    doc = json.load(sys.stdin)
except ValueError:
    sys.exit(0)
error = doc.get("error") if isinstance(doc, dict) else None
if error:
    print(", " + str(error), end="")
' < "$tmp/body" 2>/dev/null || true
}

# ---- sign-in: the person's own token, minted by the client `stardelt-mcp` -------------------

password_file="${MCP_PASSWORD_FILE:-}"
if [ -z "$password_file" ]; then
  # The public demo password (scripts/public-demo.sh creates that person and prints it).
  password_file="$tmp/password"
  printf 'demo\n' > "$password_file"
  chmod 600 "$password_file"
fi

TOKEN_OK=0
if [ ! -r "$password_file" ]; then
  bad sign-in "cannot read MCP_PASSWORD_FILE=$password_file"
elif ! ISSUER="$REALM_URL" USER="$MCP_USER" PASSWORD_FILE="$password_file" \
       TOKEN_FILE="$tmp/token" CLIENT_ID="$MCP_CLIENT" \
       python3 scripts/mint-user-token.py > "$tmp/mint.log" 2>&1; then
  bad sign-in "minting a token for $MCP_USER as $MCP_CLIENT failed: $(tail -n 1 "$tmp/mint.log" | cut -c1-200)"
elif [ ! -s "$tmp/token" ]; then
  bad sign-in "minting a token for $MCP_USER as $MCP_CLIENT wrote no token"
else
  claims="$(python3 scripts/mint-user-token.py --decode "$tmp/token" 2>&1)"
  if detail="$(python3 -c "
import re, sys
line = sys.stdin.read()

def field(name):
    match = re.search(r'(?:^|\s)' + name + r'=(\S*)', line)
    return match.group(1) if match else None

want_aud, want_azp, user = sys.argv[1], sys.argv[2], sys.argv[3]
aud, azp = field('aud'), field('azp')
# The MCP token names its own audience and the exchange actor (nova, which the realm must
# see on the subject token before it mints the engine audience). It never names the engine.
allowed = {want_aud, 'nova'}
audiences = [a for a in (aud or '').split(',') if a]
if azp != want_azp:
    print(\"the token is not the MCP client's: azp=%s\" % (azp,))
    sys.exit(1)
if want_aud not in audiences:
    print(\"the token does not name the MCP resource: aud=%s\" % (aud,))
    sys.exit(1)
extra = sorted(set(audiences) - allowed)
if extra:
    print(\"the token carries an audience it must not: %s\" % (', '.join(extra),))
    sys.exit(1)
print('%s as %s (aud=%s)' % (user, azp, aud))
" "$HOST/mcp" "$MCP_CLIENT" "$MCP_USER" <<< "$claims")"; then
    ok sign-in "$detail"
    TOKEN_OK=1
    printf 'header = "Authorization: Bearer %s"\n' "$(tr -d '\n' < "$tmp/token")" > "$tmp/auth.curlrc"
    chmod 600 "$tmp/auth.curlrc"
  else
    bad sign-in "$detail (want aud=$HOST/mcp azp=$MCP_CLIENT; $claims)"
  fi
fi

# ---- tools/list: the shipped tool list ------------------------------------------------------

if [ "$TOKEN_OK" != 1 ]; then
  bad tools/list "not run: sign-in minted no token"
  bad platform_status "not run: sign-in minted no token"
  bad write-scope "not run: sign-in minted no token"
  bad sql_read "not run: sign-in minted no token"
  bad sql_explain "not run: sign-in minted no token"
  bad catalog_search "not run: sign-in minted no token"
  bad table_health "not run: sign-in minted no token"
  bad query-log "not run: sign-in minted no token"
else

  status="$(fetch POST "$HOST/mcp" '{"jsonrpc":"2.0","id":1,"method":"tools/list"}')"
  if [ "$status" = "200" ]; then
    if detail="$(python3 -c '
import json, sys
try:
    doc = json.load(sys.stdin)
except ValueError:
    print("body is not JSON")
    sys.exit(1)
result = doc.get("result") if isinstance(doc, dict) else None
tools = (result or {}).get("tools")
if not isinstance(tools, list):
    print("no result.tools in the answer")
    sys.exit(1)
names = [t.get("name") for t in tools if isinstance(t, dict)]
missing = [name for name in sys.argv[1:] if name not in names]
if missing:
    print("%d tools, missing %s" % (len(names), ", ".join(missing)))
    sys.exit(1)
print("%d tools (%s present)" % (len(names), ", ".join(sys.argv[1:])))
' platform_status sql_read pipeline_start table_health < "$tmp/body")"; then
      ok tools/list "POST /mcp → $status, $detail"
    else
      bad tools/list "POST /mcp → $status, $detail (want platform_status, sql_read, pipeline_start, table_health)"
    fi
  else
    bad tools/list "POST /mcp → $(describe "$status")$(error_of) (want 200 with result.tools)"
  fi

  # ---- platform_status: the platform answers a tool -----------------------------------------

  status="$(fetch POST "$HOST/mcp" '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"platform_status","arguments":{}}}')"
  if [ "$status" = "200" ]; then
    if detail="$(python3 -c '
import json, sys
try:
    doc = json.load(sys.stdin)
except ValueError:
    print("body is not JSON")
    sys.exit(1)
result = doc.get("result") if isinstance(doc, dict) else None
structured = (result or {}).get("structuredContent") or {}
status, db = structured.get("status"), structured.get("db")
if status != "ok":
    print("status=%s" % (status,))
    sys.exit(1)
if db not in ("up", "disabled"):
    print("db=%s" % (db,))
    sys.exit(1)
print("status=%s db=%s kind=%s stage=%s" % (status, db, structured.get("kind"), structured.get("stage")))
' < "$tmp/body")"; then
      ok platform_status "tools/call → $detail"
    else
      bad platform_status "tools/call → $status, $detail$(error_of) (want status=ok db=up|disabled)"
    fi
  else
    bad platform_status "tools/call → $(describe "$status")$(error_of) (want 200 with structuredContent)"
  fi

  # ---- write-scope: a read token cannot write, and starts nothing ----------------------------

  status="$(fetch POST "$HOST/mcp" '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"pipeline_start","arguments":{"pipeline":"stardelt/shop-1-bronze","reason":"verify-mcp-tools"}}}')"
  if [ "$status" = "403" ]; then
    if detail="$(python3 -c '
import json, sys
try:
    doc = json.load(sys.stdin)
except ValueError:
    print("body is not JSON")
    sys.exit(1)
error = doc.get("error") if isinstance(doc, dict) else None
required = doc.get("required_scope") if isinstance(doc, dict) else None
if error != "insufficient_scope" or required != "stardelt:write":
    print("error=%s required_scope=%s" % (error, required))
    sys.exit(1)
print("pipeline_start without stardelt:write → 403 insufficient_scope")
' < "$tmp/body")"; then
      ok write-scope "$detail"
    else
      bad write-scope "POST /mcp → $status, $detail (want error=insufficient_scope required_scope=stardelt:write)"
    fi
  elif [ "$status" = "200" ]; then
    run="$(python3 -c '
import json, sys
try:
    doc = json.load(sys.stdin)
except ValueError:
    sys.exit(0)
result = doc.get("result") if isinstance(doc, dict) else None
structured = (result or {}).get("structuredContent") or {}
print(structured.get("run_id") or structured.get("run") or structured.get("id") or structured)
' < "$tmp/body" 2>/dev/null || true)"
    bad write-scope "the call was allowed and started a run: ${run:-no run id in the answer} (the stardelt-mcp token must not carry stardelt:write)"
  else
    bad write-scope "POST /mcp → $(describe "$status")$(error_of) (want 403 insufficient_scope)"
  fi

  # ---- sql_read: the engine answers through the token exchange --------------------------------

  status="$(fetch POST "$HOST/mcp" '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"sql_read","arguments":{"sql":"SELECT 1 AS one"}}}')"
  if [ "$status" = "200" ]; then
    if detail="$(python3 -c '
import json, sys
try:
    doc = json.load(sys.stdin)
except ValueError:
    print("body is not JSON")
    sys.exit(1)
result = doc.get("result") if isinstance(doc, dict) else None
if not isinstance(result, dict):
    print("no result in the answer")
    sys.exit(1)
if result.get("isError") is True:
    content = result.get("content") or [{}]
    print("tool error: %s" % ((content[0] or {}).get("text") or "the tool failed"))
    sys.exit(1)
body = result.get("structuredContent") or {}
columns = [c.get("name") for c in (body.get("columns") or []) if isinstance(c, dict)]
rows = body.get("rows")
if body.get("row_count") != 1 or rows != [[1]] or columns != ["one"]:
    print("row_count=%s rows=%s columns=%s" % (body.get("row_count"), rows, columns))
    sys.exit(1)
print("SELECT 1 AS one → one row, column one")
' < "$tmp/body")"; then
      ok sql_read "tools/call → $detail"
    else
      bad sql_read "tools/call → $detail (want row_count=1 rows=[[1]] columns=[one])"
    fi
  else
    bad sql_read "tools/call → $(describe "$status")$(error_of) (want 200 with a JSON-RPC result)"
  fi

  # ---- sql_explain: a plan, not an execution --------------------------------------------------

  status="$(fetch POST "$HOST/mcp" '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"sql_explain","arguments":{"sql":"SELECT 1 AS one"}}}')"
  if [ "$status" = "200" ]; then
    if detail="$(python3 -c '
import json, sys
try:
    doc = json.load(sys.stdin)
except ValueError:
    print("body is not JSON")
    sys.exit(1)
result = doc.get("result") if isinstance(doc, dict) else None
if not isinstance(result, dict):
    print("no result in the answer")
    sys.exit(1)
if result.get("isError") is True:
    content = result.get("content") or [{}]
    print("tool error: %s" % ((content[0] or {}).get("text") or "the tool failed"))
    sys.exit(1)
body = result.get("structuredContent") or {}
rows = body.get("rows") or []
columns = [c.get("name") for c in (body.get("columns") or []) if isinstance(c, dict)]
if not rows or not columns:
    print("rows=%s columns=%s" % (len(rows), columns))
    sys.exit(1)
print("EXPLAIN SELECT 1 AS one → %d row(s), column %s" % (len(rows), columns[0]))
' < "$tmp/body")"; then
      ok sql_explain "tools/call → $detail"
    else
      bad sql_explain "tools/call → $detail (want at least one plan row and one column)"
    fi
  else
    bad sql_explain "tools/call → $(describe "$status")$(error_of) (want 200 with a JSON-RPC result)"
  fi

  # ---- catalog_search: the visitor's table is findable -----------------------------------------

  status="$(fetch POST "$HOST/mcp" '{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"catalog_search","arguments":{"query":"customers","catalog":"bronze"}}}')"
  if [ "$status" = "200" ]; then
    if detail="$(python3 -c '
import json, sys
try:
    doc = json.load(sys.stdin)
except ValueError:
    print("body is not JSON")
    sys.exit(1)
result = doc.get("result") if isinstance(doc, dict) else None
if not isinstance(result, dict):
    print("no result in the answer")
    sys.exit(1)
if result.get("isError") is True:
    content = result.get("content") or [{}]
    print("tool error: %s" % ((content[0] or {}).get("text") or "the tool failed"))
    sys.exit(1)
body = result.get("structuredContent") or {}
matches = body.get("matches") or []
hit = [m for m in matches if isinstance(m, dict)
       and m.get("catalog") == "bronze" and m.get("namespace") == "shop" and m.get("table") == "customers"]
if not hit:
    print("matches=%s" % (["%s.%s.%s" % (m.get("catalog"), m.get("namespace"), m.get("table")) for m in matches if isinstance(m, dict)][:5],))
    sys.exit(1)
print("customers bronze → bronze.shop.customers")
' < "$tmp/body")"; then
      ok catalog_search "tools/call → $detail"
    else
      bad catalog_search "tools/call → $detail (want bronze.shop.customers among the matches)"
    fi
  else
    bad catalog_search "tools/call → $(describe "$status")$(error_of) (want 200 with a JSON-RPC result)"
  fi

  # ---- table_health: a table's freshness promise is readable through the tool -----------------

  status="$(fetch POST "$HOST/mcp" '{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"table_health","arguments":{"namespace":"shop","table":"customers","catalog":"bronze"}}}')"
  if [ "$status" = "200" ]; then
    if detail="$(python3 -c '
import json, sys
try:
    doc = json.load(sys.stdin)
except ValueError:
    print("body is not JSON")
    sys.exit(1)
result = doc.get("result") if isinstance(doc, dict) else None
if not isinstance(result, dict):
    print("no result in the answer")
    sys.exit(1)
if result.get("isError") is True:
    content = result.get("content") or [{}]
    print("tool error: %s" % ((content[0] or {}).get("text") or "the tool failed"))
    sys.exit(1)
body = result.get("structuredContent") or {}
status = body.get("status")
if status not in ("grey", "green", "amber", "red"):
    print("status=%s" % (status,))
    sys.exit(1)
print("bronze.shop.customers → status=%s detail=%s" % (status, body.get("detail")))
' < "$tmp/body")"; then
      ok table_health "tools/call → $detail"
    else
      bad table_health "tools/call → $detail (want status=grey|green|amber|red)"
    fi
  else
    bad table_health "tools/call → $(describe "$status")$(error_of) (want 200 with a JSON-RPC result)"
  fi

  # ---- query-log: the agent's statement is attributed to the person and the MCP client ---------

  found=""
  for i in 1 2 3 4 5; do
    status="$(fetch POST "$HOST/mcp" '{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"query_log_list","arguments":{"limit":5}}}')"
    if [ "$status" = "200" ] && detail="$(python3 -c '
import json, sys
try:
    doc = json.load(sys.stdin)
except ValueError:
    print("body is not JSON")
    sys.exit(1)
result = doc.get("result") if isinstance(doc, dict) else None
if not isinstance(result, dict):
    print("no result in the answer")
    sys.exit(1)
if result.get("isError") is True:
    content = result.get("content") or [{}]
    print("tool error: %s" % ((content[0] or {}).get("text") or "the tool failed"))
    sys.exit(1)
body = result.get("structuredContent") or {}
for item in body.get("items") or []:
    if not isinstance(item, dict):
        continue
    if "SELECT 1 AS one" in (item.get("sql") or "") and item.get("source") == "stardelt-mcp":
        print("SELECT 1 AS one, source=stardelt-mcp, state=%s" % (item.get("state"),))
        sys.exit(0)
print("no SELECT 1 AS one entry with source=stardelt-mcp yet")
sys.exit(1)
' < "$tmp/body")"; then
      found=1
      break
    fi
    [ "$i" -eq 5 ] || sleep 2
  done
  if [ "$found" = 1 ]; then
    ok query-log "tools/call → $detail"
  else
    bad query-log "tools/call → ${detail:-not run} (the collector is asynchronous: five reads two seconds apart)"
  fi

fi

# ---- verdict ---------------------------------------------------------------

if [ "$PASSED" -eq 9 ] && [ "$FAILED" -eq 0 ]; then
  echo "verify-mcp-tools: PASS (9 of 9)"
  exit 0
fi
echo "verify-mcp-tools: FAIL ($PASSED of 9)"
exit 1
