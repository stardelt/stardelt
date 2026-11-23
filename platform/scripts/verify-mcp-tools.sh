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
#                  `aud` is `$HOST/mcp` and its `azp` is `$MCP_CLIENT`
#   tools/list     POST $HOST/mcp {"jsonrpc":"2.0","id":1,"method":"tools/list"} → 200 and the
#                  names carry `platform_status`, `sql_read` and `pipeline_start`
#   platform_status tools/call `platform_status` → 200, `status` is `ok`, `db` is `up|disabled`
#   write-scope    tools/call `pipeline_start` → 403 `insufficient_scope` asking for
#                  `stardelt:write`, and no run is started: the `stardelt-mcp` token a device
#                  sign-in mints must not carry the optional write scope
#
# `sql_read`, `sql_explain` and `catalog_search` are listed here (so a missing tool is caught)
# but not called: they are broken on the live cluster for a measured reason and are fixed under
# stardelt/stardelt#1442. A follow-up adds the SQL call when #1442 closes.
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
if aud != want_aud or azp != want_azp:
    print(\"the token is not the MCP client's: aud=%s azp=%s\" % (aud, azp))
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
' platform_status sql_read pipeline_start < "$tmp/body")"; then
      ok tools/list "POST /mcp → $status, $detail"
    else
      bad tools/list "POST /mcp → $status, $detail (want platform_status, sql_read, pipeline_start)"
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

fi

# ---- verdict ---------------------------------------------------------------

if [ "$PASSED" -eq 4 ] && [ "$FAILED" -eq 0 ]; then
  echo "verify-mcp-tools: PASS (4 of 4)"
  exit 0
fi
echo "verify-mcp-tools: FAIL ($PASSED of 4)"
exit 1
