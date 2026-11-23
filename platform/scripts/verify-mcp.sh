#!/usr/bin/env bash
# The shipped agent endpoint answers its discovery metadata and its 401 challenge
# (stardelt/stardelt#88, #17; ticket #1438).
#
#   make verify-mcp
#   NOVA_HOST=https://stardelt.example.com bash scripts/verify-mcp.sh
#
# The 0.0.5 exit criterion is "a local AI agent uses the platform as the signed-in user": the
# hosted MCP server is delivered, but no check proved it on an install, so a release could ship
# with `/mcp` answering 404, a metadata document naming another resource, or a challenge asking
# for another scope while every other row stayed green. Three UNSIGNED requests — no credential,
# no cluster, nothing created or deleted:
#
#   metadata      GET  $HOST/.well-known/oauth-protected-resource → 200, its `resource` is
#                      $HOST/mcp, `authorization_servers[0]` is the realm, and `stardelt:read`
#                      is in `scopes_supported`
#   metadata/mcp  GET  $HOST/.well-known/oauth-protected-resource/mcp → 200, the same `resource`
#   challenge     POST $HOST/mcp with {"jsonrpc":"2.0","id":1,"method":"tools/list"} → 401 whose
#                      www-authenticate carries resource_metadata="…/oauth-protected-resource/mcp"
#                      and scope="stardelt:read"
#
# These are what the client's discovery reads before any sign-in. A signed-in `tools/list` needs
# a person's browser, which is not a check a machine can run (out of scope, #1438). With
# `spec.mcp.enabled: false` Nova answers these three paths 404, so an install that serves no
# agent endpoint FAILS this row on purpose: the row names the missing capability, it is not a
# reason to skip it.
#
# The host: NOVA_HOST, else https://$BASE_DOMAIN, else the lab's https://alpha.dev.stardelt.io —
# verify-release.sh calls this script directly and names neither, as verify-product-path.sh
# defaults too. The realm is https://id.<host>/realms/stardelt; REALM_URL overrides it for an
# install whose IdP is not at id.<host>. Nothing here is Kubernetes-specific: the requests go to
# the public host, so the check runs from a laptop as well as from the lab.
#
# Exit 0 = PASS, 1 = FAIL (one ✗ row, and every row still ran), 3 = UNVERIFIED (no usable host:
# nothing was asked, so nothing was proved).
set -uo pipefail

BASE_DOMAIN="${BASE_DOMAIN:-}"
HOST="${NOVA_HOST:-${BASE_DOMAIN:+https://${BASE_DOMAIN}}}"
HOST="${HOST:-https://alpha.dev.stardelt.io}"
HOST="${HOST%/}"
REALM_URL="${REALM_URL:-https://id.${HOST#*://}/realms/stardelt}"
REALM_URL="${REALM_URL%/}"

case "$HOST" in
  http://*|https://*) ;;
  *)
    echo "verify-mcp: UNVERIFIED (no host — '$HOST' is not an http(s) URL; set NOVA_HOST=https://<host>)"
    exit 3
    ;;
esac
command -v curl >/dev/null 2>&1 || { echo "verify-mcp: UNVERIFIED (curl not found)"; exit 3; }
command -v python3 >/dev/null 2>&1 || { echo "verify-mcp: UNVERIFIED (python3 not found)"; exit 3; }

umask 077
tmp="$(mktemp -d "${TMPDIR:-/tmp}/verify-mcp.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

PASSED=0
FAILED=0
ok()  { printf '✓ %-16s %s\n' "$1" "$2"; PASSED=$((PASSED + 1)); }
bad() { printf '✗ %-16s %s\n' "$1" "$2"; FAILED=$((FAILED + 1)); }

# fetch <method> <url> [json body]: the status code on stdout; headers in $tmp/headers, body in
# $tmp/body, curl's own message in $tmp/err. A host that never answered prints `000`.
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

# ---- metadata --------------------------------------------------------------

metadata_url="$HOST/.well-known/oauth-protected-resource"
status="$(fetch GET "$metadata_url")"
if [ "$status" = "200" ]; then
  if detail="$(python3 -c '
import json, sys
resource, realm = sys.argv[1], sys.argv[2]
try:
    doc = json.load(sys.stdin)
except ValueError:
    print("body is not JSON")
    sys.exit(1)
servers = doc.get("authorization_servers") or []
problems = []
if doc.get("resource") != resource:
    problems.append("resource=%s" % (doc.get("resource"),))
if not servers or servers[0] != realm:
    problems.append("authorization_servers[0]=%s" % ((servers[0] if servers else None),))
if "stardelt:read" not in (doc.get("scopes_supported") or []):
    problems.append("scopes_supported=%s" % (doc.get("scopes_supported"),))
if problems:
    print(", ".join(problems))
    sys.exit(1)
print("resource=%s" % (doc["resource"],))
' "$HOST/mcp" "$REALM_URL" < "$tmp/body")"; then
    ok metadata "$metadata_url → $status, $detail"
  else
    bad metadata "$metadata_url → $status, $detail (want resource=$HOST/mcp, authorization_servers[0]=$REALM_URL, scopes stardelt:read)"
  fi
else
  bad metadata "$metadata_url → $(describe "$status")"
fi

# ---- the same document for the /mcp path -----------------------------------

mcp_metadata_url="$HOST/.well-known/oauth-protected-resource/mcp"
status="$(fetch GET "$mcp_metadata_url")"
if [ "$status" = "200" ]; then
  if detail="$(python3 -c '
import json, sys
resource = sys.argv[1]
try:
    doc = json.load(sys.stdin)
except ValueError:
    print("body is not JSON")
    sys.exit(1)
if doc.get("resource") != resource:
    print("resource=%s" % (doc.get("resource"),))
    sys.exit(1)
print("resource=%s" % (doc["resource"],))
' "$HOST/mcp" < "$tmp/body")"; then
    ok metadata/mcp "$mcp_metadata_url → $status, $detail"
  else
    bad metadata/mcp "$mcp_metadata_url → $status, $detail (want resource=$HOST/mcp)"
  fi
else
  bad metadata/mcp "$mcp_metadata_url → $(describe "$status")"
fi

# ---- the challenge an unauthenticated call meets ---------------------------

status="$(fetch POST "$HOST/mcp" '{"jsonrpc":"2.0","id":1,"method":"tools/list"}')"
# Header names are case-insensitive; the values compared here are lowercase by construction.
www="$(tr -d '\r' < "$tmp/headers" 2>/dev/null | tr '[:upper:]' '[:lower:]' \
  | grep '^www-authenticate:' | tr '\n' ' ' || true)"
want_metadata="resource_metadata=\"$HOST/.well-known/oauth-protected-resource/mcp\""
want_scope='scope="stardelt:read"'
if [ "$status" = "401" ]; then
  missing=""
  case "$www" in *"$want_metadata"*) ;; *) missing="resource_metadata=\"$HOST/.well-known/oauth-protected-resource/mcp\"" ;; esac
  case "$www" in *"$want_scope"*) ;; *) missing="${missing:+$missing and }scope=\"stardelt:read\"" ;; esac
  if [ -z "$missing" ]; then
    ok challenge "POST /mcp → $status, scope=stardelt:read"
  else
    bad challenge "POST /mcp → $status, www-authenticate lacks $missing (got: ${www:-no www-authenticate header})"
  fi
else
  bad challenge "POST /mcp → $(describe "$status") (want 401 with a Bearer challenge)"
fi

# ---- verdict ---------------------------------------------------------------

if [ "$PASSED" -eq 3 ] && [ "$FAILED" -eq 0 ]; then
  echo "verify-mcp: PASS (3 of 3)"
  exit 0
fi
echo "verify-mcp: FAIL ($PASSED of 3)"
exit 1
