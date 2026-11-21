#!/usr/bin/env bash
# Prove the Nova contract the lab serves is the contract nova/openapi/nova.json carries at the
# commit the Nova pin names.
#
#   bash scripts/verify-contract.sh
#   DOCUMENT=old.json bash scripts/verify-contract.sh      # compare a local document instead
#   bash scripts/verify-contract.sh --normalize nova.json  # print the normal form (the tests use it)
#
# Why: the OpenAPI document at https://$BASE_DOMAIN/api/openapi.json is what the CLI, the MCP
# server and the UI are generated against, and nothing compared it with the committed document
# (stardelt#951, a slice of #95). verify-pins proves the live image digest matches the pin; it
# says nothing about the document inside that image. An image built from another commit, or a
# pin that was rolled but whose Deployment never restarted, serves a contract nobody committed.
# The comparison is the operation list (method, path, operationId) and the schema names, which is
# what a stale image or a missing roll changes; the request and response bodies are not compared.
#
# Exit codes: 0 the served contract matches, 1 it differs or could not be read. Needs: kubectl,
# jq, git, curl (kubectl and curl are not needed by --normalize).
set -euo pipefail

platform_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="${NAMESPACE:-stardelt}"
BASE_DOMAIN="${BASE_DOMAIN:-alpha.dev.stardelt.io}"
TIMEOUT="${TIMEOUT:-30}"
DOCUMENT="${DOCUMENT:-}"
PIN_FILE="platform/manifests/nova-deployment.yaml"
CONTRACT_FILE="nova/openapi/nova.json"

missing() { printf 'verify-contract: FAIL (%s not found)\n' "$1" >&2; exit 1; }

# One sorted line per operation and per schema: `<METHOD> <path> <operationId|->` and
# `schema <name>`. Measured on the live document, 2026-11-21: 336 lines (134 operations and 202
# schema names). `parameters` is an array and drops out of the `.value | type == "object"` filter.
normalize() {
  local file="$1"
  jq -S -r '
    (.paths // {}) as $paths
    | ( [ $paths | to_entries[] | .key as $p | .value | to_entries[]
          | select(.value | type == "object")
          | "\(.key | ascii_upcase) \($p) \(.value.operationId // "-")" ]
      + [ (.components.schemas // {}) | keys[] | "schema \(.)" ] )
    | .[]' "$file" | sort
}

command -v jq >/dev/null || missing jq

if [ "${1:-}" = "--normalize" ]; then
  [ -n "${2:-}" ] || { echo "verify-contract: FAIL (--normalize needs a document file)" >&2; exit 1; }
  [ -r "$2" ] || missing "$2"
  normalize "$2"
  exit 0
fi

command -v git >/dev/null || missing git
command -v kubectl >/dev/null || missing kubectl
command -v curl >/dev/null || missing curl

repo_root="$(git -C "$platform_root" rev-parse --show-toplevel)"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
live="$tmp/live.json"
committed="$tmp/committed.json"

image="$(kubectl -n "$NAMESPACE" get deployment nova \
  -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)"
[ -n "$image" ] || { echo "verify-contract: FAIL (no Nova deployment in namespace $NAMESPACE)" >&2; exit 1; }
case "$image" in
  *@sha256:*) ;;
  *) echo "verify-contract: FAIL (the Nova deployment carries no digest: $image)" >&2; exit 1 ;;
esac
digest="${image##*@}"

commit="$(git -C "$repo_root" log -1 --format=%H -S"$digest" -- "$PIN_FILE")"
[ -n "$commit" ] || {
  echo "verify-contract: FAIL (no commit pins $digest in $PIN_FILE)" >&2
  exit 1
}
short="${commit:0:7}"
echo "✓ nova image $digest pinned by $short"

git -C "$repo_root" show "$commit:$CONTRACT_FILE" >"$committed" 2>/dev/null || {
  echo "verify-contract: FAIL ($CONTRACT_FILE is not in commit $short)" >&2
  exit 1
}

if [ -n "$DOCUMENT" ]; then
  # -r, not -f: the ticket and its readers pass a process substitution (<(...)), which is a pipe.
  [ -r "$DOCUMENT" ] || missing "$DOCUMENT"
  cp "$DOCUMENT" "$live"
else
  url="https://$BASE_DOMAIN/api/openapi.json"
  http="$(curl -sS -f --max-time "$TIMEOUT" -o "$live" -w '%{http_code}' "$url")" && rc=0 || rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "verify-contract: FAIL (curl could not read $url: exit $rc${http:+, HTTP $http})" >&2
    exit 1
  fi
fi

jq -e . "$live" >/dev/null 2>&1 || {
  echo "verify-contract: FAIL (the served document is not JSON)" >&2
  exit 1
}

normalize "$committed" >"$tmp/committed.txt"
normalize "$live" >"$tmp/live.txt"

operations="$(grep -vc '^schema ' "$tmp/live.txt" || true)"
schemas="$(grep -c '^schema ' "$tmp/live.txt" || true)"

if diff -u "$tmp/committed.txt" "$tmp/live.txt" >"$tmp/diff.txt"; then
  echo "✓ $operations operations and $schemas schemas match $CONTRACT_FILE at $short"
  echo "verify-contract: PASS"
  exit 0
fi

head -20 "$tmp/diff.txt" >&2
changed="$(grep -E '^[+-]' "$tmp/diff.txt" | grep -vcE '^(---|\+\+\+)' || true)"
echo "verify-contract: FAIL (the served contract differs from $CONTRACT_FILE at $short: $changed operations or schemas)" >&2
exit 1
