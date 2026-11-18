#!/usr/bin/env bash
# Fail when the operator's embedded access-sync job differs from stardelt-platform's
# scripts/access-sync-job.py — the operator runs the platform script verbatim (D44), and a
# platform fix that does not reach the copy is invisible until someone diffs by hand
# (platform#1539/#1549 was exactly that).
#
#   bash scripts/check-access-sync-script.sh
#   PLATFORM_JOB=../platform/scripts/access-sync-job.py bash scripts/check-access-sync-script.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

LOCAL=crates/stardelt-operator/src/access_sync_job.py
URL="https://raw.githubusercontent.com/stardelt/stardelt-platform/main/scripts/access-sync-job.py"

remote="$(mktemp)"
trap 'rm -f "$remote"' EXIT
if [[ -n "${PLATFORM_JOB:-}" ]]; then
  cp "$PLATFORM_JOB" "$remote"
else
  curl -fsSL --retry 3 "$URL" -o "$remote" || {
    echo "✗ cannot fetch $URL" >&2
    exit 1
  }
fi

local_sha="$(sha256sum "$LOCAL" | cut -d' ' -f1)"
remote_sha="$(sha256sum "$remote" | cut -d' ' -f1)"
if [[ "$local_sha" == "$remote_sha" ]]; then
  echo "✓ $LOCAL matches stardelt-platform main ($remote_sha)"
  exit 0
fi

echo "✗ $LOCAL is not stardelt-platform's script: local $local_sha, platform $remote_sha" >&2
diff -u "$remote" "$LOCAL" >&2 || true
echo "  copy the platform file over it (platform#1539/#1549 was this drift)" >&2
exit 1
