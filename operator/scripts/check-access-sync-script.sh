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
URL="https://raw.githubusercontent.com/stardelt/stardelt/main/platform/scripts/access-sync-job.py"
# In this repository the platform is the sibling folder: read it there unless the caller chose a file.
_sibling="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/platform/scripts/access-sync-job.py"
if [[ -z "${PLATFORM_JOB:-}" && -f "$_sibling" ]]; then PLATFORM_JOB="$_sibling"; fi

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
  echo "✓ $LOCAL matches platform/scripts/access-sync-job.py in stardelt/stardelt main ($remote_sha)"
  exit 0
fi

echo "✗ $LOCAL is not the platform's access-sync script: local $local_sha, platform $remote_sha" >&2
diff -u "$remote" "$LOCAL" >&2 || true
echo "  copy the platform file over it (platform#1539/#1549 was this drift)" >&2
exit 1
