#!/usr/bin/env bash
# Fail when the maintenance CLI image (maintenance.rs IMAGE) is not the build ghcr.io serves
# under the :dev tag, or when that build names no revision. The constant is the only place a
# stardelt-cli digest is pinned; no other check in either repository sees it (operator#806).
#
#   bash scripts/check-cli-image.sh     # 0 equal, 1 differ or unreadable
#
# Needs: bash, curl, python3, network access to ghcr.io (the package is public).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

LOCAL_FILE=crates/stardelt-operator/src/maintenance.rs
REPO=stardelt/stardelt-cli
REF=dev
IMAGE="ghcr.io/$REPO:$REF"
REGISTRY=https://ghcr.io/v2/$REPO

die() { echo "$@" >&2; exit 1; }

local_digest="$(sed -n 's|^pub const IMAGE: &str = "ghcr\.io/stardelt/stardelt-cli:dev@sha256:\([0-9a-f]\{64\}\)";$|\1|p' "$LOCAL_FILE")"
[[ "$local_digest" =~ ^[0-9a-f]{64}$ ]] || die "✗ no IMAGE digest in $LOCAL_FILE"

token="$(curl -fsSL "https://ghcr.io/token?scope=repository:$REPO:pull&service=ghcr.io" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')" \
  || die "✗ cannot resolve $IMAGE"

live_digest="$(curl -fsSI -H "Authorization: Bearer $token" \
  -H 'Accept: application/vnd.oci.image.index.v1+json' \
  "$REGISTRY/manifests/$REF" \
  | tr -d '\r' | sed -n 's/^[Dd]ocker-[Cc]ontent-[Dd]igest: sha256://p' | head -n 1)" \
  || die "✗ cannot resolve $IMAGE"
[[ "$live_digest" =~ ^[0-9a-f]{64}$ ]] || die "✗ cannot resolve $IMAGE"

index="$(curl -fsSL -H "Authorization: Bearer $token" \
  -H 'Accept: application/vnd.oci.image.index.v1+json' \
  "$REGISTRY/manifests/sha256:$live_digest")" \
  || die "✗ cannot resolve $IMAGE"

amd64_digest="$(printf '%s' "$index" | python3 -c '
import json, sys
for manifest in json.load(sys.stdin).get("manifests") or []:
    platform = manifest.get("platform") or {}
    if platform.get("architecture") == "amd64" and platform.get("os") == "linux":
        print(manifest["digest"])
        break
')" || die "✗ cannot resolve $IMAGE"
[[ "$amd64_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || die "✗ cannot resolve $IMAGE"

config_digest="$(curl -fsSL -H "Authorization: Bearer $token" \
  -H 'Accept: application/vnd.oci.image.manifest.v1+json' \
  "$REGISTRY/manifests/$amd64_digest" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["config"]["digest"])')" \
  || die "✗ cannot resolve $IMAGE"
[[ "$config_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || die "✗ cannot resolve $IMAGE"

revision="$(curl -fsSL -H "Authorization: Bearer $token" "$REGISTRY/blobs/$config_digest" \
  | python3 -c '
import json, sys
labels = (json.load(sys.stdin).get("config") or {}).get("Labels") or {}
print(labels.get("org.opencontainers.image.revision", ""))
')" || die "✗ cannot resolve $IMAGE"

if [[ -z "$revision" ]]; then
  die "✗ the published image names no revision — the label fix (stardelt-python#140) is not in this build"
fi

if [[ "$local_digest" == "$live_digest" ]]; then
  echo "✓ $LOCAL_FILE names $IMAGE@sha256:$local_digest (revision $revision)"
  exit 0
fi

echo "✗ $LOCAL_FILE names $IMAGE@sha256:$local_digest," >&2
echo "  $REF resolves to $IMAGE@sha256:$live_digest (revision $revision)" >&2
echo "  update IMAGE in $LOCAL_FILE" >&2
exit 1
