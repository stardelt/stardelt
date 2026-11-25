#!/usr/bin/env bash
# Create/refresh the ghcr.io image pull secret used by stardelt workloads.
#
# The Nova image lives at a private GHCR package; the node pulls it with this
# secret. The token is never written to the repository.
#
#   ./dev/ghcr-pull-secret.sh                 # uses `gh auth token`
#   GHCR_TOKEN=... ./dev/ghcr-pull-secret.sh  # preferred: a PAT with read:packages only
#
# The secret is stored in the cluster, readable by anything that can read Secrets in
# the namespace. `gh auth token` is your full GitHub login token (often with `repo`
# scope), so prefer a dedicated read:packages token via GHCR_TOKEN.
#
# Acts on kubectl's current context (override with KUBE_CONTEXT=...); it is printed
# before anything is applied.
set -euo pipefail

NS="${NS:-stardelt}"

command -v kubectl >/dev/null 2>&1 || { echo "kubectl not found" >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "python3 not found" >&2; exit 1; }
if [ -z "${GHCR_TOKEN:-}" ] || [ -z "${GHCR_USER:-}" ]; then
  command -v gh >/dev/null 2>&1 || { echo "gh not found (or set both GHCR_USER and GHCR_TOKEN)" >&2; exit 1; }
fi

user="${GHCR_USER:-$(gh api user --jq .login)}"
if [ -n "${GHCR_TOKEN:-}" ]; then
  token="$GHCR_TOKEN"
else
  echo "note: using your gh login token; set GHCR_TOKEN to a read:packages-only PAT instead" >&2
  token="$(gh auth token)"
fi

kube=(kubectl)
if [ -n "${KUBE_CONTEXT:-}" ]; then
  kube+=(--context "$KUBE_CONTEXT")
  ctx="$KUBE_CONTEXT"
else
  ctx="$(kubectl config current-context)"
fi
echo "› kube-context: $ctx"

# The token never reaches a command line: `kubectl create secret docker-registry
# --docker-password` would put it in the process's argv (`ps`, /proc/<pid>/cmdline). Build the
# docker config in python3 from its environment and let kubectl read the manifest on stdin; the
# apply stays server-side so no last-applied-configuration carries it either (#1663, #1682).
ghcr_manifest="$(GHCR_USER_VALUE="$user" GHCR_TOKEN_VALUE="$token" SECRET_NS="$NS" python3 - <<'PY'
import base64, json, os
user = os.environ.pop("GHCR_USER_VALUE")
token = os.environ.pop("GHCR_TOKEN_VALUE")
namespace = os.environ.pop("SECRET_NS")
config = json.dumps({"auths": {"ghcr.io": {
    "username": user,
    "password": token,
    "auth": base64.b64encode(f"{user}:{token}".encode()).decode(),
}}})
print(json.dumps({
    "apiVersion": "v1",
    "kind": "Secret",
    "metadata": {"name": "ghcr-creds", "namespace": namespace},
    "type": "kubernetes.io/dockerconfigjson",
    "data": {".dockerconfigjson": base64.b64encode(config.encode()).decode()},
}))
PY
)"
printf '%s' "$ghcr_manifest" | "${kube[@]}" apply --server-side --field-manager=stardelt-scripts -f -

# A Secret an older run created with a client-side apply still carries the token in that
# annotation: heal it on every run (#1663, the shape public-demo.sh uses since #1655).
"${kube[@]}" -n "$NS" annotate secret ghcr-creds \
  kubectl.kubernetes.io/last-applied-configuration- >/dev/null 2>&1 || true

echo "✓ ghcr-creds present in namespace $NS (user $user, context $ctx)"
