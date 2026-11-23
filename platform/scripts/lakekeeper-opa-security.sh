#!/usr/bin/env bash
# Give the Lakekeeper chart's OPA-bridge sidecar the two fields `restricted` needs (#1095).
#
# Chart 0.14.0 (and 0.15.0) hardcodes the sidecar's securityContext in
# deployment template: runAsUser/runAsGroup/readOnlyRootFilesystem, no
# allowPrivilegeEscalation and no capabilities, and exposes no value key for the two.
# A Helm post-renderer would be the other way; this install already applies manifests
# after the release (lakekeeper-bootstrap.yaml, lakekeeper-network-policy.yaml), so the
# patch lives here and every lakekeeper install calls it.
#
#   NAMESPACE=stardelt bash scripts/lakekeeper-opa-security.sh
#
# `restricted` is what `make verify-pod-security` and the release gate measure; a missing
# patch makes that row red with `container "opa" must set securityContext…`.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"

command -v kubectl >/dev/null 2>&1 || { echo "lakekeeper-opa-security: UNVERIFIED (kubectl not found)"; exit 3; }
command -v jq >/dev/null 2>&1 || { echo "lakekeeper-opa-security: UNVERIFIED (jq not found)"; exit 3; }

if ! kubectl -n "$NS" get deployment lakekeeper >/dev/null 2>&1; then
  echo "lakekeeper-opa-security: no lakekeeper Deployment in $NS — nothing to patch"
  exit 0
fi
i="$(kubectl -n "$NS" get deployment lakekeeper -o json \
  | jq -r '[.spec.template.spec.containers[].name] | index("opa") // "absent"')"
if [ "$i" = absent ]; then
  echo "lakekeeper-opa-security: no opa sidecar (allowall install) — nothing to patch"
  exit 0
fi
kubectl -n "$NS" patch deployment lakekeeper --type=json -p "[
  {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/$i/securityContext/allowPrivilegeEscalation\",\"value\":false},
  {\"op\":\"add\",\"path\":\"/spec/template/spec/containers/$i/securityContext/capabilities\",\"value\":{\"drop\":[\"ALL\"]}}
]" >/dev/null
kubectl -n "$NS" rollout status deployment/lakekeeper --timeout=180s
kubectl -n "$NS" get deployment lakekeeper -o json \
  | jq -c '.spec.template.spec.containers[] | select(.name=="opa") | .securityContext'
echo "lakekeeper-opa-security: PASS"
