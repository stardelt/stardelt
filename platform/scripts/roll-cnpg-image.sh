#!/usr/bin/env bash
# Roll a CNPG Cluster's instance pod when its `spec.imageName` carries a digest the running pod
# does not, and prove the Cluster converges.
#
# Why this exists: applying an `imageName` that adds a digest to a single-instance CNPG Cluster
# does not restart the instance. The instance manager logs "Detected drift between the bootstrap
# images and the configuration. Skipping configuration reload" and refuses to start Postgres, so
# the pod stays Terminating for its 1800 s grace period and the database is down (platform#1619,
# measured on all four Clusters while rolling out #1612). A normal `kubectl delete pod` hangs on
# that shutdown; a force delete completes the rollout in ~45 s.
#
#   scripts/roll-cnpg-image.sh                    # every Cluster this repository installs
#   scripts/roll-cnpg-image.sh keycloak-pg        # one
#   CLUSTERS="nova-pg openfga-pg" scripts/roll-cnpg-image.sh
#   NAMESPACE=lab scripts/roll-cnpg-image.sh
#
# It applies nothing: run the manifest apply(s) first, then this. A Cluster whose pod already runs
# the pinned image is left alone, so the command is safe to run on a healthy install.
#
# Exit codes: 0 every Cluster runs its pinned image, 1 at least one did not converge,
# 3 the check proved nothing (no kubectl, no Cluster, no primary pod). Needs: kubectl.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
NAMESPACE="${NAMESPACE:-stardelt}"
CLUSTERS="${CLUSTERS:-${*:-lakekeeper-pg keycloak-pg nova-pg openfga-pg}}"
TIMEOUT="${TIMEOUT:-300}"
POLL_SECONDS="${POLL_SECONDS:-5}"

command -v kubectl >/dev/null || { echo "roll-cnpg-image: UNVERIFIED (kubectl not found)" >&2; exit 3; }

cluster_field() { # cluster, jsonpath
  kubectl -n "$NAMESPACE" get clusters.postgresql.cnpg.io "$1" -o jsonpath="$2" 2>/dev/null || true
}
primary_pod() {
  kubectl -n "$NAMESPACE" get pods \
    -l "cnpg.io/cluster=$1,cnpg.io/instanceRole=primary" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}
pod_image() {
  kubectl -n "$NAMESPACE" get pod "$1" \
    -o jsonpath='{.spec.containers[?(@.name=="postgres")].image}' 2>/dev/null || true
}
short() { printf '%s' "${1##*@}"; }

status=0
for name in $CLUSTERS; do
  want="$(cluster_field "$name" '{.spec.imageName}')"
  if [ -z "$want" ]; then
    echo "? $name no Cluster $NAMESPACE/$name"
    status=3
    continue
  fi
  pod="$(primary_pod "$name")"
  if [ -z "$pod" ]; then
    echo "? $name no primary pod"
    status=3
    continue
  fi
  live="$(pod_image "$pod")"
  if [ "$live" = "$want" ]; then
    printf '✓ %-14s %s\n' "$name" "$(short "$want")"
    continue
  fi
  printf '› %-14s %s runs %s; the Cluster names %s — force deleting the primary\n' \
    "$name" "$pod" "$(short "$live")" "$(short "$want")"
  kubectl -n "$NAMESPACE" delete pod "$pod" --grace-period=0 --force
  rolled=0
  for _ in $(seq 1 $((TIMEOUT / (POLL_SECONDS > 0 ? POLL_SECONDS : 1)))); do
    [ "$POLL_SECONDS" -gt 0 ] && sleep "$POLL_SECONDS"
    pod="$(primary_pod "$name")"
    live="$(pod_image "$pod")"
    if [ "$(cluster_field "$name" '{.status.conditions[?(@.type=="Ready")].status}')" = True ] \
      && [ "$live" = "$want" ]; then
      printf '✓ %-14s %s Ready\n' "$name" "$(short "$want")"
      rolled=1
      break
    fi
  done
  if [ "$rolled" -ne 1 ]; then
    printf '✗ %-14s not Ready on %s within %ss (pod %s, image %s)\n' \
      "$name" "$(short "$want")" "$TIMEOUT" "$pod" "$(short "$live")"
    status=1
  fi
done

case "$status" in
  0) echo "roll-cnpg-image: PASS (every Cluster runs its pinned Postgres image)" ;;
  1) echo "roll-cnpg-image: FAIL (a Cluster did not converge)" >&2 ;;
  *) echo "roll-cnpg-image: UNVERIFIED (a Cluster or its primary pod could not be read)" >&2 ;;
esac
exit "$status"
