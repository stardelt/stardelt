#!/usr/bin/env bash
# Prove that the ConfigMap the Spark Connect driver reads its executor image from
# (spark-connect-config) carries the reference manifests/spark.yaml pins.
#
# Why: the executor is not a workload this repository applies. The driver copies
# spark.kubernetes.container.image out of spark-connect-config when it creates an executor pod, so
# nothing the cluster runs compares that line with the pin: verify-pins.sh reads the Deployment
# spark-connect, and its `spark` row is green while the executors are not (2026-11-19: the driver
# ran sha256:70f9c9f2…, the ConfigMap carried sha256:be8f2010…, a reference no file of origin/main
# named, and `make verify-pins` printed PASS 44 of 44). `make install-spark` writes the ConfigMap
# from manifests/spark.yaml, but scripts/pin-image.sh and the pin rollup move the Deployment line
# only: a roll that does not re-apply spark.yaml leaves the executors behind. Driver and executors
# must run the same bytes — the image carries the Iceberg runtime, the AWS bundle and the Kafka
# connector a StreamApp reads with — so the two are compared by digest, not by tag.
#
#   NS=stardelt bash scripts/verify-spark-config.sh
#
# Read-only. Exit 0 = PASS, 1 = FAIL, 3 = UNVERIFIED (could not see the subject).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NS="${NS:-${NAMESPACE:-stardelt}}"

command -v kubectl >/dev/null 2>&1 || { echo "verify-spark-config: kubectl not found" >&2; exit 3; }
command -v grep >/dev/null 2>&1 || { echo "UNVERIFIED (grep not found)" >&2; exit 3; }

if ! kubectl get namespace "$NS" >/dev/null 2>&1; then
  echo "UNVERIFIED (namespace $NS does not exist)"
  exit 3
fi

# ---------------------------------------------------------------------------
# 1. The pin, read from the file `make install-spark` applies.
# ---------------------------------------------------------------------------
pin="$(sed -nE 's/^[[:space:]]*spark\.kubernetes\.container\.image[[:space:]]+(ghcr\.io\/stardelt\/spark[^[:space:]]+)[[:space:]]*$/\1/p' manifests/spark.yaml)"
if [ -z "$pin" ]; then
  echo "UNVERIFIED (no spark.kubernetes.container.image in manifests/spark.yaml)"
  exit 3
fi

# ---------------------------------------------------------------------------
# 2. Not installed is a pass: a cluster without the Compute tier has no Connect
#    server to read a stale executor image from (install-core).
# ---------------------------------------------------------------------------
if ! kubectl -n "$NS" get configmap spark-connect-config >/dev/null 2>&1; then
  echo "– spark-connect-config: not installed (no Compute tier)"
  echo "PASS (spark is not installed)"
  exit 0
fi

live="$(kubectl -n "$NS" get configmap spark-connect-config -o jsonpath='{.data.spark-defaults\.conf}' \
  2>/dev/null | sed -nE 's/^[[:space:]]*spark\.kubernetes\.container\.image[[:space:]]+([^[:space:]]+)[[:space:]]*$/\1/p')"
if [ -z "$live" ]; then
  echo "✗ spark-connect-config carries no executor image — every executor pod falls back to Spark's default image"
  echo "FAIL (spark-connect-config has no spark.kubernetes.container.image — apply manifests/spark.yaml: make install-spark)"
  exit 1
fi

# ---------------------------------------------------------------------------
# 3. The digests, not the whole reference: a tag that moved under the same digest
#    is not a finding.
# ---------------------------------------------------------------------------
pin_digest="${pin##*@}"
live_digest="${live##*@}"

if [ "$pin_digest" = "$live_digest" ]; then
  echo "✓ spark-connect-config executor image ${live_digest:0:19}… (the pin)"
  echo "PASS (the executor image is the pin)"
  exit 0
fi

echo "✗ spark-connect-config executor image ${live_digest:0:19}… (the pin is ${pin_digest:0:19}…)"
echo "FAIL (the executors run ${live_digest:0:19}…, the pin is ${pin_digest:0:19}… — apply manifests/spark.yaml: make install-spark)"
exit 1
