#!/usr/bin/env bash
# Fail when the Spark image a `spark-pipeline` task pod is told to run (PIPELINE_SPARK_IMAGE in
# helm-values/airflow.yaml) differs from the platform's Spark image reference, the runtime catalogue
# `chart/files/spark-runtimes.json`. The pod is spawned
# by a KubernetesPodOperator, so `make pin-images` cannot see it; a stale digest is a client
# whose spark-pipelines version is not the server's (#80 S2c-2).
#
#   bash scripts/check-spark-image-pin.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

image() { # image <file> <pattern>: the pattern matches the line that carries the image itself
  awk -v pattern="$2" '$0 ~ pattern { print; exit }' "$1" \
    | grep -oE 'ghcr[.]io/stardelt/spark:[^"[:space:]]+@sha256:[0-9a-f]{64}' || true
}

catalogue="$(image chart/files/spark-runtimes.json '"image": "ghcr[.]io/stardelt/spark:')"
task="$(image helm-values/airflow.yaml 'value: ghcr[.]io/stardelt/spark:')"

for pin in "$catalogue" "$task"; do
  if [[ ! "$pin" =~ @sha256:[0-9a-f]{64}$ ]]; then
    echo "check-spark-image-pin: no full sha256 digest (catalogue='${catalogue}' airflow='${task}')" >&2
    exit 1
  fi
done
if [ "$catalogue" != "$task" ]; then
  echo "check-spark-image-pin: the runtime catalogue pins $catalogue" >&2
  echo "check-spark-image-pin: but spark-pipeline task pods are pinned to $task" >&2
  exit 1
fi
echo "check-spark-image-pin: $catalogue"
