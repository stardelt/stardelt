#!/usr/bin/env bash
# Delete leftover Spark Connect executor ConfigMaps (platform#1173).
#
# Each Spark Connect session creates `spark-exec-<16 hex>-conf-map`. Spark's own
# cleanup is a collection delete, which the driver's Role intentionally lacks
# (platform#1121). This job deletes maps whose name matches that shape and which
# are older than JANITOR_MAX_AGE_SECONDS (default 3600) only when no pod mounts
# them; a map any pod references is kept whatever its age, because a long-running
# StreamApp creates its executor once and that executor cannot remount a map the
# janitor deleted (platform#1585). The ServiceAccount has `delete` by name only:
# `scripts/check-...` and the Role below forbid `deletecollection` on purpose,
# and `pods` is read-only.
set -euo pipefail

NAMESPACE="${NAMESPACE:-stardelt}"
MAX_AGE="${JANITOR_MAX_AGE_SECONDS:-3600}"
NAME_RE='^spark-exec-[0-9a-f]{16}-conf-map$'

now=$(date -u +%s)
deleted=0

# Keep every map a pod mounts: a long-running StreamApp's executor is created once and its
# ConfigMap is older than MAX_AGE while the app still uses it. If the list fails, delete
# nothing — a blind delete is what broke the pegelonline stream (platform#1585).
if ! in_use="$(kubectl -n "$NAMESPACE" get pods -o jsonpath=\
'{range .items[*]}{range .spec.volumes[*]}{.configMap.name}{"\n"}{end}{end}')"; then
  echo "spark-confmap-janitor: cannot list pods; refusing to delete anything" >&2
  exit 1
fi

while read -r name created; do
  [ -n "${name:-}" ] || continue
  [ -n "${created:-}" ] || continue
  [[ "$name" =~ $NAME_RE ]] || continue
  if printf '%s\n' "$in_use" | grep -Fxq "$name"; then
    echo "keep   $name (referenced by a pod)"
    continue
  fi
  date_part="${created%%T*}"
  time_part="${created#*T}"
  time_part="${time_part%%Z*}"
  epoch=$(date -u -d "$date_part $time_part" +%s)
  age=$(( now - epoch ))
  if [ "$age" -lt "$MAX_AGE" ]; then
    echo "keep   $name ${age}s"
    continue
  fi
  kubectl -n "$NAMESPACE" delete configmap "$name" --ignore-not-found >/dev/null
  echo "delete $name ${age}s"
  deleted=$(( deleted + 1 ))
done < <(kubectl -n "$NAMESPACE" get configmap \
  -o jsonpath='{range .items[*]}{.metadata.name} {.metadata.creationTimestamp}{"\n"}{end}')

echo "spark-confmap-janitor: deleted ${deleted} ConfigMap(s); kept everything younger than ${MAX_AGE}s"
