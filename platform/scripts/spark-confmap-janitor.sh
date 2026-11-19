#!/usr/bin/env bash
# Delete leftover Spark Connect executor ConfigMaps (platform#1173, stardelt#215).
#
# Each Spark Connect session creates `spark-exec-<16 hex>-conf-map`. Spark's own
# cleanup is a collection delete, which the driver's Role intentionally lacks
# (platform#1121). This job deletes maps whose name matches that shape and which
# are older than JANITOR_MAX_AGE_SECONDS (default 3600) unless one of three
# liveness rules keeps them:
#
#   1. a pod references the map in a volume — a long-running StreamApp creates its
#      executor once and that executor cannot remount a map the janitor deleted
#      (platform#1585);
#   2. a live pod carries the map's `spark-app-selector` label — Spark labels its
#      executors and its scheduler-created driver pod with the app id, which makes
#      the map attributable;
#   3. a live pod running the pinned Spark image started at or before the map was
#      created (within 120 s of kubelet/API-server clock skew).
#
# Rule 3 is what keeps the shared Connect server's map (stardelt#215). It runs in
# client mode inside a plain Deployment pod: no `spark-app-selector` label, no owner
# reference, and its executors scale to zero, so rules 1 and 2 never hold for it and
# the map was deleted while every query hung (measured 2026-11-19: pod started
# 01:08:45Z, map created 01:09:02Z, before any client connected — so a Spark pod that
# predates the map is its driver).
#
# JANITOR_DRY_RUN=1 prints `would delete …` and deletes nothing.
#
# The ServiceAccount has `delete` by name only: `scripts/check-...` and the Role below
# forbid `deletecollection` on purpose, and `pods` is read-only.
set -euo pipefail

NAMESPACE="${NAMESPACE:-stardelt}"
MAX_AGE="${JANITOR_MAX_AGE_SECONDS:-3600}"
DRY_RUN="${JANITOR_DRY_RUN:-}"
NAME_RE='^spark-exec-[0-9a-f]{16}-conf-map$'

now=$(date -u +%s)
deleted=0

# RFC3339 (`2026-11-19T01:09:02Z`) to epoch seconds.
epoch_of() {
  local date_part="${1%%T*}"
  local time_part="${1#*T}"
  time_part="${time_part%%Z*}"
  date -u -d "$date_part $time_part" +%s
}

# Keep every map a pod mounts, every map of an app with a live pod, and every map a live
# driver could have created. If the list fails, delete nothing — a blind delete is what
# broke the pegelonline stream (platform#1585).
if ! pods="$(kubectl -n "$NAMESPACE" get pods -o jsonpath=\
'{range .items[*]}{.metadata.name}{"|"}{.metadata.deletionTimestamp}{"|"}{.status.startTime}{"|"}{.metadata.labels.spark-app-selector}{"|"}{range .spec.volumes[*]}{.configMap.name}{" "}{end}{"|"}{range .spec.containers[*]}{.image}{" "}{end}{"\n"}{end}')"; then
  echo "spark-confmap-janitor: cannot list pods; refusing to delete anything" >&2
  exit 1
fi

in_use=""
live_apps=""
driver_starts=""
while IFS='|' read -r _pod deleting started app_id mounted images; do
  [ -n "${_pod:-}" ] || continue
  # A terminating pod is not a live owner.
  [ -z "${deleting:-}" ] || continue
  for cm in ${mounted:-}; do
    in_use="${in_use}${cm}"$'\n'
  done
  [ -z "${app_id:-}" ] || live_apps="${live_apps}${app_id}"$'\n'
  [ -n "${started:-}" ] || continue
  case "${images:-}" in
    *ghcr.io/stardelt/spark*) driver_starts="${driver_starts}$(epoch_of "$started")"$'\n' ;;
  esac
done <<< "$pods"

while IFS='|' read -r name created app_id; do
  [ -n "${name:-}" ] || continue
  [ -n "${created:-}" ] || continue
  [[ "$name" =~ $NAME_RE ]] || continue
  if printf '%s' "$in_use" | grep -Fxq "$name"; then
    echo "keep   $name (referenced by a pod)"
    continue
  fi
  if [ -n "${app_id:-}" ] && printf '%s' "$live_apps" | grep -Fxq "$app_id"; then
    echo "keep   $name (app ${app_id} has a live pod)"
    continue
  fi
  epoch=$(epoch_of "$created")
  if [ -n "$driver_starts" ]; then
    while read -r started; do
      [ -n "${started:-}" ] || continue
      if [ "$started" -le "$(( epoch + 120 ))" ]; then
        echo "keep   $name (created by a live spark driver)"
        continue 2
      fi
    done <<< "$driver_starts"
  fi
  age=$(( now - epoch ))
  if [ "$age" -lt "$MAX_AGE" ]; then
    echo "keep   $name ${age}s"
    continue
  fi
  if [ -n "$DRY_RUN" ]; then
    echo "would delete $name ${age}s"
  else
    kubectl -n "$NAMESPACE" delete configmap "$name" --ignore-not-found >/dev/null
    echo "delete $name ${age}s"
  fi
  deleted=$(( deleted + 1 ))
done < <(kubectl -n "$NAMESPACE" get configmap \
  -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.metadata.creationTimestamp}{"|"}{.metadata.labels.spark-app-selector}{"\n"}{end}')

if [ -n "$DRY_RUN" ]; then
  echo "spark-confmap-janitor: would delete ${deleted} ConfigMap(s); kept everything younger than ${MAX_AGE}s"
else
  echo "spark-confmap-janitor: deleted ${deleted} ConfigMap(s); kept everything younger than ${MAX_AGE}s"
fi
