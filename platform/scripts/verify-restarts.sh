#!/usr/bin/env bash
# Print the restartCount nobody reads (stardelt-platform#1700, option A of #1597).
#
# Why: every pod in the lab reads `Running 1/1` while the restart counts climbed
# unobserved (user-scheduler 9, spark-operator 13, operator 2 and, measured 2026-11-17,
# CloudNativePG 6, the barman plugin 5, Lakekeeper 2).
# verify-pins reads the image, verify-pod-security the security context, and nothing read
# `status.containerStatuses[].restartCount`; platform#554 is the reason the numbers are not
# zero. Same shape as platform#1458: healthy most of the time, and the one number that says
# otherwise is never printed.
#
#   scripts/verify-restarts.sh
#   ROWS=nova,user-scheduler scripts/verify-restarts.sh   # only these rows (the test uses it)
#
# Read-only: one `kubectl get pods -o json`, nothing applied, patched or deleted. The restart
# count is the evidence, never fatal: a Deployment fails only when one of its pods is not
# Ready, and a listed Deployment with no pod is the honest `–` row (hub and proxy are not
# installed in every install).
#
# Pods are matched by owner, not by name prefix: `nova-` also matches the StatefulSet pod
# `nova-pg-1`, so a pod counts only when `ownerReferences[0].kind == "ReplicaSet"` and the
# ReplicaSet name starts with `<deployment>-`.
#
# Exit codes: 0 every listed Deployment has all pods Ready (restarts do not fail), 1 at least
# one Deployment has a pod that is not Ready, 3 UNVERIFIED (no kubectl, or no listed
# Deployment has a pod in the namespace — the check proved nothing).
# Needs: kubectl, python3.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"

command -v kubectl >/dev/null || { echo "verify-restarts: UNVERIFIED (kubectl not found)"; exit 3; }
command -v python3 >/dev/null || { echo "verify-restarts: UNVERIFIED (python3 not found)"; exit 3; }

rows=(
  stardelt-operator
  nova
  user-scheduler
  hub
  proxy
  spark-operator-controller
  spark-operator-webhook
  strimzi-cluster-operator
  cnpg-cloudnative-pg
  barman-cloud-plugin-barman-cloud
  lakekeeper
)

selected=""
for row in "${rows[@]}"; do
  if [ -n "${ROWS:-}" ] && [[ ",$ROWS," != *",$row,"* ]]; then continue; fi
  selected="${selected:+$selected,}$row"
done
if [ -z "$selected" ]; then
  echo "verify-restarts: UNVERIFIED (ROWS=\"${ROWS:-}\" names none of: ${rows[*]})"
  exit 3
fi

if ! pods_json="$(kubectl -n "$NS" get pods -o json 2>&1)"; then
  echo "verify-restarts: UNVERIFIED (cannot list pods in $NS: $pods_json)"
  exit 3
fi

if ! report="$(printf '%s' "$pods_json" | RESTART_ROWS="$selected" python3 /dev/fd/3 3<<'PY'
import json, os, sys

raw = sys.stdin.read()
try:
    pods = (json.loads(raw or "{}").get("items") or [])
except ValueError:
    print("__error__ cannot parse the pod list")
    sys.exit(0)

rows = [r for r in (os.environ.get("RESTART_ROWS") or "").split(",") if r]

owned = {}
for pod in pods:
    meta = pod.get("metadata") or {}
    refs = meta.get("ownerReferences") or []
    if not refs:
        continue
    first = refs[0] or {}
    if first.get("kind") != "ReplicaSet":
        continue
    owner = first.get("name") or ""
    owned.setdefault(owner, []).append(pod)


def pods_of(name):
    found = []
    for owner, items in owned.items():
        if owner == name or owner.startswith(name + "-"):
            found.extend(items)
    return found


def is_ready(pod):
    for cond in (pod.get("status") or {}).get("conditions") or []:
        if cond.get("type") == "Ready":
            return cond.get("status") == "True"
    return False


width = max(len(name) for name in rows) + 2


def row(mark, name, rest):
    return "%s %-*s%s" % (mark, width, name, rest)


lines = []
not_ready = 0
found = 0
for name in rows:
    pods = pods_of(name)
    if not pods:
        lines.append(row("–", name, "not installed"))
        continue
    found += 1
    ready = sum(1 for p in pods if is_ready(p))
    restarts = 0
    newest = None
    for pod in pods:
        for status in (pod.get("status") or {}).get("containerStatuses") or []:
            restarts += status.get("restartCount") or 0
            term = ((status.get("lastState") or {}).get("terminated") or {})
            finished = term.get("finishedAt")
            if finished and (newest is None or finished > newest[2]):
                newest = (term.get("reason") or "?", term.get("exitCode"), finished)
    if ready < len(pods):
        lines.append(row("✗", name, "%d of %d pod(s) ready" % (ready, len(pods))))
        not_ready += 1
        continue
    text = "%d pod(s) ready, %d restart(s) in total" % (ready, restarts)
    mark = "!" if restarts > 0 else "✓"
    if newest is not None:
        reason, code, finished = newest
        text += "; last exit %s exit=%s at %s — see stardelt-platform#554" % (
            reason,
            "?" if code is None else code,
            finished,
        )
    lines.append(row(mark, name, text))

for line in lines:
    print(line)
print("__meta__ %d|%d|%d" % (not_ready, found, len(rows)))
PY
)"; then
  echo "verify-restarts: UNVERIFIED (cannot evaluate the pod list: $report)"
  exit 3
fi

if printf '%s\n' "$report" | grep -q '^__error__ '; then
  echo "verify-restarts: UNVERIFIED ($(printf '%s\n' "$report" | sed -n 's/^__error__ //p'))"
  exit 3
fi

meta="$(printf '%s\n' "$report" | grep '^__meta__ ' || true)"
printf '%s\n' "$report" | grep -v '^__meta__ ' || true
if [ -z "$meta" ]; then
  echo "verify-restarts: UNVERIFIED (cannot evaluate the pod list)"
  exit 3
fi
IFS='|' read -r not_ready found total <<< "${meta#__meta__ }"

if [ "$found" -eq 0 ]; then
  echo "verify-restarts: UNVERIFIED (no listed Deployment has a pod in $NS)"
  exit 3
fi

if [ "$not_ready" -gt 0 ]; then
  echo "verify-restarts: FAIL ($not_ready Deployment(s) with a not-Ready pod)"
  exit 1
fi

if [ "$found" -eq "$total" ]; then
  echo "verify-restarts: PASS ($total Deployment(s); the restart count is printed, never fatal)"
else
  echo "verify-restarts: PASS ($found of $total Deployment(s) have pods; the restart count is printed, never fatal)"
fi
