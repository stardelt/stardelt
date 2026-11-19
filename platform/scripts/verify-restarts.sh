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
# Read-only: one `kubectl get pods -o json`, nothing applied, patched or deleted. A listed
# Deployment with no pod is the honest `–` row (hub and proxy are not installed in every
# install).
#
# Two classes of restart, and only one of them fails (platform#233):
#  - a node-level restart is evidence, never a failure: `Unknown` (exit 255) is the node
#    closing every pod in the same second (the disk stall of platform#554), `Completed`
#    (exit 0) is a normal end. The row keeps `!` and the newest exit is printed.
#  - a real crash inside the window is a failure: a container whose newest exit is anything
#    the node did not cause (`Error`, `OOMKilled`, `ContainerCannotRun`, …) and that finished
#    within RESTART_MAX_AGE_SECONDS (default 3600) makes the row `✗` and the check exit 1.
#    A crash older than the window is still printed on the row but does not fail — the
#    incident's history stays visible.
#  - the node's own stall produces two exit shapes: `Unknown` (exit 255) on pods the runtime lost and
#    `Error` (exit 1) on components that held a Lease or a leader election ("leader election lost").
#    An `Error` exit within RESTART_NODE_STALL_WINDOW (default 120) seconds of an `Unknown`/exit=255
#    exit in the same namespace is the node's, not the component's: the row keeps `!` and says so
#    (stardelt/stardelt#60, measured 2026-11-19T02:39:54Z).
#  - a kube-apiserver stall that only costs leader leases has no `Unknown` exit at all: it makes
#    several Deployments exit 1 within the same window ("failed to renew lease", "Leaderelection
#    lost"). An `Error` exit within RESTART_NODE_STALL_WINDOW seconds of another listed
#    Deployment's `Error` exit is that cluster stall too: the row keeps `!` and names it
#    (measured 2026-11-19T07:01:09-14Z). A single Deployment's `Error` exit with no second row in
#    the window still fails; `OOMKilled` and `ContainerCannotRun` never count as lease losses.
#
# Pods are matched by owner, not by name prefix: `nova-` also matches the StatefulSet pod
# `nova-pg-1`, so a pod counts only when `ownerReferences[0].kind == "ReplicaSet"` and the
# ReplicaSet name starts with `<deployment>-`.
# A ReplicaSet belongs to the longest row that matches it: `spark-connect-auth-…` is the front's row,
# not the driver's, even though `spark-connect` is its prefix.
# spark-connect and spark-connect-auth are rows too: a driver restart drops every live session
# (stardelt/stardelt#215), and the front's crash-loop is what an unrenderable envoy.yaml looks like.
#
# Exit codes: 0 every listed Deployment has all pods Ready and no container crashed inside the
# window, 1 at least one Deployment has a pod that is not Ready or a container crashed inside
# the window, 3 UNVERIFIED (no kubectl, or no listed Deployment has a pod in the namespace —
# the check proved nothing).
# Needs: kubectl, python3.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
MAX_AGE="${RESTART_MAX_AGE_SECONDS:-3600}"

command -v kubectl >/dev/null || { echo "verify-restarts: UNVERIFIED (kubectl not found)"; exit 3; }
command -v python3 >/dev/null || { echo "verify-restarts: UNVERIFIED (python3 not found)"; exit 3; }

rows=(
  stardelt-operator
  nova
  user-scheduler
  hub
  proxy
  spark-connect
  spark-connect-auth
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

if ! report="$(printf '%s' "$pods_json" | RESTART_ROWS="$selected" RESTART_ALL_ROWS="${rows[*]}" RESTART_NS="$NS" RESTART_MAX_AGE="$MAX_AGE" python3 /dev/fd/3 3<<'PY'
import calendar, json, os, sys, time

raw = sys.stdin.read()
try:
    pods = (json.loads(raw or "{}").get("items") or [])
except ValueError:
    print("__error__ cannot parse the pod list")
    sys.exit(0)

rows = [r for r in (os.environ.get("RESTART_ROWS") or "").split(",") if r]
all_rows = [r for r in (os.environ.get("RESTART_ALL_ROWS") or "").split() if r] or rows

MAX_AGE = int(os.environ.get("RESTART_MAX_AGE") or "3600")
NS_NAME = os.environ.get("RESTART_NS") or "stardelt"
NODE_STALL_WINDOW = int(os.environ.get("RESTART_NODE_STALL_WINDOW") or "120")

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


def row_of(owner):
    """The longest row that owns this ReplicaSet: `spark-connect-auth-…` is the front's, not the driver's."""
    best = ""
    for candidate in all_rows:
        if (owner == candidate or owner.startswith(candidate + "-")) and len(candidate) > len(best):
            best = candidate
    return best


def pods_of(name):
    return [pod for owner, items in owned.items() if row_of(owner) == name for pod in items]


def is_ready(pod):
    for cond in (pod.get("status") or {}).get("conditions") or []:
        if cond.get("type") == "Ready":
            return cond.get("status") == "True"
    return False


def _epoch(ts):
    """RFC3339 (`2026-11-19T01:06:45Z`, kubelet sometimes adds a fraction) to epoch seconds."""
    if not ts or not ts.endswith("Z"):
        return None
    stamp = ts[:-1]                      # drop the Z …
    if "." in stamp:
        stamp = stamp.split(".")[0]      # … and kubelet's optional fraction
    try:
        return calendar.timegm(time.strptime(stamp, "%Y-%m-%dT%H:%M:%S"))
    except ValueError:
        return None


def node_stall_stamps(pods):
    """Every `Unknown`/exit=255 termination in the namespace — the node stall's signature."""
    stamps = []
    for pod in pods:
        for status in (pod.get("status") or {}).get("containerStatuses") or []:
            term = ((status.get("lastState") or {}).get("terminated") or {})
            if not term.get("finishedAt"):
                continue
            if term.get("reason") == "Unknown" or term.get("exitCode") == 255:
                stamp = _epoch(term["finishedAt"])
                if stamp is not None:
                    stamps.append(stamp)
    return stamps


def cluster_stall_stamps(pods):
    """`Error` exits of two or more different rows inside one window: the API server, not the components.

    Measured 2026-11-19T07:01:09-14Z on the lab: user-scheduler, spark-operator-controller and
    -webhook, CloudNativePG and the barman plugin each exited 1 after "failed to renew lease
    … context deadline exceeded" / "Leaderelection lost", and no pod in the namespace carried an
    Unknown/exit=255 exit, so `node_stall_stamps` was empty.
    """
    exits = {}
    for name in rows:
        for pod in pods_of(name):
            for status in (pod.get("status") or {}).get("containerStatuses") or []:
                term = ((status.get("lastState") or {}).get("terminated") or {})
                if term.get("reason") != "Error" or not term.get("finishedAt"):
                    continue
                stamp = _epoch(term["finishedAt"])
                if stamp is not None:
                    exits.setdefault(name, []).append(stamp)
    stamps = []
    for name, own in exits.items():
        for stamp in own:
            if any(
                abs(stamp - other) <= NODE_STALL_WINDOW
                for other_name, others in exits.items()
                if other_name != name
                for other in others
            ):
                stamps.append(stamp)
    return stamps


def node_caused(term, stalls):
    """`Error` inside a node stall's window is the node, not the component (stardelt/stardelt#60)."""
    if term[0] != "Error":
        return False
    stamp = _epoch(term[2])
    return stamp is not None and any(abs(stamp - s) <= NODE_STALL_WINDOW for s in stalls)


def crash_of(pod):
    """The newest exit the node did not cause, as (reason, exitCode, finishedAt), or None.

    `Unknown` (exit 255) is the node closing a pod — the disk stall of stardelt-platform#554 —
    and `Completed` (exit 0) is a normal end. Neither is a crash. Everything else (Error,
    OOMKilled, ContainerCannotRun) is one, and the caller decides by its age.
    """
    newest = None
    for status in (pod.get("status") or {}).get("containerStatuses") or []:
        term = ((status.get("lastState") or {}).get("terminated") or {})
        finished = term.get("finishedAt")
        reason = term.get("reason") or "?"
        if not finished or reason in ("Completed", "Unknown"):
            continue
        if newest is None or finished > newest[2]:
            newest = (reason, term.get("exitCode"), finished)
    return newest


width = max(len(name) for name in rows) + 2


def row(mark, name, rest):
    return "%s %-*s%s" % (mark, width, name, rest)


lines = []
not_ready = 0
crashed = 0
found = 0
now = int(time.time())
stalls = node_stall_stamps(pods)
cluster = cluster_stall_stamps(pods)
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
    crashes = []
    node_rows = []
    for pod in pods:
        term = crash_of(pod)
        if term is None:
            continue
        finished_epoch = _epoch(term[2])
        if finished_epoch is None or finished_epoch < now - MAX_AGE:
            continue  # unparseable or older than the window: printed with the row, never fatal
        if node_caused(term, stalls + cluster):
            node_rows.append((pod, term))
            continue
        crashes.append((pod, term))
    if crashes:
        pod, (reason, code, finished) = crashes[0]
        text += "; %d crash(es) within %ds: %s exit=%s at %s — read kubectl -n %s logs %s --previous" % (
            len(crashes), MAX_AGE, reason, "?" if code is None else code, finished, NS_NAME, pod["metadata"]["name"])
        crashed += 1
        lines.append(row("✗", name, text))
        continue
    if node_rows:
        pod, (reason, code, finished) = node_rows[0]
        stamp = _epoch(finished)
        mark = "!"
        nearest = min((abs(stamp - s) for s in stalls), default=None)
        if nearest is not None and nearest <= NODE_STALL_WINDOW:
            text += "; %d node-caused exit(s) within %ds: %s exit=%s at %s (%ds of an unknown exit=255) — see stardelt/stardelt#60" % (
                len(node_rows), MAX_AGE, reason, "?" if code is None else code, finished, nearest)
        else:
            text += "; %d cluster-wide exit(s) within %ds: %s exit=%s at %s (controllers lost their leases in one window; kube-apiserver) — see stardelt/stardelt#60" % (
                len(node_rows), MAX_AGE, reason, "?" if code is None else code, finished)
    lines.append(row(mark, name, text))

for line in lines:
    print(line)
print("__meta__ %d|%d|%d|%d" % (not_ready, crashed, found, len(rows)))
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
IFS='|' read -r not_ready crashed found total <<< "${meta#__meta__ }"

if [ "$found" -eq 0 ]; then
  echo "verify-restarts: UNVERIFIED (no listed Deployment has a pod in $NS)"
  exit 3
fi

if [ "$not_ready" -gt 0 ]; then
  echo "verify-restarts: FAIL ($not_ready Deployment(s) with a not-Ready pod)"
  exit 1
fi

if [ "$crashed" -gt 0 ]; then
  echo "verify-restarts: FAIL ($crashed Deployment(s) had a container crash within ${MAX_AGE}s)"
  exit 1
fi

if [ "$found" -eq "$total" ]; then
  echo "verify-restarts: PASS ($total Deployment(s); the restart count is printed, never fatal)"
else
  echo "verify-restarts: PASS ($found of $total Deployment(s) have pods; the restart count is printed, never fatal)"
fi
