#!/usr/bin/env bash
# A worker finishes what it is running before it exits (platform#131 A1, platform#1205).
#
#   NS=stardelt bash scripts/verify-trino-graceful-shutdown.sh
#
# The Trino chart's own connection test cannot run on this install: it mounts Secret
# <namespace>-certificates (this install's TLS material is trino-coordinator-tls) and signs
# in as admin/admin123 (Secret trino-password-db carries `superset` only), so its weight-2
# graceful-shutdown hook never runs. This is that hook's proof, run by the platform: it
# deletes one worker pod, reads the worker's own log for `Shutdown requested` (the line Trino
# writes only after the preStop drain was accepted) and for the second shutdown phase, times the
# container from the pod's `deletionTimestamp` to its own termination, and waits for a
# replacement to become Ready.
#
# platform#1205: Trino 484 shuts a worker down in two phases of `shutdown.grace-period` plus
# the task time — wait one grace period for the coordinator to notice the shutdown, wait for
# the tasks, then wait the same grace again for the task states to be observed (~240-245 s at
# the 120 s grace). The pod's terminationGracePeriodSeconds is 300 s so the JVM exits by
# itself before the kubelet's SIGKILL; its container must stop before the grace, not at it.
#
# The pod still carries a `FailedPreStopHook` event, and that is not a failure: the chart
# hardcodes the hook (`curl … PUT /v1/info/state` then `tail --pid=1`), and the tail is still
# waiting when PID 1 exits, so the kubelet reports the hook failed after a clean exit. The
# event cannot be removed without replacing the chart's hook, and it says nothing about the
# drain: the container stopped on its own before the grace, so nothing was killed. This script
# prints it as a `note:` line, never as a problem.
#
# It deletes exactly one worker pod, as the chart's test does. The Deployment replaces it.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NS="${NS:-${NAMESPACE:-stardelt}}"
problems=0
ok()  { echo "✓ $*"; }
bad() { echo "✗ $*"; problems=$((problems + 1)); }

for tool in kubectl python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "UNVERIFIED ($tool not found)"; exit 3; }
done
kubectl -n "$NS" get deploy trino-worker >/dev/null 2>&1 || {
  echo "UNVERIFIED (no Deployment trino-worker in namespace $NS)"; exit 3; }

grace="$(kubectl -n "$NS" get deploy trino-worker -o jsonpath='{.spec.template.spec.terminationGracePeriodSeconds}')"
if [ "$grace" = "300" ]; then
  ok "worker terminationGracePeriodSeconds = 300"
else
  bad "worker terminationGracePeriodSeconds = ${grace:-<unset>}, expected 300"
fi

config="$(kubectl -n "$NS" get configmap trino-worker -o jsonpath='{.data.config\.properties}' 2>/dev/null || true)"
case "$config" in
  *"shutdown.grace-period=120s"*) ok "configmap/trino-worker carries shutdown.grace-period=120s" ;;
  *) bad "configmap/trino-worker has no shutdown.grace-period=120s" ;;
esac

worker="$(kubectl -n "$NS" get pods \
  -l app.kubernetes.io/name=trino,app.kubernetes.io/component=worker \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}')"
[ -n "$worker" ] || { echo "UNVERIFIED (no running worker pod — the engine is scaled to zero)"; exit 3; }

tmp="$(mktemp -d)"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT
kubectl -n "$NS" logs -f "$worker" -c trino-worker >"$tmp/worker.log" 2>&1 &
tail_pid=$!
sleep 2
echo "› deleting worker pod $worker"
start_epoch="$(date -u +%s)"
kubectl -n "$NS" delete pod "$worker" --wait=false >/dev/null
deleted_at=""
for _ in $(seq 1 10); do
  deleted_at="$(kubectl -n "$NS" get pod "$worker" \
    -o jsonpath='{.metadata.deletionTimestamp}' 2>/dev/null || true)"
  [ -n "$deleted_at" ] && break
  sleep 1
done

# The drain ends when the worker's container stops, not when the kubelet reaps the pod object:
# a SIGKILLed container leaves exactly at the grace, and a slow containerd can keep the pod
# object around long after its process exited (measured 2026-11-22, `FailedKillPod`). Both
# timestamps are the API server's (`deletionTimestamp` and the container's `finishedAt`), so a
# clock offset between this shell and the API server cannot corrupt the elapsed time.
pod_state() {
  local out
  if ! out="$(kubectl -n "$NS" get pod "$worker" -o json 2>/dev/null)"; then
    echo gone
    return
  fi
  printf '%s' "$out" | python3 -c '
import json, sys
pod = json.load(sys.stdin)
statuses = pod.get("status", {}).get("containerStatuses") or [{}]
terminated = (statuses[0].get("state") or {}).get("terminated")
if terminated:
    print("terminated|%s|%s|%s" % (terminated.get("reason") or "",
                                   terminated.get("exitCode"),
                                   terminated.get("finishedAt") or ""))
else:
    print("running")
'
}

state="running"
terminated=""
gone=0
for _ in $(seq 1 175); do
  state="$(pod_state)"
  case "$state" in
    terminated\|*) terminated="${state#terminated|}"; break ;;
    gone) gone=1; break ;;
  esac
  sleep 2
done
stopped_at="$(date -u +%s)"
kill "$tail_pid" 2>/dev/null || true
wait "$tail_pid" 2>/dev/null || true

# platform#1205: when the kubelet has already removed the pod, its log is gone and neither
# shutdown line can be read. That is UNVERIFIED (exit 3), never a PASS and never a FAIL.
if [ "$gone" = 1 ] && ! grep -q 'Shutdown requested' "$tmp/worker.log" 2>/dev/null; then
  echo "UNVERIFIED (the kubelet removed $worker before its shutdown log could be read)"
  exit 3
fi

if grep -q 'Shutdown requested' "$tmp/worker.log"; then
  ok "the deleted worker logged 'Shutdown requested'"
else
  bad "no 'Shutdown requested' in $worker's log"
fi

# The second phase is what a pod dying at the 2 x grace minimum never reaches: Trino waits
# for the active tasks, then waits the grace again for the task states to be observed.
if awk '/All active tasks are finished/ { tasks = 1 }
        /Waiting for a grace period/ { if (tasks) found = 1 }
        END { exit(found ? 0 : 1) }' "$tmp/worker.log"; then
  ok "the deleted worker logged the second phase"
else
  bad "no second shutdown phase ('Waiting for a grace period' after 'All active tasks are finished') in $worker's log"
fi

if [ "$gone" = 0 ] && [ -z "$terminated" ]; then
  bad "worker $worker was still running 350 s after its deletion"
else
  reason=""
  exit_code=""
  finished_at=""
  if [ -n "$terminated" ]; then
    IFS='|' read -r reason exit_code finished_at <<<"$terminated"
  fi
  if [ -n "$finished_at" ] && [ -n "$deleted_at" ]; then
    elapsed="$(python3 -c 'import datetime, sys
def epoch(value):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
print(int((epoch(sys.argv[2]) - epoch(sys.argv[1])).total_seconds()))' "$deleted_at" "$finished_at")"
  else
    # The pod object was reaped before its termination could be read: fall back to this
    # shell's clock on both ends, which has no offset.
    elapsed=$((stopped_at - start_epoch))
  fi
  if [ "$elapsed" -ge "$grace" ]; then
    bad "the deleted pod left at its ${grace} s grace (${elapsed} s): the kubelet killed the worker mid-drain"
  elif [ -n "$terminated" ] && { [ "$reason" != "Completed" ] || [ "$exit_code" != "0" ]; }; then
    bad "the worker's container ended with reason ${reason:-?} (exit ${exit_code:-?}) after ${elapsed} s: the kubelet killed it mid-drain"
  else
    ok "the deleted pod left before its ${grace} s grace (${elapsed} s)"
  fi
fi

replacement=""
for _ in $(seq 1 90); do
  replacement="$(kubectl -n "$NS" get pods \
    -l app.kubernetes.io/name=trino,app.kubernetes.io/component=worker \
    --field-selector=status.phase=Running -o json \
    | python3 -c 'import json,sys
old = sys.argv[1]
rows = [p for p in json.load(sys.stdin)["items"]
        if p["metadata"]["name"] != old
        and any(c.get("type") == "Ready" and c.get("status") == "True"
                for c in p.get("status", {}).get("conditions", []))]
print(rows[0]["metadata"]["name"] if rows else "")' "$worker")"
  [ -n "$replacement" ] && break
  sleep 2
done
if [ -n "$replacement" ]; then
  ok "a replacement worker is Ready: $replacement"
else
  bad "no Ready replacement worker within 180s"
fi

# Cosmetic, and a note by design (see the header): the chart's hardcoded hook outlives the
# JVM it waits for, so the kubelet records the event even after a clean exit.
hook_event="$(kubectl -n "$NS" get events \
  --field-selector "involvedObject.name=$worker,reason=FailedPreStopHook" \
  -o jsonpath='{.items[*].message}' 2>/dev/null || true)"
if [ -n "$hook_event" ]; then
  echo "note: FailedPreStopHook is expected with the chart hook (the hook outlives the JVM); the pod left before its grace, so nothing was killed"
else
  echo "note: no FailedPreStopHook event on this pod (the chart hook returned before the JVM exited)"
fi

# The FAIL branch exits first on purpose: a trailing unconditional `exit` makes
# ShellCheck (0.9 SC2317, 0.11 SC2329) call the trap-only `cleanup` unreachable.
[ "$problems" -eq 0 ] || { echo "FAIL ($problems problem(s))"; exit 1; }
echo "PASS (6 of 6)"
