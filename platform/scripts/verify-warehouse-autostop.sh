#!/usr/bin/env bash
# The idle policy reaches 0 and wakes (#30 step A, stardelt/stardelt#1272).
#
#   make verify-warehouse-autostop           NAMESPACE=stardelt, WAIT=420
#
# Every `Ready` class is temporarily set to `autoStop: 1m`, `minWorkers: 0`,
# `maxWorkers: 1` and given a fresh `stardelt.io/last-query-at`, so the access-sync job
# (every 2 minutes) scales the one worker Deployment to 0. The classes then get
# `autoStop: 15m` and the default class a fresh stamp — what Nova's Start writes — and
# the engine must come back. `status.scale.phase` must read `Stopped` and then `Running`.
#
# This check PATCHES LIVE CONFIGURATION for at most ~10 minutes. On any exit — a `bad`
# row included — it puts back each patched class's `autoStop`/`minWorkers`/`maxWorkers`
# from the spec it saved, and its `stardelt.io/last-query-at` annotation: the saved
# value, or no annotation when the class had none. Row 6 compares the specs and stamps
# with the saved copies and is what proves the restore.
#
# A `Ready` class is `status.phase == "Ready"`, the scaler's own definition
# (`scripts/access-sync-job.py`, `scale_decision`). A `SqlWarehouse` carries no `Ready`
# condition — only `Rendered` — so the conditions are never the selector. Zero Ready
# classes is a `bad` row: nothing was patched, so nothing is proved.
#
# Exit 0 = PASS, 1 = FAIL, 3 = UNVERIFIED (no kubectl/jq, no CRD, no worker Deployment).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NAMESPACE:-stardelt}"
WAIT="${WAIT:-420}"

for tool in kubectl jq; do
  command -v "$tool" >/dev/null 2>&1 || { echo "UNVERIFIED ($tool not found)"; exit 3; }
done

scratch="${TMPDIR:-$HOME/.cache/opencode-scratch}"
mkdir -p "$scratch"
tmp="$(mktemp -d "$scratch/verify-warehouse-autostop.XXXXXX")"

fail=0
restored=0
restore_rc=0
ok() { printf 'OK %s\n' "$*"; }
bad() { printf 'BAD %s\n' "$*"; fail=1; }

# Put every patched class back: the three spec keys from the saved `.spec` (a key that was
# absent is sent as `null`, which a merge patch removes) and its stamp (or its absence).
restore() {
  if [ "$restored" = 1 ]; then return "$restore_rc"; fi
  restored=1
  local rc=0 name patch stamp f
  for f in "$tmp"/ready-*.json; do
    [ -e "$f" ] || continue
    name="$(jq -r '.name' "$f")"
    patch="$(jq -c '{spec: {
        autoStop: (.spec.autoStop // null),
        minWorkers: (.spec.minWorkers // null),
        maxWorkers: (.spec.maxWorkers // null)
      }}' "$f")"
    kubectl -n "$NAMESPACE" patch sqlwarehouse "$name" --type=merge -p "$patch" >/dev/null || rc=1
    stamp="$(jq -r '.stamp' "$f")"
    if [ -n "$stamp" ]; then
      kubectl -n "$NAMESPACE" annotate sqlwarehouse "$name" \
        "stardelt.io/last-query-at=$stamp" --overwrite >/dev/null || rc=1
    else
      kubectl -n "$NAMESPACE" annotate sqlwarehouse "$name" \
        "stardelt.io/last-query-at-" >/dev/null || rc=1
    fi
  done
  restore_rc="$rc"
  return "$rc"
}

# shellcheck disable=SC2329  # called by the trap below
on_exit() {
  local rc=$?
  if ! restore; then
    echo "BAD the classes could not be restored — they still carry the temporary spec:" >&2
    kubectl -n "$NAMESPACE" get sqlwarehouse -o json 2>/dev/null |
      jq -r '.items[] | "  \(.metadata.name) autoStop=\(.spec.autoStop // "") min=\(.spec.minWorkers // "") max=\(.spec.maxWorkers // "")"' >&2 || true
  fi
  rm -rf "$tmp"
  exit "$rc"
}
trap on_exit EXIT INT TERM

finish() {
  if [ "$fail" = 0 ]; then
    echo "RESULT=PASS"
    exit 0
  fi
  echo "RESULT=FAIL"
  exit 1
}

# `trino-worker spec.replicas readyReplicas`; `? ?` when the read fails this second.
read_workers() {
  local json
  if ! json="$(kubectl -n "$NAMESPACE" get deployment trino-worker -o json 2>/dev/null)"; then
    echo "? ?"
    return 0
  fi
  jq -r '"\(.spec.replicas // 0) \(.status.readyReplicas // 0)"' <<<"$json"
}

if ! kubectl -n "$NAMESPACE" get sqlwarehouse >/dev/null 2>&1; then
  echo "UNVERIFIED (no SqlWarehouse CRD in namespace $NAMESPACE)"
  exit 3
fi
if ! kubectl -n "$NAMESPACE" get deployment trino-worker >/dev/null 2>&1; then
  echo "UNVERIFIED (no deployment/trino-worker in namespace $NAMESPACE)"
  exit 3
fi

kubectl -n "$NAMESPACE" get sqlwarehouse -o json >"$tmp/warehouses.json"

default_count="$(jq '[.items[] | select(.spec.default == true)] | length' "$tmp/warehouses.json")"
if [ "$default_count" != "1" ]; then
  bad "exactly one default class (found $default_count: $(jq -r '[.items[] | select(.spec.default == true) | .metadata.name] | join(", ")' "$tmp/warehouses.json"))"
  finish
fi
default="$(jq -r '.items[] | select(.spec.default == true) | .metadata.name' "$tmp/warehouses.json")"

jq -r '.items[] | select(.status.phase == "Ready") | .metadata.name' "$tmp/warehouses.json" >"$tmp/classes.txt"
mapfile -t classes <"$tmp/classes.txt"
if [ "${#classes[@]}" -eq 0 ]; then
  bad "at least one Ready class (found 0 — N=0 proves nothing, so nothing is patched)"
  finish
fi

# What the restore puts back, and what row 6 compares: every class's spec and stamp.
jq -S '[.items[] | {name: .metadata.name, spec: .spec,
  stamp: (.metadata.annotations["stardelt.io/last-query-at"] // "")}] | sort_by(.name)' \
  "$tmp/warehouses.json" >"$tmp/saved.json"
for name in "${classes[@]}"; do
  jq -S --arg n "$name" '.items[] | select(.metadata.name == $n) | {name: .metadata.name, spec: .spec,
    stamp: (.metadata.annotations["stardelt.io/last-query-at"] // "")}' \
    "$tmp/warehouses.json" >"$tmp/ready-$name.json"
done

stamp_now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
for name in "${classes[@]}"; do
  kubectl -n "$NAMESPACE" annotate sqlwarehouse "$name" \
    "stardelt.io/last-query-at=$stamp_now" --overwrite >/dev/null
  kubectl -n "$NAMESPACE" patch sqlwarehouse "$name" --type=merge \
    -p '{"spec":{"autoStop":"1m","minWorkers":0,"maxWorkers":1}}' >/dev/null
done

live="$(kubectl -n "$NAMESPACE" get sqlwarehouse -o json)"
names="$(jq -R -s 'split("\n") | map(select(length > 0))' "$tmp/classes.txt")"
refused="$(jq -r --argjson names "$names" '
  .items[]
  | select(.metadata.name as $n | $names | index($n))
  | select(.spec.autoStop != "1m" or (.spec.minWorkers // 1) != 0 or (.spec.maxWorkers // 1) != 1)
  | "\(.metadata.name) autoStop=\(.spec.autoStop // "") min=\(.spec.minWorkers // "") max=\(.spec.maxWorkers // "")"
' <<<"$live")"
if [ -n "$refused" ]; then
  bad "the cluster refused autoStop=1m, minWorkers=0, maxWorkers=1: $(tr '\n' ';' <<<"$refused")"
  finish
fi
ok "every Ready class carries autoStop=1m (${#classes[@]} classes)"

started="$(date +%s)"
last="?"
while :; do
  last="$(read_workers)"
  if [ "$last" = "0 0" ]; then
    ok "the engine scaled to 0 ($(($(date +%s) - started))s)"
    phase="$(kubectl -n "$NAMESPACE" get sqlwarehouse "$default" \
      -o jsonpath='{.status.scale.phase}')" || phase=""
    if [ "$phase" = "Stopped" ]; then
      ok "status.scale.phase=Stopped"
    else
      bad "status.scale.phase=${phase:-unset} of the default class $default (expected Stopped)"
    fi
    break
  fi
  if [ "$(($(date +%s) - started))" -ge "$WAIT" ]; then
    bad "the engine did not reach 0 within ${WAIT}s (last trino-worker spec.replicas readyReplicas = $last)"
    finish
  fi
  sleep 10
done

# Wake it the way Nova's Start does, with a window a 2-minute tick cannot close.
for name in "${classes[@]}"; do
  kubectl -n "$NAMESPACE" patch sqlwarehouse "$name" --type=merge \
    -p '{"spec":{"autoStop":"15m"}}' >/dev/null
done
wake_started="$(date +%s)"
kubectl -n "$NAMESPACE" annotate sqlwarehouse "$default" \
  "stardelt.io/last-query-at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" --overwrite >/dev/null

# `true` when the value of read_workers is two numbers and at least one of each.
engine_up() {
  local replicas="${1%% *}" ready="${1##* }"
  case "$replicas" in '' | *[!0-9]*) return 1 ;; esac
  case "$ready" in '' | *[!0-9]*) return 1 ;; esac
  [ "$replicas" -ge 1 ] && [ "$ready" -ge 1 ]
}

last="?"
while :; do
  last="$(read_workers)"
  if engine_up "$last"; then
    ok "the wake annotation brought the engine back ($(($(date +%s) - wake_started))s)"
    break
  fi
  if [ "$(($(date +%s) - wake_started))" -ge "$WAIT" ]; then
    bad "the wake annotation did not bring the engine back within ${WAIT}s (last trino-worker spec.replicas readyReplicas = $last)"
    finish
  fi
  sleep 10
done

# Row 5 shares row 4's budget, counted from the annotation: the phase is written from the
# Deployment read *before* the tick's patch, so `Running` is the tick after the scale-up.
phase=""
while :; do
  phase="$(kubectl -n "$NAMESPACE" get sqlwarehouse "$default" \
    -o jsonpath='{.status.scale.phase}')" || phase=""
  if [ "$phase" = "Running" ]; then
    ok "status.scale.phase=Running"
    break
  fi
  if [ "$(($(date +%s) - wake_started))" -ge "$WAIT" ]; then
    bad "status.scale.phase=${phase:-unset} of the default class $default after ${WAIT}s from the wake (expected Running)"
    finish
  fi
  sleep 10
done

if ! restore; then
  bad "the restore did not succeed (see the kubectl errors above)"
  finish
fi
if live="$(kubectl -n "$NAMESPACE" get sqlwarehouse -o json)"; then
  jq -S '[.items[] | {name: .metadata.name, spec: .spec,
    stamp: (.metadata.annotations["stardelt.io/last-query-at"] // "")}] | sort_by(.name)' \
    <<<"$live" >"$tmp/after.json"
  if diff -u "$tmp/saved.json" "$tmp/after.json" >"$tmp/restore.diff"; then
    ok "the classes' specs and stamps are restored"
  else
    bad "the restore did not put every class back:"
    sed -n '1,60p' "$tmp/restore.diff"
  fi
else
  bad "cannot read the classes back after the restore"
fi

finish
