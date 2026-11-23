#!/usr/bin/env bash
# Every running pod is admitted by a Pod Security namespace at the level given (`--level`, default
# `restricted`; #1096 question 5). The pod templates of the namespace's CronJobs are admitted too,
# because a pod that runs for seconds is invisible to a scan of Running pods (#1363; #1362's flip
# makes a refusal a silent nightly failure).
#
#   NS=stardelt bash scripts/verify-pod-security.sh --allow '^seaweedfs-' --allow '^lakekeeper-'
#   NS=stardelt bash scripts/verify-pod-security.sh --level baseline --allow-namespace-level baseline
#
# Read-only for the platform: each live pod's spec is re-admitted with a server-side dry run into a
# scratch namespace labelled `pod-security.kubernetes.io/enforce=$level`, which is deleted
# again. Every run uses its own probe namespace `stardelt-pss-probe-<pid>`, labelled
# `stardelt.io/pod-security-probe=true`, and deletes only that one; older labelled leftovers of a
# run killed before its trap are swept at start. `--allow <regex>` names a pod that is a known
# exception; the reason belongs in the comment
# beside the flag in the Makefile. A pattern that matches no refusal is reported (drop it), and a
# refusal outside the list fails the run. `--allow-namespace-level <level>` names an accepted
# `enforce` level of the platform namespace itself (its `audit` and `warn` must be `restricted`).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
PROBE="${POD_SECURITY_PROBE_NS:-stardelt-pss-probe-$$}"
level=restricted
allow=()
allow_ns_level=()
while [ $# -gt 0 ]; do
  case "$1" in
    --allow) allow+=("${2:?--allow needs a regular expression}"); shift 2 ;;
    --allow-namespace-level)
      allow_ns_level+=("${2:?--allow-namespace-level needs a level}"); shift 2 ;;
    --level)
      level="${2:?--level needs baseline or restricted}"
      case "$level" in
        baseline|restricted) ;;
        *) echo "unknown --level: $level (baseline or restricted)" >&2; exit 2 ;;
      esac
      shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
command -v kubectl >/dev/null 2>&1 || { echo "UNVERIFIED (kubectl not found)"; exit 3; }
command -v jq >/dev/null 2>&1 || { echo "UNVERIFIED (jq not found)"; exit 3; }
tmp="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp"
  kubectl delete namespace "$PROBE" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT
# A run killed before its trap leaves its own probe namespace behind. Each one is labelled, so
# remove the ones older than ten minutes; this run's namespace is new and is never selected.
now="$(date -u +%s)"
kubectl get namespace -l stardelt.io/pod-security-probe=true -o json 2>/dev/null \
  | jq -r --argjson now "$now" \
      '.items[] | select($now - (.metadata.creationTimestamp | fromdateiso8601) > 600) | .metadata.name' \
      2>/dev/null \
  | while read -r stale; do
      kubectl delete namespace "$stale" --ignore-not-found --wait=false >/dev/null 2>&1 || true
    done || true
if ! kubectl create namespace "$PROBE" >/dev/null; then
  echo "cannot create the probe namespace $PROBE (the API server's message is above); set POD_SECURITY_PROBE_NS to another name" >&2
  exit 1
fi
if ! kubectl label namespace "$PROBE" \
    pod-security.kubernetes.io/enforce="$level" \
    pod-security.kubernetes.io/enforce-version=latest \
    stardelt.io/pod-security-probe=true >/dev/null; then
  echo "cannot label the probe namespace $PROBE $level (the API server's message is above)" >&2
  exit 1
fi

labels="$(kubectl get namespace "$NS" -o jsonpath='{.metadata.labels}' 2>&1)" || {
  echo "cannot read namespace $NS: $labels" >&2; exit 1; }
enforce="$(printf '%s' "$labels" | jq -r '."pod-security.kubernetes.io/enforce" // ""')"
audit="$(printf '%s' "$labels" | jq -r '."pod-security.kubernetes.io/audit" // ""')"
warn="$(printf '%s' "$labels" | jq -r '."pod-security.kubernetes.io/warn" // ""')"
ns_problem=""
if [ "$enforce" != restricted ]; then
  allowed_level=""
  for accepted in ${allow_ns_level[@]+"${allow_ns_level[@]}"}; do
    [ "$enforce" = "$accepted" ] && allowed_level="$accepted"
  done
  if [ -z "$allowed_level" ]; then
    ns_problem="pod-security.kubernetes.io/enforce is ${enforce:-not set} (accepted: restricted ${allow_ns_level[*]:-—})"
  fi
fi
[ "$audit" = restricted ] || ns_problem="${ns_problem:+$ns_problem; }pod-security.kubernetes.io/audit is ${audit:-not set}"
[ "$warn" = restricted ] || ns_problem="${ns_problem:+$ns_problem; }pod-security.kubernetes.io/warn is ${warn:-not set}"
if [ -n "$ns_problem" ]; then
  echo "✗ namespace $NS: $ns_problem"
  exit 1
fi
if [ "$enforce" = restricted ]; then
  echo "✓ namespace $NS enforces restricted (audit and warn restricted)"
else
  echo "– namespace $NS enforces $enforce (allowed by --allow-namespace-level; audit and warn restricted)"
fi

plural() { [ "$1" -eq 1 ] && echo exception || echo exceptions; }

total=0 passed=0 failed=0 allowed=0
: > "$tmp/allowed"
: > "$tmp/refused"

for pod in $(kubectl -n "$NS" get pods -o json \
    | jq -r '.items[] | select(.status.phase=="Running") | .metadata.name' | sort); do
  # The list and the get are two API calls; a pipeline task pod or a CronJob pod can finish
  # between them. A pod that is gone cannot violate the floor — skip it, do not abort the scan
  # (2026-11-16: a finished demo pod turned a green floor into exit 1).
  if ! kubectl -n "$NS" get pod "$pod" -o json >"$tmp/pod.read" 2>"$tmp/pod.err"; then
    echo "– $pod: gone before the probe (skipped)"
    continue
  fi
  total=$((total + 1))
  # The pod spec as the API server sees it, with only what a scratch namespace cannot carry
  # removed: its identity, the node it is pinned to, and a priority class it may not have.
  jq --arg name "probe-$total" --arg ns "$PROBE" '
      .metadata = {name: $name, namespace: $ns}
      | .spec.serviceAccountName = "default"
      | del(.status, .spec.nodeName, .spec.priorityClassName)
    ' "$tmp/pod.read" >"$tmp/pod.json"
  if kubectl -n "$PROBE" create --dry-run=server -f "$tmp/pod.json" >/dev/null 2>"$tmp/err"; then
    passed=$((passed + 1))
    continue
  fi
  reason="$(grep -o "violates PodSecurity \"$level:latest\": .*" "$tmp/err" | head -1 | cut -c1-200 || true)"
  [ -n "$reason" ] || reason="$(head -1 "$tmp/err")"
  matched=""
  for pattern in ${allow[@]+"${allow[@]}"}; do
    if printf '%s\n' "$pod" | grep -Eq "$pattern"; then matched="$pattern"; break; fi
  done
  if [ -n "$matched" ]; then
    allowed=$((allowed + 1))
    printf '%s\t%s\n' "$pod" "$matched" >> "$tmp/allowed"
    echo "– $pod: known exception ($matched)"
  else
    failed=$((failed + 1))
    printf '%s\t%s\n' "$pod" "$reason" >> "$tmp/refused"
    echo "✗ $pod: $reason"
  fi
done

templates=0 tpl_passed=0
: >"$tmp/template.refused"
# The list is written to a file and its status checked: a `for` over a command substitution
# swallows the failure, which would print `PASS … 0 of 0 CronJob template(s)` on a namespace
# whose CronJobs cannot be read — the blind spot this check exists to close.
if ! kubectl -n "$NS" get cronjobs -o json >"$tmp/cj.list" 2>"$tmp/cj.list.err"; then
  echo "✗ cannot list the CronJobs of $NS: $(head -1 "$tmp/cj.list.err")"
  exit 1
fi
cronjobs="$(jq -r '.items[].metadata.name' "$tmp/cj.list" | sort)" || {
  echo "✗ cannot read the CronJob list of $NS"; exit 1; }
for cj in $cronjobs; do
  if ! kubectl -n "$NS" get cronjob "$cj" -o json >"$tmp/cj.read" 2>"$tmp/cj.read.err"; then
    if grep -q "NotFound" "$tmp/cj.read.err"; then
      echo "– cronjob/$cj: gone before the probe (skipped)"
      continue
    fi
    # Anything but NotFound means the template was never probed; skipping it would let it
    # report PASS without being admitted.
    echo "✗ cannot read cronjob/$cj: $(head -1 "$tmp/cj.read.err")"
    exit 1
  fi
  templates=$((templates + 1))
  jq --arg name "probe-cron-$templates" --arg ns "$PROBE" '
      .spec.jobTemplate.spec.template
      | .metadata.name = $name
      | .metadata.namespace = $ns
      | .metadata.labels = (.metadata.labels // {})
      | .metadata.annotations = (.metadata.annotations // {})
      | .spec.serviceAccountName = "default"
      | del(.spec.nodeName, .spec.priorityClassName)
      | {apiVersion: "v1", kind: "Pod", metadata: .metadata, spec: .spec}
    ' "$tmp/cj.read" >"$tmp/cj.json"
  if kubectl -n "$PROBE" create --dry-run=server -f "$tmp/cj.json" >/dev/null 2>"$tmp/err"; then
    tpl_passed=$((tpl_passed + 1))
    continue
  fi
  reason="$(grep -o "violates PodSecurity \"$level:latest\": .*" "$tmp/err" | head -1 | cut -c1-200 || true)"
  [ -n "$reason" ] || reason="$(head -1 "$tmp/err")"
  matched=""
  for pattern in ${allow[@]+"${allow[@]}"}; do
    if printf 'cronjob/%s\n' "$cj" | grep -Eq "$pattern"; then matched="$pattern"; break; fi
  done
  if [ -n "$matched" ]; then
    allowed=$((allowed + 1))
    printf 'cronjob/%s\t%s\n' "$cj" "$matched" >>"$tmp/allowed"
    echo "– cronjob/$cj: known exception ($matched)"
  else
    failed=$((failed + 1))
    printf 'cronjob/%s\t%s\n' "$cj" "$reason" >>"$tmp/template.refused"
    echo "✗ cronjob/$cj: $reason"
  fi
done

for pattern in ${allow[@]+"${allow[@]}"}; do
  if ! grep -Fq "$pattern" "$tmp/allowed"; then
    echo "note: --allow '$pattern' matched no refusal; drop it from the Makefile"
  fi
done

if [ "$failed" -gt 0 ]; then
  echo "FAIL ($((passed + tpl_passed)) of $((total + templates)) admitted: $passed of $total pod(s), $tpl_passed of $templates CronJob template(s); $failed refused, $allowed known $(plural "$allowed"))"
  exit 1
fi
if [ "$passed" -lt 5 ]; then
  echo "UNVERIFIED ($passed of $total pods admitted; install the platform first)"
  exit 3
fi
echo "PASS ($((passed + tpl_passed)) admitted by $level: $passed of $total pod(s) and $tpl_passed of $templates CronJob template(s), $allowed known $(plural "$allowed"))"
