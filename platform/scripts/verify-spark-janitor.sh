#!/usr/bin/env bash
# Prove the janitor's own dry run keeps the executor ConfigMap a live Spark driver owns.
#
# Why: spark-confmap-janitor deleted the shared Spark Connect driver's executor ConfigMap at
# 01:39Z and again at 02:20Z on 2026-11-19 (stardelt#215) and the only symptom was that every
# query hung. Rule 3 of scripts/spark-confmap-janitor.sh keeps a map created within 120 s of a
# live pod running the pinned Spark image (stardelt#222). Nothing judged that rule:
# verify-cronjobs/verify-cron-schedule prove the CronJob *ran*, verify-compute proves the script
# ConfigMap is not empty.
#
# The check runs the script's own dry run, not a copy of its logic: JANITOR_MAX_AGE_SECONDS=1
# makes every other map look old, so the only map that survives is one a live driver owns.
# JANITOR_DRY_RUN=1 deletes nothing (`would delete …`), so the check is safe against the live
# namespace. The Job is created from the CronJob's jobTemplate, never by triggering the
# CronJob: the schedule is untouched. The Job is deleted on every exit path.
#
#   NS=stardelt bash scripts/verify-spark-janitor.sh
#
# Judge-only mode, no cluster: JANITOR_VERIFY_LOG names a janitor log (a previous run, or
# `kubectl -n stardelt logs job/<name>`):
#
#   JANITOR_VERIFY_LOG=scripts/tests/fixtures/janitor-keep.log \
#     bash scripts/verify-spark-janitor.sh
#
# Environment:
#   NS                  namespace, default NAMESPACE, then stardelt
#   JANITOR_VERIFY_LOG  judge this log file instead of running the Job
#
# Exit 0 = PASS, 1 = FAIL, 3 = UNVERIFIED (no kubectl, spark-connect is not running, no
# spark-exec-*-conf-map to judge, the CronJob cannot be read).
set -euo pipefail

NS="${NS:-${NAMESPACE:-stardelt}}"
LOG="${JANITOR_VERIFY_LOG:-}"
NAME_RE='spark-exec-[0-9a-f]{16}-conf-map'

PROBES=0
OK=0
BAD=0
pass_rule() { printf '✓ %-17s%s\n' "$1" "$2"; OK=$((OK + 1)); PROBES=$((PROBES + 1)); }
fail_rule() { printf '✗ %-17s%s\n' "$1" "$2"; BAD=$((BAD + 1)); PROBES=$((PROBES + 1)); }
verdict() {
  if [ "$BAD" -ne 0 ]; then
    printf 'verify-spark-janitor: FAIL (%d of %d)\n' "$BAD" "$PROBES"
    return 1
  fi
  printf 'verify-spark-janitor: PASS (%d of %d)\n' "$OK" "$PROBES"
  return 0
}

# judge <logfile>: the three rules, read from the janitor's own output. Sets `maps`, `kept` and
# `would_delete` so a caller can see what was judged.
judge() {
  local logfile="$1"
  local summary="" summary_n=""

  maps=0
  kept=""
  would_delete=""

  maps="$(grep -Eo "$NAME_RE" "$logfile" 2>/dev/null | sort -u | wc -l | tr -d ' ')" || maps=0
  kept="$(grep -E '^keep .*\(created by a live spark driver\)$' "$logfile" 2>/dev/null | head -n 1)" || kept=""
  would_delete="$(grep -cE '^would delete ' "$logfile" 2>/dev/null)" || would_delete=0
  summary="$(grep -E '^spark-confmap-janitor: (would delete|deleted) [0-9]+ ConfigMap\(s\)' "$logfile" \
    2>/dev/null | tail -n 1)" || summary=""
  if [ -n "$summary" ]; then
    summary_n="$(printf '%s\n' "$summary" \
      | sed -nE 's/.* (would delete|deleted) ([0-9]+) ConfigMap\(s\).*/\2/p')"
  fi

  # 1. The map a live driver owns is kept — rule 3 of the janitor.
  if [ -n "$kept" ]; then
    pass_rule "live driver" "$kept"
  else
    fail_rule "live driver" "no map was kept by a live driver"
  fi

  # 2. The dry run deletes nothing.
  if [ "$summary_n" = "0" ]; then
    pass_rule "nothing deleted" "$summary"
  elif [ -n "$summary_n" ]; then
    fail_rule "nothing deleted" "$summary"
    if [ "${would_delete:-0}" -gt 0 ]; then
      grep -E '^would delete ' "$logfile" 2>/dev/null | sed 's/^/    /' || true
    fi
  else
    fail_rule "nothing deleted" "no janitor summary in the log"
  fi

  # 3. The log judged an executor ConfigMap at all: an empty log would pass the first two.
  if [ "${maps:-0}" -ge 1 ]; then
    pass_rule "map present" "$maps executor ConfigMap(s) judged"
  else
    fail_rule "map present" "no spark-exec-*-conf-map in the log"
  fi
}

# ---------------------------------------------------------------------------
# Judge-only mode: a log file instead of a cluster.
# ---------------------------------------------------------------------------
if [ -n "$LOG" ]; then
  [ -f "$LOG" ] || { echo "UNVERIFIED (JANITOR_VERIFY_LOG names no file: $LOG)"; exit 3; }
  judge "$LOG"
  verdict
  exit $?
fi

# ---------------------------------------------------------------------------
# The cluster: a shared driver must be running, there must be a map to judge, and the CronJob
# must be readable. Each of those is UNVERIFIED, never a failure of the rule.
# ---------------------------------------------------------------------------
command -v kubectl >/dev/null 2>&1 || { echo "UNVERIFIED (kubectl not found)"; exit 3; }

ready="$(kubectl -n "$NS" get deploy spark-connect -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
case "${ready:-}" in
  '' | 0) echo "UNVERIFIED (spark-connect is not running — the shared driver owns the map)"; exit 3 ;;
esac

map_count="$(kubectl -n "$NS" get configmap \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null \
  | grep -cE "^${NAME_RE}$")" || map_count=0
[ "${map_count:-0}" -ge 1 ] || { echo "UNVERIFIED (no spark-exec-*-conf-map to judge)"; exit 3; }

# The Job spec, built from the CronJob's jobTemplate. The two knobs are appended only after the
# template is proven not to carry them: a template that already sets one would make this a real,
# deleting run.
JOB_PY="$(cat <<'PY'
import json, sys

name, namespace = sys.argv[1], sys.argv[2]
try:
    cronjob = json.load(sys.stdin)
except ValueError:
    sys.exit(1)
try:
    spec = cronjob["spec"]["jobTemplate"]["spec"]
    container = spec["template"]["spec"]["containers"][0]
except (KeyError, IndexError, TypeError):
    sys.exit(1)

knobs = {"JANITOR_DRY_RUN", "JANITOR_MAX_AGE_SECONDS"}
env = container.get("env") or []
if any(entry.get("name") in knobs for entry in env if isinstance(entry, dict)):
    sys.exit(2)

env.append({"name": "JANITOR_DRY_RUN", "value": "1"})
env.append({"name": "JANITOR_MAX_AGE_SECONDS", "value": "1"})
container["env"] = env

job = {
    "apiVersion": "batch/v1",
    "kind": "Job",
    "metadata": {
        "name": name,
        "namespace": namespace,
        "labels": {"app.kubernetes.io/part-of": "stardelt"},
    },
    "spec": spec,
}
json.dump(job, sys.stdout)
PY
)"

job="janitor-verify-$(date -u +%H%M%S)"
tmp="$(mktemp "${TMPDIR:-/tmp}/verify-spark-janitor.XXXXXX")"
spec_file="$tmp.job.json"
cleanup() {
  kubectl -n "$NS" delete job "$job" --ignore-not-found >/dev/null 2>&1 || true
  rm -f "$tmp" "$spec_file"
}
trap cleanup EXIT

rc=0
kubectl -n "$NS" get cronjob spark-confmap-janitor -o json \
  | python3 -c "$JOB_PY" "$job" "$NS" >"$spec_file" || rc=$?
case "$rc" in
  0) ;;
  2)
    fail_rule "dry run" "the CronJob's template already carries a janitor knob"
    verdict
    exit 1
    ;;
  *) echo "UNVERIFIED (cannot read cronjob/spark-confmap-janitor in $NS)"; exit 3 ;;
esac

kubectl -n "$NS" apply -f "$spec_file" >/dev/null
pass_rule "dry run" "job/$job carries JANITOR_DRY_RUN=1 JANITOR_MAX_AGE_SECONDS=1"

if ! kubectl -n "$NS" wait --for=condition=complete "job/$job" --timeout=180s >/dev/null; then
  fail_rule "log" "job/$job did not complete"
  verdict
  exit 1
fi

if ! kubectl -n "$NS" logs "job/$job" >"$tmp"; then
  fail_rule "log" "job/$job has no log"
  verdict
  exit 1
fi

judge "$tmp"
verdict
