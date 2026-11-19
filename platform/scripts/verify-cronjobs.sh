#!/usr/bin/env bash
# Prove every CronJob's newest Job is healthy: for each CronJob the newest Job it owns,
# found by ownerReference, either succeeded, is running, is waiting, or the CronJob is
# suspended. The janitor and ddns failed every scheduled run for most of an hour and
# nothing said so (platform#1584, platform#1580); their Jobs and the CronJob's status
# carried the failure, no existing verifier read them.
#
#   NS=stardelt bash scripts/verify-cronjobs.sh
#   NS=stardelt bash scripts/verify-cronjobs.sh --max-age
#
# Read-only: it creates, patches and deletes nothing. Exit 0 = PASS, 1 = FAIL,
# 3 = UNVERIFIED (could not see the subject).
#
# The newest Job is the signal, not the CronJob's `lastScheduleTime` (that advances on
# a failing run) and not `lastSuccessfulTime` (`successfulJobsHistoryLimit: 1` prunes
# the old success). `--max-age` (`make verify-cron-schedule`) also ages that Job against
# the CronJob's own `spec.schedule` plus a 900-second grace, so a scheduler that stops
# creating Jobs after a success is a failure too (platform#216).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NS="${NS:-${NAMESPACE:-stardelt}}"
MAX_AGE="${MAX_AGE:-}"
for arg in "$@"; do
  case "$arg" in
    --max-age) MAX_AGE=1 ;;
    *) echo "verify-cronjobs: unknown argument: $arg" >&2; exit 2 ;;
  esac
done
export MAX_AGE

command -v kubectl >/dev/null 2>&1 || { echo "verify-cronjobs: UNVERIFIED (kubectl not found)"; exit 3; }
command -v python3 >/dev/null 2>&1 || { echo "UNVERIFIED (python3 not found)"; exit 3; }

if ! kubectl get namespace "$NS" >/dev/null 2>&1; then
  echo "UNVERIFIED (namespace $NS does not exist)"
  exit 3
fi

# Never report PASS from a run that could not see both lists.
if ! cronjobs="$(kubectl -n "$NS" get cronjobs -o json 2>&1)"; then
  echo "UNVERIFIED (cannot list cronjobs in $NS: $cronjobs)"
  exit 3
fi
if ! jobs_json="$(kubectl -n "$NS" get jobs -o json 2>&1)"; then
  echo "UNVERIFIED (cannot list jobs in $NS: $jobs_json)"
  exit 3
fi

if ! report="$({ printf '%s\0%s' "$cronjobs" "$jobs_json"; } | python3 -c '
import json, os, sys, datetime

raw = sys.stdin.buffer.read()
cronjobs_raw, _, jobs_raw = raw.partition(b"\0")
ns = sys.argv[1] if len(sys.argv) > 1 else "stardelt"
MAX_AGE = bool(os.environ.get("MAX_AGE"))

GRACE_SECONDS = 900


def expected_gap_seconds(schedule):
    """Seconds between two firings for the schedule shapes this platform ships.

    */N * * * *  -> N*60     (the maint-* crons, ddns, the janitor)
    <m> * * * *  -> 3600     (hourly)
    <m> <h> * * * -> 86400   (the daily backups, demo-reset)
    Anything else is unsupported and not aged.
    """
    fields = (schedule or "").split()
    if len(fields) != 5:
        return None
    minute, hour, dom, mon, dow = fields
    if dom != "*" or mon != "*" or dow != "*":
        return None
    if minute.startswith("*/") and minute[2:].isdigit() and int(minute[2:]) > 0 and hour == "*":
        return int(minute[2:]) * 60
    if minute.isdigit() and hour == "*":
        return 3600
    if minute.isdigit() and hour.isdigit():
        return 86400
    return None


def timestamp(raw_ts):
    """RFC3339 from the API server as an aware datetime, or None."""
    if not raw_ts:
        return None
    try:
        return datetime.datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
    except ValueError:
        return None


now = datetime.datetime.now(datetime.timezone.utc)

cronjobs = (json.loads(cronjobs_raw or b"{}").get("items") or [])
jobs = (json.loads(jobs_raw or b"{}").get("items") or [])

# A Job belongs to its CronJob through ownerReferences; the name prefix is not a match
# (a manual Job named demo-reset-verify also starts with demo-reset-).
owned = {}
for job in jobs:
    meta = job.get("metadata") or {}
    for ref in (meta.get("ownerReferences") or []):
        if ref.get("kind") == "CronJob" and ref.get("name"):
            owned.setdefault(ref["name"], []).append(job)

problems = 0
count = 0
lines = []


def row(mark, name, rest):
    return "%s %-16s  %s" % (mark, name, rest)


for cronjob in sorted(cronjobs, key=lambda i: (i.get("metadata") or {}).get("name") or ""):
    name = (cronjob.get("metadata") or {}).get("name") or "?"
    count += 1
    if (cronjob.get("spec") or {}).get("suspend") is True:
        lines.append(row("–", name, "suspended"))
        continue
    owned_jobs = owned.get(name) or []
    if not owned_jobs:
        lines.append(row("–", name, "no Job yet"))
        continue
    newest = max(owned_jobs, key=lambda j: (j.get("metadata") or {}).get("creationTimestamp") or "")
    job_name = (newest.get("metadata") or {}).get("name") or "?"
    status = newest.get("status") or {}
    active = status.get("active") or 0
    succeeded = status.get("succeeded") or 0
    failed = status.get("failed") or 0
    if active > 0:
        lines.append(row("…", name, "%s is running" % job_name))
    elif succeeded > 0:
        lines.append(row("✓", name, "%s succeeded" % job_name))
    elif failed > 0:
        lines.append(row("✗", name, "%s failed (%d attempt(s))" % (job_name, failed)))
        lines.append("            hint: kubectl -n %s logs job/%s --tail=20" % (ns, job_name))
        problems += 1
    else:
        lines.append(row("…", name, "%s is waiting" % job_name))

    if MAX_AGE:
        spec = cronjob.get("spec") or {}
        schedule = spec.get("schedule") or "?"
        gap = expected_gap_seconds(spec.get("schedule"))
        if gap is None:
            lines.append(row("?", name, "schedule %s not aged (unsupported shape)" % schedule))
            continue
        created = timestamp((cronjob.get("metadata") or {}).get("creationTimestamp"))
        if created is None:
            continue
        # A CronJob younger than one interval plus the grace has not had its first real chance.
        if (now - created).total_seconds() <= gap + GRACE_SECONDS:
            continue
        job_created_raw = (newest.get("metadata") or {}).get("creationTimestamp")
        job_created = timestamp(job_created_raw)
        if job_created is not None and (now - job_created).total_seconds() > gap + GRACE_SECONDS:
            lines.append(row("✗", name, "no Job since %s (the schedule fires every %d min)"
                             % (job_created_raw, gap // 60)))
            problems += 1

for line in lines:
    print(line)
print("__meta__ %d|%d" % (problems, count))
' "$NS")"; then
  echo "UNVERIFIED (cannot evaluate the cluster state: $report)"
  exit 3
fi

meta="$(printf '%s\n' "$report" | grep '^__meta__ ' || true)"
printf '%s\n' "$report" | grep -v '^__meta__ ' || true
if [ -z "$meta" ]; then
  echo "UNVERIFIED (cannot evaluate the cluster state)"
  exit 3
fi
IFS='|' read -r problems count <<< "${meta#__meta__ }"

if [ "$problems" -gt 0 ]; then
  if [ -n "$MAX_AGE" ]; then
    echo "FAIL ($problems CronJob(s) with a failed newest Job or no Job within its schedule)"
  else
    echo "FAIL ($problems CronJob(s) with a failed newest Job)"
  fi
  exit 1
fi

if [ -n "$MAX_AGE" ]; then
  echo "PASS ($count CronJobs: every newest Job succeeded, is running, or is suspended, and none is older than its schedule)"
else
  echo "PASS ($count CronJobs: every newest Job succeeded, is running, or is suspended)"
fi
