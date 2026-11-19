#!/usr/bin/env bash
# One line per check a release depends on (stardelt/stardelt#401, #2). Runs every check even
# after a failure, keeps each check's output in a log, exits 1 if any check failed.
#   make verify-release
#   VERIFY_RELEASE_TARGETS="verify-pins verify-crds" bash scripts/verify-release.sh   # a subset
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit

default_targets="verify-trino-auth verify-catalog-permissions verify-backups-all verify-streaming
verify-crds verify-pins verify-spark-config verify-restarts check-spark-runtimes-live
verify-operator-values verify-connections verify-maintenance verify-pod-security verify-cronjobs
verify-trino-tls verify-oidc-clients verify-realm-admin verify-images verify-airflow-runs"
targets="${VERIFY_RELEASE_TARGETS:-$default_targets}"

logdir="${TMPDIR:-/tmp}/verify-release.$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$logdir"
failed=0
total=0
for t in $targets; do
  total=$((total + 1))
  start=$SECONDS
  if make --no-print-directory -s "$t" > "$logdir/$t.log" 2>&1; then
    printf '✓ %-28s %3ss\n' "$t" "$((SECONDS - start))"
  else
    printf '✗ %-28s %3ss  %s\n' "$t" "$((SECONDS - start))" "$logdir/$t.log"
    failed=$((failed + 1))
  fi
done
echo "commit $(git rev-parse --short HEAD) · $((total - failed))/$total passed · logs in $logdir"
[ "$failed" -eq 0 ]
