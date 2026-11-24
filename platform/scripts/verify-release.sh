#!/usr/bin/env bash
# One line per check a release depends on (stardelt/stardelt#401, #2). Runs every check even
# after a failure, keeps each check's output in a log, and tells three outcomes apart:
#   ✓ the check passed · ✗ the check proved something false · ? the check proved nothing (exit 3)
# The recipes are called directly, not through `make`: GNU make turns every failed recipe into
# exit 2, so a script's own exit 3 never reaches this loop (stardelt#540, measured 2026-11-20).
#   make verify-release
#   VERIFY_RELEASE_TARGETS="verify-pins verify-crds" bash scripts/verify-release.sh   # a subset
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit

# The command each Makefile target runs. `scripts/tests/test_verify_release.py` reads the Makefile
# and fails when this table and the recipes drift apart.
declare -A recipe=(
  [verify-trino-auth]='bash scripts/verify-trino-auth.sh'
  [verify-catalog-permissions]='bash scripts/verify-catalog-permissions.sh'
  [verify-backups-all]='bash scripts/verify-backups-all.sh'
  [verify-streaming]='bash scripts/verify-streaming.sh'
  [verify-crds]='bash scripts/verify-crds.sh'
  [verify-access-sync-script]='python3 scripts/check_access_sync_live.py'
  [verify-pins]='bash scripts/verify-pins.sh'
  [verify-contract]='bash scripts/verify-contract.sh'
  [verify-spark-config]='bash scripts/verify-spark-config.sh'
  [verify-restarts]='bash scripts/verify-restarts.sh'
  [check-spark-runtimes-live]='bash scripts/check-spark-runtimes.sh --live'
  [verify-operator-values]='bash scripts/verify-operator-values.sh'
  [verify-connections]='bash scripts/verify-connections.sh'
  [verify-maintenance]='bash scripts/verify-maintenance.sh'
  [verify-pod-security]='bash scripts/verify-pod-security.sh # the namespace enforces restricted (#1095)'
  [verify-cronjobs]='bash scripts/verify-cronjobs.sh'
  [verify-trino-tls]='bash scripts/verify-trino-tls.sh'
  [verify-oidc-clients]='bash scripts/verify-oidc-clients.sh'
  [verify-mcp]='bash scripts/verify-mcp.sh'
  [verify-mcp-tools]='bash scripts/verify-mcp-tools.sh'
  [verify-realm-admin]='bash scripts/verify-realm-admin.sh'
  [verify-images]='bash scripts/verify-images.sh'
  [verify-airflow-runs]='bash scripts/verify-airflow-runs.sh'
  [verify-catalog-workload-auth]='bash scripts/verify-catalog-workload-auth.sh'
  [verify-workload-identity]='bash scripts/verify-workload-identity.sh'
  [verify-spark-demo-sandbox]='bash scripts/verify-spark-demo-sandbox.sh'
  [verify-spark-demo-query]='bash scripts/verify-spark-demo-query.sh'
  [verify-spark-auth]='bash scripts/verify-spark-auth.sh'
  [verify-spark-query]='bash scripts/verify-spark-query.sh'
  [verify-spark-janitor]='bash scripts/verify-spark-janitor.sh'
  [verify-spark-demo-security]='DEMO_NS=stardelt-demo-spark bash scripts/verify-spark-demo-security.sh'
  [verify-wap]='bash scripts/verify-wap.sh'
  [verify-product-path]='bash scripts/verify-product-path.sh'
  [verify-cli]='bash scripts/verify-cli.sh # the shipped CLI as demo'
  [verify-pipeline-validate]='bash scripts/verify-pipeline-validate.sh'
  [verify-spark-pipeline]='bash scripts/verify-spark-pipeline.sh'
  [verify-spark-no-platform-key]='bash scripts/verify-spark-no-platform-key.sh'
  [verify-spark-guard]='bash scripts/verify-spark-pod-guard.sh'
  [verify-spark-driver-identity]='bash scripts/verify-spark-driver-identity.sh'
  [verify-spark-group-read]='bash scripts/verify-spark-cluster-read.sh'
  [verify-spark-group-write]='bash scripts/verify-spark-cluster-write.sh'
  [verify-superset-nova-identity]='bash scripts/verify-superset-nova-identity.sh'
  [verify-compute]='bash scripts/verify-compute.sh'
  [verify-deploy-stage]='bash scripts/verify-deploy-stage.sh'
  [verify-secret-annotations]='python3 scripts/verify_secret_annotations.py'
  [verify-trino-system-catalog]='bash scripts/verify-trino-system-catalog.sh'
  [verify-trusted-engine]='bash scripts/verify-trusted-engine.sh'
  [verify-warehouse-scale]='bash scripts/verify-warehouse-scale.sh'
  [verify-cron-schedule]='bash scripts/verify-cronjobs.sh --max-age'
  [verify-metrics]='bash scripts/verify-metrics.sh'
  [verify-seaweedfs-ports]='bash scripts/verify-seaweedfs-ports.sh'
  [verify-s3-sts]='bash scripts/verify-s3-sts.sh'
  [verify-airflow-actions]='bash scripts/verify-airflow-actions.sh'
  [verify-operator-quiet]='bash scripts/verify-operator-quiet.sh'
  [verify-pod-security-baseline]='bash scripts/verify-pod-security.sh --level baseline --allow-namespace-level baseline # a baseline probe of the same pods; the namespace enforces restricted (#1095)'
  [verify-external-secrets]='bash scripts/verify-external-secrets.sh'
  [verify-storage-capacity]='bash scripts/verify-storage-capacity.sh'
)
# The tests only: `target=command` lines that replace entries of the table above.
if [ -n "${VERIFY_RELEASE_RECIPES:-}" ]; then
  while IFS='=' read -r key value; do
    [ -n "$key" ] && recipe["$key"]="$value"
  done <<< "$VERIFY_RELEASE_RECIPES"
fi

# verify-spark-driver-identity's subject is the demo's permanent SparkCluster (#1157, #493).
default_targets="verify-trino-auth verify-catalog-permissions verify-backups-all verify-streaming
verify-crds verify-access-sync-script verify-pins verify-contract verify-spark-config verify-restarts check-spark-runtimes-live
verify-operator-values verify-connections verify-maintenance verify-pod-security verify-cronjobs
verify-trino-tls verify-oidc-clients verify-mcp verify-mcp-tools verify-realm-admin verify-images verify-airflow-runs verify-catalog-workload-auth verify-workload-identity
verify-spark-demo-sandbox verify-spark-demo-query verify-spark-auth verify-spark-query verify-spark-janitor verify-spark-demo-security
verify-wap verify-product-path verify-cli verify-pipeline-validate verify-spark-pipeline verify-spark-no-platform-key verify-spark-guard verify-spark-driver-identity verify-spark-group-read verify-spark-group-write verify-superset-nova-identity verify-compute verify-deploy-stage
verify-secret-annotations verify-trino-system-catalog verify-trusted-engine verify-warehouse-scale verify-cron-schedule verify-metrics verify-seaweedfs-ports verify-s3-sts verify-airflow-actions verify-operator-quiet verify-pod-security-baseline verify-external-secrets verify-storage-capacity"
targets="${VERIFY_RELEASE_TARGETS:-$default_targets}"

# The Makefile passes these to its recipes; the recipes are called here now, so the same defaults
# apply (Makefile: NAMESPACE ?= stardelt, OPERATOR_DIR ?= ../operator, MAX_AGE_HOURS ?= 36,
# GHCR_SECRET ?= ghcr-creds, SECRET ?= $(GHCR_SECRET)). SECRET is not exported: only the
# verify-images recipe receives it from the Makefile, and other checks read SECRET as their own
# default (verify-airflow-runs: airflow-nova-api), so it is set for that one target below.
export NAMESPACE="${NAMESPACE:-stardelt}"
export NS="$NAMESPACE"
export OPERATOR_DIR="${OPERATOR_DIR:-../operator}"
export MAX_AGE_HOURS="${MAX_AGE_HOURS:-36}"
export GHCR_SECRET="${GHCR_SECRET:-ghcr-creds}"

# The kube-context guard and its banner: `_context` is the Makefile's preflight, so a direct run
# (`bash scripts/verify-release.sh`) needs it too. Under `make verify-release` it already ran.
if [ "${MAKELEVEL:-0}" = "0" ]; then
  make --no-print-directory -s _context || exit 1
fi

# One API-server gap looks like twenty failed checks (measured 02:23Z: 1/19). An API server that
# does not answer `/readyz` is not a release failure: nothing was checked, so the run is UNVERIFIED.
if ! kubectl get --raw /readyz >/dev/null 2>&1; then
  echo "verify-release: UNVERIFIED (the API server does not answer /readyz; nothing was checked)"
  exit 3
fi

# A check whose log carries one of these is retried once; one that still cannot reach the cluster
# exits 3 and is reported `?`, never ✗.
transient='ServiceUnavailable|the server is currently unable to handle the request|error dialing backend|unable to upgrade connection|port-forward .* never came up|connection refused|i/o timeout|TLS handshake timeout'

# A run takes 15–20 minutes and the fleet merges during it, so the commit is read once, before the
# checks start: the summary must name the tree the checks ran on, never the one HEAD points at when
# the run ends (stardelt/stardelt#1385).
COMMIT="$(git rev-parse --short HEAD)"
DIRTY="$(git status --porcelain | wc -l | tr -d ' ')"

logdir="${TMPDIR:-/tmp}/verify-release.$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$logdir"
passed=0 failed=0 unverified=0 total=0
for t in $targets; do
  total=$((total + 1))
  cmd="${recipe[$t]:-}"
  if [ -z "$cmd" ]; then
    printf '✗ %-28s      no recipe for this target: add it to platform/scripts/verify-release.sh\n' "$t"
    failed=$((failed + 1))
    continue
  fi
  start=$SECONDS
  code=0
  # SECRET reaches only verify-images (Makefile: `SECRET=$(SECRET) bash scripts/verify-images.sh`);
  # every other script keeps its own default for that name.
  extra=()
  if [ "$t" = verify-images ]; then
    extra=(SECRET="${SECRET:-$GHCR_SECRET}")
  fi
  # The Makefile's defaults for the person check in verify-trino-auth (`TRINO_VERIFY_USER ?= demo`,
  # `TRINO_VERIFY_USER_PASSWORD ?= demo`, stardelt/stardelt#1499): the release must not pass with
  # that check skipped, and a caller's own values still win.
  if [ "$t" = verify-trino-auth ]; then
    extra+=(TRINO_VERIFY_USER="${TRINO_VERIFY_USER:-demo}"
            TRINO_VERIFY_USER_PASSWORD="${TRINO_VERIFY_USER_PASSWORD:-demo}")
  fi
  for attempt in 1 2; do
    if env "${extra[@]}" bash -c "$cmd" > "$logdir/$t.log" 2>&1; then
      code=0
    else
      code=$?
      if [ "$attempt" -eq 1 ] && grep -Eq "$transient" "$logdir/$t.log"; then
        sleep 10
        continue
      fi
    fi
    break
  done
  case "$code" in
    0) printf '✓ %-28s %3ss\n' "$t" "$((SECONDS - start))"; passed=$((passed + 1)) ;;
    3) printf '? %-28s %3ss  UNVERIFIED (the check proved nothing)  %s\n' "$t" "$((SECONDS - start))" "$logdir/$t.log"
       unverified=$((unverified + 1)) ;;
    *) printf '✗ %-28s %3ss  %s\n' "$t" "$((SECONDS - start))" "$logdir/$t.log"
       failed=$((failed + 1)) ;;
  esac
done
NOW="$(git rev-parse --short HEAD)"
MOVED=""
[ "$NOW" = "$COMMIT" ] || MOVED=" · the checkout moved to $NOW during the run"
[ "$DIRTY" = "0" ] || MOVED="$MOVED · $DIRTY uncommitted path(s) at the start"
echo "commit $COMMIT$MOVED · $passed/$total passed · $unverified unverified · logs in $logdir"
[ "$failed" -eq 0 ] || exit 1
[ "$unverified" -eq 0 ] || exit 3
exit 0
