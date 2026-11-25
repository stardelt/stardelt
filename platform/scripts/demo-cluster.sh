#!/usr/bin/env bash
# The demo's SparkCluster (#493 point 6a): owned by group demo, visible to visitors, Running.
#
#   --check  read-only: the cluster exists, is Running, and carries its driver identity
#   --on     apply the SparkCluster and wait for Running (no group, Secret or grant: like
#            `make install-spark-demo`, it takes no YES=1)
#   --off    delete it (needs YES=1; the finalizer prunes the driver ServiceAccount)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NAMESPACE:-stardelt}"
NAME=demo
SA=spark-cluster-driver-demo
YES="${YES:-0}"
mode=check
case "${1:-}" in
  --check) mode=check ;;
  --on) mode=on ;;
  --off) mode=off ;;
  "") ;;
  *) echo "demo-cluster: unexpected argument '$1' (accepted: --check, --on, --off)" >&2; exit 2 ;;
esac

document() {
  cat <<YAML
apiVersion: stardelt.io/v1alpha1
kind: SparkCluster
metadata:
  name: ${NAME}
  namespace: ${NS}
  labels: { app.kubernetes.io/part-of: stardelt, stardelt.io/demo: "true" }
spec:
  displayName: The public demo's Spark
  owner: { group: demo }
  viewers: [visitors]
  runtime: spark-4.1-py3.13
  size: small
  maxExecutors: 2
  sparkConf:
    spark.kubernetes.executor.request.cores: "250m"
  idleStopMinutes: 1440
  state: Running
YAML
}

if [ "$mode" = on ]; then
  document | kubectl apply -f - >/dev/null
  phase=""
  for _ in $(seq 1 60); do
    phase="$(kubectl -n "$NS" get sparkcluster "$NAME" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    case "$phase" in
      Running)
        identity="$(kubectl -n "$NS" get sparkcluster "$NAME" -o jsonpath='{.status.workloadIdentity}' 2>/dev/null || true)"
        echo "✓ SparkCluster $NAME is Running ($SA, $identity)"
        exit 0 ;;
      Failed)
        echo "SparkCluster $NAME failed: $(kubectl -n "$NS" get sparkcluster "$NAME" -o jsonpath='{.status.message}' 2>/dev/null || true)" >&2
        exit 1 ;;
    esac
    sleep 5
  done
  echo "SparkCluster $NAME did not reach Running in 300 s (last phase: ${phase:-?})" >&2
  exit 1
fi

if [ "$mode" = off ]; then
  if [ "$YES" != 1 ]; then
    echo "would: delete SparkCluster $NAME in $NS; nothing was changed — re-run with YES=1" >&2
    exit 1
  fi
  kubectl -n "$NS" delete sparkcluster "$NAME" --ignore-not-found --wait=false >/dev/null
  echo "✓ SparkCluster $NAME deleted"
  exit 0
fi

# --check: read-only.
if ! command -v kubectl >/dev/null 2>&1; then
  echo "UNVERIFIED (kubectl not found)"
  exit 3
fi
if ! document="$(kubectl -n "$NS" get sparkcluster "$NAME" -o json 2>/dev/null)"; then
  echo "✗ the SparkCluster $NAME does not exist — run make demo-cluster"
  exit 1
fi
phase="$(jq -r '.status.phase // ""' <<<"$document")"
sa="$(jq -r '.status.driverServiceAccount // ""' <<<"$document")"
identity="$(jq -r '.status.workloadIdentity // ""' <<<"$document")"
endpoint="$(jq -r '.status.endpoint // ""' <<<"$document")"
executor_request="$(jq -r '.spec.sparkConf["spark.kubernetes.executor.request.cores"] // ""' <<<"$document")"
uid="$(kubectl -n "$NS" get serviceaccount "$SA" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
expected_endpoint="sc://spc-$NAME-server.$NS.svc.cluster.local:15002"

problems=()
[ "$phase" = Running ] || problems+=("the phase is ${phase:-?}, not Running")
[ "$sa" = "$SA" ] || problems+=("the driver ServiceAccount is ${sa:-?}, not $SA")
case "$identity" in
  kubernetes~*) ;;
  *) problems+=("the workload identity is ${identity:-?}, not kubernetes~<uid>") ;;
esac
[ "$endpoint" = "$expected_endpoint" ] || problems+=("the endpoint is ${endpoint:-?}, not $expected_endpoint")
[ "$executor_request" = 250m ] || problems+=("the executor's CPU request is ${executor_request:-unset}, not 250m")
if [ -z "$uid" ]; then
  problems+=("the ServiceAccount $SA does not exist")
elif [ "${identity#kubernetes~}" != "$uid" ]; then
  problems+=("the workload identity ${identity:-?} is not the uid of $SA ($uid)")
fi

if [ "${#problems[@]}" -ne 0 ]; then
  printf '✗ %s\n' "$(IFS='; '; echo "${problems[*]}")"
  exit 1
fi
echo "✓ SparkCluster $NAME is Running ($SA, $identity; executor request 250m)"
