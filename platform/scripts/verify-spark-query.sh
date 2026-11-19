#!/usr/bin/env bash
# One query through the shared Spark Connect server: a session, range(10).count() and
# range(1000).repartition(4).count() — the second runs the ExecutePlan path with an exchange.
#
#   make verify-spark-query        (NS=… to override the namespace, TIMEOUT=240s the wait)
#
# Catches the 2026-11-18/19 outage (stardelt/stardelt#215): the Connect driver's executor
# ConfigMap was deleted, the executors hung in FailedMount, every query ended in
# "Initial job has not accepted any resources", and every other verify-* target still passed
# because they read what the pods are, not what they do.
#
# The pod carries `component: singleuser-server` because NetworkPolicy spark-connect-ingress
# admits port 15002 only from a pod with that label (manifests/spark.yaml). It is the notebook
# image, not Spark's: pyspark, pandas and pyarrow are already in it and the Spark image ships
# no client. The image comes from helm-values/jupyterhub.yaml, never from a literal here.
#
# PASS = the server ran both jobs. FAIL = it did not. UNVERIFIED (exit 3) = the client pod never
# started or never reached the server, so nothing about the query path was measured.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
POD=verify-spark-query
TIMEOUT="${TIMEOUT:-240}s"

command -v kubectl >/dev/null 2>&1 || { echo "verify-spark-query: UNVERIFIED (kubectl not found)"; exit 3; }
command -v python3 >/dev/null 2>&1 || { echo "verify-spark-query: UNVERIFIED (python3 not found)"; exit 3; }

# The client is the spawn image the hub runs; `scripts/check-notebook-image-pin.sh` keeps the
# two in step. The `dev@sha256:` tag is the only one in that file.
client_digest="$(grep -m1 -E '^[[:space:]]*tag:[[:space:]]*"dev@sha256:[0-9a-f]{64}"' helm-values/jupyterhub.yaml | sed -E 's/.*"dev@(sha256:[0-9a-f]{64})".*/\1/')"
[ -n "$client_digest" ] || { echo "verify-spark-query: UNVERIFIED (helm-values/jupyterhub.yaml names no dev@sha256 notebook image)"; exit 3; }
CLIENT_IMAGE="ghcr.io/stardelt/jupyter-singleuser:dev@${client_digest}"

# No pod is left behind, on success, on FAIL and on Ctrl-C.
trap 'kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true' EXIT
trap 'kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true; exit 130' INT

# The `component: singleuser-server` label is the NetworkPolicy's admission rule for 15002, and
# no `stardelt.io/group.visitors` label may be set (that would be the public demo's pod, which
# the policy refuses).
if ! out="$(kubectl -n "$NS" apply -f - 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${NS}
  labels:
    app.kubernetes.io/name: verify-spark-query
    component: singleuser-server
spec:
  restartPolicy: Never
  containers:
    - name: client
      image: ${CLIENT_IMAGE}
      env:
        - name: HOME
          value: /tmp
      resources:
        requests: { cpu: 100m, memory: 256Mi }
        limits: { cpu: 1, memory: 1Gi }
      command:
        - python3
        - -c
        - |
          import time
          from pyspark.sql import SparkSession
          t = time.time()
          s = SparkSession.builder.remote("sc://spark-connect.stardelt.svc.cluster.local:15002").getOrCreate()
          print("session in %.0fs" % (time.time() - t), flush=True)
          print("range10=", s.range(10).count(), flush=True)
          print("rep1000=", s.range(1000).repartition(4).count(), flush=True)
          print("done in %.0fs" % (time.time() - t), flush=True)
EOF
)"; then
  echo "verify-spark-query: UNVERIFIED (the client pod could not be created: $(printf '%s\n' "$out" | tail -n 1))"
  exit 3
fi

# Spark's own first line when no executor accepts work; anything else is a real client error.
spark_noise='^[[:space:]]*([0-9]{2}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} (INFO|DEBUG)|[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}[^ ]*[[:space:]]+(INFO|DEBUG))'

fail() {  # <logs>: the pod's last log lines, then the judged FAIL line
  local logs="$1" first_line unreachable
  printf '%s\n' "$logs" | tail -n 40
  # The 2026-11-19 outage printed "Initial job has not accepted any resources" and ended in
  # "ExecuteGrpcResponseSender: Deadline reached"; our own progress lines are not a reason.
  first_line="$(printf '%s\n' "$logs" \
    | grep -vE "$spark_noise" \
    | grep -vE '^(session in [0-9]+s|range10= [0-9]+|rep1000= [0-9]+|done in [0-9]+s)$' \
    | grep -m1 -E '[^[:space:]]' || true)"
  [ -n "$first_line" ] || first_line="no job result within ${TIMEOUT}"
  # A check that could not reach the server is UNVERIFIED, never FAIL: on 2026-11-19 the pod
  # network refused 15002 from every pod while the server was healthy (stardelt/stardelt#60).
  unreachable="$(printf '%s\n' "$logs" |
    grep -m1 -E 'UNAVAILABLE|Connection refused|Name or service not known|Temporary failure in name resolution' || true)"
  if [ -z "$unreachable" ] && [ "$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null)" = "Running" ]; then
    unreachable="$(kubectl -n "$NS" exec "$POD" -- python3 -c '
import socket, sys
try:
    socket.create_connection(("spark-connect.stardelt.svc.cluster.local", 15002), 5).close()
except Exception as e:
    print(repr(e))
    sys.exit(1)
' 2>&1 || true)"
    unreachable="$(printf '%s\n' "$unreachable" | grep -v '^command terminated with exit code' | grep -m1 -E '[^[:space:]]' || true)"
  fi
  if [ -n "$unreachable" ]; then
    echo "verify-spark-query: UNVERIFIED (the client pod did not reach spark-connect.stardelt.svc.cluster.local:15002: ${unreachable})"
    exit 3
  fi
  echo "verify-spark-query: FAIL (the shared Connect server did not run a job: ${first_line})"
  exit 1
}

# A pod that never starts says nothing about the query path: UNVERIFIED, never PASS or FAIL
# (stardelt/stardelt#60 — new pods timed out on DNS after a control-plane restart).
if ! kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$POD" --timeout="$TIMEOUT" >/dev/null 2>&1; then
  mapfile -t state < <(kubectl -n "$NS" get pod "$POD" -o json 2>/dev/null | python3 -c '
import json, sys
try:
    status = json.load(sys.stdin).get("status", {})
except Exception:
    status = {}
print(status.get("phase", "Unknown"))
waiting = (status.get("containerStatuses") or [{}])[0].get("state", {}).get("waiting") or {}
scheduled = [c for c in status.get("conditions") or []
             if c.get("type") == "PodScheduled" and c.get("status") != "True"]
reason = waiting.get("message") or waiting.get("reason") or ""
if not reason and scheduled:
    reason = scheduled[0].get("message", "")
print((reason or "").strip())
')
  phase="${state[0]:-Unknown}"
  reason="${state[1]:-}"
  if [ "$phase" = "Pending" ]; then
    echo "verify-spark-query: UNVERIFIED (the client pod stayed Pending: ${reason:-no container started})"
    exit 3
  fi
  fail "$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
fi

logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
printf '%s\n' "$logs" | grep -E '^(session in|range10=|rep1000=|done in)' || true
if grep -qx 'range10= 10' <<<"$logs" && grep -qx 'rep1000= 1000' <<<"$logs"; then
  echo "verify-spark-query: PASS (range(10)=10, range(1000).repartition(4)=1000)"
  exit 0
fi
fail "$logs"
