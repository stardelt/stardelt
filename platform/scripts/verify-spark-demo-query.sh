#!/usr/bin/env bash
# One session through the public demo's Spark sandbox: range(10).count() from a client pod that
# carries the visitor's labels (stardelt/stardelt#201).
#
#   make verify-spark-demo-query     (NS=… for the platform namespace, DEMO_NS=… for the sandbox,
#                                     TIMEOUT=240s the wait)
#
# Why: `make verify-spark-demo-sandbox` judges what the sandbox contains — Pod Security, the
# ServiceAccount, the token mount, its egress. None of that is a session: the policies could admit
# nobody, or the server could accept a connection and fail every job, and every probe would still
# be green (the stardelt/stardelt#234 shape). This is the sandbox's first session.
#
# The pod carries four labels because two admission rules must both admit it:
#   * the chart's `singleuser` policy (app: jupyterhub, release: jupyterhub,
#     component: singleuser-server) — the only egress policy that gives the pod DNS, and the rule
#     that admits 15002 in the sandbox namespace (helm-values/jupyterhub.yaml, the
#     stardelt-demo-spark rule);
#   * the sandbox's ingress (component: singleuser-server, stardelt.io/group.visitors: "true",
#     manifests/spark-demo-sandbox.yaml).
# The client image is the notebook image from helm-values/jupyterhub.yaml, never a literal: pyspark
# ships there, and the Spark image ships no client.
#
# PASS = the sandbox ran the job. FAIL = it did not. UNVERIFIED (exit 3) = the client pod never
# started or never reached the server, so nothing about the query path was measured — on
# 2026-11-19 a NetworkPolicy that drops the packet and a sandbox that is down looked identical.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
DEMO_NS="${DEMO_NS:-stardelt-demo-spark}"
POD=verify-spark-demo-query
TIMEOUT="${TIMEOUT:-240s}"
HOST="spark-demo.${DEMO_NS}.svc.cluster.local"
PORT=15002

command -v kubectl >/dev/null 2>&1 || { echo "verify-spark-demo-query: UNVERIFIED (kubectl not found)"; exit 3; }
command -v python3 >/dev/null 2>&1 || { echo "verify-spark-demo-query: UNVERIFIED (python3 not found)"; exit 3; }

if ! kubectl -n "$DEMO_NS" get deployment spark-demo >/dev/null 2>&1; then
  echo "verify-spark-demo-query: UNVERIFIED (the demo sandbox is not installed — make install-spark-demo)"
  exit 3
fi

# The client is the spawn image the hub runs; `scripts/check-notebook-image-pin.sh` keeps the
# two in step. The `dev@sha256:` tag is the only one in that file.
client_digest="$(grep -m1 -E '^[[:space:]]*tag:[[:space:]]*"dev@sha256:[0-9a-f]{64}"' helm-values/jupyterhub.yaml | sed -E 's/.*"dev@(sha256:[0-9a-f]{64})".*/\1/')"
[ -n "$client_digest" ] || { echo "verify-spark-demo-query: UNVERIFIED (helm-values/jupyterhub.yaml names no dev@sha256 notebook image)"; exit 3; }
CLIENT_IMAGE="ghcr.io/stardelt/jupyter-singleuser:dev@${client_digest}"

# No pod is left behind, on success, on FAIL and on Ctrl-C.
trap 'kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true' EXIT
trap 'kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true; exit 130' INT

# The four labels are the two admission rules (see the header): the chart's `singleuser` egress
# policy for DNS and 15002, and the sandbox's ingress for a visitor's pod.
if ! out="$(kubectl -n "$NS" apply -f - 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${NS}
  labels:
    app.kubernetes.io/name: verify-spark-demo-query
    app: jupyterhub
    release: jupyterhub
    component: singleuser-server
    stardelt.io/group.visitors: "true"
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
          s = SparkSession.builder.remote("sc://${HOST}:${PORT}").getOrCreate()
          print("session in %.0fs" % (time.time() - t), flush=True)
          print("range10=", s.range(10).count(), flush=True)
          print("done in %.0fs" % (time.time() - t), flush=True)
EOF
)"; then
  echo "verify-spark-demo-query: UNVERIFIED (the client pod could not be created: $(printf '%s\n' "$out" | tail -n 1))"
  exit 3
fi

# Spark's own first line (INFO/DEBUG noise) and this script's own progress lines are never a
# reason to call the sandbox broken; the judged line is the first one that is neither.
spark_noise='^[[:space:]]*([0-9]{2}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} (INFO|DEBUG)|[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}[^ ]*[[:space:]]+(INFO|DEBUG))'

fail() {  # <logs>: the pod's last log lines, then the judged FAIL line
  local logs="$1" first_line unreachable
  printf '%s\n' "$logs" | tail -n 40
  first_line="$(printf '%s\n' "$logs" \
    | grep -vE "$spark_noise" \
    | grep -vE '^(session in [0-9]+s|range10= [0-9]+|done in [0-9]+s)$' \
    | grep -m1 -E '[^[:space:]]' || true)"
  [ -n "$first_line" ] || first_line="no job result within ${TIMEOUT}"
  # A check that could not reach the server is UNVERIFIED, never FAIL: a NetworkPolicy that drops
  # the packet and a sandbox that is down look identical from inside the client.
  unreachable="$(printf '%s\n' "$logs" |
    grep -m1 -E 'UNAVAILABLE|Connection refused|timed out|Name or service not known|Temporary failure in name resolution' || true)"
  if [ -z "$unreachable" ] && [ "$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null)" = "Running" ]; then
    unreachable="$(kubectl -n "$NS" exec "$POD" -- python3 -c '
import socket, sys
try:
    socket.create_connection(("'"${HOST}"'", '"${PORT}"'), 5).close()
except Exception as e:
    print(repr(e))
    sys.exit(1)
' 2>&1 || true)"
    unreachable="$(printf '%s\n' "$unreachable" | grep -v '^command terminated with exit code' | grep -m1 -E '[^[:space:]]' || true)"
  fi
  if [ -n "$unreachable" ]; then
    echo "verify-spark-demo-query: UNVERIFIED (the client pod did not reach ${HOST}:${PORT}: ${unreachable})"
    exit 3
  fi
  echo "verify-spark-demo-query: FAIL (the sandbox did not run a job: ${first_line})"
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
    echo "verify-spark-demo-query: UNVERIFIED (the client pod stayed Pending: ${reason:-no container started})"
    exit 3
  fi
  fail "$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
fi

logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
printf '%s\n' "$logs" | grep -E '^(session in|range10=|done in)' || true
if grep -qx 'range10= 10' <<<"$logs"; then
  echo "verify-spark-demo-query: PASS (the demo sandbox ran range(10) = 10)"
  exit 0
fi
fail "$logs"
