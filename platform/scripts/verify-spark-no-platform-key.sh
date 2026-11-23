#!/usr/bin/env bash
# The platform's storage key does not reach code a person runs on the shared Spark Connect
# server: not the driver's environment, not the executors' configuration, not a UDF, and not an
# executor pod the admission policy admits (stardelt/stardelt#75, question 3; #427).
#
#   make verify-spark-no-platform-key   (NS=… to override the namespace, TIMEOUT=240s the wait)
#
# The check is the acceptance gate of #75's storage half: it measures the four paths the key
# travels today and fails while any of them carries it, so the removal slice is judged by it.
#   1. driver-env     — env entries of Deployment spark-connect filled from Secret
#                       stardelt-s3-creds (name `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`);
#   2. executor-conf  — `spark.kubernetes.executor.secretKeyRef.*` keys in ConfigMap
#                       spark-connect-config's spark-defaults.conf;
#   3. udf-env        — a UDF on an executor reports which of the three AWS variables it sees;
#   4. pod-guard      — ValidatingAdmissionPolicy spark-connect-executor-pods still admits an
#                       executor pod that reads Secret stardelt-s3-creds (n rules matching it).
#
# Only NAMES are printed, never values: the driver env names, the configuration keys and the
# variable names a UDF finds. A value of the key in any line of the output is a defect.
#
# The pod carries all three JupyterHub labels because two NetworkPolicies must both admit it, and
# only their union gives a client pod a working path (measured 2026-11-19):
#   * `component: singleuser-server` is spark-connect-ingress's admission rule for 15002
#     (manifests/spark.yaml);
#   * the chart's `singleuser` policy selects `app: jupyterhub, component: singleuser-server,
#     release: jupyterhub` and is the only egress policy that allows DNS (53 to CoreDNS) and
#     15002 on that label set. With `component` alone the pod is selected by
#     `notebook-public-egress` only — `0.0.0.0/0` except the private ranges — so `getent hosts`
#     fails and the connect never happens, and the check could only ever print UNVERIFIED.
# It is the notebook image, not Spark's: pyspark, pandas and pyarrow are already in it and the
# Spark image ships no client. The image comes from helm-values/jupyterhub.yaml, never a literal.
#
# PASS = all four paths are free of the key. FAIL = at least one carries it. UNVERIFIED (exit 3)
# = the client pod never started or never reached the server, so the UDF path was not measured.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
POD=verify-spark-no-platform-key
TIMEOUT="${TIMEOUT:-240}s"

command -v kubectl >/dev/null 2>&1 || { echo "verify-spark-no-platform-key: UNVERIFIED (kubectl not found)"; exit 3; }
command -v python3 >/dev/null 2>&1 || { echo "verify-spark-no-platform-key: UNVERIFIED (python3 not found)"; exit 3; }
command -v jq >/dev/null 2>&1 || { echo "verify-spark-no-platform-key: UNVERIFIED (jq not found)"; exit 3; }

# The client is the spawn image the hub runs; `scripts/check-notebook-image-pin.sh` keeps the
# two in step. The `dev@sha256:` tag is the only one in that file.
client_digest="$(grep -m1 -E '^[[:space:]]*tag:[[:space:]]*"dev@sha256:[0-9a-f]{64}"' helm-values/jupyterhub.yaml | sed -E 's/.*"dev@(sha256:[0-9a-f]{64})".*/\1/')"
[ -n "$client_digest" ] || { echo "verify-spark-no-platform-key: UNVERIFIED (helm-values/jupyterhub.yaml names no dev@sha256 notebook image)"; exit 3; }
CLIENT_IMAGE="ghcr.io/stardelt/jupyter-singleuser:dev@${client_digest}"

# No pod is left behind, on success, on FAIL and on Ctrl-C.
trap 'kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true' EXIT
trap 'kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true; exit 130' INT

# --- the three configuration facts, read before the probe so they describe the state it ran in --
# A fact the cluster cannot answer is `unreadable`, never `none`: `none` is the PASS value, and a
# read that failed must not pass for a path that is free of the key.
driver_env="$(
  kubectl -n "$NS" get deployment spark-connect -o json |
    jq -r '[.spec.template.spec.containers[].env[]? | select(.valueFrom.secretKeyRef.name? == "stardelt-s3-creds") | .name] | join(",")'
)" || driver_env=unreadable
[ -n "$driver_env" ] || driver_env=none

executor_conf="$(
  kubectl -n "$NS" get configmap spark-connect-config -o json |
    jq -r '.data["spark-defaults.conf"] // ""' |
    awk '$1 ~ /^spark\.kubernetes\.executor\.secretKeyRef\./ {print $1}' |
    paste -sd, -
)" || executor_conf=unreadable
[ -n "$executor_conf" ] || executor_conf=none

# The guard is part of the boundary: while it admits a pod that reads the Secret, a driver can
# carry the key even after paths 1 and 2 are gone, so a missing policy is a FAIL, not a skip.
if policy_json="$(kubectl get validatingadmissionpolicy spark-connect-executor-pods -o json 2>&1)"; then
  guard_rules="$(printf '%s\n' "$policy_json" | jq '[.spec.validations[].expression | select(contains("stardelt-s3-creds"))] | length')" || guard_rules=x
  if [ "${guard_rules:-x}" = x ]; then
    pod_guard=unreadable
  elif [ "$guard_rules" -gt 0 ]; then
    pod_guard="admits Secret stardelt-s3-creds (${guard_rules} rules)"
  else
    pod_guard="admits no Secret"
  fi
else
  pod_guard=missing
fi

# The facts are read here, before the probe, so they describe the state the probe ran in; they
# are printed with the udf-env line once the probe's answer is known (the four judged lines).

# The labels are the egress and ingress admission rules for 15002, and both policies must admit
# the pod (see the header): the chart's `singleuser` policy for DNS + 15002, and
# spark-connect-ingress for `component: singleuser-server`. No `stardelt.io/group.visitors` label
# may be set (that would be the public demo's pod, which the policy refuses).
if ! out="$(kubectl -n "$NS" apply -f - 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${NS}
  labels:
    app.kubernetes.io/name: verify-spark-no-platform-key
    app: jupyterhub
    release: jupyterhub
    component: singleuser-server
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    seccompProfile: { type: RuntimeDefault }
  containers:
    - name: client
      image: ${CLIENT_IMAGE}
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: { drop: ["ALL"] }
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
          from pyspark.sql import SparkSession
          from pyspark.sql.functions import udf

          s = SparkSession.builder.remote("sc://spark-connect.stardelt.svc.cluster.local:15002").getOrCreate()

          @udf("string")
          def seen(_):
              import os
              names = [k for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN") if os.environ.get(k)]
              return ",".join(names) or "none"

          rows = s.range(0, 8, 1, 4).select(seen("id").alias("v")).distinct().collect()
          print("udf-env=" + ";".join(sorted(r.v for r in rows)), flush=True)
EOF
)"; then
  echo "verify-spark-no-platform-key: UNVERIFIED (the client pod could not be created: $(printf '%s\n' "$out" | tail -n 1))"
  exit 3
fi

# Spark's own first line when no executor accepts work; anything else is a real client error.
spark_noise='^[[:space:]]*([0-9]{2}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} (INFO|DEBUG)|[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}[^ ]*[[:space:]]+(INFO|DEBUG))'

fail() {  # <logs>: the pod's last log lines, then the judged FAIL line
  local logs="$1" first_line unreachable
  printf '%s\n' "$logs" | tail -n 40
  # The 2026-11-19 outage printed "Initial job has not accepted any resources" and ended in
  # "ExecuteGrpcResponseSender: Deadline reached"; our own probe line is not a reason.
  first_line="$(printf '%s\n' "$logs" \
    | grep -vE "$spark_noise" \
    | grep -vE '^udf-env=.*$' \
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
    echo "verify-spark-no-platform-key: UNVERIFIED (the client pod did not reach spark-connect.stardelt.svc.cluster.local:15002: ${unreachable})"
    exit 3
  fi
  echo "verify-spark-no-platform-key: FAIL (the shared Connect server did not run a job: ${first_line})"
  exit 1
}

# A pod that never starts says nothing about the UDF path: UNVERIFIED, never PASS or FAIL
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
    echo "verify-spark-no-platform-key: UNVERIFIED (the client pod stayed Pending: ${reason:-no container started})"
    exit 3
  fi
  fail "$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
fi

# --- judge the four paths ---------------------------------------------------------------------
logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
udf_env="$(printf '%s\n' "$logs" | grep -m1 -E '^udf-env=' | sed -E 's/^udf-env=//' || true)"
[ -n "$udf_env" ] || udf_env=unreadable

echo "driver-env=${driver_env}"
echo "executor-conf=${executor_conf}"
echo "udf-env=${udf_env}"
echo "pod-guard=${pod_guard}"

failing=()
[ "$driver_env" = none ] || failing+=("driver-env=${driver_env}")
[ "$executor_conf" = none ] || failing+=("executor-conf=${executor_conf}")
[ "$udf_env" = none ] || failing+=("udf-env=${udf_env}")
[ "$pod_guard" = "admits no Secret" ] || failing+=("pod-guard=${pod_guard}")

if [ "${#failing[@]}" -eq 0 ]; then
  echo "verify-spark-no-platform-key: PASS (the platform storage key is on none of the four paths: driver, executors, user code, executor admission)"
  exit 0
fi
printf -v failing_list '%s, ' "${failing[@]}"
echo "verify-spark-no-platform-key: FAIL (the platform storage key reaches a person's code: ${failing_list%, })"
exit 1
