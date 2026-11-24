#!/usr/bin/env bash
# One Spark Connect workload through the shared server: a session, an aggregate, a shuffle, a
# join and a write — the fourth Sizing workload (#53 decision 1(d)).
#
#   make perf-spark              (NS=… to override the namespace, TIMEOUT=240s the wait,
#                                 PERF_SPARK_ROWS=5000000 the row count)
#
# It is the same client pod `scripts/verify-spark-query.sh` builds, so the same admission rules
# apply: all three JupyterHub labels, because two NetworkPolicies must both admit it, and only
# their union gives a client pod a working path to 15002 (see that script's header for the
# measurement of 2026-11-19). The image comes from helm-values/jupyterhub.yaml, never a literal.
#
# The data is generated (`spark.range`): a bare session on the shared server has only
# `spark_catalog`, so `gold.shop` is not addressable without the helper's per-catalog config
# (platform/docs/spark.md, "Catalog wiring"). An Iceberg row belongs to the session-identity
# work. The write goes to `/tmp/perf-write` inside the Connect server's pod, overwritten each
# run — never `saveAsTable`, because the session catalog is shared with every notebook there.
#
# PASS = all five operations finished. FAIL = one did not (the verdict names it). UNVERIFIED
# (exit 3) = the client pod never started or never reached the server, so nothing about the
# workload was measured. The JSON goes to $PERF_OUT_DIR (default ~/.cache/stardelt-perf).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
POD=perf-spark
TIMEOUT="${TIMEOUT:-240}s"

command -v kubectl >/dev/null 2>&1 || { echo "perf-spark: UNVERIFIED (kubectl not found)"; exit 3; }
command -v python3 >/dev/null 2>&1 || { echo "perf-spark: UNVERIFIED (python3 not found)"; exit 3; }

# The client is the spawn image the hub runs; `scripts/check-notebook-image-pin.sh` keeps the
# two in step. The `dev@sha256:` tag is the only one in that file.
client_digest="$(grep -m1 -E '^[[:space:]]*tag:[[:space:]]*"dev@sha256:[0-9a-f]{64}"' helm-values/jupyterhub.yaml | sed -E 's/.*"dev@(sha256:[0-9a-f]{64})".*/\1/')"
[ -n "$client_digest" ] || { echo "perf-spark: UNVERIFIED (helm-values/jupyterhub.yaml names no dev@sha256 notebook image)"; exit 3; }
CLIENT_IMAGE="ghcr.io/stardelt/jupyter-singleuser:dev@${client_digest}"

# No pod is left behind, on success, on FAIL and on Ctrl-C.
trap 'kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true' EXIT
trap 'kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true; exit 130' INT

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
    app.kubernetes.io/name: perf-spark
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
        - name: PERF_SPARK_ROWS
          value: "${PERF_SPARK_ROWS:-5000000}"
      resources:
        requests: { cpu: 100m, memory: 256Mi }
        limits: { cpu: 1, memory: 1Gi }
      command:
        - python3
        - -c
        - |
          import json, os, time, uuid
          from pyspark.sql import SparkSession

          rows = int(os.environ.get("PERF_SPARK_ROWS") or 5000000)
          out = []
          def timed(name, fn, count):
              t = time.time()
              try:
                  value = fn()
                  out.append({"id": name, "ms": int((time.time() - t) * 1000), "rows": count, "ok": True})
              except Exception as e:                      # recorded, judged below; never a crash
                  out.append({"id": name, "ms": int((time.time() - t) * 1000), "rows": 0,
                              "ok": False, "error": repr(e)[:300]})
              print("perf-row: %s %dms" % (name, out[-1]["ms"]), flush=True)

          t0 = time.time()
          s = SparkSession.builder.remote(
              "sc://spark-connect.stardelt.svc.cluster.local:15002").getOrCreate()
          out.append({"id": "session", "ms": int((time.time() - t0) * 1000), "rows": 1, "ok": True})
          print("perf-row: session %dms" % out[-1]["ms"], flush=True)

          view = "perf_r_" + uuid.uuid4().hex[:8]          # unique: the server's temp views are shared
          s.range(rows).createOrReplaceTempView(view)
          timed("aggregate", lambda: s.sql(f"SELECT count(*), sum(id) FROM {view}").collect()[0][0], rows)
          timed("shuffle", lambda: s.sql(
              f"SELECT id % 1000 AS k, count(*) c FROM {view} GROUP BY 1 ORDER BY c DESC LIMIT 3").count(), rows)
          timed("join", lambda: s.sql(
              f"SELECT count(*) FROM {view} a JOIN (SELECT id FROM {view} ORDER BY id LIMIT {rows // 50}) b "
              "ON a.id = b.id").collect()[0][0], rows // 50)
          timed("write", lambda: s.range(max(rows // 5, 1)).write.mode("overwrite")
                .parquet("/tmp/perf-write"), max(rows // 5, 1))
          s.catalog.dropTempView(view)

          doc = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                 "server": "spark-connect.stardelt.svc.cluster.local:15002",
                 "rows": rows, "rowsPerSecond": round(rows / (max(r["ms"] for r in out) / 1000.0), 0)
                 if out else 0, "operations": out}
          print("perf-json: " + json.dumps(doc), flush=True)
          bad = [r["id"] for r in out if not r["ok"]]
          print("perf-verdict: " + ("PASS" if not bad else "FAIL " + ",".join(bad)), flush=True)
EOF
)"; then
  echo "perf-spark: UNVERIFIED (the client pod could not be created: $(printf '%s\n' "$out" | tail -n 1))"
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
    | grep -vE '^(perf-(row|json|verdict): .*)$' \
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
    echo "perf-spark: UNVERIFIED (the client pod did not reach spark-connect.stardelt.svc.cluster.local:15002: ${unreachable})"
    exit 3
  fi
  echo "perf-spark: FAIL (the shared Connect server did not run a job: ${first_line})"
  exit 1
}

# A pod that never starts says nothing about the workload: UNVERIFIED, never PASS or FAIL
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
    echo "perf-spark: UNVERIFIED (the client pod stayed Pending: ${reason:-no container started})"
    exit 3
  fi
  fail "$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
fi

logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
printf '%s\n' "$logs" | grep -E '^perf-(row|json|verdict):' || true
json="$(printf '%s\n' "$logs" | sed -n 's/^perf-json: //p' | head -n 1)"
if [ -z "$json" ]; then
  echo "perf-spark: UNVERIFIED (the pod printed no perf-json line: $(printf '%s\n' "$logs" | grep -vE "$spark_noise" | grep -m1 -E '[^[:space:]]' || echo "phase $(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo Unknown)"))"
  exit 3
fi
umask 077
out="${PERF_OUT_DIR:-$HOME/.cache/stardelt-perf}/spark-$(date -u +%Y-%m-%dT%H:%M:%SZ).json"
mkdir -p "$(dirname "$out")"
printf '%s\n' "$json" > "$out"
if grep -qx 'perf-verdict: PASS' <<<"$logs"; then
  # argv, not stdin: one command has one stdin (L4).
  read -r ops rows rps < <(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(len(d.get("operations") or []), d.get("rows", 0), d.get("rowsPerSecond", 0))' "$out")
  echo "perf-spark: PASS (${ops} operations, ${rows} rows, ${rps} rows/s, JSON at ${out})"
  exit 0
fi
printf '%s\n' "$logs" | grep -E '^perf-verdict:' || echo "perf-spark: FAIL (the pod printed no perf-verdict line)"
exit 1
