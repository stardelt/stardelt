#!/usr/bin/env bash
# A SparkCluster is up: the driver's own identity, its admission guards and a live endpoint
# (platform#1487, operator#773, #774, #775).
#
#   NS=stardelt bash scripts/verify-spark-cluster.sh
#
# Creates one `small` SparkCluster named `verify-spark-cluster`, polls its phase and asserts the
# ten things the phase alone has lied about before (operator#774): the endpoint, the server
# pod's readiness and identity, the driver's ServiceAccount/Role/RoleBinding, the two admission
# policies with their bindings, the ingress NetworkPolicy, a TLS-clean server log, the executor
# path and the catalogue.
# The CR and the objects it owns are deleted on every exit path — the trap is the contract.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
NAME=verify-spark-cluster
NP="spc-${NAME}-ingress"
POD="spc-${NAME}-server"
EXEC="verify-spark-cluster-exec"
RUNTIME=spark-4.1-py3.11
CATALOGUE=stardelt-spark-runtimes
problems=0
command -v kubectl >/dev/null 2>&1 || { echo "UNVERIFIED (kubectl not found)"; exit 3; }

# The reconciler refuses a runtime the catalogue does not carry, and the phase would then be
# `Failed` for a reason that says nothing about the SparkCluster path.
if ! keys="$(kubectl -n "$NS" get configmap "$CATALOGUE" -o json 2>/dev/null \
    | python3 -c 'import json, sys; print("\n".join(sorted(json.load(sys.stdin).get("data") or {})))')"; then
  echo "UNVERIFIED (ConfigMap $CATALOGUE is not installed — run make install-spark)"; exit 3
fi
if ! grep -qx "$RUNTIME" <<<"$keys"; then
  echo "the catalogue holds these runtimes:"
  while IFS= read -r key; do echo "  $key"; done <<<"$keys"
  echo "UNVERIFIED (no runtime $RUNTIME in the catalogue)"
  exit 3
fi

IMAGE="$(kubectl -n "$NS" get configmap "$CATALOGUE" -o json \
  | python3 -c 'import json,sys; print(json.loads(json.load(sys.stdin)["data"][sys.argv[1]])["image"])' "$RUNTIME")"

# The CR owns the SparkConnect, the server pod and the ingress policy; deleting it is what stops
# the clock on all of them. The operator removes the policies and the driver's Role on its own
# schedule, so only the two owned kinds are waited for.
#
# The two listings go into variables before `grep -q` sees them: under `set -o pipefail`, `grep -q`
# exits at the first match, kubectl dies of SIGPIPE, and the pipeline's status is 141 — so
# `! kubectl … | grep -q …` answers "gone" while the pod is still Terminating.
gone() {
  local pods np
  pods="$(kubectl -n "$NS" get pods -o name 2>/dev/null || true)"
  np="$(kubectl -n "$NS" get networkpolicy "$NP" -o name 2>/dev/null || true)"
  ! grep -qE "^pod/(spc-${NAME}-|${EXEC}$)" <<<"$pods" && [ -z "$np" ]
}
cleanup() {
  kubectl -n "$NS" delete sparkcluster "$NAME" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl -n "$NS" delete pod "$EXEC" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  local deadline=$((SECONDS + 120))
  while ! gone; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "cleanup: spc-${NAME}-* pods or NetworkPolicy $NP are still there 2 min after the delete" >&2
      break
    fi
    sleep 5
  done
  return 0
}
trap cleanup EXIT

kubectl -n "$NS" apply -f - >/dev/null <<EOF
apiVersion: stardelt.io/v1alpha1
kind: SparkCluster
metadata:
  name: ${NAME}
  namespace: ${NS}
spec:
  owner:
    group: visitors
  size: small
  runtime: ${RUNTIME}
  state: Running
EOF

# `Starting` is correct while the server pod comes up (operator#774); a `Failed` phase is the
# reconciler's own word and ends the poll at once.
phase=""
deadline=$((SECONDS + 300))
while :; do
  phase="$(kubectl -n "$NS" get sparkcluster "$NAME" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  [ "$phase" = Running ] && break
  [ "$phase" = Failed ] && break
  [ "$SECONDS" -lt "$deadline" ] || break
  sleep 5
done
if [ "$phase" = Failed ]; then
  echo "the reconciler reported: $(kubectl -n "$NS" get sparkcluster "$NAME" -o jsonpath='{.status.message}' 2>/dev/null || true)"
fi

ok() { echo "✓ $1"; }
bad() { echo "✗ $1"; problems=$((problems + 1)); }

# 1. The phase the operator reports.
if [ "$phase" = Running ]; then
  ok "the phase is Running"
else
  bad "the phase is Running (it is '${phase:-empty}')"
fi

# 2. The endpoint Nova and a notebook dial.
endpoint="$(kubectl -n "$NS" get sparkcluster "$NAME" -o jsonpath='{.status.endpoint}' 2>/dev/null || true)"
case "$endpoint" in
  'sc://'*':15002') ok "the endpoint starts with sc:// and ends with :15002 ($endpoint)" ;;
  *) bad "the endpoint starts with sc:// and ends with :15002 (it is '${endpoint:-empty}')" ;;
esac

# 3. The server pod that serves the endpoint.
ready="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.containerStatuses[*].ready}' 2>/dev/null || true)"
if [ "$ready" = true ]; then
  ok "the server pod $POD is Ready"
else
  bad "the server pod $POD is Ready (it says '${ready:-absent}')"
fi

# 4. The driver's own identity: the executors' token-less ServiceAccount cannot create pods.
sa="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.spec.serviceAccountName}' 2>/dev/null || true)"
if [ "$sa" = spark-cluster-driver ]; then
  ok "the server pod runs as spark-cluster-driver"
else
  bad "the server pod's serviceAccountName is spark-cluster-driver (it is '${sa:-absent}')"
fi

# 5. ... which exists, with the Role it needs and its binding.
missing=""
for kind in serviceaccount role rolebinding; do
  kubectl -n "$NS" get "$kind" spark-cluster-driver >/dev/null 2>&1 || missing="$missing $kind"
done
if [ -z "$missing" ]; then
  ok "ServiceAccount, Role and RoleBinding spark-cluster-driver exist"
else
  bad "ServiceAccount, Role and RoleBinding spark-cluster-driver exist (missing:${missing})"
fi

# 6. The two admission guards (operator#775) and a binding that enforces each of them. The
#    bindings carry the namespace as a suffix (`-${NS}`), so they are found by their policyName.
missing=""
for policy in spark-cluster-executor-pods spark-cluster-configmaps; do
  kubectl get validatingadmissionpolicy "$policy" >/dev/null 2>&1 || missing="$missing $policy"
  binding="$(kubectl get validatingadmissionpolicybinding \
    -o jsonpath="{range .items[?(@.spec.policyName==\"${policy}\")]}{.metadata.name}{\"\\n\"}{end}" 2>/dev/null || true)"
  [ -n "$binding" ] || missing="$missing ${policy}-binding"
done
if [ -z "$missing" ]; then
  ok "policies spark-cluster-executor-pods and spark-cluster-configmaps exist, each with a binding"
else
  bad "policies spark-cluster-executor-pods and spark-cluster-configmaps exist, each with a binding (missing:${missing})"
fi

# 7. The only way in: Nova, Airflow and the owner's group.
if kubectl -n "$NS" get networkpolicy "$NP" >/dev/null 2>&1; then
  ok "NetworkPolicy $NP exists"
else
  bad "NetworkPolicy $NP exists"
fi

# 8. The failure operator#773 removed, seen from the driver's side: a driver with no token dies
#    in createTaskScheduler with a PKIX path error.
pkix="$(kubectl -n "$NS" logs "$POD" 2>/dev/null | grep -c 'PKIX path building failed' || true)"
if [ "$pkix" = 0 ]; then
  ok "the server pod's log has no 'PKIX path building failed'"
else
  bad "the server pod's log has no 'PKIX path building failed' (${pkix:-no log lines} line(s))"
fi

# 9. The executor path (platform#1511): Spark labels every executor pod `spark-role=executor`, and
#    executors must reach the driver's RPC port. The policy names the rule AND a pod with that label
#    connects — an endpoint that answers 15002 is a cluster that cannot compute, which is what this
#    script said 8 of 8 about until now.
admitted="$(kubectl -n "$NS" get networkpolicy "$NP" -o json 2>/dev/null | python3 -c '
import json, sys
rules = ((json.load(sys.stdin).get("spec") or {}).get("ingress") or [])
found = False
for rule in rules:
    peers = rule.get("from") or []
    executor = any(((p.get("podSelector") or {}).get("matchLabels") or {}).get("spark-role") == "executor" for p in peers)
    ports = {p.get("port") for p in (rule.get("ports") or [])}
    found = found or (executor and {7078, 7079} <= ports)
print("yes" if found else "no")
' 2>/dev/null || true)"
reachable=no
driver_ip="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.podIP}' 2>/dev/null || true)"
if [ -n "$driver_ip" ] \
   && kubectl -n "$NS" delete pod "$EXEC" --ignore-not-found >/dev/null 2>&1 \
   && kubectl -n "$NS" run "$EXEC" --image "$IMAGE" --labels='spark-role=executor' --restart=Never --command -- sleep 300 >/dev/null 2>&1 \
   && kubectl -n "$NS" wait --for=condition=Ready pod/"$EXEC" --timeout=120s >/dev/null 2>&1 \
   && kubectl -n "$NS" exec "$EXEC" -- timeout 10 bash -c "</dev/tcp/${driver_ip}/7078" >/dev/null 2>&1; then
  reachable=yes
fi
if [ "$admitted" = yes ] && [ "$reachable" = yes ]; then
  ok "a pod labelled spark-role=executor reaches the driver on 7078, and the policy names 7078 and 7079"
else
  bad "a pod labelled spark-role=executor reaches the driver on 7078 (the policy names it: ${admitted}; the probe connects: ${reachable})"
fi

# 10. The catalogue path (platform#1524): the driver is the process that writes Iceberg namespaces
#     and tables and asks Lakekeeper to sign S3 requests, so lakekeeper-ingress must admit a group
#     SparkCluster's server pod — labelled `sparkoperator.k8s.io/connect-name` by the Spark
#     Operator, never `app.kubernetes.io/name: spark-connect` — and the driver must reach
#     lakekeeper:8181, the chart's Service and REST port in this namespace.
named="$(kubectl -n "$NS" get networkpolicy lakekeeper-ingress -o json 2>/dev/null | python3 -c '
import json, sys
rules = ((json.load(sys.stdin).get("spec") or {}).get("ingress") or [])
found = False
for rule in rules:
    if 8181 not in {p.get("port") for p in (rule.get("ports") or [])}:
        continue
    for p in rule.get("from") or []:
        for e in ((p.get("podSelector") or {}).get("matchExpressions") or []):
            if e.get("key") == "sparkoperator.k8s.io/connect-name" and e.get("operator") == "Exists":
                found = True
print("yes" if found else "no")
' 2>/dev/null || true)"
connects=no
if kubectl -n "$NS" exec "$POD" -- timeout 10 bash -c '</dev/tcp/lakekeeper/8181' >/dev/null 2>&1; then
  connects=yes
fi
if [ "$named" = yes ] && [ "$connects" = yes ]; then
  ok "the driver reaches the catalog on lakekeeper:8181, and lakekeeper-ingress names it"
else
  bad "the driver reaches the catalog on lakekeeper:8181 (the policy names it: ${named}; the probe connects: ${connects})"
fi

# The FAIL branch exits first on purpose: a trailing unconditional `exit` makes ShellCheck
# (0.9 SC2317, 0.11 SC2329) call the trap-only `cleanup` unreachable.
[ "$problems" -eq 0 ] || { echo "FAIL ($((10 - problems)) of 10 checks passed; $problems failed)"; exit 1; }
echo "PASS (10 of 10: the endpoint, the driver's identity, its guards, a TLS-clean log, the executor path and the catalog)"
