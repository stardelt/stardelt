#!/usr/bin/env bash
# A session on a group SparkCluster writes Iceberg as the group (#493 point 1, #1179).
#
#   make verify-spark-group-write
#
# The write half of the read check. The driver of a group SparkCluster runs as
# `spark-cluster-driver-<group>` and carries a projected catalog token (audience
# `stardelt-catalog-workload`). A client pod the group's ingress admits, with no token of
# its own, names `rest.auth.type=io.stardelt.iceberg.TokenFileAuthManager` and
# `rest.auth.token-file`; the server reads its own file. This checks that the group's
# driver can CREATE TABLE, INSERT and read the rows back with the vended write credential
# (`X-Iceberg-Access-Delegation: vended-credentials`).
#
# It creates and drops the scratch schema `playground.verify_group_write` and the scratch
# table `playground.verify_group_write.rows_written`; nothing else changes. The cluster
# must already exist.
#
# Environment: NS default stardelt, CLUSTER default demo, TIMEOUT default 300s.
# Exit 0 = PASS, 1 = FAIL, 3 = UNVERIFIED (no group cluster, no kubectl/python3, no client
# image, the client pod did not finish).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
CLUSTER="${CLUSTER:-demo}"
POD=spike-spark-cluster-write
RUNTIME=spark-4.1-py3.13
HOST="spc-${CLUSTER}-server.${NS}.svc.cluster.local"
PORT=15002
CATALOG="http://lakekeeper.${NS}.svc.cluster.local:8181/catalog"
TOKEN=/var/run/secrets/stardelt-catalog/token
trap 'kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true' EXIT

TIMEOUT="${TIMEOUT:-300s}"

failures=0
unverified() { echo "UNVERIFIED ($*)"; exit 3; }
fail() { failures=$((failures + 1)); printf '✗ %s\n' "$*"; }
pass() { printf '✓ %s\n' "$*"; }

command -v kubectl >/dev/null 2>&1 || unverified "kubectl not found"
command -v python3 >/dev/null 2>&1 || unverified "python3 not found"

# R1 — the cluster: phase, endpoint, driver ServiceAccount.
if ! cluster_json="$(kubectl -n "$NS" get sparkcluster "$CLUSTER" -o json 2>&1)"; then
  unverified "SparkCluster ${CLUSTER} in ${NS}: $(printf '%s\n' "$cluster_json" | tail -n 1)"
fi
printf '%s' "$cluster_json" | python3 -c '
import json, sys
d = json.load(sys.stdin)
st = d.get("status") or {}
print("R1 cluster=%s phase=%s endpoint=%s driverServiceAccount=%s"
      % (d["metadata"]["name"], st.get("phase"), st.get("endpoint"), st.get("driverServiceAccount")))
'

# R2 — the runtime's catalog/auth keys: `none` when the runtime names no catalog at all.
if ! runtime_json="$(kubectl -n "$NS" get configmap stardelt-spark-runtimes -o json 2>&1)"; then
  echo "R2 runtime-auth=UNVERIFIED ($(printf '%s\n' "$runtime_json" | tail -n 1))"
else
  printf '%s' "$runtime_json" | python3 -c '
import json, sys
data = json.load(sys.stdin).get("data") or {}
entry = data.get(sys.argv[1])
if entry is None:
    print("R2 runtime-auth=UNVERIFIED (no %s in the catalogue)" % sys.argv[1])
    sys.exit(0)
try:
    conf = json.loads(entry).get("sparkConf") or {}
except Exception:
    conf = {}
keys = sorted(k for k in conf if k.startswith("spark.sql.catalog") or "rest.auth" in k)
print("R2 runtime-auth=%s" % (" ".join(keys) if keys else "none"))
' "$RUNTIME"
fi

# R3 — what the operator rendered for the driver: the token's audience and its mount path.
if ! connect_json="$(kubectl -n "$NS" get sparkconnect "spc-${CLUSTER}" -o json 2>&1)"; then
  echo "R3 token-audience=UNVERIFIED ($(printf '%s\n' "$connect_json" | tail -n 1))"
else
  printf '%s' "$connect_json" | python3 -c '
import json, sys
spec = (((json.load(sys.stdin).get("spec") or {}).get("server") or {}).get("template") or {}).get("spec") or {}
aud = "absent"
for volume in spec.get("volumes") or []:
    if volume.get("name") != "catalog-token":
        continue
    for source in (volume.get("projected") or {}).get("sources") or []:
        if "serviceAccountToken" in source:
            aud = source["serviceAccountToken"].get("audience", "absent")
mount = "absent"
for container in spec.get("containers") or []:
    for mount_ in container.get("volumeMounts") or []:
        if mount_.get("name") == "catalog-token":
            mount = mount_.get("mountPath", "absent")
print("R3 token-audience=%s mount-path=%s" % (aud, mount))
'
fi

# R4 — the token's catalog principal, without printing the token. `whoami` creates nothing;
# `/catalog/v1/config` is never called (it provisions the principal). `whoami` answers on the
# management API (`/management/v1/whoami`, as verify-spark-driver-identity.sh:121 uses it); the
# catalog base's `/catalog/v1/whoami` is 404, measured 2026-11-22.
MANAGEMENT="${CATALOG%/catalog}"
driver_pod="spc-${CLUSTER}-server"
if ! whoami_out="$(kubectl -n "$NS" exec "$driver_pod" -- python3 -c '
import json, sys, urllib.error, urllib.request
management, token_file = sys.argv[1], sys.argv[2]
with open(token_file) as handle:
    token = handle.read().strip()
request = urllib.request.Request(
    management + "/management/v1/whoami", headers={"Authorization": "Bearer " + token}
)
try:
    with urllib.request.urlopen(request, timeout=20) as response:
        print("R4 principal=" + str((json.load(response) or {}).get("name")))
except urllib.error.HTTPError as error:
    print("R4 principal=status=%d" % error.code)
except Exception as error:
    print("R4 principal=error=%s" % type(error).__name__)
' "$MANAGEMENT" "$TOKEN" 2>&1)"; then
  echo "R4 principal=UNVERIFIED (exec into ${driver_pod} failed: $(printf '%s\n' "$whoami_out" | tail -n 1))"
else
  printf '%s\n' "$whoami_out" | grep -m1 '^R4 ' || echo "R4 principal=UNVERIFIED (no answer)"
fi

expected_principal="system:serviceaccount:${NS}:spark-cluster-driver-${CLUSTER}"
if printf '%s\n' "$whoami_out" | grep -Fqx "R4 principal=${expected_principal}"; then
  pass "R4 the driver's token is ${expected_principal}"
else
  fail "R4 the driver's token is not ${expected_principal}: $(printf '%s' "$whoami_out" | grep -F 'R4 ' | tail -1)"
fi

# R5 — the client image is the notebook image from helm-values/jupyterhub.yaml, never a literal
# (scripts/check-notebook-image-pin.sh keeps it in step, as verify-spark-demo-query.sh reads it).
client_digest="$(grep -m1 -E '^[[:space:]]*tag:[[:space:]]*"dev@sha256:[0-9a-f]{64}"' helm-values/jupyterhub.yaml | sed -E 's/.*"dev@(sha256:[0-9a-f]{64})".*/\1/')"
[ -n "$client_digest" ] || { echo "R5 client-image=UNVERIFIED (helm-values/jupyterhub.yaml names no dev@sha256 notebook image)"; exit 3; }
CLIENT_IMAGE="ghcr.io/stardelt/jupyter-singleuser:dev@${client_digest}"
echo "R5 client-image=${CLIENT_IMAGE}"

# W1-W4 — a writer pod the group's ingress admits, with no catalog token of its own: the
# server reads the driver's file. It creates the scratch schema, writes ten rows, reads them
# back from a fresh session, then drops the table and the schema it created.
if ! out="$(kubectl -n "$NS" apply -f - 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${NS}
  labels:
    stardelt.io/group.${CLUSTER}: "true"
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    seccompProfile: { type: RuntimeDefault }
  imagePullSecrets:
    - name: ghcr-creds
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
          CONFS = (
              ("spark.sql.catalog.playground", "org.apache.iceberg.spark.SparkCatalog"),
              ("spark.sql.catalog.playground.type", "rest"),
              ("spark.sql.catalog.playground.uri", "${CATALOG}"),
              ("spark.sql.catalog.playground.warehouse", "playground"),
              ("spark.sql.catalog.playground.rest.auth.type", "io.stardelt.iceberg.TokenFileAuthManager"),
              ("spark.sql.catalog.playground.rest.auth.token-file", "${TOKEN}"),
              ("spark.sql.catalog.playground.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
              ("spark.sql.catalog.playground.header.X-Iceberg-Access-Delegation", "vended-credentials"),
              ("spark.sql.catalog.playground.s3.endpoint", "http://seaweedfs-s3.${NS}.svc.cluster.local:8333"),
              ("spark.sql.catalog.playground.s3.region", "us-east-1"),
              ("spark.sql.catalog.playground.s3.path-style-access", "true"),
          )
          s = SparkSession.builder.remote("sc://${HOST}:${PORT}").getOrCreate()
          for k, v in CONFS:
              s.conf.set(k, v)
          s.sql("CREATE NAMESPACE IF NOT EXISTS playground.verify_group_write")
          s.sql("DROP TABLE IF EXISTS playground.verify_group_write.rows_written")
          # W1
          s.sql("CREATE TABLE playground.verify_group_write.rows_written (n int) USING iceberg")
          print("W1 created= yes", flush=True)
          # W2 — the write itself, ten rows in one statement
          s.sql("INSERT INTO playground.verify_group_write.rows_written VALUES " + ",".join("(%d)" % n for n in range(1, 11)))
          print("W2 written= 10", flush=True)
          # W3 — a second, fresh session reads the committed rows back
          t = SparkSession.builder.remote("sc://${HOST}:${PORT}").create()
          for k, v in CONFS:
              t.conf.set(k, v)
          print("W3 read-back=", t.sql("SELECT count(*) AS n FROM playground.verify_group_write.rows_written").collect()[0]["n"], flush=True)
          # W4 — the cleanup this check owns, in the same session
          s.sql("DROP TABLE playground.verify_group_write.rows_written")
          s.sql("DROP NAMESPACE playground.verify_group_write")
          print("W4 dropped= yes", flush=True)
EOF
)"; then
  unverified "the client pod could not be created: $(printf '%s\n' "$out" | tail -n 1)"
fi

phase=""
end=$((SECONDS + ${TIMEOUT%s}))
while [ "$SECONDS" -lt "$end" ]; do
  phase="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  case "$phase" in
    Succeeded|Failed) break ;;
  esac
  sleep 5
done

logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
printf '%s\n' "$logs" | grep -E '^W[1234] ' || true
case "$phase" in
  Succeeded) ;;
  *) unverified "the client pod ended in phase ${phase:-Unknown} within ${TIMEOUT}" ;;
esac
if printf '%s\n' "$logs" | grep -Fqx 'W1 created= yes'; then
  pass "W1 the session created the table as the group"
else
  fail "W1 the session did not create the table"
fi
if printf '%s\n' "$logs" | grep -Fqx 'W2 written= 10'; then
  pass "W2 it wrote 10 rows"
else
  fail "W2 it did not write 10 rows"
fi
if printf '%s\n' "$logs" | grep -Fqx 'W3 read-back= 10'; then
  pass "W3 a fresh session read the 10 rows back"
else
  fail "W3 a fresh session did not read 10 rows: $(printf '%s\n' "$logs" | grep -F 'W3 ' | tail -1)"
fi
if printf '%s\n' "$logs" | grep -Fqx 'W4 dropped= yes'; then
  pass "W4 it dropped the table and the namespace"
else
  fail "W4 it did not drop the table and the namespace"
fi
if [ "$failures" -eq 0 ]; then
  echo "verify-spark-cluster-write: PASS (4 checks)"
  exit 0
fi
echo "verify-spark-cluster-write: FAIL (${failures})"
exit 1
