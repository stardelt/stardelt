#!/usr/bin/env bash
# Does a session on a group SparkCluster read Iceberg as its group? (#493, #492)
#   bash scripts/spike-spark-cluster-read.sh
# Prints R1-R5 (what is installed), S1-S3 (what a session does), then deletes its client pod.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
CLUSTER="${CLUSTER:-demo}"
POD=spike-spark-cluster-read
RUNTIME=spark-4.1-py3.13
HOST="spc-${CLUSTER}-server.${NS}.svc.cluster.local"
PORT=15002
CATALOG="http://lakekeeper.${NS}.svc.cluster.local:8181/catalog"
TOKEN=/var/run/secrets/stardelt-catalog/token
trap 'kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true' EXIT

TIMEOUT="${TIMEOUT:-300s}"

command -v kubectl >/dev/null 2>&1 || { echo "UNVERIFIED (kubectl not found)"; exit 3; }
command -v python3 >/dev/null 2>&1 || { echo "UNVERIFIED (python3 not found)"; exit 3; }

# R1 — the cluster: phase, endpoint, driver ServiceAccount.
if ! cluster_json="$(kubectl -n "$NS" get sparkcluster "$CLUSTER" -o json 2>&1)"; then
  echo "UNVERIFIED (SparkCluster ${CLUSTER} in ${NS}: $(printf '%s\n' "$cluster_json" | tail -n 1))"
  exit 3
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

# R5 — the client image is the notebook image from helm-values/jupyterhub.yaml, never a literal
# (scripts/check-notebook-image-pin.sh keeps it in step, as verify-spark-demo-query.sh reads it).
client_digest="$(grep -m1 -E '^[[:space:]]*tag:[[:space:]]*"dev@sha256:[0-9a-f]{64}"' helm-values/jupyterhub.yaml | sed -E 's/.*"dev@(sha256:[0-9a-f]{64})".*/\1/')"
[ -n "$client_digest" ] || { echo "R5 client-image=UNVERIFIED (helm-values/jupyterhub.yaml names no dev@sha256 notebook image)"; exit 3; }
CLIENT_IMAGE="ghcr.io/stardelt/jupyter-singleuser:dev@${client_digest}"
echo "R5 client-image=${CLIENT_IMAGE}"

# S1-S3 — a session from a pod the group's ingress admits, with no auth config (S2), then the
# same session naming the file-auth class and the group's catalog (S3). Nothing is changed in
# the runtime, the operator or the library: this measures what ships.
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
  imagePullSecrets:
    - name: ghcr-creds
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
          from pyspark.sql import SparkSession
          s = SparkSession.builder.remote("sc://${HOST}:${PORT}").getOrCreate()
          print("S1 range10=", s.range(10).count(), flush=True)
          try:
              print("S2 no-config=", s.sql("select count(*) as n from gold.shop.daily_revenue").collect()[0]["n"], flush=True)
          except Exception as e:
              print("S2 no-config=refused", type(e).__name__, flush=True)
          for k, v in (
              ("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog"),
              ("spark.sql.catalog.gold.type", "rest"),
              ("spark.sql.catalog.gold.uri", "${CATALOG}"),
              ("spark.sql.catalog.gold.warehouse", "gold"),
              ("spark.sql.catalog.gold.rest.auth.type", "io.stardelt.iceberg.TokenFileAuthManager"),
              ("spark.sql.catalog.gold.rest.auth.token-file", "${TOKEN}"),
              ("spark.sql.catalog.gold.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
              ("spark.sql.catalog.gold.header.X-Iceberg-Access-Delegation", "vended-credentials"),
              ("spark.sql.catalog.gold.s3.endpoint", "http://seaweedfs-s3.${NS}.svc.cluster.local:8333"),
              ("spark.sql.catalog.gold.s3.region", "us-east-1"),
              ("spark.sql.catalog.gold.s3.path-style-access", "true"),
          ):
              s.conf.set(k, v)
          try:
              print("S3 file-auth=", s.sql("select count(*) as n from gold.shop.daily_revenue").collect()[0]["n"], flush=True)
          except Exception as e:
              print("S3 file-auth=refused", type(e).__name__, str(e)[:200], flush=True)
EOF
)"; then
  echo "UNVERIFIED (the client pod could not be created: $(printf '%s\n' "$out" | tail -n 1))"
  exit 3
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
printf '%s\n' "$logs" | grep -E '^S[123] ' || true
case "$phase" in
  Succeeded|Failed) exit 0 ;;
  *) echo "UNVERIFIED (the client pod ended in phase ${phase:-Unknown} within ${TIMEOUT}: the session did not finish)"; exit 3 ;;
esac
