#!/usr/bin/env bash
# Spike for stardelt/stardelt#1091 (#493's central question): a SparkCluster driver's catalog
# token is a projected ServiceAccount token of the shared driver ServiceAccount
# `spark-cluster-driver` — which catalog principal does it become?
#
#   NS=stardelt bash scripts/spike-spark-cluster-identity.sh
#
# #493 decides one Spark Connect server per owner group, acting *as the group*. The operator
# renders that server (`SparkCluster` -> `SparkConnect spc-<name>`) and mounts the token at
# /var/run/secrets/stardelt-catalog, but nothing measured whether the catalog sees the group's
# principal (`kubernetes~<sp-<group> SA uid>`) or the shared driver SA's. The answer decides
# whether #493's slices only change client defaults or need a per-group ServiceAccount.
#
# Seven lines, printed from the running objects:
#   I1 cluster, phase, server pod and the ServiceAccount it runs as
#   I2 the catalog-token volume: audience, path, mount path
#   I3 whether the runtime catalogue or the SparkCluster sets the Iceberg REST auth manager
#   I4 the mounted token's SHA-256 prefix and remaining life
#   I5 `GET /management/v1/whoami` with that token, inside the driver: status and principal
#   I6 the owner group's workload identity (ServicePrincipal pipeline-demo) and whether the
#      driver's ServiceAccount is that same identity; the token's `sub` names its SA
#   I7 the conclusion
#
# READ-ONLY IN THE CATALOG. The one call is `/management/v1/whoami`, which creates nothing;
# `/catalog/v1/config` would *provision* the principal (#538) and is never called. No token,
# header or Secret value is printed: only status codes, the SHA-256 prefix, seconds and ids.
# The CR is deleted on every exit path — the trap is the contract.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
NAME=spike-identity
POD="spc-${NAME}-server"
CONTAINER=spark-kubernetes-driver
RUNTIME=spark-4.1-py3.13
CATALOGUE=stardelt-spark-runtimes
GROUP=pipeline-demo

command -v kubectl >/dev/null 2>&1 || { echo "UNVERIFIED (kubectl not found)"; exit 3; }

# The reconciler refuses a runtime the catalogue does not carry, and the phase would then be
# `Failed` for a reason that says nothing about identity.
if ! keys="$(kubectl -n "$NS" get configmap "$CATALOGUE" -o json 2>/dev/null \
    | python3 -c 'import json, sys; print("\n".join(sorted(json.load(sys.stdin).get("data") or {})))')"; then
  echo "UNVERIFIED (ConfigMap $CATALOGUE is not installed — run make install-spark)"; exit 3
fi
if ! grep -qx "$RUNTIME" <<<"$keys"; then
  echo "UNVERIFIED (no runtime $RUNTIME in the catalogue)"
  exit 3
fi

# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  kubectl -n "$NS" delete sparkcluster "$NAME" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if kubectl -n "$NS" get sparkcluster "$NAME" >/dev/null 2>&1; then
    echo "cleanup cluster-deleted=no"
  else
    echo "cleanup cluster-deleted=yes"
  fi
  return 0
}
trap cleanup EXIT INT TERM

kubectl -n "$NS" apply -f - >/dev/null <<EOF
apiVersion: stardelt.io/v1alpha1
kind: SparkCluster
metadata:
  name: ${NAME}
  namespace: ${NS}
spec:
  owner:
    group: demo
  size: small
  runtime: ${RUNTIME}
  state: Running
EOF

# `Starting` is correct while the server pod comes up; a `Failed` phase is the reconciler's own
# word and ends the poll at once.
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
  exit 1
fi
if [ "$phase" != Running ]; then
  echo "the phase is '${phase:-empty}' 300 s after the apply, not Running" >&2
  exit 1
fi

# 1. The cluster and the identity the server pod runs as.
sa="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.spec.serviceAccountName}' 2>/dev/null || true)"
echo "I1 cluster=$NAME phase=$phase server=$POD sa=${sa:-absent}"

# 2. The projected catalog-token volume, read from the SparkConnect the operator rendered.
if ! i2="$(kubectl -n "$NS" get sparkconnect "spc-${NAME}" -o json 2>/dev/null | python3 -c '
import json, sys
spec = ((json.load(sys.stdin).get("spec") or {}).get("server") or {}).get("template") or {}
spec = spec.get("spec") or {}
vols = spec.get("volumes") or []
mounts = []
for c in spec.get("containers") or []:
    for m in c.get("volumeMounts") or []:
        if m.get("name") == "catalog-token":
            mounts.append(m.get("mountPath") or "absent")
srcs = []
for v in vols:
    if v.get("name") != "catalog-token":
        continue
    for s in ((v.get("projected") or {}).get("sources") or []):
        if "serviceAccountToken" in s:
            srcs.append(s["serviceAccountToken"])
s = srcs[0] if srcs else {}
print("%s|%s|%s" % (s.get("audience") or "absent", s.get("path") or "absent", mounts[0] if mounts else "absent"))
')"; then
  i2="absent|absent|absent"
fi
IFS='|' read -r tok_aud tok_path tok_mount <<<"$i2"
echo "I2 volume=catalog-token audience=$tok_aud path=$tok_path mount=$tok_mount"

# 3. The Iceberg REST auth manager: the runtime catalogue's sparkConf with the SparkCluster's
#    on top (the operator's order), then the two keys #492 would need.
if ! i3="$(kubectl -n "$NS" get configmap "$CATALOGUE" -o json 2>/dev/null \
    | python3 -c '
import json, sys
sys.stdout.write(json.dumps((json.loads(json.load(sys.stdin)["data"][sys.argv[1]]) or {}).get("sparkConf") or {}))
' "$RUNTIME" 2>/dev/null)"; then
  echo "UNVERIFIED (cannot read the runtime entry $RUNTIME)" >&2
  exit 3
fi
if ! i3_cr="$(kubectl -n "$NS" get sparkcluster "$NAME" -o json 2>/dev/null \
    | python3 -c 'import json, sys; sys.stdout.write(json.dumps((json.load(sys.stdin).get("spec") or {}).get("sparkConf") or {}))')"; then
  i3_cr='{}'
fi
if ! auth="$(python3 -c '
import json, sys
conf = {}
for raw in sys.argv[1:]:
    conf.update(json.loads(raw))
def find(suffix):
    return sorted(k for k in conf if k.startswith("spark.sql.catalog.") and k.endswith(suffix))
def value(suffix):
    keys = find(suffix)
    if not keys:
        return "absent"
    return ",".join("%s" % conf[k] for k in keys)
print("%s|%s" % (value(".rest.auth.type"), value(".rest.auth.token-file")))
' "$i3" "$i3_cr")"; then
  auth="absent|absent"
fi
IFS='|' read -r auth_type auth_file <<<"$auth"
echo "I3 runtime-auth rest.auth.type=$auth_type rest.auth.token-file=$auth_file"

# 4./5. Inside the driver: the mounted token's hash and life, and the catalog's answer.
read -r -d '' PROBE <<'PY' || true
import base64, hashlib, json, re, time, urllib.error, urllib.request
path = "/var/run/secrets/stardelt-catalog/token"
with open(path, "rb") as handle:
    token = handle.read().decode("utf-8", "replace").strip()
print("sha=" + hashlib.sha256(token.encode()).hexdigest()[:12])
claims = {}
try:
    part = token.split(".")[1]
    part += "=" * (-len(part) % 4)
    claims = json.loads(base64.urlsafe_b64decode(part))
except Exception:
    claims = {}
exp = claims.get("exp")
print("exp-in=" + (str(int(exp) - int(time.time())) if isinstance(exp, (int, float)) else "absent"))
print("sub=" + (str(claims.get("sub")) if claims.get("sub") else "absent"))
request = urllib.request.Request(
    "http://lakekeeper:8181/management/v1/whoami",
    headers={"Authorization": "Bearer " + token},
)
try:
    with urllib.request.urlopen(request, timeout=15) as answer:
        code, body = answer.status, answer.read().decode("utf-8", "replace")
except urllib.error.HTTPError as failure:
    code, body = failure.code, failure.read().decode("utf-8", "replace")
except Exception:
    code, body = 0, ""
print("whoami=" + str(code))
try:
    document = json.loads(body) if body else {}
except Exception:
    document = {}
if not isinstance(document, dict):
    document = {}
principal = document.get("id")
if not principal:
    found = re.search(r"User with id ([^\s\"]+) not found", body)
    principal = found.group(1) if found else None
print("id=" + (str(principal) if principal else "absent"))
PY

probe=""
if probe="$(kubectl -n "$NS" exec "$POD" -c "$CONTAINER" -- python3 -c "$PROBE" 2>/dev/null)"; then
  :
else
  probe=""
fi
sha=absent; exp_in=absent; sub=absent; whoami=000; principal=absent
while IFS='=' read -r key value; do
  case "$key" in
    sha) sha="${value:-absent}" ;;
    exp-in) exp_in="${value:-absent}" ;;
    sub) sub="${value:-absent}" ;;
    whoami) whoami="${value:-000}" ;;
    id) principal="${value:-absent}" ;;
  esac
done <<<"$probe"
[ -n "$probe" ] || { sha=absent; exp_in=absent; sub=absent; whoami=000; principal=absent; }
echo "I4 token-sha=$sha exp-in=$exp_in"
echo "I5 whoami status=$whoami principal=$principal"

# 6. The group's identity against the driver's ServiceAccount. `status.workloadIdentity` is
#    `kubernetes~<sp-pipeline-demo SA uid>`; the token's `sub` names its own SA, which is the
#    answer even when whoami is 404.
wi="$(kubectl -n "$NS" get serviceprincipal "$GROUP" -o jsonpath='{.status.workloadIdentity}' 2>/dev/null || true)"
sa_uid="$(kubectl -n "$NS" get sa spark-cluster-driver -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
same=unknown
if [ -n "$wi" ] && [ -n "$sa_uid" ]; then
  if [ "$wi" = "kubernetes~$sa_uid" ]; then same=yes; else same=no; fi
fi
sub_sa=absent
case "$sub" in
  system:serviceaccount:*:*) sub_sa="${sub##*:}" ;;
esac
echo "I6 group-principal=$GROUP workloadIdentity=${wi:-absent} same=$same sub-sa=$sub_sa"

# 7. The conclusion, in the ticket's order.
if [ "$principal" != absent ] && [ -n "$wi" ] && [ "$principal" = "$wi" ]; then
  conclusion="the group's principal"
elif [ "$sub_sa" = spark-cluster-driver ]; then
  conclusion="the shared driver SA"
elif [ "$whoami" = 404 ]; then
  conclusion=unprovisioned
else
  conclusion=none
fi
echo "I7 conclusion=$conclusion"

if [ "$sub_sa" = absent ] && [ "$whoami" = 000 ]; then
  echo "the probe inside $POD/$CONTAINER printed nothing (python3 or the token file is missing)" >&2
  exit 1
fi
exit 0
