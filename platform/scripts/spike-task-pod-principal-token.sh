#!/usr/bin/env bash
# Spike (#1092): does the delivery path a pipeline will use work — the Airflow worker launches a
# task pod as the principal's ServiceAccount with ONE projected catalog token, and that token
# signs in at the catalog as the group's principal?
#
#   bash scripts/spike-task-pod-principal-token.sh
#
# #492 design question 3 is answered for a token minted by `kubectl create token`
# (`make verify-workload-identity`, #650/#684) but never for the path a task pod really takes:
# the pod is created by `airflow-worker` (which holds `pods: create` on the namespace), the
# principal's `sp-<name>` ServiceAccount does not automount a token, and the catalog is reached
# from inside the pod. This script builds that pod once, asks the questions from inside it and
# deletes it again; the loader slice that drops `PIPELINE_CLIENT_SECRET` cannot be written safely
# until this has been measured.
#
# READ-ONLY IN THE CATALOG. Only `/management/v1/whoami` is called. `/catalog/v1/config` is never
# called — it *provisions* a principal (`last-updated-with: config-call-creation`, #538) — and no
# `/management` write endpoint is touched. Nothing is created in the catalog, no Secret is read,
# and no token, JWT segment or header ever leaves the pod or is printed.
#
# Environment:
#   NS        namespace, default stardelt
#   PRINCIPAL the ServicePrincipal to launch as, default pipeline-demo
#   LABELS    the probe pod's labels (K=V,K=V), default stardelt.io/spike=task-principal.
#             The labels matter: `lakekeeper-ingress` admits only the sources it names, and a
#             real Airflow task pod carries `component=worker,release=airflow`. Run it both ways
#             (`LABELS=component=worker,release=airflow`) to separate "the token is refused" from
#             "the pod is not admitted to the catalog".
#   AUDIENCE  the workload audience, default stardelt-catalog-workload
#
# Exit 0 = the pod ran and the questions are printed, 1 = the API server refused the pod,
# 3 = UNVERIFIED (no kubectl, no principal, no pinned loader image).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NS="${NS:-${NAMESPACE:-stardelt}}"
PRINCIPAL="${PRINCIPAL:-pipeline-demo}"
SA="sp-${PRINCIPAL}"
POD=spike-task-principal
AUDIENCE="${AUDIENCE:-stardelt-catalog-workload}"
LABELS="${LABELS:-stardelt.io/spike=task-principal}"

unverified() { echo "UNVERIFIED ($*)"; exit 3; }

command -v kubectl >/dev/null 2>&1 || unverified "kubectl not found"
command -v python3 >/dev/null 2>&1 || unverified "python3 not found"

tmp="$(mktemp -d "${TMPDIR:-/tmp}/spike-task-pod.XXXXXX")"
# No file here ever holds a token: the token is minted by the kubelet into the pod's projected
# volume and read only from inside the pod.
cleanup() {
  kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  rm -rf "$tmp"
  # `--wait=false` returns before the pod is gone, and the next run would then be refused with
  # AlreadyExists; the pod must be gone when the script ends, so the trap waits for it.
  local gone=no
  for _ in $(seq 1 60); do
    if [ -z "$(kubectl -n "$NS" get pod "$POD" -o name 2>/dev/null || true)" ]; then
      gone=yes
      break
    fi
    sleep 1
  done
  echo "cleanup pod-deleted=$gone"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# T1. The principal and its ServiceAccount, read-only. `status.workloadIdentity` is the id the
#     catalog knows this ServiceAccount by (`kubernetes~<SA uid>`, #650).
# ---------------------------------------------------------------------------
if ! sp_json="$(kubectl -n "$NS" get serviceprincipal "$PRINCIPAL" -o json 2>"$tmp/sp.err")"; then
  unverified "cannot read ServicePrincipal $PRINCIPAL in $NS: $(tail -n 1 "$tmp/sp.err")"
fi
workload="$(printf '%s' "$sp_json" | python3 -c '
import json, sys
print((json.load(sys.stdin).get("status") or {}).get("workloadIdentity") or "")
')"
[ -n "$workload" ] || unverified "ServicePrincipal $PRINCIPAL has no status.workloadIdentity"

if ! sa_json="$(kubectl -n "$NS" get sa "$SA" -o json 2>"$tmp/sa.err")"; then
  unverified "cannot read ServiceAccount $SA: $(tail -n 1 "$tmp/sa.err")"
fi
automount="$(printf '%s' "$sa_json" | python3 -c '
import json, sys
print(json.dumps(json.load(sys.stdin).get("automountServiceAccountToken")))
')"
echo "T1 principal=$PRINCIPAL workloadIdentity=$workload sa=$SA automount=$automount"

# ---------------------------------------------------------------------------
# T2. The Airflow worker's identity can create the pod (it is the not a driver's Role).
# ---------------------------------------------------------------------------
worker="system:serviceaccount:$NS:airflow-worker"
if kubectl auth can-i create pods --as="$worker" -n "$NS" >/dev/null 2>&1; then
  can=yes
else
  can=no
fi
echo "T2 worker=$worker can-create-pods=$can"

# ---------------------------------------------------------------------------
# T3. The loader image, from the scheduler Deployment — the image a task pod really runs. It must
#     be pinned by digest; `probe-pod-spec.py` refuses a floating tag (platform#1656).
# ---------------------------------------------------------------------------
image="$(kubectl -n "$NS" get deploy airflow-scheduler \
  -o jsonpath='{.spec.template.spec.containers[0].image}' 2>"$tmp/image.err")" \
  || unverified "cannot read the airflow-scheduler image: $(tail -n 1 "$tmp/image.err")"
case "$image" in
  *@sha256:*) : ;;
  *) unverified "the airflow-scheduler image is not pinned by digest ($image)" ;;
esac
echo "T3 image=$image"

# ---------------------------------------------------------------------------
# 4. The probe Pod. `airflow-worker` creates it as `sp-<principal>`; the SA mounts no token of its
#    own, and the one credential in the pod is the projected catalog token the task will use.
# ---------------------------------------------------------------------------
python3 - "$POD" "$NS" "$SA" "$image" "$AUDIENCE" "$LABELS" >"$tmp/pod.json" <<'PY'
import json, sys

pod, namespace, sa, image, audience, labels = sys.argv[1:7]
pairs = dict(pair.split("=", 1) for pair in labels.split(",") if "=" in pair)
print(json.dumps({
    "apiVersion": "v1",
    "kind": "Pod",
    "metadata": {"name": pod, "namespace": namespace, "labels": pairs},
    "spec": {
        "serviceAccountName": sa,
        "automountServiceAccountToken": False,
        "restartPolicy": "Never",
        "containers": [{
            "name": "probe",
            "image": image,
            "command": ["sleep", "300"],
            "volumeMounts": [{
                "name": "catalog-token",
                "mountPath": "/var/run/secrets/stardelt-catalog",
                "readOnly": True,
            }],
        }],
        "volumes": [{
            "name": "catalog-token",
            "projected": {"sources": [{"serviceAccountToken": {
                "audience": audience,
                "expirationSeconds": 3600,
                "path": "token",
            }}]},
        }],
    },
}, indent=2))
PY

if ! overrides="$(python3 scripts/probe-pod-spec.py --merge <"$tmp/pod.json")"; then
  unverified "probe-pod-spec.py refused the pod"
fi

# A pod left behind by an interrupted run would turn the create into AlreadyExists.
if [ -n "$(kubectl -n "$NS" get pod "$POD" -o name 2>/dev/null || true)" ]; then
  kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  for _ in $(seq 1 60); do
    [ -z "$(kubectl -n "$NS" get pod "$POD" -o name 2>/dev/null || true)" ] && break
    sleep 1
  done
fi

if ! created="$(kubectl -n "$NS" run "$POD" --image "$image" --restart=Never \
    --overrides="$overrides" 2>&1)"; then
  # The admission answer the loader slice needs: print the API server's own words.
  echo "T5 admitted=no reason=$(printf '%s' "$created" | tail -n 1)"
  exit 1
fi

if ! wait_out="$(kubectl -n "$NS" wait --for=condition=Ready pod/"$POD" --timeout=120s 2>&1)"; then
  echo "T5 admitted=yes ready=no reason=$(printf '%s' "$wait_out" | tail -n 1)"
  exit 1
fi
echo "T5 admitted=yes"

# ---------------------------------------------------------------------------
# 5b. The questions from INSIDE the pod. The token is read here and never leaves it; the JWT is
#     decoded for its header and its subject only, and the catalog is asked who this is.
# ---------------------------------------------------------------------------
probe_py="$(cat <<'PY'
import base64, json, os, re, urllib.error, urllib.request

TOKEN = "/var/run/secrets/stardelt-catalog/token"
API = "/var/run/secrets/kubernetes.io/serviceaccount/token"


def segment(part):
    return base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))


present = os.path.isfile(TOKEN)
mode = oct(os.stat(TOKEN).st_mode & 0o777)[2:] if present else "-"
token = ""
header = "no"
subject = "absent"
if present:
    with open(TOKEN) as handle:
        token = handle.read().strip()
    try:
        parts = token.split(".")
        head = json.loads(segment(parts[0]))
        header = "yes" if head.get("alg") and head.get("kid") else "no"
        claims = json.loads(segment(parts[1]))
        subject = claims.get("sub") or "absent"
    except (ValueError, IndexError):
        header = "no"
print("T4 token-present=%s token-mode=%s header=%s" % ("yes" if present else "no", mode, header))
print("T4 token-subject=%s" % subject)
print("T4 api-server-token=%s" % ("present" if os.path.exists(API) else "absent"))

status, body = "000", ""
if token:
    request = urllib.request.Request(
        "http://lakekeeper:8181/management/v1/whoami",
        headers={"Authorization": "Bearer " + token},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as answer:
            status, body = str(answer.status), answer.read().decode()
    except urllib.error.HTTPError as error:
        status, body = str(error.code), error.read().decode()
    except Exception:
        status, body = "000", ""

# A provisioned user answers with itself; an unprovisioned one answers 404 UserNotFound and names
# the id it looked up. Both name the principal the catalog derived from the token.
principal = ""
try:
    document = json.loads(body) if body else {}
except ValueError:
    document = {}
if isinstance(document, dict):
    principal = document.get("id") or document.get("sub") or ""
if not principal:
    found = re.search(r"kubernetes~[0-9a-f-]{36}", body or "")
    principal = found.group(0) if found else ""
print("T5 whoami status=%s principal=%s" % (status, principal or "absent"))
PY
)"

if ! inside="$(kubectl -n "$NS" exec "$POD" -c probe -- python3 -c "$probe_py" 2>&1)"; then
  echo "T4 probe-failed=$(printf '%s' "$inside" | tail -n 1)"
  exit 1
fi
printf '%s\n' "$inside"

status="$(printf '%s\n' "$inside" | sed -n 's/^T5 whoami status=\([^ ]*\) .*/\1/p')"
principal="$(printf '%s\n' "$inside" | sed -n 's/^T5 whoami status=[^ ]* principal=//p')"

# ---------------------------------------------------------------------------
# T6. Is the principal the catalog derived the same id the ServicePrincipal records?
# ---------------------------------------------------------------------------
if [ -z "$principal" ] || [ "$principal" = absent ]; then
  matches=unknown
elif [ "$principal" = "$workload" ]; then
  matches=yes
else
  matches=no
fi
echo "T6 identity=$workload matches-status=$matches"

# ---------------------------------------------------------------------------
# T7. One line from T4–T6.
# ---------------------------------------------------------------------------
if [ "$(printf '%s\n' "$inside" | sed -n 's/^T4 token-present=\([^ ]*\).*/\1/p')" != yes ]; then
  conclusion="the pod gets no projected token"
elif [ "$matches" = yes ] && { [ "$status" = 200 ] || [ "$status" = 404 ]; }; then
  conclusion="the loader may drop the client secret"
elif [ "$status" = 000 ]; then
  conclusion="the pod needs an admission or RBAC change"
else
  conclusion="the token is not accepted"
fi
printf 'T7 conclusion=%s\n' "$conclusion"
