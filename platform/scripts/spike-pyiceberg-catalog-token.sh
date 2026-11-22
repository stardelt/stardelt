#!/usr/bin/env bash
# Spike (#1246, #492 design question 2, the PyIceberg half): can a PyIceberg client sign in to
# the catalog with the projected ServiceAccount token — the credential an Airflow task pod
# already carries — and read Iceberg data through Lakekeeper?
#
#   bash scripts/spike-pyiceberg-catalog-token.sh
#
# #492's target is that a workload signs in with a projected token (audience
# `stardelt-catalog-workload`) and nothing else. Measured so far on the lab: the Java client
# reads with that token (`TokenFileAuthManager`, `make verify-spark-cluster-read`, #1169), and
# a task pod launched by `airflow-worker` as `sp-<principal>` carries exactly that token
# (#1092, `spike-task-pod-principal-token.sh`). Not measured: whether PyIceberg 0.12.0 — the
# client in the pinned `ghcr.io/stardelt/airflow` image that an Airflow task, `stream_job.py`
# or the library would use — reads through the catalog with the same token, and whether it
# needs the `X-Iceberg-Access-Delegation: vended-credentials` header the Spark path sends.
# Without that answer the loader cannot drop `PIPELINE_CLIENT_SECRET`.
#
# READ-ONLY IN THE CATALOG. Only list/load/scan: no table is created, written or dropped, and
# no `/management` write endpoint is called. `/catalog/v1/config` is never called — it
# *provisions* a principal (`last-updated-with: config-call-creation`, #538). No token, JWT
# segment or credential value is printed: only class names, counts and `present`/`none`.
#
# The pod is the shape #1092 measured: created as `sp-<principal>`, `automountServiceAccountToken:
# false`, one projected `catalog-token` volume, and the labels `component=worker,release=airflow`
# that `lakekeeper-ingress` admits (a probe with its own label alone gets no packets, #1092).
#
# Environment:
#   NS         namespace, default stardelt (NAMESPACE is honoured as well)
#   PRINCIPAL  the ServicePrincipal to launch as, default pipeline-demo
#   AUDIENCE   the workload audience, default stardelt-catalog-workload
#   CATALOG_URL the catalog's REST endpoint, default
#              http://lakekeeper.${NS}.svc.cluster.local:8181/catalog
#   S3_ENDPOINT the object store, default http://seaweedfs-s3.${NS}.svc.cluster.local:8333
#
# Exit 0 = PASS/PARTIAL (the questions are answered), 1 = FAIL, 3 = UNVERIFIED (no kubectl, no
# namespace, no principal, no pinned loader image, no PyIceberg in the image).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAME=spike-pyiceberg-catalog-token
NS="${NS:-${NAMESPACE:-stardelt}}"
PRINCIPAL="${PRINCIPAL:-pipeline-demo}"
SA="sp-${PRINCIPAL}"
POD=spike-pyiceberg
AUDIENCE="${AUDIENCE:-stardelt-catalog-workload}"
CATALOG_URL="${CATALOG_URL:-http://lakekeeper.${NS}.svc.cluster.local:8181/catalog}"
S3_ENDPOINT="${S3_ENDPOINT:-http://seaweedfs-s3.${NS}.svc.cluster.local:8333}"
# The catalog admits named sources only: the task-pod labels *and* this spike's own marker.
LABELS="component=worker,release=airflow,stardelt.io/spike=pyiceberg"

unverified() { echo "$NAME: UNVERIFIED ($*)"; exit 3; }

command -v kubectl >/dev/null 2>&1 || unverified "kubectl not found"
command -v python3 >/dev/null 2>&1 || unverified "python3 not found"

tmp="$(mktemp -d "${TMPDIR:-/tmp}/spike-pyiceberg.XXXXXX")"
# No file here ever holds a token: the token is minted by the kubelet into the pod's projected
# volume and read only from inside the pod.
# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  rm -rf "$tmp"
  # Nothing to delete when the run never created the pod (a refusal before step 3): no line.
  if ! kubectl -n "$NS" get pod "$POD" -o name >/dev/null 2>&1; then
    return 0
  fi
  kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  # `--wait=false` returns before the pod is gone and the next run would be refused with
  # AlreadyExists; the pod must be gone when the script ends, so the trap waits for it.
  local gone=no
  for _ in $(seq 1 150); do
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
# 1. The namespace, the principal and its ServiceAccount, read-only. The principal's
#    `status.workloadIdentity` is only used to name it; this spike reads it from inside the pod.
# ---------------------------------------------------------------------------
kubectl get namespace "$NS" >/dev/null 2>"$tmp/ns.err" \
  || unverified "no such namespace"
kubectl -n "$NS" get serviceprincipal "$PRINCIPAL" -o name >/dev/null 2>"$tmp/sp.err" \
  || unverified "cannot read ServicePrincipal $PRINCIPAL in $NS: $(tail -n 1 "$tmp/sp.err")"
kubectl -n "$NS" get sa "$SA" -o name >/dev/null 2>"$tmp/sa.err" \
  || unverified "cannot read ServiceAccount $SA: $(tail -n 1 "$tmp/sa.err")"

# ---------------------------------------------------------------------------
# 2. The loader image, from the scheduler Deployment — the image a task pod really runs. It
#    must be pinned by digest; `probe-pod-spec.py` refuses a floating tag (platform#1656).
# ---------------------------------------------------------------------------
image="$(kubectl -n "$NS" get deploy airflow-scheduler \
  -o jsonpath='{.spec.template.spec.containers[0].image}' 2>"$tmp/image.err")" \
  || unverified "cannot read the airflow-scheduler image: $(tail -n 1 "$tmp/image.err")"
case "$image" in
  *@sha256:*) : ;;
  *) unverified "the airflow-scheduler image is not pinned by digest ($image)" ;;
esac
echo "image=$image"

# ---------------------------------------------------------------------------
# 3. The probe Pod: `airflow-worker` creates task pods as `sp-<principal>`; that SA mounts no
#    token of its own, and the one credential in the pod is the projected catalog token.
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
        # The pod block `platform/helm-values/airflow.yaml` sets for the real task pods: the
        # image's venv (/home/airflow/.local, 0770 airflow:root) and its PATH are only reachable
        # as UID 50000, which is what `make spike-pyiceberg-catalog-token` measured on the lab.
        "securityContext": {
            "runAsNonRoot": True,
            "runAsUser": 50000,
            "fsGroup": 0,
            "seccompProfile": {"type": "RuntimeDefault"},
        },
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
  for _ in $(seq 1 150); do
    [ -z "$(kubectl -n "$NS" get pod "$POD" -o name 2>/dev/null || true)" ] && break
    sleep 1
  done
fi

if ! created="$(kubectl -n "$NS" run "$POD" --image "$image" --restart=Never \
    --overrides="$overrides" 2>&1)"; then
  # The admission answer the loader slice needs: print the API server's own words.
  echo "admitted=no reason=$(printf '%s' "$created" | tail -n 1)"
  echo "$NAME: FAIL"
  exit 1
fi
if ! wait_out="$(kubectl -n "$NS" wait --for=condition=Ready pod/"$POD" --timeout=180s 2>&1)"; then
  echo "admitted=yes ready=no reason=$(printf '%s' "$wait_out" | tail -n 1)"
  echo "$NAME: FAIL"
  exit 1
fi
echo "admitted=yes sa=$SA audience=$AUDIENCE"

# ---------------------------------------------------------------------------
# 4. The questions from INSIDE the pod, one `P<n>` line each. The token is read here and never
#    leaves the pod; PyIceberg's exceptions are cut to 80 characters.
# ---------------------------------------------------------------------------
# shellcheck disable=SC2016  # expands inside the pod, from the pod's own environment
if ! inside="$(kubectl -n "$NS" exec -i "$POD" -c probe -- \
    env "CATALOG_URL=$CATALOG_URL" "S3_ENDPOINT=$S3_ENDPOINT" \
    sh -c 'src="$(mktemp "${TMPDIR:-/tmp}/spike-pyiceberg.XXXXXX.py")" || exit 3
           cat > "$src" || exit 3
           python3 "$src"
           status=$?
           rm -f "$src"
           exit $status' 2>&1 <<'PY'
import importlib.metadata as md
import os
import sys

TOKEN_FILE = "/var/run/secrets/stardelt-catalog/token"
CATALOG_URL = os.environ.get("CATALOG_URL", "")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "")
CATALOG = "gold"
TABLE = "shop.daily_revenue"
DELEGATION = "vended-credentials"


def out(line):
    print(line, flush=True)


def described(exc):
    """`Class: last 80 characters` — the catalog's messages carry no secret; cut anyway."""
    text = " ".join(str(exc).split())[-80:]
    return "%s: %s" % (type(exc).__name__, text) if text else type(exc).__name__


try:
    import pyiceberg
    import pyiceberg_core
except ModuleNotFoundError as exc:
    # The loader image's venv is `$VIRTUAL_ENV`; `kubectl run` starts the probe container
    # without the chart's PATH, so `python3` is the base interpreter and VIRTUAL_ENV is unset —
    # that is why the image's own venv path is the second candidate. Each is tried once.
    candidates = []
    venv = os.environ.get("VIRTUAL_ENV") or ""
    if venv:
        candidates.append(os.path.join(venv, "bin", "python"))
    candidates.append("/home/airflow/.local/bin/python")
    if not os.environ.get("SPIKE_REEXEC"):
        for candidate in candidates:
            if os.path.exists(candidate) and \
                    os.path.realpath(candidate) != os.path.realpath(sys.executable):
                os.environ["SPIKE_REEXEC"] = "1"
                os.execv(candidate, [candidate, os.path.abspath(__file__)])
    sys.stderr.write("pyiceberg is not importable in the loader image (%s)\n" % exc)
    sys.exit(3)

out("P1 client=%s pyiceberg=%s core=%s" % (
    sys.executable, md.version("pyiceberg"), md.version("pyiceberg-core")))

try:
    with open(TOKEN_FILE) as handle:
        token = handle.read().strip()
except OSError as exc:
    sys.stderr.write("cannot read the projected token: %s\n" % exc)
    sys.exit(3)
if not token:
    sys.stderr.write("the projected token file is empty\n")
    sys.exit(3)

from pyiceberg.catalog import load_catalog  # noqa: E402  (after the token is read)

PROPERTIES = {
    "type": "rest",
    "uri": CATALOG_URL,
    "token": token,
    "warehouse": CATALOG,
    "s3.endpoint": S3_ENDPOINT,
    "s3.region": "us-east-1",
    "s3.path-style-access": "true",
}


def vended(table):
    """Is a table-scoped storage credential on the client? Values are never printed."""
    properties = getattr(getattr(table, "io", None), "properties", None) or {}
    return any(str(key).startswith("s3.access-key-id") for key in properties)


plain_rows = -1
header_rows = -1

# --- P2: the plain read, the token from the projected file ---------------------------------
catalog = None
try:
    catalog = load_catalog(CATALOG, **PROPERTIES)
    namespaces = catalog.list_namespaces()
    out("P2 plaintoken=ok namespaces=%d" % len(namespaces))
except Exception as exc:  # noqa: BLE001  (the class and its message are the answer)
    out("P2 plaintoken=%s namespaces=-" % described(exc))

# --- P3/P4: the table and its rows ----------------------------------------------------------
table = None
if catalog is not None:
    try:
        table = catalog.load_table(TABLE)
        snapshots = len(table.metadata.snapshots or [])
        out("P3 table=%s snapshots=%d" % (TABLE, snapshots))
    except Exception as exc:  # noqa: BLE001
        out("P3 table=%s snapshots=- (%s)" % (TABLE, described(exc)))
if table is not None:
    try:
        plain_rows = table.scan().to_arrow().num_rows
        out("P4 rows=%d" % plain_rows)
    except Exception as exc:  # noqa: BLE001
        out("P4 rows=- (%s)" % described(exc))
else:
    out("P4 rows=- (no table)")

# --- P5: the same read with the delegation header named as a `header.` property -------------
delegated_table = None
header_accepted = False
try:
    delegated = load_catalog(CATALOG + "delegated", **dict(PROPERTIES, **{
        "header.X-Iceberg-Access-Delegation": DELEGATION,
    }))
    delegated_table = delegated.load_table(TABLE)
    header_rows = delegated_table.scan().to_arrow().num_rows
    header_accepted = True
    out("P5 header=ok rows=%d" % header_rows)
except Exception as exc:  # noqa: BLE001
    out("P5 header=%s rows=-" % described(exc))

# --- P6: where the S3 credential came from --------------------------------------------------
vended_present = (table is not None and vended(table)) or (
    delegated_table is not None and vended(delegated_table))
out("P6 vended-s3=%s header-accepted=%s" % (
    "present" if vended_present else "none",
    "yes" if header_accepted else "no"))

# --- P7: one verdict ------------------------------------------------------------------------
if catalog is not None and plain_rows > 0:
    verdict = "PASS"
elif catalog is not None:
    verdict = "PARTIAL"
else:
    verdict = "FAIL"
out("P7 verdict=%s" % verdict)
sys.exit(0)
PY
)"; then
  printf '%s\n' "$inside"
  echo "$NAME: UNVERIFIED (the probe pod could not run PyIceberg)"
  exit 3
fi
printf '%s\n' "$inside"

verdict="$(printf '%s\n' "$inside" | sed -n 's/^P7 verdict=//p')"
case "$verdict" in
  PASS|PARTIAL) echo "$NAME: $verdict"; exit 0 ;;
  FAIL) echo "$NAME: FAIL"; exit 1 ;;
  *) echo "$NAME: UNVERIFIED (no P7 line)"; exit 3 ;;
esac
