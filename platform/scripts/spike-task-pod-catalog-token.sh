#!/usr/bin/env bash
# Spike (#1271, #492 design question 2, the notebook/dbt half): which credential does a
# notebook or dbt task pod still need — the principal's client secret, or the catalog's
# projected token?
#
#   bash scripts/spike-task-pod-catalog-token.sh
#
# #1262 answered the `spark-pipeline` half: that task pod runs as `sp-<principal>` with one
# projected catalog token and no client secret, and the catalog authenticates the token
# (`spike-task-pod-principal-token.sh`, `spike-pyiceberg-catalog-token.sh`). The `notebook` and
# `dbt` pods still get `PIPELINE_CLIENT_ID`/`PIPELINE_CLIENT_SECRET`
# (`dags/stardelt_pipelines/notebook.py`, `dbt.py`, `principal.py`), and the library uses them
# for two different things: `_fetch_token()` mints the pod's token for Trino (`%sql`,
# `_token()`), while `_session()` sends a token to the *shared* Connect server as the person.
# Neither a shared-server session opened with the projected file nor Trino's answer to the
# projected token has been measured, so the loader slice that drops the secret cannot be
# written. Six questions, one throwaway pod:
#
#   Q1  does a session on the shared server read Iceberg with only the projected token?
#   Q2  what does stardelt._token() do with no client secret?
#   Q3  does Trino accept the projected token?
#   Q4  does the client-credentials grant still mint a token Trino accepts?
#   Q5  which catalog principal is the projected token?
#   Q6  is the pod's API-server token absent? (`automountServiceAccountToken: false`)
#
# READ-ONLY. Only `select 1` (Trino) and `/management/v1/whoami` (the catalog) are called; no
# `/catalog/v1/config` (it *provisions* a principal, #538), no write endpoint, no table touched.
# No token, JWT segment, client secret or header ever leaves the pod or is printed: the rows name
# a presence, a status code or a row count.
#
# The pod is the shape a task pod takes after #1262: created as `sp-pipeline-demo`, no
# automounted API-server token, one projected `catalog-token` volume (audience
# `stardelt-catalog-workload`). The labels are task-pod labels, and they decide which questions
# a run can take: `pipeline-spark-pipeline` is the only task-pod label spark-connect-ingress
# admits (Q1), and its `pipeline-notebook-egress` policy denies the catalog's own port, so Q5 is
# taken by a second run with the KubernetesExecutor label set a dbt task pod really carries
# (`LABELS=component=worker,release=airflow,stardelt.io/spike=task-pod-catalog-token`), which
# lakekeeper-ingress admits and no egress policy selects. One pod cannot do both.
# Environment:
#   NS         namespace, default stardelt (NAMESPACE is honoured as well)
#   PRINCIPAL  the ServicePrincipal to launch as, default pipeline-demo
#   AUDIENCE   the workload audience, default stardelt-catalog-workload
#   LABELS     the probe pod's labels, default the task-pod set above
#   TIMEOUT    how long the pod may run, default 600s
#
# Exit 0 = the six rows are printed, 1 = a row could not be taken or the pod was refused,
# 3 = UNVERIFIED (no kubectl, no namespace, no principal Secret, no pinned notebook image).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NS="${NS:-${NAMESPACE:-stardelt}}"
PRINCIPAL="${PRINCIPAL:-pipeline-demo}"
SA="sp-${PRINCIPAL}"
POD=spike-task-pod-credentials
AUDIENCE="${AUDIENCE:-stardelt-catalog-workload}"
LABELS="${LABELS:-component=worker,release=airflow,app.kubernetes.io/name=pipeline-spark-pipeline,stardelt.io/spike=task-pod-catalog-token}"
TIMEOUT="${TIMEOUT:-600s}"

unverified() { echo "UNVERIFIED ($*)"; exit 3; }

command -v kubectl >/dev/null 2>&1 || unverified "kubectl not found"
command -v python3 >/dev/null 2>&1 || unverified "python3 not found"

tmp="$(mktemp -d "${TMPDIR:-/tmp}/spike-task-pod-catalog.XXXXXX")"
# No file here ever holds a token: the token is minted by the kubelet into the pod's projected
# volume and read only from inside the pod.
# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  rm -rf "$tmp"
  if ! kubectl -n "$NS" get pod "$POD" -o name >/dev/null 2>&1; then
    return 0
  fi
  kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  # `--wait=false` returns before the pod is gone and the next run would then be refused with
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
# L1. The principal's ServiceAccount and its Secret: the pod runs as it, and the client
#     credentials Q4 mints with come from the Secret (`secretKeyRef`, never through bash).
# ---------------------------------------------------------------------------
kubectl -n "$NS" get sa "$SA" -o name >/dev/null 2>"$tmp/sa.err" \
  || unverified "no ServiceAccount $SA in $NS: $(tail -n 1 "$tmp/sa.err")"
kubectl -n "$NS" get secret "$SA" -o name >/dev/null 2>"$tmp/secret.err" \
  || unverified "no Secret $SA (the principal's credentials) in $NS: $(tail -n 1 "$tmp/secret.err")"

# ---------------------------------------------------------------------------
# L2. The image the live hub spawns: the notebook the platform really gives a person, read from
#     the hub's rendered values (Secret `hub`, `singleuser.image`). A literal here would drift
#     from the pinned digest (scripts/check-notebook-image-pin.sh guards the values file).
# ---------------------------------------------------------------------------
kubectl -n "$NS" get secret hub -o jsonpath='{.data.values\.yaml}' 2>"$tmp/hub.err" \
  | base64 -d >"$tmp/values.yaml" \
  || unverified "cannot read the hub's values: $(tail -n 1 "$tmp/hub.err")"
image="$(python3 - "$tmp/values.yaml" <<'PY'
import sys, yaml

try:
    values = yaml.safe_load(open(sys.argv[1])) or {}
except Exception:
    raise SystemExit(0)
image = ((values.get("singleuser") or {}).get("image") or {})
name, tag = image.get("name") or "", image.get("tag") or ""
print(f"{name}:{tag}" if name and tag else "")
PY
)"
[ -n "$image" ] || unverified "the hub's values name no singleuser.image"
case "$image" in
  *@sha256:*) : ;;
  *) unverified "the singleuser image is not pinned by digest ($image)" ;;
esac
echo "L image=$image"
echo "L sa=$SA automount=false"

# ---------------------------------------------------------------------------
# 3. The probe Pod, then the six questions from inside it.
# ---------------------------------------------------------------------------
python3 - "$POD" "$NS" "$SA" "$image" "$AUDIENCE" "$LABELS" >"$tmp/pod.json" <<'PY'
import json, sys

pod, namespace, sa, image, audience, labels = sys.argv[1:7]
pairs = dict(pair.split("=", 1) for pair in labels.split(",") if "=" in pair)

PROBE = r'''
import json, os, subprocess, sys, urllib.error, urllib.request

TOKEN_FILE = os.environ.get("STARDELT_CATALOG_TOKEN_FILE", "/var/run/secrets/stardelt-catalog/token")
API_TOKEN = "/var/run/secrets/kubernetes.io/serviceaccount/token"
TRINO_URL = os.environ.get("TRINO_URL", "").rstrip("/")
STATEMENT = "select 1"
missing = []


def no_credentials():
    """The environment without the principal's client credentials: a pod that has none."""
    env = dict(os.environ)
    env.pop("PIPELINE_CLIENT_ID", None)
    env.pop("PIPELINE_CLIENT_SECRET", None)
    return env


def projected_token():
    with open(TOKEN_FILE) as handle:
        return handle.read().strip()


def trino_status(token):
    """The status of one POST /v1/statement, as `stardelt.connection()` posts it."""
    import ssl
    body = STATEMENT.encode()
    request = urllib.request.Request(
        TRINO_URL + "/v1/statement",
        data=body,
        method="POST",
        headers={
            "Authorization": "Bearer " + token,
            "X-Trino-User": "demo",
            "Content-Type": "text/plain",
        },
    )
    ca = os.environ.get("REQUESTS_CA_BUNDLE") or ""
    context = ssl.create_default_context(cafile=ca) if ca else ssl.create_default_context()
    try:
        with urllib.request.urlopen(request, timeout=30, context=context) as answer:
            return answer.status
    except urllib.error.HTTPError as error:
        return error.code
    except Exception as error:
        print("Q-error trino=%s: %s" % (type(error).__name__, str(error)[:160]))
        return None


def row(name, text):
    print("%s %s" % (name, text), flush=True)


# Q1 — the shared server acts as the token's principal. The session names `gold` the way
# `stardelt._session()` does, except that the token is the projected file's contents.
Q1 = r"""
import os
from pyspark.sql import SparkSession

with open(os.environ["STARDELT_CATALOG_TOKEN_FILE"]) as handle:
    token = handle.read().strip()
session = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).appName("spike-catalog-token").create()
for key, value in (
    ("", "org.apache.iceberg.spark.SparkCatalog"),
    ("type", "rest"),
    ("rest-catalog-purge", "true"),
    ("uri", os.environ.get("STARDELT_CATALOG_URI", "http://lakekeeper.stardelt.svc.cluster.local:8181/catalog")),
    ("warehouse", "gold"),
    ("token", token),
    ("io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
    ("header.X-Iceberg-Access-Delegation", "vended-credentials"),
    ("s3.endpoint", os.environ.get("STARDELT_S3_ENDPOINT", "http://seaweedfs-s3.stardelt.svc.cluster.local:8333")),
    ("s3.region", "us-east-1"),
    ("s3.path-style-access", "true"),
):
    session.conf.set("spark.sql.catalog.gold." + key if key else "spark.sql.catalog.gold", value)
print("rows=%d" % session.table("gold.shop.daily_revenue").count(), flush=True)
"""

try:
    done = subprocess.run(
        [sys.executable, "-u", "-c", Q1],
        env=no_credentials(), capture_output=True, text=True, timeout=420,
    )
    rows = [line for line in done.stdout.splitlines() if line.startswith("rows=")]
    if done.returncode == 0 and rows:
        row("Q1", rows[-1])
    else:
        first = (done.stderr.strip().splitlines() or ["no output"])[-1]
        row("Q1", "refused=%s" % first[:240])
        missing.append("Q1")
except Exception as error:  # noqa: BLE001 - a row that cannot be taken is still printed
    row("Q1", "error=%s" % type(error).__name__)
    missing.append("Q1")

# Q2 — what the library does for Trino's token when the pod has no client credentials.
try:
    done = subprocess.run(
        [sys.executable, "-u", "-c", "import stardelt; stardelt._token()"],
        env=no_credentials(), capture_output=True, text=True, timeout=120,
    )
    sentence = ""
    for line in reversed(done.stderr.strip().splitlines()):
        if "RuntimeError" in line:
            sentence = line.strip()
            break
    if done.returncode != 0:
        row("Q2", "token=absent")
        if sentence:
            row("Q2", "error=%s" % sentence[:240])
    else:
        # _token() answered: the token was in the environment, not compared here.
        row("Q2", "token=present")
        missing.append("Q2")
except Exception as error:  # noqa: BLE001
    row("Q2", "error=%s" % type(error).__name__)
    missing.append("Q2")

# Q3 — Trino's answer to the projected token.
try:
    status = trino_status(projected_token())
    if status is None:
        row("Q3", "trino=no-answer")
        missing.append("Q3")
    else:
        row("Q3", "trino=%s" % status)
except Exception as error:  # noqa: BLE001
    row("Q3", "error=%s" % type(error).__name__)
    missing.append("Q3")

# Q4 — the client-credentials grant still mints a token Trino accepts.
try:
    import stardelt
    status = trino_status(stardelt._fetch_token())
    if status is None:
        row("Q4", "trino=no-answer")
        missing.append("Q4")
    else:
        row("Q4", "trino=%s" % status)
except Exception as error:  # noqa: BLE001
    row("Q4", "error=%s" % type(error).__name__)
    missing.append("Q4")

# Q5 — the principal the catalog derives from the projected token. `404 UserNotFound` is an
# answer (the principal exists in the token but not yet provisioned) and names the same id.
try:
    import re
    request = urllib.request.Request(
        "http://lakekeeper.stardelt.svc.cluster.local:8181/management/v1/whoami",
        headers={"Authorization": "Bearer " + projected_token()},
    )
    status, body = None, ""
    try:
        with urllib.request.urlopen(request, timeout=20) as answer:
            status, body = answer.status, answer.read().decode()
    except urllib.error.HTTPError as error:
        status, body = error.code, error.read().decode()
    principal = ""
    try:
        document = json.loads(body) if body else {}
    except ValueError:
        document = {}
    if isinstance(document, dict):
        principal = document.get("id") or document.get("name") or ""
    if not principal:
        found = re.search(r"kubernetes~[0-9a-f-]{36}", body or "")
        principal = found.group(0) if found else ""
    if status is None:
        row("Q5", "whoami=no-answer")
        missing.append("Q5")
    else:
        row("Q5", "whoami=%s principal=%s" % (status, principal or "absent"))
except Exception as error:  # noqa: BLE001
    row("Q5", "error=%s: %s" % (type(error).__name__, str(error)[:160]))
    missing.append("Q5")

# Q6 — the pod's own API-server token, which `automountServiceAccountToken: false` removes.
if os.path.exists(API_TOKEN):
    row("Q6", "api-server-token=present")
    missing.append("Q6")
else:
    row("Q6", "api-server-token=absent")

sys.exit(len(missing))
'''

print(json.dumps({
    "apiVersion": "v1",
    "kind": "Pod",
    "metadata": {"name": pod, "namespace": namespace, "labels": pairs},
    "spec": {
        "serviceAccountName": sa,
        "automountServiceAccountToken": False,
        "restartPolicy": "Never",
        "imagePullSecrets": [{"name": "ghcr-creds"}],
        "securityContext": {
            "runAsNonRoot": True,
            "runAsUser": 100,
            "runAsGroup": 100,
            "fsGroup": 100,
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "containers": [{
            "name": "probe",
            "image": image,
            "command": ["python3", "-u", "-c", PROBE],
            "securityContext": {
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
            },
            "env": [
                {"name": "HOME", "value": "/tmp"},
                {"name": "TRINO_URL", "valueFrom": {"configMapKeyRef": {
                    "name": "stardelt-identity", "key": "trinoUrl"}}},
                {"name": "REQUESTS_CA_BUNDLE", "value": "/etc/stardelt/trino-ca/ca.crt"},
                {"name": "SPARK_REMOTE", "value":
                    "sc://spark-connect.stardelt.svc.cluster.local:15002"},
                {"name": "STARDELT_CATALOG_URI", "value":
                    "http://lakekeeper.stardelt.svc.cluster.local:8181/catalog"},
                {"name": "STARDELT_CATALOG_TOKEN_FILE", "value":
                    "/var/run/secrets/stardelt-catalog/token"},
                {"name": "PIPELINE_TOKEN_URL", "value":
                    "https://id.alpha.dev.stardelt.io/realms/stardelt/protocol/openid-connect/token"},
                {"name": "PIPELINE_CLIENT_ID", "valueFrom": {"secretKeyRef": {
                    "name": sa, "key": "client-id"}}},
                {"name": "PIPELINE_CLIENT_SECRET", "valueFrom": {"secretKeyRef": {
                    "name": sa, "key": "client-secret"}}},
            ],
            "volumeMounts": [
                {"name": "catalog-token", "mountPath": "/var/run/secrets/stardelt-catalog",
                 "readOnly": True},
                {"name": "trino-ca", "mountPath": "/etc/stardelt/trino-ca", "readOnly": True},
            ],
            "resources": {
                "requests": {"cpu": "250m", "memory": "512Mi"},
                "limits": {"cpu": "2", "memory": "3Gi"},
            },
        }],
        "volumes": [
            {"name": "catalog-token", "projected": {"sources": [{"serviceAccountToken": {
                "audience": audience,
                "expirationSeconds": 3600,
                "path": "token",
            }}]}},
            {"name": "trino-ca", "secret": {"secretName": "trino-ca"}},
        ],
    },
}, indent=2))
PY

# A pod left behind by an interrupted run would turn the create into AlreadyExists.
if [ -n "$(kubectl -n "$NS" get pod "$POD" -o name 2>/dev/null || true)" ]; then
  kubectl -n "$NS" delete pod "$POD" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  for _ in $(seq 1 150); do
    [ -z "$(kubectl -n "$NS" get pod "$POD" -o name 2>/dev/null || true)" ] && break
    sleep 1
  done
fi

if ! applied="$(kubectl -n "$NS" apply -f "$tmp/pod.json" 2>&1)"; then
  echo "L admitted=no reason=$(printf '%s' "$applied" | tail -n 1)"
  exit 1
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
printf '%s\n' "$logs" | grep -E '^(Q[1-6]|Q-error) ' || true

case "$phase" in
  Succeeded|Failed) ;;
  *) echo "L phase=${phase:-Unknown} after ${TIMEOUT}"; exit 1 ;;
esac

exit_code="$(kubectl -n "$NS" get pod "$POD" \
  -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || true)"
echo "L exit-code=${exit_code:-unknown}"

if [ "${exit_code:-1}" != 0 ]; then
  echo "spike-task-pod-catalog-token: incomplete (${exit_code:-?} row(s) could not be taken)"
  exit 1
fi
echo "spike-task-pod-catalog-token: done"
