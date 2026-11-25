#!/usr/bin/env bash
# What a visitor's own code can do *inside* a demo sandbox session (stardelt/stardelt#201
# slice d, #311).
#
#   make verify-spark-demo-security      (NS=… the platform namespace, DEMO_NS=… the sandbox,
#                                         TIMEOUT=… the pod wait)
#
# Why: `make verify-spark-demo-query` proves the sandbox runs a job and
# `make verify-spark-demo-sandbox` judges what the sandbox *contains* — Pod Security, the
# ServiceAccount, the token mount, its egress. Neither is a visitor's session: a sandbox that
# loaded every catalog, read the bucket directly, carried a token or reached the internet would
# pass both. These five probes are #201's security-test list, run as the `demo` person from a pod
# that carries a visitor's labels:
#
#   catalog   a catalog `visitors` holds no grant on is refused *and* `gold`, which they may
#             read, answers — without that positive half a dead catalog server and a missing
#             grant look identical. The session's token is `demo`'s, minted the way a real
#             visitor's browser session would be.
#   vended    a real data file is read through the vended credential alone: no S3 key is set,
#             so only Lakekeeper's table-scoped credential can open it. This is the one probe
#             that needs the warehouse flag (`make demo-sts YES=1`); without it the driver is
#             refused and the probe fails with that hint.
#   files     the driver's Hadoop S3A against the gateway with NO credentials must refuse the
#             warehouse prefix: code that goes around the catalog cannot read the bucket.
#   token     the client pod carries no ServiceAccount token and cannot open
#             kubernetes.default.svc — a notebook cannot list Secrets or pods.
#   udf       a Python UDF cannot reach example.com:443. The driver is `local[2]`, so the UDF
#             runs in the driver pod and the egress policy is what is measured.
#
# `vended` is the one probe that needs the warehouse flag to stay on; the other four must keep
# refusing after it lands — a vended credential is table-scoped and read-only, so the bucket and
# the other catalogs stay closed.
#
# Exit 0 = PASS (all five), 1 = FAIL (a probe was admitted, or `gold` did not answer), 3 =
# UNVERIFIED (no kubectl, the sandbox is not installed, the pod never started, or the client
# never reached the Connect server — nothing about containment was measured then).
#
# No credential is printed: `demo`'s password is written to a mode-0600 file for the minting
# script, the token goes to a Secret, and neither reaches argv or stdout. The S3 bucket name is
# not a credential.
#
# Spark Connect exposes no client-side JVM (`SparkSession._jvm` does not exist; measured
# 2026-11-19 on pyspark 4.1.4), so the `files` probe cannot call
# `FileSystem.get(...).listStatus(...)` from the client. `spark.read` on the same `s3a://` path
# asks the *driver's* Hadoop S3A to list it (InMemoryFileIndex.listStatus), and its exception is
# the measurement.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
DEMO_NS="${DEMO_NS:-stardelt-demo-spark}"
POD=verify-spark-demo-security
TIMEOUT="${TIMEOUT:-300}"
HOST="spark-demo.${DEMO_NS}.svc.cluster.local"
PORT=15002
DEMO_USER=demo
DEMO_PASSWORD="${DEMO_PASSWORD:-demo}"

for tool in kubectl python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "verify-spark-demo-security: UNVERIFIED ($tool not found)"; exit 3; }
done

if ! kubectl -n "$DEMO_NS" get deployment spark-demo >/dev/null 2>&1; then
  echo "verify-spark-demo-security: UNVERIFIED (the demo sandbox is not installed — make install-spark-demo)"
  exit 3
fi

# The client is the spawn image the hub runs; `scripts/check-notebook-image-pin.sh` keeps the
# two in step. The `dev@sha256:` tag is the only one in that file.
client_digest="$(grep -m1 -E '^[[:space:]]*tag:[[:space:]]*"dev@sha256:[0-9a-f]{64}"' helm-values/jupyterhub.yaml | sed -E 's/.*"dev@(sha256:[0-9a-f]{64})".*/\1/')"
[ -n "$client_digest" ] || { echo "verify-spark-demo-security: UNVERIFIED (helm-values/jupyterhub.yaml names no dev@sha256 notebook image)"; exit 3; }
CLIENT_IMAGE="ghcr.io/stardelt/jupyter-singleuser:dev@${client_digest}"

tmp="$(mktemp -d "${TMPDIR:-/tmp}/verify-spark-demo-security.XXXXXX")"
chmod 700 "$tmp"
# No pod, Secret or password file is left behind: not on PASS, not on FAIL, not on Ctrl-C.
# ShellCheck (0.9 SC2317, 0.11 SC2329) calls the trap-only function unreachable.
# shellcheck disable=SC2329  # called by the EXIT trap below
cleanup() {
  kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NS" delete secret "$POD-token" --ignore-not-found >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

probes=0 ok=0 bad=0
pass() { probes=$((probes + 1)); ok=$((ok + 1)); printf '✓ %-17s%s\n' "$1" "$2"; }
fail_probe() { probes=$((probes + 1)); bad=$((bad + 1)); printf '✗ %-17s%s\n' "$1" "$2"; }

fail() {  # <logs> <reason>: the pod's tail, then the judged FAIL line
  local logs="$1"
  printf '%s\n' "$logs" | tail -n 40
  # A check that could not reach the server is UNVERIFIED, never FAIL (a NetworkPolicy that
  # drops the packet and a sandbox that is down look identical from inside the client). The
  # probe's own judged lines are excluded: `api=refused: ConnectionRefusedError` is a PASS half.
  local unreachable
  unreachable="$(printf '%s\n' "$logs" \
    | grep -vE '^(catalog|vended|files|token|api|udf)=' \
    | grep -m1 -E 'UNAVAILABLE|Connection refused|timed out|Name or service not known|Temporary failure in name resolution' || true)"
  if [ -n "$unreachable" ]; then
    echo "verify-spark-demo-security: UNVERIFIED (the client did not reach ${HOST}:${PORT}: ${unreachable})"
    exit 3
  fi
  echo "verify-spark-demo-security: FAIL ($2)"
  exit 1
}

# `demo`'s password, the one the public demo page prints. It never reaches argv or the terminal.
printf '%s' "$DEMO_PASSWORD" > "$tmp/password"
chmod 600 "$tmp/password"

ISSUER="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}' 2>/dev/null || true)"
[ -n "$ISSUER" ] || {
  echo "verify-spark-demo-security: UNVERIFIED (ConfigMap stardelt-identity has no issuerUrl — make install-identity)"
  exit 3
}

# A real signed token for the visitor, from the device grant on `stardelt-dev` (the pattern of
# scripts/verify-spark-auth.sh). Nothing here prints the token.
if ! out="$(ISSUER="$ISSUER" USER="$DEMO_USER" PASSWORD_FILE="$tmp/password" \
      TOKEN_FILE="$tmp/token" python3 scripts/mint-user-token.py 2>&1)"; then
  printf '%s\n' "$out" | tail -n 5
  echo "verify-spark-demo-security: UNVERIFIED (could not mint a token for $DEMO_USER — is the public demo installed? make public-demo YES=1)"
  exit 3
fi
printf '%s\n' "$out"

kubectl -n "$NS" create secret generic "$POD-token" --from-file=token="$tmp/token" \
  --dry-run=client -o yaml | kubectl apply --server-side --field-manager=stardelt-scripts -f - >/dev/null

S3_BUCKET="$(kubectl -n "$NS" get secret stardelt-s3-creds -o jsonpath='{.data.bucket}' 2>/dev/null | base64 -d || true)"
[ -n "$S3_BUCKET" ] || {
  echo "verify-spark-demo-security: UNVERIFIED (Secret stardelt-s3-creds has no bucket — make install-core)"
  exit 3
}

# The four labels are the two admission rules (see verify-spark-demo-query.sh): the chart's
# `singleuser` egress policy for DNS and the sandbox's 15002, and the sandbox's ingress for a
# visitor's pod. `automountServiceAccountToken: false` is what the hub's spawner sets on a
# visitor pod (measured 2026-11-19); the `token` probe measures what is left in it.
if ! out="$(kubectl -n "$NS" apply -f - 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${NS}
  labels:
    app.kubernetes.io/name: verify-spark-demo-security
    app: jupyterhub
    release: jupyterhub
    component: singleuser-server
    stardelt.io/group.visitors: "true"
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    seccompProfile: { type: RuntimeDefault }
  automountServiceAccountToken: false
  containers:
    - name: client
      image: ${CLIENT_IMAGE}
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: { drop: ["ALL"] }
      env:
        - name: HOME
          value: /tmp
        - name: SPARK_HOST
          value: ${HOST}:${PORT}
        - name: S3_BUCKET
          value: ${S3_BUCKET}
        - name: DEMO_TOKEN
          valueFrom:
            secretKeyRef: { name: ${POD}-token, key: token }
      resources:
        requests: { cpu: 100m, memory: 256Mi }
        limits: { cpu: 1, memory: 1Gi }
      command:
        - python3
        - -c
        - |
          import os
          import socket

          from pyspark.sql import SparkSession

          HOST = os.environ["SPARK_HOST"]
          S3_BUCKET = os.environ["S3_BUCKET"]
          TOKEN = os.environ["DEMO_TOKEN"]
          CATALOG_URI = "http://lakekeeper.${NS}.svc.cluster.local:8181/catalog"
          S3_ENDPOINT = "http://seaweedfs-s3.${NS}.svc.cluster.local:8333"
          SA_TOKEN = "/var/run/secrets/kubernetes.io/serviceaccount/token"


          def first_line(exc):
              lines = [l.strip() for l in str(exc).splitlines() if l.strip()]
              return (lines[0] if lines else type(exc).__name__)[:240]


          session = SparkSession.builder.remote("sc://" + HOST).create()
          # The catalogs exactly as python/stardelt/__init__.py:944-974 sets them: the
          # visitor's token, the REST catalog, vended credentials, and the gateway.
          for name in ("gold", "retail"):
              session.conf.set("spark.sql.catalog." + name,
                               "org.apache.iceberg.spark.SparkCatalog")
              for key, value in (
                  ("type", "rest"),
                  ("uri", CATALOG_URI),
                  ("warehouse", name),
                  ("token", TOKEN),
                  ("header.X-Iceberg-Access-Delegation", "vended-credentials"),
                  ("io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
                  ("s3.endpoint", S3_ENDPOINT),
                  ("s3.region", "us-east-1"),
                  ("s3.path-style-access", "true"),
              ):
                  session.conf.set("spark.sql.catalog.%s.%s" % (name, key), value)

          # 1. catalog: gold is the positive half (visitors hold browse and select on it), retail
          #    the measured refusal (no grant for the visitor's groups).
          try:
              session.sql("SHOW NAMESPACES IN gold").collect()
              print("catalog=granted:loaded", flush=True)
          except Exception as exc:
              print("catalog=gold-refused: " + first_line(exc), flush=True)
          try:
              namespaces = session.sql("SHOW NAMESPACES IN retail").collect()
              print("catalog=ADMITTED %d" % len(namespaces), flush=True)
          except Exception as exc:
              print("catalog=refused: " + first_line(exc), flush=True)

          # 2. vended: one data file through the vended credential only. No S3 key is set, so a
          #    success means Lakekeeper's table-scoped credential opened the object.
          try:
              rows = session.sql("SELECT * FROM gold.shop.daily_revenue LIMIT 1").collect()
              print("vended=ok %d row(s)" % len(rows), flush=True)
          except Exception as exc:
              print("vended=refused: " + first_line(exc), flush=True)

          # 3. files: S3A in the driver, with no credential configured.
          session.conf.set("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
          session.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
          try:
              rows = session.read.text("s3a://%s/gold/" % S3_BUCKET).count()
              print("files=ADMITTED %d" % rows, flush=True)
          except Exception as exc:
              print("files=refused: " + first_line(exc), flush=True)

          # 4. token and api: no ServiceAccount token, and the API server is not reachable.
          print("token=" + ("MOUNTED" if os.path.exists(SA_TOKEN) else "absent"), flush=True)
          try:
              socket.create_connection(("kubernetes.default.svc", 443), 3).close()
              print("api=REACHABLE", flush=True)
          except Exception as exc:
              print("api=refused: " + repr(exc), flush=True)

          # 5. udf: the driver is local[2], so this runs in the driver pod's network namespace.
          def reach():
              try:
                  socket.create_connection(("example.com", 443), 5).close()
                  return "reachable"
              except Exception as exc:
                  return "refused: " + repr(exc)

          try:
              session.udf.register("reach", reach, "string")
              value = session.sql("SELECT reach()").collect()[0][0]
              print("udf=" + ("ok (" + value + ")" if value.startswith("refused") else value),
                    flush=True)
          except Exception as exc:
              print("udf=error: " + first_line(exc), flush=True)
EOF
)"; then
  echo "verify-spark-demo-security: UNVERIFIED (the client pod could not be created: $(printf '%s\n' "$out" | tail -n 1))"
  exit 3
fi

# A pod that never starts says nothing about containment: UNVERIFIED, never PASS or FAIL.
if ! kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$POD" --timeout="${TIMEOUT}s" >/dev/null 2>&1; then
  phase="$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  if [ "$phase" = "Pending" ] || [ -z "$phase" ]; then
    reason="$(kubectl -n "$NS" get pod "$POD" -o json 2>/dev/null | python3 -c '
import json, sys
try:
    status = json.load(sys.stdin).get("status", {})
except Exception:
    status = {}
waiting = (status.get("containerStatuses") or [{}])[0].get("state", {}).get("waiting") or {}
scheduled = [c for c in status.get("conditions") or []
             if c.get("type") == "PodScheduled" and c.get("status") != "True"]
reason = waiting.get("message") or waiting.get("reason") or ""
if not reason and scheduled:
    reason = scheduled[0].get("message", "")
print((reason or "").strip())
' 2>/dev/null || true)"
    echo "verify-spark-demo-security: UNVERIFIED (the client pod stayed Pending: ${reason:-no container started})"
    exit 3
  fi
  fail "$(kubectl -n "$NS" logs "$POD" 2>&1 || true)" "the client pod ended ${phase:-Unknown}, not Succeeded"
fi

logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
judged() { printf '%s\n' "$logs" | grep -m1 -E "^$1=" || true; }

# 1. catalog: the positive half must answer, then retail must be refused.
granted="$(printf '%s\n' "$logs" | grep -m1 -E '^catalog=granted:loaded$' || true)"
catalog="$(printf '%s\n' "$logs" | grep -m1 -E '^catalog=(refused:|ADMITTED|gold-refused:)' || true)"
if [ -z "$granted" ]; then
  fail_probe catalog "gold did not answer, so a refusal of retail would prove nothing: ${catalog:-no answer}"
elif [ "${catalog#catalog=refused:}" = "$catalog" ]; then
  fail_probe catalog "retail was not refused: ${catalog:-no answer}"
else
  pass catalog "retail is refused: ${catalog#catalog=refused: } (gold answers)"
fi

# 2. vended: a real data file must come back through the vended credential alone.
vended="$(judged vended)"
if [ "${vended#vended=ok}" = "$vended" ]; then
  fail_probe vended "${vended:-no answer} (make demo-sts YES=1)"
else
  pass vended "${vended#vended=ok } from gold.shop.daily_revenue through the vended credential"
fi

# 3. files: S3A with no credential must not list the warehouse prefix.
files="$(judged files)"
if [ "${files#files=refused:}" = "$files" ]; then
  fail_probe files "${files:-no answer}"
else
  pass files "s3a://${S3_BUCKET}/gold/ is refused without credentials: ${files#files=refused: }"
fi

# 4. token and api: one probe, because a token without a reachable API server still reads
#    nothing and a reachable API server without a token authenticates nobody. Both halves.
token="$(judged token)"
api="$(judged api)"
if [ "$token" != "token=absent" ]; then
  fail_probe token "${token:-no answer}"
elif [ "${api#api=refused:}" = "$api" ]; then
  fail_probe token "${token}; ${api:-no answer}"
else
  pass token "no ServiceAccount token; kubernetes.default.svc is refused"
fi

# 5. udf: anything but `udf=ok` is the UDF having reached the internet (or never run).
udf="$(judged udf)"
if [ "${udf#udf=ok}" = "$udf" ]; then
  fail_probe udf "${udf:-no answer}"
else
  pass udf "example.com:443 is unreachable from a Spark UDF"
fi

if [ "$bad" -eq 0 ]; then
  echo "verify-spark-demo-security: PASS (${ok} of ${probes})"
  exit 0
fi
echo "verify-spark-demo-security: FAIL (${ok} of ${probes} passed)"
exit 1
