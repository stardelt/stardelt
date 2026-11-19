#!/usr/bin/env bash
# The public demo's Spark sandbox contains what it claims (stardelt/stardelt#201 slice 1).
#
# Why: the sandbox is the answer to "a visitor's `spark` must not be the shared server". The
# shared one authenticates nobody, runs every client's work with the platform's S3 key and its
# ServiceAccount may create pods in `stardelt`. This second server is meant to share nothing:
# local mode (no executors, no pod creates), its own namespace (nothing to mount but the pull
# credential), no ServiceAccount token, `restricted` Pod Security, and egress to DNS and the data
# plane only. A manifest that declares that is not a manifest that enforces it — Pod Security and
# NetworkPolicy are admission and CNI, so only the cluster can say.
#
#   make verify-spark-demo-sandbox          (NS=… for the platform namespace, DEMO_NS=… for the sandbox)
#
# Read-only apart from `kubectl exec` in the driver pod. Exit 0 = PASS, 1 = FAIL,
# 3 = UNVERIFIED (no kubectl, or the sandbox is not installed).
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NS="${NS:-${NAMESPACE:-stardelt}}"
DEMO_NS="${DEMO_NS:-stardelt-demo-spark}"
S3_HOST="seaweedfs-s3.${NS}.svc.cluster.local"
S3_PORT=8333

probes=0 ok=0 bad=0
pass() { probes=$((probes + 1)); ok=$((ok + 1)); printf '✓ %-17s%s\n' "$1" "$2"; }
fail() { probes=$((probes + 1)); bad=$((bad + 1)); printf '✗ %-17s%s\n' "$1" "$2"; }

command -v kubectl >/dev/null 2>&1 || {
  echo "verify-spark-demo-sandbox: UNVERIFIED (kubectl not found)"; exit 3; }

if ! kubectl -n "$DEMO_NS" get deployment spark-demo >/dev/null 2>&1; then
  echo "verify-spark-demo-sandbox: UNVERIFIED (no deployment spark-demo in $DEMO_NS — make install-spark-demo)"
  exit 3
fi

# One exec in the driver. Its output is what the probe judges; a failure to exec at all (no
# pod, no container) is a probe failure, not UNVERIFIED: the sandbox exists but is not usable.
# $1 = python3 program; prints the command's message, exits non-zero when exec itself failed.
# A failed program prints a traceback, so the judged line is the last one: `Traceback (most
# recent call last):` is never the evidence, the exception on the final line is (measured in
# the sandbox 2026-11-19).
exec_in_driver() {
  local out
  if ! out="$(kubectl -n "$DEMO_NS" exec deploy/spark-demo -- python3 -c "$1" 2>&1)"; then
    # Strip kubectl's own trailer so the judged line is the program's message.
    printf '%s\n' "$out" | grep -v '^command terminated with exit code' \
      | grep -E '[^[:space:]]' | tail -n 1 || true
    return 1
  fi
  printf '%s\n' "$out"
}

# ---------------------------------------------------------------------------
# 1. The namespace enforces `restricted`, so a pod that does not declare non-root,
#    no privilege escalation, dropped capabilities and RuntimeDefault is refused.
# ---------------------------------------------------------------------------
pss="$(kubectl get ns "$DEMO_NS" -o jsonpath='{.metadata.labels.pod-security\.kubernetes\.io/enforce}' 2>/dev/null || true)"
if [ "$pss" = "restricted" ]; then
  pass namespace "$DEMO_NS enforces restricted"
else
  fail namespace "$DEMO_NS enforces '${pss:-nothing}', not restricted"
fi

# ---------------------------------------------------------------------------
# 2. The ServiceAccount has no rights. It has no Role or Binding at all, so even the
#    pod-create right that makes the shared server's driver dangerous is absent.
# ---------------------------------------------------------------------------
can="$(kubectl auth can-i create pods -n "$DEMO_NS" \
  --as="system:serviceaccount:${DEMO_NS}:spark-demo" 2>/dev/null || true)"
if [ "$can" = "no" ]; then
  pass "service account" "spark-demo cannot create pods in its namespace"
else
  fail "service account" "spark-demo can create pods in its namespace (can-i: ${can:-no answer})"
fi

# ---------------------------------------------------------------------------
# 3. No token is mounted. A pod can read any Secret of its own namespace with a token
#    only if it has one and a right; both are absent here, and the mount itself is too.
# ---------------------------------------------------------------------------
sa_dir=/var/run/secrets/kubernetes.io/serviceaccount
if out="$(kubectl -n "$DEMO_NS" exec deploy/spark-demo -- ls "$sa_dir" 2>&1)"; then
  fail "no token" "$sa_dir is present ($(printf '%s' "$out" | tr '\n' ' '))"
else
  if printf '%s' "$out" | grep -q 'No such file or directory'; then
    pass "no token" "$sa_dir is empty"
  else
    fail "no token" "$sa_dir could not be read: $(printf '%s\n' "$out" | grep -v '^command terminated' | grep -m1 -E '[^[:space:]]' || echo 'no output')"
  fi
fi

# ---------------------------------------------------------------------------
# 4. No internet. A UDF cannot exfiltrate and cannot download a tool to do it with.
#
#    This judges *reachability*, not name resolution: DNS is a deliberate allowance (the pod
#    must find its own services, and CoreDNS forwards `.` to the node resolver), so
#    `example.com` resolves — measured 2026-11-19: getaddrinfo answers 172.66.147.243 while
#    `create_connection(("example.com", 443), 5)` raises `Errno 101`. A resolution answer
#    therefore proves nothing; the connect is the property.
# ---------------------------------------------------------------------------
# Prints the address it reached, so a failure says where (never a bare "connected").
NET_PROBE='import socket; socket.setdefaulttimeout(5); s=socket.create_connection(("example.com",443),5); print(s.getpeername()[0])'
if out="$(exec_in_driver "$NET_PROBE")"; then
  fail "no internet" "example.com:443 is reachable from the driver (${out:-connected})"
elif printf '%s' "$out" | grep -qE 'Network is unreachable|Connection refused'; then
  pass "no internet" "example.com:443 is unreachable from the driver"
elif printf '%s' "$out" | grep -qE 'timed out|TimeoutError'; then
  # A dropped packet looks like a refusal, and like the 2026-11-19 01:06Z datapath fault: name
  # both, so the message is not read as "the policy works" when the node is the cause.
  fail "no internet" "example.com:443 timed out — denied by policy, or the node's pod network is degraded"
elif printf '%s' "$out" | grep -qE 'Temporary failure in name resolution|Name or service not known|No address associated'; then
  fail "no internet" "example.com:443 did not resolve, which proves nothing about reachability (DNS is allowed)"
else
  fail "no internet" "example.com:443 connect ended in something other than a refusal: ${out:-no output}"
fi

# ---------------------------------------------------------------------------
# 5. The data plane is reachable, which is what slice 2 (vended credentials) needs; the
#    catalog port 8181 is in the same egress rule and is judged by that rule's existence.
# ---------------------------------------------------------------------------
if out="$(exec_in_driver "import socket; s=socket.create_connection((\"${S3_HOST}\",${S3_PORT}),5); s.close(); print('connected')")"; then
  if [ "$out" = "connected" ]; then
    pass "data plane" "${S3_HOST}:${S3_PORT} accepts a connection"
  else
    fail "data plane" "${S3_HOST}:${S3_PORT} answered without connecting: ${out}"
  fi
else
  fail "data plane" "${S3_HOST}:${S3_PORT} is not reachable: ${out:-no output}"
fi

# ---------------------------------------------------------------------------
# 6. The Deployment is up. Last, so a sandbox that is down still says what it contains.
# ---------------------------------------------------------------------------
ready="$(kubectl -n "$DEMO_NS" get deploy spark-demo -o jsonpath='{.status.readyReplicas}/{.status.replicas}' 2>/dev/null || true)"
if [ "$ready" = "1/1" ]; then
  pass deployment "spark-demo 1/1 ready"
else
  fail deployment "spark-demo ${ready:-0/0} ready"
fi

# ---------------------------------------------------------------------------
# 7. The Deployment runs the image manifests/spark.yaml pins. `make install-spark-demo` renders
#    the manifest from that line, and nothing compares the live pod with it afterwards: a
#    roll that does not re-apply the manifest leaves the sandbox on an older digest (the
#    stardelt/stardelt#224 class, one level down). Compared by digest, not by tag.
# ---------------------------------------------------------------------------
pin="$(sed -nE 's/^[[:space:]]*spark\.kubernetes\.container\.image[[:space:]]+(ghcr\.io\/stardelt\/spark[^[:space:]]+)[[:space:]]*$/\1/p' manifests/spark.yaml)"
live="$(kubectl -n "$DEMO_NS" get pod -l app.kubernetes.io/name=spark-demo -o jsonpath='{.items[0].spec.containers[0].image}' 2>/dev/null || true)"
pin_digest="${pin##*@}"
live_digest="${live##*@}"
if [ -z "$pin_digest" ] || [ -z "$live_digest" ]; then
  fail image "pin '${pin:-none}', live '${live:-none}' — make install-spark-demo"
elif [ "$live_digest" = "$pin_digest" ]; then
  pass image "spark-demo runs ${live_digest:0:19}… (the pin)"
else
  fail image "spark-demo runs ${live_digest:0:19}…, the pin is ${pin_digest:0:19}… — make install-spark-demo"
fi

if [ "$bad" -eq 0 ]; then
  echo "verify-spark-demo-sandbox: PASS (${ok} of ${probes})"
  exit 0
fi
echo "verify-spark-demo-sandbox: FAIL (${bad} of ${probes})"
exit 1
