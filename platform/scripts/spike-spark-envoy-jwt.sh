#!/usr/bin/env bash
# Spike for stardelt#208 (Q1 and Q2 of stardelt#84): is a JWT-checking proxy in front of the
# shared Spark Connect server transparent to its gRPC, and does it refuse a caller without a
# token?
#
#   Q1  does a streaming gRPC query (ExecutePlan + ReattachExecute, not one unary call) survive
#       `envoy.filters.http.jwt_authn`, and what does a call without an Authorization header get?
#   Q2  what does the Spark Connect server itself see of the caller — an identity, or only a
#       session id?
#
# One disposable namespace `spark-jwt-spike` holds Envoy, the client pod and the key/token this
# spike generates (no identity provider is needed); the trap deletes the namespace on exit and on
# SIGINT. Every answer is one `Q1:`/`Q2:` line on stdout, detail lines start with `  - `. A "no"
# is a successful measurement: the script exits 0 either way.
#
# Three optical facts about the client and the lab this script had to accept (measured
# 2026-11-18, stardelt#208):
#
#   * `;token=` in a Connect string forces TLS in the client, not only an Authorization header:
#     pyspark's DefaultChannelBuilder.secure is `use_ssl or token is not None`, and the else
#     branch is `grpc.ssl_channel_credentials()`. So the front terminates TLS and the spike's
#     listener has a TLS filter chain (self-signed, its own CA) beside the plaintext h2c one; the
#     signed leg arrives over TLS, the unsigned leg over plaintext. No way around it: with a
#     token and a non-localhost host, gRPC refuses a plaintext channel.
#   * `spark-connect-ingress` in `stardelt` admits port 15002 only from pods in the same
#     namespace labelled `component=singleuser-server`. A proxy in another namespace is refused
#     by the CNI, so the script adds one *additive* NetworkPolicy `spike-spark-envoy-jwt-allow`
#     to `stardelt` (ingress to spark-connect from namespace spark-jwt-spike, port 15002) and
#     deletes it in the trap. Nothing else in `stardelt` is touched; the name starts with
#     `spike-` so it never reads as part of the platform.
#   * the client needs the pyspark of the pinned Spark image plus its Python dependencies; the
#     pod installs them into an emptyDir at start, so the pod needs internet egress (the Spark
#     pods have it).
#
# No secret is printed: the token is written to a temp file, handed to the cluster as a Secret,
# and only its presence in Envoy's access log is reported (redacted). Needs kubectl, openssl,
# python3 and base64.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NS="spark-jwt-spike"
UPSTREAM_NS="stardelt"
UPSTREAM_HOST="spark-connect.stardelt.svc.cluster.local"
UPSTREAM_PORT="15002"
PROXY_HOST="spark-jwt-spike-envoy.spark-jwt-spike.svc.cluster.local"
PROXY="${PROXY_HOST}:15002"
# envoyproxy/envoy:v1.35.0, the multi-arch index digest (docker buildx imagetools inspect,
# 2026-11-18). The manifest below ships nowhere; the digest is pinned so a rerun measures the
# same bytes.
ENVOY_IMAGE="envoyproxy/envoy:v1.35.0@sha256:cfe1c570e72aa4207853a37bd1c32c808a0036a351b16f73732dcfd0e5775175"
# The image the platform pins for the driver and the executors (manifests/spark.yaml).
SPARK_IMAGE="$(grep -m1 'spark\.kubernetes\.container\.image' "$ROOT/platform/manifests/spark.yaml" | awk '{print $2}')"
: "${SPARK_IMAGE:?no spark.kubernetes.container.image in manifests/spark.yaml}"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/spark-envoy-jwt.XXXXXX")"

cleanup() {
  kubectl -n "$UPSTREAM_NS" delete networkpolicy spike-spark-envoy-jwt-allow --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete namespace "$NS" --wait=false >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

# --- 1. a JWT this spike can verify without an identity provider -------------------------
# 2048-bit RS256, JWKS built from the key's modulus, token signed with the key. Nothing here
# leaves $tmp except the public JWKS (Envoy's config) and the token (a Secret).
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$tmp/key.pem" 2>/dev/null
openssl rsa -in "$tmp/key.pem" -noout -modulus | cut -d= -f2 | tr -d '\n' > "$tmp/modulus.hex"

python3 - "$tmp" <<'PY'
import base64, json, os, sys, time

tmp = sys.argv[1]
b64url = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()

n_hex = open(os.path.join(tmp, "modulus.hex")).read().strip()
jwks = {"keys": [{"kty": "RSA", "kid": "spike", "use": "sig", "alg": "RS256",
                  "n": b64url(bytes.fromhex(n_hex)), "e": "AQAB"}]}
with open(os.path.join(tmp, "jwks.json"), "w") as f:
    json.dump(jwks, f)

header = {"alg": "RS256", "typ": "JWT", "kid": "spike"}
payload = {"iss": "spike", "aud": "spark-connect", "sub": "alice", "exp": int(time.time()) + 3600}
with open(os.path.join(tmp, "signing-input"), "wb") as f:
    f.write(b".".join(b64url(json.dumps(part, separators=(",", ":")).encode()).encode()
                      for part in (header, payload)))
PY
openssl dgst -sha256 -sign "$tmp/key.pem" -out "$tmp/signature.bin" "$tmp/signing-input"
{
  cat "$tmp/signing-input"
  printf '.'
  base64 -w0 < "$tmp/signature.bin" | tr '+/' '-_' | tr -d '='
} > "$tmp/token"
chmod 600 "$tmp/token"

# The front's own TLS certificate: the client only speaks TLS when the string carries a token,
# so one listener port carries two filter chains (plaintext h2c and TLS). Self-signed, one day,
# SANs for the service names a client may use.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$tmp/tls.key" -out "$tmp/tls.crt" -subj "/CN=spark-jwt-spike-envoy" \
  -addext "subjectAltName=DNS:spark-jwt-spike-envoy,DNS:spark-jwt-spike-envoy.spark-jwt-spike,DNS:${PROXY_HOST}" \
  2>/dev/null

# --- 2. Envoy in front of the shared server ---------------------------------------------
# A namespace a killed or timed-out run left behind blocks every apply below: own the name.
kubectl delete namespace "$NS" --ignore-not-found --timeout=240s >/dev/null 2>&1 || true
kubectl create namespace "$NS" >/dev/null
kubectl -n "$NS" create secret generic spike-token --from-file=token="$tmp/token" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NS" create secret tls spike-envoy-tls --cert="$tmp/tls.crt" --key="$tmp/tls.key" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n "$NS" create configmap spike-ca --from-file=ca.crt="$tmp/tls.crt" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

cat > "$tmp/envoy.yaml" <<EOF
admin:
  address:
    socket_address: {address: 127.0.0.1, port_value: 9901}
static_resources:
  listeners:
    - name: spark-connect
      address:
        socket_address: {address: 0.0.0.0, port_value: ${UPSTREAM_PORT}}
      # Without the TLS inspector a chain match on transport_protocol never sees "tls", and the
      # plaintext chain would answer a TLS ClientHello with an h2c error.
      listener_filters:
        - name: envoy.filters.listener.tls_inspector
          typed_config:
            "@type": type.googleapis.com/envoy.extensions.filters.listener.tls_inspector.v3.TlsInspector
      filter_chains:
        # The Spark Connect client's signed leg: a token in the URL forces TLS.
        - filter_chain_match:
            transport_protocol: tls
          transport_socket:
            name: envoy.transport_sockets.tls
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
              common_tls_context:
                alpn_protocols: [h2]
                tls_certificates:
                  - certificate_chain: {filename: /etc/envoy-tls/tls.crt}
                    private_key: {filename: /etc/envoy-tls/tls.key}
          filters: &filters
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: spark_connect
                # AUTO = h2c/h2 for gRPC: Spark Connect speaks HTTP/2 without TLS.
                codec_type: AUTO
                # A query stream is silent while an executor works; the default 5 min idle
                # timeout would cut it. Same reason the route has timeout: 0s.
                stream_idle_timeout: 0s
                access_log:
                  - name: envoy.access_loggers.stdout
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.access_loggers.stream.v3.StdoutAccessLog
                      log_format:
                        text_format_source:
                          inline_string: 'spike upstream=%UPSTREAM_HOST% authorization="%REQ(authorization)%" status=%RESPONSE_CODE% grpc_status=%RESP(grpc-status)%\n'
                route_config:
                  name: spark
                  virtual_hosts:
                    - name: spark
                      domains: ["*"]
                      routes:
                        - match: {prefix: "/"}
                          route:
                            cluster: spark
                            timeout: 0s
                http_filters:
                  - name: envoy.filters.http.jwt_authn
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.jwt_authn.v3.JwtAuthentication
                      providers:
                        spike:
                          issuer: spike
                          audiences: ["spark-connect"]
                          # Hand the verified JWT on: the upstream is where an identity would
                          # have to be read from.
                          forward: true
                          local_jwks:
                            inline_string: '$(tr -d '\n' < "$tmp/jwks.json")'
                      rules:
                        - match: {prefix: "/"}
                          requires: {provider_name: spike}
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
        # The same chain without TLS: an unsigned client, and the measurement of the refusal.
        - filters: *filters
  clusters:
    - name: spark
      connect_timeout: 5s
      type: STRICT_DNS
      dns_lookup_family: V4_ONLY
      load_assignment:
        cluster_name: spark
        endpoints:
          - lb_endpoints:
              - endpoint:
                  address:
                    socket_address: {address: ${UPSTREAM_HOST}, port_value: ${UPSTREAM_PORT}}
      typed_extension_protocol_options:
        envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
          "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
          explicit_http_config:
            # gRPC upstream: Envoy must not downgrade to HTTP/1.
            http2_protocol_options: {}
EOF

kubectl -n "$NS" create configmap envoy --from-file=envoy.yaml="$tmp/envoy.yaml" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-jwt-spike-envoy
  namespace: ${NS}
  labels: {app.kubernetes.io/name: spark-jwt-spike-envoy}
spec:
  replicas: 1
  selector:
    matchLabels: {app.kubernetes.io/name: spark-jwt-spike-envoy}
  template:
    metadata:
      labels: {app.kubernetes.io/name: spark-jwt-spike-envoy}
    spec:
      securityContext: {runAsNonRoot: true, runAsUser: 101, seccompProfile: {type: RuntimeDefault}}
      containers:
        - name: envoy
          image: ${ENVOY_IMAGE}
          imagePullPolicy: IfNotPresent
          args: ["-c", "/etc/envoy/envoy.yaml", "-l", "info"]
          ports:
            - {name: connect, containerPort: ${UPSTREAM_PORT}}
          readinessProbe:
            tcpSocket: {port: ${UPSTREAM_PORT}}
            initialDelaySeconds: 3
            periodSeconds: 3
          securityContext:
            allowPrivilegeEscalation: false
            capabilities: {drop: ["ALL"]}
          volumeMounts:
            - {name: config, mountPath: /etc/envoy, readOnly: true}
            - {name: tls, mountPath: /etc/envoy-tls, readOnly: true}
      volumes:
        - name: config
          configMap: {name: envoy}
        - name: tls
          secret: {secretName: spike-envoy-tls}
---
apiVersion: v1
kind: Service
metadata:
  name: spark-jwt-spike-envoy
  namespace: ${NS}
spec:
  selector: {app.kubernetes.io/name: spark-jwt-spike-envoy}
  ports:
    - {name: connect, port: ${UPSTREAM_PORT}, targetPort: ${UPSTREAM_PORT}}
EOF

# The lab's `spark-connect-ingress` admits only same-namespace notebook pods; a proxy in its own
# namespace needs an additive rule. Deleted by the trap.
kubectl apply -f - >/dev/null <<EOF
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spike-spark-envoy-jwt-allow
  namespace: ${UPSTREAM_NS}
  labels: {app.kubernetes.io/part-of: stardelt}
spec:
  podSelector:
    matchLabels: {app.kubernetes.io/name: spark-connect}
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels: {kubernetes.io/metadata.name: ${NS}}
      ports:
        - {port: ${UPSTREAM_PORT}, protocol: TCP}
EOF

# --- 3. a client that speaks Spark Connect ----------------------------------------------
cat > "$tmp/client.py" <<'PY'
import os, signal, sys
from pyspark.sql import SparkSession

HOST = os.environ["PROXY_URL"]
TOKEN = os.environ.get("TOKEN", "")


def first_line(exc):
    """The one line that says what happened: a gRPC error is a multi-line repr whose first
    line is only the exception's class, so prefer its status and details lines."""
    lines = [line.strip() for line in str(exc).strip().splitlines() if line.strip()]
    picked = [line for line in lines if "StatusCode." in line or "details =" in line]
    text = ", ".join(picked) if picked else (lines[0] if lines else type(exc).__name__)
    return text[:240]


def alert_after(seconds=240):
    def handler(_signum, _frame):
        raise TimeoutError("no answer within %ss" % seconds)
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(seconds)


def attempt(url):
    alert_after()
    try:
        return SparkSession.builder.remote(url).getOrCreate(), None
    except Exception as exc:  # noqa: BLE001 - the error text is the measurement
        return None, exc
    finally:
        signal.alarm(0)


def count(session, label):
    alert_after()
    try:
        frame = session.range(10) if label == "range10" else session.range(1000).repartition(4)
        print("%s=%s" % (label, frame.count()), flush=True)
    except Exception as exc:  # noqa: BLE001
        print("%s=error: %s" % (label, first_line(exc)), flush=True)
    finally:
        signal.alarm(0)


print("connecting signed (TLS + Bearer token)", flush=True)
signed, error = attempt("sc://%s/;token=%s" % (HOST, TOKEN))
if error is not None:
    print("signed: session error: %s" % first_line(error), flush=True)
else:
    count(signed, "range10")
    count(signed, "rep1000")  # an exchange: ExecutePlan + ReattachExecute, not one unary call
    try:
        signed.stop()
    except Exception:  # noqa: BLE001
        pass

# Without `;token=` the client keeps a plaintext channel and sends no Authorization header.
# getOrCreate alone proves nothing (it opens the channel lazily), so a real query is the test:
# the answer is either an error from the front or a row count.
print("connecting unsigned (plaintext, no token)", flush=True)
unsigned, error = attempt("sc://%s/" % HOST)
if error is not None:
    print("unsigned: refused: %s" % first_line(error), flush=True)
else:
    alert_after()
    try:
        print("unsigned: admitted (count=%s)" % unsigned.range(10).count(), flush=True)
    except Exception as exc:  # noqa: BLE001
        print("unsigned: refused: %s" % first_line(exc), flush=True)
    finally:
        signal.alarm(0)
    try:
        unsigned.stop()
    except Exception:  # noqa: BLE001
        pass
PY

kubectl -n "$NS" create configmap spike-client-script --from-file=client.py="$tmp/client.py" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: spike-client
  namespace: ${NS}
  labels: {app.kubernetes.io/name: spark-jwt-spike-client}
spec:
  restartPolicy: Never
  containers:
    - name: client
      image: ${SPARK_IMAGE}
      imagePullPolicy: IfNotPresent
      command: ["bash", "-c"]
      args:
        - |
          set -e
          export HOME=/tmp
          # pyspark lives in the image (/opt/spark/python) but its Python dependencies do not;
          # the Spark Connect client refuses to start without them.
          pip install -q --disable-pip-version-check --target /tmp/pylibs \
            "pandas>=2.2" pyarrow grpcio grpcio-status googleapis-common-protos protobuf zstandard \
            >/tmp/pip.log 2>&1 || { tail -n 5 /tmp/pip.log; exit 1; }
          export PYTHONPATH="/tmp/pylibs:/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.9-src.zip"
          exec python3 /opt/spike/client.py
      env:
        - name: PROXY_URL
          value: ${PROXY}
        # The front's certificate is self-signed; gRPC's C core reads its trust store here.
        - name: GRPC_DEFAULT_SSL_ROOTS_FILE_PATH
          value: /opt/spike-ca/ca.crt
        - name: TOKEN
          valueFrom:
            secretKeyRef: {name: spike-token, key: token}
      volumeMounts:
        - {name: script, mountPath: /opt/spike, readOnly: true}
        - {name: ca, mountPath: /opt/spike-ca, readOnly: true}
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: {drop: ["ALL"]}
  volumes:
    - name: script
      configMap: {name: spike-client-script}
    - name: ca
      configMap: {name: spike-ca}
EOF

echo "Q1: measuring gRPC through envoy's jwt_authn"
kubectl -n "$NS" wait --for=condition=Ready pod/spike-client --timeout=300s >/dev/null 2>&1 || true

# The client's own words, then the answers read off them.
for _ in $(seq 1 150); do
  phase="$(kubectl -n "$NS" get pod spike-client -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  case "${phase:-}" in Succeeded|Failed) break ;; esac
  sleep 2
done
kubectl -n "$NS" logs spike-client > "$tmp/client.log" 2>/dev/null || true
for line in connecting "signed:" range10 rep1000 unsigned; do
  sed -n "/^${line//./\.}/p" "$tmp/client.log" | sed 's/^/  - client: /' || true
done

signed_range="$(sed -n 's/^range10=//p' "$tmp/client.log" | tail -n1)"
signed_rep="$(sed -n 's/^rep1000=//p' "$tmp/client.log" | tail -n1)"
unsigned="$(sed -n 's/^unsigned: //p' "$tmp/client.log" | tail -n1)"

q1="FAIL"
if [ "${signed_range:-}" = "10" ] && [ "${signed_rep:-}" = "1000" ]; then q1="PASS"; fi
echo "Q1: gRPC through jwt_authn ${q1} (range(10)=${signed_range:-no answer}, range(1000).repartition(4)=${signed_rep:-no answer})"
if [ "$q1" = "FAIL" ]; then
  # A FAIL is only about the proxy when the upstream can run a job at all: say which it was.
  kubectl -n "$UPSTREAM_NS" get pods -l stardelt.io/spark-connect=executor \
    --no-headers -o custom-columns=NAME:.metadata.name,PHASE:.status.phase 2>/dev/null \
    | sed 's/^/  - upstream executor: /' || true
fi

case "${unsigned:-no answer}" in
  refused*) echo "Q1: an unsigned call REFUSED by envoy (${unsigned#refused: })" ;;
  admitted*) echo "Q1: an unsigned call ADMITTED (${unsigned#admitted })" ;;
  *) echo "Q1: an unsigned call NOT MEASURED (${unsigned:-no answer})" ;;
esac

# The access log is the only place the forwarded header shows: report presence, not the value.
kubectl -n "$NS" logs deploy/spark-jwt-spike-envoy > "$tmp/envoy.log" 2>/dev/null || true
if grep -q 'authorization="Bearer ' "$tmp/envoy.log"; then
  echo "Q1: authorization header reached envoy yes: Bearer <redacted, $(grep -c 'authorization="Bearer ' "$tmp/envoy.log") request(s)>"
else
  echo "Q1: authorization header reached envoy no — the client sent none"
fi
grep -o 'status=[0-9]* grpc_status=[0-9]*' "$tmp/envoy.log" | sort | uniq -c | sed 's/^/  - access log: /' || true

# --- 4. what the Connect server itself saw ----------------------------------------------
echo "Q2: reading the shared server's log for the spike's session"
sessions="$(kubectl -n "$UPSTREAM_NS" logs deploy/spark-connect --since=10m 2>/dev/null \
  | grep -o 'Session with userId: [^ ]* and sessionId: [0-9a-f-]*' | tail -n 5 || true)"
if [ -z "$sessions" ]; then
  echo "Q2: the Connect server saw the caller NOT MEASURED (no session line in the last 10 min)"
else
  printf '%s\n' "$sessions" | sed 's/^/  - spark-connect: /'
  if printf '%s\n' "$sessions" | grep -q 'userId:  and sessionId:'; then
    echo "Q2: the Connect server saw the caller nothing but a session id (userId is empty for every session)"
  else
    echo "Q2: the Connect server saw the caller USERID_SET (grep the log above)"
  fi
fi
