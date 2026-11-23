#!/usr/bin/env bash
# Ask answers end to end, against a stub model (stardelt/stardelt#1439).
#
#   NOVA_URL=https://<host> make verify-ask
#   NOVA_URL=https://<host> NAMESPACE=stardelt bash scripts/verify-ask.sh
#
# Nova's probe (`nova/backend/src/model_probe.rs`) and Ask (`nova/backend/src/ask.rs`) are
# each tested on their own functions; nothing measured the chain on an install: a
# `ModelEndpoint` written as a customer writes it (no `keyRef`), probed `Ready`, then one
# question answered with a statement that `EXPLAIN` accepted as the caller. This does:
#
#   probe   ConfigMap+Pod+Service `model-stub` serve `scripts/model-stub.py` (the smallest
#           OpenAI-compatible endpoint the probe and Ask accept); the `ModelEndpoint`
#           `model-stub` is created keyless, and within 120 s its `status` carries
#           `Ready=True`, `reachable`, `toolCalling` and `jsonOutput` true
#   ask     a token for the person `product-path` (Secret `product-path-user`), then
#           `POST /api/ask {"question": "customers shop"}` → 200, `endpoint` `model-stub`,
#           `sql` the stub's statement, `explain` `passed`
#
# The question is two words on purpose: Ask passes it verbatim to `catalog_search`, which
# requires **every** word to appear in `namespace.table` or the table's documentation, so a
# natural question matches no table and `/api/ask` answers `200` with `sql: null` before the
# model is called (measured 2026-11-23 on the lab). That is Ask's retrieval defect, filed
# separately; this check measures the platform chain (search → model → EXPLAIN).
#
# What is created is deleted on every exit: the `ModelEndpoint` first (a leftover endpoint
# would be picked up by Ask for the next person), then the Pod, the Service and the
# ConfigMap. The last check proves the endpoint is gone.
#
# A check fixture, never a shipped component: the Pod runs `python:3.13-slim` (the image
# `scripts/bridge-capture.sh` already uses) as a non-root user and passes the `restricted`
# floor of `manifests/namespace.yaml`.
#
# The password comes from Secret `product-path-user` and the token lives in a 0600 file in a
# temp dir (removed on every exit); neither is printed.
#
# Exit 0 = PASS (2 of 2), 1 = FAIL (each row still ran; the ✗ line names why), 3 = UNVERIFIED
# (nothing was asked: no NOVA_URL, no kubectl, no namespace, no Secret `product-path-user`).
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

NAMESPACE="${NAMESPACE:-stardelt}"
NOVA_URL="${NOVA_URL:-}"
NOVA_URL="${NOVA_URL%/}"
PERSON=product-path

case "$NOVA_URL" in
  http://*|https://*) ;;
  *)
    echo "verify-ask: UNVERIFIED (no Nova URL — set NOVA_URL=https://<host>)"
    exit 3
    ;;
esac
ISSUER="${ISSUER:-https://id.${NOVA_URL#*://}/realms/stardelt}"
ISSUER="${ISSUER%/}"

command -v kubectl >/dev/null 2>&1 || { echo "verify-ask: UNVERIFIED (kubectl not found)"; exit 3; }
command -v curl >/dev/null 2>&1 || { echo "verify-ask: UNVERIFIED (curl not found)"; exit 3; }
command -v python3 >/dev/null 2>&1 || { echo "verify-ask: UNVERIFIED (python3 not found)"; exit 3; }

if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "verify-ask: UNVERIFIED (namespace $NAMESPACE does not exist — check KUBECONFIG and the kube-context)"
  exit 3
fi
umask 077
tmp="$(mktemp -d "${TMPDIR:-/tmp}/verify-ask.XXXXXX")"

CLEANED=0
delete_objects() { # delete_objects: the four scratch objects, endpoint first. Idempotent.
  # The endpoint goes first: a leftover one would be picked up by Ask for the next person.
  kubectl -n "$NAMESPACE" delete modelendpoint model-stub --ignore-not-found >/dev/null 2>&1 || true
  # The pod takes the container's termination grace (~30 s); waiting keeps the next run — and
  # the ticket's own check — from finding it still Terminating.
  kubectl -n "$NAMESPACE" delete pod model-stub --ignore-not-found --timeout=120s >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete service model-stub --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete configmap model-stub --ignore-not-found >/dev/null 2>&1 || true
}
cleanup() {
  [ "$CLEANED" = 1 ] && return 0
  CLEANED=1
  delete_objects
  rm -rf "$tmp"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# A pod from an interrupted run cannot be replaced by one of the same name while it is
# terminating, and a probe that finds a closing port is not retried for a day.
delete_objects
for i in $(seq 1 24); do
  kubectl -n "$NAMESPACE" get pod model-stub >/dev/null 2>&1 || break
  [ "$i" -eq 24 ] && { echo "verify-ask: FAIL (setup) pod model-stub is still terminating after 120 s"; exit 1; }
  sleep 5
done

# The person's password: read from the Secret straight into the 0600 temp dir, never printed.
if ! kubectl -n "$NAMESPACE" get secret product-path-user -o jsonpath='{.data.password}' 2>/dev/null \
   | base64 -d > "$tmp/password" || [ ! -s "$tmp/password" ]; then
  echo "verify-ask: UNVERIFIED (Secret product-path-user is absent or carries no password — run make public-demo YES=1)"
  exit 3
fi
chmod 600 "$tmp/password"

PASSED=0
FAILED=0

row() { # row <id> <function>: one line of output, `✓ …` or `✗ <id> <reason>`.
  local out
  out="$("$2")" || true
  [ -n "$out" ] || out="FAIL the $1 row printed nothing"
  case "$out" in
    "✓ "*) printf '✓ %-16s %s\n' "$1" "${out#✓ }"; PASSED=$((PASSED + 1)) ;;
    FAIL\ *) printf '✗ %-16s %s\n' "$1" "${out#FAIL }"; FAILED=$((FAILED + 1)) ;;
    *) printf '✗ %-16s %s\n' "$1" "$out"; FAILED=$((FAILED + 1)) ;;
  esac
}

# shellcheck disable=SC2329  # called by row_ask
one_line() { # one_line <file> <max>: the body in one line, cut for a failure message.
  tr -d '\n' < "$1" 2>/dev/null | cut -c1-"${2:-400}"
}

# ---- a: the stub's source as a ConfigMap ---------------------------------------------------

if ! kubectl -n "$NAMESPACE" create configmap model-stub \
      --from-file=model-stub.py=scripts/model-stub.py --dry-run=client -o yaml \
     | kubectl -n "$NAMESPACE" apply -f - >"$tmp/apply.log" 2>&1; then
  echo "verify-ask: FAIL (setup) cannot write ConfigMap model-stub: $(tail -n 1 "$tmp/apply.log" | cut -c1-300)"
  exit 1
fi

# ---- b: the stub as a Pod and a Service ----------------------------------------------------

if ! kubectl -n "$NAMESPACE" apply -f - >"$tmp/apply.log" 2>&1 <<'YAML'
apiVersion: v1
kind: Pod
metadata:
  name: model-stub
  labels:
    app: model-stub
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: model-stub
      image: python:3.13-slim
      command: ["python3", "/stub/model-stub.py", "8000"]
      env:
        - name: PYTHONDONTWRITEBYTECODE
          value: "1"
      ports:
        - name: http
          containerPort: 8000
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
        readOnlyRootFilesystem: true
      volumeMounts:
        - name: stub
          mountPath: /stub
          readOnly: true
        - name: tmp
          mountPath: /tmp
      readinessProbe:
        httpGet:
          path: /v1/models
          port: 8000
        periodSeconds: 5
  volumes:
    - name: stub
      configMap:
        name: model-stub
    - name: tmp
      emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: model-stub
spec:
  selector:
    app: model-stub
  ports:
    - name: http
      port: 8000
      targetPort: 8000
YAML
then
  echo "verify-ask: FAIL (setup) cannot create Pod/Service model-stub: $(tail -n 1 "$tmp/apply.log" | cut -c1-300)"
  exit 1
fi

# ---- c: the ModelEndpoint, keyless, probed Ready -------------------------------------------

# shellcheck disable=SC2329  # called by row_probe
pod_ready() { # pod_ready: 0 when the stub pod's Ready condition is True and the Service routes to it.
  [ "$(kubectl -n "$NAMESPACE" get pod model-stub \
        -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" = "True" ] \
  && [ -n "$(kubectl -n "$NAMESPACE" get endpoints model-stub \
        -o jsonpath='{.subsets[0].addresses[0].ip}' 2>/dev/null)" ]
}

# shellcheck disable=SC2329  # called by row() by name
row_probe() {
  local i verdict status
  # The endpoint is created only once the stub answers through its Service: Nova's first probe
  # of a new endpoint comes within a minute, and a probe that finds a closed port is not
  # retried for an interval (a day by default).
  for i in $(seq 1 24); do
    pod_ready && break
    if [ "$i" -eq 24 ]; then
      printf 'FAIL pod model-stub is not Ready after 120 s: %s\n' \
        "$(kubectl -n "$NAMESPACE" get pod model-stub -o wide 2>&1 | tr '\n' ' ' | cut -c1-300)"
      return 1
    fi
    sleep 5
  done
  # The Service's routing rules follow the endpoints by a watch; a probe in the same instant
  # can still land before them.
  sleep 2

  if ! kubectl -n "$NAMESPACE" apply -f - >"$tmp/apply.log" 2>&1 <<YAML
apiVersion: stardelt.io/v1alpha1
kind: ModelEndpoint
metadata:
  name: model-stub
spec:
  url: http://model-stub.${NAMESPACE}.svc.cluster.local:8000/v1
  model: stub
  purposes:
    - ask
YAML
  then
    echo "FAIL cannot create ModelEndpoint model-stub: $(tail -n 1 "$tmp/apply.log" | cut -c1-300)"
    return 1
  fi

  # A new endpoint is probed on the next round (one a minute); 120 s is two rounds.
  status=""
  for i in $(seq 1 24); do
    status="$(kubectl -n "$NAMESPACE" get modelendpoint model-stub -o jsonpath='{.status}' 2>/dev/null || true)"
    verdict="$(printf '%s' "$status" | python3 -c '
import json, sys

raw = sys.stdin.read().strip()
if not raw:
    print("WAIT")
    raise SystemExit
try:
    doc = json.loads(raw)
except ValueError:
    print("WAIT")
    raise SystemExit
conditions = {c.get("type"): c.get("status") for c in doc.get("conditions") or [] if isinstance(c, dict)}
if (conditions.get("Ready") == "True" and doc.get("reachable") is True
        and doc.get("toolCalling") is True and doc.get("jsonOutput") is True):
    print("✓ ModelEndpoint model-stub Ready=True reachable=true toolCalling=true jsonOutput=true contextTokens=%s"
          % doc.get("contextTokens"))
else:
    print("WAIT")
' 2>/dev/null || true)"
    case "$verdict" in
      "✓ "*) printf '%s\n' "$verdict"; return 0 ;;
    esac
    [ "$i" -eq 24 ] && break
    sleep 5
  done
  printf 'FAIL ModelEndpoint model-stub is not Ready after 120 s (status: %s)\n' "${status:-none}"
  return 1
}

# ---- d: Ask answers the question with checked SQL ------------------------------------------

# shellcheck disable=SC2329  # called by row() by name
row_ask() {
  local code detail
  if ! ISSUER="$ISSUER" USER="$PERSON" PASSWORD_FILE="$tmp/password" TOKEN_FILE="$tmp/token" \
       python3 scripts/mint-user-token.py >"$tmp/mint.log" 2>&1; then
    printf 'FAIL cannot mint a token for %s: %s\n' "$PERSON" "$(tail -n 1 "$tmp/mint.log" | cut -c1-200)"
    return 1
  fi
  if [ ! -s "$tmp/token" ]; then
    echo "FAIL minting a token for $PERSON wrote no token"
    return 1
  fi
  # The bearer token travels in a curl config file, never on the command line.
  printf 'header = "Authorization: Bearer %s"\n' "$(tr -d '\n' < "$tmp/token")" > "$tmp/auth.curlrc"
  chmod 600 "$tmp/auth.curlrc"
  printf '{"question": "customers shop"}' > "$tmp/ask.json"

  code="$(curl -sS --connect-timeout 10 --max-time 60 -K "$tmp/auth.curlrc" \
    -H 'content-type: application/json' --data-binary "@$tmp/ask.json" \
    -o "$tmp/body" -w '%{http_code}' "$NOVA_URL/api/ask" 2>"$tmp/err")" || true
  if [ "$code" != "200" ]; then
    printf 'FAIL POST /api/ask → %s: %s\n' "${code:-000}" "$(one_line "$tmp/body")"
    return 1
  fi
  if detail="$(python3 - "$tmp/body" <<'PY'
import json, sys

try:
    doc = json.load(open(sys.argv[1]))
except (OSError, ValueError) as exc:
    print("the answer is not JSON: %s" % exc)
    sys.exit(1)
if not isinstance(doc, dict):
    print("the answer is not a JSON object")
    sys.exit(1)
endpoint, sql, explain = doc.get("endpoint"), doc.get("sql"), doc.get("explain")
if endpoint != "model-stub":
    print("endpoint=%r (want model-stub)" % (endpoint,))
    sys.exit(1)
if sql != "SELECT 1 AS one":
    print("sql=%r (want SELECT 1 AS one)" % (sql,))
    sys.exit(1)
if explain != "passed":
    print("explain=%r (want passed)" % (explain,))
    sys.exit(1)
print("POST /api/ask → 200, endpoint=%s, explain=%s" % (endpoint, explain))
PY
)"; then
    printf '✓ %s\n' "$detail"
  else
    printf 'FAIL POST /api/ask → 200, %s: %s\n' "$detail" "$(one_line "$tmp/body")"
    return 1
  fi
}

row probe row_probe
row ask row_ask

# ---- the scratch objects are gone, the endpoint first --------------------------------------

cleanup
if kubectl -n "$NAMESPACE" get modelendpoint model-stub >/dev/null 2>&1; then
  printf '✗ %-16s %s\n' cleanup "ModelEndpoint model-stub is still there after the check"
  FAILED=$((FAILED + 1))
fi

if [ "$PASSED" -eq 2 ] && [ "$FAILED" -eq 0 ]; then
  echo "PASS (2 of 2)"
  exit 0
fi
echo "verify-ask: FAIL ($PASSED of 2)"
exit 1
