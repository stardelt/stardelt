#!/usr/bin/env bash
# Spike for stardelt/stardelt#769 (#492 question 2, after #562/#650/#684): does a workload's
# *projected* ServiceAccount token carry a Spark job, and does it survive the token's rotation?
#
# #562 taught the catalog to accept a workload's ServiceAccount token, #650 gives every
# ServicePrincipal the ServiceAccount `sp-<name>`, and #684 adds `kubernetes~<uid>` to the
# owner group's catalog role. No job uses the token yet: every Spark run signs in with the
# identity provider's client secret (`CATALOG_CLIENT_ID` / `CATALOG_CLIENT_SECRET`). Two facts
# decide the next slices, and nothing measured them:
#
#   1. Does the pinned Iceberg 1.12.0 REST client accept a custom auth manager
#      (`spark.sql.catalog.<c>.rest.auth.type`) that reads the token file *on every request*,
#      so a rotated projected token is picked up without a restart — or is a static token read
#      at session build the only option?
#   2. What does a static token do when the Spark write outlives the token's rotation?
#
# This run measures five facts inside one pod of the pinned image, signed in only with the
# projected token mounted at /var/run/secrets/stardelt-catalog/token:
#
#   W1  the projected token authenticates at the catalog: its SHA-256 prefix, its remaining
#       life, and `GET /management/v1/whoami` (200, or 404 `UserNotFound` before the first
#       config call provisions the principal — both mean authentication ran).
#   W2  rotation: the pod waits 540 s (a projected token with `expirationSeconds: 600` is
#       rewritten at about 80 % of its life) and reads the file again.
#   W3  a Spark session in `local[1]` whose catalog token is the file's content read once at
#       session build writes a table in `playground.scratch` — no client secret, and no
#       `AWS_*` variable in the pod. Measured 2026-11-21: the catalog token authenticates and
#       the catalog creates the table, but the data-file write has no storage credential
#       (`playground` answers `sts-enabled: false` and vends nothing), so W3 is refused at S3.
#       W3b repeats the same write with the platform's own S3 Secret (the credential every
#       Spark run mounts today) to show that the *identity* half is the token's.
#   W4  the auth-manager SPI: the classes the pinned jar ships, `javac` in the image, and — when
#       both exist — a ~30-line `TokenFileAuthManager implements AuthManager` compiled in the
#       pod whose session is built *before* the rotation and whose write happens *after* it.
#   W5  the static-token consequence: the W3b session writes again after its build-time token
#       has expired. This is the measurement that decides whether the first client slice may
#       start with a static token.
#
# Execution order is W1, W3, W2, W5, W4's write: W3 has to run before the rotation (its token
# is the one W5 outlives), and W4's session has to be built before the rotation to prove that a
# *rotated* token is picked up by a session that already exists.
#
# The lab change, all reverted in `cleanup` on EXIT/INT/TERM:
#   - namespace `stardelt-spike-workload-token` and ServiceAccount `spike-workload` in it;
#   - one `assignee` on the existing `visitors` role for `kubernetes~<sa uid>`, re-applied every
#     20 s while the pod runs and deleted again. access-sync reconciles that role every two
#     minutes and removes an assignee that no ServicePrincipal backs (measured 2026-11-21); the
#     product path is a ServicePrincipal's owner-group catalog role (#650/#684), which is the
#     next slice's change, not this run's;
#   - one *additive* NetworkPolicy `spike-workload-token-allow` in the catalog's namespace:
#     NetworkPolicy `lakekeeper-ingress` admits named callers only, and a fresh workload
#     namespace is not one of them yet (a pod in a new namespace gets `connection refused` on
#     lakekeeper:8181, measured 2026-11-21). The policy is additive — the product's
#     `lakekeeper-ingress` is not edited — and it is deleted on every exit path.
#
# It prints no token, key or secret: names, status codes, seconds, row counts and SHA-256
# prefixes only. A token travels in a mode-0600 file (`-H @file`) or inside the pod; a
# credential in a log line is a defect.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
LK_PORT="${LK_PORT:-18181}"
AUDIENCE=stardelt-catalog-workload
SA_NS=stardelt-spike-workload-token
SA=spike-workload
POD=spike-workload-token
NETPOL=spike-workload-token-allow
WAREHOUSE=playground
ROTATE_AFTER=540

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/workload-token.XXXXXX")"
LK_PID=""
ROLE_ID=""
SA_UID=""
WID=""
ns_created=no
netpol_created=no
assigned=no
GRANT_PID=""

# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  local code name
  if [ -n "$GRANT_PID" ]; then
    rm -f "$tmp/grant-loop"
    wait "$GRANT_PID" 2>/dev/null || true
  fi
  # The two tables this run created, dropped through the catalog as the bootstrap service
  # account: the `visitors` role grants `write` and `create`, not `delete`.
  if [ -n "$WID" ] && [ -s "$tmp/auth" ]; then
    for name in workload_token workload_token_custom; do
      code="$(drop_table "$WID" scratch "$name" 2>/dev/null || echo 000)"
      echo "L cleanup drop-table=$name code=$code"
    done
  fi
  if [ "$assigned" = "yes" ] && [ -n "$ROLE_ID" ] && [ -s "$tmp/auth" ]; then
    jq -n --arg u "kubernetes~$SA_UID" \
      '{writes: [], deletes: [{type: "assignee", user: $u}]}' > "$tmp/unassign.json"
    echo "L cleanup role-unassign=$(mgmt POST "/management/v1/permissions/role/$ROLE_ID/assignments" "$tmp/unassign.json")"
  fi
  if [ "$netpol_created" = "yes" ]; then
    if kubectl -n "$NS" delete networkpolicy "$NETPOL" --ignore-not-found >/dev/null 2>&1; then
      echo "L cleanup delete-networkpolicy=ok"
    else
      echo "L cleanup delete-networkpolicy=fail"
    fi
  fi
  if [ "$ns_created" = "yes" ]; then
    if kubectl delete namespace "$SA_NS" --timeout=120s >/dev/null 2>&1; then
      echo "L cleanup delete-namespace=ok"
    else
      echo "L cleanup delete-namespace=fail"
    fi
  fi
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

for tool in kubectl curl jq python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done

# --- port-forward -------------------------------------------------------------------------
LK="http://127.0.0.1:$LK_PORT"

up() { # url -> 0 when anything but a transport failure answered
  local code
  code="$(curl -s -o /dev/null -m 2 -w '%{http_code}' "$1" 2>/dev/null)" || code="000"
  [ "${code:-000}" != "000" ]
}

forward_lk() {
  local _
  if [ -n "$LK_PID" ]; then kill "$LK_PID" >/dev/null 2>&1 || true; fi
  kubectl -n "$NS" port-forward svc/lakekeeper "$LK_PORT:8181" >/dev/null 2>&1 &
  LK_PID=$!
  for _ in $(seq 1 30); do
    if up "$LK/management/v1/info"; then return 0; fi
    sleep 1
  done
  return 1
}

if ! forward_lk; then
  echo "lakekeeper did not answer within 30s" >&2
  exit 1
fi

# --- management auth ---------------------------------------------------------------------
bootstrap_auth() {
  local issuer body
  kubectl -n "$NS" get secret svc-catalog-bootstrap -o jsonpath='{.data.client-id}' | base64 -d > "$tmp/client-id"
  kubectl -n "$NS" get secret svc-catalog-bootstrap -o jsonpath='{.data.client-secret}' | base64 -d > "$tmp/client-secret"
  issuer="$(kubectl -n "$NS" get configmap stardelt-identity -o jsonpath='{.data.issuerUrl}')"
  [ -n "$issuer" ] || { echo "ConfigMap stardelt-identity has no issuerUrl" >&2; exit 1; }
  if ! curl -fsS -X POST "${issuer%/}/protocol/openid-connect/token" \
      --data-urlencode grant_type=client_credentials \
      --data-urlencode client_id@"$tmp/client-id" \
      --data-urlencode client_secret@"$tmp/client-secret" \
      -o "$tmp/token.json" 2>/dev/null; then
    body="$(jq -r '.error // "no body"' "$tmp/token.json" 2>/dev/null || echo "no body")"
    echo "the realm's token endpoint refused svc-catalog-bootstrap: $body" >&2
    exit 1
  fi
  printf 'Authorization: Bearer %s\n' "$(jq -r .access_token "$tmp/token.json")" > "$tmp/auth"
}

mgmt() { # <method> <path> [data-file] -> status code, body in $tmp/out.json
  local method="$1" path="$2" data="${3-}" code refreshed=no reopened=no
  while :; do
    if [ -n "$data" ]; then
      code="$(curl -sS -H @"$tmp/auth" -H 'Content-Type: application/json' -X "$method" \
        -d @"$data" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    else
      code="$(curl -sS -H @"$tmp/auth" -X "$method" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    fi
    # Keycloak's client-credentials token lives 5 minutes; a long run must re-mint, not fail.
    if [ "$code" = "401" ] && [ "$refreshed" = "no" ]; then refreshed=yes; bootstrap_auth; continue; fi
    if [ "$code" = "000" ] && [ "$reopened" = "no" ]; then reopened=yes; forward_lk && continue; fi
    break
  done
  echo "${code:-000}"
}

# A call with any Bearer header file (the run's ServiceAccount tokens).
bearer() { # <method> <auth-file> <path> -> code, body in $tmp/out.json
  local method="$1" auth="$2" path="$3" code reopened=no
  while :; do
    code="$(curl -sS -H @"$auth" -X "$method" "$LK$path" -o "$tmp/out.json" -w '%{http_code}')" || code="000"
    if [ "$code" = "000" ] && [ "$reopened" = "no" ]; then reopened=yes; forward_lk && continue; fi
    break
  done
  echo "${code:-000}"
}

# `kubectl create token` writes the token to stdout only; it never reaches argv of curl.
mint_sa() { # <serviceaccount> <audience|none> <out-file>
  if [ "$2" = "none" ]; then
    kubectl -n "$SA_NS" create token "$1" --duration 10m > "$3"
  else
    kubectl -n "$SA_NS" create token "$1" --audience "$2" --duration 10m > "$3"
  fi
  chmod 600 "$3"
  printf 'Authorization: Bearer %s\n' "$(tr -d '\n' < "$3")" > "$3.auth"
  chmod 600 "$3.auth"
}

# The tables are dropped as the bootstrap account, which holds `delete` on the warehouse.
# shellcheck disable=SC2329  # called from the trap
drop_table() { # <warehouse-id> <namespace> <table> -> status code
  local code refreshed=no
  while :; do
    code="$(curl -sS -H @"$tmp/auth" -X DELETE \
      "$LK/catalog/v1/$1/namespaces/$2/tables/$3?purgeRequested=true" -o /dev/null -w '%{http_code}')" || code="000"
    # The run is longer than the bootstrap token's 5 minutes, so cleanup re-mints.
    if [ "$code" = "401" ] && [ "$refreshed" = "no" ]; then refreshed=yes; bootstrap_auth; continue; fi
    break
  done
  echo "${code:-000}"
}

# --- the workload's identity -------------------------------------------------------------
bootstrap_auth
if kubectl create namespace "$SA_NS" >/dev/null 2>&1; then ns_created=yes; fi
if [ "$ns_created" != "yes" ]; then
  echo "namespace $SA_NS already exists; not deleting something this run did not create" >&2
  exit 1
fi
kubectl label namespace "$SA_NS" \
  pod-security.kubernetes.io/enforce=restricted \
  pod-security.kubernetes.io/warn=restricted >/dev/null
kubectl -n "$SA_NS" create serviceaccount "$SA" >/dev/null
SA_UID="$(kubectl -n "$SA_NS" get sa "$SA" -o jsonpath='{.metadata.uid}')"
PRINCIPAL="kubernetes~$SA_UID"
echo "L principal=$PRINCIPAL"

# The platform's own S3 Secret, copied for the pod: every Spark run receives this key today,
# and the *identity* in the pod stays the token (W3b/W4/W5). The copy is deleted with the
# namespace; its values are never printed.
if kubectl -n "$NS" get secret stardelt-s3-creds >/dev/null 2>&1; then
  kubectl -n "$NS" get secret stardelt-s3-creds -o json \
    | jq --arg ns "$SA_NS" '{apiVersion: "v1", kind: "Secret", type: "Opaque",
        metadata: {name: "stardelt-s3-creds", namespace: $ns}, data: .data}' \
    | kubectl apply -f - >/dev/null
  echo "L s3-secret=copied (the storage half, as every Spark run gets it)"
else
  echo "L s3-secret=missing"
fi

# The first *config* call creates the catalog user (#538); its answer may still be 404 because
# the new principal holds no grant yet, but `whoami` afterwards names it.
mint_sa "$SA" "$AUDIENCE" "$tmp/sa-token"
echo "L config-provision=$(bearer GET "$tmp/sa-token.auth" "/catalog/v1/config?warehouse=$WAREHOUSE")"
code="$(bearer GET "$tmp/sa-token.auth" /management/v1/whoami)"
id="$(jq -r '.id // "none"' "$tmp/out.json" 2>/dev/null || echo none)"
echo "L principal-whoami=$code id-matches=$([ "$id" = "$PRINCIPAL" ] && echo yes || echo no)"

code="$(mgmt GET /management/v1/role)"
ROLE_ID="$(jq -r '.roles[]? | select(.name == "visitors") | .id' "$tmp/out.json" 2>/dev/null | head -n1)"
if [ -z "$ROLE_ID" ]; then
  echo "the catalog has no role 'visitors'; W3 cannot be granted" >&2
  exit 1
fi
jq -n --arg u "$PRINCIPAL" '{writes: [{type: "assignee", user: $u}], deletes: []}' > "$tmp/assign.json"
echo "L visitors-role=$ROLE_ID assign=$(mgmt POST "/management/v1/permissions/role/$ROLE_ID/assignments" "$tmp/assign.json")"
assigned=yes

# access-sync reconciles the `visitors` role every two minutes and removes an assignee that is
# not backed by a ServicePrincipal (measured 2026-11-21: the hand-made assignee was gone after
# the first cycle and the principal's config call went 404 again). The product path is a
# ServicePrincipal's owner-group catalog role (#650/#684); for this measurement the run keeps
# the one assignee applied until the pod has finished, and `cleanup` deletes it.
keep_grant() {
  local code
  while [ -f "$tmp/grant-loop" ]; do
    code="$(curl -sS -H @"$tmp/auth" -H 'Content-Type: application/json' -X POST \
      -d @"$tmp/assign.json" "$LK/management/v1/permissions/role/$ROLE_ID/assignments" \
      -o "$tmp/loop.json" -w '%{http_code}')" || code="000"
    if [ "$code" = "401" ]; then bootstrap_auth; fi
    sleep 20
  done
}
touch "$tmp/grant-loop"
keep_grant &
GRANT_PID=$!
echo "L grant-keeper=started (access-sync removes a hand-made assignee within one cycle)"

mgmt GET /management/v1/warehouse >/dev/null
WID="$(jq -r '(.warehouses // .)[] | select(.name == "'"$WAREHOUSE"'") | .id' "$tmp/out.json" 2>/dev/null | head -n1)"
echo "L warehouse=$WAREHOUSE id=${WID:-none}"
if [ -z "$WID" ] || [ "$WID" = "null" ]; then
  echo "no warehouse named $WAREHOUSE; the run cannot write" >&2
  exit 1
fi

# The catalog's authorisation cache answers 404 for 3-15 s after a grant change (#538).
sleep 20
code="$(bearer GET "$tmp/sa-token.auth" "/catalog/v1/config?warehouse=$WAREHOUSE")"
first="$code"
waited=0
if [ "$code" != "200" ]; then
  sleep 15
  waited=15
  code="$(bearer GET "$tmp/sa-token.auth" "/catalog/v1/config?warehouse=$WAREHOUSE")"
fi
echo "L grant-reached=$code after=${waited}s (first=$first)"

# --- the additive NetworkPolicy (see the header) -----------------------------------------
if ! kubectl -n "$NS" apply -f - >/dev/null 2>&1 <<EOF
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: ${NETPOL}
  namespace: ${NS}
  labels: { app.kubernetes.io/part-of: stardelt }
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: lakekeeper
      app.kubernetes.io/component: catalog
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: ${SA_NS}
      ports:
        - port: 8181
          protocol: TCP
EOF
then
  echo "the additive NetworkPolicy could not be created; the pod cannot reach the catalog" >&2
  exit 1
fi
netpol_created=yes

# --- the pod ------------------------------------------------------------------------------
image="$(grep -m1 -E '^[[:space:]]*image:[[:space:]]*ghcr\.io/stardelt/spark:' manifests/spark.yaml \
  | sed -E 's/^[[:space:]]*image:[[:space:]]*//')"
if [ -z "$image" ]; then
  echo "manifests/spark.yaml names no ghcr.io/stardelt/spark image" >&2
  exit 1
fi
echo "L image=$image"

cat > "$tmp/TokenFileAuthManager.java" <<'JAVA'
package spike;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;

/**
 * Authenticates every catalog request with the current content of a token file.
 *
 * <p>Iceberg builds a custom auth manager with `(String authType)` (or `(String, AuthManager)`),
 * so the token path arrives in the catalog properties of {@code catalogSession}.
 */
public class TokenFileAuthManager implements AuthManager {
  private volatile String path;

  public TokenFileAuthManager(String authType) {
    this.path = null;
  }

  private String path(Map<String, String> properties) {
    if (path == null) {
      String configured = properties == null ? null : properties.get("token-file");
      path = configured != null ? configured
          : System.getenv().getOrDefault("TOKEN_FILE", "/var/run/secrets/stardelt-catalog/token");
    }
    return path;
  }

  private AuthSession session(String tokenPath) {
    return new AuthSession() {
      @Override
      public HTTPRequest authenticate(HTTPRequest request) {
        try {
          String token = new String(Files.readAllBytes(Paths.get(tokenPath))).trim();
          return ImmutableHTTPRequest.builder()
              .from(request)
              .headers(request.headers().putIfAbsent(
                  HTTPHeaders.of(Collections.singletonMap("Authorization", "Bearer " + token))))
              .build();
        } catch (Exception e) {
          throw new RuntimeException("cannot read token file", e);
        }
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public AuthSession catalogSession(RESTClient client, Map<String, String> properties) {
    return session(path(properties));
  }

  @Override
  public void close() {}
}
JAVA

cat > "$tmp/run.py" <<'PY'
"""Spike #769: a workload pod's projected ServiceAccount token drives a Spark/Iceberg job."""
import base64
import datetime
import hashlib
import json
import os
import subprocess
import time
import urllib.error
import urllib.request

TOKEN_FILE = os.environ.get("TOKEN_FILE", "/var/run/secrets/stardelt-catalog/token")
CATALOG = os.environ.get("CATALOG_URI", "http://lakekeeper.stardelt.svc.cluster.local:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://seaweedfs-s3.stardelt.svc.cluster.local:8333")
ROTATE_AFTER = int(os.environ.get("ROTATE_AFTER", "540"))
ICEBERG_JAR = "/opt/spark/jars/iceberg-spark-runtime.jar"
AUTH_JAR = "/tmp/spike/token-file-auth.jar"
WAREHOUSE = "playground"
SCRATCH = "scratch"
TABLE = "workload_token"
TABLE_CUSTOM = "workload_token_custom"


def out(line):
    print(line, flush=True)


def token():
    with open(TOKEN_FILE) as handle:
        return handle.read().strip()


def digest(value):
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()[:16]


def claim(value, name):
    part = value.split(".")[1]
    part += "=" * (-len(part) % 4)
    return json.loads(base64.urlsafe_b64decode(part)).get(name)


def seconds_left(value):
    exp = claim(value, "exp")
    return int(exp - time.time()) if exp else -1


def first_line(exc):
    lines = [line.strip() for line in str(exc).splitlines() if line.strip()]
    # Py4J wraps a Java exception as "An error occurred while calling …" and puts the real
    # message on the next line, beginning with ':'. Take that one when it is there.
    for line in lines:
        if line.startswith(": "):
            return line[2:].strip()[:240]
    return (lines[0] if lines else type(exc).__name__)[:240]


def whoami(value):
    request = urllib.request.Request(
        CATALOG + "/management/v1/whoami", headers={"Authorization": "Bearer " + value})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, response.read(400).decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read(400).decode("utf-8", "replace")


def table_for(catalog, name):
    return "%s.%s.%s" % (catalog, SCRATCH, name)


started = time.time()
first = token()
code, body = whoami(first)
kind = ""
try:
    kind = json.loads(body)["error"]["type"]
except Exception:
    pass
out("W1: token=%s exp-in=%d whoami=%d%s" % (
    digest(first), seconds_left(first), code, (" error=" + kind) if kind else ""))

from pyspark.sql import SparkSession

# The token is read once, here, at session build: this is the static-token shape (W3, W5).
static = token()
session = SparkSession.builder.appName("workload-token-spike").getOrCreate()


def configure(name, values):
    prefix = "spark.sql.catalog." + name
    for key, value in values.items():
        session.conf.set(prefix + key, value)


common = {
    "": "org.apache.iceberg.spark.SparkCatalog",
    ".type": "rest",
    ".uri": CATALOG + "/catalog",
    ".warehouse": WAREHOUSE,
    ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    ".s3.endpoint": S3_ENDPOINT,
    ".s3.region": "us-east-1",
    ".s3.path-style-access": "true",
    ".header.X-Iceberg-Access-Delegation": "vended-credentials",
}
# The platform's own S3 Secret, as every Spark run receives it today. It is not an AWS_*
# variable and not the catalog's `credential`: the catalog identity stays the token.
storage = {}
if os.environ.get("S3_ACCESS_KEY"):
    storage = {".s3.access-key-id": os.environ["S3_ACCESS_KEY"],
               ".s3.secret-access-key": os.environ.get("S3_SECRET_KEY", "")}
configure("playground", dict(common, **{".token": static}))
configure("playgroundstatic", dict(dict(common, **storage), **{".token": static}))
configure("playgroundcustom", dict(dict(common, **storage), **{
    ".rest.auth.type": "spike.TokenFileAuthManager",
    ".token-file": TOKEN_FILE,
}))


def write(catalog, table):
    session.createDataFrame([(1,)], "x int").writeTo(table_for(catalog, table)).createOrReplace()
    return session.table(table_for(catalog, table)).count()


# --- W3: a write with the file's token, no client secret, before the rotation -------------
try:
    out("W3: write=ok rows=%d token-file=yes client-secret=no" % write("playground", TABLE))
except Exception as exc:
    out("W3: write=refused: %s token-file=yes client-secret=no" % first_line(exc))

# --- W3b: the same write with the platform's S3 Secret -----------------------------------
if storage:
    try:
        out("W3b: write=ok rows=%d storage=platform-secret client-secret=no" % write("playgroundstatic", TABLE))
    except Exception as exc:
        out("W3b: write=refused: %s storage=platform-secret client-secret=no" % first_line(exc))
else:
    out("W3b: not-testable (Secret stardelt-s3-creds not mounted)")

# Build the custom-auth catalog *before* the rotation, so W4 shows a rotated token being
# picked up by a client that already exists.
built = token()
try:
    session.sql("SHOW NAMESPACES IN playgroundcustom").collect()
    out("W4: custom-session-built token=%s" % digest(built))
except Exception as exc:
    out("W4: custom-session-built token=%s list=%s" % (digest(built), first_line(exc)))

# --- W2: rotation ------------------------------------------------------------------------
while time.time() < started + ROTATE_AFTER:
    time.sleep(5)
second = token()
mtime = datetime.datetime.fromtimestamp(os.path.getmtime(TOKEN_FILE), datetime.timezone.utc)
out("W2: rotated=%s exp-in-before=%d exp-in-after=%d mtime=%s" % (
    "yes" if second != first else "no",
    seconds_left(first), seconds_left(second), mtime.strftime("%Y-%m-%dT%H:%M:%SZ")))

# --- W5: the static token after it expired -----------------------------------------------
# The catalog answered a write ~10 s past the token's `exp` (measured 2026-11-21), so the
# attempt is repeated at three points; the first refusal is the answer.
refused = False
for offset in (10, 70, 130):
    while -seconds_left(static) < offset:
        time.sleep(5)
    out("W5: session-token=%s file-token=%s rotated-between=%s after-exp-s=%d" % (
        digest(static), digest(token()), "yes" if token() != static else "no", offset))
    try:
        out("W5: static-token-write-after-rotation=ok rows=%d after-exp-s=%d" % (
            write("playgroundstatic", TABLE), offset))
    except Exception as exc:
        out("W5: static-token-write-after-rotation=refused: %s after-exp-s=%d" % (
            first_line(exc), offset))
        refused = True
    if refused:
        break

# --- W4: the auth-manager SPI, and a write after the rotation ----------------------------
listing = subprocess.run(
    ["bash", "-lc", "jar tf %s | grep -E 'rest/auth|RESTCatalog' | sort" % ICEBERG_JAR],
    capture_output=True, text=True).stdout
names = sorted({line.rsplit("/", 1)[-1][: -len(".class")]
                for line in listing.split() if line.endswith(".class")})
spi = "yes" if {"AuthManager", "AuthConfig", "AuthSession"} <= set(names) else "no"
javac = "yes" if subprocess.run(["bash", "-lc", "command -v javac"],
                                capture_output=True).returncode == 0 else "no"
out("W4: auth-manager-spi=%s classes=%d interfaces=%s javac=%s" % (
    spi, len(names),
    ",".join(n for n in ("AuthManager", "AuthConfig", "AuthSession") if n in names), javac))
if spi == "yes" and javac == "yes" and os.path.exists(AUTH_JAR):
    out("W4: custom-session-token=%s file-token=%s rotated-between=%s" % (
        digest(built), digest(token()), "yes" if token() != built else "no"))
    try:
        out("W4: custom-auth-write-after-rotation=ok rows=%d" % write("playgroundcustom", TABLE_CUSTOM))
    except Exception as exc:
        out("W4: custom-auth-write-after-rotation=refused: %s" % first_line(exc))
else:
    out("W4: custom-auth=not-testable (%s)" % (
        "javac missing in the image" if javac == "no" else "the SPI is not in the pinned jar"))

session.stop()
PY

kubectl -n "$SA_NS" create configmap "$POD-script" \
  --from-file=run.py="$tmp/run.py" \
  --from-file=TokenFileAuthManager.java="$tmp/TokenFileAuthManager.java" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

if kubectl -n "$SA_NS" apply -f - >/dev/null 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${SA_NS}
  labels:
    app.kubernetes.io/name: ${POD}
spec:
  restartPolicy: Never
  serviceAccountName: ${SA}
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 185
    runAsGroup: 185
    fsGroup: 185
    seccompProfile: { type: RuntimeDefault }
  containers:
    - name: workload
      image: ${image}
      command: ["bash", "-lc"]
      args:
        - |
          set -e
          mkdir -p /tmp/spike/classes
          classpath=""
          if javac -cp /opt/spark/jars/iceberg-spark-runtime.jar -d /tmp/spike/classes \
              /opt/spike/TokenFileAuthManager.java >/dev/null 2>&1; then
            ( cd /tmp/spike/classes && jar cf /tmp/spike/token-file-auth.jar . ) >/dev/null 2>&1
            classpath=/tmp/spike/token-file-auth.jar
          fi
          exec /opt/spark/bin/spark-submit --master 'local[1]' \
            --conf spark.ui.enabled=false \
            --conf spark.driver.extraClassPath="\$classpath" \
            --conf spark.sql.shuffle.partitions=2 \
            /opt/spike/run.py
      env:
        - { name: HOME, value: /tmp }
        - { name: TOKEN_FILE, value: /var/run/secrets/stardelt-catalog/token }
        - { name: CATALOG_URI, value: "http://lakekeeper.${NS}.svc.cluster.local:8181" }
        - { name: S3_ENDPOINT, value: "http://seaweedfs-s3.${NS}.svc.cluster.local:8333" }
        - { name: ROTATE_AFTER, value: "${ROTATE_AFTER}" }
        # Not AWS_*, deliberately: the default credential chain stays empty (W3), and only the
        # catalogs W3b/W4/W5 use get these two as S3FileIO properties.
        - name: S3_ACCESS_KEY
          valueFrom: { secretKeyRef: { name: stardelt-s3-creds, key: access-key, optional: true } }
        - name: S3_SECRET_KEY
          valueFrom: { secretKeyRef: { name: stardelt-s3-creds, key: secret-key, optional: true } }
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: { drop: ["ALL"] }
      resources:
        requests: { cpu: 500m, memory: 1Gi }
        limits: { cpu: 2, memory: 3Gi }
      volumeMounts:
        - { name: catalog-token, mountPath: /var/run/secrets/stardelt-catalog, readOnly: true }
        - { name: script, mountPath: /opt/spike, readOnly: true }
  volumes:
    - name: catalog-token
      projected:
        sources:
          - serviceAccountToken:
              audience: ${AUDIENCE}
              expirationSeconds: 600
              path: token
    - name: script
      configMap: { name: ${POD}-script }
EOF
then
  started_s="$(date +%s)"
  # A loop rather than `kubectl wait`: a Failed pod (or a deleted one) must be reported at once,
  # not after the full timeout.
  phase=""
  while :; do
    phase="$(kubectl -n "$SA_NS" get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    case "$phase" in Succeeded|Failed|"") break ;; esac
    if [ $(( $(date +%s) - started_s )) -gt 1080 ]; then break; fi
    sleep 5
  done
  if [ "$phase" = "Succeeded" ]; then
    echo "L pod=succeeded after_s=$(( $(date +%s) - started_s ))"
  else
    echo "L pod=failed phase=${phase:-Missing} after_s=$(( $(date +%s) - started_s ))"
  fi
  logs="$(kubectl -n "$SA_NS" logs "$POD" 2>&1 || true)"
  printf '%s\n' "$logs" | grep -E '^(W[1-5]b?: )' > "$tmp/w.txt" || true
  cat "$tmp/w.txt"
  if ! grep -q '^W5: static-token-write-after-rotation=' "$tmp/w.txt"; then
    echo "--- the pod did not reach W5; last lines:" >&2
    printf '%s\n' "$logs" | grep -v -E '^[0-9]{2}/[0-9]{2}/[0-9]{2} ' | tail -n 15 >&2 || true
  fi
else
  echo "the workload pod could not be created" >&2
  exit 1
fi

exit 0
