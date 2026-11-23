#!/usr/bin/env bash
# Four measurements through the shared Connect front (stardelt/stardelt#84 Q4, spike #208,
# front built in stardelt/stardelt#231, authorization added in stardelt/stardelt#404), all with
# tokens Keycloak really signed:
#
#   1. a token with `aud spark-connect`, for the real user `analyst`, runs range(10) through
#      https://spark.<baseDomain> and gets 10;
#   2. a client with no token, same host, is refused with `Jwt is missing`;
#   3. a token without the audience (admin-cli's password grant) is refused too — the audience
#      is enforced, not decorative;
#   4. a token that is signed, carries the audience, and belongs to a person in `visitors` only
#      is refused with PERMISSION_DENIED — the front checks the `groups` claim, so a valid
#      token is not yet a member (stardelt/stardelt#404).
#
#   make verify-spark-auth      (NS=…, BASE_DOMAIN=…, SPARK_USER=…, VISITOR_USER=…, TIMEOUT=… to override)
#
# A curl at the ingress only shows the unsigned refusal: a misconfigured JWKS, a wrong issuer,
# a missing audience and an upstream that downgrades to HTTP/1 all look the same from outside.
# The point here is a streaming gRPC session that a real token opened.
#
# Measured on the lab 2026-11-19, all three facts below are traps the first version of this
# check fell into:
#   * the token must be MINTED. Keycloak's admin API `evaluate-scopes/
#     generate-example-access-token` answers a claims JSON object, not a JWS, and Envoy refuses
#     it. scripts/mint-user-token.py does the OAuth2 device grant on the public client
#     `stardelt-dev` (PKCE S256 is mandatory on Keycloak 26.8.2).
#   * the host is `spark.$BASE_DOMAIN`. `$BASE_DOMAIN` alone is Nova: its ingress answers 405
#     to `/spark.connect.SparkConnectService/Config`.
#   * `dev-seed-users`' values end with a newline; the minting script strips it, or a correct
#     password answers `invalid_grant`.
#
# The realm user is `SPARK_USER`, not `USER`: the environment's `USER` is the login name of
# whoever runs the check, so it can never default to a realm account.
#
# PASS = a signed query ran 10 rows, both negative lines were refused, and the visitors line
# was refused or skipped (no visitors-only user in the Secret). FAIL = one of the judgements
# (or a token without the audience, or a visitor's token) did not hold. UNVERIFIED (exit 3) = the
# check could not run at all (a tool or the Secret is missing, the client pod never started);
# a check that could not run never prints PASS or FAIL.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
BASE_DOMAIN="${BASE_DOMAIN:-alpha.dev.stardelt.io}"
SPARK_USER="${SPARK_USER:-analyst}"
# The visitor leg: the public demo's person is `demo`, in `visitors` only. `dev-seed-users` has
# no key for it (it is created by `scripts/public-demo.sh`, not `make seed-dev-access`), so its
# published password `demo` is the fallback; `VISITOR_PASSWORD` overrides every source.
VISITOR_USER="${VISITOR_USER:-demo}"
TIMEOUT="${TIMEOUT:-300}"
POD=verify-spark-auth

for tool in kubectl curl python3 base64; do
  command -v "$tool" >/dev/null 2>&1 || { echo "verify-spark-auth: UNVERIFIED ($tool not found)"; exit 3; }
done

tmp="$(mktemp -d)"
chmod 700 "$tmp"
# No pod, Secret, ConfigMap or password is left behind: not on PASS, not on FAIL, not on Ctrl-C.
cleanup() {
  kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NS" delete configmap "$POD-script" --ignore-not-found >/dev/null 2>&1 || true
  kubectl -n "$NS" delete secret "$POD-token" --ignore-not-found >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

fail() {  # <logs> <reason>: the tail of the pod log, then the judged FAIL line.
  printf '%s\n' "$1" | tail -n 40
  echo "verify-spark-auth: FAIL ($2)"
  exit 1
}

# The realm user's password, from the Secret `make seed-dev-access` creates. It never reaches
# argv or the terminal: kubectl writes it straight into the 0700 temp dir.
if ! kubectl -n "$NS" get secret dev-seed-users -o jsonpath="{.data.$SPARK_USER}" | base64 -d > "$tmp/pw"; then
  echo "verify-spark-auth: UNVERIFIED (Secret dev-seed-users has no key $SPARK_USER — run 'make seed-dev-access')"
  exit 3
fi
chmod 600 "$tmp/pw"
[ -s "$tmp/pw" ] || {
  echo "verify-spark-auth: UNVERIFIED (Secret dev-seed-users has no key $SPARK_USER — run 'make seed-dev-access')"
  exit 3
}

ISSUER="https://id.$BASE_DOMAIN/realms/stardelt"

# 1. A real signed token for the user, from the device grant on `stardelt-dev`.
if ! out="$(ISSUER="$ISSUER" USER="$SPARK_USER" PASSWORD_FILE="$tmp/pw" \
      TOKEN_FILE="$tmp/signed" python3 scripts/mint-user-token.py 2>&1)"; then
  printf '%s\n' "$out" | tail -n 5
  echo "verify-spark-auth: FAIL (could not mint a token for $SPARK_USER through the device grant)"
  exit 1
fi
printf '%s\n' "$out"
case "$out" in
  *aud=*spark-connect*) ;;
  *) echo "verify-spark-auth: FAIL (the token for $SPARK_USER carries no spark-connect audience — run 'make sync-spark-audience' (stardelt#231) and try again)"
     exit 1 ;;
esac

# 2. The control token: same user, but `admin-cli`'s password grant carries no audience for
#    this API. It must not, or the third measurement proves nothing.
if ! out="$(ADMIN_PASSWORD_GRANT=1 ISSUER="$ISSUER" USER="$SPARK_USER" PASSWORD_FILE="$tmp/pw" \
      TOKEN_FILE="$tmp/noaud" python3 scripts/mint-user-token.py 2>&1)"; then
  printf '%s\n' "$out" | tail -n 5
  echo "verify-spark-auth: FAIL (could not mint the no-audience control token through admin-cli)"
  exit 1
fi
printf '%s\n' "$out"
if ! out="$(python3 scripts/mint-user-token.py --decode "$tmp/noaud" 2>&1)"; then
  printf '%s\n' "$out" | tail -n 5
  echo "verify-spark-auth: FAIL (the control token is not a readable JWS)"
  exit 1
fi
printf '%s\n' "$out"
case "$out" in
  *spark-connect*) echo "verify-spark-auth: FAIL (the admin-cli control token carries spark-connect: the audience is not enforced)"
                   exit 1 ;;
esac

# 3. The visitor leg (stardelt/stardelt#404): a real token for a person who is in `visitors`
#    only, refused with PERMISSION_DENIED. The password comes from `dev-seed-users` when the
#    demo seed created that user; the public demo's `demo` has a fixed, published password
#    (`demo`, the "Try the demo" sign-in) and no key there, so the leg falls back to it.
#    `VISITOR_PASSWORD` overrides both.
visitor=skipped
kubectl -n "$NS" get secret dev-seed-users -o jsonpath="{.data.$VISITOR_USER}" 2>&1 \
  | base64 -d > "$tmp/visitor-pw" || true
if [ ! -s "$tmp/visitor-pw" ] && [ "$VISITOR_USER" = demo ]; then
  printf '%s' "${VISITOR_PASSWORD:-demo}" > "$tmp/visitor-pw"
fi
if [ -s "$tmp/visitor-pw" ]; then
  chmod 600 "$tmp/visitor-pw"
  if ! out="$(ISSUER="$ISSUER" USER="$VISITOR_USER" PASSWORD_FILE="$tmp/visitor-pw" \
        TOKEN_FILE="$tmp/visitor" python3 scripts/mint-user-token.py 2>&1)"; then
    printf '%s\n' "$out" | tail -n 5
    echo "verify-spark-auth: FAIL (could not mint a token for $VISITOR_USER through the device grant)"
    exit 1
  fi
  printf '%s\n' "$out"
  case "$out" in
    *aud=*spark-connect*) visitor=token ;;
    *) echo "verify-spark-auth: FAIL ($VISITOR_USER's token carries no spark-connect audience — run 'make sync-spark-audience' (stardelt#231) and try again)"
       exit 1 ;;
  esac
fi

secret_args=(--from-file=signed="$tmp/signed" --from-file=noaud="$tmp/noaud")
[ "$visitor" = token ] && secret_args+=(--from-file=visitor="$tmp/visitor")
kubectl -n "$NS" create secret generic "$POD-token" "${secret_args[@]}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# The client is the spawn image the hub runs; `scripts/check-notebook-image-pin.sh` keeps the
# two in step. The `dev@sha256:` tag is the only one in that file.
client_digest="$(grep -m1 -E '^[[:space:]]*tag:[[:space:]]*"dev@sha256:[0-9a-f]{64}"' helm-values/jupyterhub.yaml | sed -E 's/.*"dev@(sha256:[0-9a-f]{64})".*/\1/')"
if [ -z "$client_digest" ]; then
  echo "verify-spark-auth: UNVERIFIED (helm-values/jupyterhub.yaml names no dev@sha256 notebook image)"
  exit 3
fi
CLIENT_IMAGE="ghcr.io/stardelt/jupyter-singleuser:dev@${client_digest}"

# The client. Both the URL builder and the four lines are the measurement: a token in the URL
# forces TLS, so all four legs take the same ingress → Envoy path and differ only in the header.
# (`spark.$BASE_DOMAIN`, never `$BASE_DOMAIN`: that is Nova and answers 405.)
cat > "$tmp/client.py" <<'PY'
import os
import signal

from pyspark.sql import SparkSession

BASE = os.environ["BASE_DOMAIN"]
SIGNED = os.environ["SIGNED_TOKEN"]
NOAUD = os.environ["NOAUD_TOKEN"]
# Unset when the pod's optional secret key has no value: the visitor leg was skipped.
VISITOR = os.environ.get("VISITOR_TOKEN") or ""


def first_line(exc):
    lines = [l.strip() for l in str(exc).strip().splitlines() if l.strip()]
    picked = [l for l in lines if "StatusCode." in l or "details =" in l]
    return (", ".join(picked) if picked else (lines[0] if lines else type(exc).__name__))[:240]


def alarm():
    def handler(_s, _f):
        raise TimeoutError("no answer within 240s")

    signal.signal(signal.SIGALRM, handler)
    signal.alarm(240)


def query(url, rows):
    alarm()
    try:
        session = SparkSession.builder.remote(url).getOrCreate()
        try:
            return str(rows(session))
        finally:
            try:
                session.stop()
            except Exception:
                pass
    except Exception as exc:  # the error text is the measurement
        return "refused: " + first_line(exc)
    finally:
        signal.alarm(0)


print("unsigned=" + query("sc://spark.%s:443/;use_ssl=true" % BASE, lambda s: s.range(10).count()), flush=True)
print("signed=" + query("sc://spark.%s:443/;use_ssl=true;token=%s" % (BASE, SIGNED),
                        lambda s: s.range(10).count()), flush=True)
print("noaud=" + query("sc://spark.%s:443/;use_ssl=true;token=%s" % (BASE, NOAUD),
                       lambda s: s.range(10).count()), flush=True)
if VISITOR:
    print("visitor=" + query("sc://spark.%s:443/;use_ssl=true;token=%s" % (BASE, VISITOR),
                             lambda s: s.range(10).count()), flush=True)
else:
    print("visitor=skipped (no visitors-only user)", flush=True)
PY

kubectl -n "$NS" create configmap "$POD-script" --from-file=client.py="$tmp/client.py" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# The pod carries `app.kubernetes.io/name: verify-spark-auth` and nothing else: no
# NetworkPolicy selects it, so it has the egress and DNS the public host needs (measured; a
# `component: singleuser-server` pod is only selected by notebook-public-egress, which has no
# DNS rule and excludes the private ranges).
if ! out="$(kubectl -n "$NS" apply -f - 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD}
  namespace: ${NS}
  labels:
    app.kubernetes.io/name: verify-spark-auth
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
      command: ["python3", "/opt/check/client.py"]
      env:
        - name: HOME
          value: /tmp
        - name: BASE_DOMAIN
          value: ${BASE_DOMAIN}
        - name: SIGNED_TOKEN
          valueFrom:
            secretKeyRef: { name: ${POD}-token, key: signed }
        - name: NOAUD_TOKEN
          valueFrom:
            secretKeyRef: { name: ${POD}-token, key: noaud }
        # Optional: the secret has no "visitor" key when the visitors leg was skipped, and the
        # pod must still start (an env reference to a missing key would hold it in
        # CreateContainerConfigError and turn the whole check into UNVERIFIED).
        - name: VISITOR_TOKEN
          valueFrom:
            secretKeyRef: { name: ${POD}-token, key: visitor, optional: true }
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
      resources:
        requests: { cpu: 100m, memory: 256Mi }
        limits: { cpu: 1, memory: 1Gi }
      volumeMounts:
        - name: script
          mountPath: /opt/check
          readOnly: true
  volumes:
    - name: script
      configMap:
        name: ${POD}-script
EOF
)"; then
  echo "verify-spark-auth: UNVERIFIED (the client pod could not be created: $(printf '%s\n' "$out" | tail -n 1))"
  exit 3
fi

# A pod that never starts says nothing about the front: UNVERIFIED, never PASS or FAIL.
if ! kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$POD" --timeout="${TIMEOUT}s" >/dev/null 2>&1; then
  mapfile -t state < <(kubectl -n "$NS" get pod "$POD" -o json 2>/dev/null | python3 -c '
import json, sys
try:
    status = json.load(sys.stdin).get("status", {})
except Exception:
    status = {}
print(status.get("phase", "Unknown"))
waiting = (status.get("containerStatuses") or [{}])[0].get("state", {}).get("waiting") or {}
scheduled = [c for c in status.get("conditions") or []
             if c.get("type") == "PodScheduled" and c.get("status") != "True"]
reason = waiting.get("message") or waiting.get("reason") or ""
if not reason and scheduled:
    reason = scheduled[0].get("message", "")
print((reason or "").strip())
')
  phase="${state[0]:-Unknown}"
  reason="${state[1]:-}"
  if [ "$phase" = "Pending" ] || [ "$reason" != "" ]; then
    echo "verify-spark-auth: UNVERIFIED (the client pod did not run: ${reason:-$phase})"
    exit 3
  fi
  fail "$(kubectl -n "$NS" logs "$POD" 2>&1 || true)" "the client pod ended $phase, not Succeeded"
fi

logs="$(kubectl -n "$NS" logs "$POD" 2>&1 || true)"
unsigned="$(printf '%s\n' "$logs" | grep -m1 '^unsigned=' || true)"
signed="$(printf '%s\n' "$logs" | grep -m1 '^signed=' || true)"
noaud="$(printf '%s\n' "$logs" | grep -m1 '^noaud=' || true)"
visitor="$(printf '%s\n' "$logs" | grep -m1 '^visitor=' || true)"

[ "$signed" = "signed=10" ] || fail "$logs" "a signed query did not run 10 rows: ${signed:-no answer}"
case "$unsigned" in
  unsigned=refused*) ;;
  *) fail "$logs" "an unsigned call was not refused: ${unsigned:-no answer}" ;;
esac
case "$unsigned" in
  *"Jwt is missing"*) ;;
  *) fail "$logs" "an unsigned call was refused without 'Jwt is missing': $unsigned" ;;
esac
# A wrong-audience JWS gets PERMISSION_DENIED "Audiences in Jwt are not allowed"; only a
# missing token gets UNAUTHENTICATED. Both are refusals, so no status is named here.
case "$noaud" in
  noaud=refused*) ;;
  *) fail "$logs" "a token without the audience was admitted: ${noaud:-no answer}" ;;
esac
# A signed token with the audience, held by a person in `visitors` only: the RBAC filter answers
# 403, which gRPC maps to PERMISSION_DENIED. A visitor admitted is the finding this leg exists for.
case "$visitor" in
  "visitor=skipped (no visitors-only user)") ;;
  visitor=refused*PERMISSION_DENIED*) ;;
  visitor=refused*) fail "$logs" "a visitor's token was refused, but not with PERMISSION_DENIED: ${visitor:-no answer}" ;;
  *) fail "$logs" "a visitor's signed token was admitted: ${visitor:-no answer}" ;;
esac

printf '%s\n' "$unsigned" "$signed" "$noaud" "$visitor"
if [ "$visitor" = "visitor=skipped (no visitors-only user)" ]; then
  echo "verify-spark-auth: PASS (a signed query ran 10 rows; an unsigned call and a token without the audience were refused; visitor leg skipped: no visitors-only user)"
else
  echo "verify-spark-auth: PASS (a signed query ran 10 rows; an unsigned call, a token without the audience and a visitor's token were refused)"
fi
