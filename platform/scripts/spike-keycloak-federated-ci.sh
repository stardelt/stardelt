#!/usr/bin/env bash
# Spike for stardelt/stardelt#830 (#57, after #819 measured the GitHub token's claims): does the
# pinned Keycloak 26.8.2 accept a GitHub Actions OIDC token as an assertion and issue a token Nova
# can read — by which mechanism, with which claims, and which refusals?
#
# The lab, all reverted in `cleanup` on EXIT/INT/TERM:
#   - realm `spike-ci` in the running Keycloak (`deploy/keycloak` in `stardelt`), deleted whole;
#   - one Identity Provider `github` for `https://token.actions.githubusercontent.com`
#     (issuer + JWKS, signatures on, client assertions and JWT authorization grants on,
#     `allowClientIdAsAudience` so the token's `aud` `stardelt-deploy` is the accepted audience);
#   - three clients, one per mechanism the ticket names:
#       `spike-ci-federated` — `clientAuthenticatorType=federated-jwt` (mechanism b): the client
#         assertion *is* the GitHub token; Keycloak finds the client by `jwt.credential.sub`.
#       `spike-ci-jwt` — `client-secret` + JWT authorization grant (mechanism a): the GitHub token
#         is the `assertion` of `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer`, and the
#         `sub` must be a Keycloak user linked to the provider.
#       `spike-ci-selfsigned` — `client-jwt` with a self-signed certificate (mechanism c): the
#         authenticator's only key source is `jwt.credential.certificate`, there is no JWKS URL.
#   - one user `spike-ci-deployer` linked to `github`/<the scratch branch's `sub`>: mechanism (a)
#     refuses an assertion whose `sub` has no link ("User not found").
#   - one repository secret `SPIKE_CI_CLIENT_SECRET` for the mechanism (a) client's secret: a
#     confidential client cannot request a JWT authorization grant without client authentication.
#     The secret is generated here, written to a mode-0600 file, published through stdin
#     (`gh secret set` reads it) and deleted again on every exit path. It is never printed — only
#     its SHA-256 prefix.
#
# The script only sets the realm up; the GitHub token can only come from a GitHub runner, so the
# throwaway workflow lives on a scratch branch and POSTs to
# https://id.alpha.dev.stardelt.io/realms/spike-ci/protocol/openid-connect/token while the realm
# exists. It prints `R ready` with the stop file and then waits: `touch <stop-file>` (or SIGINT/
# SIGTERM) ends the run and deletes the realm.
#
# It prints no token, key or secret: names, status codes and SHA-256 prefixes only.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
umask 077

NS="${NS:-stardelt}"
REALM="${REALM:-spike-ci}"
IDP="${IDP:-github}"
CLIENT_FEDERATED=spike-ci-federated
CLIENT_JWT=spike-ci-jwt
CLIENT_SELF=spike-ci-selfsigned
USER_NAME=spike-ci-deployer
ISSUER=https://token.actions.githubusercontent.com
JWKS_URL="$ISSUER/.well-known/jwks"
AUDIENCE="${AUDIENCE:-stardelt-deploy}"
BRANCH="${SPIKE_BRANCH:-spike/830-ci-token}"
# The immutable-id form #819 measured; `sub` also carries the ref, so it pins exactly this branch.
SUB="${SPIKE_SUB:-repo:stardelt@285991931/stardelt@1453997446:ref:refs/heads/$BRANCH}"
INGRESS=spike-ci-realm
GH_REPO="${SPIKE_REPO:-stardelt/stardelt}"
SECRET_NAME="${SPIKE_SECRET_NAME:-SPIKE_CI_CLIENT_SECRET}"
POD_DIR=/tmp/spike-830
STOP="${SPIKE_STOP:-${TMPDIR:-$HOME/.cache/opencode-scratch}/spike-ci.stop}"

tmp="$(mktemp -d "${TMPDIR:-$HOME/.cache/opencode-scratch}/keycloak-ci.XXXXXX")"
realm_created=no
secret_set=no
ingress_created=no
KCADM=(kubectl -n "$NS" exec deploy/keycloak -- /opt/keycloak/bin/kcadm.sh)

# shellcheck disable=SC2329  # called by the trap below
cleanup() {
  if [ "$ingress_created" = "yes" ]; then
    if kubectl -n "$NS" delete ingress "$INGRESS" --ignore-not-found >/dev/null 2>&1; then
      echo "L cleanup ingress-deleted=yes"
    else
      echo "L cleanup ingress-deleted=no"
    fi
  fi
  if [ "$realm_created" = "yes" ]; then
    if "${KCADM[@]}" delete "realms/$REALM" -r master >/dev/null 2>&1; then
      echo "L cleanup realm-deleted=yes"
    else
      echo "L cleanup realm-deleted=no"
    fi
  fi
  if [ "$secret_set" = "yes" ]; then
    if gh secret delete "$SECRET_NAME" --repo "$GH_REPO" >/dev/null 2>&1; then
      echo "L cleanup repository-secret-deleted=yes"
    else
      echo "L cleanup repository-secret-deleted=no"
    fi
  fi
  kubectl -n "$NS" exec deploy/keycloak -- rm -rf "$POD_DIR" >/dev/null 2>&1 || true
  rm -rf "$tmp"
  rm -f "$STOP"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for tool in kubectl gh python3 openssl jq; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool" >&2; exit 1; }
done
kubectl -n "$NS" get deploy keycloak >/dev/null 2>&1 \
  || { echo "deploy/keycloak not found in namespace $NS" >&2; exit 1; }

# --- admin session inside the pod ----------------------------------------------------------
# The bootstrap admin credential is already an environment variable of the pod, so it never
# crosses a command line here and never reaches this script's output.
# shellcheck disable=SC2016  # expands inside the pod, from the pod's own environment
kubectl -n "$NS" exec deploy/keycloak -- sh -c \
  '/opt/keycloak/bin/kcadm.sh config credentials --server http://127.0.0.1:8080 --realm master \
     --user "$KC_BOOTSTRAP_ADMIN_USERNAME" --password "$KC_BOOTSTRAP_ADMIN_PASSWORD"' >/dev/null 2>&1 \
  || { echo "kcadm login inside deploy/keycloak failed" >&2; exit 1; }

pod_json() { # $1 local file, $2 name inside the pod
  kubectl -n "$NS" exec -i deploy/keycloak -- sh -c "mkdir -p $POD_DIR && cat > $POD_DIR/$2" < "$1"
}

# --- realm ------------------------------------------------------------------------------------
jq -n --arg r "$REALM" '{realm: $r, enabled: true, sslRequired: "external", loginTheme: "keycloak"}' > "$tmp/realm.json"
pod_json "$tmp/realm.json" realm.json
"${KCADM[@]}" create realms -f "$POD_DIR/realm.json" >/dev/null
realm_created=yes
echo "L realm=$REALM created=yes"

# --- publish the realm's token endpoint --------------------------------------------------------
# Measured 2026-11-21: the Ingress `keycloak` publishes only /realms/stardelt (/resources,
# /robots.txt, /admin/stardelt), so a second realm answers nginx's 404 from the internet and a
# GitHub runner could not reach it. One additive scratch Ingress for this realm, deleted in
# `cleanup`; the product's realm needs no such object — it is already published.
KC_HOST="${KC_HOST:-$(kubectl -n "$NS" get configmap keycloak-env -o jsonpath='{.data.KC_HOSTNAME}' 2>/dev/null | sed -E 's|https?://||; s|/.*||')}"
[ -n "$KC_HOST" ] || { echo "ConfigMap keycloak-env has no KC_HOSTNAME — run 'make install-identity'" >&2; exit 1; }
cat <<EOF | kubectl -n "$NS" apply -f - >/dev/null
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: $INGRESS
  annotations:
    nginx.ingress.kubernetes.io/use-regex: "true"
spec:
  ingressClassName: nginx
  rules:
    - host: $KC_HOST
      http:
        paths:
          - path: /realms/$REALM(/|\$)
            pathType: ImplementationSpecific
            backend:
              service:
                name: keycloak
                port:
                  number: 8080
EOF
ingress_created=yes
reachable=000
# The ingress controller reloads in a couple of seconds; the first request after `apply` still 404s.
for _ in $(seq 1 20); do
  reachable="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "https://$KC_HOST/realms/$REALM/.well-known/openid-configuration")"
  [ "$reachable" = "200" ] && break
  sleep 2
done
echo "L ingress=$INGRESS host=$KC_HOST path=/realms/$REALM discovery=$reachable"
[ "$reachable" = "200" ] || { echo "the realm is not reachable through $KC_HOST (HTTP $reachable)" >&2; exit 1; }

# --- identity provider ------------------------------------------------------------------------
# `clientId` doubles as the accepted audience (`allowClientIdAsAudience`): the token's `aud` is the
# string the workflow asked for, not the realm issuer. The grant flags are the defaults the docs
# name (one-time assertion, 300 s maximum, no clock skew).
jq -n --arg idp "$IDP" --arg issuer "$ISSUER" --arg jwks "$JWKS_URL" --arg client "$AUDIENCE" \
  '{alias: $idp, providerId: "oidc", enabled: true, displayName: "GitHub Actions", hideOnLogin: true,
    config: {
      issuer: $issuer, jwksUrl: $jwks, useJwksUrl: "true", validateSignature: "true",
      clientId: $client, clientSecret: "spike-unused",
      supportsClientAssertions: "true", supportsClientAssertionReuse: "false",
      allowClientIdAsAudience: "true",
      jwtAuthorizationGrantEnabled: "true",
      jwtAuthorizationGrantAssertionReuseAllowed: "false",
      jwtAuthorizationGrantMaxAllowedAssertionExpiration: "300",
      jwtAuthorizationGrantAllowedClockSkew: "0",
      syncMode: "IMPORT"
    }}' > "$tmp/idp.json"
pod_json "$tmp/idp.json" idp.json
"${KCADM[@]}" create identity-provider/instances -r "$REALM" -f "$POD_DIR/idp.json" >/dev/null
echo "L idp=$IDP issuer=$ISSUER jwks=on client-assertions=on jwt-authorization-grant=on"

# --- client (b): federated client authentication ---------------------------------------------
jq -n --arg c "$CLIENT_FEDERATED" --arg idp "$IDP" --arg sub "$SUB" \
  '{clientId: $c, enabled: true, publicClient: false, clientAuthenticatorType: "federated-jwt",
    serviceAccountsEnabled: true, standardFlowEnabled: false, implicitFlowEnabled: false,
    directAccessGrantsEnabled: false,
    attributes: {"jwt.credential.issuer": $idp, "jwt.credential.sub": $sub}}' > "$tmp/fed.json"
pod_json "$tmp/fed.json" fed.json
"${KCADM[@]}" create clients -r "$REALM" -f "$POD_DIR/fed.json" >/dev/null
echo "L client=$CLIENT_FEDERATED auth=federated-jwt idp=$IDP sub=$SUB"

# --- client (a): JWT authorization grant ------------------------------------------------------
python3 -c 'import secrets,sys;sys.stdout.write(secrets.token_urlsafe(32))' > "$tmp/client-secret"
jq -n --arg c "$CLIENT_JWT" --arg idp "$IDP" --arg aud "$AUDIENCE" --rawfile secret "$tmp/client-secret" \
  '{clientId: $c, enabled: true, publicClient: false, clientAuthenticatorType: "client-secret",
    secret: $secret, serviceAccountsEnabled: false, standardFlowEnabled: false,
    implicitFlowEnabled: false, directAccessGrantsEnabled: false,
    attributes: {
      "oauth2.jwt.authorization.grant.enabled": "true",
      "oauth2.jwt.authorization.grant.idp": $idp,
      "oauth2.jwt.authorization.grant.audience": ([{key: $idp, value: $aud}] | tojson)
    }}' > "$tmp/jwt.json"
pod_json "$tmp/jwt.json" jwt.json
"${KCADM[@]}" create clients -r "$REALM" -f "$POD_DIR/jwt.json" >/dev/null
echo "L client=$CLIENT_JWT auth=client-secret grant=jwt-bearer idp=$IDP audience=$AUDIENCE"

# --- client (c): client-jwt with a self-signed certificate ------------------------------------
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$tmp/self.key" -out "$tmp/self.crt" -days 1 \
  -subj "/CN=$CLIENT_SELF" >/dev/null 2>&1
jq -n --arg c "$CLIENT_SELF" --rawfile cert "$tmp/self.crt" \
  '{clientId: $c, enabled: true, publicClient: false, clientAuthenticatorType: "client-jwt",
    serviceAccountsEnabled: false, standardFlowEnabled: false, implicitFlowEnabled: false,
    directAccessGrantsEnabled: false,
    attributes: {"jwt.credential.certificate": $cert}}' > "$tmp/self.json"
pod_json "$tmp/self.json" self.json
"${KCADM[@]}" create clients -r "$REALM" -f "$POD_DIR/self.json" >/dev/null
echo "L client=$CLIENT_SELF auth=client-jwt key=jwt.credential.certificate (no JWKS URL)"

# --- the linked user mechanism (a) needs ------------------------------------------------------
"${KCADM[@]}" create users -r "$REALM" -s "username=$USER_NAME" -s enabled=true >/dev/null
user_id="$("${KCADM[@]}" get users -r "$REALM" -q "username=$USER_NAME" --fields id --format csv --noquotes | tail -1)"
jq -n --arg sub "$SUB" --arg user "$USER_NAME" '{userId: $sub, userName: $user}' > "$tmp/link.json"
pod_json "$tmp/link.json" link.json
"${KCADM[@]}" create "users/$user_id/federated-identity/$IDP" -r "$REALM" -f "$POD_DIR/link.json" >/dev/null
echo "L user=$USER_NAME link=$IDP/<sub>"

# --- the mechanism (a) client secret, published for the workflow -------------------------------
gh secret set "$SECRET_NAME" --repo "$GH_REPO" < "$tmp/client-secret"
secret_set=yes
echo "L secret=$SECRET_NAME repo=$GH_REPO sha256[0:16]=$(sha256sum "$tmp/client-secret" | cut -c1-16)"

rm -f "$STOP"
echo "R ready realm=$REALM stop-file=$STOP"
waited=0
while [ ! -e "$STOP" ]; do
  sleep 15
  waited=$((waited + 15))
  if [ $((waited % 60)) -eq 0 ]; then echo "R waiting realm=$REALM seconds=$waited"; fi
done
echo "R stop requested after ${waited}s"
