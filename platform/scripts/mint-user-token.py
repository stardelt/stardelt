#!/usr/bin/env python3
"""Mint a Keycloak-signed access token for one realm user (stardelt/stardelt#84 Q4).

The shared Connect front (`manifests/spark-connect-auth.yaml`, stardelt/stardelt#231) verifies
a compact JWS with Envoy's `jwt_authn`. Keycloak's admin API cannot produce one: the
`clients/<uuid>/evaluate-scopes/generate-example-access-token` endpoint — the one
`scripts/keycloak-sync-*.sh` reads `aud` from — answers a **claims JSON object**, not a signed
token (measured 2026-11-19 on Keycloak 26.8.2), so Envoy refuses it. The token has to be
minted by the token endpoint:

  (default)               OAuth2 device grant on a public client. `stardelt-dev` has
                          `publicClient: true`, the device grant and PKCE S256 as realm
                          attributes, and `make sync-spark-audience` gives its tokens
                          `aud spark-connect` (stardelt#231). The token is the user's own.
  ADMIN_PASSWORD_GRANT=1  password grant on `admin-cli`, the only realm client that keeps
                          direct grants on. Its token carries no `spark-connect` audience:
                          the control leg of `scripts/verify-spark-auth.sh`.
  --decode FILE           print `segments=… aud=… azp=… preferred_username=…` for FILE's
                          token; the subcommand `scripts/tests/test_mint_user_token.py` calls.

Environment: ISSUER, USER, PASSWORD_FILE (mode 0600; the password never reaches argv),
TOKEN_FILE (mode 0600), CLIENT_ID (default `stardelt-dev`). Nothing here prints the token.

Keycloak's device-approval page is plain HTML forms, so this drives it with urllib only: no
browser and no direct-access grant (which the realm keeps off on purpose).
"""
import base64
import hashlib
import http.cookiejar
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

FORM = re.compile(r"<form[^>]*>.*?</form>", re.S)
INPUT = re.compile(r'name="([^"]+)" value="([^"]*)"')
ACTION = re.compile(r'action="([^"]+)"')

jar = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))


def get(url):
    with opener.open(urllib.request.Request(url)) as r:
        return r.geturl(), r.read().decode("utf-8", "replace")


def post(url, data):
    body = urllib.parse.urlencode(data).encode()
    with opener.open(urllib.request.Request(url, data=body, method="POST")) as r:
        return r.geturl(), r.read().decode("utf-8", "replace")


def form(resource_url, html):
    """(absolute action URL, {input name: value}) of the only form on the page."""
    match = FORM.search(html)
    if not match:
        sys.exit(f"no form on {resource_url}: {html[:200]}")
    origin = "https://" + urllib.parse.urlsplit(resource_url).netloc
    action = ACTION.search(match.group(0)).group(1).replace("&amp;", "&")
    return (origin + action if action.startswith("/") else action), dict(INPUT.findall(match.group(0)))


def token_endpoint(issuer, data):
    """POST the token endpoint, returning the JSON body for both 2xx and 4xx."""
    req = urllib.request.Request(
        f"{issuer}/protocol/openid-connect/token", data=urllib.parse.urlencode(data).encode(), method="POST"
    )
    try:
        with opener.open(req) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode())


def b64url(segment):
    """Decode one base64url segment, padded to a multiple of 4."""
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def describe(token):
    """Print the token's shape — never the token itself."""
    parts = token.split(".")
    if len(parts) != 3:
        sys.exit(f"not a compact JWS: {len(parts)} segment(s), expected 3")
    try:
        claims = json.loads(b64url(parts[1]))
    except Exception as exc:  # a mangled file is a sentence, not a traceback
        sys.exit(f"the payload of segment 2 is not JSON: {exc}")
    aud = claims.get("aud")
    if isinstance(aud, list):
        aud = ",".join(str(a) for a in aud)
    # `admin-cli`'s password-grant token carries neither claim (measured 2026-11-19): `-`, not
    # `None`, so the control leg reads as "no user" rather than as a bug.
    print("segments=%d aud=%s azp=%s preferred_username=%s" % (
        len(parts), aud, claims.get("azp"),
        claims.get("preferred_username") or claims.get("sub") or "-"))


def mint_device(issuer, user, password, client_id):
    """The user's own token through the device grant on a public client, PKCE S256."""
    verifier = base64.urlsafe_b64encode(os.urandom(48)).rstrip(b"=").decode()
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    req = urllib.request.Request(
        f"{issuer}/protocol/openid-connect/auth/device",
        data=urllib.parse.urlencode({"client_id": client_id, "scope": "openid",
                                     "code_challenge": challenge,
                                     "code_challenge_method": "S256"}).encode(),
        method="POST",
    )
    try:
        with opener.open(req) as r:
            device = json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f"device authorization failed: {e.code} {e.read().decode()[:200]}")

    # Approve it as the user: login form, then the consent form.
    url, html = get(device["verification_uri_complete"])
    action, inputs = form(url, html)
    inputs.update({"username": user, "password": password, "credentialId": ""})
    url, html = post(action, inputs)
    action, inputs = form(url, html)
    inputs["accept"] = "Yes"
    post(action, inputs)

    # Exchange the device code for the user's own access token.
    token = None
    for _ in range(20):
        token = token_endpoint(issuer, {"client_id": client_id, "scope": "openid",
                                        "device_code": device["device_code"],
                                        "code_verifier": verifier,
                                        "grant_type": "urn:ietf:params:oauth:grant-type:device_code"})
        if "access_token" in token:
            break
        if token.get("error") not in ("authorization_pending", "slow_down"):
            sys.exit(f"token endpoint refused: {token}")
        time.sleep(5 if token.get("error") == "slow_down" else 2)
    if not token or "access_token" not in token:
        sys.exit("device authorization was never approved")
    return token["access_token"]


def mint_password(issuer, user, password):
    """The control token: `admin-cli` is the realm's only client with direct grants."""
    token = token_endpoint(issuer, {"grant_type": "password", "client_id": "admin-cli",
                                    "username": user, "password": password, "scope": "openid"})
    if "access_token" not in token:
        sys.exit(f"token endpoint refused: {token}")
    return token["access_token"]


def write_token(path, token):
    # Written for the caller (a 0700 temp dir), never printed.
    umask = os.umask(0o077)
    try:
        with open(path, "w") as f:
            f.write(token)
    finally:
        os.umask(umask)


def main(argv):
    if argv[:1] == ["--decode"]:
        if len(argv) != 2:
            sys.exit("usage: mint-user-token.py --decode <token-file>")
        with open(argv[1]) as f:
            describe(f.read().strip())
        return

    issuer = os.environ["ISSUER"].rstrip("/")
    user = os.environ["USER"]
    token_file = os.environ["TOKEN_FILE"]
    # The Secret's values end with a newline (dev-seed-users, 25 bytes for `analyst`);
    # the realm compares the exact string, so a verbatim password answers invalid_grant.
    password = open(os.environ["PASSWORD_FILE"]).read().rstrip()

    if os.environ.get("ADMIN_PASSWORD_GRANT", "") not in ("", "0"):
        token = mint_password(issuer, user, password)
    else:
        token = mint_device(issuer, user, password, os.environ.get("CLIENT_ID") or "stardelt-dev")
    write_token(token_file, token)
    print("minted: ", end="")
    describe(token)


main(sys.argv[1:])
