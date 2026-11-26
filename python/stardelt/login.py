"""``stardelt login``: sign in once with the device authorization grant (RFC 8628).

The request carries a PKCE challenge (S256) and the polls its verifier, because the platform's public client requires
PKCE even for this grant (found against the live realm: "Missing parameter: code_challenge_method"); a provider that
does not ask for it ignores the extra fields. The person gets an address and a short code, approves it in a browser, and the tokens are kept in
``$XDG_CONFIG_HOME/stardelt/credentials.json`` (``~/.config/...`` without it; the directory is mode 0700, the
file 0600). ``STARDELT_TOKEN`` still wins when it is set, for CI. The access token is renewed with the refresh
token when less than a minute is left. The device sign-in asks for ``offline_access``, so the sign-in
outlives the provider's browser session; ``stardelt logout`` revokes the refresh token when the
provider publishes a revocation endpoint. A token is never printed.
"""

import base64
import hashlib
import json
import os
import secrets
import time
from pathlib import Path

DEFAULT_CLIENT_ID = "stardelt-dev"
GRANT_DEVICE = "urn:ietf:params:oauth:grant-type:device_code"
GRANT_CLIENT = "client_credentials"
CLIENT_ASSERTION = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
ASSERTION_ENV = "STARDELT_CI_TOKEN"
SKEW = 60
TIMEOUT = 30
REVOKE_TIMEOUT = 10  # a logout must not wait a minute on a provider that does not answer


class LoginError(RuntimeError):
    """The sign-in did not work; the message says what to do next."""


class _OfflineRefused(Exception):
    """The provider issued no offline token for this person (`not_allowed` while polling)."""


def credentials_path(environ):
    """The credentials file, or ``None`` when the environment names neither XDG_CONFIG_HOME nor HOME."""
    base = environ.get("XDG_CONFIG_HOME") or (environ.get("HOME") and os.path.join(environ["HOME"], ".config"))
    return Path(base) / "stardelt" / "credentials.json" if base else None


def load(environ):
    """The stored sign-in as a dict, or ``None`` when there is none (or the file is not usable)."""
    path = credentials_path(environ)
    try:
        creds = json.loads(path.read_text()) if path else None
    except (OSError, ValueError):
        return None
    return creds if isinstance(creds, dict) and creds.get("access_token") else None


def save(environ, creds):
    path = credentials_path(environ)
    if path is None:
        raise LoginError("cannot keep the sign-in: neither XDG_CONFIG_HOME nor HOME is set")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(creds, handle, indent=2)
    os.replace(temp, path)
    return path


def revoke(session, creds):
    """Best effort: ask the provider to invalidate the stored refresh token (RFC 7009).

    A provider without a ``revocation_endpoint`` is left alone, and every failure is ignored: the
    sign-in is gone locally in any case. ``except Exception`` is deliberate — a broken discovery
    document, a session that raises, a non-JSON answer must never keep the file behind.
    """
    refresh = (creds or {}).get("refresh_token")
    issuer = (creds or {}).get("issuer")
    if not refresh or not issuer or session is None:
        return False
    try:
        endpoint = discover(session, issuer).get("revocation_endpoint")
        if not endpoint:
            return False
        session.request(
            "POST",
            endpoint,
            data={"token": refresh, "token_type_hint": "refresh_token", "client_id": creds.get("client_id")},
            timeout=REVOKE_TIMEOUT,
        )
    except Exception:
        return False
    return True


def forget(environ, session=None):
    """Delete the stored sign-in, revoking its refresh token first when a session is given.

    ``True`` when there was one.
    """
    path = credentials_path(environ)
    if path is None:
        return False
    try:
        revoke(session, load(environ))
        path.unlink()
    except OSError:
        return False
    return True


def _post(session, url, form):
    response = session.request("POST", url, data=form, timeout=TIMEOUT)
    try:
        body = response.json()
    except ValueError:
        body = {}
    return response.status_code, body if isinstance(body, dict) else {}


def discover(session, issuer):
    url = issuer.rstrip("/") + "/.well-known/openid-configuration"
    response = session.request("GET", url, timeout=TIMEOUT)
    try:
        meta = response.json() if response.status_code == 200 else {}
    except ValueError:
        meta = {}
    if not meta.get("device_authorization_endpoint") or not meta.get("token_endpoint"):
        raise LoginError(f"{issuer} does not offer the device sign-in (no device_authorization_endpoint)")
    return meta


def token_endpoint(session, issuer):
    """The issuer's `token_endpoint`, read from its discovery document."""
    url = issuer.rstrip("/") + "/.well-known/openid-configuration"
    response = session.request("GET", url, timeout=TIMEOUT)
    try:
        meta = response.json() if response.status_code == 200 else {}
    except ValueError:
        meta = {}
    if not meta.get("token_endpoint"):
        raise LoginError(f"{issuer} publishes no token endpoint")
    return meta["token_endpoint"]


def nova_sign_in(session, host):
    """Ask Nova (``GET /api/auth/config``) which identity provider to sign in with.

    Returns ``(issuer, client_id)``; ``client_id`` is ``None`` when Nova names no device client.
    """
    response = session.request("GET", host.rstrip("/") + "/api/auth/config", timeout=TIMEOUT)
    try:
        config = response.json() if response.status_code == 200 else {}
    except ValueError:
        config = {}
    if not isinstance(config, dict) or config.get("mode") != "oidc" or not config.get("issuer"):
        raise LoginError(f"{host} does not name an identity provider to sign in with (GET /api/auth/config)")
    return config["issuer"], config.get("deviceClientId") or None


def _creds(issuer, client_id, token_endpoint, body, now, previous=None):
    refresh = body.get("refresh_token") or (previous or {}).get("refresh_token")
    return {
        "issuer": issuer,
        "client_id": client_id,
        "token_endpoint": token_endpoint,
        "access_token": body["access_token"],
        "refresh_token": refresh,
        "expires_at": int(now + int(body.get("expires_in", 300))),
    }


def pkce():
    """A PKCE pair (RFC 7636, method S256): the verifier stays here, the challenge goes to the provider."""
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode("ascii")
    return verifier, challenge


def _device_start(session, endpoint, client_id, challenge, scope):
    return _post(
        session,
        endpoint,
        {"client_id": client_id, "scope": scope, "code_challenge": challenge, "code_challenge_method": "S256"},
    )


def _device_flow(session, meta, issuer, client_id, scope, say, sleep, clock):
    verifier, challenge = pkce()
    status, start = _device_start(
        session, meta["device_authorization_endpoint"], client_id, challenge, scope
    )
    if status == 400 and start.get("error") == "invalid_scope" and "offline_access" in scope:
        say("this identity provider gives no offline sign-in; the sign-in lasts as long as its session")
        status, start = _device_start(session, meta["device_authorization_endpoint"], client_id, challenge, "openid")
    if status != 200 or "device_code" not in start:
        raise LoginError(f"the identity provider refused the sign-in request (HTTP {status})")
    where = start.get("verification_uri_complete") or start.get("verification_uri", "")
    say(f"Open {where} in a browser and approve the code {start.get('user_code', '')}")
    interval = int(start.get("interval", 5))
    deadline = clock() + int(start.get("expires_in", 600))
    while clock() < deadline:
        sleep(interval)
        status, body = _post(
            session,
            meta["token_endpoint"],
            {
                "grant_type": GRANT_DEVICE,
                "device_code": start["device_code"],
                "client_id": client_id,
                "code_verifier": verifier,
            },
        )
        if status == 200 and "access_token" in body:
            return _creds(issuer, client_id, meta["token_endpoint"], body, clock())
        error = body.get("error")
        if error == "authorization_pending":
            continue
        if error == "slow_down":
            interval += 5
            continue
        if error == "access_denied":
            raise LoginError("the sign-in was refused in the browser")
        if error == "expired_token":
            break
        if error == "not_allowed" and "offline_access" in scope:
            raise _OfflineRefused()
        raise LoginError(f"the identity provider answered {error or f'HTTP {status}'}")
    raise LoginError("the code expired: run `stardelt login` again")


def device_login(session, issuer, client_id, say, sleep=time.sleep, clock=time.time):
    """Run the device flow and return the credentials dict. ``say`` receives the lines the person must read.

    The sign-in asks for ``offline_access`` (it is not asked again when the provider does not offer
    it): the refresh token then outlives the provider's SSO session, so a person signs in once and
    scripts keep working until the sign-in is revoked. A provider that refuses offline tokens for the
    person is asked once more without them, and the person approves a second code.
    """
    meta = discover(session, issuer)
    try:
        return _device_flow(session, meta, issuer, client_id, "openid offline_access", say, sleep, clock)
    except _OfflineRefused:
        say("this account may not hold an offline sign-in; approve the new code for a sign-in that lasts as long as the browser session")
        return _device_flow(session, meta, issuer, client_id, "openid", say, sleep, clock)


def federated_login(session, issuer, client_id, assertion, clock=time.time):
    """Exchange the CI's own token for a realm token (federated client authentication, #830).

    The CI token is the `client_assertion`; nothing is stored on the provider. The answer is a
    5-minute realm token and carries no refresh token, so a later call needs the CI token again.
    """
    endpoint = token_endpoint(session, issuer)
    status, body = _post(
        session,
        endpoint,
        {
            "grant_type": GRANT_CLIENT,
            "client_id": client_id,
            "scope": "openid",
            "client_assertion_type": CLIENT_ASSERTION,
            "client_assertion": assertion,
        },
    )
    if status != 200 or not body.get("access_token"):
        error = body.get("error")
        raise LoginError(
            f"the identity provider refused the CI token ({error or f'HTTP {status}'})"
        )
    return _creds(issuer, client_id, endpoint, body, clock())


def direct_login(assertion, clock=time.time):
    """Keep the CI token as the bearer itself (no exchange; a DeployStage verifies it, #1845).

    The token lives five minutes; `fresh_token` reads it from the environment again when it is
    about to expire, so no request is made here and nothing is stored on the provider.
    """
    return {
        "issuer": "",
        "client_id": "",
        "token_endpoint": "",
        "access_token": assertion,
        "refresh_token": None,
        "expires_at": int(clock() + 240),
    }


def fresh_token(environ, session, clock=time.time):
    """The stored access token, renewed first when it has less than a minute left."""
    creds = load(environ)
    if creds is None:
        raise LoginError("not signed in: run `stardelt login` or set STARDELT_TOKEN")
    if creds.get("expires_at", 0) - clock() > SKEW:
        return creds["access_token"]
    federated = creds.get("federated") or {}
    if federated:
        name = federated.get("env") or ASSERTION_ENV
        if federated.get("direct"):
            token = environ.get(name, "")
            if not token:
                raise LoginError(f"the CI token is gone: set {name} for this step")
            creds["access_token"] = token
            creds["expires_at"] = int(clock() + 240)
            save(environ, creds)
            return token
        assertion = environ.get(name, "")
        if not assertion:
            raise LoginError(f"the CI token is gone: set {name} for this step")
        renewed = federated_login(session, creds["issuer"], creds["client_id"], assertion, clock)
        renewed["federated"] = federated
        renewed["host"] = creds.get("host")
        save(environ, renewed)
        return renewed["access_token"]
    if not creds.get("refresh_token"):
        raise LoginError("the sign-in has expired: run `stardelt login` again")
    status, body = _post(
        session,
        creds["token_endpoint"],
        {"grant_type": "refresh_token", "refresh_token": creds["refresh_token"], "client_id": creds["client_id"]},
    )
    if status != 200 or "access_token" not in body:
        raise LoginError("the sign-in has ended: run `stardelt login` again")
    renewed = _creds(creds["issuer"], creds["client_id"], creds["token_endpoint"], body, clock(), creds)
    renewed["host"] = creds.get("host")
    save(environ, renewed)
    return renewed["access_token"]
