"""`stardelt doctor`: can this client work? One line per check; each failure names its fix (#1808)."""

import time

import requests

from stardelt import __version__, api, login


def run(host, environ, session, out, clock=time.time) -> int:
    """Check the client, the host, Nova, the sign-in and who you are; one line each, `✗` counts.

    Returns 0 when no check failed and 1 otherwise. Never writes a token.
    """
    failures = 0

    def write(line):
        out.write(line + "\n")

    # 1. the client
    write(f"✓ client: stardelt {__version__}")

    # 2. the host
    if not host:
        write("✗ host: none — pass --host or set STARDELT_NOVA_URL")
        write("stardelt doctor: 1 check(s) failed")
        return 1
    write(f"✓ host: {host}")

    # 3. Nova
    try:
        response = session.request("GET", host.rstrip("/") + "/api/health", timeout=10)
    except requests.RequestException as err:
        write(f"✗ nova: {host} does not answer ({type(err).__name__}) — check the address and your network")
        failures += 1
    else:
        if response.status_code != 200:
            write(f"✗ nova: /api/health answered HTTP {response.status_code} — is this Nova's address?")
            failures += 1
        else:
            version = (getattr(response, "headers", None) or {}).get("x-stardelt-version", "")
            if not version:
                write("✓ nova: answers (its release is not reported)")
            elif version.lstrip("v") == __version__.lstrip("v"):
                write(f"✓ nova: answers, release {version}")
            else:
                write(
                    f"! nova: answers, release {version}; this client is {__version__} — "
                    "install the client of Nova's release (its release notes name the line)"
                )

    # 4. the sign-in
    token = environ.get("STARDELT_TOKEN", "").strip()
    if token:
        write("✓ sign-in: STARDELT_TOKEN is set (not shown)")
    else:
        creds = login.load(environ)
        if creds is None:
            write("✗ sign-in: none — run `stardelt login`")
            failures += 1
        else:
            left = int(creds.get("expires_at", 0) - clock())
            if creds.get("federated"):
                kind = "federated (CI, direct)" if creds.get("federated", {}).get("direct") else "federated (CI)"
            elif creds.get("refresh_token"):
                kind = "renewable"
            else:
                kind = "not renewable"
            issuer = creds.get("issuer", "?")
            if left > 0:
                write(f"✓ sign-in: {issuer}, access token valid for {left // 60} min, {kind}")
            elif kind != "not renewable":
                write(f"✓ sign-in: {issuer}, access token expired, {kind} — renewed on the next call")
            else:
                write("✗ sign-in: expired and not renewable — run `stardelt login`")
                failures += 1
            if creds.get("host") and creds["host"].rstrip("/") != host.rstrip("/"):
                write(f"! sign-in: made for {creds['host']}, not {host}")

    # 5. who you are
    client = None
    if failures:
        write("- identity: not checked (fix the lines above first)")
    else:
        chosen = token if token else (lambda: login.fresh_token(environ, session, clock))
        client = api.Client(base_url=host, token=chosen, session=session)
        try:
            me = client.me()
        except login.LoginError as err:
            write(f"✗ identity: {err}")
            failures += 1
        except api.StardeltApiError as err:
            if err.status == 401:
                write("✗ identity: Nova refused the sign-in (401) — run `stardelt login` again")
            else:
                write(f"✗ identity: {err}")
            failures += 1
        except requests.RequestException as err:
            write(f"✗ identity: {type(err).__name__} while asking Nova who you are")
            failures += 1
        else:
            write(
                f"✓ identity: {me.get('user', '?')} ({me.get('auth_mode', '?')}), "
                f"warehouse {me.get('warehouse', '?')}"
            )

    # 6. the assistant: the endpoint Ask would use, or why there is none
    if client is None or failures:
        write("- assistant: not checked (fix the lines above first)")
    else:
        try:
            endpoints = client.model_endpoints()
        except (api.StardeltApiError, requests.RequestException) as err:
            write(f"! assistant: not read ({err})")
        else:
            ready = [
                endpoint
                for endpoint in endpoints
                if "ask" in (endpoint.get("purposes") or [])
                and endpoint.get("reachable") is True
                and endpoint.get("jsonOutput") is True
            ]
            ready.sort(key=lambda endpoint: endpoint.get("name") or "")
            if not ready:
                write(
                    "! assistant: off — no ModelEndpoint answers Ask; "
                    "an administrator connects one (stardelt.io/docs/guides/models)"
                )
            else:
                endpoint = ready[0]
                where = (
                    "cloud · prompts leave your cluster"
                    if endpoint.get("location") == "cloud"
                    else "on-premise · prompts stay here"
                )
                write(f"✓ assistant: {endpoint.get('name')} ({endpoint.get('model')}, {where})")

    if failures:
        write(f"stardelt doctor: {failures} check(s) failed")
        return 1
    write("stardelt doctor: all checks passed")
    return 0
