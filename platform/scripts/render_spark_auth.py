#!/usr/bin/env python3
"""Render the Spark Connect front (`manifests/spark-connect-auth.yaml`) for this install.

The front is Envoy's `jwt_authn` + RBAC pair: it verifies every token against the platform's
identity contract and admits only the members and admin groups. The contract is ConfigMap
`stardelt-identity`, so the manifest carries placeholders instead of the platform realm's
values — an install with `identity.mode: external` must admit its own provider's tokens: its
issuer, its `jwksUri`, its audience and its group claim (`stardelt#507`).

`make install-spark` reads the ConfigMap once, writes it to `.spark-identity.json` (public
endpoints and claim names, no secret) and pipes this script's stdout into `kubectl apply`.
Nothing is read from stdin, so the redirect that writes the file cannot be mistaken for the
manifest.

The derived values, from the ConfigMap's data:

    issuer          data.issuerUrl
    audience        data.apiAudience when present, else `spark-connect`
    group claim     data.groupClaim, else `groups`
    admin group     data.adminGroup, else `stardelt-admins`
    jwksUri         data.jwksUri, else the in-cluster Keycloak of `--namespace`
    jwks cluster    `jwks` (TLS, the provider's host) when jwksUri is https, else `keycloak`
    jwks TLS host   the host of data.issuerUrl

`--check` prints the derived values, one `key: value` line each, and applies nothing.
"""

import argparse
import json
import sys

PLATFORM_KEYCLOAK = (
    "http://keycloak.{namespace}.svc.cluster.local:8080"
    "/realms/stardelt/protocol/openid-connect/certs"
)


def identity_data(path):
    """The `data` of a ConfigMap as `kubectl -n NS get configmap ... -o json` prints it."""
    with open(path, encoding="utf-8") as handle:
        document = json.load(handle)
    data = document.get("data")
    if not isinstance(data, dict):
        raise SystemExit(f"render_spark_auth: {path} carries no ConfigMap data")
    return data


def host_of(url):
    """The host of a URL, without its scheme and path."""
    rest = url.split("://", 1)[1] if "://" in url else url
    return rest.split("/", 1)[0]


def derived(data, namespace, base_domain):
    """The values the manifest's placeholders stand for."""
    issuer = data.get("issuerUrl") or ""
    jwks = data.get("jwksUri") or PLATFORM_KEYCLOAK.format(namespace=namespace)
    return {
        "issuer": issuer,
        "audience": data.get("apiAudience") or "spark-connect",
        "group_claim": data.get("groupClaim") or "groups",
        "admin_group": data.get("adminGroup") or "stardelt-admins",
        "jwks_uri": jwks,
        "jwks_cluster": "jwks" if jwks.startswith("https://") else "keycloak",
        "jwks_tls_host": host_of(issuer),
        "namespace": namespace,
        "base_domain": base_domain,
    }


def render(manifest, values):
    """The manifest text with every placeholder replaced."""
    text = manifest
    for placeholder, value in (
        ("__ISSUER_URL__", values["issuer"]),
        ("__AUDIENCE__", values["audience"]),
        ("__JWKS_URI__", values["jwks_uri"]),
        ("__JWKS_CLUSTER__", values["jwks_cluster"]),
        ("__JWKS_TLS_HOST__", values["jwks_tls_host"]),
        ("__GROUP_CLAIM__", values["group_claim"]),
        ("__ADMIN_GROUP__", values["admin_group"]),
        ("__NAMESPACE__", values["namespace"]),
        ("__BASE_DOMAIN__", values["base_domain"]),
    ):
        text = text.replace(placeholder, value)
    return text


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--identity", required=True,
                        help="file holding `kubectl -n NS get configmap stardelt-identity -o json`")
    parser.add_argument("--manifest", required=True,
                        help="the front's manifest, e.g. manifests/spark-connect-auth.yaml")
    parser.add_argument("--namespace", required=True,
                        help="the platform's namespace (the in-cluster Keycloak address)")
    parser.add_argument("--base-domain", required=True,
                        help="the platform's base domain (the public Spark host)")
    parser.add_argument("--check", action="store_true",
                        help="print the derived values and exit; render nothing")
    args = parser.parse_args(argv)

    values = derived(identity_data(args.identity), args.namespace, args.base_domain)
    if args.check:
        for key in ("issuer", "audience", "group_claim", "admin_group", "jwks_uri",
                    "jwks_cluster", "jwks_tls_host"):
            print(f"{key}: {values[key]}")
        return 0
    with open(args.manifest, encoding="utf-8") as handle:
        sys.stdout.write(render(handle.read(), values))
    return 0


if __name__ == "__main__":
    sys.exit(main())
