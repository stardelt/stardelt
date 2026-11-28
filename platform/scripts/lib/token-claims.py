"""The token-claim self-check of the Keycloak convergence scripts.

A service client needs `azp`, its audiences and *no* group; it needs no `preferred_username`
(Trino uses `sub`, Lakekeeper the group claim). A principal needs its group and its username,
and its token must outlive a stream. The distinction is here so a realm that does not emit an
optional claim does not fail a correct client (stardelt/stardelt#2196 J4).
"""
import argparse
import base64
import json
import sys

PRINCIPAL_LIFESPAN = 28800


def claims_from_token(token):
    """The payload of a JWT, unsigned or not; the scripts never verify the signature here."""
    try:
        payload = token.split(".")[1]
    except IndexError:
        raise SystemExit("the token is not a JWT")
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))


def problems_for(claims, client, kind, group, audiences):
    """Every way the token fails its contract, as sentences; empty when it passes."""
    aud = claims.get("aud") if isinstance(claims.get("aud"), list) else [claims.get("aud")]
    groups = claims.get("groups") or []
    problems = []
    if claims.get("azp") != client:
        problems.append("azp is %r" % claims.get("azp"))
    for audience in audiences:
        if audience not in aud:
            problems.append("audience lacks %s (aud=%s)" % (audience, aud))
    if kind == "service":
        if groups:
            problems.append("groups is %r (a service client has no group)" % (groups,))
    else:
        if sorted(groups) != [group]:
            problems.append("groups is %r (expected [%s])" % (groups, group))
        if claims.get("preferred_username") != "service-account-" + client:
            problems.append("preferred_username is %r" % claims.get("preferred_username"))
        lifetime = claims.get("exp", 0) - claims.get("iat", 0)
        if lifetime < PRINCIPAL_LIFESPAN:
            problems.append(
                "token lifespan is %s s, a principal needs %s" % (lifetime, PRINCIPAL_LIFESPAN)
            )
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description="check one Keycloak token's claims")
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--client", required=True)
    parser.add_argument("--kind", choices=("service", "principal"), required=True)
    parser.add_argument("--group", default="")
    parser.add_argument("--audiences", default="")
    args = parser.parse_args(argv)
    with open(args.token_file) as handle:
        claims = claims_from_token(handle.read())
    audiences = [audience for audience in args.audiences.split(",") if audience]
    problems = problems_for(claims, args.client, args.kind, args.group, audiences)
    if problems:
        print("; ".join(problems), file=sys.stderr)
        return 1
    if args.kind == "service":
        print("› token claims: azp=%s aud=%s" % (claims["azp"], audiences))
    else:
        lifetime = claims.get("exp", 0) - claims.get("iat", 0)
        print(
            "› token claims: azp=%s aud=%s groups=%s preferred_username=%s exp-iat=%s"
            % (claims["azp"], audiences, claims.get("groups") or [], claims.get("preferred_username"), lifetime)
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
