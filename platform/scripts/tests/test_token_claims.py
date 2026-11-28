"""Unit tests for scripts/lib/token-claims.py (#2196 J4)."""
import base64
import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path

CHECKER = Path(__file__).resolve().parents[1] / "lib" / "token-claims.py"
_spec = importlib.util.spec_from_file_location("token_claims", CHECKER)
token_claims = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(token_claims)


def token(claims):
    def part(payload):
        raw = json.dumps(payload).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
    return "%s.%s.%s" % (part({"alg": "none", "typ": "JWT"}), part(claims), "not-a-signature")


SERVICE = {"azp": "svc-journey-outsider", "aud": ["trino", "lakekeeper"]}
PRINCIPAL = {
    "azp": "sp-journey",
    "aud": ["trino", "lakekeeper"],
    "groups": ["stardelt-users"],
    "preferred_username": "service-account-sp-journey",
    "iat": 1000,
    "exp": 29800,
}


class ProblemsTest(unittest.TestCase):
    def test_a_service_client_needs_no_preferred_username(self):
        self.assertEqual(
            token_claims.problems_for(SERVICE, "svc-journey-outsider", "service", "", ["trino", "lakekeeper"]),
            [],
        )

    def test_a_service_client_with_a_group_is_a_problem(self):
        claims = dict(SERVICE, groups=["stardelt-users"])
        problems = token_claims.problems_for(claims, "svc-journey-outsider", "service", "", ["lakekeeper"])
        self.assertTrue(any("no group" in problem for problem in problems), problems)

    def test_a_principal_needs_its_username_group_and_lifespan(self):
        for broken, fragment in (
            (dict(PRINCIPAL, preferred_username="someone"), "preferred_username"),
            (dict(PRINCIPAL, groups=[]), "groups"),
            (dict(PRINCIPAL, exp=1300), "lifespan"),
        ):
            problems = token_claims.problems_for(
                broken, "sp-journey", "principal", "stardelt-users", ["trino", "lakekeeper"]
            )
            self.assertTrue(any(fragment in problem for problem in problems), (broken, problems))

    def test_an_audience_or_azp_mismatch_is_a_problem(self):
        problems = token_claims.problems_for(
            dict(SERVICE, aud=["trino"]), "svc-other", "service", "", ["trino", "lakekeeper"]
        )
        self.assertIn("azp is 'svc-journey-outsider'", problems)
        self.assertTrue(any("audience lacks lakekeeper" in problem for problem in problems), problems)


class CommandLineTest(unittest.TestCase):
    def run_main(self, claims, kind, audiences):
        with tempfile.NamedTemporaryFile("w", suffix=".jwt") as handle:
            handle.write(token(claims))
            handle.flush()
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = token_claims.main(
                    ["--token-file", handle.name, "--client", claims["azp"], "--kind", kind,
                     "--group", "stardelt-users", "--audiences", audiences]
                )
            return code, out.getvalue(), err.getvalue()

    def test_the_command_line_accepts_a_service_token_without_a_username(self):
        code, out, err = self.run_main(SERVICE, "service", "trino,lakekeeper")
        self.assertEqual((code, err), (0, ""))
        self.assertIn("azp=svc-journey-outsider", out)


if __name__ == "__main__":
    unittest.main()
