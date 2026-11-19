"""Unit tests for `scripts/mint-user-token.py --decode` (stardelt/stardelt#232).

Standard library only, like the other tests here: `python3 -m unittest discover -s scripts/tests`
runs them in CI. No network and no cluster: a synthetic token is written to a temporary file
and the script is asked to describe it, which is the part `scripts/verify-spark-auth.sh` and a
person reading a minted token both depend on.
"""
import base64
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "mint-user-token.py"


def segment(value):
    """One base64url segment without padding, as a compact JWS carries it."""
    return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")


def token(claims):
    return "%s.%s.sig" % (segment({"alg": "none"}), segment(claims))


class MintUserTokenDecode(unittest.TestCase):
    def decode(self, value):
        with tempfile.NamedTemporaryFile("w", suffix=".token", delete=False) as f:
            f.write(value)
        path = Path(f.name)
        self.addCleanup(path.unlink)
        return subprocess.run(["python3", str(SCRIPT), "--decode", str(path)],
                              capture_output=True, text=True, timeout=60)

    def test_a_list_audience_is_joined_with_commas(self):
        done = self.decode(token({"aud": ["spark-connect", "nova"],
                                  "azp": "stardelt-dev", "preferred_username": "analyst"}))
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("segments=3", done.stdout)
        self.assertIn("aud=spark-connect,nova", done.stdout)
        self.assertIn("azp=stardelt-dev", done.stdout)

    def test_a_string_audience_is_printed_as_it_is(self):
        done = self.decode(token({"aud": "spark-connect", "azp": "page"}))
        self.assertEqual((done.returncode, done.stderr), (0, ""))
        self.assertIn("aud=spark-connect", done.stdout)

    def test_two_segments_are_refused_without_an_audience_line(self):
        done = self.decode("%s.%s" % (segment({"alg": "none"}), segment({"aud": "spark-connect"})))
        self.assertEqual(done.returncode, 1)
        self.assertIn("not a compact JWS", done.stderr)
        self.assertNotIn("aud=", done.stdout)


if __name__ == "__main__":
    unittest.main()
