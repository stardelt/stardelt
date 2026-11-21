"""The Spark image ships the projected-token catalog client (#492, #769).

Three facts keep the measured behaviour from regressing without an image build: the class reads
the file inside `authenticate` (not in the constructor or `catalogSession`), the Dockerfile
compiles *and runs* the check at build time, and the jar lands where Iceberg's class loader and
the executors find it.
"""
import unittest
from pathlib import Path

SPARK = Path(__file__).resolve().parents[2] / "images" / "spark"


class SparkImageAuthTest(unittest.TestCase):
    def test_the_token_is_read_per_request(self):
        source = (SPARK / "TokenFileAuthManager.java").read_text()
        authenticate = source[source.index("public HTTPRequest authenticate") : source.index("private String token()")]
        self.assertIn("token()", authenticate, "authenticate does not read the file")
        self.assertNotIn("this.token =", source, "the token is cached in a field")

    def test_the_build_runs_the_check_and_installs_the_jar(self):
        dockerfile = (SPARK / "Dockerfile").read_text()
        self.assertIn("TokenFileAuthManagerCheck", dockerfile)
        self.assertIn("io.stardelt.iceberg.TokenFileAuthManagerCheck", dockerfile)
        self.assertIn("/opt/spark/jars/stardelt-iceberg-auth.jar", dockerfile)

    def test_the_property_name_is_the_documented_one(self):
        source = (SPARK / "TokenFileAuthManager.java").read_text()
        self.assertIn('"rest.auth.token-file"', source)


if __name__ == "__main__":
    unittest.main()
