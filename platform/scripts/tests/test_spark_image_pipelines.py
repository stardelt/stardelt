"""The Spark image installs what the `spark-pipelines` CLI imports (platform#1443).

`spark-pipelines` is a Spark Connect client: `pyspark/sql/connect/utils.py:check_dependencies`
stops at the first missing Python package, so the CLI cannot run in an image that does not
ship them (platform#1386 S0a). This is a text test like the other Dockerfile checks here —
no image is built and no cluster is touched.
"""
import unittest
from pathlib import Path

DOCKERFILE = Path(__file__).resolve().parents[2] / "images" / "spark" / "Dockerfile"

CLI_PINS = (
    "pyyaml==6.0.3",
    "grpcio==1.85.1",
    "grpcio-status==1.85.1",
    "googleapis-common-protos==1.76.0",
    "zstandard==0.25.0",
)

# The modules the five distributions provide, as `check_dependencies` imports them.
CLI_IMPORTS = ("yaml, grpc, grpc_status, google.protobuf, zstandard")


class SparkImagePipelinesTest(unittest.TestCase):
    def test_the_cli_dependencies_are_pinned(self):
        text = DOCKERFILE.read_text()
        for pin in CLI_PINS:
            self.assertIn(pin, text, f"images/spark/Dockerfile does not pin {pin}")

    def test_the_build_asserts_them(self):
        text = DOCKERFILE.read_text()
        self.assertIn(CLI_IMPORTS[0], text, "the build assertion does not import the CLI's modules")

    def test_the_worker_python_pins_are_unchanged(self):
        text = DOCKERFILE.read_text()
        for pin in ("numpy==2.0.2", "pandas==2.2.3", "pyarrow==18.1.0"):
            self.assertIn(pin, text, f"images/spark/Dockerfile no longer pins {pin} (parity gate)")


if __name__ == "__main__":
    unittest.main()
