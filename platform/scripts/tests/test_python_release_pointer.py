"""The first-run card installs the Python release that python/pyproject.toml carries (#113)."""
import pathlib
import re
import tomllib
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
PACKAGE = PLATFORM.parent / "python" / "pyproject.toml"
CARD = PLATFORM.parent / "nova" / "frontend" / "src" / "routes" / "Overview.tsx"


class PythonReleasePointer(unittest.TestCase):
    def test_the_first_run_card_installs_the_package_version(self):
        version = tomllib.loads(PACKAGE.read_text())["project"]["version"]
        found = re.findall(r"@(?:python/)?v(\d+\.\d+\.\d+)#subdirectory=python", CARD.read_text())
        self.assertTrue(found, "the card no longer names a vX.Y.Z tag")
        self.assertEqual(set(found), {version}, f"the card must name v{version}")
