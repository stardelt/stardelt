"""A script that changes directory resolves its lib/ sources from an absolute SCRIPT_DIR."""
import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE_SOURCE = re.compile(r'^\.\s+"\$\(dirname "\$\{BASH_SOURCE\[0\]\}"\)/lib/', re.M)
ABSOLUTE_SOURCE = re.compile(r'^\.\s+"\$SCRIPT_DIR/lib/', re.M)


class ScriptLibPathTest(unittest.TestCase):
    def test_no_script_sources_lib_from_a_relative_dirname(self):
        offenders = [path.name for path in sorted((ROOT / "scripts").glob("*.sh"))
                     if RELATIVE_SOURCE.search(path.read_text())]
        self.assertEqual(offenders, [], f"source lib/ through $SCRIPT_DIR, not $(dirname $BASH_SOURCE): {offenders}")

    def test_every_script_that_sources_lib_defines_script_dir(self):
        for path in sorted((ROOT / "scripts").glob("*.sh")):
            text = path.read_text()
            if ABSOLUTE_SOURCE.search(text):
                self.assertIn('SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"', text,
                              f"{path.name} sources $SCRIPT_DIR/lib but never defines SCRIPT_DIR")


if __name__ == "__main__":
    unittest.main()
