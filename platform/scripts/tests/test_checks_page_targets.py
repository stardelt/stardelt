"""Every `verify-` target in the Makefile is named on the checks page, and nothing else is (#909)."""
import pathlib
import re
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
MAKEFILE = PLATFORM / "Makefile"
PAGE = PLATFORM.parent.parent / "stardelt-docs" / "docs" / "operations" / "checks.md"


def makefile_targets():
    """The `verify-` targets the Makefile documents with a `##` comment."""
    text = MAKEFILE.read_text()
    return {
        match.group(1)
        for match in re.finditer(r"^([a-z0-9-]+):.*##", text, re.MULTILINE)
        if match.group(1).startswith("verify-")
    }


def page_targets():
    """The `verify-` commands the checks page tells a reader to run."""
    return set(re.findall(r"make (verify-[a-z0-9-]+)", PAGE.read_text()))


class ChecksPageTargets(unittest.TestCase):
    def setUp(self):
        if not PAGE.exists():
            self.skipTest(f"{PAGE} is not checked out next to this repository")

    def test_every_makefile_target_is_on_the_page(self):
        missing = sorted(makefile_targets() - page_targets())
        self.assertEqual(
            [],
            missing,
            "a verify- target exists but the checks page does not name it: "
            + ", ".join(missing),
        )

    def test_the_page_names_no_unknown_target(self):
        unknown = sorted(page_targets() - makefile_targets())
        self.assertEqual(
            [],
            unknown,
            "the checks page names a command the Makefile does not define (renamed?): "
            + ", ".join(unknown),
        )


if __name__ == "__main__":
    unittest.main()
