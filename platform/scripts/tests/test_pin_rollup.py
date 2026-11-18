"""`scripts/pin-rollup.sh` rolls every image `scripts/check_pin_freshness.py` checks (#1728).

The checker is the definition of "a pin on `main`"; the rollup is the thing that moves it.
When the two disagree the drift is only fixable by hand, which is what happened to the
notebook image (`ghcr.io/stardelt/jupyter-singleuser:dev`, three files): it was checked but
never rolled, so `make check-pins` stayed red until a person noticed.

The app names are derived from the checker's `APPS`, not listed here, so the next image
added to the checker fails this test until the rollup covers it too.

Standard library only, no network and no cluster; discovered by
`python3 -m unittest discover -s scripts/tests`.
"""

import unittest
from pathlib import Path

from scripts.check_pin_freshness import APPS

REPO = Path(__file__).resolve().parents[2]
ROLLUP = REPO / "scripts" / "pin-rollup.sh"


def app_names() -> list[str]:
    """The distinct apps the freshness checker resolves, in the order it names them."""
    return list(dict.fromkeys(app for app, *_ in APPS))


def changes_block(text: str) -> str:
    """The lines between `changes=()` and the empty-changes early exit."""
    start = text.index("changes=()")
    end = text.index('if [ "${#changes[@]}" -eq 0 ]', start)
    return text[start:end]


class PinRollupCoversCheckerTest(unittest.TestCase):
    def test_each_checked_app_is_resolved_and_added_to_changes(self):
        text = ROLLUP.read_text()
        block = changes_block(text)
        self.assertIn("APP=jupyter WRITE_MANIFEST_ONLY=1", text,
                      "the notebook image is pinned three times; its writer lives in pin-image.sh")
        for app in app_names():
            # `pin <app>` resolves the tag's digest; without it the app is never compared.
            self.assertIn(f"(pin {app})", text,
                          f"scripts/pin-rollup.sh does not resolve the {app} pin,"
                          f" but scripts/check_pin_freshness.py checks it")
            # The `changes` entry is what the PR body and the output line report; the
            # operator is shortened to `operator` there.
            label = app.removeprefix("stardelt-")
            self.assertIn(f'changes+=("{label} ', block,
                          f"scripts/pin-rollup.sh never reports a {label} pin change"
                          f" in its `changes` block")


if __name__ == "__main__":
    unittest.main()
