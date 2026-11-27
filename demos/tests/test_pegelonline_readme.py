"""The PEGELONLINE README describes the grants the platform really has."""

import pathlib

README = pathlib.Path(__file__).resolve().parent.parent / "examples" / "pegelonline" / "README.md"


def test_the_readme_does_not_promise_a_per_view_grant():
    text = README.read_text(encoding="utf-8")
    assert "readers are granted this, not the tables behind it" not in text
    assert "grants are per **catalog** today" in text
