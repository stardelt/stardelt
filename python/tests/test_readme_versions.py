"""Every install pin in the READMEs names the version this package is (from v0.0.4 on
the repository tag, `v0.0.4`).

Release v0.2.1 shipped with both READMEs still telling a reader to install
v0.2.0, and v0.3.0 had to fix them (#145). A release is a Git tag: it cannot be
corrected afterwards, so the drift has to fail before the tag is pushed.
"""

import pathlib
import re
import tomllib

import pytest

import stardelt

REPO = pathlib.Path(__file__).resolve().parents[1]

# `stardelt` or `stardelt[jupyter]`, from python/ of this repository at the repository's v0.0.N tag.
INSTALL_PIN = re.compile(
    r"stardelt(?:\[[a-z,]+\])? @ git\+https://github\.com/stardelt/stardelt@(?:python/)?(v\d+\.\d+\.\d+)#subdirectory=python"
)

READMES = ["README.md", "stardelt/README.md"]


def pins(path):
    return INSTALL_PIN.findall((REPO / path).read_text(encoding="utf-8"))


def test_every_readme_carries_an_install_pin():
    missing = [path for path in READMES if not pins(path)]
    assert not missing, f"no install pin in {missing}"


@pytest.mark.parametrize("path", READMES)
def test_every_readme_pin_names_the_package_version(path):
    expected = f"v{stardelt.__version__}"
    assert set(pins(path)) == {expected}, (
        f"{path} pins {sorted(set(pins(path)))}, the package is {stardelt.__version__}"
    )


def test_pyproject_version_matches_the_package():
    pyproject = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["project"]["version"] == stardelt.__version__
