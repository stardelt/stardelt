#!/usr/bin/env python3
"""The dbt image's licence gate: every installed package must be OSI-permissive.

usage: check-licenses.py <licenses.json> <license-exceptions.txt>

`licenses.json` is `pip-licenses --format=json`: a list of objects with at least `Name`,
`Version` and `License`. `license-exceptions.txt` is one package per line (`#` comments
allowed): the package name, then the licence string exactly as `pip-licenses` prints it.
A package is exempt only when both match, so a version bump that changes a licence fails
the build instead of silently passing.

Standard library only: no network, no shell-out, no third-party import. Exit 0 pass,
1 offending packages, 2 unreadable or malformed input (never 0).
"""

from __future__ import annotations

import json
import re
import sys

# SPDX identifiers accepted without an exception.
ALLOWED = {
    "Apache-2.0",
    "MIT",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "PSF-2.0",
    "Python-2.0",
    "Unlicense",
    "CC0-1.0",
}

# `pip-licenses` prints a mix of classifier names and SPDX names; map the classifier
# spellings onto SPDX before the allowed test.
NORMALISE = {
    "MIT License": "MIT",
    "Apache Software License": "Apache-2.0",
    "BSD License": "BSD-3-Clause",
    "3-Clause BSD License": "BSD-3-Clause",
    "Mozilla Public License 2.0 (MPL 2.0)": "MPL-2.0",
}

WHITESPACE = re.compile(r"\s+")


class Malformed(Exception):
    """The input is not what this gate can judge; never a pass."""


def collapse(text: str) -> str:
    """Runs of whitespace become one space, and the ends are trimmed."""
    return WHITESPACE.sub(" ", text).strip()


def load_exceptions(path: str) -> dict[str, str]:
    """`package -> licence string`; the file's only purpose is this mapping."""
    exceptions: dict[str, str] = {}
    try:
        with open(path, encoding="utf-8") as handle:
            lines = handle.read().splitlines()
    except OSError as error:
        raise Malformed(f"cannot read {path}: {error.strerror or error}") from error
    for number, line in enumerate(lines, start=1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            raise Malformed(f"{path}:{number}: expected `package licence`, got {line!r}")
        exceptions[parts[0]] = collapse(parts[1])
    return exceptions


def load_packages(path: str) -> list[dict[str, str]]:
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except OSError as error:
        raise Malformed(f"cannot read {path}: {error.strerror or error}") from error
    except json.JSONDecodeError as error:
        raise Malformed(f"{path} is not valid JSON: {error}") from error
    if not isinstance(data, list):
        raise Malformed(f"{path} must be a JSON list, got {type(data).__name__}")
    packages = []
    for entry in data:
        if not isinstance(entry, dict):
            raise Malformed(f"{path} has a non-object entry: {entry!r}")
        name = entry.get("Name")
        licence = entry.get("License")
        if not isinstance(name, str) or not isinstance(licence, str):
            raise Malformed(f"{path}: an entry has no string `Name`/`License`: {entry!r}")
        packages.append(
            {
                "Name": name,
                "Version": str(entry.get("Version", "?")),
                "License": collapse(licence),
            }
        )
    return packages


def allowed_component(component: str) -> bool:
    """One leaf of the licence expression, as SPDX."""
    token = component.strip()
    while token.startswith("(") and token.endswith(")"):
        token = token[1:-1].strip()
    token = collapse(token)
    return NORMALISE.get(token, token) in ALLOWED


def allowed_expression(expression: str) -> bool:
    """`;` and ` AND ` are conjunctions, ` OR ` is a disjunction: every part must pass."""
    for part in re.split(r" AND |;", expression):
        alternatives = re.split(r" OR ", part)
        if not any(allowed_component(alternative) for alternative in alternatives):
            return False
    return True


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: check-licenses.py <licenses.json> <license-exceptions.txt>", file=sys.stderr)
        print("check-licenses: ERROR wrong arguments")
        return 2
    try:
        exceptions = load_exceptions(argv[2])
        packages = load_packages(argv[1])
    except Malformed as error:
        print(f"check-licenses: ERROR {error}")
        return 2

    offenders = []
    for package in packages:
        exempt = exceptions.get(package["Name"]) == package["License"]
        if not exempt and not allowed_expression(package["License"]):
            offenders.append(package)

    if offenders:
        for package in offenders:
            print(f"\u2717 {package['Name']} {package['Version']}: {package['License']}")
        print(
            f"check-licenses: FAIL ({len(offenders)} of {len(packages)} package(s) not allowed)"
        )
        return 1
    print(f"check-licenses: PASS ({len(packages)} package(s))")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
