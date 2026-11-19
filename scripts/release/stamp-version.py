#!/usr/bin/env python3
"""Write one stardelt version into every file that carries one (stardelt/stardelt#2).

  python3 scripts/release/stamp-version.py 0.0.4            # write the version
  python3 scripts/release/stamp-version.py --check 0.0.4    # change nothing; exit 1 on a mismatch
  python3 scripts/release/stamp-version.py --root DIR 0.0.4 # another checkout (the tests use a copy)

Only 0.0.N is accepted: there is no 0.1.0 before the maintainer says so.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

VERSION_RE = re.compile(r"^0\.0\.[0-9]+$")

# (file, pattern with exactly one group around the version, value template).
# Each pattern must match exactly once in its file.
RULES = [
    ("python/pyproject.toml", r'(?m)^version = "([^"]+)"$', "{v}"),
    ("python/stardelt/__init__.py", r'(?m)^__version__ = "([^"]+)"$', "{v}"),
    ("python/jupyterlab/package.json", r'(?m)^    "version": "([^"]+)",$', "{v}"),
    ("operator/Cargo.toml", r'(?m)^\[workspace\.package\]\nversion = "([^"]+)"$', "{v}"),
    ("nova/Cargo.toml", r'(?m)^\[workspace\.package\]\nversion = "([^"]+)"$', "{v}"),
    ("operator/Cargo.lock", r'(?m)^name = "stardelt-operator"\nversion = "([^"]+)"$', "{v}"),
    ("nova/Cargo.lock", r'(?m)^name = "stardelt-nova-backend"\nversion = "([^"]+)"$', "{v}"),
    ("nova/frontend/package.json", r'(?m)^  "version": "([^"]+)",$', "{v}"),
    ("nova/frontend/package-lock.json", r'(?m)^  "version": "([^"]+)",$', "{v}"),
    ("nova/frontend/package-lock.json",
     r'(?m)^    "": \{\n      "name": "stardelt-nova-frontend",\n      "version": "([^"]+)",$', "{v}"),
    ("platform/chart/Chart.yaml", r"(?m)^version: (\S+)$", "{v}"),
    ("platform/chart/Chart.yaml", r'(?m)^appVersion: "([^"]+)"$', "v{v}"),
    ("operator/chart/Chart.yaml", r"(?m)^version: (\S+)$", "{v}"),
    ("operator/chart/Chart.yaml", r'(?m)^appVersion: "([^"]+)"$', "v{v}"),
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("version")
    ap.add_argument("--check", action="store_true", help="change nothing; exit 1 when a file differs")
    ap.add_argument("--root", default=str(Path(__file__).resolve().parents[2]))
    args = ap.parse_args()
    if not VERSION_RE.match(args.version):
        print(f"✗ {args.version!r} is not 0.0.N", file=sys.stderr)
        return 2
    root = Path(args.root)
    wrong = 0
    for rel, pattern, template in RULES:
        path = root / rel
        text = path.read_text()
        want = template.format(v=args.version)
        matches = list(re.finditer(pattern, text))
        if len(matches) != 1:
            print(f"✗ {rel}: expected one match of {pattern!r}, found {len(matches)}", file=sys.stderr)
            return 2
        m = matches[0]
        have = m.group(1)
        if have == want:
            print(f"✓ {rel}: {have}")
            continue
        if args.check:
            print(f"✗ {rel}: {have}, want {want}")
            wrong += 1
            continue
        text = text[: m.start(1)] + want + text[m.end(1):]
        path.write_text(text)
        print(f"› {rel}: {have} → {want}")
    return 1 if wrong else 0


if __name__ == "__main__":
    sys.exit(main())
