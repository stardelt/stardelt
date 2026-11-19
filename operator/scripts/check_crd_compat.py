#!/usr/bin/env python3
"""Fail when a served CRD dropped, retyped or newly required a field since the last release.

Every kind is `v1alpha1` in `0.0.x`.  The promise is N−1: a resource that release N accepts is
accepted by release N+1, *or* the release notes name the field that changed.  The CRDs in
`chart/crds/` are generated (`crdgen`), so they can be compared with the last release tag:

    python3 scripts/check_crd_compat.py [--base REF] [--changelog ../CHANGELOG.md]

`--base` defaults to the newest `v0.0.*` tag (what `release-publish.yml` creates).  Each breaking
path must appear verbatim in the changelog file; a missing changelog counts as empty.  Exit 1 when
a breaking path is not named there.

stardelt/stardelt#433 (#23).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import yaml

CRD_DIR = Path("chart/crds")


def _kind(doc: dict) -> str:
    return (
        (doc.get("spec") or {}).get("names", {}).get("kind")
        or (doc.get("metadata") or {}).get("name")
        or "?"
    )


def _versions(doc: dict) -> dict[str, dict]:
    """Served version name -> its `schema.openAPIV3Schema`."""
    out = {}
    for version in (doc.get("spec") or {}).get("versions") or []:
        name = version.get("name")
        if name:
            out[name] = ((version.get("schema") or {}).get("openAPIV3Schema")) or {}
    return out


def _type(schema: dict) -> str | None:
    # crdgen leaves `type: object` implicit on schemas that carry `properties`.
    return schema.get("type") or ("object" if "properties" in schema else None)


def _flatten(schema: dict, path: str, is_required: bool, out: dict[str, dict]) -> None:
    """Record every field of a schema by its dotted path; array items get a `[]` suffix."""
    out[path] = {
        "type": _type(schema),
        "required": is_required,
        "has_default": "default" in schema,
    }
    properties = schema.get("properties")
    if isinstance(properties, dict):
        required = schema.get("required") or []
        for name, sub in properties.items():
            if isinstance(sub, dict):
                _flatten(sub, f"{path}.{name}", name in required, out)
    items = schema.get("items")
    if isinstance(items, dict):
        # An item inherits the array's requiredness: `spec.x[].y` is demanded only if `x` is.
        _flatten(items, f"{path}[]", is_required, out)


def _fields(doc: dict) -> dict[str, dict[str, dict]]:
    """Served version -> flattened fields, each path prefixed with the kind."""
    kind = _kind(doc)
    return {version: _walk(schema, kind) for version, schema in _versions(doc).items()}


def _walk(schema: dict, kind: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    properties = schema.get("properties")
    if isinstance(properties, dict):
        required = schema.get("required") or []
        for name, sub in properties.items():
            if isinstance(sub, dict):
                _flatten(sub, f"{kind}.{name}", name in required, out)
    return out


def _demanded(path: str, fields: dict[str, dict]) -> bool:
    """True when every schema above `path` is required, so an existing resource must supply it."""
    parent = path
    while "." in parent:
        parent = parent.rsplit(".", 1)[0]
        if "." not in parent:
            break  # the kind itself
        if not fields.get(parent, {}).get("required"):
            return False
    return True


def breaking_changes(base_docs: dict[str, dict], head_docs: dict[str, dict]) -> list[str]:
    """Every way `head_docs` breaks a resource that `base_docs` accepted.

    Keys are CRD file names, values the parsed CRD.  Adding a field is never breaking.
    """
    report: list[str] = []
    for name, base in base_docs.items():
        kind = _kind(base)
        head = head_docs.get(name)
        if head is None:
            report.append(f"{kind}: removed")
            continue

        base_versions = _fields(base)
        head_versions = _fields(head)

        for version, base_fields in base_versions.items():
            head_fields = head_versions.get(version)
            if head_fields is None:
                report.append(f"{kind} {version}: no longer served")
                continue
            for path, old in base_fields.items():
                if path.endswith("[]"):
                    continue  # an array's item schema is reported through the array itself
                new = head_fields.get(path)
                if new is None:
                    report.append(f"{path}: removed")
                    continue
                if old["type"] and new["type"] and old["type"] != new["type"]:
                    report.append(f"{path}: type {old['type']} → {new['type']}")
                if new["required"] and not old["required"]:
                    if path in base_fields or not new["has_default"]:
                        report.append(f"{path}: newly required")
            # A field that did not exist at base and arrives required without a default also
            # breaks stored resources — but only if every schema above it is required too;
            # under an optional object a stored resource simply omits the whole subtree.
            added = [p for p in head_fields if p not in base_fields]
            added.sort(key=lambda p: (p.count("."), p))
            reported: set[str] = set()
            for path in added:
                if path.endswith("[]"):
                    continue
                new = head_fields[path]
                if not new["required"] or new["has_default"]:
                    continue
                if not _demanded(path, head_fields):
                    continue
                if path.rsplit(".", 1)[0] in reported:
                    continue
                reported.add(path)
                report.append(f"{path}: newly required")
    return report


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True
    ).stdout


def _default_base() -> str | None:
    tags = [t for t in _git("tag", "--list", "v0.0.*", "--sort=-v:refname").split() if t]
    return tags[0] if tags else None


def _docs_at(base: str) -> dict[str, dict]:
    docs: dict[str, dict] = {}
    for path in sorted(CRD_DIR.glob("*.yaml")):
        repo_path = f"operator/{path.as_posix()}"
        try:
            text = _git("show", f"{base}:{repo_path}")
        except subprocess.CalledProcessError:
            continue
        docs[path.name] = yaml.safe_load(text)
    return docs


def _head_docs() -> dict[str, dict]:
    return {p.name: yaml.safe_load(p.read_text()) for p in sorted(CRD_DIR.glob("*.yaml"))}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", help="git ref to compare with (default: newest v0.0.* tag)")
    parser.add_argument(
        "--changelog", default="../CHANGELOG.md", help="file that must name every breaking change"
    )
    args = parser.parse_args(argv)

    base = args.base or _default_base()
    if not base:
        print("– no release tag yet; nothing to compare")
        return 0

    changes = breaking_changes(_docs_at(base), _head_docs())
    if not changes:
        print(f"✓ CRDs compatible with {base}")
        return 0

    changelog = Path(args.changelog)
    noted = changelog.read_text() if changelog.is_file() else ""

    failed = 0
    for line in changes:
        if line.split(":", 1)[0] in noted:
            print(f"✓ {line} (in {changelog.name})")
        else:
            print(f"✗ {line}")
            failed += 1

    if failed:
        print(
            f"✗ {failed} breaking change(s) not named in {changelog} "
            f"(promise N−1, stardelt/stardelt#23)"
        )
        return 1
    print(f"✓ CRDs compatible with {base} ({len(changes)} change(s) named in {changelog.name})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
