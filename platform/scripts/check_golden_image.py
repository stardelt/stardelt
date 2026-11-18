#!/usr/bin/env python3
"""Prove that the operator image this repository pins renders this repository's Nova pin.

`helm-values/operator.yaml` pins a build of one operator commit, and that commit's `NOVA_IMAGE`
constant (`crates/stardelt-operator/src/provision.rs`) is the Nova image an operator install
renders. Every other pin check reads the operator repository's `origin/main` instead, so all of
them were green on 2026-11-17 while an operator install rendered Nova one build behind the
`manifests/nova-deployment.yaml` pin (stardelt-operator#545).

This check asks GHCR for the pinned operator image's `org.opencontainers.image.revision` label,
reads `NOVA_IMAGE` at that commit in the operator checkout, and compares it with the platform pin.
No skopeo, no crane, no docker: stdlib `urllib.request` only.

Exit codes: 0 the digests agree, 1 they differ, 3 a pin or the image is unreadable.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
OPERATOR_REPO = "stardelt/stardelt-operator"
REGISTRY = "https://ghcr.io/v2/" + OPERATOR_REPO
TOKEN_URL = "https://ghcr.io/token?scope=repository:%s:pull&service=ghcr.io" % OPERATOR_REPO
INDEX_TYPE = "application/vnd.oci.image.index.v1+json"
MANIFEST_TYPE = "application/vnd.oci.image.manifest.v1+json"

OPERATOR_VALUES = "helm-values/operator.yaml"
NOVA_MANIFEST = "manifests/nova-deployment.yaml"
PROVISION_RS = "crates/stardelt-operator/src/provision.rs"

OPERATOR_PIN_RE = re.compile(r'^\s*tag:\s*"?dev@(sha256:[0-9a-f]{64})"?\s*$', re.M)
NOVA_PIN_RE = re.compile(
    r"^\s*image:\s+ghcr\.io/stardelt/nova:dev@(sha256:[0-9a-f]{64})\s*$", re.M
)
NOVA_IMAGE_RE = re.compile(
    r'^pub const NOVA_IMAGE: &str = "ghcr\.io/stardelt/nova:dev@(sha256:[0-9a-f]{64})";',
    re.M,
)


def _short(digest: str) -> str:
    """`95539d9b…` — the first eight hex characters of a digest, or the value unchanged."""
    m = re.search(r"sha256:([0-9a-f]{8})", digest)
    return m.group(1) + "…" if m else digest


def _only_one(found: list[str]) -> list[str]:
    """Exactly one pin, or none: two matches are ambiguous, not two pins."""
    return found if len(found) == 1 else []


def operator_pin(text: str) -> list[str]:
    """The digest of the operator image tag in `helm-values/operator.yaml`, exactly one or `[]`.

    The `dev@` prefix is required: the file has further `tag:` lines for bundled images.
    """
    return _only_one(OPERATOR_PIN_RE.findall(text))


def nova_pin(text: str) -> list[str]:
    """The digest of the Nova image in `manifests/nova-deployment.yaml`, exactly one or `[]`."""
    return _only_one(NOVA_PIN_RE.findall(text))


def nova_image_of(provision_rs: str) -> str:
    """The digest in `pub const NOVA_IMAGE: …`, or `""` when the file does not name one."""
    found = NOVA_IMAGE_RE.findall(provision_rs)
    return found[0] if len(found) == 1 else ""


def revision_of_config(config_blob: str) -> str:
    """`org.opencontainers.image.revision` from an image config blob, or `""`."""
    try:
        config = json.loads(config_blob).get("config") or {}
    except (ValueError, AttributeError):
        return ""
    labels = config.get("Labels") or {}
    revision = labels.get("org.opencontainers.image.revision") or ""
    return revision if isinstance(revision, str) else ""


def _fetch(url: str, headers: dict[str, str], timeout: float = 30.0) -> bytes:
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def resolve_revision(
    repo: str, digest: str, fetch=_fetch
) -> tuple[str, str]:
    """The `org.opencontainers.image.revision` label of `repo@digest`, or `("", error)`.

    Anonymous GHCR token, the index manifest, its `linux/amd64` manifest, that manifest's config
    blob, and the label in it — the same steps as
    `stardelt-operator/scripts/check-cli-image.sh`. `fetch(url, headers) -> bytes` is injectable so
    a test can answer without a network.
    """
    base = "https://ghcr.io/v2/" + repo
    try:
        payload = json.loads(fetch(
            "https://ghcr.io/token?scope=repository:%s:pull&service=ghcr.io" % repo, {}
        ).decode())
        token = payload.get("token") or payload.get("access_token") or ""
        if not token:
            return "", "ghcr.io answered no anonymous token for %s" % repo
        auth = {"Authorization": "Bearer " + token}

        index = json.loads(
            fetch(base + "/manifests/" + digest, dict(auth, Accept=INDEX_TYPE)).decode()
        )
        amd64 = ""
        for manifest in index.get("manifests") or []:
            platform = manifest.get("platform") or {}
            if platform.get("architecture") == "amd64" and platform.get("os") == "linux":
                amd64 = manifest.get("digest") or ""
                break
        if not amd64:
            return "", "%s names no linux/amd64 manifest" % digest

        image = json.loads(
            fetch(base + "/manifests/" + amd64, dict(auth, Accept=MANIFEST_TYPE)).decode()
        )
        config = (image.get("config") or {}).get("digest") or ""
        if not config:
            return "", "%s names no config blob" % amd64

        revision = revision_of_config(fetch(base + "/blobs/" + config, auth).decode())
    except Exception as exc:  # urllib raises a family of errors; the caller wants one line
        return "", "cannot read %s from ghcr.io: %s" % (digest, exc)
    if not revision:
        return "", "%s names no org.opencontainers.image.revision label" % digest
    return revision, ""


def _git(operator_dir: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", operator_dir, *args], capture_output=True, text=True
    )


def nova_image_at(operator_dir: str, revision: str) -> tuple[str, str]:
    """`NOVA_IMAGE` at `revision` in the operator checkout, or `("", error)`.

    The commit is fetched once when the checkout does not have it (a shallow or one-merge-behind
    clone), so an old pin can still be judged.
    """
    if not operator_dir or not revision:
        return "", "no operator checkout or revision to read"
    path = "%s:./%s" % (revision, PROVISION_RS)
    proc = _git(operator_dir, "cat-file", "-e", path)
    if proc.returncode != 0:
        _git(operator_dir, "fetch", "--quiet", "origin", revision)
        proc = _git(operator_dir, "cat-file", "-e", path)
    if proc.returncode != 0:
        return "", "%s is not in %s — run git fetch there" % (revision, operator_dir)
    show = _git(operator_dir, "show", path)
    if show.returncode != 0:
        return "", "cannot read %s at %s" % (PROVISION_RS, revision)
    digest = nova_image_of(show.stdout)
    if not digest:
        return "", "%s at %s names no NOVA_IMAGE" % (PROVISION_RS, revision)
    return digest, ""


def compare(
    operator_digest: str, revision: str, nova_in_image: str, nova_pin_digest: str
) -> tuple[list[str], int]:
    """One `✓`/`✗` line and the exit code (0 equal, 1 not, 3 unreadable or empty input)."""
    missing = []
    if not operator_digest:
        missing.append("the operator pin in " + OPERATOR_VALUES)
    if not revision:
        missing.append("the operator image's revision label")
    if not nova_in_image:
        missing.append("NOVA_IMAGE at that revision")
    if not nova_pin_digest:
        missing.append("the Nova pin in " + NOVA_MANIFEST)
    if missing:
        return ["✗ could not read " + ", ".join(missing)], 3

    head = "operator %s revision %s ships NOVA_IMAGE %s" % (
        _short(operator_digest),
        revision[:7],
        _short(nova_in_image),
    )
    if nova_in_image == nova_pin_digest:
        return ["✓ %s, the pin in %s" % (head, NOVA_MANIFEST)], 0
    return [
        "✗ %s, the pin in %s is %s" % (head, NOVA_MANIFEST, _short(nova_pin_digest))
    ], 1


def _read(path: Path) -> str:
    try:
        return path.read_text()
    except OSError:
        return ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--operator-digest",
        default=None,
        help="the operator image digest to judge (default: the pin in " + OPERATOR_VALUES + ")",
    )
    parser.add_argument(
        "--operator-dir",
        default=None,
        help="the stardelt-operator checkout (default: $OPERATOR_DIR, else ../operator)",
    )
    args = parser.parse_args(argv)

    operator_dir = (
        args.operator_dir
        or os.environ.get("OPERATOR_DIR")
        or str(REPO.parent / "operator")
    )

    operator_digest = (args.operator_digest or "").strip()
    if not operator_digest:
        pins = operator_pin(_read(REPO / OPERATOR_VALUES))
        operator_digest = pins[0] if pins else ""
    elif not operator_digest.startswith("sha256:"):
        operator_digest = "sha256:" + operator_digest

    pins = nova_pin(_read(REPO / NOVA_MANIFEST))
    nova_pin_digest = pins[0] if pins else ""

    if not operator_digest:
        lines, code = compare("", "", "", nova_pin_digest)
        for line in lines:
            print(line)
        return code

    revision, err = resolve_revision(OPERATOR_REPO, operator_digest)
    if err:
        print("✗ operator %s: %s" % (_short(operator_digest), err))
        return 3

    image, err = nova_image_at(operator_dir, revision)
    if err:
        print("✗ operator %s revision %s: %s" % (_short(operator_digest), revision[:7], err))
        return 3

    lines, code = compare(operator_digest, revision, image, nova_pin_digest)
    for line in lines:
        print(line)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
