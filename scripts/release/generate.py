#!/usr/bin/env python3
"""Pack one release's three generated files from the pins a commit carries (stardelt/stardelt#423).

  python3 scripts/release/generate.py 0.0.4
  python3 scripts/release/generate.py 0.0.4 --commit <sha> --out releases/v0.0.4

Writes `versions.yaml` (every image digest with its source revision, every chart version and
the library versions), `install.yaml` (namespace plus the rendered operator chart) and
`platform-minimal.yaml` (a copy of the example `PlatformInstance`). The digests are read from
the pins in `platform/` and from the constants in `operator/`; nothing is typed by hand.

`versions.yaml` names every image the install runs; the ones stardelt does not build carry
`source: upstream`.

Only 0.0.N is accepted: there is no 0.1.0 before the maintainer says so.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VERSION_RE = re.compile(r"^0\.0\.[0-9]+$")

# The order the images are written in.
IMAGE_ORDER = [
    "stardelt-operator",
    "nova",
    "spark",
    "superset",
    "airflow",
    "dbt",
    "jupyter-singleuser",
    "stardelt-cli",
    "keycloak",
    "postgresql-superset",
    "postgresql-airflow",
]

# `cnpg` -> `cnpg`, `PLUGIN_BARMAN_CLOUD` -> `plugin-barman-cloud`.
CHART_RE = re.compile(r"^([A-Z][A-Z_]*)_VERSION\s*[:?]?=\s*(\S+)\s*$", re.M)

# The only place a stardelt-cli digest is pinned (operator/scripts/check-cli-image.sh reads it).
CLI_RE = re.compile(
    r'^pub const IMAGE: &str = "ghcr\.io/stardelt/stardelt-cli:[^@]*@(sha256:[0-9a-f]{64})";$',
    re.M,
)

REVISION_LABEL = "org.opencontainers.image.revision"


def charts(makefile_text: str) -> list[tuple[str, str]]:
    """Every `<NAME>_VERSION` line of the platform Makefile, in file order."""
    return [(name.lower().replace("_", "-"), version) for name, version in CHART_RE.findall(makefile_text)]


def cli_digest(rs_text: str) -> str:
    """The digest of the maintenance CLI image constant, or exit 1."""
    found = CLI_RE.findall(rs_text)
    if len(found) != 1:
        print(f"✗ maintenance.rs: expected one IMAGE digest, found {len(found)}", file=sys.stderr)
        raise SystemExit(1)
    return found[0]


# The dbt task image: pinned in the Airflow values on both install paths (#1893, #1918).
DBT_RE = re.compile(
    r"^\s*value: ghcr\.io/stardelt/dbt:demo@(sha256:[0-9a-f]{64})\s*$", re.M
)


def dbt_digest(airflow_values: str) -> str:
    """The dbt task image's digest from `platform/helm-values/airflow.yaml`, or exit 1."""
    found = DBT_RE.findall(airflow_values)
    if len(found) != 1:
        print(f"✗ airflow.yaml: expected one dbt image digest, found {len(found)}", file=sys.stderr)
        raise SystemExit(1)
    return found[0]


UPSTREAM_PINS = re.compile(
    r'^pub const (?P<name>IMAGE|SUPERSET_PG_IMAGE|AIRFLOW_PG_IMAGE): &str = '
    r'"(?P<ref>[^"@]+)@(?P<digest>sha256:[0-9a-f]{64})";$',
    re.M,
)


def _upstream_constant(name: str, text: str, path: str) -> tuple[str, str]:
    """(reference without tag, digest) of one pinned constant, or exit 1."""
    found = [m for m in UPSTREAM_PINS.finditer(text) if m["name"] == name]
    if len(found) != 1:
        print(f"✗ {path}: expected one {name} digest, found {len(found)}", file=sys.stderr)
        raise SystemExit(1)
    return found[0]["ref"].rsplit(":", 1)[0], found[0]["digest"]


def upstream_pins(keycloak_rs: str, pg_dump_rs: str) -> dict[str, tuple[str, str]]:
    """The three third-party images the install runs, as (reference, digest), in order."""
    pg_dump = "operator/crates/stardelt-operator/src/pg_dump.rs"
    return {
        "keycloak": _upstream_constant(
            "IMAGE", keycloak_rs, "operator/crates/stardelt-operator/src/keycloak.rs"
        ),
        "postgresql-superset": _upstream_constant("SUPERSET_PG_IMAGE", pg_dump_rs, pg_dump),
        "postgresql-airflow": _upstream_constant("AIRFLOW_PG_IMAGE", pg_dump_rs, pg_dump),
    }


def upstream_images(root: Path) -> dict[str, tuple[str, str]]:
    """`upstream_pins` read from the operator sources."""
    src = root / "operator" / "crates" / "stardelt-operator" / "src"
    return upstream_pins(
        (src / "keycloak.rs").read_text(),
        (src / "pg_dump.rs").read_text(),
    )


def one_digest_per_app(pins: dict[str, list[str]]) -> dict[str, str]:
    """One digest per app, image names as versions.yaml writes them.

    Two different digests under one app mean the pin rollup did not converge: refuse.
    """
    images: dict[str, str] = {}
    for app, digests in pins.items():
        distinct = sorted(set(digests))
        if len(distinct) != 1:
            print(f"✗ {app}: the pins disagree: {distinct}", file=sys.stderr)
            raise SystemExit(1)
        images["jupyter-singleuser" if app == "jupyter" else app] = distinct[0]
    return images


def _get(url: str, token: str, accept: str = "") -> bytes:
    headers = {}
    # Quay answers 401 to a request that carries `Authorization: Bearer ` with nothing in it.
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if accept:
        headers["Accept"] = accept
    return urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30).read()


def registry(reference: str) -> tuple[str, str]:
    """(token URL, v2 base URL) for a full reference without tag or digest."""
    host, _, repo = reference.partition("/")
    if host == "ghcr.io":
        return (
            f"https://ghcr.io/token?scope=repository:{repo}:pull&service=ghcr.io",
            f"https://ghcr.io/v2/{repo}",
        )
    if host == "quay.io":
        return (
            f"https://quay.io/v2/auth?service=quay.io&scope=repository:{repo}:pull",
            f"https://quay.io/v2/{repo}",
        )
    raise SystemExit(f"✗ {reference}: unknown registry")


def revision(reference: str, digest: str) -> str:
    """`org.opencontainers.image.revision` of a public image at `digest`.

    `reference` is a full repository reference (`ghcr.io/stardelt/nova`,
    `quay.io/keycloak/keycloak`). What operator/scripts/check-cli-image.sh does with curl:
    anonymous token, the manifest (index or single), the linux/amd64 manifest, its config blob,
    the label. A network error is fatal — a release must not be packed blind.
    """
    token_url, base = registry(reference)
    accept = "application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json"
    try:
        token = json.loads(_get(token_url, ""))["token"]
        manifest = json.loads(_get(f"{base}/manifests/{digest}", token, accept))
        if "manifests" in manifest:
            amd64 = ""
            for entry in manifest["manifests"]:
                platform = entry.get("platform") or {}
                if platform.get("os") == "linux" and platform.get("architecture") == "amd64":
                    amd64 = entry["digest"]
                    break
            if not amd64:
                raise ValueError(f"no linux/amd64 manifest in the index {digest}")
            manifest = json.loads(
                _get(f"{base}/manifests/{amd64}", token, "application/vnd.oci.image.manifest.v1+json")
            )
        config = json.loads(_get(f"{base}/blobs/{manifest['config']['digest']}", token))
    except Exception as err:  # noqa: BLE001 — any failure here must stop the release
        print(f"✗ {reference}: cannot read {digest} from {base} ({err})", file=sys.stderr)
        raise SystemExit(1) from err
    labels = (config.get("config") or {}).get("Labels") or {}
    return labels.get(REVISION_LABEL, "")


def stardelt_revision(image: str, digest: str) -> str:
    """`org.opencontainers.image.revision` of one of the images stardelt builds."""
    return revision(f"ghcr.io/stardelt/{image}", digest)


def versions_yaml(
    version: str,
    commit: str,
    images: dict[str, str],
    charts_: list[tuple[str, str]],
    python: dict[str, str],
    upstream: frozenset[str] = frozenset(),
) -> str:
    """The versions.yaml text: every image digest, chart version and library version."""
    lines = [
        "# Generated by scripts/release/generate.py — do not edit.",
        f"version: v{version}",
        f"commit: {commit}",
        "images:",
    ]
    if sorted(images) != sorted(IMAGE_ORDER):
        print(f"✗ the images are {sorted(images)}, want {sorted(IMAGE_ORDER)}", file=sys.stderr)
        raise SystemExit(1)
    for image in IMAGE_ORDER:
        digest, rev = images[image]
        extra = ", source: upstream" if image in upstream else ""
        lines.append(f'  {image}: {{digest: "{digest}", revision: "{rev}"{extra}}}')
    lines.append("charts:")
    for name, chart_version in charts_:
        lines.append(f"  {name}: {chart_version}")
    lines.append("python:")
    for name, lib_version in python.items():
        lines.append(f"  {name}: {lib_version}")
    return "\n".join(lines) + "\n"


def _one(pattern: str, text: str, what: str) -> str:
    found = re.findall(pattern, text, re.M)
    if len(found) != 1:
        print(f"✗ {what}: expected one match of {pattern!r}, found {len(found)}", file=sys.stderr)
        raise SystemExit(1)
    return found[0]


def library_versions(root: Path) -> dict[str, str]:
    pyproject = (root / "python" / "pyproject.toml").read_text()
    package = (root / "python" / "jupyterlab" / "package.json").read_text()
    return {
        "stardelt": _one(r'^version = "([^"]+)"$', pyproject, "python/pyproject.toml"),
        "stardelt-jupyterlab": _one(r'^    "version": "([^"]+)",$', package, "python/jupyterlab/package.json"),
    }


def pins(root: Path) -> dict[str, list[str]]:
    """Every pin of every app, from the manifests check_pin_freshness.py reads."""
    sys.path.insert(0, str(root / "platform" / "scripts"))
    from check_pin_freshness import APPS, parse_pins  # noqa: PLC0415 — the platform script's list

    grouped: dict[str, list[str]] = {}
    for app, manifest, kind, _tag in APPS:
        text = (root / "platform" / manifest).read_text()
        grouped.setdefault(app, []).extend(parse_pins(text, kind))
    return grouped


def install_yaml(root: Path, version: str, commit: str, operator_digest: str) -> str:
    """The header, platform/manifests/namespace.yaml and the rendered operator chart."""
    header = (
        f"# stardelt v{version} (commit {commit}). "
        "Install: kubectl apply --server-side --force-conflicts -f install.yaml"
    )
    namespace = (root / "platform" / "manifests" / "namespace.yaml").read_text()
    rendered = subprocess.run(
        [
            "helm", "template", "stardelt-operator", "operator/chart",
            "--namespace", "stardelt",
            "--include-crds",
            "-f", "platform/helm-values/operator.yaml",
            "--set", f"image.tag=v{version}@{operator_digest}",
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    parts = [header, namespace, rendered]
    return "\n---\n".join(part.strip("\n") + "\n" for part in parts)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("version")
    ap.add_argument("--commit", default=None, help="the release commit (default: git rev-parse HEAD)")
    ap.add_argument("--out", default=None, help="output directory (default: releases/v0.0.N)")
    args = ap.parse_args(argv)

    if not VERSION_RE.match(args.version):
        print(f"✗ {args.version!r} is not 0.0.N", file=sys.stderr)
        return 2
    root = ROOT
    commit = args.commit or subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()
    out = Path(args.out) if args.out else Path(f"releases/v{args.version}")
    if not out.is_absolute():
        out = Path.cwd() / out

    images = one_digest_per_app(pins(root))
    images["dbt"] = dbt_digest(
        (root / "platform" / "helm-values" / "airflow.yaml").read_text()
    )
    images["stardelt-cli"] = cli_digest(
        (root / "operator" / "crates" / "stardelt-operator" / "src" / "maintenance.rs").read_text()
    )
    upstream = upstream_images(root)
    images.update({name: digest for name, (_ref, digest) in upstream.items()})

    image_lines = {}
    for image, digest in images.items():
        if image in upstream:
            ref, _digest = upstream[image]
            image_lines[image] = (digest, revision(ref, digest))
        else:
            image_lines[image] = (digest, stardelt_revision(image, digest))
    chart_lines = charts((root / "platform" / "Makefile").read_text())

    out.mkdir(parents=True, exist_ok=True)
    (out / "versions.yaml").write_text(
        versions_yaml(
            args.version,
            commit,
            image_lines,
            chart_lines,
            library_versions(root),
            upstream=frozenset(upstream),
        )
    )
    (out / "install.yaml").write_text(
        install_yaml(root, args.version, commit, images["stardelt-operator"])
    )
    (out / "platform-minimal.yaml").write_bytes(
        (root / "operator" / "examples" / "platform-minimal.yaml").read_bytes()
    )

    for name in ("versions.yaml", "install.yaml", "platform-minimal.yaml"):
        print(f"› {out / name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
