#!/usr/bin/env python3
"""Fail when the registry's tag moved ahead of a checked-in pin.

No cluster, no kubectl: it resolves each pin's tag with `PRINT_DIGEST=1` mode of
`scripts/pin-image.sh` and compares that digest with the one the manifests pin.
The notebook image is checked in helm-values/jupyterhub.yaml, helm-values/airflow.yaml and
chart/values.yaml because it is pinned three times. See stardelt-platform#556.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PIN_IMAGE = REPO / "scripts" / "pin-image.sh"

# (app, manifest, kind, tag): the registry tag whose digest the manifest must pin.
APPS = [
    ("nova", "manifests/nova-deployment.yaml", "image", "dev"),
    ("spark", "manifests/spark.yaml", "image", "dev"),
    ("stardelt-operator", "helm-values/operator.yaml", "tag", "dev"),
    ("superset", "helm-values/superset.yaml", "tag", "dev"),
    ("airflow", "helm-values/airflow.yaml", "digest", "demo"),
    ("jupyter", "helm-values/jupyterhub.yaml", "tag", "dev"),
    ("jupyter", "helm-values/airflow.yaml", "notebook", "dev"),
    ("jupyter", "chart/values.yaml", "tag", "dev"),
]

DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
# The notebook library's pin in the Makefile: the commit and the release it belongs to.
LIB_REF_RE = re.compile(r"^STARDELT_LIB_REF\s*:?=\s*([0-9a-f]{40})\s*$", re.M)
LIB_TAG_RE = re.compile(r"^STARDELT_LIB_TAG\s*:?=\s*(v\d+\.\d+\.\d+)\s*$", re.M)


def _short(digest: str) -> str:
    m = re.search(r"sha256:([0-9a-f]{8})", digest)
    if m:
        return m.group(1) + "…"
    # A bare commit SHA: `_short` names the notebook library's pinned commit too.
    if re.fullmatch(r"[0-9a-f]{40}", digest):
        return digest[:8] + "…"
    return digest


def parse_pins(text: str, kind: str) -> list[str]:
    """Every `sha256:<64 hex>` this file pins for one app.

    kind="image": every `image: <repo>:dev@sha256:<digest>` line and every
    `spark.kubernetes.container.image <repo>:dev@sha256:<digest>` line.
    kind="tag":   the `tag: dev@sha256:<digest>` line, unquoted (helm-values/operator.yaml)
                  or quoted (helm-values/superset.yaml).
    kind="digest": the single bare `digest: sha256:<digest>` line
    (helm-values/airflow.yaml); anything but exactly one is "no pin".
    kind="notebook": the single `value: ghcr.io/stardelt/jupyter-singleuser:dev@sha256:<digest>`
    line (PIPELINE_NOTEBOOK_IMAGE in helm-values/airflow.yaml); exactly one, like "digest".
    """
    if kind == "image":
        pattern = re.compile(
            r"^\s*(?:image:\s+ghcr\.io/stardelt/(?:nova|spark):dev@"
            r"|spark\.kubernetes\.container\.image\s+ghcr\.io/stardelt/spark:dev@)"
            r"(sha256:[0-9a-f]{64})\s*$",
            re.M,
        )
    elif kind == "tag":
        # The stardelt image's own tag, `dev@<digest>`. Scoped to that tag: the bundled
        # sub-images carry `tag:` lines of their own now (Postgres and Redis in
        # helm-values/superset.yaml, #1594) and an unscoped match counted them as this
        # app's pins.
        pattern = re.compile(r'^\s*tag:\s*"?dev@(sha256:[0-9a-f]{64})"?\s*$', re.M)
    elif kind == "digest":
        pattern = re.compile(r"^\s*digest: (sha256:[0-9a-f]{64})\s*$", re.M)
        found = pattern.findall(text)
        return found if len(found) == 1 else []
    elif kind == "notebook":
        # `value: ghcr.io/stardelt/jupyter-singleuser:dev@sha256:<digest>` — the task pods'
        # image in helm-values/airflow.yaml (PIPELINE_NOTEBOOK_IMAGE). Exactly one, like `digest`.
        pattern = re.compile(
            r"^\s*value: ghcr\.io/stardelt/jupyter-singleuser:dev@(sha256:[0-9a-f]{64})\s*$", re.M
        )
        found = pattern.findall(text)
        return found if len(found) == 1 else []
    else:
        raise ValueError(f"unknown kind: {kind}")
    return pattern.findall(text)


def compare(
    app: str, pinned: list[str], resolved: str, where: str, tag: str = "dev"
) -> list[str]:
    """One line per pin: '✓ …' when it equals `resolved`, else '✗ … — run ./scripts/pin-image.sh <app>'."""
    lines: list[str] = []
    if pinned and all(p == resolved for p in pinned):
        suffix = f" ({len(pinned)} pins)" if len(pinned) > 1 else ""
        lines.append(f"✓ {app} {tag}@{_short(resolved)} matches {where}{suffix}")
        return lines
    for p in pinned:
        if p == resolved:
            lines.append(f"✓ {app} {tag}@{_short(resolved)} matches {where}")
        else:
            lines.append(
                f"✗ {app} pinned {_short(p)}, {tag} resolves to {_short(resolved)}"
                f" — run ./scripts/pin-image.sh {app}"
            )
    if not pinned:
        lines.append(f"✗ {app} has no pin in {where} — run ./scripts/pin-image.sh {app}")
    return lines


def exit_code(lines: list[str]) -> int:
    """0 when no line starts with '✗', else 1."""
    return 1 if any(line.startswith("✗") for line in lines) else 0


def compare_lib_tag(tag: str, ref: str, tag_commit: str) -> str:
    """One line: the Makefile's tag must be the release that contains its pinned commit."""
    if tag_commit == ref:
        return f"✓ notebook library {tag}@{_short(ref)} matches the tag python/{tag} in stardelt/stardelt"
    return (
        f"✗ the Makefile names {tag} for {_short(ref)}, but the tag {tag} is {_short(tag_commit)}"
        " — set STARDELT_LIB_TAG to the release that contains the pinned commit"
    )


def resolve_tag_commit(tag: str) -> tuple[str | None, str]:
    proc = subprocess.run(
        ["git", "ls-remote", "--tags", "https://github.com/stardelt/stardelt",
         f"refs/tags/python/{tag}"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return None, (proc.stderr.strip() or "git ls-remote failed").splitlines()[0]
    line = proc.stdout.strip()
    if not line:
        return None, f"tag {tag} not found"
    return line.split()[0], ""


def resolve_digest(app: str, tag: str = "dev") -> tuple[str | None, str]:
    env = dict(os.environ)
    env["PRINT_DIGEST"] = "1"
    env["APP"] = app
    env["TAG"] = tag
    proc = subprocess.run(
        ["bash", str(PIN_IMAGE)],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        err = (proc.stderr.strip() or proc.stdout.strip() or "unknown error").splitlines()[0]
        return None, err
    digest = proc.stdout.strip()
    if not DIGEST_RE.fullmatch(digest):
        return None, f"unexpected output: {digest!r}"
    return digest, ""


def pin_text(repo: Path, manifest: str) -> tuple[str, list[str]]:
    """The committed text of `manifest`, and a note when the working tree differs.

    A pin belongs to a commit. A rollout leaves the checkout dirty, and a dirty file made
    this script print a row about a digest `main` does not carry (2026-11-15: the operator
    in the tree, the Nova pin now). Read `origin/main` when it resolves, else `HEAD`, else
    the file — and say when the working tree is not what was judged.
    """
    path = repo / manifest
    notes: list[str] = []
    rev = ""
    for candidate in ("origin/main", "HEAD"):
        proc = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--verify", "--quiet", candidate],
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            rev = candidate
            break
    if not rev:
        notes.append(f"! {manifest}: no git revision to read, judging the working tree")
        return path.read_text(), notes
    proc = subprocess.run(
        ["git", "-C", str(repo), "show", f"{rev}:{manifest}"], capture_output=True, text=True
    )
    if proc.returncode != 0:
        notes.append(f"! {manifest}: not in {rev}, judging the working tree")
        return path.read_text(), notes
    if rev == "HEAD":
        notes.append(f"! {manifest}: origin/main does not resolve here, judging HEAD")
    try:
        on_disk = path.read_text()
    except OSError:
        on_disk = proc.stdout
    if on_disk != proc.stdout:
        notes.append(f"! {manifest} differs in the working tree; judging {rev}")
    return proc.stdout, notes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--allow-offline",
        action="store_true",
        help="treat an unreachable registry as a note, not a failure",
    )
    args = parser.parse_args(argv)

    lines: list[str] = []
    notes: list[str] = []
    offline_failed = False
    for app, manifest, kind, tag in APPS:
        text, file_notes = pin_text(REPO, manifest)
        notes.extend(file_notes)
        pinned = parse_pins(text, kind)
        resolved, err = resolve_digest(app, tag)
        if resolved is None:
            lines.append(f"– {app}: the registry did not answer ({err})")
            if not args.allow_offline:
                offline_failed = True
            continue
        lines.extend(compare(app, pinned, resolved, manifest, tag))

    # The notebook library's pin: the Makefile names the release its commit is, and the
    # release is a tag `python/<tag>` in stardelt/stardelt — ask the remote which commit
    # it is (offline in `make lint`; the network lives here, in the daily check).
    makefile_text, makefile_notes = pin_text(REPO, "Makefile")
    notes.extend(makefile_notes)
    ref_match = LIB_REF_RE.search(makefile_text)
    tag_match = LIB_TAG_RE.search(makefile_text)
    if not ref_match or not tag_match:
        lines.append(
            "✗ the Makefile names no STARDELT_LIB_REF/STARDELT_LIB_TAG pair"
            " — the notebook library's pin is missing"
        )
    else:
        lib_ref, lib_tag = ref_match.group(1), tag_match.group(1)
        tag_commit, err = resolve_tag_commit(lib_tag)
        if tag_commit is None:
            lines.append(f"– notebook library: git ls-remote did not answer ({err})")
            if not args.allow_offline:
                offline_failed = True
        else:
            lines.append(compare_lib_tag(lib_tag, lib_ref, tag_commit))

    for note in dict.fromkeys(notes):
        print(note)
    for line in lines:
        print(line)
    if offline_failed:
        return 1
    return exit_code(lines)


if __name__ == "__main__":
    raise SystemExit(main())
