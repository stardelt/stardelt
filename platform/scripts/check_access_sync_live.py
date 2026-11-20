#!/usr/bin/env python3
"""Fail when the cluster's access-sync script is not the repository's.

The access-sync Pod reads its script from ConfigMap `access-sync-script`, key
`job.py`. Two install paths write it — `make access-sync` and the operator, which
embeds the same file — and nothing compared the live copy with the repository
until now: a merged script change sat in the lab until a person ran
`make access-sync` (stardelt/stardelt#697). See stardelt/stardelt#684.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

CONFIG_MAP = "access-sync-script"
KEY = "job.py"
DEFAULT_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "access-sync-job.py"


def _short(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:12]


def verdict(live: bytes, repo: bytes) -> tuple[int, str]:
    """(exit code, one line) for two copies of the script."""
    if live == repo:
        return 0, (
            f"✓ access-sync script: the cluster runs the repository's script "
            f"(sha256:{_short(live)}…, {len(live)} bytes)"
        )
    return 1, (
        f"✗ the live access-sync script differs from platform/scripts/access-sync-job.py "
        f"(live sha256:{_short(live)}…, {len(live)} bytes; "
        f"repository sha256:{_short(repo)}…, {len(repo)} bytes) — run make access-sync"
    )


def live_script(namespace: str, config_map: str, key: str) -> tuple[int, bytes | None]:
    """The cluster's copy, or (3, None) when it cannot be read."""
    run = subprocess.run(
        ["kubectl", "-n", namespace, "get", "configmap", config_map, "-o", "json"],
        capture_output=True,
        text=True,
    )
    if run.returncode != 0:
        return 3, None
    try:
        data = json.loads(run.stdout).get("data") or {}
    except json.JSONDecodeError:
        return 3, None
    if key not in data:
        return 3, None
    return 0, data[key].encode()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default="stardelt")
    parser.add_argument("--config-map", default=CONFIG_MAP)
    parser.add_argument("--key", default=KEY)
    parser.add_argument("--script", default=DEFAULT_SCRIPT, type=Path)
    args = parser.parse_args(argv)

    code, live = live_script(args.namespace, args.config_map, args.key)
    if live is None:
        print(
            f"? access-sync script: cannot read ConfigMap {args.namespace}/"
            f"{args.config_map} key {args.key} — nothing checked"
        )
        return 3
    code, line = verdict(live, args.script.read_bytes())
    print(line)
    return code


if __name__ == "__main__":
    sys.exit(main())
