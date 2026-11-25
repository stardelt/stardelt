#!/usr/bin/env bash
# Fail when README.md's CRD table and the kinds declared in the Rust sources disagree.
# operator#347: the table had lost four served kinds and called StreamApp "planned".
# The kinds live in crd.rs and, since #1628 slice 1, in the shared crate crates/stardelt-crds.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

python3 - <<'PY'
import glob
import re
import sys

sources = ["crates/stardelt-operator/src/crd.rs"] + sorted(
    glob.glob("crates/stardelt-crds/src/**/*.rs", recursive=True)
)
kinds = sorted(set(re.findall(r'kind = "([A-Za-z]+)"', "".join(open(f).read() for f in sources))))

text = open("README.md").read()
if "## CRDs" not in text:
    sys.exit("README.md has no '## CRDs' section")
section = text.split("## CRDs", 1)[1].split("###", 1)[0]
rows = sorted(set(re.findall(r"^\| `([A-Za-z]+)` \|", section, re.M)))

missing = [k for k in kinds if k not in rows]
extra = [r for r in rows if r not in kinds]
if missing or extra:
    for k in missing:
        print(f"README.md's CRD table is missing `{k}` (declared in the Rust sources)")
    for r in extra:
        print(f"README.md's CRD table lists `{r}`, which is not a CRD kind in the Rust sources")
    sys.exit(1)

print(f"✓ README lists all {len(kinds)} CRD kinds")
PY
