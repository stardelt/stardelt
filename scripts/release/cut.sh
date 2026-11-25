#!/usr/bin/env bash
# Phase C of a release (stardelt/stardelt#2): prove the checked-out commit is releasable and
# generate releases/v<version>/ from its pins. Changes nothing but that folder.
#   bash scripts/release/cut.sh 0.0.N [commit]            # commit defaults to HEAD
#   NO_STAMP_CHECK=1 bash scripts/release/cut.sh 0.0.N    # only to test this script before a stamp
#   NO_FRESH_NODE_CHECK=1 bash scripts/release/cut.sh 0.0.N   # only to test this script
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

version="${1:?usage: cut.sh 0.0.N [commit]}"
commit="${2:-$(git rev-parse HEAD)}"
[[ "$version" =~ ^0\.0\.[0-9]+$ ]] || { echo "✗ version '$version' is not 0.0.N"; exit 2; }
[[ "$(git rev-parse HEAD)" == "$(git rev-parse "$commit^{commit}")" ]] \
  || { echo "✗ check out $commit first (HEAD is $(git rev-parse --short HEAD))"; exit 2; }

echo "› every folder says $version"
if [[ "${NO_STAMP_CHECK:-0}" != 1 ]]; then
  python3 scripts/release/stamp-version.py --check "$version"
fi

echo "› every pin equals what its tag resolves to; the operator's goldens agree"
(cd platform && python3 scripts/check_pin_freshness.py --rev "$(git rev-parse HEAD)")
(cd platform && OPERATOR_DIR=../operator bash scripts/check-operator-pins.sh)
(cd platform && OPERATOR_DIR=../operator python3 scripts/check_golden_image.py)
(cd operator && bash scripts/check-cli-image.sh)

echo "› a fresh node passed at this commit (#2)"
if [[ "${NO_FRESH_NODE_CHECK:-0}" != 1 ]]; then
  python3 scripts/release/check_fresh_node.py "$(git rev-parse HEAD)"
fi

echo "› generate releases/v$version/"
python3 scripts/release/generate.py "$version" --commit "$(git rev-parse HEAD)"

echo "› Nova's architecture map at this commit (#1619)"
# The generated page reads the hand-written request flow from its output directory, so copy it
# there first; `arch_map.py --out` writes no copy of it.
mkdir -p "releases/v$version/architecture"
cp nova/docs/architecture/request-flow.mmd "releases/v$version/architecture/"
(cd nova && python3 scripts/arch_map.py --out "../releases/v$version/architecture")

echo "✓ v$version is releasable at $(git rev-parse HEAD)"
