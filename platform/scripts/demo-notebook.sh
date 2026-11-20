#!/usr/bin/env bash
# The demo's report notebook (#79 point 6): one published notebook of group `demo` that the
# visitors may read, so the Notebooks page is not empty. The file is
# `demos/examples/shop/notebooks/shop-report.ipynb`; publishing the same bytes twice stores
# nothing (Nova answers `unchanged: true`), so a rerun changes nothing.
#
#   make demo-notebook YES=1   # publish it with viewers=[visitors]
#   make demo-notebook         # check the prerequisites, print the plan, exit 1
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"
GROUP="${GROUP:-demo}"
VIEWERS="${VIEWERS:-visitors}"
YES="${YES:-0}"
NOTEBOOK=../demos/examples/shop/notebooks/shop-report.ipynb

fail() { echo "demo-notebook: $*" >&2; exit 1; }

[ -f "$NOTEBOOK" ] || fail "$NOTEBOOK is missing"

CLI="$(command -v stardelt 2>/dev/null || true)"
if [ -z "$CLI" ]; then
  CLI="${STARDELT_CLI_VENV:-$HOME/.local/venvs/stardelt-cli}/bin/stardelt"
fi
[ -x "$CLI" ] || fail "the stardelt CLI is not installed — see make public-demo-check 0b"
"$CLI" notebook publish --help 2>&1 | grep -q -- "--viewers" \
  || fail "the CLI at $CLI predates --viewers — install it from this repository (python3 -m venv <venv> && <venv>/bin/pip install -e $PWD/../python) and set STARDELT_CLI_VENV=<venv>"

if [ "$YES" = 1 ]; then
  "$CLI" --host "$NOVA" notebook publish "$NOTEBOOK" --group "$GROUP" --viewers "$VIEWERS" \
    || fail "publishing $NOTEBOOK as $GROUP failed"
  echo "› $GROUP/shop-report.ipynb: published, readable by $VIEWERS"
  echo "verify as the visitor: make verify-demo-visitor   (the notebooks row)"
  exit 0
fi

echo "would: $CLI --host $NOVA notebook publish $NOTEBOOK --group $GROUP --viewers $VIEWERS"
echo "nothing was changed — re-run with YES=1"
exit 1
