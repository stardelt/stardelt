#!/usr/bin/env bash
# The demo's public-database pipeline (#79 point 2): the bundle that reads RNAcentral's
# public PostgreSQL through the `rnacentral` Connection and writes bronze.public_db.
#
#   make demo-public-db YES=1   # the bundle, the connection's usableBy, viewers=[visitors]
#   make demo-public-db         # the plan and the prerequisite checks, exit 1
#
# In order, and idempotently:
#
#   0.  the prerequisites: kubectl, jq, a stardelt CLI that answers and reads this
#       repository's bundle, the ServicePrincipal pipeline-demo ready, and a CLI that is
#       signed in to Nova;
#   1.  the bundle in demos/examples/public-db deployed for the target prod: public-db-load,
#       scheduled 05:00 Europe/Berlin, its notebook task naming the Connection `rnacentral`;
#   2.  viewers=[visitors] on the pipeline (`stardelt bundle deploy` rewrites the whole spec,
#       so it drops viewers — rerun this script after a deploy);
#   3.  one line for the maintainer: how to trigger public-db-load's first run.
#
# The Connection itself is `make demo-connection YES=1`: its `usableBy` names `visitors` and
# `demo`, and the loader refuses any other group (`plan.py`, `_connection_for_pipeline`).
# A run with YES != 1 changes nothing: it prints every step as `would:` and exits 1.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NAMESPACE:-stardelt}"
NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"
GROUP="${GROUP:-visitors}"
YES="${YES:-0}"
BUNDLE=../demos/examples/public-db
NAME=public-db-load

fail() { echo "demo-public-db: $*" >&2; exit 1; }

# 0. The prerequisites. The CLI is the one on PATH, or the venv `make public-demo-check` row
# 0b accepts, so a shell that never activated a venv can still run the deploy.
for tool in kubectl jq; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool not found"
done
CLI="$(command -v stardelt 2>/dev/null || true)"
if [ -z "$CLI" ]; then
  CLI="${STARDELT_CLI_VENV:-$HOME/.local/venvs/stardelt-cli}/bin/stardelt"
fi
[ -x "$CLI" ] || fail "the stardelt CLI is not installed — see make public-demo-check 0b"

# The CLI must be able to read this repository's bundle before anything else runs:
# `bundle validate` is offline and read-only.
valid="$("$CLI" bundle validate "$BUNDLE" 2>&1)" || fail "the CLI at $CLI cannot read this repository's bundle: $(printf '%s' "$valid" | head -n 1) — install it from this repository (python3 -m venv <venv> && <venv>/bin/pip install -e $PWD/../python) and set STARDELT_CLI_VENV=<venv>"

principal="$(kubectl -n "$NAMESPACE" get serviceprincipals.stardelt.io pipeline-demo \
  -o jsonpath='{.status.ready}' 2>/dev/null || true)"
[ "$principal" = true ] \
  || fail "ServicePrincipal pipeline-demo is not ready (${principal:-absent}) — run make demo-access YES=1 first"
echo "› ServicePrincipal pipeline-demo: ready"

# `--host` is a global option of the CLI: it comes before the subcommand.
plan="$("$CLI" --host "$NOVA" bundle plan "$BUNDLE" -t prod 2>&1)" \
  || fail "the stardelt CLI is not signed in to $NOVA ($(printf '%s' "$plan" | tail -1)) — run: stardelt login --host $NOVA"
echo "› stardelt CLI: signed in to $NOVA"

if [ "$YES" = 1 ]; then
  # 1. The bundle: one scheduled pipeline whose notebook task names the Connection.
  "$CLI" --host "$NOVA" bundle deploy "$BUNDLE" -t prod \
    || fail "stardelt bundle deploy $BUNDLE -t prod failed"
  echo "› public-db bundle: deployed ($NAME 05:00 Europe/Berlin, notebook connection rnacentral)"

  # 2. The viewers. `kubectl patch` takes one object name, so patch the pipeline by name.
  kubectl -n "$NAMESPACE" patch pipelines.stardelt.io "$NAME" --type merge \
    -p "{\"spec\":{\"viewers\":[\"$GROUP\"]}}" >/dev/null \
    || fail "cannot add viewers=[$GROUP] to pipeline $NAME"
  echo "› viewers=[$GROUP] on $NAME"

  # 3. What is left for the maintainer: the pipeline's first history entry.
  echo "$NAME's first run: kubectl -n $NAMESPACE exec deploy/airflow-scheduler -c scheduler -- airflow dags trigger stardelt__$NAME"
  echo ""
  echo "demo-public-db: OK — $NAME reads RNAcentral through its Connection into bronze.public_db"
  exit 0
fi

mapfile -t planned < <(printf '%s\n' "$plan" | jq -Rr 'fromjson? | select(.name != null) | .name')
[ "${#planned[@]}" -gt 0 ] || fail "cannot read the plan of $BUNDLE: $(printf '%s' "$plan" | tail -1)"
printf '%s\n' "$plan" | jq -Rr 'fromjson? | select(.name != null) | "› plan: \(.name) \(.action)"'
echo "would: stardelt bundle deploy $BUNDLE -t prod --host $NOVA"
echo "nothing was changed — re-run with YES=1"
exit 1
