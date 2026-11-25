#!/usr/bin/env bash
# The demo's stream (#79 point 3): a StreamApp group `demo` owns and `visitors` may read,
# so the public Streams page shows a running app.
#
#   make demo-stream YES=1   # the bundle, one demo-stream-setup run, the StreamApp
#   make demo-stream         # the plan and the prerequisite checks, exit 1
#   bash scripts/demo-stream.sh --check   # read-only: is the StreamApp there and Running
#
# In order, and idempotently:
#
#   0.  the prerequisites: kubectl, jq, a stardelt CLI that answers and reads this
#       repository's bundle, and the ServicePrincipal pipeline-demo ready;
#   1.  the bundle in demos/examples/demo-stream deployed for the target prod: the
#       on-demand pipeline demo-stream-setup, which creates the sink table;
#   2.  one run of demo-stream-setup, because a StreamApp reads its sink's schema
#       (`stream_job.py:89`) and never creates the table;
#   3.  the StreamApp demo-pegelonline, and a wait until the operator marks it Running.
#
# The stream reads the topic the hourly `pegelonline_hourly` DAG fills and merges into
# gold.shop_dbt.pegelonline. Needs `make demo-access YES=1` and `make demo-dbt-grant YES=1`.
# A run with YES != 1 changes nothing: it prints the plan and exits 1.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NAMESPACE:-stardelt}"
NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"
GROUP="${GROUP:-visitors}"
YES="${YES:-0}"
BUNDLE=../demos/examples/demo-stream
NAME=demo-stream-setup
STREAM=demo-pegelonline

fail() { echo "demo-stream: $*" >&2; exit 1; }

if [ "${1:-}" = --check ]; then
  document="$(kubectl -n "$NAMESPACE" get streamapps.stardelt.io "$STREAM" -o json 2>/dev/null || true)"
  if [ -z "$document" ]; then
    echo "MISSING  $STREAM does not exist — make demo-stream YES=1"
    exit 1
  fi
  owner="$(jq -r '.spec.ownerGroup // ""' <<<"$document")"
  viewers="$(jq -r --arg g "$GROUP" '(.spec.viewers // []) | index($g) != null' <<<"$document")"
  phase="$(jq -r '.status.phase // "unknown"' <<<"$document")"
  if [ "$owner" = demo ] && [ "$viewers" = true ] && [ "$phase" = Running ]; then
    echo "PASS  $STREAM is owned by demo, visible to $GROUP and Running"
    exit 0
  fi
  echo "MISSING  $STREAM owner='${owner:-none}' visible to $GROUP: ${viewers:-false} phase=${phase} (want demo, true, Running) — make demo-stream YES=1"
  exit 1
fi

# 0. The prerequisites. The CLI is the one on PATH, or the venv the script names below.
for tool in kubectl jq; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool not found"
done
CLI="$(command -v stardelt 2>/dev/null || true)"
if [ -z "$CLI" ]; then
  CLI="${STARDELT_CLI_VENV:-$HOME/.local/venvs/stardelt-cli}/bin/stardelt"
fi
[ -x "$CLI" ] || fail "the stardelt CLI is not installed — install the CLI from this repository: python3 -m venv <venv> && <venv>/bin/pip install -e $PWD/../python, then set STARDELT_CLI_VENV=<venv>"

# The CLI must be able to read this repository's bundle before anything else runs:
# `bundle validate` is offline and read-only.
valid="$("$CLI" bundle validate "$BUNDLE" 2>&1)" || fail "the CLI at $CLI cannot read this repository's bundle: $(printf '%s' "$valid" | head -n 1) — install the CLI from this repository: python3 -m venv <venv> && <venv>/bin/pip install -e $PWD/../python, then set STARDELT_CLI_VENV=<venv>"

principal="$(kubectl -n "$NAMESPACE" get serviceprincipals.stardelt.io pipeline-demo \
  -o jsonpath='{.status.ready}' 2>/dev/null || true)"
[ "$principal" = true ] \
  || fail "ServicePrincipal pipeline-demo is not ready (${principal:-absent}) — run make demo-access YES=1 first"
echo "› ServicePrincipal pipeline-demo: ready"

if [ "$YES" != 1 ]; then
  echo "would: deploy demos/examples/demo-stream to prod, run demo-stream-setup once, apply StreamApp demo-pegelonline (owner demo, viewers [visitors], sink gold.shop_dbt.pegelonline)"
  echo "nothing was changed — re-run with YES=1"
  exit 1
fi

# 1. The bundle: the on-demand pipeline that creates the sink table.
"$CLI" --host "$NOVA" bundle deploy "$BUNDLE" -t prod \
  || fail "the stardelt CLI is not signed in to $NOVA, or the deploy failed — run: stardelt --host $NOVA login"
echo "› demo-stream bundle: deployed ($NAME, on demand)"

# 2. The sink. Without the table the stream's driver cannot read the sink's schema.
"$CLI" --host "$NOVA" run "$NAMESPACE" "$NAME" --wait --timeout 900 \
  || fail "$NAME did not succeed — its run above names the failing task"
echo "› $NAME: succeeded"

# 3. The StreamApp, and the operator marks it Running once the driver is up.
kubectl apply -f "$BUNDLE/streamapp.yaml" >/dev/null || fail "cannot apply $BUNDLE/streamapp.yaml"
echo "› StreamApp $STREAM: applied"

for _ in $(seq 1 60); do
  phase="$(kubectl -n "$NAMESPACE" get streamapps.stardelt.io "$STREAM" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  [ "$phase" = Running ] && break
  sleep 5
done
if [ "$phase" != Running ]; then
  message="$(kubectl -n "$NAMESPACE" get streamapps.stardelt.io "$STREAM" -o json \
    | jq -r '.status.lastError // .status.conditions[-1].message // "no message on the object"')"
  fail "the stream did not reach Running (${phase:-absent}: $message) — check the operator log for the driver's message"
fi
echo "› StreamApp $STREAM: Running, sink gold.shop_dbt.pegelonline"
