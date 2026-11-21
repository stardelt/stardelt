#!/usr/bin/env bash
# The demo's shop pipelines in one repeatable command (#335, #79 slice 2b).
#
#   make demo-shop            # print the plan and exit 1
#   make demo-shop YES=1      # converge the shop: the bundle, the dbt run, viewers
#
# The demo's Pipelines page used to be twelve one-statement pipelines, all On demand: no
# schedule, no next run, a stale history. The bundle in demos/examples/shop-bundle is the
# demo, and this script is the redeploy of it — the sequence a person ran by hand, written
# once and checkable (`make public-demo-check`, rows 18 and 19).
#
# In order:
#
#   0.  the prerequisites: kubectl, jq, openssl, a stardelt CLI that answers and reads this
#       repository's bundle, the ServicePrincipal pipeline-demo ready, and a CLI that is
#       signed in to Nova;
#   0b. the destination demo/run-feed (an ntfy webhook on a random topic,
#       viewers=[visitors]), created once — before the bundle, because Nova refuses a
#       pipeline whose notify names a destination its group does not have;
#   1.  the shop bundle deployed for the target prod: shop-1-bronze, shop-2-silver and
#       shop-3-gold, scheduled 02:00 / 02:30 / 03:00 Europe/Berlin (its pipeline files name
#       `viewers: [visitors]`, so the deploy writes them);
#   2.  the twelve one-statement legacy pipelines deleted — an explicit list, never a
#       pattern, and never one that carries .spec.schedule;
#   3.  the dbt run applied: shop-dbt, 04:00 Europe/Berlin;
#   4.  one line for the maintainer: how to trigger shop-dbt's first run.
#
# A run with YES != 1 changes nothing: it prints every step as `would:` and exits 1.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NAMESPACE:-stardelt}"
YES="${YES:-0}"
NOVA="${STARDELT_NOVA_URL:-https://alpha.dev.stardelt.io}"
GROUP=visitors
BUNDLE=../demos/examples/shop-bundle
DBT=../demos/examples/shop/pipeline-dbt.yaml
LEGACY=(
  shop-01-bronze shop-02-silver shop-03-gold shop-04-customers shop-05-products shop-06-lines
  shop-07-stg-cust shop-08-stg-prod shop-09-stg-lines shop-10-revenue shop-11-ltv shop-12-mix
)

fail() { echo "demo-shop: $*" >&2; exit 1; }

# 0. The prerequisites. The CLI is the one on PATH, or the venv `make public-demo-check` row
# 0b accepts, so a shell that never activated a venv can still run the redeploy.
for tool in kubectl jq openssl; do
  command -v "$tool" >/dev/null 2>&1 || fail "$tool not found"
done
CLI="$(command -v stardelt 2>/dev/null || true)"
if [ -z "$CLI" ]; then
  CLI="${STARDELT_CLI_VENV:-$HOME/.local/venvs/stardelt-cli}/bin/stardelt"
fi
[ -x "$CLI" ] || fail "the stardelt CLI is not installed — see make public-demo-check 0b"

# The CLI must be able to read this repository's bundle before anything else runs: one from
# an older release refuses it (`description:` on every file, the `dbt` task) and names a
# bundle file instead of itself. `bundle validate` is offline and read-only.
valid="$("$CLI" bundle validate "$BUNDLE" 2>&1)" || fail "the CLI at $CLI cannot read this repository's bundle: $(printf '%s' "$valid" | head -n 1) — install it from this repository (python3 -m venv <venv> && <venv>/bin/pip install -e $PWD/../python) and set STARDELT_CLI_VENV=<venv>"

principal="$(kubectl -n "$NAMESPACE" get serviceprincipals.stardelt.io pipeline-demo \
  -o jsonpath='{.status.ready}' 2>/dev/null || true)"
[ "$principal" = true ] \
  || fail "ServicePrincipal pipeline-demo is not ready (${principal:-absent}) — run make demo-access YES=1 first"
echo "› ServicePrincipal pipeline-demo: ready"

# 0b. The run feed (#79): an ntfy webhook on a random topic, made once and then kept.
feed="$("$CLI" --host "$NOVA" api destinations_list 2>/dev/null \
  | jq -r '.[]? | select(.owner_group == "demo" and .name == "run-feed") | .config.url' || true)"
if [ -n "$feed" ]; then
  echo "› run-feed: exists (${feed%%/stardelt-demo-*}/stardelt-demo-…)"
elif [ "$YES" = 1 ]; then
  topic="stardelt-demo-$(openssl rand -hex 8)"
  body="$(jq -n --arg url "https://ntfy.sh/$topic" --arg g "$GROUP" \
    '{owner_group: "demo", name: "run-feed", kind: "webhook", config: {url: $url}, viewers: [$g]}')"
  "$CLI" --host "$NOVA" api destinations_create --body "$body" >/dev/null \
    || fail "cannot create the destination demo/run-feed"
  echo "› run-feed: created — subscribe at https://ntfy.sh/$topic"
else
  echo "would: create the destination demo/run-feed (webhook https://ntfy.sh/stardelt-demo-<random>, viewers=[visitors])"
fi

# `--host` is a global option of the CLI: it comes before the subcommand.
plan="$("$CLI" --host "$NOVA" bundle plan "$BUNDLE" -t prod 2>&1)" \
  || { case "$plan" in
         *"sign-in"*|*"not signed in"*|*"401"*) fail "the stardelt CLI is not signed in to $NOVA — run: stardelt login --host $NOVA" ;;
         *) fail "stardelt bundle plan $BUNDLE -t prod failed: $(printf '%s' "$plan" | tail -1)" ;;
       esac; }
echo "› stardelt CLI: signed in to $NOVA; the plan reads"

# 1. The bundle: three scheduled pipelines instead of twelve On demand ones.
planned=()
if [ "$YES" = 1 ]; then
  "$CLI" --host "$NOVA" bundle deploy "$BUNDLE" -t prod \
    || fail "stardelt bundle deploy $BUNDLE -t prod failed"
  echo "› shop bundle: deployed (shop-1-bronze 02:00, shop-2-silver 02:30, shop-3-gold 03:00 Europe/Berlin)"
else
  echo "would: stardelt bundle deploy $BUNDLE -t prod --host $NOVA"
  mapfile -t planned < <(printf '%s\n' "$plan" | jq -Rr 'fromjson? | select(.name != null) | .name')
  [ "${#planned[@]}" -gt 0 ] || fail "cannot read the plan of $BUNDLE: $(printf '%s' "$plan" | tail -1)"
  printf '%s\n' "$plan" | jq -Rr 'fromjson? | select(.name != null) | "› plan: \(.name) \(.action)"'
fi

# 2. The twelve legacy pipelines. An explicit list, so a name this script does not know is
# never deleted; a scheduled pipeline is never deleted, by anyone's script.
for name in "${LEGACY[@]}"; do
  document="$(kubectl -n "$NAMESPACE" get pipelines.stardelt.io "$name" -o json 2>/dev/null || true)"
  if [ -z "$document" ]; then
    echo "› $name: absent"
    continue
  fi
  if [ "$(jq -r '.metadata.labels["stardelt.io/bundle"] // ""' <<<"$document")" != shop ]; then
    echo "› $name: not the demo's (no stardelt.io/bundle=shop)"
    continue
  fi
  if jq -e '.spec.schedule != null' <<<"$document" >/dev/null 2>&1; then
    fail "$name carries .spec.schedule ($(jq -r '.spec.schedule.cron // "?"' <<<"$document")) — this script never deletes a scheduled pipeline"
  fi
  if [ "$YES" = 1 ]; then
    kubectl -n "$NAMESPACE" delete pipelines.stardelt.io "$name" >/dev/null \
      || fail "cannot delete pipeline $name"
    echo "› $name: deleted (the bundle's shop-1-bronze replaces it)"
  else
    echo "would: delete $name"
  fi
done

# 3. The dbt run: one task on the demo's dbt project, in the bundle's own image.
if [ "$YES" = 1 ]; then
  kubectl -n "$NAMESPACE" apply -f "$DBT" >/dev/null || fail "cannot apply $DBT"
  echo "› shop-dbt: applied (04:00 Europe/Berlin)"
else
  echo "would: apply $DBT (shop-dbt, 04:00 Europe/Berlin)"
fi

# 4. What is left for the maintainer: the dbt run's first history entry.
echo "shop-dbt's first run: kubectl -n $NAMESPACE exec deploy/airflow-scheduler -- airflow dags trigger stardelt__shop-dbt"

if [ "$YES" != 1 ]; then
  echo "demo-shop: nothing was changed — re-run with YES=1"
  exit 1
fi

echo ""
echo "demo-shop: OK — the shop is the bundle's three, the dbt run and no legacy pipelines"
