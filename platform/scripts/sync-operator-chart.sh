#!/usr/bin/env bash
# Render the operator's Helm chart at an explicit image digest.
#
#   bash scripts/sync-operator-chart.sh sha256:<64 hex> [namespace]
#
# Why this exists: `scripts/pin-image.sh stardelt-operator` moves the image and the CRDs, but the
# chart is what renders the operator's ClusterRole, its NetworkPolicies and its values, and only
# `helm upgrade --install` applies them (`make install-operator`, Makefile:1293-1307). Without
# this, a chart-only fix is merged, pinned, rolled out and dark: operator#781's
# `deletecollection` rule stayed off the live ClusterRole until a manual `helm upgrade`
# (revision 18, platform#1491).
#
# A missing `helm` or operator checkout is a printed note and exit 0 (this script also runs on
# machines without the operator repo, and pin-image.sh must keep working there); a `helm`
# failure is this script's failure.
# It also names the chart's revision, so a pin never renders a stale chart silently.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

digest="${1:-}"
NS="${2:-${NS:-stardelt}}"

if [ "${#digest}" -ne 71 ] || [ "${digest#sha256:}" = "$digest" ]; then
  echo "usage: $(basename "$0") sha256:<64 hex> [namespace]" >&2
  exit 2
fi

op_dir="${OPERATOR_DIR:-../stardelt-operator}"
if [ ! -f "$op_dir/chart/Chart.yaml" ]; then
  echo "› no operator chart at $op_dir/chart — the chart was not rendered (set OPERATOR_DIR)"
  exit 0
fi
# The chart is whatever this checkout holds, and a shared checkout can be far behind
# (2026-11-15: 96 commits, 351 lines of chart). Name the revision it renders; a pin that
# moves the image forward must not render a chart backward (platform#1491's mirror).
chart_rev="$(git -C "$op_dir" rev-parse --short=7 HEAD 2>/dev/null || true)"
if [ -n "$chart_rev" ]; then
  echo "› chart from $op_dir at $chart_rev"
  chart_main="$(git -C "$op_dir" rev-parse --short=7 origin/main 2>/dev/null || true)"
  if [ -n "$chart_main" ] && [ "$chart_rev" != "$chart_main" ]; then
    behind="$(git -C "$op_dir" rev-list --count "HEAD..origin/main" 2>/dev/null || true)"
    if [ -n "$behind" ] && [ "$behind" -gt 0 ]; then
      echo "! $op_dir is $behind commit(s) behind origin/main ($chart_rev vs $chart_main) — this is not the pinned build's chart (set OPERATOR_DIR, or: git -C $op_dir pull --ff-only)" >&2
    else
      echo "! $op_dir differs from origin/main ($chart_rev vs $chart_main) — this is not the pinned build's chart (set OPERATOR_DIR, or: check out the chart's origin/main)" >&2
    fi
  fi
fi
if ! command -v helm >/dev/null 2>&1; then
  echo "› helm not found — the operator chart was not rendered"
  exit 0
fi

# -f first so the chart's own values win over helm-values/operator.yaml only where the pin is
# concerned: --set-string states the digest this run resolved, so `WRITE_MANIFEST=0` (the fast
# dev loop) cannot render the tag the file still carries.
helm upgrade --install stardelt-operator "$op_dir/chart" \
  --namespace "$NS" \
  -f helm-values/operator.yaml \
  --set-string "image.tag=dev@$digest" \
  --set 'imagePullSecrets[0].name=ghcr-creds' \
  --wait --timeout "${HELM_TIMEOUT:-5m}"
echo "› chart stardelt-operator rendered at dev@${digest:0:19}…"
