#!/usr/bin/env bash
# One fresh-node run, end to end: a throwaway Hetzner server from zero to a verified stardelt
# platform, and deleted again (stardelt/stardelt#613, slice A of #48).
#
#   bash scripts/e2e-fresh-node.sh up     # create, install the operator, apply the platform,
#                                         # verify it, delete it again; the report is printed
#                                         # and written to $RUN_DIR/report.txt
#   bash scripts/e2e-fresh-node.sh down   # delete every stardelt-fresh-* run this checkout
#                                         # started, then list what is left in Hetzner
#
# Environment:
#   FRESH_DOMAIN     the base domain the run points at Nova (the run upserts its A record);
#                    required unless DRY_RUN=1
#   E2E_RUN_ID       the run's name in $HOME/.cache/stardelt-e2e/<id> (default: UTC timestamp)
#   FRESH_NODE_TYPE  the Hetzner server type (default cpx42, 16 GB: the demo profile needs it)
#   KEEP_ON_FAILURE  1 keeps the server after a failed phase for a look (default 0)
#   DRY_RUN          1 prints the plan of `up` and touches nothing
#   YES              1 confirms that this creates a server and bills by the hour
#   NAMESPACE        the platform namespace (default stardelt)
#
# Exit codes: 0 the whole run passed (and the server is gone), 1 a phase failed (the server is
# still deleted), 3 a check could not be made, 2 usage.
#
# FRESH_NODE_SH and FRESH_DIR name the wrapper that creates and deletes the cluster and the
# directory its .fresh-*.yaml files live in. They default to this checkout; they exist so the
# unit test can fake both and never create a server.
set -euo pipefail

PLATFORM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="$(dirname "$PLATFORM_DIR")"
FRESH_NODE_SH="${FRESH_NODE_SH:-$REPO/platform/dev/hetzner-k3s/fresh-node.sh}"
FRESH_DIR="${FRESH_DIR:-$REPO/platform/dev/hetzner-k3s}"

NAMESPACE="${NAMESPACE:-stardelt}"
OPERATOR_RELEASE="stardelt-operator"
OPERATOR_DEPLOY="stardelt-operator"

DRY_RUN="${DRY_RUN:-0}"
KEEP_ON_FAILURE="${KEEP_ON_FAILURE:-0}"
FRESH_NODE_TYPE="${FRESH_NODE_TYPE:-cpx42}"
LOCATION="${LOCATION:-nbg1}"
E2E_RUN_ID="${E2E_RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
# The three paths stay in the run directory: a `down` run finds them from the same name.
FRESH_NAME="stardelt-fresh-e2e-${E2E_RUN_ID}"
RUN_DIR="$HOME/.cache/stardelt-e2e/${E2E_RUN_ID}"
# The commit this run proves (#620). Captured here, before the hour-long run, so a checkout
# that moves during it cannot change what the report claims.
RUN_COMMIT="$(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)"
[ -z "$(git -C "$REPO" status --porcelain 2>/dev/null)" ] || RUN_COMMIT="${RUN_COMMIT}-dirty"
FRESH_CONFIG="$RUN_DIR/fresh.yaml"
FRESH_KUBECONFIG="$RUN_DIR/kubeconfig.yaml"

report_lines=()
FINAL_RC=0
failure_phase=""
unknown_reason=""
diagnostics_done=0

usage() {
  cat <<'EOF'
usage: e2e-fresh-node.sh [up|down]

  up    create one throwaway Hetzner server, install the operator, apply
        operator/examples/platform-minimal.yaml, verify it, delete it again (default)
  down  delete every stardelt-fresh-* run this checkout started, then list what
        is left in Hetzner
EOF
}

# preflight runs before anything that costs money: one sentence per missing thing.
preflight() {
  local missing=0 tool
  if [ "$DRY_RUN" != 1 ] && [ "${YES:-0}" != 1 ]; then
    echo "e2e-fresh-node creates a Hetzner server: re-run with YES=1" >&2
    missing=1
  fi
  if [ -z "${FRESH_DOMAIN:-}" ] && [ "$DRY_RUN" != 1 ]; then
    echo "set FRESH_DOMAIN to a domain whose A record the run may create (cloudflare-dns.sh upserts it)" >&2
    missing=1
  fi
  if [ "$DRY_RUN" != 1 ]; then
    for tool in hetzner-k3s hcloud helm kubectl; do
      if ! command -v "$tool" >/dev/null 2>&1; then
        echo "no ${tool} on PATH" >&2
        missing=1
      fi
    done
  fi
  if [ "$missing" != 0 ]; then
    exit 1
  fi
  if [ "$DRY_RUN" = 1 ]; then
    echo "would create ${FRESH_NAME} (${FRESH_NODE_TYPE} @ ${LOCATION}), install the operator, apply platform-minimal.yaml for ${FRESH_DOMAIN:-<domain>}, verify, delete"
    exit 0
  fi
}

# collect_diagnostics keeps what a person needs after the first failed phase. It reads no
# Secret, and every call is `|| true`: diagnostics never replace the real failure.
collect_diagnostics() {
  if [ "$diagnostics_done" = 1 ]; then
    return 0
  fi
  diagnostics_done=1
  kubectl -n "$NAMESPACE" get pods -o wide >"$RUN_DIR/pods.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp >"$RUN_DIR/events.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" get platforminstance,lakehouse -o yaml >"$RUN_DIR/cr.txt" 2>&1 || true
  {
    kubectl -n "$NAMESPACE" logs "deploy/${OPERATOR_DEPLOY}" --tail=500 --all-containers 2>&1 || true
    kubectl -n "$NAMESPACE" logs "deploy/${OPERATOR_DEPLOY}" --tail=500 --all-containers --previous 2>&1 || true
  } >"$RUN_DIR/operator.log"
  helm -n "$NAMESPACE" history "$OPERATOR_RELEASE" >"$RUN_DIR/helm.txt" 2>&1 || true
}

# run_phase <name> <logfile> -- <command...>: one line in the report per phase, the command's
# own output in the log and on stdout. The log is appended to (the two platform-instance
# phases share one file). The exit code is returned, so `run_phase … || phase_rc=$?` keeps
# `set -e` from aborting before the report and the teardown.
run_phase() {
  local name="$1" log="$2" started rc=0
  shift 2
  if [ "${1:-}" = "--" ]; then
    shift
  fi
  started=$SECONDS
  "$@" 2>&1 | tee -a "$log" || rc=$?
  if [ "$rc" -eq 0 ]; then
    report_lines+=("✓ ${name} $((SECONDS - started))s")
  else
    report_lines+=("✗ ${name} $((SECONDS - started))s")
    if [ -z "$failure_phase" ]; then
      failure_phase="$name"
      collect_diagnostics
    fi
    if [ "$FINAL_RC" -eq 0 ]; then
      FINAL_RC="$rc"
    fi
    if [ "$rc" -eq 3 ]; then
      unknown_reason="the ${name} phase could not check the platform"
    fi
  fi
  return "$rc"
}

wait_for_resources() {
  kubectl -n "$NAMESPACE" wait --for=condition=Ready platforminstance/cluster --timeout=25m || return 1
  kubectl -n "$NAMESPACE" wait --for=condition=Ready lakehouse/main --timeout=25m || return 1
}

phase_up() {
  run_phase "fresh node" "$RUN_DIR/01-fresh-node.log" -- \
    env FRESH_NAME="$FRESH_NAME" FRESH_CONFIG="$FRESH_CONFIG" FRESH_KUBECONFIG="$FRESH_KUBECONFIG" \
      FRESH_DOMAIN="${FRESH_DOMAIN:-}" FRESH_NODE_TYPE="$FRESH_NODE_TYPE" \
      bash "$FRESH_NODE_SH" up || return 1

  # Everything after this point talks to the throwaway cluster, never to the current context.
  export KUBECONFIG="$FRESH_KUBECONFIG"

  run_phase "operator" "$RUN_DIR/02-operator.log" -- \
    make -C "$PLATFORM_DIR" install-operator || return 1

  sed -e "s/data\.example\.com/${FRESH_DOMAIN}/g" \
    "$REPO/operator/examples/platform-minimal.yaml" >"$RUN_DIR/platform-instance.yaml"
  run_phase "platform instance" "$RUN_DIR/03-platform-instance.log" -- \
    kubectl apply -f "$RUN_DIR/platform-instance.yaml" || return 1
  run_phase "platform ready" "$RUN_DIR/03-platform-instance.log" -- \
    wait_for_resources || return 1
  run_phase "verify" "$RUN_DIR/04-verify.log" -- \
    bash "$PLATFORM_DIR/scripts/verify-fresh-node.sh" || return 1
  return 0
}

# phase_down runs from the EXIT trap, so it also runs when a phase failed or a person pressed
# Ctrl-C. A failed teardown is a FAIL: the server bills until somebody deletes it.
phase_down() {
  local started deleted rc=0
  if [ "$KEEP_ON_FAILURE" = 1 ]; then
    report_lines+=("· teardown skipped (KEEP_ON_FAILURE=1)")
    echo "kept ${FRESH_NAME} (KEEP_ON_FAILURE=1) — delete it with: FRESH_NAME=${FRESH_NAME} bash dev/hetzner-k3s/fresh-node.sh down"
    return 0
  fi
  started=$SECONDS
  if FRESH_NAME="$FRESH_NAME" FRESH_CONFIG="$FRESH_CONFIG" FRESH_KUBECONFIG="$FRESH_KUBECONFIG" \
    bash "$FRESH_NODE_SH" down 2>&1 | tee -a "$RUN_DIR/99-teardown.log"; then
    report_lines+=("✓ teardown $((SECONDS - started))s")
  else
    rc=1
    report_lines+=("✗ teardown $((SECONDS - started))s")
    if [ -z "$failure_phase" ]; then
      failure_phase="teardown"
    fi
  fi
  # `down` removes the config on success; the name it describes is the run's own name anyway.
  deleted="$FRESH_NAME"
  if [ -f "$FRESH_CONFIG" ]; then
    deleted="$(awk -F': *' '/^cluster_name:/ {print $2; exit}' "$FRESH_CONFIG" || true)"
    if [ -z "$deleted" ]; then
      deleted="$FRESH_NAME"
    fi
  fi
  if [ "$rc" -ne 0 ]; then
    FINAL_RC=1
    echo "the server was NOT deleted — run: FRESH_NAME=${deleted} bash dev/hetzner-k3s/fresh-node.sh down" >&2
  else
    echo "deleted ${deleted}"
  fi
  return 0
}

report() {
  local final out="" line
  case "$FINAL_RC" in
    0) final="e2e-fresh-node: PASS (one server, created and deleted in ${SECONDS}s)" ;;
    3) final="e2e-fresh-node: UNVERIFIED (${unknown_reason:-a check could not be made})" ;;
    *) final="e2e-fresh-node: FAIL (see ${failure_phase:-the phases above} and ${RUN_DIR})" ;;
  esac
  if [ "${#report_lines[@]}" -gt 0 ]; then
    for line in "${report_lines[@]}"; do
      out+="${line}"$'\n'
    done
  fi
  out+="commit: ${RUN_COMMIT}"$'\n'
  out+="node type: ${FRESH_NODE_TYPE} @ ${LOCATION}"$'\n'
  out+="base domain: ${FRESH_DOMAIN:-<none>}"$'\n'
  out+="wall time: ${SECONDS}s"$'\n'
  out+="logs: ${RUN_DIR}"$'\n'
  out+="${final}"$'\n'
  printf '%s' "$out"
  printf '%s' "$out" >"$RUN_DIR/report.txt"
}

on_exit() {
  phase_down
  report
  exit "$FINAL_RC"
}

cmd_up() {
  preflight
  mkdir -p "$RUN_DIR"
  echo "› run ${E2E_RUN_ID}: ${FRESH_NAME}, logs in ${RUN_DIR}"
  trap on_exit EXIT
  phase_up || true
  exit "$FINAL_RC"
}

# The `down` path: what this checkout started first, then what Hetzner still lists.
cmd_down() {
  local -a configs=() left=()
  local cfg name listing first rest rc=0 server
  if [ -d "$FRESH_DIR" ]; then
    mapfile -t configs < <(find "$FRESH_DIR" -maxdepth 1 -type f -name '.fresh-*.yaml' | sort)
  fi
  if [ "${#configs[@]}" -gt 0 ]; then
    for cfg in "${configs[@]}"; do
      name="$(awk -F': *' '/^cluster_name:/ {print $2; exit}' "$cfg" || true)"
      if [ -z "$name" ]; then
        echo "skipping ${cfg}: no cluster_name" >&2
        rc=1
        continue
      fi
      echo "› deleting ${name} (${cfg})"
      if ! FRESH_NAME="$name" FRESH_CONFIG="$cfg" bash "$FRESH_NODE_SH" down; then
        echo "✗ ${name} was not deleted" >&2
        rc=1
        continue
      fi
      rm -f "$cfg"
    done
  fi

  if ! command -v hcloud >/dev/null 2>&1; then
    echo "hcloud not installed: cannot list servers" >&2
    if [ "$rc" -eq 0 ]; then
      return 3
    fi
    return "$rc"
  fi
  if ! listing="$(hcloud server list 2>&1)"; then
    echo "hcloud server list failed: ${listing}" >&2
    if [ "$rc" -eq 0 ]; then
      return 3
    fi
    return "$rc"
  fi
  # `hcloud server list` prints a header line, then `NAME address …`.
  while read -r first rest; do
    [ -n "$first" ] || continue
    case "$first" in
      stardelt-fresh-*) left+=("$first") ;;
    esac
  done <<<"$listing"
  if [ "${#left[@]}" -gt 0 ]; then
    for server in "${left[@]}"; do
      echo "  ${server}"
      echo "  hcloud server delete ${server}"
    done
    echo "these servers are left — delete them in the Hetzner console" >&2
    rc=1
  else
    echo "no stardelt-fresh-* server is left"
  fi
  return "$rc"
}

main() {
  local phase="${1:-up}"
  case "$phase" in
    up) cmd_up ;;
    down) cmd_down || exit $? ;;
    -h | --help | help)
      usage
      exit 0
      ;;
    *)
      echo "e2e-fresh-node: unknown phase '${phase}' (up, down)" >&2
      usage >&2
      exit 2
      ;;
  esac
}

main "$@"
