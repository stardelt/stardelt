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
#   FRESH_BASE_DOMAIN  the domain a run derives its own name from: fresh-<E2E_RUN_ID>.<base>
#                    (e.g. dev.stardelt.io); FRESH_DOMAIN, when set, wins
#   RELEASE_DIR      a release folder (install.yaml, platform-minimal.yaml) to install instead of the checkout (#1625)
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
# A release folder to install instead of the checkout: the files `release / Cut` writes, so the
# gate proves the artifact people install (#1625).
RELEASE_DIR="${RELEASE_DIR:-}"

DRY_RUN="${DRY_RUN:-0}"
KEEP_ON_FAILURE="${KEEP_ON_FAILURE:-0}"
FRESH_NODE_TYPE="${FRESH_NODE_TYPE:-cpx42}"
LOCATION="${LOCATION:-nbg1}"
E2E_RUN_ID="${E2E_RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
# The run owns its own name set: Let's Encrypt issues five certificates per exact set of names per
# week, so a shared FRESH_DOMAIN spends every other run's budget (#1739). `FRESH_DOMAIN` still wins
# when a person sets it (a rerun under an old name, or a domain that must not change).
FRESH_BASE_DOMAIN="${FRESH_BASE_DOMAIN:-}"
if [ -z "${FRESH_DOMAIN:-}" ] && [ -n "$FRESH_BASE_DOMAIN" ]; then
  FRESH_DOMAIN="fresh-${E2E_RUN_ID}.${FRESH_BASE_DOMAIN}"
fi
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
        operator/examples/platform-minimal.yaml as shipped, verify it, delete it
        again (default)
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
    echo "set FRESH_BASE_DOMAIN (e.g. dev.stardelt.io; the run becomes fresh-<run id>.<base>) or FRESH_DOMAIN" >&2
    missing=1
  fi
  if [ -n "$RELEASE_DIR" ]; then
    local file
    for file in install.yaml platform-minimal.yaml; do
      if [ ! -f "$RELEASE_DIR/$file" ]; then
        echo "RELEASE_DIR=$RELEASE_DIR has no $file (a release folder needs both)" >&2
        missing=1
      fi
    done
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

# collect_diagnostics keeps what a person needs after the first failed phase: the pods, events,
# the two custom resources, the operator's log, and the logs of every unhealthy pod (pod-logs/).
# It reads no Secret, and every call is `|| true`: diagnostics never replace the real failure.
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
  # The logs of every pod that is not healthy: the server is deleted after the run, so these
  # files are the only place a crash-looping or failed pod's reason survives (#48 run #3).
  # Not ready first (Pending, Failed, a container not ready, an init container still running or
  # waiting), then the ready ones that restarted. Succeeded pods (finished Jobs) are skipped.
  mkdir -p "$RUN_DIR/pod-logs"
  local pod
  for pod in $(kubectl -n "$NAMESPACE" get pods -o json 2>/dev/null | python3 -c '
import json, sys
broken, restarted = [], []
for p in json.load(sys.stdin).get("items", []):
    st = p.get("status", {})
    phase = st.get("phase")
    if phase == "Succeeded":
        continue
    main = st.get("containerStatuses", [])
    init = st.get("initContainerStatuses", [])
    name = p["metadata"]["name"]
    if (phase in ("Pending", "Failed")
            or any(not c.get("ready") for c in main)
            or any("running" in c.get("state", {}) or "waiting" in c.get("state", {}) for c in init)):
        broken.append(name)
    elif any(c.get("restartCount", 0) > 0 for c in main + init):
        restarted.append(name)
print("\n".join(broken + restarted))
' 2>/dev/null | head -n 20); do
    # A scheduler or volume problem is in the describe, not in a log line (#48 run #3).
    kubectl -n "$NAMESPACE" describe pod "$pod" >"$RUN_DIR/pod-logs/${pod}.describe.txt" 2>&1 || true
    {
      echo "=== kubectl logs ${pod} --all-containers --tail=300"
      kubectl -n "$NAMESPACE" logs "$pod" --all-containers --prefix --tail=300 2>&1 || true
      echo "=== previous"
      kubectl -n "$NAMESPACE" logs "$pod" --all-containers --prefix --tail=300 --previous 2>&1 || true
      echo "=== status"
      kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{range .status.initContainerStatuses[*]}{"init "}{.name}{" "}{.state}{" restarts="}{.restartCount}{"\n"}{end}{range .status.containerStatuses[*]}{.name}{" "}{.state}{" last="}{.lastState}{" restarts="}{.restartCount}{"\n"}{end}' 2>&1 || true
    } >"$RUN_DIR/pod-logs/${pod}.log"
  done
  helm -n "$NAMESPACE" history "$OPERATOR_RELEASE" >"$RUN_DIR/helm.txt" 2>&1 || true
  # The bootstrap Jobs that never finish never appear as a reason in a pod row (#48 run #3).
  kubectl -n "$NAMESPACE" get jobs -o wide >"$RUN_DIR/jobs.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" describe jobs >"$RUN_DIR/jobs.describe.txt" 2>&1 || true
  collect_failed_job_logs
}

collect_failed_job_logs() {
  # Keep the logs of the pods of every Job that has failed at least once (#1736): a hook that
  # retries replaces its failed pod within minutes, so the log must be read while the pod exists.
  # One file per pod, so a retry adds an attempt instead of overwriting the earlier one.
  local dir="$RUN_DIR/pod-logs/jobs" job pod
  mkdir -p "$dir"
  for job in $(kubectl -n "$NAMESPACE" get jobs -o json 2>/dev/null | python3 -c '
import json, sys
try:
    items = json.load(sys.stdin).get("items", [])
except Exception:
    raise SystemExit(0)
for job in items:
    status = job.get("status") or {}
    if status.get("failed", 0) > 0:
        print(job["metadata"]["name"])
' 2>/dev/null); do
    for pod in $(kubectl -n "$NAMESPACE" get pods -l "job-name=$job" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
      if [ -s "$dir/$job-$pod.log" ]; then
        continue
      fi
      {
        echo "=== job $job pod $pod"
        kubectl -n "$NAMESPACE" logs "$pod" --all-containers --prefix --tail=500 2>&1 || true
        echo "=== previous"
        kubectl -n "$NAMESPACE" logs "$pod" --all-containers --prefix --tail=500 --previous 2>&1 || true
        echo "=== status"
        kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{range .status.initContainerStatuses[*]}{"init "}{.name}{" "}{.state}{" restarts="}{.restartCount}{"\n"}{end}{range .status.containerStatuses[*]}{.name}{" "}{.state}{" last="}{.lastState}{" restarts="}{.restartCount}{"\n"}{end}' 2>&1 || true
      } >"$dir/$job-$pod.log"
    done
  done
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
  # Poll instead of one 25-minute `kubectl wait`: a retrying hook Job replaces its failed pod
  # within minutes, so its log is only readable while the wait runs (#1736). Collection is every
  # 60 s, the readiness check every 15 s.
  local deadline=$((SECONDS + 1500)) next_collect=0 platform_ready=0 lakehouse_ready=0
  while [ "$SECONDS" -lt "$deadline" ]; do
    if [ "$SECONDS" -ge "$next_collect" ]; then
      collect_failed_job_logs
      next_collect=$((SECONDS + 60))
    fi
    if kubectl -n "$NAMESPACE" wait --for=condition=Ready platforminstance/cluster --timeout=15s >/dev/null 2>&1; then
      platform_ready=1
    fi
    if kubectl -n "$NAMESPACE" wait --for=condition=Ready lakehouse/main --timeout=15s >/dev/null 2>&1; then
      lakehouse_ready=1
    fi
    if [ "$platform_ready" = 1 ] && [ "$lakehouse_ready" = 1 ]; then
      return 0
    fi
    sleep 15
  done
  if [ "$platform_ready" != 1 ]; then
    echo "e2e-fresh-node: platforminstance/cluster was not Ready after 25m" >&2
  fi
  if [ "$lakehouse_ready" != 1 ]; then
    echo "e2e-fresh-node: lakehouse/main was not Ready after 25m" >&2
  fi
  return 1
}

# install_from_release applies the release folder's install.yaml (the namespace plus the rendered
# operator chart) and waits for the Deployment the checkout's helm release would also leave
# behind (#1625). A function, not `bash -c`, so the paths stay quoted once.
install_from_release() {
  kubectl apply --server-side -f "$RELEASE_DIR/install.yaml" &&
    kubectl -n "$NAMESPACE" rollout status "deploy/${OPERATOR_DEPLOY}" --timeout=300s
}

# CoreDNS logs every query from here on, so the run can list what the platform resolves
# outside the cluster (k3s imports coredns-custom *.override into the main server block).
enable_dns_log() {
  kubectl -n kube-system create configmap coredns-custom --from-literal=log.override=log \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n kube-system rollout restart deploy/coredns
  kubectl -n kube-system rollout status deploy/coredns --timeout=120s
}

# The run's inventory: egress.txt and images.txt next to report.txt (Sovereignty page).
collect_inventory() {
  kubectl -n kube-system logs deploy/coredns --tail=-1 >"$RUN_DIR/coredns.log"
  kubectl get pods -A -o json >"$RUN_DIR/pods.json"
  python3 "$PLATFORM_DIR/scripts/fresh_node_inventory.py" --coredns-log "$RUN_DIR/coredns.log" \
    --pods-json "$RUN_DIR/pods.json" --own-domain "${FRESH_DOMAIN:-}" --out-dir "$RUN_DIR"
}

phase_up() {
  run_phase "fresh node" "$RUN_DIR/01-fresh-node.log" -- \
    env FRESH_NAME="$FRESH_NAME" FRESH_CONFIG="$FRESH_CONFIG" FRESH_KUBECONFIG="$FRESH_KUBECONFIG" \
      FRESH_DOMAIN="${FRESH_DOMAIN:-}" FRESH_NODE_TYPE="$FRESH_NODE_TYPE" \
      bash "$FRESH_NODE_SH" up || return 1

  # Everything after this point talks to the throwaway cluster, never to the current context.
  export KUBECONFIG="$FRESH_KUBECONFIG"

  run_phase "dns log" "$RUN_DIR/01-fresh-node.log" -- enable_dns_log || return 1

  if [ -n "$RELEASE_DIR" ]; then
    run_phase "operator" "$RUN_DIR/02-operator.log" -- install_from_release || return 1
  else
    run_phase "operator" "$RUN_DIR/02-operator.log" -- \
      make -C "$PLATFORM_DIR" install-operator || return 1
  fi

  local instance_source="$REPO/operator/examples/platform-minimal.yaml"
  if [ -n "$RELEASE_DIR" ]; then
    instance_source="$RELEASE_DIR/platform-minimal.yaml"
  fi
  sed -e "s/data\.example\.com/${FRESH_DOMAIN}/g" \
    "$instance_source" >"$RUN_DIR/platform-instance.yaml"
  { grep -qE '^    pipelines: true( |$)' "$RUN_DIR/platform-instance.yaml" && grep -qE '^    dashboards: true( |$)' "$RUN_DIR/platform-instance.yaml"; } || {
    echo "e2e-fresh-node: $instance_source does not turn on the pipelines and dashboards pillars, which the product phase checks — the release example must (#1810)" >&2
    # FINAL_RC is only set by run_phase, and cmd_up calls `phase_up || true`: without these two
    # lines the run would report PASS having applied nothing (#1729).
    FINAL_RC=1
    failure_phase="platform instance"
    return 1
  }
  run_phase "platform instance" "$RUN_DIR/03-platform-instance.log" -- \
    kubectl apply -f "$RUN_DIR/platform-instance.yaml" || return 1
  run_phase "platform ready" "$RUN_DIR/03-platform-instance.log" -- \
    wait_for_resources || return 1
  # `product` is the ticket's assertion list (#48 scope 2): readiness first, then Airflow,
  # Superset, Lakekeeper's token rule and Nova's sign-in page. Readiness alone would leave
  # four of the six assertions unmade. The example turns on the `pipelines` and `dashboards`
  # pillars itself, and the run applies it as shipped (#1810).
  run_phase "verify" "$RUN_DIR/04-verify.log" -- \
    bash "$PLATFORM_DIR/scripts/verify-fresh-node.sh" product || return 1
  run_phase "inventory" "$RUN_DIR/05-inventory.log" -- collect_inventory || return 1
  local inv_line
  while IFS= read -r inv_line; do
    report_lines+=("  ${inv_line}")
  done <"$RUN_DIR/inventory-summary.txt"
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
  if [ -n "$RELEASE_DIR" ]; then
    out+="install: release ${RELEASE_DIR}"$'\n'
  else
    out+="install: checkout"$'\n'
  fi
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
