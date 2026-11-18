#!/usr/bin/env bash
# A throwaway single-node cluster on Hetzner Cloud, for a fresh-node run.
#
#   bash dev/hetzner-k3s/fresh-node.sh up     # create, wait for the node, print the kubeconfig
#   bash dev/hetzner-k3s/fresh-node.sh down   # delete it again
#
# `down` without FRESH_NAME deletes the run that is on disk: the .fresh-*.yaml in this
# folder names it, so the shell that ran `up` does not have to survive.
#
# The dev cluster's config (dev/hetzner-k3s/cluster.yaml) is the only record `make dev-down`
# has of what to delete, and the dev DNS belongs to whoever runs alpha.dev: this wrapper
# therefore renders its own config (.fresh-<name>.yaml), never touches the dev DNS, and
# refuses any cluster name that is not `stardelt-fresh-*`. It never reads or writes
# cluster.yaml (platform#1018).
#
# Environment: FRESH_NAME, FRESH_CONFIG, FRESH_KUBECONFIG, FRESH_NODE_TYPE,
# FRESH_WAIT_SECONDS, FRESH_DOMAIN, FRESH_INGRESS, KEEP_ON_FAILURE, NO_WAIT, DRY_RUN
# (passed through to provision.sh).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Only a throwaway cluster may reach provision.sh/destroy.sh from here.
FRESH_PATTERN='^stardelt-fresh-[a-z0-9-]+$'

# `down` behaves differently when the caller named no cluster: it looks for the config
# `up` left behind instead of inventing a timestamped name nothing was ever created for.
FRESH_NAME_GIVEN=0
if [ -n "${FRESH_NAME+x}" ]; then FRESH_NAME_GIVEN=1; fi
FRESH_NAME="${FRESH_NAME:-stardelt-fresh-$(date -u +%Y%m%d%H%M%S)}"
FRESH_CONFIG="${FRESH_CONFIG:-$HERE/.fresh-${FRESH_NAME}.yaml}"
FRESH_KUBECONFIG="${FRESH_KUBECONFIG:-$HOME/.kube/${FRESH_NAME}.yaml}"
# 16 GB (cpx42, 8 vCPU): the `demo` profile (Airflow + Superset) does not fit an 8 GB cpx32.
FRESH_NODE_TYPE="${FRESH_NODE_TYPE:-cpx42}"
# FRESH_DOMAIN (for example fresh.dev.stardelt.io) gets an A record at the load balancer.
FRESH_DOMAIN="${FRESH_DOMAIN:-}"
# FRESH_INGRESS=0 skips traefik/cert-manager on the node.
FRESH_INGRESS="${FRESH_INGRESS:-1}"
FRESH_WAIT_SECONDS="${FRESH_WAIT_SECONDS:-600}"
KEEP_ON_FAILURE="${KEEP_ON_FAILURE:-0}"
NO_WAIT="${NO_WAIT:-0}"
DRY_RUN="${DRY_RUN:-0}"
IN_CLEANUP=0

usage() {
  echo "usage: fresh-node.sh up|down" >&2
  echo "  up    create a throwaway cluster (DRY_RUN=1 prints the plan only)" >&2
  echo "  down  delete it, together with its config and kubeconfig" >&2
}

# A name that is not ours is never touched: this keeps `stardelt-dev` out of this path even
# when the caller passes it explicitly.
refuse_foreign_name() {
  local name="$1"
  if [[ ! "$name" =~ $FRESH_PATTERN ]]; then
    echo "refusing: '${name}' is not a throwaway cluster name (${FRESH_PATTERN})" >&2
    return 1
  fi
}

do_down() {
  local name
  if [ "$FRESH_NAME_GIVEN" = 0 ]; then
    local found=()
    shopt -s nullglob
    found=("$HERE"/.fresh-stardelt-fresh-*.yaml)
    shopt -u nullglob
    if [ "${#found[@]}" = 0 ]; then
      echo "nothing to delete: no .fresh-*.yaml in dev/hetzner-k3s"
      return 0
    elif [ "${#found[@]}" = 1 ]; then
      FRESH_CONFIG="${found[0]}"
      FRESH_NAME="$(awk -F': *' '/^cluster_name:/ { print $2; exit }' "$FRESH_CONFIG")"
      FRESH_KUBECONFIG="$HOME/.kube/${FRESH_NAME}.yaml"
    else
      echo "refusing: more than one fresh cluster on disk:" >&2
      printf '  %s\n' "${found[@]}" >&2
      echo "set FRESH_NAME=<one of the above>" >&2
      exit 1
    fi
  fi
  refuse_foreign_name "$FRESH_NAME" || exit 1
  if [ ! -f "$FRESH_CONFIG" ]; then
    echo "nothing to delete: ${FRESH_CONFIG} does not exist"
    return 0
  fi
  name="$(awk -F': *' '/^cluster_name:/ { print $2; exit }' "$FRESH_CONFIG")"
  if ! refuse_foreign_name "$name"; then
    echo "  ${FRESH_CONFIG} describes cluster '${name}' — not deleting it." >&2
    exit 1
  fi
  local domain_file="$HERE/.fresh-${name}.domain"

  # What is on the cluster is ours and would outlive hetzner-k3s delete: one Hetzner load
  # balancer, one volume per claim, and the DNS record the run wrote (first #48 run).
  if [ "$DRY_RUN" != 1 ] && [ -f "$FRESH_KUBECONFIG" ] \
    && kubectl --kubeconfig "$FRESH_KUBECONFIG" get nodes >/dev/null 2>&1; then
    echo "› uninstalling traefik (the cloud controller deletes the load balancer)"
    helm uninstall traefik -n traefik --wait --ignore-not-found || true

    echo "› deleting volumes (pvc → pv → Hetzner volume)"
    kubectl --kubeconfig "$FRESH_KUBECONFIG" delete pvc --all -A --wait=true --timeout=5m \
      || echo "  pvc delete reported an error (continuing)" >&2
    local pvs=""
    for _ in $(seq 1 60); do
      pvs="$(kubectl --kubeconfig "$FRESH_KUBECONFIG" get pv -o name)" || pvs=""
      [ -z "$pvs" ] && break
      sleep 5
    done
    if [ -n "$pvs" ]; then
      echo "  volumes still present after 5 min — delete these volumes in the Hetzner console:" >&2
      while IFS= read -r pv; do echo "  $pv" >&2; done <<< "$pvs"
    fi
  fi

  if [ -f "$domain_file" ] && [ "$DRY_RUN" != 1 ]; then
    local domain
    domain="$(head -1 "$domain_file")"
    echo "› removing DNS records for ${domain}"
    NAME="$domain" bash "$HERE/../cloudflare-dns.sh" delete \
      || echo "  DNS cleanup failed — delete ${domain} in the Cloudflare dashboard" >&2
    rm -f "$domain_file"
  fi

  CLUSTER_NAME="$name" CLUSTER_CONFIG="$FRESH_CONFIG" FORCE=1 REMOVE_DNS=0 DNS_NOTE=0 DRY_RUN="$DRY_RUN" \
    bash "$HERE/destroy.sh"
  if [ "$DRY_RUN" != 1 ]; then
    rm -f "$FRESH_CONFIG" "$FRESH_KUBECONFIG"
    # A failed `up` can leave the network or the firewall behind even when the cluster is gone.
    if command -v hcloud >/dev/null 2>&1; then
      hcloud network delete "$FRESH_NAME" 2>/dev/null || true
      hcloud firewall delete "$FRESH_NAME" 2>/dev/null || true
    fi
  fi
}

# Any failure after the cluster starts being created deletes it again: a half-built throwaway
# node bills by the hour and nobody knows its name. KEEP_ON_FAILURE=1 keeps it for a look.
on_error() {
  local status="$1"
  if [ "$IN_CLEANUP" = 0 ] && [ "$KEEP_ON_FAILURE" = 0 ]; then
    IN_CLEANUP=1
    echo "✗ up failed (status ${status}) — deleting the throwaway node" >&2
    if ! do_down; then
      echo "  delete failed — run 'bash dev/hetzner-k3s/fresh-node.sh down'" >&2
    fi
  fi
  exit "$status"
}

# provision.sh turns k3s's Traefik and service load balancer off, so the operator's
# example (`ingress.className: traefik`, TLS through cert-manager) has nothing to route
# through: install both, and point FRESH_DOMAIN at the one Hetzner load balancer.
install_ingress() {
  local kc=(--kubeconfig "$FRESH_KUBECONFIG") lb_ip="" domain_file="$HERE/.fresh-${FRESH_NAME}.domain"
  echo "› installing traefik (ingress, one Hetzner load balancer @ ${LOCATION:-nbg1})"
  helm repo add traefik https://traefik.github.io/charts --force-update >/dev/null
  helm upgrade --install traefik traefik/traefik -n traefik --create-namespace --wait \
    --set "service.annotations.load-balancer\.hetzner\.cloud/location=${LOCATION:-nbg1}"

  echo "› installing cert-manager"
  helm repo add jetstack https://charts.jetstack.io --force-update >/dev/null
  helm upgrade --install cert-manager jetstack/cert-manager -n cert-manager --create-namespace \
    --set crds.enabled=true --wait
  kubectl "${kc[@]}" apply -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    privateKeySecretRef:
      name: letsencrypt-account-key
    solvers:
      - http01:
          ingress:
            ingressClassName: traefik
EOF

  for _ in $(seq 1 60); do
    lb_ip="$(kubectl "${kc[@]}" -n traefik get svc traefik \
      -o jsonpath='{.status.loadBalancer.ingress[0].ip}')" || lb_ip=""
    [ -n "$lb_ip" ] && break
    sleep 5
  done

  if [ -n "$FRESH_DOMAIN" ]; then
    if [ -n "$lb_ip" ]; then
      NAME="$FRESH_DOMAIN" IP="$lb_ip" bash "$HERE/../cloudflare-dns.sh" upsert
      printf '%s\n' "$FRESH_DOMAIN" > "$domain_file"
    else
      echo "  no load balancer address yet — not creating DNS for ${FRESH_DOMAIN}" >&2
    fi
  fi
  echo "base domain: ${FRESH_DOMAIN:-<none>} (load balancer ${lb_ip:-unknown})"
}

do_up() {
  refuse_foreign_name "$FRESH_NAME" || exit 1
  if [ -e "$FRESH_CONFIG" ]; then
    echo "refusing: ${FRESH_CONFIG} exists — run 'fresh-node.sh down' first" >&2
    exit 1
  fi

  # hetzner-k3s runs with use_agent: false, so the node is only reachable through an
  # unencrypted key: a missing one fails after the server is already billed (first #48 run).
  if [ "$DRY_RUN" != 1 ]; then
    local key
    for key in "${SSH_PRIVATE_KEY:-$HOME/.ssh/id_ed25519}" \
      "${SSH_PUBLIC_KEY:-$HOME/.ssh/id_ed25519.pub}"; do
      if [ ! -f "$key" ]; then
        echo "no SSH key at ${key}: set SSH_PUBLIC_KEY and SSH_PRIVATE_KEY (a key without passphrase; hetzner-k3s runs with use_agent: false)" >&2
        exit 1
      fi
    done
  fi

  trap 'on_error $?' ERR
  CLUSTER_NAME="$FRESH_NAME" \
    CLUSTER_CONFIG="$FRESH_CONFIG" \
    KUBECONFIG_PATH="$FRESH_KUBECONFIG" \
    NODE_TYPE="$FRESH_NODE_TYPE" \
    DNS_UPDATE=0 \
    DRY_RUN="$DRY_RUN" \
    bash "$HERE/provision.sh"

  # A dry run only prints: there is no node to wait for and no kubeconfig to export.
  if [ "$DRY_RUN" = 1 ]; then exit 0; fi

  if [ "$NO_WAIT" != 1 ]; then
    echo "› waiting up to ${FRESH_WAIT_SECONDS}s for the node to become Ready"
    kubectl --kubeconfig "$FRESH_KUBECONFIG" wait --for=condition=Ready nodes --all \
      --timeout="${FRESH_WAIT_SECONDS}s"
  fi

  if [ "$FRESH_INGRESS" != 0 ]; then
    install_ingress
  else
    echo "› ingress skipped (FRESH_INGRESS=0)"
  fi

  echo
  echo "✓ fresh node ${FRESH_NAME}"
  echo "  export KUBECONFIG=\"${FRESH_KUBECONFIG}\""
  echo "  operator: helm upgrade --install stardelt-operator <operator repo>/chart -n stardelt --create-namespace"
  echo "  delete:   FRESH_NAME=${FRESH_NAME} bash dev/hetzner-k3s/fresh-node.sh down"
}

case "${1:-}" in
  up) do_up ;;
  down) do_down ;;
  *)
    usage
    exit 2
    ;;
esac
