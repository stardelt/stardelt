#!/usr/bin/env bash
# A throwaway single-node cluster on Hetzner Cloud, for a fresh-node run.
#
#   bash dev/hetzner-k3s/fresh-node.sh up     # create, wait for the node, print the kubeconfig
#   bash dev/hetzner-k3s/fresh-node.sh down   # delete it again
#
# The dev cluster's config (dev/hetzner-k3s/cluster.yaml) is the only record `make dev-down`
# has of what to delete, and the dev DNS belongs to whoever runs alpha.dev: this wrapper
# therefore renders its own config (.fresh-<name>.yaml), never updates DNS, and refuses any
# cluster name that is not `stardelt-fresh-*`. It never reads or writes cluster.yaml
# (platform#1018).
#
# Environment: FRESH_NAME, FRESH_CONFIG, FRESH_VOLUMES, FRESH_KUBECONFIG, FRESH_NODE_TYPE,
# FRESH_WAIT_SECONDS, FRESH_INGRESS, FRESH_DOMAIN, LOCATION, KEEP_ON_FAILURE, NO_WAIT,
# DRY_RUN (passed through to provision.sh), HETZNER_ENV (default
# ~/.config/stardelt/hetzner.env: the file the teardown's HCLOUD_TOKEN comes from).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Only a throwaway cluster may reach provision.sh/destroy.sh from here.
FRESH_PATTERN='^stardelt-fresh-[a-z0-9-]+$'

# `down` has to find the run it created: when the caller did not name one, it reads the
# .fresh-*.yaml the earlier `up` left behind (do_down).
FRESH_NAME_GIVEN=0; if [ -n "${FRESH_NAME+x}" ]; then FRESH_NAME_GIVEN=1; fi
FRESH_KUBECONFIG_GIVEN=0; if [ -n "${FRESH_KUBECONFIG+x}" ]; then FRESH_KUBECONFIG_GIVEN=1; fi

FRESH_NAME="${FRESH_NAME:-stardelt-fresh-$(date -u +%Y%m%d%H%M%S)}"
FRESH_CONFIG="${FRESH_CONFIG:-$HERE/.fresh-${FRESH_NAME}.yaml}"
# The Hetzner volume ids of this run's PVs, written before teardown and read after the
# cluster is gone (cloud_cleanup / delete_volumes).
FRESH_VOLUMES="${FRESH_VOLUMES:-${FRESH_CONFIG%.yaml}.volumes}"
FRESH_VOLUME_TRIES="${FRESH_VOLUME_TRIES:-12}"
FRESH_VOLUME_SLEEP="${FRESH_VOLUME_SLEEP:-10}"
FRESH_KUBECONFIG="${FRESH_KUBECONFIG:-$HOME/.kube/${FRESH_NAME}.yaml}"
# 16 GB: the `demo` profile (Airflow + Superset) does not fit an 8 GB node (cpx32).
FRESH_NODE_TYPE="${FRESH_NODE_TYPE:-cpx42}"
FRESH_WAIT_SECONDS="${FRESH_WAIT_SECONDS:-600}"
# FRESH_INGRESS=0 skips Traefik + cert-manager (no load balancer, no cost).
FRESH_INGRESS="${FRESH_INGRESS:-1}"
KEEP_ON_FAILURE="${KEEP_ON_FAILURE:-0}"
NO_WAIT="${NO_WAIT:-0}"
DRY_RUN="${DRY_RUN:-0}"
IN_CLEANUP=0

# The teardown below calls `hcloud`, which reads its token from HCLOUD_TOKEN only. The
# maintainer keeps it in this file (provision.sh loads it the same way); without it every
# delete failed silently and the volumes of a fresh-node run stayed billed (#1632).
HETZNER_ENV="${HETZNER_ENV:-$HOME/.config/stardelt/hetzner.env}"
# shellcheck source=/dev/null
[ -f "$HETZNER_ENV" ] && { set -a; . "$HETZNER_ENV"; set +a; }

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

# Teardown in the order the cloud controller needs: the load balancer (a Service) first,
# then the volume ids are recorded, then this run's DNS record, then the cluster itself —
# the volumes and placement groups are deleted after the cluster (`delete_volumes`), and
# the network/firewall a failed `up` can leave behind (issue #86).
cloud_cleanup() {
  local kc="$FRESH_KUBECONFIG"
  if [ ! -f "$kc" ] || ! kubectl --kubeconfig "$kc" get nodes >/dev/null 2>&1; then
    return 0
  fi

  echo "› uninstalling traefik (the Hetzner load balancer goes with its Service)"
  helm --kubeconfig "$kc" uninstall traefik -n traefik --wait --ignore-not-found \
    || echo "  traefik uninstall failed (continuing)" >&2

  # The PVCs cannot go while the platform's pods mount them (pvc-protection), so this
  # does not wait: the volume ids are recorded and deleted once the server is gone.
  echo "› recording the Hetzner volumes of this run (${FRESH_VOLUMES})"
  kubectl --kubeconfig "$kc" get pv \
    -o jsonpath='{range .items[?(@.spec.csi.driver=="csi.hetzner.cloud")]}{.spec.csi.volumeHandle}{"\n"}{end}' \
    >"$FRESH_VOLUMES" 2>/dev/null || echo "  could not list PVs (continuing)" >&2
  kubectl --kubeconfig "$kc" delete pvc --all -A --wait=false >/dev/null 2>&1 || true

  local domain_file="$HERE/.fresh-${FRESH_NAME}.domain" domain
  if [ -f "$domain_file" ]; then
    domain="$(head -n 1 "$domain_file")"
    if [ -n "$domain" ]; then
      echo "› removing the DNS record for ${domain}"
      NAME="$domain" bash "$HERE/../cloudflare-dns.sh" delete \
        || echo "  DNS delete failed — remove ${domain} in Cloudflare by hand" >&2
    fi
    rm -f "$domain_file"
  fi
}

# After the server is gone Hetzner detaches its volumes within a minute or so; until
# then `hcloud volume delete` fails, so each id is retried. Only ids recorded from this
# cluster's own PVs are deleted, never a volume found by listing.
delete_volumes() {
  local name="$1" id left=""
  if ! command -v hcloud >/dev/null 2>&1; then
    if [ -s "$FRESH_VOLUMES" ]; then
      echo "warning: hcloud not found — delete these Hetzner volumes by hand:" >&2
      sed 's/^/  /' "$FRESH_VOLUMES" >&2
    fi
    return 0
  fi
  if [ -s "$FRESH_VOLUMES" ]; then
    # One sentence instead of a 15-minute retry loop that cannot succeed: hcloud reads the
    # token from the environment only (#1632).
    [ -n "${HCLOUD_TOKEN:-}" ] || {
      echo "warning: no HCLOUD_TOKEN (set it or put it in $HETZNER_ENV) — volumes not deleted:$(tr '\n' ' ' <"$FRESH_VOLUMES")" >&2
      return 0
    }
    echo "› deleting $(grep -c . "$FRESH_VOLUMES") Hetzner volume(s) of this run"
    while read -r id; do
      [ -n "$id" ] || continue
      local ok=0 err=""
      for _ in $(seq 1 "$FRESH_VOLUME_TRIES"); do
        if err="$(hcloud volume delete "$id" 2>&1 >/dev/null)"; then ok=1; break; fi
        sleep "$FRESH_VOLUME_SLEEP"
      done
      if [ "$ok" != 1 ]; then
        echo "  volume ${id}: ${err:-no error text}" >&2
        left="${left} ${id}"
      fi
    done <"$FRESH_VOLUMES"
  fi
  local pg
  for pg in $(hcloud placement-group list -o noheader -o columns=name 2>/dev/null | grep "^${name}" || true); do
    hcloud placement-group delete "$pg" >/dev/null 2>&1 || true
  done
  if [ -n "$left" ]; then
    echo "warning: these Hetzner volumes are still there — delete them by hand: hcloud volume delete${left}" >&2
    return 0
  fi
  rm -f "$FRESH_VOLUMES"
}

do_down() {
  # `up` names the cluster after the time it ran, so `down` without FRESH_NAME has to find
  # that run in the .fresh-*.yaml it wrote instead of inventing a name that matches nothing.
  if [ "$FRESH_NAME_GIVEN" != 1 ]; then
    local found=()
    shopt -s nullglob
    found=("$HERE"/.fresh-*.yaml)
    shopt -u nullglob
    case "${#found[@]}" in
      0)
        echo "nothing to delete: no .fresh-*.yaml in dev/hetzner-k3s (${HERE})"
        return 0
        ;;
      1)
        FRESH_CONFIG="${found[0]}"
        FRESH_NAME="$(awk -F': *' '/^cluster_name:/ { print $2; exit }' "$FRESH_CONFIG")"
        echo "› found the run: ${FRESH_CONFIG} (${FRESH_NAME})"
        if [ "$FRESH_KUBECONFIG_GIVEN" != 1 ]; then
          FRESH_KUBECONFIG="$HOME/.kube/${FRESH_NAME}.yaml"
        fi
        ;;
      *)
        echo "several fresh-node runs found:" >&2
        printf '  %s\n' "${found[@]}" >&2
        echo "set FRESH_NAME=<one of the above>" >&2
        return 1
        ;;
    esac
  fi
  refuse_foreign_name "$FRESH_NAME" || exit 1
  if [ ! -f "$FRESH_CONFIG" ]; then
    echo "nothing to delete: ${FRESH_CONFIG} does not exist"
    return 0
  fi
  local name
  name="$(awk -F': *' '/^cluster_name:/ { print $2; exit }' "$FRESH_CONFIG")"
  if ! refuse_foreign_name "$name"; then
    echo "  ${FRESH_CONFIG} describes cluster '${name}' — not deleting it." >&2
    exit 1
  fi
  if [ "$DRY_RUN" != 1 ]; then
    cloud_cleanup
  fi
  CLUSTER_NAME="$name" CLUSTER_CONFIG="$FRESH_CONFIG" FORCE=1 REMOVE_DNS=0 DNS_NOTE=0 DRY_RUN="$DRY_RUN" \
    bash "$HERE/destroy.sh"
  if [ "$DRY_RUN" != 1 ]; then
    # A failed `up` can leave a private network or firewall behind: hetzner-k3s creates them
    # before it creates the server, and its delete only removes them once the cluster exists.
    if command -v hcloud >/dev/null 2>&1; then
      hcloud network delete "$name" >/dev/null 2>&1 || true
      hcloud firewall delete "$name" >/dev/null 2>&1 || true
    fi
    delete_volumes "$name"
    rm -f "$FRESH_CONFIG" "$FRESH_KUBECONFIG"
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

# Traefik (a Service with a Hetzner load balancer) + cert-manager, because provision.sh
# turns both k3s addons off: without them an Ingress with `ingressClassName: traefik`
# and cert-manager TLS has nothing to route through (issue #86).
install_ingress() {
  local kc="$FRESH_KUBECONFIG" lb_ip=""
  echo "› installing traefik (load balancer in ${LOCATION:-nbg1}) and cert-manager"
  helm repo add traefik https://traefik.github.io/charts --force-update >/dev/null
  helm repo add jetstack https://charts.jetstack.io --force-update >/dev/null
  helm --kubeconfig "$kc" upgrade --install traefik traefik/traefik \
    -n traefik --create-namespace --wait \
    --set "service.annotations.load-balancer\\.hetzner\\.cloud/location=${LOCATION:-nbg1}"
  helm --kubeconfig "$kc" upgrade --install cert-manager jetstack/cert-manager \
    -n cert-manager --create-namespace --set crds.enabled=true --wait
  kubectl --kubeconfig "$kc" apply -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: letsencrypt
spec:
  acme:
    server: https://acme-v02.api.letsencrypt.org/directory
    privateKeySecretRef:
      name: letsencrypt
    solvers:
      - http01:
          ingress:
            ingressClassName: traefik
EOF

  for _ in $(seq 1 60); do
    lb_ip="$(kubectl --kubeconfig "$kc" -n traefik get svc traefik \
      -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)"
    if [ -n "$lb_ip" ]; then break; fi
    sleep 5
  done
  if [ -z "$lb_ip" ]; then
    echo "warning: traefik has no load balancer IP yet (check 'kubectl -n traefik get svc traefik')" >&2
  fi

  if [ -n "${FRESH_DOMAIN:-}" ] && [ -n "$lb_ip" ]; then
    NAME="$FRESH_DOMAIN" IP="$lb_ip" bash "$HERE/../cloudflare-dns.sh" upsert
    # The record this run owns: `down` removes exactly it again.
    printf '%s\n' "$FRESH_DOMAIN" > "$HERE/.fresh-${FRESH_NAME}.domain"
  fi
  echo "base domain: ${FRESH_DOMAIN:-<none>} (load balancer ${lb_ip:-<none>})"
}

do_up() {
  refuse_foreign_name "$FRESH_NAME" || exit 1
  if [ -e "$FRESH_CONFIG" ]; then
    echo "refusing: ${FRESH_CONFIG} exists — run 'fresh-node.sh down' first" >&2
    exit 1
  fi

  # hetzner-k3s runs with use_agent: false and needs both halves of a passphrase-free key.
  # Checking here fails before a server exists, not after it bills by the hour.
  if [ "$DRY_RUN" != 1 ]; then
    local key pub missing=""
    key="${SSH_PRIVATE_KEY:-$HOME/.ssh/id_ed25519}"
    pub="${SSH_PUBLIC_KEY:-$HOME/.ssh/id_ed25519.pub}"
    if [ ! -f "$key" ]; then missing="$key"; fi
    if [ ! -f "$pub" ]; then missing="${missing:+${missing} and }${pub}"; fi
    if [ -n "$missing" ]; then
      echo "no SSH key at ${missing}: set SSH_PUBLIC_KEY and SSH_PRIVATE_KEY (a key without passphrase; hetzner-k3s runs with use_agent: false)" >&2
      exit 1
    fi
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
