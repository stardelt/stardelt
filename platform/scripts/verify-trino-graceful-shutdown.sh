#!/usr/bin/env bash
# A worker finishes what it is running before it exits (platform#131 A1, platform#1536).
#
#   NS=stardelt bash scripts/verify-trino-graceful-shutdown.sh
#
# The Trino chart's own connection test cannot run on this install: it mounts Secret
# <namespace>-certificates (this install's TLS material is trino-coordinator-tls) and signs
# in as admin/admin123 (Secret trino-password-db carries `superset` only), so its weight-2
# graceful-shutdown hook never runs. This is that hook's proof, run by the platform: it
# deletes one worker pod and reads the worker's own log for `Shutdown requested` — the line
# Trino writes only after the preStop drain was accepted — and a replacement becoming Ready.
#
# It deletes exactly one worker pod, as the chart's test does. The Deployment replaces it.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NS="${NS:-${NAMESPACE:-stardelt}}"
problems=0
ok()  { echo "✓ $*"; }
bad() { echo "✗ $*"; problems=$((problems + 1)); }

for tool in kubectl python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "UNVERIFIED ($tool not found)"; exit 3; }
done
kubectl -n "$NS" get deploy trino-worker >/dev/null 2>&1 || {
  echo "UNVERIFIED (no Deployment trino-worker in namespace $NS)"; exit 3; }

grace="$(kubectl -n "$NS" get deploy trino-worker -o jsonpath='{.spec.template.spec.terminationGracePeriodSeconds}')"
if [ "$grace" = "240" ]; then
  ok "worker terminationGracePeriodSeconds = 240"
else
  bad "worker terminationGracePeriodSeconds = ${grace:-<unset>}, expected 240"
fi

config="$(kubectl -n "$NS" get configmap trino-worker -o jsonpath='{.data.config\.properties}' 2>/dev/null || true)"
case "$config" in
  *"shutdown.grace-period=120s"*) ok "configmap/trino-worker carries shutdown.grace-period=120s" ;;
  *) bad "configmap/trino-worker has no shutdown.grace-period=120s" ;;
esac

worker="$(kubectl -n "$NS" get pods \
  -l app.kubernetes.io/name=trino,app.kubernetes.io/component=worker \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}')"
[ -n "$worker" ] || { echo "UNVERIFIED (no running worker pod — the engine is scaled to zero)"; exit 3; }

tmp="$(mktemp -d)"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT
kubectl -n "$NS" logs -f "$worker" -c trino-worker >"$tmp/worker.log" 2>&1 &
tail_pid=$!
sleep 2
echo "› deleting worker pod $worker"
kubectl -n "$NS" delete pod "$worker" --wait=false >/dev/null

drained=0
for _ in $(seq 1 75); do
  if grep -q 'Shutdown requested' "$tmp/worker.log"; then drained=1; break; fi
  sleep 2
done
kill "$tail_pid" 2>/dev/null || true
if [ "$drained" = 1 ]; then
  ok "the deleted worker logged 'Shutdown requested'"
else
  bad "no 'Shutdown requested' in $worker's log within 150s"
fi

replacement=""
for _ in $(seq 1 90); do
  replacement="$(kubectl -n "$NS" get pods \
    -l app.kubernetes.io/name=trino,app.kubernetes.io/component=worker \
    --field-selector=status.phase=Running -o json \
    | python3 -c 'import json,sys
old = sys.argv[1]
rows = [p for p in json.load(sys.stdin)["items"]
        if p["metadata"]["name"] != old
        and any(c.get("type") == "Ready" and c.get("status") == "True"
                for c in p.get("status", {}).get("conditions", []))]
print(rows[0]["metadata"]["name"] if rows else "")' "$worker")"
  [ -n "$replacement" ] && break
  sleep 2
done
if [ -n "$replacement" ]; then
  ok "a replacement worker is Ready: $replacement"
else
  bad "no Ready replacement worker within 180s"
fi

# The FAIL branch exits first on purpose: a trailing unconditional `exit` makes
# ShellCheck (0.9 SC2317, 0.11 SC2329) call the trap-only `cleanup` unreachable.
[ "$problems" -eq 0 ] || { echo "FAIL ($problems problem(s))"; exit 1; }
echo "PASS (4 of 4)"
