#!/usr/bin/env bash
# The operator is quiet on an idle cluster (stardelt/stardelt#274 rule 7, #453).
#
# Why: #274 writes the operator's budget down — no status write, fewer than two reconciles
# per object, under 50 m CPU and under 50 KB/s network in a minute — and nothing measured
# it. Measured on the lab 2026-11-19T19:18Z, 55 min uptime: 22 reconciles, 1 m CPU per pod.
#
#   make verify-operator-quiet
#   NS=stardelt WINDOW=120 bash scripts/verify-operator-quiet.sh
#
# Read-only: `kubectl get` and the pods' own metrics/stats, nothing applied, patched or
# deleted. Two measured facts carry the budget:
#   - the operator pods are labelled app=stardelt-operator
#     (`kubectl -n stardelt get svc stardelt-operator-metrics -o jsonpath='{.spec.selector}'`
#     answers {"app":"stardelt-operator"}), so pods and the Service agree;
#   - only the leader reconciles (`stardelt_operator_leader` is 1 on one pod and 0 on the
#     other), so the standby still costs CPU and network: the budget is 50 m for the pair.
# A status write is detected twice, and both rows are required: the counter
# `stardelt_operator_status_writes_total` (rule 6, operator#571) must not move, and no
# `metadata.resourceVersion` may change. The counter is the direct signal a customer's
# Prometheus has too; resourceVersion catches a write the counter does not see (a writing
# replica that restarted inside the window) and a write that changed the object some other way.
#
# Exit codes: 0 PASS (every row inside the budget), 1 FAIL (a row outside it, each changed
# object named on its own ✗ line), 3 UNVERIFIED (no kubectl, no operator pod, the metrics
# endpoint answers nothing or has no status-write counter, kubectl top says the Metrics API is
# not available, or the kubelet stats summary cannot be read — the check proved nothing and
# must not be read as PASS).
# Needs: kubectl, python3.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS="${NS:-${NAMESPACE:-stardelt}}"
WINDOW="${WINDOW:-60}"
# A rate needs a window: WINDOW=0 is the unit tests' fast path (sleep 0), not a budget.
eff_window=$(( WINDOW > 0 ? WINDOW : 1 ))

command -v kubectl >/dev/null 2>&1 || { echo "verify-operator-quiet: UNVERIFIED (kubectl not found)"; exit 3; }
command -v python3 >/dev/null 2>&1 || { echo "verify-operator-quiet: UNVERIFIED (python3 not found)"; exit 3; }

KINDS=(catalogs connections serviceprincipals sparkclusters streamapps lakehouses platforminstances)
singular() {
  case "$1" in
    catalogs) echo catalog ;;
    connections) echo connection ;;
    serviceprincipals) echo serviceprincipal ;;
    sparkclusters) echo sparkcluster ;;
    streamapps) echo streamapp ;;
    lakehouses) echo lakehouse ;;
    platforminstances) echo platforminstance ;;
    *) echo "$1" ;;
  esac
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

pods="$(kubectl -n "$NS" get pods -l app=stardelt-operator -o jsonpath='{.items[*].metadata.name}' 2>&1)" || {
  echo "verify-operator-quiet: UNVERIFIED (cannot list the operator pods in $NS: $pods)"; exit 3; }
read -r -a op_pods <<<"$pods"
[ "${#op_pods[@]}" -gt 0 ] || { echo "verify-operator-quiet: UNVERIFIED (no pod matches app=stardelt-operator in $NS)"; exit 3; }

# ---- snapshots -------------------------------------------------------------

snap_objects() {  # <dir>
  local dir="$1" kind line
  mkdir -p "$dir"
  for kind in "${KINDS[@]}"; do
    if line="$(kubectl -n "$NS" get "$kind" \
        -o jsonpath='{range .items[*]}{.metadata.name}={.metadata.resourceVersion}{"\n"}{end}' 2>&1)"; then
      printf '%s\n' "$line" | sed -n 's/=/ /p' | awk 'NF' | sort > "$dir/$kind"
    else
      printf '%s\n' "$line" > "$dir/$kind.failed"
    fi
  done
}

net_bytes() {  # <pod> -> bytes on stdout, MISSING when the summary cannot be read
  local pod="$1" node summary
  node="$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.spec.nodeName}' 2>&1)" || { echo MISSING; return 0; }
  [ -n "$node" ] || { echo MISSING; return 0; }
  summary="$(kubectl get --raw "/api/v1/nodes/$node/proxy/stats/summary" 2>&1)" || { echo MISSING; return 0; }
  printf '%s' "$summary" | python3 -c '
import json, sys
name = sys.argv[1]
try:
    doc = json.load(sys.stdin)
except ValueError:
    print("MISSING"); sys.exit(0)
for pod in doc.get("pods") or []:
    if (pod.get("podRef") or {}).get("name") == name:
        net = pod.get("network") or {}
        print(int(net.get("txBytes", 0) or 0) + int(net.get("rxBytes", 0) or 0))
        break
else:
    print("MISSING")
' "$pod" || echo MISSING
}

snap_net() {  # <file>
  local file="$1" pod bytes
  : > "$file"
  for pod in "${op_pods[@]}"; do
    bytes="$(net_bytes "$pod")"
    printf '%s %s\n' "$pod" "$bytes" >> "$file"
  done
}

reconcile_sum() {  # -> total reconciles over every operator pod, or UNREACHABLE
  local pod out url total=0 scraped=0
  for pod in "${op_pods[@]}"; do
    url="/api/v1/namespaces/$NS/pods/$pod:8082/proxy/metrics"
    if ! out="$(kubectl get --raw "$url" 2>&1)"; then continue; fi
    printf '%s\n' "$out" | grep -q '^stardelt_operator_reconciles_total{' || continue
    scraped=$((scraped + 1))
    total=$((total + $(printf '%s\n' "$out" | awk '/^stardelt_operator_reconciles_total\{/ {s += $2} END {print s+0}')))
  done
  if [ "$scraped" -eq 0 ]; then echo UNREACHABLE; else echo "$total"; fi
}

status_write_sum() {  # -> total status writes over every operator pod, or UNREACHABLE
  local pod out url total=0 scraped=0
  for pod in "${op_pods[@]}"; do
    url="/api/v1/namespaces/$NS/pods/$pod:8082/proxy/metrics"
    if ! out="$(kubectl get --raw "$url" 2>&1)"; then continue; fi
    printf '%s\n' "$out" | grep -q '^stardelt_operator_status_writes_total{' || continue
    scraped=$((scraped + 1))
    total=$((total + $(printf '%s\n' "$out" | awk '/^stardelt_operator_status_writes_total\{/ {s += $2} END {print s+0}')))
  done
  if [ "$scraped" -eq 0 ]; then echo UNREACHABLE; else echo "$total"; fi
}

# ---- measurement -----------------------------------------------------------

mkdir -p "$tmp/one" "$tmp/two"
snap_objects "$tmp/one"
snap_net "$tmp/net1"
recon1="$(reconcile_sum)"
write1="$(status_write_sum)"
sleep "$WINDOW"
snap_net "$tmp/net2"
recon2="$(reconcile_sum)"
write2="$(status_write_sum)"
snap_objects "$tmp/two"

problems=0
note() { echo "! $*"; }

# ---- objects: no resourceVersion may move ---------------------------------

total_objects=0
per_kind=""
changed_lines=""
for kind in "${KINDS[@]}"; do
  if [ -f "$tmp/one/$kind.failed" ]; then
    note "$kind: $(sed -n '1p' "$tmp/one/$kind.failed")"
    continue
  fi
  count="$(wc -l < "$tmp/one/$kind" | tr -d ' ')"
  if [ "$count" -eq 0 ]; then continue; fi
  total_objects=$((total_objects + count))
  per_kind="${per_kind:+$per_kind, }$kind $count"
  if ! diff -q "$tmp/one/$kind" "$tmp/two/$kind" >/dev/null; then
    changed_lines="${changed_lines}$(awk -v k="$(singular "$kind")" '
      NR==FNR { a[$1] = $2; next }
      { b[$1] = $2 }
      END {
        for (n in a) {
          if (!(n in b)) printf "✗ %s/%s: resourceVersion %s → gone (deleted)\n", k, n, a[n]
          else if (a[n] != b[n]) printf "✗ %s/%s: resourceVersion %s → %s\n", k, n, a[n], b[n]
        }
        for (n in b) if (!(n in a)) printf "✗ %s/%s: resourceVersion none → %s (created)\n", k, n, b[n]
      }' "$tmp/one/$kind" "$tmp/two/$kind")"$'\n'
  fi
done

if [ -z "$per_kind" ]; then
  echo "verify-operator-quiet: UNVERIFIED (none of ${KINDS[*]} has an object in $NS)"
  exit 3
fi

if [ -n "${changed_lines%$'\n'}" ]; then
  printf '%s' "$changed_lines"
  problems=$((problems + 1))
else
  echo "✓ $total_objects resources unchanged in $WINDOW s ($per_kind)"
fi

# ---- reconciles: fewer than two per object --------------------------------

if [ "$recon1" = UNREACHABLE ] || [ "$recon2" = UNREACHABLE ]; then
  echo "verify-operator-quiet: UNVERIFIED (the operator metrics endpoint :8082 answers nothing)"
  exit 3
fi
budget=$((2 * total_objects))
delta=$((recon2 - recon1))
if [ "$delta" -lt 0 ]; then
  note "the reconcile counter went backwards (an operator pod restarted inside the window); delta read as 0"
  delta=0
fi
if [ "$delta" -lt "$budget" ]; then
  echo "✓ $delta reconciles in $WINDOW s (budget $budget)"
else
  echo "✗ $delta reconciles in $WINDOW s (budget $budget)"
  problems=$((problems + 1))
fi

# ---- status writes: the direct counter, none in the window ------------------

if [ "$write1" = UNREACHABLE ] || [ "$write2" = UNREACHABLE ]; then
  echo "verify-operator-quiet: UNVERIFIED (the operator metrics endpoint :8082 has no stardelt_operator_status_writes_total)"
  exit 3
fi
wrote=$((write2 - write1))
if [ "$wrote" -lt 0 ]; then
  note "the status-write counter went backwards (an operator pod restarted inside the window); delta read as 0"
  wrote=0
fi
if [ "$wrote" -eq 0 ]; then
  echo "✓ no status write (counter $write1 → $write2)"
else
  echo "✗ the operator wrote $wrote status(es) while nothing changed"
  problems=$((problems + 1))
fi

# ---- CPU: under 50 m for every operator pod together ----------------------

if ! top_out="$(kubectl top pods -n "$NS" -l app=stardelt-operator --no-headers 2>&1)"; then
  echo "verify-operator-quiet: UNVERIFIED (kubectl top: $(printf '%s' "$top_out" | head -n 1))"
  exit 3
fi
if printf '%s' "$top_out" | grep -qi 'metrics api not available'; then
  echo "verify-operator-quiet: UNVERIFIED (kubectl top: Metrics API not available)"
  exit 3
fi
cpu_m="$(printf '%s\n' "$top_out" | awk '
  NF >= 3 {
    v = $2
    if (v ~ /m$/) { sub(/m$/, "", v); s += v + 0 }
    else { s += (v + 0) * 1000 }
  }
  END { printf "%d", s + 0 }')"
if [ "$cpu_m" -lt 50 ]; then
  echo "✓ operator CPU $cpu_m m (budget 50 m)"
else
  echo "✗ operator CPU $cpu_m m (budget 50 m)"
  problems=$((problems + 1))
fi

# ---- network: under 50 KB/s summed over the operator pods -----------------

network="$(python3 - "$tmp/net1" "$tmp/net2" "$eff_window" <<'PY'
import sys

first, second, window = sys.argv[1], sys.argv[2], max(int(sys.argv[3]), 1)


def read(path):
    out = {}
    with open(path) as handle:
        for line in handle:
            parts = line.split()
            if len(parts) == 2 and parts[1].isdigit():
                out[parts[0]] = int(parts[1])
            elif len(parts) == 2:
                out[parts[0]] = None
    return out


one, two = read(first), read(second)
skipped, delta, missing = [], 0, 0
for pod, start in one.items():
    if start is None or two.get(pod) is None:
        missing += 1
        continue
    step = two[pod] - start
    if step < 0:
        skipped.append(pod)
        continue
    delta += step
print(delta)
for pod in skipped:
    print("SKIP " + pod)
print("MISSING %d" % missing)
PY
)"
net_delta="$(printf '%s\n' "$network" | sed -n '1p')"
printf '%s\n' "$network" | sed -n 's/^SKIP /! /p' | while IFS= read -r line; do
  echo "$line restarted inside the window; its network delta is skipped (verify-restarts' business)"
done
net_missing="$(printf '%s\n' "$network" | sed -n 's/^MISSING //p')"
if [ "${net_missing:-0}" -gt 0 ]; then
  echo "verify-operator-quiet: UNVERIFIED (cannot read the kubelet stats summary of $net_missing operator pod(s))"
  exit 3
fi
if [ "${net_delta:-0}" -lt $((51200 * eff_window)) ]; then
  printf '✓ network %.1f KB/s (budget 50 KB/s)\n' \
    "$(awk -v b="${net_delta:-0}" -v w="$eff_window" 'BEGIN { printf "%.1f", b / w / 1024 }')"
else
  printf '✗ network %.1f KB/s (budget 50 KB/s)\n' \
    "$(awk -v b="${net_delta:-0}" -v w="$eff_window" 'BEGIN { printf "%.1f", b / w / 1024 }')"
  problems=$((problems + 1))
fi

# ---- verdict --------------------------------------------------------------

if [ "$problems" -gt 0 ]; then
  echo "verify-operator-quiet: FAIL ($problems row(s) outside the quiet budget of #274 rule 7)"
  exit 1
fi
echo "verify-operator-quiet: PASS"
