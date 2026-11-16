#!/usr/bin/env bash
# A warehouse's three scale writers agree (platform#131 step A).
#
#   NS=stardelt bash scripts/verify-warehouse-scale.sh
#
# Nova writes the two annotations, the access-sync job writes status.scale from them and the
# worker Deployment, and the job scales the Deployment. This check reads all three and asserts
# they agree. Read-only: no pod, no patch, no scale.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NS="${NS:-${NAMESPACE:-stardelt}}"
for tool in kubectl python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "UNVERIFIED ($tool not found)"; exit 3; }
done
kubectl -n "$NS" get sqlwarehouses >/dev/null 2>&1 || {
  echo "UNVERIFIED (no SqlWarehouse CRD in namespace $NS)"; exit 3; }

tmp="$(mktemp -d)"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT
kubectl -n "$NS" get sqlwarehouses -o json >"$tmp/warehouses.json"
if kubectl -n "$NS" get deploy trino-worker -o json >"$tmp/worker.json" 2>/dev/null; then
  worker="$tmp/worker.json"
else
  worker="-"
  echo "· no deploy/trino-worker — the engine runs on the coordinator; only the annotations are checked"
fi

if ! python3 - "$tmp/warehouses.json" "$worker" <<'PY'
import json, sys

warehouses = json.load(open(sys.argv[1]))
deployment = json.load(open(sys.argv[2])) if sys.argv[2] != "-" else None
replicas = ((deployment or {}).get("spec") or {}).get("replicas")

problems = rows = 0
for item in warehouses.get("items", []):
    name = item["metadata"]["name"]
    status = item.get("status") or {}
    if status.get("phase") != "Ready":
        print(f"· {name}: status.phase={status.get('phase')!r} — skipped")
        continue
    rows += 1
    scale = status.get("scale")
    annotations = (item["metadata"].get("annotations") or {})
    found = []
    if scale is None:
        found.append("status.scale is missing — the access-sync job has not written this class")
        scale = {}
    if replicas is not None and scale.get("workers") != replicas:
        found.append(
            f"status.scale.workers={scale.get('workers')} but deploy/trino-worker targets {replicas}"
        )
    if replicas == 0 and scale.get("phase") != "Stopped":
        found.append(f"the engine is at 0 workers but status.scale.phase={scale.get('phase')!r}")
    if replicas and scale.get("phase") == "Stopped":
        found.append(f"status.scale.phase='Stopped' but deploy/trino-worker targets {replicas}")
    stamp = annotations.get("stardelt.io/last-query-at")
    if scale.get("lastQueryAt") != stamp:
        found.append(
            f"status.scale.lastQueryAt={scale.get('lastQueryAt')!r} but the annotation is {stamp!r}"
            " (a removed stamp must clear: platform#1539)"
        )
    raw = annotations.get("stardelt.io/queries-in-flight")
    try:
        in_flight = int(raw or 0)
    except ValueError:
        in_flight = None
        found.append(f"stardelt.io/queries-in-flight={raw!r} is not an integer")
    if in_flight is not None and scale.get("queriesInFlight") != in_flight:
        found.append(
            f"status.scale.queriesInFlight={scale.get('queriesInFlight')} but the annotation says {raw!r}"
        )
    if found:
        problems += len(found)
        for line in found:
            print(f"✗ {name}: {line}")
    else:
        print(
            f"✓ {name}: {scale.get('phase')} at {scale.get('workers')} worker(s), "
            f"in flight {scale.get('queriesInFlight')}, last query {scale.get('lastQueryAt') or '—'}"
        )

print(f"{rows} class(es) checked")
raise SystemExit(1 if problems else 0)
PY
then
  echo "FAIL (see the ✗ lines above)"
  exit 1
fi
echo "PASS (every Ready class agrees across the three writers)"
