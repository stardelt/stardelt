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
from datetime import datetime

warehouses = json.load(open(sys.argv[1]))
deployment = json.load(open(sys.argv[2])) if sys.argv[2] != "-" else None
replicas = ((deployment or {}).get("spec") or {}).get("replicas")


def parse_time(value):
    """An RFC 3339 stamp as Nova writes it (nanoseconds, "…Z"); None when it is not one.

    Nova writes nanosecond fractions the way protobuf does (…095338170Z); fromisoformat before
    3.11 refuses more than six digits, so the fraction is cut to microseconds first. Two stamps that
    differ only below that are indistinguishable here and are treated as lag, not as a disagreement.
    """
    if not isinstance(value, str):
        return None
    text = value.strip().replace("Z", "+00:00")
    head, dot, tail = text.partition(".")
    if dot:
        end = 0
        while end < len(tail) and tail[end].isdigit():
            end += 1
        text = f"{head}.{tail[:end][:6]}{tail[end:]}"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None

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
    pending = []
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
    # The two writers tick a minute (Nova) and two minutes (the access-sync job) apart, so a status
    # that is behind its annotation is normal lag, not a disagreement (platform#1551). Only the other
    # direction is a bug: the status names a stamp the object no longer carries (platform#1539).
    stamp = annotations.get("stardelt.io/last-query-at")
    status_stamp = scale.get("lastQueryAt")
    if status_stamp is None:
        if stamp is not None:
            pending.append(
                f"status.scale.lastQueryAt is unset but the annotation is {stamp!r} — the next job tick writes it"
            )
    elif stamp is None:
        found.append(
            f"status.scale.lastQueryAt={status_stamp!r} but the object carries no "
            "stardelt.io/last-query-at annotation (a removed stamp must clear: platform#1539)"
        )
    elif status_stamp != stamp:
        status_ts, stamp_ts = parse_time(status_stamp), parse_time(stamp)
        if status_ts is None or stamp_ts is None:
            found.append(
                f"stardelt.io/last-query-at={stamp!r} or status.scale.lastQueryAt={status_stamp!r} "
                "is not an RFC 3339 timestamp"
            )
        elif status_ts < stamp_ts:
            pending.append(
                f"status.scale.lastQueryAt={status_stamp!r} is behind the annotation {stamp!r} — normal lag"
            )
        elif status_ts == stamp_ts:
            pending.append(
                f"status.scale.lastQueryAt={status_stamp!r} differs from the annotation {stamp!r} "
                "below a microsecond — treated as lag"
            )
        else:
            found.append(
                f"status.scale.lastQueryAt={status_stamp!r} is ahead of the annotation {stamp!r}"
                " (the status names a stamp the object no longer has: platform#1539)"
            )
    # In-flight counts change with every query and only reach status.scale on the next job tick, so a
    # difference is expected; a stuck scaler shows up as the phase/worker contradictions above. A value
    # that is not a count at all is still a fault.
    raw = annotations.get("stardelt.io/queries-in-flight")
    try:
        in_flight = int(raw or 0)
    except ValueError:
        in_flight = None
        found.append(f"stardelt.io/queries-in-flight={raw!r} is not an integer")
    if in_flight is not None and scale.get("queriesInFlight") != in_flight:
        pending.append(
            f"status.scale.queriesInFlight={scale.get('queriesInFlight')} but the annotation says "
            f"{raw!r} — the next job tick writes it"
        )
    if found:
        problems += len(found)
        for line in found:
            print(f"✗ {name}: {line}")
    else:
        for line in pending:
            print(f"· {name}: {line} — pending")
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
