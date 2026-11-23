#!/usr/bin/env bash
# Verify the SeaweedFS volume server still has a free volume slot (stardelt/stardelt#1354).
#
#   NAMESPACE=stardelt bash scripts/verify-storage-capacity.sh    (or: make verify-storage-capacity)
#
# A volume server has a fixed number of slots (`-max` on its command line) and one collection
# holds one S3 bucket's volumes — 7 per collection. The budget is therefore a *bucket* budget:
# the chart's default (7) fits one bucket, and the small install's 100 slots fit about 14. When
# the last slot is used, the master cannot create a volume for a *new* collection and the first
# write to a new bucket blocks in the gateway until the client's read timeout (#1350).
#
# Two facts are read, both from the live cluster (the values file is not the value in force):
#   budget  `-max` on the `weed volume` command line of the statefulset; absent means the chart
#           default, 7
#   state   the volume server's own /status: every volume, its collection, read-only or not
# and one more is judged: a collection named `sts-verify-<digits>` is what `make verify-s3-sts`
# leaves behind when it deletes the bucket but not the collection (#1350).
#
# `--list-leaked` prints those collection names, one per line, and nothing else; it exists so
# `make reclaim-sts-collections` deletes exactly the collections this check reports.
#
# Exit 0 = PASS, 1 = FAIL (a slot full or a leaked collection), 3 = UNVERIFIED (no kubectl, no
# volume pod, or the status endpoint does not answer). Read-only.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NS:-${NAMESPACE:-stardelt}}"   # `make` passes NS; NAMESPACE works for a direct call

list_leaked=0
for arg in "$@"; do
  case "$arg" in
    --list-leaked) list_leaked=1 ;;
    *) echo "verify-storage-capacity: unknown argument: $arg" >&2; exit 2 ;;
  esac
done

command -v kubectl >/dev/null 2>&1 \
  || { echo "verify-storage-capacity: UNVERIFIED (kubectl not found)"; exit 3; }

err="$(mktemp)"
trap 'rm -f "$err"' EXIT

unverified() {
  echo "verify-storage-capacity: UNVERIFIED (kubectl or the volume pod is missing)"
  if [ -s "$err" ]; then sed 's/^/  /' "$err" >&2; fi
  exit 3
}

# The volume pod is the statefulset's only replica: `<statefulset>-0`. `-o name` prints
# `statefulset.apps/seaweedfs-volume`, which `kubectl exec` cannot use, so take the bare name.
if ! sts="$(kubectl -n "$NAMESPACE" get statefulset seaweedfs-volume -o jsonpath='{.metadata.name}' 2>"$err")"; then
  unverified
fi
pod="${sts}-0"

# The budget: the `weed volume` command line is a JSON array whose flag string carries `-max N`.
if ! command_line="$(kubectl -n "$NAMESPACE" get statefulset seaweedfs-volume \
                       -o jsonpath='{.spec.template.spec.containers[0].command}' 2>"$err")"; then
  unverified
fi
max="$(printf '%s' "$command_line" | python3 -c '
import json, re, sys
command = json.load(sys.stdin)
words = " ".join(command) if isinstance(command, list) else str(command)
match = re.search(r"(?:^|\s)-max(?:=|\s+)([0-9]+)(?=\s|$)", words)
print(match.group(1) if match else "")
')"
if [ -z "$max" ]; then
  max=7
  echo "-max absent (chart default 7)"
fi

# The state: the volume server answers with one object per volume, each with its Collection.
if ! status="$(kubectl -n "$NAMESPACE" exec "$pod" -- sh -c 'wget -qO- "http://localhost:8080/status?pretty=y"' 2>"$err")"; then
  unverified
fi
summary="$(printf '%s' "$status" | python3 -c '
import collections, json, sys
status = json.load(sys.stdin)
volumes = status.get("Volumes") or []
counts = collections.Counter(v.get("Collection") or "(default)" for v in volumes)
read_only = sum(1 for v in volumes if v.get("ReadOnly"))
print(f"volumes {len(volumes)}")
print(f"readonly {read_only}")
for name in sorted(counts):
    print(f"collection {name} {counts[name]}")
')"

volumes=0
readonly=0
names=()
sizes=()
while read -r key first second; do
  case "$key" in
    volumes) volumes="$first" ;;
    readonly) readonly="$first" ;;
    collection) names+=("$first"); sizes+=("$second") ;;
  esac
done <<< "$summary"

leaked=()
for i in "${!names[@]}"; do
  if [[ "${names[$i]}" =~ ^sts-verify-[0-9]+$ ]]; then leaked+=("${names[$i]}"); fi
done

if [ "$list_leaked" = 1 ]; then
  for name in "${leaked[@]}"; do echo "$name"; done
  exit 0
fi

# The table is printed on a failure only: it is what names the collections to reclaim.
collection_table() {
  local i
  printf '   '
  for i in "${!names[@]}"; do
    if [ "$i" -gt 0 ]; then printf ', '; fi
    printf '%s %s' "${names[$i]}" "${sizes[$i]}"
  done
  printf '\n'
}

free=$((max - volumes))
failed=0
if [ "$volumes" -ge "$max" ]; then
  echo "✗ volume slots $volumes/$max used, $readonly read-only, ${#names[@]} collections"
  echo "✗ no free volume slot: a new S3 bucket's first write will hang (#1350)"
  failed=1
fi
if [ "${#leaked[@]}" -gt 0 ]; then
  if [ "$failed" = 0 ]; then
    echo "✗ volume slots $volumes/$max used, $readonly read-only, ${#names[@]} collections"
  fi
  echo "✗ ${#leaked[@]} leaked sts-verify collection(s): run make reclaim-sts-collections"
  failed=1
fi
if [ "$failed" = 1 ]; then
  collection_table
  echo "verify-storage-capacity: FAIL"
  exit 1
fi

echo "verify-storage-capacity: PASS ($volumes/$max slots, $free free, $readonly read-only, ${#names[@]} collections)"
