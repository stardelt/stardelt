#!/usr/bin/env bash
# The 0.0.4 exit list (stardelt/stardelt#401) as one command: one row per exit line, numbered
# 1-6, then a summary and one exit code. #561 gave exit line 5 its own command
# (`make verify-release`); this script is the "is 0.0.4 ready?" answer the tag (#430) starts from.
#
#   make exit-status
#   GH=/path/to/gh bash scripts/exit-status.sh   # the binary is ${GH:-gh}
#
# Exit 0 = PASS (no failing line and nothing unverified), 1 = FAIL (at least one line is red),
# 3 = UNVERIFIED (a probe could not see its subject: no gh, gh not signed in, a gh call that
# answered with an error). A line that passes prints `✓`, a failing line `✗`, a line only a
# person can decide `·`, a line no probe could reach `?`.
#
# Line 1 is manual by design: the fresh-node run's report is on #48 and its verdict is the
# maintainer's. It is printed and never fails the script.
#
# Line 6 scans every `image:` value in manifests/*.yaml and helm-values/*.yaml. Three values
# are not images: an empty one, a Helm template (`{{`), and a value the installer substitutes
# from a pinned manifest at apply time (`__SPARK_IMAGE__` in manifests/spark-demo-sandbox.yaml,
# replaced by the digest on manifests/spark.yaml's Spark image line in `make install-spark-demo`).
# They are skipped: the placeholder is checked where the digest it becomes is pinned.
set -euo pipefail

GH="${GH:-gh}"
PLATFORM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="stardelt/stardelt"
MILESTONE="0.0.4"

pass=0
fail=0
unverified_count=0
manual=0

unverified() { echo "UNVERIFIED ($*)"; exit 3; }
row_pass() { printf '✓ %s  %s\n' "$1" "$2"; pass=$((pass + 1)); }
row_fail() { printf '✗ %s  %s\n' "$1" "$2"; fail=$((fail + 1)); }
row_manual() { printf '· %s  MANUAL: %s\n' "$1" "$2"; manual=$((manual + 1)); }
row_unverified() { printf '? %s  UNVERIFIED: %s\n' "$1" "$2"; unverified_count=$((unverified_count + 1)); }

# A missing gh or a gh that is not signed in is not a red line: the probe cannot see its
# subject. Every gh row below would print UNVERIFIED for the same reason.
command -v "$GH" >/dev/null 2>&1 || unverified "$GH not found"
"$GH" auth status >/dev/null 2>&1 || unverified "$GH is not signed in"

gh_state() { "$GH" issue view "$1" --repo "$REPO" --json state --jq '.state' 2>/dev/null; }

# ---------------------------------------------------------------------------
# 1. A fresh node installs from one manifest and answers a Trino query: the
#    maintainer's verdict on the run reported in #48. Manual, never fatal.
# ---------------------------------------------------------------------------
row_manual 1 "the fresh-node run's report on #48 (the maintainer's OK)"

# ---------------------------------------------------------------------------
# 2. The operator installs the whole platform (epic #16).
# ---------------------------------------------------------------------------
if state="$(gh_state 16)"; then
  case "$state" in
    CLOSED) row_pass 2 "the operator installs the whole platform (#16 CLOSED)" ;;
    *) row_fail 2 "the operator installs the whole platform (#16 $state)" ;;
  esac
else
  row_unverified 2 "cannot read #16 (gh answered with an error)"
fi

# ---------------------------------------------------------------------------
# 3. No open P0 in the milestone.
# ---------------------------------------------------------------------------
if count="$("$GH" issue list --repo "$REPO" --milestone "$MILESTONE" --label P0 --state open \
  --json number --jq 'length' 2>/dev/null)"; then
  if [ "$count" = "0" ]; then
    row_pass 3 "no open P0 in $MILESTONE"
  else
    numbers="$("$GH" issue list --repo "$REPO" --milestone "$MILESTONE" --label P0 --state open \
      --json number --jq '.[].number' 2>/dev/null | sed 's/^/#/' | paste -sd' ' - || true)"
    row_fail 3 "no open P0 in $MILESTONE: $count open — ${numbers:-unknown}"
  fi
else
  row_unverified 3 "cannot list the P0 issues of $MILESTONE (gh answered with an error)"
fi

# ---------------------------------------------------------------------------
# 4. The demo's dbt run is green as an ordinary user: #338 (the demo) and
#    #393 (the dbt run) are both closed.
# ---------------------------------------------------------------------------
open_demo=()
demo_ok=1
for n in 338 393; do
  if state="$(gh_state "$n")"; then
    [ "$state" = "CLOSED" ] || { demo_ok=0; open_demo+=("#$n $state"); }
  else
    demo_ok=-1
    break
  fi
done
if [ "$demo_ok" -eq 1 ]; then
  row_pass 4 "the demo's dbt run is green as an ordinary user (#338 CLOSED, #393 CLOSED)"
elif [ "$demo_ok" -eq 0 ]; then
  row_fail 4 "the demo's dbt run is green as an ordinary user: ${open_demo[*]}"
else
  row_unverified 4 "cannot read #338 and #393 (gh answered with an error)"
fi

# ---------------------------------------------------------------------------
# 5. The lab passes the release checks: `make verify-release` (#561). Its last
#    line names the commit, the ratio and the log directory of a failure.
# ---------------------------------------------------------------------------
# The line that names the commit, the ratio and (on a failure) the log directory. `make`
# prints its own `*** Error 1` after it, so the last line is not the informative one.
release_summary() {
  local summary
  summary="$(printf '%s\n' "$1" | grep -E '^commit .* · .*passed · .*logs in ' | tail -n 1 || true)"
  [ -n "$summary" ] || summary="$(printf '%s\n' "$1" | tail -n 1)"
  printf '%s' "$summary"
}

if release_out="$(make -s --no-print-directory -C "$PLATFORM_DIR" verify-release 2>&1)"; then
  row_pass 5 "the lab passes the release checks ($(release_summary "$release_out"))"
else
  row_fail 5 "the lab passes the release checks ($(release_summary "$release_out"))"
fi

# ---------------------------------------------------------------------------
# 6. Every image is pinned by digest. `grep -n` over both value trees; the
#    value loses its indentation, list dash and quotes before the check.
# ---------------------------------------------------------------------------
offenders=0
while IFS=: read -r path lineno value; do
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value#image:}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  value="${value#\"}"; value="${value%\"}"
  value="${value#\'}"; value="${value%\'}"
  [ -n "$value" ] || continue
  # Not an image value: a template rendered at install time, or the installer's placeholder.
  if printf '%s' "$value" | grep -q '{{'; then continue; fi
  if printf '%s' "$value" | grep -qE '^__[A-Za-z0-9_]+__$'; then continue; fi
  if printf '%s' "$value" | grep -q '@sha256:'; then continue; fi
  printf '%s:%s %s\n' "$path" "$lineno" "$value"
  offenders=$((offenders + 1))
done < <(grep -n '^\s*\(- \)\?image: ' "$PLATFORM_DIR"/manifests/*.yaml "$PLATFORM_DIR"/helm-values/*.yaml)

if [ "$offenders" -eq 0 ]; then
  row_pass 6 "every image is pinned by digest (0 offenders)"
else
  row_fail 6 "every image is pinned by digest: $offenders offender(s)"
fi

echo "exit-status: $pass pass, $fail fail, $unverified_count unverified, $manual manual"
if [ "$fail" -gt 0 ]; then exit 1; fi
if [ "$unverified_count" -gt 0 ]; then exit 3; fi
exit 0
