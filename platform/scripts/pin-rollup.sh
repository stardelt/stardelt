#!/usr/bin/env bash
# Roll the seven image pins (operator, nova, spark, loader, superset, notebook) to the digests
# their tags resolve to right now, in one PR.
# The fourth is the pipeline loader, `ghcr.io/stardelt/airflow:demo` (tag `demo`, not `dev`);
# the fifth is the BI image, `ghcr.io/stardelt/superset:dev` (two files); the sixth is the
# notebook image, `ghcr.io/stardelt/jupyter-singleuser:dev`, pinned in three files
# (helm-values/jupyterhub.yaml, helm-values/airflow.yaml, chart/values.yaml).
#   bash scripts/pin-rollup.sh            # local: rewrite the files, no git
#   OPEN_PR=1 bash scripts/pin-rollup.sh  # CI: commit, force-push branch, open/refresh the PR
#                                          # (a repository that forbids Actions from opening PRs
#                                          # keeps the branch and exits 0 — platform#648)
#                                          # With PIN_TOKEN (platform#1069) the push triggers the PR
#                                          # checks and auto-merge finishes the cycle.
#                                          # When auto-merge is unavailable the job waits for the PR
#                                          # checks and merges the roll itself, so the cycle needs
#                                          # no person.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

pin() { PRINT_DIGEST=1 ./scripts/pin-image.sh "$1"; }

extract_digest() {
  python3 - "$1" "$2" <<'PY'
import re, sys
path, kind = sys.argv[1], sys.argv[2]
text = open(path).read()
if kind == 'operator':
    m = re.search(r'^\s+tag:\s*[^\s@]+@(sha256:[0-9a-f]{64})\s*$', text, re.M)
elif kind == 'nova':
    m = re.search(r'^\s+image:\s*ghcr\.io/stardelt/nova:[^\s@]+@(sha256:[0-9a-f]{64})\s*$', text, re.M)
elif kind == 'spark':
    m = re.search(r'ghcr\.io/stardelt/spark:[^\s@]+@(sha256:[0-9a-f]{64})', text)
elif kind == 'airflow':
    m = re.search(r'^\s*digest:\s*(sha256:[0-9a-f]{64})\s*$', text, re.M)
elif kind == 'superset':
    m = re.search(r'^\s+tag:\s*"dev@(sha256:[0-9a-f]{64})"\s*$', text, re.M)
elif kind == 'jupyter':
    # The notebook marker, then the `tag:` line under it — comments sit between the two
    # in helm-values/jupyterhub.yaml, so this skips over them (the same rule
    # scripts/check-notebook-image-pin.sh's digest_after applies).
    m = re.search(r'name: ghcr\.io/stardelt/jupyter-singleuser\n'
                  r'(?:\s*#[^\n]*\n)*\s+tag: "dev@(sha256:[0-9a-f]{64})"', text)
else:
    sys.exit(f'unknown kind {kind}')
if not m:
    sys.exit(f'could not find digest for {kind} in {path}')
print(m.group(1))
PY
}

rewrite_files() {
  python3 - "$1" "$2" "$3" "$4" "$5" "$6" <<'PY'
import re, sys
op_path, nova_path, spark_path, op_digest, nova_digest, spark_digest = sys.argv[1:7]

# operator: helm-values/operator.yaml, tag: dev@sha256:...
text = open(op_path).read()
text, n = re.subn(
    r'(^\s+tag:\s*[^\s@]+)@sha256:[0-9a-f]{64}\s*$',
    lambda m: f'{m.group(1)}@{op_digest}',
    text, flags=re.M)
if n != 1:
    sys.exit(f'expected exactly one operator tag line in {op_path}, found {n}')
open(op_path, 'w').write(text)

# nova: manifests/nova-deployment.yaml
text = open(nova_path).read()
text, n = re.subn(
    r'(^\s+image:\s*ghcr\.io/stardelt/nova:[^\s@]+)@sha256:[0-9a-f]{64}\s*$',
    lambda m: f'{m.group(1)}@{nova_digest}',
    text, flags=re.M)
if n != 1:
    sys.exit(f'expected exactly one nova image line in {nova_path}, found {n}')
open(nova_path, 'w').write(text)

# spark: manifests/spark.yaml (two lines)
text = open(spark_path).read()
text, n = re.subn(
    r'(ghcr\.io/stardelt/spark:[^\s@]+)@sha256:[0-9a-f]{64}',
    lambda m: f'{m.group(1)}@{spark_digest}',
    text)
if n != 2:
    sys.exit(f'expected exactly two spark image refs in {spark_path}, found {n}')
open(spark_path, 'w').write(text)
PY
}

short() { printf '%s' "${1#sha256:}" | cut -c1-8; }

OP_FILE="helm-values/operator.yaml"
NOVA_FILE="manifests/nova-deployment.yaml"
OPERATOR_DIR="${OPERATOR_DIR:-../operator}"
SPARK_FILE="manifests/spark.yaml"
AIRFLOW_FILE="helm-values/airflow.yaml"
AIRFLOW_GROUP_FILE="manifests/airflow-group-roles.yaml"
SUPERSET_FILE="helm-values/superset.yaml"
SUPERSET_GROUP_FILE="manifests/superset-group-roles.yaml"
JUPYTER_FILE="helm-values/jupyterhub.yaml"

old_operator="$(extract_digest "$OP_FILE" operator)"
old_nova="$(extract_digest "$NOVA_FILE" nova)"
old_spark="$(extract_digest "$SPARK_FILE" spark)"
old_airflow="$(extract_digest "$AIRFLOW_FILE" airflow)"
old_superset="$(extract_digest "$SUPERSET_FILE" superset)"
old_jupyter="$(extract_digest "$JUPYTER_FILE" jupyter)"

operator_digest="$(pin stardelt-operator)"
nova_digest="$(pin nova)"
spark_digest="$(pin spark)"
airflow_digest="$(pin airflow)"
superset_digest="$(pin superset)"
jupyter_digest="$(pin jupyter)"

# Normalize: pin-image.sh may print whitespace
operator_digest="$(printf '%s' "$operator_digest" | tr -d '[:space:]')"
nova_digest="$(printf '%s' "$nova_digest" | tr -d '[:space:]')"
spark_digest="$(printf '%s' "$spark_digest" | tr -d '[:space:]')"
airflow_digest="$(printf '%s' "$airflow_digest" | tr -d '[:space:]')"
superset_digest="$(printf '%s' "$superset_digest" | tr -d '[:space:]')"
jupyter_digest="$(printf '%s' "$jupyter_digest" | tr -d '[:space:]')"

changes=()
[ "$old_operator" != "$operator_digest" ] && changes+=("operator $(short "$old_operator") → $(short "$operator_digest")")
[ "$old_nova" != "$nova_digest" ] && changes+=("nova $(short "$old_nova") → $(short "$nova_digest")")
[ "$old_spark" != "$spark_digest" ] && changes+=("spark $(short "$old_spark") → $(short "$spark_digest")")
[ "$old_airflow" != "$airflow_digest" ] && changes+=("airflow $(short "$old_airflow") → $(short "$airflow_digest")")
[ "$old_superset" != "$superset_digest" ] && changes+=("superset $(short "$old_superset") → $(short "$superset_digest")")
[ "$old_jupyter" != "$jupyter_digest" ] && changes+=("jupyter $(short "$old_jupyter") → $(short "$jupyter_digest")")

# A roll that moves a pin the operator embeds writes it into the operator's golden values, but the
# operator image it pins was built on main from the commit before. main keeps linear history, so
# the roll merges as a squash and is not rebuilt on its branch: the operator built on main from the
# squash commit is pinned by the next roll, one cycle later (stardelt/stardelt#1789, option B).

if [ "${#changes[@]}" -eq 0 ]; then
  echo "pins current (operator $(short "$operator_digest"), nova $(short "$nova_digest"), spark $(short "$spark_digest"), superset $(short "$superset_digest"), airflow $(short "$airflow_digest"), jupyter $(short "$jupyter_digest"))"
  exit 0
fi

rewrite_files "$OP_FILE" "$NOVA_FILE" "$SPARK_FILE" "$operator_digest" "$nova_digest" "$spark_digest"

# Spark is pinned in three files (manifest, runtime catalogue, the loader's task pod);
# its writer lives in pin-image.sh so the manual
# and the rollup path cannot drift.
if [ "$old_spark" != "$spark_digest" ]; then
  APP=spark WRITE_MANIFEST_ONLY=1 ./scripts/pin-image.sh >/dev/null
fi

# The loader is pinned three times in the two files below; its writer stays in
# pin-image.sh so the two callers cannot drift. WRITE_MANIFEST_ONLY skips every
# cluster call — this script runs on a GitHub runner with no kubeconfig.
if [ "$old_airflow" != "$airflow_digest" ]; then
  APP=airflow WRITE_MANIFEST_ONLY=1 ./scripts/pin-image.sh >/dev/null
fi

# Superset is pinned in two files; its writer lives in pin-image.sh so the manual and
# the rollup path cannot drift.
if [ "$old_superset" != "$superset_digest" ]; then
  APP=superset WRITE_MANIFEST_ONLY=1 ./scripts/pin-image.sh >/dev/null
fi

# The notebook image is pinned three times (helm-values/jupyterhub.yaml,
# helm-values/airflow.yaml, chart/values.yaml); its writer stays in pin-image.sh so the
# manual and the rollup path cannot drift.
if [ "$old_jupyter" != "$jupyter_digest" ]; then
  APP=jupyter WRITE_MANIFEST_ONLY=1 ./scripts/pin-image.sh >/dev/null
fi

for c in "${changes[@]}"; do echo "$c"; done

# One commit holds the operator's golden copies too (D49): a roll that moves a platform pin without
# its twin is red on main the moment the PR merges (#168), so write the twin first and let the check
# below refuse a roll it cannot finish.
# The operator's paths must also be in the git add below, or the twin is written and dropped.
bash scripts/sync-operator-goldens.sh

# The PR's own checks are suppressed for a bot push; the two pin guards are the only
# evidence for the loader and Superset halves. Run them even locally, and refuse to open
# a PR whose pins they reject.
airflow_check="$(bash scripts/check-airflow-pin.sh 2>&1)" || airflow_rc=$?
airflow_rc=${airflow_rc:-0}
superset_check="$(bash scripts/check-superset-roles.sh 2>&1)" || superset_rc=$?
superset_rc=${superset_rc:-0}
spark_check="$(bash scripts/check-spark-runtimes.sh 2>&1)" || spark_rc=$?
spark_rc=${spark_rc:-0}
printf '%s\n%s\n%s\n' "$airflow_check" "$superset_check" "$spark_check"
if [ "$airflow_rc" -ne 0 ] || [ "$superset_rc" -ne 0 ] || [ "$spark_rc" -ne 0 ]; then
  echo "pin-rollup: a pin guard failed (airflow $airflow_rc, superset $superset_rc, spark $spark_rc)" >&2
  exit 1
fi

if [ "${OPEN_PR:-0}" != "1" ]; then
  exit 0
fi

# CI path: run the two checks the workflow cannot trigger via bot push, then open/refresh the PR.
helm_out="$(helm lint chart 2>&1)" || helm_rc=$?
helm_rc=${helm_rc:-0}
parity_out="$(bash scripts/check-version-parity.sh 2>&1)" || parity_rc=$?
parity_rc=${parity_rc:-0}

body_file="$(mktemp "${TMPDIR:-/tmp}/pin-rollup-body.XXXXXX")"
trap 'rm -f "$body_file"' EXIT
{
  echo "## Digest changes"
  echo ""
  for c in "${changes[@]}"; do echo "- \`$c\`"; done
  echo ""
  if [ "${PIN_TOKEN_SET:-false}" = "true" ]; then
    echo "## Verification (also run by the required checks on this PR)"
    echo ""
    echo "The push was made with a user token, so the pull-request checks run on this PR. The rollup still ran these five checks itself: a red result stops the roll before the checks finish."
  else
    echo "## Verification (bot push suppresses pull-request checks)"
    echo ""
    echo "Pull-request checks do not run for pushes made with \`GITHUB_TOKEN\`, so the rollup ran these five checks itself. A reviewer can trust these results instead."
  fi
  echo ""
  echo "\`helm lint chart\` renders the umbrella chart's templates with its sub-charts not fetched (a GitHub release download 504 blocks the roll; they are unrelated to a digest pin). The merge to \`main\` runs the full CI \`Helm lint\` job with every dependency."
  echo ""
  echo "### helm lint chart (exit ${helm_rc})"
  echo '```'
  echo "$helm_out"
  echo '```'
  echo ""
  echo "### scripts/check-version-parity.sh (exit ${parity_rc})"
  echo '```'
  echo "$parity_out"
  echo '```'
  echo ""
  echo "### scripts/check-airflow-pin.sh (exit ${airflow_rc})"
  echo '```'
  echo "$airflow_check"
  echo '```'
  echo ""
  echo "### scripts/check-superset-roles.sh (exit ${superset_rc})"
  echo '```'
  echo "$superset_check"
  echo '```'
  echo ""
  echo "### scripts/check-spark-runtimes.sh (exit ${spark_rc})"
  echo '```'
  echo "$spark_check"
  echo '```'
} > "$body_file"

git config user.name "github-actions[bot]" 2>/dev/null || true
git config user.email "41898282+github-actions[bot]@users.noreply.github.com" 2>/dev/null || true

# The roll must describe a tree that contains origin/main: a branch created from the run's
# checkout pins images built from a commit main has moved past (#1644, where the operator pin
# predated #1629 and `verify-pins` still said PASS). A rebase is not the fix — the branch's
# images were built from its own commit — so this refuses and lets the push to main, which made
# it stale, start the next run from the new head.
require_current_main() {
  git fetch --quiet origin main
  if ! git merge-base --is-ancestor origin/main HEAD; then
    echo "pin-rollup: origin/main has moved past this run's checkout ($(git rev-parse --short HEAD)); refusing to pin images older than main. Re-run the rollup from main: gh workflow run platform-pin-rollup.yml --ref main (or wait for the next push to main)." >&2
    exit 1
  fi
}

require_current_main
branch="chore/pin-rollup"
git switch -C "$branch"
git add "$OP_FILE" "$NOVA_FILE" "$SPARK_FILE" "$AIRFLOW_FILE" "$AIRFLOW_GROUP_FILE" "$SUPERSET_FILE" "$SUPERSET_GROUP_FILE" \
  "$JUPYTER_FILE" chart/values.yaml chart/files/spark-runtimes.json \
  scripts/fixtures/spark-executor-pod.yaml \
  "$OPERATOR_DIR/crates/stardelt-operator/src/provision.rs" \
  "$OPERATOR_DIR/crates/stardelt-operator/values/airflow.yaml" \
  "$OPERATOR_DIR/crates/stardelt-operator/values/spark.yaml" \
  "$OPERATOR_DIR/crates/stardelt-operator/values/superset.yaml" \
  "$OPERATOR_DIR/crates/stardelt-operator/values/jupyterhub.yaml" \
  "$OPERATOR_DIR/crates/stardelt-operator/src/values.rs"
# The summary is generated, never hand-written, and regenerated whenever a pin moves after this
# point: the #746 branch rebuild replaces the operator digest, and an `--amend --no-edit` used to
# keep the pre-rebuild message. A merge commit then named a digest the tree does not carry (#1644).
rollup_message() {
  printf 'chore(pins): rollup image digests\n\n'
  for c in "${changes[@]}"; do printf -- '- %s\n' "$c"; done
}
git commit -m "$(rollup_message)"

# The checkout has no remote-tracking ref for the rolling branch, and
# `--force-with-lease` refuses to overwrite a ref it has not seen ("stale info"):
# fetch it first (it is absent on the very first run, which is fine).
if git ls-remote --exit-code --heads origin "$branch" >/dev/null; then
  git fetch --quiet origin "$branch"
fi
git push --force-with-lease origin "$branch"

# Reuse the open PR only. A merged or closed one for the same branch must not be
# edited: after a merge the branch is deleted and the next run opens a new PR.
title="chore(pins): rollup image digests"
pr_number="$(gh pr list --head "$branch" --state open --json number --jq '.[0].number // ""')"
if [ -n "$pr_number" ]; then
  gh pr edit "$pr_number" --title "$title" --body-file "$body_file"
  echo "updated PR #$pr_number for $branch"
else
  # The repository forbids Actions from creating pull requests (platform#648, option 2):
  # keep the pushed branch, say how to open the PR, and do not fail the roll for a
  # repository setting. The daily `pin-freshness` workflow files the visible issue
  # while the pins on `main` are behind.
  if pr_out="$(gh pr create --head "$branch" --base main --title "$title" --body-file "$body_file" 2>&1)"; then
    printf '%s\n' "$pr_out"
  else
    case "$pr_out" in
      *"not permitted to create"*|*"permitted to create or approve pull requests"*)
        echo "branch $branch pushed but no PR was opened: this repository forbids Actions from creating pull requests (platform#648)." >&2
        echo "open it by hand: gh pr create --head $branch --base main --title '$title'" >&2
        ;;
      *)
        printf '%s\n' "$pr_out" >&2
        exit 1
        ;;
    esac
  fi
fi

# With a user token the push triggers the pull-request checks, so auto-merge can
# finish the cycle without an admin merge (platform#1069). Without it the workflow
# falls back to GITHUB_TOKEN, no check reports, and a human merges.
if [ "${PIN_TOKEN_SET:-false}" = "true" ]; then
  pr_number="$(gh pr list --head "$branch" --state open --json number --jq '.[0].number // ""')"
  if [ -n "$pr_number" ]; then
    # `gh pr merge --auto` is refused when main has no protected-branch rule
    # ("Protected branch rules not configured", run 44334904654) and when the PR is
    # already clean ("Pull request is in clean status", run 44337019887). Both mean
    # nobody else will merge this roll: wait for the checks this push started, then
    # merge it here.
    # A squash, never a merge commit: main's ruleset requires linear history (stardelt/stardelt#1789).
    if auto_out="$(gh pr merge "$pr_number" --auto --squash 2>&1)"; then
      echo "auto-merge armed on PR #$pr_number: it merges when the required checks pass"
    else
      echo "auto-merge is not available; waiting for PR #$pr_number's checks (${auto_out//$'\n'/ })"
      checks=""
      for _ in $(seq 1 18); do
        checks="$(gh pr checks "$pr_number" --json name --jq 'length' 2>/dev/null || true)"
        [ -n "$checks" ] && [ "$checks" -gt 0 ] && break
        sleep 10
      done
      if [ -z "$checks" ] || [ "$checks" -eq 0 ]; then
        echo "no pull-request checks reported for PR #$pr_number after 3 minutes; leaving it open" >&2
        exit 1
      fi
      gh pr checks "$pr_number" --watch --fail-fast --interval 15
      gh pr merge "$pr_number" --squash --delete-branch
      echo "merged PR #$pr_number: the roll is on main"
    fi
  fi
else
  echo "PIN_TOKEN not set: pushed with GITHUB_TOKEN, so checks will not run and the PR needs a manual merge (platform#648 fallback)" >&2
fi

# These two also gate the roll: the PR body carries their results, and on a bot push
# (no PIN_TOKEN) a red workflow is the only signal left.
if [ "$helm_rc" -ne 0 ] || [ "$parity_rc" -ne 0 ]; then
  echo "rollup checks failed (helm lint chart: $helm_rc, version parity: $parity_rc)" >&2
  exit 1
fi
