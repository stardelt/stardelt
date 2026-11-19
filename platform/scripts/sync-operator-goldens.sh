#!/usr/bin/env bash
# Copy this repository's image pins into the operator's golden values.
#
# Since D49 the operator and the platform are one repository, so one commit holds both sides: the pin
# in `manifests/`/`helm-values/` and the operator's copy under `operator/crates/stardelt-operator/`.
# A pin move that updates only the platform side makes `platform / CI`'s goldens job red on main
# (platform#168) and leaves an operator install on the old image, so `scripts/pin-rollup.sh` calls
# this first and refuses a roll it cannot finish.
#
#   bash scripts/sync-operator-goldens.sh
#
# The operator side is judged with the same rules as scripts/check-operator-pins.sh, and the run ends
# with that check: exit 0 means every row agrees, 1 means a row still differs.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
OPERATOR_DIR="${OPERATOR_DIR:-../operator}"

# Same rule as check-operator-pins.sh's digest_of: a named line, else the first `digest:` line, else
# the first `sha256:` of the file.
digest_of() { # file [line pattern]
  local d=""
  if [ -n "${2-}" ]; then
    d="$(grep -E -- "$2" "$1" | grep -m1 -oE 'sha256:[0-9a-f]{64}' || true)"
  else
    d="$(grep -m1 -E '^[[:space:]]+digest: sha256:' "$1" | grep -oE 'sha256:[0-9a-f]{64}' || true)"
    if [ -z "$d" ]; then
      d="$(grep -m1 -oE 'sha256:[0-9a-f]{64}' "$1" || true)"
    fi
  fi
  printf '%s\n' "$d"
}

# Every image whose digest the rollup moves. name|platform file|operator file[|line pattern]
rows=(
  "nova|manifests/nova-deployment.yaml|crates/stardelt-operator/src/provision.rs"
  "loader|helm-values/airflow.yaml|crates/stardelt-operator/values/airflow.yaml"
  "spark|manifests/spark.yaml|crates/stardelt-operator/values/spark.yaml"
  "spark-auth|manifests/spark-connect-auth.yaml|crates/stardelt-operator/values/spark-connect-auth.yaml"
  "superset|helm-values/superset.yaml|crates/stardelt-operator/values/superset.yaml"
  "notebook|helm-values/jupyterhub.yaml|crates/stardelt-operator/values/jupyterhub.yaml|^[[:space:]]+tag: \"dev@sha256:"
  # The notebook image's fifth copy: PIPELINE_NOTEBOOK_IMAGE in the airflow values, on both
  # paths. The pattern names the `value:` line — the `- name: PIPELINE_NOTEBOOK_IMAGE` line
  # above it carries no digest.
  "notebook-airflow|helm-values/airflow.yaml|crates/stardelt-operator/values/airflow.yaml|jupyter-singleuser:dev@sha256:"
)

changed=""
for row in "${rows[@]}"; do
  IFS='|' read -r name platform_file operator_path pattern <<<"$row"
  want="$(digest_of "$platform_file" "${pattern-}")"
  operator_file="$OPERATOR_DIR/$operator_path"
  have="$(digest_of "$operator_file" "${pattern-}")"
  if [ -z "$want" ] || [ -z "$have" ]; then
    echo "sync-operator-goldens: UNVERIFIED ($name: a digest is unreadable in $platform_file or $operator_file)" >&2
    exit 3
  fi
  [ "$want" = "$have" ] && continue
  python3 - "$operator_file" "$have" "$want" <<'PY'
import sys

path, old, new = sys.argv[1:4]
text = open(path).read()
count = text.count(old)
if count == 0:
    sys.exit(f"sync-operator-goldens: {old} is not in {path}")
open(path, "w").write(text.replace(old, new))
print(f"  {path}: {old[:15]}… → {new[:15]}… ({count} occurrence(s))")
PY
  # The operator's unit tests repeat two of these digests as constants —
  # crates/stardelt-operator/src/values.rs asserts the loader (line 1663) and the Superset (line
  # 1468) images. A roll that moves the pin must move the constant too, or `cargo test` fails the
  # operator image build and the pin never follows (measured on PR #181: 762 passed, 1 failed).
  if grep -q -- "$have" "$OPERATOR_DIR/crates/stardelt-operator/src/values.rs" 2>/dev/null; then
    python3 - "$OPERATOR_DIR/crates/stardelt-operator/src/values.rs" "$have" "$want" <<'PY'
import sys

path, old, new = sys.argv[1:4]
text = open(path).read()
count = text.count(old)
open(path, "w").write(text.replace(old, new))
print(f"  {path}: {old[:15]}… → {new[:15]}… ({count} occurrence(s))")
PY
  fi
  changed="$changed $name"
done

if [ -z "$changed" ]; then
  echo "sync-operator-goldens: no drift (nova, loader, spark, spark-auth, superset, notebook, notebook-airflow)"
  exit 0
fi
echo "sync-operator-goldens: moved${changed}; proving it with check-operator-pins.sh"
bash scripts/check-operator-pins.sh
