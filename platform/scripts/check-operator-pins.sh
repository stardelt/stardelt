#!/usr/bin/env bash
# Compare the operator's golden image digests with this repository's pins, for every image both
# install paths share. The files live in two repositories, so no CI sees both on one revision.
#
#   scripts/check-operator-pins.sh
#
# The operator side is read from `origin/main` of the operator checkout, not from its working tree:
# the pin must be judged on the same revision the images come from, so a checkout that is one merge
# behind does not report a false mismatch (stardelt-platform#734). A checkout without an
# `origin/main` ref (tarball, shallow clone) falls back to the working tree and says so.
#
# Exit codes: 0 they agree or the operator checkout is absent (a local run outside the platform
# repository), 1 they differ, 3 a pin unreadable.
# Needs: bash, git, grep.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
OPERATOR_DIR="${OPERATOR_DIR:-../stardelt-operator}"

# name|platform file|operator file (relative to the operator checkout)[|line pattern]
images=(
  "nova|manifests/nova-deployment.yaml|crates/stardelt-operator/src/provision.rs"
  "loader|helm-values/airflow.yaml|crates/stardelt-operator/values/airflow.yaml"
  "spark|manifests/spark.yaml|crates/stardelt-operator/values/spark.yaml"
  "superset|helm-values/superset.yaml|crates/stardelt-operator/values/superset.yaml"
  "notebook|helm-values/jupyterhub.yaml|crates/stardelt-operator/values/jupyterhub.yaml|^[[:space:]]+tag: \"dev@sha256:"
  "external-secrets|helm-values/external-secrets.yaml|crates/stardelt-operator/values/external-secrets.yaml"
  "spark-operator|helm-values/spark-operator.yaml|crates/stardelt-operator/values/spark-operator.yaml"
  "strimzi|helm-values/strimzi.yaml|crates/stardelt-operator/values/kafka.yaml"
  "kafka-broker|manifests/kafka.yaml|crates/stardelt-operator/src/kafka.rs"
)

if [ ! -d "$OPERATOR_DIR" ]; then
  echo "check-operator-pins: SKIP (no operator checkout at $OPERATOR_DIR)"
  exit 0
fi

# The loader file pins with `digest: sha256:`; the Spark files carry the digest twice inside a
# `ghcr.io/stardelt/spark:dev@sha256:…` reference; the notebook file pins with
# `tag: "dev@sha256:…"` (quoted) but carries the hub's and the chart's own images above it
# (platform#1608), so a row that needs one of those names its line with the optional second
# argument (the row's 4th field) instead of relying on "first match". Nova has no values file on
# the operator side: its pin is the `NOVA_IMAGE` constant in provision.rs, whose only full digest
# it is. Read whichever form the file has: the named line, else the first `digest:` line, else
# the first `sha256:<64 hex>` match.
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

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

note=""
if git -C "$OPERATOR_DIR" rev-parse --git-dir >/dev/null 2>&1 &&
   git -C "$OPERATOR_DIR" rev-parse --verify --quiet origin/main >/dev/null; then
  :
else
  note=" (working tree; no origin/main)"
fi

fail=0
for image in "${images[@]}"; do
  IFS='|' read -r name platform_file operator_path pattern <<<"$image"
  operator_file="$tmpdir/$name.yaml"
  if [ -n "$note" ] || ! git -C "$OPERATOR_DIR" show "origin/main:$operator_path" >"$operator_file"; then
    cp "$OPERATOR_DIR/$operator_path" "$operator_file"
    note=" (working tree; no origin/main)"
  fi

  platform="$(digest_of "$platform_file" "${pattern-}")"
  operator="$(digest_of "$operator_file" "${pattern-}")"
  if [ -z "$platform" ] || [ -z "$operator" ]; then
    echo "check-operator-pins: UNVERIFIED ($name: a digest could not be read in $platform_file or $OPERATOR_DIR/$operator_path)" >&2
    exit 3
  fi
  if [ "$platform" = "$operator" ]; then
    printf '✓ %-7s both install paths run %s…%s\n' "$name" "${platform:0:15}" "$note"
  else
    printf '✗ %-7s platform %s…, operator %s… (%s)%s\n' \
      "$name" "${platform:0:15}" "${operator:0:15}" "$operator_path" "$note" >&2
    echo "  fix: bump $operator_path in the operator repository" >&2
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  exit 1
fi
echo "check-operator-pins: PASS (the operator's golden values agree with this repository's pins)"
exit 0
