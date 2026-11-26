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
OPERATOR_DIR="${OPERATOR_DIR:-../operator}"

# name|platform file|operator file (relative to the operator checkout)[|line pattern]
images=(
  "nova|manifests/nova-deployment.yaml|crates/stardelt-operator/src/provision.rs"
  "loader|helm-values/airflow.yaml|crates/stardelt-operator/values/airflow.yaml"
  "spark|chart/files/spark-runtimes.json|crates/stardelt-operator/values/spark.yaml"
  "spark-auth|manifests/spark-connect-auth.yaml|crates/stardelt-operator/values/spark-connect-auth.yaml"
  "superset|helm-values/superset.yaml|crates/stardelt-operator/values/superset.yaml"
  "notebook|helm-values/jupyterhub.yaml|crates/stardelt-operator/values/jupyterhub.yaml|^[[:space:]]+tag: \"dev@sha256:"
  "notebook-airflow|helm-values/airflow.yaml|crates/stardelt-operator/values/airflow.yaml|jupyter-singleuser:dev@sha256:"
  "external-secrets|helm-values/external-secrets.yaml|crates/stardelt-operator/values/external-secrets.yaml"
  "spark-operator|helm-values/spark-operator.yaml|crates/stardelt-operator/values/spark-operator.yaml"
  "strimzi|helm-values/strimzi.yaml|crates/stardelt-operator/values/kafka.yaml"
  "kafka-broker|manifests/kafka.yaml|crates/stardelt-operator/src/kafka.rs"
  "lakekeeper|helm-values/lakekeeper.yaml|crates/stardelt-operator/values/lakekeeper.yaml|v0\\.15\\.0@sha256:"
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
platform_toplevel="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -n "$platform_toplevel" ] &&
   [ "$(git -C "$OPERATOR_DIR" rev-parse --show-toplevel 2>/dev/null || true)" = "$platform_toplevel" ]; then
  # Both folders are in one work tree: since D49 the operator and the platform are one repository,
  # so the pin and the golden value are one commit and that commit is this tree. Reading
  # `origin/main` here would judge the base branch while the platform side reads the new tree
  # (platform#175).
  note=" (working tree; one repository since D49)"
  operator_source="$OPERATOR_DIR (working tree; one repository since D49)"
elif git -C "$OPERATOR_DIR" rev-parse --git-dir >/dev/null 2>&1 &&
   git -C "$OPERATOR_DIR" rev-parse --verify --quiet origin/main >/dev/null; then
  # Name the revision the rows read: on a checkout that is behind, every ✗ below is about the
  # sibling checkout, not about this repository (platform#1640, #1641).
  operator_source="$OPERATOR_DIR origin/main $(git -C "$OPERATOR_DIR" rev-parse --short=7 origin/main) ($(git -C "$OPERATOR_DIR" log -1 --format=%cI origin/main))"
else
  note=" (working tree; no origin/main)"
  operator_source="$OPERATOR_DIR (working tree; no origin/main)"
fi
printf 'check-operator-pins: reading %s\n' "$operator_source"

fail=0
for image in "${images[@]}"; do
  IFS='|' read -r name platform_file operator_path pattern <<<"$image"
  operator_file="$tmpdir/$name.yaml"
  if [ -n "$note" ] || ! git -C "$OPERATOR_DIR" show "origin/main:./$operator_path" >"$operator_file"; then
    cp "$OPERATOR_DIR/$operator_path" "$operator_file"
    note="${note:- (working tree; no origin/main)}"
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

# The rows above compare ONE image per file. A values file carries up to seven (jupyterhub), and a
# pin that exists on one path and not the other after the first image was invisible to them, so
# compare the whole unique digest set of every shared file (platform#1640).
shared_files=(
  "airflow|helm-values/airflow.yaml|crates/stardelt-operator/values/airflow.yaml"
  "barman-cloud|helm-values/barman-cloud.yaml|crates/stardelt-operator/values/barman-cloud.yaml"
  "cnpg|helm-values/cnpg.yaml|crates/stardelt-operator/values/cnpg.yaml"
  "external-secrets|helm-values/external-secrets.yaml|crates/stardelt-operator/values/external-secrets.yaml"
  "jupyterhub|helm-values/jupyterhub.yaml|crates/stardelt-operator/values/jupyterhub.yaml"
  "kafka|helm-values/strimzi.yaml|crates/stardelt-operator/values/kafka.yaml"
  "openbao|helm-values/openbao.yaml|crates/stardelt-operator/values/openbao.yaml"
  "openfga|helm-values/openfga.yaml|crates/stardelt-operator/values/openfga.yaml"
  "seaweedfs|helm-values/seaweedfs.yaml|crates/stardelt-operator/values/seaweedfs.yaml"
  "spark-operator|helm-values/spark-operator.yaml|crates/stardelt-operator/values/spark-operator.yaml"
  "superset|helm-values/superset.yaml|crates/stardelt-operator/values/superset.yaml"
  "trino|helm-values/trino.yaml|crates/stardelt-operator/values/trino.yaml"
  "trino-identity|helm-values/trino-identity.yaml|crates/stardelt-operator/values/trino-identity.yaml"
)

for row in "${shared_files[@]}"; do
  IFS='|' read -r name platform_file operator_path <<<"$row"
  operator_file="$tmpdir/set-$name.yaml"
  if [ -n "$note" ] || ! git -C "$OPERATOR_DIR" show "origin/main:./$operator_path" >"$operator_file"; then
    cp "$OPERATOR_DIR/$operator_path" "$operator_file"
  fi
  platform_set="$tmpdir/$name.platform"
  operator_set="$tmpdir/$name.operator"
  grep -oE 'sha256:[0-9a-f]{64}' "$platform_file" | sort -u >"$platform_set" || true
  grep -oE 'sha256:[0-9a-f]{64}' "$operator_file" | sort -u >"$operator_set" || true
  if ! diff -q "$platform_set" "$operator_set" >/dev/null; then
    printf '✗ %-14s the two files do not carry the same digests (%s)%s\n' \
      "$name" "$operator_path" "$note" >&2
    diff "$platform_set" "$operator_set" \
      | sed -n 's/^< /  platform only: /p; s/^> /  operator only: /p' >&2 || true
    fail=1
  else
    printf '✓ %-14s %s digests, both paths%s\n' "$name" "$(wc -l <"$platform_set")" "$note"
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "check-operator-pins: FAIL (read $operator_source; if that is not the operator's newest main, run: git -C $OPERATOR_DIR fetch)" >&2
  exit 1
fi
echo "check-operator-pins: PASS (the operator's golden values agree with this repository's pins)"
exit 0
