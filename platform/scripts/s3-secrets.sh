#!/usr/bin/env bash
# Generate the data-plane credentials once, against the current kube-context.
# Called by `make install` and `make install-core` before SeaweedFS is installed;
# safe to re-run.
#
#   NAMESPACE=stardelt bash scripts/s3-secrets.sh
#
# A credential is generated only when its Secret does not exist, so a re-run never
# rotates anything. To rotate, delete the Secrets named below and install again
# (docs: operations/rotating-secrets). Values go through mode-0600 temp files and
# are never put on a command line or printed. Read one back deliberately with e.g.
#   kubectl -n stardelt get secret stardelt-s3-creds -o jsonpath='{.data.access-key}' | base64 -d
#
# Secrets (all labelled app.kubernetes.io/part-of=stardelt):
#   stardelt-s3-creds     access-key secret-key endpoint bucket region sts-signing-key
#                         read by Trino, Lakekeeper bootstrap, Airflow, CNPG backups; the
#                         gateway signs its STS session tokens with `sts-signing-key`
#   seaweedfs-s3-config   seaweedfs_s3_config (S3 identities + STS/IAM half)
#                         read by the SeaweedFS S3 gateway (helm-values/seaweedfs.yaml,
#                         s3.existingConfigSecret and s3.extraArgs `-iam.config`)
#   seaweedfs-admin-ui   user password   read by the SeaweedFS admin UI (helm-values/seaweedfs.yaml, admin.secret)
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NAMESPACE:-stardelt}"

umask 077
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# rand BYTES -> 2*BYTES hex characters. Hex needs no escaping in JSON or YAML.
rand() { openssl rand -hex "$1"; }

# s3_config_json <access-key> <secret-key> <sts-signing-key>: the gateway's one configuration
# document. It carries the identity (unchanged) and the IAM half the gateway reads with
# `-iam.config` (stardelt#201 slice 2). Keep it byte-identical to the chart's version
# (chart/templates/s3-credentials.yaml); test_s3_sts_config.py compares the two as JSON.
# The role trusts exactly one principal — the identity named in `identities`, which the gateway
# turns into `arn:aws:iam::000000000000:user/<name>` (weed/s3api/auth_credentials.go's
# generatePrincipalArn). A future S3 identity therefore cannot assume the all-buckets role;
# naming "*" here would let every identity the platform adds assume it.
s3_config_json() {
  printf '%s\n' \
    "{\"identities\":[{\"name\":\"stardelt\",\"credentials\":[{\"accessKey\":\"$1\",\"secretKey\":\"$2\"}],\"actions\":[\"Admin\",\"Read\",\"Write\"]}]," \
    " \"sts\":{\"signingKey\":\"$3\"}," \
    " \"policy\":{\"storeType\":\"memory\",\"defaultEffect\":\"Deny\"}," \
    " \"policies\":[{\"name\":\"stardelt-all\",\"document\":{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:*\"],\"Resource\":[\"arn:aws:s3:::*\"]}]}}]," \
    " \"roles\":[{\"roleName\":\"stardelt\",\"roleArn\":\"arn:aws:iam::000000000000:role/stardelt\",\"trustPolicy\":{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::000000000000:user/stardelt\"},\"Action\":[\"sts:AssumeRole\"]}]},\"attachedPolicies\":[\"stardelt-all\"]}]}"
}

# Print-only mode for the unit test (scripts/tests/test_s3_sts_config.py): the same function
# without a cluster. It runs before the tool checks below because it needs neither kubectl nor
# openssl — and a shell function must be defined before it is called.
if [ "${1:-}" = "--print-config" ]; then
  [ "$#" -eq 4 ] || { echo "usage: s3-secrets.sh --print-config <access-key> <secret-key> <sts-signing-key>" >&2; exit 2; }
  s3_config_json "$2" "$3" "$4"
  exit 0
fi

command -v openssl >/dev/null 2>&1 || { echo "s3-secrets: openssl not found" >&2; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "s3-secrets: kubectl not found" >&2; exit 1; }

secret_exists() { kubectl -n "$NAMESPACE" get secret "$1" >/dev/null 2>&1; }

secret_field() {
  kubectl -n "$NAMESPACE" get secret "$1" -o "jsonpath={.data.$2}" | base64 -d
}

label_secret() {
  kubectl -n "$NAMESPACE" label secret "$1" app.kubernetes.io/part-of=stardelt >/dev/null
}

kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE" >/dev/null

# --- S3 credentials -------------------------------------------------------------
fresh_s3=0
if secret_exists stardelt-s3-creds; then
  echo "› Secret stardelt-s3-creds exists — kept"
  access_key="$(secret_field stardelt-s3-creds access-key)"
  secret_key="$(secret_field stardelt-s3-creds secret-key)"
else
  access_key="$(rand 10)"   # 20 characters, like an AWS access key id
  secret_key="$(rand 20)"   # 40 characters
  {
    printf 'access-key=%s\n' "$access_key"
    printf 'secret-key=%s\n' "$secret_key"
    printf 'endpoint=http://seaweedfs-s3.%s.svc.cluster.local:8333\n' "$NAMESPACE"
    printf 'bucket=lakehouse\n'
    printf 'region=us-east-1\n'
  } > "$tmp/s3.env"
  kubectl -n "$NAMESPACE" create secret generic stardelt-s3-creds --from-env-file="$tmp/s3.env" >/dev/null
  label_secret stardelt-s3-creds
  echo "› Secret stardelt-s3-creds created (generated)"
  fresh_s3=1
fi

# The gateway signs its STS session tokens with this key. SeaweedFS wants base64 text, so the
# Secret carries it exactly as SeaweedFS reads it. An install from before this change has no key;
# add it once (additive, never a rotation).
sts_key=""
if secret_exists stardelt-s3-creds; then
  sts_key="$(secret_field stardelt-s3-creds sts-signing-key 2>/dev/null || true)"
fi
if [ -z "$sts_key" ]; then
  sts_key="$(openssl rand -base64 32 | tr -d '\n')"
  printf '{"stringData":{"sts-signing-key":"%s"}}\n' "$sts_key" > "$tmp/sts.json"
  kubectl -n "$NAMESPACE" patch secret stardelt-s3-creds --type merge --patch-file "$tmp/sts.json" >/dev/null
  echo "› Secret stardelt-s3-creds gained sts-signing-key"
fi

# The SeaweedFS S3 gateway reads its identities from a JSON document. It must carry
# exactly the key pair above, so it is derived from stardelt-s3-creds, never generated
# separately. New credentials replace a stale config; an existing config is kept.
config_current=0
if secret_exists seaweedfs-s3-config; then
  if secret_field seaweedfs-s3-config seaweedfs_s3_config | grep -q '"sts"'; then config_current=1; fi
fi
if [ "$fresh_s3" = 1 ] || [ "$config_current" = 0 ]; then
  s3_config_json "$access_key" "$secret_key" "$sts_key" > "$tmp/seaweedfs_s3_config"
  kubectl -n "$NAMESPACE" delete secret seaweedfs-s3-config --ignore-not-found >/dev/null
  kubectl -n "$NAMESPACE" create secret generic seaweedfs-s3-config \
    --from-file=seaweedfs_s3_config="$tmp/seaweedfs_s3_config" >/dev/null
  label_secret seaweedfs-s3-config
  echo "› Secret seaweedfs-s3-config written (identities + STS, derived from stardelt-s3-creds)"
else
  echo "› Secret seaweedfs-s3-config exists with STS — kept"
fi

# --- SeaweedFS admin UI login -----------------------------------------------------------
# The object store's web UI for administrators (helm-values/seaweedfs.yaml, `admin`). Generated once,
# never rotated by a re-run, never printed.
if secret_exists seaweedfs-admin-ui; then
  echo "› Secret seaweedfs-admin-ui exists — kept"
else
  printf 'user=admin\npassword=%s\n' "$(rand 16)" > "$tmp/admin.env"
  kubectl -n "$NAMESPACE" create secret generic seaweedfs-admin-ui --from-env-file="$tmp/admin.env" >/dev/null
  label_secret seaweedfs-admin-ui
  echo "› Secret seaweedfs-admin-ui created (generated)"
fi
