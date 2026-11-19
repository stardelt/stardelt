#!/usr/bin/env bash
# Verify that the SeaweedFS S3 gateway speaks STS (stardelt#201 slice 2, spike stardelt#248).
#
#   NAMESPACE=stardelt bash scripts/verify-s3-sts.sh     (or: make verify-s3-sts)
#
# Three probes, run inside one one-shot `amazon/aws-cli` pod because the gateway is
# cluster-internal:
#   identity-put  the platform identity can write (the half that existed before STS)
#   session-in    an AssumeRole session with a table-shaped session policy can write inside
#                 its prefix
#   session-out   the same session is REFUSED under the sibling prefix of the same table
#
# The session policy is the one Lakekeeper will send: one table's bare prefix and `prefix/*`
# (a table-shaped policy needs both, spike stardelt#245). The role trusts any principal, which
# grants nothing the platform key does not already have — the restriction is the session policy.
#
# Exit 0 = PASS, 1 = FAIL (the pod reached Succeeded or Failed and its lines were judged),
# 3 = UNVERIFIED (no gateway, no Secret, or a pod that reached no terminal phase).
# No credential is printed: the CLI reads them from env vars and `--query` output is never echoed.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

NAMESPACE="${NS:-${NAMESPACE:-stardelt}}"   # `make` passes NS; NAMESPACE works for a direct call
# The pin scripts/create-backup-bucket.sh uses (same image, same digest).
S3_CHECK_IMAGE="${S3_CHECK_IMAGE:-amazon/aws-cli:2.17.0@sha256:643507c10ada7964ca6157b3d799f030b90577643da9955d319a77399ed80d73}"
pod="s3-sts-verify-$$"

command -v kubectl >/dev/null 2>&1 || { echo "verify-s3-sts: UNVERIFIED (kubectl not found)"; exit 3; }

if ! kubectl -n "$NAMESPACE" get secret stardelt-s3-creds >/dev/null 2>&1; then
  echo "verify-s3-sts: UNVERIFIED (Secret stardelt-s3-creds is missing — run 'make step-storage')"
  exit 3
fi

umask 077
tmp="$(mktemp -d)"

# shellcheck disable=SC2329  # invoked by the EXIT trap below (shellcheck >= 0.11 calls it unused)
cleanup() {
  kubectl -n "$NAMESPACE" delete pod "$pod" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT

cat > "$tmp/pod.yaml" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $pod
  namespace: $NAMESPACE
  labels: { app.kubernetes.io/part-of: stardelt }
spec:
  restartPolicy: Never
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 65534
    runAsGroup: 65534
    seccompProfile: { type: RuntimeDefault }
  containers:
    - name: sts
      image: $S3_CHECK_IMAGE
      securityContext:
        allowPrivilegeEscalation: false
        capabilities: { drop: ["ALL"] }
      command: ["sh", "-ec"]
      args:
        - |
          set -e
          EP="http://seaweedfs-s3.$NAMESPACE.svc.cluster.local:8333"
          B="sts-verify-\$(date -u +%s)"
          POLICY="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"s3:*\"],\"Resource\":[\"arn:aws:s3:::\$B/warehouse/ns/t\",\"arn:aws:s3:::\$B/warehouse/ns/t/*\"]}]}"
          s3() { aws --endpoint-url "\$EP" "\$@"; }
          cleanup() {
            s3 s3 rm "s3://\$B" --recursive >/dev/null 2>&1 || true
            s3 s3api delete-bucket --bucket "\$B" >/dev/null 2>&1 || true
          }
          trap cleanup EXIT
          n=0
          until s3 s3api list-buckets >/dev/null 2>&1; do
            n=\$((n + 1))
            if [ "\$n" -ge 30 ]; then echo "unreachable \$EP"; exit 1; fi
            sleep 2
          done
          s3 s3api create-bucket --bucket "\$B" >/dev/null
          if s3 s3api put-object --bucket "\$B" --key warehouse/ns/t/data/x --body /etc/hostname >/dev/null; then
            echo "pass identity-put"
          else
            echo "fail identity-put"
          fi
          creds="\$(s3 sts assume-role --role-arn arn:aws:iam::000000000000:role/stardelt --role-session-name verify --policy "\$POLICY" --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' --output text)" || { echo "fail assume"; exit 1; }
          set -- \$creds
          export AWS_ACCESS_KEY_ID=\$1 AWS_SECRET_ACCESS_KEY=\$2 AWS_SESSION_TOKEN=\$3
          if s3 s3api put-object --bucket "\$B" --key warehouse/ns/t/data/session --body /etc/hostname >/dev/null 2>&1; then
            echo "pass session-in"
          else
            echo "fail session-in"
          fi
          if s3 s3api put-object --bucket "\$B" --key warehouse/ns/t2/data/out --body /etc/hostname >/dev/null 2>&1; then
            echo "fail session-out"
          else
            echo "pass session-out"
          fi
      env:
        - name: HOME
          value: /tmp
        - name: AWS_ACCESS_KEY_ID
          valueFrom: { secretKeyRef: { name: stardelt-s3-creds, key: access-key } }
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom: { secretKeyRef: { name: stardelt-s3-creds, key: secret-key } }
        - name: AWS_DEFAULT_REGION
          valueFrom: { secretKeyRef: { name: stardelt-s3-creds, key: region } }
YAML

kubectl -n "$NAMESPACE" apply -f "$tmp/pod.yaml" >/dev/null

# A terminal phase is a judged run: `Succeeded` prints pass lines, `Failed` prints at least one
# `fail` line. Only a pod that reached neither phase is UNVERIFIED (image pull, unschedulable).
deadline=$((SECONDS + ${S3_STS_WAIT_SECONDS:-240}))
phase=""
while [ "$SECONDS" -lt "$deadline" ]; do
  phase="$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  case "$phase" in
    Succeeded|Failed) break ;;
  esac
  sleep "${S3_STS_POLL_SECONDS:-2}"
done
if [ "$phase" != "Succeeded" ] && [ "$phase" != "Failed" ]; then
  kubectl -n "$NAMESPACE" logs "$pod" 2>&1 || true
  echo "verify-s3-sts: UNVERIFIED (the probe pod reached no terminal phase — the gateway or its Secret is missing)"
  exit 3
fi

output="$(kubectl -n "$NAMESPACE" logs "$pod")"
printf '%s\n' "$output"

passed=0
failed=0
for probe in identity-put session-in session-out; do
  if grep -qx "pass $probe" <<<"$output"; then
    echo "✓ $probe"
    passed=$((passed + 1))
  else
    echo "✗ $probe"
    failed=$((failed + 1))
  fi
done
if grep -q '^fail ' <<<"$output"; then
  failed=$((failed + 1))
fi

if [ "$passed" -eq 3 ] && [ "$failed" -eq 0 ]; then
  echo "verify-s3-sts: PASS (3 of 3)"
  exit 0
fi
echo "verify-s3-sts: FAIL ($passed of 3)"
exit 1
