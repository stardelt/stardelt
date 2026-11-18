#!/usr/bin/env bash
# Restore the catalog's Postgres from its backup into a scratch namespace, check that
# the restored database has tables, and delete the namespace again (platform#1512).
# The check proves the catalog's contents, not only its schema: one psql call counts the
# relations, and the warehouse, namespace and tabular rows the catalog cannot work without
# (a restore that carried only _sqlx_migrations must fail).
#
# The backup is the barman-cloud plugin's object store (manifests/cnpg-objectstore.yaml,
# ObjectStore lakekeeper-pg-backups); manifests/cnpg-restore-proof.yaml reads it through a
# second ObjectStore in the restore namespace. Nothing here touches the source namespace
# except reading the S3 credentials Secret.
set -euo pipefail

SRC_NS=${NAMESPACE:-stardelt}
NS=${RESTORE_NS:-stardelt-restore}
TIMEOUT=${TIMEOUT:-900s}

if [ "$NS" = "$SRC_NS" ]; then
  echo "verify-restore: refusing to restore into the source namespace $NS" >&2
  exit 2
fi

if kubectl get namespace "$NS" >/dev/null 2>&1; then
  echo "verify-restore: namespace $NS exists — delete it or set RESTORE_NS" >&2
  exit 2
fi

if [ "${KEEP:-0}" = "1" ]; then
  trap 'echo "verify-restore: keeping namespace $NS (KEEP=1)"' EXIT
else
  trap 'kubectl delete namespace "$NS" --wait=false >/dev/null 2>&1 || true' EXIT
fi

kubectl create namespace "$NS" >/dev/null

# Never echo the Secret or write it to a file: the JSON goes through the pipe only.
kubectl -n "$SRC_NS" get secret stardelt-s3-creds -o json \
  | jq 'del(.metadata.namespace,.metadata.uid,.metadata.resourceVersion,.metadata.creationTimestamp,.metadata.managedFields)' \
  | kubectl -n "$NS" apply -f - >/dev/null

sed "s/namespace: stardelt-restore/namespace: $NS/" manifests/cnpg-restore-proof.yaml | kubectl apply -f - >/dev/null

if ! kubectl -n "$NS" wait cluster/lakekeeper-pg-restore --for=condition=Ready --timeout="$TIMEOUT"; then
  echo "verify-restore: FAIL (the restored cluster did not become Ready in $TIMEOUT)" >&2
  kubectl -n "$NS" logs -l cnpg.io/cluster=lakekeeper-pg-restore --all-containers --tail=30 >&2 || true
  exit 1
fi

read -r n warehouses namespaces tabulars < <(kubectl -n "$NS" exec lakekeeper-pg-restore-1 -c postgres -- \
  psql -U postgres -d lakekeeper -tAc \
  "select (select count(*) from information_schema.tables where table_schema not in ('pg_catalog','information_schema')), (select count(*) from public.warehouse), (select count(*) from public.namespace), (select count(*) from public.tabular)" \
  | tr '|' ' ')
n=${n//[[:space:]]/}; warehouses=${warehouses//[[:space:]]/}
namespaces=${namespaces//[[:space:]]/}; tabulars=${tabulars//[[:space:]]/}

if [ "${n:-0}" -gt 0 ] && [ "${warehouses:-0}" -gt 0 ] && [ "${tabulars:-0}" -gt 0 ]; then
  echo "verify-restore: PASS (catalog database restored, $n relations, $warehouses warehouses, $namespaces namespaces, $tabulars tabulars)"
  exit 0
fi

echo "verify-restore: FAIL (the restored catalog is empty: $n relations, $warehouses warehouses, $namespaces namespaces, $tabulars tabulars)" >&2
exit 1
