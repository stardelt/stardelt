#!/usr/bin/env bash
# Prove that the live Deployments, StatefulSets and DaemonSets, the group-roles CronJobs, the two
# spawned-notebook pins, the three helper CronJobs, the Keycloak Deployment and the two alpine
# CronJobs run the images this repository pins.
#
# Why: the recurring failure is a correct pin plus a Deployment that was never rolled to it
# (stardelt-platform#697, #704, #503, #501, #509, #510). verify-images proves a pin is readable,
# check-pins proves the registry tag has not moved; neither asks the cluster what it runs.
# The group-roles CronJobs are applied by make step-airflow, and the two notebook pins are not
# Deployments: the JupyterHub spawn image lives in Secret hub and the pipeline notebook image in
# Airflow's PIPELINE_NOTEBOOK_IMAGE, both pinned by hand. The helper CronJobs (access-sync,
# demo-reset, lakehouse-backup) are applied by hand too, and nothing read them until #1567: their
# digests could sit unapplied while every check stayed green (platform#1563). Keycloak
# (manifests/keycloak.yaml, quay.io) and the alpine CronJobs openbao-group-policies and
# spark-confmap-janitor joined them in #1579. The images the Superset, Airflow, OpenBao and
# JupyterHub charts bring themselves (#1604, #1605) are StatefulSets, one DaemonSet and the three
# JupyterHub Deployments hub, proxy and user-scheduler; they are read by the tag they carry
# (`sts-tag:`, `ds-tag:`) or by their repository (`name-tag:`), because those files name several
# tags with digests. The CNPG operator, the barman-cloud plugin and Reloader (#1616) are three
# more Deployments read by the tag they carry (`tag:`). The Kafka broker is a StrimziPodSet pod and
# the entity operator a two-container Deployment, read by `refpod:`/`refdeploy:` (#1631).
#
# The Spark Connect front (manifests/spark-connect-auth.yaml) joined them in #231: Envoy comes
# from Docker Hub, so its row is `any-image`.
#
#   scripts/verify-pins.sh
#   ROWS=access-sync,demo-reset scripts/verify-pins.sh   # only these rows (the test uses it)
#
# Exit codes: 0 all match, 1 at least one differs, 3 the check proved nothing (no kubectl, no
# Deployment, a pin that cannot be parsed). Needs: kubectl, python3.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
NAMESPACE="${NAMESPACE:-stardelt}"

command -v kubectl >/dev/null || { echo "verify-pins: UNVERIFIED (kubectl not found)" >&2; exit 3; }
command -v python3 >/dev/null || { echo "verify-pins: UNVERIFIED (python3 not found)" >&2; exit 3; }

# The rows read the pin files from this working tree. A checkout behind origin/main compares the
# cluster with old pins and prints a confident FAIL (2026-11-09: the main checkout was three days
# behind). Name the two revisions, like pin-image.sh does for the operator checkout; do not fail.
head_rev="$(git rev-parse HEAD 2>/dev/null || true)"
main_rev="$(git rev-parse --verify --quiet origin/main 2>/dev/null || true)"
if [ -n "$head_rev" ] && [ -n "$main_rev" ] && [ "$head_rev" != "$main_rev" ]; then
  printf '! verify-pins: this checkout is at %s, origin/main is %s — the pins below may be stale (git pull)\n' \
    "${head_rev:0:7}" "${main_rev:0:7}" >&2
fi

# app | Deployment | pin file | how to read the pin
rows=(
  "nova|nova|manifests/nova-deployment.yaml|image"
  "stardelt-operator|stardelt-operator|helm-values/operator.yaml|tag"
  "spark|spark-connect|manifests/spark.yaml|image"
  "spark-connect-auth|spark-connect-auth|manifests/spark-connect-auth.yaml|any-image"
  "superset|superset|helm-values/superset.yaml|tag"
  "superset-worker|superset-worker|helm-values/superset.yaml|tag"
  "airflow|airflow-api-server|helm-values/airflow.yaml|worker-tag"
  "notebook|hub|helm-values/jupyterhub.yaml|hub"
  "notebook-task|airflow-dag-processor|helm-values/airflow.yaml|env"
  "group-roles|airflow-group-roles|manifests/airflow-group-roles.yaml|cronjob"
  "superset-group-roles|superset-group-roles|manifests/superset-group-roles.yaml|cronjob"
  "access-sync|access-sync|manifests/access-sync.yaml|manifest-images"
  "demo-reset|demo-reset|manifests/demo-reset.yaml|manifest-images"
  "lakehouse-backup|lakehouse-backup|manifests/object-store-backup.yaml|manifest-images"
  "openbao-group-policies|openbao-group-policies|manifests/openbao-group-policies.yaml|manifest-images"
  "spark-confmap-janitor|spark-confmap-janitor|manifests/spark-confmap-janitor.yaml|manifest-images"
  "keycloak|keycloak|manifests/keycloak.yaml|any-image"
  "lakekeeper|lakekeeper|helm-values/lakekeeper.yaml|tag:v0.15.0:make step-lakekeeper"
  "trino|trino-coordinator|helm-values/trino.yaml|tag:484:make step-trino"
  "seaweedfs-s3|seaweedfs-s3|helm-values/seaweedfs.yaml|tag:4.25:make step-storage"
  "openfga|openfga|helm-values/openfga.yaml|tag:v1.22.1:make step-openfga"
  "cnpg-operator|cnpg-cloudnative-pg|helm-values/cnpg.yaml|tag:1.30.1:make step-cnpg"
  "barman-cloud|barman-cloud-plugin-barman-cloud|helm-values/barman-cloud.yaml|tag:v0.16.0:make step-barman-cloud-plugin"
  "reloader|reloader-reloader|helm-values/reloader.yaml|tag:v1.4.24:make step-reloader"
  "airflow-statsd|airflow-statsd|helm-values/airflow.yaml|tag:v0.30.0:make step-airflow"
  "superset-postgresql|superset-postgresql|helm-values/superset.yaml|sts-tag:14.17.0-debian-12-r3:make step-superset"
  "superset-redis-master|superset-redis-master|helm-values/superset.yaml|sts-tag:7.0.10-debian-11-r4:make step-superset"
  "airflow-postgresql|airflow-postgresql|helm-values/airflow.yaml|sts-tag:16.1.0-debian-11-r15:make step-airflow"
  "openbao|openbao|helm-values/openbao.yaml|sts-tag:2.7.2:make openbao"
  "user-placeholder|user-placeholder|helm-values/jupyterhub.yaml|sts-tag:3.10.1:make install-notebooks"
  "continuous-image-puller|continuous-image-puller|helm-values/jupyterhub.yaml|ds-tag:3.10.1:make install-notebooks"
  "hub-image|hub|helm-values/jupyterhub.yaml|name-tag:quay.io/jupyterhub/k8s-hub:make install-notebooks"
  "proxy|proxy|helm-values/jupyterhub.yaml|name-tag:quay.io/jupyterhub/configurable-http-proxy:make install-notebooks"
  "user-scheduler|user-scheduler|helm-values/jupyterhub.yaml|name-tag:registry.k8s.io/kube-scheduler:make install-notebooks"
  "external-secrets|external-secrets|helm-values/external-secrets.yaml|tag:v2.15.0:make external-secrets"
  "external-secrets-cert-controller|external-secrets-cert-controller|helm-values/external-secrets.yaml|tag:v2.15.0:make external-secrets"
  "external-secrets-webhook|external-secrets-webhook|helm-values/external-secrets.yaml|tag:v2.15.0:make external-secrets"
  "spark-operator-controller|spark-operator-controller|helm-values/spark-operator.yaml|tag:2.5.2:make install-spark-operator"
  "spark-operator-webhook|spark-operator-webhook|helm-values/spark-operator.yaml|tag:2.5.2:make install-spark-operator"
  "strimzi-cluster-operator|strimzi-cluster-operator|helm-values/strimzi.yaml|tag:1.3.1:make install-kafka"
  "kafka-broker|kafka-kafka-0|manifests/kafka.yaml|refpod:quay.io/strimzi/kafka:1.3.1-kafka-4.3.2"
  "kafka-entity-operator|kafka-entity-operator|manifests/kafka.yaml|refdeploy:quay.io/strimzi/operator:1.3.1"
)

pins="$(python3 - "${rows[@]}" <<'PY'
import re, sys

for row in sys.argv[1:]:
    app, deploy, path, kind = row.split("|")
    text = open(path, encoding="utf-8").read()
    if kind == "hub":
        # The spawn image is the only `dev@` tag in the file. The chart's own images
        # (#1605) are `4.4.5@…` and the hub image comes first, so an unanchored `tag:`
        # regex read the hub image and the spawn row could never pass (#1608 regression).
        m = re.search(r"^\s*tag:\s*[\"']?dev@(sha256:[0-9a-f]{64})", text, re.M)
    elif kind == "env":
        m = re.search(r"name:\s*PIPELINE_NOTEBOOK_IMAGE\s*\n\s*value:\s*\S+@(sha256:[0-9a-f]{64})", text, re.M)
    elif kind == "cronjob":
        m = re.search(
            r"^\s*image:\s*(?:>-\s*)?ghcr\.io/stardelt/[^\s\"']+@(sha256:[0-9a-f]{64})",
            text,
            re.M,
        )
    elif kind.startswith("tag:"):
        # `tag:<tag>[:<hint>]`: the file may carry other `tag:` lines with digests
        # (lakekeeper's OPA sidecar, superset's Postgres and Redis, #1594), so the row
        # names the tag it means instead of taking the first one.
        parts = kind.split(":", 2)
        m = re.search(
            r"^\s*tag:\s*[\"']?" + re.escape(parts[1]) + r"@(sha256:[0-9a-f]{64})",
            text,
            re.M,
        )
    elif kind.startswith(("sts-tag:", "ds-tag:")):
        # A StatefulSet/DaemonSet image (the chart defaults #1594/#1604/#1605 pinned). The file
        # may carry other tags, so the row names the tag it means, as `tag:` does.
        parts = kind.split(":", 2)
        m = re.search(
            r"^\s*tag:\s*[\"']?" + re.escape(parts[1]) + r"@(sha256:[0-9a-f]{64})",
            text,
            re.M,
        )
    elif kind.startswith("name-tag:"):
        # `name: <repository>` on one line and `tag: <tag>@<digest>` on the next. Anchoring on
        # the repository is what keeps `tag:4.4.5` from reading the hub image for the
        # image-awaiter (both are 4.4.5 in helm-values/jupyterhub.yaml).
        parts = kind.split(":", 2)
        m = re.search(
            r"^\s*name:\s*" + re.escape(parts[1]) + r"\s*\n\s*tag:\s*[\"']?\S+@(sha256:[0-9a-f]{64})",
            text,
            re.M,
        )
    elif kind.startswith(("refpod:", "refdeploy:")):
        # `ref<live>:<repository>:<tag>`: the pin is written as a full image reference in a
        # manifest (`image: "quay.io/strimzi/kafka:1.3.1-kafka-4.3.2@sha256:…"`), not as a
        # standalone `tag:` line, so the row names the repository and tag it means (#1631).
        parts = kind.split(":", 2)
        m = re.search(
            r"^\s*image:\s*[\"']?" + re.escape(parts[1] + ":" + parts[2]) + r"@(sha256:[0-9a-f]{64})",
            text,
            re.M,
        )
    elif kind in ("tag", "value"):
        m = re.search(r"^\s*(?:tag|value):\s*[\"']?\S+@(sha256:[0-9a-f]{64})", text, re.M)
    elif kind == "worker-tag":
        # The Airflow KubernetesExecutor pod image. It is the second image in the file and
        # the first `value:` with a digest is not it once `images.statsd.tag` exists (#1594),
        # so the row names the env var instead of taking the file's first `tag:`/`value:`.
        m = re.search(
            r"name:\s*AIRFLOW__KUBERNETES_EXECUTOR__WORKER_CONTAINER_TAG\s*\n\s*value:\s*[\"']?[^\"'@\n]+@(sha256:[0-9a-f]{64})",
            text,
            re.M,
        )
    elif kind == "manifest-images":
        found = re.findall(r"^\s*image:\s*(\S+)\s*$", text, re.M)
        if not found or any("@sha256:" not in image for image in found):
            print(f"{app}\t{deploy}\t{path}\tERROR: every image must be pinned by digest in {path}")
            continue
        print(f"{app}\t{deploy}\t{path}\t{','.join(sorted(found))}\tmanifest-images")
        continue
    elif kind == "any-image":
        # A Deployment image from any registry (keycloak is quay.io, not ghcr.io/stardelt). The
        # first pinned image: line wins, so a manifest with a second image would need a new kind.
        m = re.search(r"^\s*image:\s*(\S+@sha256:[0-9a-f]{64})\s*$", text, re.M)
    else:
        m = re.search(r"^\s*image:\s*(ghcr\.io/stardelt/[^\s\"']+@sha256:[0-9a-f]{64})", text, re.M)
    if not m:
        print(f"{app}\t{deploy}\t{path}\tERROR: no digest pin")
        continue
    digest = m.group(1).rsplit("@", 1)[-1] if kind in ("image", "any-image") else m.group(1)
    hint = (
        kind.split(":", 2)[2]
        if kind.startswith(("tag:", "sts-tag:", "ds-tag:", "name-tag:")) and kind.count(":") > 1
        else ""
    )
    if kind.startswith(("refpod:", "refdeploy:")):
        hint = "kubectl apply -f " + path
    print(f"{app}\t{deploy}\t{path}\t{digest}\t{kind}\t{hint}")
PY
)"

status=0
while IFS=$'\t' read -r app deploy path digest kind row_hint; do
  if [ -n "${ROWS:-}" ] && [[ ",$ROWS," != *",$app,"* ]]; then continue; fi
  if [[ "$digest" == ERROR:* ]]; then
    echo "? $app $digest"; status=3; continue
  fi
  if [ "$kind" = "manifest-images" ]; then
    live="$(kubectl -n "$NAMESPACE" get cronjob "$deploy" -o jsonpath='{range .spec.jobTemplate.spec.template.spec.initContainers[*]}{.image}{"\n"}{end}{range .spec.jobTemplate.spec.template.spec.containers[*]}{.image}{"\n"}{end}' 2>/dev/null || true)"
    if [ -z "$live" ]; then
      echo "? $app no CronJob $NAMESPACE/$deploy"; status=3; continue
    fi
    want="$(printf '%s' "$digest" | tr ',' '\n' | sort)"
    got="$(printf '%s\n' "$live" | sed '/^$/d' | sort)"
    if [ "$want" = "$got" ]; then
      printf '✓ %-18s %d image(s) match %s\n' "$app" "$(printf '%s\n' "$want" | wc -l)" "$path"
    else
      printf '✗ %-18s live %s != pin %s (%s) — kubectl apply -f %s\n' \
        "$app" "$(printf '%s' "$got" | tr '\n' ' ')" "$(printf '%s' "$want" | tr '\n' ' ')" "$path" "$path"
      status=1
    fi
    continue
  fi
  case "$kind" in
    hub) live="$(kubectl -n "$NAMESPACE" get secret hub \
             -o jsonpath='{.data.values\.yaml}' 2>/dev/null | base64 -d \
             | grep -m1 'tag: dev@sha256:' || true)" ;;
    env) live="$(kubectl -n "$NAMESPACE" get deploy "$deploy" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="PIPELINE_NOTEBOOK_IMAGE")].value}' 2>/dev/null || true)" ;;
    cronjob) live="$(kubectl -n "$NAMESPACE" get cronjob "$deploy" \
             -o jsonpath='{.spec.jobTemplate.spec.template.spec.containers[0].image}' 2>/dev/null || true)" ;;
    sts-tag:*) live="$(kubectl -n "$NAMESPACE" get sts "$deploy" -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)" ;;
    ds-tag:*) live="$(kubectl -n "$NAMESPACE" get ds "$deploy" -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)" ;;
    refpod:*) live="$(kubectl -n "$NAMESPACE" get pod "$deploy" -o jsonpath='{.spec.containers[0].image}' 2>/dev/null || true)" ;;
    refdeploy:*) live="$(kubectl -n "$NAMESPACE" get deploy "$deploy" -o jsonpath='{range .spec.template.spec.containers[*]}{.image}{"\n"}{end}' 2>/dev/null || true)" ;;
    *) live="$(kubectl -n "$NAMESPACE" get deploy "$deploy" -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)" ;;
  esac
  if [[ -z "$live" ]]; then
    echo "? $app no pin in the live configuration"; status=3; continue
  fi
  if [[ "$kind" == refdeploy:* ]]; then
    bad=""
    while IFS= read -r image; do
      [ -z "$image" ] && continue
      [[ "$image" == *"@$digest" ]] || bad="$image"
    done <<<"$live"
    if [ -z "$bad" ]; then
      printf '✓ %-18s %s\n' "$app" "${digest:0:19}… matches $path"
    else
      printf '✗ %-18s live %s != pin %s (%s) — kubectl apply -f %s\n' \
        "$app" "$bad" "$digest" "$path" "$path"
      status=1
    fi
    continue
  fi
  if [[ "$live" == *"@$digest" ]]; then
    printf '✓ %-18s %s\n' "$app" "${digest:0:19}… matches $path"
  else
    if [[ "$app" == superset* ]]; then
      hint="run make step-superset"
    else
      case "$kind" in
        hub) hint="run make install-notebooks" ;;
        env) hint="run make step-airflow" ;;
        cronjob) hint="run make step-airflow" ;;
        sts-tag:*|ds-tag:*|name-tag:*) hint="${row_hint:-run ./scripts/pin-image.sh $app}" ;;
        refpod:*|refdeploy:*) hint="kubectl apply -f $path" ;;
        any-image) hint="kubectl apply -f $path" ;;
        tag:*) hint="${row_hint:-run ./scripts/pin-image.sh $app}" ;;
        *) hint="run ./scripts/pin-image.sh $app" ;;
      esac
    fi
    live_digest="${live##*@}"
    printf '✗ %-18s live %s… != pin %s… (%s) — %s\n' \
      "$app" "${live_digest:0:19}" "${digest:0:19}" "$path" "$hint"
    status=1
  fi
done <<< "$pins"

# Every container that carries the Airflow worker tag must carry the pinned digest. The main
# containers get it from helm-values/airflow.yaml; the chart's log-groomer sidecars once got a
# direct `kubectl set env` that Helm's three-way merge keeps, because the sidecar's env is in
# neither the old nor the new manifest (platform#1687).
airflow_worker_digest="$(grep -A1 '^  - name: AIRFLOW__KUBERNETES_EXECUTOR__WORKER_CONTAINER_TAG$' \
  helm-values/airflow.yaml | sed -n 's/.*demo@\(sha256:[0-9a-f]\{64\}\).*/\1/p' | head -1)"
if [ -z "$airflow_worker_digest" ]; then
  echo "? airflow-worker-tag no pinned worker tag in helm-values/airflow.yaml"; status=3
else
  stray="$(kubectl -n "$NAMESPACE" get deploy -o json 2>/dev/null | python3 -c '
import json, sys
want = "@" + sys.argv[1]
for item in json.load(sys.stdin)["items"]:
    name = item["metadata"]["name"]
    if not name.startswith("airflow-"):
        continue
    for c in item["spec"]["template"]["spec"]["containers"]:
        for e in c.get("env") or []:
            if e.get("name") == "AIRFLOW__KUBERNETES_EXECUTOR__WORKER_CONTAINER_TAG":
                value = e.get("value") or ""
                if not value.endswith(want):
                    print(name + "/" + c["name"] + " " + value[-64:])
' "$airflow_worker_digest")"
  if [ -n "$stray" ]; then
    printf '✗ %-18s other worker tag: %s\n' airflow-worker-tag "$(printf '%s' "$stray" | tr '\n' ';')"
    status=1
  else
    printf '✓ %-18s every container carries %s…\n' airflow-worker-tag "${airflow_worker_digest:0:19}"
  fi
fi

case "$status" in
  0) echo "verify-pins: PASS (live Deployments, StatefulSets and DaemonSets, both group-roles CronJobs, the spawned-notebook pins, the three helper CronJobs, the Keycloak Deployment, the CNPG operator, the barman-cloud plugin, Reloader, the two alpine CronJobs, the Kafka broker and entity operator carry the checked-in pins, and every Airflow container carries the pinned worker tag)" ;;
  1) echo "verify-pins: FAIL (a pin does not match the live configuration)" >&2 ;;
  *) echo "verify-pins: UNVERIFIED (a pin or Deployment could not be read)" >&2 ;;
esac
exit "$status"
