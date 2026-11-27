//! A `SparkCluster` (internal#161): the Spark Operator's `SparkConnect` server that runs it.
//!
//! The operator only translates. When `spec.state` is `Running` it applies one `SparkConnect`;
//! when it is `Stopped` it deletes that object again. The pods belong to the Spark Operator.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{
    ConfigMap, Event, EventSource, ObjectReference, Pod, ServiceAccount,
};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::rbac::v1::RoleBinding;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{OwnerReference, Time};
use kube::ResourceExt;
use kube::api::{
    Api, DynamicObject, ListParams, ObjectMeta, Patch, PatchParams, PostParams, TypeMeta,
};
use kube::runtime::controller::Action;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::controller::Context;
use crate::crd::{SparkCluster, SparkClusterSize, SparkClusterSpec};
use crate::objects::{self, FIELD_MANAGER};

/// The ConfigMap that carries the runtimes the platform ships (slice 4 of internal#161).
pub const RUNTIME_CATALOGUE: &str = "stardelt-spark-runtimes";

/// One runtime entry of the catalogue: the image, the Spark version and the service account.
#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeEntry {
    pub image: String,
    #[serde(rename = "sparkVersion")]
    pub spark_version: String,
    #[serde(rename = "serviceAccount")]
    pub service_account: String,
    #[serde(default, rename = "sparkConf")]
    pub spark_conf: BTreeMap<String, String>,
}

/// Read one runtime from the catalogue ConfigMap. A ConfigMap without data, or without the
/// requested key, is an error the controller reports as `Failed`.
pub fn runtime_entry(cm: &ConfigMap, runtime: &str) -> Result<RuntimeEntry, String> {
    let Some(data) = cm.data.as_ref() else {
        return Err(format!(
            "the runtime catalogue {RUNTIME_CATALOGUE} is missing"
        ));
    };
    let Some(value) = data.get(runtime) else {
        let known = data.keys().cloned().collect::<Vec<_>>().join(", ");
        return Err(format!("unknown runtime {runtime}; known: {known}"));
    };
    serde_json::from_str(value).map_err(|e| format!("runtime {runtime} is not valid JSON: {e}"))
}

/// The numbers of a size class: driver cores and memory, executor cores and memory, and the
/// maximum number of executors.
pub fn preset(size: SparkClusterSize) -> (i32, &'static str, i32, &'static str, i32) {
    match size {
        SparkClusterSize::Small => (1, "2Gi", 1, "2Gi", 2),
        SparkClusterSize::Medium => (2, "4Gi", 2, "4Gi", 4),
        SparkClusterSize::Large => (4, "8Gi", 4, "8Gi", 8),
    }
}

/// The Spark operator's webhook wants Spark's own unit (`2g`, `512m`), not a Kubernetes
/// quantity (`2Gi`, `512Mi`). The suffixes mean the same binary multiples.
fn webhook_memory(quantity: &str) -> Result<String, String> {
    if let Some(number) = quantity.strip_suffix("Gi") {
        Ok(format!("{number}g"))
    } else if let Some(number) = quantity.strip_suffix("Mi") {
        Ok(format!("{number}m"))
    } else {
        Err(format!(
            "unsupported memory unit in `{quantity}`; use Mi or Gi"
        ))
    }
}

fn labels(name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            FIELD_MANAGER.to_string(),
        ),
        ("stardelt.io/spark-cluster".to_string(), name.to_string()),
    ])
}

/// Where a `SparkCluster` driver reads its catalog token: the projected volume's mount path.
/// The client names the file (`rest.auth.token-file`) and reads it per request, so a rotation
/// needs no restart (#492, #769).
pub const CATALOG_TOKEN_MOUNT: &str = "/var/run/secrets/stardelt-catalog";
/// The audience the catalog's Kubernetes authentication verifies
/// (`LAKEKEEPER__KUBERNETES_AUTHENTICATION_AUDIENCE` in `platform/helm-values/lakekeeper.yaml`,
/// twin `operator/crates/stardelt-operator/values/lakekeeper.yaml`). The OIDC audience
/// `lakekeeper` is a different token type and is refused on a ServiceAccount token (#538).
pub const CATALOG_TOKEN_AUDIENCE: &str = "stardelt-catalog-workload";

/// The `SparkConnect` a `SparkCluster` becomes. Pure: no client, no cluster state.
/// The driver also carries a projected ServiceAccount token with audience `stardelt-catalog-workload` at
/// /var/run/secrets/stardelt-catalog, an identity that gives no Kubernetes rights (#492).
pub fn spark_connect(
    name: &str,
    namespace: &str,
    spec: &SparkClusterSpec,
    runtime: &RuntimeEntry,
    owner: &OwnerReference,
) -> Result<DynamicObject, String> {
    let (driver_cores, driver_mem, exec_cores, exec_mem, max_executors) = preset(spec.size);
    let driver_cores = spec
        .driver
        .as_ref()
        .and_then(|d| d.cores)
        .unwrap_or(driver_cores);
    let driver_mem = spec
        .driver
        .as_ref()
        .and_then(|d| d.memory.clone())
        .unwrap_or_else(|| driver_mem.to_string());
    let exec_cores = spec
        .executor
        .as_ref()
        .and_then(|e| e.cores)
        .unwrap_or(exec_cores);
    let exec_mem = spec
        .executor
        .as_ref()
        .and_then(|e| e.memory.clone())
        .unwrap_or_else(|| exec_mem.to_string());
    let driver_mem = webhook_memory(&driver_mem)?;
    let exec_mem = webhook_memory(&exec_mem)?;
    let max_executors = spec.max_executors.unwrap_or(max_executors);

    let mut env = Vec::new();
    for var in &spec.env {
        if var.connection_ref.is_some() {
            return Err("connectionRef is not supported yet (internal#161 slice 7)".to_string());
        }
        if let Some(value) = &var.value {
            env.push(json!({ "name": var.name, "value": value }));
        }
    }

    let mut spark_conf = runtime.spark_conf.clone();
    for (key, value) in &spec.spark_conf {
        spark_conf.insert(key.clone(), value.clone());
    }
    spark_conf.insert(
        "spark.kubernetes.namespace".to_string(),
        namespace.to_string(),
    );
    let image = spec.image.clone().unwrap_or_else(|| runtime.image.clone());

    let driver_sa = spec
        .owner
        .group
        .as_deref()
        .map(crate::spark::cluster_driver_for)
        .unwrap_or_else(|| crate::spark::CLUSTER_DRIVER.to_string());

    Ok(DynamicObject {
        types: Some(TypeMeta {
            api_version: "sparkoperator.k8s.io/v1alpha1".into(),
            kind: "SparkConnect".into(),
        }),
        metadata: ObjectMeta {
            name: Some(format!("spc-{name}")),
            namespace: Some(namespace.to_string()),
            labels: Some(labels(name)),
            owner_references: Some(vec![owner.clone()]),
            ..Default::default()
        },
        data: json!({
            "spec": {
                "sparkVersion": runtime.spark_version,
                "image": image,
                "sparkConf": spark_conf,
                "dynamicAllocation": {
                    "enabled": true,
                    "minExecutors": 0,
                    "maxExecutors": max_executors,
                },
                "server": {
                    "cores": driver_cores,
                    "memory": driver_mem,
                    "template": {
                        "spec": {
                            "serviceAccountName": driver_sa,
                            "securityContext": {
                                "runAsNonRoot": true,
                                "runAsUser": 185,
                                "fsGroup": 185,
                                "seccompProfile": {"type": "RuntimeDefault"}
                            },
                            "volumes": [{
                                "name": "catalog-token",
                                "projected": {
                                    "sources": [{
                                        "serviceAccountToken": {
                                            "audience": CATALOG_TOKEN_AUDIENCE,
                                            "expirationSeconds": 3600,
                                            "path": "token",
                                        }
                                    }]
                                }
                            }],
                            "containers": [{
                                "name": "spark-kubernetes-driver",
                                "env": env,
                                "securityContext": {
                                    "allowPrivilegeEscalation": false,
                                    "capabilities": {"drop": ["ALL"]}
                                },
                                "volumeMounts": [{
                                    "name": "catalog-token",
                                    "mountPath": CATALOG_TOKEN_MOUNT,
                                    "readOnly": true,
                                }],
                            }],
                        }
                    }
                },
                "executor": {
                    "cores": exec_cores,
                    "memory": exec_mem,
                    "instances": 0,
                    "template": {
                        "spec": {
                            // The executors run user code and keep the runtime's token-less
                            // ServiceAccount (platform#1096); the driver creates the executor
                            // pods (operator#773). The Spark operator's webhook refuses this
                            // identity inside sparkConf, so it belongs in this template.
                            "serviceAccountName": runtime.service_account.clone(),
                            "securityContext": {
                                "runAsNonRoot": true,
                                "runAsUser": 185,
                                "fsGroup": 185,
                                "seccompProfile": {"type": "RuntimeDefault"}
                            },
                            "containers": [{
                                "name": "spark-kubernetes-executor",
                                "env": env,
                                "securityContext": {
                                    "allowPrivilegeEscalation": false,
                                    "capabilities": {"drop": ["ALL"]}
                                },
                            }],
                        }
                    }
                },
            }
        }),
    })
}

fn owner_reference(cluster: &SparkCluster) -> OwnerReference {
    OwnerReference {
        api_version: "stardelt.io/v1alpha1".into(),
        kind: "SparkCluster".into(),
        name: cluster.name_any(),
        uid: cluster.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

fn is_not_found(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(r) if r.code == 404)
}

/// Remove a group's driver identity once nothing uses it (#493, #1112).
///
/// `spark-cluster-driver-<group>` and its RoleBinding are applied per cluster and no object
/// owns them, so a cluster whose owner group changes — and a deleted cluster — would leave
/// them behind. Only the group driver is ever removed: the shared `spark-cluster-driver`
/// belongs to every person-owned cluster. Another live `SparkCluster` of the same group keeps
/// the identity, because both run as it.
async fn prune_driver_identity(
    client: &kube::Client,
    clusters: &Api<SparkCluster>,
    namespace: &str,
    cluster: &str,
    group: &str,
) -> Result<(), kube::Error> {
    let driver = crate::spark::cluster_driver_for(group);
    if driver == crate::spark::CLUSTER_DRIVER {
        return Ok(());
    }
    let users = clusters.list(&Default::default()).await?;
    if users.items.iter().any(|other| {
        other.name_any() != cluster
            && other.metadata.deletion_timestamp.is_none()
            && other.spec.owner.group.as_deref() == Some(group)
    }) {
        return Ok(());
    }
    match Api::<ServiceAccount>::namespaced(client.clone(), namespace)
        .delete(&driver, &Default::default())
        .await
    {
        Ok(_) => {}
        Err(e) if is_not_found(&e) => {}
        Err(e) => return Err(e),
    }
    match Api::<RoleBinding>::namespaced(client.clone(), namespace)
        .delete(&driver, &Default::default())
        .await
    {
        Ok(_) => {}
        Err(e) if is_not_found(&e) => {}
        Err(e) => return Err(e),
    }
    tracing::info!(cluster = %cluster, %namespace, driver = %driver, "spark cluster driver identity removed");
    Ok(())
}

/// The driver identity is not owned by the cluster, so somebody must remove it after the
/// object is gone; this finalizer keeps the object readable until `prune_driver_identity` ran.
const DRIVER_IDENTITY_FINALIZER: &str = "stardelt.io/driver-identity";

/// Replace `metadata.finalizers` with exactly `finalizers` (a merge patch replaces the whole
/// list, so it never appends a duplicate).
async fn patch_spark_cluster_finalizers(
    clusters: &Api<SparkCluster>,
    cluster: &SparkCluster,
    finalizers: Vec<String>,
) -> Result<(), kube::Error> {
    clusters
        .patch(
            &cluster.name_any(),
            &PatchParams::default(),
            &Patch::Merge(json!({ "metadata": { "finalizers": finalizers } })),
        )
        .await?;
    Ok(())
}

/// Add the finalizer when it is missing; a cluster that already carries it is not patched again.
async fn ensure_driver_identity_finalizer(
    clusters: &Api<SparkCluster>,
    cluster: &SparkCluster,
) -> Result<(), kube::Error> {
    if cluster
        .finalizers()
        .iter()
        .any(|f| f == DRIVER_IDENTITY_FINALIZER)
    {
        return Ok(());
    }
    let mut finalizers = cluster.finalizers().to_vec();
    finalizers.push(DRIVER_IDENTITY_FINALIZER.to_string());
    patch_spark_cluster_finalizers(clusters, cluster, finalizers).await
}

/// Remove the finalizer so the API server can finish the deletion.
async fn remove_driver_identity_finalizer(
    clusters: &Api<SparkCluster>,
    cluster: &SparkCluster,
) -> Result<(), kube::Error> {
    if !cluster
        .finalizers()
        .iter()
        .any(|f| f == DRIVER_IDENTITY_FINALIZER)
    {
        return Ok(());
    }
    let finalizers: Vec<String> = cluster
        .finalizers()
        .iter()
        .filter(|f| *f != DRIVER_IDENTITY_FINALIZER)
        .cloned()
        .collect();
    patch_spark_cluster_finalizers(clusters, cluster, finalizers).await
}

/// The `status` half of the merge patch. Absent values are written as JSON `null` so a merge
/// patch clears them instead of leaving the previous value (see operator#362).
fn status_value(
    phase: &str,
    endpoint: Option<&str>,
    started_at: Option<&str>,
    last_activity_at: Option<&str>,
    message: Option<&str>,
    generation: Option<i64>,
) -> serde_json::Value {
    json!({
        "phase": phase,
        "endpoint": endpoint,
        "startedAt": started_at,
        "lastActivityAt": last_activity_at,
        "message": message,
        "observedGeneration": generation,
    })
}

/// `status_value` plus the driver's catalog identity (#493): only this write sets the two
/// fields, so every later merge patch leaves them as they are.
fn status_with_identity(
    mut status: serde_json::Value,
    account: &ServiceAccount,
) -> serde_json::Value {
    status["driverServiceAccount"] = json!(account.metadata.name);
    if let Some(uid) = account
        .metadata
        .uid
        .as_deref()
        .filter(|uid| !uid.is_empty())
    {
        status["workloadIdentity"] = json!(format!("kubernetes~{uid}"));
    }
    status
}

async fn patch_status(
    clusters: &Api<SparkCluster>,
    name: &str,
    status: serde_json::Value,
) -> Result<(), kube::Error> {
    clusters
        .patch_status(
            name,
            &PatchParams::default(),
            &Patch::Merge(json!({ "status": status })),
        )
        .await?;
    crate::metrics::record_status_write("spark_cluster");
    Ok(())
}

/// What to do after an apply failed: retry a failure the API server can recover from (5xx,
/// transport, TLS, timeout), wait for a human on a denial (4xx). A `SparkCluster` used to stay
/// `Failed` until the object changed, because every branch returned `await_change`
/// (stardelt-platform#1511: a webhook with no endpoints).
fn action_after_failure(e: &kube::Error) -> Action {
    let retry = match e {
        kube::Error::Api(response) => response.code >= 500,
        _ => true,
    };
    if retry {
        Action::requeue(Duration::from_secs(30))
    } else {
        Action::await_change()
    }
}

/// The phase of a live `SparkConnect`, from its `status.state`.
fn phase_from_state(state: Option<&str>) -> &'static str {
    match state.map(str::to_ascii_lowercase).as_deref() {
        Some("running") | Some("ready") => "Running",
        Some("failed") => "Failed",
        _ => "Starting",
    }
}

/// The phase and the message of a `SparkCluster`, from the SparkConnect's `status.state`
/// **and** its server pod. The Spark operator sets `state: Running` from a one-second
/// readiness sample, so it says Running for a pod in `CrashLoopBackOff` (platform#1482,
/// operator#773). A cluster is Running only while its server pod's `Ready` condition is
/// `True`; the waiting reason goes into `status.message`.
fn phase_and_message(
    state: Option<&str>,
    server_pod: Option<&str>,
    pod: Option<&Pod>,
) -> (&'static str, Option<String>) {
    let phase = phase_from_state(state);
    if phase != "Running" {
        return (phase, None);
    }
    let Some(name) = server_pod else {
        return (
            "Starting",
            Some("the server pod is not there yet".to_string()),
        );
    };
    let Some(pod) = pod else {
        return (
            "Starting",
            Some(format!("the server pod {name} is not readable yet")),
        );
    };
    let ready = pod
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"));
    if ready {
        return ("Running", None);
    }
    let reason = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .and_then(|cs| {
            cs.iter()
                .find_map(|c| c.state.as_ref()?.waiting.as_ref().map(|w| w.reason.clone()))
        })
        .flatten()
        .unwrap_or_else(|| "not ready".to_string());
    (
        "Starting",
        Some(format!("the server pod {name} is not ready ({reason})")),
    )
}

/// How long a server pod must have been dead before its `SparkConnect` is recreated. A
/// crash-looping driver restarts more often than this, so it is not recreated on every poll.
pub const DRIVER_DEAD_GRACE_SECS: i64 = 300;

/// The exit code of the server pod's container when that container has terminated and the pod
/// has been dead for at least [`DRIVER_DEAD_GRACE_SECS`]; `None` while it runs, waits, or died
/// recently.
fn dead_driver(pod: &Pod, now: k8s_openapi::jiff::Timestamp) -> Option<i32> {
    pod.status
        .as_ref()?
        .container_statuses
        .as_ref()?
        .iter()
        .find_map(|c| {
            let terminated = c.state.as_ref()?.terminated.as_ref()?;
            let finished = terminated.finished_at.as_ref()?.0.as_second();
            (now.as_second() - finished >= DRIVER_DEAD_GRACE_SECS).then_some(terminated.exit_code)
        })
}

/// How often a busy cluster refreshes `lastActivityAt`. Longer than the 60 s poll, so a
/// running job does not cause a status write per minute forever.
pub const IDLE_HEARTBEAT_SECS: i64 = 300;

/// What one idle poll writes and whether the cluster must stop. Pure: `now` is passed in,
/// like `stream_app::stream_state`.
#[derive(Debug, PartialEq, Eq)]
pub struct IdleDecision {
    pub last_activity_at: Option<String>,
    pub stop: bool,
}

pub fn idle_decision(
    last_activity_at: Option<&str>,
    active: bool,
    idle_minutes: i32,
    now: k8s_openapi::jiff::Timestamp,
) -> IdleDecision {
    let parsed = last_activity_at.and_then(|s| s.parse::<k8s_openapi::jiff::Timestamp>().ok());
    if active {
        let stale = parsed.is_none_or(|t| now.as_second() - t.as_second() >= IDLE_HEARTBEAT_SECS);
        return IdleDecision {
            last_activity_at: if stale {
                Some(now.to_string())
            } else {
                last_activity_at.map(str::to_string)
            },
            stop: false,
        };
    }
    let Some(t) = parsed else {
        // First observation with no history: start the clock here, do not stop yet.
        return IdleDecision {
            last_activity_at: Some(now.to_string()),
            stop: false,
        };
    };
    IdleDecision {
        last_activity_at: last_activity_at.map(str::to_string),
        stop: idle_minutes > 0 && now.as_second() - t.as_second() >= i64::from(idle_minutes) * 60,
    }
}

/// Whether the Spark Connect server of `name` has a live executor pod. `None` means
/// "not observable yet" (the driver or its app selector is not there), never "idle".
async fn executors_running(
    client: &kube::Client,
    namespace: &str,
    server_pod: Option<&str>,
) -> Result<Option<bool>, kube::Error> {
    let Some(server_pod) = server_pod else {
        return Ok(None);
    };
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let driver = match pods.get(server_pod).await {
        Ok(driver) => driver,
        Err(e) if is_not_found(&e) => return Ok(None),
        Err(e) => return Err(e),
    };
    let Some(selector) = driver
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get("spark-app-selector"))
        .cloned()
    else {
        return Ok(None);
    };
    let executors = pods
        .list(&ListParams::default().labels(&format!(
            "spark-role=executor,spark-app-selector={selector}"
        )))
        .await?;
    Ok(Some(
        executors
            .items
            .iter()
            .any(|pod| pod.metadata.deletion_timestamp.is_none()),
    ))
}

fn endpoint_for(server_name: Option<&str>, name: &str, namespace: &str) -> String {
    let service = server_name
        .map(str::to_string)
        .unwrap_or_else(|| format!("spc-{name}-server"));
    format!("sc://{service}.{namespace}.svc.cluster.local:15002")
}

pub async fn reconcile_spark_cluster(
    cluster: Arc<SparkCluster>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    if cluster.metadata.deletion_timestamp.is_some() {
        // The finalizer keeps the object readable, so its driver identity is pruned here while
        // `spec.owner.group` still is (#493, #1112); then the API server may finish the deletion.
        let name = cluster.name_any();
        let namespace = cluster.namespace().unwrap_or_else(|| "default".into());
        let clusters: Api<SparkCluster> = Api::namespaced(ctx.client.clone(), &namespace);
        if let Some(group) = cluster.spec.owner.group.as_deref() {
            prune_driver_identity(&ctx.client, &clusters, &namespace, &name, group).await?;
        }
        remove_driver_identity_finalizer(&clusters, &cluster).await?;
        return Ok(Action::await_change());
    }
    let name = cluster.name_any();
    let namespace = cluster.namespace().unwrap_or_else(|| "default".into());
    let spec = &cluster.spec;
    let generation = cluster.metadata.generation;
    let previous = cluster.status.as_ref();
    let clusters: Api<SparkCluster> = Api::namespaced(ctx.client.clone(), &namespace);
    let connects: Api<DynamicObject> = Api::namespaced_with(
        ctx.client.clone(),
        &namespace,
        &objects::spark_connect_resource(),
    );

    // A live cluster carries the finalizer that keeps it readable until its driver identity is
    // pruned; one that already has it is not patched again.
    ensure_driver_identity_finalizer(&clusters, &cluster).await?;

    if let Err(message) = spec.validate(&name) {
        patch_status(
            &clusters,
            &name,
            status_value("Failed", None, None, None, Some(&message), generation),
        )
        .await?;
        tracing::info!(cluster = %name, %namespace, %message, "spark cluster is invalid");
        return Ok(Action::await_change());
    }

    match spec.state {
        crate::crd::SparkClusterState::Stopped => {
            match connects
                .delete(&format!("spc-{name}"), &Default::default())
                .await
            {
                Ok(_) => {
                    patch_status(
                        &clusters,
                        &name,
                        status_value("Stopping", None, None, None, None, generation),
                    )
                    .await?;
                    tracing::info!(cluster = %name, %namespace, "spark cluster stopping");
                    return Ok(Action::requeue(Duration::from_secs(10)));
                }
                Err(e) if is_not_found(&e) => {
                    patch_status(
                        &clusters,
                        &name,
                        status_value("Stopped", None, None, None, None, generation),
                    )
                    .await?;
                    tracing::info!(cluster = %name, %namespace, "spark cluster stopped");
                    return Ok(Action::await_change());
                }
                Err(e) => return Err(e),
            }
        }
        crate::crd::SparkClusterState::Running => {}
    }

    // The driver's identity must exist before the SparkConnect's driver pod is created.
    let mut driver_account = match objects::apply(
        &ctx.client,
        &namespace,
        &crate::spark::cluster_driver_service_account(&namespace),
        false,
    )
    .await
    {
        Ok(account) => account,
        Err(e) => {
            patch_status(
                &clusters,
                &name,
                status_value("Failed", None, None, None, Some(&e.to_string()), generation),
            )
            .await?;
            tracing::info!(cluster = %name, %namespace, error = %e, "spark cluster driver identity not applied");
            return Ok(action_after_failure(&e));
        }
    };
    if let Err(e) = objects::apply(
        &ctx.client,
        &namespace,
        &crate::spark::cluster_driver_role(&namespace),
        false,
    )
    .await
    {
        patch_status(
            &clusters,
            &name,
            status_value("Failed", None, None, None, Some(&e.to_string()), generation),
        )
        .await?;
        tracing::info!(cluster = %name, %namespace, error = %e, "spark cluster driver identity not applied");
        return Ok(action_after_failure(&e));
    }
    if let Err(e) = objects::apply(
        &ctx.client,
        &namespace,
        &crate::spark::cluster_driver_role_binding(&namespace),
        false,
    )
    .await
    {
        patch_status(
            &clusters,
            &name,
            status_value("Failed", None, None, None, Some(&e.to_string()), generation),
        )
        .await?;
        tracing::info!(cluster = %name, %namespace, error = %e, "spark cluster driver identity not applied");
        return Ok(action_after_failure(&e));
    }

    // A cluster owned by a group runs the group's own code, so it gets its own driver identity
    // bound to the shared Role (#493). A cluster owned by a person keeps the shared driver. The
    // applied object replaces the shared one above: it is the identity the driver runs as, so it
    // is the one the status names (#1102).
    if let Some(group) = spec.owner.group.as_deref() {
        driver_account = match objects::apply(
            &ctx.client,
            &namespace,
            &crate::spark::group_cluster_driver_service_account(&namespace, group),
            false,
        )
        .await
        {
            Ok(account) => account,
            Err(e) => {
                patch_status(
                    &clusters,
                    &name,
                    status_value("Failed", None, None, None, Some(&e.to_string()), generation),
                )
                .await?;
                tracing::info!(cluster = %name, %namespace, error = %e, "spark cluster driver identity not applied");
                return Ok(action_after_failure(&e));
            }
        };
        if let Err(e) = objects::apply(
            &ctx.client,
            &namespace,
            &crate::spark::group_cluster_driver_role_binding(&namespace, group),
            false,
        )
        .await
        {
            patch_status(
                &clusters,
                &name,
                status_value("Failed", None, None, None, Some(&e.to_string()), generation),
            )
            .await?;
            tracing::info!(cluster = %name, %namespace, error = %e, "spark cluster driver identity not applied");
            return Ok(action_after_failure(&e));
        }
    }

    // The owner group changed: the identity the last reconcile applied is nobody's now, unless
    // another cluster of that group is live (#493, #1112).
    let current_driver = spec
        .owner
        .group
        .as_deref()
        .map(crate::spark::cluster_driver_for)
        .unwrap_or_else(|| crate::spark::CLUSTER_DRIVER.to_string());
    if let Some(stale) = previous.and_then(|status| status.driver_service_account.as_deref())
        && stale != current_driver
        && let Some(group) = stale.strip_prefix(&format!("{}-", crate::spark::CLUSTER_DRIVER))
        && !group.is_empty()
    {
        prune_driver_identity(&ctx.client, &clusters, &namespace, &name, group).await?;
    }

    // The driver runs the group's own code, so the two admission policies that bound what it may
    // create go in before the SparkConnect exists (operator#773, operator#775). Each identity keeps
    // its own policy; these name the cluster driver alone.
    if let Err(e) = objects::apply_cluster(
        &ctx.client,
        &crate::spark::cluster_pod_guard_policy(),
        false,
    )
    .await
    {
        patch_status(
            &clusters,
            &name,
            status_value("Failed", None, None, None, Some(&e.to_string()), generation),
        )
        .await?;
        tracing::info!(cluster = %name, %namespace, error = %e, "spark cluster pod guard not applied");
        return Ok(action_after_failure(&e));
    }
    if let Err(e) = objects::apply_cluster(
        &ctx.client,
        &crate::spark::cluster_pod_guard_binding(&namespace),
        false,
    )
    .await
    {
        patch_status(
            &clusters,
            &name,
            status_value("Failed", None, None, None, Some(&e.to_string()), generation),
        )
        .await?;
        tracing::info!(cluster = %name, %namespace, error = %e, "spark cluster pod guard not applied");
        return Ok(action_after_failure(&e));
    }
    if let Err(e) = objects::apply_cluster(
        &ctx.client,
        &crate::spark::cluster_config_map_guard_policy(),
        false,
    )
    .await
    {
        patch_status(
            &clusters,
            &name,
            status_value("Failed", None, None, None, Some(&e.to_string()), generation),
        )
        .await?;
        tracing::info!(cluster = %name, %namespace, error = %e, "spark cluster config map guard not applied");
        return Ok(action_after_failure(&e));
    }
    if let Err(e) = objects::apply_cluster(
        &ctx.client,
        &crate::spark::cluster_config_map_guard_binding(&namespace),
        false,
    )
    .await
    {
        patch_status(
            &clusters,
            &name,
            status_value("Failed", None, None, None, Some(&e.to_string()), generation),
        )
        .await?;
        tracing::info!(cluster = %name, %namespace, error = %e, "spark cluster config map guard not applied");
        return Ok(action_after_failure(&e));
    }

    // The server authenticates nobody: the policy that admits only the owner's pods, Nova
    // and Airflow goes in before the SparkConnect exists (internal#161 slice 4d).
    let policy = match crate::network_policy::spark_connect_ingress(&name, &namespace, &spec.owner)
    {
        Ok(mut policy) => {
            policy.metadata.owner_references = Some(vec![owner_reference(&cluster)]);
            policy
        }
        Err(message) => {
            patch_status(
                &clusters,
                &name,
                status_value("Failed", None, None, None, Some(&message), generation),
            )
            .await?;
            tracing::info!(cluster = %name, %namespace, %message, "spark cluster cannot start");
            return Ok(Action::await_change());
        }
    };
    if let Err(e) = objects::apply::<NetworkPolicy>(&ctx.client, &namespace, &policy, false).await {
        patch_status(
            &clusters,
            &name,
            status_value("Failed", None, None, None, Some(&e.to_string()), generation),
        )
        .await?;
        tracing::info!(cluster = %name, %namespace, error = %e, "spark cluster ingress policy not applied");
        return Ok(action_after_failure(&e));
    }

    let runtime = match Api::<ConfigMap>::namespaced(ctx.client.clone(), &namespace)
        .get(RUNTIME_CATALOGUE)
        .await
    {
        Ok(cm) => runtime_entry(&cm, &spec.runtime),
        Err(e) if is_not_found(&e) => Err(format!(
            "the runtime catalogue {RUNTIME_CATALOGUE} is missing"
        )),
        Err(e) => return Err(e),
    };
    let runtime = match runtime {
        Ok(entry) => entry,
        Err(message) => {
            patch_status(
                &clusters,
                &name,
                status_value("Failed", None, None, None, Some(&message), generation),
            )
            .await?;
            tracing::info!(cluster = %name, %namespace, %message, "spark cluster cannot start");
            return Ok(Action::await_change());
        }
    };

    let desired = match spark_connect(
        &name,
        &namespace,
        spec,
        &runtime,
        &owner_reference(&cluster),
    ) {
        Ok(object) => object,
        Err(message) => {
            patch_status(
                &clusters,
                &name,
                status_value("Failed", None, None, None, Some(&message), generation),
            )
            .await?;
            tracing::info!(cluster = %name, %namespace, %message, "spark cluster cannot start");
            return Ok(Action::await_change());
        }
    };

    if let Err(e) = objects::apply_dynamic(
        &ctx.client,
        &namespace,
        &objects::spark_connect_resource(),
        &desired,
        false,
    )
    .await
    {
        patch_status(
            &clusters,
            &name,
            status_value("Failed", None, None, None, Some(&e.to_string()), generation),
        )
        .await?;
        tracing::info!(cluster = %name, %namespace, error = %e, "spark cluster SparkConnect not applied");
        return Ok(action_after_failure(&e));
    }

    let (state, server_name, server_pod) = match connects.get(&format!("spc-{name}")).await {
        Ok(live) => (
            live.data
                .pointer("/status/state")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            live.data
                .pointer("/status/server/serviceName")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            live.data
                .pointer("/status/server/podName")
                .and_then(Value::as_str)
                .map(str::to_string),
        ),
        Err(e) if is_not_found(&e) => (None, None, None),
        Err(e) => return Err(e),
    };

    let pod = match server_pod.as_deref() {
        Some(pod_name) => {
            match Api::<Pod>::namespaced(ctx.client.clone(), &namespace)
                .get(pod_name)
                .await
            {
                Ok(pod) => Some(pod),
                Err(e) if is_not_found(&e) => None,
                Err(e) => return Err(e),
            }
        }
        None => None,
    };
    let (phase, message) = phase_and_message(state.as_deref(), server_pod.as_deref(), pod.as_ref());

    let mut message = message;
    if phase == "Starting"
        && let (Some(pod), Some(server_pod_name)) = (pod.as_ref(), server_pod.as_deref())
        && let Some(code) = dead_driver(pod, k8s_openapi::jiff::Timestamp::now())
    {
        // The `SparkConnect` is `spc-<cluster>`, not `spc-<pod>`: the server pod is
        // `spc-<cluster>-server` (#2090).
        match connects
            .delete(&format!("spc-{name}"), &Default::default())
            .await
        {
            Err(e) if !is_not_found(&e) => return Err(e),
            _ => {}
        }
        message = Some(format!(
            "the server pod {server_pod_name} exited with {code}; recreating the server"
        ));
    }

    let endpoint =
        (phase == "Running").then(|| endpoint_for(server_name.as_deref(), &name, &namespace));
    let started_at = if phase == "Running" {
        previous
            .and_then(|s| s.started_at.clone())
            .or_else(|| Some(k8s_openapi::jiff::Timestamp::now().to_string()))
    } else {
        None
    };

    let mut last_activity = previous.and_then(|s| s.last_activity_at.clone());
    let mut stop_for_idle = false;
    if phase == "Running" && spec.idle_stop_minutes > 0 {
        match executors_running(&ctx.client, &namespace, server_pod.as_deref()).await {
            Ok(Some(active)) => {
                let decision = idle_decision(
                    last_activity.as_deref(),
                    active,
                    spec.idle_stop_minutes,
                    k8s_openapi::jiff::Timestamp::now(),
                );
                last_activity = decision.last_activity_at;
                stop_for_idle = decision.stop;
            }
            Ok(None) => {
                tracing::debug!(cluster = %name, %namespace, "idle check skipped: the driver pod is not observable")
            }
            Err(e) => {
                tracing::warn!(cluster = %name, %namespace, error = %e, "idle check failed; not stopping")
            }
        }
    }

    let next = status_with_identity(
        status_value(
            phase,
            endpoint.as_deref(),
            started_at.as_deref(),
            last_activity.as_deref(),
            message.as_deref(),
            generation,
        ),
        &driver_account,
    );
    // An unchanged status must not be patched (#274 rule 1): the 300 s requeue would otherwise
    // write the same status forever and the release gate would see a busy operator (#1340).
    if !crate::conditions::status_is_current(cluster.status.as_ref(), &next) {
        patch_status(&clusters, &name, next).await?;
    }

    if stop_for_idle {
        clusters
            .patch(
                &name,
                &PatchParams::default(),
                &Patch::Merge(json!({ "spec": { "state": "Stopped" } })),
            )
            .await?;
        let now = k8s_openapi::jiff::Timestamp::now();
        let events: Api<Event> = Api::namespaced(ctx.client.clone(), &namespace);
        let event = Event {
            metadata: ObjectMeta {
                generate_name: Some(format!("{name}-idle-")),
                namespace: Some(namespace.clone()),
                ..Default::default()
            },
            involved_object: ObjectReference {
                api_version: Some("stardelt.io/v1alpha1".into()),
                kind: Some("SparkCluster".into()),
                name: Some(name.clone()),
                namespace: Some(namespace.clone()),
                uid: cluster.uid(),
                ..Default::default()
            },
            reason: Some("IdleStop".into()),
            message: Some(format!(
                "no job ran for {} minutes; the operator stopped the cluster",
                spec.idle_stop_minutes
            )),
            type_: Some("Normal".into()),
            source: Some(EventSource {
                component: Some("stardelt-operator".into()),
                host: None,
            }),
            first_timestamp: Some(Time(now)),
            last_timestamp: Some(Time(now)),
            count: Some(1),
            ..Default::default()
        };
        // The event is informative; a refused create must not fail the reconcile.
        if let Err(e) = events.create(&PostParams::default(), &event).await {
            tracing::warn!(cluster = %name, %namespace, error = %e, "idle-stop event not created");
        }
        tracing::info!(cluster = %name, %namespace, minutes = spec.idle_stop_minutes, "spark cluster stopped after idle");
        return Ok(Action::requeue(Duration::from_secs(10)));
    }

    tracing::info!(cluster = %name, %namespace, %phase, "reconciled");

    // The phase comes from the `SparkConnect`'s status, and the controller now watches that
    // object, so this is a resync, not a poll. `idle_stop_minutes` is 15 or more on every
    // shipped size (`preset`), so a 5-minute idle check still stops a cluster inside the minute
    // the object asks for.
    let requeue = if phase == "Starting" { 30 } else { 300 };
    Ok(Action::requeue(Duration::from_secs(requeue)))
}

pub(crate) fn error_policy(
    cluster: Arc<SparkCluster>,
    err: &kube::Error,
    ctx: Arc<Context>,
) -> Action {
    crate::controller::error_policy(cluster, err, ctx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        ConnectionKeyRef, SparkClusterOwner, SparkClusterState, SparkEnvVar, SparkResources,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::Client;
    use std::sync::Mutex;

    fn owner() -> OwnerReference {
        OwnerReference {
            api_version: "stardelt.io/v1alpha1".into(),
            kind: "SparkCluster".into(),
            name: "trial".into(),
            uid: "uid-1".into(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }
    }

    fn runtime() -> RuntimeEntry {
        RuntimeEntry {
            image: "ghcr.io/example/spark@sha256:runtime".into(),
            spark_version: "4.0.1".into(),
            service_account: "spark-runtime".into(),
            spark_conf: BTreeMap::from([
                ("spark.sql.shuffle.partitions".to_string(), "8".to_string()),
                (
                    "spark.kubernetes.namespace".to_string(),
                    "wrong".to_string(),
                ),
            ]),
        }
    }

    fn spec() -> SparkClusterSpec {
        SparkClusterSpec {
            display_name: None,
            owner: SparkClusterOwner {
                group: Some("mobility-analytics".into()),
                user: None,
            },
            viewers: vec![],
            size: SparkClusterSize::Small,
            driver: None,
            executor: None,
            max_executors: None,
            runtime: "spark-4.0-py3.12".into(),
            image: None,
            env: vec![],
            spark_conf: BTreeMap::new(),
            idle_stop_minutes: 60,
            state: SparkClusterState::Stopped,
        }
    }

    /// A live cluster carries the driver-identity finalizer, the steady state a reconcile leaves
    /// (#1147); a test that wants the first reconcile builds `fresh_cluster`.
    fn cluster(state: SparkClusterState) -> SparkCluster {
        SparkCluster {
            metadata: ObjectMeta {
                name: Some("trial".into()),
                namespace: Some("data".into()),
                uid: Some("uid-1".into()),
                generation: Some(1),
                finalizers: Some(vec![DRIVER_IDENTITY_FINALIZER.into()]),
                ..Default::default()
            },
            spec: SparkClusterSpec { state, ..spec() },
            status: None,
        }
    }

    fn catalogue() -> ConfigMap {
        ConfigMap {
            metadata: ObjectMeta {
                name: Some(RUNTIME_CATALOGUE.into()),
                namespace: Some("data".into()),
                ..Default::default()
            },
            data: Some(BTreeMap::from([(
                "spark-4.0-py3.12".to_string(),
                r#"{"image":"ghcr.io/example/spark@sha256:runtime","sparkVersion":"4.0.1","serviceAccount":"spark-runtime","sparkConf":{"spark.sql.shuffle.partitions":"8"}}"#.to_string(),
            )])),
            ..Default::default()
        }
    }

    fn mock_client<F>(handler: F) -> (Client, Arc<Mutex<Vec<String>>>)
    where
        F: Fn(&str, &str, &str) -> (u16, String) + Send + Sync + 'static,
    {
        use http_body_util::BodyExt;
        let requests: Arc<Mutex<Vec<String>>> = Arc::default();
        let recorder = requests.clone();
        let handler = Arc::new(handler);
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let handler = handler.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            let query = request.uri().query().unwrap_or_default().to_string();
            async move {
                let body_bytes = request
                    .into_body()
                    .collect()
                    .await
                    .map(|b| b.to_bytes().to_vec())
                    .unwrap_or_default();
                let body = String::from_utf8_lossy(&body_bytes).to_string();
                recorder
                    .lock()
                    .expect("requests poisoned")
                    .push(format!("{method} {path}?{query} {body}"));
                let (status, document) = match identity_response(&path) {
                    Some(response) => response,
                    None => handler(&method, &path, &body),
                };
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(document.into_bytes()))
                        .expect("valid mock response"),
                )
            }
        });
        (Client::new(service, "stardelt"), requests)
    }

    fn ctx(client: Client) -> Arc<Context> {
        Arc::new(Context::new(
            client,
            Arc::new(crate::helm::testing::RecordingRunner::default()),
        ))
    }

    /// The ServiceAccount the request applies, echoed back with a uid: a typed server-side apply
    /// refuses a response of another object, and the status records the object it was given
    /// (#1102).
    fn sa_name_in(path: &str) -> Option<String> {
        let name = path
            .split('/')
            .skip_while(|segment| *segment != "serviceaccounts")
            .nth(1)?;
        name.starts_with("spark-cluster-driver")
            .then(|| name.to_string())
    }

    /// The driver identities and the two cluster guards always answer with a document of their own
    /// kind: a typed server-side apply refuses a response of another kind (operator#773,
    /// operator#775).
    fn identity_response(path: &str) -> Option<(u16, String)> {
        // A cluster owned by a group applies `spark-cluster-driver-<group>` (#493), so the mock
        // answers with the name the request asked for, and a uid the status can record (#1102).
        let object_name = path.rsplit('/').next().unwrap_or("spark-cluster-driver");
        let object = if let Some(name) = sa_name_in(path) {
            json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": { "name": name, "namespace": "data", "uid": "uid-sa" }
            })
        } else if path.contains("/roles/spark-cluster-driver") {
            json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "Role",
                "metadata": { "name": object_name, "namespace": "data" }
            })
        } else if path.contains("/rolebindings/spark-cluster-driver") {
            json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": { "name": object_name, "namespace": "data" }
            })
        } else if path.contains("/validatingadmissionpolicies/spark-cluster-executor-pods") {
            json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingAdmissionPolicy",
                "metadata": { "name": "spark-cluster-executor-pods" }
            })
        } else if path.contains("/validatingadmissionpolicybindings/spark-cluster-executor-pods") {
            json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingAdmissionPolicyBinding",
                "metadata": { "name": "spark-cluster-executor-pods-data" }
            })
        } else if path.contains("/validatingadmissionpolicies/spark-cluster-configmaps") {
            json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingAdmissionPolicy",
                "metadata": { "name": "spark-cluster-configmaps" }
            })
        } else if path.contains("/validatingadmissionpolicybindings/spark-cluster-configmaps") {
            json!({
                "apiVersion": "admissionregistration.k8s.io/v1",
                "kind": "ValidatingAdmissionPolicyBinding",
                "metadata": { "name": "spark-cluster-configmaps-data" }
            })
        } else {
            return None;
        };
        Some((200, object.to_string()))
    }

    #[test]
    fn presets_give_the_documented_numbers() {
        assert_eq!(preset(SparkClusterSize::Small), (1, "2Gi", 1, "2Gi", 2));
        assert_eq!(preset(SparkClusterSize::Medium), (2, "4Gi", 2, "4Gi", 4));
        assert_eq!(preset(SparkClusterSize::Large), (4, "8Gi", 4, "8Gi", 8));
    }

    fn pod(status: Value) -> Pod {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "p", "namespace": "data"},
            "status": status,
        }))
        .expect("pod fixture")
    }

    fn ready_pod() -> Pod {
        pod(json!({"conditions": [{"type": "Ready", "status": "True"}]}))
    }

    fn crashloop_pod() -> Pod {
        pod(json!({
            "conditions": [],
            "containerStatuses": [{
                "name": "spark-kubernetes-driver",
                "state": {"waiting": {"reason": "CrashLoopBackOff"}},
            }],
        }))
    }

    fn terminated_pod(code: i32, finished_at: &str) -> Pod {
        pod(json!({
            "conditions": [],
            "containerStatuses": [{
                "name": "spark-kubernetes-driver",
                "state": {"terminated": {"exitCode": code, "reason": "Error", "finishedAt": finished_at}},
            }],
        }))
    }

    #[test]
    fn dead_driver_reports_a_killed_server_after_the_grace() {
        let now: k8s_openapi::jiff::Timestamp = "2026-11-27T06:20:00Z".parse().unwrap();
        assert_eq!(
            dead_driver(&terminated_pod(137, "2026-11-27T06:08:20Z"), now),
            Some(137)
        );
        assert_eq!(
            dead_driver(&terminated_pod(1, "2026-11-27T06:18:00Z"), now),
            None
        );
    }

    #[test]
    fn a_running_or_waiting_pod_is_not_dead() {
        let now: k8s_openapi::jiff::Timestamp = "2026-11-27T06:20:00Z".parse().unwrap();
        assert_eq!(dead_driver(&ready_pod(), now), None);
        assert_eq!(dead_driver(&crashloop_pod(), now), None);
    }

    #[test]
    fn phase_and_message_reads_the_server_pod() {
        assert_eq!(
            phase_and_message(Some("running"), Some("p"), Some(&ready_pod())),
            ("Running", None)
        );
        assert_eq!(
            phase_and_message(Some("running"), Some("p"), Some(&crashloop_pod())),
            (
                "Starting",
                Some("the server pod p is not ready (CrashLoopBackOff)".to_string())
            )
        );
        assert_eq!(
            phase_and_message(Some("failed"), Some("p"), Some(&ready_pod())),
            ("Failed", None)
        );
    }

    #[test]
    fn the_spec_overrides_the_preset_field_by_field() {
        let mut s = spec();
        s.size = SparkClusterSize::Medium;
        s.driver = Some(SparkResources {
            cores: Some(3),
            memory: None,
        });
        s.executor = Some(SparkResources {
            cores: None,
            memory: Some("6Gi".into()),
        });
        s.max_executors = Some(6);
        let object = spark_connect("trial", "data", &s, &runtime(), &owner()).expect("build");
        assert_eq!(object.data.pointer("/spec/server/cores"), Some(&json!(3)));
        assert_eq!(
            object.data.pointer("/spec/server/memory"),
            Some(&json!("4g"))
        );
        assert_eq!(object.data.pointer("/spec/executor/cores"), Some(&json!(2)));
        assert_eq!(
            object.data.pointer("/spec/executor/memory"),
            Some(&json!("6g"))
        );
        assert_eq!(
            object.data.pointer("/spec/dynamicAllocation/maxExecutors"),
            Some(&json!(6))
        );
    }

    #[test]
    fn a_built_connect_carries_the_spark_operators_memory_unit() {
        let object = spark_connect("trial", "data", &spec(), &runtime(), &owner()).expect("build");
        assert_eq!(
            object.data.pointer("/spec/server/memory"),
            Some(&json!("2g"))
        );
        assert_eq!(
            object.data.pointer("/spec/executor/memory"),
            Some(&json!("2g"))
        );

        let mut s = spec();
        s.driver = Some(SparkResources {
            cores: None,
            memory: Some("512Mi".into()),
        });
        let object = spark_connect("trial", "data", &s, &runtime(), &owner()).expect("build");
        assert_eq!(
            object.data.pointer("/spec/server/memory"),
            Some(&json!("512m"))
        );
    }

    #[test]
    fn a_pinned_spec_image_beats_the_runtime_image() {
        let mut s = spec();
        s.image = Some("ghcr.io/example/custom@sha256:abc".into());
        let object = spark_connect("trial", "data", &s, &runtime(), &owner()).expect("build");
        assert_eq!(
            object.data.pointer("/spec/image"),
            Some(&json!("ghcr.io/example/custom@sha256:abc"))
        );

        let object = spark_connect("trial", "data", &spec(), &runtime(), &owner()).expect("build");
        assert_eq!(
            object.data.pointer("/spec/image"),
            Some(&json!("ghcr.io/example/spark@sha256:runtime"))
        );
    }

    #[test]
    fn the_platform_owned_keys_win_over_the_spec() {
        let mut s = spec();
        s.spark_conf.insert(
            "spark.kubernetes.namespace".to_string(),
            "elsewhere".to_string(),
        );
        s.spark_conf
            .insert("spark.sql.shuffle.partitions".to_string(), "16".to_string());
        let object = spark_connect("trial", "data", &s, &runtime(), &owner()).expect("build");
        assert_eq!(
            object
                .data
                .pointer("/spec/sparkConf/spark.kubernetes.namespace"),
            Some(&json!("data"))
        );
        assert_eq!(
            object
                .data
                .pointer("/spec/sparkConf/spark.kubernetes.authenticate.driver.serviceAccountName"),
            None
        );
        assert_eq!(
            object
                .data
                .pointer("/spec/server/template/spec/serviceAccountName"),
            Some(&json!("spark-cluster-driver-mobility-analytics"))
        );
        assert_eq!(
            object
                .data
                .pointer("/spec/sparkConf/spark.sql.shuffle.partitions"),
            Some(&json!("16"))
        );
    }

    #[test]
    fn a_person_owned_cluster_keeps_the_shared_driver() {
        let s = SparkClusterSpec {
            owner: SparkClusterOwner {
                group: None,
                user: Some("ada".into()),
            },
            ..spec()
        };
        let object = spark_connect("trial", "data", &s, &runtime(), &owner()).expect("build");
        assert_eq!(
            object
                .data
                .pointer("/spec/server/template/spec/serviceAccountName"),
            Some(&json!("spark-cluster-driver"))
        );
    }

    #[test]
    fn the_driver_and_the_executors_have_different_identities() {
        let object = spark_connect("trial", "data", &spec(), &runtime(), &owner()).expect("build");
        assert_eq!(
            object
                .data
                .pointer("/spec/server/template/spec/serviceAccountName"),
            Some(&json!("spark-cluster-driver-mobility-analytics"))
        );
        assert_eq!(
            object
                .data
                .pointer("/spec/executor/template/spec/serviceAccountName"),
            Some(&json!("spark-runtime"))
        );
        assert_eq!(
            object.data.pointer(
                "/spec/sparkConf/spark.kubernetes.authenticate.executor.serviceAccountName"
            ),
            None
        );
    }

    #[test]
    fn the_driver_and_executor_templates_are_pod_security_restricted() {
        let object = spark_connect("trial", "data", &spec(), &runtime(), &owner()).expect("build");
        for template in ["/spec/server/template/spec", "/spec/executor/template/spec"] {
            assert_eq!(
                object
                    .data
                    .pointer(&format!("{template}/securityContext/runAsNonRoot")),
                Some(&json!(true)),
                "{template} pod securityContext"
            );
            assert_eq!(
                object
                    .data
                    .pointer(&format!("{template}/securityContext/runAsUser")),
                Some(&json!(185)),
                "{template} runAsUser"
            );
            assert_eq!(
                object
                    .data
                    .pointer(&format!("{template}/securityContext/seccompProfile/type")),
                Some(&json!("RuntimeDefault")),
                "{template} seccompProfile"
            );
            let container = format!("{template}/containers/0/securityContext");
            assert_eq!(
                object
                    .data
                    .pointer(&format!("{container}/allowPrivilegeEscalation")),
                Some(&json!(false)),
                "{container} allowPrivilegeEscalation"
            );
            assert_eq!(
                object
                    .data
                    .pointer(&format!("{container}/capabilities/drop")),
                Some(&json!(["ALL"])),
                "{container} capabilities"
            );
        }
    }

    #[test]
    fn labels_and_the_owner_reference_name_the_cluster() {
        let object = spark_connect("trial", "data", &spec(), &runtime(), &owner()).expect("build");
        assert_eq!(object.metadata.name.as_deref(), Some("spc-trial"));
        assert_eq!(object.metadata.namespace.as_deref(), Some("data"));
        let labels = object.metadata.labels.expect("labels");
        assert_eq!(labels["app.kubernetes.io/managed-by"], "stardelt-operator");
        assert_eq!(labels["stardelt.io/spark-cluster"], "trial");
        assert_eq!(
            object.metadata.owner_references.expect("owner")[0].kind,
            "SparkCluster"
        );
    }

    #[test]
    fn env_with_a_connection_ref_is_not_supported_yet() {
        let mut s = spec();
        s.env = vec![SparkEnvVar {
            name: "TOKEN".into(),
            value: None,
            connection_ref: Some(ConnectionKeyRef {
                name: "store".into(),
                key: "token".into(),
            }),
        }];
        assert_eq!(
            spark_connect("trial", "data", &s, &runtime(), &owner()).unwrap_err(),
            "connectionRef is not supported yet (internal#161 slice 7)"
        );
    }

    #[test]
    fn a_literal_env_is_passed_to_driver_and_executors() {
        let mut s = spec();
        s.env = vec![SparkEnvVar {
            name: "MODE".into(),
            value: Some("dev".into()),
            connection_ref: None,
        }];
        let object = spark_connect("trial", "data", &s, &runtime(), &owner()).expect("build");
        assert_eq!(
            object
                .data
                .pointer("/spec/server/template/spec/containers/0/env"),
            Some(&json!([{ "name": "MODE", "value": "dev" }]))
        );
        assert_eq!(
            object
                .data
                .pointer("/spec/executor/template/spec/containers/0/env"),
            Some(&json!([{ "name": "MODE", "value": "dev" }]))
        );
    }

    /// #493 design question 1: the driver can read its own catalog token, the executors cannot.
    #[test]
    fn the_driver_reads_a_projected_catalog_token_and_an_executor_does_not() {
        let connect =
            spark_connect("analytics", "stardelt", &spec(), &runtime(), &owner()).expect("renders");
        let server = &connect.data["spec"]["server"]["template"]["spec"];
        let volume = &server["volumes"][0];
        assert_eq!(volume["name"], json!("catalog-token"));
        // The literal, not the constant: a constant that drifts from the catalog's verified audience
        // is the defect this pins (#538, `make verify-catalog-workload-auth`).
        assert_eq!(
            volume["projected"]["sources"][0]["serviceAccountToken"]["audience"],
            json!("stardelt-catalog-workload")
        );
        assert_eq!(
            volume["projected"]["sources"][0]["serviceAccountToken"]["path"],
            json!("token")
        );
        assert_eq!(
            server["containers"][0]["volumeMounts"][0]["mountPath"],
            json!(CATALOG_TOKEN_MOUNT)
        );
        assert_eq!(
            server["containers"][0]["volumeMounts"][0]["readOnly"],
            json!(true)
        );

        let executor = &connect.data["spec"]["executor"]["template"]["spec"];
        assert!(
            executor.get("volumes").is_none(),
            "an executor holds no catalog token: it uses the vended storage credential"
        );
    }

    #[test]
    fn an_unknown_runtime_names_the_known_ones() {
        let err = runtime_entry(&catalogue(), "spark-4.1-py3.13").unwrap_err();
        assert_eq!(
            err,
            "unknown runtime spark-4.1-py3.13; known: spark-4.0-py3.12"
        );
    }

    #[test]
    fn a_missing_catalogue_is_reported() {
        let mut cm = catalogue();
        cm.data = None;
        assert_eq!(
            runtime_entry(&cm, "spark-4.0-py3.12").unwrap_err(),
            "the runtime catalogue stardelt-spark-runtimes is missing"
        );
    }

    #[tokio::test]
    async fn the_driver_identity_is_applied_before_the_connect() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkconnects/spc-trial") {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Starting"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"}}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(30)));
        let log = requests.lock().expect("requests").join("\n");
        let lines: Vec<&str> = log.lines().collect();
        let connect = lines
            .iter()
            .position(|line| line.starts_with("PATCH") && line.contains("/sparkconnects/spc-trial"))
            .expect("the SparkConnect is written");
        for path in [
            "serviceaccounts/spark-cluster-driver",
            "serviceaccounts/spark-cluster-driver-mobility-analytics",
            "roles/spark-cluster-driver",
            "rolebindings/spark-cluster-driver",
            "rolebindings/spark-cluster-driver-mobility-analytics",
            "validatingadmissionpolicies/spark-cluster-executor-pods",
            "validatingadmissionpolicybindings/spark-cluster-executor-pods-data",
            "validatingadmissionpolicies/spark-cluster-configmaps",
            "validatingadmissionpolicybindings/spark-cluster-configmaps-data",
        ] {
            let index = lines
                .iter()
                .position(|line| line.starts_with("PATCH") && line.contains(path))
                .unwrap_or_else(|| panic!("{path} is applied: {log}"));
            assert!(
                index < connect,
                "{path} must be applied before the SparkConnect: {log}"
            );
        }
        let status = lines
            .iter()
            .rfind(|line| line.starts_with("PATCH") && line.contains("/sparkclusters/trial/status"))
            .expect("a status write");
        let body = status
            .split_once(' ')
            .and_then(|(_, rest)| rest.split_once(' '))
            .map(|(_, body)| body)
            .expect("the recorded line carries the body");
        let body: Value = serde_json::from_str(body).expect("the status patch is JSON");
        assert_eq!(
            body["status"]["workloadIdentity"],
            json!("kubernetes~uid-sa"),
            "{log}"
        );
        // The fixture's cluster is owned by `mobility-analytics`, so the driver runs as the
        // group's ServiceAccount (#493, #1100): the status names that one, not the shared one.
        assert_eq!(
            body["status"]["driverServiceAccount"],
            json!("spark-cluster-driver-mobility-analytics"),
            "{log}"
        );
    }

    /// The fixture cluster with the owner group and the driver identity a previous reconcile
    /// left in `status` (#1102).
    fn cluster_with_status(
        state: SparkClusterState,
        group: Option<&str>,
        driver: &str,
    ) -> SparkCluster {
        let mut object = cluster(state);
        object.spec.owner.group = group.map(str::to_string);
        object.status = Some(crate::crd::SparkClusterStatus {
            driver_service_account: Some(driver.into()),
            ..Default::default()
        });
        object
    }

    fn listed_cluster(name: &str, group: &str) -> Value {
        json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "SparkCluster",
            "metadata": { "name": name, "namespace": "data" },
            "spec": {
                "owner": { "group": group },
                "runtime": "spark-4.0-py3.12",
                "state": "Running"
            }
        })
    }

    fn cluster_list(items: Vec<Value>) -> String {
        json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "SparkClusterList",
            "items": items,
        })
        .to_string()
    }

    /// The answers a reconcile needs once it is past the driver identity: the namespace's
    /// clusters (the prune guard's LIST), the runtime catalogue, the SparkConnect, the ingress
    /// policy and the status write. The DELETE answers come from `identity_response`, which
    /// echoes the ServiceAccount and the RoleBinding the request names.
    fn prune_mock(list: String) -> (Client, Arc<Mutex<Vec<String>>>) {
        mock_client(move |method, path, _body| {
            if method == "GET" && path.ends_with("/sparkclusters") {
                return (200, list.clone());
            }
            if method == "PATCH" && path.ends_with("/sparkclusters/trial") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"new"},"runtime":"spark-4.0-py3.12","state":"Running"}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkconnects/spc-trial") {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"new"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Starting"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"}}"#.into(),
            )
        })
    }

    /// The LIST of the prune guard, as recorded in the request log.
    fn lists_clusters(log: &str) -> bool {
        log.lines()
            .any(|line| line.starts_with("GET") && line.contains("/sparkclusters"))
    }

    /// The finalizer patch of the cluster object itself, as recorded in the request log (the
    /// status write patches `/sparkclusters/trial/status` and is not this one).
    fn finalizer_patch(log: &str) -> Option<String> {
        log.lines()
            .find(|line| line.starts_with("PATCH") && line.contains("/sparkclusters/trial?"))
            .map(str::to_string)
    }

    /// Whether the reconcile deleted any object: the pod guard's policy bodies carry `DELETE`
    /// among their allowed operations, so only a request line counts.
    fn deletes_anything(log: &str) -> bool {
        log.lines().any(|line| line.starts_with("DELETE"))
    }

    /// The status patches the mock recorded, each body parsed. A request line is
    /// `METHOD path?query body`.
    fn status_patches(requests: &Arc<Mutex<Vec<String>>>) -> Vec<Value> {
        requests
            .lock()
            .expect("requests poisoned")
            .iter()
            .filter(|line| {
                line.starts_with("PATCH ") && line.contains("/sparkclusters/trial/status?")
            })
            .filter_map(|line| line.splitn(3, ' ').nth(2).map(str::to_string))
            .map(|body| serde_json::from_str(&body).expect("a patch body is JSON"))
            .collect()
    }

    #[tokio::test]
    async fn a_changed_owner_group_prunes_the_previous_driver_identity() {
        let (client, requests) = prune_mock(cluster_list(vec![listed_cluster("trial", "new")]));
        let cluster = cluster_with_status(
            SparkClusterState::Running,
            Some("new"),
            "spark-cluster-driver-old",
        );
        reconcile_spark_cluster(Arc::new(cluster), ctx(client))
            .await
            .expect("reconcile");
        let log = requests.lock().expect("requests").join("\n");
        for path in [
            "/serviceaccounts/spark-cluster-driver-old",
            "/rolebindings/spark-cluster-driver-old",
        ] {
            assert!(
                log.lines()
                    .any(|line| line.starts_with("DELETE") && line.contains(path)),
                "{path} is pruned: {log}"
            );
        }
    }

    #[tokio::test]
    async fn a_no_op_reconcile_sends_no_status_patch() {
        let (client, requests) = prune_mock(cluster_list(vec![listed_cluster("trial", "new")]));
        let cluster = cluster_with_status(
            SparkClusterState::Running,
            Some("new"),
            "spark-cluster-driver-new",
        );
        reconcile_spark_cluster(Arc::new(cluster), ctx(client.clone()))
            .await
            .expect("reconcile");
        let first = status_patches(&requests);
        assert_eq!(first.len(), 1, "the first reconcile writes the status");
        let status: crate::crd::SparkClusterStatus =
            serde_json::from_value(first[0]["status"].clone())
                .expect("the patch body carries a SparkClusterStatus");

        // The controller reads the status back on the next event: nothing changed, so nothing is
        // written (#274 rule 1). Without the guard the 300 s requeue patches forever (#1340).
        let mut current = cluster_with_status(
            SparkClusterState::Running,
            Some("new"),
            "spark-cluster-driver-new",
        );
        current.status = Some(status);
        requests.lock().expect("requests poisoned").clear();
        let action = reconcile_spark_cluster(Arc::new(current), ctx(client))
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(30)));
        let patches = status_patches(&requests);
        assert!(
            patches.is_empty(),
            "no status patch for an unchanged status: {patches:?}"
        );
    }

    #[tokio::test]
    async fn a_changed_spark_cluster_status_is_patched() {
        // A stored status whose phase is stale: the next phase must be written.
        let mut cluster = cluster_with_status(
            SparkClusterState::Running,
            Some("new"),
            "spark-cluster-driver-new",
        );
        cluster.status.as_mut().expect("status").phase = "Running".into();
        let (client, requests) = prune_mock(cluster_list(vec![listed_cluster("trial", "new")]));
        reconcile_spark_cluster(Arc::new(cluster), ctx(client))
            .await
            .expect("reconcile");
        let patches = status_patches(&requests);
        assert_eq!(
            patches.len(),
            1,
            "the changed phase is written: {patches:?}"
        );
        assert_eq!(patches[0]["status"]["phase"], json!("Starting"));
    }

    #[tokio::test]
    async fn a_live_cluster_carries_the_driver_identity_finalizer() {
        let (client, requests) = prune_mock(cluster_list(vec![listed_cluster("trial", "new")]));
        let mut fresh = cluster_with_status(
            SparkClusterState::Running,
            Some("new"),
            "spark-cluster-driver-new",
        );
        fresh.metadata.finalizers = None;
        reconcile_spark_cluster(Arc::new(fresh.clone()), ctx(client.clone()))
            .await
            .expect("reconcile");
        let log = requests.lock().expect("requests").join("\n");
        let patch = finalizer_patch(&log).expect("the finalizer is patched");
        assert!(patch.contains(DRIVER_IDENTITY_FINALIZER), "{patch}");

        // The object carries it now, so the next reconcile writes nothing (quiet operator).
        let mut current = fresh;
        current.metadata.finalizers = Some(vec![DRIVER_IDENTITY_FINALIZER.into()]);
        requests.lock().expect("requests").clear();
        reconcile_spark_cluster(Arc::new(current), ctx(client))
            .await
            .expect("reconcile");
        let log = requests.lock().expect("requests").join("\n");
        assert!(finalizer_patch(&log).is_none(), "not patched again: {log}");
    }

    #[tokio::test]
    async fn a_deleted_cluster_prunes_the_driver_identity_and_drops_the_finalizer() {
        let (client, requests) = prune_mock(cluster_list(vec![listed_cluster("trial", "old")]));
        let mut dying = cluster_with_status(
            SparkClusterState::Running,
            Some("old"),
            "spark-cluster-driver-old",
        );
        dying.metadata.deletion_timestamp = Some(Time(k8s_openapi::jiff::Timestamp::now()));
        let action = reconcile_spark_cluster(Arc::new(dying), ctx(client))
            .await
            .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        for path in [
            "/serviceaccounts/spark-cluster-driver-old",
            "/rolebindings/spark-cluster-driver-old",
        ] {
            assert!(
                log.lines()
                    .any(|line| line.starts_with("DELETE") && line.contains(path)),
                "{path} is pruned: {log}"
            );
        }
        let patch = finalizer_patch(&log).expect("the finalizer is patched");
        assert!(
            patch.contains("\"finalizers\":[]"),
            "the finalizer is removed: {patch}"
        );
    }

    #[tokio::test]
    async fn a_shared_driver_identity_is_never_pruned() {
        let (client, requests) = prune_mock(cluster_list(vec![listed_cluster("trial", "new")]));
        // The shared driver belongs to every person-owned cluster: a cluster that becomes
        // group-owned must not delete it.
        let cluster = cluster_with_status(
            SparkClusterState::Running,
            Some("new"),
            "spark-cluster-driver",
        );
        reconcile_spark_cluster(Arc::new(cluster), ctx(client))
            .await
            .expect("reconcile");
        let log = requests.lock().expect("requests").join("\n");
        assert!(!deletes_anything(&log), "nothing is deleted: {log}");
        assert!(
            !lists_clusters(&log),
            "a shared driver needs no LIST: {log}"
        );
    }

    #[tokio::test]
    async fn another_cluster_of_the_old_group_keeps_the_driver_identity() {
        let (client, requests) = prune_mock(cluster_list(vec![
            listed_cluster("trial", "new"),
            listed_cluster("other", "old"),
        ]));
        let cluster = cluster_with_status(
            SparkClusterState::Running,
            Some("new"),
            "spark-cluster-driver-old",
        );
        reconcile_spark_cluster(Arc::new(cluster), ctx(client))
            .await
            .expect("reconcile");
        let log = requests.lock().expect("requests").join("\n");
        assert!(lists_clusters(&log), "the guard lists the namespace: {log}");
        assert!(
            !deletes_anything(&log),
            "another cluster of the group still runs as the identity: {log}"
        );
    }

    #[tokio::test]
    async fn a_fellow_cluster_being_deleted_does_not_keep_the_driver_identity() {
        let mut other = listed_cluster("other", "old");
        other["metadata"]["deletionTimestamp"] =
            json!(k8s_openapi::jiff::Timestamp::now().to_string());
        let (client, requests) =
            prune_mock(cluster_list(vec![listed_cluster("trial", "old"), other]));
        let mut dying = cluster_with_status(
            SparkClusterState::Running,
            Some("old"),
            "spark-cluster-driver-old",
        );
        dying.metadata.deletion_timestamp = Some(Time(k8s_openapi::jiff::Timestamp::now()));
        reconcile_spark_cluster(Arc::new(dying), ctx(client))
            .await
            .expect("reconcile");
        let log = requests.lock().expect("requests").join("\n");
        for path in [
            "/serviceaccounts/spark-cluster-driver-old",
            "/rolebindings/spark-cluster-driver-old",
        ] {
            assert!(
                log.lines()
                    .any(|line| line.starts_with("DELETE") && line.contains(path)),
                "{path} is pruned although the group's other cluster is being deleted: {log}"
            );
        }
    }

    #[tokio::test]
    async fn running_without_a_live_object_applies_once_and_reports_starting() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkconnects/spc-trial") {
                return (
                    404,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"not found","reason":"NotFound","code":404}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Starting"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"}}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(30)));
        let log = requests.lock().expect("requests").join("\n");
        let applies = log
            .lines()
            .filter(|line| line.starts_with("PATCH") && line.contains("/sparkconnects/spc-trial"))
            .count();
        assert_eq!(applies, 1, "{log}");
        assert!(log.contains("fieldManager=stardelt-operator"), "{log}");
        assert!(log.contains("\"phase\":\"Starting\""), "{log}");
        assert!(log.contains("\"endpoint\":null"), "{log}");
    }

    #[tokio::test]
    async fn a_refused_spark_connect_is_reported_as_failed() {
        // operator#783: an apply that fails before the first status write must still leave the
        // reason in `status.message`, not an empty status.
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkconnects/spc-trial") {
                return (
                    403,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"admission webhook \"validate-sparkoperator-k8s-io-v1alpha1-sparkconnect.sparkoperator.k8s.io\" denied the request: sparkConf key \"spark.kubernetes.authenticate.executor.serviceAccountName\" is not allowed: configure the service account via the CRD spec instead","reason":"Forbidden","code":403}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Failed"}}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        let reported = log
            .lines()
            .find(|line| line.starts_with("PATCH") && line.contains("/sparkclusters/trial/status"))
            .unwrap_or_else(|| panic!("no status written: {log}"));
        assert!(reported.contains("\"phase\":\"Failed\""), "{reported}");
        assert!(
            reported.contains("configure the service account via the CRD spec instead"),
            "{reported}"
        );
    }

    #[tokio::test]
    async fn a_transient_apply_failure_is_retried() {
        // operator#788: a 5xx (a webhook with no endpoints) is recovered from by retrying, so the
        // cluster must not sit `Failed` until the object changes.
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkconnects/spc-trial") {
                return (
                    500,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"Internal error occurred: failed calling webhook \"mutate-sparkconnects.sparkoperator.k8s.io\": no endpoints available for service \"spark-operator-webhook-svc\"","reason":"InternalError","code":500}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Failed"}}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(30)));
        let log = requests.lock().expect("requests").join("\n");
        let reported = log
            .lines()
            .find(|line| line.starts_with("PATCH") && line.contains("/sparkclusters/trial/status"))
            .unwrap_or_else(|| panic!("no status written: {log}"));
        assert!(reported.contains("\"phase\":\"Failed\""), "{reported}");
        assert!(reported.contains("no endpoints available"), "{reported}");
    }

    #[tokio::test]
    async fn a_refused_ingress_policy_is_reported_as_failed() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    403,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"networkpolicies.networking.k8s.io \"spc-trial-ingress\" is forbidden","reason":"Forbidden","code":403}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Failed"}}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        let reported = log
            .lines()
            .find(|line| line.starts_with("PATCH") && line.contains("/sparkclusters/trial/status"))
            .unwrap_or_else(|| panic!("no status written: {log}"));
        assert!(reported.contains("\"phase\":\"Failed\""), "{reported}");
        assert!(
            reported.contains("networkpolicies.networking.k8s.io"),
            "{reported}"
        );
        assert!(
            !log.lines()
                .any(|line| line.starts_with("PATCH") && line.contains("/sparkconnects/spc-trial")),
            "the SparkConnect must not be applied after the policy was refused: {log}"
        );
    }

    #[tokio::test]
    async fn running_applies_the_ingress_policy() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkconnects/spc-trial") {
                return (
                    404,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"not found","reason":"NotFound","code":404}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Starting"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"}}"#.into(),
            )
        });
        reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
            .await
            .expect("reconcile");
        let log = requests.lock().expect("requests").join("\n");
        let applied = log
            .lines()
            .find(|line| {
                line.starts_with("PATCH") && line.contains("/networkpolicies/spc-trial-ingress")
            })
            .unwrap_or_else(|| panic!("no ingress policy applied: {log}"));
        assert!(
            applied.contains("fieldManager=stardelt-operator"),
            "{applied}"
        );
    }

    #[tokio::test]
    async fn a_live_running_server_reports_running_with_an_endpoint() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkconnects/spc-trial") {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"},"status":{"state":"Running","server":{"serviceName":"spc-trial-server","podName":"spc-trial-server"}}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/pods/spc-trial-server") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"spc-trial-server","namespace":"data"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Running"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"}}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
                .await
                .expect("reconcile");
        // A Running cluster is the 300 s resync now that the `SparkConnect` is watched (ticket #497).
        assert_eq!(action, Action::requeue(Duration::from_secs(300)));
        let log = requests.lock().expect("requests").join("\n");
        assert!(log.contains("\"phase\":\"Running\""), "{log}");
        assert!(
            log.contains("\"endpoint\":\"sc://spc-trial-server.data.svc.cluster.local:15002\""),
            "{log}"
        );
        assert!(log.contains("\"startedAt\":\"20"), "{log}");
        // The driver pod carries no app selector, so the idle clock is not started.
        assert!(log.contains("\"lastActivityAt\":null"), "{log}");
    }

    #[tokio::test]
    async fn a_server_pod_in_crashloopbackoff_is_not_running() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkconnects/spc-trial") {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"},"status":{"state":"Running","server":{"serviceName":"spc-trial-server","podName":"spc-trial-server"}}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/pods/spc-trial-server") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"spc-trial-server","namespace":"data"},"status":{"conditions":[],"containerStatuses":[{"name":"spark-kubernetes-driver","state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Starting"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"}}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(30)));
        let log = requests.lock().expect("requests").join("\n");
        assert!(log.contains("\"phase\":\"Starting\""), "{log}");
        assert!(log.contains("\"endpoint\":null"), "{log}");
        assert!(
            log.contains("the server pod spc-trial-server is not ready (CrashLoopBackOff)"),
            "{log}"
        );
    }

    /// #2090: the SparkConnect is `spc-<cluster>`, so a dead server pod must delete
    /// `spc-trial` — not `spc-spc-trial-server`, which does not exist and 404s.
    #[tokio::test]
    async fn a_dead_server_pod_deletes_the_connect_and_says_so() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "DELETE" && path.contains("/sparkconnects/") {
                return (
                    200,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
                );
            }
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkconnects/spc-trial") {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"},"status":{"state":"NotReady","server":{"serviceName":"spc-trial-server","podName":"spc-trial-server"}}}"#.into(),
                );
            }
            if method == "GET" && path.contains("/pods/spc-trial-server") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"spc-trial-server","namespace":"data"},"status":{"conditions":[],"containerStatuses":[{"name":"spark-kubernetes-driver","state":{"terminated":{"exitCode":137,"reason":"Error","finishedAt":"2026-01-01T00:00:00Z"}}}]}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Starting"}}"#.into(),
                );
            }
            (
                200,
                r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"}}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(30)));
        let log = requests.lock().expect("requests").join("\n");
        let deletes: Vec<&str> = log
            .lines()
            .filter(|line| line.starts_with("DELETE") && line.contains("/sparkconnects/"))
            .collect();
        assert_eq!(deletes.len(), 1, "{log}");
        assert!(
            deletes[0].contains("/sparkconnects/spc-trial?"),
            "the SparkConnect is deleted by its own name: {log}"
        );
        let patches = status_patches(&requests);
        assert!(
            patches.iter().any(|p| p["status"]["message"]
                == json!("the server pod spc-trial-server exited with 137; recreating the server")),
            "{patches:?}"
        );
    }

    #[tokio::test]
    async fn stopped_with_a_live_object_deletes_it_and_reports_stopping() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "DELETE" && path.contains("/sparkconnects/spc-trial") {
                return (
                    200,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12"},"status":{"phase":"Stopping"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Stopped)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(10)));
        let log = requests.lock().expect("requests").join("\n");
        let deletes = log
            .lines()
            .filter(|line| line.starts_with("DELETE") && line.contains("/sparkconnects/spc-trial"))
            .count();
        assert_eq!(deletes, 1, "{log}");
        assert!(log.contains("\"phase\":\"Stopping\""), "{log}");
    }

    #[tokio::test]
    async fn stopped_without_a_live_object_reports_stopped() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "DELETE" && path.contains("/sparkconnects/spc-trial") {
                return (
                    404,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"not found","reason":"NotFound","code":404}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12"},"status":{"phase":"Stopped"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Stopped)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        assert!(log.contains("\"phase\":\"Stopped\""), "{log}");
        assert!(log.contains("\"endpoint\":null"), "{log}");
        assert!(log.contains("\"startedAt\":null"), "{log}");
    }

    #[tokio::test]
    async fn an_unknown_runtime_reports_failed_with_the_message() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{}"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-9.9-py3.99"},"status":{"phase":"Failed"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let mut c = cluster(SparkClusterState::Running);
        c.spec.runtime = "spark-9.9-py3.99".into();
        let action = reconcile_spark_cluster(Arc::new(c), ctx(client))
            .await
            .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        assert!(
            log.contains("unknown runtime spark-9.9-py3.99; known: spark-4.0-py3.12"),
            "{log}"
        );
        assert!(log.contains("\"phase\":\"Failed\""), "{log}");
        assert!(
            !log.contains("/sparkconnects/"),
            "nothing is applied: {log}"
        );
    }

    #[tokio::test]
    async fn a_missing_catalogue_reports_failed() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    404,
                    r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"not found","reason":"NotFound","code":404}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Failed"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let action =
            reconcile_spark_cluster(Arc::new(cluster(SparkClusterState::Running)), ctx(client))
                .await
                .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        assert!(
            log.contains("the runtime catalogue stardelt-spark-runtimes is missing"),
            "{log}"
        );
        assert!(log.contains("\"phase\":\"Failed\""), "{log}");
    }

    #[tokio::test]
    async fn a_connection_ref_reports_failed() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Failed"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let mut c = cluster(SparkClusterState::Running);
        c.spec.env = vec![SparkEnvVar {
            name: "TOKEN".into(),
            value: None,
            connection_ref: Some(ConnectionKeyRef {
                name: "store".into(),
                key: "token".into(),
            }),
        }];
        let action = reconcile_spark_cluster(Arc::new(c), ctx(client))
            .await
            .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        assert!(
            log.contains("connectionRef is not supported yet (internal#161 slice 7)"),
            "{log}"
        );
        assert!(log.contains("\"phase\":\"Failed\""), "{log}");
    }

    #[tokio::test]
    async fn an_invalid_cluster_reports_failed_before_touching_the_catalogue() {
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial/status") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Failed"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let mut c = cluster(SparkClusterState::Running);
        c.spec.spark_conf.insert(
            "spark.kubernetes.authenticate.driver.serviceAccountName".to_string(),
            "admin".to_string(),
        );
        let action = reconcile_spark_cluster(Arc::new(c), ctx(client))
            .await
            .expect("reconcile");
        assert_eq!(action, Action::await_change());
        let log = requests.lock().expect("requests").join("\n");
        assert!(log.contains("is managed by the platform"), "{log}");
        assert!(!log.contains("/configmaps/"), "{log}");
    }

    const NOW: &str = "2026-11-09T12:00:00Z";

    /// The wiring around the pure decision: an idle Running cluster is patched to `Stopped`
    /// and gets one `IdleStop` event.
    #[tokio::test]
    async fn an_idle_running_cluster_is_stopped_with_an_event() {
        let old = {
            let now = k8s_openapi::jiff::Timestamp::now();
            (now - std::time::Duration::from_secs(2 * 60 * 60)).to_string()
        };
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "GET" && path.ends_with("/pods/spc-trial-server-abc") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"spc-trial-server-abc","namespace":"data","labels":{"spark-app-selector":"app-1"}},"status":{"conditions":[{"type":"Ready","status":"True"}]}}"#.into(),
                );
            }
            if method == "GET" && path.ends_with("/pods") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[]}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkconnects/spc-trial") {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"},"status":{"state":"Running","server":{"serviceName":"spc-trial-server","podName":"spc-trial-server-abc"}}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Running"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let mut c = cluster(SparkClusterState::Running);
        c.status = Some(crate::crd::SparkClusterStatus {
            phase: "Running".into(),
            last_activity_at: Some(old.clone()),
            ..Default::default()
        });
        let action = reconcile_spark_cluster(Arc::new(c), ctx(client))
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(10)));
        let log = requests.lock().expect("requests").join("\n");
        assert!(
            log.contains("PATCH /apis/stardelt.io/v1alpha1/namespaces/data/sparkclusters/trial")
                && log.contains(r#""spec":{"state":"Stopped"}"#),
            "{log}"
        );
        assert!(log.contains("POST") && log.contains("/events"), "{log}");
        assert!(log.contains("\"reason\":\"IdleStop\""), "{log}");

        // A busy cluster is not stopped and keeps the idle clock fresh.
        let (client, requests) = mock_client(|method, path, _body| {
            if method == "GET" && path.contains("/configmaps/stardelt-spark-runtimes") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"stardelt-spark-runtimes","namespace":"data"},"data":{"spark-4.0-py3.12":"{\"image\":\"ghcr.io/example/spark@sha256:runtime\",\"sparkVersion\":\"4.0.1\",\"serviceAccount\":\"spark-runtime\",\"sparkConf\":{}}"}}"#.into(),
                );
            }
            if method == "GET" && path.ends_with("/pods/spc-trial-server-abc") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"Pod","metadata":{"name":"spc-trial-server-abc","namespace":"data","labels":{"spark-app-selector":"app-1"}},"status":{"conditions":[{"type":"Ready","status":"True"}]}}"#.into(),
                );
            }
            if method == "GET" && path.ends_with("/pods") {
                return (
                    200,
                    r#"{"apiVersion":"v1","kind":"PodList","metadata":{},"items":[{"metadata":{"name":"spc-trial-server-abc-exec-1","namespace":"data","labels":{"spark-role":"executor"}}}]}"#.into(),
                );
            }
            if method == "GET" && path.contains("/sparkconnects/spc-trial") {
                return (
                    200,
                    r#"{"apiVersion":"sparkoperator.k8s.io/v1alpha1","kind":"SparkConnect","metadata":{"name":"spc-trial","namespace":"data"},"status":{"state":"Running","server":{"serviceName":"spc-trial-server","podName":"spc-trial-server-abc"}}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/networkpolicies/") {
                return (
                    200,
                    r#"{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"spc-trial-ingress","namespace":"data"}}"#.into(),
                );
            }
            if method == "PATCH" && path.contains("/sparkclusters/trial") {
                return (
                    200,
                    r#"{"apiVersion":"stardelt.io/v1alpha1","kind":"SparkCluster","metadata":{"name":"trial","namespace":"data"},"spec":{"owner":{"group":"mobility-analytics"},"runtime":"spark-4.0-py3.12","state":"Running"},"status":{"phase":"Running"}}"#.into(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.into(),
            )
        });
        let mut c = cluster(SparkClusterState::Running);
        c.status = Some(crate::crd::SparkClusterStatus {
            phase: "Running".into(),
            last_activity_at: Some(old.clone()),
            ..Default::default()
        });
        let action = reconcile_spark_cluster(Arc::new(c), ctx(client))
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(300)));
        let log = requests.lock().expect("requests").join("\n");
        assert!(!log.contains("\"state\":\"Stopped\""), "{log}");
        assert!(!log.contains("/events"), "{log}");
        assert!(log.contains("\"lastActivityAt\":\"20"), "{log}");
    }

    fn now() -> k8s_openapi::jiff::Timestamp {
        NOW.parse().unwrap()
    }

    #[test]
    fn an_active_cluster_without_history_records_now() {
        let decision = idle_decision(None, true, 60, now());
        assert_eq!(decision.last_activity_at, Some(now().to_string()));
        assert!(!decision.stop);
    }

    #[test]
    fn an_active_cluster_keeps_a_fresh_heartbeat() {
        let decision = idle_decision(Some("2026-11-09T11:58:00Z"), true, 60, now());
        assert_eq!(
            decision.last_activity_at,
            Some("2026-11-09T11:58:00Z".to_string())
        );
        assert!(!decision.stop);
    }

    #[test]
    fn an_active_cluster_refreshes_a_stale_heartbeat() {
        let decision = idle_decision(Some("2026-11-09T11:54:00Z"), true, 60, now());
        assert_eq!(decision.last_activity_at, Some(now().to_string()));
        assert!(!decision.stop);
    }

    #[test]
    fn the_first_idle_poll_starts_the_clock() {
        let decision = idle_decision(None, false, 60, now());
        assert_eq!(decision.last_activity_at, Some(now().to_string()));
        assert!(!decision.stop);
    }

    #[test]
    fn idle_below_the_threshold_does_not_stop() {
        let decision = idle_decision(Some("2026-11-09T11:01:00Z"), false, 60, now());
        assert_eq!(
            decision.last_activity_at,
            Some("2026-11-09T11:01:00Z".to_string())
        );
        assert!(!decision.stop);
    }

    #[test]
    fn idle_at_the_threshold_stops() {
        let decision = idle_decision(Some("2026-11-09T11:00:00Z"), false, 60, now());
        assert!(decision.stop);
    }

    #[test]
    fn zero_minutes_never_stops() {
        let decision = idle_decision(Some("2026-11-09T02:00:00Z"), false, 0, now());
        assert!(!decision.stop);
    }

    #[test]
    fn a_malformed_stored_timestamp_is_treated_as_no_history() {
        let decision = idle_decision(Some("not a timestamp"), false, 60, now());
        assert_eq!(decision.last_activity_at, Some(now().to_string()));
        assert!(!decision.stop);
    }
}
