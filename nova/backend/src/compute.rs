//! The `SparkCluster` API (internal#161 slice 5a): the Spark compute a person may use.
//!
//! The operator translates a `SparkCluster` into the Spark Operator's `SparkConnect`; Nova
//! only exposes it — lists the clusters a caller may use and sets `spec.state` (Start and
//! Stop). Nothing in Nova is state of its own.
//!
//! Next to the `SparkCluster`s the page shows the Spark servers the platform's own manifests
//! install (`stardelt/spark-connect` and the demo sandbox, `kind: "platform"`, nova#271): they
//! are plain Deployments, so Nova reads the Deployment for the shape and the `<name>-config`
//! ConfigMap for the sizes, through a whitelist of keys.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::{Api, ApiResource, DeleteParams, ListParams, Patch, PatchParams, PostParams};
use kube::core::GroupVersionKind;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;

use crate::AppState;
use crate::api_json::ApiJson;
use crate::auth::{Identity, Principal};
use crate::connection_routes::Fail;
use crate::pipelines::{ResourceError, answer, no_cluster, resource_fail};

/// The Deployment label that marks a Spark server the manifests install: `shared` for the
/// members' server, `demo` for the visitors' sandbox (`platform/manifests/spark.yaml`,
/// `spark-demo-sandbox.yaml`).
const PLATFORM_LABEL: &str = "stardelt.io/compute";
/// The public demo's Spark namespace (`platform/manifests/spark-demo-sandbox.yaml`).
const DEMO_NAMESPACE: &str = "stardelt-demo-spark";
/// When the demo sandbox is restarted (`platform/manifests/demo-reset.yaml`).
const DEMO_RESET: &str = "daily 03:00 UTC";

/// The only `spark-defaults.conf` keys the API answers. The rest of that file is the platform's:
/// ServiceAccount names, the executor pod template, Secret references and paths.
const SPARK_CONF_KEYS: &[&str] = &[
    "spark.master",
    "spark.driver.memory",
    "spark.driver.maxResultSize",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.dynamicAllocation.minExecutors",
    "spark.dynamicAllocation.maxExecutors",
    "spark.dynamicAllocation.executorIdleTimeout",
];

/// The `SparkCluster`'s owner: a group or a person, exactly one of the two.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ComputeOwner {
    pub group: Option<String>,
    pub user: Option<String>,
}

/// The pod shape of a driver or executor (`spec.driver` / `spec.executor`).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ComputeResources {
    pub cores: Option<i32>,
    pub memory: Option<String>,
}

/// One environment variable of the driver and executors. `value` only: the operator
/// refuses `connectionRef` until internal#161 slice 7, so Nova does not offer it.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ComputeEnvVar {
    pub name: String,
    pub value: String,
}

/// One `SparkCluster`, as the Compute page needs it.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ComputeCluster {
    pub name: String,
    pub display_name: Option<String>,
    pub owner: ComputeOwner,
    pub size: String,
    pub runtime: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver: Option<ComputeResources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executor: Option<ComputeResources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_executors: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default)]
    pub env: Vec<ComputeEnvVar>,
    #[serde(default)]
    pub spark_conf: std::collections::BTreeMap<String, String>,
    pub phase: String,
    /// The operator's reason while the phase is `Failed` or `Starting` (for example a refused
    /// `SparkConnect`, a runtime that is not in the catalogue, or a driver that cannot start).
    pub message: Option<String>,
    pub endpoint: Option<String>,
    pub started_at: Option<String>,
    pub last_activity_at: Option<String>,
    pub idle_stop_minutes: i64,
    pub can_use: bool,
    /// Whether the caller may start, stop or edit the cluster: an administrator, the owner
    /// group of a `SparkCluster`, its `owner.user`, or an administrator on `shared` (#706).
    pub can_act: bool,
    /// The groups that may see the cluster without being its owner (`spec.viewers`).
    pub viewers: Vec<String>,
    pub shared: bool,
    /// `cluster` for a `SparkCluster`, `platform` for a Spark server the manifests install.
    pub kind: String,
    /// `kubernetes` (executors are pods) or `local` (the demo sandbox: two threads, none).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// The container's resource limits, on a platform server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limits: Option<ComputeResources>,
    /// `members` or `visitors`: who shares a platform server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    /// When a platform server is restarted on a schedule (`daily 03:00 UTC` on the sandbox).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resets_at: Option<String>,
    /// Whether this is the Spark the caller's own notebook uses.
    #[serde(default)]
    pub yours: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ComputeResponse {
    pub clusters: Vec<ComputeCluster>,
}

/// The owner block of a spec; a missing half stays `None`.
fn owner_of(spec: &Value) -> ComputeOwner {
    let text = |pointer: &str| {
        spec.pointer(pointer)
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    ComputeOwner {
        group: text("/owner/group"),
        user: text("/owner/user"),
    }
}

/// `spec.viewers`: the groups that may use a cluster whose `owner` names another group or a
/// person. Used only by `may_use`; starting and stopping stay with `may_act`.
fn viewers_of(spec: &Value) -> Vec<String> {
    spec.get("viewers")
        .and_then(Value::as_array)
        .map(|groups| {
            groups
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Whether the caller may see the cluster: an administrator, everyone for `shared`, the
/// owner's group, or the owner themselves (`owner.user` is the sign-in name) — or one of the
/// groups in `spec.viewers` (who may use it, not start or stop it).
fn may_use(who: &Identity, name: &str, owner: &ComputeOwner, viewers: &[String]) -> bool {
    who.is_admin
        || name == "shared"
        || owner
            .group
            .as_ref()
            .is_some_and(|group| who.groups.iter().any(|g| g == group))
        || owner.user.as_deref() == Some(who.display_name.as_str())
        || viewers
            .iter()
            .any(|group| who.groups.iter().any(|mine| mine == group))
}

/// Whether the caller may start or stop the cluster. The shared cluster is always on
/// (`idleStopMinutes: 0`): only an administrator starts or stops it.
fn may_act(who: &Identity, name: &str, owner: &ComputeOwner) -> bool {
    who.is_admin
        || (name != "shared"
            && (owner
                .group
                .as_ref()
                .is_some_and(|group| who.groups.iter().any(|g| g == group))
                || owner.user.as_deref() == Some(who.display_name.as_str())))
}

/// Whether the cluster's Connect endpoint belongs to the caller: an administrator, the owner's
/// group, the owner themselves, or everyone for `shared`. A group in `spec.viewers` may see the
/// cluster but not run code on it: the server acts with its owner group's rights (#493, #75).
fn may_connect(who: &Identity, name: &str, owner: &ComputeOwner) -> bool {
    may_act(who, name, owner) || name == "shared"
}

/// The phase the page shows: the operator's `status.phase` once it has reported, otherwise
/// what the request implies (`Starting` for a requested `Running`, `Stopped` for everything
/// else).
fn phase_of(object: &Value) -> String {
    if let Some(phase) = object
        .pointer("/status/phase")
        .and_then(Value::as_str)
        .filter(|phase| !phase.is_empty())
    {
        return phase.to_string();
    }
    if object.pointer("/spec/state").and_then(Value::as_str) == Some("Running") {
        "Starting".to_string()
    } else {
        "Stopped".to_string()
    }
}

/// The operator's `status.message`, once it has reported one. Empty is `None`.
fn message_of(object: &Value) -> Option<String> {
    object
        .pointer("/status/message")
        .and_then(Value::as_str)
        .filter(|message| !message.is_empty())
        .map(str::to_string)
}

/// The environment variables of a spec, skipping entries that carry no `value` (a
/// `connectionRef` entry cannot be edited and is not shown).
fn env_of(spec: &Value) -> Vec<ComputeEnvVar> {
    spec.get("env")
        .and_then(Value::as_array)
        .map(|vars| {
            vars.iter()
                .filter_map(|var| {
                    Some(ComputeEnvVar {
                        name: var.get("name")?.as_str()?.to_string(),
                        value: var.get("value")?.as_str()?.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// One cluster as the API answers it, or `None` when the caller may not use it.
fn view(object: &Value, who: &Identity) -> Option<ComputeCluster> {
    let name = object
        .pointer("/metadata/name")
        .and_then(Value::as_str)?
        .to_string();
    let spec = object.get("spec").unwrap_or(object);
    let owner = owner_of(spec);
    let viewers = viewers_of(spec);
    if !may_use(who, &name, &owner, &viewers) {
        return None;
    }
    let connect = may_connect(who, &name, &owner);
    let act = may_act(who, &name, &owner);
    let status = object.get("status").unwrap_or(&Value::Null);
    let text = |value: Option<&Value>| value.and_then(Value::as_str).map(str::to_string);
    let resources = |pointer: &str| {
        spec.get(pointer)
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    };
    Some(ComputeCluster {
        display_name: text(spec.get("displayName")),
        shared: name == "shared",
        name,
        owner,
        size: spec
            .get("size")
            .and_then(Value::as_str)
            .unwrap_or("small")
            .to_string(),
        runtime: spec
            .get("runtime")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        driver: resources("driver"),
        executor: resources("executor"),
        max_executors: spec
            .get("maxExecutors")
            .and_then(Value::as_i64)
            .and_then(|n| i32::try_from(n).ok()),
        image: text(spec.get("image")),
        env: env_of(spec),
        spark_conf: spec
            .get("sparkConf")
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default(),
        phase: phase_of(object),
        message: message_of(object),
        endpoint: if connect {
            text(status.get("endpoint"))
        } else {
            None
        },
        started_at: text(status.get("startedAt")),
        last_activity_at: text(status.get("lastActivityAt")),
        idle_stop_minutes: spec
            .get("idleStopMinutes")
            .and_then(Value::as_i64)
            .unwrap_or(60),
        can_use: connect,
        can_act: act,
        viewers,
        kind: "cluster".to_string(),
        mode: None,
        limits: None,
        audience: None,
        resets_at: None,
        yours: false,
    })
}

/// Every cluster the caller may use, sorted by name. Pure: no cluster.
pub fn visible_clusters(objects: &[Value], who: &Identity) -> Vec<ComputeCluster> {
    let mut views: Vec<ComputeCluster> = objects
        .iter()
        .filter_map(|object| view(object, who))
        .collect();
    views.sort_by(|a, b| a.name.cmp(&b.name));
    views
}

/// A visitor of the public demo: `27-demo-mode` gives their notebook the sandbox as `spark`.
fn is_visitor(who: &Identity) -> bool {
    who.groups.iter().any(|group| group == "visitors")
}

/// The `spark-defaults.conf` a platform server runs with, whitelisted to `SPARK_CONF_KEYS`.
/// Spark accepts `key value` (the platform's own files, aligned with spaces) and `key = value`;
/// comments and blanks are dropped and a key outside the whitelist never leaves this function.
/// Pure, so the parse is testable without a cluster.
fn spark_defaults(text: &str) -> std::collections::BTreeMap<String, String> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| match line.split_once('=') {
            Some((key, value)) => Some((key.trim(), value.trim())),
            None => {
                let mut parts = line.splitn(2, char::is_whitespace);
                let key = parts.next()?.trim();
                let value = parts.next().unwrap_or_default().trim();
                (!value.is_empty()).then_some((key, value))
            }
        })
        .filter(|(key, _)| SPARK_CONF_KEYS.contains(key))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

/// The tag of an image reference: `ghcr.io/stardelt/spark:dev@sha256:…` → `dev`.
fn image_tag(image: &str) -> String {
    let without_digest = image.split('@').next().unwrap_or(image);
    match without_digest.rsplit_once(':') {
        Some((_, tag)) if !tag.is_empty() && !tag.contains('/') => tag.to_string(),
        _ => String::new(),
    }
}

/// A limit's CPU as whole cores: `1` → 1, `2000m` → 2. A fractional quota (`500m`, `250m`) is
/// not a core count and answers `None`.
fn cores_of(text: &str) -> Option<i32> {
    if let Some(milli) = text.trim().strip_suffix('m') {
        let milli: i64 = milli.trim().parse().ok()?;
        return (milli > 0 && milli % 1000 == 0).then_some((milli / 1000) as i32);
    }
    text.trim().parse().ok()
}

/// One Spark server the manifests install, as the Compute page needs it. `defaults` is the
/// Deployment's `<name>-config` ConfigMap: without it the card renders without specs.
fn platform_view(
    deployment: &Value,
    defaults: Option<&Value>,
    who: &Identity,
) -> Option<ComputeCluster> {
    let name = deployment
        .pointer("/metadata/name")
        .and_then(Value::as_str)?
        .to_string();
    // The value of the `stardelt.io/compute` label; `/` is escaped as `~1` in a JSON pointer.
    let audience = match deployment
        .pointer("/metadata/labels/stardelt.io~1compute")
        .and_then(Value::as_str)
    {
        Some("demo") => Some("visitors".to_string()),
        Some("shared") => Some("members".to_string()),
        _ => None,
    };
    let container = deployment.pointer("/spec/template/spec/containers/0");
    let image = container
        .and_then(|container| container.get("image"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let limits = container
        .and_then(|container| container.pointer("/resources/limits"))
        .map(|limits| ComputeResources {
            cores: limits.get("cpu").and_then(Value::as_str).and_then(cores_of),
            memory: limits
                .get("memory")
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    let spark_conf = defaults
        .and_then(|defaults| defaults.pointer("/data/spark-defaults.conf"))
        .and_then(Value::as_str)
        .map(spark_defaults)
        .unwrap_or_default();
    let mode = match spark_conf.get("spark.master").map(String::as_str) {
        Some(master) if master.starts_with("local") => "local",
        _ => "kubernetes",
    };
    let driver = spark_conf
        .get("spark.driver.memory")
        .map(|memory| ComputeResources {
            cores: None,
            memory: Some(memory.clone()),
        });
    let executor_cores = spark_conf
        .get("spark.executor.cores")
        .and_then(|cores| cores.parse::<i32>().ok());
    let executor_memory = spark_conf.get("spark.executor.memory").cloned();
    let executor =
        (executor_cores.is_some() || executor_memory.is_some()).then_some(ComputeResources {
            cores: executor_cores,
            memory: executor_memory,
        });
    let ready = deployment
        .pointer("/status/readyReplicas")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let visitors = audience.as_deref() == Some("visitors");
    Some(ComputeCluster {
        name,
        display_name: None,
        owner: ComputeOwner {
            // The group that shares it: the members' server belongs to `stardelt-users`, the
            // sandbox to `visitors`. `audience` carries the same fact in the words the page shows.
            group: audience.as_ref().map(|_| {
                if visitors {
                    "visitors".to_string()
                } else {
                    "stardelt-users".to_string()
                }
            }),
            user: None,
        },
        // Not a `SparkCluster` class: the card shows the limit block instead.
        size: "platform".to_string(),
        runtime: image.as_deref().map(image_tag).unwrap_or_default(),
        driver,
        executor,
        max_executors: spark_conf
            .get("spark.dynamicAllocation.maxExecutors")
            .and_then(|max| max.parse::<i32>().ok()),
        image,
        env: Vec::new(),
        spark_conf,
        phase: if ready >= 1 { "Running" } else { "Stopped" }.to_string(),
        message: None,
        endpoint: None,
        started_at: None,
        last_activity_at: None,
        idle_stop_minutes: 0,
        can_use: true,
        can_act: false,
        viewers: Vec::new(),
        shared: audience.as_deref() == Some("members"),
        kind: "platform".to_string(),
        mode: Some(mode.to_string()),
        limits,
        audience: audience.clone(),
        resets_at: visitors.then(|| DEMO_RESET.to_string()),
        yours: if is_visitor(who) {
            visitors
        } else {
            audience.as_deref() == Some("members")
        },
    })
}

/// What the page shows: every cluster the caller may use, then the visitor rule. A visitor's
/// `spark` is the demo sandbox (`27-demo-mode`), and `spark-connect-ingress` refuses them on the
/// shared server — so their page carries the sandbox only. Everyone else sees all.
fn visible_to(mut clusters: Vec<ComputeCluster>, who: &Identity) -> Vec<ComputeCluster> {
    if is_visitor(who) {
        clusters.retain(|cluster| {
            cluster.kind == "platform" && cluster.audience.as_deref() == Some("visitors")
        });
    }
    clusters
}

/// The Spark servers the manifests install: Deployments labelled `stardelt.io/compute` in Nova's
/// namespace and in the demo sandbox's, each with the `<name>-config` ConfigMap next to it.
///
/// A namespace Nova may not read (no Role) and an API that does not answer are not errors: the
/// list is logged and the page keeps what it has — a missing card, never a `502`. A missing
/// ConfigMap only costs the specs.
async fn platform_servers(state: &AppState, who: &Identity) -> Vec<ComputeCluster> {
    let Some(client) = state.kube.clone() else {
        return Vec::new();
    };
    let mut servers = Vec::new();
    for namespace in [crate::cluster::namespace(), DEMO_NAMESPACE.to_string()] {
        let deployments: Api<Deployment> = Api::namespaced(client.clone(), &namespace);
        let configmaps: Api<ConfigMap> = Api::namespaced(client.clone(), &namespace);
        let list = match deployments
            .list(&ListParams::default().labels(PLATFORM_LABEL))
            .await
        {
            Ok(list) => list,
            Err(error) => {
                tracing::warn!(namespace = %namespace, error = %error, "platform Spark servers could not be listed");
                continue;
            }
        };
        for deployment in list.items {
            let Ok(value) = serde_json::to_value(&deployment) else {
                continue;
            };
            let Some(name) = value.pointer("/metadata/name").and_then(Value::as_str) else {
                continue;
            };
            let defaults = configmaps
                .get(&format!("{name}-config"))
                .await
                .ok()
                .and_then(|configmap| serde_json::to_value(&configmap).ok());
            if let Some(server) = platform_view(&value, defaults.as_ref(), who) {
                servers.push(server);
            }
        }
    }
    servers
}

/// The one row without a cluster (demo mode, a local run): the shared cluster is always on.
fn demo_clusters() -> Vec<ComputeCluster> {
    vec![ComputeCluster {
        name: "shared".to_string(),
        display_name: Some("Shared".to_string()),
        owner: ComputeOwner {
            group: Some("stardelt-users".to_string()),
            user: None,
        },
        size: "small".to_string(),
        runtime: "spark-4.1-py3.13".to_string(),
        driver: None,
        executor: None,
        max_executors: None,
        image: None,
        env: Vec::new(),
        spark_conf: std::collections::BTreeMap::new(),
        phase: "Running".to_string(),
        message: None,
        endpoint: None,
        started_at: None,
        last_activity_at: None,
        idle_stop_minutes: 0,
        can_use: true,
        can_act: false,
        viewers: Vec::new(),
        shared: true,
        kind: "cluster".to_string(),
        mode: None,
        limits: None,
        audience: None,
        resets_at: None,
        yours: false,
    }]
}

/// The `SparkCluster` resources of Nova's namespace. `None` without a cluster connection.
fn compute_api(state: &AppState) -> Result<Api<kube::api::DynamicObject>, Fail> {
    let Some(client) = state.kube.clone() else {
        return Err(no_cluster());
    };
    Ok(Api::namespaced_with(
        client,
        &crate::cluster::namespace(),
        &ApiResource::from_gvk(&GroupVersionKind::gvk(
            "stardelt.io",
            "v1alpha1",
            "SparkCluster",
        )),
    ))
}

/// Who the caller is. `dev` mode has no IdP: the single user is the operator, an
/// administrator with no groups — exactly like `warehouses::actor`.
fn actor(principal: Option<Extension<Principal>>, state: &AppState) -> Identity {
    match principal {
        Some(Extension(principal)) => principal.identity,
        None => Identity {
            sub: state.dev_user.clone(),
            display_name: state.dev_user.clone(),
            email: None,
            name: None,
            groups: Vec::new(),
            roles: Vec::new(),
            groups_overage: false,
            is_admin: true,
        },
    }
}

fn not_found() -> Fail {
    // Indistinguishable from a cluster that does not exist (and from one the caller may not
    // use).
    Fail::new(StatusCode::NOT_FOUND, "not found")
}

/// Every cluster of Nova's namespace the caller may use.
async fn list_clusters(state: &AppState, who: &Identity) -> Result<Vec<ComputeCluster>, Fail> {
    let api = compute_api(state)?;
    let objects = api
        .list(&kube::api::ListParams::default())
        .await
        .map_err(|e| resource_fail(ResourceError::Unavailable(e.to_string())))?;
    let mut values = Vec::with_capacity(objects.items.len());
    for object in objects.items {
        values.push(
            serde_json::to_value(object)
                .map_err(|e| resource_fail(ResourceError::Unavailable(e.to_string())))?,
        );
    }
    let mut clusters = visible_clusters(&values, who);
    clusters.extend(platform_servers(state, who).await);
    Ok(visible_to(clusters, who))
}

/// Set `spec.state` and answer the updated cluster. Start and Stop are the same verb with a
/// different desired state, and both are idempotent.
async fn set_state(
    state: &AppState,
    who: &Identity,
    name: &str,
    desired: &str,
) -> Result<ComputeCluster, Fail> {
    let api = compute_api(state)?;
    let object = api.get(name).await.map_err(|e| match e {
        kube::Error::Api(er) if er.code == 404 => not_found(),
        other => resource_fail(ResourceError::Unavailable(other.to_string())),
    })?;
    let value = serde_json::to_value(&object)
        .map_err(|e| resource_fail(ResourceError::Unavailable(e.to_string())))?;
    let Some(current) = view(&value, who) else {
        return Err(not_found());
    };
    let spec = value.get("spec").unwrap_or(&value);
    let owner = owner_of(spec);
    if !may_act(who, name, &owner) {
        let sentence = if name == "shared" {
            "only an administrator may start or stop the shared cluster"
        } else if viewers_of(spec)
            .iter()
            .any(|group| who.groups.iter().any(|own| own == group))
        {
            "this cluster belongs to another group: only its owner group starts and stops it, ask them to"
        } else {
            "only the cluster's owner group may start or stop it"
        };
        return Err(Fail::new(StatusCode::FORBIDDEN, sentence));
    }
    match phase_of(&value).as_str() {
        "Starting" => {
            return Err(Fail::new(StatusCode::CONFLICT, "the cluster is starting"));
        }
        "Stopping" => {
            return Err(Fail::new(StatusCode::CONFLICT, "the cluster is stopping"));
        }
        _ => {}
    }
    if spec.get("state").and_then(Value::as_str) == Some(desired) {
        return Ok(current);
    }
    let patched = api
        .patch(
            name,
            &PatchParams::default(),
            &Patch::Merge(json!({"spec": {"state": desired}})),
        )
        .await
        .map_err(|e| match e {
            kube::Error::Api(er) if er.code == 404 => not_found(),
            other => resource_fail(ResourceError::Unavailable(other.to_string())),
        })?;
    let patched = serde_json::to_value(&patched)
        .map_err(|e| resource_fail(ResourceError::Unavailable(e.to_string())))?;
    view(&patched, who).ok_or_else(|| {
        Fail::new(
            StatusCode::CONFLICT,
            "the cluster changed while it was patched",
        )
    })
}

/// `GET /api/compute`: the Spark clusters this caller may use.
#[utoipa::path(
    get,
    operation_id = "compute_list",
    path = "/api/compute",
    tag = "public",
    description = "The Spark clusters the caller may use: an administrator sees every `SparkCluster` of the platform namespace, everyone else the ones owned by one of their groups, owned by them (`owner.user` is the sign-in name) and the `shared` one. Each row carries the owner, the class `size`, the `runtime`, the operator's `phase` (`Starting` for a requested `Running` before it has reported, `Stopped` otherwise) and `message`, the operator's reason while a cluster is `Failed` (a refused `SparkConnect`, a runtime that is not in the catalogue, a driver that cannot start), the Connect `endpoint` and `startedAt` while it runs (`endpoint` is absent when the caller reaches the cluster only through `spec.viewers`: such a server acts with its owner group's rights, so Nova issues a viewer no channel — #493), `lastActivityAt` once the operator reports it, `idleStopMinutes` (`0` on the shared cluster), `canUse`, `canAct`, whether the caller may start, stop or edit the cluster, and `shared`. Next to them the Spark servers the platform's own manifests install appear with `kind: platform` — the shared server and the public demo's sandbox — carrying `mode` (`kubernetes` or `local`), the container `limits`, who shares it (`audience`), when it is restarted (`resetsAt`) and `yours`, whether it is the Spark the caller's own notebook uses. A visitor of the public demo sees the sandbox only: their `spark` is not the shared server. Nothing in Nova is state of its own: the operator translates a `SparkCluster` into the Spark server. Without a cluster connection (demo mode, a local run) the answer is the one shared cluster. Answers `Cache-Control: no-store`.",
    responses(
        (status = 200, description = "The caller's clusters, sorted by name", body = ComputeResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn list(
    State(state): State<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
) -> Response {
    let identity = actor(principal, &state);
    let result = if state.kube.is_none() {
        Ok(ComputeResponse {
            clusters: demo_clusters(),
        })
    } else {
        list_clusters(&state, &identity)
            .await
            .map(|clusters| ComputeResponse { clusters })
    };
    match result {
        Ok(body) => (
            [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
            Json(body),
        )
            .into_response(),
        Err(fail) => fail.into_response(),
    }
}

/// `POST /api/compute/{name}/start`: set `spec.state: Running`.
#[utoipa::path(
    post,
    operation_id = "compute_start",
    path = "/api/compute/{name}/start",
    tag = "public",
    description = "Start one Spark cluster by merge-patching `spec.state: Running`; the answer is the updated cluster. Idempotent: a cluster already `Running` answers its view without a patch. A cluster the caller may not use and a name that does not exist both answer the same `404`. A non-administrator on the `shared` cluster answers `403` (it is always on). A cluster the operator is still `Starting` or `Stopping` answers `409`.",
    params(
        ("name" = String, Path, description = "The cluster's name"),
    ),
    responses(
        (status = 200, description = "The cluster, with `spec.state: Running` applied", body = ComputeCluster),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 403, description = "A non-administrator on the shared cluster", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such cluster, or the caller may not use it", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The cluster is starting or stopping", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn start(
    State(state): State<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    Path(name): Path<String>,
) -> Response {
    let identity = actor(principal, &state);
    answer(set_state(&state, &identity, &name, "Running").await)
}

/// `POST /api/compute/{name}/stop`: set `spec.state: Stopped`.
#[utoipa::path(
    post,
    operation_id = "compute_stop",
    path = "/api/compute/{name}/stop",
    tag = "public",
    description = "Stop one Spark cluster by merge-patching `spec.state: Stopped`; the answer is the updated cluster. Idempotent: a cluster already `Stopped` answers its view without a patch. A cluster the caller may not use and a name that does not exist both answer the same `404`. A non-administrator on the `shared` cluster answers `403` (it is always on). A cluster the operator is still `Starting` or `Stopping` answers `409`.",
    params(
        ("name" = String, Path, description = "The cluster's name"),
    ),
    responses(
        (status = 200, description = "The cluster, with `spec.state: Stopped` applied", body = ComputeCluster),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 403, description = "A non-administrator on the shared cluster", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such cluster, or the caller may not use it", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The cluster is starting or stopping", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn stop(
    State(state): State<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    Path(name): Path<String>,
) -> Response {
    let identity = actor(principal, &state);
    answer(set_state(&state, &identity, &name, "Stopped").await)
}

/// The body of `PUT /api/admin/compute/{name}`: everything an administrator may set.
/// No `state` — Start and Stop own it — and no `metadata`.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ComputeSpecBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub owner: ComputeOwner,
    /// Groups that may see the cluster without being its owner (`spec.viewers`), read-only.
    /// Absent keeps what the cluster carries; an empty list removes the key. At most 16.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub viewers: Option<Vec<String>>,
    #[serde(default = "default_size")]
    pub size: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver: Option<ComputeResources>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<ComputeResources>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_executors: Option<i32>,
    pub runtime: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default)]
    pub env: Vec<ComputeEnvVar>,
    #[serde(default)]
    pub spark_conf: std::collections::BTreeMap<String, String>,
    #[serde(default = "default_idle_stop_minutes")]
    pub idle_stop_minutes: i32,
}

fn default_size() -> String {
    "small".to_string()
}

fn default_idle_stop_minutes() -> i32 {
    60
}

/// A cluster name: 1 to 40 lower-case letters, digits and dashes, starting with a letter
/// and not ending in a dash — the same rule as the CRD's.
fn cluster_name_ok(name: &str) -> bool {
    let bytes = name.as_bytes();
    !name.is_empty()
        && name.len() <= 40
        && bytes[0].is_ascii_lowercase()
        && bytes[bytes.len() - 1] != b'-'
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// One person's user name: 1 to 128 letters, digits, dots, dashes or underscores, starting
/// with a letter or digit.
fn user_ok(user: &str) -> bool {
    !user.is_empty()
        && user.len() <= 128
        && user.as_bytes()[0].is_ascii_alphanumeric()
        && user
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'@' | b'-'))
}

/// `4Gi` or `512Mi`: digits without a leading zero, then exactly one of the two units.
fn memory_ok(memory: &str) -> bool {
    let Some(number) = memory
        .strip_suffix("Gi")
        .or_else(|| memory.strip_suffix("Mi"))
    else {
        return false;
    };
    !number.is_empty() && !number.starts_with('0') && number.bytes().all(|b| b.is_ascii_digit())
}

/// `spark-4.0-py3.12`: a two-part Spark version and a Python minor version.
fn runtime_ok(runtime: &str) -> bool {
    let Some(rest) = runtime.strip_prefix("spark-") else {
        return false;
    };
    let Some((spark, python)) = rest.split_once("-py3.") else {
        return false;
    };
    let digits = |value: &str| !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit());
    let spark_ok = spark
        .split_once('.')
        .is_some_and(|(major, minor)| digits(major) && digits(minor));
    spark_ok && digits(python)
}

/// An environment variable name: upper-case letters, digits and underscores, starting with
/// a letter or underscore, at most 64 bytes.
fn env_name_ok(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && matches!(name.as_bytes()[0], b'A'..=b'Z' | b'_')
        && name
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

/// Settings the platform owns: the Kubernetes identity of the pods, the catalog credentials
/// and the object-store keys a `Connection` supplies (the same predicate the operator uses).
fn managed_spark_conf_key(key: &str) -> bool {
    key.starts_with("spark.kubernetes.authenticate.")
        || key == "spark.kubernetes.namespace"
        || key.contains("serviceAccount")
        || (key.starts_with("spark.hadoop.fs.s3a.") && key.contains("key"))
        || (key.starts_with("spark.sql.catalog.")
            && (key.contains("credential") || key.contains("token")))
}

/// The name #1100 derives for a group's driver ServiceAccount; the twin of
/// `operator/crates/stardelt-operator/src/spark.rs::cluster_driver_for`.
fn driver_service_account(group: &str) -> String {
    format!("spark-cluster-driver-{group}")
}

/// Whether `group` can be part of that name: an RFC 1123 subdomain (#1111).
fn driver_service_account_name_ok(group: &str) -> bool {
    let name = driver_service_account(group);
    !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
}

/// The rules `SparkClusterSpec::validate` enforces in the operator, in the same words: a
/// body the CRD would refuse is a `400` from Nova, never the API server's message.
fn validate_spec(name: &str, body: &ComputeSpecBody) -> Result<(), String> {
    if !cluster_name_ok(name) {
        return Err(format!(
            "spark cluster name `{name}` must be 1 to 40 lower-case letters, digits and dashes, starting with a letter"
        ));
    }
    let group = body.owner.group.as_deref().filter(|s| !s.is_empty());
    let user = body.owner.user.as_deref().filter(|s| !s.is_empty());
    if group.is_some() == user.is_some() {
        return Err("set exactly one of owner.group and owner.user".to_string());
    }
    if let Some(group) = group
        && !crate::connections::group_ok(group)
    {
        return Err("owner.group must be a valid group name".to_string());
    }
    if let Some(group) = group
        && !driver_service_account_name_ok(group)
    {
        return Err(format!(
            "owner.group `{group}` would make the driver ServiceAccount `{}`, which is not a valid Kubernetes name: use lower-case letters, digits, '-' and '.', starting and ending with a letter or digit",
            driver_service_account(group)
        ));
    }
    if let Some(user) = user
        && !user_ok(user)
    {
        return Err("owner.user must be a valid user name".to_string());
    }
    if let Some(viewers) = &body.viewers {
        if viewers.len() > 16 {
            return Err("viewers holds at most 16 groups".to_string());
        }
        if let Some(bad) = viewers
            .iter()
            .find(|group| !crate::notebooks::valid_group(group))
        {
            return Err(format!("viewers holds `{bad}`, which is not a group name"));
        }
    }
    if let Some(display_name) = &body.display_name
        && display_name.chars().count() > 100
    {
        return Err("displayName must be at most 100 characters".to_string());
    }
    if !matches!(body.size.as_str(), "small" | "medium" | "large") {
        return Err("size must be small, medium or large".to_string());
    }
    for (what, resources) in [
        ("driver", body.driver.as_ref()),
        ("executor", body.executor.as_ref()),
    ] {
        let Some(resources) = resources else {
            continue;
        };
        if let Some(cores) = resources.cores
            && !(1..=16).contains(&cores)
        {
            return Err(format!("{what}.cores must be between 1 and 16"));
        }
        if let Some(memory) = &resources.memory
            && !memory_ok(memory)
        {
            return Err(format!("{what}.memory must look like `4Gi` or `512Mi`"));
        }
    }
    if let Some(max) = body.max_executors
        && !(0..=50).contains(&max)
    {
        return Err("maxExecutors must be between 0 and 50".to_string());
    }
    if !runtime_ok(&body.runtime) {
        return Err(format!(
            "runtime `{}` must look like `spark-4.0-py3.12`",
            body.runtime
        ));
    }
    if let Some(image) = &body.image {
        if image.len() > 512 {
            return Err("image must be at most 512 characters".to_string());
        }
        if !image.contains("@sha256:") {
            return Err("pin the image by digest".to_string());
        }
    }
    if body.env.len() > 50 {
        return Err("env must have at most 50 entries".to_string());
    }
    let mut seen = std::collections::BTreeSet::new();
    for var in &body.env {
        if !env_name_ok(&var.name) {
            return Err(format!(
                "env name `{}` must be upper-case letters, digits and underscores, starting with a letter or underscore",
                var.name
            ));
        }
        if !seen.insert(var.name.as_str()) {
            return Err(format!(
                "each env var name may appear once in env: `{}` appears twice",
                var.name
            ));
        }
        if var.value.len() > 1024 {
            return Err(format!(
                "env `{}` value must be at most 1024 characters",
                var.name
            ));
        }
    }
    if body.spark_conf.len() > 100 {
        return Err("sparkConf must have at most 100 entries".to_string());
    }
    for key in body.spark_conf.keys() {
        if managed_spark_conf_key(key) {
            return Err(format!("sparkConf key {key} is managed by the platform"));
        }
    }
    if !(0..=1440).contains(&body.idle_stop_minutes) {
        return Err("idleStopMinutes must be between 0 and 1440".to_string());
    }
    if body.idle_stop_minutes == 0 && name != "shared" {
        return Err("only the shared cluster may run without an idle stop".to_string());
    }
    Ok(())
}

/// The `spec.viewers` a replace writes, the pipeline rule: `None` keeps the stored list (a form
/// save must not wipe what a bundle or kubectl set), `Some([])` removes the key, a list writes it.
fn write_viewers(
    spec: &mut serde_json::Value,
    viewers: Option<&[String]>,
    stored: Option<&serde_json::Value>,
) {
    match viewers {
        Some(viewers) if !viewers.is_empty() => spec["viewers"] = json!(viewers),
        Some(_) => {
            if let Some(map) = spec.as_object_mut() {
                map.remove("viewers");
            }
        }
        None => {
            if let Some(stored) = stored
                .and_then(|object| object.pointer("/spec/viewers"))
                .cloned()
            {
                spec["viewers"] = stored;
            }
        }
    }
}

/// `PUT /api/admin/compute/{name}`: create the cluster, or replace it as a whole.
#[utoipa::path(
    put,
    operation_id = "admin_compute_upsert",
    path = "/api/admin/compute/{name}",
    tag = "internal",
    description = "Create or replace one `SparkCluster` as a whole: `displayName`, `owner`, `size`, `driver`, `executor`, `maxExecutors`, `runtime`, `image`, `env`, `sparkConf` and `idleStopMinutes`. Administrator-only (the administrator gate is `main`'s). `state` is not part of the body: Start and Stop own it, and a create starts `Stopped` while a replace keeps the state the cluster has. The body is checked against the CRD's rules first, so a value the API server would refuse answers `400` with the CRD's sentence. Answers the `ComputeCluster` view of the written object. `Cache-Control: no-store`.",
    params(
        ("name" = String, Path, description = "The cluster's name"),
    ),
    request_body = ComputeSpecBody,
    responses(
        (status = 200, description = "The cluster as written", body = ComputeCluster),
        (status = 400, description = "The body is not a valid SparkCluster spec", body = crate::openapi::ErrorResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The cluster changed while it was saved", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn upsert(
    State(state): State<Arc<AppState>>,
    principal: Option<Extension<Principal>>,
    Path(name): Path<String>,
    ApiJson(body): ApiJson<ComputeSpecBody>,
) -> Response {
    let who = actor(principal, &state);
    if let Err(message) = validate_spec(&name, &body) {
        return Fail::new(StatusCode::BAD_REQUEST, message).into_response();
    }
    let api = match compute_api(&state) {
        Ok(api) => api,
        Err(fail) => return fail.into_response(),
    };
    let existing = match api.get(&name).await {
        Ok(object) => Some(object),
        Err(kube::Error::Api(e)) if e.code == 404 => None,
        Err(e) => return resource_fail(ResourceError::Unavailable(e.to_string())).into_response(),
    };
    let mut spec = serde_json::to_value(&body).expect("a valid body is JSON");
    let state_word = existing
        .as_ref()
        .and_then(|o| o.data.pointer("/spec/state").and_then(Value::as_str))
        .unwrap_or("Stopped")
        .to_string();
    spec["state"] = json!(state_word);
    write_viewers(
        &mut spec,
        body.viewers.as_deref(),
        existing.as_ref().map(|object| &object.data),
    );
    let object = kube::api::DynamicObject {
        types: Some(kube::core::TypeMeta {
            api_version: "stardelt.io/v1alpha1".into(),
            kind: "SparkCluster".into(),
        }),
        metadata: kube::core::ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(crate::cluster::namespace()),
            resource_version: existing
                .as_ref()
                .and_then(|o| o.metadata.resource_version.clone()),
            ..Default::default()
        },
        data: json!({ "spec": spec }),
    };
    let written = match &existing {
        None => api.create(&PostParams::default(), &object).await,
        Some(_) => api.replace(&name, &PostParams::default(), &object).await,
    };
    let written = match written {
        Ok(object) => object,
        Err(kube::Error::Api(e)) if e.code == 409 => {
            return Fail::new(
                StatusCode::CONFLICT,
                "the cluster changed while it was saved",
            )
            .into_response();
        }
        Err(kube::Error::Api(e)) if e.code == 404 => return not_found().into_response(),
        Err(e) => return resource_fail(ResourceError::Unavailable(e.to_string())).into_response(),
    };
    let value = match serde_json::to_value(&written) {
        Ok(value) => value,
        Err(e) => return resource_fail(ResourceError::Unavailable(e.to_string())).into_response(),
    };
    match view(&value, &who) {
        Some(cluster) => answer(Ok::<ComputeCluster, Fail>(cluster)),
        None => Fail::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "the cluster changed while it was saved",
        )
        .into_response(),
    }
}

/// `DELETE /api/admin/compute/{name}`: remove one cluster, never the shared one.
#[utoipa::path(
    delete,
    operation_id = "admin_compute_delete",
    path = "/api/admin/compute/{name}",
    tag = "internal",
    description = "Delete one `SparkCluster`. Administrator-only (the administrator gate is `main`'s). The `shared` cluster is part of the platform and answers `409`: stop it instead. A name that does not exist answers `404`. Answers `204` with no body. `Cache-Control: no-store`.",
    params(
        ("name" = String, Path, description = "The cluster's name"),
    ),
    responses(
        (status = 204, description = "The cluster is gone"),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such cluster", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The shared cluster cannot be deleted", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn remove(
    State(state): State<Arc<AppState>>,
    _principal: Option<Extension<Principal>>,
    Path(name): Path<String>,
) -> Response {
    if name == "shared" {
        return Fail::new(
            StatusCode::CONFLICT,
            "the shared cluster is part of the platform; stop it instead of deleting it",
        )
        .into_response();
    }
    let api = match compute_api(&state) {
        Ok(api) => api,
        Err(fail) => return fail.into_response(),
    };
    match api.delete(&name, &DeleteParams::default()).await {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(kube::Error::Api(e)) if e.code == 404 => not_found().into_response(),
        Err(e) => resource_fail(ResourceError::Unavailable(e.to_string())).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::{Value, json};

    use super::{
        may_act, may_use, message_of, owner_of, phase_of, platform_view, spark_defaults,
        validate_spec, view, viewers_of, visible_clusters, visible_to, write_viewers,
    };
    use crate::auth::Identity;

    fn who(user: &str, groups: &[&str], admin: bool) -> Identity {
        Identity {
            sub: format!("{user}-sub"),
            display_name: user.into(),
            email: None,
            name: None,
            groups: groups.iter().map(|g| (*g).to_string()).collect(),
            roles: vec![],
            groups_overage: false,
            is_admin: admin,
        }
    }

    /// A `SparkCluster` object as the API server holds it.
    fn cluster(name: &str, spec: Value) -> Value {
        json!({
            "metadata": {"name": name, "namespace": crate::cluster::namespace()},
            "spec": spec
        })
    }

    #[test]
    fn owner_of_reads_a_group_a_user_or_neither() {
        let group = owner_of(&json!({"owner": {"group": "sales"}}));
        assert_eq!(group.group.as_deref(), Some("sales"));
        assert!(group.user.is_none());

        let user = owner_of(&json!({"owner": {"user": "alice"}}));
        assert_eq!(user.user.as_deref(), Some("alice"));
        assert!(user.group.is_none());

        let neither = owner_of(&json!({}));
        assert!(neither.group.is_none());
        assert!(neither.user.is_none());
    }

    #[test]
    fn may_use_admits_the_admin_the_group_the_owner_and_everyone_for_shared() {
        let group = owner_of(&json!({"owner": {"group": "sales"}}));
        let person = owner_of(&json!({"owner": {"user": "alice"}}));
        let shared = owner_of(&json!({"owner": {"group": "stardelt-users"}}));

        assert!(may_use(&who("root", &[], true), "trial", &group, &[]));
        assert!(may_use(
            &who("bob", &["sales"], false),
            "trial",
            &group,
            &[]
        ));
        assert!(may_use(&who("alice", &[], false), "trial", &person, &[]));
        assert!(!may_use(
            &who("bob", &["marketing"], false),
            "trial",
            &group,
            &[]
        ));
        assert!(!may_use(&who("bob", &[], false), "trial", &person, &[]));
        assert!(may_use(&who("bob", &[], false), "shared", &shared, &[]));
    }

    #[test]
    fn a_viewer_may_use_a_cluster_but_not_act() {
        let spec = json!({"owner": {"group": "analytics"}, "viewers": ["visitors"]});
        let owner = owner_of(&spec);
        let viewers = viewers_of(&spec);

        assert!(may_use(
            &who("demo", &["visitors"], false),
            "team",
            &owner,
            &viewers
        ));
        assert!(!may_act(&who("demo", &["visitors"], false), "team", &owner));
        assert!(!may_use(
            &who("other", &["other"], false),
            "team",
            &owner,
            &viewers
        ));
    }

    #[test]
    fn a_viewer_sees_the_cluster() {
        let spec = json!({"owner": {"group": "analytics"}, "viewers": ["visitors"]});
        let objects = [cluster("team", spec)];
        let listed = |user: &str, groups: &[&str]| {
            visible_clusters(&objects, &who(user, groups, false))
                .iter()
                .map(|view| view.name.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(listed("demo", &["visitors"]), ["team"]);
        assert!(listed("other", &["other"]).is_empty());
        let view = &visible_clusters(&objects, &who("demo", &["visitors"], false))[0];
        assert_eq!(view.viewers, vec!["visitors".to_string()]);
    }

    #[test]
    fn may_act_admits_the_same_set_but_never_a_non_admin_on_shared() {
        let group = owner_of(&json!({"owner": {"group": "sales"}}));
        let person = owner_of(&json!({"owner": {"user": "alice"}}));
        let shared = owner_of(&json!({"owner": {"group": "stardelt-users"}}));

        assert!(may_act(&who("root", &[], true), "trial", &group));
        assert!(may_act(&who("bob", &["sales"], false), "trial", &group));
        assert!(may_act(&who("alice", &[], false), "trial", &person));
        assert!(!may_act(
            &who("bob", &["marketing"], false),
            "trial",
            &group
        ));
        assert!(!may_act(&who("bob", &[], false), "trial", &person));
        assert!(!may_act(
            &who("bob", &["stardelt-users"], false),
            "shared",
            &shared
        ));
        assert!(may_act(&who("root", &[], true), "shared", &shared));
    }

    #[test]
    fn phase_of_prefers_the_operator_and_falls_back_to_the_request() {
        let reported = cluster("trial", json!({"state": "Running"}));
        let mut reported = reported;
        reported["status"] = json!({"phase": "Running"});
        assert_eq!(phase_of(&reported), "Running");

        let requested = cluster("trial", json!({"state": "Running"}));
        assert_eq!(phase_of(&requested), "Starting");

        let idle = cluster("trial", json!({}));
        assert_eq!(phase_of(&idle), "Stopped");
    }

    #[test]
    fn message_of_reads_the_operator_reason_and_treats_empty_as_none() {
        let mut failed = cluster("trial", json!({"state": "Running"}));
        failed["status"] = json!({
            "phase": "Failed",
            "message": "unknown runtime spark-9.9-py3.13; known: spark-4.1-py3.13"
        });
        assert_eq!(
            message_of(&failed).as_deref(),
            Some("unknown runtime spark-9.9-py3.13; known: spark-4.1-py3.13")
        );

        // An empty message and a missing one are the same answer: nothing to show.
        let mut empty = cluster("trial", json!({}));
        empty["status"] = json!({"phase": "Starting", "message": ""});
        assert!(message_of(&empty).is_none());

        let missing = cluster("trial", json!({}));
        assert!(message_of(&missing).is_none());
    }

    #[test]
    fn the_view_carries_the_failure_reason() {
        let mut failed = cluster("failed-trial", json!({"owner": {"group": "sales"}}));
        failed["status"] = json!({
            "phase": "Failed",
            "message": "unknown runtime spark-9.9-py3.13; known: spark-4.1-py3.13"
        });
        let mut running = cluster("running-trial", json!({"owner": {"group": "sales"}}));
        running["status"] = json!({"phase": "Running"});
        let views = visible_clusters(&[failed, running], &who("bob", &["sales"], false));
        let names: Vec<&str> = views.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(names, ["failed-trial", "running-trial"]);
        assert_eq!(views[0].phase, "Failed");
        assert_eq!(
            views[0].message.as_deref(),
            Some("unknown runtime spark-9.9-py3.13; known: spark-4.1-py3.13")
        );
        // A cluster the operator reports as Running carries no reason.
        assert_eq!(views[1].phase, "Running");
        assert!(views[1].message.is_none());
    }

    #[test]
    fn visible_clusters_filters_and_sorts() {
        let objects = vec![
            cluster(
                "zzz-trial",
                json!({"owner": {"group": "sales"}, "runtime": "spark-4.1-py3.13"}),
            ),
            cluster(
                "shared",
                json!({"owner": {"group": "stardelt-users"}, "idleStopMinutes": 0}),
            ),
            cluster("aaa-trial", json!({"owner": {"group": "sales"}})),
            cluster("marketing", json!({"owner": {"group": "marketing"}})),
        ];
        let views = visible_clusters(&objects, &who("bob", &["sales"], false));
        let names: Vec<&str> = views.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(names, ["aaa-trial", "shared", "zzz-trial"]);
        assert_eq!(views[0].size, "small");
        assert_eq!(views[0].idle_stop_minutes, 60);
        assert!(views[0].can_use);
        assert!(!views[0].shared);
        assert!(views[1].shared);
        assert_eq!(views[1].idle_stop_minutes, 0);
        // A spec without the editable blocks answers them empty, so the Edit form can prefill.
        assert!(views[0].driver.is_none());
        assert!(views[0].executor.is_none());
        assert!(views[0].max_executors.is_none());
        assert!(views[0].image.is_none());
        assert!(views[0].env.is_empty());
        assert!(views[0].spark_conf.is_empty());
    }

    #[test]
    fn the_view_carries_the_editable_fields() {
        let mut object = cluster(
            "trial",
            json!({
                "owner": {"group": "sales"},
                "runtime": "spark-4.1-py3.13",
                "driver": {"cores": 2, "memory": "4Gi"},
                "executor": {"cores": 4},
                "maxExecutors": 5,
                "image": "registry.example/spark@sha256:abc",
                "env": [{"name": "A", "value": "1"}, {"name": "B", "connectionRef": {"name": "c", "key": "k"}}],
                "sparkConf": {"spark.sql.shuffle.partitions": "8"}
            }),
        );
        object["status"] = json!({"phase": "Running"});
        let views = visible_clusters(&[object], &who("root", &[], true));
        let view = &views[0];
        let driver = view.driver.as_ref().expect("driver");
        assert_eq!(driver.cores, Some(2));
        assert_eq!(driver.memory.as_deref(), Some("4Gi"));
        assert_eq!(view.executor.as_ref().and_then(|e| e.cores), Some(4));
        assert_eq!(view.max_executors, Some(5));
        assert_eq!(
            view.image.as_deref(),
            Some("registry.example/spark@sha256:abc")
        );
        // A `connectionRef` entry cannot be edited in Nova and is not shown.
        assert_eq!(view.env.len(), 1);
        assert_eq!(view.env[0].name, "A");
        assert_eq!(view.env[0].value, "1");
        assert_eq!(
            view.spark_conf.get("spark.sql.shuffle.partitions"),
            Some(&"8".to_string())
        );
    }

    /// A valid body, for the `validate_spec` tests that change one field.
    fn body() -> super::ComputeSpecBody {
        serde_json::from_value(json!({
            "owner": {"group": "sales"},
            "runtime": "spark-4.1-py3.13"
        }))
        .expect("a valid body")
    }

    #[test]
    fn viewers_are_validated() {
        let mut b = body();
        b.viewers = Some((0..17).map(|i| format!("group{i}")).collect());
        assert_eq!(
            validate_spec("demo", &b),
            Err("viewers holds at most 16 groups".to_string())
        );
        let mut b = body();
        b.viewers = Some(vec!["not a group".to_string()]);
        assert_eq!(
            validate_spec("demo", &b),
            Err("viewers holds `not a group`, which is not a group name".to_string())
        );
        let mut b = body();
        b.viewers = Some(vec!["visitors".to_string()]);
        assert_eq!(validate_spec("demo", &b), Ok(()));
    }

    #[test]
    fn a_replace_writes_keeps_or_clears_the_viewers() {
        let stored = json!({ "spec": { "viewers": ["visitors"] } });
        let mut spec = json!({ "owner": { "group": "analytics" } });
        write_viewers(&mut spec, None, Some(&stored));
        assert_eq!(
            spec.get("viewers"),
            Some(&json!(["visitors"])),
            "absent keeps"
        );
        write_viewers(&mut spec, Some(&["analytics".to_string()]), Some(&stored));
        assert_eq!(
            spec.get("viewers"),
            Some(&json!(["analytics"])),
            "a list writes"
        );
        write_viewers(&mut spec, Some(&[]), Some(&stored));
        assert!(spec.get("viewers").is_none(), "an empty list clears");
    }

    #[test]
    fn a_valid_small_group_cluster_passes() {
        assert_eq!(validate_spec("trial", &body()), Ok(()));
    }

    #[test]
    fn exactly_one_owner_is_required() {
        let mut two = body();
        two.owner.user = Some("alice".to_string());
        assert_eq!(
            validate_spec("trial", &two),
            Err("set exactly one of owner.group and owner.user".to_string())
        );

        let mut neither = body();
        neither.owner.group = None;
        assert_eq!(
            validate_spec("trial", &neither),
            Err("set exactly one of owner.group and owner.user".to_string())
        );
    }

    #[test]
    fn only_the_shared_cluster_may_run_without_an_idle_stop() {
        let mut trial = body();
        trial.idle_stop_minutes = 0;
        assert_eq!(
            validate_spec("trial", &trial),
            Err("only the shared cluster may run without an idle stop".to_string())
        );
        assert_eq!(validate_spec("shared", &trial), Ok(()));
    }

    #[test]
    fn an_image_must_be_pinned_by_digest() {
        let mut unpinned = body();
        unpinned.image = Some("registry.example/spark:4.1".to_string());
        assert_eq!(
            validate_spec("trial", &unpinned),
            Err("pin the image by digest".to_string())
        );
    }

    #[test]
    fn a_platform_managed_spark_conf_key_is_refused() {
        let mut managed = body();
        managed.spark_conf.insert(
            "spark.kubernetes.namespace".to_string(),
            "other".to_string(),
        );
        assert_eq!(
            validate_spec("trial", &managed),
            Err("sparkConf key spark.kubernetes.namespace is managed by the platform".to_string())
        );
    }

    #[test]
    fn max_executors_stop_at_fifty() {
        let mut too_many = body();
        too_many.max_executors = Some(51);
        assert_eq!(
            validate_spec("trial", &too_many),
            Err("maxExecutors must be between 0 and 50".to_string())
        );
    }

    #[test]
    fn a_group_name_with_a_space_is_refused() {
        let mut bad = body();
        bad.owner.group = Some("a b".to_string());
        assert_eq!(
            validate_spec("trial", &bad),
            Err("owner.group must be a valid group name".to_string())
        );
    }

    #[test]
    fn a_group_that_would_make_an_invalid_driver_service_account_is_refused() {
        for group in ["Visitors", "team_a", "team."] {
            let mut bad = body();
            bad.owner.group = Some(group.to_string());
            let err = validate_spec("trial", &bad).unwrap_err();
            assert!(
                err.contains("not a valid Kubernetes name"),
                "{group}: {err}"
            );
        }
        let mut ok = body();
        ok.owner.group = Some("data-team".to_string());
        assert_eq!(validate_spec("trial", &ok), Ok(()));
    }

    #[test]
    fn a_duplicate_env_name_is_refused() {
        let mut duplicate = body();
        duplicate.env = vec![
            super::ComputeEnvVar {
                name: "A".to_string(),
                value: "1".to_string(),
            },
            super::ComputeEnvVar {
                name: "A".to_string(),
                value: "2".to_string(),
            },
        ];
        assert_eq!(
            validate_spec("trial", &duplicate),
            Err("each env var name may appear once in env: `A` appears twice".to_string())
        );
    }

    /// The demo sandbox's Deployment and `spark-demo-config` ConfigMap, as
    /// `platform/manifests/spark-demo-sandbox.yaml` writes them (values measured 2026-11-19).
    fn demo_server() -> (Value, Value) {
        let deployment = json!({
            "metadata": {
                "name": "spark-demo",
                "namespace": "stardelt-demo-spark",
                "labels": {"app.kubernetes.io/name": "spark-demo", "stardelt.io/compute": "demo"}
            },
            "spec": {"template": {"spec": {"containers": [{
                "name": "spark-demo",
                "image": "ghcr.io/stardelt/spark:dev@sha256:70f9c9f2b40b1e32b4877c31c9af6905989d0944581cd950dd056c07008448d0",
                "resources": {
                    "requests": {"cpu": "250m", "memory": "1Gi"},
                    "limits": {"cpu": "2", "memory": "4Gi"}
                }
            }]}}},
            "status": {"readyReplicas": 1}
        });
        let configmap = json!({
            "metadata": {"name": "spark-demo-config", "namespace": "stardelt-demo-spark"},
            "data": {"spark-defaults.conf": concat!(
                "# Local mode: no executors, no pod creates, no Kubernetes client.\n",
                "spark.master                        local[2]\n",
                "spark.driver.memory                 2g\n",
                "spark.driver.maxResultSize          256m\n",
                "spark.sql.shuffle.partitions        8\n",
                "spark.ui.enabled                    false\n",
                "spark.connect.grpc.binding.port     15002\n",
            )}
        });
        (deployment, configmap)
    }

    /// The shared `spark-connect` Deployment and `spark-connect-config` ConfigMap, as
    /// `platform/manifests/spark.yaml` writes them.
    fn shared_server() -> (Value, Value) {
        let deployment = json!({
            "metadata": {
                "name": "spark-connect",
                "namespace": crate::cluster::namespace(),
                "labels": {"app.kubernetes.io/name": "spark-connect", "stardelt.io/compute": "shared"}
            },
            "spec": {"template": {"spec": {"containers": [{
                "name": "spark-connect",
                "image": "ghcr.io/stardelt/spark:dev@sha256:70f9c9f2b40b1e32b4877c31c9af6905989d0944581cd950dd056c07008448d0",
                "resources": {"limits": {"cpu": "1", "memory": "3Gi"}}
            }]}}},
            "status": {"readyReplicas": 2}
        });
        let configmap = json!({
            "metadata": {"name": "spark-connect-config", "namespace": crate::cluster::namespace()},
            "data": {"spark-defaults.conf": concat!(
                "# --- Sizing on the shared 16 GB node ---\n",
                "spark.master                                            k8s://https://kubernetes.default.svc\n",
                "spark.driver.memory                                     1g\n",
                "spark.driver.maxResultSize                              512m\n",
                "spark.executor.memory                                   1g\n",
                "spark.executor.memoryOverhead                           384m\n",
                "spark.executor.cores                                    1\n",
                "spark.dynamicAllocation.minExecutors                    0\n",
                "spark.dynamicAllocation.maxExecutors                    2\n",
                "spark.dynamicAllocation.executorIdleTimeout             60s\n",
                "spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY stardelt-s3-creds:secret-key\n",
            )}
        });
        (deployment, configmap)
    }

    #[test]
    fn platform_server_specs_are_read_from_the_deployment_and_configmap() {
        let (deployment, configmap) = demo_server();
        // Production reads a `Deployment` the API server answered, not raw JSON: the round trip
        // through the k8s types must keep `Quantity` a string (`"2"`, `"4Gi"`).
        let typed: k8s_openapi::api::apps::v1::Deployment =
            serde_json::from_value(deployment).expect("a Deployment");
        let deployment = serde_json::to_value(&typed).expect("JSON");
        let view = platform_view(
            &deployment,
            Some(&configmap),
            &who("demo", &["visitors"], false),
        )
        .expect("a platform view");

        assert_eq!(view.name, "spark-demo");
        assert_eq!(view.kind, "platform");
        assert_eq!(view.mode.as_deref(), Some("local"));
        assert_eq!(view.audience.as_deref(), Some("visitors"));
        assert_eq!(view.phase, "Running");
        assert_eq!(view.runtime, "dev");
        // The measured values of the sandbox, from `spark-defaults.conf` ...
        assert_eq!(
            view.spark_conf.get("spark.master").map(String::as_str),
            Some("local[2]")
        );
        assert_eq!(
            view.driver
                .as_ref()
                .and_then(|driver| driver.memory.as_deref()),
            Some("2g")
        );
        assert_eq!(
            view.spark_conf
                .get("spark.driver.maxResultSize")
                .map(String::as_str),
            Some("256m")
        );
        // ... and from the container's limits.
        let limits = view.limits.as_ref().expect("limits");
        assert_eq!(limits.cores, Some(2));
        assert_eq!(limits.memory.as_deref(), Some("4Gi"));
        // Local mode has no executors: the page says "local mode, no executors".
        assert!(view.executor.is_none());
        assert_eq!(view.resets_at.as_deref(), Some("daily 03:00 UTC"));
        assert!(!view.shared);
    }

    #[test]
    fn a_platform_response_carries_no_secret() {
        let (mut deployment, configmap) = shared_server();
        deployment["spec"]["template"]["spec"]["containers"][0]["env"] = json!([
            {"name": "AWS_SECRET_ACCESS_KEY",
             "valueFrom": {"secretKeyRef": {"name": "stardelt-s3-creds", "key": "secret-key"}}}
        ]);
        let view = platform_view(&deployment, Some(&configmap), &who("bob", &[], false))
            .expect("a platform view");

        // The Deployment's `env` is never read: the page shows no environment.
        assert!(view.env.is_empty());
        // The ConfigMap's Secret reference and its pod-template path are outside the whitelist.
        let mut keys: Vec<&str> = view.spark_conf.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "spark.driver.maxResultSize",
                "spark.driver.memory",
                "spark.dynamicAllocation.executorIdleTimeout",
                "spark.dynamicAllocation.maxExecutors",
                "spark.dynamicAllocation.minExecutors",
                "spark.executor.cores",
                "spark.executor.memory",
                "spark.master",
            ]
        );
        let text: String = view.spark_conf.values().cloned().collect();
        assert!(!text.contains("secret-key"));
        assert!(!text.contains("stardelt-s3-creds"));
    }

    #[test]
    fn a_visitor_sees_only_the_sandbox() {
        let visitor = who("demo", &["visitors"], false);
        let (demo, demo_config) = demo_server();
        let (shared, shared_config) = shared_server();
        let clusters = vec![
            platform_view(&shared, Some(&shared_config), &visitor).expect("shared"),
            platform_view(&demo, Some(&demo_config), &visitor).expect("demo"),
            // A `SparkCluster` a visitor's group owns is not theirs either: their `spark` is the
            // sandbox.
            visible_clusters(
                &[cluster(
                    "visit-trial",
                    json!({"owner": {"group": "visitors"}}),
                )],
                &visitor,
            )[0]
            .clone(),
        ];
        let views = visible_to(clusters, &visitor);
        let names: Vec<&str> = views.iter().map(|view| view.name.as_str()).collect();
        assert_eq!(names, ["spark-demo"]);
        assert!(views[0].yours);
        assert!(views.iter().all(|view| !view.shared));
    }

    #[test]
    fn a_member_sees_the_shared_server() {
        let member = who("bob", &["stardelt-users"], false);
        let (demo, demo_config) = demo_server();
        let (shared, shared_config) = shared_server();
        let clusters = vec![
            platform_view(&demo, Some(&demo_config), &member).expect("demo"),
            platform_view(&shared, Some(&shared_config), &member).expect("shared"),
        ];
        let views = visible_to(clusters, &member);
        let names: Vec<&str> = views.iter().map(|view| view.name.as_str()).collect();
        assert_eq!(names, ["spark-demo", "spark-connect"]);
        let shared = views
            .iter()
            .find(|view| view.name == "spark-connect")
            .expect("the shared server");
        assert!(shared.yours);
        assert!(shared.shared);
        assert_eq!(shared.audience.as_deref(), Some("members"));
        assert_eq!(shared.mode.as_deref(), Some("kubernetes"));
        assert!(shared.resets_at.is_none());
        let demo = views
            .iter()
            .find(|view| view.name == "spark-demo")
            .expect("the sandbox");
        // A member's `spark` is the shared server; the sandbox is the visitors'.
        assert!(!demo.yours);
    }

    #[test]
    fn spark_defaults_keeps_only_the_whitelist() {
        let conf = concat!(
            "# a comment is not a setting\n",
            "\n",
            "spark.master          local[2]\n",
            "spark.ui.enabled      false\n",
            "spark.executor.cores = 1\n",
            "spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY stardelt-s3-creds:secret-key\n",
            "spark.driver.memory   2g\n",
        );
        let defaults = spark_defaults(conf);
        let keys: Vec<&str> = defaults.keys().map(String::as_str).collect();
        // BTreeMap order; `spark.ui.enabled` and the Secret reference are gone.
        assert_eq!(
            keys,
            [
                "spark.driver.memory",
                "spark.executor.cores",
                "spark.master"
            ]
        );
        assert_eq!(
            defaults.get("spark.driver.memory").map(String::as_str),
            Some("2g")
        );
        // Both separators are read: the manifests align with spaces, `=` works too.
        assert_eq!(
            defaults.get("spark.executor.cores").map(String::as_str),
            Some("1")
        );
        assert_eq!(
            defaults.get("spark.master").map(String::as_str),
            Some("local[2]")
        );
    }

    mod fake_kube {
        use std::sync::{Arc, Mutex};

        use axum::{
            Json, Router,
            extract::{Path, State},
            http::StatusCode,
            routing::get,
        };
        use serde_json::{Value, json};

        pub struct Fake {
            pub items: Vec<Value>,
            pub patches: Vec<Value>,
            pub created: Vec<Value>,
            pub replaced: Vec<Value>,
            pub deleted: Vec<String>,
        }

        pub type Shared = Arc<Mutex<Fake>>;

        fn failure(code: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
            (
                code,
                Json(json!({
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Failure",
                    "message": message,
                    "code": code.as_u16()
                })),
            )
        }

        async fn many(State(state): State<Shared>) -> Json<Value> {
            let state = state.lock().unwrap();
            Json(json!({
                "apiVersion": "stardelt.io/v1alpha1",
                "kind": "SparkClusterList",
                "items": state.items,
            }))
        }

        async fn one(
            State(state): State<Shared>,
            Path((_namespace, name)): Path<(String, String)>,
        ) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
            let state = state.lock().unwrap();
            state
                .items
                .iter()
                .find(|item| item["metadata"]["name"] == name)
                .cloned()
                .map(Json)
                .ok_or_else(|| failure(StatusCode::NOT_FOUND, "not found"))
        }

        async fn patch_one(
            State(state): State<Shared>,
            Path((_namespace, name)): Path<(String, String)>,
            body: String,
        ) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
            let body: Value = serde_json::from_str(&body)
                .map_err(|_| failure(StatusCode::BAD_REQUEST, "not json"))?;
            let mut state = state.lock().unwrap();
            state.patches.push(body.clone());
            let item = state
                .items
                .iter_mut()
                .find(|item| item["metadata"]["name"] == name)
                .ok_or_else(|| failure(StatusCode::NOT_FOUND, "not found"))?;
            if let Some(desired) = body.pointer("/spec/state").and_then(Value::as_str) {
                item["spec"]["state"] = json!(desired);
            }
            Ok(Json(item.clone()))
        }

        async fn create_one(
            State(state): State<Shared>,
            Path(_namespace): Path<String>,
            body: String,
        ) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
            let object: Value = serde_json::from_str(&body)
                .map_err(|_| failure(StatusCode::BAD_REQUEST, "not json"))?;
            let mut state = state.lock().unwrap();
            if let Some(name) = object.pointer("/metadata/name").and_then(Value::as_str)
                && state
                    .items
                    .iter()
                    .any(|item| item["metadata"]["name"] == name)
            {
                return Err(failure(StatusCode::CONFLICT, "already exists"));
            }
            state.created.push(object.clone());
            state.items.push(object.clone());
            Ok((StatusCode::CREATED, Json(object)))
        }

        async fn put_one(
            State(state): State<Shared>,
            Path((_namespace, name)): Path<(String, String)>,
            body: String,
        ) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
            let object: Value = serde_json::from_str(&body)
                .map_err(|_| failure(StatusCode::BAD_REQUEST, "not json"))?;
            let mut state = state.lock().unwrap();
            let item = state
                .items
                .iter_mut()
                .find(|item| item["metadata"]["name"] == name)
                .ok_or_else(|| failure(StatusCode::NOT_FOUND, "not found"))?;
            *item = object.clone();
            state.replaced.push(object.clone());
            Ok(Json(object))
        }

        async fn delete_one(
            State(state): State<Shared>,
            Path((_namespace, name)): Path<(String, String)>,
        ) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
            let mut state = state.lock().unwrap();
            let index = state
                .items
                .iter()
                .position(|item| item["metadata"]["name"] == name)
                .ok_or_else(|| failure(StatusCode::NOT_FOUND, "not found"))?;
            let removed = state.items.remove(index);
            state.deleted.push(name);
            Ok(Json(removed))
        }

        pub async fn start(items: Vec<Value>) -> (kube::Client, Shared) {
            let shared = Arc::new(Mutex::new(Fake {
                items,
                patches: Vec::new(),
                created: Vec::new(),
                replaced: Vec::new(),
                deleted: Vec::new(),
            }));
            let app = Router::new()
                .route(
                    "/apis/stardelt.io/v1alpha1/namespaces/{namespace}/sparkclusters",
                    get(many).post(create_one),
                )
                .route(
                    "/apis/stardelt.io/v1alpha1/namespaces/{namespace}/sparkclusters/{name}",
                    get(one).patch(patch_one).put(put_one).delete(delete_one),
                )
                .with_state(shared.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let url = format!("http://{addr}").parse().unwrap();
            let client = kube::Client::try_from(kube::Config::new(url)).expect("client");
            (client, shared)
        }
    }

    /// The compute routes on a test state with a fake API server, `who` signed in.
    async fn app_with(
        items: Vec<Value>,
        who: Option<Identity>,
    ) -> (axum::Router, fake_kube::Shared) {
        use axum::routing::post;

        let (client, shared) = fake_kube::start(items).await;
        let state = crate::router_tests::test_state_with_kube(client);
        let router = axum::Router::new()
            .route("/compute", axum::routing::get(super::list))
            .route("/compute/{name}/start", post(super::start))
            .route("/compute/{name}/stop", post(super::stop))
            .route(
                "/admin/compute/{name}",
                axum::routing::put(super::upsert).delete(super::remove),
            )
            .with_state(state);
        let router = match who {
            Some(identity) => router.layer(axum::Extension(crate::auth::Principal {
                identity,
                via: "session",
                access_token: None,
                client_id: None,
            })),
            None => router,
        };
        (router, shared)
    }

    async fn call(app: axum::Router, method: &str, uri: &str) -> (StatusCode, Value) {
        use tower::ServiceExt;

        let request = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        (status, value)
    }

    /// The same, with a JSON body: the administrator writes carry one.
    async fn call_json(
        app: axum::Router,
        method: &str,
        uri: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        use tower::ServiceExt;

        let request = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(body.to_string()))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        (status, value)
    }

    fn trial() -> Value {
        cluster(
            "trial",
            json!({"owner": {"group": "sales"}, "runtime": "spark-4.1-py3.13", "state": "Stopped"}),
        )
    }

    #[tokio::test]
    async fn the_list_answers_the_one_visible_cluster() {
        let items = vec![
            trial(),
            cluster(
                "marketing",
                json!({"owner": {"group": "marketing"}, "runtime": "spark-4.1-py3.13"}),
            ),
        ];
        let (app, _) = app_with(items, Some(who("bob", &["sales"], false))).await;
        let (status, body) = call(app, "GET", "/compute").await;
        assert_eq!(status, StatusCode::OK);
        let clusters = body["clusters"].as_array().expect("clusters");
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0]["name"], "trial");
        assert_eq!(clusters[0]["owner"]["group"], "sales");
    }

    #[tokio::test]
    async fn start_patches_the_state_and_answers_starting() {
        let (app, shared) = app_with(vec![trial()], Some(who("bob", &["sales"], false))).await;
        let (status, body) = call(app, "POST", "/compute/trial/start").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["phase"], "Starting");
        let patches = shared.lock().unwrap().patches.clone();
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0], json!({"spec": {"state": "Running"}}));
    }

    #[tokio::test]
    async fn a_stranger_gets_404() {
        let (app, shared) = app_with(vec![trial()], Some(who("eve", &["marketing"], false))).await;
        let (status, body) = call(app, "POST", "/compute/trial/start").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "not found");
        assert!(shared.lock().unwrap().patches.is_empty());
    }

    #[tokio::test]
    async fn a_non_admin_on_the_shared_cluster_gets_403() {
        let items = vec![cluster(
            "shared",
            json!({"owner": {"group": "stardelt-users"}, "idleStopMinutes": 0, "state": "Running"}),
        )];
        let (app, shared) = app_with(items, Some(who("bob", &["stardelt-users"], false))).await;
        let (status, body) = call(app, "POST", "/compute/shared/start").await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(
            body["error"],
            "only an administrator may start or stop the shared cluster"
        );
        assert!(shared.lock().unwrap().patches.is_empty());
    }

    #[tokio::test]
    async fn a_cluster_that_is_starting_gets_409() {
        let mut object = cluster(
            "trial",
            json!({"owner": {"group": "sales"}, "runtime": "spark-4.1-py3.13", "state": "Running"}),
        );
        object["status"] = json!({"phase": "Starting"});
        let (app, shared) = app_with(vec![object], Some(who("bob", &["sales"], false))).await;
        let (status, body) = call(app, "POST", "/compute/trial/start").await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "the cluster is starting");
        assert!(shared.lock().unwrap().patches.is_empty());
    }

    #[tokio::test]
    async fn an_admin_creates_a_cluster() {
        let (app, shared) = app_with(vec![], Some(who("root", &[], true))).await;
        let (status, body) = call_json(
            app,
            "PUT",
            "/admin/compute/trial",
            json!({"owner": {"group": "sales"}, "runtime": "spark-4.1-py3.13"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["name"], "trial");
        assert_eq!(body["phase"], "Stopped");
        let created = shared.lock().unwrap().created.clone();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0]["metadata"]["name"], "trial");
        assert_eq!(created[0]["spec"]["owner"]["group"], "sales");
        assert_eq!(created[0]["spec"]["state"], "Stopped");
        // The CRD accepts `[]` and `{}`: both always travel.
        assert_eq!(created[0]["spec"]["env"], json!([]));
        assert_eq!(created[0]["spec"]["sparkConf"], json!({}));
        // `image` is absent, never `null` (the CRD refuses `null`).
        assert!(created[0]["spec"].get("image").is_none());
    }

    #[tokio::test]
    async fn an_admin_replaces_and_keeps_the_state() {
        let (app, shared) = app_with(
            vec![cluster(
                "trial",
                json!({
                    "owner": {"group": "sales"},
                    "runtime": "spark-4.1-py3.13",
                    "size": "small",
                    "state": "Running"
                }),
            )],
            Some(who("root", &[], true)),
        )
        .await;
        let (status, body) = call_json(
            app,
            "PUT",
            "/admin/compute/trial",
            json!({"owner": {"group": "sales"}, "runtime": "spark-4.1-py3.13", "size": "large"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["size"], "large");
        let fake = shared.lock().unwrap();
        assert!(fake.created.is_empty(), "a replace never creates");
        assert_eq!(fake.replaced.len(), 1);
        assert_eq!(fake.replaced[0]["spec"]["size"], "large");
        assert_eq!(fake.replaced[0]["spec"]["state"], "Running");
    }

    #[tokio::test]
    async fn the_shared_cluster_cannot_be_deleted() {
        let (app, shared) = app_with(
            vec![cluster(
                "shared",
                json!({"owner": {"group": "stardelt-users"}, "idleStopMinutes": 0}),
            )],
            Some(who("root", &[], true)),
        )
        .await;
        let (status, body) = call(app, "DELETE", "/admin/compute/shared").await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            body["error"],
            "the shared cluster is part of the platform; stop it instead of deleting it"
        );
        assert!(shared.lock().unwrap().deleted.is_empty());
    }

    #[tokio::test]
    async fn deleting_a_missing_cluster_answers_404() {
        let (app, shared) = app_with(vec![], Some(who("root", &[], true))).await;
        let (status, body) = call(app, "DELETE", "/admin/compute/trial").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "not found");
        assert!(shared.lock().unwrap().deleted.is_empty());
    }

    #[tokio::test]
    async fn deleting_a_cluster_answers_204() {
        let (app, shared) = app_with(vec![trial()], Some(who("root", &[], true))).await;
        let (status, _) = call(app, "DELETE", "/admin/compute/trial").await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(shared.lock().unwrap().deleted, ["trial"]);
    }

    #[tokio::test]
    async fn an_invalid_body_answers_400() {
        let (app, shared) = app_with(vec![], Some(who("root", &[], true))).await;
        let (status, body) = call_json(
            app,
            "PUT",
            "/admin/compute/trial",
            json!({
                "owner": {"group": "sales"},
                "runtime": "spark-4.1-py3.13",
                "idleStopMinutes": 0
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"],
            "only the shared cluster may run without an idle stop"
        );
        assert!(shared.lock().unwrap().created.is_empty());
    }

    #[test]
    fn a_viewer_sees_no_endpoint_and_may_not_use_the_cluster() {
        let mut object = cluster(
            "team",
            json!({"owner": {"group": "analytics"}, "viewers": ["visitors"]}),
        );
        object["status"] = json!({"endpoint": "sc://team:15002"});

        let viewer = view(&object, &who("demo", &["visitors"], false))
            .expect("a viewer still sees the cluster");
        assert!(viewer.endpoint.is_none());
        assert!(!viewer.can_use);

        let owner = view(&object, &who("bob", &["analytics"], false)).expect("the owner sees it");
        assert_eq!(owner.endpoint.as_deref(), Some("sc://team:15002"));
        assert!(owner.can_use);

        let mut shared = cluster("shared", json!({"owner": {"group": "stardelt-users"}}));
        shared["status"] = json!({"endpoint": "sc://shared:15002"});
        let anyone =
            view(&shared, &who("carol", &[], false)).expect("everyone sees the shared one");
        assert_eq!(anyone.endpoint.as_deref(), Some("sc://shared:15002"));
        assert!(anyone.can_use);
    }

    #[test]
    fn a_viewer_sees_can_act_false() {
        let object = cluster(
            "team",
            json!({"owner": {"group": "analytics"}, "viewers": ["visitors"]}),
        );
        let viewer = view(&object, &who("demo", &["visitors"], false))
            .expect("a viewer still sees the cluster");
        assert!(!viewer.can_act);
        assert!(!viewer.can_use);

        let owner = view(&object, &who("bob", &["analytics"], false)).expect("the owner sees it");
        assert!(owner.can_act);

        let shared = cluster("shared", json!({"owner": {"group": "stardelt-users"}}));
        let member =
            view(&shared, &who("carol", &[], false)).expect("everyone sees the shared one");
        assert!(!member.can_act);
        assert!(member.can_use);
    }

    #[tokio::test]
    async fn a_viewer_is_refused_with_the_owner_groups_rule() {
        let items = vec![cluster(
            "team",
            json!({"owner": {"group": "analytics"}, "viewers": ["visitors"]}),
        )];
        let (app, shared) = app_with(items, Some(who("demo", &["visitors"], false))).await;
        let (status, body) = call(app, "POST", "/compute/team/start").await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(
            body["error"],
            "this cluster belongs to another group: only its owner group starts and stops it, ask them to"
        );
        assert!(shared.lock().unwrap().patches.is_empty());
    }

    #[tokio::test]
    async fn the_shared_cluster_refuses_a_member_with_the_admin_rule() {
        let items = vec![cluster(
            "shared",
            json!({"owner": {"group": "stardelt-users"}, "idleStopMinutes": 0, "state": "Running"}),
        )];
        let (app, shared) = app_with(items, Some(who("bob", &[], false))).await;
        let (status, body) = call(app, "POST", "/compute/shared/stop").await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(
            body["error"],
            "only an administrator may start or stop the shared cluster"
        );
        assert!(shared.lock().unwrap().patches.is_empty());
    }
}
