//! The Pipelines API: read (list, detail, runs, tasks, logs — stardelt-nova#188), act (run now,
//! pause, resume, stop a run — stardelt-nova#196), schedule (create, change, delete a
//! pipeline — stardelt-nova#210) and the schedule options (parameters, retries, time limit,
//! concurrency, a statement check, a next-runs preview — stardelt-nova#223); design notes
//! `design/scheduled-jobs.md` §6.1 and `design/scheduling-and-notebooks.md` §2, phase J1.
//!
//! Nova is the enforcement point. Airflow's one credential reads every DAG, so every route here
//! first finds the `Pipeline` resource, checks that the caller may see it, and only then asks
//! Airflow. A pipeline the caller may not see answers **404**, exactly like one that does not
//! exist — a refusal that distinguishes "hidden" from "absent" would leak the hidden one.
//!
//! - **Visibility** ([`visible`]): administrators see everything; everybody else sees a pipeline
//!   whose `spec.ownerGroup` is one of their groups, or whose `spec.viewers` lists one of them.
//!   A pipeline without an `ownerGroup` is administrators-only. Acting ([`may_act`]) stays with
//!   the owner group and administrators: a `viewers` entry is read-only — list, detail, runs and
//!   spec — and everybody else gets the same 404.
//! - **Airflow's naming**: `Pipeline <namespace>/<name>` has the DAG id `<namespace>__<name>`
//!   ([`airflow::dag_id_for`]). Every Airflow path is built from that id, never from a browser
//!   value, so a run id of another pipeline cannot resolve.
//! - **A new pipeline is loading**: the loader creates the DAG about 25 seconds after the
//!   resource appears. Until then the view says `loaded: false`, `state: "loading"`; after three
//!   minutes without a DAG it says `"not-loaded"` instead (the loader refused this resource).
//! - **Airflow's refusals are Nova's sentences**, not Airflow's silent behaviour:
//!   triggering a paused pipeline would queue a run that never starts (409 here), `Forbid` is not
//!   enforced at trigger time (Nova checks the latest runs itself), a `logical_date` in the past
//!   or future produces a run without tasks (Nova always sends now), and stopping a run marks it
//!   failed — it is not a cancel: in-flight work can finish and commit (see #255).
//! - **Logs**: the `::group::` source-details block (the marker with `sources`, the internal
//!   object-store path Airflow writes as the next event, and `::endgroup::`) is dropped, and no
//!   field of an answer can hold a `sources` value or an internal path. Answers are capped at 2000
//!   lines and 512 KiB and say `truncated: true` beyond that.
//! - **Unreachable upstreams are 502, never 500** (rule U12 of `design/ui-conformance.md`):
//!   without a cluster client or an Airflow client the routes answer 503 and say which one is
//!   missing; an Airflow that refuses Nova's service user answers 502 `the scheduler refused
//!   Nova's service user`, and one that is unreachable answers 502 `the scheduler did not
//!   answer`.
//! - **Audit**: every action writes one `audit` line, including a refusal that never reaches
//!   Airflow; no field can hold a note or log text.
//! - **Writes** (#210): *Schedule* creates a `Pipeline` for one of the caller's **own** groups
//!   (administrators included: they schedule for the groups they belong to) with
//!   `runAs: pipeline-<group>` — the design note's naming convention. Nova validates everything
//!   the loader (`stardelt-demos` `dags/stardelt_pipelines/plan.py`) would refuse, so a pipeline
//!   Nova writes always becomes a DAG; the principal of the group must exist and be ready.
//!   Only pipelines labelled `app.kubernetes.io/managed-by: stardelt-nova` may be changed or
//!   deleted — a `kubectl` or GitOps pipeline answers 409 instead of being rewritten. The
//!   statement is **never** audited, only its length and the cron text. A create needs no
//!   Airflow (the resource is the source of truth); the read routes keep answering 503 without it.
//!   In demo mode (`NOVA_DEMO=1`) the three read routes answer sample data from `demo.rs`; the
//!   write routes and the detail routes still answer 503.
//!
//! - **Options and parameters** (#223): a schedule carries `${params.<name>}` values (at most 16,
//!   lower-case names, values up to 256 characters) and the advanced options the loader reads
//!   (`retries`, `timeout`, `concurrency`). An omitted option keeps today's default, so an old body
//!   writes what it always wrote. Every `${…}` in the statement must be one the loader knows
//!   (`run.date`, `run.id`, `params.<name>`) — an unknown one would fail the run, so it is a 400 at
//!   create time. `POST /api/pipelines/check` asks the engine about a statement with `EXPLAIN`
//!   (never `EXPLAIN ANALYZE`, never the statement itself), and `POST /api/pipelines/preview-schedule`
//!   lists the next fire times in the schedule's zone.
//!
//! - **Notebooks** (#237): the same two routes schedule a published notebook version instead of a
//!   statement. A body gives *either* a non-empty `sql` or a `notebook`
//!   (`{ name, objectVersion, params }`) — both or neither is a 400. The resource's one task is
//!   then `{"name": "run", "kind": "notebook", "config": {notebook, objectVersion, params}}`, where
//!   `params` is the task's JSON object string with sorted keys; everything else (runAs, retries,
//!   timeout, concurrency, schedule) is the SQL shape's. Nova refuses what the CRD or the loader
//!   would refuse: the `.ipynb` name rule, at most 32 parameters named `[A-Za-z_][A-Za-z0-9_]{0,63}`,
//!   values up to 256 characters without NUL, a JSON text of at most 4096 characters — and a version
//!   that is **not in the store**: `notebooks::head_version` asks S3 (503 without `NOVA_S3_*`, 502
//!   when it does not answer), so a schedule always pins bytes that exist. A version is never a
//!   branch or a tag: the loader reads `<ownerGroup>/<notebook>` at exactly that version, so a group
//!   the caller does not belong to can never be named. An update changes the notebook it names, its
//!   version and its parameters, never what the pipeline runs: `sql` on a notebook pipeline is 400
//!   `this pipeline runs a notebook`, a notebook on a statement pipeline 400 `this pipeline runs a
//!   statement`.
//!
//! The route logic sits in plain `async fn`s that take their inputs, so the tests call them
//! without a router; each handler is the thin axum wrapper that checks sign-in and the two
//! clients. The whole surface is uncacheable (`Cache-Control: no-store`).

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

#[cfg(test)]
use std::sync::Mutex;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{FromRequestParts, Path, Query, Request},
    http::{HeaderValue, StatusCode, header, request::Parts},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Datelike, Days, LocalResult, TimeZone, Utc};
use chrono_tz::Tz;
use futures::stream::{self, StreamExt};
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::AppState;
use crate::airflow::{
    AirflowClient, AirflowError, DagRun, DagSummary, LogEvent, LogPage, TaskInstance, dag_id_for,
};
use crate::api_json::ApiJson;
use crate::audit::{self, PipelineWrite};
use crate::auth::{Identity, Principal};
use crate::connection_routes::SignedIn;
use crate::deployments::{BundlePlan, PlanChange, PlanFile, PlanIssue};
use crate::error::Fail;
use crate::notebooks::{self, NotebookStore};

/// The `Pipeline` resources of the platform namespace: the cluster, or memory in tests.
pub enum PipelineResources {
    Kube(Box<ClusterResources>),
    #[cfg(test)]
    Memory(Arc<Mutex<MemoryStore>>),
}

/// The two resource types of Nova's namespace. Boxed, so the enum stays the size of a pointer
/// beside the test double.
pub struct ClusterResources {
    pipelines: Api<DynamicObject>,
    principals: Api<DynamicObject>,
}

/// What the test double holds: the `Pipeline` resources and the `ServicePrincipal` resources of
/// the namespace, both by name.
#[cfg(test)]
#[derive(Default)]
pub struct MemoryStore {
    pub pipelines: BTreeMap<String, Value>,
    pub principals: BTreeMap<String, Value>,
}

#[derive(Debug)]
pub enum ResourceError {
    /// The name is taken: the answer is 409, never an overwrite.
    Exists,
    /// The resource changed (or vanished) since it was read: 409, never a silent overwrite.
    Conflict,
    /// The cluster did not answer or refused.
    Unavailable(String),
}

impl PipelineResources {
    /// The pipeline resources of Nova's namespace, through the cluster client.
    pub fn cluster(client: kube::Client) -> Self {
        let pipelines = Api::namespaced_with(
            client.clone(),
            &crate::cluster::namespace(),
            &ApiResource::from_gvk(&GroupVersionKind::gvk(
                "stardelt.io",
                "v1alpha1",
                "Pipeline",
            )),
        );
        // The principal of a group, `pipeline-<group>` by convention, is another resource of the
        // same namespace: Nova reads it, never writes it (an administrator creates it).
        let principals = Api::namespaced_with(
            client,
            &crate::cluster::namespace(),
            &ApiResource::from_gvk(&GroupVersionKind::gvk(
                "stardelt.io",
                "v1alpha1",
                "ServicePrincipal",
            )),
        );
        Self::Kube(Box::new(ClusterResources {
            pipelines,
            principals,
        }))
    }

    /// Every `Pipeline` of the namespace, unfiltered; the caller applies [`visible`].
    pub async fn list(&self) -> Result<Vec<Value>, ResourceError> {
        match self {
            Self::Kube(resources) => {
                let objects = resources
                    .pipelines
                    .list(&kube::api::ListParams::default())
                    .await
                    .map_err(|e| ResourceError::Unavailable(e.to_string()))?;
                objects
                    .items
                    .into_iter()
                    .map(|o| {
                        serde_json::to_value(o)
                            .map_err(|e| ResourceError::Unavailable(e.to_string()))
                    })
                    .collect()
            }
            #[cfg(test)]
            Self::Memory(items) => Ok(items
                .lock()
                .expect("memory pipeline resources poisoned")
                .pipelines
                .values()
                .cloned()
                .collect()),
        }
    }

    /// One `Pipeline` by name; `None` when it does not exist.
    pub async fn get(&self, name: &str) -> Result<Option<Value>, ResourceError> {
        match self {
            Self::Kube(resources) => resources
                .pipelines
                .get_opt(name)
                .await
                .map_err(|e| ResourceError::Unavailable(e.to_string()))
                .map(|object| {
                    object.map(|o| {
                        serde_json::to_value(o)
                            .map_err(|e| ResourceError::Unavailable(e.to_string()))
                    })
                })?
                .transpose(),
            #[cfg(test)]
            Self::Memory(items) => Ok(items
                .lock()
                .expect("memory pipeline resources poisoned")
                .pipelines
                .get(name)
                .cloned()),
        }
    }

    /// A real create, not an apply: a name that already exists is [`ResourceError::Exists`].
    /// Answers the resource as the cluster stored it.
    pub async fn create(&self, resource: Value) -> Result<Value, ResourceError> {
        match self {
            Self::Kube(resources) => {
                let object: DynamicObject = serde_json::from_value(resource)
                    .map_err(|e| ResourceError::Unavailable(format!("not a resource: {e}")))?;
                let created = resources
                    .pipelines
                    .create(&kube::api::PostParams::default(), &object)
                    .await
                    .map_err(|e| match e {
                        kube::Error::Api(er) if er.code == 409 => ResourceError::Exists,
                        other => ResourceError::Unavailable(other.to_string()),
                    })?;
                serde_json::to_value(created).map_err(|e| ResourceError::Unavailable(e.to_string()))
            }
            #[cfg(test)]
            Self::Memory(store) => {
                let mut store = store.lock().expect("memory pipeline resources poisoned");
                let name = pipeline_name(&resource).to_string();
                if store.pipelines.contains_key(&name) {
                    return Err(ResourceError::Exists);
                }
                store.pipelines.insert(name, resource.clone());
                Ok(resource)
            }
        }
    }

    /// Replace one, carrying the `resourceVersion` it was read with: a concurrent change is
    /// [`ResourceError::Conflict`], never a silent overwrite. Answers the stored resource.
    pub async fn replace(&self, name: &str, resource: Value) -> Result<Value, ResourceError> {
        match self {
            Self::Kube(resources) => {
                let object: DynamicObject = serde_json::from_value(resource)
                    .map_err(|e| ResourceError::Unavailable(format!("not a resource: {e}")))?;
                let replaced = resources
                    .pipelines
                    .replace(name, &kube::api::PostParams::default(), &object)
                    .await
                    .map_err(|e| match e {
                        // A conflict is the API server's `409`; a `404` means somebody deleted it
                        // between the read and the write — both ask the caller to reload.
                        kube::Error::Api(er) if er.code == 409 || er.code == 404 => {
                            ResourceError::Conflict
                        }
                        other => ResourceError::Unavailable(other.to_string()),
                    })?;
                serde_json::to_value(replaced)
                    .map_err(|e| ResourceError::Unavailable(e.to_string()))
            }
            #[cfg(test)]
            Self::Memory(store) => {
                let mut store = store.lock().expect("memory pipeline resources poisoned");
                let Some(current) = store.pipelines.get(name) else {
                    return Err(ResourceError::Conflict);
                };
                let read_with = current
                    .get("metadata")
                    .and_then(|metadata| metadata.get("resourceVersion"));
                let written_with = resource
                    .get("metadata")
                    .and_then(|metadata| metadata.get("resourceVersion"));
                if read_with != written_with {
                    return Err(ResourceError::Conflict);
                }
                store.pipelines.insert(name.to_string(), resource.clone());
                Ok(resource)
            }
        }
    }

    /// Delete one; a resource that is already gone is success (a `404` is not an error).
    pub async fn delete(&self, name: &str) -> Result<(), ResourceError> {
        match self {
            Self::Kube(resources) => {
                match resources
                    .pipelines
                    .delete(name, &kube::api::DeleteParams::default())
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(kube::Error::Api(er)) if er.code == 404 => Ok(()),
                    Err(e) => Err(ResourceError::Unavailable(e.to_string())),
                }
            }
            #[cfg(test)]
            Self::Memory(store) => {
                store
                    .lock()
                    .expect("memory pipeline resources poisoned")
                    .pipelines
                    .remove(name);
                Ok(())
            }
        }
    }

    /// Every `ServicePrincipal` of the namespace, unfiltered; the caller picks a group's one.
    pub async fn principals(&self) -> Result<Vec<Value>, ResourceError> {
        match self {
            Self::Kube(resources) => {
                let objects = resources
                    .principals
                    .list(&kube::api::ListParams::default())
                    .await
                    .map_err(|e| ResourceError::Unavailable(e.to_string()))?;
                objects
                    .items
                    .into_iter()
                    .map(|o| {
                        serde_json::to_value(o)
                            .map_err(|e| ResourceError::Unavailable(e.to_string()))
                    })
                    .collect()
            }
            #[cfg(test)]
            Self::Memory(store) => Ok(store
                .lock()
                .expect("memory pipeline resources poisoned")
                .principals
                .values()
                .cloned()
                .collect()),
        }
    }
}

/// The resource's `metadata.name`, or the empty string for a malformed object.
fn pipeline_name(resource: &Value) -> &str {
    resource
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(Value::as_str)
        .unwrap_or_default()
}

/// The resource's `spec`; the resource itself when the field is missing (every read then sees
/// no owner group, no tasks and no schedule, which is the safe answer).
fn spec_of(resource: &Value) -> &Value {
    resource.get("spec").unwrap_or(resource)
}

/// `spec.notify` as the shape the document and the detail page carry; `None` when the key is
/// absent or does not parse (the operator's lists may be left out, `serde(default)` fills them).
fn notify_of(spec: &Value) -> Option<PipelineNotify> {
    serde_json::from_value(spec.get("notify")?.clone()).ok()
}

/// `spec.viewers` as the list the document and the bundle carry: empty when absent.
pub(crate) fn viewers_of(spec: &Value) -> Vec<String> {
    spec.get("viewers")
        .and_then(Value::as_array)
        .map(|viewers| {
            viewers
                .iter()
                .filter_map(|group| group.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Pure rules
// ---------------------------------------------------------------------------

/// Who may see a pipeline. Administrators see all; everybody else sees a pipeline whose
/// `spec.ownerGroup` names one of their groups, or whose `spec.viewers` lists one of them —
/// a viewer reads (list, detail, runs, spec) and runs it (epic#895 decision 12).
pub fn visible(identity: &Identity, spec: &Value) -> bool {
    if identity.is_admin {
        return true;
    }
    let mine = |group: &str| identity.in_group(group);
    if spec
        .get("ownerGroup")
        .and_then(Value::as_str)
        .is_some_and(mine)
    {
        return true;
    }
    spec.get("viewers")
        .and_then(Value::as_array)
        .is_some_and(|viewers| viewers.iter().filter_map(Value::as_str).any(mine))
}

/// Who may change a pipeline: pause it, resume it, change its spec or delete it. The owner group
/// and administrators only — `spec.viewers` may not (epic#895 decision 12). Running is
/// deliberately not here: a viewer runs the owner's statement, which cannot change what the
/// pipeline does; that verb is [`visible`]. Written out rather than as a call to [`visible`],
/// because the two rules differ on purpose; a caller who may see the pipeline but not change it
/// gets 403.
pub fn may_act(identity: &Identity, spec: &Value) -> bool {
    if identity.is_admin {
        return true;
    }
    spec.get("ownerGroup")
        .and_then(Value::as_str)
        .is_some_and(|group| identity.in_group(group))
}

/// How long a resource may exist without a DAG before the view calls it `not-loaded`. The loader
/// creates a DAG about 25 seconds after the resource appears; a resource the loader refuses
/// never gets one.
const NOT_LOADED_AFTER_SECONDS: f64 = 3.0 * 60.0;

/// The single pipeline state the UI shows.
///
/// Not loaded: `loading` for the first three minutes after the resource was created (the loader
/// needs about 25 seconds), then `not-loaded` — the loader will not create this DAG, and saying
/// "loading" forever would be a lie. `created_age_seconds` is `None` when the resource carries
/// no `metadata.creationTimestamp`; then the view stays `loading`.
///
/// `paused` wins over everything else (a paused pipeline is not "failed" even if its last run
/// failed). A last run that is `running` or `queued` makes the pipeline `running`;
/// `failed`/`upstream_failed` is `failed`; `success` is `succeeded`; anything else (never ran,
/// `skipped`, …) is `idle`.
pub fn pipeline_state(
    paused: bool,
    loaded: bool,
    last_run_state: Option<&str>,
    created_age_seconds: Option<f64>,
) -> &'static str {
    if !loaded {
        return match created_age_seconds {
            Some(age) if age > NOT_LOADED_AFTER_SECONDS => "not-loaded",
            _ => "loading",
        };
    }
    if paused {
        return "paused";
    }
    match last_run_state {
        Some("running") | Some("queued") => "running",
        Some("failed") | Some("upstream_failed") => "failed",
        Some("success") => "succeeded",
        _ => "idle",
    }
}

/// `"0 3 * * * (Europe/Berlin)"`, or just the cron when the schedule has no time zone. A
/// schedule written as a bare string is accepted too (the `IngestionSource` shape).
fn schedule_of(spec: &Value) -> Option<String> {
    match spec.get("schedule")? {
        Value::String(cron) if !cron.trim().is_empty() => Some(cron.trim().to_string()),
        Value::Object(schedule) => {
            let cron = schedule.get("cron")?.as_str()?.trim();
            if cron.is_empty() {
                return None;
            }
            match schedule
                .get("timezone")
                .and_then(Value::as_str)
                .filter(|tz| !tz.is_empty())
            {
                Some(timezone) => Some(format!("{cron} ({timezone})")),
                None => Some(cron.to_string()),
            }
        }
        _ => None,
    }
}

/// The selectors of a `kind: "dbt"` task's `config` as one line: `config.select`, or the
/// `config.batches` array parsed back and joined with `, `. A `batches` text that is not a JSON
/// array of strings leaves `None` — the view never fails on a hand-written resource (`#112`).
fn dbt_select_of(config: Option<&Value>) -> Option<String> {
    let config = config?;
    if let Some(select) = config.get("select").and_then(Value::as_str) {
        return Some(select.to_string());
    }
    let batches = config.get("batches").and_then(Value::as_str)?;
    let batches: Vec<String> = serde_json::from_str(batches).ok()?;
    Some(batches.join(", "))
}

/// The task names and kinds the resource declares, in declaration order. `detailed` adds what
/// only the edit dialog needs — a `sql` task's statement, a notebook task's file name, version
/// and parameters — so the list stays small.
fn task_specs(spec: &Value, detailed: bool) -> Vec<TaskSpecView> {
    spec.get("tasks")
        .and_then(Value::as_array)
        .map(|tasks| {
            tasks
                .iter()
                .filter_map(|task| {
                    let kind = task.get("kind").and_then(Value::as_str).unwrap_or_default();
                    let config = task.get("config");
                    let notebook = detailed && kind == "notebook";
                    let dbt = detailed && kind == "dbt";
                    let spark = detailed && kind == "spark-pipeline";
                    Some(TaskSpecView {
                        name: task.get("name")?.as_str()?.to_string(),
                        kind: kind.to_string(),
                        sql: if detailed && kind == "sql" {
                            task.get("sql").and_then(Value::as_str).map(str::to_string)
                        } else {
                            None
                        },
                        notebook: if notebook {
                            config
                                .and_then(|config| config.get("notebook"))
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        } else {
                            None
                        },
                        object_version: if notebook {
                            config
                                .and_then(|config| config.get("objectVersion"))
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        } else {
                            None
                        },
                        notebook_params: if notebook {
                            config
                                .and_then(|config| config.get("params"))
                                .and_then(Value::as_str)
                                .and_then(|params| serde_json::from_str(params).ok())
                        } else {
                            None
                        },
                        connection: if notebook {
                            task.get("connection")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        } else {
                            None
                        },
                        dbt_project: if dbt {
                            config
                                .and_then(|config| config.get("project"))
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        } else {
                            None
                        },
                        dbt_select: if dbt { dbt_select_of(config) } else { None },
                        project: if spark {
                            task.get("project")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        } else {
                            None
                        },
                        refresh: if spark {
                            task.get("refresh")
                                .and_then(Value::as_str)
                                .map(str::to_string)
                        } else {
                            None
                        },
                        full_refresh: if spark {
                            task.get("fullRefresh")
                                .and_then(Value::as_array)
                                .map(|names| {
                                    names
                                        .iter()
                                        .filter_map(Value::as_str)
                                        .map(str::to_string)
                                        .collect()
                                })
                        } else {
                            None
                        },
                        writes: if detailed {
                            task.get("writes")
                                .and_then(Value::as_array)
                                .map(|writes| {
                                    writes
                                        .iter()
                                        .filter_map(Value::as_str)
                                        .map(str::to_string)
                                        .collect()
                                })
                                .unwrap_or_default()
                        } else {
                            Vec::new()
                        },
                        depends_on: task
                            .get("dependsOn")
                            .and_then(Value::as_array)
                            .map(|names| {
                                names
                                    .iter()
                                    .filter_map(Value::as_str)
                                    .map(str::to_string)
                                    .collect()
                            })
                            .unwrap_or_default(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// `${params.<name>}` values as the resource carries them.
fn params_of(spec: &Value) -> BTreeMap<String, String> {
    spec.get("params")
        .and_then(Value::as_object)
        .map(|params| {
            params
                .iter()
                .filter_map(|(key, value)| value.as_str().map(|v| (key.clone(), v.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// The retry policy as the resource carries it.
fn retries_of(spec: &Value) -> Option<RetriesView> {
    let retries = spec.get("retries")?;
    Some(RetriesView {
        attempts: retries.get("attempts")?.as_i64()?,
        backoff: retries
            .get("backoff")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

/// A duration the CRD accepts (`30s`, `5m`, `2h`, `1d`, the grammar of the loader's `plan.py`)
/// as whole minutes. `None` for anything else, so a hand-written resource never breaks the view.
fn minutes_of(text: &str) -> Option<i64> {
    let text = text.trim();
    let (number, seconds) = ["s", "m", "h", "d"].iter().find_map(|unit| {
        let number = text.strip_suffix(unit)?;
        let seconds = match *unit {
            "s" => 1,
            "m" => 60,
            "h" => 3600,
            _ => 86_400,
        };
        Some((number, seconds))
    })?;
    Some(number.parse::<i64>().ok()? * seconds / 60)
}

/// The options as the resource carries them, defaults when a field is absent (or written in a
/// form Nova does not read): the edit dialog prefills from this.
fn options_of(spec: &Value) -> PipelineOptionsView {
    let retries = spec.get("retries");
    let backoff = retries
        .and_then(|retries| retries.get("backoff"))
        .and_then(Value::as_str)
        .and_then(minutes_of)
        .unwrap_or(DEFAULT_BACKOFF_MINUTES);
    PipelineOptionsView {
        retries: retries
            .and_then(|retries| retries.get("attempts"))
            .and_then(Value::as_i64)
            .unwrap_or(DEFAULT_RETRIES),
        backoff_minutes: backoff,
        timeout_minutes: spec
            .get("timeout")
            .and_then(Value::as_str)
            .and_then(minutes_of)
            .unwrap_or(DEFAULT_TIMEOUT_MINUTES),
        concurrency: spec
            .get("concurrency")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_CONCURRENCY)
            .to_string(),
    }
}

// ---------------------------------------------------------------------------
// Time: Airflow's RFC 3339 strings and a run's duration (no date dependency)
// ---------------------------------------------------------------------------

/// Days since 1970-01-01 for a civil date (Howard Hinnant's algorithm).
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = (year - era * 400) as u64;
    let month_index = if month > 2 { month - 3 } else { month + 9 } as u64;
    let doy = (153 * month_index + 2) / 5 + u64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

/// Seconds since the Unix epoch for an RFC 3339 timestamp, fraction included. Answers `None`
/// for anything Airflow would not send (a missing date, an unknown zone, trailing text).
fn parse_rfc3339(text: &str) -> Option<f64> {
    let bytes = text.as_bytes();
    if bytes.len() < 20 {
        return None;
    }
    let number = |range: std::ops::Range<usize>| text.get(range)?.parse::<i64>().ok();
    let year = number(0..4)?;
    if bytes.get(4) != Some(&b'-') {
        return None;
    }
    let month = number(5..7)? as u32;
    if bytes.get(7) != Some(&b'-') {
        return None;
    }
    let day = number(8..10)? as u32;
    if !matches!(bytes.get(10), Some(b'T') | Some(b't') | Some(b' ')) {
        return None;
    }
    let hour = number(11..13)?;
    if bytes.get(13) != Some(&b':') {
        return None;
    }
    let minute = number(14..16)?;
    if bytes.get(16) != Some(&b':') {
        return None;
    }
    let second = number(17..19)?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=60).contains(&second)
    {
        return None;
    }

    let mut at = 19;
    let mut fraction = 0.0;
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        let start = at;
        while at < bytes.len() && bytes[at].is_ascii_digit() {
            at += 1;
        }
        if at == start {
            return None;
        }
        let digits: String = text[start..at].chars().take(9).collect();
        fraction = format!("0.{digits}").parse::<f64>().ok()?;
    }

    let offset = match bytes.get(at) {
        Some(b'Z') | Some(b'z') => {
            at += 1;
            0
        }
        Some(sign @ (b'+' | b'-')) => {
            let sign = if *sign == b'+' { 1 } else { -1 };
            let hours = number(at + 1..at + 3)?;
            if bytes.get(at + 3) != Some(&b':') {
                return None;
            }
            let minutes = number(at + 4..at + 6)?;
            at += 6;
            sign * (hours * 3_600 + minutes * 60)
        }
        _ => return None,
    };
    if at != bytes.len() {
        return None;
    }

    let seconds =
        days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second - offset;
    Some(seconds as f64 + fraction)
}

/// The seconds between two Airflow timestamps, rounded to microseconds; `None` unless both
/// parse and the end is not before the start. Airflow's ISO strings carry microseconds, and a
/// float difference would otherwise leak binary noise into the JSON.
pub(crate) fn duration_seconds(started: Option<&str>, ended: Option<&str>) -> Option<f64> {
    let ended = parse_rfc3339(ended?)?;
    let started = parse_rfc3339(started?)?;
    let duration = ended - started;
    (duration >= 0.0).then(|| (duration * 1_000_000.0).round() / 1_000_000.0)
}

/// The civil date for a number of days since 1970-01-01 (the inverse of [`days_from_civil`]).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = (days - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_index + 2) / 5 + 1) as u32;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    } as u32;
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day)
}

/// `YYYY-MM-DDTHH:MM:SSZ` for whole seconds since the epoch, always UTC and always with `Z`.
/// Airflow 3 wants a `logical_date`; Nova always sends the current second (`now_rfc3339`).
fn format_rfc3339(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (of_day / 3_600, (of_day % 3_600) / 60, of_day % 60);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Whole seconds since the epoch, as a float for the age comparison.
fn now_seconds() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs_f64())
        .unwrap_or_default()
}

/// The clock as a UTC date-time, for the statement check's "today" and the preview's "next".
/// Read through `std` on purpose: Nova never asks the machine for its local zone.
fn now_utc() -> DateTime<Utc> {
    DateTime::<Utc>::from(std::time::SystemTime::now())
}

/// Now, whole seconds, UTC, for Airflow's `logical_date`.
fn now_rfc3339() -> String {
    format_rfc3339(now_seconds() as i64)
}

/// Now, UTC, with microseconds, for Airflow's `logical_date`: Airflow's `dag_run` is unique
/// on `(dag_id, logical_date)`, so two starts inside one whole second must not share a date
/// (stardelt/stardelt#1537). Whole seconds stay the rule everywhere else.
fn now_rfc3339_micros() -> String {
    now_utc().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

/// The resource's `metadata.creationTimestamp`, as the cluster wrote it.
fn created_at_of(resource: &Value) -> Option<&str> {
    resource
        .get("metadata")
        .and_then(|metadata| metadata.get("creationTimestamp"))
        .and_then(Value::as_str)
}

/// How long ago the resource was created; `None` without a readable `creationTimestamp`.
fn created_age_seconds(resource: &Value) -> Option<f64> {
    let created = parse_rfc3339(created_at_of(resource)?)?;
    Some((now_seconds() - created).max(0.0))
}

// ---------------------------------------------------------------------------
// Writes: what *Schedule* accepts, and the resource it writes
// ---------------------------------------------------------------------------

/// The label that says Nova owns a pipeline. Anything else (kubectl, GitOps) is not ours to
/// rewrite: the answer is 409, never a silent edit of somebody's manifest.
pub const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
pub const MANAGED_BY_NOVA: &str = "stardelt-nova";

/// The longest statement a scheduled pipeline may carry.
const MAX_STATEMENT_CHARS: usize = 20_000;

/// At most this many pipelines created per user per hour (`AppState::pipeline_creates`).
const CREATES_PER_HOUR: usize = 10;
const CREATE_WINDOW: std::time::Duration = std::time::Duration::from_secs(60 * 60);

fn invalid_name() -> &'static str {
    "name must be 1 to 40 lower-case letters, digits and dashes, starting with a letter"
}

fn invalid_sql() -> &'static str {
    "give the statement to run (up to 20,000 characters)"
}

/// The name rules of the CRD and of `ServicePrincipalSpec::validate`: 1 to 40 lower-case
/// letters, digits and dashes, starting with a letter and not ending with a dash.
pub(crate) fn validate_name(name: &str) -> Result<(), String> {
    let bytes = name.as_bytes();
    let ok = !bytes.is_empty()
        && bytes.len() <= 40
        && bytes[0].is_ascii_lowercase()
        && bytes[bytes.len() - 1] != b'-'
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-');
    if ok {
        Ok(())
    } else {
        Err(invalid_name().into())
    }
}

/// `sql`, trimmed: not empty, at most 20,000 characters, no NUL; one trailing `;` is dropped so
/// the loader does not have to guess. Answers the statement as it is stored.
fn validate_sql(sql: &str) -> Result<String, String> {
    let trimmed = sql.trim();
    let statement = trimmed
        .strip_suffix(';')
        .map(str::trim_end)
        .unwrap_or(trimmed);
    if statement.is_empty()
        || statement.chars().count() > MAX_STATEMENT_CHARS
        || statement.contains('\0')
    {
        return Err(invalid_sql().into());
    }
    Ok(statement.to_string())
}

// ---------------------------------------------------------------------------
// Parameters and advanced options (`Schedule`, #223)
// ---------------------------------------------------------------------------

/// At most this many `${params.<name>}` values per schedule (#223).
const MAX_PARAMS: usize = 16;
/// The longest parameter value the resource may carry.
const MAX_PARAM_VALUE_CHARS: usize = 256;

const PARAM_NAMES: &str = "parameter names are lower-case letters, digits and underscores";
const TOO_MANY_PARAMS: &str = "at most 16 parameters";
const PARAM_VALUE_TOO_LONG: &str = "a parameter value is at most 256 characters";
const PARAM_VALUE_NUL: &str = "a parameter value cannot contain NUL";
const RETRIES_RANGE: &str = "retries are 0 to 5";
const BACKOFF_RANGE: &str = "the wait between retries is 1 to 60 minutes";
const TIMEOUT_RANGE: &str = "the time limit is 1 to 720 minutes";
const CONCURRENCY_RULE: &str = "runs are Forbid or Allow";
const COUNT_RANGE: &str = "count is 1 to 10";

/// Today's defaults for a schedule that sends no `options`: one retry after five minutes, a
/// two-hour time limit, one run at a time (what `pipeline_resource` wrote before #223).
const DEFAULT_RETRIES: i64 = 1;
const DEFAULT_BACKOFF_MINUTES: i64 = 5;
const DEFAULT_TIMEOUT_MINUTES: i64 = 120;
const DEFAULT_CONCURRENCY: &str = "Forbid";

/// `^[a-z][a-z0-9_]{0,31}$`: the names the loader expands as `${params.<name>}`.
fn valid_param_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 32
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
}

/// The parameter table a schedule may carry: at most 16 values, lower-case names, short values
/// without NUL. `Err` is the sentence the caller sees.
fn validate_params(params: &BTreeMap<String, String>) -> Result<(), String> {
    if params.len() > MAX_PARAMS {
        return Err(TOO_MANY_PARAMS.into());
    }
    for (name, value) in params {
        if !valid_param_name(name) {
            return Err(PARAM_NAMES.into());
        }
        if value.contains('\0') {
            return Err(PARAM_VALUE_NUL.into());
        }
        if value.chars().count() > MAX_PARAM_VALUE_CHARS {
            return Err(PARAM_VALUE_TOO_LONG.into());
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The notebook task (`Schedule`, #237)
// ---------------------------------------------------------------------------

/// The most parameters one notebook task may pass to papermill — the CRD's
/// `maxProperties` for `config.params`.
const MAX_NOTEBOOK_PARAMS: usize = 32;
/// The longest one notebook parameter value may be, in characters.
const MAX_NOTEBOOK_PARAM_VALUE_CHARS: usize = 256;
/// The longest the `params` JSON object string may be — the CRD's CEL limit.
const MAX_NOTEBOOK_PARAMS_JSON_CHARS: usize = 4096;

const ONE_OF_STATEMENT_NOTEBOOK_OR_DBT: &str =
    "give a statement, a notebook, a dbt run, a quality run or a spark-pipeline graph";
const NOTEBOOK_NAME_RULE: &str = "the notebook name is a .ipynb file name (letters, digits, \
                                  dots, dashes, underscores and spaces)";
const NOTEBOOK_PARAM_NAMES: &str = "parameter names are letters, digits and underscores";
const NOTEBOOK_TOO_MANY_PARAMS: &str = "at most 32 parameters";
const NOTEBOOK_PARAM_VALUE: &str = "a parameter value is at most 256 characters";
const NOTEBOOK_PARAMS_TOO_LONG: &str = "the parameters are too long";
const CONNECTION_RULE: &str = "a connection name is 1 to 40 lower-case letters, digits and \
                               dashes, starting and ending with a letter or digit";
/// An update cannot change what a pipeline runs (#237): the kind is the resource's.
const NOTEBOOK_PIPELINE: &str = "this pipeline runs a notebook";
const STATEMENT_PIPELINE: &str = "this pipeline runs a statement";

/// `^[A-Za-z_][A-Za-z0-9_]{0,63}$`: the parameter names papermill binds. Notebook names are
/// valid under the publish route's rule, so a schedule can only name a notebook a publish
/// accepts (`notebooks::valid_name`, the CRD's own regular expression).
fn valid_notebook_param_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
}

/// The notebook a schedule pins, checked the way the CRD and the loader check it. `Err` is the
/// sentence the caller sees; `Ok` is the `config.params` JSON text, keys sorted by
/// [`BTreeMap`], which is what the task carries.
fn validate_notebook(notebook: &NotebookRequest) -> Result<String, String> {
    if !notebooks::valid_name(&notebook.name) {
        return Err(NOTEBOOK_NAME_RULE.into());
    }
    if notebook.params.len() > MAX_NOTEBOOK_PARAMS {
        return Err(NOTEBOOK_TOO_MANY_PARAMS.into());
    }
    for (name, value) in &notebook.params {
        if !valid_notebook_param_name(name) {
            return Err(NOTEBOOK_PARAM_NAMES.into());
        }
        // The one sentence covers both: a value that would not fit papermill's call.
        if value.chars().count() > MAX_NOTEBOOK_PARAM_VALUE_CHARS || value.contains('\0') {
            return Err(NOTEBOOK_PARAM_VALUE.into());
        }
    }
    let params = serde_json::to_string(&notebook.params)
        .map_err(|_| NOTEBOOK_PARAMS_TOO_LONG.to_string())?;
    if params.chars().count() > MAX_NOTEBOOK_PARAMS_JSON_CHARS {
        return Err(NOTEBOOK_PARAMS_TOO_LONG.into());
    }
    if let Some(connection) = &notebook.connection
        && !connection_ok(connection)
    {
        return Err(CONNECTION_RULE.into());
    }
    Ok(params)
}

/// The CRD's rule for `tasks[].connection` (`crd.rs`: `^[a-z]([a-z0-9-]*[a-z0-9])?$`, ≤ 40).
fn connection_ok(name: &str) -> bool {
    (1..=40).contains(&name.len())
        && name.bytes().next().is_some_and(|b| b.is_ascii_lowercase())
        && name
            .bytes()
            .last()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// The `dbt` task (`#112`): the sentences of `demos/dags/stardelt_pipelines/plan.py`, verbatim,
/// so Nova refuses exactly what the loader refuses.
const DBT_NEEDS_PROJECT: &str =
    "a dbt task needs config.project (a directory under /opt/dbt/projects in the tool image)";
const DBT_NEEDS_SELECT: &str = "a dbt task needs exactly one of config.select (one dbt selector) \
                                and config.batches (a JSON array of 1 to 8 selectors, run in order)";
const DBT_BAD_THREADS: &str = "dbt threads must be a digit string from 1 to 16 (default `4`)";
const DBT_BAD_VARS: &str = "dbt vars must be a JSON object string of string values";
/// At most this many selectors in one `batches` chain (the loader's `MAX_DBT_BATCHES`).
const MAX_DBT_BATCHES: usize = 8;
/// At most this many dbt threads (the loader's `MAX_DBT_THREADS`).
const MAX_DBT_THREADS: u8 = 16;
/// The longest the `vars` JSON object string may be — the CRD's CEL limit for a `config` value.
const MAX_DBT_VARS_JSON_CHARS: usize = 4096;

/// The `quality` task (`#114`): the sentences of `demos/dags/stardelt_pipelines/plan.py`'s
/// `_quality_checks`, verbatim, so Nova refuses exactly what the loader refuses.
const MAX_QUALITY_CHECKS: usize = 50;
const MAX_CHECK_SQL_CHARS: usize = 20000;
const QUALITY_NEEDS_CHECKS: &str = "a quality task needs checks (a list of 1 to 50 SQL assertions)";
const QUALITY_CHECK_NEEDS_NAME: &str = "a check needs a name (1 to 63 letters, digits, dashes or \
                                        underscores, starting with a letter or a digit)";
const QUALITY_CHECK_NAME_TWICE: &str = "the check name is used twice";
const QUALITY_CHECK_NEEDS_SQL: &str =
    "a check's sql must be a non-empty string of at most 20000 characters";
const QUALITY_CHECK_BAD_SEVERITY: &str = "a check's severity must be error or warn";
const QUALITY_CHECK_AUDIT_TABLE: &str =
    "`${table}` needs `publish: wap` on the pipeline: it is the audit branch of a wap run";

/// A project is one directory name under the tool image's `/opt/dbt/projects`: the loader's
/// `^[a-z][a-z0-9-]{0,38}$`.
fn valid_dbt_project(project: &str) -> bool {
    let bytes = project.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 39
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// The `dbt` task's `config` map as the resource stores it, checked the way `plan.py` checks it:
/// the project, exactly one of `select` and `batches`, the threads as a digit string, and the
/// `--vars` text. `Err` is the sentence the caller sees.
fn dbt_config(request: &DbtRequest) -> Result<BTreeMap<String, String>, String> {
    if !valid_dbt_project(&request.project) {
        return Err(DBT_NEEDS_PROJECT.into());
    }
    let mut config = BTreeMap::new();
    config.insert("project".to_string(), request.project.clone());
    match (request.select.as_deref(), request.batches.as_deref()) {
        (Some(select), None) => {
            if select.trim().is_empty() {
                return Err(DBT_NEEDS_SELECT.into());
            }
            config.insert("select".to_string(), select.to_string());
        }
        (None, Some(batches)) => {
            if batches.is_empty()
                || batches.len() > MAX_DBT_BATCHES
                || batches.iter().any(|select| select.trim().is_empty())
            {
                return Err(DBT_NEEDS_SELECT.into());
            }
            let batches =
                serde_json::to_string(batches).map_err(|_| DBT_NEEDS_SELECT.to_string())?;
            config.insert("batches".to_string(), batches);
        }
        _ => return Err(DBT_NEEDS_SELECT.into()),
    }
    let threads = request.threads.unwrap_or(4);
    if threads == 0 || threads > MAX_DBT_THREADS {
        return Err(DBT_BAD_THREADS.into());
    }
    config.insert("threads".to_string(), threads.to_string());
    if let Some(vars) = &request.vars {
        let vars = serde_json::to_string(vars).map_err(|_| DBT_BAD_VARS.to_string())?;
        if vars.chars().count() > MAX_DBT_VARS_JSON_CHARS {
            return Err(DBT_BAD_VARS.into());
        }
        config.insert("vars".to_string(), vars);
    }
    Ok(config)
}

/// The `quality` task's `severity` values the loader accepts (`plan.py`'s `SEVERITIES`).
const QUALITY_SEVERITIES: [&str; 2] = ["error", "warn"];

/// `^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$`: the loader's `_CHECK_NAME`.
fn valid_check_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_')
}

/// One SQL assertion of a `quality` task as the resource stores it: the severity is resolved
/// (`"error"` when the request gave none).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct QualityCheck {
    name: String,
    sql: String,
    severity: String,
}

/// The `quality` task's checks, checked the way `plan.py`'s `_quality_checks` checks them: 1 to
/// 50 assertions, a unique loader-shaped name each, non-empty SQL, with `${table}` only when the
/// pipeline has `publish: wap` (the audit branch), and `error` or `warn`. `Err` is the sentence
/// the caller sees.
fn quality_config(
    request: &QualityRequest,
    publish: Option<Publish>,
) -> Result<Vec<QualityCheck>, String> {
    if request.checks.is_empty() || request.checks.len() > MAX_QUALITY_CHECKS {
        return Err(QUALITY_NEEDS_CHECKS.into());
    }
    let mut checks: Vec<QualityCheck> = Vec::with_capacity(request.checks.len());
    for check in &request.checks {
        if !valid_check_name(&check.name) {
            return Err(QUALITY_CHECK_NEEDS_NAME.into());
        }
        if checks.iter().any(|done| done.name == check.name) {
            return Err(QUALITY_CHECK_NAME_TWICE.into());
        }
        if check.sql.is_empty() || check.sql.chars().count() > MAX_CHECK_SQL_CHARS {
            return Err(QUALITY_CHECK_NEEDS_SQL.into());
        }
        if check.sql.contains("${table}") && publish != Some(Publish::Wap) {
            return Err(QUALITY_CHECK_AUDIT_TABLE.into());
        }
        let severity = match check.severity.as_deref() {
            None => "error",
            Some(severity) if QUALITY_SEVERITIES.contains(&severity) => severity,
            Some(_) => return Err(QUALITY_CHECK_BAD_SEVERITY.into()),
        };
        checks.push(QualityCheck {
            name: check.name.clone(),
            sql: check.sql.clone(),
            severity: severity.to_string(),
        });
    }
    Ok(checks)
}

/// `^[A-Za-z0-9][A-Za-z0-9._/-]{1,127}$`: the CRD's `project` pattern with its 128-character
/// limit, plus the segments a bundle directory may not carry.
const SPARK_PROJECT_MAX_CHARS: usize = 128;
const SPARK_NEEDS_PROJECT: &str =
    "a spark-pipeline task needs project (the bundle directory with spark-pipeline.yml)";
const SPARK_PROJECT_RULE: &str = "project must be a relative directory inside the bundle";
const SPARK_BAD_REFRESH: &str = "refresh must be incremental or full";
const SPARK_BAD_FULL_REFRESH: &str = "fullRefresh is 1 to 32 dataset names of 1 to 128 characters";
const SPARK_REFRESH_TWICE: &str =
    "a spark-pipeline task sets refresh: full or fullRefresh, not both";
/// `plan.py::SPARK_PIPELINE_NEEDS_WRITES`: a graph's catalog and database come from the task's
/// writes, so a graph without one dies in the pod (#80).
const SPARK_GRAPH_NEEDS_WRITES: &str = "spark-pipeline task `{task}` needs writes: the catalog and schema the graph writes, as \
     catalog.schema.table — stardelt sets the graph's catalog and database from it";
/// `plan.py::SPARK_PIPELINE_TWO_TARGETS`.
const SPARK_GRAPH_ONE_TARGET: &str = "spark-pipeline task `{task}` writes into more than one catalog/schema ({targets}): a graph \
     has one target; split the task";
const SPARK_NEEDS_GRAPH_FILE: &str =
    "a spark-pipeline task's files must hold spark-pipeline.yml at the project root";
const SPARK_TOO_MANY_FILES: &str = "a spark-pipeline graph is at most 64 files";
const SPARK_BAD_FILE_PATH: &str = "a graph file path is relative to the project directory";
const SPARK_GRAPH_TOO_LARGE: &str = "a spark-pipeline graph is at most 256 KiB";
/// At most this many files one graph carries (the CRD's `files` `maxProperties`).
const MAX_GRAPH_FILES: usize = 64;
/// The longest one graph file may be (the CRD's file `maxLength`).
const MAX_GRAPH_FILE_BYTES: usize = 65536;
/// The largest one graph may be over all its files; the CRD caps the count and each file, the
/// sum is checked here and by the loader.
const MAX_GRAPH_BYTES: usize = 262144;
/// At most this many datasets a full refresh rebuilds (the CRD's `fullRefresh` `maxItems`).
const MAX_FULL_REFRESH: usize = 32;
/// The longest one dataset name of `fullRefresh` may be (the CRD's item `maxLength`).
const MAX_FULL_REFRESH_NAME_CHARS: usize = 128;

/// A `spark-pipeline` task's fields checked the way the CRD checks them (#80): a relative
/// directory inside the bundle, `incremental` or `full`, and 1 to 32 dataset names — never
/// `refresh: full` together with a `fullRefresh`. `Err` is the sentence the caller sees.
fn spark_pipeline_config(request: &SparkPipelineRequest) -> Result<SparkPipelineShape, String> {
    let project = request.project.as_str();
    if project.is_empty() {
        return Err(SPARK_NEEDS_PROJECT.into());
    }
    let chars: Vec<char> = project.chars().collect();
    if chars.len() < 2
        || chars.len() > SPARK_PROJECT_MAX_CHARS
        || !chars[0].is_ascii_alphanumeric()
        || !chars
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(*c, '.' | '_' | '/' | '-'))
        || project.ends_with('/')
        || project
            .split('/')
            .any(|part| part == ".." || part == ".git")
    {
        return Err(SPARK_PROJECT_RULE.into());
    }
    if let Some(refresh) = &request.refresh
        && !matches!(refresh.as_str(), "incremental" | "full")
    {
        return Err(SPARK_BAD_REFRESH.into());
    }
    let full_refresh = request.full_refresh.clone().unwrap_or_default();
    if !full_refresh.is_empty() {
        if full_refresh.len() > MAX_FULL_REFRESH
            || full_refresh.iter().any(|name| {
                name.is_empty()
                    || name.chars().count() > MAX_FULL_REFRESH_NAME_CHARS
                    || name.chars().any(char::is_whitespace)
            })
        {
            return Err(SPARK_BAD_FULL_REFRESH.into());
        }
        if request.refresh.as_deref() == Some("full") {
            return Err(SPARK_REFRESH_TWICE.into());
        }
    }
    if !request.files.is_empty() {
        if request.files.len() > MAX_GRAPH_FILES {
            return Err(SPARK_TOO_MANY_FILES.into());
        }
        if !request.files.contains_key("spark-pipeline.yml") {
            return Err(SPARK_NEEDS_GRAPH_FILE.into());
        }
        let mut total = 0usize;
        for (path, text) in &request.files {
            if !graph_file_path_ok(path) {
                return Err(SPARK_BAD_FILE_PATH.into());
            }
            if text.len() > MAX_GRAPH_FILE_BYTES {
                return Err(SPARK_GRAPH_TOO_LARGE.into());
            }
            total += text.len();
        }
        if total > MAX_GRAPH_BYTES {
            return Err(SPARK_GRAPH_TOO_LARGE.into());
        }
    }
    Ok(SparkPipelineShape {
        project: project.to_string(),
        refresh: request.refresh.clone(),
        full_refresh,
        files: request.files.clone(),
    })
}

/// The graph's own `spark-pipeline.yml`, checked the way `plan.py::_spark_pipeline_config`
/// checks it (#80): the three keys stardelt owns, the `libraries` glob form, and the catalog
/// block the Connect session needs. `None` means it is a graph the loader accepts.
fn spark_graph_problems(files: &BTreeMap<String, String>, writes: &[String]) -> Option<String> {
    let text = files.get("spark-pipeline.yml")?;
    let document: serde_yaml::Value = match serde_yaml::from_str(text) {
        Ok(document) => document,
        Err(error) => {
            return Some(format!(
                "spark-pipeline.yml is not YAML: {}",
                error.to_string().lines().next().unwrap_or("")
            ));
        }
    };
    let Some(map) = document.as_mapping() else {
        return Some("spark-pipeline.yml is not a YAML map".into());
    };
    // `serde_yaml::Mapping::get`/`contains_key` take `str` (its `Index` impl) — never `&Value`.
    if ["catalog", "database", "storage"]
        .iter()
        .any(|key| map.contains_key(*key))
    {
        return Some(
            "spark-pipeline.yml must not set catalog, database or storage — the task's writes \
             and the group's bucket set them at run time (#80)"
                .into(),
        );
    }
    if let Some(libraries) = map.get("libraries") {
        let Some(list) = libraries.as_sequence().filter(|list| !list.is_empty()) else {
            return Some("spark-pipeline.yml libraries must be a list of glob entries".into());
        };
        for entry in list {
            let include = entry
                .as_mapping()
                .and_then(|entry| entry.get("glob"))
                .and_then(serde_yaml::Value::as_mapping)
                .and_then(|glob| glob.get("include"))
                .and_then(serde_yaml::Value::as_str)
                .filter(|include| !include.is_empty());
            let Some(include) = include else {
                return Some(
                    "spark-pipeline.yml libraries entries need glob.include: <path or pattern>"
                        .into(),
                );
            };
            let folder = include.ends_with("/**")
                && include.matches('*').count() == 2
                && include.len() > "/**".len();
            if !folder && (include.contains('*') || include.ends_with('/')) {
                return Some(format!(
                    "spark-pipeline.yml libraries glob.include must be a file path or a folder \
                     ending in /**: {include}"
                ));
            }
        }
    }
    let catalog = writes
        .first()
        .and_then(|table| table.split('.').next())
        .unwrap_or_default();
    let block = format!("spark.sql.catalog.{catalog}.type");
    let known = map
        .get("configuration")
        .and_then(serde_yaml::Value::as_mapping)
        .is_some_and(|configuration| configuration.contains_key(block.as_str()));
    if !known {
        return Some(format!(
            "catalog `{catalog}` has no `{block}` under `configuration:` — the Connect session \
             would not know the catalog"
        ));
    }
    None
}

/// A graph file's path is relative to the project directory: segments of letters, digits, `.`,
/// `_` and `-`, the first character a letter or digit, and no `..` segment.
fn graph_file_path_ok(path: &str) -> bool {
    let mut chars = path.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphanumeric() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '/' | '-'))
        && !path.split('/').any(|part| part == "..")
}

/// A `spark-pipeline` task of a task list as the resource stores it (#80): the bundle directory,
/// the resolved refresh mode and the datasets a full refresh rebuilds (`full_refresh` stays
/// empty when the request gave none).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SparkPipelineShape {
    project: String,
    refresh: Option<String>,
    full_refresh: Vec<String>,
    files: BTreeMap<String, String>,
}

/// The `run` task of a Nova-written pipeline as a write stores it (#210, #237): exactly one of a
/// statement and a pinned notebook version.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Statement {
    /// The statement as stored: trimmed, one trailing `;` dropped.
    Sql(String),
    /// The notebook fields as stored: the file name, the immutable object version and the
    /// parameter JSON text with sorted keys.
    Notebook {
        name: String,
        object_version: String,
        params: String,
        /// The `Connection` the task names (`tasks[].connection`), shape-checked only.
        connection: Option<String>,
    },
    /// A `dbt` task of a task list (`#112`): exactly the CRD's `config` map.
    Dbt(BTreeMap<String, String>),
    /// A `quality` task of a task list (`#114`): the SQL assertions, severity resolved.
    Quality(Vec<QualityCheck>),
    /// A `spark-pipeline` task of a task list (`#80`): the declared graph, checked.
    SparkPipeline(SparkPipelineShape),
    /// A whole task list, each task checked (`#905`).
    Tasks(Vec<TaskStatement>),
}

/// The body's statement with the rules of its kind applied: the SQL ones (statement, template
/// parameters, placeholders), the notebook ones (name, parameters, and a pinned version that
/// exists in the group), the dbt ones (`plan.py`'s config rules), the quality ones (`plan.py`'s
/// check rules), or the spark-pipeline ones (the CRD's project, refresh and fullRefresh rules).
/// `Err` is the sentence and the audit's reason. A body gives exactly one of a non-empty `sql`,
/// a `notebook`, a `dbt` run, a `quality` run and a `sparkPipeline` graph; two of them or none is
/// the same sentence (#237, #112, #114, #80).
#[allow(clippy::too_many_arguments)]
async fn checked_statement(
    sql: &str,
    notebook: Option<&NotebookRequest>,
    dbt: Option<&DbtRequest>,
    template_params: &BTreeMap<String, String>,
    notebooks: Option<&NotebookStore>,
    group: &str,
    quality: Option<&QualityRequest>,
    spark_pipeline: Option<&SparkPipelineRequest>,
    publish: Option<Publish>,
) -> Result<Statement, ActionError> {
    let refused = |reason: &'static str, message: String| {
        ActionError::refused(reason, Fail::new(StatusCode::BAD_REQUEST, message))
    };
    match (
        sql.trim().is_empty(),
        notebook,
        dbt,
        quality,
        spark_pipeline,
    ) {
        (true, None, Some(dbt), None, None) => dbt_config(dbt)
            .map(Statement::Dbt)
            .map_err(|message| refused("dbt", message)),
        (true, None, None, Some(quality), None) => quality_config(quality, publish)
            .map(Statement::Quality)
            .map_err(|message| refused("quality", message)),
        (true, None, None, None, Some(graph)) => spark_pipeline_config(graph)
            .map(Statement::SparkPipeline)
            .map_err(|message| refused("spark-pipeline", message)),
        (false, None, None, None, None) => {
            let sql = validate_sql(sql).map_err(|message| refused("sql", message))?;
            validate_params(template_params).map_err(|message| refused("params", message))?;
            check_placeholders(&sql, template_params)
                .map_err(|message| refused("params", message))?;
            Ok(Statement::Sql(sql))
        }
        (true, Some(notebook), None, None, None) => {
            let params =
                validate_notebook(notebook).map_err(|message| refused("notebook", message))?;
            if !notebooks::object_version_ok(&notebook.object_version) {
                // Nothing with a space in it, or longer than 128 characters, ever came out of
                // a publish: it cannot be in the store.
                return Err(refused("version", no_such_version_message(group)));
            }
            match notebooks::head_version(
                notebooks,
                group,
                &notebook.name,
                &notebook.object_version,
            )
            .await
            {
                Ok(true) => Ok(Statement::Notebook {
                    name: notebook.name.clone(),
                    object_version: notebook.object_version.clone(),
                    params,
                    connection: notebook.connection.clone(),
                }),
                Ok(false) => Err(refused("version", no_such_version_message(group))),
                Err(fail) => Err(ActionError::refused("store", fail)),
            }
        }
        _ => Err(refused("body", ONE_OF_STATEMENT_NOTEBOOK_OR_DBT.into())),
    }
}

/// One task of a task list (`#905`): its name, what must succeed first, and the statement the
/// existing per-kind rules accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskStatement {
    name: String,
    depends_on: Vec<String>,
    writes: Vec<String>,
    statement: Statement,
}

/// The task a `TaskStatement` carries as the resource stores it.
fn task_json(task: &TaskStatement) -> Value {
    let mut value = match &task.statement {
        Statement::Sql(sql) => json!({ "name": task.name, "kind": "sql", "sql": sql }),
        Statement::Notebook {
            name,
            object_version,
            params,
            connection,
        } => {
            let mut value = json!({
                "name": task.name,
                "kind": "notebook",
                "config": { "notebook": name, "objectVersion": object_version, "params": params },
            });
            if let Some(connection) = connection {
                value["connection"] = json!(connection);
            }
            value
        }
        // `checked_statement` never answers a task list.
        Statement::Dbt(config) => json!({ "name": task.name, "kind": "dbt", "config": config }),
        Statement::Quality(checks) => {
            json!({ "name": task.name, "kind": "quality", "checks": checks })
        }
        Statement::SparkPipeline(graph) => {
            let mut task =
                json!({ "name": task.name, "kind": "spark-pipeline", "project": graph.project });
            if let Some(refresh) = &graph.refresh {
                task["refresh"] = json!(refresh);
            }
            if !graph.full_refresh.is_empty() {
                task["fullRefresh"] = json!(graph.full_refresh);
            }
            if !graph.files.is_empty() {
                task["files"] = json!(graph.files);
            }
            task
        }
        Statement::Tasks(_) => unreachable!("a task carries one statement, not a list"),
    };
    if !task.depends_on.is_empty() {
        value["dependsOn"] = json!(task.depends_on);
    }
    if !task.writes.is_empty() {
        value["writes"] = json!(task.writes);
    }
    value
}

/// At most this many tasks: the CRD's `spec.tasks` `maxItems` (`PipelineSpec`, measured live).
const MAX_TASKS: usize = 30;
/// At most this many dependencies per task: the CRD's `tasks[].dependsOn` `maxItems`.
const MAX_TASK_DEPENDS_ON: usize = 8;
const TOO_MANY_TASKS: &str = "at most 30 tasks";
const NO_TASKS: &str = "give at least one task";
const TASK_NAME_RULE: &str = "task names are 1 to 63 lower-case letters, digits, dashes and underscores, starting with a \
     letter or digit";
const TOO_MANY_TASK_DEPENDS_ON: &str = "a task depends on at most 8 tasks";
const ONE_OF_STATEMENT_NOTEBOOK_OR_TASKS: &str = "give a statement, a notebook or a task list";
/// A task list is edited in its file, not in the dialog (`#905` decision 5).
const SEVERAL_TASKS: &str =
    "this pipeline has several tasks; change it in its file and deploy again";

/// `^[a-z0-9][a-z0-9_-]{0,62}$`: the lower-case subset of the CRD's task-name pattern.
fn valid_task_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-' || *byte == b'_'
        })
}

/// Refuse a task list whose dependencies loop. The operator's `PipelineSpec::validate` runs the
/// same check (Kahn's algorithm); Nova answers a sentence instead of the API server's 422.
fn check_task_cycles(tasks: &[TaskStatement]) -> Result<(), String> {
    fn visit(at: usize, tasks: &[TaskStatement], state: &mut [u8]) -> Result<(), String> {
        match state[at] {
            2 => return Ok(()),
            1 => {
                return Err(format!(
                    "task `{}` is part of a dependency cycle",
                    tasks[at].name
                ));
            }
            _ => {}
        }
        state[at] = 1;
        for dependency in &tasks[at].depends_on {
            if let Some(next) = tasks.iter().position(|task| &task.name == dependency) {
                visit(next, tasks, state)?;
            }
        }
        state[at] = 2;
        Ok(())
    }
    // 0 = unvisited, 1 = on the current path, 2 = done. At most 30 tasks, so the recursion depth
    // is bounded.
    let mut state = vec![0u8; tasks.len()];
    for at in 0..tasks.len() {
        visit(at, tasks, &mut state)?;
    }
    Ok(())
}

/// The `tasks` a body gives, checked: each task's name, its dependencies and exactly one statement
/// or pinned notebook under the rules of `checked_statement`, then the list's own rules (1 to 30
/// tasks, unique names, every dependency names a task of the list, no cycle). `Err` is one sentence
/// that names the task (`#905` decision 2).
async fn checked_tasks(
    tasks: &[PipelineTaskRequest],
    template_params: &BTreeMap<String, String>,
    notebooks: Option<&NotebookStore>,
    group: &str,
    publish: Option<Publish>,
) -> Result<Vec<TaskStatement>, ActionError> {
    let refused = |reason: &'static str, message: String| {
        ActionError::refused(reason, Fail::new(StatusCode::BAD_REQUEST, message))
    };
    if tasks.is_empty() {
        return Err(refused("tasks", NO_TASKS.into()));
    }
    if tasks.len() > MAX_TASKS {
        return Err(refused("tasks", TOO_MANY_TASKS.into()));
    }
    let mut checked: Vec<TaskStatement> = Vec::with_capacity(tasks.len());
    for task in tasks {
        if !valid_task_name(&task.name) {
            return Err(refused(
                "task-name",
                format!("task name `{}` is not valid: {TASK_NAME_RULE}", task.name),
            ));
        }
        if checked.iter().any(|done| done.name == task.name) {
            return Err(refused(
                "task-name",
                format!("task name `{}` is used twice", task.name),
            ));
        }
        if task.depends_on.len() > MAX_TASK_DEPENDS_ON {
            return Err(refused(
                "task-depends",
                format!("task `{}`: {TOO_MANY_TASK_DEPENDS_ON}", task.name),
            ));
        }
        if task
            .depends_on
            .iter()
            .any(|dependency| dependency == &task.name)
        {
            return Err(refused(
                "task-depends",
                format!("task `{}` cannot depend on itself", task.name),
            ));
        }
        if !task.writes.is_empty() && task.spark_pipeline.is_none() && task.notebook.is_none() {
            return Err(refused(
                "task-writes",
                format!(
                    "task `{}` cannot write a table: only a spark-pipeline or notebook task declares writes",
                    task.name
                ),
            ));
        }
        if task.writes.len() > 16 {
            return Err(refused(
                "task-writes",
                format!("task `{}` names more than 16 tables", task.name),
            ));
        }
        for table in &task.writes {
            let parts: Vec<&str> = table.split('.').collect();
            if parts.len() != 3
                || parts
                    .iter()
                    .any(|part| part.is_empty() || part.contains(' '))
            {
                return Err(refused(
                    "task-writes",
                    format!(
                        "task `{}` writes `{table}`, which is not catalog.schema.table",
                        task.name
                    ),
                ));
            }
        }
        if task.spark_pipeline.is_some() {
            if task.writes.is_empty() {
                return Err(refused(
                    "task-writes",
                    SPARK_GRAPH_NEEDS_WRITES.replace("{task}", &task.name),
                ));
            }
            let targets: std::collections::BTreeSet<&str> = task
                .writes
                .iter()
                .map(|table| {
                    table
                        .rsplit_once('.')
                        .map_or(table.as_str(), |(target, _)| target)
                })
                .collect();
            if targets.len() > 1 {
                return Err(refused(
                    "task-writes",
                    SPARK_GRAPH_ONE_TARGET
                        .replace("{task}", &task.name)
                        .replace(
                            "{targets}",
                            &targets.into_iter().collect::<Vec<_>>().join(", "),
                        ),
                ));
            }
        }
        if let Some(graph) = task.spark_pipeline.as_ref()
            && !graph.files.is_empty()
            && let Some(message) = spark_graph_problems(&graph.files, &task.writes)
        {
            return Err(refused(
                "task-graph",
                format!("task `{}`: {message}", task.name),
            ));
        }
        // The same rules the single `run` task gets (#821, #237); `checked_run` has already
        // validated the pipeline-level parameter table, so a refusal here is the task's own.
        let statement = checked_statement(
            &task.sql,
            task.notebook.as_ref(),
            task.dbt.as_ref(),
            template_params,
            notebooks,
            group,
            task.quality.as_ref(),
            task.spark_pipeline.as_ref(),
            publish,
        )
        .await
        .map_err(|error| error.about_task(&task.name))?;
        checked.push(TaskStatement {
            name: task.name.clone(),
            depends_on: task.depends_on.clone(),
            writes: task.writes.clone(),
            statement,
        });
    }
    for task in &checked {
        for dependency in &task.depends_on {
            if !checked.iter().any(|other| &other.name == dependency) {
                return Err(refused(
                    "task-depends",
                    format!(
                        "task `{}` depends on `{dependency}`, which is not a task of this pipeline",
                        task.name
                    ),
                ));
            }
        }
    }
    check_task_cycles(&checked).map_err(|message| refused("task-cycle", message))?;
    Ok(checked)
}

/// `publish: wap` moves `main` only after a `quality` task's checks passed, and only for the
/// tables a `spark-pipeline` or `notebook` task writes: a body that asks for it without both is
/// refused here, the same rule the CRD's CEL carries (#114).
fn check_publish(
    publish: Option<Publish>,
    tasks: Option<&[PipelineTaskRequest]>,
) -> Result<(), ActionError> {
    if publish != Some(Publish::Wap) {
        return Ok(());
    }
    let ready = tasks.is_some_and(|tasks| {
        tasks.iter().any(|task| {
            task.quality
                .as_ref()
                .is_some_and(|quality| !quality.checks.is_empty())
        }) && tasks.iter().any(|task| !task.writes.is_empty())
    });
    if ready {
        // `${table}` is ONE table's audit branch: the loader fills it in only when the pipeline
        // writes one table, so a second written table makes the check's pod fail at run time.
        let mut written = std::collections::BTreeSet::new();
        let mut placeholder = false;
        for task in tasks.unwrap_or_default() {
            written.extend(task.writes.iter().map(String::as_str));
            if let Some(quality) = task.quality.as_ref() {
                placeholder |= quality
                    .checks
                    .iter()
                    .any(|check| check.sql.contains("${table}"));
            }
        }
        if placeholder && written.len() > 1 {
            return Err(ActionError::refused(
                "publish",
                Fail::new(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "${{table}} names the audit branch of one table, and this pipeline writes {}: name the table in the check, or split the pipeline",
                        written.len()
                    ),
                ),
            ));
        }
        return Ok(());
    }
    Err(ActionError::refused(
        "publish",
        Fail::new(
            StatusCode::BAD_REQUEST,
            "publish: wap needs a task list with a quality task that has a check and a spark-pipeline or notebook task that writes a table",
        ),
    ))
}

/// What a body asks the pipeline to run: one statement, one pinned notebook, or a task list —
/// exactly one of the three, each under the rules of its kind (`#905` decision 1).
async fn checked_run(
    sql: &str,
    notebook: Option<&NotebookRequest>,
    tasks: Option<&[PipelineTaskRequest]>,
    template_params: &BTreeMap<String, String>,
    notebooks: Option<&NotebookStore>,
    group: &str,
    publish: Option<Publish>,
) -> Result<Statement, ActionError> {
    match (sql.trim().is_empty(), notebook.is_some(), tasks) {
        // A body without `tasks` behaves exactly as today, sentence included. A body's own
        // `dbt` or `quality` is not a shape this route takes (#112, #114): only a task list
        // carries one.
        (_, _, None) => {
            checked_statement(
                sql,
                notebook,
                None,
                template_params,
                notebooks,
                group,
                None,
                None,
                publish,
            )
            .await
        }
        (true, false, Some(tasks)) => {
            // Once for the whole list: the parameters are the pipeline's, shared by every task.
            validate_params(template_params).map_err(|message| {
                ActionError::refused("params", Fail::new(StatusCode::BAD_REQUEST, message))
            })?;
            checked_tasks(tasks, template_params, notebooks, group, publish)
                .await
                .map(Statement::Tasks)
        }
        _ => Err(ActionError::refused(
            "body",
            Fail::new(StatusCode::BAD_REQUEST, ONE_OF_STATEMENT_NOTEBOOK_OR_TASKS),
        )),
    }
}

/// What a stored pipeline runs, for the rules only an update has (`#905` decision 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoredShape {
    /// The form's single `run` task, of this kind (`sql` or `notebook`).
    Run(&'static str),
    /// A task list: every task is `sql`, `notebook`, `dbt`, `quality` or `spark-pipeline`, and
    /// it is not the single `run` task.
    Tasks,
    /// A shape Nova does not write (another kind, or no tasks at all).
    Other,
}

/// A task list is a task list whatever each task runs: `sql`, `notebook`, `dbt`, `quality` or
/// `spark-pipeline`.
fn is_task_list(resource: &Value) -> bool {
    if run_task_kind(resource).is_some() {
        return false;
    }
    resource
        .get("spec")
        .and_then(|spec| spec.get("tasks"))
        .and_then(Value::as_array)
        .is_some_and(|tasks| {
            !tasks.is_empty()
                && tasks.iter().all(|task| {
                    matches!(
                        task.get("kind").and_then(Value::as_str),
                        Some("sql" | "notebook" | "dbt" | "quality" | "spark-pipeline")
                    )
                })
        })
}

fn stored_shape(resource: &Value) -> StoredShape {
    match run_task_kind(resource) {
        Some("notebook") => StoredShape::Run("notebook"),
        Some(_) => StoredShape::Run("sql"),
        None if is_task_list(resource) => StoredShape::Tasks,
        None => StoredShape::Other,
    }
}

/// The word the form uses for what a pipeline runs, in `changes_of`: the single task's kind, or
/// `tasks` for a task list (`#905` decision 4).
fn run_word(resource: &Value) -> &'static str {
    match run_task_kind(resource) {
        Some("notebook") => "notebook",
        Some(_) => "statement",
        None => "tasks",
    }
}

/// The sentence a version that is not in the store answers (#237).
fn no_such_version_message(group: &str) -> String {
    format!("that notebook version does not exist in group {group}")
}

/// One `${…}` the loader's `expand.py` would replace: the offset of its `${`, the offset just
/// past its `}`, and the name inside. `from` is where the search starts.
///
/// The grammar is deliberately the loader's (`\$\{[A-Za-z_][A-Za-z0-9_.]*\}`): a `${` its regex
/// does not match is no placeholder, and the loader leaves it in the statement — Nova must not
/// refuse a statement the loader happily runs.
fn next_placeholder(statement: &str, from: usize) -> Option<(usize, usize, &str)> {
    let mut at = from;
    while let Some(found) = statement.get(at..)?.find("${") {
        let start = at + found;
        let body = start + 2;
        let rest = statement.get(body..)?;
        let length = rest
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'.')
            .count();
        let name = rest.get(..length)?;
        let first_ok = name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
        let closes = rest.as_bytes().get(length) == Some(&b'}');
        if first_ok && closes {
            return Some((start, body + length + 1, name));
        }
        at = body;
    }
    None
}

/// Every `${…}` in the statement must be one the loader can fill: `run.date`, `run.id`, or
/// `params.<name>` for a parameter this schedule defines. An unknown one fails the run
/// (`expand.py` raises), so Nova refuses the write instead.
fn check_placeholders(statement: &str, params: &BTreeMap<String, String>) -> Result<(), String> {
    let mut at = 0;
    while let Some((_, end, name)) = next_placeholder(statement, at) {
        let known = name == "run.date"
            || name == "run.id"
            || name
                .strip_prefix("params.")
                .is_some_and(|key| params.contains_key(key));
        if !known {
            return Err(format!("unknown placeholder ${{{name}}}"));
        }
        at = end;
    }
    Ok(())
}

/// The statement with every placeholder replaced, in one pass so a value that itself contains
/// `${…}` is never expanded again. Unknown placeholders are refused before this runs.
fn fill_placeholders(
    statement: &str,
    run_date: &str,
    run_id: &str,
    params: &BTreeMap<String, String>,
) -> String {
    let mut out = String::with_capacity(statement.len());
    let mut at = 0;
    while let Some((start, end, name)) = next_placeholder(statement, at) {
        out.push_str(statement.get(at..start).unwrap_or_default());
        let value = match name {
            "run.date" => Some(run_date),
            "run.id" => Some(run_id),
            other => other
                .strip_prefix("params.")
                .and_then(|key| params.get(key))
                .map(String::as_str),
        };
        match value {
            Some(value) => out.push_str(value),
            // Unreachable after `check_placeholders`; leaving the text alone is the safe answer.
            None => out.push_str(statement.get(start..end).unwrap_or_default()),
        }
        at = end;
    }
    out.push_str(statement.get(at..).unwrap_or_default());
    out
}

const CRON_FIELDS: &str = "a schedule has five fields: minute hour day month weekday";
const CRON_RANGE: &str = "a field is out of range";
const CRON_TOO_OFTEN: &str = "at most one run every 5 minutes";

const MONTH_NAMES: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];
const WEEKDAY_NAMES: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

/// One number or name in a cron field, mapped to its value (months count from 1, weekdays from
/// Sunday = 0) and checked against the field's range.
fn cron_value(text: &str, min: u32, max: u32, names: &[&str]) -> Result<u32, String> {
    let value = if let Ok(number) = text.parse::<u32>() {
        number
    } else if let Some(index) = names
        .iter()
        .position(|name| name.eq_ignore_ascii_case(text))
    {
        min + index as u32
    } else {
        return Err(CRON_RANGE.into());
    };
    if value < min || value > max {
        return Err(CRON_RANGE.into());
    }
    Ok(value)
}

/// One field: a comma list of `*`, `*/n`, `a`, `a-b` and `a-b/n`, with every number in range.
fn cron_field(field: &str, min: u32, max: u32, names: &[&str]) -> Result<BTreeSet<u32>, String> {
    let mut values = BTreeSet::new();
    for part in field.split(',') {
        if part.is_empty() {
            return Err(CRON_RANGE.into());
        }
        let (body, step) = match part.split_once('/') {
            Some((body, step)) if !step.contains('/') => (body, Some(step)),
            Some(_) => return Err(CRON_RANGE.into()),
            None => (part, None),
        };
        let step = match step {
            Some(step) => step
                .parse::<u32>()
                .ok()
                .filter(|step| *step >= 1)
                .ok_or_else(|| CRON_RANGE.to_string())?,
            None => 1,
        };
        let (start, end) = if body == "*" {
            (min, max)
        } else if let Some((start, end)) = body.split_once('-') {
            let start = cron_value(start, min, max, names)?;
            let end = cron_value(end, min, max, names)?;
            if start > end {
                return Err(CRON_RANGE.into());
            }
            (start, end)
        } else {
            let single = cron_value(body, min, max, names)?;
            // `a/n` is not one of the five shapes: only `*/n` and `a-b/n` take a step.
            if step != 1 {
                return Err(CRON_RANGE.into());
            }
            (single, single)
        };
        let mut value = start;
        while value <= end {
            values.insert(value);
            value += step;
        }
    }
    Ok(values)
}

/// The minutes of an hour a cron fires on, from its minute field.
fn cron_minutes(field: &str) -> Result<BTreeSet<u32>, String> {
    cron_field(field, 0, 59, &[])
}

/// `true` when two fires are closer than five minutes, or more than twelve fire in an hour.
/// Only the minute field can make an hourly schedule fire more often than hourly; a set with a
/// gap under five minutes (including the wrap from the last minute to the first) is refused.
fn fires_too_often(minutes: &BTreeSet<u32>) -> bool {
    if minutes.len() > 12 {
        return true;
    }
    let values: Vec<u32> = minutes.iter().copied().collect();
    let adjacently_too_close = values.windows(2).any(|pair| pair[1] - pair[0] < 5);
    let wrap_too_close = match (values.first(), values.last()) {
        (Some(first), Some(last)) if values.len() > 1 => (60 - last) + first < 5,
        _ => false,
    };
    adjacently_too_close || wrap_too_close
}

/// The schedule rules of the loader: five fields, in range, no `@daily` shortcut, no seconds,
/// and never more often than every five minutes. `Err` is the sentence the caller sees.
pub fn validate_cron(cron: &str) -> Result<(), String> {
    parse_cron(cron).map(|_| ())
}

/// A parsed cron: the values each field names. The preview walks it (#223); `validate_cron` is
/// this parser's verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CronSchedule {
    minutes: BTreeSet<u32>,
    hours: BTreeSet<u32>,
    days: BTreeSet<u32>,
    months: BTreeSet<u32>,
    weekdays: BTreeSet<u32>,
}

/// Parse and validate in one place, so the preview and the create route cannot drift: the field
/// sets, the `7` = Sunday spelling, and the same sentences.
fn parse_cron(cron: &str) -> Result<CronSchedule, String> {
    let fields: Vec<&str> = cron.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(CRON_FIELDS.into());
    }
    let minutes = cron_minutes(fields[0])?;
    let hours = cron_field(fields[1], 0, 23, &[])?;
    let days = cron_field(fields[2], 1, 31, &[])?;
    let months = cron_field(fields[3], 1, 12, &MONTH_NAMES)?;
    let mut weekdays = cron_field(fields[4], 0, 7, &WEEKDAY_NAMES)?;
    if weekdays.remove(&7) {
        weekdays.insert(0);
    }
    if fires_too_often(&minutes) {
        return Err(CRON_TOO_OFTEN.into());
    }
    Ok(CronSchedule {
        minutes,
        hours,
        days,
        months,
        weekdays,
    })
}

/// The next `count` times the schedule fires in `zone`, strictly after `now`.
///
/// Walks the local days the cron names (at most 366 of them, so a schedule that can never fire —
/// `0 0 31 2 *` — answers fewer runs instead of spinning), then their hours and minutes. Like
/// croniter, a day matches when both day fields are restricted and either matches, and when one
/// of them is unrestricted and the other matches. A wall-clock time the zone skips at a DST jump
/// does not exist and is not a run; a wall-clock time that happens twice (the clocks going back)
/// is answered with its first occurrence.
fn next_runs(
    schedule: &CronSchedule,
    zone: Tz,
    count: usize,
    now: DateTime<Utc>,
) -> Vec<DateTime<Tz>> {
    let mut runs = Vec::with_capacity(count);
    // `*` and any expression that selects every day mean the same thing; the dom/dow rule only
    // cares whether a field actually restricts the day.
    let day_restricted = schedule.days.len() < 31;
    let weekday_restricted = schedule.weekdays.len() < 7;
    let today = now.with_timezone(&zone).date_naive();
    for offset in 0..=366 {
        if runs.len() == count {
            break;
        }
        let Some(day) = today.checked_add_days(Days::new(offset)) else {
            break;
        };
        if !schedule.months.contains(&day.month()) {
            continue;
        }
        let day_of_month = schedule.days.contains(&day.day());
        let day_of_week = schedule
            .weekdays
            .contains(&day.weekday().num_days_from_sunday());
        let matches = if day_restricted && weekday_restricted {
            day_of_month || day_of_week
        } else {
            day_of_month && day_of_week
        };
        if !matches {
            continue;
        }
        for hour in &schedule.hours {
            for minute in &schedule.minutes {
                let Some(local) = day.and_hms_opt(*hour, *minute, 0) else {
                    continue;
                };
                let fires_at = match zone.from_local_datetime(&local) {
                    LocalResult::None => continue,
                    LocalResult::Single(fires_at) => fires_at,
                    LocalResult::Ambiguous(first, _) => first,
                };
                if fires_at.with_timezone(&Utc) > now {
                    runs.push(fires_at);
                    if runs.len() == count {
                        return runs;
                    }
                }
            }
        }
    }
    runs
}

/// The validated schedule: the cron text and the IANA zone, as they go into the resource.
fn validate_schedule(
    schedule: Option<&ScheduleRequest>,
) -> Result<Option<(String, String)>, String> {
    let Some(schedule) = schedule else {
        return Ok(None);
    };
    let cron = schedule.cron.trim();
    validate_cron(cron)?;
    let timezone = schedule.timezone.trim();
    if Tz::from_str(timezone).is_err() {
        return Err("unknown timezone".into());
    }
    Ok(Some((cron.to_string(), timezone.to_string())))
}

/// The sentence for a group whose principal an administrator has not created yet. It is the
/// `principals` route's message and the create/update refusal, word for word.
fn no_principal_message(group: &str) -> String {
    format!(
        "No principal for this group yet: an administrator creates one with \
         make sync-principal NAME=pipeline-{group} GROUP={group}"
    )
}

/// Why a principal is not ready: its `Ready` condition's message, or `no status yet`.
fn principal_message(principal: &Value) -> String {
    principal
        .get("status")
        .and_then(|status| status.get("conditions"))
        .and_then(Value::as_array)
        .and_then(|conditions| {
            conditions
                .iter()
                .find(|condition| condition.get("type").and_then(Value::as_str) == Some("Ready"))
        })
        .and_then(|condition| condition.get("message"))
        .and_then(Value::as_str)
        .filter(|message| !message.is_empty())
        .unwrap_or("no status yet")
        .to_string()
}

/// One group's principal: `pipeline-<group>` when a `ServicePrincipal` of that name with
/// `spec.ownerGroup` equal to the group exists, its `status.ready`, and why not when it is not.
pub(crate) fn principal_view(principals: &[Value], group: &str) -> PrincipalView {
    let name = format!("pipeline-{group}");
    let found = principals.iter().find(|principal| {
        principal
            .get("metadata")
            .and_then(|metadata| metadata.get("name"))
            .and_then(Value::as_str)
            == Some(name.as_str())
            && principal
                .get("spec")
                .and_then(|spec| spec.get("ownerGroup"))
                .and_then(Value::as_str)
                == Some(group)
    });
    match found {
        Some(principal) => {
            let ready = principal
                .get("status")
                .and_then(|status| status.get("ready"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            PrincipalView {
                group: group.to_string(),
                principal: Some(name),
                ready,
                message: (!ready).then(|| principal_message(principal)),
            }
        }
        None => PrincipalView {
            group: group.to_string(),
            principal: None,
            ready: false,
            message: Some(no_principal_message(group)),
        },
    }
}

/// Every principal the caller may see, sorted by name. The visibility rule is `visible()`,
/// the same one the pipelines list uses; a resource without `metadata.name` is skipped.
fn visible_principals(principals: &[Value], who: &Identity) -> Vec<ServicePrincipalView> {
    let mut views: Vec<ServicePrincipalView> = principals
        .iter()
        .filter_map(|principal| {
            let name = pipeline_name(principal);
            if name.is_empty() {
                return None;
            }
            let spec = spec_of(principal);
            if !visible(who, spec) {
                return None;
            }
            let purpose = spec
                .get("purpose")
                .and_then(Value::as_str)
                .unwrap_or("pipeline")
                .to_string();
            // The CRD defaults `audiences` to trino+lakekeeper, so an empty list only
            // happens for a resource that bypassed the schema.
            let audiences = spec
                .get("audiences")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            let ready = principal
                .get("status")
                .and_then(|status| status.get("ready"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let status = principal.get("status");
            let status_text = |key: &str| {
                status
                    .and_then(|status| status.get(key))
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
            };
            Some(ServicePrincipalView {
                name: name.to_string(),
                owner_group: spec
                    .get("ownerGroup")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                purpose,
                audiences,
                ready,
                message: (!ready).then(|| principal_message(principal)),
                workload_identity: status_text("workloadIdentity"),
                client_id: status_text("clientId"),
            })
        })
        .collect();
    views.sort_by(|a, b| a.name.cmp(&b.name));
    views
}

/// Whether Nova wrote this pipeline (and may therefore change or delete it).
pub(crate) fn nova_managed(resource: &Value) -> bool {
    resource
        .get("metadata")
        .and_then(|metadata| metadata.get("labels"))
        .and_then(|labels| labels.get(MANAGED_BY_LABEL))
        .and_then(Value::as_str)
        == Some(MANAGED_BY_NOVA)
}

/// The kind of the pipeline's single `run` task when the form may edit it — `sql` or
/// `notebook` (#210, #237). Anything else is a shape the form does not edit, and an update
/// never changes the kind.
fn run_task_kind(resource: &Value) -> Option<&str> {
    let task = resource
        .get("spec")?
        .get("tasks")?
        .as_array()
        .filter(|tasks| tasks.len() == 1)?
        .first()?;
    if task.get("name").and_then(Value::as_str) != Some("run") {
        return None;
    }
    match task.get("kind").and_then(Value::as_str)? {
        kind @ ("sql" | "notebook") => Some(kind),
        _ => None,
    }
}

/// The `Pipeline` *Schedule* writes: the ticket's shape (#210), the caller's parameters and
/// options (#223), a statement or a pinned notebook version (#237), the notifications it names
/// (nova#815), nothing else.
#[allow(clippy::too_many_arguments)]
fn pipeline_resource(
    name: &str,
    group: &str,
    statement: &Statement,
    schedule: Option<&(String, String)>,
    params: &BTreeMap<String, String>,
    options: &PipelineOptionsView,
    created_by: &str,
    notify: Option<&PipelineNotify>,
    viewers: &[String],
    description: Option<&str>,
    publish: Option<Publish>,
) -> Value {
    let tasks = match statement {
        Statement::Sql(sql) => json!([{ "name": "run", "kind": "sql", "sql": sql }]),
        Statement::Notebook {
            name,
            object_version,
            params,
            connection,
        } => {
            let mut task = json!({
                "name": "run",
                "kind": "notebook",
                "config": { "notebook": name, "objectVersion": object_version, "params": params },
            });
            if let Some(connection) = connection {
                task["connection"] = json!(connection);
            }
            json!([task])
        }
        Statement::Tasks(tasks) => Value::Array(tasks.iter().map(task_json).collect()),
        // A top-level body never carries a `dbt` run: only a task list does (#112).
        Statement::Dbt(_) => unreachable!("a dbt run is one task of a task list"),
        // Nor a `quality` run (#114).
        Statement::Quality(_) => unreachable!("a quality run is one task of a task list"),
        // Nor a `spark-pipeline` graph (#80).
        Statement::SparkPipeline(_) => {
            unreachable!("a spark-pipeline graph is one task of a task list")
        }
    };
    let mut spec = json!({
        "ownerGroup": group,
        "runAs": format!("pipeline-{group}"),
        "tasks": tasks,
        "concurrency": options.concurrency,
        "retries": { "attempts": options.retries, "backoff": format!("{}m", options.backoff_minutes) },
        "timeout": format!("{}m", options.timeout_minutes),
    });
    if !params.is_empty() {
        spec["params"] = json!(params);
    }
    if let Some((cron, timezone)) = schedule {
        spec["schedule"] = json!({ "cron": cron, "timezone": timezone });
    }
    // Never an empty object: the CRD refuses `notify` without an owner group, and an empty
    // object says nothing a reader can use.
    if let Some(notify) = notify.filter(|notify| !notify.is_empty()) {
        spec["notify"] = serde_json::to_value(notify).unwrap_or(Value::Null);
    }
    if !viewers.is_empty() {
        spec["viewers"] = json!(viewers);
    }
    if let Some(line) = description {
        spec["description"] = json!(line);
    }
    if publish == Some(Publish::Wap) {
        spec["publish"] = json!("wap");
    }
    let mut labels = serde_json::Map::new();
    labels.insert(MANAGED_BY_LABEL.to_string(), json!(MANAGED_BY_NOVA));
    labels.insert("stardelt.io/owner-group".to_string(), json!(group));
    json!({
        "apiVersion": "stardelt.io/v1alpha1",
        "kind": "Pipeline",
        "metadata": {
            "name": name,
            "namespace": crate::cluster::namespace(),
            "labels": Value::Object(labels),
            "annotations": { "stardelt.io/created-by": created_by },
        },
        "spec": spec,
    })
}

/// The resource an update writes: `resource` with the statement or the pinned notebook, the
/// options, the parameters and the schedule replaced by the body's (the body describes the whole
/// editable state, nothing is merged), and nothing else touched. The update and the validation
/// both build it here, so what is checked is what is written.
#[allow(clippy::too_many_arguments)]
fn updated_resource(
    resource: &Value,
    statement: &Statement,
    options: &PipelineOptionsView,
    params: &BTreeMap<String, String>,
    schedule: Option<&(String, String)>,
    notify: Option<&PipelineNotify>,
    viewers: Option<&[String]>,
    description: Option<&str>,
    publish: Option<Publish>,
) -> Value {
    let mut updated = resource.clone();
    match statement {
        Statement::Sql(sql) => updated["spec"]["tasks"][0]["sql"] = json!(sql),
        Statement::Notebook {
            name,
            object_version,
            params,
            connection,
        } => {
            updated["spec"]["tasks"][0]["config"] = json!({
                "notebook": name,
                "objectVersion": object_version,
                "params": params,
            });
            match connection {
                Some(c) => updated["spec"]["tasks"][0]["connection"] = json!(c),
                None => {
                    if let Some(task) = updated["spec"]["tasks"][0].as_object_mut() {
                        task.remove("connection");
                    }
                }
            }
        }
        Statement::Tasks(tasks) => {
            updated["spec"]["tasks"] = Value::Array(tasks.iter().map(task_json).collect());
        }
        // An update body never carries a `dbt` run: it names no `dbt` field (#112).
        Statement::Dbt(_) => unreachable!("a dbt run is one task of a task list"),
        // Nor a `quality` run (#114).
        Statement::Quality(_) => unreachable!("a quality run is one task of a task list"),
        // Nor a `spark-pipeline` graph (#80).
        Statement::SparkPipeline(_) => {
            unreachable!("a spark-pipeline graph is one task of a task list")
        }
    }
    updated["spec"]["concurrency"] = json!(options.concurrency);
    updated["spec"]["retries"] = json!({
        "attempts": options.retries,
        "backoff": format!("{}m", options.backoff_minutes),
    });
    updated["spec"]["timeout"] = json!(format!("{}m", options.timeout_minutes));
    if params.is_empty() {
        if let Some(spec) = updated.get_mut("spec").and_then(Value::as_object_mut) {
            spec.remove("params");
        }
    } else {
        updated["spec"]["params"] = json!(params);
    }
    match schedule {
        Some((cron, timezone)) => {
            updated["spec"]["schedule"] = json!({ "cron": cron, "timezone": timezone });
        }
        None => {
            if let Some(spec) = updated.get_mut("spec").and_then(Value::as_object_mut) {
                spec.remove("schedule");
            }
        }
    }
    // The body describes the whole editable state, as it does for `schedule`: `None` removes a
    // line the resource carries.
    match description {
        Some(line) => updated["spec"]["description"] = json!(line),
        None => {
            if let Some(spec) = updated.get_mut("spec").and_then(Value::as_object_mut) {
                spec.remove("description");
            }
        }
    }
    // `None` leaves what the resource carries: until the form edits notifications, a save must
    // not wipe one a bundle deployed. All three lists empty removes the key.
    match notify {
        Some(notify) if !notify.is_empty() => {
            updated["spec"]["notify"] = serde_json::to_value(notify).unwrap_or(Value::Null);
        }
        Some(_) => {
            if let Some(spec) = updated.get_mut("spec").and_then(Value::as_object_mut) {
                spec.remove("notify");
            }
        }
        None => {}
    }
    // `None` keeps what the resource carries, like `notify`: a form save must not wipe the
    // viewers a bundle deployed. All empty removes the key.
    match viewers {
        Some(viewers) if !viewers.is_empty() => updated["spec"]["viewers"] = json!(viewers),
        Some(_) => {
            if let Some(spec) = updated.get_mut("spec").and_then(Value::as_object_mut) {
                spec.remove("viewers");
            }
        }
        None => {}
    }
    // `None` keeps what the resource carries, like `viewers`: a form save must not turn an
    // audited pipeline into a direct one.
    match publish {
        Some(Publish::Wap) => updated["spec"]["publish"] = json!("wap"),
        Some(Publish::Direct) => {
            if let Some(spec) = updated.get_mut("spec").and_then(Value::as_object_mut) {
                spec.remove("publish");
            }
        }
        None => {}
    }
    updated
}

/// What differs between a stored pipeline and the resource an update would write, in the words
/// of the form: `statement` or `notebook`, `schedule`, `description`, `params`, `options` (any of
/// the concurrency, retries and time limit), and `deployment` when the bundle, the target or the
/// commit a deploy stamped differs.
fn changes_of(before_resource: &Value, after_resource: &Value) -> Vec<String> {
    let (before, after) = (spec_of(before_resource), spec_of(after_resource));
    let run = run_word(after_resource);
    let options = ["concurrency", "retries", "timeout"]
        .iter()
        .any(|key| before.get(key) != after.get(key));
    // Who deployed and when change with every deploy; the bundle, the target and the commit are
    // what a person means by "the deployment changed".
    let deployed =
        |resource: &Value| deployment_of(resource).map(|d| (d.bundle, d.target, d.revision));
    [
        (run, before.get("tasks") != after.get("tasks")),
        ("schedule", before.get("schedule") != after.get("schedule")),
        (
            "description",
            before.get("description") != after.get("description"),
        ),
        ("params", before.get("params") != after.get("params")),
        ("options", options),
        (
            "deployment",
            deployed(before_resource) != deployed(after_resource),
        ),
    ]
    .into_iter()
    .filter(|(_, changed)| *changed)
    .map(|(name, _)| name.to_string())
    .collect()
}

/// The in-memory create rate limit (`AppState::pipeline_creates`): per signed-in `sub`, the
/// creation times inside the last hour.
pub type CreateLimiter = std::sync::Mutex<HashMap<String, VecDeque<Instant>>>;

/// Take one creation slot for `sub`, or report that the hour is full. Refusals before the
/// limiter (bad name, foreign group, missing principal, bad body, name taken) never reach it;
/// only a restart forgets it.
fn take_create_slot(limiter: &CreateLimiter, sub: &str) -> bool {
    let mut map = limiter.lock().expect("create limiter poisoned");
    let now = Instant::now();
    let slots = map.entry(sub.to_string()).or_default();
    while slots
        .front()
        .is_some_and(|then| now.duration_since(*then) >= CREATE_WINDOW)
    {
        slots.pop_front();
    }
    if slots.len() >= CREATES_PER_HOUR {
        false
    } else {
        slots.push_back(now);
        true
    }
}

// ---------------------------------------------------------------------------
// Request and answer shapes (`Schedule`, #210)
// ---------------------------------------------------------------------------

/// A cron expression and its IANA zone; both required when a `schedule` object is sent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
pub struct ScheduleRequest {
    pub cron: String,
    pub timezone: String,
}

/// Who is told when a run of this pipeline ends (`spec.notify`, operator#582 / nova#815).
/// Each list names at most five destinations owned by the pipeline's `ownerGroup`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineNotify {
    /// Destinations told when the run fails, after the last retry.
    #[serde(default)]
    pub on_failure: Vec<String>,
    /// Destinations told when the run succeeds.
    #[serde(default)]
    pub on_success: Vec<String>,
    /// Destinations told on the first success after a failure.
    #[serde(default)]
    pub on_recovered: Vec<String>,
}

impl PipelineNotify {
    /// Nothing is listed, so the resource carries no `spec.notify` at all.
    pub fn is_empty(&self) -> bool {
        self.on_failure.is_empty() && self.on_success.is_empty() && self.on_recovered.is_empty()
    }

    /// The operator's shape rules (`crd.rs`, `PipelineSpec::validate`), in its order and its
    /// sentences: at most five names per list, each 1 to 64 characters without outer
    /// whitespace, and no name twice in one list.
    fn check_shape(&self) -> Result<(), String> {
        for (list, names) in [
            ("onFailure", &self.on_failure),
            ("onSuccess", &self.on_success),
            ("onRecovered", &self.on_recovered),
        ] {
            if names.len() > 5 {
                return Err(format!("notify.{list} names at most 5 destinations"));
            }
            let mut seen = BTreeSet::new();
            for name in names {
                if name.trim().is_empty() || name.chars().count() > 64 {
                    return Err(format!(
                        "notify.{list} holds `{name}`, which is not a destination name (1 to 64 characters)"
                    ));
                }
                if name != name.trim() {
                    return Err(format!(
                        "notify.{list} holds `{name}`, which has leading or trailing whitespace"
                    ));
                }
                if !seen.insert(name.as_str()) {
                    return Err(format!("notify.{list} names `{name}` twice"));
                }
            }
        }
        Ok(())
    }
}

/// The advanced options of a schedule (#223), all optional: an omitted field keeps today's
/// default (one retry, five minutes between retries, a two-hour time limit, one run at a time).
/// `Replace` is deliberately not offered — the loader treats it as `Forbid`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineOptions {
    /// Extra attempts after a failure, 0 to 5.
    #[serde(default)]
    pub retries: Option<i64>,
    /// The wait between attempts, 1 to 60 minutes.
    #[serde(default)]
    pub backoff_minutes: Option<i64>,
    /// The longest a whole run may take, 1 to 720 minutes.
    #[serde(default)]
    pub timeout_minutes: Option<i64>,
    /// `Forbid` (one run at a time, the default) or `Allow` (runs side by side).
    #[serde(default)]
    pub concurrency: Option<String>,
}

/// The options as the resource carries them, defaults filled in — what the edit dialog prefills.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineOptionsView {
    pub retries: i64,
    pub backoff_minutes: i64,
    pub timeout_minutes: i64,
    pub concurrency: String,
}

impl PipelineOptions {
    /// The options with their defaults, checked. `Err` is the sentence the caller sees; the
    /// order is the order of the fields (retries, the wait, the time limit, the run mode).
    fn resolve(&self) -> Result<PipelineOptionsView, String> {
        let retries = self.retries.unwrap_or(DEFAULT_RETRIES);
        if !(0..=5).contains(&retries) {
            return Err(RETRIES_RANGE.into());
        }
        let backoff_minutes = self.backoff_minutes.unwrap_or(DEFAULT_BACKOFF_MINUTES);
        if !(1..=60).contains(&backoff_minutes) {
            return Err(BACKOFF_RANGE.into());
        }
        let timeout_minutes = self.timeout_minutes.unwrap_or(DEFAULT_TIMEOUT_MINUTES);
        if !(1..=720).contains(&timeout_minutes) {
            return Err(TIMEOUT_RANGE.into());
        }
        let concurrency = self
            .concurrency
            .as_deref()
            .map(str::trim)
            .unwrap_or(DEFAULT_CONCURRENCY);
        if concurrency != "Allow" && concurrency != "Forbid" {
            return Err(CONCURRENCY_RULE.into());
        }
        Ok(PipelineOptionsView {
            retries,
            backoff_minutes,
            timeout_minutes,
            concurrency: concurrency.to_string(),
        })
    }
}

/// The published notebook version a schedule pins (#237): the `.ipynb` file name, the immutable
/// object version a publish answered, and the parameters the run passes to papermill.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NotebookRequest {
    /// `<group>/<name>` is the object key; the group is the pipeline's `ownerGroup`, so a
    /// notebook of another group can never be named.
    pub name: String,
    /// The S3 object version, never a branch or a tag: exactly the bytes a person published.
    pub object_version: String,
    /// Parameter values by name; at most 32, names `[A-Za-z_][A-Za-z0-9_]{0,63}`, values up to
    /// 256 characters. Written as the task's `config.params` JSON object string.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    /// The `Connection` whose secret fields the task pod receives as `CONN_<NAME>_<KEY>`
    /// (the CRD's `tasks[].connection`). Omitted: the pod gets no connection variable. Nova
    /// checks the shape only; whether the connection exists, is Ready, is of this group and is
    /// in its `usableBy` is the loader's job, and its sentence is the one the task log shows.
    #[serde(default)]
    pub connection: Option<String>,
}

/// A `dbt` task of a task list (`#112`): a project of the tool image, one selector or a chain
/// of batches, the threads and the `--vars` text. The rules are `demos/dags/stardelt_pipelines/
/// plan.py`'s, so Nova refuses what the loader would refuse.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DbtRequest {
    /// A directory name under `/opt/dbt/projects` in the tool image.
    pub project: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub select: Option<String>,
    /// 1 to 8 selectors, run in order (`config.batches` as the JSON array text).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batches: Option<Vec<String>>,
    /// 1 to 16; omitted is 4.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threads: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vars: Option<BTreeMap<String, String>>,
}

/// One SQL assertion of a `quality` task (`#114`): a name the task log shows, the statement that
/// must return no rows, and how a non-empty answer is treated.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QualityCheckRequest {
    /// `[A-Za-z0-9][A-Za-z0-9_-]{0,62}` and unique within the task.
    pub name: String,
    /// The assertion: it passes when the query returns no rows.
    pub sql: String,
    /// `error` fails the run, `warn` only logs it; omitted is `error`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
}

/// A `quality` task of a task list (`#114`): SQL assertions the loader runs after the task's
/// predecessors. The rules are `demos/dags/stardelt_pipelines/plan.py`'s, so Nova refuses what the
/// loader would refuse.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QualityRequest {
    /// 1 to 50 SQL assertions; a check passes when its query returns no rows.
    pub checks: Vec<QualityCheckRequest>,
}

/// A `spark-pipeline` task of a task list (#80): the declared graph `spark-pipelines` runs on
/// the owner group's `SparkCluster`. `project` is a directory of the bundle.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SparkPipelineRequest {
    /// The bundle directory that holds `spark-pipeline.yml`.
    pub project: String,
    /// `incremental` (the default) or `full`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh: Option<String>,
    /// Datasets to rebuild from scratch; 1 to 32 names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_refresh: Option<Vec<String>>,
    /// The graph's files, relative path to text; at most 64 files of 64 KiB each, the whole
    /// graph at most 256 KiB. `spark-pipeline.yml` must be one of them.
    #[serde(default)]
    pub files: BTreeMap<String, String>,
}

/// How a pipeline's writes reach the tables people read: straight to `main`, or through an
/// audit branch that a `quality` task must pass first (`spec.publish`, #114).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Publish {
    #[default]
    Direct,
    Wap,
}

/// One task of a task-list body (`#905`): the name the engine uses, exactly one of a statement, a
/// notebook, a dbt run, a quality run and a spark-pipeline graph, and the tasks that must succeed
/// first.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineTaskRequest {
    /// Unique within the pipeline: `[a-z0-9][a-z0-9_-]{0,62}` (the CRD also accepts capitals).
    pub name: String,
    /// The statement to run; exactly one of this, `notebook`, `dbt`, `quality` and
    /// `sparkPipeline` is given.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sql: String,
    /// A published notebook version to run instead (#237).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notebook: Option<NotebookRequest>,
    /// A `dbt` run instead of a statement or a notebook (`#112`): exactly one of the five.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dbt: Option<DbtRequest>,
    /// SQL assertions instead of a statement, a notebook or a dbt run (`#114`): exactly one of
    /// the five.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<QualityRequest>,
    /// A declared Spark graph instead of a statement, a notebook, a dbt run or SQL checks
    /// (#80): exactly one of the five.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spark_pipeline: Option<SparkPipelineRequest>,
    /// The tables a `spark-pipeline` or `notebook` task writes, as `catalog.schema.table`; at most 16.
    /// `spec.publish: wap` audits exactly these tables (#114).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub writes: Vec<String>,
    /// Names of the tasks that must succeed first; at most 8, each a task of this list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
}

/// `POST /api/pipelines`: schedule a statement for one of the caller's own groups.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreatePipelineRequest {
    pub name: String,
    pub owner_group: String,
    /// One line saying what the pipeline does (`spec.description`); at most 200 characters.
    /// Omitted or blank writes none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The statement to run; exactly one of this and `notebook` is given.
    #[serde(default)]
    pub sql: String,
    /// A published notebook version to run instead of a statement (#237).
    #[serde(default)]
    pub notebook: Option<NotebookRequest>,
    /// A whole task list instead of one `run` task (`#905`): 1 to 30 tasks, each a statement, a
    /// pinned notebook, a dbt run or SQL checks, with dependencies. Exclusive with `sql` and
    /// `notebook`.
    #[serde(default)]
    pub tasks: Option<Vec<PipelineTaskRequest>>,
    #[serde(default)]
    pub schedule: Option<ScheduleRequest>,
    /// Values for `${params.<name>}` in the statement; at most 16. A notebook's values are in
    /// `notebook.params`, not here.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    /// Retries, the wait between them, the time limit and the run mode; omitted fields keep the
    /// defaults.
    #[serde(default)]
    pub options: Option<PipelineOptions>,
    /// Which bundle, target and commit this pipeline is deployed from (`stardelt bundle deploy`).
    #[serde(default)]
    pub provenance: Option<Provenance>,
    /// Who is told when a run ends (`spec.notify`). Omitted writes none; every name must be a
    /// destination of `ownerGroup`.
    #[serde(default)]
    pub notify: Option<PipelineNotify>,
    /// Groups that may read the pipeline without being members (`spec.viewers`). At most 16,
    /// each a group name; omitted writes none. A bundle's pipeline file carries it (#79).
    #[serde(default)]
    pub viewers: Option<Vec<String>>,
    /// How the writes reach the tables people read (`spec.publish`, #114). Omitted on a create
    /// means `direct`; on an update it keeps what the pipeline says, and `direct` removes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish: Option<Publish>,
}

/// `PUT /api/pipelines/{namespace}/{name}`: change the statement, the schedule, the parameters
/// and the options — or, for a notebook pipeline, the notebook, its version and its parameters.
/// Every other field of the resource stays as it is, and what the pipeline runs never changes
/// kind.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdatePipelineRequest {
    /// The statement of a statement pipeline; a notebook pipeline refuses it.
    #[serde(default)]
    pub sql: String,
    /// One line saying what the pipeline does (`spec.description`); at most 200 characters.
    /// Omitted or blank removes the line, as the rest of the body describes the whole
    /// editable state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The notebook, its version and its parameters for a notebook pipeline; a statement
    /// pipeline refuses it.
    #[serde(default)]
    pub notebook: Option<NotebookRequest>,
    /// A whole task list instead of one `run` task (`#905`): 1 to 30 tasks, each a statement, a
    /// pinned notebook, a dbt run or SQL checks, with dependencies. Exclusive with `sql` and
    /// `notebook`.
    #[serde(default)]
    pub tasks: Option<Vec<PipelineTaskRequest>>,
    /// Omitted means no schedule: the pipeline then runs on demand only.
    #[serde(default)]
    pub schedule: Option<ScheduleRequest>,
    /// The whole parameter table; omitted means none.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    /// The whole option set; omitted means the defaults.
    #[serde(default)]
    pub options: Option<PipelineOptions>,
    /// The `resourceVersion` the caller read (`pipelines_spec`). When it is given and is not the
    /// stored one, somebody changed the pipeline in between: 409 `the pipeline changed while you
    /// edited it: reload`, and nothing is written. Omitted keeps the old behaviour (the update
    /// then only fails if the resource changes while the update itself runs).
    #[serde(default)]
    pub resource_version: Option<String>,
    /// Which bundle, target and commit this update deploys. Omitted leaves what the pipeline
    /// already says untouched; given, it replaces it.
    #[serde(default)]
    pub provenance: Option<Provenance>,
    /// Who is told when a run ends (`spec.notify`). Omitted keeps the stored `spec.notify`;
    /// given with all three lists empty, it removes it — a form save must not wipe a
    /// notification a bundle deployed until the form edits it.
    #[serde(default)]
    pub notify: Option<PipelineNotify>,
    /// Omitted keeps the stored viewers; an empty list removes them.
    #[serde(default)]
    pub viewers: Option<Vec<String>>,
    /// How the writes reach the tables people read (`spec.publish`, #114). Omitted on a create
    /// means `direct`; on an update it keeps what the pipeline says, and `direct` removes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish: Option<Publish>,
}

/// Where a pipeline came from when `stardelt bundle deploy` published it. It names things and
/// decides nothing: it is shown on the pipeline page and lets a later deploy find what a bundle
/// owns. Written as the labels `stardelt.io/bundle` and `stardelt.io/target` and the annotation
/// `stardelt.io/revision`, plus who deployed and when.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Provenance {
    /// The bundle's name: 3 to 40 lower-case letters, digits and dashes.
    pub bundle: String,
    /// The target of the bundle: 1 to 32 lower-case letters, digits and dashes.
    pub target: String,
    /// The commit the bundle was read at: 7 to 64 hex digits, with `-dirty` appended when the
    /// working tree had uncommitted changes. Omitted when the bundle is not in a Git checkout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// The bundle's repository as an `https://host/path` address, so a later stage can link the
    /// pull request that promotes a commit. Omitted when the bundle's checkout names none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
}

/// What `view_of` shows of a [`Provenance`], with who deployed and when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentView {
    pub bundle: String,
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    /// The repository the bundle was read from: `https://host/path`, for the compare link.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployed_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployed_at: Option<String>,
    /// `true` when what the deploy decides differs from what it wrote: a pipeline's run,
    /// schedule, parameters and options, or a stream's source, sink, engine and restart policy.
    /// Someone used the form, the YAML editor or `kubectl` since. Left out when they match, and
    /// for a resource stamped before the digest existed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified: Option<bool>,
}

/// `POST /api/pipelines/check`: what the engine says about a statement, without running it.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct CheckStatementRequest {
    pub sql: String,
    /// The parameters the statement's `${params.<name>}` placeholders are filled with.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
}

/// The engine's verdict. `checked: false` means this kind of statement cannot be checked here;
/// `ok` is absent then.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct CheckStatementResponse {
    pub checked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    /// The engine's sentence when `ok` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// `POST /api/pipelines/preview-schedule`: the next fire times of a cron in its zone.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct PreviewScheduleRequest {
    pub cron: String,
    pub timezone: String,
    /// How many runs to list, 1 to 10; 5 when omitted.
    #[serde(default)]
    pub count: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct PreviewScheduleResponse {
    /// The next fire times in the schedule's zone, RFC 3339 with that offset.
    pub runs: Vec<String>,
}

/// `POST /api/pipelines/validate`: what a create (or an apply) of this document would do, without doing it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ValidatePipelineResponse {
    /// True when the document would be accepted.
    pub ok: bool,
    /// `create` when the name is free, `update` when it names a pipeline Nova wrote and the caller
    /// may change; absent when `ok` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    /// Why not: one sentence per problem (today the first rule that refuses).
    pub errors: Vec<ValidationIssue>,
    /// What an `update` would change, from `statement` (or `notebook`), `schedule`,
    /// `description`, `params` and `options`; empty for a `create`, for a refusal and for an
    /// update that changes nothing.
    pub changes: Vec<String>,
    /// The next five fire times (RFC 3339, in the schedule's zone) when the document is accepted
    /// and has a schedule.
    pub next_runs: Vec<String>,
    /// `true` for an `update` of a pipeline that was changed after its last deploy
    /// (`deployment.modified` of the pipeline view): applying this document overwrites that
    /// change. Absent otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct ValidationIssue {
    pub message: String,
}

/// `GET /api/pipelines/{namespace}/{name}/spec`: the editable document of a pipeline, in the shape
/// of the body of `pipelines_create`, so it can be edited and sent to `pipelines_validate` as it is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineDocument {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_group: Option<String>,
    /// One line saying what the pipeline does (`spec.description`); left out when the
    /// resource carries none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The statement of a statement pipeline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    /// The pinned notebook of a notebook pipeline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notebook: Option<NotebookRequest>,
    /// The tasks of a task-list pipeline (`sql`, `notebook` and `dbt` tasks), in the shape
    /// `pipelines_validate` accepts; left out for a single-task pipeline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tasks: Option<Vec<PipelineTaskRequest>>,
    /// The cron and its zone; a resource without a zone is `UTC`, as the loader reads it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleRequest>,
    /// The values of `${params.<name>}`; left out when there are none.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, String>,
    /// Retries, the wait, the time limit and the run mode, defaults filled in.
    pub options: PipelineOptionsView,
    /// Who is told when a run ends; left out when the resource has none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notify: Option<PipelineNotify>,
    /// Groups that may read the pipeline without being members; left out when there are none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub viewers: Vec<String>,
    /// How the writes reach the tables people read (`spec.publish`); left out when the pipeline
    /// writes straight to `main`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish: Option<Publish>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineSpecResponse {
    pub document: PipelineDocument,
    /// The resource's `resourceVersion`, for a later edit to detect a concurrent change.
    pub resource_version: Option<String>,
    /// `nova` when Nova wrote the pipeline, `other` for one made with kubectl or by GitOps.
    pub managed_by: String,
    /// True when Nova may change it: Nova wrote it and it has the shape the form edits (one
    /// `run` task, a statement or a notebook) or a task list (every task `sql`, `notebook` or
    /// `dbt`), which an update replaces whole. A pipeline that is not editable is still shown.
    pub editable: bool,
}

/// One of the caller's groups and its pipeline principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct PrincipalView {
    pub group: String,
    /// `pipeline-<group>` when such a `ServicePrincipal` of this group exists.
    pub principal: Option<String>,
    /// The principal's `status.ready`.
    pub ready: bool,
    /// `null` when ready; otherwise the `Ready` condition's message, or `no status yet`, or the
    /// `make sync-principal` command for a group without a principal.
    pub message: Option<String>,
}

/// `GET /api/pipelines/principals`: every group of the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct PrincipalsResponse {
    pub groups: Vec<PrincipalView>,
}

/// One `ServicePrincipal` the caller may see (#479).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServicePrincipalView {
    pub name: String,
    pub owner_group: String,
    pub purpose: String,
    pub audiences: Vec<String>,
    pub ready: bool,
    /// `null` when ready; otherwise the same sentence the Schedule dialog would refuse with.
    pub message: Option<String>,
    /// `status.workloadIdentity`: the catalog principal this principal's workloads become,
    /// `kubernetes~<ServiceAccount uid>` (#492, #650). `null` until the operator has written it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workload_identity: Option<String>,
    /// `status.clientId`: the identity provider's client (`sp-<name>`); `null` until written.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

/// `GET /api/service-principals`: the principals the caller may see.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct ServicePrincipalsResponse {
    pub principals: Vec<ServicePrincipalView>,
}

// ---------------------------------------------------------------------------
// Views (serde camelCase; no raw Airflow field names and no `sources`)
// ---------------------------------------------------------------------------

/// One task as the resource declares it. A `sql` task's statement and a `notebook` task's file
/// name, version and parameters are carried only by the detail route (Schedule's edit dialog
/// prefills from them).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskSpecView {
    pub name: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,
    /// The `.ipynb` file name of a `kind: "notebook"` task (#237).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notebook: Option<String>,
    /// The immutable object version a notebook task pins.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_version: Option<String>,
    /// The parameters a notebook task passes, parsed back from `config.params`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notebook_params: Option<BTreeMap<String, String>>,
    /// The `Connection` a `kind: "notebook"` task names (`tasks[].connection`), detail route
    /// only. Shape-checked by Nova; the loader decides whether the task may use it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,
    /// The project of a `kind: "dbt"` task (`config.project`), detail route only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dbt_project: Option<String>,
    /// The selector or the batched selectors, joined with `, `, detail route only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dbt_select: Option<String>,
    /// The bundle directory of a `kind: "spark-pipeline"` task, detail route only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// `incremental` or `full` of a `kind: "spark-pipeline"` task, detail route only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh: Option<String>,
    /// The datasets a full refresh rebuilds, detail route only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_refresh: Option<Vec<String>>,
    /// `tasks[].writes` (`catalog.schema.table`), detail route only (#114): the tables a
    /// `wap` run audits.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub writes: Vec<String>,
    /// The task names this task waits for (`tasks[].dependsOn`).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
}

/// One pipeline in the list: the resource plus what Airflow says about it.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineView {
    pub name: String,
    pub namespace: String,
    /// `spec.ownerGroup`; `None` means administrators only.
    pub owner_group: Option<String>,
    /// `spec.description`: one line about what the pipeline does; absent for most.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Whether the caller may run it and stop its runs.
    pub can_run: bool,
    /// Whether the caller may also pause, resume, change or delete it — the owner group and
    /// administrators only (epic#895 decision 12).
    pub can_edit: bool,
    /// The cron schedule, with the time zone in parentheses when there is one.
    pub schedule: Option<String>,
    pub paused: bool,
    /// `false` while Airflow has no DAG for the resource yet; the loader needs about 25 seconds.
    pub loaded: bool,
    pub next_run: Option<String>,
    /// `loading`, `paused`, `running`, `failed`, `succeeded`, `idle`, or `not-loaded` after three
    /// minutes without a DAG.
    pub state: String,
    pub last_run: Option<RunView>,
    pub tasks: Vec<TaskSpecView>,
    pub run_as: Option<String>,
    /// `spec.concurrency` as the resource carries it (`Allow`, `Forbid`, `Replace`).
    pub concurrency: String,
    /// `spec.publish`: `direct` (the default) or `wap` — a `wap` run writes an audit
    /// branch, runs its checks, and only a passing run publishes (#114).
    pub publish: String,
    /// `metadata.creationTimestamp` as the cluster wrote it (RFC 3339); `None` for a malformed
    /// resource.
    pub created_at: Option<String>,
    /// The bundle, target and commit a deploy stamped on the pipeline; absent for one that was
    /// made in the interface or with `stardelt apply`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment: Option<DeploymentView>,
}

/// One DagRun. `type` is Airflow's `run_type` under the name the UI uses.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunView {
    pub run_id: String,
    pub state: String,
    #[serde(rename = "type")]
    pub run_type: String,
    pub logical_date: Option<String>,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub duration_seconds: Option<f64>,
    pub triggered_by: Option<String>,
    pub note: Option<String>,
    /// The bundle the run's pipeline was deployed from, when it came from one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundle: Option<String>,
    /// The stage (target) of that deployment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// The Git revision that deployment named; absent for a dirty tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
}

/// The body of `POST …/runs`: a run date and parameter values for this run only (nova#817).
#[derive(Debug, Default, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunStart {
    pub run_date: Option<String>,
    pub params: Option<BTreeMap<String, String>>,
}

/// One task instance of a run.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TaskView {
    pub task_id: String,
    pub state: String,
    pub try_number: u32,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub duration_seconds: Option<f64>,
    pub operator: Option<String>,
}

/// One log message. `sources` is deliberately absent: it names internal object-store paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct LogLine {
    pub timestamp: Option<String>,
    pub level: Option<String>,
    pub text: String,
}

/// One page of a task's log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LogView {
    pub lines: Vec<LogLine>,
    pub next_token: Option<String>,
    pub truncated: bool,
}

/// `GET …/runs/{run_id}/checks`: one row per declared check of the run's `quality` tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChecksResponse {
    pub checks: Vec<CheckView>,
    /// The audit branch this run left for inspection: `Some` when the pipeline publishes with
    /// `wap` and the run has not published it (`wap-prepare` succeeded, `wap-publish` did not).
    /// A passing run publishes and drops its branch, so it answers `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_branch: Option<String>,
    /// The tables the run wrote (`spec.tasks[].writes`), sorted; empty for a `direct` pipeline.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audited_tables: Vec<String>,
}

/// One check of one `quality` task. `rows` and `passed` are `None` until the task's log
/// carries the line (the task has not run, or has not reached the check yet).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CheckView {
    /// The `quality` task's name.
    pub task: String,
    /// The check's name, as declared in `spec.tasks[].checks[].name`.
    pub name: String,
    /// `error` or `warn`; `warn` never fails the run.
    pub severity: String,
    /// The rows the assertion returned; `0` is a pass.
    pub rows: Option<i64>,
    /// `Some(rows == 0)` once the log carries the line, `None` before that.
    pub passed: Option<bool>,
}

/// The retry policy of a pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct RetriesView {
    pub attempts: i64,
    pub backoff: Option<String>,
}

/// `GET /api/pipelines`.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct PipelineListResponse {
    pub pipelines: Vec<PipelineView>,
}

/// One run in the cross-pipeline list (`GET /api/runs`).
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunsListRow {
    pub namespace: String,
    pub name: String,
    /// `spec.ownerGroup`; `None` means administrators only.
    pub owner_group: Option<String>,
    /// `sql`, `notebook`, or `pipeline` for a multi-step pipeline (`run_task_kind`).
    pub kind: String,
    #[serde(flatten)]
    pub run: RunView,
}

/// `GET /api/runs`.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RunsListResponse {
    pub runs: Vec<RunsListRow>,
    /// Every owner group of a pipeline the caller can see, sorted; the runs filters do not
    /// narrow it. Administrators (`ownerGroup: null`) are not a group.
    pub groups: Vec<String>,
}

/// One pipeline that runs a published notebook (`GET /api/notebooks/{group}/{name}/usage`,
/// stardelt-nova#517): a Schedule is a one-task `Pipeline` (#516), so this is its resource.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NotebookScheduleRow {
    pub namespace: String,
    pub name: String,
    pub owner_group: String,
    /// The pinned `config.objectVersion`; a schedule runs exactly these bytes.
    pub object_version: String,
    /// `spec.schedule.cron`; `None` for a pipeline that runs on demand.
    pub cron: Option<String>,
    /// `spec.schedule.timezone`; `None` for a pipeline that runs on demand.
    pub timezone: Option<String>,
    /// `spec.runAs`.
    pub run_as: Option<String>,
}

/// `GET /api/notebooks/{group}/{name}/usage`.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct NotebookUsageResponse {
    /// The visible pipelines that run this notebook, sorted by name.
    pub schedules: Vec<NotebookScheduleRow>,
}

/// `GET /api/pipelines/{namespace}/{name}`: the view, the latest runs and the resource's
/// parameters, retry policy and options.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PipelineDetail {
    #[serde(flatten)]
    pub pipeline: PipelineView,
    pub runs: Vec<RunView>,
    pub params: BTreeMap<String, String>,
    pub retries: Option<RetriesView>,
    /// Retries, the wait between them, the time limit and the run mode as the resource carries
    /// them, with the defaults filled in when a field is absent.
    pub options: PipelineOptionsView,
    /// The public Airflow DAG URL, `None` when `NOVA_BASE_DOMAIN` is unset; the route is already visibility-gated, so no second permission check.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub airflow_url: Option<String>,
    /// Who is told when a run ends; left out when the resource has none.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notify: Option<PipelineNotify>,
}

/// `GET …/runs/{run_id}`: one run and its task instances.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
pub struct RunResponse {
    pub run: RunView,
    pub tasks: Vec<TaskView>,
}

/// One statement a run's task sent to the engine (nova#788 S5).
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunQueryView {
    /// The engine's query id.
    pub id: String,
    pub state: String,
    /// The task id from the `task=` client tag, when the loader sent one.
    pub task_id: Option<String>,
    /// Started at, epoch milliseconds.
    pub started_at: Option<i64>,
    pub wall_ms: Option<i64>,
    pub execution_ms: Option<i64>,
    pub input_bytes: Option<i64>,
    pub input_rows: Option<i64>,
    /// The statement; string literals are replaced by `?` under NOVA_QUERY_LOG_REDACT_LITERALS=1.
    pub sql: String,
}

/// `GET …/runs/{run_id}/queries` — the run's statements, slowest first.
#[derive(Debug, Clone, PartialEq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RunQueriesResponse {
    pub queries: Vec<RunQueryView>,
}

/// The note Nova writes when it starts a run. A deployed pipeline names its bundle, target and
/// revision in a suffix the run view reads back (#105); a hand-made one keeps the plain sentence.
fn run_note(who: &Identity, deployment: Option<&DeploymentView>) -> String {
    let mut note = format!("Started from Nova by {}", who.display_name);
    if let Some(deployment) = deployment {
        note.push_str(&format!(
            " [stardelt bundle={} target={}",
            deployment.bundle, deployment.target
        ));
        if let Some(revision) = &deployment.revision {
            note.push_str(&format!(" revision={revision}"));
        }
        note.push(']');
    }
    note
}

/// `run_note`'s suffix, read back: `(bundle, target, revision)`, `None` for a note without one.
fn run_provenance(note: &str) -> Option<(String, String, Option<String>)> {
    let start = note.rfind("[stardelt ")?;
    let inner = note[start + "[stardelt ".len()..].strip_suffix(']')?;
    let (mut bundle, mut target, mut revision) = (None, None, None);
    for part in inner.split_whitespace() {
        let (key, value) = part.split_once('=')?;
        match key {
            "bundle" => bundle = Some(value.to_string()),
            "target" => target = Some(value.to_string()),
            "revision" => revision = Some(value.to_string()),
            _ => return None,
        }
    }
    Some((bundle?, target?, revision))
}

fn run_view(run: &DagRun) -> RunView {
    let provenance = run_provenance(run.note.as_deref().unwrap_or_default());
    RunView {
        run_id: run.dag_run_id.clone(),
        state: run.state.clone().unwrap_or_default(),
        run_type: run.run_type.clone().unwrap_or_default(),
        logical_date: run.logical_date.clone(),
        started_at: run.start_date.clone(),
        ended_at: run.end_date.clone(),
        duration_seconds: duration_seconds(run.start_date.as_deref(), run.end_date.as_deref()),
        triggered_by: run.triggered_by.clone(),
        note: run.note.clone(),
        bundle: provenance.as_ref().map(|(bundle, _, _)| bundle.clone()),
        target: provenance.as_ref().map(|(_, target, _)| target.clone()),
        revision: provenance.and_then(|(_, _, revision)| revision),
    }
}

fn task_view(task: &TaskInstance) -> TaskView {
    TaskView {
        task_id: task.task_id.clone(),
        state: task.state.clone().unwrap_or_default(),
        try_number: task.try_number.unwrap_or(0),
        started_at: task.start_date.clone(),
        ended_at: task.end_date.clone(),
        duration_seconds: task.duration,
        operator: task.operator.clone(),
    }
}

fn view_of(
    namespace: &str,
    name: &str,
    resource: &Value,
    dag: Option<&DagSummary>,
    last_run: Option<&DagRun>,
    who: &Identity,
) -> PipelineView {
    let spec = spec_of(resource);
    let loaded = dag.is_some();
    let paused = loaded && dag.and_then(|d| d.is_paused).unwrap_or(false);
    let state = pipeline_state(
        paused,
        loaded,
        last_run.and_then(|r| r.state.as_deref()),
        created_age_seconds(resource),
    );
    PipelineView {
        name: name.to_string(),
        namespace: namespace.to_string(),
        owner_group: spec
            .get("ownerGroup")
            .and_then(Value::as_str)
            .map(str::to_string),
        description: spec
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_string),
        can_run: visible(who, spec),
        can_edit: may_act(who, spec),
        schedule: schedule_of(spec),
        paused,
        loaded,
        next_run: dag.and_then(|d| d.next_dagrun_run_after.clone()),
        state: state.to_string(),
        last_run: last_run.map(run_view),
        tasks: task_specs(spec, false),
        run_as: spec
            .get("runAs")
            .and_then(Value::as_str)
            .map(str::to_string),
        concurrency: spec
            .get("concurrency")
            .and_then(Value::as_str)
            .unwrap_or("Forbid")
            .to_string(),
        publish: spec
            .get("publish")
            .and_then(Value::as_str)
            .unwrap_or("direct")
            .to_string(),
        created_at: created_at_of(resource).map(str::to_string),
        deployment: deployment_of(resource),
    }
}

// ---------------------------------------------------------------------------
// Logs: drop the marker, strip ANSI, cap the answer
// ---------------------------------------------------------------------------

/// Remove ANSI escape sequences (`\x1b\[[0-9;]*[A-Za-z]`) from a log event. Multi-byte text
/// survives; only the escape sequences are dropped.
fn strip_ansi(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == 0x1b && bytes.get(at + 1) == Some(&b'[') {
            let mut end = at + 2;
            while end < bytes.len() && (bytes[end].is_ascii_digit() || bytes[end] == b';') {
                end += 1;
            }
            if end < bytes.len() && bytes[end].is_ascii_alphabetic() {
                at = end + 1;
                continue;
            }
        }
        let ch = text[at..].chars().next().expect("a char starts at `at`");
        out.push(ch);
        at += ch.len_utf8();
    }
    out
}

/// The start of the `::group::Log message source details` block: the marker carries `sources`,
/// and Airflow then writes the internal path as its own event.
fn starts_sources_group(event: &LogEvent) -> bool {
    !event.sources.is_empty()
        || event
            .event
            .as_deref()
            .is_some_and(|text| text.trim_start().starts_with("::group::"))
}

/// The `::endgroup::` that closes the source-details block.
fn ends_sources_group(event: &LogEvent) -> bool {
    event
        .event
        .as_deref()
        .is_some_and(|text| text.trim_start().starts_with("::endgroup::"))
}

/// The most log lines one answer carries.
const MAX_LOG_LINES: usize = 2000;
/// The most log text (bytes) one answer carries.
const MAX_LOG_BYTES: usize = 512 * 1024;
/// The most stack frames one failed task shows below its exception.
const MAX_ERROR_FRAMES: usize = 20;

/// The last segment of a frame's filename: `/home/airflow/.local/lib/python3.12/site-packages/…`
/// never reaches the page, `task_runner.py` does.
fn short_path(filename: &str) -> &str {
    filename
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or(filename)
}

fn log_view(page: LogPage) -> LogView {
    let mut lines = Vec::new();
    let mut bytes = 0usize;
    let mut truncated = false;
    // The `::group::` source-details block is three events on live Airflow: the marker (with
    // `sources`), the internal object-store path as its own event, and `::endgroup::`. All three
    // are dropped: those paths are internal and never leave this module.
    let mut in_sources_group = false;
    for event in page.content {
        if starts_sources_group(&event) {
            in_sources_group = true;
            continue;
        }
        if in_sources_group {
            if ends_sources_group(&event) {
                in_sources_group = false;
            }
            continue;
        }
        if ends_sources_group(&event) {
            continue;
        }
        let text = strip_ansi(event.event.as_deref().unwrap_or_default());
        if lines.len() >= MAX_LOG_LINES || bytes + text.len() > MAX_LOG_BYTES {
            truncated = true;
            break;
        }
        bytes += text.len();
        lines.push(LogLine {
            timestamp: event.timestamp,
            level: event.level,
            text,
        });
        // A failed task's reason lives only in `error_detail`; without it the page stops at
        // "Task failed with exception" and says nothing.
        for detail in &event.error_detail {
            let text = format!("{}: {}", detail.exc_type, detail.exc_value);
            if lines.len() >= MAX_LOG_LINES || bytes + text.len() > MAX_LOG_BYTES {
                truncated = true;
                break;
            }
            bytes += text.len();
            lines.push(LogLine {
                timestamp: None,
                level: Some("error".to_string()),
                text,
            });
            for frame in detail.frames.iter().take(MAX_ERROR_FRAMES) {
                let text = format!(
                    "  {}:{} in {}",
                    short_path(&frame.filename),
                    frame.lineno,
                    frame.name
                );
                if lines.len() >= MAX_LOG_LINES || bytes + text.len() > MAX_LOG_BYTES {
                    truncated = true;
                    break;
                }
                bytes += text.len();
                lines.push(LogLine {
                    timestamp: None,
                    level: None,
                    text,
                });
            }
        }
    }
    LogView {
        lines,
        next_token: page.continuation_token,
        truncated,
    }
}

/// The `quality` tasks of a Pipeline and their declared checks: `(task, [(check, severity)])`.
/// A check without `severity` is `error`, the CRD's default.
fn quality_tasks(pipeline: &Value) -> Vec<(String, Vec<(String, String)>)> {
    let tasks = pipeline
        .get("spec")
        .and_then(|spec| spec.get("tasks"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    tasks
        .iter()
        .filter(|task| task.get("kind").and_then(Value::as_str) == Some("quality"))
        .filter_map(|task| {
            let name = task.get("name").and_then(Value::as_str)?.to_string();
            let checks = task
                .get("checks")
                .and_then(Value::as_array)
                .map(|checks| {
                    checks
                        .iter()
                        .filter_map(|check| {
                            let name = check.get("name").and_then(Value::as_str)?.to_string();
                            let severity = check
                                .get("severity")
                                .and_then(Value::as_str)
                                .unwrap_or("error")
                                .to_string();
                            Some((name, severity))
                        })
                        .collect()
                })
                .unwrap_or_default();
            Some((name, checks))
        })
        .collect()
}

/// The audit branch a `publish: wap` run writes: `audit_` + the first 16 hex of
/// `sha256(run_id)`, the twin of `demos/dags/stardelt_pipelines/wap.py::branch_name` (#114).
pub(crate) fn audit_branch(run_id: &str) -> String {
    format!(
        "audit_{}",
        &crate::migrations::hex_lower(&Sha256::digest(run_id.as_bytes()))[..16]
    )
}

/// `spec.tasks[].writes` of a pipeline, deduplicated and sorted; empty for a `direct` pipeline.
fn audited_tables(pipeline: &Value) -> Vec<String> {
    let mut tables: Vec<String> = pipeline
        .get("spec")
        .and_then(|s| s.get("tasks"))
        .and_then(Value::as_array)
        .map(|tasks| {
            tasks
                .iter()
                .filter_map(|task| task.get("writes").and_then(Value::as_array))
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    tables.sort();
    tables.dedup();
    tables
}

/// `check <name>: <error|warn>, <n> row(s)` -> `(name, severity, n)`.
/// The marker is searched anywhere in the line: Airflow prefixes its own event text.
fn check_result(text: &str) -> Option<(String, String, i64)> {
    let start = text.find("check ")?;
    let rest = &text[start + "check ".len()..];
    let (name, rest) = rest.split_once(": ")?;
    let (severity, rest) = rest.split_once(", ")?;
    if severity != "error" && severity != "warn" {
        return None;
    }
    let rows = rest
        .strip_suffix(" row(s)")
        .or_else(|| rest.strip_suffix(" row"))?;
    Some((name.to_string(), severity.to_string(), rows.parse().ok()?))
}

// ---------------------------------------------------------------------------
// Errors and the ticketing facts
// ---------------------------------------------------------------------------

pub(crate) fn no_cluster() -> Fail {
    Fail::new(StatusCode::SERVICE_UNAVAILABLE, "no cluster connection")
}

fn scheduler_not_configured() -> Fail {
    Fail::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "the scheduler is not configured",
    )
}

fn no_such_pipeline() -> Fail {
    Fail::new(StatusCode::NOT_FOUND, "no such pipeline")
}

fn no_such_run() -> Fail {
    Fail::new(StatusCode::NOT_FOUND, "no such run")
}

fn no_such_task() -> Fail {
    Fail::new(StatusCode::NOT_FOUND, "no such task")
}

/// Airflow has no DAG for the resource yet (or the loader refused it). Acting on a pipeline that
/// does not exist in Airflow is a 409 with a sentence, not a 404: the resource does exist.
/// The sentence names the wait because a deploy that just ran is the common case
/// (`stardelt/stardelt#1571`).
fn pipeline_not_loaded() -> Fail {
    Fail::new(
        StatusCode::CONFLICT,
        "the scheduler has not loaded this pipeline yet: it loads about a minute after a deploy, so try again",
    )
}

/// Triggering a paused pipeline would succeed in Airflow and the run would sit `queued` until
/// someone resumes — it looks like it worked and nothing happens (#255). Nova refuses instead.
fn pipeline_paused() -> Fail {
    Fail::new(
        StatusCode::CONFLICT,
        "the pipeline is paused: resume it first",
    )
}

/// `Forbid` is not enforced by Airflow at trigger time: a second run is created and queued.
fn run_already_active() -> Fail {
    Fail::new(
        StatusCode::CONFLICT,
        "a run is already active: wait for it or stop it",
    )
}

/// Marking a finished run failed would rewrite history; only `queued` and `running` can stop.
fn run_not_active() -> Fail {
    Fail::new(StatusCode::CONFLICT, "the run is not active")
}

fn too_many_runs() -> Fail {
    Fail::new(
        StatusCode::TOO_MANY_REQUESTS,
        "too many runs started; wait a minute",
    )
}

/// A `Pipeline` of that name already exists; a create never overwrites one.
fn pipeline_exists() -> Fail {
    Fail::new(
        StatusCode::CONFLICT,
        "a pipeline with this name already exists",
    )
}

/// More than ten creates in an hour: the answer is 429, not a silently growing namespace.
fn too_many_creates() -> Fail {
    Fail::new(
        StatusCode::TOO_MANY_REQUESTS,
        "too many pipelines created this hour; wait before creating another",
    )
}

/// A pipeline Nova did not write is somebody's manifest: change it there, not through the form.
fn managed_outside() -> Fail {
    Fail::new(
        StatusCode::CONFLICT,
        "this pipeline is managed outside Nova (kubectl or Git); change it there",
    )
}

/// The resource changed between the read and the write: reload instead of overwriting.
fn pipeline_changed() -> Fail {
    Fail::new(
        StatusCode::CONFLICT,
        "the pipeline changed while you edited it: reload",
    )
}

/// The form edits exactly one `sql` task named `run`; anything else is not its shape.
fn shape_not_editable() -> Fail {
    Fail::new(
        StatusCode::CONFLICT,
        "this pipeline has a shape the form cannot edit",
    )
}

/// An Airflow that refused Nova's service user is a configuration problem, not an
/// unreachable scheduler: say which one it is (stardelt-platform#1047). The detail goes to
/// the log; the sentence stays one line and carries no credential.
fn scheduler_fail(e: AirflowError) -> Fail {
    match e {
        AirflowError::NotConfigured => scheduler_not_configured(),
        AirflowError::Refused(m) => {
            tracing::warn!(error = %m, "Airflow refused Nova's service user");
            Fail::new(
                StatusCode::BAD_GATEWAY,
                "the scheduler refused Nova's service user",
            )
        }
        other => {
            tracing::warn!(error = %other, "Airflow request failed");
            Fail::new(StatusCode::BAD_GATEWAY, "the scheduler did not answer")
        }
    }
}

/// A lookup that answers nothing when Airflow has no such run.
fn run_lookup_fail(e: AirflowError) -> Fail {
    match e {
        AirflowError::NotFound => no_such_run(),
        other => scheduler_fail(other),
    }
}

/// A lookup that answers nothing when Airflow has no such task (or run).
fn task_lookup_fail(e: AirflowError) -> Fail {
    match e {
        AirflowError::NotFound => no_such_task(),
        other => scheduler_fail(other),
    }
}

/// The status for a read of a resource: the sentence stays generic (a kube error names the API
/// server), the detail goes to the log.
pub(crate) fn resource_fail(e: ResourceError) -> Fail {
    match e {
        ResourceError::Unavailable(message) => {
            tracing::warn!(error = %message, "pipeline resource request failed");
            Fail::new(StatusCode::BAD_GATEWAY, "the cluster did not answer")
        }
        // A read cannot take a name or hit a conflict; both are upstream failures here.
        ResourceError::Exists | ResourceError::Conflict => {
            Fail::new(StatusCode::BAD_GATEWAY, "the cluster did not answer")
        }
    }
}

/// The status for a write: the two facts a read never has a sentence for become the caller's
/// 409, everything else is the generic upstream failure.
fn resource_write_fail(e: ResourceError) -> Fail {
    match e {
        ResourceError::Exists => pipeline_exists(),
        ResourceError::Conflict => pipeline_changed(),
        ResourceError::Unavailable(message) => {
            tracing::warn!(error = %message, "pipeline resource write failed");
            Fail::new(StatusCode::BAD_GATEWAY, "the cluster did not answer")
        }
    }
}

/// The latest runs the detail page and the run routes read. Nova never asks Airflow for an
/// id the browser sent without finding it in this list first.
const LATEST_RUNS: u32 = 20;

/// The latest runs a "run now" checks for activity: enough to catch a `Forbid`/`Replace` DAG
/// whose last run is still queued or running.
const ACTIVE_RUNS: u32 = 5;

/// At most this many runs started per user per minute.
const RUN_STARTS_PER_MINUTE: usize = 10;

/// The in-memory run-start rate limit (`AppState::run_starts`): per signed-in `sub`, the start
/// times inside the last minute. Refusals before the limiter (unloaded, paused) never reach it;
/// only a restart forgets it.
pub type RunStartsLimiter = std::sync::Mutex<std::collections::HashMap<String, VecDeque<Instant>>>;

/// Take one run-start slot for `sub`, or report that the last minute is full.
fn take_run_slot(limiter: &RunStartsLimiter, sub: &str) -> bool {
    let mut map = limiter.lock().expect("run-start limiter poisoned");
    let now = Instant::now();
    let slots = map.entry(sub.to_string()).or_default();
    while slots
        .front()
        .is_some_and(|then| now.duration_since(*then) >= std::time::Duration::from_secs(60))
    {
        slots.pop_front();
    }
    if slots.len() >= RUN_STARTS_PER_MINUTE {
        false
    } else {
        slots.push_back(now);
        true
    }
}

/// The most last-run calls in flight at once.
const LAST_RUN_CONCURRENCY: usize = 8;

/// The most run-list calls in flight at once (`GET /api/runs`).
const RUN_LIST_CONCURRENCY: usize = 8;

/// `GET /api/runs` answers this many runs when the caller gives no `limit`.
const RUNS_DEFAULT_LIMIT: usize = 50;

/// `GET /api/runs` never answers more than this many runs.
const RUNS_MAX_LIMIT: usize = 200;

/// The `kind` values `GET /api/runs` offers; `pipeline` is a multi-step pipeline.
const RUNS_KINDS: [&str; 3] = ["sql", "notebook", "pipeline"];

/// Find the resource, check the namespace and the caller in that order, and only then answer
/// the DAG id. A pipeline the caller cannot see is indistinguishable from an absent one: that
/// stays 404. A pipeline the caller can see but whose `allow` rule refuses answers 403 with the
/// reason, so the page does not say "no such pipeline" about itself.
async fn load_allowed(
    resources: &PipelineResources,
    who: &Identity,
    namespace: &str,
    name: &str,
    allow: fn(&Identity, &Value) -> bool,
) -> Result<(String, Value), Fail> {
    if namespace != crate::cluster::namespace() {
        return Err(no_such_pipeline());
    }
    let Some(resource) = resources.get(name).await.map_err(resource_fail)? else {
        return Err(no_such_pipeline());
    };
    if !allow(who, spec_of(&resource)) {
        if visible(who, spec_of(&resource)) {
            return Err(Fail::new(
                StatusCode::FORBIDDEN,
                "only the owner group may change this pipeline",
            ));
        }
        return Err(no_such_pipeline());
    }
    Ok((dag_id_for(namespace, name), resource))
}

/// A read: who may see the pipeline.
async fn load_visible(
    resources: &PipelineResources,
    who: &Identity,
    namespace: &str,
    name: &str,
) -> Result<(String, Value), Fail> {
    load_allowed(resources, who, namespace, name, visible).await
}

/// A run or a stop: a viewer may run the owner's statement and stop one of its runs.
async fn load_runnable(
    resources: &PipelineResources,
    who: &Identity,
    namespace: &str,
    name: &str,
) -> Result<(String, Value), Fail> {
    load_allowed(resources, who, namespace, name, visible).await
}

/// A change: who may pause, resume, change or delete the pipeline (a viewer may not).
async fn load_actionable(
    resources: &PipelineResources,
    who: &Identity,
    namespace: &str,
    name: &str,
) -> Result<(String, Value), Fail> {
    load_allowed(resources, who, namespace, name, may_act).await
}

fn scheduler(airflow: Option<&AirflowClient>) -> Result<&AirflowClient, Fail> {
    airflow.ok_or_else(scheduler_not_configured)
}

// ---------------------------------------------------------------------------
// The route logic. These take their inputs, so the tests call them directly.
// ---------------------------------------------------------------------------

/// `GET /api/pipelines`: the visible resources, one DAG-list call, and one last-run call per
/// visible loaded pipeline (at most eight at a time). A failing last-run call leaves that
/// pipeline's `lastRun` empty; it does not fail the list.
async fn list_pipelines(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
) -> Result<PipelineListResponse, Fail> {
    let resources = resources.ok_or_else(no_cluster)?;
    let airflow = scheduler(airflow)?;
    let namespace = crate::cluster::namespace();

    let mut resources = resources.list().await.map_err(resource_fail)?;
    resources.retain(|resource| visible(who, spec_of(resource)));
    resources.sort_by(|a, b| pipeline_name(a).cmp(pipeline_name(b)));
    if resources.is_empty() {
        // Nothing visible: do not tell Airflow (or a timing observer) that there is anything.
        return Ok(PipelineListResponse {
            pipelines: Vec::new(),
        });
    }

    let dags: BTreeMap<String, DagSummary> = airflow
        .list_dags("pipeline", true)
        .await
        .map_err(scheduler_fail)?
        .into_iter()
        .map(|dag| (dag.dag_id.clone(), dag))
        .collect();

    let loaded: Vec<(String, String)> = resources
        .iter()
        .map(|resource| pipeline_name(resource).to_string())
        .map(|name| {
            let dag_id = dag_id_for(&namespace, &name);
            (name, dag_id)
        })
        .filter(|(_, dag_id)| dags.contains_key(dag_id))
        .collect();

    let last_runs: Vec<(String, Option<DagRun>)> = stream::iter(loaded)
        .map(|(name, dag_id)| async move {
            let run = match airflow.runs(&dag_id, 1).await {
                Ok(runs) => runs.into_iter().next(),
                Err(e) => {
                    tracing::warn!(dag = %dag_id, error = %e, "could not read a pipeline's last run");
                    None
                }
            };
            (name, run)
        })
        .buffer_unordered(LAST_RUN_CONCURRENCY)
        .collect()
        .await;
    let last_runs: BTreeMap<String, Option<DagRun>> = last_runs.into_iter().collect();

    let pipelines = resources
        .iter()
        .map(|resource| {
            let name = pipeline_name(resource);
            let dag = dags.get(&dag_id_for(&namespace, name));
            let last_run = last_runs.get(name).and_then(Option::as_ref);
            view_of(&namespace, name, resource, dag, last_run, who)
        })
        .collect();
    Ok(PipelineListResponse { pipelines })
}

/// `GET /api/runs`: every run of the caller's visible pipelines, newest first, filtered by
/// `state`, `kind`, `group`, `pipeline` and `notebook`, capped at `limit`. One DAG list, then
/// one run-list call per loaded pipeline (at most eight at a time); a failing call skips that
/// pipeline instead of failing the page. Nothing visible means no call to Airflow at all.
async fn runs_across_pipelines(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    query: &BTreeMap<String, String>,
) -> Result<RunsListResponse, Fail> {
    let filter = RunsFilter::parse(query)?;

    // The notebook page is owner-group scoped: an administrator gets no wider view (#517).
    // Checked before the cluster and the list, like the usage route, so a caller outside the
    // group is refused even when no pipeline of their own is visible.
    if let Some((group, _)) = &filter.notebook
        && !who.groups.iter().any(|g| g == group)
    {
        return Err(crate::notebooks::forbidden_group(group));
    }

    let resources = resources.ok_or_else(no_cluster)?;
    let airflow = scheduler(airflow)?;
    let namespace = crate::cluster::namespace();

    let mut resources = resources.list().await.map_err(resource_fail)?;
    resources.retain(|resource| visible(who, spec_of(resource)));
    resources.sort_by(|a, b| pipeline_name(a).cmp(pipeline_name(b)));
    if resources.is_empty() {
        // Nothing visible: do not tell Airflow (or a timing observer) that there is anything.
        return Ok(RunsListResponse {
            runs: Vec::new(),
            groups: Vec::new(),
        });
    }

    // The groups of the visible pipelines, not of the rows: the runs filters and the cap must
    // not narrow the choice (`?group=` would otherwise collapse the list to one option).
    let groups: Vec<String> = resources
        .iter()
        .filter_map(|resource| spec_of(resource).get("ownerGroup").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<BTreeSet<String>>()
        .into_iter()
        .collect();

    if let Some((group, notebook)) = &filter.notebook {
        resources.retain(|resource| notebook_schedule(resource, group, notebook));
    }

    let dags: BTreeMap<String, DagSummary> = airflow
        .list_dags("pipeline", true)
        .await
        .map_err(scheduler_fail)?
        .into_iter()
        .map(|dag| (dag.dag_id.clone(), dag))
        .collect();

    let loaded: Vec<(String, String)> = resources
        .iter()
        .map(|resource| pipeline_name(resource).to_string())
        .map(|name| {
            let dag_id = dag_id_for(&namespace, &name);
            (name, dag_id)
        })
        .filter(|(_, dag_id)| dags.contains_key(dag_id))
        .collect();
    let by_name: BTreeMap<&str, &Value> = resources
        .iter()
        .map(|resource| (pipeline_name(resource), resource))
        .collect();

    let runs_per_pipeline: Vec<(String, Vec<DagRun>)> = stream::iter(loaded)
        .map(|(name, dag_id)| async move {
            let runs = match airflow.runs(&dag_id, filter.limit as u32).await {
                Ok(runs) => runs,
                Err(e) => {
                    // One DAG that is gone (or failing) must not fail the whole page.
                    tracing::warn!(dag = %dag_id, error = %e, "could not read a pipeline's runs");
                    Vec::new()
                }
            };
            (name, runs)
        })
        .buffer_unordered(RUN_LIST_CONCURRENCY)
        .collect()
        .await;

    let mut rows = Vec::new();
    for (name, runs) in runs_per_pipeline {
        let Some(resource) = by_name.get(name.as_str()) else {
            continue;
        };
        let owner_group = spec_of(resource)
            .get("ownerGroup")
            .and_then(Value::as_str)
            .map(str::to_string);
        let kind = run_task_kind(resource).unwrap_or("pipeline").to_string();
        for run in runs {
            rows.push(RunsListRow {
                namespace: namespace.clone(),
                name: name.clone(),
                owner_group: owner_group.clone(),
                kind: kind.clone(),
                run: run_view(&run),
            });
        }
    }
    Ok(filter.answer(rows, groups))
}

/// The query of `GET /api/runs`, parsed once: the five filters and the cap. Demo mode parses
/// the same query, so a bad one refuses there too.
struct RunsFilter<'a> {
    limit: usize,
    kind: Option<&'a str>,
    state: Option<&'a str>,
    group: Option<&'a str>,
    pipeline: Option<(String, String)>,
    /// The notebook as `<group>/<name>`; narrowed at the source (the rows do not carry it).
    notebook: Option<(String, String)>,
}

impl<'a> RunsFilter<'a> {
    fn parse(query: &'a BTreeMap<String, String>) -> Result<Self, Fail> {
        Ok(Self {
            limit: runs_limit(query)?,
            kind: runs_kind(query)?,
            state: query.get("state").map(String::as_str),
            group: query.get("group").map(String::as_str),
            pipeline: runs_pipeline(query)?,
            notebook: runs_notebook(query)?,
        })
    }

    /// The answer: the filters, newest first, then the cap. A run without a start date goes
    /// last; the name and the run id keep the order stable within the same second. `groups`
    /// is the unfiltered choice of the caller's visible groups.
    fn answer(&self, mut rows: Vec<RunsListRow>, groups: Vec<String>) -> RunsListResponse {
        rows.retain(|row| {
            self.state.is_none_or(|state| row.run.state == state)
                && self.kind.is_none_or(|kind| row.kind == kind)
                && self
                    .group
                    .is_none_or(|group| row.owner_group.as_deref() == Some(group))
                && self.pipeline.as_ref().is_none_or(|(namespace, name)| {
                    row.namespace == *namespace && row.name == *name
                })
        });
        rows.sort_by(|a, b| {
            b.run
                .started_at
                .cmp(&a.run.started_at)
                .then_with(|| a.name.cmp(&b.name))
                .then_with(|| a.run.run_id.cmp(&b.run.run_id))
        });
        rows.truncate(self.limit);
        RunsListResponse { runs: rows, groups }
    }
}

/// The `limit` of `GET /api/runs`: 50 when absent, a positive integer of at most 200.
fn runs_limit(query: &BTreeMap<String, String>) -> Result<usize, Fail> {
    let Some(value) = query.get("limit") else {
        return Ok(RUNS_DEFAULT_LIMIT);
    };
    let limit = value
        .parse::<usize>()
        .ok()
        .filter(|limit| *limit > 0)
        .ok_or_else(|| Fail::new(StatusCode::BAD_REQUEST, "limit must be a positive integer"))?;
    if limit > RUNS_MAX_LIMIT {
        return Err(Fail::new(
            StatusCode::BAD_REQUEST,
            "limit must be at most 200",
        ));
    }
    Ok(limit)
}

/// The `kind` of `GET /api/runs`: one of `sql`, `notebook`, `pipeline` when given.
fn runs_kind(query: &BTreeMap<String, String>) -> Result<Option<&str>, Fail> {
    let Some(kind) = query.get("kind").map(String::as_str) else {
        return Ok(None);
    };
    if RUNS_KINDS.contains(&kind) {
        Ok(Some(kind))
    } else {
        Err(Fail::new(
            StatusCode::BAD_REQUEST,
            "kind must be one of sql, notebook, pipeline",
        ))
    }
}

/// The `pipeline` filter of `GET /api/runs`: `<namespace>/<name>` when given.
fn runs_pipeline(query: &BTreeMap<String, String>) -> Result<Option<(String, String)>, Fail> {
    let Some(value) = query.get("pipeline") else {
        return Ok(None);
    };
    match value.split_once('/') {
        Some((namespace, name))
            if !namespace.is_empty() && !name.is_empty() && !name.contains('/') =>
        {
            Ok(Some((namespace.to_string(), name.to_string())))
        }
        _ => Err(Fail::new(
            StatusCode::BAD_REQUEST,
            "pipeline must be <namespace>/<name>",
        )),
    }
}

/// The `notebook` filter of `GET /api/runs`: `<group>/<name>` when given.
fn runs_notebook(query: &BTreeMap<String, String>) -> Result<Option<(String, String)>, Fail> {
    let Some(value) = query.get("notebook") else {
        return Ok(None);
    };
    match value.split_once('/') {
        Some((group, name)) if !group.is_empty() && !name.is_empty() && !name.contains('/') => {
            Ok(Some((group.to_string(), name.to_string())))
        }
        _ => Err(Fail::new(
            StatusCode::BAD_REQUEST,
            "notebook must be <group>/<name>",
        )),
    }
}

/// `GET /api/pipelines/{namespace}/{name}`: the view, the latest runs, params and retries.
/// A pipeline whose DAG does not exist yet is `loaded: false`, `state: "loading"` — not an
/// error. History of a deleted pipeline is not offered: without the resource the answer is 404.
async fn pipeline_detail(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
) -> Result<PipelineDetail, Fail> {
    let resources = resources.ok_or_else(no_cluster)?;
    let airflow = scheduler(airflow)?;
    let (dag_id, resource) = load_visible(resources, who, namespace, name).await?;

    let dag = match airflow.dag(&dag_id).await {
        Ok(dag) => Some(dag),
        Err(AirflowError::NotFound) => None,
        Err(e) => return Err(scheduler_fail(e)),
    };
    let runs = match dag {
        Some(_) => match airflow.runs(&dag_id, LATEST_RUNS).await {
            Ok(runs) => runs,
            // The DAG vanished between the two calls: show it as loading, not as a failure.
            Err(AirflowError::NotFound) => Vec::new(),
            Err(e) => return Err(scheduler_fail(e)),
        },
        None => Vec::new(),
    };
    let mut view = view_of(namespace, name, &resource, dag.as_ref(), runs.first(), who);
    let spec = spec_of(&resource);
    // The statement goes only on the detail route: the list stays small, and the caller has
    // already passed the same visibility rule that `load_visible` applies to an edit.
    view.tasks = task_specs(spec, true);
    let base = std::env::var("NOVA_BASE_DOMAIN").unwrap_or_default();
    // A non-administrator's Airflow role has no global DAG read, so the DAG page answers 403.
    let airflow_url =
        (who.is_admin && !base.is_empty()).then(|| format!("https://airflow.{base}/dags/{dag_id}"));
    Ok(PipelineDetail {
        pipeline: view,
        runs: runs.iter().map(run_view).collect(),
        params: params_of(spec),
        retries: retries_of(spec),
        options: options_of(spec),
        airflow_url,
        notify: notify_of(spec),
    })
}

/// The editable document of a stored pipeline (see [`PipelineDocument`]); built from the
/// resource alone, so it needs neither Airflow nor a notebook store.
/// The tasks a task-list resource declares, as `pipelines_validate` accepts them: the inverse of
/// `dbt_config`, with a notebook task's pinned version and parameters read back. `None` when a
/// task is of an unknown kind or carries a value that cannot be read back, so the document never
/// names a task the validator would refuse.
fn task_requests_of(spec: &Value) -> Option<Vec<PipelineTaskRequest>> {
    let tasks = spec.get("tasks")?.as_array()?;
    let mut requests = Vec::with_capacity(tasks.len());
    for task in tasks {
        let depends_on = match task.get("dependsOn") {
            Some(names) => names
                .as_array()?
                .iter()
                .map(|name| name.as_str().map(str::to_string))
                .collect::<Option<Vec<String>>>()?,
            None => Vec::new(),
        };
        let writes = match task.get("writes") {
            Some(tables) => tables
                .as_array()?
                .iter()
                .map(|table| table.as_str().map(str::to_string))
                .collect::<Option<Vec<String>>>()?,
            None => Vec::new(),
        };
        let mut request = PipelineTaskRequest {
            name: task.get("name")?.as_str()?.to_string(),
            sql: String::new(),
            notebook: None,
            dbt: None,
            quality: None,
            spark_pipeline: None,
            writes,
            depends_on,
        };
        match task.get("kind")?.as_str()? {
            "sql" => {
                request.sql = task
                    .get("sql")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
            }
            "notebook" => {
                let config = task.get("config")?;
                let params = match config.get("params").and_then(Value::as_str) {
                    Some(params) => {
                        serde_json::from_str::<BTreeMap<String, String>>(params).ok()?
                    }
                    None => BTreeMap::new(),
                };
                request.notebook = Some(NotebookRequest {
                    name: config.get("notebook")?.as_str()?.to_string(),
                    object_version: config.get("objectVersion")?.as_str()?.to_string(),
                    params,
                    connection: task
                        .get("connection")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                });
            }
            "dbt" => {
                let config = task.get("config")?;
                request.dbt = Some(DbtRequest {
                    project: config.get("project")?.as_str()?.to_string(),
                    select: config
                        .get("select")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    batches: match config.get("batches").and_then(Value::as_str) {
                        Some(batches) => Some(serde_json::from_str::<Vec<String>>(batches).ok()?),
                        None => None,
                    },
                    threads: match config.get("threads").and_then(Value::as_str) {
                        Some(threads) => Some(serde_json::from_str::<u8>(threads).ok()?),
                        None => None,
                    },
                    vars: match config.get("vars").and_then(Value::as_str) {
                        Some(vars) => {
                            Some(serde_json::from_str::<BTreeMap<String, String>>(vars).ok()?)
                        }
                        None => None,
                    },
                });
            }
            "quality" => {
                let checks = task
                    .get("checks")?
                    .as_array()?
                    .iter()
                    .map(|check| {
                        Some(QualityCheckRequest {
                            name: check.get("name")?.as_str()?.to_string(),
                            sql: check.get("sql")?.as_str()?.to_string(),
                            severity: check
                                .get("severity")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                        })
                    })
                    .collect::<Option<Vec<QualityCheckRequest>>>()?;
                request.quality = Some(QualityRequest { checks });
            }
            "spark-pipeline" => {
                request.spark_pipeline = Some(SparkPipelineRequest {
                    project: task.get("project")?.as_str()?.to_string(),
                    refresh: task
                        .get("refresh")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    full_refresh: match task.get("fullRefresh").and_then(Value::as_array) {
                        Some(names) => Some(
                            names
                                .iter()
                                .map(|name| name.as_str().map(str::to_string))
                                .collect::<Option<Vec<String>>>()?,
                        ),
                        None => None,
                    },
                    // An edit that changes the schedule must not drop the graph (#80 S2b).
                    files: match task.get("files").and_then(Value::as_object) {
                        Some(files) => files
                            .iter()
                            .map(|(path, text)| Some((path.clone(), text.as_str()?.to_string())))
                            .collect::<Option<BTreeMap<String, String>>>()?,
                        None => BTreeMap::new(),
                    },
                });
            }
            _ => return None,
        }
        requests.push(request);
    }
    Some(requests)
}

fn spec_response(resource: &Value) -> PipelineSpecResponse {
    let spec = spec_of(resource);
    let kind = run_task_kind(resource);
    let task = task_specs(spec, true).into_iter().next();
    let (sql, notebook) = match (kind, task) {
        (Some("sql"), Some(task)) => (task.sql, None),
        (Some("notebook"), Some(task)) => {
            let notebook = task
                .notebook
                .zip(task.object_version)
                .map(|(name, object_version)| NotebookRequest {
                    name,
                    object_version,
                    params: task.notebook_params.unwrap_or_default(),
                    // The document describes the whole editable state, so an update sent back
                    // as it was read keeps the task's connection.
                    connection: task.connection,
                });
            (None, notebook)
        }
        _ => (None, None),
    };
    let schedule = spec.get("schedule").and_then(|schedule| {
        let cron = schedule.get("cron")?.as_str()?;
        let timezone = schedule
            .get("timezone")
            .and_then(Value::as_str)
            .filter(|timezone| !timezone.is_empty())
            .unwrap_or("UTC");
        Some(ScheduleRequest {
            cron: cron.to_string(),
            timezone: timezone.to_string(),
        })
    });
    let managed = nova_managed(resource);
    PipelineSpecResponse {
        document: PipelineDocument {
            name: pipeline_name(resource).to_string(),
            owner_group: spec
                .get("ownerGroup")
                .and_then(Value::as_str)
                .map(str::to_string),
            description: spec
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_string),
            sql,
            notebook,
            tasks: if is_task_list(resource) {
                task_requests_of(spec)
            } else {
                None
            },
            schedule,
            params: params_of(spec),
            options: options_of(spec),
            notify: notify_of(spec),
            viewers: viewers_of(spec),
            publish: match spec.get("publish").and_then(Value::as_str) {
                Some("wap") => Some(Publish::Wap),
                _ => None,
            },
        },
        resource_version: resource
            .get("metadata")
            .and_then(|metadata| metadata.get("resourceVersion"))
            .and_then(Value::as_str)
            .map(str::to_string),
        managed_by: if managed { "nova" } else { "other" }.to_string(),
        editable: managed && (kind.is_some() || is_task_list(resource)),
    }
}

/// `GET /api/pipelines/{namespace}/{name}/spec`: who may see a pipeline may read its document.
async fn pipeline_spec(
    resources: Option<&PipelineResources>,
    who: &Identity,
    namespace: &str,
    name: &str,
) -> Result<PipelineSpecResponse, Fail> {
    let resources = resources.ok_or_else(no_cluster)?;
    let (_, resource) = load_visible(resources, who, namespace, name).await?;
    Ok(spec_response(&resource))
}

/// `GET …/runs/{run_id}`: the run and its tasks. The run is looked up through this pipeline's
/// DAG id, so a run id of another pipeline answers 404 and never reaches that DAG.
async fn run_detail(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
    run_id: &str,
) -> Result<RunResponse, Fail> {
    let resources = resources.ok_or_else(no_cluster)?;
    let airflow = scheduler(airflow)?;
    let (dag_id, _) = load_visible(resources, who, namespace, name).await?;

    let runs = airflow
        .runs(&dag_id, LATEST_RUNS)
        .await
        .map_err(run_lookup_fail)?;
    let run = runs
        .iter()
        .find(|run| run.dag_run_id == run_id)
        .ok_or_else(no_such_run)?;
    let tasks = airflow
        .task_instances(&dag_id, &run.dag_run_id)
        .await
        .map_err(task_lookup_fail)?;
    Ok(RunResponse {
        run: run_view(run),
        tasks: tasks.iter().map(task_view).collect(),
    })
}

/// One row of the run-queries read, in the query's column order. A named alias because the
/// tuple is too wide for `clippy::type_complexity`.
type RunQueryRow = (
    String,
    String,
    Vec<String>,
    String,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
);

/// The statements of one run, selected by the tags the pipeline loader sends
/// (`stardelt-demos` `dags/stardelt_pipelines/trino.py`): the pipeline tag must match too,
/// because Airflow's scheduled run ids are the same for every pipeline.
async fn run_queries_for(
    pool: &sqlx::PgPool,
    namespace: &str,
    name: &str,
    run_id: &str,
    redact: bool,
) -> Result<Vec<RunQueryView>, sqlx::Error> {
    let pipeline = format!("pipeline={namespace}/{name}");
    let run = format!("run={run_id}");
    let rows: Vec<RunQueryRow> = sqlx::query_as(
        "SELECT query_id, state, client_tags, sql_text, \
            (extract(epoch FROM started_at) * 1000)::bigint AS started_ms, \
            wall_ms, execution_ms, input_bytes, input_rows \
         FROM query_log \
         WHERE client_tags @> ARRAY[$1, $2]::text[] AND state <> 'QUEUED' \
         ORDER BY COALESCE(wall_ms, 0) DESC, started_at DESC NULLS LAST \
         LIMIT 200",
    )
    .bind(&pipeline)
    .bind(&run)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, state, tags, sql, started_at, wall_ms, execution_ms, input_bytes, input_rows)| {
                RunQueryView {
                    id,
                    state,
                    task_id: tags
                        .iter()
                        .find_map(|tag| tag.strip_prefix("task=").map(str::to_string)),
                    started_at,
                    wall_ms,
                    execution_ms,
                    input_bytes,
                    input_rows,
                    sql: if redact {
                        crate::query_log::redact_literals(&sql)
                    } else {
                        sql
                    },
                }
            },
        )
        .collect())
}

/// `GET …/runs/{run_id}/queries`: the run's statements, slowest first. Access is the
/// pipeline's (`load_visible`), not the query log's owner rule: these statements ran as the
/// pipeline's service principal.
async fn run_queries_answer(
    signed: SignedIn,
    namespace: String,
    name: String,
    run_id: String,
) -> Result<RunQueriesResponse, Fail> {
    let resources = resources(signed.state()).ok_or_else(no_cluster)?;
    load_visible(&resources, signed.identity(), &namespace, &name).await?;
    let Some(pool) = signed.state().db.as_ref() else {
        return Err(Fail::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "the state store is not configured",
        ));
    };
    let queries = run_queries_for(
        pool,
        &namespace,
        &name,
        &run_id,
        signed.state().redact_query_log_literals,
    )
    .await
    .map_err(|e| {
        tracing::warn!(error = %e, "run queries read failed");
        Fail::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "the query log is not readable",
        )
    })?;
    Ok(RunQueriesResponse { queries })
}

/// `GET …/tasks/{task_id}/logs`: one page of a task's log. `try` defaults to the task's last
/// attempt (read from the run's task instances). The group marker is dropped and the answer is
/// capped; the audit line names the user, pipeline, run and task — never the content.
#[allow(clippy::too_many_arguments)]
async fn task_logs(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
    run_id: &str,
    task_id: &str,
    try_number: Option<u32>,
    token: Option<&str>,
) -> Result<LogView, Fail> {
    let resources = resources.ok_or_else(no_cluster)?;
    let airflow = scheduler(airflow)?;
    let (dag_id, _) = load_visible(resources, who, namespace, name).await?;

    // Confirm the run belongs to this pipeline before any task id is passed on.
    let runs = airflow
        .runs(&dag_id, LATEST_RUNS)
        .await
        .map_err(run_lookup_fail)?;
    let run = runs
        .iter()
        .find(|run| run.dag_run_id == run_id)
        .ok_or_else(no_such_run)?;

    let try_number = match try_number {
        Some(0) => {
            return Err(Fail::new(
                StatusCode::BAD_REQUEST,
                "try must be a positive number",
            ));
        }
        Some(number) => number,
        None => airflow
            .task_instances(&dag_id, &run.dag_run_id)
            .await
            .map_err(task_lookup_fail)?
            .iter()
            .filter(|task| task.task_id == task_id)
            .filter_map(|task| task.try_number)
            .max()
            .unwrap_or(1),
    };

    let page = airflow
        .logs(&dag_id, &run.dag_run_id, task_id, try_number, token)
        .await
        .map_err(task_lookup_fail)?;
    audit::pipeline_log_event(who, namespace, name, &run.dag_run_id, task_id);
    Ok(log_view(page))
}

/// Whether the run still has an audit branch to inspect: `publish: wap`, `wap-prepare` succeeded
/// and `wap-publish` did not. A passing run publishes and drops its branch
/// (`demos/dags/pipeline_loader.py`, `_wap_publish`), so it has none.
fn wap_branch_present(publish: Option<&str>, instances: &[crate::airflow::TaskInstance]) -> bool {
    let state = |task: &str| {
        instances
            .iter()
            .find(|instance| instance.task_id == task)
            .and_then(|instance| instance.state.as_deref())
    };
    publish == Some("wap")
        && state("wap-prepare") == Some("success")
        && state("wap-publish") != Some("success")
}

/// `GET …/runs/{run_id}/checks`: every declared check of the run's quality tasks, with the
/// verdict its log carries. Ten log pages per task at most; a check not found stays `None`.
async fn run_checks(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
    run_id: &str,
) -> Result<ChecksResponse, Fail> {
    let resources = resources.ok_or_else(no_cluster)?;
    let airflow = scheduler(airflow)?;
    let (dag_id, pipeline) = load_visible(resources, who, namespace, name).await?;
    let runs = airflow
        .runs(&dag_id, LATEST_RUNS)
        .await
        .map_err(run_lookup_fail)?;
    let run = runs
        .iter()
        .find(|run| run.dag_run_id == run_id)
        .ok_or_else(no_such_run)?;
    let instances = airflow
        .task_instances(&dag_id, &run.dag_run_id)
        .await
        .map_err(task_lookup_fail)?;
    let mut checks = Vec::new();
    for (task, declared) in quality_tasks(&pipeline) {
        let last_try = instances
            .iter()
            .filter(|instance| instance.task_id == task)
            .filter_map(|instance| instance.try_number)
            .max();
        let mut found: std::collections::BTreeMap<String, (String, i64)> =
            std::collections::BTreeMap::new();
        if let Some(try_number) = last_try {
            let mut token: Option<String> = None;
            for _ in 0..10 {
                let page = airflow
                    .logs(
                        &dag_id,
                        &run.dag_run_id,
                        &task,
                        try_number,
                        token.as_deref(),
                    )
                    .await
                    .map_err(task_lookup_fail)?;
                let view = log_view(page);
                for line in &view.lines {
                    if let Some((check, severity, rows)) = check_result(&line.text) {
                        found.insert(check, (severity, rows));
                    }
                }
                match view.next_token {
                    Some(next) => token = Some(next),
                    None => break,
                }
            }
        }
        for (check, severity) in declared {
            let hit = found.get(&check);
            checks.push(CheckView {
                task: task.clone(),
                name: check,
                severity: hit
                    .map(|(severity, _)| severity.clone())
                    .unwrap_or(severity),
                rows: hit.map(|(_, rows)| *rows),
                passed: hit.map(|(_, rows)| *rows == 0),
            });
        }
    }
    let publish = pipeline
        .get("spec")
        .and_then(|s| s.get("publish"))
        .and_then(Value::as_str);
    let wap = publish == Some("wap");
    Ok(ChecksResponse {
        checks,
        audit_branch: wap_branch_present(publish, &instances).then(|| audit_branch(run_id)),
        audited_tables: if wap {
            audited_tables(&pipeline)
        } else {
            Vec::new()
        },
    })
}

/// `^\S{1,64}$` of `[A-Za-z0-9_-]`: what a task id in a run's object key can be.
fn valid_task_id(task: &str) -> bool {
    (1..=64).contains(&task.chars().count())
        && task
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
}

fn no_notebook_storage() -> Fail {
    Fail::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "notebook storage is not configured",
    )
}

fn no_run_copy() -> Fail {
    Fail::new(
        StatusCode::NOT_FOUND,
        "this run's executed notebook is not there: it was never written, or it expired",
    )
}

fn run_copy_too_large() -> Fail {
    Fail::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        "the executed notebook is larger than 25 MiB",
    )
}

fn bad_task_id() -> Fail {
    Fail::new(
        StatusCode::BAD_REQUEST,
        "a task id is 1 to 64 letters, digits, underscores or dashes",
    )
}

/// `GET …/tasks/{task_id}/notebook`: the executed copy of one notebook task run — exactly the
/// bytes the loader wrote to `_runs/<ownerGroup>/<pipeline>/<run_id>/<task>.ipynb` (#682). The
/// same visibility rule as the run and its log, the same audit line as a log read, and no
/// cache: the copy of a run never changes after it is written.
#[allow(clippy::too_many_arguments)]
async fn task_notebook(
    resources: Option<&PipelineResources>,
    store: Option<&NotebookStore>,
    who: &Identity,
    namespace: &str,
    name: &str,
    run_id: &str,
    task_id: &str,
    download: bool,
) -> Result<Response, Fail> {
    let resources = resources.ok_or_else(no_cluster)?;
    let (_, resource) = load_visible(resources, who, namespace, name).await?;
    let owner = spec_of(&resource)
        .get("ownerGroup")
        .and_then(Value::as_str)
        .ok_or_else(no_such_pipeline)?
        .to_string();
    if !valid_task_id(task_id) {
        return Err(bad_task_id());
    }
    let store = store.ok_or_else(no_notebook_storage)?;
    let key = notebooks::run_copy_key(&owner, name, run_id, task_id);
    let bytes = match notebooks::read_run_copy(store, &key).await {
        Ok(bytes) => bytes,
        Err(notebooks::StoreError::Refused(404)) => return Err(no_run_copy()),
        Err(notebooks::StoreError::TooLarge) => return Err(run_copy_too_large()),
        Err(error) => return Err(notebooks::store_fail(error)),
    };
    if bytes.len() > notebooks::MAX_RUN_NOTEBOOK_BYTES {
        return Err(run_copy_too_large());
    }
    // The bytes are the loader's own notebook; they go out as they are, never re-rendered.
    audit::pipeline_log_event(who, namespace, name, run_id, task_id);
    let mut response = (
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (header::CACHE_CONTROL, HeaderValue::from_static("no-store")),
        ],
        bytes,
    )
        .into_response();
    if download {
        let disposition =
            HeaderValue::from_str(&format!("attachment; filename=\"{name}-{task_id}.ipynb\""))
                .map_err(|_| bad_task_id())?;
        response
            .headers_mut()
            .insert(header::CONTENT_DISPOSITION, disposition);
    }
    Ok(response)
}

// ---------------------------------------------------------------------------
// Actions: run now, pause, resume, stop a run. Every action writes one audit line.
// ---------------------------------------------------------------------------

/// A refused or failed action: the answer and the audit outcome token (`refused:<why>` or
/// `error`). Keeping the two together is what makes "one audit line per action" true even on the
/// early returns.
struct ActionError {
    fail: Fail,
    outcome: String,
}

impl ActionError {
    fn refused(why: &'static str, fail: Fail) -> Self {
        Self {
            fail,
            outcome: format!("refused:{why}"),
        }
    }

    /// A refusal from a resource load: a pipeline the caller cannot see is `not-found`; one it
    /// can see but may not change is `forbidden` — the audit line says which, as the answer does.
    fn load_refusal(fail: Fail) -> Self {
        let why = if fail.status() == StatusCode::FORBIDDEN {
            "forbidden"
        } else {
            "not-found"
        };
        Self::refused(why, fail)
    }

    /// The same refusal with the task's name in front of the sentence (`#905` decision 2).
    fn about_task(self, name: &str) -> Self {
        Self {
            fail: Fail::new(
                self.fail.status(),
                format!("task `{name}`: {}", self.fail.message()),
            ),
            outcome: self.outcome,
        }
    }

    fn error(fail: Fail) -> Self {
        Self {
            fail,
            outcome: "error".into(),
        }
    }
}

/// `spec.concurrency` as the resource carries it; Airflow's and the CRD's default is `Forbid`.
fn concurrency_of(spec: &Value) -> &str {
    spec.get("concurrency")
        .and_then(Value::as_str)
        .unwrap_or("Forbid")
}

/// Serialises "read the active runs, then trigger" for one pipeline: without it two requests
/// that arrive together both see no active run and both start on a `Forbid` pipeline (measured
/// 2026-11-24, stardelt/stardelt#1537). One gate per pipeline, so a slow scheduler answer for
/// one pipeline never holds up a start of another; it does not hold across the run itself.
static RUN_START_GATES: std::sync::LazyLock<
    std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
> = std::sync::LazyLock::new(Default::default);

/// The start gate of `Pipeline <namespace>/<name>`, created on first use.
fn run_start_gate(namespace: &str, name: &str) -> Arc<tokio::sync::Mutex<()>> {
    Arc::clone(
        RUN_START_GATES
            .lock()
            .expect("run start gates poisoned")
            .entry(format!("{namespace}/{name}"))
            .or_default(),
    )
}

/// `POST …/runs`: start a run now. Refusals happen before Airflow is asked to trigger:
/// not loaded (409), paused (409), rate limit (429), an active run under `Forbid`/`Replace`
/// (409). `Allow` skips the activity check. `start` carries the run date and parameter values
/// of this run only (`dag_run.conf`, nova#817); it never changes the stored schedule.
pub(crate) async fn run_now(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    limiter: &RunStartsLimiter,
    who: &Identity,
    namespace: &str,
    name: &str,
    start: &RunStart,
) -> Result<RunView, Fail> {
    let result = run_now_inner(resources, airflow, limiter, who, namespace, name, start).await;
    match &result {
        Ok(run) => {
            audit::pipeline_event("run", who, namespace, name, Some(&run.run_id), "ok", None)
        }
        Err(e) => audit::pipeline_event("run", who, namespace, name, None, &e.outcome, None),
    }
    result.map_err(|e| e.fail)
}

async fn run_now_inner(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    limiter: &RunStartsLimiter,
    who: &Identity,
    namespace: &str,
    name: &str,
    start: &RunStart,
) -> Result<RunView, ActionError> {
    let gate = run_start_gate(namespace, name);
    let _start = gate.lock().await;
    let resources = resources
        .ok_or_else(no_cluster)
        .map_err(ActionError::error)?;
    let airflow = scheduler(airflow).map_err(ActionError::error)?;
    let (dag_id, resource) = load_runnable(resources, who, namespace, name)
        .await
        .map_err(ActionError::load_refusal)?;

    let dag = match airflow.dag(&dag_id).await {
        Ok(dag) => dag,
        Err(AirflowError::NotFound) => {
            return Err(ActionError::refused("not-loaded", pipeline_not_loaded()));
        }
        Err(e) => return Err(ActionError::error(scheduler_fail(e))),
    };
    if dag.is_paused == Some(true) {
        return Err(ActionError::refused("paused", pipeline_paused()));
    }

    // The rate limit comes after the checks that never touch Airflow's trigger: a refusal here
    // means a run was actually going to start.
    if !take_run_slot(limiter, &who.sub) {
        return Err(ActionError::refused("rate-limited", too_many_runs()));
    }

    if concurrency_of(spec_of(&resource)) != "Allow" {
        let runs = match airflow.runs(&dag_id, ACTIVE_RUNS).await {
            Ok(runs) => runs,
            Err(AirflowError::NotFound) => {
                return Err(ActionError::refused("not-loaded", pipeline_not_loaded()));
            }
            Err(e) => return Err(ActionError::error(scheduler_fail(e))),
        };
        if runs
            .iter()
            .any(|run| matches!(run.state.as_deref(), Some("queued") | Some("running")))
        {
            return Err(ActionError::refused("already-active", run_already_active()));
        }
    }

    // Always now, UTC, with microseconds: Airflow accepts any date and makes a run with no tasks
    // for one that is not now (#255), and its dag_run is unique on (dag_id, logical_date), so two
    // starts in one second need two dates (#1537). The date the person asked for travels as
    // `conf`, which the loader reads (`dag_run.conf`, nova#817), never as the logical date.
    let note = run_note(who, deployment_of_with(&resource, spec_digest).as_ref());
    let conf = run_conf(start);
    match airflow
        .trigger(&dag_id, &now_rfc3339_micros(), Some(&note), conf.as_ref())
        .await
    {
        Ok(run) => Ok(run_view(&run)),
        Err(e) => Err(ActionError::error(scheduler_fail(e))),
    }
}

/// The `conf` of a triggered run: only the keys the caller gave, `None` when there were none
/// (so an empty body sends exactly what Nova sent before #817).
fn run_conf(start: &RunStart) -> Option<Value> {
    let mut conf = serde_json::Map::new();
    if let Some(date) = &start.run_date {
        conf.insert("runDate".into(), Value::String(date.clone()));
    }
    if let Some(params) = &start.params {
        conf.insert("params".into(), json!(params));
    }
    if conf.is_empty() {
        None
    } else {
        Some(Value::Object(conf))
    }
}

/// `POST …/pause`: idempotent (pausing a paused pipeline answers 200) and answers the fresh view.
async fn pause_pipeline(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
) -> Result<PipelineView, Fail> {
    set_paused_action("pause", true, resources, airflow, who, namespace, name).await
}

/// `POST …/resume`: idempotent (resuming a running pipeline answers 200) and answers the fresh
/// view.
async fn resume_pipeline(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
) -> Result<PipelineView, Fail> {
    set_paused_action("resume", false, resources, airflow, who, namespace, name).await
}

#[allow(clippy::too_many_arguments)]
async fn set_paused_action(
    action: &'static str,
    paused: bool,
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
) -> Result<PipelineView, Fail> {
    let result = set_paused_inner(paused, resources, airflow, who, namespace, name).await;
    match &result {
        Ok(_) => audit::pipeline_event(action, who, namespace, name, None, "ok", None),
        Err(e) => audit::pipeline_event(action, who, namespace, name, None, &e.outcome, None),
    }
    result.map_err(|e| e.fail)
}

async fn set_paused_inner(
    paused: bool,
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
) -> Result<PipelineView, ActionError> {
    let resources = resources
        .ok_or_else(no_cluster)
        .map_err(ActionError::error)?;
    let airflow = scheduler(airflow).map_err(ActionError::error)?;
    let (dag_id, resource) = load_actionable(resources, who, namespace, name)
        .await
        .map_err(ActionError::load_refusal)?;

    // The patch answers the DAG as Airflow now has it, which is the fresh view the caller gets;
    // `404` is a pipeline the loader has not created yet.
    let dag = match airflow.set_paused(&dag_id, paused).await {
        Ok(dag) => dag,
        Err(AirflowError::NotFound) => {
            return Err(ActionError::refused("not-loaded", pipeline_not_loaded()));
        }
        Err(e) => return Err(ActionError::error(scheduler_fail(e))),
    };
    // A failing last-run read does not fail the pause: the view then has no lastRun, exactly as
    // the list behaves.
    let last_run = airflow
        .runs(&dag_id, 1)
        .await
        .ok()
        .and_then(|runs| runs.into_iter().next());
    Ok(view_of(
        namespace,
        name,
        &resource,
        Some(&dag),
        last_run.as_ref(),
        who,
    ))
}

/// `POST …/runs/{run_id}/stop`: mark an active run failed. Not a cancel: in-flight work can
/// finish and commit; the promise is only "the run is marked failed" (#255).
async fn stop_run(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
    run_id: &str,
) -> Result<RunView, Fail> {
    let result = stop_run_inner(resources, airflow, who, namespace, name, run_id).await;
    match &result {
        Ok(run) => {
            audit::pipeline_event("stop", who, namespace, name, Some(&run.run_id), "ok", None)
        }
        Err(e) => {
            audit::pipeline_event("stop", who, namespace, name, Some(run_id), &e.outcome, None)
        }
    }
    result.map_err(|e| e.fail)
}

async fn stop_run_inner(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
    run_id: &str,
) -> Result<RunView, ActionError> {
    let resources = resources
        .ok_or_else(no_cluster)
        .map_err(ActionError::error)?;
    let airflow = scheduler(airflow).map_err(ActionError::error)?;
    let (dag_id, _) = load_runnable(resources, who, namespace, name)
        .await
        .map_err(ActionError::load_refusal)?;

    // Read the run through this pipeline's DAG id first: a run id of another pipeline never
    // resolves, and the PATCH below only ever sees an id found here.
    let runs = match airflow.runs(&dag_id, LATEST_RUNS).await {
        Ok(runs) => runs,
        Err(AirflowError::NotFound) => {
            return Err(ActionError::refused("not-found", no_such_run()));
        }
        Err(e) => return Err(ActionError::error(scheduler_fail(e))),
    };
    let run = runs
        .iter()
        .find(|run| run.dag_run_id == run_id)
        .ok_or_else(|| ActionError::refused("not-found", no_such_run()))?;
    if !matches!(run.state.as_deref(), Some("queued") | Some("running")) {
        return Err(ActionError::refused("not-active", run_not_active()));
    }

    let note = format!(
        "{} by {}",
        crate::pipeline_notice::STOPPED_NOTE,
        who.display_name
    );
    match airflow
        .mark_run_failed(&dag_id, &run.dag_run_id, &note)
        .await
    {
        Ok(run) => Ok(run_view(&run)),
        Err(AirflowError::NotFound) => Err(ActionError::refused("not-found", no_such_run())),
        Err(e) => Err(ActionError::error(scheduler_fail(e))),
    }
}

/// `POST …/runs/{run_id}/retry`: clear only the failed task instances of a failed run, in
/// place. The run keeps its id, date and parameters; the tasks that already succeeded are not
/// repeated (nova#816).
async fn retry_run(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
    run_id: &str,
) -> Result<RunView, Fail> {
    let result = retry_run_inner(resources, airflow, who, namespace, name, run_id).await;
    match &result {
        Ok(run) => {
            audit::pipeline_event("retry", who, namespace, name, Some(&run.run_id), "ok", None)
        }
        Err(e) => audit::pipeline_event(
            "retry",
            who,
            namespace,
            name,
            Some(run_id),
            &e.outcome,
            None,
        ),
    }
    result.map_err(|e| e.fail)
}

async fn retry_run_inner(
    resources: Option<&PipelineResources>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
    run_id: &str,
) -> Result<RunView, ActionError> {
    let resources = resources
        .ok_or_else(no_cluster)
        .map_err(ActionError::error)?;
    let airflow = scheduler(airflow).map_err(ActionError::error)?;
    // A viewer (`spec.viewers`) may retry, exactly as it may run and stop (#816 decision 1).
    let (dag_id, _) = load_runnable(resources, who, namespace, name)
        .await
        .map_err(ActionError::load_refusal)?;

    // Read the run through this pipeline's DAG id first: a run id of another pipeline never
    // resolves, and the clear below only ever sees an id found here.
    let runs = match airflow.runs(&dag_id, LATEST_RUNS).await {
        Ok(runs) => runs,
        Err(AirflowError::NotFound) => {
            return Err(ActionError::refused("not-found", no_such_run()));
        }
        Err(e) => return Err(ActionError::error(scheduler_fail(e))),
    };
    let run = runs
        .iter()
        .find(|run| run.dag_run_id == run_id)
        .cloned()
        .ok_or_else(|| ActionError::refused("not-found", no_such_run()))?;
    if run.state.as_deref() != Some("failed") {
        return Err(ActionError::refused(
            "not-failed",
            Fail::new(StatusCode::CONFLICT, "only a failed run can be retried"),
        ));
    }

    match airflow.dag(&dag_id).await {
        Ok(dag) if dag.is_paused == Some(true) => {
            return Err(ActionError::refused(
                "paused",
                Fail::new(
                    StatusCode::CONFLICT,
                    "the pipeline is paused: resume it, then retry",
                ),
            ));
        }
        Ok(_) => {}
        Err(AirflowError::NotFound) => {
            return Err(ActionError::refused("not-found", no_such_run()));
        }
        Err(e) => return Err(ActionError::error(scheduler_fail(e))),
    }

    let cleared = match airflow.clear_failed_tasks(&dag_id, run_id).await {
        Ok(cleared) => cleared,
        Err(AirflowError::NotFound) => {
            return Err(ActionError::refused("not-found", no_such_run()));
        }
        Err(e) => return Err(ActionError::error(scheduler_fail(e))),
    };
    if cleared.is_empty() {
        return Err(ActionError::refused(
            "nothing-to-retry",
            Fail::new(
                StatusCode::CONFLICT,
                "this run has no failed tasks to retry",
            ),
        ));
    }

    // The clear re-queues the tasks; answer the run as Airflow now reports it.
    let runs = match airflow.runs(&dag_id, LATEST_RUNS).await {
        Ok(runs) => runs,
        Err(e) => return Err(ActionError::error(scheduler_fail(e))),
    };
    match runs.iter().find(|fresh| fresh.dag_run_id == run_id) {
        Some(fresh) => Ok(run_view(fresh)),
        None => Err(ActionError::error(scheduler_fail(
            AirflowError::Unavailable("the run vanished from the latest runs".into()),
        ))),
    }
}

// ---------------------------------------------------------------------------
// Writes: the principals route, create, update and delete. Every write is audited.
// ---------------------------------------------------------------------------

/// `GET /api/pipelines/principals`: every group of the caller and its pipeline principal.
async fn principal_views(
    resources: Option<&PipelineResources>,
    who: &Identity,
) -> Result<PrincipalsResponse, Fail> {
    let resources = resources.ok_or_else(no_cluster)?;
    let principals = resources.principals().await.map_err(resource_fail)?;
    let groups: BTreeSet<&str> = who
        .groups
        .iter()
        .map(String::as_str)
        .filter(|group| !group.is_empty())
        .collect();
    Ok(PrincipalsResponse {
        groups: groups
            .into_iter()
            .map(|group| principal_view(&principals, group))
            .collect(),
    })
}

async fn service_principal_views(
    resources: Option<&PipelineResources>,
    who: &Identity,
) -> Result<ServicePrincipalsResponse, Fail> {
    let resources = resources.ok_or_else(no_cluster)?;
    let principals = resources.principals().await.map_err(resource_fail)?;
    Ok(ServicePrincipalsResponse {
        principals: visible_principals(&principals, who),
    })
}

/// The name an audit line can carry: the request's, capped so a malformed one cannot flood the
/// log (the rest of the request is refused by [`validate_name`] anyway).
fn audit_name(name: &str) -> String {
    name.chars().take(64).collect()
}

/// The facts a create or update records beside the action (#210, #223, #237): the statement's
/// **length**, the cron text and — for a notebook — the group, the file name and the pinned
/// version. Never the statement, never a parameter value. A task list (`#905`) counts the
/// characters of its `sql` tasks and carries no notebook target: it is not one notebook.
struct WriteFacts {
    statement_chars: usize,
    cron: Option<String>,
    kind: &'static str,
    notebook: Option<(String, String, String)>,
}

impl WriteFacts {
    fn of(
        sql: &str,
        notebook: Option<&NotebookRequest>,
        tasks: Option<&[PipelineTaskRequest]>,
        schedule: Option<&ScheduleRequest>,
        group: &str,
    ) -> Self {
        Self {
            statement_chars: tasks
                .map(|tasks| {
                    tasks
                        .iter()
                        .map(|task| task.sql.trim().chars().count())
                        .sum()
                })
                .unwrap_or_else(|| sql.trim().chars().count()),
            cron: schedule.map(|schedule| schedule.cron.trim().to_string()),
            kind: if tasks.is_some() {
                "tasks"
            } else if notebook.is_some() {
                "notebook"
            } else {
                "sql"
            },
            notebook: notebook.map(|notebook| {
                (
                    group.to_string(),
                    notebook.name.clone(),
                    notebook.object_version.clone(),
                )
            }),
        }
    }

    fn write(&self) -> PipelineWrite<'_> {
        PipelineWrite {
            statement_chars: self.statement_chars,
            schedule: self.cron.as_deref(),
            kind: self.kind,
            notebook: self
                .notebook
                .as_ref()
                .map(|(group, name, version)| audit::NotebookTarget {
                    group,
                    name,
                    version,
                }),
        }
    }
}

/// The owner group of a pipeline, for an audit line: empty when the resource cannot be read
/// (the caller's answer is then a refusal anyway, and an audit line may not hold a guess).
async fn audit_group(resources: Option<&PipelineResources>, name: &str) -> String {
    let Some(resources) = resources else {
        return String::new();
    };
    let Ok(Some(resource)) = resources.get(name).await else {
        return String::new();
    };
    spec_of(&resource)
        .get("ownerGroup")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// `POST /api/pipelines`: create the resource, then answer its view (`loaded: false`, because
/// Airflow has not seen it yet). Audited whatever happens.
async fn create_pipeline(
    resources: Option<&PipelineResources>,
    db: Option<&sqlx::PgPool>,
    notebooks: Option<&NotebookStore>,
    limiter: &CreateLimiter,
    who: &Identity,
    body: CreatePipelineRequest,
) -> Result<PipelineView, Fail> {
    let namespace = crate::cluster::namespace();
    let pipeline = audit_name(&body.name);
    let facts = WriteFacts::of(
        &body.sql,
        body.notebook.as_ref(),
        body.tasks.as_deref(),
        body.schedule.as_ref(),
        &body.owner_group,
    );
    let result = create_pipeline_inner(resources, db, notebooks, limiter, who, body).await;
    match &result {
        Ok(_) => audit::pipeline_event(
            "create",
            who,
            &namespace,
            &pipeline,
            None,
            "ok",
            Some(facts.write()),
        ),
        Err(e) => audit::pipeline_event(
            "create",
            who,
            &namespace,
            &pipeline,
            None,
            &e.outcome,
            Some(facts.write()),
        ),
    }
    result.map_err(|e| e.fail)
}

/// Every name in `notify` must be a destination of `group` (nova#815 decision 1). The shape is
/// the operator's (`PipelineNotify::check_shape`), the ownership is the state store's: the
/// pipeline's `ownerGroup`, never the caller's groups, so an administrator cannot name another
/// group's destination either.
async fn check_notify(
    db: Option<&sqlx::PgPool>,
    group: &str,
    notify: Option<&PipelineNotify>,
) -> Result<(), Fail> {
    let Some(notify) = notify.filter(|notify| !notify.is_empty()) else {
        return Ok(());
    };
    notify
        .check_shape()
        .map_err(|message| Fail::new(StatusCode::BAD_REQUEST, message))?;
    let Some(db) = db else {
        return Err(Fail::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "this needs Nova's database, and this installation has none; ask your administrator",
        ));
    };
    let lists = [&notify.on_failure, &notify.on_success, &notify.on_recovered];
    let names: Vec<String> = lists
        .iter()
        .flat_map(|names| names.iter().cloned())
        .collect();
    let found: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM destination WHERE owner_group = $1 AND name = ANY($2)",
    )
    .bind(group)
    .bind(&names)
    .fetch_all(db)
    .await
    .map_err(|_| {
        Fail::new(
            StatusCode::BAD_GATEWAY,
            "Nova's database is not answering; try again in a minute",
        )
    })?;
    for name in lists.iter().flat_map(|names| names.iter()) {
        if !found.contains(name) {
            return Err(Fail::new(
                StatusCode::BAD_REQUEST,
                format!("there is no destination named {name} in {group}"),
            ));
        }
    }
    Ok(())
}

/// `spec.viewers` of a create or update body: at most 16 group names, the CRD's rule.
pub(crate) fn check_viewers(viewers: Option<&[String]>) -> Result<(), String> {
    let Some(viewers) = viewers else {
        return Ok(());
    };
    if viewers.len() > 16 {
        return Err("viewers holds at most 16 groups".to_string());
    }
    match viewers
        .iter()
        .find(|group| !crate::notebooks::valid_group(group))
    {
        Some(bad) => Err(format!("viewers holds `{bad}`, which is not a group name")),
        None => Ok(()),
    }
}

/// `spec.description`: one line, at most 200 characters (the CRD's bound). Blank is `None`:
/// a form that sends "" clears the line instead of writing an empty string.
fn check_description(text: Option<&str>) -> Result<Option<String>, String> {
    let Some(text) = text else { return Ok(None) };
    let line = text.trim();
    if line.is_empty() {
        return Ok(None);
    }
    if line.chars().count() > 200 {
        return Err("a pipeline description is at most 200 characters".to_string());
    }
    Ok(Some(line.to_string()))
}

/// What a create writes once every rule has passed: the statement or notebook, the schedule, the options.
struct Planned {
    statement: Statement,
    schedule: Option<(String, String)>,
    options: PipelineOptionsView,
}

/// The rules every write of a new pipeline runs before it writes, in order (a create and the
/// validate route share them, so "the check passed" and "the write succeeds" cannot drift apart).
async fn plan_pipeline(
    resources: &PipelineResources,
    db: Option<&sqlx::PgPool>,
    notebooks_store: Option<&NotebookStore>,
    who: &Identity,
    body: &CreatePipelineRequest,
) -> Result<Planned, ActionError> {
    // 1. The name the CRD (and the loader) accepts.
    validate_name(&body.name).map_err(|message| {
        ActionError::refused("name", Fail::new(StatusCode::BAD_REQUEST, message))
    })?;

    // 1b. The viewers are group names the CRD accepts (#79).
    check_viewers(body.viewers.as_deref()).map_err(|message| {
        ActionError::refused("viewers", Fail::new(StatusCode::BAD_REQUEST, message))
    })?;

    // 2. One of the caller's own groups — an administrator schedules for their groups too.
    if !who.groups.iter().any(|group| group == &body.owner_group) {
        return Err(ActionError::refused(
            "not-member",
            Fail::new(
                StatusCode::FORBIDDEN,
                format!("you are not a member of the group {}", body.owner_group),
            ),
        ));
    }

    // 2b. Every notification destination belongs to the pipeline's own group (nova#815).
    check_notify(db, &body.owner_group, body.notify.as_ref())
        .await
        .map_err(|fail| ActionError::refused("notify", fail))?;

    // 3. The principal of the group must exist and be ready: the loader refuses otherwise, and
    //    a pipeline the loader refuses would sit in the list as `not-loaded`.
    let principals = resources
        .principals()
        .await
        .map_err(|e| ActionError::error(resource_fail(e)))?;
    let principal = principal_view(&principals, &body.owner_group);
    if !principal.ready {
        return Err(ActionError::refused(
            "no-principal",
            Fail::new(StatusCode::CONFLICT, principal.message.unwrap_or_default()),
        ));
    }

    // 4. What it runs: exactly one of a statement, a notebook and a task list (#237, #905), with
    //    the rules of its kind — the statement's parameters and placeholders, or the notebook's
    //    name, parameters and a pinned version that exists in the group.
    let statement = checked_run(
        &body.sql,
        body.notebook.as_ref(),
        body.tasks.as_deref(),
        &body.params,
        notebooks_store,
        &body.owner_group,
        body.publish,
    )
    .await?;

    // 5. The advanced options, defaults filled in.
    let options = body
        .options
        .clone()
        .unwrap_or_default()
        .resolve()
        .map_err(|message| {
            ActionError::refused("options", Fail::new(StatusCode::BAD_REQUEST, message))
        })?;

    // 6. The schedule (or none, for on demand).
    let schedule = validate_schedule(body.schedule.as_ref()).map_err(|message| {
        ActionError::refused("schedule", Fail::new(StatusCode::BAD_REQUEST, message))
    })?;

    // 6b. What a deploy says about where the pipeline came from, when it says anything.
    if let Some(provenance) = &body.provenance {
        check_provenance(provenance).map_err(|message| {
            ActionError::refused("provenance", Fail::new(StatusCode::BAD_REQUEST, message))
        })?;
    }

    Ok(Planned {
        statement,
        schedule,
        options,
    })
}

const BUNDLE_LABEL: &str = "stardelt.io/bundle";
const TARGET_LABEL: &str = "stardelt.io/target";
const REVISION_NOTE: &str = "stardelt.io/revision";
const REPOSITORY_NOTE: &str = "stardelt.io/repository";
const DEPLOYED_BY_NOTE: &str = "stardelt.io/deployed-by";
const DEPLOYED_AT_NOTE: &str = "stardelt.io/deployed-at";
pub(crate) const DIGEST_NOTE: &str = "stardelt.io/spec-digest";

/// `value` as text with the keys of every object in sorted order, so two spellings of one
/// document give one text.
pub(crate) fn canonical(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let parts: Vec<String> = keys
                .into_iter()
                .map(|key| format!("{}:{}", json!(key), canonical(&map[key])))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(canonical).collect();
            format!("[{}]", parts.join(","))
        }
        other => other.to_string(),
    }
}

/// A fingerprint of what a deploy decides about a pipeline: the run, the schedule, the parameters
/// and the options, the same fields `changes_of` compares. A deploy writes it; a later difference
/// means someone changed the pipeline since.
fn spec_digest(resource: &Value) -> String {
    let spec = spec_of(resource);
    let decided = json!([
        spec.get("tasks"),
        spec.get("schedule"),
        spec.get("params"),
        spec.get("concurrency"),
        spec.get("retries"),
        spec.get("timeout"),
    ]);
    Sha256::digest(canonical(&decided).as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Lower-case letters, digits and inner dashes, `shortest` to `longest` characters.
fn dashed_name(text: &str, shortest: usize, longest: usize) -> bool {
    (shortest..=longest).contains(&text.len())
        && text
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !text.starts_with('-')
        && !text.ends_with('-')
}

/// The rules for what a deploy may stamp on a pipeline: short, plain words that are valid label
/// values. `Err` is the sentence the caller sees.
pub(crate) fn check_provenance(provenance: &Provenance) -> Result<(), String> {
    if !dashed_name(&provenance.bundle, 3, 40) {
        return Err("the bundle name must be 3 to 40 lower-case letters, digits and dashes".into());
    }
    if !dashed_name(&provenance.target, 1, 32) {
        return Err("the target must be 1 to 32 lower-case letters, digits and dashes".into());
    }
    if let Some(revision) = &provenance.revision {
        let hex = revision.strip_suffix("-dirty").unwrap_or(revision);
        let plain = (7..=64).contains(&hex.len())
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if !plain {
            return Err(
                "the revision must be 7 to 64 hex digits, with -dirty appended when the working tree had uncommitted changes"
                    .into(),
            );
        }
    }
    if let Some(repository) = &provenance.repository {
        // A scheme with no host (`https://`) passes every character rule below but is not an
        // address a link can be built from; nova#676 showed it blanking the Deployments page.
        let host = repository
            .strip_prefix("https://")
            .and_then(|rest| rest.split('/').next());
        let plain = repository.len() <= 200
            && host.is_some_and(|host| !host.is_empty())
            && !repository[8..]
                .chars()
                .any(|c| c.is_whitespace() || matches!(c, '@' | '?' | '#'));
        if !plain {
            return Err(
                "the repository must be an https address of at most 200 characters, without a user or a token"
                    .into(),
            );
        }
    }
    Ok(())
}

/// Write a deploy's provenance on `resource`: the two labels, the revision (or none), who and when.
pub(crate) fn stamp_provenance(resource: &mut Value, provenance: &Provenance, deployed_by: &str) {
    resource["metadata"]["labels"][BUNDLE_LABEL] = json!(provenance.bundle);
    resource["metadata"]["labels"][TARGET_LABEL] = json!(provenance.target);
    let notes = &mut resource["metadata"]["annotations"];
    match &provenance.revision {
        Some(revision) => notes[REVISION_NOTE] = json!(revision),
        None => {
            if let Some(map) = notes.as_object_mut() {
                map.remove(REVISION_NOTE);
            }
        }
    }
    match &provenance.repository {
        Some(repository) => notes[REPOSITORY_NOTE] = json!(repository),
        None => {
            if let Some(map) = notes.as_object_mut() {
                map.remove(REPOSITORY_NOTE);
            }
        }
    }
    notes[DEPLOYED_BY_NOTE] = json!(deployed_by);
    notes[DEPLOYED_AT_NOTE] = json!(now_rfc3339());
    let digest = spec_digest(resource);
    resource["metadata"]["annotations"][DIGEST_NOTE] = json!(digest);
}

/// What a deploy stamped on a stored resource, if it did. `digest_of` answers the resource's
/// current fingerprint: `spec_digest` for a pipeline, the stream's own digest for a `StreamApp`.
pub(crate) fn deployment_of_with(
    resource: &Value,
    digest_of: fn(&Value) -> String,
) -> Option<DeploymentView> {
    let metadata = resource.get("metadata")?;
    let label = |key: &str| {
        metadata
            .get("labels")?
            .get(key)?
            .as_str()
            .map(str::to_string)
    };
    let note = |key: &str| {
        metadata
            .get("annotations")?
            .get(key)?
            .as_str()
            .map(str::to_string)
    };
    Some(DeploymentView {
        bundle: label(BUNDLE_LABEL)?,
        target: label(TARGET_LABEL)?,
        revision: note(REVISION_NOTE),
        repository: note(REPOSITORY_NOTE),
        deployed_by: note(DEPLOYED_BY_NOTE),
        deployed_at: note(DEPLOYED_AT_NOTE),
        modified: note(DIGEST_NOTE)
            .filter(|digest| *digest != digest_of(resource))
            .map(|_| true),
    })
}

/// What a deploy stamped on a stored pipeline, if it did.
pub(crate) fn deployment_of(resource: &Value) -> Option<DeploymentView> {
    deployment_of_with(resource, spec_digest)
}

async fn create_pipeline_inner(
    resources: Option<&PipelineResources>,
    db: Option<&sqlx::PgPool>,
    notebooks_store: Option<&NotebookStore>,
    limiter: &CreateLimiter,
    who: &Identity,
    body: CreatePipelineRequest,
) -> Result<PipelineView, ActionError> {
    let resources = resources
        .ok_or_else(no_cluster)
        .map_err(ActionError::error)?;

    // The description is one line at most 200 characters, blank meaning none (`spec.description`).
    let description = check_description(body.description.as_deref()).map_err(|message| {
        ActionError::refused("description", Fail::new(StatusCode::BAD_REQUEST, message))
    })?;

    let Planned {
        statement,
        schedule,
        options,
    } = plan_pipeline(resources, db, notebooks_store, who, &body).await?;

    // A `wap` pipeline must have a quality gate and a table to publish (#114).
    check_publish(body.publish, body.tasks.as_deref())?;

    // 7. The name is free.
    if resources
        .get(&body.name)
        .await
        .map_err(|e| ActionError::error(resource_fail(e)))?
        .is_some()
    {
        return Err(ActionError::refused("exists", pipeline_exists()));
    }

    // 8. The rate limit comes last: a refusal above never consumes a slot.
    if !take_create_slot(limiter, &who.sub) {
        return Err(ActionError::refused("rate-limited", too_many_creates()));
    }

    let mut resource = pipeline_resource(
        &body.name,
        &body.owner_group,
        &statement,
        schedule.as_ref(),
        &body.params,
        &options,
        &who.display_name,
        body.notify.as_ref(),
        body.viewers.as_deref().unwrap_or_default(),
        description.as_deref(),
        body.publish,
    );
    if let Some(provenance) = &body.provenance {
        stamp_provenance(&mut resource, provenance, &who.display_name);
    }
    let written = resources.create(resource).await.map_err(|e| match e {
        // Another Nova or a `kubectl apply` won the race between the read and the create.
        ResourceError::Exists => ActionError::refused("exists", pipeline_exists()),
        other => ActionError::error(resource_write_fail(other)),
    })?;
    let mut view = view_of(
        &crate::cluster::namespace(),
        &body.name,
        &written,
        None,
        None,
        who,
    );
    // The answer is the resource as written: the loader has not seen it yet, whatever the
    // cluster's clock says about its age.
    view.loaded = false;
    view.state = "loading".to_string();
    Ok(view)
}

/// The answer of a refused check: the sentence, nothing else.
fn not_ok(message: &str) -> ValidatePipelineResponse {
    ValidatePipelineResponse {
        ok: false,
        action: None,
        errors: vec![ValidationIssue {
            message: message.to_string(),
        }],
        changes: Vec::new(),
        next_runs: Vec::new(),
        modified: None,
    }
}

/// The refusal a production stage gives a notebook schedule that no bundle deploy stamped
/// (stardelt-nova#819 decision 3). One sentence, the same on every route.
pub(crate) const PRODUCTION_NOTEBOOK: &str =
    "In production a notebook comes from Git. Deploy it with a bundle.";

/// Whether the body runs a notebook: the single-notebook form or any notebook task of a list.
fn runs_a_notebook(
    notebook: Option<&NotebookRequest>,
    tasks: Option<&[PipelineTaskRequest]>,
) -> bool {
    notebook.is_some()
        || tasks.is_some_and(|tasks| tasks.iter().any(|task| task.notebook.is_some()))
}

/// Whether a stored Pipeline resource runs a notebook task (`spec.tasks[*].kind == "notebook"`).
fn resource_runs_a_notebook(resource: &Value) -> bool {
    resource
        .get("spec")
        .and_then(|spec| spec.get("tasks"))
        .and_then(Value::as_array)
        .is_some_and(|tasks| {
            tasks
                .iter()
                .any(|task| task.get("kind").and_then(Value::as_str) == Some("notebook"))
        })
}

/// The production rule: a scheduled notebook needs a bundle deploy stamp. `has_stamp` is whether
/// the pipeline will carry one after this request (the body's, or the stored one on an update).
fn production_notebook_sentence(
    stage_kind: &str,
    has_schedule: bool,
    runs_notebook: bool,
    has_stamp: bool,
) -> Option<&'static str> {
    (stage_kind == "prod" && has_schedule && runs_notebook && !has_stamp)
        .then_some(PRODUCTION_NOTEBOOK)
}

/// The update half of the production rule (#819 decision 3). The stamp may be on the stored
/// resource: a form save sends no `provenance`, and `UpdatePipelineRequest` keeps what is
/// stored. A read that fails here is not this rule's answer — `update_pipeline` reports it.
async fn update_production_refusal(
    state: &AppState,
    who: &Identity,
    namespace: &str,
    name: &str,
    body: &UpdatePipelineRequest,
) -> Option<Fail> {
    if state.stage_kind != "prod" || body.schedule.is_none() {
        return None;
    }
    if body.provenance.is_some() || !runs_a_notebook(body.notebook.as_ref(), body.tasks.as_deref())
    {
        return None;
    }
    let resources = resources(state)?;
    let (_, stored) = load_actionable(&resources, who, namespace, name)
        .await
        .ok()?;
    if deployment_of(&stored).is_some() || !resource_runs_a_notebook(&stored) {
        return None;
    }
    Some(Fail::new(StatusCode::BAD_REQUEST, PRODUCTION_NOTEBOOK))
}

/// Run every rule of a create against `body` and say what would happen, writing nothing and
/// taking no slot of the creation limit. A refusal of a rule is an answer (`ok: false`); an
/// unreachable cluster or notebook store is still an error, as on create.
pub(crate) async fn validate_pipeline(
    resources: Option<&PipelineResources>,
    db: Option<&sqlx::PgPool>,
    notebooks_store: Option<&NotebookStore>,
    who: &Identity,
    body: CreatePipelineRequest,
) -> Result<ValidatePipelineResponse, Fail> {
    let resources = resources.ok_or_else(no_cluster)?;
    // The same rule a create and an update apply, so the answer and the write cannot disagree.
    let description = match check_description(body.description.as_deref()) {
        Ok(description) => description,
        Err(message) => return Ok(not_ok(&message)),
    };
    let planned = match plan_pipeline(resources, db, notebooks_store, who, &body).await {
        Ok(planned) => planned,
        Err(e) if e.outcome.starts_with("refused:") => return Ok(not_ok(e.fail.message())),
        Err(e) => return Err(e.fail),
    };
    let mut modified = None;
    let (action, changes) = match resources.get(&body.name).await.map_err(resource_fail)? {
        None => ("create", Vec::new()),
        Some(existing) => {
            if !may_act(who, spec_of(&existing)) {
                return Ok(not_ok(pipeline_exists().message()));
            }
            if !nova_managed(&existing) {
                return Ok(not_ok(managed_outside().message()));
            }
            if spec_of(&existing)["ownerGroup"].as_str() != Some(body.owner_group.as_str()) {
                return Ok(not_ok("the group of an existing pipeline cannot change"));
            }
            // What an update has, for three shapes (#905 decision 3).
            match (stored_shape(&existing), &planned.statement) {
                (StoredShape::Other, _) => return Ok(not_ok(shape_not_editable().message())),
                (StoredShape::Tasks, Statement::Tasks(_)) => {}
                (StoredShape::Tasks, _) => return Ok(not_ok(SEVERAL_TASKS)),
                (StoredShape::Run("notebook"), Statement::Notebook { .. }) => {}
                (StoredShape::Run("notebook"), _) => return Ok(not_ok(NOTEBOOK_PIPELINE)),
                (StoredShape::Run(_), Statement::Sql(_)) => {}
                (StoredShape::Run(_), _) => return Ok(not_ok(STATEMENT_PIPELINE)),
            }
            let mut updated = updated_resource(
                &existing,
                &planned.statement,
                &planned.options,
                &body.params,
                planned.schedule.as_ref(),
                body.notify.as_ref(),
                body.viewers.as_deref(),
                description.as_deref(),
                body.publish,
            );
            if let Some(provenance) = &body.provenance {
                stamp_provenance(&mut updated, provenance, &who.display_name);
            }
            modified = deployment_of(&existing).and_then(|deployment| deployment.modified);
            ("update", changes_of(&existing, &updated))
        }
    };
    let next_runs = planned
        .schedule
        .and_then(|(cron, timezone)| {
            let request = PreviewScheduleRequest {
                cron,
                timezone,
                count: Some(5),
            };
            preview_schedule(request, now_utc()).ok()
        })
        .map(|preview| preview.runs)
        .unwrap_or_default();
    Ok(ValidatePipelineResponse {
        ok: true,
        action: Some(action.to_string()),
        errors: Vec::new(),
        changes,
        next_runs,
        modified,
    })
}

/// The name of one listed `StreamApp` (a full object, as `streams::stream_apps` answers them).
fn stream_name(object: &Value) -> Option<&str> {
    object.pointer("/metadata/name").and_then(Value::as_str)
}

/// `POST /api/deployments/plan`: what creating and updating this bundle's rendered Pipeline
/// documents on one target would do, writing nothing (#101). Each document goes through
/// `validate_pipeline`, the same function `pipelines_validate` and every write route use, so the
/// plan and the apply cannot disagree. `recorded_files` is the `files` of the newest applied
/// deployment of this bundle and target, or `None` when none was recorded.
pub(crate) async fn plan_bundle(
    resources: Option<&PipelineResources>,
    db: Option<&sqlx::PgPool>,
    notebooks_store: Option<&NotebookStore>,
    who: &Identity,
    recorded_files: Option<&[crate::deployments::DeploymentFile]>,
    existing_streams: &[Value],
    request: crate::deployments::BundlePlanRequest,
) -> Result<crate::deployments::BundlePlan, Fail> {
    let resources = resources.ok_or_else(no_cluster)?;
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut changes = Vec::new();
    for (index, document) in request.resources.iter().enumerate() {
        let path = format!("resources[{index}]");
        let kind = document
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("Pipeline");
        match kind {
            "Pipeline" => {
                let body: CreatePipelineRequest = match serde_json::from_value(document.clone()) {
                    Ok(body) => body,
                    Err(err) => {
                        errors.push(PlanIssue {
                            path,
                            message: format!("not a pipeline document: {err}"),
                        });
                        continue;
                    }
                };
                let name = body.name.clone();
                match validate_pipeline(Some(resources), db, notebooks_store, who, body).await {
                    Ok(answer) if answer.ok => changes.push(PlanChange {
                        kind: "Pipeline".into(),
                        name,
                        action: planned_action(&answer),
                        diff: answer.changes,
                    }),
                    Ok(answer) => {
                        for issue in answer.errors {
                            errors.push(PlanIssue {
                                path: format!("{path} ({name})"),
                                message: issue.message,
                            });
                        }
                    }
                    Err(fail) => return Err(fail),
                }
            }
            "StreamApp" => {
                let body: crate::streams::StreamWrite =
                    match serde_json::from_value(document.clone()) {
                        Ok(body) => body,
                        Err(err) => {
                            errors.push(PlanIssue {
                                path: format!("{path} (stream)"),
                                message: format!("not a stream document: {err}"),
                            });
                            continue;
                        }
                    };
                if body.owner_group != request.owner_group {
                    errors.push(PlanIssue {
                        path,
                        message: format!(
                            "the stream's ownerGroup {} is not the plan's {}",
                            body.owner_group, request.owner_group
                        ),
                    });
                    continue;
                }
                match crate::streams::check_write(&body.name, &body, who) {
                    Ok(_) => {
                        // A stream write is a PUT, so the write cannot say create or update; a listed stream
                        // of this name does (#101). No stream diff yet: a changed stream still reads `update`
                        // with an empty diff.
                        let action = if existing_streams
                            .iter()
                            .any(|stream| stream_name(stream) == Some(body.name.as_str()))
                        {
                            "update"
                        } else {
                            "create"
                        };
                        changes.push(PlanChange {
                            kind: "StreamApp".into(),
                            name: body.name.clone(),
                            action: action.into(),
                            diff: Vec::new(),
                        });
                    }
                    Err(message) => errors.push(PlanIssue {
                        path: format!("{path} ({})", body.name),
                        message,
                    }),
                }
            }
            _ => errors.push(PlanIssue {
                path,
                message: format!(
                    "a plan covers Pipeline or StreamApp documents; this one is kind {kind}"
                ),
            }),
        }
    }
    // What this bundle and target own and the set no longer names: a `delete` change when the
    // caller prunes (`--prune`), a warning otherwise.
    for existing in resources.list().await.map_err(resource_fail)? {
        let Some(deployment) = deployment_of(&existing) else {
            continue;
        };
        if deployment.bundle != request.bundle || deployment.target != request.target {
            continue;
        }
        if spec_of(&existing)["ownerGroup"].as_str() != Some(request.owner_group.as_str()) {
            continue;
        }
        let name = existing
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if changes.iter().any(|change| change.name == name) {
            continue;
        }
        if request.prune {
            changes.push(PlanChange {
                kind: "Pipeline".into(),
                name: name.to_string(),
                action: "delete".into(),
                diff: Vec::new(),
            });
            continue;
        }
        warnings.push(PlanIssue {
            path: "pipelines".into(),
            message: format!(
                "{name} is deployed from this bundle and target and this plan does not name it"
            ),
        });
    }
    let before: BTreeMap<&str, &str> = recorded_files
        .unwrap_or_default()
        .iter()
        .map(|file| (file.path.as_str(), file.sha256.as_str()))
        .collect();
    let mut files: Vec<PlanFile> = request
        .files
        .iter()
        .map(|file| PlanFile {
            path: file.path.clone(),
            sha256: file.sha256.clone(),
            new: before.get(file.path.as_str()) != Some(&file.sha256.as_str()),
        })
        .collect();
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let plan_id = crate::deployments::rendered_set_id(
        &request.bundle,
        &request.target,
        &request.resources,
        &request.files,
    );
    Ok(BundlePlan {
        ok: errors.is_empty(),
        errors,
        warnings,
        changes,
        files,
        plan_id,
    })
}

/// What a document's check means for the plan: `pipelines_validate` answers `update` even when
/// nothing differs, and the plan says `unchanged` then (#101's action set).
fn planned_action(answer: &ValidatePipelineResponse) -> String {
    match answer.action.as_deref() {
        Some("update") if answer.changes.is_empty() && answer.modified != Some(true) => {
            "unchanged".into()
        }
        Some(other) => other.to_string(),
        None => "create".into(),
    }
}

/// The view after a write: the DAG and last run when Airflow is configured (a failed lookup only
/// leaves them empty), the resource alone otherwise — a write never requires Airflow.
async fn view_after_write(
    airflow: Option<&AirflowClient>,
    namespace: &str,
    name: &str,
    resource: &Value,
    who: &Identity,
) -> PipelineView {
    let Some(airflow) = airflow else {
        return view_of(namespace, name, resource, None, None, who);
    };
    let dag_id = dag_id_for(namespace, name);
    let dag = match airflow.dag(&dag_id).await {
        Ok(dag) => Some(dag),
        Err(AirflowError::NotFound) => None,
        Err(e) => {
            tracing::warn!(dag = %dag_id, error = %e, "could not read the DAG after a write");
            None
        }
    };
    let last_run = match dag {
        Some(_) => airflow
            .runs(&dag_id, 1)
            .await
            .ok()
            .and_then(|runs| runs.into_iter().next()),
        None => None,
    };
    view_of(
        namespace,
        name,
        resource,
        dag.as_ref(),
        last_run.as_ref(),
        who,
    )
}

/// `PUT /api/pipelines/{namespace}/{name}`: change the statement and the schedule of a statement
/// pipeline, or the version and parameters of a notebook pipeline, and nothing else. Audited
/// whatever happens.
#[allow(clippy::too_many_arguments)]
async fn update_pipeline(
    resources: Option<&PipelineResources>,
    db: Option<&sqlx::PgPool>,
    notebooks: Option<&NotebookStore>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
    body: UpdatePipelineRequest,
) -> Result<PipelineView, Fail> {
    // The update keeps the pipeline's group, so the audit's group is read from the resource.
    let group = audit_group(resources, name).await;
    let facts = WriteFacts::of(
        &body.sql,
        body.notebook.as_ref(),
        body.tasks.as_deref(),
        body.schedule.as_ref(),
        &group,
    );
    let result = update_pipeline_inner(
        resources, db, notebooks, airflow, who, namespace, name, body,
    )
    .await;
    match &result {
        Ok(_) => audit::pipeline_event(
            "update",
            who,
            namespace,
            name,
            None,
            "ok",
            Some(facts.write()),
        ),
        Err(e) => audit::pipeline_event(
            "update",
            who,
            namespace,
            name,
            None,
            &e.outcome,
            Some(facts.write()),
        ),
    }
    result.map_err(|e| e.fail)
}

#[allow(clippy::too_many_arguments)]
async fn update_pipeline_inner(
    resources: Option<&PipelineResources>,
    db: Option<&sqlx::PgPool>,
    notebooks_store: Option<&NotebookStore>,
    airflow: Option<&AirflowClient>,
    who: &Identity,
    namespace: &str,
    name: &str,
    body: UpdatePipelineRequest,
) -> Result<PipelineView, ActionError> {
    let resources = resources
        .ok_or_else(no_cluster)
        .map_err(ActionError::error)?;
    let (_, resource) = load_actionable(resources, who, namespace, name)
        .await
        .map_err(ActionError::load_refusal)?;

    // The description is one line at most 200 characters, blank meaning none (`spec.description`).
    let description = check_description(body.description.as_deref()).map_err(|message| {
        ActionError::refused("description", Fail::new(StatusCode::BAD_REQUEST, message))
    })?;

    // Only what Nova wrote may be rewritten; a manifest stays its author's.
    if !nova_managed(&resource) {
        return Err(ActionError::refused("not-nova", managed_outside()));
    }

    // An editor sends the version it read: a different stored version means somebody changed the
    // pipeline meanwhile, and the person must reload before anything is written.
    if let Some(read) = body.resource_version.as_deref() {
        let stored = resource
            .get("metadata")
            .and_then(|metadata| metadata.get("resourceVersion"))
            .and_then(Value::as_str);
        if stored != Some(read) {
            return Err(ActionError::refused("conflict", pipeline_changed()));
        }
    }

    // The viewers are group names the CRD accepts (#79).
    check_viewers(body.viewers.as_deref()).map_err(|message| {
        ActionError::refused("viewers", Fail::new(StatusCode::BAD_REQUEST, message))
    })?;

    // The pipeline's group: both the refusal sentence and the notebook's object key.
    let group = spec_of(&resource)
        .get("ownerGroup")
        .and_then(Value::as_str)
        .filter(|group| !group.is_empty())
        .ok_or_else(|| ActionError::refused("shape", shape_not_editable()))?
        .to_string();

    // Every notification destination belongs to the pipeline's own group, never the caller's.
    check_notify(db, &group, body.notify.as_ref())
        .await
        .map_err(|fail| ActionError::refused("notify", fail))?;

    // A body names exactly one of a statement, a notebook and a task list (#237, #905), and an
    // update never changes the shape the pipeline has: a task list stays a task list and a `run`
    // pipeline stays one. `UpdatePipelineRequest` carries no `dbt` on purpose (#112): a dbt task
    // lives only in a task list, which is edited in its file, and `run_task_kind` answers `None`
    // for one — so the form sends no update for it.
    let requested = checked_run(
        &body.sql,
        body.notebook.as_ref(),
        body.tasks.as_deref(),
        &body.params,
        notebooks_store,
        &group,
        body.publish,
    )
    .await?;
    match (stored_shape(&resource), &requested) {
        // Not Nova's shape at all; the form never edits it.
        (StoredShape::Other, _) => {
            return Err(ActionError::refused("shape", shape_not_editable()));
        }
        (StoredShape::Tasks, Statement::Tasks(_)) => {}
        (StoredShape::Tasks, _) => {
            return Err(ActionError::refused(
                "shape",
                Fail::new(StatusCode::CONFLICT, SEVERAL_TASKS),
            ));
        }
        (StoredShape::Run("notebook"), Statement::Notebook { .. }) => {}
        (StoredShape::Run("notebook"), _) => {
            return Err(ActionError::refused(
                "kind",
                Fail::new(StatusCode::BAD_REQUEST, NOTEBOOK_PIPELINE),
            ));
        }
        (StoredShape::Run(_), Statement::Sql(_)) => {}
        (StoredShape::Run(_), _) => {
            return Err(ActionError::refused(
                "kind",
                Fail::new(StatusCode::BAD_REQUEST, STATEMENT_PIPELINE),
            ));
        }
    }

    // Asking for `wap` with a task list is the same rule as on create (#114).
    if body.tasks.is_some() {
        check_publish(body.publish, body.tasks.as_deref())?;
    }

    let options = body
        .options
        .clone()
        .unwrap_or_default()
        .resolve()
        .map_err(|message| {
            ActionError::refused("options", Fail::new(StatusCode::BAD_REQUEST, message))
        })?;
    let schedule = validate_schedule(body.schedule.as_ref()).map_err(|message| {
        ActionError::refused("schedule", Fail::new(StatusCode::BAD_REQUEST, message))
    })?;

    if let Some(provenance) = &body.provenance {
        check_provenance(provenance).map_err(|message| {
            ActionError::refused("provenance", Fail::new(StatusCode::BAD_REQUEST, message))
        })?;
    }

    // The principal of the pipeline's own group must still be ready, or the change would make a
    // pipeline the loader refuses.
    let principals = resources
        .principals()
        .await
        .map_err(|e| ActionError::error(resource_fail(e)))?;
    let principal = principal_view(&principals, &group);
    if !principal.ready {
        return Err(ActionError::refused(
            "no-principal",
            Fail::new(StatusCode::CONFLICT, principal.message.unwrap_or_default()),
        ));
    }

    let mut updated = updated_resource(
        &resource,
        &requested,
        &options,
        &body.params,
        schedule.as_ref(),
        body.notify.as_ref(),
        body.viewers.as_deref(),
        description.as_deref(),
        body.publish,
    );
    if let Some(provenance) = &body.provenance {
        stamp_provenance(&mut updated, provenance, &who.display_name);
    }

    // The resource still carries the `resourceVersion` read above: a concurrent change is a
    // conflict, not a silent overwrite.
    let written = resources
        .replace(name, updated)
        .await
        .map_err(|e| match e {
            ResourceError::Conflict => ActionError::refused("conflict", pipeline_changed()),
            other => ActionError::error(resource_write_fail(other)),
        })?;
    Ok(view_after_write(airflow, namespace, name, &written, who).await)
}

/// `DELETE /api/pipelines/{namespace}/{name}`: remove a pipeline Nova wrote. Airflow is not
/// called — the resource is the source of truth, and the loader removes the DAG.
async fn delete_pipeline(
    resources: Option<&PipelineResources>,
    who: &Identity,
    namespace: &str,
    name: &str,
) -> Result<(), Fail> {
    let result = delete_pipeline_inner(resources, who, namespace, name).await;
    match &result {
        Ok(()) => audit::pipeline_event("delete", who, namespace, name, None, "ok", None),
        Err(e) => audit::pipeline_event("delete", who, namespace, name, None, &e.outcome, None),
    }
    result.map_err(|e| e.fail)
}

async fn delete_pipeline_inner(
    resources: Option<&PipelineResources>,
    who: &Identity,
    namespace: &str,
    name: &str,
) -> Result<(), ActionError> {
    let resources = resources
        .ok_or_else(no_cluster)
        .map_err(ActionError::error)?;
    let (_, resource) = load_actionable(resources, who, namespace, name)
        .await
        .map_err(ActionError::load_refusal)?;
    if !nova_managed(&resource) {
        return Err(ActionError::refused("not-nova", managed_outside()));
    }
    resources
        .delete(name)
        .await
        .map_err(|e| ActionError::error(resource_write_fail(e)))
}

// ---------------------------------------------------------------------------
// The statement check and the next-runs preview (#223): no resource, no store.
// ---------------------------------------------------------------------------

/// The engine's `source` for the Schedule dialog's statement check. Trino records it in the
/// query events, so a check reads differently from a real query or a pipeline run.
const SOURCE_SCHEDULE_CHECK: &str = "nova-schedule-check";

/// A signed-in caller for the two routes that touch no `Pipeline` and no secret store. They
/// still refuse an anonymous caller: in `oidc` mode `require_auth` refuses first, and a router
/// mounted without that middleware — as the tests mount one — answers the same 401.
pub(crate) struct Caller {
    state: Arc<AppState>,
    principal: Principal,
}

impl Caller {
    pub(crate) fn state(&self) -> &AppState {
        &self.state
    }

    pub(crate) fn principal(&self) -> &Principal {
        &self.principal
    }
}

impl FromRequestParts<Arc<AppState>> for Caller {
    type Rejection = Fail;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let principal = parts
            .extensions
            .get::<Principal>()
            .cloned()
            .ok_or_else(|| Fail::new(StatusCode::UNAUTHORIZED, "not signed in"))?;
        Ok(Self {
            state: Arc::clone(state),
            principal,
        })
    }
}

/// The keywords `EXPLAIN` accepts here: what Nova may ask the engine about.
const CHECKABLE_KEYWORDS: [&str; 8] = [
    "select", "with", "insert", "merge", "delete", "update", "values", "create",
];

/// The statement's first word, as written.
fn first_keyword(statement: &str) -> &str {
    let start = statement.trim_start();
    let end = start
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(start.len());
    &start[..end]
}

/// Whether the engine can plan this statement. `create` only when it creates from a query
/// (`CREATE … AS …`): a bare `CREATE TABLE t (a int)` has no plan, and `EXPLAIN` refuses it.
fn checkable(statement: &str) -> bool {
    let keyword = first_keyword(statement);
    if !CHECKABLE_KEYWORDS
        .iter()
        .any(|checkable| keyword.eq_ignore_ascii_case(checkable))
    {
        return false;
    }
    if keyword.eq_ignore_ascii_case("create") {
        return statement.to_ascii_lowercase().contains(" as ");
    }
    true
}

/// `POST /api/pipelines/check`: fill the placeholders and ask the engine to *plan* the
/// statement — `EXPLAIN`, as the caller, under the check's own source. It never sends
/// `EXPLAIN ANALYZE` and never the statement itself, so nothing runs and nothing is written.
/// A statement kind that cannot be explained answers `checked: false`.
async fn check_statement(
    state: &AppState,
    caller: &Principal,
    body: CheckStatementRequest,
) -> Result<CheckStatementResponse, Fail> {
    let statement =
        validate_sql(&body.sql).map_err(|message| Fail::new(StatusCode::BAD_REQUEST, message))?;
    validate_params(&body.params).map_err(|message| Fail::new(StatusCode::BAD_REQUEST, message))?;
    check_placeholders(&statement, &body.params)
        .map_err(|message| Fail::new(StatusCode::BAD_REQUEST, message))?;
    if !checkable(&statement) {
        return Ok(CheckStatementResponse {
            checked: false,
            ok: None,
            message: None,
        });
    }
    let today = now_utc().format("%Y-%m-%d").to_string();
    let sql = format!(
        "EXPLAIN {}",
        fill_placeholders(&statement, &today, "check", &body.params)
    );
    match crate::trino::execute_from(state, sql, Some(caller), SOURCE_SCHEDULE_CHECK).await {
        Ok(_) => Ok(CheckStatementResponse {
            checked: true,
            ok: Some(true),
            message: None,
        }),
        // The engine did not answer at all: 502, like every other unreachable upstream (U12).
        Err(message) if message == crate::trino::ENGINE_UNREACHABLE => {
            Err(Fail::new(StatusCode::BAD_GATEWAY, message))
        }
        // A refused statement is the engine's sentence, not a Nova error.
        Err(message) => Ok(CheckStatementResponse {
            checked: true,
            ok: Some(false),
            message: Some(message),
        }),
    }
}

/// `POST /api/pipelines/preview-schedule`: the next `count` fire times of a cron in its zone,
/// RFC 3339 with that zone's offset. `now` is a parameter so the tests can pin the clock.
fn preview_schedule(
    body: PreviewScheduleRequest,
    now: DateTime<Utc>,
) -> Result<PreviewScheduleResponse, Fail> {
    let schedule = ScheduleRequest {
        cron: body.cron,
        timezone: body.timezone,
    };
    let (cron, timezone) = validate_schedule(Some(&schedule))
        .map_err(|message| Fail::new(StatusCode::BAD_REQUEST, message))?
        .expect("a schedule was given");
    let count = body.count.unwrap_or(5);
    if !(1..=10).contains(&count) {
        return Err(Fail::new(StatusCode::BAD_REQUEST, COUNT_RANGE));
    }
    let zone: Tz = timezone
        .parse()
        .expect("validate_schedule checked the zone");
    let parsed = parse_cron(&cron).expect("validate_schedule checked the cron");
    Ok(PreviewScheduleResponse {
        runs: next_runs(&parsed, zone, count, now)
            .into_iter()
            .map(|run| run.to_rfc3339())
            .collect(),
    })
}

// ---------------------------------------------------------------------------
// The handlers: sign-in, the two clients, then one of the functions above.
// ---------------------------------------------------------------------------

/// The pipeline page is never cacheable, including the refusals an extractor produces before a
/// handler runs.
pub(crate) async fn no_store(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// The deployed pipelines of this stage, reduced for the stage comparison (`stages.rs`):
/// only the ones `stardelt bundle deploy` stamped, and only those the caller may see.
pub(crate) async fn deployed_rows(
    state: &AppState,
    who: &Identity,
) -> Result<Vec<crate::stages::StageRow>, String> {
    let resources = resources(state).ok_or("this Nova has no cluster connection")?;
    let list = resources.list().await.map_err(|e| match e {
        ResourceError::Unavailable(message) => message,
        _ => "the cluster did not answer".to_string(),
    })?;
    Ok(list
        .iter()
        .filter(|resource| visible(who, spec_of(resource)))
        .filter_map(|resource| {
            let deployment = deployment_of(resource)?;
            Some(crate::stages::StageRow {
                bundle: deployment.bundle,
                target: deployment.target,
                name: pipeline_name(resource).to_string(),
                revision: deployment.revision,
                repository: deployment.repository,
                modified: deployment.modified,
            })
        })
        .collect())
}

/// The resource client for this request; `None` without a cluster connection.
pub(crate) fn resources(state: &AppState) -> Option<PipelineResources> {
    state.kube.clone().map(PipelineResources::cluster)
}

/// The read, action and write routes, all uncacheable.
pub(crate) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/pipelines", get(list).post(create))
        // The Runs page (#516): every run of the caller's pipelines, newest first.
        .route("/runs", get(list_runs))
        // The notebook page's "where it is used": the pipelines that run one published notebook (#517).
        .route("/notebooks/{group}/{name}/usage", get(notebook_usage))
        // The caller's groups and their pipeline principals, before any `{namespace}/{name}`
        // route can see the word "principals" as a name.
        .route("/pipelines/principals", get(principals))
        // The caller's service principals (operator#63): a group's jobs, maintenance and
        // collector identities, which the Schedule dialog's `pipeline-<group>` check does not show.
        .route("/service-principals", get(service_principals))
        // The Schedule dialog's two helpers (#223): no resource and no store, but a signed-in
        // caller; placed before `{namespace}/{name}` like `principals`.
        .route("/pipelines/check", post(check))
        .route("/pipelines/preview-schedule", post(preview))
        .route("/pipelines/validate", post(validate))
        .route(
            "/pipelines/{namespace}/{name}",
            get(detail).put(update).delete(remove),
        )
        .route("/pipelines/{namespace}/{name}/spec", get(spec))
        .route("/pipelines/{namespace}/{name}/runs", post(trigger_run))
        .route("/pipelines/{namespace}/{name}/pause", post(pause))
        .route("/pipelines/{namespace}/{name}/resume", post(resume))
        .route("/pipelines/{namespace}/{name}/runs/{run_id}", get(run))
        .route(
            "/pipelines/{namespace}/{name}/runs/{run_id}/queries",
            get(run_queries),
        )
        .route(
            "/pipelines/{namespace}/{name}/runs/{run_id}/checks",
            get(checks),
        )
        .route(
            "/pipelines/{namespace}/{name}/runs/{run_id}/stop",
            post(stop),
        )
        .route(
            "/pipelines/{namespace}/{name}/runs/{run_id}/retry",
            post(retry),
        )
        .route(
            "/pipelines/{namespace}/{name}/runs/{run_id}/tasks/{task_id}/logs",
            get(logs),
        )
        .route(
            "/pipelines/{namespace}/{name}/runs/{run_id}/tasks/{task_id}/notebook",
            get(run_notebook),
        )
        .route_layer(axum::middleware::from_fn(no_store))
}

pub(crate) fn answer<T: Serialize>(result: Result<T, Fail>) -> Response {
    answer_with(StatusCode::OK, result)
}

pub(crate) fn answer_with<T: Serialize>(status: StatusCode, result: Result<T, Fail>) -> Response {
    match result {
        Ok(value) => (status, Json(value)).into_response(),
        Err(e) => e.into_response(),
    }
}

/// Demo mode has no principal and no store, so `SignedIn` refuses every read before the
/// handler runs (#500). This lets exactly the read routes that have demo samples through.
pub(crate) enum ReadCaller {
    Demo,
    /// Boxed: `SignedIn` is 208 bytes and `clippy::large_enum_variant` is a gate.
    Signed(Box<SignedIn>),
}

impl FromRequestParts<Arc<AppState>> for ReadCaller {
    type Rejection = Fail;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        if state.demo {
            return Ok(ReadCaller::Demo);
        }
        SignedIn::from_request_parts(parts, state)
            .await
            .map(|signed| ReadCaller::Signed(Box::new(signed)))
    }
}

#[utoipa::path(
    get,
    operation_id = "pipelines_list",
    path = "/api/pipelines",
    tag = "public",
    description = "The pipelines the caller may see: their groups' pipelines, and every pipeline for an administrator. A pipeline without `spec.ownerGroup` is administrators-only. `state` is one of `loading`, `paused`, `running`, `failed`, `succeeded`, `idle`; `loaded` is false while Airflow has no DAG for a new pipeline yet. Answers `Cache-Control: no-store`.",
    responses(
        (status = 200, description = "The visible pipelines, sorted by name", body = PipelineListResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The scheduler or the cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the scheduler is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn list(caller: ReadCaller) -> Response {
    let signed = match caller {
        ReadCaller::Demo => return answer(Ok(crate::demo::pipelines())),
        ReadCaller::Signed(signed) => signed,
    };
    answer(
        list_pipelines(
            resources(signed.state()).as_ref(),
            signed.state().airflow.as_ref(),
            signed.identity(),
        )
        .await,
    )
}

#[utoipa::path(
    get,
    operation_id = "runs_list",
    path = "/api/runs",
    tag = "public",
    description = "The runs of the caller's visible pipelines, newest first. A run carries the pipeline's `namespace`, `name` and `ownerGroup` and a `kind` of `sql`, `notebook` or `pipeline` (a multi-step pipeline). `limit` defaults to 50 and is at most 200. `state`, `kind`, `group`, `pipeline` (`<namespace>/<name>`) and `notebook` (`<group>/<name>`) narrow the answer; an unknown state answers no rows rather than an error. A `notebook` filter is owner-group scoped: a caller who is not a member of the group answers 403. A failed scheduler read for one pipeline is logged and skipped, not reported. Answers `Cache-Control: no-store`.",
    params(
        ("limit" = Option<usize>, Query, description = "Rows to answer; 50 when absent, at most 200"),
        ("state" = Option<String>, Query, description = "Exact match on the run's state; an unknown state answers no rows"),
        ("kind" = Option<String>, Query, description = "`sql`, `notebook` or `pipeline`"),
        ("group" = Option<String>, Query, description = "Exact match on the pipeline's `spec.ownerGroup`"),
        ("pipeline" = Option<String>, Query, description = "One pipeline as `<namespace>/<name>`"),
        ("notebook" = Option<String>, Query, description = "One published notebook as `<group>/<name>`: only the runs of its schedules"),
    ),
    responses(
        (status = 200, description = "The visible runs, newest first", body = RunsListResponse),
        (status = 400, description = "`limit`, `kind`, `pipeline` or `notebook` is invalid", body = crate::openapi::ErrorResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller is not a member of the notebook's group", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The scheduler or the cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the scheduler is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn list_runs(
    caller: ReadCaller,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let signed = match caller {
        // Demo mode has no scheduler: the sample is filtered and capped like a real answer, so
        // the page's filters and its 400s behave the same way.
        ReadCaller::Demo => return answer(demo_runs(&query)),
        ReadCaller::Signed(signed) => signed,
    };
    answer(
        runs_across_pipelines(
            resources(signed.state()).as_ref(),
            signed.state().airflow.as_ref(),
            signed.identity(),
            &query,
        )
        .await,
    )
}

/// The hosted MCP tool `pipeline_list` (#114 read slice): `GET /api/pipelines` for a caller
/// the MCP endpoint has already authenticated. Same visibility rule, same answer.
pub(crate) async fn list_for(state: &AppState, who: &Identity) -> Response {
    answer(list_pipelines(resources(state).as_ref(), state.airflow.as_ref(), who).await)
}

/// The hosted MCP tool `pipeline_runs`: `GET /api/runs` with the same filters, as the caller.
pub(crate) async fn runs_for(
    state: &AppState,
    who: &Identity,
    query: &BTreeMap<String, String>,
) -> Response {
    answer(
        runs_across_pipelines(
            resources(state).as_ref(),
            state.airflow.as_ref(),
            who,
            query,
        )
        .await,
    )
}

/// The hosted MCP tool `pipeline_run`: `GET /api/pipelines/{ns}/{name}/runs/{run_id}`, as the caller.
pub(crate) async fn run_for(
    state: &AppState,
    who: &Identity,
    namespace: &str,
    name: &str,
    run_id: &str,
) -> Response {
    answer(
        run_detail(
            resources(state).as_ref(),
            state.airflow.as_ref(),
            who,
            namespace,
            name,
            run_id,
        )
        .await,
    )
}

/// The hosted MCP tool `pipeline_task_log`: one page of a task's log, as the caller. Audited by `task_logs`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn task_log_for(
    state: &AppState,
    who: &Identity,
    namespace: &str,
    name: &str,
    run_id: &str,
    task_id: &str,
    try_number: Option<u32>,
    token: Option<&str>,
) -> Response {
    answer(
        task_logs(
            resources(state).as_ref(),
            state.airflow.as_ref(),
            who,
            namespace,
            name,
            run_id,
            task_id,
            try_number,
            token,
        )
        .await,
    )
}

/// Demo mode's `GET /api/runs`: the sample is filtered and capped like a real answer, so the
/// page's filters and its 400s behave the same way. There is no notebook schedule in demo mode,
/// so a `notebook` filter answers no rows — the same empty list the notebook page's usage route
/// answers there.
fn demo_runs(query: &BTreeMap<String, String>) -> Result<RunsListResponse, Fail> {
    let filter = RunsFilter::parse(query)?;
    let sample = crate::demo::runs();
    if filter.notebook.is_some() {
        return Ok(RunsListResponse {
            runs: Vec::new(),
            groups: sample.groups,
        });
    }
    Ok(filter.answer(sample.runs, sample.groups))
}

/// Whether this Pipeline is a Schedule of one published notebook: the owner group, the one-task
/// shape a Schedule writes (#516), `kind: notebook` and `config.notebook == name`.
fn notebook_schedule(resource: &Value, group: &str, notebook: &str) -> bool {
    let spec = spec_of(resource);
    if spec.get("ownerGroup").and_then(Value::as_str) != Some(group) {
        return false;
    }
    let Some(task) = spec
        .get("tasks")
        .and_then(Value::as_array)
        .and_then(|tasks| tasks.first())
    else {
        return false;
    };
    task.get("kind").and_then(Value::as_str) == Some("notebook")
        && task
            .get("config")
            .and_then(|config| config.get("notebook"))
            .and_then(Value::as_str)
            == Some(notebook)
}

/// The visible pipelines whose single task is `kind: notebook` with `config.notebook == name`,
/// owned by `group`. Administrators get no wider view than a member: the notebook page is
/// owner-group scoped (the rule `notebooks.rs` documents).
fn notebook_usage_rows(
    resources: &[Value],
    who: &Identity,
    group: &str,
    name: &str,
) -> Vec<NotebookScheduleRow> {
    let mut rows: Vec<NotebookScheduleRow> = resources
        .iter()
        .filter(|resource| visible(who, spec_of(resource)))
        .filter(|resource| notebook_schedule(resource, group, name))
        .filter_map(|resource| {
            let spec = spec_of(resource);
            let config = spec.get("tasks")?.as_array()?.first()?.get("config")?;
            Some(NotebookScheduleRow {
                namespace: crate::cluster::namespace(),
                name: pipeline_name(resource).to_string(),
                owner_group: group.to_string(),
                object_version: config.get("objectVersion")?.as_str()?.to_string(),
                cron: spec
                    .get("schedule")
                    .and_then(|s| s.get("cron"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                timezone: spec
                    .get("schedule")
                    .and_then(|s| s.get("timezone"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
                run_as: spec
                    .get("runAs")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
        })
        .collect();
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    rows
}

#[utoipa::path(
    get,
    operation_id = "notebooks_usage",
    path = "/api/notebooks/{group}/{name}/usage",
    tag = "public",
    description = "The pipelines that run one published notebook — the schedules and on-demand pipelines of that group, sorted by name. Each row is the one-task `Pipeline` a Schedule created (#516): its pinned `objectVersion`, its cron and timezone when it has a schedule, and its `runAs`. Owner-group scoped: a caller who is not a member of `group` answers 403, and an administrator gets no wider view. Answers `Cache-Control: no-store`.",
    params(
        ("group" = String, Path, description = "The notebook's owner group"),
        ("name" = String, Path, description = "The notebook's `.ipynb` file name"),
    ),
    responses(
        (status = 200, description = "The visible schedules, sorted by name", body = NotebookUsageResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller is not a member of the group", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn notebook_usage(
    caller: ReadCaller,
    Path((group, name)): Path<(String, String)>,
) -> Response {
    let signed = match caller {
        ReadCaller::Demo => {
            return answer(Ok(NotebookUsageResponse {
                schedules: Vec::new(),
            }));
        }
        ReadCaller::Signed(signed) => *signed,
    };
    if !signed.identity().groups.iter().any(|g| g == &group) {
        return crate::notebooks::forbidden_group(&group).into_response();
    }
    if !crate::notebooks::valid_group(&group) {
        return Fail::new(
            StatusCode::BAD_REQUEST,
            "the group name cannot be used as a notebook prefix",
        )
        .into_response();
    }
    if !crate::notebooks::valid_name(&name) {
        return Fail::new(StatusCode::BAD_REQUEST, "a notebook name ends in .ipynb")
            .into_response();
    }
    let Some(resources) = resources(signed.state()) else {
        return no_cluster().into_response();
    };
    match resources.list().await {
        Ok(list) => answer(Ok(NotebookUsageResponse {
            schedules: notebook_usage_rows(&list, signed.identity(), &group, &name),
        })),
        Err(error) => resource_fail(error).into_response(),
    }
}

#[utoipa::path(
    get,
    operation_id = "pipelines_principals",
    path = "/api/pipelines/principals",
    tag = "public",
    description = "The caller's own groups and the pipeline principal of each. `principal` is `pipeline-<group>` when a `ServicePrincipal` of that name with `spec.ownerGroup` equal to the group exists; `ready` is its `status.ready`. `message` is `null` when it is ready, otherwise the `Ready` condition's message (or `no status yet`), and for a group without a principal the `make sync-principal NAME=pipeline-<group> GROUP=<group>` command that creates one. A group without a ready principal cannot schedule. Answers `Cache-Control: no-store`.",
    responses(
        (status = 200, description = "One entry per group of the caller, sorted by group", body = PrincipalsResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn principals(caller: ReadCaller) -> Response {
    let signed = match caller {
        ReadCaller::Demo => return answer(Ok(crate::demo::pipeline_principals())),
        ReadCaller::Signed(signed) => signed,
    };
    answer(principal_views(resources(signed.state()).as_ref(), signed.identity()).await)
}

#[utoipa::path(
    get,
    operation_id = "service_principals_list",
    path = "/api/service-principals",
    tag = "public",
    description = "The service principals the caller may see: every `ServicePrincipal` of their groups, and all of them for an administrator. `purpose` is the resource's `spec.purpose` (default `pipeline`); `audiences` are the resource's audiences; `workloadIdentity` is `status.workloadIdentity` — the catalog principal `kubernetes~<ServiceAccount uid>` (#492) — and `clientId` is `status.clientId`; both are absent until the operator has written them. `ready` is `status.ready`; `message` is `null` when ready, otherwise the `Ready` condition's message, or `no status yet`. Credentials are never part of an answer. Answers `Cache-Control: no-store`.",
    responses(
        (status = 200, description = "The caller's principals, sorted by name", body = ServicePrincipalsResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn service_principals(caller: ReadCaller) -> Response {
    let signed = match caller {
        ReadCaller::Demo => return answer(Ok(crate::demo::service_principals())),
        ReadCaller::Signed(signed) => signed,
    };
    answer(service_principal_views(resources(signed.state()).as_ref(), signed.identity()).await)
}

#[utoipa::path(
    post,
    operation_id = "pipelines_check",
    path = "/api/pipelines/check",
    tag = "public",
    description = "What the engine says about a statement, without running it: Nova fills the statement's `${run.date}` (today, UTC), `${run.id}` (`check`) and `${params.<name>}` placeholders and sends `EXPLAIN <statement>` to the engine as the caller, under the source `nova-schedule-check`. It never sends `EXPLAIN ANALYZE` and never the statement itself. `checked: false` means this kind of statement cannot be checked here (only `select`, `with`, `insert`, `merge`, `delete`, `update`, `values` and `create … as …` are). A refused statement answers 200 with `ok: false` and the engine's sentence; an engine that does not answer is 502. Refusals before the engine: an empty, over-20,000-character or NUL-carrying statement 400, a parameter table or a placeholder as on create 400. Answers `Cache-Control: no-store`.",
    request_body = CheckStatementRequest,
    responses(
        (status = 200, description = "The engine's verdict, or that this statement cannot be checked", body = CheckStatementResponse),
        (status = 400, description = "The statement, a parameter or a placeholder is invalid", body = crate::openapi::ErrorResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The query engine is not reachable", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn check(
    caller: Caller,
    ApiJson(body): ApiJson<CheckStatementRequest>,
) -> Response {
    answer(check_statement(caller.state(), caller.principal(), body).await)
}

#[utoipa::path(
    post,
    operation_id = "pipelines_preview_schedule",
    path = "/api/pipelines/preview-schedule",
    tag = "public",
    description = "The next fire times of a cron in its zone, RFC 3339 with that zone's offset (for example `2026-10-21T03:00:00+02:00`), so the Schedule dialog can show what a cadence means before it is saved. A field or a zone as on create (400 with the same sentences); `count` is 1 to 10, 5 when omitted. Like croniter, a day matches when both day fields are restricted and either matches, and when one is unrestricted and the other matches. A wall-clock time the zone skips at a DST jump is not a run; a time that happens twice is answered with its first occurrence. A cron that can never fire answers fewer runs (at most 366 days are searched), not an error. Answers `Cache-Control: no-store`.",
    request_body = PreviewScheduleRequest,
    responses(
        (status = 200, description = "The next runs in the schedule's zone", body = PreviewScheduleResponse),
        (status = 400, description = "The cron, the timezone or the count is invalid", body = crate::openapi::ErrorResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn preview(
    _caller: Caller,
    ApiJson(body): ApiJson<PreviewScheduleRequest>,
) -> Response {
    answer(preview_schedule(body, now_utc()))
}

/// The document the publishing routes show as their size in #761: a scheduled statement.
const EXAMPLE_STATEMENT: &str = "select count(*) from gold.shop.orders";

/// The body `pipelines_create` and `pipelines_validate` take (#761), one kept in step with
/// [`CreatePipelineRequest`]: a test deserializes it, and every key must be a property of the
/// schema.
fn create_request_example() -> serde_json::Value {
    serde_json::json!({
        "name": "shop-marts",
        "ownerGroup": "shop",
        "description": "Rebuilds the shop's marts for the dashboard.",
        "tasks": [{"name": "run", "sql": EXAMPLE_STATEMENT}],
        "schedule": {"cron": "0 3 * * *", "timezone": "UTC"}
    })
}

/// The `PipelineView` a create answers (#761).
fn pipeline_view_example() -> serde_json::Value {
    serde_json::json!({
        "name": "shop-marts",
        "namespace": "stardelt",
        "ownerGroup": "shop",
        "canRun": true,
        "canEdit": true,
        "schedule": "0 3 * * *",
        "paused": false,
        "loaded": false,
        "state": "loading",
        "tasks": [{"name": "run", "kind": "sql", "sql": EXAMPLE_STATEMENT}],
        "concurrency": "Forbid",
        "createdAt": "2026-11-20T09:12:04Z"
    })
}

/// The verdict `pipelines_validate` answers for [`create_request_example`] (#761).
fn validate_response_example() -> serde_json::Value {
    serde_json::json!({
        "ok": true,
        "action": "create",
        "errors": [],
        "changes": [],
        "nextRuns": [
            "2026-11-21T03:00:00Z",
            "2026-11-22T03:00:00Z",
            "2026-11-23T03:00:00Z",
            "2026-11-24T03:00:00Z",
            "2026-11-25T03:00:00Z"
        ]
    })
}

/// The document `pipelines_spec` answers (#761), in the shape the create body takes.
fn spec_response_example() -> serde_json::Value {
    serde_json::json!({
        "document": {
            "name": "shop-marts",
            "ownerGroup": "shop",
            "description": "Rebuilds the shop's marts for the dashboard.",
            "tasks": [{"name": "run", "sql": EXAMPLE_STATEMENT}],
            "schedule": {"cron": "0 3 * * *", "timezone": "UTC"},
            "options": {
                "retries": 1,
                "backoffMinutes": 5,
                "timeoutMinutes": 120,
                "concurrency": "Forbid"
            }
        },
        "resourceVersion": "184931",
        "managedBy": "nova",
        "editable": true
    })
}

#[utoipa::path(
    post,
    operation_id = "pipelines_create",
    path = "/api/pipelines",
    tag = "public",
    description = "Schedule a statement or a published notebook: create a `Pipeline` for one of the caller's owner groups (`runAs: pipeline-<group>`), validated so the loader accepts it. A body gives exactly one of a non-empty `sql` and a `notebook` (`{ name, objectVersion, params }`, #237) — both or neither is 400 `give a statement, a notebook, a dbt run or a quality run`. A body may instead give `tasks` (`[{ name, sql | notebook | dbt | quality, dependsOn }]`, 1 to 30 tasks, each with exactly one source, at most 8 dependencies): `tasks` together with `sql` or `notebook` is 400 `give a statement, a notebook or a task list`; a task name outside `^[a-z0-9][a-z0-9_-]{0,62}$`, a name used twice, more than 30 tasks, a dependency on itself or on a task of another pipeline, and a cycle are 400 with a sentence that names the task. Refusals, in this order: a name outside 1 to 40 lower-case letters, digits and dashes starting with a letter 400; a group the caller is not a member of 403 (`you are not a member of the group <g>` — administrators included); a group without a ready `ServicePrincipal` 409 with the `principals` route's message; an empty, over-20,000-character or NUL-carrying statement 400; a parameter name, a parameter count or value, or a `${…}` placeholder the loader cannot fill 400; a notebook name outside `^[A-Za-z0-9][A-Za-z0-9._ -]{0,127}\\.ipynb$` 400, more than 32 notebook parameters, a value over 256 characters or NUL, or a `params` JSON text over 4096 characters 400, and a version that is not in the store 400 `that notebook version does not exist in group <g>` (503 when `NOVA_S3_*` is unset, 502 when S3 does not answer); a retry, wait, time-limit or run-mode option out of range 400; a cron that is not five in-range fields or fires more often than every five minutes 400; an unknown IANA timezone 400; a name that already exists 409; more than ten creations per user per hour 429. `options` and `params` are optional: an omitted option keeps its default (1 retry after 5 minutes, a 120-minute time limit, `Forbid`); `params` are the statement's `${params.<name>}` values, a notebook's values are in `notebook.params`. Airflow is not needed: the resource is the source of truth, and the answer is the new pipeline with `loaded: false`, `state: \"loading\"`. In a stage whose NOVA_STAGE_KIND is prod, a body that schedules a notebook is refused with 400 \"In production a notebook comes from Git. Deploy it with a bundle.\" unless it carries a bundle deploy stamp (provenance, or one already on the pipeline); other stages and statement schedules are unaffected. The audit line carries the statement's length, the cron and, for a notebook, the group, the file name and the pinned version — never the statement and never a parameter value. Answers `Cache-Control: no-store`.",
    request_body(content = CreatePipelineRequest, example = json!(create_request_example())),
    responses(
        (status = 201, description = "The resource as written", body = PipelineView, example = json!(pipeline_view_example())),
        (status = 400, description = "The name, the statement, the cron or the timezone is invalid", body = crate::openapi::ErrorResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller is not a member of the owner group", body = crate::openapi::ErrorResponse),
        (status = 409, description = "No ready principal for the group, or the name is taken", body = crate::openapi::ErrorResponse),
        (status = 429, description = "More than ten creations in the last hour", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn create(
    signed: SignedIn,
    ApiJson(body): ApiJson<CreatePipelineRequest>,
) -> Response {
    if let Some(sentence) = production_notebook_sentence(
        signed.state().stage_kind,
        body.schedule.is_some(),
        runs_a_notebook(body.notebook.as_ref(), body.tasks.as_deref()),
        body.provenance.is_some(),
    ) {
        return Fail::new(StatusCode::BAD_REQUEST, sentence).into_response();
    }
    answer_with(
        StatusCode::CREATED,
        create_pipeline(
            resources(signed.state()).as_ref(),
            signed.state().db.as_ref(),
            signed.state().notebooks.as_ref(),
            &signed.state().pipeline_creates,
            signed.identity(),
            body,
        )
        .await,
    )
}

#[utoipa::path(
    post,
    operation_id = "pipelines_validate",
    path = "/api/pipelines/validate",
    tag = "public",
    description = "Check a pipeline document without writing it: the body is the body of `pipelines_create` (the document `stardelt apply` reads). It runs every rule a create runs (the name, the caller's own group, the group's ready principal, exactly one of a statement and a notebook with the rules of its kind, the options, the schedule) and then looks at the name: free means `create`; a pipeline Nova wrote that the caller may act on, in the same group, means `update`; a pipeline written outside Nova, someone else's, or in another group is a problem. A body may instead give `tasks` (`[{ name, sql | notebook, dependsOn }]`, 1 to 30 tasks, each with exactly one of the two, at most 8 dependencies): `tasks` together with `sql` or `notebook` is 400 `give a statement, a notebook or a task list`; a task name outside `^[a-z0-9][a-z0-9_-]{0,62}$`, a name used twice, more than 30 tasks, a dependency on itself or on a task of another pipeline, and a cycle are 400 with a sentence that names the task. A rule that refuses is an answer, `ok: false` with one sentence in `errors`, status 200; nothing is written and no creation slot is taken. An accepted document with a schedule lists its next five fire times. In a stage whose NOVA_STAGE_KIND is prod, a body that schedules a notebook is refused with `ok: false` at 200 (\"In production a notebook comes from Git. Deploy it with a bundle.\") unless it carries a bundle deploy stamp (provenance, or one already on the pipeline); other stages and statement schedules are unaffected. A cluster or notebook store that does not answer is 502 or 503 as on create. Answers `Cache-Control: no-store`.",
    request_body(content = CreatePipelineRequest, example = json!(create_request_example())),
    responses(
        (status = 200, description = "The verdict: accepted (with the action) or the reason it is not", body = ValidatePipelineResponse, example = json!(validate_response_example())),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn validate(
    signed: SignedIn,
    ApiJson(body): ApiJson<CreatePipelineRequest>,
) -> Response {
    if let Some(sentence) = production_notebook_sentence(
        signed.state().stage_kind,
        body.schedule.is_some(),
        runs_a_notebook(body.notebook.as_ref(), body.tasks.as_deref()),
        body.provenance.is_some(),
    ) {
        return answer(Ok(not_ok(sentence)));
    }
    answer(
        validate_pipeline(
            resources(signed.state()).as_ref(),
            signed.state().db.as_ref(),
            signed.state().notebooks.as_ref(),
            signed.identity(),
            body,
        )
        .await,
    )
}

#[utoipa::path(
    put,
    operation_id = "pipelines_update",
    path = "/api/pipelines/{namespace}/{name}",
    tag = "public",
    description = "Change a pipeline Nova created (`app.kubernetes.io/managed-by: stardelt-nova`) and nothing else. A statement pipeline's body carries `sql`; a notebook pipeline's carries `notebook` (`{ name, objectVersion, params }`), and an update never changes which of the two it is: `sql` on a notebook pipeline is 400 `this pipeline runs a notebook`, a notebook on a statement pipeline 400 `this pipeline runs a statement`, and both or neither 400 `give a statement, a notebook, a dbt run or a quality run`. A task-list pipeline's body carries `tasks` and replaces the whole list; any other body on a task list is 409 `this pipeline has several tasks; change it in its file and deploy again`, and `tasks` on a statement or notebook pipeline is 400 `this pipeline runs a statement` / `this pipeline runs a notebook`. The body describes the whole editable state: an omitted `schedule` removes it, an omitted `params` removes the table, an omitted option keeps its default, and a notebook body's file name, version and parameters replace the task's `config`. Refusals: a pipeline the caller cannot see 404, and one it can see but may not change — a viewer (`spec.viewers`) — 403 `only the owner group may change this pipeline`; a pipeline Nova did not write, or one whose task shape is not exactly one `run` task of kind `sql` or `notebook` 409; the statement, a parameter, a placeholder, a notebook name, a parameter value or count, or a version that is not in the store (503 without `NOVA_S3_*`, 502 when S3 does not answer), an option, the cron or the timezone as on create 400; the group's principal no longer ready 409 with the `principals` route's message; a concurrent change to the resource 409 `the pipeline changed while you edited it: reload`. A body may carry `resourceVersion` (the one `pipelines_spec` answered): when it is not the stored one, the answer is that same 409 and nothing is written; omitted, the update behaves as before. In a stage whose NOVA_STAGE_KIND is prod, a body that schedules a notebook is refused with 400 \"In production a notebook comes from Git. Deploy it with a bundle.\" unless it carries a bundle deploy stamp (provenance, or one already on the pipeline); other stages and statement schedules are unaffected. The audit line carries the statement's length, the cron and, for a notebook, the group, the file name and the pinned version — never the statement and never a parameter value. Answers `Cache-Control: no-store`.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
    ),
    request_body = UpdatePipelineRequest,
    responses(
        (status = 200, description = "The resource as written", body = PipelineView),
        (status = 400, description = "The statement, the cron or the timezone is invalid", body = crate::openapi::ErrorResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may see but not change this pipeline", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller", body = crate::openapi::ErrorResponse),
        (status = 409, description = "Managed outside Nova, an uneditable shape, no ready principal, or a conflict", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn update(
    signed: SignedIn,
    Path((namespace, name)): Path<(String, String)>,
    ApiJson(body): ApiJson<UpdatePipelineRequest>,
) -> Response {
    if let Some(fail) =
        update_production_refusal(signed.state(), signed.identity(), &namespace, &name, &body).await
    {
        return fail.into_response();
    }
    answer(
        update_pipeline(
            resources(signed.state()).as_ref(),
            signed.state().db.as_ref(),
            signed.state().notebooks.as_ref(),
            signed.state().airflow.as_ref(),
            signed.identity(),
            &namespace,
            &name,
            body,
        )
        .await,
    )
}

#[utoipa::path(
    delete,
    operation_id = "pipelines_delete",
    path = "/api/pipelines/{namespace}/{name}",
    tag = "public",
    description = "Delete a pipeline Nova created. Only the owner group and administrators may delete: a viewer (`spec.viewers`) answers 403 `only the owner group may change this pipeline`, and a pipeline the caller cannot see 404. A pipeline that carries `app.kubernetes.io/managed-by: stardelt-nova` may be deleted: a `kubectl` or GitOps pipeline answers 409 instead. Airflow is not called — the resource is the source of truth; the DAG leaves the scheduler's default list after about 90 seconds while its history stays readable. Answers 204 with `Cache-Control: no-store`; the attempt is audited.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
    ),
    responses(
        (status = 204, description = "Deleted (or already gone)"),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may see but not change this pipeline", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The pipeline is managed outside Nova", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn remove(
    signed: SignedIn,
    Path((namespace, name)): Path<(String, String)>,
) -> Response {
    match delete_pipeline(
        resources(signed.state()).as_ref(),
        signed.identity(),
        &namespace,
        &name,
    )
    .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => e.into_response(),
    }
}

#[utoipa::path(
    get,
    operation_id = "pipelines_spec",
    path = "/api/pipelines/{namespace}/{name}/spec",
    tag = "public",
    description = "The editable document of one pipeline, in the shape of the body of `pipelines_create`: `name`, `ownerGroup`, a `sql` statement or a `notebook` (file name, pinned object version, parameters), a `schedule` (`cron` and `timezone`; a resource without a zone is `UTC`), the `params` table (left out when empty) and the `options` with their defaults filled in. It is built from the resource alone, so it answers while the scheduler is down. `resourceVersion` is the resource's version; `managedBy` is `nova` or `other` (kubectl or GitOps); `editable` is true only for a pipeline Nova wrote that has one `run` task of kind `sql` or `notebook`, or a task list whose every task is `sql`, `notebook` or `dbt`; an update replaces a task list whole. A pipeline that is not editable is still readable: its document then leaves out what the form cannot show. The document never holds status, annotations, a connection's secret or a token. A pipeline of another group answers 404. Answers `Cache-Control: no-store`.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
    ),
    responses(
        (status = 200, description = "The document, its resource version, who manages it, and whether it is editable", body = PipelineSpecResponse, example = json!(spec_response_example())),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn spec(
    signed: SignedIn,
    Path((namespace, name)): Path<(String, String)>,
) -> Response {
    answer(
        pipeline_spec(
            resources(signed.state()).as_ref(),
            signed.identity(),
            &namespace,
            &name,
        )
        .await,
    )
}

#[utoipa::path(
    get,
    operation_id = "pipelines_get",
    path = "/api/pipelines/{namespace}/{name}",
    tag = "public",
    description = "One pipeline: the list fields, the latest 20 runs, and the resource's `params`, `retries` and `options` (defaults filled in when a field is absent, so the edit dialog can prefill). A pipeline whose DAG does not exist yet answers 200 with `loaded: false` and `state: \"loading\"`. A pipeline of another group answers 404. History of a deleted pipeline is not offered in this version: without the resource the answer is 404.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
    ),
    responses(
        (status = 200, description = "The pipeline, its latest runs, params, retries and options", body = PipelineDetail),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The scheduler or the cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the scheduler is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn detail(
    caller: ReadCaller,
    Path((namespace, name)): Path<(String, String)>,
) -> Response {
    let signed = match caller {
        ReadCaller::Demo => {
            return match crate::demo::pipeline_detail(&namespace, &name) {
                Some(detail) => answer(Ok(detail)),
                None => answer::<PipelineDetail>(Err(no_such_pipeline())),
            };
        }
        ReadCaller::Signed(signed) => *signed,
    };
    answer(
        pipeline_detail(
            resources(signed.state()).as_ref(),
            signed.state().airflow.as_ref(),
            signed.identity(),
            &namespace,
            &name,
        )
        .await,
    )
}

#[utoipa::path(
    get,
    operation_id = "pipelines_run_get",
    path = "/api/pipelines/{namespace}/{name}/runs/{run_id}",
    tag = "public",
    description = "One run of a pipeline with its task instances. The run id is looked up through this pipeline's DAG id, so a run id of another pipeline answers 404. Answers `Cache-Control: no-store`.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
        ("run_id" = String, Path, description = "Airflow's run id, for example `manual__2026-10-20T07:23:50.608876+00:00`"),
    ),
    responses(
        (status = 200, description = "The run and its tasks", body = RunResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller, or no such run", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The scheduler or the cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the scheduler is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn run(
    signed: SignedIn,
    Path((namespace, name, run_id)): Path<(String, String, String)>,
) -> Response {
    answer(
        run_detail(
            resources(signed.state()).as_ref(),
            signed.state().airflow.as_ref(),
            signed.identity(),
            &namespace,
            &name,
            &run_id,
        )
        .await,
    )
}

#[utoipa::path(
    get,
    operation_id = "pipelines_run_queries",
    path = "/api/pipelines/{namespace}/{name}/runs/{run_id}/queries",
    tag = "public",
    description = "The statements one run sent to the query engine, slowest first, read from the `pipeline=` and `run=` client tags the pipeline loader writes. Access is the pipeline's (`load_visible`), not the query log's owner rule: these statements ran as the pipeline's service principal, so the query-log scope would hide them from the person who can see the run. Each answers taskId, startedAt, wallMs, executionMs, inputBytes, inputRows and the statement; with NOVA_QUERY_LOG_REDACT_LITERALS=1 every statement shows '?' in place of its string literals. Answers `Cache-Control: no-store`.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
        ("run_id" = String, Path, description = "Airflow's run id, for example `scheduled__2026-11-14T03:00:00+00:00`"),
    ),
    responses(
        (status = 200, description = "The run's statements, slowest first", body = RunQueriesResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the state store is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn run_queries(
    signed: SignedIn,
    Path((namespace, name, run_id)): Path<(String, String, String)>,
) -> Response {
    answer(run_queries_answer(signed, namespace, name, run_id).await)
}

#[utoipa::path(
    get,
    operation_id = "pipelines_run_checks",
    path = "/api/pipelines/{namespace}/{name}/runs/{run_id}/checks",
    tag = "public",
    description = "Every check a run's `quality` tasks declare, with its verdict: the rows the assertion returned (`0` is a pass) and the severity, read from the task's log line `check <name>: <error|warn>, <n> row(s)`. A check whose task has not run, or whose line is not in the log yet, answers `rows: null, passed: null`. `warn` checks with rows never fail the run. The same visibility rule as the run (404 for another group's pipeline).",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
        ("run_id" = String, Path, description = "Airflow's run id"),
    ),
    responses(
        (status = 200, description = "The checks of the run", body = ChecksResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller, or no such run", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The scheduler or the cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the scheduler is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn checks(
    signed: SignedIn,
    Path((namespace, name, run_id)): Path<(String, String, String)>,
) -> Response {
    answer(
        run_checks(
            resources(signed.state()).as_ref(),
            signed.state().airflow.as_ref(),
            signed.identity(),
            &namespace,
            &name,
            &run_id,
        )
        .await,
    )
}

#[utoipa::path(
    get,
    operation_id = "pipelines_run_task_logs",
    path = "/api/pipelines/{namespace}/{name}/runs/{run_id}/tasks/{task_id}/logs",
    tag = "public",
    description = "One page of a task's log: at most 2000 lines and 512 KiB, ANSI escapes removed, the `::group::` source-details block dropped — its marker, the internal object-store path it wraps and its `::endgroup::` (they are never returned). A failed task's line is followed by the exception the scheduler recorded (`ExcType: message`) and up to 20 of its stack frames, each with the file's last path segment; they count against the same caps. `try` defaults to the task's last attempt; `token` is the `nextToken` of the previous answer. Every read writes one audit line with user, pipeline, run and task — never the content. Answers `Cache-Control: no-store`.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
        ("run_id" = String, Path, description = "Airflow's run id"),
        ("task_id" = String, Path, description = "Task id"),
        ("try" = Option<u32>, Query, description = "Attempt number; defaults to the task's last try"),
        ("token" = Option<String>, Query, description = "Continuation token from a previous answer"),
    ),
    responses(
        (status = 200, description = "The log page", body = LogView),
        (status = 400, description = "`try` is not a positive number", body = crate::openapi::ErrorResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller, no such run, or no such task", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The scheduler or the cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the scheduler is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn logs(
    signed: SignedIn,
    Path((namespace, name, run_id, task_id)): Path<(String, String, String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let try_number = match query.get("try").map(|value| value.parse::<u32>()) {
        None => None,
        Some(Ok(number)) => Some(number),
        Some(Err(_)) => {
            return Fail::new(StatusCode::BAD_REQUEST, "try must be a positive number")
                .into_response();
        }
    };
    answer(
        task_logs(
            resources(signed.state()).as_ref(),
            signed.state().airflow.as_ref(),
            signed.identity(),
            &namespace,
            &name,
            &run_id,
            &task_id,
            try_number,
            query.get("token").map(String::as_str),
        )
        .await,
    )
}

#[utoipa::path(
    get,
    operation_id = "pipelines_run_task_notebook",
    path = "/api/pipelines/{namespace}/{name}/runs/{run_id}/tasks/{task_id}/notebook",
    tag = "public",
    description = "The executed copy of one notebook task run: the notebook with its outputs and tracebacks, as the run wrote it. That copy is the run's log. The same visibility rule as the run (404 for another group's pipeline), the same audit line as a log read, and `Cache-Control: no-store`. `download=true` also names the file `<pipeline>-<task>.ipynb` in `Content-Disposition`.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
        ("run_id" = String, Path, description = "Airflow's run id"),
        ("task_id" = String, Path, description = "Task id: 1 to 64 letters, digits, underscores or dashes"),
        ("download" = Option<bool>, Query, description = "Answer as a file attachment"),
    ),
    responses(
        (status = 200, description = "The executed notebook, as the run wrote it", body = String),
        (status = 400, description = "The task id is not 1 to 64 letters, digits, underscores or dashes, or `download` is not true or false", body = crate::openapi::ErrorResponse),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller, a pipeline without a group, or no executed copy for this task", body = crate::openapi::ErrorResponse),
        (status = 413, description = "The executed notebook is larger than 25 MiB", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The notebook store did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or notebook storage is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn run_notebook(
    signed: SignedIn,
    Path((namespace, name, run_id, task_id)): Path<(String, String, String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let download = match query.get("download").map(String::as_str) {
        None | Some("false") => false,
        Some("true") => true,
        Some(_) => {
            return Fail::new(StatusCode::BAD_REQUEST, "download must be true or false")
                .into_response();
        }
    };
    match task_notebook(
        resources(signed.state()).as_ref(),
        signed.state().notebooks.as_ref(),
        signed.identity(),
        &namespace,
        &name,
        &run_id,
        &task_id,
        download,
    )
    .await
    {
        Ok(response) => response,
        Err(e) => e.into_response(),
    }
}

/// The body of `POST …/runs` (nova#817): absent, or `{}`, starts a run for now with the
/// schedule's parameters, exactly as before. A run date must be `YYYY-MM-DD`; parameters are
/// checked like a saved parameter (`validate_params`), and the names and values are never
/// echoed in the refusal.
fn run_start(body: &[u8]) -> Result<RunStart, Fail> {
    if body.is_empty() {
        return Ok(RunStart::default());
    }
    let start: RunStart = serde_json::from_slice(body)
        .map_err(|_| Fail::new(StatusCode::BAD_REQUEST, "the run body is not valid JSON"))?;
    if let Some(date) = &start.run_date {
        chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .map_err(|_| Fail::new(StatusCode::BAD_REQUEST, "runDate is a date (YYYY-MM-DD)"))?;
    }
    if let Some(params) = &start.params {
        validate_params(params).map_err(|message| Fail::new(StatusCode::BAD_REQUEST, message))?;
    }
    Ok(start)
}

#[utoipa::path(
    post,
    operation_id = "pipelines_run_trigger",
    path = "/api/pipelines/{namespace}/{name}/runs",
    tag = "public",
    description = "Start a run now. A viewer (`spec.viewers`) may run the owner's statement and stop its runs: running cannot change what the pipeline does. Nova refuses instead of passing Airflow's silent behaviour on: a pipeline the scheduler has not loaded yet answers 409, a paused pipeline 409 (Airflow would create a run that waits `queued` until someone resumes), more than ten starts per user per minute 429, and a `Forbid` or `Replace` pipeline whose latest five runs include a queued or running one 409 (`Allow` skips that check). The body is optional: `runDate` (a `YYYY-MM-DD` calendar date, past or future) applies to this run only, as does `params`, this run's values of the schedule's `${params.<name>}` placeholders (at most 16, lower-case names, values up to 256 characters); the stored schedule never changes, and neither value is audited. `logicalDate` stays now, UTC, with microseconds so two starts in one second are two runs and the run still has tasks — the date travels as Airflow's `dag_run.conf` instead. An absent body or `{}` starts a run for now with the schedule's parameters, exactly as before. Every attempt that reaches the run logic, including a refusal, writes one audit line without note text. Answers `Cache-Control: no-store`.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
    ),
    request_body = Option<RunStart>,
    responses(
        (status = 201, description = "The new run, queued", body = RunView),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 400, description = "The body is not JSON, the run date is not `YYYY-MM-DD`, or a parameter is refused", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The pipeline is not loaded, is paused, or already has an active run", body = crate::openapi::ErrorResponse),
        (status = 429, description = "More than ten runs were started in the last minute", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The scheduler or the cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the scheduler is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn trigger_run(
    signed: SignedIn,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let start = match run_start(&body) {
        Ok(start) => start,
        Err(fail) => return fail.into_response(),
    };
    answer_with(
        StatusCode::CREATED,
        run_now(
            resources(signed.state()).as_ref(),
            signed.state().airflow.as_ref(),
            &signed.state().run_starts,
            signed.identity(),
            &namespace,
            &name,
            &start,
        )
        .await,
    )
}

#[utoipa::path(
    post,
    operation_id = "pipelines_pause",
    path = "/api/pipelines/{namespace}/{name}/pause",
    tag = "public",
    description = "Pause a pipeline. Idempotent: pausing a paused pipeline answers 200. Changing a pipeline is the owner group's and administrators' verb: a viewer (`spec.viewers`) may read and run it but not pause it, and answers 403 `only the owner group may change this pipeline` — a pipeline the caller cannot see stays 404. A pipeline the scheduler has not loaded answers 409. Answers the fresh `PipelineView` — the DAG as Airflow returned it after the patch — and `Cache-Control: no-store`. The attempt is audited, including a refusal.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
    ),
    responses(
        (status = 200, description = "The pipeline, now paused", body = PipelineView),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may see but not change this pipeline", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The scheduler has not loaded this pipeline yet", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The scheduler or the cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the scheduler is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn pause(
    signed: SignedIn,
    Path((namespace, name)): Path<(String, String)>,
) -> Response {
    answer(
        pause_pipeline(
            resources(signed.state()).as_ref(),
            signed.state().airflow.as_ref(),
            signed.identity(),
            &namespace,
            &name,
        )
        .await,
    )
}

#[utoipa::path(
    post,
    operation_id = "pipelines_resume",
    path = "/api/pipelines/{namespace}/{name}/resume",
    tag = "public",
    description = "Resume a pipeline. Idempotent: resuming a running pipeline answers 200. Changing a pipeline is the owner group's and administrators' verb: a viewer (`spec.viewers`) may read and run it but not resume it, and answers 403 `only the owner group may change this pipeline` — a pipeline the caller cannot see stays 404. A pipeline the scheduler has not loaded answers 409. Answers the fresh `PipelineView` — the DAG as Airflow returned it after the patch — and `Cache-Control: no-store`. The attempt is audited, including a refusal.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
    ),
    responses(
        (status = 200, description = "The pipeline, now unpaused", body = PipelineView),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may see but not change this pipeline", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The scheduler has not loaded this pipeline yet", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The scheduler or the cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the scheduler is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn resume(
    signed: SignedIn,
    Path((namespace, name)): Path<(String, String)>,
) -> Response {
    answer(
        resume_pipeline(
            resources(signed.state()).as_ref(),
            signed.state().airflow.as_ref(),
            signed.identity(),
            &namespace,
            &name,
        )
        .await,
    )
}

#[utoipa::path(
    post,
    operation_id = "pipelines_run_stop",
    path = "/api/pipelines/{namespace}/{name}/runs/{run_id}/stop",
    tag = "public",
    description = "Stop a run: mark it failed. A viewer (`spec.viewers`) may stop a run, as it may start one. This is **not a cancel** — work already in flight can finish and commit its side effects (stardelt-nova#255), so the answer promises only that the run is marked failed. The run id is looked up through this pipeline's DAG id first: a run id of another pipeline answers 404 and is never patched. A run that is not `queued` or `running` answers 409 and is not patched. Answers the fresh `RunView` and `Cache-Control: no-store`. The attempt is audited, including a refusal.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
        ("run_id" = String, Path, description = "Airflow's run id"),
    ),
    responses(
        (status = 200, description = "The run, now failed", body = RunView),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller, or no such run", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The run is not active", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The scheduler or the cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the scheduler is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn stop(
    signed: SignedIn,
    Path((namespace, name, run_id)): Path<(String, String, String)>,
) -> Response {
    answer(
        stop_run(
            resources(signed.state()).as_ref(),
            signed.state().airflow.as_ref(),
            signed.identity(),
            &namespace,
            &name,
            &run_id,
        )
        .await,
    )
}

#[utoipa::path(
    post,
    operation_id = "pipelines_run_retry",
    path = "/api/pipelines/{namespace}/{name}/runs/{run_id}/retry",
    tag = "public",
    description = "Retry the failed tasks of a run, **in the same run**: the run keeps its id, date and parameters, and the tasks that already succeeded are not repeated (Airflow's run-scoped clear). A viewer (`spec.viewers`) may retry, as it may run and stop. A paused pipeline, a run that is not `failed`, and a run with no failed tasks are refused with 409. The run id is looked up through this pipeline's DAG id first: a run id of another pipeline answers 404 and nothing is cleared. Answers the fresh `RunView` and `Cache-Control: no-store`. Every attempt is audited, including a refusal.",
    params(
        ("namespace" = String, Path, description = "The Pipeline's namespace (Nova's own)"),
        ("name" = String, Path, description = "Pipeline name"),
        ("run_id" = String, Path, description = "Airflow's run id"),
    ),
    responses(
        (status = 200, description = "The run, with its failed tasks cleared for another attempt", body = RunView),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such pipeline for this caller, or no such run", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The run is not failed, the pipeline is paused, or there is nothing to retry", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The scheduler or the cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the scheduler is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn retry(
    signed: SignedIn,
    Path((namespace, name, run_id)): Path<(String, String, String)>,
) -> Response {
    answer(
        retry_run(
            resources(signed.state()).as_ref(),
            signed.state().airflow.as_ref(),
            signed.identity(),
            &namespace,
            &name,
            &run_id,
        )
        .await,
    )
}

/// Apply one document a recorded deployment stored, as `who`: the same create or update the
/// bundle deploy made, so `POST /api/deployments/{id}/rollback` replays exactly what ran.
///
/// The inner functions are used on purpose: the "in production a notebook comes from Git"
/// rule is a handler rule, and the recorded deploy was accepted when it was made. A create
/// takes the same ten-per-hour slot as a hand create; a bundle with more pipelines than that
/// is recorded `failed` and can be replayed after the window.
pub(crate) async fn apply_document(
    resources: Option<&PipelineResources>,
    db: Option<&sqlx::PgPool>,
    notebooks: Option<&NotebookStore>,
    airflow: Option<&AirflowClient>,
    limiter: &CreateLimiter,
    who: &Identity,
    document: &serde_json::Value,
) -> Result<&'static str, Fail> {
    let create: CreatePipelineRequest = serde_json::from_value(document.clone()).map_err(|e| {
        Fail::new(
            StatusCode::BAD_REQUEST,
            format!("the recorded document no longer fits a pipeline: {e}"),
        )
    })?;
    let Some(resources) = resources else {
        return Err(no_cluster());
    };
    match resources.get(&create.name).await {
        Ok(None) => create_pipeline_inner(Some(resources), db, notebooks, limiter, who, create)
            .await
            .map(|_| "created")
            .map_err(|e| e.fail),
        Ok(Some(_)) => {
            let update: UpdatePipelineRequest =
                serde_json::from_value(document.clone()).map_err(|e| {
                    Fail::new(
                        StatusCode::BAD_REQUEST,
                        format!("the recorded document no longer fits a pipeline: {e}"),
                    )
                })?;
            update_pipeline_inner(
                Some(resources),
                db,
                notebooks,
                airflow,
                who,
                &crate::cluster::namespace(),
                &create.name,
                update,
            )
            .await
            .map(|_| "updated")
            .map_err(|e| e.fail)
        }
        Err(e) => Err(resource_fail(e)),
    }
}

#[cfg(test)]
mod tests;
