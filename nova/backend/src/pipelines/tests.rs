use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// Serialises the tests that set/remove NOVA_BASE_DOMAIN: the environment is process-global.
/// Tokio's mutex, because the guard is held across the test's awaits.
static BASE_DOMAIN_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

use axum::{
    Extension,
    body::Body,
    http::{Request, StatusCode, header},
};
use serde_json::{Value, json};
use tower::ServiceExt;

use super::*;
use crate::airflow::{AirflowConfig, AirflowError, DagRun, DagSummary};
use crate::auth::{Principal, Secret};
use crate::notebooks::NotebookStore;
use crate::notebooks::fake::{FakeS3, notebook_store};
use crate::secret_store::StoreClient;

/// A value that must never appear in a log answer: the marker's `sources` path.
const SOURCES_CANARY: &str = "s3://backups/airflow-logs/dag_id=stardelt__smoke/run_id=manual__2026-10-20T07:23:50.608876+00:00/task_id=insert/attempt=3.log";
/// The path Airflow sends as the group's *body* event on live Airflow 3 (a separate event,
/// no `sources`): it must never appear in an answer either.
const SOURCES_BODY_CANARY: &str = "s3://backups/airflow-logs/dag_id=stardelt__smoke/run_id=manual__2026-10-20T07:23:50.608876+00:00/task_id=insert/attempt=2.log";
const SMOKE_RUN: &str = "manual__2026-10-20T07:23:50.608876+00:00";
const OTHER_RUN: &str = "manual__2026-10-20T09:00:00.000000+00:00";

fn who(sub: &str, groups: &[&str], admin: bool) -> Identity {
    Identity {
        sub: sub.into(),
        display_name: sub.into(),
        email: None,
        name: None,
        groups: groups.iter().map(|g| g.to_string()).collect(),
        roles: vec![],
        groups_overage: false,
        is_admin: admin,
    }
}

fn admin() -> Identity {
    who("sub-root", &["stardelt-admins"], true)
}

/// One `Pipeline` resource, with the fields the operator's CRD carries.
fn resource(name: &str, owner_group: Option<&str>) -> Value {
    json!({
        "apiVersion": "stardelt.io/v1alpha1",
        "kind": "Pipeline",
        "metadata": {
            "name": name,
            "namespace": crate::cluster::namespace(),
            "creationTimestamp": now_rfc3339(),
        },
        "spec": {
            "ownerGroup": owner_group,
            "tasks": [
                { "name": "schema", "kind": "sql" },
                { "name": "insert", "kind": "sql" }
            ],
            "schedule": { "cron": "0 3 * * *", "timezone": "Europe/Berlin" },
            "runAs": "pipeline-analytics",
            "params": { "target": "warehouse.smoke" },
            "retries": { "attempts": 2, "backoff": "5m" },
            "concurrency": "Forbid",
            "engine": "airflow"
        }
    })
}

/// The same resource with another `spec.concurrency`.
fn with_concurrency(mut resource: Value, concurrency: &str) -> Value {
    resource["spec"]["concurrency"] = json!(concurrency);
    resource
}

/// The same resource with `spec.viewers`: groups that may read it but not act (epic#895
/// decision 12).
fn with_viewers(mut resource: Value, viewers: &[&str]) -> Value {
    resource["spec"]["viewers"] = json!(viewers);
    resource
}

/// The same resource, created `seconds` ago (for the `loading`/`not-loaded` boundary).
fn with_created_ago(mut resource: Value, seconds: i64) -> Value {
    resource["metadata"]["creationTimestamp"] =
        json!(format_rfc3339(now_seconds() as i64 - seconds));
    resource
}

fn limiter() -> RunStartsLimiter {
    Mutex::new(std::collections::HashMap::new())
}

/// The pipelines router with a fake cluster (one `smoke` pipeline) and a fake Airflow,
/// signed in as a member of `analytics`: what a browser calls for `POST …/runs` (nova#817).
async fn run_route(airflow: AirflowClient) -> Router {
    let (store, _store_fake) = crate::secret_store::fake::start().await;
    let kube = fake_kube::start(vec![resource("smoke", Some("analytics"))]).await;
    router()
        .with_state(test_state(Some(kube), airflow, store))
        .layer(Extension(principal(who("sub-ada", &["analytics"], false))))
}

fn memory(items: &[(&str, Value)]) -> PipelineResources {
    memory_with(items, &[])
}

fn memory_with(items: &[(&str, Value)], principals: &[(&str, Value)]) -> PipelineResources {
    let pipelines: BTreeMap<String, Value> = items
        .iter()
        .map(|(name, value)| ((*name).to_string(), value.clone()))
        .collect();
    let principals: BTreeMap<String, Value> = principals
        .iter()
        .map(|(name, value)| ((*name).to_string(), value.clone()))
        .collect();
    PipelineResources::Memory(Arc::new(Mutex::new(MemoryStore {
        pipelines,
        principals,
    })))
}

fn dag(dag_id: &str) -> Value {
    json!({
        "dag_id": dag_id,
        "dag_display_name": dag_id,
        "is_paused": false,
        "is_stale": false,
        "timetable_description": "0 3 * * *",
        "next_dagrun_run_after": "2026-10-21T03:00:00Z",
        "last_parsed_time": "2026-10-20T07:24:29.819955Z",
        "tags": [{ "name": "pipeline" }, { "name": "stardelt" }]
    })
}

fn run(dag_run_id: &str, state: &str) -> Value {
    json!({
        "dag_run_id": dag_run_id,
        "state": state,
        "run_type": "manual",
        "logical_date": "2026-10-20T07:23:50Z",
        "start_date": "2026-10-20T07:23:50.608876Z",
        "end_date": "2026-10-20T07:23:58.876923Z",
        "note": "from the test",
        "triggered_by": "rest_api",
        "triggering_user_name": "nova-api",
        "conf": {},
        "run_after": "2026-10-20T07:23:50.608876Z"
    })
}

fn task(task_id: &str, try_number: u32) -> Value {
    json!({
        "task_id": task_id,
        "state": "success",
        "try_number": try_number,
        "start_date": "2026-10-20T07:23:50.608876Z",
        "end_date": "2026-10-20T07:23:58.876923Z",
        "duration": 8.268047,
        "operator": "PythonOperator",
        "map_index": -1
    })
}

fn log_page() -> Value {
    json!({
        "content": [
            {
                "event": "::group::Log message source details",
                "sources": [SOURCES_CANARY]
            },
            {
                "event": SOURCES_BODY_CANARY
            },
            {
                "event": "::endgroup::"
            },
            {
                "timestamp": "2026-10-20T07:23:51.000000Z",
                "level": "info",
                "event": "\u{1b}[32minsert\u{1b}[0m done"
            },
            {
                "timestamp": "2026-10-20T07:23:52.000000Z",
                "level": "warning",
                "event": "still here"
            }
        ],
        "continuation_token": null
    })
}

/// A fake Airflow with the few routes the read API uses. The marker's `sources` path is a
/// canary: it must never appear in an answer.
mod fake_airflow {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::{Arc, Mutex};

    use axum::{
        Json, Router,
        extract::{OriginalUri, Path, Query, State},
        http::{HeaderMap, StatusCode, Uri, header},
        routing::{get, patch, post},
    };
    use serde_json::{Value, json};

    use crate::airflow::{AirflowClient, AirflowConfig};
    use crate::auth::Secret;

    use super::{OTHER_RUN, SMOKE_RUN, dag, log_page, run, task};

    const PASSWORD: &str = "pw-canary-pipeline-4a17";
    const TOKEN: &str = "tok-canary-pipeline-4a17";

    type Refusal = (StatusCode, Json<Value>);

    fn refused() -> Refusal {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "detail": "Invalid credentials" })),
        )
    }

    #[derive(Default)]
    pub struct Fake {
        /// `METHOD path?query` of every call except the sign-in.
        pub calls: Vec<String>,
        pub dags: Vec<Value>,
        pub runs: BTreeMap<String, Vec<Value>>,
        pub tasks: BTreeMap<String, Vec<Value>>,
        pub log_page: Value,
        /// DAG ids whose run listing answers `500`.
        pub fail_runs: BTreeSet<String>,
        /// Every trigger's JSON body, in order, so a test can read the `logical_date` and
        /// the note Nova sent.
        pub trigger_bodies: Vec<Value>,
        /// Every run-scoped clear's JSON body, in order (nova#816).
        pub clear_bodies: Vec<Value>,
        /// When set, the clear selects nothing: a run with no failed task.
        pub clear_empty: bool,
        /// Delay before every `dags`, `runs` and batch answer, to time Nova's fan-out (spike).
        pub latency: std::time::Duration,
        /// Every batch listing's JSON body, in order.
        pub batch_bodies: Vec<Value>,
        /// When set, the batch listing answers `500`.
        pub fail_batch: bool,
    }

    pub type Shared = Arc<Mutex<Fake>>;

    fn seeded() -> Fake {
        let mut fake = Fake {
            dags: vec![
                dag("stardelt__smoke"),
                dag("stardelt__other"),
                dag("stardelt__hidden"),
            ],
            log_page: log_page(),
            ..Fake::default()
        };
        fake.runs
            .insert("stardelt__smoke".into(), vec![run(SMOKE_RUN, "success")]);
        fake.runs
            .insert("stardelt__other".into(), vec![run(OTHER_RUN, "failed")]);
        fake.runs.insert(
            "stardelt__hidden".into(),
            vec![run("hidden__2026-10-20T08:00:00+00:00", "success")],
        );
        fake.tasks
            .insert(SMOKE_RUN.into(), vec![task("insert", 3), task("schema", 1)]);
        fake.tasks.insert(OTHER_RUN.into(), vec![task("insert", 1)]);
        fake
    }

    fn line(uri: &Uri) -> String {
        uri.path_and_query()
            .map(|p| p.as_str().to_string())
            .unwrap_or_else(|| uri.path().to_string())
    }

    fn remember(state: &mut Fake, method: &str, uri: &Uri) {
        state.calls.push(format!("{method} {}", line(uri)));
    }

    fn authorize(headers: &HeaderMap) -> Result<(), Refusal> {
        let expected = format!("Bearer {TOKEN}");
        if headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            != Some(expected.as_str())
        {
            return Err(refused());
        }
        Ok(())
    }

    async fn login(Json(body): Json<Value>) -> (StatusCode, Json<Value>) {
        if body["username"].as_str() == Some("nova-api")
            && body["password"].as_str() == Some(PASSWORD)
        {
            return (StatusCode::CREATED, Json(json!({ "access_token": TOKEN })));
        }
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "detail": "Invalid credentials" })),
        )
    }

    async fn dags(
        State(state): State<Shared>,
        OriginalUri(uri): OriginalUri,
        Query(query): Query<BTreeMap<String, String>>,
        headers: HeaderMap,
    ) -> Result<Json<Value>, Refusal> {
        let latency = state.lock().unwrap().latency;
        tokio::time::sleep(latency).await;
        let mut state = state.lock().unwrap();
        remember(&mut state, "GET", &uri);
        authorize(&headers)?;
        let total = state.dags.len();
        let offset: usize = query
            .get("offset")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let limit: usize = query
            .get("limit")
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);
        let page: Vec<Value> = state
            .dags
            .iter()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect();
        Ok(Json(json!({ "dags": page, "total_entries": total })))
    }

    async fn one_dag(
        State(state): State<Shared>,
        OriginalUri(uri): OriginalUri,
        Path(dag_id): Path<String>,
        headers: HeaderMap,
    ) -> Result<Json<Value>, Refusal> {
        let mut state = state.lock().unwrap();
        remember(&mut state, "GET", &uri);
        authorize(&headers)?;
        match state.dags.iter().find(|dag| dag["dag_id"] == dag_id) {
            Some(dag) => Ok(Json(dag.clone())),
            None => Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "detail": "DAG not found" })),
            )),
        }
    }

    async fn runs(
        State(state): State<Shared>,
        OriginalUri(uri): OriginalUri,
        Path(dag_id): Path<String>,
        Query(query): Query<BTreeMap<String, String>>,
        headers: HeaderMap,
    ) -> Result<Json<Value>, Refusal> {
        let latency = state.lock().unwrap().latency;
        tokio::time::sleep(latency).await;
        let mut state = state.lock().unwrap();
        remember(&mut state, "GET", &uri);
        authorize(&headers)?;
        if state.fail_runs.contains(&dag_id) {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "detail": "boom" })),
            ));
        }
        let Some(runs) = state.runs.get(&dag_id) else {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "detail": "DAG not found" })),
            ));
        };
        let limit: usize = query
            .get("limit")
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);
        let page: Vec<Value> = runs.iter().take(limit).cloned().collect();
        Ok(Json(
            json!({ "dag_runs": page, "total_entries": runs.len() }),
        ))
    }

    /// Airflow 3.2.2's `POST /api/v2/dags/~/dagRuns/list`: the runs of `dag_ids`, newest
    /// `start_date` first, at most `page_limit` (default 100) in total, each with its `dag_id`.
    async fn runs_batch(
        State(state): State<Shared>,
        OriginalUri(uri): OriginalUri,
        Path(dag_id): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Result<Json<Value>, Refusal> {
        let latency = state.lock().unwrap().latency;
        tokio::time::sleep(latency).await;
        let mut state = state.lock().unwrap();
        remember(&mut state, "POST", &uri);
        authorize(&headers)?;
        if state.fail_batch {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "detail": "boom" })),
            ));
        }
        if dag_id != "~" {
            return Err((
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({ "detail": "the batch listing takes the dag_id ~" })),
            ));
        }
        state.batch_bodies.push(body.clone());
        let wanted: Vec<String> = body["dag_ids"]
            .as_array()
            .map(|ids| {
                ids.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let limit = body["page_limit"].as_u64().unwrap_or(100) as usize;
        let mut runs: Vec<Value> = state
            .runs
            .iter()
            .filter(|(id, _)| wanted.is_empty() || wanted.contains(*id))
            .flat_map(|(id, runs)| {
                runs.iter().map(move |run| {
                    let mut run = run.clone();
                    run["dag_id"] = json!(id);
                    run
                })
            })
            .collect();
        runs.sort_by(|a, b| b["start_date"].as_str().cmp(&a["start_date"].as_str()));
        let total = runs.len();
        runs.truncate(limit);
        Ok(Json(json!({ "dag_runs": runs, "total_entries": total })))
    }

    fn run_belongs(state: &Fake, dag_id: &str, run_id: &str) -> bool {
        state
            .runs
            .get(dag_id)
            .is_some_and(|runs| runs.iter().any(|run| run["dag_run_id"] == run_id))
    }

    async fn tasks(
        State(state): State<Shared>,
        OriginalUri(uri): OriginalUri,
        Path((dag_id, run_id)): Path<(String, String)>,
        headers: HeaderMap,
    ) -> Result<Json<Value>, Refusal> {
        let mut state = state.lock().unwrap();
        remember(&mut state, "GET", &uri);
        authorize(&headers)?;
        if !run_belongs(&state, &dag_id, &run_id) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "detail": "DagRun not found" })),
            ));
        }
        let tasks = state.tasks.get(&run_id).cloned().unwrap_or_default();
        Ok(Json(
            json!({ "task_instances": tasks, "total_entries": tasks.len() }),
        ))
    }

    async fn logs(
        State(state): State<Shared>,
        OriginalUri(uri): OriginalUri,
        Path((dag_id, run_id, _task_id, _try_number)): Path<(String, String, String, u32)>,
        headers: HeaderMap,
    ) -> Result<Json<Value>, Refusal> {
        let mut state = state.lock().unwrap();
        remember(&mut state, "GET", &uri);
        authorize(&headers)?;
        if !run_belongs(&state, &dag_id, &run_id) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "detail": "DagRun not found" })),
            ));
        }
        Ok(Json(state.log_page.clone()))
    }

    async fn trigger(
        State(state): State<Shared>,
        OriginalUri(uri): OriginalUri,
        Path(dag_id): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Result<(StatusCode, Json<Value>), Refusal> {
        let mut state = state.lock().unwrap();
        remember(&mut state, "POST", &uri);
        authorize(&headers)?;
        if !state.dags.iter().any(|dag| dag["dag_id"] == dag_id) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "detail": "DAG not found" })),
            ));
        }
        let logical_date = body["logical_date"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let run = json!({
            "dag_run_id": format!("manual__{logical_date}"),
            "state": "queued",
            "run_type": "manual",
            "logical_date": logical_date,
            "start_date": null,
            "end_date": null,
            "note": body.get("note").cloned().unwrap_or(Value::Null),
            "triggered_by": "rest_api",
            "triggering_user_name": "nova-api"
        });
        state.trigger_bodies.push(body);
        state.runs.entry(dag_id).or_default().insert(0, run.clone());
        Ok((StatusCode::CREATED, Json(run)))
    }

    async fn patch_dag(
        State(state): State<Shared>,
        OriginalUri(uri): OriginalUri,
        Path(dag_id): Path<String>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Result<Json<Value>, Refusal> {
        let mut state = state.lock().unwrap();
        remember(&mut state, "PATCH", &uri);
        authorize(&headers)?;
        let Some(dag) = state.dags.iter_mut().find(|dag| dag["dag_id"] == dag_id) else {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "detail": "DAG not found" })),
            ));
        };
        if let Some(paused) = body.get("is_paused") {
            dag["is_paused"] = paused.clone();
        }
        Ok(Json(dag.clone()))
    }

    async fn patch_run(
        State(state): State<Shared>,
        OriginalUri(uri): OriginalUri,
        Path((dag_id, run_id)): Path<(String, String)>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Result<Json<Value>, Refusal> {
        let mut state = state.lock().unwrap();
        remember(&mut state, "PATCH", &uri);
        authorize(&headers)?;
        let Some(runs) = state.runs.get_mut(&dag_id) else {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "detail": "DAG not found" })),
            ));
        };
        let Some(run) = runs.iter_mut().find(|run| run["dag_run_id"] == run_id) else {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "detail": "DagRun not found" })),
            ));
        };
        if let Some(value) = body.get("state") {
            run["state"] = value.clone();
        }
        Ok(Json(run.clone()))
    }

    /// The run-scoped clear (nova#816): only the failed tasks are selected.
    async fn clear_run(
        State(state): State<Shared>,
        OriginalUri(uri): OriginalUri,
        Path((dag_id, run_id)): Path<(String, String)>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Result<Json<Value>, Refusal> {
        let mut state = state.lock().unwrap();
        remember(&mut state, "POST", &uri);
        authorize(&headers)?;
        if !run_belongs(&state, &dag_id, &run_id) {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({ "detail": "DagRun not found" })),
            ));
        }
        state.clear_bodies.push(body);
        if state.clear_empty {
            return Ok(Json(json!({ "task_instances": [] })));
        }
        Ok(Json(json!({
            "task_instances": [
                { "task_id": "load", "state": "upstream_failed", "try_number": 1 }
            ]
        })))
    }

    pub async fn start() -> (AirflowClient, Shared) {
        let shared: Shared = Arc::new(Mutex::new(seeded()));
        let app = Router::new()
            .route("/auth/token", post(login))
            .route("/api/v2/dags", get(dags))
            .route("/api/v2/dags/{dag_id}", get(one_dag).patch(patch_dag))
            .route("/api/v2/dags/{dag_id}/dagRuns", get(runs).post(trigger))
            .route("/api/v2/dags/{dag_id}/dagRuns/list", post(runs_batch))
            .route("/api/v2/dags/{dag_id}/dagRuns/{run_id}", patch(patch_run))
            .route(
                "/api/v2/dags/{dag_id}/dagRuns/{run_id}/clear",
                post(clear_run),
            )
            .route(
                "/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
                get(tasks),
            )
            .route(
                "/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}",
                get(logs),
            )
            .with_state(Arc::clone(&shared));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let config = AirflowConfig {
            url: format!("http://{addr}"),
            user: "nova-api".into(),
            password: Secret::new(PASSWORD),
        };
        (AirflowClient::new(reqwest::Client::new(), config), shared)
    }
}

/// A fake Kubernetes API serving the `Pipeline` and `ServicePrincipal` routes the resource
/// client uses, including the writes and the `resourceVersion` check of a replace.
mod fake_kube {
    use std::sync::{Arc, Mutex};

    use axum::{
        Json, Router,
        extract::{Path, State},
        http::StatusCode,
        routing::get,
    };
    use serde_json::{Value, json};

    struct Fake {
        items: Vec<Value>,
        principals: Vec<Value>,
        version: u64,
    }

    type Shared = Arc<Mutex<Fake>>;

    fn not_found() -> (StatusCode, Json<Value>) {
        (
            StatusCode::NOT_FOUND,
            Json(json!({
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "message": "not found",
                "reason": "NotFound",
                "code": 404
            })),
        )
    }

    fn conflict() -> (StatusCode, Json<Value>) {
        (
            StatusCode::CONFLICT,
            Json(json!({
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "message": "the object has been modified",
                "reason": "Conflict",
                "code": 409
            })),
        )
    }

    async fn list(State(state): State<Shared>) -> Json<Value> {
        let state = state.lock().unwrap();
        Json(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "PipelineList",
            "metadata": { "resourceVersion": "1" },
            "items": state.items
        }))
    }

    async fn principal_list(State(state): State<Shared>) -> Json<Value> {
        let state = state.lock().unwrap();
        Json(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "ServicePrincipalList",
            "metadata": { "resourceVersion": "1" },
            "items": state.principals
        }))
    }

    async fn one(
        State(state): State<Shared>,
        Path((_namespace, name)): Path<(String, String)>,
    ) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
        let state = state.lock().unwrap();
        match state
            .items
            .iter()
            .find(|item| item["metadata"]["name"] == name)
        {
            Some(item) => Ok(Json(item.clone())),
            None => Err(not_found()),
        }
    }

    async fn create(
        State(state): State<Shared>,
        Path(_namespace): Path<String>,
        Json(body): Json<Value>,
    ) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
        let mut state = state.lock().unwrap();
        let name = body["metadata"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if state
            .items
            .iter()
            .any(|item| item["metadata"]["name"] == name)
        {
            return Err(conflict());
        }
        state.version += 1;
        let resource_version = state.version.to_string();
        let mut stored = body;
        stored["metadata"]["resourceVersion"] = json!(resource_version);
        stored["metadata"]["creationTimestamp"] = json!("2026-10-20T10:00:00Z");
        state.items.push(stored.clone());
        Ok((StatusCode::CREATED, Json(stored)))
    }

    async fn replace(
        State(state): State<Shared>,
        Path((_namespace, name)): Path<(String, String)>,
        Json(body): Json<Value>,
    ) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
        let mut state = state.lock().unwrap();
        let Some(index) = state
            .items
            .iter()
            .position(|item| item["metadata"]["name"] == name)
        else {
            return Err(not_found());
        };
        if state.items[index]["metadata"]["resourceVersion"] != body["metadata"]["resourceVersion"]
        {
            return Err(conflict());
        }
        state.version += 1;
        let resource_version = state.version.to_string();
        let mut stored = body;
        stored["metadata"]["resourceVersion"] = json!(resource_version);
        state.items[index] = stored.clone();
        Ok(Json(stored))
    }

    async fn delete_one(
        State(state): State<Shared>,
        Path((_namespace, name)): Path<(String, String)>,
    ) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
        let mut state = state.lock().unwrap();
        let Some(index) = state
            .items
            .iter()
            .position(|item| item["metadata"]["name"] == name)
        else {
            return Err(not_found());
        };
        state.items.remove(index);
        Ok(Json(json!({
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Success",
            "code": 200
        })))
    }

    pub async fn start(items: Vec<Value>) -> kube::Client {
        start_with(items, vec![]).await
    }

    pub async fn start_with(items: Vec<Value>, principals: Vec<Value>) -> kube::Client {
        let shared: Shared = Arc::new(Mutex::new(Fake {
            items,
            principals,
            version: 1,
        }));
        let app = Router::new()
            .route(
                "/apis/stardelt.io/v1alpha1/namespaces/{namespace}/pipelines",
                get(list).post(create),
            )
            .route(
                "/apis/stardelt.io/v1alpha1/namespaces/{namespace}/pipelines/{name}",
                get(one).put(replace).delete(delete_one),
            )
            .route(
                "/apis/stardelt.io/v1alpha1/namespaces/{namespace}/serviceprincipals",
                get(principal_list),
            )
            .with_state(shared);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let url = format!("http://{addr}").parse().unwrap();
        kube::Client::try_from(kube::Config::new(url)).expect("a client for the fake API")
    }
}

/// A fake Trino coordinator with the one endpoint the statement check uses. It records the
/// statement text and the headers, so a test can prove what Nova sent — and what it never sent.
mod fake_trino {
    use std::sync::{Arc, Mutex};

    use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
    use serde_json::{Value, json};

    #[derive(Default)]
    pub struct Fake {
        /// The body of every `/v1/statement` POST, in order.
        pub statements: Vec<String>,
        /// The `X-Trino-Source` of every POST.
        pub sources: Vec<String>,
        /// The `X-Trino-User` of every POST: who the check ran as.
        pub users: Vec<String>,
        /// Answer this error document instead of a result.
        pub error: Option<Value>,
    }

    pub type Shared = Arc<Mutex<Fake>>;

    async fn statement(
        State(state): State<Shared>,
        headers: HeaderMap,
        body: String,
    ) -> Json<Value> {
        let mut fake = state.lock().expect("fake trino poisoned");
        fake.statements.push(body);
        fake.sources.push(
            headers
                .get("x-trino-source")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string(),
        );
        fake.users.push(
            headers
                .get("x-trino-user")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string(),
        );
        match &fake.error {
            // No `nextUri`: the client stops on the first document.
            Some(error) => Json(json!({ "id": "q-check", "error": error, "stats": {} })),
            None => Json(json!({
                "id": "q-check",
                "columns": [{ "name": "plan", "type": "varchar" }],
                "data": [["Output[columnNames = [x]]"]],
                "stats": {}
            })),
        }
    }

    pub async fn start(error: Option<Value>) -> (String, Shared) {
        let shared: Shared = Arc::new(Mutex::new(Fake {
            error,
            ..Fake::default()
        }));
        let app = Router::new()
            .route("/v1/statement", post(statement))
            .with_state(shared.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), shared)
    }
}

fn test_state(
    kube: Option<kube::Client>,
    airflow: AirflowClient,
    store: StoreClient,
) -> Arc<AppState> {
    test_state_at(kube, airflow, store, "http://127.0.0.1:1")
}

fn test_state_at(
    kube: Option<kube::Client>,
    airflow: AirflowClient,
    store: StoreClient,
    trino_url: &str,
) -> Arc<AppState> {
    Arc::new(AppState {
        dev_user: "test".into(),
        auth_dev: false,
        default_theme: "graphite",
        trino_url: trino_url.into(),
        lakekeeper_url: "http://127.0.0.1:1".into(),
        shared_accounts: vec![],
        // No catalog client in these tests: Lakekeeper is called as before (#222).
        catalog_token: None,
        exchange: None,
        superset_url: "http://127.0.0.1:1".into(),
        superset_api_url: "http://127.0.0.1:1".into(),
        superset: None,
        jupyterhub_url: "http://127.0.0.1:1".into(),
        warehouse_name: "warehouse".into(),
        stage_name: None,
        stage_kind: "dev",
        stages: Vec::new(),
        catalog_ids: std::sync::RwLock::new(std::collections::HashMap::new()),
        catalog_access: std::sync::Mutex::new(std::collections::HashMap::new()),
        catalog_metrics: std::sync::Mutex::new(std::collections::HashMap::new()),
        metrics_in_flight: std::sync::Mutex::new(std::collections::HashSet::new()),
        metrics_round: tokio::sync::Mutex::new(()),
        metrics_refresh: std::sync::Mutex::new(None),
        catalog_layers: std::sync::Mutex::new(None),
        principal_owners: std::sync::Mutex::new(None),
        http: reqwest::Client::new(),
        forward_identity: false,
        redact_query_log_literals: false,
        showback: None,
        mcp: None,
        auth: None,
        jobs: Mutex::new(Vec::new()),
        kube,
        workloads: crate::cluster::WorkloadCache::default(),
        admin_console_url: None,
        db: None,
        store: Some(store),
        break_glass_reads: Mutex::new(std::collections::HashMap::new()),
        run_starts: Mutex::new(std::collections::HashMap::new()),
        pipeline_creates: Mutex::new(std::collections::HashMap::new()),
        catalog_access_changes: Mutex::new(std::collections::HashMap::new()),
        team_creates: Mutex::new(HashMap::new()),
        airflow: Some(airflow),
        table_locks: Mutex::new(std::collections::HashMap::new()),
        people_touched: Mutex::new(std::collections::HashMap::new()),
        notebooks: None,
        workspace: None,
        statements: Default::default(),
    })
}

fn principal(identity: Identity) -> Principal {
    Principal {
        identity,
        via: "session",
        access_token: Some(Secret::new("good")),
        client_id: None,
    }
}

// -----------------------------------------------------------------------
// Visibility
// -----------------------------------------------------------------------

#[test]
fn visibility_follows_the_owner_group() {
    let member = who("sub-ada", &["analytics"], false);
    let outsider = who("sub-bob", &["finance"], false);
    let administrator = who("sub-root", &[], true);
    let owned = json!({ "ownerGroup": "analytics" });

    assert!(
        visible(&member, &owned),
        "a member sees the group's pipeline"
    );
    assert!(!visible(&outsider, &owned), "another group does not");
    assert!(visible(&administrator, &owned), "an administrator sees all");

    let unowned = json!({});
    assert!(
        !visible(&member, &unowned),
        "no ownerGroup is administrators-only"
    );
    assert!(visible(&administrator, &unowned));

    let other = json!({ "ownerGroup": "finance" });
    assert!(
        !visible(&member, &other),
        "membership is by exact group name"
    );
}

#[tokio::test]
async fn list_hides_other_groups_pipelines() {
    let resources = memory(&[
        ("smoke", resource("smoke", Some("analytics"))),
        ("hidden", resource("hidden", Some("finance"))),
    ]);
    let (airflow, fake) = fake_airflow::start().await;

    let answer = list_pipelines(
        Some(&resources),
        Some(&airflow),
        &who("sub-ada", &["analytics"], false),
    )
    .await
    .expect("the list succeeds");

    let names: Vec<&str> = answer
        .pipelines
        .iter()
        .map(|pipeline| pipeline.name.as_str())
        .collect();
    assert_eq!(names, ["smoke"], "only the caller's group is listed");

    let calls = fake.lock().unwrap().calls.clone();
    assert!(
        !calls.iter().any(|call| call.contains("stardelt__hidden")),
        "Airflow heard nothing about the hidden pipeline: {calls:?}"
    );
    let batch = fake
        .lock()
        .unwrap()
        .batch_bodies
        .last()
        .cloned()
        .expect("one batch listing");
    let ids = batch["dag_ids"].to_string();
    assert!(
        ids.contains("stardelt__smoke") && !ids.contains("stardelt__hidden"),
        "the batch asks for the visible pipeline only: {ids}"
    );
}

/// Spike: Airflow calls and time of `GET /api/pipelines` today, and of the batch listing.
/// Ignored: it prints numbers (`--ignored --nocapture`).
#[tokio::test]
#[ignore]
async fn spike_airflow_fanout_timing() {
    const LATENCY: std::time::Duration = std::time::Duration::from_millis(20);
    for (n, busy) in [(10usize, 0usize), (50, 0), (200, 0), (50, 150)] {
        let (airflow, fake) = fake_airflow::start().await;
        let mut names: Vec<String> = (0..n).map(|i| format!("p{i:03}")).collect();
        if busy > 0 {
            names.push("busy".to_string());
        }
        {
            let mut f = fake.lock().unwrap();
            f.latency = LATENCY;
            f.dags = names
                .iter()
                .map(|name| dag(&format!("stardelt__{name}")))
                .collect();
            f.runs.clear();
            for (i, name) in names.iter().enumerate() {
                let count = if name == "busy" { busy } else { 1 };
                let day = if name == "busy" { 2 } else { 1 };
                let runs = (0..count)
                    .map(|k| {
                        let mut r = run(&format!("{name}__run{k}"), "success");
                        let s = if name == "busy" { k } else { i };
                        r["start_date"] =
                            json!(format!("2026-11-{day:02}T{:02}:{:02}:00Z", s / 60, s % 60));
                        r
                    })
                    .collect();
                f.runs.insert(format!("stardelt__{name}"), runs);
            }
        }
        let items: Vec<(&str, Value)> = names
            .iter()
            .map(|name| (name.as_str(), resource(name, Some("analytics"))))
            .collect();
        let resources = memory(&items);
        let caller = who("sub-ada", &["analytics"], false);
        let dag_ids: Vec<String> = names
            .iter()
            .map(|name| format!("stardelt__{name}"))
            .collect();
        // Sign in once, so neither measurement pays for the token.
        airflow.list_dags("pipeline", true).await.expect("warm-up");

        fake.lock().unwrap().calls.clear();
        let started = std::time::Instant::now();
        let answer = list_pipelines(Some(&resources), Some(&airflow), &caller)
            .await
            .expect("the list succeeds");
        let quo_ms = started.elapsed().as_millis();
        let quo_calls = fake.lock().unwrap().calls.len();
        assert_eq!(answer.pipelines.len(), names.len());

        fake.lock().unwrap().calls.clear();
        let started = std::time::Instant::now();
        airflow.list_dags("pipeline", true).await.expect("dags");
        let runs = airflow.runs_batch(&dag_ids, 100).await.expect("batch");
        let batch_ms = started.elapsed().as_millis();
        let batch_calls = fake.lock().unwrap().calls.len();
        let covered: BTreeSet<&str> = runs.iter().map(|(id, _)| id.as_str()).collect();
        println!(
            "n={n} busy={busy} status_quo_calls={quo_calls} status_quo_ms={quo_ms} \
             batch_calls={batch_calls} batch_ms={batch_ms} batch_covered={}/{}",
            covered.len(),
            names.len()
        );
    }
}

#[tokio::test]
async fn detail_of_a_hidden_pipeline_is_404_not_403() {
    let resources = memory(&[("hidden", resource("hidden", Some("finance")))]);
    let (airflow, fake) = fake_airflow::start().await;
    let outsider = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let error = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &outsider,
        &namespace,
        "hidden",
    )
    .await
    .expect_err("hidden");
    assert_eq!(error.status(), StatusCode::NOT_FOUND);
    assert_eq!(error.message(), "no such pipeline");
    assert!(
        fake.lock().unwrap().calls.is_empty(),
        "the hidden pipeline never reaches Airflow"
    );
}

#[tokio::test]
async fn runs_and_logs_of_a_hidden_pipeline_are_404_not_403() {
    let resources = memory(&[("hidden", resource("hidden", Some("finance")))]);
    let (airflow, fake) = fake_airflow::start().await;
    let outsider = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let error = run_detail(
        Some(&resources),
        Some(&airflow),
        &outsider,
        &namespace,
        "hidden",
        "hidden__2026-10-20T08:00:00+00:00",
    )
    .await
    .expect_err("hidden");
    assert_eq!(error.status(), StatusCode::NOT_FOUND);

    let error = task_logs(
        Some(&resources),
        Some(&airflow),
        &outsider,
        &namespace,
        "hidden",
        "hidden__2026-10-20T08:00:00+00:00",
        "insert",
        Some(1),
        None,
    )
    .await
    .expect_err("hidden");
    assert_eq!(error.status(), StatusCode::NOT_FOUND);
    assert!(
        fake.lock().unwrap().calls.is_empty(),
        "neither route asks Airflow about a hidden pipeline"
    );
}

#[tokio::test]
async fn run_queries_of_a_hidden_pipeline_is_404_before_the_state_store_is_read() {
    // The state has no database: a route that read the store before the visibility check
    // would answer 503 "the state store is not configured", not 404.
    let kube = fake_kube::start(vec![resource("hidden", Some("finance"))]).await;
    let (store, _store_fake) = crate::secret_store::fake::start().await;
    let (airflow, airflow_fake) = fake_airflow::start().await;
    let namespace = crate::cluster::namespace();
    let app = router()
        .with_state(test_state(Some(kube), airflow, store))
        .layer(Extension(principal(who("sub-ada", &["analytics"], false))));

    let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/pipelines/{namespace}/hidden/runs/scheduled__2026-11-14T03:00:00+00:00/queries"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["error"], "no such pipeline");
    assert!(
        airflow_fake.lock().unwrap().calls.is_empty(),
        "a hidden pipeline never reaches Airflow"
    );
}

#[tokio::test]
async fn run_notebook_of_a_hidden_pipeline_is_404_not_403() {
    let resources = memory(&[("hidden", resource("hidden", Some("finance")))]);
    let outsider = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let error = task_notebook(
        Some(&resources),
        None,
        &outsider,
        &namespace,
        "hidden",
        "hidden__2026-10-20T08:00:00+00:00",
        "run",
        false,
    )
    .await
    .expect_err("hidden");
    assert_eq!(error.status(), StatusCode::NOT_FOUND);
    assert_eq!(error.message(), "no such pipeline");
}

#[tokio::test]
async fn run_notebook_over_the_run_copy_cap_is_413() {
    let resources = memory(&[("daily", resource("daily", Some("analytics")))]);
    let s3 = FakeS3::start().await;
    let store = notebook_store(&s3.base);
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();
    let run_id = "scheduled__2026-11-10T03:00:00+00:00";
    let key = notebooks::run_copy_key("analytics", "daily", run_id, "run");

    // A 6 MiB executed copy is over the publish limit and under the run-copy cap.
    s3.put_raw(&key, &vec![b'x'; 6 * 1024 * 1024], "alice");
    let response = task_notebook(
        Some(&resources),
        Some(&store),
        &ada,
        &namespace,
        "daily",
        run_id,
        "run",
        false,
    )
    .await
    .expect("a 6 MiB executed copy");
    assert_eq!(response.status(), StatusCode::OK);

    // Over the run-copy cap the new object answers the promised 413, not a 502.
    s3.put_raw(
        &key,
        &vec![b'x'; notebooks::MAX_RUN_NOTEBOOK_BYTES + 1024],
        "alice",
    );
    let error = task_notebook(
        Some(&resources),
        Some(&store),
        &ada,
        &namespace,
        "daily",
        run_id,
        "run",
        false,
    )
    .await
    .expect_err("over the run-copy cap");
    assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        error.message(),
        "the executed notebook is larger than 25 MiB"
    );
}

// -----------------------------------------------------------------------
// State
// -----------------------------------------------------------------------

#[test]
fn pipeline_state_table() {
    for loaded in [true, false] {
        for paused in [true, false] {
            for last in [
                None,
                Some("running"),
                Some("queued"),
                Some("failed"),
                Some("upstream_failed"),
                Some("success"),
                Some("skipped"),
            ] {
                let state = pipeline_state(paused, loaded, last, Some(0.0));
                let expected = if !loaded {
                    "loading"
                } else if paused {
                    "paused"
                } else {
                    match last {
                        Some("running") | Some("queued") => "running",
                        Some("failed") | Some("upstream_failed") => "failed",
                        Some("success") => "succeeded",
                        _ => "idle",
                    }
                };
                assert_eq!(
                    state, expected,
                    "paused={paused} loaded={loaded} last={last:?}"
                );
            }
        }
    }
    assert_eq!(
        pipeline_state(true, true, Some("failed"), Some(0.0)),
        "paused",
        "paused beats a failed last run"
    );
    assert_eq!(
        pipeline_state(false, true, Some("queued"), Some(0.0)),
        "running",
        "queued counts as running"
    );
    assert_eq!(
        pipeline_state(false, true, None, Some(0.0)),
        "idle",
        "never ran is idle"
    );
}

#[tokio::test]
async fn a_new_pipeline_without_a_dag_is_loading() {
    let resources = memory(&[("brand-new", resource("brand-new", Some("analytics")))]);
    let (airflow, _fake) = fake_airflow::start().await;

    let detail = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "brand-new",
    )
    .await
    .expect("a pipeline without a DAG is not an error");

    assert!(!detail.pipeline.loaded);
    assert_eq!(detail.pipeline.state, "loading");
    assert!(!detail.pipeline.paused);
    assert_eq!(detail.pipeline.next_run, None);
    assert_eq!(detail.pipeline.last_run, None);
    assert!(detail.runs.is_empty());
}

#[tokio::test]
async fn list_calls_airflow_once_for_the_dags() {
    let resources = memory(&[
        ("smoke", resource("smoke", Some("analytics"))),
        ("other", resource("other", Some("analytics"))),
    ]);
    let (airflow, fake) = fake_airflow::start().await;

    list_pipelines(
        Some(&resources),
        Some(&airflow),
        &who("sub-ada", &["analytics"], false),
    )
    .await
    .expect("the list succeeds");

    let calls = fake.lock().unwrap().calls.clone();
    let dag_lists = calls
        .iter()
        .filter(|call| call.starts_with("GET /api/v2/dags?"))
        .count();
    let last_runs = calls
        .iter()
        .filter(|call| call.contains("/dagRuns?") && !call.contains("/logs/"))
        .count();
    assert_eq!(
        dag_lists, 1,
        "one DAG list for the whole request: {calls:?}"
    );
    let dag_list = calls
        .iter()
        .find(|call| call.starts_with("GET /api/v2/dags?"))
        .expect("a DAG list call");
    assert!(
        dag_list.contains("tags=pipeline") && dag_list.contains("exclude_stale=false"),
        "the list asks for pipeline DAGs including stale ones: {dag_list}"
    );
    assert_eq!(
        last_runs, 0,
        "the batch covers both pipelines, so no per-pipeline call: {calls:?}"
    );
    let batches = calls
        .iter()
        .filter(|call| call.starts_with("POST /api/v2/dags/~/dagRuns/list"))
        .count();
    assert_eq!(
        batches, 1,
        "one batch listing for the whole request: {calls:?}"
    );
}

#[tokio::test]
async fn list_reads_a_pipeline_the_batch_does_not_cover_one_by_one() {
    let resources = memory(&[
        ("smoke", resource("smoke", Some("analytics"))),
        ("other", resource("other", Some("analytics"))),
    ]);
    let (airflow, fake) = fake_airflow::start().await;
    fake.lock().unwrap().runs.remove("stardelt__other");

    let answer = list_pipelines(
        Some(&resources),
        Some(&airflow),
        &who("sub-ada", &["analytics"], false),
    )
    .await
    .expect("the list succeeds");

    let calls = fake.lock().unwrap().calls.clone();
    let one_by_one: Vec<&String> = calls
        .iter()
        .filter(|call| call.contains("/dagRuns?") && !call.contains("/logs/"))
        .collect();
    assert_eq!(
        one_by_one.len(),
        1,
        "only the uncovered pipeline is read alone: {calls:?}"
    );
    assert!(
        one_by_one[0].contains("stardelt__other/dagRuns?"),
        "{calls:?}"
    );
    let smoke = answer
        .pipelines
        .iter()
        .find(|p| p.name == "smoke")
        .expect("smoke");
    assert!(
        smoke.last_run.is_some(),
        "the batch answered smoke's last run"
    );
}

#[tokio::test]
async fn runs_lists_only_visible_pipelines_newest_first() {
    let resources = memory(&[
        ("smoke", resource("smoke", Some("analytics"))),
        ("other", resource("other", Some("finance"))),
    ]);
    let (airflow, fake) = fake_airflow::start().await;
    {
        let mut fake = fake.lock().unwrap();
        fake.runs.insert(
            "stardelt__smoke".into(),
            vec![
                run_started("scheduled__smoke-old", "failed", "2026-10-19T06:00:00Z"),
                run_started(SMOKE_RUN, "success", "2026-10-20T07:23:50Z"),
            ],
        );
        fake.runs.insert(
            "stardelt__other".into(),
            vec![run_started(OTHER_RUN, "success", "2026-10-21T09:00:00Z")],
        );
    }

    let answer = runs_across_pipelines(
        Some(&resources),
        Some(&airflow),
        &who("sub-ada", &["analytics"], false),
        &BTreeMap::new(),
    )
    .await
    .expect("the runs list succeeds");

    assert_eq!(
        answer.runs.len(),
        2,
        "only the visible pipeline's runs: {:?}",
        answer.runs
    );
    assert!(answer.runs.iter().all(|row| row.name == "smoke"));
    assert!(
        answer
            .runs
            .iter()
            .all(|row| row.owner_group.as_deref() == Some("analytics"))
    );
    assert!(
        answer
            .runs
            .iter()
            .all(|row| ["sql", "notebook", "pipeline"].contains(&row.kind.as_str()))
    );
    let starts: Vec<Option<&str>> = answer
        .runs
        .iter()
        .map(|row| row.run.started_at.as_deref())
        .collect();
    assert_eq!(
        starts,
        vec![Some("2026-10-20T07:23:50Z"), Some("2026-10-19T06:00:00Z")],
        "newest first"
    );
}

#[tokio::test]
async fn runs_filters_by_state_kind_group_and_limit() {
    fn filter(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    let resources = memory(&[
        (
            "smoke",
            run_task_resource("smoke", Some("analytics"), "sql"),
        ),
        (
            "notes",
            run_task_resource("notes", Some("analytics"), "notebook"),
        ),
        (
            "revenue",
            run_task_resource("revenue", Some("finance"), "sql"),
        ),
    ]);
    let (airflow, fake) = fake_airflow::start().await;
    {
        let mut fake = fake.lock().unwrap();
        fake.dags = vec![
            dag("stardelt__smoke"),
            dag("stardelt__notes"),
            dag("stardelt__revenue"),
        ];
        // Newest first, as Airflow answers: a `limit` then keeps the newest run.
        fake.runs.insert(
            "stardelt__smoke".into(),
            vec![
                run_started(SMOKE_RUN, "success", "2026-10-20T07:23:50Z"),
                run_started("scheduled__smoke-old", "failed", "2026-10-19T06:00:00Z"),
            ],
        );
        fake.runs.insert(
            "stardelt__notes".into(),
            vec![run_started(
                "scheduled__notes",
                "failed",
                "2026-10-18T06:00:00Z",
            )],
        );
        fake.runs.insert(
            "stardelt__revenue".into(),
            vec![run_started(
                "scheduled__revenue",
                "failed",
                "2026-10-17T06:00:00Z",
            )],
        );
    }
    let ada = who("sub-ada", &["analytics"], false);

    let all = runs_across_pipelines(Some(&resources), Some(&airflow), &ada, &filter(&[]))
        .await
        .expect("the unfiltered list succeeds");
    assert_eq!(all.runs.len(), 3, "the visible runs: {:?}", all.runs);

    let successes = runs_across_pipelines(
        Some(&resources),
        Some(&airflow),
        &ada,
        &filter(&[("state", "success")]),
    )
    .await
    .expect("a state filter succeeds");
    assert_eq!(successes.runs.len(), 1, "{:?}", successes.runs);
    assert_eq!(successes.runs[0].run.run_id, SMOKE_RUN);
    assert!(
        !successes
            .runs
            .iter()
            .any(|row| row.run.run_id == "scheduled__smoke-old"),
        "an older run of another state is not in the answer"
    );

    let news = runs_across_pipelines(
        Some(&resources),
        Some(&airflow),
        &ada,
        &filter(&[("state", "news")]),
    )
    .await
    .expect("an unknown state is not an error");
    assert!(news.runs.is_empty(), "{:?}", news.runs);

    let sql = runs_across_pipelines(
        Some(&resources),
        Some(&airflow),
        &ada,
        &filter(&[("kind", "sql")]),
    )
    .await
    .expect("a kind filter succeeds");
    assert_eq!(sql.runs.len(), 2, "{:?}", sql.runs);
    assert!(
        sql.runs
            .iter()
            .all(|row| row.kind == "sql" && row.name == "smoke")
    );

    let finance = runs_across_pipelines(
        Some(&resources),
        Some(&airflow),
        &ada,
        &filter(&[("group", "finance")]),
    )
    .await
    .expect("a group filter succeeds");
    assert!(finance.runs.is_empty(), "{:?}", finance.runs);

    let one = runs_across_pipelines(
        Some(&resources),
        Some(&airflow),
        &ada,
        &filter(&[("limit", "1")]),
    )
    .await
    .expect("a limit succeeds");
    assert_eq!(one.runs.len(), 1);
    assert_eq!(one.runs[0].run.run_id, SMOKE_RUN, "the newest row survives");
}

#[tokio::test]
async fn runs_rejects_a_bad_limit_and_a_bad_kind() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    let ada = who("sub-ada", &["analytics"], false);
    let cases: [(&str, &str, &str); 5] = [
        ("limit", "x", "limit must be a positive integer"),
        ("limit", "0", "limit must be a positive integer"),
        ("limit", "201", "limit must be at most 200"),
        (
            "kind",
            "bundle",
            "kind must be one of sql, notebook, pipeline",
        ),
        ("pipeline", "smoke", "pipeline must be <namespace>/<name>"),
    ];
    for (key, value, sentence) in cases {
        let query: BTreeMap<String, String> =
            [(key.to_string(), value.to_string())].into_iter().collect();
        let error = runs_across_pipelines(Some(&resources), Some(&airflow), &ada, &query)
            .await
            .expect_err("a bad query is refused");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{key}={value}");
        assert_eq!(error.message(), sentence, "{key}={value}");
    }
    assert!(
        fake.lock().unwrap().calls.is_empty(),
        "a refused query never reaches the scheduler"
    );
}

#[tokio::test]
async fn runs_asks_airflow_for_one_dag_list_and_one_run_list_per_loaded_pipeline() {
    let resources = memory(&[
        ("smoke", resource("smoke", Some("analytics"))),
        ("unloaded", resource("unloaded", Some("analytics"))),
    ]);
    let (airflow, fake) = fake_airflow::start().await;

    runs_across_pipelines(
        Some(&resources),
        Some(&airflow),
        &who("sub-ada", &["analytics"], false),
        &BTreeMap::new(),
    )
    .await
    .expect("the runs list succeeds");

    let calls = fake.lock().unwrap().calls.clone();
    let dag_lists = calls
        .iter()
        .filter(|call| call.starts_with("GET /api/v2/dags?"))
        .count();
    assert_eq!(
        dag_lists, 1,
        "one DAG list for the whole request: {calls:?}"
    );
    let run_lists: Vec<&String> = calls
        .iter()
        .filter(|call| call.contains("/dagRuns?"))
        .collect();
    assert_eq!(
        run_lists.len(),
        1,
        "one run list, only for the loaded pipeline: {calls:?}"
    );
    assert!(
        run_lists
            .iter()
            .all(|call| call.contains("stardelt__smoke")),
        "{calls:?}"
    );
    assert!(
        !calls.iter().any(|call| call.contains("unloaded")),
        "no call for a DAG the scheduler does not hold: {calls:?}"
    );
}

#[tokio::test]
async fn runs_names_every_visible_group_whatever_the_filter() {
    let resources = memory(&[
        ("alpha", resource("alpha", Some("a"))),
        ("beta", resource("beta", Some("b"))),
        ("root-only", resource("root-only", None)),
    ]);
    let (airflow, fake) = fake_airflow::start().await;
    {
        let mut fake = fake.lock().unwrap();
        fake.dags = vec![dag("stardelt__alpha"), dag("stardelt__beta")];
        fake.runs.insert(
            "stardelt__alpha".into(),
            vec![run_started(
                "scheduled__alpha",
                "success",
                "2026-10-20T07:00:00Z",
            )],
        );
        fake.runs.insert(
            "stardelt__beta".into(),
            vec![run_started(
                "scheduled__beta",
                "success",
                "2026-10-21T07:00:00Z",
            )],
        );
    }

    let all = runs_across_pipelines(Some(&resources), Some(&airflow), &admin(), &BTreeMap::new())
        .await
        .expect("the unfiltered list succeeds");
    assert_eq!(all.runs.len(), 2, "one run each: {:?}", all.runs);
    assert_eq!(
        all.groups,
        vec!["a".to_string(), "b".to_string()],
        "every visible group, sorted; administrators are not a group"
    );

    let a = runs_across_pipelines(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &BTreeMap::from([("group".to_string(), "a".to_string())]),
    )
    .await
    .expect("a group filter succeeds");
    assert_eq!(a.runs.len(), 1, "only group a's run: {:?}", a.runs);
    assert_eq!(
        a.groups,
        vec!["a".to_string(), "b".to_string()],
        "the group filter does not narrow the choice"
    );
}

#[tokio::test]
async fn runs_filter_parses_a_notebook() {
    let query = BTreeMap::from([("notebook".to_string(), "analytics/smoke.ipynb".to_string())]);
    let filter = RunsFilter::parse(&query).expect("a group and a name parse");
    assert_eq!(
        filter.notebook,
        Some(("analytics".to_string(), "smoke.ipynb".to_string()))
    );
    for value in ["smoke.ipynb", "analytics/", "/smoke.ipynb", "analytics/a/b"] {
        let query = BTreeMap::from([("notebook".to_string(), value.to_string())]);
        let error = match RunsFilter::parse(&query) {
            Ok(_) => panic!("{value} must be refused"),
            Err(error) => error,
        };
        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{value}");
        assert_eq!(
            error.message(),
            "notebook must be <group>/<name>",
            "{value}"
        );
    }
}

#[tokio::test]
async fn a_notebook_schedule_is_recognised_by_group_notebook_and_task_kind() {
    let schedule = notebook_run_resource("smoke-daily", "analytics", "smoke.ipynb");
    assert!(
        notebook_schedule(&schedule, "analytics", "smoke.ipynb"),
        "the schedule of that notebook"
    );
    assert!(
        !notebook_schedule(&schedule, "analytics", "other.ipynb"),
        "another notebook name"
    );
    assert!(
        !notebook_schedule(&schedule, "finance", "smoke.ipynb"),
        "another owner group"
    );
    let sql = run_task_resource("smoke-daily", Some("analytics"), "sql");
    assert!(
        !notebook_schedule(&sql, "analytics", "smoke.ipynb"),
        "a sql task is no notebook schedule"
    );
    let no_config = run_task_resource("smoke-daily", Some("analytics"), "notebook");
    assert!(
        !notebook_schedule(&no_config, "analytics", "smoke.ipynb"),
        "the notebook name is part of the shape"
    );
}

#[tokio::test]
async fn runs_notebook_filter_answers_only_that_notebooks_runs() {
    let resources = memory(&[
        (
            "smoke-daily",
            notebook_run_resource("smoke-daily", "analytics", "smoke.ipynb"),
        ),
        (
            "other-daily",
            notebook_run_resource("other-daily", "analytics", "other.ipynb"),
        ),
    ]);
    let (airflow, fake) = fake_airflow::start().await;
    {
        let mut fake = fake.lock().unwrap();
        fake.dags = vec![dag("stardelt__smoke-daily"), dag("stardelt__other-daily")];
        fake.runs.insert(
            "stardelt__smoke-daily".into(),
            vec![run_started(
                "scheduled__smoke",
                "success",
                "2026-10-20T07:00:00Z",
            )],
        );
        fake.runs.insert(
            "stardelt__other-daily".into(),
            vec![run_started(
                "scheduled__other",
                "success",
                "2026-10-21T07:00:00Z",
            )],
        );
    }
    let ada = who("sub-ada", &["analytics"], false);

    let answer = runs_across_pipelines(
        Some(&resources),
        Some(&airflow),
        &ada,
        &BTreeMap::from([("notebook".to_string(), "analytics/smoke.ipynb".to_string())]),
    )
    .await
    .expect("a notebook filter succeeds");
    assert_eq!(answer.runs.len(), 1, "{:?}", answer.runs);
    assert_eq!(answer.runs[0].run.run_id, "scheduled__smoke");
    assert_eq!(
        answer.groups,
        vec!["analytics".to_string()],
        "every visible group, not only the notebook's"
    );
    let calls = fake.lock().unwrap().calls.clone();
    assert!(
        !calls.iter().any(|call| call.contains("other-daily")),
        "the filter narrows the resources before any Airflow call: {calls:?}"
    );
}

#[tokio::test]
async fn runs_notebook_filter_refuses_a_non_member() {
    let resources = memory(&[(
        "smoke-daily",
        notebook_run_resource("smoke-daily", "analytics", "smoke.ipynb"),
    )]);
    let (airflow, fake) = fake_airflow::start().await;
    let error = runs_across_pipelines(
        Some(&resources),
        Some(&airflow),
        &who("sub-1", &["ops"], false),
        &BTreeMap::from([("notebook".to_string(), "analytics/smoke.ipynb".to_string())]),
    )
    .await
    .expect_err("a non-member is refused");
    assert_eq!(error.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        error.message(),
        "you are not a member of the group analytics"
    );
    assert!(
        fake.lock().unwrap().calls.is_empty(),
        "a refused query never reaches the scheduler"
    );
}

/// One notebook schedule as the Schedule form writes it (#516): a single `run` task of
/// `kind: notebook` naming the file.
fn notebook_run_resource(name: &str, owner_group: &str, notebook: &str) -> Value {
    let mut resource = run_task_resource(name, Some(owner_group), "notebook");
    resource["spec"]["tasks"][0]["config"] = json!({ "notebook": notebook, "objectVersion": "v1" });
    resource
}

/// One `Pipeline` the Schedule form could have written: a single `run` task of `kind`.
fn run_task_resource(name: &str, owner_group: Option<&str>, kind: &str) -> Value {
    let mut resource = resource(name, owner_group);
    resource["spec"]["tasks"] = json!([{ "name": "run", "kind": kind }]);
    resource
}

/// The same run as [`run`], started at `started` (RFC 3339).
fn run_started(id: &str, state: &str, started: &str) -> Value {
    let mut run = run(id, state);
    run["start_date"] = json!(started);
    run["logical_date"] = json!(started);
    run
}

#[tokio::test]
async fn a_failing_last_run_call_does_not_fail_the_list() {
    let resources = memory(&[
        ("smoke", resource("smoke", Some("analytics"))),
        ("other", resource("other", Some("analytics"))),
    ]);
    let (airflow, fake) = fake_airflow::start().await;
    {
        let mut fake = fake.lock().unwrap();
        fake.fail_batch = true;
        fake.fail_runs.insert("stardelt__other".into());
    }

    let answer = list_pipelines(
        Some(&resources),
        Some(&airflow),
        &who("sub-ada", &["analytics"], false),
    )
    .await
    .expect("a failing batch and one broken last-run call must not fail the list");

    let smoke = answer
        .pipelines
        .iter()
        .find(|pipeline| pipeline.name == "smoke")
        .expect("smoke");
    let other = answer
        .pipelines
        .iter()
        .find(|pipeline| pipeline.name == "other")
        .expect("other");
    assert!(smoke.last_run.is_some());
    assert_eq!(smoke.state, "succeeded");
    assert_eq!(other.last_run, None, "the failed call leaves lastRun empty");
    assert_eq!(other.state, "idle");
}

// -----------------------------------------------------------------------
// Logs
// -----------------------------------------------------------------------

#[tokio::test]
async fn log_lines_drop_the_group_marker_and_ansi() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, _fake) = fake_airflow::start().await;

    let view = task_logs(
        Some(&resources),
        Some(&airflow),
        &who("sub-ada", &["analytics"], false),
        &crate::cluster::namespace(),
        "smoke",
        SMOKE_RUN,
        "insert",
        Some(3),
        None,
    )
    .await
    .expect("the log is readable");

    assert_eq!(view.lines.len(), 2, "the group marker is dropped");
    assert_eq!(view.lines[0].text, "insert done", "ANSI escapes are gone");
    assert_eq!(view.lines[1].text, "still here");

    let serialized = serde_json::to_string(&view).expect("serialises");
    assert!(
        !serialized.contains(SOURCES_CANARY),
        "the marker's object-store path must not be in the answer: {serialized}"
    );
    assert!(
        !serialized.contains(SOURCES_BODY_CANARY),
        "the group's body path must not be in the answer: {serialized}"
    );
    assert!(!serialized.contains("::endgroup::"));
    assert!(!serialized.contains("s3://"));
    assert!(!serialized.contains("::group::"));
    assert!(!serialized.contains("sources"));
}

#[tokio::test]
async fn log_answer_is_capped_and_marked_truncated() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;

    let events: Vec<Value> = (0..MAX_LOG_LINES + 1)
        .map(|index| json!({ "level": "info", "event": format!("line {index}") }))
        .collect();
    fake.lock().unwrap().log_page = json!({
        "content": events,
        "continuation_token": null
    });
    let view = task_logs(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
        SMOKE_RUN,
        "insert",
        Some(1),
        None,
    )
    .await
    .expect("the log is readable");
    assert_eq!(view.lines.len(), MAX_LOG_LINES);
    assert!(view.truncated, "more lines existed than the cap allows");

    let big: Vec<Value> = (0..600)
        .map(|_| json!({ "level": "info", "event": "x".repeat(1000) }))
        .collect();
    fake.lock().unwrap().log_page = json!({
        "content": big,
        "continuation_token": null
    });
    let view = task_logs(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
        SMOKE_RUN,
        "insert",
        Some(1),
        None,
    )
    .await
    .expect("the log is readable");
    assert!(view.lines.len() < 600, "the byte cap cut the answer");
    assert!(view.truncated);
    let bytes: usize = view.lines.iter().map(|line| line.text.len()).sum();
    assert!(bytes <= MAX_LOG_BYTES);
}

#[tokio::test]
async fn log_token_is_passed_through() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    fake.lock().unwrap().log_page = json!({
        "content": [{ "level": "info", "event": "page one" }],
        "continuation_token": "next-page-token"
    });

    let view = task_logs(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
        SMOKE_RUN,
        "insert",
        None,
        Some("page-1"),
    )
    .await
    .expect("the log is readable");
    assert_eq!(view.next_token.as_deref(), Some("next-page-token"));

    let calls = fake.lock().unwrap().calls.clone();
    let log_call = calls
        .iter()
        .rev()
        .find(|call| call.contains("/logs/"))
        .expect("a log call");
    assert!(
        log_call.contains("token=page-1"),
        "the continuation token is passed on: {log_call}"
    );
    assert!(
        log_call.contains("/logs/3?"),
        "try defaults to the task's last try (3): {log_call}"
    );
}

/// One failed event whose exception carries `frames` frames.
fn failed_event(frames: usize) -> Value {
    let frames: Vec<Value> = (0..frames)
            .map(|index| {
                json!({
                    "filename": "/home/airflow/.local/lib/python3.12/site-packages/airflow/task/task_runner.py",
                    "lineno": 1263 + index,
                    "name": format!("frame_{index}")
                })
            })
            .collect();
    json!({
        "timestamp": "2026-11-11T06:07:40.133090Z",
        "event": "Task failed with exception",
        "level": "error",
        "error_detail": [{
            "exc_type": "TrinoError",
            "exc_value": "CATALOG_NOT_FOUND: line 1:1: Catalog 'bronze' not found",
            "is_cause": false,
            "frames": frames
        }]
    })
}

#[tokio::test]
async fn log_error_detail_reaches_the_lines() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    fake.lock().unwrap().log_page = json!({
        "content": [
            { "level": "info", "event": "starting" },
            failed_event(2)
        ],
        "continuation_token": null
    });

    let view = task_logs(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
        SMOKE_RUN,
        "insert",
        Some(1),
        None,
    )
    .await
    .expect("the log is readable");

    assert!(!view.truncated);
    let texts: Vec<&str> = view.lines.iter().map(|line| line.text.as_str()).collect();
    assert_eq!(
        texts,
        vec![
            "starting",
            "Task failed with exception",
            "TrinoError: CATALOG_NOT_FOUND: line 1:1: Catalog 'bronze' not found",
            "  task_runner.py:1263 in frame_0",
            "  task_runner.py:1264 in frame_1",
        ]
    );
    assert_eq!(
        view.lines[2].level.as_deref(),
        Some("error"),
        "the exception is an error level line"
    );
    assert_eq!(view.lines[2].timestamp, None, "the detail has no timestamp");
    assert_eq!(view.lines[3].level, None, "a frame carries no level");
}

#[tokio::test]
async fn an_event_without_error_detail_is_unchanged() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    fake.lock().unwrap().log_page = json!({
        "content": [
            { "level": "info", "event": "starting" },
            {
                "level": "error",
                "event": "Task failed with exception",
                "error_detail": []
            }
        ],
        "continuation_token": null
    });

    let view = task_logs(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
        SMOKE_RUN,
        "insert",
        Some(1),
        None,
    )
    .await
    .expect("the log is readable");

    assert_eq!(view.lines.len(), 2, "an empty detail appends nothing");
    assert_eq!(view.lines[1].text, "Task failed with exception");
    assert_eq!(view.lines[1].level.as_deref(), Some("error"));
    assert!(!view.truncated);
}

#[tokio::test]
async fn a_long_traceback_is_cut_to_the_frame_cap() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    fake.lock().unwrap().log_page = json!({
        "content": [failed_event(30)],
        "continuation_token": null
    });

    let view = task_logs(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
        SMOKE_RUN,
        "insert",
        Some(1),
        None,
    )
    .await
    .expect("the log is readable");

    let frames = view.lines.iter().filter(|line| line.text.starts_with("  "));
    assert_eq!(frames.count(), MAX_ERROR_FRAMES);
    assert_eq!(view.lines.len(), 1 + 1 + MAX_ERROR_FRAMES);
}

#[tokio::test]
async fn the_error_detail_counts_against_the_line_cap() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    let mut events: Vec<Value> = (0..MAX_LOG_LINES - 5)
        .map(|index| json!({ "level": "info", "event": format!("line {index}") }))
        .collect();
    events.push(failed_event(30));
    fake.lock().unwrap().log_page = json!({
        "content": events,
        "continuation_token": null
    });

    let view = task_logs(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
        SMOKE_RUN,
        "insert",
        Some(1),
        None,
    )
    .await
    .expect("the log is readable");

    assert!(
        view.lines.len() <= MAX_LOG_LINES,
        "the page stayed under the line cap: {}",
        view.lines.len()
    );
    assert!(view.truncated, "the detail overflowed the page");
}

// -----------------------------------------------------------------------
// A run id never crosses pipelines
// -----------------------------------------------------------------------
#[tokio::test]
async fn a_run_id_of_another_pipeline_does_not_resolve() {
    let resources = memory(&[
        ("smoke", resource("smoke", Some("analytics"))),
        ("other", resource("other", Some("analytics"))),
    ]);
    let (airflow, fake) = fake_airflow::start().await;

    let error = run_detail(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
        OTHER_RUN,
    )
    .await
    .expect_err("other's run id must not resolve under smoke");
    assert_eq!(error.status(), StatusCode::NOT_FOUND);
    assert_eq!(error.message(), "no such run");

    let calls = fake.lock().unwrap().calls.clone();
    assert!(
        !calls.iter().any(|call| call.contains("stardelt__other")),
        "the other pipeline's DAG was never addressed: {calls:?}"
    );
    assert!(
        calls.iter().all(|call| call.contains("stardelt__smoke")),
        "every Airflow path used smoke's DAG id: {calls:?}"
    );
}

// -----------------------------------------------------------------------
// Upstreams, headers, view shape
// -----------------------------------------------------------------------

#[tokio::test]
async fn airflow_down_is_502_and_not_configured_is_503_for_every_action() {
    let resources = memory(&[(
        "smoke",
        with_concurrency(resource("smoke", Some("analytics")), "Allow"),
    )]);
    let namespace = crate::cluster::namespace();
    let root = admin();
    let limiter = limiter();

    // No scheduler client: every action answers 503 and names the missing client.
    let error = run_now(
        Some(&resources),
        None,
        &limiter,
        &root,
        &namespace,
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect_err("no Airflow client");
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.message(), "the scheduler is not configured");
    let error = pause_pipeline(Some(&resources), None, &root, &namespace, "smoke")
        .await
        .expect_err("no Airflow client");
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.message(), "the scheduler is not configured");
    let error = resume_pipeline(Some(&resources), None, &root, &namespace, "smoke")
        .await
        .expect_err("no Airflow client");
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.message(), "the scheduler is not configured");
    let error = stop_run(
        Some(&resources),
        None,
        &root,
        &namespace,
        "smoke",
        SMOKE_RUN,
    )
    .await
    .expect_err("no Airflow client");
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.message(), "the scheduler is not configured");

    // No cluster client at all: the read routes still say which one is missing.
    let error = list_pipelines(None, None, &root)
        .await
        .expect_err("no cluster client");
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.message(), "no cluster connection");

    // An Airflow that does not answer: 502, never 500, for every action.
    let down = AirflowClient::new(
        reqwest::Client::new(),
        AirflowConfig {
            url: "http://127.0.0.1:9".into(),
            user: "nova-api".into(),
            password: Secret::new("x"),
        },
    );
    for (action, error) in [
        (
            "run",
            run_now(
                Some(&resources),
                Some(&down),
                &limiter,
                &root,
                &namespace,
                "smoke",
                &RunStart::default(),
            )
            .await
            .expect_err("Airflow is down"),
        ),
        (
            "pause",
            pause_pipeline(Some(&resources), Some(&down), &root, &namespace, "smoke")
                .await
                .expect_err("Airflow is down"),
        ),
        (
            "resume",
            resume_pipeline(Some(&resources), Some(&down), &root, &namespace, "smoke")
                .await
                .expect_err("Airflow is down"),
        ),
        (
            "stop",
            stop_run(
                Some(&resources),
                Some(&down),
                &root,
                &namespace,
                "smoke",
                SMOKE_RUN,
            )
            .await
            .expect_err("Airflow is down"),
        ),
    ] {
        assert_eq!(error.status(), StatusCode::BAD_GATEWAY, "{action}");
        assert_eq!(error.message(), "the scheduler did not answer", "{action}");
    }
}

#[test]
fn a_refused_sign_in_is_not_reported_as_an_unanswered_scheduler() {
    let fail = scheduler_fail(AirflowError::Refused("401".into()));
    assert_eq!(fail.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(fail.message(), "the scheduler refused Nova's service user");
}

#[test]
fn an_unreachable_airflow_still_says_it_did_not_answer() {
    let fail = scheduler_fail(AirflowError::Unavailable("connect refused".into()));
    assert_eq!(fail.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(fail.message(), "the scheduler did not answer");
}

#[tokio::test]
async fn responses_are_not_cacheable() {
    let (store, _store_fake) = crate::secret_store::fake::start().await;
    let (airflow, _airflow_fake) = fake_airflow::start().await;
    let kube = fake_kube::start(vec![resource("smoke", Some("analytics"))]).await;
    let app = router()
        .with_state(test_state(Some(kube), airflow, store))
        .layer(Extension(principal(admin())));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/pipelines")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store"),
        "a successful pipeline answer is never cached"
    );
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["pipelines"][0]["name"], "smoke");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/pipelines/{}/no-such-pipeline",
                    crate::cluster::namespace()
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store"),
        "a refusal is never cached either"
    );

    let namespace = crate::cluster::namespace();
    for (uri, expected) in [
        (
            format!("/pipelines/{namespace}/smoke/runs"),
            StatusCode::CREATED,
        ),
        (
            format!("/pipelines/{namespace}/smoke/pause"),
            StatusCode::OK,
        ),
        (
            format!("/pipelines/{namespace}/smoke/resume"),
            StatusCode::OK,
        ),
        (
            format!("/pipelines/{namespace}/smoke/runs/{SMOKE_RUN}/stop"),
            StatusCode::CONFLICT,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{uri}");
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store"),
            "every action answer, success or refusal, is uncacheable: {uri}"
        );
    }
}

#[test]
fn the_view_has_no_raw_airflow_fields() {
    let resource = resource("smoke", Some("analytics"));
    let dag: DagSummary = serde_json::from_value(json!({
        "dag_id": "stardelt__smoke",
        "is_paused": false,
        "is_stale": false,
        "next_dagrun_run_after": "2026-10-21T03:00:00Z",
        "tags": [{ "name": "pipeline" }]
    }))
    .unwrap();
    let run: DagRun = serde_json::from_value(run(SMOKE_RUN, "success")).unwrap();
    let view = view_of(
        "stardelt",
        "smoke",
        &resource,
        Some(&dag),
        Some(&run),
        &who("sub-ada", &["analytics"], false),
    );

    let value = serde_json::to_value(&view).expect("serialises");
    let keys: BTreeSet<&str> = value
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        BTreeSet::from([
            "name",
            "namespace",
            "ownerGroup",
            "canRun",
            "canEdit",
            "schedule",
            "paused",
            "loaded",
            "nextRun",
            "state",
            "lastRun",
            "tasks",
            "runAs",
            "concurrency",
            "publish",
            "createdAt",
        ])
    );
    let serialized = serde_json::to_string(&value).unwrap();
    for raw in [
        "dag_id",
        "is_paused",
        "is_stale",
        "next_dagrun_run_after",
        "dag_run_id",
        "run_type",
        "start_date",
        "end_date",
        "try_number",
        "sources",
        "continuation_token",
    ] {
        assert!(
            !serialized.contains(raw),
            "the view must not carry the raw Airflow field {raw}: {serialized}"
        );
    }

    let run = view.last_run.expect("a last run");
    let run = serde_json::to_value(&run).unwrap();
    let keys: BTreeSet<&str> = run
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        BTreeSet::from([
            "runId",
            "state",
            "type",
            "logicalDate",
            "startedAt",
            "endedAt",
            "durationSeconds",
            "triggeredBy",
            "note",
        ])
    );
    let duration = run["durationSeconds"].as_f64().expect("a duration");
    assert!(
        (duration - 8.268047).abs() < 1e-9,
        "the run duration is the difference of its timestamps: {duration}"
    );
}

/// `spec.description` is a sentence a person reads on the Pipelines page (#327): it reaches the
/// view unchanged, and a pipeline without one carries no key at all.
#[test]
fn a_pipeline_description_reaches_the_view() {
    let mut described = resource("shop", Some("analytics"));
    described["spec"]["description"] = json!("Generates the shop's bronze layer");
    let view = view_of("stardelt", "shop", &described, None, None, &admin());
    assert_eq!(
        view.description.as_deref(),
        Some("Generates the shop's bronze layer")
    );
    assert_eq!(
        serde_json::to_value(&view).unwrap()["description"],
        json!("Generates the shop's bronze layer")
    );

    let plain = view_of(
        "stardelt",
        "shop",
        &resource("shop", Some("analytics")),
        None,
        None,
        &admin(),
    );
    assert_eq!(plain.description, None);
    assert!(
        !serde_json::to_value(&plain)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("description"),
        "a pipeline without a description leaves the key out, rather than sending null"
    );
}

/// A create writes the body's line as `spec.description`, and leaves the key out of a body
/// that names none (#997).
#[test]
fn a_create_writes_the_description() {
    let described = pipeline_resource(
        "shop-bronze",
        "analytics",
        &Statement::Sql("select 1".into()),
        None,
        &BTreeMap::new(),
        &default_options(),
        "sub-ada",
        None,
        &[],
        Some("loads the shop"),
        None,
    );
    assert_eq!(spec_of(&described)["description"], json!("loads the shop"));

    let plain = pipeline_resource(
        "shop-bronze",
        "analytics",
        &Statement::Sql("select 1".into()),
        None,
        &BTreeMap::new(),
        &default_options(),
        "sub-ada",
        None,
        &[],
        None,
        None,
    );
    assert!(
        spec_of(&plain).get("description").is_none(),
        "a body that names no description writes no key"
    );
}

/// The update body describes the whole editable state: a line replaces the stored one, and a
/// body without one removes it (#997).
#[test]
fn an_update_replaces_and_clears_the_description() {
    let mut stored = resource("shop-bronze", Some("analytics"));
    stored["spec"]["description"] = json!("the old line");
    let statement = Statement::Sql("select 2".into());

    let replaced = updated_resource(
        &stored,
        &statement,
        &default_options(),
        &BTreeMap::new(),
        None,
        None,
        None,
        Some("the new line"),
        None,
    );
    assert_eq!(spec_of(&replaced)["description"], json!("the new line"));

    let cleared = updated_resource(
        &stored,
        &statement,
        &default_options(),
        &BTreeMap::new(),
        None,
        None,
        None,
        None,
        None,
    );
    assert!(
        spec_of(&cleared).get("description").is_none(),
        "a body without a description removes the stored line"
    );
}

/// The CRD's bound: a line of at most 200 characters, counted in characters, not bytes.
#[test]
fn a_description_longer_than_200_characters_is_refused() {
    assert_eq!(
        check_description(Some(&"ü".repeat(201))).unwrap_err(),
        "a pipeline description is at most 200 characters"
    );
    assert_eq!(
        check_description(Some(&"ü".repeat(200)))
            .unwrap()
            .as_deref(),
        Some("ü".repeat(200).as_str())
    );
}

/// A form that sends an empty or whitespace-only line clears it rather than writing `""`.
#[test]
fn a_blank_description_is_none() {
    assert_eq!(check_description(Some("")), Ok(None));
    assert_eq!(check_description(Some("  ")), Ok(None));
    assert_eq!(check_description(None), Ok(None));
    assert_eq!(
        check_description(Some("  loads the shop  ")),
        Ok(Some("loads the shop".to_string()))
    );
}

/// `pipelines_spec` answers the line, so an edit sent back as it was read keeps it; a
/// resource without one leaves the key out (#997).
#[test]
fn spec_response_carries_the_description() {
    let mut described = resource("shop-bronze", Some("analytics"));
    described["spec"]["description"] = json!("Generates the shop's bronze layer");
    let document = spec_response(&described).document;
    assert_eq!(
        document.description.as_deref(),
        Some("Generates the shop's bronze layer")
    );
    assert_eq!(
        serde_json::to_value(&document).unwrap()["description"],
        json!("Generates the shop's bronze layer")
    );

    let plain = spec_response(&resource("shop-bronze", Some("analytics"))).document;
    assert_eq!(plain.description, None);
    assert!(
        !serde_json::to_value(&plain)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("description"),
        "a resource without a description leaves the key out"
    );
}

#[tokio::test]
async fn detail_carries_params_retries_and_schedule() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, _fake) = fake_airflow::start().await;

    let detail = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
    )
    .await
    .expect("the detail is readable");

    assert_eq!(
        detail.pipeline.schedule.as_deref(),
        Some("0 3 * * * (Europe/Berlin)")
    );
    assert_eq!(
        detail.params.get("target").map(String::as_str),
        Some("warehouse.smoke")
    );
    let retries = detail.retries.expect("a retry policy");
    assert_eq!(retries.attempts, 2);
    assert_eq!(retries.backoff.as_deref(), Some("5m"));
    assert_eq!(detail.runs.len(), 1);
    assert_eq!(detail.pipeline.last_run.as_ref().unwrap().run_id, SMOKE_RUN);
}

#[tokio::test]
async fn detail_carries_the_statement() {
    let mut smoke = resource("smoke", Some("analytics"));
    smoke["spec"]["tasks"] = json!([{ "name": "run", "kind": "sql", "sql": "select 1" }]);
    let resources = memory(&[("smoke", smoke)]);
    let (airflow, _fake) = fake_airflow::start().await;

    let detail = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
    )
    .await
    .expect("the detail is readable");

    assert_eq!(detail.pipeline.tasks.len(), 1);
    assert_eq!(detail.pipeline.tasks[0].sql.as_deref(), Some("select 1"));
}

#[tokio::test]
async fn detail_includes_airflow_url_from_base_domain() {
    let _base_domain = BASE_DOMAIN_LOCK.lock().await;
    unsafe {
        std::env::set_var("NOVA_BASE_DOMAIN", "alpha.dev.stardelt.io");
    }
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, _fake) = fake_airflow::start().await;

    let detail = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
    )
    .await
    .expect("the detail is readable");

    assert_eq!(
        detail.airflow_url.as_deref(),
        Some("https://airflow.alpha.dev.stardelt.io/dags/stardelt__smoke")
    );
    unsafe {
        std::env::remove_var("NOVA_BASE_DOMAIN");
    }
}

#[tokio::test]
async fn detail_hides_airflow_url_from_non_admins() {
    let _base_domain = BASE_DOMAIN_LOCK.lock().await;
    unsafe {
        std::env::set_var("NOVA_BASE_DOMAIN", "alpha.dev.stardelt.io");
    }
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, _fake) = fake_airflow::start().await;

    let detail = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &who("sub-ada", &["analytics"], false),
        &crate::cluster::namespace(),
        "smoke",
    )
    .await
    .expect("the detail is readable");

    assert!(
        detail.airflow_url.is_none(),
        "a non-administrator is not sent to a DAG page that answers 403"
    );
    unsafe {
        std::env::remove_var("NOVA_BASE_DOMAIN");
    }
}

#[tokio::test]
async fn detail_airflow_url_none_when_base_domain_unset() {
    let _base_domain = BASE_DOMAIN_LOCK.lock().await;
    unsafe {
        std::env::remove_var("NOVA_BASE_DOMAIN");
    }
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, _fake) = fake_airflow::start().await;

    let detail = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
    )
    .await
    .expect("the detail is readable");

    assert!(detail.airflow_url.is_none());
}

#[tokio::test]
async fn list_does_not_carry_the_statement() {
    let mut smoke = resource("smoke", Some("analytics"));
    smoke["spec"]["tasks"] = json!([{ "name": "run", "kind": "sql", "sql": "select 1" }]);
    let resources = memory(&[("smoke", smoke)]);
    let (airflow, _fake) = fake_airflow::start().await;

    let answer = list_pipelines(Some(&resources), Some(&airflow), &admin())
        .await
        .expect("the list succeeds");

    let tasks = &answer.pipelines[0].tasks;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].sql, None);
    let value = serde_json::to_value(&tasks[0]).expect("serialises");
    assert!(
        value.get("sql").is_none(),
        "the list does not carry the statement: {value}"
    );
}

#[tokio::test]
async fn detail_task_without_statement_has_none() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, _fake) = fake_airflow::start().await;

    let detail = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
    )
    .await
    .expect("the detail is readable");

    assert!(
        !detail.pipeline.tasks.is_empty(),
        "the fixture declares tasks"
    );
    for task in &detail.pipeline.tasks {
        assert_eq!(task.sql, None, "{} has no statement", task.name);
    }
}

#[test]
fn timestamps_parse_with_fractions_and_offsets() {
    assert_eq!(parse_rfc3339("1970-01-01T00:00:00Z"), Some(0.0));
    assert_eq!(
        parse_rfc3339("2026-10-20T07:23:50.608876Z"),
        Some(1_792_481_030.608_876)
    );
    assert_eq!(
        parse_rfc3339("2026-10-20T07:23:50+00:00"),
        parse_rfc3339("2026-10-20T07:23:50Z")
    );
    assert_eq!(
        parse_rfc3339("2026-10-20T09:23:50+02:00"),
        parse_rfc3339("2026-10-20T07:23:50Z")
    );
    assert_eq!(parse_rfc3339("not a date"), None);
    assert_eq!(parse_rfc3339("2026-10-20T07:23:50Zjunk"), None);
    let duration = duration_seconds(
        Some("2026-10-20T07:23:50.608876Z"),
        Some("2026-10-20T07:23:58.876923Z"),
    )
    .expect("a duration");
    assert!(
        (duration - 8.268047).abs() < 1e-9,
        "fractions are preserved: {duration}"
    );
    assert_eq!(duration_seconds(None, Some("2026-10-20T07:23:58Z")), None);
}

#[test]
fn ansi_sequences_are_removed_but_text_survives() {
    assert_eq!(strip_ansi("\u{1b}[32minsert\u{1b}[0m done"), "insert done");
    assert_eq!(strip_ansi("plain"), "plain");
    assert_eq!(strip_ansi("grün \u{1b}[1;31mrot\u{1b}[0m"), "grün rot");
    // The ticket's pattern matches `ESC [ n` as well: the text loses those three bytes.
    assert_eq!(strip_ansi("\u{1b}[not-a-sequence"), "ot-a-sequence");
}

#[test]
fn a_check_line_carries_its_verdict() {
    assert_eq!(
        check_result("check no_null_ids: error, 0 row(s)"),
        Some(("no_null_ids".to_string(), "error".to_string(), 0))
    );
    assert_eq!(
        check_result("check unique_ids: warn, 3 row(s)"),
        Some(("unique_ids".to_string(), "warn".to_string(), 3))
    );
    assert_eq!(
        check_result("INFO - check a: error, 1 row"),
        Some(("a".to_string(), "error".to_string(), 1))
    );
    assert_eq!(check_result("Task failed with exception"), None);
    assert_eq!(check_result("check a: fatal, 2 row(s)"), None);
}

#[test]
fn the_quality_tasks_of_a_pipeline_carry_their_checks() {
    let pipeline = serde_json::json!({"spec": {"tasks": [
        {"name": "load", "kind": "notebook"},
        {"name": "gate", "kind": "quality", "checks": [
            {"name": "a", "sql": "SELECT 1"},
            {"name": "b", "sql": "SELECT 2", "severity": "warn"}
        ]}
    ]}});
    let tasks = quality_tasks(&pipeline);
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].0, "gate");
    assert_eq!(
        tasks[0].1,
        vec![
            ("a".to_string(), "error".to_string()),
            ("b".to_string(), "warn".to_string())
        ]
    );
}

#[test]
fn audit_branch_matches_the_loader() {
    assert_eq!(
        audit_branch("manual__2026-11-22T01:00:00+00:00"),
        "audit_49229452c902639d"
    );
}

#[test]
fn a_passing_wap_run_has_no_audit_branch_to_inspect() {
    let instance = |task: &str, state: &str| crate::airflow::TaskInstance {
        task_id: task.to_string(),
        state: Some(state.to_string()),
        try_number: Some(1),
        start_date: None,
        end_date: None,
        duration: None,
        operator: None,
        map_index: None,
    };
    assert!(wap_branch_present(
        Some("wap"),
        &[
            instance("wap-prepare", "success"),
            instance("wap-publish", "upstream_failed"),
        ],
    ));
    assert!(wap_branch_present(
        Some("wap"),
        &[instance("wap-prepare", "success")],
    ));
    assert!(!wap_branch_present(
        Some("wap"),
        &[
            instance("wap-prepare", "success"),
            instance("wap-publish", "success"),
        ],
    ));
    assert!(!wap_branch_present(
        Some("wap"),
        &[instance("wap-prepare", "failed")],
    ));
    assert!(!wap_branch_present(
        Some("direct"),
        &[instance("wap-prepare", "success")],
    ));
}

#[test]
fn audited_tables_reads_the_writes_lists() {
    let pipeline = serde_json::json!({"spec": {"tasks": [
        {"name": "a", "kind": "spark", "writes": ["gold.shop.t"]},
        {"name": "b", "kind": "spark", "writes": ["gold.shop.t", "gold.shop.u"]},
        {"name": "c", "kind": "notebook"}
    ]}});
    assert_eq!(
        audited_tables(&pipeline),
        vec!["gold.shop.t".to_string(), "gold.shop.u".to_string()]
    );
    assert!(audited_tables(&serde_json::json!({"spec": {"publish": "direct"}})).is_empty());
}

// -----------------------------------------------------------------------
// Actions: who may act, run now, pause, resume, stop, audit
// -----------------------------------------------------------------------

/// Set the fake's DAG paused state, as an operator would through Airflow.
fn set_dag_paused(fake: &fake_airflow::Shared, dag_id: &str, paused: bool) {
    for dag in &mut fake.lock().unwrap().dags {
        if dag["dag_id"] == dag_id {
            dag["is_paused"] = json!(paused);
        }
    }
}

#[tokio::test]
async fn only_the_owner_group_and_admins_may_act_and_others_get_404() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    let outsider = who("sub-bob", &["finance"], false);
    let namespace = crate::cluster::namespace();
    let limiter = limiter();

    for (action, error) in [
        (
            "run",
            run_now(
                Some(&resources),
                Some(&airflow),
                &limiter,
                &outsider,
                &namespace,
                "smoke",
                &RunStart::default(),
            )
            .await
            .expect_err("an outsider may not run"),
        ),
        (
            "pause",
            pause_pipeline(
                Some(&resources),
                Some(&airflow),
                &outsider,
                &namespace,
                "smoke",
            )
            .await
            .expect_err("an outsider may not pause"),
        ),
        (
            "resume",
            resume_pipeline(
                Some(&resources),
                Some(&airflow),
                &outsider,
                &namespace,
                "smoke",
            )
            .await
            .expect_err("an outsider may not resume"),
        ),
        (
            "stop",
            stop_run(
                Some(&resources),
                Some(&airflow),
                &outsider,
                &namespace,
                "smoke",
                SMOKE_RUN,
            )
            .await
            .expect_err("an outsider may not stop"),
        ),
    ] {
        assert_eq!(error.status(), StatusCode::NOT_FOUND, "{action}");
        assert_eq!(error.message(), "no such pipeline", "{action}");
    }
    assert!(
        fake.lock().unwrap().calls.is_empty(),
        "a hidden pipeline never reaches Airflow: {:?}",
        fake.lock().unwrap().calls
    );

    let owned = resource("smoke", Some("analytics"));
    let spec = spec_of(&owned);
    assert!(may_act(&who("sub-ada", &["analytics"], false), spec));
    assert!(may_act(&admin(), spec));
    assert!(!may_act(&outsider, spec));
}

/// A shared pipeline (epic#895 decision 12): `spec.viewers` may read it and run it; changing
/// it stays the owner group's.
fn shared() -> Value {
    with_viewers(resource("shop", Some("demo")), &["visitors"])
}

#[test]
fn visible_admits_a_viewer_while_may_act_refuses_it() {
    let shared = shared();
    let spec = spec_of(&shared);

    let viewer = who("sub-vera", &["visitors"], false);
    assert!(visible(&viewer, spec), "a viewer sees the pipeline");
    assert!(!may_act(&viewer, spec), "a viewer may not act on it");

    let owner = who("sub-dana", &["demo"], false);
    assert!(visible(&owner, spec), "the owner group sees it");
    assert!(may_act(&owner, spec), "the owner group acts on it");

    assert!(visible(&admin(), spec), "an administrator sees it");
    assert!(may_act(&admin(), spec), "an administrator acts on it");

    let outsider = who("sub-bob", &["finance"], false);
    assert!(!visible(&outsider, spec));
    assert!(!may_act(&outsider, spec));
}

#[tokio::test]
async fn a_viewer_runs_the_shared_pipeline_and_the_run_is_audited_as_the_viewer() {
    let resources = memory(&[
        ("shop", shared()),
        ("smoke", resource("smoke", Some("analytics"))),
    ]);
    let (airflow, fake) = fake_airflow::start().await;
    // The loader has created the shared pipeline's DAG, with no run yet.
    {
        let mut fake = fake.lock().unwrap();
        fake.dags.push(dag("stardelt__shop"));
        fake.runs.insert("stardelt__shop".into(), Vec::new());
    }
    let viewer = who("sub-vera-807", &["visitors"], false);
    let namespace = crate::cluster::namespace();
    let _ = audit::test_sink::take_for("sub-vera-807");

    // The list and the detail show a pipeline the viewer may run.
    let answer = list_pipelines(Some(&resources), Some(&airflow), &viewer)
        .await
        .expect("the list succeeds");
    let names: Vec<&str> = answer
        .pipelines
        .iter()
        .map(|pipeline| pipeline.name.as_str())
        .collect();
    assert_eq!(names, ["shop"], "the viewer's list has the shared pipeline");
    assert!(answer.pipelines[0].can_run, "the viewer may run it");
    assert!(
        !answer.pipelines[0].can_edit,
        "the viewer may not change it"
    );

    let detail = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &viewer,
        &namespace,
        "shop",
    )
    .await
    .expect("the viewer reads the detail");
    assert!(detail.pipeline.can_run, "a viewer's detail may run");
    assert!(
        !detail.pipeline.can_edit,
        "a viewer's detail may not change"
    );
    pipeline_spec(Some(&resources), &viewer, &namespace, "shop")
        .await
        .expect("the viewer reads the spec");

    // Run now: a viewer starts the owner's statement, as the viewer.
    let run = run_now(
        Some(&resources),
        Some(&airflow),
        &limiter(),
        &viewer,
        &namespace,
        "shop",
        &RunStart::default(),
    )
    .await
    .expect("a viewer may run the shared pipeline");
    assert_eq!(run.state, "queued");
    assert_eq!(
        run.note.as_deref(),
        Some("Started from Nova by sub-vera-807")
    );
    {
        let fake = fake.lock().unwrap();
        assert_eq!(
            fake.trigger_bodies.len(),
            1,
            "Airflow was asked to trigger: {:?}",
            fake.calls
        );
        assert_eq!(
            fake.trigger_bodies[0]["note"],
            "Started from Nova by sub-vera-807"
        );
        assert!(
            fake.calls
                .iter()
                .any(|call| call == "POST /api/v2/dags/stardelt__shop/dagRuns"),
            "the trigger names the shared pipeline: {:?}",
            fake.calls
        );
    }

    let records = audit::test_sink::take_for("sub-vera-807");
    assert_eq!(
        records.len(),
        1,
        "one audit record for the run: {records:?}"
    );
    assert_eq!(records[0].action, "run");
    assert_eq!(records[0].outcome, "ok");
    assert_eq!(records[0].sub, "sub-vera-807", "the viewer is the actor");
    assert_eq!(records[0].pipeline, "shop");
    assert_eq!(records[0].run.as_deref(), Some(run.run_id.as_str()));
}

#[tokio::test]
async fn a_viewer_who_changes_the_shared_pipeline_gets_403_and_the_reason() {
    let resources = memory(&[("shop", shared())]);
    let (airflow, fake) = fake_airflow::start().await;
    let viewer = who("sub-vera", &["visitors"], false);
    let namespace = crate::cluster::namespace();
    let calls_before = fake.lock().unwrap().calls.clone();

    let error = pause_pipeline(
        Some(&resources),
        Some(&airflow),
        &viewer,
        &namespace,
        "shop",
    )
    .await
    .expect_err("a viewer may not pause");
    assert_eq!(error.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        error.message(),
        "only the owner group may change this pipeline"
    );

    let error = update_pipeline(
        Some(&resources),
        None,
        None,
        Some(&airflow),
        &viewer,
        &namespace,
        "shop",
        update_body("select 2;"),
    )
    .await
    .expect_err("a viewer may not change the spec");
    assert_eq!(error.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        error.message(),
        "only the owner group may change this pipeline"
    );

    assert_eq!(
        fake.lock().unwrap().calls,
        calls_before,
        "a refused change never reaches Airflow"
    );
    let stored = resources
        .get("shop")
        .await
        .expect("the store answers")
        .expect("the pipeline is still there");
    assert_eq!(
        stored["spec"]["tasks"][0]["name"], "schema",
        "the refused update wrote nothing"
    );
}

#[tokio::test]
async fn a_hidden_pipeline_still_answers_404_for_a_run_and_a_change() {
    let resources = memory(&[("hidden", resource("hidden", Some("other-team")))]);
    let (airflow, fake) = fake_airflow::start().await;
    let outsider = who("sub-vera", &["visitors"], false);
    let namespace = crate::cluster::namespace();

    for (action, error) in [
        (
            "run",
            run_now(
                Some(&resources),
                Some(&airflow),
                &limiter(),
                &outsider,
                &namespace,
                "hidden",
                &RunStart::default(),
            )
            .await
            .expect_err("an outsider may not run"),
        ),
        (
            "pause",
            pause_pipeline(
                Some(&resources),
                Some(&airflow),
                &outsider,
                &namespace,
                "hidden",
            )
            .await
            .expect_err("an outsider may not pause"),
        ),
        (
            "update",
            update_pipeline(
                Some(&resources),
                None,
                None,
                Some(&airflow),
                &outsider,
                &namespace,
                "hidden",
                update_body("select 2;"),
            )
            .await
            .expect_err("an outsider may not change"),
        ),
    ] {
        assert_eq!(error.status(), StatusCode::NOT_FOUND, "{action}");
        assert_eq!(error.message(), "no such pipeline", "{action}");
    }
    assert!(
        fake.lock().unwrap().calls.is_empty(),
        "a hidden pipeline never reaches Airflow: {:?}",
        fake.lock().unwrap().calls
    );
}

#[test]
fn can_run_and_can_edit_differ_for_a_viewer_an_owner_and_an_admin() {
    let shared = shared();
    let viewer = who("sub-vera", &["visitors"], false);
    let owner = who("sub-dana", &["demo"], false);

    let view = view_of("stardelt", "shop", &shared, None, None, &viewer);
    assert!(view.can_run, "a viewer may run");
    assert!(!view.can_edit, "a viewer may not change");

    let view = view_of("stardelt", "shop", &shared, None, None, &owner);
    assert!(view.can_run, "the owner may run");
    assert!(view.can_edit, "the owner may change");

    let view = view_of("stardelt", "shop", &shared, None, None, &admin());
    assert!(view.can_run, "an administrator may run");
    assert!(view.can_edit, "an administrator may change");
}

#[tokio::test]
async fn run_now_triggers_with_now_and_a_note() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    let member = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let started = now_seconds();
    let run = run_now(
        Some(&resources),
        Some(&airflow),
        &limiter(),
        &member,
        &namespace,
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect("the run starts");
    assert_eq!(run.state, "queued");
    assert_eq!(run.note.as_deref(), Some("Started from Nova by sub-ada"));
    assert!(run.run_id.starts_with("manual__"));

    let fake = fake.lock().unwrap();
    assert_eq!(fake.trigger_bodies.len(), 1, "exactly one trigger");
    let body = &fake.trigger_bodies[0];
    let logical_date = body["logical_date"].as_str().expect("a logical_date");
    assert!(
        logical_date.ends_with('Z'),
        "always UTC with a Z: {logical_date}"
    );
    assert!(
        logical_date
            .split('.')
            .nth(1)
            .is_some_and(|f| f.len() == 7 && f.ends_with('Z')),
        "microseconds: {logical_date}"
    );
    let at = parse_rfc3339(logical_date).expect("RFC 3339");
    assert!(
        (at - started).abs() <= 2.0,
        "logical_date is now, not a guess: {logical_date} vs {started}"
    );
    assert_eq!(body["note"], "Started from Nova by sub-ada");
    assert!(
        fake.calls
            .iter()
            .any(|call| call == "POST /api/v2/dags/stardelt__smoke/dagRuns"),
        "Airflow was asked to trigger: {:?}",
        fake.calls
    );
}

/// The run's note names the deployment it came from, read back as the run view's fields
/// (#105).
#[tokio::test]
async fn run_now_carries_the_deploy_provenance_in_its_note() {
    let mut smoke = resource("smoke", Some("analytics"));
    stamp_provenance(
        &mut smoke,
        &Provenance {
            bundle: "shop-etl".into(),
            target: "prod".into(),
            revision: Some("9f3c2a1".into()),
            repository: None,
        },
        "alex",
    );
    let resources = memory(&[("smoke", smoke)]);
    let (airflow, fake) = fake_airflow::start().await;
    let member = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let run = run_now(
        Some(&resources),
        Some(&airflow),
        &limiter(),
        &member,
        &namespace,
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect("the run starts");

    assert_eq!(run.bundle.as_deref(), Some("shop-etl"));
    assert_eq!(run.target.as_deref(), Some("prod"));
    assert_eq!(run.revision.as_deref(), Some("9f3c2a1"));
    assert_eq!(
        run.note.as_deref(),
        Some(
            "Started from Nova by sub-ada [stardelt bundle=shop-etl target=prod revision=9f3c2a1]"
        )
    );

    let fake = fake.lock().unwrap();
    assert_eq!(fake.trigger_bodies.len(), 1, "exactly one trigger");
    let note = fake.trigger_bodies[0]["note"]
        .as_str()
        .expect("the trigger carries a note");
    assert!(note.contains("bundle=shop-etl"), "the bundle: {note}");
    assert!(note.contains("target=prod"), "the target: {note}");
    assert!(note.contains("revision=9f3c2a1"), "the revision: {note}");
}

/// A note with the provenance suffix answers its parts; anything else answers nothing (#105).
#[test]
fn run_provenance_reads_the_note_suffix() {
    assert_eq!(
        run_provenance(
            "Started from Nova by Ada [stardelt bundle=shop-etl target=prod revision=9f3c2a1]"
        ),
        Some((
            "shop-etl".to_string(),
            "prod".to_string(),
            Some("9f3c2a1".to_string())
        ))
    );
    assert_eq!(
        run_provenance("Started from Nova by Ada [stardelt bundle=shop-etl target=prod]"),
        Some(("shop-etl".to_string(), "prod".to_string(), None)),
        "a deployment with no revision names none"
    );
    assert_eq!(
        run_provenance("Started from Nova by Ada"),
        None,
        "a note without a suffix answers nothing"
    );
    assert_eq!(
        run_provenance("Started from Nova by Ada [stardelt bundle=shop-etl"),
        None,
        "an unterminated suffix answers nothing"
    );
}

/// The route with a run date and parameters: they reach Airflow as `conf`, and the logical
/// date stays now (nova#817).
#[tokio::test]
async fn pipelines_run_trigger_sends_the_run_date_and_params_as_conf() {
    let (airflow, fake) = fake_airflow::start().await;
    let app = run_route(airflow).await;

    let started = now_seconds();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/pipelines/{}/smoke/runs",
                    crate::cluster::namespace()
                ))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "runDate": "2026-11-13", "params": { "days": "7" } }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let fake = fake.lock().unwrap();
    assert_eq!(fake.trigger_bodies.len(), 1, "exactly one trigger");
    let body = &fake.trigger_bodies[0];
    assert_eq!(
        body["conf"]["runDate"], "2026-11-13",
        "the date travels as conf: {body}"
    );
    assert_eq!(body["conf"]["params"]["days"], "7", "the params travel too");
    let logical_date = body["logical_date"].as_str().expect("a logical_date");
    assert_ne!(logical_date, "2026-11-13", "the logical date stays now");
    assert!(
        logical_date
            .split('.')
            .nth(1)
            .is_some_and(|f| f.len() == 7 && f.ends_with('Z')),
        "now, with microseconds, UTC: {logical_date}"
    );
    let at = parse_rfc3339(logical_date).expect("RFC 3339");
    assert!(
        (at - started).abs() <= 2.0,
        "logical_date is now, not the run date: {logical_date} vs {started}"
    );
}

/// No body, exactly as the interface posts today: no `conf` key at all (nova#817).
#[tokio::test]
async fn pipelines_run_trigger_without_a_body_sends_no_conf() {
    let (airflow, fake) = fake_airflow::start().await;
    let app = run_route(airflow).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/pipelines/{}/smoke/runs",
                    crate::cluster::namespace()
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let fake = fake.lock().unwrap();
    assert_eq!(fake.trigger_bodies.len(), 1, "exactly one trigger");
    let body = &fake.trigger_bodies[0];
    assert!(
        body.get("conf").is_none(),
        "an empty body sends no conf: {body}"
    );
    assert_eq!(body["note"], "Started from Nova by sub-ada");
}

/// A date that is not `YYYY-MM-DD` is a 400 before Airflow is asked (nova#817).
#[tokio::test]
async fn pipelines_run_trigger_refuses_a_bad_run_date() {
    let (airflow, fake) = fake_airflow::start().await;
    let app = run_route(airflow).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/pipelines/{}/smoke/runs",
                    crate::cluster::namespace()
                ))
                .header("content-type", "application/json")
                .body(Body::from(json!({ "runDate": "13.11.2026" }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["error"], "runDate is a date (YYYY-MM-DD)");
    assert!(
        fake.lock().unwrap().trigger_bodies.is_empty(),
        "nothing was triggered"
    );
}

/// A saved parameter's rules apply to a run's parameters too: a name that is not
/// lower-case, and 17 of them, are a 400 (nova#817).
#[tokio::test]
async fn pipelines_run_trigger_refuses_more_than_16_params() {
    let (airflow, fake) = fake_airflow::start().await;
    let app = run_route(airflow).await;
    let uri = format!("/pipelines/{}/smoke/runs", crate::cluster::namespace());

    for (body, sentence) in [
        (
            json!({ "params": { "Days": "7" } }),
            "parameter names are lower-case letters, digits and underscores",
        ),
        (
            json!({ "params": (0..17).map(|n| (format!("p{n}"), "v")).collect::<BTreeMap<_, _>>() }),
            "at most 16 parameters",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&uri)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["error"], sentence);
    }
    assert!(
        fake.lock().unwrap().trigger_bodies.is_empty(),
        "nothing was triggered"
    );
}

#[tokio::test]
async fn run_now_refuses_a_paused_pipeline_and_does_not_trigger() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    set_dag_paused(&fake, "stardelt__smoke", true);

    let error = run_now(
        Some(&resources),
        Some(&airflow),
        &limiter(),
        &who("sub-ada", &["analytics"], false),
        &crate::cluster::namespace(),
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect_err("a paused pipeline does not start");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.message(), "the pipeline is paused: resume it first");
    let fake = fake.lock().unwrap();
    assert!(
        !fake.calls.iter().any(|call| call.starts_with("POST ")),
        "Airflow was not asked to trigger: {:?}",
        fake.calls
    );
    assert!(fake.trigger_bodies.is_empty());
}

#[tokio::test]
async fn run_now_refuses_when_a_run_is_active_under_forbid() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    fake.lock().unwrap().runs.insert(
        "stardelt__smoke".into(),
        vec![run("manual__2026-10-20T09:30:00Z", "running")],
    );

    let error = run_now(
        Some(&resources),
        Some(&airflow),
        &limiter(),
        &who("sub-ada", &["analytics"], false),
        &crate::cluster::namespace(),
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect_err("Forbid refuses a second run");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(
        error.message(),
        "a run is already active: wait for it or stop it"
    );
    let fake = fake.lock().unwrap();
    assert!(fake.trigger_bodies.is_empty(), "nothing was triggered");
    assert!(!fake.calls.iter().any(|call| call.starts_with("POST ")));
}

#[tokio::test]
async fn run_now_allows_a_second_run_under_allow() {
    let resources = memory(&[(
        "smoke",
        with_concurrency(resource("smoke", Some("analytics")), "Allow"),
    )]);
    let (airflow, fake) = fake_airflow::start().await;
    fake.lock().unwrap().runs.insert(
        "stardelt__smoke".into(),
        vec![run("manual__2026-10-20T09:30:00Z", "running")],
    );

    let run = run_now(
        Some(&resources),
        Some(&airflow),
        &limiter(),
        &who("sub-ada", &["analytics"], false),
        &crate::cluster::namespace(),
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect("Allow skips the activity check");
    assert_eq!(run.state, "queued");
    assert_eq!(fake.lock().unwrap().trigger_bodies.len(), 1);
}

/// Two starts inside one second are two runs: Airflow's `dag_run` is unique on
/// `(dag_id, logical_date)`, so Nova sends microseconds in the logical date
/// (stardelt/stardelt#1537).
#[tokio::test]
async fn two_starts_in_one_second_are_two_runs() {
    let resources = memory(&[(
        "smoke",
        with_concurrency(resource("smoke", Some("analytics")), "Allow"),
    )]);
    let (airflow, fake) = fake_airflow::start().await;
    let member = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();
    let limiter = limiter();

    let first = run_now(
        Some(&resources),
        Some(&airflow),
        &limiter,
        &member,
        &namespace,
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect("the first start");
    let second = run_now(
        Some(&resources),
        Some(&airflow),
        &limiter,
        &member,
        &namespace,
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect("the second start");

    assert_ne!(first.run_id, second.run_id, "two runs, two run ids");

    let fake = fake.lock().unwrap();
    assert_eq!(fake.trigger_bodies.len(), 2, "two triggers");
    let dates: Vec<&str> = fake
        .trigger_bodies
        .iter()
        .map(|body| {
            body["logical_date"]
                .as_str()
                .expect("the trigger carries a logical_date")
        })
        .collect();
    assert_ne!(dates[0], dates[1], "two starts, two logical dates");
    for date in dates {
        let fraction = date
            .split_once('.')
            .unwrap_or_else(|| panic!("a fractional part: {date}"))
            .1;
        let digits = fraction
            .strip_suffix('Z')
            .unwrap_or_else(|| panic!("microseconds before Z: {date}"));
        assert!(
            digits.len() == 6 && digits.chars().all(|c| c.is_ascii_digit()),
            "exactly six digits of microseconds: {date}"
        );
    }
}

/// The `Forbid` check and the trigger are one critical section: two requests that arrive
/// together do not both start a run (stardelt/stardelt#1537).
#[tokio::test]
async fn run_now_serialises_a_forbid_race() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    let member = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();
    let limiter = limiter();
    let start = RunStart::default();

    let (first, second) = tokio::join!(
        run_now(
            Some(&resources),
            Some(&airflow),
            &limiter,
            &member,
            &namespace,
            "smoke",
            &start,
        ),
        run_now(
            Some(&resources),
            Some(&airflow),
            &limiter,
            &member,
            &namespace,
            "smoke",
            &start,
        ),
    );

    let starts = usize::from(first.is_ok()) + usize::from(second.is_ok());
    assert_eq!(starts, 1, "exactly one start: {first:?} {second:?}");
    let refused = match (&first, &second) {
        (Ok(_), Err(error)) | (Err(error), Ok(_)) => error,
        _ => panic!("one Ok and one refusal: {first:?} {second:?}"),
    };
    assert_eq!(refused.status(), StatusCode::CONFLICT);
    assert_eq!(
        refused.message(),
        "a run is already active: wait for it or stop it"
    );
    assert_eq!(
        fake.lock().unwrap().trigger_bodies.len(),
        1,
        "only one start reached Airflow"
    );
}

/// One pipeline's start gate never holds up another pipeline's start.
#[tokio::test]
async fn run_now_does_not_wait_for_another_pipelines_gate() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, _fake) = fake_airflow::start().await;
    let member = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();
    let limiter = limiter();
    let other = run_start_gate(&namespace, "another-pipeline");
    let _held = other.lock().await;

    let started = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        run_now(
            Some(&resources),
            Some(&airflow),
            &limiter,
            &member,
            &namespace,
            "smoke",
            &RunStart::default(),
        ),
    )
    .await
    .expect("a held gate of another pipeline must not block this start");
    assert!(started.is_ok(), "{started:?}");
}

#[tokio::test]
async fn run_now_refuses_an_unloaded_pipeline() {
    let resources = memory(&[("brand-new", resource("brand-new", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;

    let error = run_now(
        Some(&resources),
        Some(&airflow),
        &limiter(),
        &who("sub-ada", &["analytics"], false),
        &crate::cluster::namespace(),
        "brand-new",
        &RunStart::default(),
    )
    .await
    .expect_err("no DAG yet");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(
        error.message(),
        "the scheduler has not loaded this pipeline yet: it loads about a minute after a deploy, so try again"
    );
    assert!(
        fake.lock().unwrap().trigger_bodies.is_empty(),
        "there is nothing to trigger"
    );
}

#[tokio::test]
async fn run_now_is_rate_limited_per_user() {
    let resources = memory(&[
        (
            "smoke",
            with_concurrency(resource("smoke", Some("analytics")), "Allow"),
        ),
        ("other", resource("other", Some("analytics"))),
    ]);
    let (airflow, fake) = fake_airflow::start().await;
    set_dag_paused(&fake, "stardelt__other", true);
    let namespace = crate::cluster::namespace();
    let limiter = limiter();
    let ada = who("sub-ada", &["analytics"], false);

    // Refusals before the limiter do not count: ten paused refusals still leave ten slots.
    for _ in 0..10 {
        let error = run_now(
            Some(&resources),
            Some(&airflow),
            &limiter,
            &ada,
            &namespace,
            "other",
            &RunStart::default(),
        )
        .await
        .expect_err("paused");
        assert_eq!(error.message(), "the pipeline is paused: resume it first");
    }
    for attempt in 0..10 {
        run_now(
            Some(&resources),
            Some(&airflow),
            &limiter,
            &ada,
            &namespace,
            "smoke",
            &RunStart::default(),
        )
        .await
        .unwrap_or_else(|error| panic!("start {attempt}: {}", error.message()));
    }
    let error = run_now(
        Some(&resources),
        Some(&airflow),
        &limiter,
        &ada,
        &namespace,
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect_err("the eleventh start in a minute");
    assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(error.message(), "too many runs started; wait a minute");

    // Another user has their own ten.
    let run = run_now(
        Some(&resources),
        Some(&airflow),
        &limiter,
        &who("sub-bob", &["analytics"], false),
        &namespace,
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect("another user is unaffected");
    assert_eq!(run.state, "queued");
}

#[tokio::test]
async fn pause_and_resume_are_idempotent_and_return_the_fresh_view() {
    let resources = memory(&[
        ("smoke", resource("smoke", Some("analytics"))),
        ("brand-new", resource("brand-new", Some("analytics"))),
    ]);
    let (airflow, fake) = fake_airflow::start().await;
    let member = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let paused = pause_pipeline(
        Some(&resources),
        Some(&airflow),
        &member,
        &namespace,
        "smoke",
    )
    .await
    .expect("pause");
    assert!(paused.paused);
    assert_eq!(paused.state, "paused");
    let again = pause_pipeline(
        Some(&resources),
        Some(&airflow),
        &member,
        &namespace,
        "smoke",
    )
    .await
    .expect("pausing a paused pipeline is fine");
    assert!(again.paused, "the fresh DAG says paused");

    let resumed = resume_pipeline(
        Some(&resources),
        Some(&airflow),
        &member,
        &namespace,
        "smoke",
    )
    .await
    .expect("resume");
    assert!(!resumed.paused);
    assert_eq!(
        resumed.state, "succeeded",
        "the fresh view carries the last run"
    );
    let again = resume_pipeline(
        Some(&resources),
        Some(&airflow),
        &member,
        &namespace,
        "smoke",
    )
    .await
    .expect("resuming a running pipeline is fine");
    assert!(!again.paused);

    let patches = fake
        .lock()
        .unwrap()
        .calls
        .iter()
        .filter(|call| call.starts_with("PATCH /api/v2/dags/stardelt__smoke"))
        .count();
    assert_eq!(patches, 4, "two pauses and two resumes reached Airflow");

    let error = pause_pipeline(
        Some(&resources),
        Some(&airflow),
        &member,
        &namespace,
        "brand-new",
    )
    .await
    .expect_err("no DAG yet");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(
        error.message(),
        "the scheduler has not loaded this pipeline yet: it loads about a minute after a deploy, so try again"
    );
}

#[tokio::test]
async fn stop_marks_only_active_runs_failed() {
    const ACTIVE_RUN: &str = "manual__2026-10-20T09:30:00Z";
    const FAILED_RUN: &str = "manual__2026-10-20T09:00:00Z";
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    fake.lock().unwrap().runs.insert(
        "stardelt__smoke".into(),
        vec![
            run(ACTIVE_RUN, "running"),
            run(FAILED_RUN, "failed"),
            run(SMOKE_RUN, "success"),
        ],
    );
    let namespace = crate::cluster::namespace();
    let member = who("sub-ada", &["analytics"], false);

    for finished in [SMOKE_RUN, FAILED_RUN] {
        let error = stop_run(
            Some(&resources),
            Some(&airflow),
            &member,
            &namespace,
            "smoke",
            finished,
        )
        .await
        .expect_err("a finished run cannot stop");
        assert_eq!(error.status(), StatusCode::CONFLICT, "{finished}");
        assert_eq!(error.message(), "the run is not active");
    }
    assert!(
        !fake
            .lock()
            .unwrap()
            .calls
            .iter()
            .any(|call| call.starts_with("PATCH ")),
        "a finished run is not patched"
    );

    let stopped = stop_run(
        Some(&resources),
        Some(&airflow),
        &member,
        &namespace,
        "smoke",
        ACTIVE_RUN,
    )
    .await
    .expect("an active run stops");
    assert_eq!(stopped.state, "failed");
    assert_eq!(stopped.run_id, ACTIVE_RUN);
    let fake = fake.lock().unwrap();
    let patches = fake
        .calls
        .iter()
        .filter(|call| call.starts_with("PATCH /api/v2/dags/stardelt__smoke/dagRuns/"))
        .count();
    assert_eq!(patches, 1, "exactly one PATCH: {:?}", fake.calls);
    assert_eq!(
        fake.runs["stardelt__smoke"]
            .iter()
            .find(|run| run["dag_run_id"] == ACTIVE_RUN)
            .expect("the run")["state"],
        "failed"
    );
}

#[tokio::test]
async fn retry_clears_only_a_failed_run() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    fake.lock().unwrap().runs.insert(
        "stardelt__smoke".into(),
        vec![run(SMOKE_RUN, "failed"), run(OTHER_RUN, "success")],
    );
    let namespace = crate::cluster::namespace();
    let member = who("sub-ada", &["analytics"], false);

    let retried = retry_run(
        Some(&resources),
        Some(&airflow),
        &member,
        &namespace,
        "smoke",
        SMOKE_RUN,
    )
    .await
    .expect("a failed run retries");
    assert_eq!(retried.run_id, SMOKE_RUN);
    assert_eq!(retried.state, "failed", "the same run, not a new one");

    let fake = fake.lock().unwrap();
    let clears: Vec<&str> = fake
        .calls
        .iter()
        .map(String::as_str)
        .filter(|call| call.contains("/clear"))
        .collect();
    assert_eq!(
        clears,
        vec![
            "POST /api/v2/dags/stardelt__smoke/dagRuns/manual__2026-10-20T07%3A23%3A50.608876%2B00%3A00/clear"
        ],
        "exactly one clear, on this run: {:?}",
        fake.calls
    );
    assert_eq!(fake.clear_bodies.len(), 1);
    assert_eq!(fake.clear_bodies[0]["only_failed"], json!(true));
    assert_eq!(fake.clear_bodies[0]["dry_run"], json!(false));
}

#[tokio::test]
async fn retry_refuses_a_running_run() {
    const ACTIVE_RUN: &str = "manual__2026-10-20T09:30:00Z";
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    fake.lock()
        .unwrap()
        .runs
        .insert("stardelt__smoke".into(), vec![run(ACTIVE_RUN, "running")]);

    let error = retry_run(
        Some(&resources),
        Some(&airflow),
        &who("sub-ada", &["analytics"], false),
        &crate::cluster::namespace(),
        "smoke",
        ACTIVE_RUN,
    )
    .await
    .expect_err("a running run cannot be retried");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.message(), "only a failed run can be retried");
    let fake = fake.lock().unwrap();
    assert!(
        !fake.calls.iter().any(|call| call.contains("/clear")),
        "nothing is cleared: {:?}",
        fake.calls
    );
}

#[tokio::test]
async fn retry_refuses_a_paused_pipeline() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    {
        let mut fake = fake.lock().unwrap();
        fake.runs
            .insert("stardelt__smoke".into(), vec![run(SMOKE_RUN, "failed")]);
        let dag = fake
            .dags
            .iter_mut()
            .find(|dag| dag["dag_id"] == "stardelt__smoke")
            .expect("the smoke DAG");
        dag["is_paused"] = json!(true);
    }

    let error = retry_run(
        Some(&resources),
        Some(&airflow),
        &who("sub-ada", &["analytics"], false),
        &crate::cluster::namespace(),
        "smoke",
        SMOKE_RUN,
    )
    .await
    .expect_err("a paused pipeline cannot retry");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert!(
        error.message().starts_with("the pipeline is paused"),
        "{}",
        error.message()
    );
    let fake = fake.lock().unwrap();
    assert!(
        !fake.calls.iter().any(|call| call.contains("/clear")),
        "nothing is cleared: {:?}",
        fake.calls
    );
}

#[tokio::test]
async fn retry_refuses_an_empty_clear() {
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let (airflow, fake) = fake_airflow::start().await;
    {
        let mut fake = fake.lock().unwrap();
        fake.runs
            .insert("stardelt__smoke".into(), vec![run(SMOKE_RUN, "failed")]);
        fake.clear_empty = true;
    }

    let error = retry_run(
        Some(&resources),
        Some(&airflow),
        &who("sub-ada", &["analytics"], false),
        &crate::cluster::namespace(),
        "smoke",
        SMOKE_RUN,
    )
    .await
    .expect_err("nothing selected is a refusal");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.message(), "this run has no failed tasks to retry");
}

#[tokio::test]
async fn stop_ignores_a_run_id_of_another_pipeline() {
    let resources = memory(&[
        ("smoke", resource("smoke", Some("analytics"))),
        ("other", resource("other", Some("analytics"))),
    ]);
    let (airflow, fake) = fake_airflow::start().await;

    let error = stop_run(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
        OTHER_RUN,
    )
    .await
    .expect_err("another pipeline's run id does not resolve under smoke");
    assert_eq!(error.status(), StatusCode::NOT_FOUND);
    assert_eq!(error.message(), "no such run");
    let fake = fake.lock().unwrap();
    assert!(
        !fake
            .calls
            .iter()
            .any(|call| call.starts_with("PATCH ") || call.contains("stardelt__other")),
        "no PATCH, and the other DAG was never addressed: {:?}",
        fake.calls
    );
}

#[tokio::test]
async fn every_action_is_audited_including_refusals() {
    let resources = memory(&[(
        "smoke",
        with_concurrency(resource("smoke", Some("analytics")), "Allow"),
    )]);
    let (airflow, fake) = fake_airflow::start().await;
    let namespace = crate::cluster::namespace();
    // A `sub` of this test's own: the sink is process-wide, and other tests' records must
    // not be mistaken for these.
    let actor = who("sub-audit-196", &["stardelt-admins"], true);
    let _ = audit::test_sink::take_for("sub-audit-196");

    let started = run_now(
        Some(&resources),
        Some(&airflow),
        &limiter(),
        &actor,
        &namespace,
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect("run");
    set_dag_paused(&fake, "stardelt__smoke", true);
    run_now(
        Some(&resources),
        Some(&airflow),
        &limiter(),
        &actor,
        &namespace,
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect_err("paused");
    pause_pipeline(
        Some(&resources),
        Some(&airflow),
        &actor,
        &namespace,
        "smoke",
    )
    .await
    .expect("pause");
    resume_pipeline(
        Some(&resources),
        Some(&airflow),
        &actor,
        &namespace,
        "smoke",
    )
    .await
    .expect("resume");
    stop_run(
        Some(&resources),
        Some(&airflow),
        &actor,
        &namespace,
        "smoke",
        SMOKE_RUN,
    )
    .await
    .expect_err("the seeded run already succeeded");
    stop_run(
        Some(&resources),
        Some(&airflow),
        &actor,
        &namespace,
        "smoke",
        &started.run_id,
    )
    .await
    .expect("stop the queued run");
    let down = AirflowClient::new(
        reqwest::Client::new(),
        AirflowConfig {
            url: "http://127.0.0.1:9".into(),
            user: "nova-api".into(),
            password: Secret::new("x"),
        },
    );
    run_now(
        Some(&resources),
        Some(&down),
        &limiter(),
        &actor,
        &namespace,
        "smoke",
        &RunStart::default(),
    )
    .await
    .expect_err("Airflow is down");

    let records: Vec<audit::test_sink::Record> = audit::test_sink::take_for("sub-audit-196");
    let outcomes: Vec<(&str, &str)> = records
        .iter()
        .map(|record| (record.action.as_str(), record.outcome.as_str()))
        .collect();
    assert_eq!(
        outcomes,
        [
            ("run", "ok"),
            ("run", "refused:paused"),
            ("pause", "ok"),
            ("resume", "ok"),
            ("stop", "refused:not-active"),
            ("stop", "ok"),
            ("run", "error"),
        ],
        "one record per action, refusals and errors included"
    );
    assert!(
        records
            .iter()
            .all(|record| record.pipeline == "smoke" && record.namespace == namespace),
        "the record names the pipeline: {records:?}"
    );
    assert_eq!(
        records[0].run.as_deref(),
        Some(started.run_id.as_str()),
        "a started run is named"
    );
    assert_eq!(
        records[1].run, None,
        "a refusal before the trigger has no run"
    );
    assert_eq!(records[4].run.as_deref(), Some(SMOKE_RUN));
    // No field of the record can hold the note or log text: there is none to hold it.
    let serialized = format!("{records:?}");
    assert!(
        !serialized.contains("Started from Nova"),
        "the fixed note text is not repeated in the audit: {serialized}"
    );
}

#[tokio::test]
async fn not_loaded_after_three_minutes() {
    assert_eq!(
        pipeline_state(false, false, None, Some(120.0)),
        "loading",
        "two minutes is still loading"
    );
    assert_eq!(
        pipeline_state(false, false, None, Some(240.0)),
        "not-loaded",
        "four minutes without a DAG is not loading"
    );
    assert_eq!(
        pipeline_state(false, false, None, None),
        "loading",
        "without a creationTimestamp there is nothing to compare"
    );
    assert_eq!(
        pipeline_state(false, true, None, Some(600.0)),
        "idle",
        "a loaded pipeline is never not-loaded"
    );

    let resources = memory(&[
        (
            "two-minutes",
            with_created_ago(resource("two-minutes", Some("analytics")), 120),
        ),
        (
            "four-minutes",
            with_created_ago(resource("four-minutes", Some("analytics")), 240),
        ),
    ]);
    let (airflow, _fake) = fake_airflow::start().await;
    let namespace = crate::cluster::namespace();

    let young = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &namespace,
        "two-minutes",
    )
    .await
    .expect("the detail is readable");
    assert_eq!(young.pipeline.state, "loading");
    assert!(young.pipeline.created_at.is_some(), "createdAt is exposed");

    let old = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &namespace,
        "four-minutes",
    )
    .await
    .expect("the detail is readable");
    assert!(!old.pipeline.loaded);
    assert_eq!(old.pipeline.state, "not-loaded");
    assert!(old.pipeline.created_at.is_some());
}

// -----------------------------------------------------------------------
// Writes: principals, create, update, delete (#210)
// -----------------------------------------------------------------------

const NOT_READY_MESSAGE: &str = "the client credentials are not written yet";

/// A `ServicePrincipal` as the operator writes it. `None` leaves `status` off, as a resource
/// the operator has not seen yet.
fn service_principal(name: &str, group: &str, ready: Option<bool>) -> Value {
    let mut resource = json!({
        "apiVersion": "stardelt.io/v1alpha1",
        "kind": "ServicePrincipal",
        "metadata": { "name": name, "namespace": crate::cluster::namespace() },
        "spec": { "ownerGroup": group },
    });
    if let Some(ready) = ready {
        resource["status"] = json!({
            "ready": ready,
            "clientId": name,
            "workloadIdentity": format!("kubernetes~{name}-uid"),
            "conditions": [{
                "type": "Ready",
                "status": if ready { "True" } else { "False" },
                "reason": if ready { "Ready" } else { "Pending" },
                "message": if ready { "the client exists" } else { NOT_READY_MESSAGE },
            }],
        });
    }
    resource
}

/// A namespace with the `pipeline-analytics` principal ready: what a create needs.
fn analytics_ready() -> PipelineResources {
    memory_with(
        &[],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        )],
    )
}

fn create_body(name: &str, group: &str, sql: &str) -> CreatePipelineRequest {
    CreatePipelineRequest {
        name: name.into(),
        owner_group: group.into(),
        description: None,
        sql: sql.into(),
        notebook: None,
        tasks: None,
        schedule: Some(ScheduleRequest {
            cron: "0 3 * * *".into(),
            timezone: "Europe/Berlin".into(),
        }),
        params: BTreeMap::new(),
        options: None,
        provenance: None,
        notify: None,
        viewers: None,
        publish: None,
    }
}

fn update_body(sql: &str) -> UpdatePipelineRequest {
    UpdatePipelineRequest {
        sql: sql.into(),
        description: None,
        notebook: None,
        tasks: None,
        schedule: None,
        params: BTreeMap::new(),
        options: None,
        resource_version: None,
        provenance: None,
        notify: None,
        viewers: None,
        publish: None,
    }
}

/// A task-list entry for the tests (`task` is taken by the Airflow run double above).
fn pipeline_task(name: &str, sql: &str, depends_on: &[&str]) -> PipelineTaskRequest {
    PipelineTaskRequest {
        name: name.into(),
        sql: sql.into(),
        notebook: None,
        dbt: None,
        quality: None,
        spark_pipeline: None,
        writes: Vec::new(),
        depends_on: depends_on.iter().map(|name| (*name).to_string()).collect(),
    }
}

/// A `dbt` task entry for the tests (`task` is taken by the Airflow run double above).
fn dbt_task(name: &str, dbt: DbtRequest, depends_on: &[&str]) -> PipelineTaskRequest {
    PipelineTaskRequest {
        name: name.into(),
        sql: String::new(),
        notebook: None,
        dbt: Some(dbt),
        quality: None,
        spark_pipeline: None,
        writes: Vec::new(),
        depends_on: depends_on.iter().map(|name| (*name).to_string()).collect(),
    }
}

/// A `quality` task entry for the tests (`#114`).
fn quality_task(
    name: &str,
    checks: Vec<QualityCheckRequest>,
    depends_on: &[&str],
) -> PipelineTaskRequest {
    PipelineTaskRequest {
        name: name.into(),
        sql: String::new(),
        notebook: None,
        dbt: None,
        quality: Some(QualityRequest { checks }),
        spark_pipeline: None,
        writes: Vec::new(),
        depends_on: depends_on.iter().map(|name| (*name).to_string()).collect(),
    }
}

/// One SQL assertion of a quality task.
fn check(name: &str, sql: &str, severity: Option<&str>) -> QualityCheckRequest {
    QualityCheckRequest {
        name: name.into(),
        sql: sql.into(),
        severity: severity.map(str::to_string),
    }
}

/// A `spark-pipeline` task entry for the tests (`#80`).
fn spark_pipeline_task(
    name: &str,
    project: &str,
    refresh: Option<&str>,
    full_refresh: Option<&[&str]>,
    depends_on: &[&str],
) -> PipelineTaskRequest {
    PipelineTaskRequest {
        name: name.into(),
        sql: String::new(),
        notebook: None,
        dbt: None,
        quality: None,
        spark_pipeline: Some(SparkPipelineRequest {
            project: project.into(),
            refresh: refresh.map(str::to_string),
            full_refresh: full_refresh
                .map(|names| names.iter().map(|name| (*name).to_string()).collect()),
            files: BTreeMap::new(),
        }),
        writes: vec!["gold.shop.t".into()],
        depends_on: depends_on.iter().map(|name| (*name).to_string()).collect(),
    }
}

/// The defaults `pipeline_resource` writes when a body sends no options.
fn default_options() -> PipelineOptionsView {
    PipelineOptions::default()
        .resolve()
        .expect("the defaults are in range")
}

fn create_limiter() -> CreateLimiter {
    CreateLimiter::default()
}

/// A pipeline Nova wrote, carrying the `resourceVersion` a read would see.
fn nova_pipeline(name: &str, group: &str) -> Value {
    let mut resource = pipeline_resource(
        name,
        group,
        &Statement::Sql("select 1".into()),
        Some(&("0 3 * * *".to_string(), "Europe/Berlin".to_string())),
        &BTreeMap::new(),
        &default_options(),
        "sub-ada",
        None,
        &[],
        None,
        None,
    );
    resource["metadata"]["resourceVersion"] = json!("7");
    resource
}

/// The sentence the ticket specifies, spelled out rather than built by the function under
/// test, so a typo in the code cannot hide behind the test.
fn missing_principal_sentence(group: &str) -> String {
    format!(
        "No principal for this group yet: an administrator creates one with \
             make sync-principal NAME=pipeline-{group} GROUP={group}"
    )
}

async fn stored_pipeline(resources: &PipelineResources, name: &str) -> Option<Value> {
    resources.get(name).await.expect("memory read")
}

// -----------------------------------------------------------------------
// The production notebook rule (#819 decision 3)
// -----------------------------------------------------------------------

#[test]
fn production_refuses_a_hand_scheduled_notebook() {
    assert_eq!(
        production_notebook_sentence("prod", true, true, false),
        Some(PRODUCTION_NOTEBOOK)
    );
    assert_eq!(production_notebook_sentence("prod", true, true, true), None);
    assert_eq!(
        production_notebook_sentence("prod", false, true, false),
        None
    );
    assert_eq!(
        production_notebook_sentence("prod", true, false, false),
        None
    );
    assert_eq!(production_notebook_sentence("dev", true, true, false), None);
    assert_eq!(
        production_notebook_sentence("test", true, true, false),
        None
    );
}

#[test]
fn a_notebook_task_list_is_a_notebook_body() {
    let report = NotebookRequest {
        name: "report.ipynb".into(),
        object_version: "v1".into(),
        params: BTreeMap::new(),
        connection: None,
    };
    let mixed = [
        pipeline_task("load", "insert into gold.report select 1", &[]),
        PipelineTaskRequest {
            name: "report".into(),
            sql: String::new(),
            notebook: Some(report.clone()),
            dbt: None,
            quality: None,
            spark_pipeline: None,
            writes: Vec::new(),
            depends_on: vec!["load".into()],
        },
    ];
    assert!(runs_a_notebook(None, Some(&mixed)));
    let sql_only = [pipeline_task("load", "select 1", &[])];
    assert!(!runs_a_notebook(None, Some(&sql_only)));
    assert!(runs_a_notebook(Some(&report), None));
}

#[test]
fn a_stored_notebook_task_list_runs_a_notebook() {
    assert!(resource_runs_a_notebook(&json!({
        "spec": { "tasks": [
            { "name": "a", "kind": "notebook" },
            { "name": "b", "kind": "sql" }
        ] }
    })));
    assert!(!resource_runs_a_notebook(&json!({
        "spec": { "tasks": [{ "name": "a", "kind": "sql" }] }
    })));
}

#[tokio::test]
async fn production_refuses_a_notebook_schedule_without_a_bundle() {
    let (store, _store_fake) = crate::secret_store::fake::start().await;
    let (airflow, _airflow_fake) = fake_airflow::start().await;
    let mut state = test_state(None, airflow, store);
    Arc::get_mut(&mut state).expect("a fresh state").stage_kind = "prod";
    let namespace = crate::cluster::namespace();
    let app = router().with_state(state).layer(Extension(principal(who(
        "sub-ada",
        &["analytics"],
        false,
    ))));

    let hand = json!({
        "name": "nightly",
        "ownerGroup": "analytics",
        "notebook": { "name": "report.ipynb", "objectVersion": "v1" },
        "schedule": { "cron": "0 3 * * *", "timezone": "UTC" }
    });
    let post = |body: &Value| {
        Request::builder()
            .method("POST")
            .uri("/pipelines")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };

    // Without the stamp: 400 with exactly the sentence.
    let response = app.clone().oneshot(post(&hand)).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["error"], PRODUCTION_NOTEBOOK);

    // The validate route answers the same sentence as a refusal: 200, `ok: false`.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/pipelines/validate")
                .header("content-type", "application/json")
                .body(Body::from(hand.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["ok"], false);
    assert_eq!(value["errors"][0]["message"], PRODUCTION_NOTEBOOK);

    // With the stamp the rule lets it through: this state has no cluster.
    let mut stamped = hand.clone();
    stamped["provenance"] = json!({ "bundle": "shop", "target": "prod", "revision": "abc1234" });
    let response = app.clone().oneshot(post(&stamped)).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["error"], "no cluster connection");

    // A dev stage does not know the rule.
    let (store, _store_fake) = crate::secret_store::fake::start().await;
    let (airflow, _airflow_fake) = fake_airflow::start().await;
    let dev = router()
        .with_state(test_state(None, airflow, store))
        .layer(Extension(principal(who("sub-ada", &["analytics"], false))));
    let response = dev.oneshot(post(&hand)).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["error"], "no cluster connection");

    // The update half: an unstamped stored notebook schedule is refused, a stamped one is
    // not. The stored resource is the stamp's second home.
    let mut unstamped = nova_pipeline("nightly", "analytics");
    unstamped["spec"]["tasks"] = json!([
        { "name": "run", "kind": "notebook",
          "config": { "notebook": "report.ipynb", "objectVersion": "v1", "params": "{}" } }
    ]);
    let mut stamped = unstamped.clone();
    stamped["metadata"]["labels"][BUNDLE_LABEL] = json!("shop");
    stamped["metadata"]["labels"][TARGET_LABEL] = json!("prod");
    let form = json!({
        "notebook": { "name": "report.ipynb", "objectVersion": "v1" },
        "schedule": { "cron": "0 3 * * *", "timezone": "UTC" }
    });
    for (stored, expected_sentence) in [(unstamped, true), (stamped, false)] {
        let kube = fake_kube::start_with(
            vec![stored],
            vec![service_principal(
                "pipeline-analytics",
                "analytics",
                Some(true),
            )],
        )
        .await;
        let (store, _store_fake) = crate::secret_store::fake::start().await;
        let (airflow, _airflow_fake) = fake_airflow::start().await;
        let mut state = test_state(Some(kube), airflow, store);
        Arc::get_mut(&mut state).expect("a fresh state").stage_kind = "prod";
        let app = router().with_state(state).layer(Extension(principal(who(
            "sub-ada",
            &["analytics"],
            false,
        ))));
        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/pipelines/{namespace}/nightly"))
                    .header("content-type", "application/json")
                    .body(Body::from(form.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        if expected_sentence {
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(value["error"], PRODUCTION_NOTEBOOK);
        } else {
            assert_ne!(value["error"], PRODUCTION_NOTEBOOK, "{value}");
        }
    }
}

#[test]
fn validate_cron_table() {
    for cron in [
        "0 3 * * *",
        "*/15 * * * *",
        "0 9 * * 1-5",
        "30 6 1 * *",
        "0 8 * * mon",
        "0 */2 * * *",
        "5,35 * * * *",
    ] {
        assert_eq!(validate_cron(cron), Ok(()), "{cron} must be accepted");
    }
    for (cron, sentence) in [
        ("* * * * *", CRON_TOO_OFTEN),
        ("*/2 * * * *", CRON_TOO_OFTEN),
        ("0 3 * *", CRON_FIELDS),
        ("@daily", CRON_FIELDS),
        ("0 3 * * * *", CRON_FIELDS),
        ("0 25 * * *", CRON_RANGE),
        ("61 * * * *", CRON_RANGE),
        ("a b c d e", CRON_RANGE),
        ("*/0 * * * *", CRON_RANGE),
        ("5-1 * * * *", CRON_RANGE),
        ("", CRON_FIELDS),
    ] {
        assert_eq!(
            validate_cron(cron).expect_err(cron),
            sentence,
            "{cron:?} must be refused with the field sentence"
        );
    }
    // No shortcuts and no seconds, named months and weekdays in range, `a/n` not in the
    // grammar, a step larger than the field is still one fire.
    for cron in [
        "@hourly",
        "0 3 * * * *",
        "0 3 * * mon-fri/0",
        "0 3 * * jan",
        "5/10 * * * *",
        "0 0 * * 8",
    ] {
        assert!(validate_cron(cron).is_err(), "{cron} must be refused");
    }
    assert_eq!(validate_cron("0 3 1 jan-mar sun"), Ok(()));
    assert_eq!(validate_cron("0 3 * * 0-7"), Ok(()));
}

#[tokio::test]
async fn create_writes_the_resource_with_owner_runas_labels_and_defaults() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);

    let view = create_pipeline(
        Some(&resources),
        None,
        None,
        &create_limiter(),
        &ada,
        create_body("daily-trips", "analytics", "select 1;"),
    )
    .await
    .expect("the pipeline is created");

    assert_eq!(view.name, "daily-trips");
    assert_eq!(view.owner_group.as_deref(), Some("analytics"));
    assert_eq!(view.run_as.as_deref(), Some("pipeline-analytics"));
    assert!(!view.loaded, "Airflow has not seen it yet");
    assert_eq!(view.state, "loading");
    assert_eq!(view.schedule.as_deref(), Some("0 3 * * * (Europe/Berlin)"));

    let stored = stored_pipeline(&resources, "daily-trips")
        .await
        .expect("the resource is written");
    assert_eq!(
        stored,
        json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Pipeline",
            "metadata": {
                "name": "daily-trips",
                "namespace": crate::cluster::namespace(),
                "labels": {
                    "app.kubernetes.io/managed-by": "stardelt-nova",
                    "stardelt.io/owner-group": "analytics"
                },
                "annotations": { "stardelt.io/created-by": "sub-ada" }
            },
            "spec": {
                "ownerGroup": "analytics",
                "runAs": "pipeline-analytics",
                "schedule": { "cron": "0 3 * * *", "timezone": "Europe/Berlin" },
                "tasks": [ { "name": "run", "kind": "sql", "sql": "select 1" } ],
                "concurrency": "Forbid",
                "retries": { "attempts": 1, "backoff": "5m" },
                // The default 120 minutes in the loader's `<n>m` grammar (#223).
                "timeout": "120m"
            }
        }),
        "the whole resource, exactly as the ticket specifies it"
    );
}

#[tokio::test]
async fn create_without_schedule_omits_it() {
    let resources = analytics_ready();
    let mut body = create_body("on-demand", "analytics", "select 1");
    body.schedule = None;

    let view = create_pipeline(
        Some(&resources),
        None,
        None,
        &create_limiter(),
        &who("sub-ada", &["analytics"], false),
        body,
    )
    .await
    .expect("an on-demand pipeline is created");

    assert_eq!(view.schedule, None);
    let stored = stored_pipeline(&resources, "on-demand")
        .await
        .expect("the resource is written");
    assert!(
        stored["spec"].get("schedule").is_none(),
        "an on-demand pipeline carries no schedule: {stored}"
    );
    assert_eq!(stored["spec"]["tasks"][0]["sql"], "select 1");
}

#[tokio::test]
async fn create_refuses_a_group_the_caller_is_not_in() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let error = create_pipeline(
        Some(&resources),
        None,
        None,
        &create_limiter(),
        &ada,
        create_body("smoke", "finance", "select 1"),
    )
    .await
    .expect_err("not a member of finance");
    assert_eq!(error.status(), StatusCode::FORBIDDEN);
    assert_eq!(error.message(), "you are not a member of the group finance");
    assert!(
        resources.list().await.unwrap().is_empty(),
        "nothing is written for a group the caller is not in"
    );

    // An administrator is no substitute: they schedule for the groups they belong to.
    let root = who("sub-root", &["stardelt-admins"], true);
    let error = create_pipeline(
        Some(&resources),
        None,
        None,
        &create_limiter(),
        &root,
        create_body("smoke", "analytics", "select 1"),
    )
    .await
    .expect_err("an administrator is not a member of analytics");
    assert_eq!(error.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        error.message(),
        "you are not a member of the group analytics"
    );
    assert!(resources.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn create_needs_a_ready_principal_of_the_same_group() {
    let ada = who("sub-ada", &["analytics"], false);
    let limiter = create_limiter();

    // No principal at all.
    let resources = memory(&[]);
    let error = create_pipeline(
        Some(&resources),
        None,
        None,
        &limiter,
        &ada,
        create_body("smoke", "analytics", "select 1"),
    )
    .await
    .expect_err("no principal");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.message(), missing_principal_sentence("analytics"));
    assert!(resources.list().await.unwrap().is_empty());

    // The principal exists but is not ready: the condition's message is the sentence.
    let resources = memory_with(
        &[],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(false)),
        )],
    );
    let error = create_pipeline(
        Some(&resources),
        None,
        None,
        &limiter,
        &ada,
        create_body("smoke", "analytics", "select 1"),
    )
    .await
    .expect_err("not ready");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.message(), NOT_READY_MESSAGE);
    assert!(resources.list().await.unwrap().is_empty());

    // The right name, but another group's principal: treated as absent.
    let resources = memory_with(
        &[],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "finance", Some(true)),
        )],
    );
    let error = create_pipeline(
        Some(&resources),
        None,
        None,
        &limiter,
        &ada,
        create_body("smoke", "analytics", "select 1"),
    )
    .await
    .expect_err("another group's principal");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.message(), missing_principal_sentence("analytics"));
    assert!(resources.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn create_validates_name_sql_timezone() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let limiter = create_limiter();
    let long_name = "a".repeat(41);

    for name in [
        "",
        "Analytics",
        "1analytics",
        "analytics-",
        long_name.as_str(),
    ] {
        let error = create_pipeline(
            Some(&resources),
            None,
            None,
            &limiter,
            &ada,
            create_body(name, "analytics", "select 1"),
        )
        .await
        .expect_err("invalid name");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{name:?}");
        assert_eq!(error.message(), invalid_name(), "{name:?}");
    }

    // A blank statement is neither a statement nor a notebook (#237).
    let error = create_pipeline(
        Some(&resources),
        None,
        None,
        &limiter,
        &ada,
        create_body("smoke", "analytics", "   "),
    )
    .await
    .expect_err("no statement");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.message(), ONE_OF_STATEMENT_NOTEBOOK_OR_DBT);

    for sql in [
        "x".repeat(MAX_STATEMENT_CHARS + 1),
        "select 1\0".to_string(),
        ";".to_string(),
    ] {
        let error = create_pipeline(
            Some(&resources),
            None,
            None,
            &limiter,
            &ada,
            create_body("smoke", "analytics", &sql),
        )
        .await
        .expect_err("invalid statement");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.message(), invalid_sql());
    }

    let mut body = create_body("smoke", "analytics", "select 1");
    body.schedule = Some(ScheduleRequest {
        cron: "0 3 * * *".into(),
        timezone: "Mars/Olympus".into(),
    });
    let error = create_pipeline(Some(&resources), None, None, &limiter, &ada, body)
        .await
        .expect_err("not an IANA zone");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.message(), "unknown timezone");

    for (cron, sentence) in [("* * * * *", CRON_TOO_OFTEN), ("0 3 * *", CRON_FIELDS)] {
        let mut body = create_body("smoke", "analytics", "select 1");
        body.schedule = Some(ScheduleRequest {
            cron: cron.into(),
            timezone: "UTC".into(),
        });
        let error = create_pipeline(Some(&resources), None, None, &limiter, &ada, body)
            .await
            .expect_err("invalid cron");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.message(), sentence);
    }
    assert!(
        resources.list().await.unwrap().is_empty(),
        "no refusal wrote anything"
    );

    // One trailing `;` is dropped from the stored statement; the rest is trimmed.
    create_pipeline(
        Some(&resources),
        None,
        None,
        &limiter,
        &ada,
        create_body("trailing", "analytics", "  select 42 ;  "),
    )
    .await
    .expect("a trailing semicolon is fine");
    let stored = stored_pipeline(&resources, "trailing")
        .await
        .expect("written");
    assert_eq!(stored["spec"]["tasks"][0]["sql"], "select 42");
}

#[tokio::test]
async fn create_twice_is_a_conflict() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let limiter = create_limiter();

    create_pipeline(
        Some(&resources),
        None,
        None,
        &limiter,
        &ada,
        create_body("smoke", "analytics", "select 1"),
    )
    .await
    .expect("the first create wins");

    let error = create_pipeline(
        Some(&resources),
        None,
        None,
        &limiter,
        &ada,
        create_body("smoke", "analytics", "select 2"),
    )
    .await
    .expect_err("the name is taken");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.message(), "a pipeline with this name already exists");
    let stored = stored_pipeline(&resources, "smoke")
        .await
        .expect("still there");
    assert_eq!(
        stored["spec"]["tasks"][0]["sql"], "select 1",
        "the first pipeline is never overwritten"
    );
}

#[tokio::test]
async fn create_is_rate_limited() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let limiter = create_limiter();

    for index in 0..CREATES_PER_HOUR {
        create_pipeline(
            Some(&resources),
            None,
            None,
            &limiter,
            &ada,
            create_body(&format!("daily-{index}"), "analytics", "select 1"),
        )
        .await
        .unwrap_or_else(|error| panic!("create {index}: {}", error.message()));
    }
    let error = create_pipeline(
        Some(&resources),
        None,
        None,
        &limiter,
        &ada,
        create_body("one-more", "analytics", "select 1"),
    )
    .await
    .expect_err("the eleventh create in an hour");
    assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        error.message(),
        "too many pipelines created this hour; wait before creating another"
    );
    assert!(
        stored_pipeline(&resources, "one-more").await.is_none(),
        "a rate-limited create writes nothing"
    );
    assert_eq!(resources.list().await.unwrap().len(), CREATES_PER_HOUR);

    // Another person has their own ten.
    create_pipeline(
        Some(&resources),
        None,
        None,
        &limiter,
        &who("sub-bob", &["analytics"], false),
        create_body("bobs-job", "analytics", "select 1"),
    )
    .await
    .expect("another user is unaffected");
}

#[tokio::test]
async fn update_changes_the_editable_fields_only() {
    let mut original = nova_pipeline("smoke", "analytics");
    original["spec"]["params"] = json!({ "days": "1" });
    original["spec"]["engine"] = json!("airflow");
    let resources = memory_with(
        &[("smoke", original.clone())],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        )],
    );
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    // The body carries the whole editable state: statement, schedule, params, options.
    let update = UpdatePipelineRequest {
        sql: "select 2;".into(),
        description: None,
        notebook: None,
        tasks: None,
        schedule: None,
        params: BTreeMap::from([
            ("days".to_string(), "7".to_string()),
            ("region".to_string(), "eu".to_string()),
        ]),
        options: Some(PipelineOptions {
            retries: Some(0),
            timeout_minutes: Some(30),
            ..PipelineOptions::default()
        }),
        resource_version: None,
        provenance: None,
        notify: None,
        viewers: None,
        publish: None,
    };
    let view = update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "smoke",
        update,
    )
    .await
    .expect("the update succeeds");
    assert_eq!(view.schedule, None);

    let stored = stored_pipeline(&resources, "smoke").await.expect("written");
    assert_eq!(stored["spec"]["tasks"][0]["sql"], "select 2");
    assert!(
        stored["spec"].get("schedule").is_none(),
        "an omitted schedule is removed: {stored}"
    );
    assert_eq!(
        stored["spec"]["params"],
        json!({ "days": "7", "region": "eu" }),
        "the parameter table is replaced, not merged"
    );
    assert_eq!(
        stored["spec"]["retries"],
        json!({ "attempts": 0, "backoff": "5m" })
    );
    assert_eq!(stored["spec"]["timeout"], json!("30m"));
    assert_eq!(stored["spec"]["concurrency"], json!("Forbid"));
    assert_eq!(
        stored["spec"]["engine"], "airflow",
        "every field the form does not edit stays"
    );
    assert_eq!(stored["spec"]["runAs"], "pipeline-analytics");
    assert_eq!(stored["metadata"]["resourceVersion"], "7");
    assert_eq!(
        stored["metadata"]["annotations"]["stardelt.io/created-by"],
        "sub-ada"
    );

    // A new schedule goes back in; the statement changes again.
    let view = update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "smoke",
        UpdatePipelineRequest {
            sql: "select 3".into(),
            description: None,
            notebook: None,
            tasks: None,
            schedule: Some(ScheduleRequest {
                cron: "30 6 * * mon-fri".into(),
                timezone: "UTC".into(),
            }),
            params: BTreeMap::new(),
            options: None,
            resource_version: None,
            provenance: None,
            notify: None,
            viewers: None,
            publish: None,
        },
    )
    .await
    .expect("the second update succeeds");
    assert_eq!(view.schedule.as_deref(), Some("30 6 * * mon-fri (UTC)"));
    let stored = stored_pipeline(&resources, "smoke").await.expect("written");
    assert_eq!(stored["spec"]["tasks"][0]["sql"], "select 3");
    assert_eq!(
        stored["spec"]["schedule"],
        json!({ "cron": "30 6 * * mon-fri", "timezone": "UTC" })
    );
    assert!(
        stored["spec"].get("params").is_none(),
        "an omitted table is removed: {stored}"
    );
    assert_eq!(stored["spec"]["engine"], "airflow");
}

#[tokio::test]
async fn update_refuses_a_pipeline_not_created_by_nova() {
    let mut legacy = nova_pipeline("legacy", "analytics");
    legacy["metadata"]["labels"]
        .as_object_mut()
        .unwrap()
        .remove(MANAGED_BY_LABEL);
    let resources = memory_with(
        &[("legacy", legacy.clone())],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        )],
    );
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    for caller in [&ada, &admin()] {
        let error = update_pipeline(
            Some(&resources),
            None,
            None,
            None,
            caller,
            &namespace,
            "legacy",
            update_body("select 2"),
        )
        .await
        .expect_err("kubectl's pipeline is not Nova's to rewrite");
        assert_eq!(error.status(), StatusCode::CONFLICT);
        assert_eq!(
            error.message(),
            "this pipeline is managed outside Nova (kubectl or Git); change it there"
        );
    }
    assert_eq!(
        stored_pipeline(&resources, "legacy")
            .await
            .expect("unchanged"),
        legacy
    );
}

#[tokio::test]
async fn update_refuses_a_shape_it_cannot_edit() {
    let mut two_tasks = nova_pipeline("two-tasks", "analytics");
    two_tasks["spec"]["tasks"] = json!([
        { "name": "run", "kind": "sql", "sql": "select 1" },
        { "name": "check", "kind": "sql", "sql": "select 2" }
    ]);
    let mut other_name = nova_pipeline("other-name", "analytics");
    other_name["spec"]["tasks"] = json!([
        { "name": "load", "kind": "sql", "sql": "select 1" }
    ]);
    let mut python_task = nova_pipeline("python-task", "analytics");
    python_task["spec"]["tasks"] = json!([
        { "name": "load", "kind": "python" }
    ]);
    let resources = memory_with(
        &[
            ("two-tasks", two_tasks),
            ("other-name", other_name),
            ("python-task", python_task),
        ],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        )],
    );
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    // A task list is a shape Nova writes, but a single-statement body cannot edit it; a
    // shape Nova does not write keeps the form's refusal.
    for (name, expected) in [
        ("two-tasks", SEVERAL_TASKS),
        ("other-name", SEVERAL_TASKS),
        (
            "python-task",
            "this pipeline has a shape the form cannot edit",
        ),
    ] {
        let error = update_pipeline(
            Some(&resources),
            None,
            None,
            None,
            &ada,
            &namespace,
            name,
            update_body("select 2"),
        )
        .await
        .expect_err("the form cannot edit this shape");
        assert_eq!(error.status(), StatusCode::CONFLICT, "{name}");
        assert_eq!(error.message(), expected, "{name}");
    }
    assert_eq!(
        stored_pipeline(&resources, "two-tasks").await.unwrap()["spec"]["tasks"]
            .as_array()
            .unwrap()
            .len(),
        2,
        "a refused update changes nothing"
    );
}

#[tokio::test]
async fn create_with_tasks_writes_the_whole_list() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("shop-silver", "analytics", "");
    body.tasks = Some(vec![
        pipeline_task(
            "load",
            "insert into silver.orders select * from bronze.orders",
            &[],
        ),
        pipeline_task("check", "select count(*) from silver.orders", &["load"]),
    ]);
    create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("a task list");
    let stored = stored_pipeline(&resources, "shop-silver")
        .await
        .expect("written");
    assert_eq!(
        stored["spec"]["tasks"],
        json!([
            { "name": "load", "kind": "sql",
              "sql": "insert into silver.orders select * from bronze.orders" },
            { "name": "check", "kind": "sql",
              "sql": "select count(*) from silver.orders", "dependsOn": ["load"] }
        ])
    );
}

#[tokio::test]
async fn create_with_tasks_accepts_a_notebook_task() {
    let resources = analytics_ready();
    let s3 = FakeS3::start().await;
    let version = s3.put_raw("analytics/report.ipynb", b"{\"nbformat\":4}", "ada");
    let store = notebook_store(&s3.base);
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("nightly-report", "analytics", "");
    body.tasks = Some(vec![
        pipeline_task("load", "insert into gold.report select 1", &[]),
        PipelineTaskRequest {
            name: "report".into(),
            sql: String::new(),
            notebook: Some(NotebookRequest {
                name: "report.ipynb".into(),
                object_version: version.clone(),
                params: BTreeMap::new(),
                connection: None,
            }),
            dbt: None,
            quality: None,
            spark_pipeline: None,
            writes: Vec::new(),
            depends_on: vec!["load".into()],
        },
    ]);
    create_pipeline(
        Some(&resources),
        None,
        Some(&store),
        &create_limiter(),
        &ada,
        body,
    )
    .await
    .expect("a mixed list");
    let stored = stored_pipeline(&resources, "nightly-report")
        .await
        .expect("written");
    assert_eq!(
        stored["spec"]["tasks"][1],
        json!({
            "name": "report",
            "kind": "notebook",
            "dependsOn": ["load"],
            "config": { "notebook": "report.ipynb", "objectVersion": version, "params": "{}" }
        })
    );
}

/// The connection is a task-level field (`tasks[].connection`), not a `config` one: the CRD
/// reads it there, and the loader turns it into `CONN_<NAME>_<KEY>` `secretKeyRef`s.
#[tokio::test]
async fn a_notebook_task_keeps_its_connection() {
    let resources = analytics_ready();
    let s3 = FakeS3::start().await;
    let version = s3.put_raw("analytics/report.ipynb", b"{\"nbformat\":4}", "ada");
    let store = notebook_store(&s3.base);
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("report-with-connection", "analytics", "");
    body.tasks = Some(vec![PipelineTaskRequest {
        name: "report".into(),
        sql: String::new(),
        notebook: Some(NotebookRequest {
            name: "report.ipynb".into(),
            object_version: version.clone(),
            params: BTreeMap::new(),
            connection: Some("crm-db".into()),
        }),
        dbt: None,
        quality: None,
        spark_pipeline: None,
        writes: Vec::new(),
        depends_on: Vec::new(),
    }]);
    create_pipeline(
        Some(&resources),
        None,
        Some(&store),
        &create_limiter(),
        &ada,
        body,
    )
    .await
    .expect("a notebook task naming a connection");
    let stored = stored_pipeline(&resources, "report-with-connection")
        .await
        .expect("written");
    let task = &stored["spec"]["tasks"][0];
    assert_eq!(task["connection"], "crm-db", "{stored}");
    assert!(
        task["config"].get("connection").is_none(),
        "the CRD reads the connection at task level, not in config: {stored}"
    );
}

/// Shape only: whether the connection exists, is Ready, is of the pipeline's group and is in
/// its `usableBy` is the loader's rule, and its sentence is the one the task log shows.
#[tokio::test]
async fn a_connection_name_outside_the_crd_rule_is_refused() {
    const SENTENCE: &str = "a connection name is 1 to 40 lower-case letters, digits and \
                                dashes, starting and ending with a letter or digit";
    let resources = analytics_ready();
    let s3 = FakeS3::start().await;
    let version = s3.put_raw("analytics/report.ipynb", b"{}", "ada");
    let store = notebook_store(&s3.base);
    let ada = who("sub-ada", &["analytics"], false);
    let limiter = create_limiter();
    for name in [
        "CRM_DB".to_string(),
        "crm_db".to_string(),
        "-crm".to_string(),
        "crm-".to_string(),
        String::new(),
        "a".repeat(41),
    ] {
        create_refused(
            &resources,
            &store,
            &limiter,
            &ada,
            NotebookRequest {
                name: "report.ipynb".into(),
                object_version: version.clone(),
                params: BTreeMap::new(),
                connection: Some(name.clone()),
            },
            SENTENCE,
        )
        .await;
    }
    // `crm-db` passes the shape rule (its existence is the loader's check).
    assert!(connection_ok("crm-db"));
    assert!(!connection_ok(""));
}

#[tokio::test]
async fn a_dbt_task_list_is_checked_and_stored() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("shop-marts", "analytics", "");
    body.tasks = Some(vec![dbt_task(
        "build",
        DbtRequest {
            project: "shop".into(),
            select: None,
            batches: Some(vec!["tag:load".into(), "tag:marts".into()]),
            threads: None,
            vars: None,
        },
        &[],
    )]);
    create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("a dbt task list");
    let stored = stored_pipeline(&resources, "shop-marts")
        .await
        .expect("written");
    assert_eq!(
        stored["spec"]["tasks"][0],
        json!({
            "name": "build",
            "kind": "dbt",
            "config": {
                "project": "shop",
                "batches": "[\"tag:load\",\"tag:marts\"]",
                "threads": "4"
            }
        })
    );
}

#[tokio::test]
async fn a_dbt_task_refuses_the_loader_s_sentences() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let refused = |dbt: DbtRequest| {
        let resources = &resources;
        let ada = &ada;
        async move {
            let mut body = create_body("shop-marts", "analytics", "");
            body.tasks = Some(vec![dbt_task("build", dbt, &[])]);
            let error = create_pipeline(Some(resources), None, None, &create_limiter(), ada, body)
                .await
                .expect_err("refused");
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            error.message().to_string()
        }
    };
    let dbt = |project: &str, select: Option<&str>, batches: Option<&[&str]>| DbtRequest {
        project: project.into(),
        select: select.map(str::to_string),
        batches: batches.map(|batches| batches.iter().map(|s| (*s).to_string()).collect()),
        threads: None,
        vars: None,
    };

    // A project is one directory name under /opt/dbt/projects.
    assert_eq!(
        refused(dbt("Shop", Some("tag:load"), None)).await,
        format!("task `build`: {DBT_NEEDS_PROJECT}")
    );
    // One selector or a chain of batches, never both and never neither.
    assert_eq!(
        refused(dbt("shop", Some("tag:load"), Some(&["tag:marts"]))).await,
        format!("task `build`: {DBT_NEEDS_SELECT}")
    );
    assert_eq!(
        refused(dbt("shop", None, None)).await,
        format!("task `build`: {DBT_NEEDS_SELECT}")
    );
    // 1 to 16 threads, omitted is 4.
    let mut no_threads = dbt("shop", Some("tag:load"), None);
    no_threads.threads = Some(0);
    assert_eq!(
        refused(no_threads).await,
        format!("task `build`: {DBT_BAD_THREADS}")
    );
    let mut too_many_threads = dbt("shop", Some("tag:load"), None);
    too_many_threads.threads = Some(17);
    assert_eq!(
        refused(too_many_threads).await,
        format!("task `build`: {DBT_BAD_THREADS}")
    );
    // `vars` is a JSON object string of string values, at most 4096 characters.
    let mut long_vars = dbt("shop", Some("tag:load"), None);
    long_vars.vars = Some(BTreeMap::from([("day".to_string(), "7".repeat(5000))]));
    assert_eq!(
        refused(long_vars).await,
        format!("task `build`: {DBT_BAD_VARS}")
    );
    // A `vars` value that is not a string never reaches those rules: the request's own type
    // refuses it in the extractor (`ApiJson`), before Nova sees a body at all.
    let error = serde_json::from_value::<CreatePipelineRequest>(json!({
        "name": "shop-marts",
        "ownerGroup": "analytics",
        "tasks": [{ "name": "build",
                    "dbt": { "project": "shop", "select": "tag:load", "vars": { "day": 7 } } }]
    }))
    .expect_err("a vars value that is not a string is not this type");
    assert!(
        error.to_string().contains("expected a string"),
        "got {error}"
    );
    // Nothing was written by any of the refusals.
    assert!(stored_pipeline(&resources, "shop-marts").await.is_none());
}

#[tokio::test]
async fn a_dbt_task_cannot_also_carry_a_statement() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("shop-marts", "analytics", "");
    body.tasks = Some(vec![PipelineTaskRequest {
        name: "build".into(),
        sql: "select 1".into(),
        notebook: None,
        dbt: Some(DbtRequest {
            project: "shop".into(),
            select: Some("tag:load".into()),
            batches: None,
            threads: None,
            vars: None,
        }),
        quality: None,
        spark_pipeline: None,
        writes: Vec::new(),
        depends_on: vec![],
    }]);
    let error = create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect_err("both");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message(),
        format!("task `build`: {ONE_OF_STATEMENT_NOTEBOOK_OR_DBT}")
    );
    assert!(stored_pipeline(&resources, "shop-marts").await.is_none());
}

#[test]
fn a_stored_dbt_task_list_is_a_task_list() {
    let resource = json!({
        "spec": {
            "tasks": [{ "name": "build", "kind": "dbt",
                        "config": { "project": "shop", "select": "tag:load", "threads": "4" } }]
        }
    });
    assert_eq!(stored_shape(&resource), StoredShape::Tasks);
}

#[tokio::test]
async fn create_with_tasks_accepts_a_quality_task() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("shop-quality", "analytics", "");
    body.tasks = Some(vec![quality_task(
        "gate",
        vec![check("no_null_ids", "SELECT 1 WHERE 1=0", Some("warn"))],
        &[],
    )]);
    create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("a quality task list");
    let stored = stored_pipeline(&resources, "shop-quality")
        .await
        .expect("written");
    assert_eq!(
        stored["spec"]["tasks"][0],
        json!({
            "name": "gate",
            "kind": "quality",
            "checks": [{
                "name": "no_null_ids",
                "sql": "SELECT 1 WHERE 1=0",
                "severity": "warn"
            }]
        })
    );
}

#[tokio::test]
async fn a_quality_task_refuses_the_loader_s_rules() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let refused = |checks: Vec<QualityCheckRequest>| {
        let resources = &resources;
        let ada = &ada;
        async move {
            let mut body = create_body("shop-quality", "analytics", "");
            body.tasks = Some(vec![quality_task("gate", checks, &[])]);
            let error = create_pipeline(Some(resources), None, None, &create_limiter(), ada, body)
                .await
                .expect_err("refused");
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            error.message().to_string()
        }
    };

    assert_eq!(
        refused(Vec::new()).await,
        format!("task `gate`: {QUALITY_NEEDS_CHECKS}")
    );
    assert_eq!(
        refused(
            (0..51)
                .map(|at| check(&format!("c{at}"), "select 1", None))
                .collect()
        )
        .await,
        format!("task `gate`: {QUALITY_NEEDS_CHECKS}")
    );
    // A name is the loader's check-name pattern.
    assert_eq!(
        refused(vec![check("-bad", "select 1", None)]).await,
        format!("task `gate`: {QUALITY_CHECK_NEEDS_NAME}")
    );
    assert_eq!(
        refused(vec![
            check("same", "select 1", None),
            check("same", "select 2", None)
        ])
        .await,
        format!("task `gate`: {QUALITY_CHECK_NAME_TWICE}")
    );
    assert_eq!(
        refused(vec![check("empty", "", None)]).await,
        format!("task `gate`: {QUALITY_CHECK_NEEDS_SQL}")
    );
    assert_eq!(
        refused(vec![check("long", &"x".repeat(21000), None)]).await,
        format!("task `gate`: {QUALITY_CHECK_NEEDS_SQL}")
    );
    assert_eq!(
        refused(vec![check("fatal", "select 1", Some("fatal"))]).await,
        format!("task `gate`: {QUALITY_CHECK_BAD_SEVERITY}")
    );
    assert_eq!(
        refused(vec![check("audit", "select * from ${table}", None)]).await,
        format!("task `gate`: {QUALITY_CHECK_AUDIT_TABLE}")
    );
    // Nothing was written by any of the refusals.
    assert!(stored_pipeline(&resources, "shop-quality").await.is_none());
}

#[tokio::test]
async fn a_quality_task_cannot_also_carry_a_statement() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("shop-quality", "analytics", "");
    body.tasks = Some(vec![PipelineTaskRequest {
        name: "gate".into(),
        sql: "select 1".into(),
        notebook: None,
        dbt: None,
        quality: Some(QualityRequest {
            checks: vec![check("no_null_ids", "select 1", None)],
        }),
        spark_pipeline: None,
        writes: Vec::new(),
        depends_on: vec![],
    }]);
    let error = create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect_err("both");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message(),
        format!("task `gate`: {ONE_OF_STATEMENT_NOTEBOOK_OR_DBT}")
    );
    assert!(stored_pipeline(&resources, "shop-quality").await.is_none());
}

#[test]
fn a_stored_quality_task_list_is_a_task_list() {
    let resource = json!({
        "spec": {
            "tasks": [{ "name": "gate", "kind": "quality",
                        "checks": [{ "name": "no_null_ids", "sql": "select 1",
                                     "severity": "warn" }] }]
        }
    });
    assert!(is_task_list(&resource));
    assert_eq!(stored_shape(&resource), StoredShape::Tasks);
}

#[tokio::test]
async fn spec_returns_a_task_list_with_a_quality_task() {
    let mut stored = nova_pipeline("shop-quality", "analytics");
    stored["spec"]["tasks"] = json!([{
        "name": "gate",
        "kind": "quality",
        "checks": [{ "name": "no_null_ids", "sql": "SELECT 1 WHERE 1=0",
                     "severity": "warn" }]
    }]);
    let resources = memory_with(&[("shop-quality", stored)], &analytics_principal());
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let answer = pipeline_spec(Some(&resources), &ada, &namespace, "shop-quality")
        .await
        .expect("visible to its group");

    // #762 merged after the ticket's snapshot: a Nova-written task list is edited whole, so
    // the document is now editable (its own test pins the flip).
    assert!(
        answer.editable,
        "a Nova-written task list is edited whole (#762)"
    );
    assert_eq!(
        serde_json::to_value(&answer.document).unwrap()["tasks"],
        json!([{
            "name": "gate",
            "quality": { "checks": [{ "name": "no_null_ids",
                                      "sql": "SELECT 1 WHERE 1=0",
                                      "severity": "warn" }] }
        }])
    );
}

#[tokio::test]
async fn validate_carries_a_quality_task() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("shop-quality", "analytics", "");
    body.tasks = Some(vec![quality_task(
        "gate",
        vec![check("no_null_ids", "SELECT 1 WHERE 1=0", Some("warn"))],
        &[],
    )]);
    let answer = validate_pipeline(Some(&resources), None, None, &ada, body)
        .await
        .expect("an answer");
    assert!(answer.ok, "{answer:?}");
    assert_eq!(answer.action.as_deref(), Some("create"));
    assert!(answer.errors.is_empty());
}

#[tokio::test]
async fn update_replaces_a_quality_task_list() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();
    let mut body = create_body("shop-quality", "analytics", "");
    body.tasks = Some(vec![quality_task(
        "gate",
        vec![check("no_null_ids", "select 1", None)],
        &[],
    )]);
    create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("created");

    let mut update = update_body("");
    update.tasks = Some(vec![
        pipeline_task("load", "select 2", &[]),
        quality_task(
            "gate",
            vec![check("no_null_ids", "select 3", Some("warn"))],
            &["load"],
        ),
    ]);
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "shop-quality",
        update,
    )
    .await
    .expect("updated");
    let stored = stored_pipeline(&resources, "shop-quality")
        .await
        .expect("stored");
    assert_eq!(stored["spec"]["tasks"].as_array().unwrap().len(), 2);
    assert_eq!(
        stored["spec"]["tasks"][1],
        json!({
            "name": "gate",
            "kind": "quality",
            "dependsOn": ["load"],
            "checks": [{ "name": "no_null_ids", "sql": "select 3",
                         "severity": "warn" }]
        })
    );
}

/// A `wap` create writes the branch decision and the tables its `spark-pipeline` task
/// publishes (#114).
#[tokio::test]
async fn a_create_with_publish_wap_writes_the_branch_and_the_tables() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut load = spark_pipeline_task("load", "graphs/shop", None, None, &[]);
    load.writes = vec!["gold.shop.t".into()];
    let mut body = create_body("shop-wap", "analytics", "");
    body.tasks = Some(vec![
        load,
        quality_task("check", vec![check("rows", "select 1", None)], &["load"]),
    ]);
    body.publish = Some(Publish::Wap);

    create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("a wap pipeline");
    let stored = stored_pipeline(&resources, "shop-wap")
        .await
        .expect("written");
    assert_eq!(stored["spec"]["publish"], json!("wap"));
    assert_eq!(stored["spec"]["tasks"][0]["writes"], json!(["gold.shop.t"]));
    assert!(
        stored["spec"]["tasks"][1].get("writes").is_none(),
        "a quality task writes no table: {}",
        stored["spec"]["tasks"][1]
    );
}

/// A `publish: wap` pipeline may use `${table}` in a check: it is the audit branch the
/// loader fills in for a wap run (#858, #881).
#[tokio::test]
async fn a_wap_create_accepts_a_table_placeholder_check() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut load = spark_pipeline_task("load", "graphs/shop", None, None, &[]);
    load.writes = vec!["gold.shop.t".into()];
    let mut body = create_body("shop-wap", "analytics", "");
    body.tasks = Some(vec![
        load,
        quality_task(
            "check",
            vec![check("rows", "select count(*) from ${table}", None)],
            &["load"],
        ),
    ]);
    body.publish = Some(Publish::Wap);

    create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("a wap pipeline with an audit-branch check");
    let stored = stored_pipeline(&resources, "shop-wap")
        .await
        .expect("written");
    let sql = stored["spec"]["tasks"][1]["checks"][0]["sql"]
        .as_str()
        .expect("the check's sql");
    assert!(sql.contains("${table}"), "stored sql: {sql}");
}

/// A `${table}` check names ONE table's audit branch; a wap pipeline that writes two
/// tables is refused instead of failing in the check's pod (#114).
#[tokio::test]
async fn a_wap_create_refuses_a_table_placeholder_with_two_written_tables() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut first = spark_pipeline_task("first", "graphs/a", None, None, &[]);
    first.writes = vec!["gold.shop.a".into()];
    let mut second = spark_pipeline_task("second", "graphs/b", None, None, &[]);
    second.writes = vec!["gold.shop.b".into()];
    let mut body = create_body("shop-wap", "analytics", "");
    body.tasks = Some(vec![
        first,
        second,
        quality_task(
            "check",
            vec![check("rows", "select count(*) from ${table}", None)],
            &["first", "second"],
        ),
    ]);
    body.publish = Some(Publish::Wap);

    let error = create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect_err("one check cannot read two audit branches");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message(),
        "${table} names the audit branch of one table, and this pipeline writes 2: \
             name the table in the check, or split the pipeline"
    );
    assert!(stored_pipeline(&resources, "shop-wap").await.is_none());
}

/// A `publish: direct` pipeline refuses `${table}` in a check: only a wap run has an audit
/// branch (#881).
#[tokio::test]
async fn a_direct_create_refuses_a_table_placeholder_check() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut load = spark_pipeline_task("load", "graphs/shop", None, None, &[]);
    load.writes = vec!["gold.shop.t".into()];
    let mut body = create_body("shop-wap", "analytics", "");
    body.tasks = Some(vec![
        load,
        quality_task(
            "check",
            vec![check("rows", "select count(*) from ${table}", None)],
            &["load"],
        ),
    ]);
    body.publish = Some(Publish::Direct);

    let error = create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect_err("a direct pipeline has no audit branch");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message(),
        format!("task `check`: {QUALITY_CHECK_AUDIT_TABLE}")
    );
    assert!(stored_pipeline(&resources, "shop-wap").await.is_none());
}

/// `publish: wap` without a quality task that checks and a `spark-pipeline` or `notebook`
/// task that writes is refused with the ticket's sentence, and nothing is written (#114).
#[tokio::test]
async fn a_wap_create_without_a_quality_check_and_a_written_table_is_refused() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut load = spark_pipeline_task("load", "graphs/shop", None, None, &[]);
    load.writes = vec!["gold.shop.t".into()];
    let mut body = create_body("shop-wap", "analytics", "");
    body.tasks = Some(vec![load]);
    body.publish = Some(Publish::Wap);

    let error = create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect_err("no quality task");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message(),
        "publish: wap needs a task list with a quality task that has a check and a spark-pipeline or notebook task that writes a table"
    );
    assert!(stored_pipeline(&resources, "shop-wap").await.is_none());
}

/// `writes` belongs to a `spark-pipeline` or `notebook` task; a `sql` task that names one is
/// refused (#114).
#[tokio::test]
async fn a_spark_or_notebook_task_declares_writes() {
    let resources = analytics_ready();
    let s3 = FakeS3::start().await;
    let version = s3.put_raw("analytics/smoke.ipynb", b"{\"nbformat\":4}", "ada");
    let store = notebook_store(&s3.base);
    let ada = who("sub-ada", &["analytics"], false);
    let mut task = pipeline_task("extract", "", &[]);
    task.notebook = Some(NotebookRequest {
        name: "smoke.ipynb".into(),
        object_version: version,
        params: BTreeMap::new(),
        connection: None,
    });
    task.writes = vec!["gold.shop.t".into()];
    let mut body = create_body("shop-writes", "analytics", "");
    body.tasks = Some(vec![task]);

    create_pipeline(
        Some(&resources),
        None,
        Some(&store),
        &create_limiter(),
        &ada,
        body,
    )
    .await
    .expect("a notebook task declares writes");
    let stored = stored_pipeline(&resources, "shop-writes")
        .await
        .expect("written");
    assert_eq!(stored["spec"]["tasks"][0]["writes"], json!(["gold.shop.t"]));

    let mut load = pipeline_task("load", "select 1", &[]);
    load.writes = vec!["gold.shop.t".into()];
    let mut body = create_body("shop-sql-writes", "analytics", "");
    body.tasks = Some(vec![load]);

    let error = create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect_err("a sql task cannot write a table");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message(),
        "task `load` cannot write a table: only a spark-pipeline or notebook task declares writes"
    );
    assert!(
        stored_pipeline(&resources, "shop-sql-writes")
            .await
            .is_none()
    );
}

/// `Some(Direct)` removes `spec.publish`, `None` keeps what the resource says (#114).
#[test]
fn an_update_with_publish_direct_removes_it_and_none_keeps_it() {
    let stored = pipeline_resource(
        "shop-wap",
        "analytics",
        &Statement::Sql("select 1".into()),
        None,
        &BTreeMap::new(),
        &default_options(),
        "sub-ada",
        None,
        &[],
        None,
        Some(Publish::Wap),
    );
    assert_eq!(spec_of(&stored)["publish"], json!("wap"));
    let statement = Statement::Sql("select 2".into());
    let update = |publish: Option<Publish>| {
        updated_resource(
            &stored,
            &statement,
            &default_options(),
            &BTreeMap::new(),
            None,
            None,
            None,
            None,
            publish,
        )
    };

    assert!(
        spec_of(&update(Some(Publish::Direct)))
            .get("publish")
            .is_none(),
        "direct removes the key"
    );
    assert_eq!(
        spec_of(&update(None))["publish"],
        json!("wap"),
        "an omitted publish keeps the stored one"
    );
    assert_eq!(
        spec_of(&update(Some(Publish::Wap)))["publish"],
        json!("wap")
    );
}

/// The document reads `spec.publish` back, so an editor can send it as it was read (#114).
#[test]
fn spec_response_round_trips_publish() {
    let resource = pipeline_resource(
        "shop-wap",
        "analytics",
        &Statement::Sql("select 1".into()),
        None,
        &BTreeMap::new(),
        &default_options(),
        "sub-ada",
        None,
        &[],
        None,
        Some(Publish::Wap),
    );
    let document = spec_response(&resource).document;
    assert_eq!(document.publish, Some(Publish::Wap));
    assert_eq!(
        serde_json::to_value(&document).expect("serialises")["publish"],
        json!("wap")
    );

    let direct = nova_pipeline("shop-direct", "analytics");
    let document = spec_response(&direct).document;
    assert_eq!(document.publish, None);
    assert!(
        serde_json::to_value(&document)
            .expect("serialises")
            .get("publish")
            .is_none(),
        "a direct pipeline carries no key"
    );
}

#[tokio::test]
async fn create_with_tasks_accepts_a_spark_pipeline_task() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("shop-graph", "analytics", "");
    body.tasks = Some(vec![spark_pipeline_task(
        "build",
        "spark/shop",
        Some("incremental"),
        Some(&["gold.report"]),
        &[],
    )]);
    create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("a spark-pipeline task list");
    let stored = stored_pipeline(&resources, "shop-graph")
        .await
        .expect("written");
    assert_eq!(
        stored["spec"]["tasks"][0],
        json!({
            "name": "build",
            "kind": "spark-pipeline",
            "project": "spark/shop",
            "refresh": "incremental",
            "fullRefresh": ["gold.report"],
            "writes": ["gold.shop.t"],
        })
    );
}

#[tokio::test]
async fn a_spark_pipeline_task_refuses_the_crd_rules() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let refused = |project: &'static str,
                   refresh: Option<&'static str>,
                   full_refresh: Option<&'static [&'static str]>| {
        let resources = &resources;
        let ada = &ada;
        async move {
            let mut body = create_body("shop-graph", "analytics", "");
            body.tasks = Some(vec![spark_pipeline_task(
                "build",
                project,
                refresh,
                full_refresh,
                &[],
            )]);
            let error = create_pipeline(Some(resources), None, None, &create_limiter(), ada, body)
                .await
                .expect_err("refused");
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            error.message().to_string()
        }
    };

    assert_eq!(
        refused("", None, None).await,
        format!("task `build`: {SPARK_NEEDS_PROJECT}")
    );
    // The CRD's project is a relative directory of the bundle.
    assert_eq!(
        refused("/spark/shop", None, None).await,
        format!("task `build`: {SPARK_PROJECT_RULE}")
    );
    assert_eq!(
        refused("spark/../shop", None, None).await,
        format!("task `build`: {SPARK_PROJECT_RULE}")
    );
    assert_eq!(
        refused("spark/shop", Some("sometimes"), None).await,
        format!("task `build`: {SPARK_BAD_REFRESH}")
    );
    assert_eq!(
        refused("spark/shop", Some("full"), Some(&["gold.report"])).await,
        format!("task `build`: {SPARK_REFRESH_TWICE}")
    );
    // Nothing was written by any of the refusals.
    assert!(stored_pipeline(&resources, "shop-graph").await.is_none());
}

/// The graph's document as the loader reads it: writes, one target, and the file (#80).
#[tokio::test]
async fn a_spark_pipeline_task_needs_writes_and_one_target_and_a_checked_graph() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let graph = |text: &str, writes: &[&str]| {
        let mut task = spark_pipeline_task("build", "spark/shop", None, None, &[]);
        task.writes = writes.iter().map(|table| (*table).to_string()).collect();
        task.spark_pipeline
            .as_mut()
            .expect("a graph task")
            .files
            .insert("spark-pipeline.yml".into(), text.into());
        task
    };
    let refused = |task: PipelineTaskRequest| {
        let resources = &resources;
        let ada = &ada;
        async move {
            let mut body = create_body("shop-graph", "analytics", "");
            body.tasks = Some(vec![task]);
            let error = create_pipeline(Some(resources), None, None, &create_limiter(), ada, body)
                .await
                .expect_err("refused");
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            error.message().to_string()
        }
    };
    let ok = "name: shop\nconfiguration:\n  spark.sql.catalog.gold.type: rest\n";

    assert_eq!(
        refused(graph(ok, &[])).await,
        "spark-pipeline task `build` needs writes: the catalog and schema the graph writes, \
             as catalog.schema.table — stardelt sets the graph's catalog and database from it"
    );
    assert_eq!(
        refused(graph(ok, &["gold.a.t1", "silver.a.t2"])).await,
        "spark-pipeline task `build` writes into more than one catalog/schema (gold.a, \
             silver.a): a graph has one target; split the task"
    );
    assert_eq!(
        refused(graph("name: shop\ncatalog: gold\n", &["gold.shop.t"])).await,
        "task `build`: spark-pipeline.yml must not set catalog, database or storage — the \
             task's writes and the group's bucket set them at run time (#80)"
    );
    assert_eq!(
        refused(graph(
            "name: shop\nlibraries:\n  - glob:\n      include: '*.sql'\n\
                 configuration:\n  spark.sql.catalog.gold.type: rest\n",
            &["gold.shop.t"],
        ))
        .await,
        "task `build`: spark-pipeline.yml libraries glob.include must be a file path or a \
             folder ending in /**: *.sql"
    );
    assert_eq!(
        refused(graph("name: shop\n", &["gold.shop.t"])).await,
        "task `build`: catalog `gold` has no `spark.sql.catalog.gold.type` under \
             `configuration:` — the Connect session would not know the catalog"
    );
    // The accepted twin, so a rule that refuses everything fails here.
    let mut body = create_body("shop-graph", "analytics", "");
    body.tasks = Some(vec![graph(
        "name: shop\nlibraries:\n  - glob:\n      include: sql/**\n\
             configuration:\n  spark.sql.catalog.gold.type: rest\n",
        &["gold.shop.t"],
    )]);
    create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("a graph the loader accepts");
}

#[tokio::test]
async fn a_spark_pipeline_task_cannot_also_carry_a_statement() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("shop-graph", "analytics", "");
    let mut task = spark_pipeline_task("build", "spark/shop", None, None, &[]);
    task.sql = "select 1".into();
    body.tasks = Some(vec![task]);
    let error = create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect_err("both");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message(),
        format!("task `build`: {ONE_OF_STATEMENT_NOTEBOOK_OR_DBT}")
    );
    assert!(stored_pipeline(&resources, "shop-graph").await.is_none());
}

#[test]
fn a_stored_spark_pipeline_task_list_is_a_task_list() {
    let resource = json!({
        "spec": {
            "tasks": [{ "name": "build", "kind": "spark-pipeline",
                        "project": "spark/shop", "refresh": "incremental",
                        "fullRefresh": ["gold.report"] }]
        }
    });
    assert!(is_task_list(&resource));
    assert_eq!(stored_shape(&resource), StoredShape::Tasks);
}

#[tokio::test]
async fn spec_returns_a_task_list_with_a_spark_pipeline_task() {
    let mut stored = nova_pipeline("shop-graph", "analytics");
    stored["spec"]["tasks"] = json!([{
        "name": "build",
        "kind": "spark-pipeline",
        "project": "spark/shop",
        "refresh": "incremental",
        "fullRefresh": ["gold.report"],
        "files": { "spark-pipeline.yml": "name: shop\n", "sql/trips.sql": "select 1\n" }
    }]);
    let resources = memory_with(&[("shop-graph", stored)], &analytics_principal());
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let answer = pipeline_spec(Some(&resources), &ada, &namespace, "shop-graph")
        .await
        .expect("visible to its group");

    assert!(
        answer.editable,
        "a Nova-written task list is edited whole (#762)"
    );
    assert_eq!(
        serde_json::to_value(&answer.document).unwrap()["tasks"],
        json!([{
            "name": "build",
            "sparkPipeline": {
                "project": "spark/shop",
                "refresh": "incremental",
                "fullRefresh": ["gold.report"],
                "files": {
                    "spark-pipeline.yml": "name: shop\n",
                    "sql/trips.sql": "select 1\n"
                }
            }
        }])
    );
}

#[tokio::test]
async fn update_replaces_a_spark_pipeline_task_list() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();
    let mut body = create_body("shop-graph", "analytics", "");
    body.tasks = Some(vec![spark_pipeline_task(
        "build",
        "spark/shop",
        None,
        None,
        &[],
    )]);
    create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("created");

    let mut update = update_body("");
    update.tasks = Some(vec![
        pipeline_task("load", "select 2", &[]),
        spark_pipeline_task("build", "spark/shop", Some("full"), None, &["load"]),
    ]);
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "shop-graph",
        update,
    )
    .await
    .expect("updated");
    let stored = stored_pipeline(&resources, "shop-graph")
        .await
        .expect("stored");
    assert_eq!(stored["spec"]["tasks"].as_array().unwrap().len(), 2);
    assert_eq!(
        stored["spec"]["tasks"][1],
        json!({
            "name": "build",
            "kind": "spark-pipeline",
            "project": "spark/shop",
            "refresh": "full",
            "dependsOn": ["load"],
            "writes": ["gold.shop.t"],
        })
    );
}

/// A task list's `sparkPipeline` carries the graph's files onto the resource (#80 S2b).
#[tokio::test]
async fn a_spark_pipeline_task_stores_its_graph_files() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("shop-graph", "analytics", "");
    let mut build = spark_pipeline_task("build", "spark/shop", None, None, &[]);
    let graph = build.spark_pipeline.as_mut().expect("a graph task");
    graph.files.insert(
        "spark-pipeline.yml".into(),
        "name: shop\nconfiguration:\n  spark.sql.catalog.gold.type: rest\n".into(),
    );
    graph
        .files
        .insert("sql/trips.sql".into(), "select 1\n".into());
    body.tasks = Some(vec![build]);
    create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("a spark-pipeline task with its graph");
    let stored = stored_pipeline(&resources, "shop-graph")
        .await
        .expect("stored");
    assert_eq!(
        stored["spec"]["tasks"][0]["files"],
        json!({ "spark-pipeline.yml":
                "name: shop\nconfiguration:\n  spark.sql.catalog.gold.type: rest\n",
                "sql/trips.sql": "select 1\n" })
    );
    assert_eq!(stored["spec"]["tasks"][0]["project"], json!("spark/shop"));
}

/// A graph's files without `spark-pipeline.yml` at the root are the CRD's refusal, in Nova's
/// words (#80 S2b).
#[tokio::test]
async fn a_spark_pipeline_graph_must_hold_spark_pipeline_yml() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("shop-graph", "analytics", "");
    let mut build = spark_pipeline_task("build", "spark/shop", None, None, &[]);
    build
        .spark_pipeline
        .as_mut()
        .expect("a graph task")
        .files
        .insert("sql/trips.sql".into(), "select 1\n".into());
    body.tasks = Some(vec![build]);
    let error = create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect_err("no spark-pipeline.yml");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message(),
        "task `build`: a spark-pipeline task's files must hold spark-pipeline.yml at the project root"
    );
    assert!(stored_pipeline(&resources, "shop-graph").await.is_none());
}

/// The graph's own bounds (#80 S2b): at most 64 files, paths relative to the project directory,
/// and 64 KiB per file and 256 KiB for the whole graph.
#[test]
fn a_spark_pipeline_graph_is_bounded() {
    let graph = |files: BTreeMap<String, String>| SparkPipelineRequest {
        project: "spark/shop".into(),
        refresh: None,
        full_refresh: None,
        files,
    };
    let with = |extra: &[(String, String)]| {
        let mut files = BTreeMap::from([("spark-pipeline.yml".to_string(), "name: shop\n".into())]);
        for (path, text) in extra {
            files.insert(path.clone(), text.clone());
        }
        files
    };

    let many: Vec<(String, String)> = (0..64)
        .map(|at| (format!("sql/{at}.sql"), "select 1\n".to_string()))
        .collect();
    assert_eq!(
        spark_pipeline_config(&graph(with(&many))).unwrap_err(),
        "a spark-pipeline graph is at most 64 files"
    );
    for path in ["../outside.sql", "/abs.sql", "sql/a b.sql"] {
        assert_eq!(
            spark_pipeline_config(&graph(with(&[(
                path.to_string(),
                "select 1\n".to_string()
            )])))
            .unwrap_err(),
            "a graph file path is relative to the project directory",
            "{path}"
        );
    }
    assert_eq!(
        spark_pipeline_config(&graph(with(&[(
            "sql/big.sql".to_string(),
            "x".repeat(65537)
        )])))
        .unwrap_err(),
        "a spark-pipeline graph is at most 256 KiB"
    );
    let large: Vec<(String, String)> = (0..5)
        .map(|at| (format!("sql/{at}.sql"), "x".repeat(60000)))
        .collect();
    assert_eq!(
        spark_pipeline_config(&graph(with(&large))).unwrap_err(),
        "a spark-pipeline graph is at most 256 KiB"
    );
}

#[test]
fn a_dbt_task_reaches_the_detail_view() {
    let spec = json!({
        "tasks": [{ "name": "build", "kind": "dbt",
                    "config": { "project": "shop", "batches": "[\"tag:load\",\"tag:marts\"]",
                                "threads": "4" } }]
    });
    let detailed = task_specs(&spec, true);
    assert_eq!(detailed[0].dbt_project.as_deref(), Some("shop"));
    assert_eq!(
        detailed[0].dbt_select.as_deref(),
        Some("tag:load, tag:marts")
    );
    let listed = task_specs(&spec, false);
    assert_eq!(listed[0].dbt_project, None, "the list stays small");
    assert_eq!(listed[0].dbt_select, None, "the list stays small");
}

#[tokio::test]
async fn task_list_refusals_name_the_task() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let refused = |body: CreatePipelineRequest| {
        let resources = &resources;
        let ada = &ada;
        async move {
            create_pipeline(Some(resources), None, None, &create_limiter(), ada, body)
                .await
                .expect_err("refused")
                .message()
                .to_string()
        }
    };
    let mut cases: Vec<(CreatePipelineRequest, String)> = Vec::new();

    let mut both = create_body("both", "analytics", "select 1");
    both.tasks = Some(vec![pipeline_task("load", "select 1", &[])]);
    cases.push((both, ONE_OF_STATEMENT_NOTEBOOK_OR_TASKS.into()));

    let mut empty = create_body("empty", "analytics", "");
    empty.tasks = Some(Vec::new());
    cases.push((empty, NO_TASKS.into()));

    let mut many = create_body("many", "analytics", "");
    many.tasks = Some(
        (0..31)
            .map(|at| pipeline_task(&format!("t{at}"), "select 1", &[]))
            .collect(),
    );
    cases.push((many, TOO_MANY_TASKS.into()));

    let mut capital = create_body("capital", "analytics", "");
    capital.tasks = Some(vec![pipeline_task("Load", "select 1", &[])]);
    cases.push((
        capital,
        format!("task name `Load` is not valid: {TASK_NAME_RULE}"),
    ));

    let mut twice = create_body("twice", "analytics", "");
    twice.tasks = Some(vec![
        pipeline_task("load", "select 1", &[]),
        pipeline_task("load", "select 2", &[]),
    ]);
    cases.push((twice, "task name `load` is used twice".into()));

    let mut itself = create_body("itself", "analytics", "");
    itself.tasks = Some(vec![pipeline_task("load", "select 1", &["load"])]);
    cases.push((itself, "task `load` cannot depend on itself".into()));

    let mut unknown = create_body("unknown", "analytics", "");
    unknown.tasks = Some(vec![pipeline_task("load", "select 1", &["nope"])]);
    cases.push((
        unknown,
        "task `load` depends on `nope`, which is not a task of this pipeline".into(),
    ));

    let mut cycle = create_body("cycle", "analytics", "");
    cycle.tasks = Some(vec![
        pipeline_task("a", "select 1", &["b"]),
        pipeline_task("b", "select 2", &["a"]),
    ]);
    assert_eq!(
        refused(cycle).await,
        "task `a` is part of a dependency cycle"
    );

    let mut wide = create_body("wide", "analytics", "");
    wide.tasks = Some(vec![pipeline_task(
        "load",
        "select 1",
        &["a", "b", "c", "d", "e", "f", "g", "h", "i"],
    )]);
    assert_eq!(
        refused(wide).await,
        format!("task `load`: {TOO_MANY_TASK_DEPENDS_ON}")
    );

    for (body, expected) in cases {
        assert_eq!(refused(body).await, expected);
    }
}

#[tokio::test]
async fn update_replaces_the_whole_task_list() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();
    let mut body = create_body("shop-silver", "analytics", "");
    body.tasks = Some(vec![pipeline_task("load", "select 1", &[])]);
    create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("created");

    let mut update = update_body("");
    update.tasks = Some(vec![
        pipeline_task("load", "select 2", &[]),
        pipeline_task("check", "select 3", &["load"]),
    ]);
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "shop-silver",
        update,
    )
    .await
    .expect("updated");
    let stored = stored_pipeline(&resources, "shop-silver")
        .await
        .expect("stored");
    assert_eq!(stored["spec"]["tasks"].as_array().unwrap().len(), 2);
    assert_eq!(stored["spec"]["tasks"][0]["sql"], "select 2");

    // A single-statement body on a task list is the sentence decision 5 fixes.
    let refused = update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "shop-silver",
        update_body("select 9"),
    )
    .await
    .expect_err("a task list is not a statement pipeline");
    assert_eq!(refused.status(), StatusCode::CONFLICT);
    assert_eq!(refused.message(), SEVERAL_TASKS);

    // And a task list never becomes a statement pipeline, nor the other way round.
    let mut statement = nova_pipeline("smoke", "analytics");
    statement["metadata"]["resourceVersion"] = json!("7");
    let resources = memory_with(
        &[("smoke", statement)],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        )],
    );
    let mut tasks_body = update_body("");
    tasks_body.tasks = Some(vec![pipeline_task("load", "select 1", &[])]);
    let refused = update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "smoke",
        tasks_body,
    )
    .await
    .expect_err("a statement pipeline is not a task list");
    assert_eq!(refused.message(), STATEMENT_PIPELINE);
}

#[tokio::test]
async fn validate_lists_tasks_when_the_list_changed() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("shop-silver", "analytics", "");
    body.tasks = Some(vec![pipeline_task("load", "select 1", &[])]);
    create_pipeline(
        Some(&resources),
        None,
        None,
        &create_limiter(),
        &ada,
        body.clone(),
    )
    .await
    .expect("created");

    let same = validate_pipeline(Some(&resources), None, None, &ada, body.clone())
        .await
        .expect("an answer");
    assert_eq!(same.action.as_deref(), Some("update"));
    assert!(same.changes.is_empty(), "{:?}", same.changes);

    body.tasks = Some(vec![pipeline_task("load", "select 2", &[])]);
    let drifted = validate_pipeline(Some(&resources), None, None, &ada, body)
        .await
        .expect("an answer");
    assert_eq!(drifted.changes, vec!["tasks".to_string()]);
}

#[tokio::test]
async fn update_conflict_on_a_changed_resource_version() {
    let original = nova_pipeline("smoke", "analytics");
    let resources = memory_with(
        &[("smoke", original.clone())],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        )],
    );

    // A replace carrying the version read before somebody else wrote is a conflict.
    let mut stale = original.clone();
    stale["metadata"]["resourceVersion"] = json!("6");
    stale["spec"]["tasks"][0]["sql"] = json!("select 9");
    let error = resources
        .replace("smoke", stale)
        .await
        .expect_err("a stale resourceVersion is a conflict");
    assert!(matches!(error, ResourceError::Conflict));
    assert_eq!(
        stored_pipeline(&resources, "smoke").await.unwrap()["spec"]["tasks"][0]["sql"],
        "select 1",
        "the stale write changed nothing"
    );

    // A resource deleted between the read and the write is the same answer for the caller.
    assert!(matches!(
        resources.replace("absent", original).await,
        Err(ResourceError::Conflict)
    ));

    let fail = resource_write_fail(ResourceError::Conflict);
    assert_eq!(fail.status(), StatusCode::CONFLICT);
    assert_eq!(
        fail.message(),
        "the pipeline changed while you edited it: reload"
    );
}

#[tokio::test]
async fn update_with_a_stale_version_is_refused_before_it_writes() {
    let resources = memory_with(
        &[("smoke", nova_pipeline("smoke", "analytics"))],
        &analytics_principal(),
    );
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();
    let sql_of = |resource: Value| resource["spec"]["tasks"][0]["sql"].clone();

    let mut stale = update_body("select 9");
    stale.resource_version = Some("6".into());
    let error = update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "smoke",
        stale,
    )
    .await
    .expect_err("somebody wrote in between");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(
        error.message(),
        "the pipeline changed while you edited it: reload"
    );
    assert_eq!(
        sql_of(stored_pipeline(&resources, "smoke").await.unwrap()),
        "select 1",
        "a refused update writes nothing"
    );

    // The version the document route handed out is the one an update accepts, once.
    let read = pipeline_spec(Some(&resources), &ada, &namespace, "smoke")
        .await
        .expect("readable")
        .resource_version
        .expect("a version");
    let mut current = update_body("select 9");
    current.resource_version = Some(read);
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "smoke",
        current.clone(),
    )
    .await
    .expect("the version it read");
    assert_eq!(
        sql_of(stored_pipeline(&resources, "smoke").await.unwrap()),
        "select 9"
    );

    // Somebody else writes meanwhile (the test store does not bump versions by itself, the
    // cluster does): the version the editor read is stale now.
    if let PipelineResources::Memory(store) = &resources {
        store.lock().unwrap().pipelines.get_mut("smoke").unwrap()["metadata"]["resourceVersion"] =
            json!("8");
    }
    current.sql = "select 10".into();
    let error = update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "smoke",
        current,
    )
    .await
    .expect_err("the stored version moved on");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(
        sql_of(stored_pipeline(&resources, "smoke").await.unwrap()),
        "select 9",
        "the stale edit changed nothing"
    );
}

fn provenance(bundle: &str, target: &str, revision: Option<&str>) -> Provenance {
    Provenance {
        bundle: bundle.into(),
        target: target.into(),
        revision: revision.map(str::to_string),
        repository: None,
    }
}

#[tokio::test]
async fn a_deploy_stamps_where_the_pipeline_came_from_and_the_view_shows_it() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);

    let mut body = create_body("daily-trips", "analytics", "select 1");
    body.provenance = Some(provenance("shop-etl", "prod", Some("0123abc-dirty")));
    let view = create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("created");
    let deployment = view.deployment.expect("the view shows it");
    assert_eq!(
        (
            deployment.bundle.as_str(),
            deployment.target.as_str(),
            deployment.revision.as_deref()
        ),
        ("shop-etl", "prod", Some("0123abc-dirty"))
    );
    assert!(deployment.deployed_by.is_some() && deployment.deployed_at.is_some());

    let stored = stored_pipeline(&resources, "daily-trips").await.unwrap();
    assert_eq!(
        stored["metadata"]["labels"]["stardelt.io/bundle"],
        "shop-etl"
    );
    assert_eq!(stored["metadata"]["labels"]["stardelt.io/target"], "prod");
    assert_eq!(
        stored["metadata"]["annotations"]["stardelt.io/revision"],
        "0123abc-dirty"
    );
    assert_eq!(
        stored["metadata"]["labels"]["app.kubernetes.io/managed-by"], "stardelt-nova",
        "the labels that were there stay"
    );
    assert!(stored["metadata"]["annotations"]["stardelt.io/created-by"].is_string());

    let plain = create_pipeline(
        Some(&resources),
        None,
        None,
        &create_limiter(),
        &ada,
        create_body("by-hand", "analytics", "select 1"),
    )
    .await
    .expect("created");
    assert_eq!(
        plain.deployment, None,
        "a pipeline made by hand says nothing"
    );
}

#[tokio::test]
async fn a_deploy_that_names_something_odd_is_refused_before_anything_is_written() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    for (bundle, target, revision, repository, sentence) in [
        ("Shop", "prod", None, None, "the bundle name must be"),
        ("ab", "prod", None, None, "the bundle name must be"),
        ("shop-etl", "Prod", None, None, "the target must be"),
        ("shop-etl", "", None, None, "the target must be"),
        (
            "shop-etl",
            "prod",
            Some("main"),
            None,
            "the revision must be",
        ),
        (
            "shop-etl",
            "prod",
            Some("abc"),
            None,
            "the revision must be",
        ),
        (
            "shop-etl",
            "prod",
            None,
            Some("http://git.example/a"),
            "the repository must be",
        ),
        (
            "shop-etl",
            "prod",
            None,
            Some("https://"),
            "the repository must be",
        ),
        (
            "shop-etl",
            "prod",
            None,
            Some("https:///shop"),
            "the repository must be",
        ),
        (
            "shop-etl",
            "prod",
            None,
            Some("https://ada:tok@git.example/a"),
            "the repository must be",
        ),
    ] {
        let mut body = create_body("daily-trips", "analytics", "select 1");
        body.provenance = Some(Provenance {
            repository: repository.map(str::to_string),
            ..provenance(bundle, target, revision)
        });
        let error = create_pipeline(
            Some(&resources),
            None,
            None,
            &create_limiter(),
            &ada,
            body.clone(),
        )
        .await
        .expect_err("refused");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{bundle} {target}");
        assert!(error.message().starts_with(sentence), "{}", error.message());
        let answer = validate_pipeline(Some(&resources), None, None, &ada, body)
            .await
            .expect("an answer");
        assert!(!answer.ok);
        assert_eq!(answer.errors[0].message, error.message());
    }
    assert!(resources.list().await.unwrap().is_empty());
}

#[test]
fn the_digest_ignores_key_order_and_what_a_deploy_does_not_decide() {
    let one = json!({"spec": {
        "ownerGroup": "x",
        "tasks": [{"kind": "run", "sql": "select 1"}],
        "params": {"a": "1", "b": "2"},
    }});
    let same = json!({"spec": {
        "params": {"b": "2", "a": "1"},
        "tasks": [{"sql": "select 1", "kind": "run"}],
        "ownerGroup": "y",
    }});
    let other = json!({"spec": {
        "tasks": [{"kind": "run", "sql": "select 2"}],
        "params": {"a": "1", "b": "2"},
    }});
    assert_eq!(
        canonical(&json!({"b": [1, {"d": 1, "c": null}], "a": "x"})),
        r#"{"a":"x","b":[1,{"c":null,"d":1}]}"#
    );
    assert_eq!(spec_digest(&one), spec_digest(&same));
    assert_ne!(spec_digest(&one), spec_digest(&other));
    for key in ["schedule", "params", "concurrency", "retries", "timeout"] {
        let mut changed = one.clone();
        changed["spec"][key] = json!("another");
        assert_ne!(spec_digest(&one), spec_digest(&changed), "{key} is decided");
    }
    assert_eq!(spec_digest(&one).len(), 64);
}

#[tokio::test]
async fn a_change_after_the_deploy_shows_as_modified_and_the_next_deploy_clears_it() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let mut body = create_body("daily-trips", "analytics", "select 1");
    body.provenance = Some(provenance("shop-etl", "prod", Some("aaaaaaa")));
    let created = create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("created");
    assert_eq!(
        created.deployment.expect("stamped").modified,
        None,
        "right after the deploy nothing differs"
    );

    // The form or the YAML editor: no stamp in the body, so the old stamp and digest stay.
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "daily-trips",
        update_body("select 2"),
    )
    .await
    .expect("updated");
    let edited = stored_pipeline(&resources, "daily-trips").await.unwrap();
    assert_eq!(deployment_of(&edited).expect("kept").modified, Some(true));

    // The next deploy writes a new digest.
    let mut deploy = update_body("select 2");
    deploy.provenance = Some(provenance("shop-etl", "prod", Some("bbbbbbb")));
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "daily-trips",
        deploy,
    )
    .await
    .expect("updated");
    let deployed = stored_pipeline(&resources, "daily-trips").await.unwrap();
    assert_eq!(deployment_of(&deployed).expect("stamped").modified, None);

    // A pipeline stamped before the digest existed says nothing either way.
    let mut old = deployed.clone();
    old["metadata"]["annotations"]
        .as_object_mut()
        .unwrap()
        .remove("stardelt.io/spec-digest");
    old["spec"]["schedule"] = json!({"cron": "5 4 * * *", "timezone": "UTC"});
    assert_eq!(deployment_of(&old).expect("stamped").modified, None);
}

#[tokio::test]
async fn a_plan_marks_a_pipeline_changed_after_its_deploy() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();
    let document = plan_document("daily-trips", "analytics", "select 1", "0 3 * * *");

    // The deploy stamps the pipeline, so a change after it can be told apart from the bundle.
    let mut body: CreatePipelineRequest =
        serde_json::from_value(document.clone()).expect("a create body");
    body.provenance = Some(provenance("shop-etl", "prod", Some("aaaaaaa")));
    create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect("created");

    // Someone changes the schedule outside the bundle; the stamp and its digest stay.
    let mut edited = update_body("select 1");
    edited.schedule = Some(ScheduleRequest {
        cron: "5 4 * * *".into(),
        timezone: "Europe/Berlin".into(),
    });
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "daily-trips",
        edited,
    )
    .await
    .expect("updated");

    let plan = plan_bundle(
        Some(&resources),
        None,
        None,
        &ada,
        None,
        &[],
        plan_request(vec![document.clone()]),
    )
    .await
    .expect("an answer");
    assert!(plan.ok, "{:?}", plan.errors);
    assert_eq!(plan.changes.len(), 1, "{:?}", plan.changes);
    let change = &plan.changes[0];
    assert_eq!(change.action, "update");
    assert!(
        change.modified,
        "the plan says the deploy would overwrite the change"
    );
    assert_eq!(change.diff, vec!["schedule"], "{:?}", change.diff);

    // The deploy itself writes a new digest; the same document is then no change at all.
    let mut applied = update_body("select 1");
    applied.schedule = Some(ScheduleRequest {
        cron: "0 3 * * *".into(),
        timezone: "Europe/Berlin".into(),
    });
    applied.provenance = Some(provenance("shop-etl", "prod", Some("bbbbbbb")));
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "daily-trips",
        applied,
    )
    .await
    .expect("updated");

    let again = plan_bundle(
        Some(&resources),
        None,
        None,
        &ada,
        None,
        &[],
        plan_request(vec![document]),
    )
    .await
    .expect("an answer");
    assert!(again.ok, "{:?}", again.errors);
    assert_eq!(again.changes.len(), 1, "{:?}", again.changes);
    assert_eq!(again.changes[0].action, "unchanged");
    assert!(!again.changes[0].modified, "the deploy cleared the change");
    assert!(again.changes[0].diff.is_empty());
}

#[tokio::test]
async fn the_check_says_when_an_update_would_overwrite_a_change_made_after_the_deploy() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();
    let check = |body: CreatePipelineRequest| {
        let resources = &resources;
        let ada = &ada;
        async move {
            validate_pipeline(Some(resources), None, None, ada, body)
                .await
                .expect("an answer")
        }
    };

    let mut body = create_body("daily-trips", "analytics", "select 1");
    body.provenance = Some(provenance("shop-etl", "prod", Some("aaaaaaa")));
    create_pipeline(
        Some(&resources),
        None,
        None,
        &create_limiter(),
        &ada,
        body.clone(),
    )
    .await
    .expect("created");
    let same = check(body.clone()).await;
    assert_eq!(same.modified, None, "nothing changed since the deploy");

    // The form or the YAML editor changes it; the next deploy would overwrite that.
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "daily-trips",
        update_body("select 2"),
    )
    .await
    .expect("updated");
    let drifted = check(body.clone()).await;
    assert_eq!(drifted.action.as_deref(), Some("update"));
    assert_eq!(drifted.modified, Some(true));

    // A create has nothing to overwrite, and a pipeline no deploy touched has no baseline.
    let mut fresh = create_body("other", "analytics", "select 1");
    fresh.provenance = body.provenance.clone();
    assert_eq!(check(fresh).await.modified, None);
    create_pipeline(
        Some(&resources),
        None,
        None,
        &create_limiter(),
        &ada,
        create_body("by-hand", "analytics", "select 1"),
    )
    .await
    .expect("created");
    let by_hand = check(create_body("by-hand", "analytics", "select 2")).await;
    assert_eq!(by_hand.modified, None);

    // The deploy itself clears it.
    let mut deploy = update_body("select 1");
    deploy.provenance = body.provenance.clone();
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "daily-trips",
        deploy,
    )
    .await
    .expect("updated");
    assert_eq!(check(body).await.modified, None);
}

#[tokio::test]
async fn an_update_replaces_the_stamp_when_it_names_one_and_keeps_it_when_it_does_not() {
    let mut stored = nova_pipeline("daily-trips", "analytics");
    stamp_provenance(
        &mut stored,
        &provenance("shop-etl", "prod", Some("aaaaaaa")),
        "ada",
    );
    let resources = memory_with(&[("daily-trips", stored)], &analytics_principal());
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    // The check: the same stamp is no change, another commit is, and saying nothing is not.
    let mut body = create_body("daily-trips", "analytics", "select 1");
    body.provenance = Some(provenance("shop-etl", "prod", Some("aaaaaaa")));
    let same = validate_pipeline(Some(&resources), None, None, &ada, body.clone())
        .await
        .expect("an answer");
    assert!(same.changes.is_empty(), "{:?}", same.changes);
    body.provenance = Some(provenance("shop-etl", "prod", Some("bbbbbbb")));
    let newer = validate_pipeline(Some(&resources), None, None, &ada, body.clone())
        .await
        .expect("an answer");
    assert_eq!(newer.changes, vec!["deployment"]);
    body.provenance = None;
    let silent = validate_pipeline(Some(&resources), None, None, &ada, body)
        .await
        .expect("an answer");
    assert!(silent.changes.is_empty());

    // The write: no stamp in the body leaves the stored one alone.
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "daily-trips",
        update_body("select 2"),
    )
    .await
    .expect("updated");
    let kept = stored_pipeline(&resources, "daily-trips").await.unwrap();
    assert_eq!(kept["metadata"]["labels"]["stardelt.io/target"], "prod");
    assert_eq!(
        kept["metadata"]["annotations"]["stardelt.io/revision"],
        "aaaaaaa"
    );

    // A stamp in the body replaces it, and a stamp without a commit removes the old commit.
    let mut deploy = update_body("select 3");
    deploy.provenance = Some(provenance("shop-etl", "dev", None));
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "daily-trips",
        deploy,
    )
    .await
    .expect("updated");
    let replaced = stored_pipeline(&resources, "daily-trips").await.unwrap();
    assert_eq!(replaced["metadata"]["labels"]["stardelt.io/target"], "dev");
    assert!(
        replaced["metadata"]["annotations"]
            .get("stardelt.io/revision")
            .is_none()
    );
}

#[tokio::test]
async fn update_rechecks_the_principal() {
    let namespace = crate::cluster::namespace();
    let ada = who("sub-ada", &["analytics"], false);

    // The principal exists but stopped being ready.
    let resources = memory_with(
        &[("smoke", nova_pipeline("smoke", "analytics"))],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(false)),
        )],
    );
    let error = update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "smoke",
        update_body("select 2"),
    )
    .await
    .expect_err("not ready any more");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.message(), NOT_READY_MESSAGE);
    assert_eq!(
        stored_pipeline(&resources, "smoke").await.unwrap()["spec"]["tasks"][0]["sql"],
        "select 1"
    );

    // The principal is gone (an administrator deleted it).
    let resources = memory_with(&[("smoke", nova_pipeline("smoke", "analytics"))], &[]);
    let error = update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &namespace,
        "smoke",
        update_body("select 2"),
    )
    .await
    .expect_err("no principal");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(error.message(), missing_principal_sentence("analytics"));
}

#[tokio::test]
async fn delete_removes_only_nova_managed_pipelines_the_caller_may_act_on() {
    let mut legacy = nova_pipeline("legacy", "analytics");
    legacy["metadata"]["labels"]
        .as_object_mut()
        .unwrap()
        .remove(MANAGED_BY_LABEL);
    let resources = memory_with(
        &[
            ("mine", nova_pipeline("mine", "analytics")),
            ("legacy", legacy),
            ("foreign", nova_pipeline("foreign", "finance")),
        ],
        &[],
    );
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    delete_pipeline(Some(&resources), &ada, &namespace, "mine")
        .await
        .expect("the owner group deletes its own Nova pipeline");
    assert!(stored_pipeline(&resources, "mine").await.is_none());
    let error = delete_pipeline(Some(&resources), &ada, &namespace, "mine")
        .await
        .expect_err("a deleted pipeline is gone");
    assert_eq!(error.status(), StatusCode::NOT_FOUND);

    let error = delete_pipeline(Some(&resources), &ada, &namespace, "legacy")
        .await
        .expect_err("kubectl's pipeline is not Nova's to delete");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(
        error.message(),
        "this pipeline is managed outside Nova (kubectl or Git); change it there"
    );
    assert!(
        stored_pipeline(&resources, "legacy").await.is_some(),
        "a refused delete removes nothing"
    );

    let error = delete_pipeline(Some(&resources), &admin(), &namespace, "legacy")
        .await
        .expect_err("an administrator does not delete somebody's manifest either");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert!(stored_pipeline(&resources, "legacy").await.is_some());
}

#[tokio::test]
async fn delete_of_a_hidden_pipeline_is_404() {
    let resources = memory_with(&[("foreign", nova_pipeline("foreign", "finance"))], &[]);
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let error = delete_pipeline(Some(&resources), &ada, &namespace, "foreign")
        .await
        .expect_err("another group's pipeline");
    assert_eq!(error.status(), StatusCode::NOT_FOUND);
    assert_eq!(error.message(), "no such pipeline");
    assert!(
        stored_pipeline(&resources, "foreign").await.is_some(),
        "a hidden pipeline is not deleted"
    );
}

#[tokio::test]
async fn principals_route_reports_each_group() {
    let resources = memory_with(
        &[],
        &[
            (
                "pipeline-analytics",
                service_principal("pipeline-analytics", "analytics", Some(true)),
            ),
            (
                "pipeline-finance",
                service_principal("pipeline-finance", "finance", Some(false)),
            ),
        ],
    );
    let who = who("sub-ada", &["analytics", "finance", "ops"], false);

    let answer = principal_views(Some(&resources), &who)
        .await
        .expect("the principals are readable");
    assert_eq!(
        answer.groups,
        [
            PrincipalView {
                group: "analytics".into(),
                principal: Some("pipeline-analytics".into()),
                ready: true,
                message: None,
            },
            PrincipalView {
                group: "finance".into(),
                principal: Some("pipeline-finance".into()),
                ready: false,
                message: Some(NOT_READY_MESSAGE.into()),
            },
            PrincipalView {
                group: "ops".into(),
                principal: None,
                ready: false,
                message: Some(missing_principal_sentence("ops")),
            },
        ],
        "one entry per group, sorted, with the sentence of the ticket"
    );
}

#[tokio::test]
async fn service_principals_route_is_group_scoped() {
    let resources = memory_with(
        &[],
        &[
            (
                "pipeline-analytics",
                service_principal("pipeline-analytics", "analytics", Some(true)),
            ),
            (
                "collector-finance",
                service_principal("collector-finance", "finance", Some(false)),
            ),
        ],
    );
    let who = who("sub-ada", &["analytics"], false);
    let answer = service_principal_views(Some(&resources), &who)
        .await
        .expect("the service principals are readable");
    assert_eq!(answer.principals.len(), 1);
    let view = &answer.principals[0];
    assert_eq!(view.name, "pipeline-analytics");
    assert_eq!(view.owner_group, "analytics");
    assert_eq!(view.purpose, "pipeline");
    assert!(view.audiences.is_empty());
    assert!(view.ready);
    assert_eq!(view.message, None);
    assert_eq!(
        view.workload_identity.as_deref(),
        Some("kubernetes~pipeline-analytics-uid")
    );
    assert_eq!(view.client_id.as_deref(), Some("pipeline-analytics"));
}

#[tokio::test]
async fn service_principals_route_shows_everything_to_an_administrator() {
    let resources = memory_with(
        &[],
        &[
            (
                "pipeline-analytics",
                service_principal("pipeline-analytics", "analytics", Some(true)),
            ),
            (
                "collector-finance",
                service_principal("collector-finance", "finance", Some(false)),
            ),
        ],
    );
    let answer = service_principal_views(Some(&resources), &admin())
        .await
        .expect("the service principals are readable");
    let names: Vec<&str> = answer
        .principals
        .iter()
        .map(|principal| principal.name.as_str())
        .collect();
    assert_eq!(names, vec!["collector-finance", "pipeline-analytics"]);
    let client_ids: Vec<&str> = answer
        .principals
        .iter()
        .map(|principal| {
            principal
                .client_id
                .as_deref()
                .expect("every visible principal carries its client id")
        })
        .collect();
    assert_eq!(client_ids, vec!["collector-finance", "pipeline-analytics"]);
}

#[tokio::test]
async fn service_principals_route_reports_purpose_and_why_not_ready() {
    let principal = json!({
        "apiVersion": "stardelt.io/v1alpha1",
        "kind": "ServicePrincipal",
        "metadata": {
            "name": "collector-analytics",
            "namespace": crate::cluster::namespace(),
        },
        "spec": {
            "ownerGroup": "analytics",
            "purpose": "collector",
            "audiences": ["trino", "lakekeeper"]
        },
        "status": {
            "ready": false,
            "conditions": [{
                "type": "Ready",
                "status": "False",
                "reason": "Pending",
                "message": NOT_READY_MESSAGE,
            }],
        }
    });
    let resources = memory_with(&[], &[("collector-analytics", principal)]);
    let who = who("sub-ada", &["analytics"], false);
    let answer = service_principal_views(Some(&resources), &who)
        .await
        .expect("the service principals are readable");
    assert_eq!(answer.principals.len(), 1);
    let view = &answer.principals[0];
    assert_eq!(view.purpose, "collector");
    assert_eq!(view.audiences, vec!["trino", "lakekeeper"]);
    assert!(!view.ready);
    assert_eq!(view.message.as_deref(), Some(NOT_READY_MESSAGE));
    assert_eq!(view.workload_identity, None);
    assert_eq!(view.client_id, None);
}

#[tokio::test]
async fn audit_has_lengths_not_text() {
    let resources = analytics_ready();
    let actor = who("sub-audit-210", &["analytics"], false);
    let namespace = crate::cluster::namespace();
    let limiter = create_limiter();
    const CANARY: &str = "select 'canary-7f3a'";
    let _ = audit::test_sink::take_for("sub-audit-210");

    create_pipeline(
        Some(&resources),
        None,
        None,
        &limiter,
        &actor,
        create_body("canary", "analytics", CANARY),
    )
    .await
    .expect("create");
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &actor,
        &namespace,
        "canary",
        update_body(CANARY),
    )
    .await
    .expect("update");
    delete_pipeline(Some(&resources), &actor, &namespace, "canary")
        .await
        .expect("delete");

    let records = audit::test_sink::take_for("sub-audit-210");
    assert_eq!(
        records
            .iter()
            .map(|record| record.action.as_str())
            .collect::<Vec<_>>(),
        ["create", "update", "delete"]
    );
    assert_eq!(
        records[0].statement_chars,
        Some(CANARY.chars().count()),
        "the create records the statement's length"
    );
    assert_eq!(records[0].schedule.as_deref(), Some("0 3 * * *"));
    assert_eq!(
        records[1].statement_chars,
        Some(CANARY.chars().count()),
        "the update records the statement's length"
    );
    assert_eq!(
        records[2].statement_chars, None,
        "a delete has no statement"
    );
    let serialized = format!("{records:?}");
    assert!(
        !serialized.contains("canary-7f3a"),
        "the statement is never audited: {serialized}"
    );
}

#[tokio::test]
async fn cluster_down_is_503_and_responses_are_not_cacheable() {
    let (store, _store_fake) = crate::secret_store::fake::start().await;
    let (airflow, _airflow_fake) = fake_airflow::start().await;
    let namespace = crate::cluster::namespace();
    let app = router()
        .with_state(test_state(None, airflow, store))
        .layer(Extension(principal(admin())));

    let create = json!({
        "name": "nightly",
        "ownerGroup": "stardelt-admins",
        "sql": "select 1",
        "schedule": { "cron": "0 3 * * *", "timezone": "Europe/Berlin" }
    });
    let update = json!({ "sql": "select 2", "schedule": null });
    let routes: [(&str, String, Option<Value>); 4] = [
        ("POST", "/pipelines".to_string(), Some(create)),
        ("GET", "/pipelines/principals".to_string(), None),
        (
            "PUT",
            format!("/pipelines/{namespace}/nightly"),
            Some(update),
        ),
        ("DELETE", format!("/pipelines/{namespace}/nightly"), None),
    ];
    for (method, uri, body) in routes {
        let mut request = Request::builder().method(method).uri(&uri);
        let body = match body {
            Some(body) => {
                request = request.header("content-type", "application/json");
                Body::from(body.to_string())
            }
            None => Body::empty(),
        };
        let response = app
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{method} {uri}"
        );
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store"),
            "{method} {uri} is never cached"
        );
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["error"], "no cluster connection", "{method} {uri}");
    }

    // The success statuses through the router and the real client (a fake API server).
    let kube = fake_kube::start_with(
        vec![],
        vec![service_principal(
            "pipeline-analytics",
            "analytics",
            Some(true),
        )],
    )
    .await;
    let (store, _store_fake) = crate::secret_store::fake::start().await;
    let (airflow, _airflow_fake) = fake_airflow::start().await;
    let app = router()
        .with_state(test_state(Some(kube), airflow, store))
        .layer(Extension(principal(who("sub-ada", &["analytics"], false))));

    // principals: 200 and the ready entry.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/pipelines/principals")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["groups"][0]["principal"], "pipeline-analytics");
    assert_eq!(value["groups"][0]["ready"], true);

    // create: 201 with the new pipeline as `loading`.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "name": "nightly",
                        "ownerGroup": "analytics",
                        "sql": "select 1",
                        "schedule": { "cron": "0 3 * * *", "timezone": "Europe/Berlin" }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["name"], "nightly");
    assert_eq!(value["loaded"], false);
    assert_eq!(value["state"], "loading");

    // update: 200.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/pipelines/{namespace}/nightly"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "sql": "select 2", "schedule": null }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["schedule"], Value::Null);

    // delete: 204 and no body.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/pipelines/{namespace}/nightly"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    assert!(bytes.is_empty(), "204 carries no body");
}

// -----------------------------------------------------------------------
// Schedule options, the statement check and the next-runs preview (#223)
// -----------------------------------------------------------------------

/// A state whose engine is the fake, for the two routes that need no cluster.
async fn check_state(trino_url: &str) -> Arc<AppState> {
    let (store, _store_fake) = crate::secret_store::fake::start().await;
    let (airflow, _airflow_fake) = fake_airflow::start().await;
    test_state_at(None, airflow, store, trino_url)
}

fn check_body(sql: &str) -> CheckStatementRequest {
    CheckStatementRequest {
        sql: sql.into(),
        params: BTreeMap::new(),
    }
}

fn params(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

#[tokio::test]
async fn params_and_options_written_to_the_resource() {
    let resources = analytics_ready();
    let mut body = create_body("daily-trips", "analytics", "select '${params.day}'");
    body.params = params(&[("day", "2026-10-01")]);
    body.options = Some(PipelineOptions {
        retries: Some(3),
        backoff_minutes: Some(10),
        timeout_minutes: Some(360),
        concurrency: Some("Allow".into()),
    });

    create_pipeline(
        Some(&resources),
        None,
        None,
        &create_limiter(),
        &who("sub-ada", &["analytics"], false),
        body,
    )
    .await
    .expect("the pipeline is created");

    let stored = stored_pipeline(&resources, "daily-trips")
        .await
        .expect("the resource is written");
    assert_eq!(
        stored["spec"]["params"],
        json!({ "day": "2026-10-01" }),
        "the parameter table is written as given: {stored}"
    );
    assert_eq!(
        stored["spec"]["retries"],
        json!({ "attempts": 3, "backoff": "10m" }),
        "the CRD's retries shape: {stored}"
    );
    assert_eq!(
        stored["spec"]["timeout"],
        json!("360m"),
        "the loader's duration grammar: {stored}"
    );
    assert_eq!(stored["spec"]["concurrency"], json!("Allow"));
    assert_eq!(
        stored["spec"]["tasks"][0]["sql"], "select '${params.day}'",
        "the statement is stored unexpanded: the loader fills it when the run starts"
    );
}

#[tokio::test]
async fn defaults_when_omitted() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);

    create_pipeline(
        Some(&resources),
        None,
        None,
        &create_limiter(),
        &ada,
        create_body("plain", "analytics", "select 1"),
    )
    .await
    .expect("a create without params and options is fine");

    let stored = stored_pipeline(&resources, "plain").await.expect("written");
    assert_eq!(
        stored["spec"]["retries"],
        json!({ "attempts": 1, "backoff": "5m" })
    );
    assert_eq!(stored["spec"]["timeout"], json!("120m"));
    assert_eq!(stored["spec"]["concurrency"], json!("Forbid"));
    assert!(
        stored["spec"].get("params").is_none(),
        "no parameter table is written when none is given: {stored}"
    );

    // The update body describes the whole editable state: an omitted option is the default,
    // never "keep whatever was there".
    let mut update = update_body("select 2");
    update.options = Some(PipelineOptions {
        retries: Some(4),
        ..PipelineOptions::default()
    });
    update_pipeline(
        Some(&resources),
        None,
        None,
        None,
        &ada,
        &crate::cluster::namespace(),
        "plain",
        update,
    )
    .await
    .expect("the update succeeds");
    let stored = stored_pipeline(&resources, "plain").await.expect("written");
    assert_eq!(
        stored["spec"]["retries"],
        json!({ "attempts": 4, "backoff": "5m" })
    );
    assert_eq!(stored["spec"]["timeout"], json!("120m"));
    assert_eq!(stored["spec"]["concurrency"], json!("Forbid"));
}

#[tokio::test]
async fn option_ranges_refused_with_sentences() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let limiter = create_limiter();
    let too_many: BTreeMap<String, String> = (0..17)
        .map(|index| (format!("p{index}"), "1".to_string()))
        .collect();
    let long_value = "x".repeat(MAX_PARAM_VALUE_CHARS + 1);

    let cases: Vec<(BTreeMap<String, String>, PipelineOptions, &str)> = vec![
        (
            BTreeMap::new(),
            PipelineOptions {
                retries: Some(-1),
                ..PipelineOptions::default()
            },
            RETRIES_RANGE,
        ),
        (
            BTreeMap::new(),
            PipelineOptions {
                retries: Some(6),
                ..PipelineOptions::default()
            },
            RETRIES_RANGE,
        ),
        (
            BTreeMap::new(),
            PipelineOptions {
                backoff_minutes: Some(0),
                ..PipelineOptions::default()
            },
            BACKOFF_RANGE,
        ),
        (
            BTreeMap::new(),
            PipelineOptions {
                backoff_minutes: Some(61),
                ..PipelineOptions::default()
            },
            BACKOFF_RANGE,
        ),
        (
            BTreeMap::new(),
            PipelineOptions {
                timeout_minutes: Some(0),
                ..PipelineOptions::default()
            },
            TIMEOUT_RANGE,
        ),
        (
            BTreeMap::new(),
            PipelineOptions {
                timeout_minutes: Some(721),
                ..PipelineOptions::default()
            },
            TIMEOUT_RANGE,
        ),
        (
            BTreeMap::new(),
            PipelineOptions {
                concurrency: Some("Replace".into()),
                ..PipelineOptions::default()
            },
            CONCURRENCY_RULE,
        ),
        (
            params(&[("Day", "1")]),
            PipelineOptions::default(),
            PARAM_NAMES,
        ),
        (
            params(&[("day", &long_value)]),
            PipelineOptions::default(),
            PARAM_VALUE_TOO_LONG,
        ),
        (
            params(&[("day", "a\0b")]),
            PipelineOptions::default(),
            PARAM_VALUE_NUL,
        ),
        (too_many, PipelineOptions::default(), TOO_MANY_PARAMS),
    ];

    for (index, (parameters, options, sentence)) in cases.into_iter().enumerate() {
        let mut body = create_body(&format!("refused-{index}"), "analytics", "select 1");
        body.params = parameters;
        body.options = Some(options);
        let error = create_pipeline(Some(&resources), None, None, &limiter, &ada, body)
            .await
            .expect_err("the option is out of range");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{sentence}");
        assert_eq!(error.message(), sentence);
    }
    assert!(
        resources.list().await.unwrap().is_empty(),
        "a refused option writes nothing"
    );
}

#[tokio::test]
async fn unknown_placeholder_refused() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let limiter = create_limiter();

    for (sql, params, sentence) in [
        (
            "select '${params.missing}'",
            params(&[("day", "1")]),
            "unknown placeholder ${params.missing}",
        ),
        (
            "select '${run.when}'",
            BTreeMap::new(),
            "unknown placeholder ${run.when}",
        ),
    ] {
        let mut body = create_body("typo", "analytics", sql);
        body.params = params;
        let error = create_pipeline(Some(&resources), None, None, &limiter, &ada, body)
            .await
            .expect_err("the loader would fail this run");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{sql}");
        assert_eq!(error.message(), sentence, "{sql}");
    }
    assert!(resources.list().await.unwrap().is_empty());

    // The three the loader fills pass, and so does a `${…}` its regex does not match (it
    // leaves such text in the statement, so Nova must not refuse it).
    let statement = "select '${run.date}', '${run.id}', '${params.day}', '${a-b}'";
    let mut body = create_body("defined", "analytics", statement);
    body.params = params(&[("day", "2026-10-01")]);
    create_pipeline(Some(&resources), None, None, &limiter, &ada, body)
        .await
        .expect("every placeholder is one the loader fills");
    let stored = stored_pipeline(&resources, "defined")
        .await
        .expect("written");
    assert_eq!(stored["spec"]["tasks"][0]["sql"], statement);
}

#[tokio::test]
async fn detail_returns_params_and_options() {
    let mut smoke = resource("smoke", Some("analytics"));
    smoke["spec"]["params"] = json!({ "day": "2026-10-01" });
    smoke["spec"]["retries"] = json!({ "attempts": 3, "backoff": "30m" });
    smoke["spec"]["timeout"] = json!("6h");
    smoke["spec"]["concurrency"] = json!("Allow");
    let resources = memory(&[("smoke", smoke)]);
    let (airflow, _fake) = fake_airflow::start().await;

    let detail = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
    )
    .await
    .expect("the detail is readable");
    assert_eq!(
        detail.params.get("day").map(String::as_str),
        Some("2026-10-01")
    );
    assert_eq!(
        detail.options,
        PipelineOptionsView {
            retries: 3,
            backoff_minutes: 30,
            timeout_minutes: 360,
            concurrency: "Allow".into(),
        }
    );

    // A resource without the fields answers the defaults, so the dialog always has values.
    let resources = memory(&[("smoke", resource("smoke", Some("analytics")))]);
    let detail = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &admin(),
        &crate::cluster::namespace(),
        "smoke",
    )
    .await
    .expect("the detail is readable");
    assert_eq!(
        detail.options,
        PipelineOptionsView {
            retries: 2,
            backoff_minutes: 5,
            // `spec.timeout` is absent in the fixture: the default two hours.
            timeout_minutes: 120,
            concurrency: "Forbid".into(),
        }
    );
}

#[tokio::test]
async fn check_sends_explain_never_analyze() {
    let (trino, fake) = fake_trino::start(None).await;
    let state = check_state(&trino).await;
    let ada = principal(who("sub-ada", &["analytics"], false));

    let answer = check_statement(&state, &ada, check_body("select 1"))
        .await
        .expect("the engine accepts the statement");
    assert_eq!(
        answer,
        CheckStatementResponse {
            checked: true,
            ok: Some(true),
            message: None,
        }
    );

    let fake = fake.lock().expect("fake trino");
    assert_eq!(fake.statements, vec!["EXPLAIN select 1".to_string()]);
    assert_eq!(fake.sources, vec![SOURCE_SCHEDULE_CHECK.to_string()]);
    assert_eq!(
        fake.users,
        vec!["sub-ada".to_string()],
        "the check runs as the caller, so the engine's query log names them"
    );
    assert!(
        !fake.statements[0].to_ascii_uppercase().contains("ANALYZE"),
        "never EXPLAIN ANALYZE: it would run the statement"
    );
}

#[tokio::test]
async fn check_fills_placeholders() {
    let (trino, fake) = fake_trino::start(None).await;
    let state = check_state(&trino).await;
    let ada = principal(who("sub-ada", &["analytics"], false));

    let statement = "select '${run.date}' as d, '${run.id}' as i, '${params.day}' as p";
    let mut body = check_body(statement);
    body.params = params(&[("day", "2026-01-01")]);
    check_statement(&state, &ada, body)
        .await
        .expect("the placeholders are filled");

    let today = now_utc().format("%Y-%m-%d").to_string();
    let fake = fake.lock().expect("fake trino");
    assert_eq!(
        fake.statements,
        vec![format!(
            "EXPLAIN select '{today}' as d, 'check' as i, '2026-01-01' as p"
        )],
        "run.date is today (UTC), run.id is `check`, params as given"
    );
    assert!(
        !fake.statements[0].contains("${"),
        "no placeholder reaches the engine: {}",
        fake.statements[0]
    );
}

#[tokio::test]
async fn check_skips_unsupported_kinds() {
    let (trino, fake) = fake_trino::start(None).await;
    let state = check_state(&trino).await;
    let ada = principal(who("sub-ada", &["analytics"], false));

    for sql in [
        "drop table x",
        "selec 1",
        "show tables",
        "create table t (a int)",
        "grant select on t to public",
    ] {
        let answer = check_statement(&state, &ada, check_body(sql))
            .await
            .unwrap_or_else(|error| panic!("{sql}: {}", error.message()));
        assert_eq!(
            answer,
            CheckStatementResponse {
                checked: false,
                ok: None,
                message: None,
            },
            "{sql} cannot be checked here"
        );
    }
    assert!(
        fake.lock().expect("fake trino").statements.is_empty(),
        "a kind of statement Nova cannot check never reaches the engine"
    );

    // `CREATE … AS …` has a plan, so it is checked.
    let answer = check_statement(&state, &ada, check_body("CREATE TABLE t AS SELECT 1"))
        .await
        .expect("a create-from-query is checkable");
    assert_eq!(answer.ok, Some(true));
    assert_eq!(
        fake.lock().expect("fake trino").statements,
        vec!["EXPLAIN CREATE TABLE t AS SELECT 1".to_string()]
    );
}

#[tokio::test]
async fn check_returns_engine_message_on_error() {
    let error = json!({
        "errorName": "SYNTAX_ERROR",
        "message": "line 1:1: mismatched input 'selec'. Expecting: 'ALTER', 'ANALYZE', ...",
    });
    let (trino, _fake) = fake_trino::start(Some(error)).await;
    let state = check_state(&trino).await;
    let ada = principal(who("sub-ada", &["analytics"], false));

    let answer = check_statement(&state, &ada, check_body("select 1"))
        .await
        .expect("a refused statement is an answer, not a failure");
    assert!(answer.checked);
    assert_eq!(answer.ok, Some(false));
    assert_eq!(
        answer.message.as_deref(),
        Some(
            "SYNTAX_ERROR: line 1:1: mismatched input 'selec'. Expecting: 'ALTER', 'ANALYZE', ..."
        ),
        "the engine's own sentence reaches the dialog"
    );
    assert!(
        !answer
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("failureInfo"),
        "no stack trace in an answer"
    );

    // An engine that does not answer is 502 (U12), not "the engine said no".
    let state = check_state("http://127.0.0.1:1").await;
    let error = check_statement(&state, &ada, check_body("select 1"))
        .await
        .expect_err("the engine is unreachable");
    assert_eq!(error.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(error.message(), crate::trino::ENGINE_UNREACHABLE);
}

#[tokio::test]
async fn validate_accepts_a_new_pipeline_and_writes_nothing() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);

    let answer = validate_pipeline(
        Some(&resources),
        None,
        None,
        &ada,
        create_body("daily-trips", "analytics", "select 1"),
    )
    .await
    .expect("an answer");

    assert!(answer.ok);
    assert_eq!(answer.action.as_deref(), Some("create"));
    assert!(answer.errors.is_empty());
    assert_eq!(answer.next_runs.len(), 5, "the schedule is previewed");
    assert!(
        resources.list().await.unwrap().is_empty(),
        "a check writes nothing"
    );
}

#[tokio::test]
async fn validate_says_what_create_says_when_a_rule_refuses() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);

    let mut bad_cron = create_body("daily-trips", "analytics", "select 1");
    bad_cron.schedule = Some(ScheduleRequest {
        cron: "not a cron".into(),
        timezone: "Europe/Berlin".into(),
    });
    for body in [
        create_body("smoke", "finance", "select 1"),
        create_body("daily-trips", "analytics", "  "),
        bad_cron,
    ] {
        let refusal = create_pipeline(
            Some(&resources),
            None,
            None,
            &create_limiter(),
            &ada,
            body.clone(),
        )
        .await
        .expect_err("create refuses it");
        let answer = validate_pipeline(Some(&resources), None, None, &ada, body)
            .await
            .expect("a refusal is an answer, not an error");
        assert!(!answer.ok);
        assert_eq!(answer.action, None);
        assert!(answer.next_runs.is_empty());
        assert_eq!(
            answer.errors,
            vec![ValidationIssue {
                message: refusal.message().to_string()
            }]
        );
    }
    assert!(resources.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn validate_names_an_update_only_for_a_pipeline_the_caller_may_change() {
    let principals = [
        (
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        ),
        (
            "pipeline-finance",
            service_principal("pipeline-finance", "finance", Some(true)),
        ),
    ];
    let mut legacy = nova_pipeline("legacy", "analytics");
    legacy["metadata"]["labels"]
        .as_object_mut()
        .unwrap()
        .remove(MANAGED_BY_LABEL);
    let resources = memory_with(
        &[
            ("daily-trips", nova_pipeline("daily-trips", "analytics")),
            ("theirs", nova_pipeline("theirs", "finance")),
            ("legacy", legacy),
        ],
        &principals,
    );
    let ada = who("sub-ada", &["analytics"], false);

    let own = validate_pipeline(
        Some(&resources),
        None,
        None,
        &ada,
        create_body("daily-trips", "analytics", "select 2"),
    )
    .await
    .expect("an answer");
    assert!(own.ok);
    assert_eq!(own.action.as_deref(), Some("update"));

    for (name, group, sentence) in [
        (
            "theirs",
            "analytics",
            "a pipeline with this name already exists",
        ),
        (
            "legacy",
            "analytics",
            "this pipeline is managed outside Nova (kubectl or Git); change it there",
        ),
    ] {
        let answer = validate_pipeline(
            Some(&resources),
            None,
            None,
            &ada,
            create_body(name, group, "select 2"),
        )
        .await
        .expect("an answer");
        assert!(!answer.ok, "{name}");
        assert_eq!(answer.errors[0].message, sentence, "{name}");
    }

    // A member of both groups still cannot move a pipeline to the other group.
    let both = who("sub-bob", &["analytics", "finance"], false);
    let moved = validate_pipeline(
        Some(&resources),
        None,
        None,
        &both,
        create_body("theirs", "analytics", "select 2"),
    )
    .await
    .expect("an answer");
    assert!(!moved.ok);
    assert_eq!(
        moved.errors[0].message,
        "the group of an existing pipeline cannot change"
    );
}

#[tokio::test]
async fn validate_lists_what_an_update_would_change() {
    let resources = memory_with(
        &[("daily-trips", nova_pipeline("daily-trips", "analytics"))],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        )],
    );
    let ada = who("sub-ada", &["analytics"], false);
    let tuned = PipelineOptions {
        retries: Some(3),
        ..PipelineOptions::default()
    };

    let same = create_body("daily-trips", "analytics", "select 1");
    let mut unscheduled = same.clone();
    unscheduled.schedule = None;
    let mut retried = same.clone();
    retried.options = Some(tuned.clone());
    let mut everything = create_body("daily-trips", "analytics", "select 2");
    everything.schedule = None;
    everything.options = Some(tuned);
    let mut parametrised = create_body("daily-trips", "analytics", "select '${params.day}'");
    parametrised.params = BTreeMap::from([("day".to_string(), "2026-11-01".to_string())]);

    for (body, expected) in [
        (same, vec![]),
        (
            create_body("daily-trips", "analytics", "select 2"),
            vec!["statement"],
        ),
        (unscheduled, vec!["schedule"]),
        (retried, vec!["options"]),
        (everything, vec!["statement", "schedule", "options"]),
        (parametrised, vec!["statement", "params"]),
    ] {
        let answer = validate_pipeline(Some(&resources), None, None, &ada, body)
            .await
            .expect("an answer");
        assert!(answer.ok, "{answer:?}");
        assert_eq!(answer.action.as_deref(), Some("update"));
        assert_eq!(answer.changes, expected);
    }
    assert_eq!(
        stored_pipeline(&resources, "daily-trips").await.unwrap()["spec"]["tasks"][0]["sql"],
        "select 1",
        "a check writes nothing"
    );

    let new = validate_pipeline(
        Some(&resources),
        None,
        None,
        &ada,
        create_body("other", "analytics", "select 1"),
    )
    .await
    .expect("an answer");
    assert_eq!(new.action.as_deref(), Some("create"));
    assert!(
        new.changes.is_empty(),
        "a create changes nothing that exists"
    );
}

#[tokio::test]
async fn validate_refuses_the_updates_that_update_refuses() {
    let mut notebook = nova_pipeline("report", "analytics");
    notebook["spec"]["tasks"] = json!([{
        "name": "run",
        "kind": "notebook",
        "config": { "notebook": "report.ipynb", "objectVersion": "v1", "params": "{}" }
    }]);
    let mut two_tasks = nova_pipeline("two-tasks", "analytics");
    two_tasks["spec"]["tasks"] = json!([
        { "name": "run", "kind": "sql", "sql": "select 1" },
        { "name": "check", "kind": "sql", "sql": "select 2" }
    ]);
    let resources = memory_with(
        &[("report", notebook), ("two-tasks", two_tasks)],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        )],
    );
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    for name in ["report", "two-tasks"] {
        let refusal = update_pipeline(
            Some(&resources),
            None,
            None,
            None,
            &ada,
            &namespace,
            name,
            update_body("select 2"),
        )
        .await
        .expect_err("update refuses it");
        let answer = validate_pipeline(
            Some(&resources),
            None,
            None,
            &ada,
            create_body(name, "analytics", "select 2"),
        )
        .await
        .expect("a refusal is an answer");
        assert!(!answer.ok, "{name}");
        assert_eq!(answer.action, None, "{name}");
        assert!(answer.changes.is_empty(), "{name}");
        assert_eq!(answer.errors[0].message, refusal.message(), "{name}");
    }
}

#[tokio::test]
async fn validate_answers_an_anonymous_caller_as_create_does() {
    let (store, _store_fake) = crate::secret_store::fake::start().await;
    let (airflow, _airflow_fake) = fake_airflow::start().await;
    let (trino, _fake) = fake_trino::start(None).await;
    let app = router().with_state(test_state_at(None, airflow, store, &trino));
    let body = json!({ "name": "daily-trips", "ownerGroup": "analytics", "sql": "select 1" });

    let mut answers = Vec::new();
    for uri in ["/pipelines", "/pipelines/validate"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store"),
            "{uri}"
        );
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        answers.push((status, bytes));
    }
    assert!(
        answers[0].0.is_client_error() || answers[0].0 == StatusCode::SERVICE_UNAVAILABLE,
        "create refuses an anonymous caller: {}",
        answers[0].0
    );
    assert_eq!(answers[0], answers[1], "validate refuses it the same way");
}

fn analytics_principal() -> [(&'static str, Value); 1] {
    [(
        "pipeline-analytics",
        service_principal("pipeline-analytics", "analytics", Some(true)),
    )]
}

#[tokio::test]
async fn spec_is_the_document_that_validate_accepts_unchanged() {
    let mut stored = nova_pipeline("daily-trips", "analytics");
    stored["spec"]["params"] = json!({ "day": "2026-11-01" });
    stored["spec"]["tasks"][0]["sql"] = json!("select '${params.day}'");
    let resources = memory_with(&[("daily-trips", stored)], &analytics_principal());
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let answer = pipeline_spec(Some(&resources), &ada, &namespace, "daily-trips")
        .await
        .expect("visible to its group");

    assert_eq!(answer.managed_by, "nova");
    assert!(answer.editable);
    assert_eq!(answer.resource_version.as_deref(), Some("7"));
    let document = answer.document;
    assert_eq!(document.name, "daily-trips");
    assert_eq!(document.owner_group.as_deref(), Some("analytics"));
    assert_eq!(document.sql.as_deref(), Some("select '${params.day}'"));
    assert_eq!(document.notebook, None);
    assert_eq!(
        document.schedule,
        Some(ScheduleRequest {
            cron: "0 3 * * *".into(),
            timezone: "Europe/Berlin".into()
        })
    );
    assert_eq!(
        document.params,
        BTreeMap::from([("day".to_string(), "2026-11-01".to_string())])
    );
    assert_eq!(document.options, default_options());

    // Sent back as it is, the document is an update that changes nothing.
    let body = CreatePipelineRequest {
        name: document.name.clone(),
        owner_group: document.owner_group.clone().unwrap(),
        description: document.description.clone(),
        sql: document.sql.clone().unwrap(),
        notebook: None,
        tasks: None,
        schedule: document.schedule.clone(),
        params: document.params.clone(),
        options: Some(PipelineOptions {
            retries: Some(document.options.retries),
            backoff_minutes: Some(document.options.backoff_minutes),
            timeout_minutes: Some(document.options.timeout_minutes),
            concurrency: Some(document.options.concurrency.clone()),
        }),
        provenance: None,
        notify: None,
        viewers: None,
        publish: None,
    };
    let verdict = validate_pipeline(Some(&resources), None, None, &ada, body)
        .await
        .expect("an answer");
    assert!(verdict.ok, "{verdict:?}");
    assert_eq!(verdict.action.as_deref(), Some("update"));
    assert!(verdict.changes.is_empty(), "{:?}", verdict.changes);
}

#[tokio::test]
async fn spec_returns_a_task_list_with_a_dbt_task() {
    let mut stored = nova_pipeline("shop-dbt", "analytics");
    stored["spec"]["tasks"] = json!([
        { "name": "load", "kind": "sql", "sql": "select 1" },
        {
            "name": "build",
            "kind": "dbt",
            "dependsOn": ["load"],
            "config": {
                "project": "shop",
                "batches": "[\"tag:seed\",\"tag:marts\"]",
                "threads": "4",
                "vars": "{\"day\":\"7\"}"
            }
        }
    ]);
    let resources = memory_with(&[("shop-dbt", stored)], &analytics_principal());
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let answer = pipeline_spec(Some(&resources), &ada, &namespace, "shop-dbt")
        .await
        .expect("visible to its group");

    assert!(
        answer.editable,
        "a Nova-written task list is edited whole by the form"
    );
    assert_eq!(
        serde_json::to_value(&answer.document).unwrap()["tasks"],
        json!([
            { "name": "load", "sql": "select 1" },
            {
                "name": "build",
                "dbt": {
                    "project": "shop",
                    "batches": ["tag:seed", "tag:marts"],
                    "threads": 4,
                    "vars": { "day": "7" }
                },
                "dependsOn": ["load"]
            }
        ])
    );
}

#[tokio::test]
async fn spec_marks_a_nova_task_list_editable() {
    let mut stored = nova_pipeline("daily-loads", "analytics");
    stored["spec"]["tasks"] = json!([
        { "name": "load", "kind": "sql", "sql": "select 1" },
        { "name": "report", "kind": "sql", "sql": "select 2", "dependsOn": ["load"] }
    ]);
    let mut outside = stored.clone();
    outside["metadata"]["labels"]
        .as_object_mut()
        .unwrap()
        .remove(MANAGED_BY_LABEL);
    let resources = memory_with(
        &[("daily-loads", stored), ("outside", outside)],
        &analytics_principal(),
    );
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let answer = pipeline_spec(Some(&resources), &ada, &namespace, "daily-loads")
        .await
        .expect("visible to its group");
    assert!(answer.editable, "a Nova-written task list is edited whole");
    assert_eq!(
        serde_json::to_value(&answer.document).unwrap()["tasks"],
        json!([
            { "name": "load", "sql": "select 1" },
            { "name": "report", "sql": "select 2", "dependsOn": ["load"] }
        ])
    );

    let not_nova = pipeline_spec(Some(&resources), &ada, &namespace, "outside")
        .await
        .expect("visible to its group");
    assert!(
        !not_nova.editable,
        "a task list kubectl or GitOps wrote stays read-only"
    );
}

#[tokio::test]
async fn spec_reads_a_notebook_and_a_zone_less_schedule() {
    let mut stored = nova_pipeline("report", "analytics");
    stored["spec"]["tasks"] = json!([{
        "name": "run",
        "kind": "notebook",
        "config": {
            "notebook": "report.ipynb",
            "objectVersion": "v1",
            "params": "{\"days\":\"7\"}"
        }
    }]);
    stored["spec"]["schedule"] = json!({ "cron": "0 6 * * *" });
    let resources = memory_with(&[("report", stored)], &analytics_principal());
    let ada = who("sub-ada", &["analytics"], false);

    let answer = pipeline_spec(
        Some(&resources),
        &ada,
        &crate::cluster::namespace(),
        "report",
    )
    .await
    .expect("visible");

    assert!(answer.editable);
    assert_eq!(answer.document.sql, None);
    assert_eq!(
        answer.document.notebook,
        Some(NotebookRequest {
            name: "report.ipynb".into(),
            object_version: "v1".into(),
            params: BTreeMap::from([("days".to_string(), "7".to_string())]),
            connection: None,
        })
    );
    assert_eq!(
        answer.document.schedule,
        Some(ScheduleRequest {
            cron: "0 6 * * *".into(),
            timezone: "UTC".into()
        }),
        "no zone means UTC"
    );
}

#[tokio::test]
async fn spec_shows_a_pipeline_it_does_not_let_you_edit() {
    let mut legacy = nova_pipeline("legacy", "analytics");
    legacy["metadata"]["labels"]
        .as_object_mut()
        .unwrap()
        .remove(MANAGED_BY_LABEL);
    let mut odd_shape = nova_pipeline("odd", "analytics");
    odd_shape["spec"]["tasks"] = json!([
        { "name": "run", "kind": "sql", "sql": "select 1" },
        { "name": "check", "kind": "python", "config": { "script": "print(1)" } }
    ]);
    let resources = memory_with(
        &[("legacy", legacy), ("odd", odd_shape)],
        &analytics_principal(),
    );
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let outside = pipeline_spec(Some(&resources), &ada, &namespace, "legacy")
        .await
        .expect("readable");
    assert_eq!(outside.managed_by, "other");
    assert!(!outside.editable, "Nova did not write it");
    assert_eq!(outside.document.sql.as_deref(), Some("select 1"));

    let odd = pipeline_spec(Some(&resources), &ada, &namespace, "odd")
        .await
        .expect("readable");
    assert_eq!(odd.managed_by, "nova");
    assert!(
        !odd.editable,
        "the form cannot show a task kind it never writes"
    );
    assert_eq!(odd.document.sql, None, "no statement is claimed for it");
    assert_eq!(odd.document.name, "odd");
}

#[tokio::test]
async fn notebook_usage_lists_only_this_groups_pipelines_for_the_notebook() {
    let notebook = |name: &str, group: &str, file: &str, version: &str| {
        pipeline_resource(
            name,
            group,
            &Statement::Notebook {
                name: file.to_string(),
                object_version: version.to_string(),
                params: Default::default(),
                connection: None,
            },
            Some(&("0 3 * * *".to_string(), "UTC".to_string())),
            &Default::default(),
            &default_options(),
            "ann",
            None,
            &[],
            None,
            None,
        )
    };
    let resources = vec![
        notebook("smoke-daily", "analytics", "smoke.ipynb", "v3"),
        notebook("other-daily", "analytics", "other.ipynb", "v1"),
        notebook("fin-daily", "finance", "smoke.ipynb", "v2"),
    ];
    let ann = who("sub-ann", &["analytics"], false);
    let rows = notebook_usage_rows(&resources, &ann, "analytics", "smoke.ipynb");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "smoke-daily");
    assert_eq!(rows[0].object_version, "v3");
    assert_eq!(rows[0].cron.as_deref(), Some("0 3 * * *"));
    // The finance pipeline runs the same file, but the notebook page is group-scoped.
    assert!(notebook_usage_rows(&resources, &ann, "analytics", "fin-daily").is_empty());
}

#[tokio::test]
async fn spec_of_another_groups_pipeline_is_404_and_needs_no_scheduler() {
    let resources = memory_with(
        &[("daily-trips", nova_pipeline("daily-trips", "analytics"))],
        &analytics_principal(),
    );
    let eve = who("sub-eve", &["finance"], false);
    let error = pipeline_spec(
        Some(&resources),
        &eve,
        &crate::cluster::namespace(),
        "daily-trips",
    )
    .await
    .expect_err("not hers");
    assert_eq!(error.status(), StatusCode::NOT_FOUND);

    let error = pipeline_spec(None, &eve, &crate::cluster::namespace(), "daily-trips")
        .await
        .expect_err("no cluster");
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn spec_route_answers_an_anonymous_caller_as_detail_does() {
    let (store, _store_fake) = crate::secret_store::fake::start().await;
    let (airflow, _airflow_fake) = fake_airflow::start().await;
    let (trino, _fake) = fake_trino::start(None).await;
    let app = router().with_state(test_state_at(None, airflow, store, &trino));

    let mut answers = Vec::new();
    for uri in [
        "/pipelines/stardelt/daily",
        "/pipelines/stardelt/daily/spec",
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store"),
            "{uri}"
        );
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        answers.push((status, bytes));
    }
    assert_ne!(answers[0].0, StatusCode::NOT_FOUND, "detail is routed");
    assert_eq!(
        answers[0], answers[1],
        "the spec route refuses it the same way"
    );
}

#[tokio::test]
async fn validate_without_a_cluster_is_503() {
    let ada = who("sub-ada", &["analytics"], false);
    let error = validate_pipeline(
        None,
        None,
        None,
        &ada,
        create_body("x", "analytics", "select 1"),
    )
    .await
    .expect_err("no cluster, no answer");
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// One rendered Pipeline document in the shape `POST /api/pipelines` takes: what a bundle
/// plan reads.
fn plan_document(name: &str, group: &str, sql: &str, cron: &str) -> Value {
    json!({
        "name": name,
        "ownerGroup": group,
        "sql": sql,
        "schedule": {"cron": cron, "timezone": "Europe/Berlin"},
    })
}

fn plan_request(resources: Vec<Value>) -> crate::deployments::BundlePlanRequest {
    crate::deployments::BundlePlanRequest {
        bundle: "shop-etl".into(),
        owner_group: "analytics".into(),
        target: "prod".into(),
        revision: Some("0123abc".into()),
        resources,
        files: vec![crate::deployments::DeploymentFile {
            path: "pipelines/marts.yaml".into(),
            sha256: "a".repeat(64),
        }],
        prune: false,
    }
}

#[tokio::test]
async fn bundle_plan_answers_create_update_and_a_refusal() {
    let resources = memory_with(
        &[("daily-trips", nova_pipeline("daily-trips", "analytics"))],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        )],
    );
    let ada = who("sub-ada", &["analytics"], false);
    let request = plan_request(vec![
        plan_document("new-marts", "analytics", "select 1", "0 3 * * *"),
        plan_document("bad-cron", "analytics", "select 1", "not a cron"),
        plan_document("daily-trips", "analytics", "select 2", "0 3 * * *"),
    ]);
    let before = resources.list().await.expect("memory list").len();
    let plan = plan_bundle(
        Some(&resources),
        None,
        None,
        &ada,
        None,
        &[],
        request.clone(),
    )
    .await
    .expect("an answer");

    assert!(!plan.ok, "one document is refused");
    assert_eq!(plan.errors.len(), 1, "{:?}", plan.errors);
    assert_eq!(plan.errors[0].message, CRON_FIELDS);
    assert!(
        plan.errors[0].path.starts_with("resources[1]"),
        "the refusal names where it is: {}",
        plan.errors[0].path
    );

    let create = plan
        .changes
        .iter()
        .find(|change| change.name == "new-marts")
        .expect("a create");
    assert_eq!(
        (create.kind.as_str(), create.action.as_str()),
        ("Pipeline", "create")
    );
    assert!(create.diff.is_empty(), "a create changes nothing");
    let update = plan
        .changes
        .iter()
        .find(|change| change.name == "daily-trips")
        .expect("an update");
    assert_eq!(update.action, "update");
    assert!(!update.diff.is_empty(), "an update says what it changes");

    assert_eq!(
        resources.list().await.expect("memory list").len(),
        before,
        "the plan writes nothing"
    );
    assert_eq!(
        plan.plan_id,
        crate::deployments::rendered_set_id(
            &request.bundle,
            &request.target,
            &request.resources,
            &request.files
        ),
        "the planId names the set the plan describes"
    );
    assert_eq!(plan.files.len(), 1);
    assert!(plan.files[0].new, "no deployment was recorded yet");
}

#[tokio::test]
async fn bundle_plan_names_owned_pipelines_the_set_omits() {
    let mut owned = nova_pipeline("daily-trips", "analytics");
    stamp_provenance(&mut owned, &provenance("shop-etl", "prod", None), "ada");
    let resources = memory_with(
        &[("daily-trips", owned)],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        )],
    );
    let ada = who("sub-ada", &["analytics"], false);
    let plan = plan_bundle(
        Some(&resources),
        None,
        None,
        &ada,
        None,
        &[],
        plan_request(vec![plan_document(
            "other-marts",
            "analytics",
            "select 1",
            "0 3 * * *",
        )]),
    )
    .await
    .expect("an answer");

    assert_eq!(plan.warnings.len(), 1, "{:?}", plan.warnings);
    assert!(
        plan.warnings[0].message.contains("daily-trips"),
        "{}",
        plan.warnings[0].message
    );
    assert!(
        plan.changes
            .iter()
            .all(|change| change.name != "daily-trips"),
        "a pipeline the set omits is no change"
    );
}

#[tokio::test]
async fn bundle_plan_with_prune_names_the_deletes() {
    let mut owned = nova_pipeline("daily-trips", "analytics");
    stamp_provenance(&mut owned, &provenance("shop-etl", "prod", None), "ada");
    let resources = memory_with(
        &[("daily-trips", owned)],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        )],
    );
    let ada = who("sub-ada", &["analytics"], false);
    let mut request = plan_request(vec![plan_document(
        "other-marts",
        "analytics",
        "select 1",
        "0 3 * * *",
    )]);
    request.prune = true;
    let plan = plan_bundle(Some(&resources), None, None, &ada, None, &[], request)
        .await
        .expect("an answer");

    assert!(plan.warnings.is_empty(), "{:?}", plan.warnings);
    assert_eq!(plan.changes.len(), 2, "{:?}", plan.changes);
    let create = plan
        .changes
        .iter()
        .find(|change| change.name == "other-marts")
        .expect("a create");
    assert_eq!(create.action, "create");
    let delete = plan
        .changes
        .iter()
        .find(|change| change.name == "daily-trips")
        .expect("a delete");
    assert_eq!(delete.kind, "Pipeline");
    assert_eq!(delete.name, "daily-trips");
    assert_eq!(delete.action, "delete");
    assert!(delete.diff.is_empty(), "a delete carries no diff");
}

#[tokio::test]
async fn bundle_plan_without_a_cluster_is_503() {
    let ada = who("sub-ada", &["analytics"], false);
    let error = plan_bundle(
        None,
        None,
        None,
        &ada,
        None,
        &[],
        plan_request(vec![plan_document(
            "new-marts",
            "analytics",
            "select 1",
            "0 3 * * *",
        )]),
    )
    .await
    .expect_err("no cluster, no plan");
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// One rendered StreamApp document in the shape `PUT /api/streams/{name}` takes.
fn stream_document(name: &str, group: &str) -> Value {
    json!({
        "kind": "StreamApp",
        "name": name,
        "ownerGroup": group,
        "source": {"topic": "events"},
        "sink": {"catalog": "bronze", "table": "events.raw", "mode": "append"},
    })
}

#[tokio::test]
async fn a_plan_accepts_a_stream_document() {
    let resources = memory_with(&[], &[]);
    let ada = who("sub-ada", &["analytics"], false);
    let plan = plan_bundle(
        Some(&resources),
        None,
        None,
        &ada,
        None,
        &[],
        plan_request(vec![stream_document("events", "analytics")]),
    )
    .await
    .expect("an answer");

    assert!(plan.ok, "{:?}", plan.errors);
    assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    assert_eq!(plan.changes.len(), 1, "{:?}", plan.changes);
    let change = &plan.changes[0];
    assert_eq!(change.kind, "StreamApp");
    assert_eq!(change.name, "events");
    assert_eq!(change.action, "create");
}

#[tokio::test]
async fn a_plan_names_an_existing_stream_as_an_update() {
    let resources = memory_with(&[], &[]);
    let ada = who("sub-ada", &["analytics"], false);
    let existing = vec![json!({"metadata": {"name": "events"}})];
    let plan = plan_bundle(
        Some(&resources),
        None,
        None,
        &ada,
        None,
        &existing,
        plan_request(vec![stream_document("events", "analytics")]),
    )
    .await
    .expect("an answer");

    assert!(plan.ok, "{:?}", plan.errors);
    assert_eq!(plan.changes.len(), 1, "{:?}", plan.changes);
    assert_eq!(plan.changes[0].kind, "StreamApp");
    assert_eq!(plan.changes[0].action, "update");
}

#[tokio::test]
async fn a_plan_refuses_a_bad_stream_document() {
    let resources = memory_with(&[], &[]);
    let ada = who("sub-ada", &["analytics"], false);
    let mut document = stream_document("events", "analytics");
    document["sink"]["mode"] = json!("upsert");
    let plan = plan_bundle(
        Some(&resources),
        None,
        None,
        &ada,
        None,
        &[],
        plan_request(vec![document]),
    )
    .await
    .expect("an answer");

    assert!(!plan.ok, "the stream is refused");
    assert_eq!(plan.errors.len(), 1, "{:?}", plan.errors);
    assert!(
        plan.errors[0].message.contains("mode"),
        "{}",
        plan.errors[0].message
    );
}

#[tokio::test]
async fn a_plan_refuses_a_stream_of_another_group() {
    let resources = memory_with(&[], &[]);
    let ada = who("sub-ada", &["analytics"], false);
    let plan = plan_bundle(
        Some(&resources),
        None,
        None,
        &ada,
        None,
        &[],
        plan_request(vec![stream_document("events", "other")]),
    )
    .await
    .expect("an answer");

    assert_eq!(plan.errors.len(), 1, "{:?}", plan.errors);
    let message = &plan.errors[0].message;
    assert!(message.contains("other"), "{message}");
    assert!(message.contains("analytics"), "{message}");
}

#[test]
fn preview_next_runs_table() {
    let now = "2026-10-20T12:00:00Z"
        .parse::<DateTime<Utc>>()
        .expect("a fixed clock");
    let table: [(&str, [&str; 5]); 4] = [
        // Europe/Berlin: the clocks go back on 25 October, so the fifth run is CET.
        (
            "0 3 * * *",
            [
                "2026-10-21T03:00:00+02:00",
                "2026-10-22T03:00:00+02:00",
                "2026-10-23T03:00:00+02:00",
                "2026-10-24T03:00:00+02:00",
                "2026-10-25T03:00:00+01:00",
            ],
        ),
        (
            "*/15 * * * *",
            [
                "2026-10-20T14:15:00+02:00",
                "2026-10-20T14:30:00+02:00",
                "2026-10-20T14:45:00+02:00",
                "2026-10-20T15:00:00+02:00",
                "2026-10-20T15:15:00+02:00",
            ],
        ),
        (
            "0 9 * * 1-5",
            [
                "2026-10-21T09:00:00+02:00",
                "2026-10-22T09:00:00+02:00",
                "2026-10-23T09:00:00+02:00",
                "2026-10-26T09:00:00+01:00",
                "2026-10-27T09:00:00+01:00",
            ],
        ),
        (
            "30 6 1 * *",
            [
                "2026-11-01T06:30:00+01:00",
                "2026-12-01T06:30:00+01:00",
                "2027-01-01T06:30:00+01:00",
                "2027-02-01T06:30:00+01:00",
                "2027-03-01T06:30:00+01:00",
            ],
        ),
    ];

    for (cron, expected) in table {
        let answer = preview_schedule(
            PreviewScheduleRequest {
                cron: cron.into(),
                timezone: "Europe/Berlin".into(),
                count: None,
            },
            now,
        )
        .unwrap_or_else(|error| panic!("{cron}: {}", error.message()));
        assert_eq!(
            answer.runs,
            expected
                .iter()
                .map(|run| (*run).to_string())
                .collect::<Vec<_>>(),
            "{cron}"
        );
    }

    // 02:30 does not exist on 29 March 2026 (the clocks jump 02:00 → 03:00): it is skipped.
    let before_the_jump = "2026-03-28T12:00:00Z"
        .parse::<DateTime<Utc>>()
        .expect("a fixed clock");
    let answer = preview_schedule(
        PreviewScheduleRequest {
            cron: "30 2 * * *".into(),
            timezone: "Europe/Berlin".into(),
            count: Some(3),
        },
        before_the_jump,
    )
    .expect("a valid cron");
    assert_eq!(
        answer.runs,
        vec![
            "2026-03-30T02:30:00+02:00".to_string(),
            "2026-03-31T02:30:00+02:00".to_string(),
            "2026-04-01T02:30:00+02:00".to_string(),
        ]
    );

    // Both day fields restricted: a day matches when either does (croniter's rule).
    let answer = preview_schedule(
        PreviewScheduleRequest {
            cron: "0 4 1 * 1".into(),
            timezone: "Europe/Berlin".into(),
            count: Some(4),
        },
        now,
    )
    .expect("a valid cron");
    assert_eq!(
        answer.runs,
        vec![
            "2026-10-26T04:00:00+01:00".to_string(),
            "2026-11-01T04:00:00+01:00".to_string(),
            "2026-11-02T04:00:00+01:00".to_string(),
            "2026-11-09T04:00:00+01:00".to_string(),
        ]
    );

    // A cron that can never fire answers no runs (at most 366 days are searched), not an error.
    let answer = preview_schedule(
        PreviewScheduleRequest {
            cron: "0 0 31 2 *".into(),
            timezone: "Europe/Berlin".into(),
            count: Some(5),
        },
        now,
    )
    .expect("the cron itself is valid");
    assert!(answer.runs.is_empty(), "31 February never comes");
}

#[test]
fn preview_rejects_what_create_rejects() {
    let now = "2026-10-20T12:00:00Z"
        .parse::<DateTime<Utc>>()
        .expect("a fixed clock");

    for (cron, timezone, sentence) in [
        ("* * * * *", "UTC", CRON_TOO_OFTEN),
        ("0 3 * *", "UTC", CRON_FIELDS),
        ("0 25 * * *", "UTC", CRON_RANGE),
        ("0 3 * * *", "Mars/Olympus", "unknown timezone"),
    ] {
        let error = preview_schedule(
            PreviewScheduleRequest {
                cron: cron.into(),
                timezone: timezone.into(),
                count: None,
            },
            now,
        )
        .unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{cron}",);
        assert_eq!(error.message(), sentence, "{cron}");
    }

    let error = preview_schedule(
        PreviewScheduleRequest {
            cron: "0 3 * * *".into(),
            timezone: "UTC".into(),
            count: Some(11),
        },
        now,
    )
    .unwrap_err();
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.message(), COUNT_RANGE);

    let answer = preview_schedule(
        PreviewScheduleRequest {
            cron: "0 3 * * *".into(),
            timezone: "UTC".into(),
            count: Some(1),
        },
        now,
    )
    .expect("count 1 is fine");
    assert_eq!(answer.runs.len(), 1);
}

#[tokio::test]
async fn check_and_preview_need_a_signed_in_person() {
    let (store, _store_fake) = crate::secret_store::fake::start().await;
    let (airflow, _airflow_fake) = fake_airflow::start().await;
    let (trino, fake) = fake_trino::start(None).await;
    let app = router().with_state(test_state_at(None, airflow, store, &trino));

    for (uri, body) in [
        ("/pipelines/check", json!({ "sql": "select 1" })),
        (
            "/pipelines/preview-schedule",
            json!({ "cron": "0 3 * * *", "timezone": "Europe/Berlin" }),
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{uri}");
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store"),
            "a refusal is never cached either: {uri}"
        );
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["error"], "not signed in", "{uri}");
    }
    assert!(
        fake.lock().expect("fake trino").statements.is_empty(),
        "an anonymous caller never reaches the engine"
    );
}

// -----------------------------------------------------------------------
// Notebook schedules (#237)
// -----------------------------------------------------------------------

/// A create body that schedules `notebook` at `version` for `group`, no statement.
fn notebook_body(name: &str, group: &str, notebook: &str, version: &str) -> CreatePipelineRequest {
    CreatePipelineRequest {
        name: name.into(),
        owner_group: group.into(),
        description: None,
        sql: String::new(),
        notebook: Some(NotebookRequest {
            name: notebook.into(),
            object_version: version.into(),
            params: BTreeMap::new(),
            connection: None,
        }),
        tasks: None,
        schedule: Some(ScheduleRequest {
            cron: "0 3 * * *".into(),
            timezone: "Europe/Berlin".into(),
        }),
        params: BTreeMap::new(),
        options: None,
        provenance: None,
        notify: None,
        viewers: None,
        publish: None,
    }
}

fn update_notebook_body(notebook: &str, version: &str) -> UpdatePipelineRequest {
    UpdatePipelineRequest {
        sql: String::new(),
        description: None,
        notebook: Some(NotebookRequest {
            name: notebook.into(),
            object_version: version.into(),
            params: BTreeMap::new(),
            connection: None,
        }),
        tasks: None,
        schedule: None,
        params: BTreeMap::new(),
        options: None,
        resource_version: None,
        provenance: None,
        notify: None,
        viewers: None,
        publish: None,
    }
}

/// A notebook pipeline Nova wrote, as a `head_version` needs nothing: the resource only.
fn stored_notebook_pipeline(name: &str, group: &str, notebook: &str, version: &str) -> Value {
    let mut resource = pipeline_resource(
        name,
        group,
        &Statement::Notebook {
            name: notebook.into(),
            object_version: version.into(),
            params: "{}".into(),
            connection: None,
        },
        Some(&("0 3 * * *".to_string(), "Europe/Berlin".to_string())),
        &BTreeMap::new(),
        &default_options(),
        "sub-ada",
        None,
        &[],
        None,
        None,
    );
    resource["metadata"]["resourceVersion"] = json!("7");
    resource
}

#[tokio::test]
async fn notebook_request_builds_the_notebook_task() {
    let resources = analytics_ready();
    let s3 = FakeS3::start().await;
    let version = s3.put_raw("analytics/report.ipynb", b"{\"nbformat\":4}", "ada");
    let store = notebook_store(&s3.base);
    let ada = who("sub-ada", &["analytics"], false);

    let mut body = notebook_body("nightly-report", "analytics", "report.ipynb", &version);
    // Deliberately not sorted: the task's `params` must be, whatever the body's order.
    body.notebook.as_mut().expect("notebook").params = BTreeMap::from([
        ("region".to_string(), "eu".to_string()),
        ("days".to_string(), "7".to_string()),
    ]);

    let view = create_pipeline(
        Some(&resources),
        None,
        Some(&store),
        &create_limiter(),
        &ada,
        body,
    )
    .await
    .expect("a notebook schedule");

    assert!(!view.loaded, "the loader has not seen it yet");
    assert_eq!(view.state, "loading");
    assert_eq!(view.tasks.len(), 1);
    assert_eq!(view.tasks[0].kind, "notebook");
    assert_eq!(view.tasks[0].sql, None);

    let stored = stored_pipeline(&resources, "nightly-report")
        .await
        .expect("written");
    assert_eq!(
        stored["spec"],
        json!({
            "ownerGroup": "analytics",
            "runAs": "pipeline-analytics",
            "tasks": [{
                "name": "run",
                "kind": "notebook",
                "config": {
                    "notebook": "report.ipynb",
                    "objectVersion": version,
                    "params": "{\"days\":\"7\",\"region\":\"eu\"}",
                },
            }],
            "concurrency": "Forbid",
            "retries": { "attempts": 1, "backoff": "5m" },
            "timeout": "120m",
            "schedule": { "cron": "0 3 * * *", "timezone": "Europe/Berlin" },
        }),
        "the notebook task exactly: one config object, params as a sorted JSON string, no sql"
    );
    assert!(
        stored["spec"]["tasks"][0].get("sql").is_none(),
        "a notebook task carries no statement: {stored}"
    );
    assert!(
        stored["spec"].get("params").is_none(),
        "the notebook's values live in config.params, not in spec.params: {stored}"
    );
    assert_eq!(
        stored["metadata"]["labels"],
        json!({
            MANAGED_BY_LABEL: MANAGED_BY_NOVA,
            "stardelt.io/owner-group": "analytics",
        }),
        "the label set stays what it was: no kind label is added"
    );
    assert_eq!(
        stored["metadata"]["annotations"]["stardelt.io/created-by"],
        "sub-ada"
    );
}

#[tokio::test]
async fn exactly_one_of_sql_and_notebook() {
    const SENTENCE: &str = ONE_OF_STATEMENT_NOTEBOOK_OR_DBT;
    let resources = analytics_ready();
    let s3 = FakeS3::start().await;
    let version = s3.put_raw("analytics/report.ipynb", b"{}", "ada");
    let store = notebook_store(&s3.base);
    let ada = who("sub-ada", &["analytics"], false);
    let limiter = create_limiter();

    // Neither: no statement and no notebook.
    let mut body = notebook_body("either", "analytics", "report.ipynb", &version);
    body.notebook = None;
    let error = create_pipeline(Some(&resources), None, Some(&store), &limiter, &ada, body)
        .await
        .expect_err("neither");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.message(), SENTENCE);

    // Both: the body would have to choose.
    let mut body = notebook_body("either", "analytics", "report.ipynb", &version);
    body.sql = "select 1".into();
    let error = create_pipeline(Some(&resources), None, Some(&store), &limiter, &ada, body)
        .await
        .expect_err("both");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.message(), SENTENCE);

    // An update says the same: it changes one or the other, never both.
    let resources_with_pipeline = memory_with(
        &[(
            "nightly",
            stored_notebook_pipeline("nightly", "analytics", "report.ipynb", &version),
        )],
        &[(
            "pipeline-analytics",
            service_principal("pipeline-analytics", "analytics", Some(true)),
        )],
    );
    let mut update = update_notebook_body("report.ipynb", &version);
    update.sql = "select 1".into();
    let error = update_pipeline(
        Some(&resources_with_pipeline),
        None,
        Some(&store),
        None,
        &ada,
        &crate::cluster::namespace(),
        "nightly",
        update,
    )
    .await
    .expect_err("an update gives one of them too");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.message(), SENTENCE);

    // A blank statement is no statement: the notebook is what runs.
    let mut body = notebook_body("blank-sql", "analytics", "report.ipynb", &version);
    body.sql = "  \n\t ".into();
    create_pipeline(Some(&resources), None, Some(&store), &limiter, &ada, body)
        .await
        .expect("a blank statement next to a notebook is not a second one");
    let stored = stored_pipeline(&resources, "blank-sql")
        .await
        .expect("written");
    assert_eq!(stored["spec"]["tasks"][0]["kind"], "notebook");
}

/// One refused notebook body, with the sentence the ticket names.
async fn create_refused(
    resources: &PipelineResources,
    store: &NotebookStore,
    limiter: &CreateLimiter,
    who: &Identity,
    notebook: NotebookRequest,
    expected: &str,
) {
    let printed = format!("{notebook:?}");
    let mut body = notebook_body("smoke", "analytics", "report.ipynb", "v1");
    body.notebook = Some(notebook);
    let error = create_pipeline(Some(resources), None, Some(store), limiter, who, body)
        .await
        .expect_err(&printed);
    assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{expected}");
    assert_eq!(error.message(), expected);
}

#[tokio::test]
async fn bad_names_and_params_refused_with_sentences() {
    let resources = analytics_ready();
    let s3 = FakeS3::start().await;
    let version = s3.put_raw("analytics/report.ipynb", b"{}", "ada");
    let store = notebook_store(&s3.base);
    let ada = who("sub-ada", &["analytics"], false);
    let limiter = create_limiter();

    for name in [
        "report",
        "report.txt",
        "-report.ipynb",
        "re/port.ipynb",
        ".ipynb",
    ] {
        create_refused(
            &resources,
            &store,
            &limiter,
            &ada,
            NotebookRequest {
                name: name.into(),
                object_version: version.clone(),
                params: BTreeMap::new(),
                connection: None,
            },
            NOTEBOOK_NAME_RULE,
        )
        .await;
    }
    create_refused(
        &resources,
        &store,
        &limiter,
        &ada,
        NotebookRequest {
            name: format!("{}.ipynb", "a".repeat(129)),
            object_version: version.clone(),
            params: BTreeMap::new(),
            connection: None,
        },
        NOTEBOOK_NAME_RULE,
    )
    .await;

    for name in ["re-gion", "re.gion", "", "1region", "reg ion", "region!"] {
        create_refused(
            &resources,
            &store,
            &limiter,
            &ada,
            NotebookRequest {
                name: "report.ipynb".into(),
                object_version: version.clone(),
                params: BTreeMap::from([(name.to_string(), "eu".to_string())]),
                connection: None,
            },
            NOTEBOOK_PARAM_NAMES,
        )
        .await;
    }

    let too_many: BTreeMap<String, String> = (0..=MAX_NOTEBOOK_PARAMS)
        .map(|i| (format!("p{i}"), "1".to_string()))
        .collect();
    create_refused(
        &resources,
        &store,
        &limiter,
        &ada,
        NotebookRequest {
            name: "report.ipynb".into(),
            object_version: version.clone(),
            params: too_many,
            connection: None,
        },
        NOTEBOOK_TOO_MANY_PARAMS,
    )
    .await;

    create_refused(
        &resources,
        &store,
        &limiter,
        &ada,
        NotebookRequest {
            name: "report.ipynb".into(),
            object_version: version.clone(),
            params: BTreeMap::from([(
                "region".to_string(),
                "x".repeat(MAX_NOTEBOOK_PARAM_VALUE_CHARS + 1),
            )]),
            connection: None,
        },
        NOTEBOOK_PARAM_VALUE,
    )
    .await;
    create_refused(
        &resources,
        &store,
        &limiter,
        &ada,
        NotebookRequest {
            name: "report.ipynb".into(),
            object_version: version.clone(),
            params: BTreeMap::from([("region".to_string(), "eu\0us".to_string())]),
            connection: None,
        },
        NOTEBOOK_PARAM_VALUE,
    )
    .await;

    // Every value fits, the JSON text does not: 32 × 200 characters is over 4096.
    let long: BTreeMap<String, String> = (0..MAX_NOTEBOOK_PARAMS)
        .map(|i| (format!("p{i}"), "y".repeat(200)))
        .collect();
    assert!(
        serde_json::to_string(&long).expect("json").chars().count()
            > MAX_NOTEBOOK_PARAMS_JSON_CHARS
    );
    create_refused(
        &resources,
        &store,
        &limiter,
        &ada,
        NotebookRequest {
            name: "report.ipynb".into(),
            object_version: version.clone(),
            params: long,
            connection: None,
        },
        NOTEBOOK_PARAMS_TOO_LONG,
    )
    .await;

    assert!(
        stored_pipeline(&resources, "smoke").await.is_none(),
        "a refused body writes nothing"
    );
}

#[tokio::test]
async fn missing_version_refused_400() {
    let resources = analytics_ready();
    let s3 = FakeS3::start().await;
    let version = s3.put_raw("analytics/report.ipynb", b"{}", "ada");
    let store = notebook_store(&s3.base);
    let ada = who("sub-ada", &["analytics"], false);
    let limiter = create_limiter();
    let sentence = "that notebook version does not exist in group analytics";

    // A deleted (or never published) version of a notebook that does exist.
    for pinned in ["v99".to_string(), "v1 ".to_string(), "v".repeat(129)] {
        let body = notebook_body("smoke", "analytics", "report.ipynb", &pinned);
        let error = create_pipeline(Some(&resources), None, Some(&store), &limiter, &ada, body)
            .await
            .expect_err("no such version");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{pinned:?}");
        assert_eq!(error.message(), sentence, "{pinned:?}");
    }

    // A notebook nobody published.
    let body = notebook_body("smoke", "analytics", "missing.ipynb", &version);
    let error = create_pipeline(Some(&resources), None, Some(&store), &limiter, &ada, body)
        .await
        .expect_err("no such notebook");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.message(), sentence);

    assert!(
        stored_pipeline(&resources, "smoke").await.is_none(),
        "nothing is pinned to bytes that do not exist"
    );
    assert!(!s3.is_empty());
}

#[tokio::test]
async fn s3_unconfigured_503() {
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    // No `NOVA_S3_*`: Nova cannot prove the version exists, so it schedules nothing.
    let body = notebook_body("smoke", "analytics", "report.ipynb", "v1");
    let error = create_pipeline(Some(&resources), None, None, &create_limiter(), &ada, body)
        .await
        .expect_err("no store");
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.message(), "notebook storage is not configured");
    assert!(stored_pipeline(&resources, "smoke").await.is_none());

    // A statement pipeline needs no store at all: nothing changed for it.
    create_pipeline(
        Some(&resources),
        None,
        None,
        &create_limiter(),
        &ada,
        create_body("statement", "analytics", "select 1"),
    )
    .await
    .expect("a statement schedule still works without S3");
}

#[tokio::test]
async fn foreign_group_403() {
    let resources = analytics_ready();
    let s3 = FakeS3::start().await;
    let version = s3.put_raw("finance/budget.ipynb", b"{}", "fin");
    let store = notebook_store(&s3.base);
    let ada = who("sub-ada", &["analytics"], false);

    // The owner group must be one of the caller's own — an administrator included.
    for caller in [&ada, &admin()] {
        let body = notebook_body("their-budget", "finance", "budget.ipynb", &version);
        let error = create_pipeline(
            Some(&resources),
            None,
            Some(&store),
            &create_limiter(),
            caller,
            body,
        )
        .await
        .expect_err("another group's notebook");
        assert_eq!(error.status(), StatusCode::FORBIDDEN);
        assert_eq!(error.message(), "you are not a member of the group finance");
    }
    assert!(stored_pipeline(&resources, "their-budget").await.is_none());

    // A notebook pipeline of another group is hidden: 404, exactly like an absent one,
    // so a schedule never tells an outsider that the notebook exists.
    let resources = memory_with(
        &[(
            "budget",
            stored_notebook_pipeline("budget", "finance", "budget.ipynb", &version),
        )],
        &[],
    );
    let error = update_pipeline(
        Some(&resources),
        None,
        Some(&store),
        None,
        &ada,
        &crate::cluster::namespace(),
        "budget",
        update_notebook_body("budget.ipynb", &version),
    )
    .await
    .expect_err("hidden");
    assert_eq!(error.status(), StatusCode::NOT_FOUND);
    assert_eq!(error.message(), "no such pipeline");
}

#[tokio::test]
async fn update_changes_version_and_params_not_kind() {
    let s3 = FakeS3::start().await;
    let first = s3.put_raw("analytics/report.ipynb", b"{\"v\":1}", "ada");
    let second = s3.put_raw("analytics/report.ipynb", b"{\"v\":2}", "ada");
    let store = notebook_store(&s3.base);
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let mut body = notebook_body("nightly-report", "analytics", "report.ipynb", &first);
    body.notebook.as_mut().expect("notebook").params =
        BTreeMap::from([("days".to_string(), "1".to_string())]);
    create_pipeline(
        Some(&resources),
        None,
        Some(&store),
        &create_limiter(),
        &ada,
        body,
    )
    .await
    .expect("create");

    // The version and the parameters change; the notebook and the kind stay.
    let mut update = update_notebook_body("report.ipynb", &second);
    update.notebook.as_mut().expect("notebook").params = BTreeMap::from([
        ("region".to_string(), "eu".to_string()),
        ("days".to_string(), "7".to_string()),
    ]);
    update.schedule = Some(ScheduleRequest {
        cron: "30 6 * * mon-fri".into(),
        timezone: "UTC".into(),
    });
    let view = update_pipeline(
        Some(&resources),
        None,
        Some(&store),
        None,
        &ada,
        &namespace,
        "nightly-report",
        update,
    )
    .await
    .expect("the update succeeds");
    assert_eq!(view.schedule.as_deref(), Some("30 6 * * mon-fri (UTC)"));

    let stored = stored_pipeline(&resources, "nightly-report")
        .await
        .expect("written");
    assert_eq!(
        stored["spec"]["tasks"][0],
        json!({
            "name": "run",
            "kind": "notebook",
            "config": {
                "notebook": "report.ipynb",
                "objectVersion": second,
                "params": "{\"days\":\"7\",\"region\":\"eu\"}",
            },
        }),
        "the whole notebook state is written, and the kind does not change"
    );
    assert!(
        stored["spec"]["tasks"][0].get("sql").is_none(),
        "a notebook pipeline never grows a statement: {stored}"
    );

    // `sql` on a notebook pipeline: the schema would have to change, so it is refused.
    let error = update_pipeline(
        Some(&resources),
        None,
        Some(&store),
        None,
        &ada,
        &namespace,
        "nightly-report",
        update_body("select 2"),
    )
    .await
    .expect_err("a notebook pipeline runs a notebook");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.message(), NOTEBOOK_PIPELINE);
    assert_eq!(
        stored_pipeline(&resources, "nightly-report")
            .await
            .expect("unchanged")["spec"]["tasks"][0]["config"]["objectVersion"],
        second,
        "a refused update changes nothing"
    );

    // And a statement pipeline does not turn into a notebook one.
    let mut statement = create_body("nightly-sql", "analytics", "select 1");
    statement.schedule = None;
    create_pipeline(
        Some(&resources),
        None,
        Some(&store),
        &create_limiter(),
        &ada,
        statement,
    )
    .await
    .expect("create a statement pipeline");
    let error = update_pipeline(
        Some(&resources),
        None,
        Some(&store),
        None,
        &ada,
        &namespace,
        "nightly-sql",
        update_notebook_body("report.ipynb", &second),
    )
    .await
    .expect_err("a statement pipeline runs a statement");
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.message(), STATEMENT_PIPELINE);
    assert_eq!(
        stored_pipeline(&resources, "nightly-sql")
            .await
            .expect("unchanged")["spec"]["tasks"][0]["sql"],
        "select 1"
    );
}

#[tokio::test]
async fn detail_returns_notebook_fields() {
    let version = "v-7f3a";
    let resources = memory_with(
        &[(
            "nightly",
            stored_notebook_pipeline("nightly", "analytics", "report.ipynb", version),
        )],
        &[],
    );
    let (airflow, _fake) = fake_airflow::start().await;
    let ada = who("sub-ada", &["analytics"], false);
    let namespace = crate::cluster::namespace();

    let detail = pipeline_detail(
        Some(&resources),
        Some(&airflow),
        &ada,
        &namespace,
        "nightly",
    )
    .await
    .expect("the detail is readable");
    assert_eq!(
        detail.pipeline.tasks,
        vec![TaskSpecView {
            name: "run".into(),
            kind: "notebook".into(),
            sql: None,
            notebook: Some("report.ipynb".into()),
            object_version: Some(version.into()),
            notebook_params: Some(BTreeMap::new()),
            connection: None,
            dbt_project: None,
            dbt_select: None,
            project: None,
            refresh: None,
            full_refresh: None,
            writes: vec![],
            depends_on: vec![],
        }],
        "the edit dialog gets the version and the parameters it must prefill"
    );

    // The list stays small: the fields are the detail's, like `sql` since #218.
    let list = list_pipelines(Some(&resources), Some(&airflow), &ada)
        .await
        .expect("the list succeeds");
    assert_eq!(
        list.pipelines[0].tasks,
        vec![TaskSpecView {
            name: "run".into(),
            kind: "notebook".into(),
            sql: None,
            notebook: None,
            object_version: None,
            notebook_params: None,
            connection: None,
            dbt_project: None,
            dbt_select: None,
            project: None,
            refresh: None,
            full_refresh: None,
            writes: vec![],
            depends_on: vec![],
        }]
    );

    // A statement pipeline keeps carrying its `sql` and none of the notebook fields.
    let resources = memory(&[("smoke", nova_pipeline("smoke", "analytics"))]);
    let detail = pipeline_detail(Some(&resources), Some(&airflow), &ada, &namespace, "smoke")
        .await
        .expect("the detail is readable");
    assert_eq!(
        detail.pipeline.tasks,
        vec![TaskSpecView {
            name: "run".into(),
            kind: "sql".into(),
            sql: Some("select 1".into()),
            notebook: None,
            object_version: None,
            notebook_params: None,
            connection: None,
            dbt_project: None,
            dbt_select: None,
            project: None,
            refresh: None,
            full_refresh: None,
            writes: vec![],
            depends_on: vec![],
        }]
    );
}

#[test]
fn task_specs_carry_the_dependencies() {
    let spec = json!({
        "tasks": [
            { "name": "load", "kind": "sql", "sql": "insert into silver.orders select 1" },
            { "name": "check", "kind": "sql",
              "sql": "select count(*) from silver.orders", "dependsOn": ["load"] }
        ]
    });
    let tasks = task_specs(&spec, false);
    assert_eq!(tasks.len(), 2);
    assert!(tasks[0].depends_on.is_empty(), "load waits for nothing");
    assert_eq!(tasks[1].depends_on, vec!["load".to_string()]);
}

#[tokio::test]
async fn audit_has_no_param_values() {
    const CANARY: &str = "CANARY-param-4b1d";
    const SUB: &str = "sub-audit-237";
    let s3 = FakeS3::start().await;
    let version = s3.put_raw("analytics/report.ipynb", b"{}", "ada");
    let store = notebook_store(&s3.base);
    let resources = analytics_ready();
    let ada = who(SUB, &["analytics"], false);
    let namespace = crate::cluster::namespace();
    let _ = audit::test_sink::take_for(SUB);

    let mut body = notebook_body("nightly-report", "analytics", "report.ipynb", &version);
    body.notebook.as_mut().expect("notebook").params = BTreeMap::from([
        ("region".to_string(), CANARY.to_string()),
        ("days".to_string(), "7".to_string()),
    ]);
    create_pipeline(
        Some(&resources),
        None,
        Some(&store),
        &create_limiter(),
        &ada,
        body,
    )
    .await
    .expect("create");

    let mut update = update_notebook_body("report.ipynb", &version);
    update.notebook.as_mut().expect("notebook").params =
        BTreeMap::from([("region".to_string(), CANARY.to_string())]);
    update_pipeline(
        Some(&resources),
        None,
        Some(&store),
        None,
        &ada,
        &namespace,
        "nightly-report",
        update,
    )
    .await
    .expect("update");

    let records = audit::test_sink::take_for(SUB);
    assert_eq!(
        records
            .iter()
            .map(|record| record.action.as_str())
            .collect::<Vec<_>>(),
        ["create", "update"]
    );
    for record in &records {
        assert_eq!(record.kind, "notebook", "the audit says what it runs");
        assert_eq!(record.notebook_group.as_deref(), Some("analytics"));
        assert_eq!(record.notebook.as_deref(), Some("report.ipynb"));
        assert_eq!(record.version.as_deref(), Some(version.as_str()));
        assert_eq!(
            record.statement_chars,
            Some(0),
            "a notebook has no statement to measure"
        );
    }
    assert_eq!(records[0].schedule.as_deref(), Some("0 3 * * *"));
    assert_eq!(
        records[1].schedule, None,
        "the update's body carries no schedule, so it removes it"
    );
    let serialized = format!("{records:?}");
    assert!(
        !serialized.contains(CANARY),
        "no parameter value is ever audited: {serialized}"
    );
    assert!(
        !serialized.contains("region"),
        "not even a parameter name: {serialized}"
    );
    let stored = stored_pipeline(&resources, "nightly-report")
        .await
        .expect("written");
    assert!(
        stored["spec"]["tasks"][0]["config"]["params"]
            .as_str()
            .expect("params")
            .contains(CANARY),
        "the value is in the resource, where papermill reads it"
    );
}

/// `spec.notify` is written under the CRD's keys and read back as the document's shape
/// (nova#815 decision 1); a pipeline without one carries no key.
#[test]
fn pipeline_notify_is_written_and_read_back() {
    let notify = PipelineNotify {
        on_failure: vec!["pager".into()],
        ..PipelineNotify::default()
    };
    let resource = pipeline_resource(
        "daily-trips",
        "analytics",
        &Statement::Sql("select 1".into()),
        None,
        &BTreeMap::new(),
        &default_options(),
        "sub-ada",
        Some(&notify),
        &[],
        None,
        None,
    );
    assert_eq!(spec_of(&resource)["notify"]["onFailure"], json!(["pager"]));

    let document = spec_response(&resource).document;
    assert_eq!(document.notify, Some(notify));
    let printed = serde_json::to_value(&document).expect("the document serialises");
    assert_eq!(printed["notify"]["onFailure"], json!(["pager"]));

    let plain = nova_pipeline("no-notify", "analytics");
    let bare = serde_json::to_value(spec_response(&plain).document).expect("serialises");
    assert!(
        bare.get("notify").is_none(),
        "a pipeline without notify carries no key: {bare}"
    );
    assert_eq!(notify_of(spec_of(&plain)), None);
}

/// The operator's sentences, word for word, in its order (`crd.rs`,
/// `PipelineSpec::validate`).
#[test]
fn pipeline_notify_refuses_bad_names() {
    let six = PipelineNotify {
        on_success: ["a", "b", "c", "d", "e", "f"].map(str::to_string).to_vec(),
        ..PipelineNotify::default()
    };
    assert_eq!(
        six.check_shape().expect_err("six names"),
        "notify.onSuccess names at most 5 destinations"
    );

    let padded = PipelineNotify {
        on_failure: vec![" pager".into()],
        ..PipelineNotify::default()
    };
    assert_eq!(
        padded.check_shape().expect_err("a padded name"),
        "notify.onFailure holds ` pager`, which has leading or trailing whitespace"
    );

    let blank = PipelineNotify {
        on_recovered: vec!["   ".into()],
        ..PipelineNotify::default()
    };
    assert_eq!(
        blank.check_shape().expect_err("a blank name"),
        "notify.onRecovered holds `   `, which is not a destination name (1 to 64 characters)"
    );

    let long = "x".repeat(65);
    let too_long = PipelineNotify {
        on_failure: vec![long.clone()],
        ..PipelineNotify::default()
    };
    assert_eq!(
        too_long.check_shape().expect_err("too long"),
        format!(
            "notify.onFailure holds `{long}`, which is not a destination name (1 to 64 characters)"
        )
    );

    let twice = PipelineNotify {
        on_success: vec!["pager".into(), "pager".into()],
        ..PipelineNotify::default()
    };
    assert_eq!(
        twice.check_shape().expect_err("a duplicate"),
        "notify.onSuccess names `pager` twice"
    );

    assert_eq!(PipelineNotify::default().check_shape(), Ok(()));
}

/// `None` leaves the stored notifications alone, an all-empty `Some` removes them, a filled
/// `Some` replaces them — a form save must not wipe what a bundle deployed.
#[test]
fn an_update_without_notify_keeps_it_an_empty_one_removes_it() {
    let stored = pipeline_resource(
        "daily-trips",
        "analytics",
        &Statement::Sql("select 1".into()),
        None,
        &BTreeMap::new(),
        &default_options(),
        "sub-ada",
        Some(&PipelineNotify {
            on_failure: vec!["pager".into()],
            ..PipelineNotify::default()
        }),
        &[],
        None,
        None,
    );
    let statement = Statement::Sql("select 2".into());

    let kept = updated_resource(
        &stored,
        &statement,
        &default_options(),
        &BTreeMap::new(),
        None,
        None,
        None,
        None,
        None,
    );
    assert_eq!(
        spec_of(&kept)["tasks"][0]["sql"],
        json!("select 2"),
        "the update itself is written"
    );
    assert_eq!(spec_of(&kept)["notify"]["onFailure"], json!(["pager"]));

    let removed = updated_resource(
        &stored,
        &statement,
        &default_options(),
        &BTreeMap::new(),
        None,
        Some(&PipelineNotify::default()),
        None,
        None,
        None,
    );
    assert!(
        spec_of(&removed).get("notify").is_none(),
        "an all-empty notify removes the key: {}",
        spec_of(&removed)
    );

    let replaced = updated_resource(
        &stored,
        &statement,
        &default_options(),
        &BTreeMap::new(),
        None,
        Some(&PipelineNotify {
            on_success: vec!["digest".into()],
            ..PipelineNotify::default()
        }),
        None,
        None,
        None,
    );
    assert_eq!(
        spec_of(&replaced)["notify"],
        json!({ "onSuccess": ["digest"], "onFailure": [], "onRecovered": [] })
    );
}

/// A create body's `viewers` is written as `spec.viewers`; an absent or empty list writes no
/// key (#79 point 6).
#[test]
fn a_create_body_with_viewers_writes_them() {
    let statement = Statement::Sql("select 1".into());
    let write = |viewers: &[String]| {
        pipeline_resource(
            "daily-trips",
            "analytics",
            &statement,
            None,
            &BTreeMap::new(),
            &default_options(),
            "sub-ada",
            None,
            viewers,
            None,
            None,
        )
    };

    let with = write(&["visitors".to_string()]);
    assert_eq!(spec_of(&with)["viewers"], json!(["visitors"]));

    let without = write(&[]);
    assert!(
        spec_of(&without).get("viewers").is_none(),
        "an empty list writes no key: {without}"
    );
}

/// `None` leaves the stored viewers alone, an empty `Some` removes them, a filled `Some`
/// replaces them — a form save must not wipe what a bundle deployed (#79 point 6).
#[test]
fn an_update_writes_viewers_only_when_given() {
    let stored = pipeline_resource(
        "daily-trips",
        "analytics",
        &Statement::Sql("select 1".into()),
        None,
        &BTreeMap::new(),
        &default_options(),
        "sub-ada",
        None,
        &["visitors".to_string()],
        None,
        None,
    );
    let statement = Statement::Sql("select 2".into());
    let update = |viewers: Option<&[String]>| {
        updated_resource(
            &stored,
            &statement,
            &default_options(),
            &BTreeMap::new(),
            None,
            None,
            viewers,
            None,
            None,
        )
    };

    assert_eq!(
        spec_of(&update(Some(&["readers".to_string()])))["viewers"],
        json!(["readers"]),
        "the body's list replaces the stored one"
    );
    assert!(
        spec_of(&update(None)).get("viewers").is_some(),
        "an omitted list keeps the stored viewers"
    );
    assert_eq!(spec_of(&update(None))["viewers"], json!(["visitors"]));
    assert!(
        spec_of(&update(Some(&[]))).get("viewers").is_none(),
        "an empty list removes the key"
    );
}

/// The spec document carries `spec.viewers`, so `stardelt apply --export` re-applies them.
#[test]
fn a_spec_document_carries_the_viewers() {
    let with = with_viewers(
        resource("smoke", Some("analytics")),
        &["visitors", "readers"],
    );
    assert_eq!(
        spec_response(&with).document.viewers,
        vec!["visitors".to_string(), "readers".to_string()]
    );

    let without = spec_response(&resource("smoke", Some("analytics")));
    assert!(
        without.document.viewers.is_empty(),
        "a resource without viewers answers an empty list"
    );
}

/// A viewer that is not a group name, and more than 16 of them, are refused with the CRD's
/// own rule (#79 point 6).
#[test]
fn a_viewer_that_is_not_a_group_name_is_refused() {
    assert_eq!(check_viewers(None), Ok(()));
    assert_eq!(check_viewers(Some(&[])), Ok(()));
    assert_eq!(
        check_viewers(Some(&["visitors".to_string()])),
        Ok(()),
        "a group name passes"
    );
    assert_eq!(
        check_viewers(Some(&["visitors".to_string(), "not a group".to_string()])),
        Err("viewers holds `not a group`, which is not a group name".to_string())
    );

    let seventeen: Vec<String> = (0..17).map(|n| format!("group-{n}")).collect();
    assert_eq!(
        check_viewers(Some(&seventeen)),
        Err("viewers holds at most 16 groups".to_string())
    );
    let sixteen = &seventeen[..16];
    assert_eq!(check_viewers(Some(sixteen)), Ok(()));
}

/// A pipeline created with `viewers` is in the list of a person of that group (#79 point 6).
#[tokio::test]
async fn a_created_pipeline_with_viewers_is_visible_to_one() {
    let (store, _store_fake) = crate::secret_store::fake::start().await;
    let (airflow, _airflow_fake) = fake_airflow::start().await;
    let kube = fake_kube::start_with(
        vec![],
        vec![service_principal(
            "pipeline-analytics",
            "analytics",
            Some(true),
        )],
    )
    .await;
    let state = test_state(Some(kube), airflow, store);
    let creator = router()
        .with_state(Arc::clone(&state))
        .layer(Extension(principal(who("sub-ada", &["analytics"], false))));
    let visitor = router().with_state(state).layer(Extension(principal(who(
        "sub-visitor",
        &["visitors"],
        false,
    ))));

    let response = creator
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/pipelines")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "name": "daily-trips",
                        "ownerGroup": "analytics",
                        "sql": "select 1",
                        "schedule": { "cron": "0 3 * * *", "timezone": "UTC" },
                        "viewers": ["visitors"],
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = visitor
        .oneshot(
            Request::builder()
                .uri("/pipelines")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        body["pipelines"][0]["name"], "daily-trips",
        "a viewer of the pipeline sees it in the list: {body}"
    );
}

/// The group check reads the store: a name of another group is a 400 with the sentence, and
/// no store at all is a 503.
#[tokio::test]
#[ignore = "needs NOVA_TEST_DATABASE_URL"]
async fn the_notify_check_reads_the_group() {
    let pool = crate::db::test_support::migrated_pool().await;
    sqlx::query(
            "INSERT INTO destination (owner_group, name, kind, config, created_at, updated_at) \
             VALUES ('A', 'pager', 'email', '{\"recipients\": [\"oncall@example.com\"]}'::jsonb, 0, 0)",
        )
        .execute(&pool)
        .await
        .expect("the destination is stored");
    let notify = PipelineNotify {
        on_failure: vec!["pager".into()],
        ..PipelineNotify::default()
    };
    assert!(
        check_notify(Some(&pool), "A", Some(&notify)).await.is_ok(),
        "group A owns pager"
    );
    let other = check_notify(Some(&pool), "B", Some(&notify))
        .await
        .expect_err("group B does not own pager");
    assert_eq!(other.status(), StatusCode::BAD_REQUEST);
    assert_eq!(other.message(), "there is no destination named pager in B");
    let no_store = check_notify(None, "A", Some(&notify))
        .await
        .expect_err("no state store");
    assert_eq!(no_store.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        no_store.message(),
        "this needs Nova's database, and this installation has none; ask your administrator"
    );
}

/// The same rule through the two routes: a create is a 400 with the sentence, and the
/// validate route answers it as `ok: false`, 200 (nova#815 slice b acceptance).
#[tokio::test]
#[ignore = "needs NOVA_TEST_DATABASE_URL"]
async fn a_foreign_destination_is_refused_by_create_and_validate() {
    let pool = crate::db::test_support::migrated_pool().await;
    sqlx::query(
            "INSERT INTO destination (owner_group, name, kind, config, created_at, updated_at) \
             VALUES ('finance', 'ledger', 'email', '{\"recipients\": [\"oncall@example.com\"]}'::jsonb, 0, 0)",
        )
        .execute(&pool)
        .await
        .expect("the destination is stored");
    let resources = analytics_ready();
    let ada = who("sub-ada", &["analytics"], false);
    let mut body = create_body("daily-trips", "analytics", "select 1");
    body.notify = Some(PipelineNotify {
        on_failure: vec!["ledger".into()],
        ..PipelineNotify::default()
    });

    let refusal = create_pipeline(
        Some(&resources),
        Some(&pool),
        None,
        &create_limiter(),
        &ada,
        body.clone(),
    )
    .await
    .expect_err("finance's destination is not analytics'");
    assert_eq!(refusal.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        refusal.message(),
        "there is no destination named ledger in analytics"
    );

    let answer = validate_pipeline(Some(&resources), Some(&pool), None, &ada, body)
        .await
        .expect("a refusal is an answer, not an error");
    assert!(!answer.ok);
    assert_eq!(answer.action, None);
    assert_eq!(
        answer.errors,
        vec![ValidationIssue {
            message: "there is no destination named ledger in analytics".into()
        }]
    );
    assert!(resources.list().await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "needs NOVA_TEST_DATABASE_URL"]
async fn the_run_queries_read_the_tags_the_loader_sends() {
    let pool = crate::db::test_support::migrated_pool().await;
    // Two statements of the run, one statement of the same run id in another pipeline,
    // and one statement of another run of this pipeline: only the first two may answer.
    let seed = [
        (
            "rq-1",
            "{stardelt-pipeline,pipeline=stardelt/mobility-medallion,task=gold_kpis,run=scheduled__2026-11-14T03:00:00+00:00}",
            "select 'a' from t",
            6000_i64,
        ),
        (
            "rq-2",
            "{stardelt-pipeline,pipeline=stardelt/mobility-medallion,task=quality_gate,run=scheduled__2026-11-14T03:00:00+00:00}",
            "select 'b' from t",
            700_i64,
        ),
        (
            "rq-3",
            "{stardelt-pipeline,pipeline=stardelt/other,task=gold_kpis,run=scheduled__2026-11-14T03:00:00+00:00}",
            "select 'c' from t",
            9999_i64,
        ),
        (
            "rq-4",
            "{stardelt-pipeline,pipeline=stardelt/mobility-medallion,task=gold_kpis,run=scheduled__2026-11-13T03:00:00+00:00}",
            "select 'd' from t",
            9999_i64,
        ),
    ];
    for (id, tags, sql, wall) in seed {
        sqlx::query(
                "INSERT INTO query_log (query_id, state, principal, client_tags, sql_text, sql_hash, created_at, started_at, wall_ms) \
                 VALUES ($1, 'FINISHED', 'svc-pipeline', $2::text[], $3, $4, now(), now(), $5)",
            )
            .bind(id).bind(tags).bind(sql).bind(format!("h-{id}")).bind(wall)
            .execute(&pool).await.unwrap();
    }
    let queries = run_queries_for(
        &pool,
        "stardelt",
        "mobility-medallion",
        "scheduled__2026-11-14T03:00:00+00:00",
        false,
    )
    .await
    .unwrap();
    let ids: Vec<&str> = queries.iter().map(|q| q.id.as_str()).collect();
    assert_eq!(ids, ["rq-1", "rq-2"]); // slowest first; other pipeline and other run excluded
    assert_eq!(queries[0].task_id.as_deref(), Some("gold_kpis"));
    let redacted = run_queries_for(
        &pool,
        "stardelt",
        "mobility-medallion",
        "scheduled__2026-11-14T03:00:00+00:00",
        true,
    )
    .await
    .unwrap();
    assert!(
        !redacted[0].sql.contains("'a'"),
        "the literal is redacted: {}",
        redacted[0].sql
    );
}

/// A rollback replays a stored document: the first pass creates, a second pass of the same
/// document updates, and the store still holds one pipeline.
#[tokio::test]
async fn apply_document_creates_and_then_updates() {
    // `analytics_ready()` is `memory(&[])` plus the group's ready principal: a create refuses
    // a group whose `pipeline-<group>` principal is missing (409), so the fixture needs it.
    let resources = analytics_ready();
    let limiter = create_limiter();
    let ada = who("sub-ada", &["analytics"], false);
    let document = json!({"name": "daily", "ownerGroup": "analytics", "sql": "select 1"});

    let action = apply_document(
        Some(&resources),
        None,
        None,
        None,
        &limiter,
        &ada,
        &document,
    )
    .await
    .expect("the first pass creates the pipeline");
    assert_eq!(action, "created");

    let action = apply_document(
        Some(&resources),
        None,
        None,
        None,
        &limiter,
        &ada,
        &document,
    )
    .await
    .expect("the second pass updates it");
    assert_eq!(action, "updated");

    assert_eq!(
        resources.list().await.expect("memory list").len(),
        1,
        "one pipeline, not two"
    );
}
