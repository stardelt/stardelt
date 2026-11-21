//! The graph of a `spark-pipeline` task as a ConfigMap (#80 S2c).
//!
//! The run pod of a `spark-pipeline` task cannot read a `Pipeline` object; spike #894 measured
//! that a read-only ConfigMap mount carries the graph (`items[].path` maps `sql/mv_one.sql` back
//! to a nested path, which a ConfigMap key cannot hold). One ConfigMap per `spark-pipeline` task,
//! named `spark-project-<pipeline>-<task>`, with `files` as its `data`; the pod mounts it at
//! `/opt/spark-project`. A task without files, and every other kind, renders nothing, so a
//! `Pipeline` written before S2b reconciles to silence.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    Api, Resource, ResourceExt,
    api::{DeleteParams, ListParams, Patch, PatchParams, PostParams},
    runtime::controller::Action,
};
use sha2::{Digest, Sha256};

use crate::{
    controller::Context,
    crd::{Pipeline, PipelineTask, TaskKind},
};

/// The name prefix every rendered graph ConfigMap carries.
pub const SPARK_PROJECT_PREFIX: &str = "spark-project-";
/// The label that names the `Pipeline` a graph ConfigMap belongs to.
pub const PROJECT_LABEL: &str = "stardelt.io/spark-project";
/// The label that names the task a graph ConfigMap belongs to.
pub const TASK_LABEL: &str = "stardelt.io/spark-task";
/// The annotation holding the graph's digest, so a reader can tell two graphs apart.
pub const DIGEST_ANNOTATION: &str = "stardelt.io/graph-digest";

/// The ConfigMap a task's graph lives in: `spark-project-<pipeline>-<task>`.
pub fn config_map_name(pipeline: &str, task: &str) -> String {
    format!("{SPARK_PROJECT_PREFIX}{pipeline}-{task}")
}

/// Hex sha256 over `path\0content\0` in `BTreeMap` order: stable across builds, unlike a
/// `DefaultHasher`.
pub fn graph_digest(files: &BTreeMap<String, String>) -> String {
    let mut hasher = Sha256::new();
    for (path, content) in files {
        hasher.update(path.as_bytes());
        hasher.update([0u8]);
        hasher.update(content.as_bytes());
        hasher.update([0u8]);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The ConfigMap a task wants, or `None` when it carries no graph: not a `spark-pipeline` task,
/// or a `spark-pipeline` task whose `files` is empty.
pub fn config_map_for(pipeline: &Pipeline, task: &PipelineTask) -> Option<ConfigMap> {
    if task.kind != TaskKind::SparkPipeline || task.files.is_empty() {
        return None;
    }
    let pipeline_name = pipeline.name_any();
    let name = config_map_name(&pipeline_name, &task.name);
    let labels = BTreeMap::from([
        (
            "app.kubernetes.io/managed-by".to_string(),
            "stardelt-operator".to_string(),
        ),
        (PROJECT_LABEL.to_string(), pipeline_name),
        (TASK_LABEL.to_string(), task.name.clone()),
    ]);
    let annotations = BTreeMap::from([(DIGEST_ANNOTATION.to_string(), graph_digest(&task.files))]);
    Some(ConfigMap {
        metadata: kube::core::ObjectMeta {
            name: Some(name),
            namespace: pipeline.namespace(),
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: pipeline.controller_owner_ref(&()).map(|owner| vec![owner]),
            ..Default::default()
        },
        data: Some(task.files.clone()),
        ..Default::default()
    })
}

/// Whether the live ConfigMap differs from the wanted one. Only `data` and the three labels
/// count (#274 rule 1: a quiet reconcile writes nothing).
fn needs_write(existing: &ConfigMap, wanted: &ConfigMap) -> bool {
    if existing.data != wanted.data {
        return true;
    }
    ["app.kubernetes.io/managed-by", PROJECT_LABEL, TASK_LABEL]
        .iter()
        .any(|key| existing.labels().get(*key) != wanted.labels().get(*key))
}

/// The `Pipeline` controller: render one graph ConfigMap per `spark-pipeline` task, converge a
/// changed graph, and delete the ones a task list no longer wants.
pub async fn reconcile_spark_project(
    pipeline: Arc<Pipeline>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    let namespace = pipeline.namespace().unwrap_or_default();
    let name = pipeline.name_any();
    let api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace);

    let wanted: BTreeMap<String, ConfigMap> = pipeline
        .spec
        .tasks
        .iter()
        .filter_map(|task| config_map_for(&pipeline, task))
        .map(|config_map| (config_map.name_any(), config_map))
        .collect();

    for (config_map_name, wanted_map) in &wanted {
        let graphs = wanted_map.data.as_ref().map_or(0, BTreeMap::len);
        match api.get_opt(config_map_name).await? {
            None => {
                api.create(&PostParams::default(), wanted_map).await?;
                tracing::info!(
                    component = "spark-project",
                    config_map = %config_map_name,
                    graphs,
                    "created"
                );
            }
            Some(existing) => {
                if !needs_write(&existing, wanted_map) {
                    continue;
                }
                api.patch(
                    config_map_name,
                    &PatchParams::apply("stardelt-operator").force(),
                    &Patch::Apply(wanted_map),
                )
                .await?;
                tracing::info!(
                    component = "spark-project",
                    config_map = %config_map_name,
                    graphs,
                    "updated"
                );
            }
        }
    }

    // A task removed, or its `files` emptied while the Pipeline lives: the owner reference only
    // covers a deleted Pipeline.
    let selector = format!("{PROJECT_LABEL}={name}");
    let listed = api.list(&ListParams::default().labels(&selector)).await?;
    for existing in listed.items {
        let existing_name = existing.name_any();
        if wanted.contains_key(&existing_name) {
            continue;
        }
        api.delete(&existing_name, &DeleteParams::default()).await?;
        tracing::info!(
            component = "spark-project",
            config_map = %existing_name,
            "deleted"
        );
    }

    Ok(Action::requeue(Duration::from_secs(3600)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Mutex;

    use http_body_util::BodyExt;
    use serde_json::{Value, json};

    type Recorded = Arc<Mutex<Vec<(String, String, String)>>>;

    fn mock_client<F>(handler: F) -> (kube::Client, Recorded)
    where
        F: Fn(&str, &str, &str) -> (u16, String) + Send + Sync + 'static,
    {
        let requests: Recorded = Arc::default();
        let recorder = requests.clone();
        let handler = Arc::new(handler);
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let recorder = recorder.clone();
            let handler = handler.clone();
            let method = request.method().to_string();
            let path = request.uri().path().to_string();
            async move {
                let body_bytes = request
                    .into_body()
                    .collect()
                    .await
                    .map(|body| body.to_bytes().to_vec())
                    .unwrap_or_default();
                let body = String::from_utf8_lossy(&body_bytes).to_string();
                recorder.lock().expect("requests poisoned").push((
                    method.clone(),
                    path.clone(),
                    body.clone(),
                ));
                let (status, document) = handler(&method, &path, &body);
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(kube::client::Body::from(document.into_bytes()))
                        .expect("valid mock response"),
                )
            }
        });
        (kube::Client::new(service, "stardelt"), requests)
    }

    const NOT_FOUND: &str = r#"{"kind":"Status","apiVersion":"v1","status":"Failure","message":"not found","reason":"NotFound","code":404}"#;

    fn context(client: kube::Client) -> Arc<Context> {
        Arc::new(Context::new(
            client,
            Arc::new(crate::helm::testing::RecordingRunner::default()),
        ))
    }

    fn pipeline(tasks: Value) -> Pipeline {
        serde_json::from_value(json!({
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Pipeline",
            "metadata": { "name": "demo", "namespace": "stardelt", "uid": "uid-1", "generation": 1 },
            "spec": { "ownerGroup": "demo", "tasks": tasks }
        }))
        .expect("Pipeline")
    }

    fn graph(files: Value) -> Value {
        json!({
            "name": "build",
            "kind": "spark-pipeline",
            "project": "graph",
            "files": files
        })
    }

    fn two_files() -> Value {
        json!({
            "spark-pipeline.yml": "name: check\nlibraries:\n  - glob:\n      include: sql/**\n",
            "sql/mv_one.sql": "CREATE MATERIALIZED VIEW IF NOT EXISTS range_one AS SELECT * FROM range(1);\n"
        })
    }

    fn count(requests: &Recorded, method: &str, fragment: &str) -> usize {
        requests
            .lock()
            .expect("requests poisoned")
            .iter()
            .filter(|(m, path, _)| m == method && path.contains(fragment))
            .count()
    }

    #[test]
    fn only_a_spark_pipeline_task_with_files_renders_a_config_map() {
        let p = pipeline(json!([
            { "name": "load", "kind": "sql", "sql": "SELECT 1" },
            { "name": "empty", "kind": "spark-pipeline", "project": "graph" }
        ]));
        assert!(
            config_map_for(&p, &p.spec.tasks[0]).is_none(),
            "sql renders nothing"
        );
        assert!(
            config_map_for(&p, &p.spec.tasks[1]).is_none(),
            "a task without files renders nothing"
        );

        let p = pipeline(json!([graph(two_files())]));
        let config_map = config_map_for(&p, &p.spec.tasks[0]).expect("a graph renders a ConfigMap");
        assert_eq!(
            config_map.metadata.name.as_deref(),
            Some("spark-project-demo-build")
        );
        assert_eq!(config_map.metadata.namespace.as_deref(), Some("stardelt"));
        assert_eq!(
            config_map.data.as_ref().map(|d| d.len()),
            Some(2),
            "both graphs files ride the ConfigMap"
        );
        let labels = config_map.labels();
        assert_eq!(
            labels
                .get("app.kubernetes.io/managed-by")
                .map(String::as_str),
            Some("stardelt-operator")
        );
        assert_eq!(labels.get(PROJECT_LABEL).map(String::as_str), Some("demo"));
        assert_eq!(labels.get(TASK_LABEL).map(String::as_str), Some("build"));
        assert_eq!(
            config_map
                .metadata
                .owner_references
                .as_ref()
                .map(|o| o[0].kind.as_str()),
            Some("Pipeline"),
            "the Pipeline owns its graph ConfigMap"
        );
    }

    #[tokio::test]
    async fn a_two_file_graph_is_created_once() {
        let created: Arc<Mutex<Option<Value>>> = Arc::default();
        let state = created.clone();
        let (client, requests) = mock_client(move |method, path, body| {
            if method == "GET" && path.ends_with("/configmaps/spark-project-demo-build") {
                return (404, NOT_FOUND.to_string());
            }
            if method == "POST" && path.ends_with("/configmaps") {
                let document: Value = serde_json::from_str(body).expect("json");
                *state.lock().expect("poisoned") = Some(document.clone());
                return (201, document.to_string());
            }
            if method == "GET" && path.ends_with("/configmaps") {
                return (
                    200,
                    json!({"apiVersion": "v1", "kind": "ConfigMapList", "items": []}).to_string(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.to_string(),
            )
        });
        let p = pipeline(json!([graph(two_files())]));

        let action = reconcile_spark_project(Arc::new(p), context(client))
            .await
            .expect("reconcile");
        assert_eq!(action, Action::requeue(Duration::from_secs(3600)));

        assert_eq!(
            count(&requests, "POST", "/configmaps"),
            1,
            "exactly one create"
        );
        let created = created
            .lock()
            .expect("poisoned")
            .clone()
            .expect("a POST body");
        assert_eq!(created["metadata"]["name"], "spark-project-demo-build");
        assert_eq!(
            created["data"]["spark-pipeline.yml"],
            two_files()["spark-pipeline.yml"]
        );
        assert_eq!(
            created["data"]["sql/mv_one.sql"],
            two_files()["sql/mv_one.sql"]
        );
    }

    #[tokio::test]
    async fn a_converged_graph_is_not_written_again() {
        let live: Arc<Mutex<Option<Value>>> = Arc::default();
        let state = live.clone();
        let (client, requests) = mock_client(move |method, path, body| {
            if method == "GET" && path.ends_with("/configmaps/spark-project-demo-build") {
                let live = state.lock().expect("poisoned").clone();
                return match live {
                    Some(document) => (200, document.to_string()),
                    None => (404, NOT_FOUND.to_string()),
                };
            }
            if method == "POST" && path.ends_with("/configmaps") {
                let document: Value = serde_json::from_str(body).expect("json");
                *state.lock().expect("poisoned") = Some(document.clone());
                return (201, document.to_string());
            }
            if method == "GET" && path.ends_with("/configmaps") {
                let live = state.lock().expect("poisoned").clone();
                let items = live.map(|document| vec![document]).unwrap_or_default();
                return (
                    200,
                    json!({"apiVersion": "v1", "kind": "ConfigMapList", "items": items})
                        .to_string(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.to_string(),
            )
        });
        let ctx = context(client);

        reconcile_spark_project(Arc::new(pipeline(json!([graph(two_files())]))), ctx.clone())
            .await
            .expect("first reconcile");
        assert_eq!(count(&requests, "POST", "/configmaps"), 1, "one create");

        reconcile_spark_project(Arc::new(pipeline(json!([graph(two_files())]))), ctx)
            .await
            .expect("second reconcile");
        assert_eq!(
            count(&requests, "POST", "/configmaps"),
            1,
            "a converged graph sends no second create"
        );
        assert_eq!(
            count(&requests, "PATCH", "/configmaps"),
            0,
            "a converged graph sends no patch"
        );
    }

    #[tokio::test]
    async fn a_task_that_loses_its_files_deletes_the_config_map() {
        let (client, requests) = mock_client(move |method, path, _body| {
            if method == "GET" && path.ends_with("/configmaps") {
                return (
                    200,
                    json!({
                        "apiVersion": "v1",
                        "kind": "ConfigMapList",
                        "items": [{
                            "apiVersion": "v1",
                            "kind": "ConfigMap",
                            "metadata": {
                                "name": "spark-project-demo-build",
                                "namespace": "stardelt",
                                "labels": { PROJECT_LABEL: "demo", TASK_LABEL: "build" }
                            },
                            "data": { "spark-pipeline.yml": "name: check\n" }
                        }]
                    })
                    .to_string(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.to_string(),
            )
        });
        let p = pipeline(json!([
            { "name": "build", "kind": "spark-pipeline", "project": "graph" }
        ]));

        reconcile_spark_project(Arc::new(p), context(client))
            .await
            .expect("reconcile");

        assert_eq!(
            count(&requests, "DELETE", "/configmaps/spark-project-demo-build"),
            1,
            "the sweep deletes a graph no task wants"
        );
    }

    #[tokio::test]
    async fn two_spark_pipeline_tasks_write_two_config_maps() {
        let created: Arc<Mutex<HashMap<String, Value>>> = Arc::default();
        let state = created.clone();
        let (client, requests) = mock_client(move |method, path, body| {
            if method == "GET" && path.contains("/configmaps/spark-project-demo-") {
                return (404, NOT_FOUND.to_string());
            }
            if method == "POST" && path.ends_with("/configmaps") {
                let document: Value = serde_json::from_str(body).expect("json");
                let name = document["metadata"]["name"]
                    .as_str()
                    .expect("a name")
                    .to_string();
                state
                    .lock()
                    .expect("poisoned")
                    .insert(name, document.clone());
                return (201, document.to_string());
            }
            if method == "GET" && path.ends_with("/configmaps") {
                return (
                    200,
                    json!({"apiVersion": "v1", "kind": "ConfigMapList", "items": []}).to_string(),
                );
            }
            (
                200,
                r#"{"kind":"Status","apiVersion":"v1","status":"Success"}"#.to_string(),
            )
        });
        let mut second = graph(two_files());
        second["name"] = json!("publish");
        let p = pipeline(json!([graph(two_files()), second]));

        reconcile_spark_project(Arc::new(p), context(client))
            .await
            .expect("reconcile");

        assert_eq!(count(&requests, "POST", "/configmaps"), 2, "two creates");
        let mut names = created
            .lock()
            .expect("poisoned")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(
            names,
            vec![
                "spark-project-demo-build".to_string(),
                "spark-project-demo-publish".to_string()
            ]
        );
    }
}
