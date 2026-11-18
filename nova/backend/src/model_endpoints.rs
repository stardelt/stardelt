//! `GET /api/model-endpoints` (stardelt-operator#609 slice c): the model endpoints an
//! administrator connected, as Ask and the probe read them.
//!
//! The answer never carries the endpoint's URL or its `keyRef`: a signed-in person needs to
//! know that a model exists, which model it is, where prompts go and whether the probe found
//! it usable — not the internal hostname or the name of the Connection holding the key. The
//! key itself never reaches Nova through this route (the kubelet projects it into the pod).
//!
//! Without a cluster (demo mode, a local run) the answer is an empty list: no endpoint means
//! no AI feature, which is the design's switch. A cluster that refuses is a 502.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind};
use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

use crate::AppState;
use crate::connection_routes::Fail;

/// One endpoint as the UI and Ask need it. No `url`, no `keyRef`.
#[derive(Debug, Clone, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelEndpointRow {
    pub name: String,
    pub model: String,
    /// `on-premise` | `cloud` (the CRD's default is `on-premise`).
    pub location: String,
    pub purposes: Vec<String>,
    /// May query results be sent to this model? Off by default.
    pub send_rows: bool,
    pub reachable: Option<bool>,
    pub tool_calling: Option<bool>,
    pub json_output: Option<bool>,
    pub context_tokens: Option<i64>,
    pub checked_at: Option<String>,
    pub message: Option<String>,
    /// `spec.quota.perGroupMonthlyTokens`: the tokens one group may spend a month, if set.
    #[serde(rename = "perGroupMonthlyTokens")]
    pub quota_per_group_monthly: Option<i64>,
    /// `spec.quota.perPersonDailyTokens`: the tokens one person may spend a day, if set.
    #[serde(rename = "perPersonDailyTokens")]
    pub quota_per_person_daily: Option<i64>,
}

/// The `ModelEndpoint` resources of the platform namespace.
pub(crate) fn endpoints_api(client: kube::Client) -> Api<DynamicObject> {
    Api::namespaced_with(
        client,
        &crate::cluster::namespace(),
        &ApiResource::from_gvk(&GroupVersionKind::gvk(
            "stardelt.io",
            "v1alpha1",
            "ModelEndpoint",
        )),
    )
}

/// One object as a row; absent fields take the CRD's defaults.
pub(crate) fn row(object: &Value) -> ModelEndpointRow {
    let spec = object.get("spec").unwrap_or(&Value::Null);
    let status = object.get("status").unwrap_or(&Value::Null);
    let quota = crate::model_calls::quota_of(object);
    ModelEndpointRow {
        name: object
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        model: spec
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        location: spec
            .get("location")
            .and_then(Value::as_str)
            .unwrap_or("on-premise")
            .to_string(),
        purposes: spec
            .get("purposes")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_else(|| vec!["ask".into(), "agents".into()]),
        send_rows: spec
            .get("sendRows")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        reachable: status.get("reachable").and_then(Value::as_bool),
        tool_calling: status.get("toolCalling").and_then(Value::as_bool),
        json_output: status.get("jsonOutput").and_then(Value::as_bool),
        context_tokens: status.get("contextTokens").and_then(Value::as_i64),
        checked_at: status
            .get("checkedAt")
            .and_then(Value::as_str)
            .map(str::to_string),
        message: status
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_string),
        quota_per_group_monthly: quota.per_group_monthly,
        quota_per_person_daily: quota.per_person_daily,
    }
}

/// The rows of a list answer, sorted by name so the UI's order does not move.
fn rows(objects: &[Value]) -> Vec<ModelEndpointRow> {
    let mut rows: Vec<_> = objects.iter().map(row).collect();
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    rows
}

/// `GET /api/model-endpoints`: the models an administrator connected to the platform.
#[utoipa::path(
    get,
    operation_id = "model_endpoints_list",
    path = "/api/model-endpoints",
    tag = "public",
    description = "Every `ModelEndpoint` of the platform namespace, name order: `name`, `model`, `location` (`on-premise` | `cloud`, default `on-premise`), `purposes` (default `[\"ask\",\"agents\"]`), `sendRows` (may query results be sent to this model; off by default), the quota of `spec.quota` — `perGroupMonthlyTokens` and `perPersonDailyTokens`, each `null` for no limit — and the probe's last answer — `reachable`, `toolCalling`, `jsonOutput`, `contextTokens`, `checkedAt`, `message`, each `null` until the probe has run. The endpoint's `url` and its `keyRef` are absent on purpose: the shape says that a model exists, what it is, where prompts go and whether it works, never the internal hostname or the name of the Connection holding the key. Without a cluster connection (demo mode, a local run) the answer is an empty list: no endpoint means no AI feature. A cluster that refuses or fails is a 502, never an empty list that would read as \"no model\". Answers `Cache-Control: no-store`.",
    responses(
        (status = 200, description = "One row per endpoint, name order; empty when none is connected", body = [ModelEndpointRow]),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer, or Nova may not list modelendpoints", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn list(State(state): State<Arc<AppState>>) -> Response {
    let objects = match state.kube.clone() {
        None => Vec::new(),
        Some(client) => match endpoints_api(client)
            .list(&kube::api::ListParams::default())
            .await
        {
            Ok(list) => {
                let mut values = Vec::with_capacity(list.items.len());
                for object in list.items {
                    match serde_json::to_value(object) {
                        Ok(value) => values.push(value),
                        Err(e) => {
                            tracing::warn!(error = %e, "model endpoint object is not JSON");
                            return Fail::new(
                                StatusCode::BAD_GATEWAY,
                                "the cluster did not answer",
                            )
                            .into_response();
                        }
                    }
                }
                values
            }
            Err(e) => {
                tracing::warn!(error = %e, "model endpoint list failed");
                return Fail::new(StatusCode::BAD_GATEWAY, "the cluster did not answer")
                    .into_response();
            }
        },
    };
    (
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(rows(&objects)),
    )
        .into_response()
}

/// `POST /api/model-endpoints/{name}/probe` — an administrator asks Nova to check one
/// endpoint now, instead of waiting for the minute round.
#[utoipa::path(
    post,
    operation_id = "model_endpoint_probe",
    path = "/api/model-endpoints/{name}/probe",
    tag = "internal",
    description = "Start the same probe the minute round runs, for one endpoint only, and answer at once. Administrator-only (the administrator gate is `main`'s). The answer is **not** the probe's result: a probe makes up to three calls of up to 120 seconds each, so this answers `202` with the endpoint as it is now — `GET /api/model-endpoints` shows the new `checkedAt`, `reachable` and `message` when the probe has written its `status`. A second call while a probe of the same endpoint runs is also `202` and starts nothing: two clicks make one probe. Answers `Cache-Control: no-store`.",
    params(
        ("name" = String, Path, description = "The endpoint's name"),
    ),
    responses(
        (status = 202, description = "The probe is running (or was already running); the endpoint as it is now", body = ModelEndpointRow),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 403, description = "Not an administrator", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such model endpoint", body = crate::openapi::ErrorResponse),
        (status = 500, description = "Nova could not build its HTTP client (the platform CA did not load)", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn probe(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    let Some(client) = state.kube.clone() else {
        return Fail::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "Nova has no cluster connection",
        )
        .into_response();
    };
    let api = endpoints_api(client);
    let object = match api.get(&name).await {
        Ok(object) => object,
        Err(kube::Error::Api(e)) if e.code == 404 => {
            return Fail::new(StatusCode::NOT_FOUND, format!("no model endpoint {name}"))
                .into_response();
        }
        Err(e) => {
            tracing::warn!(error = %e, "model endpoint get failed");
            return Fail::new(StatusCode::BAD_GATEWAY, "the cluster did not answer")
                .into_response();
        }
    };
    // The probe would be a silent skip without a client; the caller has to hear it.
    if crate::model_probe::http_client().is_none() {
        return Fail::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Nova could not reach its own HTTP client",
        )
        .into_response();
    }
    // Already running: the answer is still 202, and no second probe starts.
    if crate::model_probe::try_start(&name) {
        let state = Arc::clone(&state);
        let object = object.clone();
        let api = api.clone();
        tokio::spawn(async move {
            crate::model_probe::probe_named(&state, &object, &api).await;
        });
    }
    let view = row(&serde_json::to_value(&object).unwrap_or_default());
    let mut answer = (StatusCode::ACCEPTED, Json(view)).into_response();
    answer
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    answer
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Two endpoints as the API server holds them: object A takes every default, object B
    /// names every field and carries a probe result.
    fn objects() -> Vec<Value> {
        vec![
            json!({
                "metadata": {"name": "default"},
                "spec": {
                    "url": "https://llm.internal.example/v1",
                    "model": "qwen3-4b",
                    "keyRef": {"name": "llm-key", "key": "api-key"}
                }
            }),
            json!({
                "metadata": {"name": "cloud"},
                "spec": {
                    "url": "https://llm.internal.example/v1",
                    "model": "frontier-1",
                    "keyRef": {"name": "llm-key", "key": "api-key"},
                    "location": "cloud",
                    "sendRows": true,
                    "quota": {"perGroupMonthlyTokens": 2000000}
                },
                "status": {
                    "reachable": true,
                    "toolCalling": false,
                    "jsonOutput": true,
                    "contextTokens": 32768,
                    "checkedAt": "2026-11-15T00:00:00Z",
                    "message": "ok"
                }
            }),
        ]
    }

    #[test]
    fn rows_take_the_crd_defaults() {
        let objects = objects();
        let a = row(&objects[0]);
        assert_eq!(a.name, "default");
        assert_eq!(a.model, "qwen3-4b");
        assert_eq!(a.location, "on-premise");
        assert_eq!(a.purposes, vec!["ask".to_string(), "agents".to_string()]);
        assert!(!a.send_rows);
        assert_eq!(a.reachable, None);
        assert_eq!(a.tool_calling, None);
        assert_eq!(a.json_output, None);
        assert_eq!(a.context_tokens, None);
        assert_eq!(a.checked_at, None);
        assert_eq!(a.message, None);
        assert_eq!(a.quota_per_group_monthly, None);
        assert_eq!(a.quota_per_person_daily, None);

        let b = row(&objects[1]);
        assert_eq!(b.name, "cloud");
        assert_eq!(b.location, "cloud");
        assert!(b.send_rows);
        assert_eq!(b.reachable, Some(true));
        assert_eq!(b.tool_calling, Some(false));
        assert_eq!(b.json_output, Some(true));
        assert_eq!(b.context_tokens, Some(32768));
        assert_eq!(b.checked_at.as_deref(), Some("2026-11-15T00:00:00Z"));
        assert_eq!(b.message.as_deref(), Some("ok"));
        assert_eq!(b.quota_per_group_monthly, Some(2_000_000));
        assert_eq!(b.quota_per_person_daily, None);
    }

    /// The quota travels to the UI under the CRD's own field names, so a reader can match
    /// `perGroupMonthlyTokens` in the answer against `spec.quota` in the resource.
    #[test]
    fn the_quota_is_serialised_under_the_crd_names() {
        let value = serde_json::to_value(row(&objects()[1])).expect("JSON");
        assert_eq!(value["perGroupMonthlyTokens"], json!(2_000_000));
        assert_eq!(value["perPersonDailyTokens"], Value::Null);
        assert_eq!(
            serde_json::to_value(row(&objects()[0])).expect("JSON")["perGroupMonthlyTokens"],
            Value::Null
        );
    }

    #[test]
    fn rows_never_carry_the_url_or_the_connection() {
        let text = serde_json::to_string(&rows(&objects())).expect("JSON");
        assert!(!text.contains("llm.internal.example"), "{text}");
        assert!(!text.contains("llm-key"), "{text}");
        assert!(!text.contains("api-key"), "{text}");
    }

    #[test]
    fn rows_sort_by_name() {
        let ordered: Vec<Value> = objects().into_iter().rev().collect();
        let rows = rows(&ordered);
        assert_eq!(
            rows.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
            vec!["cloud", "default"]
        );
    }

    /// `GET /api/model-endpoints` answers an empty list and `Cache-Control: no-store` without
    /// a cluster client — the shape `make demo` sees.
    #[tokio::test]
    async fn the_route_is_empty_and_uncacheable_without_a_cluster() {
        use axum::{Router, body::Body, http::Request, routing::get};
        use tower::ServiceExt;

        let state = crate::router_tests::test_state();
        let app = Router::new()
            .route("/model-endpoints", get(list))
            .with_state(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/model-endpoints")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("no-store")
        );
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("JSON");
        assert_eq!(body, json!([]));
    }

    /// `POST /api/model-endpoints/{name}/probe` without a cluster client is a 503 with the
    /// sentence that says why — never a 202 for a probe that cannot run.
    #[tokio::test]
    async fn a_missing_cluster_is_503() {
        use axum::Router;
        use axum::body::Body;
        use axum::http::Request;
        use axum::routing::post;
        use tower::ServiceExt;

        let state = crate::router_tests::test_state();
        let app = Router::new()
            .route("/model-endpoints/{name}/probe", post(probe))
            .with_state(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/model-endpoints/default/probe")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let text = String::from_utf8(bytes.to_vec()).expect("utf8");
        assert!(text.contains("Nova has no cluster connection"), "{text}");
    }
}
