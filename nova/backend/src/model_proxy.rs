//! The OpenAI-compatible model proxy (agent harness slice 2, stardelt/stardelt#2380).
//!
//! Both runtimes of the agent harness need a model and neither may hold a model key: the
//! local container on a person's device and the managed pod. Nova already holds the keys
//! (projected at `/var/run/stardelt/models/<endpoint>/key`), knows the person, checks the
//! budget and records every call — so this module serves the endpoint through the OpenAI
//! protocol, with the person's own Nova token as the API key:
//!
//! - `POST /api/models/{endpoint}/v1/chat/completions` — an OpenAI chat-completions body in,
//!   the endpoint's answer out;
//! - `GET /api/models/{endpoint}/v1/models` — the one model that endpoint serves.
//!
//! A client points its OpenAI base URL at `https://<nova>/api/models/<endpoint>/v1`.
//!
//! What the module keeps, as Ask does:
//!
//! - **The body is the client's, within limits.** `model` is replaced by the endpoint's
//!   `spec.model`, `max_tokens` is capped at 4096, `stream: true` is refused, the body may be
//!   at most 1 MiB, and `messages`, `tools`, `tool_choice` and `response_format` pass through.
//! - **The person pays and the call is recorded.** The budget check runs before the call and
//!   exactly one `model_call` row is written per call, with `feature: "agent"`.
//! - **The endpoint's URL and key never leave the call.** They are in no answer, header, log
//!   line or `model_call` row.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    Extension,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

use crate::AppState;
use crate::model_endpoints::ModelEndpointRow;

/// The largest body the proxy takes.
const MAX_BODY_BYTES: usize = 1 << 20;
/// The token cap the proxy applies (`max_tokens`), and the value an absent one takes.
const MAX_TOKENS: u64 = 4096;
/// How much of a refused answer the client and the `model_call` row see.
const REFUSAL_CHARS: usize = 2000;

/// A response that must not be cached: every answer of this module carries `no-store`.
fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// One refusal in the API's envelope.
fn fail(status: StatusCode, message: impl Into<String>) -> Response {
    no_store(crate::error::Fail::new(status, message).into_response())
}

/// The client's body with the endpoint's model, a capped `max_tokens`, and no streaming.
///
/// A body that is not a JSON object is `400`; `stream: true` is `400` too. Everything else
/// (`messages`, `tools`, `tool_choice`, `response_format`) passes through unchanged.
pub(crate) fn prepare(body: Value, model: &str) -> Result<Value, (StatusCode, String)> {
    let mut body = body;
    let Some(fields) = body.as_object_mut() else {
        return Err((
            StatusCode::BAD_REQUEST,
            "the body must be a JSON object".to_string(),
        ));
    };
    if fields.get("stream").and_then(Value::as_bool) == Some(true) {
        return Err((
            StatusCode::BAD_REQUEST,
            "streaming is not supported yet; send stream: false".to_string(),
        ));
    }
    fields.insert("model".to_string(), Value::String(model.to_string()));
    match fields.get("max_tokens").and_then(Value::as_u64) {
        Some(tokens) if tokens <= MAX_TOKENS => {}
        _ => {
            fields.insert("max_tokens".to_string(), json!(MAX_TOKENS));
        }
    }
    Ok(body)
}

/// Whether an endpoint may serve an agent: purpose `agents` and a reachable last probe.
pub(crate) fn admitted(row: &ModelEndpointRow) -> Result<(), (StatusCode, String)> {
    if !row.purposes.iter().any(|purpose| purpose == "agents") {
        return Err((
            StatusCode::FORBIDDEN,
            format!("endpoint `{}` is not offered to agents", row.name),
        ));
    }
    if row.reachable != Some(true) {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("endpoint `{}` is not reachable now", row.name),
        ));
    }
    Ok(())
}

/// The `ModelEndpoint` objects of the platform namespace as the API server holds them, as
/// `ask.rs` reads them.
// The `Err` is the ready-made refusal this returns, as in `ask.rs`.
#[allow(clippy::result_large_err)]
async fn endpoint_objects(kube: kube::Client) -> Result<Vec<Value>, Response> {
    match crate::model_endpoints::endpoints_api(kube)
        .list(&kube::api::ListParams::default())
        .await
    {
        Ok(list) => {
            let mut values = Vec::with_capacity(list.items.len());
            for object in list.items {
                match serde_json::to_value(object) {
                    Ok(value) => values.push(value),
                    Err(e) => {
                        tracing::warn!(error = %e, "model proxy: a model endpoint is not JSON")
                    }
                }
            }
            Ok(values)
        }
        Err(e) => {
            tracing::warn!(error = %e, "model proxy: model endpoint list failed");
            Err(fail(StatusCode::BAD_GATEWAY, "the cluster did not answer"))
        }
    }
}

/// The endpoint the caller named, or the refusal that answers instead.
#[allow(clippy::result_large_err)]
async fn endpoint_object(state: &AppState, name: &str) -> Result<Value, Response> {
    let Some(kube) = state.kube.clone() else {
        return Err(fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "no model endpoint is connected",
        ));
    };
    let objects = endpoint_objects(kube).await?;
    objects
        .into_iter()
        .find(|object| object.pointer("/metadata/name").and_then(Value::as_str) == Some(name))
        .ok_or_else(|| fail(StatusCode::NOT_FOUND, format!("no model endpoint `{name}`")))
}

/// An answer in the upstream's content type, or JSON when it named none.
fn pass_through(status: StatusCode, content_type: Option<HeaderValue>, body: Vec<u8>) -> Response {
    let content_type = content_type.unwrap_or_else(|| HeaderValue::from_static("application/json"));
    no_store((status, [(header::CONTENT_TYPE, content_type)], body).into_response())
}

/// Everything a `model_call` row needs that does not change between the calls of one request.
struct Recording<'a> {
    pool: &'a sqlx::PgPool,
    sub: &'a str,
    group: Option<&'a str>,
    endpoint: &'a str,
    model: &'a str,
}

impl Recording<'_> {
    /// The single `model_call` row this call writes, best effort as in Ask.
    async fn write(
        &self,
        outcome: &str,
        usage: Option<&Value>,
        prompt: &str,
        answer: Option<&str>,
        latency_ms: u64,
    ) {
        let call = crate::model_calls::Call {
            sub: self.sub,
            group: self.group,
            feature: "agent",
            endpoint: self.endpoint,
            model: self.model,
            tokens: crate::model_calls::tokens(usage, prompt, answer),
            latency_ms,
            outcome,
            prompt,
            answer,
        };
        if let Err(e) = crate::model_calls::record(self.pool, &call).await {
            tracing::warn!(error = %e, "could not store the agent model call");
        }
    }
}

/// The OpenAI chat-completions route — the endpoint, through the person.
#[utoipa::path(
    post,
    operation_id = "model_proxy_chat",
    path = "/api/models/{endpoint}/v1/chat/completions",
    tag = "public",
    description = "The OpenAI chat-completions protocol, answered by the `ModelEndpoint` named in the path, on behalf of the signed-in caller. Point an OpenAI client's base URL at `https://<nova>/api/models/<endpoint>/v1` and use the person's own Nova token as the API key: the budget and the audit then cover the agent's work. The caller must be signed in (`401`); the endpoint must exist (`404`), list the purpose `agents` (`403`) and its last probe must have found it reachable (`503`). The body must be a JSON object of at most 1 MiB (`413`); `model` is replaced by the endpoint's `spec.model`, `max_tokens` is capped at 4096 (absent means 4096), `stream: true` is refused with `400` (`streaming is not supported yet; send stream: false`), and `messages`, `tools`, `tool_choice` and `response_format` pass through unchanged. The person's or their group's model budget is checked before the call (`429` as on `/api/ask`), and exactly one `model_call` row is written with `feature: agent` — the body's `messages` as its prompt, the answer's `usage` as its tokens, and `ok`, `refused` or `error` as its outcome. A 2xx answer passes through with its status; a non-2xx answer passes through with its status and its body cut to 2000 characters; no answer at all is a `502`. The endpoint's URL and key never appear in an answer or a log line. Answers `Cache-Control: no-store`; streaming is not supported yet.",
    params(
        ("endpoint" = String, Path, description = "The `ModelEndpoint`'s name"),
    ),
    responses(
        (status = 200, description = "The endpoint's own answer, passed through", body = serde_json::Value),
        (status = 400, description = "The body is not a JSON object, or it asks for `stream: true`", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The endpoint does not list the purpose `agents`", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such model endpoint", body = crate::openapi::ErrorResponse),
        (status = 413, description = "The body is larger than 1 MiB", body = crate::openapi::ErrorResponse),
        (status = 429, description = "The person's or their group's model budget is spent", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The model endpoint did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No state store, no cluster connection, no HTTP client, or the endpoint is not reachable now", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn chat(
    State(state): State<Arc<AppState>>,
    caller: Option<Extension<crate::auth::Principal>>,
    Path(endpoint): Path<String>,
    body: Bytes,
) -> Response {
    let Some(caller) = caller else {
        return fail(StatusCode::UNAUTHORIZED, "not signed in");
    };
    if body.len() > MAX_BODY_BYTES {
        return fail(
            StatusCode::PAYLOAD_TOO_LARGE,
            "the body is larger than 1 MiB",
        );
    }
    let object = match endpoint_object(&state, &endpoint).await {
        Ok(object) => object,
        Err(response) => return response,
    };
    let row = crate::model_endpoints::row(&object);
    if let Err((status, message)) = admitted(&row) {
        return fail(status, message);
    }
    let Ok(request) = serde_json::from_slice::<Value>(&body) else {
        return fail(StatusCode::BAD_REQUEST, "the body must be a JSON object");
    };
    let prepared = match prepare(request, &row.model) {
        Ok(prepared) => prepared,
        Err((status, message)) => return fail(status, message),
    };
    let Some(pool) = state.db.as_ref() else {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "the model proxy needs Nova's database, and this installation has none",
        );
    };
    let url = object
        .pointer("/spec/url")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    if url.is_empty() {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("endpoint `{}` is not reachable now", row.name),
        );
    }
    let Some(client) = crate::model_probe::http_client() else {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "Nova could not reach its own HTTP client",
        );
    };
    // The same group is checked against the quota and written to the row, so spend and
    // limit agree.
    let sub = caller.0.identity.sub.clone();
    let group = caller.0.identity.groups.first().cloned();
    if let Err(refusal) = crate::model_calls::check(
        pool,
        &crate::model_calls::quota_of(&object),
        group.as_deref(),
        &sub,
    )
    .await
    {
        return crate::model_calls::refuse(&refusal);
    }
    let prompt = prepared
        .get("messages")
        .map(ToString::to_string)
        .unwrap_or_default();
    let recording = Recording {
        pool,
        sub: &sub,
        group: group.as_deref(),
        endpoint: &endpoint,
        model: &row.model,
    };
    let started = Instant::now();
    let mut outgoing = client
        .post(format!(
            "{}/chat/completions",
            crate::model_chat::base_url(&url)
        ))
        .json(&prepared);
    if let Some(key) = crate::model_chat::key_for(&endpoint) {
        outgoing = outgoing.bearer_auth(key);
    }
    let response = match outgoing.send().await {
        Ok(response) => response,
        Err(e) => {
            tracing::warn!(
                error = %e.without_url(),
                "model proxy: the endpoint did not answer"
            );
            recording
                .write(
                    "error",
                    None,
                    &prompt,
                    None,
                    started.elapsed().as_millis() as u64,
                )
                .await;
            return fail(StatusCode::BAD_GATEWAY, "the model endpoint did not answer");
        }
    };
    let latency_ms = started.elapsed().as_millis() as u64;
    let status = response.status();
    let content_type = response.headers().get(header::CONTENT_TYPE).cloned();
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::warn!(
                error = %e.without_url(),
                "model proxy: the endpoint's answer was not read"
            );
            recording
                .write("error", None, &prompt, None, latency_ms)
                .await;
            return fail(StatusCode::BAD_GATEWAY, "the model endpoint did not answer");
        }
    };
    let text = String::from_utf8_lossy(&bytes).to_string();
    let parsed: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    let usage = parsed.get("usage").cloned();
    let outcome = if status.is_success() { "ok" } else { "refused" };
    recording
        .write(outcome, usage.as_ref(), &prompt, Some(&text), latency_ms)
        .await;
    let code = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    if status.is_success() {
        return pass_through(code, content_type, bytes.to_vec());
    }
    if text.chars().count() > REFUSAL_CHARS {
        let cut: String = text.chars().take(REFUSAL_CHARS).collect();
        return pass_through(
            code,
            Some(HeaderValue::from_static("text/plain; charset=utf-8")),
            cut.into_bytes(),
        );
    }
    pass_through(code, content_type, bytes.to_vec())
}

/// The OpenAI model list for one endpoint.
#[utoipa::path(
    get,
    operation_id = "model_proxy_models",
    path = "/api/models/{endpoint}/v1/models",
    tag = "public",
    description = "The OpenAI model-list protocol for one `ModelEndpoint`: `{\"object\":\"list\",\"data\":[{\"id\":\"<spec.model>\",\"object\":\"model\"}]}`. A client that checks `GET /v1/models` before its first call reads this. The caller must be signed in (`401`); the endpoint must exist (`404`), list the purpose `agents` (`403`) and its last probe must have found it reachable (`503`), exactly as on the chat-completions route. The endpoint's URL and key never appear in the answer. Answers `Cache-Control: no-store`.",
    params(
        ("endpoint" = String, Path, description = "The `ModelEndpoint`'s name"),
    ),
    responses(
        (status = 200, description = "The endpoint's model in the OpenAI list shape", body = serde_json::Value),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The endpoint does not list the purpose `agents`", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such model endpoint", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No cluster connection, or the endpoint is not reachable now", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn models(
    State(state): State<Arc<AppState>>,
    caller: Option<Extension<crate::auth::Principal>>,
    Path(endpoint): Path<String>,
) -> Response {
    if caller.is_none() {
        return fail(StatusCode::UNAUTHORIZED, "not signed in");
    }
    let object = match endpoint_object(&state, &endpoint).await {
        Ok(object) => object,
        Err(response) => return response,
    };
    let row = crate::model_endpoints::row(&object);
    if let Err((status, message)) = admitted(&row) {
        return fail(status, message);
    }
    no_store(
        (
            StatusCode::OK,
            axum::Json(json!({
                "object": "list",
                "data": [{"id": row.model, "object": "model"}],
            })),
        )
            .into_response(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One endpoint as the API server holds it, with the probe's answer.
    fn endpoint(purposes: &[&str], reachable: Value) -> ModelEndpointRow {
        crate::model_endpoints::row(&json!({
            "metadata": {"name": "ernie"},
            "spec": {"url": "https://llm.internal.example/v1", "model": "spec", "purposes": purposes},
            "status": {"reachable": reachable},
        }))
    }

    #[test]
    fn prepare_replaces_the_model_and_caps_max_tokens() {
        let body = json!({"model": "x", "max_tokens": 99999, "messages": []});
        let prepared = prepare(body, "spec").expect("a prepared body");
        assert_eq!(prepared["model"], json!("spec"));
        assert_eq!(prepared["max_tokens"], json!(4096));
    }

    #[test]
    fn prepare_inserts_max_tokens_when_absent() {
        let prepared = prepare(json!({"messages": []}), "spec").expect("a prepared body");
        assert_eq!(prepared["max_tokens"], json!(4096));
        assert_eq!(prepared["model"], json!("spec"));
    }

    #[test]
    fn prepare_refuses_streaming() {
        let error =
            prepare(json!({"stream": true, "messages": []}), "spec").expect_err("a refusal");
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert_eq!(
            error.1,
            "streaming is not supported yet; send stream: false"
        );
        // A body that is not an object is the other 400.
        let error = prepare(json!([1, 2]), "spec").expect_err("a refusal");
        assert_eq!(error.0, StatusCode::BAD_REQUEST);
        assert_eq!(error.1, "the body must be a JSON object");
    }

    #[test]
    fn prepare_keeps_tools_and_tool_choice() {
        let tools = json!([{"type": "function", "function": {"name": "sql_explain"}}]);
        let body = json!({
            "model": "x",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": "hi"}],
            "tools": tools,
            "tool_choice": "auto",
            "response_format": {"type": "json_object"},
        });
        let prepared = prepare(body, "spec").expect("a prepared body");
        assert_eq!(prepared["tools"], tools);
        assert_eq!(prepared["tool_choice"], json!("auto"));
        assert_eq!(prepared["response_format"], json!({"type": "json_object"}));
        assert_eq!(prepared["max_tokens"], json!(10), "under the cap is kept");
    }

    #[test]
    fn admitted_refuses_an_endpoint_without_the_agents_purpose() {
        let row = endpoint(&["ask"], json!(true));
        let error = admitted(&row).expect_err("a refusal");
        assert_eq!(error.0, StatusCode::FORBIDDEN);
        assert_eq!(error.1, "endpoint `ernie` is not offered to agents");
    }

    #[test]
    fn admitted_refuses_an_unreachable_endpoint() {
        let row = endpoint(&["ask", "agents"], json!(false));
        let error = admitted(&row).expect_err("a refusal");
        assert_eq!(error.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.1, "endpoint `ernie` is not reachable now");
        // No probe answer yet is not reachable either.
        assert!(admitted(&endpoint(&["agents"], Value::Null)).is_err());
        assert!(admitted(&endpoint(&["ask", "agents"], json!(true))).is_ok());
    }
}
