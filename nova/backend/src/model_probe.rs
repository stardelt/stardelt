//! The `ModelEndpoint` probe (stardelt-operator#609 slice c2): Nova calls the endpoint and
//! writes its `status`, so the UI can say whether a model answers, calls tools and returns
//! JSON before Ask is switched on.
//!
//! Four requests, the ones platform#1099 measured against a real server:
//!
//! 1. `GET {base}/models` — best effort, only for the context window (`data[0].meta.n_ctx`).
//! 2. `POST {base}/chat/completions` — a plain question: does the endpoint answer at all?
//! 3. the same call with a `tools` entry — does it return a call for `sql_explain`?
//! 4. the same call with `response_format` — does it return the JSON object it was asked for?
//!
//! A key is whatever the kubelet projected at `/var/run/stardelt/models/<name>/key`; Nova
//! never reads a Secret and never logs the key, the URL or the `keyRef`. A keyless endpoint
//! is probed without the header, never skipped.
//!
//! The loop is the shape of [`crate::catalog_metrics::spawn_refresher`]: one round a minute, and
//! each round probes only the endpoints with no `status.checkedAt` or one older than
//! [`DEFAULT_INTERVAL`] (a day). A new endpoint is therefore probed within a minute, and needs no
//! scheduler of its own.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};
use tracing::{info, warn};

use crate::AppState;

/// Where the kubelet projects each endpoint's key (`stardelt-operator` `nova.rs`
/// `MODEL_KEYS_PATH`, and the chart's `mountPath`). Nova never reads a Secret.
const MODEL_KEYS_PATH: &str = "/var/run/stardelt/models";
/// A probe call's token cap: thinking mode eats an uncapped budget (platform#1099).
const MAX_TOKENS: u32 = 512;
/// One call's wall-clock limit; the spike's slowest answer was 91 s on a CPU model.
const CALL_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_INTERVAL: Duration = Duration::from_secs(86_400);
const MIN_INTERVAL: u64 = 300;
const MAX_INTERVAL: u64 = 604_800;
/// How often the probe looks for endpoints that are due (a new one, or one a day old).
const ROUND_EVERY: Duration = Duration::from_secs(60);
/// The `/models` call is a convenience: it never holds the round up.
const MODELS_TIMEOUT: Duration = Duration::from_secs(10);
/// How much of an error body a message may carry.
const BODY_CHARS: usize = 200;
/// The plain chat check's prompt — also the text whose bytes and hash the call record keeps.
const CHAT_PROMPT: &str = "Reply with one short sentence about lakes.";

/// What one probe found. The field names are the CRD's (`ModelEndpointStatus`), so the
/// merge patch needs no translation: `toolCalling`, `jsonOutput`, `contextTokens`, `checkedAt`.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Outcome {
    pub reachable: Option<bool>,
    pub tool_calling: Option<bool>,
    pub json_output: Option<bool>,
    pub context_tokens: Option<i64>,
    pub checked_at: String,
    pub message: Option<String>,
}

impl Outcome {
    /// `{"status": {…}}` for `Api::patch_status` — only the fields a probe can know.
    pub(crate) fn patch(&self) -> Value {
        json!({ "status": self })
    }
}

/// The current time. The workspace builds `chrono` without its `clock` feature, so
/// `Utc::now()` is not available; the standard library is.
fn now() -> DateTime<Utc> {
    DateTime::<Utc>::from(std::time::SystemTime::now())
}

/// Where the projected keys live; `NOVA_MODEL_KEYS_DIR` is the test seam.
fn keys_dir() -> PathBuf {
    match std::env::var("NOVA_MODEL_KEYS_DIR") {
        Ok(dir) if !dir.trim().is_empty() => PathBuf::from(dir),
        _ => PathBuf::from(MODEL_KEYS_PATH),
    }
}

/// The endpoint's key, or `None` when nothing was projected for it (or it is unreadable).
fn key_at(dir: &Path, name: &str) -> Option<String> {
    let key = std::fs::read_to_string(dir.join(name).join("key")).ok()?;
    let key = key.trim();
    (!key.is_empty()).then(|| key.to_string())
}

/// Whether this endpoint needs a probe now: no `status.checkedAt` yet (a new endpoint), an
/// unparseable one, or one older than `max_age` (the daily round).
fn needs_probe(checked_at: Option<&str>, now: DateTime<Utc>, max_age: Duration) -> bool {
    let Some(checked_at) = checked_at.and_then(|value| DateTime::parse_from_rfc3339(value).ok())
    else {
        return true;
    };
    let Ok(max_age) = chrono::Duration::from_std(max_age) else {
        return true;
    };
    now.signed_duration_since(checked_at) > max_age
}

/// The endpoint's base URL without its trailing slash: the CRD's example is
/// `https://llm.internal.example/v1`, so the calls below append `/models` and
/// `/chat/completions`.
fn base_url(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

/// One chat call's body; `extra` carries what a specific check adds (`tools`, `response_format`).
fn chat_body(model: &str, extra: Value) -> Value {
    let mut body = json!({
        "model": model,
        "max_tokens": MAX_TOKENS,
        "messages": [{"role": "user", "content": CHAT_PROMPT}],
    });
    if let (Some(object), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
        for (key, value) in extra {
            object.insert(key.clone(), value.clone());
        }
    }
    body
}

/// The tool check's body fragment: one function, `sql_explain`.
const TOOL_CHECK: &str = r#"{"tools": [{"type": "function", "function": {"name": "sql_explain", "description": "Check a SQL statement without running it", "parameters": {"type": "object", "properties": {"sql": {"type": "string"}}, "required": ["sql"]}}}]}"#;

/// The JSON check's body fragment: an object with both fields, as the spike measured it.
const JSON_CHECK: &str = r#"{"response_format": {"type": "json_schema", "json_schema": {"name": "answer", "schema": {"type": "object", "properties": {"sql": {"type": "string"}, "explanation": {"type": "string"}}, "required": ["sql", "explanation"]}}}}"#;

/// A fragment as the body `chat_body` merges it into.
fn fragment(raw: &str) -> Value {
    serde_json::from_str(raw).expect("the checks are valid JSON")
}

/// The first `n` characters of a body, never a multi-byte boundary panic.
fn short(body: &str) -> String {
    body.chars().take(BODY_CHARS).collect()
}

/// One call: the shared headers, the endpoint's own base and the body. A transport error
/// is handed back as text, so the outcome says what went wrong without a `reqwest` type.
async fn send(
    client: &reqwest::Client,
    base: &str,
    key: Option<&str>,
    body: &Value,
) -> Result<reqwest::Response, String> {
    let mut request = client.post(format!("{base}/chat/completions")).json(body);
    if let Some(key) = key {
        request = request.bearer_auth(key);
    }
    request.send().await.map_err(|e| e.to_string())
}

/// A non-2xx answer as the message it belongs in: `503: <first 200 characters>`.
fn refusal(status: reqwest::StatusCode, body: &str) -> String {
    format!("{}: {}", status.as_u16(), short(body))
}

/// Record the plain chat check as one `model_call` row (design nova#890, slice 1). Best
/// effort: an endpoint that answered, refused or failed still gets its row, and a failed
/// insert is a log line, never a probe failure. `usage` is the answer body's `usage` object
/// when the endpoint sent one; `answer` is its content.
async fn note(
    db: Option<&sqlx::PgPool>,
    name: &str,
    model: &str,
    latency_ms: u64,
    outcome: &str,
    usage: Option<&Value>,
    answer: Option<&str>,
) {
    let Some(pool) = db else {
        return;
    };
    let call = crate::model_calls::Call {
        sub: "nova",
        group: None,
        feature: "probe",
        endpoint: name,
        model,
        tokens: crate::model_calls::tokens(usage, CHAT_PROMPT, answer),
        latency_ms,
        outcome,
        prompt: CHAT_PROMPT,
        answer,
    };
    if let Err(e) = crate::model_calls::record(pool, &call).await {
        warn!(endpoint = %name, error = %e, "model probe: the call record failed");
    }
}

/// Probe one endpoint: the three measured calls, in order, each one's failure not stopping
/// the next. Never panics; never carries the key or the URL anywhere.
async fn probe(
    client: &reqwest::Client,
    name: &str,
    url: &str,
    model: &str,
    key: Option<&str>,
    db: Option<&sqlx::PgPool>,
) -> Outcome {
    let base = base_url(url);
    let checked_at = now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let mut outcome = Outcome {
        reachable: None,
        tool_calling: None,
        json_output: None,
        context_tokens: None,
        checked_at,
        message: None,
    };

    // (a) The context window, when the server names it (llama.cpp answers `meta.n_ctx`).
    // Never a reason to fail the probe.
    let mut request = client.get(format!("{base}/models")).timeout(MODELS_TIMEOUT);
    if let Some(key) = key {
        request = request.bearer_auth(key);
    }
    if let Ok(response) = request.send().await
        && response.status().is_success()
        && let Ok(body) = response.json::<Value>().await
    {
        outcome.context_tokens = body.pointer("/data/0/meta/n_ctx").and_then(Value::as_i64);
    }

    // (b) Does it answer at all? Everything after this is pointless when it does not.
    let started = std::time::Instant::now();
    match send(client, &base, key, &chat_body(model, json!({}))).await {
        Ok(response) if response.status().is_success() => {
            outcome.reachable = Some(true);
            let body: Value = response.json().await.unwrap_or(Value::Null);
            let answer = body
                .pointer("/choices/0/message/content")
                .and_then(Value::as_str);
            note(
                db,
                name,
                model,
                started.elapsed().as_millis() as u64,
                "ok",
                body.get("usage"),
                answer,
            )
            .await;
        }
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            outcome.reachable = Some(false);
            outcome.message = Some(refusal(status, &body));
            note(
                db,
                name,
                model,
                started.elapsed().as_millis() as u64,
                "refused",
                None,
                None,
            )
            .await;
            return outcome;
        }
        Err(e) => {
            outcome.reachable = Some(false);
            outcome.message = Some(e);
            note(
                db,
                name,
                model,
                started.elapsed().as_millis() as u64,
                "error",
                None,
                None,
            )
            .await;
            return outcome;
        }
    }

    // (c) A tool call for `sql_explain`, and (d) JSON in the shape it was asked for. A call
    // that fails carries its reason, so the status says why it is not usable.
    let mut detail: Option<String> = None;
    match send(client, &base, key, &chat_body(model, fragment(TOOL_CHECK))).await {
        Ok(response) if response.status().is_success() => {
            let body: Value = response.json().await.unwrap_or(Value::Null);
            outcome.tool_calling = Some(
                body.pointer("/choices/0/message/tool_calls/0/function/name")
                    .and_then(Value::as_str)
                    == Some("sql_explain"),
            );
        }
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            outcome.tool_calling = Some(false);
            detail = Some(refusal(status, &body));
        }
        Err(e) => {
            outcome.tool_calling = Some(false);
            detail = Some(e);
        }
    }

    match send(client, &base, key, &chat_body(model, fragment(JSON_CHECK))).await {
        Ok(response) if response.status().is_success() => {
            let body: Value = response.json().await.unwrap_or(Value::Null);
            let content = body
                .pointer("/choices/0/message/content")
                .and_then(Value::as_str)
                .unwrap_or_default();
            outcome.json_output = Some(matches!(
                serde_json::from_str::<Value>(content),
                Ok(Value::Object(object)) if object.contains_key("sql") && object.contains_key("explanation")
            ));
        }
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            outcome.json_output = Some(false);
            detail.get_or_insert_with(|| refusal(status, &body));
        }
        Err(e) => {
            outcome.json_output = Some(false);
            detail.get_or_insert(e);
        }
    }

    // One sentence naming the first failure, when there is one: the reading order the
    // caller needs, not a list of everything the model cannot do.
    let first_failure = match (outcome.tool_calling, outcome.json_output) {
        (Some(true), Some(true)) => None,
        (Some(true), _) => Some("the endpoint answers but returned no JSON"),
        _ => Some("the endpoint answers but returned no tool call"),
    };
    outcome.message = match (first_failure, detail) {
        (None, _) => None,
        (Some(sentence), None) => Some(sentence.to_string()),
        (Some(sentence), Some(detail)) => Some(format!("{sentence}: {detail}")),
    };
    outcome
}

/// `NOVA_MODEL_PROBE_SECONDS`, clamped to [`MIN_INTERVAL`]..=[`MAX_INTERVAL`]; a value that
/// is not a number is the default, so a typo cannot stop the probe.
fn interval_from_env(raw: Option<&str>) -> Duration {
    let secs = raw
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_INTERVAL);
    secs.clamp(
        Duration::from_secs(MIN_INTERVAL),
        Duration::from_secs(MAX_INTERVAL),
    )
}

/// Start the probe: one round per [`ROUND_EVERY`] for as long as the process runs, each round
/// probing only the endpoints that are new or older than the interval. Answers whether a task
/// was started: demo mode and a Nova without a cluster client have none.
pub(crate) fn spawn_loop(state: Arc<AppState>) -> bool {
    if state.demo || state.kube.is_none() {
        info!(
            demo = state.demo,
            "model probe off — a ModelEndpoint's status stays as the last writer left it"
        );
        return false;
    }
    let interval = interval_from_env(std::env::var("NOVA_MODEL_PROBE_SECONDS").ok().as_deref());
    info!(
        interval_secs = interval.as_secs(),
        round_every_secs = ROUND_EVERY.as_secs(),
        "model probe started"
    );
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(ROUND_EVERY).await;
            round(&state, interval).await;
        }
    });
    true
}

/// One round: every endpoint whose status is missing or older than `max_age`, one at a time.
/// The round never ends because an endpoint is slow or dead, and never starts over because
/// the list is empty — an endpoint may be created later.
async fn round(state: &Arc<AppState>, max_age: Duration) {
    let Some(kube) = state.kube.clone() else {
        return;
    };
    let api = crate::model_endpoints::endpoints_api(kube);
    let objects = match api.list(&kube::api::ListParams::default()).await {
        Ok(objects) => objects,
        Err(e) => {
            warn!(error = %e, "model probe: the endpoint list failed — trying again next round");
            return;
        }
    };
    let client = match crate::add_platform_ca(reqwest::Client::builder())
        .and_then(|builder| Ok(builder.timeout(CALL_TIMEOUT).build()?))
    {
        Ok(client) => client,
        Err(e) => {
            warn!(error = %e, "model probe: no HTTP client — trying again next round");
            return;
        }
    };
    let now = now();
    let dir = keys_dir();
    for object in &objects {
        let name = object
            .metadata
            .name
            .as_deref()
            .unwrap_or_default()
            .to_string();
        if name.is_empty() {
            continue;
        }
        let checked_at = object
            .data
            .pointer("/status/checkedAt")
            .and_then(Value::as_str);
        if !needs_probe(checked_at, now, max_age) {
            continue;
        }
        let url = object
            .data
            .pointer("/spec/url")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let model = object
            .data
            .pointer("/spec/model")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if url.is_empty() || model.is_empty() {
            warn!(endpoint = %name, "model probe: no url or model — skipped");
            continue;
        }
        let key = key_at(&dir, &name);
        let outcome = probe(
            &client,
            &name,
            url,
            model,
            key.as_deref(),
            state.db.as_ref(),
        )
        .await;
        info!(
            endpoint = %name,
            reachable = ?outcome.reachable,
            tool_calling = ?outcome.tool_calling,
            json_output = ?outcome.json_output,
            "model probe: one endpoint checked"
        );
        let patch = kube::api::Patch::Merge(outcome.patch());
        if let Err(e) = api
            .patch_status(&name, &kube::api::PatchParams::default(), &patch)
            .await
        {
            warn!(endpoint = %name, error = %e, "model probe: the status patch failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::State,
        routing::{get, post},
    };
    use chrono::TimeZone;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn needs_probe_only_when_there_is_no_recent_answer() {
        let now = Utc.with_ymd_and_hms(2026, 11, 15, 12, 0, 0).unwrap();
        let day = Duration::from_secs(86_400);
        assert!(needs_probe(None, now, day));
        assert!(!needs_probe(Some("2026-11-15T11:00:00Z"), now, day));
        assert!(needs_probe(Some("2026-11-14T11:00:00Z"), now, day));
        assert!(needs_probe(Some("not a date"), now, day));
    }

    #[test]
    fn the_key_file_is_trimmed_and_a_missing_one_is_none() {
        let dir = std::env::temp_dir().join(format!("nova-model-keys-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("endpoint")).unwrap();
        std::fs::write(dir.join("endpoint").join("key"), "  sk-1 \n").unwrap();
        assert_eq!(key_at(&dir, "endpoint"), Some("sk-1".to_string()));
        assert_eq!(key_at(&dir, "nothing-projected"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A mock endpoint: `/models` names a context window, `/chat/completions` answers the
    /// shape the body asked for.
    struct Fake {
        tool_calls: bool,
        asked: Mutex<Vec<String>>,
    }

    async fn serve(tool_calls: bool) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let fake = Arc::new(Fake {
            tool_calls,
            asked: Mutex::new(Vec::new()),
        });
        let app = Router::new()
            .route(
                "/v1/models",
                get(|| async { Json(json!({"data": [{"id": "m", "meta": {"n_ctx": 32768}}]})) }),
            )
            .route(
                "/v1/chat/completions",
                post(|State(fake): State<Arc<Fake>>, body: String| async move {
                    let body: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
                    if body.get("tools").is_some() {
                        fake.asked.lock().unwrap().push("tools".into());
                        if fake.tool_calls {
                            return Json(json!({"choices": [{"message": {"role": "assistant", "content": null, "tool_calls": [{"id": "1", "type": "function", "function": {"name": "sql_explain", "arguments": "{\"sql\":\"select 1\"}"}}]}}]}));
                        }
                        return Json(json!({"choices": [{"message": {"role": "assistant", "content": "I cannot call tools."}}]}));
                    }
                    if body.get("response_format").is_some() {
                        fake.asked.lock().unwrap().push("json".into());
                        return Json(json!({"choices": [{"message": {"role": "assistant", "content": "{\"sql\": \"select 1\", \"explanation\": \"one row\"}"}}]}));
                    }
                    fake.asked.lock().unwrap().push("chat".into());
                    Json(json!({"choices": [{"message": {"role": "assistant", "content": "Lakes hold water."}}]}))
                }),
            )
            .with_state(fake);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base
    }

    #[tokio::test]
    async fn an_endpoint_that_calls_tools_and_returns_json_reports_all_three() {
        let base = serve(true).await;
        let client = reqwest::Client::builder()
            .timeout(CALL_TIMEOUT)
            .build()
            .unwrap();
        let outcome = probe(
            &client,
            "probe-check",
            &format!("{base}/v1/"),
            "m",
            None,
            None,
        )
        .await;
        assert_eq!(outcome.reachable, Some(true));
        assert_eq!(outcome.tool_calling, Some(true));
        assert_eq!(outcome.json_output, Some(true));
        assert_eq!(outcome.context_tokens, Some(32_768));
        assert_eq!(outcome.message, None);
        assert!(outcome.checked_at.ends_with('Z'), "{}", outcome.checked_at);
    }

    #[tokio::test]
    async fn a_dead_endpoint_is_unreachable_with_a_message() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let client = reqwest::Client::builder()
            .timeout(CALL_TIMEOUT)
            .build()
            .unwrap();
        let outcome = probe(
            &client,
            "probe-check",
            &format!("http://127.0.0.1:{port}/v1"),
            "m",
            None,
            None,
        )
        .await;
        assert_eq!(outcome.reachable, Some(false));
        assert!(outcome.message.is_some());
        assert_eq!(outcome.tool_calling, None);
        assert_eq!(outcome.json_output, None);
        assert!(outcome.checked_at.ends_with('Z'));
    }

    #[tokio::test]
    async fn an_answer_without_a_tool_call_is_not_tool_calling() {
        let base = serve(false).await;
        let client = reqwest::Client::builder()
            .timeout(CALL_TIMEOUT)
            .build()
            .unwrap();
        let outcome = probe(
            &client,
            "probe-check",
            &format!("{base}/v1"),
            "m",
            None,
            None,
        )
        .await;
        assert_eq!(outcome.reachable, Some(true));
        assert_eq!(outcome.tool_calling, Some(false));
        assert_eq!(outcome.json_output, Some(true));
        assert_eq!(
            outcome.message.as_deref(),
            Some("the endpoint answers but returned no tool call")
        );
    }
}
