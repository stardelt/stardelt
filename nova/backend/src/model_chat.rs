//! One client for every chat call Nova makes to a model endpoint (design nova#890, slice 2).
//!
//! The probe, Ask and the agent proxy all send the same request shape — the endpoint's own
//! base URL, its model, its key, the messages, and whatever a feature adds to the body
//! (`tools`, `response_format`) — so they share this one implementation. Two rules that the
//! rest of the module relies on:
//!
//! - **The URL never leaves this module.** A transport error is reported with
//!   `without_url()`, so no error, log line or `status.message` carries the internal
//!   hostname (nova#990).
//! - **The answer is returned as sent.** A tool call answers `content: null` and puts the
//!   call in `message.tool_calls`, which is why the caller gets the whole assistant message,
//!   not only its text.

use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};

/// Where the kubelet projects each endpoint's key (`stardelt-operator` `nova.rs`
/// `MODEL_KEYS_PATH`, and the chart's `mountPath`). Nova never reads a Secret.
const MODEL_KEYS_PATH: &str = "/var/run/stardelt/models";
/// A chat call's token cap: thinking mode eats an uncapped budget (platform#1099).
pub(crate) const MAX_TOKENS: u32 = 512;
/// One call's wall-clock limit; the spike's slowest answer was 91 s on a CPU model.
pub(crate) const CALL_TIMEOUT: Duration = Duration::from_secs(120);
/// How much of an error body a message may carry.
const BODY_CHARS: usize = 200;

/// One chat call: the endpoint's own base URL, its model, its key, the messages, and an
/// optional fragment merged into the body (`tools`, `response_format`).
pub(crate) struct Chat<'a> {
    pub base_url: &'a str,
    pub model: &'a str,
    pub key: Option<&'a str>,
    pub messages: Value,
    pub extra: Value,
}

/// What one call answered.
pub(crate) struct Answer {
    /// `choices[0].message.content`; empty when the endpoint answered with a tool call
    /// (`content: null`), which is the probe's tool-check case.
    pub content: String,
    /// `choices[0].message` as sent. The probe's tool check reads
    /// `/tool_calls/0/function/name` here — a tool call carries no `content`, so the
    /// decision cannot be made from `content`.
    pub message: Value,
    /// The endpoint's `usage` object, when it sent one.
    pub usage: Option<Value>,
    pub latency_ms: u64,
}

/// Why a call did not answer.
pub(crate) enum ChatError {
    /// The endpoint refused: `503: <first 200 characters of the body>` — the status is
    /// already in this string.
    Refused(String),
    /// No answer at all (DNS, TLS, timeout); the message has no URL.
    Transport(String),
}

/// The endpoint's base URL without its trailing slash: the CRD's example is
/// `https://llm.internal.example/v1`, so the calls below append `/models` and
/// `/chat/completions`.
pub(crate) fn base_url(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

/// The first `n` characters of a body, never a multi-byte boundary panic.
fn short(body: &str) -> String {
    body.chars().take(BODY_CHARS).collect()
}

/// Where the projected keys live; `NOVA_MODEL_KEYS_DIR` is the test seam. The probe's loop
/// reads it too, so this stays the one definition.
pub(crate) fn keys_dir() -> std::path::PathBuf {
    match std::env::var("NOVA_MODEL_KEYS_DIR") {
        Ok(dir) if !dir.trim().is_empty() => std::path::PathBuf::from(dir),
        _ => std::path::PathBuf::from(MODEL_KEYS_PATH),
    }
}

/// The endpoint's key, or `None` when nothing was projected for it (or it is unreadable).
fn key_at(dir: &Path, name: &str) -> Option<String> {
    let key = std::fs::read_to_string(dir.join(name).join("key")).ok()?;
    let key = key.trim();
    (!key.is_empty()).then(|| key.to_string())
}

/// The key projected for the endpoint named `name`.
pub(crate) fn key_for(name: &str) -> Option<String> {
    key_at(&keys_dir(), name)
}

/// One chat call. The body is `model`, `messages`, `max_tokens` and every top-level key of
/// `extra`; a non-2xx answer is a [`ChatError::Refused`], no answer at all a
/// [`ChatError::Transport`] without the URL.
pub(crate) async fn chat(client: &reqwest::Client, call: &Chat<'_>) -> Result<Answer, ChatError> {
    let mut body = json!({
        "model": call.model,
        "max_tokens": MAX_TOKENS,
        "messages": call.messages,
    });
    if let (Some(object), Some(extra)) = (body.as_object_mut(), call.extra.as_object()) {
        for (key, value) in extra {
            object.insert(key.clone(), value.clone());
        }
    }
    let mut request = client
        .post(format!("{}/chat/completions", base_url(call.base_url)))
        .json(&body);
    if let Some(key) = call.key {
        request = request.bearer_auth(key);
    }
    let started = std::time::Instant::now();
    let response = request
        .send()
        .await
        .map_err(|e| ChatError::Transport(e.without_url().to_string()))?;
    let latency_ms = started.elapsed().as_millis() as u64;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(ChatError::Refused(format!(
            "{}: {}",
            status.as_u16(),
            short(&body)
        )));
    }
    let body: Value = response.json().await.unwrap_or(Value::Null);
    let message = body
        .pointer("/choices/0/message")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let content = message
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Ok(Answer {
        content,
        message,
        usage: body.get("usage").cloned(),
        latency_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::post};
    use std::sync::Arc;

    /// A server that answers the given body with the given status.
    async fn serve(status: u16, body: Value) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route(
                "/v1/chat/completions",
                post(move || {
                    let body = body.clone();
                    async move {
                        (
                            axum::http::StatusCode::from_u16(status).unwrap(),
                            Json(body),
                        )
                    }
                }),
            )
            .with_state(Arc::new(()));
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        base
    }

    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(CALL_TIMEOUT)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn an_answer_carries_its_text_message_and_usage() {
        let base = serve(
            200,
            json!({
                "choices": [{"message": {"role": "assistant", "content": "Lakes hold water."}}],
                "usage": {"prompt_tokens": 7, "completion_tokens": 2},
            }),
        )
        .await;
        let answer = chat(
            &client(),
            &Chat {
                base_url: &format!("{base}/v1/"),
                model: "m",
                key: None,
                messages: json!([{"role": "user", "content": "hi"}]),
                extra: json!({}),
            },
        )
        .await
        .ok()
        .expect("an answer");
        assert_eq!(answer.content, "Lakes hold water.");
        assert_eq!(
            answer.message.pointer("/role").and_then(Value::as_str),
            Some("assistant")
        );
        assert_eq!(
            answer
                .usage
                .as_ref()
                .and_then(|u| u.get("prompt_tokens"))
                .and_then(Value::as_i64),
            Some(7)
        );
    }

    /// A tool call answers `content: null`; the call itself is only in the message.
    #[tokio::test]
    async fn a_tool_call_has_no_text_but_a_message() {
        let base = serve(
            200,
            json!({"choices": [{"message": {"role": "assistant", "content": null,
                "tool_calls": [{"id": "1", "type": "function",
                    "function": {"name": "sql_explain", "arguments": "{}"}}]}}]}),
        )
        .await;
        let answer = chat(
            &client(),
            &Chat {
                base_url: &format!("{base}/v1"),
                model: "m",
                key: None,
                messages: json!([]),
                extra: json!({}),
            },
        )
        .await
        .ok()
        .expect("an answer");
        assert_eq!(answer.content, "");
        assert_eq!(
            answer
                .message
                .pointer("/tool_calls/0/function/name")
                .and_then(Value::as_str),
            Some("sql_explain")
        );
    }

    /// A refusal names the status and how much of the body, and no URL.
    #[tokio::test]
    async fn a_refusal_carries_the_status_and_the_body() {
        let base = serve(503, json!({"error": "overloaded"})).await;
        let error = chat(
            &client(),
            &Chat {
                base_url: &format!("{base}/v1"),
                model: "m",
                key: None,
                messages: json!([]),
                extra: json!({}),
            },
        )
        .await
        .err()
        .expect("a refusal");
        match error {
            ChatError::Refused(message) => {
                assert!(message.starts_with("503: "), "{message}");
                assert!(message.contains("overloaded"), "{message}");
                assert!(!message.contains("http://"), "{message}");
            }
            ChatError::Transport(message) => panic!("expected a refusal, got {message}"),
        }
    }

    /// A transport error carries no URL and no port (nova#990).
    #[tokio::test]
    async fn a_transport_error_carries_no_url() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let error = chat(
            &client(),
            &Chat {
                base_url: &format!("http://127.0.0.1:{port}/v1"),
                model: "m",
                key: None,
                messages: json!([]),
                extra: json!({}),
            },
        )
        .await
        .err()
        .expect("a transport error");
        let ChatError::Transport(message) = error else {
            panic!("expected a transport error");
        };
        assert!(!message.contains("http://"), "{message}");
        assert!(!message.contains(&port.to_string()), "{message}");
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
}
