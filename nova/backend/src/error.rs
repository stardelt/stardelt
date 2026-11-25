//! The one error answer of the API: a status and one sentence, `{"error": "<sentence>"}` — the
//! `ErrorResponse` schema in `openapi.rs`. The sentence never contains a value, a token, a URL or
//! an upstream body.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};

/// One non-2xx answer: a status and a sentence. The sentence never contains a value.
#[derive(Debug)]
pub(crate) struct Fail {
    status: StatusCode,
    message: String,
}

impl Fail {
    pub(crate) fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    /// The status, for a caller that inspects a refusal without serialising it (the pipeline
    /// loader tells a hidden pipeline's 404 from a viewer's 403 in the audit line).
    pub(crate) fn status(&self) -> StatusCode {
        self.status
    }

    /// The sentence of a refusal.
    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

impl IntoResponse for Fail {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.message })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_fail_is_the_error_envelope() {
        let response = Fail::new(StatusCode::NOT_FOUND, "no such thing").into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers()["content-type"],
            "application/json",
            "errors are JSON"
        );
        let bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(&bytes[..], br#"{"error":"no such thing"}"#);
    }
}
