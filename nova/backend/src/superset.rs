//! Superset's dashboard API, read half: the ZIP Superset exports for one dashboard (#115).
//!
//! A bundle is the platform's answer to "dashboards in Git": Nova exports a dashboard, the
//! bundle carries it, and the import slice puts it back. This module speaks the export half
//! and nothing else.
//!
//! Two facts the spike (#1210) measured decide the design, and they are not guessable from
//! Superset's documentation:
//!
//! - with `Authorization: Bearer <access_token>` the GETs work over plain http to the
//!   cluster Service (`login 200`, `csrf 200`, `export 200`, 18444 bytes);
//! - the CSRF/session **POST** does not, because Superset sets `SESSION_COOKIE_SECURE`
//!   whenever `STARDELT_PUBLIC_URL` is https, and a `Secure` cookie is not stored over
//!   http (`302 → /login/`). The import slice needs a session, so Nova builds its API
//!   base as the **public https URL** when the platform has a base domain (see
//!   `AppState::superset_api_url`); that certificate is public, so no CA is added for it.
//!
//! The token lives a short while, so [`SupersetClient::export`] repeats once after a `401`
//! with a fresh sign-in (measured: the expired token answers `401`, a new one works).
//!
//! Nova acts as its own Superset service user ([`SERVICE_USER`]): it could export any
//! dashboard, so the route is administrator-only. A bundle author's narrower rule belongs
//! with the import slice, which reads the bundle's owner group.
//!
//! **No password and no token ever reach a log line, an error string or a `Debug`.** The
//! password is held in [`crate::auth::Secret`] (`[redacted]`), every upstream message is
//! scrubbed before it is stored, and no token-holding type derives `Debug`.

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::AppState;
use crate::auth::Secret;

/// The Superset user Nova signs in as, unless `NOVA_SUPERSET_USER` names another.
pub const SERVICE_USER: &str = "stardelt-nova";

/// The sentence an installation without the service user answers with.
const NO_SERVICE_USER: &str = "this instance has no Superset service user; run scripts/app-secrets.sh, then make step-superset";

/// Longest upstream sentence Nova repeats; the rest is cut.
const MAX_DETAIL: usize = 200;

/// A call that has not answered within this time is a failure, not a hanging screen.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// The Superset account Nova acts as.
pub struct ServiceUser {
    pub username: String,
    /// `Debug` prints `[redacted]` (see [`Secret`]).
    pub password: Secret,
}

/// `NOVA_SUPERSET_PASSWORD` (non-empty) is required; `NOVA_SUPERSET_USER` defaults to
/// [`SERVICE_USER`]. `None` when the password is unset or empty: an install without
/// `scripts/app-secrets.sh` simply answers [`SupersetError::NoServiceUser`].
pub fn service_user() -> Option<ServiceUser> {
    service_user_with(|key| std::env::var(key).ok())
}

fn service_user_with(get: impl Fn(&str) -> Option<String>) -> Option<ServiceUser> {
    let password = get("NOVA_SUPERSET_PASSWORD")?;
    if password.is_empty() {
        return None;
    }
    let username = get("NOVA_SUPERSET_USER")
        .map(|user| user.trim().to_string())
        .filter(|user| !user.is_empty())
        .unwrap_or_else(|| SERVICE_USER.to_string());
    Some(ServiceUser {
        username,
        password: Secret::new(password),
    })
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why a call failed. `Display` and `Debug` carry neither the password nor a token: any
/// upstream text is scrubbed and cut before it is stored here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupersetError {
    /// `NOVA_SUPERSET_PASSWORD` is not set.
    NoServiceUser,
    /// Superset's own status code and `message`, scrubbed and cut to [`MAX_DETAIL`] chars.
    Status(u16, String),
    /// Connection failed, timed out, or the answer was not the JSON expected.
    Transport(String),
}

impl std::fmt::Display for SupersetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SupersetError::NoServiceUser => write!(f, "{NO_SERVICE_USER}"),
            SupersetError::Status(code, message) if message.is_empty() => {
                write!(f, "Superset answered {code}")
            }
            SupersetError::Status(code, message) => {
                write!(f, "Superset answered {code}: {message}")
            }
            SupersetError::Transport(_) => write!(f, "Superset is unreachable"),
        }
    }
}

impl std::error::Error for SupersetError {}

impl IntoResponse for SupersetError {
    fn into_response(self) -> Response {
        match self {
            SupersetError::NoServiceUser => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": NO_SERVICE_USER })),
            )
                .into_response(),
            SupersetError::Status(404, message) => {
                (StatusCode::NOT_FOUND, Json(json!({ "error": message }))).into_response()
            }
            SupersetError::Status(code, message) if message.is_empty() => (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": format!("Superset answered {code}") })),
            )
                .into_response(),
            SupersetError::Status(code, message) => (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": format!("Superset answered {code}: {message}") })),
            )
                .into_response(),
            SupersetError::Transport(detail) => {
                // The detail can name the URL and the system's error; it never holds a
                // credential. It goes to the log, the answer stays one sentence.
                tracing::warn!(reason = %detail, "Superset export failed");
                (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({ "error": "Superset is unreachable" })),
                )
                    .into_response()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The client
// ---------------------------------------------------------------------------

/// The Superset API base and the service user Nova signs in as. One is built per request:
/// the sign-in token is short-lived and a bundle export is a rare, deliberate action.
pub struct SupersetClient {
    http: reqwest::Client,
    base: String,
    user: ServiceUser,
}

impl SupersetClient {
    /// `base` is the public https URL (see the module doc); a trailing slash is dropped.
    pub fn new(base: &str, user: ServiceUser) -> Self {
        // `NOVA_TRINO_CA_PEM` was already validated when the process started (`http_client`).
        // A builder that refuses here leaves the request on the system roots rather than
        // panicking in a handler.
        let http = crate::add_platform_ca(reqwest::Client::builder().cookie_store(true))
            .and_then(|builder| {
                builder
                    .timeout(REQUEST_TIMEOUT)
                    .build()
                    .map_err(|e| anyhow::anyhow!("Superset client: {e}"))
            })
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            http,
            base: base.trim().trim_end_matches('/').to_string(),
            user,
        }
    }

    /// `POST /api/v1/security/login`, answering the `access_token`. The password travels in
    /// the body and nowhere else; neither it nor the token is logged.
    async fn login(&self) -> Result<String, SupersetError> {
        let response = self
            .http
            .post(format!("{}/api/v1/security/login", self.base))
            .json(&json!({
                "username": self.user.username,
                "password": self.user.password.expose(),
                "provider": "db",
                "refresh": true,
            }))
            .send()
            .await
            .map_err(|e| SupersetError::Transport(e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(self.status_error(status, response, "").await);
        }
        let body: Value = response
            .json()
            .await
            .map_err(|e| SupersetError::Transport(e.to_string()))?;
        body["access_token"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| {
                SupersetError::Transport("Superset's sign-in answer carries no access_token".into())
            })
    }

    /// The ZIP archive Superset exports for one dashboard, exactly as Superset sends it
    /// (the `databases/` folder included).
    ///
    /// A `401` means the token from this call expired; Nova signs in once more and repeats
    /// the GET once. Every other non-`200` is Superset's status and message.
    pub async fn export(&self, id: i64) -> Result<Vec<u8>, SupersetError> {
        let url = format!("{}/api/v1/dashboard/export/?q=[{id}]", self.base);
        let mut token = self.login().await?;
        let mut response = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| SupersetError::Transport(e.to_string()))?;
        if response.status() == StatusCode::UNAUTHORIZED {
            token = self.login().await?;
            response = self
                .http
                .get(&url)
                .bearer_auth(&token)
                .send()
                .await
                .map_err(|e| SupersetError::Transport(e.to_string()))?;
        }
        let status = response.status();
        if status == StatusCode::OK {
            return response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|e| SupersetError::Transport(e.to_string()));
        }
        if status == StatusCode::NOT_FOUND {
            // The handler knows the id; the answer must name it.
            return Err(SupersetError::Status(
                404,
                format!("Superset has no dashboard {id}"),
            ));
        }
        Err(self.status_error(status, response, &token).await)
    }

    /// Superset's status and its JSON `message` (when there is one), scrubbed of the
    /// password and of `token`, cut to [`MAX_DETAIL`] characters.
    async fn status_error(
        &self,
        status: StatusCode,
        response: reqwest::Response,
        token: &str,
    ) -> SupersetError {
        let body = response.text().await.unwrap_or_default();
        let message = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|body| body["message"].as_str().map(str::to_string))
            .unwrap_or_default();
        SupersetError::Status(status.as_u16(), self.clean(&message, token))
    }

    /// Removes the two canaries an upstream could repeat (the measured `401` body once
    /// echoed the submitted value) and cuts the sentence at a character boundary.
    fn clean(&self, message: &str, token: &str) -> String {
        let mut message = message.replace(self.user.password.expose(), "[redacted]");
        if !token.is_empty() {
            message = message.replace(token, "[redacted]");
        }
        message.chars().take(MAX_DETAIL).collect()
    }
}

// ---------------------------------------------------------------------------
// The route
// ---------------------------------------------------------------------------

/// `GET /api/admin/dashboards/{id}/export`: the ZIP Superset exports for one dashboard,
/// exactly as Superset sends it (including its `databases/` folder). Administrator-only.
/// Nova acts as its own Superset service user (`stardelt-nova`).
#[utoipa::path(
    get,
    path = "/api/admin/dashboards/{id}/export",
    tag = "internal",
    description = "The ZIP archive Superset exports for one dashboard, exactly as Superset sends it (including its `databases/` folder). Administrator-only. Nova acts as its own Superset service user (`stardelt-nova`).",
    params(
        ("id" = i64, Path, description = "Superset's numeric dashboard id"),
    ),
    responses(
        (status = 200, description = "The ZIP archive Superset exports for one dashboard", body = String, content_type = "application/zip"),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller is not a platform administrator", body = crate::openapi::ErrorResponse),
        (status = 404, description = "Superset has no such dashboard", body = crate::openapi::ErrorResponse),
        (status = 502, description = "Superset answered an error, or could not be reached", body = crate::openapi::ErrorResponse),
        (status = 503, description = "This instance has no Superset service user", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn export_dashboard(State(state): State<Arc<AppState>>, Path(id): Path<i64>) -> Response {
    // The admin table is mounted behind `auth::require_admin` (`main.rs`), so the handler
    // carries no permission check of its own.
    let Some(user) = state.superset.as_ref() else {
        return SupersetError::NoServiceUser.into_response();
    };
    let user = ServiceUser {
        username: user.username.clone(),
        password: Secret::new(user.password.expose().to_string()),
    };
    match SupersetClient::new(&state.superset_api_url, user)
        .export(id)
        .await
    {
        Ok(bytes) => {
            let mut response = bytes.into_response();
            let headers = response.headers_mut();
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/zip"),
            );
            if let Ok(disposition) =
                HeaderValue::from_str(&format!("attachment; filename=\"dashboard-{id}.zip\""))
            {
                headers.insert(header::CONTENT_DISPOSITION, disposition);
            }
            response
        }
        Err(error) => error.into_response(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod fake {
    //! The three Superset routes the client uses. The password it accepts and the token it
    //! hands out are canaries: neither may ever show up in an error.

    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use axum::{
        Json, Router,
        body::Body,
        extract::{Query, State},
        http::{HeaderMap, StatusCode, header},
        response::{IntoResponse, Response},
        routing::{get, post},
    };
    use serde_json::{Value, json};

    use super::{SERVICE_USER, ServiceUser, SupersetClient};
    use crate::auth::Secret;

    pub(super) const PASSWORD: &str = "pw-canary-4e7b";
    pub(super) const TOKEN: &str = "tok-canary-4e7b";
    pub(super) const ARCHIVE: &[u8] = b"PK\x03\x04canary";

    #[derive(Default)]
    pub(super) struct Fake {
        /// How many sign-in calls arrived.
        pub(super) logins: usize,
        /// How many more `401` answers to serve on an export before accepting the token.
        pub(super) unauthorized: usize,
        /// Answer every export with a `500` and a talkative message that repeats the
        /// password it was sent.
        pub(super) failing: bool,
    }

    pub(super) type Shared = Arc<Mutex<Fake>>;

    fn refused() -> Response {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "message": "Invalid credentials" })),
        )
            .into_response()
    }

    async fn login(State(s): State<Shared>, Json(body): Json<Value>) -> Response {
        let mut state = s.lock().unwrap();
        state.logins += 1;
        if body["password"].as_str() == Some(PASSWORD) {
            return (StatusCode::OK, Json(json!({ "access_token": TOKEN }))).into_response();
        }
        refused()
    }

    async fn export_one(
        State(s): State<Shared>,
        Query(query): Query<BTreeMap<String, String>>,
        headers: HeaderMap,
    ) -> Response {
        let mut state = s.lock().unwrap();
        if state.unauthorized > 0 {
            state.unauthorized -= 1;
            return refused();
        }
        if state.failing {
            // Deliberately chatty: it repeats what was sent, so a client that copies the
            // body into an error would print the credential.
            let sent = headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": format!("boom for {sent} {PASSWORD}") })),
            )
                .into_response();
        }
        let expected = format!("Bearer {TOKEN}");
        if headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            != Some(expected.as_str())
        {
            return refused();
        }
        if query.get("q").map(String::as_str) != Some("[7]") {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "message": "Not found" })),
            )
                .into_response();
        }
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/zip")],
            Body::from(ARCHIVE),
        )
            .into_response()
    }

    /// Start the fake and a client signed in against it.
    pub(super) async fn start() -> (SupersetClient, Shared) {
        let shared: Shared = Arc::new(Mutex::new(Fake::default()));
        let app = Router::new()
            .route("/api/v1/security/login", post(login))
            .route("/api/v1/dashboard/export/", get(export_one))
            .with_state(Arc::clone(&shared));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let user = ServiceUser {
            username: SERVICE_USER.to_string(),
            password: Secret::new(PASSWORD),
        };
        (SupersetClient::new(&format!("http://{addr}"), user), shared)
    }
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::http::StatusCode;

    use super::fake::{self, ARCHIVE, PASSWORD, TOKEN};
    use super::*;

    /// Serialises the tests that set/remove `NOVA_SUPERSET_*`: the environment is
    /// process-global. Tokio's mutex, because the guard is held across an await.
    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn export_returns_the_archive_bytes() {
        let (client, _fake) = fake::start().await;
        let bytes = client.export(7).await.unwrap();
        assert_eq!(bytes, ARCHIVE);
    }

    #[tokio::test]
    async fn an_expired_token_is_refreshed_once() {
        let (client, fake) = fake::start().await;
        fake.lock().unwrap().unauthorized = 1;
        let bytes = client.export(7).await.unwrap();
        assert_eq!(bytes, ARCHIVE);
        assert_eq!(
            fake.lock().unwrap().logins,
            2,
            "the 401 costs exactly one new sign-in"
        );
    }

    #[tokio::test]
    async fn no_service_user_answers_the_sentence() {
        let _env = ENV_LOCK.lock().await;
        unsafe {
            std::env::remove_var("NOVA_SUPERSET_PASSWORD");
        }
        assert!(service_user().is_none(), "an unset password means no user");

        let response = SupersetError::NoServiceUser.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(
            body.contains("this instance has no Superset service user"),
            "the answer names the fix: {body}"
        );
    }

    #[tokio::test]
    async fn an_error_never_contains_the_password_or_the_token() {
        let (client, fake) = fake::start().await;
        fake.lock().unwrap().failing = true;
        let error = client.export(7).await.unwrap_err();
        let display = error.to_string();
        let debug = format!("{error:?}");
        for text in [&display, &debug] {
            assert!(!text.contains(PASSWORD), "the password reached: {text}");
            assert!(!text.contains(TOKEN), "the token reached: {text}");
        }
        assert!(
            display.contains("500"),
            "the upstream status stays in the sentence: {display}"
        );

        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(!body.contains(PASSWORD), "the password reached: {body}");
        assert!(!body.contains(TOKEN), "the token reached: {body}");
    }
}
