//! Superset's dashboard API for bundles: the ZIP Superset exports for one dashboard, and the
//! import client that puts one back (#115).
//!
//! A bundle is the platform's answer to "dashboards in Git": Nova exports a dashboard, the
//! bundle carries it, and the import slice puts it back. This module speaks both halves of
//! the Superset conversation; the bundle's own rules are not decided here.
//!
//! Facts the spike (#1210) measured decide the design, and they are not guessable from
//! Superset's documentation:
//!
//! - with `Authorization: Bearer <access_token>` the GETs work over plain http to the
//!   cluster Service (`login 200`, `csrf 200`, `export 200`, 18444 bytes);
//! - the import is a **session POST**: `login` → `GET /api/v1/security/csrf_token/` →
//!   `POST /api/v1/dashboard/import/` with the ZIP as the multipart field `formData`,
//!   `overwrite` as a form field, and the `X-CSRFToken` and `Referer` headers. A POST
//!   without them fails;
//! - the POST does not work over plain http either, because Superset sets
//!   `SESSION_COOKIE_SECURE` whenever `STARDELT_PUBLIC_URL` is https, and a `Secure`
//!   cookie is not stored over http (`302 → /login/`). That is why Nova builds its API
//!   base as the **public https URL** when the platform has a base domain (see
//!   `AppState::superset_api_url`); that certificate is public, so no CA is added for it.
//!
//! The client uses five Superset routes: `login`, the dashboard `export`, the dashboard
//! `import`, the database list (`GET /api/v1/database/`) and the database export
//! (`GET /api/v1/database/export/`). The import route rewrites an exported dashboard so that
//! every dataset points at the target's own `Trino <catalog>` database (#1216): the list finds
//! that database by name, its export carries the uuid the rewrite writes (#115 a/3b).
//!
//! The token lives a short while, so every GET the client sends ([`SupersetClient::export`],
//! [`SupersetClient::databases`], [`SupersetClient::database_export`]) and
//! [`SupersetClient::import`] repeat once after a `401` with a fresh sign-in (measured: the
//! expired token answers `401`, a new one works).
//!
//! Nova acts as its own Superset service user ([`SERVICE_USER`]): it could export or import
//! any dashboard, so the routes are administrator-only. The bundle's own rules — the owner
//! group guard, the synthesized `databases/` folder (an archive without one imports `200`
//! and links none of its charts), the provenance stamp and the prune — belong to the import
//! route (#115 a/3), not here.
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

/// One database the target Superset knows, as the import route needs it: the id to export and
/// the name (`Trino Gold`) to match a dataset's catalog against.
// The import route that reads this is #115 a/3b2.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupersetDatabase {
    pub id: i64,
    pub name: String,
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

    /// `GET /api/v1/database/?q=(page_size:200)`: every database the target Superset knows, as
    /// `(id, database_name)`. A row without a numeric `id` or a string `database_name` is
    /// skipped; an answer without a `result` array is `Transport`.
    ///
    /// A `401` means the token from this call expired; Nova signs in once more and repeats the
    /// GET once. Every other non-`200` is Superset's status and message.
    // The import route that calls this is #115 a/3b2.
    #[allow(dead_code)]
    pub async fn databases(&self) -> Result<Vec<SupersetDatabase>, SupersetError> {
        let url = format!("{}/api/v1/database/?q=(page_size:200)", self.base);
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
        if status != StatusCode::OK {
            return Err(self.status_error(status, response, &token).await);
        }
        let body: Value = response
            .json()
            .await
            .map_err(|e| SupersetError::Transport(e.to_string()))?;
        let Some(rows) = body["result"].as_array() else {
            return Err(SupersetError::Transport(
                "Superset's database list carries no result".into(),
            ));
        };
        Ok(rows
            .iter()
            .filter_map(|row| {
                Some(SupersetDatabase {
                    id: row["id"].as_i64()?,
                    name: row["database_name"].as_str()?.to_string(),
                })
            })
            .collect())
    }

    /// `GET /api/v1/database/export/?q=[{id}]`: the ZIP Superset exports for one database
    /// (a `databases` member inside). A `404` names the id; every other non-`200` is
    /// Superset's status and message.
    ///
    /// A `401` means the token from this call expired; Nova signs in once more and repeats the
    /// GET once.
    // The import route that calls this is #115 a/3b2.
    #[allow(dead_code)]
    pub async fn database_export(&self, id: i64) -> Result<Vec<u8>, SupersetError> {
        let url = format!("{}/api/v1/database/export/?q=[{id}]", self.base);
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
            // The caller knows the id; the answer must name it.
            return Err(SupersetError::Status(
                404,
                format!("Superset has no database {id}"),
            ));
        }
        Err(self.status_error(status, response, &token).await)
    }

    /// `GET /api/v1/security/csrf_token`, answering `(bearer, csrf)`.
    ///
    /// The CSRF token belongs to the session the sign-in minted, so both come from one call:
    /// a `401` signs in once more and repeats the GET once, and the returned bearer is the
    /// new one the caller must send. An answer without a string `result` is a transport
    /// error, and the token is never stored in an error (see [`Self::clean`]).
    async fn csrf(&self, token: &str) -> Result<(String, String), SupersetError> {
        let url = format!("{}/api/v1/security/csrf_token/", self.base);
        let mut token = token.to_string();
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
        if !status.is_success() {
            return Err(self.status_error(status, response, &token).await);
        }
        let body: Value = response
            .json()
            .await
            .map_err(|e| SupersetError::Transport(e.to_string()))?;
        let csrf = body["result"]
            .as_str()
            .ok_or_else(|| {
                SupersetError::Transport("Superset's CSRF answer carries no result".into())
            })?
            .to_string();
        Ok((token, csrf))
    }

    /// Imports one ZIP bundle: `POST /api/v1/dashboard/import/` with the archive as the
    /// multipart field `formData`, `overwrite` as a form field, and the `X-CSRFToken` and
    /// `Referer` headers the session needs. Answers Superset's JSON body.
    ///
    /// A `401` means the session expired; Nova signs in once more, fetches a fresh CSRF token
    /// and sends the same body once more. Every other non-2xx is Superset's status and
    /// message; the CSRF token is scrubbed like the bearer.
    // The import route that calls this is #115 a/3.
    #[allow(dead_code)]
    pub async fn import(&self, archive: &[u8], overwrite: bool) -> Result<Value, SupersetError> {
        let url = format!("{}/api/v1/dashboard/import/", self.base);
        let referer = format!("{}/", self.base);
        // A `multipart::Form` is consumed by the request, so a retry builds a fresh one.
        let form = || -> Result<reqwest::multipart::Form, SupersetError> {
            let part = reqwest::multipart::Part::bytes(archive.to_vec())
                .file_name("dashboard.zip")
                .mime_str("application/zip")
                .map_err(|e| SupersetError::Transport(e.to_string()))?;
            Ok(reqwest::multipart::Form::new()
                .part("formData", part)
                .text("overwrite", if overwrite { "true" } else { "false" }))
        };
        let (mut token, mut csrf) = {
            let bearer = self.login().await?;
            self.csrf(&bearer).await?
        };
        let send = |token: &str, csrf: &str| -> Result<reqwest::RequestBuilder, SupersetError> {
            Ok(self
                .http
                .post(&url)
                .bearer_auth(token)
                .header("X-CSRFToken", csrf)
                .header(reqwest::header::REFERER, &referer)
                .multipart(form()?))
        };
        let mut response = send(&token, &csrf)?
            .send()
            .await
            .map_err(|e| SupersetError::Transport(e.to_string()))?;
        if response.status() == StatusCode::UNAUTHORIZED {
            let (fresh_token, fresh_csrf) = {
                let bearer = self.login().await?;
                self.csrf(&bearer).await?
            };
            token = fresh_token;
            csrf = fresh_csrf;
            response = send(&token, &csrf)?
                .send()
                .await
                .map_err(|e| SupersetError::Transport(e.to_string()))?;
        }
        let status = response.status();
        if !status.is_success() {
            return Err(self
                .import_status_error(status, response, &token, &csrf)
                .await);
        }
        response
            .json()
            .await
            .map_err(|_| SupersetError::Transport("Superset's import answer is not JSON".into()))
    }

    /// [`Self::status_error`] plus the CSRF token: an import answer that echoes a header must
    /// not store it either.
    async fn import_status_error(
        &self,
        status: StatusCode,
        response: reqwest::Response,
        token: &str,
        csrf: &str,
    ) -> SupersetError {
        let body = response.text().await.unwrap_or_default();
        let message = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|body| body["message"].as_str().map(str::to_string))
            .unwrap_or_default()
            .replace(csrf, "[redacted]");
        SupersetError::Status(status.as_u16(), self.clean(&message, token))
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
    //! The five Superset routes the client uses. The password it accepts and the token it
    //! hands out are canaries: neither may ever show up in an error.

    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use axum::{
        Json, Router,
        body::{Body, Bytes},
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
    pub(super) const CSRF: &str = "csrf-canary";
    pub(super) const ARCHIVE: &[u8] = b"PK\x03\x04canary";
    pub(super) const DATABASE_ARCHIVE: &[u8] = b"PK\x03\x04database-canary";

    #[derive(Default)]
    pub(super) struct Fake {
        /// How many sign-in calls arrived.
        pub(super) logins: usize,
        /// How many more `401` answers to serve on an export before accepting the token.
        pub(super) unauthorized: usize,
        /// How many more `401` answers to serve on an import before accepting the session.
        pub(super) unauthorized_imports: usize,
        /// Answer every export with a `500` and a talkative message that repeats the
        /// password it was sent.
        pub(super) failing: bool,
        /// The headers and the raw body of the last import that arrived.
        pub(super) import_headers: BTreeMap<String, String>,
        pub(super) import_body: Vec<u8>,
    }

    pub(super) type Shared = Arc<Mutex<Fake>>;

    fn refused() -> Response {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "message": "Invalid credentials" })),
        )
            .into_response()
    }

    /// The `Authorization: Bearer <TOKEN>` header, or the value that arrived instead.
    fn bearer(headers: &HeaderMap) -> String {
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string()
    }

    fn has_bearer(headers: &HeaderMap) -> bool {
        bearer(headers) == format!("Bearer {TOKEN}")
    }

    async fn login(State(s): State<Shared>, Json(body): Json<Value>) -> Response {
        let mut state = s.lock().unwrap();
        state.logins += 1;
        if body["password"].as_str() == Some(PASSWORD) {
            return (StatusCode::OK, Json(json!({ "access_token": TOKEN }))).into_response();
        }
        refused()
    }

    async fn csrf_token(headers: HeaderMap) -> Response {
        if !has_bearer(&headers) {
            return refused();
        }
        (StatusCode::OK, Json(json!({ "result": CSRF }))).into_response()
    }

    async fn import_dashboard(
        State(s): State<Shared>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let mut state = s.lock().unwrap();
        state.import_headers = headers
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_string(), value.to_string()))
            })
            .collect();
        state.import_body = body.to_vec();
        if state.unauthorized_imports > 0 {
            state.unauthorized_imports -= 1;
            return refused();
        }
        if !has_bearer(&headers) {
            return refused();
        }
        let csrf = headers
            .get("x-csrftoken")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        if csrf != CSRF {
            return refused();
        }
        if state.failing {
            // Deliberately chatty: it repeats the credentials it was sent, so a client that
            // copies the body into an error would print them.
            let message = format!("boom for {} {PASSWORD} csrf {csrf}", bearer(&headers));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": message })),
            )
                .into_response();
        }
        (
            StatusCode::OK,
            Json(json!({ "result": { "message": "OK", "dashboard_count": 1 } })),
        )
            .into_response()
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

    async fn list_databases(State(s): State<Shared>, headers: HeaderMap) -> Response {
        let mut state = s.lock().unwrap();
        if state.unauthorized > 0 {
            state.unauthorized -= 1;
            return refused();
        }
        if state.failing {
            // Deliberately chatty, like `export_one`: it repeats what was sent.
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
        if !has_bearer(&headers) {
            return refused();
        }
        (
            StatusCode::OK,
            Json(json!({
                "count": 1,
                "result": [{ "id": 3, "database_name": "Trino Gold" }],
            })),
        )
            .into_response()
    }

    async fn export_database(
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
        if !has_bearer(&headers) {
            return refused();
        }
        if query.get("q").map(String::as_str) != Some("[3]") {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "message": "Not found" })),
            )
                .into_response();
        }
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/zip")],
            Body::from(DATABASE_ARCHIVE),
        )
            .into_response()
    }

    /// Start the fake and a client signed in against it.
    pub(super) async fn start() -> (SupersetClient, Shared) {
        let shared: Shared = Arc::new(Mutex::new(Fake::default()));
        let app = Router::new()
            .route("/api/v1/security/login", post(login))
            .route("/api/v1/security/csrf_token/", get(csrf_token))
            .route("/api/v1/dashboard/export/", get(export_one))
            .route("/api/v1/dashboard/import/", post(import_dashboard))
            .route("/api/v1/database/", get(list_databases))
            .route("/api/v1/database/export/", get(export_database))
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

    use super::fake::{self, ARCHIVE, DATABASE_ARCHIVE, PASSWORD, TOKEN};
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
    async fn databases_lists_the_names_and_ids() {
        let (client, _fake) = fake::start().await;
        let databases = client.databases().await.unwrap();
        assert_eq!(
            databases,
            vec![SupersetDatabase {
                id: 3,
                name: "Trino Gold".to_string(),
            }]
        );
    }

    #[tokio::test]
    async fn database_export_returns_the_archive_bytes() {
        let (client, _fake) = fake::start().await;
        let bytes = client.database_export(3).await.unwrap();
        assert_eq!(bytes, DATABASE_ARCHIVE);
    }

    #[tokio::test]
    async fn the_database_list_refreshes_an_expired_token_once() {
        let (client, fake) = fake::start().await;
        fake.lock().unwrap().unauthorized = 1;
        let databases = client.databases().await.unwrap();
        assert_eq!(
            databases,
            vec![SupersetDatabase {
                id: 3,
                name: "Trino Gold".to_string(),
            }]
        );
        assert_eq!(
            fake.lock().unwrap().logins,
            2,
            "the 401 costs exactly one new sign-in"
        );
    }

    #[tokio::test]
    async fn a_missing_database_is_named() {
        let (client, _fake) = fake::start().await;
        match client.database_export(9).await {
            Err(SupersetError::Status(404, message)) => assert_eq!(
                message, "Superset has no database 9",
                "the answer names the id it asked for"
            ),
            other => panic!("expected a named 404, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_database_error_never_contains_the_password_or_the_token() {
        let (client, fake) = fake::start().await;
        fake.lock().unwrap().failing = true;
        let list_error = client.databases().await.unwrap_err();
        let export_error = client.database_export(3).await.unwrap_err();
        for error in [&list_error, &export_error] {
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
        }
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
    async fn an_import_sends_the_archive_as_form_data_and_overwrite() {
        let (client, fake) = fake::start().await;
        let value = client.import(ARCHIVE, true).await.unwrap();
        assert_eq!(
            value["result"]["dashboard_count"], 1,
            "the parsed answer comes back: {value}"
        );

        let state = fake.lock().unwrap();
        let body = String::from_utf8_lossy(&state.import_body).to_string();
        assert!(
            body.contains("name=\"formData\""),
            "the archive travels as the field formData: {body}"
        );
        assert!(
            body.contains("PK\u{3}\u{4}canary"),
            "the archive's bytes travel in the body: {body}"
        );
        let after = body
            .split("name=\"overwrite\"")
            .nth(1)
            .expect("the overwrite field is in the body");
        assert!(after.contains("true"), "overwrite=true was asked: {after}");
        assert_eq!(
            state.import_headers.get("x-csrftoken").map(String::as_str),
            Some(fake::CSRF),
            "the session's CSRF token travels as a header"
        );
        assert!(
            state
                .import_headers
                .get("referer")
                .is_some_and(|referer| referer == &format!("{}/", client.base)),
            "the referer is the API base: {:?}",
            state.import_headers.get("referer")
        );
    }

    #[tokio::test]
    async fn an_import_can_ask_not_to_overwrite() {
        let (client, fake) = fake::start().await;
        client.import(ARCHIVE, false).await.unwrap();
        let state = fake.lock().unwrap();
        let body = String::from_utf8_lossy(&state.import_body).to_string();
        let after = body
            .split("name=\"overwrite\"")
            .nth(1)
            .expect("the overwrite field is in the body");
        assert!(
            after.contains("false"),
            "overwrite=false was asked: {after}"
        );
        assert!(
            !after.contains("true"),
            "the false answer is not shadowed by a true: {after}"
        );
    }

    #[tokio::test]
    async fn an_expired_token_repeats_the_import_once() {
        let (client, fake) = fake::start().await;
        fake.lock().unwrap().unauthorized_imports = 1;
        let value = client.import(ARCHIVE, true).await.unwrap();
        assert_eq!(value["result"]["dashboard_count"], 1);
        assert_eq!(
            fake.lock().unwrap().logins,
            2,
            "the 401 costs exactly one new sign-in"
        );
    }

    #[tokio::test]
    async fn an_import_error_never_echoes_the_password_or_token() {
        let (client, fake) = fake::start().await;
        fake.lock().unwrap().failing = true;
        let error = client.import(ARCHIVE, true).await.unwrap_err();
        let display = error.to_string();
        let debug = format!("{error:?}");
        for text in [&display, &debug] {
            assert!(!text.contains(PASSWORD), "the password reached: {text}");
            assert!(!text.contains(TOKEN), "the token reached: {text}");
            assert!(!text.contains(fake::CSRF), "the CSRF token reached: {text}");
            assert!(text.contains("[redacted]"), "nothing was scrubbed: {text}");
        }
        assert!(
            display.contains("500"),
            "the upstream status stays in the sentence: {display}"
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
