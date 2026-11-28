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
//! any dashboard, so the export route is administrator-only. The dashboard import route
//! ([`import_dashboard`], `POST /api/dashboards/import`, #115 a/3b2) sits on the public table
//! instead, because a bundle's author is not an administrator: it checks the caller against
//! the bundle's owner group and, for a deploy principal, against its own stage.
//!
//! **No password and no token ever reach a log line, an error string or a `Debug`.** The
//! password is held in [`crate::auth::Secret`] (`[redacted]`), every upstream message is
//! scrubbed before it is stored, and no token-holding type derives `Debug`.

use axum::{
    Extension, Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use utoipa::ToSchema;

use crate::AppState;
use crate::auth::Secret;
use crate::superset_archive::{self, ArchiveError, Stamp};

/// The Superset user Nova signs in as, unless `NOVA_SUPERSET_USER` names another.
pub const SERVICE_USER: &str = "stardelt-nova";

/// The sentence an installation without the service user answers with.
const NO_SERVICE_USER: &str = "this instance has no Superset service user; run scripts/app-secrets.sh, then make step-superset";

/// Longest upstream sentence Nova repeats; the rest is cut.
const MAX_DETAIL: usize = 200;

/// Largest dashboard archive the import route accepts; larger is a `413`.
const MAX_IMPORT_BYTES: usize = 16 * 1024 * 1024;

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

    /// `GET /api/v1/dashboard/{id}`: the dashboard's `certified_by` and
    /// `certification_details`, each `""` when Superset answers `null`. The prune rule reads
    /// both (#115 decision 6): the bundle names the dashboard, the group authorises its removal.
    pub async fn provenance(&self, id: i64) -> Result<(String, String), SupersetError> {
        let url = format!("{}/api/v1/dashboard/{id}", self.base);
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
        if status == StatusCode::NOT_FOUND {
            return Err(SupersetError::Status(
                404,
                format!("Superset has no dashboard {id}"),
            ));
        }
        if !status.is_success() {
            return Err(self.status_error(status, response, &token).await);
        }
        let body: Value = response
            .json()
            .await
            .map_err(|e| SupersetError::Transport(e.to_string()))?;
        let text = |key: &str| body["result"][key].as_str().unwrap_or_default().to_string();
        Ok((text("certified_by"), text("certification_details")))
    }

    /// `GET /api/v1/database/?q=(page_size:200)`: every database the target Superset knows, as
    /// `(id, database_name)`. A row without a numeric `id` or a string `database_name` is
    /// skipped; an answer without a `result` array is `Transport`.
    ///
    /// A `401` means the token from this call expired; Nova signs in once more and repeats the
    /// GET once. Every other non-`200` is Superset's status and message.
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

    /// `DELETE /api/v1/dashboard/{id}`: a session write like the import (`login` → `csrf` →
    /// `X-CSRFToken` and `Referer`), retried once on `401`. `204`-style success is Superset's
    /// `2xx`; a `404` names the id.
    pub async fn delete(&self, id: i64) -> Result<(), SupersetError> {
        let url = format!("{}/api/v1/dashboard/{id}", self.base);
        let referer = format!("{}/", self.base);
        let send = |token: &str, csrf: &str| {
            self.http
                .delete(&url)
                .bearer_auth(token)
                .header("X-CSRFToken", csrf)
                .header(reqwest::header::REFERER, &referer)
        };
        let (mut token, mut csrf) = {
            let bearer = self.login().await?;
            self.csrf(&bearer).await?
        };
        let mut response = send(&token, &csrf)
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
            response = send(&token, &csrf)
                .send()
                .await
                .map_err(|e| SupersetError::Transport(e.to_string()))?;
        }
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Err(SupersetError::Status(
                404,
                format!("Superset has no dashboard {id}"),
            ));
        }
        if !status.is_success() {
            return Err(self
                .import_status_error(status, response, &token, &csrf)
                .await);
        }
        Ok(())
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

/// Why the import refused: the archive itself (a 400 for the caller) or Superset (its own
/// status and sentence).
#[derive(Debug)]
pub(crate) enum ImportError {
    Archive(ArchiveError),
    Superset(SupersetError),
}

impl From<ArchiveError> for ImportError {
    fn from(error: ArchiveError) -> Self {
        ImportError::Archive(error)
    }
}

impl From<SupersetError> for ImportError {
    fn from(error: SupersetError) -> Self {
        ImportError::Superset(error)
    }
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportError::Archive(error) => error.fmt(f),
            ImportError::Superset(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ImportError {}

impl ImportError {
    pub(crate) fn into_response(self) -> Response {
        match self {
            // `UnsafePath`, `NotZip` and `NoCatalog` are the caller's upload, so 400; the
            // missing database is the target's state, so 422 with the sentence naming it.
            ImportError::Archive(error @ ArchiveError::MissingDatabase(_)) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({ "error": error.to_string() })),
            )
                .into_response(),
            ImportError::Archive(error) => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": error.to_string() })),
            )
                .into_response(),
            ImportError::Superset(error) => error.into_response(),
        }
    }
}

/// What one import put on the target: the database stems that were mapped (`["Trino_Gold"]`)
/// and Superset's own answer.
#[derive(Debug)]
pub(crate) struct ImportedDashboard {
    pub databases: Vec<String>,
    pub result: Value,
}

/// One dashboard archive for the target: every dataset points at the target's own database,
/// the target's `databases/` file is added, every dashboard carries the stamp, then Superset
/// imports it. Answers Superset's own JSON body.
///
/// The target database for a dataset's `catalog: gold` is the database whose name maps to the
/// stem `Trino_Gold` (a space becomes `_`, the export's naming). Its uuid is read from the
/// database's own export — never written by us — so the target keeps its connection URI.
pub(crate) async fn import_archive(
    client: &SupersetClient,
    archive: &[u8],
    stamp: &Stamp<'_>,
    overwrite: bool,
) -> Result<ImportedDashboard, ImportError> {
    let catalogs = superset_archive::dataset_catalogs(archive)?;
    let mut ids = BTreeMap::new();
    for database in client.databases().await? {
        ids.insert(database.name.replace(' ', "_"), database.id);
    }
    let mut databases = BTreeMap::new();
    for catalog in &catalogs {
        let stem = superset_archive::database_stem(catalog);
        let Some(id) = ids.get(&stem).copied() else {
            return Err(ArchiveError::MissingDatabase(stem).into());
        };
        let (member, bytes) =
            superset_archive::database_member(&client.database_export(id).await?)?;
        if member != stem {
            return Err(ArchiveError::MissingDatabase(member).into());
        }
        databases.insert(stem, bytes);
    }
    let rewritten = superset_archive::rewrite(archive, &databases, stamp)?;
    let result = client.import(&rewritten, overwrite).await?;
    Ok(ImportedDashboard {
        databases: databases.keys().cloned().collect(),
        result,
    })
}

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
        Ok(bytes) => zip_response(bytes, id),
        Err(error) => error.into_response(),
    }
}

/// A ZIP answer with Superset's content type and a download name; both export routes use it,
/// so the administrator route's answer is unchanged by #115 a/3c.
fn zip_response(bytes: Vec<u8>, id: i64) -> Response {
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

/// `GET /api/dashboards/{id}/export`: the ZIP of one dashboard for a bundle author. A member
/// of the bundle's owner group may call it (the maintainer's decision on #115 a/3c); a deploy
/// principal is confined to its own group by `deploy_refusal`. `databases/` is dropped
/// server-side, so the target's connection URI never leaves Nova.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DashboardExportQuery {
    /// The bundle that owns the dashboard, 1 to 64 characters.
    pub bundle: String,
    /// The bundle's owner group; the caller must be a member.
    pub owner_group: String,
}

/// Why this caller may not export a bundle's dashboard, or `None` when it may. A deploy
/// principal is confined to the stage its client names, which here is its own stage, so the
/// `deploy_refusal` call is the import route's rule with the export verb.
pub(crate) fn export_refusal(
    identity: &crate::auth::Identity,
    owner_group: &str,
    client_id: Option<&str>,
) -> Option<String> {
    let target = crate::auth::deploy_stage(client_id).unwrap_or("");
    crate::deployments::deploy_refusal(identity, owner_group, target, client_id, "export")
}

/// Why `dashboard` may not be removed for `bundle` by `owner_group`, or `None` when it may.
/// The import stamps `certified_by: "stardelt bundle <bundle>"` and
/// `certification_details: "<target> @ <commit> by <owner_group>"` (decision 6 of #115,
/// amended); both must name this bundle and this group. A dashboard without the group in its
/// stamp — a person's own, another bundle's, another group's — is never touched.
pub(crate) fn delete_refusal(
    certified: &str,
    details: &str,
    bundle: &str,
    owner_group: &str,
) -> Option<String> {
    let bundle = bundle.trim();
    let owner_group = owner_group.trim();
    if certified.trim() != format!("stardelt bundle {bundle}") {
        return Some(format!(
            "dashboard is not certified by bundle {bundle}; only a dashboard this bundle imported is removed"
        ));
    }
    match details.rsplit_once(" by ") {
        Some((_, group)) if group.trim() == owner_group => None,
        _ => Some(format!(
            "dashboard is not certified for group {owner_group}; a dashboard another group imported is not removed"
        )),
    }
}

/// `DELETE /api/dashboards/{id}` query: the bundle whose import stamped the dashboard.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DashboardDeleteQuery {
    /// The bundle that owns the dashboard, 1 to 64 characters.
    pub bundle: String,
    /// The bundle's owner group; the caller must be a member.
    pub owner_group: String,
}

/// `DELETE /api/dashboards/{id}`: remove one dashboard **this bundle imported**. The
/// dashboard's `certified_by` must be `stardelt bundle <bundle>` and its
/// `certification_details` must end with ` by <ownerGroup>`; a dashboard without the stamp,
/// or one another group imported, is refused and stays (#115 decision 6). The caller's rule
/// is the export's owner-group rule.
///
/// 204 on success; 403 for a non-member or an unstamped dashboard; 404 when Superset has
/// no such dashboard; 502 when Superset refuses; 503 without a service user.
#[utoipa::path(
    delete,
    path = "/api/dashboards/{id}",
    tag = "public",
    operation_id = "dashboards_delete",
    description = "Remove one dashboard that this bundle imported. The dashboard's `certified_by` must be `stardelt bundle <bundle>` and its `certification_details` must name the caller's `ownerGroup` as the group that imported it; a dashboard without that stamp is refused. The caller must be a member of the bundle's owner group, and a deploy principal may only remove for its own group.",
    params(
        ("id" = i64, Path, description = "Superset's numeric dashboard id"),
        ("bundle" = String, Query, description = "The bundle that imported the dashboard"),
        ("ownerGroup" = String, Query, description = "The bundle's owner group; the caller must be a member"),
    ),
    responses(
        (status = 204, description = "Deleted"),
        (status = 400, description = "The bundle name or owner group is empty", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller is not a member of the bundle's owner group, or the dashboard is not stamped by this bundle", body = crate::openapi::ErrorResponse),
        (status = 404, description = "Superset has no such dashboard", body = crate::openapi::ErrorResponse),
        (status = 502, description = "Superset answered an error, or could not be reached", body = crate::openapi::ErrorResponse),
        (status = 503, description = "This instance has no Superset service user", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn delete_dashboard(
    State(state): State<Arc<AppState>>,
    caller: Option<Extension<crate::auth::Principal>>,
    Path(id): Path<i64>,
    Query(query): Query<DashboardDeleteQuery>,
) -> Response {
    if query.bundle.trim().is_empty() || query.owner_group.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "bundle and ownerGroup are required" })),
        )
            .into_response();
    }
    let Some(caller) = caller.as_ref().map(|c| &c.0) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "sign in to remove a dashboard" })),
        )
            .into_response();
    };
    let identity = match crate::deployments::deploy_identity(&state, caller).await {
        Ok(identity) => identity,
        Err(sentence) => {
            return (StatusCode::BAD_GATEWAY, Json(json!({ "error": sentence }))).into_response();
        }
    };
    if let Some(sentence) =
        export_refusal(&identity, &query.owner_group, caller.client_id.as_deref())
    {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": sentence }))).into_response();
    }
    let Some(user) = state.superset.as_ref() else {
        return SupersetError::NoServiceUser.into_response();
    };
    let client = SupersetClient::new(
        &state.superset_api_url,
        ServiceUser {
            username: user.username.clone(),
            password: Secret::new(user.password.expose().to_string()),
        },
    );
    let (certified, details) = match client.provenance(id).await {
        Ok(stamp) => stamp,
        Err(error) => return error.into_response(),
    };
    if let Some(sentence) = delete_refusal(&certified, &details, &query.bundle, &query.owner_group)
    {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": sentence }))).into_response();
    }
    match client.delete(id).await {
        Ok(()) => {
            tracing::info!(
                target: "audit",
                sub = %identity.sub,
                id,
                bundle = %query.bundle.trim(),
                "dashboard deleted"
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => error.into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/api/dashboards/{id}/export",
    tag = "public",
    operation_id = "dashboards_export",
    description = "The ZIP archive of one dashboard for a bundle's author: the caller must be a member of the bundle's owner group, and a deploy principal may only export for its own stage. The archive's `databases/` folder is dropped server-side, so a bundle never receives the target's connection URI.",
    params(
        ("id" = i64, Path, description = "Superset's numeric dashboard id"),
        ("bundle" = String, Query, description = "The bundle that owns the dashboard"),
        ("ownerGroup" = String, Query, description = "The bundle's owner group; the caller must be a member"),
    ),
    responses(
        (status = 200, description = "The ZIP archive Superset exports for one dashboard, without `databases/`", body = String, content_type = "application/zip"),
        (status = 400, description = "The bundle name or owner group is empty", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller is not a member of the bundle's owner group, or a deploy principal names another stage", body = crate::openapi::ErrorResponse),
        (status = 404, description = "Superset has no such dashboard", body = crate::openapi::ErrorResponse),
        (status = 502, description = "Superset answered an error, or could not be reached", body = crate::openapi::ErrorResponse),
        (status = 503, description = "This instance has no Superset service user", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn export_dashboard_for_bundle(
    State(state): State<Arc<AppState>>,
    caller: Option<Extension<crate::auth::Principal>>,
    Path(id): Path<i64>,
    Query(query): Query<DashboardExportQuery>,
) -> Response {
    if query.bundle.trim().is_empty() || query.owner_group.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "bundle and ownerGroup are required" })),
        )
            .into_response();
    }
    let Some(caller) = caller.as_ref().map(|c| &c.0) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "sign in to export a dashboard" })),
        )
            .into_response();
    };
    let identity = match crate::deployments::deploy_identity(&state, caller).await {
        Ok(identity) => identity,
        Err(sentence) => {
            return (StatusCode::BAD_GATEWAY, Json(json!({ "error": sentence }))).into_response();
        }
    };
    if let Some(sentence) =
        export_refusal(&identity, &query.owner_group, caller.client_id.as_deref())
    {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": sentence }))).into_response();
    }
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
        Ok(bytes) => match superset_archive::without_databases(&bytes) {
            Ok(stripped) => zip_response(stripped, id),
            Err(error) => (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": error.to_string() })),
            )
                .into_response(),
        },
        Err(error) => error.into_response(),
    }
}

/// `POST /api/dashboards/import`: one dashboard archive for the target, for dashboard bundles
/// (#115 a/3b2). The query names the bundle, its owner group and the target; the body is the
/// ZIP the CLI exported (or zipped from `dashboards/<slug>/`).
///
/// The caller must be a member of the bundle's owner group, and a deploy principal (#57) may
/// only import to the stage its client names, the same rule `POST /api/deployments` uses.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DashboardImportQuery {
    /// The bundle that owns the dashboards, 1 to 64 characters.
    pub bundle: String,
    /// The bundle's owner group; the caller must be a member.
    pub owner_group: String,
    /// The stage the import is for (`dev`, `prod`, …).
    pub target: String,
    /// The commit the bundle was read at, for the provenance stamp.
    #[serde(default)]
    pub commit: String,
    /// Replace a dashboard of the same name (`stardelt bundle deploy` passes its own flag).
    #[serde(default)]
    pub overwrite: bool,
}

#[utoipa::path(
    post,
    operation_id = "dashboards_import",
    path = "/api/dashboards/import",
    tag = "public",
    description = "Import one Superset dashboard archive for a bundle's target: every dataset is remapped onto the target's own `Trino <catalog>` database, the target's `databases/` file is added, every dashboard is stamped `certified_by: stardelt bundle <bundle>` / `certification_details: <target> @ <commit>`, and Superset imports the result. The caller must be a member of the bundle's owner group; a deploy principal may only import to its own stage. The answer is Superset's own import result plus the database stems that were mapped.",
    params(
        ("bundle" = String, Query, description = "The bundle whose dashboards these are"),
        ("ownerGroup" = String, Query, description = "The bundle's owner group"),
        ("target" = String, Query, description = "The stage the import is for"),
        ("commit" = Option<String>, Query, description = "The commit the bundle was read at"),
        ("overwrite" = Option<bool>, Query, description = "Replace a dashboard of the same name"),
    ),
    request_body(content = Vec<u8>, content_type = "application/zip", description = "The dashboard archive (Superset's export, `databases/` optional)"),
    responses(
        (status = 200, description = "Superset's import result and the mapped database stems", body = serde_json::Value),
        (status = 400, description = "The body is not a dashboard archive, or a name is empty or too long", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller is not a member of the bundle's owner group, or a deploy principal names another stage", body = crate::openapi::ErrorResponse),
        (status = 413, description = "The archive is larger than 16 MiB", body = crate::openapi::ErrorResponse),
        (status = 422, description = "The target has no database for one of the archive's catalogs", body = crate::openapi::ErrorResponse),
        (status = 502, description = "Superset answered an error, or could not be reached", body = crate::openapi::ErrorResponse),
        (status = 503, description = "This instance has no Superset service user", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn import_dashboard(
    State(state): State<Arc<AppState>>,
    caller: Option<Extension<crate::auth::Principal>>,
    Query(query): Query<DashboardImportQuery>,
    // The body extractor has to be last: axum consumes the request for it.
    body: Bytes,
) -> Response {
    if query.bundle.trim().is_empty()
        || query.owner_group.trim().is_empty()
        || query.target.trim().is_empty()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "bundle, ownerGroup and target are required" })),
        )
            .into_response();
    }
    if body.len() > MAX_IMPORT_BYTES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({ "error": "the dashboard archive is larger than 16 MiB" })),
        )
            .into_response();
    }
    let Some(caller) = caller.as_ref().map(|c| &c.0) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "sign in to import a dashboard" })),
        )
            .into_response();
    };
    let identity = match crate::deployments::deploy_identity(&state, caller).await {
        Ok(identity) => identity,
        Err(sentence) => {
            return (StatusCode::BAD_GATEWAY, Json(json!({ "error": sentence }))).into_response();
        }
    };
    if let Some(sentence) = crate::deployments::deploy_refusal(
        &identity,
        &query.owner_group,
        &query.target,
        caller.client_id.as_deref(),
        "import",
    ) {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": sentence }))).into_response();
    }
    let Some(user) = state.superset.as_ref() else {
        return SupersetError::NoServiceUser.into_response();
    };
    let user = ServiceUser {
        username: user.username.clone(),
        password: Secret::new(user.password.expose().to_string()),
    };
    let client = SupersetClient::new(&state.superset_api_url, user);
    let stamp = Stamp {
        bundle: query.bundle.trim(),
        target: query.target.trim(),
        commit: query.commit.trim(),
        owner_group: query.owner_group.trim(),
    };
    match import_archive(&client, &body, &stamp, query.overwrite).await {
        Ok(imported) => {
            tracing::info!(
                bundle = %stamp.bundle,
                target = %stamp.target,
                owner_group = %query.owner_group,
                databases = ?imported.databases,
                "dashboard archive imported"
            );
            (
                StatusCode::OK,
                Json(json!({ "databases": imported.databases, "result": imported.result })),
            )
                .into_response()
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
        extract::{Path, Query, State},
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

    /// A real one-member ZIP (the database file for `Trino Gold`, `uuid: target-gold`), because
    /// the import reads the uuid out of it (#115 a/3b2).
    pub(super) fn database_zip() -> Vec<u8> {
        let mut written = Vec::new();
        {
            let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut written));
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            zip.start_file("database_export/databases/Trino_Gold.yaml", options)
                .unwrap();
            std::io::Write::write_all(&mut zip, b"database_name: Trino Gold\nuuid: target-gold\n")
                .unwrap();
            zip.finish().unwrap();
        }
        written
    }

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
        /// The `certified_by` the one dashboard answers with (`None` answers `null`).
        pub(super) certified: Option<String>,
        /// The `certification_details` the one dashboard answers with (`None` answers `null`).
        pub(super) details: Option<String>,
        /// How many more `401` answers to serve on the dashboard GET.
        pub(super) unauthorized_dashboards: usize,
        /// How many more `401` answers to serve on the dashboard DELETE.
        pub(super) unauthorized_deletes: usize,
        /// Every id a delete removed, in order.
        pub(super) deletions: Vec<i64>,
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
            Body::from(database_zip()),
        )
            .into_response()
    }

    async fn one_dashboard(
        State(s): State<Shared>,
        Path(id): Path<i64>,
        _headers: HeaderMap,
    ) -> Response {
        let mut state = s.lock().unwrap();
        if state.unauthorized_dashboards > 0 {
            state.unauthorized_dashboards -= 1;
            return refused();
        }
        if id != 7 {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "message": "Not found" })),
            )
                .into_response();
        }
        (
            StatusCode::OK,
            Json(json!({ "result": {
                "certified_by": state.certified,
                "certification_details": state.details,
            } })),
        )
            .into_response()
    }

    async fn delete_dashboard(
        State(s): State<Shared>,
        Path(id): Path<i64>,
        headers: HeaderMap,
    ) -> Response {
        let mut state = s.lock().unwrap();
        if state.unauthorized_deletes > 0 {
            state.unauthorized_deletes -= 1;
            return refused();
        }
        if !has_bearer(&headers) {
            return refused();
        }
        if headers.get("x-csrftoken").and_then(|v| v.to_str().ok()) != Some(CSRF) {
            return refused();
        }
        if id != 7 {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "message": "Not found" })),
            )
                .into_response();
        }
        state.deletions.push(id);
        (StatusCode::OK, Json(json!({ "message": "Deleted" }))).into_response()
    }

    /// Start the fake and a client signed in against it.
    pub(super) async fn start() -> (SupersetClient, Shared) {
        let shared: Shared = Arc::new(Mutex::new(Fake::default()));
        let app = Router::new()
            .route("/api/v1/security/login", post(login))
            .route("/api/v1/security/csrf_token/", get(csrf_token))
            .route("/api/v1/dashboard/export/", get(export_one))
            .route(
                "/api/v1/dashboard/{id}",
                get(one_dashboard).delete(delete_dashboard),
            )
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

    use super::fake::{self, ARCHIVE, PASSWORD, TOKEN};
    use super::*;

    fn person(groups: &[&str], is_admin: bool) -> crate::auth::Identity {
        crate::auth::Identity {
            sub: "u".to_string(),
            display_name: "u".to_string(),
            email: None,
            name: None,
            groups: groups.iter().map(|g| (*g).to_string()).collect(),
            roles: Vec::new(),
            groups_overage: false,
            is_admin,
        }
    }

    #[test]
    fn an_export_is_refused_outside_the_bundle_owner_group() {
        assert_eq!(
            export_refusal(&person(&["analytics"], false), "analytics", None),
            None
        );
        let refused = export_refusal(&person(&["visitors"], false), "analytics", None)
            .expect("a non-member is refused");
        assert!(refused.contains("only a member"), "{refused}");
        assert_eq!(export_refusal(&person(&[], true), "analytics", None), None);
        assert_eq!(
            export_refusal(
                &person(&["analytics"], false),
                "analytics",
                Some("stardelt-deploy-prod")
            ),
            None,
            "a deploy principal of the group may export"
        );
        let refused = export_refusal(
            &person(&[], false),
            "analytics",
            Some("stardelt-deploy-prod"),
        )
        .expect("a group-less deploy principal is refused");
        assert!(refused.contains("its own group"), "{refused}");
    }

    /// The members of a ZIP as `(name, text)`, in archive order.
    fn unzip(archive: &[u8]) -> Vec<(String, String)> {
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(archive)).expect("a ZIP");
        let mut out = Vec::new();
        for index in 0..zip.len() {
            let mut file = zip.by_index(index).expect("a member");
            let name = file.name().expect("a member name").to_string();
            let mut text = String::new();
            std::io::Read::read_to_string(&mut file, &mut text).expect("member text");
            out.push((name, text));
        }
        out
    }

    /// One member of a ZIP, by the end of its name.
    fn member(archive: &[u8], suffix: &str) -> String {
        unzip(archive)
            .into_iter()
            .find(|(name, _)| name.ends_with(suffix))
            .unwrap_or_else(|| panic!("the archive has no member ending {suffix}"))
            .1
    }

    /// The archive a recorded multipart body carried: from the ZIP's first local header to the
    /// end of its end-of-central-directory record.
    fn archive_in(multipart: &[u8]) -> Vec<u8> {
        let start = multipart
            .windows(4)
            .position(|w| w == b"PK\x03\x04")
            .expect("the body carries a ZIP part");
        let end = multipart
            .windows(4)
            .rposition(|w| w == b"PK\x05\x06")
            .expect("the ZIP part ends")
            + 22;
        multipart[start..end].to_vec()
    }

    /// The ZIP the CLI uploads: the folder as-is under one top directory.
    fn zipped(files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut written = Vec::new();
        {
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            let mut writer = zip::ZipWriter::new(std::io::Cursor::new(&mut written));
            for (name, bytes) in files {
                writer.start_file(*name, options).expect("write member");
                std::io::Write::write_all(&mut writer, bytes).expect("write bytes");
            }
            writer.finish().expect("finish the ZIP");
        }
        written
    }

    /// A dataset member of the shop bundle's export, pointing at `catalog`.
    fn dataset(catalog: &str) -> Vec<u8> {
        format!("table_name: orders\ndatabase_uuid: old-uuid\ncatalog: {catalog}\n").into_bytes()
    }

    fn dashboard() -> Vec<u8> {
        b"dashboard_title: Shop\ncertified_by: null\ncertification_details: null\n".to_vec()
    }

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
        let database = member(&bytes, "databases/Trino_Gold.yaml");
        assert!(
            database.contains("uuid: target-gold"),
            "the export is the target's own database file: {database}"
        );
    }

    #[tokio::test]
    async fn a_stamped_dashboard_is_deleted() {
        let (client, fake) = fake::start().await;
        {
            let mut state = fake.lock().unwrap();
            state.certified = Some("stardelt bundle shop".to_string());
            state.details = Some("prod @ abc1234 by shop".to_string());
        }
        let (certified, details) = client.provenance(7).await.unwrap();
        assert_eq!(certified, "stardelt bundle shop");
        assert_eq!(details, "prod @ abc1234 by shop");
        assert_eq!(delete_refusal(&certified, &details, "shop", "shop"), None);
        client.delete(7).await.expect("the delete");
        assert_eq!(fake.lock().unwrap().deletions, vec![7]);
    }

    #[test]
    fn another_groups_dashboard_is_never_deleted() {
        // The attack the first review found: the caller names another group's bundle and
        // their own group. The recorded importing group is not theirs, so it stays.
        let refused = delete_refusal(
            "stardelt bundle analytics-shop",
            "prod @ abc1234 by analytics",
            "analytics-shop",
            "visitors",
        )
        .expect("another group's dashboard must not be removed");
        assert!(refused.contains("visitors"), "{refused}");
    }

    #[test]
    fn a_dashboard_without_the_group_is_never_deleted() {
        // The pre-amendment stamp carries no ` by <group>`: refused, never touched.
        let refused = delete_refusal(
            "stardelt bundle shop",
            "prod @ abc1234",
            "shop",
            "analytics",
        )
        .expect("a dashboard without the group must not be removed");
        assert!(refused.contains("analytics"), "{refused}");
    }

    #[test]
    fn the_stamp_rule_names_the_bundle() {
        assert_eq!(
            delete_refusal(
                "stardelt bundle shop",
                "prod @ abc1234 by shop",
                "shop",
                "shop"
            ),
            None
        );
        assert_eq!(
            delete_refusal(
                "stardelt bundle shop",
                "prod @ abc1234 by shop",
                " shop ",
                "shop"
            ),
            None
        );
        for certified in ["", "stardelt bundle other"] {
            let refused = delete_refusal(certified, "prod @ abc1234 by shop", "shop", "shop")
                .unwrap_or_else(|| panic!("{certified:?} must not be removed"));
            assert!(refused.contains("shop"), "{refused}");
        }
    }

    #[tokio::test]
    async fn a_certified_get_reads_null_as_no_stamp() {
        let (client, _fake) = fake::start().await;
        let (certified, details) = client.provenance(7).await.unwrap();
        assert_eq!((certified.as_str(), details.as_str()), ("", ""));
        assert!(delete_refusal(&certified, &details, "shop", "shop").is_some());
    }

    #[tokio::test]
    async fn a_certified_get_retries_after_a_401() {
        let (client, fake) = fake::start().await;
        {
            let mut state = fake.lock().unwrap();
            state.unauthorized_dashboards = 1;
            state.certified = Some("stardelt bundle shop".to_string());
            state.details = Some("prod @ abc1234 by shop".to_string());
        }
        let (certified, details) = client.provenance(7).await.expect("the retried GET");
        assert_eq!(certified, "stardelt bundle shop");
        assert_eq!(details, "prod @ abc1234 by shop");
    }

    #[tokio::test]
    async fn the_delete_retries_after_a_401() {
        let (client, fake) = fake::start().await;
        fake.lock().unwrap().unauthorized_deletes = 1;
        client.delete(7).await.expect("the retried delete");
        assert_eq!(fake.lock().unwrap().deletions, vec![7]);
    }

    #[tokio::test]
    async fn a_missing_dashboard_is_a_404() {
        let (client, _fake) = fake::start().await;
        match client.provenance(8).await {
            Err(SupersetError::Status(404, sentence)) => {
                assert!(sentence.contains('8'), "{sentence}")
            }
            other => panic!("expected a 404, got {other:?}"),
        }
        match client.delete(8).await {
            Err(SupersetError::Status(404, sentence)) => {
                assert!(sentence.contains('8'), "{sentence}")
            }
            other => panic!("expected a 404, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_import_remaps_each_dataset_and_stamps_the_dashboard() {
        let (client, fake) = fake::start().await;
        let archive = zipped(&[
            (
                "shop-export/datasets/Trino_Gold/orders.yaml",
                &dataset("gold"),
            ),
            ("shop-export/dashboards/Shop.yaml", &dashboard()),
        ]);
        let imported = import_archive(
            &client,
            &archive,
            &Stamp {
                bundle: "shop",
                target: "prod",
                commit: "abc123",
                owner_group: "analytics",
            },
            false,
        )
        .await
        .expect("the import");
        assert_eq!(imported.databases, vec!["Trino_Gold".to_string()]);
        assert_eq!(
            imported.result["result"]["dashboard_count"], 1,
            "Superset's own answer comes back: {}",
            imported.result
        );

        let sent = archive_in(&fake.lock().unwrap().import_body);
        let dataset = member(&sent, "datasets/Trino_Gold/orders.yaml");
        assert!(
            dataset.contains("database_uuid: \"target-gold\""),
            "the dataset points at the target database: {dataset}"
        );
        assert!(
            !dataset.contains("old-uuid"),
            "the export's own database uuid is gone: {dataset}"
        );
        let dashboard = member(&sent, "dashboards/Shop.yaml");
        assert!(
            dashboard.contains("certified_by: \"stardelt bundle shop\""),
            "the dashboard names the bundle: {dashboard}"
        );
        assert!(
            dashboard.contains("certification_details: \"prod @ abc123 by analytics\""),
            "the dashboard names the target, the commit and the importing group: {dashboard}"
        );
        let database = member(&sent, "databases/Trino_Gold.yaml");
        assert!(
            database.contains("uuid: target-gold"),
            "the target's own database file was added: {database}"
        );
    }

    #[tokio::test]
    async fn an_archive_without_datasets_is_an_error() {
        let (client, _fake) = fake::start().await;
        let archive = zipped(&[("shop-export/dashboards/Shop.yaml", &dashboard())]);
        match import_archive(
            &client,
            &archive,
            &Stamp {
                bundle: "shop",
                target: "prod",
                commit: "abc123",
                owner_group: "analytics",
            },
            false,
        )
        .await
        {
            Err(ImportError::Archive(ArchiveError::NoDatasets)) => {}
            other => panic!("expected NoDatasets, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_catalog_the_target_lacks_names_the_database() {
        let (client, _fake) = fake::start().await;
        // The fake's list has only `Trino Gold`, so `silver` has no database.
        let archive = zipped(&[
            (
                "shop-export/datasets/Trino_Silver/orders.yaml",
                &dataset("silver"),
            ),
            ("shop-export/dashboards/Shop.yaml", &dashboard()),
        ]);
        match import_archive(
            &client,
            &archive,
            &Stamp {
                bundle: "shop",
                target: "prod",
                commit: "abc123",
                owner_group: "analytics",
            },
            false,
        )
        .await
        {
            Err(ImportError::Archive(ArchiveError::MissingDatabase(name))) => {
                assert_eq!(name, "Trino_Silver", "the answer names the database")
            }
            other => panic!("expected MissingDatabase, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_unsafe_archive_member_is_refused() {
        let (client, _fake) = fake::start().await;
        let archive = zipped(&[
            ("shop-export/../evil.yaml", &dataset("gold")),
            ("shop-export/dashboards/Shop.yaml", &dashboard()),
        ]);
        match import_archive(
            &client,
            &archive,
            &Stamp {
                bundle: "shop",
                target: "prod",
                commit: "abc123",
                owner_group: "analytics",
            },
            false,
        )
        .await
        {
            Err(ImportError::Archive(ArchiveError::UnsafePath(_))) => {}
            other => panic!("expected UnsafePath, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_import_never_logs_the_password_or_the_token() {
        let (client, fake) = fake::start().await;
        fake.lock().unwrap().failing = true;
        let archive = zipped(&[
            (
                "shop-export/datasets/Trino_Gold/orders.yaml",
                &dataset("gold"),
            ),
            ("shop-export/dashboards/Shop.yaml", &dashboard()),
        ]);
        let error = import_archive(
            &client,
            &archive,
            &Stamp {
                bundle: "shop",
                target: "prod",
                commit: "abc123",
                owner_group: "analytics",
            },
            false,
        )
        .await
        .expect_err("the fake refuses");
        let display = error.to_string();
        let debug = format!("{error:?}");
        for text in [&display, &debug] {
            assert!(!text.contains(PASSWORD), "the password reached: {text}");
            assert!(!text.contains(TOKEN), "the token reached: {text}");
            assert!(!text.contains(fake::CSRF), "the CSRF token reached: {text}");
        }
        assert!(
            display.contains("500"),
            "the upstream status stays in the sentence: {display}"
        );
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
