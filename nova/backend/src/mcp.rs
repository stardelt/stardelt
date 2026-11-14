//! The hosted MCP endpoint (stardelt-nova#112, design `design/agent-access.md` §4).
//!
//! One endpoint, one bearer token, eleven read-only tools: an MCP-capable agent reaches the
//! platform's catalog as the signed-in person with no client install. The local
//! `stardelt mcp serve` stays the stdio server; this is the hosted one at `/mcp`, outside
//! `/api` so it never appears in the SPA fallback or the OpenAPI document.
//!
//! - **Off by default.** `NOVA_MCP_ENABLED` off (or unset) leaves every route here a 404
//!   JSON answer, never the SPA. On, the endpoint refuses to start without
//!   `NOVA_AUTH_MODE=oidc` and `NOVA_MCP_AUDIENCE`: it has no anonymous mode.
//! - **Its own audience.** `NOVA_MCP_AUDIENCE` names the resource identifier the IdP mints
//!   tokens for (`https://<nova host>/mcp`), so a token minted for the browser is refused
//!   (`auth::AuthState::authenticate_bearer_for`). The session cookie is never accepted.
//! - **One scope.** Every call needs `stardelt:read`, which the realm client `stardelt-mcp`
//!   grants by default (stardelt-platform#1369). A token without it gets 403
//!   `insufficient_scope` with the scope named, so a client can step up.
//! - **Discovery.** `/.well-known/oauth-protected-resource` (RFC 9728) and its `/mcp` twin
//!   tell a client where to sign in, and the 401 challenge points at them.
//! - **Read tools only.** Each tool calls the catalog handler the `/api/catalog/*` route
//!   calls, with the caller's own principal; no tool writes. Every call writes one line on
//!   the `audit` target before it answers.
//! - **Guarded SQL.** `sql_read` and `sql_explain` run ONE read-only statement as the caller,
//!   with the row cap and timeout below; `query_log_list` reads the caller's own history. The
//!   classifier is a first filter, never the security boundary.
//! - **No new dependency.** Three JSON-RPC methods and `serde_json` are enough; a JSON-RPC
//!   or MCP crate would need a licence check first.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;

use crate::AppState;
use crate::auth::{AuthError, BearerFacts, Principal};
use crate::{catalog, catalog_search};

/// The protocol revision this endpoint speaks.
const PROTOCOL_VERSION: &str = "2025-06-18";

/// The scope every tool call needs; the realm client `stardelt-mcp` grants it by default
/// (stardelt-platform#1369).
pub const READ_SCOPE: &str = "stardelt:read";

/// Longest tool text before it is cut short; past this the answer carries no
/// `structuredContent` and says so (a whole catalog does not fit in a model's context).
const MAX_TOOL_TEXT_CHARS: usize = 20_000;

/// Calls one person may make per minute on `/mcp` (`NOVA_MCP_CALLS_PER_MINUTE`, default 60).
/// `initialize` and `tools/list` never consume a slot: a session makes one of each.
const CALLS_PER_MINUTE: usize = 60;

/// The per-person call limit (`AppState`-lifetime, shared by every cloned `McpConfig`): per
/// signed-in `sub`, the `tools/call` instants inside the last minute. Only a restart forgets it.
pub type CallsLimiter = Arc<Mutex<HashMap<String, VecDeque<Instant>>>>;

/// The MCP resource's configuration (`NOVA_MCP_ENABLED`, `NOVA_MCP_AUDIENCE`,
/// `NOVA_MCP_MAX_ROWS`, `NOVA_MCP_QUERY_TIMEOUT_SECS`, `NOVA_MCP_ALLOWED_CLIENTS`,
/// `NOVA_MCP_CALLS_PER_MINUTE`).
#[derive(Clone, Debug)]
pub struct McpConfig {
    /// The resource identifier the IdP mints access tokens for: `https://<nova host>/mcp`.
    pub audience: String,
    /// Rows one `sql_read` call may return (`NOVA_MCP_MAX_ROWS`, default 200, clamped 1..=1000).
    pub max_rows: usize,
    /// Cut-off for one `sql_read`/`sql_explain` call (`NOVA_MCP_QUERY_TIMEOUT_SECS`, default 60, clamped 1..=300).
    pub query_timeout: std::time::Duration,
    /// Client ids (`azp`) allowed to call the endpoint (`NOVA_MCP_ALLOWED_CLIENTS`,
    /// comma separated). Empty = every client the audience accepts. A token without an
    /// `azp` is refused while the list is non-empty.
    pub allowed_clients: Vec<String>,
    /// `tools/call`s one person may make per minute (`NOVA_MCP_CALLS_PER_MINUTE`, default 60, 1..=600).
    pub calls_per_minute: usize,
    /// The call instants already taken, per `sub`; cloned configs share it.
    pub calls: CallsLimiter,
}

impl McpConfig {
    pub fn from_env(oidc: bool) -> Result<Option<Self>, String> {
        Self::from_lookup(|k| std::env::var(k).ok(), oidc)
    }

    pub fn from_lookup(
        get: impl Fn(&str) -> Option<String>,
        oidc: bool,
    ) -> Result<Option<Self>, String> {
        let enabled = matches!(
            get("NOVA_MCP_ENABLED").as_deref(),
            Some("1") | Some("true") | Some("TRUE") | Some("yes")
        );
        if !enabled {
            return Ok(None);
        }
        if !oidc {
            return Err(
                "NOVA_MCP_ENABLED needs NOVA_AUTH_MODE=oidc: the MCP endpoint has no anonymous mode"
                    .into(),
            );
        }
        let audience = get("NOVA_MCP_AUDIENCE")
            .unwrap_or_default()
            .trim()
            .trim_end_matches('/')
            .to_string();
        if audience.is_empty() {
            return Err(
                "NOVA_MCP_ENABLED is set but NOVA_MCP_AUDIENCE is empty: name the resource identifier tokens for /mcp carry"
                    .into(),
            );
        }
        let max_rows = get("NOVA_MCP_MAX_ROWS")
            .and_then(|v| v.trim().parse::<usize>().ok())
            .map(|n| n.clamp(1, 1000))
            .unwrap_or(200);
        let query_timeout_secs = get("NOVA_MCP_QUERY_TIMEOUT_SECS")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|n| *n > 0)
            .map(|n| n.min(300))
            .unwrap_or(60);
        let allowed_clients = get("NOVA_MCP_ALLOWED_CLIENTS")
            .map(|v| {
                v.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let calls_per_minute = get("NOVA_MCP_CALLS_PER_MINUTE")
            .and_then(|v| v.trim().parse::<usize>().ok())
            .map(|n| n.clamp(1, 600))
            .unwrap_or(CALLS_PER_MINUTE);
        Ok(Some(Self {
            audience,
            max_rows,
            query_timeout: std::time::Duration::from_secs(query_timeout_secs),
            allowed_clients,
            calls_per_minute,
            calls: Arc::new(Mutex::new(HashMap::new())),
        }))
    }
}

/// The MCP routes. Merged into the top-level app before the SPA fallback (`main.rs`), so
/// `/mcp` never reaches the static files.
pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/mcp", post(handle).get(method_not_allowed))
        .route("/.well-known/oauth-protected-resource", get(metadata))
        .route(
            "/.well-known/oauth-protected-resource/mcp",
            get(metadata_mcp),
        )
}

/// `GET` on the JSON-RPC endpoint: it only takes a POST, and says so.
async fn method_not_allowed() -> Response {
    let mut response = (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({ "error": "method not allowed" })),
    )
        .into_response();
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST"));
    response
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))).into_response()
}

/// RFC 6750 §3.1: a valid token that lacks the scope this resource needs.
fn insufficient_scope() -> Response {
    let mut response = (
        StatusCode::FORBIDDEN,
        Json(json!({ "error": "insufficient_scope", "required_scope": READ_SCOPE })),
    )
        .into_response();
    let challenge = format!(
        "Bearer error=\"insufficient_scope\", error_description=\"the token has no {READ_SCOPE} scope\", scope=\"{READ_SCOPE}\""
    );
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_str(&challenge)
            .unwrap_or_else(|_| HeaderValue::from_static("Bearer error=\"insufficient_scope\"")),
    );
    response
}

/// `GET /.well-known/oauth-protected-resource`.
async fn metadata(State(state): State<Arc<AppState>>) -> Response {
    protected_resource(&state)
}

/// `GET /.well-known/oauth-protected-resource/mcp`: the same document for the resource's
/// own path, which is where a client that knows only `/mcp` looks.
async fn metadata_mcp(State(state): State<Arc<AppState>>) -> Response {
    protected_resource(&state)
}

/// RFC 9728 metadata: which resource the token is for, and which authorization server
/// mints it. 404 with the switch off, like every MCP route.
fn protected_resource(state: &AppState) -> Response {
    if state.mcp.is_none() {
        return not_found();
    }
    let auth = state.auth.as_ref().expect("mcp implies oidc");
    Json(json!({
        "resource": format!("{}/mcp", auth.config.public_url),
        "authorization_servers": [auth.config.issuer],
        "bearer_methods_supported": ["header"],
        "scopes_supported": [READ_SCOPE],
    }))
    .into_response()
}

/// Whether `origin` is the origin of `public_url`: scheme, host and port. Anything that
/// does not parse is not the origin.
fn same_origin(public_url: &str, origin: &str) -> bool {
    let (Ok(expected), Ok(given)) = (Url::parse(public_url), Url::parse(origin)) else {
        return false;
    };
    expected.scheme() == given.scheme()
        && expected.host_str() == given.host_str()
        && expected.port_or_known_default() == given.port_or_known_default()
}

/// The JSON-RPC request body. Origin first (a browser call from another site is refused
/// before anything else), then the resource's own bearer token, then the method.
async fn handle(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    let Some(cfg) = state.mcp.clone() else {
        return not_found();
    };
    let auth = state.auth.as_ref().expect("mcp implies oidc");
    let public_url = auth.config.public_url.clone();

    // A browser sends `Origin`; a command line client does not, and is allowed. A present
    // Origin that is not Nova's own origin (or that does not parse) is refused.
    if let Some(origin) = headers.get(header::ORIGIN) {
        let allowed = origin
            .to_str()
            .is_ok_and(|origin| same_origin(&public_url, origin));
        if !allowed {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "origin not allowed" })),
            )
                .into_response();
        }
    }

    let audiences = [cfg.audience.clone()];
    let (principal, facts) = match auth
        .authenticate_bearer_for_facts(&headers, &audiences)
        .await
    {
        Ok(ok) => ok,
        Err(err) => {
            // The MCP challenge names the metadata document, so a client that never heard
            // of this resource can find out where to sign in (RFC 9728 §5.1).
            let invalid = !matches!(err, AuthError::Unauthenticated);
            let mut response = err.into_response();
            let mut challenge = format!(
                "Bearer resource_metadata=\"{public_url}/.well-known/oauth-protected-resource/mcp\", scope=\"{READ_SCOPE}\""
            );
            if invalid {
                challenge.push_str(", error=\"invalid_token\"");
            }
            if let Ok(value) = HeaderValue::from_str(&challenge) {
                response
                    .headers_mut()
                    .insert(header::WWW_AUTHENTICATE, value);
            }
            return response;
        }
    };

    if !facts.scopes.iter().any(|s| s == READ_SCOPE) {
        return insufficient_scope();
    }

    if !cfg.allowed_clients.is_empty() {
        let allowed = facts
            .client_id
            .as_deref()
            .is_some_and(|client| cfg.allowed_clients.iter().any(|c| c == client));
        if !allowed {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "client not allowed", "client": facts.client_id })),
            )
                .into_response();
        }
    }

    let request: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return rpc_error(None, -32700, "parse error"),
    };
    // A batch is outside what this endpoint offers; one request per call.
    if request.is_array() {
        return rpc_error(None, -32600, "invalid request");
    }
    let id = request.get("id").cloned();
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");
    match method {
        "initialize" => rpc_result(
            id,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "stardelt", "version": env!("CARGO_PKG_VERSION") },
            }),
        ),
        // A notification: nothing to answer, and no id to answer it with.
        "notifications/initialized" => StatusCode::ACCEPTED.into_response(),
        "tools/list" => rpc_result(id, json!({ "tools": tools() })),
        "tools/call" => {
            if !take_call_slot(&cfg.calls, &principal.identity.sub, cfg.calls_per_minute) {
                tracing::info!(
                    target: "audit",
                    sub = %principal.identity.sub,
                    client = facts.client_id.as_deref().unwrap_or(""),
                    tool = "tools/call",
                    status = "rate_limited",
                    "mcp tool call"
                );
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [(header::RETRY_AFTER, HeaderValue::from_static("60"))],
                    Json(json!({
                        "error": "too_many_requests",
                        "limit": cfg.calls_per_minute,
                        "window_seconds": 60,
                    })),
                )
                    .into_response();
            }
            call_tool(&state, &cfg, &principal, &facts, id, &request).await
        }
        _ => rpc_error(id, -32601, "method not found"),
    }
}

/// The eleven read-only tools, in a fixed order so a client's menu does not move.
fn tools() -> Value {
    json!([
        {
            "name": "platform_status",
            "description": "Whether the platform's control plane is up, whether its state store is enabled, and which stage this Nova serves.",
            "inputSchema": { "type": "object", "properties": {}, "additionalProperties": false },
            "annotations": { "readOnlyHint": true },
        },
        {
            "name": "catalog_list",
            "description": "The catalogs the signed-in person may browse, with their medallion layer and owner.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "catalog": { "type": "string", "description": "Catalog to work in; omitted, the default one" },
                },
                "additionalProperties": false,
            },
            "annotations": { "readOnlyHint": true },
        },
        {
            "name": "catalog_describe",
            "description": "Iceberg metadata for one table: schema, snapshots and properties.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "namespace": { "type": "string" },
                    "table": { "type": "string" },
                    "catalog": { "type": "string" },
                },
                "required": ["namespace", "table"],
                "additionalProperties": false,
            },
            "annotations": { "readOnlyHint": true },
        },
        {
            "name": "catalog_search",
            "description": "Find tables by name: words separated by spaces, every word must be there.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "catalog": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100 },
                },
                "required": ["query"],
                "additionalProperties": false,
            },
            "annotations": { "readOnlyHint": true },
        },
        {
            "name": "sql_read",
            "description": "Run ONE read-only SQL statement (SELECT, WITH, VALUES, SHOW, DESCRIBE, TABLE, EXPLAIN) as the signed-in person. At most 200 rows by default (the platform sets the cap, 1 000 at most); a statement that runs longer than 60 seconds is cancelled. Writes, a second statement and EXPLAIN ANALYZE are refused. Aggregate or add a LIMIT instead of reading everything.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sql": { "type": "string" },
                    "maxRows": { "type": "integer", "minimum": 1, "maximum": 1000 },
                },
                "required": ["sql"],
                "additionalProperties": false,
            },
            "annotations": { "readOnlyHint": true },
        },
        {
            "name": "sql_explain",
            "description": "Show the query plan of ONE read-only SQL statement without running it (EXPLAIN, never EXPLAIN ANALYZE).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sql": { "type": "string" },
                },
                "required": ["sql"],
                "additionalProperties": false,
            },
            "annotations": { "readOnlyHint": true },
        },
        {
            "name": "query_log_list",
            "description": "The signed-in person's own recent queries: state, duration, statement preview.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100 },
                },
                "additionalProperties": false,
            },
            "annotations": { "readOnlyHint": true },
        },
        {
            "name": "pipeline_list",
            "description": "The pipelines the signed-in person may see: owner group, schedule, state and last run.",
            "inputSchema": { "type": "object", "properties": {}, "additionalProperties": false },
            "annotations": { "readOnlyHint": true },
        },
        {
            "name": "pipeline_runs",
            "description": "Recent runs of the pipelines the signed-in person may see, newest first. Narrow with pipeline (<namespace>/<name>) and state (for example failed).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline": { "type": "string", "description": "<namespace>/<name>" },
                    "state": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100 },
                },
                "additionalProperties": false,
            },
            "annotations": { "readOnlyHint": true },
        },
        {
            "name": "pipeline_run",
            "description": "One run of a pipeline with its tasks and their states: find the task that failed.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline": { "type": "string", "description": "<namespace>/<name>" },
                    "run_id": { "type": "string" },
                },
                "required": ["pipeline", "run_id"],
                "additionalProperties": false,
            },
            "annotations": { "readOnlyHint": true },
        },
        {
            "name": "pipeline_task_log",
            "description": "One page of a task's log, with the error and stack frames of a failed task. Pass next_token from the previous answer to read on.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pipeline": { "type": "string", "description": "<namespace>/<name>" },
                    "run_id": { "type": "string" },
                    "task_id": { "type": "string" },
                    "try": { "type": "integer", "minimum": 1 },
                    "next_token": { "type": "string" },
                },
                "required": ["pipeline", "run_id", "task_id"],
                "additionalProperties": false,
            },
            "annotations": { "readOnlyHint": true },
        },
    ])
}

/// `tools/call`: run one read tool as the caller and answer in MCP's shape.
async fn call_tool(
    state: &Arc<AppState>,
    cfg: &McpConfig,
    principal: &Principal,
    facts: &BearerFacts,
    id: Option<Value>,
    request: &Value,
) -> Response {
    let params = request.get("params").cloned().unwrap_or(Value::Null);
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
    let argument = |key: &str| {
        arguments
            .get(key)
            .and_then(Value::as_str)
            .map(str::to_string)
    };

    let started = std::time::Instant::now();
    let (status, body) = match name {
        "platform_status" => (StatusCode::OK, platform_status(state)),
        "catalog_list" => {
            let response =
                catalog::list_catalogs(State(state.clone()), Some(Extension(principal.clone())))
                    .await;
            body_of(response).await
        }
        "catalog_describe" => {
            let ns = argument("namespace").unwrap_or_default();
            let tbl = argument("table").unwrap_or_default();
            let response = catalog::table_metadata(
                State(state.clone()),
                axum::extract::Path((ns, tbl)),
                axum::extract::Query(catalog::CatalogParam {
                    catalog: argument("catalog"),
                }),
                Some(Extension(principal.clone())),
            )
            .await;
            body_of(response).await
        }
        "catalog_search" => {
            let query = arguments
                .get("query")
                .and_then(Value::as_str)
                .map(str::to_string);
            let limit = arguments.get("limit").and_then(Value::as_u64);
            let response = catalog_search::search(
                State(state.clone()),
                axum::extract::Query(catalog_search::SearchParams {
                    q: query,
                    catalog: argument("catalog"),
                    limit: limit.map(|n| n as usize),
                    tag: None,
                    owner: None,
                    domain: None,
                }),
                Some(Extension(principal.clone())),
            )
            .await;
            body_of(response).await
        }
        "sql_read" | "sql_explain" => {
            let explain = name == "sql_explain";
            let sql = arguments
                .get("sql")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let statement = match classify(sql, explain) {
                Ok(statement) => statement,
                Err(message) => {
                    return tool_result(id, StatusCode::BAD_REQUEST, json!({ "error": message }));
                }
            };
            let asked = arguments
                .get("maxRows")
                .and_then(Value::as_u64)
                .map(|n| n as usize);
            let limit = asked.unwrap_or(cfg.max_rows).clamp(1, cfg.max_rows);
            match run_statement(state, cfg, principal, &statement, limit).await {
                Ok(result) => (
                    StatusCode::OK,
                    serde_json::to_value(result).unwrap_or(Value::Null),
                ),
                Err(e) => (crate::trino::error_status(&e), json!({ "error": e })),
            }
        }
        "query_log_list" => {
            let limit = arguments
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or(20)
                .clamp(1, 100);
            let mut query = std::collections::HashMap::new();
            query.insert("scope".to_string(), "mine".to_string());
            query.insert("limit".to_string(), limit.to_string());
            let response = crate::query_log_api::list(
                State(state.clone()),
                Some(Extension(principal.clone())),
                axum::extract::Query(query),
            )
            .await;
            body_of(response).await
        }
        "pipeline_list" => {
            let response = crate::pipelines::list_for(state, &principal.identity).await;
            body_of(response).await
        }
        "pipeline_runs" => {
            let limit = arguments
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or(20)
                .clamp(1, 100);
            let mut query = std::collections::BTreeMap::new();
            query.insert("limit".to_string(), limit.to_string());
            if let Some(pipeline) = argument("pipeline") {
                query.insert("pipeline".to_string(), pipeline);
            }
            if let Some(run_state) = argument("state") {
                query.insert("state".to_string(), run_state);
            }
            let response = crate::pipelines::runs_for(state, &principal.identity, &query).await;
            body_of(response).await
        }
        "pipeline_run" | "pipeline_task_log" => {
            let pipeline = argument("pipeline").unwrap_or_default();
            let run_id = argument("run_id").unwrap_or_default();
            match pipeline.split_once('/') {
                Some((namespace, pipeline_name))
                    if !namespace.is_empty() && !pipeline_name.is_empty() && !run_id.is_empty() =>
                {
                    let response = if name == "pipeline_run" {
                        crate::pipelines::run_for(
                            state,
                            &principal.identity,
                            namespace,
                            pipeline_name,
                            &run_id,
                        )
                        .await
                    } else {
                        let task_id = argument("task_id").unwrap_or_default();
                        let try_number = arguments
                            .get("try")
                            .and_then(Value::as_u64)
                            .and_then(|n| u32::try_from(n).ok());
                        let token = argument("next_token");
                        crate::pipelines::task_log_for(
                            state,
                            &principal.identity,
                            namespace,
                            pipeline_name,
                            &run_id,
                            &task_id,
                            try_number,
                            token.as_deref(),
                        )
                        .await
                    };
                    body_of(response).await
                }
                _ => (
                    StatusCode::BAD_REQUEST,
                    json!({ "error": "pipeline must be <namespace>/<name>, and run_id must be given" }),
                ),
            }
        }
        other => return rpc_error(id, -32602, &format!("unknown tool: {other}")),
    };

    let is_error = !status.is_success();
    let outcome = if is_error { "error" } else { "ok" };
    // One line per tool call, before the answer: who ran what, with which client, how it
    // ended, how long, and a fingerprint of the arguments (never the arguments themselves).
    let arguments_sha256 = arguments_hash(&arguments);
    tracing::info!(
        target: "audit",
        sub = %principal.identity.sub,
        client = facts.client_id.as_deref().unwrap_or(""),
        tool = %name,
        status = %outcome,
        duration_ms = started.elapsed().as_millis() as u64,
        arguments_sha256 = %arguments_sha256,
        "mcp tool call"
    );

    tool_result(id, status, body)
}

/// `sql_read`/`sql_explain`: one statement, as the caller, with the row cap and the timeout.
async fn run_statement(
    state: &Arc<AppState>,
    cfg: &McpConfig,
    principal: &Principal,
    statement: &str,
    max_rows: usize,
) -> Result<crate::trino::QueryResult, String> {
    let run = crate::trino::execute_from(
        state,
        statement.to_string(),
        Some(principal),
        "stardelt-mcp",
    );
    match tokio::time::timeout(cfg.query_timeout, run).await {
        Err(_) => Err(format!(
            "the statement did not finish in {} seconds and was cancelled; aggregate or add a LIMIT",
            cfg.query_timeout.as_secs()
        )),
        Ok(Err(e)) => Err(e),
        Ok(Ok(mut result)) => {
            if result.rows.len() > max_rows {
                result.rows.truncate(max_rows);
                result.row_count = result.rows.len();
                result.truncated = true;
            }
            Ok(result)
        }
    }
}

/// The words a guarded statement may start with, plus `explain`.
const READ_KEYWORDS: [&str; 6] = ["select", "with", "values", "show", "describe", "table"];

/// `sql` without comments (quoted text kept), and whether a second statement follows the first.
fn strip_comments(sql: &str) -> (String, bool) {
    let chars: Vec<char> = sql.chars().collect();
    let mut out: Vec<char> = Vec::new();
    let mut semicolons: Vec<usize> = Vec::new();
    let mut quote: Option<char> = None;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if let Some(q) = quote {
            out.push(c);
            if c == q {
                quote = None;
            }
            i += 1;
        } else if c == '\'' || c == '"' {
            quote = Some(c);
            out.push(c);
            i += 1;
        } else if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            let mut end = i + 2;
            while end + 1 < chars.len() && !(chars[end] == '*' && chars[end + 1] == '/') {
                end += 1;
            }
            i = if end + 1 < chars.len() {
                end + 2
            } else {
                chars.len()
            };
            out.push(' ');
        } else {
            if c == ';' {
                semicolons.push(out.len());
            }
            out.push(c);
            i += 1;
        }
    }
    let text: String = out.into_iter().collect();
    let another = semicolons.iter().any(|at| {
        !text
            .chars()
            .skip(at + 1)
            .collect::<String>()
            .trim_matches([' ', '\t', '\r', '\n', ';'])
            .is_empty()
    });
    (text, another)
}

/// The statement to send, or the sentence that says why it is refused. Ported from the local
/// server's `guarded_sql.classify`, so both see the same statements and the same sentences.
fn classify(sql: &str, explain: bool) -> Result<String, String> {
    if sql.trim().is_empty() {
        return Err("the statement is empty".into());
    }
    let (text, another) = strip_comments(sql);
    if another {
        return Err("exactly one statement is allowed".into());
    }
    let statement = text.trim().trim_end_matches(';').trim().to_string();
    let words: Vec<String> = statement
        .split_whitespace()
        .map(|word| word.to_lowercase())
        .collect();
    let Some(first_word) = words.first() else {
        return Err("the statement is empty".into());
    };
    let first = first_word.trim_start_matches('(');
    if first == "explain" {
        // EXPLAIN plans without running; EXPLAIN ANALYZE runs the statement and is refused.
        if words.get(1).is_some_and(|next| next == "analyze") {
            return Err("EXPLAIN ANALYZE runs the statement and is refused; use EXPLAIN".into());
        }
        return Ok(statement);
    }
    if !READ_KEYWORDS.contains(&first) {
        return Err(format!(
            "only reading statements run here (SELECT, WITH, VALUES, SHOW, DESCRIBE, TABLE), not {}",
            first.to_uppercase()
        ));
    }
    Ok(if explain {
        format!("EXPLAIN {statement}")
    } else {
        statement
    })
}

/// `platform_status` needs no handler: the state the process was built with.
fn platform_status(state: &AppState) -> Value {
    json!({
        "status": "ok",
        "db": if state.db.is_some() { "up" } else { "disabled" },
        "stage": state.stage_name.clone(),
        "kind": state.stage_kind,
    })
}

/// A handler's `Response` as its status and JSON body (an unreadable body is `null`).
async fn body_of(response: Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 2_000_000)
        .await
        .unwrap_or_default();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Take one `tools/call` slot for `sub`, or report that the last minute is full.
fn take_call_slot(limiter: &CallsLimiter, sub: &str, limit: usize) -> bool {
    let mut map = limiter.lock().expect("mcp call limiter poisoned");
    let now = Instant::now();
    let slots = map.entry(sub.to_string()).or_default();
    while slots
        .front()
        .is_some_and(|then| now.duration_since(*then) >= std::time::Duration::from_secs(60))
    {
        slots.pop_front();
    }
    if slots.len() >= limit {
        false
    } else {
        slots.push_back(now);
        true
    }
}

/// SHA-256 of a tool call's `arguments` JSON, lowercase hex. Stable for identical JSON so
/// two calls can be compared, and one-way so the log never holds a statement or a value.
fn arguments_hash(arguments: &Value) -> String {
    let text = serde_json::to_vec(arguments).unwrap_or_default();
    Sha256::digest(&text)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// MCP's tool answer: the JSON as text in `content`, the JSON itself in
/// `structuredContent`. A refusal is `isError` with the handler's own sentence; a body
/// past the context budget loses `structuredContent` and says where it was cut.
fn tool_result(id: Option<Value>, status: StatusCode, body: Value) -> Response {
    let result = if status.is_success() {
        let text = serde_json::to_string(&body).unwrap_or_else(|_| "null".into());
        if text.chars().count() > MAX_TOOL_TEXT_CHARS {
            let cut: String = text.chars().take(MAX_TOOL_TEXT_CHARS).collect();
            json!({
                "content": [{
                    "type": "text",
                    "text": format!("{cut}\n…truncated; narrow the request"),
                }],
            })
        } else {
            json!({
                "content": [{ "type": "text", "text": text }],
                "structuredContent": body,
            })
        }
    } else {
        let sentence = body
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("the request failed");
        json!({
            "content": [{ "type": "text", "text": sentence }],
            "isError": true,
        })
    };
    rpc_result(id, result)
}

/// A JSON-RPC result: `200`, `application/json`, the request's `id` echoed (`null` when
/// there was none).
fn rpc_result(id: Option<Value>, result: Value) -> Response {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id.unwrap_or(Value::Null),
        "result": result,
    }))
    .into_response()
}

/// A JSON-RPC error in the standard shape.
fn rpc_error(id: Option<Value>, code: i64, message: &str) -> Response {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id.unwrap_or(Value::Null),
        "error": { "code": code, "message": message },
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::Body;
    use axum::http::Request;
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::auth::{
        self, BearerFacts, BearerValidator, BoxFuture, ClaimConfig, Identity, OidcConfig, Provider,
        Secret,
    };

    /// A validator that accepts `"good"` for the MCP audience only, like the IdP.
    struct Stub;

    impl BearerValidator for Stub {
        fn validate<'a>(&'a self, token: &'a str) -> BoxFuture<'a, Result<Identity, AuthError>> {
            Box::pin(async move { validate(token) })
        }

        fn validate_for<'a>(
            &'a self,
            token: &'a str,
            audiences: &'a [String],
        ) -> BoxFuture<'a, Result<Identity, AuthError>> {
            Box::pin(async move {
                if audiences.len() == 1 && audiences[0] == "stardelt-mcp" {
                    validate(token)
                } else {
                    Err(AuthError::InvalidToken("wrong audience"))
                }
            })
        }

        fn validate_for_scopes<'a>(
            &'a self,
            token: &'a str,
            audiences: &'a [String],
        ) -> BoxFuture<'a, Result<(Identity, Vec<String>), AuthError>> {
            Box::pin(async move {
                if !(audiences.len() == 1 && audiences[0] == "stardelt-mcp") {
                    return Err(AuthError::InvalidToken("wrong audience"));
                }
                let identity = validate(token)?;
                let scopes = if token == "good" {
                    vec![READ_SCOPE.to_string()]
                } else {
                    vec!["openid".to_string()]
                };
                Ok((identity, scopes))
            })
        }

        fn validate_for_facts<'a>(
            &'a self,
            token: &'a str,
            audiences: &'a [String],
        ) -> BoxFuture<'a, Result<(Identity, BearerFacts), AuthError>> {
            Box::pin(async move {
                if !(audiences.len() == 1 && audiences[0] == "stardelt-mcp") {
                    return Err(AuthError::InvalidToken("wrong audience"));
                }
                let identity = validate(token)?;
                let (scopes, client_id) = match token {
                    "good" => (
                        vec![READ_SCOPE.to_string()],
                        Some("stardelt-mcp".to_string()),
                    ),
                    "other" => (vec![READ_SCOPE.to_string()], Some("rogue".to_string())),
                    _ => (vec!["openid".to_string()], None),
                };
                Ok((identity, BearerFacts { scopes, client_id }))
            })
        }
    }

    fn validate(token: &str) -> Result<Identity, AuthError> {
        if !matches!(token, "good" | "plain" | "other") {
            return Err(AuthError::InvalidToken("bad signature"));
        }
        Ok(Identity {
            sub: "alice".into(),
            display_name: "alice".into(),
            email: None,
            name: None,
            groups: vec![],
            roles: vec![],
            groups_overage: false,
            is_admin: false,
        })
    }

    fn oidc_config() -> OidcConfig {
        OidcConfig {
            issuer: "https://id.example.com/realms/stardelt".into(),
            client_id: "nova".into(),
            client_secret: Secret::new("s"),
            scopes: "openid".into(),
            audiences: vec!["nova".into()],
            public_url: "https://nova.example.com".into(),
            claims: ClaimConfig::default(),
            session_secret: Secret::new("0123456789abcdef0123456789abcdef"),
            device_client_id: None,
        }
    }

    fn stub_auth() -> Arc<auth::AuthState> {
        auth::AuthState::with_validator(
            oidc_config(),
            Arc::new(Provider::new(
                reqwest::Client::new(),
                "https://id.example.com/realms/stardelt".into(),
            )),
            Arc::new(Stub),
            None,
        )
    }

    /// The app the image serves: the MCP routes, `/api`, and the SPA fallback.
    fn test_app_with(
        tag: &str,
        enabled: bool,
        allowed_clients: Vec<String>,
    ) -> (Router, crate::router_tests::StaticDir) {
        let mut state = crate::router_tests::test_state();
        if enabled {
            let s = Arc::get_mut(&mut state).expect("a fresh state");
            s.auth = Some(stub_auth());
            s.mcp = Some(McpConfig {
                audience: "stardelt-mcp".into(),
                max_rows: 200,
                query_timeout: std::time::Duration::from_secs(60),
                allowed_clients,
                calls_per_minute: CALLS_PER_MINUTE,
                calls: Arc::new(Mutex::new(HashMap::new())),
            });
        }
        let dir = crate::router_tests::StaticDir::new(tag);
        let app = crate::app(state, crate::api_router(), dir.path());
        (app, dir)
    }

    fn test_app_with_limit(
        tag: &str,
        calls_per_minute: usize,
    ) -> (Router, crate::router_tests::StaticDir) {
        let mut state = crate::router_tests::test_state();
        let s = Arc::get_mut(&mut state).expect("a fresh state");
        s.auth = Some(stub_auth());
        s.mcp = Some(McpConfig {
            audience: "stardelt-mcp".into(),
            max_rows: 200,
            query_timeout: std::time::Duration::from_secs(60),
            allowed_clients: vec![],
            calls_per_minute,
            calls: Arc::new(Mutex::new(HashMap::new())),
        });
        let dir = crate::router_tests::StaticDir::new(tag);
        let app = crate::app(state, crate::api_router(), dir.path());
        (app, dir)
    }

    fn test_app(tag: &str, enabled: bool) -> (Router, crate::router_tests::StaticDir) {
        test_app_with(tag, enabled, Vec::new())
    }

    async fn send(app: Router, request: Request<Body>) -> Response {
        app.oneshot(request).await.unwrap()
    }

    fn post(body: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    fn authorized(mut request: Request<Body>) -> Request<Body> {
        request
            .headers_mut()
            .insert(header::AUTHORIZATION, "Bearer good".parse().unwrap());
        request
    }

    fn authorized_with(token: &str, mut request: Request<Body>) -> Request<Body> {
        request.headers_mut().insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        request
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    async fn json_body(response: Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the body reads");
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    }

    #[tokio::test]
    async fn disabled_is_404_everywhere() {
        let (app, _dir) = test_app("mcp-disabled", false);
        for (method, uri) in [
            ("POST", "/mcp"),
            ("GET", "/.well-known/oauth-protected-resource"),
            ("GET", "/.well-known/oauth-protected-resource/mcp"),
        ] {
            let request = Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap();
            let response = send(app.clone(), request).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {uri}");
            assert_eq!(
                json_body(response).await,
                json!({ "error": "not found" }),
                "{method} {uri}"
            );
        }
        // The SPA fallback is untouched.
        assert_eq!(send(app, get("/")).await.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metadata_is_public_json() {
        let (app, _dir) = test_app("mcp-metadata", true);
        for uri in [
            "/.well-known/oauth-protected-resource",
            "/.well-known/oauth-protected-resource/mcp",
        ] {
            let response = send(app.clone(), get(uri)).await;
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            assert!(
                response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .is_some_and(|v| v.starts_with("application/json")),
                "{uri} answers JSON"
            );
            let body = json_body(response).await;
            assert_eq!(body["resource"], "https://nova.example.com/mcp");
            assert_eq!(
                body["authorization_servers"],
                json!(["https://id.example.com/realms/stardelt"])
            );
        }
    }

    #[tokio::test]
    async fn post_without_token_is_401() {
        let (app, _dir) = test_app("mcp-401", true);
        let response = send(app, post("{}")).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let challenge = response
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(
            challenge.starts_with(
                "Bearer resource_metadata=\"https://nova.example.com/.well-known/oauth-protected-resource/mcp\""
            ),
            "{challenge}"
        );
    }

    #[tokio::test]
    async fn the_call_limit_refuses_the_third_call_and_lists_do_not_count() {
        let (app, _dir) = test_app_with_limit("mcp-limit", 2);
        let call = r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"platform_status","arguments":{}}}"#;
        // 1st and 2nd: 200. `tools/list` in between: 200 and no slot.
        for _ in 0..2 {
            let response = send(app.clone(), authorized(post(call))).await;
            assert_eq!(response.status(), StatusCode::OK);
            let listed = send(
                app.clone(),
                authorized(post(r#"{"jsonrpc":"2.0","id":9,"method":"tools/list"}"#)),
            )
            .await;
            assert_eq!(listed.status(), StatusCode::OK);
        }
        // 3rd: 429 with the header and the body the client can act on.
        let refused = send(app, authorized(post(call))).await;
        assert_eq!(refused.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            refused
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("60")
        );
        let body = json_body(refused).await;
        assert_eq!(body["error"], "too_many_requests");
        assert_eq!(body["limit"], 2);
        assert_eq!(body["window_seconds"], 60);
    }

    #[tokio::test]
    async fn foreign_origin_is_403() {
        let (app, _dir) = test_app("mcp-origin", true);

        let request = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("origin", "https://evil.example")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        let response = send(app.clone(), request).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            json_body(response).await,
            json!({ "error": "origin not allowed" })
        );

        // Nova's own origin passes the check and is then refused for the missing token.
        let request = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("origin", "https://nova.example.com")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap();
        assert_eq!(send(app, request).await.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn initialize_then_tools_list() {
        let (app, _dir) = test_app("mcp-tools", true);

        let response = send(
            app.clone(),
            authorized(post(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
            )),
        )
        .await;
        let body = json_body(response).await;
        assert_eq!(body["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(body["id"], 1);

        let response = send(
            app.clone(),
            authorized(post(r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#)),
        )
        .await;
        let body = json_body(response).await;
        let names: Vec<&str> = body["result"]["tools"]
            .as_array()
            .expect("tools")
            .iter()
            .map(|tool| tool["name"].as_str().unwrap_or_default())
            .collect();
        assert_eq!(
            names,
            vec![
                "platform_status",
                "catalog_list",
                "catalog_describe",
                "catalog_search",
                "sql_read",
                "sql_explain",
                "query_log_list",
                "pipeline_list",
                "pipeline_runs",
                "pipeline_run",
                "pipeline_task_log"
            ]
        );

        let response = send(
            app,
            authorized(post(
                r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"platform_status","arguments":{}}}"#,
            )),
        )
        .await;
        let body = json_body(response).await;
        assert!(body["result"].get("isError").is_none(), "{body}");
        let text = body["result"]["content"][0]["text"]
            .as_str()
            .expect("the text");
        let status: Value = serde_json::from_str(text).expect("the tool text is JSON");
        assert_eq!(status["status"], "ok");
        assert_eq!(status["db"], "disabled");
    }

    /// One `tools/call`'s `result`, as MCP answers it.
    async fn call(app: Router, name: &str, arguments: Value) -> Value {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": { "name": name, "arguments": arguments },
        });
        let response = send(app, authorized(post(&body.to_string()))).await;
        let body = json_body(response).await;
        body["result"].clone()
    }

    fn text_of(result: &Value) -> &str {
        result["content"][0]["text"].as_str().expect("the text")
    }

    #[test]
    fn classify_accepts_a_commented_select() {
        assert_eq!(classify("/* c */ select 1", false), Ok("select 1".into()));
        assert_eq!(
            classify("WITH x AS (SELECT 1) SELECT * FROM x", false),
            Ok("WITH x AS (SELECT 1) SELECT * FROM x".into())
        );
        assert_eq!(classify("select 1", true), Ok("EXPLAIN select 1".into()));
        assert_eq!(
            classify("-- nothing", false),
            Err("the statement is empty".into())
        );
    }

    #[test]
    fn classify_refuses_a_write() {
        let message = classify("DROP TABLE t", false).expect_err("a write is refused");
        assert!(
            message.contains("only reading statements run here"),
            "{message}"
        );
        assert!(message.contains("DROP"), "{message}");
    }

    #[test]
    fn classify_refuses_two_statements() {
        assert_eq!(
            classify("SELECT 1; SELECT 2", false),
            Err("exactly one statement is allowed".into())
        );
    }

    #[test]
    fn classify_refuses_explain_analyze() {
        let message = classify("EXPLAIN ANALYZE SELECT 1", false).expect_err("refused");
        assert!(message.contains("EXPLAIN ANALYZE"), "{message}");
    }

    #[tokio::test]
    async fn sql_read_refuses_a_write_before_the_engine() {
        let (app, _dir) = test_app("mcp-sql-write", true);
        let result = call(app, "sql_read", json!({ "sql": "DROP TABLE t" })).await;
        assert_eq!(result["isError"], true, "{result}");
        assert!(
            text_of(&result).contains("only reading statements run here"),
            "{result}"
        );
    }

    #[tokio::test]
    async fn sql_read_refuses_explain_analyze() {
        let (app, _dir) = test_app("mcp-sql-analyze", true);
        let result = call(
            app,
            "sql_read",
            json!({ "sql": "EXPLAIN ANALYZE SELECT 1" }),
        )
        .await;
        assert_eq!(result["isError"], true, "{result}");
        assert!(text_of(&result).contains("EXPLAIN ANALYZE"), "{result}");
    }

    #[tokio::test]
    async fn sql_read_reaches_the_engine() {
        // `test_state()` points Trino at `http://127.0.0.1:1`: an answer here proves the
        // statement passed the classifier and was sent to the engine.
        let (app, _dir) = test_app("mcp-sql-engine", true);
        let result = call(app, "sql_read", json!({ "sql": "SELECT 1" })).await;
        assert_eq!(result["isError"], true, "{result}");
        assert_eq!(text_of(&result), "the query engine is not reachable");
    }

    #[tokio::test]
    async fn query_log_list_without_a_state_store() {
        let (app, _dir) = test_app("mcp-query-log", true);
        let result = call(app, "query_log_list", json!({})).await;
        assert_eq!(result["isError"], true, "{result}");
        assert!(
            text_of(&result).contains("this needs Nova's database"),
            "{result}"
        );
    }

    #[tokio::test]
    async fn pipeline_list_without_a_cluster() {
        let (app, _dir) = test_app("mcp-pipeline-list", true);
        let result = call(app, "pipeline_list", json!({})).await;
        assert_eq!(result["isError"], true, "{result}");
        assert!(
            text_of(&result).contains("no cluster connection"),
            "{result}"
        );
    }

    #[tokio::test]
    async fn pipeline_runs_without_a_cluster() {
        let (app, _dir) = test_app("mcp-pipeline-runs", true);
        let result = call(app, "pipeline_runs", json!({})).await;
        assert_eq!(result["isError"], true, "{result}");
        assert!(
            text_of(&result).contains("no cluster connection"),
            "{result}"
        );
    }

    #[tokio::test]
    async fn pipeline_runs_rejects_a_bad_pipeline_argument() {
        let (app, _dir) = test_app("mcp-pipeline-bad", true);
        let result = call(app, "pipeline_runs", json!({ "pipeline": "no-slash" })).await;
        assert_eq!(result["isError"], true, "{result}");
    }

    #[tokio::test]
    async fn pipeline_run_without_a_cluster() {
        let (app, _dir) = test_app("mcp-pipeline-run", true);
        let result = call(
            app,
            "pipeline_run",
            json!({ "pipeline": "stardelt/shop", "run_id": "r1" }),
        )
        .await;
        assert_eq!(result["isError"], true, "{result}");
        assert!(
            text_of(&result).contains("no cluster connection"),
            "{result}"
        );
    }

    #[tokio::test]
    async fn pipeline_task_log_without_a_cluster() {
        let (app, _dir) = test_app("mcp-pipeline-task-log", true);
        let result = call(
            app,
            "pipeline_task_log",
            json!({ "pipeline": "stardelt/shop", "run_id": "r1", "task_id": "t1" }),
        )
        .await;
        assert_eq!(result["isError"], true, "{result}");
        assert!(
            text_of(&result).contains("no cluster connection"),
            "{result}"
        );
    }

    #[tokio::test]
    async fn pipeline_run_needs_namespace_and_run() {
        let (app, _dir) = test_app("mcp-pipeline-run-args", true);
        let result = call(
            app.clone(),
            "pipeline_run",
            json!({ "pipeline": "no-slash", "run_id": "r1" }),
        )
        .await;
        assert_eq!(result["isError"], true, "{result}");
        assert!(
            text_of(&result).contains("pipeline must be <namespace>/<name>"),
            "{result}"
        );

        let result = call(app, "pipeline_run", json!({ "pipeline": "stardelt/shop" })).await;
        assert_eq!(result["isError"], true, "{result}");
        assert!(
            text_of(&result).contains("pipeline must be <namespace>/<name>"),
            "{result}"
        );
    }

    #[tokio::test]
    async fn get_mcp_is_405() {
        let (app, _dir) = test_app("mcp-405", true);
        let response = send(app, get("/mcp")).await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        let allow = response
            .headers()
            .get(header::ALLOW)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(allow.contains("POST"), "{allow}");
    }

    #[test]
    fn from_lookup() {
        // Off: no configuration at all.
        assert!(
            McpConfig::from_lookup(|_| None, true)
                .expect("off")
                .is_none()
        );

        // On without an audience: refuses, naming the variable.
        let err =
            McpConfig::from_lookup(|k| (k == "NOVA_MCP_ENABLED").then(|| "1".to_string()), true)
                .expect_err("an audience is required");
        assert!(err.contains("NOVA_MCP_AUDIENCE"), "{err}");

        // On without oidc: there is no anonymous mode.
        let err = McpConfig::from_lookup(
            |k| match k {
                "NOVA_MCP_ENABLED" => Some("1".to_string()),
                "NOVA_MCP_AUDIENCE" => Some("https://nova.example.com/mcp".to_string()),
                _ => None,
            },
            false,
        )
        .expect_err("oidc is required");
        assert!(err.contains("NOVA_AUTH_MODE=oidc"), "{err}");

        // On, with an audience: the trailing slash is trimmed.
        let cfg = McpConfig::from_lookup(
            |k| match k {
                "NOVA_MCP_ENABLED" => Some("true".to_string()),
                "NOVA_MCP_AUDIENCE" => Some("https://nova.example.com/mcp/".to_string()),
                _ => None,
            },
            true,
        )
        .expect("a configuration")
        .expect("enabled");
        assert_eq!(cfg.audience, "https://nova.example.com/mcp");
        // The defaults, when neither limit variable is set.
        assert_eq!(cfg.max_rows, 200);
        assert_eq!(cfg.query_timeout, std::time::Duration::from_secs(60));

        // Explicit values are read, and clamped to 1..=1000 rows and 1..=300 seconds.
        let cfg = McpConfig::from_lookup(
            |k| match k {
                "NOVA_MCP_ENABLED" => Some("true".to_string()),
                "NOVA_MCP_AUDIENCE" => Some("https://nova.example.com/mcp".to_string()),
                "NOVA_MCP_MAX_ROWS" => Some("5000".to_string()),
                "NOVA_MCP_QUERY_TIMEOUT_SECS" => Some("90".to_string()),
                _ => None,
            },
            true,
        )
        .expect("a configuration")
        .expect("enabled");
        assert_eq!(cfg.max_rows, 1000);
        assert_eq!(cfg.query_timeout, std::time::Duration::from_secs(90));

        let cfg = McpConfig::from_lookup(
            |k| match k {
                "NOVA_MCP_ENABLED" => Some("true".to_string()),
                "NOVA_MCP_AUDIENCE" => Some("https://nova.example.com/mcp".to_string()),
                "NOVA_MCP_MAX_ROWS" => Some("0".to_string()),
                "NOVA_MCP_QUERY_TIMEOUT_SECS" => Some("0".to_string()),
                _ => None,
            },
            true,
        )
        .expect("a configuration")
        .expect("enabled");
        assert_eq!(cfg.max_rows, 1);
        assert_eq!(cfg.query_timeout, std::time::Duration::from_secs(60));

        // No allowlist by default; a configured one is trimmed and split on commas.
        assert!(cfg.allowed_clients.is_empty());
        let cfg = McpConfig::from_lookup(
            |k| match k {
                "NOVA_MCP_ENABLED" => Some("true".to_string()),
                "NOVA_MCP_AUDIENCE" => Some("https://nova.example.com/mcp".to_string()),
                "NOVA_MCP_ALLOWED_CLIENTS" => Some(" stardelt-mcp, stardelt-cli ".to_string()),
                _ => None,
            },
            true,
        )
        .expect("a configuration")
        .expect("enabled");
        assert_eq!(cfg.allowed_clients, vec!["stardelt-mcp", "stardelt-cli"]);
    }

    #[test]
    fn arguments_hash_is_stable_and_hex() {
        let a = json!({ "sql": "select 1", "maxRows": 5 });
        assert_eq!(arguments_hash(&a).len(), 64);
        assert!(arguments_hash(&a).chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(
            arguments_hash(&a),
            arguments_hash(&json!({ "sql": "select 1", "maxRows": 5 }))
        );
        assert_ne!(
            arguments_hash(&a),
            arguments_hash(&json!({ "sql": "select 2", "maxRows": 5 }))
        );
    }

    #[tokio::test]
    async fn a_client_outside_the_allowlist_is_403() {
        let (app, _dir) = test_app_with("mcp-client-403", true, vec!["stardelt-mcp".into()]);
        let list = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
        let denied = send(app.clone(), authorized_with("other", post(list))).await;
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            json_body(denied).await,
            json!({ "error": "client not allowed", "client": "rogue" })
        );
        let allowed = send(app, authorized_with("good", post(list))).await;
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn a_token_without_the_read_scope_is_403() {
        let (app, _dir) = test_app("mcp-scope-403", true);
        let response = send(
            app,
            authorized_with(
                "plain",
                post(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#),
            ),
        )
        .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let challenge = response
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(
            challenge.contains("error=\"insufficient_scope\""),
            "{challenge}"
        );
        assert!(challenge.contains("scope=\"stardelt:read\""), "{challenge}");
        assert_eq!(
            json_body(response).await,
            json!({ "error": "insufficient_scope", "required_scope": "stardelt:read" })
        );
    }

    #[tokio::test]
    async fn challenge_names_the_read_scope() {
        let (app, _dir) = test_app("mcp-scope-challenge", true);
        let response = send(
            app,
            post(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let challenge = response
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(challenge.contains("scope=\"stardelt:read\""), "{challenge}");
    }

    #[tokio::test]
    async fn metadata_names_the_read_scope() {
        let (app, _dir) = test_app("mcp-scope-metadata", true);
        let response = send(app, get("/.well-known/oauth-protected-resource/mcp")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            json_body(response).await["scopes_supported"],
            json!([READ_SCOPE])
        );
    }
}
