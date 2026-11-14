//! The administrator's audit view of the hosted MCP endpoint (design agent-access.md §4, T7).
//! One row per tool call, written from `mcp.rs` after the call; the tracing line stays. Reads
//! newest first, administrators only (`admin_router()` puts `auth::require_admin` in front).

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderValue, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use tracing::{info, warn};
use utoipa::ToSchema;

use crate::AppState;
use crate::saved_queries::{store_failed, unavailable};

/// How long rows are kept when `NOVA_MCP_AUDIT_RETENTION_DAYS` is not set.
const DEFAULT_RETENTION_DAYS: i64 = 30;

/// How often old rows are removed.
const PURGE_EVERY: Duration = Duration::from_secs(3600);

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 500;

/// One recorded call. `atMs` is epoch milliseconds, like every other time in Nova's API.
#[derive(Debug, Serialize, FromRow, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct McpCall {
    pub at_ms: i64,
    pub sub: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    pub tool: String,
    pub status: String,
    pub duration_ms: i64,
    pub arguments_sha256: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AuditParams {
    limit: Option<i64>,
    sub: Option<String>,
    tool: Option<String>,
}

/// The answer of `GET /api/admin/mcp-audit`: the calls, newest first.
#[derive(Debug, Serialize, ToSchema)]
pub struct McpCalls {
    pub calls: Vec<McpCall>,
}

/// Best effort: a failed insert must never fail the tool call.
pub(crate) async fn record(
    pool: &PgPool,
    sub: &str,
    client: Option<&str>,
    tool: &str,
    status: &str,
    duration_ms: u64,
    arguments_sha256: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO mcp_call (sub, client, tool, status, duration_ms, arguments_sha256) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(sub)
    .bind(client)
    .bind(tool)
    .bind(status)
    .bind(i64::try_from(duration_ms).unwrap_or(i64::MAX))
    .bind(arguments_sha256)
    .execute(pool)
    .await
    .map(|_| ())
}

#[utoipa::path(
    get,
    operation_id = "admin_mcp_audit",
    path = "/api/admin/mcp-audit",
    tag = "internal",
    description = "The last calls of the hosted MCP endpoint (`/mcp`), newest first: when, the signed-in subject, the client id (`azp`), the tool, the status, the duration and a SHA-256 of the arguments — never the arguments themselves, and never a result. `limit` is 1 to 500 (50 by default); `sub` and `tool` narrow by exact match. Administrator-only; a non-administrator answers 403 `this view is for platform administrators`. Without a state store the answer is 503. Answers `Cache-Control: no-store`.",
    params(
        ("limit" = Option<i64>, Query, description = "How many calls, 1 to 500 (default 50)"),
        ("sub" = Option<String>, Query, description = "Only this subject"),
        ("tool" = Option<String>, Query, description = "Only this tool"),
    ),
    responses(
        (status = 200, description = "The calls, newest first", body = McpCalls),
        (status = 401, description = "No session", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller is not a platform administrator", body = crate::openapi::ErrorResponse),
        (status = 503, description = "Nova has no state store", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn list(
    State(state): State<Arc<AppState>>,
    Query(params): Query<AuditParams>,
) -> Response {
    let Some(pool) = state.db.as_ref() else {
        return unavailable();
    };
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let rows: Result<Vec<McpCall>, sqlx::Error> = sqlx::query_as(
        "SELECT (extract(epoch FROM at) * 1000)::bigint AS at_ms, sub, client, tool, status, \
            duration_ms, arguments_sha256 FROM mcp_call \
         WHERE ($1::text IS NULL OR sub = $1) AND ($2::text IS NULL OR tool = $2) \
         ORDER BY at DESC LIMIT $3",
    )
    .bind(params.sub.as_deref())
    .bind(params.tool.as_deref())
    .bind(limit)
    .fetch_all(pool)
    .await;
    match rows {
        Ok(calls) => (
            [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
            Json(McpCalls { calls }),
        )
            .into_response(),
        Err(e) => store_failed(e),
    }
}

/// Remove old rows once an hour for as long as Nova runs.
pub(crate) fn spawn_retention(pool: PgPool) {
    let days = retention_days_from(|name| std::env::var(name).ok());
    let Some(days) = days else {
        info!("mcp audit retention is off (NOVA_MCP_AUDIT_RETENTION_DAYS=0)");
        return;
    };
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(PURGE_EVERY);
        loop {
            tick.tick().await;
            match purge(&pool, days).await {
                Ok(0) => {}
                Ok(rows) => info!(rows, days, "removed old mcp call rows"),
                Err(e) => warn!(error = %e, "mcp audit purge failed"),
            }
        }
    });
}

/// Delete the rows older than `days`.
pub(crate) async fn purge(pool: &PgPool, days: i64) -> sqlx::Result<u64> {
    let result =
        sqlx::query("DELETE FROM mcp_call WHERE at < now() - make_interval(days => $1::int)")
            .bind(days as i32)
            .execute(pool)
            .await?;
    Ok(result.rows_affected())
}

/// Retention from the environment: unset or unparsable gives the default, `0` switches the
/// purge off (`None`), anything else is the number of days.
fn retention_days_from(get: impl Fn(&str) -> Option<String>) -> Option<i64> {
    match get("NOVA_MCP_AUDIT_RETENTION_DAYS").map(|v| v.trim().to_string()) {
        None => Some(DEFAULT_RETENTION_DAYS),
        Some(v) if v.is_empty() => Some(DEFAULT_RETENTION_DAYS),
        Some(v) => match v.parse::<i64>() {
            Ok(0) => None,
            Ok(days) if days > 0 => Some(days),
            _ => {
                warn!(value = %v, "the NOVA_MCP_AUDIT_RETENTION_DAYS value is not a positive number; using the default");
                Some(DEFAULT_RETENTION_DAYS)
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_support::migrated_pool;

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn a_recorded_call_is_readable() {
        let pool = migrated_pool().await;
        record(
            &pool,
            "alice",
            Some("stardelt-mcp"),
            "sql_read",
            "ok",
            7,
            "abc",
        )
        .await
        .unwrap();
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM mcp_call")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
        let (sub, client, tool, status, duration_ms, arguments_sha256): (
            String,
            Option<String>,
            String,
            String,
            i64,
            String,
        ) = sqlx::query_as(
            "SELECT sub, client, tool, status, duration_ms, arguments_sha256 FROM mcp_call",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(sub, "alice");
        assert_eq!(client.as_deref(), Some("stardelt-mcp"));
        assert_eq!(tool, "sql_read");
        assert_eq!(status, "ok");
        assert_eq!(duration_ms, 7);
        assert_eq!(arguments_sha256, "abc");
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn retention_removes_only_old_rows() {
        let pool = migrated_pool().await;
        record(&pool, "old", None, "sql_read", "ok", 1, "aa")
            .await
            .unwrap();
        sqlx::query("UPDATE mcp_call SET at = now() - interval '40 days'")
            .execute(&pool)
            .await
            .unwrap();
        record(&pool, "new", None, "sql_read", "ok", 1, "bb")
            .await
            .unwrap();
        assert_eq!(purge(&pool, 30).await.unwrap(), 1);
        let left: Vec<String> = sqlx::query_scalar("SELECT sub FROM mcp_call")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(left, ["new"]);
    }
}
