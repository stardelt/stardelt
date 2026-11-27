//! What the platform used of each model endpoint this month (design nova#890, item 5).
//!
//! The administrator's read of `model_call`: tokens per group and endpoint since the first
//! of the month, and the last calls. Administrators only (`admin_router()` puts
//! `auth::require_admin` in front). No prompt and no answer text: the table stores sizes and
//! hashes, so this view cannot leak a conversation.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderValue, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use utoipa::ToSchema;

use crate::AppState;
use crate::saved_queries::{store_failed, unavailable};

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 500;

/// The month's tokens, per group and endpoint.
#[derive(Debug, Serialize, FromRow, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UsageRow {
    /// The group, or `(none)` for `nova`'s probe rows and callers without a group.
    pub group: String,
    pub endpoint: String,
    pub tokens_in: i64,
    pub tokens_out: i64,
    pub calls: i64,
    /// Calls whose token counts were estimated (`ceil(bytes / 4)`), so the page can say "about".
    pub estimated_calls: i64,
}

/// One recorded call, without `sub`-scoped text.
#[derive(Debug, Serialize, FromRow, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UsageCall {
    pub at_ms: i64,
    pub sub: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    pub feature: String,
    pub endpoint: String,
    pub model: String,
    pub tokens_in: i64,
    pub tokens_out: i64,
    pub tokens_estimated: bool,
    pub latency_ms: i64,
    pub outcome: String,
}

/// One endpoint's 👍 and 👎 this month, from `model_call.feedback`. An endpoint nobody rated
/// is absent from the answer, not a zero row.
#[derive(Debug, Serialize, FromRow, ToSchema)]
pub struct UsageFeedback {
    pub endpoint: String,
    pub up: i64,
    pub down: i64,
}

/// The answer of `GET /api/admin/model-usage`.
#[derive(Debug, Serialize, ToSchema)]
pub struct ModelUsage {
    /// `YYYY-MM`, the calendar month the totals cover (UTC).
    pub month: String,
    pub groups: Vec<UsageRow>,
    pub calls: Vec<UsageCall>,
    /// This month's votes per endpoint, name order; an endpoint nobody rated is absent.
    pub feedback: Vec<UsageFeedback>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UsageParams {
    limit: Option<i64>,
    group: Option<String>,
}

/// `limit` 1–500, 50 by default.
fn limit_from(limit: Option<i64>) -> i64 {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

/// `YYYY-MM` of the month the totals cover, against the database's clock so a stale process
/// clock cannot move the month.
async fn month(pool: &PgPool) -> Result<String, sqlx::Error> {
    sqlx::query_scalar("SELECT to_char(date_trunc('month', now()), 'YYYY-MM')")
        .fetch_one(pool)
        .await
}

/// This month's tokens, per group and endpoint, the largest first.
async fn totals(pool: &PgPool) -> Result<Vec<UsageRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT coalesce(group_name, '(none)') AS \"group\", endpoint, \
            sum(tokens_in)::bigint AS tokens_in, sum(tokens_out)::bigint AS tokens_out, \
            count(*)::bigint AS calls, \
            count(*) FILTER (WHERE tokens_estimated)::bigint AS estimated_calls \
         FROM model_call WHERE at >= date_trunc('month', now()) \
         GROUP BY 1, 2 ORDER BY sum(tokens_in) + sum(tokens_out) DESC, 1, 2",
    )
    .fetch_all(pool)
    .await
}

/// The last calls, newest first, optionally narrowed to one group.
async fn calls(
    pool: &PgPool,
    group: Option<&str>,
    limit: i64,
) -> Result<Vec<UsageCall>, sqlx::Error> {
    sqlx::query_as(
        "SELECT (extract(epoch FROM at) * 1000)::bigint AS at_ms, sub, \
            group_name AS \"group\", feature, endpoint, model, tokens_in, tokens_out, \
            tokens_estimated, latency_ms, outcome \
         FROM model_call WHERE ($1::text IS NULL OR group_name = $1) \
         ORDER BY at DESC LIMIT $2",
    )
    .bind(group)
    .bind(limit)
    .fetch_all(pool)
    .await
}

/// This month's votes per endpoint, name order; an endpoint nobody rated is absent.
async fn feedback(pool: &PgPool) -> Result<Vec<UsageFeedback>, sqlx::Error> {
    sqlx::query_as(
        "SELECT endpoint, \
            count(*) FILTER (WHERE feedback = 'up')::bigint AS up, \
            count(*) FILTER (WHERE feedback = 'down')::bigint AS \"down\" \
         FROM model_call WHERE at >= date_trunc('month', now()) \
         GROUP BY endpoint HAVING count(feedback) > 0 ORDER BY endpoint",
    )
    .fetch_all(pool)
    .await
}

#[utoipa::path(
    get,
    operation_id = "admin_model_usage",
    path = "/api/admin/model-usage",
    tag = "internal",
    description = "What the platform used of each model endpoint this month (UTC). `month` is the calendar month as `YYYY-MM`; `groups` holds the month's tokens per group and endpoint, the largest total first — `(none)` is `nova`'s probe rows and callers without a group; `estimatedCalls` counts the calls whose tokens were estimated as `ceil(bytes / 4)`, so the page can say \"about\". `calls` holds the last calls, newest first; `limit` is 1 to 500 (50 by default) and `group` narrows `calls` only (the totals are always the whole month). `feedback` carries, per endpoint, how many of this month's calls the people rated 👍 (`up`) and 👎 (`down`); an endpoint nobody rated is absent. Never who voted. No prompt and no answer text: the table stores sizes and SHA-256 hashes. Administrator-only; a non-administrator answers 403 `this view is for platform administrators`. Without a state store the answer is 503. Answers `Cache-Control: no-store`.",
    params(
        ("limit" = Option<i64>, Query, description = "How many calls, 1 to 500 (default 50)"),
        ("group" = Option<String>, Query, description = "Only this group in `calls`; the totals stay the whole month"),
    ),
    responses(
        (status = 200, description = "The month's totals and the last calls", body = ModelUsage),
        (status = 401, description = "No session", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller is not a platform administrator", body = crate::openapi::ErrorResponse),
        (status = 503, description = "Nova has no state store", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn list(
    State(state): State<Arc<AppState>>,
    Query(params): Query<UsageParams>,
) -> Response {
    let Some(pool) = state.db.as_ref() else {
        return unavailable();
    };
    let limit = limit_from(params.limit);
    let usage = async {
        Ok::<ModelUsage, sqlx::Error>(ModelUsage {
            month: month(pool).await?,
            groups: totals(pool).await?,
            calls: calls(pool, params.group.as_deref(), limit).await?,
            feedback: feedback(pool).await?,
        })
    }
    .await;
    match usage {
        Ok(usage) => (
            [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
            Json(usage),
        )
            .into_response(),
        Err(e) => store_failed(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_support::migrated_pool;
    use crate::model_calls::{self, Call, Tokens};

    fn probe(tokens_in: i64) -> Call<'static> {
        Call {
            sub: "nova",
            group: None,
            feature: "probe",
            endpoint: "default",
            model: "qwen3-4b",
            tokens: Tokens {
                input: tokens_in,
                output: 5,
                estimated: false,
            },
            latency_ms: 41,
            outcome: "ok",
            prompt: "ping",
            answer: Some("pong"),
        }
    }

    #[test]
    fn the_limit_is_clamped() {
        assert_eq!(limit_from(Some(1)), 1);
        assert_eq!(limit_from(Some(900)), 500);
        assert_eq!(limit_from(None), 50);
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn month_totals_and_calls() {
        let pool = migrated_pool().await;
        // Two probe rows (no group) with the larger total, so `(none)` sorts first, and one
        // `ask` row for the `analytics` group.
        model_calls::record(&pool, &probe(2_000)).await.unwrap();
        model_calls::record(&pool, &probe(2_000)).await.unwrap();
        model_calls::record(
            &pool,
            &Call {
                sub: "alice",
                group: Some("analytics"),
                feature: "ask",
                endpoint: "default",
                model: "qwen3-4b",
                tokens: Tokens {
                    input: 1_000,
                    output: 10,
                    estimated: false,
                },
                latency_ms: 300,
                outcome: "ok",
                prompt: "why",
                answer: Some("because"),
            },
        )
        .await
        .unwrap();

        let month = month(&pool).await.unwrap();
        assert_eq!(month.len(), 7);
        assert_eq!(&month[4..5], "-");

        let groups = totals(&pool).await.unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].group, "(none)");
        assert_eq!(groups[0].calls, 2);
        assert_eq!(groups[1].group, "analytics");
        assert_eq!(groups[1].tokens_in, 1_000);

        let all = calls(&pool, None, 50).await.unwrap();
        assert_eq!(all.len(), 3);
        assert!(all[0].at_ms >= all[1].at_ms && all[1].at_ms >= all[2].at_ms);
        assert_eq!(all[0].sub, "alice");
        let narrowed = calls(&pool, Some("analytics"), 50).await.unwrap();
        assert_eq!(narrowed.len(), 1);
        assert_eq!(narrowed[0].sub, "alice");
        assert_eq!(narrowed[0].tokens_in, 1_000);

        // One 👍 on the ask row: the endpoint's month carries one vote, no vote.
        sqlx::query("UPDATE model_call SET feedback = 'up' WHERE sub = 'alice'")
            .execute(&pool)
            .await
            .unwrap();
        let votes = feedback(&pool).await.unwrap();
        assert_eq!(votes.len(), 1, "no vote is not a zero row");
        assert_eq!(votes[0].endpoint, "default");
        assert_eq!((votes[0].up, votes[0].down), (1, 0));
    }
}
