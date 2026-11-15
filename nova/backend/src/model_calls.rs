//! One row per call Nova makes to a model endpoint (design nova#890, slice 1).
//!
//! The row is the budget's and the administrator's evidence: who called (a person's `sub`, or
//! `nova` for the probe), which group, which feature (`ask`, `agent`, `probe`), which endpoint
//! and model, how many tokens, how long, and how it ended. **No text**: the prompt and the
//! answer are stored as a byte count and a SHA-256 each, so a person leaving needs no deletion
//! and a leaked dump carries no conversation. Full text is a later opt-in on the endpoint.
//!
//! Writes are best effort: a failed insert never fails the model call (the same rule the MCP
//! audit follows). Rows are pruned hourly, 30 days by default.

use std::time::Duration;

use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::{info, warn};

use crate::query_hints::commas;

/// How long rows are kept when `NOVA_MODEL_CALL_RETENTION_DAYS` is not set.
const DEFAULT_RETENTION_DAYS: i64 = 30;

/// How often old rows are removed.
const PURGE_EVERY: Duration = Duration::from_secs(3600);

/// One call to record. `feature` and `outcome` are the strings the table's CHECKs allow.
pub(crate) struct Call<'a> {
    /// The person (`sub`), or `nova` for the probe.
    pub sub: &'a str,
    /// The person's group, when there is one.
    pub group: Option<&'a str>,
    /// `ask`, `agent` or `probe`.
    pub feature: &'a str,
    /// The `ModelEndpoint` object's name — never its URL.
    pub endpoint: &'a str,
    /// The model id the endpoint serves.
    pub model: &'a str,
    pub tokens: Tokens,
    pub latency_ms: u64,
    /// `ok`, `refused` or `error`.
    pub outcome: &'a str,
    pub prompt: &'a str,
    pub answer: Option<&'a str>,
}

/// A call's token counts, and whether any of them had to be estimated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tokens {
    pub input: i64,
    pub output: i64,
    pub estimated: bool,
}

/// Token counts from the endpoint's `usage` (`prompt_tokens`, `completion_tokens`) when it
/// answers with one, else `ceil(bytes / 4)` — the design's rule, marked as an estimate so the
/// usage page can say "about".
pub(crate) fn tokens(usage: Option<&Value>, prompt: &str, answer: Option<&str>) -> Tokens {
    let from_usage = |name: &str| usage.and_then(|u| u.get(name)).and_then(Value::as_i64);
    let input = from_usage("prompt_tokens");
    let output = from_usage("completion_tokens");
    let estimate = |text: Option<&str>| text.map_or(0, |t| (t.len() as i64 + 3) / 4);
    Tokens {
        input: input.unwrap_or_else(|| estimate(Some(prompt))),
        output: output.unwrap_or_else(|| estimate(answer)),
        estimated: input.is_none() || output.is_none(),
    }
}

/// The lowercase hex SHA-256 of a text's bytes, the same encoding `mcp_call` uses.
fn sha256(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Insert one row. The caller decides whether an error is worth a log line.
pub(crate) async fn record(pool: &PgPool, call: &Call<'_>) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO model_call (sub, group_name, feature, endpoint, model, tokens_in, \
            tokens_out, tokens_estimated, latency_ms, outcome, prompt_bytes, prompt_sha256, \
            answer_bytes, answer_sha256) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
    )
    .bind(call.sub)
    .bind(call.group)
    .bind(call.feature)
    .bind(call.endpoint)
    .bind(call.model)
    .bind(call.tokens.input)
    .bind(call.tokens.output)
    .bind(call.tokens.estimated)
    .bind(i64::try_from(call.latency_ms).unwrap_or(i64::MAX))
    .bind(call.outcome)
    .bind(call.prompt.len() as i64)
    .bind(sha256(call.prompt))
    .bind(call.answer.map(|answer| answer.len() as i64))
    .bind(call.answer.map(sha256))
    .execute(pool)
    .await
    .map(|_| ())
}

/// Remove old rows once an hour for as long as Nova runs.
pub(crate) fn spawn_retention(pool: PgPool) {
    let days = retention_days_from(|name| std::env::var(name).ok());
    let Some(days) = days else {
        info!("model call retention is off (NOVA_MODEL_CALL_RETENTION_DAYS=0)");
        return;
    };
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(PURGE_EVERY);
        loop {
            tick.tick().await;
            match purge(&pool, days).await {
                Ok(0) => {}
                Ok(rows) => info!(rows, days, "removed old model call rows"),
                Err(e) => warn!(error = %e, "model call purge failed"),
            }
        }
    });
}

/// Delete the rows older than `days`.
pub(crate) async fn purge(pool: &PgPool, days: i64) -> sqlx::Result<u64> {
    let result =
        sqlx::query("DELETE FROM model_call WHERE at < now() - make_interval(days => $1::int)")
            .bind(days as i32)
            .execute(pool)
            .await?;
    Ok(result.rows_affected())
}

/// Retention from the environment: unset or unparsable gives the default, `0` switches the
/// purge off (`None`), anything else is the number of days.
fn retention_days_from(get: impl Fn(&str) -> Option<String>) -> Option<i64> {
    match get("NOVA_MODEL_CALL_RETENTION_DAYS").map(|v| v.trim().to_string()) {
        None => Some(DEFAULT_RETENTION_DAYS),
        Some(v) if v.is_empty() => Some(DEFAULT_RETENTION_DAYS),
        Some(v) => match v.parse::<i64>() {
            Ok(0) => None,
            Ok(days) if days > 0 => Some(days),
            _ => {
                warn!(value = %v, "the NOVA_MODEL_CALL_RETENTION_DAYS value is not a positive number; using the default");
                Some(DEFAULT_RETENTION_DAYS)
            }
        },
    }
}

/// `spec.quota` of one endpoint; either field absent means no limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Quota {
    pub per_group_monthly: Option<i64>,
    pub per_person_daily: Option<i64>,
}

/// Read `spec.quota` from a `ModelEndpoint` object as the API server returns it.
// called by the Ask endpoint (nova#891) and the agent proxy (slice 3); remove the allow there
#[allow(dead_code)]
pub(crate) fn quota_of(object: &Value) -> Quota {
    let quota = object.pointer("/spec/quota").unwrap_or(&Value::Null);
    Quota {
        per_group_monthly: quota.get("perGroupMonthlyTokens").and_then(Value::as_i64),
        per_person_daily: quota.get("perPersonDailyTokens").and_then(Value::as_i64),
    }
}

/// A spent budget: what was used, against which limit, over which window.
pub(crate) struct Refusal {
    pub used: i64,
    pub limit: i64,
    pub window: &'static str,
    pub subject: &'static str,
}

impl Refusal {
    /// The sentence the UI shows, e.g. "Your group used 2,015,000 of its 2,000,000 tokens
    /// this month. Ask an administrator to raise the quota."
    pub(crate) fn sentence(&self) -> String {
        let used = commas(self.used);
        let limit = commas(self.limit);
        let spent = match (self.subject, self.window) {
            ("group", "month") => {
                format!("Your group used {used} of its {limit} tokens this month")
            }
            ("person", "day") => format!("You used {used} of your {limit} tokens today"),
            (_, window) => format!("{used} of {limit} tokens were used this {window}"),
        };
        format!("{spent}. Ask an administrator to raise the quota.")
    }
}

/// Tokens the group spent this calendar month.
async fn group_tokens(pool: &PgPool, group: &str) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        "SELECT CAST(COALESCE(SUM(tokens_in + tokens_out), 0) AS BIGINT) FROM model_call \
         WHERE group_name = $1 AND at >= date_trunc('month', now())",
    )
    .bind(group)
    .fetch_one(pool)
    .await
}

/// Tokens the person spent today.
async fn person_tokens(pool: &PgPool, sub: &str) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        "SELECT CAST(COALESCE(SUM(tokens_in + tokens_out), 0) AS BIGINT) FROM model_call \
         WHERE sub = $1 AND at >= date_trunc('day', now())",
    )
    .bind(sub)
    .fetch_one(pool)
    .await
}

/// Check both limits before a call. `group` is `None` for a person without a group (the
/// per-group limit is skipped then). The first spent budget wins.
///
/// A database error is **not** a refusal: it is logged and the call goes through, the same
/// rule [`record`] follows — a broken audit table must not stop the product.
// called by the Ask endpoint (nova#891) and the agent proxy (slice 3); remove the allow there
#[allow(dead_code)]
pub(crate) async fn check(
    pool: &PgPool,
    quota: &Quota,
    group: Option<&str>,
    sub: &str,
) -> Result<(), Refusal> {
    if let (Some(limit), Some(group)) = (quota.per_group_monthly, group) {
        match group_tokens(pool, group).await {
            Ok(used) if used >= limit => {
                return Err(Refusal {
                    used,
                    limit,
                    window: "month",
                    subject: "group",
                });
            }
            Ok(_) => {}
            Err(e) => {
                warn!(error = %e, "the group model budget could not be read — the call goes through")
            }
        }
    }
    if let Some(limit) = quota.per_person_daily {
        match person_tokens(pool, sub).await {
            Ok(used) if used >= limit => {
                return Err(Refusal {
                    used,
                    limit,
                    window: "day",
                    subject: "person",
                });
            }
            Ok(_) => {}
            Err(e) => {
                warn!(error = %e, "the person's model budget could not be read — the call goes through")
            }
        }
    }
    Ok(())
}

/// The 429 answer: the sentence in `message`, plus the numbers, so the UI needs no second call.
// called by the Ask endpoint (nova#891) and the agent proxy (slice 3); remove the allow there
#[allow(dead_code)]
pub(crate) fn refuse(refusal: &Refusal) -> Response {
    let body = json!({
        "error": "model_budget_exceeded",
        "message": refusal.sentence(),
        "limit": refusal.limit,
        "used": refusal.used,
        "window": refusal.window,
    });
    let mut response = (StatusCode::TOO_MANY_REQUESTS, Json(body)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_support::migrated_pool;
    use serde_json::json;

    #[test]
    fn a_usage_field_is_taken_as_it_is() {
        let usage = json!({"prompt_tokens": 12, "completion_tokens": 4});
        assert_eq!(
            tokens(Some(&usage), "0123456789ab", Some("four")),
            Tokens {
                input: 12,
                output: 4,
                estimated: false
            }
        );
    }

    #[test]
    fn a_missing_usage_is_estimated_from_the_bytes() {
        let usage = json!({"prompt_tokens": 12});
        let estimated = tokens(Some(&usage), "0123456789ab", Some("four"));
        assert_eq!(estimated.input, 12);
        assert_eq!(estimated.output, 1);
        assert!(estimated.estimated);
        let none = tokens(None, "0123456789ab", None);
        assert_eq!((none.input, none.output), (3, 0));
        assert!(none.estimated);
    }

    #[test]
    fn the_hash_is_lowercase_hex() {
        let hash = sha256("hello");
        assert_eq!(hash.len(), 64);
        assert!(
            hash.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_ne!(hash, sha256("hello!"));
    }

    #[test]
    fn zero_switches_the_retention_off() {
        assert_eq!(retention_days_from(|_| None), Some(30));
        assert_eq!(retention_days_from(|_| Some("0".into())), None);
        assert_eq!(retention_days_from(|_| Some("7".into())), Some(7));
        assert_eq!(retention_days_from(|_| Some("x".into())), Some(30));
    }

    #[test]
    fn a_quota_is_read_from_the_crd_or_is_no_limit() {
        assert_eq!(quota_of(&json!({"spec": {"model": "m"}})), Quota::default());
        assert_eq!(
            quota_of(&json!({"spec": {"quota": {"perGroupMonthlyTokens": 2_000_000}}})),
            Quota {
                per_group_monthly: Some(2_000_000),
                per_person_daily: None,
            }
        );
        assert_eq!(
            quota_of(&json!({"spec": {"quota": {"perPersonDailyTokens": 20_000}}})),
            Quota {
                per_group_monthly: None,
                per_person_daily: Some(20_000),
            }
        );
        assert_eq!(
            quota_of(
                &json!({"spec": {"quota": {"perGroupMonthlyTokens": 2_000_000,
                "perPersonDailyTokens": 20_000}}})
            ),
            Quota {
                per_group_monthly: Some(2_000_000),
                per_person_daily: Some(20_000),
            }
        );
    }

    #[test]
    fn a_refusal_says_what_was_spent_and_asks_for_an_administrator() {
        let month = Refusal {
            used: 2_015_000,
            limit: 2_000_000,
            window: "month",
            subject: "group",
        };
        assert_eq!(
            month.sentence(),
            "Your group used 2,015,000 of its 2,000,000 tokens this month. \
             Ask an administrator to raise the quota."
        );
        let day = Refusal {
            used: 20_000,
            limit: 20_000,
            window: "day",
            subject: "person",
        };
        assert_eq!(
            day.sentence(),
            "You used 20,000 of your 20,000 tokens today. \
             Ask an administrator to raise the quota."
        );
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn a_spent_group_budget_refuses_the_next_call() {
        let pool = migrated_pool().await;
        let call = |sub: &'static str, group: Option<&'static str>, tokens: i64| Call {
            sub,
            group,
            feature: "ask",
            endpoint: "default",
            model: "m",
            tokens: Tokens {
                input: tokens,
                output: 0,
                estimated: false,
            },
            latency_ms: 1,
            outcome: "ok",
            prompt: "",
            answer: None,
        };
        // This month, this group: 10 tokens. Another group: 100. 40 days old: 50.
        record(&pool, &call("old", Some("analytics"), 50))
            .await
            .unwrap();
        sqlx::query("UPDATE model_call SET at = now() - interval '40 days' WHERE sub = 'old'")
            .execute(&pool)
            .await
            .unwrap();
        record(&pool, &call("alice", Some("analytics"), 10))
            .await
            .unwrap();
        record(&pool, &call("bob", Some("finance"), 100))
            .await
            .unwrap();

        let spent = Quota {
            per_group_monthly: Some(10),
            per_person_daily: None,
        };
        let refusal = check(&pool, &spent, Some("analytics"), "alice")
            .await
            .expect_err("the group budget is spent");
        assert_eq!((refusal.used, refusal.limit), (10, 10));
        assert_eq!((refusal.window, refusal.subject), ("month", "group"));
        // The 40-day-old row and the other group are not counted.
        assert!(refusal.used < 60);

        assert!(
            check(&pool, &Quota::default(), Some("analytics"), "alice")
                .await
                .is_ok(),
            "no quota is no limit"
        );
        assert!(
            check(
                &pool,
                &Quota {
                    per_group_monthly: Some(11),
                    per_person_daily: None,
                },
                Some("analytics"),
                "alice"
            )
            .await
            .is_ok(),
            "a limit above the spend lets the call through"
        );
        assert!(
            check(
                &pool,
                &Quota {
                    per_group_monthly: Some(1),
                    per_person_daily: None,
                },
                None,
                "alice"
            )
            .await
            .is_ok(),
            "a person without a group skips the group limit"
        );
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn a_spent_daily_budget_refuses_the_next_call() {
        let pool = migrated_pool().await;
        let call = |sub: &'static str, tokens: i64| Call {
            sub,
            group: None,
            feature: "ask",
            endpoint: "default",
            model: "m",
            tokens: Tokens {
                input: 0,
                output: tokens,
                estimated: false,
            },
            latency_ms: 1,
            outcome: "ok",
            prompt: "",
            answer: None,
        };
        // Today, this person: 5 tokens. Another person: 100. Two days ago: 50.
        record(&pool, &call("alice", 50)).await.unwrap();
        sqlx::query("UPDATE model_call SET at = now() - interval '2 days' WHERE sub = 'alice'")
            .execute(&pool)
            .await
            .unwrap();
        record(&pool, &call("alice", 5)).await.unwrap();
        record(&pool, &call("bob", 100)).await.unwrap();

        let spent = Quota {
            per_group_monthly: None,
            per_person_daily: Some(5),
        };
        let refusal = check(&pool, &spent, None, "alice")
            .await
            .expect_err("the daily budget is spent");
        assert_eq!((refusal.used, refusal.limit), (5, 5));
        assert_eq!((refusal.window, refusal.subject), ("day", "person"));
        assert!(
            check(&pool, &Quota::default(), None, "alice").await.is_ok(),
            "no quota is no limit"
        );
        assert!(
            check(
                &pool,
                &Quota {
                    per_group_monthly: None,
                    per_person_daily: Some(101),
                },
                None,
                "bob"
            )
            .await
            .is_ok(),
            "another person's day is their own: bob spent 100, not alice's 5"
        );
        assert!(
            check(&pool, &spent, None, "bob").await.is_err(),
            "bob's own 100 tokens are over the 5-token limit"
        );
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn a_recorded_call_is_readable() {
        let pool = migrated_pool().await;
        record(
            &pool,
            &Call {
                sub: "alice",
                group: Some("analytics"),
                feature: "probe",
                endpoint: "default",
                model: "qwen3-4b",
                tokens: Tokens {
                    input: 7,
                    output: 3,
                    estimated: false,
                },
                latency_ms: 41,
                outcome: "ok",
                prompt: "hi",
                answer: Some("hello"),
            },
        )
        .await
        .unwrap();
        let (sub, group_name, tokens_in, tokens_out, prompt_sha256, answer_bytes): (
            String,
            Option<String>,
            i64,
            i64,
            String,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT sub, group_name, tokens_in, tokens_out, prompt_sha256, answer_bytes \
             FROM model_call",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(sub, "alice");
        assert_eq!(group_name.as_deref(), Some("analytics"));
        assert_eq!((tokens_in, tokens_out), (7, 3));
        assert_eq!(prompt_sha256, sha256("hi"));
        assert_eq!(answer_bytes, Some(5));
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn retention_removes_only_old_rows() {
        let pool = migrated_pool().await;
        let call = |sub: &'static str| Call {
            sub,
            group: None,
            feature: "probe",
            endpoint: "default",
            model: "m",
            tokens: Tokens {
                input: 1,
                output: 0,
                estimated: true,
            },
            latency_ms: 1,
            outcome: "error",
            prompt: "",
            answer: None,
        };
        record(&pool, &call("old")).await.unwrap();
        sqlx::query("UPDATE model_call SET at = now() - interval '40 days'")
            .execute(&pool)
            .await
            .unwrap();
        record(&pool, &call("new")).await.unwrap();
        assert_eq!(purge(&pool, 30).await.unwrap(), 1);
        let left: Vec<String> = sqlx::query_scalar("SELECT sub FROM model_call")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(left, ["new"]);
    }
}
