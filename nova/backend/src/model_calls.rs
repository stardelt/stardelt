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

use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::{info, warn};

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
