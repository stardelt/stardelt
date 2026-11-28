//! `POST /api/ask` — a question in, checked SQL out (design nova#891 slice a).
//!
//! Ask is the product's AI door: a person asks in words, a model endpoint that an
//! administrator connected with purpose `ask` writes one statement, and Nova checks that
//! statement with `EXPLAIN` **as the asking person** before it answers. The statement is
//! never run here — the UI runs it, if the person asks for it.
//!
//! Two rules the module keeps:
//!
//! - **Metadata only.** The model receives the question and the documentation of at most
//!   five candidate tables (description, tags, owner, domain, columns, and the table's
//!   metric definitions: name, description, expression, time column, dimensions). Never a
//!   row, never a result, whatever `spec.sendRows` says.
//! - **The endpoint's URL and key never leave the call.** They are read from the
//!   `ModelEndpoint` object and the projected key file, used in the one `chat()` call, and
//!   are in no answer, header, log line or `model_call` row.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;

use crate::AppState;
use crate::api_json::ApiJson;

/// The longest question Ask takes.
const MAX_QUESTION_CHARS: usize = 2_000;
/// How many tables the model may choose from.
const CANDIDATES: usize = 5;
/// How many catalogs a question without `?catalog=` searches, in `list_catalogs` order.
const MAX_CATALOGS: usize = 10;
/// How long the `EXPLAIN` check of one statement may take.
const ASK_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// The sentence a person reads when nothing matches their question.
const NO_TABLE: &str =
    "I found no table you can see that matches. Try the table's name, or browse the Catalog.";
/// The one sentence that says why Ask cannot answer: no endpoint, or no usable endpoint.
const NO_ENDPOINT: &str =
    "no ask model is ready: connect a ModelEndpoint with purpose ask, and wait for its probe";
const SYSTEM_PROMPT: &str = "You write one Trino SQL statement for a question about the tables you are given. Use only these tables, fully qualified as catalog.namespace.table, and only the columns their documentation lists; never invent a table or a column. You receive metadata only, never rows. When an entry carries a verifiedQueries list and one of its questions matches the question, copy that query's shape — its tables, joins and filters — instead of writing a new one from scratch. When an entry carries a guide, follow it: it says how the table's values join, which rows to leave out and what its numbers mean. Answer with one JSON object with exactly two fields: \"sql\" — one SELECT or WITH statement, no trailing semicolon — and \"sentence\" — one sentence of at most 200 characters saying what the statement answers. Never write DDL or DML, and never write EXPLAIN.";

/// The agent's system prompt: the model works with the read-only tools and answers in words.
const AGENT_SYSTEM_PROMPT: &str = "You answer questions about the data the person may read, using the tools. Use the catalog and documentation tools to find tables, sql_read to look at data (read-only), and answer in words, naming the tables and numbers you used. Never invent a table, a column or a number.";

/// How many of a table's verified queries the model receives (newest first).
const VERIFIED_QUERIES_IN_CONTEXT: usize = 5;
/// Longest verified SQL kept in the context; a longer one is cut with `…`.
const MAX_VERIFIED_SQL_CHARS: usize = 4000;
/// The longest previous statement a follow-up may send back.
const MAX_PREVIOUS_SQL_CHARS: usize = 8_000;
/// How many earlier turns of the conversation one question may carry.
const MAX_THREAD_TURNS: usize = 10;
/// The longest answer in words one earlier turn may carry.
const MAX_TURN_ANSWER_CHARS: usize = 8_000;
/// How many endpoints one auto-mode question may ask: the ranked one, and one fallback (D3).
const FALLBACK_ENDPOINTS: usize = 2;

/// `POST /api/ask` body.
#[derive(Deserialize, utoipa::ToSchema)]
pub(crate) struct AskRequest {
    /// The question, in words, 1 to 2000 characters.
    pub question: String,
    /// The turn this question refines (the panel's last answered turn), or absent.
    #[serde(default)]
    pub previous: Option<AskPrevious>,
    /// The conversation so far since **New question**, oldest first, without the question
    /// being asked now, at most 10 turns. In agent mode the model reads it as earlier
    /// messages, and `previous` is then not used.
    #[serde(default)]
    pub thread: Vec<AskTurn>,
    /// The `ModelEndpoint` that answers. Absent or `auto` uses auto mode: the ready
    /// endpoints ranked by this catalog's eval results, falling back once. A name asks
    /// that endpoint only, and is a `400` when it cannot answer Ask now.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// `agent` or `sql`. Absent means `agent` when the endpoint that answers can use tools
    /// and Nova's MCP endpoint is on, and today's one-statement answer otherwise (D1).
    #[serde(default)]
    pub mode: Option<String>,
}

/// The answered turn a follow-up refines, as the panel sends it: the earlier question, the
/// statement it got and the tables it was written for.
#[derive(Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AskPrevious {
    /// The earlier question, 1 to 2000 characters.
    pub question: String,
    /// The earlier statement, 1 to 8000 characters.
    pub sql: String,
    /// The tables the earlier answer named, at most five.
    pub tables: Vec<AskTable>,
}

/// One earlier turn of the conversation: the question and the answer in words the person saw.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AskTurn {
    /// The earlier question, 1 to 2000 characters.
    pub question: String,
    /// The earlier answer in words, 1 to 8000 characters.
    pub answer: String,
}

/// `POST /api/ask` query: the catalog to search, or the default one.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct AskQuery {
    pub catalog: Option<String>,
}

/// One table the model was given.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AskTable {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

/// What Ask answers: the statement, the sentence, the tables it chose from and the check.
#[derive(Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AskAnswer {
    /// The `ModelEndpoint` that answered — never its URL.
    pub endpoint: String,
    pub model: String,
    /// The endpoints asked, in order: one, or two when auto mode fell back.
    pub tried: Vec<String>,
    /// The `model_call` row the answer's last call wrote; `null` when the store failed.
    pub call_id: Option<i64>,
    /// The statement; `null` when no table matched, kept when the check failed.
    pub sql: Option<String>,
    /// One sentence saying what the statement answers, or why there is none.
    pub sentence: String,
    pub tables: Vec<AskTable>,
    /// `passed`, `failed` or `not_run`
    pub explain: String,
    /// Why the `EXPLAIN` check failed, when it did.
    pub error: Option<String>,
    /// `agent` when the model used the read-only tools, `sql` for one checked statement.
    pub mode: String,
    /// The agent's answer in words; `null` for a `sql` answer.
    pub text: Option<String>,
    /// The tool calls an agent answer made, in order; empty for a `sql` answer.
    pub steps: Vec<crate::ask_agent::AskStep>,
}

/// One conversation from a person's History.
#[derive(Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AskConversation {
    pub id: i64,
    pub question: String,
    pub endpoint: String,
    pub model: Option<String>,
    pub call_id: Option<i64>,
    pub sql: Option<String>,
    pub sentence: String,
    pub tables: Value,
    pub explain: String,
    pub error: Option<String>,
    pub created_at: String,
    /// Kept: a pinned conversation is never pruned.
    pub pinned: bool,
}

/// Every endpoint that may answer Ask, in name order: purpose `ask`, the probe found it
/// reachable, and it answered in a JSON shape. Empty means the feature is off.
fn ready_ask_endpoints(objects: &[Value]) -> Vec<(String, Value)> {
    let mut ready: Vec<(String, Value)> = objects
        .iter()
        .map(|object| (crate::model_endpoints::row(object), object))
        .filter(|(row, _)| {
            row.purposes.iter().any(|purpose| purpose == "ask")
                && row.reachable == Some(true)
                && row.json_output == Some(true)
        })
        .map(|(row, object)| (row.name, object.clone()))
        .collect();
    ready.sort_by(|a, b| a.0.cmp(&b.0));
    ready
}

/// How many of one endpoint's eval runs on this catalog passed in the last 30 days:
/// `endpoint → (passed, total)`.
async fn eval_shares(
    pool: &sqlx::PgPool,
    catalog: &str,
) -> Result<std::collections::HashMap<String, (i64, i64)>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT r.endpoint, count(*) FILTER (WHERE r.passed), count(*) \
         FROM eval_run r JOIN eval e ON e.id = r.eval_id \
         WHERE e.catalog = $1 AND r.endpoint IS NOT NULL AND r.created_at > now() - interval '30 days' \
         GROUP BY r.endpoint",
    )
    .bind(catalog)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.get::<String, _>(0),
                (row.get::<i64, _>(1), row.get::<i64, _>(2)),
            )
        })
        .collect())
}

/// An endpoint's pass share on the catalog, or `None` when it has fewer than three runs:
/// three runs are the least that says anything.
fn pass_share(shares: &std::collections::HashMap<String, (i64, i64)>, name: &str) -> Option<f64> {
    let (passed, total) = *shares.get(name)?;
    (total >= 3).then(|| passed as f64 / total as f64)
}

/// Auto mode's order: the endpoints with at least three eval runs by their pass share,
/// highest first; the others after them, in name order; ties by name.
fn rank(
    mut ready: Vec<(String, Value)>,
    shares: &std::collections::HashMap<String, (i64, i64)>,
) -> Vec<(String, Value)> {
    ready.sort_by(|a, b| {
        match (pass_share(shares, &a.0), pass_share(shares, &b.0)) {
            (Some(one), Some(other)) => other.total_cmp(&one),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
        .then_with(|| a.0.cmp(&b.0))
    });
    ready
}

/// The endpoint a request names, when it names one: absent, blank or `auto` is auto mode.
fn requested_endpoint(requested: Option<&str>) -> Option<&str> {
    match requested.map(str::trim) {
        Some(name) if !name.is_empty() && name != "auto" => Some(name),
        _ => None,
    }
}

/// The endpoints one question may ask, in the order it asks them: the named one, or the
/// auto ranking of D2. A named endpoint that cannot answer Ask is a `400`.
// The `Err` is the ready-made refusal this returns, as in `catalog_search::filter_value`.
#[allow(clippy::result_large_err)]
fn choose_endpoints(
    ready: Vec<(String, Value)>,
    requested: Option<&str>,
    shares: &std::collections::HashMap<String, (i64, i64)>,
) -> Result<Vec<(String, Value)>, Response> {
    match requested_endpoint(requested) {
        Some(name) => match ready.into_iter().find(|(ready, _)| ready == name) {
            Some(found) => Ok(vec![found]),
            None => Err(fail(
                StatusCode::BAD_REQUEST,
                format!("endpoint `{name}` cannot answer Ask now"),
            )),
        },
        None => Ok(rank(ready, shares)),
    }
}

/// Whether a question is short enough for the every-word catalog search. A longer one goes
/// straight to `search_any_word`: the search refuses `q` over its 100 characters.
fn every_word_search(question: &str) -> bool {
    question.chars().count() <= crate::catalog_search::MAX_QUERY_CHARS
}

/// The context the model receives: one entry per candidate table, metadata only. A field
/// the documentation does not have is `null`.
fn context_of(
    candidates: &[AskTable],
    docs: &[Value],
    verified: &[Value],
    metrics: &[Vec<crate::metric_definitions::MetricDefinition>],
) -> Value {
    let field = |doc: &Value, name: &str| doc.get(name).cloned().unwrap_or(Value::Null);
    let entries: Vec<Value> = candidates
        .iter()
        .enumerate()
        .map(|(index, table)| {
            let doc = docs.get(index).unwrap_or(&Value::Null);
            let empty = Vec::new();
            let table_metrics =
                serde_json::to_value(metrics.get(index).unwrap_or(&empty)).unwrap_or(Value::Null);
            json!({
                "name": format!("{}.{}.{}", table.catalog, table.namespace, table.table),
                "description": field(doc, "description"),
                "tags": field(doc, "tags"),
                "owner": field(doc, "owner"),
                "domain": field(doc, "domain"),
                "guide": field(doc, "guide"),
                "columns": field(doc, "columns"),
                "metrics": table_metrics,
                "verifiedQueries": verified
                    .get(index)
                    .map(verified_of)
                    .unwrap_or_else(|| Value::Array(Vec::new())),
            })
        })
        .collect();
    Value::Array(entries)
}

/// What the model receives of one table's verified queries: question and SQL, newest first,
/// at most [`VERIFIED_QUERIES_IN_CONTEXT`]. No expected rows and no author.
fn verified_of(list: &Value) -> Value {
    let rows = list.as_array().map(Vec::as_slice).unwrap_or(&[]);
    let entries: Vec<Value> = rows
        .iter()
        .take(VERIFIED_QUERIES_IN_CONTEXT)
        .filter_map(|row| {
            let sql = row.get("sql")?.as_str()?;
            let sql = if sql.chars().count() > MAX_VERIFIED_SQL_CHARS {
                sql.chars()
                    .take(MAX_VERIFIED_SQL_CHARS - 1)
                    .collect::<String>()
                    + "…"
            } else {
                sql.to_string()
            };
            Some(json!({
                "question": row.get("question").cloned().unwrap_or(Value::Null),
                "sql": sql,
            }))
        })
        .collect();
    Value::Array(entries)
}

/// A `Response` that must not be cached: every answer of this module carries `no-store`.
fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

/// One refusal in the API's envelope.
fn fail(status: StatusCode, message: impl Into<String>) -> Response {
    no_store(crate::error::Fail::new(status, message).into_response())
}

/// The `200` answer.
fn answer(body: AskAnswer) -> Response {
    no_store((StatusCode::OK, Json(body)).into_response())
}

/// Keep one conversation for the person's History, and keep at most 20 of them.
///
/// A failed write is a warning, never a failed answer: the person asked a question and the
/// answer is what matters. `tables` is stored as the answer's own JSON.
async fn record_conversation(pool: &sqlx::PgPool, sub: &str, question: &str, body: &AskAnswer) {
    let tables = serde_json::to_value(&body.tables).unwrap_or_else(|_| json!([]));
    let inserted = sqlx::query(
        "INSERT INTO ask_conversation \
         (sub, question, endpoint, model, call_id, sql, sentence, tables, explain, error) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(sub)
    .bind(question)
    .bind(&body.endpoint)
    .bind(&body.model)
    .bind(body.call_id)
    .bind(&body.sql)
    .bind(&body.sentence)
    .bind(tables)
    .bind(&body.explain)
    .bind(&body.error)
    .execute(pool)
    .await;
    if let Err(e) = inserted {
        tracing::warn!(error = %e, "ask: the conversation was not stored");
        return;
    }
    let pruned = sqlx::query(
        "DELETE FROM ask_conversation WHERE sub = $1 AND NOT pinned AND id NOT IN ( \
           SELECT id FROM ask_conversation WHERE sub = $1 AND NOT pinned \
           ORDER BY created_at DESC, id DESC LIMIT 20)",
    )
    .bind(sub)
    .execute(pool)
    .await;
    if let Err(e) = pruned {
        tracing::warn!(error = %e, "ask: old conversations were not pruned");
    }
}

/// A handler's `Response` as its status and JSON body (an unreadable body is `null`).
async fn body_of(response: Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap_or_default();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// The model's content as `(sql, sentence)`, or `None` when it is not that JSON.
pub(crate) fn parse_answer(content: &str) -> Option<(String, String)> {
    let value: Value = serde_json::from_str(content.trim()).ok()?;
    let sql = value.get("sql")?.as_str()?.trim().to_string();
    let sentence = value.get("sentence")?.as_str()?.trim().to_string();
    (!sql.is_empty()).then_some((sql, sentence))
}

/// True when the endpoint sent no content and said it ran out of tokens: a reasoning model
/// spent the whole answer budget thinking, so no retry in the same shape can help (nova#2395).
fn empty_by_length(answer_content: &str, finish_reason: Option<&str>) -> bool {
    answer_content.trim().is_empty() && finish_reason == Some("length")
}

/// One parsed answer, with the text it came from (the retry shows it to the model).
struct First {
    sql: String,
    sentence: String,
    content: String,
    /// The `model_call` row this answer wrote, when the store took it.
    call_id: Option<i64>,
}

/// Why one endpoint did not answer, and whether another one may be asked.
struct CallFailure {
    /// The refusal the person reads.
    response: Response,
    /// `true` for the reasons D3 names — a transport error, an upstream 5xx or 429, and an
    /// answer that is not the JSON shape. Our own budget refusal is `false`: the person's
    /// spent budget stays visible.
    retryable: bool,
}

/// What one endpoint answered, before the question's own fields (`tables`, `tried`).
#[derive(Debug)]
struct Answered {
    endpoint: String,
    model: String,
    call_id: Option<i64>,
    sql: Option<String>,
    sentence: String,
    explain: String,
    error: Option<String>,
    /// `agent` or `sql`.
    mode: &'static str,
    /// The agent's answer in words; `None` on the `sql` path.
    text: Option<String>,
    /// The tool calls an agent answer made; empty on the `sql` path.
    steps: Vec<crate::ask_agent::AskStep>,
}

/// True when an upstream refusal is a `5xx` or a `429` — the case another endpoint may
/// answer. The status is the refusal's first field (`model_chat::ChatError::Refused`).
fn refusable_by_another(message: &str) -> bool {
    let status: Option<u16> = message
        .split(':')
        .next()
        .map(str::trim)
        .and_then(|status| status.parse().ok());
    matches!(status, Some(429) | Some(500..=599))
}

/// Everything a chat call needs that does not change between the first call and the retry.
#[derive(Clone)]
struct Attempt<'a> {
    client: &'a reqwest::Client,
    pool: &'a sqlx::PgPool,
    object: &'a Value,
    /// The `ModelEndpoint` object's name — never its URL.
    name: &'a str,
    model: &'a str,
    /// The endpoint's base URL; it stays inside this module.
    url: &'a str,
    key: Option<&'a str>,
    sub: &'a str,
    group: Option<&'a str>,
    /// What is stored as the call's prompt and hashed into its row.
    prompt: &'a str,
}

impl Attempt<'_> {
    /// One budget check, one chat call, one `model_call` row.
    // The `Err` is the ready-made refusal this returns, as in `catalog_search::filter_value`.
    #[allow(clippy::result_large_err)]
    async fn call(&self, messages: &Value) -> Result<First, CallFailure> {
        if let Err(refusal) = crate::model_calls::check(
            self.pool,
            &crate::model_calls::quota_of(self.object),
            self.group,
            self.sub,
        )
        .await
        {
            // The person's spent budget is not a reason to ask another endpoint.
            return Err(CallFailure {
                response: crate::model_calls::refuse(&refusal),
                retryable: false,
            });
        }
        let extra = json!({
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "answer",
                    "schema": {
                        "type": "object",
                        "properties": {"sql": {"type": "string"}, "sentence": {"type": "string"}},
                        "required": ["sql", "sentence"]
                    }
                }
            }
        });
        let started = std::time::Instant::now();
        let call = crate::model_chat::Chat {
            base_url: self.url,
            model: self.model,
            key: self.key,
            messages: messages.clone(),
            extra,
        };
        let answer = match crate::model_chat::chat(self.client, &call).await {
            Ok(answer) => answer,
            Err(error) => {
                use crate::model_chat::ChatError;
                let (outcome, sentence, retryable) = match &error {
                    ChatError::Refused(sentence) => {
                        ("refused", sentence.clone(), refusable_by_another(sentence))
                    }
                    ChatError::Transport(sentence) => ("error", sentence.clone(), true),
                };
                let _ = self
                    .record(outcome, None, None, started.elapsed().as_millis() as u64)
                    .await;
                return Err(CallFailure {
                    response: fail(
                        StatusCode::BAD_GATEWAY,
                        format!("the model endpoint did not answer: {sentence}"),
                    ),
                    retryable,
                });
            }
        };
        let parsed = parse_answer(&answer.content);
        let outcome = if parsed.is_some() { "ok" } else { "error" };
        let call_id = self
            .record(
                outcome,
                answer.usage.as_ref(),
                Some(&answer.content),
                answer.latency_ms,
            )
            .await;
        match parsed {
            Some((sql, sentence)) => Ok(First {
                sql,
                sentence,
                content: answer.content,
                call_id,
            }),
            // Not the JSON shape: another endpoint may answer it. An empty answer cut by
            // the token cap names that instead, since no fallback of ours would help the
            // person understand why every model answered nothing (nova#2395).
            None => {
                let refusal = if empty_by_length(&answer.content, answer.finish_reason.as_deref()) {
                    let budget = answer
                        .usage
                        .as_ref()
                        .and_then(|usage| usage.get("completion_tokens"))
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::from(crate::model_chat::MAX_TOKENS));
                    format!(
                        "the model used its whole answer budget ({budget} tokens) before answering; try another model"
                    )
                } else {
                    "the model did not answer with SQL. Try again, or ask differently.".to_string()
                };
                Err(CallFailure {
                    response: fail(StatusCode::BAD_GATEWAY, refusal),
                    retryable: true,
                })
            }
        }
    }

    /// The one row this call writes, and its `id` when the store took it. Best effort: a
    /// failure is a warning, never a failed answer.
    async fn record(
        &self,
        outcome: &str,
        usage: Option<&Value>,
        answer: Option<&str>,
        latency_ms: u64,
    ) -> Option<i64> {
        let call = crate::model_calls::Call {
            sub: self.sub,
            group: self.group,
            feature: "ask",
            endpoint: self.name,
            model: self.model,
            tokens: crate::model_calls::tokens(usage, self.prompt, answer),
            latency_ms,
            outcome,
            prompt: self.prompt,
            answer,
        };
        match crate::model_calls::record(self.pool, &call).await {
            Ok(id) => Some(id),
            Err(e) => {
                tracing::warn!(error = %e, "could not store the ask model call");
                None
            }
        }
    }
}

/// What the agent's model closure shares with the handler that awaits it: the budget refusal
/// (so a spent budget stays a `429`, never a `502`), and the last `model_call` row written.
#[derive(Default)]
struct AgentShared {
    /// The refusal the person reads; set when the budget check said no.
    refusal: Option<Response>,
    /// The row the answer reports as `callId`.
    last_call: Option<i64>,
}

/// One agent chat call: the budget check first, then the call and its own `model_call` row
/// (D3). The endpoint's URL and key stay in the [`Attempt`], as on the `sql` path.
async fn agent_model_call(
    attempt: Attempt<'_>,
    messages: Value,
    with_tools: bool,
    offered: &Value,
    shared: &std::sync::Mutex<AgentShared>,
) -> Result<crate::model_chat::Answer, crate::model_chat::ChatError> {
    use crate::model_chat::ChatError;
    if let Err(refusal) = crate::model_calls::check(
        attempt.pool,
        &crate::model_calls::quota_of(attempt.object),
        attempt.group,
        attempt.sub,
    )
    .await
    {
        if let Ok(mut state) = shared.lock() {
            state.refusal = Some(crate::model_calls::refuse(&refusal));
        }
        // Never surfaces: the handler answers with the refusal from the slot above.
        return Err(ChatError::Refused(String::new()));
    }
    let extra = if with_tools {
        json!({ "tools": offered })
    } else {
        json!({})
    };
    let started = std::time::Instant::now();
    let result = crate::model_chat::chat(
        attempt.client,
        &crate::model_chat::Chat {
            base_url: attempt.url,
            model: attempt.model,
            key: attempt.key,
            messages,
            extra,
        },
    )
    .await;
    let call_id = match &result {
        Ok(answer) => {
            // A tool call carries no text; its row keeps the message so the call is visible.
            let stored = (!answer.content.is_empty())
                .then(|| answer.content.clone())
                .or_else(|| serde_json::to_string(&answer.message).ok());
            attempt
                .record(
                    "ok",
                    answer.usage.as_ref(),
                    stored.as_deref(),
                    answer.latency_ms,
                )
                .await
        }
        Err(error) => {
            let (outcome, sentence) = match error {
                ChatError::Refused(sentence) => ("refused", sentence.clone()),
                ChatError::Transport(sentence) => ("error", sentence.clone()),
            };
            attempt
                .record(
                    outcome,
                    None,
                    Some(&sentence),
                    started.elapsed().as_millis() as u64,
                )
                .await
        }
    };
    if let Ok(mut state) = shared.lock() {
        state.last_call = call_id;
    }
    result
}

/// One `tools/call` answer as the value the model receives: the tool's structured body, or
/// its sentence when the call failed.
fn tool_value(body: &Value) -> Value {
    let result = body.get("result").cloned().unwrap_or(Value::Null);
    if result.get("isError") == Some(&Value::Bool(true)) {
        let sentence = result
            .pointer("/content/0/text")
            .and_then(Value::as_str)
            .unwrap_or("the tool failed");
        return json!({ "error": sentence });
    }
    if let Some(structured) = result.get("structuredContent") {
        return structured.clone();
    }
    let text = result
        .pointer("/content/0/text")
        .and_then(Value::as_str)
        .unwrap_or_default();
    serde_json::from_str(text).unwrap_or_else(|_| json!({ "text": text }))
}

/// The person the tools run as: the caller's own `Principal`, or dev mode's administrator.
fn tool_principal(
    caller: Option<&crate::auth::Principal>,
    state: &AppState,
) -> crate::auth::Principal {
    match caller {
        Some(caller) => caller.clone(),
        None => crate::auth::Principal {
            identity: crate::auth::Identity::dev_admin(&state.dev_user),
            via: "session",
            access_token: None,
            client_id: None,
        },
    }
}

/// Everything the agent path needs that the handler already resolved (D1, D2, D6).
struct AgentAsk<'a> {
    state: &'a Arc<AppState>,
    principal: Option<&'a crate::auth::Principal>,
    pool: &'a sqlx::PgPool,
    question: &'a str,
    previous: Option<&'a AskPrevious>,
    /// The conversation so far; when it is not empty, `previous` is not used (D2).
    thread: &'a [AskTurn],
    /// The endpoints the question may ask, in order: the named one, or the auto ranking.
    endpoints: &'a [(String, Value)],
    /// The MCP tool list (`mcp::tools()`): only its read-only entries are offered.
    mcp_tools: &'a Value,
    sub: &'a str,
    group: Option<&'a str>,
}

/// Everything one agent question needs that does not change between its endpoints.
struct AgentCall<'a> {
    state: &'a Arc<AppState>,
    principal: Option<&'a crate::auth::Principal>,
    pool: &'a sqlx::PgPool,
    client: &'a reqwest::Client,
    sub: &'a str,
    group: Option<&'a str>,
    messages: &'a Value,
    /// The OpenAI `tools` array: the endpoints' read-only menu.
    offered: &'a Value,
    /// What the rows store as the call's prompt.
    prompt: &'a str,
}

impl AgentCall<'_> {
    /// The answer one endpoint gives in agent mode, or the refusal another one may answer.
    // The `Err` is the ready-made refusal this returns, as in `catalog_search::filter_value`.
    #[allow(clippy::result_large_err)]
    async fn one(&self, name: &str, object: &Value) -> Result<Answered, CallFailure> {
        let model = endpoint_model(object);
        let url = object
            .pointer("/spec/url")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string();
        if url.is_empty() || model.is_empty() {
            return Err(CallFailure {
                response: fail(StatusCode::SERVICE_UNAVAILABLE, NO_ENDPOINT),
                retryable: true,
            });
        }
        let key = crate::model_chat::key_for(name);
        let attempt = Attempt {
            client: self.client,
            pool: self.pool,
            object,
            name,
            model: &model,
            url: &url,
            key: key.as_deref(),
            sub: self.sub,
            group: self.group,
            prompt: self.prompt,
        };
        let shared = std::sync::Arc::new(std::sync::Mutex::new(AgentShared::default()));
        let model_call = {
            let shared = std::sync::Arc::clone(&shared);
            let offered = self.offered.clone();
            move |messages: Value, with_tools: bool| {
                let shared = std::sync::Arc::clone(&shared);
                let offered = offered.clone();
                let attempt = attempt.clone();
                async move { agent_model_call(attempt, messages, with_tools, &offered, &shared).await }
            }
        };
        let state = Arc::clone(self.state);
        let cfg = std::sync::Arc::new(
            state
                .mcp
                .clone()
                .expect("the handler checked Nova's MCP endpoint"),
        );
        let principal = tool_principal(self.principal, &state);
        let tool_call = move |name: String, arguments: Value| {
            let state = Arc::clone(&state);
            let cfg = std::sync::Arc::clone(&cfg);
            let principal = principal.clone();
            async move {
                let request = json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": { "name": name, "arguments": arguments },
                });
                let facts = crate::auth::BearerFacts {
                    scopes: vec![crate::mcp::READ_SCOPE.to_string()],
                    client_id: None,
                };
                let response = crate::mcp::call_tool(
                    &state,
                    &cfg,
                    &principal,
                    &facts,
                    Some(json!(1)),
                    &request,
                )
                .await;
                let (_, body) = body_of(response).await;
                tool_value(&body)
            }
        };
        let send_rows = crate::model_endpoints::row(object).send_rows;
        let outcome = match crate::ask_agent::run(
            model_call,
            tool_call,
            self.messages.clone(),
            self.offered,
            send_rows,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                let refusal = shared
                    .lock()
                    .ok()
                    .and_then(|mut state| state.refusal.take());
                if let Some(response) = refusal {
                    return Err(CallFailure {
                        response,
                        retryable: false,
                    });
                }
                use crate::model_chat::ChatError;
                let (sentence, retryable) = match &error {
                    ChatError::Refused(sentence) => {
                        (sentence.clone(), refusable_by_another(sentence))
                    }
                    ChatError::Transport(sentence) => (sentence.clone(), true),
                };
                return Err(CallFailure {
                    response: fail(
                        StatusCode::BAD_GATEWAY,
                        format!("the model endpoint did not answer: {sentence}"),
                    ),
                    retryable,
                });
            }
        };
        let call_id = shared.lock().ok().and_then(|state| state.last_call);
        let text = outcome.text;
        let sentence = text
            .as_deref()
            .and_then(|text| text.lines().find(|line| !line.trim().is_empty()))
            .map(|line| line.trim().to_string())
            .unwrap_or_else(|| "the model answered without words".to_string());
        Ok(Answered {
            endpoint: name.to_string(),
            model,
            call_id,
            sql: outcome.last_sql,
            sentence,
            explain: "not_run".to_string(),
            error: None,
            mode: "agent",
            text,
            steps: outcome.steps,
        })
    }
}

/// The messages one agent question sends (D2): the system prompt, then the conversation so
/// far when there is one (each turn's question and answer), else the `previous` pair, then
/// the question.
fn agent_messages(thread: &[AskTurn], previous: Option<&AskPrevious>, question: &str) -> Value {
    let mut messages: Vec<Value> = vec![json!({
        "role": "system",
        "content": AGENT_SYSTEM_PROMPT,
    })];
    if thread.is_empty() {
        // D2: older clients and the CLI keep the single `previous` pair.
        if let Some(previous) = previous {
            messages.push(json!({"role": "user", "content": previous.question}));
            messages.push(json!({"role": "assistant", "content": previous.sql}));
        }
    } else {
        for turn in thread {
            messages.push(json!({"role": "user", "content": turn.question}));
            messages.push(json!({"role": "assistant", "content": turn.answer}));
        }
    }
    messages.push(json!({"role": "user", "content": question}));
    Value::Array(messages)
}

/// The agent path: the model works with the read-only tools and answers in words (D1–D6).
async fn agent_ask(plan: AgentAsk<'_>) -> Response {
    let Some(client) = crate::model_probe::http_client() else {
        return fail(StatusCode::SERVICE_UNAVAILABLE, NO_ENDPOINT);
    };
    let offered = crate::ask_agent::openai_tools(plan.mcp_tools);
    let messages = agent_messages(plan.thread, plan.previous, plan.question);
    // A fallback endpoint that cannot use tools cannot do what this question asks for.
    let endpoints: Vec<(String, Value)> = plan
        .endpoints
        .iter()
        .take(FALLBACK_ENDPOINTS)
        .filter(|(_, object)| crate::model_endpoints::row(object).tool_calling == Some(true))
        .cloned()
        .collect();
    let asking = AgentCall {
        state: plan.state,
        principal: plan.principal,
        pool: plan.pool,
        client: &client,
        sub: plan.sub,
        group: plan.group,
        messages: &messages,
        offered: &offered,
        prompt: plan.question,
    };
    let (answered, tried) = match ask_ranked(&endpoints, |index| {
        let (name, object) = &endpoints[index];
        let asking = &asking;
        async move { asking.one(name, object).await }
    })
    .await
    {
        Ok(answer) => answer,
        Err(response) => return response,
    };
    let body = AskAnswer {
        endpoint: answered.endpoint,
        model: answered.model,
        tried,
        call_id: answered.call_id,
        sql: answered.sql,
        sentence: answered.sentence,
        tables: Vec::new(),
        explain: answered.explain,
        error: answered.error,
        mode: answered.mode.to_string(),
        text: answered.text,
        steps: answered.steps,
    };
    record_conversation(plan.pool, plan.sub, plan.question, &body).await;
    answer(body)
}

/// One question's attempt on one endpoint: the model call, `EXPLAIN` as the caller, and at
/// most one correction. Everything that does not change between the endpoints is here.
struct Asking<'a> {
    state: &'a Arc<AppState>,
    principal: Option<&'a crate::auth::Principal>,
    pool: &'a sqlx::PgPool,
    client: &'a reqwest::Client,
    /// What is stored as the call's prompt and hashed into its row.
    user_text: &'a str,
    messages: &'a Value,
    sub: &'a str,
    group: Option<&'a str>,
}

impl Asking<'_> {
    /// Asks one endpoint and checks what it wrote, then asks it once more with the
    /// engine's sentence when the statement does not run.
    // The `Err` is the ready-made refusal this returns, as in `catalog_search::filter_value`.
    #[allow(clippy::result_large_err)]
    async fn one(&self, name: &str, object: &Value) -> Result<Answered, CallFailure> {
        let url = object
            .pointer("/spec/url")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string();
        let model = object
            .pointer("/spec/model")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string();
        if url.is_empty() || model.is_empty() {
            // A broken object is not an answer: the next endpoint may still say it.
            return Err(CallFailure {
                response: fail(StatusCode::SERVICE_UNAVAILABLE, NO_ENDPOINT),
                retryable: true,
            });
        }
        let key = crate::model_chat::key_for(name);
        let attempt = Attempt {
            client: self.client,
            pool: self.pool,
            object,
            name,
            model: &model,
            url: &url,
            key: key.as_deref(),
            sub: self.sub,
            group: self.group,
            prompt: self.user_text,
        };
        let first = attempt.call(self.messages).await?;
        let Some(error) = check_statement(self.state, self.principal, &first.sql)
            .await
            .err()
        else {
            return Ok(Answered {
                endpoint: name.to_string(),
                model,
                call_id: first.call_id,
                sql: Some(first.sql),
                sentence: first.sentence,
                explain: "passed".to_string(),
                error: None,
                mode: "sql",
                text: None,
                steps: Vec::new(),
            });
        };

        // One correction: the model sees its own statement and why it does not run.
        let correction = format!(
            "That statement is not runnable: {error}. Answer with the corrected statement in the same JSON shape."
        );
        let retry = json!([
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": self.user_text},
            {"role": "assistant", "content": first.content},
            {"role": "user", "content": correction},
        ]);
        let second = attempt.call(&retry).await?;
        Ok(
            match check_statement(self.state, self.principal, &second.sql).await {
                Ok(()) => Answered {
                    endpoint: name.to_string(),
                    model,
                    call_id: second.call_id,
                    sql: Some(second.sql),
                    sentence: second.sentence,
                    explain: "passed".to_string(),
                    error: None,
                    mode: "sql",
                    text: None,
                    steps: Vec::new(),
                },
                Err(second_error) => Answered {
                    endpoint: name.to_string(),
                    model,
                    call_id: second.call_id,
                    sql: Some(second.sql),
                    sentence: second.sentence,
                    explain: "failed".to_string(),
                    error: Some(second_error),
                    mode: "sql",
                    text: None,
                    steps: Vec::new(),
                },
            },
        )
    }
}

/// Asks the endpoints in their order, at most [`FALLBACK_ENDPOINTS`] of them, and answers
/// with what it tried. A named endpoint is a one-item list, so it never falls back; our
/// own budget refusal stops the loop too (`CallFailure::retryable`).
// The `Err` is the ready-made refusal this returns, as in `catalog_search::filter_value`.
#[allow(clippy::result_large_err)]
async fn ask_ranked<F, Fut>(
    endpoints: &[(String, Value)],
    mut one: F,
) -> Result<(Answered, Vec<String>), Response>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<Answered, CallFailure>>,
{
    let mut tried = Vec::new();
    let mut failure: Option<Response> = None;
    for (index, (name, _)) in endpoints.iter().take(FALLBACK_ENDPOINTS).enumerate() {
        tried.push(name.clone());
        match one(index).await {
            Ok(answered) => return Ok((answered, tried)),
            Err(call) => {
                failure = Some(call.response);
                if !call.retryable {
                    break;
                }
            }
        }
    }
    Err(failure.unwrap_or_else(|| fail(StatusCode::SERVICE_UNAVAILABLE, NO_ENDPOINT)))
}

/// D1: whether one question is answered in agent mode. Absent means `agent` when the model
/// can use tools and `sql` otherwise; an explicit `agent` on a model that cannot use tools
/// is refused, because the question asked for something that model cannot do.
fn ask_mode(requested: Option<&str>, can_use_tools: bool) -> Result<bool, &'static str> {
    match requested {
        Some("sql") => Ok(false),
        Some("agent") if can_use_tools => Ok(true),
        Some("agent") => {
            Err("this model cannot use tools; send mode \"sql\" or choose another model")
        }
        Some(_) => Err("mode is agent or sql"),
        None => Ok(can_use_tools),
    }
}

/// The model's statement, through the MCP guard and then `EXPLAIN` — never run.
async fn check_statement(
    state: &Arc<AppState>,
    principal: Option<&crate::auth::Principal>,
    sql: &str,
) -> Result<(), String> {
    let statement = crate::mcp::classify(sql, true)?;
    crate::mcp::run_statement(
        state,
        principal,
        &statement,
        1,
        ASK_QUERY_TIMEOUT,
        "ask",
        None,
    )
    .await
    .map(|_| ())
}

/// Why a previous turn may not be refined, or `None` when it may.
fn previous_refusal(previous: &AskPrevious) -> Option<String> {
    if previous.question.trim().is_empty() || previous.question.chars().count() > MAX_QUESTION_CHARS
    {
        return Some(format!(
            "the previous question must be 1 to {MAX_QUESTION_CHARS} characters"
        ));
    }
    if previous.sql.trim().is_empty() {
        return Some(format!(
            "the previous statement must be 1 to {MAX_PREVIOUS_SQL_CHARS} characters"
        ));
    }
    if previous.sql.chars().count() > MAX_PREVIOUS_SQL_CHARS {
        return Some(format!(
            "the previous statement is longer than {MAX_PREVIOUS_SQL_CHARS} characters"
        ));
    }
    if previous.tables.len() > CANDIDATES {
        return Some(format!(
            "the previous turn names more than {CANDIDATES} tables"
        ));
    }
    None
}

/// Why a conversation thread may not be sent, or `None` when it may.
fn thread_refusal(thread: &[AskTurn]) -> Option<String> {
    if thread.len() > MAX_THREAD_TURNS {
        return Some(format!(
            "the thread must be at most {MAX_THREAD_TURNS} turns"
        ));
    }
    for turn in thread {
        if turn.question.trim().is_empty() || turn.question.chars().count() > MAX_QUESTION_CHARS {
            return Some(format!(
                "each thread question must be 1 to {MAX_QUESTION_CHARS} characters"
            ));
        }
        if turn.answer.trim().is_empty() || turn.answer.chars().count() > MAX_TURN_ANSWER_CHARS {
            return Some(format!(
                "each thread answer must be 1 to {MAX_TURN_ANSWER_CHARS} characters"
            ));
        }
    }
    None
}

/// Two table names are the same table when all three parts are equal.
fn same_table(a: &AskTable, b: &AskTable) -> bool {
    a.catalog == b.catalog && a.namespace == b.namespace && a.table == b.table
}

/// The tables a follow-up may choose from: the previous turn's first, then the new search's
/// matches, without duplicates and at most [`CANDIDATES`]. Without a previous turn the
/// search's answer is used unchanged.
fn follow_up_candidates(previous: Option<&AskPrevious>, found: Vec<AskTable>) -> Vec<AskTable> {
    let Some(previous) = previous else {
        return found;
    };
    let mut merged: Vec<AskTable> = previous
        .tables
        .iter()
        .map(|table| AskTable {
            catalog: table.catalog.clone(),
            namespace: table.namespace.clone(),
            table: table.table.clone(),
        })
        .collect();
    for table in found {
        if !merged.iter().any(|kept| same_table(kept, &table)) {
            merged.push(table);
        }
    }
    merged.truncate(CANDIDATES);
    merged
}

/// Every `catalog.namespace.table` the question names, in order (D1). Trailing punctuation
/// (`?`, `.`, `,`, `!`) is ignored.
fn qualified_names(question: &str) -> Vec<AskTable> {
    let allowed = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-';
    question
        .split_whitespace()
        .filter_map(|word| {
            let word = word.to_lowercase();
            let word = word.trim_end_matches(['?', '.', ',', '!']);
            let parts: Vec<&str> = word.split('.').collect();
            if parts.len() != 3
                || parts
                    .iter()
                    .any(|part| part.is_empty() || !part.chars().all(allowed))
            {
                return None;
            }
            Some(AskTable {
                catalog: parts[0].to_string(),
                namespace: parts[1].to_string(),
                table: parts[2].to_string(),
            })
        })
        .collect()
}

/// D1 names first, then the searched matches; duplicates dropped; at most [`CANDIDATES`].
fn merge_candidates(named: Vec<AskTable>, searched: Vec<AskTable>) -> Vec<AskTable> {
    let mut merged: Vec<AskTable> = Vec::with_capacity(named.len() + searched.len());
    for table in named.into_iter().chain(searched) {
        if !merged.iter().any(|kept| same_table(kept, &table)) {
            merged.push(table);
        }
        if merged.len() == CANDIDATES {
            break;
        }
    }
    merged
}

/// The tables the question matches in `catalogs`, in that order (D2): the every-word search
/// in each, then — when none matched in any — the any-word fallback in each, until
/// [`CANDIDATES`] are collected. A catalog that does not answer is the `502` a failed
/// search answers today.
// The `Err` is the ready-made refusal this returns, as in `Attempt::call`.
#[allow(clippy::result_large_err)]
async fn search_in_catalogs(
    state: &Arc<AppState>,
    caller: &Option<axum::Extension<crate::auth::Principal>>,
    question: &str,
    catalogs: &[String],
) -> Result<Vec<AskTable>, Response> {
    let mut found: Vec<AskTable> = Vec::new();
    // The search refuses `q` over 100 characters; such a question goes to the fallback.
    if every_word_search(question) {
        for catalog in catalogs {
            let response = crate::catalog_search::search(
                State(Arc::clone(state)),
                Query(crate::catalog_search::SearchParams {
                    q: Some(question.to_string()),
                    catalog: Some(catalog.clone()),
                    limit: Some(CANDIDATES - found.len()),
                    tag: None,
                    owner: None,
                    domain: None,
                }),
                caller.clone(),
            )
            .await;
            let (status, body) = body_of(response).await;
            if status != StatusCode::OK {
                let sentence = body
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("the catalog did not answer")
                    .to_string();
                return Err(fail(StatusCode::BAD_GATEWAY, sentence));
            }
            if let Some(matches) = body.get("matches").and_then(Value::as_array) {
                found.extend(matches.iter().filter_map(ask_table));
            }
            if found.len() >= CANDIDATES {
                found.truncate(CANDIDATES);
                return Ok(found);
            }
        }
    }
    if found.is_empty() {
        let words: Vec<String> = question.split_whitespace().map(str::to_lowercase).collect();
        for catalog in catalogs {
            match crate::catalog_search::search_any_word(
                state,
                caller.as_ref().map(|c| &c.0),
                Some(catalog),
                &words,
                CANDIDATES - found.len(),
            )
            .await
            {
                Ok(matches) => found.extend(matches.into_iter().map(|table| AskTable {
                    catalog: table.catalog,
                    namespace: table.namespace,
                    table: table.table,
                })),
                Err(sentence) => return Err(fail(StatusCode::BAD_GATEWAY, sentence)),
            }
            if found.len() >= CANDIDATES {
                found.truncate(CANDIDATES);
                break;
            }
        }
        if !found.is_empty() {
            tracing::info!(
                tables = found.len(),
                "ask: no table had every word; using the best word matches"
            );
        }
    }
    Ok(found)
}

/// Drop the candidates whose documentation is `Value::Null` — the caller may not read that
/// table, so it is not in the prompt — together with their documentation entries.
fn drop_unreadable(candidates: Vec<AskTable>, docs: Vec<Value>) -> (Vec<AskTable>, Vec<Value>) {
    candidates
        .into_iter()
        .zip(docs)
        .filter(|(_, doc)| !doc.is_null())
        .unzip()
}

/// The user message: the previous turn in words when this question refines it, the question,
/// and the candidate tables' documentation. `previous.question` follows the question's own
/// limit, its statement the 8000-character one.
fn user_text_of(question: &str, previous: Option<&AskPrevious>, context: &Value) -> String {
    let before = previous
        .map(|previous| {
            format!(
                "The previous question: {}\nIts statement: {}\nThe question below refines it; change that statement unless the question asks for something new.\n\n",
                previous.question, previous.sql
            )
        })
        .unwrap_or_default();
    format!(
        "{before}The question: {question}\n\nThe tables you may use, with their documentation:\n{context}"
    )
}

/// The model an endpoint object names, trimmed; empty when the object has none.
fn endpoint_model(object: &Value) -> String {
    object
        .pointer("/spec/model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string()
}

/// The answer when no candidate table is left: no statement, and a sentence saying so.
fn no_table_answer(endpoint: String, model: String, tried: Vec<String>) -> AskAnswer {
    AskAnswer {
        endpoint,
        model,
        tried,
        call_id: None,
        sql: None,
        sentence: NO_TABLE.to_string(),
        tables: Vec::new(),
        explain: "not_run".to_string(),
        error: None,
        mode: "sql".to_string(),
        text: None,
        steps: Vec::new(),
    }
}

/// `POST /api/ask` — a question in, checked SQL out.
#[utoipa::path(
    post,
    operation_id = "ask_answer",
    path = "/api/ask",
    tag = "public",
    description = "A question in words, one Trino statement out. A follow-up may name the answered turn it refines in `previous` — that turn's question, statement and tables — so a second message like \"only org domains\" changes the first statement instead of starting over: the previous turn's tables are tried first and each is re-read as the caller, which drops the ones the caller may not read. In agent mode, `thread` (at most 10 turns, each its question and the answer in words) is the conversation so far and is sent to the model as earlier messages; with a `thread`, `previous` is not used in agent mode. Nova searches as the caller for at most five tables — a table the question names in full (`catalog.namespace.table`) first, then, in every catalog the caller may browse (bronze, silver, gold, then the rest alphabetically, at most ten; the one `catalog` names, when given), the tables whose name contains every word of the question, or, when none does, the tables sharing the most words with it — sends the question and **the documentation of those tables** — description, tags, owner, domain, column names and their descriptions, and the tables' metric definitions (name, description, expression, time column, dimensions) — **and up to five of their verified queries** (question and statement only, never the expected rows) to one `ModelEndpoint` whose purpose list contains `ask` and whose probe found it reachable with JSON output, and answers the statement the model writes. `endpoint` names the one that answers; absent or `auto` is auto mode, which asks the ready endpoint whose eval runs on this catalog passed most often in the last 30 days (three runs at least; ties and endpoints with fewer runs in name order) and, when it fails with no answer, an upstream 5xx or 429, or an answer that is not the JSON shape, once more to the next one — at most two endpoints per question, and a named endpoint never falls back. `tried` names the endpoints asked, in order. `mode` is `agent` or `sql`; absent is `agent` when the endpoint that answers can use tools (its probe found tool calling, and Nova's MCP endpoint is on) and `sql` otherwise. In agent mode the model works with the platform's **read-only** MCP tools as the caller, holding the `stardelt:read` scope only: at most 8 model calls and 6 tool calls per question, every tool result cut at 8 000 characters, and the endpoint's `sendRows` decides whether a `sql_read` result reaches the model whole or as its columns and row count. The answer then carries `text` (the answer in words) and `steps` (the tool calls, with at most 50 rows of a `sql_read` for the panel); `sql` is the last statement it ran, and nothing is `EXPLAIN`-checked because the tools themselves are read-only. An explicit `agent` on a model that cannot use tools is a `400`. No row, no query result and no table data ever leaves the platform, whatever the endpoint's `sendRows` says; never the endpoint's URL or its key either. The statement is **checked with `EXPLAIN` as the caller and is not run**: `explain` is `passed` or `failed`, and a failed check keeps the SQL in the answer with the engine's sentence in `error` (one correction is asked of the model first). No matching table is a `200` with `sql: null` and a sentence saying so; a model endpoint that is refused, unreachable or does not answer in the JSON shape is a `502`. A spent budget is the `429` the model budget answers (see `/api/admin/model-usage`). Every answer carries `Cache-Control: no-store`.",
    params(("catalog" = Option<String>, Query, description = "The catalog to search for tables; omitted, every catalog the caller may browse")),
    request_body = AskRequest,
    responses(
        (status = 200, description = "The statement, its sentence, the tables it was written for and the `EXPLAIN` result", body = AskAnswer),
        (status = 400, description = "The question is empty or longer than 2000 characters, the previous turn's question, statement or table list breaks its limit, `endpoint` names a `ModelEndpoint` that cannot answer Ask now, `mode` is neither `agent` nor `sql`, or `mode` is `agent` and the model cannot use tools", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 429, description = "The person's or their group's model budget is spent", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog, the cluster or the model endpoint did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No state store, no cluster connection, no HTTP client, or no ready `ask` endpoint", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn ask(
    State(state): State<Arc<AppState>>,
    caller: Option<axum::Extension<crate::auth::Principal>>,
    Query(params): Query<AskQuery>,
    Json(request): Json<AskRequest>,
) -> Response {
    let question = request.question.trim().to_string();
    if question.is_empty() || question.chars().count() > MAX_QUESTION_CHARS {
        return fail(
            StatusCode::BAD_REQUEST,
            "a question is required: 1 to 2000 characters",
        );
    }
    if let Some(refusal) = request.previous.as_ref().and_then(previous_refusal) {
        return fail(StatusCode::BAD_REQUEST, refusal);
    }
    if let Some(refusal) = thread_refusal(&request.thread) {
        return fail(StatusCode::BAD_REQUEST, refusal);
    }
    let Some(pool) = state.db.as_ref() else {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "Ask needs Nova's database, and this installation has none",
        );
    };
    let Some(kube) = state.kube.clone() else {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "no model endpoint is connected",
        );
    };
    let objects: Vec<Value> = match crate::model_endpoints::endpoints_api(kube)
        .list(&kube::api::ListParams::default())
        .await
    {
        Ok(list) => {
            let mut values = Vec::with_capacity(list.items.len());
            for object in list.items {
                match serde_json::to_value(object) {
                    Ok(value) => values.push(value),
                    Err(e) => tracing::warn!(error = %e, "ask: a model endpoint is not JSON"),
                }
            }
            values
        }
        Err(e) => {
            tracing::warn!(error = %e, "ask: model endpoint list failed");
            return fail(StatusCode::BAD_GATEWAY, "the cluster did not answer");
        }
    };
    let ready = ready_ask_endpoints(&objects);
    if ready.is_empty() {
        return fail(StatusCode::SERVICE_UNAVAILABLE, NO_ENDPOINT);
    }
    // Auto mode ranks the ready endpoints by this catalog's evals; a named endpoint is
    // taken as asked and refused when it cannot answer (D1).
    let catalog = params
        .catalog
        .clone()
        .unwrap_or_else(|| state.warehouse_name.clone());
    let shares = match eval_shares(pool, &catalog).await {
        Ok(shares) => shares,
        Err(e) => {
            // Without the eval history auto is today's behaviour, name order.
            tracing::warn!(error = %e, "ask: the eval shares were not read");
            std::collections::HashMap::new()
        }
    };
    let endpoints = match choose_endpoints(ready, request.endpoint.as_deref(), &shares) {
        Ok(endpoints) => endpoints,
        Err(response) => return response,
    };
    let Some((first_name, first_object)) = endpoints.first() else {
        return fail(StatusCode::SERVICE_UNAVAILABLE, NO_ENDPOINT);
    };
    // The same group is checked against the quota and written to the row, so spend and
    // limit agree.
    let sub = match caller.as_ref() {
        Some(caller) => caller.0.identity.sub.clone(),
        None => state.dev_user.clone(),
    };
    let group = caller
        .as_ref()
        .and_then(|caller| caller.0.identity.groups.first().cloned());

    // D1: agent mode is the default when the endpoint that answers can use tools. Without
    // Nova's MCP endpoint there are no tools to run, so the model cannot use them here.
    let mcp_tools = state.mcp.as_ref().map(|_| crate::mcp::tools());
    let can_use_tools =
        mcp_tools.is_some() && crate::model_endpoints::row(first_object).tool_calling == Some(true);
    let agent = match ask_mode(request.mode.as_deref(), can_use_tools) {
        Ok(agent) => agent,
        Err(message) => return fail(StatusCode::BAD_REQUEST, message),
    };
    if agent {
        let tools = mcp_tools.expect("checked by can_use_tools");
        return agent_ask(AgentAsk {
            state: &state,
            principal: caller.as_ref().map(|caller| &caller.0),
            pool,
            question: &question,
            previous: request.previous.as_ref(),
            thread: &request.thread,
            endpoints: &endpoints,
            mcp_tools: &tools,
            sub: &sub,
            group: group.as_deref(),
        })
        .await;
    }

    // D1: a table the question names in full is a candidate first, whatever the search says.
    let named = qualified_names(&question);
    // D2: without `?catalog=` every catalog the caller may browse is searched, in
    // `list_catalogs` order; with it, that catalog only, as before.
    let catalogs: Vec<String> = match params.catalog.clone() {
        Some(catalog) => vec![catalog],
        None => {
            let response =
                crate::catalog::list_catalogs(State(Arc::clone(&state)), caller.clone()).await;
            let (status, body) = body_of(response).await;
            if status != StatusCode::OK {
                let sentence = body
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("the catalog did not answer")
                    .to_string();
                return fail(StatusCode::BAD_GATEWAY, sentence);
            }
            body.get("catalogs")
                .and_then(Value::as_array)
                .map(|catalogs| {
                    catalogs
                        .iter()
                        .filter_map(|catalog| catalog.get("name")?.as_str().map(str::to_string))
                        .take(MAX_CATALOGS)
                        .collect()
                })
                .unwrap_or_default()
        }
    };
    let searched = match search_in_catalogs(&state, &caller, &question, &catalogs).await {
        Ok(searched) => searched,
        Err(response) => return response,
    };
    // A follow-up keeps the previous turn's tables first, so a question that names no table
    // still has the statement it refines.
    let mut candidates =
        follow_up_candidates(request.previous.as_ref(), merge_candidates(named, searched));
    if candidates.is_empty() {
        let body = no_table_answer(
            first_name.clone(),
            endpoint_model(first_object),
            vec![first_name.clone()],
        );
        record_conversation(pool, &sub, &question, &body).await;
        return answer(body);
    }

    let mut docs = Vec::with_capacity(candidates.len());
    for table in &candidates {
        let response = crate::catalog_docs::read(
            State(Arc::clone(&state)),
            Path((table.namespace.clone(), table.table.clone())),
            Query(crate::catalog::CatalogParam {
                catalog: Some(table.catalog.clone()),
            }),
            caller.clone(),
        )
        .await;
        let (status, body) = body_of(response).await;
        docs.push(if status == StatusCode::OK {
            body
        } else {
            Value::Null
        });
    }
    // A table the caller may not read is not in the prompt, and not in the answer: sending
    // its name back would name a table the person learned nothing about.
    let readable = drop_unreadable(candidates, docs);
    candidates = readable.0;
    docs = readable.1;
    if candidates.is_empty() {
        let body = no_table_answer(
            first_name.clone(),
            endpoint_model(first_object),
            vec![first_name.clone()],
        );
        record_conversation(pool, &sub, &question, &body).await;
        return answer(body);
    }

    let mut verified = Vec::with_capacity(candidates.len());
    for table in &candidates {
        let response = crate::verified_queries::list(
            State(Arc::clone(&state)),
            Path((table.namespace.clone(), table.table.clone())),
            Query(crate::catalog::CatalogParam {
                catalog: Some(table.catalog.clone()),
            }),
            caller.clone(),
        )
        .await;
        let (status, body) = body_of(response).await;
        verified.push(if status == StatusCode::OK {
            body
        } else {
            Value::Null
        });
    }

    let mut metrics = Vec::with_capacity(candidates.len());
    for table in &candidates {
        let rows = crate::metric_definitions::list_rows(
            pool,
            &table.catalog,
            &table.namespace,
            &table.table,
        )
        .await
        .unwrap_or_default();
        metrics.push(rows);
    }

    let context = context_of(&candidates, &docs, &verified, &metrics);
    let user_text = user_text_of(&question, request.previous.as_ref(), &context);
    let messages = json!([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_text},
    ]);

    let Some(client) = crate::model_probe::http_client() else {
        return fail(StatusCode::SERVICE_UNAVAILABLE, NO_ENDPOINT);
    };
    let asking = Asking {
        state: &state,
        principal: caller.as_ref().map(|caller| &caller.0),
        pool,
        client: &client,
        user_text: &user_text,
        messages: &messages,
        sub: &sub,
        group: group.as_deref(),
    };
    let (answered, tried) = match ask_ranked(&endpoints, |index| {
        asking.one(&endpoints[index].0, &endpoints[index].1)
    })
    .await
    {
        Ok(answer) => answer,
        Err(response) => return response,
    };
    let body = AskAnswer {
        endpoint: answered.endpoint,
        model: answered.model,
        tried,
        call_id: answered.call_id,
        sql: answered.sql,
        sentence: answered.sentence,
        tables: candidates,
        explain: answered.explain,
        error: answered.error,
        mode: answered.mode.to_string(),
        text: answered.text,
        steps: answered.steps,
    };
    record_conversation(pool, &sub, &question, &body).await;
    answer(body)
}

/// `POST /api/ask/feedback` body.
#[derive(Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AskFeedback {
    /// The `model_call` row `POST /api/ask` answered as `callId`.
    pub call_id: i64,
    /// `up` or `down`.
    pub vote: String,
}

/// `POST /api/ask/feedback` — one 👍 or 👎 on the row an Ask answer wrote (nova#891 slice b).
#[utoipa::path(
    post,
    operation_id = "ask_feedback",
    path = "/api/ask/feedback",
    tag = "public",
    description = "Writes the caller's own 👍 or 👎 on the `model_call` row that `/api/ask` returned as `callId` — the row the answer's model call wrote, and the input of the model check. A vote is `up` or `down`. A call id that belongs to somebody else is a `404`, never a `403`: an id probe learns nothing. The vote overwrites the row's earlier one; retention is the row's own. Every answer carries `Cache-Control: no-store`.",
    request_body = AskFeedback,
    responses(
        (status = 204, description = "The vote is stored"),
        (status = 400, description = "`vote` is not `up` or `down`", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No call with that id belongs to the caller", body = crate::openapi::ErrorResponse),
        (status = 500, description = "The vote was not stored", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No state store", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn feedback(
    State(state): State<Arc<AppState>>,
    caller: Option<axum::Extension<crate::auth::Principal>>,
    Json(body): Json<AskFeedback>,
) -> Response {
    if body.vote != "up" && body.vote != "down" {
        return fail(StatusCode::BAD_REQUEST, "a vote is up or down");
    }
    let Some(pool) = state.db.as_ref() else {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "Ask needs Nova's database, and this installation has none",
        );
    };
    let sub = match caller.as_ref() {
        Some(caller) => caller.0.identity.sub.clone(),
        None => state.dev_user.clone(),
    };
    let result = sqlx::query("UPDATE model_call SET feedback = $1 WHERE id = $2 AND sub = $3")
        .bind(&body.vote)
        .bind(body.call_id)
        .bind(&sub)
        .execute(pool)
        .await;
    match result {
        Ok(done) if done.rows_affected() == 1 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => fail(StatusCode::NOT_FOUND, "no such call"),
        Err(e) => {
            tracing::warn!(error = %e, "could not store the ask vote");
            fail(StatusCode::INTERNAL_SERVER_ERROR, "the vote was not stored")
        }
    }
}

/// `GET /api/ask/history` — the caller's last 20 conversations (nova#891 slice c).
#[utoipa::path(
    get,
    operation_id = "ask_history",
    path = "/api/ask/history",
    tag = "public",
    description = "The caller's own last 20 Ask questions and answers, newest first. Nobody else's conversations are readable, and there is no way to ask for another person's. Every answer carries `Cache-Control: no-store`.",
    responses(
        (status = 200, description = "The conversations, newest first", body = Vec<AskConversation>),
        (status = 503, description = "No state store", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn history(
    State(state): State<Arc<AppState>>,
    caller: Option<axum::Extension<crate::auth::Principal>>,
) -> Response {
    let Some(pool) = state.db.as_ref() else {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "Ask needs Nova's database, and this installation has none",
        );
    };
    let sub = match caller.as_ref() {
        Some(caller) => caller.0.identity.sub.clone(),
        None => state.dev_user.clone(),
    };
    let rows: Result<Vec<sqlx::postgres::PgRow>, sqlx::Error> = sqlx::query(
        "(SELECT id, question, endpoint, model, call_id, sql, sentence, tables, explain, error, \
                 to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, \
                 pinned \
          FROM ask_conversation WHERE sub = $1 AND pinned \
          ORDER BY ask_conversation.created_at DESC, id DESC LIMIT 20) \
         UNION ALL \
         (SELECT id, question, endpoint, model, call_id, sql, sentence, tables, explain, error, \
                 to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, \
                 pinned \
          FROM ask_conversation WHERE sub = $1 AND NOT pinned \
          ORDER BY ask_conversation.created_at DESC, id DESC LIMIT 20)",
    )
    .bind(&sub)
    .fetch_all(pool)
    .await;
    match rows {
        Ok(rows) => {
            let items: Vec<AskConversation> = rows.into_iter().map(conversation_of).collect();
            no_store((StatusCode::OK, Json(items)).into_response())
        }
        Err(e) => {
            tracing::warn!(error = %e, "ask: the history could not be read");
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "the history was not read",
            )
        }
    }
}

/// One `ask_conversation` row as `AskConversation`; the select names every column.
fn conversation_of(row: sqlx::postgres::PgRow) -> AskConversation {
    AskConversation {
        id: row.get("id"),
        question: row.get("question"),
        endpoint: row.get("endpoint"),
        model: row.get("model"),
        call_id: row.get("call_id"),
        sql: row.get("sql"),
        sentence: row.get("sentence"),
        tables: row.get("tables"),
        explain: row.get("explain"),
        error: row.get("error"),
        created_at: row.get("created_at"),
        pinned: row.get("pinned"),
    }
}

/// The pin body: `pinned` keeps the conversation, or lets it fall off.
#[derive(Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PinRequest {
    /// `true` keeps the conversation for good; `false` lets it fall off with the other unpinned ones.
    pub pinned: bool,
}

/// `PUT …/pin` — keep one of the caller's conversations, or let it go.
///
/// A pinned conversation is never pruned: the prune keeps the newest 20 **unpinned** ones
/// (maintainer, 2026-11-28). A person holds at most 20 pinned; the 21st is a `409`.
#[utoipa::path(
    put,
    operation_id = "ask_history_pin",
    path = "/api/ask/history/{id}/pin",
    tag = "public",
    description = "Keeps one of the caller's own conversations (`pinned: true`) or lets it fall off with the newest 20 unpinned (`pinned: false`), and answers the row. A pinned conversation is never pruned. A person holds at most 20 pinned conversations; a 21st is a `409` `at most 20 conversations can be pinned; unpin one first`. Only the caller's own conversations can be pinned: another person's id, or a missing one, is a `404` `no such conversation`, the same answer for both, so an id probe learns nothing. Every answer carries `Cache-Control: no-store`.",
    params(("id" = i64, Path, description = "The conversation's id, from History")),
    request_body = PinRequest,
    responses(
        (status = 200, description = "The conversation, as History lists it", body = AskConversation),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No conversation with that id belongs to the caller", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The caller already holds 20 pinned conversations", body = crate::openapi::ErrorResponse),
        (status = 500, description = "The conversation was not pinned", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No state store", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn history_pin(
    State(state): State<Arc<AppState>>,
    caller: Option<axum::Extension<crate::auth::Principal>>,
    Path(id): Path<i64>,
    ApiJson(body): ApiJson<PinRequest>,
) -> Response {
    let Some(pool) = state.db.as_ref() else {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "Ask needs Nova's database, and this installation has none",
        );
    };
    let sub = match caller.as_ref() {
        Some(caller) => caller.0.identity.sub.clone(),
        None => state.dev_user.clone(),
    };
    if body.pinned {
        // The row itself does not count against the limit, so pinning a pinned row is a no-op.
        let held = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM ask_conversation WHERE sub = $1 AND pinned AND id <> $2",
        )
        .bind(&sub)
        .bind(id)
        .fetch_one(pool)
        .await;
        match held {
            Ok(held) if held >= 20 => {
                return fail(
                    StatusCode::CONFLICT,
                    "at most 20 conversations can be pinned; unpin one first",
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = %e, "ask: the pinned count could not be read");
                return fail(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "the conversation was not pinned",
                );
            }
        }
    }
    let updated = sqlx::query(
        "UPDATE ask_conversation SET pinned = $3 WHERE id = $1 AND sub = $2 \
         RETURNING id, question, endpoint, model, call_id, sql, sentence, tables, explain, error, \
                   to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, \
                   pinned",
    )
    .bind(id)
    .bind(&sub)
    .bind(body.pinned)
    .fetch_optional(pool)
    .await;
    match updated {
        Ok(Some(row)) => no_store((StatusCode::OK, Json(conversation_of(row))).into_response()),
        Ok(None) => fail(StatusCode::NOT_FOUND, "no such conversation"),
        Err(e) => {
            tracing::warn!(error = %e, "ask: the conversation was not pinned");
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "the conversation was not pinned",
            )
        }
    }
}

/// `DELETE …/{id}` — remove one of the caller's own conversations.
#[utoipa::path(
    delete,
    operation_id = "ask_history_delete",
    path = "/api/ask/history/{id}",
    tag = "public",
    description = "Removes one of the caller's own Ask conversations for good. Only the caller's own: another person's id, or a missing one, is a `404` `no such conversation`, the same answer for both, so an id probe learns nothing. Every answer carries `Cache-Control: no-store`.",
    params(("id" = i64, Path, description = "The conversation's id, from History")),
    responses(
        (status = 204, description = "The conversation is gone"),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No conversation with that id belongs to the caller", body = crate::openapi::ErrorResponse),
        (status = 500, description = "The conversation was not deleted", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No state store", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn history_delete(
    State(state): State<Arc<AppState>>,
    caller: Option<axum::Extension<crate::auth::Principal>>,
    Path(id): Path<i64>,
) -> Response {
    let Some(pool) = state.db.as_ref() else {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "Ask needs Nova's database, and this installation has none",
        );
    };
    let sub = match caller.as_ref() {
        Some(caller) => caller.0.identity.sub.clone(),
        None => state.dev_user.clone(),
    };
    let deleted = sqlx::query("DELETE FROM ask_conversation WHERE id = $1 AND sub = $2")
        .bind(id)
        .bind(&sub)
        .execute(pool)
        .await;
    match deleted {
        Ok(done) if done.rows_affected() == 1 => no_store(StatusCode::NO_CONTENT.into_response()),
        Ok(_) => fail(StatusCode::NOT_FOUND, "no such conversation"),
        Err(e) => {
            tracing::warn!(error = %e, "ask: the conversation was not deleted");
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                "the conversation was not deleted",
            )
        }
    }
}

/// One search match as a table; a match without all three parts is dropped.
fn ask_table(match_: &Value) -> Option<AskTable> {
    let part = |name: &str| match_.get(name)?.as_str().map(str::to_string);
    Some(AskTable {
        catalog: part("catalog")?,
        namespace: part("namespace")?,
        table: part("table")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::post;
    use tower::ServiceExt;

    /// One endpoint as the API server holds it, with the probe's answer.
    fn endpoint(name: &str, purposes: &[&str], reachable: Value, json_output: Value) -> Value {
        json!({
            "metadata": {"name": name},
            "spec": {
                "url": "https://llm.internal.example/v1",
                "model": "qwen3-4b",
                "purposes": purposes,
                "keyRef": {"name": "llm-key", "key": "api-key"}
            },
            "status": {"reachable": reachable, "toolCalling": true, "jsonOutput": json_output},
        })
    }

    #[test]
    fn ready_lists_every_ask_endpoint_that_is_reachable_with_json_in_name_order() {
        let objects = vec![
            endpoint("agents-only", &["agents"], json!(true), json!(true)),
            endpoint("no-json", &["ask"], json!(true), Value::Null),
            endpoint("unreachable", &["ask"], json!(false), json!(true)),
            endpoint("works", &["ask", "agents"], json!(true), json!(true)),
            endpoint("alpha", &["ask"], json!(true), json!(true)),
        ];
        let ready = ready_ask_endpoints(&objects);
        let names: Vec<&str> = ready.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "works"], "name order, every ready one");
        assert_eq!(ready[1].1.pointer("/spec/model"), Some(&json!("qwen3-4b")));
        assert!(ready_ask_endpoints(&[]).is_empty());
    }

    /// The two D2 tests ask the same question of `rank`: what the catalog's evals say.
    fn ready_pair() -> Vec<(String, Value)> {
        vec![
            (
                "a".to_string(),
                endpoint("a", &["ask"], json!(true), json!(true)),
            ),
            (
                "b".to_string(),
                endpoint("b", &["ask"], json!(true), json!(true)),
            ),
        ]
    }

    #[test]
    fn auto_ranks_by_the_catalogs_eval_share() {
        let mut shares = std::collections::HashMap::new();
        shares.insert("a".to_string(), (1, 4));
        shares.insert("b".to_string(), (4, 4));
        let ranked = rank(ready_pair(), &shares);
        let names: Vec<&str> = ranked.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["b", "a"], "four of four before one of four");
    }

    #[test]
    fn auto_without_evals_keeps_name_order() {
        let ranked = rank(ready_pair(), &std::collections::HashMap::new());
        let names: Vec<&str> = ranked.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["a", "b"], "today's behaviour without evals");
        // Fewer than three runs is no verdict: those endpoints come after a ranked one.
        let mut shares = std::collections::HashMap::new();
        shares.insert("a".to_string(), (0, 2));
        shares.insert("b".to_string(), (1, 3));
        let ranked = rank(ready_pair(), &shares);
        let names: Vec<&str> = ranked.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["b", "a"], "one of three beats two runs");
        // An equal share is decided by name.
        let mut ties = std::collections::HashMap::new();
        ties.insert("a".to_string(), (2, 4));
        ties.insert("b".to_string(), (3, 6));
        let ranked = rank(ready_pair(), &ties);
        let names: Vec<&str> = ranked.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["a", "b"], "a tie goes by name");
    }

    /// One answer as `Asking::one` would return it for `name`.
    fn answered(name: &str) -> Answered {
        Answered {
            endpoint: name.to_string(),
            model: "qwen3-4b".to_string(),
            call_id: None,
            sql: Some("SELECT 1".to_string()),
            sentence: "One.".to_string(),
            explain: "passed".to_string(),
            error: None,
            mode: "sql",
            text: None,
            steps: Vec::new(),
        }
    }

    #[tokio::test]
    async fn auto_falls_back_once_on_an_upstream_429() {
        let ranked = vec![
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Null),
            ("c".to_string(), Value::Null),
        ];
        let (answer, tried) = ask_ranked(&ranked, |index| async move {
            if index == 0 {
                return Err(CallFailure {
                    response: fail(
                        StatusCode::BAD_GATEWAY,
                        "the model endpoint did not answer: 429: slow down",
                    ),
                    retryable: true,
                });
            }
            Ok(answered("b"))
        })
        .await
        .expect("the second endpoint answers");
        assert_eq!(tried, vec!["a", "b"], "two endpoints at most");
        assert_eq!(answer.endpoint, "b");
    }

    #[tokio::test]
    async fn auto_does_not_fall_back_on_the_persons_budget() {
        let ranked = ready_pair();
        let asked = std::cell::Cell::new(0);
        let response = ask_ranked(&ranked, |_| {
            asked.set(asked.get() + 1);
            async {
                Err(CallFailure {
                    response: fail(
                        StatusCode::TOO_MANY_REQUESTS,
                        "Your group used its 2 M tokens",
                    ),
                    retryable: false,
                })
            }
        })
        .await
        .expect_err("a spent budget is the answer");
        assert_eq!(asked.get(), 1, "the second endpoint is not asked");
        let (status, body) = body_of(response).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body["error"], json!("Your group used its 2 M tokens"));
    }

    #[tokio::test]
    async fn a_named_endpoint_that_is_not_ready_is_refused() {
        let ready = ready_pair();
        let shares = std::collections::HashMap::new();
        let response = choose_endpoints(ready.clone(), Some("gone"), &shares).expect_err("a 400");
        let (status, body) = body_of(response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"],
            json!("endpoint `gone` cannot answer Ask now")
        );
        // A ready one by name is taken as asked, even when the ranking prefers another.
        let mut shares = std::collections::HashMap::new();
        shares.insert("a".to_string(), (4, 4));
        let chosen = choose_endpoints(ready, Some("b"), &shares).expect("b answers");
        let names: Vec<&str> = chosen.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, vec!["b"]);
    }

    #[tokio::test]
    async fn a_named_endpoint_never_falls_back() {
        let ready = ready_pair();
        let shares = std::collections::HashMap::new();
        let chosen = choose_endpoints(ready, Some("b"), &shares).expect("b answers");
        let mut asked = Vec::new();
        let response = ask_ranked(&chosen, |index| {
            asked.push(chosen[index].0.clone());
            async {
                Err(CallFailure {
                    response: fail(
                        StatusCode::BAD_GATEWAY,
                        "the model endpoint did not answer: 503: busy",
                    ),
                    retryable: true,
                })
            }
        })
        .await
        .expect_err("the named endpoint's failure is the answer");
        assert_eq!(asked, vec!["b"], "no other endpoint is asked");
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn agent_mode_is_the_default_only_when_the_model_can_use_tools() {
        assert_eq!(ask_mode(None, true), Ok(true));
        assert_eq!(ask_mode(None, false), Ok(false));
        assert_eq!(ask_mode(Some("sql"), true), Ok(false), "`sql` is forced");
        assert_eq!(ask_mode(Some("agent"), true), Ok(true));
        assert!(
            ask_mode(Some("agent"), false).is_err(),
            "no tools, no agent"
        );
        assert!(
            ask_mode(Some("chat"), true).is_err(),
            "an unknown mode is refused"
        );
    }

    #[test]
    fn the_context_carries_the_documented_fields_and_no_rows() {
        let candidates = vec![
            AskTable {
                catalog: "warehouse".into(),
                namespace: "bronze".into(),
                table: "trips".into(),
            },
            AskTable {
                catalog: "warehouse".into(),
                namespace: "bronze".into(),
                table: "zones".into(),
            },
        ];
        let docs = vec![
            json!({
                "description": "One row per trip",
                "tags": ["raw", "gold"],
                "owner": "analytics",
                "domain": "mobility",
                "guide": "Join zones on id; drop cancelled trips.",
                "columns": [
                    {"name": "id", "type": "bigint", "description": "the trip"},
                    {"name": "zone", "type": "varchar", "description": null}
                ],
                "canEdit": false
            }),
            Value::Null,
        ];
        let metrics = vec![
            vec![crate::metric_definitions::MetricDefinition {
                id: 1,
                name: "revenue".into(),
                description: "Orders times their unit price, in euros".into(),
                expression: "sum(total_cents) / 100.0".into(),
                time_column: Some("ordered_at".into()),
                dimensions: vec!["country".into()],
                verified_by: "data-eng".into(),
                verified_at: "2026-11-27T00:00:00.000Z".into(),
            }],
            Vec::new(),
        ];
        let context = context_of(&candidates, &docs, &[], &metrics);
        assert_eq!(context[0]["name"], json!("warehouse.bronze.trips"));
        assert_eq!(context[0]["description"], json!("One row per trip"));
        assert_eq!(context[0]["tags"], json!(["raw", "gold"]));
        assert_eq!(context[0]["owner"], json!("analytics"));
        assert_eq!(context[0]["domain"], json!("mobility"));
        assert_eq!(
            context[0]["guide"],
            json!("Join zones on id; drop cancelled trips.")
        );
        assert_eq!(context[0]["columns"].as_array().map(Vec::len), Some(2));
        assert_eq!(context[0]["columns"][1]["description"], Value::Null);
        // An unreadable documentation is the table with `null` fields, not an error.
        assert_eq!(context[1]["name"], json!("warehouse.bronze.zones"));
        assert_eq!(context[1]["description"], Value::Null);
        assert_eq!(context[1]["columns"], Value::Null);
        assert_eq!(context[1]["guide"], Value::Null);
        assert_eq!(context[0]["metrics"][0]["name"], json!("revenue"));
        assert_eq!(context[0]["metrics"][0]["timeColumn"], json!("ordered_at"));
        assert_eq!(context[1]["metrics"], json!([]));
        assert_eq!(context[0]["verifiedQueries"], json!([]));
    }

    #[test]
    fn the_context_carries_at_most_five_verified_queries_and_cuts_a_long_one() {
        let candidates = vec![AskTable {
            catalog: "warehouse".into(),
            namespace: "bronze".into(),
            table: "trips".into(),
        }];
        let rows: Vec<Value> = (0..7)
            .map(|i| {
                json!({
                    "question": format!("question {i}"),
                    "sql": if i == 0 {
                        "x".repeat(MAX_VERIFIED_SQL_CHARS + 10)
                    } else {
                        format!("SELECT {i}")
                    },
                    "expected": [[1]],
                    "verifiedBy": "demo",
                })
            })
            .collect();
        let context = context_of(&candidates, &[Value::Null], &[Value::Array(rows)], &[]);
        let sent = context[0]["verifiedQueries"].as_array().expect("a list");
        assert_eq!(sent.len(), VERIFIED_QUERIES_IN_CONTEXT, "at most five");
        assert_eq!(sent[0]["question"], json!("question 0"));
        assert_eq!(sent[4]["sql"], json!("SELECT 4"), "newest five, in order");
        let sql = sent[0]["sql"].as_str().expect("a string");
        assert_eq!(sql.chars().count(), MAX_VERIFIED_SQL_CHARS, "cut with …");
        assert!(sql.ends_with('…'));
        assert_eq!(sent[0].get("expected"), None, "no rows are sent");
        assert_eq!(sent[0].get("verifiedBy"), None, "no author is sent");
        // A table whose read failed carries an empty list, not `null`.
        let empty = context_of(&candidates, &[Value::Null], &[Value::Null], &[]);
        assert_eq!(empty[0]["verifiedQueries"], json!([]));
    }

    #[test]
    fn an_answer_serialises_in_camel_case_with_null_sql() {
        let answer = AskAnswer {
            endpoint: "default".into(),
            model: "qwen3-4b".into(),
            tried: vec!["default".into()],
            call_id: Some(7),
            sql: None,
            sentence: NO_TABLE.into(),
            tables: vec![AskTable {
                catalog: "warehouse".into(),
                namespace: "bronze".into(),
                table: "trips".into(),
            }],
            explain: "not_run".into(),
            error: None,
            mode: "sql".into(),
            text: None,
            steps: Vec::new(),
        };
        let value = serde_json::to_value(&answer).unwrap();
        assert_eq!(value["explain"], json!("not_run"));
        assert_eq!(value["sql"], Value::Null);
        assert_eq!(value["error"], Value::Null);
        assert_eq!(value["tables"][0]["catalog"], json!("warehouse"));
        assert!(value.get("endpoint").is_some());
        // The id the feedback route takes back, in camel case.
        assert_eq!(value["callId"], json!(7));
        assert!(
            serde_json::to_string(&answer)
                .unwrap()
                .contains("\"callId\":7")
        );
    }

    #[test]
    fn an_answer_without_a_stored_call_answers_a_null_call_id() {
        let answer = AskAnswer {
            endpoint: "default".into(),
            model: "qwen3-4b".into(),
            tried: vec!["default".into()],
            call_id: None,
            sql: Some("SELECT 1".into()),
            sentence: "One.".into(),
            tables: vec![],
            explain: "passed".into(),
            error: None,
            mode: "sql".into(),
            text: None,
            steps: Vec::new(),
        };
        let value = serde_json::to_value(&answer).unwrap();
        assert_eq!(value["callId"], Value::Null);
        assert!(
            serde_json::to_string(&answer)
                .unwrap()
                .contains("\"callId\":null")
        );
    }

    #[test]
    fn a_model_answer_is_read_from_its_json() {
        assert_eq!(
            parse_answer("{\"sql\": \"SELECT 1\", \"sentence\": \"One.\"}"),
            Some(("SELECT 1".into(), "One.".into()))
        );
        assert_eq!(parse_answer("not json"), None);
        assert_eq!(parse_answer("{\"sql\": \" \", \"sentence\": \"x\"}"), None);
    }

    /// An empty answer the endpoint cut at `length` is named; a non-empty one, or another
    /// finish reason, is not (nova#2395).
    #[test]
    fn an_empty_answer_cut_by_length_is_named() {
        assert!(empty_by_length("", Some("length")));
        assert!(!empty_by_length("x", Some("length")));
        assert!(!empty_by_length("", Some("stop")));
    }

    fn app(state: Arc<AppState>) -> Router {
        Router::new().route("/api/ask", post(ask)).with_state(state)
    }

    /// A request to `POST /api/ask`, its status, its body and its `Cache-Control`.
    async fn ask_call(app: &Router, question: &str) -> (StatusCode, Value, Option<String>) {
        ask_body_call(app, json!({ "question": question })).await
    }

    /// One `POST /api/ask` with the given body, its status, its body and its `Cache-Control`.
    async fn ask_body_call(app: &Router, body: Value) -> (StatusCode, Value, Option<String>) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/ask")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let cache = response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
            cache,
        )
    }

    #[tokio::test]
    async fn an_empty_question_is_a_400() {
        let (status, body, _) = ask_call(&app(crate::router_tests::test_state()), "   ").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"],
            json!("a question is required: 1 to 2000 characters")
        );
    }

    #[tokio::test]
    async fn a_question_without_a_state_store_is_a_503_and_is_not_cached() {
        let (status, body, cache) =
            ask_call(&app(crate::router_tests::test_state()), "how many rows?").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body["error"],
            json!("Ask needs Nova's database, and this installation has none")
        );
        assert_eq!(cache.as_deref(), Some("no-store"));
    }

    // --- a follow-up refines the previous statement (nova#2341) ------------------------

    fn table(namespace: &str, table: &str) -> AskTable {
        AskTable {
            catalog: "bronze".into(),
            namespace: namespace.into(),
            table: table.into(),
        }
    }

    fn previous_turn() -> AskPrevious {
        AskPrevious {
            question: "can you tell me what I can do here?".into(),
            sql: "SELECT id, display_name FROM bronze.public_db.rnacentral_databases".into(),
            tables: vec![table("public_db", "rnacentral_databases")],
        }
    }

    #[test]
    fn agent_messages_puts_the_thread_between_system_and_question() {
        let thread = vec![
            AskTurn {
                question: "where is my shop data?".into(),
                answer: "It is in silver.shop and gold.shop_dbt.".into(),
            },
            AskTurn {
                question: "and how many customers?".into(),
                answer: "12 in silver.shop.customers.".into(),
            },
        ];
        let messages = agent_messages(
            &thread,
            Some(&previous_turn()),
            "and last month?",
        );
        let messages = messages.as_array().expect("an array of messages");
        assert_eq!(messages.len(), 6, "system, two pairs, the question");
        assert_eq!(messages[0].pointer("/role"), Some(&json!("system")));
        assert_eq!(
            messages[0].pointer("/content"),
            Some(&json!(AGENT_SYSTEM_PROMPT))
        );
        assert_eq!(messages[1].pointer("/role"), Some(&json!("user")));
        assert_eq!(
            messages[1].pointer("/content"),
            Some(&json!("where is my shop data?"))
        );
        assert_eq!(messages[2].pointer("/role"), Some(&json!("assistant")));
        assert_eq!(
            messages[2].pointer("/content"),
            Some(&json!("It is in silver.shop and gold.shop_dbt."))
        );
        assert_eq!(
            messages[3].pointer("/content"),
            Some(&json!("and how many customers?"))
        );
        assert_eq!(
            messages[4].pointer("/content"),
            Some(&json!("12 in silver.shop.customers."))
        );
        assert_eq!(messages[5].pointer("/role"), Some(&json!("user")));
        assert_eq!(messages[5].pointer("/content"), Some(&json!("and last month?")));
    }

    #[test]
    fn agent_messages_without_a_thread_keeps_the_previous_pair() {
        let messages = agent_messages(&[], Some(&previous_turn()), "and only org domains?");
        let messages = messages.as_array().expect("an array of messages");
        assert_eq!(messages.len(), 4, "system, the previous pair, the question");
        assert_eq!(messages[0].pointer("/role"), Some(&json!("system")));
        assert_eq!(
            messages[1].pointer("/content"),
            Some(&json!("can you tell me what I can do here?"))
        );
        assert_eq!(messages[2].pointer("/role"), Some(&json!("assistant")));
        assert_eq!(
            messages[2].pointer("/content"),
            Some(&json!(
                "SELECT id, display_name FROM bronze.public_db.rnacentral_databases"
            ))
        );
        assert_eq!(
            messages[3].pointer("/content"),
            Some(&json!("and only org domains?"))
        );
        // Neither a thread nor a previous turn: just the system prompt and the question.
        let bare = agent_messages(&[], None, "hello?");
        assert_eq!(bare.as_array().expect("an array of messages").len(), 2);
    }

    #[test]
    fn agent_messages_prefers_the_thread_over_previous() {
        let thread = vec![AskTurn {
            question: "where is my shop data?".into(),
            answer: "It is in silver.shop.".into(),
        }];
        let messages = agent_messages(&thread, Some(&previous_turn()), "and per month?");
        let messages = messages.as_array().expect("an array of messages");
        assert_eq!(messages.len(), 4, "system, the thread's pair, the question");
        assert!(
            !messages
                .iter()
                .any(|message| message.pointer("/content")
                    == Some(&json!(
                        "SELECT id, display_name FROM bronze.public_db.rnacentral_databases"
                    ))),
            "the previous statement is not sent when a thread is there: {messages:?}"
        );
        assert_eq!(
            messages[2].pointer("/content"),
            Some(&json!("It is in silver.shop."))
        );
    }

    #[test]
    fn thread_refusal_refuses_eleven_turns_and_an_empty_answer() {
        let turn = || AskTurn {
            question: "where is my shop data?".into(),
            answer: "It is in silver.shop.".into(),
        };
        assert_eq!(thread_refusal(&[]), None, "no thread is fine");
        assert_eq!(thread_refusal(&vec![turn(); MAX_THREAD_TURNS]), None);
        let refusal = thread_refusal(&vec![turn(); MAX_THREAD_TURNS + 1])
            .expect("eleven turns are refused");
        assert!(refusal.contains("10"), "{refusal}");
        let empty_answer = AskTurn {
            question: "and then?".into(),
            answer: "   ".into(),
        };
        let refusal = thread_refusal(&[empty_answer]).expect("an empty answer is refused");
        assert!(refusal.contains("answer"), "{refusal}");
        let empty_question = AskTurn {
            question: String::new(),
            answer: "It is in silver.shop.".into(),
        };
        assert!(thread_refusal(&[empty_question]).is_some(), "an empty question is refused");
        let long_answer = AskTurn {
            question: "and then?".into(),
            answer: "x".repeat(MAX_TURN_ANSWER_CHARS + 1),
        };
        assert!(thread_refusal(&[long_answer]).is_some(), "an 8001-character answer is refused");
    }

    #[test]
    fn a_follow_up_keeps_the_previous_tables_when_the_new_words_match_none() {
        let previous = previous_turn();
        // "rewrite the query to find only org domains" names no table, so the search finds
        // none and the handler would otherwise answer `NO_TABLE`.
        let candidates = follow_up_candidates(Some(&previous), Vec::new());
        assert_eq!(candidates, previous.tables);
        // The model receives that table in its context, with the refinement.
        let docs = vec![json!({"description": "Rfam RNA databases"})];
        let context = context_of(&candidates, &docs, &[], &[]);
        let prompt = user_text_of(
            "rewrite the query to find only org domains",
            Some(&previous),
            &context,
        );
        assert!(
            prompt.contains("bronze.public_db.rnacentral_databases"),
            "{prompt}"
        );
        assert!(prompt.contains("Rfam RNA databases"), "{prompt}");
        // A new match is kept after the previous turn's tables, once.
        let found = vec![
            table("public_db", "rnacentral_databases"),
            table("public_db", "other"),
        ];
        let merged = follow_up_candidates(Some(&previous), found);
        assert_eq!(
            merged,
            vec![
                table("public_db", "rnacentral_databases"),
                table("public_db", "other")
            ]
        );
        // A plain question uses the search's answer unchanged.
        assert_eq!(
            follow_up_candidates(None, vec![table("public_db", "other")]),
            vec![table("public_db", "other")]
        );
    }

    #[test]
    fn a_follow_up_drops_a_previous_table_the_caller_cannot_read() {
        let previous = previous_turn();
        // The docs loop above turns a 403 or 404 documentation read into `Value::Null`.
        let (candidates, docs) = drop_unreadable(previous.tables.clone(), vec![Value::Null]);
        assert!(
            candidates.is_empty(),
            "an unreadable table stays in the prompt"
        );
        assert!(docs.is_empty());
        // A readable one survives with its documentation.
        let (candidates, docs) = drop_unreadable(
            previous.tables.clone(),
            vec![json!({"description": "Rfam"})],
        );
        assert_eq!(candidates, previous.tables);
        assert_eq!(docs.len(), 1);
        // With no other candidate the answer is `NO_TABLE`, exactly as the empty search is.
        let body = no_table_answer("default".into(), "qwen3-4b".into(), vec!["default".into()]);
        assert_eq!(body.sentence, NO_TABLE);
        assert_eq!(body.sql, None);
        assert_eq!(body.explain, "not_run");
    }

    // --- every catalog is searched, and a name in full wins (stardelt/stardelt#2364) ----

    fn full(catalog: &str, namespace: &str, table: &str) -> AskTable {
        AskTable {
            catalog: catalog.into(),
            namespace: namespace.into(),
            table: table.into(),
        }
    }

    #[test]
    fn a_question_naming_a_table_in_full_yields_it() {
        assert_eq!(
            qualified_names("how to query that table silver.shop.stg_customers?"),
            vec![full("silver", "shop", "stg_customers")]
        );
        // Lower case first, then the trailing punctuation.
        assert_eq!(
            qualified_names("Silver.Shop.Stg_Customers."),
            vec![full("silver", "shop", "stg_customers")]
        );
    }

    #[test]
    fn two_part_names_and_urls_are_not_tables() {
        assert!(qualified_names("see shop.orders or https://x.io/a").is_empty());
        assert!(qualified_names("gold.shop").is_empty());
        assert!(qualified_names("a.b.c.d").is_empty());
    }

    #[test]
    fn named_tables_come_first_and_duplicates_go() {
        let a = full("silver", "shop", "stg_customers");
        let b = full("gold", "shop", "sales");
        let c = full("bronze", "shop", "raw_sales");
        assert_eq!(
            merge_candidates(vec![a.clone()], vec![b.clone(), a.clone(), c.clone()]),
            vec![a, b, c]
        );
    }

    #[test]
    fn merge_stops_at_five() {
        let named: Vec<AskTable> = (0..3)
            .map(|i| full("silver", "shop", &format!("named_{i}")))
            .collect();
        let searched: Vec<AskTable> = (0..4)
            .map(|i| full("gold", "shop", &format!("found_{i}")))
            .collect();
        let merged = merge_candidates(named, searched);
        assert_eq!(merged.len(), CANDIDATES);
        assert_eq!(merged[0].table, "named_0");
        assert_eq!(merged[4].table, "found_1");
    }

    #[test]
    fn a_follow_up_puts_the_previous_statement_in_the_prompt() {
        let previous = previous_turn();
        let prompt = user_text_of("only org domains", Some(&previous), &json!([]));
        assert!(prompt.contains("Its statement:"), "{prompt}");
        assert!(prompt.contains(&previous.sql), "{prompt}");
        assert!(
            prompt.contains("The previous question: can you tell me what I can do here?"),
            "{prompt}"
        );
        assert!(
            prompt.contains(
                "The question below refines it; change that statement unless the question asks for something new."
            ),
            "{prompt}"
        );
        // The question still comes after the previous turn.
        assert!(
            prompt.find("The question: only org domains") > prompt.find("Its statement:"),
            "{prompt}"
        );
        // A plain question carries none of the previous turn.
        let plain = user_text_of("how many rows?", None, &json!([]));
        assert!(!plain.contains("Its statement:"));
        assert!(plain.starts_with("The question: how many rows?"));
    }

    #[tokio::test]
    async fn a_previous_statement_over_8000_characters_is_refused() {
        let body = json!({
            "question": "only org domains",
            "previous": {
                "question": "can you tell me what I can do here?",
                "sql": format!("SELECT {}", "x".repeat(8000)),
                "tables": [{"catalog": "bronze", "namespace": "public_db", "table": "rnacentral_databases"}]
            }
        });
        let (status, body, cache) =
            ask_body_call(&app(crate::router_tests::test_state()), body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"],
            json!("the previous statement is longer than 8000 characters")
        );
        assert_eq!(cache.as_deref(), Some("no-store"));
        // At the limit it is accepted: the request goes on to the state store's 503.
        let at_limit = json!({
            "question": "only org domains",
            "previous": {
                "question": "can you tell me what I can do here?",
                "sql": "x".repeat(8000),
                "tables": []
            }
        });
        let (status, _, _) = ask_body_call(&app(crate::router_tests::test_state()), at_limit).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    // --- the feedback route (slice b) -------------------------------------------------

    fn feedback_app(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/api/ask/feedback", post(feedback))
            .with_state(state)
    }

    /// One `POST /api/ask/feedback`, its status and its body.
    async fn feedback_call(app: &Router, body: Value) -> (StatusCode, Value) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/ask/feedback")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn a_vote_that_is_neither_up_nor_down_is_a_400() {
        let (status, body) = feedback_call(
            &feedback_app(crate::router_tests::test_state()),
            json!({"callId": 1, "vote": "sideways"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], json!("a vote is up or down"));
    }

    #[tokio::test]
    async fn a_vote_without_a_state_store_is_a_503_and_does_not_panic() {
        let (status, body) = feedback_call(
            &feedback_app(crate::router_tests::test_state()),
            json!({"callId": 1, "vote": "up"}),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body["error"],
            json!("Ask needs Nova's database, and this installation has none")
        );
    }

    /// One caller, as the middleware would put it on the request.
    fn principal(sub: &str) -> crate::auth::Principal {
        crate::auth::Principal {
            identity: crate::auth::Identity {
                sub: sub.into(),
                display_name: sub.into(),
                email: None,
                name: None,
                groups: vec![],
                roles: vec![],
                groups_overage: false,
                is_admin: false,
            },
            via: "session",
            access_token: None,
            client_id: None,
        }
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn a_vote_lands_on_the_callers_own_row_and_another_persons_id_is_a_404() {
        use crate::db::test_support::migrated_pool;

        let pool = migrated_pool().await;
        let call = |sub: &'static str| crate::model_calls::Call {
            sub,
            group: None,
            feature: "ask",
            endpoint: "default",
            model: "qwen3-4b",
            tokens: crate::model_calls::Tokens {
                input: 10,
                output: 5,
                estimated: false,
            },
            latency_ms: 120,
            outcome: "ok",
            prompt: "how many rows?",
            answer: Some("{\"sql\": \"SELECT 1\", \"sentence\": \"One.\"}"),
        };
        // The id `/api/ask` would have answered as `callId`.
        let id = crate::model_calls::record(&pool, &call("alice"))
            .await
            .unwrap();
        let feedback_of = |id: i64| {
            sqlx::query_scalar::<_, Option<String>>("SELECT feedback FROM model_call WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
        };

        let state = crate::router_tests::test_state_with_db(Some(pool.clone()));
        let vote = |call_id: i64| json!({"callId": call_id, "vote": "down"});

        // Somebody else's id: a 404, and nothing is written.
        let response = feedback(
            State(Arc::clone(&state)),
            Some(axum::Extension(principal("bob"))),
            Json(serde_json::from_value(vote(id)).unwrap()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(feedback_of(id).await.unwrap(), None);

        // The caller's own id: a 204, and the row carries the vote.
        let response = feedback(
            State(Arc::clone(&state)),
            Some(axum::Extension(principal("alice"))),
            Json(serde_json::from_value(vote(id)).unwrap()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(feedback_of(id).await.unwrap().as_deref(), Some("down"));
    }

    // --- the history route (slice c) --------------------------------------------------

    fn history_app(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/api/ask/history", axum::routing::get(history))
            .with_state(state)
    }

    /// One `GET /api/ask/history`, its status, its body and its cache control.
    async fn history_call(app: &Router) -> (StatusCode, Value, Option<String>) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/ask/history")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let cache = response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
            cache,
        )
    }

    #[tokio::test]
    async fn a_history_without_a_state_store_is_a_503() {
        let (status, body, cache) =
            history_call(&history_app(crate::router_tests::test_state())).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            body["error"],
            json!("Ask needs Nova's database, and this installation has none")
        );
        assert_eq!(cache.as_deref(), Some("no-store"));
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn the_history_keeps_the_last_20_of_ones_own_conversations() {
        use crate::db::test_support::migrated_pool;

        let pool = migrated_pool().await;
        sqlx::query("DELETE FROM ask_conversation")
            .execute(&pool)
            .await
            .unwrap();
        let answer_of = |question: &str| AskAnswer {
            endpoint: "default".into(),
            model: "qwen3-4b".into(),
            tried: vec!["default".into()],
            call_id: None,
            sql: Some("SELECT 1".into()),
            sentence: format!("the answer to {question}"),
            tables: Vec::new(),
            explain: "passed".into(),
            error: None,
            mode: "sql".into(),
            text: None,
            steps: Vec::new(),
        };
        // Twenty-one of alice's, then one of bob's.
        for number in 0..21 {
            let question = format!("alice question {number}");
            record_conversation(&pool, "alice", &question, &answer_of(&question)).await;
        }
        let bobs = "bob question 0";
        record_conversation(&pool, "bob", bobs, &answer_of(bobs)).await;

        let state = crate::router_tests::test_state_with_db(Some(pool.clone()));
        let as_person = |sub: &str| -> Router {
            Router::new()
                .route("/api/ask/history", axum::routing::get(history))
                .with_state(Arc::clone(&state))
                .layer(axum::Extension(principal(sub)))
        };

        // The retention is the table's, not the read's: alice keeps 20 rows.
        let stored = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM ask_conversation WHERE sub = 'alice'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored, 20);

        // Alice keeps exactly 20, newest first, and none of bob's.
        let (status, body, cache) = history_call(&as_person("alice")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cache.as_deref(), Some("no-store"));
        let items = body.as_array().expect("a list of conversations");
        assert_eq!(items.len(), 20);
        assert_eq!(items[0]["question"], json!("alice question 20"));
        assert!(
            !body.to_string().contains(bobs),
            "bob's question is in alice's history: {body}"
        );

        // Bob sees his one, and none of alice's.
        let (status, body, _) = history_call(&as_person("bob")).await;
        assert_eq!(status, StatusCode::OK);
        let items = body.as_array().expect("a list of conversations");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["question"], json!(bobs));
    }

    // --- pinning and deleting (maintainer, 2026-11-28) --------------------------------

    /// One answer as `record_conversation` stores it.
    fn conversation_answer(question: &str) -> AskAnswer {
        AskAnswer {
            endpoint: "default".into(),
            model: "qwen3-4b".into(),
            tried: vec!["default".into()],
            call_id: None,
            sql: Some("SELECT 1".into()),
            sentence: format!("the answer to {question}"),
            tables: Vec::new(),
            explain: "passed".into(),
            error: None,
            mode: "sql".into(),
            text: None,
            steps: Vec::new(),
        }
    }

    /// The three history routes as one person sees them.
    fn conversation_app(state: Arc<AppState>, sub: &str) -> Router {
        Router::new()
            .route("/api/ask/history", axum::routing::get(history))
            .route("/api/ask/history/{id}/pin", axum::routing::put(history_pin))
            .route(
                "/api/ask/history/{id}",
                axum::routing::delete(history_delete),
            )
            .with_state(state)
            .layer(axum::Extension(principal(sub)))
    }

    /// One `PUT …/pin` (with a body) or `DELETE …` (without), its status and its body.
    async fn conversation_call(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder().method(method).uri(uri);
        let request = match body {
            Some(body) => request
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
            None => request.body(Body::empty()).unwrap(),
        };
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    /// One stored conversation's id.
    async fn conversation_id(pool: &sqlx::PgPool, sub: &str, question: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT id FROM ask_conversation WHERE sub = $1 AND question = $2",
        )
        .bind(sub)
        .bind(question)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn a_pinned_conversation_survives_the_prune() {
        use crate::db::test_support::migrated_pool;

        let pool = migrated_pool().await;
        sqlx::query("DELETE FROM ask_conversation")
            .execute(&pool)
            .await
            .unwrap();
        let kept = "alice question to keep";
        record_conversation(&pool, "alice", kept, &conversation_answer(kept)).await;
        let id = conversation_id(&pool, "alice", kept).await;

        let state = crate::router_tests::test_state_with_db(Some(pool.clone()));
        let app = conversation_app(Arc::clone(&state), "alice");
        let (status, body) = conversation_call(
            &app,
            "PUT",
            &format!("/api/ask/history/{id}/pin"),
            Some(json!({"pinned": true})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["pinned"], json!(true));
        assert_eq!(body["question"], json!(kept));

        // Twenty-one more answers: the prune keeps the newest 20 unpinned, and never the pinned one.
        for number in 0..21 {
            let question = format!("alice question {number}");
            record_conversation(&pool, "alice", &question, &conversation_answer(&question)).await;
        }

        let (status, body, _) = history_call(&app).await;
        assert_eq!(status, StatusCode::OK);
        let items = body.as_array().expect("a list of conversations");
        assert_eq!(items.len(), 21, "20 unpinned and the pinned one: {body}");
        assert_eq!(items[0]["question"], json!(kept), "pinned first: {body}");
        assert_eq!(items[0]["pinned"], json!(true));
        assert_eq!(items[1]["question"], json!("alice question 20"));
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn another_persons_conversation_is_not_found() {
        use crate::db::test_support::migrated_pool;

        let pool = migrated_pool().await;
        sqlx::query("DELETE FROM ask_conversation")
            .execute(&pool)
            .await
            .unwrap();
        let question = "alice question 0";
        record_conversation(&pool, "alice", question, &conversation_answer(question)).await;
        let id = conversation_id(&pool, "alice", question).await;

        let state = crate::router_tests::test_state_with_db(Some(pool.clone()));
        let bob = conversation_app(Arc::clone(&state), "bob");
        let (status, body) = conversation_call(
            &bob,
            "PUT",
            &format!("/api/ask/history/{id}/pin"),
            Some(json!({"pinned": true})),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], json!("no such conversation"));
        let (status, body) =
            conversation_call(&bob, "DELETE", &format!("/api/ask/history/{id}"), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], json!("no such conversation"));

        // Bob touched nothing of alice's, and an id that does not exist is the same 404.
        let pinned = sqlx::query_scalar::<_, bool>(
            "SELECT pinned FROM ask_conversation WHERE sub = 'alice' AND id = $1",
        )
        .bind(id)
        .fetch_optional(&pool)
        .await
        .unwrap();
        assert_eq!(
            pinned,
            Some(false),
            "bob's pin or delete touched alice's row"
        );
        let alice = conversation_app(Arc::clone(&state), "alice");
        let (status, _) =
            conversation_call(&alice, "DELETE", "/api/ask/history/999999", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn the_twenty_first_pin_is_refused() {
        use crate::db::test_support::migrated_pool;

        let pool = migrated_pool().await;
        sqlx::query("DELETE FROM ask_conversation")
            .execute(&pool)
            .await
            .unwrap();
        for number in 0..20 {
            let question = format!("alice question {number}");
            record_conversation(&pool, "alice", &question, &conversation_answer(&question)).await;
        }

        let state = crate::router_tests::test_state_with_db(Some(pool.clone()));
        let app = conversation_app(Arc::clone(&state), "alice");
        for number in 0..20 {
            let id = conversation_id(&pool, "alice", &format!("alice question {number}")).await;
            let (status, _) = conversation_call(
                &app,
                "PUT",
                &format!("/api/ask/history/{id}/pin"),
                Some(json!({"pinned": true})),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "pin {number}");
        }

        // A 21st unpinned conversation stays (20 is the prune's limit for unpinned rows) and
        // cannot be pinned.
        let extra = "alice question extra";
        record_conversation(&pool, "alice", extra, &conversation_answer(extra)).await;
        let id = conversation_id(&pool, "alice", extra).await;
        let (status, body) = conversation_call(
            &app,
            "PUT",
            &format!("/api/ask/history/{id}/pin"),
            Some(json!({"pinned": true})),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(
            body["error"],
            json!("at most 20 conversations can be pinned; unpin one first")
        );

        // Unpinning one makes room.
        let first = conversation_id(&pool, "alice", "alice question 0").await;
        let (status, _) = conversation_call(
            &app,
            "PUT",
            &format!("/api/ask/history/{first}/pin"),
            Some(json!({"pinned": false})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = conversation_call(
            &app,
            "PUT",
            &format!("/api/ask/history/{id}/pin"),
            Some(json!({"pinned": true})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[test]
    fn a_question_of_100_characters_uses_the_every_word_search() {
        assert!(every_word_search(&"a".repeat(100)));
    }

    #[test]
    fn a_longer_question_skips_it() {
        assert!(!every_word_search(&"a".repeat(101)));
    }

    #[test]
    fn characters_not_bytes_count() {
        assert!(every_word_search(&"ä".repeat(100)));
    }
}
