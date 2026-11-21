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
//!   five candidate tables (description, tags, owner, domain, columns). Never a row, never
//!   a result, whatever `spec.sendRows` says.
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

/// The longest question Ask takes.
const MAX_QUESTION_CHARS: usize = 2_000;
/// How many tables the model may choose from.
const CANDIDATES: usize = 5;
/// How long the `EXPLAIN` check of one statement may take.
const ASK_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// The sentence a person reads when nothing matches their question.
const NO_TABLE: &str =
    "I found no table you can see that matches. Try the table's name, or browse the Catalog.";
/// The one sentence that says why Ask cannot answer: no endpoint, or no usable endpoint.
const NO_ENDPOINT: &str =
    "no ask model is ready: connect a ModelEndpoint with purpose ask, and wait for its probe";
const SYSTEM_PROMPT: &str = "You write one Trino SQL statement for a question about the tables you are given. Use only these tables, fully qualified as catalog.namespace.table, and only the columns their documentation lists; never invent a table or a column. You receive metadata only, never rows. Answer with one JSON object with exactly two fields: \"sql\" — one SELECT or WITH statement, no trailing semicolon — and \"sentence\" — one sentence of at most 200 characters saying what the statement answers. Never write DDL or DML, and never write EXPLAIN.";

/// `POST /api/ask` body.
#[derive(Deserialize, utoipa::ToSchema)]
pub(crate) struct AskRequest {
    /// The question, in words, 1 to 2000 characters.
    pub question: String,
}

/// One table the model was given.
#[derive(Serialize, utoipa::ToSchema)]
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
}

/// The first endpoint (name order) that may answer Ask: purpose `ask`, the probe found it
/// reachable, and it answered in a JSON shape. `None` means the feature is off.
fn ready_ask_endpoint(objects: &[Value]) -> Option<(String, Value)> {
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
    ready.into_iter().next()
}

/// The context the model receives: one entry per candidate table, metadata only. A field
/// the documentation does not have is `null`.
fn context_of(candidates: &[AskTable], docs: &[Value]) -> Value {
    let field = |doc: &Value, name: &str| doc.get(name).cloned().unwrap_or(Value::Null);
    let entries: Vec<Value> = candidates
        .iter()
        .enumerate()
        .map(|(index, table)| {
            let doc = docs.get(index).unwrap_or(&Value::Null);
            json!({
                "name": format!("{}.{}.{}", table.catalog, table.namespace, table.table),
                "description": field(doc, "description"),
                "tags": field(doc, "tags"),
                "owner": field(doc, "owner"),
                "domain": field(doc, "domain"),
                "columns": field(doc, "columns"),
            })
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
    no_store((status, Json(json!({ "error": message.into() }))).into_response())
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
        "DELETE FROM ask_conversation WHERE sub = $1 AND id NOT IN ( \
           SELECT id FROM ask_conversation WHERE sub = $1 \
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
fn parse_answer(content: &str) -> Option<(String, String)> {
    let value: Value = serde_json::from_str(content.trim()).ok()?;
    let sql = value.get("sql")?.as_str()?.trim().to_string();
    let sentence = value.get("sentence")?.as_str()?.trim().to_string();
    (!sql.is_empty()).then_some((sql, sentence))
}

/// One parsed answer, with the text it came from (the retry shows it to the model).
struct First {
    sql: String,
    sentence: String,
    content: String,
    /// The `model_call` row this answer wrote, when the store took it.
    call_id: Option<i64>,
}

/// Everything a chat call needs that does not change between the first call and the retry.
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
    async fn call(&self, messages: &Value) -> Result<First, Response> {
        if let Err(refusal) = crate::model_calls::check(
            self.pool,
            &crate::model_calls::quota_of(self.object),
            self.group,
            self.sub,
        )
        .await
        {
            return Err(crate::model_calls::refuse(&refusal));
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
                let (outcome, sentence) = match &error {
                    ChatError::Refused(sentence) => ("refused", sentence.clone()),
                    ChatError::Transport(sentence) => ("error", sentence.clone()),
                };
                let _ = self
                    .record(outcome, None, None, started.elapsed().as_millis() as u64)
                    .await;
                return Err(fail(
                    StatusCode::BAD_GATEWAY,
                    format!("the model endpoint did not answer: {sentence}"),
                ));
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
            None => Err(fail(
                StatusCode::BAD_GATEWAY,
                "the model did not answer with SQL. Try again, or ask differently.",
            )),
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

/// `POST /api/ask` — a question in, checked SQL out.
#[utoipa::path(
    post,
    operation_id = "ask_answer",
    path = "/api/ask",
    tag = "public",
    description = "A question in words, one Trino statement out. Nova searches the catalog as the caller for at most five tables, sends the question and **the documentation of those tables only** — description, tags, owner, domain, column names and their descriptions — to the first `ModelEndpoint` (name order) whose purpose list contains `ask` and whose probe found it reachable with JSON output, and answers the statement the model writes. No row, no query result and no table data ever leaves the platform, whatever the endpoint's `sendRows` says; never the endpoint's URL or its key either. The statement is **checked with `EXPLAIN` as the caller and is not run**: `explain` is `passed` or `failed`, and a failed check keeps the SQL in the answer with the engine's sentence in `error` (one correction is asked of the model first). No matching table is a `200` with `sql: null` and a sentence saying so; a model endpoint that is refused, unreachable or does not answer in the JSON shape is a `502`. A spent budget is the `429` the model budget answers (see `/api/admin/model-usage`). Every answer carries `Cache-Control: no-store`.",
    request_body = AskRequest,
    responses(
        (status = 200, description = "The statement, its sentence, the tables it was written for and the `EXPLAIN` result", body = AskAnswer),
        (status = 400, description = "The question is empty or longer than 2000 characters", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 429, description = "The person's or their group's model budget is spent", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog, the cluster or the model endpoint did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "No state store, no cluster connection, no HTTP client, or no ready `ask` endpoint", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn ask(
    State(state): State<Arc<AppState>>,
    caller: Option<axum::Extension<crate::auth::Principal>>,
    Json(request): Json<AskRequest>,
) -> Response {
    let question = request.question.trim().to_string();
    if question.is_empty() || question.chars().count() > MAX_QUESTION_CHARS {
        return fail(
            StatusCode::BAD_REQUEST,
            "a question is required: 1 to 2000 characters",
        );
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
    let Some((name, object)) = ready_ask_endpoint(&objects) else {
        return fail(StatusCode::SERVICE_UNAVAILABLE, NO_ENDPOINT);
    };
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
        return fail(StatusCode::SERVICE_UNAVAILABLE, NO_ENDPOINT);
    }
    let key = crate::model_chat::key_for(&name);
    // The same group is checked against the quota and written to the row, so spend and
    // limit agree.
    let sub = match caller.as_ref() {
        Some(caller) => caller.0.identity.sub.clone(),
        None => state.dev_user.clone(),
    };
    let group = caller
        .as_ref()
        .and_then(|caller| caller.0.identity.groups.first().cloned());

    let response = crate::catalog_search::search(
        State(Arc::clone(&state)),
        Query(crate::catalog_search::SearchParams {
            q: Some(question.clone()),
            catalog: None,
            limit: Some(CANDIDATES),
            tag: None,
            owner: None,
            domain: None,
        }),
        caller.clone(),
    )
    .await;
    let (status, found) = body_of(response).await;
    if status != StatusCode::OK {
        let sentence = found
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("the catalog did not answer")
            .to_string();
        return fail(StatusCode::BAD_GATEWAY, sentence);
    }
    let candidates: Vec<AskTable> = found
        .get("matches")
        .and_then(Value::as_array)
        .map(|matches| matches.iter().filter_map(ask_table).collect())
        .unwrap_or_default();
    if candidates.is_empty() {
        let body = AskAnswer {
            endpoint: name,
            model,
            call_id: None,
            sql: None,
            sentence: NO_TABLE.to_string(),
            tables: Vec::new(),
            explain: "not_run".to_string(),
            error: None,
        };
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

    let context = context_of(&candidates, &docs);
    let user_text = format!(
        "The question: {question}\n\nThe tables you may use, with their documentation:\n{context}"
    );
    let messages = json!([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_text},
    ]);

    let Some(client) = crate::model_probe::http_client() else {
        return fail(StatusCode::SERVICE_UNAVAILABLE, NO_ENDPOINT);
    };
    let attempt = Attempt {
        client: &client,
        pool,
        object: &object,
        name: &name,
        model: &model,
        url: &url,
        key: key.as_deref(),
        sub: &sub,
        group: group.as_deref(),
        prompt: &user_text,
    };
    let principal = caller.as_ref().map(|caller| &caller.0);
    let first = match attempt.call(&messages).await {
        Ok(first) => first,
        Err(response) => return response,
    };
    let Some(error) = check_statement(&state, principal, &first.sql).await.err() else {
        let body = AskAnswer {
            endpoint: name,
            model,
            call_id: first.call_id,
            sql: Some(first.sql),
            sentence: first.sentence,
            tables: candidates,
            explain: "passed".to_string(),
            error: None,
        };
        record_conversation(pool, &sub, &question, &body).await;
        return answer(body);
    };

    // One correction: the model sees its own statement and why it does not run.
    let correction = format!(
        "That statement is not runnable: {error}. Answer with the corrected statement in the same JSON shape."
    );
    let retry = json!([
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_text},
        {"role": "assistant", "content": first.content},
        {"role": "user", "content": correction},
    ]);
    let second = match attempt.call(&retry).await {
        Ok(second) => second,
        Err(response) => return response,
    };
    let body = match check_statement(&state, principal, &second.sql).await {
        Ok(()) => AskAnswer {
            endpoint: name,
            model,
            call_id: second.call_id,
            sql: Some(second.sql),
            sentence: second.sentence,
            tables: candidates,
            explain: "passed".to_string(),
            error: None,
        },
        Err(second_error) => AskAnswer {
            endpoint: name,
            model,
            call_id: second.call_id,
            sql: Some(second.sql),
            sentence: second.sentence,
            tables: candidates,
            explain: "failed".to_string(),
            error: Some(second_error),
        },
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
        "SELECT id, question, endpoint, model, call_id, sql, sentence, tables, explain, error, \
                to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at \
         FROM ask_conversation WHERE sub = $1 \
         ORDER BY ask_conversation.created_at DESC, id DESC LIMIT 20",
    )
    .bind(&sub)
    .fetch_all(pool)
    .await;
    match rows {
        Ok(rows) => {
            let items: Vec<AskConversation> = rows
                .into_iter()
                .map(|row| AskConversation {
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
                })
                .collect();
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
    fn ready_picks_the_first_ask_endpoint_that_is_reachable_with_json() {
        let objects = vec![
            endpoint("agents-only", &["agents"], json!(true), json!(true)),
            endpoint("no-json", &["ask"], json!(true), Value::Null),
            endpoint("unreachable", &["ask"], json!(false), json!(true)),
            endpoint("works", &["ask", "agents"], json!(true), json!(true)),
        ];
        let (name, object) = ready_ask_endpoint(&objects).expect("one ready endpoint");
        assert_eq!(name, "works");
        assert_eq!(object.pointer("/spec/model"), Some(&json!("qwen3-4b")));
        // Name order decides between two ready endpoints.
        let two = vec![
            endpoint("zulu", &["ask"], json!(true), json!(true)),
            endpoint("alpha", &["ask"], json!(true), json!(true)),
        ];
        assert_eq!(
            ready_ask_endpoint(&two).map(|(name, _)| name).as_deref(),
            Some("alpha")
        );
        assert!(ready_ask_endpoint(&[]).is_none());
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
                "columns": [
                    {"name": "id", "type": "bigint", "description": "the trip"},
                    {"name": "zone", "type": "varchar", "description": null}
                ],
                "canEdit": false
            }),
            Value::Null,
        ];
        let context = context_of(&candidates, &docs);
        assert_eq!(context[0]["name"], json!("warehouse.bronze.trips"));
        assert_eq!(context[0]["description"], json!("One row per trip"));
        assert_eq!(context[0]["tags"], json!(["raw", "gold"]));
        assert_eq!(context[0]["owner"], json!("analytics"));
        assert_eq!(context[0]["domain"], json!("mobility"));
        assert_eq!(context[0]["columns"].as_array().map(Vec::len), Some(2));
        assert_eq!(context[0]["columns"][1]["description"], Value::Null);
        // An unreadable documentation is the table with `null` fields, not an error.
        assert_eq!(context[1]["name"], json!("warehouse.bronze.zones"));
        assert_eq!(context[1]["description"], Value::Null);
        assert_eq!(context[1]["columns"], Value::Null);
    }

    #[test]
    fn an_answer_serialises_in_camel_case_with_null_sql() {
        let answer = AskAnswer {
            endpoint: "default".into(),
            model: "qwen3-4b".into(),
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
            call_id: None,
            sql: Some("SELECT 1".into()),
            sentence: "One.".into(),
            tables: vec![],
            explain: "passed".into(),
            error: None,
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

    fn app(state: Arc<AppState>) -> Router {
        Router::new().route("/api/ask", post(ask)).with_state(state)
    }

    /// A request to `POST /api/ask`, its status, its body and its `Cache-Control`.
    async fn ask_call(app: &Router, question: &str) -> (StatusCode, Value, Option<String>) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/ask")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "question": question }).to_string()))
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
            call_id: None,
            sql: Some("SELECT 1".into()),
            sentence: format!("the answer to {question}"),
            tables: Vec::new(),
            explain: "passed".into(),
            error: None,
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
}
