//! Verified queries (design nova#911 slice 2a): a question about a table, the SQL that
//! answers it, who verified it and when.
//!
//! The table's owners keep them next to the table's guide (`catalog_docs.rs`): an
//! administrator or the owner group adds and removes one, and everyone who may see the
//! table reads them. For a person a verified query is the trusted way to compute a number;
//! for an agent it is an example to copy instead of guessing SQL.
//!
//! A query may carry an **expected result** and be **checked** (design nova#912 slice 1): a
//! question with a known answer is a test of the data and of its definitions. `POST
//! /api/verified-queries/{id}/check` runs the statement as the caller and compares the rows
//! with the expected ones; the last check is stored on the row. Whoever may see the table may
//! check it.
//!
//! Only one read-only statement is accepted, checked with `mcp::classify` — the same
//! classifier that guards `sql_read` and Ask. The statement runs only from the check.
//!
//! Without a state store (`NOVA_DATABASE_URL` unset) the routes answer
//! `503 {"error":"this needs Nova's database, …"}`. The list asks the table's
//! documentation first, so a table the caller may not see answers what the documentation
//! route answers; the two writes check the request itself (400), then the store (503),
//! then the table's rule (403) — so a bad body is a 400 and a statement that is not a read
//! never needs a database, and an installation that cannot store a query says so instead
//! of calling the catalog.
//!
//! Every write writes one line on the `audit` target with the caller, the table and the
//! row's id — never the SQL text.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::AppState;
use crate::api_json::ApiJson;
use crate::auth::Principal;
use crate::catalog::CatalogParam;
use crate::saved_queries::{error, not_found, store_failed, unavailable};
use crate::trino::QueryResult;

/// Most characters a question may carry (the table's CHECK allows the same).
pub const MAX_QUESTION_CHARS: usize = 500;
/// Most characters the SQL may carry.
pub const MAX_SQL_CHARS: usize = 20_000;
/// Most rows an expected result may carry.
pub const MAX_EXPECTED_ROWS: usize = 100;
/// Most characters a difference message may carry.
const DIFF_CHARS: usize = 300;
/// Rows one list answers, newest first.
const LIST_LIMIT: usize = 50;
/// Rows one bulk check runs, in id order.
const CHECK_ALL_LIMIT: usize = 50;
/// The answer when a statement runs and no expected result is stored.
pub const RAN_WITHOUT_EXPECTED: &str = "ran, no expected result to compare";
/// The answer when the rows equal the expected result.
pub const RESULT_MATCHES: &str = "the result matches the expected result";
/// The answer when the engine cut the result before the end.
const TOO_LARGE: &str = "the result is larger than 100 rows";
/// The answer when a create body carries something that is not a list of rows.
const BAD_EXPECTED: &str = "expected must be a list of at most 100 rows";

/// A row as the API returns it. `verified_at` and `last_check_at` are RFC 3339 in UTC.
#[derive(Debug, Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct VerifiedQuery {
    pub id: i64,
    /// The question a person asks, in words.
    pub question: String,
    /// The one read-only statement that answers it.
    pub sql: String,
    /// The `sub` of the person who verified it.
    #[serde(rename = "verifiedBy")]
    #[schema(rename = "verifiedBy")]
    pub verified_by: String,
    /// When it was verified, RFC 3339.
    #[serde(rename = "verifiedAt")]
    #[schema(rename = "verifiedAt")]
    pub verified_at: String,
    /// The expected result: one array per row, each holding the row's values, or `null`.
    #[schema(value_type = Option<Vec<Vec<Object>>>)]
    pub expected: Option<serde_json::Value>,
    /// When the query was last checked, RFC 3339, or `null` when it never was.
    #[serde(rename = "lastCheckAt")]
    #[schema(rename = "lastCheckAt")]
    pub last_check_at: Option<String>,
    /// Whether the last check passed, or `null` when it never ran.
    #[serde(rename = "lastCheckPassed")]
    #[schema(rename = "lastCheckPassed")]
    pub last_check_passed: Option<bool>,
    /// What the last check found, in one sentence, or `null` when it never ran.
    #[serde(rename = "lastCheckMessage")]
    #[schema(rename = "lastCheckMessage")]
    pub last_check_message: Option<String>,
}

/// `POST /api/catalog/tables/{ns}/{tbl}/verified-queries` body.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct NewVerifiedQuery {
    /// The question, at most 500 characters.
    pub question: String,
    /// One read-only statement, at most 20 000 characters.
    pub sql: String,
    /// The known answer: up to 100 rows, each an array of values, or absent.
    #[serde(default)]
    #[schema(value_type = Option<Vec<Vec<Object>>>)]
    pub expected: Option<serde_json::Value>,
}

/// `POST /api/catalog/tables/{ns}/{tbl}/verified-queries/check` answer: every query of one
/// table checked, with the totals.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct CheckAllAnswer {
    /// How many queries were checked.
    pub checked: usize,
    /// How many of them passed.
    pub passed: usize,
    /// How many of them failed.
    pub failed: usize,
    /// The rows as the checks left them, in id order.
    pub items: Vec<VerifiedQuery>,
}

/// The totals of one bulk check: how many rows, how many pass, how many fail.
fn totals(items: &[VerifiedQuery]) -> (usize, usize, usize) {
    let passed = items
        .iter()
        .filter(|item| item.last_check_passed == Some(true))
        .count();
    let failed = items
        .iter()
        .filter(|item| item.last_check_passed == Some(false))
        .count();
    (items.len(), passed, failed)
}

/// The row a delete or a check needs before it can ask the table's rule: where it lives and
/// what it runs.
#[derive(sqlx::FromRow)]
struct TableRef {
    catalog: String,
    namespace: String,
    table_name: String,
}

/// What a check runs and compares against.
#[derive(sqlx::FromRow)]
struct CheckTarget {
    catalog: String,
    namespace: String,
    table_name: String,
    sql: String,
    expected: Option<serde_json::Value>,
}

/// The columns [`VerifiedQuery`] reads; the timestamps as RFC 3339 in UTC.
const ROW_COLUMNS: &str = "id, question, sql, verified_by, expected, \
    to_char(verified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS verified_at, \
    CASE WHEN last_check_at IS NULL THEN NULL ELSE \
        to_char(last_check_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') END \
        AS last_check_at, \
    last_check_passed, last_check_message";

/// An expected result is a JSON array of rows, each an array of values, at most 100.
fn valid_expected(value: &serde_json::Value) -> bool {
    match value.as_array() {
        Some(rows) => {
            rows.len() <= MAX_EXPECTED_ROWS && rows.iter().all(|row| row.as_array().is_some())
        }
        None => false,
    }
}

/// Trim the text fields and enforce the same limits the table's CHECK constraints do, so
/// a caller gets a sentence instead of a database error.
fn validate(req: NewVerifiedQuery) -> Result<NewVerifiedQuery, String> {
    let question = req.question.trim().to_string();
    if question.is_empty() {
        return Err("the question must not be empty".into());
    }
    if question.chars().count() > MAX_QUESTION_CHARS {
        return Err(format!(
            "the question must be at most {MAX_QUESTION_CHARS} characters"
        ));
    }
    let sql = req.sql.trim().to_string();
    if sql.is_empty() {
        return Err("sql must not be empty".into());
    }
    if sql.chars().count() > MAX_SQL_CHARS {
        return Err(format!("sql must be at most {MAX_SQL_CHARS} characters"));
    }
    let expected = match req.expected {
        None | Some(serde_json::Value::Null) => None,
        Some(value) if valid_expected(&value) => Some(value),
        Some(_) => return Err(BAD_EXPECTED.into()),
    };
    Ok(NewVerifiedQuery {
        question,
        sql,
        expected,
    })
}

/// Delete the rows of tables the catalog no longer lists. `present` is the complete
/// `(namespace, table_name)` list of one successful scan; a row for anything else in
/// this catalog is stale. Best effort: a scan must not fail because the cleanup did.
pub async fn prune_missing(
    pool: Option<&PgPool>,
    catalog: &str,
    present: &[(String, String)],
) -> u64 {
    let Some(pool) = pool else { return 0 };
    let namespaces: Vec<String> = present.iter().map(|(ns, _)| ns.clone()).collect();
    let tables: Vec<String> = present.iter().map(|(_, table)| table.clone()).collect();
    let result = sqlx::query(
        "DELETE FROM verified_query v \
         WHERE v.catalog = $1 \
           AND NOT EXISTS ( \
             SELECT 1 FROM unnest($2::text[], $3::text[]) AS t(ns, tbl) \
             WHERE t.ns = v.namespace AND t.tbl = v.table_name \
           )",
    )
    .bind(catalog)
    .bind(&namespaces)
    .bind(&tables)
    .execute(pool)
    .await;
    match result {
        Ok(done) => done.rows_affected(),
        Err(e) => {
            tracing::warn!("verified queries not pruned for {catalog}: {e}");
            0
        }
    }
}

/// `GET /api/catalog/tables/{ns}/{tbl}/verified-queries` — the table's, newest first.
#[utoipa::path(
    get,
    operation_id = "verified_queries_list",
    path = "/api/catalog/tables/{ns}/{tbl}/verified-queries",
    tag = "public",
    description = "The verified queries of one table: questions its owners answered with one read-only statement, who verified each and when, its expected result and the last check's outcome. Newest first, at most 50. A table the caller may not see answers what the documentation route answers. `?catalog=` picks the catalog.",
    params(
        ("ns" = String, Path, description = "Namespace (schema) name"),
        ("tbl" = String, Path, description = "Table name"),
        crate::catalog::CatalogParam,
    ),
    responses(
        (status = 200, description = "The table's verified queries, newest first", body = Vec<VerifiedQuery>),
        (status = 400, description = "A name is not a single path segment, or the catalog name is invalid", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog or no such table", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn list(
    State(s): State<Arc<AppState>>,
    Path((ns, tbl)): Path<(String, String)>,
    Query(q): Query<CatalogParam>,
    caller: Option<Extension<Principal>>,
) -> Response {
    if s.demo {
        return crate::catalog_docs::demo_unavailable();
    }
    let caller = caller.as_ref().map(|c| &c.0);
    let (name, _doc) =
        match crate::catalog_docs::documentation_for(&s, caller, q.catalog.as_deref(), &ns, &tbl)
            .await
        {
            Ok(found) => found,
            Err(response) => return response,
        };
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let sql = format!(
        "SELECT {ROW_COLUMNS} FROM verified_query \
         WHERE catalog = $1 AND namespace = $2 AND table_name = $3 \
         ORDER BY verified_at DESC LIMIT {LIST_LIMIT}"
    );
    match sqlx::query_as::<_, VerifiedQuery>(sqlx::AssertSqlSafe(sql))
        .bind(name)
        .bind(ns)
        .bind(tbl)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => store_failed(e),
    }
}

/// `POST /api/catalog/tables/{ns}/{tbl}/verified-queries` — `201` with the stored row.
#[utoipa::path(
    post,
    operation_id = "verified_queries_create",
    path = "/api/catalog/tables/{ns}/{tbl}/verified-queries",
    tag = "public",
    description = "Verify one answer to a question about this table: the question (at most 500 characters), one read-only statement (at most 20 000) and, optionally, the known answer as up to 100 rows of values. Only the owner group of the table and administrators may add one; the caller is recorded as `verifiedBy`. The statement is checked on create and run only by `POST /api/verified-queries/{id}/check`. `?catalog=` picks the catalog.",
    params(
        ("ns" = String, Path, description = "Namespace (schema) name"),
        ("tbl" = String, Path, description = "Table name"),
        crate::catalog::CatalogParam,
    ),
    request_body = NewVerifiedQuery,
    responses(
        (status = 201, description = "The stored row", body = VerifiedQuery),
        (status = 400, description = "The question or the SQL is empty or too long, `expected` is not a list of at most 100 rows, the catalog name is invalid, or the statement is not one read-only statement", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog, or only the owner group and administrators may verify a query", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog or no such table", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn create(
    State(s): State<Arc<AppState>>,
    Path((ns, tbl)): Path<(String, String)>,
    Query(q): Query<CatalogParam>,
    caller: Option<Extension<Principal>>,
    ApiJson(req): ApiJson<NewVerifiedQuery>,
) -> Response {
    let new = match validate(req) {
        Ok(new) => new,
        Err(message) => return error(StatusCode::BAD_REQUEST, &message),
    };
    let statement = match crate::mcp::classify(&new.sql, false) {
        Ok(statement) => statement,
        Err(message) => return error(StatusCode::BAD_REQUEST, &message),
    };
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let caller = caller.as_ref().map(|c| &c.0);
    let (name, doc) =
        match crate::catalog_docs::documentation_for(&s, caller, q.catalog.as_deref(), &ns, &tbl)
            .await
        {
            Ok(found) => found,
            Err(response) => return response,
        };
    if !doc.can_edit {
        return error(StatusCode::FORBIDDEN, &doc.edit_rule);
    }
    let who = crate::catalog_docs::caller_identity(&s, caller);
    let sql = format!(
        "INSERT INTO verified_query \
         (catalog, namespace, table_name, question, sql, verified_by, expected) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING {ROW_COLUMNS}"
    );
    match sqlx::query_as::<_, VerifiedQuery>(sqlx::AssertSqlSafe(sql))
        .bind(&name)
        .bind(&ns)
        .bind(&tbl)
        .bind(&new.question)
        .bind(&statement)
        .bind(&who.sub)
        .bind(&new.expected)
        .fetch_one(pool)
        .await
    {
        Ok(row) => {
            tracing::info!(
                target: "audit",
                sub = %who.sub,
                catalog = %name,
                table = %format!("{ns}.{tbl}"),
                id = row.id,
                "verified query created",
            );
            (StatusCode::CREATED, Json(row)).into_response()
        }
        Err(e) => store_failed(e),
    }
}

/// `POST /api/verified-queries/{id}/check` — run the statement as the caller and compare it
/// with the expected result; `200` with the updated row either way.
///
/// Whoever may see the table may check it: the check is the owner's assertion re-run, not an
/// edit. An engine error is a failed check with the engine's message — the check itself worked.
#[utoipa::path(
    post,
    operation_id = "verified_queries_check",
    path = "/api/verified-queries/{id}/check",
    tag = "public",
    description = "Run one verified query as the caller and compare its rows with the expected result, store the outcome on the row and answer the row. Without an expected result it passes when the statement runs; with one it passes when the rows are equal as a multiset, numbers compared after rounding to six decimals. A readable difference says what was expected and what came back. Whoever may see the table may check it.",
    params(("id" = i64, Path, description = "The verified query's id")),
    responses(
        (status = 200, description = "The updated row: `lastCheckPassed` and `lastCheckMessage` say what the check found", body = VerifiedQuery),
        (status = 400, description = "The stored SQL is no longer one read-only statement", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such verified query, catalog or table", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn check(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Path(id): Path<String>,
) -> Response {
    let Ok(id) = id.parse::<i64>() else {
        return not_found();
    };
    let caller = caller.as_ref().map(|c| &c.0);
    match check_one(&s, caller, id).await {
        Ok(row) => Json(row).into_response(),
        Err(response) => response,
    }
}

/// Run one verified query as `caller` and store the outcome: the body of `check`, shared with
/// the table's bulk check so both run and record a check the same way.
#[allow(clippy::result_large_err)]
async fn check_one(
    s: &Arc<AppState>,
    caller: Option<&Principal>,
    id: i64,
) -> Result<VerifiedQuery, Response> {
    let Some(pool) = s.db.as_ref() else {
        return Err(unavailable());
    };
    let row = match sqlx::query_as::<_, CheckTarget>(
        "SELECT catalog, namespace, table_name, sql, expected FROM verified_query WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    {
        Ok(Some(row)) => row,
        Ok(None) => return Err(not_found()),
        Err(e) => return Err(store_failed(e)),
    };
    crate::catalog_docs::documentation_for(
        s,
        caller,
        Some(&row.catalog),
        &row.namespace,
        &row.table_name,
    )
    .await?;
    let statement = match crate::mcp::classify(&row.sql, false) {
        Ok(statement) => statement,
        Err(message) => return Err(error(StatusCode::BAD_REQUEST, &message)),
    };
    let sub = crate::catalog_docs::caller_identity(s, caller).sub;
    let (passed, message) =
        match crate::trino::execute_from_tagged(s, statement, caller, "nova-eval", "eval").await {
            Err(message) => (false, message),
            Ok(result) => match row.expected.as_ref() {
                None => (true, RAN_WITHOUT_EXPECTED.to_string()),
                Some(expected) => match compare(expected, &result) {
                    Ok(()) => (true, RESULT_MATCHES.to_string()),
                    Err(message) => (false, message),
                },
            },
        };
    let sql = format!(
        "UPDATE verified_query \
         SET last_check_at = now(), last_check_passed = $2, last_check_message = $3 \
         WHERE id = $1 RETURNING {ROW_COLUMNS}"
    );
    match sqlx::query_as::<_, VerifiedQuery>(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .bind(passed)
        .bind(&message)
        .fetch_one(pool)
        .await
    {
        Ok(updated) => {
            tracing::info!(
                target: "audit",
                sub = %sub,
                catalog = %row.catalog,
                table = %format!("{}.{}", row.namespace, row.table_name),
                id,
                passed,
                "verified query checked",
            );
            Ok(updated)
        }
        Err(e) => Err(store_failed(e)),
    }
}

/// `POST /api/catalog/tables/{ns}/{tbl}/verified-queries/check` — run every verified query of
/// one table as the caller, one after another, and answer the totals.
#[utoipa::path(
    post,
    operation_id = "verified_queries_check_all",
    path = "/api/catalog/tables/{ns}/{tbl}/verified-queries/check",
    tag = "public",
    description = "Run every verified query of one table as the caller, one after another, in id order and at most 50, and answer how many were checked, how many passed and how many failed, with the rows as the checks left them. Each row is checked exactly as `POST /api/verified-queries/{id}/check` checks it; a scheduled job calls this once instead of once per query. A table the caller may not see answers what the documentation route answers. `?catalog=` picks the catalog.",
    params(
        ("ns" = String, Path, description = "Namespace (schema) name"),
        ("tbl" = String, Path, description = "Table name"),
        crate::catalog::CatalogParam,
    ),
    responses(
        (status = 200, description = "The totals and the rows the checks left", body = CheckAllAnswer),
        (status = 400, description = "A name is not a single path segment, the catalog name is invalid, or a stored statement is no longer one read-only statement", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog or no such table", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn check_all(
    State(s): State<Arc<AppState>>,
    Path((ns, tbl)): Path<(String, String)>,
    Query(q): Query<CatalogParam>,
    caller: Option<Extension<Principal>>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let caller = caller.as_ref().map(|c| &c.0);
    let (name, _doc) =
        match crate::catalog_docs::documentation_for(&s, caller, q.catalog.as_deref(), &ns, &tbl)
            .await
        {
            Ok(found) => found,
            Err(response) => return response,
        };
    let sql = format!(
        "SELECT id FROM verified_query \
         WHERE catalog = $1 AND namespace = $2 AND table_name = $3 \
         ORDER BY id LIMIT {CHECK_ALL_LIMIT}"
    );
    let ids = match sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql))
        .bind(&name)
        .bind(&ns)
        .bind(&tbl)
        .fetch_all(pool)
        .await
    {
        Ok(ids) => ids,
        Err(e) => return store_failed(e),
    };
    // One after another, never in parallel: the engine is shared with every other caller.
    let mut items = Vec::with_capacity(ids.len());
    for id in ids {
        match check_one(&s, caller, id).await {
            Ok(row) => items.push(row),
            Err(response) => return response,
        }
    }
    let (checked, passed, failed) = totals(&items);
    let sub = crate::catalog_docs::caller_identity(&s, caller).sub;
    tracing::info!(
        target: "audit",
        sub = %sub,
        catalog = %name,
        table = %format!("{ns}.{tbl}"),
        checked,
        passed,
        failed,
        "verified queries checked",
    );
    Json(CheckAllAnswer {
        checked,
        passed,
        failed,
        items,
    })
    .into_response()
}

/// Compare a statement's result with an expected one.
///
/// Equal as a multiset: the rows are sorted by their JSON text, so another order passes. A
/// number counts as its value rounded to six decimals. A result the engine cut before the end
/// never passes — it is a prefix, not the answer.
pub(crate) fn compare(expected: &serde_json::Value, result: &QueryResult) -> Result<(), String> {
    if expected.is_null() {
        return Ok(());
    }
    if result.truncated {
        return Err(TOO_LARGE.into());
    }
    let Some(rows) = expected.as_array() else {
        return Err(BAD_EXPECTED.into());
    };
    if rows.iter().any(|row| row.as_array().is_none()) {
        return Err(BAD_EXPECTED.into());
    }
    let want: Vec<String> = rows.iter().map(row_text).collect();
    let got: Vec<String> = result
        .rows
        .iter()
        .map(|row| row_text(&serde_json::Value::Array(row.clone())))
        .collect();
    if want.len() != got.len() {
        return Err(short(&format!(
            "expected {} rows, got {}",
            want.len(),
            got.len()
        )));
    }
    let mut sorted_want = want.clone();
    sorted_want.sort();
    let mut sorted_got = got.clone();
    sorted_got.sort();
    if sorted_want == sorted_got {
        return Ok(());
    }
    for (i, (w, g)) in want.iter().zip(got.iter()).enumerate() {
        if w != g {
            return Err(short(&format!(
                "row {} differs: expected {w}, got {g}",
                i + 1
            )));
        }
    }
    Err(short("the rows differ"))
}

/// One row as comparable text: numbers rounded to six decimals, keys in their JSON order.
fn row_text(row: &serde_json::Value) -> String {
    serde_json::to_string(&rounded(row)).unwrap_or_default()
}

/// Every number in `value`, rounded to six decimals; everything else unchanged. A whole
/// number is an integer in the text, so `1` and `1.0` compare equal and say `1`.
fn rounded(value: &serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                return Value::Number(i.into());
            }
            if let Some(u) = n.as_u64() {
                return Value::Number(u.into());
            }
            let Some(f) = n.as_f64() else {
                return value.clone();
            };
            let scaled = (f * 1_000_000.0).round() / 1_000_000.0;
            let number = if scaled.fract() == 0.0 && scaled.abs() < 9e15 {
                serde_json::Number::from(scaled as i64)
            } else {
                match serde_json::Number::from_f64(scaled) {
                    Some(number) => number,
                    None => return value.clone(),
                }
            };
            Value::Number(number)
        }
        Value::Array(items) => Value::Array(items.iter().map(rounded).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, item)| (key.clone(), rounded(item)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// A difference message, cut at [`DIFF_CHARS`] characters.
fn short(message: &str) -> String {
    message.chars().take(DIFF_CHARS).collect()
}

/// `DELETE /api/verified-queries/{id}` — `204`; `404` when the id is not a row.
#[utoipa::path(
    delete,
    operation_id = "verified_queries_delete",
    path = "/api/verified-queries/{id}",
    tag = "public",
    description = "Remove one verified query. Only the owner group of its table and administrators may delete it.",
    params(("id" = i64, Path, description = "The verified query's id")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 403, description = "Only the owner group and administrators may delete a verified query", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such verified query, or the id is not a number", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn delete(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Path(id): Path<String>,
) -> Response {
    let Ok(id) = id.parse::<i64>() else {
        return not_found();
    };
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let row = match sqlx::query_as::<_, TableRef>(
        "SELECT catalog, namespace, table_name FROM verified_query WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    {
        Ok(Some(row)) => row,
        Ok(None) => return not_found(),
        Err(e) => return store_failed(e),
    };
    let caller = caller.as_ref().map(|c| &c.0);
    let (name, doc) = match crate::catalog_docs::documentation_for(
        &s,
        caller,
        Some(&row.catalog),
        &row.namespace,
        &row.table_name,
    )
    .await
    {
        Ok(found) => found,
        Err(response) => return response,
    };
    if !doc.can_edit {
        return error(StatusCode::FORBIDDEN, &doc.edit_rule);
    }
    let sub = crate::catalog_docs::caller_identity(&s, caller).sub;
    match sqlx::query("DELETE FROM verified_query WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
    {
        Ok(done) if done.rows_affected() == 1 => {
            tracing::info!(
                target: "audit",
                sub = %sub,
                catalog = %name,
                table = %format!("{}.{}", row.namespace, row.table_name),
                id,
                "verified query deleted",
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(_) => not_found(),
        Err(e) => store_failed(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new(question: &str, sql: &str) -> NewVerifiedQuery {
        NewVerifiedQuery {
            question: question.into(),
            sql: sql.into(),
            expected: None,
        }
    }

    #[tokio::test]
    async fn prune_missing_without_a_pool_is_a_no_op() {
        assert_eq!(prune_missing(None, "gold", &[]).await, 0);
    }

    /// One verified query per table, written straight into the store (the route needs a
    /// catalog and a caller).
    async fn insert_query(pool: &PgPool, catalog: &str, namespace: &str, table: &str) {
        sqlx::query(
            "INSERT INTO verified_query \
             (catalog, namespace, table_name, question, sql, verified_by) \
             VALUES ($1, $2, $3, 'How many rows?', 'SELECT count(*) FROM t', 'test')",
        )
        .bind(catalog)
        .bind(namespace)
        .bind(table)
        .execute(pool)
        .await
        .expect("insert a verified query");
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn prune_missing_deletes_rows_for_vanished_tables_only() {
        let pool = crate::db::test_support::migrated_pool().await;
        insert_query(&pool, "gold", "shop", "daily_revenue").await;
        insert_query(&pool, "gold", "shop", "orders").await;
        insert_query(&pool, "silver", "shop", "daily_revenue").await;

        let deleted = prune_missing(
            Some(&pool),
            "gold",
            &[("shop".to_string(), "daily_revenue".to_string())],
        )
        .await;
        assert_eq!(deleted, 1, "only the vanished table's row");
        let (gold, survivor): (i64, String) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM verified_query WHERE catalog = 'gold'), table_name \
             FROM verified_query WHERE catalog = 'gold'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(gold, 1);
        assert_eq!(survivor, "daily_revenue");
        let (silver,): (i64,) =
            sqlx::query_as("SELECT count(*) FROM verified_query WHERE catalog = 'silver'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(silver, 1, "another catalog is never touched");

        // An empty listing deletes every row of that catalog and none of another.
        let deleted = prune_missing(Some(&pool), "gold", &[]).await;
        assert_eq!(deleted, 1);
        let (gold, silver): (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM verified_query WHERE catalog = 'gold'), \
                    (SELECT count(*) FROM verified_query WHERE catalog = 'silver')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!((gold, silver), (0, 1));

        sqlx::query("DELETE FROM verified_query")
            .execute(&pool)
            .await
            .unwrap();
    }

    /// A result as the engine returns it: one array per row.
    fn result(rows: &[&[serde_json::Value]], truncated: bool) -> QueryResult {
        QueryResult {
            columns: Vec::new(),
            rows: rows.iter().map(|row| row.to_vec()).collect(),
            row_count: rows.len(),
            query_id: None,
            stats: None,
            truncated,
        }
    }

    fn rows(text: &str) -> serde_json::Value {
        serde_json::from_str(text).expect("valid JSON")
    }

    #[test]
    fn validate_trims_both_fields() {
        let ok = validate(new("  Revenue last month?  ", "  SELECT 1  ")).expect("valid");
        assert_eq!(ok.question, "Revenue last month?");
        assert_eq!(ok.sql, "SELECT 1");
    }

    #[test]
    fn validate_refuses_an_empty_question() {
        let err = validate(new("   ", "SELECT 1")).expect_err("empty question");
        assert_eq!(err, "the question must not be empty");
    }

    #[test]
    fn validate_refuses_an_empty_statement() {
        let err = validate(new("q", "  \n ")).expect_err("empty sql");
        assert_eq!(err, "sql must not be empty");
    }

    #[test]
    fn validate_refuses_a_question_over_the_limit() {
        let long = "q".repeat(MAX_QUESTION_CHARS);
        assert!(
            validate(new(&long, "SELECT 1")).is_ok(),
            "exactly at the limit"
        );
        let err = validate(new(&"q".repeat(MAX_QUESTION_CHARS + 1), "SELECT 1"))
            .expect_err("over the limit");
        assert_eq!(
            err,
            format!("the question must be at most {MAX_QUESTION_CHARS} characters")
        );
    }

    #[test]
    fn validate_refuses_sql_over_the_limit() {
        let long = "s".repeat(MAX_SQL_CHARS);
        assert!(validate(new("q", &long)).is_ok(), "exactly at the limit");
        let err = validate(new("q", &"s".repeat(MAX_SQL_CHARS + 1))).expect_err("over the limit");
        assert_eq!(
            err,
            format!("sql must be at most {MAX_SQL_CHARS} characters")
        );
    }

    #[test]
    fn classify_refuses_a_write_and_names_the_statement() {
        let message = crate::mcp::classify("DELETE FROM x", false).expect_err("a write");
        assert!(
            message.starts_with("only reading statements run here"),
            "{message}"
        );
        assert!(message.contains("DELETE"), "{message}");
    }

    #[test]
    fn a_row_serializes_with_camel_case_keys() {
        let value = serde_json::to_value(VerifiedQuery {
            id: 1,
            question: "q".into(),
            sql: "SELECT 1".into(),
            verified_by: "ada".into(),
            verified_at: "2026-11-15T11:03:46.000Z".into(),
            expected: Some(rows(r#"[[1]]"#)),
            last_check_at: Some("2026-11-15T12:00:00.000Z".into()),
            last_check_passed: Some(true),
            last_check_message: Some(RESULT_MATCHES.into()),
        })
        .expect("serializes");
        assert_eq!(value["verifiedBy"], serde_json::json!("ada"));
        assert_eq!(
            value["verifiedAt"],
            serde_json::json!("2026-11-15T11:03:46.000Z")
        );
        assert_eq!(value["expected"], rows(r#"[[1]]"#));
        assert_eq!(
            value["lastCheckAt"],
            serde_json::json!("2026-11-15T12:00:00.000Z")
        );
        assert_eq!(value["lastCheckPassed"], serde_json::json!(true));
        assert_eq!(value["lastCheckMessage"], serde_json::json!(RESULT_MATCHES));
    }

    #[test]
    fn totals_count_the_rows_that_passed_and_the_rows_that_failed() {
        let row = |passed: Option<bool>| VerifiedQuery {
            id: 1,
            question: "q".into(),
            sql: "SELECT 1".into(),
            verified_by: "ada".into(),
            verified_at: "2026-11-15T11:03:46.000Z".into(),
            expected: None,
            last_check_at: None,
            last_check_passed: passed,
            last_check_message: None,
        };
        let items = [
            row(Some(true)),
            row(Some(false)),
            row(Some(true)),
            row(None),
        ];
        assert_eq!(totals(&items), (4, 2, 1));
    }

    #[test]
    fn totals_of_no_rows_are_zero() {
        assert_eq!(totals(&[]), (0, 0, 0));
    }

    #[test]
    fn compare_passes_when_the_rows_are_equal_in_another_order() {
        let expected = rows(r#"[[2, "b"], [1, "a"]]"#);
        let got = result(
            &[
                &[serde_json::json!(1), serde_json::json!("a")],
                &[serde_json::json!(2), serde_json::json!("b")],
            ],
            false,
        );
        assert_eq!(compare(&expected, &got), Ok(()));
    }

    #[test]
    fn compare_fails_with_the_count_when_one_row_is_extra() {
        let expected = rows(r#"[[1], [2], [3]]"#);
        let got = result(
            &[
                &[serde_json::json!(1)],
                &[serde_json::json!(2)],
                &[serde_json::json!(3)],
                &[serde_json::json!(4)],
            ],
            false,
        );
        assert_eq!(
            compare(&expected, &got),
            Err("expected 3 rows, got 4".into())
        );
    }

    #[test]
    fn compare_fails_with_the_row_when_one_value_changes() {
        let expected = rows(r#"[[1, "a"], [2, "b"]]"#);
        let got = result(
            &[
                &[serde_json::json!(1), serde_json::json!("a")],
                &[serde_json::json!(2), serde_json::json!("c")],
            ],
            false,
        );
        let message = compare(&expected, &got).expect_err("differs");
        assert_eq!(message, r#"row 2 differs: expected [2,"b"], got [2,"c"]"#);
        assert!(message.chars().count() <= 300, "{message}");
    }

    #[test]
    fn compare_treats_1_0000001_as_1() {
        let expected = rows("[[1.0000001]]");
        let got = result(&[&[serde_json::json!(1.0)]], false);
        assert_eq!(compare(&expected, &got), Ok(()));
    }

    #[test]
    fn compare_passes_with_no_expected_result() {
        let got = result(&[&[serde_json::json!(1)]], false);
        assert_eq!(compare(&serde_json::Value::Null, &got), Ok(()));
    }

    #[test]
    fn compare_fails_a_truncated_result() {
        let expected = rows("[[1]]");
        let got = result(&[&[serde_json::json!(1)]], true);
        assert_eq!(compare(&expected, &got), Err(TOO_LARGE.into()));
    }

    #[test]
    fn validate_refuses_101_expected_rows() {
        let hundred: Vec<serde_json::Value> = (0..MAX_EXPECTED_ROWS)
            .map(|i| serde_json::json!([i]))
            .collect();
        let mut at_limit = new("q", "SELECT 1");
        at_limit.expected = Some(serde_json::Value::Array(hundred.clone()));
        assert!(validate(at_limit).is_ok(), "exactly at the limit");

        let mut over = new("q", "SELECT 1");
        over.expected = Some(serde_json::json!(
            hundred
                .into_iter()
                .chain(std::iter::once(serde_json::json!([0])))
                .collect::<Vec<_>>()
        ));
        let err = validate(over).expect_err("101 rows");
        assert_eq!(err, BAD_EXPECTED);
    }
}
