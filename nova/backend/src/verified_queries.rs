//! Verified queries (design nova#911 slice 2a): a question about a table, the SQL that
//! answers it, who verified it and when.
//!
//! The table's owners keep them next to the table's guide (`catalog_docs.rs`): an
//! administrator or the owner group adds and removes one, and everyone who may see the
//! table reads them. For a person a verified query is the trusted way to compute a number;
//! for an agent it is an example to copy instead of guessing SQL.
//!
//! Only one read-only statement is accepted, checked with `mcp::classify` — the same
//! classifier that guards `sql_read` and Ask. The stored SQL is never run from here: this
//! module reads and lists, nothing else.
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

use crate::AppState;
use crate::api_json::ApiJson;
use crate::auth::Principal;
use crate::catalog::CatalogParam;
use crate::saved_queries::{error, not_found, store_failed, unavailable};

/// Most characters a question may carry (the table's CHECK allows the same).
pub const MAX_QUESTION_CHARS: usize = 500;
/// Most characters the SQL may carry.
pub const MAX_SQL_CHARS: usize = 20_000;
/// Rows one list answers, newest first.
const LIST_LIMIT: usize = 50;

/// A row as the API returns it. `verified_at` is RFC 3339 in UTC.
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
}

/// `POST /api/catalog/tables/{ns}/{tbl}/verified-queries` body.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct NewVerifiedQuery {
    /// The question, at most 500 characters.
    pub question: String,
    /// One read-only statement, at most 20 000 characters.
    pub sql: String,
}

/// The row a delete needs before it can ask the table's rule: the table it belongs to.
#[derive(sqlx::FromRow)]
struct TableRef {
    catalog: String,
    namespace: String,
    table_name: String,
}

/// The columns [`VerifiedQuery`] reads; the timestamp as RFC 3339 in UTC.
const ROW_COLUMNS: &str = "id, question, sql, verified_by, \
    to_char(verified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS verified_at";

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
    Ok(NewVerifiedQuery { question, sql })
}

/// `GET /api/catalog/tables/{ns}/{tbl}/verified-queries` — the table's, newest first.
#[utoipa::path(
    get,
    operation_id = "verified_queries_list",
    path = "/api/catalog/tables/{ns}/{tbl}/verified-queries",
    tag = "public",
    description = "The verified queries of one table: questions its owners answered with one read-only statement, who verified each and when. Newest first, at most 50. A table the caller may not see answers what the documentation route answers. `?catalog=` picks the catalog.",
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
    description = "Verify one answer to a question about this table: the question (at most 500 characters) and one read-only statement (at most 20 000). Only the owner group of the table and administrators may add one; the caller is recorded as `verifiedBy`. The statement is checked, never run. `?catalog=` picks the catalog.",
    params(
        ("ns" = String, Path, description = "Namespace (schema) name"),
        ("tbl" = String, Path, description = "Table name"),
        crate::catalog::CatalogParam,
    ),
    request_body = NewVerifiedQuery,
    responses(
        (status = 201, description = "The stored row", body = VerifiedQuery),
        (status = 400, description = "The question or the SQL is empty or too long, the catalog name is invalid, or the statement is not one read-only statement", body = crate::openapi::ErrorResponse),
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
        "INSERT INTO verified_query (catalog, namespace, table_name, question, sql, verified_by) \
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING {ROW_COLUMNS}"
    );
    match sqlx::query_as::<_, VerifiedQuery>(sqlx::AssertSqlSafe(sql))
        .bind(&name)
        .bind(&ns)
        .bind(&tbl)
        .bind(&new.question)
        .bind(&statement)
        .bind(&who.sub)
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
        }
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
        })
        .expect("serializes");
        assert_eq!(value["verifiedBy"], serde_json::json!("ada"));
        assert_eq!(
            value["verifiedAt"],
            serde_json::json!("2026-11-15T11:03:46.000Z")
        );
    }
}
