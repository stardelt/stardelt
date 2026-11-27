//! Evals (stardelt/stardelt#125, slices 1 and 2): a question with a known answer, kept next
//! to the catalog's other context and served by the API, and the runs that compare an
//! answer with it.
//!
//! Slice 1 stores the eval and serves it. Slice 2 takes an answer the runner posted — the
//! SQL an agent wrote and the rows it returned — compares it with `expected` here, in Nova,
//! and stores the verdict as one row of `eval_run`. The API never executes a stored eval and
//! never sends the rows to a model: the expected answer is the owner's assertion, like a
//! verified query's expected result.
//!
//! `expected` is one of three shapes, and anything else is refused with a sentence:
//!
//! - `{"kind":"rows","rows":[[…],…]}` — the answer is this small table.
//! - `{"kind":"value","value":<scalar>}` — the answer is `1284`.
//! - `{"kind":"uses","tables":["shop.orders", …]}` — the SQL must use these tables.
//!
//! Reading needs what reading the catalog's products needs ([`crate::catalog::catalog_scope`]),
//! because an eval is catalog context like a guide. Writing, deleting and posting a run need
//! the caller to be a member of the row's `ownerGroup` or an administrator.
//!
//! Without a state store (`NOVA_DATABASE_URL` unset) the routes answer
//! `503 {"error":"this needs Nova's database, …"}`. A create checks the request itself (400),
//! then the caller's group rule (403), then the store (503).
//!
//! Every write writes one line on the `audit` target with the caller, the catalog and the
//! row's id — never the expected answer.

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
use crate::saved_queries::{error, store_failed, unavailable, validate_group};

/// Most characters a question may carry (the table's CHECK allows the same).
const MAX_QUESTION_CHARS: usize = 500;
/// Most rows a `rows` answer may carry.
const MAX_ROWS: usize = 100;
/// Most values one row of a `rows` answer may carry.
const MAX_VALUES_PER_ROW: usize = 50;
/// Most characters one string cell may carry.
const MAX_CELL_CHARS: usize = 500;
/// Most `namespace.table` names a `uses` answer may carry.
const MAX_TABLES: usize = 10;
/// Most characters a posted answer's `sql` may carry.
const MAX_SQL_CHARS: usize = 20_000;
/// Most characters the runner's own `error` may carry.
const MAX_ERROR_CHARS: usize = 2_000;
/// Most characters a posted `model` or `endpoint` name may carry.
const MAX_NAME_CHARS: usize = 200;
/// How many runs a page carries when the caller names no `limit`.
const DEFAULT_RUNS_LIMIT: i64 = 20;
/// The largest page of runs.
const MAX_RUNS_LIMIT: i64 = 100;

/// The refusal when `expected` is not one of the three shapes.
const BAD_SHAPE: &str = "expected must be an object with kind rows, value or uses";
/// The refusal when the `rows` shape is out of bounds.
const BAD_ROWS: &str = "expected.rows must hold 1 to 100 rows of 1 to 50 values, each a string of at most 500 characters, a number, a boolean or null";
/// The refusal when the `tables` shape is out of bounds.
const BAD_TABLES: &str = "expected.tables must hold 1 to 10 namespace.table names";
/// The refusal when the `value` shape is not a scalar.
const BAD_VALUE: &str = "expected.value must be a string, a number, a boolean or null";
/// The refusal when the caller may not write the eval.
const NOT_YOURS: &str = "only a member of the owner group or an administrator may write an eval";
/// The refusal when an answer's rows are out of bounds.
const BAD_ANSWER_ROWS: &str = "rows must hold 1 to 100 rows of 1 to 50 values, each a string of at most 500 characters, a number, a boolean or null";
/// The refusal when the caller may not post a run.
const NOT_YOURS_RUN: &str = "only a member of the owner group or an administrator may run an eval";
/// The verdict when two tables of rows are not the same rows.
const ROWS_DIFFER: &str = "the answer's rows differ from the expected rows";
/// The verdict when a `uses` answer carries no statement.
const NO_SQL: &str = "the answer carried no SQL";

/// The columns [`Eval`] reads; the timestamp as RFC 3339 in UTC.
const ROW_COLUMNS: &str = "id, owner_group, question, expected, created_by, \
    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS created_at";

/// One eval as the API returns it.
#[derive(Debug, Serialize, sqlx::FromRow, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Eval {
    pub id: i64,
    /// The group that owns the eval; only its members and administrators may change it.
    pub owner_group: String,
    /// The question a person asks, in words, 1 to 500 characters.
    pub question: String,
    /// The known answer: `{"kind":"rows",…}`, `{"kind":"value",…}` or `{"kind":"uses",…}`.
    #[schema(value_type = serde_json::Value)]
    pub expected: serde_json::Value,
    /// The `sub` of the person who wrote the eval.
    pub created_by: String,
    /// When it was written, RFC 3339.
    pub created_at: String,
}

/// `POST /api/catalog/evals` body.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NewEval {
    /// The group that owns the eval.
    pub owner_group: String,
    /// The question, 1 to 500 characters.
    pub question: String,
    /// The known answer: `{"kind":"rows",…}`, `{"kind":"value",…}` or `{"kind":"uses",…}`.
    #[schema(value_type = serde_json::Value)]
    pub expected: serde_json::Value,
}

/// The columns [`EvalRun`] reads; the timestamp as RFC 3339 in UTC.
const RUN_COLUMNS: &str = "id, eval_id, passed, reason, sql, model, endpoint, tokens, created_by, \
    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS created_at";

/// One posted answer and its verdict, as the API returns it.
#[derive(Debug, Serialize, sqlx::FromRow, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EvalRun {
    pub id: i64,
    /// The eval the run answers.
    pub eval_id: i64,
    /// Whether the answer was the expected one.
    pub passed: bool,
    /// Why it failed; absent when it passed.
    pub reason: Option<String>,
    /// The statement the agent wrote, if the runner had one.
    pub sql: Option<String>,
    /// The model that wrote the answer, if the runner named it.
    pub model: Option<String>,
    /// The model endpoint that answered, if the runner named it.
    pub endpoint: Option<String>,
    /// The tokens the model call used, if the runner counted them.
    pub tokens: Option<i64>,
    /// The `sub` of the person who posted the run.
    pub created_by: String,
    /// When it was posted, RFC 3339.
    pub created_at: String,
}

/// `POST /api/evals/{id}/runs` body: one answer, compared with the eval's `expected` here.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NewEvalRun {
    /// The statement an agent wrote, at most 20 000 characters.
    pub sql: Option<String>,
    /// The rows the statement returned: 1 to 100 rows of 1 to 50 scalars.
    pub rows: Option<Vec<Vec<serde_json::Value>>>,
    /// The runner's own failure (Ask refused, the statement errored), at most 2 000 characters.
    pub error: Option<String>,
    /// The model that wrote the answer, at most 200 characters.
    pub model: Option<String>,
    /// The model endpoint that answered, at most 200 characters.
    pub endpoint: Option<String>,
    /// The tokens the model call used; not negative.
    pub tokens: Option<i64>,
}

/// The eval a run answers: its owner group and its expected answer.
#[derive(sqlx::FromRow)]
struct EvalExpected {
    owner_group: String,
    expected: serde_json::Value,
}

/// `?limit=` of `GET /api/evals/{id}/runs`, read as a string so a bad value answers our sentence.
#[derive(Debug, Deserialize)]
pub(crate) struct RunsParams {
    limit: Option<String>,
}

/// The owner group of one row, read before a delete asks the group rule.
#[derive(sqlx::FromRow)]
struct EvalRef {
    owner_group: String,
}

/// A scalar cell: a string of at most 500 characters, a number, a boolean or `null`.
fn is_scalar(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(text) => text.chars().count() <= MAX_CELL_CHARS,
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) | serde_json::Value::Null => true,
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => false,
    }
}

/// A plain identifier: a letter or underscore, then letters, digits and underscores.
fn valid_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A `namespace.table` name; each half is a plain identifier.
fn valid_table_name(value: &str) -> bool {
    match value.split_once('.') {
        Some((namespace, table)) => valid_identifier(namespace) && valid_identifier(table),
        None => false,
    }
}

/// The `rows` shape: 1 to 100 rows, each 1 to 50 scalar values.
fn valid_rows(value: &serde_json::Value) -> bool {
    let Some(rows) = value.as_array() else {
        return false;
    };
    (1..=MAX_ROWS).contains(&rows.len())
        && rows.iter().all(|row| {
            row.as_array().is_some_and(|cells| {
                (1..=MAX_VALUES_PER_ROW).contains(&cells.len()) && cells.iter().all(is_scalar)
            })
        })
}

/// The `uses` shape: 1 to 10 `namespace.table` names.
fn valid_tables(value: &serde_json::Value) -> bool {
    let Some(tables) = value.as_array() else {
        return false;
    };
    (1..=MAX_TABLES).contains(&tables.len())
        && tables
            .iter()
            .all(|table| table.as_str().is_some_and(valid_table_name))
}

/// `expected` is an object with a `kind` and exactly the keys that kind names.
fn validate_expected(value: &serde_json::Value) -> Result<(), String> {
    let Some(object) = value.as_object() else {
        return Err(BAD_SHAPE.into());
    };
    // `kind` plus exactly the one key that kind names; any other key is another shape.
    if object.len() != 2 {
        return Err(BAD_SHAPE.into());
    }
    match object.get("kind").and_then(|kind| kind.as_str()) {
        Some("rows") => match object.get("rows") {
            Some(rows) if valid_rows(rows) => Ok(()),
            Some(_) => Err(BAD_ROWS.into()),
            None => Err(BAD_SHAPE.into()),
        },
        Some("uses") => match object.get("tables") {
            Some(tables) if valid_tables(tables) => Ok(()),
            Some(_) => Err(BAD_TABLES.into()),
            None => Err(BAD_SHAPE.into()),
        },
        Some("value") => match object.get("value") {
            Some(value) if is_scalar(value) => Ok(()),
            Some(_) => Err(BAD_VALUE.into()),
            None => Err(BAD_SHAPE.into()),
        },
        _ => Err(BAD_SHAPE.into()),
    }
}

/// The refusal when an answer's rows are out of bounds, or `None` when they are fine.
fn answer_rows_problem(rows: &[Vec<serde_json::Value>]) -> Option<&'static str> {
    let out_of_bounds = !(1..=MAX_ROWS).contains(&rows.len())
        || rows.iter().any(|row| {
            !(1..=MAX_VALUES_PER_ROW).contains(&row.len())
                || row.iter().any(|cell| !is_scalar(cell))
        });
    out_of_bounds.then_some(BAD_ANSWER_ROWS)
}

/// Two cells are equal: numbers by value (`1` and `1.0` are equal), anything else exactly.
fn same_scalar(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
        return a == b;
    }
    a == b
}

/// Whether `sql` names `table` as a whole `namespace.table` token: the character before and
/// after the match is not one of `A-Za-z0-9_.`. Both sides compare lower-cased.
fn names_table(sql: &str, table: &str) -> bool {
    /// One maximal run of `A-Za-z0-9_.`; every other character is a separator.
    fn token(ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'
    }
    let table = table.to_lowercase();
    sql.to_lowercase()
        .split(|ch: char| !token(ch))
        .any(|run| run == table)
}

/// Whether `answer` is the expected answer; `Err` carries the sentence the page shows.
fn verdict(expected: &serde_json::Value, answer: &NewEvalRun) -> Result<(), String> {
    if let Some(failure) = answer.error.as_deref() {
        return Err(format!("the answer carried an error: {}", failure.trim()));
    }
    let object = expected.as_object();
    let rows = answer.rows.as_deref().unwrap_or(&[]);
    match object.and_then(|o| o.get("kind")).and_then(|k| k.as_str()) {
        Some("value") => {
            if rows.len() != 1 {
                return Err(format!("the answer had {} rows, expected 1", rows.len()));
            }
            if rows[0].len() != 1 {
                return Err(format!(
                    "the answer had {} cells, expected 1",
                    rows[0].len()
                ));
            }
            let wanted = object
                .and_then(|o| o.get("value"))
                .unwrap_or(&serde_json::Value::Null);
            if same_scalar(&rows[0][0], wanted) {
                Ok(())
            } else {
                Err(ROWS_DIFFER.into())
            }
        }
        Some("rows") => {
            let wanted = object
                .and_then(|o| o.get("rows"))
                .and_then(|rows| rows.as_array())
                .cloned()
                .unwrap_or_default();
            let mut wanted: Vec<String> = wanted
                .iter()
                .map(serde_json::to_string)
                .collect::<Result<_, _>>()
                .map_err(|_| ROWS_DIFFER.to_string())?;
            let mut got: Vec<String> = rows
                .iter()
                .map(serde_json::to_string)
                .collect::<Result<_, _>>()
                .map_err(|_| ROWS_DIFFER.to_string())?;
            wanted.sort_unstable();
            got.sort_unstable();
            if wanted == got {
                Ok(())
            } else {
                Err(ROWS_DIFFER.into())
            }
        }
        Some("uses") => {
            let Some(sql) = answer.sql.as_deref() else {
                return Err(NO_SQL.into());
            };
            let tables: Vec<&str> = object
                .and_then(|o| o.get("tables"))
                .and_then(|tables| tables.as_array())
                .map(|tables| tables.iter().filter_map(|t| t.as_str()).collect())
                .unwrap_or_default();
            for table in tables {
                if !names_table(sql, table) {
                    return Err(format!("the SQL does not use {table}"));
                }
            }
            Ok(())
        }
        _ => Err(BAD_SHAPE.into()),
    }
}

/// Whether `principal` may write an eval owned by `group`: a member of the group, an
/// administrator, or — in `dev` mode, where there is one person — anybody.
fn member_of(group: &str, principal: Option<&Principal>) -> bool {
    principal.is_none_or(|p| p.identity.is_admin || p.identity.groups.iter().any(|g| g == group))
}

/// `404` for an id that is not a row.
fn no_such_eval() -> Response {
    error(StatusCode::NOT_FOUND, "no such eval")
}

/// `GET /api/catalog/evals` — the catalog's evals, in id order.
#[utoipa::path(
    get,
    operation_id = "catalog_eval_list",
    path = "/api/catalog/evals",
    tag = "public",
    description = "The evals of one catalog: questions with a known answer, kept as catalog context. A question may appear twice. In id order. `?catalog=` picks the catalog; the caller must be able to browse it, and without a state store the route answers 503.",
    params(crate::catalog::CatalogParam),
    responses(
        (status = 200, description = "The catalog's evals, in id order", body = Vec<Eval>),
        (status = 400, description = "The catalog name is invalid", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn list(
    State(s): State<Arc<AppState>>,
    Query(q): Query<CatalogParam>,
    caller: Option<Extension<Principal>>,
) -> Response {
    let caller = caller.as_ref().map(|c| &c.0);
    // An installation that cannot store an eval says so before it calls the catalog.
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let (name, _id) = match crate::catalog::catalog_scope(&s, caller, q.catalog.as_deref()).await {
        Ok(scope) => scope,
        Err(response) => return response,
    };
    let sql = format!("SELECT {ROW_COLUMNS} FROM eval WHERE catalog = $1 ORDER BY id");
    match sqlx::query_as::<_, Eval>(sqlx::AssertSqlSafe(sql))
        .bind(&name)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => store_failed(e),
    }
}

/// `POST /api/catalog/evals` — `201` with the stored row.
#[utoipa::path(
    post,
    operation_id = "catalog_eval_create",
    path = "/api/catalog/evals",
    tag = "public",
    description = "Store one eval: a question (1 to 500 characters) and its known answer, `{\"kind\":\"rows\",\"rows\":[[…],…]}` (1 to 100 rows of 1 to 50 scalars, a string at most 500 characters), `{\"kind\":\"value\",\"value\":<scalar>}`, or `{\"kind\":\"uses\",\"tables\":[\"namespace.table\", …]}` (1 to 10 names). Only a member of `ownerGroup` or an administrator may write one; the caller is recorded as `createdBy`. The eval is stored, never executed. There is no update route: changing an eval is delete and create. `?catalog=` picks the catalog.",
    params(crate::catalog::CatalogParam),
    request_body = NewEval,
    responses(
        (status = 201, description = "The stored row", body = Eval),
        (status = 400, description = "The question, the owner group or the expected answer is not in the accepted shape, or the catalog name is invalid", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog, or only a member of the owner group or an administrator may write an eval", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn create(
    State(s): State<Arc<AppState>>,
    Query(q): Query<CatalogParam>,
    caller: Option<Extension<Principal>>,
    ApiJson(req): ApiJson<NewEval>,
) -> Response {
    let question = req.question.trim().to_string();
    if !(1..=MAX_QUESTION_CHARS).contains(&question.chars().count()) {
        return error(
            StatusCode::BAD_REQUEST,
            "question must be 1 to 500 characters",
        );
    }
    let owner_group = match validate_group(&req.owner_group) {
        Ok(group) => group,
        Err(message) => return error(StatusCode::BAD_REQUEST, &message),
    };
    if let Err(message) = validate_expected(&req.expected) {
        return error(StatusCode::BAD_REQUEST, &message);
    }
    let caller = caller.as_ref().map(|c| &c.0);
    if !member_of(&owner_group, caller) {
        return error(StatusCode::FORBIDDEN, NOT_YOURS);
    }
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let (catalog, _id) = match crate::catalog::catalog_scope(&s, caller, q.catalog.as_deref()).await
    {
        Ok(scope) => scope,
        Err(response) => return response,
    };
    let who = crate::catalog_docs::caller_identity(&s, caller);
    let sql = format!(
        "INSERT INTO eval (catalog, owner_group, question, expected, created_by) \
         VALUES ($1, $2, $3, $4, $5) RETURNING {ROW_COLUMNS}"
    );
    match sqlx::query_as::<_, Eval>(sqlx::AssertSqlSafe(sql))
        .bind(&catalog)
        .bind(&owner_group)
        .bind(&question)
        .bind(&req.expected)
        .bind(&who.sub)
        .fetch_one(pool)
        .await
    {
        Ok(row) => {
            tracing::info!(
                target: "audit",
                sub = %who.sub,
                catalog = %catalog,
                id = row.id,
                "eval created",
            );
            (StatusCode::CREATED, Json(row)).into_response()
        }
        Err(e) => store_failed(e),
    }
}

/// `DELETE /api/evals/{id}` — `204`; `404` when the id is not a row.
#[utoipa::path(
    delete,
    operation_id = "eval_delete",
    path = "/api/evals/{id}",
    tag = "public",
    description = "Remove one eval. Only a member of its owner group or an administrator may delete it; the eval was stored, never executed.",
    params(("id" = i64, Path, description = "The eval's id")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 403, description = "Only a member of the owner group or an administrator may delete an eval", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such eval, or the id is not a number", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn delete(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Path(id): Path<String>,
) -> Response {
    let Ok(id) = id.parse::<i64>() else {
        return no_such_eval();
    };
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let row = match sqlx::query_as::<_, EvalRef>("SELECT owner_group FROM eval WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
    {
        Ok(Some(row)) => row,
        Ok(None) => return no_such_eval(),
        Err(e) => return store_failed(e),
    };
    let caller = caller.as_ref().map(|c| &c.0);
    if !member_of(&row.owner_group, caller) {
        return error(StatusCode::FORBIDDEN, NOT_YOURS);
    }
    let sub = crate::catalog_docs::caller_identity(&s, caller).sub;
    match sqlx::query("DELETE FROM eval WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
    {
        Ok(done) if done.rows_affected() == 1 => {
            tracing::info!(
                target: "audit",
                sub = %sub,
                id,
                "eval deleted",
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(_) => no_such_eval(),
        Err(e) => store_failed(e),
    }
}

/// `GET /api/evals/{id}/runs` — the eval's runs, newest first.
#[utoipa::path(
    get,
    operation_id = "eval_run_list",
    path = "/api/evals/{id}/runs",
    tag = "public",
    description = "The runs of one eval — one posted answer and its verdict each — newest first. `?limit=` is 1 to 100, 20 by default. Reading a run is reading the eval's history, so the route needs a session like the rest of the API; it answers 404 for an id that is not an eval. Without a state store the route answers 503.",
    params(
        ("id" = i64, Path, description = "The eval's id"),
        ("limit" = Option<String>, Query, description = "How many runs, 1 to 100 (default 20)"),
    ),
    responses(
        (status = 200, description = "The eval's runs, newest first", body = Vec<EvalRun>),
        (status = 400, description = "limit is not a number from 1 to 100", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such eval, or the id is not a number", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn runs(
    State(s): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(q): Query<RunsParams>,
) -> Response {
    let Ok(id) = id.parse::<i64>() else {
        return no_such_eval();
    };
    let limit = match q.limit.as_deref() {
        None => DEFAULT_RUNS_LIMIT,
        Some(raw) => match raw.parse::<i64>() {
            Ok(n) if (1..=MAX_RUNS_LIMIT).contains(&n) => n,
            _ => return error(StatusCode::BAD_REQUEST, "limit must be 1 to 100"),
        },
    };
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    match sqlx::query_scalar::<_, bool>("SELECT true FROM eval WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
    {
        Ok(Some(_)) => {}
        Ok(None) => return no_such_eval(),
        Err(e) => return store_failed(e),
    }
    let sql =
        format!("SELECT {RUN_COLUMNS} FROM eval_run WHERE eval_id = $1 ORDER BY id DESC LIMIT $2");
    match sqlx::query_as::<_, EvalRun>(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .bind(limit)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => store_failed(e),
    }
}

/// `POST /api/evals/{id}/runs` — compare one answer and store the verdict, `201` with the row.
#[utoipa::path(
    post,
    operation_id = "eval_run_create",
    path = "/api/evals/{id}/runs",
    tag = "public",
    description = "Post one answer to an eval — the SQL an agent wrote, the rows it returned, or the runner's own error — and store the verdict: Nova compares it with the eval's `expected` here, never sends the rows to a model. `value` passes for one row of one equal cell (numbers by value); `rows` passes when the two tables are equal as a multiset; `uses` passes when the SQL names every expected `namespace.table`; an `error` fails with its sentence. Only a member of the eval's owner group or an administrator may post one; the caller is recorded as `createdBy`. The reason sentences are the page's text.",
    params(("id" = i64, Path, description = "The eval's id")),
    request_body = NewEvalRun,
    responses(
        (status = 201, description = "The stored run and its verdict", body = EvalRun),
        (status = 400, description = "The answer is over one of the limits", body = crate::openapi::ErrorResponse),
        (status = 403, description = "Only a member of the owner group or an administrator may run an eval", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such eval, or the id is not a number", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn create_run(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Path(id): Path<String>,
    ApiJson(req): ApiJson<NewEvalRun>,
) -> Response {
    let Ok(id) = id.parse::<i64>() else {
        return no_such_eval();
    };
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let eval = match sqlx::query_as::<_, EvalExpected>(
        "SELECT owner_group, expected FROM eval WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    {
        Ok(Some(row)) => row,
        Ok(None) => return no_such_eval(),
        Err(e) => return store_failed(e),
    };
    let caller = caller.as_ref().map(|c| &c.0);
    if !member_of(&eval.owner_group, caller) {
        return error(StatusCode::FORBIDDEN, NOT_YOURS_RUN);
    }
    if let Some(rows) = req.rows.as_deref()
        && let Some(problem) = answer_rows_problem(rows)
    {
        return error(StatusCode::BAD_REQUEST, problem);
    }
    if req
        .sql
        .as_ref()
        .is_some_and(|sql| sql.chars().count() > MAX_SQL_CHARS)
    {
        return error(
            StatusCode::BAD_REQUEST,
            "sql must be at most 20000 characters",
        );
    }
    if req
        .error
        .as_ref()
        .is_some_and(|failure| failure.chars().count() > MAX_ERROR_CHARS)
    {
        return error(
            StatusCode::BAD_REQUEST,
            "error must be at most 2000 characters",
        );
    }
    if req
        .model
        .as_ref()
        .is_some_and(|model| model.chars().count() > MAX_NAME_CHARS)
    {
        return error(
            StatusCode::BAD_REQUEST,
            "model must be at most 200 characters",
        );
    }
    if req
        .endpoint
        .as_ref()
        .is_some_and(|endpoint| endpoint.chars().count() > MAX_NAME_CHARS)
    {
        return error(
            StatusCode::BAD_REQUEST,
            "endpoint must be at most 200 characters",
        );
    }
    if req.tokens.is_some_and(|tokens| tokens < 0) {
        return error(StatusCode::BAD_REQUEST, "tokens must not be negative");
    }
    let (passed, reason) = match verdict(&eval.expected, &req) {
        Ok(()) => (true, None),
        Err(reason) => (false, Some(reason)),
    };
    let who = crate::catalog_docs::caller_identity(&s, caller);
    let sql = format!(
        "INSERT INTO eval_run \
             (eval_id, passed, reason, sql, model, endpoint, tokens, created_by) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING {RUN_COLUMNS}"
    );
    match sqlx::query_as::<_, EvalRun>(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .bind(passed)
        .bind(&reason)
        .bind(&req.sql)
        .bind(&req.model)
        .bind(&req.endpoint)
        .bind(req.tokens)
        .bind(&who.sub)
        .fetch_one(pool)
        .await
    {
        Ok(row) => {
            tracing::info!(
                target: "audit",
                sub = %who.sub,
                eval_id = id,
                id = row.id,
                passed,
                "eval run stored",
            );
            (StatusCode::CREATED, Json(row)).into_response()
        }
        Err(e) => store_failed(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Identity;

    fn principal(sub: &str, groups: &[&str], is_admin: bool) -> Principal {
        Principal {
            identity: Identity {
                sub: sub.into(),
                display_name: sub.into(),
                email: None,
                name: None,
                groups: groups.iter().map(|g| g.to_string()).collect(),
                roles: vec![],
                groups_overage: false,
                is_admin,
            },
            via: "session",
            access_token: None,
            client_id: None,
        }
    }

    #[test]
    fn a_rows_eval_is_accepted() {
        let expected = serde_json::json!({
            "kind": "rows",
            "rows": [[1, "gold", true], [2, null, false]],
        });
        assert_eq!(validate_expected(&expected), Ok(()));
    }

    #[test]
    fn a_bad_expected_is_refused_with_its_sentence() {
        assert_eq!(
            validate_expected(&serde_json::json!(3)),
            Err(BAD_SHAPE.into())
        );
        assert_eq!(
            validate_expected(&serde_json::json!({"kind": "median", "rows": [[1]]})),
            Err(BAD_SHAPE.into())
        );
        let rows: Vec<serde_json::Value> = (0..101).map(|i| serde_json::json!([i])).collect();
        assert_eq!(
            validate_expected(&serde_json::json!({"kind": "rows", "rows": rows})),
            Err(BAD_ROWS.into())
        );
        assert_eq!(
            validate_expected(&serde_json::json!({"kind": "uses", "tables": ["orders"]})),
            Err(BAD_TABLES.into())
        );
    }

    #[test]
    fn a_non_member_may_not_write() {
        let sales = principal("ada", &["sales"], false);
        assert!(!member_of("analytics", Some(&sales)));
        assert!(member_of(
            "analytics",
            Some(&principal("ada", &["analytics"], false))
        ));
        assert!(member_of("analytics", Some(&principal("root", &[], true))));
        assert!(member_of("analytics", None), "dev mode has one person");
    }

    /// An answer with `rows` and nothing else.
    fn answer(rows: Option<Vec<Vec<serde_json::Value>>>) -> NewEvalRun {
        NewEvalRun {
            sql: None,
            rows,
            error: None,
            model: None,
            endpoint: None,
            tokens: None,
        }
    }

    /// An answer that carries only `sql`.
    fn sql_answer(sql: &str) -> NewEvalRun {
        NewEvalRun {
            sql: Some(sql.into()),
            ..answer(None)
        }
    }

    #[test]
    fn a_value_answer_passes_and_a_wrong_one_fails() {
        let expected = serde_json::json!({"kind": "value", "value": 1284});
        assert_eq!(
            verdict(
                &expected,
                &answer(Some(vec![vec![serde_json::json!(1284)]]))
            ),
            Ok(())
        );
        assert_eq!(
            verdict(
                &expected,
                &answer(Some(vec![vec![serde_json::json!(1284.0)]]))
            ),
            Ok(()),
            "numbers compare by value"
        );
        assert_eq!(
            verdict(
                &expected,
                &answer(Some(vec![vec![serde_json::json!(1285)]]))
            ),
            Err(ROWS_DIFFER.into())
        );
        assert_eq!(
            verdict(&expected, &answer(None)),
            Err("the answer had 0 rows, expected 1".into())
        );
        assert_eq!(
            verdict(
                &expected,
                &answer(Some(vec![vec![
                    serde_json::json!(1284),
                    serde_json::json!(1),
                    serde_json::json!(2),
                ]]))
            ),
            Err("the answer had 3 cells, expected 1".into())
        );
    }

    #[test]
    fn rows_compare_as_a_multiset() {
        let expected = serde_json::json!({"kind": "rows", "rows": [[1, "a"], [2, null]]});
        assert_eq!(
            verdict(
                &expected,
                &answer(Some(vec![
                    vec![serde_json::json!(2), serde_json::Value::Null],
                    vec![serde_json::json!(1), serde_json::json!("a")],
                ]))
            ),
            Ok(()),
            "the same two rows in the other order"
        );
        assert_eq!(
            verdict(
                &expected,
                &answer(Some(vec![
                    vec![serde_json::json!(2), serde_json::json!("b")],
                    vec![serde_json::json!(1), serde_json::json!("a")],
                ]))
            ),
            Err(ROWS_DIFFER.into())
        );
    }

    #[test]
    fn uses_names_every_table_and_rejects_a_bigger_name() {
        let expected = serde_json::json!({"kind": "uses", "tables": ["shop.orders"]});
        assert_eq!(
            verdict(&expected, &sql_answer("SELECT count(*) FROM shop.orders")),
            Ok(())
        );
        assert_eq!(
            verdict(
                &expected,
                &sql_answer("SELECT count(*) FROM shop.orders_archive")
            ),
            Err("the SQL does not use shop.orders".into())
        );
        assert_eq!(
            verdict(&expected, &sql_answer("SELECT count(*) FROM myshop.orders")),
            Err("the SQL does not use shop.orders".into())
        );
        assert_eq!(
            verdict(&expected, &answer(None)),
            Err(NO_SQL.into()),
            "an answer without sql cannot show that it uses the table"
        );
    }

    #[test]
    fn an_error_answer_fails_with_its_sentence() {
        let expected = serde_json::json!({"kind": "value", "value": 1});
        let failed = NewEvalRun {
            error: Some(" timeout ".into()),
            ..answer(None)
        };
        assert_eq!(
            verdict(&expected, &failed),
            Err("the answer carried an error: timeout".into())
        );
    }

    #[test]
    fn an_answer_over_the_limits_is_refused() {
        let hundred_and_one: Vec<Vec<serde_json::Value>> =
            (0..101).map(|i| vec![serde_json::json!(i)]).collect();
        assert_eq!(answer_rows_problem(&hundred_and_one), Some(BAD_ANSWER_ROWS));
        let long_cell = vec![vec![serde_json::json!("x".repeat(501))]];
        assert_eq!(answer_rows_problem(&long_cell), Some(BAD_ANSWER_ROWS));
        let largest = vec![vec![serde_json::json!(1); 50]; 100];
        assert_eq!(answer_rows_problem(&largest), None);
    }

    #[test]
    fn a_non_member_may_not_run() {
        let sales = principal("ada", &["sales"], false);
        assert!(!member_of("analytics", Some(&sales)), "{NOT_YOURS_RUN}");
        assert!(member_of(
            "analytics",
            Some(&principal("ada", &["analytics"], false))
        ));
        assert!(member_of("analytics", Some(&principal("root", &[], true))));
        assert!(member_of("analytics", None), "dev mode has one person");
    }
}
