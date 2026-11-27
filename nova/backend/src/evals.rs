//! Evals (stardelt/stardelt#125, slice 1): a question with a known answer, kept next to
//! the catalog's other context and served by the API.
//!
//! This slice stores the eval and serves it; the run that asks Ask and compares the answer
//! is slice 2. The API never executes a stored eval — the expected answer is the owner's
//! assertion, like a verified query's expected result.
//!
//! `expected` is one of three shapes, and anything else is refused with a sentence:
//!
//! - `{"kind":"rows","rows":[[…],…]}` — the answer is this small table.
//! - `{"kind":"value","value":<scalar>}` — the answer is `1284`.
//! - `{"kind":"uses","tables":["shop.orders", …]}` — the SQL must use these tables.
//!
//! Reading needs what reading the catalog's products needs ([`crate::catalog::catalog_scope`]),
//! because an eval is catalog context like a guide. Writing and deleting need the caller to be
//! a member of the row's `ownerGroup` or an administrator.
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
}
