//! Metric definitions (stardelt/stardelt#124, the fourth kind of context): a named number
//! over one table, the SQL expression that computes it, an optional time column and the
//! dimensions it may be cut by.
//!
//! Two rules shape the module:
//!
//! - **A definition is stored, never executed.** The expression is an expression over the
//!   table's columns (`sum(total_cents) / 100.0`), not a statement: it carries no semicolon
//!   and no comment, and the API never runs it. It is the owner's assertion, the same trust
//!   model as a verified query, which only its own check executes.
//! - **The write rule is the documentation's.** Reading a definition needs what reading the
//!   table's documentation needs; writing one (create, delete) needs `doc.can_edit`, the same
//!   rule the table's guide and its verified queries use.
//!
//! Without a state store (`NOVA_DATABASE_URL` unset) the routes answer
//! `503 {"error":"this needs Nova's database, …"}`. A write checks the request itself (400),
//! then the store (503), then the table's rule (403).
//!
//! Every write writes one line on the `audit` target with the caller, the table and the
//! definition's name — never the expression text.

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

/// Most characters a name may carry (the table's CHECK allows the same shape).
const NAME_CHARS: usize = 64;
/// Most characters a description may carry.
const DESCRIPTION_CHARS: usize = 500;
/// Most characters an expression may carry.
const EXPRESSION_CHARS: usize = 2_000;
/// Most dimensions one definition may list.
const MAX_DIMENSIONS: usize = 10;

/// The columns of one row, with `verified_at` as RFC 3339 in UTC.
const ROW_COLUMNS: &str = "id, name, description, expression, time_column, dimensions, \
    verified_by, to_char(verified_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS verified_at";

/// One metric definition as the API returns it.
#[derive(Debug, Serialize, sqlx::FromRow, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricDefinition {
    pub id: i64,
    pub name: String,
    pub description: String,
    /// A SQL expression over the table's columns, e.g. `sum(total_cents) / 100.0`.
    pub expression: String,
    /// The column that carries the time the metric is counted over, or `null`.
    pub time_column: Option<String>,
    /// The columns the metric may be grouped by, in the order the owner wrote them.
    pub dimensions: Vec<String>,
    /// The `sub` of the person who wrote the definition.
    pub verified_by: String,
    /// When it was written, RFC 3339.
    pub verified_at: String,
}

/// `POST /api/catalog/tables/{ns}/{tbl}/metrics` body.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NewMetricDefinition {
    pub name: String,
    pub description: String,
    pub expression: String,
    pub time_column: Option<String>,
    #[serde(default)]
    pub dimensions: Vec<String>,
}

/// What a delete needs to know about a row before it may remove it.
#[derive(sqlx::FromRow)]
struct MetricRef {
    catalog: String,
    namespace: String,
    table_name: String,
    name: String,
}

/// A name is at most 64 characters of lower-case letters, digits and underscores, starting
/// with a letter.
fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    name.chars().count() <= NAME_CHARS
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// A plain column name: a letter or underscore, then letters, digits and underscores.
fn valid_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Enforce the limits the table's CHECK constraints do, so a caller gets a sentence instead
/// of a database error.
fn validate(new: &NewMetricDefinition) -> Result<(), String> {
    if !valid_name(&new.name) {
        return Err("name must start with a lower-case letter and hold only lower-case letters, digits and underscores, at most 64".into());
    }
    let description_chars = new.description.chars().count();
    if !(1..=DESCRIPTION_CHARS).contains(&description_chars) {
        return Err(format!(
            "description must be 1 to {DESCRIPTION_CHARS} characters"
        ));
    }
    let expression_chars = new.expression.chars().count();
    if !(1..=EXPRESSION_CHARS).contains(&expression_chars)
        || new.expression.contains(';')
        || new.expression.contains("--")
        || new.expression.contains("/*")
    {
        return Err(format!(
            "expression must be a SQL expression of 1 to {EXPRESSION_CHARS} characters: no semicolon and no comment"
        ));
    }
    if let Some(time_column) = new.time_column.as_deref()
        && !valid_identifier(time_column)
    {
        return Err("timeColumn must be a column name".into());
    }
    let mut seen: Vec<&str> = Vec::with_capacity(new.dimensions.len());
    for dimension in &new.dimensions {
        if !valid_identifier(dimension) || seen.contains(&dimension.as_str()) {
            return Err(format!(
                "dimensions must be at most {MAX_DIMENSIONS} distinct column names"
            ));
        }
        seen.push(dimension);
    }
    if new.dimensions.len() > MAX_DIMENSIONS {
        return Err(format!(
            "dimensions must be at most {MAX_DIMENSIONS} distinct column names"
        ));
    }
    Ok(())
}

/// The definitions of one table, `name` order. Also read by Ask and MCP (the next slice).
pub(crate) async fn list_rows(
    pool: &PgPool,
    catalog: &str,
    ns: &str,
    tbl: &str,
) -> Result<Vec<MetricDefinition>, sqlx::Error> {
    let sql = format!(
        "SELECT {ROW_COLUMNS} FROM metric_definition \
         WHERE catalog = $1 AND namespace = $2 AND table_name = $3 ORDER BY name"
    );
    sqlx::query_as::<_, MetricDefinition>(sqlx::AssertSqlSafe(sql))
        .bind(catalog)
        .bind(ns)
        .bind(tbl)
        .fetch_all(pool)
        .await
}

/// Insert one definition; a duplicate name is the unique index's error (`store_failed`).
pub(crate) async fn insert_row(
    pool: &PgPool,
    catalog: &str,
    ns: &str,
    tbl: &str,
    new: &NewMetricDefinition,
    sub: &str,
) -> Result<MetricDefinition, sqlx::Error> {
    let sql = format!(
        "INSERT INTO metric_definition \
         (catalog, namespace, table_name, name, description, expression, time_column, dimensions, verified_by) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING {ROW_COLUMNS}"
    );
    sqlx::query_as::<_, MetricDefinition>(sqlx::AssertSqlSafe(sql))
        .bind(catalog)
        .bind(ns)
        .bind(tbl)
        .bind(&new.name)
        .bind(&new.description)
        .bind(&new.expression)
        .bind(&new.time_column)
        .bind(new.dimensions.clone())
        .bind(sub)
        .fetch_one(pool)
        .await
}

/// Remove one definition; `false` when no row has that id.
pub(crate) async fn delete_row(pool: &PgPool, id: i64) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("DELETE FROM metric_definition WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() == 1)
}

/// `GET /api/catalog/tables/{ns}/{tbl}/metrics` — the table's, in `name` order.
#[utoipa::path(
    get,
    operation_id = "metric_definitions_list",
    path = "/api/catalog/tables/{ns}/{tbl}/metrics",
    tag = "public",
    description = "The metric definitions of one table: named numbers its owners declared over its columns, with the SQL expression that computes each, an optional time column and the dimensions it may be cut by. A definition is stored, never executed, by this API. In name order, no pagination: a table with more than 50 metrics is not a shape this milestone meets. A table the caller may not see answers what the documentation route answers. `?catalog=` picks the catalog.",
    params(
        ("ns" = String, Path, description = "Namespace (schema) name"),
        ("tbl" = String, Path, description = "Table name"),
        crate::catalog::CatalogParam,
    ),
    responses(
        (status = 200, description = "The table's metric definitions, in name order", body = Vec<MetricDefinition>),
        (status = 400, description = "A name is not a single path segment, or the catalog name is invalid", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog or no such table", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn list(
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
    match list_rows(pool, &name, &ns, &tbl).await {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => store_failed(e),
    }
}

/// `POST /api/catalog/tables/{ns}/{tbl}/metrics` — `201` with the stored row.
#[utoipa::path(
    post,
    operation_id = "metric_definitions_create",
    path = "/api/catalog/tables/{ns}/{tbl}/metrics",
    tag = "public",
    description = "Declare one metric of this table: a name (lower-case letters, digits and underscores, at most 64), a description (1 to 500 characters), the SQL expression that computes it (at most 2000 characters, no semicolon and no comment), an optional time column and up to 10 distinct dimensions. The expression is stored, never executed: it is the owner's assertion. Only the owner group of the table and administrators may add one; the caller is recorded as `verifiedBy`. `?catalog=` picks the catalog.",
    params(
        ("ns" = String, Path, description = "Namespace (schema) name"),
        ("tbl" = String, Path, description = "Table name"),
        crate::catalog::CatalogParam,
    ),
    request_body = NewMetricDefinition,
    responses(
        (status = 201, description = "The stored row", body = MetricDefinition),
        (status = 400, description = "The name, description, expression, timeColumn or dimensions are not in the accepted shape, or the catalog name is invalid", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog, or only the owner group and administrators may add a metric definition", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog or no such table", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn create(
    State(s): State<Arc<AppState>>,
    Path((ns, tbl)): Path<(String, String)>,
    Query(q): Query<CatalogParam>,
    caller: Option<Extension<Principal>>,
    ApiJson(req): ApiJson<NewMetricDefinition>,
) -> Response {
    if let Err(message) = validate(&req) {
        return error(StatusCode::BAD_REQUEST, &message);
    }
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
    match insert_row(pool, &name, &ns, &tbl, &req, &who.sub).await {
        Ok(row) => {
            tracing::info!(
                target: "audit",
                sub = %who.sub,
                catalog = %name,
                table = %format!("{ns}.{tbl}"),
                name = %row.name,
                "metric definition created",
            );
            (StatusCode::CREATED, Json(row)).into_response()
        }
        Err(e) => store_failed(e),
    }
}

/// `DELETE /api/metrics/{id}` — `204`; `404` when the id is not a row.
#[utoipa::path(
    delete,
    operation_id = "metric_definitions_delete",
    path = "/api/metrics/{id}",
    tag = "public",
    description = "Remove one metric definition; it was stored, never executed. Only the owner group of its table and administrators may delete it.",
    params(("id" = i64, Path, description = "The metric definition's id")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 403, description = "Only the owner group and administrators may delete a metric definition", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such metric definition, or the id is not a number", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn delete(
    State(s): State<Arc<AppState>>,
    Path(id): Path<String>,
    caller: Option<Extension<Principal>>,
) -> Response {
    let Ok(id) = id.parse::<i64>() else {
        return not_found();
    };
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let row = match sqlx::query_as::<_, MetricRef>(
        "SELECT catalog, namespace, table_name, name FROM metric_definition WHERE id = $1",
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
    match delete_row(pool, id).await {
        Ok(true) => {
            tracing::info!(
                target: "audit",
                sub = %sub,
                catalog = %name,
                table = %format!("{}.{}", row.namespace, row.table_name),
                name = %row.name,
                id,
                "metric definition deleted",
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => not_found(),
        Err(e) => store_failed(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new(name: &str) -> NewMetricDefinition {
        NewMetricDefinition {
            name: name.into(),
            description: "total revenue in currency units".into(),
            expression: "sum(total_cents) / 100.0".into(),
            time_column: None,
            dimensions: vec![],
        }
    }

    #[test]
    fn a_name_that_is_not_a_slug_is_refused() {
        assert!(validate(&new("revenue")).is_ok());
        assert!(validate(&new("Revenue")).is_err());
        assert!(validate(&new("1_revenue")).is_err());
    }

    #[test]
    fn a_semicolon_is_refused() {
        let mut definition = new("revenue");
        definition.expression = "sum(x); drop table t".into();
        assert!(validate(&definition).is_err());
    }

    #[test]
    fn dimensions_are_plain_identifiers() {
        let mut definition = new("revenue");
        definition.dimensions = vec!["order date".into()];
        assert!(validate(&definition).is_err());
        definition.dimensions = vec!["country".into()];
        assert!(validate(&definition).is_ok());
    }

    #[test]
    fn the_time_column_is_optional() {
        let mut definition = new("revenue");
        definition.time_column = None;
        assert!(validate(&definition).is_ok());
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn rows_are_written_listed_and_deleted() {
        let pool = crate::db::test_support::migrated_pool().await;
        let mut revenue = new("revenue");
        revenue.dimensions = vec!["country".into()];
        let mut orders = new("orders");
        orders.expression = "count(*)".into();
        let stored = insert_row(&pool, "warehouse", "sales", "orders", &revenue, "sub-a")
            .await
            .expect("insert revenue");
        assert_eq!(stored.name, "revenue");
        assert_eq!(stored.dimensions, vec!["country".to_string()]);
        insert_row(&pool, "warehouse", "sales", "orders", &orders, "sub-a")
            .await
            .expect("insert orders");
        let rows = list_rows(&pool, "warehouse", "sales", "orders")
            .await
            .expect("list rows");
        let names: Vec<&str> = rows.iter().map(|row| row.name.as_str()).collect();
        assert_eq!(names, vec!["orders", "revenue"]);
        assert!(delete_row(&pool, stored.id).await.expect("delete"));
        assert!(!delete_row(&pool, stored.id).await.expect("delete again"));
    }
}
