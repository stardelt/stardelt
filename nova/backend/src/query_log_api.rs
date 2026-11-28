//! Reading the query log (decision D25, stardelt-nova#116): `GET /api/query-log` and
//! `GET /api/query-log/{id}`.
//!
//! A person sees their own queries; an administrator can see everyone's (`scope=all`) and is
//! audited when opening somebody else's statement; the `team` scope holds the queries of the
//! service principals the person's group owns. The rule itself lives in `query_log.rs`; the SQL
//! here only narrows rows, and every row is checked against that rule again before it leaves.
//! Somebody else's id answers `404`, exactly like a missing one.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use kube::api::{Api, ApiResource, DynamicObject, GroupVersionKind, ListParams};
use serde::Serialize;
use sqlx::{FromRow, PgPool, Postgres, QueryBuilder};
use utoipa::ToSchema;

use crate::AppState;
use crate::auth::Principal;
use crate::people;
use crate::query_hints;
use crate::query_log::{RowOwner, Scope, Viewer, in_scope, redact_literals, visible};
use crate::saved_queries::{error, not_found, store_failed, unavailable};

const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 200;
const MAX_SEARCH_CHARS: usize = 200;
/// The statement preview in the list; the detail has the whole text.
const PREVIEW_CHARS: i32 = 300;

/// How long the subject→owner lookup is reused. A principal is created rarely; a stale
/// minute only costs one member not seeing a brand-new principal's queries yet.
const OWNER_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// `status.subject -> spec.ownerGroup` for the `ServicePrincipal` resources of Nova's
/// namespace. A resource without `status.subject` (external identity mode, or a principal
/// the operator has not converged yet) is skipped: its queries stay visible to
/// administrators only. A missing cluster client, a refused list or a read that is not JSON
/// yields an empty map — the team scope then shows nothing, never an error.
pub fn owner_groups(principals: &[serde_json::Value]) -> HashMap<String, String> {
    principals
        .iter()
        .filter_map(|p| {
            let subject = p["status"]["subject"].as_str()?.trim();
            let group = p["spec"]["ownerGroup"].as_str()?.trim();
            (!subject.is_empty() && !group.is_empty())
                .then(|| (subject.to_string(), group.to_string()))
        })
        .collect()
}

async fn principal_owners(s: &AppState) -> HashMap<String, String> {
    if let Some((at, map)) = s
        .principal_owners
        .lock()
        .expect("principal owners poisoned")
        .as_ref()
        && at.elapsed() < OWNER_TTL
    {
        return map.clone();
    }
    let Some(kube) = s.kube.clone() else {
        return HashMap::new();
    };
    let api: Api<DynamicObject> = Api::namespaced_with(
        kube,
        &crate::cluster::namespace(),
        &ApiResource::from_gvk(&GroupVersionKind::gvk(
            "stardelt.io",
            "v1alpha1",
            "ServicePrincipal",
        )),
    );
    let map = match api.list(&ListParams::default()).await {
        Ok(list) => owner_groups(
            &list
                .items
                .iter()
                .filter_map(|o| serde_json::to_value(o).ok())
                .collect::<Vec<_>>(),
        ),
        Err(e) => {
            tracing::warn!(error = %e, "cannot list ServicePrincipals; the team scope stays empty");
            HashMap::new()
        }
    };
    *s.principal_owners
        .lock()
        .expect("principal owners poisoned") = Some((std::time::Instant::now(), map.clone()));
    map
}

/// Subject → name, for the `by=principal` usage rows. `query_log.principal` holds the OIDC
/// subject for every signed-in query (measured on the lab 2026-11-17: 1211 demo rows under its
/// sub, 0 under `demo`), and rows written before that still carry the sign-in name.
async fn person_names(pool: &PgPool) -> Result<HashMap<String, String>, sqlx::Error> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT sub, COALESCE(NULLIF(display_name, ''), username) FROM person")
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().collect())
}

/// Replace each key that names a person's subject with that person's name. A key that matches no
/// person stays exactly as the log wrote it — the `superset` engine user, or someone the
/// directory no longer has; an honest literal beats a blank cell.
fn name_principals(items: &mut [UsageRow], names: &HashMap<String, String>) {
    for item in items {
        if let Some(name) = names.get(&item.key) {
            item.key = name.clone();
        }
    }
}

/// The person asking, with owned data (a [`Viewer`] borrows it).
struct Caller {
    username: String,
    sub: String,
    groups: Vec<String>,
    is_admin: bool,
}

impl Caller {
    /// In `oidc` mode the engine user is the token's `preferred_username`, which the identity
    /// keeps as `display_name`. In `dev` mode there is one user and he is the operator.
    fn of(principal: Option<&Principal>, dev_user: &str) -> Self {
        match principal {
            Some(p) => Self {
                username: p.identity.display_name.clone(),
                sub: p.identity.sub.clone(),
                groups: p.identity.groups.clone(),
                is_admin: p.identity.is_admin,
            },
            None => {
                let dev = crate::auth::Identity::dev_admin(dev_user);
                Self {
                    username: dev.display_name,
                    sub: dev.sub,
                    groups: dev.groups,
                    is_admin: dev.is_admin,
                }
            }
        }
    }

    fn viewer(&self) -> Viewer<'_> {
        Viewer {
            username: &self.username,
            sub: &self.sub,
            groups: &self.groups,
            is_admin: self.is_admin,
        }
    }
}

/// Is this row the caller's own? The engine user is the `sub` once authz 8/8 is live, and the
/// sign-in name before it; both count, exactly as `Scope::Mine` does.
fn is_own_row(principal: &str, who: &Caller) -> bool {
    principal == who.username || principal == who.sub
}

/// The checked query string of the list route.
#[derive(Debug, PartialEq)]
struct Params {
    scope: Scope,
    state: Option<String>,
    warehouse: Option<String>,
    search: Option<String>,
    from_ms: Option<i64>,
    to_ms: Option<i64>,
    limit: i64,
    cursor: Option<(i64, String)>,
}

fn parse_params(raw: &HashMap<String, String>) -> Result<Params, String> {
    let scope = match raw.get("scope").map(String::as_str) {
        None | Some("mine") => Scope::Mine,
        Some("team") => Scope::Team,
        Some("all") => Scope::All,
        Some(_) => return Err("scope must be mine, team or all".into()),
    };
    let state = match raw.get("state").map(|s| s.to_ascii_uppercase()) {
        None => None,
        Some(s) if ["QUEUED", "FINISHED", "FAILED", "CANCELED"].contains(&s.as_str()) => Some(s),
        Some(_) => return Err("state must be QUEUED, FINISHED, FAILED or CANCELED".into()),
    };
    let warehouse = raw.get("warehouse").filter(|w| !w.is_empty()).cloned();
    if warehouse.as_ref().is_some_and(|w| w.len() > 63) {
        return Err("warehouse is too long".into());
    }
    let search = raw
        .get("q")
        .map(|q| q.trim().to_string())
        .filter(|q| !q.is_empty());
    if search
        .as_ref()
        .is_some_and(|q| q.chars().count() > MAX_SEARCH_CHARS)
    {
        return Err(format!("q must be at most {MAX_SEARCH_CHARS} characters"));
    }
    let number = |name: &str| -> Result<Option<i64>, String> {
        raw.get(name)
            .map(|v| {
                v.parse::<i64>()
                    .map_err(|_| format!("{name} must be a number (epoch milliseconds)"))
            })
            .transpose()
    };
    let limit = match raw.get("limit") {
        None => DEFAULT_LIMIT,
        Some(v) => match v.parse::<i64>() {
            Ok(n) if (1..=MAX_LIMIT).contains(&n) => n,
            _ => return Err(format!("limit must be between 1 and {MAX_LIMIT}")),
        },
    };
    let cursor = raw
        .get("cursor")
        .map(|c| decode_cursor(c).ok_or_else(|| "invalid cursor".to_string()))
        .transpose()?;
    Ok(Params {
        scope,
        state,
        warehouse,
        search,
        from_ms: number("from")?,
        to_ms: number("to")?,
        limit,
        cursor,
    })
}

#[derive(Debug, PartialEq)]
struct RepeatedParams {
    scope: Scope,
    limit: i64,
}

fn parse_repeated_params(raw: &HashMap<String, String>) -> Result<RepeatedParams, String> {
    let scope = match raw.get("scope").map(String::as_str) {
        None | Some("mine") => Scope::Mine,
        Some("team") => Scope::Team,
        Some("all") => Scope::All,
        Some(_) => return Err("scope must be mine, team or all".into()),
    };
    let limit = match raw.get("limit") {
        None => 20,
        Some(value) => match value.parse::<i64>() {
            Ok(n) if (1..=100).contains(&n) => n,
            _ => return Err("limit must be a number from 1 to 100".into()),
        },
    };
    Ok(RepeatedParams { scope, limit })
}

/// `created_at` in microseconds plus the id: the keyset position after the last item of a page.
fn encode_cursor(created_us: i64, id: &str) -> String {
    URL_SAFE_NO_PAD.encode(format!("{created_us}:{id}"))
}

fn decode_cursor(cursor: &str) -> Option<(i64, String)> {
    let text = String::from_utf8(URL_SAFE_NO_PAD.decode(cursor).ok()?).ok()?;
    let (us, id) = text.split_once(':')?;
    (!id.is_empty()).then(|| Some((us.parse().ok()?, id.to_string())))?
}

/// The checked query string of the export route. Unlike the list, the order is ascending,
/// so a writer can keep one keyset position and continue from it after a restart.
#[derive(Debug, PartialEq)]
struct ExportParams {
    from_ms: Option<i64>,
    cursor: Option<(i64, String)>,
    limit: i64,
}

const DEFAULT_EXPORT_LIMIT: i64 = 500;
const MAX_EXPORT_LIMIT: i64 = 1000;

fn parse_export_params(raw: &HashMap<String, String>) -> Result<ExportParams, String> {
    let from_ms = raw
        .get("from")
        .map(|v| {
            v.parse::<i64>()
                .map_err(|_| "from must be a number (epoch milliseconds)".to_string())
        })
        .transpose()?;
    let cursor = raw
        .get("cursor")
        .map(|c| decode_cursor(c).ok_or_else(|| "invalid cursor".to_string()))
        .transpose()?;
    let limit = match raw.get("limit") {
        None => DEFAULT_EXPORT_LIMIT,
        Some(v) => match v.parse::<i64>() {
            Ok(n) if (1..=MAX_EXPORT_LIMIT).contains(&n) => n,
            _ => return Err("limit must be between 1 and 1000".into()),
        },
    };
    Ok(ExportParams {
        from_ms,
        cursor,
        limit,
    })
}

/// `%`, `_` and `\` in a search text are literals, not wildcards.
fn like_pattern(text: &str) -> String {
    let mut out = String::from("%");
    for c in text.chars() {
        if matches!(c, '%' | '_' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('%');
    out
}

#[derive(FromRow, Debug)]
struct ListRow {
    query_id: String,
    state: String,
    principal: String,
    source: Option<String>,
    warehouse: String,
    query_type: Option<String>,
    sql_preview: String,
    created_us: i64,
    wall_ms: Option<i64>,
    output_rows: Option<i64>,
    error_code: Option<String>,
}

/// One row of the query log list; the statement is cut to a preview.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryLogItem {
    id: String,
    state: String,
    /// The raw engine user (the `sub` once authz 8/8 is live).
    user: String,
    /// The person's display name when Nova knows the engine user; `None` falls back to
    /// `user` in the UI.
    user_name: Option<String>,
    source: Option<String>,
    warehouse: String,
    query_type: Option<String>,
    sql: String,
    created_at: i64,
    wall_ms: Option<i64>,
    output_rows: Option<i64>,
    error_code: Option<String>,
}

impl From<ListRow> for QueryLogItem {
    fn from(r: ListRow) -> Self {
        Self {
            id: r.query_id,
            state: r.state,
            user: r.principal,
            user_name: None,
            source: r.source,
            warehouse: r.warehouse,
            query_type: r.query_type,
            sql: r.sql_preview,
            created_at: r.created_us / 1000,
            wall_ms: r.wall_ms,
            output_rows: r.output_rows,
            error_code: r.error_code,
        }
    }
}

/// One page of the query log; `nextCursor` is null on the last page.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryLogPage {
    items: Vec<QueryLogItem>,
    next_cursor: Option<String>,
}

/// `GET /api/query-log?scope=mine|team|all&state=&warehouse=&q=&from=&to=&limit=&cursor=`
#[utoipa::path(
    get,
    operation_id = "query_log_list",
    path = "/api/query-log",
    tag = "public",
    description = "The statements run on the engine, newest first, a page at a time. A person sees their own; an administrator may ask for everyone's with scope=all. Times are epoch milliseconds. With NOVA_QUERY_LOG_REDACT_LITERALS=1 somebody else's statement shows '?' in place of every string literal.",
    params(
        ("scope" = Option<String>, Query, description = "mine (default), team or all. team is the queries of the service principals owned by the caller's groups; all is for administrators"),
        ("state" = Option<String>, Query, description = "QUEUED, FINISHED, FAILED or CANCELED, in any case"),
        ("warehouse" = Option<String>, Query, description = "Only queries that ran on this warehouse"),
        ("q" = Option<String>, Query, description = "Text inside the statement, or an exact query id; at most 200 characters"),
        ("from" = Option<i64>, Query, description = "Created at or after this time"),
        ("to" = Option<i64>, Query, description = "Created at or before this time"),
        ("limit" = Option<i64>, Query, description = "Page size from 1 to 200; 50 by default"),
        ("cursor" = Option<String>, Query, description = "The nextCursor of the previous page"),
    ),
    responses(
        (status = 200, description = "One page of the log", body = QueryLogPage),
        (status = 400, description = "A parameter is not valid; the message names it", body = crate::openapi::ErrorResponse),
        (status = 403, description = "scope=all by somebody who is not an administrator", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn list(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Query(raw): Query<HashMap<String, String>>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let who = Caller::of(caller.as_ref().map(|c| &c.0), &s.dev_user);
    let params = match parse_params(&raw) {
        Ok(p) => p,
        Err(message) => return error(StatusCode::BAD_REQUEST, &message),
    };
    if params.scope == Scope::All && !who.is_admin {
        return error(StatusCode::FORBIDDEN, "the all scope is for administrators");
    }
    let owners = if params.scope == Scope::Team {
        principal_owners(&s).await
    } else {
        HashMap::new()
    };
    if params.scope == Scope::Team && owners.is_empty() {
        return Json(serde_json::json!({ "items": [], "nextCursor": null })).into_response();
    }

    let mut q: QueryBuilder<Postgres> = QueryBuilder::new(format!(
        "SELECT query_id, state, principal, source, warehouse, query_type, \
         left(sql_text, {PREVIEW_CHARS}) AS sql_preview, \
         (extract(epoch FROM created_at) * 1000000)::bigint AS created_us, \
         wall_ms, output_rows, error_code FROM query_log WHERE true"
    ));
    if params.scope == Scope::Mine {
        // The engine user is the sub once authz 8/8 is live; rows written before it (or in a
        // deployment that has not switched) still carry the sign-in name.
        q.push(" AND (principal = ")
            .push_bind(who.username.clone())
            .push(" OR principal = ")
            .push_bind(who.sub.clone())
            .push(")");
    }
    if params.scope == Scope::Team {
        let subjects: Vec<String> = owners.keys().cloned().collect();
        q.push(" AND principal = ANY(")
            .push_bind(subjects)
            .push(")");
    }
    if let Some(state) = &params.state {
        q.push(" AND state = ").push_bind(state.clone());
    }
    if let Some(w) = &params.warehouse {
        q.push(" AND warehouse = ").push_bind(w.clone());
    }
    if let Some(ms) = params.from_ms {
        q.push(" AND created_at >= 'epoch'::timestamptz + (")
            .push_bind(ms)
            .push("::bigint * interval '1 millisecond')");
    }
    if let Some(ms) = params.to_ms {
        q.push(" AND created_at <= 'epoch'::timestamptz + (")
            .push_bind(ms)
            .push("::bigint * interval '1 millisecond')");
    }
    if let Some(text) = &params.search {
        q.push(" AND (sql_text ILIKE ")
            .push_bind(like_pattern(text))
            .push(" ESCAPE '\\' OR query_id = ")
            .push_bind(text.clone())
            .push(")");
    }
    if let Some((us, id)) = &params.cursor {
        q.push(" AND (created_at, query_id) < ('epoch'::timestamptz + (")
            .push_bind(*us)
            .push("::bigint * interval '1 microsecond'), ")
            .push_bind(id.clone())
            .push(")");
    }
    q.push(" ORDER BY created_at DESC, query_id DESC LIMIT ")
        .push_bind(params.limit + 1);

    let mut rows: Vec<ListRow> = match q.build_query_as().fetch_all(pool).await {
        Ok(rows) => rows,
        Err(e) => return store_failed(e),
    };
    let more = rows.len() as i64 > params.limit;
    rows.truncate(params.limit as usize);
    let viewer = who.viewer();
    let before = rows.len();
    rows.retain(|r| {
        let owner = RowOwner {
            principal: &r.principal,
            owner_group: owners.get(&r.principal).map(String::as_str),
        };
        in_scope(params.scope, &owner, &viewer)
    });
    if rows.len() != before {
        tracing::warn!(
            dropped = before - rows.len(),
            "query-log rows outside the caller's scope were dropped"
        );
    }
    let next = if more {
        rows.last()
            .map(|r| encode_cursor(r.created_us, &r.query_id))
    } else {
        None
    };
    let mut items: Vec<QueryLogItem> = rows.into_iter().map(QueryLogItem::from).collect();
    // An engine user that is a UUID is a person's subject; the directory turns it into a
    // name. Deduplicated, because a page is often the same person's queries.
    let mut subs: Vec<String> = items
        .iter()
        .filter(|item| people::is_uuid(&item.user))
        .map(|item| item.user.clone())
        .collect();
    subs.sort();
    subs.dedup();
    if !subs.is_empty() {
        let display = people::names(pool, &subs).await;
        for item in &mut items {
            item.user_name = display.get(&item.user).cloned();
        }
    }
    if s.redact_query_log_literals {
        for item in &mut items {
            if !is_own_row(&item.user, &who) {
                item.sql = redact_literals(&item.sql);
            }
        }
    }
    Json(QueryLogPage {
        items,
        next_cursor: next,
    })
    .into_response()
}

/// The window of the usage route when the caller names none: the last seven days.
const DEFAULT_WINDOW_MS: i64 = 7 * 86_400_000;
/// The usage answer is a summary, not a log: the biggest 100 keys are enough.
const USAGE_LIMIT: i64 = 100;

/// What the usage route adds up by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum By {
    Principal,
    Warehouse,
    Day,
    Group,
}

impl By {
    fn as_str(self) -> &'static str {
        match self {
            Self::Principal => "principal",
            Self::Warehouse => "warehouse",
            Self::Day => "day",
            Self::Group => "group",
        }
    }
}

/// The checked query string of the usage route; the window is always complete.
#[derive(Debug, PartialEq)]
struct UsageParams {
    scope: Scope,
    warehouse: Option<String>,
    by: By,
    from_ms: i64,
    to_ms: i64,
}

fn parse_usage_params(raw: &HashMap<String, String>, now_ms: i64) -> Result<UsageParams, String> {
    let params = parse_params(raw)?;
    let by = match raw.get("by").map(String::as_str) {
        None | Some("principal") => By::Principal,
        Some("warehouse") => By::Warehouse,
        Some("day") => By::Day,
        Some("group") => By::Group,
        Some(_) => return Err("by must be principal, warehouse, day or group".into()),
    };
    Ok(UsageParams {
        scope: params.scope,
        warehouse: params.warehouse,
        by,
        from_ms: params.from_ms.unwrap_or(now_ms - DEFAULT_WINDOW_MS),
        to_ms: params.to_ms.unwrap_or(now_ms),
    })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The optional showback rate: CPU time at a price, an estimate and never a bill.
#[derive(Debug, Clone, PartialEq)]
pub struct Showback {
    pub cpu_hour_price: f64,
    pub currency: String,
}

impl Showback {
    /// Both install settings or neither (`NOVA_CPU_HOUR_PRICE`, `NOVA_CPU_HOUR_CURRENCY`).
    pub fn from_env() -> Result<Option<Self>, String> {
        parse_showback(
            std::env::var("NOVA_CPU_HOUR_PRICE").ok().as_deref(),
            std::env::var("NOVA_CPU_HOUR_CURRENCY").ok().as_deref(),
        )
    }
}

/// The parsing half of [`Showback::from_env`], so it is testable without touching the
/// process environment.
fn parse_showback(price: Option<&str>, currency: Option<&str>) -> Result<Option<Showback>, String> {
    let (raw_price, raw_currency) = match (price, currency) {
        (None, None) => return Ok(None),
        (Some(_), None) | (None, Some(_)) => {
            return Err(
                "NOVA_CPU_HOUR_PRICE and NOVA_CPU_HOUR_CURRENCY must be set together".into(),
            );
        }
        (Some(p), Some(c)) => (p.trim(), c),
    };
    let cpu_hour_price: f64 = raw_price
        .parse()
        .map_err(|_| format!("NOVA_CPU_HOUR_PRICE is not a positive number: {raw_price}"))?;
    if !cpu_hour_price.is_finite() || cpu_hour_price <= 0.0 {
        return Err(format!(
            "NOVA_CPU_HOUR_PRICE is not a positive number: {raw_price}"
        ));
    }
    // Not lowercased: a currency is shown as the administrator wrote it.
    let currency = raw_currency.trim();
    if currency.is_empty() || currency.chars().count() > 8 {
        return Err(format!(
            "NOVA_CPU_HOUR_CURRENCY must be 1 to 8 characters, got: {currency}"
        ));
    }
    Ok(Some(Showback {
        cpu_hour_price,
        currency: currency.to_string(),
    }))
}

/// CPU time at the configured rate, rounded to cents. `None` when showback is off.
fn estimate(cpu_ms: i64, showback: Option<&Showback>) -> Option<f64> {
    let value = cpu_ms as f64 / 3_600_000.0 * showback?.cpu_hour_price;
    Some((value * 100.0).round() / 100.0)
}

/// One line of the usage answer: a person, a warehouse or a day, and what it cost.
#[derive(Debug, Serialize, ToSchema, FromRow)]
#[serde(rename_all = "camelCase")]
#[schema(as = QueryUsageRow)]
pub struct UsageRow {
    /// The principal (engine user), the warehouse name, the UTC day as `YYYY-MM-DD`, or the
    /// group a person's token carried.
    key: String,
    queries: i64,
    /// How many of `queries` failed.
    failed: i64,
    cpu_ms: i64,
    wall_ms: i64,
    input_bytes: i64,
    written_bytes: i64,
    /// CPU time at the configured price, rounded to cents; `null` when no price is configured.
    #[sqlx(default)]
    estimated_cost: Option<f64>,
}

impl UsageRow {
    fn total(items: &[UsageRow], showback: Option<&Showback>) -> Self {
        let cpu_ms: i64 = items.iter().map(|i| i.cpu_ms).sum();
        Self {
            key: "total".into(),
            queries: items.iter().map(|i| i.queries).sum(),
            failed: items.iter().map(|i| i.failed).sum(),
            cpu_ms,
            wall_ms: items.iter().map(|i| i.wall_ms).sum(),
            input_bytes: items.iter().map(|i| i.input_bytes).sum(),
            written_bytes: items.iter().map(|i| i.written_bytes).sum(),
            estimated_cost: estimate(cpu_ms, showback),
        }
    }
}

/// The rate `estimatedCost` uses; absent when showback is off.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UsagePrice {
    cpu_hour_price: f64,
    currency: String,
}

impl From<&Showback> for UsagePrice {
    fn from(s: &Showback) -> Self {
        Self {
            cpu_hour_price: s.cpu_hour_price,
            currency: s.currency.clone(),
        }
    }
}

/// Compute and bytes added up, with the window that was used.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UsagePage {
    /// principal, warehouse, day or group.
    by: String,
    /// The window that was added up, epoch milliseconds.
    from: i64,
    to: i64,
    items: Vec<UsageRow>,
    /// Every item added together; its `key` is `total`.
    total: UsageRow,
    /// The rate `estimatedCost` uses; absent when showback is off.
    price: Option<UsagePrice>,
}

impl UsagePage {
    fn empty(by: By, from_ms: i64, to_ms: i64, showback: Option<&Showback>) -> Self {
        Self {
            by: by.as_str().into(),
            from: from_ms,
            to: to_ms,
            items: vec![],
            total: UsageRow::total(&[], showback),
            price: showback.map(UsagePrice::from),
        }
    }
}

/// The totals row of a `by=group` window, read by its own query so a person in two groups is
/// not counted twice. It has no `key` column; [`UsageTotals::into_row`] names it `total`.
#[derive(FromRow)]
struct UsageTotals {
    queries: i64,
    failed: i64,
    cpu_ms: i64,
    wall_ms: i64,
    input_bytes: i64,
    written_bytes: i64,
}

impl UsageTotals {
    fn into_row(self, showback: Option<&Showback>) -> UsageRow {
        UsageRow {
            key: "total".into(),
            queries: self.queries,
            failed: self.failed,
            cpu_ms: self.cpu_ms,
            wall_ms: self.wall_ms,
            input_bytes: self.input_bytes,
            written_bytes: self.written_bytes,
            estimated_cost: estimate(self.cpu_ms, showback),
        }
    }
}

/// The scope, warehouse and window predicates of the usage route, in one place so the rows and
/// the `by=group` total cannot drift.
fn push_usage_predicates(
    q: &mut QueryBuilder<Postgres>,
    params: &UsageParams,
    who: &Caller,
    owners: &HashMap<String, String>,
) {
    if params.scope == Scope::Mine {
        q.push(" AND (principal = ")
            .push_bind(who.username.clone())
            .push(" OR principal = ")
            .push_bind(who.sub.clone())
            .push(")");
    }
    if params.scope == Scope::Team {
        let subjects: Vec<String> = owners.keys().cloned().collect();
        q.push(" AND principal = ANY(")
            .push_bind(subjects)
            .push(")");
    }
    if let Some(w) = &params.warehouse {
        q.push(" AND warehouse = ").push_bind(w.clone());
    }
    q.push(" AND created_at >= 'epoch'::timestamptz + (")
        .push_bind(params.from_ms)
        .push("::bigint * interval '1 millisecond')");
    q.push(" AND created_at <= 'epoch'::timestamptz + (")
        .push_bind(params.to_ms)
        .push("::bigint * interval '1 millisecond')");
}

/// `GET /api/query-log/usage?scope=&by=&warehouse=&from=&to=` — compute and bytes added up.
#[utoipa::path(
    get,
    operation_id = "query_log_usage",
    path = "/api/query-log/usage",
    tag = "public",
    description = "Compute and bytes added up per person, warehouse, day or group, with the query log's visibility: your own, your team's service principals, or everyone's for administrators. A group is the `groups` claim the person's token carried at the last write to the person directory; a principal the directory does not know, such as the superset service account, and a person with no group fall into one honest 'unknown' row. A person in several groups appears in each of them, so the total counts every query once. The window defaults to the last 7 days. When a price per CPU-hour is configured, each row carries an estimated cost — an estimate, never a bill.",
    params(
        ("scope" = Option<String>, Query, description = "mine (default), team or all. team is the service principals owned by the caller's groups; all is for administrators"),
        ("by" = Option<String>, Query, description = "principal (default), warehouse, day (UTC) or group (the groups the sign-in token carried)"),
        ("warehouse" = Option<String>, Query, description = "Only usage of this warehouse"),
        ("from" = Option<i64>, Query, description = "Created at or after this time; the last 7 days by default"),
        ("to" = Option<i64>, Query, description = "Created at or before this time; now by default"),
    ),
    responses(
        (status = 200, description = "The rows of the window and their total", body = UsagePage),
        (status = 400, description = "A parameter is not valid; the message names it", body = crate::openapi::ErrorResponse),
        (status = 403, description = "scope=all by somebody who is not an administrator", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn usage(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Query(raw): Query<HashMap<String, String>>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let who = Caller::of(caller.as_ref().map(|c| &c.0), &s.dev_user);
    let params = match parse_usage_params(&raw, now_ms()) {
        Ok(p) => p,
        Err(message) => return error(StatusCode::BAD_REQUEST, &message),
    };
    if params.scope == Scope::All && !who.is_admin {
        return error(StatusCode::FORBIDDEN, "the all scope is for administrators");
    }
    let owners = if params.scope == Scope::Team {
        principal_owners(&s).await
    } else {
        HashMap::new()
    };
    if params.scope == Scope::Team && owners.is_empty() {
        return Json(UsagePage::empty(
            params.by,
            params.from_ms,
            params.to_ms,
            s.showback.as_ref(),
        ))
        .into_response();
    }

    let key = match params.by {
        By::Principal => "principal",
        By::Warehouse => "warehouse",
        By::Day => "to_char(date_trunc('day', created_at AT TIME ZONE 'UTC'), 'YYYY-MM-DD')",
        // A group is not a log column: it lives on the person row the token wrote. A principal
        // the directory does not know, a `superset` service account, or a person with no group
        // is still usage, so it lands in one honest `unknown` row instead of vanishing.
        By::Group => "COALESCE(g.name, 'unknown')",
    };
    let from = match params.by {
        By::Group => {
            "query_log \
             LEFT JOIN person p ON p.sub = query_log.principal OR p.username = query_log.principal \
             LEFT JOIN LATERAL unnest(p.groups) AS g(name) ON true \
             WHERE true"
        }
        _ => "query_log WHERE true",
    };
    // As in `list`, the engine user is the sub once authz 8/8 is live; rows written before it
    // still carry the sign-in name.
    let mut q: QueryBuilder<Postgres> = QueryBuilder::new(format!(
        "SELECT {key} AS key, count(*) AS queries, \
         count(*) FILTER (WHERE state = 'FAILED') AS failed, \
         COALESCE(sum(cpu_ms), 0)::bigint AS cpu_ms, \
         COALESCE(sum(wall_ms), 0)::bigint AS wall_ms, \
         COALESCE(sum(input_bytes), 0)::bigint AS input_bytes, \
         COALESCE(sum(written_bytes), 0)::bigint AS written_bytes \
         FROM {from}"
    ));
    push_usage_predicates(&mut q, &params, &who, &owners);
    q.push(match params.by {
        By::Principal | By::Warehouse | By::Group => {
            " GROUP BY 1 ORDER BY cpu_ms DESC, key ASC LIMIT "
        }
        By::Day => " GROUP BY 1 ORDER BY key ASC LIMIT ",
    })
    .push_bind(USAGE_LIMIT);

    // No second check per row here: a warehouse or a day is not a principal, so the scope can
    // only narrow the rows in the WHERE above (for `by=principal`, it already is the rule).
    let mut items: Vec<UsageRow> = match q.build_query_as().fetch_all(pool).await {
        Ok(rows) => rows,
        Err(e) => return store_failed(e),
    };
    for row in &mut items {
        row.estimated_cost = estimate(row.cpu_ms, s.showback.as_ref());
    }
    if matches!(params.by, By::Principal) {
        match person_names(pool).await {
            Ok(names) => name_principals(&mut items, &names),
            Err(e) => return store_failed(e),
        }
    }
    // A person in two groups appears in two rows, so adding the rows up would count them twice.
    // For `by=group` the total is its own query over the window, without the person join.
    let total = if params.by == By::Group {
        let mut t: QueryBuilder<Postgres> = QueryBuilder::new(
            "SELECT count(*) AS queries, \
             count(*) FILTER (WHERE state = 'FAILED') AS failed, \
             COALESCE(sum(cpu_ms), 0)::bigint AS cpu_ms, \
             COALESCE(sum(wall_ms), 0)::bigint AS wall_ms, \
             COALESCE(sum(input_bytes), 0)::bigint AS input_bytes, \
             COALESCE(sum(written_bytes), 0)::bigint AS written_bytes \
             FROM query_log WHERE true",
        );
        push_usage_predicates(&mut t, &params, &who, &owners);
        match t.build_query_as::<UsageTotals>().fetch_one(pool).await {
            Ok(totals) => totals.into_row(s.showback.as_ref()),
            Err(e) => return store_failed(e),
        }
    } else {
        UsageRow::total(&items, s.showback.as_ref())
    };
    Json(UsagePage {
        by: params.by.as_str().into(),
        from: params.from_ms,
        to: params.to_ms,
        items,
        total,
        price: s.showback.as_ref().map(UsagePrice::from),
    })
    .into_response()
}

#[derive(Debug, Serialize, FromRow, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RepeatedStatement {
    pub sql_fingerprint: String,
    /// The most recent run's statement.
    #[serde(rename = "sql")]
    #[schema(rename = "sql")]
    pub sql: String,
    pub runs: i64,
    pub total_ms: Option<i64>,
    pub average_ms: Option<i64>,
    pub max_ms: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RepeatedQueries {
    pub statements: Vec<RepeatedStatement>,
    /// The supported acceleration path; the same sentence for the API, the CLI and MCP.
    pub suggestion: String,
}

/// The supported acceleration path: #32 measured that the shipped catalog refuses
/// materialized views, so a suggestion names the refresh-table pattern instead.
const SUGGESTION: &str = "A statement you run repeatedly is a candidate for a table refreshed by a scheduled pipeline: a `sql` task with `CREATE OR REPLACE TABLE … AS SELECT …`, on the same schedule the query runs today (the shop example's 10_daily_revenue.sql is the pattern). Materialized views are refused by the shipped Iceberg REST catalog, so this page never creates one. Nothing is created or changed here.";

#[utoipa::path(
    get,
    operation_id = "query_log_repeated",
    path = "/api/query-log/repeated",
    tag = "public",
    description = "The statements the caller runs repeatedly, worst first (stardelt#38): the last 30 days of FINISHED queries, grouped by their shape (string literals and whitespace do not split a group; numeric literals do), at least 2 runs per group, ordered by total execution time. `scope` is `mine` (default), `team` (the service principals the caller's groups own) or `all` (administrators only); `limit` is 1 to 100, default 20. The answer carries one sentence about the supported acceleration path — a `sql` task with `CREATE OR REPLACE TABLE … AS SELECT …` in a scheduled pipeline. Nothing is created or changed.",
    params(
        ("scope" = Option<String>, Query, description = "mine (default), team, or all for administrators"),
        ("limit" = Option<i64>, Query, description = "How many groups, 1 to 100 (default 20)"),
    ),
    responses(
        (status = 200, description = "The repeated statements", body = RepeatedQueries),
        (status = 400, description = "scope or limit is not one of the accepted values", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The all scope is for administrators", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn repeated(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Query(raw): Query<HashMap<String, String>>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let who = Caller::of(caller.as_ref().map(|c| &c.0), &s.dev_user);
    let params = match parse_repeated_params(&raw) {
        Ok(p) => p,
        Err(message) => return error(StatusCode::BAD_REQUEST, &message),
    };
    if params.scope == Scope::All && !who.is_admin {
        return error(StatusCode::FORBIDDEN, "the all scope is for administrators");
    }
    let owners = if params.scope == Scope::Team {
        principal_owners(&s).await
    } else {
        HashMap::new()
    };
    if params.scope == Scope::Team && owners.is_empty() {
        return Json(RepeatedQueries {
            statements: Vec::new(),
            suggestion: SUGGESTION.into(),
        })
        .into_response();
    }

    let mut q: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT sql_fingerprint, \
            (array_agg(sql_text ORDER BY created_at DESC))[1] AS sql, \
            count(*)::bigint AS runs, \
            sum(execution_ms)::bigint AS total_ms, \
            avg(execution_ms)::bigint AS average_ms, \
            max(execution_ms)::bigint AS max_ms \
         FROM query_log \
         WHERE state = 'FINISHED' AND sql_fingerprint IS NOT NULL \
           AND created_at > now() - interval '30 days'",
    );
    if params.scope == Scope::Mine {
        q.push(" AND (principal = ")
            .push_bind(who.username.clone())
            .push(" OR principal = ")
            .push_bind(who.sub.clone())
            .push(")");
    }
    if params.scope == Scope::Team {
        let subjects: Vec<String> = owners.keys().cloned().collect();
        q.push(" AND principal = ANY(")
            .push_bind(subjects)
            .push(")");
    }
    q.push(
        " GROUP BY sql_fingerprint HAVING count(*) >= 2 ORDER BY total_ms DESC NULLS LAST LIMIT ",
    )
    .push_bind(params.limit);

    match q
        .build_query_as::<RepeatedStatement>()
        .fetch_all(pool)
        .await
    {
        Ok(statements) => Json(RepeatedQueries {
            statements,
            suggestion: SUGGESTION.into(),
        })
        .into_response(),
        Err(e) => store_failed(e),
    }
}

/// The profile row's own fields the hints need: who ran it and how long it waited.
#[derive(FromRow)]
struct ProfileRow {
    principal: String,
    queued_ms: Option<i64>,
    execution_ms: Option<i64>,
}

#[derive(FromRow, Debug)]
struct DetailRow {
    query_id: String,
    warehouse: String,
    state: String,
    principal: String,
    source: Option<String>,
    client_tags: Vec<String>,
    catalog: Option<String>,
    schema_name: Option<String>,
    query_type: Option<String>,
    sql_text: String,
    sql_hash: String,
    created_ms: i64,
    started_ms: Option<i64>,
    ended_ms: Option<i64>,
    queued_ms: Option<i64>,
    planning_ms: Option<i64>,
    execution_ms: Option<i64>,
    wall_ms: Option<i64>,
    cpu_ms: Option<i64>,
    peak_memory_bytes: Option<i64>,
    input_rows: Option<i64>,
    input_bytes: Option<i64>,
    output_rows: Option<i64>,
    output_bytes: Option<i64>,
    written_rows: Option<i64>,
    written_bytes: Option<i64>,
    error_code: Option<String>,
    error_message: Option<String>,
    tables_read: Vec<String>,
    tables_written: Vec<String>,
}

/// How long the query waited, planned and ran, in milliseconds.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryTiming {
    queued_ms: Option<i64>,
    planning_ms: Option<i64>,
    execution_ms: Option<i64>,
    wall_ms: Option<i64>,
    cpu_ms: Option<i64>,
}

/// How much data the query read, wrote and held in memory.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryData {
    input_rows: Option<i64>,
    input_bytes: Option<i64>,
    output_rows: Option<i64>,
    output_bytes: Option<i64>,
    written_rows: Option<i64>,
    written_bytes: Option<i64>,
    peak_memory_bytes: Option<i64>,
}

/// Why a query failed.
#[derive(Serialize, ToSchema)]
pub struct QueryFailure {
    code: String,
    message: Option<String>,
}

/// One earlier run of the same statement (nova#788 S4).
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HistoryRun {
    pub id: String,
    pub state: String,
    /// Finished at, epoch milliseconds; null while the engine never reported an end.
    pub ended_at: Option<i64>,
    pub wall_ms: Option<i64>,
    pub execution_ms: Option<i64>,
    pub input_bytes: Option<i64>,
    pub input_rows: Option<i64>,
}

/// The runs of the same statement that the caller can see, newest first.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryHistory {
    pub runs: Vec<HistoryRun>,
    /// Median wall time of `runs` in milliseconds; null when no run has a timing.
    pub median_wall_ms: Option<i64>,
    /// True when this query's wall time is more than twice the median and at least three runs were compared.
    pub slower: bool,
}

/// The median of the runs' wall times (integer milliseconds) and whether `current` is more
/// than twice it. Fewer than three timed runs is never "slower": there is nothing to compare.
pub fn history_summary(values: &[Option<i64>], current: Option<i64>) -> (Option<i64>, bool) {
    let mut timed: Vec<i64> = values.iter().filter_map(|v| *v).collect();
    if timed.is_empty() {
        return (None, false);
    }
    timed.sort_unstable();
    let n = timed.len();
    let median = if n % 2 == 1 {
        timed[n / 2]
    } else {
        (timed[n / 2 - 1] + timed[n / 2]) / 2
    };
    let slower = n >= 3 && median > 0 && matches!(current, Some(c) if c > 2 * median);
    (Some(median), slower)
}

/// Everything stored about one query: the whole statement, timing, data volumes, tables.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryLogDetail {
    id: String,
    state: String,
    /// The raw engine user (the `sub` once authz 8/8 is live).
    user: String,
    user_name: Option<String>,
    source: Option<String>,
    client_tags: Vec<String>,
    warehouse: String,
    catalog: Option<String>,
    schema: Option<String>,
    query_type: Option<String>,
    sql: String,
    sql_hash: String,
    created_at: i64,
    started_at: Option<i64>,
    ended_at: Option<i64>,
    timing: QueryTiming,
    data: QueryData,
    tables_read: Vec<String>,
    tables_written: Vec<String>,
    error: Option<QueryFailure>,
}

/// `GET /api/query-log/{id}` — everything stored about one query the caller may see.
#[utoipa::path(
    get,
    operation_id = "query_log_detail",
    path = "/api/query-log/{id}",
    tag = "public",
    description = "One query with its whole statement. Somebody else's query answers 404, exactly like a missing one; an administrator who opens another person's query is audited. With NOVA_QUERY_LOG_REDACT_LITERALS=1 somebody else's statement shows '?' in place of every string literal.",
    params(("id" = String, Path, description = "The engine's query id")),
    responses(
        (status = 200, description = "The stored query", body = QueryLogDetail),
        (status = 404, description = "No such query, or it is not the caller's to see", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn detail(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Path(id): Path<String>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let who = Caller::of(caller.as_ref().map(|c| &c.0), &s.dev_user);
    let row: Option<DetailRow> = match sqlx::query_as(
        "SELECT query_id, warehouse, state, principal, source, client_tags, catalog, schema_name, \
            query_type, sql_text, sql_hash, \
            (extract(epoch FROM created_at) * 1000)::bigint AS created_ms, \
            (extract(epoch FROM started_at) * 1000)::bigint AS started_ms, \
            (extract(epoch FROM ended_at) * 1000)::bigint AS ended_ms, \
            queued_ms, planning_ms, execution_ms, wall_ms, cpu_ms, peak_memory_bytes, input_rows, \
            input_bytes, output_rows, output_bytes, written_rows, written_bytes, error_code, \
            error_message, tables_read, tables_written \
         FROM query_log WHERE query_id = $1",
    )
    .bind(&id)
    .fetch_optional(pool)
    .await
    {
        Ok(row) => row,
        Err(e) => return store_failed(e),
    };
    let Some(r) = row else {
        return not_found();
    };
    let owners = principal_owners(&s).await;
    let owner = RowOwner {
        principal: &r.principal,
        owner_group: owners.get(&r.principal).map(String::as_str),
    };
    if !visible(&owner, &who.viewer()) {
        return not_found();
    }
    if r.principal != who.username && r.principal != who.sub {
        // An administrator opening somebody else's statement leaves a trace.
        tracing::info!(
            target: "audit",
            actor = %who.username,
            sub = %who.sub,
            action = "query_log.read_other",
            query_id = %r.query_id,
            owner = %r.principal,
            "administrator opened another user's query"
        );
    }
    let user_name = if people::is_uuid(&r.principal) {
        people::names(pool, std::slice::from_ref(&r.principal))
            .await
            .get(&r.principal)
            .cloned()
    } else {
        None
    };
    let error = r.error_code.map(|code| QueryFailure {
        code,
        message: r.error_message,
    });
    let sql = if s.redact_query_log_literals && !is_own_row(&r.principal, &who) {
        redact_literals(&r.sql_text)
    } else {
        r.sql_text.clone()
    };
    Json(QueryLogDetail {
        id: r.query_id,
        state: r.state,
        user: r.principal,
        user_name,
        source: r.source,
        client_tags: r.client_tags,
        warehouse: r.warehouse,
        catalog: r.catalog,
        schema: r.schema_name,
        query_type: r.query_type,
        sql,
        sql_hash: r.sql_hash,
        created_at: r.created_ms,
        started_at: r.started_ms,
        ended_at: r.ended_ms,
        timing: QueryTiming {
            queued_ms: r.queued_ms,
            planning_ms: r.planning_ms,
            execution_ms: r.execution_ms,
            wall_ms: r.wall_ms,
            cpu_ms: r.cpu_ms,
        },
        data: QueryData {
            input_rows: r.input_rows,
            input_bytes: r.input_bytes,
            output_rows: r.output_rows,
            output_bytes: r.output_bytes,
            written_rows: r.written_rows,
            written_bytes: r.written_bytes,
            peak_memory_bytes: r.peak_memory_bytes,
        },
        tables_read: r.tables_read,
        tables_written: r.tables_written,
        error,
    })
    .into_response()
}

/// `GET /api/query-log/{id}/profile` — where one finished query spent its time: the engine's
/// plan and the per-operator statistics it reported.
#[utoipa::path(
    get,
    operation_id = "query_log_profile",
    path = "/api/query-log/{id}/profile",
    tag = "public",
    description = "The plan and per-operator statistics of one finished query. The same rule as the detail: somebody else's query answers 404, exactly like a missing one, and an administrator who opens another person's profile is audited. A query without a stored profile answers 404 with a sentence (still running, older than the profile retention, or the engine sent none). With NOVA_QUERY_LOG_REDACT_LITERALS=1 somebody else's plan shows '?' in place of every string literal. The answer also carries \"hints\", an array of deterministic suggestions computed from the profile and the query's timing, each with its evidence and a documentation link; an empty array means no rule found evidence.",
    params(("id" = String, Path, description = "The engine's query id")),
    responses(
        (status = 200, description = "The stored profile: plan, operators, spill and split counts, inputs", body = Object),
        (status = 404, description = "No such query, it is not the caller's to see, or it has no profile", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn profile(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Path(id): Path<String>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let who = Caller::of(caller.as_ref().map(|c| &c.0), &s.dev_user);
    let row: Option<ProfileRow> = match sqlx::query_as(
        "SELECT principal, queued_ms, execution_ms FROM query_log WHERE query_id = $1",
    )
    .bind(&id)
    .fetch_optional(pool)
    .await
    {
        Ok(row) => row,
        Err(e) => return store_failed(e),
    };
    let Some(row) = row else {
        return not_found();
    };
    let principal = row.principal;
    let owners = principal_owners(&s).await;
    let owner = RowOwner {
        principal: &principal,
        owner_group: owners.get(&principal).map(String::as_str),
    };
    if !visible(&owner, &who.viewer()) {
        return not_found();
    }
    if principal != who.username && principal != who.sub {
        // An administrator opening somebody else's profile leaves a trace.
        tracing::info!(
            target: "audit",
            actor = %who.username,
            sub = %who.sub,
            action = "query_log.read_other_profile",
            query_id = %id,
            owner = %principal,
            "administrator opened another user's query profile"
        );
    }
    let stored: Option<serde_json::Value> =
        match sqlx::query_scalar("SELECT profile FROM query_profile WHERE query_id = $1")
            .bind(&id)
            .fetch_optional(pool)
            .await
        {
            Ok(row) => row,
            Err(e) => return store_failed(e),
        };
    let Some(mut profile) = stored else {
        return error(
            StatusCode::NOT_FOUND,
            "no profile for this query (it is still running, older than the profile retention, or the engine sent none)",
        );
    };
    if s.redact_query_log_literals && !is_own_row(&principal, &who) {
        let redacted = profile["plan"].as_str().map(redact_literals);
        if let Some(plan) = redacted {
            profile["plan"] = serde_json::Value::String(plan);
        }
    }
    // Hints are computed, never stored: the rules improve without a backfill.
    let hints = query_hints::hints(&profile, row.queued_ms, row.execution_ms);
    profile["hints"] = serde_json::to_value(hints).expect("hints serialize");
    (
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(profile),
    )
        .into_response()
}

/// The visibility key of a history: whose statement it is, and which statement.
#[derive(FromRow)]
struct HistoryBase {
    principal: String,
    sql_hash: String,
}

/// One stored run of the statement.
#[derive(FromRow)]
struct HistoryRow {
    query_id: String,
    state: String,
    ended_ms: Option<i64>,
    wall_ms: Option<i64>,
    execution_ms: Option<i64>,
    input_bytes: Option<i64>,
    input_rows: Option<i64>,
}

/// `GET /api/query-log/{id}/history` — the last runs of the same statement, and whether this
/// one is slower than their median.
#[utoipa::path(
    get,
    operation_id = "query_log_history",
    path = "/api/query-log/{id}/history",
    tag = "public",
    description = "The last 20 runs of the same statement by the same principal: each one's state, end time, wall and execution time, bytes and rows read, newest first. The same visibility rule as the detail: somebody else's query answers 404, exactly like a missing one, and an administrator who opens another person's history is audited. medianWallMs is the median of the returned runs' wall times; slower is true when this run's wall time is more than twice that median and at least three runs were compared. Answers Cache-Control: no-store.",
    params(("id" = String, Path, description = "The engine's query id")),
    responses(
        (status = 200, description = "The runs of the same statement, newest first", body = QueryHistory),
        (status = 404, description = "No such query, or it is not the caller's to see", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn history(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Path(id): Path<String>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let who = Caller::of(caller.as_ref().map(|c| &c.0), &s.dev_user);
    let base: Option<HistoryBase> =
        match sqlx::query_as("SELECT principal, sql_hash FROM query_log WHERE query_id = $1")
            .bind(&id)
            .fetch_optional(pool)
            .await
        {
            Ok(row) => row,
            Err(e) => return store_failed(e),
        };
    let Some(base) = base else {
        return not_found();
    };
    let owners = principal_owners(&s).await;
    let owner = RowOwner {
        principal: &base.principal,
        owner_group: owners.get(&base.principal).map(String::as_str),
    };
    if !visible(&owner, &who.viewer()) {
        return not_found();
    }
    if base.principal != who.username && base.principal != who.sub {
        // An administrator opening somebody else's history leaves a trace.
        tracing::info!(
            target: "audit",
            actor = %who.username,
            sub = %who.sub,
            action = "query_log.read_other_history",
            query_id = %id,
            owner = %base.principal,
            "administrator opened another user's query history"
        );
    }
    let rows: Vec<HistoryRow> = match sqlx::query_as(
        "SELECT l.query_id, l.state, \
            (extract(epoch FROM l.ended_at) * 1000)::bigint AS ended_ms, \
            l.wall_ms, l.execution_ms, l.input_bytes, l.input_rows \
         FROM query_log l \
         WHERE l.principal = $1 AND l.sql_hash = $2 AND l.state <> 'QUEUED' \
         ORDER BY l.ended_at DESC NULLS LAST, l.query_id DESC \
         LIMIT 20",
    )
    .bind(&base.principal)
    .bind(&base.sql_hash)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => return store_failed(e),
    };
    let runs: Vec<HistoryRun> = rows
        .into_iter()
        .map(|r| HistoryRun {
            id: r.query_id,
            state: r.state,
            ended_at: r.ended_ms,
            wall_ms: r.wall_ms,
            execution_ms: r.execution_ms,
            input_bytes: r.input_bytes,
            input_rows: r.input_rows,
        })
        .collect();
    let current = runs.iter().find(|r| r.id == id).and_then(|r| r.wall_ms);
    let values: Vec<Option<i64>> = runs.iter().map(|r| r.wall_ms).collect();
    let (median_wall_ms, slower) = history_summary(&values, current);
    (
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(QueryHistory {
            runs,
            median_wall_ms,
            slower,
        }),
    )
        .into_response()
}

/// One stored row of the query log, with every column the table has.
#[derive(FromRow)]
struct ExportRow {
    query_id: String,
    warehouse: String,
    state: String,
    principal: String,
    source: Option<String>,
    client_tags: Vec<String>,
    catalog: Option<String>,
    schema_name: Option<String>,
    query_type: Option<String>,
    sql_text: String,
    sql_hash: String,
    created_ms: i64,
    started_ms: Option<i64>,
    ended_ms: Option<i64>,
    updated_ms: i64,
    queued_ms: Option<i64>,
    planning_ms: Option<i64>,
    execution_ms: Option<i64>,
    wall_ms: Option<i64>,
    cpu_ms: Option<i64>,
    peak_memory_bytes: Option<i64>,
    input_rows: Option<i64>,
    input_bytes: Option<i64>,
    output_rows: Option<i64>,
    output_bytes: Option<i64>,
    written_rows: Option<i64>,
    written_bytes: Option<i64>,
    error_code: Option<String>,
    error_message: Option<String>,
    tables_read: Vec<String>,
    tables_written: Vec<String>,
}

impl From<ExportRow> for QueryLogExportRow {
    fn from(r: ExportRow) -> Self {
        let error = r.error_code.map(|code| QueryFailure {
            code,
            message: r.error_message,
        });
        Self {
            id: r.query_id,
            state: r.state,
            user: r.principal,
            source: r.source,
            client_tags: r.client_tags,
            warehouse: r.warehouse,
            catalog: r.catalog,
            schema: r.schema_name,
            query_type: r.query_type,
            sql: r.sql_text,
            sql_hash: r.sql_hash,
            created_at: r.created_ms,
            started_at: r.started_ms,
            ended_at: r.ended_ms,
            updated_at: r.updated_ms,
            timing: QueryTiming {
                queued_ms: r.queued_ms,
                planning_ms: r.planning_ms,
                execution_ms: r.execution_ms,
                wall_ms: r.wall_ms,
                cpu_ms: r.cpu_ms,
            },
            data: QueryData {
                input_rows: r.input_rows,
                input_bytes: r.input_bytes,
                output_rows: r.output_rows,
                output_bytes: r.output_bytes,
                written_rows: r.written_rows,
                written_bytes: r.written_bytes,
                peak_memory_bytes: r.peak_memory_bytes,
            },
            tables_read: r.tables_read,
            tables_written: r.tables_written,
            error,
        }
    }
}

/// One row of the export: every stored column, so a writer can copy the table whole.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryLogExportRow {
    id: String,
    state: String,
    user: String,
    source: Option<String>,
    client_tags: Vec<String>,
    warehouse: String,
    catalog: Option<String>,
    schema: Option<String>,
    query_type: Option<String>,
    sql: String,
    sql_hash: String,
    created_at: i64,
    started_at: Option<i64>,
    ended_at: Option<i64>,
    updated_at: i64,
    timing: QueryTiming,
    data: QueryData,
    tables_read: Vec<String>,
    tables_written: Vec<String>,
    error: Option<QueryFailure>,
}

/// One ascending page of the export; `nextCursor` is null on the last page.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueryLogExportPage {
    rows: Vec<QueryLogExportRow>,
    next_cursor: Option<String>,
}

/// `GET /api/admin/query-log/export?from=&cursor=&limit=` — the whole log, ascending.
#[utoipa::path(
    get,
    operation_id = "query_log_export",
    path = "/api/admin/query-log/export",
    tag = "public",
    description = "Administrator only. Every stored column of the query log, oldest first, a page at a time — the read a job copies into the platform's own tables so usage outlives the 90-day window. The order is ascending (createdAt, then id), so a writer keeps the nextCursor of one page and continues from it after a restart; nextCursor is null on the last page. limit is 1 to 1000, 500 by default; from is an epoch-millisecond lower bound. With NOVA_QUERY_LOG_REDACT_LITERALS=1 every statement shows '?' in place of each string literal.",
    params(
        ("from" = Option<i64>, Query, description = "Created at or after this time, epoch milliseconds"),
        ("cursor" = Option<String>, Query, description = "The nextCursor of the previous page; rows after it, ascending"),
        ("limit" = Option<i64>, Query, description = "Page size from 1 to 1000; 500 by default"),
    ),
    responses(
        (status = 200, description = "One ascending page of the whole log", body = QueryLogExportPage),
        (status = 400, description = "A parameter is not valid; the message names it", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn export(
    State(s): State<Arc<AppState>>,
    Query(raw): Query<HashMap<String, String>>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let params = match parse_export_params(&raw) {
        Ok(p) => p,
        Err(message) => return error(StatusCode::BAD_REQUEST, &message),
    };
    let cursor_us = params.cursor.as_ref().map(|(us, _)| *us);
    let cursor_id = params
        .cursor
        .as_ref()
        .map(|(_, id)| id.clone())
        .unwrap_or_default();
    let rows: Vec<ExportRow> = match sqlx::query_as(
        "SELECT query_id, warehouse, state, principal, source, client_tags, catalog, schema_name, \
                query_type, sql_text, sql_hash, \
                (extract(epoch FROM created_at) * 1000)::bigint AS created_ms, \
                (extract(epoch FROM started_at) * 1000)::bigint AS started_ms, \
                (extract(epoch FROM ended_at) * 1000)::bigint AS ended_ms, \
                (extract(epoch FROM updated_at) * 1000)::bigint AS updated_ms, \
                queued_ms, planning_ms, execution_ms, wall_ms, cpu_ms, peak_memory_bytes, \
                input_rows, input_bytes, output_rows, output_bytes, written_rows, written_bytes, \
                error_code, error_message, tables_read, tables_written \
           FROM query_log \
          WHERE ($1::bigint IS NULL OR created_at >= 'epoch'::timestamptz + ($1::bigint * interval '1 millisecond')) \
            AND ($2::bigint IS NULL OR (created_at, query_id) > ('epoch'::timestamptz + ($2::bigint * interval '1 microsecond'), $3)) \
          ORDER BY created_at ASC, query_id ASC \
          LIMIT $4",
    )
    .bind(params.from_ms)
    .bind(cursor_us)
    .bind(cursor_id)
    .bind(params.limit + 1)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => return store_failed(e),
    };
    let more = rows.len() as i64 > params.limit;
    let mut rows = rows;
    rows.truncate(params.limit as usize);
    let next = if more {
        rows.last()
            .map(|r| encode_cursor(r.created_ms * 1000, &r.query_id))
    } else {
        None
    };
    let mut rows: Vec<QueryLogExportRow> = rows.into_iter().map(QueryLogExportRow::from).collect();
    if s.redact_query_log_literals {
        for row in &mut rows {
            row.sql = redact_literals(&row.sql);
        }
    }
    Json(QueryLogExportPage {
        rows,
        next_cursor: next,
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// The UI and the SDKs read these names; typing the responses must not rename one.
    #[test]
    fn the_detail_keeps_its_wire_names() {
        let detail = QueryLogDetail {
            id: "q1".into(),
            state: "FAILED".into(),
            user: "u".into(),
            user_name: None,
            source: None,
            client_tags: vec![],
            warehouse: "analytics".into(),
            catalog: None,
            schema: None,
            query_type: None,
            sql: "select 1".into(),
            sql_hash: "h".into(),
            created_at: 1,
            started_at: None,
            ended_at: None,
            timing: QueryTiming {
                queued_ms: None,
                planning_ms: None,
                execution_ms: None,
                wall_ms: Some(5),
                cpu_ms: None,
            },
            data: QueryData {
                input_rows: None,
                input_bytes: None,
                output_rows: None,
                output_bytes: None,
                written_rows: None,
                written_bytes: None,
                peak_memory_bytes: Some(7),
            },
            tables_read: vec![],
            tables_written: vec![],
            error: Some(QueryFailure {
                code: "X".into(),
                message: None,
            }),
        };
        let v = serde_json::to_value(&detail).unwrap();
        for key in [
            "id",
            "state",
            "user",
            "userName",
            "source",
            "clientTags",
            "warehouse",
            "catalog",
            "schema",
            "queryType",
            "sql",
            "sqlHash",
            "createdAt",
            "startedAt",
            "endedAt",
            "timing",
            "data",
            "tablesRead",
            "tablesWritten",
            "error",
        ] {
            assert!(v.get(key).is_some(), "the detail lost {key}");
        }
        assert_eq!(v["timing"]["wallMs"], 5);
        assert_eq!(v["data"]["peakMemoryBytes"], 7);
        assert_eq!(v["error"]["code"], "X");
        let page = serde_json::to_value(QueryLogPage {
            items: vec![],
            next_cursor: None,
        })
        .unwrap();
        assert_eq!(page, serde_json::json!({ "items": [], "nextCursor": null }));
    }

    #[test]
    fn query_log_export_parameters_and_cursor() {
        let p = parse_export_params(&raw(&[])).unwrap();
        assert_eq!(
            p,
            ExportParams {
                from_ms: None,
                cursor: None,
                limit: 500,
            }
        );
        assert_eq!(
            parse_export_params(&raw(&[("limit", "1000")]))
                .unwrap()
                .limit,
            1000
        );
        assert_eq!(
            parse_export_params(&raw(&[("limit", "1001")])).unwrap_err(),
            "limit must be between 1 and 1000"
        );
        assert_eq!(
            parse_export_params(&raw(&[("limit", "0")])).unwrap_err(),
            "limit must be between 1 and 1000"
        );
        assert_eq!(
            parse_export_params(&raw(&[("from", "abc")])).unwrap_err(),
            "from must be a number (epoch milliseconds)"
        );
        assert_eq!(
            parse_export_params(&raw(&[("cursor", "nonsense")])).unwrap_err(),
            "invalid cursor"
        );
        let p = parse_export_params(&raw(&[
            ("cursor", &encode_cursor(1_700_000_000_000_000, "q1")),
            ("from", "1700000000000"),
        ]))
        .unwrap();
        assert_eq!(p.from_ms, Some(1_700_000_000_000));
        assert_eq!(p.cursor, Some((1_700_000_000_000_000, "q1".to_string())));
    }

    /// The exporter reads these names; typing the responses must not rename one.
    #[test]
    fn the_export_row_keeps_its_wire_names() {
        let row = QueryLogExportRow {
            id: "q1".into(),
            state: "FINISHED".into(),
            user: "u".into(),
            source: None,
            client_tags: vec![],
            warehouse: "analytics".into(),
            catalog: None,
            schema: None,
            query_type: None,
            sql: "select 1".into(),
            sql_hash: "h".into(),
            created_at: 1,
            started_at: None,
            ended_at: None,
            updated_at: 2,
            timing: QueryTiming {
                queued_ms: None,
                planning_ms: None,
                execution_ms: None,
                wall_ms: None,
                cpu_ms: None,
            },
            data: QueryData {
                input_rows: None,
                input_bytes: None,
                output_rows: None,
                output_bytes: None,
                written_rows: None,
                written_bytes: None,
                peak_memory_bytes: None,
            },
            tables_read: vec![],
            tables_written: vec![],
            error: None,
        };
        let v = serde_json::to_value(&row).unwrap();
        for key in ["createdAt", "sqlHash", "tablesWritten", "updatedAt"] {
            assert!(v.get(key).is_some(), "the export row lost {key}");
        }
        let page = serde_json::to_value(QueryLogExportPage {
            rows: vec![],
            next_cursor: None,
        })
        .unwrap();
        assert_eq!(page, serde_json::json!({ "rows": [], "nextCursor": null }));
    }

    #[test]
    fn history_summary_is_the_median_and_the_doubling_flag() {
        assert_eq!(
            history_summary(&[Some(900), Some(1000), Some(2324)], Some(2324)),
            (Some(1000), true)
        );
        assert_eq!(
            history_summary(&[Some(900), Some(1000)], Some(900)),
            (Some(950), false)
        );
        assert_eq!(
            history_summary(&[Some(1000), Some(1000), None], Some(1999)),
            (Some(1000), false)
        );
        assert_eq!(history_summary(&[], None), (None, false));
    }

    #[test]
    fn the_defaults_are_my_queries_fifty_at_a_time() {
        let p = parse_params(&raw(&[])).unwrap();
        assert_eq!((p.scope, p.limit), (Scope::Mine, 50));
        assert!(p.state.is_none() && p.cursor.is_none() && p.search.is_none());
    }

    #[test]
    fn repeated_params_default_to_mine_and_twenty() {
        let p = parse_repeated_params(&HashMap::new()).expect("the defaults");
        assert_eq!((p.scope, p.limit), (Scope::Mine, 20));
        let mut raw = HashMap::new();
        raw.insert("scope".into(), "team".into());
        raw.insert("limit".into(), "5".into());
        let p = parse_repeated_params(&raw).expect("the values");
        assert_eq!((p.scope, p.limit), (Scope::Team, 5));
        raw.insert("scope".into(), "everyone".into());
        assert_eq!(
            parse_repeated_params(&raw).unwrap_err(),
            "scope must be mine, team or all"
        );
        raw.insert("scope".into(), "mine".into());
        raw.insert("limit".into(), "0".into());
        assert_eq!(
            parse_repeated_params(&raw).unwrap_err(),
            "limit must be a number from 1 to 100"
        );
    }

    #[test]
    fn every_parameter_is_checked_with_a_sentence() {
        let bad = |pairs: &[(&str, &str)], needle: &str| {
            let message = parse_params(&raw(pairs)).unwrap_err();
            assert!(
                message.contains(needle),
                "{message} should contain {needle}"
            );
        };
        bad(&[("scope", "everyone")], "scope");
        bad(&[("state", "RUNNING")], "state");
        bad(&[("limit", "0")], "limit");
        bad(&[("limit", "201")], "limit");
        bad(&[("limit", "many")], "limit");
        bad(&[("from", "yesterday")], "from");
        bad(&[("cursor", "###")], "cursor");
        bad(&[("q", &"x".repeat(201))], "q must");
    }

    #[test]
    fn state_is_case_insensitive_and_filters_parse() {
        let p = parse_params(&raw(&[
            ("scope", "all"),
            ("state", "failed"),
            ("warehouse", "analytics"),
            ("q", " nation "),
            ("from", "1000"),
            ("to", "2000"),
            ("limit", "7"),
        ]))
        .unwrap();
        assert_eq!(p.scope, Scope::All);
        assert_eq!(p.state.as_deref(), Some("FAILED"));
        assert_eq!(p.warehouse.as_deref(), Some("analytics"));
        assert_eq!(p.search.as_deref(), Some("nation"));
        assert_eq!((p.from_ms, p.to_ms, p.limit), (Some(1000), Some(2000), 7));
    }

    #[test]
    fn a_cursor_round_trips_and_garbage_does_not_decode() {
        let c = encode_cursor(1_760_873_605_058_999, "20261019_113323_00000_tnvu4");
        assert_eq!(
            decode_cursor(&c),
            Some((
                1_760_873_605_058_999,
                "20261019_113323_00000_tnvu4".to_string()
            ))
        );
        assert_eq!(decode_cursor("not base64 !"), None);
        assert_eq!(decode_cursor(&URL_SAFE_NO_PAD.encode("12:")), None);
        assert_eq!(decode_cursor(&URL_SAFE_NO_PAD.encode("x:y")), None);
    }

    #[test]
    fn wildcards_in_a_search_text_are_literals() {
        assert_eq!(like_pattern("50%_off\\"), "%50\\%\\_off\\\\%");
        assert_eq!(like_pattern("plain"), "%plain%");
    }

    #[test]
    fn the_owner_map_reads_the_subject_and_the_group_and_skips_the_unset() {
        let principals = vec![
            serde_json::json!({"metadata": {"name": "pipeline-mobility-analytics"},
                               "spec": {"ownerGroup": "mobility-analytics"},
                               "status": {"subject": "e385bcb7-9484-48b9-8d60-8f509fc6cbd9"}}),
            serde_json::json!({"metadata": {"name": "pipeline-old"},
                               "spec": {"ownerGroup": "old"}}),
            serde_json::json!({"metadata": {"name": "pipeline-retail-analytics"},
                               "spec": {"ownerGroup": "retail-analytics"},
                               "status": {"subject": "2b256a3d-aad8-4f02-960e-6fadcfc09a88"}}),
        ];
        let map = owner_groups(&principals);
        assert_eq!(map.len(), 2, "{map:?}");
        assert_eq!(
            map.get("e385bcb7-9484-48b9-8d60-8f509fc6cbd9")
                .map(String::as_str),
            Some("mobility-analytics")
        );
        assert_eq!(
            map.get("2b256a3d-aad8-4f02-960e-6fadcfc09a88")
                .map(String::as_str),
            Some("retail-analytics")
        );
    }

    #[test]
    fn a_team_row_is_the_owner_groups_row() {
        let groups = ["mobility-analytics".to_string()];
        let member = Viewer {
            username: "carol",
            sub: "carol",
            groups: &groups,
            is_admin: false,
        };
        let subject = "e385bcb7-9484-48b9-8d60-8f509fc6cbd9";
        let owners = HashMap::from([(subject.to_string(), "mobility-analytics".to_string())]);
        let row = RowOwner {
            principal: subject,
            owner_group: owners.get(subject).map(String::as_str),
        };
        assert!(in_scope(Scope::Team, &row, &member));
        let retail = ["retail-analytics".to_string()];
        let outsider = Viewer {
            username: "dave",
            sub: "dave",
            groups: &retail,
            is_admin: false,
        };
        assert!(!in_scope(Scope::Team, &row, &outsider));
    }

    #[test]
    fn by_group_is_parsed_and_an_unknown_dimension_is_named() {
        let params = parse_usage_params(&raw(&[("by", "group")]), 1_000).unwrap();
        assert_eq!(params.by, By::Group);
        assert_eq!(
            parse_usage_params(&raw(&[("by", "week")]), 1_000).unwrap_err(),
            "by must be principal, warehouse, day or group"
        );
    }

    #[test]
    fn showback_parses_a_price_and_refuses_half_a_setting() {
        assert_eq!(parse_showback(None, None), Ok(None));
        assert_eq!(
            parse_showback(Some("0.12"), Some("EUR")),
            Ok(Some(Showback {
                cpu_hour_price: 0.12,
                currency: "EUR".into(),
            }))
        );
        assert_eq!(
            parse_showback(Some("0.12"), None),
            Err("NOVA_CPU_HOUR_PRICE and NOVA_CPU_HOUR_CURRENCY must be set together".into())
        );
        assert_eq!(
            parse_showback(None, Some("EUR")),
            Err("NOVA_CPU_HOUR_PRICE and NOVA_CPU_HOUR_CURRENCY must be set together".into())
        );
        for bad in ["0", "abc"] {
            assert_eq!(
                parse_showback(Some(bad), Some("EUR")),
                Err(format!(
                    "NOVA_CPU_HOUR_PRICE is not a positive number: {bad}"
                ))
            );
        }
        assert_eq!(
            parse_showback(Some("0.12"), Some("")),
            Err("NOVA_CPU_HOUR_CURRENCY must be 1 to 8 characters, got: ".into())
        );
    }

    #[test]
    fn an_estimate_is_cpu_time_at_the_rate_rounded_to_cents() {
        let showback = Showback {
            cpu_hour_price: 0.12,
            currency: "EUR".into(),
        };
        assert_eq!(estimate(3_600_000, Some(&showback)), Some(0.12));
        assert_eq!(estimate(3_600_000, None), None);
    }

    #[test]
    fn a_principal_key_is_named_and_an_unknown_one_is_left_alone() {
        let row = |key: &str| UsageRow {
            key: key.into(),
            queries: 1,
            failed: 0,
            cpu_ms: 1,
            wall_ms: 1,
            input_bytes: 1,
            written_bytes: 1,
            estimated_cost: None,
        };
        let mut items = [row("5549059e-d41f-4706-ba9b-d1b4c5948492"), row("superset")];
        let names = HashMap::from([(
            "5549059e-d41f-4706-ba9b-d1b4c5948492".to_string(),
            "demo".to_string(),
        )]);
        name_principals(&mut items, &names);
        assert_eq!(items[0].key, "demo");
        assert_eq!(items[1].key, "superset");
    }
}
