//! Table documentation: description, tags, owner, domain and column descriptions
//! (stardelt-nova#189, design ticket #104; what the catalog spike stardelt-platform#242
//! measured).
//!
//! Where the fields live, and why each goes the way it does:
//!
//! - **Table description** is the Iceberg property `comment`. `COMMENT ON TABLE` and the
//!   REST `set-properties` update write the same property, but Nova uses the REST path so
//!   one request can carry every property change at once.
//! - **Column description** is the field's `doc` in a **new schema**: only Trino's
//!   `COMMENT ON COLUMN` writes it, so this route runs that statement through
//!   [`crate::trino::execute_from`] as the caller (`source = nova-catalog`). Iceberg keeps
//!   every historical schema, so the read picks the schema whose `schema-id` equals
//!   `current-schema-id` — never `schemas[0]`.
//! - **Tags, owner and domain** are the custom properties `stardelt.tags` (sorted, comma
//!   separated), `stardelt.owner` and `stardelt.domain`. Trino cannot write custom
//!   properties, so they go through one Lakekeeper commit
//!   (`POST …/tables/<table>` with `set-properties` / `remove-properties`, empty
//!   `requirements`).
//!
//! ## The interim rule, and what it is not
//!
//! Lakekeeper runs with `authz.backend: allowall` and cannot refuse an edit. Until the
//! catalog enforces permissions, **Nova** applies [`may_edit`]: an administrator may edit;
//! a member of the current owner group may edit; a table without an owner may be edited by
//! anybody, but the first editor must set the owner to one of their own groups. Everybody
//! else is refused. This is enforced by Nova only: anyone who reaches Lakekeeper or Trino
//! directly can still change these properties. The read answer says which side of the rule
//! the caller is on (`canEdit` / `editRule`), and the OpenAPI description repeats the
//! caveat.
//!
//! ## One editor at a time
//!
//! Concurrent commits to one table deadlock in Lakekeeper 0.12.2 (40 simultaneous
//! commits: three succeeded, 24 answered 500 `Error starting transaction`, 13 answered 503
//! `deadlock detected`, none answered 409). Nova therefore serialises edits **per table**
//! with [`AppState::table_locks`] — every write holds the table's lock for the whole
//! operation, including the load that decides the rule — and retries a commit answered 500
//! or 503 up to three attempts in total (waits 200, 400 ms), then answers 502 `the catalog
//! is busy, try again`. The lock is in-process: it covers one Nova replica. Running more
//! than one replica needs a database lock, which is a later ticket — until then the
//! manifests run one.
//!
//! ## Limits and answers
//!
//! A description is trimmed and capped at 4,000 characters and must not contain a NUL; a
//! guide (the `stardelt.guide` property) is trimmed at its ends, capped at 16,000
//! characters, keeps its line breaks and must not contain a NUL; at
//! most 20 tags are allowed, each `^[a-z0-9][a-z0-9_-]{0,31}$` and unique; an owner is a
//! group name ([`crate::connections::group_ok`]); a domain is
//! `^[a-z0-9][a-z0-9_-]{0,39}$`. Validation happens before any request is sent. A key that
//! is absent from a write body changes nothing; `null` (or an empty tag list, or a blank
//! description) removes the property. A write with nothing to change sends no commit and
//! answers the current documentation.
//!
//! The route logic sits in plain `async fn`s that take their inputs, so the tests below run
//! them against a fake Lakekeeper and a recorded SQL closure without a cluster.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Days, Duration, LocalResult, SecondsFormat, TimeZone, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use tracing::warn;

use crate::api_json::ApiJson;
use crate::auth::{Identity, Principal};
use crate::data::quote_ident;
use crate::{AppState, audit, catalog};

/// Most characters a description may hold, after trimming.
pub const MAX_DESCRIPTION_CHARS: usize = 4000;
/// Most characters a guide may hold, after trimming.
pub const MAX_GUIDE_CHARS: usize = 16000;
/// Most tags one table may carry.
pub const MAX_TAGS: usize = 20;

/// One column of the current schema, as the documentation read reports it.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ColumnDocumentation {
    /// Field name, exactly as the current schema spells it.
    pub name: String,
    /// The field's type as text; a complex type is its JSON text.
    pub r#type: String,
    /// The field's Iceberg `doc`, or `null`.
    pub description: Option<String>,
}

/// The documentation of one table, plus what the caller may do with it.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct TableDocumentation {
    /// The Iceberg `comment` property, or `null`.
    pub description: Option<String>,
    /// The `stardelt.tags` property, split on commas, trimmed and sorted.
    pub tags: Vec<String>,
    /// The `stardelt.owner` property: a group name, or `null`.
    pub owner: Option<String>,
    /// The `stardelt.domain` property, or `null`.
    pub domain: Option<String>,
    /// The `stardelt.guide` property: how to use this table (joins, filters, definitions), or `null`.
    pub guide: Option<String>,
    /// The fields of the **current** schema, in schema order.
    pub columns: Vec<ColumnDocumentation>,
    /// Whether the interim rule lets this caller edit (see the module docs).
    #[serde(rename = "canEdit")]
    #[schema(rename = "canEdit")]
    pub can_edit: bool,
    /// One sentence for a person: why `canEdit` has the value it has.
    #[serde(rename = "editRule")]
    #[schema(rename = "editRule")]
    pub edit_rule: String,
    /// Whether this caller may roll the table back (catalog owner group or administrator).
    #[serde(rename = "canRollback")]
    #[schema(rename = "canRollback")]
    pub can_rollback: bool,
    /// One sentence for a person: why `canRollback` has the value it has.
    #[serde(rename = "rollbackRule")]
    #[schema(rename = "rollbackRule")]
    pub rollback_rule: String,
}

/// The Iceberg table property that carries the freshness promise (#402).
pub const FRESHNESS_PROPERTY: &str = "stardelt.freshness";
const FRESHNESS_DEFAULT_GRACE_MINUTES: u32 = 60;
const FRESHNESS_MAX_GRACE_MINUTES: u32 = 1440;
const NO_PROMISE: &str = "No freshness promise is set.";

/// The freshness promise stored as JSON text in [`FRESHNESS_PROPERTY`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Freshness {
    /// The deadline: a 24-hour clock time, `HH:MM`.
    pub by: String,
    /// The IANA zone the deadline is a wall-clock time in.
    pub timezone: String,
    /// How long after the deadline the table is still `amber`.
    #[serde(default = "default_grace_minutes")]
    pub grace_minutes: u32,
    /// When Nova wrote the promise (RFC 3339); the baseline of a table with no snapshot.
    pub set_at: String,
}

fn default_grace_minutes() -> u32 {
    FRESHNESS_DEFAULT_GRACE_MINUTES
}

/// One table's freshness: the promise, the newest commit and the status both imply.
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableFreshness {
    /// The stored promise, or `null` when the table carries none (or one that does not parse).
    pub promise: Option<Freshness>,
    /// `grey`, `green`, `amber` or `red`.
    pub status: String,
    /// The newest commit, RFC 3339, or `null` for a table with no snapshot.
    pub last_write: Option<String>,
    /// The most recent deadline at or before now, RFC 3339, or `null`.
    pub due_at: Option<String>,
    /// The first deadline strictly after now, RFC 3339, or `null`.
    pub next_due_at: Option<String>,
    /// One sentence a person or an assistant can repeat.
    pub detail: String,
    /// Whether the interim rule lets this caller set the promise.
    pub can_edit: bool,
}

/// One promised table in `GET /api/catalog/health`.
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListedHealth {
    pub catalog: String,
    pub namespace: String,
    pub name: String,
    /// `grey`, `green`, `amber` or `red`; a listed table is never `grey`.
    pub status: String,
    /// The newest commit, RFC 3339, or `null` for a table with no snapshot.
    pub last_write: Option<String>,
    /// The most recent deadline at or before now, RFC 3339, or `null`.
    pub due_at: Option<String>,
    /// The first deadline strictly after now, RFC 3339, or `null`.
    pub next_due_at: Option<String>,
    /// Whether the interim rule lets this caller set the promise.
    pub can_edit: bool,
}

/// `GET /api/catalog/health`: every table that carries a freshness promise, worst first.
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CatalogHealth {
    pub tables: Vec<ListedHealth>,
    /// How many tables were read; `tables` stops at 200.
    pub checked: usize,
}

/// `PUT /api/catalog/tables/{ns}/{tbl}/health` body.
///
/// `by: null` removes the promise; an absent key changes nothing.
#[derive(Debug, Default, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FreshnessUpdate {
    /// The deadline (`null` removes the promise).
    // `present` separates an absent key from a `null` one; a plain `Option<Option<String>>`
    // folds both into `None`, which would make `by: null` unrepresentable.
    #[serde(default, deserialize_with = "present")]
    #[schema(value_type = Option<String>)]
    pub by: Option<Option<String>>,
    /// The IANA zone of the deadline; required with a deadline.
    pub timezone: Option<String>,
    /// How long after the deadline the table is still `amber` (0 to 1440, default 60).
    pub grace_minutes: Option<u32>,
}

/// `PUT /api/catalog/tables/{ns}/{tbl}/documentation` body.
///
/// Every key is optional. Absent changes nothing; `null` removes the property; an empty tag
/// list and a blank description also remove. A field whose value equals what the table
/// already stores is not a change.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct DocumentationUpdate {
    /// New table description (`null` removes it).
    #[serde(default, deserialize_with = "present")]
    #[schema(value_type = Option<String>)]
    pub description: Option<Option<String>>,
    /// New tag list (`null` or `[]` removes them).
    #[serde(default, deserialize_with = "present")]
    #[schema(value_type = Option<Vec<String>>)]
    pub tags: Option<Option<Vec<String>>>,
    /// New owner group (`null` removes it).
    #[serde(default, deserialize_with = "present")]
    #[schema(value_type = Option<String>)]
    pub owner: Option<Option<String>>,
    /// New domain (`null` removes it).
    #[serde(default, deserialize_with = "present")]
    #[schema(value_type = Option<String>)]
    pub domain: Option<Option<String>>,
    /// New guide (`null` or blank removes it).
    #[serde(default, deserialize_with = "present")]
    #[schema(value_type = Option<String>)]
    pub guide: Option<Option<String>>,
}

/// `PUT /api/catalog/tables/{ns}/{tbl}/columns/{column}/description` body.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct ColumnDescriptionUpdate {
    /// New column description (`null` removes it).
    #[serde(default, deserialize_with = "present")]
    #[schema(value_type = Option<String>)]
    pub description: Option<Option<String>>,
}

/// Distinguish "key absent" (`None`, through `#[serde(default)]`) from "key present with
/// `null`" (`Some(None)`) — serde's plain `Option<Option<T>>` folds both into `None`.
fn present<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

/// A refusal with one sentence and the status it answers.
#[derive(Debug)]
struct DocError {
    status: StatusCode,
    message: String,
}

impl DocError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    #[cfg(test)]
    fn status(&self) -> StatusCode {
        self.status
    }

    #[cfg(test)]
    fn message(&self) -> &str {
        &self.message
    }
}

impl IntoResponse for DocError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

/// A failed load/commit, mapped to the sentence the caller sees. Upstream failures are 502,
/// never 500 (rule U12 of `design/ui-conformance.md`); a missing table is 404.
#[derive(Debug)]
enum CatalogError {
    Unreachable,
    Malformed,
    NotFound,
    Busy,
    Refused(StatusCode),
    InvalidName(String),
}

fn catalog_fail(e: CatalogError) -> DocError {
    match e {
        CatalogError::Unreachable => {
            DocError::new(StatusCode::BAD_GATEWAY, "the catalog is not reachable")
        }
        CatalogError::Malformed => DocError::new(
            StatusCode::BAD_GATEWAY,
            "the catalog returned an unexpected response",
        ),
        CatalogError::NotFound => DocError::new(StatusCode::NOT_FOUND, "no such table"),
        CatalogError::Busy => {
            DocError::new(StatusCode::BAD_GATEWAY, "the catalog is busy, try again")
        }
        CatalogError::Refused(status) => DocError::new(
            StatusCode::BAD_GATEWAY,
            format!("the catalog answered {status}"),
        ),
        CatalogError::InvalidName(what) => {
            DocError::new(StatusCode::BAD_REQUEST, format!("invalid name '{what}'"))
        }
    }
}

/// The bits of a load-table answer this module reads. `columns` is the **current** schema's
/// field list — Lakekeeper answers every historical schema, so `schemas[0]` is not it.
struct Loaded {
    properties: serde_json::Map<String, Value>,
    columns: Vec<LoadedColumn>,
    /// The newest commit's time (`snapshots[].timestamp-ms` of `current-snapshot-id`).
    last_write: Option<DateTime<Utc>>,
}

struct LoadedColumn {
    name: String,
    ty: String,
    doc: Option<String>,
}

/// Lakekeeper's catalog REST under one warehouse, with the caller's authorization (when the
/// platform forwards it). The route resolves the warehouse id and the catalog name; this
/// builds URLs and names the catalog in the statements and edit locks it keys on.
struct Catalog<'a> {
    http: &'a reqwest::Client,
    /// `NOVA_LAKEKEEPER_URL/catalog/v1/<warehouse-id>`
    base: String,
    /// The warehouse id `base` is built from; `catalog_json` takes paths, not URLs.
    id: &'a str,
    authorization: Option<&'a str>,
    /// The catalog's name (`warehouse`, `finance`, …), for SQL and the edit lock.
    name: &'a str,
}

impl Catalog<'_> {
    fn table_url(&self, ns: &str, tbl: &str) -> Result<String, CatalogError> {
        let (Some(ns), Some(tbl)) = (catalog::path_segment(ns), catalog::path_segment(tbl)) else {
            return Err(CatalogError::InvalidName(format!("{ns}.{tbl}")));
        };
        Ok(format!("{}/namespaces/{ns}/tables/{tbl}", self.base))
    }

    /// `GET …/tables/<table>`: the properties and the current schema's columns.
    async fn load(&self, ns: &str, tbl: &str) -> Result<Loaded, CatalogError> {
        let url = self.table_url(ns, tbl)?;
        let mut request = self.http.get(&url);
        if let Some(header) = self.authorization {
            request = request.header(reqwest::header::AUTHORIZATION, header);
        }
        let response = request.send().await.map_err(|e| {
            warn!(error = %e, "lakekeeper load-table failed");
            CatalogError::Unreachable
        })?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        match status.as_u16() {
            200 => parse_loaded(&body),
            404 => Err(CatalogError::NotFound),
            code => {
                warn!(status = code, "lakekeeper load-table refused");
                Err(CatalogError::Refused(status))
            }
        }
    }

    /// One Iceberg `CommitTable` carrying `set-properties` and/or `remove-properties`.
    ///
    /// Lakekeeper 0.12.2 answers 500/503 while another commit to the same table is in
    /// flight; those are retried (200, 400 ms) because they are contention, not a refusal.
    /// Three attempts in total, then [`CatalogError::Busy`]. The body's `Error ID` is
    /// logged when the catalog includes one — it is the only way to correlate the line with
    /// Lakekeeper's own log.
    async fn commit(&self, ns: &str, tbl: &str, body: &Value) -> Result<(), CatalogError> {
        let url = self.table_url(ns, tbl)?;
        const ATTEMPTS: usize = 3;
        let mut wait = std::time::Duration::from_millis(200);
        for attempt in 1..=ATTEMPTS {
            let mut request = self.http.post(&url).json(body);
            if let Some(header) = self.authorization {
                request = request.header(reqwest::header::AUTHORIZATION, header);
            }
            let response = request.send().await.map_err(|e| {
                warn!(attempt, error = %e, "lakekeeper commit failed");
                CatalogError::Unreachable
            })?;
            let status = response.status();
            if status.is_success() {
                return Ok(());
            }
            let text = response.text().await.unwrap_or_default();
            let error_id = catalog_error_id(&text);
            match status.as_u16() {
                404 => return Err(CatalogError::NotFound),
                500 | 503 if attempt < ATTEMPTS => {
                    warn!(
                        attempt,
                        status = status.as_u16(),
                        error_id = error_id.as_deref().unwrap_or(""),
                        "catalog commit contended; retrying"
                    );
                    tokio::time::sleep(wait).await;
                    wait *= 2;
                }
                500 | 503 => {
                    warn!(
                        attempt,
                        status = status.as_u16(),
                        error_id = error_id.as_deref().unwrap_or(""),
                        "catalog commit still contended; giving up"
                    );
                    return Err(CatalogError::Busy);
                }
                _ => {
                    warn!(
                        attempt,
                        status = status.as_u16(),
                        error_id = error_id.as_deref().unwrap_or(""),
                        "catalog commit refused"
                    );
                    return Err(CatalogError::Refused(status));
                }
            }
        }
        Err(CatalogError::Busy)
    }
}

/// Lakekeeper's error id, when its body carries one. Accepts the JSON shapes seen in the
/// wild (`error.id`, `error.errorId`, `id`) and falls back to an `Error ID: <id>` in the
/// message. The text is never echoed to a client.
fn catalog_error_id(body: &str) -> Option<String> {
    if let Ok(value) = serde_json::from_str::<Value>(body) {
        for pointer in ["/error/id", "/error/errorId", "/id", "/errorId"] {
            if let Some(id) = value.pointer(pointer).and_then(Value::as_str) {
                return Some(id.to_string());
            }
        }
    }
    let rest = body.split("Error ID").nth(1)?;
    let id: String = rest
        .trim_start_matches([':', ' ', '"', '\''])
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '-')
        .collect();
    (!id.is_empty()).then_some(id)
}

fn parse_loaded(body: &str) -> Result<Loaded, CatalogError> {
    let value: Value = serde_json::from_str(body).map_err(|_| CatalogError::Malformed)?;
    let metadata = value.get("metadata").ok_or(CatalogError::Malformed)?;
    let properties = match metadata.get("properties") {
        Some(Value::Object(map)) => map.clone(),
        None => serde_json::Map::new(),
        _ => return Err(CatalogError::Malformed),
    };
    let current = metadata
        .get("current-schema-id")
        .and_then(Value::as_i64)
        .ok_or(CatalogError::Malformed)?;
    let schema = metadata
        .get("schemas")
        .and_then(Value::as_array)
        .and_then(|schemas| {
            schemas
                .iter()
                .find(|s| s.get("schema-id").and_then(Value::as_i64) == Some(current))
        })
        .ok_or(CatalogError::Malformed)?;
    let fields = schema
        .get("fields")
        .and_then(Value::as_array)
        .ok_or(CatalogError::Malformed)?;
    let mut columns = Vec::with_capacity(fields.len());
    for field in fields {
        let name = field
            .get("name")
            .and_then(Value::as_str)
            .ok_or(CatalogError::Malformed)?;
        let ty = field.get("type").ok_or(CatalogError::Malformed)?;
        // A primitive type is a string in JSON; a struct/list/map is an object. The
        // documented answer is the type as text, so a complex type becomes its JSON text.
        let ty = match ty {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        columns.push(LoadedColumn {
            name: name.to_string(),
            ty,
            doc: field.get("doc").and_then(Value::as_str).map(str::to_string),
        });
    }
    Ok(Loaded {
        properties,
        columns,
        last_write: last_write_of(metadata),
    })
}

/// The commit time of the newest snapshot, when the table has one. `current-snapshot-id`
/// names one entry of `snapshots`; a table with no snapshot (`-1` or no entry) has none.
/// Anything missing or malformed answers `None`, never an error: a table whose snapshot list
/// a person edited must still load.
fn last_write_of(metadata: &Value) -> Option<DateTime<Utc>> {
    let current = metadata
        .get("current-snapshot-id")
        .and_then(Value::as_i64)?;
    if current == -1 {
        return None;
    }
    let timestamp = metadata
        .get("snapshots")?
        .as_array()?
        .iter()
        .find(|snapshot| snapshot.get("snapshot-id").and_then(Value::as_i64) == Some(current))?
        .get("timestamp-ms")?
        .as_i64()?;
    DateTime::from_timestamp_millis(timestamp)
}

fn property(properties: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    properties
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Tags are stored as one comma-separated property. Split, trim and drop empties on the way
/// out; the write side always stores the sorted canonical form.
fn split_tags(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string)
        .collect()
}

// ---------------------------------------------------------------------------
// The freshness promise (#402 slice 1): `stardelt.freshness`
// ---------------------------------------------------------------------------

/// A deadline as it is stored: trimmed `HH:MM` on a 24-hour clock.
fn checked_by(raw: &str) -> Result<String, String> {
    let text = raw.trim();
    match by_clock(text) {
        Some(_) => Ok(text.to_string()),
        None => Err("the deadline is a 24-hour clock time like 07:00".into()),
    }
}

/// The hour and minute of an `HH:MM` deadline; `None` when the clock is not a wall time.
fn by_clock(by: &str) -> Option<(u32, u32)> {
    let bytes = by.as_bytes();
    if bytes.len() != 5 || bytes[2] != b':' {
        return None;
    }
    if !bytes
        .iter()
        .enumerate()
        .all(|(i, b)| i == 2 || b.is_ascii_digit())
    {
        return None;
    }
    let hour = by.get(0..2)?.parse::<u32>().ok()?;
    let minute = by.get(3..5)?.parse::<u32>().ok()?;
    (hour <= 23 && minute <= 59).then_some((hour, minute))
}

fn checked_timezone(raw: &str) -> Result<String, String> {
    let text = raw.trim();
    match Tz::from_str(text) {
        Ok(_) => Ok(text.to_string()),
        Err(_) => Err("the timezone is an IANA name like Europe/Berlin".into()),
    }
}

/// The stored promise, or `None` when the text is not one. A hand-edited property that does
/// not parse is **no promise** (grey), never an error: the status route keeps answering.
fn parse_freshness(raw: &str) -> Option<Freshness> {
    let promise: Freshness = serde_json::from_str(raw).ok()?;
    let by = checked_by(&promise.by).ok()?;
    let timezone = checked_timezone(&promise.timezone).ok()?;
    if promise.grace_minutes > FRESHNESS_MAX_GRACE_MINUTES {
        return None;
    }
    DateTime::parse_from_rfc3339(&promise.set_at).ok()?;
    Some(Freshness {
        by,
        timezone,
        ..promise
    })
}

/// One wall-clock deadline in `zone`, or `None` when the zone skips that wall clock at a DST
/// jump. An ambiguous wall clock is its first occurrence — the rule `pipelines.rs`'s
/// `next_runs` states.
fn instant_on(day: chrono::NaiveDate, hour: u32, minute: u32, zone: Tz) -> Option<DateTime<Utc>> {
    let local = day.and_hms_opt(hour, minute, 0)?;
    match zone.from_local_datetime(&local) {
        LocalResult::None => None,
        LocalResult::Single(at) => Some(at.with_timezone(&Utc)),
        LocalResult::Ambiguous(first, _) => Some(first.with_timezone(&Utc)),
    }
}

/// The most recent deadline at or before `now`, and the first one strictly after it.
///
/// "Most recent" tries today, then yesterday; "next" tries today, tomorrow and the day after.
/// A wall clock the zone skips does not exist and is not a deadline, so the previous day's is.
fn deadlines(
    by: &str,
    zone: Tz,
    now: DateTime<Utc>,
) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    let Some((hour, minute)) = by_clock(by) else {
        return (None, None);
    };
    let today = now.with_timezone(&zone).date_naive();
    let mut due = None;
    for back in 0..=1u64 {
        let Some(day) = today.checked_sub_days(Days::new(back)) else {
            continue;
        };
        if let Some(at) = instant_on(day, hour, minute, zone)
            && at <= now
        {
            due = Some(at);
            break;
        }
    }
    let mut next = None;
    for ahead in 0..=2u64 {
        let Some(day) = today.checked_add_days(Days::new(ahead)) else {
            continue;
        };
        if let Some(at) = instant_on(day, hour, minute, zone)
            && at > now
        {
            next = Some(at);
            break;
        }
    }
    (due, next)
}

/// The status of one promise at `now`: the status, the most recent deadline, the next one and
/// the sentence a person can repeat.
fn freshness_health(
    promise: &Freshness,
    last_write: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> (String, Option<DateTime<Utc>>, Option<DateTime<Utc>>, String) {
    const FRESH: &str = "Fresh: the newest write is after the deadline.";
    const AMBER: &str = "Late, within the grace period.";
    const RED: &str =
        "Late: the newest write is before the deadline and the grace period has passed.";
    let Ok(zone) = Tz::from_str(&promise.timezone) else {
        return ("green".into(), None, None, FRESH.into());
    };
    let (due, next) = deadlines(&promise.by, zone, now);
    // A zone oddity that names no deadline still leaves the route answering: a valid promise
    // is never an error (#402).
    let Some(due) = due else {
        return ("green".into(), None, next, FRESH.into());
    };
    // The baseline: the newest commit, or — for a table with no snapshot yet — the moment the
    // promise was set, so a promise on an empty table is not instantly red.
    let baseline = last_write.or_else(|| {
        DateTime::parse_from_rfc3339(&promise.set_at)
            .ok()
            .map(|at| at.with_timezone(&Utc))
    });
    let grace = Duration::minutes(i64::from(promise.grace_minutes));
    let (status, detail) = match baseline {
        Some(at) if at >= due => ("green", FRESH),
        _ if now < due + grace => ("amber", AMBER),
        _ => ("red", RED),
    };
    (status.to_string(), Some(due), next, detail.to_string())
}

fn rfc3339(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Now, in UTC. The workspace builds `chrono` without its `clock` feature (`nova/Cargo.toml`),
/// so `Utc::now()` does not exist; the standard library's clock does.
fn now() -> DateTime<Utc> {
    DateTime::<Utc>::from(std::time::SystemTime::now())
}

/// The read answer for a loaded table: the promise, its status and what the caller may do.
fn health_of(loaded: &Loaded, who: &Identity) -> TableFreshness {
    let can_edit = may_edit(
        who,
        property(&loaded.properties, "stardelt.owner").as_deref(),
    )
    .is_ok();
    let last_write = loaded.last_write;
    let promise = property(&loaded.properties, FRESHNESS_PROPERTY)
        .as_deref()
        .and_then(parse_freshness);
    let Some(promise) = promise else {
        return TableFreshness {
            promise: None,
            status: "grey".into(),
            last_write: last_write.map(rfc3339),
            due_at: None,
            next_due_at: None,
            detail: NO_PROMISE.into(),
            can_edit,
        };
    };
    let (status, due, next, detail) = freshness_health(&promise, last_write, now());
    TableFreshness {
        promise: Some(promise),
        status,
        last_write: last_write.map(rfc3339),
        due_at: due.map(rfc3339),
        next_due_at: next.map(rfc3339),
        detail,
        can_edit,
    }
}

/// The interim rule: who may roll back this table.
pub fn may_rollback(identity: &Identity, catalog_owner: Option<&str>) -> bool {
    if identity.is_admin {
        return true;
    }
    match catalog_owner {
        Some(owner) => in_group(&identity.groups, owner),
        None => false,
    }
}

pub fn rollback_rule(identity: &Identity, catalog_owner: Option<&str>) -> String {
    if may_rollback(identity, catalog_owner) {
        if identity.is_admin {
            return "You can roll back because you are an administrator.".into();
        }
        if let Some(owner) = catalog_owner {
            return format!(
                "You can roll back because you belong to the catalog owner group {owner}."
            );
        }
    }
    if let Some(owner) = catalog_owner {
        format!("Rolling a table back needs the catalog owner group {owner} or an administrator.")
    } else {
        "Rolling a table back needs the catalog owner group or an administrator.".into()
    }
}

/// Build the read answer from a load-table result. The rule is evaluated for `who`.
fn documentation(
    loaded: &Loaded,
    who: &Identity,
    can_rollback: bool,
    rollback_rule: String,
) -> TableDocumentation {
    let owner = property(&loaded.properties, "stardelt.owner");
    let can_edit = may_edit(who, owner.as_deref()).is_ok();
    TableDocumentation {
        description: property(&loaded.properties, "comment"),
        tags: property(&loaded.properties, "stardelt.tags")
            .map(|raw| split_tags(&raw))
            .unwrap_or_default(),
        owner: owner.clone(),
        domain: property(&loaded.properties, "stardelt.domain"),
        guide: property(&loaded.properties, "stardelt.guide"),
        columns: loaded
            .columns
            .iter()
            .map(|c| ColumnDocumentation {
                name: c.name.clone(),
                r#type: c.ty.clone(),
                description: c.doc.clone(),
            })
            .collect(),
        can_edit,
        edit_rule: edit_rule(who, owner.as_deref()),
        can_rollback,
        rollback_rule,
    }
}

/// Group names compare exactly, or with Keycloak's leading `/` (full group path) — the same
/// rule `auth::has_group` uses.
fn in_group(groups: &[String], wanted: &str) -> bool {
    groups
        .iter()
        .any(|g| g == wanted || g.strip_prefix('/') == Some(wanted))
}

/// The interim rule: who may edit this table's documentation.
///
/// Enforced by Nova only — the catalog does not enforce it yet, so anyone who reaches
/// Lakekeeper or Trino directly can still change these properties. A no-owner table may be
/// edited by anybody, but the **write** must then set `owner` to one of the caller's own
/// groups (see [`claim_allowed`]); this function only answers whether editing is allowed at
/// all.
pub fn may_edit(identity: &Identity, current_owner: Option<&str>) -> Result<(), String> {
    if identity.is_admin {
        return Ok(());
    }
    match current_owner {
        Some(owner) if in_group(&identity.groups, owner) => Ok(()),
        Some(owner) => Err(format!(
            "only members of the owner group {owner} and administrators may edit this"
        )),
        None => Ok(()),
    }
}

/// The sentence the read answer carries for `canEdit`. Success cases say why; a refusal
/// repeats the rule in a sentence fit for a person.
fn edit_rule(identity: &Identity, current_owner: Option<&str>) -> String {
    if identity.is_admin {
        return "You can edit because you are an administrator.".into();
    }
    match current_owner {
        Some(owner) if in_group(&identity.groups, owner) => {
            format!("You can edit because you belong to the owner group {owner}.")
        }
        Some(owner) => {
            format!("Only members of the owner group {owner} and administrators may edit this.")
        }
        None => {
            "You can edit, but you must set an owner group of your own: the table has no owner yet."
                .into()
        }
    }
}

/// The no-owner clause: a non-administrator may only edit an ownerless table while claiming
/// it for one of their own groups. Administrators may set any valid group name (or none).
fn claim_allowed(
    who: &Identity,
    current_owner: Option<&str>,
    requested_owner: &Option<Option<String>>,
) -> bool {
    if current_owner.is_some() || who.is_admin {
        return true;
    }
    match requested_owner {
        Some(Some(group)) => in_group(&who.groups, group),
        _ => false,
    }
}

/// A description as it is stored, or `None` when it should be removed. Trims, caps the
/// length and refuses a NUL.
fn checked_description(raw: Option<String>) -> Result<Option<String>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let text = raw.trim();
    if text.chars().count() > MAX_DESCRIPTION_CHARS {
        return Err(format!(
            "description must be at most {MAX_DESCRIPTION_CHARS} characters"
        ));
    }
    if text.contains('\0') {
        return Err("description must not contain a NUL character".into());
    }
    if text.is_empty() {
        return Ok(None);
    }
    Ok(Some(text.to_string()))
}

/// A guide as it is stored, or `None` when it should be removed. Trims only the ends — the
/// line breaks of a plain-text or Markdown guide stay as written — caps the length and
/// refuses a NUL.
fn checked_guide(raw: Option<String>) -> Result<Option<String>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let text = raw.trim();
    if text.chars().count() > MAX_GUIDE_CHARS {
        return Err(format!(
            "guide must be at most {MAX_GUIDE_CHARS} characters"
        ));
    }
    if text.contains('\0') {
        return Err("guide must not contain a NUL character".into());
    }
    if text.is_empty() {
        return Ok(None);
    }
    Ok(Some(text.to_string()))
}

/// A tag: 1 to 32 lower-case ASCII letters, digits, `-` or `_`, starting with a letter or
/// digit — `^[a-z0-9][a-z0-9_-]{0,31}$`.
fn tag_ok(tag: &str) -> bool {
    let bytes = tag.as_bytes();
    (1..=32).contains(&bytes.len())
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-' || *b == b'_')
}

/// A domain: 1 to 40 lower-case ASCII letters, digits, `-` or `_`, starting with a letter or
/// digit — `^[a-z0-9][a-z0-9_-]{0,39}$`.
fn domain_ok(domain: &str) -> bool {
    let bytes = domain.as_bytes();
    (1..=40).contains(&bytes.len())
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-' || *b == b'_')
}

/// A validated write: each `Some` is a key that was present. `Some(None)` removes, and for
/// tags an empty list is normalised to `None` (remove) before the diff.
struct Requested {
    description: Option<Option<String>>,
    tags: Option<Option<Vec<String>>>,
    owner: Option<Option<String>>,
    domain: Option<Option<String>>,
    guide: Option<Option<String>>,
}

fn validate_update(req: DocumentationUpdate) -> Result<Requested, String> {
    let description = match req.description {
        None => None,
        Some(raw) => Some(checked_description(raw)?),
    };
    let tags = match req.tags {
        None => None,
        Some(None) => Some(None),
        Some(Some(tags)) => {
            if tags.len() > MAX_TAGS {
                return Err(format!("at most {MAX_TAGS} tags are allowed"));
            }
            if tags.is_empty() {
                Some(None)
            } else {
                let mut seen = BTreeSet::new();
                for tag in &tags {
                    if !tag_ok(tag) {
                        return Err(
                            "tags must be 1 to 32 lower-case letters, digits, dashes or \
                             underscores, starting with a letter or digit"
                                .into(),
                        );
                    }
                    if !seen.insert(tag.as_str()) {
                        return Err("tags must be unique".into());
                    }
                }
                let mut sorted = tags;
                sorted.sort();
                Some(Some(sorted))
            }
        }
    };
    let owner = match req.owner {
        None => None,
        Some(None) => Some(None),
        Some(Some(owner)) => {
            if !crate::connections::group_ok(&owner) {
                return Err(
                    "owner must be a group name: 1 to 64 letters, digits, dots, dashes or \
                     underscores, starting with a letter or digit"
                        .into(),
                );
            }
            Some(Some(owner))
        }
    };
    let domain = match req.domain {
        None => None,
        Some(None) => Some(None),
        Some(Some(domain)) => {
            if !domain_ok(&domain) {
                return Err(
                    "domain must be 1 to 40 lower-case letters, digits, dashes or underscores, \
                     starting with a letter or digit"
                        .into(),
                );
            }
            Some(Some(domain))
        }
    };
    let guide = match req.guide {
        None => None,
        Some(raw) => Some(checked_guide(raw)?),
    };
    Ok(Requested {
        description,
        tags,
        owner,
        domain,
        guide,
    })
}

/// The per-table edit lock. Held for the whole write, including the load that decides the
/// rule and the statement a column write runs. The catalog is part of the key: `sales.orders`
/// in `finance` and in `warehouse` are two tables.
fn table_lock(state: &AppState, catalog: &str, ns: &str, tbl: &str) -> Arc<tokio::sync::Mutex<()>> {
    let key = format!("{catalog}.{ns}.{tbl}");
    Arc::clone(
        state
            .table_locks
            .lock()
            .expect("table locks poisoned")
            .entry(key)
            .or_default(),
    )
}

async fn catalog_owner_from_kube(state: &AppState, catalog_name: &str) -> Option<String> {
    let client = state.kube.clone()?;
    match crate::admin_catalogs::catalogs_api(client)
        .get_opt(catalog_name)
        .await
    {
        Ok(Some(object)) => {
            let value = serde_json::to_value(object).ok()?;
            value
                .get("spec")?
                .get("owner")?
                .as_str()
                .map(|s| s.to_string())
        }
        _ => None,
    }
}

/// `GET /api/catalog/tables/{ns}/{tbl}/documentation`.
async fn read_documentation(
    state: &AppState,
    catalog: &Catalog<'_>,
    who: &Identity,
    ns: &str,
    tbl: &str,
) -> Result<TableDocumentation, DocError> {
    let loaded = catalog.load(ns, tbl).await.map_err(catalog_fail)?;
    let catalog_owner = catalog_owner_from_kube(state, catalog.name).await;
    let can_rollback_v = may_rollback(who, catalog_owner.as_deref());
    let rollback_rule_v = rollback_rule(who, catalog_owner.as_deref());
    Ok(documentation(&loaded, who, can_rollback_v, rollback_rule_v))
}

/// `PUT /api/catalog/tables/{ns}/{tbl}/documentation`.
///
/// Validation runs before the lock and before any request. The lock covers the load that
/// decides the rule, the commit and the fresh load the answer is built from.
async fn write_documentation(
    state: &AppState,
    catalog: &Catalog<'_>,
    who: &Identity,
    ns: &str,
    tbl: &str,
    req: DocumentationUpdate,
) -> Result<TableDocumentation, DocError> {
    let requested = validate_update(req).map_err(|e| DocError::new(StatusCode::BAD_REQUEST, e))?;
    let lock = table_lock(state, catalog.name, ns, tbl);
    let _guard = lock.lock().await;

    let loaded = catalog.load(ns, tbl).await.map_err(catalog_fail)?;
    let catalog_owner = catalog_owner_from_kube(state, catalog.name).await;
    let can_rollback_v = may_rollback(who, catalog_owner.as_deref());
    let rollback_rule_v = rollback_rule(who, catalog_owner.as_deref());
    let current = documentation(&loaded, who, can_rollback_v, rollback_rule_v);
    may_edit(who, current.owner.as_deref()).map_err(|e| DocError::new(StatusCode::FORBIDDEN, e))?;
    if !claim_allowed(who, current.owner.as_deref(), &requested.owner) {
        return Err(DocError::new(
            StatusCode::BAD_REQUEST,
            "choose an owner: one of your groups",
        ));
    }

    let mut set: BTreeMap<String, String> = BTreeMap::new();
    let mut remove: Vec<String> = Vec::new();
    let mut changed: Vec<&str> = Vec::new();

    if let Some(next) = &requested.description
        && next.as_deref() != current.description.as_deref()
    {
        changed.push("description");
        match next {
            Some(text) => {
                set.insert("comment".into(), text.clone());
            }
            None => remove.push("comment".into()),
        }
    }
    if let Some(next) = &requested.tags {
        let next = next.clone().unwrap_or_default();
        if next != current.tags {
            changed.push("tags");
            if next.is_empty() {
                remove.push("stardelt.tags".into());
            } else {
                set.insert("stardelt.tags".into(), next.join(","));
            }
        }
    }
    if let Some(next) = &requested.owner
        && next.as_deref() != current.owner.as_deref()
    {
        changed.push("owner");
        match next {
            Some(owner) => {
                set.insert("stardelt.owner".into(), owner.clone());
            }
            None => remove.push("stardelt.owner".into()),
        }
    }
    if let Some(next) = &requested.domain
        && next.as_deref() != current.domain.as_deref()
    {
        changed.push("domain");
        match next {
            Some(domain) => {
                set.insert("stardelt.domain".into(), domain.clone());
            }
            None => remove.push("stardelt.domain".into()),
        }
    }
    if let Some(next) = &requested.guide
        && next.as_deref() != current.guide.as_deref()
    {
        changed.push("guide");
        match next {
            Some(guide) => {
                set.insert("stardelt.guide".into(), guide.clone());
            }
            None => remove.push("stardelt.guide".into()),
        }
    }

    if changed.is_empty() {
        return Ok(current);
    }

    let mut updates = Vec::new();
    if !set.is_empty() {
        updates.push(json!({ "action": "set-properties", "updates": set }));
    }
    if !remove.is_empty() {
        updates.push(json!({ "action": "remove-properties", "removals": remove }));
    }
    let body = json!({ "requirements": [], "updates": updates });
    catalog.commit(ns, tbl, &body).await.map_err(catalog_fail)?;

    audit::catalog_doc_event(who, &format!("{ns}.{tbl}"), &changed);
    let loaded = catalog.load(ns, tbl).await.map_err(catalog_fail)?;
    let catalog_owner = catalog_owner_from_kube(state, catalog.name).await;
    let can_rollback_v = may_rollback(who, catalog_owner.as_deref());
    let rollback_rule_v = rollback_rule(who, catalog_owner.as_deref());
    let doc = documentation(&loaded, who, can_rollback_v, rollback_rule_v);
    crate::catalog_doc_store::record(state.db.as_ref(), catalog.name, ns, tbl, &doc).await;
    Ok(doc)
}

/// `GET /api/catalog/tables/{ns}/{tbl}/health`: the table's freshness, evaluated at the read.
async fn freshness_for(
    _state: &Arc<AppState>,
    catalog: &Catalog<'_>,
    who: &Identity,
    ns: &str,
    tbl: &str,
) -> Result<TableFreshness, DocError> {
    let loaded = catalog.load(ns, tbl).await.map_err(catalog_fail)?;
    Ok(health_of(&loaded, who))
}

/// Most tables `GET /api/catalog/health` reads in one call.
const HEALTH_LIST_MAX_TABLES: usize = 200;

/// The order a person and an assistant read the list in: the broken tables first.
fn health_rank(status: &str) -> u8 {
    match status {
        "red" => 0,
        "amber" => 1,
        "green" => 2,
        _ => 3,
    }
}

/// `GET /api/catalog/health`: every table that carries a freshness promise, worst first.
///
/// The namespace and table lists are the Lakekeeper calls `namespaces_list` and
/// `tables_list` make; only single-level namespaces are read, and views are never asked
/// for (a view cannot carry the property). A table whose load fails is skipped: a table
/// that vanished between the list and the load is not an error. The status is the same
/// evaluation [`health_of`] makes for one table; that is the only rule.
async fn health_list(
    state: &Arc<AppState>,
    who: &Identity,
    catalog: &Catalog<'_>,
) -> Result<CatalogHealth, DocError> {
    let listing_error = |response: Response| {
        DocError::new(
            response.status(),
            "the catalog did not answer the table list",
        )
    };
    let answer = catalog::catalog_json(state, format!("/catalog/v1/{}/namespaces", catalog.id))
        .await
        .map_err(listing_error)?;
    let mut namespaces: Vec<String> = answer["namespaces"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|levels| match levels.as_array()?.as_slice() {
            [only] => only.as_str().map(str::to_string),
            _ => None,
        })
        .collect();
    namespaces.sort();

    let mut tables = Vec::new();
    let mut checked = 0usize;
    'namespaces: for ns in &namespaces {
        let Some(segment) = catalog::path_segment(ns) else {
            continue;
        };
        let list = catalog::catalog_json(
            state,
            format!("/catalog/v1/{}/namespaces/{segment}/tables", catalog.id),
        )
        .await
        .map_err(listing_error)?;
        let mut names: Vec<String> = list["identifiers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|id| id["name"].as_str().map(str::to_string))
            .collect();
        names.sort();
        for name in names {
            if checked >= HEALTH_LIST_MAX_TABLES {
                break 'namespaces;
            }
            // A table that vanished between the list and the load is not an error: the next
            // call lists it again, or it is gone for good.
            let Ok(loaded) = catalog.load(ns, &name).await else {
                continue;
            };
            checked += 1;
            let health = health_of(&loaded, who);
            if health.promise.is_none() {
                continue;
            }
            tables.push(ListedHealth {
                catalog: catalog.name.to_string(),
                namespace: ns.clone(),
                name,
                status: health.status,
                last_write: health.last_write,
                due_at: health.due_at,
                next_due_at: health.next_due_at,
                can_edit: health.can_edit,
            });
        }
    }
    tables.sort_by(|a, b| {
        (health_rank(&a.status), &a.namespace, &a.name).cmp(&(
            health_rank(&b.status),
            &b.namespace,
            &b.name,
        ))
    });
    Ok(CatalogHealth { tables, checked })
}

/// The validated write: remove the promise, or store this one.
enum FreshnessWrite {
    Remove,
    Set(Freshness),
}

/// `PUT /api/catalog/tables/{ns}/{tbl}/health`.
///
/// Validation runs before the lock and before any request. The lock covers the load that
/// decides the rule and the commit; the answer is built from a fresh load.
async fn write_freshness(
    state: &Arc<AppState>,
    catalog: &Catalog<'_>,
    who: &Identity,
    ns: &str,
    tbl: &str,
    req: FreshnessUpdate,
) -> Result<TableFreshness, DocError> {
    let write = validate_freshness(req)?;
    let lock = table_lock(state, catalog.name, ns, tbl);
    let _guard = lock.lock().await;

    let loaded = catalog.load(ns, tbl).await.map_err(catalog_fail)?;
    may_edit(
        who,
        property(&loaded.properties, "stardelt.owner").as_deref(),
    )
    .map_err(|e| DocError::new(StatusCode::FORBIDDEN, e))?;

    let current_text = property(&loaded.properties, FRESHNESS_PROPERTY);
    // A write of the promise that is already stored sends no commit. It compares the promise
    // itself, not only the text: Nova stamps a new `setAt` on every write, so the text would
    // differ every second even when nothing else changed.
    let unchanged = match &write {
        FreshnessWrite::Remove => current_text.is_none(),
        FreshnessWrite::Set(next) => current_text
            .as_deref()
            .and_then(parse_freshness)
            .is_some_and(|current| {
                current.by == next.by
                    && current.timezone == next.timezone
                    && current.grace_minutes == next.grace_minutes
            }),
    };
    if unchanged {
        return Ok(health_of(&loaded, who));
    }

    let updates = match &write {
        FreshnessWrite::Remove => {
            vec![json!({ "action": "remove-properties", "removals": [FRESHNESS_PROPERTY] })]
        }
        FreshnessWrite::Set(promise) => {
            let text = serde_json::to_string(promise).map_err(|_| {
                DocError::new(
                    StatusCode::BAD_REQUEST,
                    "the freshness promise could not be encoded",
                )
            })?;
            let mut set = serde_json::Map::new();
            set.insert(FRESHNESS_PROPERTY.to_string(), Value::String(text));
            vec![json!({ "action": "set-properties", "updates": set })]
        }
    };
    let body = json!({ "requirements": [], "updates": updates });
    catalog.commit(ns, tbl, &body).await.map_err(catalog_fail)?;

    audit::catalog_doc_event(who, &format!("{ns}.{tbl}"), &["freshness"]);
    let loaded = catalog.load(ns, tbl).await.map_err(catalog_fail)?;
    Ok(health_of(&loaded, who))
}

/// The body of a freshness write, validated before the lock.
fn validate_freshness(req: FreshnessUpdate) -> Result<FreshnessWrite, DocError> {
    let bad = |message: &str| DocError::new(StatusCode::BAD_REQUEST, message);
    let Some(by) = req.by else {
        return Err(bad(
            "by is required: name a deadline like 07:00, or null to remove the promise",
        ));
    };
    let Some(raw) = by else {
        if req.timezone.is_some() || req.grace_minutes.is_some() {
            return Err(bad("removing a promise takes only by: null"));
        }
        return Ok(FreshnessWrite::Remove);
    };
    let by = checked_by(&raw).map_err(|e| bad(&e))?;
    let Some(timezone) = req.timezone else {
        return Err(bad("a timezone is required with a deadline"));
    };
    let timezone = checked_timezone(&timezone).map_err(|e| bad(&e))?;
    let grace_minutes = req.grace_minutes.unwrap_or(FRESHNESS_DEFAULT_GRACE_MINUTES);
    if grace_minutes > FRESHNESS_MAX_GRACE_MINUTES {
        return Err(bad("graceMinutes must be 0 to 1440"));
    }
    Ok(FreshnessWrite::Set(Freshness {
        by,
        timezone,
        grace_minutes,
        set_at: rfc3339(now()),
    }))
}

/// `PUT /api/catalog/tables/{ns}/{tbl}/columns/{column}/description`.
///
/// The column must be a field of the **current** schema, matched exactly; the name from the
/// metadata (never the path text) goes into the statement, through `quote_ident`. The
/// statement runs as the caller with the `nova-catalog` source, so the engine records the
/// person. The owner claim of [`claim_allowed`] needs an owner field the body does not have,
/// so a column write only asks [`may_edit`].
// Eight inputs is the seam the tests need: the table, the rule's identity, the statement
// and the injected runner. Grouping them would only move the arguments elsewhere.
#[allow(clippy::too_many_arguments)]
async fn write_column_description<F, Fut>(
    state: &AppState,
    catalog: &Catalog<'_>,
    who: &Identity,
    ns: &str,
    tbl: &str,
    column: &str,
    req: ColumnDescriptionUpdate,
    run_sql: F,
) -> Result<TableDocumentation, DocError>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let requested = match req.description {
        None => None,
        Some(raw) => {
            Some(checked_description(raw).map_err(|e| DocError::new(StatusCode::BAD_REQUEST, e))?)
        }
    };
    let lock = table_lock(state, catalog.name, ns, tbl);
    let _guard = lock.lock().await;

    let loaded = catalog.load(ns, tbl).await.map_err(catalog_fail)?;
    let catalog_owner = catalog_owner_from_kube(state, catalog.name).await;
    let can_rollback_v = may_rollback(who, catalog_owner.as_deref());
    let rollback_rule_v = rollback_rule(who, catalog_owner.as_deref());
    let current = documentation(&loaded, who, can_rollback_v, rollback_rule_v);
    may_edit(who, current.owner.as_deref()).map_err(|e| DocError::new(StatusCode::FORBIDDEN, e))?;

    let Some(field) = loaded.columns.iter().find(|c| c.name == column) else {
        return Err(DocError::new(StatusCode::NOT_FOUND, "no such column"));
    };

    let loaded = match requested {
        None => loaded,
        Some(description) => {
            let sql =
                comment_on_column_sql(catalog.name, ns, tbl, &field.name, description.as_deref());
            run_sql(sql)
                .await
                .map_err(|e| DocError::new(crate::trino::error_status(&e), e))?;
            let table = format!("{ns}.{tbl}");
            let changed = format!("column:{}", field.name);
            audit::catalog_doc_event(who, &table, &[changed.as_str()]);
            catalog.load(ns, tbl).await.map_err(catalog_fail)?
        }
    };
    let catalog_owner = catalog_owner_from_kube(state, catalog.name).await;
    let can_rollback_v = may_rollback(who, catalog_owner.as_deref());
    let rollback_rule_v = rollback_rule(who, catalog_owner.as_deref());
    let doc = documentation(&loaded, who, can_rollback_v, rollback_rule_v);
    crate::catalog_doc_store::record(state.db.as_ref(), catalog.name, ns, tbl, &doc).await;
    Ok(doc)
}

/// The only escaping a Trino string literal needs at these sizes: `'` doubles, and nothing
/// else changes (backslash, `"`, newlines, `--`, `;` and emoji survive as written).
fn sql_literal(raw: &str) -> String {
    raw.replace('\'', "''")
}

/// `COMMENT ON COLUMN <catalog>.<ns>.<tbl>.<column> IS '<text>'` (or `IS NULL`). Every
/// identifier goes through `quote_ident`, so a field named `a"b` or `x; DROP TABLE y` is one
/// quoted identifier, never SQL.
fn comment_on_column_sql(
    catalog: &str,
    ns: &str,
    tbl: &str,
    column: &str,
    description: Option<&str>,
) -> String {
    let value = match description {
        Some(text) => format!("'{}'", sql_literal(text)),
        None => "NULL".to_string(),
    };
    format!(
        "COMMENT ON COLUMN {}.{}.{}.{} IS {value}",
        quote_ident(catalog),
        quote_ident(ns),
        quote_ident(tbl),
        quote_ident(column),
    )
}

/// The caller's identity: the principal in `oidc` mode, the static user as an administrator
/// in `dev` mode (the same convention as `apps.rs` and the break-glass log).
pub fn caller_identity(state: &AppState, caller: Option<&Principal>) -> Identity {
    crate::auth::caller_identity(&state.dev_user, caller)
}

/// Build the catalog client for one request from a resolved warehouse id.
///
/// The catalog is called with Nova's own service token (`catalog_token.rs`), never the
/// caller's: the catalog sees one identity, Nova's. Which catalog the caller may use is
/// decided before this, by `catalog::catalog_scope` (asked of the engine with the caller's
/// own token).
async fn catalog_or_error(
    state: &AppState,
    id: &str,
) -> Result<(String, Option<String>), Box<Response>> {
    let authorization = match catalog::lakekeeper_authorization(state).await {
        Ok(authorization) => authorization,
        Err(e) => {
            return Err(Box::new(
                (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({ "error": e.to_string() })),
                )
                    .into_response(),
            ));
        }
    };
    Ok((
        format!("{}/catalog/v1/{id}", state.lakekeeper_url),
        authorization,
    ))
}

#[utoipa::path(
    get,
    operation_id = "table_documentation_get",
    path = "/api/catalog/tables/{ns}/{tbl}/documentation",
    tag = "public",
    description = "A table's documentation: the Iceberg `comment` as `description`, the `stardelt.tags`/`stardelt.owner`/`stardelt.domain`/`stardelt.guide` custom properties, and the description and type of every column of the **current** schema. `stardelt.guide` is a guide in plain text or Markdown, at most 16000 characters: how to use the table. `?catalog=` picks the catalog. `canEdit`/`editRule` apply Nova's interim rule: until the catalog enforces permissions, Nova decides who may edit, and anyone who reaches Lakekeeper or Trino directly can still change these properties.",
    params(
        ("ns" = String, Path, description = "Namespace (schema) name"),
        ("tbl" = String, Path, description = "Table name"),
        crate::catalog::CatalogParam,
    ),
    responses(
        (status = 200, description = "The table's documentation", body = TableDocumentation),
        (status = 400, description = "A name is not a single path segment, or the catalog name is invalid", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog or no such table", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn read(
    State(s): State<Arc<AppState>>,
    Path((ns, tbl)): Path<(String, String)>,
    Query(q): Query<crate::catalog::CatalogParam>,
    caller: Option<axum::Extension<Principal>>,
) -> Response {
    let caller = caller.as_ref().map(|c| &c.0);
    match documentation_for(&s, caller, q.catalog.as_deref(), &ns, &tbl).await {
        Ok((_catalog, doc)) => Json(doc).into_response(),
        Err(response) => response,
    }
}

/// The documentation of one table, resolved for `caller`: the catalog **name** it was read
/// from and the documentation itself.
///
/// Shared with the routes that need the same visibility and the same owner rule without
/// repeating them (`verified_queries.rs`): a table the caller cannot see answers exactly
/// what `read` answers.
#[allow(clippy::result_large_err)]
pub(crate) async fn documentation_for(
    s: &Arc<AppState>,
    caller: Option<&Principal>,
    catalog: Option<&str>,
    ns: &str,
    tbl: &str,
) -> Result<(String, TableDocumentation), Response> {
    let who = caller_identity(s, caller);
    let (name, id) = crate::catalog::catalog_scope(s, caller, catalog).await?;
    let (base, authorization) = catalog_or_error(s, &id).await.map_err(|e| *e)?;
    let client = Catalog {
        http: &s.http,
        base,
        id: &id,
        authorization: authorization.as_deref(),
        name: &name,
    };
    match read_documentation(s, &client, &who, ns, tbl).await {
        Ok(doc) => Ok((name, doc)),
        Err(e) => Err(e.into_response()),
    }
}

#[utoipa::path(
    put,
    operation_id = "table_documentation_update",
    path = "/api/catalog/tables/{ns}/{tbl}/documentation",
    tag = "public",
    description = "Set or remove a table's description, tags, owner, domain and guide in one Lakekeeper commit. `?catalog=` picks the catalog. Every key is optional: absent changes nothing, `null` (or an empty tag list, or a blank description or guide) removes the property, and a write with nothing to change sends no commit. Description at most 4,000 characters; guide at most 16,000 characters and keeps its line breaks; at most 20 unique tags `^[a-z0-9][a-z0-9_-]{0,31}$`; owner a group name; domain `^[a-z0-9][a-z0-9_-]{0,39}$`. An ownerless table may be edited by anybody, but the request must then set `owner` to one of the caller's own groups. **The interim rule is enforced by Nova; the catalog does not enforce it yet** — anyone who reaches Lakekeeper or Trino directly can still change these properties. Edits of one table are serialised by Nova and a contended commit is retried; a catalog that stays busy answers 502 `the catalog is busy, try again`.",
    params(
        ("ns" = String, Path, description = "Namespace (schema) name"),
        ("tbl" = String, Path, description = "Table name"),
        crate::catalog::CatalogParam,
    ),
    request_body = DocumentationUpdate,
    responses(
        (status = 200, description = "The documentation after the change, freshly loaded", body = TableDocumentation),
        (status = 400, description = "The body is invalid, the catalog name is invalid, or the table has no owner and this request does not claim it", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog, or only the owner group and administrators may edit", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog or no such table", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable, did not answer, or stayed busy", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn update(
    State(s): State<Arc<AppState>>,
    Path((ns, tbl)): Path<(String, String)>,
    Query(q): Query<crate::catalog::CatalogParam>,
    caller: Option<axum::Extension<Principal>>,
    ApiJson(req): ApiJson<DocumentationUpdate>,
) -> Response {
    let caller = caller.as_ref().map(|c| &c.0);
    let who = caller_identity(&s, caller);
    let (name, id) = match crate::catalog::catalog_scope(&s, caller, q.catalog.as_deref()).await {
        Ok(scope) => scope,
        Err(response) => return response,
    };
    let (base, authorization) = match catalog_or_error(&s, &id).await {
        Ok(v) => v,
        Err(response) => return *response,
    };
    let catalog = Catalog {
        http: &s.http,
        base,
        id: &id,
        authorization: authorization.as_deref(),
        name: &name,
    };
    match write_documentation(&s, &catalog, &who, &ns, &tbl, req).await {
        Ok(doc) => Json(doc).into_response(),
        Err(e) => e.into_response(),
    }
}

#[utoipa::path(
    get,
    operation_id = "table_freshness_get",
    path = "/api/catalog/tables/{ns}/{tbl}/health",
    tag = "public",
    description = "Whether this table's data is as fresh as its owner promised. The promise is the Iceberg table property `stardelt.freshness`, a JSON object `{\"by\":\"07:00\",\"timezone\":\"Europe/Berlin\",\"graceMinutes\":60,\"setAt\":\"2026-11-25T06:00:00Z\"}`: a wall-clock deadline in an IANA zone, a grace period of 0 to 1440 minutes (default 60) and the moment the promise was set. `?catalog=` picks the catalog. `status` is `grey` when the table has no promise (or one that does not parse — never an error), `green` when the newest commit (else `setAt`) is at or after the most recent deadline, `amber` while `now` is still inside the grace after that deadline, and `red` once the grace has passed. `lastWrite` is the newest commit's time and `dueAt`/`nextDueAt` the most recent and the next deadline. A wall clock the zone skips at a DST jump is not a deadline. The interim rule is enforced by Nova: `canEdit` says whether this caller may set the promise.",
    params(
        ("ns" = String, Path, description = "Namespace (schema) name"),
        ("tbl" = String, Path, description = "Table name"),
        crate::catalog::CatalogParam,
    ),
    responses(
        (status = 200, description = "The table's freshness at the moment of the read", body = TableFreshness),
        (status = 400, description = "A name is not a single path segment, or the catalog name is invalid", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog or no such table", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn health(
    State(s): State<Arc<AppState>>,
    Path((ns, tbl)): Path<(String, String)>,
    Query(q): Query<crate::catalog::CatalogParam>,
    caller: Option<axum::Extension<Principal>>,
) -> Response {
    let caller = caller.as_ref().map(|c| &c.0);
    let who = caller_identity(&s, caller);
    let (name, id) = match crate::catalog::catalog_scope(&s, caller, q.catalog.as_deref()).await {
        Ok(scope) => scope,
        Err(response) => return response,
    };
    let (base, authorization) = match catalog_or_error(&s, &id).await {
        Ok(v) => v,
        Err(response) => return *response,
    };
    let catalog = Catalog {
        http: &s.http,
        base,
        id: &id,
        authorization: authorization.as_deref(),
        name: &name,
    };
    match freshness_for(&s, &catalog, &who, &ns, &tbl).await {
        Ok(view) => Json(view).into_response(),
        Err(e) => e.into_response(),
    }
}

#[utoipa::path(
    get,
    operation_id = "catalog_health_list",
    path = "/api/catalog/health",
    tag = "public",
    description = "Every table of one catalog that carries a freshness promise, the most broken first: `red`, then `amber`, then `green`, and inside one status by `namespace.name`. The list is the answer `table_health` gives for one table, applied to every table the catalog lists; the promise is the Iceberg property `stardelt.freshness`. `?catalog=` picks the catalog. Views are not listed (they cannot carry the property), and a table that vanished between the list and the read is skipped. A table without a promise is not listed. `checked` is how many tables were read; the list stops after 200 tables, so a larger catalog answers a prefix. The interim rule is enforced by Nova: `canEdit` says whether this caller may set a table's promise.",
    params(crate::catalog::CatalogParam),
    responses(
        (status = 200, description = "The promised tables, worst first", body = CatalogHealth),
        (status = 400, description = "The catalog name is invalid", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable or did not answer", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn list_health(
    State(s): State<Arc<AppState>>,
    Query(q): Query<crate::catalog::CatalogParam>,
    caller: Option<axum::Extension<Principal>>,
) -> Response {
    let caller = caller.as_ref().map(|c| &c.0);
    let who = caller_identity(&s, caller);
    let (name, id) = match crate::catalog::catalog_scope(&s, caller, q.catalog.as_deref()).await {
        Ok(scope) => scope,
        Err(response) => return response,
    };
    let (base, authorization) = match catalog_or_error(&s, &id).await {
        Ok(v) => v,
        Err(response) => return *response,
    };
    let catalog = Catalog {
        http: &s.http,
        base,
        id: &id,
        authorization: authorization.as_deref(),
        name: &name,
    };
    match health_list(&s, &who, &catalog).await {
        Ok(answer) => Json(answer).into_response(),
        Err(e) => e.into_response(),
    }
}

#[utoipa::path(
    put,
    operation_id = "table_freshness_update",
    path = "/api/catalog/tables/{ns}/{tbl}/health",
    tag = "public",
    description = "Set or remove a table's freshness promise, stored as the Iceberg property `stardelt.freshness` (`{\"by\":\"07:00\",\"timezone\":\"Europe/Berlin\",\"graceMinutes\":60,\"setAt\":\"...\"}`). `?catalog=` picks the catalog. `by` is required: a 24-hour clock time `HH:MM` names a deadline, `null` removes the promise. A deadline needs an IANA `timezone`; `graceMinutes` is 0 to 1440 and defaults to 60. Removing a promise takes only `by: null`. A write with nothing to change sends no commit and answers the current status. The promise travels with the table, so any engine can read it; Nova evaluates it when the status is read. The interim rule is enforced by Nova: the catalog does not enforce it yet, and anyone who reaches Lakekeeper or Trino directly can still change the property.",
    params(
        ("ns" = String, Path, description = "Namespace (schema) name"),
        ("tbl" = String, Path, description = "Table name"),
        crate::catalog::CatalogParam,
    ),
    request_body = FreshnessUpdate,
    responses(
        (status = 200, description = "The table's freshness after the change, freshly loaded", body = TableFreshness),
        (status = 400, description = "The body is invalid, the catalog name is invalid, or the table has no owner and this request does not claim it", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog, or only the owner group and administrators may edit", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog or no such table", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog is not reachable, did not answer, or stayed busy", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn update_health(
    State(s): State<Arc<AppState>>,
    Path((ns, tbl)): Path<(String, String)>,
    Query(q): Query<crate::catalog::CatalogParam>,
    caller: Option<axum::Extension<Principal>>,
    ApiJson(req): ApiJson<FreshnessUpdate>,
) -> Response {
    let caller = caller.as_ref().map(|c| &c.0);
    let who = caller_identity(&s, caller);
    let (name, id) = match crate::catalog::catalog_scope(&s, caller, q.catalog.as_deref()).await {
        Ok(scope) => scope,
        Err(response) => return response,
    };
    let (base, authorization) = match catalog_or_error(&s, &id).await {
        Ok(v) => v,
        Err(response) => return *response,
    };
    let catalog = Catalog {
        http: &s.http,
        base,
        id: &id,
        authorization: authorization.as_deref(),
        name: &name,
    };
    match write_freshness(&s, &catalog, &who, &ns, &tbl, req).await {
        Ok(view) => Json(view).into_response(),
        Err(e) => e.into_response(),
    }
}

#[utoipa::path(
    put,
    operation_id = "column_description_update",
    path = "/api/catalog/tables/{ns}/{tbl}/columns/{column}/description",
    tag = "public",
    description = "Set or remove one column's description. The column must be a field of the current schema, matched exactly. Nova runs `COMMENT ON COLUMN <catalog>.<ns>.<tbl>.<column>` through Trino **as the caller** (source `nova-catalog`), because a column description lives in a new Iceberg schema and only the engine writes it. `?catalog=` picks the catalog. Same 4,000-character limit and null rule as the table description. The interim rule is enforced by Nova; the catalog does not enforce it yet.",
    params(
        ("ns" = String, Path, description = "Namespace (schema) name"),
        ("tbl" = String, Path, description = "Table name"),
        ("column" = String, Path, description = "Column name, exactly as the current schema spells it"),
        crate::catalog::CatalogParam,
    ),
    request_body = ColumnDescriptionUpdate,
    responses(
        (status = 200, description = "The table's documentation after the change, freshly loaded", body = TableDocumentation),
        (status = 400, description = "The body is invalid, or the catalog name is invalid", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not browse that catalog, or only the owner group and administrators may edit", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such catalog, no such table or no such column in the current schema", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The catalog or the engine is not reachable", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn update_column(
    State(s): State<Arc<AppState>>,
    Path((ns, tbl, column)): Path<(String, String, String)>,
    Query(q): Query<crate::catalog::CatalogParam>,
    caller: Option<axum::Extension<Principal>>,
    ApiJson(req): ApiJson<ColumnDescriptionUpdate>,
) -> Response {
    let caller = caller.as_ref().map(|c| &c.0);
    let who = caller_identity(&s, caller);
    let (name, id) = match crate::catalog::catalog_scope(&s, caller, q.catalog.as_deref()).await {
        Ok(scope) => scope,
        Err(response) => return response,
    };
    let (base, authorization) = match catalog_or_error(&s, &id).await {
        Ok(v) => v,
        Err(response) => return *response,
    };
    let catalog = Catalog {
        http: &s.http,
        base,
        id: &id,
        authorization: authorization.as_deref(),
        name: &name,
    };
    let trino_state = Arc::clone(&s);
    let result = write_column_description(
        &s,
        &catalog,
        &who,
        &ns,
        &tbl,
        &column,
        req,
        |sql| async move {
            crate::trino::execute_from(&trino_state, sql, caller, "nova-catalog")
                .await
                .map(|_| ())
        },
    )
    .await;
    match result {
        Ok(doc) => Json(doc).into_response(),
        Err(e) => e.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::{
        Router,
        extract::{Path, State},
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::get,
    };
    use serde_json::{Value, json};

    use super::*;
    use crate::auth::Identity;

    // -----------------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------------

    fn who(sub: &str, groups: &[&str], admin: bool) -> Identity {
        Identity {
            sub: sub.into(),
            display_name: sub.into(),
            email: None,
            name: None,
            groups: groups.iter().map(|g| g.to_string()).collect(),
            roles: Vec::new(),
            groups_overage: false,
            is_admin: admin,
        }
    }

    fn admin() -> Identity {
        who("admin", &[], true)
    }

    /// A load-table answer with two schemas. Schema 1 carries the `doc`s; the properties are
    /// the ones the read tests expect.
    fn load_fixture() -> Value {
        json!({
            "metadata-location": "s3://warehouse/sales/orders/metadata/00001.metadata.json",
            "metadata": {
                "format-version": 2,
                "current-schema-id": 1,
                "schemas": [
                    {
                        "type": "struct",
                        "schema-id": 0,
                        "fields": [
                            { "id": 1, "name": "id", "required": true, "type": "long" },
                            { "id": 2, "name": "amount", "required": false, "type": "double" },
                            { "id": 5, "name": "legacy", "required": false, "type": "string" }
                        ]
                    },
                    {
                        "type": "struct",
                        "schema-id": 1,
                        "fields": [
                            { "id": 1, "name": "id", "required": true, "type": "long",
                              "doc": "the identifier" },
                            { "id": 2, "name": "amount", "required": false, "type": "double",
                              "doc": "the amount" },
                            { "id": 3, "name": "a\"b", "required": false, "type": "string",
                              "doc": "quoted name" },
                            { "id": 4, "name": "x; DROP TABLE y", "required": false,
                              "type": { "type": "list", "element": "string" },
                              "doc": "not SQL" }
                        ]
                    }
                ],
                "properties": {
                    "comment": "orders, one row per order",
                    "stardelt.tags": "finance, pii , ",
                    "stardelt.owner": "data-eng",
                    "stardelt.domain": "sales"
                }
            }
        })
    }

    #[derive(Default)]
    struct FakeState {
        load: Value,
        load_status: Option<u16>,
        /// Statuses the next commits answer, in order; an empty list means 200.
        commit_statuses: Vec<u16>,
        commits: Vec<Value>,
        requests: usize,
        in_flight: usize,
        max_in_flight: usize,
        commit_delay: Option<Duration>,
        /// The namespaces the list endpoint answers, and each one's tables.
        namespaces: Vec<String>,
        tables: std::collections::HashMap<String, Vec<String>>,
        /// Load answers per `ns/tbl`; a table not named here answers `load`.
        loads: std::collections::HashMap<String, Value>,
    }

    #[derive(Clone, Default)]
    struct Fake {
        inner: Arc<Mutex<FakeState>>,
    }

    impl Fake {
        fn set_load(&self, load: Value) {
            self.inner.lock().unwrap().load = load;
        }

        /// One namespace with these tables; each table answers the same load unless
        /// [`Fake::set_table_load`] gives it its own.
        fn set_catalog(&self, namespace: &str, tables: &[&str]) {
            let mut s = self.inner.lock().unwrap();
            s.namespaces = vec![namespace.to_string()];
            s.tables.insert(
                namespace.to_string(),
                tables.iter().map(|t| t.to_string()).collect(),
            );
        }

        fn set_table_load(&self, ns: &str, tbl: &str, load: Value) {
            self.inner
                .lock()
                .unwrap()
                .loads
                .insert(format!("{ns}/{tbl}"), load);
        }

        fn fail_load(&self, status: u16) {
            self.inner.lock().unwrap().load_status = Some(status);
        }

        fn fail_commits(&self, statuses: &[u16]) {
            self.inner.lock().unwrap().commit_statuses = statuses.to_vec();
        }

        fn delay_commits(&self, delay: Duration) {
            self.inner.lock().unwrap().commit_delay = Some(delay);
        }

        fn snapshot(&self) -> (Vec<Value>, usize, usize) {
            let s = self.inner.lock().unwrap();
            (s.commits.clone(), s.requests, s.max_in_flight)
        }

        fn reset_stats(&self) {
            let mut s = self.inner.lock().unwrap();
            s.commits.clear();
            s.requests = 0;
            s.in_flight = 0;
            s.max_in_flight = 0;
        }
    }

    async fn load_handler(
        State(fake): State<Fake>,
        Path((_wh, ns, tbl)): Path<(String, String, String)>,
    ) -> Response {
        let (status, body) = {
            let mut s = fake.inner.lock().unwrap();
            s.requests += 1;
            let body = s
                .loads
                .get(&format!("{ns}/{tbl}"))
                .cloned()
                .unwrap_or_else(|| s.load.clone());
            (s.load_status.unwrap_or(200), body)
        };
        match status {
            200 => Json(body).into_response(),
            404 => (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": { "message": "no such table" } })),
            )
                .into_response(),
            other => (
                StatusCode::from_u16(other).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                Json(json!({ "error": { "message": "nope" } })),
            )
                .into_response(),
        }
    }

    async fn commit_handler(
        State(fake): State<Fake>,
        Path((_wh, _ns, _tbl)): Path<(String, String, String)>,
        Json(body): Json<Value>,
    ) -> Response {
        let (status, delay) = {
            let mut s = fake.inner.lock().unwrap();
            s.requests += 1;
            s.commits.push(body);
            s.in_flight += 1;
            s.max_in_flight = s.max_in_flight.max(s.in_flight);
            let status = if s.commit_statuses.is_empty() {
                200
            } else {
                s.commit_statuses.remove(0)
            };
            (status, s.commit_delay)
        };
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        {
            let mut s = fake.inner.lock().unwrap();
            s.in_flight = s.in_flight.saturating_sub(1);
        }
        match status {
            200 => Json(json!({})).into_response(),
            404 => (StatusCode::NOT_FOUND, Json(json!({ "error": {} }))).into_response(),
            other => (
                StatusCode::from_u16(other).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                Json(json!({
                    "error": {
                        "message": "Error starting transaction",
                        "id": "err-42"
                    }
                })),
            )
                .into_response(),
        }
    }

    /// A pretend Lakekeeper: namespaces, their tables, one table's GET and POST.
    async fn fake_catalog() -> (Fake, String) {
        let fake = Fake::default();
        fake.set_load(load_fixture());
        let app = Router::new()
            .route("/catalog/v1/{wh}/namespaces", get(namespaces_handler))
            .route(
                "/catalog/v1/{wh}/namespaces/{ns}/tables",
                get(tables_handler),
            )
            .route(
                "/catalog/v1/{wh}/namespaces/{ns}/tables/{tbl}",
                get(load_handler).post(commit_handler),
            )
            .with_state(fake.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (fake, format!("http://{addr}"))
    }

    async fn namespaces_handler(State(fake): State<Fake>) -> Response {
        let namespaces: Vec<Value> = fake
            .inner
            .lock()
            .unwrap()
            .namespaces
            .iter()
            .map(|ns| json!([ns]))
            .collect();
        Json(json!({ "namespaces": namespaces })).into_response()
    }

    async fn tables_handler(
        State(fake): State<Fake>,
        Path((_wh, ns)): Path<(String, String)>,
    ) -> Response {
        let names = fake
            .inner
            .lock()
            .unwrap()
            .tables
            .get(&ns)
            .cloned()
            .unwrap_or_default();
        let identifiers: Vec<Value> = names
            .iter()
            .map(|name| json!({ "namespace": [ns], "name": name }))
            .collect();
        Json(json!({ "identifiers": identifiers })).into_response()
    }

    fn test_state() -> Arc<AppState> {
        Arc::new(AppState {
            dev_user: "test".into(),
            auth_dev: false,
            default_theme: "graphite",
            trino_url: "http://127.0.0.1:1".into(),
            lakekeeper_url: "http://127.0.0.1:1".into(),
            shared_accounts: vec![],
            // No catalog client in these tests: Lakekeeper is called as before (#222).
            catalog_token: None,
            exchange: None,
            superset_url: "http://127.0.0.1:1".into(),
            superset_api_url: "http://127.0.0.1:1".into(),
            superset: None,
            jupyterhub_url: "http://127.0.0.1:1".into(),
            warehouse_name: "warehouse".into(),
            stage_name: None,
            stage_kind: "dev",
            stages: Vec::new(),
            catalog_ids: std::sync::RwLock::new(std::collections::HashMap::new()),
            catalog_access: std::sync::Mutex::new(std::collections::HashMap::new()),
            catalog_metrics: std::sync::Mutex::new(std::collections::HashMap::new()),
            metrics_in_flight: std::sync::Mutex::new(std::collections::HashSet::new()),
            metrics_round: tokio::sync::Mutex::new(()),
            metrics_refresh: std::sync::Mutex::new(None),
            catalog_layers: std::sync::Mutex::new(None),
            principal_owners: std::sync::Mutex::new(None),
            http: reqwest::Client::new(),
            forward_identity: false,
            redact_query_log_literals: false,
            showback: None,
            mcp: None,
            auth: None,
            jobs: Mutex::new(Vec::new()),
            kube: None,
            workloads: crate::cluster::WorkloadCache::default(),
            admin_console_url: None,
            db: None,
            store: None,
            break_glass_reads: Mutex::new(std::collections::HashMap::new()),
            run_starts: std::sync::Mutex::new(std::collections::HashMap::new()),
            pipeline_creates: Mutex::new(std::collections::HashMap::new()),
            catalog_access_changes: Mutex::new(std::collections::HashMap::new()),
            team_creates: Mutex::new(std::collections::HashMap::new()),
            airflow: None,
            table_locks: Mutex::new(std::collections::HashMap::new()),
            people_touched: Mutex::new(std::collections::HashMap::new()),
            notebooks: None,
            workspace: None,
            statements: Default::default(),
        })
    }

    fn catalog<'a>(state: &'a AppState, addr: &str) -> Catalog<'a> {
        Catalog {
            http: &state.http,
            base: format!("{addr}/catalog/v1/wh"),
            id: "wh",
            authorization: None,
            name: "warehouse",
        }
    }

    fn update_body(
        description: Option<Option<&str>>,
        tags: Option<Option<&[&str]>>,
        owner: Option<Option<&str>>,
        domain: Option<Option<&str>>,
    ) -> DocumentationUpdate {
        DocumentationUpdate {
            description: description.map(|d| d.map(str::to_string)),
            tags: tags.map(|t| t.map(|list| list.iter().map(|s| s.to_string()).collect())),
            owner: owner.map(|o| o.map(str::to_string)),
            domain: domain.map(|d| d.map(str::to_string)),
            guide: None,
        }
    }

    /// A body whose only key is `guide`.
    fn guide_body(guide: Option<Option<&str>>) -> DocumentationUpdate {
        DocumentationUpdate {
            description: None,
            tags: None,
            owner: None,
            domain: None,
            guide: guide.map(|g| g.map(str::to_string)),
        }
    }

    /// The load fixture with a guide stored in `stardelt.guide`.
    fn load_fixture_with_guide(guide: &str) -> Value {
        let mut load = load_fixture();
        load["metadata"]["properties"]
            .as_object_mut()
            .expect("properties")
            .insert("stardelt.guide".into(), json!(guide));
        load
    }

    fn status_of(e: DocError) -> (StatusCode, String) {
        (e.status(), e.message().to_string())
    }

    // -----------------------------------------------------------------------
    // 1. Read: the current schema, not schemas[0]
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn read_picks_the_current_schema() {
        let (_fake, addr) = fake_catalog().await;
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        let doc = read_documentation(&state, &catalog, &member, "sales", "orders")
            .await
            .expect("the table loads");
        assert_eq!(doc.columns.len(), 4, "schema 1 is the current one");
        assert_eq!(
            doc.columns[0].description.as_deref(),
            Some("the identifier")
        );
        assert_eq!(doc.columns[1].description.as_deref(), Some("the amount"));
        assert_eq!(
            doc.columns[3].r#type, r#"{"element":"string","type":"list"}"#,
            "a complex type is its JSON text"
        );

        let mut older = load_fixture();
        older["metadata"]["current-schema-id"] = json!(0);
        _fake.set_load(older);
        let doc = read_documentation(&state, &catalog, &member, "sales", "orders")
            .await
            .expect("the older schema also loads");
        assert_eq!(doc.columns.len(), 3, "schema 0 is current now");
        assert!(
            doc.columns.iter().all(|c| c.description.is_none()),
            "schema 0 has no doc fields"
        );
    }

    // -----------------------------------------------------------------------
    // 2. Read: properties and the rule sentence
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn read_splits_tags_and_reports_owner_domain_description() {
        let (_fake, addr) = fake_catalog().await;
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        let doc = read_documentation(&state, &catalog, &member, "sales", "orders")
            .await
            .expect("the table loads");
        assert_eq!(
            doc.description.as_deref(),
            Some("orders, one row per order")
        );
        assert_eq!(doc.tags, ["finance", "pii"]);
        assert_eq!(doc.owner.as_deref(), Some("data-eng"));
        assert_eq!(doc.domain.as_deref(), Some("sales"));
        assert!(doc.can_edit);
        assert_eq!(
            doc.edit_rule,
            "You can edit because you belong to the owner group data-eng."
        );
        let serialized = serde_json::to_value(&doc).expect("the answer serializes");
        assert_eq!(serialized["canEdit"], json!(true));
        assert_eq!(serialized["editRule"], json!(doc.edit_rule));
        assert_eq!(serialized["columns"][0]["name"], json!("id"));
        assert!(serialized["columns"][0]["description"].is_string());
    }

    // -----------------------------------------------------------------------
    // 3. Write: one commit with set and remove; a no-op sends nothing
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn write_sends_one_commit_with_set_and_remove() {
        let (fake, addr) = fake_catalog().await;
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        let doc = write_documentation(
            &state,
            &catalog,
            &member,
            "sales",
            "orders",
            update_body(
                Some(Some("new description")),
                Some(Some(&["vip", "pii"])),
                None,
                Some(None),
            ),
        )
        .await
        .expect("the write succeeds");
        assert!(!doc.can_edit || doc.owner.as_deref() == Some("data-eng"));

        let (commits, _, _) = fake.snapshot();
        assert_eq!(commits.len(), 1, "one commit carries both actions");
        assert_eq!(commits[0]["requirements"], json!([]), "no requirements");
        assert_eq!(
            commits[0]["updates"],
            json!([
                {
                    "action": "set-properties",
                    "updates": {
                        "comment": "new description",
                        "stardelt.tags": "pii,vip"
                    }
                },
                {
                    "action": "remove-properties",
                    "removals": ["stardelt.domain"]
                }
            ]),
            "tags are sorted and joined; the domain is removed"
        );
    }

    #[tokio::test]
    async fn a_noop_write_sends_nothing() {
        let (fake, addr) = fake_catalog().await;
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        let doc = write_documentation(
            &state,
            &catalog,
            &member,
            "sales",
            "orders",
            update_body(
                Some(Some("orders, one row per order")),
                Some(Some(&["pii", "finance"])),
                Some(Some("data-eng")),
                Some(Some("sales")),
            ),
        )
        .await
        .expect("the no-op succeeds");
        assert_eq!(doc.tags, ["finance", "pii"]);

        let (commits, requests, _) = fake.snapshot();
        assert!(commits.is_empty(), "nothing changed, so nothing was sent");
        assert_eq!(requests, 1, "only the load that decided the rule ran");

        // An empty body is a no-op too.
        let _ = write_documentation(
            &state,
            &catalog,
            &member,
            "sales",
            "orders",
            update_body(None, None, None, None),
        )
        .await
        .expect("an empty body is a no-op");
        assert!(fake.snapshot().0.is_empty());
    }

    // -----------------------------------------------------------------------
    // 4. Validation happens before any request
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn validation_refuses_before_any_request() {
        let (fake, addr) = fake_catalog().await;
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        let long = "x".repeat(MAX_DESCRIPTION_CHARS + 1);
        let many: Vec<String> = (0..=MAX_TAGS).map(|i| format!("tag{i}")).collect();
        let many: Vec<&str> = many.iter().map(String::as_str).collect();
        let cases = [
            update_body(Some(Some(long.as_str())), None, None, None),
            update_body(None, Some(Some(&many)), None, None),
            update_body(None, Some(Some(&["Finance"])), None, None),
            update_body(None, Some(Some(&["pii", "pii"])), None, None),
            update_body(None, None, Some(Some("bad group!")), Some(Some("sales.eu"))),
            update_body(Some(Some("a\0b")), None, None, None),
        ];
        for req in cases {
            let (status, message) = status_of(
                write_documentation(&state, &catalog, &member, "sales", "orders", req)
                    .await
                    .expect_err("the body is invalid"),
            );
            assert_eq!(status, StatusCode::BAD_REQUEST, "{message}");
            assert!(!message.is_empty());
        }
        assert_eq!(
            fake.snapshot().1,
            0,
            "validation refused before the catalog saw a request"
        );
    }

    // -----------------------------------------------------------------------
    // 5. Retries and the busy answer
    // -----------------------------------------------------------------------

    #[tokio::test(start_paused = true)]
    async fn retries_500_and_503_then_succeeds() {
        let (fake, addr) = fake_catalog().await;
        fake.fail_commits(&[500, 503]);
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        let started = tokio::time::Instant::now();
        let doc = write_documentation(
            &state,
            &catalog,
            &member,
            "sales",
            "orders",
            update_body(Some(Some("new")), None, None, None),
        )
        .await
        .expect("the third attempt succeeds");
        let elapsed = started.elapsed();
        assert_eq!(
            doc.description.as_deref(),
            Some("orders, one row per order")
        );
        assert_eq!(fake.snapshot().0.len(), 3, "two retries after the first");
        assert!(
            elapsed >= Duration::from_millis(600) && elapsed < Duration::from_millis(1000),
            "200 ms + 400 ms of waits, got {elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_with_502_after_three_attempts() {
        let (fake, addr) = fake_catalog().await;
        fake.fail_commits(&[500, 503, 500, 503]);
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        let started = tokio::time::Instant::now();
        let error = write_documentation(
            &state,
            &catalog,
            &member,
            "sales",
            "orders",
            update_body(Some(Some("new")), None, None, None),
        )
        .await
        .expect_err("three attempts are enough");
        assert_eq!(error.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(error.message(), "the catalog is busy, try again");
        assert_eq!(fake.snapshot().0.len(), 3, "exactly three attempts");
        assert!(
            started.elapsed() >= Duration::from_millis(600),
            "the waits happened"
        );
    }

    #[tokio::test]
    async fn a_404_is_a_404() {
        let (fake, addr) = fake_catalog().await;
        fake.fail_load(404);
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        let error = read_documentation(&state, &catalog, &member, "sales", "orders")
            .await
            .expect_err("no such table");
        assert_eq!(error.status(), StatusCode::NOT_FOUND);
        assert_eq!(error.message(), "no such table");
    }

    // -----------------------------------------------------------------------
    // 6. Per-table serialisation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn edits_of_one_table_are_serialised() {
        let (fake, addr) = fake_catalog().await;
        fake.delay_commits(Duration::from_millis(100));
        let state = test_state();
        let catalog = catalog(&state, &addr);

        let a = who("ada", &["data-eng"], false);
        let b = who("bob", &["data-eng"], false);
        let first = update_body(Some(Some("first")), None, Some(Some("data-eng")), None);
        let second = update_body(Some(Some("second")), None, Some(Some("data-eng")), None);
        let (r1, r2) = tokio::join!(
            write_documentation(&state, &catalog, &a, "sales", "orders", first),
            write_documentation(&state, &catalog, &b, "sales", "orders", second),
        );
        r1.expect("the first write succeeds");
        r2.expect("the second write succeeds");
        let (commits, _, max) = fake.snapshot();
        assert_eq!(commits.len(), 2);
        assert_eq!(max, 1, "never two commits of one table in flight");

        // Two different tables are independent: their commits may overlap.
        fake.reset_stats();
        let (r1, r2) = tokio::join!(
            write_documentation(
                &state,
                &catalog,
                &a,
                "sales",
                "orders_a",
                update_body(Some(Some("a")), None, Some(Some("data-eng")), None)
            ),
            write_documentation(
                &state,
                &catalog,
                &b,
                "sales",
                "orders_b",
                update_body(Some(Some("b")), None, Some(Some("data-eng")), None)
            ),
        );
        r1.expect("the first table's write succeeds");
        r2.expect("the second table's write succeeds");
        let (commits, _, max) = fake.snapshot();
        assert_eq!(commits.len(), 2);
        assert_eq!(max, 2, "different tables may edit at the same time");
    }

    // -----------------------------------------------------------------------
    // 7. The SQL literal and the quoting of metadata names
    // -----------------------------------------------------------------------

    #[test]
    fn catalog_error_id_is_read_from_the_body() {
        assert_eq!(
            catalog_error_id(r#"{"error":{"message":"x","id":"abc-1"}}"#).as_deref(),
            Some("abc-1")
        );
        assert_eq!(
            catalog_error_id("deadlock detected (Error ID: 7f3a2)").as_deref(),
            Some("7f3a2")
        );
        assert_eq!(catalog_error_id("no id here"), None);
    }

    #[test]
    fn sql_literal_round_trips_the_nasty_text() {
        let text = "it's \"quoted\"\n-- not a comment🙂;";
        assert_eq!(
            sql_literal(text),
            "it''s \"quoted\"\n-- not a comment🙂;",
            "only the apostrophe doubles"
        );
        assert_eq!(
            comment_on_column_sql("finance", "a\"b", "x; DROP TABLE y", "amount", Some("it's")),
            "COMMENT ON COLUMN \"finance\".\"a\"\"b\".\"x; DROP TABLE y\".\"amount\" IS 'it''s'"
        );
        assert_eq!(
            comment_on_column_sql("warehouse", "sales", "orders", "amount", None),
            "COMMENT ON COLUMN \"warehouse\".\"sales\".\"orders\".\"amount\" IS NULL"
        );
    }

    #[tokio::test]
    async fn nasty_column_names_only_reach_sql_through_quote_ident() {
        let (_fake, addr) = fake_catalog().await;
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);
        let ran = Arc::new(Mutex::new(Vec::new()));

        for column in ["a\"b", "x; DROP TABLE y"] {
            let seen = Arc::clone(&ran);
            write_column_description(
                &state,
                &catalog,
                &member,
                "a\"b",
                "x; DROP TABLE y",
                column,
                ColumnDescriptionUpdate {
                    description: Some(Some("ok".into())),
                },
                move |sql| {
                    seen.lock().unwrap().push(sql);
                    async { Ok(()) }
                },
            )
            .await
            .expect("the column is in the current schema");
        }
        let ran = ran.lock().unwrap();
        assert_eq!(ran.len(), 2);
        assert!(
            ran[0].contains("\"a\"\"b\""),
            "the quote is doubled: {}",
            ran[0]
        );
        assert!(
            ran[1].contains("\"x; DROP TABLE y\""),
            "the semicolon stays inside one identifier: {}",
            ran[1]
        );
    }

    // -----------------------------------------------------------------------
    // 8. An unknown column is a 404 before any statement
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn unknown_column_is_404_and_runs_no_statement() {
        let (_fake, addr) = fake_catalog().await;
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);
        let ran = Arc::new(Mutex::new(Vec::new()));

        for column in ["nope", "legacy"] {
            // `legacy` exists in schema 0 but the current schema is 1; `nope` nowhere.
            let seen = Arc::clone(&ran);
            let error = write_column_description(
                &state,
                &catalog,
                &member,
                "sales",
                "orders",
                column,
                ColumnDescriptionUpdate {
                    description: Some(Some("x".into())),
                },
                move |sql| {
                    seen.lock().unwrap().push(sql);
                    async { Ok(()) }
                },
            )
            .await
            .expect_err("not a current-schema column");
            assert_eq!(error.status(), StatusCode::NOT_FOUND);
            assert_eq!(error.message(), "no such column");
        }
        assert!(
            ran.lock().unwrap().is_empty(),
            "no COMMENT ON COLUMN for a column that is not in the current schema"
        );
    }

    // -----------------------------------------------------------------------
    // 9. The interim rule
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn interim_rule_table() {
        let owner = Some("data-eng");
        assert!(may_edit(&admin(), owner).is_ok(), "an administrator");
        assert!(
            may_edit(&who("ada", &["data-eng"], false), owner).is_ok(),
            "a member of the owner group"
        );
        assert!(
            may_edit(&who("ada", &["/data-eng"], false), owner).is_ok(),
            "Keycloak's full group path matches"
        );
        assert_eq!(
            may_edit(&who("bob", &["analytics"], false), owner).expect_err("not the owner group"),
            "only members of the owner group data-eng and administrators may edit this"
        );
        assert!(
            may_edit(&who("bob", &[], false), None).is_ok(),
            "anybody may edit an ownerless table"
        );

        // The write side of the no-owner clause.
        let (fake, addr) = fake_catalog().await;
        let mut no_owner = load_fixture();
        no_owner["metadata"]["properties"]
            .as_object_mut()
            .unwrap()
            .remove("stardelt.owner");
        fake.set_load(no_owner);
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let bob = who("bob", &["analytics"], false);

        let error = write_documentation(
            &state,
            &catalog,
            &bob,
            "sales",
            "orders",
            update_body(None, None, Some(Some("data-eng")), None),
        )
        .await
        .expect_err("a non-member cannot claim the table for somebody else");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.message(), "choose an owner: one of your groups");

        let error = write_documentation(
            &state,
            &catalog,
            &bob,
            "sales",
            "orders",
            update_body(Some(Some("new")), None, None, None),
        )
        .await
        .expect_err("an ownerless table must be claimed");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.message(), "choose an owner: one of your groups");
        assert!(fake.snapshot().0.is_empty(), "nothing was committed");

        let doc = write_documentation(
            &state,
            &catalog,
            &bob,
            "sales",
            "orders",
            update_body(None, None, Some(Some("analytics")), None),
        )
        .await
        .expect("the first editor claims the table for their own group");
        assert!(
            doc.owner.is_none(),
            "the fake catalog does not mutate its fixture"
        );

        let (commits, _, _) = fake.snapshot();
        assert_eq!(
            commits[0]["updates"][0]["updates"]["stardelt.owner"],
            json!("analytics")
        );

        // An administrator may choose any valid group name.
        let doc = write_documentation(
            &state,
            &catalog,
            &admin(),
            "sales",
            "orders",
            update_body(None, None, Some(Some("any-group")), None),
        )
        .await;
        assert!(doc.is_ok(), "an administrator claims any group");
    }

    // -----------------------------------------------------------------------
    // 10. canEdit agrees with the rule; the audit line names, never quotes
    // -----------------------------------------------------------------------

    #[test]
    fn can_edit_matches_the_rule() {
        let loaded = {
            let body = load_fixture().to_string();
            parse_loaded(&body).expect("the fixture parses")
        };
        let identities = [
            admin(),
            who("ada", &["data-eng"], false),
            who("bob", &["analytics"], false),
            who("nobody", &[], false),
        ];
        for owner in [Some("data-eng"), None] {
            for identity in &identities {
                let mut loaded = Loaded {
                    properties: loaded.properties.clone(),
                    columns: Vec::new(),
                    last_write: None,
                };
                match owner {
                    Some(owner) => {
                        loaded
                            .properties
                            .insert("stardelt.owner".into(), json!(owner));
                    }
                    None => {
                        loaded.properties.remove("stardelt.owner");
                    }
                }
                let doc = documentation(&loaded, identity, false, "".to_string());
                assert_eq!(
                    doc.can_edit,
                    may_edit(identity, owner).is_ok(),
                    "canEdit must be the rule for {owner:?}"
                );
                assert!(!doc.edit_rule.is_empty());
            }
        }
    }

    #[derive(Default)]
    struct Captured(String);

    impl tracing::field::Visit for Captured {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            use std::fmt::Write;
            let _ = write!(self.0, "{}={value:?} ", field.name());
        }
    }

    struct Capture(Arc<Mutex<String>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut fields = Captured::default();
            event.record(&mut fields);
            self.0.lock().unwrap().push_str(&fields.0);
        }
    }

    #[test]
    fn audit_names_not_text() {
        const CANARY: &str = "top-secret-description-text";
        let written = Arc::new(Mutex::new(String::new()));
        let subscriber =
            tracing_subscriber::layer::SubscriberExt::with(tracing_subscriber::registry(), {
                Capture(Arc::clone(&written))
            });
        tracing::subscriber::with_default(subscriber, || {
            audit::catalog_doc_event(
                &who("ada", &["data-eng"], false),
                "sales.orders",
                &["description", "column:id"],
            );
        });
        let line = written.lock().unwrap().clone();
        assert!(
            line.contains("description") && line.contains("column:id"),
            "the audit line names what changed: {line}"
        );
        assert!(
            !line.contains(CANARY),
            "the audit line never carries the text: {line}"
        );
        assert!(line.contains("sales.orders"), "the table is named: {line}");
    }

    // -----------------------------------------------------------------------
    // 11. The guide: the `stardelt.guide` property
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn a_guide_longer_than_the_cap_is_refused_before_any_request() {
        let (fake, addr) = fake_catalog().await;
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);
        let long = "x".repeat(MAX_GUIDE_CHARS + 1);

        let error = write_documentation(
            &state,
            &catalog,
            &member,
            "sales",
            "orders",
            guide_body(Some(Some(long.as_str()))),
        )
        .await
        .expect_err("16001 characters are too many");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.message(), "guide must be at most 16000 characters");
        assert_eq!(
            fake.snapshot().1,
            0,
            "the refusal happened before the catalog saw a request"
        );

        // Exactly the cap is accepted.
        let at_cap = "y".repeat(MAX_GUIDE_CHARS);
        assert!(checked_guide(Some(at_cap)).is_ok());
    }

    #[tokio::test]
    async fn a_guide_keeps_its_line_breaks() {
        let (fake, addr) = fake_catalog().await;
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        write_documentation(
            &state,
            &catalog,
            &member,
            "sales",
            "orders",
            guide_body(Some(Some(
                "\nJoin on customer_id.\nDrop status = 'test'.\n",
            ))),
        )
        .await
        .expect("the guide is stored");
        let (commits, _, _) = fake.snapshot();
        assert_eq!(
            commits[0]["updates"][0]["updates"]["stardelt.guide"],
            json!("Join on customer_id.\nDrop status = 'test'."),
            "only the ends are trimmed; the line breaks stay"
        );
        assert_eq!(
            checked_guide(Some("a\nb\r\nc".into())),
            Ok(Some("a\nb\r\nc".into())),
            "a Markdown guide keeps every line break as written"
        );
    }

    #[tokio::test]
    async fn null_removes_the_guide() {
        let (fake, addr) = fake_catalog().await;
        fake.set_load(load_fixture_with_guide("Read with the customer join."));
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        write_documentation(
            &state,
            &catalog,
            &member,
            "sales",
            "orders",
            guide_body(Some(None)),
        )
        .await
        .expect("the guide is removed");
        let (commits, _, _) = fake.snapshot();
        assert_eq!(
            commits[0]["updates"],
            json!([{ "action": "remove-properties", "removals": ["stardelt.guide"] }]),
            "a null guide is a remove-properties of stardelt.guide"
        );
    }

    #[tokio::test]
    async fn documentation_reads_the_guide_from_the_properties() {
        let (fake, addr) = fake_catalog().await;
        fake.set_load(load_fixture_with_guide(
            "Join `customer_id` to analytics.customers.",
        ));
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        let doc = read_documentation(&state, &catalog, &member, "sales", "orders")
            .await
            .expect("the table loads");
        assert_eq!(
            doc.guide.as_deref(),
            Some("Join `customer_id` to analytics.customers.")
        );
        let serialized = serde_json::to_value(&doc).expect("the answer serializes");
        assert_eq!(
            serialized["guide"],
            json!("Join `customer_id` to analytics.customers."),
            "the read answer carries the guide"
        );

        // A table without the property answers `null`.
        fake.set_load(load_fixture());
        let doc = read_documentation(&state, &catalog, &member, "sales", "orders")
            .await
            .expect("the table loads");
        assert!(doc.guide.is_none());
    }

    // -----------------------------------------------------------------------
    // 12. The freshness promise (#402 slice 1): status, parse, DST, write
    // -----------------------------------------------------------------------

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .expect("a test time parses")
            .with_timezone(&Utc)
    }

    fn promise(by: &str, timezone: &str, grace_minutes: u32) -> Freshness {
        Freshness {
            by: by.into(),
            timezone: timezone.into(),
            grace_minutes,
            set_at: "2026-11-01T00:00:00Z".into(),
        }
    }

    /// A load-table answer carrying `promise` as the Iceberg property, and — when
    /// `last_write_ms` is given — a current snapshot at that time.
    fn load_with_promise(promise: &Freshness, last_write_ms: Option<i64>) -> Value {
        let mut body = load_fixture();
        body["metadata"]["properties"]
            .as_object_mut()
            .expect("properties")
            .insert(
                FRESHNESS_PROPERTY.into(),
                json!(serde_json::to_string(promise).expect("the promise serializes")),
            );
        if let Some(ms) = last_write_ms {
            body["metadata"]["current-snapshot-id"] = json!(7);
            body["metadata"]["snapshots"] = json!([{ "snapshot-id": 7, "timestamp-ms": ms }]);
        }
        body
    }

    #[test]
    fn freshness_is_green_before_the_deadline_amber_inside_the_grace_and_red_after() {
        let promise = promise("07:00", "Europe/Berlin", 60);
        let last_write = Some(at("2026-11-25T04:00:00Z"));
        // 07:00 Berlin is 06:00Z in November, so the grace ends at 07:00Z.
        assert_eq!(
            freshness_health(&promise, last_write, at("2026-11-25T05:00:00Z")).0,
            "green",
            "the newest write is after yesterday's deadline"
        );
        assert_eq!(
            freshness_health(&promise, last_write, at("2026-11-25T06:30:00Z")).0,
            "amber",
            "past today's deadline, inside the grace"
        );
        assert_eq!(
            freshness_health(&promise, last_write, at("2026-11-25T07:30:00Z")).0,
            "red",
            "the grace has passed"
        );
    }

    #[test]
    fn a_promise_is_grey_without_a_promise_and_when_it_does_not_parse() {
        assert_eq!(parse_freshness("{}"), None, "no deadline, no promise");
        assert_eq!(
            parse_freshness(
                r#"{"by":"25:00","timezone":"Europe/Berlin","graceMinutes":60,"setAt":"2026-11-01T00:00:00Z"}"#
            ),
            None,
            "25:00 is not a wall-clock time"
        );
        assert_eq!(
            parse_freshness(
                r#"{"by":"07:00","timezone":"Mars/Olympus","graceMinutes":60,"setAt":"2026-11-01T00:00:00Z"}"#
            ),
            None,
            "an unknown zone is not a promise"
        );

        let loaded = parse_loaded(&load_fixture().to_string()).expect("the fixture parses");
        let view = health_of(&loaded, &admin());
        assert_eq!(view.status, "grey");
        assert_eq!(view.detail, NO_PROMISE);
        assert!(view.promise.is_none());
        assert!(view.last_write.is_none(), "the fixture has no snapshot");
    }

    #[test]
    fn a_deadline_in_the_dst_gap_is_skipped() {
        // 2026-03-29 is the Berlin spring-forward: 02:00 jumps to 03:00, so 02:30 does not
        // exist that day and the most recent deadline is the previous day's.
        let promise = promise("02:30", "Europe/Berlin", 60);
        let (status, due, _, _) = freshness_health(
            &promise,
            Some(at("2026-03-28T03:00:00Z")),
            at("2026-03-29T02:00:00Z"),
        );
        assert_eq!(status, "green");
        assert_eq!(
            due,
            Some(at("2026-03-28T01:30:00Z")),
            "02:30 CET is 01:30Z, a day earlier"
        );
    }

    fn freshness_body() -> FreshnessUpdate {
        FreshnessUpdate {
            by: Some(Some("07:00".into())),
            timezone: Some("Europe/Berlin".into()),
            grace_minutes: None,
        }
    }

    #[tokio::test]
    async fn freshness_is_written_removed_and_read_back_through_the_fake_catalog() {
        let (fake, addr) = fake_catalog().await;
        let state = test_state();
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        write_freshness(
            &state,
            &catalog,
            &member,
            "sales",
            "orders",
            freshness_body(),
        )
        .await
        .expect("the promise is stored");
        let (commits, _, _) = fake.snapshot();
        assert_eq!(commits.len(), 1, "one commit stores the promise");
        assert_eq!(commits[0]["requirements"], json!([]), "no requirements");
        assert_eq!(commits[0]["updates"][0]["action"], json!("set-properties"));
        let text = commits[0]["updates"][0]["updates"][FRESHNESS_PROPERTY]
            .as_str()
            .expect("the promise is the property's JSON text")
            .to_string();
        assert!(text.contains("\"graceMinutes\":60"), "the default: {text}");
        assert!(text.contains("\"setAt\""), "the write stamps setAt: {text}");
        assert!(text.contains("\"by\":\"07:00\""), "{text}");

        // The fake catalog does not store a commit, so hand it the property the write sent;
        // writing the same promise again then changes nothing and sends nothing.
        let mut stored = load_fixture();
        stored["metadata"]["properties"]
            .as_object_mut()
            .expect("properties")
            .insert(FRESHNESS_PROPERTY.into(), json!(text));
        fake.set_load(stored);
        fake.reset_stats();
        write_freshness(
            &state,
            &catalog,
            &member,
            "sales",
            "orders",
            freshness_body(),
        )
        .await
        .expect("the same promise answers the current status");
        assert!(
            fake.snapshot().0.is_empty(),
            "nothing changed, so nothing was sent"
        );

        // An absent `by` is a refusal, not a removal.
        let absent = write_freshness(
            &state,
            &catalog,
            &member,
            "sales",
            "orders",
            FreshnessUpdate::default(),
        )
        .await
        .expect_err("a body without a deadline names nothing");
        assert_eq!(absent.status(), StatusCode::BAD_REQUEST);

        // The removal body as it arrives: an explicit `null` is a removal, an absent key
        // changes nothing (and is refused above).
        let removal: FreshnessUpdate =
            serde_json::from_str(r#"{"by":null}"#).expect("a removal body parses");
        assert_eq!(
            removal.by,
            Some(None),
            "an explicit null is not an absent key"
        );
        write_freshness(&state, &catalog, &member, "sales", "orders", removal)
            .await
            .expect("the promise is removed");
        let (commits, _, _) = fake.snapshot();
        assert_eq!(commits.len(), 1);
        assert_eq!(
            commits[0]["updates"],
            json!([{ "action": "remove-properties", "removals": [FRESHNESS_PROPERTY] }]),
            "by: null is a remove-properties of stardelt.freshness"
        );
    }

    #[tokio::test]
    async fn the_health_list_is_red_first_and_skips_tables_without_a_promise() {
        let (fake, addr) = fake_catalog().await;
        let state = crate::router_tests::test_state_with_lakekeeper(&addr);
        let catalog = catalog(&state, &addr);
        let member = who("ada", &["data-eng"], false);

        // Deadlines are relative to the real clock the route reads: `by 00:00 UTC` makes
        // today's deadline the most recent one, and the newest write decides the status.
        let now = now();
        fake.set_catalog("sales", &["green_orders", "red_orders", "plain"]);
        fake.set_table_load(
            "sales",
            "green_orders",
            load_with_promise(&promise("00:00", "UTC", 60), Some(now.timestamp_millis())),
        );
        fake.set_table_load(
            "sales",
            "red_orders",
            load_with_promise(
                &promise("00:00", "UTC", 0),
                Some((now - chrono::Duration::days(30)).timestamp_millis()),
            ),
        );
        fake.set_table_load("sales", "plain", load_fixture());

        let answer = health_list(&state, &member, &catalog)
            .await
            .expect("the list answers");
        assert_eq!(answer.checked, 3, "all three tables were read");
        let listed: Vec<(&str, &str)> = answer
            .tables
            .iter()
            .map(|t| (t.name.as_str(), t.status.as_str()))
            .collect();
        assert_eq!(
            listed,
            vec![("red_orders", "red"), ("green_orders", "green")],
            "the red table first, the amber/red order by status, the unpromised table skipped"
        );
        assert!(
            answer
                .tables
                .iter()
                .all(|t| t.catalog == "warehouse" && t.namespace == "sales"),
            "every row names the catalog and the namespace it was read from"
        );
        assert!(
            answer.tables.iter().all(|t| t.can_edit),
            "ada owns the group"
        );
    }

    #[test]
    fn the_last_write_is_the_current_snapshots_time() {
        let mut body = load_fixture();
        body["metadata"]["current-snapshot-id"] = json!(7);
        body["metadata"]["snapshots"] = json!([
            { "snapshot-id": 6, "timestamp-ms": 1789000000000i64 },
            { "snapshot-id": 7, "timestamp-ms": 1790000000000i64 }
        ]);
        let loaded = parse_loaded(&body.to_string()).expect("the fixture parses");
        assert_eq!(
            loaded.last_write,
            DateTime::from_timestamp_millis(1790000000000),
            "the time of the snapshot current-snapshot-id names"
        );
        assert_eq!(
            rfc3339(loaded.last_write.expect("a snapshot")),
            "2026-09-21T14:13:20Z"
        );

        // `-1` is an empty table, and so is a fixture without snapshot keys.
        let mut empty = load_fixture();
        empty["metadata"]["current-snapshot-id"] = json!(-1);
        empty["metadata"]["snapshots"] = json!([]);
        let loaded = parse_loaded(&empty.to_string()).expect("the fixture parses");
        assert_eq!(loaded.last_write, None);
        let loaded = parse_loaded(&load_fixture().to_string()).expect("the fixture parses");
        assert_eq!(loaded.last_write, None);
    }
}
