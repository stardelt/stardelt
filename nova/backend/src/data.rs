//! Load & transfer: CSV/JSON ingestion into an Iceberg table, and table-to-table
//! copies. The operations issue DDL/DML through Trino.

use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::auth::Principal;
use crate::{AppState, api_json::ApiJson, catalog, jobs, trino};

#[derive(Clone, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UploadRequest {
    #[serde(default)]
    pub catalog: Option<String>,
    pub namespace: String,
    pub table: String,
    #[serde(default = "default_format")]
    pub format: String,
    pub content: String,
}

fn default_format() -> String {
    "csv".into()
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct UploadResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    pub namespace: String,
    pub table: String,
    pub rows_loaded: usize,
    pub job_id: String,
}

#[derive(Clone, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TransferRequest {
    #[serde(default)]
    pub catalog: Option<String>,
    pub source: String,
    pub target: String,
    #[serde(default = "default_mode")]
    pub mode: String,
}

fn default_mode() -> String {
    "overwrite".into()
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct TransferResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_catalog: Option<String>,
    pub source: String,
    pub target: String,
    pub rows: usize,
    pub job_id: String,
}

#[utoipa::path(
    post,
    operation_id = "ingest_upload",
    path = "/api/ingest/upload",
    tag = "internal",
    description = "Load a CSV or JSON text into the table `catalog.namespace.table` (default: the caller's first writable catalog, otherwise `shared`; `warehouse` is administrators-only). `catalog` is optional. The whole file is the string `content` of the request. `format` is `csv` (the default; any value except `json` is read as CSV) or `json` (an array of objects). Column types are inferred from the values. The rows are built in a staging table first and then replace the target, so an existing table of that name is replaced, not appended to. The statements run with the caller's own identity, so the engine's permissions apply. Every call is recorded as a job (`job_id`, listed by `GET /api/jobs`). A file that cannot be read or a statement the engine refuses is a 400 with a sentence, and the job is recorded as failed.",
    request_body = UploadRequest,
    responses(
        (status = 200, description = "The table was loaded", body = UploadResult),
        (status = 400, description = "The file could not be read or the engine refused a statement", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not write that catalog", body = crate::openapi::ErrorResponse),
        (status = 404, description = "Catalog not found", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn upload(
    State(s): State<Arc<AppState>>,
    caller: Option<axum::Extension<Principal>>,
    // The body extractor has to be last: axum consumes the request for it.
    ApiJson(req): ApiJson<UploadRequest>,
) -> Response {
    let p = caller.as_ref().map(|c| &c.0);
    let catalog_name = if let Some(ref c) = req.catalog {
        if c.trim().is_empty() {
            None
        } else {
            Some(c.trim().to_string())
        }
    } else {
        None
    };

    let catalog_name = match catalog_name {
        Some(name) => {
            if !catalog::valid_catalog(&name) {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "catalog name must be lower-case letters and digits" })),
                )
                    .into_response();
            }
            // Check if caller may write
            if !catalog::catalog_can_write(&s, p, &name).await {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({ "error": format!("you cannot write catalog '{name}'") })),
                )
                    .into_response();
            }
            // Check if catalog exists/browsable? Also ensure it's in allowed set
            let allowed = match catalog::allowed_catalogs(&s, p).await {
                Ok(a) => a,
                Err(e) => {
                    return (
                        crate::trino::error_status(&e),
                        Json(serde_json::json!({ "error": e })),
                    )
                        .into_response();
                }
            };
            if !allowed.contains(&name) {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({ "error": format!("you cannot browse catalog '{name}'") })),
                )
                    .into_response();
            }
            name
        }
        None => match catalog::first_writable_catalog(&s, p).await {
            Some(name) => name,
            None => {
                return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({ "error": "no catalog you may write — ask an administrator for a group catalog" })),
                    )
                        .into_response();
            }
        },
    };

    let fmt = req.format.to_lowercase();
    let title = format!("{}.{}.{}", catalog_name, req.namespace, req.table);
    let (user, owner) = caller
        .as_ref()
        .map(|c| (c.0.identity.display_name.clone(), c.0.identity.sub.clone()))
        .unwrap_or_else(|| (s.dev_user.clone(), s.dev_user.clone()));

    let (s2, req2, fmt2) = (s.clone(), req.clone(), fmt.clone());
    let p2 = caller.map(|c| c.0);
    let cat2 = catalog_name.clone();
    let outcome =
        tokio::spawn(async move { real_upload(&s2, &req2, &fmt2, p2.as_ref(), &cat2).await })
            .await
            .map_err(|e| e.to_string())
            .and_then(|r| r);
    match outcome {
        Ok(n) => {
            let job = jobs::record(
                &s.jobs,
                "load",
                title,
                "succeeded",
                Some(format!("{} via {fmt}", rows(n))),
                Some(n),
                &user,
                &owner,
            );
            Json(UploadResult {
                catalog: Some(catalog_name.clone()),
                namespace: req.namespace,
                table: req.table,
                rows_loaded: n,
                job_id: job.id,
            })
            .into_response()
        }
        Err(e) => {
            jobs::record(
                &s.jobs,
                "load",
                title,
                "failed",
                Some(e.clone()),
                None,
                &user,
                &owner,
            );
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
    }
}

#[utoipa::path(
    post,
    operation_id = "tables_transfer",
    path = "/api/transfer",
    tag = "internal",
    description = "Copy tables between catalogs (defaulting to the caller's first writable catalog). `catalog` (or `source_catalog`/`target_catalog`) may be used; source and target must be in the same catalog for now. `source` and `target` are `namespace.table`. `mode` is `overwrite` (the default: the target is dropped and rebuilt from the source) or `append` (rows are added to the target). Overwriting a table with itself is refused, because the copy would read from a table that no longer exists; appending to itself is allowed. The statements run with the caller's own identity, so the engine's permissions apply. The answer counts the rows now in the target. Every call is recorded as a job (`job_id`, listed by `GET /api/jobs`). A refusal or an engine error is a 400 with a sentence, and the job is recorded as failed.",
    request_body = TransferRequest,
    responses(
        (status = 200, description = "The table was copied", body = TransferResult),
        (status = 400, description = "The mode, a table name or the copy was refused", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller may not write that catalog", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn transfer(
    State(s): State<Arc<AppState>>,
    caller: Option<axum::Extension<Principal>>,
    // The body extractor has to be last: axum consumes the request for it.
    ApiJson(req): ApiJson<TransferRequest>,
) -> Response {
    let p = caller.as_ref().map(|c| &c.0);
    // Resolve catalog
    let catalog_name = if let Some(ref c) = req.catalog {
        if c.trim().is_empty() {
            None
        } else {
            Some(c.trim().to_string())
        }
    } else {
        None
    };

    let catalog_name = match catalog_name {
        Some(name) => {
            if !catalog::valid_catalog(&name) {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "catalog name must be lower-case letters and digits" })),
                )
                    .into_response();
            }
            if !catalog::catalog_can_write(&s, p, &name).await {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({ "error": format!("you cannot write catalog '{name}'") })),
                )
                    .into_response();
            }
            let allowed = match catalog::allowed_catalogs(&s, p).await {
                Ok(a) => a,
                Err(e) => {
                    return (
                        crate::trino::error_status(&e),
                        Json(serde_json::json!({ "error": e })),
                    )
                        .into_response();
                }
            };
            if !allowed.contains(&name) {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({ "error": format!("you cannot browse catalog '{name}'") })),
                )
                    .into_response();
            }
            name
        }
        None => match catalog::first_writable_catalog(&s, p).await {
            Some(name) => name,
            None => {
                return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({ "error": "no catalog you may write — ask an administrator for a group catalog" })),
                    )
                        .into_response();
            }
        },
    };

    let title = format!("{} → {}", req.source, req.target);
    let (user, owner) = caller
        .as_ref()
        .map(|c| (c.0.identity.display_name.clone(), c.0.identity.sub.clone()))
        .unwrap_or_else(|| (s.dev_user.clone(), s.dev_user.clone()));

    let (s2, req2) = (s.clone(), req.clone());
    let p2 = caller.map(|c| c.0);
    let cat2 = catalog_name.clone();
    let cat2_clone = cat2.clone();
    let outcome = tokio::spawn(async move { real_transfer(&s2, &req2, p2.as_ref(), &cat2).await })
        .await
        .map_err(|e| e.to_string())
        .and_then(|r| r);
    match outcome {
        Ok(n) => {
            let job = jobs::record(
                &s.jobs,
                "transfer",
                title,
                "succeeded",
                Some(format!("{} {}", req.mode, rows(n))),
                Some(n),
                &user,
                &owner,
            );
            Json(TransferResult {
                source_catalog: Some(cat2_clone.clone()),
                target_catalog: Some(cat2_clone.clone()),
                source: req.source,
                target: req.target,
                rows: n,
                job_id: job.id,
            })
            .into_response()
        }
        Err(e) => {
            jobs::record(
                &s.jobs,
                "transfer",
                title,
                "failed",
                Some(e.clone()),
                None,
                &user,
                &owner,
            );
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": e })),
            )
                .into_response()
        }
    }
}

/// Parse `ns.table` or `catalog.ns.table` into `(ns, table)`.
fn parse_ref(raw: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = raw.trim().split('.').filter(|p| !p.is_empty()).collect();
    if parts.len() < 2 {
        return Err(format!("expected schema.table, got '{raw}'"));
    }
    Ok((
        parts[parts.len() - 2].to_string(),
        parts[parts.len() - 1].to_string(),
    ))
}

fn parse_csv(content: &str) -> Result<(Vec<String>, Vec<Vec<Value>>), String> {
    let records = read_csv_records(content);
    // Drop blank lines: a single unquoted empty cell is not a record.
    let records: Vec<Vec<Cell>> = records
        .into_iter()
        .filter(|r| !(r.len() == 1 && !r[0].1 && r[0].0.trim().is_empty()))
        .collect();
    let header = records.first().ok_or("empty CSV")?;
    let columns: Vec<String> = header.iter().map(|(t, _)| t.trim().to_string()).collect();
    if columns.is_empty() || columns.iter().all(|c| c.is_empty()) {
        return Err("missing CSV header".into());
    }
    let want = columns.len();
    let mut rows = Vec::new();
    for (idx, rec) in records.iter().skip(1).enumerate() {
        let n = idx + 1;
        if rec.len() != want {
            return Err(format!(
                "row {n} has {} fields but the header has {want}",
                rec.len()
            ));
        }
        rows.push(
            rec.iter()
                .map(|(t, quoted)| {
                    let raw = if *quoted { t.as_str() } else { t.trim() };
                    coerce(raw)
                })
                .collect(),
        );
    }
    Ok((columns, rows))
}

fn parse_json_table(content: &str) -> Result<(Vec<String>, Vec<Vec<Value>>), String> {
    let value: Value = serde_json::from_str(content).map_err(|e| format!("invalid JSON: {e}"))?;
    let arr = value.as_array().ok_or("JSON must be an array of objects")?;
    let mut columns: Vec<String> = Vec::new();
    for row in arr {
        if let Some(obj) = row.as_object() {
            for key in obj.keys() {
                if !columns.contains(key) {
                    columns.push(key.clone());
                }
            }
        }
    }
    if columns.is_empty() {
        return Err("no object keys found".into());
    }
    let rows = arr
        .iter()
        .map(|row| {
            let obj = row.as_object();
            columns
                .iter()
                .map(|c| obj.and_then(|o| o.get(c)).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();
    Ok((columns, rows))
}

type Cell = (String, bool);

fn read_csv_records(content: &str) -> Vec<Vec<Cell>> {
    let mut records: Vec<Vec<Cell>> = Vec::new();
    let mut row: Vec<Cell> = Vec::new();
    let mut cur = String::new();
    let mut was_quoted = false;
    let mut in_quoted = false;
    let mut after_quote = false;
    let chars: Vec<char> = content.chars().collect();
    let mut i = 0;
    let mut field_has_content = false;
    while i < chars.len() {
        let ch = chars[i];
        if in_quoted {
            if ch == '"' {
                if i + 1 < chars.len() && chars[i + 1] == '"' {
                    cur.push('"');
                    i += 2;
                    field_has_content = true;
                    continue;
                } else {
                    in_quoted = false;
                    after_quote = true;
                    i += 1;
                    continue;
                }
            } else if ch == '\r' {
                if i + 1 < chars.len() && chars[i + 1] == '\n' {
                    cur.push('\n');
                    i += 2;
                } else {
                    cur.push('\n');
                    i += 1;
                }
                field_has_content = true;
                continue;
            } else {
                cur.push(ch);
                i += 1;
                field_has_content = true;
                continue;
            }
        }
        // Not in quotes.
        if !field_has_content && !was_quoted && cur.is_empty() && ch == '"' {
            in_quoted = true;
            was_quoted = true;
            after_quote = false;
            field_has_content = true;
            i += 1;
            continue;
        }
        if ch == ',' {
            row.push((std::mem::take(&mut cur), std::mem::take(&mut was_quoted)));
            field_has_content = false;
            after_quote = false;
            i += 1;
            continue;
        }
        if ch == '\r' || ch == '\n' {
            if ch == '\r' && i + 1 < chars.len() && chars[i + 1] == '\n' {
                i += 2;
            } else {
                i += 1;
            }
            row.push((std::mem::take(&mut cur), std::mem::take(&mut was_quoted)));
            field_has_content = false;
            after_quote = false;
            records.push(std::mem::take(&mut row));
            continue;
        }
        if after_quote && ch == '"' {
            // A quote after a closed quoted section is literal text.
            cur.push(ch);
            i += 1;
            field_has_content = true;
            continue;
        }
        cur.push(ch);
        i += 1;
        field_has_content = true;
    }
    // Flush the trailing field/record when the input does not end with a newline.
    if !cur.is_empty() || was_quoted || field_has_content || !row.is_empty() {
        row.push((cur, was_quoted));
        records.push(row);
    }
    records
}

fn is_integer_text(s: &str) -> bool {
    let t = s.strip_prefix('-').unwrap_or(s);
    if t.is_empty() {
        return false;
    }
    if t == "0" {
        return true;
    }
    if t.starts_with('0') {
        return false;
    }
    !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit())
}

fn is_float_text(s: &str) -> bool {
    let t = s.strip_prefix('-').unwrap_or(s);
    if t.is_empty() {
        return false;
    }
    // Split off a trailing exponent: e/E + optional sign + digits.
    let (mantissa, exponent) = match t.find(['e', 'E']) {
        Some(pos) => {
            let (m, e) = t.split_at(pos);
            let exp = &e[1..];
            let exp = exp
                .strip_prefix('+')
                .unwrap_or_else(|| exp.strip_prefix('-').unwrap_or(exp));
            if m.is_empty() || exp.is_empty() || !exp.bytes().all(|b| b.is_ascii_digit()) {
                return false;
            }
            (m, true)
        }
        None => (t, false),
    };
    let _ = exponent;
    let dot = match mantissa.find('.') {
        Some(d) => d,
        None => return false,
    };
    if mantissa[dot + 1..].contains('.') {
        return false;
    }
    let (int_part, frac_part) = mantissa.split_at(dot);
    let frac_part = &frac_part[1..];
    if frac_part.is_empty() || !frac_part.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    if int_part.is_empty() {
        return false;
    }
    if int_part == "0" {
        return true;
    }
    if int_part.starts_with('0') {
        return false;
    }
    int_part.bytes().all(|b| b.is_ascii_digit())
}

fn coerce(raw: &str) -> Value {
    if raw.is_empty() || raw.eq_ignore_ascii_case("null") {
        Value::Null
    } else if is_integer_text(raw) {
        match raw.parse::<i64>() {
            Ok(i) => json!(i),
            Err(_) => json!(raw),
        }
    } else if is_float_text(raw) {
        match raw.parse::<f64>() {
            Ok(f) if f.is_finite() => json!(f),
            _ => json!(raw),
        }
    } else {
        json!(raw)
    }
}

/// Name of the staging table a load is built in before it replaces the target.
/// The `__nova_load_` infix makes an orphan (left by a killed pod) recognisable.
fn staging_name(table: &str, stamp: u64) -> String {
    format!("{table}__nova_load_{stamp}")
}

async fn load_rows(
    state: &AppState,
    table: &str,
    ddl_cols: &str,
    rows: &[Vec<Value>],
    types: &[String],
    caller: Option<&crate::auth::Principal>,
) -> Result<usize, String> {
    trino::execute(state, format!("CREATE TABLE {table} ({ddl_cols})"), caller).await?;
    let mut loaded = 0usize;
    for chunk in rows.chunks(1000) {
        let values = chunk
            .iter()
            .map(|row| {
                format!(
                    "({})",
                    row.iter()
                        .zip(types.iter())
                        .map(|(v, ty)| sql_literal_for(v, ty))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        trino::execute(
            state,
            format!("INSERT INTO {table} VALUES {values}"),
            caller,
        )
        .await?;
        loaded += chunk.len();
    }
    Ok(loaded)
}

async fn real_upload(
    state: &AppState,
    req: &UploadRequest,
    fmt: &str,
    caller: Option<&crate::auth::Principal>,
    catalog: &str,
) -> Result<usize, String> {
    let (columns, rows) = if fmt == "json" {
        parse_json_table(&req.content)?
    } else {
        parse_csv(&req.content)?
    };
    if columns.is_empty() {
        return Err("no columns found".into());
    }

    let types: Vec<String> = (0..columns.len()).map(|i| infer_type(&rows, i)).collect();
    trino::execute(
        state,
        format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{}",
            quote_ident(catalog),
            quote_ident(&req.namespace)
        ),
        caller,
    )
    .await?;
    let target = format!(
        "{}.{}.{}",
        quote_ident(catalog),
        quote_ident(&req.namespace),
        quote_ident(&req.table)
    );
    let ddl_cols = columns
        .iter()
        .zip(types.iter())
        .map(|(c, t)| format!("{} {}", quote_ident(c), t))
        .collect::<Vec<_>>()
        .join(", ");

    let staging = format!(
        "{}.{}.{}",
        quote_ident(catalog),
        quote_ident(&req.namespace),
        quote_ident(&staging_name(&req.table, crate::jobs::now_ms()))
    );

    let loaded = match load_rows(state, &staging, &ddl_cols, &rows, &types, caller).await {
        Ok(n) => n,
        Err(e) => {
            let _ = trino::execute(state, format!("DROP TABLE IF EXISTS {staging}"), caller).await;
            return Err(e);
        }
    };
    let swap = trino::execute(
        state,
        format!("CREATE OR REPLACE TABLE {target} AS SELECT * FROM {staging}"),
        caller,
    )
    .await;
    let _ = trino::execute(state, format!("DROP TABLE IF EXISTS {staging}"), caller).await;
    swap?;
    Ok(loaded)
}

async fn real_transfer(
    state: &AppState,
    req: &TransferRequest,
    caller: Option<&crate::auth::Principal>,
    catalog: &str,
) -> Result<usize, String> {
    let (sns, stbl) = parse_ref(&req.source)?;
    let (tns, ttbl) = parse_ref(&req.target)?;
    let source = format!(
        "{}.{}.{}",
        quote_ident(catalog),
        quote_ident(&sns),
        quote_ident(&stbl)
    );
    let target = format!(
        "{}.{}.{}",
        quote_ident(catalog),
        quote_ident(&tns),
        quote_ident(&ttbl)
    );
    check_transfer(&req.mode, (&sns, &stbl), (&tns, &ttbl))?;

    if req.mode.eq_ignore_ascii_case("append") {
        trino::execute(
            state,
            format!("INSERT INTO {target} SELECT * FROM {source}"),
            caller,
        )
        .await?;
    } else {
        trino::execute(
            state,
            format!(
                "CREATE SCHEMA IF NOT EXISTS {}.{}",
                quote_ident(catalog),
                quote_ident(&tns)
            ),
            caller,
        )
        .await?;
        trino::execute(
            state,
            format!("CREATE OR REPLACE TABLE {target} AS SELECT * FROM {source}"),
            caller,
        )
        .await?;
    }

    let count = trino::execute(state, format!("SELECT count(*) FROM {target}"), caller).await?;
    let n = count
        .rows
        .first()
        .and_then(|r| r.first())
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;
    Ok(n)
}

pub(crate) fn check_transfer_mode(mode: &str) -> Result<(), String> {
    let m = mode.to_ascii_lowercase();
    if m == "overwrite" || m == "append" {
        Ok(())
    } else {
        Err("mode must be 'overwrite' or 'append'".into())
    }
}

/// An overwrite drops the target before copying into it, so it must never be the
/// source: the copy would read from a table that no longer exists, and the data
/// would be gone. Trino folds unquoted identifiers to lower case, hence the
/// case-insensitive comparison.
fn check_transfer(mode: &str, source: (&str, &str), target: (&str, &str)) -> Result<(), String> {
    check_transfer_mode(mode)?;
    let same = source.0.eq_ignore_ascii_case(target.0) && source.1.eq_ignore_ascii_case(target.1);
    if same && !mode.eq_ignore_ascii_case("append") {
        return Err(format!(
            "source and target are the same table ({}.{}); overwrite would delete it",
            source.0, source.1
        ));
    }
    Ok(())
}

fn infer_type(rows: &[Vec<Value>], col: usize) -> String {
    let mut ints = true;
    let mut numbers = true;
    let mut bools = true;
    let mut seen = false;
    for row in rows.iter() {
        match row.get(col) {
            None | Some(Value::Null) => {}
            Some(Value::Bool(_)) => {
                ints = false;
                numbers = false;
            }
            Some(Value::Number(n)) => {
                bools = false;
                seen = true;
                if !n.is_i64() && !n.is_u64() {
                    ints = false;
                }
            }
            Some(_) => {
                ints = false;
                numbers = false;
                bools = false;
            }
        }
    }
    if !seen && bools {
        return "boolean".into();
    }
    if seen && ints {
        "bigint".into()
    } else if seen && numbers {
        "double".into()
    } else if bools
        && rows
            .iter()
            .any(|r| matches!(r.get(col), Some(Value::Bool(_))))
    {
        "boolean".into()
    } else {
        "varchar".into()
    }
}

fn sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

fn sql_literal_for(v: &Value, ty: &str) -> String {
    if ty == "varchar" {
        match v {
            Value::Null => "NULL".into(),
            Value::Bool(b) => format!("'{b}'"),
            Value::Number(n) => format!("'{n}'"),
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            other => format!("'{}'", other.to_string().replace('\'', "''")),
        }
    } else {
        sql_literal(v)
    }
}

pub fn quote_ident(raw: &str) -> String {
    format!("\"{}\"", raw.replace('"', "\"\""))
}

/// `1 row`, `3 rows`.
fn rows(n: usize) -> String {
    if n == 1 {
        "1 row".into()
    } else {
        format!("{n} rows")
    }
}

#[cfg(test)]
mod tests {
    use super::{check_transfer, parse_csv, parse_ref, staging_name};
    use serde_json::json;

    #[test]
    fn unknown_transfer_mode_is_refused() {
        assert!(check_transfer("bogus", ("a", "b"), ("c", "d")).is_err());
        assert!(check_transfer("REPLACE", ("a", "b"), ("c", "d")).is_err());
        assert!(check_transfer("APPEND", ("a", "b"), ("c", "d")).is_ok());
    }

    #[test]
    fn staging_name_is_distinct_from_target() {
        let s = staging_name("Trips", 42).to_lowercase();
        assert_eq!(s, "trips__nova_load_42");
        assert_ne!(s, "trips");
    }

    #[test]
    fn mixed_column_is_varchar_and_quotes_numbers() {
        use serde_json::json;
        let rows = vec![vec![json!(1), json!(2)], vec![json!("abc"), json!(3)]];
        assert_eq!(super::infer_type(&rows, 0), "varchar");
        assert_eq!(super::sql_literal_for(&json!(7), "varchar"), "'7'");
        assert_eq!(super::sql_literal_for(&json!(true), "varchar"), "'true'");
        assert_eq!(super::sql_literal_for(&json!(7), "bigint"), "7");
    }

    #[test]
    fn rows_are_pluralised() {
        assert_eq!(super::rows(0), "0 rows");
        assert_eq!(super::rows(1), "1 row");
        assert_eq!(super::rows(2), "2 rows");
    }

    #[test]
    fn overwriting_a_table_with_itself_is_refused() {
        assert!(check_transfer("overwrite", ("raw", "trips"), ("raw", "trips")).is_err());
        assert!(check_transfer("overwrite", ("raw", "Trips"), ("RAW", "trips")).is_err());
        assert!(check_transfer("replace", ("raw", "trips"), ("raw", "trips")).is_err());
    }

    #[test]
    fn distinct_tables_and_appends_are_allowed() {
        assert!(check_transfer("overwrite", ("raw", "trips"), ("curated", "trips")).is_ok());
        assert!(check_transfer("append", ("raw", "trips"), ("raw", "trips")).is_ok());
    }

    #[test]
    fn ref_parsing_strips_catalog() {
        assert_eq!(
            parse_ref("warehouse.a.b").unwrap(),
            ("a".into(), "b".into())
        );
        assert_eq!(parse_ref("a.b").unwrap(), ("a".into(), "b".into()));
        assert!(parse_ref("b").is_err());
    }

    #[test]
    fn csv_escaped_quotes_survive() {
        let (cols, rows) = parse_csv("a,b\n1,\"say \"\"hi\"\"\"").unwrap();
        assert_eq!(cols, vec!["a", "b"]);
        assert_eq!(rows, vec![vec![json!(1), json!("say \"hi\"")]]);
    }

    #[test]
    fn csv_embedded_newline_stays_one_row() {
        let (cols, rows) = parse_csv("a,b\n1,\"line1\nline2\"\n2,x").unwrap();
        assert_eq!(cols, vec!["a", "b"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], json!("line1\nline2"));
    }

    #[test]
    fn csv_ragged_row_is_an_error_naming_the_row() {
        let err = parse_csv("a,b\n1,2\n3\n").unwrap_err();
        assert!(err.contains("row 2"), "{err}");
        assert!(err.contains("1") && err.contains("2"), "{err}");
    }

    #[test]
    fn leading_zero_stays_text() {
        let (_, rows) = parse_csv("zip\n00501").unwrap();
        assert_eq!(rows[0][0], json!("00501"));
        let (_, rows) = parse_csv("v\n007.5").unwrap();
        assert_eq!(rows[0][0], json!("007.5"));
        let (_, rows) = parse_csv("v\n0.5").unwrap();
        assert_eq!(rows[0][0], json!(0.5));
    }

    #[test]
    fn nan_and_inf_stay_text() {
        let (_, rows) = parse_csv("w\nnan\ninf\n").unwrap();
        assert_eq!(rows[0][0], json!("nan"));
        assert_eq!(rows[1][0], json!("inf"));
    }

    #[test]
    fn huge_integer_stays_text() {
        let (_, rows) = parse_csv("v\n9223372036854775808").unwrap();
        assert_eq!(rows[0][0], json!("9223372036854775808"));
    }
}
