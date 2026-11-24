//! Access requests (D12, stardelt/stardelt#489 slice 1): one person asks to read or write a
//! catalog, its owner group approves.
//!
//! This slice stores the request and nothing else. A request changes no grant: the approval
//! slice applies it to the catalog, and until then a request is only a question. So the
//! routes here are safe on an installation that has no approval path yet.
//!
//! A person creates one, lists their own (newest first, at most 50), reads one and withdraws
//! one. Somebody else's id answers `404`, never `403`: the API never confirms that another
//! person's request exists.
//!
//! The requester is the signed-in caller, never the body: `requesterSub`/`requesterName` are
//! read from the identity. The group is the one the request asks for, and the caller must
//! already be a member of it — the approval slice re-checks membership against the
//! directory, so this is the cheap early refusal, not the authority. An IdP that left the
//! groups out of the token (`groups_overage`) cannot be judged here, so the request is
//! accepted and the approval slice decides.
//!
//! `manage` is the catalog owner's verb and is never requestable. Without a state store
//! (`NOVA_DATABASE_URL` unset) every route answers
//! `503 {"error":"this needs Nova's database, and this installation has none; ask your administrator"}`.

use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::api_json::ApiJson;
use crate::auth::{Identity, Principal};
use crate::saved_queries::{error, not_found, store_failed, unavailable};

/// Most characters a reason may carry.
pub const MAX_REASON_CHARS: usize = 500;
/// The verbs a person may request. `manage` belongs to the catalog's owner.
pub const VERBS: [&str; 4] = ["browse", "select", "write", "create"];
/// Rows one list answers, newest first.
const LIST_LIMIT: usize = 50;
/// The answer when a request was already withdrawn.
const ALREADY_WITHDRAWN: &str = "this request was withdrawn";
/// The answer when a body asks for the catalog owner's verb.
const MANAGE: &str =
    "manage belongs to the catalog's owner; request browse, select, write or create";

/// `POST /api/access-requests` body. The requester is never part of it.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateAccessRequest {
    /// The group whose approval is asked for; the caller must be a member.
    pub group: String,
    /// The catalog the access is asked for.
    pub catalog: String,
    /// What to ask for: one or more of `browse`, `select`, `write`, `create`.
    pub verbs: Vec<String>,
    /// Why, in one sentence, at most 500 characters.
    pub reason: String,
}

/// A row as the API returns it. `created_at` and `state_changed_at` are RFC 3339 in UTC.
#[derive(Debug, Serialize, sqlx::FromRow, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccessRequest {
    pub id: i64,
    /// The `sub` of the person who asked.
    #[serde(rename = "requesterSub")]
    #[schema(rename = "requesterSub")]
    pub requester_sub: String,
    /// Their display name, as the token carried it.
    #[serde(rename = "requesterName")]
    #[schema(rename = "requesterName")]
    pub requester_name: String,
    /// The group whose approval is asked for.
    #[serde(rename = "groupName")]
    #[schema(rename = "groupName")]
    pub group_name: String,
    /// The catalog the access is asked for.
    pub catalog: String,
    /// The requested verbs, sorted: one or more of `browse`, `select`, `write`, `create`.
    pub verbs: Vec<String>,
    /// Why, in one sentence.
    pub reason: String,
    /// `open` or `withdrawn`.
    pub state: String,
    /// When it was created, RFC 3339.
    #[serde(rename = "createdAt")]
    #[schema(rename = "createdAt")]
    pub created_at: String,
    /// When its state last changed, RFC 3339.
    #[serde(rename = "stateChangedAt")]
    #[schema(rename = "stateChangedAt")]
    pub state_changed_at: String,
}

/// The columns [`AccessRequest`] reads; the timestamps as RFC 3339 in UTC.
const ROW_COLUMNS: &str = "id, requester_sub, requester_name, group_name, catalog, verbs, reason, state, \
    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS created_at, \
    to_char(state_changed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS state_changed_at";

/// The group membership rule of this slice: a caller who is not in the group may not ask for
/// it. An IdP that left the groups out of the token (`groups_overage`) cannot be judged, so
/// it is not refused — the approval slice asks the directory.
fn refused_group(identity: &Identity, group: &str) -> Option<String> {
    if identity.groups_overage {
        return None;
    }
    if identity.groups.iter().any(|g| g == group) {
        return None;
    }
    Some(format!("you are not a member of group {group}"))
}

/// At least one verb, every verb one of [`VERBS`], no duplicates, sorted.
fn clean_verbs(verbs: &[String]) -> Result<Vec<String>, String> {
    if verbs.is_empty() {
        return Err("at least one verb must be requested".into());
    }
    let mut cleaned: Vec<String> = Vec::with_capacity(verbs.len());
    for verb in verbs {
        let verb = verb.trim();
        if verb == "manage" {
            return Err(MANAGE.into());
        }
        if !VERBS.contains(&verb) {
            return Err(format!(
                "`{verb}` is not a verb; request browse, select, write or create"
            ));
        }
        if cleaned.iter().any(|seen| seen == verb) {
            return Err(format!("`{verb}` is requested twice"));
        }
        cleaned.push(verb.to_string());
    }
    cleaned.sort();
    Ok(cleaned)
}

/// Trim the text fields and enforce the limits, so a caller gets a sentence instead of a
/// database error.
fn validate(req: CreateAccessRequest) -> Result<CreateAccessRequest, String> {
    let group = req.group.trim().to_string();
    if group.is_empty() {
        return Err("the group must not be empty".into());
    }
    let catalog = req.catalog.trim().to_string();
    if catalog.is_empty() {
        return Err("the catalog must not be empty".into());
    }
    let reason = req.reason.trim().to_string();
    if reason.is_empty() {
        return Err("the reason must not be empty".into());
    }
    if reason.chars().count() > MAX_REASON_CHARS {
        return Err(format!(
            "the reason must be at most {MAX_REASON_CHARS} characters"
        ));
    }
    let verbs = clean_verbs(&req.verbs)?;
    Ok(CreateAccessRequest {
        group,
        catalog,
        verbs,
        reason,
    })
}

/// The body of `create`: validate the request (400), then the caller's group (403), then the
/// store (503), then store the row.
#[allow(clippy::result_large_err)]
async fn create_row(
    s: &Arc<AppState>,
    caller: Option<&Principal>,
    req: CreateAccessRequest,
) -> Result<AccessRequest, Response> {
    let new = match validate(req) {
        Ok(new) => new,
        Err(message) => return Err(error(StatusCode::BAD_REQUEST, &message)),
    };
    let identity = requester(s, caller);
    // In dev mode there is no Principal: the single operator is the caller and no IdP
    // supplied groups, so there is no membership to judge.
    if caller.is_some()
        && let Some(message) = refused_group(&identity, &new.group)
    {
        return Err(error(StatusCode::FORBIDDEN, &message));
    }
    let Some(pool) = s.db.as_ref() else {
        return Err(unavailable());
    };
    let sql = format!(
        "INSERT INTO access_request \
         (requester_sub, requester_name, group_name, catalog, verbs, reason) \
         VALUES ($1, $2, $3, $4, $5, $6) RETURNING {ROW_COLUMNS}"
    );
    sqlx::query_as::<_, AccessRequest>(sqlx::AssertSqlSafe(sql))
        .bind(&identity.sub)
        .bind(&identity.display_name)
        .bind(&new.group)
        .bind(&new.catalog)
        .bind(&new.verbs)
        .bind(&new.reason)
        .fetch_one(pool)
        .await
        .map_err(store_failed)
}

/// The body of `list`: the caller's own requests, newest first.
#[allow(clippy::result_large_err)]
async fn list_rows(
    s: &Arc<AppState>,
    caller: Option<&Principal>,
) -> Result<Vec<AccessRequest>, Response> {
    let Some(pool) = s.db.as_ref() else {
        return Err(unavailable());
    };
    let sql = format!(
        "SELECT {ROW_COLUMNS} FROM access_request \
         WHERE requester_sub = $1 ORDER BY created_at DESC, id DESC LIMIT {LIST_LIMIT}"
    );
    sqlx::query_as::<_, AccessRequest>(sqlx::AssertSqlSafe(sql))
        .bind(requester(s, caller).sub)
        .fetch_all(pool)
        .await
        .map_err(store_failed)
}

/// The body of `read`: the caller's own row, or a 404 that confirms nothing.
#[allow(clippy::result_large_err)]
async fn row_for(
    s: &Arc<AppState>,
    caller: Option<&Principal>,
    id: i64,
) -> Result<AccessRequest, Response> {
    let Some(pool) = s.db.as_ref() else {
        return Err(unavailable());
    };
    let sql =
        format!("SELECT {ROW_COLUMNS} FROM access_request WHERE id = $1 AND requester_sub = $2");
    match sqlx::query_as::<_, AccessRequest>(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .bind(requester(s, caller).sub)
        .fetch_optional(pool)
        .await
    {
        Ok(Some(row)) => Ok(row),
        Ok(None) => Err(not_found()),
        Err(e) => Err(store_failed(e)),
    }
}

/// The body of `withdraw`: the caller's own open row becomes `withdrawn`; a withdrawn one is
/// a 409, somebody else's a 404.
#[allow(clippy::result_large_err)]
async fn withdraw_row(
    s: &Arc<AppState>,
    caller: Option<&Principal>,
    id: i64,
) -> Result<AccessRequest, Response> {
    let Some(pool) = s.db.as_ref() else {
        return Err(unavailable());
    };
    let sub = requester(s, caller).sub;
    let sql = format!(
        "UPDATE access_request \
         SET state = 'withdrawn', state_changed_at = now() \
         WHERE id = $1 AND requester_sub = $2 AND state = 'open' RETURNING {ROW_COLUMNS}"
    );
    match sqlx::query_as::<_, AccessRequest>(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .bind(&sub)
        .fetch_optional(pool)
        .await
    {
        Ok(Some(row)) => return Ok(row),
        Ok(None) => {}
        Err(e) => return Err(store_failed(e)),
    }
    // Nothing changed: either it is not the caller's (404, as for any unknown id) or it was
    // already withdrawn (409, the caller's own row, so no leak).
    let existing: Option<(String,)> = match sqlx::query_as(
        "SELECT state FROM access_request WHERE id = $1 AND requester_sub = $2",
    )
    .bind(id)
    .bind(&sub)
    .fetch_optional(pool)
    .await
    {
        Ok(row) => row,
        Err(e) => return Err(store_failed(e)),
    };
    match existing {
        Some(_) => Err(error(StatusCode::CONFLICT, ALREADY_WITHDRAWN)),
        None => Err(not_found()),
    }
}

/// The identity of the caller: the token's, or the static dev user when there is no
/// Principal (`catalog_docs::caller_identity`).
fn requester(s: &AppState, caller: Option<&Principal>) -> Identity {
    crate::catalog_docs::caller_identity(s, caller)
}

/// `GET /api/access-requests` — the caller's own requests, newest first, at most 50.
#[utoipa::path(
    get,
    operation_id = "access_requests_list",
    path = "/api/access-requests",
    tag = "public",
    description = "The caller's own access requests: which catalog and group they asked for, the requested verbs and why, and whether the request is still open. Newest first, at most 50. Another person's requests are never listed.",
    responses(
        (status = 200, description = "The caller's requests, newest first", body = Vec<AccessRequest>),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn list(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
) -> Response {
    match list_rows(&s, caller.as_ref().map(|c| &c.0)).await {
        Ok(rows) => Json(rows).into_response(),
        Err(response) => response,
    }
}

/// `POST /api/access-requests` — `201` with the stored row.
#[utoipa::path(
    post,
    operation_id = "access_requests_create",
    path = "/api/access-requests",
    tag = "public",
    description = "Ask the owner group of a catalog for access: the group (the caller must be a member of it), the catalog, one or more of `browse`, `select`, `write`, `create`, and a reason of at most 500 characters. `manage` belongs to the catalog's owner and cannot be requested. The requester is the signed-in caller, never part of the body. An owner group's IdP that leaves the groups out of the token cannot be judged here, so such a request is accepted and the approval decides. The request changes no grant.",
    request_body = CreateAccessRequest,
    responses(
        (status = 201, description = "The stored row", body = AccessRequest),
        (status = 400, description = "The group, catalog or reason is empty, the reason is over 500 characters, or a verb is unknown, repeated or `manage`", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller is not a member of the requested group", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn create(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    // The body extractor has to be last: axum consumes the request for it.
    ApiJson(req): ApiJson<CreateAccessRequest>,
) -> Response {
    match create_row(&s, caller.as_ref().map(|c| &c.0), req).await {
        Ok(row) => (StatusCode::CREATED, Json(row)).into_response(),
        Err(response) => response,
    }
}

/// `GET /api/access-requests/{id}` — the caller's own row; somebody else's is a 404.
#[utoipa::path(
    get,
    operation_id = "access_requests_read",
    path = "/api/access-requests/{id}",
    tag = "public",
    description = "One of the caller's own access requests. Another person's request answers 404, exactly like an id that does not exist: the API never confirms that it is there.",
    params(("id" = i64, Path, description = "The access request's id")),
    responses(
        (status = 200, description = "The row", body = AccessRequest),
        (status = 404, description = "No such request for this caller", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn read(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Path(id): Path<String>,
) -> Response {
    let Ok(id) = id.parse::<i64>() else {
        return not_found();
    };
    match row_for(&s, caller.as_ref().map(|c| &c.0), id).await {
        Ok(row) => Json(row).into_response(),
        Err(response) => response,
    }
}

/// `DELETE /api/access-requests/{id}` — withdraw one's own open request; `200` with the row.
#[utoipa::path(
    delete,
    operation_id = "access_requests_withdraw",
    path = "/api/access-requests/{id}",
    tag = "public",
    description = "Withdraw one of the caller's own open access requests: its state becomes `withdrawn` and it keeps that state. A request that is already withdrawn answers 409, another person's 404. Nothing is deleted, and no grant is touched.",
    params(("id" = i64, Path, description = "The access request's id")),
    responses(
        (status = 200, description = "The row, now `withdrawn`", body = AccessRequest),
        (status = 404, description = "No such request for this caller", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The request was already withdrawn", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn withdraw(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Path(id): Path<String>,
) -> Response {
    let Ok(id) = id.parse::<i64>() else {
        return not_found();
    };
    match withdraw_row(&s, caller.as_ref().map(|c| &c.0), id).await {
        Ok(row) => Json(row).into_response(),
        Err(response) => response,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn identity(sub: &str, groups: &[&str]) -> Identity {
        Identity {
            sub: sub.into(),
            display_name: format!("{sub} (test)"),
            email: None,
            name: None,
            groups: groups.iter().map(|g| (*g).to_string()).collect(),
            roles: Vec::new(),
            groups_overage: false,
            is_admin: false,
        }
    }

    fn principal(sub: &str, groups: &[&str]) -> Principal {
        Principal {
            identity: identity(sub, groups),
            via: "session",
            access_token: None,
            client_id: None,
        }
    }

    fn new(group: &str, catalog: &str, verbs: &[&str], reason: &str) -> CreateAccessRequest {
        CreateAccessRequest {
            group: group.into(),
            catalog: catalog.into(),
            verbs: verbs.iter().map(|v| (*v).to_string()).collect(),
            reason: reason.into(),
        }
    }

    /// One request through the app; the answer's status and JSON body.
    async fn call(
        app: &axum::Router,
        method: &str,
        uri: &str,
        body: Option<&str>,
    ) -> (StatusCode, serde_json::Value) {
        let mut request = Request::builder().method(method).uri(uri);
        if body.is_some() {
            request = request.header("content-type", "application/json");
        }
        let request = request
            .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, value)
    }

    #[test]
    fn clean_verbs_refuses_empty_unknown_and_duplicate() {
        assert_eq!(
            clean_verbs(&[]),
            Err("at least one verb must be requested".into())
        );
        assert_eq!(
            clean_verbs(&["drop".into()]),
            Err("`drop` is not a verb; request browse, select, write or create".into())
        );
        assert_eq!(
            clean_verbs(&["select".into(), "select".into()]),
            Err("`select` is requested twice".into())
        );
        assert_eq!(
            clean_verbs(&["write".into(), "select".into()]),
            Ok(vec!["select".into(), "write".into()]),
            "at least one, known, unique, sorted"
        );
    }

    #[test]
    fn refused_group_names_the_missing_group() {
        let member = identity("ada", &["finance"]);
        assert_eq!(refused_group(&member, "finance"), None);
        let outsider = identity("ada", &["sales"]);
        assert_eq!(
            refused_group(&outsider, "finance"),
            Some("you are not a member of group finance".into())
        );
        let overage = Identity {
            groups_overage: true,
            ..identity("ada", &[])
        };
        assert_eq!(
            refused_group(&overage, "finance"),
            None,
            "an overage token cannot be judged, so the approval decides"
        );
    }

    /// A body that is not a request is a 400 before the store is consulted, so an
    /// installation without a database still explains what is wrong with the body.
    #[tokio::test]
    async fn a_request_with_bad_verbs_is_400() {
        let (app, _dir) = crate::router_tests::test_app("access-requests-400");
        let (status, body) = call(
            &app,
            "POST",
            "/api/access-requests",
            Some(r#"{"group":"finance","catalog":"gold","verbs":[],"reason":"the revenue dashboard"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "at least one verb must be requested");

        let (status, body) = call(
            &app,
            "POST",
            "/api/access-requests",
            Some(r#"{"group":"finance","catalog":"gold","verbs":["manage"],"reason":"the revenue dashboard"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], MANAGE);

        let (status, body) = call(
            &app,
            "POST",
            "/api/access-requests",
            Some(r#"{"group":"","catalog":"gold","verbs":["select"],"reason":"the revenue dashboard"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "the group must not be empty");
    }

    /// A valid body on an installation without a state store says the store is missing.
    #[tokio::test]
    async fn a_request_without_a_database_is_503() {
        let (app, _dir) = crate::router_tests::test_app("access-requests-503");
        let (status, body) = call(
            &app,
            "POST",
            "/api/access-requests",
            Some(r#"{"group":"finance","catalog":"gold","verbs":["select"],"reason":"the revenue dashboard"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let message = body["error"].as_str().expect("the error envelope");
        assert!(message.contains("Nova's database"), "got {message}");
    }

    /// The rows of one person: created, listed newest first, read back, withdrawn once, and
    /// never visible to somebody else.
    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn the_store_lists_the_callers_rows_and_withdraws_once() {
        let pool = crate::db::test_support::migrated_pool().await;
        let s = crate::router_tests::test_state_with_db(Some(pool));
        let demo = principal("demo", &["finance"]);
        let other = principal("other-person", &["finance"]);
        async fn create(
            s: &Arc<AppState>,
            who: &Principal,
            req: CreateAccessRequest,
        ) -> AccessRequest {
            match create_row(s, Some(who), req).await {
                Ok(row) => row,
                Err(_) => panic!("a request is stored"),
            }
        }

        let first = create(&s, &demo, new("finance", "gold", &["select"], "one")).await;
        assert_eq!(first.requester_sub, "demo");
        assert_eq!(first.requester_name, "demo (test)");
        assert_eq!(first.verbs, vec!["select".to_string()]);
        assert_eq!(first.state, "open");
        assert!(first.created_at.ends_with('Z'), "{}", first.created_at);
        let second = create(&s, &demo, new("finance", "silver", &["write"], "two")).await;
        let foreign = create(&s, &other, new("finance", "gold", &["browse"], "three")).await;

        let rows = match list_rows(&s, Some(&demo)).await {
            Ok(rows) => rows,
            Err(_) => panic!("the caller's rows are listed"),
        };
        assert_eq!(
            rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![second.id, first.id],
            "two rows of the caller, newest first"
        );

        assert_eq!(
            row_for(&s, Some(&demo), foreign.id)
                .await
                .err()
                .map(|r| r.status()),
            Some(StatusCode::NOT_FOUND),
            "another person's request is not found"
        );
        assert_eq!(
            row_for(&s, Some(&demo), first.id)
                .await
                .map(|row| row.id)
                .ok(),
            Some(first.id)
        );

        let withdrawn = match withdraw_row(&s, Some(&demo), first.id).await {
            Ok(row) => row,
            Err(_) => panic!("an open request is withdrawn"),
        };
        assert_eq!(withdrawn.state, "withdrawn");
        assert_eq!(
            withdraw_row(&s, Some(&demo), first.id)
                .await
                .err()
                .map(|r| r.status()),
            Some(StatusCode::CONFLICT),
            "a second withdraw is a 409"
        );
        assert_eq!(
            withdraw_row(&s, Some(&other), first.id)
                .await
                .err()
                .map(|r| r.status()),
            Some(StatusCode::NOT_FOUND),
            "another person's request is not found"
        );
    }
}
