//! The deployment record (design nova#357 slice 1): what one bundle deploy put on a stage,
//! from which commit and by whom.
//!
//! A bundle deploy stamps each pipeline with `bundle`, `target`, `revision` and
//! `repository`, so the pipelines say what runs but never what was deployed, when and by
//! whom. This table is that history: one row per deploy, written by the person who asked
//! for it. The pipelines stay the source of truth for what runs; nothing here is a
//! resource the platform reconciles.
//!
//! - `POST /api/deployments` records one deploy — only an administrator or a member of the
//!   pipeline's owner group may record one, and the row says which person did.
//! - `GET /api/deployments` lists the caller's rows, newest first, at most 50, optionally
//!   narrowed to one `bundle` and one `target`.
//! - `GET /api/deployments/{id}` answers one row of the caller's groups, or 404.
//! - `actor` is the caller's subject (`sub`, the stable key) and `actor_name` the name the
//!   sign-in provided (`preferred_username` → `email` → `upn` → `name` → `sub`); the page
//!   shows the name.
//! - A deployment row is kept for ten days even when it is beyond the newest 50 of its bundle
//!   and target; older rows beyond that window are deleted when the next row of that bundle and
//!   target is recorded (design nova#357).
//!
//! The state store is checked first: without one (`NOVA_DATABASE_URL` unset) both routes
//! answer `503 {"error":"this needs Nova's database, …"}`, whatever the caller; with one and
//! a caller whose identity is missing, `401`. Every write writes one line on the `audit` target with the caller, the
//! row's id, the bundle and the target — never the rendered resources.

use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;
use crate::api_json::ApiJson;
use crate::auth::{Identity, Principal};
use crate::connection_routes::Fail;
use crate::saved_queries::{error, store_failed, unavailable};

/// Most characters a bundle, an owner group or a target may carry (the table's CHECK
/// constraints allow the same).
const MAX_NAME_CHARS: usize = 64;
/// Most pipelines one deployment may list.
const MAX_PIPELINES: usize = 200;
/// Most rendered resources one deployment may carry.
const MAX_RESOURCES: usize = 200;
/// Most file digests one deployment may list (the bundle's own cap is 100).
const MAX_FILE_DIGESTS: usize = 200;
/// Rows one list answers, newest first.
const LIST_LIMIT: usize = 50;
/// The status a deployment row may carry.
const STATUSES: &[&str] = &["applied", "failed"];
/// The action a pipeline row may carry.
const ACTIONS: &[&str] = &["created", "updated", "deleted", "unchanged"];
/// The answer when a caller has no identity.
const SIGN_IN: &str = "sign in to record or read deployments";
/// Rows one bundle, target and owner group keeps, however old they are.
const KEEP_PER_TARGET: i64 = 50;
/// Days a deployment row is kept even when it is beyond [`KEEP_PER_TARGET`].
const KEEP_DAYS: i32 = 10;

/// `POST /api/deployments` body: one deploy.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NewDeployment {
    /// The bundle that was deployed, 1 to 64 characters, no `/`.
    pub bundle: String,
    /// The owner group of the deployed pipelines; the caller must be a member.
    pub owner_group: String,
    /// The stage the deploy went to, 1 to 64 characters, no `/`.
    pub target: String,
    /// The commit that was deployed, or absent when it is not known.
    #[serde(default)]
    pub revision: Option<String>,
    /// The bundle's repository, or absent.
    #[serde(default)]
    pub repository: Option<String>,
    /// `applied` or `failed`.
    pub status: String,
    /// The pipelines the deploy touched, at most 200.
    #[serde(default)]
    pub pipelines: Vec<DeploymentPipeline>,
    /// The rendered Pipeline documents this deploy applied; empty for a failed deploy.
    #[serde(default)]
    pub resources: Vec<serde_json::Value>,
    /// The files of the bundle this deploy read, at most 200; empty when the caller sent none.
    #[serde(default)]
    pub files: Vec<DeploymentFile>,
    /// The deployment a rollback replayed, or absent. It is display metadata: the page links to it, and a row the
    /// caller may not see answers 404 there.
    #[serde(default)]
    pub previous_deployment_id: Option<i64>,
}

/// One file of the bundle the deploy read: its path inside the bundle and the sha256 of its bytes.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeploymentFile {
    /// The path relative to the bundle directory, with `/` separators.
    pub path: String,
    /// The file's content, lowercase hex sha256 (64 characters).
    pub sha256: String,
}

/// One pipeline of a deploy: what changed in it.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeploymentPipeline {
    pub namespace: String,
    pub name: String,
    /// `created`, `updated`, `deleted` or `unchanged`.
    pub action: String,
}

/// A row as the API returns it. `created_at` is RFC 3339 in UTC.
#[derive(Debug, Serialize, sqlx::FromRow, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Deployment {
    pub id: i64,
    pub bundle: String,
    pub owner_group: String,
    pub target: String,
    pub revision: Option<String>,
    pub repository: Option<String>,
    pub status: String,
    /// The pipelines the deploy touched, as the caller sent them.
    #[schema(value_type = Vec<DeploymentPipeline>)]
    pub pipelines: serde_json::Value,
    /// The rendered Pipeline documents the deploy applied, as the caller sent them.
    #[schema(value_type = Vec<Object>)]
    pub resources: serde_json::Value,
    /// The files of the bundle the deploy read, as the caller sent them.
    #[schema(value_type = Vec<DeploymentFile>)]
    pub files: serde_json::Value,
    /// The deployment this row replayed, when it is a rollback; `None` for a deploy that was not one.
    pub previous_deployment_id: Option<i64>,
    /// The `sub` of the person who recorded the deploy.
    pub actor: String,
    /// The name the caller's sign-in provided; `None` for rows recorded before migration 0025.
    pub actor_name: Option<String>,
    /// When it was recorded, RFC 3339.
    pub created_at: String,
}

/// `GET /api/deployments` query: narrow the list to one bundle and/or one target.
#[derive(Debug, Deserialize, Default)]
pub struct ListParams {
    pub bundle: Option<String>,
    pub target: Option<String>,
}

/// The columns [`Deployment`] reads; the timestamp as RFC 3339 in UTC.
const ROW_COLUMNS: &str = "id, bundle, owner_group, target, revision, repository, status, \
    pipelines, resources, files, previous_deployment_id, actor, actor_name, \
    to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS created_at";

/// Whether `identity` may record a deployment of `owner_group`: an administrator, or a
/// member of the group.
pub fn may_record(identity: &Identity, owner_group: &str) -> bool {
    identity.is_admin || identity.groups.iter().any(|g| g == owner_group)
}

/// A name the API stores: 1 to 64 characters, no `/`.
fn valid_name(value: &str) -> bool {
    let len = value.chars().count();
    (1..=MAX_NAME_CHARS).contains(&len) && !value.contains('/')
}

/// The body rules the table's CHECK constraints enforce, repeated as one sentence so a
/// caller gets a `400` instead of a database error.
pub fn validate(new: &NewDeployment) -> Result<(), String> {
    if !valid_name(&new.bundle) {
        return Err(format!(
            "bundle must be 1 to {MAX_NAME_CHARS} characters and must not contain '/'"
        ));
    }
    if !valid_name(&new.owner_group) {
        return Err(format!(
            "ownerGroup must be 1 to {MAX_NAME_CHARS} characters and must not contain '/'"
        ));
    }
    if !valid_name(&new.target) {
        return Err(format!(
            "target must be 1 to {MAX_NAME_CHARS} characters and must not contain '/'"
        ));
    }
    if !STATUSES.contains(&new.status.as_str()) {
        return Err("status must be `applied` or `failed`".into());
    }
    if new.pipelines.len() > MAX_PIPELINES {
        return Err(format!("pipelines must be at most {MAX_PIPELINES} rows"));
    }
    if new.resources.len() > MAX_RESOURCES {
        return Err(format!(
            "a deployment may list at most {MAX_RESOURCES} resources"
        ));
    }
    if serde_json::to_vec(&new.resources).unwrap_or_default().len() > 1024 * 1024 {
        return Err("the rendered resources must be under 1 MiB".into());
    }
    if new.files.len() > MAX_FILE_DIGESTS {
        return Err(format!(
            "a deployment may list at most {MAX_FILE_DIGESTS} files"
        ));
    }
    for file in &new.files {
        let inside = !file.path.is_empty()
            && file.path.chars().count() <= 512
            && !file.path.starts_with('/')
            && !file.path.split('/').any(|s| s == "..");
        let digest = file.sha256.len() == 64 && file.sha256.bytes().all(|b| b.is_ascii_hexdigit());
        if !inside || !digest {
            return Err(
                "every file needs a path inside the bundle and a 64-character hex sha256".into(),
            );
        }
    }
    for pipeline in &new.pipelines {
        if pipeline.namespace.is_empty() {
            return Err("every pipeline needs a namespace".into());
        }
        if pipeline.name.is_empty() {
            return Err("every pipeline needs a name".into());
        }
        if !ACTIONS.contains(&pipeline.action.as_str()) {
            return Err(
                "a pipeline action must be `created`, `updated`, `deleted` or `unchanged`".into(),
            );
        }
    }
    Ok(())
}

/// Delete the rows of one bundle, target and owner group that are older than [`KEEP_DAYS`] days
/// and not among the newest [`KEEP_PER_TARGET`]. Answers the number of rows removed. A database
/// error answers `0`: a recorded deploy must not fail because its retention did.
async fn prune(pool: &sqlx::PgPool, owner_group: &str, bundle: &str, target: &str) -> u64 {
    sqlx::query(
        "DELETE FROM deployment \
         WHERE owner_group = $1 AND bundle = $2 AND target = $3 \
           AND created_at < now() - make_interval(days => $4) \
           AND id NOT IN ( \
               SELECT id FROM deployment \
               WHERE owner_group = $1 AND bundle = $2 AND target = $3 \
               ORDER BY created_at DESC, id DESC LIMIT $5 \
           )",
    )
    .bind(owner_group)
    .bind(bundle)
    .bind(target)
    .bind(KEEP_DAYS)
    .bind(KEEP_PER_TARGET)
    .execute(pool)
    .await
    .map(|done| done.rows_affected())
    .unwrap_or(0)
}

/// `GET /api/deployments` — the caller's rows, newest first, at most 50.
#[utoipa::path(
    get,
    operation_id = "deployments_list",
    path = "/api/deployments",
    tag = "public",
    description = "The deployment records the caller may see: an administrator sees every row, anybody else the rows of their own groups. Newest first, at most 50, a bare JSON array. `?bundle=` and `?target=` narrow the list to one bundle and one stage.",
    params(
        ("bundle" = Option<String>, Query, description = "Only rows of this bundle"),
        ("target" = Option<String>, Query, description = "Only rows of this stage"),
    ),
    responses(
        (status = 200, description = "The rows, newest first", body = Vec<Deployment>),
        (status = 401, description = "The caller has no identity", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn list(
    State(s): State<Arc<AppState>>,
    Query(q): Query<ListParams>,
    caller: Option<Extension<Principal>>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let Some(caller) = caller.as_ref().map(|c| &c.0) else {
        return error(StatusCode::UNAUTHORIZED, SIGN_IN);
    };
    let sql = format!(
        "SELECT {ROW_COLUMNS} FROM deployment \
         WHERE ($1 = TRUE OR owner_group = ANY($2)) \
           AND ($3::TEXT IS NULL OR bundle = $3) \
           AND ($4::TEXT IS NULL OR target = $4) \
         ORDER BY created_at DESC LIMIT {LIST_LIMIT}"
    );
    match sqlx::query_as::<_, Deployment>(sqlx::AssertSqlSafe(sql))
        .bind(caller.identity.is_admin)
        .bind(&caller.identity.groups)
        .bind(q.bundle.as_deref())
        .bind(q.target.as_deref())
        .fetch_all(pool)
        .await
    {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => store_failed(e),
    }
}

/// One row the caller may see, by id: an administrator sees every row, anybody else only a row
/// of their own groups. A missing id and somebody else's row are both `Ok(None)`, so no route
/// says which ids exist.
async fn visible(
    pool: &sqlx::PgPool,
    id: i64,
    who: &Identity,
) -> Result<Option<Deployment>, sqlx::Error> {
    let sql = format!(
        "SELECT {ROW_COLUMNS} FROM deployment \
         WHERE id = $1 AND ($2 = TRUE OR owner_group = ANY($3))"
    );
    sqlx::query_as::<_, Deployment>(sqlx::AssertSqlSafe(sql))
        .bind(id)
        .bind(who.is_admin)
        .bind(&who.groups)
        .fetch_optional(pool)
        .await
}

/// The one INSERT behind `record` and `rollback`: a row with the caller as `actor`.
async fn store(
    pool: &sqlx::PgPool,
    new: &NewDeployment,
    who: &Identity,
) -> Result<Deployment, sqlx::Error> {
    let pipelines = serde_json::to_value(&new.pipelines)
        .unwrap_or_else(|_| serde_json::Value::Array(Vec::new()));
    let sql = format!(
        "INSERT INTO deployment \
         (bundle, owner_group, target, revision, repository, status, pipelines, resources, files, actor, actor_name, previous_deployment_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) RETURNING {ROW_COLUMNS}"
    );
    sqlx::query_as::<_, Deployment>(sqlx::AssertSqlSafe(sql))
        .bind(&new.bundle)
        .bind(&new.owner_group)
        .bind(&new.target)
        .bind(&new.revision)
        .bind(&new.repository)
        .bind(&new.status)
        .bind(&pipelines)
        .bind(
            serde_json::to_value(&new.resources)
                .unwrap_or_else(|_| serde_json::Value::Array(Vec::new())),
        )
        .bind(
            serde_json::to_value(&new.files)
                .unwrap_or_else(|_| serde_json::Value::Array(Vec::new())),
        )
        .bind(&who.sub)
        .bind(&who.display_name)
        .bind(new.previous_deployment_id)
        .fetch_one(pool)
        .await
}

/// `GET /api/deployments/{id}` — one row, or 404 when it is not one the caller may see.
#[utoipa::path(
    get,
    operation_id = "deployments_get",
    path = "/api/deployments/{id}",
    tag = "public",
    description = "One deployment record by id: an administrator sees every row, anybody else only a row of their own groups. A row that does not exist and a row of somebody else's group answer the same 404, so the route never says which ids exist.",
    params(("id" = i64, Path, description = "The deployment's id")),
    responses(
        (status = 200, description = "The row", body = Deployment),
        (status = 401, description = "The caller has no identity", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such row among the ones the caller may see", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn detail(
    State(s): State<Arc<AppState>>,
    Path(id): Path<i64>,
    caller: Option<Extension<Principal>>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let Some(caller) = caller.as_ref().map(|c| &c.0) else {
        return error(StatusCode::UNAUTHORIZED, SIGN_IN);
    };
    match visible(pool, id, &caller.identity).await {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            &format!("no deployment {id} among the ones you may see"),
        ),
        Err(e) => store_failed(e),
    }
}

/// `POST /api/deployments` — `201` with the stored row.
#[utoipa::path(
    post,
    operation_id = "deployments_record",
    path = "/api/deployments",
    tag = "public",
    description = "Record one bundle deploy: the bundle, the owner group of its pipelines, the stage it went to, the commit and repository, whether it applied or failed, and the pipelines it touched. Only an administrator or a member of the owner group may record one; the caller is stored as `actor`.",
    request_body = NewDeployment,
    responses(
        (status = 201, description = "The stored row", body = Deployment),
        (status = 400, description = "A name is empty, too long or carries a `/`, the status or an action is not one of the known values, or there are more than 200 pipelines", body = crate::openapi::ErrorResponse),
        (status = 401, description = "The caller has no identity", body = crate::openapi::ErrorResponse),
        (status = 403, description = "Only a member of the pipeline's owner group may record a deployment", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn record(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    ApiJson(new): ApiJson<NewDeployment>,
) -> Response {
    if let Err(message) = validate(&new) {
        return error(StatusCode::BAD_REQUEST, &message);
    }
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let Some(caller) = caller.as_ref().map(|c| &c.0) else {
        return error(StatusCode::UNAUTHORIZED, SIGN_IN);
    };
    if !may_record(&caller.identity, &new.owner_group) {
        return error(
            StatusCode::FORBIDDEN,
            "only a member of the pipeline's owner group may record a deployment",
        );
    }
    match store(pool, &new, &caller.identity).await {
        Ok(row) => {
            let pruned = prune(pool, &row.owner_group, &row.bundle, &row.target).await;
            tracing::info!(
                target: "audit",
                sub = %caller.identity.sub,
                id = row.id,
                bundle = %row.bundle,
                target = %row.target,
                pruned,
                "deployment recorded",
            );
            (StatusCode::CREATED, Json(row)).into_response()
        }
        Err(e) => store_failed(e),
    }
}

/// Whether a refusal from replaying one stored document is the platform's rule saying no (the
/// rollback answers one `409` with that sentence) rather than an upstream failure that keeps its
/// own status. A `400` (the document no longer fits a pipeline) and a `429` (the create window is
/// full) stay what they are: the caller sees why, not a conflict.
fn is_rule_refusal(fail: &Fail) -> bool {
    matches!(
        fail.status(),
        StatusCode::FORBIDDEN | StatusCode::NOT_FOUND | StatusCode::CONFLICT
    )
}

/// `POST /api/deployments/{id}/rollback` — replay a recorded deployment, `201` with the new row.
#[utoipa::path(
    post,
    operation_id = "deployments_rollback",
    path = "/api/deployments/{id}/rollback",
    tag = "public",
    description = "Roll back to what a recorded deployment applied: the stored rendered Pipeline documents of a row of the caller's groups are replayed as the caller — each missing one is created and each existing one updated — and the replay is recorded as a new deployment row whose `previousDeploymentId` points at the source. It needs no Git: it applies exactly what ran. Only a row with status `applied` and at least one stored resource can be replayed. A document the rules refuse stops the replay: the row is still recorded, as `failed`, and the answer is 409 with the sentence (400 when a stored document no longer fits a pipeline, 502/503 when the cluster did not answer).",
    params(("id" = i64, Path, description = "The deployment's id")),
    responses(
        (status = 201, description = "The new row recording the replay", body = Deployment),
        (status = 400, description = "A stored document no longer fits a pipeline", body = crate::openapi::ErrorResponse),
        (status = 401, description = "The caller has no identity", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such row among the ones the caller may see", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The row is not `applied`, it recorded no rendered resources, or a document was refused", body = crate::openapi::ErrorResponse),
        (status = 502, description = "The cluster or the notebook store did not answer", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured, or there is no cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn rollback(
    State(s): State<Arc<AppState>>,
    Path(id): Path<i64>,
    caller: Option<Extension<Principal>>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let Some(caller) = caller.as_ref().map(|c| &c.0) else {
        return error(StatusCode::UNAUTHORIZED, SIGN_IN);
    };
    let row = match visible(pool, id, &caller.identity).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return error(
                StatusCode::NOT_FOUND,
                &format!("no deployment {id} among the ones you may see"),
            );
        }
        Err(e) => return store_failed(e),
    };
    if row.status != "applied" {
        return error(
            StatusCode::CONFLICT,
            &format!(
                "deployment {id} is {}: there is nothing to roll back to",
                row.status
            ),
        );
    }
    // The rendered documents this deploy applied; `validate` capped them at 200 when it was
    // recorded.
    let documents: Vec<serde_json::Value> =
        serde_json::from_value(row.resources.clone()).unwrap_or_default();
    if documents.is_empty() {
        return error(
            StatusCode::CONFLICT,
            &format!(
                "deployment {id} recorded no rendered resources: deploy it again and roll that back"
            ),
        );
    }

    let resources = crate::pipelines::resources(&s);
    let mut touched: Vec<DeploymentPipeline> = Vec::new();
    let mut refusal: Option<Fail> = None;
    for document in &documents {
        match crate::pipelines::apply_document(
            resources.as_ref(),
            s.db.as_ref(),
            s.notebooks.as_ref(),
            s.airflow.as_ref(),
            &s.pipeline_creates,
            &caller.identity,
            document,
        )
        .await
        {
            Ok(action) => touched.push(DeploymentPipeline {
                namespace: crate::cluster::namespace(),
                name: document["name"].as_str().unwrap_or_default().to_string(),
                action: action.to_string(),
            }),
            Err(fail) => {
                refusal = Some(fail);
                break;
            }
        }
    }

    let replay = NewDeployment {
        bundle: row.bundle.clone(),
        owner_group: row.owner_group.clone(),
        target: row.target.clone(),
        revision: row.revision.clone(),
        repository: row.repository.clone(),
        status: if refusal.is_none() {
            "applied"
        } else {
            "failed"
        }
        .into(),
        pipelines: touched,
        resources: documents,
        files: serde_json::from_value(row.files.clone()).unwrap_or_default(),
        previous_deployment_id: Some(id),
    };
    let stored = match store(pool, &replay, &caller.identity).await {
        Ok(row) => row,
        Err(e) => return store_failed(e),
    };
    let pruned = prune(pool, &stored.owner_group, &stored.bundle, &stored.target).await;
    tracing::info!(
        target: "audit",
        sub = %caller.identity.sub,
        id,
        new_id = stored.id,
        bundle = %stored.bundle,
        target = %stored.target,
        pruned,
        "deployment rolled back",
    );
    match refusal {
        None => (StatusCode::CREATED, Json(stored)).into_response(),
        Some(fail) if is_rule_refusal(&fail) => error(StatusCode::CONFLICT, fail.message()),
        Some(fail) => fail.into_response(),
    }
}

/// The deployment routes, merged into the API router.
pub(crate) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/deployments", get(list).post(record))
        .route("/deployments/{id}", get(detail))
        .route("/deployments/{id}/rollback", post(rollback))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deployment(status: &str) -> NewDeployment {
        NewDeployment {
            bundle: "sales".into(),
            owner_group: "analytics".into(),
            target: "prod".into(),
            revision: None,
            repository: None,
            status: status.into(),
            pipelines: Vec::new(),
            resources: Vec::new(),
            files: Vec::new(),
            previous_deployment_id: None,
        }
    }

    fn identity(sub: &str, groups: &[&str], is_admin: bool) -> Identity {
        Identity {
            sub: sub.into(),
            display_name: sub.into(),
            email: None,
            name: None,
            groups: groups.iter().map(|g| (*g).to_string()).collect(),
            roles: Vec::new(),
            groups_overage: false,
            is_admin,
        }
    }

    fn principal(sub: &str, groups: &[&str]) -> Principal {
        Principal {
            identity: identity(sub, groups, false),
            via: "session",
            access_token: None,
        }
    }

    #[test]
    fn may_record_admits_an_administrator_and_a_member_and_refuses_a_non_member() {
        assert!(
            may_record(&identity("root", &[], true), "analytics"),
            "an administrator may record any group's deployment"
        );
        assert!(
            may_record(&identity("ada", &["shop", "analytics"], false), "analytics"),
            "a member of the owner group may record"
        );
        assert!(
            !may_record(&identity("bob", &["shop"], false), "analytics"),
            "somebody else's group may not"
        );
    }

    #[test]
    fn validate_refuses_a_status_outside_applied_and_failed() {
        assert!(validate(&deployment("applied")).is_ok());
        assert!(validate(&deployment("failed")).is_ok());
        let err = validate(&deployment("deployed")).expect_err("unknown status");
        assert!(err.contains("applied") && err.contains("failed"), "{err}");
    }

    #[test]
    fn validate_refuses_a_bundle_name_with_a_slash() {
        let mut new = deployment("applied");
        new.bundle = "shop/sales".into();
        assert!(validate(&new).is_err(), "a slash is not a bundle name");
        new.bundle = "sales".into();
        assert!(validate(&new).is_ok());
    }

    #[test]
    fn validate_refuses_201_pipelines_and_accepts_200() {
        let pipeline = |i: usize| DeploymentPipeline {
            namespace: "shop".into(),
            name: format!("pipeline_{i}"),
            action: "updated".into(),
        };
        let mut new = deployment("applied");
        new.pipelines = (0..MAX_PIPELINES).map(pipeline).collect();
        assert!(validate(&new).is_ok(), "exactly at the limit");
        new.pipelines = (0..=MAX_PIPELINES).map(pipeline).collect();
        let err = validate(&new).expect_err("201 pipelines");
        assert!(err.contains("200"), "{err}");
    }

    #[test]
    fn resources_are_stored_and_limited() {
        let mut new = deployment("applied");
        new.resources = vec![serde_json::json!({"kind": "Pipeline", "metadata": {"name": "p"}})];
        assert!(validate(&new).is_ok(), "one rendered Pipeline document");
        new.resources = (0..=MAX_RESOURCES)
            .map(|i| serde_json::json!({"kind": "Pipeline", "metadata": {"name": format!("p{i}")}}))
            .collect();
        let err = validate(&new).expect_err("201 resources");
        assert!(err.contains("200"), "{err}");
    }

    #[test]
    fn files_are_stored_and_limited() {
        let mut new = deployment("applied");
        let digest = "a".repeat(64);
        new.files = vec![
            DeploymentFile {
                path: "sql/load.sql".into(),
                sha256: digest.clone(),
            },
            DeploymentFile {
                path: "resources/load.yaml".into(),
                sha256: digest,
            },
        ];
        assert!(validate(&new).is_ok(), "two files of the bundle");
        let stored = serde_json::to_value(&new.files).expect("serialises");
        assert_eq!(stored[0]["path"], "sql/load.sql");
        assert_eq!(stored[1]["sha256"].as_str().map(str::len), Some(64));
        new.files = (0..=MAX_FILE_DIGESTS)
            .map(|i| DeploymentFile {
                path: format!("sql/{i}.sql"),
                sha256: "a".repeat(64),
            })
            .collect();
        let err = validate(&new).expect_err("201 files");
        assert!(err.contains("200"), "{err}");
        new.files = vec![DeploymentFile {
            path: "sql/load.sql".into(),
            sha256: "abc".into(),
        }];
        assert!(validate(&new).is_err(), "a sha256 of `abc` is not a digest");
        new.files = vec![DeploymentFile {
            path: "sql/load.sql".into(),
            sha256: "a".repeat(64),
        }];
        assert!(
            validate(&new).is_ok(),
            "the digest is back to 64 hex characters"
        );
    }

    #[tokio::test]
    async fn list_answers_503_before_401_without_a_state_store() {
        let response = list(
            State(crate::router_tests::test_state_with_db(None)),
            Query(ListParams::default()),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn detail_answers_503_before_401_without_a_state_store() {
        let response = detail(
            State(crate::router_tests::test_state_with_db(None)),
            Path(1),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn record_answers_503_before_401_without_a_state_store() {
        let response = record(
            State(crate::router_tests::test_state_with_db(None)),
            None,
            ApiJson(deployment("applied")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn rollback_answers_503_before_401_without_a_state_store() {
        let response = rollback(
            State(crate::router_tests::test_state_with_db(None)),
            Path(1),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// One row straight into the store: the route needs a caller, and the test needs a
    /// timestamp it chose, so the order of two rows is not up to the clock.
    async fn insert(pool: &sqlx::PgPool, owner: &str, bundle: &str, target: &str, seconds: f64) {
        sqlx::query(
            "INSERT INTO deployment \
             (bundle, owner_group, target, status, pipelines, actor, actor_name, created_at) \
             VALUES ($1, $2, $3, 'applied', '[]'::jsonb, 'test', 'test', now() + make_interval(secs => $4))",
        )
        .bind(bundle)
        .bind(owner)
        .bind(target)
        .bind(seconds)
        .execute(pool)
        .await
        .expect("insert a deployment");
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn prune_keeps_the_newest_fifty_and_never_deletes_a_row_younger_than_ten_days() {
        let pool = crate::db::test_support::migrated_pool().await;
        let old = -20.0 * 86_400.0;
        for _ in 0..55 {
            insert(&pool, "analytics", "sales", "prod", old).await;
        }
        for _ in 0..3 {
            insert(&pool, "analytics", "sales", "prod", -60.0).await;
        }
        insert(&pool, "finance", "sales", "prod", old).await;

        assert_eq!(
            prune(&pool, "analytics", "sales", "prod").await,
            8,
            "58 rows, the newest 50 stay"
        );

        let left: i64 =
            sqlx::query_scalar("SELECT count(*) FROM deployment WHERE owner_group = 'analytics'")
                .fetch_one(&pool)
                .await
                .expect("count the analytics rows");
        assert_eq!(left, 50);
        let young: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM deployment \
             WHERE owner_group = 'analytics' AND created_at > now() - INTERVAL '10 days'",
        )
        .fetch_one(&pool)
        .await
        .expect("count the young rows");
        assert_eq!(young, 3, "a row younger than ten days is never deleted");
        let other: i64 =
            sqlx::query_scalar("SELECT count(*) FROM deployment WHERE owner_group = 'finance'")
                .fetch_one(&pool)
                .await
                .expect("count the finance rows");
        assert_eq!(
            other, 1,
            "another group's rows of the same bundle and target are untouched"
        );
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn rollback_answers_404_for_another_groups_row() {
        let pool = crate::db::test_support::migrated_pool().await;
        let state = crate::router_tests::test_state_with_db(Some(pool.clone()));
        insert(&pool, "analytics", "sales", "prod", 0.0).await;
        let id: i64 =
            sqlx::query_scalar("SELECT id FROM deployment WHERE owner_group = 'analytics'")
                .fetch_one(&pool)
                .await
                .expect("the analytics row");

        let response = rollback(
            State(state),
            Path(id),
            Some(Extension(principal("bob", &["shop"]))),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "a row of another group is not one the caller may see"
        );
    }

    /// One row with a chosen status and rendered resources; answers its id.
    async fn insert_row(
        pool: &sqlx::PgPool,
        owner: &str,
        status: &str,
        resources: serde_json::Value,
    ) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO deployment \
             (bundle, owner_group, target, status, pipelines, resources, actor, actor_name) \
             VALUES ('sales', $1, 'prod', $2, '[]'::jsonb, $3, 'test', 'test') RETURNING id",
        )
        .bind(owner)
        .bind(status)
        .bind(resources)
        .fetch_one(pool)
        .await
        .expect("insert a deployment")
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn rollback_refuses_a_failed_deployment() {
        let pool = crate::db::test_support::migrated_pool().await;
        let state = crate::router_tests::test_state_with_db(Some(pool.clone()));
        let id = insert_row(
            &pool,
            "analytics",
            "failed",
            serde_json::json!([{"name": "daily", "ownerGroup": "analytics", "sql": "select 1"}]),
        )
        .await;

        let response = rollback(
            State(state),
            Path(id),
            Some(Extension(principal("ada", &["analytics"]))),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the refusal is a body");
        let answer: serde_json::Value = serde_json::from_slice(&body).expect("one error");
        assert_eq!(
            answer["error"],
            format!("deployment {id} is failed: there is nothing to roll back to")
        );
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn rollback_refuses_a_row_without_resources() {
        let pool = crate::db::test_support::migrated_pool().await;
        let state = crate::router_tests::test_state_with_db(Some(pool.clone()));
        let id = insert_row(&pool, "analytics", "applied", serde_json::json!([])).await;

        let response = rollback(
            State(state),
            Path(id),
            Some(Extension(principal("ada", &["analytics"]))),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the refusal is a body");
        let answer: serde_json::Value = serde_json::from_slice(&body).expect("one error");
        assert_eq!(
            answer["error"],
            format!(
                "deployment {id} recorded no rendered resources: deploy it again and roll that back"
            )
        );
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn record_stores_the_callers_name_next_to_the_subject() {
        let pool = crate::db::test_support::migrated_pool().await;
        let state = crate::router_tests::test_state_with_db(Some(pool));
        let caller = Principal {
            identity: Identity {
                display_name: "Ada Lovelace".into(),
                ..identity("ada", &["analytics"], false)
            },
            via: "session",
            access_token: None,
        };
        let response = record(
            State(state),
            Some(Extension(caller)),
            ApiJson(deployment("applied")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the row is a body");
        let row: serde_json::Value = serde_json::from_slice(&body).expect("one row");
        assert_eq!(row["actor"], "ada");
        assert_eq!(row["actorName"], "Ada Lovelace");
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn list_answers_only_the_callers_groups_rows_newest_first() {
        let pool = crate::db::test_support::migrated_pool().await;
        let state = crate::router_tests::test_state_with_db(Some(pool.clone()));
        insert(&pool, "analytics", "sales", "prod", 0.0).await;
        insert(&pool, "analytics", "sales", "staging", 2.0).await;
        insert(&pool, "finance", "books", "prod", 1.0).await;

        let response = list(
            State(state),
            Query(ListParams::default()),
            Some(Extension(principal("ada", &["analytics"]))),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the list is a body");
        let rows: Vec<serde_json::Value> =
            serde_json::from_slice(&body).expect("a bare JSON array");
        let targets: Vec<&str> = rows
            .iter()
            .map(|row| row["target"].as_str().expect("a target"))
            .collect();
        assert_eq!(
            targets,
            vec!["staging", "prod"],
            "only the caller's group, newest first"
        );
        assert!(
            rows.iter().all(|row| row["ownerGroup"] == "analytics"),
            "{rows:#?}"
        );

        let filtered = list(
            State(crate::router_tests::test_state_with_db(Some(pool))),
            Query(ListParams {
                bundle: Some("sales".into()),
                target: Some("prod".into()),
            }),
            Some(Extension(principal("ada", &["analytics"]))),
        )
        .await;
        assert_eq!(filtered.status(), StatusCode::OK);
        let body = axum::body::to_bytes(filtered.into_body(), usize::MAX)
            .await
            .expect("the list is a body");
        let rows: Vec<serde_json::Value> =
            serde_json::from_slice(&body).expect("a bare JSON array");
        assert_eq!(rows.len(), 1, "{rows:#?}");
        assert_eq!(rows[0]["target"], "prod");
    }

    #[tokio::test]
    #[ignore = "needs NOVA_TEST_DATABASE_URL"]
    async fn detail_answers_ones_own_row_and_404_for_another_groups_row() {
        let pool = crate::db::test_support::migrated_pool().await;
        let state = crate::router_tests::test_state_with_db(Some(pool.clone()));
        insert(&pool, "analytics", "sales", "prod", 0.0).await;
        insert(&pool, "finance", "sales", "prod", 1.0).await;
        let own: i64 = sqlx::query_scalar("SELECT id FROM deployment WHERE owner_group = $1")
            .bind("analytics")
            .fetch_one(&pool)
            .await
            .expect("the analytics row");
        let other: i64 = sqlx::query_scalar("SELECT id FROM deployment WHERE owner_group = $1")
            .bind("finance")
            .fetch_one(&pool)
            .await
            .expect("the finance row");

        let ada = || principal("ada", &["analytics"]);
        let response = detail(
            State(crate::router_tests::test_state_with_db(Some(pool.clone()))),
            Path(own),
            Some(Extension(ada())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("one row is a body");
        let row: serde_json::Value = serde_json::from_slice(&body).expect("one row");
        assert_eq!(row["bundle"], "sales");

        let response = detail(State(state), Path(other), Some(Extension(ada()))).await;
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "another group's row is not one the caller may see"
        );

        let admin = Principal {
            identity: Identity {
                is_admin: true,
                ..identity("root", &[], false)
            },
            via: "session",
            access_token: None,
        };
        for id in [own, other] {
            let response = detail(
                State(crate::router_tests::test_state_with_db(Some(pool.clone()))),
                Path(id),
                Some(Extension(admin.clone())),
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::OK,
                "an administrator answers both rows (id {id})"
            );
        }
    }
}
