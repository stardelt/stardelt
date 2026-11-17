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
//!
//! Without a state store (`NOVA_DATABASE_URL` unset) both routes answer
//! `503 {"error":"this needs Nova's database, …"}`, and a caller whose identity is missing
//! answers `401`. Every write writes one line on the `audit` target with the caller, the
//! row's id, the bundle and the target — never the rendered resources.

use std::sync::Arc;

use axum::{
    Extension, Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;
use crate::api_json::ApiJson;
use crate::auth::{Identity, Principal};
use crate::saved_queries::{error, store_failed, unavailable};

/// Most characters a bundle, an owner group or a target may carry (the table's CHECK
/// constraints allow the same).
const MAX_NAME_CHARS: usize = 64;
/// Most pipelines one deployment may list.
const MAX_PIPELINES: usize = 200;
/// Rows one list answers, newest first.
const LIST_LIMIT: usize = 50;
/// The status a deployment row may carry.
const STATUSES: &[&str] = &["applied", "failed"];
/// The action a pipeline row may carry.
const ACTIONS: &[&str] = &["created", "updated", "deleted", "unchanged"];
/// The answer when a caller has no identity.
const SIGN_IN: &str = "sign in to record or read deployments";

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
    /// The `sub` of the person who recorded the deploy.
    pub actor: String,
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
    pipelines, actor, \
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
    let Some(caller) = caller.as_ref().map(|c| &c.0) else {
        return error(StatusCode::UNAUTHORIZED, SIGN_IN);
    };
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
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
    let Some(caller) = caller.as_ref().map(|c| &c.0) else {
        return error(StatusCode::UNAUTHORIZED, SIGN_IN);
    };
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    if !may_record(&caller.identity, &new.owner_group) {
        return error(
            StatusCode::FORBIDDEN,
            "only a member of the pipeline's owner group may record a deployment",
        );
    }
    let pipelines = serde_json::to_value(&new.pipelines)
        .unwrap_or_else(|_| serde_json::Value::Array(Vec::new()));
    let sql = format!(
        "INSERT INTO deployment \
         (bundle, owner_group, target, revision, repository, status, pipelines, actor) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING {ROW_COLUMNS}"
    );
    match sqlx::query_as::<_, Deployment>(sqlx::AssertSqlSafe(sql))
        .bind(&new.bundle)
        .bind(&new.owner_group)
        .bind(&new.target)
        .bind(&new.revision)
        .bind(&new.repository)
        .bind(&new.status)
        .bind(&pipelines)
        .bind(&caller.identity.sub)
        .fetch_one(pool)
        .await
    {
        Ok(row) => {
            tracing::info!(
                target: "audit",
                sub = %caller.identity.sub,
                id = row.id,
                bundle = %row.bundle,
                target = %row.target,
                "deployment recorded",
            );
            (StatusCode::CREATED, Json(row)).into_response()
        }
        Err(e) => store_failed(e),
    }
}

/// The deployment routes, merged into the API router.
pub(crate) fn router() -> Router<Arc<AppState>> {
    Router::new().route("/deployments", get(list).post(record))
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

    /// One row straight into the store: the route needs a caller, and the test needs a
    /// timestamp it chose, so the order of two rows is not up to the clock.
    async fn insert(pool: &sqlx::PgPool, owner: &str, bundle: &str, target: &str, seconds: f64) {
        sqlx::query(
            "INSERT INTO deployment \
             (bundle, owner_group, target, status, pipelines, actor, created_at) \
             VALUES ($1, $2, $3, 'applied', '[]'::jsonb, 'test', now() + make_interval(secs => $4))",
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
}
