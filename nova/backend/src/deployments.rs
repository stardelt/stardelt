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
use kube::Api;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

use crate::AppState;
use crate::api_json::ApiJson;
use crate::auth::{Identity, Principal, deploy_stage};
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
    /// The rendered documents this deploy applied — `Pipeline` and `StreamApp`; empty for a failed deploy.
    #[serde(default)]
    pub resources: Vec<serde_json::Value>,
    /// The files of the bundle this deploy read, at most 200; empty when the caller sent none.
    #[serde(default)]
    pub files: Vec<DeploymentFile>,
    /// The deployment a rollback replayed, or absent. It is display metadata: the page links to it, and a row the
    /// caller may not see answers 404 there.
    #[serde(default)]
    pub previous_deployment_id: Option<i64>,
    /// The `planId` of the plan this apply followed (`POST /api/deployments/plan`), or absent. When
    /// given, the row must describe the same rendered set the plan named; otherwise 409.
    #[serde(default)]
    pub plan_id: Option<String>,
}

/// One file of the bundle the deploy read: its path inside the bundle and the sha256 of its bytes.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeploymentFile {
    /// The path relative to the bundle directory, with `/` separators.
    pub path: String,
    /// The file's content, lowercase hex sha256 (64 characters).
    pub sha256: String,
}

/// `POST /api/deployments/plan` body: one bundle's rendered Pipeline documents and file digests,
/// for one target. The same `resources` list the record call carries, so a `planId` names both.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BundlePlanRequest {
    pub bundle: String,
    pub owner_group: String,
    pub target: String,
    /// The commit the bundle was read at; display metadata, not part of the plan's identity.
    /// Nothing reads it yet: the record carries the revision, and a later slice shows it beside
    /// the plan.
    #[serde(default)]
    #[allow(dead_code)]
    pub revision: Option<String>,
    pub resources: Vec<serde_json::Value>,
    #[serde(default)]
    pub files: Vec<DeploymentFile>,
    /// Whether the apply that follows removes what this bundle and target own and the set no longer
    /// names (`stardelt bundle deploy --prune`). The plan names that set as `delete` changes; the
    /// plan itself removes nothing.
    #[serde(default)]
    pub prune: bool,
}

/// `POST /api/deployments/plan` answer: what applying that set would do, and its identity.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BundlePlan {
    pub ok: bool,
    pub errors: Vec<PlanIssue>,
    pub warnings: Vec<PlanIssue>,
    pub changes: Vec<PlanChange>,
    pub files: Vec<PlanFile>,
    /// The identity of the set this plan describes; `POST /api/deployments` answers 409 when a
    /// `planId` it is given does not name the set it was sent.
    pub plan_id: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PlanIssue {
    /// Where the problem is: `resources[2] (gold)` or `resources` (the set).
    pub path: String,
    /// One sentence, the same sentence the write route would refuse with.
    pub message: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanChange {
    /// `Pipeline` or `StreamApp`.
    pub kind: String,
    pub name: String,
    /// `create`, `update`, `unchanged` or `delete` (from `ValidatePipelineResponse::action`, with
    /// `create` when `action` is absent). A `delete` is a pipeline this bundle and target own that
    /// the set omits, present only when `prune` is true, and it carries no `diff`.
    pub action: String,
    /// What an `update` would change, the sentences `pipelines_validate` answers.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub diff: Vec<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanFile {
    pub path: String,
    pub sha256: String,
    /// `true` when the newest applied deployment of this bundle and target does not name this path
    /// with this digest; every file when no deployment was recorded yet.
    pub new: bool,
}

/// One pipeline of a deploy: what changed in it.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeploymentPipeline {
    pub namespace: String,
    pub name: String,
    /// `created`, `updated`, `deleted` or `unchanged`.
    pub action: String,
}

/// The row the document shows for a `Deployment` answer (#761): one applied deploy of the
/// `shop` bundle, with its pipeline, one rendered resource and one file digest. Every 2xx
/// answer of the four routes uses it, so the document shows the same row everywhere. Written
/// out by hand on purpose: [`crate::openapi::tests`] fails when a key here is no property of
/// `Deployment`, so a renamed or removed field cannot stay in the example.
fn row_example() -> serde_json::Value {
    serde_json::json!({
        "id": 41,
        "bundle": "shop",
        "ownerGroup": "shop",
        "target": "production",
        "status": "applied",
        "revision": "4f3c2a1",
        "repository": "https://github.com/acme/shop",
        "pipelines": [
            {"namespace": "stardelt", "name": "shop-marts", "action": "updated"}
        ],
        "resources": [{
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Pipeline",
            "metadata": {"name": "shop-marts"},
            "spec": {"tasks": [{"name": "run", "kind": "sql", "sql": "select 1"}]}
        }],
        "files": [{
            "path": "shop-marts.yaml",
            "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        }],
        "previousDeploymentId": null,
        "actor": "sub-ada",
        "actorName": "ada",
        "createdAt": "2026-11-20T09:12:04Z"
    })
}

/// [`row_example`] as the rollback route answers it: the new row, pointing at the replayed one.
fn rollback_row_example() -> serde_json::Value {
    let mut row = row_example();
    row["previousDeploymentId"] = serde_json::json!(40);
    row
}

/// The example `POST /api/deployments` takes: a creatable deploy, so without rendered
/// `resources`. It must deserialize as [`NewDeployment`]; a test checks that.
fn new_deployment_example() -> serde_json::Value {
    serde_json::json!({
        "bundle": "shop",
        "ownerGroup": "shop",
        "target": "production",
        "revision": "4f3c2a1",
        "repository": "https://github.com/acme/shop",
        "status": "applied",
        "pipelines": [
            {"namespace": "stardelt", "name": "shop-marts", "action": "updated"}
        ],
        "files": [{
            "path": "shop-marts.yaml",
            "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        }]
    })
}

/// The example `POST /api/deployments/plan` takes: one rendered Pipeline document and one file
/// digest. It must deserialize as [`BundlePlanRequest`]; a test checks that.
fn bundle_plan_request_example() -> serde_json::Value {
    serde_json::json!({
        "bundle": "shop-etl",
        "ownerGroup": "analytics",
        "target": "production",
        "revision": "4f3c2a1",
        "prune": true,
        "resources": [{
            "apiVersion": "stardelt.io/v1alpha1",
            "kind": "Pipeline",
            "metadata": {"name": "shop-marts"},
            "spec": {"ownerGroup": "analytics", "tasks": [{"name": "run", "kind": "sql", "sql": "select 1"}]}
        }],
        "files": [{
            "path": "shop-marts.yaml",
            "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        }]
    })
}

/// The example [`BundlePlan`] the plan route answers: one update with its diff and one new file.
fn bundle_plan_example() -> serde_json::Value {
    serde_json::json!({
        "ok": true,
        "errors": [],
        "warnings": [],
        "changes": [{
            "kind": "Pipeline",
            "name": "shop-marts",
            "action": "update",
            "diff": ["the schedule changed from 0 3 * * * to 0 4 * * *"]
        }],
        "files": [{
            "path": "shop-marts.yaml",
            "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "new": true
        }],
        "planId": "a3f1c0f4b9a4ce7a7b2a0f2e8a3a3fd0c9de5c4f5e1c2b3a4958687788a9b0c1"
    })
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

/// The sentence for a token whose client names another stage than the one asked for. Shared with
/// `record`, `plan` and `rollback`, which answer it before they read the stage object, so a stage
/// refusal never depends on the cluster (#1038).
fn stage_sentence(stage: &str, verb: &str) -> String {
    format!(
        "this token belongs to deploy stage `{stage}`; it may only {verb} a deployment to that stage"
    )
}

/// Why this caller may not record or plan a deployment to `target`, or `None` when it may.
/// A deploy principal (#57) is confined to the stage its client names and to its own group;
/// everyone else keeps the administrator-or-member rule. `verb` is `record` or `plan`.
pub fn deploy_refusal(
    identity: &Identity,
    owner_group: &str,
    target: &str,
    client_id: Option<&str>,
    verb: &str,
) -> Option<String> {
    let member = identity.groups.iter().any(|g| g == owner_group);
    match deploy_stage(client_id) {
        Some(stage) if stage != target => Some(stage_sentence(stage, verb)),
        Some(_) if !member => Some(format!(
            "a deploy principal may only {verb} a deployment of its own group"
        )),
        Some(_) => None,
        None if may_record(identity, owner_group) => None,
        None => Some(format!(
            "only a member of the pipeline's owner group may {verb} a deployment"
        )),
    }
}

/// `spec.group` of the `DeployStage` the token's `azp` names (#1038). `Ok(None)` when the object
/// has no group; `Err` is the sentence for a 502 when the cluster cannot answer.
pub(crate) async fn stage_owner_group(
    state: &AppState,
    stage: &str,
) -> Result<Option<String>, String> {
    let Some(client) = state.kube.clone() else {
        return Err("the cluster is not reachable: no deploy stage could be read".into());
    };
    let api: Api<DynamicObject> = Api::namespaced_with(
        client,
        &crate::cluster::namespace(),
        &ApiResource::from_gvk(&GroupVersionKind::gvk(
            "stardelt.io",
            "v1alpha1",
            "DeployStage",
        )),
    );
    let stage = api.get(stage).await.map_err(|e| match e {
        kube::Error::Api(a) if a.code == 404 => {
            format!("deploy stage `{stage}` is not on the cluster")
        }
        other => format!("the cluster did not answer for deploy stage `{stage}`: {other}"),
    })?;
    Ok(stage_group_of(&stage.data))
}

/// The group one `DeployStage`'s JSON names, if any; the parsing half of `stage_owner_group`.
fn stage_group_of(data: &serde_json::Value) -> Option<String> {
    data.get("spec")
        .and_then(|s| s.get("group"))
        .and_then(|g| g.as_str())
        .map(str::to_string)
        .filter(|g| !g.is_empty())
}

/// The identity a deploy principal acts with: its own groups replaced by the stage's group, so
/// `deploy_refusal` reads "its own group" from the object (#1038). A person's identity is
/// returned unchanged.
async fn deploy_identity(state: &AppState, caller: &Principal) -> Result<Identity, String> {
    let mut identity = caller.identity.clone();
    if let Some(stage) = deploy_stage(caller.client_id.as_deref())
        && let Some(group) = stage_owner_group(state, stage).await?
    {
        identity.groups = vec![group];
    }
    Ok(identity)
}

/// The kind a stored document is replayed as. Only `StreamApp` is a second kind; a document
/// without a `kind` (everything recorded before #940) and an unknown one replay as a Pipeline,
/// so they are refused with the pipeline sentence rather than silently skipped.
fn stored_document_kind(document: &serde_json::Value) -> &'static str {
    if document.get("kind").and_then(serde_json::Value::as_str) == Some("StreamApp") {
        "StreamApp"
    } else {
        "Pipeline"
    }
}

/// The identity of a rendered set: the lowercase hex SHA-256 of the bundle, the target, the
/// rendered documents **in the order given** and the files **sorted by path** — one line each,
/// the form `migrations::plan_id` uses (#101). `plan` answers it and `record` recomputes it, so
/// a `planId` proves the row describes the set that was planned. The revision is metadata and is
/// not part of it.
pub(crate) fn rendered_set_id(
    bundle: &str,
    target: &str,
    resources: &[serde_json::Value],
    files: &[DeploymentFile],
) -> String {
    let mut lines = vec![format!("bundle={bundle}"), format!("target={target}")];
    for document in resources {
        let json = serde_json::to_string(document).expect("a JSON value serializes");
        lines.push(format!("resource\t{json}"));
    }
    let mut files: Vec<&DeploymentFile> = files.iter().collect();
    files.sort_by(|a, b| a.path.cmp(&b.path).then_with(|| a.sha256.cmp(&b.sha256)));
    lines.extend(
        files
            .iter()
            .map(|f| format!("file\t{}\t{}", f.path, f.sha256)),
    );
    crate::migrations::hex_lower(&Sha256::digest(lines.join("\n").as_bytes()))
}

/// `None` when the apply names no plan or names this set's plan; the 409's sentence when the id
/// is for another set.
fn plan_problem(new: &NewDeployment) -> Option<String> {
    let plan_id = new.plan_id.as_ref()?;
    let ours = rendered_set_id(&new.bundle, &new.target, &new.resources, &new.files);
    (plan_id != &ours)
        .then(|| "the plan you applied is not the plan you saw: plan the bundle again".to_string())
}

/// A name the API stores: 1 to 64 characters, no `/`.
fn valid_name(value: &str) -> bool {
    let len = value.chars().count();
    (1..=MAX_NAME_CHARS).contains(&len) && !value.contains('/')
}

/// The rule for one stored name. `what` is `"bundle"`, `"ownerGroup"` or `"target"`; `Err` is the
/// sentence the caller sees. The plan and the record use it, so they cannot drift.
pub(crate) fn name_rule(what: &str, value: &str) -> Result<(), String> {
    if !valid_name(value) {
        return Err(format!(
            "{what} must be 1 to {MAX_NAME_CHARS} characters and must not contain '/'"
        ));
    }
    Ok(())
}

/// The rule for the bundle's file digests. `Err` is the sentence the caller sees. The plan and the
/// record use it, so they cannot drift.
pub(crate) fn file_rules(files: &[DeploymentFile]) -> Result<(), String> {
    for file in files {
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
    Ok(())
}

/// The body rules the table's CHECK constraints enforce, repeated as one sentence so a
/// caller gets a `400` instead of a database error.
pub fn validate(new: &NewDeployment) -> Result<(), String> {
    name_rule("bundle", &new.bundle)?;
    name_rule("ownerGroup", &new.owner_group)?;
    name_rule("target", &new.target)?;
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
    file_rules(&new.files)?;
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
        (status = 200, description = "The rows, newest first", body = Vec<Deployment>, example = json!([row_example()])),
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

/// The one row with the id, whoever owns it: `rollback` reads it before it knows which group the
/// caller acts as (#1038). Every route that answers a row of another group says `404`.
async fn row_by_id(pool: &sqlx::PgPool, id: i64) -> Result<Option<Deployment>, sqlx::Error> {
    let sql = format!("SELECT {ROW_COLUMNS} FROM deployment WHERE id = $1");
    sqlx::query_as::<_, Deployment>(sqlx::AssertSqlSafe(sql))
        .bind(id)
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
        (status = 200, description = "The row", body = Deployment, example = json!(row_example())),
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
    description = "Record one bundle deploy: the bundle, the owner group of its pipelines, the stage it went to, the commit and repository, whether it applied or failed, and the rendered documents it applied (its `Pipeline`s and its `StreamApp`s). Only an administrator or a member of the owner group may record one; the caller is stored as `actor`. A token issued to the Keycloak client `stardelt-deploy-<stage>` (a deploy principal, #57) is refused with 403 unless the target is `<stage>` and the caller is a member of the owner group; an administrator's credential has no such client and is unaffected.",
    request_body(content = NewDeployment, example = json!(new_deployment_example())),
    responses(
        (status = 201, description = "The stored row", body = Deployment, example = json!(row_example())),
        (status = 400, description = "A name is empty, too long or carries a `/`, the status or an action is not one of the known values, or there are more than 200 pipelines", body = crate::openapi::ErrorResponse),
        (status = 401, description = "The caller has no identity", body = crate::openapi::ErrorResponse),
        (status = 403, description = "Only a member of the pipeline's owner group may record a deployment; a deploy principal may only record a deployment to its stage and of its own group", body = crate::openapi::ErrorResponse),
        (status = 409, description = "The planId does not name the rendered set of this row", body = crate::openapi::ErrorResponse),
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
    if let Some(sentence) = plan_problem(&new) {
        return error(StatusCode::CONFLICT, &sentence);
    }
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let Some(caller) = caller.as_ref().map(|c| &c.0) else {
        return error(StatusCode::UNAUTHORIZED, SIGN_IN);
    };
    // The stage the token names needs no cluster to compare: a token of another stage is
    // refused before the stage object is read (#1038).
    if let Some(stage) = deploy_stage(caller.client_id.as_deref())
        && stage != new.target
    {
        return error(StatusCode::FORBIDDEN, &stage_sentence(stage, "record"));
    }
    let identity = match deploy_identity(&s, caller).await {
        Ok(identity) => identity,
        Err(sentence) => return error(StatusCode::BAD_GATEWAY, &sentence),
    };
    if let Some(sentence) = deploy_refusal(
        &identity,
        &new.owner_group,
        &new.target,
        caller.client_id.as_deref(),
        "record",
    ) {
        return error(StatusCode::FORBIDDEN, &sentence);
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

/// `POST /api/deployments/plan` — what the set would do, `200` with the plan, nothing written.
#[utoipa::path(
    post,
    operation_id = "deployments_plan",
    path = "/api/deployments/plan",
    tag = "public",
    description = "Say what applying one bundle's rendered Pipeline documents to one stage would do, writing nothing. Every document goes through the same create rules `pipelines_validate` and the write routes use, so the plan and the apply cannot disagree: `changes` names a `create`, an `update` (with the sentences it would change) or an `unchanged` document, `errors` carries the rule that refuses one, and `warnings` names a pipeline this bundle and stage own that the set does not name (a warning unless `prune` is true, when it is a `delete` change). `files` says which of the bundle's files are new against the newest applied deployment of this bundle and stage. `planId` is the identity of the rendered set: the SHA-256 of the bundle, the stage, the documents in the order given and the files sorted by path. A later `POST /api/deployments` may carry it as `planId`, and answers 409 when the row's set is not the planned one. A `prune: true` request names each owned pipeline the set omits as a `delete` change. The stage modes are not covered yet; a StreamApp document is checked with the rules its write runs, and reads as an update when a StreamApp of that name exists, a create otherwise (a stream write is a PUT, so a stream change carries no diff yet). A token issued to the Keycloak client `stardelt-deploy-<stage>` (a deploy principal, #57) answers 403 unless the target is `<stage>` and the caller is a member of the owner group; an administrator's credential has no such client and is unaffected.",
    request_body(content = BundlePlanRequest, example = json!(bundle_plan_request_example())),
    responses(
        (status = 200, description = "The plan, with its identity", body = BundlePlan, example = json!(bundle_plan_example())),
        (status = 400, description = "A name is empty, too long or carries a `/`, or there are more than 200 resources or 200 file digests", body = crate::openapi::ErrorResponse),
        (status = 401, description = "The caller has no identity", body = crate::openapi::ErrorResponse),
        (status = 403, description = "Only a member of the pipeline's owner group may plan a deployment; a deploy principal may only plan a deployment to its stage and of its own group", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured, or there is no cluster connection", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn plan(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    ApiJson(request): ApiJson<BundlePlanRequest>,
) -> Response {
    if let Err(message) = validate_plan(&request) {
        return error(StatusCode::BAD_REQUEST, &message);
    }
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let Some(caller) = caller.as_ref().map(|c| &c.0) else {
        return error(StatusCode::UNAUTHORIZED, SIGN_IN);
    };
    if let Some(stage) = deploy_stage(caller.client_id.as_deref())
        && stage != request.target
    {
        return error(StatusCode::FORBIDDEN, &stage_sentence(stage, "plan"));
    }
    let identity = match deploy_identity(&s, caller).await {
        Ok(identity) => identity,
        Err(sentence) => return error(StatusCode::BAD_GATEWAY, &sentence),
    };
    if let Some(sentence) = deploy_refusal(
        &identity,
        &request.owner_group,
        &request.target,
        caller.client_id.as_deref(),
        "plan",
    ) {
        return error(StatusCode::FORBIDDEN, &sentence);
    }
    let recorded =
        match last_applied(pool, &request.owner_group, &request.bundle, &request.target).await {
            Ok(row) => row,
            Err(e) => return store_failed(e),
        };
    let files = recorded
        .as_ref()
        .and_then(|row| serde_json::from_value::<Vec<DeploymentFile>>(row.files.clone()).ok());
    let resources = crate::pipelines::resources(&s);
    let streams = match crate::streams::stream_apps(&s).await {
        Ok(streams) => streams,
        Err(fail) => return crate::pipelines::answer::<crate::deployments::BundlePlan>(Err(fail)),
    };
    crate::pipelines::answer(
        crate::pipelines::plan_bundle(
            resources.as_ref(),
            s.db.as_ref(),
            s.notebooks.as_ref(),
            &identity,
            files.as_deref(),
            &streams,
            request,
        )
        .await,
    )
}

/// The newest `applied` row of one bundle, target and owner group: its `files` say what the plan
/// calls new. `None` when none was recorded or none is applied.
async fn last_applied(
    pool: &sqlx::PgPool,
    owner_group: &str,
    bundle: &str,
    target: &str,
) -> Result<Option<Deployment>, sqlx::Error> {
    let sql = format!(
        "SELECT {ROW_COLUMNS} FROM deployment \
         WHERE owner_group = $1 AND bundle = $2 AND target = $3 AND status = 'applied' \
         ORDER BY id DESC LIMIT 1"
    );
    sqlx::query_as::<_, Deployment>(sqlx::AssertSqlSafe(sql))
        .bind(owner_group)
        .bind(bundle)
        .bind(target)
        .fetch_optional(pool)
        .await
}

/// The body rules of a plan: the names and file digests the record enforces, plus its own caps.
pub(crate) fn validate_plan(request: &BundlePlanRequest) -> Result<(), String> {
    name_rule("bundle", &request.bundle)?;
    name_rule("ownerGroup", &request.owner_group)?;
    name_rule("target", &request.target)?;
    if request.resources.len() > MAX_RESOURCES {
        return Err(format!(
            "a plan may carry at most {MAX_RESOURCES} resources"
        ));
    }
    if serde_json::to_vec(&request.resources)
        .unwrap_or_default()
        .len()
        > 1024 * 1024
    {
        return Err("the rendered resources must be under 1 MiB".into());
    }
    if request.files.len() > MAX_FILE_DIGESTS {
        return Err(format!(
            "a plan may carry at most {MAX_FILE_DIGESTS} file digests"
        ));
    }
    file_rules(&request.files)
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
    description = "Roll back to what a recorded deployment applied: the stored rendered documents of a row of the caller's groups are replayed as the caller — each missing one is created and each existing one updated — and the replay is recorded as a new deployment row whose `previousDeploymentId` points at the source. It needs no Git: it applies exactly what ran. Only a row with status `applied` and at least one stored resource can be replayed. A `StreamApp` document is replayed with the rules of `streams_put`: a stream the caller's group owns is created or updated, and a stream of another group or one Nova did not write is refused with that route's sentence. A document the rules refuse stops the replay: the row is still recorded, as `failed`, and the answer is 409 with the sentence (400 when a stored document no longer fits its kind — a pipeline or a stream, 502/503 when the cluster did not answer). A token issued to the Keycloak client `stardelt-deploy-<stage>` (a deploy principal, #57) is refused with 403 unless the row's stage is `<stage>` and the caller is a member of the row's owner group; an administrator's credential has no such client and is unaffected.",
    params(("id" = i64, Path, description = "The deployment's id")),
    responses(
        (status = 201, description = "The new row recording the replay", body = Deployment, example = json!(rollback_row_example())),
        (status = 400, description = "A stored document no longer fits its kind (a pipeline or a stream)", body = crate::openapi::ErrorResponse),
        (status = 401, description = "The caller has no identity", body = crate::openapi::ErrorResponse),
        (status = 403, description = "A deploy principal may not roll back a row of another stage or of another owner group", body = crate::openapi::ErrorResponse),
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
    // The row is read by id first: a token that names another stage is refused before the stage
    // object is read, so that refusal never depends on the cluster (#1038).
    let row = match row_by_id(pool, id).await {
        Ok(Some(row)) => row,
        Ok(None) => {
            return error(
                StatusCode::NOT_FOUND,
                &format!("no deployment {id} among the ones you may see"),
            );
        }
        Err(e) => return store_failed(e),
    };
    if let Some(stage) = deploy_stage(caller.client_id.as_deref())
        && stage != row.target
    {
        return error(StatusCode::FORBIDDEN, &stage_sentence(stage, "roll back"));
    }
    // A deploy principal (#57) acts as a member of its stage's group: `deploy_identity` reads the
    // `DeployStage` for that group, and nothing else about the caller changes.
    let identity = match deploy_identity(&s, caller).await {
        Ok(identity) => identity,
        Err(sentence) => return error(StatusCode::BAD_GATEWAY, &sentence),
    };
    // The rule `visible` applies in SQL, with the identity the caller now acts as.
    if !may_record(&identity, &row.owner_group) {
        return error(
            StatusCode::NOT_FOUND,
            &format!("no deployment {id} among the ones you may see"),
        );
    }
    if row.status != "applied" {
        return error(
            StatusCode::CONFLICT,
            &format!(
                "deployment {id} is {}: there is nothing to roll back to",
                row.status
            ),
        );
    }
    // A deploy principal (#57) replays nothing outside its own stage or group: a rollback
    // applies the stored documents and records a new row, so it is a deploy to `row.target`.
    if let Some(sentence) = deploy_refusal(
        &identity,
        &row.owner_group,
        &row.target,
        caller.client_id.as_deref(),
        "roll back",
    ) {
        return error(StatusCode::FORBIDDEN, &sentence);
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
        let outcome = match stored_document_kind(document) {
            "StreamApp" => crate::streams::apply_stream_document(&s, &identity, document).await,
            _ => {
                crate::pipelines::apply_document(
                    resources.as_ref(),
                    s.db.as_ref(),
                    s.notebooks.as_ref(),
                    s.airflow.as_ref(),
                    &s.pipeline_creates,
                    &identity,
                    document,
                )
                .await
            }
        };
        match outcome {
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
        plan_id: None,
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
        .route("/deployments/plan", post(plan))
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
            plan_id: None,
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
            client_id: None,
        }
    }

    #[test]
    fn a_deploy_principal_is_confined_to_its_stage() {
        let id = identity("gh", &["shop"], false);
        assert_eq!(
            deploy_refusal(&id, "shop", "prod", Some("stardelt-deploy-prod"), "record"),
            None,
            "the stage its client names is allowed"
        );
        let refused = deploy_refusal(&id, "shop", "test", Some("stardelt-deploy-prod"), "record")
            .expect("another stage is refused");
        assert!(refused.contains("deploy stage `prod`"), "{refused}");
        assert!(refused.contains("to that stage"), "{refused}");
    }

    #[test]
    fn a_deploy_principal_is_confined_to_its_own_group() {
        for groups in [&[][..], &["visitors"][..]] {
            let refused = deploy_refusal(
                &identity("gh", groups, false),
                "shop",
                "prod",
                Some("stardelt-deploy-prod"),
                "record",
            )
            .expect("a group-less or foreign deploy principal is refused");
            assert!(refused.contains("its own group"), "{refused}");
        }
    }

    #[test]
    fn a_person_is_unaffected() {
        assert_eq!(
            deploy_refusal(
                &identity("ada", &["shop"], false),
                "shop",
                "prod",
                None,
                "record"
            ),
            None,
            "a member of the owner group records as before"
        );
        let refused = deploy_refusal(
            &identity("bob", &["visitors"], false),
            "shop",
            "prod",
            None,
            "record",
        )
        .expect("a non-member without a client id is still refused");
        assert!(
            refused.contains("only a member of the pipeline's owner group may record"),
            "{refused}"
        );
        assert_eq!(
            deploy_refusal(&identity("root", &[], true), "shop", "prod", None, "record"),
            None,
            "an administrator still passes"
        );
    }

    #[test]
    fn stage_group_of_reads_the_spec() {
        assert_eq!(
            stage_group_of(&serde_json::json!({"spec": {"group": "demo"}})).as_deref(),
            Some("demo")
        );
        assert_eq!(stage_group_of(&serde_json::json!({"spec": {}})), None);
        assert_eq!(
            stage_group_of(&serde_json::json!({"spec": {"group": ""}})),
            None,
            "an empty group names none"
        );
        assert_eq!(stage_group_of(&serde_json::json!({})), None);
    }

    #[test]
    fn a_stage_group_makes_the_group_rule_pass() {
        let mut identity = identity("gh", &[], false);
        identity.groups = vec!["demo".to_string()];
        assert_eq!(
            deploy_refusal(
                &identity,
                "demo",
                "dev",
                Some("stardelt-deploy-dev"),
                "plan"
            ),
            None,
            "the stage's group is the deploy principal's own group"
        );
        let mut without = identity.clone();
        without.groups = Vec::new();
        let refused = deploy_refusal(&without, "demo", "dev", Some("stardelt-deploy-dev"), "plan")
            .expect("no group means the own-group sentence");
        assert!(refused.contains("its own group"), "{refused}");
    }

    #[test]
    fn a_session_principal_has_no_client_id() {
        let caller = principal("ada", &["shop"]);
        assert_eq!(caller.client_id, None, "a session carries no client id");
        assert_eq!(
            deploy_refusal(
                &caller.identity,
                "shop",
                "prod",
                caller.client_id.as_deref(),
                "record"
            ),
            None,
            "a session is not mistaken for a deploy principal"
        );
        assert!(may_record(&caller.identity, "shop"));
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

    fn file(path: &str, sha256: &str) -> DeploymentFile {
        DeploymentFile {
            path: path.into(),
            sha256: sha256.into(),
        }
    }

    fn plan_request() -> BundlePlanRequest {
        BundlePlanRequest {
            bundle: "sales".into(),
            owner_group: "analytics".into(),
            target: "prod".into(),
            revision: None,
            resources: vec![serde_json::json!({"kind": "Pipeline", "metadata": {"name": "p"}})],
            files: vec![file("b.yaml", &"b".repeat(64))],
            prune: false,
        }
    }

    #[test]
    fn rendered_set_id_ignores_file_order_and_keeps_resource_order() {
        let documents = vec![
            serde_json::json!({"kind": "Pipeline", "metadata": {"name": "one"}}),
            serde_json::json!({"kind": "Pipeline", "metadata": {"name": "two"}}),
        ];
        let files = vec![
            file("a.sql", &"a".repeat(64)),
            file("b.sql", &"b".repeat(64)),
        ];
        let sorted = rendered_set_id("sales", "prod", &documents, &files);
        let reversed_files = vec![files[1].clone(), files[0].clone()];
        assert_eq!(
            rendered_set_id("sales", "prod", &documents, &reversed_files),
            sorted,
            "the files are sorted by path before they are hashed"
        );
        let reversed_documents = vec![documents[1].clone(), documents[0].clone()];
        assert_ne!(
            rendered_set_id("sales", "prod", &reversed_documents, &files),
            sorted,
            "the documents keep the order they were given in"
        );
        assert_ne!(
            rendered_set_id("sales", "test", &documents, &files),
            sorted,
            "the target is part of the identity"
        );
    }

    #[test]
    fn rendered_set_id_changes_with_a_digest() {
        let documents = vec![serde_json::json!({"kind": "Pipeline", "metadata": {"name": "one"}})];
        let first = rendered_set_id(
            "sales",
            "prod",
            &documents,
            &[file("a.sql", &"a".repeat(64))],
        );
        let second = rendered_set_id(
            "sales",
            "prod",
            &documents,
            &[file("a.sql", &"b".repeat(64))],
        );
        assert_ne!(first, second, "one changed digest is another set");
        assert_eq!(first.len(), 64, "lower-case hex sha256");
        assert!(
            first
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        );
    }

    #[test]
    fn plan_problem_refuses_another_sets_id() {
        let mut new = deployment("applied");
        assert_eq!(plan_problem(&new), None, "no planId is no problem");
        let ours = rendered_set_id(&new.bundle, &new.target, &new.resources, &new.files);
        new.plan_id = Some(ours.clone());
        assert_eq!(plan_problem(&new), None, "the row's own planId is the set");
        new.plan_id = Some("0".repeat(64));
        assert_eq!(
            plan_problem(&new).as_deref(),
            Some("the plan you applied is not the plan you saw: plan the bundle again"),
            "another set's planId is a 409"
        );
        new.plan_id = Some(ours);
        new.files = vec![file("a.sql", &"a".repeat(64))];
        assert!(
            plan_problem(&new).is_some(),
            "a file the plan did not name changes the set"
        );
    }

    #[test]
    fn validate_plan_refuses_the_names_the_record_refuses() {
        assert!(validate_plan(&plan_request()).is_ok());
        for set in [
            |r: &mut BundlePlanRequest| r.bundle = "shop/sales".into(),
            |r: &mut BundlePlanRequest| r.owner_group = String::new(),
            |r: &mut BundlePlanRequest| r.target = "t".repeat(MAX_NAME_CHARS + 1),
        ] {
            let mut request = plan_request();
            set(&mut request);
            let err = validate_plan(&request).expect_err("the record refuses this name too");
            assert!(
                err.contains("must not contain '/'") || err.contains("1 to"),
                "{err}"
            );
            let mut new = deployment("applied");
            new.bundle = request.bundle.clone();
            new.owner_group = request.owner_group.clone();
            new.target = request.target.clone();
            assert_eq!(
                validate_plan(&request).err(),
                validate(&new).err(),
                "one sentence for both routes"
            );
        }
        let mut request = plan_request();
        request.files = vec![file("a.sql", "abc")];
        assert_eq!(
            validate_plan(&request),
            Err("every file needs a path inside the bundle and a 64-character hex sha256".into()),
            "the plan refuses what the record refuses"
        );
    }

    #[test]
    fn validate_plan_refuses_201_resources() {
        let mut request = plan_request();
        request.resources = (0..=MAX_RESOURCES)
            .map(|i| serde_json::json!({"kind": "Pipeline", "metadata": {"name": format!("p{i}")}}))
            .collect();
        let err = validate_plan(&request).expect_err("201 resources");
        assert!(err.contains("200"), "{err}");
        request.resources.truncate(MAX_RESOURCES);
        assert!(validate_plan(&request).is_ok(), "exactly at the limit");
    }

    #[tokio::test]
    async fn plan_answers_503_before_401_without_a_state_store() {
        let response = plan(
            State(crate::router_tests::test_state_with_db(None)),
            None,
            ApiJson(plan_request()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
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
    async fn a_deploy_principal_may_not_roll_back_another_stage() {
        let pool = crate::db::test_support::migrated_pool().await;
        let state = crate::router_tests::test_state_with_db(Some(pool.clone()));
        let id = insert_row(
            &pool,
            "analytics",
            "applied",
            serde_json::json!([{"name": "daily", "ownerGroup": "analytics", "sql": "select 1"}]),
        )
        .await;
        let caller = |client_id: Option<&str>| {
            Some(Extension(Principal {
                client_id: client_id.map(str::to_string),
                ..principal("gh", &["analytics"])
            }))
        };

        // The deploy principal's client names `test`; the row is a `prod` deploy.
        let response = rollback(
            State(state.clone()),
            Path(id),
            caller(Some("stardelt-deploy-test")),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "a deploy principal may not roll back another stage's row"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the refusal is a body");
        let answer: serde_json::Value = serde_json::from_slice(&body).expect("one error");
        assert!(
            answer["error"]
                .as_str()
                .unwrap_or_default()
                .contains("deploy stage `test`"),
            "{answer}"
        );

        // A member of the owner group without a deploy client still gets past the rule (the
        // row then stops at the cluster, which this test does not have).
        let response = rollback(State(state.clone()), Path(id), caller(None)).await;
        assert_ne!(response.status(), StatusCode::FORBIDDEN);
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
            client_id: None,
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
            client_id: None,
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

    #[test]
    fn a_stored_document_is_replayed_as_its_kind() {
        assert_eq!(
            stored_document_kind(&serde_json::json!({"kind": "StreamApp"})),
            "StreamApp"
        );
        assert_eq!(
            stored_document_kind(&serde_json::json!({"kind": "Pipeline"})),
            "Pipeline"
        );
        assert_eq!(stored_document_kind(&serde_json::json!({})), "Pipeline");
        assert_eq!(
            stored_document_kind(&serde_json::json!({"kind": "SqlMigration"})),
            "Pipeline"
        );
    }
}
