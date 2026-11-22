//! Nova's public API description, generated from code (ticket #107).
//!
//! The `#[utoipa::path]` annotations live on the handlers (`main.rs` for
//! `/api/me`, `catalog.rs` for the catalog routes) and the `ToSchema` derives
//! on their request/response types; this module only collects them into one
//! document and serves it. Nothing here is hand-written YAML, so the
//! description cannot drift from the code — the `committed_document_is_fresh`
//! test below fails CI when the checked-in document is stale.
//!
//! Routes carry one of two tags: `public` (stable — tools and scripts may rely
//! on it) or `internal` (UI-only — may change without notice). Only `public`
//! is promised stable. `info.version` is Nova's own crate version and follows
//! semantic versioning for the `public` surface: a breaking change to a
//! `public` route bumps it.
//!
//! `GET /api/openapi.json` is served **unauthenticated** on purpose: the
//! document describes shapes, not data. It contains no user rows, no tokens,
//! no secrets and no upstream URLs — only paths, schema names and prose. The
//! `serves_the_document_unauthenticated` test pins that (status 200 with no
//! session, and no secret-shaped word in the JSON).
//!
//! The document names one server, the relative URL `/`, so a generated client
//! resolves every path against the address it fetched it from; and it states
//! the error convention: every non-2xx answer is an `ErrorResponse`
//! (`{"error": "<one sentence>"}`) except where an operation documents another
//! body (`POST /api/migrations/apply`'s 409 `ApplyAnswer`), and a missing or
//! invalid credential is a 401 `ErrorResponse` on every operation the global
//! `security` covers.
//!
//! Regenerate the checked-in document after changing an annotation or schema:
//!
//! ```bash
//! UPDATE_OPENAPI=1 cargo test -p stardelt-nova-backend openapi
//! ```

use axum::{http::header, response::IntoResponse};
use serde::Serialize;
use utoipa::OpenApi;

/// Every non-2xx answer under `/api/` is this envelope (see README "HTTP
/// conventions"), so every error response in the document points here.
#[derive(Serialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    /// Human-readable sentence. Never contains tokens, URLs or stack traces.
    pub error: String,
}

/// The collected document: every path in this first slice, with the schemas
/// the `body = …` attributes point at.
#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "Nova API",
        description = "The stardelt Nova gateway: catalog browse, queries and jobs over one identity. In `dev` mode (the default) every route answers without sign-in; in `oidc` mode every described route below requires a session or bearer token except this document itself. Only routes tagged `public` are promised stable. Every non-2xx answer is `{\"error\": \"<one sentence>\"}` (`ErrorResponse`), unless the operation documents another body: `POST /api/migrations/apply` answers 409 with `ApplyAnswer` and the refused migration file. A missing or invalid credential is 401 with an `ErrorResponse` on every operation the global `security` covers; the three routes that answer without a credential (the document itself, health and the sign-in config) carry an empty `security`."
    ),
    paths(
        crate::me,
        crate::health,
        crate::openapi::serve,
        crate::auth::config,
        crate::catalog::warehouse_info,
        crate::warehouses::list,
        crate::warehouses::start,
        crate::model_endpoints::list,
        crate::model_endpoints::probe,
        crate::ask::ask,
        crate::ask::feedback,
        crate::ask::history,
        crate::compute::list,
        crate::compute::start,
        crate::compute::stop,
        crate::compute::upsert,
        crate::compute::remove,
        crate::workspace::get,
        crate::workspace::start,
        crate::workspace::stop,
        crate::catalog::list_catalogs,
        crate::catalog_metrics::get,
        crate::catalog_metrics::refresh,
        crate::catalog::list_namespaces,
        crate::catalog::create_namespace,
        crate::catalog::list_tables,
        crate::catalog_search::search,
        crate::catalog::table_metadata,
        crate::catalog::table_history,
        crate::catalog::table_rollback,
        crate::catalog_docs::read,
        crate::catalog_docs::update,
        crate::catalog_docs::update_column,
        crate::verified_queries::list,
        crate::verified_queries::create,
        crate::verified_queries::delete,
        crate::verified_queries::check,
        crate::verified_queries::check_all,
        crate::connection_routes::list,
        crate::connection_routes::create,
        crate::connection_routes::rotate_personal,
        crate::connection_routes::rotate_group,
        crate::connection_routes::rotate_managed,
        crate::connection_routes::delete_personal,
        crate::connection_routes::delete_group,
        crate::connection_routes::delete_managed,
        crate::connection_routes::break_glass_group,
        crate::connection_routes::break_glass_personal,
        crate::connection_routes::break_glass_managed,
        crate::connection_routes::break_glass_log,
        crate::pipelines::list,
        crate::stages::compare,
        crate::deployments::record,
        crate::deployments::list,
        crate::deployments::detail,
        crate::deployments::rollback,
        crate::deployments::plan,
        crate::pipelines::list_runs,
        crate::pipelines::notebook_usage,
        crate::streams::list,
        crate::streams::patch,
        crate::streams::validate,
        crate::streams::put,
        crate::streams::remove,
        crate::migrations::plan_handler,
        crate::migrations::apply_handler,
        crate::migrations::approve_handler,
        crate::pipelines::principals,
        crate::pipelines::service_principals,
        crate::pipelines::check,
        crate::pipelines::preview,
        crate::pipelines::validate,
        crate::pipelines::create,
        crate::pipelines::detail,
        crate::pipelines::spec,
        crate::pipelines::update,
        crate::pipelines::remove,
        crate::pipelines::trigger_run,
        crate::pipelines::pause,
        crate::pipelines::resume,
        crate::pipelines::run,
        crate::pipelines::run_queries,
        crate::pipelines::checks,
        crate::pipelines::stop,
        crate::pipelines::retry,
        crate::pipelines::logs,
        crate::pipelines::run_notebook,
        crate::trino::run_query,
        crate::saved_queries::list,
        crate::saved_queries::create,
        crate::saved_queries::remove,
        crate::saved_queries::share,
        crate::saved_queries::unshare,
        crate::destinations::list,
        crate::destinations::create,
        crate::destinations::remove,
        crate::destinations::test,
        crate::query_log_api::list,
        crate::query_log_api::usage,
        crate::query_log_api::detail,
        crate::query_log_api::profile,
        crate::query_log_api::history,
        crate::lineage_api::graph,
        crate::lineage_api::columns,
        crate::lineage_api::run_detail,
        crate::lineage_api::read_tables,
        crate::user_settings::list,
        crate::user_settings::put,
        crate::user_settings::remove,
        crate::notebooks::publish,
        crate::notebooks::remove,
        crate::notebooks::list,
        crate::notebooks::versions,
        crate::notebooks::parameters,
        crate::notebooks::content,
        crate::statements::submit,
        crate::statements::list,
        crate::statements::poll,
        crate::statements::chunk,
        crate::statements::cancel,
        crate::admin_catalogs::list,
        crate::mcp_audit::list,
        crate::model_usage::list,
        crate::admin_catalogs::update_access,
        crate::admin_catalogs::create_team,
        crate::apps::list,
        crate::apps::links,
        crate::apps::detail,
        crate::apps::embeds,
        crate::cluster::list_handler,
        crate::trino_cluster,
        crate::jobs::list,
        crate::data::upload,
        crate::data::transfer,
    ),
    components(schemas(
        crate::Me,
        crate::OidcMe,
        crate::Health,
        crate::statements::SubmitRequest,
        crate::statements::StatementList,
        crate::statements::StatementListItem,
        crate::statements::StatementView,
        crate::statements::StatementState,
        crate::statements::ChunkView,
        crate::auth::AuthConfig,
        crate::db::DbStatus,
        crate::catalog::WarehouseInfo,
        crate::warehouses::SqlWarehouse,
        crate::model_endpoints::ModelEndpointRow,
        crate::ask::AskRequest,
        crate::ask::AskAnswer,
        crate::ask::AskTable,
        crate::ask::AskFeedback,
        crate::ask::AskConversation,
        crate::compute::ComputeOwner,
        crate::compute::ComputeCluster,
        crate::compute::ComputeResponse,
        crate::compute::ComputeResources,
        crate::compute::ComputeEnvVar,
        crate::compute::ComputeSpecBody,
        crate::workspace::WorkspaceView,
        crate::catalog::CatalogInfo,
        crate::catalog::CatalogsResponse,
        crate::catalog::RollbackRequest,
        crate::catalog_metrics::CatalogStats,
        crate::catalog_metrics::CatalogMetricsView,
        crate::catalog_metrics::MetricsTotals,
        crate::catalog_metrics::MetricsResponse,
        crate::catalog_metrics::RefreshAccepted,
        crate::catalog::NamespacesResponse,
        crate::catalog_search::SearchMatch,
        crate::catalog_search::SearchResult,
        crate::catalog::TablesResponse,
        crate::catalog::TableIdentifier,
        crate::catalog::TableMetadataResponse,
        crate::catalog::CreateNamespaceRequest,
        crate::catalog::CreateNamespaceResponse,
        crate::catalog_docs::TableDocumentation,
        crate::catalog_docs::ColumnDocumentation,
        crate::catalog_docs::DocumentationUpdate,
        crate::catalog_docs::ColumnDescriptionUpdate,
        crate::verified_queries::VerifiedQuery,
        crate::deployments::NewDeployment,
        crate::deployments::Deployment,
        crate::deployments::BundlePlanRequest,
        crate::deployments::BundlePlan,
        crate::deployments::PlanIssue,
        crate::deployments::PlanChange,
        crate::deployments::PlanFile,
        crate::verified_queries::NewVerifiedQuery,
        crate::verified_queries::CheckAllAnswer,
        crate::connections::NewConnection,
        crate::connections::ConnectionView,
        crate::connections::BreakGlassNotice,
        crate::connections::SyncView,
        crate::connections::Scope,
        crate::connection_routes::RotateRequest,
        crate::connection_routes::ConnectionsResponse,
        crate::connection_routes::BreakGlassRequest,
        crate::connection_routes::BreakGlassResponse,
        crate::connection_routes::BreakGlassLogResponse,
        crate::connection_routes::BreakGlassLogRow,
        crate::pipelines::PipelineView,
        crate::pipelines::PipelineListResponse,
        crate::pipelines::PipelineDetail,
        crate::pipelines::PipelineSpecResponse,
        crate::pipelines::Provenance,
        crate::pipelines::DeploymentView,
        crate::pipelines::PipelineDocument,
        crate::pipelines::NotebookRequest,
        crate::pipelines::PipelineTaskRequest,
        crate::pipelines::PrincipalsResponse,
        crate::pipelines::PrincipalView,
        crate::pipelines::ServicePrincipalView,
        crate::pipelines::ServicePrincipalsResponse,
        crate::streams::StreamState,
        crate::streams::StreamView,
        crate::streams::StreamsResponse,
        crate::streams::StreamWrite,
        crate::streams::StreamSourceBody,
        crate::streams::StreamSinkBody,
        crate::streams::StreamValidation,
        crate::streams::StreamIssue,
        crate::pipelines::CreatePipelineRequest,
        crate::pipelines::UpdatePipelineRequest,
        crate::pipelines::ScheduleRequest,
        crate::pipelines::PipelineOptions,
        crate::pipelines::PipelineOptionsView,
        crate::pipelines::CheckStatementRequest,
        crate::pipelines::CheckStatementResponse,
        crate::pipelines::PreviewScheduleRequest,
        crate::pipelines::PreviewScheduleResponse,
        crate::pipelines::ValidatePipelineResponse,
        crate::pipelines::ValidationIssue,
        crate::pipelines::RunView,
        crate::pipelines::RunStart,
        crate::pipelines::RunsListRow,
        crate::pipelines::RunsListResponse,
        crate::pipelines::NotebookScheduleRow,
        crate::pipelines::NotebookUsageResponse,
        crate::pipelines::RunResponse,
        crate::pipelines::TaskView,
        crate::pipelines::RunQueryView,
        crate::pipelines::RunQueriesResponse,
        crate::pipelines::TaskSpecView,
        crate::pipelines::LogView,
        crate::pipelines::ChecksResponse,
        crate::pipelines::CheckView,
        crate::pipelines::LogLine,
        crate::pipelines::RetriesView,
        crate::trino::QueryRequest,
        crate::trino::QueryResult,
        crate::trino::Column,
        crate::saved_queries::SavedQuery,
        crate::saved_queries::NewSavedQuery,
        crate::saved_queries::NewShare,
        crate::destinations::Destination,
        crate::destinations::NewDestination,
        crate::destinations::DestinationTest,
        crate::query_log_api::QueryLogPage,
        crate::query_log_api::UsagePage,
        crate::query_log_api::UsageRow,
        crate::query_log_api::QueryLogItem,
        crate::query_log_api::QueryLogDetail,
        crate::query_log_api::QueryTiming,
        crate::query_log_api::QueryData,
        crate::query_log_api::QueryFailure,
        crate::query_log_api::QueryHistory,
        crate::query_log_api::HistoryRun,
        crate::user_settings::StoredSetting,
        crate::notebooks::PublishedNotebook,
        crate::notebooks::DeletedNotebook,
        crate::notebooks::NotebookSummary,
        crate::notebooks::NotebookVersion,
        crate::notebooks::NotebookParameter,
        crate::notebooks::NotebookParameters,
        crate::admin_catalogs::AdminCatalog,
        crate::admin_catalogs::CatalogGrant,
        crate::admin_catalogs::CatalogCondition,
        crate::admin_catalogs::CatalogAccessUpdate,
        crate::admin_catalogs::NewTeam,
        crate::admin_catalogs::NewTeamResponse,
        crate::mcp_audit::McpCall,
        crate::mcp_audit::McpCalls,
        crate::model_usage::ModelUsage,
        crate::model_usage::UsageRow,
        crate::model_usage::UsageCall,
        ErrorResponse,
    )),
    tags(
        (name = "public", description = "Stable surface for tools and scripts. Breaking changes bump `info.version`."),
        (name = "internal", description = "UI-only surface. May change or disappear without notice; do not build tools on it."),
    )
)]
pub struct ApiDoc;

/// The served document. The version is stamped from the crate version at
/// compile time (see the module docs for the semver promise).
pub fn document() -> utoipa::openapi::OpenApi {
    use utoipa::openapi::security::{
        ApiKey, ApiKeyValue, HttpAuthScheme, HttpBuilder, SecurityRequirement, SecurityScheme,
    };
    let mut doc = ApiDoc::openapi();
    doc.info.version = env!("CARGO_PKG_VERSION").to_string();
    // A relative URL: the same document is served by every instance, and a generated client
    // resolves it against the address it fetched the document from.
    let mut server = utoipa::openapi::server::Server::new("/");
    server.description = Some(
        "This Nova instance. Every path below starts with `/api`; a missing or invalid \
         credential is answered with 401 and an ErrorResponse."
            .to_string(),
    );
    doc.servers = Some(vec![server]);
    // How a client proves who it is (see auth.rs): either credential works on every
    // route below; generated clients, the CLI and the MCP server read this.
    let components = doc.components.get_or_insert_with(Default::default);
    components.add_security_scheme(
        "bearer",
        SecurityScheme::Http(
            HttpBuilder::new()
                .scheme(HttpAuthScheme::Bearer)
                .bearer_format("JWT")
                .description(Some(
                    "An access token from the platform's identity provider, sent in the `Authorization` header. In `dev` mode no credential is needed.",
                ))
                .build(),
        ),
    );
    components.add_security_scheme(
        "session",
        SecurityScheme::ApiKey(ApiKey::Cookie(ApiKeyValue::with_description(
            "nova_session",
            "The signed session cookie the browser sign-in sets. Same rules as the bearer token.",
        ))),
    );
    doc.security = Some(vec![
        SecurityRequirement::new("bearer", Vec::<String>::new()),
        SecurityRequirement::new("session", Vec::<String>::new()),
    ]);
    // Three routes deliberately answer without a session or token (see their annotations).
    // OpenAPI's empty list overrides the document's global requirement, so a generated client
    // does not attach a credential to them.
    for id in ["openapi_document", "health", "auth_config"] {
        for item in doc.paths.paths.values_mut() {
            if let Some(op) = item.get.as_mut()
                && op.operation_id.as_deref() == Some(id)
            {
                op.security = Some(Vec::new());
            }
        }
    }
    doc
}

/// The exact bytes served at `GET /api/openapi.json` and committed to
/// `openapi/nova.json` (plus one trailing newline in the file).
pub fn document_json() -> String {
    serde_json::to_string_pretty(&document()).expect("the OpenAPI document serializes")
}

/// Serve the document. Deliberately outside sign-in (wired next to
/// `/api/health` in `main`): it carries shapes, not data.
#[utoipa::path(
    get,
    operation_id = "openapi_document",
    path = "/api/openapi.json",
    tag = "public",
    description = "This document. It answers without sign-in and carries shapes, not data.",
    responses(
        (status = 200, description = "The OpenAPI 3.1 document", body = Object),
    )
)]
pub async fn serve() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/json")],
        document_json(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Path of the committed document, relative to the crate root.
    fn committed_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../openapi/nova.json")
    }

    /// Every `public` operation says what it does: the API reference and the MCP/CLI
    /// consumers show this text, and a bare path tells a reader nothing (#95).
    #[test]
    fn every_public_operation_has_a_description() {
        let doc = serde_json::to_value(document()).expect("the document serializes");
        let mut missing = Vec::new();
        for (path, item) in doc["paths"].as_object().expect("paths") {
            for (method, op) in item.as_object().expect("a path item") {
                if !["get", "post", "put", "patch", "delete"].contains(&method.as_str()) {
                    continue;
                }
                let public = op["tags"]
                    .as_array()
                    .is_some_and(|tags| tags.iter().any(|t| t == "public"));
                let text = |key: &str| op[key].as_str().is_some_and(|s| !s.trim().is_empty());
                if public && !text("description") && !text("summary") {
                    missing.push(format!("{} {path}", method.to_uppercase()));
                }
            }
        }
        assert!(
            missing.is_empty(),
            "public operations without a description: {missing:?}"
        );
    }

    /// The CI check: the committed document must equal what the annotations
    /// generate. Regenerate with `UPDATE_OPENAPI=1 cargo test -p
    /// stardelt-nova-backend openapi` and commit the result alongside the
    /// annotation change.
    #[test]
    fn committed_document_is_fresh() {
        let path = committed_path();
        let generated = format!("{}\n", document_json());
        if std::env::var("UPDATE_OPENAPI").is_ok() {
            std::fs::create_dir_all(path.parent().expect("openapi dir")).expect("openapi dir");
            std::fs::write(&path, &generated).expect("write the regenerated document");
            return;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_else(|_| {
            panic!(
                "missing {} — run UPDATE_OPENAPI=1 cargo test -p stardelt-nova-backend openapi",
                path.display()
            )
        });
        assert_eq!(
            committed, generated,
            "openapi/nova.json is stale — run UPDATE_OPENAPI=1 cargo test -p stardelt-nova-backend openapi and commit the result"
        );
    }

    /// Routes that are served but not in the document yet. This list only
    /// shrinks: document a route (add its `#[utoipa::path]`) and delete its
    /// line here. Adding a route without either fails the test below.
    const UNDOCUMENTED: &[&str] = &[
        // Browser redirects, not a JSON API: no client calls these meaningfully.
        "/api/auth/login",
        "/api/auth/callback",
        "/api/auth/logout",
        // The MCP server is served at the root; its own contract lands with #88.
        "/mcp",
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource/mcp",
    ];

    /// The `.route("<path>"` literals inside the router function that starts
    /// at `marker` in `src`, each prefixed with `prefix` — where the router is
    /// mounted: `/api` for everything in `api_router`, `/api/auth` for the
    /// sign-in routes, and `""` for the MCP server, which `main`'s `app`
    /// merges at the root. The function body ends at the first closing brace
    /// in column 0.
    fn routes_in(src: &str, marker: &str, prefix: &str) -> Vec<String> {
        let start = src
            .find(marker)
            .unwrap_or_else(|| panic!("{marker} not found"));
        let body = &src[start..];
        let body = &body[..body.find("\n}\n").expect("end of the router function")];
        let mut paths = Vec::new();
        let mut rest = body;
        while let Some(i) = rest.find(".route(") {
            rest = rest[i + ".route(".len()..].trim_start();
            if let Some(quoted) = rest.strip_prefix('"')
                && let Some(end) = quoted.find('"')
            {
                paths.push(format!("{prefix}{}", &quoted[..end]));
            }
        }
        paths
    }

    /// Every route Nova serves under `/api` must be in the OpenAPI document or
    /// in `UNDOCUMENTED`, and `UNDOCUMENTED` may not list a documented or
    /// vanished route. One contract is what the SDKs, the CLI and the MCP
    /// server are generated from (stardelt-nova#229).
    #[test]
    fn every_api_route_is_documented_or_listed() {
        let main_rs = include_str!("main.rs");
        let mut served = Vec::new();
        for marker in ["fn public_router()", "fn api_router()", "fn admin_router()"] {
            served.extend(routes_in(main_rs, marker, "/api"));
        }
        served.extend(routes_in(
            include_str!("pipelines.rs"),
            "fn router()",
            "/api",
        ));
        served.extend(routes_in(
            include_str!("notebooks.rs"),
            "fn router()",
            "/api",
        ));
        served.extend(routes_in(
            include_str!("statements.rs"),
            "fn router()",
            "/api",
        ));
        for source in [
            include_str!("stages.rs"),
            include_str!("deployments.rs"),
            include_str!("streams.rs"),
            include_str!("migrations.rs"),
        ] {
            served.extend(routes_in(source, "fn router()", "/api"));
        }
        // The browser sign-in routes: their path prefix is what the document and the ingress use.
        served.extend(routes_in(
            include_str!("auth.rs"),
            "pub fn router",
            "/api/auth",
        ));
        served.extend(routes_in(
            include_str!("auth.rs"),
            "pub fn dev_router",
            "/api/auth",
        ));
        // The MCP server is merged at the root by `main`'s `app`, not under /api.
        served.extend(routes_in(include_str!("mcp.rs"), "pub fn router()", ""));
        served.sort();
        served.dedup();
        assert!(
            served.len() > 120,
            "the scan of main.rs, pipelines.rs, notebooks.rs, statements.rs, stages.rs, \
             deployments.rs, streams.rs, migrations.rs, auth.rs and mcp.rs found only {} routes",
            served.len()
        );

        let value = serde_json::to_value(document()).expect("the document is JSON");
        let documented: Vec<String> = value["paths"]
            .as_object()
            .expect("paths is an object")
            .keys()
            .cloned()
            .collect();

        let mut missing: Vec<&String> = served
            .iter()
            .filter(|p| !documented.contains(p) && !UNDOCUMENTED.contains(&p.as_str()))
            .collect();
        missing.sort();
        missing.dedup();
        assert!(
            missing.is_empty(),
            "served but neither documented nor listed in UNDOCUMENTED: {missing:#?}"
        );

        let stale: Vec<&&str> = UNDOCUMENTED
            .iter()
            .filter(|p| documented.iter().any(|d| d == **p) || !served.iter().any(|s| s == **p))
            .collect();
        assert!(
            stale.is_empty(),
            "UNDOCUMENTED lists a route that is now documented or no longer served: {stale:#?}"
        );
    }

    /// A client can tell from the document how to authenticate: two schemes
    /// (bearer token, session cookie) and a global requirement naming both.
    #[test]
    fn security_schemes_are_documented() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        let schemes = &value["components"]["securitySchemes"];
        assert_eq!(schemes["bearer"]["type"], "http");
        assert_eq!(schemes["bearer"]["scheme"], "bearer");
        assert_eq!(schemes["session"]["type"], "apiKey");
        assert_eq!(schemes["session"]["in"], "cookie");
        assert_eq!(schemes["session"]["name"], "nova_session");
        let required = value["security"]
            .as_array()
            .expect("a global security list");
        assert_eq!(required.len(), 2, "bearer or session");
    }

    /// A generated client resolves relative path operations against a server, so
    /// the document names exactly one: this instance, whatever address it was
    /// fetched from.
    #[test]
    fn the_document_names_its_server() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        let servers = value["servers"].as_array().expect("servers is an array");
        assert_eq!(servers.len(), 1, "exactly one server: this instance");
        assert_eq!(servers[0]["url"], "/");
        assert!(
            servers[0]["description"]
                .as_str()
                .is_some_and(|d| !d.is_empty()),
            "the server carries a non-empty description"
        );
    }

    /// The operations a bundle deploy and a reviewer use (#761): path, method, the 2xx status
    /// whose body carries an example, and whether the operation also takes a request body.
    const PUBLISHING: &[(&str, &str, &str, bool)] = &[
        ("/api/deployments", "get", "200", false),
        ("/api/deployments", "post", "201", true),
        ("/api/deployments/{id}", "get", "200", false),
        ("/api/deployments/{id}/rollback", "post", "201", false),
        ("/api/deployments/plan", "post", "200", true),
        ("/api/pipelines", "post", "201", true),
        ("/api/pipelines/validate", "post", "200", true),
        (
            "/api/pipelines/{namespace}/{name}/spec",
            "get",
            "200",
            false,
        ),
    ];

    /// Every publishing operation answers with an example of its 2xx body, and the three that
    /// take a body carry one too — the document is what a client generator and the MCP server
    /// read, so a caller never has to infer a field from prose (#95 item 5, #761).
    #[test]
    fn the_publishing_operations_carry_examples() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        for (path, method, status, has_body) in PUBLISHING {
            let operation = &value["paths"][path][method];
            assert!(operation.is_object(), "{method} {path} is in the document");
            if *has_body {
                assert!(
                    !operation["requestBody"]["content"]["application/json"]["example"].is_null(),
                    "{method} {path} carries a request example"
                );
            }
            assert!(
                !operation["responses"][status]["content"]["application/json"]["example"].is_null(),
                "{method} {path} {status} carries a response example"
            );
        }
    }

    /// The two request examples deserialize into their types: a renamed, removed or newly
    /// required field fails here rather than in a client's hands (#761).
    #[test]
    fn the_request_examples_parse() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        let record = value["paths"]["/api/deployments"]["post"]["requestBody"]["content"]
            ["application/json"]["example"]
            .clone();
        serde_json::from_value::<crate::deployments::NewDeployment>(record)
            .expect("the deployments request example is a NewDeployment");
        let create =
            value["paths"]["/api/pipelines"]["post"]["requestBody"]["content"]["application/json"]
                ["example"]
                .clone();
        serde_json::from_value::<crate::pipelines::CreatePipelineRequest>(create)
            .expect("the pipelines request example is a CreatePipelineRequest");
    }

    /// The `properties` of the schema a media type points at, following a `$ref` into
    /// `components.schemas` and the `items` of an array.
    fn schema_properties<'a>(
        document: &'a serde_json::Value,
        media: &'a serde_json::Value,
    ) -> &'a serde_json::Map<String, serde_json::Value> {
        fn resolve<'a>(
            document: &'a serde_json::Value,
            schema: &'a serde_json::Value,
        ) -> &'a serde_json::Value {
            match schema["$ref"].as_str() {
                Some(reference) => {
                    let name = reference.rsplit('/').next().expect("a $ref names a schema");
                    &document["components"]["schemas"][name]
                }
                None => schema,
            }
        }
        let mut schema = resolve(document, &media["schema"]);
        if schema["type"] == "array" {
            schema = resolve(document, &schema["items"]);
        }
        schema["properties"]
            .as_object()
            .unwrap_or_else(|| panic!("the example's schema carries properties: {schema}"))
    }

    /// Every key of every publishing example is a property of its schema: a field removed from
    /// a struct cannot stay in the example, and an example cannot invent one (#761). Nested
    /// values are not walked — the shapes they name are the caller's data.
    #[test]
    fn an_example_names_only_schema_properties() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        for (path, method, status, has_body) in PUBLISHING {
            let operation = &value["paths"][path][method];
            let mut media = Vec::new();
            if *has_body {
                media.push((
                    &operation["requestBody"]["content"]["application/json"],
                    format!("{method} {path} (request)"),
                ));
            }
            media.push((
                &operation["responses"][status]["content"]["application/json"],
                format!("{method} {path} {status}"),
            ));
            for (media, what) in media {
                let example = &media["example"];
                let object = match example.as_array() {
                    Some(array) => array.first().cloned().unwrap_or_default(),
                    None => example.clone(),
                };
                let Some(object) = object.as_object() else {
                    continue;
                };
                let properties = schema_properties(&value, media);
                for key in object.keys() {
                    assert!(
                        properties.contains_key(key),
                        "{what}: `{key}` is no property of the schema (properties: {:?})",
                        properties.keys().collect::<Vec<_>>()
                    );
                }
            }
        }
    }

    /// A generated client names its methods after the `operationId`, so two
    /// operations may not share one. A handler whose function name is generic
    /// (`list`, `create`, `update`, `remove`) sets `operation_id = "<resource>_<verb>"`
    /// in its `#[utoipa::path]`.
    #[test]
    fn operation_ids_are_unique() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        let mut seen = std::collections::BTreeMap::new();
        let mut clashes = Vec::new();
        for (path, item) in value["paths"].as_object().expect("paths is an object") {
            for (method, op) in item.as_object().expect("a path item is an object") {
                let Some(id) = op["operationId"].as_str() else {
                    continue;
                };
                if let Some(first) = seen.insert(id.to_string(), format!("{method} {path}")) {
                    clashes.push(format!("{id}: {first} and {method} {path}"));
                }
            }
        }
        assert!(clashes.is_empty(), "duplicate operationIds: {clashes:#?}");
    }

    /// The SDKs, the CLI and the MCP server name their methods and tools after the
    /// operation ids, so an id must read as `<resource>_<action>` (`pipelines_get`,
    /// `table_history`), never as a bare verb (`get`, `run`, `logs`) that says nothing
    /// outside its file. `me` and `health` are the two single-word ids.
    #[test]
    fn operation_ids_are_resource_first_snake_case() {
        const BARE_VERBS: &[&str] = &[
            "list", "get", "read", "create", "update", "delete", "remove", "run", "stop", "rotate",
            "break", "trigger", "publish", "check", "preview", "pause", "resume", "refresh",
        ];
        let value = serde_json::to_value(document()).expect("the document is JSON");
        let mut bad = Vec::new();
        for (path, item) in value["paths"].as_object().expect("paths is an object") {
            for (method, op) in item.as_object().expect("a path item is an object") {
                let Some(id) = op["operationId"].as_str() else {
                    continue;
                };
                let words: Vec<&str> = id.split('_').collect();
                let snake = words.iter().all(|w| {
                    !w.is_empty()
                        && w.chars()
                            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
                });
                let ok = id == "me"
                    || id == "health"
                    || (snake && words.len() >= 2 && !BARE_VERBS.contains(&words[0]));
                if !ok {
                    bad.push(format!("{id} ({method} {path})"));
                }
            }
        }
        assert!(
            bad.is_empty(),
            "operation ids must be <resource>_<action> in snake case: {bad:#?}"
        );
    }

    /// A generated client builds the URL from the declared path parameters, so the
    /// `{name}` parts of a path and its `in: path` parameters must be the same set:
    /// a query parameter documented as a path one becomes a required argument.
    #[test]
    fn path_parameters_match_the_path_template() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        let mut bad = Vec::new();
        for (path, item) in value["paths"].as_object().expect("paths is an object") {
            let mut in_path: Vec<&str> = path
                .split('/')
                .filter_map(|part| part.strip_prefix('{')?.strip_suffix('}'))
                .collect();
            in_path.sort_unstable();
            for (method, op) in item.as_object().expect("a path item is an object") {
                let mut declared: Vec<&str> = op["parameters"]
                    .as_array()
                    .map(|all| all.iter().filter(|p| p["in"] == "path"))
                    .into_iter()
                    .flatten()
                    .filter_map(|p| p["name"].as_str())
                    .collect();
                declared.sort_unstable();
                if declared != in_path {
                    bad.push(format!(
                        "{method} {path}: declared {declared:?}, template {in_path:?}"
                    ));
                }
            }
        }
        assert!(
            bad.is_empty(),
            "path parameters differ from the path: {bad:#?}"
        );
    }

    /// The service inventory is for the interface and for operators, not a promise to tools:
    /// all four routes are `internal`, and the two that show cluster data can answer 403.
    #[test]
    fn the_service_inventory_is_internal_and_only_the_operator_routes_can_answer_403() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        for (path, id, forbidden) in [
            ("/api/apps", "apps_list", true),
            ("/api/apps/{id}", "apps_detail", true),
            ("/api/apps/links", "apps_links", false),
            ("/api/embeds", "apps_embeds", false),
            ("/api/workloads", "workloads_list", true),
            ("/api/trino/cluster", "trino_cluster", true),
        ] {
            let op = &value["paths"][path]["get"];
            assert_eq!(op["operationId"], id, "{path}");
            assert_eq!(op["tags"], serde_json::json!(["internal"]), "{path}");
            assert_eq!(op["responses"].get("403").is_some(), forbidden, "{path}");
        }
    }

    /// A notification destination is an address a tool points at, so the four routes are a
    /// public promise (stardelt/stardelt#464): the SDK, the CLI and the MCP server are generated
    /// from `public` operations only, and the demo's ntfy run feed creates one from a script.
    #[test]
    fn the_notification_destination_routes_are_public() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        for (path, method, id) in [
            ("/api/destinations", "get", "destinations_list"),
            ("/api/destinations", "post", "destinations_create"),
            ("/api/destinations/{id}", "delete", "destinations_remove"),
            ("/api/destinations/{id}/test", "post", "destinations_test"),
        ] {
            let op = &value["paths"][path][method];
            assert_eq!(op["operationId"], id, "{path}");
            assert_eq!(op["tags"], serde_json::json!(["public"]), "{path}");
        }
    }

    /// Loading and copying a table are for the interface: `internal`, open to every
    /// signed-in user (the engine's own permissions decide), and the two writes are POSTs.
    #[test]
    fn the_data_movement_routes_are_internal_and_open_to_every_signed_in_user() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        for (path, method, id) in [
            ("/api/ingest/upload", "post", "ingest_upload"),
            ("/api/transfer", "post", "tables_transfer"),
            ("/api/jobs", "get", "jobs_list"),
        ] {
            let op = &value["paths"][path][method];
            assert_eq!(op["operationId"], id, "{path}");
            assert_eq!(op["tags"], serde_json::json!(["internal"]), "{path}");
            // Catalog-scoped writes may return 403 when the caller may not write a catalog.
            if path == "/api/jobs" {
                assert!(op["responses"].get("403").is_none(), "{path}");
            } else {
                assert!(op["responses"].get("403").is_some(), "{path}");
            }
            assert!(op["responses"].get("401").is_some(), "{path}");
        }
    }

    /// A setting's value is any JSON (a string, a number, a list), so the schema
    /// must not say `object`: a generated client would refuse the rest.
    #[test]
    fn a_setting_value_is_any_json() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        let body = &value["paths"]["/api/settings/{key}"]["put"]["requestBody"]["content"]["application/json"]
            ["schema"];
        assert!(body.get("type").is_none(), "the PUT body is typed: {body}");
        let stored = &value["components"]["schemas"]["StoredSetting"]["properties"]["value"];
        assert!(
            stored.get("type").is_none(),
            "the stored value is typed: {stored}"
        );
    }

    /// A cell of a query result is any JSON scalar, array or object, so the row
    /// schema must not say `object`: a generated client would refuse the numbers.
    #[test]
    fn a_result_cell_is_any_json() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        let cell =
            &value["components"]["schemas"]["QueryResult"]["properties"]["rows"]["items"]["items"];
        assert!(cell.get("type").is_none(), "a result cell is typed: {cell}");
    }

    /// A non-2xx answer a generated client cannot type is an answer it cannot
    /// handle. Every one names a body: `ErrorResponse`, except
    /// `post /api/migrations/apply` `409`, which answers an `ApplyAnswer` with
    /// the `errors` list of a changed or reordered migration file (the CLI
    /// reads it, `stardelt migrations apply`).
    #[test]
    fn every_error_response_names_a_body() {
        const METHODS: &[&str] = &["get", "put", "post", "delete", "patch", "head", "options"];
        const APPLY_409: (&str, &str) = ("/api/migrations/apply", "post");
        let value = serde_json::to_value(document()).expect("the document is JSON");
        let mut bad = Vec::new();
        for (path, item) in value["paths"].as_object().expect("paths is an object") {
            for method in METHODS {
                let Some(op) = item.get(method) else {
                    continue;
                };
                for (code, response) in op["responses"].as_object().expect("responses") {
                    if code.starts_with('2') {
                        continue;
                    }
                    let schema = &response["content"]["application/json"]["schema"]["$ref"];
                    let expected = if (path.as_str(), *method) == APPLY_409 && code == "409" {
                        "#/components/schemas/ApplyAnswer"
                    } else {
                        "#/components/schemas/ErrorResponse"
                    };
                    if schema != expected {
                        bad.push(format!("{method} {path} {code}: {schema}"));
                    }
                }
            }
        }
        assert!(
            bad.is_empty(),
            "a non-2xx answer must name its body: {bad:#?}"
        );
    }

    /// The document's `security` is global; only the credential-free routes carry an empty list.
    /// Every other operation must say it answers `401`, so a generated client can tell a missing
    /// credential from a bad request.
    #[test]
    fn every_authenticated_operation_lists_401() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        let mut bad = Vec::new();
        for (_path, item) in value["paths"].as_object().expect("paths is an object") {
            for (method, op) in item.as_object().expect("a path item is an object") {
                if !matches!(method.as_str(), "get" | "put" | "post" | "delete" | "patch") {
                    continue;
                }
                let Some(id) = op["operationId"].as_str() else {
                    continue;
                };
                let credential_free = op
                    .get("security")
                    .and_then(|value| value.as_array())
                    .is_some_and(|list| list.is_empty());
                if credential_free {
                    continue;
                }
                if !op["responses"]
                    .as_object()
                    .expect("responses is an object")
                    .contains_key("401")
                {
                    bad.push(id.to_string());
                }
            }
        }
        assert!(
            bad.is_empty(),
            "these authenticated operations do not list 401: {bad:#?}"
        );
    }

    /// The document's `security` is global, so a route that answers without a
    /// credential says so with the empty list. Those are the document itself,
    /// the readiness probe and the sign-in configuration; `me` inherits the
    /// global bearer-or-session requirement and must not carry an empty one.
    #[test]
    fn the_credential_free_routes_say_so() {
        let value = serde_json::to_value(document()).expect("the document is JSON");
        let mut ops = std::collections::BTreeMap::new();
        for item in value["paths"].as_object().expect("paths").values() {
            for op in item.as_object().expect("a path item is an object").values() {
                if let Some(id) = op["operationId"].as_str() {
                    ops.insert(id.to_string(), op);
                }
            }
        }
        for id in ["openapi_document", "health", "auth_config"] {
            let op = ops.get(id).unwrap_or_else(|| panic!("{id} is documented"));
            assert_eq!(
                op.get("security"),
                Some(&serde_json::json!([])),
                "{id} answers without a credential"
            );
        }
        let me = ops.get("me").expect("me is documented");
        assert!(
            me.get("security").is_none(),
            "me inherits the global requirement"
        );
    }

    /// The document endpoint answers without a session and carries no data:
    /// no tokens, no cookies, no secrets — only paths, schemas and prose.
    #[test]
    fn document_carries_no_secrets() {
        let json = document_json();
        for word in ["access_token", "refresh_token", "id_token", "Secret"] {
            assert!(
                !json.contains(word),
                "the public document must not mention {word:?}"
            );
        }
        let value: serde_json::Value = serde_json::from_str(&json).expect("the document is JSON");
        assert_eq!(
            value["info"]["version"].as_str(),
            Some(env!("CARGO_PKG_VERSION")),
            "the version field follows the crate version"
        );
        for path in [
            "/api/me",
            "/api/catalog/warehouse",
            "/api/catalog/namespaces",
            "/api/catalog/namespaces/{ns}/tables",
            "/api/catalog/tables/{ns}/{tbl}",
        ] {
            assert!(
                value["paths"].get(path).is_some(),
                "the document describes {path}"
            );
        }
    }
}
