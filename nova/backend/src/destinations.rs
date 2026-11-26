//! Notification destinations: named email and webhook endpoints owned by one group
//! (design ticket stardelt-nova#273, slice a).
//!
//! A destination is an *address* other parts of Nova point at — an alert rule, a
//! pipeline failure notice, an access request. Slice a is the resource and its CRUD;
//! slice b adds one test message (`POST /api/destinations/{id}/test`), sent to a
//! webhook and recorded in `destination_delivery`. Mail waits for the platform's SMTP
//! relay, so an email destination answers `400` there rather than pretending to send.
//!
//! The owner group decides who sees a destination, the same rule the rest of Nova uses
//! for group-facing resources: a member of the group, or an administrator; another
//! group's id answers `404`, exactly like an id that does not exist. In `dev` mode
//! there is no principal and one person, so every destination is theirs.
//!
//! The four routes are `public` (stardelt/stardelt#464): a destination is an address a pipeline
//! or an operator tool points at, so the CLI and the MCP server must be able to create one —
//! `stardelt api destinations_create` is generated from the contract.
//!
//! No secret is stored here. A webhook's header secrets wait for the service-identity
//! decision (slice c).
//!
//! Without a state store (`NOVA_DATABASE_URL` unset) all four routes answer
//! `503 {"error":"this needs Nova's database, and this installation has none; ask your administrator"}`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

use crate::AppState;
use crate::api_json::ApiJson;
use crate::auth::Principal;
use crate::saved_queries::{error, not_found, store_failed, unavailable, validate_group};

/// The longest webhook address Nova stores, in characters.
const MAX_URL_CHARS: usize = 2048;
/// A destination's name and its owner group, like the table's CHECK constraints.
const MAX_NAME_CHARS: usize = 64;
/// An email destination carries 1 to 20 recipients.
const MAX_RECIPIENTS: usize = 20;

/// A named email or webhook endpoint owned by one group (`GET /api/destinations`).
#[derive(Serialize, FromRow, Debug, PartialEq, utoipa::ToSchema)]
pub struct Destination {
    pub id: i64,
    pub owner_group: String,
    pub name: String,
    /// `email` or `webhook`.
    pub kind: String,
    /// The kind's address: `{"recipients": [...]}` or `{"url": "https://…"}`.
    #[schema(value_type = Value)]
    pub config: serde_json::Value,
    /// Groups that may see this destination, read-only. Empty: its owner group only.
    pub viewers: Vec<String>,
    /// Epoch milliseconds, like `Job.started_at`.
    pub created_at: i64,
    /// Epoch milliseconds, like `Job.started_at`.
    pub updated_at: i64,
}

/// `POST /api/destinations` body.
#[derive(Deserialize, Debug, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct NewDestination {
    pub owner_group: String,
    pub name: String,
    pub kind: String,
    /// The kind's address: `{"recipients": [...]}` or `{"url": "https://…"}`.
    #[schema(value_type = Value)]
    pub config: serde_json::Value,
    #[serde(default)]
    pub viewers: Vec<String>,
}

/// `{"recipients": [1 to 20 non-empty addresses]}` and nothing else needed.
fn email_config(config: &serde_json::Value) -> bool {
    config
        .get("recipients")
        .and_then(|v| v.as_array())
        .is_some_and(|list| {
            (1..=MAX_RECIPIENTS).contains(&list.len())
                && list
                    .iter()
                    .all(|r| r.as_str().is_some_and(|s| !s.trim().is_empty()))
        })
}

/// The sentence a refused send carries; it is also the resolver's error.
const NOT_PUBLIC: &str = "the webhook address is not a public address";

/// Whether `ip` is a public address. A webhook goes to a public address only: everything
/// else is a server-side request forgery target from inside the cluster.
///
/// The ranges are written by hand because `IpAddr::is_global` and `is_shared` are not
/// stable. An IPv4-mapped IPv6 address answers for its IPv4 part.
fn is_public(ip: std::net::IpAddr) -> bool {
    use std::net::IpAddr;
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_unspecified()
                || v4.is_loopback()
                // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
                || v4.is_private()
                // 169.254.0.0/16
                || v4.is_link_local()
                // 100.64.0.0/10, the shared address space
                || (o[0] == 100 && (64..=127).contains(&o[1]))
                || v4.is_broadcast()
                || v4.is_multicast()
                // The documentation ranges: 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24.
                || (o[0] == 192 && o[1] == 0 && o[2] == 2)
                || (o[0] == 198 && o[1] == 51 && o[2] == 100)
                || (o[0] == 203 && o[1] == 0 && o[2] == 113))
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_public(IpAddr::V4(v4));
            }
            let seg = v6.segments();
            !(v6.is_unspecified()
                || v6.is_loopback()
                || v6.is_multicast()
                // fc00::/7, the unique local addresses
                || (seg[0] & 0xfe00) == 0xfc00
                // fe80::/10, the link-local addresses
                || (seg[0] & 0xffc0) == 0xfe80
                // 2001:db8::/32, the documentation range
                || (seg[0] == 0x2001 && seg[1] == 0x0db8))
        }
    }
}

/// Whether `url` names a public host. reqwest does not ask the resolver about an IP
/// literal, so [`webhook_config`] and [`post_webhook`] check one themselves.
fn url_is_public(url: &str) -> bool {
    match url::Url::parse(url).map(|u| u.host().map(|h| h.to_owned())) {
        Ok(Some(url::Host::Ipv4(ip))) => is_public(std::net::IpAddr::V4(ip)),
        Ok(Some(url::Host::Ipv6(ip))) => is_public(std::net::IpAddr::V6(ip)),
        Ok(_) => true,
        Err(_) => false,
    }
}

/// The host names an administrator named in `NOVA_WEBHOOK_ALLOW_HOSTS` (comma-separated,
/// case-insensitive): a receiver inside the cluster keeps all its addresses.
fn allowed_hosts() -> Vec<String> {
    std::env::var("NOVA_WEBHOOK_ALLOW_HOSTS")
        .unwrap_or_default()
        .split(',')
        .map(|host| host.trim().to_lowercase())
        .filter(|host| !host.is_empty())
        .collect()
}

/// The resolver the webhook client uses: a name keeps only its public addresses, unless an
/// administrator allowed it in [`allowed_hosts`].
struct PublicOnly {
    allow: Vec<String>,
}

impl reqwest::dns::Resolve for PublicOnly {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        let allowed = self.allow.iter().any(|a| *a == host.to_lowercase());
        Box::pin(async move {
            let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                .await?
                .filter(|addr| allowed || is_public(addr.ip()))
                .collect();
            if addrs.is_empty() {
                return Err(NOT_PUBLIC.into());
            }
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Nova's webhook client: public addresses only and no redirects. It does not trust the
/// platform CA, so `NOVA_TRINO_CA_PEM` never widens what a webhook may reach.
pub(crate) fn webhook_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .dns_resolver(Arc::new(PublicOnly {
                allow: allowed_hosts(),
            }))
            .build()
            .expect("building Nova's webhook client")
    })
}

/// `{"url": "http(s)://…"}`, at most [`MAX_URL_CHARS`] characters and parseable, and to a
/// public address.
fn webhook_config(config: &serde_json::Value) -> bool {
    let Some(raw) = config.get("url").and_then(|v| v.as_str()) else {
        return false;
    };
    raw.chars().count() <= MAX_URL_CHARS
        && matches!(url::Url::parse(raw), Ok(u) if matches!(u.scheme(), "http" | "https"))
        && url_is_public(raw)
}

/// Trim the text fields and check the address, so a caller gets a sentence instead of
/// a database error. The sentences are answered `400` by [`create`].
fn validate(req: NewDestination) -> Result<NewDestination, String> {
    if req.kind != "email" && req.kind != "webhook" {
        return Err("kind must be email or webhook".into());
    }
    if req.kind == "email" && !email_config(&req.config) {
        return Err(format!(
            "an email destination needs config.recipients: 1 to {MAX_RECIPIENTS} addresses"
        ));
    }
    if req.kind == "webhook" && !webhook_config(&req.config) {
        return Err("a webhook destination needs config.url with an http or https address".into());
    }
    let name = req.name.trim().to_string();
    if name.is_empty() || name.chars().count() > MAX_NAME_CHARS {
        return Err(format!("name must be 1 to {MAX_NAME_CHARS} characters"));
    }
    let owner_group = validate_group(&req.owner_group)?;
    if req.viewers.len() > 16 {
        return Err("viewers holds at most 16 groups".into());
    }
    let mut viewers = Vec::with_capacity(req.viewers.len());
    for group in &req.viewers {
        match validate_group(group) {
            Ok(g) => viewers.push(g),
            Err(_) => {
                return Err(format!(
                    "viewers holds `{group}`, which is not a group name"
                ));
            }
        }
    }
    Ok(NewDestination {
        owner_group,
        name,
        kind: req.kind,
        config: req.config,
        viewers,
    })
}

/// Whether `principal` may work with destinations owned by `group`: a member of the
/// group, an administrator, or — in `dev` mode, where there is one person — anybody.
fn member_of(group: &str, principal: Option<&Principal>) -> bool {
    principal.is_none_or(|p| p.identity.is_admin || p.identity.groups.iter().any(|g| g == group))
}

/// Whether `principal` may see this destination: the dev mode rule above, its owner
/// group, or an administrator.
///
/// `_dev_user` keeps the shape the other group-facing resources use; in `dev` mode the
/// static user is the only person, so it belongs to every group.
// The handlers filter in SQL (one round trip); the alert slice (b) asks row by row.
#[allow(dead_code)]
pub fn may_see(destination: &Destination, principal: Option<&Principal>, _dev_user: &str) -> bool {
    member_of(&destination.owner_group, principal)
        || principal.is_some_and(|p| {
            p.identity
                .groups
                .iter()
                .any(|g| destination.viewers.iter().any(|v| v == g))
        })
}

/// `GET /api/destinations` — the caller's groups' destinations, newest first.
///
/// `?group=<group>` narrows to one group and answers `403` for a group the caller is
/// not in; the group rule here is the same one [`create`] applies.
#[utoipa::path(
    get,
    operation_id = "destinations_list",
    path = "/api/destinations",
    tag = "public",
    description = "The notification destinations of the caller's groups, newest first. `?group=<group>` narrows to one group the caller belongs to. Times are epoch milliseconds.",
    params(
        ("group" = Option<String>, Query, description = "Only this group's destinations"),
    ),
    responses(
        (status = 200, description = "The caller's groups' destinations", body = Vec<Destination>),
        (status = 403, description = "The caller is not a member of the requested group", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn list(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let principal = caller.as_ref().map(|c| &c.0);
    let narrow = params
        .get("group")
        .map(|g| g.trim().to_string())
        .filter(|g| !g.is_empty());
    if let Some(group) = &narrow
        && !member_of(group, principal)
    {
        return error(
            StatusCode::FORBIDDEN,
            &format!("you are not a member of the group {group}"),
        );
    }
    let groups: Vec<String> = principal
        .map(|p| p.identity.groups.clone())
        .unwrap_or_default();
    // `$1` carries the dev-mode and administrator case: every group is theirs.
    let see_all = principal.is_none_or(|p| p.identity.is_admin);
    let sql = "SELECT id, owner_group, name, kind, config, viewers, created_at, updated_at \
               FROM destination \
               WHERE ($1::boolean OR owner_group = ANY($2) OR viewers && $2) \
                 AND ($3::text IS NULL OR owner_group = $3) \
               ORDER BY created_at DESC, id DESC";
    match sqlx::query_as::<_, Destination>(sql)
        .bind(see_all)
        .bind(groups)
        .bind(narrow)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => Json(rows).into_response(),
        Err(e) => store_failed(e),
    }
}

/// `POST /api/destinations` — `201` with the stored row.
#[utoipa::path(
    post,
    operation_id = "destinations_create",
    path = "/api/destinations",
    tag = "public",
    description = "Create a notification destination for one of the caller's groups. An `email` destination carries `config.recipients` (1 to 20 addresses); a `webhook` destination carries `config.url` (http or https). Nothing here sends anything. A webhook must be a public address; Nova never follows a redirect.",
    request_body = NewDestination,
    responses(
        (status = 201, description = "The stored row", body = Destination),
        (status = 400, description = "The kind, the address, the name or the group is not valid", body = crate::openapi::ErrorResponse),
        (status = 403, description = "The caller is not a member of the owner group", body = crate::openapi::ErrorResponse),
        (status = 409, description = "A destination with this name already exists in the group", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn create(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    // The body extractor has to be last: axum consumes the request for it.
    ApiJson(body): ApiJson<NewDestination>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let new = match validate(body) {
        Ok(new) => new,
        Err(message) => return error(StatusCode::BAD_REQUEST, &message),
    };
    if !member_of(&new.owner_group, caller.as_ref().map(|c| &c.0)) {
        return error(
            StatusCode::FORBIDDEN,
            &format!("you are not a member of the group {}", new.owner_group),
        );
    }
    // Epoch milliseconds, the unit `Job.started_at` uses. `chrono` is built without its
    // `clock` feature (workspace `Cargo.toml`), so `Utc::now()` is not available.
    let now = crate::jobs::now_ms() as i64;
    let duplicate = format!(
        "a destination named {} already exists in {}",
        new.name, new.owner_group
    );
    let sql = "INSERT INTO destination (owner_group, name, kind, config, viewers, created_at, updated_at) \
               VALUES ($1, $2, $3, $4, $5, $6, $6) \
               RETURNING id, owner_group, name, kind, config, viewers, created_at, updated_at";
    match sqlx::query_as::<_, Destination>(sql)
        .bind(&new.owner_group)
        .bind(&new.name)
        .bind(&new.kind)
        .bind(&new.config)
        .bind(&new.viewers)
        .bind(now)
        .fetch_one(pool)
        .await
    {
        Ok(row) => (StatusCode::CREATED, Json(row)).into_response(),
        // The UNIQUE (owner_group, name) the table declares, for the race the check
        // before the insert cannot close.
        Err(e) if is_unique_violation(&e) => error(StatusCode::CONFLICT, &duplicate),
        Err(e) => store_failed(e),
    }
}

/// `DELETE /api/destinations/{id}` — `204`; `404` for another group's id or none.
///
/// No reference check yet: alerts do not exist, so nothing can point at a destination.
#[utoipa::path(
    delete,
    operation_id = "destinations_remove",
    path = "/api/destinations/{id}",
    tag = "public",
    description = "Delete one of the caller's groups' notification destinations.",
    params(("id" = i64, Path, description = "The destination's id")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "No such destination, or it is not the caller's", body = crate::openapi::ErrorResponse),
        (status = 400, description = "The id is not a number", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn remove(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Path(id): Path<i64>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let principal = caller.as_ref().map(|c| &c.0);
    let groups: Vec<String> = principal
        .map(|p| p.identity.groups.clone())
        .unwrap_or_default();
    let see_all = principal.is_none_or(|p| p.identity.is_admin);
    let done = sqlx::query(
        "DELETE FROM destination WHERE id = $1 AND ($2::boolean OR owner_group = ANY($3))",
    )
    .bind(id)
    .bind(see_all)
    .bind(groups)
    .execute(pool)
    .await;
    match done {
        Ok(done) if done.rows_affected() == 1 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => not_found(),
        Err(e) => store_failed(e),
    }
}

/// What one webhook attempt did; `detail` must never contain the URL or a header value.
#[derive(Debug, PartialEq)]
pub(crate) struct DeliveryOutcome {
    /// `delivered` or `failed`, the values the table's CHECK allows.
    pub status: &'static str,
    /// The webhook's answer, when there was one.
    pub http_status: Option<u16>,
    /// One sentence for the person who asked; never `reqwest::Error::to_string()`, which
    /// embeds the URL.
    pub detail: String,
}

/// The fixed sentence a test message carries, so a receiver can recognise it.
const TEST_MESSAGE: &str = "stardelt test message";
/// How long a webhook has to answer before the attempt counts as failed.
const DELIVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// POST the fixed test message to `url`, with a 5-second bound. The sentence in `detail`
/// is Nova's own; `reqwest::Error::to_string()` is not used because it embeds the URL.
///
/// No retry and no backoff: the alert sender owns that loop (slice c and later).
pub(crate) async fn deliver_webhook(
    client: &reqwest::Client,
    url: &str,
    destination: &str,
) -> DeliveryOutcome {
    let body = serde_json::json!({ "text": TEST_MESSAGE, "destination": destination });
    post_webhook(client, url, &body).await
}

/// POST any body to `url`, with the same 5-second bound and the same sentences as the test
/// message. The pipeline failure notice (`pipeline_notice.rs`) brings its own document.
pub(crate) async fn post_webhook(
    client: &reqwest::Client,
    url: &str,
    body: &serde_json::Value,
) -> DeliveryOutcome {
    // Rows stored before the public-address rule existed reach this path too.
    if !url_is_public(url) {
        return DeliveryOutcome {
            status: "failed",
            http_status: None,
            detail: NOT_PUBLIC.into(),
        };
    }
    let sent = client
        .post(url)
        .timeout(DELIVERY_TIMEOUT)
        .json(body)
        .send()
        .await;
    match sent {
        Ok(response) => {
            let status = response.status();
            DeliveryOutcome {
                status: if status.is_success() {
                    "delivered"
                } else {
                    "failed"
                },
                http_status: Some(status.as_u16()),
                detail: format!("HTTP {status}"),
            }
        }
        Err(e) if e.is_timeout() => DeliveryOutcome {
            status: "failed",
            http_status: None,
            detail: "the webhook did not answer within 5 seconds".into(),
        },
        Err(e) if e.is_connect() => DeliveryOutcome {
            status: "failed",
            http_status: None,
            detail: "the webhook was unreachable".into(),
        },
        Err(_) => DeliveryOutcome {
            status: "failed",
            http_status: None,
            detail: "the webhook request failed".into(),
        },
    }
}

/// Whether an attempt counts as delivered. `DestinationTest::ok` is exactly this.
fn is_delivered(outcome: &DeliveryOutcome) -> bool {
    outcome.status == "delivered"
}

/// `POST /api/destinations/{id}/test` answer: what the attempt did.
#[derive(Serialize, Debug, PartialEq, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DestinationTest {
    /// True only for `delivered`.
    pub ok: bool,
    /// The webhook's HTTP status, when it answered.
    pub status: Option<u16>,
    /// One sentence; never the URL or a header value.
    pub detail: String,
}

/// The one delivery record this module writes; `subject_kind` is always `test` here.
const INSERT_DELIVERY: &str = "INSERT INTO destination_delivery \
     (destination_id, subject_kind, status, http_status, detail, attempted_at) \
     VALUES ($1, 'test', $2, $3, $4, $5)";

/// `POST /api/destinations/{id}/test` — send one test message to a webhook destination
/// and record the attempt.
///
/// An email destination answers `400` until the platform's SMTP relay is configured; an
/// id that does not exist, or one the caller may not see, answers the module's `404`
/// rather than a `403` that would confirm the id. A failed delivery record is logged and
/// the delivery result still answered: the message was already sent.
#[utoipa::path(
    post,
    operation_id = "destinations_test",
    path = "/api/destinations/{id}/test",
    tag = "public",
    description = "Send one test message to a webhook destination and record the attempt. An email destination answers 400 until the platform's SMTP relay exists. The answer never contains the webhook's URL.",
    params(("id" = i64, Path, description = "The destination's id")),
    responses(
        (status = 200, description = "What the attempt did", body = DestinationTest),
        (status = 400, description = "An email destination, or a config without a usable url", body = crate::openapi::ErrorResponse),
        (status = 404, description = "No such destination, or it is not the caller's", body = crate::openapi::ErrorResponse),
        (status = 503, description = "The state store is not configured", body = crate::openapi::ErrorResponse),
        (status = 401, description = "Not signed in", body = crate::openapi::ErrorResponse),
    )
)]
pub async fn test(
    State(s): State<Arc<AppState>>,
    caller: Option<Extension<Principal>>,
    Path(id): Path<i64>,
) -> Response {
    let Some(pool) = s.db.as_ref() else {
        return unavailable();
    };
    let found = sqlx::query_as::<_, Destination>(
        "SELECT id, owner_group, name, kind, config, viewers, created_at, updated_at \
         FROM destination WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await;
    let row = match found {
        Ok(Some(row)) => row,
        Ok(None) => return not_found(),
        Err(e) => return store_failed(e),
    };
    if !member_of(&row.owner_group, caller.as_ref().map(|c| &c.0)) {
        return not_found();
    }
    if row.kind == "email" {
        return error(
            StatusCode::BAD_REQUEST,
            "email delivery needs the platform's SMTP relay, which is not configured",
        );
    }
    let Some(url) = row.config.get("url").and_then(|v| v.as_str()) else {
        return error(
            StatusCode::BAD_REQUEST,
            "a webhook destination needs config.url with an http or https address",
        );
    };
    let outcome = deliver_webhook(
        webhook_client(),
        url,
        &format!("{}/{}", row.owner_group, row.name),
    )
    .await;
    // Epoch milliseconds, like `create` (`chrono` has no `clock` feature here).
    let attempted_at = crate::jobs::now_ms() as i64;
    if let Err(e) = sqlx::query(INSERT_DELIVERY)
        .bind(row.id)
        .bind(outcome.status)
        .bind(outcome.http_status.map(i32::from))
        .bind(&outcome.detail)
        .bind(attempted_at)
        .execute(pool)
        .await
    {
        tracing::warn!(error = %e, "destination delivery record failed");
    }
    Json(DestinationTest {
        ok: is_delivered(&outcome),
        status: outcome.http_status,
        detail: outcome.detail,
    })
    .into_response()
}

/// PostgreSQL's `unique_violation`.
fn is_unique_violation(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(|d| d.code())
        .is_some_and(|code| code == "23505")
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

    fn email(owner_group: &str, name: &str, recipients: serde_json::Value) -> NewDestination {
        NewDestination {
            owner_group: owner_group.into(),
            name: name.into(),
            kind: "email".into(),
            config: serde_json::json!({ "recipients": recipients }),
            viewers: Vec::new(),
        }
    }

    fn webhook(owner_group: &str, name: &str, url: &str) -> NewDestination {
        NewDestination {
            owner_group: owner_group.into(),
            name: name.into(),
            kind: "webhook".into(),
            config: serde_json::json!({ "url": url }),
            viewers: Vec::new(),
        }
    }

    fn destination(owner_group: &str) -> Destination {
        Destination {
            id: 1,
            owner_group: owner_group.into(),
            name: "on call".into(),
            kind: "webhook".into(),
            config: serde_json::json!({ "url": "https://example.com/hook" }),
            viewers: Vec::new(),
            created_at: 0,
            updated_at: 0,
        }
    }

    #[test]
    fn webhook_refuses_non_public_addresses() {
        let refused = [
            "http://127.0.0.1/x",
            "http://10.1.2.3/x",
            "http://169.254.169.254/x",
            "http://[::1]/x",
            "http://[fd00::1]/x",
        ];
        for url in refused {
            assert!(
                !webhook_config(&serde_json::json!({ "url": url })),
                "a webhook to {url} must be refused"
            );
        }
        assert!(webhook_config(
            &serde_json::json!({ "url": "https://example.com/hook" })
        ));
        assert!(is_public("8.8.8.8".parse().unwrap()));
        assert!(!is_public("100.64.0.1".parse().unwrap()));
    }

    #[test]
    fn validate_trims_and_accepts_an_email_and_a_webhook() {
        let ok = validate(email(
            "  analytics  ",
            "  on call  ",
            serde_json::json!(["a@b.c"]),
        ))
        .unwrap();
        assert_eq!(ok.owner_group, "analytics");
        assert_eq!(ok.name, "on call");
        assert_eq!(ok.kind, "email");
        assert_eq!(ok.config["recipients"][0], "a@b.c");

        let ok = validate(webhook("analytics", "pager", "https://example.com/hook")).unwrap();
        assert_eq!(ok.kind, "webhook");
        assert_eq!(ok.config["url"], "https://example.com/hook");

        // Besides "a@b.c" and "https" the limits themselves are accepted at the edge.
        let twenty: Vec<String> = (0..20).map(|i| format!("p{i}@b.c")).collect();
        assert!(validate(email("g", "n", serde_json::json!(twenty))).is_ok());
        let long = format!(
            "https://{}.example.com/{}",
            "a".repeat(100),
            "b".repeat(1900)
        );
        assert!(long.chars().count() <= MAX_URL_CHARS);
        assert!(validate(webhook("g", "n", &long)).is_ok());
        assert!(validate(webhook("g", "n", "http://example.com/hook")).is_ok());
    }

    #[test]
    fn validate_rejects_an_unknown_kind() {
        let mut req = webhook("g", "n", "https://example.com/hook");
        req.kind = "smtp".into();
        assert_eq!(validate(req).unwrap_err(), "kind must be email or webhook");
    }

    #[test]
    fn validate_rejects_a_bad_email_config() {
        let message = "an email destination needs config.recipients: 1 to 20 addresses";
        let err = |r| validate(r).unwrap_err();
        assert_eq!(err(email("g", "n", serde_json::json!([]))), message);
        assert_eq!(
            err(email("g", "n", serde_json::json!(["a@b.c", "  "]))),
            message
        );
        assert_eq!(
            err(email("g", "n", serde_json::json!(["a@b.c", 7]))),
            message
        );
        let many: Vec<String> = (0..21).map(|i| format!("p{i}@b.c")).collect();
        assert_eq!(err(email("g", "n", serde_json::json!(many))), message);
        // Not the shape at all: no `recipients`, or a body that is not an object.
        assert_eq!(err(email("g", "n", serde_json::Value::Null)), message);
        let mut req = webhook("g", "n", "https://example.com/hook");
        req.kind = "email".into();
        assert_eq!(err(req), message);
    }

    #[test]
    fn validate_rejects_a_bad_webhook_config() {
        let message = "a webhook destination needs config.url with an http or https address";
        let err = |r| validate(r).unwrap_err();
        assert_eq!(err(webhook("g", "n", "")), message);
        assert_eq!(err(webhook("g", "n", "example.com/hook")), message);
        assert_eq!(err(webhook("g", "n", "ftp://example.com/hook")), message);
        assert_eq!(err(webhook("g", "n", "javascript:alert(1)")), message);
        let too_long = format!("https://example.com/{}", "b".repeat(MAX_URL_CHARS));
        assert_eq!(err(webhook("g", "n", &too_long)), message);
        let mut req = email("g", "n", serde_json::json!(["a@b.c"]));
        req.kind = "webhook".into();
        assert_eq!(err(req), message);
    }

    #[test]
    fn validate_rejects_a_bad_name_and_reuses_the_group_sentence() {
        let err = |r| validate(r).unwrap_err();
        assert_eq!(
            err(email("g", "   ", serde_json::json!(["a@b.c"]))),
            "name must be 1 to 64 characters"
        );
        assert_eq!(
            err(email("g", &"n".repeat(65), serde_json::json!(["a@b.c"]))),
            "name must be 1 to 64 characters"
        );
        assert!(validate(email("g", &"n".repeat(64), serde_json::json!(["a@b.c"]))).is_ok());
        let bad_group = "group must be 1 to 64 characters, letters, digits, dot, underscore or \
                         dash, starting and ending with a letter or digit";
        assert_eq!(
            err(email("-g", "n", serde_json::json!(["a@b.c"]))),
            bad_group
        );
        assert_eq!(err(email("", "n", serde_json::json!(["a@b.c"]))), bad_group);
        assert_eq!(
            err(email(&"g".repeat(65), "n", serde_json::json!(["a@b.c"]))),
            bad_group
        );
    }

    #[test]
    fn may_see_is_the_owner_group_or_an_administrator_and_everything_in_dev_mode() {
        let owner = destination("analytics");
        let member = principal("alice", &["analytics"], false);
        let outsider = principal("bob", &["sales"], false);
        let admin = principal("root", &["sales"], true);
        assert!(may_see(&owner, Some(&member), "stardelt-dev"));
        assert!(!may_see(&owner, Some(&outsider), "stardelt-dev"));
        assert!(may_see(&owner, Some(&admin), "stardelt-dev"));
        assert!(may_see(&owner, None, "stardelt-dev"));
    }

    #[test]
    fn a_viewer_sees_a_destination_and_cannot_send_it() {
        let mut row = destination("demo");
        row.viewers = vec!["visitors".into()];
        let visitor = principal("demo-person", &["visitors"], false);
        let member = principal("alice", &["demo"], false);
        assert!(may_see(&row, Some(&visitor), "stardelt-dev"));
        assert!(may_see(&row, Some(&member), "stardelt-dev"));
        assert!(
            !member_of("demo", Some(&visitor)),
            "a viewer is not a member: no test message, no delete"
        );
        assert!(member_of("demo", Some(&member)));
    }

    #[test]
    fn validate_accepts_viewers_and_refuses_a_bad_group() {
        let mut req = webhook("demo", "demo-feed", "https://ntfy.sh/demo-4711");
        req.viewers = vec!["visitors".into(), "analytics".into()];
        assert_eq!(validate(req).unwrap().viewers, ["visitors", "analytics"]);
        let mut bad = webhook("demo", "demo-feed", "https://ntfy.sh/demo-4711");
        bad.viewers = vec!["visitors".into(), "bad group!".into()];
        assert_eq!(
            validate(bad).unwrap_err(),
            "viewers holds `bad group!`, which is not a group name"
        );
        let mut many = webhook("demo", "demo-feed", "https://ntfy.sh/demo-4711");
        many.viewers = vec!["visitors".into(); 17];
        assert_eq!(
            validate(many).unwrap_err(),
            "viewers holds at most 16 groups"
        );
    }

    // --- the test message (slice b) -------------------------------------------------
    // A local mock receiver on `127.0.0.1:0`, the pattern `airflow.rs` uses for its fake.
    // The URL carries a canary so a test can prove `detail` never repeats it.

    /// The unique string every mock URL below carries; no `detail` may contain it.
    const CANARY: &str = "canary-destination-url";
    const DESTINATION: &str = "analytics/revenue";

    /// One mock receiver answering `status` after `delay`; returns its base URL.
    async fn mock(status: StatusCode, delay: std::time::Duration) -> String {
        let app = axum::Router::new().route(
            "/{*rest}",
            axum::routing::any(move |body: String| async move {
                tokio::time::sleep(delay).await;
                (status, body)
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        // A name, not the IP literal: an IP literal is refused at send time, and the mock
        // receiver is only ever reached by this test.
        format!("http://localhost:{}/{CANARY}", addr.port())
    }

    fn assert_no_canary(outcome: &DeliveryOutcome) {
        assert!(
            !outcome.detail.contains(CANARY),
            "detail names the URL: {}",
            outcome.detail
        );
    }

    #[tokio::test]
    async fn deliver_webhook_posts_the_fixed_test_message() {
        // Record what the receiver really got: method, path and body.
        type Seen = Arc<std::sync::Mutex<Option<(String, String, serde_json::Value)>>>;
        let seen: Seen = Arc::new(std::sync::Mutex::new(None));
        let recorder = Arc::clone(&seen);
        let app = axum::Router::new().route(
            "/{*rest}",
            axum::routing::post(
                move |method: axum::http::Method,
                      uri: axum::http::Uri,
                      axum::Json(body): axum::Json<serde_json::Value>| {
                    let recorder = Arc::clone(&recorder);
                    async move {
                        *recorder.lock().unwrap() =
                            Some((method.to_string(), uri.path().to_string(), body));
                        StatusCode::NO_CONTENT
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let url = format!("http://localhost:{}/{CANARY}", addr.port());
        let outcome = deliver_webhook(&reqwest::Client::new(), &url, DESTINATION).await;
        assert_eq!(outcome.status, "delivered");
        assert_eq!(outcome.http_status, Some(204));
        assert!(outcome.detail.starts_with("HTTP 204"), "{}", outcome.detail);
        assert_no_canary(&outcome);
        let (method, path, body) = seen
            .lock()
            .unwrap()
            .clone()
            .expect("the receiver was called");
        assert_eq!(method, "POST");
        assert_eq!(path, format!("/{CANARY}"));
        assert_eq!(
            body,
            serde_json::json!({ "text": "stardelt test message", "destination": DESTINATION })
        );
    }

    #[tokio::test]
    async fn deliver_webhook_reports_a_failed_response() {
        let url = mock(StatusCode::INTERNAL_SERVER_ERROR, std::time::Duration::ZERO).await;
        let outcome = deliver_webhook(&reqwest::Client::new(), &url, DESTINATION).await;
        assert_eq!(outcome.status, "failed");
        assert_eq!(outcome.http_status, Some(500));
        assert!(outcome.detail.starts_with("HTTP 500"), "{}", outcome.detail);
        assert_no_canary(&outcome);
    }

    #[tokio::test]
    async fn deliver_webhook_gives_up_after_the_bound() {
        let url = mock(StatusCode::NO_CONTENT, std::time::Duration::from_secs(6)).await;
        let outcome = deliver_webhook(&reqwest::Client::new(), &url, DESTINATION).await;
        assert_eq!(outcome.status, "failed");
        assert_eq!(outcome.http_status, None);
        assert_eq!(
            outcome.detail,
            "the webhook did not answer within 5 seconds"
        );
        assert_no_canary(&outcome);
    }

    #[tokio::test]
    async fn a_connect_error_names_no_url() {
        // The canary sits in the host part and does not resolve, so this is the
        // unreachable branch and the error that carries the URL is not echoed.
        let url = format!("http://{CANARY}.invalid/hook");
        let outcome = deliver_webhook(&reqwest::Client::new(), &url, DESTINATION).await;
        assert_eq!(outcome.status, "failed");
        assert_eq!(outcome.http_status, None);
        assert_eq!(outcome.detail, "the webhook was unreachable");
        assert_no_canary(&outcome);
    }

    #[test]
    fn ok_is_true_only_for_a_delivered_attempt() {
        let delivered = DeliveryOutcome {
            status: "delivered",
            http_status: Some(204),
            detail: "HTTP 204 No Content".into(),
        };
        let failed = DeliveryOutcome {
            status: "failed",
            http_status: None,
            detail: "the webhook was unreachable".into(),
        };
        assert!(is_delivered(&delivered));
        assert!(!is_delivered(&failed));
    }
}
