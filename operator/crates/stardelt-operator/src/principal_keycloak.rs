//! Keycloak client convergence for `ServicePrincipal` in `identity.mode: platform`.
//!
//! Pure builders ([`client_payload`], [`credentials_secret`]) are tested without a cluster.
//! [`ensure_principal_client`] calls the Keycloak Service directly (never through the API
//! server's service proxy: kube's own auth layer replaces an `Authorization` header with the
//! operator's ServiceAccount token, so the administrator's bearer token never arrived — #856).
//! It does not create the owner group or grant admin roles.
//!
//! Secret generation uses [`crate::keycloak::generate_secret`] (48 lower-case hex characters),
//! not the platform script's 40 mixed-case alphanumerics: Keycloak accepts any non-empty
//! value and one generator means one place to audit.

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use k8s_openapi::{
    ByteString,
    api::core::v1::{ConfigMap, Secret},
};
use kube::{Api, api::DeleteParams};

use crate::{
    controller::Context,
    crd::{PlatformInstance, PrincipalAudience, ServicePrincipalSpec, principal_client_id},
    keycloak::{self, ENV_CONFIG_MAP},
    objects,
};

/// The Keycloak client representation a `ServicePrincipal` wants, exactly as
/// `stardelt-platform/scripts/keycloak-sync-principal.sh` builds it (step 6 there):
/// confidential, service-account only, one audience mapper per audience, a `groups`
/// mapper, and `stardelt-managed: principal` so a delete can find it again.
///
/// The caller inserts `"secret"` into this object before a create or a rotate —
/// [`client_payload`] itself must not contain a secret.
pub fn client_payload(name: &str, spec: &ServicePrincipalSpec) -> serde_json::Value {
    let client_id = principal_client_id(name);
    let mut mappers: Vec<serde_json::Value> = spec
        .audiences
        .iter()
        .map(|aud| {
            let aud = audience_value(*aud);
            serde_json::json!({
                "name": format!("aud-{aud}"),
                "protocol": "openid-connect",
                "protocolMapper": "oidc-audience-mapper",
                "consentRequired": false,
                "config": {
                    "included.custom.audience": aud,
                    "id.token.claim": "false",
                    "access.token.claim": "true",
                    "introspection.token.claim": "true"
                }
            })
        })
        .collect();
    mappers.push(serde_json::json!({
        "name": "groups",
        "protocol": "openid-connect",
        "protocolMapper": "oidc-group-membership-mapper",
        "consentRequired": false,
        "config": {
            "claim.name": "groups",
            "full.path": "false",
            "id.token.claim": "true",
            "access.token.claim": "true",
            "userinfo.token.claim": "true",
            "introspection.token.claim": "true"
        }
    }));

    serde_json::json!({
        "clientId": client_id,
        "name": format!("Service principal {client_id}"),
        "description": format!(
            "Service principal owned by group {} (scheduled jobs, D20/D21).",
            spec.owner_group
        ),
        "enabled": true,
        "protocol": "openid-connect",
        "publicClient": false,
        "clientAuthenticatorType": "client-secret",
        "standardFlowEnabled": false,
        "implicitFlowEnabled": false,
        "directAccessGrantsEnabled": false,
        "serviceAccountsEnabled": true,
        "redirectUris": [],
        "webOrigins": [],
        // #334: Iceberg's OAuth2 refresh at 300 s fails with invalid_client; a token that
        // outlives a demo stream never needs to refresh (the jupyterhub client does the same).
        "attributes": {
            "stardelt-managed": "principal",
            "access.token.lifespan": "28800"
        },
        "protocolMappers": mappers
    })
}

fn audience_value(audience: PrincipalAudience) -> &'static str {
    match audience {
        PrincipalAudience::Trino => "trino",
        PrincipalAudience::Lakekeeper => "lakekeeper",
        PrincipalAudience::LakekeeperTrino => "lakekeeper-trino",
    }
}

/// Secret `sp-<name>` with the keys the platform's own clients carry.
///
/// `client-id` is `sp-<name>`, `client-secret` is `secret`, and `token-url` and `subject` let a
/// caller sign in as this principal; all as `ByteString`.
pub fn credentials_secret(
    name: &str,
    namespace: &str,
    secret: &str,
    token_url: &str,
    subject: &str,
) -> Secret {
    let client_id = principal_client_id(name);
    Secret {
        metadata: kube::api::ObjectMeta {
            name: Some(client_id.clone()),
            namespace: Some(namespace.into()),
            labels: Some(BTreeMap::from([
                (
                    "app.kubernetes.io/part-of".to_string(),
                    "stardelt".to_string(),
                ),
                (
                    "stardelt.io/managed-by".to_string(),
                    "stardelt-operator".to_string(),
                ),
            ])),
            ..Default::default()
        },
        data: Some(BTreeMap::from([
            ("client-id".to_string(), ByteString(client_id.into_bytes())),
            (
                "client-secret".to_string(),
                ByteString(secret.as_bytes().to_vec()),
            ),
            (
                "token-url".to_string(),
                ByteString(token_url.as_bytes().to_vec()),
            ),
            (
                "subject".to_string(),
                ByteString(subject.as_bytes().to_vec()),
            ),
        ])),
        ..Default::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientOutcome {
    Created { client_id: String, subject: String },
    Converged { client_id: String, subject: String },
    OwnerGroupMissing { group: String },
    Unavailable { message: String },
}

/// One Keycloak call, with everything the transport needs. `namespace` names the Keycloak
/// Service; `path` is the Keycloak path (for example `/admin/realms/stardelt/clients`).
#[derive(Clone, Debug)]
pub struct KeycloakRequest {
    pub namespace: String,
    pub method: String,
    pub path: String,
    pub token: Option<String>,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
    /// `KC_HOSTNAME` with its scheme stripped: Keycloak runs with `KC_PROXY_HEADERS=xforwarded`,
    /// so the transport sends it as `X-Forwarded-Host`.
    pub host: String,
}

/// The transport to Keycloak. Production goes to the Service; tests inject a recorder, the
/// way `HelmRunner` is injected (#856).
pub trait KeycloakTransport: Send + Sync {
    /// `Ok((status, body))` for any HTTP answer; `Err` only when no answer arrived.
    fn request(
        &self,
        request: KeycloakRequest,
    ) -> BoxFuture<'static, Result<(u16, String), String>>;
}

/// `http://keycloak.<namespace>.svc.cluster.local:8080<path>` — the Service the operator's own
/// `keycloak-ingress` NetworkPolicy admits on 8080 (`network_policy.rs::keycloak_ingress`).
pub fn keycloak_service_url(namespace: &str, path: &str) -> String {
    format!("http://keycloak.{namespace}.svc.cluster.local:8080{path}")
}

/// The production transport: one `reqwest` client with rustls, a 30 s timeout.
pub struct HttpKeycloakTransport;

impl KeycloakTransport for HttpKeycloakTransport {
    fn request(&self, r: KeycloakRequest) -> BoxFuture<'static, Result<(u16, String), String>> {
        Box::pin(async move {
            static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
            let client = CLIENT.get_or_init(|| {
                reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(30))
                    .build()
                    .expect("a reqwest client with rustls is always buildable")
            });
            let mut request = client.request(
                reqwest::Method::from_bytes(r.method.as_bytes())
                    .map_err(|e| format!("cannot build the Keycloak request: {e}"))?,
                keycloak_service_url(&r.namespace, &r.path),
            );
            if let Some(token) = &r.token {
                request = request.header("Authorization", format!("Bearer {token}"));
            }
            if let Some(content_type) = &r.content_type {
                request = request.header("Content-Type", content_type);
            }
            if !r.host.is_empty() {
                request = request
                    .header("X-Forwarded-Host", &r.host)
                    .header("X-Forwarded-Proto", "https");
            }
            if !r.body.is_empty() {
                request = request.body(r.body);
            }
            let response = request
                .send()
                .await
                .map_err(|e| format!("Keycloak is not reachable: {e}"))?;
            let status = response.status().as_u16();
            let body = response
                .text()
                .await
                .map_err(|e| format!("Keycloak is not reachable: {e}"))?;
            Ok((status, body))
        })
    }
}

/// The sentence a caller sees for a Keycloak answer. A 401 reads as a 401 (it was `answered 0:
/// ` while the request went through the API server proxy, which returned Keycloak's own body and
/// kube-rs turned it into a `Status` without a code — #856).
fn map_keycloak_error(status: u16, body: &str) -> String {
    if status == 404 {
        return "Keycloak is not installed in this namespace".to_string();
    }
    if status == 401 || status == 403 {
        return "Keycloak refused the administrator token".to_string();
    }
    let body = body.trim().chars().take(200).collect::<String>();
    format!("Keycloak answered {status}: {body}")
}

fn secret_text(secret: &Secret, key: &str) -> Option<String> {
    let bytes = secret.data.as_ref()?.get(key)?;
    let text = String::from_utf8_lossy(&bytes.0).trim().to_string();
    (!text.is_empty()).then_some(text)
}

fn url_encode_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(byte))
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// A master-realm token for the built-in administrator: Secret `keycloak-admin`,
/// keys `username` and `password`, `client_id=admin-cli`, `grant_type=password`.
async fn admin_token(ctx: &Context, namespace: &str) -> Result<String, String> {
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let admin = secrets
        .get_opt("keycloak-admin")
        .await
        .map_err(|e| format!("cannot read Secret keycloak-admin: {e}"))?
        .ok_or_else(|| "Secret keycloak-admin is missing".to_string())?;
    let username = secret_text(&admin, "username")
        .ok_or_else(|| "Secret keycloak-admin has no username".to_string())?;
    let password = secret_text(&admin, "password")
        .ok_or_else(|| "Secret keycloak-admin has no password".to_string())?;

    let form = format!(
        "grant_type=password&client_id=admin-cli&username={}&password={}",
        url_encode_component(&username),
        url_encode_component(&password)
    );
    let body = keycloak_request(
        ctx,
        namespace,
        "POST",
        "/realms/master/protocol/openid-connect/token",
        None,
        Some("application/x-www-form-urlencoded"),
        form.into_bytes(),
    )
    .await?;
    let parsed: serde_json::Value = serde_json::from_str(&body)
        .map_err(|_| "Keycloak returned a token response that is not JSON".to_string())?;
    parsed
        .get("access_token")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| "Keycloak returned no access_token".to_string())
}

/// One request to Keycloak. `token` is the administrator's bearer token (absent only for the
/// token endpoint). `KC_HOSTNAME` still travels as `X-Forwarded-Host` because Keycloak runs
/// with `KC_PROXY_HEADERS=xforwarded`; the transport adds it.
async fn keycloak_request(
    ctx: &Context,
    namespace: &str,
    method: &str,
    path: &str,
    token: Option<&str>,
    content_type: Option<&str>,
    body: Vec<u8>,
) -> Result<String, String> {
    let config_maps: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
    let config = config_maps
        .get_opt(ENV_CONFIG_MAP)
        .await
        .map_err(|e| format!("cannot read ConfigMap {ENV_CONFIG_MAP}: {e}"))?
        .ok_or_else(|| format!("ConfigMap {ENV_CONFIG_MAP} is missing"))?;
    let host = config
        .data
        .as_ref()
        .and_then(|d| d.get("KC_HOSTNAME"))
        .ok_or_else(|| format!("ConfigMap {ENV_CONFIG_MAP} has no KC_HOSTNAME"))?
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();
    let (status, text) = ctx
        .keycloak
        .request(KeycloakRequest {
            namespace: namespace.to_string(),
            method: method.to_string(),
            path: path.to_string(),
            token: token.map(str::to_string),
            content_type: content_type.map(str::to_string),
            body,
            // Host travels with the request so the transport needs no ConfigMap read.
            host,
        })
        .await?;
    if status >= 400 {
        return Err(map_keycloak_error(status, &text));
    }
    Ok(text)
}

/// The platform-mode instance that owns `namespace`, if any.
pub async fn platform_instance_for_namespace(
    ctx: &Context,
    namespace: &str,
) -> Result<Option<PlatformInstance>, kube::Error> {
    Ok(ctx
        .platforms
        .state()
        .into_iter()
        .find(|pi| pi.spec.namespace == namespace)
        .map(|pi| (*pi).clone()))
}

/// Make an existing realm issue the audience the Spark front checks (#263 slice A).
///
/// `--import-realm` imports a realm once, so a realm created by an earlier operator never gains
/// the `spark-connect` client or its two `aud-spark-connect` mappers; the front that
/// `ensure_spark` applies would then refuse every token. Idempotent: every object is read first,
/// so a second call writes nothing.
pub async fn ensure_spark_audience(ctx: &Context, namespace: &str) -> Result<(), String> {
    let token = admin_token(ctx, namespace).await?;

    // 1. The bearer-only client the front names as its audience (keycloak_realm.json).
    let clients = keycloak_request(
        ctx,
        namespace,
        "GET",
        "/admin/realms/stardelt/clients?clientId=spark-connect",
        Some(&token),
        None,
        Vec::new(),
    )
    .await?;
    let clients: serde_json::Value = serde_json::from_str(&clients)
        .map_err(|_| "Keycloak returned a client list that is not JSON".to_string())?;
    if clients.as_array().is_none_or(|list| list.is_empty()) {
        let client = serde_json::json!({
            "clientId": "spark-connect",
            "name": "Spark Connect",
            "enabled": true,
            "protocol": "openid-connect",
            "bearerOnly": true,
            "publicClient": false,
            "standardFlowEnabled": false,
            "implicitFlowEnabled": false,
            "directAccessGrantsEnabled": false,
            "serviceAccountsEnabled": false
        });
        keycloak_request(
            ctx,
            namespace,
            "POST",
            "/admin/realms/stardelt/clients",
            Some(&token),
            Some("application/json"),
            serde_json::to_vec(&client)
                .map_err(|e| format!("cannot encode the Keycloak request: {e}"))?,
        )
        .await?;
    }

    // 2. The audience mapper on the two clients a person signs in with.
    for client_id in ["jupyterhub", "stardelt-dev"] {
        let listed = keycloak_request(
            ctx,
            namespace,
            "GET",
            &format!("/admin/realms/stardelt/clients?clientId={client_id}"),
            Some(&token),
            None,
            Vec::new(),
        )
        .await?;
        let listed: serde_json::Value = serde_json::from_str(&listed)
            .map_err(|_| format!("Keycloak returned the {client_id} list that is not JSON"))?;
        let Some(uuid) = listed
            .as_array()
            .and_then(|list| list.first())
            .and_then(|c| c.get("id"))
            .and_then(|id| id.as_str())
        else {
            return Err(format!("Keycloak has no client {client_id}"));
        };
        let mappers_path = format!("/admin/realms/stardelt/clients/{uuid}/protocol-mappers/models");
        let mappers = keycloak_request(
            ctx,
            namespace,
            "GET",
            &mappers_path,
            Some(&token),
            None,
            Vec::new(),
        )
        .await?;
        let mappers: serde_json::Value = serde_json::from_str(&mappers)
            .map_err(|_| format!("Keycloak returned the {client_id} mappers that are not JSON"))?;
        let present = mappers.as_array().is_some_and(|list| {
            list.iter()
                .any(|m| m.get("name").and_then(|v| v.as_str()) == Some("aud-spark-connect"))
        });
        if !present {
            let mapper = serde_json::json!({
                "name": "aud-spark-connect",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-audience-mapper",
                "consentRequired": false,
                "config": {
                    "included.client.audience": "spark-connect",
                    "id.token.claim": "false",
                    "access.token.claim": "true",
                    "introspection.token.claim": "true"
                }
            });
            keycloak_request(
                ctx,
                namespace,
                "POST",
                &mappers_path,
                Some(&token),
                Some("application/json"),
                serde_json::to_vec(&mapper)
                    .map_err(|e| format!("cannot encode the Keycloak request: {e}"))?,
            )
            .await?;
        }
    }
    Ok(())
}

/// Delete the Keycloak client and the credentials Secret of one principal.
/// Mirrors `keycloak-sync-principal.sh --delete`: only a client carrying
/// `stardelt-managed: principal` may be removed; anything else is an error, not a guess.
pub async fn delete_principal_client(
    ctx: &Context,
    namespace: &str,
    name: &str,
) -> Result<(), String> {
    let client_id = principal_client_id(name);
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let instance = platform_instance_for_namespace(ctx, namespace)
        .await
        .map_err(|e| format!("cannot list PlatformInstances: {e}"))?;
    if instance
        .as_ref()
        .is_none_or(|pi| pi.spec.identity.mode != crate::crd::IdentityMode::Platform)
    {
        match secrets.delete(&client_id, &DeleteParams::default()).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => return Err(format!("cannot delete Secret {client_id}: {e}")),
        }
        return Ok(());
    }

    let token = admin_token(ctx, namespace).await?;
    let clients_json = keycloak_request(
        ctx,
        namespace,
        "GET",
        &format!("/admin/realms/stardelt/clients?clientId={client_id}"),
        Some(&token),
        None,
        Vec::new(),
    )
    .await?;
    let clients: Vec<serde_json::Value> = serde_json::from_str(&clients_json)
        .map_err(|_| "Keycloak returned a client list that is not JSON".to_string())?;
    if let Some(client) = clients.first() {
        let managed = client
            .get("attributes")
            .and_then(|a| a.get("stardelt-managed"))
            .and_then(|v| v.as_str());
        if managed != Some("principal") {
            return Err(format!(
                "refusing to delete client {client_id}: it does not carry stardelt-managed=principal"
            ));
        }
        let id = client
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("Keycloak client {client_id} has no id"))?;
        keycloak_request(
            ctx,
            namespace,
            "DELETE",
            &format!("/admin/realms/stardelt/clients/{id}"),
            Some(&token),
            None,
            Vec::new(),
        )
        .await?;
    }

    match secrets.delete(&client_id, &DeleteParams::default()).await {
        Ok(_) => {}
        Err(kube::Error::Api(e)) if e.code == 404 => {}
        Err(e) => return Err(format!("cannot delete Secret {client_id}: {e}")),
    }
    Ok(())
}

/// Converge the Keycloak client and Secret for one principal.
///
/// The Secret is the source of truth for the client secret: with a Secret, Keycloak is
/// updated to its value (a rotated or adopted principal keeps working); without one, a new
/// value is generated and written to the Secret only after Keycloak accepted it.
pub async fn ensure_principal_client(
    ctx: &Context,
    namespace: &str,
    name: &str,
    spec: &ServicePrincipalSpec,
) -> ClientOutcome {
    match ensure_principal_client_inner(ctx, namespace, name, spec).await {
        Ok(outcome) => outcome,
        Err(message) => ClientOutcome::Unavailable { message },
    }
}

async fn ensure_principal_client_inner(
    ctx: &Context,
    namespace: &str,
    name: &str,
    spec: &ServicePrincipalSpec,
) -> Result<ClientOutcome, String> {
    let client_id = principal_client_id(name);
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);

    // 1. Read Secret sp-<name> if present; take client-secret when both keys are present.
    let existing_secret = secrets
        .get_opt(&client_id)
        .await
        .map_err(|e| format!("cannot read Secret {client_id}: {e}"))?;
    let (known_secret, secret_needs_write) = match existing_secret.as_ref() {
        Some(secret) => {
            let cid = secret_text(secret, "client-id");
            let csec = secret_text(secret, "client-secret");
            match (cid, csec) {
                (Some(cid), Some(csec)) => (
                    Some(csec),
                    cid != client_id
                        || secret_text(secret, "token-url").is_none()
                        || secret_text(secret, "subject").is_none(),
                ),
                _ => (None, false),
            }
        }
        None => (None, false),
    };

    let token = admin_token(ctx, namespace).await?;

    // 2. GET clients?clientId=sp-<name>
    let clients_json = keycloak_request(
        ctx,
        namespace,
        "GET",
        &format!("/admin/realms/stardelt/clients?clientId={client_id}"),
        Some(&token),
        None,
        Vec::new(),
    )
    .await?;
    let clients: Vec<serde_json::Value> = serde_json::from_str(&clients_json)
        .map_err(|_| "Keycloak returned a client list that is not JSON".to_string())?;
    let existing_uuid = clients
        .first()
        .and_then(|c| c.get("id"))
        .and_then(|v| v.as_str())
        .map(str::to_string);

    // 4 (checked before mutations). Owner group must exist: never create it, and never
    // create or mutate a client for a group the IdP does not know.
    let groups_json = keycloak_request(
        ctx,
        namespace,
        "GET",
        &format!(
            "/admin/realms/stardelt/groups?search={}",
            url_encode_component(&spec.owner_group)
        ),
        Some(&token),
        None,
        Vec::new(),
    )
    .await?;
    let groups: Vec<serde_json::Value> = serde_json::from_str(&groups_json)
        .map_err(|_| "Keycloak returned a group list that is not JSON".to_string())?;
    let owner_group = groups.iter().find(|g| {
        g.get("name")
            .and_then(|v| v.as_str())
            .is_some_and(|n| n == spec.owner_group)
    });
    let Some(owner_group) = owner_group else {
        return Ok(ClientOutcome::OwnerGroupMissing {
            group: spec.owner_group.clone(),
        });
    };
    let owner_group_id = owner_group
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "the owner group has no id".to_string())?
        .to_string();

    // 3. Create or converge the client. The Secret is the source of truth for the secret.
    let (secret_value, created, uuid) = if let Some(uuid) = existing_uuid {
        if let Some(secret) = known_secret.as_deref() {
            let mut payload = client_payload(name, spec);
            payload["secret"] = serde_json::Value::String(secret.to_string());
            keycloak_request(
                ctx,
                namespace,
                "PUT",
                &format!("/admin/realms/stardelt/clients/{uuid}"),
                Some(&token),
                Some("application/json"),
                serde_json::to_vec(&payload)
                    .map_err(|e| format!("cannot encode the Keycloak request: {e}"))?,
            )
            .await?;
            (secret.to_string(), false, uuid)
        } else {
            (String::new(), false, uuid)
        }
    } else {
        let secret = match known_secret {
            Some(secret) => secret,
            None => keycloak::generate_secret()
                .map_err(|e| format!("cannot generate a client secret: {e}"))?,
        };
        let mut payload = client_payload(name, spec);
        payload["secret"] = serde_json::Value::String(secret.clone());
        keycloak_request(
            ctx,
            namespace,
            "POST",
            "/admin/realms/stardelt/clients",
            Some(&token),
            Some("application/json"),
            serde_json::to_vec(&payload)
                .map_err(|e| format!("cannot encode the Keycloak request: {e}"))?,
        )
        .await?;
        // POST returns 201 with no body through this transport: GET the uuid.
        let clients_json = keycloak_request(
            ctx,
            namespace,
            "GET",
            &format!("/admin/realms/stardelt/clients?clientId={client_id}"),
            Some(&token),
            None,
            Vec::new(),
        )
        .await?;
        let clients: Vec<serde_json::Value> = serde_json::from_str(&clients_json)
            .map_err(|_| "Keycloak returned a client list that is not JSON".to_string())?;
        let uuid = clients
            .first()
            .and_then(|c| c.get("id"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| format!("Keycloak created {client_id} but it cannot be found again"))?;
        (secret, true, uuid)
    };

    // 5. Service-account user membership: only the owner group.
    let sa_json = keycloak_request(
        ctx,
        namespace,
        "GET",
        &format!("/admin/realms/stardelt/clients/{uuid}/service-account-user"),
        Some(&token),
        None,
        Vec::new(),
    )
    .await?;
    let sa: serde_json::Value = serde_json::from_str(&sa_json)
        .map_err(|_| "Keycloak returned a service-account user that is not JSON".to_string())?;
    let uid = sa
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "the service-account user has no id".to_string())?;
    let subject = uid.to_string();

    let user_groups_json = keycloak_request(
        ctx,
        namespace,
        "GET",
        &format!("/admin/realms/stardelt/users/{uid}/groups"),
        Some(&token),
        None,
        Vec::new(),
    )
    .await?;
    let user_groups: Vec<serde_json::Value> = serde_json::from_str(&user_groups_json)
        .map_err(|_| "Keycloak returned a user group list that is not JSON".to_string())?;
    for group in &user_groups {
        let gid = group.get("id").and_then(|v| v.as_str()).unwrap_or("");
        if gid != owner_group_id && !gid.is_empty() {
            let _ = keycloak_request(
                ctx,
                namespace,
                "DELETE",
                &format!("/admin/realms/stardelt/users/{uid}/groups/{gid}"),
                Some(&token),
                None,
                Vec::new(),
            )
            .await;
        }
    }
    keycloak_request(
        ctx,
        namespace,
        "PUT",
        &format!("/admin/realms/stardelt/users/{uid}/groups/{owner_group_id}"),
        Some(&token),
        None,
        Vec::new(),
    )
    .await?;

    // 6. Write the Secret when the value is new, the client-id disagrees, or the
    // contract keys a caller reads (token-url, subject) are missing.
    if !secret_value.is_empty() && (created || secret_needs_write || existing_secret.is_none()) {
        let instance = platform_instance_for_namespace(ctx, namespace)
            .await
            .map_err(|e| format!("cannot list PlatformInstances: {e}"))?;
        let token_url = instance
            .as_ref()
            .and_then(|pi| crate::identity::issuer_url(&pi.spec).ok().flatten())
            .map(|issuer| format!("{issuer}/protocol/openid-connect/token"))
            .ok_or_else(|| {
                "the platform instance has no issuerUrl; cannot write token-url".to_string()
            })?;
        objects::apply(
            &ctx.client,
            namespace,
            &credentials_secret(name, namespace, &secret_value, &token_url, &subject),
            false,
        )
        .await
        .map_err(|e| format!("cannot write Secret {client_id}: {e}"))?;
    }

    if created {
        Ok(ClientOutcome::Created { client_id, subject })
    } else {
        Ok(ClientOutcome::Converged { client_id, subject })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helm::testing::RecordingRunner;
    use http::Request;
    use kube::client::Body;
    use std::sync::{Arc, Mutex};
    use tower::service_fn;

    fn spec_json(audiences: &[&str]) -> ServicePrincipalSpec {
        serde_json::from_value(serde_json::json!({
            "ownerGroup": "analytics",
            "audiences": audiences,
        }))
        .expect("spec")
    }

    #[test]
    fn the_credentials_secret_carries_the_platform_keys() {
        let s = credentials_secret(
            "demo",
            "data",
            "s3cret",
            "https://id.example/realms/stardelt/protocol/openid-connect/token",
            "69a2b3c4",
        );
        assert_eq!(secret_text(&s, "client-id").as_deref(), Some("sp-demo"));
        assert_eq!(secret_text(&s, "client-secret").as_deref(), Some("s3cret"));
        assert_eq!(
            secret_text(&s, "token-url").as_deref(),
            Some("https://id.example/realms/stardelt/protocol/openid-connect/token")
        );
        assert_eq!(secret_text(&s, "subject").as_deref(), Some("69a2b3c4"));
    }

    #[test]
    fn the_payload_is_the_platform_scripts_client() {
        let spec = spec_json(&["trino", "lakekeeper"]);
        let payload = client_payload("demo", &spec);
        assert_eq!(payload["clientId"], "sp-demo");
        assert_eq!(payload["serviceAccountsEnabled"], true);
        assert_eq!(payload["publicClient"], false);
        assert_eq!(payload["clientAuthenticatorType"], "client-secret");
        assert_eq!(payload["attributes"]["stardelt-managed"], "principal");
        assert_eq!(payload["attributes"]["access.token.lifespan"], "28800");
        let mappers = payload["protocolMappers"].as_array().expect("mappers");
        let names: Vec<&str> = mappers
            .iter()
            .map(|m| m["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["aud-trino", "aud-lakekeeper", "groups"]);
        assert_eq!(mappers[0]["config"]["included.custom.audience"], "trino");
        assert!(
            mappers[0]["config"]
                .get("included.client.audience")
                .is_none()
        );
        assert!(payload.get("secret").is_none(), "must not contain a secret");
    }

    #[test]
    fn the_trusted_engine_audience_is_its_own_mapper() {
        let spec = spec_json(&["lakekeeper", "lakekeeper-trino"]);
        let payload = client_payload("catalog-trino", &spec);
        let mappers = payload["protocolMappers"].as_array().expect("mappers");
        let names: Vec<&str> = mappers
            .iter()
            .map(|m| m["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["aud-lakekeeper", "aud-lakekeeper-trino", "groups"]);
        assert_eq!(
            mappers[1]["config"]["included.custom.audience"],
            "lakekeeper-trino"
        );
    }

    #[test]
    fn the_keycloak_url_names_the_service_and_the_port() {
        assert_eq!(
            keycloak_service_url("stardelt", "/admin/realms/stardelt/clients"),
            "http://keycloak.stardelt.svc.cluster.local:8080/admin/realms/stardelt/clients"
        );
    }

    #[test]
    fn a_refusal_is_the_refused_token_sentence_not_a_zero() {
        assert_eq!(
            map_keycloak_error(401, ""),
            "Keycloak refused the administrator token"
        );
        assert_eq!(
            map_keycloak_error(403, r#"{"error":"forbidden"}"#),
            "Keycloak refused the administrator token"
        );
    }

    #[test]
    fn a_missing_install_and_an_unexpected_code_keep_their_sentences() {
        assert_eq!(
            map_keycloak_error(404, ""),
            "Keycloak is not installed in this namespace"
        );
        assert_eq!(
            map_keycloak_error(400, "  invalid request \n"),
            "Keycloak answered 400: invalid request"
        );
        assert_eq!(
            map_keycloak_error(500, r#"{"error":"boom"}"#),
            r#"Keycloak answered 500: {"error":"boom"}"#
        );
    }

    type Recorded = Arc<Mutex<Vec<(String, String, String)>>>;

    /// The injected Keycloak transport (#856): the same `handler` the kube mock uses, so every
    /// test keeps one place where its Keycloak answers live.
    struct MockKeycloak {
        handler: Arc<dyn Fn(String, String, String) -> (u16, serde_json::Value) + Send + Sync>,
        requests: Recorded,
    }

    impl KeycloakTransport for MockKeycloak {
        fn request(&self, r: KeycloakRequest) -> BoxFuture<'static, Result<(u16, String), String>> {
            let handler = self.handler.clone();
            let requests = self.requests.clone();
            Box::pin(async move {
                let body = String::from_utf8_lossy(&r.body).to_string();
                let uri = format!("/proxy{}", r.path);
                requests.lock().expect("requests poisoned").push((
                    r.method.clone(),
                    uri.clone(),
                    body.clone(),
                ));
                let (status, document) = handler(r.method, uri, body);
                Ok((
                    status,
                    serde_json::to_string(&document).expect("mock document"),
                ))
            })
        }
    }

    fn mock_ctx(
        handler: impl Fn(String, String, String) -> (u16, serde_json::Value) + Send + Sync + 'static,
    ) -> (Arc<Context>, Recorded) {
        let requests: Recorded = Arc::default();
        let recorder = requests.clone();
        let handler: Arc<dyn Fn(String, String, String) -> (u16, serde_json::Value) + Send + Sync> =
            Arc::new(handler);
        let keycloak_handler = handler.clone();
        let service = service_fn(move |request: Request<Body>| {
            let recorder = recorder.clone();
            let handler = handler.clone();
            let method = request.method().to_string();
            let uri = request.uri().to_string();
            async move {
                let body = request
                    .into_body()
                    .collect_bytes()
                    .await
                    .expect("mock body");
                let body_str = String::from_utf8_lossy(&body).to_string();
                recorder.lock().expect("requests poisoned").push((
                    method.clone(),
                    uri.clone(),
                    body_str.clone(),
                ));
                let (status, document) = handler(method, uri, body_str);
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::to_vec(&document).expect("mock document"),
                        ))
                        .expect("valid mock response"),
                )
            }
        });
        let client = kube::Client::new(service, "stardelt");
        // The platform store replaces the LIST `platform_instance_for_namespace` used to make
        // (#274 rule 5); the instance is the one the old mock answered with.
        let (platforms, mut writer) = kube::runtime::reflector::store();
        writer.apply_watcher_event(&kube::runtime::watcher::Event::Init);
        writer.apply_watcher_event(&kube::runtime::watcher::Event::InitApply(
            serde_json::from_value(serde_json::json!({
                "apiVersion": "stardelt.io/v1alpha1",
                "kind": "PlatformInstance",
                "metadata": { "name": "cluster" },
                "spec": {
                    "namespace": "stardelt",
                    "baseDomain": "data.example.com",
                    "identity": { "mode": "platform" }
                }
            }))
            .expect("deserialize PlatformInstance"),
        ));
        writer.apply_watcher_event(&kube::runtime::watcher::Event::InitDone);
        drop(writer);
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(RecordingRunner::default()),
            keycloak: Arc::new(MockKeycloak {
                handler: keycloak_handler,
                requests: requests.clone(),
            }),
            platforms,
        });
        (ctx, requests)
    }

    fn b64(s: &str) -> String {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let bytes = s.as_bytes();
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let group = (u32::from(chunk[0]) << 16)
                | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
                | u32::from(*chunk.get(2).unwrap_or(&0));
            for pos in 0..4 {
                if pos <= chunk.len() {
                    out.push(char::from(TABLE[((group >> (18 - 6 * pos)) & 63) as usize]));
                } else {
                    out.push('=');
                }
            }
        }
        out
    }

    fn cm_env() -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": { "name": "keycloak-env", "namespace": "stardelt" },
            "data": { "KC_HOSTNAME": "https://id.data.example.com" }
        })
    }
    fn admin_secret() -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": { "name": "keycloak-admin", "namespace": "stardelt" },
            "data": { "username": b64("admin"), "password": b64("secretpass") }
        })
    }
    fn principal_secret(value: &str) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": { "name": "sp-demo", "namespace": "stardelt" },
            "data": { "client-id": b64("sp-demo"), "client-secret": b64(value) }
        })
    }

    #[tokio::test]
    async fn an_existing_secret_wins_over_a_new_value() {
        let (ctx, requests) = mock_ctx(|method, uri, _body| {
            if method == "GET" && uri.contains("/configmaps/keycloak-env") {
                return (200, cm_env());
            }
            if method == "GET" && uri.contains("/secrets/keycloak-admin") {
                return (200, admin_secret());
            }
            if method == "GET" && uri.ends_with("/secrets/sp-demo") {
                return (200, principal_secret("kept-value"));
            }
            if uri.contains("/proxy/realms/master/protocol/openid-connect/token") {
                return (200, serde_json::json!({"access_token": "tok"}));
            }
            if method == "GET"
                && uri.contains("/proxy/admin/realms/stardelt/clients?clientId=sp-demo")
            {
                return (
                    200,
                    serde_json::json!([{"id": "uuid-1", "clientId": "sp-demo"}]),
                );
            }
            if method == "GET" && uri.contains("/proxy/admin/realms/stardelt/groups?search=") {
                return (
                    200,
                    serde_json::json!([{"id": "gid-1", "name": "analytics"}]),
                );
            }
            if method == "GET" && uri.contains("/service-account-user") {
                return (200, serde_json::json!({"id": "uid-1"}));
            }
            if method == "GET" && uri.contains("/users/uid-1/groups") {
                return (200, serde_json::json!([]));
            }
            if method == "GET" && uri.contains("/apis/stardelt.io/v1alpha1/platforminstances") {
                return (
                    200,
                    serde_json::json!({
                        "apiVersion": "stardelt.io/v1alpha1",
                        "kind": "PlatformInstanceList",
                        "metadata": {},
                        "items": [{
                            "apiVersion": "stardelt.io/v1alpha1",
                            "kind": "PlatformInstance",
                            "metadata": { "name": "cluster" },
                            "spec": {
                                "namespace": "stardelt",
                                "baseDomain": "data.example.com",
                                "identity": { "mode": "platform" }
                            }
                        }]
                    }),
                );
            }
            if method == "PUT" || method == "POST" || method == "DELETE" || method == "PATCH" {
                return (200, serde_json::json!({}));
            }
            (
                404,
                serde_json::json!({"kind":"Status","code":404,"message":"not found"}),
            )
        });

        let spec = spec_json(&["trino", "lakekeeper"]);
        let outcome = ensure_principal_client(&ctx, "stardelt", "demo", &spec).await;
        assert!(
            matches!(outcome, ClientOutcome::Converged { .. }),
            "{outcome:?}"
        );

        let requests = requests.lock().expect("poisoned");
        let puts: Vec<_> = requests
            .iter()
            .filter(|(m, u, _)| {
                m == "PUT" && u.contains("/proxy/admin/realms/stardelt/clients/uuid-1")
            })
            .collect();
        assert_eq!(puts.len(), 1, "expected one client PUT: {requests:?}");
        let body: serde_json::Value = serde_json::from_str(&puts[0].2).expect("json");
        assert_eq!(body["secret"], "kept-value");
        // No generated value appears in any request.
        for (m, u, b) in requests.iter() {
            if u.contains("/proxy/admin/realms/stardelt/clients") && (m == "POST" || m == "PUT") {
                let s = b.to_string();
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&s) {
                    assert_eq!(
                        v["secret"], "kept-value",
                        "{m} {u} leaked a different secret"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn an_existing_realm_gains_the_spark_audience() {
        let (ctx, requests) = mock_ctx(|method, uri, _body| {
            if method == "GET" && uri.contains("/configmaps/keycloak-env") {
                return (200, cm_env());
            }
            if method == "GET" && uri.contains("/secrets/keycloak-admin") {
                return (200, admin_secret());
            }
            if uri.contains("/proxy/realms/master/protocol/openid-connect/token") {
                return (200, serde_json::json!({"access_token": "tok"}));
            }
            if method == "GET"
                && uri.contains("/proxy/admin/realms/stardelt/clients?clientId=spark-connect")
            {
                return (200, serde_json::json!([]));
            }
            if method == "GET"
                && uri.contains("/proxy/admin/realms/stardelt/clients?clientId=jupyterhub")
            {
                return (200, serde_json::json!([{"id": "uuid-jh"}]));
            }
            if method == "GET"
                && uri.contains("/proxy/admin/realms/stardelt/clients?clientId=stardelt-dev")
            {
                return (200, serde_json::json!([{"id": "uuid-sd"}]));
            }
            if method == "GET" && uri.contains("/protocol-mappers/models") {
                return (200, serde_json::json!([]));
            }
            if method == "POST" {
                return (201, serde_json::json!({}));
            }
            (
                404,
                serde_json::json!({"kind":"Status","code":404,"message":"not found"}),
            )
        });

        ensure_spark_audience(&ctx, "stardelt").await.expect("sync");

        let requests = requests.lock().expect("poisoned");
        let posts: Vec<_> = requests
            .iter()
            .filter(|(m, u, _)| m == "POST" && !u.contains("/protocol/openid-connect/token"))
            .collect();
        let client_posts: Vec<_> = posts
            .iter()
            .filter(|(_, u, _)| u.ends_with("/proxy/admin/realms/stardelt/clients"))
            .collect();
        assert_eq!(
            client_posts.len(),
            1,
            "expected one client POST: {requests:?}"
        );
        let body: serde_json::Value = serde_json::from_str(&client_posts[0].2).expect("json");
        assert_eq!(body["clientId"], "spark-connect");
        for uuid in ["uuid-jh", "uuid-sd"] {
            let mapper_posts: Vec<_> = posts
                .iter()
                .filter(|(_, u, _)| {
                    u.ends_with(&format!("/clients/{uuid}/protocol-mappers/models"))
                })
                .collect();
            assert_eq!(
                mapper_posts.len(),
                1,
                "expected one mapper POST for {uuid}: {requests:?}"
            );
            assert!(
                mapper_posts[0].2.contains("\"aud-spark-connect\""),
                "mapper body: {}",
                mapper_posts[0].2
            );
        }
    }

    #[tokio::test]
    async fn a_realm_that_already_carries_the_audience_gets_no_post() {
        let (ctx, requests) = mock_ctx(|method, uri, _body| {
            if method == "GET" && uri.contains("/configmaps/keycloak-env") {
                return (200, cm_env());
            }
            if method == "GET" && uri.contains("/secrets/keycloak-admin") {
                return (200, admin_secret());
            }
            if uri.contains("/proxy/realms/master/protocol/openid-connect/token") {
                return (200, serde_json::json!({"access_token": "tok"}));
            }
            if method == "GET"
                && uri.contains("/proxy/admin/realms/stardelt/clients?clientId=spark-connect")
            {
                return (
                    200,
                    serde_json::json!([{"id": "uuid-sc", "clientId": "spark-connect"}]),
                );
            }
            if method == "GET"
                && uri.contains("/proxy/admin/realms/stardelt/clients?clientId=jupyterhub")
            {
                return (200, serde_json::json!([{"id": "uuid-jh"}]));
            }
            if method == "GET"
                && uri.contains("/proxy/admin/realms/stardelt/clients?clientId=stardelt-dev")
            {
                return (200, serde_json::json!([{"id": "uuid-sd"}]));
            }
            if method == "GET" && uri.contains("/protocol-mappers/models") {
                return (
                    200,
                    serde_json::json!([{"id": "m1", "name": "aud-spark-connect"}]),
                );
            }
            if method == "POST" {
                return (201, serde_json::json!({}));
            }
            (
                404,
                serde_json::json!({"kind":"Status","code":404,"message":"not found"}),
            )
        });

        ensure_spark_audience(&ctx, "stardelt").await.expect("sync");

        let requests = requests.lock().expect("poisoned");
        // The admin token is itself a POST; everything else must be a read.
        let writes: Vec<_> = requests
            .iter()
            .filter(|(m, u, _)| {
                (*m == "POST" || *m == "PUT" || *m == "DELETE" || *m == "PATCH")
                    && !u.contains("/protocol/openid-connect/token")
            })
            .collect();
        assert!(writes.is_empty(), "no write expected: {writes:?}");
    }

    #[tokio::test]
    async fn a_missing_owner_group_writes_no_secret() {
        let (ctx, requests) = mock_ctx(|method, uri, _body| {
            if method == "GET" && uri.contains("/configmaps/keycloak-env") {
                return (200, cm_env());
            }
            if method == "GET" && uri.contains("/secrets/keycloak-admin") {
                return (200, admin_secret());
            }
            if method == "GET" && uri.ends_with("/secrets/sp-demo") {
                return (
                    404,
                    serde_json::json!({"kind":"Status","code":404,"reason":"NotFound","message":"not found"}),
                );
            }
            if uri.contains("/proxy/realms/master/protocol/openid-connect/token") {
                return (200, serde_json::json!({"access_token": "tok"}));
            }
            if method == "GET"
                && uri.contains("/proxy/admin/realms/stardelt/clients?clientId=sp-demo")
            {
                return (200, serde_json::json!([]));
            }
            if method == "POST"
                && uri.contains("/proxy/admin/realms/stardelt/clients")
                && !uri.contains("clientId=")
            {
                return (201, serde_json::json!({}));
            }
            if method == "GET" && uri.contains("/proxy/admin/realms/stardelt/groups?search=") {
                return (200, serde_json::json!([{"id":"gid-x","name":"other"}]));
            }
            if method == "PUT" || method == "DELETE" || method == "PATCH" {
                return (200, serde_json::json!({}));
            }
            (
                404,
                serde_json::json!({"kind":"Status","code":404,"message":"not found"}),
            )
        });

        let spec = spec_json(&["trino"]);
        let outcome = ensure_principal_client(&ctx, "stardelt", "demo", &spec).await;
        assert_eq!(
            outcome,
            ClientOutcome::OwnerGroupMissing {
                group: "analytics".into()
            }
        );

        let requests = requests.lock().expect("poisoned");
        assert!(
            !requests
                .iter()
                .any(|(m, u, _)| m == "PATCH" && u.contains("/secrets/sp-demo")),
            "no Secret patch expected: {requests:?}"
        );
    }
}
