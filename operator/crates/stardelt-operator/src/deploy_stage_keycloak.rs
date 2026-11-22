//! The Keycloak client of a `DeployStage` (#57 slice 3).
//!
//! Keycloak's `federated-jwt` authenticator accepts a CI assertion for exactly one
//! client: `jwt.credential.issuer` names the realm's identity-provider alias and
//! `jwt.credential.sub` the subject it accepts. One client per `DeployStage`, named
//! `stardelt-deploy-<stage>`, is what Nova's principal rule (slice 4) reads as `azp`.
//! There is no secret: the client never signs in by itself.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::{
    Api, ResourceExt,
    api::{Patch, PatchParams},
    runtime::controller::Action,
};
use serde_json::json;

use crate::{
    controller::Context,
    crd::DeployStage,
    keycloak::ENV_CONFIG_MAP,
    principal_keycloak::{admin_token, keycloak_request},
};

/// The finalizer this controller owns: present while the stage's client must be deleted.
pub const DEPLOY_STAGE_FINALIZER: &str = "stardelt.io/deploy-stage-client";

/// The Keycloak client id a stage owns: `stardelt-deploy-<stage>`.
pub fn client_id(stage: &str) -> String {
    format!("stardelt-deploy-{stage}")
}

/// The client representation a `DeployStage` wants: a public-in-name-only client with
/// `federated-jwt` authentication, the CI subject pinned in its attributes, and one audience
/// mapper for the stage's audience. The attribute names are the ones measured against
/// Keycloak 26.8.2 (`platform/scripts/spike-keycloak-federated-ci.sh`).
pub fn client_payload(stage: &DeployStage) -> serde_json::Value {
    let name = stage.metadata.name.clone().unwrap_or_default();
    let subject = &stage.spec.subject;
    let audience = &stage.spec.audience;
    json!({
        "clientId": client_id(&name),
        "name": format!("Deploy stage {name}"),
        "description": format!("A CI job that deploys to stage {name}; its subject is pinned on the DeployStage."),
        "enabled": true,
        "protocol": "openid-connect",
        "publicClient": false,
        "clientAuthenticatorType": "federated-jwt",
        "standardFlowEnabled": false,
        "implicitFlowEnabled": false,
        "directAccessGrantsEnabled": false,
        "serviceAccountsEnabled": true,
        "redirectUris": [],
        "webOrigins": [],
        "attributes": {
            "stardelt-managed": "deploy-stage",
            "jwt.credential.issuer": "github-actions",
            "jwt.credential.sub": subject
        },
        "protocolMappers": [{
            "name": format!("aud-{audience}"),
            "protocol": "openid-connect",
            "protocolMapper": "oidc-audience-mapper",
            "consentRequired": false,
            "config": {
                "included.custom.audience": audience,
                "id.token.claim": "false",
                "access.token.claim": "true",
                "introspection.token.claim": "true"
            }
        }, {
            // Nova forwards this token to Trino, which checks aud against its client id "trino" (#1164).
            "name": "aud-trino",
            "protocol": "openid-connect",
            "protocolMapper": "oidc-audience-mapper",
            "consentRequired": false,
            "config": {
                "included.client.audience": "trino",
                "id.token.claim": "false",
                "access.token.claim": "true",
                "introspection.token.claim": "true"
            }
        }]
    })
}

/// The audience the first `aud-…` mapper of a client carries, if there is one.
fn mapper_audience(value: &serde_json::Value) -> Option<&str> {
    value
        .get("protocolMappers")?
        .as_array()?
        .iter()
        .find(|m| {
            m.get("name")
                .and_then(|n| n.as_str())
                .is_some_and(|n| n.starts_with("aud-"))
        })?
        .get("config")?
        .get("included.custom.audience")?
        .as_str()
}

/// Whether a client carries the mapper that puts Trino in the token's audience (#1164).
fn has_trino_audience(value: &serde_json::Value) -> bool {
    value
        .get("protocolMappers")
        .and_then(|m| m.as_array())
        .is_some_and(|mappers| {
            mappers.iter().any(|m| {
                m.get("config")
                    .and_then(|c| c.get("included.client.audience"))
                    .and_then(|a| a.as_str())
                    == Some("trino")
            })
        })
}

/// Whether the rendered client differs from what Keycloak answers (#274 rule 1: a second
/// reconcile writes nothing). A missing `aud-…` mapper counts as a difference.
pub fn needs_update(existing: &serde_json::Value, desired: &serde_json::Value) -> bool {
    if existing.get("clientAuthenticatorType") != desired.get("clientAuthenticatorType") {
        return true;
    }
    // A client rendered before #1024 carries `serviceAccountsEnabled: false`; without this the
    // stage keeps answering `unauthorized_client` for ever (#1009).
    if existing.get("serviceAccountsEnabled") != desired.get("serviceAccountsEnabled") {
        return true;
    }
    for key in ["jwt.credential.issuer", "jwt.credential.sub"] {
        if existing.get("attributes").and_then(|a| a.get(key))
            != desired.get("attributes").and_then(|a| a.get(key))
        {
            return true;
        }
    }
    mapper_audience(existing) != mapper_audience(desired)
        || has_trino_audience(existing) != has_trino_audience(desired)
}

/// The stage that owns `stage`'s subject when it is not `stage` itself: the oldest stage in
/// the namespace that pins the same `spec.subject` (creationTimestamp, then name). Keycloak
/// cannot tell two federated clients with one (issuer, sub) apart and refuses the second (#1163).
pub fn subject_holder(stage: &DeployStage, all: &[DeployStage]) -> Option<String> {
    let key = |s: &DeployStage| {
        (
            s.metadata.creation_timestamp.clone().map(|t| t.0),
            s.name_any(),
        )
    };
    let holder = all
        .iter()
        .filter(|s| s.spec.subject == stage.spec.subject && s.metadata.deletion_timestamp.is_none())
        .min_by_key(|s| key(s))?;
    let name = holder.name_any();
    (name != stage.name_any()).then_some(name)
}

/// Whether this namespace owns a Keycloak: ConfigMap `keycloak-env` carries `KC_HOSTNAME` and
/// Secret `keycloak-admin` exists. A dev-mode install has neither, and the operator never
/// writes to an identity provider it does not own.
pub async fn keycloak_owns_namespace(ctx: &Context, namespace: &str) -> Result<bool, String> {
    let config_maps: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), namespace);
    let config = config_maps
        .get_opt(ENV_CONFIG_MAP)
        .await
        .map_err(|e| format!("cannot read ConfigMap {ENV_CONFIG_MAP}: {e}"))?;
    let Some(config) = config else {
        return Ok(false);
    };
    let host = config
        .data
        .as_ref()
        .and_then(|d| d.get("KC_HOSTNAME"))
        .map(|h| h.trim())
        .unwrap_or("");
    if host.is_empty() {
        return Ok(false);
    }
    let secrets: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);
    let admin = secrets
        .get_opt("keycloak-admin")
        .await
        .map_err(|e| format!("cannot read Secret keycloak-admin: {e}"))?;
    Ok(admin.is_some())
}

/// Create or converge the stage's client. Reads first, so a converged client costs one GET.
pub async fn ensure_deploy_stage_client(
    ctx: &Context,
    namespace: &str,
    stage: &DeployStage,
) -> Result<(), String> {
    let name = stage.metadata.name.clone().unwrap_or_default();
    let client_id = client_id(&name);
    let payload = client_payload(stage);
    let token = admin_token(ctx, namespace).await?;

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
    let clients: Vec<serde_json::Value> = serde_json::from_str(&listed)
        .map_err(|_| "Keycloak returned a client list that is not JSON".to_string())?;

    match clients.first() {
        None => {
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
        }
        Some(existing) => {
            if !needs_update(existing, &payload) {
                return Ok(());
            }
            let id = existing
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("Keycloak client {client_id} has no id"))?;
            keycloak_request(
                ctx,
                namespace,
                "PUT",
                &format!("/admin/realms/stardelt/clients/{id}"),
                Some(&token),
                Some("application/json"),
                serde_json::to_vec(&payload)
                    .map_err(|e| format!("cannot encode the Keycloak request: {e}"))?,
            )
            .await?;
        }
    }
    Ok(())
}

/// Delete the stage's client, refusing a client the operator did not render.
pub async fn delete_deploy_stage_client(
    ctx: &Context,
    namespace: &str,
    stage: &str,
) -> Result<(), String> {
    let client_id = client_id(stage);
    let token = admin_token(ctx, namespace).await?;
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
    let clients: Vec<serde_json::Value> = serde_json::from_str(&listed)
        .map_err(|_| "Keycloak returned a client list that is not JSON".to_string())?;
    let Some(client) = clients.first() else {
        return Ok(());
    };
    let managed = client
        .get("attributes")
        .and_then(|a| a.get("stardelt-managed"))
        .and_then(|v| v.as_str());
    if managed != Some("deploy-stage") {
        return Err(format!(
            "refusing to delete client {client_id}: it does not carry stardelt-managed=deploy-stage"
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
    Ok(())
}

/// The `DeployStage` controller: render the client when the namespace owns a Keycloak, hold a
/// finalizer while it exists, and delete the client on removal.
pub async fn reconcile_deploy_stage(
    stage: Arc<DeployStage>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    let name = stage.name_any();
    let namespace = stage.namespace().unwrap_or_else(|| "default".into());

    if stage.metadata.deletion_timestamp.is_some() {
        // No Keycloak, no client to delete: without this the removal would sit on
        // `admin_token`'s error for ever once the namespace stopped owning an identity provider.
        let owns_keycloak = keycloak_owns_namespace(&ctx, &namespace)
            .await
            .map_err(|message| kube::Error::Service(anyhow::anyhow!("{message}").into()))?;
        if owns_keycloak {
            if let Err(message) = delete_deploy_stage_client(&ctx, &namespace, &name).await {
                tracing::warn!(stage = %name, %namespace, %message, "finalize failed");
                return Err(kube::Error::Service(
                    anyhow::anyhow!("cannot delete the deploy-stage client: {message}").into(),
                ));
            }
        } else {
            tracing::info!(
                stage = %name,
                %namespace,
                "no Keycloak in this namespace; nothing to delete"
            );
        }
        let stages: Api<DeployStage> = Api::namespaced(ctx.client.clone(), &namespace);
        let finalizers: Vec<String> = stage
            .finalizers()
            .iter()
            .filter(|f| f.as_str() != DEPLOY_STAGE_FINALIZER)
            .cloned()
            .collect();
        stages
            .patch(
                &name,
                &PatchParams::default(),
                &Patch::Merge(json!({ "metadata": { "finalizers": finalizers } })),
            )
            .await?;
        return Ok(Action::await_change());
    }

    if !keycloak_owns_namespace(&ctx, &namespace)
        .await
        .map_err(|message| kube::Error::Service(anyhow::anyhow!("{message}").into()))?
    {
        tracing::info!(
            stage = %name,
            "no Keycloak in this namespace; no deploy client rendered"
        );
        return Ok(Action::requeue(Duration::from_secs(600)));
    }

    if let Err(message) = stage.spec.validate() {
        tracing::warn!(stage = %name, %message, "DeployStage spec is invalid; no client rendered");
        return Ok(Action::requeue(Duration::from_secs(600)));
    }

    if !stage
        .finalizers()
        .iter()
        .any(|f| f == DEPLOY_STAGE_FINALIZER)
    {
        let stages: Api<DeployStage> = Api::namespaced(ctx.client.clone(), &namespace);
        let mut finalizers = stage.finalizers().to_vec();
        finalizers.push(DEPLOY_STAGE_FINALIZER.to_string());
        stages
            .patch(
                &name,
                &PatchParams::default(),
                &Patch::Merge(json!({ "metadata": { "finalizers": finalizers } })),
            )
            .await?;
    }

    let stages: Api<DeployStage> = Api::namespaced(ctx.client.clone(), &namespace);
    let all = stages.list(&kube::api::ListParams::default()).await?.items;
    if let Some(holder) = subject_holder(&stage, &all) {
        tracing::warn!(stage = %name, %namespace, %holder,
            "stage {holder} already pins this subject; no deploy client rendered (#1163)");
        if let Err(message) = delete_deploy_stage_client(&ctx, &namespace, &name).await {
            tracing::warn!(stage = %name, %namespace, %message, "cannot remove the duplicate client");
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    match ensure_deploy_stage_client(&ctx, &namespace, &stage).await {
        Err(message) => {
            tracing::warn!(stage = %name, %namespace, %message, "deploy client not rendered");
            Ok(Action::requeue(Duration::from_secs(30)))
        }
        Ok(()) => Ok(Action::requeue(Duration::from_secs(300))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::PlatformInstance;
    use crate::helm::testing::RecordingRunner;
    use crate::principal_keycloak::{KeycloakRequest, KeycloakTransport};
    use futures::future::BoxFuture;
    use http::Request;
    use kube::client::Body;
    use std::sync::{Arc, Mutex};
    use tower::service_fn;

    fn stage() -> DeployStage {
        serde_json::from_value(json!({
            "metadata": { "name": "prod", "namespace": "stardelt" },
            "spec": { "subject": "repo:stardelt/stardelt:ref:refs/heads/main" }
        }))
        .expect("DeployStage")
    }

    fn stage_named(name: &str, subject: &str, created: &str) -> DeployStage {
        serde_json::from_value(json!({
            "metadata": {
                "name": name,
                "namespace": "stardelt",
                "creationTimestamp": created
            },
            "spec": { "subject": subject }
        }))
        .expect("DeployStage")
    }

    #[test]
    fn the_older_stage_keeps_the_subject() {
        let subject = "repo:stardelt/stardelt:ref:refs/heads/main";
        let dev = stage_named("dev", subject, "2026-11-22T02:46:00Z");
        let demo_ci = stage_named("demo-ci", subject, "2026-11-22T12:20:00Z");
        let all = vec![dev.clone(), demo_ci.clone()];
        assert_eq!(subject_holder(&demo_ci, &all), Some("dev".to_string()));
        assert_eq!(subject_holder(&dev, &all), None);
    }

    #[test]
    fn different_subjects_do_not_conflict() {
        let dev = stage_named(
            "dev",
            "repo:stardelt/stardelt:ref:refs/heads/main",
            "2026-11-22T02:46:00Z",
        );
        let staging = stage_named(
            "staging",
            "repo:stardelt/stardelt:ref:refs/heads/staging",
            "2026-11-22T12:20:00Z",
        );
        let all = vec![dev.clone(), staging.clone()];
        assert_eq!(subject_holder(&dev, &all), None);
        assert_eq!(subject_holder(&staging, &all), None);
    }

    #[test]
    fn a_stage_being_deleted_does_not_hold_the_subject() {
        let subject = "repo:stardelt/stardelt:ref:refs/heads/main";
        let mut dev = stage_named("dev", subject, "2026-11-22T02:46:00Z");
        dev.metadata.deletion_timestamp =
            Some(serde_json::from_str("\"2026-11-22T12:30:00Z\"").expect("timestamp"));
        let demo_ci = stage_named("demo-ci", subject, "2026-11-22T12:20:00Z");
        let all = vec![dev, demo_ci.clone()];
        assert_eq!(subject_holder(&demo_ci, &all), None);
    }

    #[test]
    fn the_payload_pins_the_subject_and_the_audience() {
        let payload = client_payload(&stage());
        assert_eq!(payload["clientId"], "stardelt-deploy-prod");
        assert_eq!(payload["clientAuthenticatorType"], "federated-jwt");
        assert_eq!(payload["attributes"]["stardelt-managed"], "deploy-stage");
        assert_eq!(
            payload["attributes"]["jwt.credential.issuer"],
            "github-actions"
        );
        assert_eq!(
            payload["attributes"]["jwt.credential.sub"],
            "repo:stardelt/stardelt:ref:refs/heads/main"
        );
        assert_eq!(
            payload["protocolMappers"][0]["config"]["included.custom.audience"],
            "stardelt-deploy"
        );
        assert_eq!(payload["protocolMappers"][1]["name"], "aud-trino");
        assert_eq!(
            payload["protocolMappers"][1]["config"]["included.client.audience"],
            "trino"
        );
        assert_eq!(payload["serviceAccountsEnabled"], true);
    }

    #[test]
    fn a_client_without_the_trino_audience_needs_a_write() {
        let desired = client_payload(&stage());
        let mut existing = desired.clone();
        let mappers = existing["protocolMappers"]
            .as_array_mut()
            .expect("protocolMappers is an array");
        mappers.remove(1);
        assert!(needs_update(&existing, &desired));
        assert!(!needs_update(&desired, &desired));
    }

    #[test]
    fn a_client_without_a_service_account_needs_a_write() {
        let payload = client_payload(&stage());
        let mut old = payload.clone();
        old["serviceAccountsEnabled"] = json!(false);
        assert!(needs_update(&old, &payload));
    }

    #[test]
    fn a_matching_client_needs_no_write() {
        let payload = client_payload(&stage());
        assert!(!needs_update(&payload, &payload));
    }

    #[test]
    fn a_changed_subject_needs_a_write() {
        let payload = client_payload(&stage());

        let mut other_subject = payload.clone();
        other_subject["attributes"]["jwt.credential.sub"] =
            json!("repo:stardelt/stardelt:ref:refs/heads/release");
        assert!(needs_update(&other_subject, &payload));

        let mut other_audience = payload.clone();
        other_audience["protocolMappers"][0]["config"]["included.custom.audience"] =
            json!("something-else");
        assert!(needs_update(&other_audience, &payload));

        let mut no_mapper = payload.clone();
        no_mapper["protocolMappers"] = json!([]);
        assert!(needs_update(&no_mapper, &payload));
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
        let (platforms, _writer) = kube::runtime::reflector::store::<PlatformInstance>();
        let (identity, _identity_writer) = kube::runtime::reflector::store();
        let ctx = Arc::new(Context {
            client,
            helm: Arc::new(RecordingRunner::default()),
            keycloak: Arc::new(MockKeycloak {
                handler: keycloak_handler,
                requests: requests.clone(),
            }),
            platforms,
            identity,
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
        json!({
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": { "name": "keycloak-env", "namespace": "stardelt" },
            "data": { "KC_HOSTNAME": "https://id.data.example.com" }
        })
    }
    fn admin_secret() -> serde_json::Value {
        json!({
            "apiVersion": "v1", "kind": "Secret",
            "metadata": { "name": "keycloak-admin", "namespace": "stardelt" },
            "data": { "username": b64("admin"), "password": b64("secretpass") }
        })
    }

    /// The two answers every Keycloak call needs: the ConfigMap that names the host and the
    /// admin Secret. `None` means "not installed in this namespace".
    fn gateway(method: &str, uri: &str) -> Option<(u16, serde_json::Value)> {
        if method == "GET" && uri.contains("/configmaps/keycloak-env") {
            return Some((200, cm_env()));
        }
        if method == "GET" && uri.contains("/secrets/keycloak-admin") {
            return Some((200, admin_secret()));
        }
        if uri.contains("/proxy/realms/master/protocol/openid-connect/token") {
            return Some((200, json!({"access_token": "tok"})));
        }
        // The subject-holder lookup (#1163): only the stage under test pins this subject.
        if method == "GET" && uri.contains("/deploystages") {
            return Some((
                200,
                json!({
                    "apiVersion": "stardelt.io/v1alpha1",
                    "kind": "DeployStageList",
                    "items": [{
                        "metadata": {
                            "name": "prod",
                            "namespace": "stardelt",
                            "creationTimestamp": "2026-11-22T02:46:00Z"
                        },
                        "spec": { "subject": "repo:stardelt/stardelt:ref:refs/heads/main" }
                    }]
                }),
            ));
        }
        None
    }

    #[tokio::test]
    async fn the_client_is_created_once_and_converged_afterwards() {
        // The list the GET answers next: empty first, then the recorded POST body.
        let created: Arc<Mutex<Option<serde_json::Value>>> = Arc::default();
        let state = created.clone();
        let (ctx, requests) = mock_ctx(move |method, uri, body| {
            if let Some(answer) = gateway(&method, &uri) {
                return answer;
            }
            if method == "GET"
                && uri
                    .contains("/proxy/admin/realms/stardelt/clients?clientId=stardelt-deploy-prod")
            {
                let answer = state.lock().expect("poisoned").clone();
                return match answer {
                    Some(client) => (200, json!([client])),
                    None => (200, json!([])),
                };
            }
            if method == "POST" && uri.ends_with("/proxy/admin/realms/stardelt/clients") {
                let client: serde_json::Value = serde_json::from_str(&body).expect("json");
                *state.lock().expect("poisoned") = Some(client);
                return (201, json!({}));
            }
            (
                404,
                json!({"kind":"Status","code":404,"message":"not found"}),
            )
        });

        ensure_deploy_stage_client(&ctx, "stardelt", &stage())
            .await
            .expect("first ensure");

        let writes = |requests: &Recorded| {
            requests
                .lock()
                .expect("poisoned")
                .iter()
                .filter(|(m, u, _)| {
                    (*m == "POST" || *m == "PUT" || *m == "DELETE")
                        && !u.contains("/protocol/openid-connect/token")
                })
                .map(|(m, u, _)| format!("{m} {u}"))
                .collect::<Vec<_>>()
        };
        let first = writes(&requests);
        assert_eq!(
            first,
            vec!["POST /proxy/admin/realms/stardelt/clients".to_string()],
            "one POST expected"
        );

        ensure_deploy_stage_client(&ctx, "stardelt", &stage())
            .await
            .expect("second ensure");
        let after = writes(&requests);
        assert_eq!(after, first, "a converged client must not be written again");

        // A list whose pinned subject differs is converged with exactly one PUT.
        let (ctx, requests) = mock_ctx(|method, uri, _body| {
            if let Some(answer) = gateway(&method, &uri) {
                return answer;
            }
            if method == "GET"
                && uri
                    .contains("/proxy/admin/realms/stardelt/clients?clientId=stardelt-deploy-prod")
            {
                return (
                    200,
                    json!([{
                        "id": "uuid-1",
                        "clientId": "stardelt-deploy-prod",
                        "clientAuthenticatorType": "federated-jwt",
                        "attributes": {
                            "stardelt-managed": "deploy-stage",
                            "jwt.credential.issuer": "github-actions",
                            "jwt.credential.sub": "repo:someone/else:ref:refs/heads/main"
                        },
                        "protocolMappers": [{
                            "name": "aud-stardelt-deploy",
                            "config": { "included.custom.audience": "stardelt-deploy" }
                        }, {
                            "name": "aud-trino",
                            "config": { "included.client.audience": "trino" }
                        }]
                    }]),
                );
            }
            if method == "PUT" || method == "POST" || method == "DELETE" {
                return (200, json!({}));
            }
            (
                404,
                json!({"kind":"Status","code":404,"message":"not found"}),
            )
        });

        ensure_deploy_stage_client(&ctx, "stardelt", &stage())
            .await
            .expect("ensure");

        let requests = requests.lock().expect("poisoned");
        let puts: Vec<_> = requests
            .iter()
            .filter(|(m, u, _)| {
                m == "PUT" && u.contains("/proxy/admin/realms/stardelt/clients/uuid-1")
            })
            .collect();
        assert_eq!(puts.len(), 1, "exactly one PUT expected: {requests:?}");
        let body: serde_json::Value = serde_json::from_str(&puts[0].2).expect("json");
        assert_eq!(
            body["attributes"]["jwt.credential.sub"],
            "repo:stardelt/stardelt:ref:refs/heads/main"
        );
    }

    #[tokio::test]
    async fn a_foreign_client_is_not_deleted() {
        let (ctx, requests) = mock_ctx(|method, uri, _body| {
            if let Some(answer) = gateway(&method, &uri) {
                return answer;
            }
            if method == "GET"
                && uri
                    .contains("/proxy/admin/realms/stardelt/clients?clientId=stardelt-deploy-prod")
            {
                return (
                    200,
                    json!([{
                        "id": "uuid-1",
                        "clientId": "stardelt-deploy-prod",
                        "attributes": { "stardelt-managed": "principal" }
                    }]),
                );
            }
            if method == "DELETE" {
                return (200, json!({}));
            }
            (
                404,
                json!({"kind":"Status","code":404,"message":"not found"}),
            )
        });

        let error = delete_deploy_stage_client(&ctx, "stardelt", "prod")
            .await
            .expect_err("a foreign client must be refused");
        assert_eq!(
            error,
            "refusing to delete client stardelt-deploy-prod: it does not carry stardelt-managed=deploy-stage"
        );

        let requests = requests.lock().expect("poisoned");
        assert!(
            !requests.iter().any(|(m, _, _)| m == "DELETE"),
            "no DELETE expected: {requests:?}"
        );
    }
    #[tokio::test]
    async fn removing_a_stage_without_keycloak_releases_the_finalizer() {
        let deleting: DeployStage = serde_json::from_value(json!({
            "metadata": {
                "name": "prod",
                "namespace": "stardelt",
                "deletionTimestamp": "2026-11-21T15:00:00Z",
                "finalizers": [DEPLOY_STAGE_FINALIZER]
            },
            "spec": { "subject": "repo:stardelt/stardelt:ref:refs/heads/main" }
        }))
        .expect("DeployStage");
        let (ctx, requests) = mock_ctx(|method, uri, _body| {
            if method == "GET" && uri.contains("/configmaps/keycloak-env") {
                return (
                    404,
                    json!({"kind":"Status","code":404,"reason":"NotFound","message":"not found"}),
                );
            }
            if method == "PATCH" {
                return (
                    200,
                    json!({
                        "apiVersion": "stardelt.io/v1alpha1",
                        "kind": "DeployStage",
                        "metadata": { "name": "prod", "namespace": "stardelt", "finalizers": [] },
                        "spec": { "subject": "repo:stardelt/stardelt:ref:refs/heads/main" }
                    }),
                );
            }
            (
                404,
                json!({"kind":"Status","code":404,"message":"not found"}),
            )
        });

        reconcile_deploy_stage(Arc::new(deleting), ctx)
            .await
            .expect("removal must not need a Keycloak");

        let requests = requests.lock().expect("poisoned");
        assert!(
            !requests.iter().any(|(_, u, _)| u.starts_with("/proxy")),
            "no Keycloak request expected: {requests:?}"
        );
        let patches: Vec<_> = requests
            .iter()
            .filter(|(m, u, _)| m == "PATCH" && u.contains("/deploystages/prod"))
            .collect();
        assert_eq!(
            patches.len(),
            1,
            "one finalizer patch expected: {requests:?}"
        );
        let body: serde_json::Value = serde_json::from_str(&patches[0].2).expect("json");
        assert_eq!(body["metadata"]["finalizers"], json!([]));
    }
}
