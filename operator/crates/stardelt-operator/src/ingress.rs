//! The Ingress objects of the platform's applications (master spec §5, step O2 of
//! `design/operator-parity.md`). Pure builders: they return typed objects and touch no cluster;
//! the controller step that applies them comes later.
//!
//! Every field mirrors `stardelt-platform` `chart/templates/ingress.yaml`: Nova answers on
//! `spec.baseDomain` itself and every other application on a subdomain (the chart's
//! `stardelt.appHost`); the class, the cert-manager issuer annotation and the TLS block come
//! from `spec.ingress`. The six applications built today (Nova, identity provider, Airflow, JupyterHub, Superset, Trino); Lakekeeper stays unpublished until it authenticates.

use std::collections::BTreeMap;

use k8s_openapi::api::networking::v1::{
    HTTPIngressPath, HTTPIngressRuleValue, Ingress, IngressBackend, IngressRule,
    IngressServiceBackend, IngressSpec, IngressTLS, ServiceBackendPort,
};
use kube::api::ObjectMeta;

use crate::{
    conditions::set_condition,
    crd::{Condition, IdentityMode, IngressMode, PlatformInstanceSpec},
    keycloak, nova,
};

/// The host an application answers on: the base domain itself for an empty `subdomain`, else
/// `<subdomain>.<base domain>`.
pub fn host(base_domain: &str, subdomain: &str) -> String {
    if subdomain.is_empty() {
        base_domain.to_string()
    } else {
        format!("{subdomain}.{base_domain}")
    }
}

/// The name of the TLS Secret: `template` with `{host}` replaced by the host name with its dots
/// turned into dashes.
pub fn secret_name(template: &str, host: &str) -> String {
    template.replace("{host}", &host.replace('.', "-"))
}

/// What the identity provider publishes: the realm's public surface and the admin console and
/// REST API of the `stardelt` realm, never the master realm. Plain path prefixes
/// (`pathType: Prefix`), matched segment by segment by every Ingress controller including
/// Traefik; the ingress-nginx regexes these replaced were read as literal prefixes there and
/// answered 404 (stardelt/stardelt#882).
pub const KEYCLOAK_PATHS: [&str; 6] = [
    "/realms/stardelt",
    "/resources",
    "/robots.txt",
    "/admin/stardelt",
    "/admin/realms/stardelt",
    "/admin/serverinfo",
];

/// The Ingress of Nova, on the base domain. `None` unless `spec.ingress.enabled` and a base
/// domain is set.
pub fn nova(spec: &PlatformInstanceSpec) -> Option<Ingress> {
    build(spec, nova::NAME, nova::NAME, 8080, "", &["/"], &[])
}

/// The Ingress of the identity provider, on `id.<base domain>`, with `identity.mode: platform`
/// only. `None` otherwise, like [`nova`].
pub fn keycloak(spec: &PlatformInstanceSpec) -> Option<Ingress> {
    if spec.identity.mode != IdentityMode::Platform {
        return None;
    }
    build(
        spec,
        keycloak::NAME,
        keycloak::NAME,
        8080,
        "id",
        &KEYCLOAK_PATHS,
        &[],
    )
}

/// The Ingress of Airflow, on `airflow.<base domain>`, with the `pipelines` pillar. Airflow
/// signs in through the platform IdP (values/airflow.yaml), so this is not an anonymous host.
pub fn airflow(spec: &PlatformInstanceSpec) -> Option<Ingress> {
    if !spec.pillars.pipelines {
        return None;
    }
    build(
        spec,
        "airflow",
        "airflow-api-server",
        8080,
        "airflow",
        &["/"],
        &[],
    )
}

/// The Ingress of JupyterHub, on `jupyter.<base domain>`, with the `notebooks` pillar.
/// JupyterHub signs in through the platform IdP (values/jupyterhub.yaml).
pub fn jupyterhub(spec: &PlatformInstanceSpec) -> Option<Ingress> {
    if !spec.pillars.notebooks {
        return None;
    }
    build(
        spec,
        "jupyterhub",
        "proxy-public",
        80,
        "jupyter",
        &["/"],
        &[],
    )
}

/// The Ingress of Superset, on `superset.<base domain>`, with the `dashboards` pillar.
/// Superset signs in through the platform IdP (values/superset.yaml), and the realm's
/// redirect URI `https://superset.<base domain>/oauth-authorized/keycloak`
/// (`identity.rs`, `Hosts::superset`; substituted in `keycloak.rs`) only resolves through
/// this host. The platform chart knows the same service and port
/// (`chart/templates/ingress.yaml`) but publishes it only behind
/// `ingress.exposeUnauthenticated`; the operator publishes it whenever the pillar is on,
/// as for Airflow.
pub fn superset(spec: &PlatformInstanceSpec) -> Option<Ingress> {
    if !spec.pillars.dashboards {
        return None;
    }
    build(spec, "superset", "superset", 8088, "superset", &["/"], &[])
}

/// The Ingress of Trino, on `trino.<base domain>`: the one path outside clients (Superset,
/// BI tools, `curl`) take to the query engine. Published **only with an IdP**
/// (`identity.mode != none`): without the overlay (`values/trino-identity.yaml`) Trino has no
/// authenticator, and this host would be anonymous SQL on the internet.
///
/// Backend port 8080 and plain HTTP to the Service are correct: the coordinator's
/// `http-server.process-forwarded=true` (the identity overlay) reads ingress-nginx's
/// `X-Forwarded-Proto`, while `allow-insecure-over-http=false` still refuses a direct plain
/// request. The two paths are the platform's `chart/templates/ingress.yaml` list: statements
/// and result polling under `/v1/statement`, the browser sign-in under `/oauth2`.
pub fn trino(spec: &PlatformInstanceSpec) -> Option<Ingress> {
    if spec.identity.mode == IdentityMode::None {
        return None;
    }
    build(
        spec,
        "trino",
        "trino",
        8080,
        "trino",
        &["/v1/statement", "/oauth2"],
        &[],
    )
}

/// The token-checking front's public name, `spark.<base domain>` (stardelt/stardelt#231, #84 Q4):
/// gRPC, so nginx must speak HTTP/2 to the back end, and a query can be silent for as long as an
/// executor works — the platform manifest's 3600 s read/send timeouts.
pub fn spark(spec: &PlatformInstanceSpec) -> Option<Ingress> {
    if spec.identity.mode == IdentityMode::None {
        return None;
    }
    build(
        spec,
        "spark-connect",
        "spark-connect-auth",
        15002,
        "spark",
        &["/"],
        &[
            ("nginx.ingress.kubernetes.io/backend-protocol", "GRPC"),
            ("nginx.ingress.kubernetes.io/proxy-read-timeout", "3600"),
            ("nginx.ingress.kubernetes.io/proxy-send-timeout", "3600"),
        ],
    )
}

/// The one builder every application Ingress goes through: class, TLS, extra annotations and the
/// path list come from the instance's ingress settings. Seven arguments, one per field it copies.
#[allow(clippy::too_many_arguments)]
fn build(
    spec: &PlatformInstanceSpec,
    name: &str,
    service: &str,
    port: i32,
    subdomain: &str,
    paths: &[&str],
    extra: &[(&str, &str)],
) -> Option<Ingress> {
    let settings = &spec.ingress;
    if !settings.enabled {
        return None;
    }
    let host = host(spec.base_domain.as_deref()?, subdomain);
    let mut annotations = BTreeMap::new();
    if settings.tls.enabled
        && let Some(issuer) = settings.tls.issuer.as_deref().filter(|i| !i.is_empty())
    {
        annotations.insert(
            "cert-manager.io/cluster-issuer".to_string(),
            issuer.to_string(),
        );
    }
    for (key, value) in extra {
        annotations.insert((*key).to_string(), (*value).to_string());
    }
    if !settings.allowed_cidrs.is_empty() {
        annotations.insert(
            "nginx.ingress.kubernetes.io/whitelist-source-range".to_string(),
            settings.allowed_cidrs.join(","),
        );
    }
    let backend = |service: &str| IngressBackend {
        service: Some(IngressServiceBackend {
            name: service.to_string(),
            port: Some(ServiceBackendPort {
                number: Some(port),
                ..ServiceBackendPort::default()
            }),
        }),
        ..IngressBackend::default()
    };
    Some(Ingress {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(spec.namespace.clone()),
            labels: Some(BTreeMap::from([(
                "app.kubernetes.io/part-of".to_string(),
                "stardelt".to_string(),
            )])),
            annotations: (!annotations.is_empty()).then_some(annotations),
            ..ObjectMeta::default()
        },
        spec: Some(IngressSpec {
            ingress_class_name: settings.class_name.clone(),
            tls: settings.tls.enabled.then(|| {
                vec![IngressTLS {
                    hosts: Some(vec![host.clone()]),
                    secret_name: Some(secret_name(&settings.tls.secret_name_template, &host)),
                }]
            }),
            rules: Some(vec![IngressRule {
                host: Some(host),
                http: Some(HTTPIngressRuleValue {
                    paths: paths
                        .iter()
                        .map(|path| HTTPIngressPath {
                            path: Some(path.to_string()),
                            path_type: "Prefix".to_string(),
                            backend: backend(service),
                        })
                        .collect(),
                }),
            }]),
            ..IngressSpec::default()
        }),
        ..Ingress::default()
    })
}

/// `AllowListNotEnforced` (#24 item 3, option (a)): `spec.ingress.allowedCidrs` is honoured
/// only by the ingress-nginx annotations. In gateway mode the Gateway's listener owns source
/// filtering, so a non-empty list is ignored — and that must be visible on the resource, not
/// only in the CRD's doc comment.
pub fn allow_list_condition(
    spec: &PlatformInstanceSpec,
    generation: Option<i64>,
    previous: Option<&Condition>,
    now: &str,
) -> Option<Condition> {
    if !spec.ingress.enabled
        || spec.ingress.mode != IngressMode::Gateway
        || spec.ingress.allowed_cidrs.is_empty()
    {
        return None;
    }
    Some(set_condition(
        previous.map(std::slice::from_ref).unwrap_or(&[]),
        "AllowListNotEnforced",
        "True",
        "GatewayOwnsSourceFiltering",
        Some(
            "spec.ingress.allowedCidrs is ignored in gateway mode: the Gateway's listener owns source filtering; configure it there or add a NetworkPolicy".to_string(),
        ),
        generation,
        now,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(extra: serde_json::Value) -> PlatformInstanceSpec {
        let mut value = serde_json::json!({
            "namespace": "stardelt",
            "baseDomain": "data.example.com",
            "ingress": { "enabled": true },
        });
        for (key, item) in extra.as_object().expect("an object") {
            value[key] = item.clone();
        }
        serde_json::from_value(value).expect("a PlatformInstanceSpec")
    }

    fn rule_of(ingress: &Ingress) -> (String, Vec<(String, String)>) {
        let rule = ingress.spec.clone().expect("spec").rules.expect("rules")[0].clone();
        let paths = rule
            .http
            .expect("http")
            .paths
            .into_iter()
            .map(|p| (p.path.expect("path"), p.path_type))
            .collect();
        (rule.host.expect("host"), paths)
    }

    #[test]
    fn hosts_are_the_base_domain_or_a_subdomain_of_it() {
        assert_eq!(host("data.example.com", ""), "data.example.com");
        assert_eq!(host("data.example.com", "id"), "id.data.example.com");
    }

    #[test]
    fn the_secret_name_turns_the_dots_of_the_host_into_dashes() {
        assert_eq!(
            secret_name("{host}-tls", "id.data.example.com"),
            "id-data-example-com-tls"
        );
        assert_eq!(secret_name("tls-{host}", "a.b"), "tls-a-b");
    }

    #[test]
    fn nothing_is_built_unless_ingress_is_enabled_and_a_domain_is_set() {
        let off = spec(serde_json::json!({ "ingress": { "enabled": false } }));
        assert!(nova(&off).is_none());
        let no_domain = spec(serde_json::json!({ "baseDomain": null }));
        assert!(nova(&no_domain).is_none());
    }

    #[test]
    fn nova_answers_on_the_base_domain_and_nothing_more() {
        let ingress = nova(&spec(serde_json::json!({}))).expect("an Ingress");
        assert_eq!(ingress.metadata.name.as_deref(), Some("nova"));
        assert_eq!(ingress.metadata.namespace.as_deref(), Some("stardelt"));
        assert_eq!(
            ingress.metadata.labels.as_ref().expect("labels")["app.kubernetes.io/part-of"],
            "stardelt"
        );
        assert!(ingress.metadata.annotations.is_none(), "no TLS, no regex");
        let (host, paths) = rule_of(&ingress);
        assert_eq!(host, "data.example.com");
        assert_eq!(paths, [("/".to_string(), "Prefix".to_string())]);
        let spec = ingress.spec.expect("spec");
        assert!(
            spec.ingress_class_name.is_none(),
            "the cluster default class"
        );
        assert!(spec.tls.is_none());
        let backend = spec.rules.expect("rules")[0]
            .http
            .clone()
            .expect("http")
            .paths[0]
            .backend
            .service
            .clone()
            .expect("service");
        assert_eq!(backend.name, "nova");
        assert_eq!(backend.port.expect("port").number, Some(8080));
    }

    #[test]
    fn the_class_and_tls_come_from_the_settings() {
        let spec = spec(serde_json::json!({ "ingress": {
            "enabled": true,
            "className": "traefik",
            "tls": { "enabled": true, "issuer": "letsencrypt" }
        }}));
        let ingress = nova(&spec).expect("an Ingress");
        assert_eq!(
            ingress.metadata.annotations.as_ref().expect("annotations")["cert-manager.io/cluster-issuer"],
            "letsencrypt"
        );
        let inner = ingress.spec.expect("spec");
        assert_eq!(inner.ingress_class_name.as_deref(), Some("traefik"));
        let tls = inner.tls.expect("tls");
        assert_eq!(tls.len(), 1);
        assert_eq!(tls[0].hosts, Some(vec!["data.example.com".to_string()]));
        assert_eq!(tls[0].secret_name.as_deref(), Some("data-example-com-tls"));
    }

    #[test]
    fn the_allow_list_is_written_on_every_public_ingress() {
        let s = spec(serde_json::json!({
            "identity": { "mode": "platform" },
            "pillars": { "pipelines": true, "notebooks": true, "dashboards": true },
            "ingress": { "enabled": true, "allowedCidrs": ["203.0.113.0/24", "10.0.0.0/8"] }
        }));
        let expected = "203.0.113.0/24,10.0.0.0/8";
        for ingress in [
            nova(&s).expect("nova"),
            keycloak(&s).expect("keycloak"),
            airflow(&s).expect("airflow"),
            jupyterhub(&s).expect("jupyterhub"),
            superset(&s).expect("superset"),
            trino(&s).expect("trino"),
        ] {
            assert_eq!(
                ingress.metadata.annotations.as_ref().expect("annotations")["nginx.ingress.kubernetes.io/whitelist-source-range"],
                expected,
                "{:?}",
                ingress.metadata.name
            );
        }
    }

    #[test]
    fn an_empty_allow_list_writes_no_annotation() {
        let ingress = nova(&spec(serde_json::json!({}))).expect("an Ingress");
        assert!(
            ingress.metadata.annotations.is_none(),
            "empty means open, and open means no annotation"
        );
    }

    #[test]
    fn the_identity_provider_publishes_only_the_stardelt_realm() {
        let ingress = keycloak(&spec(
            serde_json::json!({ "identity": { "mode": "platform" } }),
        ))
        .expect("an Ingress");
        assert_eq!(ingress.metadata.name.as_deref(), Some("keycloak"));
        assert!(
            ingress.metadata.annotations.is_none(),
            "no controller-specific annotation on a plain-prefix Ingress"
        );
        let (host, paths) = rule_of(&ingress);
        assert_eq!(host, "id.data.example.com");
        assert_eq!(paths.len(), 6);
        assert!(paths.iter().all(|(_, kind)| kind == "Prefix"));
        assert!(
            paths
                .iter()
                .all(|(path, _)| path != "/" && !path.starts_with("/admin/master")),
            "the master realm and the root stay unreachable: {paths:?}"
        );
        assert!(paths.contains(&("/realms/stardelt".to_string(), "Prefix".to_string())));
        assert!(
            paths
                .iter()
                .all(|(path, _)| !path.contains('(') && !path.contains('\\')),
            "no regex syntax survives: {paths:?}"
        );
    }

    #[test]
    fn the_identity_provider_has_an_ingress_only_with_mode_platform() {
        for mode in ["none", "external"] {
            let value = serde_json::json!({ "identity": { "mode": mode, "issuerUrl": "https://idp.example.com",
                "apiAudience": "api", "clients": { "nova": { "clientId": "nova", "clientSecretRef": { "name": "s" } } } } });
            assert!(keycloak(&spec(value)).is_none(), "{mode}");
        }
    }

    #[test]
    fn airflow_answers_on_its_subdomain_with_the_api_server() {
        let ingress = airflow(&spec(
            serde_json::json!({ "pillars": { "pipelines": true } }),
        ))
        .expect("an Ingress");
        let (host, paths) = rule_of(&ingress);
        assert_eq!(host, "airflow.data.example.com");
        assert_eq!(paths, [("/".to_string(), "Prefix".to_string())]);
        let inner = ingress.spec.expect("spec");
        let backend = inner.rules.expect("rules")[0]
            .http
            .clone()
            .expect("http")
            .paths[0]
            .backend
            .service
            .clone()
            .expect("service");
        assert_eq!(backend.name, "airflow-api-server");
        assert_eq!(backend.port.expect("port").number, Some(8080));
    }

    #[test]
    fn jupyterhub_answers_on_jupyter_and_needs_the_notebooks_pillar() {
        let ingress = jupyterhub(&spec(
            serde_json::json!({ "pillars": { "notebooks": true } }),
        ))
        .expect("an Ingress");
        let (host, paths) = rule_of(&ingress);
        assert_eq!(host, "jupyter.data.example.com");
        assert_eq!(paths, [("/".to_string(), "Prefix".to_string())]);
        let inner = ingress.spec.expect("spec");
        let backend = inner.rules.expect("rules")[0]
            .http
            .clone()
            .expect("http")
            .paths[0]
            .backend
            .service
            .clone()
            .expect("service");
        assert_eq!(backend.name, "proxy-public");
        assert_eq!(backend.port.expect("port").number, Some(80));
        assert!(jupyterhub(&spec(serde_json::json!({}))).is_none());
    }

    #[test]
    fn superset_answers_on_its_subdomain_with_the_superset_service() {
        let ingress = superset(&spec(serde_json::json!({"pillars": {"dashboards": true}})))
            .expect("an Ingress");
        let (host, paths) = rule_of(&ingress);
        assert_eq!(host, "superset.data.example.com");
        assert_eq!(paths, [("/".to_string(), "Prefix".to_string())]);
        let inner = ingress.spec.expect("spec");
        let backend = inner.rules.expect("rules")[0]
            .http
            .clone()
            .expect("http")
            .paths[0]
            .backend
            .service
            .clone()
            .expect("service");
        assert_eq!(backend.name, "superset");
        assert_eq!(backend.port.expect("port").number, Some(8088));
    }

    #[test]
    fn superset_needs_the_dashboards_pillar() {
        assert!(superset(&spec(serde_json::json!({}))).is_none());
    }

    #[test]
    fn airflow_needs_the_pipelines_pillar() {
        assert!(airflow(&spec(serde_json::json!({}))).is_none());
    }

    #[test]
    fn trino_is_published_only_with_sign_in() {
        let anonymous = spec(serde_json::json!({}));
        assert!(trino(&anonymous).is_none());

        let signed_in = spec(serde_json::json!({
            "identity": {"mode": "platform"},
            "ingress": { "enabled": true, "tls": { "enabled": true, "issuer": "letsencrypt" } }
        }));
        let ingress = trino(&signed_in).expect("an Ingress");
        assert_eq!(ingress.metadata.name.as_deref(), Some("trino"));
        let rule = &ingress.spec.as_ref().unwrap().rules.as_ref().unwrap()[0];
        assert_eq!(rule.host.as_deref(), Some("trino.data.example.com"));
        let paths: Vec<_> = rule
            .http
            .as_ref()
            .unwrap()
            .paths
            .iter()
            .map(|p| (p.path.clone().unwrap(), p.path_type.clone()))
            .collect();
        assert_eq!(
            paths,
            vec![
                ("/v1/statement".to_string(), "Prefix".to_string()),
                ("/oauth2".to_string(), "Prefix".to_string()),
            ]
        );
        let backend = rule.http.as_ref().unwrap().paths[0]
            .backend
            .service
            .as_ref()
            .unwrap();
        assert_eq!(backend.name, "trino");
        assert_eq!(backend.port.as_ref().unwrap().number, Some(8080));
        let tls = ingress.spec.as_ref().unwrap().tls.as_ref().unwrap();
        assert_eq!(tls.len(), 1);
        assert_eq!(
            tls[0].secret_name.as_deref(),
            Some("trino-data-example-com-tls")
        );
    }

    #[test]
    fn spark_is_published_only_with_sign_in_and_speaks_grpc() {
        assert!(
            spark(&spec(serde_json::json!({}))).is_none(),
            "no identity, no front"
        );
        assert!(
            spark(&spec(serde_json::json!({"identity": {"mode": "none"}}))).is_none(),
            "identity.mode: none"
        );

        let signed_in = spec(serde_json::json!({
            "identity": {"mode": "platform"},
            "ingress": { "enabled": true, "tls": { "enabled": true, "issuer": "letsencrypt" } }
        }));
        let ingress = spark(&signed_in).expect("an Ingress");
        assert_eq!(ingress.metadata.name.as_deref(), Some("spark-connect"));
        let (host, paths) = rule_of(&ingress);
        assert_eq!(host, "spark.data.example.com");
        assert_eq!(paths, [("/".to_string(), "Prefix".to_string())]);
        let rule = &ingress.spec.as_ref().unwrap().rules.as_ref().unwrap()[0];
        let backend = rule.http.as_ref().unwrap().paths[0]
            .backend
            .service
            .as_ref()
            .unwrap();
        assert_eq!(backend.name, "spark-connect-auth");
        assert_eq!(backend.port.as_ref().unwrap().number, Some(15002));
        let annotations = ingress.metadata.annotations.as_ref().expect("annotations");
        assert_eq!(annotations["cert-manager.io/cluster-issuer"], "letsencrypt");
        assert_eq!(
            annotations["nginx.ingress.kubernetes.io/backend-protocol"],
            "GRPC"
        );
        assert_eq!(
            annotations["nginx.ingress.kubernetes.io/proxy-read-timeout"],
            "3600"
        );
        assert_eq!(
            annotations["nginx.ingress.kubernetes.io/proxy-send-timeout"],
            "3600"
        );

        let no_domain = spec(serde_json::json!({
            "baseDomain": null,
            "identity": {"mode": "platform"}
        }));
        assert!(spark(&no_domain).is_none(), "no base domain, no host");
    }

    fn gateway_spec(allowed_cidrs: serde_json::Value) -> PlatformInstanceSpec {
        spec(serde_json::json!({
            "ingress": {
                "enabled": true,
                "mode": "gateway",
                "gateway": { "name": "platform" },
                "allowedCidrs": allowed_cidrs,
            }
        }))
    }

    #[test]
    fn allow_list_condition_needs_gateway_mode_and_a_non_empty_list() {
        let ingress_mode = spec(serde_json::json!({
            "ingress": { "enabled": true, "allowedCidrs": ["10.0.0.0/8"] }
        }));
        assert!(
            allow_list_condition(&ingress_mode, Some(1), None, "2026-11-01T00:00:00Z").is_none(),
            "ingress mode writes the annotation, so nothing is ignored"
        );

        let no_list = gateway_spec(serde_json::json!([]));
        assert!(
            allow_list_condition(&no_list, Some(1), None, "2026-11-01T00:00:00Z").is_none(),
            "an empty list is open, nothing is ignored"
        );

        let gateway = gateway_spec(serde_json::json!(["10.0.0.0/8"]));
        let condition = allow_list_condition(&gateway, Some(7), None, "2026-11-01T00:00:00Z")
            .expect("a condition");
        assert_eq!(condition.type_, "AllowListNotEnforced");
        assert_eq!(condition.status, "True");
        assert_eq!(
            condition.reason.as_deref(),
            Some("GatewayOwnsSourceFiltering")
        );
        assert!(
            condition
                .message
                .as_deref()
                .expect("a message")
                .contains("allowedCidrs"),
            "{:?}",
            condition.message
        );
        assert_eq!(condition.observed_generation, Some(7));

        let disabled = spec(serde_json::json!({
            "ingress": {
                "enabled": false,
                "mode": "gateway",
                "gateway": { "name": "platform" },
                "allowedCidrs": ["10.0.0.0/8"],
            }
        }));
        assert!(
            allow_list_condition(&disabled, Some(1), None, "2026-11-01T00:00:00Z").is_none(),
            "no routing, no condition"
        );
    }

    #[test]
    fn allow_list_condition_keeps_its_transition_time() {
        let gateway = gateway_spec(serde_json::json!(["10.0.0.0/8"]));
        let first = allow_list_condition(&gateway, Some(2), None, "2026-11-01T00:00:00Z")
            .expect("a condition");
        assert_eq!(
            first.last_transition_time.as_deref(),
            Some("2026-11-01T00:00:00Z")
        );
        let second = allow_list_condition(&gateway, Some(3), Some(&first), "2026-11-02T00:00:00Z")
            .expect("a condition");
        assert_eq!(
            second.last_transition_time.as_deref(),
            Some("2026-11-01T00:00:00Z"),
            "a repeated reconcile keeps the time it first became True"
        );
    }
}
