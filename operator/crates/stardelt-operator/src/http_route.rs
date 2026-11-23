//! The Gateway API `HTTPRoute` objects of the platform's applications: the same six public
//! surfaces as [`crate::ingress`], for an instance with `spec.ingress.mode: gateway`.
//!
//! Gateway API (`gateway.networking.k8s.io/v1`) is the portable ingress object: the `Gateway`
//! an installation already runs owns the listeners, the TLS certificates and source filtering,
//! and one `HTTPRoute` per application attaches a host name and path prefixes to it. Nothing is
//! rendered here for `mode: ingress`, and the gRPC front of Spark Connect is not here either: an
//! `HTTPRoute` has no backend protocol, so the front's Gateway API kind `GRPCRoute` lives in
//! [`crate::grpc_route`].

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{DynamicObject, ObjectMeta, TypeMeta};
use kube::core::{ApiResource, GroupVersionKind};
use serde_json::json;

use crate::crd::{IdentityMode, IngressMode, PlatformInstanceSpec};
use crate::ingress::{self, KEYCLOAK_PATHS};
use crate::{keycloak, nova};

/// The `HTTPRoute` resource (`gateway.networking.k8s.io/v1`).
pub fn resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("gateway.networking.k8s.io", "v1", "HTTPRoute"),
        "httproutes",
    )
}

/// The `HTTPRoute` of Nova, on the base domain. `None` unless `spec.ingress.enabled`,
/// `spec.ingress.mode: gateway`, `spec.ingress.gateway` and a base domain are all set.
pub fn nova(spec: &PlatformInstanceSpec) -> Option<DynamicObject> {
    route(spec, nova::NAME, nova::NAME, 8080, "", &["/"])
}

/// The `HTTPRoute` of the identity provider, on `id.<base domain>`, with `identity.mode:
/// platform` only. The paths are [`KEYCLOAK_PATHS`], so the master realm stays unpublished by
/// construction.
pub fn keycloak(spec: &PlatformInstanceSpec) -> Option<DynamicObject> {
    if spec.identity.mode != IdentityMode::Platform {
        return None;
    }
    route(
        spec,
        keycloak::NAME,
        keycloak::NAME,
        8080,
        "id",
        &KEYCLOAK_PATHS,
    )
}

/// The `HTTPRoute` of Airflow, on `airflow.<base domain>`, with the `pipelines` pillar.
pub fn airflow(spec: &PlatformInstanceSpec) -> Option<DynamicObject> {
    if !spec.pillars.pipelines {
        return None;
    }
    route(
        spec,
        "airflow",
        "airflow-api-server",
        8080,
        "airflow",
        &["/"],
    )
}

/// The `HTTPRoute` of JupyterHub, on `jupyter.<base domain>`, with the `notebooks` pillar.
pub fn jupyterhub(spec: &PlatformInstanceSpec) -> Option<DynamicObject> {
    if !spec.pillars.notebooks {
        return None;
    }
    route(spec, "jupyterhub", "proxy-public", 80, "jupyter", &["/"])
}

/// The `HTTPRoute` of Superset, on `superset.<base domain>`, with the `dashboards` pillar.
pub fn superset(spec: &PlatformInstanceSpec) -> Option<DynamicObject> {
    if !spec.pillars.dashboards {
        return None;
    }
    route(spec, "superset", "superset", 8088, "superset", &["/"])
}

/// The `HTTPRoute` of Trino, on `trino.<base domain>`, published only with an IdP
/// (`identity.mode != none`): without the authenticator this host would be anonymous SQL.
pub fn trino(spec: &PlatformInstanceSpec) -> Option<DynamicObject> {
    if spec.identity.mode == IdentityMode::None {
        return None;
    }
    route(
        spec,
        "trino",
        "trino",
        8080,
        "trino",
        &["/v1/statement", "/oauth2"],
    )
}

/// Every `HTTPRoute` this instance publishes, gated exactly like
/// [`crate::provision::ensure_ingress`]: the Lakehouse pillar for Nova and Trino, the identity
/// provider for `identity.mode: platform`, and one pillar per other application.
pub fn all(spec: &PlatformInstanceSpec) -> Vec<DynamicObject> {
    let mut objects = Vec::new();
    if spec.pillars.lakehouse {
        objects.extend(nova(spec));
        objects.extend(trino(spec));
    }
    objects.extend(keycloak(spec));
    if spec.pillars.pipelines {
        objects.extend(airflow(spec));
    }
    if spec.pillars.notebooks {
        objects.extend(jupyterhub(spec));
    }
    if spec.pillars.dashboards {
        objects.extend(superset(spec));
    }
    objects
}

/// The one builder every application route goes through. Six arguments, one per field it copies.
fn route(
    spec: &PlatformInstanceSpec,
    name: &str,
    service: &str,
    port: i32,
    subdomain: &str,
    paths: &[&str],
) -> Option<DynamicObject> {
    let settings = &spec.ingress;
    if !settings.enabled || settings.mode != IngressMode::Gateway {
        return None;
    }
    let gateway = settings.gateway.as_ref()?;
    let hostname = ingress::host(spec.base_domain.as_deref()?, subdomain);
    let mut parent_ref = json!({ "name": gateway.name });
    if let Some(namespace) = gateway.namespace.as_deref() {
        parent_ref["namespace"] = json!(namespace);
    }
    Some(DynamicObject {
        types: Some(TypeMeta {
            api_version: "gateway.networking.k8s.io/v1".into(),
            kind: "HTTPRoute".into(),
        }),
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(spec.namespace.clone()),
            labels: Some(BTreeMap::from([(
                "app.kubernetes.io/part-of".to_string(),
                "stardelt".to_string(),
            )])),
            ..ObjectMeta::default()
        },
        data: json!({ "spec": {
            "parentRefs": [parent_ref],
            "hostnames": [hostname],
            "rules": [{
                "matches": paths
                    .iter()
                    .map(|path| json!({"path": {"type": "PathPrefix", "value": path}}))
                    .collect::<Vec<_>>(),
                "backendRefs": [{"name": service, "port": port}],
            }],
        }}),
    })
}

/// `objects::own` is bounded to `DynamicType = ()`, which a `DynamicObject` (whose dynamic type
/// is its `ApiResource`) does not satisfy in kube 5, so the reference is stamped here. Same
/// contract as [`crate::monitoring`]'s copy: idempotent, never a duplicate.
pub fn own(object: &mut DynamicObject, owner: &OwnerReference) {
    let owners = object
        .metadata
        .owner_references
        .get_or_insert_with(Vec::new);
    if owners.iter().any(|existing| existing.uid == owner.uid) {
        return;
    }
    owners.push(owner.clone());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(extra: serde_json::Value) -> PlatformInstanceSpec {
        let mut value = serde_json::json!({
            "namespace": "stardelt",
            "baseDomain": "data.example.com",
            "identity": { "mode": "platform" },
            "pillars": { "pipelines": true, "notebooks": true, "dashboards": true },
            "ingress": {
                "enabled": true,
                "mode": "gateway",
                "gateway": { "name": "platform" }
            },
        });
        for (key, item) in extra.as_object().expect("an object") {
            value[key] = item.clone();
        }
        serde_json::from_value(value).expect("a PlatformInstanceSpec")
    }

    /// The path prefixes of the route's single rule, each with its match type.
    fn paths(route: &DynamicObject) -> Vec<(String, String)> {
        route.data["spec"]["rules"][0]["matches"]
            .as_array()
            .expect("matches")
            .iter()
            .map(|m| {
                (
                    m["path"]["value"].as_str().expect("value").to_string(),
                    m["path"]["type"].as_str().expect("type").to_string(),
                )
            })
            .collect()
    }

    fn backend(route: &DynamicObject) -> (String, i64) {
        let backend = &route.data["spec"]["rules"][0]["backendRefs"][0];
        (
            backend["name"].as_str().expect("name").to_string(),
            backend["port"].as_i64().expect("port"),
        )
    }

    #[test]
    fn nova_attaches_the_base_domain_to_the_gateway() {
        let route = nova(&spec(serde_json::json!({}))).expect("an HTTPRoute");
        assert_eq!(
            route.types.as_ref().expect("types").api_version,
            "gateway.networking.k8s.io/v1"
        );
        assert_eq!(route.types.as_ref().expect("types").kind, "HTTPRoute");
        assert_eq!(route.metadata.name.as_deref(), Some("nova"));
        assert_eq!(route.metadata.namespace.as_deref(), Some("stardelt"));
        assert_eq!(
            route.metadata.labels.as_ref().expect("labels")["app.kubernetes.io/part-of"],
            "stardelt"
        );
        assert_eq!(
            route.data["spec"]["parentRefs"],
            json!([{"name": "platform"}])
        );
        assert_eq!(route.data["spec"]["hostnames"], json!(["data.example.com"]));
        assert_eq!(paths(&route), [("/".to_string(), "PathPrefix".to_string())]);
        assert_eq!(backend(&route), ("nova".to_string(), 8080));
    }

    #[test]
    fn the_gateway_namespace_is_carried_when_it_is_set() {
        let route = nova(&spec(serde_json::json!({
            "ingress": {
                "enabled": true,
                "mode": "gateway",
                "gateway": { "name": "edge", "namespace": "gateway-system" }
            }
        })))
        .expect("an HTTPRoute");
        assert_eq!(
            route.data["spec"]["parentRefs"],
            json!([{"name": "edge", "namespace": "gateway-system"}])
        );
    }

    #[test]
    fn keycloak_publishes_the_stardelt_realm_only_on_id() {
        let route = keycloak(&spec(serde_json::json!({}))).expect("an HTTPRoute");
        assert_eq!(route.metadata.name.as_deref(), Some("keycloak"));
        assert_eq!(
            route.data["spec"]["hostnames"],
            json!(["id.data.example.com"])
        );
        assert_eq!(backend(&route), ("keycloak".to_string(), 8080));
        let rendered: Vec<String> = paths(&route).into_iter().map(|(path, _)| path).collect();
        assert_eq!(rendered, KEYCLOAK_PATHS);
        assert!(
            rendered.iter().all(|path| !path.contains("/realms/master")),
            "the master realm stays unpublished: {rendered:?}"
        );
    }

    #[test]
    fn airflow_answers_on_its_subdomain_with_the_api_server() {
        let route = airflow(&spec(serde_json::json!({}))).expect("an HTTPRoute");
        assert_eq!(route.metadata.name.as_deref(), Some("airflow"));
        assert_eq!(
            route.data["spec"]["hostnames"],
            json!(["airflow.data.example.com"])
        );
        assert_eq!(paths(&route), [("/".to_string(), "PathPrefix".to_string())]);
        assert_eq!(backend(&route), ("airflow-api-server".to_string(), 8080));
    }

    #[test]
    fn jupyterhub_answers_on_jupyter_with_the_proxy() {
        let route = jupyterhub(&spec(serde_json::json!({}))).expect("an HTTPRoute");
        assert_eq!(route.metadata.name.as_deref(), Some("jupyterhub"));
        assert_eq!(
            route.data["spec"]["hostnames"],
            json!(["jupyter.data.example.com"])
        );
        assert_eq!(paths(&route), [("/".to_string(), "PathPrefix".to_string())]);
        assert_eq!(backend(&route), ("proxy-public".to_string(), 80));
    }

    #[test]
    fn superset_answers_on_its_subdomain_with_the_superset_service() {
        let route = superset(&spec(serde_json::json!({}))).expect("an HTTPRoute");
        assert_eq!(route.metadata.name.as_deref(), Some("superset"));
        assert_eq!(
            route.data["spec"]["hostnames"],
            json!(["superset.data.example.com"])
        );
        assert_eq!(paths(&route), [("/".to_string(), "PathPrefix".to_string())]);
        assert_eq!(backend(&route), ("superset".to_string(), 8088));
    }

    #[test]
    fn trino_carries_the_statement_and_sign_in_paths() {
        let route = trino(&spec(serde_json::json!({}))).expect("an HTTPRoute");
        assert_eq!(route.metadata.name.as_deref(), Some("trino"));
        assert_eq!(
            route.data["spec"]["hostnames"],
            json!(["trino.data.example.com"])
        );
        assert_eq!(
            paths(&route),
            [
                ("/v1/statement".to_string(), "PathPrefix".to_string()),
                ("/oauth2".to_string(), "PathPrefix".to_string()),
            ]
        );
        assert_eq!(backend(&route), ("trino".to_string(), 8080));
    }

    #[test]
    fn an_ingress_mode_spec_renders_no_route() {
        let ingress_mode = spec(serde_json::json!({ "ingress": { "enabled": true } }));
        assert!(all(&ingress_mode).is_empty());
        assert!(nova(&ingress_mode).is_none());
        assert!(keycloak(&ingress_mode).is_none());
    }

    #[test]
    fn gateway_mode_without_a_gateway_renders_nothing() {
        let no_gateway =
            spec(serde_json::json!({ "ingress": { "enabled": true, "mode": "gateway" } }));
        assert!(all(&no_gateway).is_empty());
    }

    #[test]
    fn nothing_is_rendered_unless_ingress_is_enabled_or_a_domain_is_set() {
        let off = spec(serde_json::json!({ "ingress": {
            "enabled": false, "mode": "gateway", "gateway": { "name": "platform" }
        }}));
        assert!(all(&off).is_empty());
        let no_domain = spec(serde_json::json!({ "baseDomain": null }));
        assert!(all(&no_domain).is_empty());
    }

    #[test]
    fn the_pillars_and_the_identity_mode_gate_the_routes() {
        let bare = spec(serde_json::json!({
            "identity": { "mode": "none" },
            "pillars": { "lakehouse": false, "pipelines": false, "notebooks": false, "dashboards": false },
        }));
        assert!(all(&bare).is_empty(), "{:?}", all(&bare));

        let lakehouse = spec(serde_json::json!({
            "pillars": { "lakehouse": true, "pipelines": false, "notebooks": false, "dashboards": false },
        }));
        let names: Vec<String> = all(&lakehouse)
            .iter()
            .map(|route| route.metadata.name.clone().expect("name"))
            .collect();
        assert_eq!(names, ["nova", "trino", "keycloak"]);
    }
}
