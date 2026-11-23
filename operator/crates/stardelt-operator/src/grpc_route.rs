//! The Gateway API `GRPCRoute` of the token-checking Spark Connect front: the one public
//! surface [`crate::http_route`] cannot carry, because an `HTTPRoute` has no backend protocol
//! and the front speaks gRPC.
//!
//! Gateway API (`gateway.networking.k8s.io/v1`) gives a `GRPCRoute` no per-route read or send
//! timeout: a Gateway that cuts a long-running call is a setting of the Gateway, not of this
//! object. One rule without `matches` matches every gRPC method on the host, exactly as the
//! front's Ingress publishes `/` for `mode: ingress`.

use std::collections::BTreeMap;

use kube::api::{DynamicObject, ObjectMeta, TypeMeta};
use kube::core::{ApiResource, GroupVersionKind};
use serde_json::json;

use crate::crd::{IdentityMode, IngressMode, PlatformInstanceSpec};
use crate::ingress;

/// The `GRPCRoute` resource (`gateway.networking.k8s.io/v1`).
pub fn resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("gateway.networking.k8s.io", "v1", "GRPCRoute"),
        "grpcroutes",
    )
}

/// The `GRPCRoute` of the Spark Connect front, on `spark.<base domain>`. `None` unless
/// `spec.ingress.enabled`, `spec.ingress.mode: gateway`, `spec.ingress.gateway` and a base
/// domain are all set, and never without an identity provider: without the verifier the host
/// would be anonymous gRPC, the same gate as [`crate::ingress::spark`].
pub fn spark(spec: &PlatformInstanceSpec) -> Option<DynamicObject> {
    let settings = &spec.ingress;
    if !settings.enabled || settings.mode != IngressMode::Gateway {
        return None;
    }
    let gateway = settings.gateway.as_ref()?;
    let hostname = ingress::host(spec.base_domain.as_deref()?, "spark");
    if spec.identity.mode == IdentityMode::None {
        return None;
    }
    let mut parent_ref = json!({ "name": gateway.name });
    if let Some(namespace) = gateway.namespace.as_deref() {
        parent_ref["namespace"] = json!(namespace);
    }
    Some(DynamicObject {
        types: Some(TypeMeta {
            api_version: "gateway.networking.k8s.io/v1".into(),
            kind: "GRPCRoute".into(),
        }),
        metadata: ObjectMeta {
            name: Some("spark-connect".to_string()),
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
                "backendRefs": [{"name": "spark-connect-auth", "port": 15002}],
            }],
        }}),
    })
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

    #[test]
    fn spark_attaches_its_front_to_the_gateway_on_the_spark_subdomain() {
        let route = spark(&spec(serde_json::json!({}))).expect("a GRPCRoute");
        assert_eq!(
            route.types.as_ref().expect("types").api_version,
            "gateway.networking.k8s.io/v1"
        );
        assert_eq!(route.types.as_ref().expect("types").kind, "GRPCRoute");
        assert_eq!(route.metadata.name.as_deref(), Some("spark-connect"));
        assert_eq!(route.metadata.namespace.as_deref(), Some("stardelt"));
        assert_eq!(
            route.metadata.labels.as_ref().expect("labels")["app.kubernetes.io/part-of"],
            "stardelt"
        );
        assert_eq!(
            route.data["spec"]["parentRefs"],
            json!([{"name": "platform"}])
        );
        assert_eq!(
            route.data["spec"]["hostnames"],
            json!(["spark.data.example.com"])
        );
        let rule = &route.data["spec"]["rules"][0];
        assert!(
            rule.get("matches").is_none(),
            "an empty rule matches every gRPC method: {rule:?}"
        );
        assert_eq!(
            rule["backendRefs"],
            json!([{"name": "spark-connect-auth", "port": 15002}])
        );
    }

    #[test]
    fn the_gateway_namespace_is_carried_when_it_is_set() {
        let route = spark(&spec(serde_json::json!({
            "ingress": {
                "enabled": true,
                "mode": "gateway",
                "gateway": { "name": "edge", "namespace": "gateway-system" }
            }
        })))
        .expect("a GRPCRoute");
        assert_eq!(
            route.data["spec"]["parentRefs"],
            json!([{"name": "edge", "namespace": "gateway-system"}])
        );
    }

    #[test]
    fn an_ingress_mode_or_unauthenticated_spec_renders_no_grpc_route() {
        let ingress_mode = spec(serde_json::json!({ "ingress": { "enabled": true } }));
        assert!(spark(&ingress_mode).is_none());
        let anonymous = spec(serde_json::json!({ "identity": { "mode": "none" } }));
        assert!(spark(&anonymous).is_none());
    }

    #[test]
    fn nothing_is_rendered_without_a_domain_a_gateway_name_or_ingress() {
        let no_domain = spec(serde_json::json!({ "baseDomain": null }));
        assert!(spark(&no_domain).is_none());
        // `GatewayRef::name` is required, so `gateway` is either absent (this case) or named.
        let no_gateway = spec(serde_json::json!({ "ingress": {
            "enabled": true, "mode": "gateway", "gateway": null
        }}));
        assert!(spark(&no_gateway).is_none());
        let off = spec(serde_json::json!({ "ingress": {
            "enabled": false, "mode": "gateway", "gateway": { "name": "platform" }
        }}));
        assert!(spark(&off).is_none());
    }
}
