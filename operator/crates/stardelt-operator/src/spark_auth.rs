//! The token-checking front of Spark Connect (`stardelt/stardelt#231`, D44 twin of
//! `platform/manifests/spark-connect-auth.yaml`): an Envoy whose `jwt_authn` verifies a
//! Keycloak-signed access token with audience `spark-connect` before a gRPC frame reaches the
//! driver (stardelt#84 Q4, spike stardelt#208), and whose `rbac` filter next to it admits only
//! the realm groups `stardelt-users` and `stardelt-admins` — a valid token of a `visitors`-only
//! person is refused with PERMISSION_DENIED (stardelt#404).
//!
//! Pure builders, object for object with the platform manifest. The Ingress lives in
//! [`crate::ingress::spark`]; this module builds the ConfigMap, the Deployment, the Service and
//! the NetworkPolicy in front of `spark-connect:15002`.

use std::collections::BTreeMap;

use k8s_openapi::api::{
    apps::v1::{Deployment, DeploymentSpec},
    core::v1::{
        Capabilities, ConfigMap, ConfigMapVolumeSource, Container, ContainerPort,
        EmptyDirVolumeSource, PodSecurityContext, PodSpec, PodTemplateSpec, Probe,
        ResourceRequirements, SeccompProfile, SecurityContext, Service, ServicePort, ServiceSpec,
        TCPSocketAction, Volume, VolumeMount,
    },
    networking::v1::{
        IPBlock, NetworkPolicy, NetworkPolicyIngressRule, NetworkPolicyPeer, NetworkPolicyPort,
        NetworkPolicySpec,
    },
};
use k8s_openapi::apimachinery::pkg::{
    api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
};
use kube::api::ObjectMeta;

/// The name of every object this front builds; the Ingress is named `spark-connect`.
pub const NAME: &str = "spark-connect-auth";

/// The port the front listens on, and the driver's gRPC port behind it.
pub const PORT: i32 = 15002;

/// The Envoy document itself (`stardelt-platform`'s `data["envoy.yaml"]`, with `__NAMESPACE__`
/// where the two service addresses carried `stardelt`).
pub const ENVOY_CONFIG: &str = include_str!("spark_connect_auth_envoy.yaml");

/// The front's Envoy digest, from `values/spark-connect-auth.yaml` — its own file so the pin
/// check compares one digest with one digest (see the file's comment).
#[derive(serde::Deserialize)]
struct Golden {
    #[serde(rename = "envoyImage")]
    envoy_image: String,
}

fn golden() -> Golden {
    serde_yaml::from_str(include_str!("../values/spark-connect-auth.yaml"))
        .expect("values/spark-connect-auth.yaml is not valid YAML")
}

pub fn envoy_image() -> String {
    golden().envoy_image
}

/// The rendered document: the two namespace-qualified addresses and the issuer's domain.
pub fn envoy_config(namespace: &str, base_domain: &str) -> String {
    ENVOY_CONFIG
        .replace("__NAMESPACE__", namespace)
        .replace("__BASE_DOMAIN__", base_domain)
}

fn name_labels() -> BTreeMap<String, String> {
    BTreeMap::from([("app.kubernetes.io/name".to_string(), NAME.to_string())])
}

fn labels() -> BTreeMap<String, String> {
    let mut labels = name_labels();
    labels.insert(
        "app.kubernetes.io/part-of".to_string(),
        "stardelt".to_string(),
    );
    labels
}

fn meta(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some(namespace.to_string()),
        labels: Some(labels()),
        ..ObjectMeta::default()
    }
}

pub fn config_map(namespace: &str, base_domain: &str) -> ConfigMap {
    ConfigMap {
        metadata: meta(NAME, namespace),
        data: Some(BTreeMap::from([(
            "envoy.yaml".to_string(),
            envoy_config(namespace, base_domain),
        )])),
        ..ConfigMap::default()
    }
}

fn probe(initial_delay_seconds: i32, period_seconds: i32) -> Probe {
    Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::String("connect".into()),
            ..TCPSocketAction::default()
        }),
        initial_delay_seconds: Some(initial_delay_seconds),
        period_seconds: Some(period_seconds),
        ..Probe::default()
    }
}

/// The front itself: one Envoy container, non-root (the image's own user is 101), a read-only
/// root filesystem and the ConfigMap mounted at `/etc/envoy`.
pub fn deployment(namespace: &str) -> Deployment {
    let selector = LabelSelector {
        match_labels: Some(name_labels()),
        ..LabelSelector::default()
    };
    Deployment {
        metadata: meta(NAME, namespace),
        spec: DeploymentSpec {
            replicas: Some(1),
            selector,
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels()),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        run_as_user: Some(101),
                        fs_group: Some(101),
                        seccomp_profile: Some(SeccompProfile {
                            type_: "RuntimeDefault".into(),
                            ..SeccompProfile::default()
                        }),
                        ..PodSecurityContext::default()
                    }),
                    containers: vec![Container {
                        name: "envoy".into(),
                        image: Some(envoy_image()),
                        args: Some(vec![
                            "-c".into(),
                            "/etc/envoy/envoy.yaml".into(),
                            "--log-level".into(),
                            "info".into(),
                        ]),
                        ports: Some(vec![ContainerPort {
                            name: Some("connect".into()),
                            container_port: PORT,
                            protocol: Some("TCP".into()),
                            ..ContainerPort::default()
                        }]),
                        readiness_probe: Some(probe(3, 10)),
                        liveness_probe: Some(probe(15, 20)),
                        resources: Some(ResourceRequirements {
                            requests: Some(BTreeMap::from([
                                ("cpu".to_string(), Quantity("50m".into())),
                                ("memory".to_string(), Quantity("64Mi".into())),
                            ])),
                            limits: Some(BTreeMap::from([
                                ("cpu".to_string(), Quantity("500m".into())),
                                ("memory".to_string(), Quantity("256Mi".into())),
                            ])),
                            ..Default::default()
                        }),
                        security_context: Some(SecurityContext {
                            allow_privilege_escalation: Some(false),
                            capabilities: Some(Capabilities {
                                drop: Some(vec!["ALL".into()]),
                                ..Capabilities::default()
                            }),
                            read_only_root_filesystem: Some(true),
                            ..SecurityContext::default()
                        }),
                        volume_mounts: Some(vec![
                            VolumeMount {
                                name: "config".into(),
                                mount_path: "/etc/envoy".into(),
                                read_only: Some(true),
                                ..VolumeMount::default()
                            },
                            VolumeMount {
                                name: "tmp".into(),
                                mount_path: "/tmp".into(),
                                ..VolumeMount::default()
                            },
                        ]),
                        ..Container::default()
                    }],
                    volumes: Some(vec![
                        Volume {
                            name: "config".into(),
                            config_map: Some(ConfigMapVolumeSource {
                                name: NAME.into(),
                                ..ConfigMapVolumeSource::default()
                            }),
                            ..Volume::default()
                        },
                        Volume {
                            name: "tmp".into(),
                            empty_dir: Some(EmptyDirVolumeSource::default()),
                            ..Volume::default()
                        },
                    ]),
                    ..PodSpec::default()
                }),
            },
            ..DeploymentSpec::default()
        },
        ..Deployment::default()
    }
}

pub fn service(namespace: &str) -> Service {
    Service {
        metadata: meta(NAME, namespace),
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".into()),
            selector: Some(name_labels()),
            ports: Some(vec![ServicePort {
                name: Some("connect".into()),
                port: PORT,
                target_port: Some(IntOrString::String("connect".into())),
                protocol: Some("TCP".into()),
                ..ServicePort::default()
            }]),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    }
}

/// Who may reach the front: the ingress controller (the public name `spark.<base domain>`) and
/// member notebook pods (the in-cluster path a later slice points the library at).
pub fn network_policy(namespace: &str) -> NetworkPolicy {
    let peer = |labels: &[(&str, &str)], namespace_selector: bool| NetworkPolicyPeer {
        pod_selector: Some(LabelSelector {
            match_labels: Some(BTreeMap::from_iter(
                labels
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
            )),
            ..LabelSelector::default()
        }),
        namespace_selector: namespace_selector.then(|| LabelSelector {
            match_labels: Some(BTreeMap::from([(
                "kubernetes.io/metadata.name".to_string(),
                "ingress-nginx".to_string(),
            )])),
            ..LabelSelector::default()
        }),
        ip_block: None::<IPBlock>,
    };
    NetworkPolicy {
        metadata: ObjectMeta {
            name: Some(format!("{NAME}-ingress")),
            namespace: Some(namespace.to_string()),
            labels: Some(labels()),
            ..ObjectMeta::default()
        },
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(LabelSelector {
                match_labels: Some(name_labels()),
                ..LabelSelector::default()
            }),
            policy_types: Some(vec!["Ingress".to_string()]),
            ingress: Some(vec![NetworkPolicyIngressRule {
                from: Some(vec![
                    peer(&[], true),
                    peer(&[("component", "singleuser-server")], false),
                ]),
                ports: Some(vec![NetworkPolicyPort {
                    port: Some(IntOrString::Int(PORT)),
                    protocol: Some("TCP".into()),
                    ..NetworkPolicyPort::default()
                }]),
            }]),
            egress: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_envoy_config_follows_the_namespace_and_the_base_domain() {
        let config = envoy_config("data", "lab.example.org");
        assert!(
            config.contains("spark-connect.data.svc.cluster.local"),
            "{config}"
        );
        assert!(
            config.contains("keycloak.data.svc.cluster.local"),
            "{config}"
        );
        assert!(
            config.contains("issuer: https://id.lab.example.org/realms/stardelt"),
            "{config}"
        );
        assert!(
            config.contains("audiences: [\"spark-connect\"]"),
            "{config}"
        );
        for absent in ["__NAMESPACE__", "__BASE_DOMAIN__", ".stardelt.svc"] {
            assert!(!config.contains(absent), "{absent} left in the document");
        }
    }

    #[test]
    fn the_envoy_config_is_the_platform_manifest() {
        let manifest = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../platform/manifests/spark-connect-auth.yaml"
        ));
        let document = manifest
            .split("\n---")
            .find(|document| document.contains("kind: ConfigMap"))
            .expect("the platform manifest's ConfigMap");
        let parsed: serde_yaml::Value =
            serde_yaml::from_str(document).expect("the ConfigMap document parses");
        let platform = parsed["data"]["envoy.yaml"]
            .as_str()
            .expect("data.envoy.yaml");
        // This is the "a copied script is a dependency" rule (#16 §7): the shipped document is
        // the platform manifest's, with the namespace substituted and nothing else changed. The
        // split drops the file's final line break, so compare the content, not that one byte.
        assert_eq!(
            platform
                .replace(
                    ".stardelt.svc.cluster.local",
                    ".__NAMESPACE__.svc.cluster.local"
                )
                .trim_end(),
            ENVOY_CONFIG.trim_end()
        );
    }

    #[test]
    fn the_deployment_runs_the_golden_envoy_image() {
        let deployment = deployment("data");
        let pod = deployment.spec.template.spec.expect("pod spec");
        let container = &pod.containers[0];
        assert_eq!(container.name, "envoy");
        assert_eq!(container.image.as_deref(), Some(envoy_image().as_str()));
        assert!(
            envoy_image()
                .ends_with("cfe1c570e72aa4207853a37bd1c32c808a0036a351b16f73732dcfd0e5775175")
        );
        assert_eq!(deployment.spec.replicas, Some(1));
    }

    #[test]
    fn the_service_and_the_policy_point_at_the_front() {
        let service = service("data");
        assert_eq!(service.metadata.name.as_deref(), Some(NAME));
        assert_eq!(service.metadata.namespace.as_deref(), Some("data"));
        let spec = service.spec.expect("service spec");
        assert_eq!(spec.type_.as_deref(), Some("ClusterIP"));
        assert_eq!(
            spec.selector.as_ref().expect("selector")["app.kubernetes.io/name"],
            NAME
        );
        let port = &spec.ports.expect("ports")[0];
        assert_eq!(port.name.as_deref(), Some("connect"));
        assert_eq!(port.port, PORT);
        assert_eq!(
            port.target_port,
            Some(IntOrString::String("connect".into()))
        );

        let policy = network_policy("data");
        assert_eq!(
            policy.metadata.name.as_deref(),
            Some("spark-connect-auth-ingress")
        );
        let spec = policy.spec.expect("policy spec");
        assert_eq!(
            spec.pod_selector
                .as_ref()
                .expect("pod selector")
                .match_labels
                .as_ref()
                .expect("labels")["app.kubernetes.io/name"],
            NAME
        );
        let rules = spec.ingress.expect("ingress rules");
        assert_eq!(rules.len(), 1);
        let peers = rules[0].from.as_ref().expect("peers");
        assert_eq!(peers.len(), 2);
        assert_eq!(
            peers[0]
                .namespace_selector
                .as_ref()
                .expect("the ingress controller's namespace")
                .match_labels
                .as_ref()
                .expect("labels")["kubernetes.io/metadata.name"],
            "ingress-nginx"
        );
        assert_eq!(
            peers[1]
                .pod_selector
                .as_ref()
                .expect("pod selector")
                .match_labels
                .as_ref()
                .expect("labels")["component"],
            "singleuser-server"
        );
        let ports = rules[0].ports.as_ref().expect("ports");
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].port, Some(IntOrString::Int(PORT)));
    }
}
