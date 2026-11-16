//! The streaming bus (D42): one KRaft Kafka on Strimzi, one combined controller and broker node pool. Pure builders; no user, no `StreamApp`. A `StreamApp` on the internal bus creates its own `KafkaTopic`.

use kube::api::{DynamicObject, ObjectMeta, TypeMeta};
use serde_json::json;

use crate::crd::Profile;

pub const NAME: &str = "kafka";

/// Strimzi's broker image, pinned by digest as `stardelt-platform/manifests/kafka.yaml` does
/// (platform#1624). The cluster operator's `STRIMZI_KAFKA_IMAGES` map cannot pin the broker, so
/// the `Kafka` resource names the image itself.
pub const BROKER_IMAGE: &str = "quay.io/strimzi/kafka:1.3.1-kafka-4.3.2@sha256:9cea505e434a53cea40941d2c6e9aa5896792fe9c9380feb1d7adea3867a3a0b";

/// The topic and user operator containers, the same digest as `values/kafka.yaml` pins
/// (`STRIMZI_DEFAULT_*_OPERATOR_IMAGE`) and the platform's `manifests/kafka.yaml` sets.
pub const OPERATOR_IMAGE: &str = "quay.io/strimzi/operator:1.3.1@sha256:55f43849559a0d151be473c588c89e9d72649ecadf088c8b9d92d37654a5e5eb";

fn meta(namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(NAME.into()),
        namespace: Some(namespace.into()),
        labels: Some(std::collections::BTreeMap::from([
            (
                "app.kubernetes.io/managed-by".to_string(),
                "stardelt-operator".to_string(),
            ),
            ("stardelt.io/component".to_string(), "kafka".to_string()),
            // Strimzi pairs the pool with its Kafka through this label.
            ("strimzi.io/cluster".to_string(), NAME.to_string()),
        ])),
        ..Default::default()
    }
}

/// `KafkaNodePool` `kafka`: one node that is both controller and broker. The demo profile keeps a
/// 5Gi claim with `deleteClaim: false` — the same as the platform's `manifests/kafka.yaml`, so a
/// broker restart does not delete the topics a `StreamApp` reads; `full` keeps 10Gi.
pub fn node_pool(namespace: &str, profile: Profile) -> DynamicObject {
    let storage = match profile {
        Profile::Demo => {
            json!({"type": "persistent-claim", "size": "5Gi", "deleteClaim": false})
        }
        _ => json!({"type": "persistent-claim", "size": "10Gi", "deleteClaim": false}),
    };
    DynamicObject {
        types: Some(TypeMeta {
            api_version: "kafka.strimzi.io/v1".into(),
            kind: "KafkaNodePool".into(),
        }),
        metadata: meta(namespace),
        data: json!({"spec": {
            "replicas": 1,
            "roles": ["controller", "broker"],
            "storage": storage,
            "resources": {
                "requests": {"cpu": "500m", "memory": "1Gi"},
                "limits": {"cpu": "1", "memory": "1536Mi"},
            },
            "jvmOptions": {"-Xms": "1g", "-Xmx": "1g"},
            // Pod Security `restricted` (platform#1096 question 5, twin of platform#1197).
            // Node pools are enabled, so the broker pod is rendered from THIS template — a
            // `Kafka.spec.kafka.template` would be ignored. `fsGroup: 0` is what Strimzi sets
            // today; keep it so the topic volume's group ownership does not change.
            "template": {
                "pod": {"securityContext": {
                    "fsGroup": 0,
                    "runAsNonRoot": true,
                    "seccompProfile": {"type": "RuntimeDefault"},
                }},
                "kafkaContainer": {"securityContext": {
                    "allowPrivilegeEscalation": false,
                    "capabilities": {"drop": ["ALL"]},
                    "seccompProfile": {"type": "RuntimeDefault"},
                }},
            },
        }}),
    }
}

/// `Kafka` `kafka`: one plain listener on 9092, in-cluster only (no Ingress, no TLS, in step
/// with Spark Connect's ClusterIP stance). Replication factors are 1 because there is one node.
pub fn broker(namespace: &str) -> DynamicObject {
    let mut metadata = meta(namespace);
    metadata.annotations = Some(std::collections::BTreeMap::from([(
        "strimzi.io/node-pools".to_string(),
        "enabled".to_string(),
    )]));
    DynamicObject {
        types: Some(TypeMeta {
            api_version: "kafka.strimzi.io/v1".into(),
            kind: "Kafka".into(),
        }),
        metadata,
        data: json!({
            "spec": {
                "kafka": {
                    "image": BROKER_IMAGE,
                    "listeners": [{"name": "plain", "port": 9092, "type": "internal", "tls": false}],
                    "config": {
                        "offsets.topic.replication.factor": 1,
                        "transaction.state.log.replication.factor": 1,
                        "transaction.state.log.min.isr": 1,
                        "default.replication.factor": 1,
                        "min.insync.replicas": 1,
                    },
                },
                // Pod Security `restricted` (platform#1096 question 5). The entity operator is a
                // `Kafka` workload (not a node pool), so its template is here; both operator
                // containers need the same block.
                "entityOperator": {
                    "topicOperator": {"image": OPERATOR_IMAGE},
                    "userOperator": {"image": OPERATOR_IMAGE},
                    "template": {
                        "pod": {"securityContext": {
                            "runAsNonRoot": true,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        }},
                        "topicOperatorContainer": {"securityContext": {
                            "allowPrivilegeEscalation": false,
                            "capabilities": {"drop": ["ALL"]},
                            "seccompProfile": {"type": "RuntimeDefault"},
                        }},
                        "userOperatorContainer": {"securityContext": {
                            "allowPrivilegeEscalation": false,
                            "capabilities": {"drop": ["ALL"]},
                            "seccompProfile": {"type": "RuntimeDefault"},
                        }},
                    },
                },
            },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_objects_use_the_served_api_version() {
        // Strimzi 1.3.1 (planner.rs chart_version) serves only kafka.strimzi.io/v1; v1beta2 is a
        // 404 that ensure_kafka_broker reads as "waiting for Strimzi" forever.
        assert_eq!(
            node_pool("data", Profile::Demo).types.unwrap().api_version,
            "kafka.strimzi.io/v1"
        );
        assert_eq!(
            broker("data").types.unwrap().api_version,
            "kafka.strimzi.io/v1"
        );
    }

    #[test]
    fn node_pool_is_one_combined_pool() {
        let pool = node_pool("data", Profile::Demo);
        assert_eq!(pool.data["spec"]["replicas"], 1);
        assert_eq!(pool.data["spec"]["roles"], json!(["controller", "broker"]));
        assert_eq!(
            pool.data["spec"]["storage"],
            json!({"type": "persistent-claim", "size": "5Gi", "deleteClaim": false})
        );
        let labels = pool.metadata.labels.as_ref().expect("labels");
        assert_eq!(labels.get("strimzi.io/cluster"), Some(&"kafka".to_string()));
    }

    #[test]
    fn full_profile_keeps_the_log_on_a_claim() {
        let pool = node_pool("data", Profile::Full);
        assert_eq!(
            pool.data["spec"]["storage"],
            json!({"type": "persistent-claim", "size": "10Gi", "deleteClaim": false})
        );
    }

    #[test]
    fn broker_listens_plain_inside_the_cluster() {
        let broker = broker("data");
        assert_eq!(
            broker.data["spec"]["kafka"]["listeners"],
            json!([{"name": "plain", "port": 9092, "type": "internal", "tls": false}])
        );
        let config = &broker.data["spec"]["kafka"]["config"];
        assert_eq!(config["offsets.topic.replication.factor"], 1);
        assert_eq!(config["transaction.state.log.replication.factor"], 1);
        assert_eq!(config["default.replication.factor"], 1);
        assert_eq!(
            broker
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("strimzi.io/node-pools")),
            Some(&"enabled".to_string())
        );
    }

    #[test]
    fn node_pool_template_declares_the_restricted_fields() {
        // Node pools are enabled: the broker pod comes from KafkaNodePool.spec.template.
        let pool = node_pool("data", Profile::Demo);
        assert_eq!(
            pool.data["spec"]["template"]["pod"]["securityContext"],
            json!({"fsGroup": 0, "runAsNonRoot": true,
                   "seccompProfile": {"type": "RuntimeDefault"}})
        );
        assert_eq!(
            pool.data["spec"]["template"]["kafkaContainer"]["securityContext"],
            json!({"allowPrivilegeEscalation": false,
                   "capabilities": {"drop": ["ALL"]},
                   "seccompProfile": {"type": "RuntimeDefault"}})
        );
    }

    #[test]
    fn entity_operator_template_declares_the_restricted_fields() {
        let broker = broker("data");
        let template = &broker.data["spec"]["entityOperator"]["template"];
        assert_eq!(
            template["pod"]["securityContext"],
            json!({"runAsNonRoot": true, "seccompProfile": {"type": "RuntimeDefault"}})
        );
        for name in ["topicOperatorContainer", "userOperatorContainer"] {
            assert_eq!(
                template[name]["securityContext"],
                json!({"allowPrivilegeEscalation": false,
                       "capabilities": {"drop": ["ALL"]},
                       "seccompProfile": {"type": "RuntimeDefault"}}),
                "{name} would keep the restricted violation"
            );
        }
    }

    #[test]
    fn the_broker_and_both_entity_operators_are_pinned_by_digest() {
        // platform#1624 pinned these in the platform path; the operator path must not float.
        assert_eq!(
            BROKER_IMAGE,
            "quay.io/strimzi/kafka:1.3.1-kafka-4.3.2@sha256:9cea505e434a53cea40941d2c6e9aa5896792fe9c9380feb1d7adea3867a3a0b"
        );
        assert_eq!(
            OPERATOR_IMAGE,
            "quay.io/strimzi/operator:1.3.1@sha256:55f43849559a0d151be473c588c89e9d72649ecadf088c8b9d92d37654a5e5eb"
        );
        let kafka = broker("data");
        assert_eq!(kafka.data["spec"]["kafka"]["image"], BROKER_IMAGE);
        assert_eq!(
            kafka.data["spec"]["entityOperator"]["topicOperator"]["image"],
            OPERATOR_IMAGE
        );
        assert_eq!(
            kafka.data["spec"]["entityOperator"]["userOperator"]["image"],
            OPERATOR_IMAGE
        );
    }
}
