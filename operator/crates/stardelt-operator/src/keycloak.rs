//! The Keycloak workload for `identity.mode: platform` (master spec §5 row 3): the realm and
//! environment ConfigMaps, the Service and the Deployment. Pure builders: they return typed
//! objects and touch no cluster; [`crate::provision::ensure_keycloak`] applies them.
//!
//! Every field mirrors `stardelt-platform` `manifests/keycloak.yaml` (the Service and the
//! Deployment) and `chart/templates/keycloak.yaml` (the two ConfigMaps), so the operator can
//! take over the identity workload without a Helm chart of its own. Hosts come from
//! `spec.baseDomain`: Nova answers on the base domain, every other application on a
//! subdomain — the chart's `stardelt.appHost`.

use std::collections::BTreeMap;

use k8s_openapi::{
    ByteString,
    api::{
        apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy},
        core::v1::{
            Capabilities, ConfigMap, ConfigMapEnvSource, ConfigMapVolumeSource, Container,
            ContainerPort, EnvFromSource, EnvVar, EnvVarSource, HTTPGetAction, PodSecurityContext,
            PodSpec, PodTemplateSpec, Probe, ResourceRequirements, SeccompProfile, Secret,
            SecretKeySelector, SecurityContext, Service, ServicePort, ServiceSpec, Volume,
            VolumeMount,
        },
    },
    apimachinery::pkg::{
        api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
    },
};
use kube::api::ObjectMeta;

use crate::{
    crd::{IdentityLdap, IdentityLdapTls, PlatformInstanceSpec},
    identity::{self, IdentityError},
};

/// Name of the Service, the Deployment, the container and of `app.kubernetes.io/name`.
pub const NAME: &str = "keycloak";
/// ConfigMap holding the realm import.
pub const REALM_CONFIG_MAP: &str = "keycloak-realm";
/// ConfigMap holding `KC_HOSTNAME`.
pub const ENV_CONFIG_MAP: &str = "keycloak-env";
/// Key of the realm import inside [`REALM_CONFIG_MAP`].
pub const REALM_KEY: &str = "stardelt-realm.json";
/// Where Keycloak reads realm imports from.
pub const REALM_MOUNT: &str = "/opt/keycloak/data/import";
/// Image of the identity provider. `stardelt-platform` pins the same tag in the Makefile, the
/// manifest and `chart/values.yaml` (their `check-version-parity.sh` keeps the three equal).
pub const IMAGE: &str = "quay.io/keycloak/keycloak:26.8.2@sha256:c68e286cacd87b942ac1939a0a1481461109d6acd3a57dc496969a319a76bbf0";

/// The realm template, verbatim from `stardelt-platform` (`chart/files/keycloak-realm.json`).
/// Hosts are rendered per instance; the client secrets stay `${STARDELT_*_SECRET}` placeholders
/// that Keycloak resolves from the Deployment's environment.
pub const REALM_TEMPLATE: &str = include_str!("keycloak_realm.json");

/// A Secret the platform IdP needs, as `stardelt-platform` `scripts/identity-bootstrap.sh`
/// creates it. `fixed` keys carry the same literal value on every install; `generated` keys get a
/// fresh [`generate_secret`] value when the key is first added. A key that is already there is
/// never rewritten.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentitySecret {
    /// Name of the Secret.
    pub name: &'static str,
    /// Keys that always hold the same literal value (`username=admin`).
    pub fixed: &'static [(&'static str, &'static str)],
    /// Keys generated once, then kept for the life of the install.
    pub generated: &'static [&'static str],
    /// Keys that hold a Fernet key (32 random bytes, url-safe base64, 44 characters): generated
    /// once and kept, like `generated`, but in the format Airflow's encryption needs.
    pub fernet: &'static [&'static str],
}

impl IdentitySecret {
    /// Every key of the Secret, fixed keys first, then generated ones — the order
    /// `identity-bootstrap.sh` writes them in.
    pub fn key_names(&self) -> Vec<&'static str> {
        self.fixed
            .iter()
            .map(|(key, _)| *key)
            .chain(self.generated.iter().copied())
            .chain(self.fernet.iter().copied())
            .collect()
    }
}

/// The identity Secrets the operator manages for `identity.mode: platform`, with the exact names
/// and key sets of `scripts/identity-bootstrap.sh` (the same keys the realm's
/// `${STARDELT_*_SECRET}` placeholders resolve from). The script also creates
/// `stardelt-nova-session` and the Trino Secrets; those belong to the Nova and Trino components,
/// not to Keycloak, and are not managed here.
pub const IDENTITY_SECRETS: [IdentitySecret; 3] = [
    IdentitySecret {
        name: "keycloak-admin",
        fixed: &[("username", "admin")],
        generated: &["password"],
        fernet: &[],
    },
    IdentitySecret {
        name: "stardelt-realm-admin",
        fixed: &[("username", "admin")],
        generated: &["password"],
        fernet: &[],
    },
    IdentitySecret {
        name: "stardelt-oidc-clients",
        fixed: &[],
        generated: &[
            "nova",
            "superset",
            "airflow",
            "jupyterhub",
            "trino",
            "trino-group-sync",
            "pipeline-runner",
        ],
        fernet: &[],
    },
];

/// A random hex string of `len` random bytes (two lower-case hex characters each). The 24-byte
/// case is [`generate_secret`]; OpenBao's static seal key needs 32 bytes (64 hex characters),
/// the same size `openssl rand -hex 32` produces in `scripts/openbao-seal-key.sh`.
pub fn generate_hex(len: usize) -> Result<String, IdentityError> {
    let mut bytes = vec![0u8; len];
    getrandom::fill(&mut bytes)
        .map_err(|e| IdentityError(format!("cannot generate a credential: {e}")))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// One generated credential: 48 lower-case hex chars (`openssl rand -hex 24` equivalent), from
/// the same `getrandom` the S3 keys use. A failure is returned, never panicked on: the caller
/// reports it as a status message.
pub fn generate_secret() -> Result<String, IdentityError> {
    let mut bytes = [0u8; 24];
    getrandom::fill(&mut bytes)
        .map_err(|e| IdentityError(format!("cannot generate an identity credential: {e}")))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

/// The alphabet of URL-safe base64 (RFC 4648 section 5).
const BASE64_URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// URL-safe base64 with `=` padding. Written here so that a Fernet key needs no new dependency.
fn base64_url(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let group = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for position in 0..4 {
            if position <= chunk.len() {
                out.push(char::from(
                    BASE64_URL[((group >> (18 - 6 * position)) & 63) as usize],
                ));
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// A Fernet key: 32 random bytes as URL-safe base64, 44 characters ending in `=`. Airflow encrypts
/// the connections it stores with it and refuses to start with anything else.
pub fn generate_fernet_key() -> Result<String, IdentityError> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|e| IdentityError(format!("cannot generate a Fernet key: {e}")))?;
    Ok(base64_url(&bytes))
}

/// Which keys a Secret still needs: `desired` minus the keys in `existing`, in `desired` order.
/// The values in `existing` are irrelevant — the operator never rewrites a key that is there, so
/// it only needs to know which names are present — and keys nobody asked for are ignored.
pub fn missing_keys(existing: &BTreeMap<String, String>, desired: &[&str]) -> Vec<String> {
    desired
        .iter()
        .filter(|key| !existing.contains_key(**key))
        .map(|key| (*key).to_string())
        .collect()
}

/// The Secret for one row of [`IDENTITY_SECRETS`], applied only when the Secret is absent: fixed
/// keys as given, generated keys with a fresh credential.
pub fn identity_secret(desired: &IdentitySecret, namespace: &str) -> Result<Secret, IdentityError> {
    let mut data = BTreeMap::new();
    for (key, value) in desired.fixed {
        data.insert((*key).to_string(), ByteString(value.as_bytes().to_vec()));
    }
    for key in desired.generated {
        data.insert(
            (*key).to_string(),
            ByteString(generate_secret()?.into_bytes()),
        );
    }
    for key in desired.fernet {
        data.insert(
            (*key).to_string(),
            ByteString(generate_fernet_key()?.into_bytes()),
        );
    }
    Ok(Secret {
        metadata: meta(desired.name, namespace),
        data: Some(data),
        ..Secret::default()
    })
}

/// The realm's `components` object for a customer directory: one read-only LDAP federation and
/// the two mappers that make an imported user signable (`#39`, measured in `#1516` on 26.8.2).
///
/// Three shapes are refused by Keycloak's realm import and must not be "simplified":
/// `subComponents` is a sibling of `config` (inside it: `Cannot deserialize value of type
/// ArrayList<String> from Object value`); a components entry has no `providerType` key (the map
/// key is the provider type); and without the `username` `user-attribute-ldap-mapper` the import
/// succeeds but every sign-in fails `User returned from LDAP has null username`. The factory
/// creates seven mappers for an API-created provider; the five others only fill names, mail and
/// timestamps and are not needed for the `groups` claim the platform reads.
fn ldap_components(ldap: &IdentityLdap) -> serde_json::Value {
    let sync = ldap.sync_seconds.to_string();
    let scheme = match ldap.tls {
        IdentityLdapTls::Ldaps => "ldaps",
        _ => "ldap",
    };
    let mut config = serde_json::Map::new();
    config.insert("enabled".into(), serde_json::json!(["true"]));
    config.insert("vendor".into(), serde_json::json!(["other"]));
    config.insert(
        "connectionUrl".into(),
        serde_json::json!([format!("{scheme}://{}:{}", ldap.host, ldap.port)]),
    );
    // Keycloak reads `startTls` as a string list; a `null` value would fail the import, so
    // the key is only present for `starttls`.
    if ldap.tls == IdentityLdapTls::Starttls {
        config.insert("startTls".into(), serde_json::json!(["true"]));
    }
    config.insert("usersDn".into(), serde_json::json!([ldap.users_dn.clone()]));
    config.insert("authType".into(), serde_json::json!(["simple"]));
    // Resolved by Keycloak's ${...} placeholders from the container env, so the
    // bind DN and its password are never written into the ConfigMap.
    config.insert(
        "bindDn".into(),
        serde_json::json!(["${STARDELT_LDAP_BIND_DN}"]),
    );
    config.insert(
        "bindCredential".into(),
        serde_json::json!(["${STARDELT_LDAP_BIND_PASSWORD}"]),
    );
    config.insert("editMode".into(), serde_json::json!(["READ_ONLY"]));
    config.insert("usernameLDAPAttribute".into(), serde_json::json!(["cn"]));
    config.insert("rdnLDAPAttribute".into(), serde_json::json!(["cn"]));
    config.insert("uuidLDAPAttribute".into(), serde_json::json!(["entryUUID"]));
    config.insert(
        "userObjectClasses".into(),
        serde_json::json!(["inetOrgPerson, organizationalPerson"]),
    );
    config.insert("searchScope".into(), serde_json::json!(["2"]));
    config.insert("importEnabled".into(), serde_json::json!(["true"]));
    config.insert("fullSyncPeriod".into(), serde_json::json!([sync.clone()]));
    config.insert("changedSyncPeriod".into(), serde_json::json!([sync]));
    serde_json::json!({
        "org.keycloak.storage.UserStorageProvider": [{
            "name": "ldap",
            "providerId": "ldap",
            "config": config,
            "subComponents": {
                "org.keycloak.storage.ldap.mappers.LDAPStorageMapper": [{
                    "name": "username",
                    "providerId": "user-attribute-ldap-mapper",
                    "config": {
                        "ldap.attribute": ["cn"],
                        "user.model.attribute": ["username"],
                        "read.only": ["true"],
                        "is.mandatory.in.ldap": ["true"],
                        "always.read.value.from.ldap": ["false"],
                    },
                }, {
                    "name": "groups",
                    "providerId": "group-ldap-mapper",
                    "config": {
                        "groups.dn": [ldap.group_dn.clone()],
                        "group.object.classes": ["groupOfNames"],
                        "group.name.ldap.attribute": ["cn"],
                        "membership.ldap.attribute": ["member"],
                        "membership.user.ldap.attribute": ["cn"],
                        "groups.path": ["/"],
                        "mode": ["READ_ONLY"],
                        "drop.non.existing.groups.during.sync": ["false"],
                    },
                }],
            },
        }],
    })
}

/// The Realm import of this instance, or the reason it cannot be rendered.
fn realm_json(spec: &PlatformInstanceSpec) -> Result<String, IdentityError> {
    let domain = identity::base_domain(spec)?;
    let hosts = identity::hosts(domain);
    let mut realm = REALM_TEMPLATE.to_string();
    for (placeholder, host) in [
        ("__BASE_DOMAIN__", &hosts.nova),
        ("__NOVA_HOST__", &hosts.nova),
        ("__TRINO_HOST__", &hosts.trino),
        ("__LAKEKEEPER_HOST__", &hosts.lakekeeper),
        ("__SUPERSET_HOST__", &hosts.superset),
        ("__AIRFLOW_HOST__", &hosts.airflow),
        ("__JUPYTER_HOST__", &hosts.jupyter),
    ] {
        realm = realm.replace(placeholder, host);
    }
    // Without a directory the function returns the template as it is: the chart does not
    // re-serialise either, and the two must render the same realm.
    let Some(ldap) = spec.identity.ldap.as_ref() else {
        return Ok(realm);
    };
    let mut parsed: serde_json::Value = serde_json::from_str(&realm)
        .map_err(|e| IdentityError(format!("keycloak_realm.json is not a JSON document: {e}")))?;
    if let Some(object) = parsed.as_object_mut() {
        object.insert("components".into(), ldap_components(ldap));
    }
    serde_json::to_string_pretty(&parsed)
        .map_err(|e| IdentityError(format!("the realm cannot be serialised: {e}")))
}

/// Metadata of an object that belongs to the platform rather than to a Lakehouse.
fn meta(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.into()),
        namespace: Some(namespace.into()),
        labels: Some(BTreeMap::from([(
            "app.kubernetes.io/part-of".to_string(),
            "stardelt".to_string(),
        )])),
        ..ObjectMeta::default()
    }
}

/// The realm import Keycloak reads on first start (`--import-realm`).
pub fn realm_config_map(spec: &PlatformInstanceSpec) -> Result<ConfigMap, IdentityError> {
    Ok(ConfigMap {
        metadata: meta(REALM_CONFIG_MAP, &spec.namespace),
        data: Some(BTreeMap::from([(REALM_KEY.to_string(), realm_json(spec)?)])),
        ..ConfigMap::default()
    })
}

/// The environment of the Deployment: `KC_HOSTNAME` pins the issuer to `https://id.<domain>`.
pub fn env_config_map(spec: &PlatformInstanceSpec) -> Result<ConfigMap, IdentityError> {
    let domain = identity::base_domain(spec)?;
    Ok(ConfigMap {
        metadata: meta(ENV_CONFIG_MAP, &spec.namespace),
        data: Some(BTreeMap::from([(
            "KC_HOSTNAME".to_string(),
            format!("https://id.{domain}"),
        )])),
        ..ConfigMap::default()
    })
}

/// The pod labels, the Deployment selector and the Service selector.
fn name_selector() -> BTreeMap<String, String> {
    BTreeMap::from([("app.kubernetes.io/name".to_string(), NAME.to_string())])
}

fn workload_labels() -> BTreeMap<String, String> {
    let mut labels = name_selector();
    labels.insert(
        "app.kubernetes.io/part-of".to_string(),
        "stardelt".to_string(),
    );
    labels
}

/// The Service in front of the Deployment; the ingress (installed by the platform, not by
/// this operator) targets it by name.
pub fn service(namespace: &str) -> Service {
    Service {
        metadata: ObjectMeta {
            labels: Some(workload_labels()),
            ..meta(NAME, namespace)
        },
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".into()),
            selector: Some(name_selector()),
            ports: Some(vec![
                ServicePort {
                    name: Some("http".into()),
                    port: 8080,
                    target_port: Some(IntOrString::String("http".into())),
                    ..ServicePort::default()
                },
                // Prometheus text on the management port (#607); the ingress never exposes 9000.
                ServicePort {
                    name: Some("metrics".into()),
                    port: 9000,
                    target_port: Some(IntOrString::String("management".into())),
                    ..ServicePort::default()
                },
            ]),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    }
}

fn value_env(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.into(),
        value: Some(value.into()),
        ..EnvVar::default()
    }
}

fn secret_env(name: &str, secret: &str, key: &str) -> EnvVar {
    EnvVar {
        name: name.into(),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: secret.into(),
                key: key.into(),
                optional: None,
            }),
            ..EnvVarSource::default()
        }),
        ..EnvVar::default()
    }
}

fn probe(path: &str, period_seconds: i32, failure_threshold: Option<i32>) -> Probe {
    Probe {
        http_get: Some(HTTPGetAction {
            path: Some(path.into()),
            port: IntOrString::String("management".into()),
            ..HTTPGetAction::default()
        }),
        period_seconds: Some(period_seconds),
        failure_threshold,
        ..Probe::default()
    }
}

fn port(name: &str, container_port: i32) -> ContainerPort {
    ContainerPort {
        name: Some(name.into()),
        container_port,
        ..ContainerPort::default()
    }
}

fn resources() -> ResourceRequirements {
    let quantity = |values: [(&str, &str); 2]| {
        values
            .into_iter()
            .map(|(name, value)| (name.to_string(), Quantity(value.into())))
            .collect::<BTreeMap<_, _>>()
    };
    ResourceRequirements {
        requests: Some(quantity([("cpu", "100m"), ("memory", "512Mi")])),
        limits: Some(quantity([("cpu", "1"), ("memory", "1Gi")])),
        ..ResourceRequirements::default()
    }
}

fn container(ldap: Option<&IdentityLdap>) -> Container {
    let mut env = vec![
        value_env("KC_DB", "postgres"),
        value_env(
            "KC_DB_URL",
            "jdbc:postgresql://keycloak-pg-rw:5432/keycloak",
        ),
        secret_env("KC_DB_USERNAME", "keycloak-pg-app", "username"),
        secret_env("KC_DB_PASSWORD", "keycloak-pg-app", "password"),
        // TLS terminates at the ingress; trust its X-Forwarded-* headers.
        value_env("KC_HTTP_ENABLED", "true"),
        value_env("KC_PROXY_HEADERS", "xforwarded"),
        value_env("KC_HEALTH_ENABLED", "true"),
        // Prometheus text on the management port (#607).
        value_env("KC_METRICS_ENABLED", "true"),
        value_env("KC_CACHE", "local"),
        value_env(
            "JAVA_OPTS_KC_HEAP",
            "-XX:MaxRAMPercentage=65 -XX:InitialRAMPercentage=25",
        ),
        secret_env("KC_BOOTSTRAP_ADMIN_USERNAME", "keycloak-admin", "username"),
        secret_env("KC_BOOTSTRAP_ADMIN_PASSWORD", "keycloak-admin", "password"),
        // Resolved by Keycloak's ${...} placeholders in the realm import, so no secret is
        // ever written into the ConfigMap.
        secret_env(
            "STARDELT_REALM_ADMIN_PASSWORD",
            "stardelt-realm-admin",
            "password",
        ),
        secret_env(
            "STARDELT_NOVA_CLIENT_SECRET",
            "stardelt-oidc-clients",
            "nova",
        ),
        secret_env(
            "STARDELT_SUPERSET_CLIENT_SECRET",
            "stardelt-oidc-clients",
            "superset",
        ),
        secret_env(
            "STARDELT_AIRFLOW_CLIENT_SECRET",
            "stardelt-oidc-clients",
            "airflow",
        ),
        secret_env(
            "STARDELT_JUPYTERHUB_CLIENT_SECRET",
            "stardelt-oidc-clients",
            "jupyterhub",
        ),
        // Trino authenticates its OAuth2 client with this secret (Trino 480 rejects a
        // public client at startup).
        secret_env(
            "STARDELT_TRINO_CLIENT_SECRET",
            "stardelt-oidc-clients",
            "trino",
        ),
        // The continuous group-file sync (platform#167) and the scheduled pipelines
        // (platform#118) each run as their own service-account client.
        secret_env(
            "STARDELT_TRINO_GROUP_SYNC_CLIENT_SECRET",
            "stardelt-oidc-clients",
            "trino-group-sync",
        ),
        secret_env(
            "STARDELT_PIPELINE_RUNNER_CLIENT_SECRET",
            "stardelt-oidc-clients",
            "pipeline-runner",
        ),
    ];
    // The bind DN and its password live in the Secret the spec names; Keycloak resolves them
    // from these two variables while it imports the realm (#39).
    if let Some(ldap) = ldap {
        env.push(secret_env(
            "STARDELT_LDAP_BIND_DN",
            &ldap.bind_secret_ref.name,
            &ldap.bind_secret_ref.dn_key,
        ));
        env.push(secret_env(
            "STARDELT_LDAP_BIND_PASSWORD",
            &ldap.bind_secret_ref.name,
            &ldap.bind_secret_ref.password_key,
        ));
    }
    Container {
        name: NAME.into(),
        image: Some(IMAGE.into()),
        // Production mode without `--optimized`: the stock image has no pre-built Postgres
        // config, so Keycloak runs its build step on boot. `--import-realm` creates realm
        // `stardelt` from [`REALM_MOUNT`] on first start and skips it afterwards.
        args: Some(vec!["start".into(), "--import-realm".into()]),
        env_from: Some(vec![EnvFromSource {
            config_map_ref: Some(ConfigMapEnvSource {
                name: ENV_CONFIG_MAP.into(),
                optional: None,
            }),
            ..EnvFromSource::default()
        }]),
        env: Some(env),
        ports: Some(vec![port("http", 8080), port("management", 9000)]),
        startup_probe: Some(probe("/health/started", 5, Some(60))),
        readiness_probe: Some(probe("/health/ready", 10, None)),
        liveness_probe: Some(probe("/health/live", 20, Some(5))),
        resources: Some(resources()),
        security_context: Some(SecurityContext {
            allow_privilege_escalation: Some(false),
            capabilities: Some(Capabilities {
                drop: Some(vec!["ALL".into()]),
                ..Capabilities::default()
            }),
            ..SecurityContext::default()
        }),
        volume_mounts: Some(vec![VolumeMount {
            name: "realm".into(),
            mount_path: REALM_MOUNT.into(),
            read_only: Some(true),
            ..VolumeMount::default()
        }]),
        ..Container::default()
    }
}

/// The Keycloak Deployment. Single replica: `KC_CACHE=local`, no clustering.
pub fn deployment(namespace: &str, ldap: Option<&IdentityLdap>) -> Deployment {
    Deployment {
        metadata: ObjectMeta {
            labels: Some(workload_labels()),
            ..meta(NAME, namespace)
        },
        spec: DeploymentSpec {
            replicas: Some(1),
            strategy: Some(DeploymentStrategy {
                type_: Some("Recreate".into()),
                ..DeploymentStrategy::default()
            }),
            selector: LabelSelector {
                match_labels: Some(name_selector()),
                ..LabelSelector::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(workload_labels()),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    automount_service_account_token: Some(false),
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        seccomp_profile: Some(SeccompProfile {
                            type_: "RuntimeDefault".into(),
                            ..SeccompProfile::default()
                        }),
                        ..PodSecurityContext::default()
                    }),
                    containers: vec![container(ldap)],
                    volumes: Some(vec![Volume {
                        name: "realm".into(),
                        config_map: Some(ConfigMapVolumeSource {
                            name: REALM_CONFIG_MAP.into(),
                            ..ConfigMapVolumeSource::default()
                        }),
                        ..Volume::default()
                    }]),
                    ..PodSpec::default()
                }),
            },
            ..DeploymentSpec::default()
        },
        ..Deployment::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The placeholder names the platform chart substitutes.
    const PLACEHOLDERS: [&str; 7] = [
        "__BASE_DOMAIN__",
        "__NOVA_HOST__",
        "__TRINO_HOST__",
        "__LAKEKEEPER_HOST__",
        "__SUPERSET_HOST__",
        "__AIRFLOW_HOST__",
        "__JUPYTER_HOST__",
    ];

    fn spec(base_domain: Option<&str>) -> PlatformInstanceSpec {
        serde_json::from_value(serde_json::json!({
            "namespace": "stardelt",
            "identity": { "mode": "platform" },
            "baseDomain": base_domain,
        }))
        .expect("a PlatformInstanceSpec")
    }

    #[test]
    fn the_realm_config_map_has_the_substituted_template_under_its_documented_key() {
        let cm = realm_config_map(&spec(Some("data.example.com"))).expect("a ConfigMap");
        assert_eq!(cm.metadata.name.as_deref(), Some("keycloak-realm"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("stardelt"));
        assert_eq!(
            cm.metadata.labels.as_ref().unwrap()["app.kubernetes.io/part-of"],
            "stardelt"
        );
        let data = cm.data.expect("data");
        assert_eq!(data.len(), 1, "one key: {data:?}");
        let realm = &data["stardelt-realm.json"];
        for placeholder in PLACEHOLDERS {
            assert!(
                !realm.contains(placeholder),
                "{placeholder} was not substituted"
            );
        }
        assert!(!realm.contains("__"), "a placeholder is left in the realm");
        // The hosts the clients are registered for.
        for host in [
            "admin@data.example.com",
            "https://data.example.com/api/auth/callback",
            "https://trino.data.example.com/oauth2/callback",
            "https://lakekeeper.data.example.com/ui/*",
            "https://superset.data.example.com/oauth-authorized/keycloak",
            "https://airflow.data.example.com/auth/oauth-authorized/keycloak",
            "https://jupyter.data.example.com/hub/oauth_callback",
        ] {
            assert!(realm.contains(host), "the realm has no {host}");
        }
        // Substituting hosts keeps the template a valid realm import, and its client secrets
        // stay ${...} placeholders that Keycloak resolves from the environment.
        let parsed: serde_json::Value = serde_json::from_str(realm).expect("the realm is JSON");
        assert_eq!(parsed["realm"], "stardelt");
        assert_eq!(parsed["users"][0]["email"], "admin@data.example.com");
        assert_eq!(
            parsed["clients"][0]["secret"],
            "${STARDELT_NOVA_CLIENT_SECRET}"
        );
    }

    #[test]
    fn every_realm_description_fits_keycloaks_255_character_column() {
        // CLIENT.DESCRIPTION, KEYCLOAK_ROLE.DESCRIPTION and KEYCLOAK_GROUP.DESCRIPTION are varchar(255):
        // a longer one makes the first import fail and Keycloak crash-loop (stardelt-platform#293 run).
        fn walk(v: &serde_json::Value, path: &str, bad: &mut Vec<String>) {
            match v {
                serde_json::Value::Object(m) => {
                    for (k, x) in m {
                        if k == "description"
                            && let Some(s) = x.as_str()
                            && s.chars().count() > 255
                        {
                            bad.push(format!("{path}.{k} ({} chars)", s.chars().count()));
                        }
                        walk(x, &format!("{path}.{k}"), bad);
                    }
                }
                serde_json::Value::Array(a) => {
                    for (i, x) in a.iter().enumerate() {
                        walk(x, &format!("{path}[{i}]"), bad)
                    }
                }
                _ => {}
            }
        }
        let realm: serde_json::Value =
            serde_json::from_str(REALM_TEMPLATE).expect("the realm template is JSON");
        let mut bad = Vec::new();
        walk(&realm, "", &mut bad);
        assert!(bad.is_empty(), "descriptions over 255 characters: {bad:?}");
    }

    /// `stardelt-mcp` is the client MCP agents sign in with (operator#69 slice 1): public, PKCE
    /// and device grant, and its `aud-mcp` mapper mints the audience Nova's `/mcp` validates.
    #[test]
    fn the_realm_creates_the_mcp_client() {
        let realm = realm_config_map(&spec(Some("data.example.com")))
            .expect("a ConfigMap")
            .data
            .expect("data")
            .remove("stardelt-realm.json")
            .expect("the realm");
        let parsed: serde_json::Value = serde_json::from_str(&realm).expect("the realm is JSON");
        let client = parsed["clients"]
            .as_array()
            .expect("clients")
            .iter()
            .find(|c| c["clientId"] == "stardelt-mcp")
            .expect("the realm has no stardelt-mcp client");
        assert_eq!(client["publicClient"].as_bool(), Some(true));
        assert_eq!(
            client["attributes"]["oauth2.device.authorization.grant.enabled"],
            "true"
        );
        let mappers = client["protocolMappers"].as_array().expect("mappers");
        let mapper = |name: &str| {
            mappers
                .iter()
                .find(|m| m["name"] == name)
                .unwrap_or_else(|| panic!("the client has no {name} mapper"))
        };
        assert_eq!(
            mapper("aud-mcp")["config"]["included.custom.audience"],
            "https://data.example.com/mcp"
        );
        assert!(
            mappers.iter().all(|m| m["name"] != "aud-trino"),
            "the MCP token must not carry the engine audience"
        );
        mapper("groups");
        // Nova's /mcp requires `stardelt:read` on every call and `stardelt:write` on a run.
        let default_scopes = client["defaultClientScopes"]
            .as_array()
            .expect("defaultClientScopes");
        assert!(
            default_scopes.iter().any(|s| s == "stardelt:read"),
            "stardelt:read is not a default scope"
        );
        assert_eq!(
            client["optionalClientScopes"],
            serde_json::json!(["stardelt:write"])
        );
        let realm_scopes: std::collections::BTreeSet<&str> = parsed["clientScopes"]
            .as_array()
            .expect("clientScopes")
            .iter()
            .map(|s| s["name"].as_str().expect("a scope name"))
            .collect();
        assert_eq!(
            realm_scopes,
            ["kc-admin-api", "stardelt:read", "stardelt:write"]
                .into_iter()
                .collect()
        );
    }

    /// A DeployStage's client authenticates with `clientAuthenticatorType=federated-jwt` and
    /// `jwt.credential.issuer=github-actions`; Keycloak resolves that alias only from a realm
    /// identity provider. The entry is enabled and hidden: Keycloak does not validate an
    /// assertion through a disabled provider (#1009), and `hideOnLogin` keeps it off the login
    /// page because it is reached by a client assertion, never by a person. A fresh install
    /// ships it without baking a customer's repository into a product file (#908, #57 slice 1).
    #[test]
    fn the_realm_offers_the_github_actions_identity_provider() {
        let realm = realm_config_map(&spec(Some("data.example.com")))
            .expect("a ConfigMap")
            .data
            .expect("data")
            .remove("stardelt-realm.json")
            .expect("the realm");
        let parsed: serde_json::Value = serde_json::from_str(&realm).expect("the realm is JSON");
        let providers = parsed["identityProviders"]
            .as_array()
            .expect("identityProviders");
        assert_eq!(providers.len(), 1, "exactly one identity provider");
        let provider = &providers[0];
        assert_eq!(provider["alias"], "github-actions");
        assert_eq!(provider["providerId"], "oidc");
        assert_eq!(provider["enabled"].as_bool(), Some(true));
        assert_eq!(provider["hideOnLogin"].as_bool(), Some(true));
        assert_eq!(
            provider["config"]["issuer"],
            "https://token.actions.githubusercontent.com"
        );
        assert_eq!(provider["config"]["clientId"], "stardelt-deploy");
        assert_eq!(provider["config"]["supportsClientAssertions"], "true");
        assert_eq!(provider["config"]["allowClientIdAsAudience"], "true");
    }

    /// Keycloak 26.8 accepts a service-account token at /admin only with the `kc-admin-api`
    /// audience (#883). An import drops a client's reference to the built-in scope, because the
    /// server creates that scope after the representation's clients. The realm therefore declares
    /// the scope itself, and access-sync's client takes it by default.
    #[test]
    fn the_group_sync_client_gets_the_admin_api_audience() {
        let realm = realm_config_map(&spec(Some("data.example.com")))
            .expect("a ConfigMap")
            .data
            .expect("data")
            .remove("stardelt-realm.json")
            .expect("the realm");
        let parsed: serde_json::Value = serde_json::from_str(&realm).expect("realm JSON");
        let client = parsed["clients"]
            .as_array()
            .expect("clients")
            .iter()
            .find(|c| c["clientId"] == "trino-group-sync")
            .expect("the trino-group-sync client");
        assert_eq!(
            client["defaultClientScopes"],
            serde_json::json!(["kc-admin-api"])
        );
        let scope = parsed["clientScopes"]
            .as_array()
            .expect("clientScopes")
            .iter()
            .find(|s| s["name"] == "kc-admin-api")
            .expect("the realm declares kc-admin-api");
        assert_eq!(
            scope["protocolMappers"][0]["protocolMapper"],
            "oidc-admin-api-audience-mapper"
        );
    }

    /// The front (`spark-connect-auth`, stardelt/stardelt#231) refuses a token whose `aud` has no
    /// `spark-connect`, so the realm must issue it: a bearer-only client to name as the audience,
    /// and the mapper on the two clients a person's token comes from (#84 Q4).
    #[test]
    fn the_realm_issues_the_spark_connect_audience() {
        let realm = realm_config_map(&spec(Some("data.example.com")))
            .expect("a ConfigMap")
            .data
            .expect("data")
            .remove("stardelt-realm.json")
            .expect("the realm");
        let parsed: serde_json::Value = serde_json::from_str(&realm).expect("the realm is JSON");
        let clients = parsed["clients"].as_array().expect("clients");
        let find = |id: &str| {
            clients
                .iter()
                .find(|c| c["clientId"] == id)
                .unwrap_or_else(|| panic!("the realm has no {id} client"))
        };
        let spark = find("spark-connect");
        assert_eq!(spark["bearerOnly"].as_bool(), Some(true));
        assert_eq!(spark["publicClient"].as_bool(), Some(false));
        assert_eq!(spark["standardFlowEnabled"].as_bool(), Some(false));
        for id in ["jupyterhub", "stardelt-dev"] {
            let mappers = find(id)["protocolMappers"].as_array().expect("mappers");
            let mapper = mappers
                .iter()
                .find(|m| m["name"] == "aud-spark-connect")
                .unwrap_or_else(|| panic!("{id} has no aud-spark-connect mapper"));
            assert_eq!(
                mapper["config"]["included.client.audience"], "spark-connect",
                "{id}'s aud-spark-connect names the wrong audience"
            );
            assert_eq!(mapper["config"]["access.token.claim"], "true");
        }
        let carriers = clients
            .iter()
            .filter(|c| {
                c["protocolMappers"]
                    .as_array()
                    .is_some_and(|ms| ms.iter().any(|m| m["name"] == "aud-spark-connect"))
            })
            .count();
        assert_eq!(
            carriers, 2,
            "only jupyterhub and stardelt-dev carry aud-spark-connect"
        );
    }

    #[test]
    fn the_realm_follows_the_base_domain_and_needs_one() {
        let realm = realm_config_map(&spec(Some("lab.example.org")))
            .expect("a ConfigMap")
            .data
            .expect("data")
            .remove("stardelt-realm.json")
            .expect("the realm");
        assert!(realm.contains("https://lab.example.org/api/auth/callback"));
        assert!(realm.contains("https://trino.lab.example.org"));
        assert!(realm.contains("admin@lab.example.org"));
        assert!(!realm.contains("data.example.com"));
        let error = realm_config_map(&spec(None)).unwrap_err();
        assert!(error.0.contains("baseDomain"), "{error}");
    }

    #[test]
    fn the_env_config_map_pins_the_issuer_hostname() {
        let cm = env_config_map(&spec(Some("data.example.com"))).expect("a ConfigMap");
        assert_eq!(cm.metadata.name.as_deref(), Some("keycloak-env"));
        assert_eq!(cm.metadata.namespace.as_deref(), Some("stardelt"));
        assert_eq!(
            cm.metadata.labels.as_ref().unwrap()["app.kubernetes.io/part-of"],
            "stardelt"
        );
        assert_eq!(
            cm.data.expect("data")["KC_HOSTNAME"],
            "https://id.data.example.com"
        );
        assert!(env_config_map(&spec(None)).is_err());
    }

    #[test]
    fn the_service_exposes_http_on_8080() {
        let service = service("stardelt");
        assert_eq!(service.metadata.name.as_deref(), Some("keycloak"));
        assert_eq!(service.metadata.namespace.as_deref(), Some("stardelt"));
        let labels = service.metadata.labels.as_ref().expect("labels");
        assert_eq!(labels["app.kubernetes.io/name"], "keycloak");
        assert_eq!(labels["app.kubernetes.io/part-of"], "stardelt");
        let spec = service.spec.expect("spec");
        assert_eq!(spec.type_.as_deref(), Some("ClusterIP"));
        assert_eq!(spec.selector.unwrap()["app.kubernetes.io/name"], "keycloak");
        let ports = spec.ports.expect("ports");
        assert_eq!(ports.len(), 2);
        assert_eq!(ports[0].name.as_deref(), Some("http"));
        assert_eq!(ports[0].port, 8080);
        assert_eq!(
            ports[0].target_port,
            Some(IntOrString::String("http".into()))
        );
        assert_eq!(ports[1].name.as_deref(), Some("metrics"));
        assert_eq!(ports[1].port, 9000);
        assert_eq!(
            ports[1].target_port,
            Some(IntOrString::String("management".into()))
        );
    }

    fn pod(deployment: &Deployment) -> (Container, PodSpec) {
        let spec = deployment.spec.clone();
        let pod = spec.template.spec.expect("pod spec");
        (pod.containers[0].clone(), pod)
    }

    #[test]
    fn the_deployment_mirrors_the_platform_manifest() {
        let deployment = deployment("stardelt", None);
        assert_eq!(deployment.metadata.name.as_deref(), Some("keycloak"));
        assert_eq!(deployment.metadata.namespace.as_deref(), Some("stardelt"));
        let (container, pod) = pod(&deployment);
        for labels in [
            deployment.metadata.labels.as_ref().expect("labels"),
            deployment
                .spec
                .template
                .metadata
                .as_ref()
                .unwrap()
                .labels
                .as_ref()
                .expect("pod labels"),
        ] {
            assert_eq!(labels["app.kubernetes.io/name"], "keycloak");
            assert_eq!(labels["app.kubernetes.io/part-of"], "stardelt");
        }
        let spec = deployment.spec.clone();
        assert_eq!(spec.replicas, Some(1));
        assert_eq!(
            spec.strategy.and_then(|s| s.type_),
            Some("Recreate".into()),
            "one replica with KC_CACHE=local must not roll two pods"
        );
        assert_eq!(
            spec.selector.match_labels.unwrap()["app.kubernetes.io/name"],
            "keycloak"
        );

        assert_eq!(container.name, "keycloak");
        assert_eq!(
            container.image.as_deref(),
            Some("quay.io/keycloak/keycloak:26.8.2")
        );
        assert_eq!(
            container
                .args
                .as_ref()
                .map(|args| args.join(" "))
                .as_deref(),
            Some("start --import-realm")
        );
        assert_eq!(
            container.env_from.as_ref().expect("envFrom")[0]
                .config_map_ref
                .as_ref()
                .unwrap()
                .name,
            "keycloak-env"
        );
        let ports = container.ports.as_ref().expect("ports");
        assert_eq!(
            (ports[0].name.as_deref(), ports[0].container_port),
            (Some("http"), 8080)
        );
        assert_eq!(
            (ports[1].name.as_deref(), ports[1].container_port),
            (Some("management"), 9000)
        );

        let startup = container.startup_probe.as_ref().expect("startup probe");
        assert_eq!(
            (startup.period_seconds, startup.failure_threshold),
            (Some(5), Some(60))
        );
        let readiness = container.readiness_probe.as_ref().expect("readiness probe");
        assert_eq!(readiness.period_seconds, Some(10));
        let liveness = container.liveness_probe.as_ref().expect("liveness probe");
        assert_eq!(
            (liveness.period_seconds, liveness.failure_threshold),
            (Some(20), Some(5))
        );
        for probe in [startup, readiness, liveness] {
            let http = probe.http_get.as_ref().expect("an HTTP probe");
            assert_eq!(http.port, IntOrString::String("management".into()));
        }
        assert_eq!(
            startup.http_get.as_ref().unwrap().path.as_deref(),
            Some("/health/started")
        );
        assert_eq!(
            readiness.http_get.as_ref().unwrap().path.as_deref(),
            Some("/health/ready")
        );
        assert_eq!(
            liveness.http_get.as_ref().unwrap().path.as_deref(),
            Some("/health/live")
        );

        let resources = container.resources.expect("resources");
        let requests = resources.requests.expect("requests");
        assert_eq!(requests["cpu"], Quantity("100m".into()));
        assert_eq!(requests["memory"], Quantity("512Mi".into()));
        let limits = resources.limits.expect("limits");
        assert_eq!(limits["cpu"], Quantity("1".into()));
        assert_eq!(limits["memory"], Quantity("1Gi".into()));

        let security = container.security_context.expect("security context");
        assert_eq!(security.allow_privilege_escalation, Some(false));
        assert_eq!(
            security.capabilities.unwrap().drop,
            Some(vec!["ALL".into()])
        );
        assert_eq!(pod.automount_service_account_token, Some(false));
        let pod_security = pod.security_context.expect("pod security context");
        assert_eq!(pod_security.run_as_non_root, Some(true));
        assert_eq!(
            pod_security.seccomp_profile.map(|p| p.type_),
            Some("RuntimeDefault".into())
        );

        let mount = container.volume_mounts.as_ref().expect("volume mounts")[0].clone();
        assert_eq!(mount.name, "realm");
        assert_eq!(mount.mount_path, "/opt/keycloak/data/import");
        assert_eq!(mount.read_only, Some(true));
        let volumes = pod.volumes.expect("volumes");
        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].name, "realm");
        assert_eq!(
            volumes[0].config_map.as_ref().unwrap().name,
            "keycloak-realm"
        );
    }

    #[test]
    fn the_deployment_reads_every_secret_the_realm_needs() {
        let (container, _) = pod(&deployment("stardelt", None));
        let env = container.env.expect("env");
        let named: Vec<(&str, Option<&str>)> = env
            .iter()
            .map(|e| (e.name.as_str(), e.value.as_deref()))
            .collect();
        assert_eq!(
            named,
            [
                ("KC_DB", Some("postgres")),
                (
                    "KC_DB_URL",
                    Some("jdbc:postgresql://keycloak-pg-rw:5432/keycloak")
                ),
                ("KC_DB_USERNAME", None),
                ("KC_DB_PASSWORD", None),
                ("KC_HTTP_ENABLED", Some("true")),
                ("KC_PROXY_HEADERS", Some("xforwarded")),
                ("KC_HEALTH_ENABLED", Some("true")),
                ("KC_METRICS_ENABLED", Some("true")),
                ("KC_CACHE", Some("local")),
                (
                    "JAVA_OPTS_KC_HEAP",
                    Some("-XX:MaxRAMPercentage=65 -XX:InitialRAMPercentage=25")
                ),
                ("KC_BOOTSTRAP_ADMIN_USERNAME", None),
                ("KC_BOOTSTRAP_ADMIN_PASSWORD", None),
                ("STARDELT_REALM_ADMIN_PASSWORD", None),
                ("STARDELT_NOVA_CLIENT_SECRET", None),
                ("STARDELT_SUPERSET_CLIENT_SECRET", None),
                ("STARDELT_AIRFLOW_CLIENT_SECRET", None),
                ("STARDELT_JUPYTERHUB_CLIENT_SECRET", None),
                ("STARDELT_TRINO_CLIENT_SECRET", None),
                ("STARDELT_TRINO_GROUP_SYNC_CLIENT_SECRET", None),
                ("STARDELT_PIPELINE_RUNNER_CLIENT_SECRET", None),
            ]
        );
        // Every secret reference names the Secret and key `manifests/keycloak.yaml` uses.
        let references: Vec<(&str, &str)> = env
            .iter()
            .filter_map(|e| {
                let selector = e.value_from.as_ref()?.secret_key_ref.as_ref()?;
                Some((selector.name.as_str(), selector.key.as_str()))
            })
            .collect();
        assert_eq!(
            references,
            [
                ("keycloak-pg-app", "username"),
                ("keycloak-pg-app", "password"),
                ("keycloak-admin", "username"),
                ("keycloak-admin", "password"),
                ("stardelt-realm-admin", "password"),
                ("stardelt-oidc-clients", "nova"),
                ("stardelt-oidc-clients", "superset"),
                ("stardelt-oidc-clients", "airflow"),
                ("stardelt-oidc-clients", "jupyterhub"),
                ("stardelt-oidc-clients", "trino"),
                ("stardelt-oidc-clients", "trino-group-sync"),
                ("stardelt-oidc-clients", "pipeline-runner"),
            ]
        );
    }

    #[test]
    fn the_realm_template_is_the_platform_file() {
        assert!(REALM_TEMPLATE.starts_with("{\n  \"realm\": \"stardelt\","));
        for placeholder in PLACEHOLDERS {
            assert!(REALM_TEMPLATE.contains(placeholder), "{placeholder}");
        }
        assert!(
            !REALM_TEMPLATE.contains("__STARDELT"),
            "secrets are ${{...}} placeholders, not host placeholders"
        );
    }

    #[test]
    fn the_identity_secret_table_matches_the_platform_bootstrap() {
        assert_eq!(
            IDENTITY_SECRETS.map(|desired| desired.name),
            [
                "keycloak-admin",
                "stardelt-realm-admin",
                "stardelt-oidc-clients"
            ]
        );
        for desired in IDENTITY_SECRETS {
            match desired.name {
                "keycloak-admin" | "stardelt-realm-admin" => {
                    assert_eq!(desired.fixed.to_vec(), [("username", "admin")]);
                    assert_eq!(desired.generated.to_vec(), ["password"]);
                }
                "stardelt-oidc-clients" => {
                    assert!(desired.fixed.is_empty());
                    assert_eq!(
                        desired.generated.to_vec(),
                        [
                            "nova",
                            "superset",
                            "airflow",
                            "jupyterhub",
                            "trino",
                            "trino-group-sync",
                            "pipeline-runner"
                        ]
                    );
                }
                other => panic!("unexpected Secret {other}"),
            }
        }
        assert_eq!(IDENTITY_SECRETS[0].key_names(), ["username", "password"]);
        assert_eq!(
            IDENTITY_SECRETS[2].key_names(),
            [
                "nova",
                "superset",
                "airflow",
                "jupyterhub",
                "trino",
                "trino-group-sync",
                "pipeline-runner"
            ]
        );
    }

    #[test]
    fn a_generated_secret_is_48_lower_case_hex_chars_and_unique() {
        let first = generate_secret().expect("a credential");
        let second = generate_secret().expect("a credential");
        assert_eq!(first.len(), 48);
        assert!(
            first
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "not lower-case hex"
        );
        assert_ne!(first, second);
    }

    #[test]
    fn missing_keys_keeps_the_desired_order_and_ignores_keys_nobody_asked_for() {
        let desired = ["nova", "superset", "airflow"];
        assert_eq!(missing_keys(&BTreeMap::new(), &desired), desired);
        let existing = BTreeMap::from([
            ("superset".to_string(), "present".to_string()),
            ("legacy".to_string(), "ignored".to_string()),
        ]);
        assert_eq!(missing_keys(&existing, &desired), ["nova", "airflow"]);
        let complete: BTreeMap<String, String> = desired
            .iter()
            .map(|key| ((*key).to_string(), String::new()))
            .collect();
        assert!(missing_keys(&complete, &desired).is_empty());
    }

    #[test]
    fn an_identity_secret_is_labelled_and_carries_fixed_and_generated_keys() {
        let secret = identity_secret(&IDENTITY_SECRETS[0], "stardelt").expect("a Secret");
        assert_eq!(secret.metadata.name.as_deref(), Some("keycloak-admin"));
        assert_eq!(secret.metadata.namespace.as_deref(), Some("stardelt"));
        assert!(
            secret.metadata.owner_references.is_none(),
            "the realm owns the credentials, not the PlatformInstance"
        );
        assert_eq!(
            secret.metadata.labels.as_ref().expect("labels")["app.kubernetes.io/part-of"],
            "stardelt"
        );
        let data = secret.data.expect("data");
        assert_eq!(data["username"].0, b"admin".to_vec());
        assert_eq!(data["password"].0.len(), 48, "48 hex chars");

        let clients = identity_secret(&IDENTITY_SECRETS[2], "stardelt").expect("a Secret");
        let data = clients.data.expect("data");
        assert_eq!(data.len(), 7, "one credential per client");
        for key in IDENTITY_SECRETS[2].generated {
            assert_eq!(data[*key].0.len(), 48, "{key}");
        }
    }

    #[test]
    fn base64_url_matches_the_rfc_vectors_and_the_url_safe_alphabet() {
        assert_eq!(base64_url(b""), "");
        assert_eq!(base64_url(b"f"), "Zg==");
        assert_eq!(base64_url(b"fo"), "Zm8=");
        assert_eq!(base64_url(b"foo"), "Zm9v");
        assert_eq!(base64_url(b"foobar"), "Zm9vYmFy");
        assert_eq!(
            base64_url(&[0u8; 32]),
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        );
        assert_eq!(
            base64_url(&[255u8; 32]),
            "__________________________________________8="
        );
        let counting: Vec<u8> = (0..32).collect();
        assert_eq!(
            base64_url(&counting),
            "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
        );
        let mixed: Vec<u8> = [0xfb, 0xff, 0xfe]
            .iter()
            .copied()
            .cycle()
            .take(30)
            .chain([0xfb, 0xff])
            .collect();
        assert_eq!(
            base64_url(&mixed),
            "-__--__--__--__--__--__--__--__--__--__--_8="
        );
    }

    #[test]
    fn a_fernet_key_has_44_url_safe_characters_ending_in_one_padding_sign_and_differs_each_time() {
        let key = generate_fernet_key().expect("a key");
        assert_eq!(key.len(), 44);
        assert!(key.ends_with('='));
        assert!(
            key[..43]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "not url-safe base64: {key}"
        );
        assert_ne!(key, generate_fernet_key().expect("a second key"));
    }

    #[test]
    fn an_identity_secret_with_a_fernet_key_carries_one_in_that_format() {
        const WITH_FERNET: IdentitySecret = IdentitySecret {
            name: "demo",
            fixed: &[("username", "demo")],
            generated: &["password"],
            fernet: &["fernet-key"],
        };
        assert_eq!(
            WITH_FERNET.key_names(),
            ["username", "password", "fernet-key"]
        );
        let secret = identity_secret(&WITH_FERNET, "stardelt").expect("a Secret");
        let data = secret.data.expect("data");
        assert_eq!(data["fernet-key"].0.len(), 44);
        assert_eq!(data["password"].0.len(), 48, "a plain credential stays hex");
    }

    #[test]
    fn generate_hex_produces_the_requested_length_and_differs() {
        let key = generate_hex(32).expect("a key");
        assert_eq!(key.len(), 64);
        assert!(key.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(!key.chars().any(|c| c.is_ascii_uppercase()));
        assert_ne!(key, generate_hex(32).expect("a second key"));
        assert_eq!(generate_secret().expect("secret").len(), 48);
    }

    /// The directory of the two tests below: #1516's spike realm, as measured.
    fn ldap() -> IdentityLdap {
        use crate::crd::IdentityLdapBindSecret;
        IdentityLdap {
            host: "dir.example.com".into(),
            port: 389,
            tls: IdentityLdapTls::None,
            users_dn: "ou=users,dc=example,dc=org".into(),
            group_dn: "ou=groups,dc=example,dc=org".into(),
            bind_secret_ref: IdentityLdapBindSecret {
                name: "corp-directory".into(),
                dn_key: "bind-dn".into(),
                password_key: "bind-password".into(),
            },
            sync_seconds: 3600,
        }
    }

    fn spec_with_ldap() -> PlatformInstanceSpec {
        let mut spec = spec(Some("data.example.com"));
        spec.identity.ldap = Some(ldap());
        spec
    }

    fn realm_of(spec: &PlatformInstanceSpec) -> serde_json::Value {
        let realm = realm_config_map(spec)
            .expect("a ConfigMap")
            .data
            .expect("data")
            .remove("stardelt-realm.json")
            .expect("the realm");
        serde_json::from_str(&realm).expect("the realm is JSON")
    }

    /// The directory's transport is the schema's only switch: `none` and `starttls` speak
    /// plaintext to 389 (StartTLS upgrades it), `ldaps` is TLS from the first byte to 636.
    #[test]
    fn the_ldap_connection_url_carries_the_tls_choice() {
        let mut plain = ldap();
        plain.tls = IdentityLdapTls::None;
        let config =
            &ldap_components(&plain)["org.keycloak.storage.UserStorageProvider"][0]["config"];
        assert_eq!(
            config["connectionUrl"],
            serde_json::json!(["ldap://dir.example.com:389"])
        );
        assert!(
            config.get("startTls").is_none(),
            "plaintext carries no startTls key"
        );

        let mut starttls = ldap();
        starttls.tls = IdentityLdapTls::Starttls;
        let config =
            &ldap_components(&starttls)["org.keycloak.storage.UserStorageProvider"][0]["config"];
        assert_eq!(
            config["connectionUrl"],
            serde_json::json!(["ldap://dir.example.com:389"])
        );
        assert_eq!(config["startTls"], serde_json::json!(["true"]));

        let mut ldaps = ldap();
        ldaps.tls = IdentityLdapTls::Ldaps;
        ldaps.port = 636;
        let config =
            &ldap_components(&ldaps)["org.keycloak.storage.UserStorageProvider"][0]["config"];
        assert_eq!(
            config["connectionUrl"],
            serde_json::json!(["ldaps://dir.example.com:636"])
        );
        assert!(
            config.get("startTls").is_none(),
            "ldaps carries no startTls key"
        );
    }

    /// The `components` object is the import form measured in #1516: `subComponents` is a sibling
    /// of `config`, no entry carries `providerType`, and the `username` mapper is what makes an
    /// imported user signable.
    #[test]
    fn the_ldap_federation_renders_only_when_configured() {
        let plain = realm_of(&spec(Some("data.example.com")));
        assert!(
            plain.get("components").is_none(),
            "a realm without a directory carries no components: {}",
            plain
                .get("components")
                .map(ToString::to_string)
                .unwrap_or_default()
        );

        let realm = realm_of(&spec_with_ldap());
        let provider = &realm["components"]["org.keycloak.storage.UserStorageProvider"][0];
        assert_eq!(provider["providerId"], "ldap");
        assert!(
            provider.get("providerType").is_none(),
            "no providerType key"
        );
        assert_eq!(
            provider["config"]["connectionUrl"],
            serde_json::json!(["ldap://dir.example.com:389"])
        );
        assert_eq!(
            provider["config"]["editMode"],
            serde_json::json!(["READ_ONLY"])
        );
        assert_eq!(
            provider["config"]["fullSyncPeriod"],
            serde_json::json!(["3600"])
        );
        assert_eq!(
            provider["config"]["bindDn"],
            serde_json::json!(["${STARDELT_LDAP_BIND_DN}"])
        );
        assert_eq!(
            provider["config"]["bindCredential"],
            serde_json::json!(["${STARDELT_LDAP_BIND_PASSWORD}"])
        );
        let mappers =
            provider["subComponents"]["org.keycloak.storage.ldap.mappers.LDAPStorageMapper"]
                .as_array()
                .expect("the two mappers");
        assert_eq!(mappers.len(), 2, "the username mapper and the group mapper");
        assert_eq!(mappers[0]["providerId"], "user-attribute-ldap-mapper");
        assert_eq!(
            mappers[0]["config"]["user.model.attribute"],
            serde_json::json!(["username"])
        );
        assert_eq!(mappers[1]["providerId"], "group-ldap-mapper");
        assert_eq!(
            mappers[1]["config"]["groups.dn"],
            serde_json::json!(["ou=groups,dc=example,dc=org"])
        );
    }

    #[test]
    fn the_keycloak_container_reads_the_bind_secret_only_when_ldap_is_set() {
        let bind = ldap();
        let (container, _) = pod(&deployment("stardelt", Some(&bind)));
        let env = container.env.expect("env");
        let selector = |name: &str| {
            env.iter()
                .find(|e| e.name == name)
                .unwrap_or_else(|| panic!("no {name} env var"))
                .value_from
                .as_ref()
                .and_then(|v| v.secret_key_ref.as_ref())
                .map(|s| (s.name.clone(), s.key.clone()))
        };
        assert_eq!(
            selector("STARDELT_LDAP_BIND_PASSWORD"),
            Some(("corp-directory".into(), "bind-password".into()))
        );
        assert_eq!(
            selector("STARDELT_LDAP_BIND_DN"),
            Some(("corp-directory".into(), "bind-dn".into()))
        );

        let (plain, _) = pod(&deployment("stardelt", None));
        let names: Vec<String> = plain
            .env
            .expect("env")
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(
            names.iter().all(|n| !n.starts_with("STARDELT_LDAP_")),
            "a realm without a directory gets no bind variables: {names:?}"
        );
    }

    #[test]
    fn the_keycloak_image_is_pinned_by_digest() {
        let (tag, digest) = IMAGE
            .split_once('@')
            .expect("IMAGE carries @sha256:<64 hex>");
        assert_eq!(tag, "quay.io/keycloak/keycloak:26.8.2");
        assert!(
            digest.len() == 71
                && digest.starts_with("sha256:")
                && digest[7..].chars().all(|c| c.is_ascii_hexdigit()),
            "IMAGE digest is not sha256:<64 hex>: {IMAGE}"
        );
    }

    #[test]
    fn ldap_needs_platform_mode() {
        let message = "ldap only works with mode platform, which runs the Keycloak that imports it";
        let mut external = spec_with_ldap();
        external.identity.mode = crate::crd::IdentityMode::External;
        assert_eq!(identity::validate(&external).unwrap_err().0, message);
        let mut none = spec_with_ldap();
        none.identity.mode = crate::crd::IdentityMode::None;
        assert_eq!(identity::validate(&none).unwrap_err().0, message);
        identity::validate(&spec_with_ldap()).expect("platform mode accepts a directory");
    }
}
