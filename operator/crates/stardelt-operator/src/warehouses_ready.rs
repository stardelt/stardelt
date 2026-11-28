//! Whether the engine has loaded every `SqlWarehouse` of a Lakehouse (fresh-node run po263c):
//! the coordinator reads `resource-groups.json` once at start, so a Lakehouse is not `Ready`
//! until access-sync has rendered each warehouse and the coordinator has rolled onto that file.

use crate::crd::{ComponentStatus, SqlWarehouse};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{Api, Client};

pub const NAME: &str = "warehouses";
const CONFIG_MAP: &str = "trino-warehouses";
const KEY: &str = "resource-groups.json";
const COORDINATOR: &str = "trino-coordinator";
const STAMP: &str = "stardelt.io/trino-warehouses";

/// Every `"name"` value anywhere in the resource-group file.
fn group_names(value: &serde_json::Value, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(name)) = map.get("name") {
                out.push(name.clone());
            }
            map.values().for_each(|v| group_names(v, out));
        }
        serde_json::Value::Array(items) => items.iter().for_each(|v| group_names(v, out)),
        _ => {}
    }
}

/// Why the engine has not loaded the warehouses yet, or `None` when it has.
/// `declared` are the SqlWarehouse names; `file` the ConfigMap's `resource-groups.json`;
/// `version` its `resourceVersion`; `stamp` the coordinator template's annotation; `rolled`
/// whether the coordinator Deployment has finished its rollout.
pub fn waiting(
    declared: &[String],
    file: Option<&str>,
    version: Option<&str>,
    stamp: Option<&str>,
    rolled: bool,
) -> Option<String> {
    let mut rendered = Vec::new();
    if let Some(parsed) = file.and_then(|f| serde_json::from_str::<serde_json::Value>(f).ok()) {
        group_names(&parsed, &mut rendered);
    }
    let missing: Vec<&str> = declared
        .iter()
        .filter(|name| !rendered.contains(&format!("wh-{name}")))
        .map(String::as_str)
        .collect();
    if !missing.is_empty() {
        return Some(format!(
            "waiting for access-sync to render SqlWarehouse {} into ConfigMap {CONFIG_MAP}",
            missing.join(", ")
        ));
    }
    if version.is_none() || stamp != version {
        return Some(format!(
            "waiting for the coordinator to load ConfigMap {CONFIG_MAP}"
        ));
    }
    if !rolled {
        return Some("waiting for the coordinator to finish rolling out".into());
    }
    None
}

/// Whether `deployment` has finished rolling out its current template.
fn rolled_out(deployment: &Deployment) -> bool {
    let want = deployment.spec.replicas.unwrap_or(1);
    let Some(status) = deployment.status.as_ref() else {
        return false;
    };
    status.observed_generation.unwrap_or(0) >= deployment.metadata.generation.unwrap_or(0)
        && status.updated_replicas.unwrap_or(0) == want
        && status.available_replicas.unwrap_or(0) == want
}

/// The `warehouses` component of `lakehouse` in `namespace`, or `None` when it declares no
/// SqlWarehouse. A read that fails is a not-ready component with the error as its message.
pub async fn status(client: &Client, namespace: &str, lakehouse: &str) -> Option<ComponentStatus> {
    let component = |ready: bool, message: String| ComponentStatus {
        name: NAME.into(),
        ready,
        message: Some(message),
        applied_hash: None,
    };
    let declared: Vec<String> = match Api::<SqlWarehouse>::namespaced(client.clone(), namespace)
        .list(&Default::default())
        .await
    {
        Ok(list) => list
            .items
            .into_iter()
            .filter(|w| w.spec.lakehouse_ref.name == lakehouse)
            .filter_map(|w| w.metadata.name)
            .collect(),
        Err(e) => return Some(component(false, e.to_string())),
    };
    if declared.is_empty() {
        return None;
    }
    let map = Api::<ConfigMap>::namespaced(client.clone(), namespace)
        .get_opt(CONFIG_MAP)
        .await
        .ok()
        .flatten();
    let file = map
        .as_ref()
        .and_then(|m| m.data.as_ref())
        .and_then(|d| d.get(KEY))
        .cloned();
    let version = map
        .as_ref()
        .and_then(|m| m.metadata.resource_version.clone());
    let coordinator = Api::<Deployment>::namespaced(client.clone(), namespace)
        .get_opt(COORDINATOR)
        .await
        .ok()
        .flatten();
    let stamp = coordinator
        .as_ref()
        .and_then(|d| d.spec.template.metadata.as_ref())
        .and_then(|m| m.annotations.as_ref())
        .and_then(|a| a.get(STAMP))
        .map(|v| v.to_string());
    let rolled = coordinator.as_ref().is_some_and(rolled_out);
    Some(
        match waiting(
            &declared,
            file.as_deref(),
            version.as_deref(),
            stamp.as_deref(),
            rolled,
        ) {
            Some(why) => component(false, why),
            None => component(true, format!("loaded: {}", declared.join(", "))),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::waiting;

    #[test]
    fn a_warehouse_access_sync_has_not_rendered_is_named() {
        let why = waiting(
            &["small".into()],
            Some(r#"{"rootGroups":[{"name":"admin"}]}"#),
            Some("2392"),
            Some("2392"),
            true,
        )
        .expect("not ready");
        assert!(why.contains("SqlWarehouse small"), "{why}");
    }

    #[test]
    fn a_rendered_file_the_coordinator_has_not_loaded_waits() {
        let file = r#"{"rootGroups":[{"name":"global","subGroups":[{"name":"wh-small"}]}]}"#;
        let why = waiting(
            &["small".into()],
            Some(file),
            Some("9829"),
            Some("2392"),
            true,
        )
        .expect("not ready");
        assert!(why.contains("to load ConfigMap trino-warehouses"), "{why}");
    }

    #[test]
    fn a_loaded_file_waits_for_the_rollout() {
        let file = r#"{"rootGroups":[{"name":"global","subGroups":[{"name":"wh-small"}]}]}"#;
        let why = waiting(
            &["small".into()],
            Some(file),
            Some("9829"),
            Some("9829"),
            false,
        )
        .expect("not ready");
        assert!(why.contains("finish rolling out"), "{why}");
    }

    #[test]
    fn a_loaded_and_rolled_coordinator_is_ready() {
        let file = r#"{"rootGroups":[{"name":"global","subGroups":[{"name":"wh-small"}]}]}"#;
        assert_eq!(
            waiting(
                &["small".into()],
                Some(file),
                Some("9829"),
                Some("9829"),
                true
            ),
            None
        );
    }

    #[test]
    fn no_file_at_all_names_every_warehouse() {
        let why = waiting(&["a".into(), "b".into()], None, None, None, false).expect("not ready");
        assert!(why.contains("SqlWarehouse a, b"), "{why}");
    }
}
