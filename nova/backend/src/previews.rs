//! Expired preview copies (#106 slice 2b): a preview deploy writes `stardelt.io/preview`
//! and `stardelt.io/expires-at` on the resources it deploys (#1939); this collector removes
//! them when the moment passes, so a pull request cannot leave a copy behind.
//!
//! It deletes only what the platform itself stamped as a preview: the Nova label is present,
//! both annotations parse, and the expiry is in the past. Anything else is a person's
//! annotation and is left alone.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::AppState;
use crate::pipelines::{
    EXPIRES_NOTE, MANAGED_BY_LABEL, MANAGED_BY_NOVA, PREVIEW_NOTE, PipelineResources,
};

/// The collector's period.
const TICK: Duration = Duration::from_secs(600);
/// Most deletions one tick performs; a backlog drains over several ticks.
const MAX_DELETIONS: usize = 100;

/// The preview id the CLI accepts: 1 to 20 lower-case letters, digits and dashes.
fn valid_preview(id: &str) -> bool {
    (1..=20).contains(&id.chars().count())
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && id
            .as_bytes()
            .first()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && id
            .as_bytes()
            .last()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// Whether `resource` is a preview this collector may remove now.
fn expired(resource: &Value, now: DateTime<Utc>) -> bool {
    let metadata = &resource["metadata"];
    if metadata["labels"][MANAGED_BY_LABEL].as_str() != Some(MANAGED_BY_NOVA) {
        return false;
    }
    let Some(preview) = metadata["annotations"][PREVIEW_NOTE].as_str() else {
        return false;
    };
    if !valid_preview(preview) {
        return false;
    }
    let Some(at) = metadata["annotations"][EXPIRES_NOTE]
        .as_str()
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
    else {
        return false;
    };
    at <= now
}

/// Start the collector. Off — with one line, never a panic — without a cluster client.
pub fn spawn(state: Arc<AppState>) {
    if state.kube.is_none() {
        tracing::info!("preview expiry is off: the cluster client is not configured");
        return;
    }
    tokio::spawn(async move {
        tokio::time::sleep(TICK).await;
        let mut ticks = tokio::time::interval(TICK);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            if let Err(e) = run_once(&state).await {
                tracing::warn!(error = %e, "the preview expiry tick failed");
            }
            ticks.tick().await;
        }
    });
}

async fn run_once(state: &AppState) -> Result<usize, String> {
    let Some(kube) = state.kube.clone() else {
        return Ok(0);
    };
    let now = DateTime::<Utc>::from(std::time::SystemTime::now());
    let mut removed = 0;
    let resources = PipelineResources::cluster(kube.clone());
    for resource in resources.list().await.map_err(|e| format!("{e:?}"))? {
        if removed >= MAX_DELETIONS {
            return Ok(removed);
        }
        if !expired(&resource, now) {
            continue;
        }
        let name = resource["metadata"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        match resources.delete(&name).await {
            Ok(()) => {
                audit(&resource, "Pipeline", now);
                removed += 1;
            }
            Err(e) => {
                tracing::warn!(resource = %name, error = ?e, "an expired preview pipeline was not removed")
            }
        }
    }
    let streams = crate::streams::stream_apps(state)
        .await
        .map_err(|e| format!("{e:?}"))?;
    for resource in streams {
        if removed >= MAX_DELETIONS {
            break;
        }
        if !expired(&resource, now) {
            continue;
        }
        let name = resource["metadata"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        match crate::streams::delete_stream(state, &name).await {
            Ok(()) => {
                audit(&resource, "StreamApp", now);
                removed += 1;
            }
            Err(e) => {
                tracing::warn!(resource = %name, error = ?e, "an expired preview stream was not removed")
            }
        }
    }
    Ok(removed)
}

fn audit(resource: &Value, kind: &str, now: DateTime<Utc>) {
    let metadata = &resource["metadata"];
    tracing::info!(
        target: "audit",
        action = "preview_expired",
        kind = %kind,
        namespace = metadata["namespace"].as_str().unwrap_or(""),
        name = metadata["name"].as_str().unwrap_or(""),
        preview = metadata["annotations"][PREVIEW_NOTE].as_str().unwrap_or(""),
        expires_at = metadata["annotations"][EXPIRES_NOTE].as_str().unwrap_or(""),
        at = %now.to_rfc3339(),
        "expired preview removed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn resource(preview: Option<&str>, expires: Option<&str>, managed: bool) -> Value {
        let mut annotations = serde_json::Map::new();
        if let Some(id) = preview {
            annotations.insert(PREVIEW_NOTE.into(), json!(id));
        }
        if let Some(text) = expires {
            annotations.insert(EXPIRES_NOTE.into(), json!(text));
        }
        json!({
            "metadata": {
                "namespace": "stardelt",
                "name": "pr-1-load",
                // MANAGED_BY_LABEL is "app.kubernetes.io/managed-by"; `json!` needs a literal key.
                "labels": { "app.kubernetes.io/managed-by": if managed { MANAGED_BY_NOVA } else { "someone" } },
                "annotations": annotations,
            }
        })
    }

    #[test]
    fn an_expired_preview_is_removed_and_a_live_one_is_not() {
        let now = at("2026-11-26T12:00:00Z");
        assert!(expired(
            &resource(Some("pr-1"), Some("2026-11-26T11:59:00Z"), true),
            now
        ));
        assert!(!expired(
            &resource(Some("pr-1"), Some("2026-11-26T12:01:00Z"), true),
            now
        ));
    }

    #[test]
    fn only_what_nova_stamped_as_a_preview_is_removed() {
        let now = at("2026-11-26T12:00:00Z");
        assert!(
            !expired(
                &resource(Some("pr-1"), Some("2026-11-26T11:00:00Z"), false),
                now
            ),
            "not Nova's"
        );
        assert!(
            !expired(
                &resource(Some("PR_1"), Some("2026-11-26T11:00:00Z"), true),
                now
            ),
            "not a CLI id"
        );
        assert!(
            !expired(&resource(Some("pr-1"), None, true), now),
            "no expiry"
        );
        assert!(
            !expired(&resource(None, Some("2026-11-26T11:00:00Z"), true), now),
            "no preview id"
        );
        assert!(
            !expired(&resource(Some("pr-1"), Some("tomorrow"), true), now),
            "not a timestamp"
        );
    }
}
