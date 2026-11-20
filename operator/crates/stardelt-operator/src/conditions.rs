//! One rule for every `status.conditions` array: `lastTransitionTime` moves only when `status` changes.
//!
//! #272 measured what a copy costs: a fresh `Timestamp::now()` on every pass made the Catalog controller
//! rewrite its own status ~9×/s. Every controller builds conditions through this module now.

use crate::crd::Condition;

/// The condition for `(type_, status)`, keeping the previous transition time when the same type already
/// had the same status.
pub fn set_condition(
    existing: &[Condition],
    type_: &str,
    status: &str,
    reason: &str,
    message: Option<String>,
    observed_generation: Option<i64>,
    now: &str,
) -> Condition {
    let last_transition_time = existing
        .iter()
        .find(|c| c.type_ == type_ && c.status == status)
        .and_then(|c| c.last_transition_time.clone())
        .or_else(|| Some(now.to_string()));
    Condition {
        type_: type_.into(),
        status: status.into(),
        reason: Some(reason.into()),
        message,
        last_transition_time,
        observed_generation,
    }
}

/// `existing` with `next` replacing the condition of the same type; every other type is kept.
///
/// The replacement keeps the `lastTransitionTime` of an existing condition with the same type *and* the
/// same status (the Kubernetes convention), so a repeated no-op reconcile writes the same bytes.
///
/// Keeping every other type matters because a controller patch must never wipe
/// `CatalogReady`/`GrantsReady`/`Drift` (the access-sync job writes the whole array with a merge-patch).
/// Keeping the time matters because `next` carries `Timestamp::now()`: without it a status patch lands on
/// every pass, the watch reports "object updated" and the controller reconciles itself (~9×/s per
/// catalog, stardelt#272).
pub fn merge_condition(existing: &[Condition], next: Condition) -> Vec<Condition> {
    let next = match existing
        .iter()
        .find(|c| c.type_ == next.type_ && c.status == next.status)
    {
        Some(holding) => Condition {
            last_transition_time: holding.last_transition_time.clone(),
            ..next
        },
        None => next,
    };
    let mut conditions: Vec<Condition> = existing
        .iter()
        .filter(|c| c.type_ != next.type_)
        .cloned()
        .collect();
    conditions.push(next);
    conditions
}

/// Whether a merge-patch status body `next` (without the `status`/`metadata` wrapper) would leave
/// the stored status unchanged. Only keys `next` names are compared: a merge patch leaves every
/// other key alone. A `null` in `next` deletes its key, so it matches only an absent or `null`
/// key. Objects are compared key by key, arrays and scalars as a whole (a merge patch replaces
/// them). An unchanged status must not be patched (#274 rule 1).
pub fn status_is_current<T: serde::Serialize>(
    current: Option<&T>,
    next: &serde_json::Value,
) -> bool {
    let Some(current) = current else {
        return false;
    };
    let Ok(current) = serde_json::to_value(current) else {
        return false;
    };
    merge_is_noop(&current, next)
}

fn merge_is_noop(target: &serde_json::Value, patch: &serde_json::Value) -> bool {
    use serde_json::Value;
    match patch {
        Value::Object(fields) => {
            let Value::Object(target) = target else {
                return false;
            };
            fields
                .iter()
                .all(|(key, value)| match (value, target.get(key)) {
                    (Value::Null, None | Some(Value::Null)) => true,
                    (Value::Null, Some(_)) | (_, None) => false,
                    (value, Some(existing)) => merge_is_noop(existing, value),
                })
        }
        other => target == other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::jiff::Timestamp;
    use serde_json::json;

    fn condition(type_: &str, status: &str, time: &str) -> Condition {
        Condition {
            type_: type_.into(),
            status: status.into(),
            reason: Some(status.into()),
            message: None,
            last_transition_time: Some(time.into()),
            observed_generation: None,
        }
    }

    #[test]
    fn a_new_condition_takes_now() {
        let c = set_condition(&[], "Ready", "True", "Ready", None, Some(3), "t1");
        assert_eq!(c.last_transition_time.as_deref(), Some("t1"));
    }

    #[test]
    fn the_time_holds_while_the_status_holds() {
        let existing = vec![condition("Ready", "True", "t0")];
        let c = set_condition(&existing, "Ready", "True", "Ready", None, Some(3), "t1");
        assert_eq!(c.last_transition_time.as_deref(), Some("t0"));
    }

    #[test]
    fn the_time_moves_when_the_status_flips() {
        let existing = vec![condition("Ready", "False", "t0")];
        let c = set_condition(&existing, "Ready", "True", "Ready", None, Some(3), "t1");
        assert_eq!(c.last_transition_time.as_deref(), Some("t1"));
    }

    #[test]
    fn merge_keeps_every_other_type_and_replaces_its_own() {
        let existing = vec![
            condition("Ready", "False", "t0"),
            condition("CatalogReady", "True", "t0"),
            condition("GrantsReady", "True", "t0"),
        ];
        let merged = merge_condition(&existing, condition("Ready", "True", "t1"));
        assert_eq!(merged.len(), 3);
        assert_eq!(
            merged.iter().filter(|c| c.type_ == "Ready").count(),
            1,
            "exactly one Ready"
        );
        assert_eq!(merged[0], existing[1]);
        assert_eq!(merged[1], existing[2]);
    }

    #[test]
    fn merge_holds_the_time_when_the_status_holds() {
        let existing = vec![condition("MaintenanceReady", "True", "t0")];
        let next = Condition {
            last_transition_time: Some(Timestamp::now().to_string()),
            ..condition("MaintenanceReady", "True", "t1")
        };
        let merged = merge_condition(&existing, next);
        assert_eq!(
            merged
                .last()
                .expect("merged")
                .last_transition_time
                .as_deref(),
            Some("t0")
        );
    }

    #[test]
    fn a_key_the_patch_does_not_name_is_ignored() {
        let current = json!({ "phase": "Ready", "observedGeneration": 3 });
        assert!(status_is_current(
            Some(&current),
            &json!({ "observedGeneration": 3 })
        ));
    }

    #[test]
    fn a_null_matches_only_an_absent_key() {
        for current in [json!({}), json!({ "lastError": null })] {
            assert!(
                status_is_current(Some(&current), &json!({ "lastError": null })),
                "{current}"
            );
        }
        assert!(!status_is_current(
            Some(&json!({ "lastError": "x" })),
            &json!({ "lastError": null })
        ));
    }

    #[test]
    fn an_array_is_compared_whole() {
        let current = json!({ "conditions": [{ "type": "A" }, { "type": "B" }] });
        assert!(!status_is_current(
            Some(&current),
            &json!({ "conditions": [{ "type": "A" }] })
        ));
    }

    #[test]
    fn a_nested_object_is_compared_key_by_key() {
        let current = json!({ "maintenance": { "lastRun": "t", "extra": 1 } });
        assert!(status_is_current(
            Some(&current),
            &json!({ "maintenance": { "lastRun": "t" } })
        ));
        assert!(!status_is_current(
            Some(&current),
            &json!({ "maintenance": { "lastRun": "u" } })
        ));
    }

    #[test]
    fn no_stored_status_is_never_current() {
        assert!(!status_is_current::<serde_json::Value>(None, &json!({})));
    }
}
