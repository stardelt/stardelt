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

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::jiff::Timestamp;

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
}
