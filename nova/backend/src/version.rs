//! The running Nova's version (#55 slice 1): what the footer and the update card compare.
use axum::Json;
use serde::Serialize;

/// The version and the commit of the running process.
#[derive(Serialize, utoipa::ToSchema)]
pub struct Version {
    /// `NOVA_VERSION` when the deployment sets it, else the crate version.
    pub version: String,
    /// `NOVA_COMMIT` when the deployment sets it, else `unknown`.
    pub commit: String,
}

/// One environment value, where empty counts as absent: a container that exports
/// `NOVA_VERSION=""` must not hide the crate version behind an empty string.
fn present(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

/// The values the route answers, from the two environment values (a pure function, so the
/// fallbacks are tested without touching the process environment).
pub fn current(version: Option<String>, commit: Option<String>) -> Version {
    Version {
        version: present(version).unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()),
        commit: present(commit).unwrap_or_else(|| "unknown".to_string()),
    }
}

#[utoipa::path(
    get,
    operation_id = "nova_version",
    path = "/api/version",
    tag = "public",
    description = "The version and commit of the running Nova process: `version` is `NOVA_VERSION` when the deployment sets it and the crate version otherwise, `commit` is `NOVA_COMMIT` or `unknown`. Nothing is fetched and nothing leaves the instance.",
    responses(
        (status = 200, description = "The running version", body = Version),
        (status = 401, description = "No token to sign in with", body = crate::openapi::ErrorResponse),
    )
)]
pub(crate) async fn version() -> Json<Version> {
    Json(current(
        std::env::var("NOVA_VERSION").ok(),
        std::env::var("NOVA_COMMIT").ok(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both values given: the environment wins over the fallbacks.
    #[test]
    fn current_takes_both_values() {
        let v = current(Some("0.5.0".into()), Some("abc1234".into()));
        assert_eq!(v.version, "0.5.0");
        assert_eq!(v.commit, "abc1234");
    }

    /// Neither value set: the crate version and `unknown`.
    #[test]
    fn current_falls_back_to_the_crate() {
        let v = current(None, None);
        assert_eq!(v.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(v.commit, "unknown");
    }

    /// An empty environment value counts as absent.
    #[test]
    fn current_treats_empty_as_absent() {
        let v = current(Some(String::new()), Some(String::new()));
        assert_eq!(v.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(v.commit, "unknown");
    }
}
