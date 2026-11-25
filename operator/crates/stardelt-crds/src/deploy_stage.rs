//! The `DeployStage` kind (#1628 slice 1): the bundle target and the CI subject allowed to deploy to it (#57).

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_deploy_audience() -> String {
    "stardelt-deploy".into()
}

/// One deploy stage: the bundle target is the object's name and `spec.subject` is the exact CI subject allowed to deploy to it (#57).
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "stardelt.io",
    version = "v1alpha1",
    kind = "DeployStage",
    namespaced,
    shortname = "dstage",
    doc = "One deploy stage: the bundle target names the object, and spec.subject is the exact CI subject (a GitHub Actions `sub`) allowed to deploy to that stage (#57). The operator renders one Keycloak client per object; Nova authorizes a deploy whose token was issued to that client."
)]
pub struct DeployStageSpec {
    /// The exact CI subject as the identity provider receives it: `repo:<owner>/<name>:ref:refs/heads/<branch>` or `repo:<owner>/<name>:environment:<name>` (with GitHub's immutable ids: `repo:<owner>@<id>/<name>@<id>:ref:refs/heads/<branch>`).
    #[schemars(
        length(min = 8, max = 512),
        regex(pattern = r"^repo:[^\s]+:(ref:refs/heads/[^\s]+|environment:[a-z0-9][a-z0-9-]*)$")
    )]
    pub subject: String,
    /// The GitHub `environment` the token was issued for, when the subject names one. Pinning an environment drops the `ref` part of the subject (#819), so the two are either/or.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 64), regex(pattern = r"^[a-z0-9][a-z0-9-]*$"))]
    pub environment: Option<String>,
    /// The audience the realm client's mapper puts in the token this stage accepts. Default `stardelt-deploy`.
    #[serde(default = "default_deploy_audience")]
    #[schemars(
        length(min = 1, max = 128),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
    )]
    pub audience: String,
    /// The owner group this stage deploys (#57): Nova lets a deploy principal whose client is this
    /// stage's act on a bundle of this group only. Optional; without it the deploy principal gets
    /// the "own group" refusal, which is the behaviour before this field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        length(min = 1, max = 64),
        regex(pattern = r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    )]
    pub group: Option<String>,
}

impl DeployStageSpec {
    pub fn validate(&self) -> Result<(), String> {
        // The audience rule the schema's `pattern` and length enforce, repeated so an offline
        // `validate` refuses what admission would (D33/D34), as the other `validate()`s here do.
        let audience = self.audience.as_bytes();
        let audience_ok = !audience.is_empty()
            && audience.len() <= 128
            && audience[0].is_ascii_alphanumeric()
            && audience[1..]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'));
        if !audience_ok {
            return Err(
                "spec.audience must be 1 to 128 letters, digits and `.`, `_`, `:`, `-`, starting with a letter or digit".into(),
            );
        }
        if let Some(group) = &self.group {
            let ok = !group.is_empty()
                && group.len() <= 64
                && group.as_bytes()[0].is_ascii_alphanumeric()
                && group
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
            if !ok {
                return Err("spec.group must be 1 to 64 letters, digits and `.`, `_`, `-`, starting with a letter or digit".into());
            }
        }
        if self.subject.contains(":pull_request") {
            return Err("a pull_request token names no branch and cannot be pinned to a stage; pin :ref:refs/heads/<branch> or :environment:<name>".into());
        }
        match &self.environment {
            Some(env) if !self.subject.ends_with(&format!(":environment:{env}")) => {
                Err(format!("spec.environment {env} does not match spec.subject"))
            }
            None if self.subject.contains(":environment:") => Err(
                "name the environment in spec.environment too, so the object says what the token carries"
                    .into(),
            ),
            _ => Ok(()),
        }
    }
}
