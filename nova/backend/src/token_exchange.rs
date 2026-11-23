//! The engine token the MCP SQL tools run as, minted by RFC 8693 token exchange
//! (`NOVA_MCP_EXCHANGE_AUDIENCE`, stardelt/stardelt#1455).
//!
//! The hosted MCP server (`mcp.rs`) is called with the person's MCP token, whose audience is
//! `stardelt-mcp`; Trino only accepts a token whose audience is `trino`. Forwarding the MCP
//! token therefore failed (`malformed Trino response …`), so Nova exchanges it at the identity
//! provider for an engine token, as **itself**: the actor is Nova's own confidential client
//! (`NOVA_OIDC_CLIENT_ID`/`_SECRET`, the realm client `nova`), never the caller.
//!
//! - `NOVA_MCP_EXCHANGE_AUDIENCE` names the audience of the minted token, `trino` by default.
//!   An explicitly empty value turns the exchange off — the escape hatch that restores the
//!   old forwarding behaviour.
//! - The endpoint is the `token_endpoint` of the identity provider's discovery document, so an
//!   install on any OIDC provider needs no URL; a provider that advertises none leaves the
//!   exchange off.
//! - One token per subject in memory, reused until 60 s before `exp`; expired entries are
//!   dropped on every insert, and at 256 entries the cache is cleared.
//! - Neither the client secret nor a token — the subject's or the minted one — is ever logged,
//!   returned or part of an error. Both live in [`Secret`], whose `Debug` prints
//!   `[redacted]`, and every sentence in [`Error`] is fixed text: no response body reaches it
//!   and no URL is repeated.
//!
//! The D44 twin is `catalog_token.rs`: same `Config`/`Env`/`resolve` shape, same `Secret`,
//! same cache and same fake-IdP tests.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::auth::{Secret, sanitize_code};

/// Renew this long before `exp`, so a request cannot race the expiry.
const REFRESH_MARGIN: Duration = Duration::from_secs(60);

/// Entries the cache may hold before it is cleared (one per subject token seen).
const MAX_CACHED: usize = 256;

/// The RFC 8693 grant type.
const GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
const ACCESS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";

/// The variable that names the audience of the minted token.
const AUDIENCE_VAR: &str = "NOVA_MCP_EXCHANGE_AUDIENCE";
const DEFAULT_AUDIENCE: &str = "trino";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Where the engine token comes from and who asks for it.
#[derive(Clone, Debug)]
pub struct TokenExchangeConfig {
    /// The realm's token endpoint, e.g.
    /// `https://id.<domain>/realms/stardelt/protocol/openid-connect/token`.
    pub token_url: String,
    /// The acting client: Nova's own confidential client, `nova`. Not a secret.
    pub client_id: String,
    /// That client's secret. `Debug` prints `[redacted]` (see [`Secret`]).
    pub client_secret: Secret,
    /// The audience the minted token must carry: `aud: trino` by default.
    pub audience: String,
}

/// `NOVA_MCP_EXCHANGE_AUDIENCE`, before the token endpoint is known.
///
/// The endpoint is always the discovered one ([`TokenExchangeEnv::resolve`]); Nova has no
/// explicit exchange-URL variable, and needs none.
#[derive(Clone, Debug)]
pub struct TokenExchangeEnv {
    audience: String,
}

impl TokenExchangeEnv {
    /// Unset means `trino`; an explicitly empty value means the exchange is off, which is the
    /// escape hatch back to forwarding the caller's token. Any other value is the audience,
    /// trimmed.
    pub fn from_env_with(get: impl Fn(&str) -> Option<String>) -> Result<Option<Self>, String> {
        match get(AUDIENCE_VAR) {
            None => Ok(Some(Self {
                audience: DEFAULT_AUDIENCE.into(),
            })),
            Some(value) => match value.trim() {
                "" => Ok(None),
                audience => Ok(Some(Self {
                    audience: audience.to_string(),
                })),
            },
        }
    }

    pub fn from_env() -> Result<Option<Self>, String> {
        Self::from_env_with(|k| std::env::var(k).ok())
    }

    /// The finished configuration: the discovered `token_endpoint` (checked `http(s)` + host)
    /// and the acting client Nova asks as. `None` when the provider advertises no endpoint —
    /// the exchange stays off rather than failing at the first statement.
    ///
    /// `Err` only for an endpoint that is not an `http(s)` URL with a host.
    pub fn resolve(
        self,
        discovery_token_endpoint: Option<String>,
        client_id: String,
        client_secret: Secret,
    ) -> Result<Option<TokenExchangeConfig>, String> {
        let Some(token_url) = discovery_token_endpoint
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
        else {
            return Ok(None);
        };

        let parsed = url::Url::parse(&token_url)
            .map_err(|_| "the OIDC discovery token_endpoint is not a URL".to_string())?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err("the OIDC discovery token_endpoint must be an http(s) URL".into());
        }

        Ok(Some(TokenExchangeConfig {
            token_url,
            client_id,
            client_secret,
            audience: self.audience,
        }))
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why the engine token could not be minted.
///
/// The sentences are fixed text: no secret, no subject token, no minted token, no response
/// body, no URL. A tool answers them to its caller, so they have to stay safe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The endpoint did not answer at all (DNS, TCP, TLS).
    Unreachable,
    /// It answered non-2xx. Carries the status and the RFC 6749 `error` code only.
    Refused(u16, String),
    /// It answered 2xx without the fields a token response must have.
    Malformed,
}

impl Error {
    /// The sentence a tool shows: what failed, and nothing that could carry a credential.
    pub fn sentence(&self) -> String {
        match self {
            Error::Unreachable => {
                "the engine token could not be minted: the identity provider is not reachable"
                    .to_string()
            }
            Error::Refused(status, code) if code.is_empty() => format!(
                "the engine token could not be minted: exchange refused (HTTP {status})"
            ),
            Error::Refused(status, code) => format!(
                "the engine token could not be minted: exchange refused (HTTP {status} {code})"
            ),
            Error::Malformed => {
                "the engine token could not be minted: the identity provider answered no usable token"
                    .to_string()
            }
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.sentence())
    }
}

// ---------------------------------------------------------------------------
// The client
// ---------------------------------------------------------------------------

/// One cached engine token per subject, and the client that mints them.
pub struct TokenExchange {
    config: TokenExchangeConfig,
    http: reqwest::Client,
    /// Guarded across the fetch on purpose: a caller that arrives while a fetch is running
    /// waits for that attempt instead of starting a second one.
    cached: Mutex<HashMap<u64, CachedToken>>,
}

#[derive(Debug)]
struct CachedToken {
    token: Secret,
    /// `exp - 60 s`: from here on the next call mints a new token.
    refresh_at: Instant,
}

#[derive(Deserialize)]
struct ExchangeResponse {
    access_token: String,
    /// Seconds the token is valid. Required: without it Nova cannot know when to renew,
    /// and a token cached forever is worse than a loud failure.
    expires_in: u64,
}

/// The cache key: a hash of the subject token, so the table never holds the token itself.
fn subject_key(subject: &Secret) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    subject.expose().hash(&mut hasher);
    hasher.finish()
}

impl TokenExchange {
    /// The client and its configuration. `http` is the caller's client (Nova's shared
    /// upstream one, so the platform CA is trusted); its timeouts, if any, govern the
    /// exchange request too.
    pub fn new(http: reqwest::Client, config: TokenExchangeConfig) -> Self {
        Self {
            config,
            http,
            cached: Mutex::new(HashMap::new()),
        }
    }

    /// The engine token for `subject`: minted once and reused until it is within
    /// [`REFRESH_MARGIN`] of `exp`.
    pub async fn exchange(&self, subject: &Secret) -> Result<Secret, Error> {
        let key = subject_key(subject);
        let mut cached = self.cached.lock().await;
        if let Some(entry) = cached.get(&key)
            && Instant::now() < entry.refresh_at
        {
            return Ok(entry.token.clone());
        }
        let fresh = self.mint(subject).await?;
        let token = fresh.token.clone();
        // Bounded memory: expired subjects go, and at the cap the table is cleared rather
        // than an eviction policy being argued about.
        let now = Instant::now();
        cached.retain(|_, entry| now < entry.refresh_at);
        if cached.len() >= MAX_CACHED {
            cached.clear();
        }
        cached.insert(key, fresh);
        Ok(token)
    }

    async fn mint(&self, subject: &Secret) -> Result<CachedToken, Error> {
        // The client authenticates with `client_id`/`client_secret` in the form, never with
        // `Basic`: the realm's `nova` client uses `client_secret_post` (RFC 8414).
        let response = self
            .http
            .post(&self.config.token_url)
            .form(&[
                ("grant_type", GRANT_TYPE),
                ("subject_token", subject.expose()),
                ("subject_token_type", ACCESS_TOKEN_TYPE),
                ("requested_token_type", ACCESS_TOKEN_TYPE),
                ("audience", self.config.audience.as_str()),
                ("client_id", self.config.client_id.as_str()),
                ("client_secret", self.config.client_secret.expose()),
            ])
            .send()
            .await
            .map_err(|_| Error::Unreachable)?;

        let status = response.status();
        if !status.is_success() {
            // The RFC 6749 error code only. The body is deliberately not read for anything
            // else: it is upstream text, and an intermediary may echo the request —
            // credentials included — into it.
            let code = response
                .json::<serde_json::Value>()
                .await
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(sanitize_code))
                .unwrap_or_default();
            return Err(Error::Refused(status.as_u16(), code));
        }

        let body = response
            .json::<ExchangeResponse>()
            .await
            .map_err(|_| Error::Malformed)?;
        let ttl = Duration::from_secs(body.expires_in);
        Ok(CachedToken {
            token: Secret::new(body.access_token),
            refresh_at: Instant::now() + ttl.saturating_sub(REFRESH_MARGIN),
        })
    }
}

/// `Debug` names the endpoint, the client and the audience, prints the secret as
/// `[redacted]`, and never touches the cached tokens.
impl std::fmt::Debug for TokenExchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenExchange")
            .field("token_url", &self.config.token_url)
            .field("client_id", &self.config.client_id)
            .field("client_secret", &self.config.client_secret)
            .field("audience", &self.config.audience)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::{Json, Router};
    use serde_json::json;

    use super::{Error, Secret, TokenExchange, TokenExchangeConfig};

    const CLIENT: &str = "nova";
    const SECRET_CANARY: &str = "client-secret-canary-4d81";
    const SUBJECT_CANARY: &str = "mcp-subject-canary-42a1";
    const TOKEN_CANARY: &str = "exchanged-token-canary-9d0b";

    /// How many exchanges the fake IdP was asked for, and with which form.
    #[derive(Default)]
    struct Seen {
        requests: AtomicUsize,
        forms: StdMutex<Vec<std::collections::HashMap<String, String>>>,
    }

    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        format!("http://{addr}")
    }

    /// An IdP token endpoint that answers like Keycloak's exchange does.
    async fn token_endpoint(seen: Arc<Seen>) -> String {
        let handler = move |State(seen): State<Arc<Seen>>,
                            axum::Form(form): axum::Form<
            std::collections::HashMap<String, String>,
        >| {
            let seen = seen.clone();
            async move {
                seen.requests.fetch_add(1, Ordering::SeqCst);
                seen.forms.lock().expect("forms").push(form);
                Json(json!({
                    "access_token": TOKEN_CANARY,
                    "expires_in": 300,
                    "token_type": "Bearer",
                }))
            }
        };
        format!(
            "{}/token",
            serve(
                Router::new()
                    .route("/token", post(handler))
                    .with_state(seen),
            )
            .await
        )
    }

    /// An IdP that refuses the exchange, with a body that (as an intermediary might) echoes
    /// the client secret back.
    async fn refusing_token_endpoint() -> String {
        let base = serve(Router::new().route(
            "/token",
            post(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid_request", "echo": SECRET_CANARY})),
                )
            }),
        ))
        .await;
        format!("{base}/token")
    }

    /// An IdP that answers 200 without the fields a token response must have.
    async fn tokenless_endpoint() -> String {
        let base = serve(Router::new().route(
            "/token",
            post(|| async { Json(json!({"expires_in": 300, "token_type": "Bearer"})) }),
        ))
        .await;
        format!("{base}/token")
    }

    fn config(token_url: &str) -> TokenExchangeConfig {
        TokenExchangeConfig {
            token_url: token_url.to_string(),
            client_id: CLIENT.into(),
            client_secret: Secret::new(SECRET_CANARY),
            audience: "trino".into(),
        }
    }

    fn subject() -> Secret {
        Secret::new(SUBJECT_CANARY)
    }

    fn exchange(token_url: &str) -> TokenExchange {
        TokenExchange::new(reqwest::Client::new(), config(token_url))
    }

    #[tokio::test]
    async fn exchange_is_cached_per_subject() {
        let seen = Arc::new(Seen::default());
        let base = token_endpoint(seen.clone()).await;
        let exchange = exchange(&base);

        let first = exchange
            .exchange(&subject())
            .await
            .expect("an engine token");
        assert_eq!(first.expose(), TOKEN_CANARY);

        // The same person's token again: answered from the cache, not the IdP.
        let second = exchange
            .exchange(&subject())
            .await
            .expect("the cached token");
        assert_eq!(second.expose(), TOKEN_CANARY);
        assert_eq!(seen.requests.load(Ordering::SeqCst), 1);

        let forms = seen.forms.lock().expect("forms");
        let form = forms.first().expect("one form");
        assert_eq!(
            form.get("grant_type").map(String::as_str),
            Some("urn:ietf:params:oauth:grant-type:token-exchange")
        );
        assert_eq!(
            form.get("subject_token").map(String::as_str),
            Some(SUBJECT_CANARY)
        );
        assert_eq!(
            form.get("subject_token_type").map(String::as_str),
            Some("urn:ietf:params:oauth:token-type:access_token")
        );
        assert_eq!(
            form.get("requested_token_type").map(String::as_str),
            Some("urn:ietf:params:oauth:token-type:access_token")
        );
        assert_eq!(form.get("audience").map(String::as_str), Some("trino"));
        assert_eq!(form.get("client_id").map(String::as_str), Some(CLIENT));
        assert_eq!(
            form.get("client_secret").map(String::as_str),
            Some(SECRET_CANARY)
        );
    }

    #[tokio::test]
    async fn refusal_names_the_status_and_the_code() {
        let base = refusing_token_endpoint().await;
        let error = exchange(&base)
            .exchange(&subject())
            .await
            .expect_err("refused");

        assert_eq!(error, Error::Refused(400, "invalid_request".into()));
        let sentence = error.sentence();
        assert!(sentence.contains("400"), "{sentence}");
        assert!(sentence.contains("invalid_request"), "{sentence}");
        assert!(!sentence.contains(SECRET_CANARY), "{sentence}");
        assert!(!sentence.contains(SUBJECT_CANARY), "{sentence}");
    }

    #[tokio::test]
    async fn answer_without_a_token_is_malformed() {
        let base = tokenless_endpoint().await;
        let error = exchange(&base)
            .exchange(&subject())
            .await
            .expect_err("no usable token");
        assert_eq!(error, Error::Malformed);
        assert_eq!(
            error.sentence(),
            "the engine token could not be minted: the identity provider answered no usable token"
        );
    }

    #[test]
    fn debug_never_prints_the_secret() {
        let rendered = format!("{:?}", config("https://id.example.test/token"));
        assert!(!rendered.contains(SECRET_CANARY), "{rendered}");
        assert!(rendered.contains("[redacted]"), "{rendered}");
    }
}
