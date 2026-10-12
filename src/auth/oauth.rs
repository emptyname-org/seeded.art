//! Process-local OAuth2 authorization codes and access tokens for one first-party client.
//! Restarting revokes every outstanding code and token, as it does sessions.
use std::{
    cmp::Reverse,
    collections::HashMap,
    fmt,
    net::IpAddr,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use super::{AuthIdentity, TrustedClientIp};
use crate::client_ip::rate_ip;

pub const CODE_LIFETIME: Duration = Duration::from_secs(60);
pub const TOKEN_LIFETIME: Duration = Duration::from_secs(300);
pub const MAX_STATE_BYTES: usize = 512;
pub const MAX_NEXT_BYTES: usize = 2048;
const RATE_WINDOW: Duration = Duration::from_secs(60);

/// The single confidential client. Only a SHA-256 digest of its secret is kept.
#[derive(Clone)]
pub struct OAuthClient {
    client_id: String,
    secret_sha256: [u8; 32],
    redirect_uri: String,
}

impl fmt::Debug for OAuthClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuthClient")
            .field("client_id", &self.client_id)
            .field("redirect_uri", &self.redirect_uri)
            .finish_non_exhaustive()
    }
}

impl OAuthClient {
    pub fn new(client_id: &str, secret: &str, redirect_uri: &str) -> Result<Self, String> {
        if client_id.is_empty()
            || client_id.len() > 128
            || !client_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._~-".contains(&byte))
        {
            return Err(
                "SEEDED_OAUTH_CLIENT_ID must be 1-128 characters of A-Z, a-z, 0-9, '.', '_', '~' or '-'"
                    .into(),
            );
        }
        if secret.len() < 16 || secret.len() > 512 || secret.chars().any(char::is_control) {
            return Err(
                "SEEDED_OAUTH_CLIENT_SECRET must be 16-512 bytes without control characters".into(),
            );
        }
        let invalid = || {
            "SEEDED_OAUTH_REDIRECT_URI must be absolute HTTPS (HTTP only on loopback) without credentials or fragment"
                .to_string()
        };
        let url = reqwest::Url::parse(redirect_uri).map_err(|_| invalid())?;
        let loopback = url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || redirect_uri.len() > 512
            || redirect_uri.chars().any(char::is_control)
            || redirect_uri.contains('\\')
        {
            return Err(invalid());
        }
        let mut secret_sha256 = [0; 32];
        secret_sha256.copy_from_slice(&Sha256::digest(secret.as_bytes()));
        Ok(Self {
            client_id: client_id.into(),
            secret_sha256,
            redirect_uri: redirect_uri.into(),
        })
    }
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
    pub fn redirect_uri(&self) -> &str {
        &self.redirect_uri
    }
    /// Constant-time in the secret and the identifier.
    pub fn authenticate(&self, client_id: &str, secret: &str) -> bool {
        let id = Sha256::digest(client_id.as_bytes())
            .as_slice()
            .ct_eq(Sha256::digest(self.client_id.as_bytes()).as_slice());
        let secret = Sha256::digest(secret.as_bytes())
            .as_slice()
            .ct_eq(&self.secret_sha256);
        (id & secret).into()
    }
}

/// Monotonic time for expiry. `manual` moves only through `advance`, for deterministic tests.
#[derive(Clone)]
pub struct OAuthClock {
    started: Instant,
    manual: Option<Arc<AtomicU64>>,
}
impl OAuthClock {
    pub fn system() -> Self {
        Self {
            started: Instant::now(),
            manual: None,
        }
    }
    pub fn manual() -> Self {
        Self {
            started: Instant::now(),
            manual: Some(Arc::new(AtomicU64::new(0))),
        }
    }
    /// No effect on the system clock.
    pub fn advance(&self, by: Duration) {
        if let Some(manual) = &self.manual {
            manual.fetch_add(by.as_millis() as u64, Ordering::SeqCst);
        }
    }
    fn now(&self) -> Duration {
        match &self.manual {
            Some(manual) => Duration::from_millis(manual.load(Ordering::SeqCst)),
            None => self.started.elapsed(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct OAuthLimits {
    pub codes: usize,
    pub codes_per_session: usize,
    pub codes_per_address: usize,
    pub codes_per_network: usize,
    pub tokens: usize,
    pub tokens_per_session: usize,
    pub tokens_per_address: usize,
    /// Keyed by the visitor's network at authorization, not the client's.
    pub tokens_per_network: usize,
    /// Failed client authentications per client network per minute.
    pub client_failures_per_minute: u32,
    pub clients: usize,
}
impl Default for OAuthLimits {
    fn default() -> Self {
        Self {
            codes: 1024,
            codes_per_session: 2,
            codes_per_address: 4,
            codes_per_network: 16,
            tokens: 1024,
            tokens_per_session: 2,
            tokens_per_address: 4,
            tokens_per_network: 16,
            client_failures_per_minute: 60,
            clients: 4096,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OAuthError {
    /// The store holds nothing to reclaim; only with a zero capacity.
    Full,
    RateLimited,
    Unavailable,
}

pub(crate) struct CodeGrant {
    pub(crate) client_id: String,
    pub(crate) redirect_uri: String,
    pub(crate) identity: AuthIdentity,
    /// The visitor's network at authorization.
    pub(crate) network: IpAddr,
}
struct Code {
    grant: CodeGrant,
    serial: u64,
    expires: Duration,
}
struct Token {
    identity: AuthIdentity,
    network: IpAddr,
    serial: u64,
    expires: Duration,
}
/// A code or token's owners, for per-owner caps and reclaiming at capacity.
trait Held {
    fn identity(&self) -> &AuthIdentity;
    fn network(&self) -> IpAddr;
    fn serial(&self) -> u64;
}
impl Held for Code {
    fn identity(&self) -> &AuthIdentity {
        &self.grant.identity
    }
    fn network(&self) -> IpAddr {
        self.grant.network
    }
    fn serial(&self) -> u64 {
        self.serial
    }
}
impl Held for Token {
    fn identity(&self) -> &AuthIdentity {
        &self.identity
    }
    fn network(&self) -> IpAddr {
        self.network
    }
    fn serial(&self) -> u64 {
        self.serial
    }
}
struct Window {
    started: Duration,
    count: u32,
}
#[derive(Default)]
struct Grants {
    codes: HashMap<[u8; 32], Code>,
    serial: u64,
    tokens: HashMap<[u8; 32], Token>,
    clients: HashMap<IpAddr, Window>,
}

pub struct OAuthState {
    client: OAuthClient,
    clock: OAuthClock,
    limits: OAuthLimits,
    inner: Mutex<Grants>,
}

impl OAuthState {
    pub fn new(client: OAuthClient) -> Self {
        Self {
            client,
            clock: OAuthClock::system(),
            limits: OAuthLimits::default(),
            inner: Mutex::new(Grants::default()),
        }
    }
    pub fn with_clock(mut self, clock: OAuthClock) -> Self {
        self.clock = clock;
        self
    }
    pub fn with_limits(mut self, limits: OAuthLimits) -> Self {
        self.limits = limits;
        self
    }
    pub fn client(&self) -> &OAuthClient {
        &self.client
    }
    fn lock(&self) -> Result<MutexGuard<'_, Grants>, OAuthError> {
        self.inner.lock().map_err(|_| OAuthError::Unavailable)
    }

    /// A single-use code for the configured client and redirect, bound to this session.
    pub(crate) fn issue_code(
        &self,
        identity: &AuthIdentity,
        client: TrustedClientIp,
    ) -> Result<String, OAuthError> {
        let code = random_token()?;
        let network = rate_ip(client.0);
        let now = self.clock.now();
        let mut grants = self.lock()?;
        grants.codes.retain(|_, code| code.expires > now);
        // A session, address and network keep only their newest codes, so free sessions
        // cannot fill the shared store.
        make_room(
            &mut grants.codes,
            [
                self.limits.codes_per_session,
                self.limits.codes_per_address,
                self.limits.codes_per_network,
                self.limits.codes,
            ],
            identity,
            network,
        )?;
        grants.serial += 1;
        let serial = grants.serial;
        grants.codes.insert(
            digest(&code),
            Code {
                grant: CodeGrant {
                    client_id: self.client.client_id.clone(),
                    redirect_uri: self.client.redirect_uri.clone(),
                    identity: identity.clone(),
                    network,
                },
                serial,
                expires: now + CODE_LIFETIME,
            },
        );
        Ok(code)
    }

    /// Removes the code whatever the outcome; returns it only while unexpired.
    pub(crate) fn take_code(&self, code: &str) -> Result<Option<CodeGrant>, OAuthError> {
        let now = self.clock.now();
        let code = self.lock()?.codes.remove(&digest(code));
        Ok(code
            .filter(|code| code.expires > now)
            .map(|code| code.grant))
    }

    /// Bounded like codes, so repeated sign-ins of one visitor cannot fill the shared store.
    pub(crate) fn issue_token(&self, grant: CodeGrant) -> Result<String, OAuthError> {
        let token = random_token()?;
        let now = self.clock.now();
        let mut grants = self.lock()?;
        grants.tokens.retain(|_, token| token.expires > now);
        make_room(
            &mut grants.tokens,
            [
                self.limits.tokens_per_session,
                self.limits.tokens_per_address,
                self.limits.tokens_per_network,
                self.limits.tokens,
            ],
            &grant.identity,
            grant.network,
        )?;
        grants.serial += 1;
        let serial = grants.serial;
        grants.tokens.insert(
            digest(&token),
            Token {
                identity: grant.identity,
                network: grant.network,
                serial,
                expires: now + TOKEN_LIFETIME,
            },
        );
        Ok(token)
    }

    pub(crate) fn token_identity(&self, token: &str) -> Result<Option<AuthIdentity>, OAuthError> {
        let now = self.clock.now();
        let key = digest(token);
        let mut grants = self.lock()?;
        match grants.tokens.get(&key) {
            Some(token) if token.expires > now => Ok(Some(token.identity.clone())),
            Some(_) => {
                grants.tokens.remove(&key);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    /// `RateLimited` while this network has no failed client authentications left this minute.
    /// Checked before authenticating, so a network over its budget cannot keep guessing.
    pub(crate) fn admit_client_attempt(&self, client: TrustedClientIp) -> Result<(), OAuthError> {
        let ip = rate_ip(client.0);
        let now = self.clock.now();
        match self.lock()?.clients.get(&ip) {
            Some(window)
                if now.saturating_sub(window.started) < RATE_WINDOW
                    && window.count >= self.limits.client_failures_per_minute =>
            {
                Err(OAuthError::RateLimited)
            }
            _ => Ok(()),
        }
    }

    /// Spends one failed client authentication from the network's fixed one-minute window.
    /// The authenticated client never spends it, so callbacks with junk codes cannot block it.
    pub(crate) fn charge_client_failure(&self, client: TrustedClientIp) -> Result<(), OAuthError> {
        let ip = rate_ip(client.0);
        let now = self.clock.now();
        let mut grants = self.lock()?;
        if !grants.clients.contains_key(&ip) && grants.clients.len() >= self.limits.clients {
            grants
                .clients
                .retain(|_, window| now.saturating_sub(window.started) < RATE_WINDOW);
            if grants.clients.len() >= self.limits.clients {
                return Err(OAuthError::RateLimited);
            }
        }
        let window = grants.clients.entry(ip).or_insert(Window {
            started: now,
            count: 0,
        });
        if now.saturating_sub(window.started) >= RATE_WINDOW {
            *window = Window {
                started: now,
                count: 0,
            };
        }
        if window.count >= self.limits.client_failures_per_minute {
            return Err(OAuthError::RateLimited);
        }
        window.count += 1;
        Ok(())
    }
}

/// Room for one more entry of this session, address and network: each keeps only its newest
/// entries under `[session, address, network, total]` limits, and a full store gives up the
/// oldest entry of the network or address holding the most.
fn make_room<T: Held>(
    entries: &mut HashMap<[u8; 32], T>,
    [session, address, network, total]: [usize; 4],
    identity: &AuthIdentity,
    ip: IpAddr,
) -> Result<(), OAuthError> {
    keep_newest(entries, session, |entry| {
        entry.identity().session_id() == identity.session_id()
    });
    keep_newest(entries, address, |entry| {
        entry.identity().address() == identity.address()
    });
    keep_newest(entries, network, |entry| entry.network() == ip);
    while entries.len() >= total {
        let key = busiest_oldest(entries).ok_or(OAuthError::Full)?;
        entries.remove(&key);
    }
    Ok(())
}

/// Removes the oldest entries matching `member` so that a new one leaves at most `limit`.
fn keep_newest<T: Held>(
    entries: &mut HashMap<[u8; 32], T>,
    limit: usize,
    member: impl Fn(&T) -> bool,
) {
    let keep = limit.max(1) - 1;
    let mut own: Vec<(u64, [u8; 32])> = entries
        .iter()
        .filter(|(_, entry)| member(entry))
        .map(|(key, entry)| (entry.serial(), *key))
        .collect();
    if own.len() > keep {
        own.sort_unstable();
        for (_, key) in &own[..own.len() - keep] {
            entries.remove(key);
        }
    }
}

/// The oldest entry of the network or address holding the most entries.
fn busiest_oldest<T: Held>(entries: &HashMap<[u8; 32], T>) -> Option<[u8; 32]> {
    #[derive(PartialEq, Eq, Hash)]
    enum Holder<'a> {
        Network(IpAddr),
        Address(&'a str),
    }
    let mut holders: HashMap<Holder<'_>, (usize, u64, [u8; 32])> = HashMap::new();
    for (key, entry) in entries {
        for holder in [
            Holder::Network(entry.network()),
            Holder::Address(entry.identity().address()),
        ] {
            let held = holders.entry(holder).or_insert((0, entry.serial(), *key));
            held.0 += 1;
            if entry.serial() < held.1 {
                (held.1, held.2) = (entry.serial(), *key);
            }
        }
    }
    holders
        .into_values()
        .max_by_key(|(count, oldest, _)| (*count, Reverse(*oldest)))
        .map(|(_, _, key)| key)
}

/// `next` after sign-in: only this site's authorize endpoint, as a plain path and query.
pub fn valid_next(next: &str) -> bool {
    next.len() <= MAX_NEXT_BYTES
        && next.starts_with("/oauth/authorize?")
        && !next.contains("//")
        && next
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'\\')
}

/// A Flarum username (3-30 of `[A-Za-z0-9_-]`) from the shown name, else from the address.
pub fn forum_username(name: Option<&str>, address: &str) -> String {
    if let Some(name) = name {
        let mut username = String::new();
        let mut gap = false;
        for character in name.chars() {
            if character.is_ascii_alphanumeric() || character == '_' || character == '-' {
                if gap && !username.is_empty() {
                    username.push('_');
                }
                gap = false;
                username.push(character);
            } else {
                gap = true;
            }
        }
        if username.len() > 30 {
            username.truncate(30);
            username.truncate(username.trim_end_matches('_').len());
        }
        if username.len() >= 3 {
            return username;
        }
    }
    match (address.get(..8), address.len().checked_sub(5)) {
        (Some(head), Some(tail)) if address.len() >= 13 => format!("{head}-{}", &address[tail..]),
        _ => address.to_owned(),
    }
}

fn digest(token: &str) -> [u8; 32] {
    let mut out = [0; 32];
    out.copy_from_slice(&Sha256::digest(token.as_bytes()));
    out
}

fn random_token() -> Result<String, OAuthError> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(|_| OAuthError::Unavailable)?;
    Ok(base64url(&bytes))
}

const URL_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Unpadded base64url.
fn base64url(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk.iter().enumerate().fold(0_u32, |n, (index, byte)| {
            n | (u32::from(*byte) << (16 - 8 * index))
        });
        for index in 0..=chunk.len() {
            out.push(URL_ALPHABET[((n >> (18 - 6 * index)) & 63) as usize] as char);
        }
    }
    out
}

/// Padded standard base64, as HTTP Basic credentials use.
pub(crate) fn decode_base64(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    if bytes.is_empty() || bytes.len() % 4 != 0 {
        return None;
    }
    let chunks = bytes.len() / 4;
    let mut out = Vec::with_capacity(chunks * 3);
    for (index, chunk) in bytes.chunks(4).enumerate() {
        let pad = chunk.iter().rev().take_while(|byte| **byte == b'=').count();
        if pad > 2 || (pad > 0 && index + 1 != chunks) {
            return None;
        }
        let mut n = 0_u32;
        for (position, byte) in chunk[..4 - pad].iter().enumerate() {
            let value = match byte {
                b'A'..=b'Z' => byte - b'A',
                b'a'..=b'z' => byte - b'a' + 26,
                b'0'..=b'9' => byte - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                _ => return None,
            };
            n |= u32::from(value) << (18 - 6 * position);
        }
        out.extend_from_slice(&n.to_be_bytes()[1..4 - pad]);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_matches_reference_vectors() {
        assert_eq!(base64url(&[0xfb, 0xff]), "-_8");
        assert_eq!(base64url(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64url(&[0; 32]).len(), 43);
        assert_eq!(decode_base64("Zm9vOmJhcg==").unwrap(), b"foo:bar");
        assert_eq!(decode_base64("Zm9vYg==").unwrap(), b"foob");
        assert_eq!(decode_base64("Zm9vYmE=").unwrap(), b"fooba");
        for bad in ["", "Zm9", "Zm=v", "Zm9v====", "Zg==Zg==", "Zm9v!A=="] {
            assert!(decode_base64(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn client_secret_is_checked_against_its_digest() {
        let client = OAuthClient::new(
            "forum",
            "0123456789abcdef",
            "https://forum.example/auth/seeded",
        )
        .unwrap();
        assert!(client.authenticate("forum", "0123456789abcdef"));
        assert!(!client.authenticate("forum", "0123456789abcdeF"));
        assert!(!client.authenticate("forum2", "0123456789abcdef"));
        assert!(!format!("{client:?}").contains("0123456789abcdef"));
    }

    #[test]
    fn redirect_uri_must_be_https_or_loopback_http() {
        for uri in [
            "https://forum.example/auth/seeded",
            "http://127.0.0.1:9/cb",
            "http://localhost/cb",
            "http://[::1]:8/cb",
        ] {
            assert!(
                OAuthClient::new("forum", "0123456789abcdef", uri).is_ok(),
                "{uri}"
            );
        }
        for uri in [
            "http://forum.example/cb",
            "forum.example/cb",
            "/cb",
            "https://user:pass@forum.example/cb",
            "https://forum.example/cb#x",
            "javascript:alert(1)",
        ] {
            assert!(
                OAuthClient::new("forum", "0123456789abcdef", uri).is_err(),
                "{uri}"
            );
        }
        assert!(OAuthClient::new("", "0123456789abcdef", "https://f.example/").is_err());
        assert!(OAuthClient::new("a b", "0123456789abcdef", "https://f.example/").is_err());
        assert!(OAuthClient::new("forum", "short", "https://f.example/").is_err());
    }

    #[test]
    fn forum_username_keeps_usable_names_and_falls_back_to_the_address() {
        let address = "tz1gBXG9fg8RMDH69KfKqwoTH5sFDmzt5yzm";
        let fallback = "tz1gBXG9-t5yzm";
        for (name, expected) in [
            (Some("zancan-2"), "zancan-2"),
            (Some("Zancan_Art"), "Zancan_Art"),
            (Some("zancan.eth"), "zancan_eth"),
            (Some("  Jo  Smith!! "), "Jo_Smith"),
            (Some("ab"), fallback),
            (Some("ゼロ"), fallback),
            (Some("a b"), "a_b"),
            (Some("!!!"), fallback),
            (None, fallback),
            (Some(&"x".repeat(40)), &"x".repeat(30)),
            (Some(&format!("{} y", "x".repeat(29))), &"x".repeat(29)),
        ] {
            assert_eq!(forum_username(name, address), expected, "{name:?}");
        }
    }

    #[test]
    fn next_must_be_a_plain_authorize_path() {
        assert!(valid_next("/oauth/authorize?state=x"));
        for next in [
            "//evil.example/oauth/authorize?x",
            "/\\evil",
            "https://evil.example/oauth/authorize?x",
            "/oauth/authorizeX?x",
            "/oauth/authorize?a=//b",
            "/oauth/authorize?a=\u{7f}",
            "/oauth/authorize?a b",
        ] {
            assert!(!valid_next(next), "{next}");
        }
        assert!(valid_next(&format!(
            "/oauth/authorize?{}",
            "a".repeat(2031)
        )));
        assert!(!valid_next(&format!(
            "/oauth/authorize?{}",
            "a".repeat(2032)
        )));
    }

    #[test]
    fn failed_client_authentications_are_limited_per_client_network_per_minute() {
        let client = OAuthClient::new("forum", "0123456789abcdef", "https://f.example/").unwrap();
        let clock = OAuthClock::manual();
        let state = OAuthState::new(client)
            .with_clock(clock.clone())
            .with_limits(OAuthLimits {
                client_failures_per_minute: 2,
                clients: 1,
                ..OAuthLimits::default()
            });
        let first = TrustedClientIp("192.0.2.1".parse().unwrap());
        let second = TrustedClientIp("192.0.2.2".parse().unwrap());
        for _ in 0..2 {
            assert!(state.admit_client_attempt(first).is_ok());
            assert!(state.charge_client_failure(first).is_ok());
        }
        assert_eq!(
            state.admit_client_attempt(first),
            Err(OAuthError::RateLimited)
        );
        assert_eq!(
            state.charge_client_failure(first),
            Err(OAuthError::RateLimited)
        );
        assert!(state.admit_client_attempt(second).is_ok());
        assert_eq!(
            state.charge_client_failure(second),
            Err(OAuthError::RateLimited)
        );
        clock.advance(RATE_WINDOW);
        assert!(state.admit_client_attempt(first).is_ok());
        assert!(state.charge_client_failure(second).is_ok());
        assert!(state.admit_client_attempt(first).is_ok());
        assert_eq!(
            state.charge_client_failure(first),
            Err(OAuthError::RateLimited)
        );
    }
}
