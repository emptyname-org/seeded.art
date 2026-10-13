//! Bounded process-local sign-in. Restarting deliberately revokes every session.
mod challenge;
pub mod crypto;
pub mod oauth;

use challenge::Challenge;

use axum::{
    Json,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, hash_map::RandomState},
    hash::BuildHasher,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

use crate::{client_ip::rate_ip, model::EditProof};

const CHALLENGE_LIFETIME: Duration = Duration::from_secs(300);
const SESSION_LIFETIME: Duration = Duration::from_secs(8 * 3600);
const MAX_SESSIONS: usize = 4096;
const MAX_ACCOUNTS: usize = 8192;
const REPLAY_BUCKETS: usize = 65536;
const MAX_SESSIONS_PER_ADDRESS: usize = 8;

/// Set only by transport/proxy validation, never parsed from request headers here.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct TrustedClientIp(pub IpAddr);
impl TrustedClientIp {
    fn canonical(self) -> Self {
        Self(match self.0 {
            IpAddr::V6(ip) => ip.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(self.0),
            ip => ip,
        })
    }

    fn family(self) -> Self {
        Self(match self.canonical().0 {
            IpAddr::V6(ip) => Ipv6Addr::from(u128::from(ip) & (u128::MAX << 80)).into(),
            ip => ip,
        })
    }
}

/// User sentences without their number; responses append "(error N)." from `number`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    #[error("Sign in again")]
    Unauthenticated,
    #[error("Request rejected; reload the page")]
    Forbidden,
    #[error("Sign-in failed; try again")]
    InvalidSignature,
    #[error("Sign-in expired; try again")]
    Expired,
    #[error("Too many attempts; wait a minute")]
    RateLimited,
    #[error("Sign-in is unavailable right now")]
    Unavailable,
    #[error("Sign-in is unavailable right now")]
    Configuration,
}

impl AuthError {
    pub fn status(self) -> StatusCode {
        match self {
            Self::Unauthenticated | Self::InvalidSignature | Self::Expired => {
                StatusCode::UNAUTHORIZED
            }
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::Unavailable | Self::Configuration => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
    /// The fixed number shown with the sentence and logged; docs/errors.md lists them.
    pub fn number(self) -> u16 {
        match self {
            Self::Forbidden => 102,
            Self::InvalidSignature => 103,
            Self::Expired => 104,
            Self::RateLimited => 105,
            Self::Unavailable => 106,
            Self::Configuration => 107,
            Self::Unauthenticated => 109,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Unauthenticated => "Unauthenticated",
            Self::Forbidden => "Forbidden",
            Self::InvalidSignature => "InvalidSignature",
            Self::Expired => "Expired",
            Self::RateLimited => "RateLimited",
            Self::Unavailable => "Unavailable",
            Self::Configuration => "Configuration",
        }
    }
    /// The sentence with its number.
    pub fn message(self) -> String {
        crate::error_number::numbered(&self.to_string(), self.number())
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        crate::error_number::log(self.number(), self.name());
        let mut response = (
            self.status(),
            Json(serde_json::json!({"error":self.message(),"number":self.number()})),
        )
            .into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
        if self == Self::RateLimited {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, "60".parse().unwrap());
        }
        response
    }
}

#[derive(Clone)]
pub struct AuthIdentity {
    address: String,
    session_id: String,
    csrf: String,
    pub(crate) proof: EditProof,
    expires: Instant,
    expires_at: u64,
    client: TrustedClientIp,
    touched: Instant,
}

impl AuthIdentity {
    pub fn address(&self) -> &str {
        &self.address
    }
    /// A digest for binding server-side work, never a cookie bearer.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
    pub fn csrf_token(&self) -> &str {
        &self.csrf
    }
    pub fn public_key(&self) -> &str {
        &self.proof.public_key
    }
    pub fn view(&self) -> SessionView {
        SessionView {
            address: self.address.clone(),
            csrf: self.csrf.clone(),
            expires_at: self.expires_at,
        }
    }
}

#[derive(Serialize)]
pub struct SessionView {
    pub address: String,
    pub csrf: String,
    pub expires_at: u64,
}

#[derive(Serialize)]
pub struct ChallengeView {
    pub message: String,
    pub payload: String,
    pub csrf: String,
    pub expires_at: u64,
}

pub struct ChallengeGrant {
    pub view: ChallengeView,
    pub cookie: String,
}
pub struct SessionGrant {
    pub identity: AuthIdentity,
    pub cookie: String,
    pub clear_challenge: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignInRequest {
    pub address: String,
    pub public_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyRequest {
    pub signature: String,
}

struct Rate {
    started: Instant,
    count: u32,
}
impl Rate {
    fn new(now: Instant) -> Self {
        Self {
            started: now,
            count: 0,
        }
    }
    fn take(&mut self, now: Instant, limit: u32) -> Result<(), AuthError> {
        if now.duration_since(self.started) >= Duration::from_secs(60) {
            *self = Self::new(now);
        }
        if self.count >= limit {
            return Err(AuthError::RateLimited);
        }
        self.count += 1;
        Ok(())
    }
}
struct Account {
    sign_ins: Rate,
    edits: Rate,
    generation: u64,
    touched: Instant,
}

struct Replay {
    hash: RandomState,
    buckets: Box<[u64]>,
    generation: u64,
}
impl Replay {
    fn new() -> Self {
        Self {
            hash: RandomState::new(),
            buckets: vec![0; REPLAY_BUCKETS].into_boxed_slice(),
            generation: 0,
        }
    }

    fn bucket(&self, address: &str) -> usize {
        self.hash.hash_one(address) as usize % REPLAY_BUCKETS
    }

    fn forgotten(&self, address: &str) -> u64 {
        self.buckets[self.bucket(address)]
    }

    fn remember(&mut self, address: &str, generation: u64) {
        let bucket = self.bucket(address);
        self.buckets[bucket] = self.buckets[bucket].max(generation);
    }

    fn advance(&mut self) -> Result<u64, AuthError> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(AuthError::Unavailable)?;
        Ok(self.generation)
    }
}

struct Registry {
    sessions: HashMap<String, AuthIdentity>,
    accounts: HashMap<String, Account>,
    replay: Replay,
    verifying: HashMap<String, usize>,
    next_cleanup: Instant,
}
impl Registry {
    fn prune(&mut self, now: Instant) {
        if now < self.next_cleanup {
            return;
        }
        self.sessions.retain(|_, entry| entry.expires > now);
        let active_accounts = self.active_accounts();
        let expired: Vec<_> = self
            .accounts
            .iter()
            .filter(|(address, entry)| {
                !active_accounts.contains(*address)
                    && now.duration_since(entry.touched) >= SESSION_LIFETIME + CHALLENGE_LIFETIME
            })
            .map(|(address, _)| address.clone())
            .collect();
        for address in expired {
            self.forget_account(&address);
        }
        self.next_cleanup = now + Duration::from_secs(60);
    }

    fn active_accounts(&self) -> HashSet<String> {
        self.sessions
            .values()
            .map(|session| session.address.clone())
            .chain(self.verifying.keys().cloned())
            .collect()
    }

    fn generation(&self, address: &str) -> u64 {
        self.accounts
            .get(address)
            .map_or_else(|| self.replay.forgotten(address), |entry| entry.generation)
    }

    fn forget_account(&mut self, address: &str) {
        if let Some(entry) = self.accounts.remove(address) {
            // A collision may expire an unused cookie, but can never revive a used one.
            self.replay.remember(address, entry.generation);
        }
    }

    fn make_account_room(&mut self, address: &str) -> Result<(), AuthError> {
        if !self.accounts.contains_key(address) && self.accounts.len() >= MAX_ACCOUNTS {
            let active = self.active_accounts();
            let oldest = self
                .accounts
                .iter()
                .filter(|(key, _)| !active.contains(*key))
                .min_by_key(|(_, account)| account.touched)
                .map(|(key, _)| key.clone())
                // Sessions + verification pins are capped below MAX_ACCOUNTS.
                .ok_or(AuthError::Unavailable)?;
            self.forget_account(&oldest);
        }
        Ok(())
    }

    fn make_session_room(&mut self) {
        if self.sessions.len() < MAX_SESSIONS {
            return;
        }
        let mut family_counts = HashMap::<TrustedClientIp, usize>::new();
        let mut client_counts = HashMap::<TrustedClientIp, usize>::new();
        for session in self.sessions.values() {
            *family_counts.entry(session.client.family()).or_default() += 1;
            *client_counts.entry(session.client).or_default() += 1;
        }
        let oldest = self
            .sessions
            .iter()
            .min_by_key(|(key, session)| {
                (
                    std::cmp::Reverse(family_counts[&session.client.family()]),
                    std::cmp::Reverse(client_counts[&session.client]),
                    session.touched,
                    *key,
                )
            })
            .map(|(key, _)| key.clone());
        if let Some(key) = oldest {
            self.sessions.remove(&key);
        }
    }
}

struct VerificationPin {
    registry: Arc<Mutex<Registry>>,
    address: String,
}
impl Drop for VerificationPin {
    fn drop(&mut self) {
        let Ok(mut registry) = self.registry.lock() else {
            return;
        };
        if let Some(count) = registry.verifying.get_mut(&self.address) {
            *count -= 1;
            if *count == 0 {
                registry.verifying.remove(&self.address);
            }
        }
    }
}

#[derive(Clone)]
pub struct AuthState {
    origin: Arc<str>,
    secure: bool,
    challenge_secret: [u8; 32],
    started: Instant,
    inner: Arc<Mutex<Registry>>,
    crypto: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    pub(crate) writes: Arc<Semaphore>,
    pub(crate) write_waiting: Arc<Semaphore>,
}

impl AuthState {
    pub fn new(origin: &str) -> Result<Self, AuthError> {
        let parsed = reqwest::Url::parse(origin).map_err(|_| AuthError::Configuration)?;
        let local = parsed.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if parsed.origin().ascii_serialization() != origin
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || !(parsed.scheme() == "https" || (parsed.scheme() == "http" && local))
        {
            return Err(AuthError::Configuration);
        }
        // Fail startup if operating-system entropy is unavailable.
        let mut challenge_secret = [0; 32];
        getrandom::fill(&mut challenge_secret).map_err(|_| AuthError::Unavailable)?;
        let now = Instant::now();
        Ok(Self {
            origin: origin.into(),
            secure: parsed.scheme() == "https",
            challenge_secret,
            started: now,
            inner: Arc::new(Mutex::new(Registry {
                sessions: HashMap::new(),
                accounts: HashMap::new(),
                replay: Replay::new(),
                verifying: HashMap::new(),
                next_cleanup: now,
            })),
            crypto: Arc::new(Semaphore::new(2)),
            waiting: Arc::new(Semaphore::new(16)),
            writes: Arc::new(Semaphore::new(1)),
            write_waiting: Arc::new(Semaphore::new(8)),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Registry>, AuthError> {
        self.inner.lock().map_err(|_| AuthError::Unavailable)
    }
    fn session_name(&self) -> &str {
        if self.secure {
            "__Host-seeded_session"
        } else {
            "seeded_session"
        }
    }
    fn challenge_name(&self) -> &str {
        if self.secure {
            "__Host-seeded_challenge"
        } else {
            "seeded_challenge"
        }
    }
    fn cookie(&self, name: &str, value: &str, lifetime: u64) -> String {
        format!(
            "{name}={value}; Path=/; HttpOnly; SameSite=Strict; Max-Age={lifetime}{}",
            if self.secure { "; Secure" } else { "" }
        )
    }
    pub fn clear_session_cookie(&self) -> String {
        self.cookie(self.session_name(), "", 0)
    }
    pub fn clear_challenge_cookie(&self) -> String {
        self.cookie(self.challenge_name(), "", 0)
    }

    pub fn check_origin(&self, headers: &HeaderMap) -> Result<(), AuthError> {
        if headers.get_all(header::ORIGIN).iter().count() != 1
            || headers
                .get(header::ORIGIN)
                .and_then(|value| value.to_str().ok())
                != Some(&self.origin)
            || headers
                .get("sec-fetch-site")
                .is_some_and(|value| value != "same-origin" && value != "none")
        {
            return Err(AuthError::Forbidden);
        }
        Ok(())
    }

    pub fn identity(&self, headers: &HeaderMap) -> Result<Option<AuthIdentity>, AuthError> {
        let Some(bearer) = cookie_value(headers, self.session_name())? else {
            return Ok(None);
        };
        let id = fingerprint(&bearer);
        let now = Instant::now();
        let mut registry = self.lock()?;
        registry.prune(now);
        match registry.sessions.get_mut(&id) {
            Some(identity) if identity.expires > now => {
                identity.touched = now;
                Ok(Some(identity.clone()))
            }
            _ => {
                registry.sessions.remove(&id);
                Ok(None)
            }
        }
    }

    pub fn require(&self, headers: &HeaderMap) -> Result<AuthIdentity, AuthError> {
        self.check_origin(headers)?;
        let identity = self.identity(headers)?.ok_or(AuthError::Unauthenticated)?;
        check_csrf(headers, &identity.csrf)?;
        Ok(identity)
    }

    pub fn is_current(&self, identity: &AuthIdentity) -> Result<bool, AuthError> {
        let now = Instant::now();
        Ok(self
            .lock()?
            .sessions
            .get_mut(identity.session_id())
            .is_some_and(|entry| {
                if entry.expires <= now {
                    return false;
                }
                entry.touched = now;
                true
            }))
    }

    /// For trusted in-process callers. HTTP routes must use `challenge_for`.
    pub fn challenge(
        &self,
        headers: &HeaderMap,
        request: SignInRequest,
    ) -> Result<ChallengeGrant, AuthError> {
        self.challenge_for(
            TrustedClientIp(Ipv4Addr::LOCALHOST.into()),
            headers,
            request,
        )
    }

    pub fn challenge_for(
        &self,
        client: TrustedClientIp,
        headers: &HeaderMap,
        request: SignInRequest,
    ) -> Result<ChallengeGrant, AuthError> {
        self.check_origin(headers)?;
        if request.address.len() != 36
            || crypto::public_key_address(&request.public_key)? != request.address
        {
            return Err(AuthError::InvalidSignature);
        }
        // Reject ambiguous cookies without allocating state for pending challenges.
        cookie_value(headers, self.challenge_name())?;
        let rate_client = TrustedClientIp(rate_ip(client.canonical().0));
        let (generation, deadline) = {
            let mut registry = self.lock()?;
            let now = Instant::now();
            registry.prune(now);
            // Unverified public keys never allocate or charge address-wide state.
            let generation = registry.generation(&request.address);
            // Snapshot generation and deadline together so delayed issuance cannot
            // outlive retention of a later sign-in's replay generation.
            let deadline = now.duration_since(self.started).as_millis() as u64
                + CHALLENGE_LIFETIME.as_millis() as u64;
            (generation, deadline)
        };
        let challenge = Challenge {
            client: rate_client,
            address: request.address,
            public_key: request.public_key,
            csrf: token()?,
            nonce: token()?,
            issued: unix_now()?,
            deadline,
            generation,
        };
        let bearer = challenge.seal(&self.challenge_secret, &self.origin)?;
        Ok(ChallengeGrant {
            view: ChallengeView {
                message: challenge.message(&self.origin),
                payload: challenge.payload(&self.origin),
                csrf: challenge.csrf.clone(),
                expires_at: challenge.expires_at(),
            },
            cookie: self.cookie(self.challenge_name(), &bearer, CHALLENGE_LIFETIME.as_secs()),
        })
    }

    fn elapsed_millis(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn check_challenge(&self, registry: &Registry, challenge: &Challenge) -> Result<(), AuthError> {
        if challenge.deadline <= self.elapsed_millis()
            || registry.generation(&challenge.address) != challenge.generation
        {
            return Err(AuthError::Expired);
        }
        Ok(())
    }

    /// For trusted in-process callers. HTTP routes must use `verify_for`.
    pub async fn verify(
        &self,
        headers: &HeaderMap,
        request: VerifyRequest,
    ) -> Result<SessionGrant, AuthError> {
        self.verify_for(
            TrustedClientIp(Ipv4Addr::LOCALHOST.into()),
            headers,
            request,
        )
        .await
    }

    pub async fn verify_for(
        &self,
        client: TrustedClientIp,
        headers: &HeaderMap,
        request: VerifyRequest,
    ) -> Result<SessionGrant, AuthError> {
        self.check_origin(headers)?;
        crypto::validate_signature_encoding(&request.signature)?;
        let bearer = cookie_value(headers, self.challenge_name())?.ok_or(AuthError::Expired)?;
        let old_session =
            cookie_value(headers, self.session_name())?.map(|value| fingerprint(&value));
        let client = client.canonical();
        let rate_client = TrustedClientIp(rate_ip(client.0));
        let challenge = Challenge::open(&bearer, &self.challenge_secret, &self.origin)?;
        check_csrf(headers, &challenge.csrf)?;
        // Bound pins through session creation, including completed crypto replies.
        let _waiting = self
            .waiting
            .clone()
            .try_acquire_owned()
            .map_err(|_| AuthError::RateLimited)?;
        let pin = {
            let mut registry = self.lock()?;
            let now = Instant::now();
            registry.prune(now);
            self.check_challenge(&registry, &challenge)?;
            *registry
                .verifying
                .entry(challenge.address.clone())
                .or_default() += 1;
            VerificationPin {
                registry: self.inner.clone(),
                address: challenge.address.clone(),
            }
        };
        let permit = tokio::time::timeout(
            Duration::from_millis(250),
            self.crypto.clone().acquire_owned(),
        )
        .await
        .map_err(|_| AuthError::RateLimited)?
        .map_err(|_| AuthError::Unavailable)?;
        self.check_challenge(&*self.lock()?, &challenge)?;
        let message = challenge.message(&self.origin);
        let payload = crypto::packed_message(&message);
        let task = tokio::task::spawn_blocking(move || {
            // Cancellation never releases the crypto slot before work has stopped.
            let _permit = permit;
            crypto::verify_signature(
                &challenge.address,
                &challenge.public_key,
                &payload,
                &request.signature,
            )?;
            Ok::<_, AuthError>((challenge, request.signature, pin))
        });
        let (challenge, signature, _pin) = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .map_err(|_| AuthError::Unavailable)?
            .map_err(|_| AuthError::Unavailable)??;
        let now = Instant::now();
        let session = token()?;
        let identity = AuthIdentity {
            address: challenge.address.clone(),
            session_id: fingerprint(&session),
            csrf: token()?,
            expires: now + SESSION_LIFETIME,
            expires_at: unix_now()? + SESSION_LIFETIME.as_secs(),
            client: rate_client,
            touched: now,
            proof: EditProof {
                message,
                signature,
                public_key: challenge.public_key.clone(),
            },
        };
        let mut registry = self.lock()?;
        let now = Instant::now();
        self.check_challenge(&registry, &challenge)?;
        registry.prune(now);
        registry.make_account_room(&identity.address)?;
        let account = registry
            .accounts
            .entry(identity.address.clone())
            .or_insert_with(|| Account {
                sign_ins: Rate::new(now),
                edits: Rate::new(now),
                generation: challenge.generation,
                touched: now,
            });
        account.sign_ins.take(now, 16)?;
        account.touched = now;
        // Consume all older challenges for this address atomically with session creation.
        let generation = registry.replay.advance()?;
        registry
            .accounts
            .get_mut(&identity.address)
            .unwrap()
            .generation = generation;
        let own: Vec<_> = registry
            .sessions
            .iter()
            .filter(|(_, session)| session.address == identity.address)
            .map(|(id, session)| (id.clone(), session.expires))
            .collect();
        let rotates_own = old_session
            .as_ref()
            .is_some_and(|old| own.iter().any(|(id, _)| id == old));
        let evict = if own.len() - usize::from(rotates_own) >= MAX_SESSIONS_PER_ADDRESS {
            own.iter()
                .min_by_key(|(_, expires)| *expires)
                .map(|(id, _)| id.clone())
        } else {
            None
        };
        if let Some(old) = old_session {
            registry.sessions.remove(&old);
        }
        if let Some(old) = evict {
            registry.sessions.remove(&old);
        }
        registry.make_session_room();
        registry
            .sessions
            .insert(identity.session_id.clone(), identity.clone());
        Ok(SessionGrant {
            identity,
            cookie: self.cookie(self.session_name(), &session, SESSION_LIFETIME.as_secs()),
            clear_challenge: self.clear_challenge_cookie(),
        })
    }

    /// Called after session + CSRF verification, before joining the write queue.
    pub fn admit_profile_edit(&self, identity: &AuthIdentity) -> Result<(), AuthError> {
        let now = Instant::now();
        let mut registry = self.lock()?;
        if registry
            .sessions
            .get(identity.session_id())
            .is_none_or(|entry| entry.expires <= now)
        {
            return Err(AuthError::Unauthenticated);
        }
        let account = registry
            .accounts
            .get_mut(identity.address())
            .ok_or(AuthError::Unauthenticated)?;
        account.edits.take(now, 6)?;
        account.touched = now;
        Ok(())
    }

    pub fn logout(&self, headers: &HeaderMap, all: bool) -> Result<(), AuthError> {
        let identity = self.require(headers)?;
        self.logout_identity(&identity, all)
    }

    fn logout_identity(&self, identity: &AuthIdentity, all: bool) -> Result<(), AuthError> {
        let mut registry = self.lock()?;
        registry.sessions.remove(identity.session_id());
        if all {
            let generation = registry.replay.advance()?;
            registry
                .sessions
                .retain(|_, entry| entry.address != identity.address);
            if let Some(account) = registry.accounts.get_mut(&identity.address) {
                account.generation = generation;
                account.touched = Instant::now();
            } else {
                // The last session and account may disappear after require() unlocks.
                registry.replay.remember(&identity.address, generation);
            }
        }
        Ok(())
    }
}

fn unix_now() -> Result<u64, AuthError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| AuthError::Unavailable)
}
fn token() -> Result<String, AuthError> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(|_| AuthError::Unavailable)?;
    Ok(hex::encode(bytes))
}
fn fingerprint(token: &str) -> String {
    hex::encode(crypto::blake::<32>(token.as_bytes()))
}
fn check_csrf(headers: &HeaderMap, expected: &str) -> Result<(), AuthError> {
    let value = headers
        .get("x-csrf-token")
        .and_then(|value| value.to_str().ok())
        .ok_or(AuthError::Forbidden)?;
    if headers.get_all("x-csrf-token").iter().count() != 1
        || value.len() != 64
        || !bool::from(value.as_bytes().ct_eq(expected.as_bytes()))
    {
        return Err(AuthError::Forbidden);
    }
    Ok(())
}
fn cookie_value(headers: &HeaderMap, name: &str) -> Result<Option<String>, AuthError> {
    let mut found = None;
    let mut length = 0;
    for header in headers.get_all(header::COOKIE) {
        length += header.as_bytes().len();
        if length > 8192 {
            return Err(AuthError::Unauthenticated);
        }
        let text = header.to_str().map_err(|_| AuthError::Unauthenticated)?;
        for part in text.split(';') {
            if let Some((key, value)) = part.trim().split_once('=') {
                if key == name {
                    if found.is_some()
                        || if name.ends_with("challenge") {
                            value.is_empty() || value.len() > challenge::MAX_COOKIE_LENGTH
                        } else {
                            value.len() != 64
                                || !value.bytes().all(|byte| {
                                    byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
                                })
                        }
                    {
                        return Err(AuthError::Unauthenticated);
                    }
                    found = Some(value.to_owned());
                }
            }
        }
    }
    Ok(found)
}

#[cfg(test)]
mod tests;
