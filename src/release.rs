//! Bounded current issuer reads. No historic sale is interpreted as a mint quote.
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use reqwest::{Client, Url};
use serde_json::Value;
use tokio::sync::Semaphore;

use crate::model::Project;

const V1: &str = "KT1AEVuykWeuuFX7QkEAMNtffzwhe1Z98hJS";
const V1_NEXT: &str = "KT1XCoGnfupWk7Sp8536EfrxcP73LmT68Nyr";
const V2: &str = "KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv";
const V3: &str = "KT1Xpmp15KfqoePNW9HczFmqaGNHwadV2a3b";
const MAX_BODY: usize = 128 * 1024;
const MAX_CACHE: usize = 128;
const TTL: Duration = Duration::from_secs(60);
const REQUEST_BURST: u32 = 64;

struct RequestBudget {
    remaining: u32,
    updated: Instant,
}

impl RequestBudget {
    fn take(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.updated).as_secs();
        if elapsed > 0 {
            self.remaining = self
                .remaining
                .saturating_add(elapsed.min(u64::from(REQUEST_BURST)) as u32)
                .min(REQUEST_BURST);
            self.updated += Duration::from_secs(elapsed);
        }
        if self.remaining == 0 {
            return false;
        }
        self.remaining -= 1;
        true
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Availability {
    Open,
    Paused,
    Disabled,
    SoldOut,
    Scheduled,
    Restricted,
}
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Pricing {
    Fixed(u64),
    Dutch,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct Release {
    pub availability: Availability,
    pub pricing: Pricing,
    pub remaining: u64,
    pub supply: u64,
    pub opens_at: Option<String>,
    pub level: u64,
    pub checked_at: String,
    pub issuer: String,
}

struct Cached {
    generation: u64,
    inserted: Instant,
    release: Release,
}
#[derive(Clone)]
pub struct ReleaseClient {
    http: Client,
    base: Url,
    cache: Arc<Mutex<HashMap<(String, u64), Cached>>>,
    permits: Arc<Semaphore>,
    timeout: Duration,
    generation: Arc<AtomicU64>,
    budget: Arc<Mutex<RequestBudget>>,
}

#[derive(Debug, thiserror::Error)]
pub enum ReleaseError {
    #[error("Invalid release API origin")]
    Origin,
    #[error("Release HTTP client unavailable")]
    Client(#[from] reqwest::Error),
}

impl ReleaseClient {
    pub fn new(base: &str) -> Result<Self, ReleaseError> {
        let mut base = Url::parse(base).map_err(|_| ReleaseError::Origin)?;
        if !matches!(base.scheme(), "http" | "https")
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(ReleaseError::Origin);
        }
        let prefix = base.path().trim_end_matches('/');
        let path = if prefix.ends_with("/v1") {
            format!("{prefix}/")
        } else {
            format!("{prefix}/v1/")
        };
        base.set_path(&path);
        Ok(Self {
            http: Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(2))
                .build()?,
            base,
            cache: Arc::new(Mutex::new(HashMap::new())),
            permits: Arc::new(Semaphore::new(4)),
            timeout: Duration::from_secs(2),
            generation: Arc::new(AtomicU64::new(0)),
            budget: Arc::new(Mutex::new(RequestBudget {
                remaining: REQUEST_BURST,
                updated: Instant::now(),
            })),
        })
    }

    pub async fn get(&self, project: &Project) -> Option<Release> {
        let issuer = project.issuer.as_deref()?;
        if !matches!(issuer, V1 | V1_NEXT | V2 | V3) {
            return None;
        }
        let key = (issuer.to_owned(), project.id);
        if let Some(cached) = self
            .cache
            .lock()
            .ok()?
            .get(&key)
            .filter(|entry| entry.inserted.elapsed() < TTL)
        {
            return Some(cached.release.clone());
        }
        let _permit = self.permits.clone().try_acquire_owned().ok()?;
        let generation = self
            .generation
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .ok()?;
        let release = tokio::time::timeout(self.timeout, self.fetch(issuer, project.id))
            .await
            .ok()??;
        let mut cache = self.cache.lock().ok()?;
        if let Some(newer) = cache
            .get(&key)
            .filter(|entry| entry.generation > generation || entry.release.level > release.level)
        {
            // Late work must not replace newer state or renew its freshness window.
            return (newer.inserted.elapsed() < TTL).then(|| newer.release.clone());
        }
        cache.retain(|_, entry| entry.inserted.elapsed() < TTL);
        if cache.len() >= MAX_CACHE {
            let oldest = cache
                .iter()
                .min_by_key(|(_, entry)| entry.inserted)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                cache.remove(&oldest);
            }
        }
        cache.insert(
            key,
            Cached {
                generation,
                inserted: Instant::now(),
                release: release.clone(),
            },
        );
        Some(release)
    }

    async fn json(&self, path: &str) -> Option<Value> {
        // Charge every upstream read, including retries and partial failures.
        // Exhaustion is transient and never writes a negative cache entry.
        if !self.budget.lock().ok()?.take(Instant::now()) {
            return None;
        }
        let mut response = self
            .http
            .get(self.base.join(path).ok()?)
            .send()
            .await
            .ok()?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|n| n > MAX_BODY as u64)
        {
            return None;
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.ok()? {
            if chunk.len() > MAX_BODY.saturating_sub(bytes.len()) {
                return None;
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).ok()
    }

    async fn fetch(&self, issuer: &str, id: u64) -> Option<Release> {
        let head = self.json("head").await?;
        let level = number(&head["level"])?;
        let checked_at = head["timestamp"].as_str()?.to_owned();
        let now = utc_nanos(&checked_at)?;
        let block_hash = head["hash"].as_str()?;
        // Historical state at the sampled head avoids mixing storage from different blocks.
        let storage = self
            .json(&format!("contracts/{issuer}/storage?level={level}"))
            .await?;
        let paused = storage["paused"].as_bool()?;
        let row = if issuer == V1 {
            let rows = self.json(&format!("contracts/{issuer}/bigmaps/ledger/historical_keys/{level}?key.nat={id}&active=true&limit=2")).await?;
            let rows = rows.as_array()?;
            if rows.len() != 1 || number(&rows[0]["key"]["nat"]) != Some(id) {
                return None;
            }
            rows[0].clone()
        } else {
            self.json(&format!(
                "contracts/{issuer}/bigmaps/ledger/historical_keys/{level}/{id}"
            ))
            .await?
        };
        if row["active"].as_bool() != Some(true)
            || (issuer != V1 && number(&row["key"]) != Some(id))
        {
            return None;
        }
        let entry = &row["value"];
        let remaining = number(&entry["balance"])?;
        let supply = number(&entry["supply"])?;
        if remaining > supply {
            return None;
        }
        let enabled = entry["enabled"].as_bool()?;
        let (pricing, opens_at, mint_enabled) = if matches!(issuer, V1 | V1_NEXT) {
            (Pricing::Fixed(number(&entry["price"])?), None, true)
        } else {
            let pricing_id = number(&entry["pricing_id"])?;
            let registration = &storage["pricing_contracts"][pricing_id.to_string()];
            let address = registration["address"].as_str()?;
            let fixed = match (issuer, pricing_id, address) {
                (V2, 0, "KT1FHzHxuMaNLYG8LdniY45M6RCfkF3AoXFh")
                | (V3, 0, "KT1V24J6FVuKPU3xy6gVF6wJ3zdRXBheQhaV") => true,
                (V2, 1, "KT1EzLrXRCXij42pKfbZPn48PuxrnVki1aYY")
                | (V3, 1, "KT1MFgHKorMWXeVL6qrpgjZmemirafppSg9q") => false,
                _ => return None,
            };
            let pricing_row = self
                .json(&format!(
                    "contracts/{address}/bigmaps/pricings/historical_keys/{level}/{id}"
                ))
                .await?;
            if pricing_row["active"].as_bool() != Some(true)
                || number(&pricing_row["key"]) != Some(id)
            {
                return None;
            }
            let pricing_storage = self
                .json(&format!("contracts/{address}/storage?level={level}"))
                .await?;
            let authorized = pricing_storage["authorized"][issuer].as_bool()?;
            let values = &pricing_row["value"];
            let pricing = if fixed {
                Pricing::Fixed(number(&values["price"])?)
            } else {
                let levels = values["levels"].as_object()?;
                if levels.is_empty()
                    || levels.len() > 1024
                    || levels.values().any(|price| number(price).is_none())
                    || number(&values["decrement_duration"])? == 0
                {
                    return None;
                }
                Pricing::Dutch
            };
            let opens = values.get("opens_at")?;
            let opens_at = if opens.is_null() {
                None
            } else {
                Some(opens.as_str()?.to_owned())
            };
            if let Some(opens) = &opens_at {
                utc_nanos(opens)?;
            }
            let rows = self.json(&format!("contracts/{issuer}/bigmaps/entrypoints_enabled/historical_keys/{level}?key=mint&limit=2")).await?;
            let rows = rows.as_array()?;
            if rows.len() != 1
                || rows[0]["key"].as_str() != Some("mint")
                || rows[0]["active"].as_bool() != Some(true)
            {
                return None;
            }
            (
                pricing,
                opens_at,
                rows[0]["value"].as_bool()? && registration["enabled"].as_bool()? && authorized,
            )
        };
        let reserved = match entry.get("reserves") {
            None if matches!(issuer, V1 | V1_NEXT) => 0,
            Some(Value::Array(reserves)) => reserves.iter().try_fold(0_u64, |sum, reserve| {
                sum.checked_add(number(&reserve["amount"])?)
            })?,
            _ => return None,
        };
        if issuer == V3
            && (!entry.get("has_tickets")?.is_boolean()
                || !entry.as_object()?.contains_key("open_editions"))
        {
            return None;
        }
        let restricted = entry
            .get("open_editions")
            .is_some_and(|value| !value.is_null())
            || entry
                .get("has_tickets")
                .is_some_and(|value| value != &Value::Bool(false));
        let availability = if paused {
            Availability::Paused
        } else if !enabled || !mint_enabled {
            Availability::Disabled
        } else if restricted {
            Availability::Restricted
        } else if remaining == 0 {
            Availability::SoldOut
        } else if opens_at
            .as_deref()
            .and_then(utc_nanos)
            .is_some_and(|opens| opens > now)
        {
            Availability::Scheduled
        } else if reserved >= remaining {
            Availability::Restricted
        } else {
            Availability::Open
        };
        let confirmed = self.json(&format!("blocks/{level}")).await?;
        if confirmed["hash"].as_str() != Some(block_hash)
            || confirmed["timestamp"].as_str() != Some(&checked_at)
        {
            return None;
        }
        Some(Release {
            availability,
            pricing,
            remaining,
            supply,
            opens_at,
            level,
            checked_at,
            issuer: issuer.into(),
        })
    }
}

fn number(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| {
        let text = value.as_str()?;
        (!text.is_empty() && text.bytes().all(|c| c.is_ascii_digit()))
            .then(|| text.parse().ok())
            .flatten()
    })
}

// UTC RFC3339 emitted by TzKT; retain fractional precision for opening schedules.
fn utc_nanos(value: &str) -> Option<i128> {
    if !value.is_ascii() || value.len() < 20 {
        return None;
    }
    let (date, time) = value.split_once('T')?;
    if date.len() != 10 || &date[4..5] != "-" || &date[7..8] != "-" {
        return None;
    }
    let digits = |text: &str| -> Option<i64> {
        if !text.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        text.parse().ok()
    };
    let year = digits(&date[..4])?;
    let month = usize::try_from(digits(&date[5..7])?).ok()?;
    let day = digits(&date[8..])?;
    if !(1970..=9999).contains(&year) || !(1..=12).contains(&month) {
        return None;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=month_days[month - 1]).contains(&day) {
        return None;
    }
    let raw_time = time.strip_suffix('Z')?;
    let (raw_time, fraction) = if let Some((seconds, fraction)) = raw_time.split_once('.') {
        if fraction.is_empty() || fraction.len() > 9 {
            return None;
        }
        (
            seconds,
            i128::from(digits(fraction)?) * 10_i128.pow(9 - fraction.len() as u32),
        )
    } else {
        (raw_time, 0)
    };
    if raw_time.len() != 8 || &raw_time[2..3] != ":" || &raw_time[5..6] != ":" {
        return None;
    }
    let hour = digits(&raw_time[..2])?;
    let minute = digits(&raw_time[3..5])?;
    let second = digits(&raw_time[6..])?;
    if !(0..24).contains(&hour) || !(0..60).contains(&minute) || !(0..60).contains(&second) {
        return None;
    }
    let leap_days = |year: i64| year / 4 - year / 100 + year / 400;
    let days = (year - 1970) * 365 + leap_days(year - 1) - leap_days(1969)
        + month_days[..month - 1].iter().sum::<i64>()
        + day
        - 1;
    Some(i128::from(days * 86400 + hour * 3600 + minute * 60 + second) * 1_000_000_000 + fraction)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        extract::{Request, State},
        http::StatusCode,
        response::IntoResponse,
    };
    use serde_json::json;
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicUsize, Ordering},
    };

    const FIXED: &str = "KT1V24J6FVuKPU3xy6gVF6wJ3zdRXBheQhaV";
    const DUTCH: &str = "KT1MFgHKorMWXeVL6qrpgjZmemirafppSg9q";
    const TIME: &str = "2026-10-05T10:00:00.100Z";
    type Replies = HashMap<String, VecDeque<(StatusCode, String)>>;
    #[derive(Default)]
    struct MockState {
        replies: Mutex<Replies>,
        requests: Mutex<Vec<String>>,
        delay_ms: AtomicUsize,
        gates: Mutex<HashMap<String, VecDeque<Arc<Semaphore>>>>,
    }
    struct Mock {
        base: String,
        state: Arc<MockState>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Mock {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    impl Mock {
        async fn new(issuer: &str) -> Self {
            let state = Arc::new(MockState::default());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}/proxy/v1", listener.local_addr().unwrap());
            let app = Router::new()
                .fallback(mock_request)
                .with_state(state.clone());
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let mock = Self { base, state, task };
            mock.json(
                "head",
                json!({"level":123,"hash":"Bsample","timestamp":TIME}),
            );
            mock.json(
                "blocks/123",
                json!({"level":123,"hash":"Bsample","timestamp":TIME}),
            );
            mock.json(&format!("contracts/{issuer}/storage?level=123"), json!({"paused":false,"pricing_contracts":{"0":{"address":FIXED,"enabled":true},"1":{"address":DUTCH,"enabled":true}}}));
            let key = if issuer == V1 {
                json!({"nat":"29437","address":"tz1creator"})
            } else {
                json!("29437")
            };
            let row = json!({"active":true,"key":key,"value":{"price":"20000000","balance":"310","supply":"320","enabled":true,"pricing_id":"0","reserves":[{"amount":"19"}],"has_tickets":false,"open_editions":null}});
            mock.json(
                &ledger_path(issuer),
                if issuer == V1 { json!([row]) } else { row },
            );
            mock.json(&format!("contracts/{issuer}/bigmaps/entrypoints_enabled/historical_keys/123?key=mint&limit=2"), json!([{"active":true,"key":"mint","value":true}]));
            for pricer in [FIXED, DUTCH] {
                mock.json(
                    &format!("contracts/{pricer}/storage?level=123"),
                    json!({"authorized":{issuer:true}}),
                );
                mock.json(&format!("contracts/{pricer}/bigmaps/pricings/historical_keys/123/29437"), json!({"active":true,"key":"29437","value":{"price":"20000000","opens_at":null,"levels":{"0":"20000000","1":"10000000"},"decrement_duration":"60"}}));
            }
            if issuer == V2 {
                let mut replies = mock.state.replies.lock().unwrap();
                let previous = replies.clone();
                for (path, values) in previous {
                    let path = path
                        .replace(FIXED, "KT1FHzHxuMaNLYG8LdniY45M6RCfkF3AoXFh")
                        .replace(DUTCH, "KT1EzLrXRCXij42pKfbZPn48PuxrnVki1aYY");
                    let values = values
                        .into_iter()
                        .map(|(status, body)| {
                            (
                                status,
                                body.replace(FIXED, "KT1FHzHxuMaNLYG8LdniY45M6RCfkF3AoXFh")
                                    .replace(DUTCH, "KT1EzLrXRCXij42pKfbZPn48PuxrnVki1aYY"),
                            )
                        })
                        .collect();
                    replies.insert(path, values);
                }
            }
            mock
        }
        fn json(&self, path: &str, value: Value) {
            self.reply(path, vec![(StatusCode::OK, value.to_string())]);
        }
        fn reply(&self, path: &str, replies: Vec<(StatusCode, String)>) {
            self.state
                .replies
                .lock()
                .unwrap()
                .insert(format!("/proxy/v1/{path}"), replies.into());
        }
        fn change(&self, path: &str, edit: impl FnOnce(&mut Value)) {
            let key = format!("/proxy/v1/{path}");
            let mut replies = self.state.replies.lock().unwrap();
            let entry = replies.get_mut(&key).unwrap().front_mut().unwrap();
            let mut value: Value = serde_json::from_str(&entry.1).unwrap();
            edit(&mut value);
            entry.1 = value.to_string();
        }
        fn count(&self) -> usize {
            self.state.requests.lock().unwrap().len()
        }
    }
    async fn mock_request(
        State(state): State<Arc<MockState>>,
        request: Request,
    ) -> impl IntoResponse {
        let path = request.uri().to_string();
        state.requests.lock().unwrap().push(path.clone());
        let delay = state.delay_ms.load(Ordering::Relaxed);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
        }
        let response = {
            let mut replies = state.replies.lock().unwrap();
            let Some(values) = replies.get_mut(&path) else {
                return (StatusCode::NOT_FOUND, String::new());
            };
            if values.len() > 1 {
                values.pop_front().unwrap()
            } else {
                values.front().unwrap().clone()
            }
        };
        let gate = state
            .gates
            .lock()
            .unwrap()
            .get_mut(&path)
            .and_then(VecDeque::pop_front);
        if let Some(gate) = gate {
            let _permit = gate.acquire().await.unwrap();
        }
        response
    }

    fn ledger_path(issuer: &str) -> String {
        if issuer == V1 {
            format!(
                "contracts/{issuer}/bigmaps/ledger/historical_keys/123?key.nat=29437&active=true&limit=2"
            )
        } else {
            format!("contracts/{issuer}/bigmaps/ledger/historical_keys/123/29437")
        }
    }
    fn project(issuer: &str) -> Project {
        Project {
            id: 29437,
            title: "Test".into(),
            artists: vec![],
            description: None,
            preview: None,
            contract: None,
            issuer: Some(issuer.into()),
            artifact_uri: None,
            editions: None,
            first_mint: None,
            last_mint: None,
            index_state: crate::model::IndexState::Indexed,
            slug: None,
            supply: None,
            thumbnail_uri: None,
            display_uri: None,
            source: crate::model::Source::Fixture,
        }
    }

    #[test]
    fn utc_schedule_parser_handles_leap_centuries_and_fractional_opening_times() {
        assert_eq!(utc_nanos("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            utc_nanos("2000-02-29T12:00:00.123Z"),
            Some(951825600123000000)
        );
        assert_eq!(
            utc_nanos("2100-03-01T00:00:00Z").unwrap() - utc_nanos("2100-02-28T00:00:00Z").unwrap(),
            86_400_000_000_000
        );
        assert!(utc_nanos("2026-10-05T10:00:00.101Z") > utc_nanos(TIME));
        for value in [
            "2100-02-29T00:00:00Z",
            "2026-04-31T00:00:00Z",
            "2026-00-01T00:00:00Z",
            "2026-01-00T00:00:00Z",
            "2026-01-01T24:00:00Z",
            "2026-01-01T00:60:00Z",
            "2026-01-01T00:00:60Z",
            "2026-01-01T00:00:00+01:00",
            "2026-01-01T00:00:00.Z",
            "2026-1-01T00:00:00Z",
            "2026-01-01T0:00:00Z",
            "2026-01-01T00:00:00.1234567890Z",
            "２０２６-01-01T00:00:00Z",
        ] {
            assert!(utc_nanos(value).is_none(), "{value}");
        }
    }
    #[test]
    fn request_budget_refills_without_losing_fractional_time_or_exceeding_burst() {
        let now = Instant::now();
        let mut budget = RequestBudget {
            remaining: 0,
            updated: now,
        };
        assert!(!budget.take(now + Duration::from_millis(999)));
        assert!(budget.take(now + Duration::from_millis(1500)));
        assert!(!budget.take(now + Duration::from_millis(1999)));
        assert!(budget.take(now + Duration::from_secs(2)));
        assert!(budget.take(now + Duration::from_secs(1000)));
        assert_eq!(budget.remaining, REQUEST_BURST - 1);
    }
    #[tokio::test]
    async fn exhausted_upstream_budget_preserves_cache_and_recovers_without_negative_entries() {
        let mock = Mock::new(V3).await;
        let client = ReleaseClient::new(&mock.base).unwrap();
        assert!(client.get(&project(V3)).await.is_some());
        let before = mock.count();
        {
            let mut budget = client.budget.lock().unwrap();
            budget.remaining = 0;
            budget.updated = Instant::now() + Duration::from_secs(1);
        }
        assert!(client.get(&project(V3)).await.is_some());
        assert_eq!(mock.count(), before);
        client.cache.lock().unwrap().clear();
        assert!(client.get(&project(V3)).await.is_none());
        assert_eq!(mock.count(), before);
        assert!(client.cache.lock().unwrap().is_empty());
        client.budget.lock().unwrap().updated = Instant::now() - Duration::from_secs(8);
        assert!(client.get(&project(V3)).await.is_some());
        assert_eq!(mock.count(), before * 2);
    }
    #[test]
    fn api_base_preserves_configured_proxy_prefix_without_credentials_or_query() {
        for (input, expected) in [
            ("https://example.com", "https://example.com/v1/"),
            (
                "https://example.com/proxy/v1/",
                "https://example.com/proxy/v1/",
            ),
            ("https://example.com/proxy", "https://example.com/proxy/v1/"),
        ] {
            assert_eq!(ReleaseClient::new(input).unwrap().base.as_str(), expected);
        }
        for input in [
            "ftp://example.com",
            "https://name@example.com",
            "https://example.com/?key=secret",
            "https://example.com/#fragment",
        ] {
            assert!(ReleaseClient::new(input).is_err());
        }
    }
    #[tokio::test]
    async fn current_release_is_pinned_authorized_and_cached_with_finite_capacity() {
        let mock = Mock::new(V3).await;
        let client = ReleaseClient::new(&mock.base).unwrap();
        let release = client.get(&project(V3)).await.unwrap();
        assert_eq!(release.availability, Availability::Open);
        assert_eq!(release.pricing, Pricing::Fixed(20_000_000));
        assert_eq!(
            (release.remaining, release.supply, release.level),
            (310, 320, 123)
        );
        let count = mock.count();
        assert_eq!(count, 7);
        assert!(client.get(&project(V3)).await.is_some());
        assert_eq!(mock.count(), count);
        assert!(
            mock.state
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|path| !path.contains("/keys/") && !path.contains("/keys?"))
        );
        {
            let mut cache = client.cache.lock().unwrap();
            cache.clear();
            for id in 0..MAX_CACHE {
                cache.insert(
                    (V3.into(), id as u64),
                    Cached {
                        generation: 0,
                        inserted: Instant::now(),
                        release: release.clone(),
                    },
                );
            }
        }
        assert!(client.get(&project(V3)).await.is_some());
        assert_eq!(client.cache.lock().unwrap().len(), MAX_CACHE);
        client
            .cache
            .lock()
            .unwrap()
            .get_mut(&(V3.into(), 29437))
            .unwrap()
            .inserted = Instant::now() - TTL;
        assert!(client.get(&project(V3)).await.is_some());
        assert_eq!(mock.count(), count * 3);
    }
    #[tokio::test]
    async fn expired_higher_block_is_unavailable_instead_of_returned_or_replaced_by_lower_head() {
        let mock = Mock::new(V3).await;
        let client = ReleaseClient::new(&mock.base).unwrap();
        assert!(client.get(&project(V3)).await.is_some());
        let expired = Instant::now() - TTL;
        {
            let mut cache = client.cache.lock().unwrap();
            let entry = cache.get_mut(&(V3.into(), 29437)).unwrap();
            entry.release.level = 124;
            entry.inserted = expired;
        }
        for _ in 0..2 {
            assert!(client.get(&project(V3)).await.is_none());
            let cache = client.cache.lock().unwrap();
            let entry = &cache[&(V3.into(), 29437)];
            assert_eq!(entry.release.level, 124);
            assert_eq!(entry.inserted, expired);
        }
    }

    #[tokio::test]
    async fn older_completed_reads_cannot_replace_newer_state_or_renew_cache_age() {
        for next_level in [123, 124] {
            let mock = Mock::new(V3).await;
            let client = ReleaseClient::new(&mock.base).unwrap();
            let gate = Arc::new(Semaphore::new(0));
            mock.state.gates.lock().unwrap().insert(
                "/proxy/v1/blocks/123".into(),
                VecDeque::from([gate.clone()]),
            );
            let slow = {
                let client = client.clone();
                tokio::spawn(async move { client.get(&project(V3)).await })
            };
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if mock
                        .state
                        .gates
                        .lock()
                        .unwrap()
                        .get("/proxy/v1/blocks/123")
                        .is_some_and(VecDeque::is_empty)
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            // The old response is already captured. A later read sees a newer block or reorg.
            {
                let mut replies = mock.state.replies.lock().unwrap();
                let previous = replies.clone();
                for (path, values) in previous {
                    let path = path.replace("123", &next_level.to_string());
                    let values = values
                        .into_iter()
                        .map(|(status, body)| {
                            (
                                status,
                                body.replace("Bsample", "Bnew")
                                    .replace("\"level\":123", &format!("\"level\":{next_level}"))
                                    .replace("20000000", "40000000"),
                            )
                        })
                        .collect();
                    replies.insert(path, values);
                }
            }
            let newer = client.get(&project(V3)).await.unwrap();
            assert_eq!(newer.level, next_level);
            assert_eq!(newer.pricing, Pricing::Fixed(40_000_000));
            let inserted = client.cache.lock().unwrap()[&(V3.into(), 29437)].inserted;
            gate.add_permits(1);
            let late = slow.await.unwrap().unwrap();
            assert_eq!(late.level, next_level);
            assert_eq!(late.pricing, Pricing::Fixed(40_000_000));
            let cached = client.cache.lock().unwrap();
            assert_eq!(cached[&(V3.into(), 29437)].inserted, inserted);
            assert_eq!(cached[&(V3.into(), 29437)].release.level, next_level);
        }
    }

    #[tokio::test]
    async fn both_v1_ledger_shapes_work_but_ambiguous_pair_keys_do_not() {
        for issuer in [V1, V1_NEXT, V2] {
            let mock = Mock::new(issuer).await;
            let client = ReleaseClient::new(&mock.base).unwrap();
            assert_eq!(
                client.get(&project(issuer)).await.unwrap().pricing,
                Pricing::Fixed(20_000_000)
            );
            if issuer == V1 {
                client.cache.lock().unwrap().clear();
                mock.change(&ledger_path(issuer), |value| {
                    let row = value[0].clone();
                    value.as_array_mut().unwrap().push(row);
                });
                assert!(client.get(&project(issuer)).await.is_none());
                assert!(client.cache.lock().unwrap().is_empty());
            }
        }
    }
    #[tokio::test]
    async fn closed_scheduled_and_restricted_states_do_not_claim_open() {
        let mock = Mock::new(V3).await;
        let client = ReleaseClient::new(&mock.base).unwrap();
        let ledger = ledger_path(V3);
        let storage = format!("contracts/{V3}/storage?level=123");
        for (field, value, expected) in [
            ("enabled", json!(false), Availability::Disabled),
            ("balance", json!("0"), Availability::SoldOut),
            ("has_tickets", json!(true), Availability::Restricted),
            (
                "open_editions",
                json!({"ends_at":TIME}),
                Availability::Restricted,
            ),
            (
                "reserves",
                json!([{"amount":"310"}]),
                Availability::Restricted,
            ),
        ] {
            let previous = {
                let replies = mock.state.replies.lock().unwrap();
                serde_json::from_str::<Value>(&replies[&format!("/proxy/v1/{ledger}")][0].1)
                    .unwrap()["value"][field]
                    .clone()
            };
            mock.change(&ledger, |row| row["value"][field] = value);
            assert_eq!(
                client.get(&project(V3)).await.unwrap().availability,
                expected
            );
            client.cache.lock().unwrap().clear();
            mock.change(&ledger, |row| row["value"][field] = previous);
        }
        mock.change(&storage, |value| value["paused"] = json!(true));
        assert_eq!(
            client.get(&project(V3)).await.unwrap().availability,
            Availability::Paused
        );
        client.cache.lock().unwrap().clear();
        mock.change(&storage, |value| value["paused"] = json!(false));
        mock.change(
            &format!("contracts/{FIXED}/bigmaps/pricings/historical_keys/123/29437"),
            |row| row["value"]["opens_at"] = json!("2026-10-05T10:00:00.101Z"),
        );
        assert_eq!(
            client.get(&project(V3)).await.unwrap().availability,
            Availability::Scheduled
        );
        client.cache.lock().unwrap().clear();
        mock.change(&ledger, |row| row["value"]["pricing_id"] = json!("1"));
        assert_eq!(
            client.get(&project(V3)).await.unwrap().pricing,
            Pricing::Dutch
        );
    }
    #[tokio::test]
    async fn uncertain_or_reorganized_data_is_not_cached_and_a_later_read_recovers() {
        for case in [
            "paused",
            "price",
            "authorization",
            "missing_date",
            "wrong_key",
            "reorg",
            "oversize",
            "500",
        ] {
            let mock = Mock::new(V3).await;
            let client = ReleaseClient::new(&mock.base).unwrap();
            let original = mock.state.replies.lock().unwrap().clone();
            match case {
                "paused" => mock.change(&format!("contracts/{V3}/storage?level=123"), |v| {
                    v["paused"] = json!(null)
                }),
                "price" => mock.change(&format!("contracts/{V3}/storage?level=123"), |v| {
                    v["pricing_contracts"]["0"]["address"] = json!("KT1Unknown")
                }),
                "authorization" => mock
                    .change(&format!("contracts/{FIXED}/storage?level=123"), |v| {
                        v["authorized"] = json!({})
                    }),
                "missing_date" => mock.change(
                    &format!("contracts/{FIXED}/bigmaps/pricings/historical_keys/123/29437"),
                    |v| {
                        v["value"].as_object_mut().unwrap().remove("opens_at");
                    },
                ),
                "wrong_key" => mock.change(&ledger_path(V3), |v| v["key"] = json!("1")),
                "reorg" => mock.change("blocks/123", |v| v["hash"] = json!("Bdifferent")),
                "oversize" => mock.reply("head", vec![(StatusCode::OK, "x".repeat(MAX_BODY + 1))]),
                _ => mock.reply(
                    "head",
                    vec![(StatusCode::INTERNAL_SERVER_ERROR, "temporary".into())],
                ),
            }
            assert!(client.get(&project(V3)).await.is_none(), "{case}");
            assert!(client.cache.lock().unwrap().is_empty());
            *mock.state.replies.lock().unwrap() = original;
            assert!(client.get(&project(V3)).await.is_some(), "{case}");
        }
    }
    #[tokio::test]
    async fn timeout_cancellation_and_admission_release_slots_without_poisoning_cache() {
        let mock = Mock::new(V3).await;
        mock.state.delay_ms.store(200, Ordering::Relaxed);
        let client = ReleaseClient::new(&mock.base).unwrap();
        let mut tasks = vec![];
        for _ in 0..4 {
            let client = client.clone();
            tasks.push(tokio::spawn(async move { client.get(&project(V3)).await }));
        }
        for _ in 0..100 {
            if mock.count() == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(mock.count(), 4);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), client.get(&project(V3)))
                .await
                .unwrap()
                .is_none()
        );
        for task in tasks {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        assert_eq!(client.permits.available_permits(), 4);
        assert!(client.cache.lock().unwrap().is_empty());
        let mut short = client.clone();
        short.timeout = Duration::from_millis(10);
        assert!(short.get(&project(V3)).await.is_none());
        assert_eq!(client.permits.available_permits(), 4);
        assert!(client.cache.lock().unwrap().is_empty());
        mock.state.delay_ms.store(0, Ordering::Relaxed);
        assert!(client.get(&project(V3)).await.is_some());
    }
}
