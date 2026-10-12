//! Catalogue-scale ingestion.
//!
//! - [`frank`]: bootstrap from Frank Force's fxhash archive (KilledByAPixel/fxhashArchive).
//! - [`sync`]: tokens, mints and ledger owners from TzKT, with resumable cursors.
//! - [`escrow`]: sellers of tokens listed in marketplace escrow contracts.
//! - [`offers`]: fxhash buy offers and block times of listings and offers.
//! - [`verify`]: catalogue records against chain.
//! - [`identity`]: curated corrections of who an address is.
//! - [`profiles`]: names, bios, links and avatar URIs from the fxhash registry and objkt.
//! - [`upgrade`]: local schema upgrade and rebuild, no network.
//! - [`mirror`]: preservation mirror of every visible project's generator package.
//! - [`stats`]: operational counts and sizes.
//!
//! Bootstrap never writes `tokens`; sync never writes catalogue facts. Disagreements are
//! recorded (`catalogue_tokens.verify_state`, `ingest_issues`), never overwritten.

pub mod escrow;
pub mod frank;
pub mod http;
pub mod identity;
pub mod mirror;
pub mod offers;
pub mod profiles;
pub mod stats;
pub mod sync;
pub mod tzkt;
pub mod upgrade;
pub mod verify;

use std::time::{SystemTime, UNIX_EPOCH};

use crate::store::StoreError;

pub use crate::store::TOKEN_CONTRACTS;

pub const GENTK_V1: &str = TOKEN_CONTRACTS[0];
pub const GENTK_V2: &str = TOKEN_CONTRACTS[1];
pub const FX_PARAMS: &str = TOKEN_CONTRACTS[2];

/// Issuer contracts and the token contract they mint on. Verified 2026-10-03 against TzKT:
/// every applied `mint` on each token contract came from these senders (v1: 282,139 from the
/// old issuer + 307,007 from the second; v2: 1,030,141; fx(params): 226,346).
pub const ISSUERS: [(&str, &str); 4] = [
    ("KT1AEVuykWeuuFX7QkEAMNtffzwhe1Z98hJS", GENTK_V1),
    ("KT1XCoGnfupWk7Sp8536EfrxcP73LmT68Nyr", GENTK_V1),
    ("KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv", GENTK_V2),
    ("KT1Xpmp15KfqoePNW9HczFmqaGNHwadV2a3b", FX_PARAMS),
];

pub const USER_AGENT: &str = "seeded.art-archive-dev";

/// Blocks below head after which a level is final (Tenderbake: 2). Escrow and offer passes
/// record this level and re-read anything stored from above it.
pub const FINALITY: u64 = 2;

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Api(#[from] http::ApiError),
    #[error("invalid source {file}: {reason}")]
    Source { file: String, reason: String },
    #[error("io: {0}")]
    Io(String),
    #[error("{0}")]
    Usage(String),
}

impl IngestError {
    /// Transient failures leave state resumable; a later run may succeed unchanged.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            IngestError::Api(http::ApiError::Transient(_))
                | IngestError::Store(StoreError::Unavailable(_))
        )
    }
}

pub(crate) fn source_err(file: impl Into<String>, reason: impl Into<String>) -> IngestError {
    IngestError::Source {
        file: file.into(),
        reason: reason.into(),
    }
}

pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Days since 1970-01-01 to (year, month, day), proleptic Gregorian (H. Hinnant).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// `YYYY-MM-DDTHH:MM:SSZ`.
pub(crate) fn iso_utc(secs: i64) -> String {
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let s = secs.rem_euclid(86_400);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        s / 3600,
        s % 3600 / 60,
        s % 60
    )
}

pub(crate) fn now_iso() -> String {
    iso_utc(unix_now())
}

/// Query parameters of an artifact URI (fragment ignored); first occurrence wins.
pub(crate) fn query_param<'a>(uri: &'a str, key: &str) -> Option<&'a str> {
    let uri = uri.split('#').next().unwrap_or(uri);
    let (_, query) = uri.split_once('?')?;
    query
        .split('&')
        .filter_map(|pair| pair.split_once('=').or(Some((pair, ""))))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
}

/// Peak resident set size of this process in KiB (Linux `VmHWM`), if available.
pub fn peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_utc_formats_known_instants() {
        assert_eq!(iso_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso_utc(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(iso_utc(1_791_043_199), "2026-10-03T15:59:59Z");
    }

    #[test]
    fn query_param_reads_first_value_and_ignores_fragment() {
        let uri = "onchfs://x/?fxhash=oo1&fxiteration=2&fxminter=tz1#0xab";
        assert_eq!(query_param(uri, "fxhash"), Some("oo1"));
        assert_eq!(query_param(uri, "fxiteration"), Some("2"));
        assert_eq!(query_param(uri, "fxminter"), Some("tz1"));
        assert_eq!(query_param("ipfs://x", "fxhash"), None);
        assert_eq!(
            query_param("ipfs://x?fxhash=a&fxhash=b", "fxhash"),
            Some("a")
        );
    }
}
