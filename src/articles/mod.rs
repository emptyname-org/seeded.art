//! fx(text) articles: chain facts from TzKT, metadata and bodies from IPFS, stored locally in
//! their own SQLite file (`var/articles.sqlite` by default), separate from the catalogue.
//!
//! - [`tzkt`]: one chain snapshot (tokens, token metadata pointers, token data, ledger
//!   balances, marketplace v3 listings, moderation).
//! - [`ipfs`]: content-addressed fetch with gateway fallback and CID verification.
//! - [`parse`]: metadata JSON and the media an article body embeds.
//! - [`refs`]: references from bodies to fxhash projects and tokens, resolved against the
//!   catalogue.
//! - [`names`]: the author names the site displays, copied from the catalogue for search.
//! - [`sync`]: chain refresh plus resumable, incremental IPFS fetching.
//! - [`store`]: schema and the read API for the web.
//!
//! Bodies are raw fx-markdown exactly as published: untrusted input. The web must render them
//! with a sanitizing renderer (no raw HTML, no scripts or event handlers, allow-listed URL
//! schemes, media through the content origin only).

pub mod http;
pub mod ipfs;
pub mod names;
pub mod parse;
pub mod refs;
pub mod store;
pub mod sync;
pub mod tzkt;

#[cfg(test)]
mod tests;

/// fx(text) FA2 contract (`NEXT_PUBLIC_TZ_CT_ADDRESS_ARTICLES` in fxhash-website `.env`).
pub const ARTICLES_CONTRACT: &str = "KT1GtbuswcNMGhHF2TSuH1Yfaqn16do8Qtva";
/// Article moderation contract (`NEXT_PUBLIC_TZ_CT_ADDRESS_ARTICLE_MODERATION`).
pub const MODERATION_CONTRACT: &str = "KT1A36z7nG4zPDbhjyrzhYf9SCn5ipPZeRMQ";
/// fxhash marketplace v3: holds listed article editions in escrow.
pub const MARKETPLACE_V3: &str = "KT1M1NyU9X4usEimt2f3kDaijZnDMNBu42Ja";
/// Production database path, relative to the deployment root.
pub const DEFAULT_DB_PATH: &str = "var/articles.sqlite";
pub const USER_AGENT: &str = "seeded.art-archive-dev";

/// Seconds since the Unix epoch.
pub(crate) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Peak resident set size of this process, from `/proc/self/status`.
pub fn peak_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
}
