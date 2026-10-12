//! Price history of fxhash tokens from Tezos chain data (TzKT): primary mints and secondary
//! sales, prices in mutez.
//!
//! The data lives in its own SQLite file (`var/sales.sqlite`, [`schema::SCHEMA_VERSION`]); the
//! web attaches it read-only to the archive connection ([`store::attach`]).
//!
//! - [`markets`]: marketplace contracts and how each recognizes a sale (see `MARKETS.md`).
//! - [`chain`]: validated TzKT rows and page fetchers.
//! - [`interpret`]: operation groups to sales, pure apart from escrow bookkeeping.
//! - [`indexer`]: resumable backfill and incremental sync, one cursor per stream.
//! - [`store`]: read API for the web.

pub mod chain;
pub mod indexer;
pub mod interpret;
pub mod markets;
pub mod schema;
pub mod store;

use std::fmt;

use crate::ingest::http::ApiError;

pub use store::{ProjectMintSummary, ProjectSalesSummary, Sale};

/// What kind of trade a row records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SaleKind {
    /// Primary: the mint call on an fxhash issuer.
    Mint,
    /// A listing bought at its price.
    Sale,
    /// An offer (token or collection) accepted by the holder.
    OfferAccept,
    /// An auction settled to the winning bidder (English), or bought (Dutch).
    Auction,
    /// A paid token move by a marketplace entrypoint without a known rule.
    Other,
}

impl SaleKind {
    pub const ALL: [SaleKind; 5] = [
        SaleKind::Mint,
        SaleKind::Sale,
        SaleKind::OfferAccept,
        SaleKind::Auction,
        SaleKind::Other,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            SaleKind::Mint => "mint",
            SaleKind::Sale => "sale",
            SaleKind::OfferAccept => "offer_accept",
            SaleKind::Auction => "auction",
            SaleKind::Other => "other",
        }
    }

    pub fn parse(s: &str) -> Option<SaleKind> {
        SaleKind::ALL.into_iter().find(|k| k.as_str() == s)
    }
}

impl fmt::Display for SaleKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Currency of a price: tez, objkt's wrapped tez (1:1, 6 decimals) or another token contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Currency {
    Tez,
    /// wXTZ by objkt.com (FA1.2), redeemable 1:1 for tez.
    Wxtz,
    /// Any other token contract; such prices are not mutez and stay NULL.
    Token(String),
}

impl Currency {
    pub fn as_str(&self) -> &str {
        match self {
            Currency::Tez => "tez",
            Currency::Wxtz => "wxtz",
            Currency::Token(c) => c,
        }
    }

    /// Whether a price in this currency is a tez amount in mutez.
    pub fn is_tez_like(&self) -> bool {
        matches!(self, Currency::Tez | Currency::Wxtz)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SalesError {
    #[error(transparent)]
    Api(#[from] ApiError),
    #[error("sales database: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("sales database unavailable: {0}")]
    Unavailable(String),
    #[error("sales database schema: {0}")]
    Schema(String),
    #[error("io: {0}")]
    Io(String),
    #[error("{0}")]
    Usage(String),
}

impl SalesError {
    /// Transient failures leave every cursor where it was; a later run resumes unchanged.
    pub fn is_transient(&self) -> bool {
        match self {
            SalesError::Api(ApiError::Transient(_)) => true,
            SalesError::Db(e) => matches!(
                e.sqlite_error_code(),
                Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
            ),
            _ => false,
        }
    }

    pub fn is_interrupted(&self) -> bool {
        matches!(self, SalesError::Api(ApiError::Interrupted))
    }
}

/// A permanent error for malformed upstream data.
pub(crate) fn bad(what: &str, detail: impl fmt::Display) -> SalesError {
    SalesError::Api(ApiError::Permanent(format!(
        "malformed TzKT {what}: {detail}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_round_trip() {
        for k in SaleKind::ALL {
            assert_eq!(SaleKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(SaleKind::parse("gift"), None);
    }

    #[test]
    fn busy_database_is_transient_and_bad_data_is_not() {
        let busy = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            None,
        );
        assert!(SalesError::Db(busy).is_transient());
        assert!(!bad("op", "x").is_transient());
        assert!(SalesError::Api(ApiError::Transient("503".into())).is_transient());
        assert!(SalesError::Api(ApiError::Interrupted).is_interrupted());
    }
}
