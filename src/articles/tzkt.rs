//! One chain snapshot of fx(text) from TzKT API v1.
//!
//! Lists page by TzKT `id` (`id.gt`); big-map keys page by `offset.cr` (they ignore `id.gt`).
//! Every page is checked: at most `limit` rows, ids strictly increasing. Totals are capped so a
//! misbehaving API cannot grow memory without bound.

use serde::Deserialize;
use serde_json::Value;

use super::http::{ApiError, JsonClient};
use super::{ARTICLES_CONTRACT, MARKETPLACE_V3, MODERATION_CONTRACT};

/// TzKT's largest page.
pub const MAX_PAGE: usize = 10_000;
/// Largest plausible row counts; more is a permanent error.
const MAX_TOKENS: usize = 1_000_000;
const MAX_BALANCES: usize = 10_000_000;
const MAX_KEYS: usize = 1_000_000;

/// An fx(text) token as TzKT indexes it.
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub token_id: u64,
    pub first_level: u64,
    pub first_time: String,
    /// Minted minus burned.
    pub supply: i64,
    pub minted: i64,
    pub burned: i64,
    /// TzKT's resolved metadata JSON object, when it has one.
    pub metadata: Option<Value>,
}

/// `token_metadata[token_id]`: the metadata pointer (`token_info[""]`, hex bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataPointer {
    pub token_id: u64,
    /// UTF-8 text of `token_info[""]`; `None` when missing or not UTF-8.
    pub uri: Option<String>,
    pub last_level: u64,
    /// Big-map updates of this key: 1 + metadata revisions.
    pub updates: u64,
}

/// `token_data[token_id]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenData {
    pub token_id: u64,
    pub minter: String,
    /// Per mille.
    pub royalties: Option<i64>,
    pub metadata_locked: bool,
    /// `(address, pct per mille of royalties)`, in contract order.
    pub splits: Vec<(String, i64)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Balance {
    pub id: u64,
    pub token_id: u64,
    pub address: String,
    pub amount: i64,
    pub last_level: u64,
}

/// An active marketplace v3 listing of an article (editions held in escrow).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listing {
    pub listing_id: u64,
    pub token_id: u64,
    pub seller: String,
    pub amount: i64,
    /// Price per edition in mutez; `None` unless the currency is tez.
    pub price_mutez: Option<i64>,
    pub last_level: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Moderation {
    pub token_id: u64,
    pub state: i64,
    pub reason_id: Option<i64>,
    pub last_level: u64,
}

/// A moderation reason: `(id, text)`.
pub type Reason = (i64, String);

/// Everything the sync needs from chain, read at about one level.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ChainSnapshot {
    pub level: u64,
    pub tokens: Vec<Token>,
    pub pointers: Vec<MetadataPointer>,
    pub token_data: Vec<TokenData>,
    pub balances: Vec<Balance>,
    pub listings: Vec<Listing>,
    pub moderation: Vec<Moderation>,
    pub reasons: Vec<Reason>,
}

pub struct Tzkt<'a> {
    pub client: &'a JsonClient,
    /// Rows per page, at most [`MAX_PAGE`]; tests use small pages.
    pub page: usize,
}

fn bad(what: &str, detail: impl std::fmt::Display) -> ApiError {
    ApiError::Permanent(format!("malformed TzKT {what}: {detail}"))
}

/// A decimal id or level that fits SQLite's i64.
fn num(s: &str, what: &str) -> Result<u64, ApiError> {
    s.parse::<u64>()
        .ok()
        .filter(|n| i64::try_from(*n).is_ok())
        .ok_or_else(|| bad(what, format!("{s:?} is not a number below 2^63")))
}

fn fit(v: u64, what: &str) -> Result<u64, ApiError> {
    i64::try_from(v)
        .map(|_| v)
        .map_err(|_| bad(what, format!("{v} is not below 2^63")))
}

/// A nat amount; values above i64::MAX are stored as i64::MAX.
fn amount(s: &str, what: &str) -> Result<i64, ApiError> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad(what, format!("{s:?} is not a natural number")));
    }
    Ok(s.parse::<i64>().unwrap_or(i64::MAX))
}

fn text(v: &Value) -> Option<&str> {
    v.as_str()
}

fn decode_hex_utf8(v: &Value) -> Option<String> {
    let bytes = hex::decode(v.as_str()?).ok()?;
    String::from_utf8(bytes).ok()
}

/// Ids strictly increasing after `after`, at most `limit` rows.
fn check_page(what: &str, ids: &[u64], limit: usize, after: u64) -> Result<(), ApiError> {
    if ids.len() > limit {
        return Err(bad(
            what,
            format!("{} rows for a page of {limit}", ids.len()),
        ));
    }
    let mut last = after;
    for id in ids {
        if *id <= last {
            return Err(bad(what, format!("ids repeat or go backwards at {id}")));
        }
        last = *id;
    }
    Ok(())
}

#[derive(Deserialize)]
struct RawToken {
    id: u64,
    #[serde(rename = "tokenId")]
    token_id: String,
    #[serde(rename = "firstLevel")]
    first_level: u64,
    #[serde(rename = "firstTime")]
    first_time: String,
    #[serde(rename = "totalSupply")]
    total_supply: String,
    #[serde(rename = "totalMinted")]
    total_minted: String,
    #[serde(rename = "totalBurned")]
    total_burned: String,
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Deserialize)]
struct RawKey {
    id: u64,
    key: Value,
    #[serde(default)]
    value: Value,
    #[serde(rename = "lastLevel")]
    last_level: u64,
    #[serde(default)]
    updates: u64,
}

#[derive(Deserialize)]
struct RawBalance {
    id: u64,
    #[serde(rename = "token.tokenId")]
    token_id: String,
    #[serde(rename = "account.address")]
    address: String,
    balance: String,
    #[serde(rename = "lastLevel")]
    last_level: u64,
}

const TOKEN_SELECT: &str =
    "id,tokenId,firstLevel,firstTime,totalSupply,totalMinted,totalBurned,metadata";
const BALANCE_SELECT: &str = "id,token.tokenId,account.address,balance,lastLevel";

impl Tzkt<'_> {
    fn list<R: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<Vec<R>, ApiError> {
        self.client
            .get_json(path)?
            .ok_or_else(|| ApiError::Permanent(format!("{path}: list endpoint is absent")))
    }

    /// Pages a TzKT list by `id.gt`.
    fn paged<R, F>(&self, path: &str, cap: usize, id: F) -> Result<Vec<R>, ApiError>
    where
        R: for<'de> Deserialize<'de>,
        F: Fn(&R) -> u64,
    {
        let page = self.page.clamp(1, MAX_PAGE);
        let mut out: Vec<R> = Vec::new();
        let mut last = 0u64;
        loop {
            let rows: Vec<R> =
                self.list(&format!("{path}&sort.asc=id&id.gt={last}&limit={page}"))?;
            let ids: Vec<u64> = rows.iter().map(&id).collect();
            check_page(path, &ids, page, last)?;
            let n = rows.len();
            if let Some(l) = ids.last() {
                last = *l;
            }
            out.extend(rows);
            if out.len() > cap {
                return Err(bad(path, format!("more than {cap} rows")));
            }
            if n < page {
                return Ok(out);
            }
        }
    }

    /// Pages active big-map keys by `offset.cr`. No `select`: TzKT returns a null `id` with it.
    fn keys(&self, contract: &str, map: &str, filter: &str) -> Result<Vec<RawKey>, ApiError> {
        let page = self.page.clamp(1, MAX_PAGE);
        let path = format!("/contracts/{contract}/bigmaps/{map}/keys?active=true{filter}");
        let mut out: Vec<RawKey> = Vec::new();
        let mut last = 0u64;
        loop {
            let rows: Vec<RawKey> =
                self.list(&format!("{path}&sort.asc=id&offset.cr={last}&limit={page}"))?;
            let ids: Vec<u64> = rows.iter().map(|r| r.id).collect();
            check_page(&path, &ids, page, last)?;
            let n = rows.len();
            if let Some(l) = ids.last() {
                last = *l;
            }
            out.extend(rows);
            if out.len() > MAX_KEYS {
                return Err(bad(&path, format!("more than {MAX_KEYS} keys")));
            }
            if n < page {
                return Ok(out);
            }
        }
    }

    pub fn head_level(&self) -> Result<u64, ApiError> {
        #[derive(Deserialize)]
        struct Head {
            level: u64,
        }
        let head: Head = self
            .client
            .get_json("/head")?
            .ok_or_else(|| ApiError::Permanent("/head is absent".into()))?;
        fit(head.level, "head level")
    }

    pub fn tokens(&self) -> Result<Vec<Token>, ApiError> {
        let raw: Vec<RawToken> = self.paged(
            &format!("/tokens?contract={ARTICLES_CONTRACT}&select={TOKEN_SELECT}"),
            MAX_TOKENS,
            |r: &RawToken| r.id,
        )?;
        raw.into_iter()
            .map(|r| {
                Ok(Token {
                    token_id: num(&r.token_id, "token id")?,
                    first_level: fit(r.first_level, "token firstLevel")?,
                    first_time: r.first_time,
                    supply: amount(&r.total_supply, "totalSupply")?,
                    minted: amount(&r.total_minted, "totalMinted")?,
                    burned: amount(&r.total_burned, "totalBurned")?,
                    metadata: r.metadata.filter(Value::is_object),
                })
            })
            .collect()
    }

    pub fn pointers(&self) -> Result<Vec<MetadataPointer>, ApiError> {
        self.keys(ARTICLES_CONTRACT, "token_metadata", "")?
            .into_iter()
            .map(|r| {
                let token_id = num(text(&r.key).unwrap_or_default(), "token_metadata key")?;
                Ok(MetadataPointer {
                    token_id,
                    uri: decode_hex_utf8(&r.value["token_info"][""]),
                    last_level: fit(r.last_level, "token_metadata lastLevel")?,
                    updates: r.updates,
                })
            })
            .collect()
    }

    pub fn token_data(&self) -> Result<Vec<TokenData>, ApiError> {
        self.keys(ARTICLES_CONTRACT, "token_data", "")?
            .into_iter()
            .map(|r| {
                let token_id = num(text(&r.key).unwrap_or_default(), "token_data key")?;
                let v = &r.value;
                let minter = text(&v["minter"])
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| bad("token_data", format!("token {token_id} lacks minter")))?
                    .to_owned();
                let splits = v["royalties_split"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|s| {
                                let address = text(&s["address"])?.to_owned();
                                let pct = text(&s["pct"])?.parse::<i64>().ok()?;
                                Some((address, pct))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(TokenData {
                    token_id,
                    minter,
                    royalties: text(&v["royalties"]).and_then(|s| s.parse().ok()),
                    metadata_locked: v["metadata_locked"].as_bool().unwrap_or(false),
                    splits,
                })
            })
            .collect()
    }

    /// Positive ledger balances.
    pub fn balances(&self) -> Result<Vec<Balance>, ApiError> {
        let raw: Vec<RawBalance> = self.paged(
            &format!(
                "/tokens/balances?token.contract={ARTICLES_CONTRACT}&balance.gt=0\
                 &select={BALANCE_SELECT}"
            ),
            MAX_BALANCES,
            |r: &RawBalance| r.id,
        )?;
        let mut out = Vec::with_capacity(raw.len());
        for r in raw {
            let amount = amount(&r.balance, "balance")?;
            if amount > 0 {
                out.push(Balance {
                    id: fit(r.id, "balance id")?,
                    token_id: num(&r.token_id, "balance token")?,
                    address: r.address,
                    amount,
                    last_level: fit(r.last_level, "balance lastLevel")?,
                });
            }
        }
        Ok(out)
    }

    /// Active marketplace v3 listings of fx(text) tokens.
    pub fn listings(&self) -> Result<Vec<Listing>, ApiError> {
        let contracts = self.keys(MARKETPLACE_V3, "asset_contracts", "")?;
        let Some(id) = contracts
            .iter()
            .find(|r| text(&r.value["fa2"]) == Some(ARTICLES_CONTRACT))
            .and_then(|r| text(&r.key).map(str::to_owned))
        else {
            return Ok(Vec::new());
        };
        let id = num(&id, "asset_contracts key")?;
        let mut out = Vec::new();
        for r in self.keys(
            MARKETPLACE_V3,
            "listings",
            &format!("&value.asset.contract_id={id}"),
        )? {
            let v = &r.value;
            let listing_id = num(text(&r.key).unwrap_or_default(), "listing key")?;
            if text(&v["asset"]["contract_id"]).map(|s| num(s, "contract_id")) != Some(Ok(id)) {
                return Err(bad(
                    "listing",
                    format!("{listing_id} is for another contract"),
                ));
            }
            let tez = v["currency"].get("tez").is_some();
            out.push(Listing {
                listing_id,
                token_id: num(
                    text(&v["asset"]["asset_id"]).unwrap_or_default(),
                    "asset_id",
                )?,
                seller: text(&v["seller"])
                    .ok_or_else(|| bad("listing", format!("{listing_id} lacks seller")))?
                    .to_owned(),
                amount: amount(text(&v["asset"]["amount"]).unwrap_or_default(), "listed")?,
                price_mutez: if tez {
                    text(&v["amount"]).and_then(|s| s.parse().ok())
                } else {
                    None
                },
                last_level: fit(r.last_level, "listing lastLevel")?,
            });
        }
        Ok(out)
    }

    pub fn moderation(&self) -> Result<(Vec<Moderation>, Vec<Reason>), ApiError> {
        let flags = self
            .keys(MODERATION_CONTRACT, "tokens", "")?
            .into_iter()
            .map(|r| {
                Ok(Moderation {
                    token_id: num(text(&r.key).unwrap_or_default(), "moderation key")?,
                    state: text(&r.value["state"])
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| bad("moderation", "state is not a number"))?,
                    reason_id: text(&r.value["reason"]).and_then(|s| s.parse().ok()),
                    last_level: fit(r.last_level, "moderation lastLevel")?,
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        let reasons = self
            .keys(MODERATION_CONTRACT, "reasons", "")?
            .into_iter()
            .filter_map(|r| {
                let id = text(&r.key)?.parse::<i64>().ok()?;
                Some((id, text(&r.value)?.to_owned()))
            })
            .collect();
        Ok((flags, reasons))
    }

    /// The whole snapshot. Head level first, so every row is at or after it.
    pub fn snapshot(&self) -> Result<ChainSnapshot, ApiError> {
        let level = self.head_level()?;
        let tokens = self.tokens()?;
        let pointers = self.pointers()?;
        let token_data = self.token_data()?;
        let balances = self.balances()?;
        let listings = self.listings()?;
        let (moderation, reasons) = self.moderation()?;
        Ok(ChainSnapshot {
            level,
            tokens,
            pointers,
            token_data,
            balances,
            listings,
            moderation,
            reasons,
        })
    }
}
