//! TzKT API v1 (https://api.tzkt.io): typed rows for fxhash tokens, mints and ledger balances.
//!
//! Lists are paged by TzKT's internal `id` (`sort.asc=id&id.gt=<last>`), the cursor TzKT
//! recommends. Token id ranges use numeric filters (`tokenId.ge/lt`,
//! `parameter.token_id.ge/lt`, `token.tokenId.ge/lt`).

use serde::Deserialize;
use serde_json::Value;

use super::http::{ApiError, Client};

/// TzKT's largest page.
pub const MAX_PAGE: usize = 10_000;
/// Ids per `.in=` filter, keeping URLs short.
pub const MAX_IN: usize = 100;

const TOKEN_SELECT: &str = "id,tokenId,firstTime,firstLevel,lastLevel,metadata.name,\
     metadata.artifactUri,metadata.displayUri,metadata.thumbnailUri,metadata.attributes,\
     metadata.iterationHash";
const MINT_SELECT: &str = "id,level,timestamp,sender,parameter";
const BALANCE_SELECT: &str = "id,token.tokenId,account.address,balance,lastLevel";

/// One applied `mint` call on a token contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mint {
    pub op_id: u64,
    pub level: u64,
    pub timestamp: String,
    /// Sender: the issuer contract.
    pub issuer: String,
    pub token_id: u64,
    /// `issuer_id`: the fxhash project id.
    pub project_id: u64,
    pub iteration: u32,
    /// `address`: the account the token was minted to.
    pub minter: String,
    /// fx(params) `input_bytes` as hex without `0x`; `None` when absent or empty.
    pub params: Option<String>,
}

/// A token as TzKT indexes it, with the metadata fields we keep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainToken {
    pub id: u64,
    pub token_id: u64,
    pub first_time: String,
    pub first_level: u64,
    pub last_level: u64,
    pub name: Option<String>,
    pub artifact_uri: Option<String>,
    pub display_uri: Option<String>,
    pub thumbnail_uri: Option<String>,
    /// `[name, value]` pairs in metadata order; first occurrence of a name wins.
    pub attributes: Vec<(String, String)>,
    pub iteration_hash: Option<String>,
}

impl ChainToken {
    /// The fxhash seed: `iterationHash`, else the artifact's `fxhash` query parameter.
    pub fn seed(&self) -> Option<&str> {
        self.iteration_hash
            .as_deref()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                self.artifact_uri
                    .as_deref()
                    .and_then(|u| super::query_param(u, "fxhash"))
                    .filter(|s| !s.is_empty())
            })
    }
}

/// One ledger balance row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Balance {
    pub id: u64,
    pub token_id: u64,
    pub address: String,
    pub positive: bool,
    pub last_level: u64,
}

/// One key of a contract big map, removed keys included (`active = false`).
#[derive(Debug, Clone, PartialEq)]
pub struct BigMapKey {
    pub id: u64,
    /// The key as text (JSON for structured keys).
    pub key: String,
    pub value: Value,
    pub active: bool,
    pub last_level: u64,
}

#[derive(Deserialize)]
struct RawBigMapKey {
    id: u64,
    key: Value,
    #[serde(default)]
    value: Value,
    active: bool,
    #[serde(rename = "lastLevel")]
    last_level: u64,
}

pub struct Tzkt {
    pub client: Client,
}

/// TzKT API base ending in `/v1`: accepts `https://api.tzkt.io` or `https://api.tzkt.io/v1`,
/// with or without a trailing slash. `None` unless http(s) with a host.
pub fn api_base(url: &str) -> Option<String> {
    let url = url.trim().trim_end_matches('/');
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    if rest.is_empty() || rest.starts_with('/') || url.contains(['?', '#', ' ']) {
        return None;
    }
    Some(if url.ends_with("/v1") {
        url.to_owned()
    } else {
        format!("{url}/v1")
    })
}

fn bad(what: &str, detail: impl std::fmt::Display) -> ApiError {
    ApiError::Permanent(format!("malformed TzKT {what}: {detail}"))
}

fn str_of(v: &Option<Value>) -> Option<String> {
    v.as_ref().and_then(Value::as_str).map(str::to_owned)
}

/// Ids and levels must fit SQLite's i64, so later casts never wrap.
fn fit(v: u64, what: &str) -> Result<u64, ApiError> {
    i64::try_from(v)
        .map(|_| v)
        .map_err(|_| bad(what, format!("{v} is not below 2^63")))
}

/// A decimal id or level that fits SQLite's i64.
fn num(s: &str, what: &str) -> Result<u64, ApiError> {
    s.parse::<u64>()
        .ok()
        .filter(|n| i64::try_from(*n).is_ok())
        .ok_or_else(|| bad(what, format!("{s:?} is not a number below 2^63")))
}

#[derive(Deserialize)]
struct RawToken {
    id: u64,
    #[serde(rename = "tokenId")]
    token_id: String,
    #[serde(rename = "firstTime")]
    first_time: String,
    #[serde(rename = "firstLevel")]
    first_level: u64,
    #[serde(rename = "lastLevel")]
    last_level: u64,
    #[serde(rename = "metadata.name", default)]
    name: Option<Value>,
    #[serde(rename = "metadata.artifactUri", default)]
    artifact_uri: Option<Value>,
    #[serde(rename = "metadata.displayUri", default)]
    display_uri: Option<Value>,
    #[serde(rename = "metadata.thumbnailUri", default)]
    thumbnail_uri: Option<Value>,
    #[serde(rename = "metadata.attributes", default)]
    attributes: Option<Value>,
    #[serde(rename = "metadata.iterationHash", default)]
    iteration_hash: Option<Value>,
}

/// Attribute values that are not strings keep their JSON text; nulls and unnamed entries drop.
fn attributes(v: Option<Value>) -> Vec<(String, String)> {
    let Some(Value::Array(items)) = v else {
        return Vec::new();
    };
    let mut out: Vec<(String, String)> = Vec::with_capacity(items.len());
    for item in items {
        let Some(name) = item.get("name").and_then(Value::as_str) else {
            continue;
        };
        let value = match item.get("value") {
            None | Some(Value::Null) => continue,
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
        };
        if !out.iter().any(|(n, _)| n == name) {
            out.push((name.to_owned(), value));
        }
    }
    out
}

impl TryFrom<RawToken> for ChainToken {
    type Error = ApiError;

    fn try_from(r: RawToken) -> Result<Self, ApiError> {
        Ok(ChainToken {
            id: fit(r.id, "token id")?,
            token_id: num(&r.token_id, "token")?,
            first_time: r.first_time,
            first_level: fit(r.first_level, "token firstLevel")?,
            last_level: fit(r.last_level, "token lastLevel")?,
            name: str_of(&r.name),
            artifact_uri: str_of(&r.artifact_uri),
            display_uri: str_of(&r.display_uri),
            thumbnail_uri: str_of(&r.thumbnail_uri),
            attributes: attributes(r.attributes),
            iteration_hash: str_of(&r.iteration_hash),
        })
    }
}

#[derive(Deserialize)]
struct RawMint {
    id: u64,
    level: u64,
    timestamp: String,
    sender: Value,
    parameter: Value,
}

impl TryFrom<RawMint> for Mint {
    type Error = ApiError;

    fn try_from(r: RawMint) -> Result<Self, ApiError> {
        let v = &r.parameter["value"];
        let field = |k: &str| {
            v.get(k)
                .and_then(Value::as_str)
                .ok_or_else(|| bad("mint", format!("op {} lacks parameter {k}", r.id)))
        };
        let iteration = num(field("iteration")?, "mint iteration")?;
        Ok(Mint {
            op_id: fit(r.id, "mint id")?,
            level: fit(r.level, "mint level")?,
            timestamp: r.timestamp,
            issuer: r.sender["address"]
                .as_str()
                .ok_or_else(|| bad("mint", format!("op {} lacks sender", r.id)))?
                .to_owned(),
            token_id: num(field("token_id")?, "mint token_id")?,
            project_id: num(field("issuer_id")?, "mint issuer_id")?,
            iteration: u32::try_from(iteration)
                .map_err(|_| bad("mint", format!("iteration {iteration}")))?,
            minter: field("address")?.to_owned(),
            params: v
                .get("input_bytes")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(|s| s.trim_start_matches("0x").to_owned()),
        })
    }
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

impl TryFrom<RawBalance> for Balance {
    type Error = ApiError;

    fn try_from(r: RawBalance) -> Result<Self, ApiError> {
        let digits = r.balance.trim_start_matches('0');
        if !r.balance.bytes().all(|b| b.is_ascii_digit()) || r.balance.is_empty() {
            return Err(bad("balance", format!("{:?}", r.balance)));
        }
        Ok(Balance {
            id: fit(r.id, "balance id")?,
            token_id: num(&r.token_id, "balance token")?,
            address: r.address,
            positive: !digits.is_empty(),
            last_level: fit(r.last_level, "balance lastLevel")?,
        })
    }
}

/// TzKT filter on a JSON field (transaction parameters, big-map keys and values). TzKT rejects `.in`
/// there with fewer than two items ("JSON array must contain at least two items"), so a single
/// value becomes an equality filter.
pub(crate) fn json_in<T: std::fmt::Display>(field: &str, values: &[T]) -> String {
    match values {
        [one] => format!("{field}={one}"),
        _ => format!(
            "{field}.in={}",
            values
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

fn ids(list: &[u64]) -> String {
    list.iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

impl Tzkt {
    pub fn new(client: Client) -> Tzkt {
        Tzkt { client }
    }

    /// Pages `path` (which already has its filters) by id until a short page, validating
    /// every page (see [`check_page`]) and stopping once more than `cap` rows arrived.
    fn paged<R, T>(
        &self,
        path: &str,
        page: usize,
        cap: usize,
        keep: impl Fn(&T) -> bool,
    ) -> Result<Vec<T>, ApiError>
    where
        R: for<'de> Deserialize<'de>,
        T: TryFrom<R, Error = ApiError> + HasId,
    {
        let mut out = Vec::new();
        let mut last = 0;
        loop {
            let rows: Vec<R> = self
                .client
                .get_json(&format!("{path}&sort.asc=id&id.gt={last}&limit={page}"))?
                .ok_or_else(|| ApiError::Permanent(format!("{path}: list endpoint is absent")))?;
            let rows: Vec<T> = rows
                .into_iter()
                .map(T::try_from)
                .collect::<Result<_, _>>()?;
            check_page(path, &rows, page, last, &keep)?;
            let n = rows.len();
            if let Some(t) = rows.last() {
                last = t.id();
            }
            out.extend(rows);
            if out.len() > cap {
                return Err(ApiError::Permanent(format!(
                    "{path}: more than {cap} rows for this filter"
                )));
            }
            if n < page {
                return Ok(out);
            }
        }
    }

    /// One validated page after `after`, for incremental cursors.
    fn page<R, T>(
        &self,
        path: &str,
        page: usize,
        after: u64,
        keep: impl Fn(&T) -> bool,
    ) -> Result<Vec<T>, ApiError>
    where
        R: for<'de> Deserialize<'de>,
        T: TryFrom<R, Error = ApiError> + HasId,
    {
        let rows: Vec<R> = self
            .client
            .get_json(&format!("{path}&sort.asc=id&id.gt={after}&limit={page}"))?
            .ok_or_else(|| ApiError::Permanent(format!("{path}: list endpoint is absent")))?;
        let rows: Vec<T> = rows
            .into_iter()
            .map(T::try_from)
            .collect::<Result<_, _>>()?;
        check_page(path, &rows, page, after, &keep)?;
        Ok(rows)
    }

    /// One page of `contract`'s big map `path` after key id `after`, with extra `filter`s.
    /// Big-map keys ignore `id.gt`; their id cursor is `offset.cr`.
    /// Rows `keep` rejects are a filter the API did not apply: a permanent error.
    pub fn bigmap_keys(
        &self,
        contract: &str,
        path: &str,
        filter: &str,
        after: u64,
        page: usize,
        keep: impl Fn(&BigMapKey) -> bool,
    ) -> Result<Vec<BigMapKey>, ApiError> {
        let rows: Vec<RawBigMapKey> = self
            .client
            .get_json(&format!(
                "/contracts/{contract}/bigmaps/{path}/keys?{filter}&sort.asc=id\
                 &offset.cr={after}&limit={page}"
            ))?
            .ok_or_else(|| ApiError::Permanent(format!("big map {contract}/{path} is absent")))?;
        let keys: Vec<BigMapKey> = rows
            .into_iter()
            .map(|r| {
                Ok(BigMapKey {
                    id: fit(r.id, "big map key id")?,
                    key: match r.key {
                        Value::String(s) => s,
                        other => other.to_string(),
                    },
                    value: r.value,
                    active: r.active,
                    last_level: fit(r.last_level, "big map key lastLevel")?,
                })
            })
            .collect::<Result<_, ApiError>>()?;
        check_page(
            &format!("big map {contract}/{path}"),
            &keys,
            page,
            after,
            &keep,
        )?;
        Ok(keys)
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

    /// Lowest and highest token id on `contract`; `None` when it has no tokens.
    pub fn token_bounds(&self, contract: &str) -> Result<Option<(u64, u64)>, ApiError> {
        let edge = |dir: &str| -> Result<Option<u64>, ApiError> {
            let rows: Vec<String> = self
                .client
                .get_json(&format!(
                    "/tokens?contract={contract}&select=tokenId&sort.{dir}=tokenId&limit=1"
                ))?
                .ok_or_else(|| ApiError::Permanent("token bounds endpoint is absent".into()))?;
            rows.first().map(|s| num(s, "token bound")).transpose()
        };
        match (edge("asc")?, edge("desc")?) {
            (None, None) => Ok(None),
            (Some(lo), Some(hi)) if lo <= hi && hi - lo < MAX_TOKEN_SPAN => Ok(Some((lo, hi))),
            other => Err(ApiError::Permanent(format!(
                "{contract}: implausible token id bounds {other:?}"
            ))),
        }
    }

    /// Lowest and highest token id named by applied mints on `contract` (mints are
    /// sequential, so the first and last mint operations carry them).
    pub fn mint_bounds(&self, contract: &str) -> Result<Option<(u64, u64)>, ApiError> {
        let edge = |dir: &str| -> Result<Option<u64>, ApiError> {
            let rows: Vec<RawMint> = self
                .client
                .get_json(&format!(
                    "/operations/transactions?target={contract}&entrypoint=mint&status=applied\
                     &sort.{dir}=id&limit=1&select={MINT_SELECT}"
                ))?
                .ok_or_else(|| ApiError::Permanent("mint bounds endpoint is absent".into()))?;
            rows.into_iter()
                .next()
                .map(|r| Mint::try_from(r).map(|m| m.token_id))
                .transpose()
        };
        match (edge("asc")?, edge("desc")?) {
            (None, None) => Ok(None),
            (Some(a), Some(b)) => Ok(Some((a.min(b), a.max(b)))),
            other => Err(ApiError::Permanent(format!(
                "{contract}: inconsistent mint bounds {other:?}"
            ))),
        }
    }

    /// Applied mints naming token ids below `lo` or above `hi` (normally none). Each side is
    /// capped at [`MAX_PAGE`] rows.
    pub fn mints_outside(&self, contract: &str, lo: u64, hi: u64) -> Result<Vec<Mint>, ApiError> {
        let mut out = self.paged::<RawMint, _>(
            &format!(
                "/operations/transactions?target={contract}&entrypoint=mint&status=applied\
                 &parameter.token_id.gt={hi}&select={MINT_SELECT}"
            ),
            MAX_PAGE,
            MAX_PAGE,
            |m: &Mint| m.token_id > hi,
        )?;
        if lo > 0 {
            out.extend(self.paged::<RawMint, _>(
                &format!(
                    "/operations/transactions?target={contract}&entrypoint=mint&status=applied\
                     &parameter.token_id.lt={lo}&select={MINT_SELECT}"
                ),
                MAX_PAGE,
                MAX_PAGE,
                |m: &Mint| m.token_id < lo,
            )?);
        }
        Ok(out)
    }

    /// Tokens of `[lo, hi)` last changed at or below level `upto`.
    pub fn tokens_range(
        &self,
        contract: &str,
        lo: u64,
        hi: u64,
        upto: u64,
    ) -> Result<Vec<ChainToken>, ApiError> {
        self.paged::<RawToken, _>(
            &format!(
                "/tokens?contract={contract}&tokenId.ge={lo}&tokenId.lt={hi}\
                 &lastLevel.le={upto}&select={TOKEN_SELECT}"
            ),
            MAX_PAGE,
            span(lo, hi)?,
            |t: &ChainToken| (lo..hi).contains(&t.token_id) && t.last_level <= upto,
        )
    }

    /// Mints of `[lo, hi)` at or below level `upto`.
    pub fn mints_range(
        &self,
        contract: &str,
        lo: u64,
        hi: u64,
        upto: u64,
    ) -> Result<Vec<Mint>, ApiError> {
        self.paged::<RawMint, _>(
            &format!(
                "/operations/transactions?target={contract}&entrypoint=mint&status=applied\
                 &parameter.token_id.ge={lo}&parameter.token_id.lt={hi}&level.le={upto}\
                 &select={MINT_SELECT}"
            ),
            MAX_PAGE,
            span(lo, hi)?,
            |m: &Mint| (lo..hi).contains(&m.token_id) && m.level <= upto,
        )
    }

    /// Positive balances of `[lo, hi)` last changed at or below level `upto`.
    pub fn balances_range(
        &self,
        contract: &str,
        lo: u64,
        hi: u64,
        upto: u64,
    ) -> Result<Vec<Balance>, ApiError> {
        self.paged::<RawBalance, _>(
            &format!(
                "/tokens/balances?token.contract={contract}&token.tokenId.ge={lo}\
                 &token.tokenId.lt={hi}&balance.gt=0&lastLevel.le={upto}&select={BALANCE_SELECT}"
            ),
            MAX_PAGE,
            span(lo, hi)?.saturating_mul(HOLDERS_PER_TOKEN),
            |b: &Balance| b.positive && (lo..hi).contains(&b.token_id) && b.last_level <= upto,
        )
    }

    /// Whether `token_id` exists on `contract`: a 200 without it, or a 404, is a definite no.
    pub fn token_exists(&self, contract: &str, token_id: u64) -> Result<bool, ApiError> {
        let rows: Option<Vec<String>> = self.client.get_json(&format!(
            "/tokens?contract={contract}&tokenId={token_id}&select=tokenId&limit=1"
        ))?;
        Ok(rows.is_some_and(|r| r.iter().any(|t| t.parse() == Ok(token_id))))
    }

    /// Tokens of `list`; with `upto`, only those last changed at or below that level.
    pub fn tokens_by_ids(
        &self,
        contract: &str,
        list: &[u64],
        upto: Option<u64>,
    ) -> Result<Vec<ChainToken>, ApiError> {
        let mut out = Vec::new();
        for chunk in list.chunks(MAX_IN) {
            out.extend(self.paged::<RawToken, _>(
                &format!(
                    "/tokens?contract={contract}&tokenId.in={}{}&select={TOKEN_SELECT}",
                    ids(chunk),
                    cap("lastLevel", upto)
                ),
                MAX_PAGE,
                chunk.len(),
                |t: &ChainToken| {
                    chunk.contains(&t.token_id) && upto.is_none_or(|u| t.last_level <= u)
                },
            )?);
        }
        Ok(out)
    }

    /// Mints of `list`; with `upto`, only those at or below that level.
    pub fn mints_by_ids(
        &self,
        contract: &str,
        list: &[u64],
        upto: Option<u64>,
    ) -> Result<Vec<Mint>, ApiError> {
        let mut out = Vec::new();
        for chunk in list.chunks(MAX_IN) {
            out.extend(self.paged::<RawMint, _>(
                &format!(
                    "/operations/transactions?target={contract}&entrypoint=mint&status=applied\
                     &{}{}&select={MINT_SELECT}",
                    json_in("parameter.token_id", chunk),
                    cap("level", upto)
                ),
                MAX_PAGE,
                chunk.len(),
                |m: &Mint| chunk.contains(&m.token_id) && upto.is_none_or(|u| m.level <= u),
            )?);
        }
        Ok(out)
    }

    /// Positive balances of `list`; with `upto`, only rows last changed at or below that level.
    pub fn balances_by_ids(
        &self,
        contract: &str,
        list: &[u64],
        upto: Option<u64>,
    ) -> Result<Vec<Balance>, ApiError> {
        let mut out = Vec::new();
        for chunk in list.chunks(MAX_IN) {
            out.extend(self.paged::<RawBalance, _>(
                &format!(
                    "/tokens/balances?token.contract={contract}&token.tokenId.in={}\
                     &balance.gt=0{}&select={BALANCE_SELECT}",
                    ids(chunk),
                    cap("lastLevel", upto)
                ),
                MAX_PAGE,
                chunk.len().saturating_mul(HOLDERS_PER_TOKEN),
                |b: &Balance| {
                    b.positive
                        && chunk.contains(&b.token_id)
                        && upto.is_none_or(|u| b.last_level <= u)
                },
            )?);
        }
        Ok(out)
    }

    /// Tokens last changed in levels `(level, upto]`, one page after TzKT id `after`.
    pub fn tokens_changed(
        &self,
        contract: &str,
        (level, upto): (u64, u64),
        after: u64,
        page: usize,
    ) -> Result<Vec<ChainToken>, ApiError> {
        self.page::<RawToken, _>(
            &format!(
                "/tokens?contract={contract}&lastLevel.gt={level}&lastLevel.le={upto}\
                 &select={TOKEN_SELECT}"
            ),
            page,
            after,
            |t: &ChainToken| t.last_level > level && t.last_level <= upto,
        )
    }

    /// Balance rows (zero included) last changed in levels `(level, upto]`, one page after
    /// id `after`.
    pub fn balances_changed(
        &self,
        contract: &str,
        (level, upto): (u64, u64),
        after: u64,
        page: usize,
    ) -> Result<Vec<Balance>, ApiError> {
        self.page::<RawBalance, _>(
            &format!(
                "/tokens/balances?token.contract={contract}&lastLevel.gt={level}\
                 &lastLevel.le={upto}&select={BALANCE_SELECT}"
            ),
            page,
            after,
            |b: &Balance| b.last_level > level && b.last_level <= upto,
        )
    }

    /// Mints in levels `(level, upto]`, one page after operation id `after`.
    pub fn mints_since(
        &self,
        contract: &str,
        (level, upto): (u64, u64),
        after: u64,
        page: usize,
    ) -> Result<Vec<Mint>, ApiError> {
        self.page::<RawMint, _>(
            &format!(
                "/operations/transactions?target={contract}&entrypoint=mint&status=applied\
                 &level.gt={level}&level.le={upto}&select={MINT_SELECT}"
            ),
            page,
            after,
            |m: &Mint| m.level > level && m.level <= upto,
        )
    }
}

/// Largest plausible token id span of one contract (the largest has about 1.03 M tokens).
pub const MAX_TOKEN_SPAN: u64 = 100_000_000;
/// Positive balance rows accepted per token (fxhash tokens have exactly one holder).
const HOLDERS_PER_TOKEN: usize = 4;

/// `&<field>.le=<upto>`, or nothing without a cap.
fn cap(field: &str, upto: Option<u64>) -> String {
    upto.map_or_else(String::new, |u| format!("&{field}.le={u}"))
}

fn span(lo: u64, hi: u64) -> Result<usize, ApiError> {
    hi.checked_sub(lo)
        .filter(|n| *n <= MAX_TOKEN_SPAN)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| ApiError::Permanent(format!("token id range [{lo}, {hi}) is invalid")))
}

/// At most `limit` rows, ids strictly increasing after `after`, every row accepted by `keep`.
pub(crate) fn check_page<T: HasId>(
    what: &str,
    rows: &[T],
    limit: usize,
    after: u64,
    keep: &dyn Fn(&T) -> bool,
) -> Result<(), ApiError> {
    if rows.len() > limit {
        return Err(ApiError::Permanent(format!(
            "{what}: {} rows for a page of {limit}",
            rows.len()
        )));
    }
    let mut last = after;
    for row in rows {
        if row.id() <= last {
            return Err(ApiError::Permanent(format!(
                "{what}: ids repeat or go backwards at {}",
                row.id()
            )));
        }
        last = row.id();
        if !keep(row) {
            return Err(ApiError::Permanent(format!(
                "{what}: row {} is outside the requested filter",
                row.id()
            )));
        }
    }
    Ok(())
}

pub trait HasId {
    fn id(&self) -> u64;
}

impl HasId for ChainToken {
    fn id(&self) -> u64 {
        self.id
    }
}

impl HasId for Mint {
    fn id(&self) -> u64 {
        self.op_id
    }
}

impl HasId for Balance {
    fn id(&self) -> u64 {
        self.id
    }
}

impl HasId for BigMapKey {
    fn id(&self) -> u64 {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::http::{Fetch, HttpResponse, RetryPolicy, TransportError};
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// Serves scripted bodies in order; every request must have one.
    struct Script(Mutex<VecDeque<String>>);

    impl Fetch for Script {
        fn get(&self, _url: &str) -> Result<HttpResponse, TransportError> {
            let body = self
                .0
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request");
            Ok(HttpResponse {
                status: 200,
                body: body.into_bytes(),
            })
        }
    }

    fn api(bodies: Vec<String>) -> Tzkt {
        let retry = RetryPolicy {
            attempts: 1,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        };
        Tzkt::new(Client::new(
            Arc::new(Script(Mutex::new(bodies.into()))),
            "http://x/v1",
            retry,
            Duration::ZERO,
            Arc::new(AtomicBool::new(false)),
        ))
    }

    fn token(id: u64, token_id: u64) -> serde_json::Value {
        serde_json::json!({"id": id, "tokenId": token_id.to_string(),
            "firstTime": "2022-01-01T00:00:00Z", "firstLevel": 1, "lastLevel": 2})
    }

    fn page(rows: Vec<serde_json::Value>) -> String {
        serde_json::Value::Array(rows).to_string()
    }

    fn permanent<T: std::fmt::Debug>(r: Result<T, ApiError>, needle: &str) {
        match r {
            Err(ApiError::Permanent(m)) => assert!(m.contains(needle), "{m}"),
            other => panic!("expected a permanent error with {needle:?}, got {other:?}"),
        }
    }

    /// Answers every request with an empty page and records its URL.
    struct Recorder(Arc<Mutex<Vec<String>>>);

    impl Fetch for Recorder {
        fn get(&self, url: &str) -> Result<HttpResponse, TransportError> {
            self.0.lock().unwrap().push(url.to_string());
            Ok(HttpResponse {
                status: 200,
                body: b"[]".to_vec(),
            })
        }
    }

    #[test]
    fn json_filters_use_equality_for_one_value_because_tzkt_rejects_single_item_in() {
        assert_eq!(json_in("key", &[7u64]), "key=7");
        assert_eq!(json_in("key", &[7u64, 8]), "key.in=7,8");
        assert_eq!(
            json_in("value.fa2", &["KT1a", "KT1b"]),
            "value.fa2.in=KT1a,KT1b"
        );
        let urls = Arc::new(Mutex::new(Vec::new()));
        let retry = RetryPolicy {
            attempts: 1,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        };
        let api = Tzkt::new(Client::new(
            Arc::new(Recorder(urls.clone())),
            "http://x/v1",
            retry,
            Duration::ZERO,
            Arc::new(AtomicBool::new(false)),
        ));
        assert!(
            api.mints_by_ids("KT1x", &[63029], Some(9))
                .unwrap()
                .is_empty()
        );
        assert!(api.mints_by_ids("KT1x", &[1, 2], None).unwrap().is_empty());
        let urls = urls.lock().unwrap();
        assert!(
            urls[0].contains("&parameter.token_id=63029&"),
            "{}",
            urls[0]
        );
        assert!(!urls[0].contains(".in="), "{}", urls[0]);
        assert!(
            urls[1].contains("&parameter.token_id.in=1,2&"),
            "{}",
            urls[1]
        );
    }

    #[test]
    fn endless_full_pages_stop_at_the_row_cap() {
        // A full page of rows that all claim the 3-token range: more than it can hold.
        let full = page((1..=MAX_PAGE as u64).map(|i| token(i, i % 3)).collect());
        permanent(
            api(vec![full]).tokens_range("KT1x", 0, 3, 10),
            "more than 3 rows",
        );
    }

    #[test]
    fn repeated_reordered_out_of_filter_and_oversized_rows_are_rejected() {
        permanent(
            api(vec![page(vec![token(5, 1), token(5, 2)])]).tokens_range("KT1x", 0, 3, 10),
            "repeat or go backwards",
        );
        permanent(
            api(vec![page(vec![token(5, 1), token(3, 2)])]).tokens_range("KT1x", 0, 3, 10),
            "repeat or go backwards",
        );
        permanent(
            api(vec![page(vec![token(5, 7)])]).tokens_range("KT1x", 0, 3, 10),
            "outside the requested filter",
        );
        permanent(
            api(vec![page(vec![token(5, 1)])]).tokens_by_ids("KT1x", &[2], None),
            "outside the requested filter",
        );
        // An incremental page larger than asked for.
        permanent(
            api(vec![page(vec![token(5, 1), token(6, 2), token(7, 3)])]).tokens_changed(
                "KT1x",
                (1, 10),
                0,
                2,
            ),
            "rows for a page of 2",
        );
        // A row that did not change after the requested level, or changed after the cap.
        permanent(
            api(vec![page(vec![token(5, 1)])]).tokens_changed("KT1x", (2, 10), 0, 10),
            "outside the requested filter",
        );
        permanent(
            api(vec![page(vec![token(5, 1)])]).tokens_changed("KT1x", (0, 1), 0, 10),
            "outside the requested filter",
        );
        permanent(
            api(vec![page(vec![token(5, 1)])]).tokens_by_ids("KT1x", &[1], Some(1)),
            "outside the requested filter",
        );
        // Ids that do not fit SQLite are refused at parse time.
        permanent(
            api(vec![page(vec![token(u64::MAX, 1)])]).tokens_range("KT1x", 0, 3, 10),
            "below 2^63",
        );
    }

    #[test]
    fn huge_or_overflowing_token_bounds_are_permanent() {
        permanent(
            api(vec!["[\"0\"]".into(), "[\"18446744073709551615\"]".into()]).token_bounds("KT1x"),
            "below 2^63",
        );
        permanent(
            api(vec!["[\"0\"]".into(), "[\"900000000\"]".into()]).token_bounds("KT1x"),
            "implausible token id bounds",
        );
        permanent(
            api(vec!["[\"5\"]".into(), "[\"1\"]".into()]).token_bounds("KT1x"),
            "implausible token id bounds",
        );
        assert_eq!(
            api(vec!["[]".into(), "[]".into()]).token_bounds("KT1x"),
            Ok(None)
        );
        assert_eq!(
            api(vec!["[\"0\"]".into(), "[\"589145\"]".into()]).token_bounds("KT1x"),
            Ok(Some((0, 589_145)))
        );
    }

    #[test]
    fn api_base_accepts_the_root_or_v1_with_or_without_slash() {
        for url in [
            "https://api.tzkt.io",
            "https://api.tzkt.io/",
            "https://api.tzkt.io/v1",
            "https://api.tzkt.io/v1/",
            " https://api.tzkt.io/v1// ",
        ] {
            assert_eq!(
                api_base(url).as_deref(),
                Some("https://api.tzkt.io/v1"),
                "{url:?}"
            );
        }
        assert_eq!(
            api_base("http://127.0.0.1:5000").as_deref(),
            Some("http://127.0.0.1:5000/v1")
        );
        for bad in [
            "api.tzkt.io",
            "ftp://api.tzkt.io",
            "https://",
            "https:///v1",
            "https://x/v1?a=1",
        ] {
            assert_eq!(api_base(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn attributes_keep_first_name_and_stringify_values() {
        let v: Value = serde_json::json!([
            {"name": "Palette", "value": "Gold"},
            {"name": "Wind", "value": -0.4},
            {"name": "Palette", "value": "Other"},
            {"name": "Flag", "value": true},
            {"value": "unnamed"},
            {"name": "Empty", "value": null}
        ]);
        assert_eq!(
            attributes(Some(v)),
            vec![
                ("Palette".to_owned(), "Gold".to_owned()),
                ("Wind".to_owned(), "-0.4".to_owned()),
                ("Flag".to_owned(), "true".to_owned()),
            ]
        );
        assert!(attributes(Some(Value::Null)).is_empty());
    }

    #[test]
    fn seed_falls_back_to_the_artifact_query() {
        let mut t = ChainToken {
            id: 1,
            token_id: 1,
            first_time: "2022-01-01T00:00:00Z".into(),
            first_level: 1,
            last_level: 1,
            name: None,
            artifact_uri: Some("ipfs://Qm?fxhash=ooA".into()),
            display_uri: None,
            thumbnail_uri: None,
            attributes: Vec::new(),
            iteration_hash: None,
        };
        assert_eq!(t.seed(), Some("ooA"));
        t.iteration_hash = Some("ooB".into());
        assert_eq!(t.seed(), Some("ooB"));
        t.iteration_hash = Some(String::new());
        t.artifact_uri = Some("ipfs://Qm".into());
        assert_eq!(t.seed(), None);
    }
}
