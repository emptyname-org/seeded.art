//! Validated TzKT rows and page fetchers for the sales indexer.
//!
//! Every list is paged by TzKT's operation id (`sort.asc=id&id.gt=<last>`), and every page is
//! checked: at most the requested rows, ids strictly increasing, each row inside the filter
//! asked for (TzKT silently ignores filters it does not know). Ids, levels and counters must
//! fit SQLite's i64; tez amounts must stay below [`MAX_MUTEZ`]. Anything else is a permanent
//! error and no cursor moves.

use std::fmt;

use serde::de::{self, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};

use crate::ingest::http::ApiError;
use crate::ingest::tzkt::{HasId, MAX_IN, Tzkt, check_page};

use super::markets::{TICKET_ISSUER, TICKETS};

pub const OP_SELECT: &str =
    "id,level,timestamp,hash,counter,nonce,sender,target,amount,parameter,status";
const TICKET_SELECT: &str = "id,level,token.tokenId,transactionId,from,to";
/// Ten times the tez supply: no single amount or price can be larger.
pub const MAX_MUTEZ: u64 = 10_000_000_000_000_000;
/// Rows accepted for one level of one stream (the busiest levels have a few hundred).
const MAX_LEVEL_ROWS: usize = 200_000;

fn bad(what: &str, detail: impl std::fmt::Display) -> ApiError {
    ApiError::Permanent(format!("malformed TzKT {what}: {detail}"))
}

fn base58(s: &str) -> bool {
    s.bytes()
        .all(|b| b.is_ascii_alphanumeric() && !matches!(b, b'0' | b'O' | b'I' | b'l'))
}

/// A Tezos account address: `tz1`-`tz4`, `KT1` or `sr1` plus 33 base58 characters.
pub fn is_address(s: &str) -> bool {
    // base58 first: it guarantees ASCII, so the prefix slice is on a char boundary.
    s.len() == 36 && base58(s) && ["tz1", "tz2", "tz3", "tz4", "KT1", "sr1"].contains(&&s[..3])
}

/// An operation hash: `o` plus 50 base58 characters.
pub fn is_op_hash(s: &str) -> bool {
    s.len() == 51 && s.starts_with('o') && base58(s)
}

/// `YYYY-MM-DDTHH:MM:SSZ`, as TzKT writes block times.
pub fn is_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 20
        && b.iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            10 => *c == b'T',
            13 | 16 => *c == b':',
            19 => *c == b'Z',
            _ => c.is_ascii_digit(),
        })
}

/// A value that fits SQLite's i64.
fn fit(v: u64, what: &str) -> Result<u64, ApiError> {
    i64::try_from(v)
        .map(|_| v)
        .map_err(|_| bad(what, format!("{v} is not below 2^63")))
}

/// A Michelson nat as TzKT renders it (a decimal string), below 2^63.
pub fn nat(v: &Value, what: &str) -> Result<u64, ApiError> {
    let s = v
        .as_str()
        .ok_or_else(|| bad(what, format!("{v} is not a decimal string")))?;
    if s.is_empty() || s.len() > 19 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad(what, format!("{s:?} is not a number below 2^63")));
    }
    let n: u64 = s
        .parse()
        .map_err(|_| bad(what, format!("{s:?} is not a number")))?;
    fit(n, what)
}

/// A mutez amount (decimal string) no larger than [`MAX_MUTEZ`].
pub fn mutez(v: &Value, what: &str) -> Result<u64, ApiError> {
    let n = nat(v, what)?;
    if n > MAX_MUTEZ {
        return Err(bad(what, format!("{n} mutez is implausible")));
    }
    Ok(n)
}

pub fn address(v: &Value, what: &str) -> Result<String, ApiError> {
    v.as_str()
        .filter(|s| is_address(s))
        .map(str::to_owned)
        .ok_or_else(|| bad(what, format!("{v} is not an address")))
}

/// Object keys of parameter values the rules read; every other key is skipped while parsing,
/// which keeps large unused fields (token metadata, royalty splits, fx(params) bytes) out of
/// memory.
const KEEP_KEYS: [&str; 11] = [
    "from_",
    "txs",
    "to_",
    "token_id",
    "amount",
    "from",
    "to",
    "value",
    "address",
    "ticket_id",
    "price",
];

/// A parameter value reduced to [`KEEP_KEYS`] at every depth.
#[derive(Debug, Clone, PartialEq, Default, Serialize)]
#[serde(transparent)]
pub struct Lean(pub Value);

impl<'de> Deserialize<'de> for Lean {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Lean, D::Error> {
        d.deserialize_any(LeanVisitor).map(Lean)
    }
}

struct LeanVisitor;

impl<'de> Visitor<'de> for LeanVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
        Ok(Number::from_f64(v).map_or(Value::Null, Value::Number))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
        Ok(Value::String(v.to_owned()))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Value, E> {
        Ok(Value::String(v))
    }

    fn visit_none<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut out = Vec::new();
        while let Some(Lean(v)) = seq.next_element()? {
            out.push(v);
        }
        Ok(Value::Array(out))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut out = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if KEEP_KEYS.contains(&key.as_str()) {
                let Lean(v) = map.next_value()?;
                out.insert(key, v);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(Value::Object(out))
    }
}

#[derive(Deserialize)]
struct RawAccount {
    address: String,
}

#[derive(Deserialize)]
struct RawParameter {
    entrypoint: String,
    #[serde(default)]
    value: Lean,
}

#[derive(Deserialize)]
pub(crate) struct RawOp {
    id: u64,
    level: u64,
    timestamp: String,
    hash: String,
    counter: u64,
    #[serde(default)]
    nonce: Option<u64>,
    sender: RawAccount,
    #[serde(default)]
    target: Option<RawAccount>,
    amount: u64,
    #[serde(default)]
    parameter: Option<RawParameter>,
    status: String,
}

/// One applied transaction (external or internal).
#[derive(Debug, Clone, PartialEq)]
pub struct Op {
    pub id: u64,
    pub level: u64,
    pub timestamp: String,
    pub hash: String,
    pub counter: u64,
    pub nonce: Option<u64>,
    pub sender: String,
    pub target: String,
    /// Tez sent, in mutez.
    pub amount: u64,
    pub entrypoint: Option<String>,
    /// The parameter value, reduced to the keys the rules read (see [`Lean`]).
    pub value: Value,
}

impl TryFrom<RawOp> for Op {
    type Error = ApiError;

    fn try_from(r: RawOp) -> Result<Op, ApiError> {
        let id = fit(r.id, "operation id")?;
        let target = r
            .target
            .map(|t| t.address)
            .ok_or_else(|| bad("operation", format!("{id} has no target")))?;
        if r.status != "applied" {
            return Err(bad("operation", format!("{id} has status {:?}", r.status)));
        }
        if !is_op_hash(&r.hash) || !is_timestamp(&r.timestamp) {
            return Err(bad(
                "operation",
                format!("{id}: hash {:?} or time {:?}", r.hash, r.timestamp),
            ));
        }
        if !is_address(&r.sender.address) || !is_address(&target) {
            return Err(bad(
                "operation",
                format!("{id}: sender {:?} or target {target:?}", r.sender.address),
            ));
        }
        if r.amount > MAX_MUTEZ {
            return Err(bad("operation", format!("{id}: amount {}", r.amount)));
        }
        let (entrypoint, value) = match r.parameter {
            Some(p) => (Some(p.entrypoint), p.value.0),
            None => (None, Value::Null),
        };
        Ok(Op {
            id,
            level: fit(r.level, "operation level")?,
            timestamp: r.timestamp,
            hash: r.hash,
            counter: fit(r.counter, "operation counter")?,
            nonce: r.nonce.map(|n| fit(n, "operation nonce")).transpose()?,
            sender: r.sender.address,
            target,
            amount: r.amount,
            entrypoint,
            value,
        })
    }
}

impl HasId for Op {
    fn id(&self) -> u64 {
        self.id
    }
}

impl Op {
    pub fn is_call(&self, entrypoint: &str) -> bool {
        self.entrypoint.as_deref() == Some(entrypoint)
    }
}

/// Parses and validates a JSON array of transactions in TzKT's shape (the indexer's select).
pub fn ops_from_json(json: &[u8]) -> Result<Vec<Op>, ApiError> {
    let rows: Vec<RawOp> = serde_json::from_slice(json)
        .map_err(|e| ApiError::Permanent(format!("unusable JSON: {e}")))?;
    rows.into_iter().map(Op::try_from).collect()
}

/// One token moved by an FA2 `transfer`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Move {
    pub token_id: u64,
    pub from: String,
    pub to: String,
}

/// Token moves of an FA2 `transfer` on an fxhash token contract. Zero-amount entries move
/// nothing and are skipped; these tokens are unique, so an amount above 1 is malformed.
pub fn fa2_moves(op: &Op) -> Result<Vec<Move>, ApiError> {
    let what = format!("FA2 transfer {}", op.id);
    let batches = op
        .value
        .as_array()
        .ok_or_else(|| bad(&what, "parameter is not a list"))?;
    let mut out = Vec::new();
    for b in batches {
        let from = address(&b["from_"], &what)?;
        let txs = b["txs"]
            .as_array()
            .ok_or_else(|| bad(&what, "txs is not a list"))?;
        for t in txs {
            let to = address(&t["to_"], &what)?;
            let token_id = nat(&t["token_id"], &what)?;
            match nat(&t["amount"], &what)? {
                0 => {}
                1 => out.push(Move {
                    token_id,
                    from: from.clone(),
                    to,
                }),
                n => return Err(bad(&what, format!("amount {n} of unique token {token_id}"))),
            }
        }
    }
    Ok(out)
}

/// Whether the parameter is an FA1.2 `transfer` (`{from, to, value}`, `value` a decimal nat
/// of any size); an error when it has a `value` but not that shape.
pub fn is_fa12_transfer(op: &Op) -> Result<bool, ApiError> {
    if !op.value.is_object() || op.value.get("value").is_none() {
        return Ok(false);
    }
    let what = format!("FA1.2 transfer {}", op.id);
    address(&op.value["from"], &what)?;
    address(&op.value["to"], &what)?;
    let value = &op.value["value"];
    if !value
        .as_str()
        .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(bad(&what, format!("{value} is not a nat")));
    }
    Ok(true)
}

/// The value of an FA1.2 `transfer` as mutez (no larger than [`MAX_MUTEZ`]), if the parameter
/// has that shape. Only for wXTZ: other tokens' values are not mutez.
pub fn fa12_value(op: &Op) -> Result<Option<u64>, ApiError> {
    if !is_fa12_transfer(op)? {
        return Ok(None);
    }
    mutez(&op.value["value"], &format!("FA1.2 transfer {}", op.id)).map(Some)
}

/// `(token_id, receiver)` of an fxhash token contract `mint`.
pub fn mint_param(op: &Op) -> Result<(u64, String), ApiError> {
    let what = format!("mint {}", op.id);
    Ok((
        nat(&op.value["token_id"], &what)?,
        address(&op.value["address"], &what)?,
    ))
}

/// The ticket redeemed by a `mint_with_ticket` call.
pub fn ticket_param(op: &Op) -> Result<u64, ApiError> {
    nat(
        &op.value["ticket_id"],
        &format!("mint_with_ticket {}", op.id),
    )
}

/// A mint ticket created on [`TICKETS`]: transfer from nobody, with its creating operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TicketMint {
    /// TzKT transfer id (the cursor).
    pub id: u64,
    pub level: u64,
    pub ticket_id: u64,
    pub op_id: u64,
    pub owner: String,
    from_nobody: bool,
}

#[derive(Deserialize)]
pub(crate) struct RawTicketMint {
    id: u64,
    level: u64,
    #[serde(rename = "token.tokenId")]
    token_id: String,
    #[serde(rename = "transactionId", default)]
    transaction_id: Option<u64>,
    #[serde(default)]
    from: Option<RawAccount>,
    #[serde(default)]
    to: Option<RawAccount>,
}

impl TryFrom<RawTicketMint> for TicketMint {
    type Error = ApiError;

    fn try_from(r: RawTicketMint) -> Result<TicketMint, ApiError> {
        let what = format!("ticket transfer {}", r.id);
        let owner =
            r.to.map(|a| a.address)
                .filter(|a| is_address(a))
                .ok_or_else(|| bad(&what, "no receiver"))?;
        Ok(TicketMint {
            id: fit(r.id, &what)?,
            level: fit(r.level, &what)?,
            ticket_id: nat(&Value::String(r.token_id), &what)?,
            op_id: fit(
                r.transaction_id
                    .ok_or_else(|| bad(&what, "not made by a transaction"))?,
                &what,
            )?,
            owner,
            from_nobody: r.from.is_none(),
        })
    }
}

impl HasId for TicketMint {
    fn id(&self) -> u64 {
        self.id
    }
}

/// The price a ticket's `mint` call on [`TICKETS`] records (what the issuer charged).
pub fn ticket_price(op: &Op) -> Result<u64, ApiError> {
    mutez(&op.value["price"], &format!("ticket mint {}", op.id))
}

/// Page fetchers over the shared TzKT client.
pub struct Feed<'a> {
    pub api: &'a Tzkt,
    /// Rows per page (TzKT allows up to 10,000).
    pub page: usize,
}

impl Feed<'_> {
    /// One validated page of `path` (filters included) after operation id `after`.
    pub fn ops_page(
        &self,
        path: &str,
        after: u64,
        limit: usize,
        keep: &dyn Fn(&Op) -> bool,
    ) -> Result<Vec<Op>, ApiError> {
        let rows: Vec<RawOp> = self
            .api
            .client
            .get_json(&format!(
                "{path}&sort.asc=id&id.gt={after}&limit={limit}&select={OP_SELECT}"
            ))?
            .ok_or_else(|| ApiError::Permanent(format!("{path}: list endpoint is absent")))?;
        let ops = rows
            .into_iter()
            .map(Op::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        check_page(path, &ops, limit, after, keep)?;
        Ok(ops)
    }

    /// Every row of `path`, paged; more than `cap` rows is a permanent error.
    pub fn ops_all(
        &self,
        path: &str,
        keep: &dyn Fn(&Op) -> bool,
        cap: usize,
    ) -> Result<Vec<Op>, ApiError> {
        let mut out: Vec<Op> = Vec::new();
        loop {
            let after = out.last().map_or(0, |o| o.id);
            let rows = self.ops_page(path, after, self.page, keep)?;
            let n = rows.len();
            out.extend(rows);
            if out.len() > cap {
                return Err(ApiError::Permanent(format!("{path}: more than {cap} rows")));
            }
            if n < self.page {
                return Ok(out);
            }
        }
    }

    /// The next rows of `path` after `after`, ending on a level boundary, so an operation group
    /// is never split: a full page drops its last level, which the next page starts with;
    /// a full page of a single level is completed with the rest of that level.
    pub fn ops_levels(
        &self,
        path: &str,
        after: u64,
        keep: &dyn Fn(&Op) -> bool,
    ) -> Result<Vec<Op>, ApiError> {
        let mut rows = self.ops_page(path, after, self.page, keep)?;
        if rows.len() < self.page {
            return Ok(rows);
        }
        let last = rows[rows.len() - 1].level;
        if rows[0].level != last {
            rows.retain(|o| o.level < last);
            return Ok(rows);
        }
        let level_path = format!("{path}&level={last}");
        loop {
            let from = rows[rows.len() - 1].id;
            let more = self.ops_page(&level_path, from, self.page, &|o| {
                o.level == last && keep(o)
            })?;
            let n = more.len();
            rows.extend(more);
            if rows.len() > MAX_LEVEL_ROWS {
                return Err(ApiError::Permanent(format!(
                    "{path}: more than {MAX_LEVEL_ROWS} rows at level {last}"
                )));
            }
            if n < self.page {
                return Ok(rows);
            }
        }
    }

    /// Operations by TzKT id, every one of them; `keep` must accept each.
    pub fn ops_by_ids(&self, ids: &[u64], keep: &dyn Fn(&Op) -> bool) -> Result<Vec<Op>, ApiError> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(MAX_IN) {
            let list = chunk
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let path = format!("/operations/transactions?id.in={list}");
            let rows =
                self.ops_page(&path, 0, chunk.len(), &|o| chunk.contains(&o.id) && keep(o))?;
            if rows.len() != chunk.len() {
                return Err(ApiError::Permanent(format!(
                    "{} of {} operations by id are missing",
                    chunk.len() - rows.len(),
                    chunk.len()
                )));
            }
            out.extend(rows);
        }
        Ok(out)
    }

    /// Tickets created up to `safe`, one page after transfer id `after`.
    pub fn ticket_mints(&self, safe: u64, after: u64) -> Result<Vec<TicketMint>, ApiError> {
        let path =
            format!("/tokens/transfers?token.contract={TICKETS}&from.null=true&level.le={safe}");
        let rows: Vec<RawTicketMint> = self
            .api
            .client
            .get_json(&format!(
                "{path}&sort.asc=id&id.gt={after}&limit={}&select={TICKET_SELECT}",
                self.page
            ))?
            .ok_or_else(|| ApiError::Permanent(format!("{path}: list endpoint is absent")))?;
        let rows = rows
            .into_iter()
            .map(TicketMint::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        check_page(&path, &rows, self.page, after, &|t: &TicketMint| {
            t.from_nobody && t.level <= safe
        })?;
        Ok(rows)
    }

    /// The `mint` operations that created `tickets`, with their recorded price.
    pub fn ticket_prices(&self, tickets: &[TicketMint]) -> Result<Vec<(u64, u64)>, ApiError> {
        let ids: Vec<u64> = tickets.iter().map(|t| t.op_id).collect();
        let ops = self.ops_by_ids(&ids, &|o| {
            o.target == TICKETS && o.sender == TICKET_ISSUER.address && o.is_call("mint")
        })?;
        ops.iter().map(|o| Ok((o.id, ticket_price(o)?))).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn raw(v: Value) -> Result<Op, ApiError> {
        Op::try_from(serde_json::from_value::<RawOp>(v).unwrap())
    }

    fn op_json() -> Value {
        json!({"id": 5, "level": 2, "timestamp": "2022-01-01T00:00:00Z",
            "hash": "ooUia6U1pKmPCSzWuSRW42YQEReM2B1BgyssGvb7aocZ9HR2po9", "counter": 7,
            "sender": {"address": "tz1bz2CaKYAKiByEBrJdnnXjhVFqjy6YnVhv"},
            "target": {"alias": "x", "address": "KT1QJ71jypKGgyTNtXjkCAYJZNhCKWiHuT2r"},
            "amount": 209812500, "parameter": {"entrypoint": "buy", "value": "12564"},
            "status": "applied"})
    }

    #[test]
    fn addresses_hashes_and_times_are_checked() {
        assert!(is_address("tz1bz2CaKYAKiByEBrJdnnXjhVFqjy6YnVhv"));
        assert!(is_address("KT1QJ71jypKGgyTNtXjkCAYJZNhCKWiHuT2r"));
        assert!(!is_address("tz5bz2CaKYAKiByEBrJdnnXjhVFqjy6YnVhv"));
        assert!(!is_address("tz1bz2CaKYAKiByEBrJdnnXjhVFqjy6YnVh0"));
        assert!(!is_address("tz1short"));
        assert!(!is_address("tz\u{e9}bz2CaKYAKiByEBrJdnnXjhVFqjy6YnVh"));
        assert!(is_op_hash(
            "ooUia6U1pKmPCSzWuSRW42YQEReM2B1BgyssGvb7aocZ9HR2po9"
        ));
        assert!(!is_op_hash(
            "BMaNqnMTRNRvfqWVFEsEjFC8RD6eLUuW3EWea7X6wJSuL2ntmkN"
        ));
        assert!(is_timestamp("2021-12-22T20:23:31Z"));
        assert!(!is_timestamp("2021-12-22 20:23:31"));
    }

    #[test]
    fn operations_parse_and_bad_values_are_rejected() {
        let op = raw(op_json()).unwrap();
        assert_eq!(op.amount, 209_812_500);
        assert!(op.is_call("buy"));
        for (field, value) in [
            ("amount", json!(MAX_MUTEZ + 1)),
            ("id", json!(u64::MAX)),
            ("status", json!("backtracked")),
            ("hash", json!("nothash")),
            ("timestamp", json!("yesterday")),
            ("sender", json!({"address": "tz1bogus"})),
            ("target", Value::Null),
        ] {
            let mut v = op_json();
            v[field] = value;
            assert!(
                matches!(raw(v), Err(ApiError::Permanent(_))),
                "{field} not rejected"
            );
        }
    }

    #[test]
    fn parameter_values_keep_only_the_keys_the_rules_read() {
        let mut v = op_json();
        v["parameter"] = json!({"entrypoint": "mint", "value": {
            "address": "tz1bz2CaKYAKiByEBrJdnnXjhVFqjy6YnVhv", "token_id": "7",
            "metadata": {"": "6970"}, "royalties_split": [{"pct": "500"}],
            "nested": {"price": "1"}, "input_bytes": "00ff"}});
        let op = raw(v).unwrap();
        assert_eq!(
            op.value,
            json!({"address": "tz1bz2CaKYAKiByEBrJdnnXjhVFqjy6YnVhv", "token_id": "7"})
        );
        assert_eq!(
            mint_param(&op).unwrap(),
            (7, "tz1bz2CaKYAKiByEBrJdnnXjhVFqjy6YnVhv".to_owned())
        );
    }

    #[test]
    fn nats_and_mutez_are_bounded() {
        assert_eq!(nat(&json!("42"), "x"), Ok(42));
        for v in [
            json!(42),
            json!("-1"),
            json!("1e3"),
            json!(""),
            json!("99999999999999999999"),
            json!("9223372036854775808"),
        ] {
            assert!(nat(&v, "x").is_err(), "{v}");
        }
        assert!(mutez(&json!((MAX_MUTEZ + 1).to_string()), "x").is_err());
    }

    #[test]
    fn fa2_moves_skip_zero_amounts_and_reject_duplicated_units() {
        let mut op = raw(op_json()).unwrap();
        op.value = json!([{"from_": "tz1bz2CaKYAKiByEBrJdnnXjhVFqjy6YnVhv", "txs": [
            {"to_": "KT1QJ71jypKGgyTNtXjkCAYJZNhCKWiHuT2r", "token_id": "7", "amount": "1"},
            {"to_": "KT1QJ71jypKGgyTNtXjkCAYJZNhCKWiHuT2r", "token_id": "8", "amount": "0"}]}]);
        let moves = fa2_moves(&op).unwrap();
        assert_eq!(moves.len(), 1);
        assert_eq!(moves[0].token_id, 7);
        op.value[0]["txs"][0]["amount"] = json!("2");
        assert!(fa2_moves(&op).is_err());
        op.value = json!({"from": "x"});
        assert!(fa2_moves(&op).is_err());
    }
}
