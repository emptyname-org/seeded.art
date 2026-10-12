//! Operation groups to sales rows. Pure except for escrow bookkeeping and ticket lookups,
//! which go through the [`Escrow`] and [`TicketPrices`] traits (SQLite in the indexer, maps in
//! tests).
//!
//! Tezos runs internal operations depth first and TzKT numbers operations in execution order,
//! so an operation emitted by a contract call belongs to the latest call on that contract with
//! the same group hash and counter and a lower id.

use std::collections::HashMap;

use super::chain::{self, MAX_MUTEZ, Move, Op};
use super::markets::{Issuer, Market, Price, WXTZ, is_token_contract};
use super::{Currency, SaleKind, SalesError, bad};

/// A sales row to write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSale {
    pub contract: String,
    pub token_id: u64,
    pub kind: SaleKind,
    pub price_mutez: Option<u64>,
    pub currency: Currency,
    pub seller: Option<String>,
    pub buyer: String,
    pub marketplace: &'static str,
    pub marketplace_contract: &'static str,
    pub op_hash: String,
    /// TzKT id of the operation that moved (or minted) the token.
    pub op_id: u64,
    pub level: u64,
    pub timestamp: String,
}

/// Something the rules could not settle; the row (if any) keeps what is certain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    pub op_id: u64,
    pub op_hash: String,
    pub level: u64,
    pub reason: &'static str,
    pub detail: String,
}

/// Cross-checks of buy-now prices: the call amount against what the market paid out.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Checks {
    pub amount_equals_payout: u64,
    pub amount_differs_from_payout: u64,
}

impl Checks {
    pub fn add(&mut self, other: Checks) {
        self.amount_equals_payout += other.amount_equals_payout;
        self.amount_differs_from_payout += other.amount_differs_from_payout;
    }
}

#[derive(Debug, Default)]
pub struct Outcome {
    pub sales: Vec<NewSale>,
    pub issues: Vec<Issue>,
    pub checks: Checks,
    /// Tokens put into escrow (listings, auctions).
    pub deposits: u64,
    /// Tokens handed back from escrow unsold.
    pub returns: u64,
}

/// Who listed each token a market holds. One instance per market.
pub trait Escrow {
    fn deposit(
        &mut self,
        contract: &str,
        token_id: u64,
        seller: &str,
        op_id: u64,
    ) -> Result<(), SalesError>;
    /// Removes and returns the depositor, if known.
    fn take(&mut self, contract: &str, token_id: u64) -> Result<Option<String>, SalesError>;
}

/// Prices paid for mint tickets.
pub trait TicketPrices {
    fn ticket_price(&mut self, ticket_id: u64) -> Result<Option<u64>, SalesError>;
}

fn checked_sum(a: u64, b: u64, op: &Op) -> Result<u64, SalesError> {
    a.checked_add(b)
        .filter(|s| *s <= MAX_MUTEZ)
        .ok_or_else(|| bad("payout", format!("sum overflows in {}", op.hash)))
}

struct Child<'a> {
    op: &'a Op,
    contract: &'a str,
    mv: Move,
    tez: u64,
    wxtz: u64,
}

/// One call on the market and what it emitted.
struct Call<'a> {
    op: &'a Op,
    children: Vec<Child<'a>>,
    /// The child whose payouts follow: the latest fxhash token moved, until a transfer of
    /// another token ends its run.
    current: Option<usize>,
    tez: u64,
    wxtz: u64,
    /// An FA1.2 transfer of a token other than wXTZ (another currency).
    other_currency: Option<String>,
    /// An FA2 transfer on a contract that is not an fxhash token (another NFT or an FA2
    /// currency).
    foreign_fa2: Option<String>,
}

impl<'a> Call<'a> {
    fn new(op: &'a Op) -> Call<'a> {
        Call {
            op,
            children: Vec::new(),
            current: None,
            tez: 0,
            wxtz: 0,
            other_currency: None,
            foreign_fa2: None,
        }
    }

    fn add(&mut self, tez: u64, wxtz: u64, by: &Op) -> Result<(), SalesError> {
        self.tez = checked_sum(self.tez, tez, by)?;
        self.wxtz = checked_sum(self.wxtz, wxtz, by)?;
        if let Some(c) = self.current.and_then(|i| self.children.get_mut(i)) {
            c.tez = checked_sum(c.tez, tez, by)?;
            c.wxtz = checked_sum(c.wxtz, wxtz, by)?;
        }
        Ok(())
    }

    fn paid(&self) -> bool {
        self.tez > 0 || self.wxtz > 0 || self.other_currency.is_some()
    }
}

fn issue(op: &Op, reason: &'static str, detail: impl Into<String>) -> Issue {
    Issue {
        op_id: op.id,
        op_hash: op.hash.clone(),
        level: op.level,
        reason,
        detail: detail.into(),
    }
}

/// Interprets `ops` (strictly increasing ids, whole levels, every operation of `market` at
/// those levels or at least every group that moved an fxhash token).
pub fn market_ops(
    market: &'static Market,
    ops: &[Op],
    escrow: &mut dyn Escrow,
) -> Result<Outcome, SalesError> {
    if ops.windows(2).any(|w| w[0].id >= w[1].id) {
        return Err(bad("operations", "ids are not strictly increasing"));
    }
    let mut out = Outcome::default();
    let mut calls: Vec<Call> = Vec::new();
    let mut open: HashMap<(&str, u64), usize> = HashMap::new();
    for op in ops {
        let from_market = op.sender == market.address;
        let to_market = op.target == market.address;
        if from_market == to_market {
            continue;
        }
        if to_market {
            if op.entrypoint.is_some() {
                open.insert((op.hash.as_str(), op.counter), calls.len());
                calls.push(Call::new(op));
            }
            continue;
        }
        let is_transfer = op.is_call("transfer");
        let Some(&ci) = open.get(&(op.hash.as_str(), op.counter)) else {
            if is_transfer && is_token_contract(&op.target) {
                out.issues
                    .push(issue(op, "orphan_move", "no market call precedes it"));
            }
            continue;
        };
        let call = &mut calls[ci];
        let mut wxtz = 0;
        if is_transfer && is_token_contract(&op.target) {
            for mv in chain::fa2_moves(op)? {
                call.children.push(Child {
                    op,
                    contract: &op.target,
                    mv,
                    tez: 0,
                    wxtz: 0,
                });
                call.current = Some(call.children.len() - 1);
            }
        } else if is_transfer {
            // Only a wXTZ value is read (and bounded); another token's is only shape-checked.
            let fa12 = if op.target == WXTZ {
                chain::fa12_value(op)?
            } else {
                chain::is_fa12_transfer(op)?.then_some(0)
            };
            match fa12 {
                Some(v) if op.target == WXTZ => {
                    if op.value["to"].as_str() != Some(market.address) {
                        wxtz = v;
                    }
                }
                Some(_) => {
                    call.other_currency.get_or_insert_with(|| op.target.clone());
                }
                None => {
                    call.foreign_fa2.get_or_insert_with(|| op.target.clone());
                    call.current = None;
                }
            }
        }
        call.add(op.amount, wxtz, op)?;
    }
    for call in calls {
        settle(market, call, escrow, &mut out)?;
    }
    Ok(out)
}

enum Role {
    Deposit,
    Skip,
    Candidate {
        seller: Option<String>,
        kind: SaleKind,
    },
}

/// Price and currency from payouts, `None` when not a tez amount.
fn payout(
    tez: u64,
    wxtz: u64,
    other: Option<&str>,
) -> Result<(Option<u64>, Currency), &'static str> {
    match (tez > 0, wxtz > 0, other) {
        (true, false, None) => Ok((Some(tez), Currency::Tez)),
        (false, true, None) => Ok((Some(wxtz), Currency::Wxtz)),
        (false, false, Some(c)) => Ok((None, Currency::Token(c.to_owned()))),
        (false, false, None) => Ok((Some(0), Currency::Tez)),
        _ => Err("mixed_currency"),
    }
}

fn settle(
    market: &'static Market,
    call: Call,
    escrow: &mut dyn Escrow,
    out: &mut Outcome,
) -> Result<(), SalesError> {
    let rule = call.op.entrypoint.as_deref().and_then(|e| market.rule(e));
    let paid = call.paid();
    let mut roles = Vec::with_capacity(call.children.len());
    for c in &call.children {
        let mv = &c.mv;
        let role = if mv.to == market.address {
            escrow.deposit(c.contract, mv.token_id, &mv.from, c.op.id)?;
            out.deposits += 1;
            Role::Deposit
        } else {
            let (seller, from_escrow) = if mv.from == market.address {
                (escrow.take(c.contract, mv.token_id)?, true)
            } else {
                (Some(mv.from.clone()), false)
            };
            if from_escrow && !paid && seller.as_deref() == Some(mv.to.as_str()) {
                out.returns += 1;
                Role::Skip
            } else if mv.from == mv.to {
                Role::Skip
            } else {
                match (rule, paid) {
                    (Some(r), _) => Role::Candidate {
                        seller,
                        kind: r.kind,
                    },
                    (None, true) => Role::Candidate {
                        seller,
                        kind: SaleKind::Other,
                    },
                    (None, false) => {
                        out.issues.push(issue(
                            c.op,
                            "unrecognized_move",
                            format!(
                                "{} moved token {} without payment",
                                call.op.entrypoint.as_deref().unwrap_or("?"),
                                mv.token_id
                            ),
                        ));
                        Role::Skip
                    }
                }
            }
        };
        roles.push(role);
    }
    let candidates: Vec<usize> = roles
        .iter()
        .enumerate()
        .filter(|(_, r)| matches!(r, Role::Candidate { .. }))
        .map(|(i, _)| i)
        .collect();
    let prices = price(&call, rule.map(|r| r.price), &candidates, out);
    for (i, (price_mutez, currency)) in candidates.iter().zip(prices) {
        let c = &call.children[*i];
        let Role::Candidate { seller, kind } = &roles[*i] else {
            unreachable!("candidates are filtered")
        };
        if seller.is_none() {
            out.issues.push(issue(
                c.op,
                "seller_unknown",
                format!(
                    "token {} left escrow without a recorded listing",
                    c.mv.token_id
                ),
            ));
        }
        out.sales.push(NewSale {
            contract: c.contract.to_owned(),
            token_id: c.mv.token_id,
            kind: *kind,
            price_mutez,
            currency,
            seller: seller.clone(),
            buyer: c.mv.to.clone(),
            marketplace: market.name,
            marketplace_contract: market.address,
            op_hash: c.op.hash.clone(),
            op_id: c.op.id,
            level: c.op.level,
            timestamp: c.op.timestamp.clone(),
        });
    }
    Ok(())
}

/// Prices for the sold children `idx` of `call`, in order.
///
/// One fxhash token alone in its call: the call amount (buy now) or the whole payout. Several
/// tokens, or other tokens moved in the same call: each token's own payouts (those following
/// its transfer), which must all be non-zero and, for a buy now of fxhash tokens only, add up
/// to the amount; otherwise no price is invented.
fn price(
    call: &Call,
    rule: Option<Price>,
    idx: &[usize],
    out: &mut Outcome,
) -> Vec<(Option<u64>, Currency)> {
    let unknown = |out: &mut Outcome, reason: &'static str| {
        out.issues
            .push(issue(call.op, reason, format!("{} sold", idx.len())));
        vec![(None, Currency::Tez); idx.len()]
    };
    let foreign = call.foreign_fa2.is_some();
    if idx.is_empty() {
        return Vec::new();
    }
    if idx.len() == 1 && !foreign {
        if rule == Some(Price::Amount) && call.op.amount > 0 {
            let amount = call.op.amount;
            if call.tez == amount {
                out.checks.amount_equals_payout += 1;
                return vec![(Some(amount), Currency::Tez)];
            }
            // fxhash v1 keeps an overpayment: the payout is the listing price.
            out.checks.amount_differs_from_payout += 1;
            out.issues.push(issue(
                call.op,
                "amount_differs_from_payout",
                format!("paid {amount}, paid out {}", call.tez),
            ));
            let price = if call.tez > 0 && call.tez < amount {
                call.tez
            } else {
                amount
            };
            return vec![(Some(price), Currency::Tez)];
        }
        return match payout(call.tez, call.wxtz, call.other_currency.as_deref()) {
            Ok(p) => vec![p],
            Err(reason) => unknown(out, reason),
        };
    }
    if call.tez == 0 && call.wxtz == 0 {
        // Paid in another token, or not at all.
        return match call.other_currency.as_ref().or(call.foreign_fa2.as_ref()) {
            Some(c) => vec![(None, Currency::Token(c.clone())); idx.len()],
            None => vec![(Some(0), Currency::Tez); idx.len()],
        };
    }
    let ops: Vec<u64> = idx.iter().map(|i| call.children[*i].op.id).collect();
    if ops.windows(2).any(|w| w[0] == w[1]) {
        return unknown(out, "bulk_unsplittable");
    }
    let mut prices = Vec::with_capacity(idx.len());
    for i in idx {
        let c = &call.children[*i];
        match payout(c.tez, c.wxtz, call.other_currency.as_deref()) {
            Ok((Some(p), currency)) if p > 0 => prices.push((Some(p), currency)),
            Ok(_) => return unknown(out, "bulk_unsplittable"),
            Err(reason) => return unknown(out, reason),
        }
    }
    let tez: u64 = prices
        .iter()
        .filter(|(_, c)| *c == Currency::Tez)
        .filter_map(|(p, _)| *p)
        .sum();
    if rule == Some(Price::Amount) && call.op.amount > 0 && !foreign && tez != call.op.amount {
        return unknown(out, "bulk_amount_mismatch");
    }
    prices
}

/// Mints of one issuer: `children` are the token contract `mint` calls it made, `parents` its
/// own `mint`/`mint_with_ticket` calls over the same levels; both by increasing id.
pub fn mint_ops(
    issuer: &'static Issuer,
    children: &[Op],
    parents: &[Op],
    tickets: &mut dyn TicketPrices,
) -> Result<Outcome, SalesError> {
    for list in [children, parents] {
        if list.windows(2).any(|w| w[0].id >= w[1].id) {
            return Err(bad("operations", "ids are not strictly increasing"));
        }
    }
    let mut out = Outcome::default();
    let mut latest: HashMap<(&str, u64), (&Op, u32)> = HashMap::new();
    let mut p = parents.iter().peekable();
    for child in children {
        while let Some(parent) = p.next_if(|o| o.id < child.id) {
            latest.insert((parent.hash.as_str(), parent.counter), (parent, 0));
        }
        let (token_id, receiver) = chain::mint_param(child)?;
        let price_mutez = match latest.get_mut(&(child.hash.as_str(), child.counter)) {
            None => {
                out.issues.push(issue(
                    child,
                    "mint_without_issuer_call",
                    token_id.to_string(),
                ));
                None
            }
            Some((_, n)) if *n > 0 => {
                out.issues.push(issue(
                    child,
                    "issuer_call_minted_twice",
                    token_id.to_string(),
                ));
                None
            }
            Some((parent, n)) => {
                *n += 1;
                if parent.is_call("mint") {
                    Some(parent.amount)
                } else if parent.is_call("mint_with_ticket") && issuer.tickets {
                    let ticket = chain::ticket_param(parent)?;
                    let price = tickets.ticket_price(ticket)?;
                    if price.is_none() {
                        out.issues
                            .push(issue(child, "ticket_unknown", ticket.to_string()));
                    }
                    price
                } else {
                    out.issues.push(issue(
                        child,
                        "unexpected_issuer_call",
                        parent.entrypoint.clone().unwrap_or_default(),
                    ));
                    None
                }
            }
        };
        out.sales.push(NewSale {
            contract: issuer.token_contract.to_owned(),
            token_id,
            kind: SaleKind::Mint,
            price_mutez,
            currency: Currency::Tez,
            seller: None,
            buyer: receiver,
            marketplace: issuer.name,
            marketplace_contract: issuer.address,
            op_hash: child.hash.clone(),
            op_id: child.id,
            level: child.level,
            timestamp: child.timestamp.clone(),
        });
    }
    Ok(out)
}

/// In-memory escrow and tickets, for tests and dry runs.
#[derive(Debug, Default)]
pub struct MemoryBook {
    pub escrow: HashMap<(String, u64), String>,
    pub tickets: HashMap<u64, u64>,
}

impl Escrow for MemoryBook {
    fn deposit(
        &mut self,
        contract: &str,
        token_id: u64,
        seller: &str,
        _op_id: u64,
    ) -> Result<(), SalesError> {
        self.escrow
            .insert((contract.to_owned(), token_id), seller.to_owned());
        Ok(())
    }

    fn take(&mut self, contract: &str, token_id: u64) -> Result<Option<String>, SalesError> {
        Ok(self.escrow.remove(&(contract.to_owned(), token_id)))
    }
}

impl TicketPrices for MemoryBook {
    fn ticket_price(&mut self, ticket_id: u64) -> Result<Option<u64>, SalesError> {
        Ok(self.tickets.get(&ticket_id).copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sales::markets::MARKETS;
    use serde_json::{Value, json};

    const BUYER: &str = "tz1bz2CaKYAKiByEBrJdnnXjhVFqjy6YnVhv";
    const SELLER: &str = "tz1NABW5hTM33gVgPjMHmYgaLdKoGvaek7FQ";
    const OTHER_NFT: &str = "KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton";
    const GENTK: &str = "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi";

    /// objkt.com marketplace v3.1 (shared, `fulfill_ask_bulk` buys at the call amount).
    fn market() -> &'static Market {
        MARKETS
            .iter()
            .find(|m| m.address == "KT1Xjap1TwmDR1d8yEd8ErkraAj2mbdMrPZY")
            .unwrap()
    }

    fn op(id: u64, sender: &str, target: &str, amount: u64, ep: Option<&str>, value: Value) -> Op {
        Op {
            id,
            level: 10,
            timestamp: "2024-01-01T00:00:00Z".into(),
            hash: "ooUia6U1pKmPCSzWuSRW42YQEReM2B1BgyssGvb7aocZ9HR2po9".into(),
            counter: 1,
            nonce: None,
            sender: sender.into(),
            target: target.into(),
            amount,
            entrypoint: ep.map(str::to_owned),
            value,
        }
    }

    fn transfer(id: u64, contract: &str, token: &str) -> Op {
        let m = market().address;
        op(
            id,
            m,
            contract,
            0,
            Some("transfer"),
            json!([{"from_": SELLER, "txs": [{"to_": BUYER, "token_id": token, "amount": "1"}]}]),
        )
    }

    fn pay(id: u64, mutez: u64) -> Op {
        op(id, market().address, SELLER, mutez, None, Value::Null)
    }

    fn bulk(rest: Vec<Op>) -> Outcome {
        let mut ops = vec![op(
            1,
            BUYER,
            market().address,
            15_000_000,
            Some("fulfill_ask_bulk"),
            json!({}),
        )];
        ops.extend(rest);
        market_ops(market(), &ops, &mut MemoryBook::default()).unwrap()
    }

    #[test]
    fn a_bulk_buy_mixing_collections_prices_each_token_by_its_own_payouts() {
        let after = bulk(vec![
            transfer(2, GENTK, "7"),
            pay(3, 10_000_000),
            transfer(4, OTHER_NFT, "1"),
            pay(5, 5_000_000),
        ]);
        assert_eq!(after.sales.len(), 1);
        assert_eq!(after.sales[0].price_mutez, Some(10_000_000));
        let before = bulk(vec![
            transfer(2, OTHER_NFT, "1"),
            pay(3, 5_000_000),
            transfer(4, GENTK, "7"),
            pay(5, 10_000_000),
        ]);
        assert_eq!(before.sales[0].price_mutez, Some(10_000_000));
        assert!(after.issues.is_empty() && before.issues.is_empty());
    }

    #[test]
    fn payouts_that_cannot_be_told_apart_leave_the_price_unknown() {
        // Both tokens moved before any payout.
        let out = bulk(vec![
            transfer(2, GENTK, "7"),
            transfer(3, OTHER_NFT, "1"),
            pay(4, 15_000_000),
        ]);
        assert_eq!(out.sales[0].price_mutez, None);
        assert_eq!(out.issues[0].reason, "bulk_unsplittable");
        // Two fxhash tokens whose payouts do not add up to what was paid.
        let out = bulk(vec![
            transfer(2, GENTK, "7"),
            pay(3, 10_000_000),
            transfer(4, GENTK, "8"),
            pay(5, 1_000_000),
        ]);
        assert!(out.sales.iter().all(|s| s.price_mutez.is_none()));
        assert_eq!(out.issues[0].reason, "bulk_amount_mismatch");
        // Two fxhash tokens, each followed by its payout: split exactly.
        let out = bulk(vec![
            transfer(2, GENTK, "7"),
            pay(3, 10_000_000),
            transfer(4, GENTK, "8"),
            pay(5, 5_000_000),
        ]);
        let prices: Vec<_> = out.sales.iter().map(|s| s.price_mutez).collect();
        assert_eq!(prices, [Some(10_000_000), Some(5_000_000)]);
    }

    #[test]
    fn a_payout_in_another_fa12_token_of_any_size_never_stops_the_page() {
        const KUSD: &str = "KT1K9gCRgaLRFKTErYt1wVxA3Frb9FjasjTV";
        let m = market().address;
        let fa12 = |id: u64, token: &str, value: &str| {
            op(
                id,
                m,
                token,
                0,
                Some("transfer"),
                json!({"from": BUYER, "to": SELLER, "value": value}),
            )
        };
        let second = |mut o: Op| {
            o.counter = 2;
            o
        };
        let call = |id: u64| op(id, BUYER, m, 0, Some("fulfill_ask_bulk"), json!({}));
        // An fxhash sale, then an unrelated one at the same level paid in an 18-decimal
        // token: 0.1 unit (1e17) and 100 units (above 2^63).
        let ops = vec![
            call(1),
            transfer(2, GENTK, "7"),
            pay(3, 5_000_000),
            second(call(4)),
            second(transfer(5, OTHER_NFT, "1")),
            second(fa12(6, KUSD, "100000000000000000")),
            second(fa12(7, KUSD, "100000000000000000000")),
        ];
        let out = market_ops(market(), &ops, &mut MemoryBook::default()).unwrap();
        assert_eq!(out.sales.len(), 1);
        assert_eq!(out.sales[0].contract, GENTK);
        // wXTZ stays bounded, and a malformed value is still rejected.
        for (token, value) in [(WXTZ, (MAX_MUTEZ + 1).to_string()), (KUSD, "1e18".into())] {
            let ops = vec![call(1), transfer(2, GENTK, "7"), fa12(3, token, &value)];
            assert!(
                market_ops(market(), &ops, &mut MemoryBook::default()).is_err(),
                "{token} {value}"
            );
        }
    }

    #[test]
    fn payout_sums_that_overflow_are_rejected() {
        let ops = vec![
            op(
                1,
                BUYER,
                market().address,
                0,
                Some("fulfill_offer"),
                json!({}),
            ),
            transfer(2, GENTK, "7"),
            pay(3, chain::MAX_MUTEZ),
            pay(4, 1),
        ];
        assert!(market_ops(market(), &ops, &mut MemoryBook::default()).is_err());
    }
}
