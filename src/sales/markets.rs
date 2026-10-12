//! Contracts that trade or mint fxhash tokens, and how each records a sale.
//!
//! Inventory (2026-10-05, TzKT): applied `transfer` calls on the three fxhash token contracts,
//! grouped by the contract that executed them. The markets below cover 99.9 % of the
//! contract-executed token transfers; the rest are fxhash mint-pass and swap contracts, gacha
//! machines and giveaways, none of them a marketplace. Details: `tmp/claude-sales/MARKETS.md`.
//!
//! A sale is a token move executed by the market inside one of the rule entrypoints:
//! - token into the market: escrow deposit (listing or auction), the depositor is the seller;
//! - token out of the market back to its depositor without payment: cancel or unsold auction;
//! - otherwise the receiver is the buyer and the seller is the sender (or the depositor).
//!
//! Prices: [`Price::Amount`] is the tez sent with the call (buy now), or the payout when the
//! market paid out less (an overpayment it kept); [`Price::Payout`] is what the market pays out
//! in that call (offers and auctions), in tez or wXTZ.

use super::SaleKind;
use crate::ingest::{ISSUERS, TOKEN_CONTRACTS};

/// objkt.com wrapped tez (FA1.2, 1:1 with tez, 6 decimals). Offers and auctions on objkt
/// markets from v2 auctions on are paid in it.
pub const WXTZ: &str = "KT1TjnZYs5CGLbmV6yuW169P8Pnr9BiVwwjz";
/// fxhash mint tickets (FA2): bought on the fx(params) issuer, redeemed by `mint_with_ticket`.
pub const TICKETS: &str = "KT19etLCjCCzTLFFAxsxLFsVYMRPetr2bTD5";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Price {
    /// The tez amount of the market call (the buyer's payment).
    Amount,
    /// The tez (else wXTZ) the market pays out within the call.
    Payout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rule {
    pub entrypoint: &'static str,
    pub kind: SaleKind,
    pub price: Price,
}

/// Dedicated markets trade only fxhash tokens, so their whole history is read; shared markets
/// are read only at the levels where they moved an fxhash token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Dedicated,
    Shared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Market {
    pub address: &'static str,
    pub name: &'static str,
    pub scope: Scope,
    pub rules: &'static [Rule],
}

const fn rule(entrypoint: &'static str, kind: SaleKind, price: Price) -> Rule {
    Rule {
        entrypoint,
        kind,
        price,
    }
}

use Price::{Amount, Payout};
use SaleKind::{Auction, OfferAccept, Sale};

/// Ranked by fxhash token transfers executed (inventory above).
pub const MARKETS: [Market; 11] = [
    Market {
        address: "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u",
        name: "fxhash marketplace v2",
        scope: Scope::Dedicated,
        rules: &[
            rule("listing_accept", Sale, Amount),
            rule("offer_accept", OfferAccept, Payout),
            rule("collection_offer_accept", OfferAccept, Payout),
            rule("auction_fulfill", Auction, Payout),
        ],
    },
    Market {
        address: "KT1Xo5B7PNBAeynZPmca4bRh6LQow4og1Zb9",
        name: "fxhash marketplace v1",
        scope: Scope::Dedicated,
        rules: &[rule("collect", Sale, Amount)],
    },
    Market {
        address: "KT1FvqJwEDWb1Gwc55Jd1jjTHRVWbYKUUpyq",
        name: "objkt.com marketplace v1",
        scope: Scope::Shared,
        rules: &[
            rule("fulfill_ask", Sale, Amount),
            rule("fulfill_bid", OfferAccept, Payout),
        ],
    },
    Market {
        address: "KT18p94vjkkHYY3nPmernmgVR7HdZFzE7NAk",
        name: "objkt.com English auctions v2",
        scope: Scope::Shared,
        rules: &[rule("settle_auction", Auction, Payout)],
    },
    Market {
        address: "KT1WvzYHCNBvDSdwafTHv7nJ1dWmZ8GCYuuC",
        name: "objkt.com marketplace v2",
        scope: Scope::Shared,
        rules: &[
            rule("fulfill_ask", Sale, Amount),
            rule("fulfill_offer", OfferAccept, Payout),
        ],
    },
    Market {
        address: "KT1XjcRq5MLAzMKQ3UHsrue2SeU2NbxUrzmU",
        name: "objkt.com English auctions v1",
        scope: Scope::Shared,
        rules: &[rule("conclude_auction", Auction, Payout)],
    },
    Market {
        address: "KT1SwbTqhSKF6Pdokiu1K4Fpi17ahPPzmt1X",
        name: "objkt.com marketplace v3.2",
        scope: Scope::Shared,
        rules: &[
            rule("fulfill_ask", Sale, Amount),
            rule("fulfill_ask_bulk", Sale, Amount),
            rule("fulfill_offer", OfferAccept, Payout),
            rule("fulfill_offer_bulk", OfferAccept, Payout),
            rule("fulfill_offer_gallery", OfferAccept, Payout),
        ],
    },
    Market {
        address: "KT19NMJYBC4AiprtetMW1K2damXgGRCbJWD5",
        name: "objkt.com collection offers",
        scope: Scope::Shared,
        rules: &[rule("fulfill_collection_offer", OfferAccept, Payout)],
    },
    Market {
        address: "KT18iSHoRW1iogamADWwQSDoZa3QkN4izkqj",
        name: "objkt.com English auctions v3",
        scope: Scope::Shared,
        rules: &[rule("settle_auction", Auction, Payout)],
    },
    Market {
        address: "KT1Xjap1TwmDR1d8yEd8ErkraAj2mbdMrPZY",
        name: "objkt.com marketplace v3.1",
        scope: Scope::Shared,
        rules: &[
            rule("fulfill_ask", Sale, Amount),
            rule("fulfill_ask_bulk", Sale, Amount),
            rule("fulfill_offer", OfferAccept, Payout),
            rule("fulfill_offer_bulk", OfferAccept, Payout),
        ],
    },
    Market {
        address: "KT1QJ71jypKGgyTNtXjkCAYJZNhCKWiHuT2r",
        name: "objkt.com Dutch auctions v1",
        scope: Scope::Shared,
        rules: &[rule("buy", Auction, Amount)],
    },
];

impl Market {
    pub fn rule(&self, entrypoint: &str) -> Option<&'static Rule> {
        self.rules.iter().find(|r| r.entrypoint == entrypoint)
    }

    pub fn by_address(address: &str) -> Option<&'static Market> {
        MARKETS.iter().find(|m| m.address == address)
    }
}

/// An fxhash issuer: its mint calls carry the primary price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Issuer {
    pub address: &'static str,
    pub token_contract: &'static str,
    pub name: &'static str,
    /// Whether it sells mint tickets ([`TICKETS`]) redeemed by `mint_with_ticket`.
    pub tickets: bool,
}

pub const ISSUER_LIST: [Issuer; 4] = [
    Issuer {
        address: ISSUERS[0].0,
        token_contract: ISSUERS[0].1,
        name: "fxhash issuer v1 (first)",
        tickets: false,
    },
    Issuer {
        address: ISSUERS[1].0,
        token_contract: ISSUERS[1].1,
        name: "fxhash issuer v1",
        tickets: false,
    },
    Issuer {
        address: ISSUERS[2].0,
        token_contract: ISSUERS[2].1,
        name: "fxhash issuer v2",
        tickets: false,
    },
    Issuer {
        address: ISSUERS[3].0,
        token_contract: ISSUERS[3].1,
        name: "fxhash issuer fx(params)",
        tickets: true,
    },
];

/// The issuer that sells mint tickets.
pub const TICKET_ISSUER: &Issuer = &ISSUER_LIST[3];

pub fn is_token_contract(address: &str) -> bool {
    TOKEN_CONTRACTS.contains(&address)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markets_and_issuers_are_distinct_and_well_formed() {
        let mut seen: Vec<&str> = MARKETS.iter().map(|m| m.address).collect();
        seen.extend(ISSUER_LIST.iter().map(|i| i.address));
        seen.extend([WXTZ, TICKETS]);
        seen.extend(TOKEN_CONTRACTS);
        let n = seen.len();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), n, "an address appears twice");
        for a in seen {
            assert!(super::super::chain::is_address(a), "{a}");
        }
        for m in &MARKETS {
            assert!(!m.rules.is_empty(), "{}", m.name);
            for r in m.rules {
                assert_eq!(m.rule(r.entrypoint), Some(r));
                assert_ne!(r.kind, SaleKind::Mint);
            }
        }
        assert!(TICKET_ISSUER.tickets);
        assert_eq!(ISSUER_LIST.iter().filter(|i| i.tickets).count(), 1);
    }

    #[test]
    fn rules_cover_buy_now_offers_and_auctions() {
        let v2 = Market::by_address("KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u").unwrap();
        assert_eq!(v2.rule("listing_accept").unwrap().price, Price::Amount);
        assert_eq!(v2.rule("offer_accept").unwrap().kind, SaleKind::OfferAccept);
        assert_eq!(v2.rule("listing"), None);
        assert_eq!(v2.rule("listing_cancel"), None);
        assert_eq!(Market::by_address("tz1x"), None);
    }
}
