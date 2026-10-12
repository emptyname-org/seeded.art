//! Sale recognition per marketplace rule and mint prices, on recorded TzKT rows
//! (tests/fixtures/sales, recorded 2026-10-05). Expected sellers are the depositors seen in the
//! listing operations; they match who received the seller's share of each payout.

mod sales_support;

use sales_support::{FX_V1, FX_V2, PARAMS, V1, V2, fixture};
use seeded_archive::sales::chain::{Op, ops_from_json, ticket_price};
use seeded_archive::sales::interpret::{MemoryBook, NewSale, Outcome, market_ops, mint_ops};
use seeded_archive::sales::markets::{ISSUER_LIST, Market, TICKETS};
use seeded_archive::sales::{Currency, SaleKind};

fn all_ops() -> Vec<Op> {
    ops_from_json(&fixture("ops.json")).unwrap()
}

/// Every recorded row of `market`, interpreted in id order with a fresh escrow.
fn run(market: &str) -> Outcome {
    let m = Market::by_address(market).unwrap();
    let ops: Vec<Op> = all_ops()
        .into_iter()
        .filter(|o| o.sender == market || o.target == market)
        .collect();
    market_ops(m, &ops, &mut MemoryBook::default()).unwrap()
}

fn sale<'a>(out: &'a Outcome, hash: &str, contract: &str, token: u64) -> &'a NewSale {
    let found: Vec<&NewSale> = out
        .sales
        .iter()
        .filter(|s| s.op_hash == hash && s.contract == contract && s.token_id == token)
        .collect();
    assert_eq!(found.len(), 1, "{hash} {contract} {token}: {found:?}");
    found[0]
}

#[allow(clippy::too_many_arguments)]
fn check(
    out: &Outcome,
    hash: &str,
    contract: &str,
    token: u64,
    kind: SaleKind,
    price: Option<u64>,
    currency: Currency,
    seller: &str,
    buyer: &str,
) {
    let s = sale(out, hash, contract, token);
    assert_eq!(s.kind, kind, "{hash}");
    assert_eq!(s.price_mutez, price, "{hash}");
    assert_eq!(s.currency, currency, "{hash}");
    assert_eq!(s.seller.as_deref(), Some(seller), "{hash}");
    assert_eq!(s.buyer, buyer, "{hash}");
    assert!(s.op_id > 0 && s.level > 0 && s.timestamp.ends_with('Z'));
}

fn no_sale(out: &Outcome, hash: &str) {
    assert!(
        out.sales.iter().all(|s| s.op_hash != hash),
        "{hash} must not be a sale"
    );
}

#[test]
fn fxhash_v2_listings_sell_at_the_call_amount_to_the_escrow_depositor() {
    let out = run(FX_V2);
    check(
        &out,
        "op1p6gN4Np1vr9LoPzjYzQLEMoq1PvtDJocqPvkrPtfysCCqEqZ",
        V2,
        862_456,
        SaleKind::Sale,
        Some(25_000_000),
        Currency::Tez,
        "tz1g9kNppBSkWZCLyw24KNGuEgMgC2brhar3",
        "tz1MKKVFZyH1euozVqtCB2dcuaFCcYJn67qZ",
    );
    check(
        &out,
        "ooohTVK5M8hhqqFYW9HAZcqNoAX8JuDQedCeTTY4pxbLfpHDQ93",
        V2,
        747_114,
        SaleKind::Sale,
        Some(12_000_000),
        Currency::Tez,
        "tz2Vx5khr7Q1eNLLtzBZqP54knZZdn1LpPCq",
        "tz1YRq9tBUNLnVsAAGrDEem8JHjgswjdxQiW",
    );
    assert_eq!(out.checks.amount_differs_from_payout, 0);
    assert!(out.issues.is_empty(), "{:?}", out.issues);
}

#[test]
fn fxhash_v2_offers_are_priced_by_what_the_market_pays_out() {
    let out = run(FX_V2);
    check(
        &out,
        "oouxLgB9P9Cqry2JPSZ2vyS5LpKdyYqFG2m9wFvjgFuVbCcjxAJ",
        V2,
        745_907,
        SaleKind::OfferAccept,
        Some(10_000_000),
        Currency::Tez,
        "tz1inPpZMzFUv5mkmqDMEC8sYxmEq53vxRhw",
        "tz1goAv2PaGVNnfTad9aRF9Kx1QMjmiKMvPP",
    );
    // A listing cancelled right before the offer is accepted: the cancel is no sale.
    check(
        &out,
        "ooNc59Xu18RapJUoaJ4iEFRS2fFhH8oKAvh6HEtjaokRgzxZK4G",
        PARAMS,
        25_926,
        SaleKind::OfferAccept,
        Some(110_000_000),
        Currency::Tez,
        "tz1M4mSCZeGrHgSP6Ccu4v6qNaWc1CfTJJJJ",
        "tz1LXeAGjv94wzZb9e6sRVvCvHN5J5AMjcjT",
    );
    assert_eq!(
        out.sales
            .iter()
            .filter(|s| s.op_hash == "ooNc59Xu18RapJUoaJ4iEFRS2fFhH8oKAvh6HEtjaokRgzxZK4G")
            .count(),
        1
    );
    // One collection offer filled twice in one group.
    for token in [1_535_788, 1_539_105] {
        check(
            &out,
            "opGtq3v67oVzYUiBVyuwNXNsdW34eVfxVRM4XpU4VrvtUwaVfiZ",
            V2,
            token,
            SaleKind::OfferAccept,
            Some(1_555_000),
            Currency::Tez,
            "tz1Ys6esG7nTGmRng9U1vCwbRq7ZD6zQmYpn",
            "tz1X95uJRaZkK4VDoPpf2UuSzX4T24E8eCrb",
        );
    }
    check(
        &out,
        "op2NSu1HAA3arLepxUxjqEVx9Mwnx5dPGLYB4ruJ4PgSXptUEEz",
        V1,
        239_711,
        SaleKind::OfferAccept,
        Some(15_000_000),
        Currency::Tez,
        "tz1UETEJjeDs31qdiNA8ab3tE3ctZfQ51ETR",
        "tz1RT94jREafiHzgtmkKbiVnDynriUYEi62D",
    );
}

#[test]
fn fxhash_v1_collects_sell_escrowed_listings() {
    let out = run(FX_V1);
    check(
        &out,
        "oonJTXaSP7zcDtfW9g6jZpPe56MFGm9m5SyAypTzxM2N9sAz4Wm",
        V1,
        301_604,
        SaleKind::Sale,
        Some(3_500_000),
        Currency::Tez,
        "tz1iw8onBgMQ9weVRVggu989TJHxE6Vq9Mtk",
        "tz1U5pq84qx98BQV6BRapZPnwHZ2nzR45FCH",
    );
    for (token, seller) in [
        (65_039, "tz1TeW2ZW2dDJtf5AdhjnesGWZ2urg5mTBxd"),
        (146_944, "tz1U7fEDMivRzicvjnXjmaJd8wjH4QxzVcKs"),
    ] {
        check(
            &out,
            "opWLEfeY5ZC2LdxBNYJ9wRLz6UM17E7SsESkp5fvWtVJqwPHWQt",
            V1,
            token,
            SaleKind::Sale,
            Some(10_000_000),
            Currency::Tez,
            seller,
            "tz1RytjjMduXapWhyUK26cYkqLXEsU6NhG1k",
        );
    }
    // 333 tez sent for a 100 tez listing; the market paid out 100 and kept the rest.
    check(
        &out,
        "opVepn9RtTkrKCmLwu67kueLU31K4o34QXTZhEgX9c36DuPh7dE",
        V1,
        202,
        SaleKind::Sale,
        Some(100_000_000),
        Currency::Tez,
        "tz1P26stJ7vqhjNvYyRUMUo365Lae1d6TuwL",
        "tz1cTS1WwovU7SC783xgJxZrzr151mcshmNi",
    );
    assert_eq!(out.checks.amount_differs_from_payout, 1);
    assert!(
        out.issues
            .iter()
            .any(|i| i.reason == "amount_differs_from_payout"
                && i.op_hash == "opVepn9RtTkrKCmLwu67kueLU31K4o34QXTZhEgX9c36DuPh7dE")
    );
}

#[test]
fn objkt_v1_asks_sell_from_escrow_and_bids_are_priced_by_payout() {
    let out = run("KT1FvqJwEDWb1Gwc55Jd1jjTHRVWbYKUUpyq");
    check(
        &out,
        "ooSjCjCN5pXxmmwGmiDi1BVmcQFm1A98QkqbFZDGMPX6A2k9TJo",
        V1,
        19_376,
        SaleKind::Sale,
        Some(10_000_000),
        Currency::Tez,
        "tz1ibTknNagJCgQPgcbPvJhpSyS5vxgdyFt4",
        "tz2PtodPvJkpttLfHDsc8ecRv3wmwJPPh7F8",
    );
    check(
        &out,
        "opLFbEtXSnL4eQEauDmv9WzGS3qq3KzDsXB6B4fkJ6shutdxdUb",
        V1,
        13_636,
        SaleKind::Sale,
        Some(1_000_000),
        Currency::Tez,
        "tz1a2ZeWmyNQ8BiuFNTE4vmFEP9MBaP76QPX",
        "tz2PtodPvJkpttLfHDsc8ecRv3wmwJPPh7F8",
    );
    check(
        &out,
        "opANVnpsCobEyhFzRkkMtHDJCjLcQTSCgoTV21VyafC6Vr4KRcd",
        V1,
        17_935,
        SaleKind::OfferAccept,
        Some(40_000_000),
        Currency::Tez,
        "tz1hKBFiaJ11oEBn6oAcdZV8c1JNZJLvZTsC",
        "tz1NABW5hTM33gVgPjMHmYgaLdKoGvaek7FQ",
    );
    check(
        &out,
        "ooFpPkkSFA6fpMicHv5rrzzSCpWwz1smPxNkYyNMceCBU6TPFPa",
        V1,
        33_213,
        SaleKind::OfferAccept,
        Some(20_000_000),
        Currency::Tez,
        "tz1eRdGHypszU3i2xa86ZH7yagUXEa7YZ6qZ",
        "tz1VGZPvqytFVAoSAsM8VrUveJdL3PpVy6K9",
    );
}

#[test]
fn objkt_v2_asks_offers_and_a_flip_in_one_group() {
    let out = run("KT1WvzYHCNBvDSdwafTHv7nJ1dWmZ8GCYuuC");
    check(
        &out,
        "oonnj3ktGHDamA64UWV8GiqwmDDEb6ZpuNCW27VCUgFJibpeS67",
        V2,
        1_307_007,
        SaleKind::Sale,
        Some(47_000_000),
        Currency::Tez,
        "tz1YS56LdD3eu78fwWv2XKMZxv5A7Bi2Eh43",
        "tz1Y6p2zN62DiGu3W66mcjUB4NwwShVuExnA",
    );
    check(
        &out,
        "ontDVQrX2VFFfdBumcJAHKbUhroWTBJ6Xm8Vxh8hET8TUwucQFx",
        V1,
        425_262,
        SaleKind::Sale,
        Some(100_000),
        Currency::Tez,
        "tz1TynnRz6e8Zr8AHa8xXh6hxo5gtBpTpDDt",
        "tz1L1ZTJg9jTBcC8KXTQeMNLcxeGN2BPsCXk",
    );
    // Bought at 50 tez and sold into an offer at 100 tez in the same group: two rows.
    let flip: Vec<&NewSale> = out
        .sales
        .iter()
        .filter(|s| s.op_hash == "opGyLSQRL3UsBbHYzGg8nLUJYmxsSvMZUdMPy8H3kbUTxVy9MMf")
        .collect();
    assert_eq!(flip.len(), 2);
    assert_eq!(
        (flip[0].kind, flip[0].price_mutez, flip[0].buyer.as_str()),
        (
            SaleKind::Sale,
            Some(50_000_000),
            "tz1hn8RbCwkwm4gfu89dkERyWtkVmVXo6Jv8"
        )
    );
    assert_eq!(
        (flip[1].kind, flip[1].price_mutez, flip[1].buyer.as_str()),
        (
            SaleKind::OfferAccept,
            Some(100_000_000),
            "tz1fecaAvBRYYCkoTfSe9MGRgHVzhsadC9Zf"
        )
    );
    assert!(flip[0].op_id < flip[1].op_id);
    check(
        &out,
        "op3auPHQzj8o5tdxa6ULkmJ44qaXMccVf65gruLJW62EbHiwEzn",
        V2,
        1_477_839,
        SaleKind::OfferAccept,
        Some(50_000_000),
        Currency::Tez,
        "tz1inPpZMzFUv5mkmqDMEC8sYxmEq53vxRhw",
        "tz1b9C71xHXaFfJ4MKLfseDuPhbPE8Dxmiis",
    );
}

#[test]
fn a_price_in_another_token_is_kept_unpriced_with_its_currency() {
    let out = run("KT1WvzYHCNBvDSdwafTHv7nJ1dWmZ8GCYuuC");
    check(
        &out,
        "oowbuEU61PHgHw3XJB8bxK4aXmeYZbriuVoo56xbvhTiwWnyw84",
        PARAMS,
        27_169,
        SaleKind::Sale,
        None,
        Currency::Token("KT1MZg99PxMDEENwB4Fi64xkqAVh5d1rv8Z9".into()),
        "tz1eeq85Mq5Ypi19RsCWgc3QtzHKE4NnkjKR",
        "tz1VKHC43brmDVFnN2WsJcrXQZ9qpUVeeQ1M",
    );
}

#[test]
fn objkt_v3_asks_wxtz_offers_free_asks_and_unsplittable_bulk_buys() {
    let v31 = run("KT1Xjap1TwmDR1d8yEd8ErkraAj2mbdMrPZY");
    check(
        &v31,
        "opYNsrGTP4Rs4QBAmxq1adiHB7bkDkiLw8sGr7y5z4VL4VQZ2w4",
        V2,
        792_592,
        SaleKind::Sale,
        Some(1_750_000_000),
        Currency::Tez,
        "tz1fdv4qjH467zGFJycBA1wX7kXgMdKSHfeq",
        "tz1cLWwSeXu979EjhbB7okmqzh9NVBaXDDEN",
    );
    check(
        &v31,
        "ooKJmFTBUVKH69h3nZVKTPJZ4qSw31sY9N2zCJc44q6gsVxnfha",
        V1,
        85_943,
        SaleKind::OfferAccept,
        Some(150_000_000),
        Currency::Wxtz,
        "tz1Lzzs5HipSpdNi1ra28iwdKqC92eR2xMwT",
        "tz1M5QPtZ6WLTkotfonH6QXNpncjUudVmTWN",
    );
    // Five tokens in one FA2 transfer, one payout for all: no per-token price is invented.
    let bulk: Vec<&NewSale> = v31
        .sales
        .iter()
        .filter(|s| s.op_hash == "ooyQ7JxdsmHvEWnXniXmZqfgKuYNxEeCSa6TRKGZETMgEx3hEyJ")
        .collect();
    assert_eq!(bulk.len(), 5);
    assert!(bulk.iter().all(|s| s.price_mutez.is_none()
        && s.seller.as_deref() == Some("tz1U4HMCKSp3FnmYGgdtUCNX4mYef8SCAucr")
        && s.buyer == "tz1Qjun4X5ZGeh6b4E8Puk2cSA8TyKPLFoaH"));
    assert!(v31.issues.iter().any(|i| i.reason == "bulk_unsplittable"));

    let v32 = run("KT1SwbTqhSKF6Pdokiu1K4Fpi17ahPPzmt1X");
    // Bought for another address (proxy_for): the receiver is the buyer.
    check(
        &v32,
        "ooGTm5FcxiAJUwBNZqWG2cXmoJuWYmSwBfMh1FRBxvz5DSHMMLT",
        V2,
        895_946,
        SaleKind::Sale,
        Some(275_000_000),
        Currency::Tez,
        "tz1PXYuqZxFhgNsYJDWXhPTNDqjSzm9qWxmC",
        "tz1L3B8ZdLFTMZLvP9YG6VcDetc6aWQpHi2R",
    );
    check(
        &v32,
        "op6YbrHZ9RUsecJmn16LCBcSwaqanDfU7NnREgDLWUHw6qfoUW1",
        PARAMS,
        186_347,
        SaleKind::OfferAccept,
        Some(1_000_000),
        Currency::Wxtz,
        "tz1RH5aJvuk7Qw4UYbwdjfahWHaKsVPgwBhk",
        "tz1LpGGTVx9yNZx2yJzT26gyKknuKogBUZ4k",
    );
    check(
        &v32,
        "ooug3akPNpDBvSGYKvKP7SV6HPuZwnBCCzmqWa7D1uhApX6Hqek",
        V2,
        1_538_591,
        SaleKind::Sale,
        Some(0),
        Currency::Tez,
        "tz1U72EG4beo6ZztyheWMQ4tKR2EqjPYTBoq",
        "tz1TsVfUz9acjQ6B3kq8sWAgJgmeguZARVcz",
    );
    check(
        &v32,
        "ooNu5NVFg8Gq6SaggMFHdiLVaoEnAUGagHrnp4p1mrUx54SLr6a",
        PARAMS,
        215_615,
        SaleKind::OfferAccept,
        Some(500_000),
        Currency::Tez,
        "tz2QyvJXTDUaDcHs3aeVoxkgUDx9mrUz2pNB",
        "tz1ZXcFZr7gPts6PJwrcmczWPuNRRVZRQ3Zb",
    );
}

#[test]
fn english_auctions_sell_to_the_winner_and_unsold_lots_go_back() {
    let v1 = run("KT1XjcRq5MLAzMKQ3UHsrue2SeU2NbxUrzmU");
    check(
        &v1,
        "opEKo7Ea11ovHnCkzepY3A19jKXzt2ApESoyezkuzkrN31vEmKB",
        V1,
        35_746,
        SaleKind::Auction,
        Some(7_000_000),
        Currency::Tez,
        "tz1Nbgg46KKQVGt2itPZYARPfcgRQq11HSTp",
        "tz1Y2cxNGivA7em9w5R7ZoL1juGPmMfWBu3Y",
    );
    // Settled next to auctions of other collections: only the fxhash token is a row.
    check(
        &v1,
        "onhy1chYia8X6T5xJaje2C4rWHTYQAo4SREzh31wQLARupA6482",
        V1,
        32_642,
        SaleKind::Auction,
        Some(5_000_000),
        Currency::Tez,
        "tz1Ym9Ued9v2N2wwsrtQ52HRGGn7qDmzuUZU",
        "tz1PcF3U6KDKxLX5CNYUgh31FtrensvXS1vQ",
    );
    no_sale(&v1, "oojgXbcSHXPHXmJzV7wypD7UMZ3iJDmD62wmAeVstRGxp6AmBJr");
    assert!(v1.returns >= 1);

    let v2 = run("KT18p94vjkkHYY3nPmernmgVR7HdZFzE7NAk");
    check(
        &v2,
        "ooaDVeQAve3DxjnYiEYzJhsgF7BvTge7yNveSdGEvhq1baF857M",
        V1,
        508_088,
        SaleKind::Auction,
        Some(1_000_000),
        Currency::Wxtz,
        "tz1LMJnjC67ZCvwCK4CzWCNhF6uf4i91A6dC",
        "tz1ghu8YcCTnmUEV2xKBvSxbet3myMiWUGjo",
    );
    check(
        &v2,
        "ooA8PbHbJjmZVkKfFVcR1wXgYXTjjiwMUUhVz84nsVkPXAXqAHV",
        V1,
        516_976,
        SaleKind::Auction,
        Some(1_000_000),
        Currency::Wxtz,
        "tz1WLEQbtzExRxdTmgAPsthMCycVKzRK4Myz",
        "tz1UYavVixcaCt4cbErPT1iDutq9W4cuCEHs",
    );
    no_sale(&v2, "ooow3JbzNBpMMGXbTVtnTKT5hY3ivbhWaz8eoMH2DzoKipmb9UV");

    let v3 = run("KT18iSHoRW1iogamADWwQSDoZa3QkN4izkqj");
    check(
        &v3,
        "opNiic7pfbVEhzQwKvfup1RyMSdsCsJQbWV92mmEkp8iVEbgGQc",
        V2,
        1_568_481,
        SaleKind::Auction,
        Some(1_010_000),
        Currency::Wxtz,
        "tz1VdNZByhisD65TD8AhQK2qsfn4JvHKxWP1",
        "tz2M9sX1NesKCZVT8xyBFs7DpX4cxEeyyXdJ",
    );
    check(
        &v3,
        "oopaXfU9CSMN8LoE4UuLormcMCE7KSxWBv4Xnmtk7cnsLqDf7Rq",
        PARAMS,
        2809,
        SaleKind::Auction,
        Some(37_000_000),
        Currency::Wxtz,
        "tz1cgZ6PWKoER3gvW3jGKPHgBkRnpj8XzLm2",
        "tz1hx7d1qWCHkSDy6HXVmZRzWvpNMHxSNvuC",
    );
}

#[test]
fn dutch_auctions_sell_at_the_amount_paid() {
    let out = run("KT1QJ71jypKGgyTNtXjkCAYJZNhCKWiHuT2r");
    check(
        &out,
        "ooUia6U1pKmPCSzWuSRW42YQEReM2B1BgyssGvb7aocZ9HR2po9",
        V1,
        110_486,
        SaleKind::Auction,
        Some(209_812_500),
        Currency::Tez,
        "tz1Nbgg46KKQVGt2itPZYARPfcgRQq11HSTp",
        "tz1bz2CabKhva5Y5Q3NWf3u7pBFQF72KJNm5",
    );
    check(
        &out,
        "onkAYQgwCECn52uLmAc8pnDUUMMUS8oj9eDFrBqaDWLBPGhhJkt",
        V1,
        10_303,
        SaleKind::Auction,
        Some(2_000_216),
        Currency::Tez,
        "tz1dhw3w6YBjVTF1aw6KdRQJYnXYwezuygqX",
        "tz1Y9HHJpZBQdm6xvf9C1nzQYKAU5kifqZLs",
    );
}

#[test]
fn objkt_collection_offers_in_tez_and_wxtz() {
    let out = run("KT19NMJYBC4AiprtetMW1K2damXgGRCbJWD5");
    check(
        &out,
        "oojtQwzE2w3iL7w5L9JegmbpMg143afXxbbZQy2j8a4o4u7aSHo",
        V2,
        1_330_916,
        SaleKind::OfferAccept,
        Some(3_000_000),
        Currency::Tez,
        "tz1fyXibzW2g3Xd17YtV4RsV7hnJTDpZsxyn",
        "tz1bafXvnKt87p1yXPV3vvVpe6gyD7BCdjyh",
    );
    check(
        &out,
        "op2wPvYjT6H9Wux7dycWCUmWQ18twWBJYqAtJ731KiLEobsMvbB",
        V2,
        1_527_257,
        SaleKind::OfferAccept,
        Some(2_004_999),
        Currency::Wxtz,
        "tz1ehGMhxarDhwppx14PX1Y3eqbZ5W9Bc2Y1",
        "tz1WrHs2eZHGZaEhtebnsXPoV5KeR1PRzBxv",
    );
}

/// Mints of `issuer` over every recorded row, tickets priced from the recorded ticket mints.
fn mints(issuer: &str) -> Outcome {
    let i = ISSUER_LIST.iter().find(|i| i.address == issuer).unwrap();
    let ops = all_ops();
    let children: Vec<Op> = ops
        .iter()
        .filter(|o| o.sender == issuer && o.target == i.token_contract && o.is_call("mint"))
        .cloned()
        .collect();
    let parents: Vec<Op> = ops
        .iter()
        .filter(|o| o.target == issuer && (o.is_call("mint") || o.is_call("mint_with_ticket")))
        .cloned()
        .collect();
    let mut book = MemoryBook::default();
    for t in sales_support::tickets_json() {
        let op_id = t["transactionId"].as_u64().unwrap();
        let ticket: u64 = t["token.tokenId"].as_str().unwrap().parse().unwrap();
        let op = ops
            .iter()
            .find(|o| o.id == op_id && o.target == TICKETS)
            .unwrap();
        book.tickets.insert(ticket, ticket_price(op).unwrap());
    }
    mint_ops(i, &children, &parents, &mut book).unwrap()
}

fn mint<'a>(out: &'a Outcome, hash: &str) -> &'a NewSale {
    let found: Vec<&NewSale> = out.sales.iter().filter(|s| s.op_hash == hash).collect();
    assert_eq!(found.len(), 1, "{hash}");
    assert_eq!(found[0].kind, SaleKind::Mint);
    assert_eq!(found[0].seller, None);
    found[0]
}

#[test]
fn mints_are_priced_by_the_issuer_call_amount() {
    let v2 = mints("KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv");
    let paid = mint(&v2, "oohrT9bKRMBUEWrABoGUmweJYgmXezELTFEfhQA6p6DYxDe5yr3");
    assert_eq!(
        (paid.contract.as_str(), paid.token_id, paid.price_mutez),
        (V2, 590_146, Some(1_000_000))
    );
    assert_eq!(paid.buyer, "tz1ZRjuE2STA75rzXr78GcXNUZhYubwxaTYd");
    assert_eq!(
        paid.marketplace_contract,
        "KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv"
    );
    let free = mint(&v2, "opAJmGfeb34YT6f5NTfNfKYdQKD7UVPNvT2ABvgc2Bm3BjV41cC");
    assert_eq!((free.token_id, free.price_mutez), (1_089_146, Some(0)));

    let v1 = mints("KT1AEVuykWeuuFX7QkEAMNtffzwhe1Z98hJS");
    let first = mint(&v1, "oo7zgsEKRTPEYH2Q5x4pBFWnbeR2mJS8vEGx9RcGexABu3QvdvV");
    assert_eq!(
        (first.contract.as_str(), first.token_id, first.price_mutez),
        (V1, 2029, Some(100_000))
    );
    assert!(v1.issues.is_empty() && v2.issues.is_empty());
}

#[test]
fn a_ticket_mint_is_priced_by_the_ticket_purchase() {
    let out = mints("KT1Xpmp15KfqoePNW9HczFmqaGNHwadV2a3b");
    let m = mint(&out, "opMx9qaD6AWjqgDkLxiag1dW8o8vPQ9MMWejcMHfbqPoVNysx1d");
    assert_eq!(
        (m.contract.as_str(), m.token_id, m.price_mutez),
        (PARAMS, 16_408, Some(16_000_000))
    );
    assert_eq!(m.buyer, "tz1NkoBm4akiLs1ADTe6dLuae1EWWbJnkYps");
    // Buying a ticket mints no token.
    assert!(
        out.sales
            .iter()
            .all(|s| s.op_hash != "oo74HmHAvkgHXWm5LAuuvsTKuHzvwUZ563Q4xa7vHkyscRQriQ9")
    );
    // Without the ticket the mint keeps no price and says why.
    let i = &ISSUER_LIST[3];
    let ops = all_ops();
    let children: Vec<Op> = ops
        .iter()
        .filter(|o| o.sender == i.address && o.target == PARAMS && o.is_call("mint"))
        .cloned()
        .collect();
    let parents: Vec<Op> = ops
        .iter()
        .filter(|o| o.target == i.address)
        .cloned()
        .collect();
    let bare = mint_ops(i, &children, &parents, &mut MemoryBook::default()).unwrap();
    assert_eq!(bare.sales[0].price_mutez, None);
    assert_eq!(bare.issues[0].reason, "ticket_unknown");
}

#[test]
fn a_mint_without_its_issuer_call_is_flagged_not_priced() {
    let i = &ISSUER_LIST[2];
    let children: Vec<Op> = all_ops()
        .into_iter()
        .filter(|o| o.sender == i.address && o.target == V2 && o.is_call("mint"))
        .collect();
    let out = mint_ops(i, &children, &[], &mut MemoryBook::default()).unwrap();
    assert_eq!(out.sales.len(), children.len());
    assert!(out.sales.iter().all(|s| s.price_mutez.is_none()));
    assert!(
        out.issues
            .iter()
            .all(|i| i.reason == "mint_without_issuer_call")
    );
}

#[test]
fn every_recorded_market_row_is_explained() {
    for m in seeded_archive::sales::markets::MARKETS.iter() {
        let out = run(m.address);
        let unexpected: Vec<_> = out
            .issues
            .iter()
            .filter(|i| i.reason != "bulk_unsplittable" && i.reason != "amount_differs_from_payout")
            .collect();
        assert!(unexpected.is_empty(), "{}: {unexpected:?}", m.name);
    }
}
