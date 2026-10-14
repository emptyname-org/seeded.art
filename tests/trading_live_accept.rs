//! Live mainnet checks of offer acceptance with the deployed platform fee. Ignored by default:
//! `cargo test --test trading_live_accept -- --ignored --nocapture`. Each case prepares and
//! simulates an accept as the edition's current owner; nothing is signed or broadcast.

use seeded_archive::trading::{Action, Kind, Token, TradingState, http::Http, types::PlatformFee};

const FEE_ADDRESS: &str = "tz1hCUrKAsam1ZBEVWyZnrzuQCwNhywUWrz6";
const FXHASH_TREASURY: &str = "KT1P2BXYb894MekrCcSrnidzQYPVqitLoVLc";

/// Active fxhash v2 offers and the current owner of a matching edition (TzKT, 2026-10-07).
const CASES: &[(Kind, u64, &str, u64, &str)] = &[
    (
        Kind::Token,
        24104,
        "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE",
        130810,
        "tz1cjHCorsyNqN3cWWKoYpzycBJmpifyoWd6",
    ),
    (
        Kind::Token,
        55799,
        "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi",
        1274273,
        "tz1dmo6rwvv1LZCPaCuJK3MVZQywo1Tk8pD1",
    ),
    (
        Kind::Token,
        79581,
        "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE",
        132,
        "tz1KzVdMTyLDeeVcsR5mWPAiXWcUSDVLetBk",
    ),
    (
        Kind::Collection,
        116662,
        "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi",
        784640,
        "tz1N1vuWBsDtGMyj3No8NfeJdCBFu8v6gvSX",
    ),
    (
        Kind::Collection,
        17567,
        "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE",
        145971,
        "tz1WSjHnc7Kc7Ccj1AjxNN9Kqbb5pCqbVhYy",
    ),
];

#[tokio::test]
#[ignore = "explicit public mainnet reads and read-only simulation; never injects"]
async fn live_accepts_pay_the_platform_fee_to_the_configured_address() {
    let transport = Http::new(
        "https://prod.tcinfra.net/rpc/mainnet",
        "https://api.tzkt.io/v1",
    )
    .unwrap();
    let fee = PlatformFee {
        bps: 250,
        address: Some(FEE_ADDRESS.into()),
    };
    let state = TradingState::with_transport_and_fee(transport, fee).unwrap();
    let mut passed = 0;
    for &(kind, id, contract, token_id, owner) in CASES {
        let action = Action::Accept {
            kind,
            id,
            token: Token {
                contract: contract.into(),
                token_id,
            },
        };
        let review = match state
            .prepare(owner, "live-accept-probe", vec![action])
            .await
        {
            Ok(review) => review,
            Err(error) => {
                println!("{kind:?} offer {id}: not preparable now: {error}");
                continue;
            }
        };
        let fees = review.simulation.accept_fees.clone().expect("accept fees");
        let price = review.items[0].price_mutez;
        assert_eq!(fees.price_mutez, price);
        assert_eq!(fees.platform_fee_address, FEE_ADDRESS);
        assert_eq!(fees.platform_fee_bps, 250);
        assert_eq!(fees.platform_fee_mutez, price * 250 / 10_000);
        assert!(fees.fxhash_fee_mutez > 0, "fxhash takes its fee");
        let fee_transfer = review
            .simulation
            .operations
            .iter()
            .find(|operation| operation.destination == FEE_ADDRESS)
            .expect("platform fee transfer in the group");
        assert_eq!(fee_transfer.source, owner);
        assert_eq!(
            fee_transfer.amount,
            (fees.platform_fee_mutez - fees.platform_fee_returned_mutez).to_string()
        );
        assert!(review.simulation.gas > 0);
        assert!(
            review
                .simulation
                .operations
                .iter()
                .all(|operation| operation.destination != FXHASH_TREASURY),
            "the seller sends nothing to fxhash directly; the marketplace pays its fee"
        );
        println!(
            "{kind:?} offer {id}: price {} tez, royalties {}, fxhash fee {}, platform fee {} (returned {}), seller net {}, {} operations, gas {}",
            price as f64 / 1e6,
            fees.royalties_mutez as f64 / 1e6,
            fees.fxhash_fee_mutez as f64 / 1e6,
            fees.platform_fee_mutez as f64 / 1e6,
            fees.platform_fee_returned_mutez as f64 / 1e6,
            fees.seller_net_mutez as f64 / 1e6,
            review.simulation.operations.len(),
            review.simulation.gas
        );
        passed += 1;
    }
    assert!(
        passed >= 3,
        "at least three live offers simulated; {passed} did"
    );
}
