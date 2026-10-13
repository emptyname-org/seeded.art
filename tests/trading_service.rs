use seeded_archive::trading::{
    Action, Kind, TradingError, TradingState,
    chain::normalized,
    http::{Endpoint, Reply, Transport},
    types::{FXHASH_TREASURY, PlatformFee, TOKENS, V1, V2, operation_group},
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
const ACTOR: &str = "tz1P719ymGCtNwavZKh2LmhMs53dGsEMzjnm";
const OTHER: &str = "tz1ewF2P3erZJSP8m35QiNEKGtwj7qsBhF8z";
const FEE_ADDRESS: &str = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
fn accept(kind: Kind) -> Action {
    Action::Accept {
        kind,
        id: 7,
        token: seeded_archive::trading::Token {
            contract: TOKENS[2].into(),
            token_id: 1,
        },
    }
}

#[tokio::test]
async fn accepts_pay_rounded_per_token_fee_after_operator_removal_and_confirm_it() {
    for kind in [Kind::Token, Kind::Collection] {
        let m = Mock::new();
        m.price.store(1_000_039, Ordering::SeqCst);
        let s = state(&m);
        let r = s.prepare(OTHER, "s", vec![accept(kind)]).await.unwrap();
        let operations = &r.simulation.operations;
        assert_eq!(operations.len(), 4);
        assert_eq!(operations[2].parameters["value"][0]["prim"], "Right");
        let fee = operations.last().unwrap();
        assert_eq!(fee.source, OTHER);
        assert_eq!(fee.destination, FEE_ADDRESS);
        assert_eq!(fee.amount, "25000");
        assert!(fee.parameters.is_null());
        assert!(
            serde_json::to_value(fee)
                .unwrap()
                .get("parameters")
                .is_none()
        );
        let split = r.simulation.accept_fees.as_ref().unwrap();
        assert_eq!(split.price_mutez, 1_000_039);
        assert_eq!(split.royalties_mutez, 150_004);
        assert_eq!(split.fxhash_fee_mutez, 25_000);
        assert_eq!(split.platform_fee_mutez, 25_000);
        assert_eq!(split.seller_net_mutez, 800_035);
        if kind == Kind::Collection {
            assert_eq!(r.items[0].remaining, 12);
        }
        let ready = s.check(OTHER, "s", &r.ticket).await.unwrap();
        assert_eq!(ready.operations.last().unwrap().amount, "25000");
        s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
    }
}

#[tokio::test]
async fn rounded_zero_has_no_minimum_but_still_requires_configured_recipient() {
    for price in [0, 1, 39, 40] {
        let m = Mock::new();
        m.price.store(price, Ordering::SeqCst);
        let r = state(&m)
            .prepare(OTHER, "s", vec![accept(Kind::Token)])
            .await
            .unwrap();
        assert_eq!(
            r.simulation.accept_fees.unwrap().platform_fee_mutez,
            price / 40
        );
        assert_eq!(
            r.simulation.operations.len(),
            if price < 40 { 3 } else { 4 }
        );
    }
    let m = Mock::new();
    let s = TradingState::with_transport(Arc::new(MockTransport(m.clone())));
    assert!(matches!(
        s.prepare(OTHER, "s", vec![accept(Kind::Token)]).await,
        Err(TradingError::FeeUnconfigured)
    ));
    assert!(m.calls.lock().unwrap().is_empty());
    let cancelled = s.prepare(ACTOR, "s", vec![action()]).await.unwrap();
    assert!(cancelled.simulation.accept_fees.is_none());
    assert!(
        cancelled
            .simulation
            .operations
            .iter()
            .all(|op| op.amount == "0" && op.destination == V2)
    );
}

#[tokio::test]
async fn cancel_all_has_no_platform_transfer_with_or_without_fee_configuration() {
    for configured in [false, true] {
        let m = Mock::new();
        let s = if configured {
            state(&m)
        } else {
            TradingState::with_transport(Arc::new(MockTransport(m.clone())))
        };
        let actions = vec![
            action(),
            Action::Cancel {
                marketplace: V2.into(),
                kind: Kind::Collection,
                id: 7,
            },
        ];
        let review = s.prepare(ACTOR, "s", actions).await.unwrap();
        assert_eq!(review.simulation.operations.len(), 2);
        assert!(review.simulation.accept_fees.is_none());
        assert!(
            review
                .simulation
                .operations
                .iter()
                .all(|op| op.amount == "0" && op.destination == V2)
        );
    }
}

#[tokio::test]
async fn changed_price_requires_new_fee_review_and_failed_fee_transfer_releases_retry() {
    let m = Mock::new();
    let s = state(&m);
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    m.price.store(2_000_001, Ordering::SeqCst);
    assert!(matches!(
        s.check(OTHER, "s", &r.ticket).await,
        Err(TradingError::Changed)
    ));
    let fresh = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    assert_eq!(
        fresh.simulation.accept_fees.unwrap().platform_fee_mutez,
        50_000
    );
    s.discard_review(OTHER, "s", &fresh.ticket).unwrap();
    m.simulation_mode.store(3, Ordering::SeqCst);
    assert!(matches!(
        s.prepare(OTHER, "s", vec![accept(Kind::Token)]).await,
        Err(TradingError::Simulation)
    ));
    m.simulation_mode.store(0, Ordering::SeqCst);
    assert!(
        s.prepare(OTHER, "s", vec![accept(Kind::Token)])
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn seller_directed_royalty_is_included_in_net_and_unit_fee_confirms() {
    let m = Mock::new();
    m.royalty_to_seller.store(true, Ordering::SeqCst);
    let s = state(&m);
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    let fees = r.simulation.accept_fees.unwrap();
    assert_eq!(fees.royalties_mutez, 45_000);
    assert_eq!(fees.seller_net_mutez, 905_000);
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    let mut canonical = m.last.lock().unwrap().clone();
    canonical.last_mut().unwrap()["parameters"] =
        json!({"entrypoint":"default","value":{"prim":"Unit"}});
    *m.receipt.lock().unwrap() = Some(json!(canonical));
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
}

#[tokio::test]
async fn applied_accept_without_reviewed_fee_is_terminal_and_address_bound() {
    for alteration in ["omit", "amount", "destination", "parameters"] {
        let m = Mock::new();
        let s = state(&m);
        let r = s
            .prepare(OTHER, "s", vec![accept(Kind::Token)])
            .await
            .unwrap();
        s.check(OTHER, "s", &r.ticket).await.unwrap();
        let mut rows = m.last.lock().unwrap().clone();
        if alteration == "omit" {
            rows.pop();
        } else {
            rows.last_mut().unwrap()[alteration] = match alteration {
                "amount" => json!("1"),
                "destination" => json!(ACTOR),
                _ => json!({"entrypoint":"steal","value":{"prim":"Unit"}}),
            };
        }
        *m.receipt.lock().unwrap() = Some(json!(rows));
        assert!(
            matches!(
                s.confirm(OTHER, "s", &r.ticket, &op_hash()).await,
                Err(TradingError::FeeEvasion)
            ),
            "{alteration}"
        );
        let calls = m.calls.lock().unwrap().len();
        m.fail.store(true, Ordering::SeqCst);
        assert!(matches!(
            s.confirm(OTHER, "new-session", &r.ticket, &op_hash()).await,
            Err(TradingError::FeeEvasion)
        ));
        assert!(matches!(
            s.confirm(ACTOR, "s", &r.ticket, &op_hash()).await,
            Err(TradingError::Unauthorized)
        ));
        assert!(matches!(
            s.confirm(OTHER, "s", &r.ticket, &format!("o{}", "b".repeat(50)))
                .await,
            Err(TradingError::Changed)
        ));
        assert_eq!(m.calls.lock().unwrap().len(), calls);
    }
}

#[tokio::test]
async fn unrelated_failed_or_fully_paid_groups_are_not_fee_evasion_and_allow_retry() {
    for alteration in [
        "source",
        "counter",
        "parameters",
        "failed",
        "paid",
        "unavailable",
    ] {
        let m = Mock::new();
        let s = state(&m);
        let r = s
            .prepare(OTHER, "s", vec![accept(Kind::Token)])
            .await
            .unwrap();
        s.check(OTHER, "s", &r.ticket).await.unwrap();
        let good = m.last.lock().unwrap().clone();
        let mut rows = good.clone();
        rows.pop();
        match alteration {
            "source" => rows[1]["source"] = json!(ACTOR),
            "counter" => rows[1]["counter"] = json!("123"),
            "parameters" => rows[1]["parameters"]["value"] = json!({"int":"987"}),
            "failed" => rows[1]["metadata"]["operation_result"]["status"] = json!("backtracked"),
            "paid" => {
                rows.push(good.last().unwrap().clone());
                rows.last_mut().unwrap()["amount"] = json!("25001");
            }
            _ => m.fail.store(true, Ordering::SeqCst),
        }
        *m.receipt.lock().unwrap() = Some(json!(rows));
        let result = s.confirm(OTHER, "s", &r.ticket, &op_hash()).await;
        if alteration == "paid" {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert!(
                matches!(
                    result,
                    Err(TradingError::Changed | TradingError::Unavailable)
                ),
                "{alteration}: {result:?}"
            );
        }
        m.fail.store(false, Ordering::SeqCst);
        *m.receipt.lock().unwrap() = Some(json!(good));
        s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
    }
}

#[tokio::test]
async fn contract_fee_recipient_requires_pinned_unit_default_and_rechecks_after_review() {
    let m = Mock::new();
    let s = TradingState::with_transport_and_fee(
        Arc::new(MockTransport(m.clone())),
        PlatformFee {
            bps: 250,
            address: Some(FXHASH_TREASURY.into()),
        },
    )
    .unwrap();
    m.fee_entrypoint_mode.store(1, Ordering::SeqCst);
    assert!(matches!(
        s.prepare(OTHER, "s", vec![accept(Kind::Token)]).await,
        Err(TradingError::FeeRecipient)
    ));
    m.fee_entrypoint_mode.store(2, Ordering::SeqCst);
    assert!(matches!(
        s.prepare(OTHER, "s", vec![accept(Kind::Token)]).await,
        Err(TradingError::Unavailable)
    ));
    m.fee_entrypoint_mode.store(0, Ordering::SeqCst);
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    assert!(m.calls.lock().unwrap().iter().any(|p| p
        == &format!(
            "/chains/main/blocks/{}/context/contracts/{FXHASH_TREASURY}/entrypoints/default",
            block()
        )));
    m.fee_entrypoint_mode.store(1, Ordering::SeqCst);
    assert!(matches!(
        s.check(OTHER, "s", &r.ticket).await,
        Err(TradingError::FeeRecipient)
    ));
}

#[test]
fn fee_math_checks_addresses_and_does_not_overflow_large_mutez() {
    let tz4 = bs58::encode([[6, 161, 166].as_slice(), &[7; 20]].concat())
        .with_check()
        .into_string();
    assert!(!seeded_archive::trading::types::valid_fee_address(&tz4));
    assert!(
        PlatformFee {
            bps: u16::MAX,
            address: Some(FEE_ADDRESS.into())
        }
        .amount(u64::MAX)
        .is_err()
    );
    let fee = PlatformFee {
        bps: 250,
        address: Some(FEE_ADDRESS.into()),
    };
    assert_eq!(
        fee.amount(u64::MAX).unwrap(),
        (u128::from(u64::MAX) * 250 / 10_000) as u64
    );
    for prefix in [[6, 161, 159], [6, 161, 161], [6, 161, 164], [2, 90, 121]] {
        let address = bs58::encode([prefix.as_slice(), &[7; 20]].concat())
            .with_check()
            .into_string();
        assert!(seeded_archive::trading::types::valid_fee_address(&address));
    }
    assert!(
        PlatformFee {
            bps: 1001,
            address: Some(FEE_ADDRESS.into())
        }
        .validate()
        .is_err()
    );
}

#[tokio::test]
async fn fee_self_transfer_and_contract_return_are_credited_in_seller_net() {
    for (recipient, refund, returned, net) in [
        (OTHER, 0, 25_000, 825_000),
        (FXHASH_TREASURY, 12_500, 12_500, 812_500),
    ] {
        let m = Mock::new();
        m.fee_returned.store(refund, Ordering::SeqCst);
        let s = TradingState::with_transport_and_fee(
            Arc::new(MockTransport(m.clone())),
            PlatformFee {
                bps: 250,
                address: Some(recipient.into()),
            },
        )
        .unwrap();
        let r = s
            .prepare(OTHER, "s", vec![accept(Kind::Token)])
            .await
            .unwrap();
        let split = r.simulation.accept_fees.unwrap();
        assert_eq!(split.platform_fee_mutez, 25_000);
        assert_eq!(split.platform_fee_returned_mutez, returned);
        assert_eq!(split.seller_net_mutez, net);
        assert_eq!(
            r.simulation.operations.last().unwrap().destination,
            recipient
        );
        s.check(OTHER, "s", &r.ticket).await.unwrap();
    }
}
fn block() -> String {
    format!("B{}", "a".repeat(50))
}
fn op_hash() -> String {
    format!("o{}", "a".repeat(50))
}
fn action() -> Action {
    Action::Cancel {
        marketplace: V2.into(),
        kind: Kind::Token,
        id: 7,
    }
}
fn now() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut days = seconds / 86400;
    let mut year = 1970;
    loop {
        let n = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
            366
        } else {
            365
        };
        if days < n {
            break;
        }
        days -= n;
        year += 1;
    }
    let mut month = 1;
    for n in [
        31,
        if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
            29
        } else {
            28
        },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ] {
        if days < n {
            break;
        }
        days -= n;
        month += 1;
    }
    format!(
        "{year:04}-{month:02}-{:02}T{:02}:{:02}:{:02}Z",
        days + 1,
        seconds % 86400 / 3600,
        seconds % 3600 / 60,
        seconds % 60
    )
}
struct Mock {
    price: AtomicU64,
    height: AtomicU64,
    audit_rows: Mutex<Vec<Value>>,
    started: tokio::sync::Notify,
    simulation_mode: AtomicU64,
    changed: AtomicBool,
    bad_code: AtomicBool,
    reorg: AtomicBool,
    // A node behind the indexer: lookups of blocks below the head answer 404 (Changed).
    lagging: AtomicBool,
    // The next N head reads come from a node one block behind.
    behind: AtomicU64,
    // Sender of the looked-up operation; `None` for an operation the indexer does not know.
    sender: Mutex<Option<String>>,
    // The looked-up operation's status ("applied" or "failed").
    op_status: Mutex<&'static str>,
    fail: AtomicBool,
    stall: AtomicBool,
    hint_mode: AtomicU64,
    royalty_to_seller: AtomicBool,
    fee_returned: AtomicU64,
    fee_entrypoint_mode: AtomicU64,
    owner: Mutex<String>,
    unheld: AtomicBool,
    counter: AtomicU64,
    receipt: Mutex<Option<Value>>,
    last: Mutex<Vec<Value>>,
    calls: Mutex<Vec<String>>,
}
impl Mock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            price: AtomicU64::new(1_000_000),
            height: AtomicU64::new(20_000_000),
            audit_rows: Mutex::new(vec![]),
            started: tokio::sync::Notify::new(),
            simulation_mode: AtomicU64::new(0),
            changed: AtomicBool::new(false),
            bad_code: AtomicBool::new(false),
            reorg: AtomicBool::new(false),
            lagging: AtomicBool::new(false),
            behind: AtomicU64::new(0),
            sender: Mutex::new(Some(OTHER.into())),
            op_status: Mutex::new("applied"),
            fail: AtomicBool::new(false),
            stall: AtomicBool::new(false),
            hint_mode: AtomicU64::new(0),
            royalty_to_seller: AtomicBool::new(false),
            fee_returned: AtomicU64::new(0),
            fee_entrypoint_mode: AtomicU64::new(0),
            owner: Mutex::new(ACTOR.into()),
            unheld: AtomicBool::new(false),
            counter: AtomicU64::new(4),
            receipt: Mutex::new(None),
            last: Mutex::new(vec![]),
            calls: Mutex::new(vec![]),
        })
    }
    fn response(
        &self,
        endpoint: Endpoint,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, TradingError> {
        self.calls.lock().unwrap().push(path.into());
        if self.fail.load(Ordering::SeqCst) {
            return Err(TradingError::Unavailable);
        }
        let keys: Value =
            serde_json::from_str(include_str!("../src/trading/fixtures/key_hashes.json")).unwrap();
        match endpoint {
            Endpoint::Indexer => {
                if path == "/head" {
                    return Ok(json!({"level":self.height.load(Ordering::SeqCst),"synced":true}));
                }
                if path.starts_with("/blocks/") {
                    return Ok(json!("2024-01-01T00:00:00Z"));
                }
                if path.starts_with("/tokens/balances") {
                    if self.unheld.load(Ordering::SeqCst) {
                        return Ok(json!([]));
                    }
                    return Ok(json!([{"balance":"1"}]));
                }
                if path.starts_with("/operations/transactions?") {
                    return Ok(json!(self.audit_rows.lock().unwrap().clone()));
                }
                if path.starts_with("/operations/transactions/") {
                    let status = *self.op_status.lock().unwrap();
                    if path.contains("status=applied") && status != "applied" {
                        return Ok(json!([]));
                    }
                    return Ok(match self.sender.lock().unwrap().clone() {
                        Some(sender) => {
                            json!([{"level":19_999_997,"status":status,"sender":{"address":sender}}])
                        }
                        None => json!([]),
                    });
                }
                let parts: Vec<_> = path.split('/').collect();
                if parts.len() == 5 && parts[1] == "bigmaps" {
                    let key = parts[4].split('?').next().unwrap();
                    let level = if parts[2] == "149790" {
                        let pins: Value =
                            serde_json::from_str(include_str!("../src/trading/pins.json")).unwrap();
                        pins[key]["level"].as_u64().unwrap()
                    } else {
                        1
                    };
                    return Ok(
                        json!({"hash":keys[key],"active":!self.changed.load(Ordering::SeqCst),"firstLevel":1,"lastLevel":level}),
                    );
                }
            }
            Endpoint::Rpc => {
                if path == "/chains/main/chain_id" {
                    return Ok(json!("NetXdQprcVkpaWU"));
                }
                if path.ends_with("/header") {
                    let behind = self
                        .behind
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok();
                    let level = self.height.load(Ordering::SeqCst) - u64::from(behind);
                    return Ok(json!({"level":level,"timestamp":now()}));
                }
                if path.ends_with("/hash")
                    && self.lagging.load(Ordering::SeqCst)
                    && path
                        .trim_start_matches("/chains/main/blocks/")
                        .split('/')
                        .next()
                        .and_then(|level| level.parse::<u64>().ok())
                        .is_some_and(|level| level < self.height.load(Ordering::SeqCst))
                {
                    return Err(TradingError::Changed);
                }
                if path.ends_with("/hash") {
                    return Ok(json!(
                        if path.contains('~') && self.reorg.load(Ordering::SeqCst) {
                            format!("B{}", "b".repeat(50))
                        } else {
                            block()
                        }
                    ));
                }
                if path.ends_with("/script") {
                    let mut v: Value = serde_json::from_str(include_str!(
                        "../src/trading/fixtures/v2_script.json"
                    ))
                    .unwrap();
                    if self.bad_code.load(Ordering::SeqCst) {
                        v["code"][0] = json!({"prim":"oops"});
                    }
                    return Ok(v);
                }
                if path.contains("/context/big_maps/") {
                    let parts: Vec<_> = path.split('/').collect();
                    let map = parts[7];
                    let hash = parts[8];
                    let key = keys
                        .as_object()
                        .unwrap()
                        .iter()
                        .find(|(_, v)| v.as_str() == Some(hash))
                        .map(|(k, _)| k.as_str())
                        .unwrap();
                    return Ok(match map {
                        "149790" => {
                            let rows: Vec<Value> = serde_json::from_str(include_str!(
                                "../src/trading/fixtures/lambdas.json"
                            ))
                            .unwrap();
                            rows.into_iter().find(|r| r["key"]["int"] == key).unwrap()["value"]
                                .clone()
                        }
                        "149784" => json!({"prim":"True"}),
                        "149786" => {
                            let i: usize = key.parse().unwrap();
                            json!({"prim":"Pair","args":[{"string":TOKENS[i]},{"string":seeded_archive::trading::types::VIEWS[i]}]})
                        }
                        "149787" => match self.hint_mode.load(Ordering::SeqCst) {
                            0 => return Err(TradingError::Changed),
                            2 => return Err(TradingError::Unavailable),
                            _ => {
                                json!({"prim":"Pair","args":[{"prim":"Pair","args":[{"int":"1"},{"int":"2"}]},{"int":"1000000"},{"string":OTHER}]})
                            }
                        },
                        "149789" => {
                            json!({"prim":"Pair","args":[{"string":self.owner.lock().unwrap().clone()},{"prim":"Pair","args":[{"int":"1"},{"int":"2"}]},{"int":self.price.load(Ordering::SeqCst).to_string()}]})
                        }
                        "149783" => {
                            json!({"prim":"Pair","args":[{"int":"12"},{"string":self.owner.lock().unwrap().clone()},{"int":"29437"},{"int":self.price.load(Ordering::SeqCst).to_string()}]})
                        }
                        _ => panic!("unexpected map {map}"),
                    });
                }
                if path.ends_with("/entrypoints/default") {
                    return match self.fee_entrypoint_mode.load(Ordering::SeqCst) {
                        1 => Ok(json!({"prim":"nat"})),
                        2 => Err(TradingError::Unavailable),
                        3 => Ok(json!({"prim":"unit","annots":["%default",":label"]})),
                        4 => Err(TradingError::Changed),
                        _ => Ok(json!({"prim":"unit"})),
                    };
                }
                if path.ends_with("/manager_key") {
                    return Ok(json!("revealed"));
                }
                if path.ends_with("/counter") {
                    return Ok(json!(self.counter.load(Ordering::SeqCst).to_string()));
                }
                if path.ends_with("/mempool/filter") {
                    return Ok(
                        json!({"minimal_fees":"100","minimal_nanotez_per_gas_unit":["100","1"],"minimal_nanotez_per_byte":["1000","1"]}),
                    );
                }
                if path.ends_with("/constants") {
                    return Ok(json!({"cost_per_byte":"250"}));
                }
                if path.ends_with("/simulate_operation") {
                    let mut contents = body.unwrap()["operation"]["contents"]
                        .as_array()
                        .unwrap()
                        .clone();
                    for c in &mut contents {
                        c["metadata"] = json!({"operation_result":{"status":"applied","consumed_milligas":"1000000","paid_storage_size_diff":"0"}});
                        if c["parameters"].is_null() && self.fee_returned.load(Ordering::SeqCst) > 0
                        {
                            c["metadata"]["internal_operation_results"] = json!([{"kind":"transaction","source":c["destination"],"destination":c["source"],"amount":self.fee_returned.load(Ordering::SeqCst).to_string(),"result":{"status":"applied","consumed_milligas":"100000","paid_storage_size_diff":"0"}}]);
                        }
                        if matches!(
                            c["parameters"]["entrypoint"].as_str(),
                            Some("offer_accept" | "collection_offer_accept")
                        ) {
                            let price = self.price.load(Ordering::SeqCst);
                            let pool = price * 150 / 1000;
                            let (first, second) = (pool * 700 / 1000, pool * 300 / 1000);
                            let fxhash = price * 25 / 1000;
                            let seller = price - first - second - fxhash;
                            let royalty_recipient = if self.royalty_to_seller.load(Ordering::SeqCst)
                            {
                                c["source"].as_str().unwrap()
                            } else {
                                ACTOR
                            };
                            let paid = |destination: &str, amount: u64| json!({"kind":"transaction","source":V2,"destination":destination,"amount":amount.to_string(),"result":{"status":"applied","consumed_milligas":"100000","paid_storage_size_diff":"0"}});
                            c["metadata"]["internal_operation_results"] = json!([
                                paid(royalty_recipient, first),
                                paid(FEE_ADDRESS, second),
                                paid(FXHASH_TREASURY, fxhash),
                                paid(c["source"].as_str().unwrap(), seller)
                            ]);
                        }
                    }
                    if self.simulation_mode.load(Ordering::SeqCst) == 1 {
                        contents[0]["metadata"]["operation_result"]["status"] = json!("failed");
                    }
                    if self.simulation_mode.load(Ordering::SeqCst) == 2 {
                        contents[0]["metadata"]["operation_result"]["consumed_milligas"] =
                            json!("9000000000");
                    }
                    if self.simulation_mode.load(Ordering::SeqCst) == 3 {
                        contents.last_mut().unwrap()["metadata"]["operation_result"]["status"] =
                            json!("failed");
                    }
                    *self.last.lock().unwrap() = contents.clone();
                    return Ok(json!({"contents":contents}));
                }
                if path.ends_with("/forge/operations") {
                    return Ok(json!("ab".repeat(120)));
                }
                if path.ends_with("/operation_hashes/3") {
                    return Ok(json!([op_hash()]));
                }
                if path.ends_with("/operations/3/0") {
                    let given = self.receipt.lock().unwrap().clone();
                    let explicit = given.is_some();
                    let mut contents =
                        given.unwrap_or_else(|| json!(self.last.lock().unwrap().clone()));
                    let status = *self.op_status.lock().unwrap();
                    // An explicit test group is returned as given.
                    if status != "applied" && !explicit {
                        for c in contents.as_array_mut().into_iter().flatten() {
                            c["metadata"]["operation_result"]["status"] = json!(status);
                        }
                    }
                    return Ok(json!({"hash":op_hash(),"contents":contents}));
                }
            }
        }
        panic!("unexpected path {path}")
    }
}
struct MockTransport(Arc<Mock>);
impl Transport for MockTransport {
    fn request(&self, endpoint: Endpoint, path: String, body: Option<Value>) -> Reply {
        let mock = self.0.clone();
        Box::pin(async move {
            if mock.stall.load(Ordering::SeqCst) {
                mock.started.notify_one();
                std::future::pending::<()>().await;
            }
            mock.response(endpoint, &path, body)
        })
    }
}
fn state(mock: &Arc<Mock>) -> TradingState {
    TradingState::with_transport_and_fee(
        Arc::new(MockTransport(mock.clone())),
        PlatformFee {
            bps: 250,
            address: Some(FEE_ADDRESS.into()),
        },
    )
    .unwrap()
}
#[test]
fn builders_match_interpreter_cases_and_refuse_recipient_fields() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("../src/trading/fixtures/builder_cases.json")).unwrap();
    for case in cases {
        let action: Action = serde_json::from_value(case["action"].clone()).unwrap();
        let calls = operation_group(case["actor"].as_str().unwrap(), &action).unwrap();
        let expected = case["calls"].as_array().unwrap();
        assert_eq!(calls.len(), expected.len());
        for (call, expected) in calls.iter().zip(expected) {
            assert_eq!(call.source, expected["source"]);
            assert_eq!(call.destination, expected["destination"]);
            assert_eq!(call.amount, "0");
            assert_eq!(
                call.parameters["entrypoint"],
                expected["parameters"]["entrypoint"]
            );
            assert_eq!(
                normalized(&call.parameters["value"]).unwrap(),
                normalized(&expected["parameters"]["value"]).unwrap()
            );
        }
    }
    assert!(
        serde_json::from_value::<Action>(
            json!({"action":"cancel","marketplace":V2,"kind":"token","id":7,"recipient":OTHER})
        )
        .is_err()
    );
    assert!(
        Action::Cancel {
            marketplace: V1.into(),
            kind: Kind::Token,
            id: 7
        }
        .validate()
        .is_err()
    );
}
#[tokio::test]
async fn prepare_check_is_session_bound_and_consumed_once() {
    let m = Mock::new();
    let s = state(&m);
    let review = s.prepare(ACTOR, "session", vec![action()]).await.unwrap();
    assert_eq!(review.items[0].price_mutez, 1_000_000);
    assert!(matches!(
        s.check(ACTOR, "other", &review.ticket).await,
        Err(TradingError::Unauthorized)
    ));
    let ready = s.check(ACTOR, "session", &review.ticket).await.unwrap();
    assert_eq!(ready.operations[0].source, ACTOR);
    assert_eq!(ready.operations[0].counter, "5");
    assert!(matches!(
        s.check(ACTOR, "session", &review.ticket).await,
        Err(TradingError::Expired)
    ));
    assert!(matches!(
        s.prepare(ACTOR, "session", vec![action()]).await,
        Err(TradingError::SigningPending(_))
    ));
    s.confirm(ACTOR, "new-session", &review.ticket, &op_hash())
        .await
        .unwrap();
    m.fail.store(true, Ordering::SeqCst);
    s.confirm(ACTOR, "session", &review.ticket, &op_hash())
        .await
        .unwrap();
    assert!(matches!(
        s.confirm(OTHER, "other-session", &review.ticket, &op_hash())
            .await,
        Err(TradingError::Unauthorized)
    ));
    assert!(matches!(
        s.confirm(
            ACTOR,
            "session",
            &review.ticket,
            &format!("o{}", "b".repeat(50))
        )
        .await,
        Err(TradingError::Changed)
    ));
    m.fail.store(false, Ordering::SeqCst);
    assert!(s.prepare(ACTOR, "session", vec![action()]).await.is_ok());
}
#[tokio::test]
async fn invalid_confirmations_do_not_drain_admission_and_restart_receipts_expire() {
    let m = Mock::new();
    let s = state(&m);
    let missing = "b".repeat(48);
    // A missing receipt is checked against the chain once (the operation here belongs to
    // another address); repeats of that hash answer pending without upstream calls.
    let mut looked_up = 0;
    for n in 1..=1100u32 {
        match s
            .confirm_from(
                ACTOR,
                "current",
                &missing,
                &op_hash(),
                n.to_be_bytes().into(),
            )
            .await
        {
            Err(TradingError::Unauthorized) => looked_up += 1,
            Err(TradingError::Pending) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(looked_up, 1);
    assert_eq!(m.calls.lock().unwrap().len(), 1, "one indexer lookup");
    // Ten distinct hashes per address, then a retryable limit instead of a false expiry.
    for n in 1..10u32 {
        let hash = format!("o{}{n}", "c".repeat(49));
        assert!(matches!(
            s.confirm(ACTOR, "current", &missing, &hash).await,
            Err(TradingError::Unauthorized)
        ));
    }
    assert!(matches!(
        s.confirm(ACTOR, "current", &missing, &format!("o{}", "d".repeat(50)))
            .await,
        Err(TradingError::RateLimited)
    ));
    m.calls.lock().unwrap().clear();
    let review = s
        .prepare(ACTOR, "old-session", vec![action()])
        .await
        .unwrap();
    s.check(ACTOR, "old-session", &review.ticket).await.unwrap();
    for _ in 0..250 {
        assert!(matches!(
            s.prepare(ACTOR, "current", vec![action()]).await,
            Err(TradingError::SigningPending(_))
        ));
        assert!(matches!(
            s.confirm(OTHER, "other", &review.ticket, &op_hash()).await,
            Err(TradingError::Unauthorized)
        ));
        assert!(matches!(
            s.confirm(ACTOR, "current", "bad-ticket", &op_hash()).await,
            Err(TradingError::Invalid)
        ));
        assert!(matches!(
            s.confirm(ACTOR, "current", &review.ticket, "bad-hash")
                .await,
            Err(TradingError::Invalid)
        ));
    }
    let before = m.calls.lock().unwrap().len();
    // After a restart the receipt is gone: one chain lookup, which finds another address's
    // operation here.
    assert!(matches!(
        state(&m)
            .confirm(ACTOR, "new-session", &review.ticket, &op_hash())
            .await,
        Err(TradingError::Unauthorized)
    ));
    assert_eq!(m.calls.lock().unwrap().len(), before + 1);
    s.confirm(ACTOR, "new-session", &review.ticket, &op_hash())
        .await
        .unwrap();
    let before = m.calls.lock().unwrap().len();
    for _ in 0..250 {
        s.confirm(ACTOR, "another-session", &review.ticket, &op_hash())
            .await
            .unwrap();
    }
    assert_eq!(m.calls.lock().unwrap().len(), before);
    assert!(
        s.prepare(ACTOR, "new-session", vec![action()])
            .await
            .is_ok()
    );
}
#[tokio::test]
async fn changed_price_owner_code_reorg_and_counter_fail_closed() {
    for change in 0..5 {
        let m = Mock::new();
        let s = state(&m);
        let r = s.prepare(ACTOR, "s", vec![action()]).await.unwrap();
        match change {
            0 => {
                m.price.store(2, Ordering::SeqCst);
            }
            1 => {
                *m.owner.lock().unwrap() = OTHER.into();
            }
            2 => {
                m.bad_code.store(true, Ordering::SeqCst);
            }
            3 => {
                m.reorg.store(true, Ordering::SeqCst);
            }
            _ => {
                m.counter.store(5, Ordering::SeqCst);
            }
        }
        assert!(s.check(ACTOR, "s", &r.ticket).await.is_err());
        assert!(matches!(
            s.check(ACTOR, "s", &r.ticket).await,
            Err(TradingError::Expired)
        ));
    }
}
#[tokio::test]
async fn unavailable_prepare_can_retry_and_cancelled_prepare_releases_slot() {
    let m = Mock::new();
    let s = Arc::new(state(&m));
    m.fail.store(true, Ordering::SeqCst);
    assert!(s.prepare(ACTOR, "s", vec![action()]).await.is_err());
    m.fail.store(false, Ordering::SeqCst);
    m.stall.store(true, Ordering::SeqCst);
    let task = {
        let s = s.clone();
        tokio::spawn(async move { s.prepare(ACTOR, "s", vec![action()]).await })
    };
    tokio::time::timeout(Duration::from_secs(1), m.started.notified())
        .await
        .unwrap();
    assert!(matches!(
        s.prepare(ACTOR, "s", vec![action()]).await,
        Err(TradingError::Busy)
    ));
    task.abort();
    let _ = task.await;
    m.stall.store(false, Ordering::SeqCst);
    assert!(s.prepare(ACTOR, "s", vec![action()]).await.is_ok());
}
#[tokio::test]
async fn confirmation_rejects_wrong_id_counter_order_and_partial_group() {
    for change in 0..5 {
        let m = Mock::new();
        let s = state(&m);
        let second = Action::Cancel {
            marketplace: V2.into(),
            kind: Kind::Token,
            id: 8,
        };
        let r = s.prepare(ACTOR, "s", vec![action(), second]).await.unwrap();
        s.check(ACTOR, "s", &r.ticket).await.unwrap();
        let mut rows = m.last.lock().unwrap().clone();
        match change {
            0 => rows[0]["parameters"]["value"] = json!({"int":"999"}),
            1 => rows[0]["counter"] = json!("99"),
            2 => rows.reverse(),
            3 => {
                rows.pop();
            }
            _ => rows[0]["metadata"]["operation_result"]["status"] = json!("backtracked"),
        };
        *m.receipt.lock().unwrap() = Some(json!(rows));
        assert!(s.confirm(ACTOR, "s", &r.ticket, &op_hash()).await.is_err());
        assert!(matches!(
            s.prepare(ACTOR, "s", vec![action()]).await,
            Err(TradingError::SigningPending(_))
        ));
    }
}
#[tokio::test]
async fn duplicate_actions_limits_and_discard_are_bounded() {
    let m = Mock::new();
    let s = state(&m);
    assert!(matches!(
        s.prepare(ACTOR, "s", vec![action(); 51]).await,
        Err(TradingError::Invalid)
    ));
    assert!(matches!(
        s.prepare(ACTOR, "s", vec![action(); 2]).await,
        Err(TradingError::Invalid)
    ));
    let r = s.prepare(ACTOR, "s", vec![action()]).await.unwrap();
    assert!(s.discard_review(ACTOR, "other", &r.ticket).is_err());
    s.discard_review(ACTOR, "s", &r.ticket).unwrap();
    assert!(s.prepare(ACTOR, "s", vec![action()]).await.is_ok());
}

#[tokio::test]
async fn new_session_of_the_same_address_replaces_its_pending_review() {
    let m = Mock::new();
    let s = state(&m);
    let old = s.prepare(ACTOR, "old", vec![action()]).await.unwrap();
    assert!(matches!(
        s.prepare(ACTOR, "old", vec![action()]).await,
        Err(TradingError::Busy)
    ));
    let new = s.prepare(ACTOR, "new", vec![action()]).await.unwrap();
    assert!(s.check(ACTOR, "old", &old.ticket).await.is_err());
    assert!(s.discard_review(ACTOR, "old", &old.ticket).is_err());
    s.check(ACTOR, "new", &new.ticket).await.unwrap();
    // Once operations are issued, no session replaces them.
    assert!(matches!(
        s.prepare(ACTOR, "old", vec![action()]).await,
        Err(TradingError::SigningPending(_))
    ));
}

#[tokio::test]
#[ignore = "explicit public mainnet reads and read-only simulation; never injects"]
async fn live_readonly_cancel_simulation_never_broadcasts() {
    struct Trace(Arc<dyn Transport>);
    impl Transport for Trace {
        fn request(&self, endpoint: Endpoint, path: String, body: Option<Value>) -> Reply {
            let transport = self.0.clone();
            Box::pin(async move {
                let result = transport.request(endpoint, path.clone(), body).await;
                eprintln!("{path}: {}", if result.is_ok() { "ok" } else { "error" });
                result
            })
        }
    }
    let transport = seeded_archive::trading::http::Http::new(
        "https://rpc.tzkt.io/mainnet",
        "https://api.tzkt.io/v1",
    )
    .unwrap();
    let state = TradingState::with_transport(Arc::new(Trace(transport)));
    let result = state
        .prepare(
            ACTOR,
            "read-only-probe",
            vec![Action::Cancel {
                marketplace: V2.into(),
                kind: Kind::Token,
                id: 79120,
            }],
        )
        .await
        .unwrap();
    assert_eq!(result.items[0].price_mutez, 333_300);
    assert_eq!(result.simulation.operations[0].amount, "0");
    assert!(result.simulation.gas > 0);
}

#[tokio::test]
async fn cancelled_check_before_ready_allows_new_review_but_old_ticket_is_dead() {
    let m = Mock::new();
    let s = Arc::new(state(&m));
    let r = s.prepare(ACTOR, "s", vec![action()]).await.unwrap();
    m.stall.store(true, Ordering::SeqCst);
    let ticket = r.ticket.clone();
    let task = {
        let s = s.clone();
        tokio::spawn(async move { s.check(ACTOR, "s", &ticket).await })
    };
    tokio::time::timeout(Duration::from_secs(1), m.started.notified())
        .await
        .unwrap();
    task.abort();
    let _ = task.await;
    m.stall.store(false, Ordering::SeqCst);
    assert!(matches!(
        s.check(ACTOR, "s", &r.ticket).await,
        Err(TradingError::Expired)
    ));
    assert!(s.prepare(ACTOR, "s", vec![action()]).await.is_ok());
    for mode in [1, 2] {
        let m = Mock::new();
        m.simulation_mode.store(mode, Ordering::SeqCst);
        assert!(state(&m).prepare(ACTOR, "s", vec![action()]).await.is_err());
    }
}
#[tokio::test]
async fn fifth_concurrent_chain_request_has_a_short_bounded_wait() {
    let m = Mock::new();
    m.stall.store(true, Ordering::SeqCst);
    let s = Arc::new(state(&m));
    let mut tasks = Vec::new();
    for suffix in ['1', '2', '3', '4'] {
        let actor = format!("{}{}", &ACTOR[..35], suffix);
        let task = {
            let s = s.clone();
            tokio::spawn(async move {
                s.prepare_from(
                    &actor,
                    "s",
                    vec![action()],
                    None,
                    std::net::IpAddr::from([127, 0, 0, suffix as u8]),
                )
                .await
            })
        };
        tasks.push(task);
        tokio::time::timeout(Duration::from_secs(1), m.started.notified())
            .await
            .unwrap();
    }
    assert!(matches!(
        s.prepare(ACTOR, "s", vec![action()]).await,
        Err(TradingError::Busy)
    ));
    for t in tasks {
        t.abort();
        let _ = t.await;
    }
    m.stall.store(false, Ordering::SeqCst);
    assert!(s.prepare(ACTOR, "s", vec![action()]).await.is_ok());
}

#[tokio::test]
async fn accepting_with_a_token_the_address_does_not_hold_names_the_token_and_contract() {
    let m = Mock::new();
    m.unheld.store(true, Ordering::SeqCst);
    let accept = Action::Accept {
        kind: Kind::Collection,
        id: 7,
        token: seeded_archive::trading::Token {
            contract: TOKENS[2].into(),
            token_id: 1209122,
        },
    };
    let result = state(&m)
        .prepare_from(
            OTHER,
            "s",
            vec![accept],
            None,
            std::net::Ipv4Addr::LOCALHOST.into(),
        )
        .await;
    let Err(error) = result else {
        panic!("an unheld token must not prepare")
    };
    assert!(matches!(
        &error,
        TradingError::NotHeld { contract, token_id: 1209122 } if contract == TOKENS[2]
    ));
    assert_eq!(
        error.to_string(),
        format!(
            "This address does not hold token 1209122 on contract {}",
            TOKENS[2]
        )
    );
}

#[tokio::test]
async fn stale_escrow_hint_does_not_block_owned_token_but_transient_lookup_fails_closed() {
    let hint = Action::Cancel {
        marketplace: V2.into(),
        kind: Kind::Listing,
        id: 9,
    };
    let accept = Action::Accept {
        kind: Kind::Token,
        id: 7,
        token: seeded_archive::trading::Token {
            contract: TOKENS[2].into(),
            token_id: 1,
        },
    };
    for mode in [0, 1, 2] {
        let m = Mock::new();
        m.hint_mode.store(mode, Ordering::SeqCst);
        let s = state(&m);
        let result = s
            .prepare_from(
                OTHER,
                "s",
                vec![accept.clone()],
                Some(hint.clone()),
                std::net::Ipv4Addr::LOCALHOST.into(),
            )
            .await;
        if mode == 2 {
            assert!(matches!(result, Err(TradingError::Unavailable)));
        } else {
            let r = result.unwrap();
            assert_eq!(r.items.len(), if mode == 0 { 1 } else { 2 });
            assert_eq!(r.simulation.operations.len(), if mode == 0 { 4 } else { 5 });
        }
    }
    let m = Mock::new();
    m.simulation_mode.store(1, Ordering::SeqCst);
    assert!(matches!(
        state(&m)
            .prepare_from(
                OTHER,
                "s",
                vec![accept],
                Some(hint),
                std::net::Ipv4Addr::LOCALHOST.into()
            )
            .await,
        Err(TradingError::Simulation)
    ));
}

struct AuditFile(std::path::PathBuf);
impl AuditFile {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::path::PathBuf::from(format!(
            "tmp/trading-audit-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
    fn path(&self) -> std::path::PathBuf {
        self.0.join("trading.sqlite")
    }
    fn conn(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.path()).unwrap()
    }
    fn due(&self) {
        self.conn()
            .execute("UPDATE pending SET next_try=0", [])
            .unwrap();
    }
    fn count(&self, table: &str) -> i64 {
        self.conn()
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }
}
impl Drop for AuditFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn audit_candidate(m: &Mock, counter: u64) {
    m.height.store(20_000_004, Ordering::SeqCst);
    *m.audit_rows.lock().unwrap() =
        vec![json!({"id":100,"hash":op_hash(),"level":20_000_001,"counter":counter})];
}
#[tokio::test]
async fn expired_fee_reviews_reconcile_after_restart_without_client_confirmation() {
    for omit in [false, true] {
        let db = AuditFile::new();
        let m = Mock::new();
        let s = state(&m).with_fee_audit(&db.path()).unwrap();
        let r = s
            .prepare(OTHER, "private-session", vec![accept(Kind::Token)])
            .await
            .unwrap();
        assert_eq!(db.count("pending"), 0);
        s.check(OTHER, "private-session", &r.ticket).await.unwrap();
        let binding: String = db
            .conn()
            .query_row("SELECT binding FROM pending", [], |r| r.get(0))
            .unwrap();
        assert!(!binding.contains("private-session"));
        assert!(!binding.contains(&r.ticket));
        assert_eq!(db.count("pending"), 1);
        let counter = m.last.lock().unwrap()[1]["counter"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        if omit {
            m.last.lock().unwrap().pop();
        }
        audit_candidate(&m, counter);
        s.reconcile_fees().await.unwrap(); // The 30-minute browser window is still open.
        assert_eq!(db.count("outcomes"), 0);
        drop(s);
        db.due();
        let restarted = state(&m).with_fee_audit(&db.path()).unwrap();
        restarted.reconcile_fees().await.unwrap();
        assert_eq!(db.count("pending"), 0);
        assert_eq!(db.count("outcomes"), 1);
        let evidence: String = db
            .conn()
            .query_row("SELECT evidence FROM outcomes", [], |r| r.get(0))
            .unwrap();
        let evidence: Value = serde_json::from_str(&evidence).unwrap();
        assert_eq!(!evidence["fee_evasion"].is_null(), omit);
        if omit {
            assert_eq!(evidence["fee_evasion"]["observed_mutez"], "0");
        }
        restarted.reconcile_fees().await.unwrap();
        assert_eq!(db.count("outcomes"), 1);
    }
}
#[tokio::test]
async fn audit_outages_cancellation_and_unfinalized_results_keep_retryable_work() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = Arc::new(state(&m).with_fee_audit(&db.path()).unwrap());
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    db.due();
    m.fail.store(true, Ordering::SeqCst);
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("pending"), 1);
    assert_eq!(db.count("outcomes"), 0);
    db.due();
    m.fail.store(false, Ordering::SeqCst);
    m.stall.store(true, Ordering::SeqCst);
    let cloned = s.clone();
    let task = tokio::spawn(async move { cloned.reconcile_fees().await });
    m.started.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(db.count("pending"), 1);
    m.stall.store(false, Ordering::SeqCst);
    let counter = m.last.lock().unwrap()[1]["counter"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    audit_candidate(&m, counter);
    m.height.store(20_000_002, Ordering::SeqCst);
    db.due();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("outcomes"), 0);
    m.height.store(20_000_004, Ordering::SeqCst);
    db.due();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("outcomes"), 1);
}
#[tokio::test]
async fn audit_and_client_confirmation_share_unique_evidence_and_reject_other_hash() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    let counter = m.last.lock().unwrap()[1]["counter"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    m.last.lock().unwrap().pop();
    audit_candidate(&m, counter);
    db.due();
    let hash = op_hash();
    let (a, b) = tokio::join!(s.reconcile_fees(), s.confirm(OTHER, "s", &r.ticket, &hash));
    a.unwrap();
    assert!(matches!(b, Err(TradingError::FeeEvasion)));
    assert_eq!(db.count("outcomes"), 1);
    assert_eq!(db.count("pending"), 0);
    assert!(matches!(
        s.confirm(OTHER, "s", &r.ticket, &format!("o{}", "b".repeat(50)))
            .await,
        Err(TradingError::Changed)
    ));
}
#[tokio::test]
async fn audit_write_failure_is_best_effort_and_cancels_remain_available() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    db.conn().execute_batch("CREATE TRIGGER fail_record BEFORE INSERT ON pending BEGIN SELECT RAISE(ABORT,'disk full'); END;").unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
    let cancel = s.prepare(ACTOR, "s", vec![action()]).await.unwrap();
    s.check(ACTOR, "s", &cancel.ticket).await.unwrap();
    assert_eq!(db.count("pending"), 0);
}
#[tokio::test]
async fn unrelated_counter_never_becomes_fee_evidence_and_scan_progress_survives_restart() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    audit_candidate(&m, 999);
    db.due();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("outcomes"), 0);
    assert_eq!(db.count("pending"), 1);
    drop(s);
    m.audit_rows.lock().unwrap().clear();
    db.due();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    s.reconcile_fees().await.unwrap();
    assert!(
        m.calls
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.contains("id.gt=100"))
    );
}
#[test]
fn audit_refuses_unrelated_databases_and_symlinks() {
    let db = AuditFile::new();
    let c = db.conn();
    c.execute_batch("CREATE TABLE unrelated(value TEXT)")
        .unwrap();
    drop(c);
    assert!(state(&Mock::new()).with_fee_audit(&db.path()).is_err());
    #[cfg(unix)]
    {
        let link = db.0.join("link.sqlite");
        std::os::unix::fs::symlink(db.path().canonicalize().unwrap(), &link).unwrap();
        assert!(state(&Mock::new()).with_fee_audit(&link).is_err());
    }
}

#[tokio::test]
async fn changed_fee_terms_are_ambiguous_instead_of_a_false_evasion() {
    let db = AuditFile::new();
    let m = Mock::new();
    let first = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = first
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    first.check(OTHER, "s", &r.ticket).await.unwrap();
    let old_group = m.last.lock().unwrap().clone();
    let counter = old_group[1]["counter"].as_str().unwrap().parse().unwrap();
    let changed = TradingState::with_transport_and_fee(
        Arc::new(MockTransport(m.clone())),
        PlatformFee {
            bps: 300,
            address: Some(FEE_ADDRESS.into()),
        },
    )
    .unwrap()
    .with_fee_audit(&db.path())
    .unwrap();
    let r = changed
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    changed.check(OTHER, "s", &r.ticket).await.unwrap();
    *m.receipt.lock().unwrap() = Some(json!(old_group));
    assert_eq!(db.count("pending"), 2);
    audit_candidate(&m, counter);
    db.due();
    changed.reconcile_fees().await.unwrap();
    assert_eq!(db.count("pending"), 0);
    assert_eq!(db.count("outcomes"), 1);
    let evidence: String = db
        .conn()
        .query_row("SELECT evidence FROM outcomes", [], |r| r.get(0))
        .unwrap();
    let evidence: Value = serde_json::from_str(&evidence).unwrap();
    assert_eq!(evidence["status"], "ambiguous_terms");
    assert!(evidence["fee_evasion"].is_null());
    // The newer review must not accuse the earlier, fully-paid signed group.
    changed
        .confirm(OTHER, "s", &r.ticket, &op_hash())
        .await
        .unwrap();
}

#[tokio::test]
async fn canonically_consumed_counter_with_different_offer_is_terminal_without_accusation() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    let counter = m.last.lock().unwrap()[1]["counter"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    m.last.lock().unwrap()[1]["parameters"]["value"] = json!({"int":"999"});
    audit_candidate(&m, counter);
    db.due();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("pending"), 0);
    let evidence: String = db
        .conn()
        .query_row("SELECT evidence FROM outcomes", [], |r| r.get(0))
        .unwrap();
    let evidence: Value = serde_json::from_str(&evidence).unwrap();
    assert_eq!(evidence["status"], "unmatched_group");
    assert!(evidence["fee_evasion"].is_null());
}

#[tokio::test]
async fn audit_retries_unverified_rpc_groups_without_skipping_indexer_candidates() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    let group = m.last.lock().unwrap().clone();
    let counter = group[1]["counter"].as_str().unwrap().parse().unwrap();
    audit_candidate(&m, counter);
    db.due();
    *m.receipt.lock().unwrap() = Some(json!([]));
    s.reconcile_fees().await.unwrap();
    let cursor: i64 = db
        .conn()
        .query_row("SELECT cursor FROM pending", [], |r| r.get(0))
        .unwrap();
    assert_eq!(cursor, 0);
    assert_eq!(db.count("outcomes"), 0);
    *m.receipt.lock().unwrap() = Some(json!(group));
    db.due();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("outcomes"), 1);
}

#[tokio::test]
async fn annotated_contract_default_passes_prepare_and_check_but_unoriginated_recipient_fails() {
    let m = Mock::new();
    let s = TradingState::with_transport_and_fee(
        Arc::new(MockTransport(m.clone())),
        PlatformFee {
            bps: 250,
            address: Some(FXHASH_TREASURY.into()),
        },
    )
    .unwrap();
    m.fee_entrypoint_mode.store(4, Ordering::SeqCst);
    assert!(matches!(
        s.prepare(OTHER, "s", vec![accept(Kind::Token)]).await,
        Err(TradingError::FeeRecipient)
    ));
    m.fee_entrypoint_mode.store(3, Ordering::SeqCst);
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
}

#[tokio::test]
async fn unavailable_audit_returns_specific_error_without_disabling_cancels() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_optional_fee_audit(&db.0); // A directory cannot be an audit DB.
    assert_eq!(s.platform_fee().address.as_deref(), Some(FEE_ADDRESS));
    assert!(matches!(
        s.prepare(OTHER, "s", vec![accept(Kind::Token)]).await,
        Err(TradingError::FeeAuditUnavailable)
    ));
    let cancel = s.prepare(ACTOR, "s", vec![action()]).await.unwrap();
    s.check(ACTOR, "s", &cancel.ticket).await.unwrap();
}

#[tokio::test]
async fn abandoned_bindings_expire_and_orphan_terms_are_removed() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    db.conn()
        .execute("UPDATE pending SET created_at=1", [])
        .unwrap();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("pending"), 0);
    assert_eq!(db.count("terms"), 0);
    assert_eq!(db.count("outcomes"), 0);
}

#[tokio::test]
async fn one_corrupt_audit_row_does_not_prevent_valid_evidence() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    db.conn().execute("INSERT INTO pending(id,actor,counter,binding,next_try,created_at) SELECT 'corrupt',actor,counter,'{',0,created_at FROM pending LIMIT 1", []).unwrap();
    let counter = m.last.lock().unwrap()[1]["counter"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    audit_candidate(&m, counter);
    db.due();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("pending"), 0);
    assert_eq!(db.count("outcomes"), 1);
}

#[tokio::test]
async fn finalized_counter_consumed_elsewhere_closes_binding_without_evasion() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    m.height.store(20_000_004, Ordering::SeqCst);
    m.counter.store(100, Ordering::SeqCst);
    db.due();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("pending"), 0);
    assert_eq!(db.count("terms"), 1); // Orphan cleanup is deferred to maintenance.
    db.conn()
        .execute("UPDATE audit_maintenance SET last_run=0", [])
        .unwrap();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("terms"), 0);
    assert_eq!(db.count("outcomes"), 0);
}

#[tokio::test]
async fn settled_evidence_and_terms_have_bounded_retention() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    m.last.lock().unwrap().pop();
    s.confirm(OTHER, "s", &r.ticket, &op_hash())
        .await
        .unwrap_err();
    assert_eq!(db.count("outcomes"), 1);
    assert_eq!(db.count("settlements"), 1);
    db.conn()
        .execute("UPDATE outcomes SET recorded_at=1", [])
        .unwrap();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("outcomes"), 0);
    assert_eq!(db.count("settlements"), 0);
    assert_eq!(db.count("terms"), 0);
}

#[tokio::test]
async fn confirmation_fetches_one_group_not_the_unbounded_manager_pass() {
    let m = Mock::new();
    let s = state(&m);
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
    let calls = m.calls.lock().unwrap();
    assert!(calls.iter().any(|p| p.ends_with("/operation_hashes/3")));
    assert!(calls.iter().any(|p| p.ends_with("/operations/3/0")));
    assert!(!calls.iter().any(|p| p.ends_with("/operations/3")));
}

#[tokio::test]
async fn a_head_briefly_behind_the_review_is_reread_before_reporting_a_change() {
    let m = Mock::new();
    let s = state(&m);
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    m.behind.store(1, Ordering::SeqCst);
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    // A head that stays behind is reported as a change after the re-reads.
    let m = Mock::new();
    let s = state(&m);
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    m.behind.store(100, Ordering::SeqCst);
    assert!(matches!(
        s.check(OTHER, "s", &r.ticket).await,
        Err(TradingError::Changed)
    ));
}

#[tokio::test]
async fn an_operation_whose_receipt_was_lost_is_confirmed_from_the_chain_for_its_sender() {
    let dir = AuditFile::new();
    let request = dir.0.join("sync.request");
    let m = Mock::new();
    // A restart lost every receipt: the page still holds its ticket and hash.
    let s = state(&m).with_sync_request(request.clone());
    let ticket = "a".repeat(48);
    s.confirm(OTHER, "s", &ticket, &op_hash()).await.unwrap();
    assert!(
        request.exists(),
        "a recent confirmed trade still starts the sync"
    );
    // Another address's operation is not confirmed for this one.
    assert!(matches!(
        s.confirm(ACTOR, "s", &ticket, &op_hash()).await,
        Err(TradingError::Unauthorized)
    ));
    // Not final yet, or unknown to the indexer: still pending.
    m.height.store(19_999_998, Ordering::SeqCst);
    assert!(matches!(
        s.confirm(OTHER, "s", &ticket, &op_hash()).await,
        Err(TradingError::Pending)
    ));
    *m.sender.lock().unwrap() = None;
    assert!(matches!(
        s.confirm(OTHER, "s", &ticket, &op_hash()).await,
        Err(TradingError::Pending)
    ));
}

#[tokio::test(start_paused = true)]
async fn a_submitted_operation_shows_as_done_and_is_confirmed_in_the_background() {
    let dir = AuditFile::new();
    let request = dir.0.join("sync.request");
    let m = Mock::new();
    let s = Arc::new(state(&m).with_sync_request(request.clone()));
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Collection)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    assert!(s.overlay(OTHER).is_empty());
    s.submitted(OTHER, "s", &r.ticket, &op_hash()).unwrap();
    // The NFT leaves the address and the collection offer has one token fewer, at once.
    let overlay = s.overlay(OTHER);
    assert!(overlay.tokens.contains(&seeded_archive::trading::Token {
        contract: TOKENS[2].into(),
        token_id: 1
    }));
    assert_eq!(overlay.taken.get(&7).map(|(_, n)| *n), Some(1));
    assert!(s.overlay(ACTOR).is_empty(), "only the sender sees it");
    // Reporting the same operation again changes nothing; another hash is refused.
    s.submitted(OTHER, "s", &r.ticket, &op_hash()).unwrap();
    assert!(matches!(
        s.submitted(OTHER, "s", &r.ticket, &format!("o{}", "b".repeat(50))),
        Err(TradingError::Changed)
    ));
    // The watcher confirms it without the page and starts the index sync.
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(request.exists());
    assert!(!s.overlay(OTHER).is_empty(), "kept until the index has it");
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_failed_submission_withdraws_its_effects_and_reports_the_failure() {
    let m = Mock::new();
    let s = Arc::new(state(&m));
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    *m.op_status.lock().unwrap() = "failed";
    s.submitted(OTHER, "s", &r.ticket, &op_hash()).unwrap();
    assert!(!s.overlay(OTHER).is_empty());
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(s.overlay(OTHER).is_empty(), "the records come back");
    assert!(matches!(
        s.confirm(OTHER, "s", &r.ticket, &op_hash()).await,
        Err(TradingError::Failed)
    ));
    // A ticket without a checked receipt cannot be reported.
    assert!(matches!(
        s.submitted(OTHER, "s", &"c".repeat(48), &op_hash()),
        Err(TradingError::Expired)
    ));
}

#[tokio::test(start_paused = true)]
async fn the_watcher_never_confirms_an_operation_that_differs_from_its_review() {
    let m = Mock::new();
    let s = Arc::new(state(&m));
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    // Somebody else's applied operation reported under this ticket.
    *m.receipt.lock().unwrap() = Some(
        json!([{"kind":"transaction","source":ACTOR,"destination":V2,"amount":"0","counter":"1","metadata":{"operation_result":{"status":"applied"}}}]),
    );
    s.submitted(OTHER, "s", &r.ticket, &op_hash()).unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(s.overlay(OTHER).is_empty(), "its effects are withdrawn");
    assert!(matches!(
        s.confirm(OTHER, "s", &r.ticket, &op_hash()).await,
        Err(TradingError::Unverified)
    ));
}

#[tokio::test(start_paused = true)]
async fn an_upstream_outage_is_never_reported_as_a_failed_operation() {
    let m = Mock::new();
    let s = Arc::new(state(&m));
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    s.submitted(OTHER, "s", &r.ticket, &op_hash()).unwrap();
    m.fail.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(10 * 60)).await;
    assert!(
        !s.overlay(OTHER).is_empty(),
        "still pending while upstream is down"
    );
    tokio::time::sleep(Duration::from_secs(6 * 60)).await;
    assert!(s.overlay(OTHER).is_empty());
    assert!(matches!(
        s.confirm(OTHER, "s", &r.ticket, &op_hash()).await,
        Err(TradingError::Unverified)
    ));
}

#[tokio::test(start_paused = true)]
async fn a_failure_in_a_block_that_is_not_final_is_not_a_verdict() {
    let m = Mock::new();
    let s = Arc::new(state(&m));
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    // Reported failed in a block that two more blocks have not followed yet.
    *m.op_status.lock().unwrap() = "failed";
    m.height.store(19_999_998, Ordering::SeqCst);
    s.submitted(OTHER, "s", &r.ticket, &op_hash()).unwrap();
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(!s.overlay(OTHER).is_empty(), "still pending");
    // The block was replaced and the operation applied.
    *m.op_status.lock().unwrap() = "applied";
    m.height.store(20_000_000, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(60)).await;
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn an_operation_unseen_for_long_is_unverified_and_can_still_confirm() {
    let m = Mock::new();
    let s = Arc::new(state(&m));
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    *m.sender.lock().unwrap() = None;
    s.submitted(OTHER, "s", &r.ticket, &op_hash()).unwrap();
    tokio::time::sleep(Duration::from_secs(16 * 60)).await;
    assert!(s.overlay(OTHER).is_empty());
    assert!(matches!(
        s.confirm(OTHER, "s", &r.ticket, &op_hash()).await,
        Err(TradingError::Unverified)
    ));
    // The operation turns up applied after all: the page's next check confirms it.
    *m.sender.lock().unwrap() = Some(OTHER.into());
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_canonical_group_without_a_failure_status_is_not_a_verdict() {
    let m = Mock::new();
    let s = Arc::new(state(&m));
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    // The indexer says failed, but the final canonical group carries no result status.
    *m.op_status.lock().unwrap() = "failed";
    *m.receipt.lock().unwrap() = Some(
        json!([{"kind":"transaction","source":OTHER,"destination":V2,"amount":"0","counter":"1","metadata":{}}]),
    );
    s.submitted(OTHER, "s", &r.ticket, &op_hash()).unwrap();
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(
        !s.overlay(OTHER).is_empty(),
        "missing evidence keeps it pending"
    );
    // Then the reviewed group is seen applied.
    *m.op_status.lock().unwrap() = "applied";
    *m.receipt.lock().unwrap() = None;
    tokio::time::sleep(Duration::from_secs(60)).await;
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn the_watcher_keeps_a_fee_evasion_verdict() {
    let m = Mock::new();
    let s = Arc::new(state(&m));
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    // The included group lacks the reviewed fee transfer.
    m.last.lock().unwrap().pop();
    s.submitted(OTHER, "s", &r.ticket, &op_hash()).unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(matches!(
        s.confirm(OTHER, "s", &r.ticket, &op_hash()).await,
        Err(TradingError::FeeEvasion)
    ));
}

#[tokio::test]
async fn a_node_behind_the_indexer_keeps_the_confirmation_pending() {
    let m = Mock::new();
    let s = state(&m);
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    m.lagging.store(true, Ordering::SeqCst);
    assert!(matches!(
        s.confirm(OTHER, "s", &r.ticket, &op_hash()).await,
        Err(TradingError::Pending)
    ));
    m.lagging.store(false, Ordering::SeqCst);
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
}

#[tokio::test]
async fn the_first_confirmation_requests_an_index_sync_once() {
    let dir = AuditFile::new();
    let request = dir.0.join("sync.request");
    let m = Mock::new();
    let s = state(&m).with_sync_request(request.clone());
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    // Not yet final: no request.
    m.height.store(19_999_998, Ordering::SeqCst);
    assert!(matches!(
        s.confirm(OTHER, "s", &r.ticket, &op_hash()).await,
        Err(TradingError::Pending)
    ));
    assert!(!request.exists());
    m.height.store(20_000_000, Ordering::SeqCst);
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
    assert!(request.exists(), "a confirmed trade asks for a sync");
    // Polling the confirmed receipt again does not ask again.
    std::fs::remove_file(&request).unwrap();
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
    assert!(!request.exists());
}

#[tokio::test]
async fn unmatched_sibling_cannot_discard_matching_evasion_at_the_same_counter() {
    let db = AuditFile::new();
    let m = Mock::new();
    let first = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = first
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    first.check(OTHER, "s", &r.ticket).await.unwrap();
    db.conn()
        .execute("UPDATE pending SET next_try=-1", [])
        .unwrap();
    let second = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = second
        .prepare(OTHER, "s", vec![accept(Kind::Collection)])
        .await
        .unwrap();
    second.check(OTHER, "s", &r.ticket).await.unwrap();
    db.conn()
        .execute("UPDATE pending SET next_try=0 WHERE next_try>0", [])
        .unwrap();
    let counter = m.last.lock().unwrap()[1]["counter"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    m.last.lock().unwrap().pop();
    audit_candidate(&m, counter);
    second.reconcile_fees().await.unwrap();
    assert_eq!(db.count("pending"), 0);
    assert_eq!(db.count("settlements"), 2);
    assert_eq!(db.count("outcomes"), 1);
    let evidence: String = db
        .conn()
        .query_row("SELECT evidence FROM outcomes", [], |r| r.get(0))
        .unwrap();
    let evidence: Value = serde_json::from_str(&evidence).unwrap();
    assert_eq!(evidence["status"], "classified");
    assert_eq!(evidence["fee_evasion"]["observed_mutez"], "0");
    assert!(matches!(
        second.confirm(OTHER, "s", &r.ticket, &op_hash()).await,
        Err(TradingError::FeeEvasion)
    ));
    assert_eq!(db.count("outcomes"), 1);
}

#[tokio::test]
async fn audit_caps_each_actor_and_global_capacity_without_blocking_accepts() {
    let db = AuditFile::new();
    let m = Mock::new();
    for counter in 0..34 {
        m.counter.store(counter, Ordering::SeqCst);
        let s = state(&m).with_fee_audit(&db.path()).unwrap();
        let r = s
            .prepare(OTHER, "s", vec![accept(Kind::Token)])
            .await
            .unwrap();
        s.check(OTHER, "s", &r.ticket).await.unwrap();
    }
    assert_eq!(db.count("pending"), 32);
    assert_eq!(db.count("terms"), 32);
    let first_id: String = db
        .conn()
        .query_row(
            "SELECT id FROM pending ORDER BY counter,id LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let last_counter: i64 = db
        .conn()
        .query_row("SELECT max(counter) FROM pending", [], |r| r.get(0))
        .unwrap();
    let first_counter: i64 = db
        .conn()
        .query_row("SELECT min(counter) FROM pending", [], |r| r.get(0))
        .unwrap();
    assert_eq!(last_counter - first_counter, 31);
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT count(*) FROM terms WHERE binding_id=?1",
                [&first_id],
                |r| r.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    for counter in 34..36 {
        m.counter.store(counter, Ordering::SeqCst);
        let s = state(&m).with_fee_audit(&db.path()).unwrap();
        let r = s
            .prepare(OTHER, "s", vec![accept(Kind::Token)])
            .await
            .unwrap();
        s.check(OTHER, "s", &r.ticket).await.unwrap();
    }
    assert_eq!(db.count("pending"), 32);
    assert_eq!(db.count("terms"), 32);
    assert_eq!(
        db.conn()
            .query_row(
                "SELECT count(*) FROM terms WHERE binding_id=?1",
                [&first_id],
                |r| r.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    let newest: i64 = db
        .conn()
        .query_row("SELECT max(counter) FROM pending", [], |r| r.get(0))
        .unwrap();
    let oldest: i64 = db
        .conn()
        .query_row("SELECT min(counter) FROM pending", [], |r| r.get(0))
        .unwrap();
    assert_eq!(newest - oldest, 31);

    db.conn()
        .execute_batch(
            "DELETE FROM pending; DELETE FROM terms;
        WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<16384)
        INSERT INTO pending(id,actor,counter,binding,next_try,created_at)
        SELECT CAST(x AS TEXT),'another-'||x,1,'{}',unixepoch()+86400,unixepoch() FROM n;",
        )
        .unwrap();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    assert_eq!(db.count("pending"), 16384);
}

#[tokio::test]
async fn actor_eviction_rolls_back_when_new_binding_insert_fails() {
    let db = AuditFile::new();
    let m = Mock::new();
    for counter in 0..32 {
        m.counter.store(counter, Ordering::SeqCst);
        let s = state(&m).with_fee_audit(&db.path()).unwrap();
        let r = s
            .prepare(OTHER, "s", vec![accept(Kind::Token)])
            .await
            .unwrap();
        s.check(OTHER, "s", &r.ticket).await.unwrap();
    }
    let oldest: String = db
        .conn()
        .query_row(
            "SELECT id FROM pending ORDER BY counter,id LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    db.conn()
        .execute_batch(
            "CREATE TRIGGER fail_latest_pending BEFORE INSERT ON pending
             WHEN NEW.counter>(SELECT max(counter) FROM pending)
             BEGIN SELECT RAISE(ABORT,'disk full'); END;",
        )
        .unwrap();
    m.counter.store(32, Ordering::SeqCst);
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    assert_eq!(db.count("pending"), 32);
    assert_eq!(db.count("terms"), 32);
    assert_eq!(
        db.conn()
            .query_row("SELECT count(*) FROM pending WHERE id=?1", [&oldest], |r| r
                .get::<_, i64>(0),)
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn audit_prunes_only_from_due_at_most_once_per_minute() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    db.conn()
        .execute("UPDATE pending SET created_at=1", [])
        .unwrap();

    m.counter.store(1, Ordering::SeqCst);
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    assert_eq!(db.count("pending"), 2); // record does not prune.

    s.reconcile_fees().await.unwrap(); // First due call prunes expired rows.
    assert_eq!(db.count("pending"), 1);
    db.conn()
        .execute("UPDATE pending SET created_at=1", [])
        .unwrap();
    s.reconcile_fees().await.unwrap(); // A second call within the minute skips prune.
    assert_eq!(db.count("pending"), 1);

    db.conn()
        .execute(
            "UPDATE audit_maintenance SET last_run=0 WHERE name='prune'",
            [],
        )
        .unwrap();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("pending"), 0);
    assert_eq!(db.count("terms"), 0);
}

#[test]
fn existing_v2_audit_database_gets_pruning_indexes() {
    let db = AuditFile::new();
    let m = Mock::new();
    let trading = state(&m).with_fee_audit(&db.path()).unwrap();
    drop(trading);
    db.conn()
        .execute_batch(
            "DROP INDEX pending_age; DROP INDEX pending_actor_age;
             DROP INDEX outcomes_recent; DROP TABLE audit_maintenance;",
        )
        .unwrap();
    let state = state(&m).with_fee_audit(&db.path()).unwrap();
    drop(state);
    let conn = db.conn();
    for name in ["pending_age", "pending_actor_age", "outcomes_recent"] {
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='index' AND name=?1",
                [name],
                |r| r.get::<_, i64>(0),
            )
            .unwrap(),
            1,
            "missing index {name}"
        );
    }
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='audit_maintenance'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn audit_v1_upgrade_preserves_pending_work_and_prior_settlements() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    let c = db.conn();
    c.execute_batch(
        "DROP INDEX pending_age; DROP INDEX pending_actor_age;
        ALTER TABLE pending DROP COLUMN created_at;
        DROP TABLE settlements;
        INSERT INTO outcomes VALUES('old-operation','old-binding','{}',unixepoch());
        PRAGMA user_version=1;",
    )
    .unwrap();
    drop(c);
    drop(s);
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    assert_eq!(db.count("pending"), 1);
    assert_eq!(db.count("settlements"), 1);
    assert_eq!(
        db.conn()
            .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    for name in ["pending_age", "pending_actor_age", "outcomes_recent"] {
        assert_eq!(
            db.conn()
                .query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type='index' AND name=?1",
                    [name],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap(),
            1,
            "missing index {name} after v1 migration"
        );
    }
    let counter = m.last.lock().unwrap()[1]["counter"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    audit_candidate(&m, counter);
    db.due();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("outcomes"), 2);
    assert_eq!(db.count("pending"), 0);
}

#[tokio::test]
async fn failed_evidence_write_does_not_make_applied_acceptance_retryable() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    db.conn().execute_batch("CREATE TRIGGER fail_outcome BEFORE INSERT ON outcomes BEGIN SELECT RAISE(ABORT,'disk full'); END;").unwrap();
    m.last.lock().unwrap().pop();
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap();
    assert_eq!(db.count("pending"), 1); // Background evidence remains retryable.
    assert_eq!(db.count("outcomes"), 0);
    m.fail.store(true, Ordering::SeqCst);
    s.confirm(OTHER, "s", &r.ticket, &op_hash()).await.unwrap(); // Receipt is terminal.
}

#[tokio::test]
async fn matching_evasion_upgrades_legacy_unmatched_sibling_evidence_once() {
    let db = AuditFile::new();
    let m = Mock::new();
    let s = state(&m).with_fee_audit(&db.path()).unwrap();
    let r = s
        .prepare(OTHER, "s", vec![accept(Kind::Token)])
        .await
        .unwrap();
    s.check(OTHER, "s", &r.ticket).await.unwrap();
    db.conn()
        .execute(
            "INSERT INTO outcomes VALUES(?1,'older-review',?2,unixepoch())",
            rusqlite::params![
                op_hash(),
                json!({"status":"unmatched_group","fee_evasion":null}).to_string()
            ],
        )
        .unwrap();
    let counter = m.last.lock().unwrap()[1]["counter"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    m.last.lock().unwrap().pop();
    audit_candidate(&m, counter);
    db.due();
    s.reconcile_fees().await.unwrap();
    assert_eq!(db.count("pending"), 0);
    assert_eq!(db.count("outcomes"), 1);
    let evidence: String = db
        .conn()
        .query_row("SELECT evidence FROM outcomes", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&evidence).unwrap()["fee_evasion"]["observed_mutez"],
        "0"
    );
    assert!(matches!(
        s.confirm(OTHER, "s", &r.ticket, &op_hash()).await,
        Err(TradingError::FeeEvasion)
    ));
    assert_eq!(db.count("outcomes"), 1);
}
