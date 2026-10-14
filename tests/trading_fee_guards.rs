use seeded_archive::trading::{
    Operation, TradingError,
    chain::{Chain, Head, Inputs},
    http::{Endpoint, Reply, Transport},
    types::{Action, FXHASH_TREASURY, Kind, PlatformFee, TOKENS, Token, V2, operation_group},
};
use serde_json::{Value, json};
use std::sync::Arc;

const ACTOR: &str = "tz1P719ymGCtNwavZKh2LmhMs53dGsEMzjnm";
const FEE_ADDRESS: &str = "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL";
const PRICE: u64 = 1_000_039;
const FXHASH_FEE: u64 = 25_000;
const ROYALTY: u64 = 150_004;
const SELLER: u64 = PRICE - FXHASH_FEE - ROYALTY;

#[derive(Clone)]
struct Mock {
    treasury: u64,
    royalty: u64,
    seller: u64,
}

impl Transport for Mock {
    fn request(&self, endpoint: Endpoint, path: String, body: Option<Value>) -> Reply {
        let this = self.clone();
        Box::pin(async move {
            assert!(matches!(endpoint, Endpoint::Rpc));
            let value = if path.ends_with("/manager_key") {
                json!("revealed")
            } else if path.ends_with("/counter") {
                json!("4")
            } else if path.ends_with("/constants") {
                json!({"cost_per_byte":"250"})
            } else if path.ends_with("/mempool/filter") {
                json!({"minimal_fees":"100","minimal_nanotez_per_gas_unit":["100","1"],"minimal_nanotez_per_byte":["1000","1"]})
            } else if path.ends_with("/forge/operations") {
                json!("ab".repeat(120))
            } else if path.ends_with("/simulate_operation") {
                let contents = body.unwrap()["operation"]["contents"]
                    .as_array()
                    .unwrap()
                    .clone();
                let rows: Vec<Value> = contents
                    .iter()
                    .map(|operation| {
                        let mut result = json!({
                            "metadata": {
                                "operation_result": {
                                    "status":"applied",
                                    "consumed_milligas":"1000000",
                                    "paid_storage_size_diff":"0"
                                }
                            }
                        });
                        if operation["destination"] == V2
                            && matches!(operation["parameters"]["entrypoint"].as_str(), Some("offer_accept" | "collection_offer_accept"))
                        {
                            let paid = |destination: &str, amount: u64| json!({
                                "kind":"transaction",
                                "source":V2,
                                "destination":destination,
                                "amount":amount.to_string(),
                                "result":{"status":"applied","consumed_milligas":"100000","paid_storage_size_diff":"0"}
                            });
                            result["metadata"]["internal_operation_results"] = json!([
                                paid(FXHASH_TREASURY, this.treasury),
                                paid("tz1ewF2P3erZJSP8m35QiNEKGtwj7qsBhF8z", this.royalty),
                                paid(ACTOR, this.seller)
                            ]);
                        }
                        result
                    })
                    .collect();
                json!({"contents":rows})
            } else {
                panic!("unexpected RPC path: {path}")
            };
            Ok(value)
        })
    }
}

fn chain(price: u64, treasury: u64, seller: u64, bps: u16) -> Chain {
    Chain::new(
        Arc::new(Mock {
            treasury,
            royalty: price.saturating_sub(treasury).saturating_sub(seller),
            seller,
        }),
        PlatformFee {
            bps,
            address: Some(FEE_ADDRESS.into()),
        },
    )
}

fn accept_ops() -> Vec<Operation> {
    operation_group(
        ACTOR,
        &Action::Accept {
            kind: Kind::Token,
            id: 7,
            token: Token {
                contract: TOKENS[2].into(),
                token_id: 1,
            },
        },
    )
    .unwrap()
}

fn fee_transfer(amount: u64) -> Operation {
    Operation::transfer(ACTOR, FEE_ADDRESS, amount)
}

async fn simulate(
    chain: &Chain,
    operations: Vec<Operation>,
) -> Result<seeded_archive::trading::chain::Simulation, TradingError> {
    chain
        .simulate(
            ACTOR,
            &Head {
                hash: format!("B{}", "b".repeat(50)),
                level: 20_000_000,
            },
            operations,
            &Inputs {
                counter: 4,
                filter: json!({"minimal_fees":"100","minimal_nanotez_per_gas_unit":["100","1"],"minimal_nanotez_per_byte":["1000","1"]}),
                cost_per_byte: Some(250),
            },
        )
        .await
}

#[tokio::test]
async fn acceptance_requires_exactly_one_final_platform_fee_transfer() {
    let chain = chain(PRICE, FXHASH_FEE, SELLER, 250);

    let mut missing = accept_ops();
    assert!(matches!(
        simulate(&chain, missing.clone()).await,
        Err(TradingError::Simulation)
    ));

    missing.push(fee_transfer(25_000));
    let mut duplicate = missing.clone();
    duplicate.push(fee_transfer(25_000));
    assert!(matches!(
        simulate(&chain, duplicate).await,
        Err(TradingError::Simulation)
    ));

    let mut wrong_amount = accept_ops();
    wrong_amount.push(fee_transfer(24_999));
    assert!(matches!(
        simulate(&chain, wrong_amount).await,
        Err(TradingError::Simulation)
    ));

    let mut misplaced = accept_ops();
    misplaced.insert(0, fee_transfer(25_000));
    assert!(matches!(
        simulate(&chain, misplaced).await,
        Err(TradingError::Simulation)
    ));
}

#[tokio::test]
async fn cancellation_cannot_carry_a_plain_fee_transfer() {
    let chain = chain(PRICE, FXHASH_FEE, SELLER, 250);
    let cancel = Action::Cancel {
        marketplace: V2.into(),
        kind: Kind::Token,
        id: 7,
    };
    let mut operations = operation_group(ACTOR, &cancel).unwrap();
    operations.push(fee_transfer(1));
    assert!(matches!(
        simulate(&chain, operations).await,
        Err(TradingError::Invalid)
    ));
}

#[tokio::test]
async fn acceptance_rejects_underpaid_treasury_and_negative_seller_net() {
    let mut underpaid = accept_ops();
    underpaid.push(fee_transfer(25_000));
    assert!(matches!(
        simulate(&chain(PRICE, FXHASH_FEE - 1, SELLER + 1, 250), underpaid).await,
        Err(TradingError::Simulation)
    ));

    let mut negative = accept_ops();
    negative.push(fee_transfer(10));
    assert!(matches!(
        simulate(&chain(100, 2, 0, 1_000), negative).await,
        Err(TradingError::Simulation)
    ));
}

#[tokio::test]
async fn odd_mutez_price_is_accepted_and_zero_fee_needs_no_transfer() {
    let mut odd = accept_ops();
    odd.push(fee_transfer(25_000));
    let accepted = simulate(&chain(PRICE, FXHASH_FEE, SELLER, 250), odd)
        .await
        .unwrap();
    let fees = accepted.accept_fees.unwrap();
    assert_eq!(fees.price_mutez, PRICE);
    assert_eq!(fees.platform_fee_mutez, 25_000);
    assert_eq!(fees.seller_net_mutez, SELLER - 25_000);

    let zero = simulate(&chain(39, 0, 39, 0), accept_ops()).await.unwrap();
    let fees = zero.accept_fees.unwrap();
    assert_eq!(fees.price_mutez, 39);
    assert_eq!(fees.platform_fee_mutez, 0);
}

#[test]
fn kt1_default_accepts_annotations_but_requires_exact_unit_type() {
    use seeded_archive::trading::chain::unit_default_entrypoint;

    assert!(unit_default_entrypoint(&json!({"prim":"unit","annots":["%default"]})).unwrap());
    assert!(!unit_default_entrypoint(&json!({"prim":"unit","args":[{"prim":"unit"}]})).unwrap());
    assert!(!unit_default_entrypoint(&json!({"prim":"string","annots":["%default"]})).unwrap());
}
