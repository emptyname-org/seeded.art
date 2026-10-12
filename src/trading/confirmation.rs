//! Classify finalized operation groups; a fee audit never retries an applied acceptance.
use super::{Plan, TradingError, chain, same_parameters, types::V2};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct FeeEvasion {
    pub operation_hash: String,
    pub block_hash: String,
    pub level: u64,
    pub actor: String,
    pub recipient: String,
    #[serde(
        serialize_with = "super::types::decimal",
        deserialize_with = "decimal_string"
    )]
    pub expected_mutez: u64,
    #[serde(
        serialize_with = "super::types::decimal",
        deserialize_with = "decimal_string"
    )]
    pub observed_mutez: u64,
}

fn decimal_string<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(serde::de::Error::custom(
            "expected canonical decimal string",
        ));
    }
    value.parse().map_err(serde::de::Error::custom)
}

fn applied(row: &Value) -> bool {
    row["metadata"]["operation_result"]["status"] == "applied"
        && match row["metadata"].get("internal_operation_results") {
            None => true,
            Some(Value::Array(rows)) => rows.iter().all(|r| r["result"]["status"] == "applied"),
            _ => false,
        }
}
fn matches(row: &Value, actor: &str, op: &super::Operation) -> Result<bool, TradingError> {
    Ok(row["kind"] == "transaction"
        && row["source"] == actor
        && row["destination"] == op.destination
        && row["amount"] == op.amount
        && row["counter"] == op.counter
        && same_parameters(&row["parameters"], &op.parameters)?
        && applied(row))
}

pub(super) fn classify(
    actor: &str,
    plan: &Plan,
    contents: &[Value],
    hash: &str,
    block: &str,
    level: u64,
) -> Result<Option<FeeEvasion>, TradingError> {
    let fee = plan
        .simulation
        .accept_fees
        .as_ref()
        .map(|fees| (fees.platform_fee_address.as_str(), fees.platform_fee_mutez));
    classify_operations(
        actor,
        &plan.simulation.operations,
        fee,
        contents,
        hash,
        block,
        level,
    )
}

pub(super) fn classify_operations(
    actor: &str,
    expected: &[super::Operation],
    fee: Option<(&str, u64)>,
    contents: &[Value],
    hash: &str,
    block: &str,
    level: u64,
) -> Result<Option<FeeEvasion>, TradingError> {
    if contents.len() == expected.len()
        && contents
            .iter()
            .zip(expected)
            .all(|(row, op)| matches(row, actor, op).unwrap_or(false))
    {
        return Ok(None);
    }
    let (fee_address, fee_mutez) = fee.ok_or(TradingError::Changed)?;
    if fee_address == actor
        && fee_mutez > 0
        && expected.last().is_some_and(|op| {
            op.source == actor
                && op.destination == actor
                && op.amount == fee_mutez.to_string()
                && op.parameters.is_null()
        })
        && contents.len() + 1 == expected.len()
        && contents
            .iter()
            .zip(&expected[..expected.len() - 1])
            .all(|(row, op)| matches(row, actor, op).unwrap_or(false))
    {
        return Ok(None);
    }
    if fee_address == actor
        || fee_mutez == 0
        || contents.is_empty()
        || contents.len() > 100
        || !contents.iter().all(applied)
    {
        return Err(TradingError::Changed);
    }
    let accept = expected
        .iter()
        .find(|op| {
            op.destination == V2
                && matches!(
                    op.parameters["entrypoint"].as_str(),
                    Some("offer_accept" | "collection_offer_accept")
                )
        })
        .ok_or(TradingError::Changed)?;
    if !contents
        .iter()
        .any(|row| matches(row, actor, accept).unwrap_or(false))
    {
        return Err(TradingError::Changed);
    }
    let mut observed = 0_u64;
    for row in contents {
        if row["kind"] == "transaction"
            && row["source"] == actor
            && row["destination"] == fee_address
            && same_parameters(&row["parameters"], &Value::Null)?
        {
            observed = observed
                .checked_add(chain::number(&row["amount"])?)
                .ok_or(TradingError::TooLarge)?;
        }
    }
    if observed >= fee_mutez {
        return Ok(None);
    }
    Ok(Some(FeeEvasion {
        operation_hash: hash.into(),
        block_hash: block.into(),
        level,
        actor: actor.into(),
        recipient: fee_address.into(),
        expected_mutez: fee_mutez,
        observed_mutez: observed,
    }))
}

pub(super) fn log(event: &FeeEvasion) {
    use std::io::Write;
    // App stderr is the operational journal; never include cookies, tickets or signatures.
    let _ = writeln!(
        std::io::stderr().lock(),
        "{}",
        serde_json::json!({
            "event":"platform_fee_evasion", "evidence":event
        })
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trading::{
        chain::{Checked, Head, Simulation},
        types::{AcceptFees, Operation},
    };
    use serde_json::json;

    #[test]
    fn fee_evasion_round_trips_canonical_decimal_strings() {
        let evidence = FeeEvasion {
            operation_hash: "op".into(),
            block_hash: "block".into(),
            level: 2,
            actor: "tz1actor".into(),
            recipient: "tz1platform".into(),
            expected_mutez: 50,
            observed_mutez: 20,
        };
        let encoded = serde_json::to_value(&evidence).unwrap();
        assert_eq!(encoded["expected_mutez"], "50");
        assert_eq!(
            serde_json::from_value::<FeeEvasion>(encoded)
                .unwrap()
                .observed_mutez,
            20
        );
    }

    #[test]
    fn fee_evasion_rejects_noncanonical_decimal_mutez() {
        let mut encoded = json!({
            "operation_hash":"op", "block_hash":"block", "level":2,
            "actor":"tz1actor", "recipient":"tz1platform",
            "expected_mutez":"50", "observed_mutez":"20"
        });
        encoded["expected_mutez"] = json!(50);
        assert!(serde_json::from_value::<FeeEvasion>(encoded.clone()).is_err());
        encoded["expected_mutez"] = json!("050");
        assert!(serde_json::from_value::<FeeEvasion>(encoded).is_err());
    }

    fn acceptance() -> Operation {
        Operation {
            kind: "transaction",
            source: "tz1actor".into(),
            counter: "41".into(),
            destination: V2.into(),
            amount: "0".into(),
            parameters: json!({"entrypoint":"offer_accept","value":{"prim":"Unit"}}),
            fee: "100".into(),
            gas_limit: "1000".into(),
            storage_limit: "0".into(),
        }
    }

    fn applied_content(op: &Operation, counter: &str) -> Value {
        let mut row = op.rpc("tz1actor", counter.parse().unwrap());
        row["metadata"] = json!({"operation_result":{"status":"applied"}});
        row
    }

    fn classify_fee_group(
        accept: Value,
        payments: &[u64],
        recipient: &str,
    ) -> Result<Option<FeeEvasion>, TradingError> {
        let actor = "tz1actor";
        let expected = [acceptance(), Operation::transfer(actor, recipient, 50)];
        let mut contents = vec![accept];
        for (index, amount) in payments.iter().enumerate() {
            let mut payment = Operation::transfer(actor, recipient, *amount);
            payment.counter = (42 + index as u64).to_string();
            contents.push(applied_content(&payment, &payment.counter));
        }
        classify_operations(
            actor,
            &expected,
            Some((recipient, 50)),
            &contents,
            "op",
            "block",
            2,
        )
    }

    #[test]
    fn reshaped_split_payment_meeting_fee_is_classified_paid() {
        let accept = applied_content(&acceptance(), "41");
        assert!(
            classify_fee_group(accept, &[20, 30], "tz1platform")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn reshaped_excess_payment_is_classified_paid() {
        let accept = applied_content(&acceptance(), "41");
        assert!(
            classify_fee_group(accept, &[60], "tz1platform")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn reshaped_missing_payment_records_evasion() {
        let accept = applied_content(&acceptance(), "41");
        let evasion = classify_fee_group(accept, &[20], "tz1platform")
            .unwrap()
            .unwrap();
        assert_eq!(evasion.expected_mutez, 50);
        assert_eq!(evasion.observed_mutez, 20);
    }

    #[test]
    fn reshaped_group_rejects_accept_counter_or_parameters_mismatch() {
        let mut wrong_counter = applied_content(&acceptance(), "42");
        let fee = applied_content(&Operation::transfer("tz1actor", "tz1platform", 50), "43");
        assert!(matches!(
            classify_fee_group(wrong_counter.clone(), &[50], "tz1platform"),
            Err(TradingError::Changed)
        ));

        wrong_counter["counter"] = json!("41");
        wrong_counter["parameters"]["entrypoint"] = json!("offer_cancel");
        let group = vec![wrong_counter, fee];
        assert!(matches!(
            classify_operations(
                "tz1actor",
                &[
                    acceptance(),
                    Operation::transfer("tz1actor", "tz1platform", 50)
                ],
                Some(("tz1platform", 50)),
                &group,
                "op",
                "block",
                2,
            ),
            Err(TradingError::Changed)
        ));
    }

    #[test]
    fn reshaped_group_rejects_failed_internal_accept_result() {
        let mut accept = applied_content(&acceptance(), "41");
        accept["metadata"]["internal_operation_results"] = json!([{
            "result":{"status":"failed"}
        }]);
        assert!(matches!(
            classify_fee_group(accept, &[50], "tz1platform"),
            Err(TradingError::Changed)
        ));
    }

    #[test]
    fn self_fee_missing_transfer_remains_valid() {
        let accept = applied_content(&acceptance(), "41");
        let expected = [
            acceptance(),
            Operation::transfer("tz1actor", "tz1actor", 50),
        ];
        assert!(
            classify_operations(
                "tz1actor",
                &expected,
                Some(("tz1actor", 50)),
                &[accept],
                "op",
                "block",
                2,
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn missing_final_self_transfer_is_confirmed_without_fee_evasion() {
        let actor = "tz1actor";
        let accept = Operation {
            kind: "transaction",
            source: actor.into(),
            counter: "1".into(),
            destination: V2.into(),
            amount: "0".into(),
            parameters: json!({"entrypoint":"offer_accept","value":{"prim":"Unit"}}),
            fee: "100".into(),
            gas_limit: "1000".into(),
            storage_limit: "0".into(),
        };
        let transfer = Operation::transfer(actor, actor, 25);
        let plan = Plan {
            session: String::new(),
            ticket: String::new(),
            checked: Checked {
                head: Head {
                    hash: String::new(),
                    level: 1,
                },
                reviews: vec![],
                proof: String::new(),
                operations: vec![],
            },
            simulation: Simulation {
                operations: vec![accept.clone(), transfer],
                fee_mutez: 0,
                storage_burn_cap_mutez: 0,
                gas: 0,
                bytes: 0,
                counter: 0,
                proceeds_mutez: "0".into(),
                accept_fees: Some(AcceptFees {
                    price_mutez: 100,
                    royalties_mutez: 0,
                    fxhash_fee_mutez: 0,
                    platform_fee_mutez: 25,
                    platform_fee_returned_mutez: 25,
                    platform_fee_bps: 250,
                    platform_fee_address: actor.into(),
                    seller_net_mutez: 100,
                }),
            },
        };
        let content = json!({
            "kind":"transaction", "source":actor, "destination":V2,
            "amount":"0", "counter":"1", "parameters":accept.parameters,
            "metadata":{"operation_result":{"status":"applied"}}
        });
        assert!(
            classify(actor, &plan, &[content], "op", "block", 2)
                .unwrap()
                .is_none()
        );
    }
}
