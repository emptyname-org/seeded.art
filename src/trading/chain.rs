use super::{
    http::{Endpoint, Transport},
    types::*,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Head {
    pub hash: String,
    pub level: u64,
}
#[derive(Clone)]
pub struct Checked {
    pub head: Head,
    pub reviews: Vec<ReviewItem>,
    pub proof: String,
    pub operations: Vec<Operation>,
}
#[derive(Clone, Debug, Serialize)]
pub struct Simulation {
    pub operations: Vec<Operation>,
    #[serde(serialize_with = "super::types::decimal")]
    pub fee_mutez: u64,
    #[serde(serialize_with = "super::types::decimal")]
    pub storage_burn_cap_mutez: u64,
    pub gas: u64,
    pub bytes: usize,
    pub counter: u64,
    pub proceeds_mutez: String,
    pub accept_fees: Option<AcceptFees>,
}
pub struct Chain {
    pub transport: Arc<dyn Transport>,
    pub fee: PlatformFee,
}
impl Chain {
    pub async fn rpc(&self, path: String, body: Option<Value>) -> Result<Value, TradingError> {
        self.transport.request(Endpoint::Rpc, path, body).await
    }
    pub async fn indexer(&self, path: String) -> Result<Value, TradingError> {
        self.transport.request(Endpoint::Indexer, path, None).await
    }
    pub async fn head(&self) -> Result<Head, TradingError> {
        if self.rpc("/chains/main/chain_id".into(), None).await? != MAINNET {
            return Err(TradingError::CodeChanged);
        }
        let header = self
            .rpc("/chains/main/blocks/head/header".into(), None)
            .await?;
        let level = number(&header["level"])?;
        let timestamp = timestamp(text(&header["timestamp"])?)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| TradingError::Unavailable)?
            .as_secs();
        if now.abs_diff(timestamp) > 90 {
            return Err(TradingError::Unavailable);
        }
        let hash = text(
            &self
                .rpc(format!("/chains/main/blocks/{level}/hash"), None)
                .await?,
        )?
        .to_owned();
        if hash.len() != 51 || !hash.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(TradingError::Unavailable);
        }
        let indexed = self.indexer("/head".into()).await?;
        if indexed["synced"] != true || number(&indexed["level"])?.abs_diff(level) > 2 {
            return Err(TradingError::Unavailable);
        }
        Ok(Head { hash, level })
    }
    pub async fn ancestor(&self, old: &Head, new: &Head) -> Result<(), TradingError> {
        let distance = new
            .level
            .checked_sub(old.level)
            .filter(|n| *n <= 20)
            .ok_or(TradingError::Changed)?;
        if self
            .rpc(
                format!("/chains/main/blocks/{}~{distance}/hash", new.hash),
                None,
            )
            .await?
            != old.hash
        {
            return Err(TradingError::Changed);
        }
        Ok(())
    }
    async fn bigmap(
        &self,
        head: &Head,
        map: u64,
        key: &str,
    ) -> Result<(Value, Value), TradingError> {
        if key.len() > 80 || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err(TradingError::Invalid);
        }
        let row = self
            .indexer(format!("/bigmaps/{map}/keys/{key}?micheline=2"))
            .await?;
        if row["active"] != true || number(&row["lastLevel"])? > head.level {
            return Err(TradingError::Changed);
        }
        let hash = text(&row["hash"])?;
        if hash != key_hash(key)? {
            return Err(TradingError::Changed);
        }
        if hash.len() != 54 || !hash.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(TradingError::Unavailable);
        }
        let raw = self
            .rpc(
                format!(
                    "/chains/main/blocks/{}/context/big_maps/{map}/{hash}",
                    head.hash
                ),
                None,
            )
            .await?;
        Ok((row, raw))
    }
    pub async fn inspect(&self, actor: &str, actions: &[Action]) -> Result<Checked, TradingError> {
        self.inspect_with_hint(actor, actions, None).await
    }
    pub async fn inspect_with_hint(
        &self,
        actor: &str,
        actions: &[Action],
        hint: Option<&Action>,
    ) -> Result<Checked, TradingError> {
        if actions.is_empty() || actions.len() > MAX_ACTIONS {
            return Err(TradingError::Invalid);
        }
        let mut unique = HashSet::new();
        let accepts = actions
            .iter()
            .filter(|a| matches!(a, Action::Accept { .. }))
            .count();
        if accepts > 1 || (accepts == 1 && actions.len() > 2) {
            return Err(TradingError::Invalid);
        }
        if accepts > 0 {
            self.fee.recipient()?;
        }
        for action in actions {
            action.validate()?;
            if !unique.insert((action.market(), action.kind(), action.id())) {
                return Err(TradingError::Invalid);
            }
        }
        let head = self.head().await?;
        if accepts > 0 {
            let address = self.fee.recipient()?;
            if address.starts_with("KT1") {
                let entrypoint = self.rpc(format!(
                    "/chains/main/blocks/{}/context/contracts/{address}/entrypoints/default", head.hash
                ), None).await.map_err(|error| {
                    if matches!(error, TradingError::Changed) {
                        TradingError::FeeRecipient
                    } else {
                        error
                    }
                })?;
                if !unit_default_entrypoint(&entrypoint)? {
                    return Err(TradingError::FeeRecipient);
                }
            }
        }
        let mut effective = actions.to_vec();
        if let Some(hint) = hint {
            let [Action::Accept { token, .. }] = actions else {
                return Err(TradingError::Invalid);
            };
            let Action::Cancel {
                marketplace,
                kind: Kind::Listing,
                id,
            } = hint
            else {
                return Err(TradingError::Invalid);
            };
            hint.validate()?;
            let map = if marketplace == V1 { 22799 } else { 149787 };
            let hash = key_hash(&id.to_string())?;
            let raw = self
                .rpc(
                    format!(
                        "/chains/main/blocks/{}/context/big_maps/{map}/{hash}",
                        head.hash
                    ),
                    None,
                )
                .await;
            match raw {
                Err(TradingError::Changed) => {} // Confirmed absent at this fixed block; simulation still proves ownership.
                Err(e) => return Err(e),
                Ok(raw) => {
                    let v = leaves(&raw)?;
                    let active = if marketplace == V1 {
                        if v.len() != 6 {
                            return Err(TradingError::Changed);
                        }
                        number(&v[2])? > 0
                    } else {
                        if v.len() != 4 {
                            return Err(TradingError::Changed);
                        }
                        true
                    };
                    if active {
                        let (owner, listed) = if marketplace == V1 {
                            (
                                text(&v[1])?,
                                Token {
                                    contract: TOKENS[0].into(),
                                    token_id: number(&v[3])?,
                                },
                            )
                        } else {
                            (text(&v[3])?, token_ref(&v[0], &v[1])?)
                        };
                        if owner != actor || listed != *token {
                            return Err(TradingError::Changed);
                        }
                        effective.insert(0, hint.clone());
                    }
                }
            }
        }
        let actions = effective.as_slice();
        let mut proofs = Vec::new();
        let mut reviews = Vec::new();
        let mut operations = Vec::new();
        let mut platform_transfer = None;
        let pins: Value = serde_json::from_str(include_str!("pins.json")).expect("compiled pins");
        let mut scripts = BTreeMap::new();
        let mut checked_eps = HashSet::new();
        let mut checked_versions = HashSet::new();
        for action in actions {
            if !scripts.contains_key(action.market()) {
                let script = self
                    .rpc(
                        format!(
                            "/chains/main/blocks/{}/context/contracts/{}/script",
                            head.hash,
                            action.market()
                        ),
                        None,
                    )
                    .await?;
                let expected = if action.market() == V2 {
                    "1dc67ad05ad65658df838f26716f3ae532d301f56096640495f749815133e2d1"
                } else {
                    "5db80ed5a9c23a8b13a240726c1a68a39dc7331b477781019965b3784c6e7153"
                };
                if digest(&script["code"]) != expected {
                    return Err(TradingError::CodeChanged);
                }
                let ty = script["code"]
                    .as_array()
                    .and_then(|a| a.iter().find(|v| v["prim"] == "storage"))
                    .and_then(|v| v["args"].get(0))
                    .ok_or(TradingError::CodeChanged)?;
                let fields = fields(ty, &script["storage"])?;
                if fields.get("fees").and_then(|v| v.as_str()) != Some("25")
                    || fields.get("treasury").and_then(|v| v.as_str())
                        != Some("KT1P2BXYb894MekrCcSrnidzQYPVqitLoVLc")
                {
                    return Err(TradingError::CodeChanged);
                }
                if action.market() == V2 {
                    for (name, n) in [
                        ("listings", 149787),
                        ("offers", 149789),
                        ("collection_offers", 149783),
                        ("entrypoints_enabled", 149784),
                        ("gentk_contracts", 149786),
                    ] {
                        if fields.get(name).and_then(|v| v.as_str()) != Some(n.to_string().as_str())
                        {
                            return Err(TradingError::CodeChanged);
                        }
                    }
                    if fields.get("paused") != Some(&json!(false)) {
                        return Err(TradingError::Changed);
                    }
                    // The unannotated final field is the mutable entrypoint big map.
                    if leaves(&script["storage"])?.last() != Some(&json!("149790")) {
                        return Err(TradingError::CodeChanged);
                    }
                } else if fields.get("enabled") != Some(&json!(true))
                    || fields.get("offers") != Some(&json!("22799"))
                    || fields.get("objkts") != Some(&json!(TOKENS[0]))
                {
                    return Err(TradingError::CodeChanged);
                }
                scripts.insert(action.market().to_owned(), script);
            }
            if action.market() == V2 && checked_eps.insert(action.entrypoint()) {
                let id = match action.entrypoint() {
                    "collection_offer_accept" => 5,
                    "collection_offer_cancel" => 6,
                    "listing_cancel" => 9,
                    "offer_accept" => 11,
                    "offer_cancel" => 12,
                    _ => return Err(TradingError::Invalid),
                };
                let (row, lambda) = self.bigmap(&head, 149790, &id.to_string()).await?;
                let pin = &pins[id.to_string()];
                if row["lastLevel"] != pin["level"] || digest(&lambda) != text(&pin["sha256"])? {
                    return Err(TradingError::CodeChanged);
                }
                let (_, enabled) = self.bigmap(&head, 149784, action.entrypoint()).await?;
                if enabled["prim"] != "True" {
                    return Err(TradingError::Changed);
                }
                proofs.push(json!([id, digest(&lambda)]));
            }
            let map = match (action.market(), action.kind()) {
                (V1, _) => 22799,
                (_, Kind::Listing) => 149787,
                (_, Kind::Token) => 149789,
                (_, Kind::Collection) => 149783,
            };
            let (row, raw) = self.bigmap(&head, map, &action.id().to_string()).await?;
            let values = leaves(&raw)?;
            let (counterparty, price, remaining, token, project) = if action.market() == V1 {
                if values.len() != 6 {
                    return Err(TradingError::Changed);
                }
                (
                    text(&values[1])?.to_owned(),
                    number(&values[4])?,
                    number(&values[2])?,
                    Some(Token {
                        contract: TOKENS[0].into(),
                        token_id: number(&values[3])?,
                    }),
                    None,
                )
            } else {
                match action.kind() {
                    Kind::Listing => {
                        if values.len() != 4 {
                            return Err(TradingError::Changed);
                        }
                        (
                            text(&values[3])?.to_owned(),
                            number(&values[2])?,
                            1,
                            Some(token_ref(&values[0], &values[1])?),
                            None,
                        )
                    }
                    Kind::Token => {
                        if values.len() != 4 {
                            return Err(TradingError::Changed);
                        }
                        (
                            text(&values[0])?.to_owned(),
                            number(&values[3])?,
                            1,
                            Some(token_ref(&values[1], &values[2])?),
                            None,
                        )
                    }
                    Kind::Collection => {
                        if values.len() != 4 {
                            return Err(TradingError::Changed);
                        }
                        (
                            text(&values[1])?.to_owned(),
                            number(&values[3])?,
                            number(&values[0])?,
                            None,
                            Some(number(&values[2])?),
                        )
                    }
                }
            };
            if remaining == 0 || price.checked_mul(remaining).is_none() {
                return Err(TradingError::Changed);
            }
            let mapped_token = match action {
                Action::Accept { token, .. } => Some(token),
                _ if action.market() == V2 => token.as_ref(),
                _ => None,
            };
            if let Some(mapped) = mapped_token {
                let selected = TOKENS
                    .iter()
                    .position(|c| *c == mapped.contract)
                    .ok_or(TradingError::Invalid)?;
                let versions: Vec<usize> = if matches!(action, Action::Accept { .. }) {
                    vec![0, 1, 2]
                } else {
                    vec![selected]
                };
                for version in versions {
                    if checked_versions.insert(version) {
                        let (_, mapping) = self.bigmap(&head, 149786, &version.to_string()).await?;
                        if leaves(&mapping)? != vec![json!(TOKENS[version]), json!(VIEWS[version])]
                        {
                            return Err(TradingError::CodeChanged);
                        }
                        proofs.push(mapping);
                    }
                }
            }
            match action {
                Action::Cancel { .. } if counterparty != actor => {
                    return Err(TradingError::Unauthorized);
                }
                Action::Accept { token: chosen, .. } => {
                    if counterparty == actor || token.as_ref().is_some_and(|t| t != chosen) {
                        return Err(TradingError::Unauthorized);
                    }
                    // FA2 transfer authorization and collection membership are checked by the
                    // pinned accept lambda and token contract during the complete atomic simulation.
                    let balances=self.indexer(format!("/tokens/balances?account={actor}&token.contract={}&token.tokenId={}&balance.gt=0&limit=2",chosen.contract,chosen.token_id)).await?;
                    let owned = balances
                        .as_array()
                        .is_some_and(|rows| rows.len() == 1 && rows[0]["balance"] == "1");
                    let reclaimed = actions.first().is_some_and(|a| {
                        matches!(
                            a,
                            Action::Cancel {
                                kind: Kind::Listing,
                                ..
                            }
                        )
                    }) && reviews
                        .first()
                        .is_some_and(|r: &ReviewItem| r.token.as_ref() == Some(chosen));
                    if !owned && !reclaimed {
                        return Err(TradingError::NotHeld {
                            contract: chosen.contract.clone(),
                            token_id: chosen.token_id,
                        });
                    }
                }
                _ => {}
            }
            let created_level = number(&row["firstLevel"])?;
            let block = self
                .indexer(format!("/blocks/{created_level}?select=timestamp"))
                .await?;
            let created_at = text(block.get("timestamp").unwrap_or(&block))?.to_owned();
            proofs.push(json!([action, &raw, row["firstLevel"], row["lastLevel"]]));
            reviews.push(ReviewItem {
                action: action.clone(),
                counterparty,
                price_mutez: price,
                remaining,
                created_at,
                created_level,
                token,
                project_id: project,
            });
            operations.extend(operation_group(actor, action)?);
            if matches!(action, Action::Accept { .. }) {
                let address = self.fee.recipient()?;
                let amount = self.fee.amount(price)?;
                proofs.push(json!(["platform_fee", self.fee.bps, address, amount]));
                // A rounded zero has no minimum and needs no transfer to an implicit account.
                if amount > 0 {
                    platform_transfer = Some(Operation::transfer(actor, address, amount));
                }
            }
        }
        operations.extend(platform_transfer);
        Ok(Checked {
            head,
            reviews,
            proof: digest(&json!(proofs)),
            operations,
        })
    }
    pub async fn simulate(
        &self,
        actor: &str,
        head: &Head,
        mut operations: Vec<Operation>,
    ) -> Result<Simulation, TradingError> {
        let prefix = format!("/chains/main/blocks/{}", head.hash);
        if self
            .rpc(
                format!("{prefix}/context/contracts/{actor}/manager_key"),
                None,
            )
            .await?
            .is_null()
        {
            return Err(TradingError::Unrevealed);
        }
        let counter = number(
            &self
                .rpc(format!("{prefix}/context/contracts/{actor}/counter"), None)
                .await?,
        )?;
        let constants = self
            .rpc(format!("{prefix}/context/constants"), None)
            .await?;
        let cost = number(&constants["cost_per_byte"])?;
        if operations.is_empty() || operations.len() > MAX_ACTIONS {
            return Err(TradingError::TooLarge);
        }
        let per_gas = MAX_GAS / operations.len() as u64;
        for (i, op) in operations.iter_mut().enumerate() {
            op.counter = counter
                .checked_add(i as u64 + 1)
                .ok_or(TradingError::TooLarge)?
                .to_string();
            op.gas_limit = per_gas.to_string();
            op.storage_limit = "1000".into();
        }
        let contents = rpc_contents(&operations, actor, counter)?;
        let signature = bs58::encode([vec![4, 130, 43], vec![0; 64]].concat())
            .with_check()
            .into_string();
        let simulated=self.rpc(format!("{prefix}/helpers/scripts/simulate_operation"),Some(json!({"chain_id":MAINNET,"latency":0,"operation":{"branch":head.hash,"contents":contents,"signature":signature}}))).await?;
        let results = simulated["contents"]
            .as_array()
            .filter(|r| r.len() == operations.len())
            .ok_or(TradingError::Simulation)?;
        let accept_fees = self.accept_fees(actor, &operations, results)?;
        let mut gas = 0u64;
        let mut storage = 0u64;
        let mut proceeds = 0u64;
        for (op, result) in operations.iter_mut().zip(results) {
            let mut used = 0u64;
            let mut paid = 0u64;
            collect_result(
                &result["metadata"]["operation_result"],
                &mut used,
                &mut paid,
            )?;
            if let Some(internals) = result["metadata"]["internal_operation_results"].as_array() {
                if internals.len() > 128 {
                    return Err(TradingError::TooLarge);
                }
                for internal in internals {
                    collect_result(&internal["result"], &mut used, &mut paid)?;
                    if internal["kind"] == "transaction" && internal["destination"] == actor {
                        proceeds = proceeds
                            .checked_add(number(&internal["amount"])?)
                            .ok_or(TradingError::TooLarge)?;
                    }
                }
            }
            let limit = used.checked_add(500).ok_or(TradingError::TooLarge)?;
            let storage_limit = paid.checked_add(64).ok_or(TradingError::TooLarge)?;
            if limit > per_gas || storage_limit > 1000 {
                return Err(TradingError::TooLarge);
            }
            op.gas_limit = limit.to_string();
            op.storage_limit = storage_limit.to_string();
            gas = gas.checked_add(limit).ok_or(TradingError::TooLarge)?;
            storage = storage
                .checked_add(storage_limit)
                .ok_or(TradingError::TooLarge)?;
        }
        // Forge the group once; bound each transaction encoding independently so
        // batching does not multiply the entire group size into every fee.
        let forged = self
            .rpc(
                format!("{prefix}/helpers/forge/operations"),
                Some(
                    json!({"branch":head.hash,"contents":rpc_contents(&operations,actor,counter)?}),
                ),
            )
            .await?;
        let bytes = text(&forged)?.len() / 2 + 64 + operations.len() * 40;
        if bytes > MAX_BYTES || gas > MAX_GAS {
            return Err(TradingError::TooLarge);
        }
        let filter = self.rpc("/chains/main/mempool/filter".into(), None).await?;
        let minimum = number(&filter["minimal_fees"])?;
        let mut fee = 0u64;
        for (index, op) in operations.iter_mut().enumerate() {
            // The fixed bound includes addresses and maximally encoded counters,
            // fees and limits; these fixed ASCII parameters encode shorter than JSON.
            let encoded =
                op.parameters.to_string().len() as u64 + 160 + if index == 0 { 96 } else { 0 };
            let n = minimum
                .checked_add(100)
                .and_then(|n| {
                    n.checked_add(
                        rate(
                            &filter["minimal_nanotez_per_gas_unit"],
                            number(&json!(op.gas_limit)).ok()?,
                        )
                        .ok()?,
                    )
                })
                .and_then(|n| {
                    n.checked_add(rate(&filter["minimal_nanotez_per_byte"], encoded).ok()?)
                })
                .ok_or(TradingError::TooLarge)?;
            op.fee = n.to_string();
            fee = fee.checked_add(n).ok_or(TradingError::TooLarge)?;
        }
        if fee > 2_000_000 {
            return Err(TradingError::TooLarge);
        }
        let burn = storage.checked_mul(cost).ok_or(TradingError::TooLarge)?;
        let final_run=self.rpc(format!("{prefix}/helpers/scripts/simulate_operation"),Some(json!({"chain_id":MAINNET,"latency":0,"operation":{"branch":head.hash,"contents":rpc_contents(&operations,actor,counter)?,"signature":signature}}))).await?;
        let final_results = final_run["contents"]
            .as_array()
            .filter(|r| r.len() == operations.len())
            .ok_or(TradingError::Simulation)?;
        if self.accept_fees(actor, &operations, final_results)? != accept_fees {
            return Err(TradingError::Simulation);
        }
        for (result, op) in final_results.iter().zip(&operations) {
            let mut used = 0;
            let mut paid = 0;
            collect_result(
                &result["metadata"]["operation_result"],
                &mut used,
                &mut paid,
            )?;
            if let Some(internals) = result["metadata"]["internal_operation_results"].as_array() {
                if internals.len() > 128 {
                    return Err(TradingError::TooLarge);
                }
                for i in internals {
                    collect_result(&i["result"], &mut used, &mut paid)?;
                }
            }
            if used > number(&json!(op.gas_limit))? || paid > number(&json!(op.storage_limit))? {
                return Err(TradingError::Simulation);
            }
        }
        Ok(Simulation {
            operations,
            fee_mutez: fee,
            storage_burn_cap_mutez: burn,
            gas,
            bytes,
            counter,
            proceeds_mutez: proceeds.to_string(),
            accept_fees,
        })
    }

    fn accept_fees(
        &self,
        actor: &str,
        operations: &[Operation],
        results: &[Value],
    ) -> Result<Option<AcceptFees>, TradingError> {
        let mut accepted = None;
        for (operation, result) in operations.iter().zip(results) {
            if operation.destination != V2
                || !matches!(
                    operation.parameters["entrypoint"].as_str(),
                    Some("offer_accept" | "collection_offer_accept")
                )
            {
                continue;
            }
            if accepted.is_some() {
                return Err(TradingError::Invalid);
            }
            let address = self.fee.recipient()?;
            let internals = result["metadata"]["internal_operation_results"]
                .as_array()
                .filter(|rows| rows.len() <= 128)
                .ok_or(TradingError::Simulation)?;
            let (mut price, mut seller, mut treasury) = (0u64, 0u64, 0u64);
            for row in internals {
                if row["kind"] != "transaction" || row["source"] != V2 {
                    continue;
                }
                let amount = number(&row["amount"])?;
                price = price.checked_add(amount).ok_or(TradingError::TooLarge)?;
                if row["destination"] == actor {
                    seller = seller.checked_add(amount).ok_or(TradingError::TooLarge)?;
                }
                if row["destination"] == FXHASH_TREASURY {
                    treasury = treasury.checked_add(amount).ok_or(TradingError::TooLarge)?;
                }
            }
            let fxhash_fee = (u128::from(price) * 25 / 1000) as u64;
            if treasury < fxhash_fee {
                return Err(TradingError::Simulation);
            }
            let royalties = price
                .checked_sub(fxhash_fee)
                .and_then(|n| n.checked_sub(seller))
                .ok_or(TradingError::Simulation)?;
            let platform_fee = self.fee.amount(price)?;
            let mut returned = if address == actor { platform_fee } else { 0 };
            let transfers: Vec<_> = operations
                .iter()
                .filter(|op| op.parameters.is_null())
                .collect();
            if platform_fee == 0 {
                if !transfers.is_empty() {
                    return Err(TradingError::Simulation);
                }
            } else if transfers.len() != 1
                || operations.last().is_none_or(|op| {
                    op.source != actor
                        || op.destination != address
                        || op.amount != platform_fee.to_string()
                        || !op.parameters.is_null()
                })
            {
                return Err(TradingError::Simulation);
            }
            if platform_fee > 0 {
                if let Some(rows) = results
                    .last()
                    .and_then(|r| r["metadata"]["internal_operation_results"].as_array())
                {
                    if rows.len() > 128 {
                        return Err(TradingError::TooLarge);
                    }
                    for row in rows {
                        if row["kind"] == "transaction" && row["destination"] == actor {
                            returned = returned
                                .checked_add(number(&row["amount"])?)
                                .ok_or(TradingError::TooLarge)?;
                        }
                    }
                }
            }
            let seller_net = seller
                .checked_add(returned)
                .and_then(|n| n.checked_sub(platform_fee))
                .ok_or(TradingError::Simulation)?;
            accepted = Some(AcceptFees {
                price_mutez: price,
                royalties_mutez: royalties,
                fxhash_fee_mutez: fxhash_fee,
                platform_fee_mutez: platform_fee,
                platform_fee_returned_mutez: returned,
                platform_fee_bps: self.fee.bps,
                platform_fee_address: address.to_owned(),
                seller_net_mutez: seller_net,
            });
        }
        if accepted.is_none() && operations.iter().any(|op| op.parameters.is_null()) {
            return Err(TradingError::Invalid);
        }
        Ok(accepted)
    }
}
fn rpc_contents(ops: &[Operation], actor: &str, counter: u64) -> Result<Vec<Value>, TradingError> {
    ops.iter()
        .enumerate()
        .map(|(i, op)| {
            counter
                .checked_add(i as u64 + 1)
                .map(|n| op.rpc(actor, n))
                .ok_or(TradingError::TooLarge)
        })
        .collect()
}
fn collect_result(value: &Value, gas: &mut u64, storage: &mut u64) -> Result<(), TradingError> {
    if value["status"] != "applied" {
        return Err(TradingError::Simulation);
    }
    *gas = gas
        .checked_add(number(&value["consumed_milligas"])?.div_ceil(1000))
        .ok_or(TradingError::TooLarge)?;
    let paid = value
        .get("paid_storage_size_diff")
        .map(number)
        .transpose()?
        .unwrap_or(0);
    *storage = storage
        .checked_add(
            paid.checked_add(if value["allocated_destination_contract"] == true {
                257
            } else {
                0
            })
            .ok_or(TradingError::TooLarge)?,
        )
        .ok_or(TradingError::TooLarge)?;
    Ok(())
}
fn token_ref(id: &Value, version: &Value) -> Result<Token, TradingError> {
    Ok(Token {
        contract: TOKENS
            .get(number(version)? as usize)
            .ok_or(TradingError::CodeChanged)?
            .to_string(),
        token_id: number(id)?,
    })
}
pub fn number(value: &Value) -> Result<u64, TradingError> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
        .ok_or(TradingError::Unavailable)
}
pub fn text(value: &Value) -> Result<&str, TradingError> {
    value.as_str().ok_or(TradingError::Unavailable)
}
pub fn digest(value: &Value) -> String {
    hex::encode(Sha256::digest(value.to_string().as_bytes()))
}
fn leaves(value: &Value) -> Result<Vec<Value>, TradingError> {
    fn visit(v: &Value, out: &mut Vec<Value>) -> Result<(), TradingError> {
        if out.len() > 128 {
            return Err(TradingError::TooLarge);
        }
        if let Some(a) = v.as_array().or_else(|| {
            (v["prim"] == "Pair")
                .then(|| v["args"].as_array())
                .flatten()
        }) {
            for v in a {
                visit(v, out)?;
            }
            return Ok(());
        }
        if let Some(s) = v.get("int").or_else(|| v.get("string")) {
            out.push(s.clone());
            return Ok(());
        }
        if let Some(s) = v["bytes"].as_str() {
            out.push(json!(decode_address(s)?));
            return Ok(());
        }
        if v["prim"] == "True" || v["prim"] == "False" {
            out.push(json!(v["prim"] == "True"));
            return Ok(());
        }
        Err(TradingError::Unavailable)
    }
    let mut out = Vec::new();
    visit(value, &mut out)?;
    Ok(out)
}
fn decode_address(s: &str) -> Result<String, TradingError> {
    let bytes = hex::decode(s).map_err(|_| TradingError::Unavailable)?;
    if bytes.len() != 22 {
        return Err(TradingError::Unavailable);
    }
    let (prefix, payload) = match (bytes[0], bytes[1]) {
        (0, 0) => (vec![6, 161, 159], &bytes[2..]),
        (0, 1) => (vec![6, 161, 161], &bytes[2..]),
        (0, 2) => (vec![6, 161, 164], &bytes[2..]),
        (1, _) if bytes[21] == 0 => (vec![2, 90, 121], &bytes[1..21]),
        _ => return Err(TradingError::Unavailable),
    };
    Ok(bs58::encode([prefix, payload.to_vec()].concat())
        .with_check()
        .into_string())
}
fn fields(ty: &Value, value: &Value) -> Result<BTreeMap<String, Value>, TradingError> {
    fn names(ty: &Value, out: &mut Vec<String>) {
        if ty["prim"] == "pair" {
            if let Some(a) = ty["args"].as_array() {
                for t in a {
                    names(t, out);
                }
            }
        } else {
            out.push(
                ty["annots"]
                    .as_array()
                    .and_then(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .find_map(|s| s.strip_prefix('%'))
                    })
                    .unwrap_or("")
                    .into(),
            );
        }
    }
    let mut keys = Vec::new();
    names(ty, &mut keys);
    let values = leaves(value)?;
    if keys.len() != values.len() {
        return Err(TradingError::CodeChanged);
    }
    Ok(keys.into_iter().zip(values).collect())
}
fn timestamp(raw: &str) -> Result<u64, TradingError> {
    let b = raw.as_bytes();
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || !raw.ends_with('Z')
    {
        return Err(TradingError::Unavailable);
    }
    let n = |start, end| {
        raw.get(start..end)
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or(TradingError::Unavailable)
    };
    let y = n(0, 4)?;
    let m = n(5, 7)?;
    let d = n(8, 10)?;
    let h = n(11, 13)?;
    let min = n(14, 16)?;
    let sec = n(17, 19)?;
    if !(1970..=2100).contains(&y)
        || !(1..=12).contains(&m)
        || !(1..=31).contains(&d)
        || h > 23
        || min > 59
        || sec > 60
    {
        return Err(TradingError::Unavailable);
    }
    let y = y - i64::from(m <= 2);
    let era = y / 400;
    let yoe = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let days = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
    u64::try_from(days * 86400 + h * 3600 + min * 60 + sec).map_err(|_| TradingError::Unavailable)
}

/// Normalize RPC comb pairs and optimized addresses without losing left-pair structure.
pub fn normalized(value: &Value) -> Result<Value, TradingError> {
    if let Some(a) = value.as_array() {
        return a
            .iter()
            .map(normalized)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array);
    }
    if let Some(bytes) = value["bytes"].as_str() {
        if bytes.len() == 44 {
            if let Ok(address) = decode_address(bytes) {
                return Ok(json!({"string":address}));
            }
        }
    }
    let mut v = value.clone();
    if let Some(args) = value["args"].as_array() {
        let mut args = args.iter().map(normalized).collect::<Result<Vec<_>, _>>()?;
        if value["prim"] == "Pair" && args.len() > 2 {
            let first = args.remove(0);
            v["args"] = json!([first, normalized(&json!({"prim":"Pair","args":args}))?]);
        } else {
            v["args"] = json!(args);
        }
    }
    Ok(v)
}

pub fn unit_default_entrypoint(value: &Value) -> Result<bool, TradingError> {
    fn strip_annots(value: &mut Value) {
        match value {
            Value::Array(values) => values.iter_mut().for_each(strip_annots),
            Value::Object(fields) => {
                fields.remove("annots");
                fields.values_mut().for_each(strip_annots);
            }
            _ => {}
        }
    }
    let mut value = normalized(value)?;
    strip_annots(&mut value);
    Ok(value == json!({"prim":"unit"}))
}

fn key_hash(key: &str) -> Result<String, TradingError> {
    let mut packed = vec![5];
    if let Ok(mut n) = key.parse::<u64>() {
        packed.push(0);
        let first = (n & 63) as u8;
        n >>= 6;
        packed.push(first | if n > 0 { 128 } else { 0 });
        while n > 0 {
            let next = (n & 127) as u8;
            n >>= 7;
            packed.push(next | if n > 0 { 128 } else { 0 });
        }
    } else {
        packed.push(1);
        let n = u32::try_from(key.len()).map_err(|_| TradingError::Invalid)?;
        packed.extend(n.to_be_bytes());
        packed.extend(key.as_bytes());
    }
    let hash = <blake2::Blake2b<blake2::digest::consts::U32> as blake2::Digest>::digest(packed);
    Ok(bs58::encode([vec![13, 44, 64, 27], hash.to_vec()].concat())
        .with_check()
        .into_string())
}

fn rate(ratio: &Value, units: u64) -> Result<u64, TradingError> {
    let numerator = number(&ratio[0])? as u128;
    let denominator = number(&ratio[1])? as u128;
    if denominator == 0 {
        return Err(TradingError::Unavailable);
    }
    let value = (u128::from(units) * numerator).div_ceil(denominator * 1000);
    u64::try_from(value).map_err(|_| TradingError::TooLarge)
}
