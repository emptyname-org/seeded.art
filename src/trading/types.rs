use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const MAINNET: &str = "NetXdQprcVkpaWU";
pub const V2: &str = "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u";
pub const V1: &str = "KT1Xo5B7PNBAeynZPmca4bRh6LQow4og1Zb9";
pub const TOKENS: [&str; 3] = [
    "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE",
    "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi",
    "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr",
];
pub const VIEWS: [&str; 3] = ["KT1WRpPyXHSCbTfDQFWVXbMpxxvf7Y7KChxc", TOKENS[1], TOKENS[2]];
pub const MAX_ACTIONS: usize = 50;
pub const MAX_PLATFORM_FEE_BPS: u16 = 1000;
pub const MAX_GAS: u64 = 800_000;
pub const MAX_BYTES: usize = 28_000;
pub const FXHASH_TREASURY: &str = "KT1P2BXYb894MekrCcSrnidzQYPVqitLoVLc";

pub fn valid_fee_address(address: &str) -> bool {
    if address.len() != 36 {
        return false;
    }
    let Ok(decoded) = bs58::decode(address).with_check(None).into_vec() else {
        return false;
    };
    decoded.len() == 23 && matches!(&decoded[..3], [6, 161, 159 | 161 | 164] | [2, 90, 121])
}

#[derive(Clone, Debug)]
pub struct PlatformFee {
    pub bps: u16,
    pub address: Option<String>,
}
impl Default for PlatformFee {
    fn default() -> Self {
        Self {
            bps: 250,
            address: None,
        }
    }
}
impl PlatformFee {
    pub fn validate(&self) -> Result<(), TradingError> {
        if self.bps > MAX_PLATFORM_FEE_BPS
            || self
                .address
                .as_deref()
                .is_some_and(|a| !valid_fee_address(a))
        {
            return Err(TradingError::Invalid);
        }
        Ok(())
    }
    pub fn recipient(&self) -> Result<&str, TradingError> {
        self.validate()?;
        self.address.as_deref().ok_or(TradingError::FeeUnconfigured)
    }
    pub fn amount(&self, price: u64) -> Result<u64, TradingError> {
        self.validate()?;
        u64::try_from(u128::from(price) * u128::from(self.bps) / 10_000)
            .map_err(|_| TradingError::TooLarge)
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct AcceptFees {
    #[serde(serialize_with = "decimal")]
    pub price_mutez: u64,
    /// Actual royalties paid away from the seller, including each split's rounding.
    #[serde(serialize_with = "decimal")]
    pub royalties_mutez: u64,
    #[serde(serialize_with = "decimal")]
    pub fxhash_fee_mutez: u64,
    #[serde(serialize_with = "decimal")]
    pub platform_fee_mutez: u64,
    #[serde(serialize_with = "decimal")]
    pub platform_fee_returned_mutez: u64,
    pub platform_fee_bps: u16,
    pub platform_fee_address: String,
    #[serde(serialize_with = "decimal")]
    pub seller_net_mutez: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum TradingError {
    #[error("Acceptances are unavailable because the platform fee audit could not be opened")]
    FeeAuditUnavailable,
    #[error("Offer accepts are unavailable until the platform fee address is configured")]
    FeeUnconfigured,
    #[error("The platform fee contract needs a unit default entrypoint")]
    FeeRecipient,
    #[error(
        "Acceptance applied on chain without the reviewed platform fee. Do not resend the acceptance"
    )]
    FeeEvasion,
    #[error("Too many trading checks from this address or connection; retry in a minute")]
    RateLimited,
    #[error(
        "A checked operation may still be pending. Confirm it or retry in {0} seconds; do not resend it"
    )]
    SigningPending(u64),
    #[error("Trading receipt capacity is full; retry in {0} seconds")]
    ReceiptCapacity(u64),
    #[error("Operation is awaiting canonical confirmation")]
    Pending,
    #[error("Invalid trading request")]
    Invalid,
    #[error("Only the verified address may perform this action")]
    Unauthorized,
    #[error("This address does not hold token {token_id} on contract {contract}")]
    NotHeld { contract: String, token_id: u64 },
    #[error("Market state changed or is no longer active; refresh and review again")]
    Changed,
    #[error("Market code or configuration changed; trading requires a new code review")]
    CodeChanged,
    #[error("Chain data is unavailable or stale; no operation was prepared")]
    Unavailable,
    #[error("The operation failed or was dropped; nothing changed")]
    Failed,
    #[error("The operation could not be verified against its review")]
    Unverified,
    #[error("Another trading request is active; finish it before preparing another")]
    Busy,
    #[error("Review expired or was already used; refresh and prepare again")]
    Expired,
    #[error("Simulation failed; no operation is ready to sign")]
    Simulation,
    #[error("This group exceeds the resource cap; select fewer actions")]
    TooLarge,
    #[error("Reveal this address in its signing app before trading")]
    Unrevealed,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Listing,
    Token,
    Collection,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct Token {
    pub contract: String,
    pub token_id: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    Cancel {
        marketplace: String,
        kind: Kind,
        id: u64,
    },
    Accept {
        kind: Kind,
        id: u64,
        token: Token,
    },
}

impl Action {
    pub fn validate(&self) -> Result<(), TradingError> {
        match self {
            Self::Cancel {
                marketplace,
                kind,
                id,
            } if *id <= 9_007_199_254_740_991
                && (marketplace == V2 || (marketplace == V1 && *kind == Kind::Listing)) =>
            {
                Ok(())
            }
            Self::Accept { kind, id, token }
                if *kind != Kind::Listing
                    && *id <= 9_007_199_254_740_991
                    && token.token_id <= 9_007_199_254_740_991
                    && TOKENS.contains(&token.contract.as_str()) =>
            {
                Ok(())
            }
            _ => Err(TradingError::Invalid),
        }
    }
    pub fn market(&self) -> &str {
        match self {
            Self::Cancel { marketplace, .. } => marketplace,
            Self::Accept { .. } => V2,
        }
    }
    pub fn kind(&self) -> Kind {
        match self {
            Self::Cancel { kind, .. } | Self::Accept { kind, .. } => *kind,
        }
    }
    pub fn id(&self) -> u64 {
        match self {
            Self::Cancel { id, .. } | Self::Accept { id, .. } => *id,
        }
    }
    pub fn entrypoint(&self) -> &'static str {
        match self {
            Self::Cancel { marketplace, .. } if marketplace == V1 => "cancel_offer",
            Self::Cancel {
                kind: Kind::Listing,
                ..
            } => "listing_cancel",
            Self::Cancel {
                kind: Kind::Token, ..
            } => "offer_cancel",
            Self::Cancel { .. } => "collection_offer_cancel",
            Self::Accept {
                kind: Kind::Token, ..
            } => "offer_accept",
            Self::Accept { .. } => "collection_offer_accept",
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Operation {
    pub kind: &'static str,
    pub source: String,
    pub counter: String,
    pub destination: String,
    pub amount: String,
    #[serde(skip_serializing_if = "Value::is_null")]
    pub parameters: Value,
    pub fee: String,
    pub gas_limit: String,
    pub storage_limit: String,
}

impl Operation {
    pub fn transfer(source: &str, destination: &str, amount: u64) -> Self {
        Self {
            source: source.into(),
            amount: amount.to_string(),
            parameters: Value::Null,
            ..Self::transaction(destination, "default", json!({"prim":"Unit"}))
        }
    }
    fn transaction(destination: &str, entrypoint: &str, value: Value) -> Self {
        Self {
            kind: "transaction",
            source: String::new(),
            counter: String::new(),
            destination: destination.into(),
            amount: "0".into(),
            parameters: json!({"entrypoint":entrypoint,"value":value}),
            fee: "0".into(),
            gas_limit: "0".into(),
            storage_limit: "0".into(),
        }
    }
    pub fn rpc(&self, actor: &str, counter: u64) -> Value {
        let mut value = serde_json::to_value(self).expect("serializable operation");
        value["source"] = json!(actor);
        value["counter"] = json!(counter.to_string());
        value
    }
}

pub fn operation_group(actor: &str, action: &Action) -> Result<Vec<Operation>, TradingError> {
    action.validate()?;
    let value = match action {
        Action::Accept {
            kind: Kind::Collection,
            id,
            token,
        } => {
            let version = TOKENS
                .iter()
                .position(|contract| *contract == token.contract)
                .ok_or(TradingError::Invalid)?;
            json!({"prim":"Pair","args":[{"prim":"Pair","args":[{"int":token.token_id.to_string()},{"int":version.to_string()}]},{"int":id.to_string()}]})
        }
        _ => json!({"int":action.id().to_string()}),
    };
    let mut call = Operation::transaction(action.market(), action.entrypoint(), value);
    call.source = actor.into();
    match action {
        Action::Cancel { .. } => Ok(vec![call]),
        Action::Accept { token, .. } => {
            let operator = |variant| {
                let mut op = Operation::transaction(
                    &token.contract,
                    "update_operators",
                    json!([{"prim":variant,"args":[{"prim":"Pair","args":[{"string":actor},{"prim":"Pair","args":[{"string":V2},{"int":token.token_id.to_string()}]}]}]}]),
                );
                op.source = actor.into();
                op
            };
            Ok(vec![operator("Left"), call, operator("Right")])
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ReviewItem {
    pub action: Action,
    pub counterparty: String,
    #[serde(serialize_with = "decimal")]
    pub price_mutez: u64,
    #[serde(serialize_with = "decimal")]
    pub remaining: u64,
    pub created_at: String,
    pub created_level: u64,
    pub token: Option<Token>,
    pub project_id: Option<u64>,
}

pub fn tez(mutez: u64) -> String {
    format!("{}.{:06}", mutez / 1_000_000, mutez % 1_000_000)
}

pub(super) fn decimal<S: serde::Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}
