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

/// User sentences without their number; the web layer appends "(error N)." from `number`.
#[derive(Debug, thiserror::Error)]
pub enum TradingError {
    #[error("Accepting offers is unavailable right now")]
    FeeAuditUnavailable,
    #[error("Accepting offers is unavailable right now")]
    FeeUnconfigured,
    #[error("Accepting offers is unavailable right now")]
    FeeRecipient,
    /// Never shown: the page treats the acceptance as done; the server logs it.
    #[error("Acceptance applied without the reviewed platform fee")]
    FeeEvasion,
    #[error("Too many requests; wait a minute")]
    RateLimited,
    #[error("Your previous operation is still pending; wait {0} seconds")]
    SigningPending(u64),
    #[error("Busy; try again in {0} seconds")]
    ReceiptCapacity(u64),
    /// A status, shown without its number.
    #[error("Waiting for confirmation…")]
    Pending,
    #[error("Request failed")]
    Invalid,
    #[error("Sign in again")]
    Unauthorized,
    #[error("Your address doesn't hold that NFT")]
    NotHeld {
        // Read through Debug in the trading step log and by tests.
        #[allow(dead_code)]
        contract: String,
        #[allow(dead_code)]
        token_id: u64,
    },
    #[error("This listing or offer changed; review it again")]
    Changed,
    #[error("Trading is unavailable right now")]
    CodeChanged,
    #[error("Trading is unavailable right now; try again shortly")]
    Unavailable,
    #[error("The operation failed; nothing changed")]
    Failed,
    #[error("The operation couldn't be confirmed; check it in your wallet")]
    Unverified,
    #[error("Another request is still running; wait for it")]
    Busy,
    #[error("The review expired; review again")]
    Expired,
    #[error("This can't be done right now")]
    Simulation,
    #[error("Too many items; select fewer")]
    TooLarge,
    #[error("Your address must make one transaction before it can trade here")]
    Unrevealed,
}

impl TradingError {
    /// The fixed number shown with the sentence and logged; docs/errors.md lists them.
    pub fn number(&self) -> u16 {
        match self {
            Self::FeeAuditUnavailable => 221,
            Self::FeeUnconfigured => 222,
            Self::FeeRecipient => 223,
            Self::RateLimited => 224,
            Self::SigningPending(_) => 225,
            Self::ReceiptCapacity(_) => 226,
            Self::Invalid => 227,
            Self::Unauthorized => 228,
            Self::NotHeld { .. } => 229,
            Self::Changed => 230,
            Self::CodeChanged => 231,
            Self::Unavailable => 232,
            Self::Busy => 233,
            Self::Expired => 234,
            Self::Simulation => 235,
            Self::TooLarge => 236,
            Self::Unrevealed => 237,
            Self::Failed => 301,
            Self::Unverified => 302,
            Self::Pending => 310,
            Self::FeeEvasion => 320,
        }
    }
    /// The variant name, for the journal: carries no address or token.
    pub fn name(&self) -> &'static str {
        match self {
            Self::FeeAuditUnavailable => "FeeAuditUnavailable",
            Self::FeeUnconfigured => "FeeUnconfigured",
            Self::FeeRecipient => "FeeRecipient",
            Self::FeeEvasion => "FeeEvasion",
            Self::RateLimited => "RateLimited",
            Self::SigningPending(_) => "SigningPending",
            Self::ReceiptCapacity(_) => "ReceiptCapacity",
            Self::Pending => "Pending",
            Self::Invalid => "Invalid",
            Self::Unauthorized => "Unauthorized",
            Self::NotHeld { .. } => "NotHeld",
            Self::Changed => "Changed",
            Self::CodeChanged => "CodeChanged",
            Self::Unavailable => "Unavailable",
            Self::Failed => "Failed",
            Self::Unverified => "Unverified",
            Self::Busy => "Busy",
            Self::Expired => "Expired",
            Self::Simulation => "Simulation",
            Self::TooLarge => "TooLarge",
            Self::Unrevealed => "Unrevealed",
        }
    }
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
