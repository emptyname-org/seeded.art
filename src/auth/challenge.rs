use blake2::{
    Blake2bMac,
    digest::{Mac, consts::U32},
};
use serde::{Deserialize, Serialize};

use super::{AuthError, CHALLENGE_LIFETIME, TrustedClientIp, crypto};

pub(super) const MAX_COOKIE_LENGTH: usize = 3072;

/// No server-side state is allocated until a signature proves key ownership.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Challenge {
    pub client: TrustedClientIp,
    pub address: String,
    pub public_key: String,
    pub csrf: String,
    pub nonce: String,
    pub issued: u64,
    /// Milliseconds since this process's AuthState was created; immune to clock changes.
    pub deadline: u64,
    pub generation: u64,
}

impl Challenge {
    pub fn message(&self, origin: &str) -> String {
        format!(
            "Tezos Signed Message: {origin}\nSign in to seeded.art\nAddress: {}\nNonce: {}\nIssued at: {}\nExpires at: {}\nThis signature signs in only. It does not authorize a transaction or sign future profile edits.",
            self.address,
            self.nonce,
            self.issued,
            self.expires_at()
        )
    }

    pub fn payload(&self, origin: &str) -> String {
        crypto::packed_message(&self.message(origin))
    }

    pub fn expires_at(&self) -> u64 {
        self.issued + CHALLENGE_LIFETIME.as_secs()
    }

    pub fn seal(&self, secret: &[u8; 32], origin: &str) -> Result<String, AuthError> {
        let bytes = serde_json::to_vec(self).map_err(|_| AuthError::Unavailable)?;
        let tag = mac(secret, origin, &bytes).finalize().into_bytes();
        let cookie = format!("{}.{}", hex::encode(bytes), hex::encode(tag));
        if cookie.len() > MAX_COOKIE_LENGTH {
            return Err(AuthError::Unavailable);
        }
        Ok(cookie)
    }

    pub fn open(cookie: &str, secret: &[u8; 32], origin: &str) -> Result<Self, AuthError> {
        if cookie.len() > MAX_COOKIE_LENGTH {
            return Err(AuthError::Expired);
        }
        let (data, tag) = cookie.split_once('.').ok_or(AuthError::Expired)?;
        if tag.len() != 64 {
            return Err(AuthError::Expired);
        }
        let bytes = hex::decode(data).map_err(|_| AuthError::Expired)?;
        let tag = hex::decode(tag).map_err(|_| AuthError::Expired)?;
        mac(secret, origin, &bytes)
            .verify_slice(&tag)
            .map_err(|_| AuthError::Expired)?;
        serde_json::from_slice(&bytes).map_err(|_| AuthError::Expired)
    }
}

fn mac(secret: &[u8; 32], origin: &str, bytes: &[u8]) -> Blake2bMac<U32> {
    let mut mac = Blake2bMac::<U32>::new_from_slice(secret).expect("fixed BLAKE2b key length");
    mac.update(b"seeded.art challenge v1\0");
    mac.update(&(origin.len() as u64).to_be_bytes());
    mac.update(origin.as_bytes());
    mac.update(bytes);
    mac
}
