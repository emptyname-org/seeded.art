//! Tezos signs BLAKE2b-256 of the complete packed Micheline string.
use blake2::{
    Blake2bVar,
    digest::{Update, VariableOutput},
};
use k256::ecdsa::signature::hazmat::PrehashVerifier;

use super::AuthError;

const KEY_PREFIXES: [(&[u8], &[u8], usize); 3] = [
    (&[13, 15, 37, 217], &[6, 161, 159], 32),
    (&[3, 254, 226, 86], &[6, 161, 161], 33),
    (&[3, 178, 139, 127], &[6, 161, 164], 33),
];
const SIGNATURE_PREFIXES: [&[u8]; 3] = [
    &[9, 245, 205, 134, 18],
    &[13, 115, 101, 19, 63],
    &[54, 240, 44, 52],
];

pub fn packed_message(message: &str) -> String {
    let mut bytes = Vec::with_capacity(message.len() + 6);
    bytes.extend_from_slice(&[5, 1]);
    bytes.extend_from_slice(&(message.len() as u32).to_be_bytes());
    bytes.extend_from_slice(message.as_bytes());
    hex::encode(bytes)
}

pub(crate) fn blake<const N: usize>(bytes: &[u8]) -> [u8; N] {
    let mut hash = Blake2bVar::new(N).expect("fixed supported BLAKE2b size");
    hash.update(bytes);
    let mut out = [0; N];
    hash.finalize_variable(&mut out)
        .expect("fixed BLAKE2b output");
    out
}

fn decode(text: &str) -> Result<Vec<u8>, AuthError> {
    if text.is_empty() || text.len() > 128 {
        return Err(AuthError::InvalidSignature);
    }
    bs58::decode(text)
        .with_check(None)
        .into_vec()
        .map_err(|_| AuthError::InvalidSignature)
}

pub fn public_key_address(public_key: &str) -> Result<String, AuthError> {
    let key = decode(public_key)?;
    let (prefix, address_prefix, length) = KEY_PREFIXES
        .iter()
        .find(|(prefix, _, length)| key.starts_with(prefix) && key.len() == prefix.len() + length)
        .ok_or(AuthError::InvalidSignature)?;
    let raw = &key[prefix.len()..];
    if *length == 33 && !matches!(raw[0], 2 | 3) {
        return Err(AuthError::InvalidSignature);
    }
    let mut bytes = address_prefix.to_vec();
    bytes.extend_from_slice(&blake::<20>(raw));
    Ok(bs58::encode(bytes).with_check().into_string())
}

pub(crate) fn validate_signature_encoding(signature: &str) -> Result<(), AuthError> {
    let bytes = decode(signature)?;
    if SIGNATURE_PREFIXES
        .iter()
        .copied()
        .chain(std::iter::once(&[4, 130, 43][..]))
        .any(|prefix| {
            bytes
                .strip_prefix(prefix)
                .is_some_and(|raw| raw.len() == 64)
        })
    {
        Ok(())
    } else {
        Err(AuthError::InvalidSignature)
    }
}

pub fn verify_signature(
    address: &str,
    public_key: &str,
    payload: &str,
    signature: &str,
) -> Result<(), AuthError> {
    if payload.len() > 8192 || public_key_address(public_key)? != address {
        return Err(AuthError::InvalidSignature);
    }
    let key = decode(public_key)?;
    let kind = KEY_PREFIXES
        .iter()
        .position(|(prefix, _, _)| key.starts_with(prefix))
        .ok_or(AuthError::InvalidSignature)?;
    let raw_key = &key[KEY_PREFIXES[kind].0.len()..];
    let signature = decode(signature)?;
    let raw_signature = signature
        .strip_prefix(SIGNATURE_PREFIXES[kind])
        .or_else(|| signature.strip_prefix(&[4, 130, 43]))
        .filter(|signature| signature.len() == 64)
        .ok_or(AuthError::InvalidSignature)?;
    let bytes = hex::decode(payload).map_err(|_| AuthError::InvalidSignature)?;
    if bytes.len() < 6
        || bytes[..2] != [5, 1]
        || u32::from_be_bytes(
            bytes[2..6]
                .try_into()
                .map_err(|_| AuthError::InvalidSignature)?,
        ) as usize
            != bytes.len() - 6
    {
        return Err(AuthError::InvalidSignature);
    }
    let hash = blake::<32>(&bytes);
    let valid = match kind {
        0 => {
            let key: &[u8; 32] = raw_key
                .try_into()
                .map_err(|_| AuthError::InvalidSignature)?;
            ed25519_dalek::VerifyingKey::from_bytes(key)
                .and_then(|key| {
                    key.verify_strict(&hash, &ed25519_dalek::Signature::from_slice(raw_signature)?)
                })
                .is_ok()
        }
        1 => k256::ecdsa::VerifyingKey::from_sec1_bytes(raw_key)
            .and_then(|key| {
                key.verify_prehash(&hash, &k256::ecdsa::Signature::from_slice(raw_signature)?)
            })
            .is_ok(),
        2 => p256::ecdsa::VerifyingKey::from_sec1_bytes(raw_key)
            .and_then(|key| {
                key.verify_prehash(&hash, &p256::ecdsa::Signature::from_slice(raw_signature)?)
            })
            .is_ok(),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(AuthError::InvalidSignature)
    }
}
