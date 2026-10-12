//! Content-addressed fetch of one `ipfs://` object through an ordered gateway list.
//!
//! Each gateway is asked for a trustless CAR (`?format=car&dag-scope=entity`); every block is
//! checked against its CID and the UnixFS file assembled by the content layer's verifier
//! (`content::car`). A gateway that answers with plain bytes instead is checked when the CID
//! allows it (raw codec: sha256 of the bytes; identity: the bytes inside the CID); plain
//! dag-pb bytes cannot be checked and are kept only as an unverified fallback, after every
//! gateway was tried for a verified copy.
//!
//! Outcomes stay distinct: a verified copy, a permanent fact about the object (bad URI,
//! directory, over the size cap), "every gateway said not found", and transient trouble.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use cid::Cid;
use sha2::{Digest, Sha256};

use super::http::{Fetch, TransportError};
use crate::content::ErrorKind;
use crate::content::car::{self, Outcome as CarOutcome};
use crate::content::path::{self, Segment};

const CAR_TYPE: &str = "application/vnd.ipld.car";
const CODEC_RAW: u64 = 0x55;
const HASH_SHA256: u64 = 0x12;
const HASH_IDENTITY: u64 = 0x00;

/// How stored bytes were checked against their CID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verification {
    /// Every block of a trustless CAR matched its CID.
    Car,
    /// Raw-codec CID: sha256 of the plain bytes matched.
    Sha256,
    /// Identity CID: the bytes are inside the CID.
    Identity,
    /// Plain dag-pb gateway bytes; not checked.
    Unverified,
}

impl Verification {
    pub fn as_str(self) -> &'static str {
        match self {
            Verification::Car => "ipfs-car",
            Verification::Sha256 => "sha256",
            Verification::Identity => "identity",
            Verification::Unverified => "unverified",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "ipfs-car" => Verification::Car,
            "sha256" => Verification::Sha256,
            "identity" => Verification::Identity,
            "unverified" => Verification::Unverified,
            _ => return None,
        })
    }

    pub fn is_verified(self) -> bool {
        self != Verification::Unverified
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ok {
        data: Vec<u8>,
        verification: Verification,
        gateway: String,
    },
    /// Every gateway answered 404/410 this time. Not yet proof of absence.
    NotFound(String),
    /// A permanent fact: unusable URI, a directory, or larger than the cap.
    Absent(String),
    /// Retry later.
    Transient(String),
    /// Stopped before an answer; says nothing about the object.
    Interrupted,
}

/// A parsed `ipfs://<cid>[/path]` reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpfsRef {
    /// Normalized URI: the storage key for the object.
    pub key: String,
    pub cid: Cid,
    cid_text: String,
    segments: Vec<Segment>,
}

/// Parses `ipfs://<cid>[/path]`. Errors are permanent: the URI can never be fetched.
pub fn ipfs_ref(uri: &str) -> Result<IpfsRef, String> {
    let rest = uri
        .trim()
        .strip_prefix("ipfs://")
        .ok_or_else(|| "not an ipfs:// URI".to_owned())?;
    let rest = rest.strip_prefix("ipfs/").unwrap_or(rest);
    let (cid_text, path_text) = rest.split_once('/').unwrap_or((rest, ""));
    let cid = car::parse_cid(cid_text).map_err(|e| format!("invalid CID: {}", e.message))?;
    let segments = path::split(path_text).map_err(|e| format!("invalid path: {}", e.message))?;
    let mut key = format!("ipfs://{cid_text}");
    for s in &segments {
        key.push('/');
        key.push_str(&s.url_encoded());
    }
    Ok(IpfsRef {
        key,
        cid,
        cid_text: cid_text.to_owned(),
        segments,
    })
}

/// Validates a gateway prefix: http(s), a host, no credentials, query or fragment, ending in
/// `/ipfs/`.
pub fn gateway(text: &str) -> Result<String, String> {
    let url = reqwest::Url::parse(text.trim()).map_err(|_| format!("invalid gateway {text:?}"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.path().ends_with("/ipfs/")
    {
        return Err(format!(
            "gateway {text:?} must be http(s) without credentials, query or fragment, \
             ending in /ipfs/"
        ));
    }
    Ok(url.to_string())
}

enum Attempt {
    Verified(Vec<u8>, Verification),
    Plain(Vec<u8>),
    NotFound,
    TooLarge,
    /// A verified permanent fact about the object.
    Permanent(String),
    Failed(String),
}

pub struct Fetcher {
    fetch: Arc<dyn Fetch>,
    gateways: Vec<String>,
    /// Largest accepted object, in bytes.
    cap: u64,
    stop: Arc<AtomicBool>,
    pub requests: AtomicU64,
}

impl Fetcher {
    pub fn new(
        fetch: Arc<dyn Fetch>,
        gateways: Vec<String>,
        cap: u64,
        stop: Arc<AtomicBool>,
    ) -> Fetcher {
        Fetcher {
            fetch,
            gateways,
            cap,
            stop,
            requests: AtomicU64::new(0),
        }
    }

    /// CAR transport allowance: block framing and CIDs on top of the file bytes.
    fn car_cap(&self) -> u64 {
        self.cap
            .saturating_add(self.cap / 16)
            .saturating_add(64 << 10)
    }

    fn get(
        &self,
        url: &str,
        accept: &str,
        cap: u64,
    ) -> Result<super::http::HttpResponse, TransportError> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.fetch.get(url, accept, cap)
    }

    pub fn fetch(&self, uri: &str) -> Outcome {
        let r = match ipfs_ref(uri) {
            Ok(r) => r,
            Err(e) => return Outcome::Absent(e),
        };
        if r.cid.hash().code() == HASH_IDENTITY
            && r.cid.codec() == CODEC_RAW
            && r.segments.is_empty()
        {
            let data = r.cid.hash().digest().to_vec();
            if data.len() as u64 > self.cap {
                return Outcome::Absent(format!("larger than {} bytes", self.cap));
            }
            return Outcome::Ok {
                data,
                verification: Verification::Identity,
                gateway: String::new(),
            };
        }
        let mut plain: Option<(Vec<u8>, String)> = None;
        let mut errors: Vec<String> = Vec::new();
        let (mut not_found, mut too_large) = (0usize, 0usize);
        for gw in &self.gateways {
            if self.stop.load(Ordering::Relaxed) {
                return Outcome::Interrupted;
            }
            match self.attempt(gw, &r) {
                Attempt::Verified(data, verification) => {
                    return Outcome::Ok {
                        data,
                        verification,
                        gateway: gw.clone(),
                    };
                }
                Attempt::Permanent(why) => return Outcome::Absent(why),
                Attempt::Plain(data) => {
                    if plain.is_none() {
                        plain = Some((data, gw.clone()));
                    }
                }
                Attempt::NotFound => {
                    not_found += 1;
                    errors.push(format!("{gw}: not found"));
                }
                Attempt::TooLarge => {
                    too_large += 1;
                    errors.push(format!("{gw}: larger than {} bytes", self.cap));
                }
                Attempt::Failed(e) => errors.push(format!("{gw}: {e}")),
            }
        }
        if let Some((data, gateway)) = plain {
            return Outcome::Ok {
                data,
                verification: Verification::Unverified,
                gateway,
            };
        }
        let n = self.gateways.len();
        let summary = errors.join("; ");
        if n > 0 && not_found == n {
            Outcome::NotFound(summary)
        } else if n > 0 && too_large > 0 && not_found + too_large == n {
            Outcome::Absent(format!(
                "larger than {} bytes at every gateway that had it",
                self.cap
            ))
        } else if n == 0 {
            Outcome::Transient("no IPFS gateways configured".into())
        } else {
            Outcome::Transient(summary)
        }
    }

    fn attempt(&self, gw: &str, r: &IpfsRef) -> Attempt {
        let mut url = format!("{gw}{}", r.cid_text);
        for s in &r.segments {
            url.push('/');
            url.push_str(&s.url_encoded());
        }
        let car_cap = self.car_cap();
        let reply = match self.get(
            &format!("{url}?format=car&dag-scope=entity"),
            CAR_TYPE,
            car_cap,
        ) {
            Ok(reply) => reply,
            Err(TransportError::TooLarge(_)) => return Attempt::TooLarge,
            Err(e) => return Attempt::Failed(e.to_string()),
        };
        match reply.status {
            200 if reply.content_type.as_deref() == Some(CAR_TYPE) => {
                match car::resolve(&reply.body, r.cid, &r.segments, car_cap) {
                    Ok(CarOutcome::File(data)) if data.len() as u64 > self.cap => {
                        Attempt::Permanent(format!("larger than {} bytes", self.cap))
                    }
                    Ok(CarOutcome::File(data)) => Attempt::Verified(data, Verification::Car),
                    Ok(CarOutcome::Directory) => {
                        Attempt::Permanent("a directory, not a file".into())
                    }
                    Err(e) => match e.kind {
                        ErrorKind::TooLarge => {
                            Attempt::Permanent(format!("larger than {} bytes", self.cap))
                        }
                        ErrorKind::NotFound => Attempt::Permanent(e.message),
                        _ => Attempt::Failed(format!("CAR rejected: {}", e.message)),
                    },
                }
            }
            200 => self.plain(r, reply.body),
            406 | 415 | 501 => match self.get(&url, "*/*", self.cap) {
                Ok(p) if p.status == 200 => self.plain(r, p.body),
                Ok(p) if matches!(p.status, 404 | 410) => Attempt::NotFound,
                Ok(p) => Attempt::Failed(format!("HTTP {}", p.status)),
                Err(TransportError::TooLarge(_)) => Attempt::TooLarge,
                Err(e) => Attempt::Failed(e.to_string()),
            },
            404 | 410 => Attempt::NotFound,
            s => Attempt::Failed(format!("HTTP {s}")),
        }
    }

    /// Plain bytes: checked when the CID allows, else an unverified candidate.
    fn plain(&self, r: &IpfsRef, body: Vec<u8>) -> Attempt {
        if body.len() as u64 > self.cap {
            return Attempt::TooLarge;
        }
        if !r.segments.is_empty() || r.cid.codec() != CODEC_RAW {
            return Attempt::Plain(body);
        }
        let hash = r.cid.hash();
        let matches = match hash.code() {
            HASH_SHA256 if hash.size() == 32 => Sha256::digest(&body).as_slice() == hash.digest(),
            HASH_IDENTITY => body == hash.digest(),
            _ => return Attempt::Plain(body),
        };
        if matches {
            let v = if hash.code() == HASH_IDENTITY {
                Verification::Identity
            } else {
                Verification::Sha256
            };
            Attempt::Verified(body, v)
        } else {
            Attempt::Failed("plain bytes do not match the CID".into())
        }
    }
}
