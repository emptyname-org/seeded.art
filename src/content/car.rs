//! Bounded CARv1 verifier and UnixFS reader (raw, files and flat directories).
//!
//! See https://ipld.io/specs/transport/car/carv1/ and
//! https://specs.ipfs.tech/unixfs/. Unsupported layouts fail closed. [`CarStream`] verifies
//! a whole-DAG CAR block by block as it arrives; [`resolve_with`] reads verified blocks from
//! any store.

use std::collections::{HashMap, HashSet};
use std::io::Cursor;

use cid::Cid;
use sha2::{Digest, Sha256};

use super::{ContentError, path::Segment};

const MAX_BLOCKS: usize = 16_384;
const MAX_DEPTH: usize = 128;
const MAX_HEADER: usize = 4096;

pub(crate) enum Outcome {
    File(Vec<u8>),
    Directory,
}

/// Bounded CAR transfer allowance for block CIDs, section lengths, and DAG framing.
pub(crate) fn transport_cap(object_cap: u64) -> u64 {
    object_cap
        .saturating_add(object_cap / 64)
        .saturating_add(64 * 1024)
}

/// Validates the textual CID before it is included in any URL or command.
pub(crate) fn parse_cid(text: &str) -> Result<Cid, ContentError> {
    if text.len() > 256 || !text.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Err(ContentError::bad_request("invalid IPFS CID"));
    }
    let decoded = if text.starts_with("Qm") {
        cid::multibase::Base::Base58Btc.decode(text)
    } else {
        cid::multibase::decode(text).map(|(_, bytes)| bytes)
    }
    .map_err(|_| ContentError::bad_request("invalid IPFS CID"))?;
    let parsed = Cid::try_from(decoded.as_slice())
        .map_err(|_| ContentError::bad_request("invalid IPFS CID"))?;
    // cid::try_from reads one CID and permits trailing bytes. Incoming text
    // must describe exactly one canonical binary CID.
    if parsed.to_bytes() != decoded {
        return Err(ContentError::bad_request("noncanonical IPFS CID"));
    }
    Ok(parsed)
}

pub(crate) fn resolve(
    data: &[u8],
    root: Cid,
    path: &[Segment],
    cap: u64,
) -> Result<Outcome, ContentError> {
    if data.len() as u64 > transport_cap(cap) {
        return Err(ContentError::too_large(cap));
    }
    let mut input = data;
    let header = take_section(&mut input)?;
    if header.len() > MAX_HEADER {
        return Err(ContentError::invalid("CAR header too large"));
    }
    check_header(header)?;
    let mut blocks = HashMap::new();
    let mut count = 0;
    while !input.is_empty() {
        count += 1;
        if count > MAX_BLOCKS {
            return Err(ContentError::invalid("too many CAR blocks"));
        }
        let section = take_section(&mut input)?;
        let mut cursor = Cursor::new(section);
        let cid = Cid::read_bytes(&mut cursor)
            .map_err(|_| ContentError::invalid("invalid CAR block CID"))?;
        let bytes = &section[cursor.position() as usize..];
        verify_block(cid, bytes)?;
        // Identical repeated blocks are legal CAR transport redundancy. They do
        // not allocate another block; conflicting duplicates fail hash checking.
        blocks.insert(normalize(cid), bytes);
    }
    let root = normalize(root);
    if !blocks.contains_key(&root) && root.hash().code() != 0 {
        return Err(ContentError::invalid("CAR omits requested root proof"));
    }
    let dag = Dag { blocks };
    let mut current = root;
    for segment in path {
        let node = dag.node(&current)?;
        if node.kind != 1 {
            return Err(ContentError::not_found("path traverses a file"));
        }
        current = node
            .links
            .iter()
            .find(|link| link.name == segment.decoded)
            .ok_or_else(|| ContentError::not_found("path absent from verified directory"))?
            .cid;
    }
    let node = dag.node(&current)?;
    if node.kind == 1 {
        return Ok(Outcome::Directory);
    }
    let size = file_size(&node, cap)?;
    let mut bytes = Vec::with_capacity(size as usize);
    let mut work = Work {
        active: HashSet::new(),
        visits: 0,
        bytes: cap.saturating_mul(4).max(4096),
    };
    dag.append_file(current, &mut bytes, size, &mut work, 0)?;
    Ok(Outcome::File(bytes))
}

/// What a verified block says about the DAG, for walking a whole package.
pub(crate) struct NodeInfo {
    /// 0 raw or file leaf, 1 directory, 2 file.
    pub kind: u64,
    /// Declared file bytes: `filesize`, else data plus block sizes.
    pub file_size: u64,
    pub links: Vec<LinkInfo>,
}

pub(crate) struct LinkInfo {
    pub cid: Cid,
}

/// Decodes a block already checked against `cid` (raw or DAG-PB UnixFS).
pub(crate) fn inspect(cid: &Cid, block: &[u8]) -> Result<NodeInfo, ContentError> {
    let node = view(cid, block)?;
    let declared = node
        .blocksizes
        .iter()
        .fold(node.data.len() as u64, |sum, size| {
            sum.saturating_add(*size)
        });
    Ok(NodeInfo {
        kind: node.kind,
        file_size: node.size.unwrap_or(declared),
        links: node
            .links
            .into_iter()
            .map(|link| LinkInfo { cid: link.cid })
            .collect(),
    })
}

/// Loads a block by its normalized CID; `None` when the store lacks it.
pub(crate) type Loader<'a> = dyn FnMut(&Cid) -> Result<Option<Vec<u8>>, ContentError> + 'a;
type ProbeLoader<'a> = dyn FnMut(&Cid) -> Result<Option<(Vec<u8>, u64)>, ContentError> + 'a;

#[derive(Debug, Clone)]
pub(crate) struct LeafPlan {
    pub cid: Cid,
    pub block_bytes: u64,
    pub directory: bool,
}

/// Decode a probed leaf without allocating graph maps or link vectors.
pub(super) fn mirrored_leaf(
    plan: &LeafPlan,
    block: Vec<u8>,
    cap: u64,
) -> Result<Vec<u8>, ContentError> {
    if block.len() as u64 > plan.block_bytes {
        return Err(ContentError::too_large(plan.block_bytes));
    }
    if plan.cid.codec() == 0x55 {
        if block.len() as u64 > cap {
            return Err(ContentError::too_large(cap));
        }
        return Ok(block);
    }
    let mut input = block.as_slice();
    while !input.is_empty() {
        if varint(&mut input)? != 10 {
            return Err(ContentError::invalid("mirrored leaf changed to a graph"));
        }
        take_section(&mut input)?;
    }
    let node = view(&plan.cid, &block)?;
    if !matches!(node.kind, 0 | 2)
        || !node.links.is_empty()
        || !node.blocksizes.is_empty()
        || file_size(&node, cap)? != node.data.len() as u64
    {
        return Err(ContentError::invalid("invalid mirrored leaf"));
    }
    Ok(node.data.to_vec())
}

/// Size hints only: no hashing. Incomplete metadata uses the configured bound;
/// the subsequent reader enforces the hint even if the backing file changes.
pub(super) fn probe_mirrored(
    root: Cid,
    path: &[Segment],
    cap: u64,
    load: &mut ProbeLoader<'_>,
) -> Result<Option<u64>, ContentError> {
    Ok(probe_mirrored_plan(root, path, cap, load)?.map(|(size, _)| size))
}

pub(super) fn probe_mirrored_plan(
    root: Cid,
    path: &[Segment],
    cap: u64,
    load: &mut ProbeLoader<'_>,
) -> Result<Option<(u64, Option<LeafPlan>)>, ContentError> {
    let mut current = normalize(root);
    let mut remaining = path.iter();
    let mut index = false;
    for _ in 0..=MAX_DEPTH {
        let Some((prefix, length)) = load(&current)? else {
            return Ok(None);
        };
        if current.codec() == 0x55 {
            if remaining.next().is_some() {
                return Err(ContentError::not_found("path traverses a file"));
            }
            return if length <= cap {
                Ok(Some((
                    length,
                    Some(LeafPlan {
                        cid: current,
                        block_bytes: length,
                        directory: index,
                    }),
                )))
            } else {
                Err(ContentError::too_large(cap))
            };
        }
        if prefix.len() as u64 != length {
            return Ok(Some((cap, None)));
        }
        let node = view(&current, &prefix)?;
        let segment = remaining.next();
        if node.kind != 1 {
            if segment.is_some() {
                return Err(ContentError::not_found("path traverses a file"));
            }
            let size = file_size(&node, cap)?;
            let leaf = (node.links.is_empty() && length <= (1 << 20)).then_some(LeafPlan {
                cid: current,
                block_bytes: length,
                directory: index,
            });
            return Ok(Some((size, leaf)));
        }
        let name = if let Some(segment) = segment {
            segment.decoded.as_str()
        } else if !index {
            index = true;
            "index.html"
        } else {
            return Err(ContentError::invalid("directory index is not a file"));
        };
        current = node
            .links
            .iter()
            .find(|link| link.name == name)
            .ok_or_else(|| ContentError::not_found("path absent from mirrored directory"))?
            .cid;
    }
    Err(ContentError::invalid("UnixFS path too deep"))
}

/// Inspect complete CAR sections available in a bounded prefix. Never verifies bytes.
pub(super) fn probe_car_prefix(
    prefix: &[u8],
    root: Cid,
    path: &[Segment],
    cap: u64,
) -> Option<u64> {
    let mut input = prefix;
    let header = take_section(&mut input).ok()?;
    if header.len() > MAX_HEADER || check_header(header).is_err() {
        return None;
    }
    let mut blocks = HashMap::new();
    while !input.is_empty() && blocks.len() < MAX_BLOCKS {
        let Ok(section) = take_section(&mut input) else {
            break;
        };
        let mut cursor = Cursor::new(section);
        let cid = normalize(Cid::read_bytes(&mut cursor).ok()?);
        blocks.insert(cid, &section[cursor.position() as usize..]);
    }
    probe_mirrored(root, path, cap, &mut |cid| {
        Ok(blocks
            .get(cid)
            .map(|bytes| (bytes.to_vec(), bytes.len() as u64)))
    })
    .ok()
    .flatten()
}

fn file_size(node: &Node<'_>, cap: u64) -> Result<u64, ContentError> {
    let size = node.size.unwrap_or_else(|| {
        node.blocksizes
            .iter()
            .fold(node.data.len() as u64, |sum, size| {
                sum.saturating_add(*size)
            })
    });
    if size > cap {
        return Err(ContentError::too_large(cap));
    }
    Ok(size)
}

/// [`resolve`] over blocks from `load` instead of one CAR: same path rules, size checks and
/// verification of every block. `Ok(None)`: a block the answer needs is not in the store.
pub(crate) fn resolve_with(
    root: Cid,
    path: &[Segment],
    cap: u64,
    load: &mut Loader<'_>,
) -> Result<Option<Outcome>, ContentError> {
    Ok(resolve_blocks(root, path, cap, load, true)?.map(|(body, _)| body))
}

/// Read immutable blocks already verified by the mirror writer. Keeps traversal
/// and size bounds, but does not hash their bytes. Returns the resolved file CID
/// for an ETag that also requires no body hash.
pub(super) fn resolve_mirrored_with(
    root: Cid,
    path: &[Segment],
    cap: u64,
    load: &mut Loader<'_>,
) -> Result<Option<(Outcome, Cid)>, ContentError> {
    resolve_blocks(root, path, cap, load, false)
}

fn resolve_blocks(
    root: Cid,
    path: &[Segment],
    cap: u64,
    load: &mut Loader<'_>,
    verify: bool,
) -> Result<Option<(Outcome, Cid)>, ContentError> {
    let mut store = BlockSet {
        blocks: HashMap::new(),
        // A file's blocks hold its bytes plus node overhead; directories on the path are small.
        budget: cap.saturating_add(4 << 20),
        cap,
        verify,
    };
    let mut current = normalize(root);
    for segment in path {
        if !store.load(current, load)? {
            return Ok(None);
        }
        let node = view(&current, store.get(&current))?;
        if node.kind != 1 {
            return Err(ContentError::not_found("path traverses a file"));
        }
        current = node
            .links
            .iter()
            .find(|link| link.name == segment.decoded)
            .ok_or_else(|| ContentError::not_found("path absent from verified directory"))?
            .cid;
    }
    if !store.load(current, load)? {
        return Ok(None);
    }
    if view(&current, store.get(&current))?.kind == 1 {
        return Ok(Some((Outcome::Directory, current)));
    }
    // Load the file's whole graph once; each block is expanded the first time it appears.
    let mut pending = vec![current];
    let mut expanded = HashSet::new();
    while let Some(cid) = pending.pop() {
        if !store.load(cid, load)? {
            return Ok(None);
        }
        if cid.codec() == 0x70 && expanded.insert(cid) {
            let node = view(&cid, store.get(&cid))?;
            if pending.len().saturating_add(node.links.len()) > MAX_BLOCKS {
                return Err(ContentError::invalid(
                    "UnixFS pending graph exceeds work budget",
                ));
            }
            pending.extend(node.links.iter().map(|link| link.cid));
        }
    }
    let dag = Dag {
        blocks: store
            .blocks
            .iter()
            .map(|(cid, bytes)| (*cid, bytes.as_slice()))
            .collect(),
    };
    let size = file_size(&dag.node(&current)?, cap)?;
    let mut bytes = Vec::with_capacity(size as usize);
    let mut work = Work {
        active: HashSet::new(),
        visits: 0,
        bytes: cap.saturating_mul(4).max(4096),
    };
    dag.append_file(current, &mut bytes, size, &mut work, 0)?;
    Ok(Some((Outcome::File(bytes), current)))
}

struct BlockSet {
    blocks: HashMap<Cid, Vec<u8>>,
    budget: u64,
    cap: u64,
    verify: bool,
}

impl BlockSet {
    fn load(&mut self, cid: Cid, load: &mut Loader<'_>) -> Result<bool, ContentError> {
        if cid.hash().code() == 0x00 || self.blocks.contains_key(&cid) {
            return Ok(true);
        }
        if self.blocks.len() >= MAX_BLOCKS {
            return Err(ContentError::invalid("too many blocks"));
        }
        let Some(bytes) = load(&cid)? else {
            return Ok(false);
        };
        if self.verify {
            verify_block(cid, &bytes)?;
        }
        self.budget = self
            .budget
            .checked_sub(bytes.len() as u64)
            .ok_or_else(|| ContentError::too_large(self.cap))?;
        self.blocks.insert(cid, bytes);
        Ok(true)
    }

    fn get<'a>(&'a self, cid: &'a Cid) -> &'a [u8] {
        self.blocks
            .get(cid)
            .map_or_else(|| cid.hash().digest(), Vec::as_slice)
    }
}

/// Incremental CARv1 reader: feed body chunks with [`CarStream::push`], take verified blocks
/// with [`CarStream::next`]. Holds at most one section plus the last chunk.
pub(crate) struct CarStream {
    buffer: Vec<u8>,
    start: usize,
    header: bool,
    max_block: usize,
    consumed: u64,
}

impl CarStream {
    pub(crate) fn new(max_block: usize) -> Self {
        Self {
            buffer: Vec::new(),
            start: 0,
            header: false,
            max_block,
            consumed: 0,
        }
    }

    /// Bytes of the whole sections read so far, header included.
    pub(crate) fn consumed(&self) -> u64 {
        self.consumed
    }

    pub(crate) fn push(&mut self, chunk: &[u8]) {
        if self.start > 0 {
            self.buffer.drain(..self.start);
            self.start = 0;
        }
        self.buffer.extend_from_slice(chunk);
    }

    /// The next block, checked against its CID (normalized to v1); `None` until a whole
    /// section has arrived.
    pub(crate) fn next(&mut self) -> Result<Option<(Cid, Vec<u8>)>, ContentError> {
        loop {
            let data = &self.buffer[self.start..];
            let Some((length, used)) = peek_varint(data)? else {
                return Ok(None);
            };
            let limit = if self.header {
                self.max_block.saturating_add(128)
            } else {
                MAX_HEADER
            };
            let length = usize::try_from(length)
                .ok()
                .filter(|length| *length <= limit)
                .ok_or_else(|| {
                    if self.header {
                        ContentError::too_large(self.max_block as u64)
                    } else {
                        ContentError::invalid("CAR header too large")
                    }
                })?;
            if data.len() - used < length {
                return Ok(None);
            }
            let section = &data[used..used + length];
            self.start += used + length;
            self.consumed += (used + length) as u64;
            if !self.header {
                check_header(section)?;
                self.header = true;
                continue;
            }
            let mut cursor = Cursor::new(section);
            let cid = Cid::read_bytes(&mut cursor)
                .map_err(|_| ContentError::invalid("invalid CAR block CID"))?;
            let bytes = &section[cursor.position() as usize..];
            verify_block(cid, bytes)?;
            return Ok(Some((normalize(cid), bytes.to_vec())));
        }
    }

    /// After the body ended: anything left over is a truncated transfer.
    pub(crate) fn finish(&self) -> Result<(), ContentError> {
        if !self.header || self.start < self.buffer.len() {
            return Err(ContentError::unavailable("truncated CAR stream"));
        }
        Ok(())
    }
}

/// A varint at the start of `bytes`, with its length; `None` if more bytes are needed.
fn peek_varint(bytes: &[u8]) -> Result<Option<(u64, usize)>, ContentError> {
    match bytes.iter().take(10).position(|b| b & 128 == 0) {
        Some(end) => {
            let mut slice = &bytes[..=end];
            Ok(Some((varint(&mut slice)?, end + 1)))
        }
        None if bytes.len() >= 10 => Err(ContentError::invalid("varint overflow")),
        None => Ok(None),
    }
}

pub(crate) fn verify_block(cid: Cid, bytes: &[u8]) -> Result<(), ContentError> {
    let matches = match cid.hash().code() {
        0x12 if cid.hash().size() == 32 => Sha256::digest(bytes).as_slice() == cid.hash().digest(),
        0x00 => bytes == cid.hash().digest(),
        _ => return Err(ContentError::invalid("unsupported IPFS hash")),
    };
    if !matches {
        return Err(ContentError::invalid("CAR block hash mismatch"));
    }
    Ok(())
}

/// CIDv1 with the same codec and multihash; CIDv0 and v1 name the same block.
pub(crate) fn normalize(cid: Cid) -> Cid {
    Cid::new_v1(cid.codec(), *cid.hash())
}

struct Dag<'a> {
    blocks: HashMap<Cid, &'a [u8]>,
}

struct Work {
    active: HashSet<Cid>,
    visits: usize,
    bytes: u64,
}

struct Link {
    cid: Cid,
    name: String,
}

struct Node<'a> {
    kind: u64,
    data: &'a [u8],
    size: Option<u64>,
    blocksizes: Vec<u64>,
    links: Vec<Link>,
}

impl Dag<'_> {
    fn node<'a>(&'a self, cid: &'a Cid) -> Result<Node<'a>, ContentError> {
        let data = match self.blocks.get(cid) {
            Some(bytes) => *bytes,
            // Trustless gateways omit identity blocks; the CID contains bytes.
            None if cid.hash().code() == 0x00 => cid.hash().digest(),
            None => return Err(ContentError::unavailable("CAR is missing a linked block")),
        };
        view(cid, data)
    }

    fn append_file(
        &self,
        cid: Cid,
        out: &mut Vec<u8>,
        cap: u64,
        work: &mut Work,
        depth: usize,
    ) -> Result<(), ContentError> {
        work.visits += 1;
        if work.visits > MAX_BLOCKS || depth > MAX_DEPTH || !work.active.insert(cid) {
            return Err(ContentError::invalid("UnixFS graph is cyclic or too deep"));
        }
        let encoded_size = self
            .blocks
            .get(&cid)
            .map_or(cid.hash().digest().len(), |b| b.len()) as u64;
        work.bytes = work
            .bytes
            .checked_sub(encoded_size)
            .ok_or_else(|| ContentError::invalid("UnixFS traversal exceeds work budget"))?;
        let node = self.node(&cid)?;
        if !matches!(node.kind, 0 | 2) {
            return Err(ContentError::invalid("directory inside UnixFS file"));
        }
        if node.blocksizes.len() != node.links.len() {
            return Err(ContentError::invalid(
                "UnixFS block sizes do not match links",
            ));
        }
        let start = out.len();
        if out.len() as u64 + node.data.len() as u64 > cap {
            return Err(ContentError::too_large(cap));
        }
        out.extend_from_slice(node.data);
        for (link, expected) in node.links.iter().zip(&node.blocksizes) {
            let before = out.len();
            self.append_file(link.cid, out, cap, work, depth + 1)?;
            if (out.len() - before) as u64 != *expected {
                return Err(ContentError::invalid("UnixFS child size mismatch"));
            }
        }
        if node
            .size
            .is_some_and(|size| size != (out.len() - start) as u64)
        {
            return Err(ContentError::invalid("UnixFS file size mismatch"));
        }
        work.active.remove(&cid);
        Ok(())
    }
}

fn view<'a>(cid: &Cid, data: &'a [u8]) -> Result<Node<'a>, ContentError> {
    match cid.codec() {
        0x55 => Ok(Node {
            kind: 0,
            data,
            size: Some(data.len() as u64),
            blocksizes: vec![],
            links: vec![],
        }),
        0x70 => decode_node(data),
        _ => Err(ContentError::invalid("unsupported IPFS codec")),
    }
}

fn decode_node(mut data: &[u8]) -> Result<Node<'_>, ContentError> {
    let mut unix = None;
    let mut links = Vec::new();
    while !data.is_empty() {
        match varint(&mut data)? {
            10 if unix.is_none() => unix = Some(take_section(&mut data)?),
            18 if links.len() < MAX_BLOCKS => links.push(decode_link(take_section(&mut data)?)?),
            _ => return Err(ContentError::invalid("invalid DAG-PB node field")),
        }
    }
    let mut input = unix.ok_or_else(|| ContentError::invalid("missing UnixFS data"))?;
    let mut kind = None;
    let mut body = None;
    let mut size = None;
    let mut blocksizes = Vec::new();
    while !input.is_empty() {
        let tag = varint(&mut input)?;
        match tag {
            8 if kind.is_none() => kind = Some(varint(&mut input)?),
            18 if body.is_none() => body = Some(take_section(&mut input)?),
            24 if size.is_none() => size = Some(varint(&mut input)?),
            32 if blocksizes.len() < MAX_BLOCKS => blocksizes.push(varint(&mut input)?),
            34 => {
                let mut packed = take_section(&mut input)?;
                while !packed.is_empty() && blocksizes.len() < MAX_BLOCKS {
                    blocksizes.push(varint(&mut packed)?);
                }
                if !packed.is_empty() {
                    return Err(ContentError::invalid("too many UnixFS blocks"));
                }
            }
            // Optional metadata cannot affect file contents or paths.
            40 | 48 | 56 => {
                varint(&mut input)?;
            }
            66 => {
                take_section(&mut input)?;
            }
            _ => return Err(ContentError::invalid("invalid UnixFS field")),
        }
    }
    let kind = kind.ok_or_else(|| ContentError::invalid("missing UnixFS type"))?;
    if !matches!(kind, 0..=2) {
        return Err(ContentError::invalid(
            "unsupported UnixFS type (HAMT, symlink or metadata)",
        ));
    }
    if kind == 1 {
        let mut names = HashSet::new();
        for link in &links {
            if link.name.is_empty() || !names.insert(&link.name) {
                return Err(ContentError::invalid("ambiguous UnixFS directory names"));
            }
        }
    }
    Ok(Node {
        kind,
        data: body.unwrap_or_default(),
        size,
        blocksizes,
        links,
    })
}

fn decode_link(mut bytes: &[u8]) -> Result<Link, ContentError> {
    let mut cid = None;
    let mut name = None;
    let mut size = false;
    while !bytes.is_empty() {
        match varint(&mut bytes)? {
            10 if cid.is_none() => {
                let raw = take_section(&mut bytes)?;
                let mut cursor = Cursor::new(raw);
                let parsed = Cid::read_bytes(&mut cursor)
                    .map_err(|_| ContentError::invalid("invalid DAG-PB link CID"))?;
                if cursor.position() != raw.len() as u64 {
                    return Err(ContentError::invalid("trailing link CID data"));
                }
                cid = Some(normalize(parsed));
            }
            18 if name.is_none() => {
                name = Some(
                    std::str::from_utf8(take_section(&mut bytes)?)
                        .map_err(|_| ContentError::invalid("non-UTF8 directory name"))?
                        .to_owned(),
                )
            }
            24 if !size => {
                varint(&mut bytes)?;
                size = true;
            }
            _ => return Err(ContentError::invalid("invalid DAG-PB link")),
        }
    }
    Ok(Link {
        cid: cid.ok_or_else(|| ContentError::invalid("missing link CID"))?,
        name: name.unwrap_or_default(),
    })
}

fn take_section<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8], ContentError> {
    let n =
        usize::try_from(varint(bytes)?).map_err(|_| ContentError::invalid("length overflow"))?;
    if n > bytes.len() {
        return Err(ContentError::invalid("truncated block"));
    }
    let (section, rest) = bytes.split_at(n);
    *bytes = rest;
    Ok(section)
}

fn varint(bytes: &mut &[u8]) -> Result<u64, ContentError> {
    let mut out = 0_u64;
    for shift in (0..70).step_by(7) {
        let b = *bytes
            .first()
            .ok_or_else(|| ContentError::invalid("truncated varint"))?;
        *bytes = &bytes[1..];
        if shift == 63 && b > 1 {
            return Err(ContentError::invalid("varint overflow"));
        }
        out |= u64::from(b & 127) << shift;
        if b & 128 == 0 {
            if shift > 0 && b == 0 {
                return Err(ContentError::invalid("noncanonical varint"));
            }
            return Ok(out);
        }
    }
    Err(ContentError::invalid("varint overflow"))
}

// A deliberately narrow CAR header decoder: definite map, roots and version.
// No recursive CBOR allocation or unbounded extension fields.
fn check_header(mut bytes: &[u8]) -> Result<(), ContentError> {
    if cbor_head(&mut bytes, 5)? != 2 {
        return Err(ContentError::invalid("invalid CAR header map"));
    }
    let mut version = false;
    let mut roots = false;
    for _ in 0..2 {
        let key = cbor_bytes(&mut bytes, 3)?;
        match key {
            b"version" if !version => {
                if cbor_head(&mut bytes, 0)? != 1 {
                    return Err(ContentError::invalid("unsupported CAR version"));
                }
                version = true;
            }
            b"roots" if !roots => {
                let count = cbor_head(&mut bytes, 4)?;
                if count > 16 {
                    return Err(ContentError::invalid("invalid CAR roots count"));
                }
                for _ in 0..count {
                    if cbor_head(&mut bytes, 6)? != 42 {
                        return Err(ContentError::invalid("invalid CAR CID tag"));
                    }
                    let raw = cbor_bytes(&mut bytes, 2)?;
                    if raw.first() != Some(&0) {
                        return Err(ContentError::invalid("invalid CAR root CID"));
                    }
                    let mut cursor = Cursor::new(&raw[1..]);
                    Cid::read_bytes(&mut cursor)
                        .map_err(|_| ContentError::invalid("invalid CAR root CID"))?;
                    if cursor.position() as usize != raw.len() - 1 {
                        return Err(ContentError::invalid("trailing root CID data"));
                    }
                }
                roots = true;
            }
            _ => return Err(ContentError::invalid("unexpected CAR header field")),
        }
    }
    if !version || !roots || !bytes.is_empty() {
        return Err(ContentError::invalid("invalid CAR header"));
    }
    // Trustless Gateway §5.2 leaves header roots unspecified. They may name
    // the resolved file, not the requested root. The requested CID's verified
    // block and its actual links anchor path traversal, never this header.
    Ok(())
}

fn cbor_bytes<'a>(bytes: &mut &'a [u8], major: u8) -> Result<&'a [u8], ContentError> {
    let n = usize::try_from(cbor_head(bytes, major)?)
        .map_err(|_| ContentError::invalid("CBOR length overflow"))?;
    if n > bytes.len() {
        return Err(ContentError::invalid("truncated CAR header"));
    }
    let (data, rest) = bytes.split_at(n);
    *bytes = rest;
    Ok(data)
}

fn cbor_head(bytes: &mut &[u8], major: u8) -> Result<u64, ContentError> {
    let b = *bytes
        .first()
        .ok_or_else(|| ContentError::invalid("truncated CAR header"))?;
    *bytes = &bytes[1..];
    if b >> 5 != major {
        return Err(ContentError::invalid("unexpected CAR header type"));
    }
    let low = b & 31;
    if low < 24 {
        return Ok(u64::from(low));
    }
    let n = match low {
        24 => 1,
        25 => 2,
        26 => 4,
        27 => 8,
        _ => return Err(ContentError::invalid("indefinite CAR header")),
    };
    if bytes.len() < n {
        return Err(ContentError::invalid("truncated CAR header"));
    }
    let mut value = 0_u64;
    for &b in &bytes[..n] {
        value = (value << 8) | u64::from(b);
    }
    *bytes = &bytes[n..];
    if (n == 1 && value < 24) || (n > 1 && value < (1_u64 << ((n / 2) * 8))) {
        return Err(ContentError::invalid("noncanonical CAR header integer"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_varint(mut n: u64) -> Vec<u8> {
        let mut out = Vec::new();
        while n >= 128 {
            out.push(n as u8 | 128);
            n >>= 7;
        }
        out.push(n as u8);
        out
    }

    fn raw_block(data: &[u8]) -> (Cid, Vec<u8>) {
        let digest = Sha256::digest(data);
        (
            Cid::new_v1(
                0x55,
                cid::multihash::Multihash::wrap(0x12, &digest).unwrap(),
            ),
            data.to_vec(),
        )
    }

    fn car(root: Cid, blocks: &[(Cid, Vec<u8>)]) -> Vec<u8> {
        let mut header = vec![0xa2, 0x65];
        header.extend(b"roots");
        header.extend([0x81, 0xd8, 0x2a, 0x58, root.to_bytes().len() as u8 + 1, 0]);
        header.extend(root.to_bytes());
        header.push(0x67);
        header.extend(b"version");
        header.push(1);
        let mut out = encode_varint(header.len() as u64);
        out.extend(header);
        for (cid, data) in blocks {
            out.extend(encode_varint((cid.to_bytes().len() + data.len()) as u64));
            out.extend(cid.to_bytes());
            out.extend(data);
        }
        out
    }

    #[test]
    fn mirrored_repeated_links_cannot_grow_the_pending_graph_without_bound() {
        fn field(tag: u8, bytes: &[u8]) -> Vec<u8> {
            let mut out = vec![tag];
            out.extend(encode_varint(bytes.len() as u64));
            out.extend(bytes);
            out
        }
        fn branch(child: Cid) -> (Cid, Vec<u8>) {
            let mut unix = vec![8, 2, 24, 0];
            for _ in 0..MAX_BLOCKS {
                unix.extend([32, 0]);
            }
            let mut data = field(10, &unix);
            let link = field(18, &field(10, &child.to_bytes()));
            for _ in 0..MAX_BLOCKS {
                data.extend(&link);
            }
            let cid = Cid::new_v1(
                0x70,
                cid::multihash::Multihash::wrap(0x12, &Sha256::digest(&data)).unwrap(),
            );
            (cid, data)
        }
        let leaf = raw_block(b"");
        let child = branch(leaf.0);
        let root = branch(child.0);
        let blocks = HashMap::from([leaf, child, root.clone()]);
        let error = resolve_mirrored_with(root.0, &[], 4 << 20, &mut |cid| {
            Ok(blocks.get(cid).cloned())
        })
        .err()
        .unwrap();
        assert!(error.message.contains("pending graph exceeds work budget"));
    }

    #[test]
    fn decoded_file_capacity_matches_size_on_both_read_paths() {
        let block = raw_block(&vec![7; 65537]);
        let proof = car(block.0, std::slice::from_ref(&block));
        let Outcome::File(bytes) = resolve(&proof, block.0, &[], 1 << 20).unwrap() else {
            panic!("file");
        };
        assert_eq!((bytes.len(), bytes.capacity()), (65537, 65537));
        let Some((Outcome::File(bytes), _)) =
            resolve_mirrored_with(block.0, &[], 1 << 20, &mut |_| Ok(Some(block.1.clone())))
                .unwrap()
        else {
            panic!("file");
        };
        assert_eq!((bytes.len(), bytes.capacity()), (65537, 65537));
    }

    #[test]
    fn mirrored_size_probe_reads_no_raw_payload_and_never_hashes_it() {
        let block = raw_block(b"written");
        let size = probe_mirrored(block.0, &[], 100, &mut |_| Ok(Some((vec![], 7)))).unwrap();
        assert_eq!(size, Some(7));
        assert!(
            probe_mirrored(block.0, &[], 100, &mut |_| Ok(None))
                .unwrap()
                .is_none()
        );
        assert!(probe_mirrored(block.0, &[], 6, &mut |_| Ok(Some((vec![], 7)))).is_err());
    }

    #[test]
    fn object_cap_limits_reconstructed_bytes_not_car_framing() {
        let bytes = vec![7; 4096];
        let block = raw_block(&bytes);
        let proof = car(block.0, std::slice::from_ref(&block));
        assert!(proof.len() as u64 > bytes.len() as u64);
        assert!(matches!(
            resolve(&proof, block.0, &[], bytes.len() as u64),
            Ok(Outcome::File(file)) if file == bytes
        ));
        assert_eq!(
            resolve(&proof, block.0, &[], bytes.len() as u64 - 1)
                .err()
                .expect("oversized reconstructed file fails")
                .kind,
            super::super::ErrorKind::TooLarge
        );
    }

    #[test]
    fn car_transport_cap_adds_bounded_framing_headroom() {
        assert_eq!(transport_cap(0), 64 * 1024);
        assert_eq!(transport_cap(64), 64 * 1024 + 65);
        assert_eq!(transport_cap(u64::MAX), u64::MAX);
    }

    #[test]
    fn stream_yields_the_same_verified_blocks_for_every_chunking() {
        let blocks = [raw_block(b"first"), raw_block(&[7; 300]), raw_block(b"")];
        let data = car(blocks[0].0, &blocks);
        for size in [1, 2, 3, 7, 64, data.len()] {
            let mut stream = CarStream::new(1024);
            let mut seen = Vec::new();
            for chunk in data.chunks(size) {
                stream.push(chunk);
                while let Some(block) = stream.next().unwrap() {
                    seen.push(block);
                }
            }
            stream.finish().unwrap();
            assert_eq!(seen, blocks.to_vec(), "chunk size {size}");
            assert_eq!(stream.consumed(), data.len() as u64);
        }
        let mut cut = CarStream::new(1024);
        cut.push(&data[..data.len() - 1]);
        while cut.next().unwrap().is_some() {}
        assert_eq!(
            cut.finish().unwrap_err().kind,
            super::super::ErrorKind::Unavailable
        );
    }

    #[test]
    fn stream_rejects_tampered_and_oversized_blocks() {
        let block = raw_block(b"artwork");
        let mut data = car(block.0, std::slice::from_ref(&block));
        *data.last_mut().unwrap() ^= 1;
        let mut stream = CarStream::new(1024);
        stream.push(&data);
        assert_eq!(
            stream.next().unwrap_err().kind,
            super::super::ErrorKind::Invalid
        );
        let big = raw_block(&[1; 600]);
        let mut stream = CarStream::new(256);
        stream.push(&car(big.0, std::slice::from_ref(&big)));
        assert_eq!(
            stream.next().unwrap_err().kind,
            super::super::ErrorKind::TooLarge
        );
    }
    #[test]
    fn mirrored_leaf_plan_refuses_source_growth_and_graph_replacement() {
        let (cid, bytes) = raw_block(b"abcd");
        let plan = LeafPlan {
            cid,
            block_bytes: 4,
            directory: false,
        };
        assert_eq!(mirrored_leaf(&plan, bytes, 4).unwrap(), b"abcd");
        assert!(mirrored_leaf(&plan, vec![0; 5], 4).is_err());
        let cid = Cid::new_v1(0x70, *plan.cid.hash());
        let plan = LeafPlan {
            cid,
            block_bytes: 16,
            directory: false,
        };
        assert!(mirrored_leaf(&plan, vec![18, 0], 16).is_err());
    }
}
