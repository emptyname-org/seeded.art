//! Minimal HPACK (RFC 7541) decoder for ONCHFS file metadata.
//!
//! ONCHFS stores a file's HTTP headers (content-type, content-encoding) as an
//! HPACK block with a 256-byte dynamic table. Tables below are generated from
//! the RFC 7541 text (Appendix A and B).

use std::collections::HashMap;
use std::sync::OnceLock;

/// Dynamic table size used by the ONCHFS encoder (`onchfs-js`).
const DYNAMIC_TABLE_MAX: usize = 256;
const MAX_HEADERS: usize = 32;
const MAX_STRING: usize = 4096;

/// A decoding failure; the block is not valid HPACK.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("hpack: {0}")]
pub struct HpackError(pub &'static str);

/// RFC 7541 Appendix A, indices 1..=61.
const STATIC_TABLE: [(&str, &str); 61] = [
    (":authority", ""),
    (":method", "GET"),
    (":method", "POST"),
    (":path", "/"),
    (":path", "/index.html"),
    (":scheme", "http"),
    (":scheme", "https"),
    (":status", "200"),
    (":status", "204"),
    (":status", "206"),
    (":status", "304"),
    (":status", "400"),
    (":status", "404"),
    (":status", "500"),
    ("accept-charset", ""),
    ("accept-encoding", "gzip, deflate"),
    ("accept-language", ""),
    ("accept-ranges", ""),
    ("accept", ""),
    ("access-control-allow-origin", ""),
    ("age", ""),
    ("allow", ""),
    ("authorization", ""),
    ("cache-control", ""),
    ("content-disposition", ""),
    ("content-encoding", ""),
    ("content-language", ""),
    ("content-length", ""),
    ("content-location", ""),
    ("content-range", ""),
    ("content-type", ""),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("expect", ""),
    ("expires", ""),
    ("from", ""),
    ("host", ""),
    ("if-match", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("if-range", ""),
    ("if-unmodified-since", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("max-forwards", ""),
    ("proxy-authenticate", ""),
    ("proxy-authorization", ""),
    ("range", ""),
    ("referer", ""),
    ("refresh", ""),
    ("retry-after", ""),
    ("server", ""),
    ("set-cookie", ""),
    ("strict-transport-security", ""),
    ("transfer-encoding", ""),
    ("user-agent", ""),
    ("vary", ""),
    ("via", ""),
    ("www-authenticate", ""),
];

/// RFC 7541 Appendix B: (code aligned to LSB, bit length) for symbols 0..=256.
const HUFFMAN: [(u32, u8); 257] = [
    (0x1ff8, 13),
    (0x7fffd8, 23),
    (0xfffffe2, 28),
    (0xfffffe3, 28),
    (0xfffffe4, 28),
    (0xfffffe5, 28),
    (0xfffffe6, 28),
    (0xfffffe7, 28),
    (0xfffffe8, 28),
    (0xffffea, 24),
    (0x3ffffffc, 30),
    (0xfffffe9, 28),
    (0xfffffea, 28),
    (0x3ffffffd, 30),
    (0xfffffeb, 28),
    (0xfffffec, 28),
    (0xfffffed, 28),
    (0xfffffee, 28),
    (0xfffffef, 28),
    (0xffffff0, 28),
    (0xffffff1, 28),
    (0xffffff2, 28),
    (0x3ffffffe, 30),
    (0xffffff3, 28),
    (0xffffff4, 28),
    (0xffffff5, 28),
    (0xffffff6, 28),
    (0xffffff7, 28),
    (0xffffff8, 28),
    (0xffffff9, 28),
    (0xffffffa, 28),
    (0xffffffb, 28),
    (0x14, 6),
    (0x3f8, 10),
    (0x3f9, 10),
    (0xffa, 12),
    (0x1ff9, 13),
    (0x15, 6),
    (0xf8, 8),
    (0x7fa, 11),
    (0x3fa, 10),
    (0x3fb, 10),
    (0xf9, 8),
    (0x7fb, 11),
    (0xfa, 8),
    (0x16, 6),
    (0x17, 6),
    (0x18, 6),
    (0x0, 5),
    (0x1, 5),
    (0x2, 5),
    (0x19, 6),
    (0x1a, 6),
    (0x1b, 6),
    (0x1c, 6),
    (0x1d, 6),
    (0x1e, 6),
    (0x1f, 6),
    (0x5c, 7),
    (0xfb, 8),
    (0x7ffc, 15),
    (0x20, 6),
    (0xffb, 12),
    (0x3fc, 10),
    (0x1ffa, 13),
    (0x21, 6),
    (0x5d, 7),
    (0x5e, 7),
    (0x5f, 7),
    (0x60, 7),
    (0x61, 7),
    (0x62, 7),
    (0x63, 7),
    (0x64, 7),
    (0x65, 7),
    (0x66, 7),
    (0x67, 7),
    (0x68, 7),
    (0x69, 7),
    (0x6a, 7),
    (0x6b, 7),
    (0x6c, 7),
    (0x6d, 7),
    (0x6e, 7),
    (0x6f, 7),
    (0x70, 7),
    (0x71, 7),
    (0x72, 7),
    (0xfc, 8),
    (0x73, 7),
    (0xfd, 8),
    (0x1ffb, 13),
    (0x7fff0, 19),
    (0x1ffc, 13),
    (0x3ffc, 14),
    (0x22, 6),
    (0x7ffd, 15),
    (0x3, 5),
    (0x23, 6),
    (0x4, 5),
    (0x24, 6),
    (0x5, 5),
    (0x25, 6),
    (0x26, 6),
    (0x27, 6),
    (0x6, 5),
    (0x74, 7),
    (0x75, 7),
    (0x28, 6),
    (0x29, 6),
    (0x2a, 6),
    (0x7, 5),
    (0x2b, 6),
    (0x76, 7),
    (0x2c, 6),
    (0x8, 5),
    (0x9, 5),
    (0x2d, 6),
    (0x77, 7),
    (0x78, 7),
    (0x79, 7),
    (0x7a, 7),
    (0x7b, 7),
    (0x7ffe, 15),
    (0x7fc, 11),
    (0x3ffd, 14),
    (0x1ffd, 13),
    (0xffffffc, 28),
    (0xfffe6, 20),
    (0x3fffd2, 22),
    (0xfffe7, 20),
    (0xfffe8, 20),
    (0x3fffd3, 22),
    (0x3fffd4, 22),
    (0x3fffd5, 22),
    (0x7fffd9, 23),
    (0x3fffd6, 22),
    (0x7fffda, 23),
    (0x7fffdb, 23),
    (0x7fffdc, 23),
    (0x7fffdd, 23),
    (0x7fffde, 23),
    (0xffffeb, 24),
    (0x7fffdf, 23),
    (0xffffec, 24),
    (0xffffed, 24),
    (0x3fffd7, 22),
    (0x7fffe0, 23),
    (0xffffee, 24),
    (0x7fffe1, 23),
    (0x7fffe2, 23),
    (0x7fffe3, 23),
    (0x7fffe4, 23),
    (0x1fffdc, 21),
    (0x3fffd8, 22),
    (0x7fffe5, 23),
    (0x3fffd9, 22),
    (0x7fffe6, 23),
    (0x7fffe7, 23),
    (0xffffef, 24),
    (0x3fffda, 22),
    (0x1fffdd, 21),
    (0xfffe9, 20),
    (0x3fffdb, 22),
    (0x3fffdc, 22),
    (0x7fffe8, 23),
    (0x7fffe9, 23),
    (0x1fffde, 21),
    (0x7fffea, 23),
    (0x3fffdd, 22),
    (0x3fffde, 22),
    (0xfffff0, 24),
    (0x1fffdf, 21),
    (0x3fffdf, 22),
    (0x7fffeb, 23),
    (0x7fffec, 23),
    (0x1fffe0, 21),
    (0x1fffe1, 21),
    (0x3fffe0, 22),
    (0x1fffe2, 21),
    (0x7fffed, 23),
    (0x3fffe1, 22),
    (0x7fffee, 23),
    (0x7fffef, 23),
    (0xfffea, 20),
    (0x3fffe2, 22),
    (0x3fffe3, 22),
    (0x3fffe4, 22),
    (0x7ffff0, 23),
    (0x3fffe5, 22),
    (0x3fffe6, 22),
    (0x7ffff1, 23),
    (0x3ffffe0, 26),
    (0x3ffffe1, 26),
    (0xfffeb, 20),
    (0x7fff1, 19),
    (0x3fffe7, 22),
    (0x7ffff2, 23),
    (0x3fffe8, 22),
    (0x1ffffec, 25),
    (0x3ffffe2, 26),
    (0x3ffffe3, 26),
    (0x3ffffe4, 26),
    (0x7ffffde, 27),
    (0x7ffffdf, 27),
    (0x3ffffe5, 26),
    (0xfffff1, 24),
    (0x1ffffed, 25),
    (0x7fff2, 19),
    (0x1fffe3, 21),
    (0x3ffffe6, 26),
    (0x7ffffe0, 27),
    (0x7ffffe1, 27),
    (0x3ffffe7, 26),
    (0x7ffffe2, 27),
    (0xfffff2, 24),
    (0x1fffe4, 21),
    (0x1fffe5, 21),
    (0x3ffffe8, 26),
    (0x3ffffe9, 26),
    (0xffffffd, 28),
    (0x7ffffe3, 27),
    (0x7ffffe4, 27),
    (0x7ffffe5, 27),
    (0xfffec, 20),
    (0xfffff3, 24),
    (0xfffed, 20),
    (0x1fffe6, 21),
    (0x3fffe9, 22),
    (0x1fffe7, 21),
    (0x1fffe8, 21),
    (0x7ffff3, 23),
    (0x3fffea, 22),
    (0x3fffeb, 22),
    (0x1ffffee, 25),
    (0x1ffffef, 25),
    (0xfffff4, 24),
    (0xfffff5, 24),
    (0x3ffffea, 26),
    (0x7ffff4, 23),
    (0x3ffffeb, 26),
    (0x7ffffe6, 27),
    (0x3ffffec, 26),
    (0x3ffffed, 26),
    (0x7ffffe7, 27),
    (0x7ffffe8, 27),
    (0x7ffffe9, 27),
    (0x7ffffea, 27),
    (0x7ffffeb, 27),
    (0xffffffe, 28),
    (0x7ffffec, 27),
    (0x7ffffed, 27),
    (0x7ffffee, 27),
    (0x7ffffef, 27),
    (0x7fffff0, 27),
    (0x3ffffee, 26),
    (0x3fffffff, 30),
];

/// Decodes an HPACK header block into ordered (name, value) pairs.
pub fn decode(raw: &[u8]) -> Result<Vec<(String, String)>, HpackError> {
    let mut input = Reader { buf: raw, pos: 0 };
    let mut dynamic: Vec<(String, String)> = Vec::new();
    let mut table_max = DYNAMIC_TABLE_MAX;
    let mut out = Vec::new();
    while let Some(first) = input.peek() {
        if out.len() >= MAX_HEADERS {
            return Err(HpackError("too many headers"));
        }
        if first & 0x80 != 0 {
            let index = input.integer(7)?;
            out.push(lookup(&dynamic, index)?);
        } else if first & 0x40 != 0 {
            let header = literal(&mut input, &dynamic, 6)?;
            insert(&mut dynamic, table_max, header.clone());
            out.push(header);
        } else if first & 0x20 != 0 {
            let size = input.integer(5)?;
            if size > DYNAMIC_TABLE_MAX {
                return Err(HpackError("table size update above limit"));
            }
            table_max = size;
            evict(&mut dynamic, table_max, 0);
        } else {
            // 0000xxxx without indexing, 0001xxxx never indexed.
            out.push(literal(&mut input, &dynamic, 4)?);
        }
    }
    Ok(out)
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn peek(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    fn byte(&mut self) -> Result<u8, HpackError> {
        let b = self.peek().ok_or(HpackError("truncated block"))?;
        self.pos += 1;
        Ok(b)
    }

    fn integer(&mut self, prefix: u32) -> Result<usize, HpackError> {
        let mask = (1u16 << prefix) as u8 - 1;
        let mut value = (self.byte()? & mask) as usize;
        if value < mask as usize {
            return Ok(value);
        }
        let mut shift = 0u32;
        loop {
            let b = self.byte()?;
            if shift > 21 {
                return Err(HpackError("integer overflow"));
            }
            value += ((b & 0x7f) as usize) << shift;
            shift += 7;
            if b & 0x80 == 0 {
                return Ok(value);
            }
        }
    }

    fn string(&mut self) -> Result<String, HpackError> {
        let huffman = self.peek().ok_or(HpackError("truncated block"))? & 0x80 != 0;
        let len = self.integer(7)?;
        if len > MAX_STRING || self.pos + len > self.buf.len() {
            return Err(HpackError("bad string length"));
        }
        let bytes = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        let decoded = if huffman {
            huffman_decode(bytes)?
        } else {
            bytes.to_vec()
        };
        String::from_utf8(decoded).map_err(|_| HpackError("string is not utf-8"))
    }
}

fn literal(
    input: &mut Reader<'_>,
    dynamic: &[(String, String)],
    prefix: u32,
) -> Result<(String, String), HpackError> {
    let index = input.integer(prefix)?;
    let name = if index == 0 {
        input.string()?
    } else {
        lookup(dynamic, index)?.0
    };
    let value = input.string()?;
    Ok((name.to_ascii_lowercase(), value))
}

fn lookup(dynamic: &[(String, String)], index: usize) -> Result<(String, String), HpackError> {
    match index {
        0 => Err(HpackError("index 0")),
        1..=61 => {
            let (n, v) = STATIC_TABLE[index - 1];
            Ok((n.to_string(), v.to_string()))
        }
        _ => dynamic
            .get(index - 62)
            .cloned()
            .ok_or(HpackError("index out of range")),
    }
}

fn entry_size(entry: &(String, String)) -> usize {
    entry.0.len() + entry.1.len() + 32
}

fn evict(dynamic: &mut Vec<(String, String)>, max: usize, incoming: usize) {
    let mut size: usize = dynamic.iter().map(entry_size).sum();
    while size + incoming > max {
        match dynamic.pop() {
            Some(e) => size -= entry_size(&e),
            None => break,
        }
    }
}

fn insert(dynamic: &mut Vec<(String, String)>, max: usize, entry: (String, String)) {
    let size = entry_size(&entry);
    evict(dynamic, max, size);
    if size <= max {
        dynamic.insert(0, entry);
    }
}

fn huffman_codes() -> &'static HashMap<(u8, u32), u16> {
    static CODES: OnceLock<HashMap<(u8, u32), u16>> = OnceLock::new();
    CODES.get_or_init(|| {
        HUFFMAN
            .iter()
            .enumerate()
            .map(|(sym, &(code, len))| ((len, code), sym as u16))
            .collect()
    })
}

fn huffman_decode(bytes: &[u8]) -> Result<Vec<u8>, HpackError> {
    let codes = huffman_codes();
    let mut out = Vec::with_capacity(bytes.len() * 8 / 5);
    let (mut code, mut len) = (0u32, 0u8);
    for &b in bytes {
        for i in (0..8).rev() {
            code = (code << 1) | u32::from((b >> i) & 1);
            len += 1;
            if len > 30 {
                return Err(HpackError("bad huffman code"));
            }
            if let Some(&sym) = codes.get(&(len, code)) {
                if sym == 256 {
                    return Err(HpackError("EOS in huffman string"));
                }
                out.push(sym as u8);
                code = 0;
                len = 0;
            }
        }
    }
    // Padding: fewer than 8 bits, all ones (an EOS prefix).
    if len >= 8 || code != (1u32 << len) - 1 {
        return Err(HpackError("bad huffman padding"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect()
    }

    #[test]
    fn decodes_onchfs_html_gzip_metadata() {
        let raw = hex::decode("5f87497ca589d34d1f5a839bd9ab").unwrap();
        let want = pairs(&[("content-type", "text/html"), ("content-encoding", "gzip")]);
        assert_eq!(decode(&raw).unwrap(), want);
    }

    #[test]
    fn decodes_onchfs_javascript_gzip_metadata() {
        let raw = hex::decode("5f901d75d0620d263d4c741f71a0961ab4ff5a839bd9ab").unwrap();
        let want = pairs(&[
            ("content-type", "application/javascript"),
            ("content-encoding", "gzip"),
        ]);
        assert_eq!(decode(&raw).unwrap(), want);
    }

    #[test]
    fn decodes_rfc7541_c4_huffman_requests_with_dynamic_table() {
        // C.4.1 followed by C.4.2, which indexes the entry C.4.1 inserted.
        let raw =
            hex::decode("828684418cf1e3c2e5f23a6ba0ab90f4ff828684be5886a8eb10649cbf").unwrap();
        let want = pairs(&[
            (":method", "GET"),
            (":scheme", "http"),
            (":path", "/"),
            (":authority", "www.example.com"),
            (":method", "GET"),
            (":scheme", "http"),
            (":path", "/"),
            (":authority", "www.example.com"),
            ("cache-control", "no-cache"),
        ]);
        assert_eq!(decode(&raw).unwrap(), want);
    }

    #[test]
    fn rejects_truncated_and_bad_padding() {
        assert!(decode(&[0x5f]).is_err());
        assert!(decode(&hex::decode("5f87497ca589d34d1f5a839bd9").unwrap()).is_err());
        // Huffman string whose padding is zero bits.
        assert!(decode(&[0x40, 0x81, 0x00, 0x00]).is_err());
    }

    #[test]
    fn empty_block_has_no_headers() {
        assert!(decode(&[]).unwrap().is_empty());
    }
}
