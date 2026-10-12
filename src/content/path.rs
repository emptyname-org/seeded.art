//! Request path validation, shared by ONCHFS and IPFS.

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};

use super::ContentError;

const MAX_PATH_BYTES: usize = 4096;
const MAX_SEGMENTS: usize = 64;

/// Characters `encodeURIComponent` leaves alone; ONCHFS names are stored with
/// everything else escaped.
const URI_COMPONENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'!')
    .remove(b'~')
    .remove(b'*')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')');

/// One path segment: as received, and percent-decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub raw: String,
    pub decoded: String,
}

impl Segment {
    /// Names to try against a directory, most exact first, deduplicated.
    pub fn candidates(&self) -> Vec<String> {
        let mut out = vec![self.raw.clone()];
        for c in [uri_component(&self.decoded), self.decoded.clone()] {
            if !out.contains(&c) {
                out.push(c);
            }
        }
        out
    }

    /// The segment escaped for use in an outbound URL.
    pub fn url_encoded(&self) -> String {
        uri_component(&self.decoded)
    }
}

/// `encodeURIComponent` equivalent.
pub fn uri_component(s: &str) -> String {
    utf8_percent_encode(s, URI_COMPONENT).to_string()
}

/// Splits a request path into segments. Empty segments are dropped, as the
/// ONCHFS reference resolver does. Rejects `.`/`..` (also when escaped),
/// backslashes, control characters, bad escapes, `?`/`#` and oversize paths.
pub fn split(path: &str) -> Result<Vec<Segment>, ContentError> {
    if path.len() > MAX_PATH_BYTES {
        return Err(ContentError::bad_request("path too long"));
    }
    if path.contains(['?', '#']) {
        return Err(ContentError::bad_request("query or fragment in path"));
    }
    let mut out = Vec::new();
    for raw in path.split('/').filter(|s| !s.is_empty()) {
        if out.len() == MAX_SEGMENTS {
            return Err(ContentError::bad_request("path too deep"));
        }
        out.push(segment(raw)?);
    }
    Ok(out)
}

fn segment(raw: &str) -> Result<Segment, ContentError> {
    let bytes = raw.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'%'
            && !(bytes.get(i + 1).is_some_and(u8::is_ascii_hexdigit)
                && bytes.get(i + 2).is_some_and(u8::is_ascii_hexdigit))
        {
            return Err(ContentError::bad_request("malformed percent escape"));
        }
    }
    let decoded = percent_decode_str(raw)
        .decode_utf8()
        .map_err(|_| ContentError::bad_request("path is not utf-8"))?
        .into_owned();
    if decoded == "." || decoded == ".." {
        return Err(ContentError::bad_request("dot segment in path"));
    }
    if decoded
        .chars()
        .any(|c| c == '/' || c == '\\' || c.is_control())
    {
        return Err(ContentError::bad_request("forbidden character in path"));
    }
    Ok(Segment {
        raw: raw.to_string(),
        decoded,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::ErrorKind;

    fn bad(p: &str) -> bool {
        split(p).is_err_and(|e| e.kind == ErrorKind::BadRequest)
    }

    #[test]
    fn drops_empty_segments() {
        let s = split("/a//b/").unwrap();
        assert_eq!(
            s.iter().map(|x| x.decoded.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert!(split("").unwrap().is_empty());
    }

    #[test]
    fn rejects_traversal_in_every_spelling() {
        for p in [
            "..", "a/../b", "./x", "%2e%2e/x", "%2E%2e", "a%2fb", "a%5cb", "a\\b", "%00",
        ] {
            assert!(bad(p), "{p}");
        }
    }

    #[test]
    fn rejects_bad_escapes_query_and_size() {
        for p in ["%", "%4", "%zz", "a?b", "a#b", "%ff"] {
            assert!(bad(p), "{p}");
        }
        assert!(bad(&"a/".repeat(65)));
        assert!(bad(&"a".repeat(4097)));
    }

    #[test]
    fn candidates_cover_raw_component_and_decoded_forms() {
        let s = split("my%20file%27s.js").unwrap().remove(0);
        assert_eq!(s.decoded, "my file's.js");
        assert_eq!(
            s.candidates(),
            ["my%20file%27s.js", "my%20file's.js", "my file's.js"]
        );
        assert_eq!(s.url_encoded(), "my%20file's.js");
    }
}
