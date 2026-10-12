//! Pure preparation of derived artwork documents. Original bytes stay in the content cache.
//!
//! Sandbox compatibility and the legacy precision remedy derive from Frank Force's
//! fxhashArchive (MIT); see assets/player/LICENSE.Frank-Force.

use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};

use crate::model::Token;

const SANDBOX_SHIM: &str = include_str!("../assets/player/sandbox.js");
const BRIDGE: &str = include_str!("../assets/player/bridge.js");
const LEGACY_PATCH: &str = "<script id=\"fxhash-legacy-patch\">(function(){Math.pow = (a,b) => (a===58 && b===11) ? 24986644000165536000 : a**b;})();</script>";

pub struct PrepareRequest<'a> {
    pub html: &'a str,
    /// Absolute URL of the resolver directory, ending in a slash.
    pub base_url: &'a str,
    /// Per-frame random identifier, supplied by the parent and checked by its bridge.
    pub instance: &'a str,
}

pub struct PreparedHtml {
    pub html: String,
    pub legacy_patch: bool,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum PlayerError {
    #[error("Invalid artwork content origin")]
    InvalidOrigin,
    #[error("Invalid artwork resolver base")]
    InvalidBase,
    #[error("Invalid playback instance")]
    InvalidInstance,
    #[error("Stored artwork seed parameters conflict with the token")]
    SeedConflict,
    #[error("Stored artwork parameters conflict with the token params")]
    ParamsConflict,
    #[error("Invalid stored artwork parameters")]
    InvalidParameters,
}

fn encode(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn absolute_http(value: &str) -> Option<axum::http::Uri> {
    let uri = value.parse::<axum::http::Uri>().ok()?;
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || uri.host().is_none_or(str::is_empty)
        || uri.authority()?.as_str().contains('@')
        || value.contains('#')
        || value.chars().any(char::is_control)
    {
        return None;
    }
    Some(uri)
}

fn decoded(value: &str) -> Result<String, PlayerError> {
    let bytes = value.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'%'
            && (index + 2 >= bytes.len()
                || !bytes[index + 1..=index + 2]
                    .iter()
                    .all(u8::is_ascii_hexdigit))
        {
            return Err(PlayerError::InvalidParameters);
        }
    }
    percent_decode_str(&value.replace('+', " "))
        .decode_utf8()
        .map(|text| text.into_owned())
        .map_err(|_| PlayerError::InvalidParameters)
}

/// Preserve raw query/fragment bytes; fill absent seed fields and chain params.
pub fn render_url(content_origin: &str, token: &Token) -> Result<String, PlayerError> {
    let origin = absolute_http(content_origin).ok_or(PlayerError::InvalidOrigin)?;
    if origin.path() != "/" || origin.query().is_some() {
        return Err(PlayerError::InvalidOrigin);
    }
    let (before_hash, fragment) = token
        .artifact_uri
        .split_once('#')
        .map_or((token.artifact_uri.as_str(), None), |(uri, fragment)| {
            (uri, Some(fragment))
        });
    let query = before_hash.split_once('?').map_or("", |(_, query)| query);
    if query.len() > 65_536
        || fragment.is_some_and(|value| value.len() > 65_536)
        || token.artifact_uri.chars().any(char::is_control)
    {
        return Err(PlayerError::InvalidParameters);
    }
    let missing_fragment = match token.params.as_deref() {
        Some(params) => {
            if params.is_empty()
                || params.len() > 65_534
                || params.len() % 2 != 0
                || !params.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(PlayerError::InvalidParameters);
            }
            match fragment {
                Some(value)
                    if value
                        .strip_prefix("0x")
                        .is_some_and(|value| value.eq_ignore_ascii_case(params)) =>
                {
                    None
                }
                Some(_) => return Err(PlayerError::ParamsConflict),
                None => Some(format!("0x{params}")),
            }
        }
        None => None,
    };
    let expected = [
        ("fxhash", token.seed.clone()),
        ("fxiteration", token.iteration.to_string()),
        ("fxminter", token.minter.clone()),
    ];
    let mut present = [false; 3];
    let mut params_present = false;
    for part in query.split('&').filter(|part| !part.is_empty()) {
        let (key, value) = part.split_once('=').unwrap_or((part, ""));
        let key = decoded(key)?;
        if key == "__seeded_instance" {
            return Err(PlayerError::InvalidParameters);
        }
        if key == "fxparams" {
            let value = decoded(value)?;
            if params_present
                || token.params.as_deref().is_some_and(|params| {
                    !value
                        .strip_prefix("0x")
                        .unwrap_or(&value)
                        .eq_ignore_ascii_case(params)
                })
            {
                return Err(PlayerError::ParamsConflict);
            }
            params_present = true;
        }
        if let Some(index) = expected.iter().position(|(name, _)| *name == key) {
            let value = decoded(value)?;
            if present[index] || expected[index].1 != value {
                return Err(PlayerError::SeedConflict);
            }
            present[index] = true;
        }
    }
    let mut suffix = query.to_owned();
    for (index, (name, value)) in expected.iter().enumerate() {
        if !present[index] {
            if !suffix.is_empty() {
                suffix.push('&');
            }
            suffix.push_str(name);
            suffix.push('=');
            suffix.push_str(&encode(value));
        }
    }
    let mut result = format!(
        "{}/render/{}/{}?{suffix}",
        content_origin.trim_end_matches('/'),
        encode(&token.contract),
        token.token_id
    );
    if let Some(fragment) = fragment.or(missing_fragment.as_deref()) {
        result.push('#');
        result.push_str(fragment);
    }
    Ok(result)
}

/// Fingerprint only the old fxhash base58 decoder, leaving v2 and unrelated powers intact.
pub fn needs_legacy_patch(html: &str) -> bool {
    let compact: String = html
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect();
    compact.contains("Math.pow(alphabet.length,")
        && html
            .split(|character: char| {
                !character.is_ascii_alphanumeric() && character != '_' && character != '$'
            })
            .any(|word| word == "fxhash")
}

fn escape_attribute(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Find a leading doctype or structural tag without mistaking comments/JS strings for tags.
fn insertion_point(html: &str) -> usize {
    let bytes = html.as_bytes();
    let mut cursor = usize::from(bytes.starts_with(&[0xef, 0xbb, 0xbf])) * 3;
    let mut after_doctype = cursor;
    while cursor < bytes.len().min(4096) {
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        if bytes.get(cursor..cursor + 4) == Some(b"<!--") {
            let Some(end) = html[cursor + 4..].find("-->") else {
                return after_doctype;
            };
            cursor += end + 7;
            continue;
        }
        if bytes
            .get(cursor..cursor + 9)
            .is_some_and(|tag| tag.eq_ignore_ascii_case(b"<!doctype"))
        {
            let Some(end) = html[cursor..].find('>') else {
                return after_doctype;
            };
            cursor += end + 1;
            after_doctype = cursor;
            continue;
        }
        for tag in [b"<html".as_slice(), b"<head".as_slice()] {
            if bytes
                .get(cursor..cursor + tag.len())
                .is_some_and(|value| value.eq_ignore_ascii_case(tag))
                && bytes
                    .get(cursor + tag.len())
                    .is_some_and(|byte| byte.is_ascii_whitespace() || *byte == b'>')
            {
                let mut quote = None;
                for (offset, byte) in bytes[cursor + tag.len()..].iter().enumerate() {
                    if let Some(mark) = quote {
                        if *byte == mark {
                            quote = None;
                        }
                    } else if *byte == b'\'' || *byte == b'"' {
                        quote = Some(*byte);
                    } else if *byte == b'>' {
                        return cursor + tag.len() + offset + 1;
                    }
                }
            }
        }
        return after_doctype;
    }
    after_doctype
}

pub fn prepare_html(request: PrepareRequest<'_>) -> Result<PreparedHtml, PlayerError> {
    if request.instance.len() != 32
        || !request
            .instance
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(PlayerError::InvalidInstance);
    }
    let base = absolute_http(request.base_url).ok_or(PlayerError::InvalidBase)?;
    if !base.path().ends_with('/')
        || base.query().is_some()
        || !base.path().starts_with("/content/")
    {
        return Err(PlayerError::InvalidBase);
    }
    let legacy_patch = needs_legacy_patch(request.html);
    let at = insertion_point(request.html);
    let mut html =
        String::with_capacity(request.html.len() + SANDBOX_SHIM.len() + BRIDGE.len() + 512);
    html.push_str(&request.html[..at]);
    html.push_str(
        "\n<!-- Derived SEEDED runner; original artwork bytes remain unchanged. -->\n<base href=\"",
    );
    html.push_str(&escape_attribute(request.base_url));
    html.push_str("\">\n");
    if legacy_patch {
        html.push_str(LEGACY_PATCH);
    }
    html.push_str("<script data-seeded-instance=\"");
    html.push_str(request.instance);
    html.push_str("\">\n");
    html.push_str(BRIDGE);
    html.push('\n');
    html.push_str(SANDBOX_SHIM);
    html.push_str("\n</script>\n");
    html.push_str(&request.html[at..]);
    Ok(PreparedHtml { html, legacy_patch })
}

/// Generic failure document for the separate artwork origin; never include resolver details.
pub fn prepare_error_document(instance: &str, status: u16) -> Result<String, PlayerError> {
    if instance.len() != 32 || !instance.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(PlayerError::InvalidInstance);
    }
    Ok(format!(
        "<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><title>NFT unavailable</title><p>NFT unavailable ({status}). Return to the archive to retry.</p><script>parent.postMessage({{type:'seeded-player',instance:'{instance}',state:'unavailable'}},'*');</script></html>"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token() -> Token {
        Token { contract: "KT1test".into(), token_id: 4, project_id: 1, iteration: 9, seed: "ooSeed".into(), minter: "tz1Minter".into(), minted: String::new(), artifact_uri: "ipfs://QmX/index.html?fxhash=ooSeed&fxiteration=9&fxminter=tz1Minter&fxparams=%26raw#0x1234%ab".into(), preview: None, features: Vec::new(), owner: None, display_uri: None, thumbnail_uri: None, params: None, verification: crate::model::Verification::Unverified, source: crate::model::Source::Fixture }
    }

    #[test]
    fn artwork_parameters_and_fragment_remain_byte_exact() {
        assert_eq!(
            render_url("http://127.0.0.1:3919", &token()).unwrap(),
            "http://127.0.0.1:3919/render/KT1test/4?fxhash=ooSeed&fxiteration=9&fxminter=tz1Minter&fxparams=%26raw#0x1234%ab"
        );
    }

    #[test]
    fn missing_seed_fields_are_filled_without_changing_parameters() {
        let mut token = token();
        token.artifact_uri = "ipfs://QmX/?fxhash=ooSeed&custom=a+b#0x42".into();
        assert!(
            render_url("https://art.example", &token)
                .unwrap()
                .ends_with("?fxhash=ooSeed&custom=a+b&fxiteration=9&fxminter=tz1Minter#0x42")
        );
    }

    #[test]
    fn chain_params_fill_only_absent_fragments_and_preserve_matching_bytes() {
        let mut token = token();
        token.params = Some("00a1Ff".into());
        token.artifact_uri = "ipfs://QmX/?custom=a+b&encoded=%2f&fxhash=ooSeed".into();
        let original = token.artifact_uri.clone();
        assert!(
            render_url("https://art.example", &token)
                .unwrap()
                .ends_with("?custom=a+b&encoded=%2f&fxhash=ooSeed&fxiteration=9&fxminter=tz1Minter#0x00a1Ff")
        );
        assert_eq!(token.artifact_uri, original);
        token.artifact_uri.push_str("#0x00A1ff");
        assert!(
            render_url("https://art.example", &token)
                .unwrap()
                .ends_with("#0x00A1ff")
        );
    }

    #[test]
    fn query_params_match_chain_bytes_without_rewriting_the_uri() {
        let mut token = token();
        token.params = Some("00ab".into());
        for value in ["00AB", "0x00ab", "%30%30ab"] {
            token.artifact_uri = format!("ipfs://QmX/?fxparams={value}#0x00ab");
            assert!(
                render_url("https://art.example", &token)
                    .unwrap()
                    .contains(&format!("fxparams={value}&"))
            );
        }
        for query in [
            "fxparams=00ac",
            "fxparams=",
            "fxparams=00ab&%66xparams=00ab",
        ] {
            token.artifact_uri = format!("ipfs://QmX/?{query}#0x00ab");
            assert_eq!(
                render_url("https://art.example", &token),
                Err(PlayerError::ParamsConflict)
            );
        }
    }

    #[test]
    fn conflicting_params_and_invalid_chain_hex_cannot_produce_playback_urls() {
        let mut token = token();
        token.params = Some("1234".into());
        for fragment in ["", "0x1235", "0x12%33%34", "chapter", "0X1234"] {
            token.artifact_uri = format!("ipfs://QmX/#{fragment}");
            assert_eq!(
                render_url("https://art.example", &token),
                Err(PlayerError::ParamsConflict)
            );
        }
        token.artifact_uri = "ipfs://QmX/".into();
        for params in ["", "0", "0x1234", "zz", "12 34", &"a".repeat(65_536)] {
            token.params = Some(params.into());
            assert_eq!(
                render_url("https://art.example", &token),
                Err(PlayerError::InvalidParameters)
            );
        }
    }

    #[test]
    fn conflicting_or_duplicate_seed_parameters_fail() {
        for query in [
            "fxhash=other",
            "fxhash=ooSeed&fxhash=ooSeed",
            "fxhash=ooSeed&%66xhash=ooSeed",
            "fxiteration=8",
            "fxminter=other",
            "__seeded_instance=old",
            "fxhash=%xx",
        ] {
            let mut token = token();
            token.artifact_uri = format!("ipfs://QmX/?{query}");
            assert!(
                render_url("http://127.0.0.1:3919", &token).is_err(),
                "{query}"
            );
        }
    }

    #[test]
    fn non_origins_cannot_become_frame_urls() {
        for origin in [
            "javascript:bad",
            "//host",
            "https://user@host",
            "https://host/path",
            "https://host?query",
            "https://host/#fragment",
        ] {
            assert!(render_url(origin, &token()).is_err());
        }
    }

    #[test]
    fn shim_and_v1_patch_precede_artwork_without_rewriting_original_bytes() {
        let original = "<!doctype html><HTML lang=\"en\"><head><script>var fxhash='ooSeed';Math . pow ( alphabet . length , 11);</script></head></HTML>";
        let result = prepare_html(PrepareRequest {
            html: original,
            base_url: "http://127.0.0.1:3919/content/ipfs/QmX/",
            instance: "0123456789abcdef0123456789abcdef",
        })
        .unwrap();
        assert!(result.legacy_patch);
        assert!(result.html.starts_with("<!doctype html><HTML lang=\"en\">"));
        assert!(result.html.ends_with("<head><script>var fxhash='ooSeed';Math . pow ( alphabet . length , 11);</script></head></HTML>"));
        assert!(
            result.html.find("fxhash-legacy-patch").unwrap()
                < result.html.find("var fxhash").unwrap()
        );
    }

    #[test]
    fn v2_and_unrelated_powers_do_not_receive_precision_patch() {
        assert!(!needs_legacy_patch("var fxhash='oo'; Math.pow(x, 2)"));
        assert!(!needs_legacy_patch(
            "Math.pow(alphabet.length, 11); var notfxhash='x'"
        ));
        assert!(!needs_legacy_patch(
            "var fxhash='oo'; let b58dec=s=>[...s].reduce((p,c)=>p*alphabet.length+alphabet.indexOf(c),0)"
        ));
    }

    #[test]
    fn headless_and_misleading_structure_still_prepend_before_scripts() {
        for original in [
            "<script>var text='<head>';</script>",
            "<!-- <html> --><script>go()</script><html>",
            "<header>Header</header><script>go()</script>",
        ] {
            assert_eq!(insertion_point(original), 0);
        }
        assert_eq!(
            insertion_point("<!doctype html><script>go()</script><head>"),
            15
        );
        assert_eq!(
            insertion_point("<head data-x='>'><script>go()</script>"),
            17
        );
    }

    #[test]
    fn invalid_instance_and_base_fail_before_any_html_is_returned() {
        for (instance, base) in [
            ("bad", "http://host/content/ipfs/x/"),
            ("0123456789abcdef0123456789abcdef", "javascript:alert(1)"),
            (
                "0123456789abcdef0123456789abcdef",
                "http://host/content/ipfs/x/index.html",
            ),
        ] {
            assert!(
                prepare_html(PrepareRequest {
                    html: "<html></html>",
                    base_url: base,
                    instance
                })
                .is_err()
            );
        }
    }

    #[test]
    fn failure_documents_report_only_bounded_identity_and_status() {
        let html = prepare_error_document("0123456789abcdef0123456789abcdef", 503).unwrap();
        assert!(html.contains("state:'unavailable'"));
        assert!(html.contains("NFT unavailable (503)"));
        assert!(prepare_error_document("</script><script>bad()", 503).is_err());
    }
}
