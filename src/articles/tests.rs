//! Unit tests: metadata and media parsing, TzKT page validation, JSON retries, object state.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::http::{ApiError, Fetch, HttpResponse, JsonClient, RetryPolicy, TransportError};
use super::ipfs::{Outcome, Verification};
use super::parse::{self, Media};
use super::store::{ArticleStore, FetchState, ObjectKind, PendingObject};
use super::tzkt::Tzkt;

struct Script(Mutex<VecDeque<Result<HttpResponse, TransportError>>>);

impl Fetch for Script {
    fn get(&self, _url: &str, _accept: &str, _cap: u64) -> Result<HttpResponse, TransportError> {
        self.0
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected request")
    }
}

fn reply(status: u16, body: &str) -> Result<HttpResponse, TransportError> {
    Ok(HttpResponse {
        status,
        content_type: Some("application/json".into()),
        body: body.as_bytes().to_vec(),
    })
}

fn client(script: Vec<Result<HttpResponse, TransportError>>, attempts: u32) -> JsonClient {
    JsonClient::new(
        Arc::new(Script(Mutex::new(script.into()))),
        "http://x/v1/",
        RetryPolicy {
            attempts,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        },
        Duration::ZERO,
        Arc::new(AtomicBool::new(false)),
    )
}

fn permanent<T: std::fmt::Debug>(r: Result<T, ApiError>, needle: &str) {
    match r {
        Err(ApiError::Permanent(m)) => assert!(m.contains(needle), "{m}"),
        other => panic!("expected a permanent error with {needle:?}, got {other:?}"),
    }
}

#[test]
fn metadata_keeps_article_fields_and_ignores_junk() {
    let v = json!({
        "decimals": 0, "symbol": "ARTKL", "name": "  Color layers ",
        "description": "In this work, we see layers of colored walls",
        "minter": "tz1MqNDsm9mJGaN4x9V8XXAM5xzbw3hHwTZi", "type": "article",
        "tags": ["#color", " #color ", "", 7, null, "beauty"],
        "language": "en-US",
        "artifactUri": "ipfs://Qmd99qDnUmdMk1SQBhDN9daqVVF47Hcn71BGBzb2MEQtPC",
        "displayUri": "ipfs://QmdVXCGXYZX2ffsfDinfHNndG2ShUPGxeP3REM7WH4qegp",
        "thumbnailUri": "ipfs://QmdUMxVyjEbRcVFH2F1p1p5ncFnpnGjmxNR4TnmNvNcW1o",
        "thumbnailCaption": 12, "platforms": ["fxhash"]
    });
    let m = parse::metadata(&v).unwrap();
    assert_eq!(m.name.as_deref(), Some("Color layers"));
    assert_eq!(m.tags, vec!["#color".to_owned(), "beauty".to_owned()]);
    assert_eq!(m.language.as_deref(), Some("en-US"));
    assert_eq!(
        m.artifact_uri.as_deref(),
        Some("ipfs://Qmd99qDnUmdMk1SQBhDN9daqVVF47Hcn71BGBzb2MEQtPC")
    );
    assert_eq!(m.thumbnail_caption, None, "non-string caption is ignored");
    assert_eq!(
        m.minter.as_deref(),
        Some("tz1MqNDsm9mJGaN4x9V8XXAM5xzbw3hHwTZi")
    );
    assert_eq!(parse::metadata(&json!([1])), None);
    assert_eq!(parse::metadata(&json!("x")), None);
    let long = parse::metadata(&json!({"name": "x".repeat(30_000)})).unwrap();
    assert_eq!(long.name.unwrap().chars().count(), 20_000);
}

#[test]
fn media_lists_images_html_and_directives_but_not_code() {
    let body = "# Title\n\
        ![Parnassus #20, mpkoz](ipfs://QmcUqFHsgaB9VaebBPFgyX6eEP67nkfGBkGgPM2d2Q22Nz)\n\
        Text ![](https://ipfs.io/ipfs/QmZ52YTjbBFpSb7eGWWq8cH1SX1P57RnHbf8d2Kc4Uz18A/a.png \"t\") and \
        ![x](<https://example.com/a b.png>)\n\
        ![again](ipfs://QmcUqFHsgaB9VaebBPFgyX6eEP67nkfGBkGgPM2d2Q22Nz)\n\
        <img alt=\"a\" SRC='https://bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.ipfs.dweb.link/x.jpg'>\n\
        <video controls><source src=\"ipfs://QmVideo1\" type=\"video/mp4\"></video>\n\
        ::video[]{src=\"https://ipfs.io/ipfs/Qmc2vsRCWwXiiuFeaQ1Gq89uofJxKSFDs4iA8cC6TcP8T2\"}\n\
        ::embed-media[]{href=\"https://www.youtube.com/watch?v=-eE7dwoHMEI\"}\n\
        ::tezos-storage-pointer[]{contract=\"KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE\" path=\"token_metadata::0\"}\n\
        ```\n![in code](ipfs://QmCode)\n```\n\
        ~~~md\n<img src=\"ipfs://QmCode2\">\n~~~\n";
    let m = parse::media(body);
    let got: Vec<(&str, &str, Option<&str>)> = m
        .iter()
        .map(|Media { kind, src, ipfs }| (kind.as_str(), src.as_str(), ipfs.as_deref()))
        .collect();
    assert_eq!(
        got,
        vec![
            (
                "image",
                "ipfs://QmcUqFHsgaB9VaebBPFgyX6eEP67nkfGBkGgPM2d2Q22Nz",
                Some("ipfs://QmcUqFHsgaB9VaebBPFgyX6eEP67nkfGBkGgPM2d2Q22Nz")
            ),
            (
                "image",
                "https://ipfs.io/ipfs/QmZ52YTjbBFpSb7eGWWq8cH1SX1P57RnHbf8d2Kc4Uz18A/a.png",
                Some("ipfs://QmZ52YTjbBFpSb7eGWWq8cH1SX1P57RnHbf8d2Kc4Uz18A/a.png")
            ),
            ("image", "https://example.com/a b.png", None),
            (
                "image",
                "https://bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.ipfs.dweb.link/x.jpg",
                Some("ipfs://bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi/x.jpg")
            ),
            ("media", "ipfs://QmVideo1", Some("ipfs://QmVideo1")),
            (
                "video",
                "https://ipfs.io/ipfs/Qmc2vsRCWwXiiuFeaQ1Gq89uofJxKSFDs4iA8cC6TcP8T2",
                Some("ipfs://Qmc2vsRCWwXiiuFeaQ1Gq89uofJxKSFDs4iA8cC6TcP8T2")
            ),
            (
                "embed-media",
                "https://www.youtube.com/watch?v=-eE7dwoHMEI",
                None
            ),
        ]
    );
    assert!(parse::media("![unclosed](ipfs://Qm\n![](  )\n::x\n::y[]{}").len() <= 1);
}

#[test]
fn ipfs_uri_recognises_gateway_forms_only() {
    assert_eq!(
        parse::ipfs_uri("ipfs://ipfs/QmA/b?x#y").as_deref(),
        Some("ipfs://QmA/b")
    );
    assert_eq!(
        parse::ipfs_uri("https://gateway.fxhash.xyz/ipfs/QmA/").as_deref(),
        Some("ipfs://QmA")
    );
    for none in [
        "https://example.com/QmA",
        "https://x.ipfs./a",
        "ipfs://",
        "data:image/png;base64,AAA",
        "https://example.com/ipfs/Q m",
    ] {
        assert_eq!(parse::ipfs_uri(none), None, "{none:?}");
    }
}

#[test]
fn json_client_retries_transient_failures_and_reports_absence() {
    let c = client(
        vec![
            Err(TransportError::Transient("reset".into())),
            reply(503, ""),
            reply(200, "[1]"),
        ],
        3,
    );
    assert_eq!(c.get_json::<Vec<u8>>("/a").unwrap(), Some(vec![1]));
    assert_eq!(c.retries.load(Ordering::Relaxed), 2);
    assert!(matches!(
        client(vec![reply(429, ""), reply(500, "")], 2).get_json::<u8>("/a"),
        Err(ApiError::Transient(_))
    ));
    assert_eq!(
        client(vec![reply(404, "")], 1).get_json::<u8>("/a"),
        Ok(None)
    );
    permanent(
        client(vec![reply(400, "")], 3).get_json::<u8>("/a"),
        "HTTP 400",
    );
    permanent(
        client(vec![reply(200, "{")], 3).get_json::<u8>("/a"),
        "unusable JSON",
    );
    permanent(
        client(vec![Err(TransportError::TooLarge(1))], 3).get_json::<u8>("/a"),
        "larger than",
    );
    let stopped = JsonClient::new(
        Arc::new(Script(Mutex::new(VecDeque::new()))),
        "http://x/v1",
        RetryPolicy::none(),
        Duration::ZERO,
        Arc::new(AtomicBool::new(true)),
    );
    assert_eq!(stopped.get_json::<u8>("/a"), Err(ApiError::Interrupted));
}

fn tzkt_with(bodies: Vec<serde_json::Value>) -> (JsonClient, usize) {
    (
        client(
            bodies
                .into_iter()
                .map(|b| reply(200, &b.to_string()))
                .collect(),
            1,
        ),
        2,
    )
}

fn token(id: u64, token_id: &str, supply: &str) -> serde_json::Value {
    json!({"id": id, "tokenId": token_id, "firstLevel": 1, "firstTime": "2022-08-30T17:23:59Z",
        "totalSupply": supply, "totalMinted": supply, "totalBurned": "0"})
}

#[test]
fn tzkt_pages_are_validated() {
    // Repeated ids within a page.
    let (c, page) = tzkt_with(vec![json!([token(5, "1", "1"), token(5, "2", "1")])]);
    permanent(Tzkt { client: &c, page }.tokens(), "repeat or go backwards");
    // More rows than asked for.
    let (c, page) = tzkt_with(vec![json!([
        token(1, "1", "1"),
        token(2, "2", "1"),
        token(3, "3", "1")
    ])]);
    permanent(Tzkt { client: &c, page }.tokens(), "rows for a page of 2");
    // Second page going backwards.
    let (c, page) = tzkt_with(vec![
        json!([token(4, "1", "1"), token(6, "2", "1")]),
        json!([token(6, "3", "1")]),
    ]);
    permanent(Tzkt { client: &c, page }.tokens(), "repeat or go backwards");
    // Non-numeric amounts are refused; huge ones saturate.
    let (c, page) = tzkt_with(vec![json!([token(1, "1", "-3")])]);
    permanent(Tzkt { client: &c, page }.tokens(), "natural number");
    let (c, page) = tzkt_with(vec![json!([token(1, "1", "99999999999999999999999")])]);
    let t = Tzkt { client: &c, page }.tokens().unwrap();
    assert_eq!(t[0].supply, i64::MAX);
    // A full page is followed by another request; a short page ends paging.
    let (c, page) = tzkt_with(vec![
        json!([token(1, "1", "1"), token(2, "2", "1")]),
        json!([token(3, "3", "1")]),
    ]);
    assert_eq!(Tzkt { client: &c, page }.tokens().unwrap().len(), 3);
}

#[test]
fn listings_for_another_contract_are_refused() {
    let contracts = json!([{"id": 1, "key": "2", "value": {"fa2": super::ARTICLES_CONTRACT},
        "lastLevel": 1}]);
    let listing = json!([{"id": 2, "key": "17", "lastLevel": 5, "value": {
        "amount": "1000000", "seller": "tz1cNakLoctaqaCtBZSQGZs5RgAciYqB7cNf",
        "currency": {"tez": {}}, "asset": {"amount": "3", "asset_id": "1", "contract_id": "1"}}}]);
    let (c, page) = tzkt_with(vec![contracts, listing]);
    permanent(Tzkt { client: &c, page }.listings(), "another contract");
}

#[test]
fn a_stored_object_is_never_downgraded() {
    let mut s = ArticleStore::open_in_memory().unwrap();
    let item = PendingObject {
        key: "ipfs://QmA".into(),
        kind: ObjectKind::Body,
    };
    let ok = Outcome::Ok {
        data: b"# hi ![](ipfs://QmB)".to_vec(),
        verification: Verification::Car,
        gateway: "http://gw/ipfs/".into(),
    };
    assert_eq!(
        s.record_objects(&[(item.clone(), Outcome::Transient("x".into()))])
            .unwrap(),
        vec![Some(FetchState::Transient)]
    );
    assert_eq!(
        s.record_objects(&[(item.clone(), ok)]).unwrap(),
        vec![Some(FetchState::Ok)]
    );
    for later in [
        Outcome::Transient("y".into()),
        Outcome::Absent("z".into()),
        Outcome::NotFound("w".into()),
    ] {
        assert_eq!(
            s.record_objects(&[(item.clone(), later)]).unwrap(),
            vec![None]
        );
    }
    let st = s.stats().unwrap();
    assert_eq!(st.media, 1);
    assert_eq!(st.body_bytes, 20);
    // An unverified copy is replaced by a verified one only.
    let maybe = PendingObject {
        key: "ipfs://QmU".into(),
        kind: ObjectKind::Body,
    };
    let copy = |text: &str, verification| Outcome::Ok {
        data: text.as_bytes().to_vec(),
        verification,
        gateway: String::new(),
    };
    for (outcome, written) in [
        (
            copy("unverified", Verification::Unverified),
            Some(FetchState::Ok),
        ),
        (copy("unverified again", Verification::Unverified), None),
        (Outcome::Absent("no".into()), None),
        (copy("verified", Verification::Car), Some(FetchState::Ok)),
        (copy("unverified later", Verification::Unverified), None),
    ] {
        assert_eq!(
            s.record_objects(&[(maybe.clone(), outcome)]).unwrap(),
            vec![written]
        );
    }
    let st = s.stats().unwrap();
    assert!(
        st.objects.iter().any(|(k, state, v, n, bytes)| k == "body"
            && state == "ok"
            && v.as_deref() == Some("ipfs-car")
            && *n == 2
            && *bytes == 28),
        "{:?}",
        st.objects
    );
    // Unverified non-UTF-8 is transient; verified non-UTF-8 is absent; interrupted writes nothing.
    let bin = |v| Outcome::Ok {
        data: vec![0xff, 0xfe],
        verification: v,
        gateway: String::new(),
    };
    let other = |k: &str| PendingObject {
        key: k.into(),
        kind: ObjectKind::Metadata,
    };
    assert_eq!(
        s.record_objects(&[
            (other("ipfs://QmC"), bin(Verification::Unverified)),
            (other("ipfs://QmD"), bin(Verification::Car)),
            (other("ipfs://QmE"), Outcome::Interrupted),
        ])
        .unwrap(),
        vec![Some(FetchState::Transient), Some(FetchState::Absent), None]
    );
}
