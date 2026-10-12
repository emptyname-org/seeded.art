//! References from articles to fxhash projects and tokens: every written form, catalogue
//! resolution (with, without, later, positive-only), generator roots, visibility rules, the
//! holder and project listings, and the schema 1 -> 2 migration. No network.

#[path = "articles_support/mod.rs"]
mod support;

use seeded_archive::articles::MARKETPLACE_V3;
use seeded_archive::articles::ipfs::{Outcome, Verification, ipfs_ref};
use seeded_archive::articles::refs::{
    self, GENTK_V1, GENTK_V2, GENTK_V3, MAX_REFS, Reference, Target, open_archive,
};
use seeded_archive::articles::store::{
    ArticleCursor, ArticleRef, ArticleStore, ArticleStoreError, ObjectKind, PendingObject, RefKind,
};
use seeded_archive::articles::sync::SyncOptions;
use support::{Art, Harness, art, catalogue, scratch, store_bodies, synthetic};

const ALICE: &str = "tz1fepn7jZsCYBqCDhpM63hzh9g2Ytqk4Tpv";
const BOB: &str = "tz1cNakLoctaqaCtBZSQGZs5RgAciYqB7cNf";
const CAROL: &str = "tz1TSdoeS5udkYmVr1cEpErTqxGVrC3Kcd2J";
/// A real generator root (project #12066) and its CIDv1 spelling.
const GENERATOR: &str = "QmS9n1UK3WYvMzR5zkaJ3WzS2QzWatVAjve52MBW9J7myi";
const LIVE: &str = "QmdjkkALThxrHc8D4L7rBoEqgjPFVJRUh5SBeKd3d9btFW";
const ONCHFS: &str = "5e5f7d3b2a1c0e9f8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e0d9c8b7a6f5e";

fn v1(cid: &str) -> String {
    cid::Cid::try_from(cid)
        .unwrap()
        .into_v1()
        .unwrap()
        .to_string()
}

fn root(cid: &str) -> Target {
    Target::Root {
        scheme: "ipfs",
        cid: v1(cid),
    }
}

fn token(contract: &'static str, token_id: u64) -> Target {
    Target::Token { contract, token_id }
}

fn slug(s: &str) -> Target {
    Target::ProjectSlug(s.into())
}

fn targets(body: &str) -> Vec<(Target, &'static str)> {
    refs::references(body)
        .into_iter()
        .map(|Reference { target, form, .. }| (target, form))
        .collect()
}

fn bad_request<T: std::fmt::Debug>(r: Result<T, ArticleStoreError>) -> String {
    match r {
        Err(ArticleStoreError::BadRequest(m)) => m,
        other => panic!("expected BadRequest, got {other:?}"),
    }
}

fn ids(page: &seeded_archive::articles::store::ArticlePage) -> Vec<u64> {
    page.items.iter().map(|a| a.token_id).collect()
}

/// `(kind, project, contract, token)` of what an article page would link.
fn linked(refs: &[ArticleRef]) -> Vec<(RefKind, u64, Option<&str>, Option<u64>)> {
    refs.iter()
        .map(|r| (r.kind, r.project_id, r.contract.as_deref(), r.token_id))
        .collect()
}

#[test]
fn every_reference_form_is_parsed_in_order_once() {
    let body = format!(
        "# Forms\n\
         ::tezos-storage-pointer[]{{contract=\"KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv\" path=\"ledger::12066\" data_spec=\"FX-GEN-DATA-002\"}}\n\
         ::tezos-storage-pointer[A caption]{{contract=\"KT1Xpmp15KfqoePNW9HczFmqaGNHwadV2a3b.mainnet\" path=\"ledger::31707\"}}\n\
         ::tezos-storage-pointer[]{{contract=\"KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE\" path=\"token_metadata::470729\"}}\n\
         ::tezos-storage-pointer[]{{contract=\"KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi\" path=\"token_metadata::1053029\"}}\n\
         ::tezos-storage-pointer[]{{contract=\"KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr\" path=\"token_metadata::217311\"}}\n\
         See [one](https://www.fxhash.xyz/generative/9097) and <https://fxhash.xyz/marketplace/generative/25828>.\n\
         Slugs: https://www.fxhash.xyz/generative/slug/tally! and [star](https://www.fxhash.xyz/generative/slug/star-(complex)).\n\
         New: www.fxhash.xyz/project/adding-to, http://www.fxhash.xyz/project/31743/?x=1#y\n\
         Tokens: https://www.fxhash.xyz/gentk/30615 https://www.fxhash.xyz/gentk/FX0-788903 \
         https://www.fxhash.xyz/gentk/FX1-42874 \
         https://www.fxhash.xyz/gentk/KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr-38899 \
         https://www.fxhash.xyz/gentk/slug/garden-monoliths-84\n\
         Markets: https://objkt.com/asset/fxhash/1168718 https://objkt.com/tokens/fxparams/42134 \
         https://objkt.com/asset/KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE/7 \
         https://tzkt.io/KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE/tokens/259443/metadata\n\
         Live: [view](https://gateway.fxhash2.xyz/ipfs/{LIVE}/?fxhash=ooKR) ![g](ipfs://{GENERATOR}) \
         <a href=\"onchfs://{ONCHFS}/index.html\">x</a>\n"
    );
    use refs::{FORM_FXHASH as U, FORM_GENERATOR as G, FORM_MARKET as M, FORM_POINTER as P};
    assert_eq!(
        targets(&body),
        vec![
            (Target::Project(12066), P),
            (Target::Project(31707), P),
            (token(GENTK_V1, 470729), P),
            (token(GENTK_V2, 1053029), P),
            (token(GENTK_V3, 217311), P),
            (Target::Project(9097), U),
            (Target::Project(25828), U),
            (slug("tally!"), U),
            (slug("star-(complex)"), U),
            (slug("adding-to"), U),
            (slug("31743"), U),
            (token(GENTK_V1, 30615), U),
            (token(GENTK_V2, 788903), U),
            (token(GENTK_V3, 42874), U),
            (token(GENTK_V3, 38899), U),
            (Target::TokenSlug("garden-monoliths-84".into()), U),
            (token(GENTK_V2, 1168718), M),
            (token(GENTK_V3, 42134), M),
            (token(GENTK_V1, 7), M),
            (token(GENTK_V1, 259443), M),
            (root(LIVE), G),
            (root(GENERATOR), G),
            (
                Target::Root {
                    scheme: "onchfs",
                    cid: ONCHFS.into()
                },
                G
            ),
        ]
    );
    let first = &refs::references(&body)[0];
    assert!(
        first
            .source
            .starts_with("::tezos-storage-pointer[]{contract=")
    );
    assert_eq!(first.target.kind(), "project");
    assert_eq!(refs::references(&body)[2].target.kind(), "token");
}

#[test]
fn spellings_of_one_target_collapse_and_edges_parse() {
    let body = format!(
        "https://www.fxhash.xyz/generative/9097. then **https://www.fxhash.xyz/generative/9097**\n\
         [again](https://www.fxhash.xyz/marketplace/generative/9097)\n\
         https://www.fxhash.xyz/generative/slug/punevyr%27s-face and \
         https://www.fxhash.xyz/generative/slug/star-\\(complex\\)\n\
         https://www.fxhash.xyz/gentk/{} is v2, https://www.fxhash.xyz/gentk/{} is v1\n\
         https://{}.ipfs.dweb.link/ and ipfs://{GENERATOR}?fxhash=oo\n\
         HTTPS://WWW.FXHASH.XYZ/generative/77\n",
        refs::GENTK_V2_FIRST_ID,
        refs::GENTK_V2_FIRST_ID - 1,
        v1(GENERATOR),
    );
    assert_eq!(
        targets(&body)
            .into_iter()
            .map(|(t, _)| t)
            .collect::<Vec<_>>(),
        vec![
            Target::Project(9097),
            slug("punevyr's-face"),
            slug("star-(complex)"),
            token(GENTK_V2, refs::GENTK_V2_FIRST_ID),
            token(GENTK_V1, refs::GENTK_V2_FIRST_ID - 1),
            root(GENERATOR),
            Target::Project(77),
        ]
    );
    // One row per target, capped.
    let many = (0..MAX_REFS + 10)
        .map(|i| format!("https://www.fxhash.xyz/generative/{i}"))
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(refs::references(&many).len(), MAX_REFS);
}

#[test]
fn non_references_and_code_are_ignored() {
    let body = "\
        ::tezos-storage-pointer[]{contract=\"KT1XCoGnfupWk7Sp8536EfrxcP73LmT68Nyr\" path=\"ledger::31707\" data_spec=\"FX-GEN-DATA-002\"}\n\
        ::tezos-storage-pointer[]{contract=\"KT1NkZho1yRkDdQnN4Mz93sDYyY2pPrEHTNs\" path=\"token_metadata::880\"}\n\
        ::tezos-storage-pointer[]{contract=\"0x0944a9414f2385D4D01B4CeC14a0F438BF6401fD\" path=\"token_metadata::1\"}\n\
        ::tezos-storage-pointer[]{contract=\"KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv\" path=\"token_metadata::5\"}\n\
        ::tezos-storage-pointer[]{contract=\"KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv\" path=\"ledger::x\"}\n\
        ::tezos-storage-pointer[]\n\
        ::tezos-storage-pointerx[]{contract=\"KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv\" path=\"ledger::5\"}\n\
        https://www.fxhash.xyz/generative/0x1EF4B99BE3F3787E215C6dfA8B96C87E8b421a00 \
        https://www.fxhash.xyz/gentk/0x8b5C1Ed5E65fCd7d83a8D02a5738B5C2f7A53193-6 \
        https://www.fxhash.xyz/gentk/FX7-12 https://www.fxhash.xyz/gentk/slug/no-iteration\n\
        https://www.fxhash.xyz/u/Haiver https://www.fxhash.xyz/pkh/tz1RcQM4Vgi1fkNTFTJtgBAxf7zqaJCStXt4/articles \
        https://www.fxhash.xyz/article/fauvism https://www.fxhash.xyz/doc/artist/params https://www.fxhash.xyz/ \
        https://www.fxhash.xyz/generative/slug/\n\
        https://api.fxhash.xyz/graphql https://media.fxhash.xyz/w_256/QmbgsPQG3XnptBMYwLKHhQf4bSrNRf2323XNVwFne2oyCe \
        https://bestoffxhash.beehiiv.com/subscribe https://notfxhash.xyz/generative/1 \
        https://www.fxhash.xyz.evil.com/generative/1 xwww.fxhash.xyz/generative/1\n\
        https://objkt.com/asset/hicetnunc/544411 https://objkt.com/collection/KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE \
        https://tzkt.io/KT1Qzv1jLcPAgzT92FsAoS5c91hpYDWNWYss/tokens/3\n\
        Inline `https://www.fxhash.xyz/generative/2` code.\n\
        ```\nhttps://www.fxhash.xyz/generative/3\n```\n\
        ~~~\n::tezos-storage-pointer[]{contract=\"KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv\" path=\"ledger::4\"}\n~~~\n";
    assert_eq!(targets(body), Vec::new());
}

#[test]
fn ids_above_i64_max_are_no_reference_and_never_block_bodies() {
    let max = i64::MAX as u64;
    let poison = format!(
        "https://www.fxhash.xyz/generative/9999999999999999999 \
         https://www.fxhash.xyz/gentk/FX1-9999999999999999999 \
         https://www.fxhash.xyz/gentk/FX0-{over} https://www.fxhash.xyz/gentk/{over} \
         https://objkt.com/asset/fxhash/{over} \
         https://tzkt.io/{GENTK_V3}/tokens/{over}\n\
         ::tezos-storage-pointer[]{{contract=\"KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv\" \
         path=\"ledger::18446744073709551615\"}}\n\
         ::tezos-storage-pointer[]{{contract=\"{GENTK_V3}\" path=\"token_metadata::{over}\"}}\n\
         https://www.fxhash.xyz/generative/{max}\n",
        over = max + 1,
    );
    assert_eq!(
        targets(&poison),
        vec![(Target::Project(max), refs::FORM_FXHASH)]
    );
    // Stored in one batch with a good body: both land.
    let arts = [
        art(50, ALICE, &poison),
        art(51, BOB, "https://www.fxhash.xyz/generative/1"),
    ];
    let store = synthetic(&arts, &[], &[]);
    for a in &arts {
        assert_eq!(
            store.article(a.id).unwrap().body.state,
            seeded_archive::articles::store::FetchState::Ok
        );
    }
    assert_eq!(store.stats().unwrap().refs, 2);
}

#[test]
fn target_keys_round_trip() {
    for t in [
        Target::Project(0),
        slug("a:b"),
        token(GENTK_V3, 42),
        Target::TokenSlug("x-1".into()),
        root(GENERATOR),
        Target::Root {
            scheme: "onchfs",
            cid: ONCHFS.into(),
        },
    ] {
        assert_eq!(Target::from_key(&t.key()), Some(t.clone()), "{t:?}");
    }
    assert_eq!(Target::from_key("token:KT1Unknown:1"), None);
    assert_eq!(Target::from_key("nonsense"), None);
}

/// Articles 10–14 and the catalogue facts the resolution tests use.
fn scenario() -> (Vec<Art>, ArticleStore) {
    let body10 = format!(
        "::tezos-storage-pointer[]{{contract=\"KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv\" path=\"ledger::100\"}}\n\
         ::tezos-storage-pointer[]{{contract=\"{GENTK_V1}\" path=\"token_metadata::5000\"}}\n\
         [tally](https://www.fxhash.xyz/generative/slug/tally!)\n\
         https://www.fxhash.xyz/gentk/slug/garden-monoliths-84\n\
         [live](https://gateway.fxhash2.xyz/ipfs/{GENERATOR}/?fxhash=oo) ![not a generator](ipfs://{LIVE})\n\
         ::tezos-storage-pointer[]{{contract=\"{GENTK_V2}\" path=\"token_metadata::700000\"}}\n"
    );
    let mut hidden = art(
        12,
        BOB,
        "Hidden article. https://www.fxhash.xyz/generative/100",
    );
    hidden.hidden = true;
    let arts = vec![
        art(10, ALICE, &body10),
        art(
            11,
            BOB,
            "https://www.fxhash.xyz/generative/100 https://www.fxhash.xyz/generative/106",
        ),
        hidden,
        art(13, CAROL, "No references here."),
        art(14, CAROL, "https://www.fxhash.xyz/generative/107"),
    ];
    let store = synthetic(&arts, &[], &[]);
    (arts, store)
}

const GEN_URI: &str = "ipfs://QmS9n1UK3WYvMzR5zkaJ3WzS2QzWatVAjve52MBW9J7myi";

fn catalogue_v1(path: &std::path::Path, with_700000: bool, flag_100: bool, with_5000: bool) {
    let mut tokens = vec![(GENTK_V1, 6000, 103, 84)];
    if with_5000 {
        tokens.push((GENTK_V1, 5000, 101, 1));
    }
    if with_700000 {
        tokens.push((GENTK_V2, 700000, 105, 3));
    }
    catalogue(
        path,
        &[
            (100, Some("p100"), flag_100, None),
            (101, Some("p101"), false, None),
            (102, Some("tally!"), false, None),
            (103, Some("garden-monoliths"), false, None),
            // A flagged copy sharing the generator: the visible original wins.
            (99, Some("copy"), true, Some(GEN_URI)),
            (104, Some("p104"), false, Some(GEN_URI)),
            (105, Some("p105"), false, None),
            (106, Some("p106"), true, None),
        ],
        &tokens,
    );
}

#[test]
fn without_a_catalogue_references_are_stored_but_nothing_is_returned() {
    let (_, store) = scenario();
    let s = store.stats().unwrap();
    // #10: project, 2 tokens, slug, gentk slug (generator candidates wait for a catalogue);
    // #11: 2 projects; #12 (hidden): 1; #14: 1.
    assert_eq!(s.refs, 9);
    assert_eq!(s.refs_unresolved, 4, "tokens and slugs need the catalogue");
    assert_eq!((s.ref_articles, s.ref_articles_visible), (4, 0));
    assert_eq!(store.article_refs(10).unwrap(), Vec::new());
    assert_eq!(store.articles_for_project(100, None, 10).unwrap().total, 0);
    // Extraction is idempotent.
    let mut store = store;
    assert_eq!(store.refresh_refs().unwrap(), 0);
}

#[test]
fn tokens_slugs_and_generators_resolve_through_the_catalogue_on_later_runs_too() {
    let (_, mut store) = scenario();
    let path = scratch("refs-catalogue");
    catalogue_v1(&path, false, false, true);
    let archive = open_archive(&path).unwrap();
    let r = store.resolve_refs(&archive).unwrap();
    assert_eq!(r.looked_up, 4, "2 tokens, 1 slug, 1 gentk slug");
    assert_eq!(r.resolved, 3);
    assert_eq!(r.generators, 1, "{LIVE} is no project's generator");
    assert_eq!(r.unresolved, 1, "GENTK v2 #700000 is not in this catalogue");
    let got = store.article_refs(10).unwrap();
    assert_eq!(
        linked(&got),
        vec![
            (RefKind::Project, 100, None, None),
            (RefKind::Token, 101, Some(GENTK_V1), Some(5000)),
            (RefKind::Project, 102, None, None),
            (RefKind::Token, 103, Some(GENTK_V1), Some(6000)),
            (RefKind::Project, 104, None, None),
        ]
    );
    assert_eq!(got[4].form, "generator");
    assert!(got[4].source_text.contains(GENERATOR));
    assert_eq!(
        got[2].source_text,
        "https://www.fxhash.xyz/generative/slug/tally!"
    );
    assert_eq!(store.articles_for_project(105, None, 10).unwrap().total, 0);
    drop(archive);

    // A later catalogue knows the token: resolved on the next run.
    catalogue_v1(&path, true, false, true);
    let archive = open_archive(&path).unwrap();
    let r = store.resolve_refs(&archive).unwrap();
    assert_eq!((r.resolved, r.generators, r.unresolved), (1, 0, 0));
    assert_eq!(
        ids(&store.articles_for_project(105, None, 10).unwrap()),
        vec![10]
    );
    drop(archive);

    // Positive only: a catalogue lacking a token keeps the earlier answer; a newly flagged
    // project disappears from every read.
    catalogue_v1(&path, true, true, false);
    let archive = open_archive(&path).unwrap();
    let r = store.resolve_refs(&archive).unwrap();
    assert_eq!((r.resolved, r.unresolved), (0, 0));
    assert_eq!(
        ids(&store.articles_for_project(101, None, 10).unwrap()),
        vec![10]
    );
    assert_eq!(store.articles_for_project(100, None, 10).unwrap().total, 0);
    assert!(
        store
            .article_refs(10)
            .unwrap()
            .iter()
            .all(|r| r.project_id != 100)
    );
    drop(archive);
    support::remove_db(&path);
}

#[test]
fn hidden_articles_and_flagged_or_unknown_projects_are_never_returned() {
    let (_, mut store) = scenario();
    let path = scratch("refs-hidden");
    catalogue_v1(&path, true, false, true);
    store.resolve_refs(&open_archive(&path).unwrap()).unwrap();
    // #12 is moderated: not listed, refs NotFound.
    assert_eq!(
        ids(&store.articles_for_project(100, None, 10).unwrap()),
        vec![11, 10]
    );
    assert!(matches!(
        store.article_refs(12),
        Err(ArticleStoreError::NotFound { .. })
    ));
    assert!(matches!(
        store.article_refs(999),
        Err(ArticleStoreError::NotFound { .. })
    ));
    // #106 is flagged in the catalogue, #107 unknown to it.
    assert_eq!(
        linked(&store.article_refs(11).unwrap()),
        vec![(RefKind::Project, 100, None, None)]
    );
    assert_eq!(store.articles_for_project(106, None, 10).unwrap().total, 0);
    assert_eq!(store.article_refs(14).unwrap(), Vec::new());
    assert_eq!(store.articles_for_project(107, None, 10).unwrap().total, 0);
    let s = store.stats().unwrap();
    assert_eq!((s.ref_projects_visible, s.ref_projects_hidden), (6, 1));
    assert_eq!(s.ref_articles_visible, 2);
    bad_request(store.articles_for_project(u64::MAX, None, 10));
    bad_request(store.articles_for_project(100, None, 0));
    support::remove_db(&path);
}

#[test]
fn articles_for_project_pages_by_keyset() {
    let arts: Vec<Art> = (30..35)
        .map(|id| art(id, ALICE, "https://www.fxhash.xyz/generative/7"))
        .collect();
    let mut store = synthetic(&arts, &[], &[]);
    let path = scratch("refs-project-pages");
    catalogue(&path, &[(7, Some("seven"), false, None)], &[]);
    store.resolve_refs(&open_archive(&path).unwrap()).unwrap();
    let first = store.articles_for_project(7, None, 2).unwrap();
    assert_eq!((ids(&first), first.total), (vec![34, 33], 5));
    let second = store
        .articles_for_project(7, first.next.as_ref(), 2)
        .unwrap();
    assert_eq!(ids(&second), vec![32, 31]);
    let third = store
        .articles_for_project(7, second.next.as_ref(), 2)
        .unwrap();
    assert_eq!((ids(&third), third.next), (vec![30], None));
    // A cursor only resumes its own listing.
    bad_request(store.articles_for_project(8, first.next.as_ref(), 2));
    bad_request(store.articles_page(None, first.next.as_ref(), 2));
    support::remove_db(&path);
}

#[test]
fn articles_by_holder_credits_escrow_to_sellers_and_pages_by_keyset() {
    let mut hidden = art(23, BOB, "hidden");
    hidden.hidden = true;
    let arts = vec![
        art(20, ALICE, "a"),
        art(21, ALICE, "b"),
        art(22, BOB, "c"),
        hidden,
    ];
    let store = synthetic(
        &arts,
        &[
            (20, BOB, 3),
            (21, BOB, 1),
            (22, BOB, 10),
            (23, BOB, 2),
            (20, MARKETPLACE_V3, 5),
            (21, MARKETPLACE_V3, 4),
        ],
        // #20's escrow is all CAROL's listing; #21's escrow is 2 listed by BOB, 2 unexplained.
        &[(1, 20, CAROL, 5), (2, 21, BOB, 2)],
    );
    let held = |a: &str| -> Vec<(u64, u64, u64)> {
        store
            .articles_by_holder(a, None, None, 10)
            .unwrap()
            .items
            .iter()
            .map(|h| (h.summary.token_id, h.amount, h.listed))
            .collect()
    };
    assert_eq!(held(BOB), vec![(22, 10, 0), (21, 3, 2), (20, 3, 0)]);
    assert_eq!(held(CAROL), vec![(20, 5, 5)]);
    assert_eq!(held(MARKETPLACE_V3), vec![(21, 2, 0)]);
    assert_eq!(held(ALICE), Vec::new());
    let first = store.articles_by_holder(BOB, None, None, 2).unwrap();
    assert_eq!(first.total, 3, "the hidden #23 is not counted");
    assert_eq!(first.items.len(), 2);
    let rest = store
        .articles_by_holder(BOB, None, first.next.as_ref(), 2)
        .unwrap();
    assert_eq!(
        (rest.items[0].summary.token_id, rest.items.len(), rest.next),
        (20, 1, None)
    );
    assert_eq!(rest.items[0].summary.author, ALICE);
    bad_request(store.articles_by_holder(CAROL, None, first.next.as_ref(), 2));
    bad_request(store.articles_by_holder("tz1short", None, None, 2));
    bad_request(store.articles_by_holder(BOB, None, None, 0));
    bad_request(store.articles_by_holder(BOB, None, None, 101));
    let reparsed = ArticleCursor::parse(first.next.as_ref().unwrap().as_str()).unwrap();
    assert_eq!(
        store
            .articles_by_holder(BOB, None, Some(&reparsed), 2)
            .unwrap()
            .items[0]
            .summary
            .token_id,
        20
    );
    // The author listing is unchanged.
    assert_eq!(
        ids(&store.articles_by_author(ALICE, None, None, 10).unwrap()),
        vec![21, 20]
    );
}

#[test]
fn author_and_holder_listings_narrow_to_search_text_with_their_own_cursors() {
    let arts = vec![
        art(20, ALICE, "a"),
        art(21, ALICE, "b"),
        art(22, BOB, "c"),
        art(121, ALICE, "d"),
    ];
    let store = synthetic(
        &arts,
        &[(20, BOB, 1), (21, BOB, 1), (22, BOB, 1), (121, BOB, 1)],
        &[],
    );
    let held = |text: Option<&str>| -> (Vec<u64>, u64) {
        let page = store.articles_by_holder(BOB, text, None, 10).unwrap();
        (
            page.items.iter().map(|h| h.summary.token_id).collect(),
            page.total,
        )
    };
    let authored = |text: Option<&str>| -> (Vec<u64>, u64) {
        let page = store.articles_by_author(ALICE, text, None, 10).unwrap();
        (ids(&page), page.total)
    };
    // Titles are "Article {id}"; a term matches as a prefix, so "21" skips #121.
    assert_eq!(held(Some("21")), (vec![21], 1));
    assert_eq!(authored(Some("21")), (vec![21], 1));
    assert_eq!(held(Some(ALICE)), (vec![121, 21, 20], 3));
    assert_eq!(held(Some("zzz")), (vec![], 0));
    assert_eq!(authored(Some("zzz")), (vec![], 0));
    for blank in [None, Some(""), Some("  ")] {
        assert_eq!(held(blank), (vec![121, 22, 21, 20], 4));
        assert_eq!(authored(blank), (vec![121, 21, 20], 3));
    }
    // A cursor resumes only the listing with the same search text.
    let first = store
        .articles_by_holder(BOB, Some("Article"), None, 2)
        .unwrap();
    let rest = store
        .articles_by_holder(BOB, Some("Article"), first.next.as_ref(), 2)
        .unwrap();
    assert_eq!(
        rest.items
            .iter()
            .map(|h| h.summary.token_id)
            .collect::<Vec<_>>(),
        vec![21, 20]
    );
    bad_request(store.articles_by_holder(BOB, None, first.next.as_ref(), 2));
    let first = store
        .articles_by_author(ALICE, Some("Article"), None, 2)
        .unwrap();
    assert_eq!(
        ids(&store
            .articles_by_author(ALICE, Some("Article"), first.next.as_ref(), 2)
            .unwrap()),
        vec![20]
    );
    bad_request(store.articles_by_author(ALICE, Some("21"), first.next.as_ref(), 2));
    let long = "x".repeat(201);
    bad_request(store.articles_by_holder(BOB, Some(&long), None, 2));
    bad_request(store.articles_by_author(ALICE, Some(&long), None, 2));
}

#[test]
fn references_follow_body_changes_in_the_same_transaction() {
    let a = art(40, ALICE, "https://www.fxhash.xyz/generative/1");
    let mut store = synthetic(std::slice::from_ref(&a), &[], &[]);
    assert_eq!(store.stats().unwrap().refs, 1);
    // A metadata revision names a new body: the old body's references go at once.
    let b = art(
        40,
        ALICE,
        "https://www.fxhash.xyz/generative/2 and /generative/3",
    );
    store
        .apply_snapshot(&support::snapshot(std::slice::from_ref(&b), &[], &[]))
        .unwrap();
    assert_eq!(store.stats().unwrap().refs, 0, "new body not fetched yet");
    store_bodies(&mut store, std::slice::from_ref(&b));
    let path = scratch("refs-body-change");
    catalogue(
        &path,
        &[(1, None, false, None), (2, None, false, None)],
        &[],
    );
    store.resolve_refs(&open_archive(&path).unwrap()).unwrap();
    assert_eq!(
        linked(&store.article_refs(40).unwrap()),
        vec![(RefKind::Project, 2, None, None)]
    );
    support::remove_db(&path);
}

#[test]
fn a_verified_copy_replacing_unverified_bytes_is_extracted_again() {
    let real = art(50, ALICE, "https://www.fxhash.xyz/generative/2");
    let mut store = ArticleStore::open_in_memory().unwrap();
    store
        .apply_snapshot(&support::snapshot(std::slice::from_ref(&real), &[], &[]))
        .unwrap();
    let key = ipfs_ref(&format!(
        "ipfs://{}",
        support::raw_cid(real.body.as_bytes())
    ))
    .unwrap()
    .key;
    let record = |store: &mut ArticleStore, data: &str, verification| {
        store
            .record_objects(&[(
                PendingObject {
                    key: key.clone(),
                    kind: ObjectKind::Body,
                },
                Outcome::Ok {
                    data: data.as_bytes().to_vec(),
                    verification,
                    gateway: support::GW1.into(),
                },
            )])
            .unwrap();
    };
    let path = scratch("refs-reverified");
    catalogue(
        &path,
        &[(1, None, false, None), (2, None, false, None)],
        &[],
    );
    let archive = open_archive(&path).unwrap();
    // Unverified gateway bytes that are not the real body.
    record(
        &mut store,
        "https://www.fxhash.xyz/generative/1",
        Verification::Unverified,
    );
    store.resolve_refs(&archive).unwrap();
    assert_eq!(store.article_refs(50).unwrap()[0].project_id, 1);
    record(&mut store, &real.body, Verification::Sha256);
    store.resolve_refs(&archive).unwrap();
    assert_eq!(
        linked(&store.article_refs(50).unwrap()),
        vec![(RefKind::Project, 2, None, None)]
    );
    drop(archive);
    support::remove_db(&path);
}

#[test]
fn open_archive_is_read_only_and_refuses_missing_or_foreign_files() {
    let missing = scratch("refs-missing");
    assert!(bad_request(open_archive(&missing)).contains("does not exist"));
    let foreign = scratch("refs-foreign");
    drop(ArticleStore::open(&foreign).unwrap());
    assert!(bad_request(open_archive(&foreign)).contains("not a seeded.art catalogue"));
    let path = scratch("refs-readonly");
    catalogue(&path, &[(1, None, false, None)], &[]);
    let conn = open_archive(&path).unwrap();
    assert!(conn.is_readonly("main").unwrap());
    assert!(conn.execute("DELETE FROM projects", []).is_err());
    drop(conn);
    for p in [missing, foreign, path] {
        support::remove_db(&p);
    }
}

#[test]
fn sync_resolves_with_the_catalogue_and_fails_fast_without_a_usable_one() {
    let path = scratch("refs-sync");
    catalogue(
        &path,
        &[
            (0, Some("fxhash-generative-logo"), false, None),
            (44, None, false, None),
            (17146, None, false, None),
        ],
        &[
            (GENTK_V1, 0, 0, 1),
            (GENTK_V1, 8717, 44, 1),
            (GENTK_V1, 8115, 44, 2),
            (GENTK_V1, 8142, 44, 3),
            (GENTK_V2, 1052977, 17146, 1),
            (GENTK_V2, 1052964, 17146, 2),
            (GENTK_V2, 1052953, 17146, 3),
        ],
    );
    let mut h = Harness::new();
    let r = h
        .sync(&SyncOptions {
            concurrency: 2,
            archive: Some(path.clone()),
            ..SyncOptions::default()
        })
        .unwrap();
    let refs = r.refs.unwrap();
    // #0 points at GENTK v1 #0 (and at a foreign contract, not a reference); #1 at six
    // tokens of two projects.
    assert_eq!((refs.resolved, refs.unresolved), (7, 0));
    assert_eq!(
        linked(&h.store.article_refs(0).unwrap()),
        vec![(RefKind::Token, 0, Some(GENTK_V1), Some(0))]
    );
    assert_eq!(h.store.article_refs(1).unwrap().len(), 6);
    assert_eq!(
        ids(&h.store.articles_for_project(44, None, 10).unwrap()),
        vec![1]
    );

    // An interrupted run skips resolution; the next run does it.
    let mut h = Harness::new();
    *h.net.stop_after.lock().unwrap() = Some((4, h.stop.clone()));
    let opts = SyncOptions {
        concurrency: 1,
        archive: Some(path.clone()),
        ..SyncOptions::default()
    };
    let r = h.sync(&opts).unwrap();
    assert!(r.interrupted);
    assert_eq!(r.refs, None);
    *h.net.stop_after.lock().unwrap() = None;
    h.stop.store(false, std::sync::atomic::Ordering::SeqCst);
    let r = h.sync(&opts).unwrap();
    assert_eq!(r.refs.unwrap().unresolved, 0);
    assert_eq!(h.store.article_refs(1).unwrap().len(), 6);

    let mut h = Harness::new();
    let err = h
        .sync(&SyncOptions {
            archive: Some(scratch("refs-sync-missing")),
            ..SyncOptions::default()
        })
        .unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");
    assert!(h.net.requests.lock().unwrap().is_empty(), "no network work");
    support::remove_db(&path);
}

/// `(article, target, position, kind, form, project, contract, token, source)` rows.
fn dump(path: &std::path::Path) -> Vec<Vec<rusqlite::types::Value>> {
    let c = rusqlite::Connection::open(path).unwrap();
    let mut stmt = c
        .prepare("SELECT * FROM article_refs ORDER BY article_id, position")
        .unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |r| (0..n).map(|i| r.get(i)).collect())
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

#[test]
fn migration_from_schema_1_backfills_references_without_network() {
    let path = scratch("refs-migration");
    let mut h = Harness::new();
    h.store = ArticleStore::open(&path).unwrap();
    h.sync_default();
    drop(h);
    let before = dump(&path);
    assert_eq!(before.len(), 7, "#0: 1 token, #1: 6 tokens");
    // Back to exactly a schema 1 file: without the v2 tables and columns, with v1's names.
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch(
            "DROP TABLE article_refs; DROP TABLE ref_projects;
             ALTER TABLE articles DROP COLUMN refs_key;
             ALTER TABLE articles DROP COLUMN refs_rules;
             DROP TABLE names;
             CREATE TABLE names (address TEXT PRIMARY KEY, name TEXT NOT NULL,
               last_level INTEGER NOT NULL) WITHOUT ROWID;
             PRAGMA user_version = 1;",
        )
        .unwrap();
    }
    // The web cannot read it until migrated, and says why.
    match ArticleStore::open_read_only(&path) {
        Err(ArticleStoreError::Unavailable(m)) => assert!(m.contains("schema 1"), "{m}"),
        other => panic!("expected Unavailable, got {:?}", other.map(|_| ())),
    }
    // Opening read-write migrates and backfills from stored bodies; no fetcher exists here.
    let mut store = ArticleStore::open(&path).unwrap();
    assert_eq!(store.refresh_refs().unwrap(), 0, "already backfilled");
    drop(store);
    assert_eq!(dump(&path), before);
    let ro = ArticleStore::open_read_only(&path).unwrap();
    assert_eq!(ro.stats().unwrap().refs, 7);
    drop(ro);
    support::remove_db(&path);
}
