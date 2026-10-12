use std::{
    collections::BTreeMap,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use seeded_archive::{
    import::import_fixtures,
    model::{AddressKind, Owner, Profile, Project, Snapshot, Token},
    store::{Page, Store},
    web::views::{self, PageInfo, ProjectPage, TokenPage},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
const CONTRACT: &str = "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr";
const MARKET: &str = "KT1Market";
const SELLER: &str = "tz1Seller";

fn fixture() -> (Project, Token, Snapshot) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let directory = root.join(format!(
        "tmp/ownership-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let mut store = Store::open(&directory.join("state.sqlite")).unwrap();
    import_fixtures(&mut store, &root.join("assets/fixtures")).unwrap();
    let result = (
        store.project(29437).unwrap(),
        store.token(CONTRACT, 133459).unwrap(),
        store.snapshot().unwrap(),
    );
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
    result
}

fn pages(project: Project, token: Token, snapshot: Snapshot) -> Vec<String> {
    let detail = views::render_token(TokenPage {
        history: String::new(),
        instructions: String::new(),
        project: project.clone(),
        token: token.clone(),
        snapshot: snapshot.clone(),
        shares: vec![],
        previous: None,
        next: None,
        content_origin: None,
    })
    .unwrap();
    let mut second = token.clone();
    second.iteration += 1;
    second.token_id += 1;
    let project_page = || {
        views::render_project(ProjectPage {
            articles: String::new(),
            metadata: seeded_archive::model::ProjectMeta {
                id: project.id,
                published: None,
                mint_opens_at: None,
                tags: vec![],
                generative_uri: project.artifact_uri.clone(),
                author_address: None,
                artist_addresses: vec![],
            },
            release: String::new(),
            history: String::new(),
            query: String::new(),
            trait_name: String::new(),
            trait_value: String::new(),
            project: project.clone(),
            tokens: Page {
                items: vec![token.clone(), second.clone()],
                total: 2,
                offset: 0,
                limit: 24,
            },
            snapshot: snapshot.clone(),
            content_origin: None,
            pagination: PageInfo {
                page: 1,
                previous: None,
                next: None,
            },
        })
        .unwrap()
    };
    vec![detail, project_page()]
}

fn profile(address: &str, name: &str) -> Profile {
    Profile {
        address: address.into(),
        name: Some(name.into()),
        name_source: None,
        bio: None,
        website: None,
        links: vec![],
        avatar_uri: None,
        kind: AddressKind::Account,
        project_count: 0,
        held: 0,
        listed: 0,
    }
}

/// A profile page; collected views list `tokens` under their project titles.
fn profile_page(
    profile: Profile,
    view: &str,
    tokens: Vec<Token>,
    project_titles: BTreeMap<u64, String>,
    snapshot: &Snapshot,
) -> String {
    let total = tokens.len() as u64;
    views::render_profile(views::ProfilePage {
        articles: String::new(),
        profile,
        projects: Page {
            items: vec![],
            total: 0,
            offset: 0,
            limit: 24,
        },
        tokens: Page {
            items: tokens,
            total,
            offset: 0,
            limit: 24,
        },
        project_titles,
        project_artists: BTreeMap::new(),
        snapshot: snapshot.clone(),
        view: view.into(),
        query: String::new(),
        trait_name: String::new(),
        trait_value: String::new(),
        pagination: PageInfo {
            page: 1,
            previous: None,
            next: None,
        },
        content_origin: None,
        own_profile: false,
        market_count: 0,
    })
    .unwrap()
}

#[test]
fn escrow_surfaces_link_to_the_seller_and_keep_the_ledger_source() {
    let (project, mut token, snapshot) = fixture();
    let owner = token.owner.as_mut().unwrap();
    owner.owner = Owner {
        address: MARKET.into(),
        alias: Some("Marketplace".into()),
    };
    owner.escrow = Some("fxhash marketplace v2".into());
    owner.listed_by = Some(Owner {
        address: SELLER.into(),
        alias: Some("<Seller>".into()),
    });
    owner.source = "https://example.org/ownership-snapshot".into();
    let collector = profile_page(
        Profile {
            held: 1,
            listed: 1,
            ..profile(SELLER, "<Seller>")
        },
        "collected",
        vec![token.clone()],
        BTreeMap::from([(project.id, project.title.clone())]),
        &snapshot,
    );
    assert!(collector.contains("Includes 1 listing."));
    let mut rendered = pages(project, token, snapshot);
    assert!(!rendered[0].contains("Ledger holder"));
    assert!(rendered[0].contains("class=\"owner-address\">tz1Seller</code>"));

    // The token page shows only "Owner", the seller's name and address.
    let detail = &rendered[0];
    assert!(detail.contains("<span>Owner</span>"));
    for removed in ["Listed by", "Listed on", "Snapshot 20"] {
        assert!(!detail.contains(removed), "{removed}");
    }
    assert!(detail.contains("&#60;Seller&#62;") && detail.contains("href=\"/profile/tz1Seller\""));
    rendered.remove(0);
    rendered.push(collector);
    for html in rendered {
        assert!(html.contains("Owner: "));
        assert!(!html.contains("Listed by") && !html.contains("Listed on"));
        assert!(html.contains("&#60;Seller&#62;"));
        assert!(html.contains("href=\"/profile/tz1Seller\""));
        assert!(!html.contains("href=\"/profile/KT1Market\""));
        assert!(!html.contains("<Seller>"));
    }
}

#[test]
fn unresolved_escrow_and_burned_editions_never_link_to_fake_collectors() {
    let (project, mut token, snapshot) = fixture();
    let owner = token.owner.as_mut().unwrap();
    owner.owner.address = MARKET.into();
    owner.escrow = Some("Marketplace".into());
    owner.listed_by = None;
    for html in pages(project.clone(), token.clone(), snapshot.clone()) {
        assert!(html.contains("Seller not yet resolved"));
        assert!(!html.contains(&format!("href=\"/profile/{MARKET}\"")));
    }
    token.owner.as_mut().unwrap().burned = true;
    for html in pages(project, token, snapshot) {
        assert!(html.contains("Burned"));
        assert!(!html.contains(&format!("href=\"/profile/{MARKET}\"")));
        assert!(!html.contains("Seller not yet resolved"));
    }
}

#[test]
fn keeper_identity_uses_the_address_or_fixture_id_instead_of_a_website_claim() {
    let (_, _, snapshot) = fixture();
    let render = |profile| profile_page(profile, "projects", vec![], BTreeMap::new(), &snapshot);
    let impostor = Profile {
        website: Some("https://emptyname.org".into()),
        ..profile("tz1Other", "Another artist")
    };
    assert!(!render(impostor).contains("Keeper of this archive"));
    let html = render(profile(
        "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL",
        "Another artist",
    ));
    assert!(!html.contains("Keeper of this archive"));
    assert!(!html.contains("Co-creator of Arboreal."));
    assert!(!html.contains("An archive by"));
}

#[test]
fn artist_directory_pluralizes_artists_and_project_counts() {
    use seeded_archive::model::ArtistSummary;
    let (_, _, snapshot) = fixture();
    for total in [0, 1, 2] {
        let html = views::render_artists(views::ArtistsPage {
            avatars: Default::default(),
            article_counts: Default::default(),
            content_origin: None,
            artists: Page {
                items: (0..total)
                    .map(|id| ArtistSummary {
                        id: id.to_string(),
                        name: format!("Artist {id}"),
                        project_count: total as u32,
                    })
                    .collect(),
                total,
                offset: 0,
                limit: 24,
            },
            snapshot: snapshot.clone(),
            pagination: PageInfo {
                page: 1,
                previous: None,
                next: None,
            },
        })
        .unwrap();
        let suffix = if total == 1 { "" } else { "s" };
        assert!(html.contains(&format!("<span>{total} artist{suffix}</span>")));
        if total > 0 {
            assert!(html.contains(&format!(
                "<span class=\"person-list-detail\">{total} project{suffix}"
            )));
        }
    }
}

#[test]
fn artist_collected_pane_reports_total_marketplace_listings_only_when_present() {
    let (project, token, snapshot) = fixture();
    for (view, listed, expected) in [
        ("collected", 0, None),
        ("collected", 1, Some("Includes 1 listing.")),
        ("collected", 8, Some("Includes 8 listings.")),
        ("projects", 8, None),
    ] {
        let html = profile_page(
            Profile {
                held: 12,
                listed,
                ..profile(SELLER, "An artist")
            },
            view,
            vec![token.clone()],
            BTreeMap::from([(project.id, project.title.clone())]),
            &snapshot,
        );
        if let Some(expected) = expected {
            assert!(html.contains(expected));
        } else {
            assert!(!html.contains("Includes"));
        }
    }
}

#[test]
fn unclassified_contract_holders_have_holdings_without_wallet_or_escrow_claims() {
    let (project, mut token, snapshot) = fixture();
    let owner = token.owner.as_mut().unwrap();
    owner.owner = Owner {
        address: CONTRACT.into(),
        alias: Some("<Contract>".into()),
    };
    owner.escrow = None;
    owner.listed_by = None;
    owner.burned = false;
    let collector = profile_page(
        Profile {
            kind: AddressKind::Contract,
            held: 1,
            ..profile(CONTRACT, "<Contract>")
        },
        "collected",
        vec![token.clone()],
        BTreeMap::from([(project.id, project.title.clone())]),
        &snapshot,
    );
    assert!(collector.contains("<p class=\"page-intro\">Contract holder</p>"));
    assert!(collector.contains("Collected <span>1</span>"));
    assert!(!collector.contains("held directly or listed by"));
    assert!(!collector.contains("Collector profile"));
    assert!(!collector.contains("this wallet"));
    let mut rendered = pages(project, token, snapshot);
    assert!(rendered[0].contains("<span>Owner</span>"));
    rendered.push(collector);
    for html in rendered {
        assert!(html.contains("Owner"));
        assert!(html.contains("&#60;Contract&#62;"));
        assert!(html.contains(&format!("href=\"/profile/{CONTRACT}\"")));
        assert!(!html.contains("Listed by"));
        assert!(!html.contains("Listed on"));
        assert!(!html.contains("Seller not yet resolved"));
    }
}
