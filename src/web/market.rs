use super::{AppState, views::Meta};
use crate::{
    auth::{AuthError, AuthIdentity, TrustedClientIp},
    store::{
        Cursor,
        market::{MarketCounts, OfferKind},
    },
    trading::{
        Action, Kind, Overlay, Token, TradingError, TradingState, action_json,
        types::{TOKENS, V2, tez},
    },
};
use askama::Template;
use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State, rejection::QueryRejection},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub fn trading_router() -> Router<AppState> {
    Router::new()
        .route("/profile/{address}/market", get(page))
        .route("/api/market/prepare", post(prepare))
        .route("/api/market/check", post(check))
        .route("/api/market/confirm", post(confirm))
        .route("/api/market/submitted", post(submitted))
        .route("/api/market/discard", post(discard))
        .layer(DefaultBodyLimit::max(24 * 1024))
}
fn identity(state: &AppState, headers: &HeaderMap, post: bool) -> Result<AuthIdentity, Response> {
    let auth = state
        .auth
        .as_ref()
        .ok_or_else(|| AuthError::Unavailable.into_response())?;
    if post {
        auth.require(headers).map_err(IntoResponse::into_response)
    } else {
        auth.identity(headers)
            .map_err(IntoResponse::into_response)?
            .ok_or_else(|| AuthError::Unauthenticated.into_response())
    }
}
fn trading(state: &AppState) -> Result<Arc<TradingState>, Response> {
    state
        .trading
        .clone()
        .ok_or_else(|| error(TradingError::Unavailable))
}
fn error(e: TradingError) -> Response {
    let code = match e {
        TradingError::Pending => "Pending",
        TradingError::Unavailable => "Unavailable",
        TradingError::FeeAuditUnavailable => "FeeAuditUnavailable",
        TradingError::FeeUnconfigured | TradingError::FeeRecipient => "Unconfigured",
        TradingError::FeeEvasion => "FeeEvasion",
        TradingError::Busy => "Busy",
        TradingError::RateLimited => "RateLimited",
        TradingError::SigningPending(_) => "SigningPending",
        TradingError::ReceiptCapacity(_) => "ReceiptCapacity",
        TradingError::Expired => "Expired",
        TradingError::Unauthorized => "Unauthorized",
        TradingError::NotHeld { .. } => "NotHeld",
        TradingError::Failed => "Failed",
        TradingError::Unverified => "Unverified",
        _ => "Permanent",
    };
    let status = match e {
        TradingError::Invalid => StatusCode::BAD_REQUEST,
        TradingError::Unauthorized | TradingError::NotHeld { .. } => StatusCode::FORBIDDEN,
        TradingError::Unavailable
        | TradingError::FeeAuditUnavailable
        | TradingError::FeeUnconfigured
        | TradingError::FeeRecipient => StatusCode::SERVICE_UNAVAILABLE,
        TradingError::Busy | TradingError::RateLimited | TradingError::ReceiptCapacity(_) => {
            StatusCode::TOO_MANY_REQUESTS
        }
        _ => StatusCode::CONFLICT,
    };
    let retry = match e {
        TradingError::SigningPending(n) => Some(n),
        TradingError::ReceiptCapacity(n) => Some(n),
        TradingError::RateLimited => Some(60),
        TradingError::Busy => Some(1),
        _ => None,
    };
    let mut body = json!({"error":e.to_string(),"code":code,"retry_after_seconds":retry});
    if matches!(e, TradingError::FeeEvasion) {
        body["acceptance_applied"] = json!(true);
    }
    let mut r = (status, Json(body)).into_response();
    if let Some(n) = retry {
        if let Ok(value) = n.to_string().parse() {
            r.headers_mut().insert(header::RETRY_AFTER, value);
        }
    }
    r.headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    r
}
fn browser_error(status: StatusCode, title: &str, message: &str) -> Response {
    let html = super::views::render_error(super::views::ErrorPage {
        status: status.as_u16(),
        title: title.into(),
        message: message.into(),
    })
    .unwrap_or_else(|_| "Page unavailable".into());
    private((status, Html(html)))
}
fn client(value: Option<Extension<TrustedClientIp>>) -> Result<std::net::IpAddr, Response> {
    value
        .map(|Extension(ip)| ip.0)
        .ok_or_else(|| error(TradingError::Unavailable))
}
fn private(value: impl IntoResponse) -> Response {
    let mut r = value.into_response();
    r.headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    r
}
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct MarketQuery {
    #[serde(default)]
    view: String,
    after: Option<String>,
    token_contract: Option<String>,
    token_id: Option<u64>,
}
#[derive(Serialize)]
struct Row {
    title: String,
    url: String,
    /// The NFT's image (listings, token offers) or the project's (collection offers).
    thumb: Option<String>,
    thumb_url: String,
    kind: String,
    id: u64,
    amount: String,
    amount_mutez: Option<String>,
    remaining: u64,
    date: String,
    counterparty: String,
    address: String,
    action: String,
    accept: bool,
    contract: String,
    token_id: String,
    /// Held tokens a received collection offer can be accepted with.
    held: Vec<HeldChoice>,
    supported: bool,
}
#[derive(Serialize)]
struct HeldChoice {
    /// `contract/token_id`.
    value: String,
    label: String,
    selected: bool,
    /// The NFT's thumbnail (the project's when it has none) and page, shown while selected.
    thumb: Option<String>,
    url: String,
}
#[derive(Template)]
#[template(path = "market.html")]
struct MarketPage {
    meta: Meta,
    header: super::views::ProfileHeader,
    address: String,
    name: String,
    view: String,
    rows: Vec<Row>,
    total: u64,
    counts: MarketCounts,
    pages: super::views::PageInfo,
    contracts: Vec<&'static str>,
    coverage: Vec<String>,
    trading_enabled: bool,
    accepts_enabled: bool,
    accepts_unavailable_reason: &'static str,
    accepts_unavailable_message: String,
    platform_fee_percent: String,
    platform_fee_bps: u16,
}
async fn page(
    State(state): State<AppState>,
    Path(address): Path<String>,
    request: super::agent::Request,
    headers: HeaderMap,
    query: Result<Query<MarketQuery>, QueryRejection>,
) -> Response {
    let auth = match state.auth.as_ref() {
        Some(auth) => auth,
        None => {
            return browser_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Address sign-in unavailable",
                "Try again later.",
            );
        }
    };
    let who = match auth.identity(&headers) {
        Ok(Some(who)) => who,
        Ok(None) | Err(AuthError::Unauthenticated) | Err(AuthError::Expired) => {
            return if request.json {
                super::agent::error(StatusCode::UNAUTHORIZED)
            } else {
                private(Redirect::to("/auth"))
            };
        }
        Err(_) => {
            return browser_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Address sign-in unavailable",
                "Try again later.",
            );
        }
    };
    if who.address() != address {
        return browser_error(
            StatusCode::FORBIDDEN,
            "This is another address",
            "Open Listings and offers from your connected address profile.",
        );
    }
    let query = match query {
        Ok(Query(query)) => query,
        Err(_) => {
            return browser_error(
                StatusCode::BAD_REQUEST,
                "Invalid market view",
                "Open Listings and offers again from your profile.",
            );
        }
    };
    let view = if query.view.is_empty() {
        "listings".to_owned()
    } else {
        query.view
    };
    if !["listings", "offers", "received"].contains(&view.as_str()) {
        return browser_error(
            StatusCode::BAD_REQUEST,
            "Invalid market view",
            "Open Listings and offers again from your profile.",
        );
    }
    let after = match query.after.map(|s| Cursor::parse(&s)).transpose() {
        Ok(v) => v,
        Err(_) => {
            return browser_error(
                StatusCode::BAD_REQUEST,
                "Invalid market view",
                "Open Listings and offers again from your profile.",
            );
        }
    };
    if query
        .token_contract
        .as_ref()
        .is_some_and(|c| !TOKENS.contains(&c.as_str()))
        || query.token_contract.is_some() != query.token_id.is_some()
    {
        return browser_error(
            StatusCode::BAD_REQUEST,
            "Invalid market view",
            "Open Listings and offers again from your profile.",
        );
    }
    let json_requested = request.json;
    let fee = state
        .trading
        .as_ref()
        .map(|s| s.platform_fee().clone())
        .unwrap_or_default();
    let trading_enabled = state.trading.is_some();
    let audit_unavailable = state
        .trading
        .as_ref()
        .is_some_and(|trading| trading.fee_audit_unavailable());
    let accepts_enabled = trading_enabled && fee.recipient().is_ok() && !audit_unavailable;
    let accepts_unavailable_reason = if audit_unavailable {
        "fee_audit_unavailable"
    } else if trading_enabled && fee.recipient().is_err() {
        "fee_recipient_unconfigured"
    } else if !trading_enabled {
        "trading_unavailable"
    } else {
        ""
    };
    let accepts_unavailable_message = match accepts_unavailable_reason {
        "fee_audit_unavailable" => {
            "Acceptances unavailable: platform fee audit could not be opened."
        }
        "fee_recipient_unconfigured" => {
            "Acceptances unavailable: no platform fee recipient configured."
        }
        "trading_unavailable" => "Trading is unavailable.",
        _ => "",
    }
    .to_owned();
    let platform_fee_percent = format!("{:.2}", f64::from(fee.bps) / 100.0)
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned();
    let content_origin = state.content_origin.clone();
    let overlay = state.trading.as_ref().map(|t| t.overlay(&address));
    let result = state
        .query(move |store| {
            let pending = pending(store, &address, overlay.as_ref())?;
            let mut rows = Vec::new();
            let mut skipped = 0;
            let total;
            let next;
            if view == "listings" {
                let page = store.listings_by_seller(&address, after.as_ref(), 50)?;
                total = page.total;
                next = page.next;
                for l in page.items {
                    if pending
                        .listings
                        .contains(&(l.marketplace.contract.to_owned(), l.listing_id))
                    {
                        skipped += 1;
                        continue;
                    }
                    let action = Action::Cancel {
                        marketplace: l.marketplace.contract.into(),
                        kind: Kind::Listing,
                        id: l.listing_id,
                    };
                    let url = format!("/token/{}/{}", l.token.contract, l.token.token_id);
                    rows.push(Row {
                        title: format!("{} #{}", l.token.project.title, l.token.iteration),
                        thumb: super::views::image_preview(
                            l.image.preview.as_deref(),
                            l.image.uri.as_deref(),
                            content_origin.as_deref(),
                        ),
                        thumb_url: url.clone(),
                        url,
                        kind: l.marketplace.name.into(),
                        id: l.listing_id,
                        amount: l
                            .price_mutez
                            .map(tez)
                            .unwrap_or_else(|| "Not indexed".into()),
                        amount_mutez: l.price_mutez.map(|p| p.to_string()),
                        remaining: 1,
                        date: l
                            .created_at
                            .unwrap_or_else(|| format!("Block {}", l.created_level)),
                        counterparty: String::new(),
                        address: address.clone(),
                        action: action_json(&action),
                        accept: false,
                        contract: l.token.contract,
                        token_id: l.token.token_id.to_string(),
                        held: Vec::new(),
                        supported: l.marketplace.family == "fxhash",
                    });
                }
            } else {
                let page = if view == "offers" {
                    store.offers_by_buyer(&address, after.as_ref(), 50)?
                } else {
                    store.offers_received(&address, after.as_ref(), 50)?
                };
                total = page.total;
                next = page.next;
                for offer in page.items {
                    let kind = if offer.kind == OfferKind::Token {
                        Kind::Token
                    } else {
                        Kind::Collection
                    };
                    // Submitted operations show as done until the index has them.
                    let gone = if view == "offers" {
                        pending.offers_made.contains(&(kind, offer.offer_id))
                    } else {
                        pending.offers_received.contains(&(kind, offer.offer_id))
                            || offer.token.as_ref().is_some_and(|t| {
                                pending.tokens.contains(&Token {
                                    contract: t.contract.clone(),
                                    token_id: t.token_id,
                                })
                            })
                    };
                    let remaining = offer.remaining.saturating_sub(
                        pending.taken.get(&offer.offer_id).copied().unwrap_or(0),
                    );
                    if gone || remaining == 0 {
                        skipped += 1;
                        continue;
                    }
                    let token = offer.token.as_ref().map(|t| Token {
                        contract: t.contract.clone(),
                        token_id: t.token_id,
                    });
                    let mut selected = token.or_else(|| {
                        query
                            .token_contract
                            .as_ref()
                            .zip(query.token_id)
                            .map(|(c, id)| Token {
                                contract: c.clone(),
                                token_id: id,
                            })
                    });
                    // A received collection offer is accepted with a token the address holds.
                    let mut held = if kind == Kind::Collection && view == "received" {
                        store.held_project_tokens(&address, offer.project.id)?
                    } else {
                        Vec::new()
                    };
                    if kind == Kind::Collection && view == "received" {
                        held.retain(|t| {
                            !pending.tokens.contains(&Token {
                                contract: t.contract.clone(),
                                token_id: t.token_id,
                            })
                        });
                        if held.is_empty() {
                            skipped += 1;
                            continue;
                        }
                        if !selected.as_ref().is_some_and(|s| {
                            held.iter()
                                .any(|t| t.contract == s.contract && t.token_id == s.token_id)
                        }) {
                            selected = None;
                        }
                    }
                    if selected.is_none() {
                        selected = held.first().map(|t| Token {
                            contract: t.contract.clone(),
                            token_id: t.token_id,
                        });
                    }
                    let project_thumb = super::views::image_preview(
                        offer.image.preview.as_deref(),
                        offer.image.uri.as_deref(),
                        content_origin.as_deref(),
                    );
                    let held: Vec<HeldChoice> = held
                        .into_iter()
                        .map(|t| HeldChoice {
                            selected: selected.as_ref().is_some_and(|s| {
                                s.contract == t.contract && s.token_id == t.token_id
                            }),
                            thumb: super::views::image_preview(
                                t.image.preview.as_deref(),
                                t.image.uri.as_deref(),
                                content_origin.as_deref(),
                            )
                            .or_else(|| project_thumb.clone()),
                            url: format!("/token/{}/{}", t.contract, t.token_id),
                            value: format!("{}/{}", t.contract, t.token_id),
                            label: format!(
                                "#{} · ID {}{}",
                                t.iteration,
                                t.token_id,
                                if t.listed { " · listed" } else { "" }
                            ),
                        })
                        .collect();
                    let accept = view == "received";
                    let action = if accept {
                        json!({"action":"accept","kind":kind,"id":offer.offer_id}).to_string()
                    } else {
                        action_json(&Action::Cancel {
                            marketplace: V2.into(),
                            kind,
                            id: offer.offer_id,
                        })
                    };
                    let project_url = format!("/project/{}", offer.project.id);
                    // A received collection offer shows the held NFT chosen to accept it with.
                    let (thumb, thumb_url) = match held.iter().find(|c| c.selected) {
                        Some(choice) => (choice.thumb.clone(), choice.url.clone()),
                        None => (
                            project_thumb,
                            offer.token.as_ref().map_or_else(
                                || project_url.clone(),
                                |t| format!("/token/{}/{}", t.contract, t.token_id),
                            ),
                        ),
                    };
                    rows.push(Row {
                        thumb,
                        thumb_url,
                        title: offer.project.title,
                        url: project_url,
                        kind: if kind == Kind::Token {
                            "Token offer"
                        } else {
                            "Collection offer"
                        }
                        .into(),
                        id: offer.offer_id,
                        amount: tez(offer.price_mutez),
                        amount_mutez: Some(offer.price_mutez.to_string()),
                        remaining,
                        date: offer
                            .created_at
                            .unwrap_or_else(|| format!("Block {}", offer.created_level)),
                        counterparty: String::new(),
                        address: offer.buyer,
                        action,
                        accept,
                        contract: selected
                            .as_ref()
                            .map(|t| t.contract.clone())
                            .unwrap_or_else(|| TOKENS[2].into()),
                        token_id: selected.map(|t| t.token_id.to_string()).unwrap_or_default(),
                        held,
                        supported: true,
                    });
                }
            }
            let addresses: Vec<_> = rows
                .iter()
                .map(|r| r.address.as_str())
                .chain(std::iter::once(address.as_str()))
                .collect();
            let names = store.display_names(&addresses)?;
            for row in &mut rows {
                row.counterparty = names
                    .get(&row.address)
                    .cloned()
                    .unwrap_or_else(|| row.address.clone());
            }
            let snapshot = store.snapshot()?;
            let coverage = market_coverage(store, &view)?;
            let mut first = format!("/profile/{address}/market?view={view}");
            if let Some((contract, id)) = query.token_contract.as_ref().zip(query.token_id) {
                first.push_str(&format!("&token_contract={contract}&token_id={id}"));
            }
            let (mut profile, _) = super::displayed_profile(store, &address)?;
            profile.held = profile.held.saturating_sub(pending.tokens.len() as u64);
            let counts = pending.counts(store.market_counts(&address)?);
            let total = total.saturating_sub(skipped);
            let page = MarketPage {
                meta: Meta::new("Listings and offers", "", &snapshot),
                header: super::views::profile_header(
                    &profile,
                    content_origin.as_deref(),
                    Some(counts.total()),
                    "market",
                ),
                name: names
                    .get(&address)
                    .cloned()
                    .unwrap_or_else(|| address.clone()),
                pages: super::views::PageInfo {
                    page: 1,
                    previous: after.as_ref().map(|_| first.clone()),
                    next: next.map(|c| super::with_cursor(first.clone(), "after", Some(c.as_str()))),
                },
                address,
                view,
                rows,
                total,
                counts,
                contracts: TOKENS.to_vec(),
                coverage,
                trading_enabled,
                accepts_enabled,
                accepts_unavailable_reason,
                accepts_unavailable_message,
                platform_fee_percent,
                platform_fee_bps: fee.bps,
            };
            let items: Vec<_> = page.rows.iter().map(|row| {
                let mut value=serde_json::json!(row);
                value["token_id"]=row.token_id.parse::<u64>().map_or(serde_json::Value::Null, |id| json!(id));
                value["action"]=serde_json::from_str(&row.action).unwrap_or(serde_json::Value::Null);
                value["record_key"]=serde_json::json!(format!("{}:{}:{}",value["action"]["marketplace"].as_str().unwrap_or(V2),value["action"]["kind"].as_str().unwrap_or_default(),row.id));
                value
            }).collect();
            let data=serde_json::json!({"address":page.address,"name":page.name,"view":page.view,"items":items,"total":page.total,"coverage":page.coverage,"trading_enabled":page.trading_enabled,"accepts_enabled":page.accepts_enabled,"accepts_unavailable_reason":page.accepts_unavailable_reason,"accepts_unavailable_message":page.accepts_unavailable_message,"platform_fee_bps":page.platform_fee_bps,"pagination":{"first":first,"next":page.pages.next}});
            request.respond("market",data,None,None,|| page.render())
        })
        .await;
    match result {
        Ok(response) => match state.auth.as_ref().map(|auth| auth.is_current(&who)) {
            Some(Ok(true)) => private(response),
            _ => {
                if json_requested {
                    super::agent::error(StatusCode::UNAUTHORIZED)
                } else {
                    private(Redirect::to("/auth"))
                }
            }
        },
        Err(e) => e.into_response(),
    }
}
/// Effects of the address's submitted operations that the index does not show yet: each is
/// checked against the index, so it stops applying once the sync has it.
#[derive(Default)]
pub(super) struct Pending {
    listings: HashSet<(String, u64)>,
    offers_made: HashSet<(Kind, u64)>,
    offers_received: HashSet<(Kind, u64)>,
    pub(super) tokens: HashSet<Token>,
    taken: HashMap<u64, u64>,
    collections_done: u64,
}

impl Pending {
    pub(super) fn counts(&self, counts: MarketCounts) -> MarketCounts {
        MarketCounts {
            listings: counts.listings.saturating_sub(self.listings.len() as u64),
            offers_made: counts
                .offers_made
                .saturating_sub(self.offers_made.len() as u64),
            offers_received: counts
                .offers_received
                .saturating_sub(self.offers_received.len() as u64 + self.collections_done),
        }
    }
}

pub(super) fn pending(
    store: &crate::store::Store,
    address: &str,
    overlay: Option<&Overlay>,
) -> Result<Pending, crate::store::StoreError> {
    let mut pending = Pending::default();
    let Some(overlay) = overlay.filter(|o| !o.is_empty()) else {
        return Ok(pending);
    };
    for record in &overlay.records {
        let offer_kind = match record.kind {
            Kind::Listing => {
                if store.listing_live(&record.marketplace, record.id)? {
                    pending
                        .listings
                        .insert((record.marketplace.clone(), record.id));
                }
                continue;
            }
            Kind::Token => OfferKind::Token,
            Kind::Collection => OfferKind::Collection,
        };
        if let Some((buyer, _)) = store.offer_live(&record.marketplace, offer_kind, record.id)? {
            if buyer == address {
                pending.offers_made.insert((record.kind, record.id));
            } else {
                pending.offers_received.insert((record.kind, record.id));
            }
        }
    }
    for token in &overlay.tokens {
        if store.holds(address, &token.contract, token.token_id)? {
            pending.tokens.insert(token.clone());
        }
    }
    for (id, (reviewed, taken)) in &overlay.taken {
        // Still the amount the review saw: the index has not applied the acceptance yet.
        if let Some((_, remaining)) = store.offer_live(V2, OfferKind::Collection, *id)? {
            if remaining == *reviewed {
                pending.taken.insert(*id, *taken);
                if *taken >= remaining {
                    pending.collections_done += 1;
                }
            }
        }
    }
    Ok(pending)
}

fn market_coverage(
    store: &crate::store::Store,
    view: &str,
) -> Result<Vec<String>, crate::store::StoreError> {
    use rusqlite::OptionalExtension;
    type Coverage = Option<(Option<i64>, Option<String>)>;
    let read = |sql: &str,
                parameters: &[&dyn rusqlite::ToSql]|
     -> Result<Coverage, crate::store::StoreError> {
        store
            .conn()
            .query_row(sql, parameters, |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()
            .map_err(|_| crate::store::StoreError::Internal("Coverage lookup failed".into()))
    };
    let label = |name: &str, value: Coverage| match value {
        Some((Some(level), Some(at))) => {
            format!("{name}: indexed through block {level}; completed {at}")
        }
        _ => format!("{name}: no completed indexing pass"),
    };
    let mut coverage = Vec::new();
    if view == "listings" {
        for source in crate::store::market::MARKETPLACES {
            let value = read(
                "SELECT synced_level,synced_at FROM escrow_state WHERE escrow=?1",
                &[&source.contract],
            )?;
            coverage.push(label(source.name, value));
        }
    } else {
        for (path, name) in [
            ("offers", "Token offers"),
            ("collection_offers", "Collection offers"),
        ] {
            let value = read(
                "SELECT synced_level,synced_at FROM bigmap_state WHERE contract=?1 AND path=?2",
                &[&V2, &path],
            )?;
            coverage.push(label(name, value));
        }
    }
    Ok(coverage)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Prepare {
    actions: Vec<Action>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ticket {
    ticket: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Confirmation {
    ticket: String,
    hash: String,
}
async fn prepare(
    State(state): State<AppState>,
    headers: HeaderMap,
    trusted: Option<Extension<TrustedClientIp>>,
    Json(body): Json<Prepare>,
) -> Response {
    let client = match client(trusted) {
        Ok(ip) => ip,
        Err(r) => return r,
    };
    let who = match identity(&state, &headers, true) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let trading = match trading(&state) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if body.actions.iter().any(|a| a.validate().is_err()) {
        return error(TradingError::Invalid);
    }
    let mut hint = None;
    if let [Action::Accept { token, .. }] = body.actions.as_slice() {
        let token = token.clone();
        let actor = who.address().to_owned();
        let listing=state.query(move|store|{
            use rusqlite::OptionalExtension;
            store.conn().query_row("SELECT escrow,listing FROM escrow_listings WHERE contract=?1 AND token_id=?2 AND seller=?3 LIMIT 1",rusqlite::params![token.contract,token.token_id as i64,actor],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).optional().map_err(|_|crate::store::StoreError::Internal("Listing lookup failed".into()))
        }).await;
        match listing {
            Ok(Some((marketplace, id))) => {
                let Ok(id) = id.parse::<u64>() else {
                    return error(TradingError::Invalid);
                };
                if marketplace == V2 || marketplace == crate::trading::types::V1 {
                    hint = Some(Action::Cancel {
                        marketplace,
                        kind: Kind::Listing,
                        id,
                    });
                }
            }
            Ok(None) => {}
            Err(_) => return error(TradingError::Unavailable),
        }
    }
    match trading
        .prepare_from(who.address(), who.session_id(), body.actions, hint, client)
        .await
    {
        Ok(review) => {
            if let Err(r) = same_session(&state, &headers, &who) {
                return r;
            }
            let addresses: Vec<String> = review
                .items
                .iter()
                .map(|i| i.counterparty.clone())
                .collect();
            let names = state
                .query(move |s| {
                    s.display_names(&addresses.iter().map(String::as_str).collect::<Vec<_>>())
                })
                .await
                .unwrap_or_default();
            if let Err(r) = same_session(&state, &headers, &who) {
                return r;
            }
            private(Json(json!({"review":review,"names":names})))
        }
        Err(e) => error(e),
    }
}
fn same_session(state: &AppState, headers: &HeaderMap, old: &AuthIdentity) -> Result<(), Response> {
    let current = identity(state, headers, true)?;
    if current.session_id() != old.session_id() {
        return Err(AuthError::Unauthenticated.into_response());
    }
    Ok(())
}
async fn check(
    State(state): State<AppState>,
    headers: HeaderMap,
    trusted: Option<Extension<TrustedClientIp>>,
    Json(body): Json<Ticket>,
) -> Response {
    let client = match client(trusted) {
        Ok(ip) => ip,
        Err(r) => return r,
    };
    let who = match identity(&state, &headers, true) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let trading = match trading(&state) {
        Ok(v) => v,
        Err(r) => return r,
    };
    match trading
        .check_from(who.address(), who.session_id(), &body.ticket, client)
        .await
    {
        Ok(ready) => {
            if let Err(r) = same_session(&state, &headers, &who) {
                return r;
            }
            private(Json(ready))
        }
        Err(e) => error(e),
    }
}
async fn confirm(
    State(state): State<AppState>,
    headers: HeaderMap,
    trusted: Option<Extension<TrustedClientIp>>,
    Json(body): Json<Confirmation>,
) -> Response {
    let client = match client(trusted) {
        Ok(ip) => ip,
        Err(r) => return r,
    };
    let who = match identity(&state, &headers, true) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let trading = match trading(&state) {
        Ok(v) => v,
        Err(r) => return r,
    };
    match trading
        .confirm_from(
            who.address(),
            who.session_id(),
            &body.ticket,
            &body.hash,
            client,
        )
        .await
    {
        Ok(()) => private(Json(json!({"confirmed":true}))),
        Err(TradingError::Pending) => private(Json(json!({"confirmed":false,"pending":true}))),
        Err(e) => error(e),
    }
}
/// The page reports the operation its signing app submitted: its effects show at once.
async fn submitted(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Confirmation>,
) -> Response {
    let who = match identity(&state, &headers, true) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let trading = match trading(&state) {
        Ok(v) => v,
        Err(r) => return r,
    };
    match trading.submitted(who.address(), who.session_id(), &body.ticket, &body.hash) {
        Ok(()) => private(Json(json!({"submitted":true}))),
        Err(e) => error(e),
    }
}
async fn discard(
    State(state): State<AppState>,
    headers: HeaderMap,
    trusted: Option<Extension<TrustedClientIp>>,
    Json(body): Json<Ticket>,
) -> Response {
    let _client = match client(trusted) {
        Ok(ip) => ip,
        Err(r) => return r,
    };
    let who = match identity(&state, &headers, true) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let trading = match trading(&state) {
        Ok(v) => v,
        Err(r) => return r,
    };
    match trading.discard_review(who.address(), who.session_id(), &body.ticket) {
        Ok(()) => private(Json(json!({"discarded":true}))),
        Err(e) => error(e),
    }
}

#[cfg(test)]
mod receipt_capacity_tests {
    use super::*;

    #[tokio::test]
    async fn receipt_capacity_error_has_distinct_retry_response() {
        let response = error(TradingError::ReceiptCapacity(121));
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER], "121");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["code"], "ReceiptCapacity");
        assert_eq!(value["retry_after_seconds"], 121);
        assert!(value.get("acceptance_applied").is_none());
    }

    #[tokio::test]
    async fn only_fee_evasion_reports_acceptance_as_applied() {
        let response = error(TradingError::FeeEvasion);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["acceptance_applied"], true);

        let response = error(TradingError::Unavailable);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(value.get("acceptance_applied").is_none());
    }
}
