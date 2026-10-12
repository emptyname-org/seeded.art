//! Marketplace reads: escrow listings per seller, and fxhash buy offers per buyer, token, holder
//! and id. Additive to [`Store`]; every read joins an open [`Store::read_snapshot`].
//!
//! Only visible tokens and projects count: flagged (hidden) projects and tokens missing from
//! the index are excluded from lists, and a direct [`Store::offer`] lookup of one is NotFound.
//! Lists show live offers only; [`Store::offer`] also returns ended ones (`active = false`).

use rusqlite::{Connection, Row, ToSql};
use serde::Serialize;
use serde_json::Value;

use super::{
    Cursor, Db, KEY_NAMES, KeysetPage, Store, StoreError, check_limit, get_opt_u64, get_u64,
    is_contract, key_params, listing_id, not_found, require_address, sql_id,
};

/// v6 -> v7, additive: listing prices, buy offers, resumable big-map passes, block times, the
/// turn order of sync jobs, and listing removals not yet final (`escrow_removed`, so a reorg
/// that drops a removal can be undone).
/// Existing escrow listings have no price, so every escrow pass is reset to a full re-read
/// (the next `seeded-index escrow`, about a minute) that stores it.
pub(crate) const SCHEMA_V7: &str = "
ALTER TABLE escrow_listings ADD COLUMN price INTEGER CHECK (price IS NULL OR price >= 0);
CREATE INDEX escrow_listings_seller ON escrow_listings (seller, level DESC, escrow, listing);
UPDATE escrow_state SET synced_level = NULL, pass_level = NULL, pass_full = NULL, cursor_id = 0;
CREATE TABLE market_offers (
  marketplace TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('token', 'collection')),
  offer_id INTEGER NOT NULL CHECK (offer_id >= 0),
  buyer TEXT NOT NULL,
  price INTEGER NOT NULL CHECK (price > 0),
  remaining INTEGER NOT NULL CHECK (remaining >= 0),
  token_contract TEXT,
  token_id INTEGER CHECK (token_id IS NULL OR token_id >= 0),
  project_id INTEGER CHECK (project_id IS NULL OR project_id >= 0),
  created_level INTEGER NOT NULL CHECK (created_level >= 0),
  level INTEGER NOT NULL CHECK (level >= created_level),
  active INTEGER NOT NULL CHECK (active IN (0, 1)),
  PRIMARY KEY (marketplace, kind, offer_id),
  CHECK ((kind = 'token') = (token_contract IS NOT NULL AND token_id IS NOT NULL)),
  CHECK ((kind = 'collection') = (project_id IS NOT NULL)),
  CHECK (active = 0 OR remaining > 0)
) STRICT, WITHOUT ROWID;
CREATE INDEX market_offers_buyer ON market_offers (buyer, created_level DESC, kind, offer_id)
  WHERE active = 1;
CREATE INDEX market_offers_token ON market_offers (token_contract, token_id)
  WHERE active = 1 AND kind = 'token';
CREATE INDEX market_offers_project ON market_offers (project_id)
  WHERE active = 1 AND kind = 'collection';
CREATE TABLE bigmap_state (
  contract TEXT NOT NULL,
  path TEXT NOT NULL,
  synced_level INTEGER,
  synced_at TEXT,
  pass_level INTEGER,
  pass_full INTEGER,
  cursor_id INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (contract, path)
) STRICT, WITHOUT ROWID;
CREATE TABLE block_times (
  level INTEGER PRIMARY KEY CHECK (level >= 0),
  time TEXT NOT NULL
) STRICT;
CREATE TABLE sync_rotation (
  job TEXT PRIMARY KEY,
  served INTEGER NOT NULL
) STRICT, WITHOUT ROWID;
CREATE TABLE escrow_removed (
  escrow TEXT NOT NULL,
  listing TEXT NOT NULL,
  level INTEGER NOT NULL,
  PRIMARY KEY (escrow, listing)
) STRICT, WITHOUT ROWID;
";

pub(crate) fn migrate_v7(tx: &Connection) -> Result<(), StoreError> {
    tx.execute_batch(SCHEMA_V7).db()
}

/// Most offers [`Store::offers_for_token`] returns (highest price first). The largest live
/// case on 2026-10-05 was 14 (9 collection + 5 token offers).
pub const MAX_TOKEN_OFFERS: u32 = 500;

/// Most tokens [`Store::held_project_tokens`] returns for one collection offer.
pub const MAX_HELD_CHOICES: u32 = 500;

/// A marketplace contract the archive reads. The fxhash ones match `ingest::escrow::ESCROWS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Marketplace {
    pub contract: &'static str,
    pub name: &'static str,
    /// "fxhash" or "objkt".
    pub family: &'static str,
    pub version: u32,
}

pub const FXHASH_V2: Marketplace = Marketplace {
    contract: "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u",
    name: "fxhash marketplace v2",
    family: "fxhash",
    version: 2,
};

pub const FXHASH_V1: Marketplace = Marketplace {
    contract: "KT1Xo5B7PNBAeynZPmca4bRh6LQow4og1Zb9",
    name: "fxhash marketplace v1",
    family: "fxhash",
    version: 1,
};

pub const OBJKT_V1: Marketplace = Marketplace {
    contract: "KT1FvqJwEDWb1Gwc55Jd1jjTHRVWbYKUUpyq",
    name: "objkt.com marketplace (old)",
    family: "objkt",
    version: 1,
};

pub const MARKETPLACES: [Marketplace; 3] = [FXHASH_V2, FXHASH_V1, OBJKT_V1];

impl Marketplace {
    pub fn by_contract(contract: &str) -> Option<Marketplace> {
        MARKETPLACES
            .iter()
            .copied()
            .find(|m| m.contract == contract)
    }
}

/// Token offers name one token; collection offers any token of one project.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OfferKind {
    Token,
    Collection,
}

impl OfferKind {
    pub fn as_str(self) -> &'static str {
        match self {
            OfferKind::Token => "token",
            OfferKind::Collection => "collection",
        }
    }

    pub fn parse(s: &str) -> Option<OfferKind> {
        match s {
            "token" => Some(OfferKind::Token),
            "collection" => Some(OfferKind::Collection),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectRef {
    pub id: u64,
    pub title: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TokenRef {
    pub contract: String,
    pub token_id: u64,
    pub iteration: u32,
    pub project: ProjectRef,
}

/// A record's thumbnail: a local preview path, else a content URI (thumbnail before display).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ImageRef {
    pub preview: Option<String>,
    pub uri: Option<String>,
}

/// A token an address holds; `listed` when it sits in a marketplace escrow for that address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeldToken {
    pub contract: String,
    pub token_id: u64,
    pub iteration: u64,
    pub listed: bool,
    pub image: ImageRef,
}

/// Records on an address's Listings and offers page, per view.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct MarketCounts {
    pub listings: u64,
    pub offers_made: u64,
    pub offers_received: u64,
}

impl MarketCounts {
    pub fn total(&self) -> u64 {
        self.listings + self.offers_made + self.offers_received
    }
}

/// A live listing whose token sits in the marketplace's escrow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Listing {
    pub marketplace: Marketplace,
    /// Key of the marketplace's listing big map (v1 calls it an offer id).
    pub listing_id: u64,
    pub seller: String,
    pub token: TokenRef,
    /// The listed token's thumbnail.
    pub image: ImageRef,
    /// `None` only until the first escrow pass after the v7 upgrade has re-read the listing.
    pub price_mutez: Option<u64>,
    /// The listing key's last change, which is its creation: any update ends a listing
    /// (v2 removes it; v1 and objkt set the amount to 0).
    pub created_level: u64,
    /// Block time of `created_level` (UTC, ISO 8601), once resolved.
    pub created_at: Option<String>,
}

/// An fxhash v2 buy offer: tez held by the marketplace until cancelled or accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Offer {
    pub marketplace: Marketplace,
    pub kind: OfferKind,
    /// Key of `offers` or `collection_offers`; the two counters overlap, so ids are unique
    /// only together with `kind`.
    pub offer_id: u64,
    pub buyer: String,
    /// Paid per token.
    pub price_mutez: u64,
    /// Tokens still wanted: 1 for a token offer; a collection offer's `amount`. For an ended
    /// offer, the last indexed value (0 once exhausted).
    pub remaining: u64,
    /// Tez held for this offer: `price_mutez * remaining` while active, else 0.
    pub locked_mutez: u64,
    /// The token of a token offer; `None` for collection offers.
    pub token: Option<TokenRef>,
    /// The token's project, or the collection offer's project.
    pub project: ProjectRef,
    /// The token's thumbnail for a token offer, else the project's.
    pub image: ImageRef,
    pub created_level: u64,
    pub created_at: Option<String>,
    /// Level of the last change the index applied.
    pub updated_level: u64,
    /// Live when last indexed; ended offers (cancelled, accepted, exhausted) are `false`.
    pub active: bool,
}

fn internal(why: impl Into<String>) -> StoreError {
    StoreError::Internal(why.into())
}

fn marketplace(contract: &str) -> Result<Marketplace, StoreError> {
    Marketplace::by_contract(contract)
        .ok_or_else(|| internal(format!("unknown marketplace {contract}")))
}

const LISTING_COLS: &str = "l.escrow, l.listing, l.seller, l.contract, l.token_id, t.iteration, \
     p.id, p.title, l.price, l.level, (SELECT time FROM block_times WHERE level = l.level), \
     t.preview, coalesce(t.thumbnail_uri, t.display_uri)";
/// Listings of visible tokens only. Block times are a column subquery, so counts skip them.
const LISTING_FROM: &str = "FROM escrow_listings l \
     JOIN tokens t ON t.contract = l.contract AND t.token_id = l.token_id \
     JOIN projects p ON p.id = t.project_id AND p.hidden = 0";
/// Listings of the seller `:addr`.
const SELLER_LISTINGS: &str = "l.seller = :addr";

struct ListingRow {
    escrow: String,
    listing: String,
    seller: String,
    contract: String,
    token_id: u64,
    iteration: u32,
    project_id: u64,
    title: String,
    price: Option<u64>,
    level: u64,
    time: Option<String>,
    image: ImageRef,
}

fn listing_row(r: &Row<'_>) -> rusqlite::Result<ListingRow> {
    Ok(ListingRow {
        escrow: r.get(0)?,
        listing: r.get(1)?,
        seller: r.get(2)?,
        contract: r.get(3)?,
        token_id: get_u64(r, 4)?,
        iteration: r.get(5)?,
        project_id: get_u64(r, 6)?,
        title: r.get(7)?,
        price: get_opt_u64(r, 8)?,
        level: get_u64(r, 9)?,
        time: r.get(10)?,
        image: ImageRef {
            preview: r.get(11)?,
            uri: r.get(12)?,
        },
    })
}

fn finish_listing(row: ListingRow) -> Result<Listing, StoreError> {
    let listing_id = row
        .listing
        .parse::<u64>()
        .map_err(|_| internal(format!("listing key {:?} is not a number", row.listing)))?;
    Ok(Listing {
        marketplace: marketplace(&row.escrow)?,
        listing_id,
        seller: row.seller,
        token: TokenRef {
            contract: row.contract,
            token_id: row.token_id,
            iteration: row.iteration,
            project: ProjectRef {
                id: row.project_id,
                title: row.title,
            },
        },
        image: row.image,
        price_mutez: row.price,
        created_level: row.level,
        created_at: row.time,
    })
}

const OFFER_COLS: &str = "o.marketplace, o.kind, o.offer_id, o.buyer, o.price, o.remaining, \
     o.token_contract, o.token_id, t.iteration, p.id, p.title, o.created_level, \
     (SELECT time FROM block_times WHERE level = o.created_level), o.level, o.active, \
     CASE WHEN o.kind = 'token' THEN t.preview ELSE p.preview END, \
     CASE WHEN o.kind = 'token' THEN coalesce(t.thumbnail_uri, t.display_uri) \
       ELSE coalesce(p.thumbnail_uri, p.display_uri) END";
/// Offers on visible tokens or projects only; a token offer whose token is not in the index
/// has no project and drops out.
const OFFER_FROM: &str = "FROM market_offers o \
     LEFT JOIN tokens t ON o.kind = 'token' AND t.contract = o.token_contract \
       AND t.token_id = o.token_id \
     JOIN projects p ON p.id = coalesce(t.project_id, o.project_id) AND p.hidden = 0";
/// Live offers made by `:addr`.
const BUYER_OFFERS: &str = "o.buyer = :addr AND o.active = 1";
/// Live offers `:addr` can accept: see [`Store::offers_received`].
const RECEIVED_OFFERS: &str = "o.active = 1 AND o.buyer <> :addr AND \
     ((o.kind = 'token' AND t.holder_address = :addr) OR (o.kind = 'collection' \
     AND EXISTS (SELECT 1 FROM tokens h WHERE h.holder_address = :addr \
     AND h.project_id = o.project_id)))";
const OFFER_ORDER: &str = "o.created_level DESC, o.kind, o.offer_id";
const OFFER_KEYSET: &str = "(o.created_level < :k0 OR (o.created_level = :k0 \
     AND (o.kind, o.offer_id) > (:k1, :k2)))";

struct OfferRow {
    marketplace: String,
    kind: String,
    offer_id: u64,
    buyer: String,
    price: u64,
    remaining: u64,
    token_contract: Option<String>,
    token_id: Option<u64>,
    iteration: Option<u32>,
    project_id: u64,
    title: String,
    created_level: u64,
    time: Option<String>,
    level: u64,
    active: bool,
    image: ImageRef,
}

fn offer_row(r: &Row<'_>) -> rusqlite::Result<OfferRow> {
    Ok(OfferRow {
        marketplace: r.get(0)?,
        kind: r.get(1)?,
        offer_id: get_u64(r, 2)?,
        buyer: r.get(3)?,
        price: get_u64(r, 4)?,
        remaining: get_u64(r, 5)?,
        token_contract: r.get(6)?,
        token_id: get_opt_u64(r, 7)?,
        iteration: r.get(8)?,
        project_id: get_u64(r, 9)?,
        title: r.get(10)?,
        created_level: get_u64(r, 11)?,
        time: r.get(12)?,
        level: get_u64(r, 13)?,
        active: r.get::<_, i64>(14)? == 1,
        image: ImageRef {
            preview: r.get(15)?,
            uri: r.get(16)?,
        },
    })
}

fn finish_offer(row: OfferRow) -> Result<Offer, StoreError> {
    let kind = OfferKind::parse(&row.kind).ok_or_else(|| internal("unknown offer kind"))?;
    let project = ProjectRef {
        id: row.project_id,
        title: row.title,
    };
    let token = match (kind, row.token_contract, row.token_id, row.iteration) {
        (OfferKind::Token, Some(contract), Some(token_id), Some(iteration)) => Some(TokenRef {
            contract,
            token_id,
            iteration,
            project: project.clone(),
        }),
        (OfferKind::Token, ..) => return Err(internal("token offer without its token")),
        (OfferKind::Collection, ..) => None,
    };
    let locked_mutez = if row.active {
        row.price
            .checked_mul(row.remaining)
            .filter(|n| i64::try_from(*n).is_ok())
            .ok_or_else(|| internal("locked amount overflows"))?
    } else {
        0
    };
    Ok(Offer {
        marketplace: marketplace(&row.marketplace)?,
        kind,
        offer_id: row.offer_id,
        buyer: row.buyer,
        price_mutez: row.price,
        remaining: row.remaining,
        locked_mutez,
        token,
        project,
        image: row.image,
        created_level: row.created_level,
        created_at: row.time,
        updated_level: row.level,
        active: row.active,
    })
}

fn count(c: &Connection, sql: &str, params: &[(&str, &dyn ToSql)]) -> Result<u64, StoreError> {
    let n: i64 = c
        .prepare_cached(sql)
        .db()?
        .query_row(params, |r| r.get(0))
        .db()?;
    u64::try_from(n).map_err(|_| internal("negative count"))
}

/// Up to `limit` rows of `select … from … [AND keyset] ORDER BY order`, plus whether more follow.
#[allow(clippy::too_many_arguments)] // one call shape shared by every keyset listing here
fn keyset_rows<R>(
    c: &Connection,
    cols: &str,
    from_where: &str,
    params: &[(&str, &dyn ToSql)],
    keyset: &str,
    order: &str,
    key: Option<&[Value]>,
    limit: u32,
    row: impl FnMut(&Row<'_>) -> rusqlite::Result<R>,
) -> Result<(Vec<R>, bool), StoreError> {
    let kp = key.map(key_params);
    let mut all = params.to_vec();
    let mut sql = format!("SELECT {cols} {from_where}");
    if let Some(kp) = &kp {
        sql.push_str(" AND ");
        sql.push_str(keyset);
        for (i, v) in kp.iter().enumerate() {
            all.push((KEY_NAMES[i], v.as_ref()));
        }
    }
    let fetch = i64::from(limit) + 1;
    all.push((":limit", &fetch));
    sql.push_str(&format!(" ORDER BY {order} LIMIT :limit"));
    let mut rows = c
        .prepare_cached(&sql)
        .db()?
        .query_map(all.as_slice(), row)
        .db()?
        .collect::<rusqlite::Result<Vec<R>>>()
        .db()?;
    let more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    Ok((rows, more))
}

fn offer_key(o: &Offer) -> Vec<Value> {
    vec![
        Value::from(o.created_level),
        Value::from(o.kind.as_str()),
        Value::from(o.offer_id),
    ]
}

impl Store {
    /// Live escrow listings by `seller` on visible tokens, newest first (level, then
    /// marketplace contract and listing key as text). Includes the old objkt marketplace;
    /// check [`Marketplace::family`] before offering an action.
    pub fn listings_by_seller(
        &self,
        seller: &str,
        after: Option<&Cursor>,
        limit: u32,
    ) -> Result<KeysetPage<Listing>, StoreError> {
        check_limit(limit)?;
        require_address(seller)?;
        let listing = listing_id(&["listings_by_seller", seller]);
        let key = after.map(|c| c.key(&listing, 3)).transpose()?;
        self.read(|c| {
            let from_where = format!("{LISTING_FROM} WHERE {SELLER_LISTINGS}");
            let params: [(&str, &dyn ToSql); 1] = [(":addr", &seller)];
            let total = count(c, &format!("SELECT count(*) {from_where}"), &params)?;
            let (rows, more) = keyset_rows(
                c,
                LISTING_COLS,
                &from_where,
                &params,
                "(l.level < :k0 OR (l.level = :k0 AND (l.escrow, l.listing) > (:k1, :k2)))",
                "l.level DESC, l.escrow, l.listing",
                key.as_deref(),
                limit,
                listing_row,
            )?;
            let next = if more {
                rows.last().map(|r| {
                    vec![
                        Value::from(r.level),
                        Value::from(r.escrow.clone()),
                        Value::from(r.listing.clone()),
                    ]
                })
            } else {
                None
            };
            let items = rows
                .into_iter()
                .map(finish_listing)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(KeysetPage {
                items,
                next: next.map(|k| Cursor::encode(&listing, k)),
                total,
            })
        })
    }

    /// Live offers made by `buyer` on visible tokens or projects, newest first.
    pub fn offers_by_buyer(
        &self,
        buyer: &str,
        after: Option<&Cursor>,
        limit: u32,
    ) -> Result<KeysetPage<Offer>, StoreError> {
        check_limit(limit)?;
        require_address(buyer)?;
        let listing = listing_id(&["offers_by_buyer", buyer]);
        let key = after.map(|c| c.key(&listing, 3)).transpose()?;
        self.read(|c| {
            let from_where = format!("{OFFER_FROM} WHERE {BUYER_OFFERS}");
            let params: [(&str, &dyn ToSql); 1] = [(":addr", &buyer)];
            offers_keyset(c, &from_where, &params, &listing, key.as_deref(), limit)
        })
    }

    /// Live offers `holder` can accept now: token offers on visible tokens it holds (its
    /// escrow listings included: cancel the listing first, in the same batch) and collection
    /// offers on visible projects where it holds a token. Its own offers are left out.
    pub fn offers_received(
        &self,
        holder: &str,
        after: Option<&Cursor>,
        limit: u32,
    ) -> Result<KeysetPage<Offer>, StoreError> {
        check_limit(limit)?;
        require_address(holder)?;
        let listing = listing_id(&["offers_received", holder]);
        let key = after.map(|c| c.key(&listing, 3)).transpose()?;
        self.read(|c| {
            let from_where = format!("{OFFER_FROM} WHERE {RECEIVED_OFFERS}");
            let params: [(&str, &dyn ToSql); 1] = [(":addr", &holder)];
            offers_keyset(c, &from_where, &params, &listing, key.as_deref(), limit)
        })
    }

    /// Records on `address`'s Listings and offers page: its listings, the offers it made and
    /// the offers it received, counted as [`Self::listings_by_seller`],
    /// [`Self::offers_by_buyer`] and [`Self::offers_received`] count them.
    pub fn market_counts(&self, address: &str) -> Result<MarketCounts, StoreError> {
        require_address(address)?;
        self.read(|c| {
            let params: [(&str, &dyn ToSql); 1] = [(":addr", &address)];
            let offers = |filter: &str| {
                count(
                    c,
                    &format!("SELECT count(*) {OFFER_FROM} WHERE {filter}"),
                    &params,
                )
            };
            Ok(MarketCounts {
                listings: count(
                    c,
                    &format!("SELECT count(*) {LISTING_FROM} WHERE {SELLER_LISTINGS}"),
                    &params,
                )?,
                offers_made: offers(BUYER_OFFERS)?,
                offers_received: offers(RECEIVED_OFFERS)?,
            })
        })
    }

    /// Whether the index still shows `escrow`'s listing `id` as live.
    pub fn listing_live(&self, escrow: &str, id: u64) -> Result<bool, StoreError> {
        self.read(|c| {
            c.prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM escrow_listings WHERE escrow = ?1 AND listing = ?2)",
            )
            .db()?
            .query_row(rusqlite::params![escrow, id.to_string()], |r| r.get(0))
            .db()
        })
    }

    /// Buyer and remaining amount of an offer the index still shows as active.
    pub fn offer_live(
        &self,
        marketplace: &str,
        kind: OfferKind,
        id: u64,
    ) -> Result<Option<(String, u64)>, StoreError> {
        let Some(id) = sql_id(id) else {
            return Ok(None);
        };
        self.read(|c| {
            c.prepare_cached(
                "SELECT buyer, remaining FROM market_offers \
                 WHERE marketplace = ?1 AND kind = ?2 AND offer_id = ?3 AND active = 1",
            )
            .db()?
            .query_row(rusqlite::params![marketplace, kind.as_str(), id], |r| {
                Ok((r.get(0)?, get_u64(r, 1)?))
            })
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                e => Err(e),
            })
            .db()
        })
    }

    /// Whether the index still shows `holder` holding the token.
    pub fn holds(&self, holder: &str, contract: &str, token_id: u64) -> Result<bool, StoreError> {
        let Some(id) = sql_id(token_id) else {
            return Ok(false);
        };
        self.read(|c| {
            c.prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM tokens \
                 WHERE contract = ?1 AND token_id = ?2 AND holder_address = ?3)",
            )
            .db()?
            .query_row(rusqlite::params![contract, id, holder], |r| r.get(0))
            .db()
        })
    }

    /// Tokens of `project_id` that `holder` holds, to accept a collection offer with: held
    /// directly before listed in escrow, then by iteration; at most [`MAX_HELD_CHOICES`].
    pub fn held_project_tokens(
        &self,
        holder: &str,
        project_id: u64,
    ) -> Result<Vec<HeldToken>, StoreError> {
        require_address(holder)?;
        let Some(project) = sql_id(project_id) else {
            return Ok(Vec::new());
        };
        self.read(|c| {
            c.prepare_cached(
                "SELECT contract, token_id, iteration, owner_address IS NOT holder_address, \
                 preview, coalesce(thumbnail_uri, display_uri) \
                 FROM tokens WHERE holder_address = ?1 AND project_id = ?2 \
                 ORDER BY owner_address IS NOT holder_address, iteration LIMIT ?3",
            )
            .db()?
            .query_map(
                rusqlite::params![holder, project, i64::from(MAX_HELD_CHOICES)],
                |r| {
                    Ok(HeldToken {
                        contract: r.get(0)?,
                        token_id: r.get::<_, i64>(1)? as u64,
                        iteration: r.get::<_, i64>(2)? as u64,
                        listed: r.get(3)?,
                        image: ImageRef {
                            preview: r.get(4)?,
                            uri: r.get(5)?,
                        },
                    })
                },
            )
            .db()?
            .collect::<rusqlite::Result<_>>()
            .db()
        })
    }

    /// Live offers that can buy this token: its token offers plus the collection offers on its
    /// project, highest price first, at most [`MAX_TOKEN_OFFERS`]. NotFound unless the token
    /// is visible.
    pub fn offers_for_token(
        &self,
        contract: &str,
        token_id: u64,
    ) -> Result<Vec<Offer>, StoreError> {
        let key = format!("{contract}/{token_id}");
        let id = sql_id(token_id)
            .filter(|_| is_contract(contract))
            .ok_or_else(|| not_found("token", key.clone()))?;
        self.read(|c| {
            let project: Option<i64> = c
                .prepare_cached(
                    "SELECT t.project_id FROM tokens t JOIN projects p ON p.id = t.project_id \
                     AND p.hidden = 0 WHERE t.contract = ?1 AND t.token_id = ?2",
                )
                .db()?
                .query_row(rusqlite::params![contract, id], |r| r.get(0))
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
                .db()?;
            let Some(project) = project else {
                return Err(not_found("token", key));
            };
            let sql = format!(
                "SELECT {OFFER_COLS} {OFFER_FROM} WHERE o.active = 1 AND ((o.kind = 'token' \
                 AND o.token_contract = :contract AND o.token_id = :id) OR (o.kind = 'collection' \
                 AND o.project_id = :project)) \
                 ORDER BY o.price DESC, o.created_level, o.kind, o.offer_id LIMIT :limit"
            );
            let limit = i64::from(MAX_TOKEN_OFFERS);
            let rows = c
                .prepare_cached(&sql)
                .db()?
                .query_map(
                    rusqlite::named_params! {
                        ":contract": contract, ":id": id, ":project": project, ":limit": limit
                    },
                    offer_row,
                )
                .db()?
                .collect::<rusqlite::Result<Vec<_>>>()
                .db()?;
            rows.into_iter().map(finish_offer).collect()
        })
    }

    /// One offer as last indexed, live or ended, for re-checking before a transaction is
    /// built (the chain itself must still be read; see ENTRYPOINTS.md). NotFound when never
    /// indexed, or when its token or project is not visible.
    pub fn offer(
        &self,
        marketplace: &str,
        kind: OfferKind,
        offer_id: u64,
    ) -> Result<Offer, StoreError> {
        let key = format!("{marketplace}/{}/{offer_id}", kind.as_str());
        let id = sql_id(offer_id)
            .filter(|_| is_contract(marketplace))
            .ok_or_else(|| not_found("offer", key.clone()))?;
        self.read(|c| {
            let sql = format!(
                "SELECT {OFFER_COLS} {OFFER_FROM} WHERE o.marketplace = ?1 AND o.kind = ?2 \
                 AND o.offer_id = ?3"
            );
            let row = c
                .prepare_cached(&sql)
                .db()?
                .query_row(rusqlite::params![marketplace, kind.as_str(), id], offer_row)
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    e => Err(e),
                })
                .db()?;
            row.map_or_else(|| Err(not_found("offer", key)), finish_offer)
        })
    }
}

fn offers_keyset(
    c: &Connection,
    from_where: &str,
    params: &[(&str, &dyn ToSql)],
    listing: &str,
    key: Option<&[Value]>,
    limit: u32,
) -> Result<KeysetPage<Offer>, StoreError> {
    let total = count(c, &format!("SELECT count(*) {from_where}"), params)?;
    let (rows, more) = keyset_rows(
        c,
        OFFER_COLS,
        from_where,
        params,
        OFFER_KEYSET,
        OFFER_ORDER,
        key,
        limit,
        offer_row,
    )?;
    let items = rows
        .into_iter()
        .map(finish_offer)
        .collect::<Result<Vec<_>, _>>()?;
    let next = if more {
        items.last().map(offer_key)
    } else {
        None
    };
    Ok(KeysetPage {
        items,
        next: next.map(|k| Cursor::encode(listing, k)),
        total,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marketplaces_match_the_escrow_registry() {
        for e in &crate::ingest::escrow::ESCROWS {
            let m = Marketplace::by_contract(e.address).expect("escrow without marketplace");
            assert_eq!(m.name, e.name);
        }
        assert_eq!(MARKETPLACES.len(), crate::ingest::escrow::ESCROWS.len());
    }

    #[test]
    fn offer_kinds_round_trip() {
        for k in [OfferKind::Token, OfferKind::Collection] {
            assert_eq!(OfferKind::parse(k.as_str()), Some(k));
        }
        assert_eq!(OfferKind::parse("listing"), None);
    }
}
