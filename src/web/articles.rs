use super::{
    AppState, WebError,
    agent::Request,
    search::SearchScope,
    views::{Meta, PageInfo, date, encode},
    with_cursor,
};
use crate::{
    articles::store::{ArticleCursor, ArticleRef, ArticleStore, ArticleStoreError, RefKind},
    markup,
    model::ProjectSummary,
    store::{Store, StoreError},
};
use askama::Template;
use axum::{
    Router,
    extract::{Path, Query, State, rejection::QueryRejection},
    response::Response,
    routing::get,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Default, Deserialize)]
struct Search {
    #[serde(default)]
    q: String,
    after: Option<String>,
    author: Option<String>,
    holder: Option<String>,
    project: Option<u64>,
}

#[derive(Template)]
#[template(path = "articles.html")]
struct ArticlesView {
    embedded: bool,
    region: &'static str,
    coverage: Vec<String>,
    meta: Meta,
    cards: Vec<Card>,
    total: u64,
    pages: PageInfo,
    scope_label: &'static str,
    scope_name: String,
    scope_url: String,
}
struct Card {
    id: u64,
    title: String,
    description: String,
    author: String,
    address: String,
    date: String,
    image: String,
    editions: u64,
    holding: String,
}

pub(super) enum Scope {
    Author(String),
    Holder(String),
    Project(u64),
    /// Global search: title, author or tag words.
    Text(String),
}

impl Scope {
    fn url(&self) -> String {
        match self {
            Self::Author(address) => format!("/articles?author={}", encode(address)),
            Self::Holder(address) => format!("/articles?holder={}", encode(address)),
            Self::Project(id) => format!("/articles?project={id}"),
            Self::Text(text) => format!("/search?q={}&scope=articles", encode(text)),
        }
    }

    fn label(&self, store: &Store) -> Result<(&'static str, String, String), StoreError> {
        match self {
            Self::Author(address) | Self::Holder(address) => Ok((
                if matches!(self, Self::Author(_)) {
                    "by"
                } else {
                    "held by"
                },
                store
                    .display_names(&[address])?
                    .remove(address)
                    .unwrap_or_else(|| address.clone()),
                format!("/profile/{}", encode(address)),
            )),
            Self::Project(id) => Ok(("about", store.project(*id)?.title, format!("/project/{id}"))),
            Self::Text(text) => Ok(("matching", text.clone(), self.url())),
        }
    }
}

struct CardRequest<'a> {
    scope: Option<&'a Scope>,
    /// Search text applied within an author or holder scope.
    text: Option<&'a str>,
    after: Option<&'a ArticleCursor>,
    limit: u32,
    origin: Option<&'a str>,
}

struct Cards {
    items: Vec<Card>,
    raw_items: Vec<Value>,
    total: u64,
    next: Option<ArticleCursor>,
}

fn cards(
    store: &Store,
    articles: &ArticleStore,
    request: CardRequest<'_>,
) -> Result<Cards, StoreError> {
    let (items, total, next) = if let Some(Scope::Holder(address)) = request.scope {
        let data = articles
            .articles_by_holder(address, request.text, request.after, request.limit)
            .map_err(map_error)?;
        (
            data.items
                .into_iter()
                .map(|held| {
                    let holding = format!(
                        "{} held{}",
                        held.amount,
                        if held.listed > 0 {
                            format!(" · {} listed", held.listed)
                        } else {
                            String::new()
                        }
                    );
                    (held.summary, holding, Some((held.amount, held.listed)))
                })
                .collect::<Vec<_>>(),
            data.total,
            data.next,
        )
    } else {
        let data = match request.scope {
            Some(Scope::Author(address)) => {
                articles.articles_by_author(address, request.text, request.after, request.limit)
            }
            Some(Scope::Project(id)) => {
                articles.articles_for_project(*id, request.after, request.limit)
            }
            Some(Scope::Text(text)) => {
                articles.articles_page(Some(text), request.after, request.limit)
            }
            _ => articles.articles_page(None, request.after, request.limit),
        }
        .map_err(map_error)?;
        (
            data.items
                .into_iter()
                .map(|summary| (summary, String::new(), None))
                .collect(),
            data.total,
            data.next,
        )
    };
    let names = store.display_names(
        &items
            .iter()
            .map(|(a, _, _)| a.author.as_str())
            .collect::<Vec<_>>(),
    )?;
    let raw_items = items
        .iter()
        .map(|(summary, _, holding)| {
            let author_name = names
                .get(&summary.author)
                .cloned()
                .unwrap_or_else(|| summary.author.clone());
            let mut item = serde_json::Map::new();
            let mut raw_summary = serde_json::to_value(summary).unwrap_or(Value::Null);
            if let Value::Object(fields) = &mut raw_summary {
                fields.insert("author_name".into(), Value::Null);
            }
            item.insert("summary".into(), raw_summary);
            item.insert("author_name".into(), json!(author_name));
            if let Some((amount, listed)) = holding {
                item.insert("amount".into(), json!(amount));
                item.insert("listed".into(), json!(listed));
            }
            Value::Object(item)
        })
        .collect();
    Ok(Cards {
        items: items
            .into_iter()
            .map(|(a, holding, _)| Card {
                id: a.token_id,
                title: a
                    .title
                    .unwrap_or_else(|| format!("Article #{}", a.token_id)),
                description: a.description.unwrap_or_default(),
                author: names
                    .get(&a.author)
                    .cloned()
                    .unwrap_or_else(|| a.author.clone()),
                address: a.author,
                date: date(&a.created_at),
                editions: a.editions,
                image: request
                    .origin
                    .filter(|_| a.thumbnail_uri.is_some())
                    .map_or_else(String::new, |origin| {
                        format!("{origin}/article-media/{}/0", a.token_id)
                    }),
                holding,
            })
            .collect(),
        raw_items,
        total,
        next,
    })
}

pub(super) struct SectionResult {
    pub html: String,
    pub data: Value,
}

#[derive(Template)]
#[template(path = "article_section.html")]
struct Section {
    embedded: bool,
    region: &'static str,
    class: &'static str,
    title: &'static str,
    total: u64,
    coverage: Vec<String>,
    cards: Vec<Card>,
    pages: PageInfo,
}

/// `text` narrows author and holder sections to matching articles; other scopes ignore it.
pub(super) fn section_page(
    store: &Store,
    path: Option<&std::path::Path>,
    scope: Scope,
    text: Option<&str>,
    origin: Option<&str>,
    after: Option<&str>,
    base: &str,
) -> Result<SectionResult, StoreError> {
    let (id, title) = match scope {
        Scope::Author(_) => ("profile-articles", "Articles created"),
        Scope::Holder(_) => ("profile-articles", "Articles collected"),
        Scope::Project(_) => ("project-articles", "Related articles"),
        Scope::Text(_) => ("search-articles", "Articles"),
    };
    let limit = if matches!(scope, Scope::Text(_)) {
        12
    } else {
        3
    };
    let after = after
        .map(ArticleCursor::parse)
        .transpose()
        .map_err(map_error)?;
    let result = (|| {
        let path = path.ok_or_else(|| StoreError::Unavailable("Articles unavailable".into()))?;
        let articles = ArticleStore::open_read_only(path).map_err(map_error)?;
        let before = sync_state(path);
        let data = cards(
            store,
            &articles,
            CardRequest {
                scope: Some(&scope),
                text,
                after: after.as_ref(),
                limit,
                origin,
            },
        )?;
        Ok::<_, StoreError>((data, sync_label(before, sync_state(path))))
    })();
    let (data, as_of) = match result {
        Ok(result) => result,
        Err(StoreError::BadRequest(error)) => return Err(StoreError::BadRequest(error)),
        Err(StoreError::NotFound { what, key }) => {
            return Err(StoreError::NotFound { what, key });
        }
        Err(_) => {
            return Ok(SectionResult {
                html: String::new(),
                data: json!({"available": false, "items": [], "total": null, "pagination": {"first": null, "next": null}, "as_of": null}),
            });
        }
    };
    let next = data.next.as_ref().map_or_else(String::new, |cursor| {
        format!(
            "{}#{id}",
            with_cursor(base.to_owned(), "articles_after", Some(cursor.as_str()))
        )
    });
    let payload = json!({
        "available": true,
        "items": data.raw_items,
        "total": data.total,
        "pagination": {"first": base, "next": if next.is_empty() { Value::Null } else { json!(next) }},
        "as_of": as_of.clone(),
    });
    if data.total == 0 {
        return Ok(SectionResult {
            html: String::new(),
            data: payload,
        });
    }
    let html = Section {
        embedded: true,
        region: id,
        // Search sections share one wrapper; elsewhere the section follows other content.
        class: if id == "search-articles" {
            "search-section"
        } else {
            "article-section"
        },
        title,
        total: data.total,
        coverage: vec![as_of],
        cards: data.items,
        pages: PageInfo {
            page: 1,
            previous: after.as_ref().map(|_| format!("{base}#{id}")),
            next: (!next.is_empty()).then_some(next),
        },
    }
    .render()
    .map_err(|_| StoreError::Internal("Article section rendering failed".into()))?;
    Ok(SectionResult {
        html,
        data: payload,
    })
}
#[derive(Template)]
#[template(path = "article.html")]
struct ArticleView {
    as_of: String,
    meta: Meta,
    id: u64,
    title: String,
    author: String,
    address: String,
    date: String,
    body: String,
    image: String,
    image_link: String,
    caption: String,
    media: String,
    editions: u64,
    held: u64,
    listed: u64,
    verification: String,
    revisions: u64,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/articles", get(list))
        .route("/article/{id}", get(article))
}

pub(super) fn map_error(error: ArticleStoreError) -> StoreError {
    match error {
        ArticleStoreError::BadRequest(_) => StoreError::BadRequest("Invalid article query".into()),
        ArticleStoreError::NotFound { .. } => StoreError::NotFound {
            what: "article",
            key: String::new(),
        },
        ArticleStoreError::Unavailable(_) => StoreError::Unavailable("Articles unavailable".into()),
        ArticleStoreError::Internal(_) => StoreError::Internal("Article read failed".into()),
    }
}

fn sync_state(path: &std::path::Path) -> Option<(u64, u64)> {
    // ArticleStore validates the configured file before this bounded metadata-only read.
    let conn = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    conn.busy_timeout(std::time::Duration::from_millis(250))
        .ok()?;
    let (level, time): (Option<i64>, Option<i64>) = conn
        .query_row(
            "SELECT chain_level,chain_synced_at FROM sync_state WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok()?;
    Some((level?.try_into().ok()?, time?.try_into().ok()?))
}
fn sync_label(before: Option<(u64, u64)>, after: Option<(u64, u64)>) -> String {
    if before != after {
        return "Index sync changed during this read; reload for updated coverage.".into();
    }
    match after {
        Some((level, time)) if time <= 253_402_300_799 => {
            let at = std::time::UNIX_EPOCH + std::time::Duration::from_secs(time);
            format!(
                "Last completed index sync: block {level} · {}.",
                httpdate::fmt_http_date(at)
            )
        }
        _ => "Index sync coverage is not recorded.".into(),
    }
}

async fn list(
    request: Request,
    State(state): State<AppState>,
    query: Result<Query<Search>, QueryRejection>,
) -> Result<Response, WebError> {
    let Query(query) = query.map_err(|_| WebError::bad_request())?;
    if let Some(moved) = super::search::moved_query(&query.q, SearchScope::Articles)? {
        return Ok(moved);
    }
    let scopes = usize::from(query.author.is_some())
        + usize::from(query.holder.is_some())
        + usize::from(query.project.is_some());
    if scopes > 1 || query.project.is_some_and(|id| id > i64::MAX as u64) {
        return Err(WebError::bad_request());
    }
    let scope = query
        .author
        .map(Scope::Author)
        .or_else(|| query.holder.map(Scope::Holder))
        .or_else(|| query.project.map(Scope::Project));
    let path = state
        .articles_database
        .clone()
        .ok_or_else(WebError::unavailable)?;
    let origin = state.content_origin.clone();
    let response_request = request.clone();
    let base = scope
        .as_ref()
        .map_or_else(|| "/articles".to_owned(), Scope::url);
    state
        .query(move |store| {
            // Check current catalogue visibility before trusting cached article references.
            let (scope_label, scope_name, scope_url) = scope
                .as_ref()
                .map(|scope| scope.label(store))
                .transpose()?
                .unwrap_or(("", String::new(), String::new()));
            let articles = ArticleStore::open_read_only(&path).map_err(map_error)?;
            let sync_before = sync_state(&path);
            let after = query
                .after
                .as_deref()
                .map(ArticleCursor::parse)
                .transpose()
                .map_err(map_error)?;
            let data = cards(
                store,
                &articles,
                CardRequest {
                    scope: scope.as_ref(),
                    text: None,
                    after: after.as_ref(),
                    limit: 24,
                    origin: origin.as_deref(),
                },
            )?;
            let next = data.next.map_or_else(String::new, |cursor| {
                with_cursor(base.clone(), "after", Some(cursor.as_str()))
            });
            let as_of = sync_label(sync_before, sync_state(&path));
            let payload = json!({
                "items": data.raw_items,
                "total": data.total,
                "pagination": {
                    "first": base,
                "next": if next.is_empty() { Value::Null } else { json!(next.clone()) },
            },
                "as_of": as_of.clone(),
            });
            let view = ArticlesView {
                embedded: false,
                region: "articles",
                coverage: vec![as_of],
                meta: Meta::new("Articles", "articles", &store.snapshot()?)
                    .scoped(SearchScope::Articles),
                cards: data.items,
                total: data.total,
                pages: PageInfo {
                    page: 1,
                    previous: after.as_ref().map(|_| base.clone()),
                    next: (!next.is_empty()).then_some(next),
                },
                scope_label,
                scope_name,
                scope_url,
            };
            response_request.respond("articles", payload, None, None, || view.render())
        })
        .await
}

fn readable_text(text: &str, media: &[crate::articles::store::MediaRef]) -> String {
    let refs: Vec<_> = media
        .iter()
        .filter(|m| m.kind != "image")
        .take(40)
        .collect();
    let mut fence: Option<(u8, usize)> = None;
    text.lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            if line.len() - trimmed.len() <= 3 {
                let bytes = trimmed.as_bytes();
                if let Some(&first) = bytes.first().filter(|&&c| c == b'`' || c == b'~') {
                    let count = bytes.iter().take_while(|&&c| c == first).count();
                    if count >= 3 {
                        match fence {
                            None => fence = Some((first, count)),
                            Some((open, minimum))
                                if first == open
                                    && count >= minimum
                                    && trimmed[count..].trim().is_empty() =>
                            {
                                fence = None
                            }
                            _ => {}
                        }
                        return true;
                    }
                }
                if fence.is_none()
                    && ["::video[", "::audio[", "::embed-media["]
                        .iter()
                        .any(|prefix| trimmed.starts_with(prefix))
                {
                    return !refs.iter().any(|m| trimmed.contains(&m.src));
                }
            }
            true
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Most embedded cards rendered per article; later pointers become plain links.
const MAX_EMBEDS: usize = 120;

fn embed_key(target: &crate::articles::refs::Target) -> Option<(String, String)> {
    use crate::articles::refs::Target;
    match target {
        Target::Project(id) => Some((format!("project:{id}"), format!("/project/{id}"))),
        Target::Token { contract, token_id } => Some((
            format!("token:{contract}:{token_id}"),
            format!("/token/{contract}/{token_id}"),
        )),
        _ => None,
    }
}

/// The HTML for an article line holding an fxhash storage pointer: the item's card, a plain
/// link past [`MAX_EMBEDS`], or nothing for an item outside the visible archive.
fn embed_for(line: &str, cards: &HashMap<String, Option<String>>) -> Option<String> {
    let (key, path) = embed_key(&crate::articles::refs::pointer(line)?)?;
    Some(match cards.get(&key) {
        Some(Some(card)) => card.clone(),
        Some(None) => format!("<p><a href=\"{path}\">{path}</a></p>"),
        None => String::new(),
    })
}

/// Cards for the visible items an article embeds, keyed as [`embed_key`]; `None` past
/// [`MAX_EMBEDS`].
fn embedded_cards(
    store: &Store,
    references: &[ArticleRef],
    origin: Option<&str>,
) -> Result<HashMap<String, Option<String>>, StoreError> {
    let mut cards = HashMap::new();
    let mut rendered = 0;
    for reference in references.iter().filter(|r| r.form == "storage-pointer") {
        let token = reference.contract.as_deref().zip(reference.token_id);
        let key = match (&reference.kind, token) {
            (RefKind::Token, Some((contract, token_id))) => format!("token:{contract}:{token_id}"),
            (RefKind::Project, _) => format!("project:{}", reference.project_id),
            _ => continue,
        };
        if cards.contains_key(&key) {
            continue;
        }
        if rendered == MAX_EMBEDS {
            cards.insert(key, None);
            continue;
        }
        let html = match (&reference.kind, token) {
            (RefKind::Token, Some((contract, token_id))) => {
                let mut tokens = match store.token(contract, token_id) {
                    Ok(token) => [token],
                    Err(StoreError::NotFound { .. }) => continue,
                    Err(error) => return Err(error),
                };
                super::normalized_tokens(store, &mut tokens)?;
                let mut project = store.project(tokens[0].project_id)?;
                super::normalized_artists(store, &mut project.artists)?;
                super::views::render_embedded_token(
                    &tokens[0],
                    &project.title,
                    &project.artists,
                    origin,
                )
            }
            _ => {
                let project = match store.project(reference.project_id) {
                    Ok(project) => project,
                    Err(StoreError::NotFound { .. }) => continue,
                    Err(error) => return Err(error),
                };
                let published = match store.project_meta(project.id) {
                    Ok(meta) => meta.published,
                    Err(StoreError::NotFound { .. }) => None,
                    Err(error) => return Err(error),
                };
                let mut summaries = [ProjectSummary {
                    id: project.id,
                    title: project.title,
                    artists: project.artists,
                    preview: project.preview,
                    editions: project.editions,
                    index_state: project.index_state,
                    slug: project.slug,
                    supply: project.supply,
                    thumbnail_uri: project.thumbnail_uri,
                    display_uri: project.display_uri,
                    published,
                }];
                super::normalized_projects(store, &mut summaries)?;
                super::views::render_embedded_project(&summaries[0], origin)
            }
        };
        let html =
            html.map_err(|_| StoreError::Internal("Article embed rendering failed".into()))?;
        cards.insert(key, Some(html));
        rendered += 1;
    }
    Ok(cards)
}

/// Paths on this site for links to referenced items, keyed by the URL as written.
fn local_paths(references: &[ArticleRef]) -> HashMap<String, String> {
    references
        .iter()
        .filter(|r| matches!(r.form.as_str(), "fxhash-url" | "marketplace-url"))
        .map(|r| {
            let path = match (&r.kind, &r.contract, r.token_id) {
                (RefKind::Token, Some(contract), Some(token_id)) => {
                    format!("/token/{contract}/{token_id}")
                }
                _ => format!("/project/{}", r.project_id),
            };
            (r.source_text.clone(), path)
        })
        .collect()
}

fn visible_references(
    store: &Store,
    references: Vec<crate::articles::store::ArticleRef>,
) -> Result<Vec<crate::articles::store::ArticleRef>, StoreError> {
    let mut ids: Vec<_> = references
        .iter()
        .filter_map(|reference| i64::try_from(reference.project_id).ok())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    let mut visible = std::collections::HashSet::new();
    for batch in ids.chunks(200) {
        let placeholders = vec!["?"; batch.len()].join(",");
        let sql = format!("SELECT id FROM projects WHERE hidden=0 AND id IN ({placeholders})");
        let mut statement = store
            .conn()
            .prepare(&sql)
            .map_err(|_| StoreError::Unavailable("Article references unavailable".into()))?;
        let rows = statement
            .query_map(rusqlite::params_from_iter(batch), |row| {
                row.get::<_, i64>(0)
            })
            .map_err(|_| StoreError::Unavailable("Article references unavailable".into()))?;
        for row in rows {
            visible.insert(
                row.map_err(|_| StoreError::Unavailable("Article references unavailable".into()))?,
            );
        }
    }
    Ok(references
        .into_iter()
        .filter(|reference| {
            i64::try_from(reference.project_id).is_ok_and(|id| visible.contains(&id))
        })
        .collect())
}

async fn article(
    request: Request,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, WebError> {
    let id = super::numeric_id(&id)?;
    let path = state
        .articles_database
        .clone()
        .ok_or_else(WebError::unavailable)?;
    let origin = state.content_origin.clone();
    let response_request = request.clone();
    state
        .query(move |store| {
            let articles = ArticleStore::open_read_only(&path).map_err(map_error)?;
            let sync_before=sync_state(&path);
            let article = articles.article(id).map_err(map_error)?;
            let references = visible_references(store, articles.article_refs(id).map_err(map_error)?)?;
            let cards = embedded_cards(store, &references, origin.as_deref())?;
            let local_paths = local_paths(&references);
            let names = store.display_names(&[article.summary.author.as_str()])?;
            let as_of = sync_label(sync_before, sync_state(&path));
            let author = names
                .get(&article.summary.author)
                .cloned()
                .unwrap_or_else(|| article.summary.author.clone());
            let mut raw_article = serde_json::to_value(&article)
                .map_err(|_| StoreError::Internal("Article serialization failed".into()))?;
            if let Some(summary) = raw_article.as_object_mut() {
                summary.insert("author_name".into(), Value::Null);
            }
            if let Some(body) = raw_article.get_mut("body").and_then(Value::as_object_mut) {
                body.remove("error");
            }
            let mut media_urls = Vec::new();
            if let Some(origin) = origin.as_ref() {
                if article.display_uri.is_some() {
                    media_urls.push(json!({"slot": 1, "url": format!("{origin}/article-media/{id}/1"), "kind": "display"}));
                } else if article.summary.thumbnail_uri.is_some() {
                    media_urls.push(json!({"slot": 0, "url": format!("{origin}/article-media/{id}/0"), "kind": "thumbnail"}));
                }
                media_urls.extend(article.media.iter().enumerate().filter_map(|(slot, media)| {
                    if media.ipfs.is_some() {
                        Some(json!({"slot": slot + 2, "url": format!("{origin}/article-media/{id}/{}", slot + 2), "kind": media.kind}))
                    } else {
                        markup::safe_link(&media.src).map(|url| {
                            json!({"slot": null, "url": url, "kind": media.kind})
                        })
                    }
                }));
            }
            let payload = json!({
                "article": raw_article,
                "body_untrusted": true,
                "author_name": author.clone(),
                "references": references,
                "as_of": as_of.clone(),
                "media_urls": media_urls,
            });
            let body = article.body.text.as_ref().map_or_else(
                || {
                    format!(
                        "<p>Article text is {}.</p>",
                        match article.body.state {
                            crate::articles::store::FetchState::Pending => "not fetched yet",
                            _ => "unavailable",
                        }
                    )
                },
                |text| {
                    markup::render_article(
                        &readable_text(text, &article.media),
                        &|src| {
                            let slot = article
                                .media
                                .iter()
                                .position(|m| m.src == src || m.ipfs.as_deref() == Some(src))?;
                            origin
                                .as_ref()
                                .map(|o| format!("{o}/article-media/{id}/{}", slot + 2))
                        },
                        &|line| embed_for(line, &cards),
                        &|url| local_paths.get(url).cloned(),
                    )
                },
            );
            let mut media_html = String::new();
            for (slot, media) in article.media.iter().enumerate().filter(|(_, m)| m.kind != "image").take(40) {
                use std::fmt::Write;
                if let Some(origin) = origin.as_ref().filter(|_| media.ipfs.is_some()) {
                    let url = format!("{origin}/article-media/{id}/{}", slot + 2);
                    match media.kind.as_str() {
                        "video" => { let _ = write!(media_html, "<video controls preload=\"none\" src=\"{}\" aria-label=\"Article video\"></video>", markup::escape(&url)); }
                        "audio" => { let _ = write!(media_html, "<audio controls preload=\"none\" src=\"{}\" aria-label=\"Article audio\"></audio>", markup::escape(&url)); }
                        _ => {}
                    }
                } else if let Some(url) = markup::safe_link(&media.src) {
                    let _ = write!(media_html, "<p><a href=\"{}\" rel=\"nofollow ugc noopener noreferrer\">Open embedded media</a></p>", markup::escape(&url));
                }
            }
            let title = article
                .summary
                .title
                .unwrap_or_else(|| format!("Article #{id}"));
            // The thumbnail opens the full image when both exist.
            let (image, image_link) = origin
                .as_ref()
                .map(|o| {
                    let slot = |n: u8| format!("{o}/article-media/{id}/{n}");
                    match (article.summary.thumbnail_uri.is_some(), article.display_uri.is_some()) {
                        (true, true) => (slot(0), slot(1)),
                        (true, false) => (slot(0), String::new()),
                        (false, true) => (slot(1), String::new()),
                        (false, false) => Default::default(),
                    }
                })
                .unwrap_or_default();
            let view = ArticleView {
                    as_of,
                    meta: Meta::new(&title, "articles", &store.snapshot()?)
                        .scoped(SearchScope::Articles),
                    id,
                    title,
                    author,
                    address: article.summary.author,
                    date: date(&article.summary.created_at),
                    image,
                    image_link,
                caption: article.thumbnail_caption.unwrap_or_default(),
                body,
                media: media_html,
                    editions: article.summary.editions,
                    held: article.holders,
                    listed: article.listed,
                        verification: article
                        .body
                        .verification
                        .unwrap_or_else(|| "unverified".into()),
                    revisions: article.revisions,
                };
            response_request.respond("article", payload, Some(&format!("/article/{id}")), None, || view.render())
        })
        .await
}

/// Visible article counts per author for directory entries; empty when the articles
/// database is unavailable, so a missing count never breaks the page.
pub(super) fn author_counts(
    path: Option<&std::path::Path>,
    addresses: &[&str],
) -> std::collections::HashMap<String, u64> {
    path.and_then(|path| ArticleStore::open_read_only(path).ok())
        .and_then(|articles| articles.counts_by_author(addresses).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sync_labels_use_recorded_metadata_and_withhold_missing_or_changed_coverage() {
        assert!(
            sync_label(Some((123, 0)), Some((123, 0))).contains("block 123 · Thu, 01 Jan 1970")
        );
        assert!(sync_label(None, None).contains("not recorded"));
        assert!(sync_label(Some((123, 0)), Some((124, 1))).contains("changed during this read"));
        assert!(sync_label(Some((123, u64::MAX)), Some((123, u64::MAX))).contains("not recorded"));
    }
    #[test]
    fn displayed_media_directives_leave_code_examples_untouched() {
        let m = vec![crate::articles::store::MediaRef {
            kind: "video".into(),
            src: "ipfs://sample".into(),
            ipfs: Some("ipfs://sample".into()),
        }];
        let line = "::video[]{src=\"ipfs://sample\"}";
        assert_eq!(
            readable_text(&format!("before\n{line}\nafter"), &m),
            "before\nafter"
        );
        let code = format!("```text\n{line}\n```\nend");
        assert_eq!(readable_text(&code, &m), code);
        assert_eq!(readable_text(line, &[]), line);
    }
}
