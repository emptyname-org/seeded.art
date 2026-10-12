//! Global catalogue and profile search, with one cursor per result section.
use axum::{
    Router,
    extract::{Query, State, rejection::QueryRejection},
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use serde::Deserialize;

use super::{
    AppState, PAGE_SIZE, WebError, count_page, encode, keyset_page, normalized_artist_summaries,
    normalized_projects, project_volumes, query, single_page, validate_text,
    views::{PageInfo, SearchPage},
};
use crate::store::{Cursor, KeysetPage, Page, ProjectFilter, ProjectSort};

#[derive(Default, Deserialize)]
struct SearchParams {
    #[serde(default)]
    q: String,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    sort: String,
    projects_after: Option<String>,
    artists_after: Option<String>,
    collectors_after: Option<String>,
    articles_after: Option<String>,
}

const SECTIONS: [&str; 3] = ["projects", "artists", "collectors"];

/// The result sections a search covers. Every tab uses the same search and results page;
/// the Artists and Articles tabs only narrow the sections.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SearchScope {
    /// Projects, artists, collectors and articles.
    All,
    /// Artists and collectors.
    People,
    /// Articles.
    Articles,
}

impl SearchScope {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "" => Some(Self::All),
            "people" => Some(Self::People),
            "articles" => Some(Self::Articles),
            _ => None,
        }
    }

    /// The `scope` parameter; empty for every section, which stays out of URLs.
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::All => "",
            Self::People => "people",
            Self::Articles => "articles",
        }
    }

    /// The primary navigation tab the scope belongs to.
    pub(super) fn tab(self) -> &'static str {
        match self {
            Self::All => "",
            Self::People => "artists",
            Self::Articles => "articles",
        }
    }

    /// Where a blank search in this scope returns.
    fn listing(self) -> &'static str {
        match self {
            Self::All => "/projects",
            Self::People => "/artists",
            Self::Articles => "/articles",
        }
    }

    /// Whether `section` (an index into [`SECTIONS`]) is searched.
    fn covers(self, section: usize) -> bool {
        match self {
            Self::All => true,
            Self::People => section != 0,
            Self::Articles => false,
        }
    }

    fn articles(self) -> bool {
        self != Self::People
    }
}

pub(super) fn router() -> Router<AppState> {
    Router::new().route("/search", get(search))
}

/// Free text on a listing page searches that tab's scope instead; the listing's other
/// parameters are dropped. Blank text leaves the listing in place.
pub(super) fn moved_query(text: &str, scope: SearchScope) -> Result<Option<Response>, WebError> {
    validate_text(text)?;
    Ok((!text.trim().is_empty())
        .then(|| Redirect::to(&search_base(text, scope, "volume")).into_response()))
}

/// The projects order and its canonical name; `volume` is the default and stays out of URLs.
pub(super) fn project_sort(value: &str) -> Option<(ProjectSort, &'static str)> {
    match value {
        "newest" => Some((ProjectSort::IdDesc, "newest")),
        "oldest" => Some((ProjectSort::IdAsc, "oldest")),
        "" | "volume" => Some((ProjectSort::VolumeDesc, "volume")),
        _ => None,
    }
}

/// `/search` with the query, a narrowed scope and a non-default sort.
fn search_base(text: &str, scope: SearchScope, sort: &str) -> String {
    let mut url = format!("/search?q={}", encode(text));
    if scope != SearchScope::All {
        url.push_str(&format!("&scope={}", scope.name()));
    }
    if sort != "volume" {
        url.push_str(&format!("&sort={sort}"));
    }
    url
}

fn section_url(
    base: &str,
    cursors: &[Option<Cursor>; 3],
    articles_after: Option<&str>,
    section: usize,
    after: Option<&Cursor>,
) -> String {
    let mut url = base.to_owned();
    for (index, name) in SECTIONS.iter().enumerate() {
        let cursor = if index == section {
            after
        } else {
            cursors[index].as_ref()
        };
        if let Some(cursor) = cursor {
            url.push_str(&format!("&{name}_after={}", cursor.as_str()));
        }
    }
    if let Some(cursor) = articles_after {
        url.push_str(&format!("&articles_after={}", encode(cursor)));
    }
    url.push_str(&format!("#search-{}", SECTIONS[section]));
    url
}

/// The search URL with every section cursor except the articles one, which the
/// articles section appends to it.
fn articles_base(base: &str, cursors: &[Option<Cursor>; 3]) -> String {
    let mut url = base.to_owned();
    for (index, name) in SECTIONS.iter().enumerate() {
        if let Some(cursor) = &cursors[index] {
            url.push_str(&format!("&{name}_after={}", cursor.as_str()));
        }
    }
    url
}

fn section_page<T>(
    data: KeysetPage<T>,
    base: &str,
    cursors: &[Option<Cursor>; 3],
    articles_after: Option<&str>,
    section: usize,
) -> (Page<T>, PageInfo) {
    let pagination = PageInfo {
        page: 1,
        previous: cursors[section]
            .as_ref()
            .map(|_| section_url(base, cursors, articles_after, section, None)),
        next: data
            .next
            .as_ref()
            .map(|after| section_url(base, cursors, articles_after, section, Some(after))),
    };
    (keyset_page(data.items, data.total), pagination)
}

fn empty_section<T>() -> (Page<T>, PageInfo) {
    (count_page(0), single_page())
}

async fn search(
    State(state): State<AppState>,
    request: super::agent::Request,
    params: Result<Query<SearchParams>, QueryRejection>,
) -> Result<Response, WebError> {
    let params = query(params)?;
    validate_text(&params.q)?;
    let text = params.q.trim().to_owned();
    let scope = SearchScope::parse(&params.scope).ok_or_else(WebError::bad_request)?;
    let (sort, mut sort_name) = project_sort(&params.sort).ok_or_else(WebError::bad_request)?;
    if !scope.covers(0) {
        sort_name = "volume";
    }
    if crate::store::is_tezos_address(&text) {
        return Ok(Redirect::to(&format!("/profile/{text}")).into_response());
    }
    let cursors = [
        params.projects_after,
        params.artists_after,
        params.collectors_after,
    ]
    .map(|value| value.as_deref().map(Cursor::parse).transpose());
    let [projects_after, artists_after, collectors_after] = cursors;
    let cursors = [projects_after?, artists_after?, collectors_after?];
    let articles_after = params.articles_after;
    let out_of_scope = cursors
        .iter()
        .enumerate()
        .any(|(section, cursor)| cursor.is_some() && !scope.covers(section))
        || (articles_after.is_some() && !scope.articles());
    if out_of_scope
        || (text.is_empty() && (cursors.iter().any(Option::is_some) || articles_after.is_some()))
    {
        return Err(WebError::bad_request());
    }
    if text.is_empty() && !request.json {
        return Ok(Redirect::to(scope.listing()).into_response());
    }
    let base = search_base(&text, scope, sort_name);
    let content_origin = state.content_origin.clone();
    let articles_path = state.articles_database.clone();
    state
        .query(move |store| {
            let (
                (mut projects, projects_pagination),
                (mut artists, artists_pagination),
                (collectors, collectors_pagination),
            ) = (
                if text.is_empty() || !scope.covers(0) {
                    empty_section()
                } else {
                    section_page(
                        store.projects_page(
                            &ProjectFilter {
                                text: Some(text.clone()),
                                artist: None,
                                sort,
                            },
                            cursors[0].as_ref(),
                            PAGE_SIZE,
                        )?,
                        &base,
                        &cursors,
                        articles_after.as_deref(),
                        0,
                    )
                },
                if text.is_empty() || !scope.covers(1) {
                    empty_section()
                } else {
                    section_page(
                        store.artists_page(Some(&text), cursors[1].as_ref(), PAGE_SIZE)?,
                        &base,
                        &cursors,
                        articles_after.as_deref(),
                        1,
                    )
                },
                if text.is_empty() || !scope.covers(2) {
                    empty_section()
                } else {
                    section_page(
                        store.collectors_search(&text, cursors[2].as_ref(), PAGE_SIZE)?,
                        &base,
                        &cursors,
                        articles_after.as_deref(),
                        2,
                    )
                },
            );
            normalized_projects(store, &mut projects.items)?;
            normalized_artist_summaries(store, &mut artists.items)?;
            let addresses: Vec<_> = artists
                .items
                .iter()
                .map(|artist| artist.id.as_str())
                .chain(
                    collectors
                        .items
                        .iter()
                        .map(|profile| profile.address.as_str()),
                )
                .filter(|address| crate::store::is_tezos_address(address))
                .collect();
            let avatars = store.avatars(&addresses)?;
            let article_counts =
                super::articles::author_counts(articles_path.as_deref(), &addresses);
            let articles = if text.is_empty() || !scope.articles() {
                super::articles::SectionResult {
                    html: String::new(),
                    data: serde_json::json!({"available": true, "items": [], "total": 0}),
                }
            } else {
                super::articles::section_page(
                    store,
                    articles_path.as_deref(),
                    super::articles::Scope::Text(text.clone()),
                    None,
                    content_origin.as_deref(),
                    articles_after.as_deref(),
                    &articles_base(&base, &cursors),
                )?
            };
            let project_volumes = project_volumes(store, &projects.items, sort)?;
            let page = SearchPage {
                query: text,
                scope,
                sort: sort_name.to_owned(),
                article_counts,
                articles: articles.data,
                articles_html: articles.html,
                project_volumes,
                projects,
                artists,
                collectors,
                projects_pagination,
                artists_pagination,
                collectors_pagination,
                avatars,
                content_origin,
                snapshot: store.snapshot()?,
            };
            request.respond("search", serde_json::json!(&page), None, None, || {
                super::views::render_search(page)
            })
        })
        .await
}
