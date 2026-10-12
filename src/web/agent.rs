//! Content negotiation and discovery for the same public page data.
use axum::{
    Json, Router,
    extract::FromRequestParts,
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header, request::Parts},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};

use super::{AppState, WebError};
use crate::{markup::escape, store::StoreError};

#[derive(serde::Serialize)]
pub(super) struct TokenRef<'a> {
    contract: &'a str,
    token_id: u64,
    iteration: u32,
}
impl<'a> From<&'a crate::model::Token> for TokenRef<'a> {
    fn from(token: &'a crate::model::Token) -> Self {
        Self {
            contract: &token.contract,
            token_id: token.token_id,
            iteration: token.iteration,
        }
    }
}
#[derive(serde::Serialize)]
pub(super) struct TokenData<'a> {
    pub instructions: &'a str,
    pub content_origin: Option<&'a str>,
    pub token: &'a crate::model::Token,
    pub project: &'a crate::model::Project,
    pub shares: &'a [crate::model::FeatureShare],
    pub snapshot: &'a crate::model::Snapshot,
    pub previous: Option<TokenRef<'a>>,
    pub next: Option<TokenRef<'a>>,
    pub history: Option<Value>,
}

fn canonical_path(uri: &Uri) -> String {
    let keys: &[&str] = match uri.path() {
        "/" | "/projects" => &["after", "artist", "sort"],
        "/artists" => &["after"],
        "/articles" => &["after", "author", "holder", "project"],
        "/search" => &[
            "q",
            "scope",
            "sort",
            "projects_after",
            "artists_after",
            "collectors_after",
            "articles_after",
        ],
        path if path.ends_with("/market") => &["after", "token_contract", "token_id", "view"],
        _ => &[],
    };
    let pairs: std::collections::BTreeMap<_, _> = reqwest::Url::parse(&format!(
        "http://local{}",
        uri.path_and_query().map_or("/", |p| p.as_str())
    ))
    .map(|url| url.query_pairs().into_owned().collect())
    .unwrap_or_default();
    let mut url =
        reqwest::Url::parse(&format!("http://local{}", uri.path())).expect("validated request URI");
    let legacy_sort = matches!(
        pairs.get("sort").map(String::as_str),
        Some("title" | "artist")
    );
    for key in keys {
        if let Some(value) = pairs.get(*key).filter(|v| !v.is_empty()) {
            if *key == "after" && legacy_sort {
                continue;
            }
            let value = if *key == "sort" {
                match value.as_str() {
                    "id" => "oldest",
                    "title" | "artist" => "volume",
                    other => other,
                }
            } else {
                value
            };
            if *key == "sort" && value == "volume" {
                continue;
            }
            url.query_pairs_mut().append_pair(key, value);
        }
    }
    // The projects listing is the home page: its unfiltered form is `/`.
    let path = match url.path() {
        "/" | "/projects" if url.query().is_none() => "/",
        "/" => "/projects",
        other => other,
    };
    format!(
        "{path}{}",
        url.query().map(|q| format!("?{q}")).unwrap_or_default()
    )
}

fn query_map(uri: &Uri) -> std::collections::HashMap<String, String> {
    uri.query()
        .map(|q| {
            reqwest::Url::parse(&format!("http://local/?{q}"))
                .map(|url| url.query_pairs().into_owned().collect())
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

/// The robots directive for a page. Thin, filtered, paginated and private pages are kept out
/// of search results but their links are still followed.
fn robots(
    kind: &str,
    path: &str,
    home: bool,
    query: &std::collections::HashMap<String, String>,
    data: &Value,
) -> &'static str {
    const HIDE: &str = "noindex, follow";
    let set = |key: &str| query.get(key).is_some_and(|v| !v.is_empty());
    let profile = &data["profile"];
    let artist = profile["project_count"].as_u64().unwrap_or_default() > 0;
    match kind {
        _ if path == "/search" => HIDE,
        _ if path.starts_with("/token/") && path.ends_with("/full") => "noindex",
        "auth" | "profile_edit" | "artwork" => "noindex",
        "market" | "collectors" => HIDE,
        "projects" if !home => HIDE,
        "artists" if set("after") => HIDE,
        "articles"
            if ["after", "author", "holder", "project"]
                .iter()
                .any(|k| set(k)) =>
        {
            HIDE
        }
        "project" if set("after") || set("articles_after") => HIDE,
        // Escrows, the burn address and contracts without projects are not profiles of people.
        "profile" if profile["kind"] != "account" && !artist => HIDE,
        "profile" if set("after") || set("articles_after") => HIDE,
        "profile" if artist && query.get("view").is_some_and(|v| v == "collected") => HIDE,
        "profile"
            if !artist
                && profile["held"].as_u64().unwrap_or_default() < 5
                && profile["name"].is_null()
                && profile["bio"].is_null() =>
        {
            HIDE
        }
        "article" if data["article"]["body_state"] != "ok" => HIDE,
        _ => "max-image-preview:large",
    }
}

/// Paginated project and profile pages differ from page one, so they name themselves.
fn paged_canonical(
    kind: &str,
    base: &str,
    query: &std::collections::HashMap<String, String>,
    data: &Value,
) -> String {
    let artist = data["profile"]["project_count"]
        .as_u64()
        .unwrap_or_default()
        > 0;
    let mut keys = vec!["after", "articles_after"];
    if kind == "profile" && artist {
        keys.push("view");
    }
    // A cursor only resumes its own filtered listing, so a paged canonical keeps the filter.
    if keys[..2]
        .iter()
        .any(|k| query.get(*k).is_some_and(|v| !v.is_empty()))
    {
        keys.extend(["q", "trait_name", "trait_value"]);
    }
    let mut url = reqwest::Url::parse(&format!("http://local{base}")).expect("handler canonical");
    for key in keys {
        match query.get(key).filter(|v| !v.is_empty()) {
            Some(v) if key != "view" || v == "collected" => {
                url.query_pairs_mut().append_pair(key, v);
            }
            _ => {}
        }
    }
    format!(
        "{}{}",
        url.path(),
        url.query().map(|q| format!("?{q}")).unwrap_or_default()
    )
}

fn meta_tags(head: &super::seo::Head, canonical: &str) -> String {
    let mut out = format!(
        "<meta name=\"description\" content=\"{}\"><meta property=\"og:site_name\" content=\"seeded.art\"><meta property=\"og:type\" content=\"{}\"><meta property=\"og:title\" content=\"{}\"><meta property=\"og:description\" content=\"{}\"><meta property=\"og:url\" content=\"{}\">",
        escape(&head.description),
        head.og_type,
        escape(&head.og_title),
        escape(&head.description),
        escape(canonical)
    );
    for (property, content) in &head.extra {
        out.push_str(&format!(
            "<meta property=\"{property}\" content=\"{}\">",
            escape(content)
        ));
    }
    match &head.image {
        Some((url, alt)) => out.push_str(&format!(
            "<meta property=\"og:image\" content=\"{}\"><meta property=\"og:image:alt\" content=\"{}\"><meta name=\"twitter:card\" content=\"summary_large_image\"><meta name=\"twitter:image:alt\" content=\"{}\">",
            escape(url),
            escape(alt),
            escape(alt)
        )),
        None => {
            // Pages without their own artwork share the site card.
            let origin = canonical
                .find("://")
                .and_then(|i| canonical[i + 3..].find('/').map(|j| &canonical[..i + 3 + j]))
                .unwrap_or(canonical);
            out.push_str(&format!(
                "<meta property=\"og:image\" content=\"{}/static/og.png\"><meta property=\"og:image:width\" content=\"1200\"><meta property=\"og:image:height\" content=\"630\"><meta property=\"og:image:alt\" content=\"seeded.art\"><meta name=\"twitter:card\" content=\"summary_large_image\"><meta name=\"twitter:image:alt\" content=\"seeded.art\">",
                escape(origin)
            ));
        }
    }
    out
}

#[derive(Clone)]
pub(super) struct Request {
    pub json: bool,
    pub uri: Uri,
    pub origin: String,
    pub site: std::sync::Arc<super::seo::SiteFacts>,
}

impl FromRequestParts<AppState> for Request {
    type Rejection = WebError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, WebError> {
        Ok(Self {
            json: wants_json(&parts.headers),
            uri: parts.uri.clone(),
            origin: state.app_origin.clone(),
            site: state.site_facts(),
        })
    }
}

pub(super) fn wants_json(headers: &HeaderMap) -> bool {
    let mut json_quality = 0.0_f32;
    let mut html_quality = 0.0_f32;
    for value in headers.get_all(header::ACCEPT) {
        let Ok(value) = value.to_str() else { continue };
        for media in value.split(',') {
            let mut parts = media.trim().split(';');
            let mime = parts.next().unwrap_or_default().trim();
            let quality = parts
                .find_map(|part| part.trim().strip_prefix("q="))
                .map_or(1.0, |q| q.parse::<f32>().unwrap_or(0.0));
            let quality = if quality.is_finite() && (0.0..=1.0).contains(&quality) {
                quality
            } else {
                0.0
            };
            if mime.eq_ignore_ascii_case("application/json") {
                json_quality = json_quality.max(quality);
            } else if mime.eq_ignore_ascii_case("text/html") {
                html_quality = html_quality.max(quality);
            }
        }
    }
    json_quality > 0.0 && json_quality >= html_quality
}

impl Request {
    pub fn respond(
        &self,
        kind: &str,
        data: Value,
        canonical: Option<&str>,
        structured: Option<Value>,
        html: impl FnOnce() -> askama::Result<String>,
    ) -> Result<Response, StoreError> {
        let path = self.uri.path_and_query().map_or("/", |p| p.as_str());
        let url = format!("{}{path}", self.origin);
        let query = query_map(&self.uri);
        let home = kind == "projects"
            && ["after", "artist", "sort"]
                .iter()
                .all(|k| query.get(*k).is_none_or(String::is_empty));
        let normalized_path = canonical_path(&self.uri);
        let paged = canonical.map(|base| match kind {
            "project" | "profile" => paged_canonical(kind, base, &query, &data),
            _ => base.to_owned(),
        });
        let canonical = format!(
            "{}{}",
            self.origin,
            paged.as_deref().unwrap_or(&normalized_path)
        )
        .replace('<', "%3C")
        .replace('>', "%3E")
        .replace('"', "%22");
        let mut response = if self.json {
            Json(json!({
                "version":"1", "kind":kind, "url":url, "canonical":canonical,
                "data":data,
                "links":{"self":url,"openapi":"/openapi.json","llms":"/llms.txt","agents":"/agents.md"}
            }))
            .into_response()
        } else {
            let mut head = format!(
                "<link rel=\"canonical\" href=\"{}\"><link rel=\"alternate\" type=\"application/json\" href=\"{}\"><link rel=\"service-desc\" type=\"application/vnd.oai.openapi+json\" href=\"/openapi.json\"><link rel=\"help\" type=\"text/markdown\" href=\"/agents.md\">",
                escape(&canonical),
                escape(&url)
            );
            let head_tags = super::seo::head(kind, home, &query, &data, &self.site);
            if let Some(tags) = &head_tags {
                head.push_str(&meta_tags(tags, &canonical));
            }
            if let Some(schema) = structured.or_else(|| {
                super::seo::structured(
                    kind,
                    home,
                    &data,
                    &self.origin,
                    &canonical,
                    head_tags.as_ref(),
                )
            }) {
                // Escape HTML delimiters, including </script> in indexed descriptions.
                let text = schema
                    .to_string()
                    .replace('<', "\\u003c")
                    .replace('>', "\\u003e")
                    .replace('&', "\\u0026");
                head.push_str(&format!(
                    "<script type=\"application/ld+json\">{text}</script>"
                ));
            }
            let mut page = super::render(html())?;
            if let (Some(tags), Some(start), Some(end)) =
                (&head_tags, page.find("<title>"), page.find("</title>"))
            {
                if start < end {
                    page.replace_range(start + "<title>".len()..end, &escape(&tags.title));
                }
            }
            Html(page.replacen("</head>", &format!("{head}</head>"), 1)).into_response()
        };
        let robots = robots(kind, self.uri.path(), home, &query, &data);
        response
            .headers_mut()
            .insert("x-robots-tag", HeaderValue::from_static(robots));
        if let Ok(link) = HeaderValue::from_str(&format!(
            "<{canonical}>; rel=\"canonical\", </openapi.json>; rel=\"service-desc\", </agents.md>; rel=\"help\""
        )) {
            response.headers_mut().insert(header::LINK, link);
        }
        Ok(response)
    }
}

pub(super) fn error(status: StatusCode) -> Response {
    (status, Json(json!({"version":"1","error":{
        "status":status.as_u16(),
        "code":match status.as_u16(){400=>"bad_request",401=>"unauthenticated",403=>"forbidden",404=>"not_found",405=>"method_not_allowed",429=>"rate_limited",503=>"unavailable",_=>"request_failed"},
        "message":status.canonical_reason().unwrap_or("Request failed")
    }}))).into_response()
}

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/openapi.json",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    include_str!("../../docs/openapi.json"),
                )
            }),
        )
        .route(
            "/agents.md",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/markdown; charset=utf-8")],
                    include_str!("../../docs/agents.md"),
                )
            }),
        )
        .route(
            "/llms.txt",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                    include_str!("../../docs/llms.txt"),
                )
            }),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn thin_paged_and_private_profiles_are_not_indexed() {
        let collector = |held: u64, name: Value, bio: Value| json!({"profile": {"kind": "account", "project_count": 0, "held": held, "name": name, "bio": bio}});
        let artist =
            json!({"profile": {"kind": "account", "project_count": 2, "held": 0, "name": "A"}});
        let none = q(&[]);
        let index = "max-image-preview:large";
        let hide = "noindex, follow";
        assert_eq!(
            robots(
                "profile",
                "/profile/x",
                false,
                &none,
                &collector(1, Value::Null, Value::Null)
            ),
            hide
        );
        assert_eq!(
            robots(
                "profile",
                "/profile/x",
                false,
                &none,
                &collector(4, json!("named"), Value::Null)
            ),
            index
        );
        assert_eq!(
            robots(
                "profile",
                "/profile/x",
                false,
                &none,
                &collector(1, Value::Null, json!("bio"))
            ),
            index
        );
        assert_eq!(
            robots(
                "profile",
                "/profile/x",
                false,
                &none,
                &collector(5, Value::Null, Value::Null)
            ),
            index
        );
        assert_eq!(
            robots("profile", "/profile/x", false, &none, &artist),
            index
        );
        assert_eq!(
            robots(
                "profile",
                "/profile/x",
                false,
                &q(&[("view", "collected")]),
                &artist
            ),
            hide
        );
        assert_eq!(
            robots(
                "profile",
                "/profile/x",
                false,
                &q(&[("after", "c")]),
                &artist
            ),
            hide
        );
        assert_eq!(
            robots(
                "profile",
                "/profile/x",
                false,
                &none,
                &json!({"profile": {"kind": "contract"}})
            ),
            hide
        );
        let collaboration = json!({"profile": {"kind": "contract", "project_count": 3, "held": 0}});
        assert_eq!(
            robots("profile", "/profile/KT1x", false, &none, &collaboration),
            index
        );
        assert_eq!(
            robots(
                "project",
                "/project/1",
                false,
                &q(&[("after", "c")]),
                &json!({})
            ),
            hide
        );
        assert_eq!(
            robots(
                "project",
                "/project/1",
                false,
                &q(&[("q", "gold")]),
                &json!({})
            ),
            index
        );
        assert_eq!(
            robots(
                "article",
                "/article/1",
                false,
                &none,
                &json!({"article": {"body_state": "pending"}})
            ),
            hide
        );
        assert_eq!(
            robots(
                "article",
                "/article/1",
                false,
                &none,
                &json!({"article": {"body_state": "ok"}})
            ),
            index
        );
        assert_eq!(robots("projects", "/", true, &none, &json!({})), index);
        assert_eq!(
            robots(
                "projects",
                "/projects",
                false,
                &q(&[("sort", "newest")]),
                &json!({})
            ),
            hide
        );
        assert_eq!(
            robots("token", "/token/KT1/1/full", false, &none, &json!({})),
            "noindex"
        );
    }

    #[test]
    fn paginated_project_and_profile_pages_name_themselves() {
        let artist = json!({"profile": {"project_count": 1}});
        let collector = json!({"profile": {"project_count": 0}});
        assert_eq!(
            paged_canonical(
                "project",
                "/project/1",
                &q(&[("after", "abc"), ("q", "x")]),
                &json!({})
            ),
            "/project/1?after=abc&q=x"
        );
        assert_eq!(
            paged_canonical(
                "profile",
                "/profile/tz1",
                &q(&[("view", "collected"), ("q", "Arb"), ("after", "C")]),
                &collector
            ),
            "/profile/tz1?after=C&q=Arb"
        );
        assert_eq!(
            paged_canonical("project", "/project/1", &q(&[("q", "x")]), &json!({})),
            "/project/1"
        );
        assert_eq!(
            paged_canonical(
                "profile",
                "/profile/tz1",
                &q(&[("view", "collected")]),
                &artist
            ),
            "/profile/tz1?view=collected"
        );
        assert_eq!(
            paged_canonical(
                "profile",
                "/profile/tz1",
                &q(&[("view", "collected")]),
                &collector
            ),
            "/profile/tz1"
        );
        assert_eq!(
            paged_canonical(
                "profile",
                "/profile/tz1",
                &q(&[("articles_after", "z")]),
                &collector
            ),
            "/profile/tz1?articles_after=z"
        );
    }
    #[test]
    fn explicit_json_respects_quality_and_does_not_change_browser_defaults() {
        for (accept, expected) in [
            ("application/json", true),
            ("application/json;q=0", false),
            ("text/html,application/json;q=0.5", false),
            ("*/*", false),
            ("application/json;q=NaN", false),
            ("application/json;q=1,text/html;q=0.2", true),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::ACCEPT, accept.parse().unwrap());
            assert_eq!(wants_json(&headers), expected, "{accept}");
        }
    }
}
