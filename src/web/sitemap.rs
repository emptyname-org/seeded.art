//! Bounded sitemap shards; no deep offsets or full token-table response.
use super::{AppState, WebError};
use crate::{
    markup::escape,
    store::{StoreError, TOKEN_CONTRACTS},
};
use axum::{
    Router,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use rusqlite::{Connection, OpenFlags, params};
use std::sync::atomic::{AtomicBool, Ordering};

const SPAN: i64 = 5_000;
/// When each page type's title, description, structured data or main template last changed.
/// Raise it with such a change so crawlers revisit; later sales still win per page.
const TEMPLATE_DATE: &str = "2026-10-11";
const MAX_MAPS: usize = 49_000;
const PREFIXES: [&str; 5] = ["tz1", "tz2", "tz3", "tz4", "KT1"];
const ALPHABET: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
static TRUNCATION_LOGGED: AtomicBool = AtomicBool::new(false);

fn log_truncation(message: &str) {
    if !TRUNCATION_LOGGED.swap(true, Ordering::Relaxed) {
        eprintln!("{message}");
    }
}

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/sitemap.xml", get(index))
        .route("/sitemap/pages.xml", get(pages))
        .route("/sitemap/projects/{start}", get(projects))
        .route("/sitemap/tokens/{contract}/{start}", get(tokens))
        .route("/sitemap/profiles/{tail}", get(profiles))
        .route("/sitemap/articles/{start}", get(articles))
        .route("/robots.txt", get(robots))
}
fn db_error(_: rusqlite::Error) -> StoreError {
    StoreError::Unavailable("Sitemap unavailable".into())
}
fn xml(origin: &str, paths: Vec<String>, index: bool) -> Response {
    let entries = paths.into_iter().map(|path| (path, None)).collect();
    xml_dated(origin, entries, index)
}
/// The later of the template date and a page's own latest change, as `YYYY-MM-DD`.
fn lastmod(changed: Option<&str>) -> String {
    let changed: String = changed.unwrap_or_default().chars().take(10).collect();
    if changed.as_str() > TEMPLATE_DATE {
        changed
    } else {
        TEMPLATE_DATE.to_owned()
    }
}
fn xml_dated(origin: &str, entries: Vec<(String, Option<String>)>, index: bool) -> Response {
    let (root, item) = if index {
        ("sitemapindex", "sitemap")
    } else {
        ("urlset", "url")
    };
    let mut text = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><{root} xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">"
    );
    for (path, modified) in entries {
        let modified = modified
            .map(|date| format!("<lastmod>{}</lastmod>", escape(&date)))
            .unwrap_or_default();
        text.push_str(&format!(
            "<{item}><loc>{}</loc>{modified}</{item}>",
            escape(&format!("{origin}{path}"))
        ));
    }
    text.push_str(&format!("</{root}>"));
    (
        [
            (header::CONTENT_TYPE, "application/xml; charset=utf-8"),
            (header::HeaderName::from_static("x-robots-tag"), "noindex"),
        ],
        text,
    )
        .into_response()
}
fn add_shards(paths: &mut Vec<String>, prefix: &str, max: Option<i64>) -> Result<(), StoreError> {
    if let Some(max) = max.filter(|n| *n >= 0) {
        let count = max / SPAN + 1;
        let available = MAX_MAPS.saturating_sub(paths.len()) as i64;
        if count > available {
            log_truncation("sitemap_truncated: shard bound");
        }
        for n in 0..count.min(available) {
            paths.push(format!("{prefix}/{}", n * SPAN));
        }
    }
    Ok(())
}
fn article_connection(path: &std::path::Path) -> Result<Connection, StoreError> {
    crate::articles::store::ArticleStore::open_read_only(path)
        .map_err(super::articles::map_error)?;
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(db_error)?;
    conn.busy_timeout(std::time::Duration::from_millis(250))
        .map_err(db_error)?;
    Ok(conn)
}
async fn index(State(state): State<AppState>) -> Result<Response, WebError> {
    let origin = state.app_origin.clone();
    let articles = state.articles_database.clone();
    state
        .query(move |store| {
            let mut paths = vec!["/sitemap/pages.xml".into()];
            let mut profile_check = store
                .conn()
                .prepare_cached(
                    "SELECT EXISTS(SELECT 1 FROM holder_counts \
                         WHERE held>0 AND holder_address>=?1 AND holder_address<?2) \
                     OR EXISTS(SELECT 1 FROM artists a WHERE a.address>=?1 AND a.address<?2 \
                         AND EXISTS(SELECT 1 FROM project_artists pa \
                             JOIN projects p ON p.id=pa.project_id \
                             WHERE pa.artist_id=a.id AND p.hidden=0))",
                )
                .map_err(db_error)?;
            for prefix in PREFIXES {
                for character in ALPHABET.chars() {
                    let shard = format!("{prefix}{character}");
                    let upper = format!("{}{}", prefix, char::from(character as u8 + 1));
                    let populated = profile_check
                        .query_row(params![shard, upper], |row| row.get::<_, bool>(0))
                        .map_err(db_error)?;
                    if populated {
                        paths.push(format!("/sitemap/profiles/{shard}"));
                    }
                }
            }
            let max = store
                .conn()
                .query_row("SELECT max(id) FROM projects WHERE hidden=0", [], |r| {
                    r.get(0)
                })
                .map_err(db_error)?;
            add_shards(&mut paths, "/sitemap/projects", max)?;
            for contract in TOKEN_CONTRACTS {
                let max = store
                    .conn()
                    .query_row(
                        "SELECT max(token_id) FROM tokens WHERE contract=?",
                        [contract],
                        |r| r.get(0),
                    )
                    .map_err(db_error)?;
                add_shards(&mut paths, &format!("/sitemap/tokens/{contract}"), max)?;
            }
            if let Some(path) = articles {
                match article_connection(&path).and_then(|conn| {
                    conn.query_row(
                        "SELECT max(token_id) FROM articles WHERE hidden=0",
                        [],
                        |r| r.get(0),
                    )
                    .map_err(db_error)
                }) {
                    Ok(max) => add_shards(&mut paths, "/sitemap/articles", max)?,
                    Err(_) => eprintln!("sitemap_articles_unavailable: secondary index omitted"),
                }
            }
            Ok(xml(&origin, paths, true))
        })
        .await
}
async fn pages(State(state): State<AppState>) -> Response {
    // Search and the collector lookup are not indexed, so they are not listed.
    xml_dated(
        &state.app_origin,
        ["/", "/artists", "/articles", "/about"]
            .into_iter()
            .map(|path| (path.to_owned(), Some(lastmod(None))))
            .collect(),
        false,
    )
}
/// Latest sale per key since the template date; empty when the sales index is not attached.
fn recent_sales(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> std::collections::HashMap<i64, String> {
    conn.prepare(sql)
        .and_then(|mut st| {
            st.query_map(params, |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<Result<_, _>>()
        })
        .unwrap_or_default()
}
fn start(value: &str) -> Result<i64, WebError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|v| *v >= 0 && *v <= i64::MAX - SPAN && *v % SPAN == 0)
        .ok_or_else(WebError::bad_request)
}
async fn projects(
    State(state): State<AppState>,
    Path(value): Path<String>,
) -> Result<Response, WebError> {
    let start = start(&value)?;
    let origin = state.app_origin.clone();
    state
        .query(move |store| {
            let ids = store
                .conn()
                .prepare_cached(
                    "SELECT id FROM projects WHERE hidden=0 AND id>=? AND id<? ORDER BY id",
                )
                .map_err(db_error)?
                .query_map(params![start, start + SPAN], |r| r.get::<_, i64>(0))
                .map_err(db_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(db_error)?;
            let sold = recent_sales(
                store.conn(),
                // CROSS JOIN keeps sales first: few rows pass the date, then one token lookup each.
                "SELECT t.project_id, max(s.timestamp) FROM sales.sales s \
                 CROSS JOIN tokens t ON t.contract = s.contract AND t.token_id = s.token_id \
                 WHERE s.timestamp > ?3 AND t.project_id >= ?1 AND t.project_id < ?2 \
                 GROUP BY t.project_id",
                params![start, start + SPAN, TEMPLATE_DATE],
            );
            Ok(xml_dated(
                &origin,
                ids.into_iter()
                    .map(|id| {
                        let date = lastmod(sold.get(&id).map(String::as_str));
                        (format!("/project/{id}"), Some(date))
                    })
                    .collect(),
                false,
            ))
        })
        .await
}
async fn tokens(
    State(state): State<AppState>,
    Path((contract, value)): Path<(String, String)>,
) -> Result<Response, WebError> {
    let start = start(&value)?;
    let origin = state.app_origin.clone();
    if !TOKEN_CONTRACTS.contains(&contract.as_str()) {
        return Err(WebError::bad_request());
    }
    state.query(move|store|{
        let ids=store.conn().prepare_cached("SELECT t.token_id FROM tokens t JOIN projects p ON p.id=t.project_id WHERE p.hidden=0 AND t.contract=? AND t.token_id>=? AND t.token_id<? ORDER BY t.token_id").map_err(db_error)?.query_map(params![contract,start,start+SPAN],|r|r.get::<_,i64>(0)).map_err(db_error)?.collect::<Result<Vec<_>,_>>().map_err(db_error)?;
        let sold = recent_sales(
            store.conn(),
            "SELECT token_id, max(timestamp) FROM sales.sales \
             WHERE contract = ?1 AND token_id >= ?2 AND token_id < ?3 AND timestamp > ?4 \
             GROUP BY token_id",
            params![contract, start, start + SPAN, TEMPLATE_DATE],
        );
        Ok(xml_dated(
            &origin,
            ids.into_iter()
                .map(|id| {
                    let date = lastmod(sold.get(&id).map(String::as_str));
                    (format!("/token/{contract}/{id}"), Some(date))
                })
                .collect(),
            false,
        ))
    }).await
}
async fn profiles(
    State(state): State<AppState>,
    Path(tail): Path<String>,
) -> Result<Response, WebError> {
    if tail.len() == 1 && tail.is_ascii() && ALPHABET.contains(&tail[..]) {
        let mut response = StatusCode::MOVED_PERMANENTLY.into_response();
        response.headers_mut().insert(
            header::LOCATION,
            "/sitemap.xml".parse().expect("static location"),
        );
        return Ok(response);
    }
    if !tail.is_ascii()
        || tail.len() != 4
        || !PREFIXES.contains(&&tail[..3])
        || !ALPHABET.contains(&tail[3..])
    {
        return Err(WebError::bad_request());
    }
    let origin = state.app_origin.clone();
    state.query(move|store|{
        let upper = format!("{}{}", &tail[..3], char::from(tail.as_bytes()[3]+1));
        // Same exclusions as the profile robots rule: holders listed only when they are
        // accounts holding 5 or more editions or carrying a name; artists always.
        let sql="SELECT address FROM (SELECT h.holder_address AS address FROM holder_counts h WHERE h.held>0 AND h.holder_address>=?1 AND h.holder_address<?2 AND h.holder_address NOT LIKE 'KT1%' AND NOT EXISTS(SELECT 1 FROM known_addresses k WHERE k.address=h.holder_address) AND (h.held>=5 OR EXISTS(SELECT 1 FROM users u WHERE u.address=h.holder_address AND u.name IS NOT NULL AND u.name<>'' AND NOT EXISTS(SELECT 1 FROM users v WHERE v.name_key=u.name_key AND v.address<>u.address))) UNION SELECT a.address FROM artists a WHERE a.address>=?1 AND a.address<?2 AND EXISTS(SELECT 1 FROM project_artists pa JOIN projects p ON p.id=pa.project_id WHERE pa.artist_id=a.id AND p.hidden=0)) ORDER BY address LIMIT 50001";
        let mut addresses=store.conn().prepare_cached(sql).map_err(db_error)?.query_map(params![tail,upper],|r|r.get::<_,String>(0)).map_err(db_error)?.collect::<Result<Vec<_>,_>>().map_err(db_error)?;
        if addresses.len()>50_000 { addresses.truncate(50_000); log_truncation("sitemap_truncated: shard bound"); }
        Ok(xml_dated(&origin,addresses.into_iter().filter(|a|crate::store::is_tezos_address(a)).map(|a|(format!("/profile/{a}"),Some(lastmod(None)))).collect(),false))
    }).await
}
async fn articles(
    State(state): State<AppState>,
    Path(value): Path<String>,
) -> Result<Response, WebError> {
    let start = start(&value)?;
    let origin = state.app_origin.clone();
    let path = state
        .articles_database
        .clone()
        .ok_or_else(WebError::unavailable)?;
    state.query(move|_|{
        let conn=article_connection(&path)?;
        // Articles without a fetched body are noindex, so they are not listed.
        let ids=conn.prepare_cached("SELECT a.token_id FROM articles a JOIN objects o ON o.uri=a.body_key WHERE a.hidden=0 AND a.body_uri IS NOT NULL AND o.state='ok' AND a.token_id>=? AND a.token_id<? ORDER BY a.token_id").map_err(db_error)?.query_map(params![start,start+SPAN],|r|r.get::<_,i64>(0)).map_err(db_error)?.collect::<Result<Vec<_>,_>>().map_err(db_error)?;
        Ok(xml_dated(&origin,ids.into_iter().map(|id|(format!("/article/{id}"),Some(lastmod(None)))).collect(),false))
    }).await
}
async fn robots(State(state): State<AppState>) -> Response {
    (StatusCode::OK,[(header::CONTENT_TYPE,"text/plain; charset=utf-8")],format!("# Disallow lines are for crawlers. Agents may sign in with their own address; see {0}/agents.md\nUser-agent: *\nAllow: /\nDisallow: /auth\nDisallow: /api/auth/\nDisallow: /api/market/\nDisallow: /health/\nDisallow: /oauth/\nDisallow: /token/*/full\nSitemap: {0}/sitemap.xml\n",state.app_origin)).into_response()
}
