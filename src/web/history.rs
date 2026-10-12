//! Historical prices are evidence of past trades, not current offers.
use crate::{
    markup::escape,
    model::Project,
    sales::{SaleKind, store},
};
use rusqlite::Connection;
use std::{
    collections::HashMap,
    fmt::Write,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

pub fn tez(mutez: u64) -> String {
    let whole = mutez / 1_000_000;
    let fraction = mutez % 1_000_000;
    if fraction == 0 {
        whole.to_string()
    } else {
        format!("{whole}.{fraction:06}")
            .trim_end_matches('0')
            .to_owned()
    }
}

type Summaries = (
    Option<store::ProjectMintSummary>,
    Option<store::ProjectSalesSummary>,
    String,
);
struct CachedSummary {
    created: Instant,
    value: Summaries,
}
type SummaryCache = HashMap<(String, String, u64), CachedSummary>;
static SUMMARIES: OnceLock<Mutex<SummaryCache>> = OnceLock::new();
const CACHE_TTL: Duration = Duration::from_secs(30);
const CACHE_LIMIT: usize = 256;

fn summaries(conn: &Connection, id: u64) -> Result<Summaries, crate::sales::SalesError> {
    let id_sql =
        i64::try_from(id).map_err(|_| crate::sales::SalesError::Usage("Invalid project".into()))?;
    // Visibility is checked on every request, including cache hits.
    let visible: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM projects WHERE id=? AND hidden=0)",
        [id_sql],
        |r| r.get(0),
    )?;
    if !visible {
        return Ok((None, None, String::new()));
    }
    let sales_path: String = conn.query_row(
        "SELECT file FROM pragma_database_list WHERE name='sales'",
        [],
        |r| r.get(0),
    )?;
    let key = (conn.path().unwrap_or_default().to_owned(), sales_path, id);
    let cache = SUMMARIES.get_or_init(|| Mutex::new(HashMap::new()));
    let now = Instant::now();
    if let Ok(entries) = cache.lock() {
        if let Some(entry) = entries
            .get(&key)
            .filter(|e| now.duration_since(e.created) < CACHE_TTL)
        {
            return Ok(entry.value.clone());
        }
    }
    let value = (
        store::project_mint_summary(conn, id)?,
        store::project_sales_summary(conn, id)?,
        sync_coverage(conn),
    );
    if let Ok(mut entries) = cache.lock() {
        entries.retain(|_, e| now.duration_since(e.created) < CACHE_TTL);
        if entries.len() >= CACHE_LIMIT {
            if let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, e)| e.created)
                .map(|(k, _)| k.clone())
            {
                entries.remove(&oldest);
            }
        }
        entries.insert(
            key,
            CachedSummary {
                created: now,
                value: value.clone(),
            },
        );
    }
    Ok(value)
}

fn sync_coverage(conn: &Connection) -> String {
    let read = || -> rusqlite::Result<Vec<(String, Option<i64>)>> {
        let mut query =
            conn.prepare("SELECT stream,synced_level FROM sales.cursors ORDER BY stream LIMIT 65")?;
        query
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect()
    };
    let Ok(rows) = read() else {
        return "<p class=\"subtle\">Sales sync coverage is unavailable.</p>".into();
    };
    if rows.len() > 64 {
        return "<p class=\"subtle\">Sales sync coverage is unavailable.</p>".into();
    }
    let streams = crate::sales::indexer::Stream::all();
    let mut html = String::from(
        "<details class=\"technical-details\"><summary>Last completed sales index sync</summary><dl class=\"source-facts\">",
    );
    for stream in streams {
        let level = rows
            .iter()
            .find(|(name, _)| *name == stream.name())
            .and_then(|(_, level)| *level)
            .filter(|n| *n >= 0);
        let state = level.map_or_else(
            || "No completed sync recorded".into(),
            |n| format!("Block {n}"),
        );
        let _ = write!(
            html,
            "<div><dt>{}</dt><dd>{state}</dd></div>",
            escape(stream.label())
        );
    }
    html.push_str("</dl></details>");
    html
}

pub fn project(conn: &Connection, project: &Project) -> String {
    let summaries = summaries(conn, project.id);
    let mut html = String::from(
        "<section class=\"history-block\" aria-label=\"Mint history\"><h2>Mint history</h2><dl class=\"facts\">",
    );
    let supply = project
        .supply
        .map_or_else(|| "Not recorded".into(), |n| n.to_string());
    let editions = project
        .editions
        .map_or_else(|| "Not indexed".into(), |n| n.to_string());
    let _ = write!(
        html,
        "<div><dt>Supply</dt><dd>{supply}</dd></div><div><dt>Indexed NFTs</dt><dd>{editions}</dd></div>"
    );
    if let Ok(Some(opens)) = conn.query_row(
        "SELECT mint_opens_at FROM projects WHERE id=? AND hidden=0",
        [i64::try_from(project.id).unwrap_or(-1)],
        |r| r.get::<_, Option<String>>(0),
    ) {
        let _ = write!(
            html,
            "<div><dt>Mint scheduled</dt><dd>{}</dd></div>",
            escape(&opens)
        );
    }
    match summaries.as_ref().map(|(mint, _, _)| mint) {
        Ok(Some(mints)) => {
            let _ = write!(
                html,
                "<div><dt>Primary mints recorded</dt><dd>{}</dd></div>",
                mints.minted
            );
            if let (Some(min), Some(max)) = (mints.min_mutez, mints.max_mutez) {
                let prices = if min == max {
                    tez(min)
                } else {
                    format!("{}–{}", tez(min), tez(max))
                };
                let _ = write!(
                    html,
                    "<div><dt>Prices paid at mint</dt><dd>{prices} tez · {} priced mints</dd></div>",
                    mints.priced
                );
            }
        }
        Ok(None) => {
            html.push_str("<div><dt>Mint history</dt><dd>No indexed transactions</dd></div>")
        }
        Err(_) => html.push_str("<div><dt>Mint history</dt><dd>Temporarily unavailable</dd></div>"),
    }
    html.push_str("</dl></section><section class=\"history-block\" aria-label=\"Market history\"><h2>Market history</h2>");
    match summaries.as_ref().map(|(_, sales, _)| sales) {
        Ok(Some(summary)) => {
            let _ = write!(
                html,
                "<dl class=\"facts\"><div><dt>Secondary trades</dt><dd>{}</dd></div><div><dt>Recorded volume</dt><dd>{} tez / wXTZ</dd></div>",
                summary.count,
                tez(summary.volume_mutez)
            );
            if let Some(price) = summary.floor_last_sold_mutez {
                let _ = write!(
                    html,
                    "<div><dt>Lowest latest priced sale</dt><dd>{} tez / wXTZ</dd></div>",
                    tez(price)
                );
            }
            html.push_str("</dl>");
        }
        Ok(None) => html.push_str("<p>No secondary trades indexed.</p>"),
        Err(_) => html.push_str("<p>Price history is temporarily unavailable.</p>"),
    }
    if let Ok((_, _, coverage)) = &summaries {
        html.push_str(coverage);
    }
    html.push_str("</section>");
    html
}

pub fn token(conn: &Connection, contract: &str, id: u64) -> String {
    let mut html = String::from(
        "<section class=\"history-block\" aria-label=\"Price history\"><h2>Price history</h2>",
    );
    match store::sales_for_token(conn, contract, id, 100) {
        Ok(sales) if sales.is_empty() => {
            html.push_str("<p>No mint or sale transactions indexed.</p>")
        }
        Ok(sales) => {
            html.push_str("<div class=\"table-scroll\"><table><thead><tr><th scope=\"col\">Date</th><th scope=\"col\">Event</th><th scope=\"col\">Price</th><th scope=\"col\">Market</th></tr></thead><tbody>");
            for sale in &sales {
                let kind = match sale.kind {
                    SaleKind::Mint => "Mint",
                    SaleKind::Sale => "Sale",
                    SaleKind::OfferAccept => "Offer accepted",
                    SaleKind::Auction => "Auction",
                    SaleKind::Other => "Paid transfer",
                };
                let price = match (&*sale.currency, sale.price_mutez) {
                    ("tez", Some(n)) => format!("{} tez", tez(n)),
                    ("wxtz", Some(n)) => format!("{} wXTZ", tez(n)),
                    (_, _) => format!("Unpriced ({})", escape(&sale.currency)),
                };
                let date = escape(&super::views::date(&sale.timestamp));
                let op = percent_encoding::utf8_percent_encode(
                    &sale.op_hash,
                    percent_encoding::NON_ALPHANUMERIC,
                );
                let _ = write!(
                    html,
                    "<tr><td><a href=\"https://tzkt.io/{op}\" rel=\"noopener noreferrer\">{date}</a></td><td>{kind}</td><td>{price}</td><td>{}</td></tr>",
                    escape(&sale.marketplace)
                );
            }
            html.push_str("</tbody></table></div>");
            if sales.len() == 100 {
                html.push_str("<p class=\"subtle\">100 latest</p>");
            }
        }
        Err(_) => html.push_str("<p>Price history is temporarily unavailable.</p>"),
    }
    html.push_str("</section>");
    html
}

/// Structured historical evidence; unavailable data is distinct from no trades.
pub(super) fn token_data(conn: &Connection, contract: &str, id: u64) -> serde_json::Value {
    match store::sales_for_token(conn, contract, id, 100) {
        Ok(items) => {
            serde_json::json!({"available":true,"items":items,"limit":100,"coverage":coverage_data(conn)})
        }
        Err(_) => serde_json::json!({"available":false}),
    }
}
pub(super) fn project_data(conn: &Connection, id: u64) -> serde_json::Value {
    match summaries(conn, id) {
        Ok((mints, sales, _)) => {
            serde_json::json!({"available":true,"mints":mints,"sales":sales,"coverage":coverage_data(conn)})
        }
        Err(_) => serde_json::json!({"available":false}),
    }
}
fn coverage_data(conn: &Connection) -> serde_json::Value {
    let read = || -> rusqlite::Result<Vec<(String, Option<i64>)>> {
        conn.prepare("SELECT stream,synced_level FROM sales.cursors ORDER BY stream LIMIT 65")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect()
    };
    match read() {
        Ok(rows) if rows.len() <= 64 => serde_json::json!(
            rows.into_iter()
                .map(|(stream, level)| serde_json::json!({"stream":stream,"level":level}))
                .collect::<Vec<_>>()
        ),
        _ => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mutez_conversion_is_exact_for_full_integer_range() {
        assert_eq!(tez(0), "0");
        assert_eq!(tez(1), "0.000001");
        assert_eq!(tez(1_250_000), "1.25");
        assert_eq!(tez(u64::MAX), "18446744073709.551615");
    }
    #[test]
    fn missing_history_is_not_reported_as_zero_sales() {
        let conn = Connection::open_in_memory().unwrap();
        let html = token(&conn, "unknown", 1);
        assert!(html.contains("temporarily unavailable"));
        assert!(!html.contains("No mint"));
    }
}
