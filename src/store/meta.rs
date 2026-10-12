//! Project catalogue metadata: publication time, tags and credited addresses.

use rusqlite::Connection;
use serde_json::Value;

use super::{Db, Store, StoreError, get_u64, not_found, sql_id};
use crate::model::ProjectMeta;

/// One row per credited artist (one row with NULL artist columns when none), in credit order.
/// Driven by the projects and project_artists primary keys.
const PROJECT_META: &str = "SELECT p.id, p.created_at, p.mint_opens_at, p.tags, \
     p.generative_uri, p.author_address, a.address FROM projects p \
     LEFT JOIN project_artists pa ON pa.project_id = p.id \
     LEFT JOIN artists a ON a.id = pa.artist_id \
     WHERE p.id = ?1 AND p.hidden = 0 ORDER BY pa.position";

impl Store {
    /// NotFound for unknown and flagged (hidden) projects, like [`Store::project`].
    pub fn project_meta(&self, id: u64) -> Result<ProjectMeta, StoreError> {
        let key = sql_id(id).ok_or_else(|| not_found("project", id))?;
        self.read(|c| project_meta(c, key))?
            .ok_or_else(|| not_found("project", id))
    }
}

fn project_meta(c: &Connection, key: i64) -> Result<Option<ProjectMeta>, StoreError> {
    let mut st = c.prepare_cached(PROJECT_META).db()?;
    let mut rows = st.query([key]).db()?;
    let mut meta: Option<ProjectMeta> = None;
    while let Some(r) = rows.next().db()? {
        if meta.is_none() {
            meta = Some(ProjectMeta {
                id: get_u64(r, 0).db()?,
                published: r.get(1).db()?,
                mint_opens_at: r.get(2).db()?,
                tags: parse_tags(r.get::<_, Option<String>>(3).db()?.as_deref()),
                generative_uri: r.get(4).db()?,
                author_address: r.get(5).db()?,
                artist_addresses: Vec::new(),
            });
        }
        if let (Some(m), Some(address)) = (meta.as_mut(), r.get::<_, Option<String>>(6).db()?) {
            m.artist_addresses.push(address);
        }
    }
    Ok(meta)
}

/// Tags from the stored JSON array; anything else (NULL, garbage, another type) is no tags.
/// Non-string and blank items are dropped.
fn parse_tags(raw: Option<&str>) -> Vec<String> {
    match raw.map(serde_json::from_str::<Value>) {
        Some(Ok(Value::Array(items))) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_parse_and_tolerate_null_or_garbage() {
        assert_eq!(
            parse_tags(Some(r#"["trees", "black and white"]"#)),
            ["trees", "black and white"]
        );
        assert_eq!(
            parse_tags(Some(r#"["a", 1, null, " ", " b "]"#)),
            ["a", "b"]
        );
        for bad in [
            None,
            Some(""),
            Some("null"),
            Some("not json"),
            Some("[\"open"),
            Some("{\"a\":1}"),
            Some("\"trees\""),
            Some("42"),
        ] {
            assert!(parse_tags(bad).is_empty(), "{bad:?}");
        }
    }

    #[test]
    fn project_meta_is_one_indexed_lookup() {
        let store = Store::open_in_memory().unwrap();
        let mut st = store
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {PROJECT_META}"))
            .unwrap();
        let mut rows = st.raw_query();
        let mut plan = Vec::new();
        while let Some(r) = rows.next().unwrap() {
            plan.push(r.get::<_, String>(3).unwrap());
        }
        assert_eq!(plan.len(), 3, "{plan:?}");
        for step in &plan {
            assert!(step.starts_with("SEARCH "), "{plan:?}");
        }
        assert!(plan[0].contains("INTEGER PRIMARY KEY"), "{plan:?}");
        assert!(plan[1].contains("PRIMARY KEY"), "{plan:?}");
    }
}
