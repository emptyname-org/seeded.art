//! Page titles, descriptions, link previews and structured data, built from each page's JSON
//! view so HTML metadata and the JSON API always say the same thing.
use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};

/// Archive-wide facts for metadata, refreshed in the background (see `AppState::site_facts`).
#[derive(Default)]
pub(crate) struct SiteFacts {
    pub projects: u64,
    pub editions: u64,
    /// Visible projects whose title and artists collide, with their telling suffix.
    pub project_suffixes: HashMap<u64, String>,
    /// Articles whose title is shared or shorter than 8 characters.
    pub article_ids: HashSet<u64>,
}

pub(super) struct Head {
    pub title: String,
    pub og_title: String,
    pub description: String,
    pub og_type: &'static str,
    pub image: Option<(String, String)>,
    /// Extra `property`/`content` pairs, e.g. article times and tags.
    pub extra: Vec<(&'static str, String)>,
}

const BRAND: &str = " | seeded.art";
const MAX_TITLE: usize = 60;
const MAX_DESCRIPTION: usize = 160;
pub(super) const ORGANIZATION: &str = "Independent archive of fxhash generative art on Tezos.";
const SITE_TITLE: &str = "seeded.art: generative art on Tezos (fxhash archive)";

fn len(text: &str) -> usize {
    text.chars().count()
}

fn text(value: &Value) -> &str {
    value.as_str().unwrap_or_default()
}

/// Whitespace collapsed; `None` when empty.
fn clean(value: &Value) -> Option<String> {
    let text = value
        .as_str()?
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    (!text.is_empty()).then_some(text)
}

/// `text` cut to at most `budget` characters: at a sentence end when that keeps 60 or more,
/// else at a word boundary with "…".
fn excerpt(text: &str, budget: usize) -> Option<String> {
    if len(text) <= budget {
        return Some(text.to_owned());
    }
    if budget < 20 {
        return None;
    }
    let head: String = text.chars().take(budget).collect();
    let sentence = head
        .char_indices()
        .filter(|&(i, c)| matches!(c, '.' | '!' | '?') && head[i + 1..].starts_with(' '))
        .map(|(i, _)| i + 1)
        .rfind(|&end| len(&head[..end]) >= 60);
    if let Some(end) = sentence {
        return Some(head[..end].to_owned());
    }
    let words: String = text.chars().take(budget - 1).collect();
    let cut = words.rfind(' ').map_or(words.as_str(), |i| &words[..i]);
    Some(format!("{}…", cut.trim_end_matches([',', ';', ':', ' '])))
}

/// Clauses joined with spaces while the total stays within the description limit.
fn description(clauses: impl IntoIterator<Item = String>) -> String {
    let mut out = String::new();
    for clause in clauses {
        let next = if out.is_empty() {
            clause.clone()
        } else {
            format!("{out} {clause}")
        };
        if len(&next) > MAX_DESCRIPTION {
            break;
        }
        out = next;
    }
    out
}

/// "a", "a and b", "a, b and c".
fn and_list(names: &[String]) -> String {
    match names {
        [] => String::new(),
        [one] => one.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

/// `{record} | {label} | seeded.art`, shortened to 60 characters: brand dropped, then more
/// than two artists folded, then the record cut at a word.
fn record_title(name: &str, artists: &[String], label: &str) -> (String, String) {
    let by = |list: &str| {
        if list.is_empty() {
            name.to_owned()
        } else {
            format!("{name} by {list}")
        }
    };
    let all = by(&artists.join(", "));
    let full = format!("{all} | {label}{BRAND}");
    if len(&full) <= MAX_TITLE {
        return (full, format!("{all} | {label}"));
    }
    let mut record = all;
    if len(&format!("{record} | {label}")) > MAX_TITLE && artists.len() > 2 {
        record = by(&format!("{} and {} more", artists[0], artists.len() - 1));
    }
    let suffix = format!(" | {label}");
    if len(&record) + len(&suffix) > MAX_TITLE {
        record = excerpt(&record, MAX_TITLE - len(&suffix)).unwrap_or(record);
    }
    let title = format!("{record}{suffix}");
    (title.clone(), title)
}

fn site_title(title: &str) -> (String, String) {
    let og = title.strip_suffix(BRAND).unwrap_or(title).to_owned();
    (title.to_owned(), og)
}

fn names(list: &Value) -> Vec<String> {
    list.as_array()
        .into_iter()
        .flatten()
        .filter_map(|artist| clean(&artist["name"]))
        .collect()
}

fn count(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn date(value: &Value) -> String {
    text(value).chars().take(10).collect()
}

fn image(origin: &Value, uris: &[&Value]) -> Option<String> {
    let origin = origin.as_str()?;
    uris.iter()
        .filter_map(|uri| uri.as_str())
        .find_map(|uri| super::artwork::preview_url(origin, uri))
}

/// The project name as titles show it, with a collision suffix when needed.
fn project_name(project: &Value, site: &SiteFacts) -> String {
    let title = clean(&project["title"]).unwrap_or_else(|| format!("Project {}", project["id"]));
    match project["id"]
        .as_u64()
        .and_then(|id| site.project_suffixes.get(&id))
    {
        Some(suffix) => format!("{title} {suffix}"),
        None => title,
    }
}

pub(super) fn article_name(article: &Value, site: &SiteFacts) -> String {
    let id = article["token_id"].as_u64().unwrap_or_default();
    match clean(&article["title"]) {
        Some(title) if site.article_ids.contains(&id) => format!("{title} #{id}"),
        Some(title) => title,
        None => format!("Article #{id}"),
    }
}

fn page(title: (String, String), description: String) -> Head {
    Head {
        title: title.0,
        og_title: title.1,
        description,
        og_type: "website",
        image: None,
        extra: Vec::new(),
    }
}

/// Head metadata for a page kind; `None` keeps the template's title and adds nothing.
pub(super) fn head(
    kind: &str,
    home: bool,
    query: &HashMap<String, String>,
    data: &Value,
    site: &SiteFacts,
) -> Option<Head> {
    match kind {
        "projects" if home => Some(page(
            site_title(SITE_TITLE),
            if site.projects > 0 {
                format!(
                    "Archive of fxhash generative art on Tezos: {} projects and {} NFTs, each replayable from its seed, with artist profiles and articles.",
                    count(site.projects),
                    count(site.editions)
                )
            } else {
                "Archive of fxhash generative art on Tezos: projects and NFTs, each replayable from its seed, with artist profiles and articles.".into()
            },
        )),
        "projects" => {
            let order = match text(&data["sort"]) {
                "newest" => "newest first",
                "oldest" => "oldest first",
                _ => "by volume",
            };
            let title = match clean(&data["artist"]) {
                Some(artist) => format!("fxhash projects by {artist}{BRAND}"),
                None => format!("fxhash projects, {order}{BRAND}"),
            };
            Some(page(
                site_title(&title),
                format!("fxhash projects in the archive, {order}."),
            ))
        }
        "project" => {
            let project = &data["project"];
            let artists = names(&project["artists"]);
            let name = project_name(project, site);
            let minted = project["editions"].as_u64().unwrap_or_default();
            let editions = match project["supply"].as_u64() {
                Some(supply) => format!("{} of {} NFTs", count(minted), count(supply)),
                None => format!("{} NFTs", count(minted)),
            };
            let by = if artists.is_empty() {
                String::new()
            } else {
                format!(" by {}", and_list(&artists))
            };
            let lead = format!("fxhash generative art on Tezos{by}, {editions}.");
            let description = match clean(&project["description"]) {
                Some(statement) => {
                    let budget = MAX_DESCRIPTION.saturating_sub(len(&lead) + 1);
                    match excerpt(&statement, budget) {
                        Some(part) => format!("{lead} {part}"),
                        None => lead,
                    }
                }
                None => {
                    let published = date(&data["metadata"]["published"]);
                    let mut clauses = vec![format!("fxhash generative art on Tezos{by}.")];
                    if !published.is_empty() {
                        clauses = vec![format!("fxhash generative art on Tezos{by}, published {published}.")];
                    }
                    clauses.push(format!("{editions} minted, each replayable from its seed."));
                    description(clauses)
                }
            };
            let mut head = page(record_title(&name, &artists, "fxhash project"), description);
            head.image = image(
                &data["content_origin"],
                &[&project["display_uri"], &project["thumbnail_uri"]],
            )
            .map(|url| (url, format!("{} project preview", text(&project["title"]))));
            Some(head)
        }
        "token" => {
            let (project, token) = (&data["project"], &data["token"]);
            let artists = names(&project["artists"]);
            let iteration = &token["iteration"];
            let name = format!("{} #{iteration}", project_name(project, site));
            let mut head = page(
                record_title(&name, &artists, "fxhash NFT"),
                token_description(data, &artists),
            );
            head.image = image(
                &data["content_origin"],
                &[&token["display_uri"], &token["thumbnail_uri"]],
            )
            .map(|url| {
                (
                    url,
                    format!("{} #{iteration} static NFT preview", text(&project["title"])),
                )
            });
            Some(head)
        }
        "profile" => profile_head(data),
        "article" => {
            let article = &data["article"];
            let name = article_name(article, site);
            let author = clean(&data["author_name"]).unwrap_or_else(|| text(&article["author"]).to_owned());
            let lead = format!(
                "Article by {author}, published on fxhash {}.",
                date(&article["created_at"])
            );
            let description = match clean(&article["description"]) {
                Some(summary) => match excerpt(&summary, MAX_DESCRIPTION.saturating_sub(len(&lead) + 1)) {
                    Some(part) => format!("{lead} {part}"),
                    None => lead,
                },
                None => lead,
            };
            let mut head = page(record_title(&name, &[], "fxhash article"), description);
            head.og_type = "article";
            head.image = article_image(data).map(|url| (url, name.clone()));
            if let Some(at) = article["created_at"].as_str() {
                head.extra.push(("article:published_time", at.to_owned()));
            }
            for tag in article["tags"].as_array().into_iter().flatten().filter_map(Value::as_str) {
                head.extra.push(("article:tag", tag.to_owned()));
            }
            Some(head)
        }
        "articles" => Some(page(
            site_title(&format!("fxhash articles: full text and authors{BRAND}")),
            format!(
                "Full text of {} articles published on fxhash, with author, date and links to the projects and NFTs they mention.",
                count(data["total"].as_u64().unwrap_or_default())
            ),
        )),
        "artists" => Some(page(
            site_title(&format!("fxhash artists: profiles by Tezos address{BRAND}")),
            "Artists who published generative art on fxhash, with their projects, NFTs and articles in the archive.".into(),
        )),
        "collectors" => Some(page(
            site_title(&format!("Collectors: look up a Tezos address{BRAND}")),
            "Look up any Tezos address to see the fxhash NFTs it holds in the archive.".into(),
        )),
        "about" => Some(page(
            site_title("About seeded.art"),
            "Seeded.art is an archive of NFTs rescued from fxhash after its shutdown: what is available now, what is planned, credits and contact.".into(),
        )),
        "search" => {
            let q = query.get("q").map(String::as_str).unwrap_or_default().trim();
            let shown = excerpt(q, 30).unwrap_or_default();
            Some(page(
                site_title(&format!("Search: {shown}{BRAND}")),
                format!("Search results for {shown} in the fxhash archive."),
            ))
        }
        _ => None,
    }
}

/// The traits that tell an edition apart from its siblings, rarest first.
fn token_description(data: &Value, artists: &[String]) -> String {
    let (project, token) = (&data["project"], &data["token"]);
    let minted = date(&token["minted"]);
    let features: Vec<(&str, String)> = token["features"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|pair| {
            let name = pair.get(0)?.as_str()?;
            let value = match pair.get(1)? {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            Some((name, value))
        })
        .collect();
    let share = |name: &str, value: &str| {
        data["shares"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|s| s["name"].as_str() == Some(name) && s["value"].as_str() == Some(value))
    };
    let rarest = features
        .iter()
        .filter_map(|(name, value)| {
            let s = share(name, value)?;
            let (n, total) = (s["count"].as_u64()?, s["total"].as_u64()?);
            (total > 0).then_some((n as f64 / total as f64, *name, value.as_str(), n, total))
        })
        .min_by(|a, b| a.0.total_cmp(&b.0));
    if features.is_empty() {
        let supply = project["supply"]
            .as_u64()
            .map_or_else(String::new, |s| format!(" of {}", count(s)));
        let by = if artists.is_empty() {
            String::new()
        } else {
            format!(" by {}", and_list(artists))
        };
        return format!(
            "NFT {}{supply} of {}{by}, minted {minted} on Tezos.",
            token["iteration"],
            text(&project["title"])
        );
    }
    let mut clauses = vec![format!("Minted {minted} on Tezos.")];
    let mut rest: Vec<String> = features
        .iter()
        .map(|(name, value)| format!("{name} {value}"))
        .collect();
    if let Some((_, name, value, n, total)) = rarest {
        clauses.push(format!(
            "Rarest trait: {name} {value}, {n} of {total} indexed NFTs."
        ));
        rest.retain(|trait_text| *trait_text != format!("{name} {value}"));
    }
    let mut out = description(clauses);
    if !rest.is_empty() {
        let mut others = String::new();
        for trait_text in rest {
            let candidate = if others.is_empty() {
                format!("Other traits: {trait_text}")
            } else {
                format!("{others}, {trait_text}")
            };
            if len(&out) + 1 + len(&candidate) + 1 > MAX_DESCRIPTION {
                break;
            }
            others = candidate;
        }
        if !others.is_empty() {
            out = format!("{out} {others}.");
        }
    }
    out
}

fn profile_head(data: &Value) -> Option<Head> {
    let profile = &data["profile"];
    let address = text(&profile["address"]);
    let name = clean(&profile["name"]);
    let shown = name.clone().unwrap_or_else(|| address.to_owned());
    let projects = profile["project_count"].as_u64().unwrap_or_default();
    let held = profile["held"].as_u64().unwrap_or_default();
    let mut head = if profile["kind"] != "account" && projects == 0 {
        page(
            site_title(&format!("{shown}{BRAND}")),
            format!("Tezos address {address} in the fxhash archive."),
        )
    } else if projects > 0 {
        let titles: Vec<String> = data["projects"]["items"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| clean(&p["title"]))
            .collect();
        let lead = format!(
            "{shown}, fxhash artist on Tezos: {} project{} in the archive",
            count(projects),
            if projects == 1 { "" } else { "s" }
        );
        let closing = format!("Profile for address {address}.");
        let mut listed = String::new();
        for title in titles {
            let candidate = if listed.is_empty() {
                title
            } else {
                format!("{listed}, {title}")
            };
            if len(&lead) + 2 + len(&candidate) + 2 + len(&closing) > MAX_DESCRIPTION {
                break;
            }
            listed = candidate;
        }
        let first = if listed.is_empty() {
            format!("{lead}.")
        } else {
            format!("{lead}, {listed}.")
        };
        page(
            record_title(&shown, &[], "fxhash artist"),
            description([first, closing]),
        )
    } else {
        let editions = format!(
            "{} archived NFT{}",
            count(held),
            if held == 1 { "" } else { "s" }
        );
        let lead = match &name {
            Some(name) => format!("{name}, fxhash collector on Tezos holding {editions}."),
            None => format!("fxhash collector on Tezos holding {editions}."),
        };
        page(
            record_title(&shown, &[], "fxhash collector"),
            description([lead, format!("Collection for address {address}.")]),
        )
    };
    head.og_type = "profile";
    let origin = &data["content_origin"];
    head.image = if profile["avatar_uri"].is_string() && origin.is_string() {
        Some(format!("{}/profile-media/{address}", text(origin)))
    } else {
        let first = |list: &str| data[list]["items"].get(0).cloned().unwrap_or(Value::Null);
        let item = if projects > 0 {
            first("projects")
        } else {
            first("tokens")
        };
        image(origin, &[&item["display_uri"], &item["thumbnail_uri"]])
    }
    .map(|url| (url, shown.clone()));
    Some(head)
}

/// The article thumbnail when it has one (lighter, under X's 5 MB card limit), else its hero.
fn article_image(data: &Value) -> Option<String> {
    let article = &data["article"];
    let origin = data["content_origin"].as_str().or_else(|| {
        data["media_urls"]
            .as_array()?
            .iter()
            .find_map(|m| m["url"].as_str()?.split("/article-media/").next())
    })?;
    let id = article["token_id"].as_u64()?;
    if article["thumbnail_uri"].is_string() {
        Some(format!("{origin}/article-media/{id}/0"))
    } else if article["display_uri"].is_string() {
        Some(format!("{origin}/article-media/{id}/1"))
    } else {
        None
    }
}

fn person_id(origin: &str, address: &str) -> String {
    format!("{origin}/profile/{address}#person")
}

fn person(artist: &Value, origin: &str) -> Value {
    let mut person = json!({"@type":"Person","name":artist["name"]});
    if let Some(address) = artist["address"].as_str() {
        person["@id"] = json!(person_id(origin, address));
        person["url"] = json!(format!("{origin}/profile/{address}"));
        person["identifier"] = json!(address);
    }
    person
}

fn crumbs(items: &[(&str, Option<String>)]) -> Value {
    json!({"@type":"BreadcrumbList","itemListElement":items.iter().enumerate().map(|(i, (name, url))| {
        let mut item = json!({"@type":"ListItem","position":i + 1,"name":name});
        if let Some(url) = url {
            item["item"] = json!(url);
        }
        item
    }).collect::<Vec<_>>()})
}

fn website(origin: &str) -> String {
    format!("{origin}/#website")
}

pub(super) fn organization(origin: &str) -> Value {
    json!({"@type":"Organization","@id":format!("{origin}/#organization"),"name":"seeded.art",
        "url":format!("{origin}/"),"description":ORGANIZATION,
        "founder":{"@type":"Person","@id":person_id(origin, "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL"),
            "name":"Emptyname","sameAs":"https://emptyname.org"},
        "contactPoint":{"@type":"ContactPoint","contactType":"customer support","url":"https://forum.seeded.art/"}})
}

fn artwork_image(url: Option<String>, artists: &Value, origin: &str) -> Value {
    let Some(url) = url else { return Value::Null };
    let credits = names(artists);
    json!({"@type":"ImageObject","contentUrl":url,
        "creator":artists.as_array().into_iter().flatten()
            .filter_map(|a| a["address"].as_str().map(|addr| json!({"@id":person_id(origin, addr)})))
            .collect::<Vec<_>>(),
        "creditText":credits.join(", ")})
}

/// JSON-LD for a page, or `None` for pages without structured data.
pub(super) fn structured(
    kind: &str,
    home: bool,
    data: &Value,
    origin: &str,
    canonical: &str,
    head: Option<&Head>,
) -> Option<Value> {
    let description = head.map(|h| h.description.clone());
    let graph = match kind {
        "projects" if home => vec![
            json!({"@type":"WebSite","@id":website(origin),"url":format!("{origin}/"),
                "name":"seeded.art","alternateName":"fxhash archive","description":description,
                "inLanguage":"en","publisher":{"@id":format!("{origin}/#organization")},
                "potentialAction":{"@type":"SearchAction",
                    "target":{"@type":"EntryPoint","urlTemplate":format!("{origin}/search?q={{search_term_string}}")},
                    "query-input":"required name=search_term_string"}}),
            organization(origin),
        ],
        "project" | "token" => {
            let project = &data["project"];
            let project_url = format!("{origin}/project/{}", project["id"]);
            let creators: Vec<Value> = project["artists"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|a| person(a, origin))
                .collect();
            let mut art = json!({"@type":"VisualArtwork","@id":canonical,"url":canonical,
                "artform":"Generative art","artEdition":project["supply"],"creator":creators});
            if kind == "project" {
                art["name"] = project["title"].clone();
                art["description"] = clean(&project["description"])
                    .map(|d| json!(d.chars().take(1000).collect::<String>()))
                    .unwrap_or(Value::Null);
                art["datePublished"] = data["metadata"]["published"].clone();
                art["identifier"] = json!({"@type":"PropertyValue","propertyID":"fxhash project ID","value":project["id"]});
                art["keywords"] = data["metadata"]["tags"]
                    .as_array()
                    .map(|tags| {
                        json!(
                            tags.iter()
                                .filter_map(Value::as_str)
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })
                    .unwrap_or(Value::Null);
                art["image"] = artwork_image(
                    image(
                        &data["content_origin"],
                        &[&project["display_uri"], &project["thumbnail_uri"]],
                    ),
                    &project["artists"],
                    origin,
                );
                art["thumbnailUrl"] = image(&data["content_origin"], &[&project["thumbnail_uri"]])
                    .map_or(Value::Null, Value::from);
                vec![
                    art,
                    crumbs(&[
                        ("Projects", Some(format!("{origin}/"))),
                        (text(&project["title"]), None),
                    ]),
                ]
            } else {
                let token = &data["token"];
                let name = format!("{} #{}", text(&project["title"]), token["iteration"]);
                let contract = text(&token["contract"]);
                art["name"] = json!(name);
                art["position"] = token["iteration"].clone();
                art["description"] = json!(description);
                art["dateCreated"] = token["minted"].clone();
                art["identifier"] = json!([
                    {"@type":"PropertyValue","propertyID":"Tezos token","value":format!("{contract}:{}", token["token_id"])},
                    {"@type":"PropertyValue","propertyID":"fxhash seed","value":token["seed"]},
                ]);
                art["keywords"] = json!(
                    token["features"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|pair| {
                            let value = match pair.get(1)? {
                                Value::String(s) => s.clone(),
                                other => other.to_string(),
                            };
                            Some(format!("{}: {value}", pair.get(0)?.as_str()?))
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                art["image"] = artwork_image(
                    image(
                        &data["content_origin"],
                        &[&token["display_uri"], &token["thumbnail_uri"]],
                    ),
                    &project["artists"],
                    origin,
                );
                art["thumbnailUrl"] = image(&data["content_origin"], &[&token["thumbnail_uri"]])
                    .map_or(Value::Null, Value::from);
                art["sameAs"] = json!(format!(
                    "https://objkt.com/tokens/{contract}/{}",
                    token["token_id"]
                ));
                art["isPartOf"] =
                    json!({"@type":"VisualArtwork","@id":project_url,"name":project["title"]});
                vec![
                    art,
                    crumbs(&[
                        ("Projects", Some(format!("{origin}/"))),
                        (text(&project["title"]), Some(project_url.clone())),
                        (&name, None),
                    ]),
                ]
            }
        }
        "profile" => {
            let profile = &data["profile"];
            let address = text(&profile["address"]);
            let name = profile["name"]
                .as_str()
                .map_or_else(|| profile["address"].clone(), |name| json!(name));
            let projects = profile["project_count"].as_u64().unwrap_or_default();
            if profile["kind"] != "account" && projects == 0 {
                vec![
                    json!({"@type":"Thing","@id":canonical,"url":canonical,"identifier":address,"name":name}),
                ]
            } else {
                let page_url = format!("{origin}/profile/{address}");
                let links: Vec<Value> = profile["links"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|link| link.get("url").or(Some(link)).and_then(Value::as_str))
                    .chain(profile["website"].as_str())
                    .filter(|url| url.starts_with("https://") || url.starts_with("http://"))
                    .map(Value::from)
                    .collect();
                let (section, section_url) =
                    if profile["project_count"].as_u64().unwrap_or_default() > 0 {
                        ("Artists", format!("{origin}/artists"))
                    } else {
                        ("Collectors", format!("{origin}/collectors"))
                    };
                let shown = name.as_str().unwrap_or(address).to_owned();
                vec![
                    json!({"@type":"ProfilePage","@id":page_url,"url":page_url,"name":name,
                        "mainEntity":{"@id":person_id(origin, address)}}),
                    // A contract with projects is a collaboration, not a person.
                    json!({"@type":if profile["kind"] == "account" { "Person" } else { "Organization" },
                        "@id":person_id(origin, address),"url":page_url,
                        "name":name,"identifier":address,
                        "image":head.and_then(|h| h.image.as_ref()).map(|(url, _)| url.clone()),
                        "description":profile["bio"],
                        "sameAs":if links.is_empty() { Value::Null } else { json!(links) }}),
                    crumbs(&[(section, Some(section_url)), (&shown, None)]),
                ]
            }
        }
        "article" => {
            let article = &data["article"];
            let author = text(&article["author"]);
            let mentions: Vec<Value> = data["references"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|r| {
                    match (
                        r["kind"].as_str()?,
                        r["project_id"].as_u64(),
                        r["contract"].as_str(),
                        r["token_id"].as_u64(),
                    ) {
                        ("project", Some(id), _, _) => Some(format!("{origin}/project/{id}")),
                        ("token", _, Some(contract), Some(id)) => {
                            Some(format!("{origin}/token/{contract}/{id}"))
                        }
                        _ => None,
                    }
                })
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .map(|url| json!({"@id":url}))
                .collect();
            let headline = clean(&article["title"])
                .unwrap_or_else(|| format!("Article #{}", article["token_id"]));
            vec![
                json!({"@type":"Article","@id":canonical,"url":canonical,"identifier":article["token_id"],
                    "headline":headline,"description":clean(&article["description"]),
                    "datePublished":article["created_at"],
                    "inLanguage":clean(&article["language"]),
                    "keywords":article["tags"].as_array().map(|t| json!(t.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(", "))),
                    "image":article_image(data),
                    "author":{"@type":"Person","@id":person_id(origin, author),
                        "url":format!("{origin}/profile/{author}"),"name":data["author_name"]},
                    "mentions":if mentions.is_empty() { Value::Null } else { json!(mentions) },
                    "isPartOf":{"@id":website(origin)}}),
                crumbs(&[
                    ("Articles", Some(format!("{origin}/articles"))),
                    (&headline, None),
                ]),
            ]
        }
        "artists" | "articles" => vec![
            json!({"@type":"CollectionPage","@id":canonical,"url":canonical,
            "name":head.map(|h| h.og_title.clone()),"isPartOf":{"@id":website(origin)}}),
        ],
        "about" => vec![json!({"@type":"AboutPage","@id":canonical,"url":canonical,
            "name":head.map(|h| h.og_title.clone()),"isPartOf":{"@id":website(origin)},
            "mainEntity":organization(origin)})],
        _ => return None,
    };
    let mut graph: Vec<Value> = graph
        .into_iter()
        .map(|mut node| {
            strip_nulls(&mut node);
            node
        })
        .collect();
    let value = if graph.len() == 1 {
        let mut node = graph.remove(0);
        node["@context"] = json!("https://schema.org");
        node
    } else {
        json!({"@context":"https://schema.org","@graph":graph})
    };
    Some(value)
}

fn strip_nulls(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|_, v| !v.is_null());
            map.values_mut().for_each(strip_nulls);
        }
        Value::Array(items) => items.iter_mut().for_each(strip_nulls),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn titles_stay_within_sixty_characters_and_drop_the_brand_first() {
        let artists = vec!["Doppelganger".to_owned(), "Emptyname".to_owned()];
        let (title, og) = record_title("Arboreal", &artists, "fxhash project");
        assert_eq!(
            title,
            "Arboreal by Doppelganger, Emptyname | fxhash project"
        );
        assert_eq!(og, title);
        let (title, og) = record_title("Doppelganger", &[], "fxhash artist");
        assert_eq!(title, "Doppelganger | fxhash artist | seeded.art");
        assert_eq!(og, "Doppelganger | fxhash artist");
        let many: Vec<String> = (1..=5).map(|i| format!("Artist Number {i}")).collect();
        let (title, _) = record_title("Garden", &many, "fxhash project");
        assert_eq!(
            title,
            "Garden by Artist Number 1 and 4 more | fxhash project"
        );
        let long = "An extremely long project title that keeps going well past the limit";
        let (title, _) = record_title(long, &[], "fxhash project");
        assert!(len(&title) <= MAX_TITLE, "{title}");
        assert!(title.ends_with("… | fxhash project"), "{title}");
    }

    #[test]
    fn excerpts_end_at_a_sentence_or_a_word() {
        let text = "There is no vision without thought, but it is not enough to think in order to see. More words follow here for a long while.";
        assert_eq!(
            excerpt(text, 90).unwrap(),
            "There is no vision without thought, but it is not enough to think in order to see."
        );
        let cut = excerpt(text, 50).unwrap();
        assert!(cut.ends_with('…') && len(&cut) <= 50, "{cut}");
        assert_eq!(excerpt("short", 50).unwrap(), "short");
        assert_eq!(
            and_list(&["a".into(), "b".into(), "c".into()]),
            "a, b and c"
        );
        assert_eq!(count(1_753_633), "1,753,633");
    }

    #[test]
    fn edition_descriptions_lead_with_the_rarest_trait_and_fit() {
        let data = json!({
            "project": {"title": "Arboreal", "supply": 320, "artists": []},
            "token": {"iteration": 1, "minted": "2023-11-10", "features": [
                ["Palette", "Gold"], ["Bushiness", "Normal"], ["Wind", "-0.4"], ["Twist", "4.8"],
                ["Thickness", "92"], ["Bend", "10"], ["Angularity", "1667"]]},
            "shares": [
                {"name": "Palette", "value": "Gold", "count": 1, "total": 10},
                {"name": "Bushiness", "value": "Normal", "count": 6, "total": 10}]
        });
        let text = token_description(&data, &[]);
        assert!(text.starts_with("Minted 2023-11-10 on Tezos. Rarest trait: Palette Gold, 1 of 10 indexed NFTs. Other traits: Bushiness Normal"), "{text}");
        assert!(
            len(&text) <= MAX_DESCRIPTION && text.ends_with('.'),
            "{text}"
        );
        let bare = json!({"project": {"title": "Arboreal", "supply": 320}, "token": {"iteration": 4, "minted": "2023-11-10", "features": []}});
        assert_eq!(
            token_description(&bare, &["Emptyname".into()]),
            "NFT 4 of 320 of Arboreal by Emptyname, minted 2023-11-10 on Tezos."
        );
    }

    #[test]
    fn collector_descriptions_count_editions_in_the_singular_and_plural() {
        let one = json!({"profile": {"kind": "account", "address": "tz1abc", "name": "Ann",
            "held": 1, "project_count": 0}});
        assert_eq!(
            profile_head(&one).unwrap().description,
            "Ann, fxhash collector on Tezos holding 1 archived NFT. Collection for address tz1abc."
        );
        let many = json!({"profile": {"kind": "account", "address": "tz1abc", "held": 4095,
            "project_count": 0}});
        assert!(
            profile_head(&many)
                .unwrap()
                .description
                .contains("4,095 archived NFTs")
        );
    }

    #[test]
    fn colliding_projects_and_articles_get_telling_suffixes() {
        let mut site = SiteFacts::default();
        site.project_suffixes.insert(7, "(2022)".into());
        site.article_ids.insert(1700);
        assert_eq!(
            project_name(&json!({"id": 7, "title": "Untitled"}), &site),
            "Untitled (2022)"
        );
        assert_eq!(
            project_name(&json!({"id": 8, "title": "Untitled"}), &site),
            "Untitled"
        );
        assert_eq!(
            article_name(&json!({"token_id": 1700, "title": "#9"}), &site),
            "#9 #1700"
        );
        assert_eq!(
            article_name(&json!({"token_id": 1701, "title": null}), &site),
            "Article #1701"
        );
    }
}
