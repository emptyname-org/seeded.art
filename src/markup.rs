//! Markdown from chain metadata is untrusted; only this renderer emits trusted HTML.
use pulldown_cmark::{CowStr, Event, Options, Parser, Tag, TagEnd, html};

const MAX_BYTES: usize = 262_144;

pub fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

pub fn safe_link(value: &str) -> Option<String> {
    if value.is_empty() || value.chars().any(|c| c.is_control() || c == '\\') {
        return None;
    }
    if value.starts_with('#') || (value.starts_with('/') && !value.starts_with("//")) {
        return Some(value.to_owned());
    }
    let url = reqwest::Url::parse(value).ok()?;
    if !matches!(url.scheme(), "https" | "http")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    Some(url.to_string())
}

pub fn render(text: &str) -> String {
    render_with_media(text, &|_| None)
}

/// Media URLs come from indexed article references, never directly from markdown.
pub fn render_with_media(text: &str, media: &impl Fn(&str) -> Option<String>) -> String {
    render_article(text, media, &|_| None, &|_| None)
}

/// Article bodies. `embed` may replace a plain-text paragraph line (such as an fxhash storage
/// pointer) with trusted HTML; `local` may map a link destination to a path on this site.
pub fn render_article(
    text: &str,
    media: &impl Fn(&str) -> Option<String>,
    embed: &impl Fn(&str) -> Option<String>,
    local: &impl Fn(&str) -> Option<String>,
) -> String {
    let mut end = text.len().min(MAX_BYTES);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut events = Vec::new();
    let mut links = Vec::new();
    let mut image: Option<(String, String)> = None;
    let options = Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH;
    for event in Parser::new_ext(&text[..end], options) {
        if let Some((src, alt)) = image.as_mut() {
            match event {
                Event::End(TagEnd::Image) => {
                    let value = if let Some(url) = media(src).and_then(|u| safe_link(&u)) {
                        format!(
                            "<img src=\"{}\" alt=\"{}\" loading=\"lazy\" referrerpolicy=\"no-referrer\">",
                            escape(&url),
                            escape(alt)
                        )
                    } else {
                        escape(alt)
                    };
                    events.push(Event::Html(CowStr::from(value)));
                    image = None;
                }
                Event::Text(value) | Event::Code(value) => alt.push_str(&value),
                Event::SoftBreak | Event::HardBreak => alt.push(' '),
                _ => {}
            }
            continue;
        }
        match event {
            Event::Html(_) | Event::InlineHtml(_) => {}
            Event::Start(Tag::HtmlBlock) | Event::End(TagEnd::HtmlBlock) => {}
            Event::Start(Tag::Image { dest_url, .. }) => {
                image = Some((dest_url.into_string(), String::new()))
            }
            Event::Start(Tag::Link { dest_url, .. }) => {
                let path = local(&dest_url).and_then(|path| safe_link(&path));
                let url = safe_link(&dest_url);
                links.push(path.is_some() || url.is_some());
                let value = match (path, url) {
                    (Some(path), _) => format!("<a href=\"{}\">", escape(&path)),
                    (None, Some(u)) => format!(
                        "<a href=\"{}\" rel=\"nofollow ugc noopener noreferrer\">",
                        escape(&u)
                    ),
                    (None, None) => "<span>".to_owned(),
                };
                events.push(Event::Html(value.into()));
            }
            Event::End(TagEnd::Link) => events.push(Event::Html(
                if links.pop().unwrap_or(false) {
                    "</a>"
                } else {
                    "</span>"
                }
                .into(),
            )),
            Event::Start(Tag::Heading { level, .. }) => events.push(Event::Start(Tag::Heading {
                level,
                id: None,
                classes: Vec::new(),
                attrs: Vec::new(),
            })),
            Event::SoftBreak => events.push(Event::HardBreak),
            other => events.push(other),
        }
    }
    // After filtering, so raw HTML from the markdown never reaches an embed.
    let events = with_embeds(events, embed);
    let mut output = String::new();
    html::push_html(&mut output, events.into_iter());
    if end < text.len() {
        output.push_str("<p>Text truncated.</p>");
    }
    output
}

/// Paragraphs of plain text whose lines `embed` replaces are split around the embeds.
fn with_embeds<'a>(
    events: Vec<Event<'a>>,
    embed: &impl Fn(&str) -> Option<String>,
) -> Vec<Event<'a>> {
    let mut out = Vec::with_capacity(events.len());
    let mut index = 0;
    while index < events.len() {
        if matches!(events[index], Event::Start(Tag::Paragraph)) {
            let inner_start = index + 1;
            let closing = events[inner_start..]
                .iter()
                .position(|event| matches!(event, Event::End(TagEnd::Paragraph)));
            if let Some(length) = closing {
                let inner = &events[inner_start..inner_start + length];
                // Line breaks are already hard breaks; dropped raw HTML left nothing behind.
                if inner
                    .iter()
                    .all(|event| matches!(event, Event::Text(_) | Event::HardBreak))
                {
                    let mut lines = vec![String::new()];
                    for event in inner {
                        match event {
                            Event::Text(text) => {
                                if let Some(line) = lines.last_mut() {
                                    line.push_str(text);
                                }
                            }
                            _ => lines.push(String::new()),
                        }
                    }
                    let embeds: Vec<_> = lines.iter().map(|line| embed(line.trim())).collect();
                    if embeds.iter().any(Option::is_some) {
                        let mut paragraph: Vec<Event<'a>> = Vec::new();
                        for (line, html) in lines.into_iter().zip(embeds) {
                            match html {
                                Some(html) => {
                                    flush_paragraph(&mut out, &mut paragraph);
                                    out.push(Event::Html(CowStr::from(html)));
                                }
                                None => {
                                    if !paragraph.is_empty() {
                                        paragraph.push(Event::HardBreak);
                                    }
                                    paragraph.push(Event::Text(CowStr::from(line)));
                                }
                            }
                        }
                        flush_paragraph(&mut out, &mut paragraph);
                        index = inner_start + length + 1;
                        continue;
                    }
                }
            }
        }
        out.push(events[index].clone());
        index += 1;
    }
    out
}

fn flush_paragraph<'a>(out: &mut Vec<Event<'a>>, paragraph: &mut Vec<Event<'a>>) {
    if !paragraph.is_empty() {
        out.push(Event::Start(Tag::Paragraph));
        out.append(paragraph);
        out.push(Event::End(TagEnd::Paragraph));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scripts_and_raw_html_never_execute() {
        let text = render(
            "<script>alert(1)</script>\n\n<img src=x onerror=alert(2)>\n\n**Hello** <iframe src=x></iframe>",
        );
        assert!(!text.contains("<script"));
        assert!(!text.contains("<img"));
        assert!(!text.contains("<iframe"));
        assert!(text.contains("<strong>Hello</strong>"));
    }
    #[test]
    fn unsafe_links_are_plain_text_and_instructions_keep_lines() {
        let text = render(
            "[bad](javascript:alert%281%29)\n[encoded](javascript&#58;alert)\n[ok](https://example.org/a?q=1&b=2)",
        );
        assert!(!text.contains("href=\"javascript"));
        assert!(text.contains("<br />"));
        assert!(text.contains("https://example.org/a?q=1&amp;b=2"));
        assert!(text.contains("rel=\"nofollow ugc noopener noreferrer\""));
        for url in [
            "//evil.test",
            "https://user:pass@example.org",
            "data:text/html,a",
            "/\\evil.test",
            " /x\n",
        ] {
            assert!(safe_link(url).is_none());
        }
    }
    #[test]
    fn images_require_an_indexed_media_mapping() {
        assert_eq!(render("![A](https://tracker.test/x)"), "<p>A</p>\n");
        let text = render_with_media("![A & B](ipfs://example)", &|src| {
            (src == "ipfs://example").then(|| "/article-media/1/2".into())
        });
        assert!(text.contains("src=\"/article-media/1/2\""));
        assert!(text.contains("alt=\"A &amp; B\""));
        assert!(!text.contains("ipfs://"));
    }
    #[test]
    fn embedded_lines_split_their_paragraph_and_other_text_stays_escaped() {
        let embed = |line: &str| {
            line.starts_with("::item")
                .then(|| "<div>card</div>".to_owned())
        };
        let html = render_article(
            "Intro & <b>more</b>\n::item{1}\nOutro\n\n**bold** ::item{2}",
            &|_| None,
            &embed,
            &|_| None,
        );
        assert!(
            html.starts_with("<p>Intro &amp; more</p>\n<div>card</div>\n<p>Outro</p>"),
            "{html}"
        );
        assert!(html.contains("<strong>bold</strong> ::item{2}"), "{html}");
    }

    #[test]
    fn local_paths_replace_links_without_external_rel() {
        let local = |url: &str| {
            (url == "https://www.fxhash.xyz/generative/1").then(|| "/project/1".to_owned())
        };
        let html = render_article(
            "[a](https://www.fxhash.xyz/generative/1) [b](https://example.org/)",
            &|_| None,
            &|_| None,
            &local,
        );
        assert!(html.contains("<a href=\"/project/1\">a</a>"), "{html}");
        assert!(html.contains(
            "<a href=\"https://example.org/\" rel=\"nofollow ugc noopener noreferrer\">b</a>"
        ));
    }

    #[test]
    fn unicode_is_bounded_without_panicking() {
        let text = render(&"é".repeat(MAX_BYTES));
        assert!(text.ends_with("<p>Text truncated.</p>"));
    }
}
