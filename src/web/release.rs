use crate::{
    markup::escape,
    release::{Availability, Pricing, Release},
};
use std::fmt::Write;

pub fn render(release: Option<&Release>) -> String {
    let mut html =
        String::from("<section class=\"history-block\" aria-label=\"Release\"><h2>Release</h2>");
    if let Some(release) = release {
        let status = match release.availability {
            Availability::Open => "Open",
            Availability::Paused => "Closed — issuer paused",
            Availability::Disabled => "Closed — minting disabled",
            Availability::SoldOut => "Closed — sold out",
            Availability::Scheduled => "Scheduled",
            Availability::Restricted => "Restricted mint conditions",
        };
        let pricing = match release.pricing {
            Pricing::Fixed(price) => format!("Fixed · {} tez", super::history::tez(price)),
            Pricing::Dutch => "Dutch auction".into(),
        };
        let _ = write!(
            html,
            "<dl class=\"facts\"><div><dt>Mint status</dt><dd>{status}</dd></div><div><dt>Pricing type</dt><dd>{pricing}</dd></div><div><dt>Remaining supply</dt><dd>{} of {}</dd></div>",
            release.remaining, release.supply
        );
        if let Some(opens) = &release.opens_at {
            let _ = write!(html, "<div><dt>Opens</dt><dd>{}</dd></div>", escape(opens));
        }
        let _ = write!(
            html,
            "</dl><p class=\"subtle\">Chain state at block {} · {}.</p>",
            release.level,
            escape(&release.checked_at)
        );
    } else {
        html.push_str("<p>Current mint status and pricing are unavailable. Reload to retry.</p>");
    }
    html.push_str("<p class=\"subtle\">Primary minting creates a new NFT. This site replays existing NFTs.</p></section>");
    html
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_labels_observed_conditions_without_a_dutch_price_quote() {
        let mut release = Release {
            availability: Availability::Open,
            pricing: Pricing::Fixed(20_000_001),
            remaining: 310,
            supply: 320,
            opens_at: None,
            level: 123,
            checked_at: "2026-10-05T10:00:00Z".into(),
            issuer: String::new(),
        };
        let html = render(Some(&release));
        assert!(html.contains("Fixed · 20.000001 tez"));
        assert!(html.contains("310 of 320"));
        assert!(html.contains("Chain state at block 123"));
        release.pricing = Pricing::Dutch;
        release.availability = Availability::Restricted;
        release.opens_at = Some("<unsafe>".into());
        let html = render(Some(&release));
        assert!(html.contains("Restricted mint conditions"));
        assert!(html.contains("Dutch auction"));
        assert!(!html.contains("20.000001 tez"));
        assert!(html.contains("&lt;unsafe&gt;"));
        let unavailable = render(None);
        assert!(unavailable.contains("unavailable. Reload to retry"));
        assert!(!unavailable.contains("<dd>Open</dd>"));
    }
}
