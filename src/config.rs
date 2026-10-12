use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, time::Duration};

use crate::content::{ContentConfig, DEFAULT_IPFS_GATEWAYS, KuboConfig, TZKT_MAINNET};

/// Which listeners one `serve` process runs. Split roles run as separate processes, so an
/// artwork failure or overload cannot take the application down.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Role {
    /// Both listeners in one process.
    #[default]
    All,
    /// Pages, sessions, sign-in, market and the fee-audit worker.
    App,
    /// Artwork: render, content, media and playback status.
    Content,
}

/// Read only by the application listener; the content role ignores them.
const APP_SETTINGS: &[&str] = &[
    "SEEDED_OAUTH_CLIENT_ID",
    "SEEDED_OAUTH_CLIENT_SECRET",
    "SEEDED_OAUTH_REDIRECT_URI",
    "SEEDED_TRADING_RPC",
    "SEEDED_TRADING_TZKT",
    "SEEDED_TRADING_DATABASE",
    "SEEDED_PLATFORM_FEE_BPS",
    "SEEDED_PLATFORM_FEE_ADDRESS",
];
/// Read only by the artwork listener; the app role ignores them.
const CONTENT_SETTINGS: &[&str] = &[
    "SEEDED_IPFS_GATEWAYS",
    "SEEDED_KUBO_BIN",
    "SEEDED_KUBO_PATH",
    "SEEDED_KUBO_TIMEOUT_SECS",
    "SEEDED_CACHE",
    "SEEDED_CACHE_BYTES",
    "SEEDED_MIRROR",
    "SEEDED_MIRROR_BYTES",
    "SEEDED_MAX_OBJECT_BYTES",
    "SEEDED_TZKT_TIMEOUT_SECS",
    "SEEDED_TZKT_CONCURRENCY",
];

impl Role {
    /// `--role` wins over `SEEDED_ROLE`; neither means [`Role::All`].
    pub fn resolve(flag: Option<&str>, environment: Option<&str>) -> Result<Self, ConfigError> {
        match flag.or(environment) {
            None | Some("all") => Ok(Self::All),
            Some("app") => Ok(Self::App),
            Some("content") => Ok(Self::Content),
            Some(_) => Err(ConfigError(
                "role must be all, app or content (--role or SEEDED_ROLE)".into(),
            )),
        }
    }

    pub fn serves_app(self) -> bool {
        matches!(self, Self::All | Self::App)
    }

    pub fn serves_content(self) -> bool {
        matches!(self, Self::All | Self::Content)
    }

    fn ignores(self, name: &str) -> bool {
        match self {
            Self::All => false,
            Self::App => CONTENT_SETTINGS.contains(&name),
            Self::Content => APP_SETTINGS.contains(&name),
        }
    }
}

/// Settings a role ignores keep their defaults and are not validated by that role.
#[derive(Clone, Debug)]
pub struct Config {
    pub role: Role,
    pub bind: SocketAddr,
    pub content_bind: SocketAddr,
    pub trust_loopback_proxy: bool,
    pub app_origin: String,
    pub content_origin: String,
    pub database: PathBuf,
    pub sales_database: PathBuf,
    pub articles_database: PathBuf,
    pub trading_database: PathBuf,
    pub assets: PathBuf,
    pub cache: PathBuf,
    pub cache_bytes: u64,
    /// Preservation mirror; an empty `SEEDED_MIRROR` disables it.
    pub mirror: Option<PathBuf>,
    pub mirror_bytes: u64,
    pub object_bytes: u64,
    pub assemblies: usize,
    pub tzkt_url: String,
    pub trading_rpc: String,
    pub trading_tzkt: String,
    pub platform_fee_bps: u16,
    pub platform_fee_address: Option<String>,
    pub ipfs_gateways: Vec<String>,
    pub upstream_timeout: Duration,
    pub upstream_requests: usize,
    pub backup_timeout: Duration,
    pub kubo: Option<KuboConfig>,
    /// Forum sign-in; unset disables the OAuth endpoints.
    pub oauth: Option<crate::auth::oauth::OAuthClient>,
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid configuration: {0}")]
pub struct ConfigError(String);

impl Config {
    /// Every setting, validated as for [`Role::All`].
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_env_for(Role::All)
    }

    pub fn from_env_for(role: Role) -> Result<Self, ConfigError> {
        let mut variables = BTreeMap::new();
        for (key, value) in std::env::vars_os() {
            let Ok(key) = key.into_string() else {
                continue;
            };
            if key.starts_with("SEEDED_") {
                let value = value
                    .into_string()
                    .map_err(|_| ConfigError(format!("{key} must be UTF-8")))?;
                variables.insert(key, value);
            }
        }
        Self::from_values_for(&variables, role)
    }

    pub fn from_values(values: &BTreeMap<String, String>) -> Result<Self, ConfigError> {
        Self::from_values_for(values, Role::All)
    }

    pub fn from_values_for(
        values: &BTreeMap<String, String>,
        role: Role,
    ) -> Result<Self, ConfigError> {
        let values: &BTreeMap<String, String> = &values
            .iter()
            .filter(|(name, _)| !role.ignores(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        let value = |name: &str, fallback: &str| {
            values.get(name).cloned().unwrap_or_else(|| fallback.into())
        };
        let bind: SocketAddr = value("SEEDED_BIND", "127.0.0.1:3918")
            .parse()
            .map_err(|_| ConfigError("SEEDED_BIND must be an IP address and port".into()))?;
        let content_bind: SocketAddr = value("SEEDED_CONTENT_BIND", "127.0.0.1:3919")
            .parse()
            .map_err(|_| {
                ConfigError("SEEDED_CONTENT_BIND must be an IP address and port".into())
            })?;
        if !bind.ip().is_loopback()
            || !content_bind.ip().is_loopback()
            || bind == content_bind
            || bind.port() == 0
            || content_bind.port() == 0
        {
            return Err(ConfigError(
                "listeners must use different loopback addresses".into(),
            ));
        }
        let app = origin(&value("SEEDED_APP_ORIGIN", &format!("http://{bind}")))?;
        let trust_loopback_proxy = match value("SEEDED_TRUST_LOOPBACK_PROXY", "false").as_str() {
            "true" => true,
            "false" => false,
            _ => {
                return Err(ConfigError(
                    "SEEDED_TRUST_LOOPBACK_PROXY must be true or false".into(),
                ));
            }
        };
        let content = origin(&value(
            "SEEDED_CONTENT_ORIGIN",
            &format!("http://{content_bind}"),
        ))?;
        if app.origin() == content.origin() {
            return Err(ConfigError(
                "artwork and application origins must differ".into(),
            ));
        }
        if !(loopback_host(&app) && loopback_host(&content))
            && (app.scheme() != "https"
                || content.scheme() != "https"
                || app.host_str() == content.host_str())
        {
            return Err(ConfigError(
                "deployed origins require distinct HTTPS hosts".into(),
            ));
        }
        let number = |name: &str, fallback: u64, maximum: u64| -> Result<u64, ConfigError> {
            let n = value(name, &fallback.to_string())
                .parse::<u64>()
                .map_err(|_| ConfigError(format!("{name} must be an unsigned integer")))?;
            if n > maximum {
                return Err(ConfigError(format!("{name} exceeds its supported limit")));
            }
            Ok(n)
        };
        let object_bytes = number("SEEDED_MAX_OBJECT_BYTES", 32 << 20, 256 << 20)?;
        let assemblies = number("SEEDED_CONTENT_CONCURRENCY", 4, 16)? as usize;
        if object_bytes == 0 || assemblies == 0 {
            return Err(ConfigError(
                "object and concurrency limits must be positive".into(),
            ));
        }
        let tzkt_url = upstream(&value("SEEDED_TZKT_URL", TZKT_MAINNET), false)?;
        let trading_rpc = upstream(
            &value("SEEDED_TRADING_RPC", "https://rpc.tzkt.io/mainnet"),
            false,
        )?;
        let trading_tzkt = upstream(
            &value("SEEDED_TRADING_TZKT", "https://api.tzkt.io/v1"),
            false,
        )?;
        let platform_fee_bps = number(
            "SEEDED_PLATFORM_FEE_BPS",
            250,
            u64::from(crate::trading::types::MAX_PLATFORM_FEE_BPS),
        )
        .map_err(|_| {
            ConfigError("SEEDED_PLATFORM_FEE_BPS must be an integer from 0 to 1000 (0–10%)".into())
        })? as u16;
        let platform_fee_address = values
            .get("SEEDED_PLATFORM_FEE_ADDRESS")
            .filter(|address| !address.trim().is_empty())
            .cloned();
        if platform_fee_address
            .as_deref()
            .is_some_and(|address| !crate::trading::types::valid_fee_address(address))
        {
            return Err(ConfigError(
                "SEEDED_PLATFORM_FEE_ADDRESS must be a valid tz1/tz2/tz3/KT1 address without an entrypoint"
                    .into(),
            ));
        }
        for endpoint in [&trading_rpc, &trading_tzkt] {
            let url = reqwest::Url::parse(endpoint)
                .map_err(|_| ConfigError("invalid trading endpoint".into()))?;
            if url.scheme() != "https" && !(app.scheme() == "http" && loopback_host(&url)) {
                return Err(ConfigError(
                    "trading endpoints require HTTPS or development loopback".into(),
                ));
            }
        }
        let ipfs_gateways = match values.get("SEEDED_IPFS_GATEWAYS") {
            None => DEFAULT_IPFS_GATEWAYS
                .iter()
                .map(|value| value.to_string())
                .collect(),
            Some(list) => {
                let gateways = list
                    .split(',')
                    .filter(|entry| !entry.trim().is_empty())
                    .map(|entry| upstream(entry.trim(), true))
                    .collect::<Result<Vec<_>, _>>()?;
                if gateways.len() > 8 {
                    return Err(ConfigError(
                        "at most eight IPFS gateways are supported".into(),
                    ));
                }
                gateways
            }
        };
        let timeout = number("SEEDED_TZKT_TIMEOUT_SECS", 30, 300)?;
        let requests = number("SEEDED_TZKT_CONCURRENCY", 4, 64)?;
        let kubo_timeout = number("SEEDED_KUBO_TIMEOUT_SECS", 3, 30)?;
        let backup_timeout = number("SEEDED_BACKUP_TIMEOUT_SECS", 300, 3600)?;
        if timeout == 0 || requests == 0 || kubo_timeout == 0 || backup_timeout == 0 {
            return Err(ConfigError("upstream limits must be positive".into()));
        }
        if !(loopback_host(&app) && loopback_host(&content))
            && std::iter::once(&tzkt_url)
                .chain(&ipfs_gateways)
                .any(|url| !url.starts_with("https://"))
        {
            return Err(ConfigError("deployed upstreams require HTTPS".into()));
        }
        let kubo = values
            .get("SEEDED_KUBO_BIN")
            .map(|binary| {
                if binary.is_empty() || value("SEEDED_KUBO_PATH", "var/ipfs").is_empty() {
                    return Err(ConfigError(
                        "Kubo executable and repository must be nonempty".into(),
                    ));
                }
                Ok(KuboConfig {
                    binary: PathBuf::from(binary),
                    ipfs_path: PathBuf::from(value("SEEDED_KUBO_PATH", "var/ipfs")),
                    timeout: Duration::from_secs(kubo_timeout),
                })
            })
            .transpose()?;
        let oauth = oauth_client(values)?;
        Ok(Self {
            role,
            bind,
            content_bind,
            trust_loopback_proxy,
            app_origin: app.origin().ascii_serialization(),
            content_origin: content.origin().ascii_serialization(),
            database: PathBuf::from(value("SEEDED_DATABASE", "var/seeded.sqlite")),
            sales_database: PathBuf::from(value("SEEDED_SALES_DATABASE", "var/sales.sqlite")),
            articles_database: PathBuf::from(value(
                "SEEDED_ARTICLES_DATABASE",
                "var/articles.sqlite",
            )),
            trading_database: PathBuf::from(value("SEEDED_TRADING_DATABASE", "var/trading.sqlite")),
            assets: PathBuf::from(value("SEEDED_ASSETS", "assets")),
            cache: PathBuf::from(value("SEEDED_CACHE", "var/cache")),
            cache_bytes: number("SEEDED_CACHE_BYTES", 2 << 30, 1 << 50)?,
            mirror: Some(value("SEEDED_MIRROR", "var/mirror"))
                .filter(|path| !path.is_empty())
                .map(PathBuf::from),
            mirror_bytes: number("SEEDED_MIRROR_BYTES", 50 << 30, 1 << 50)?,
            object_bytes,
            assemblies,
            trading_rpc,
            trading_tzkt,
            platform_fee_bps,
            platform_fee_address,
            tzkt_url,
            ipfs_gateways,
            upstream_timeout: Duration::from_secs(timeout),
            upstream_requests: requests as usize,
            backup_timeout: Duration::from_secs(backup_timeout),
            kubo,
            oauth,
        })
    }

    pub fn content_config(&self) -> ContentConfig {
        let mut config = ContentConfig::mainnet(&self.cache);
        config.max_cache_bytes = self.cache_bytes;
        config.mirror_dir = self.mirror.clone();
        config.max_object_bytes = self.object_bytes;
        config.max_concurrent_assemblies = self.assemblies;
        config.tzkt_base = self.tzkt_url.clone();
        if let Ok(mut base) = reqwest::Url::parse(&self.tzkt_url) {
            if let Some(path) = base.path().strip_suffix("/v1").map(str::to_owned) {
                base.set_path(&path);
            }
            config.tzkt_base = base.as_str().trim_end_matches('/').to_owned();
        }
        config.ipfs_gateways = self.ipfs_gateways.clone();
        config.request_timeout = self.upstream_timeout;
        config.max_concurrent_requests = self.upstream_requests;
        config.kubo = self.kubo.clone();
        config
    }
}

fn oauth_client(
    values: &BTreeMap<String, String>,
) -> Result<Option<crate::auth::oauth::OAuthClient>, ConfigError> {
    let names = [
        "SEEDED_OAUTH_CLIENT_ID",
        "SEEDED_OAUTH_CLIENT_SECRET",
        "SEEDED_OAUTH_REDIRECT_URI",
    ];
    let set: Vec<_> = names
        .iter()
        .map(|name| values.get(*name).filter(|value| !value.trim().is_empty()))
        .collect();
    match set.as_slice() {
        [None, None, None] => Ok(None),
        [Some(id), Some(secret), Some(redirect)] => {
            crate::auth::oauth::OAuthClient::new(id, secret, redirect)
                .map(Some)
                .map_err(ConfigError)
        }
        _ => Err(ConfigError(format!(
            "set all of {} or none",
            names.join(", ")
        ))),
    }
}

fn upstream(text: &str, gateway: bool) -> Result<String, ConfigError> {
    let invalid = || {
        ConfigError("upstreams require HTTP(S) without credentials, query or fragment; gateways end in /ipfs/".into())
    };
    let url = reqwest::Url::parse(text).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || text.chars().any(char::is_control)
        || text.contains('\\')
        || (gateway && !url.path().ends_with("/ipfs/"))
    {
        return Err(invalid());
    }
    Ok(if gateway {
        url.to_string()
    } else {
        url.as_str().trim_end_matches('/').to_string()
    })
}

fn origin(text: &str) -> Result<reqwest::Url, ConfigError> {
    let invalid = || {
        ConfigError(
            "origins must be absolute HTTP(S) without credentials, path, query or fragment".into(),
        )
    };
    let url = reqwest::Url::parse(text).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || text.chars().any(char::is_control)
        || text.contains('\\')
    {
        return Err(invalid());
    }
    Ok(url)
}

fn loopback_host(url: &reqwest::Url) -> bool {
    url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(values: &[(&str, &str)]) -> Result<Config, ConfigError> {
        Config::from_values(
            &values
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
    }
    #[test]
    fn defaults_keep_artwork_separate_and_bound_storage() {
        let config = config(&[]).unwrap();
        assert_ne!(config.app_origin, config.content_origin);
        assert_eq!(config.cache_bytes, 2 << 30);
        assert_eq!(config.mirror, Some(PathBuf::from("var/mirror")));
        assert_eq!(config.mirror_bytes, 50 << 30);
        assert_eq!(
            config.content_config().mirror_dir,
            Some(PathBuf::from("var/mirror"))
        );
        let unmirrored = super::Config::from_values(
            &[("SEEDED_MIRROR".to_string(), String::new())]
                .into_iter()
                .collect(),
        )
        .unwrap();
        assert_eq!(unmirrored.content_config().mirror_dir, None);
        assert_eq!(config.assemblies, 4);
        assert!(!config.trust_loopback_proxy);
        assert_eq!(config.platform_fee_bps, 250);
        assert_eq!(config.platform_fee_address, None);
        assert_eq!(config.trading_database, PathBuf::from("var/trading.sqlite"));
    }

    fn config_for(role: Role, values: &[(&str, &str)]) -> Result<Config, ConfigError> {
        Config::from_values_for(
            &values
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            role,
        )
    }

    #[test]
    fn role_flag_wins_over_environment_and_defaults_to_all() {
        assert_eq!(Role::resolve(None, None).unwrap(), Role::All);
        assert_eq!(Role::resolve(None, Some("app")).unwrap(), Role::App);
        assert_eq!(
            Role::resolve(Some("content"), Some("app")).unwrap(),
            Role::Content
        );
        assert_eq!(Role::resolve(Some("all"), Some("app")).unwrap(), Role::All);
        for bad in ["", "APP", "both", "app,content"] {
            assert!(Role::resolve(Some(bad), None).is_err(), "{bad:?}");
            assert!(Role::resolve(None, Some(bad)).is_err(), "{bad:?}");
        }
        assert!(Role::All.serves_app() && Role::All.serves_content());
        assert!(Role::App.serves_app() && !Role::App.serves_content());
        assert!(!Role::Content.serves_app() && Role::Content.serves_content());
        assert_eq!(config(&[]).unwrap().role, Role::All);
        assert_eq!(config_for(Role::App, &[]).unwrap().role, Role::App);
    }

    #[test]
    fn each_role_validates_only_its_own_settings() {
        let app_only = [
            vec![("SEEDED_OAUTH_CLIENT_ID", "forum")],
            vec![("SEEDED_PLATFORM_FEE_ADDRESS", "tz1invalid")],
            vec![("SEEDED_PLATFORM_FEE_BPS", "5000")],
            vec![("SEEDED_TRADING_RPC", "http://example.org")],
        ];
        let content_only = [
            vec![("SEEDED_IPFS_GATEWAYS", "https://example.org/wrong")],
            vec![("SEEDED_KUBO_BIN", "ipfs"), ("SEEDED_KUBO_PATH", "")],
            vec![("SEEDED_CACHE_BYTES", "x")],
            vec![("SEEDED_MAX_OBJECT_BYTES", "999999999999")],
            vec![("SEEDED_TZKT_CONCURRENCY", "0")],
        ];
        for values in &app_only {
            assert!(config_for(Role::All, values).is_err(), "{values:?}");
            assert!(config_for(Role::App, values).is_err(), "{values:?}");
            assert!(config_for(Role::Content, values).is_ok(), "{values:?}");
        }
        for values in &content_only {
            assert!(config_for(Role::All, values).is_err(), "{values:?}");
            assert!(config_for(Role::Content, values).is_err(), "{values:?}");
            assert!(config_for(Role::App, values).is_ok(), "{values:?}");
        }
        for role in [Role::All, Role::App, Role::Content] {
            for values in [
                vec![("SEEDED_CONTENT_ORIGIN", "http://127.0.0.1:3918/")],
                vec![("SEEDED_BIND", "0.0.0.0:3918")],
                vec![("SEEDED_TZKT_URL", "ftp://example.org")],
                vec![("SEEDED_CONTENT_CONCURRENCY", "0")],
                vec![("SEEDED_TRUST_LOOPBACK_PROXY", "1")],
            ] {
                assert!(config_for(role, &values).is_err(), "{role:?} {values:?}");
            }
        }
        let content = config_for(
            Role::Content,
            &[
                ("SEEDED_CACHE", "/state/cache"),
                ("SEEDED_TRADING_DATABASE", "/state/trading.sqlite"),
            ],
        )
        .unwrap();
        assert_eq!(content.cache, PathBuf::from("/state/cache"));
        assert_eq!(
            content.trading_database,
            PathBuf::from("var/trading.sqlite")
        );
        let app = config_for(
            Role::App,
            &[
                ("SEEDED_CACHE", "/state/cache"),
                ("SEEDED_TRADING_DATABASE", "/state/trading.sqlite"),
            ],
        )
        .unwrap();
        assert_eq!(app.cache, PathBuf::from("var/cache"));
        assert_eq!(app.trading_database, PathBuf::from("/state/trading.sqlite"));
    }

    #[test]
    fn oauth_client_needs_all_three_settings_or_none() {
        assert!(config(&[]).unwrap().oauth.is_none());
        let all = [
            ("SEEDED_OAUTH_CLIENT_ID", "forum"),
            ("SEEDED_OAUTH_CLIENT_SECRET", "0123456789abcdef"),
            (
                "SEEDED_OAUTH_REDIRECT_URI",
                "https://forum.example/auth/seeded",
            ),
        ];
        let client = config(&all).unwrap().oauth.unwrap();
        assert_eq!(client.client_id(), "forum");
        assert_eq!(client.redirect_uri(), "https://forum.example/auth/seeded");
        for missing in 0..3 {
            let partial: Vec<_> = all
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != missing)
                .map(|(_, pair)| *pair)
                .collect();
            assert!(config(&partial).is_err(), "{partial:?}");
        }
        assert!(config(&[all[0]]).is_err());
        assert!(
            config(&[
                all[0],
                all[1],
                (
                    "SEEDED_OAUTH_REDIRECT_URI",
                    "http://forum.example/auth/seeded"
                )
            ])
            .is_err()
        );
    }

    #[test]
    fn trading_database_path_can_be_configured() {
        assert_eq!(
            config(&[("SEEDED_TRADING_DATABASE", "/state/trading.sqlite")])
                .unwrap()
                .trading_database,
            PathBuf::from("/state/trading.sqlite")
        );
    }

    #[test]
    fn platform_fee_configuration_accepts_valid_addresses_and_bounded_rates() {
        for address in [
            "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL",
            "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u",
        ] {
            assert_eq!(
                config(&[("SEEDED_PLATFORM_FEE_ADDRESS", address)])
                    .unwrap()
                    .platform_fee_address
                    .as_deref(),
                Some(address)
            );
        }
        assert_eq!(
            config(&[("SEEDED_PLATFORM_FEE_ADDRESS", "  ")])
                .unwrap()
                .platform_fee_address,
            None
        );
        for (rate, expected) in [("0", 0), ("1000", 1000), ("250", 250)] {
            assert_eq!(
                config(&[("SEEDED_PLATFORM_FEE_BPS", rate)])
                    .unwrap()
                    .platform_fee_bps,
                expected
            );
        }
        for rate in ["1001", "2500", "10000", "-1", "x"] {
            assert!(config(&[("SEEDED_PLATFORM_FEE_BPS", rate)]).is_err());
        }
        for address in [
            "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEM",
            "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u%default",
        ] {
            assert!(config(&[("SEEDED_PLATFORM_FEE_ADDRESS", address)]).is_err());
        }
    }
    #[test]
    fn proxy_identity_requires_explicit_boolean_configuration() {
        assert!(
            config(&[("SEEDED_TRUST_LOOPBACK_PROXY", "true")])
                .unwrap()
                .trust_loopback_proxy
        );
        assert!(
            !config(&[("SEEDED_TRUST_LOOPBACK_PROXY", "false")])
                .unwrap()
                .trust_loopback_proxy
        );
        for value in ["", "1", "on", "TRUE", "false,true"] {
            assert!(config(&[("SEEDED_TRUST_LOOPBACK_PROXY", value)]).is_err());
        }
    }
    #[test]
    fn trading_never_uses_public_plaintext_or_deployed_loopback_rpc() {
        assert!(config(&[("SEEDED_TRADING_RPC", "http://example.org")]).is_err());
        assert!(config(&[("SEEDED_TRADING_RPC", "http://127.0.0.1:1234")]).is_ok());
        assert!(
            config(&[
                ("SEEDED_APP_ORIGIN", "https://site.example"),
                ("SEEDED_CONTENT_ORIGIN", "https://art.example"),
                ("SEEDED_TRADING_RPC", "http://127.0.0.1:1234")
            ])
            .is_err()
        );
    }

    #[test]
    fn same_origin_aliases_and_public_plaintext_are_rejected() {
        for values in [
            vec![("SEEDED_CONTENT_ORIGIN", "http://127.0.0.1:3918/")],
            vec![
                ("SEEDED_APP_ORIGIN", "https://site.example"),
                ("SEEDED_CONTENT_ORIGIN", "https://SITE.EXAMPLE:443/"),
            ],
            vec![("SEEDED_APP_ORIGIN", "http://site.example")],
            vec![("SEEDED_CONTENT_ORIGIN", "https://content.example/path")],
            vec![("SEEDED_BIND", "0.0.0.0:3918")],
            vec![("SEEDED_CONTENT_CONCURRENCY", "0")],
            vec![("SEEDED_MAX_OBJECT_BYTES", "999999999999")],
        ] {
            assert!(config(&values).is_err(), "{values:?}");
        }
        assert!(
            config(&[
                ("SEEDED_APP_ORIGIN", "https://seeded.example"),
                ("SEEDED_CONTENT_ORIGIN", "https://art.example")
            ])
            .is_ok()
        );
    }

    #[test]
    fn upstream_configuration_keeps_kubo_in_a_named_repository() {
        assert!(config(&[("SEEDED_KUBO_BIN", "ipfs"), ("SEEDED_KUBO_PATH", "")]).is_err());
        assert!(config(&[("SEEDED_TZKT_URL", "https://person:secret@example.org")]).is_err());
        assert!(config(&[("SEEDED_IPFS_GATEWAYS", "https://example.org/wrong")]).is_err());
        let config = config(&[
            ("SEEDED_KUBO_BIN", "ipfs"),
            ("SEEDED_KUBO_PATH", "var/isolated-ipfs"),
            ("SEEDED_IPFS_GATEWAYS", ""),
            ("SEEDED_BIND", "127.0.0.1:4000"),
        ])
        .unwrap();
        assert_eq!(config.app_origin, "http://127.0.0.1:4000");
        let content = config.content_config();
        assert!(content.ipfs_gateways.is_empty());
        assert_eq!(
            content.kubo.unwrap().ipfs_path,
            PathBuf::from("var/isolated-ipfs")
        );
    }

    #[test]
    fn content_accepts_tzkt_base_with_or_without_api_version() {
        for base in [
            "https://api.tzkt.io",
            "https://api.tzkt.io/v1",
            "https://api.tzkt.io/v1/",
        ] {
            assert_eq!(
                config(&[("SEEDED_TZKT_URL", base)])
                    .unwrap()
                    .content_config()
                    .tzkt_base,
                "https://api.tzkt.io"
            );
        }
        for (base, expected) in [
            ("https://v1", "https://v1"),
            (
                "https://example.org/prefix/v1/v1",
                "https://example.org/prefix/v1",
            ),
            ("https://example.org/v10", "https://example.org/v10"),
        ] {
            assert_eq!(
                config(&[("SEEDED_TZKT_URL", base)])
                    .unwrap()
                    .content_config()
                    .tzkt_base,
                expected
            );
        }
    }

    #[test]
    fn deployed_upstreams_require_tls_and_backup_and_kubo_budgets_are_independent() {
        let mut values = vec![
            ("SEEDED_APP_ORIGIN", "https://seeded.example"),
            ("SEEDED_CONTENT_ORIGIN", "https://art.example"),
        ];
        values.push(("SEEDED_TZKT_URL", "http://api.example"));
        assert!(config(&values).is_err());
        values.pop();
        values.push(("SEEDED_IPFS_GATEWAYS", "http://gateway.example/ipfs/"));
        assert!(config(&values).is_err());
        let local = config(&[
            ("SEEDED_TZKT_URL", "http://127.0.0.1:4000"),
            ("SEEDED_KUBO_BIN", "ipfs"),
            ("SEEDED_TZKT_TIMEOUT_SECS", "60"),
            ("SEEDED_BACKUP_TIMEOUT_SECS", "600"),
        ])
        .unwrap();
        assert_eq!(local.backup_timeout, Duration::from_secs(600));
        assert_eq!(local.kubo.unwrap().timeout, Duration::from_secs(3));
        for (key, value) in [
            ("SEEDED_BACKUP_TIMEOUT_SECS", "0"),
            ("SEEDED_BACKUP_TIMEOUT_SECS", "3601"),
            ("SEEDED_KUBO_TIMEOUT_SECS", "0"),
            ("SEEDED_KUBO_TIMEOUT_SECS", "31"),
        ] {
            assert!(config(&[(key, value)]).is_err());
        }
    }
}
