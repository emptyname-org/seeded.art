//! Forwarded addresses are accepted only from the explicitly trusted loopback proxy.
use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use axum::{
    extract::{ConnectInfo, Request, State, connect_info::Connected},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    serve::IncomingStream,
};

use crate::{auth::TrustedClientIp, transport::BoundedListener};

/// Budget identity only; authentication relies on browser-bound signed challenges.
pub(crate) fn rate_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or_else(|| IpAddr::V6(Ipv6Addr::from(u128::from(ip) & (u128::MAX << 64)))),
        ip => ip,
    }
}

#[derive(Clone, Copy)]
pub struct PeerAddr(pub SocketAddr);

impl Connected<IncomingStream<'_, BoundedListener>> for PeerAddr {
    fn connect_info(stream: IncomingStream<'_, BoundedListener>) -> Self {
        Self(*stream.remote_addr())
    }
}

fn resolve(peer: IpAddr, trust_proxy: bool, headers: &HeaderMap) -> Result<IpAddr, StatusCode> {
    let peer = match peer {
        IpAddr::V6(ip) => ip.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(peer),
        ip => ip,
    };
    let ip = if trust_proxy && peer.is_loopback() {
        let mut values = headers.get_all("x-forwarded-for").iter();
        match values.next() {
            None => return Err(StatusCode::BAD_REQUEST),
            Some(value) => {
                if values.next().is_some() {
                    return Err(StatusCode::BAD_REQUEST);
                }
                value
                    .to_str()
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .ok_or(StatusCode::BAD_REQUEST)?
            }
        }
    } else {
        peer
    };
    Ok(match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
        _ => ip,
    })
}

pub(crate) fn peer_addr(request: &Request) -> Option<SocketAddr> {
    request
        .extensions()
        .get::<ConnectInfo<crate::transport::ConnectionInfo>>()
        .map(|ConnectInfo(info)| info.address)
        .or_else(|| {
            request
                .extensions()
                .get::<ConnectInfo<PeerAddr>>()
                .map(|ConnectInfo(peer)| peer.0)
        })
}

pub async fn identify(
    State(trust_proxy): State<bool>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(peer) = peer_addr(&request) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("cache-control", "no-store")],
        )
            .into_response();
    };
    let direct_health = matches!(
        *request.method(),
        axum::http::Method::GET | axum::http::Method::HEAD
    ) && matches!(request.uri().path(), "/health/live" | "/health/ready")
        && request.uri().query().is_none()
        && !request.headers().contains_key("x-forwarded-for");
    let ip = match resolve(peer.ip(), trust_proxy && !direct_health, request.headers()) {
        Ok(ip) => ip,
        Err(status) => return (status, [("cache-control", "no-store")]).into_response(),
    };
    request.headers_mut().remove("x-forwarded-for");
    request.headers_mut().remove("forwarded");
    request.extensions_mut().insert(TrustedClientIp(ip));
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, http::Request as HttpRequest, middleware, routing::get};
    use tower::ServiceExt;

    #[test]
    fn rate_identity_groups_ipv6_prefixes_without_changing_transport_identity() {
        let first = "2001:db8:1234:5678::1".parse().unwrap();
        let same = "2001:db8:1234:5678:ffff:ffff:ffff:ffff".parse().unwrap();
        let next = "2001:db8:1234:5679::1".parse().unwrap();
        assert_eq!(rate_ip(first), rate_ip(same));
        assert_ne!(rate_ip(first), rate_ip(next));
        let ipv4 = "192.0.2.7".parse().unwrap();
        assert_eq!(rate_ip("::ffff:192.0.2.7".parse().unwrap()), ipv4);
        assert_eq!(rate_ip(ipv4), ipv4);
        assert_ne!(rate_ip(ipv4), rate_ip("192.0.2.8".parse().unwrap()));
        assert_eq!(resolve(first, false, &HeaderMap::new()), Ok(first));
        let mut forwarded = HeaderMap::new();
        forwarded.insert("x-forwarded-for", same.to_string().parse().unwrap());
        assert_eq!(
            resolve("127.0.0.1".parse().unwrap(), true, &forwarded),
            Ok(same)
        );
    }

    #[test]
    fn forwarded_headers_require_explicit_loopback_trust_and_single_ip() {
        let local = "127.0.0.1".parse().unwrap();
        let remote = "198.51.100.20".parse().unwrap();
        let mut headers = HeaderMap::new();
        assert_eq!(resolve(local, true, &headers), Err(StatusCode::BAD_REQUEST));
        assert_eq!(
            resolve("::ffff:127.0.0.1".parse().unwrap(), true, &headers),
            Err(StatusCode::BAD_REQUEST)
        );
        assert_eq!(resolve(remote, true, &headers), Ok(remote));
        headers.insert("x-forwarded-for", "192.0.2.7".parse().unwrap());
        assert_eq!(resolve(local, false, &headers), Ok(local));
        assert_eq!(resolve(remote, true, &headers), Ok(remote));
        assert_eq!(
            resolve(local, true, &headers),
            Ok("192.0.2.7".parse().unwrap())
        );
        for value in [
            "192.0.2.7, 127.0.0.1",
            "unknown",
            "192.0.2.7:123",
            " 192.0.2.7",
        ] {
            headers.insert("x-forwarded-for", value.parse().unwrap());
            assert!(resolve(local, true, &headers).is_err());
        }
        headers.insert("x-forwarded-for", "::ffff:192.0.2.7".parse().unwrap());
        assert_eq!(
            resolve(local, true, &headers),
            Ok("192.0.2.7".parse().unwrap())
        );
        headers.append("x-forwarded-for", "192.0.2.8".parse().unwrap());
        assert!(resolve(local, true, &headers).is_err());
    }

    #[tokio::test]
    async fn bounded_listener_supplies_peer_for_real_http_requests() {
        for trust_proxy in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let limits = crate::request_limits::RequestLimits::new();
            let metrics = limits.metrics_handle();
            let app = crate::request_limits::protect(
                Router::new().route(
                    "/",
                    get(
                        |axum::Extension(ip): axum::Extension<TrustedClientIp>| async move {
                            ip.0.to_string()
                        },
                    ),
                ),
                limits,
                trust_proxy,
            );
            let server = tokio::spawn(async move {
                axum::serve(
                    BoundedListener::new(listener),
                    app.into_make_service_with_connect_info::<crate::transport::ConnectionInfo>(),
                )
                .await
                .unwrap();
            });
            let result = reqwest::Client::new()
                .get(format!("http://{address}/"))
                .header("x-forwarded-for", "192.0.2.12")
                .send()
                .await
                .unwrap();
            assert_eq!(result.status(), StatusCode::OK);
            assert_eq!(
                result.text().await.unwrap(),
                if trust_proxy {
                    "192.0.2.12"
                } else {
                    "127.0.0.1"
                }
            );
            assert_eq!(metrics.snapshot().missing_client_ip, 0);
            server.abort();
            assert!(server.await.unwrap_err().is_cancelled());
        }
    }

    #[tokio::test]
    async fn middleware_uses_transport_identity_and_removes_untrusted_forwarding() {
        let app = Router::new()
            .route(
                "/",
                get(|request: Request| async move {
                    assert!(!request.headers().contains_key("x-forwarded-for"));
                    assert!(!request.headers().contains_key("forwarded"));
                    assert_eq!(
                        request
                            .extensions()
                            .get::<TrustedClientIp>()
                            .unwrap()
                            .0
                            .to_string(),
                        "192.0.2.8"
                    );
                    StatusCode::NO_CONTENT
                }),
            )
            .layer(middleware::from_fn_with_state(false, identify));
        let mut request = HttpRequest::builder()
            .uri("/")
            .header("x-forwarded-for", "127.0.0.2")
            .header("forwarded", "for=127.0.0.3")
            .body(Body::empty())
            .unwrap();
        request
            .extensions_mut()
            .insert(ConnectInfo(PeerAddr("192.0.2.8:123".parse().unwrap())));
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            app.oneshot(HttpRequest::new(Body::empty()))
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn trusted_proxy_requires_one_forwarded_ip_except_exact_read_only_health() {
        let app = Router::new()
            .fallback(|request: Request| async move {
                assert_eq!(
                    request.extensions().get::<TrustedClientIp>().unwrap().0,
                    "127.0.0.1".parse::<IpAddr>().unwrap()
                );
                StatusCode::NO_CONTENT
            })
            .layer(middleware::from_fn_with_state(true, identify));
        for (method, path, forwarded, expected) in [
            ("GET", "/", None, StatusCode::BAD_REQUEST),
            ("GET", "/health/live", None, StatusCode::NO_CONTENT),
            ("HEAD", "/health/ready", None, StatusCode::NO_CONTENT),
            ("POST", "/health/live", None, StatusCode::BAD_REQUEST),
            ("GET", "/health/live?extra=1", None, StatusCode::BAD_REQUEST),
            ("GET", "/health/live/", None, StatusCode::BAD_REQUEST),
            ("GET", "/health/live", Some(""), StatusCode::BAD_REQUEST),
            (
                "GET",
                "/health/ready",
                Some("127.0.0.1, 192.0.2.1"),
                StatusCode::BAD_REQUEST,
            ),
            ("GET", "/", Some("127.0.0.1"), StatusCode::NO_CONTENT),
        ] {
            let mut request = HttpRequest::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .unwrap();
            request
                .extensions_mut()
                .insert(ConnectInfo(PeerAddr("127.0.0.1:1234".parse().unwrap())));
            if let Some(value) = forwarded {
                request
                    .headers_mut()
                    .insert("x-forwarded-for", value.parse().unwrap());
            }
            assert_eq!(
                app.clone().oneshot(request).await.unwrap().status(),
                expected,
                "{method} {path}"
            );
        }
    }
}
