use super::*;
use ed25519_dalek::Signer;
use k256::ecdsa::signature::hazmat::PrehashSigner;

const ORIGIN: &str = "https://seeded.art";

fn encoded(prefix: &[u8], bytes: &[u8]) -> String {
    bs58::encode([prefix, bytes].concat())
        .with_check()
        .into_string()
}
fn key(kind: usize) -> String {
    match kind {
        0 => encoded(
            &[13, 15, 37, 217],
            ed25519_dalek::SigningKey::from_bytes(&[7; 32])
                .verifying_key()
                .as_bytes(),
        ),
        1 => encoded(
            &[3, 254, 226, 86],
            k256::ecdsa::SigningKey::from_bytes((&[7; 32]).into())
                .unwrap()
                .verifying_key()
                .to_encoded_point(true)
                .as_bytes(),
        ),
        _ => encoded(
            &[3, 178, 139, 127],
            p256::ecdsa::SigningKey::from_bytes((&[7; 32]).into())
                .unwrap()
                .verifying_key()
                .to_encoded_point(true)
                .as_bytes(),
        ),
    }
}
fn sign(kind: usize, payload: &str) -> String {
    let hash = crypto::blake::<32>(&hex::decode(payload).unwrap());
    match kind {
        0 => encoded(
            &[9, 245, 205, 134, 18],
            &ed25519_dalek::SigningKey::from_bytes(&[7; 32])
                .sign(&hash)
                .to_bytes(),
        ),
        1 => {
            let signature: k256::ecdsa::Signature =
                k256::ecdsa::SigningKey::from_bytes((&[7; 32]).into())
                    .unwrap()
                    .sign_prehash(&hash)
                    .unwrap();
            encoded(&[13, 115, 101, 19, 63], &signature.to_bytes())
        }
        _ => {
            let signature: p256::ecdsa::Signature =
                p256::ecdsa::SigningKey::from_bytes((&[7; 32]).into())
                    .unwrap()
                    .sign_prehash(&hash)
                    .unwrap();
            encoded(&[54, 240, 44, 52], &signature.to_bytes())
        }
    }
}
fn headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(header::ORIGIN, ORIGIN.parse().unwrap());
    headers
}
fn issue(auth: &AuthState, kind: usize) -> (ChallengeGrant, HeaderMap) {
    let public_key = key(kind);
    let grant = auth
        .challenge(
            &headers(),
            SignInRequest {
                address: crypto::public_key_address(&public_key).unwrap(),
                public_key,
            },
        )
        .unwrap();
    let mut headers = headers();
    headers.insert(
        header::COOKIE,
        grant.cookie.split(';').next().unwrap().parse().unwrap(),
    );
    headers.insert("x-csrf-token", grant.view.csrf.parse().unwrap());
    (grant, headers)
}
async fn signin(auth: &AuthState, kind: usize) -> (SessionGrant, HeaderMap) {
    let (challenge, headers) = issue(auth, kind);
    let grant = auth
        .verify(
            &headers,
            VerifyRequest {
                signature: sign(kind, &challenge.view.payload),
            },
        )
        .await
        .unwrap();
    let mut headers = super::tests::headers();
    headers.insert(
        header::COOKIE,
        grant.cookie.split(';').next().unwrap().parse().unwrap(),
    );
    headers.insert("x-csrf-token", grant.identity.csrf_token().parse().unwrap());
    (grant, headers)
}

#[test]
fn three_curves_verify_real_signatures_and_bind_key_address_and_message() {
    let payload = crypto::packed_message("Tezos Signed Message: https://seeded.art fixture");
    for kind in 0..3 {
        let key = key(kind);
        let address = crypto::public_key_address(&key).unwrap();
        assert!(crate::store::profile::public_key_matches_address(
            &key, &address
        ));
        let signature = sign(kind, &payload);
        assert!(crypto::verify_signature(&address, &key, &payload, &signature).is_ok());
        assert!(
            crypto::verify_signature(
                &address,
                &key,
                &crypto::packed_message("different"),
                &signature
            )
            .is_err()
        );
        assert!(
            crypto::verify_signature(&format!("{}x", &address[..35]), &key, &payload, &signature)
                .is_err()
        );
        let raw = bs58::decode(&signature)
            .with_check(None)
            .into_vec()
            .unwrap();
        let generic = encoded(&[4, 130, 43], &raw[raw.len() - 64..]);
        assert!(crypto::verify_signature(&address, &key, &payload, &generic).is_ok());
        let wrong = encoded(&[54, 240, 44, 52], &raw[raw.len() - 64..]);
        if kind != 2 {
            assert!(crypto::verify_signature(&address, &key, &payload, &wrong).is_err());
        }
    }
}

#[test]
fn malformed_checksums_keys_signatures_and_packing_fail_closed() {
    let key = key(0);
    let address = crypto::public_key_address(&key).unwrap();
    let payload = crypto::packed_message("small");
    for bad in [
        String::new(),
        "x".repeat(129),
        key[..key.len() - 1].into(),
        encoded(&[13, 15, 37, 217], &[0; 31]),
    ] {
        assert!(crypto::public_key_address(&bad).is_err());
    }
    for bad in [
        "".into(),
        "00".into(),
        "050100000002ff".into(),
        "ab".repeat(4097),
    ] {
        assert!(crypto::verify_signature(&address, &key, &bad, &sign(0, &payload)).is_err());
    }
    let weak = encoded(&[13, 15, 37, 217], &[0; 32]);
    assert!(
        crypto::verify_signature(
            &crypto::public_key_address(&weak).unwrap(),
            &weak,
            &payload,
            &encoded(&[9, 245, 205, 134, 18], &[0; 64])
        )
        .is_err()
    );
}

#[tokio::test]
async fn one_time_challenge_is_bound_to_cookie_origin_and_csrf() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (challenge, headers) = issue(&auth, 0);
    assert!(challenge.view.message.contains(ORIGIN));
    assert!(
        challenge
            .view
            .message
            .contains("does not authorize a transaction")
    );
    let signature = sign(0, &challenge.view.payload);
    let mut bad = headers.clone();
    bad.insert(header::ORIGIN, "https://other.example".parse().unwrap());
    assert!(matches!(
        auth.verify(
            &bad,
            VerifyRequest {
                signature: signature.clone()
            }
        )
        .await,
        Err(AuthError::Forbidden)
    ));
    let mut bad = headers.clone();
    bad.remove("x-csrf-token");
    assert!(matches!(
        auth.verify(
            &bad,
            VerifyRequest {
                signature: signature.clone()
            }
        )
        .await,
        Err(AuthError::Forbidden)
    ));
    let mut bad = headers.clone();
    bad.remove(header::COOKIE);
    assert!(matches!(
        auth.verify(
            &bad,
            VerifyRequest {
                signature: signature.clone()
            }
        )
        .await,
        Err(AuthError::Expired)
    ));
    assert!(
        auth.verify(
            &headers,
            VerifyRequest {
                signature: signature.clone()
            }
        )
        .await
        .is_ok()
    );
    assert!(matches!(
        auth.verify(&headers, VerifyRequest { signature }).await,
        Err(AuthError::Expired)
    ));
}

#[tokio::test]
async fn racing_replies_issue_exactly_one_session() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (challenge, headers) = issue(&auth, 1);
    let a = VerifyRequest {
        signature: sign(1, &challenge.view.payload),
    };
    let b = VerifyRequest {
        signature: a.signature.clone(),
    };
    let (a, b) = tokio::join!(auth.verify(&headers, a), auth.verify(&headers, b));
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(auth.lock().unwrap().sessions.len(), 1);
}

#[tokio::test]
async fn invalid_signature_keeps_challenge_retryable_without_allocating_an_account() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (challenge, headers) = issue(&auth, 2);
    assert!(matches!(
        auth.verify(
            &headers,
            VerifyRequest {
                signature: sign(2, &crypto::packed_message("wrong"))
            }
        )
        .await,
        Err(AuthError::InvalidSignature)
    ));
    assert!(auth.lock().unwrap().accounts.is_empty());
    auth.verify(
        &headers,
        VerifyRequest {
            signature: sign(2, &challenge.view.payload),
        },
    )
    .await
    .unwrap();
    assert!(
        signin(&auth, 2)
            .await
            .0
            .identity
            .address()
            .starts_with("tz3")
    );
}

#[tokio::test]
async fn expired_challenges_and_sessions_cannot_authenticate() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (challenge, mut headers) = issue(&auth, 0);
    let bearer = cookie_value(&headers, auth.challenge_name())
        .unwrap()
        .unwrap();
    let mut expired = Challenge::open(&bearer, &auth.challenge_secret, ORIGIN).unwrap();
    expired.deadline = 0;
    let cookie = expired.seal(&auth.challenge_secret, ORIGIN).unwrap();
    headers.insert(
        header::COOKIE,
        format!("{}={cookie}", auth.challenge_name())
            .parse()
            .unwrap(),
    );
    assert!(matches!(
        auth.verify(
            &headers,
            VerifyRequest {
                signature: sign(0, &challenge.view.payload)
            }
        )
        .await,
        Err(AuthError::Expired)
    ));
    let (_, headers) = signin(&auth, 0).await;
    auth.lock()
        .unwrap()
        .sessions
        .values_mut()
        .next()
        .unwrap()
        .expires = Instant::now() - Duration::from_secs(1);
    assert!(auth.identity(&headers).unwrap().is_none());
}

#[tokio::test]
async fn sessions_rotate_require_csrf_and_revoke_across_clones_and_restart() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (first, old_headers) = signin(&auth, 0).await;
    assert!(first.cookie.contains("__Host-seeded_session="));
    for flag in ["HttpOnly", "SameSite=Strict", "Secure", "Path=/"] {
        assert!(first.cookie.contains(flag));
    }
    assert_ne!(
        first.identity.session_id(),
        first
            .cookie
            .split('=')
            .nth(1)
            .unwrap()
            .split(';')
            .next()
            .unwrap()
    );
    let (challenge, mut verify_headers) = issue(&auth, 0);
    let both = format!(
        "{}; {}",
        verify_headers
            .get(header::COOKIE)
            .unwrap()
            .to_str()
            .unwrap(),
        old_headers.get(header::COOKIE).unwrap().to_str().unwrap()
    );
    verify_headers.insert(header::COOKIE, both.parse().unwrap());
    let second = auth
        .verify(
            &verify_headers,
            VerifyRequest {
                signature: sign(0, &challenge.view.payload),
            },
        )
        .await
        .unwrap();
    assert!(auth.identity(&old_headers).unwrap().is_none());
    let mut current = headers();
    current.insert(
        header::COOKIE,
        second.cookie.split(';').next().unwrap().parse().unwrap(),
    );
    assert!(matches!(auth.require(&current), Err(AuthError::Forbidden)));
    current.insert(
        "x-csrf-token",
        second.identity.csrf_token().parse().unwrap(),
    );
    assert!(auth.require(&current).is_ok());
    assert!(
        AuthState::new(ORIGIN)
            .unwrap()
            .identity(&current)
            .unwrap()
            .is_none()
    );
    let (_, another) = signin(&auth, 0).await;
    auth.clone().logout(&current, true).unwrap();
    assert!(auth.identity(&current).unwrap().is_none());
    assert!(auth.identity(&another).unwrap().is_none());
    assert!(!auth.is_current(&second.identity).unwrap());
}

#[test]
fn stateless_issuance_does_not_limit_neighbours_or_replace_existing_challenges() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (first, _) = issue(&auth, 0);
    for _ in 0..128 {
        let (replacement, _) = issue(&auth, 0);
        assert_ne!(first.view.payload, replacement.view.payload);
    }
    let registry = auth.lock().unwrap();
    assert!(registry.accounts.is_empty());
    assert!(registry.verifying.is_empty());
    assert_eq!(registry.replay.generation, 0);
}

#[test]
fn origins_and_cookie_ambiguity_are_rejected() {
    for origin in [
        "http://seeded.art",
        "https://seeded.art/path",
        "https://seeded.art/",
        "https://person@seeded.art",
        "file:///x",
    ] {
        assert!(AuthState::new(origin).is_err());
    }
    assert!(AuthState::new("http://127.0.0.1:3918").is_ok());
    assert!(AuthState::new("http://[::1]:3918").is_ok());
    let auth = AuthState::new(ORIGIN).unwrap();
    let (_, mut headers) = issue(&auth, 0);
    headers.append(header::ORIGIN, ORIGIN.parse().unwrap());
    assert_eq!(auth.check_origin(&headers), Err(AuthError::Forbidden));
    let mut headers = super::tests::headers();
    headers.insert("sec-fetch-site", "same-site".parse().unwrap());
    assert_eq!(auth.check_origin(&headers), Err(AuthError::Forbidden));
    headers.insert(
        header::COOKIE,
        format!(
            "__Host-seeded_session={}; __Host-seeded_session={}",
            "a".repeat(64),
            "a".repeat(64)
        )
        .parse()
        .unwrap(),
    );
    assert!(auth.identity(&headers).is_err());
}

#[test]
fn cancelling_crypto_keeps_worker_slot_until_completion_without_late_session() {
    blocked_verification(false);
}

#[test]
fn revoking_all_sessions_also_rejects_a_signature_verification_already_in_flight() {
    blocked_verification(true);
}

fn blocked_verification(revoke: bool) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let auth = AuthState::new(ORIGIN).unwrap();
        let logged_in = if revoke {
            Some(signin(&auth, 0).await.1)
        } else {
            None
        };
        let (challenge, headers) = issue(&auth, 0);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let blocked = tokio::task::spawn_blocking(move || {
            ready_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        ready_rx.await.unwrap();
        let clone = auth.clone();
        let verify = tokio::spawn(async move {
            clone
                .verify(
                    &headers,
                    VerifyRequest {
                        signature: sign(0, &challenge.view.payload),
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while auth.crypto.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(auth.waiting.available_permits(), 15);
        if let Some(headers) = logged_in.as_ref() {
            auth.logout(headers, true).unwrap();
            let mut registry = auth.lock().unwrap();
            let address = crypto::public_key_address(&key(0)).unwrap();
            registry.accounts.get_mut(&address).unwrap().touched =
                Instant::now() - Duration::from_secs(10 * 3600);
            registry.next_cleanup = Instant::now();
            registry.prune(Instant::now());
            assert_eq!(registry.accounts[&address].generation, 2);
            assert!(!registry.verifying.is_empty());
            for n in 0..MAX_ACCOUNTS - 1 {
                let now = Instant::now();
                registry.accounts.insert(
                    format!("idle{n}"),
                    Account {
                        sign_ins: Rate::new(now),
                        edits: Rate::new(now),
                        generation: 0,
                        touched: now - CHALLENGE_LIFETIME,
                    },
                );
            }
            registry.make_account_room("another-account").unwrap();
            assert_eq!(registry.accounts[&address].generation, 2);
        } else {
            verify.abort();
        }
        assert_eq!(auth.crypto.available_permits(), 1);
        release_tx.send(()).unwrap();
        blocked.await.unwrap();
        let result = verify.await;
        if revoke {
            assert!(matches!(result, Ok(Err(AuthError::Expired))));
        } else {
            assert!(matches!(result,Err(error) if error.is_cancelled()));
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while auth.crypto.available_permits() != 2 || !auth.lock().unwrap().verifying.is_empty()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(auth.lock().unwrap().sessions.is_empty());
        assert!(auth.lock().unwrap().verifying.is_empty());
        assert_eq!(auth.waiting.available_permits(), 16);
        assert!(
            signin(&auth, 0)
                .await
                .0
                .identity
                .address()
                .starts_with("tz1")
        );
    });
}

#[tokio::test]
async fn exhausted_crypto_queue_is_retryable_without_consuming_a_challenge() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (challenge, headers) = issue(&auth, 0);
    let request = || VerifyRequest {
        signature: sign(0, &challenge.view.payload),
    };
    let waiting = auth.waiting.clone().acquire_many_owned(16).await.unwrap();
    assert!(matches!(
        auth.verify(&headers, request()).await,
        Err(AuthError::RateLimited)
    ));
    drop(waiting);
    let workers = auth.crypto.clone().acquire_many_owned(2).await.unwrap();
    assert!(matches!(
        auth.verify(&headers, request()).await,
        Err(AuthError::RateLimited)
    ));
    assert!(auth.lock().unwrap().accounts.is_empty());
    drop(workers);
    assert!(auth.verify(&headers, request()).await.is_ok());
}

#[tokio::test]
async fn cancelling_mobile_verification_waiter_releases_pins_and_keeps_challenge_retryable() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (challenge, headers) = issue_for(&auth, 0, client(1));
    let mobile = TrustedClientIp("2001:db8:1:2::1".parse().unwrap());
    let signature = sign(0, &challenge.view.payload);
    let workers = auth.crypto.clone().acquire_many_owned(2).await.unwrap();
    let pending = {
        let auth = auth.clone();
        let headers = headers.clone();
        let signature = signature.clone();
        tokio::spawn(async move {
            auth.verify_for(mobile, &headers, VerifyRequest { signature })
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        while auth.lock().unwrap().verifying.len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let mut registry = auth.lock().unwrap();
        registry.next_cleanup = Instant::now();
        registry.prune(Instant::now());
        assert_eq!(registry.verifying.len(), 1);
    }
    pending.abort();
    assert!(matches!(pending.await, Err(error) if error.is_cancelled()));
    assert!(auth.lock().unwrap().verifying.is_empty());
    assert_eq!(auth.waiting.available_permits(), 16);
    assert!(auth.lock().unwrap().accounts.is_empty());
    drop(workers);
    auth.verify_for(mobile, &headers, VerifyRequest { signature })
        .await
        .unwrap();
    assert!(auth.lock().unwrap().verifying.is_empty());
}

#[tokio::test]
async fn session_saturation_evicts_oldest_from_heaviest_client_and_prunes_expired() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let grant = signin_for(&auth, 0, client(1)).await;
    let oldest = signin_for(&auth, 1, client(2)).await;
    {
        let mut registry = auth.lock().unwrap();
        let old = Instant::now() - Duration::from_secs(120);
        registry
            .sessions
            .get_mut(oldest.identity.session_id())
            .unwrap()
            .touched = old;
        for i in 0..MAX_SESSIONS - 2 {
            let mut identity = grant.identity.clone();
            identity.session_id = format!("fixture{i}");
            identity.address = format!("fixture-address{i}");
            identity.touched = old + Duration::from_millis(i as u64 + 1);
            registry
                .sessions
                .insert(identity.session_id.clone(), identity);
        }
    }
    let next = signin_for(&auth, 2, client(3)).await;
    assert!(auth.is_current(&next.identity).unwrap());
    assert!(auth.is_current(&oldest.identity).unwrap());
    assert!(auth.is_current(&grant.identity).unwrap());
    assert_eq!(auth.lock().unwrap().sessions.len(), MAX_SESSIONS);
    assert!(!auth.lock().unwrap().sessions.contains_key("fixture0"));
    {
        let mut registry = auth.lock().unwrap();
        for identity in registry.sessions.values_mut() {
            identity.expires = Instant::now() - Duration::from_secs(1);
        }
        registry.next_cleanup = Instant::now();
    }
    signin_for(&auth, 1, client(4)).await;
    assert_eq!(auth.lock().unwrap().sessions.len(), 1);
}

#[tokio::test]
async fn session_saturation_selects_heaviest_ipv6_family_then_prefix_before_age() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let grant = signin_for(&auth, 0, client(1)).await;
    let mut registry = auth.lock().unwrap();
    registry.sessions.clear();
    let old = Instant::now() - Duration::from_secs(120);
    for (prefix, count, age) in [
        ("2001:db8:1:1::", 1200, 2),
        ("2001:db8:1:2::", 1100, 1),
        ("2001:db8:2:1::", MAX_SESSIONS - 2300, 0),
    ] {
        for n in 0..count {
            let mut identity = grant.identity.clone();
            identity.session_id = format!("{prefix}-{n}");
            identity.address = format!("fixture-address-{prefix}-{n}");
            identity.client = TrustedClientIp(prefix.parse().unwrap());
            identity.touched = old + Duration::from_secs(if n == 0 { age } else { 3 });
            registry
                .sessions
                .insert(identity.session_id.clone(), identity);
        }
    }
    registry.make_session_room();
    assert_eq!(registry.sessions.len(), MAX_SESSIONS - 1);
    assert!(!registry.sessions.contains_key("2001:db8:1:1::-0"));
    assert!(registry.sessions.contains_key("2001:db8:1:2::-0"));
    assert!(registry.sessions.contains_key("2001:db8:2:1::-0"));
    registry.make_session_room();
    assert_eq!(registry.sessions.len(), MAX_SESSIONS - 1);
}

#[tokio::test]
async fn session_saturation_across_single_session_clients_evicts_least_recent() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let grant = signin_for(&auth, 0, client(1)).await;
    {
        let mut registry = auth.lock().unwrap();
        registry.sessions.clear();
        let old = Instant::now() - Duration::from_secs(120);
        for n in 0..MAX_SESSIONS {
            let mut identity = grant.identity.clone();
            identity.session_id = format!("fixture{n}");
            identity.address = format!("fixture-address{n}");
            identity.client = TrustedClientIp(Ipv4Addr::from(0x0a00_0000 + n as u32).into());
            identity.touched =
                old + Duration::from_millis(if n == 7 { 0 } else { 1000 + n as u64 });
            registry
                .sessions
                .insert(identity.session_id.clone(), identity);
        }
    }
    let next = signin_for(&auth, 1, client(2)).await;
    assert!(auth.is_current(&next.identity).unwrap());
    let registry = auth.lock().unwrap();
    assert_eq!(registry.sessions.len(), MAX_SESSIONS);
    assert!(!registry.sessions.contains_key("fixture7"));
    assert!(registry.sessions.contains_key("fixture0"));
    assert!(registry.sessions.contains_key("fixture8"));
}

fn client(n: u8) -> TrustedClientIp {
    TrustedClientIp(Ipv4Addr::new(192, 0, 2, n).into())
}
fn issue_for(auth: &AuthState, kind: usize, ip: TrustedClientIp) -> (ChallengeGrant, HeaderMap) {
    let public_key = key(kind);
    let grant = auth
        .challenge_for(
            ip,
            &headers(),
            SignInRequest {
                address: crypto::public_key_address(&public_key).unwrap(),
                public_key,
            },
        )
        .unwrap();
    let mut headers = headers();
    headers.insert(
        header::COOKIE,
        grant.cookie.split(';').next().unwrap().parse().unwrap(),
    );
    headers.insert("x-csrf-token", grant.view.csrf.parse().unwrap());
    (grant, headers)
}
async fn signin_for(auth: &AuthState, kind: usize, ip: TrustedClientIp) -> SessionGrant {
    let (grant, headers) = issue_for(auth, kind, ip);
    auth.verify_for(
        ip,
        &headers,
        VerifyRequest {
            signature: sign(kind, &grant.view.payload),
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn unauthenticated_victim_attempts_and_malformed_input_cannot_spend_verified_allowances() {
    let auth = AuthState::new(ORIGIN).unwrap();
    for _ in 0..200 {
        assert!(matches!(
            auth.challenge_for(
                client(1),
                &headers(),
                SignInRequest {
                    address: "invalid".into(),
                    public_key: "invalid".into()
                }
            ),
            Err(AuthError::InvalidSignature)
        ));
        assert!(matches!(
            auth.verify_for(
                client(1),
                &headers(),
                VerifyRequest {
                    signature: "invalid".into()
                }
            )
            .await,
            Err(AuthError::InvalidSignature)
        ));
    }
    for _ in 0..8 {
        issue_for(&auth, 0, client(1));
    }
    for n in 0..200 {
        let mut spoofed = headers();
        spoofed.insert(
            "x-forwarded-for",
            format!("192.0.2.{}", n % 255).parse().unwrap(),
        );
        auth.challenge_for(
            client(1),
            &spoofed,
            SignInRequest {
                address: crypto::public_key_address(&key(0)).unwrap(),
                public_key: key(0),
            },
        )
        .unwrap();
    }
    {
        let registry = auth.lock().unwrap();
        assert!(registry.accounts.is_empty());
    }
    let signed = signin_for(&auth, 0, client(2)).await;
    assert!(auth.is_current(&signed.identity).unwrap());
    assert_eq!(auth.lock().unwrap().accounts.len(), 1);
}

#[tokio::test]
async fn mobile_network_change_keeps_cookie_csrf_signature_binding() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (grant, headers) = issue_for(&auth, 0, client(1));
    let signature = sign(0, &grant.view.payload);
    let mobile = TrustedClientIp("2001:db8:1:2::1".parse().unwrap());
    let mut missing_cookie = headers.clone();
    missing_cookie.remove(header::COOKIE);
    assert!(matches!(
        auth.verify_for(
            mobile,
            &missing_cookie,
            VerifyRequest {
                signature: signature.clone()
            }
        )
        .await,
        Err(AuthError::Expired)
    ));
    let mut missing_csrf = headers.clone();
    missing_csrf.remove("x-csrf-token");
    assert!(matches!(
        auth.verify_for(
            mobile,
            &missing_csrf,
            VerifyRequest {
                signature: signature.clone()
            }
        )
        .await,
        Err(AuthError::Forbidden)
    ));
    auth.verify_for(
        mobile,
        &headers,
        VerifyRequest {
            signature: signature.clone(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        auth.verify_for(client(1), &headers, VerifyRequest { signature })
            .await,
        Err(AuthError::Expired)
    ));
    let registry = auth.lock().unwrap();
    assert_eq!(registry.sessions.len(), 1);
    assert_eq!(
        registry.sessions.values().next().unwrap().client,
        TrustedClientIp(rate_ip(mobile.0))
    );
}

#[tokio::test]
async fn shared_ipv6_and_mapped_ipv4_do_not_share_sign_in_quotas() {
    for (first, same) in [
        ("2001:db8:1:2::1", "2001:db8:1:2::abcd"),
        ("192.0.2.1", "::ffff:192.0.2.1"),
    ] {
        let first = TrustedClientIp(first.parse().unwrap());
        let same = TrustedClientIp(same.parse().unwrap());
        let auth = AuthState::new(ORIGIN).unwrap();
        for _ in 0..16 {
            signin_for(&auth, 0, first).await;
        }
        let (grant, headers) = issue_for(&auth, 0, first);
        assert!(matches!(
            auth.verify_for(
                first,
                &headers,
                VerifyRequest {
                    signature: sign(0, &grant.view.payload),
                }
            )
            .await,
            Err(AuthError::RateLimited)
        ));
        // A verified address's budget cannot lock out another address on the same network.
        signin_for(&auth, 1, same).await;
    }
}

#[test]
fn hundreds_of_addresses_on_one_prefix_can_request_stateless_challenges() {
    let auth = AuthState::new(ORIGIN).unwrap();
    for n in 0..200 {
        let ip = TrustedClientIp(format!("2001:db8:1:2::{:x}", n + 1).parse().unwrap());
        let public_key = encoded(&[13, 15, 37, 217], &[n; 32]);
        auth.challenge_for(
            ip,
            &headers(),
            SignInRequest {
                address: crypto::public_key_address(&public_key).unwrap(),
                public_key,
            },
        )
        .unwrap();
    }
    assert!(auth.lock().unwrap().accounts.is_empty());
}

#[tokio::test]
async fn ipv6_network_change_retains_canonical_session_client() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let first = TrustedClientIp("2001:db8:1:2::1".parse().unwrap());
    let same = TrustedClientIp("2001:db8:1:2::2".parse().unwrap());
    let (grant, headers) = issue_for(&auth, 0, first);
    let signature = sign(0, &grant.view.payload);
    let session = auth
        .verify_for(same, &headers, VerifyRequest { signature })
        .await
        .unwrap();
    assert_eq!(
        session.identity.client,
        TrustedClientIp("2001:db8:1:2::".parse().unwrap())
    );
}

#[tokio::test]
async fn session_cap_evicts_only_oldest_for_verified_address_and_rotation_preserves_peers() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let other = signin_for(&auth, 1, client(250)).await;
    let mut sessions = Vec::new();
    for n in 1..=9 {
        sessions.push(signin_for(&auth, 0, client(n)).await);
    }
    assert!(!auth.is_current(&sessions[0].identity).unwrap());
    for session in &sessions[1..] {
        assert!(auth.is_current(&session.identity).unwrap());
    }
    assert!(auth.is_current(&other.identity).unwrap());
    assert_eq!(auth.lock().unwrap().sessions.len(), 9);
    let (challenge, mut headers) = issue_for(&auth, 0, client(10));
    headers.insert(
        header::COOKIE,
        format!(
            "{}; {}",
            challenge.cookie.split(';').next().unwrap(),
            sessions[8].cookie.split(';').next().unwrap()
        )
        .parse()
        .unwrap(),
    );
    auth.verify_for(
        client(10),
        &headers,
        VerifyRequest {
            signature: sign(0, &challenge.view.payload),
        },
    )
    .await
    .unwrap();
    assert!(!auth.is_current(&sessions[8].identity).unwrap());
    for session in &sessions[1..8] {
        assert!(auth.is_current(&session.identity).unwrap());
    }
    assert_eq!(auth.lock().unwrap().sessions.len(), 9);
}

#[tokio::test]
async fn profile_edit_limit_survives_session_rotation_isolates_addresses_and_recovers() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (a, _) = signin(&auth, 0).await;
    for _ in 0..6 {
        auth.admit_profile_edit(&a.identity).unwrap();
    }
    assert_eq!(
        auth.admit_profile_edit(&a.identity),
        Err(AuthError::RateLimited)
    );
    let (again, _) = signin(&auth, 0).await;
    assert_eq!(
        auth.admit_profile_edit(&again.identity),
        Err(AuthError::RateLimited)
    );
    let (b, b_headers) = signin(&auth, 1).await;
    auth.admit_profile_edit(&b.identity).unwrap();
    auth.logout(&b_headers, true).unwrap();
    assert_eq!(
        auth.admit_profile_edit(&b.identity),
        Err(AuthError::Unauthenticated)
    );
    auth.lock()
        .unwrap()
        .accounts
        .get_mut(a.identity.address())
        .unwrap()
        .edits
        .started = Instant::now() - Duration::from_secs(61);
    auth.admit_profile_edit(&again.identity).unwrap();
}

#[tokio::test]
async fn expired_account_pruning_retains_replay_watermarks() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let session = signin_for(&auth, 0, client(1)).await;
    let mut registry = auth.lock().unwrap();
    registry.sessions.clear();
    let now = Instant::now();
    registry
        .accounts
        .get_mut(session.identity.address())
        .unwrap()
        .touched = now - SESSION_LIFETIME - CHALLENGE_LIFETIME;
    registry.next_cleanup = now;
    registry.prune(now);
    assert!(registry.accounts.is_empty());
    assert_eq!(registry.generation(session.identity.address()), 1);
}

#[test]
fn replay_watermarks_are_bounded_monotonic_and_use_distinct_generations() {
    let mut replay = Replay::new();
    for n in 0..REPLAY_BUCKETS * 2 {
        let generation = replay.advance().unwrap();
        replay.remember(&format!("address{n}"), generation);
    }
    assert_eq!(replay.buckets.len(), REPLAY_BUCKETS);
    assert_eq!(replay.generation, (REPLAY_BUCKETS * 2) as u64);
    let before = replay.forgotten("address1");
    replay.remember("address1", 0);
    assert_eq!(replay.forgotten("address1"), before);
}

#[tokio::test]
async fn distinct_prefix_challenge_flood_cannot_evict_an_existing_challenge() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (protected, verification_headers) = issue_for(&auth, 2, client(200));
    // Exceed both the old 1024-challenge cap and the rate-map capacity.
    for n in 0..MAX_ACCOUNTS + 32 {
        let ip = TrustedClientIp(format!("2001:db8:{n:x}:1::1").parse().unwrap());
        issue_for(&auth, 0, ip);
    }
    {
        let registry = auth.lock().unwrap();
        assert!(registry.accounts.is_empty());
        assert!(registry.verifying.is_empty());
        assert_eq!(registry.replay.generation, 0);
    }
    auth.verify_for(
        client(200),
        &verification_headers,
        VerifyRequest {
            signature: sign(2, &protected.view.payload),
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn recent_account_saturation_admits_newcomers_and_preserves_active_accounts() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let active = signin_for(&auth, 0, client(1)).await;
    let (_, logout_headers) = signin(&auth, 1).await;
    auth.logout(&logout_headers, false).unwrap();
    let (pending, pending_headers) = issue_for(&auth, 1, client(2));
    let pending_address = crypto::public_key_address(&key(1)).unwrap();
    {
        let now = Instant::now();
        let mut registry = auth.lock().unwrap();
        registry
            .accounts
            .get_mut(active.identity.address())
            .unwrap()
            .touched = now - Duration::from_secs(7200);
        for n in 0..MAX_ACCOUNTS - 2 {
            registry.accounts.insert(
                format!("idle{n}"),
                Account {
                    sign_ins: Rate::new(now),
                    edits: Rate::new(now),
                    generation: 0,
                    touched: now - Duration::from_secs(30) + Duration::from_millis(n as u64),
                },
            );
        }
    }
    let next = signin_for(&auth, 2, client(3)).await;
    assert!(auth.is_current(&next.identity).unwrap());
    assert!(auth.is_current(&active.identity).unwrap());
    {
        let registry = auth.lock().unwrap();
        assert_eq!(registry.accounts.len(), MAX_ACCOUNTS);
        assert!(!registry.accounts.contains_key("idle0"));
        assert!(registry.accounts.contains_key(&pending_address));
    }
    assert!(
        auth.verify_for(
            client(2),
            &pending_headers,
            VerifyRequest {
                signature: sign(1, &pending.view.payload)
            }
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn replay_bucket_collision_only_expires_unused_cookies_and_reissue_recovers() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (pending, pending_headers) = issue(&auth, 0);
    let address = crypto::public_key_address(&key(0)).unwrap();
    {
        let mut registry = auth.lock().unwrap();
        let bucket = registry.replay.bucket(&address);
        let generation = registry.replay.advance().unwrap();
        // Model an evicted address hashing to the same bucket, without a random search.
        registry.replay.buckets[bucket] = generation;
    }
    assert!(matches!(
        auth.verify(
            &pending_headers,
            VerifyRequest {
                signature: sign(0, &pending.view.payload),
            }
        )
        .await,
        Err(AuthError::Expired)
    ));
    signin(&auth, 0).await;
}

#[tokio::test]
async fn concurrent_profile_edits_share_one_address_allowance() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (grant, _) = signin(&auth, 0).await;
    let barrier = Arc::new(std::sync::Barrier::new(20));
    let tasks: Vec<_> = (0..20)
        .map(|_| {
            let auth = auth.clone();
            let identity = grant.identity.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                auth.admit_profile_edit(&identity)
            })
        })
        .collect();
    let results: Vec<_> = tasks.into_iter().map(|task| task.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 6);
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Err(AuthError::RateLimited))
            .count(),
        14
    );
}

#[tokio::test]
async fn challenge_cookies_reject_tampering_restart_and_another_origin() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (grant, verification_headers) = issue(&auth, 0);
    assert!(grant.cookie.len() < 4096);
    let bearer = cookie_value(&verification_headers, auth.challenge_name())
        .unwrap()
        .unwrap();
    let original = Challenge::open(&bearer, &auth.challenge_secret, ORIGIN).unwrap();
    assert!(Challenge::open(&bearer, &auth.challenge_secret, "https://other.example").is_err());
    let (data, tag) = bearer.split_once('.').unwrap();
    let value: serde_json::Value = serde_json::from_slice(&hex::decode(data).unwrap()).unwrap();
    for (field, replacement) in [
        ("client", serde_json::json!("192.0.2.250")),
        (
            "address",
            serde_json::json!(crypto::public_key_address(&key(1)).unwrap()),
        ),
        ("public_key", serde_json::json!(key(1))),
        ("csrf", serde_json::json!("a".repeat(64))),
        ("nonce", serde_json::json!("b".repeat(64))),
        ("issued", serde_json::json!(original.issued + 1)),
        ("deadline", serde_json::json!(original.deadline + 1)),
        ("generation", serde_json::json!(original.generation + 1)),
    ] {
        let mut changed = value.clone();
        changed[field] = replacement;
        let forged = format!(
            "{}.{tag}",
            hex::encode(serde_json::to_vec(&changed).unwrap())
        );
        let mut headers = verification_headers.clone();
        headers.insert(
            header::COOKIE,
            format!("{}={forged}", auth.challenge_name())
                .parse()
                .unwrap(),
        );
        assert!(
            matches!(
                auth.verify(
                    &headers,
                    VerifyRequest {
                        signature: sign(0, &grant.view.payload),
                    }
                )
                .await,
                Err(AuthError::Expired)
            ),
            "accepted tampered {field}"
        );
    }
    assert!(matches!(
        AuthState::new(ORIGIN)
            .unwrap()
            .verify(
                &verification_headers,
                VerifyRequest {
                    signature: sign(0, &grant.view.payload)
                },
            )
            .await,
        Err(AuthError::Expired)
    ));
    let mut duplicate = verification_headers.clone();
    duplicate.append(header::COOKIE, verification_headers[header::COOKIE].clone());
    assert!(matches!(
        auth.verify(
            &duplicate,
            VerifyRequest {
                signature: sign(0, &grant.view.payload),
            }
        )
        .await,
        Err(AuthError::Unauthenticated)
    ));
    auth.verify(
        &verification_headers,
        VerifyRequest {
            signature: sign(0, &grant.view.payload),
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn signing_in_consumes_older_challenges_across_tabs_but_issuance_alone_does_not() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (first, first_headers) = issue(&auth, 0);
    let (second, second_headers) = issue(&auth, 0);
    auth.verify(
        &first_headers,
        VerifyRequest {
            signature: sign(0, &first.view.payload),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        auth.verify(
            &second_headers,
            VerifyRequest {
                signature: sign(0, &second.view.payload),
            }
        )
        .await,
        Err(AuthError::Expired)
    ));
}

#[tokio::test]
async fn logout_all_revokes_pending_cookies_and_retains_replay_generation() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (session, session_headers) = signin(&auth, 0).await;
    let (pending, pending_headers) = issue(&auth, 0);
    {
        let mut registry = auth.lock().unwrap();
        registry
            .accounts
            .get_mut(session.identity.address())
            .unwrap()
            .touched = Instant::now() - Duration::from_secs(10 * 3600);
    }
    auth.logout(&session_headers, true).unwrap();
    {
        let mut registry = auth.lock().unwrap();
        let now = Instant::now();
        registry.next_cleanup = now;
        registry.prune(now);
        assert_eq!(registry.accounts[session.identity.address()].generation, 2);
        registry
            .accounts
            .get_mut(session.identity.address())
            .unwrap()
            .touched = now - Duration::from_secs(1);
        for n in 0..MAX_ACCOUNTS - 1 {
            registry.accounts.insert(
                format!("idle{n}"),
                Account {
                    sign_ins: Rate::new(now),
                    edits: Rate::new(now),
                    generation: 0,
                    touched: now,
                },
            );
        }
        registry.make_account_room("fresh-address").unwrap();
        assert!(!registry.accounts.contains_key(session.identity.address()));
        assert_eq!(registry.generation(session.identity.address()), 2);
    }
    assert!(matches!(
        auth.verify(
            &pending_headers,
            VerifyRequest {
                signature: sign(0, &pending.view.payload),
            }
        )
        .await,
        Err(AuthError::Expired)
    ));
    signin_for(&auth, 0, client(2)).await;
}

#[tokio::test]
async fn logout_all_revokes_pending_challenge_when_account_is_evicted_after_authentication() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (_, session_headers) = signin(&auth, 0).await;
    let (pending, pending_headers) = issue(&auth, 0);
    // Pause logout-all after its authenticated identity is read.
    let identity = auth.require(&session_headers).unwrap();
    // A concurrent ordinary logout removes the last pin on this account.
    auth.logout(&session_headers, false).unwrap();
    {
        let mut registry = auth.lock().unwrap();
        let now = Instant::now();
        registry
            .accounts
            .get_mut(identity.address())
            .unwrap()
            .touched = now - Duration::from_secs(1);
        for n in 0..MAX_ACCOUNTS - 1 {
            registry.accounts.insert(
                format!("recent{n}"),
                Account {
                    sign_ins: Rate::new(now),
                    edits: Rate::new(now),
                    generation: 0,
                    touched: now,
                },
            );
        }
        registry.make_account_room("newcomer").unwrap();
        assert!(!registry.accounts.contains_key(identity.address()));
        assert_eq!(registry.generation(identity.address()), 1);
    }
    // Resume the already-authenticated logout-all after the eviction.
    auth.logout_identity(&identity, true).unwrap();
    assert_eq!(auth.lock().unwrap().generation(identity.address()), 2);
    assert!(matches!(
        auth.verify(
            &pending_headers,
            VerifyRequest {
                signature: sign(0, &pending.view.payload),
            }
        )
        .await,
        Err(AuthError::Expired)
    ));
    assert!(auth.lock().unwrap().sessions.is_empty());
    signin(&auth, 0).await;
}

#[tokio::test]
async fn used_challenge_cannot_resurrect_after_recent_account_eviction_and_recreation() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let (grant, headers) = issue(&auth, 0);
    let signature = sign(0, &grant.view.payload);
    let signed = auth
        .verify(
            &headers,
            VerifyRequest {
                signature: signature.clone(),
            },
        )
        .await
        .unwrap();
    {
        let mut registry = auth.lock().unwrap();
        registry.sessions.clear();
        let now = Instant::now();
        registry
            .accounts
            .get_mut(signed.identity.address())
            .unwrap()
            .touched = now - Duration::from_secs(1);
        for n in 0..MAX_ACCOUNTS - 1 {
            registry.accounts.insert(
                format!("recent{n}"),
                Account {
                    sign_ins: Rate::new(now),
                    edits: Rate::new(now),
                    generation: 0,
                    touched: now,
                },
            );
        }
        registry.make_account_room("fresh-address").unwrap();
        assert!(!registry.accounts.contains_key(signed.identity.address()));
        assert_eq!(registry.generation(signed.identity.address()), 1);
    }
    let request = || VerifyRequest {
        signature: signature.clone(),
    };
    assert!(matches!(
        auth.verify(&headers, request()).await,
        Err(AuthError::Expired)
    ));
    signin_for(&auth, 0, client(1)).await;
    assert!(matches!(
        auth.verify(&headers, request()).await,
        Err(AuthError::Expired)
    ));
    assert!(auth.lock().unwrap().accounts.len() <= MAX_ACCOUNTS);
}

#[tokio::test]
async fn fresh_key_session_storm_preserves_unrelated_and_recent_shared_ip_sessions() {
    let auth = AuthState::new(ORIGIN).unwrap();
    let unrelated = signin_for(&auth, 0, client(2)).await;
    let shared = signin_for(&auth, 1, client(1)).await;
    let mut first = None;
    for n in 1..=MAX_SESSIONS + 1 {
        let mut seed = [0; 32];
        seed[..8].copy_from_slice(&(n as u64).to_be_bytes());
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&seed);
        let public_key = encoded(&[13, 15, 37, 217], signing_key.verifying_key().as_bytes());
        let grant = auth
            .challenge_for(
                client(1),
                &headers(),
                SignInRequest {
                    address: crypto::public_key_address(&public_key).unwrap(),
                    public_key,
                },
            )
            .unwrap();
        let signature = encoded(
            &[9, 245, 205, 134, 18],
            &signing_key
                .sign(&crypto::blake::<32>(
                    &hex::decode(&grant.view.payload).unwrap(),
                ))
                .to_bytes(),
        );
        let mut headers = headers();
        headers.insert(
            header::COOKIE,
            grant.cookie.split(';').next().unwrap().parse().unwrap(),
        );
        headers.insert("x-csrf-token", grant.view.csrf.parse().unwrap());
        let signed = auth
            .verify_for(
                client(1),
                &headers,
                VerifyRequest {
                    signature: signature.clone(),
                },
            )
            .await
            .unwrap();
        if n == 1 {
            first = Some((signed.identity, headers, signature));
        }
        if n == MAX_SESSIONS - 2 {
            // An active client behind the same CGNAT keeps its session under pressure.
            assert!(auth.is_current(&shared.identity).unwrap());
        }
        let registry = auth.lock().unwrap();
        assert_eq!(registry.sessions.len(), (n + 2).min(MAX_SESSIONS));
        assert!(registry.accounts.len() <= MAX_ACCOUNTS);
    }
    assert!(auth.is_current(&unrelated.identity).unwrap());
    assert!(auth.is_current(&shared.identity).unwrap());
    let (first, headers, signature) = first.unwrap();
    assert!(!auth.is_current(&first).unwrap());
    assert!(matches!(
        auth.verify_for(client(1), &headers, VerifyRequest { signature })
            .await,
        Err(AuthError::Expired)
    ));
}
