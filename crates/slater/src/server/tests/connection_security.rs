// SPDX-License-Identifier: Apache-2.0
//! `connection_security` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── Connection-security limits ────────────────────────────────────────────

#[test]
fn semaphore_permits_maps_zero_to_unlimited() {
    assert_eq!(semaphore_permits(0), Semaphore::MAX_PERMITS);
    assert_eq!(semaphore_permits(5), 5);
}

#[test]
fn per_ip_key_keeps_ipv4_and_masks_ipv6_to_64() {
    use std::net::{IpAddr, Ipv4Addr};
    let v4: IpAddr = Ipv4Addr::new(203, 0, 113, 5).into();
    assert_eq!(per_ip_key(v4), v4, "IPv4 keys on the full /32");

    let a: IpAddr = "2001:db8:1:2:3:4:5:6".parse().unwrap();
    let b: IpAddr = "2001:db8:1:2:ffff:ffff:ffff:ffff".parse().unwrap();
    assert_eq!(per_ip_key(a), per_ip_key(b), "same /64 ⇒ same key");
    let c: IpAddr = "2001:db8:1:3::1".parse().unwrap();
    assert_ne!(
        per_ip_key(a),
        per_ip_key(c),
        "different /64 ⇒ different key"
    );
}

#[test]
fn try_acquire_per_ip_caps_and_releases() {
    use std::net::{IpAddr, Ipv4Addr};
    let map: Arc<Mutex<HashMap<IpAddr, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let key: IpAddr = Ipv4Addr::LOCALHOST.into();
    let g1 = try_acquire_per_ip(&map, key, 2).expect("first slot");
    let g2 = try_acquire_per_ip(&map, key, 2).expect("second slot");
    assert!(
        try_acquire_per_ip(&map, key, 2).is_none(),
        "third is over the cap"
    );
    drop(g1);
    let g3 = try_acquire_per_ip(&map, key, 2).expect("a freed slot is reusable");
    drop(g2);
    drop(g3);
    assert!(
        map.lock().unwrap().is_empty(),
        "the map drains to empty once all sources disconnect"
    );
}

#[tokio::test]
async fn framed_enforces_the_body_cap_and_a_larger_cap_admits_the_same_message() {
    use tokio::io::duplex;
    // A single ~1000-byte chunked message (len header + body + 00 00 terminator).
    let body = vec![0xABu8; 1000];
    let mut wire = Vec::new();
    wire.extend_from_slice(&(body.len() as u16).to_be_bytes());
    wire.extend_from_slice(&body);
    wire.extend_from_slice(&[0, 0]);

    // Under a 256-byte cap the framer refuses it before allocating the body.
    let (mut client, server) = duplex(1 << 16);
    client.write_all(&wire).await.unwrap();
    let mut framed = Framed::new(server, 256);
    assert!(
        framed.read_message().await.is_err(),
        "a 1000-byte message must be refused under a 256-byte cap"
    );

    // The identical bytes are accepted once the cap is raised (the post-auth case).
    let (mut client, server) = duplex(1 << 16);
    client.write_all(&wire).await.unwrap();
    let mut framed = Framed::new(server, 4096);
    let got = framed
        .read_message()
        .await
        .unwrap()
        .expect("a full message");
    assert_eq!(got, body);
}

#[tokio::test]
async fn login_deadline_closes_an_idle_unauthenticated_connection() {
    let (_root, ctx) = build_ctx_limited(
        "login_deadline",
        TestLimits {
            login_timeout_ms: 200,
            ..Default::default()
        },
    );
    let addr = spawn_server(ctx).await;
    // Connect but never send the handshake: the server must close us out.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut buf = [0u8; 4];
    match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await {
        Ok(Ok(0)) | Ok(Err(_)) => {} // clean EOF or reset — both mean "closed"
        Ok(Ok(n)) => panic!("server sent {n} bytes to an unauthenticated idle peer"),
        Err(_) => panic!("server did not close the idle pre-auth connection in time"),
    }
}

#[tokio::test]
async fn pre_auth_cap_is_tight_then_relaxes_after_login() {
    let (_root, ctx) = build_ctx_limited(
        "diff_cap",
        TestLimits {
            max_pre_auth_bytes: 512,
            max_message_bytes: 1 << 20,
            ..Default::default()
        },
    );
    let addr = spawn_server(ctx).await;

    // Pre-auth: a HELLO whose user-agent body blows past 512 bytes is refused —
    // the connection closes before the message is decoded.
    {
        let mut c = Client::connect(addr).await;
        let huge = "x".repeat(4000);
        c.send(PsValue::Struct {
            tag: message::tag::HELLO,
            fields: vec![PsValue::Map(vec![(
                "user_agent".into(),
                PsValue::str(&huge),
            )])],
        })
        .await;
        let mut buf = [0u8; 4];
        match tokio::time::timeout(Duration::from_secs(2), c.stream.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => {}
            Ok(Ok(n)) => {
                panic!("server accepted a {n}-byte reply to an oversized pre-auth msg")
            }
            Err(_) => panic!("server did not reject the oversized pre-auth message"),
        }
    }

    // Post-auth: the same connection, once authenticated, accepts a RUN whose
    // parameter map far exceeds the pre-auth cap (proving the ratchet).
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    let pad = "x".repeat(4000); // > 512-byte pre-auth cap, < 1 MiB post-auth cap
    c.send(PsValue::Struct {
        tag: message::tag::RUN,
        fields: vec![
            PsValue::str("RETURN 1 AS one"),
            PsValue::Map(vec![("pad".into(), PsValue::str(&pad))]),
            PsValue::Map(vec![("db".into(), PsValue::str("people"))]),
        ],
    })
    .await;
    assert_eq!(
        c.recv().await.0,
        message::tag::SUCCESS,
        "a large post-auth message must be read, not rejected by the pre-auth cap"
    );
}

#[tokio::test]
async fn pre_auth_budget_rejects_excess_anonymous_connections() {
    let (_root, ctx) = build_ctx_limited(
        "pre_auth_budget",
        TestLimits {
            max_pre_auth_connections: 1,
            ..Default::default()
        },
    );
    let addr = spawn_server(ctx).await;

    // A holds the only antechamber slot (handshake done, not yet authenticated).
    let _a = Client::connect(addr).await;

    // B is accepted at TCP level but the handler rejects it for lack of a slot,
    // so its handshake never completes.
    let mut b = TcpStream::connect(addr).await.unwrap();
    let mut hs = Vec::new();
    hs.extend_from_slice(&handshake::PREAMBLE);
    hs.extend_from_slice(&[0, 0, 4, 5]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    let _ = b.write_all(&hs).await;
    let mut reply = [0u8; 4];
    match tokio::time::timeout(Duration::from_secs(2), b.read_exact(&mut reply)).await {
        Ok(Err(_)) => {} // EOF / reset: rejected as expected
        Ok(Ok(_)) => panic!("second anonymous connection should have been rejected"),
        Err(_) => panic!("server neither served nor rejected the excess anon connection"),
    }
}

#[tokio::test]
async fn global_connection_cap_blocks_until_a_slot_frees() {
    let (_root, ctx) = build_ctx_limited("global_cap", TestLimits::default());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conn_limit = Arc::new(Semaphore::new(1)); // exactly one slot
    let (_tx, rx) = tokio::sync::oneshot::channel::<String>();
    tokio::spawn(accept_loop(listener, ctx, None, conn_limit, rx));

    // First client takes the only slot.
    let a = Client::connect(addr).await;
    // Second cannot be serviced while at capacity (the permit is taken before
    // accept, so the server never reads B's handshake).
    assert!(
        tokio::time::timeout(Duration::from_millis(300), Client::connect(addr))
            .await
            .is_err(),
        "a second connection must not be serviced while at capacity"
    );
    // Freeing the first frees the slot.
    drop(a);
    tokio::time::timeout(Duration::from_secs(2), Client::connect(addr))
        .await
        .expect("a slot must free once the first connection closes");
}

/// A throwaway self-signed acceptor, minted in-process — no key material in the repo
/// and nothing to expire. The TLS tests below never validate the chain (the client
/// side is a raw socket), so a bare `localhost` leaf is all the server needs.
fn test_tls_acceptor() -> TlsAcceptor {
    let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(issued.key_pair.serialize_der());
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![issued.cert.der().clone()], key.into())
        .unwrap();
    TlsAcceptor::from(Arc::new(config))
}

/// HIK-72. A peer that completes TCP and then never sends a ClientHello must be torn
/// down and must not hold a connection slot while it stalls.
///
/// The regression this pins has two halves, and the test fails on either:
///
/// 1. **Ordering.** The antechamber permit is taken at `accept()`, so a socket still
///    inside the TLS handshake is *counted* against `maxPreAuthConnections`. When the
///    permit was taken behind the handshake (in `handle_connection`), anonymous TLS
///    sockets were uncounted and could occupy the entire global pool — the plaintext
///    path's headroom guarantee simply did not exist on the TLS path.
/// 2. **Liveness.** The handshake is bounded, so the slot comes back. With exactly one
///    global permit, B is served only if A's stalled handshake is actually torn down;
///    before the fix A held it forever and the accept loop stopped draining the queue.
///
/// `loginTimeoutMs` is deliberately **off** here: the handshake bound has to stand on
/// its own, or the guard would evaporate for any operator who widened the login window.
#[tokio::test]
async fn a_stalled_tls_handshake_does_not_hold_a_connection_permit() {
    let (_root, ctx) = build_ctx_limited(
        "tls_slow_loris",
        TestLimits {
            // 1s, sampled at 200ms below: a 5× margin, so a loaded CI box cannot
            // tear A down before the mid-handshake assertion gets to look at it.
            tls_handshake_timeout_ms: 1_000,
            login_timeout_ms: 0, // off on purpose — the handshake bound stands alone
            max_pre_auth_connections: 8,
            ..Default::default()
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conn_limit = Arc::new(Semaphore::new(1)); // exactly one slot to fight over
    let (_tx, rx) = tokio::sync::oneshot::channel::<String>();
    let gauges = ctx.clone();
    tokio::spawn(accept_loop(
        listener,
        ctx,
        Some(test_tls_acceptor()),
        conn_limit,
        rx,
    ));

    // A: completes the TCP handshake, then says nothing at all. Never a ClientHello.
    let _slow_loris = TcpStream::connect(addr).await.unwrap();

    // Half 1 — while A is stalled *mid-handshake*, it is already accounted for: it
    // holds an antechamber slot. (The global pool is not worth asserting on: the
    // accept loop reserves its next permit *before* parking in `accept()`, so with a
    // pool of one, "none available" is just as true of an idle server.)
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        gauges.pre_auth_limit.available_permits(),
        7,
        "a socket stalled mid-ClientHello must hold a pre-auth slot — the permit is \
             taken at accept, not behind the TLS handshake"
    );

    // Half 2 — B gets served only once A's handshake is torn down and its permits are
    // released. Note connecting proves nothing: the kernel completes the TCP handshake
    // into the listen backlog even while the accept loop is parked on `conn_limit`. So
    // B speaks, and waits to be spoken to — a rustls server that has actually accepted
    // the socket answers a bogus ClientHello with an alert (or closes); a server whose
    // accept loop is starved leaves B's read hanging forever, which is the pre-fix
    // behaviour this test is here to catch.
    let mut b = TcpStream::connect(addr).await.unwrap();
    b.write_all(b"\x16\x03\x01\x00\x05not a real ClientHello")
        .await
        .unwrap();
    let mut buf = [0u8; 8];
    tokio::time::timeout(Duration::from_secs(5), b.read(&mut buf))
        .await
        .expect(
            "the accept loop never came back round: the stalled TLS handshake is still \
                 holding the only connection permit",
        )
        .ok();

    // And A is gone, not merely overtaken: the slot it held comes *back*. It was torn
    // down at the deadline rather than held for as long as the attacker cares to keep
    // the socket open — which is the whole claim. (B is on its way out too, having
    // been sent an alert, so wait for the pool to settle rather than sampling it.)
    let settled = tokio::time::timeout(Duration::from_secs(5), async {
        while gauges.pre_auth_limit.available_permits() < 8 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        settled.is_ok(),
        "the stalled handshake never released its antechamber slot ({} still in use)",
        8 - gauges.pre_auth_limit.available_permits(),
    );
}

/// HIK-103. A peer that completes the pre-auth handshake and then never drains its
/// receive window must not park the server in a pre-auth write while holding an
/// antechamber permit — the login deadline has to bound the *writes* of that window,
/// not only its reads (HIK-72 covered the reads).
///
/// The mock stream hands over a valid ClientHello and then returns `Poll::Pending` on
/// every write: a zero-window client that reads nothing back. Two halves, and the fix
/// is what makes the first pass:
///   * **bounded** — with a login deadline set, `handle_connection` is torn down at the
///     deadline (a [`WriteDeadlineExceeded`]) and the antechamber permit comes back.
///     Before the fix the write ignored the deadline and this hung for the full 5s.
///   * **unbounded** — with no deadline (the pre-fix write behaviour on *every* path),
///     the write parks and the permit stays held, proving the stall and the permit-hold
///     are real and that the deadline is precisely what releases them.
#[tokio::test]
async fn a_stalled_pre_auth_write_is_bounded_by_the_login_deadline() {
    /// Delivers a fixed ClientHello, then reads nothing more and never accepts a write —
    /// a peer with a zero receive window that has stopped draining the socket.
    struct StallWriter {
        hello: Vec<u8>,
        read_off: usize,
    }
    impl AsyncRead for StallWriter {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let this = self.get_mut();
            let remaining = &this.hello[this.read_off..];
            if remaining.is_empty() {
                // Handshake delivered; now silent. (The write blocks first regardless.)
                return std::task::Poll::Pending;
            }
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            this.read_off += n;
            std::task::Poll::Ready(Ok(()))
        }
    }
    impl AsyncWrite for StallWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Pending // zero receive window: never accepts a byte
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    // A valid 20-byte ClientHello: preamble + four version proposals (5.4 first).
    let client_hello = || {
        let mut h = Vec::new();
        h.extend_from_slice(&handshake::PREAMBLE);
        h.extend_from_slice(&[0, 0, 4, 5]);
        h.extend_from_slice(&[0, 0, 0, 0]);
        h.extend_from_slice(&[0, 0, 0, 0]);
        h.extend_from_slice(&[0, 0, 0, 0]);
        h
    };
    let (_root, ctx) = build_ctx("hik103_stalled_pre_auth_write");

    // Bounded: a login deadline is set, so the stalled reply write is torn down at it
    // and the antechamber permit is released. (`handle_connection` owns the permit, so
    // returning drops it.) Pre-fix, the write ignored the deadline and this hung 5s.
    let sem = Arc::new(Semaphore::new(1));
    let permit = sem.clone().try_acquire_owned().unwrap();
    assert_eq!(sem.available_permits(), 0, "the permit is held on entry");
    let pre_auth = PreAuth {
        permit: Some(permit),
        deadline: Some(TokioInstant::now() + Duration::from_millis(200)),
    };
    let stream = StallWriter {
        hello: client_hello(),
        read_off: 0,
    };
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        handle_connection(stream, ctx.clone(), pre_auth),
    )
    .await
    .expect("the stalled pre-auth write was never torn down at the login deadline");
    let err =
        outcome.expect_err("a stalled pre-auth write must surface an error, not close cleanly");
    assert!(
        err.downcast_ref::<WriteDeadlineExceeded>().is_some(),
        "the teardown must be the write-deadline breach, got: {err:?}"
    );
    assert_eq!(
        sem.available_permits(),
        1,
        "the antechamber permit must come back once the stalled write is torn down"
    );

    // Unbounded (the pre-fix write behaviour): no deadline, so the write parks forever
    // and keeps its permit. Sampled while the task is still alive — proof that the stall
    // and the permit-hold are real, and that the deadline above is what tears them down.
    let sem2 = Arc::new(Semaphore::new(1));
    let permit2 = sem2.clone().try_acquire_owned().unwrap();
    let pre_auth2 = PreAuth {
        permit: Some(permit2),
        deadline: None,
    };
    let stream2 = StallWriter {
        hello: client_hello(),
        read_off: 0,
    };
    let ctx2 = ctx.clone();
    let task = tokio::spawn(async move { handle_connection(stream2, ctx2, pre_auth2).await });
    tokio::time::sleep(Duration::from_millis(300)).await; // clears the handshake read, parks in the write
    assert!(
        !task.is_finished(),
        "with no write deadline the pre-auth write parks — the pre-fix behaviour"
    );
    assert_eq!(
        sem2.available_permits(),
        0,
        "a parked pre-auth write keeps holding its antechamber permit"
    );
    task.abort();
    let _ = std::fs::remove_dir_all(&_root);
}

/// The TLS handshake is bounded by whichever of the two deadlines lands first, and
/// by either one alone when the other is off.
#[tokio::test]
async fn tls_handshake_deadline_is_the_sooner_of_the_two_bounds() {
    let deadline_ms = |login_timeout_ms, tls_handshake_timeout_ms| {
        let (_root, ctx) = build_ctx_limited(
            "tls_deadline",
            TestLimits {
                login_timeout_ms,
                tls_handshake_timeout_ms,
                ..Default::default()
            },
        );
        let pre_auth = PreAuth::admit(&ctx).expect("antechamber is empty");
        let now = TokioInstant::now();
        pre_auth
            .tls_deadline(&ctx)
            .map(|dl| (dl - now).as_millis() as u64)
    };
    // The login window is the whole pre-auth budget, so a handshake bound inside it
    // is what binds; a login window shorter than the handshake bound overrides it.
    assert!(matches!(deadline_ms(10_000, 5_000), Some(ms) if (4_900..=5_000).contains(&ms)));
    assert!(matches!(deadline_ms(1_000, 5_000), Some(ms) if (900..=1_000).contains(&ms)));
    // Either alone still bounds the handshake. The `loginTimeoutMs = 0` row is the
    // one that matters: it is why the handshake gets its own knob at all.
    assert!(matches!(deadline_ms(0, 5_000), Some(ms) if (4_900..=5_000).contains(&ms)));
    assert!(matches!(deadline_ms(10_000, 0), Some(ms) if (9_900..=10_000).contains(&ms)));
    // Both off = unbounded. Documented as "do not".
    assert_eq!(deadline_ms(0, 0), None);
}

#[tokio::test]
async fn per_ip_cap_rejects_excess_from_one_source() {
    let (_root, ctx) = build_ctx_limited(
        "per_ip_cap",
        TestLimits {
            max_per_ip: 1,
            ..Default::default()
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conn_limit = Arc::new(Semaphore::new(1024)); // generous; isolate the per-IP gate
    let (_tx, rx) = tokio::sync::oneshot::channel::<String>();
    tokio::spawn(accept_loop(listener, ctx, None, conn_limit, rx));

    // First connection from 127.0.0.1 is fine.
    let _a = Client::connect(addr).await;
    // A second from the same source is accepted then dropped by the per-IP cap.
    let mut b = TcpStream::connect(addr).await.unwrap();
    let mut hs = Vec::new();
    hs.extend_from_slice(&handshake::PREAMBLE);
    hs.extend_from_slice(&[0, 0, 4, 5]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    hs.extend_from_slice(&[0, 0, 0, 0]);
    let _ = b.write_all(&hs).await;
    let mut reply = [0u8; 4];
    match tokio::time::timeout(Duration::from_secs(2), b.read_exact(&mut reply)).await {
        Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("second connection from the same source should be rejected"),
        Err(_) => panic!("server neither served nor rejected the per-IP excess"),
    }
}
