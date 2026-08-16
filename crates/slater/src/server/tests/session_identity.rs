// SPDX-License-Identifier: Apache-2.0
//! `session_identity` — see the parent module. Split out of the single 15k-line
//! `server/tests.rs`; a pure relocation, no test logic changed.

use super::*;

// ── HIK-123: session state must not outlive the identity it belongs to ──────────
//
// A Bolt connection can carry more than one principal (LOGOFF→LOGON, or a bare
// re-LOGON). Every one of these drives a *real socket* through the actual message
// loop, because the bug lived in the handlers' bookkeeping, not in a helper.

/// An ACL with a reader on the fixture graph and a second user who holds no grant
/// at all — the "next user on the pooled connection".
fn two_user_acl_json() -> serde_json::Value {
    serde_json::json!({
        "users": {
            // A: may read the fixture graph.
            "reporting": {
                "passwordArgon2id": hash_password("pw").unwrap(),
                "grants": { "people": ["read"] }
            },
            // B: authenticates fine, but is granted nothing anywhere.
            "intruder": {
                "passwordArgon2id": hash_password("pw2").unwrap(),
                "grants": {}
            }
        }
    })
}

fn two_user_ctx(tag: &str) -> (std::path::PathBuf, Arc<ConnCtx>) {
    build_ctx_limited(
        tag,
        TestLimits {
            acl_json: Some(two_user_acl_json()),
            ..Default::default()
        },
    )
}

/// (a) The cross-user read: A's buffered rows must not be drainable by B.
///
/// Before the fix, LOGOFF cleared only `sess.user`, so `sess.pending` still held A's
/// rows and `Request::Pull` handed them to B without ever looking at `sess.user`.
#[tokio::test]
async fn logoff_does_not_leave_the_prior_users_rows_for_the_next_user() {
    let (root, ctx) = two_user_ctx("server_hik123_pending");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;

    // A authenticates and RUNs, buffering rows it never pulls.
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::run("MATCH (n:Person) RETURN n.name AS name"))
        .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // A leaves; B takes the same connection.
    c.send(Client::logoff()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("intruder", "pw2")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // B pulls. Any RECORD here is A's data on B's session.
    c.send(Client::pull_all()).await;
    let (tag, _) = c.recv().await;
    assert_ne!(
        tag,
        message::tag::RECORD,
        "PULL after LOGOFF/LOGON returned the previous user's buffered rows"
    );
    assert_eq!(
        tag,
        message::tag::FAILURE,
        "a PULL with no RUN of its own must fail, not succeed silently"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The same leak reached without a LOGOFF at all — `authenticate` deliberately
/// permits re-LOGON on an authenticated session (token rotation), so the identity
/// can change while `pending` survives. Fixing only the LOGOFF handler leaves this
/// path open; it is why the clear lives in `authenticate` too.
#[tokio::test]
async fn a_bare_relogon_does_not_inherit_the_prior_users_rows() {
    let (root, ctx) = two_user_ctx("server_hik123_relogon");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;

    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::run("MATCH (n:Person) RETURN n.name AS name"))
        .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // No LOGOFF — B simply LOGONs over A.
    c.send(Client::logon("intruder", "pw2")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    c.send(Client::pull_all()).await;
    let (tag, _) = c.recv().await;
    assert_ne!(
        tag,
        message::tag::RECORD,
        "a re-LOGON inherited the previous user's buffered rows"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// (b) The read-ACL bypass: A's open-transaction graph must not carry B's RUN.
///
/// B holds no read grant on `people`, so the *only* way B's db-less RUN can be
/// served is the `tx_graph` arm short-circuiting `select_graph`/`can_read`. Before
/// the fix it did exactly that and returned A's graph.
#[tokio::test]
async fn logoff_does_not_leave_the_prior_users_transaction_graph_for_the_next_user() {
    let (root, ctx) = two_user_ctx("server_hik123_tx_graph");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;

    // A opens a transaction naming the graph → sess.tx_graph = Some("people").
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::begin_db("people")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // A leaves mid-transaction; B takes the connection.
    c.send(Client::logoff()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("intruder", "pw2")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // B runs a db-less query. It must be refused on B's own (empty) grants.
    c.send(Client::run_no_db("MATCH (n:Person) RETURN n.name AS name"))
        .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(
        tag,
        message::tag::FAILURE,
        "a db-less RUN was served from the prior user's transaction graph"
    );
    assert_eq!(
        fields[0].get("code").and_then(PsValue::as_str),
        Some(CODE_FORBIDDEN),
        "the refusal must be an authorization failure on B's grants"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// **Graph names must not be enumerable through the failure.** Any authenticated
/// user — including one holding no grant anywhere — may name an arbitrary graph in
/// `BEGIN {db: …}`. If an *existing* graph they cannot read fails differently from a
/// name the server does not host, the pair is an oracle: probe a name, read the
/// failure, learn whether this deployment serves it.
///
/// The oracle had three channels, so this asserts all three: the legacy `code`
/// (`…Security.Forbidden` vs `…Database.DatabaseNotFound`), the `message` text, and
/// the derived `gql_status` (`42000` vs `50000` — `Failure::gqlstatus` maps FORBIDDEN
/// into the syntax-or-access class, so a client reading only GQLSTATUS could still
/// tell the two apart).
///
/// The probed name is normalised out of the message before comparison, because the
/// message legitimately echoes back whatever the caller asked for — that is the
/// caller's own input and reveals nothing.
#[tokio::test]
async fn an_unreadable_graph_is_indistinguishable_from_a_missing_one() {
    let (root, ctx) = two_user_ctx("server_hik221_enumeration");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;

    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    // `intruder` authenticates but is granted nothing; `people` exists and is
    // readable only by `reporting`.
    c.send(Client::logon("intruder", "pw2")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // Everything the client can observe about a failure, with the probed name
    // replaced so the two responses are comparable.
    let observable = |fields: &[PsValue], probed: &str| -> Vec<(String, String)> {
        let PsValue::Map(m) = &fields[0] else {
            panic!("a FAILURE must carry a metadata map");
        };
        m.iter()
            .map(|(k, v)| {
                let raw = v.as_str().unwrap_or_default();
                (k.clone(), raw.replace(probed, "<probed>"))
            })
            .collect()
    };

    // (a) A graph that exists, which this user may not read.
    c.send(Client::begin_db("people")).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::FAILURE, "an ungranted BEGIN must fail");
    let unreadable = observable(&fields, "people");
    c.send(Client::reset()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // (b) A name the server does not host at all.
    c.send(Client::begin_db("no_such_graph")).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(
        tag,
        message::tag::FAILURE,
        "an unknown-graph BEGIN must fail"
    );
    let missing = observable(&fields, "no_such_graph");

    assert_eq!(
        unreadable, missing,
        "an existing-but-unreadable graph must be indistinguishable from a missing \
         one; any difference here is a graph-name oracle"
    );

    // The failure must not become a *new* oracle: the `available:` list is
    // `can_read`-filtered, so a user with no grants must be told about no graphs.
    let msg = missing
        .iter()
        .find(|(k, _)| k == "message")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    assert!(
        !msg.contains("people"),
        "the failure leaked a graph name the user cannot read: {msg}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// The `tx_graph` arm re-checks the ACL per RUN, not once at BEGIN — so a grant
/// revoked by an ACL hot-reload stops being served *inside* an open transaction,
/// with no identity change involved. Independent of the LOGOFF clear: this one
/// survives a correct session-state handoff.
#[tokio::test]
async fn a_grant_revoked_mid_transaction_stops_serving_reads() {
    let (root, ctx) = two_user_ctx("server_hik123_revoke");
    let acl_path = root.join("acl.json");
    // Hold the handle so the test can drive the reload itself: `snapshot()` does not
    // poll, and hanging the assertion on mtime-granularity polling would make it flaky
    // (or, worse, pass against a stale ACL for the wrong reason).
    let acl = ctx.acl.clone();
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;

    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::logon("reporting", "pw")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::begin_db("people")).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // In-transaction RUN is served while the grant stands.
    c.send(Client::run_no_db("MATCH (n:Person) RETURN n.name AS name"))
        .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    c.send(Client::pull_all()).await;
    assert_eq!(c.recv().await.0, message::tag::RECORD);
    while c.recv().await.0 == message::tag::RECORD {}

    // The operator revokes the read grant, and the hot-reload picks it up.
    std::fs::write(
        &acl_path,
        serde_json::json!({
            "users": {
                "reporting": {
                    "passwordArgon2id": hash_password("pw").unwrap(),
                    "grants": {}
                }
            }
        })
        .to_string(),
    )
    .unwrap();
    assert!(acl.reload(), "the revoked ACL must install");
    assert!(
        !acl.snapshot().can_read("reporting", "people"),
        "precondition: the grant is gone from the live ACL"
    );

    // The next RUN in the *same* transaction must not ride the BEGIN-time decision.
    c.send(Client::run_no_db("MATCH (n:Person) RETURN n.name AS name"))
        .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(
        tag,
        message::tag::FAILURE,
        "a read was served on a grant revoked mid-transaction"
    );
    assert_eq!(
        fields[0].get("code").and_then(PsValue::as_str),
        Some(CODE_FORBIDDEN)
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// HIK-90 regression: argon2id must not run on the reactor.
///
/// `#[tokio::test]` is a **current-thread** runtime — the one place a blocked reactor
/// is directly observable. Spawned tasks only advance when the test yields, and a
/// single `yield_now()` gives every ready task exactly one poll. If the verify runs
/// inline (the bug), that one trip through the scheduler costs `FLOOD × one verify`
/// — the whole server is deaf for that long. With the verify handed to a blocking
/// thread, the poll parks immediately and the reactor comes straight back.
///
/// The bound is calibrated against a *measured* verify on this machine and build
/// profile rather than a hard-coded millisecond count, so it neither flakes on a slow
/// box nor passes vacuously on a fast one.
#[tokio::test]
async fn concurrent_logons_do_not_block_the_reactor() {
    const FLOOD: usize = 8;
    let (root, ctx) = build_ctx("server_auth_off_reactor");

    // Calibrate: what one verify costs. An unknown principal deliberately burns a
    // full dummy hash (anti-enumeration), so this is the flood's per-attempt price.
    // The first unknown-principal verify also *mints* the lazy dummy hash — a second
    // argon2 — so warm it before timing anything.
    assert!(!verify_off_reactor(&ctx, "nobody", "wrong", None)
        .await
        .unwrap());
    let t0 = Instant::now();
    assert!(!verify_off_reactor(&ctx, "nobody", "wrong", None)
        .await
        .unwrap());
    let one_verify = t0.elapsed();
    assert!(
        one_verify >= Duration::from_millis(1),
        "argon2id should cost real time; measured {one_verify:?} — is the ACL path being skipped?"
    );

    let flood: Vec<_> = (0..FLOOD)
        .map(|_| {
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let mut sess = pre_auth_session();
                authenticate(&mut sess, &ctx, &logon_meta("nobody", "wrong")).await
            })
        })
        .collect();

    let t0 = Instant::now();
    tokio::task::yield_now().await;
    let reactor_stall = t0.elapsed();
    assert!(
        reactor_stall < one_verify,
        "the reactor was held for {reactor_stall:?} while {FLOOD} LOGONs verified \
             (one verify = {one_verify:?}) — the hash is running on a reactor worker"
    );

    // …and every attempt still failed: this is not a fast-path that skips the hash.
    for t in flood {
        let err = t.await.unwrap().unwrap_err();
        assert_eq!(err.code, CODE_UNAUTHORIZED);
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// The concurrency cap is what stops the fix from simply *moving* the denial of
/// service into tokio's 512-thread blocking pool (which query execution shares).
///
/// Two things are checked. The direct one: while a flood is in flight, no permit is
/// left, and once it drains every permit is back — the permit lives with the hash, not
/// with the caller, so a client that hangs up mid-`LOGON` cannot leak the cap. The
/// corroborating one: 6 verifies under a cap of 2 take several *waves* of a single
/// verify's wall time, i.e. they did not all run at once. (This box has more than two
/// cores, so uncapped they would not serialise on CPU alone.)
#[tokio::test]
async fn concurrent_verifies_are_capped() {
    const FLOOD: usize = 6;
    const CAP: usize = 2;
    let (root, ctx) = build_ctx_limited(
        "server_auth_capped",
        TestLimits {
            max_concurrent_auth: CAP,
            ..Default::default()
        },
    );
    assert_eq!(ctx.auth_limit.available_permits(), CAP);

    // Warm the lazily-minted dummy hash, then time a single verify (see
    // `concurrent_logons_do_not_block_the_reactor`).
    assert!(!verify_off_reactor(&ctx, "nobody", "wrong", None)
        .await
        .unwrap());
    let t0 = Instant::now();
    assert!(!verify_off_reactor(&ctx, "nobody", "wrong", None)
        .await
        .unwrap());
    let one_verify = t0.elapsed();

    let flood: Vec<_> = (0..FLOOD)
        .map(|_| {
            let ctx = ctx.clone();
            tokio::spawn(async move { verify_off_reactor(&ctx, "nobody", "wrong", None).await })
        })
        .collect();
    tokio::task::yield_now().await;
    assert_eq!(
        ctx.auth_limit.available_permits(),
        0,
        "every verify permit should be in use while a flood is queued"
    );

    let t0 = Instant::now();
    for t in flood {
        assert!(!t.await.unwrap().unwrap());
    }
    let elapsed = t0.elapsed();
    // FLOOD/CAP = 3 waves in principle; assert 2, so per-verify variance (the first
    // hash pays the cold 19 MiB allocation) cannot flake it. Uncapped, all FLOOD would
    // run together and this would land near a single verify.
    assert!(
        elapsed >= one_verify * 2,
        "{FLOOD} verifies under a cap of {CAP} finished in {elapsed:?} (one verify = \
             {one_verify:?}) — they cannot have been serialised into waves"
    );
    // The cap is fully released once the flood drains — the permit lives with the
    // hash, not with the caller.
    assert_eq!(ctx.auth_limit.available_permits(), CAP);
    let _ = std::fs::remove_dir_all(&root);
}

/// The unknown-principal path must keep burning a full argon2id verify against the
/// dummy hash: that equalisation is what stops username enumeration by timing, and
/// moving the hash off the reactor must not have "optimised" it away.
#[tokio::test]
async fn unknown_principal_still_pays_for_a_full_verify() {
    let (root, ctx) = build_ctx("server_auth_timing_equalised");

    // Warm the lazily-built dummy hash so its one-off mint is not counted.
    assert!(!verify_off_reactor(&ctx, "nobody", "wrong", None)
        .await
        .unwrap());

    let t0 = Instant::now();
    assert!(!verify_off_reactor(&ctx, "reporting", "wrong", None)
        .await
        .unwrap());
    let known_user = t0.elapsed();

    let t0 = Instant::now();
    assert!(!verify_off_reactor(&ctx, "no-such-user", "wrong", None)
        .await
        .unwrap());
    let unknown_user = t0.elapsed();

    // Same work, so the same order of magnitude. A skipped verify would be orders of
    // magnitude faster, which is exactly what the enumeration attack looks for.
    //
    // Two-sided (HIK-222). The old assertion was `unknown * 2 >= known`, which bounded
    // only the "unknown is suspiciously fast" direction and tolerated the known path
    // being up to 2x slower — so a deployment whose stored hashes were minted at
    // stronger-than-default parameters diverged silently. The unknown path must not be
    // conspicuously *slower* either: that is the same oracle with the sign flipped.
    assert!(
        unknown_user * 2 >= known_user,
        "an unknown principal took {unknown_user:?} against {known_user:?} for a known \
             one — the timing equalisation is gone"
    );
    assert!(
        known_user * 4 >= unknown_user,
        "an unknown principal took {unknown_user:?} against {known_user:?} for a known \
             one — the unknown path is conspicuously slower, which enumerates just as \
             well as being faster"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// **The equalisation must track hashes the operator minted elsewhere.** `acl.json`
/// accepts any valid PHC string, and argon2 verification derives at the *stored*
/// parameters — so a deployment whose hashes came from a third-party tool used to run
/// its known-user path at one cost and its unknown-user path at `Argon2::default()`'s.
/// That divergence is username enumeration by timing, and it was live for anyone not
/// using `slater hash-password`. (HIK-222)
///
/// The mechanism is pinned deterministically by `acl::tests::the_equalisation_hash_*`;
/// this is the end-to-end backstop that the wall-clock actually follows.
#[tokio::test]
async fn an_unknown_principal_tracks_non_default_stored_parameters() {
    // Deliberately *cheaper* than the default (m=19456, t=2), so the fixture is quick:
    // the divergence to detect is the same either way, and argon2 is unoptimised in a
    // debug build. Pre-fix the unknown path burns the ~19 MiB default dummy against
    // this user's 64 KiB hash — a large, easily-measured gap in the wrong direction.
    let weak = {
        let params = argon2::Params::new(64, 1, 1, None).unwrap();
        let a = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
        let salt = argon2::password_hash::SaltString::generate(
            &mut argon2::password_hash::rand_core::OsRng,
        );
        argon2::password_hash::PasswordHasher::hash_password(&a, b"pw", &salt)
            .unwrap()
            .to_string()
    };
    let (root, ctx) = build_ctx_limited(
        "server_auth_timing_nondefault",
        TestLimits {
            acl_json: Some(serde_json::json!({
                "users": { "weak": { "passwordArgon2id": weak, "grants": {} } }
            })),
            ..Default::default()
        },
    );

    // Warm any lazily-built state so a one-off mint is not counted.
    assert!(!verify_off_reactor(&ctx, "nobody", "wrong", None)
        .await
        .unwrap());

    let t0 = Instant::now();
    assert!(!verify_off_reactor(&ctx, "weak", "wrong", None)
        .await
        .unwrap());
    let known_user = t0.elapsed();

    let t0 = Instant::now();
    assert!(!verify_off_reactor(&ctx, "no-such-user", "wrong", None)
        .await
        .unwrap());
    let unknown_user = t0.elapsed();

    // Both derive at m=64,t=1 now, so they land within a small factor. The pre-fix gap
    // is ~300x (19456*2 vs 64*1 block computations), so a loose bound still catches it
    // decisively while leaving ample room for scheduler noise on a tiny workload.
    assert!(
        unknown_user * 10 >= known_user && known_user * 10 >= unknown_user,
        "unknown {unknown_user:?} vs known {known_user:?}: the unknown-principal path \
             is not deriving at the stored parameters, so a username can be found by \
             timing on any deployment that minted its hashes elsewhere"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The login deadline bounds the *wait* for a verify permit — a queued attempt must
/// not outlive the login window it belongs to. But that window is the **pre-auth** one:
/// it must not be turned around and used to refuse a re-auth (LOGON without LOGOFF —
/// token rotation) on a session that authenticated long ago, whose deadline is in the
/// past by construction. (An expired deadline only bites when the acquire actually has
/// to wait; with a permit free, `timeout_at` takes it and never trips.)
#[tokio::test]
async fn an_expired_login_deadline_bounds_the_pre_auth_wait_only() {
    let (root, ctx) = build_ctx_limited(
        "server_auth_deadline",
        TestLimits {
            max_concurrent_auth: 1,
            ..Default::default()
        },
    );
    let expired = TokioInstant::now() - Duration::from_secs(1);

    // Occupy the single verify permit, so the next attempt must queue.
    let hog = {
        let ctx = ctx.clone();
        tokio::spawn(async move { verify_off_reactor(&ctx, "nobody", "wrong", None).await })
    };
    tokio::task::yield_now().await;
    assert_eq!(ctx.auth_limit.available_permits(), 0);

    // Unauthenticated and past its deadline: refused rather than queued — even with
    // the right password, so an anonymous flood cannot sit in the queue for ever.
    let mut anon = pre_auth_session();
    anon.login_deadline = Some(expired);
    let err = authenticate(&mut anon, &ctx, &logon_meta("reporting", "pw"))
        .await
        .unwrap_err();
    assert_eq!(err.code, CODE_UNAUTHORIZED);
    assert!(
        anon.user.is_none(),
        "a timed-out attempt must not authenticate"
    );

    // Already authenticated: the same expired deadline must NOT refuse a re-auth. It
    // waits for the permit (which the hog is holding) and then verifies.
    let mut live = pre_auth_session();
    live.user = Some("reporting".into());
    live.login_deadline = Some(expired);
    authenticate(&mut live, &ctx, &logon_meta("reporting", "pw"))
        .await
        .unwrap();
    assert_eq!(live.user.as_deref(), Some("reporting"));

    assert!(!hog.await.unwrap().unwrap());
    let _ = std::fs::remove_dir_all(&root);
}

/// A connection gets a small allowance of failed LOGONs and is then hung up on, so a
/// single socket cannot keep queueing verifies for its whole login window.
#[tokio::test]
async fn repeated_bad_logons_close_the_connection() {
    let (root, ctx) = build_ctx_limited(
        "server_auth_attempt_cap",
        TestLimits {
            max_auth_failures: 2,
            ..Default::default()
        },
    );
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // A failed LOGON puts the connection in the Bolt FAILED state, so a stuffer must
    // RESET between guesses — that is the attempt loop the cap has to bound.
    c.send(Client::logon("reporting", "wrong")).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::FAILURE);
    assert_eq!(
        fields[0].get("code").and_then(PsValue::as_str),
        Some(CODE_UNAUTHORIZED)
    );
    c.send(Client::reset()).await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);

    // Second failure spends the allowance: the FAILURE is still reported…
    c.send(Client::logon("reporting", "wrong")).await;
    assert_eq!(c.recv().await.0, message::tag::FAILURE);

    // …and then the server hangs up — RESET does not launder the attempt count, and no
    // further guess on this socket ever reaches the hash.
    let mut tmp = [0u8; 64];
    let n = c.stream.read(&mut tmp).await.unwrap();
    assert_eq!(
        n, 0,
        "the connection should have been closed after 2 failed LOGONs"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A ctx whose fixture graph has the writable layer on, with an explicit
/// `maxConcurrentWrites` cap — the harness for the write-gate tests.
fn build_gated_write_ctx(tag: &str, max_concurrent_writes: usize) -> (PathBuf, Arc<ConnCtx>) {
    build_ctx_limited(
        tag,
        TestLimits {
            writable: true,
            max_concurrent_writes,
            ..Default::default()
        },
    )
}

/// A batched `UNWIND … MERGE … SET` over `rows` fresh business keys, prefixed by `tag`
/// so concurrent jobs never touch the same key. Batched on purpose: it is a *single*
/// write (one resolve sweep, one group commit, one fsync) that costs enough wall time
/// to be measured against, which is what the reactor-stall calibration needs.
fn batch_write_job(tag: &str, rows: usize) -> (WriteJob, HashMap<String, Val>) {
    let list = Val::List(
        (0..rows)
            .map(|i| {
                Val::Map(vec![
                    ("name".into(), Val::Str(format!("{tag}-{i}"))),
                    ("age".into(), Val::Int(i as i64)),
                ])
            })
            .collect(),
    );
    let params = HashMap::from([("rows".to_string(), list)]);
    let stmt = match parser::parse_statement(
        "UNWIND $rows AS r MERGE (n:Person {name: r.name}) SET n.age = r.age",
    )
    .unwrap()
    {
        parser::ast::Statement::Write(w) => w,
        _ => panic!("expected a node write"),
    };
    (WriteJob::Node(Box::new(stmt)), params)
}

/// Live node count over the core ⊕ delta — proof a write actually landed.
fn overlaid_node_count(ctx: &Arc<ConnCtx>, graph: &str) -> i64 {
    let gen = ctx.graphs.get(graph).unwrap();
    let writer = ctx.graphs.writer(graph).unwrap();
    let view = MergedView::new(gen.as_ref(), writer.delta_snapshot());
    let res = Engine::new(&view, ctx.cache.as_ref())
        .run(&parser::parse("MATCH (n:Person) RETURN count(*)").unwrap())
        .unwrap();
    match res.rows[0][0] {
        Val::Int(n) => n,
        ref v => panic!("count is not an int: {v:?}"),
    }
}

/// HIK-87 regression: write execution must not run on the reactor.
///
/// `#[tokio::test]` is a **current-thread** runtime — the one place a blocked reactor is
/// directly observable. Spawned tasks only advance when the test yields, and a single
/// `yield_now()` gives every ready task exactly one poll. If the writes run inline (the
/// bug), that one trip through the scheduler costs FLOOD × one write — the whole server
/// is deaf for that long, every other connection on that worker included. With the write
/// handed to a blocking thread, each poll parks immediately and the reactor comes
/// straight back.
///
/// The bound is calibrated against a *measured* write on this box and build profile
/// rather than a hard-coded millisecond, so it neither flakes on a slow machine nor
/// passes vacuously on a fast one.
#[tokio::test]
async fn writes_do_not_block_the_reactor() {
    const FLOOD: usize = 8;
    const ROWS: usize = 500;
    let (root, ctx) = build_gated_write_ctx("server_writes_off_reactor", 4);
    let gen = ctx.graphs.get("people").unwrap();
    let writer = ctx.graphs.writer("people").expect("writable layer is on");

    // Calibrate: what one write of this shape costs. Warm first — the first write mints
    // the WAL segment and faults in the ISAM blocks the resolve sweeps.
    let (job, params) = batch_write_job("warm", ROWS);
    execute_write_off_reactor(&ctx, &writer, &gen, job, params, TEST_BOLT_VERSION)
        .await
        .unwrap();
    let (job, params) = batch_write_job("calibrate", ROWS);
    let t0 = Instant::now();
    execute_write_off_reactor(&ctx, &writer, &gen, job, params, TEST_BOLT_VERSION)
        .await
        .unwrap();
    let one_write = t0.elapsed();
    assert!(
        one_write >= Duration::from_millis(1),
        "a {ROWS}-row group commit should cost real time; measured {one_write:?} — is the \
             write actually resolving and fsyncing?"
    );

    // Build the jobs up front: parsing and materialising the rows is caller-side work,
    // and it must not be confused with the execution we are timing.
    let jobs: Vec<_> = (0..FLOOD)
        .map(|i| batch_write_job(&format!("flood-{i}"), ROWS))
        .collect();
    let flood: Vec<_> = jobs
        .into_iter()
        .map(|(job, params)| {
            let ctx = ctx.clone();
            let writer = writer.clone();
            let gen = gen.clone();
            tokio::spawn(async move {
                execute_write_off_reactor(&ctx, &writer, &gen, job, params, TEST_BOLT_VERSION).await
            })
        })
        .collect();

    let t0 = Instant::now();
    tokio::task::yield_now().await;
    let reactor_stall = t0.elapsed();
    assert!(
        reactor_stall < one_write,
        "the reactor was held for {reactor_stall:?} while {FLOOD} writes executed (one \
             write = {one_write:?}) — write execution is running on a reactor worker"
    );

    // …and every write still committed: this is not a fast path that skipped the work.
    for t in flood {
        t.await.unwrap().unwrap();
    }
    assert_eq!(
        overlaid_node_count(&ctx, "people"),
        3 + ((FLOOD + 2) * ROWS) as i64,
        "3 fixture people + every row of the warm, calibration and flood batches"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// The concurrency cap is what stops the fix from simply *moving* the denial of service
/// into tokio's 512-thread blocking pool — the pool query execution runs on. Writes to
/// one graph serialise behind that graph's single `DeltaWriter` lock, so an uncapped
/// `spawn_blocking` would hand the pool an unbounded queue of tasks that immediately
/// park on a mutex, and reads would starve behind them.
///
/// While a flood is in flight no permit is left; once it drains every permit is back —
/// and every write committed, in one graph's serialised order.
#[tokio::test]
async fn concurrent_writes_are_capped() {
    const FLOOD: usize = 6;
    const CAP: usize = 2;
    const ROWS: usize = 500;
    let (root, ctx) = build_gated_write_ctx("server_writes_capped", CAP);
    assert_eq!(ctx.write_limit.available_permits(), CAP);
    let gen = ctx.graphs.get("people").unwrap();
    let writer = ctx.graphs.writer("people").expect("writable layer is on");

    let jobs: Vec<_> = (0..FLOOD)
        .map(|i| batch_write_job(&format!("capped-{i}"), ROWS))
        .collect();
    let flood: Vec<_> = jobs
        .into_iter()
        .map(|(job, params)| {
            let ctx = ctx.clone();
            let writer = writer.clone();
            let gen = gen.clone();
            tokio::spawn(async move {
                execute_write_off_reactor(&ctx, &writer, &gen, job, params, TEST_BOLT_VERSION).await
            })
        })
        .collect();
    tokio::task::yield_now().await;
    assert_eq!(
        ctx.write_limit.available_permits(),
        0,
        "every write permit should be in use while a flood is queued"
    );

    for t in flood {
        t.await.unwrap().unwrap();
    }
    // The cap is fully released once the flood drains — the permit lives with the write,
    // not with the caller — and no write was lost to the gate.
    assert_eq!(ctx.write_limit.available_permits(), CAP);
    assert_eq!(
        overlaid_node_count(&ctx, "people"),
        3 + (FLOOD * ROWS) as i64,
        "every capped write committed its whole batch"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A `spawn_blocking` task cannot be cancelled: if the client hangs up mid-write, the
/// await on the join handle is dropped but the write runs to completion — as it must,
/// since its WAL append and fsync may already have happened (we never un-commit a
/// durable write; we simply never get to ack it).
///
/// So the permit is moved *into* the closure. Held in the async frame instead, it would
/// be released the instant the caller was cancelled while the write still ran — and a
/// flood of clients that disconnect mid-write could overrun the cap at will, which is
/// exactly the blocking-pool starvation the cap exists to prevent.
#[tokio::test]
async fn an_abandoned_write_holds_its_permit_and_still_commits() {
    const ROWS: usize = 2_000;
    const CAP: usize = 2;
    let (root, ctx) = build_gated_write_ctx("server_write_abandoned", CAP);
    let gen = ctx.graphs.get("people").unwrap();
    let writer = ctx.graphs.writer("people").expect("writable layer is on");

    let (job, params) = batch_write_job("abandoned", ROWS);
    let task = {
        let ctx = ctx.clone();
        let writer = writer.clone();
        let gen = gen.clone();
        tokio::spawn(async move {
            execute_write_off_reactor(&ctx, &writer, &gen, job, params, TEST_BOLT_VERSION).await
        })
    };
    // One poll: the permit is taken and the write is handed to the blocking pool.
    tokio::task::yield_now().await;
    assert_eq!(ctx.write_limit.available_permits(), CAP - 1);

    // The client hangs up: the caller is cancelled, the write is not.
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        ctx.write_limit.available_permits(),
        CAP - 1,
        "an abandoned write must keep its permit while it is still running — releasing it \
             at cancellation lets a hung-up client overrun the cap"
    );

    // It runs to completion and its rows are durable, permit released only then.
    while ctx.write_limit.available_permits() < CAP {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        overlaid_node_count(&ctx, "people"),
        3 + ROWS as i64,
        "the abandoned write committed; a durable write is never rolled back because the \
             client stopped listening"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn write_query_is_rejected_read_only() {
    let (root, ctx) = build_ctx("server_readonly");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "pw")).await;
    c.recv().await;

    c.send(Client::run("CREATE (n:Person {name: 'Mallory'})"))
        .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::FAILURE);
    assert_eq!(
        fields[0].get("code").and_then(PsValue::as_str),
        Some(CODE_ACCESS_MODE)
    );

    // After a FAILURE the connection is FAILED: a further RUN is IGNORED until RESET.
    c.send(Client::run("MATCH (n) RETURN n")).await;
    assert_eq!(c.recv().await.0, message::tag::IGNORED);
    c.send(PsValue::Struct {
        tag: message::tag::RESET,
        fields: vec![],
    })
    .await;
    assert_eq!(c.recv().await.0, message::tag::SUCCESS);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn vector_knn_query_returns_nodes_and_scores_over_bolt() {
    let (root, ctx) = build_ctx("server_knn");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "pw")).await;
    c.recv().await;

    // Query equals Alice's embedding → Alice (id 0) is the nearest, score ~0.
    c.send(Client::run(
        "CALL db.idx.vector.queryNodes('Person', 'embedding', 2, vecf32([0.1, 0.2, 0.3])) \
             YIELD node, score RETURN id(node) AS id, score",
    ))
    .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    assert_eq!(
        fields[0].get("fields"),
        Some(&PsValue::List(vec![
            PsValue::str("id"),
            PsValue::str("score")
        ]))
    );

    c.send(Client::pull_all()).await;
    let mut ids = Vec::new();
    loop {
        let (tag, fields) = c.recv().await;
        if tag == message::tag::RECORD {
            if let PsValue::List(vals) = &fields[0] {
                ids.push(vals[0].as_int().unwrap());
                // First hit is the exact match: score ~0.
                if ids.len() == 1 {
                    match &vals[1] {
                        PsValue::Float(f) => assert!(f.abs() < 1e-6, "exact match score ~0"),
                        other => panic!("score should be a float, got {other:?}"),
                    }
                }
            }
        } else {
            assert_eq!(tag, message::tag::SUCCESS);
            break;
        }
    }
    assert_eq!(ids, vec![0, 2], "Alice (exact) then Carol");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn meta_stats_procedure_returns_counts_over_bolt() {
    // Phase 11: a metadata CALL flows through the normal RUN/PULL query path
    // (it is NOT a pre-parse interception), so its Map output is PackStream-
    // encoded like any other value.
    let (root, ctx) = build_ctx("server_metastats");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "pw")).await;
    c.recv().await;

    c.send(Client::run(
        "CALL db.meta.stats() YIELD labels, nodeCount, relCount RETURN labels, nodeCount, relCount",
    ))
    .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    assert_eq!(
        fields[0].get("fields"),
        Some(&PsValue::List(vec![
            PsValue::str("labels"),
            PsValue::str("nodeCount"),
            PsValue::str("relCount"),
        ]))
    );

    c.send(Client::pull_all()).await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::RECORD);
    let PsValue::List(vals) = &fields[0] else {
        panic!("expected a record list, got {:?}", fields[0]);
    };
    // labels is a {label: count} map; nodeCount/relCount are the scalar totals.
    assert_eq!(vals[0].get("Person"), Some(&PsValue::Int(3)));
    assert_eq!(vals[0].get("Company"), Some(&PsValue::Int(2)));
    assert_eq!(vals[1].as_int(), Some(5));
    assert_eq!(vals[2].as_int(), Some(5));

    let (tag, _) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn whole_graph_reltype_metadata_over_bolt() {
    // The unanchored introspection queries that broke the incident, answered
    // over the wire from resident metadata. Fixture: KNOWS×3, WORKS_AT×2.
    let (root, ctx) = build_ctx("server_reltype_meta");
    let addr = spawn_server(ctx).await;
    let mut c = Client::connect(addr).await;
    c.send(Client::hello()).await;
    c.recv().await;
    c.send(Client::logon("reporting", "pw")).await;
    c.recv().await;

    // A1 — DISTINCT type(r): one column `t`, one record per reltype.
    c.send(Client::run("MATCH ()-[r]->() RETURN DISTINCT type(r) AS t"))
        .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    assert_eq!(
        fields[0].get("fields"),
        Some(&PsValue::List(vec![PsValue::str("t")]))
    );
    c.send(Client::pull_all()).await;
    let mut types = Vec::new();
    loop {
        let (tag, fields) = c.recv().await;
        if tag == message::tag::RECORD {
            let PsValue::List(vals) = &fields[0] else {
                panic!("expected a record list, got {:?}", fields[0]);
            };
            types.push(vals[0].as_str().unwrap().to_string());
        } else {
            assert_eq!(tag, message::tag::SUCCESS);
            break;
        }
    }
    types.sort();
    assert_eq!(types, vec!["KNOWS".to_string(), "WORKS_AT".to_string()]);

    // B1 — type(r), count(*): two columns, per-reltype edge counts.
    c.send(Client::run(
        "MATCH ()-[r]->() RETURN type(r) AS t, count(*) AS c",
    ))
    .await;
    let (tag, fields) = c.recv().await;
    assert_eq!(tag, message::tag::SUCCESS);
    assert_eq!(
        fields[0].get("fields"),
        Some(&PsValue::List(vec![PsValue::str("t"), PsValue::str("c")]))
    );
    c.send(Client::pull_all()).await;
    let mut counts = std::collections::HashMap::new();
    loop {
        let (tag, fields) = c.recv().await;
        if tag == message::tag::RECORD {
            let PsValue::List(vals) = &fields[0] else {
                panic!("expected a record list, got {:?}", fields[0]);
            };
            counts.insert(
                vals[0].as_str().unwrap().to_string(),
                vals[1].as_int().unwrap(),
            );
        } else {
            assert_eq!(tag, message::tag::SUCCESS);
            break;
        }
    }
    assert_eq!(counts.get("KNOWS"), Some(&3));
    assert_eq!(counts.get("WORKS_AT"), Some(&2));
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn identical_query_is_served_from_the_result_cache() {
    let (root, ctx) = build_ctx("server_resultcache");
    let addr = spawn_server(ctx.clone()).await;

    let drive = move |query: &'static str| async move {
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        c.recv().await;
        c.send(Client::logon("reporting", "pw")).await;
        c.recv().await;
        c.send(Client::run(query)).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::pull_all()).await;
        let mut rows = 0;
        loop {
            let (tag, _) = c.recv().await;
            if tag == message::tag::RECORD {
                rows += 1;
            } else {
                break;
            }
        }
        rows
    };

    let q = "MATCH (n:Person) RETURN n.name AS name ORDER BY name";
    let first = drive(q).await;
    let after_first = ctx.result_cache.metrics();
    assert_eq!(after_first.misses, 1, "first run is a cache miss");
    assert_eq!(ctx.result_cache.len(), 1);

    let second = drive(q).await;
    let after_second = ctx.result_cache.metrics();
    assert_eq!(first, second, "both runs return the same row count");
    assert_eq!(after_second.misses, 1, "second run adds no miss");
    assert!(after_second.hits >= 1, "second run is a cache hit");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn nondeterministic_query_bypasses_the_result_cache() {
    let (root, ctx) = build_ctx("server_resultcache_nd");
    let addr = spawn_server(ctx.clone()).await;

    let drive = move |query: &'static str| async move {
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        c.recv().await;
        c.send(Client::logon("reporting", "pw")).await;
        c.recv().await;
        c.send(Client::run(query)).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::pull_all()).await;
        loop {
            let (tag, _) = c.recv().await;
            if tag != message::tag::RECORD {
                break;
            }
        }
    };

    // A query calling timestamp() is never written to (or read from) the cache.
    let q = "RETURN timestamp() AS t";
    drive(q).await;
    drive(q).await;
    let m = ctx.result_cache.metrics();
    assert_eq!(
        ctx.result_cache.len(),
        0,
        "non-deterministic query is not cached"
    );
    assert_eq!(m.hits, 0, "no cache hit for a non-deterministic query");

    // Sanity: a deterministic query in the same context still caches normally.
    drive("RETURN 1 AS one").await;
    assert_eq!(ctx.result_cache.len(), 1, "deterministic query is cached");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn open_all_discovers_the_fixture_graph() {
    let (root, _graph, _) = testgen::write_basic("server_openall");
    let graphs = Graphs::open_all(&root, None).unwrap();
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs.names(), vec!["people".to_string()]);
    assert!(graphs.get("people").is_some());
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn tls_acceptor_is_none_when_disabled() {
    let cfg = TlsConfig::default();
    assert!(!cfg.enabled());
    assert!(build_tls_acceptor(&cfg).unwrap().is_none());
}
