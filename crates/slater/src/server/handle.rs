// SPDX-License-Identifier: Apache-2.0
//! Per-connection handling: handshake, auth, and the RUN/PULL request loop.
//!
//! Split out of `server.rs` as a child module (a pure relocation). Shared types,
//! consts and helpers stay in the parent, reached via `use super::*`; the parent
//! re-exports this module's items so sibling modules can call them by name.

use super::*;

/// Run one Bolt connection from handshake to close.
///
/// `pre_auth` carries the antechamber slot and the login deadline, both already armed at
/// the TCP `accept()` by [`serve_conn`] — the deadline therefore covers the TLS handshake
/// that has just happened as well as the Bolt handshake about to happen, as one budget.
pub(crate) async fn handle_connection<S>(
    stream: S,
    ctx: Arc<ConnCtx>,
    pre_auth: PreAuth,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    // Antechamber slot, held until LOGON succeeds; the deadline the pre-auth phase (TLS
    // handshake → Bolt handshake → HELLO → LOGON) must finish within — the slow-loris
    // guard a byte cap alone leaves open.
    let PreAuth {
        permit: mut pre_auth_permit,
        deadline: login_deadline,
    } = pre_auth;

    // Start under the tight pre-auth body cap; it ratchets up once LOGON succeeds. The
    // write deadline is armed to the same login deadline that bounds the pre-auth reads, so
    // the handshake reply below is on the budget too (HIK-103); it is cleared on LOGON.
    let mut framed = Framed::new(stream, ctx.max_pre_auth_bytes);
    framed.write_deadline = login_deadline;

    // Handshake: 20 bytes (preamble + four proposals), reply with the agreed
    // 4-byte version, or four zero bytes if we share none (the client disconnects).
    let mut hello = [0u8; 20];
    match login_deadline {
        Some(dl) => timeout_at(dl, framed.stream.read_exact(&mut hello))
            .await
            .map_err(|_| {
                ctx.diag.record_login_timeout();
                anyhow!("handshake not completed within the login deadline")
            })?
            .context("read handshake")?,
        None => framed
            .stream
            .read_exact(&mut hello)
            .await
            .context("read handshake")?,
    };
    let reply = handshake::handle_client_hello(&hello)?;
    framed.write_all_bounded(&reply).await?;
    framed.flush().await?;
    if reply == handshake::NO_VERSION {
        return Ok(());
    }
    let mut sess = Session {
        user: None,
        failed: false,
        pending: None,
        tx_graph: None,
        version: (reply[3], reply[2]),
        auth_failures: 0,
        login_deadline,
    };

    loop {
        // Sync the per-connection budgets to the current auth state before each read.
        // The framer is auth-blind, so its cap is set here; the pre-auth permit is
        // released on the transition to authenticated and reclaimed on LOGOFF.
        if sess.user.is_some() {
            framed.max_body = ctx.max_message_bytes;
            // Authenticated: leave writes unbounded — the client is trusted to read its own
            // (possibly large) results at its own pace; a slow reader here is back-pressure,
            // not the pre-auth slow-loris the deadline defends against (HIK-103).
            framed.write_deadline = None;
            pre_auth_permit = None; // free the antechamber slot for the next anon peer
        } else {
            framed.max_body = ctx.max_pre_auth_bytes;
            // Unauthenticated: the login deadline bounds this window's writes as well as its
            // reads, so a stalled pre-auth reply cannot pin an antechamber permit (HIK-103).
            framed.write_deadline = login_deadline;
            if pre_auth_permit.is_none() {
                // Returned to unauthenticated (LOGOFF / re-auth): reclaim a slot or
                // close. A logged-off connection must not keep the generous budget,
                // nor sit anonymous outside the antechamber cap.
                match ctx.pre_auth_limit.clone().try_acquire_owned() {
                    Ok(p) => pre_auth_permit = Some(p),
                    Err(_) => {
                        debug!("pre-auth budget full on re-auth; closing connection");
                        ctx.diag.record_rejected_pre_auth();
                        break;
                    }
                }
            }
        }

        // Read the next message under the auth-appropriate deadline.
        let read = if sess.user.is_some() {
            match ctx.idle_timeout_ms {
                0 => framed.read_message().await,
                ms => match timeout(Duration::from_millis(ms), framed.read_message()).await {
                    Ok(r) => r,
                    Err(_) => {
                        debug!("authenticated connection idle past the timeout; closing");
                        ctx.diag.record_idle_timeout();
                        return Ok(());
                    }
                },
            }
        } else {
            match login_deadline {
                Some(dl) => match timeout_at(dl, framed.read_message()).await {
                    Ok(r) => r,
                    Err(_) => {
                        debug!("login deadline exceeded before authentication; closing");
                        ctx.diag.record_login_timeout();
                        return Ok(());
                    }
                },
                None => framed.read_message().await,
            }
        };
        let body = match read {
            Ok(Some(b)) => b,
            Ok(None) => break, // clean EOF
            Err(e) => {
                // Classify a reassembly-cap breach for diagnostics before the error
                // propagates and closes the connection. `sess.user.is_none()` selects
                // the pre-auth vs authenticated counter (the caps differ by auth state).
                if e.to_string().contains("exceed") {
                    ctx.diag.record_msg_too_large(sess.user.is_none());
                }
                return Err(e);
            }
        };

        let req = match message::decode_request(&body) {
            Ok(r) => r,
            Err(e) => {
                framed
                    .write_message(&message::failure(CODE_REQUEST, &e.to_string()))
                    .await?;
                framed.flush().await?;
                sess.failed = true;
                continue;
            }
        };

        // GOODBYE closes; RESET clears a failed/streaming state unconditionally.
        match &req {
            message::Request::Goodbye => break,
            message::Request::Reset => {
                sess.failed = false;
                sess.clear_user_state();
                framed.write_message(&message::success(vec![])).await?;
                framed.flush().await?;
                continue;
            }
            _ => {}
        }

        if sess.failed {
            framed.write_message(&message::ignored()).await?;
            framed.flush().await?;
            continue;
        }

        match handle_request(&mut sess, &ctx, req).await {
            Ok(msgs) => {
                for m in &msgs {
                    framed.write_message(m).await?;
                }
                framed.flush().await?;
            }
            Err(f) => {
                framed.write_message(&f.to_message()).await?;
                framed.flush().await?;
                sess.failed = true;
            }
        }

        // Per-connection auth-attempt cap. The failure has been reported, so hang up:
        // a socket that has burned its allowance must not keep queueing argon2 verifies
        // (each ~19 MiB and tens of ms) for the rest of its login window. Per connection,
        // never per account — this cannot be used to lock a victim's user out.
        if ctx.max_auth_failures > 0 && sess.auth_failures >= ctx.max_auth_failures {
            debug!(
                failures = sess.auth_failures,
                "authentication-attempt cap reached; closing connection"
            );
            break;
        }
    }
    Ok(())
}

/// Verify `basic`-scheme credentials from a `LOGON` (or 4.4 `HELLO`) metadata map
/// against the ACL, recording the user on the session on success.
///
/// The credential check itself is deliberately *not* done here — see
/// [`verify_off_reactor`], which runs it on a blocking thread under a concurrency cap.
pub(crate) async fn authenticate(
    sess: &mut Session,
    ctx: &Arc<ConnCtx>,
    meta: &PsValue,
) -> std::result::Result<(), Failure> {
    let scheme = meta.get("scheme").and_then(PsValue::as_str).unwrap_or("");
    if scheme != "basic" {
        sess.auth_failures = sess.auth_failures.saturating_add(1);
        ctx.diag.record_auth_failure();
        return Err(Failure::unauthorized(
            "only the 'basic' authentication scheme is supported",
        ));
    }
    let principal = meta
        .get("principal")
        .and_then(PsValue::as_str)
        .unwrap_or("")
        .to_string();
    let credentials = meta
        .get("credentials")
        .and_then(PsValue::as_str)
        .unwrap_or("")
        .to_string();

    // The login deadline governs the *pre-auth* window, so it bounds the wait for a
    // verify permit only while the session is unauthenticated: an anonymous peer's queued
    // attempt cannot outlive the window it belongs to. A LOGON on an already-authenticated
    // session (re-auth / token rotation, without a LOGOFF) is past that window by
    // construction and must not be refused by it.
    let deadline = sess.login_deadline.filter(|_| sess.user.is_none());
    let verified = verify_off_reactor(ctx, &principal, &credentials, deadline).await;
    match verified {
        Ok(true) => {
            sess.auth_failures = 0;
            // A LOGON is an identity transition even without a preceding LOGOFF (see the
            // deadline note above: re-auth / token rotation is explicitly allowed), so the
            // outgoing principal's state goes here too — otherwise `A LOGON → RUN →
            // B LOGON` leaks exactly what fixing LOGOFF alone would close (HIK-123).
            // Unconditional, not `if principal != old`: re-authenticating as the *same*
            // name may still pick up a hot-reloaded ACL that revoked the grant which
            // resolved `tx_graph`, and Bolt only permits LOGON from READY, where a
            // caller has no stream left to lose.
            sess.clear_user_state();
            sess.user = Some(principal);
            Ok(())
        }
        Ok(false) => {
            sess.auth_failures = sess.auth_failures.saturating_add(1);
            ctx.diag.record_auth_failure();
            Err(Failure::unauthorized("invalid principal or credentials"))
        }
        Err(f) => {
            sess.auth_failures = sess.auth_failures.saturating_add(1);
            ctx.diag.record_auth_failure();
            Err(f)
        }
    }
}

/// Poll the ACL and verify one credential pair **off the reactor**, under a concurrency
/// cap. Returns whether the credentials are good; `Err` is a refusal that never reveals
/// whether the principal exists.
///
/// argon2id is expensive on purpose — ~19 MiB of scratch and tens of ms of CPU per
/// verify — and an unknown principal burns the *same* cost against a dummy hash
/// ([`crate::acl::Acl::verify`]) so a missing account cannot be spotted by timing. That
/// equalisation is a security property and stays; what must not happen is paying for it
/// on a reactor worker, where a handful of concurrent `LOGON`s wedge every thread the
/// server has (query execution has always run on `spawn_blocking` — auth was the odd one
/// out). So:
///
/// * the poll (a filesystem re-read of `acl.json`) and the hash both move to a blocking
///   thread, leaving the reactor free to keep driving every other connection's IO;
/// * `auth_limit` caps how many verifies run **at once**. Without it the naive fix would
///   merely relocate the denial of service: tokio's blocking pool is 512 threads deep
///   with an unbounded queue, so an auth flood would park gigabytes of argon2 scratch
///   *and* starve query execution of the very threads it runs on. Callers wait for a
///   permit asynchronously — no thread, no reactor worker — and their number is already
///   bounded by the pre-auth connection cap;
/// * the permit is moved **into** the blocking closure, so it is released when the hash
///   actually finishes rather than when a hung-up client cancels the await (a cancelled
///   `spawn_blocking` still runs to completion; releasing early would let the cap be
///   overrun by clients that disconnect mid-LOGON).
///
/// The wait for a permit is bounded by the connection's login deadline, so a queued
/// verify cannot outlive the login window it belongs to.
pub(crate) async fn verify_off_reactor(
    ctx: &Arc<ConnCtx>,
    principal: &str,
    credentials: &str,
    login_deadline: Option<TokioInstant>,
) -> std::result::Result<bool, Failure> {
    let acquire = ctx.auth_limit.clone().acquire_owned();
    let permit = match login_deadline {
        Some(dl) => timeout_at(dl, acquire).await.map_err(|_| {
            debug!("login deadline passed while queued for a password verify; refusing");
            ctx.diag.record_login_timeout();
            Failure::unauthorized("authentication timed out")
        })?,
        None => acquire.await,
    }
    .map_err(|_| Failure::unauthorized("server is shutting down"))?;

    let acl = ctx.acl.clone();
    let graphs = ctx.graphs.clone();
    let principal = principal.to_string();
    let credentials = credentials.to_string();
    tokio::task::spawn_blocking(move || {
        // Held until the hash is done, not until the caller stops waiting for it.
        let _permit = permit;
        // Pick up any out-of-band ACL edit before authenticating — but only adopt one
        // whose digest still matches the served generation's `aclBlake3` stamp. A
        // post-generation edit to `acl.json` (e.g. self-granting a read) is refused and
        // the last-good ACL kept; the legitimate way to change access control is to
        // rebuild and publish a generation stamped against the new file.
        acl.poll_checked(|digest| graphs.acl_digest_acceptable(digest));
        acl.snapshot().verify(&principal, &credentials)
    })
    .await
    .map_err(|e| {
        // A panicked/aborted verify fails closed.
        warn!(error = %e, "password verification task did not complete");
        Failure::unauthorized("invalid principal or credentials")
    })
}

/// Handle one decoded request, returning the messages to send back (in order) or a
/// `Failure` (which the caller writes and then enters the FAILED state).
pub(crate) async fn handle_request(
    sess: &mut Session,
    ctx: &Arc<ConnCtx>,
    req: message::Request,
) -> std::result::Result<Vec<PsValue>, Failure> {
    use message::Request;
    match req {
        Request::Hello(meta) => {
            // Bolt 5.x carries auth in a separate LOGON; the 4.4 fallback embeds it
            // in HELLO. Authenticate here only when credentials are present, so a
            // 5.x HELLO (no `scheme`) simply opens the connection.
            if meta.get("scheme").is_some() {
                authenticate(sess, ctx, &meta).await?;
            }
            Ok(vec![message::success(vec![
                ("server".into(), PsValue::str(SERVER_AGENT)),
                (
                    "connection_id".into(),
                    PsValue::str(uuid::Uuid::new_v4().to_string()),
                ),
            ])])
        }

        Request::Logon(meta) => {
            authenticate(sess, ctx, &meta).await?;
            Ok(vec![message::success(vec![])])
        }

        // De-authenticating hands this connection back to whoever LOGONs next, so the
        // prior user's buffered rows and open-transaction graph go with them (HIK-123).
        Request::Logoff => {
            sess.user = None;
            sess.clear_user_state();
            Ok(vec![message::success(vec![])])
        }

        // Slater only ever runs a read transaction; BEGIN/COMMIT/ROLLBACK carry no
        // execution state. BEGIN *may* name the target graph in its `db` metadata —
        // when it does, resolve and validate it now so an unknown/ambiguous graph
        // fails at BEGIN rather than at the first RUN. When it does not (some clients,
        // e.g. Memgraph Lab, put `db` on the RUN inside the transaction instead),
        // leave the transaction unbound so that RUN resolves the graph itself.
        Request::Begin(meta) => {
            let user = sess
                .user
                .as_deref()
                .ok_or_else(|| Failure::unauthorized("not authenticated; send LOGON first"))?;
            sess.tx_graph = match meta
                .get("db")
                .and_then(PsValue::as_str)
                .filter(|s| !s.is_empty())
            {
                Some(_) => Some(ctx.select_graph(&meta, user, None)?),
                None => None,
            };
            Ok(vec![message::success(vec![])])
        }
        Request::Commit | Request::Rollback => {
            sess.tx_graph = None;
            Ok(vec![message::success(vec![])])
        }

        Request::Run {
            query,
            params,
            extra,
        } => {
            let user = sess
                .user
                .clone()
                .ok_or_else(|| Failure::unauthorized("not authenticated; send LOGON first"))?;
            let sticky = ctx.current_selection(&user);
            debug!(db = ?extra.get("db"), selected = ?sticky, query = %query, "WIRE-DIAG: RUN");
            // Strip an optional leading `GQL` / `CYPHER` dialect selector (Neo4j's
            // `CYPHER 5` / `CYPHER 25` form) before anything inspects the statement,
            // so the USE check, Memgraph detection, introspection and the parser all
            // see the bare query. Routing is a no-op — one parser serves both
            // languages (DECISIONS D40) — so we simply drop the prefix.
            let query = strip_dialect_prefix(&query).to_string();
            // `USE <graph>` / `USE DATABASE <graph>` selects the user's graph in-band
            // (clients that never send the Bolt `db` field, e.g. Memgraph Lab, rely on
            // this). Validate the target and remember it per-user for later db-less
            // statements; answer with an empty result like a Memgraph database switch.
            if let Some(target) = parse_use_statement(&query) {
                if ctx.graphs.get(&target).is_none() || !ctx.acl.snapshot().can_read(&user, &target)
                {
                    let mut served: Vec<String> = ctx
                        .graphs
                        .names()
                        .into_iter()
                        .filter(|g| ctx.acl.snapshot().can_read(&user, g))
                        .collect();
                    served.sort();
                    return Err(Failure::new(
                        CODE_NOT_FOUND,
                        format!("cannot USE '{target}' (available: {})", served.join(", ")),
                    ));
                }
                debug!(graph = %target, "WIRE-DIAG: USE selected graph");
                ctx.set_selection(&user, &target);
                sess.pending = Some(Pending {
                    rows: vec![],
                    sent: 0,
                });
                return Ok(vec![message::success(vec![(
                    "fields".into(),
                    PsValue::List(vec![]),
                )])]);
            }
            // Remember (per-user) when a client reveals itself as Memgraph, so the
            // dialect-sensitive introspection answers below match what it expects.
            if is_memgraph_dialect_query(&normalize_query(&query)) {
                ctx.mark_memgraph(&user);
            }
            // A browser GUI fires introspection (`CALL db.labels()`, `SHOW …`) on
            // connect; answer those from the manifest before the read-only Cypher
            // grammar (which forbids them) ever sees the query.
            if let Some((columns, rows)) = ctx.introspect(
                &user,
                &extra,
                &query,
                sticky.as_deref(),
                ctx.is_memgraph(&user),
            )? {
                sess.pending = Some(Pending { rows, sent: 0 });
                return Ok(vec![message::success(vec![(
                    "fields".into(),
                    PsValue::List(columns.into_iter().map(PsValue::String).collect()),
                )])]);
            }
            // Inside an explicit transaction the graph was resolved at BEGIN and the
            // RUN carries no `db`; otherwise resolve from the RUN's `db`, else the
            // user's sticky `USE` selection, else their single readable graph.
            let graph = match &sess.tx_graph {
                // The graph was resolved and ACL-checked at BEGIN — but that check was
                // made for whoever was authenticated *then*, against the ACL as it read
                // *then*. Neither is guaranteed to still hold: the ACL hot-reloads (a
                // grant can be revoked mid-transaction), and the session's principal can
                // change under an open transaction. Re-check per RUN rather than trust
                // the BEGIN-time decision — a read must never be served on a grant the
                // current user does not currently hold (HIK-123).
                Some(g) => {
                    if !ctx.acl.snapshot().can_read(&user, g) {
                        return Err(Failure::new(
                            CODE_FORBIDDEN,
                            format!("user '{user}' has no read grant on graph '{g}'"),
                        ));
                    }
                    g.clone()
                }
                None => {
                    let g = ctx.select_graph(&extra, &user, sticky.as_deref())?;
                    // If this query named the graph explicitly (e.g. Memgraph Lab puts
                    // the chosen database on its connection-test query but sends none on
                    // the editor queries that follow), remember it per-user so those
                    // later db-less queries inherit it across the pool.
                    if extra
                        .get("db")
                        .and_then(PsValue::as_str)
                        .filter(|s| !s.is_empty())
                        .is_some()
                    {
                        ctx.set_selection(&user, &g);
                    }
                    g
                }
            };
            let gen = ctx.graphs.get(&graph).ok_or_else(|| {
                Failure::new(CODE_NOT_FOUND, format!("graph '{graph}' is not served"))
            })?;
            let param_vals = params_to_vals(&params)?;
            // The writable layer is per-graph and off unless configured; when it is
            // on, a query may be a write. Parse synchronously so a syntax /
            // read-only error is classified cleanly.
            let writer = ctx.graphs.writer(&graph);
            let (columns, rows) = match &writer {
                Some(w) => {
                    let stmt = parser::parse_statement(&query).map_err(|e| {
                        // The writable layer IS enabled for this graph, so a write-clause
                        // rejection from the write parser means the *shape* is not one the
                        // writable grammar supports — not that the connection is read-only.
                        if e.downcast_ref::<parser::WriteClauseRejected>().is_some() {
                            Failure::new(
                                CODE_ACCESS_MODE,
                                "unsupported write: the writable layer accepts business-key \
                                 MERGE / SET / REMOVE / [DETACH] DELETE, CREATE / INSERT (GQL), \
                                 and relationship writes only"
                                    .to_string(),
                            )
                        } else {
                            Failure::from_query_error(&e)
                        }
                    })?;
                    // A `read` grant selected the graph; mutating it needs `write` too.
                    authorize_statement(&ctx.acl.snapshot(), &user, &graph, &stmt)?;
                    match stmt {
                        // The three write shapes all execute off the reactor, under the
                        // `maxConcurrentWrites` cap — see `execute_write_off_reactor`.
                        parser::ast::Statement::Write(stmt) => {
                            let out = execute_write_off_reactor(
                                ctx,
                                w,
                                &gen,
                                WriteJob::Node(Box::new(stmt)),
                                param_vals,
                            )
                            .await?;
                            maybe_maintain_delta(ctx, &graph, w).await;
                            out
                        }
                        parser::ast::Statement::Create(stmt) => {
                            let out = execute_write_off_reactor(
                                ctx,
                                w,
                                &gen,
                                WriteJob::Create(stmt),
                                param_vals,
                            )
                            .await?;
                            maybe_maintain_delta(ctx, &graph, w).await;
                            out
                        }
                        parser::ast::Statement::WriteEdge(stmt) => {
                            let out = execute_write_off_reactor(
                                ctx,
                                w,
                                &gen,
                                WriteJob::Edge(stmt),
                                param_vals,
                            )
                            .await?;
                            maybe_maintain_delta(ctx, &graph, w).await;
                            out
                        }
                        parser::ast::Statement::Consolidate => {
                            execute_consolidate(ctx, &graph).await?
                        }
                        parser::ast::Statement::Read(ast) => {
                            // Overlay this graph's delta iff it was resolved against the
                            // generation we're about to read (dense ids are per-build).
                            let overlay = delta_for_read(w, &gen);
                            run_query(ctx, gen, &query, ast, param_vals, sess.version, overlay)
                                .await?
                        }
                    }
                }
                None => {
                    let ast = parser::parse(&query).map_err(|e| {
                        // No writer for this graph ⇒ the writable layer is not enabled, so this
                        // connection cannot mutate at all. Reword the write-clause rejection into a
                        // connection-level message — distinct from an ACL write-grant denial
                        // (reported by `authorize_statement` when the layer IS enabled) and from an
                        // unsupported-shape rejection (the `Some` arm above).
                        if e.downcast_ref::<parser::WriteClauseRejected>().is_some() {
                            Failure::new(
                                CODE_ACCESS_MODE,
                                "this slater connection is read-only: the writable layer is not \
                                 enabled (set delta.enabled)"
                                    .to_string(),
                            )
                        } else {
                            Failure::from_query_error(&e)
                        }
                    })?;
                    run_query(
                        ctx,
                        gen,
                        &query,
                        ast,
                        param_vals,
                        sess.version,
                        ReadOverlay::empty(),
                    )
                    .await?
                }
            };
            sess.pending = Some(Pending { rows, sent: 0 });
            Ok(vec![message::success(vec![(
                "fields".into(),
                PsValue::List(columns.into_iter().map(PsValue::String).collect()),
            )])])
        }

        Request::Pull(meta) => {
            // Rows are only ever served to an authenticated session — the same bar RUN
            // sets. Defence in depth for HIK-123: this is the check whose absence turned a
            // stale buffer into a cross-user read, so it holds even if some future path
            // leaves `pending` behind across an identity change.
            if sess.user.is_none() {
                return Err(Failure::unauthorized("not authenticated; send LOGON first"));
            }
            let pending = sess
                .pending
                .as_mut()
                .ok_or_else(|| Failure::new(CODE_REQUEST, "PULL without a preceding RUN".into()))?;
            let n = meta.get("n").and_then(PsValue::as_int).unwrap_or(-1);
            let remaining = pending.rows.len() - pending.sent;
            let take = if n < 0 {
                remaining
            } else {
                (n as usize).min(remaining)
            };
            let mut msgs = Vec::with_capacity(take + 1);
            for row in &pending.rows[pending.sent..pending.sent + take] {
                msgs.push(message::record(row.clone()));
            }
            pending.sent += take;
            let has_more = pending.sent < pending.rows.len();
            let mut meta = vec![("has_more".into(), PsValue::Bool(has_more))];
            // The final SUCCESS (no more rows) carries the additive GQLSTATUS
            // completion status; intermediate ones do not, since the query is not yet
            // complete.
            if !has_more {
                meta.extend(gqlstatus_completion(pending.rows.len()));
                sess.pending = None;
            }
            msgs.push(message::success(meta));
            Ok(msgs)
        }

        Request::Discard(meta) => {
            // Authenticated-only, on the same reasoning as PULL (HIK-123). DISCARD streams
            // no rows, but its completion metadata reports the buffer's size — the result's
            // cardinality is not an unauthenticated session's to learn either.
            if sess.user.is_none() {
                return Err(Failure::unauthorized("not authenticated; send LOGON first"));
            }
            // DISCARD honours its `n` exactly as PULL does — it just drops the rows
            // instead of streaming them. `n < 0` (the default) discards everything;
            // a positive `n` discards up to `n` and leaves `has_more` set if the
            // buffer still holds rows (a subsequent PULL/DISCARD continues from there).
            let Some(pending) = sess.pending.as_mut() else {
                // Nothing pending: a bare completion (mirrors the whole-buffer case).
                let mut meta = vec![("has_more".into(), PsValue::Bool(false))];
                meta.extend(gqlstatus_completion(0));
                return Ok(vec![message::success(meta)]);
            };
            let n = meta.get("n").and_then(PsValue::as_int).unwrap_or(-1);
            let remaining = pending.rows.len() - pending.sent;
            let drop = if n < 0 {
                remaining
            } else {
                (n as usize).min(remaining)
            };
            pending.sent += drop;
            let has_more = pending.sent < pending.rows.len();
            let mut meta = vec![("has_more".into(), PsValue::Bool(has_more))];
            // Only the terminal message (buffer drained) carries the additive
            // GQLSTATUS completion status, matching the final PULL.
            if !has_more {
                meta.extend(gqlstatus_completion(pending.rows.len()));
                sess.pending = None;
            }
            Ok(vec![message::success(meta)])
        }

        // Handled before dispatch.
        Request::Reset | Request::Goodbye => Ok(vec![message::success(vec![])]),
    }
}
