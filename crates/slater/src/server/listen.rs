// SPDX-License-Identifier: Apache-2.0
//! The listener: serve/accept loop, TLS, per-IP throttling, connection spawn.
//!
//! Split out of `server.rs` as a child module (a pure relocation). Shared types,
//! consts and helpers stay in the parent, reached via `use super::*`; the parent
//! re-exports this module's items so sibling modules can call them by name.

use super::*;

pub async fn serve(cfg: AppConfig) -> Result<()> {
    let listener = TcpListener::bind((cfg.server.bind.as_str(), cfg.server.port))
        .await
        .with_context(|| format!("bind {}:{}", cfg.server.bind, cfg.server.port))?;
    serve_with_listener(cfg, listener).await
}

/// Construct the storage backend from config: the local filesystem rooted at
/// `data_dir` (default), or an S3 object store. The `s3` backend requires the
/// binary to be built with the `s3` cargo feature.
pub(crate) fn build_store(cfg: &AppConfig) -> Result<Arc<dyn ObjectStore>> {
    match cfg.data_backend.kind.as_str() {
        "fs" | "" => Ok(Arc::new(FsObjectStore::new(cfg.data_dir()))),
        "s3" => {
            #[cfg(feature = "s3")]
            {
                let s = &cfg.data_backend.s3;
                // Explicit config credentials are the primary mechanism; empty
                // strings fall back to the standard AWS chain (env/profile/role).
                let access_key = (!s.aws_access_key.is_empty()).then(|| s.aws_access_key.clone());
                let secret_key = (!s.aws_secret_key.is_empty()).then(|| s.aws_secret_key.clone());
                let session_token =
                    (!s.aws_session_token.is_empty()).then(|| s.aws_session_token.clone());
                let scfg = graph_format::store::s3::S3Config {
                    bucket: s.bucket.clone(),
                    region: s.region.clone(),
                    endpoint: (!s.endpoint.is_empty()).then(|| s.endpoint.clone()),
                    prefix: s.prefix.clone(),
                    path_style: s.path_style,
                    access_key,
                    secret_key,
                    session_token,
                };
                let store = graph_format::store::s3::S3ObjectStore::connect(&scfg)
                    .context("connect S3 data backend")?;
                let store: Arc<dyn ObjectStore> = Arc::new(store);
                // Optional local-disk second cache tier in front of S3.
                if s.disk_cache_bytes > 0 {
                    if s.disk_cache_dir.is_empty() {
                        bail!(
                            "dataBackend.s3.diskCacheBytes is set but diskCacheDir is empty — \
                             a writable cache directory is required (and must not be tmpfs)"
                        );
                    }
                    let cache = graph_format::store::diskcache::DiskCache::open(
                        &s.disk_cache_dir,
                        s.disk_cache_bytes as u64,
                        graph_format::store::diskcache::write_behind_budget(
                            cfg.cache.block_cache_bytes as u64,
                            s.disk_cache_bytes as u64,
                        ),
                    )
                    .context("open S3 disk cache")?;
                    Ok(Arc::new(
                        graph_format::store::diskcache::CachingObjectStore::new(store, cache),
                    ))
                } else {
                    Ok(store)
                }
            }
            #[cfg(not(feature = "s3"))]
            {
                bail!(
                    "data_backend.kind = \"s3\" but this slater binary was built without the \
                     `s3` cargo feature"
                )
            }
        }
        "gcs" => {
            #[cfg(feature = "gcs")]
            {
                let g = &cfg.data_backend.gcs;
                let gcfg = graph_format::store::gcs::GcsConfig {
                    bucket: g.bucket.clone(),
                    prefix: g.prefix.clone(),
                    endpoint: (!g.endpoint.is_empty()).then(|| g.endpoint.clone()),
                    credentials_path: (!g.credentials_path.is_empty())
                        .then(|| g.credentials_path.clone()),
                    credentials_json: (!g.credentials_json.is_empty())
                        .then(|| g.credentials_json.clone()),
                    anonymous: g.anonymous,
                };
                let store = graph_format::store::gcs::GcsObjectStore::connect(&gcfg)
                    .context("connect GCS data backend")?;
                let store: Arc<dyn ObjectStore> = Arc::new(store);
                // Optional local-disk second cache tier in front of GCS (the same
                // backend-agnostic decorator used for S3).
                if g.disk_cache_bytes > 0 {
                    if g.disk_cache_dir.is_empty() {
                        bail!(
                            "dataBackend.gcs.diskCacheBytes is set but diskCacheDir is empty — \
                             a writable cache directory is required (and must not be tmpfs)"
                        );
                    }
                    let cache = graph_format::store::diskcache::DiskCache::open(
                        &g.disk_cache_dir,
                        g.disk_cache_bytes as u64,
                        graph_format::store::diskcache::write_behind_budget(
                            cfg.cache.block_cache_bytes as u64,
                            g.disk_cache_bytes as u64,
                        ),
                    )
                    .context("open GCS disk cache")?;
                    Ok(Arc::new(
                        graph_format::store::diskcache::CachingObjectStore::new(store, cache),
                    ))
                } else {
                    Ok(store)
                }
            }
            #[cfg(not(feature = "gcs"))]
            {
                bail!(
                    "data_backend.kind = \"gcs\" but this slater binary was built without the \
                     `gcs` cargo feature"
                )
            }
        }
        other => bail!("unknown data_backend.kind {other:?} (expected \"fs\", \"s3\", or \"gcs\")"),
    }
}

// DESIGN: the crate is a library + thin binary, and `serve` is split into a
// bind step + [`serve_with_listener`], so the bounded-RSS *headline* test can
// drive the real production wiring in-process over an ephemeral loopback port
// and sample its own RSS — rather than asserting on a mock. See D34.

/// Pin every sealed **segment** index's resident PQ codes (HIK-113) into the vector-index
/// pool, keyed by the **segment uuid** + segment-local ordinal (segment uuids are globally
/// unique, so they never collide with a base generation's key). Returns the count pinned.
///
/// Idempotent: re-pinning the same `(seg_uuid, ord)` replaces its byte accounting. Only the PQ
/// codes are pinned (bounded — ~m bytes per vector); the `.vamana` blocks read the evictable
/// LRU, and a `ResidentMatrix` is deliberately never installed for a segment (the Σ-over-
/// segments pinning trap: pinned bytes are charged to the budget but never reclaimed by
/// eviction, so an unbounded pinned set would grow the budget without bound — see `cache.rs`).
pub(crate) fn pin_segment_pqs(gen: &Generation, cache: &VectorIndexCache) -> usize {
    let mut n = 0;
    for seg in gen.stack().segments() {
        if let Some(vg) = &seg.vector_graph {
            for ix in vg.iter() {
                cache.pin(seg.manifest.segment_uuid, ix.ord, ix.pq.clone());
                n += 1;
            }
        }
    }
    n
}

/// Unpin the PQ codes of every sealed segment index the retiring generation `old` carried that
/// the newly-served `new` does **not** — the concrete gap the pinning trap warns about (HIK-113).
///
/// `unpin_generation` frees only the base generation's pinned set (keyed by the *generation*
/// uuid); a segment's PQ is keyed by the *segment* uuid, so without this call every T3 merge (or
/// any swap that drops a segment) would leak the retired segment's pinned bytes forever. A
/// segment carried by **both** generations (e.g. one a merge left untouched) keeps its pin — the
/// new generation re-pinned it, and its uuid is in `kept`.
pub(crate) fn unpin_retired_segment_pqs(
    old: &Generation,
    new: &Generation,
    cache: &VectorIndexCache,
) {
    let mut kept: HashSet<(u128, u32)> = HashSet::new();
    for seg in new.stack().segments() {
        if let Some(vg) = &seg.vector_graph {
            for ix in vg.iter() {
                kept.insert((seg.manifest.segment_uuid.0.as_u128(), ix.ord));
            }
        }
    }
    for seg in old.stack().segments() {
        if let Some(vg) = &seg.vector_graph {
            for ix in vg.iter() {
                if !kept.contains(&(seg.manifest.segment_uuid.0.as_u128(), ix.ord)) {
                    cache.unpin(seg.manifest.segment_uuid, ix.ord);
                }
            }
        }
    }
}

/// Serve on an already-bound listener. Factored out of [`serve`] so a caller can
/// bind the port itself — notably the bounded-RSS integration test, which binds
/// an ephemeral `127.0.0.1:0` loopback port, learns its address, and then drives
/// the *real* server wiring in-process: graph open, ACL, the three cache pools at
/// the configured budgets, resident-PQ pinning, and the generation guard (D34).
pub async fn serve_with_listener(cfg: AppConfig, listener: TcpListener) -> Result<()> {
    let acl = Arc::new(AclHandle::load(&cfg.acl_path).context("load ACL")?);
    cfg.encryption
        .check_key_file_outside_data_dir(cfg.data_dir())
        .context("validate at-rest encryption key location")?;
    let master_key = cfg
        .encryption
        .load_key()
        .context("load at-rest encryption key")?;
    let store = build_store(&cfg)?;
    let verify_integrity = cfg.data_backend.verify_integrity_resolved();
    let mut graphs = Graphs::open_all_with_store(
        store,
        master_key.as_deref().map(Vec::as_slice),
        verify_integrity,
        Some(cfg.cache.range_index_cache_bytes),
        cfg.cache.degree_column,
        Some(cfg.cache.degree_column_bytes),
    )?;
    graphs.set_manifest_policy(Some(PathBuf::from(&cfg.acl_path)), cfg.require_acl_stamp);
    graphs
        .verify_manifest_policy()
        .context("manifest authentication policy")?;
    // The shared columnar block cache — created before the writable layer so off-heap L0
    // delta segments page through this same budget + eviction domain (Phase C / D54).
    let cache = Arc::new(BlockCache::new(cfg.cache.block_cache_bytes));
    if cfg.delta.enabled {
        graphs
            .enable_writable_layer(&cfg.delta, Path::new(cfg.data_dir()), Some(cache.gf()))
            .context("enable writable layer")?;
        info!(wal_dir = %cfg.delta.wal_dir, off_heap_l0 = cfg.delta.off_heap_l0, "writable layer enabled");
    }
    let graphs = Arc::new(graphs);
    if graphs.is_empty() {
        warn!(data_dir = %cfg.data_dir(), "no graphs found to serve");
    }
    let result_cache = Arc::new(ResultCache::new(cfg.cache.result_cache_bytes));
    // The vector-index pool, and pin every generation's resident PQ codes into it
    // so the disk-native ANN path navigates from memory (the milestone DESIGN).
    let vector_cache = Arc::new(VectorIndexCache::new(cfg.cache.vector_cache_bytes));
    let mut pinned = 0usize;
    for gen in graphs.current_generations() {
        for vi in gen.vamana_indexes() {
            vector_cache.pin(gen.uuid(), vi.ord, vi.pq.clone());
            pinned += 1;
        }
        // Sealed segment indexes (HIK-113) pin their resident PQ under the segment uuid.
        pinned += pin_segment_pqs(gen.as_ref(), &vector_cache);
    }
    if pinned > 0 {
        info!(
            indexes = pinned,
            bytes = vector_cache.bytes(),
            "pinned resident PQ codes into the vector-index pool"
        );
    }
    // Parse the reload strategy up front so a fat-fingered config fails at boot.
    let reload_strategy = cfg.reload_strategy()?;
    let tls = build_tls_acceptor(&cfg.tls)?;

    // Global concurrent-connection cap. A permit is acquired *before* `accept()`,
    // so at capacity we stop draining the kernel accept queue — back-pressure lands
    // in the listen backlog and then the OS refuses new SYNs, rather than the heap
    // filling with sockets we cannot service. This is what makes the bounded-RSS
    // guarantee hold under adversarial connection load (each connection's buffers
    // live outside the cache budget). Built before `ctx` so a clone can live in it
    // for the diagnostics snapshot to read live occupancy.
    let conn_limit = Arc::new(Semaphore::new(semaphore_permits(
        cfg.server.max_connections,
    )));
    // Parse the off-peak consolidation window up front so a malformed cron spec fails at
    // startup rather than silently never gating (Phase 4d follow-up).
    let consolidate_window = crate::cron_window::CronWindow::parse(&cfg.delta.consolidate_window)
        .with_context(|| {
        format!(
            "invalid delta.consolidateWindow {:?}",
            cfg.delta.consolidate_window
        )
    })?;

    let ctx = Arc::new(ConnCtx {
        acl,
        graphs,
        cache,
        vector_cache,
        rw_indexes: Arc::new(RwIndexCache::new()),
        rw_index_cfg: cfg.vector_query.rw_index.into(),
        result_cache,
        max_rows: cfg.query.max_rows as usize,
        timeout_ms: cfg.query.timeout_ms,
        max_intermediate: cfg.query.max_intermediate,
        max_scan: cfg.query.max_scan,
        intermediate_budget: Arc::new(GlobalIntermediateBudget::new(
            cfg.query.max_intermediate_global,
        )),
        max_shortest_path_explore: cfg.query.max_shortest_path_explore,
        adj_stream_threshold: cfg.query.adj_stream_threshold,
        adj_stream_chunk: cfg.query.adj_stream_chunk,
        fanout_pool: build_fanout_pool(cfg.query.max_fanout),
        beam_width: cfg.vector_query.beam_width as usize,
        temp_beam_width: cfg.vector_query.temp_beam_width as usize,
        bind_addr: format!("{}:{}", cfg.server.bind, cfg.server.port),
        default_graph: Some(cfg.default_graph.clone()).filter(|g| !g.is_empty()),
        use_selection: RwLock::new(HashMap::new()),
        memgraph_users: RwLock::new(HashSet::new()),
        max_message_bytes: cfg.server.max_message_bytes,
        max_pre_auth_bytes: cfg.server.max_pre_auth_bytes,
        login_timeout_ms: cfg.server.login_timeout_ms,
        tls_handshake_timeout_ms: cfg.server.tls_handshake_timeout_ms,
        idle_timeout_ms: cfg.server.idle_timeout_ms,
        pre_auth_limit: Arc::new(Semaphore::new(semaphore_permits(
            cfg.server.max_pre_auth_connections,
        ))),
        auth_limit: Arc::new(Semaphore::new(semaphore_permits(
            cfg.server.max_concurrent_auth,
        ))),
        max_auth_failures: cfg.server.max_auth_failures,
        write_limit: Arc::new(Semaphore::new(semaphore_permits(
            cfg.server.max_concurrent_writes,
        ))),
        per_ip: Arc::new(Mutex::new(HashMap::new())),
        max_per_ip: cfg.server.max_connections_per_ip,
        diag: Arc::new(crate::diag::Diagnostics::new(cfg.load_test_diagnostics)),
        conn_limit: conn_limit.clone(),
        max_connections: cfg.server.max_connections,
        max_pre_auth_connections: cfg.server.max_pre_auth_connections,
        data_dir: PathBuf::from(cfg.data_dir()),
        builder_bin: cfg.delta.builder_bin.clone(),
        memtable_bytes: cfg.delta.memtable_bytes,
        l0_compaction_trigger: cfg.delta.l0_compaction_trigger,
        segment_flush_bytes: cfg.delta.segment_flush_bytes,
        max_upper_segments: cfg.delta.max_upper_segments,
        segment_gc_grace_secs: cfg.delta.segment_gc_grace_secs,
        delta_core_percent: cfg.delta.delta_core_percent,
        delta_hard_bytes: cfg.delta.delta_hard_bytes,
        consolidate_window,
    });
    if cfg.load_test_diagnostics {
        warn!(
            "load-test diagnostics ENABLED (loadTestDiagnostics=true): extra counters \
             are maintained and `CALL slater.diagnostics()` is answerable — do not enable \
             on a production replica"
        );
    }

    info!(
        bind = %cfg.server.bind,
        port = cfg.server.port,
        tls = tls.is_some(),
        graphs = ctx.graphs.len(),
        poll_ms = cfg.generation_poll_ms,
        reload_strategy = %cfg.reload_strategy,
        max_connections = cfg.server.max_connections,
        max_pre_auth_connections = cfg.server.max_pre_auth_connections,
        max_connections_per_ip = cfg.server.max_connections_per_ip,
        max_concurrent_auth = cfg.server.max_concurrent_auth,
        max_auth_failures = cfg.server.max_auth_failures,
        max_concurrent_writes = cfg.server.max_concurrent_writes,
        login_timeout_ms = cfg.server.login_timeout_ms,
        tls_handshake_timeout_ms = cfg.server.tls_handshake_timeout_ms,
        "slater Bolt listener ready"
    );

    // The in-flight generation guard: poll each graph's `current` and, on a change,
    // either swap the validated new generation in place or signal a clean exit.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<String>();
    spawn_generation_guard(
        ctx.graphs.clone(),
        ctx.vector_cache.clone(),
        reload_strategy,
        cfg.generation_poll_interval(),
        shutdown_tx,
        Some(ctx.acl.clone()),
    );

    // The cache-maintenance task: reclaim idle entries past the configured TTL.
    // Disabled (not spawned) when `cache.cacheTtlMs` is 0 — caches then evict on
    // budget pressure alone, as before.
    if let Some(ttl) = cfg.cache.cache_ttl() {
        info!(
            ttl_ms = cfg.cache.cache_ttl_ms,
            "cache idle-TTL eviction enabled"
        );
        spawn_cache_maintenance(
            ctx.cache.clone(),
            ctx.vector_cache.clone(),
            ctx.result_cache.clone(),
            ctx.graphs.clone(),
            ttl,
        );
    }

    // Returning the post-burst heap high-water to the OS during idle is jemalloc's
    // job now (Linux global allocator with background purge threads — see main.rs),
    // so there is no manual idle-trim task to spawn here.

    // Warm the block/vector cache by running the configured query and discarding
    // its results, before we accept any connection — so the first real client
    // query of that shape is served from a warm cache.
    warm_cache(&cfg.cache_warming_query, &ctx).await;

    accept_loop(listener, ctx, tls, conn_limit, shutdown_rx).await
}

/// Run the configured cache-warming query against every served graph and throw
/// the rows away — the read pulls the blocks it touches into the caches so the
/// first matching client query is warm. No-op when `cacheWarmingQuery` is empty.
///
/// A parse error is logged and warming skipped — a fat-fingered warming query is
/// never a reason to take the server down, warming is a pure optimisation. A
/// per-graph execution error is likewise logged and skipped: the same string is
/// run against every graph and need not be valid against each one's schema. The
/// query is bounded by the same `query.*` limits and timeout a real client query
/// gets, so warming can never run unbounded.
pub(crate) async fn warm_cache(warming_query: &str, ctx: &Arc<ConnCtx>) {
    let query = warming_query.trim();
    if query.is_empty() {
        return;
    }
    let ast = match parser::parse(query) {
        Ok(ast) => Arc::new(ast),
        Err(e) => {
            // Don't abort boot for a bad warming query — log it and serve cold.
            error!(error = %e, "cache-warming query failed to parse; skipping warm-up");
            return;
        }
    };

    let generations = ctx.graphs.current_generations();
    let cache = ctx.cache.clone();
    let vector_cache = ctx.vector_cache.clone();
    let beam_width = ctx.beam_width;
    let temp_beam_width = ctx.temp_beam_width;
    let max_rows = ctx.max_rows;
    let max_intermediate = ctx.max_intermediate;
    let max_scan = ctx.max_scan;
    let intermediate_budget = ctx.intermediate_budget.clone();
    let max_shortest_path_explore = ctx.max_shortest_path_explore;
    let adj_stream_threshold = ctx.adj_stream_threshold;
    let adj_stream_chunk = ctx.adj_stream_chunk;
    let fanout_pool = ctx.fanout_pool.clone();
    let timeout_ms = ctx.timeout_ms;

    // Execution is blocking and CPU-bound — keep it off the async runtime.
    let started = Instant::now();
    let _ = tokio::task::spawn_blocking(move || {
        for gen in generations {
            let g_start = Instant::now();
            let mut engine = Engine::new(gen.as_ref(), cache.as_ref())
                .with_vector_cache(vector_cache.as_ref(), beam_width)
                .with_temp_beam_width(temp_beam_width)
                .with_max_rows(max_rows)
                .with_max_intermediate(max_intermediate)
                .with_max_scan(max_scan)
                .with_global_budget(intermediate_budget.as_ref())
                .with_max_shortest_path_explore(max_shortest_path_explore)
                .with_adj_stream(adj_stream_threshold, adj_stream_chunk)
                .with_fanout_pool(fanout_pool.clone());
            if timeout_ms > 0 {
                engine = engine.with_deadline(Instant::now() + Duration::from_millis(timeout_ms));
            }
            match engine.run(&ast) {
                Ok(result) => {
                    info!(
                        graph = gen.graph(),
                        rows = result.rows.len(),
                        ms = g_start.elapsed().as_millis() as u64,
                        "cache-warming query completed"
                    );
                    // Results discarded — the side effect (warm cache) is the point.
                    drop(result);
                }
                Err(e) => {
                    warn!(
                        graph = gen.graph(),
                        error = %e,
                        "cache-warming query failed for graph; skipping it"
                    );
                }
            }
        }
    })
    .await;
    info!(
        ms = started.elapsed().as_millis() as u64,
        "cache warm-up finished"
    );
}

/// The connection accept loop: reserve a global slot, accept, apply the per-source
/// cap, and spawn a handler holding both. Factored out of [`serve_with_listener`]
/// so the limit wiring can be driven directly in tests over a loopback listener.
pub(crate) async fn accept_loop(
    listener: TcpListener,
    ctx: Arc<ConnCtx>,
    tls: Option<TlsAcceptor>,
    conn_limit: Arc<Semaphore>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<String>,
) -> Result<()> {
    loop {
        // Reserve a global slot before pulling the next connection off the queue.
        // Held for the connection's lifetime (moved into the task below); dropped
        // here if accept fails, the per-source cap rejects, or shutdown fires.
        let permit = conn_limit
            .clone()
            .acquire_owned()
            .await
            .expect("connection semaphore is never closed");
        tokio::select! {
            accepted = listener.accept() => {
                let (sock, peer) = accepted.context("accept")?;
                sock.set_nodelay(true).ok();
                // Per-source cap: a single address (IPv4 /32, IPv6 /64) may not
                // monopolise the global pool. Checked here so a rejected source costs
                // only an accepted-then-closed FD, never a spawned task.
                let per_ip_guard = if ctx.max_per_ip > 0 {
                    match try_acquire_per_ip(&ctx.per_ip, per_ip_key(peer.ip()), ctx.max_per_ip) {
                        Some(g) => Some(g),
                        None => {
                            warn!(%peer, "per-source connection cap reached; rejecting connection");
                            ctx.diag.record_rejected_per_ip();
                            continue; // drops `permit` and `sock`, freeing the slot
                        }
                    }
                } else {
                    None
                };
                ctx.diag.record_accepted();
                let ctx = ctx.clone();
                let tls = tls.clone();
                let diag = ctx.diag.clone(); // error sink; `ctx` is moved into `serve_conn`
                tokio::spawn(async move {
                    let _permit = permit; // global slot, released on connection end
                    let _per_ip_guard = per_ip_guard; // per-source count, released on end
                    if let Err(e) = serve_conn(sock, tls, ctx).await {
                        // A write torn down at the login deadline is a login-window timeout,
                        // same as a stalled pre-auth read — count it on the same gauge (HIK-103).
                        if e.downcast_ref::<WriteDeadlineExceeded>().is_some() {
                            diag.record_login_timeout();
                        }
                        warn!(%peer, error = %e, "connection ended with error");
                    }
                });
            }
            reason = &mut shutdown_rx => {
                let graph = reason.unwrap_or_else(|_| "unknown".to_string());
                bail!(
                    "generation for graph '{graph}' changed on disk; exiting non-zero so the \
                     orchestrator restarts against it (reloadStrategy=exit)"
                );
            }
        }
    }
}

/// Permit count for a configurable limit, mapping 0 ("unlimited") to the largest
/// value a tokio [`Semaphore`] accepts so the acquire path stays uniform.
pub(crate) fn semaphore_permits(limit: usize) -> usize {
    if limit == 0 {
        Semaphore::MAX_PERMITS
    } else {
        limit
    }
}

/// The per-source counting key: the full address for IPv4 (/32), the /64 prefix
/// for IPv6. An attacker controls an entire /64, so keying on the full v6 address
/// would let them sidestep the cap by varying the low bits.
pub(crate) fn per_ip_key(addr: IpAddr) -> IpAddr {
    match addr {
        IpAddr::V4(v4) => IpAddr::V4(v4),
        IpAddr::V6(v6) => {
            let o = v6.octets();
            let mut masked = [0u8; 16];
            masked[..8].copy_from_slice(&o[..8]); // keep the /64 prefix, zero the rest
            IpAddr::V6(std::net::Ipv6Addr::from(masked))
        }
    }
}

/// Decrements a source's live connection count when the connection ends.
pub(crate) struct PerIpGuard {
    map: Arc<Mutex<HashMap<IpAddr, usize>>>,
    key: IpAddr,
}

impl Drop for PerIpGuard {
    fn drop(&mut self) {
        let mut map = self.map.lock().unwrap();
        if let Some(count) = map.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                map.remove(&self.key); // keep the map bounded by *active* sources
            }
        }
    }
}

/// Reserve one connection slot for `key`, or `None` if the source is already at
/// `max`. The returned guard releases the slot on drop.
pub(crate) fn try_acquire_per_ip(
    map: &Arc<Mutex<HashMap<IpAddr, usize>>>,
    key: IpAddr,
    max: usize,
) -> Option<PerIpGuard> {
    // Only called when `max > 0`, so a fresh (count 0) entry is always admitted and
    // never left behind on the rejection path.
    let mut guard = map.lock().unwrap();
    let count = guard.entry(key).or_insert(0);
    if *count >= max {
        return None;
    }
    *count += 1;
    drop(guard);
    Some(PerIpGuard {
        map: map.clone(),
        key,
    })
}

/// The two things that bound an *anonymous* connection: its antechamber slot and its
/// deadline to stop being anonymous. Both are taken at the TCP `accept()`, in
/// [`PreAuth::admit`], and handed down through the TLS handshake into the Bolt state
/// machine — **not** created inside `handle_connection`, which is one stage too late.
///
/// That ordering is the fix for HIK-72. When the permit and the deadline were armed
/// behind the TLS handshake, a peer that completed TCP and then simply never sent a
/// ClientHello was invisible to both: it sat in `acceptor.accept()` forever, holding the
/// (Optionally) wrap the socket in TLS, then run the Bolt connection.
///
/// The antechamber slot and the login deadline are taken *here*, before the TLS
/// handshake, and the handshake itself is bounded — see [`PreAuth`].
pub(crate) async fn serve_conn(
    sock: TcpStream,
    tls: Option<TlsAcceptor>,
    ctx: Arc<ConnCtx>,
) -> Result<()> {
    let pre_auth = match PreAuth::admit(&ctx) {
        Some(p) => p,
        None => return Ok(()), // antechamber full; the socket is dropped here
    };
    match tls {
        Some(acceptor) => {
            let stream = match pre_auth.tls_deadline(&ctx) {
                Some(dl) => timeout_at(dl, acceptor.accept(sock))
                    .await
                    .map_err(|_| {
                        debug!("TLS handshake not completed within the deadline; closing");
                        ctx.diag.record_login_timeout();
                        anyhow!("TLS handshake not completed within the deadline")
                    })?
                    .context("TLS handshake")?,
                None => acceptor.accept(sock).await.context("TLS handshake")?,
            };
            handle_connection(stream, ctx, pre_auth).await
        }
        None => handle_connection(sock, ctx, pre_auth).await,
    }
}

/// Build a `rustls` acceptor from the configured cert/key, or `None` when TLS is
/// disabled (plaintext, for loopback dev).
pub(crate) fn build_tls_acceptor(tls: &TlsConfig) -> Result<Option<TlsAcceptor>> {
    if !tls.enabled() {
        return Ok(None);
    }
    let certs = load_certs(&tls.cert)?;
    let key = load_key(&tls.key)?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("build rustls server config")?;
    Ok(Some(TlsAcceptor::from(Arc::new(config))))
}

pub(crate) fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader = std::io::BufReader::new(
        std::fs::File::open(path).with_context(|| format!("open TLS certificate {path}"))?,
    );
    let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        bail!("no certificates found in {path}");
    }
    Ok(certs)
}

pub(crate) fn load_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let mut reader = std::io::BufReader::new(
        std::fs::File::open(path).with_context(|| format!("open TLS private key {path}"))?,
    );
    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| anyhow!("no private key found in {path}"))
}

// ── Connection state machine ──────────────────────────────────────────────────
