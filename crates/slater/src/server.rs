// SPDX-License-Identifier: Apache-2.0
//! The `tokio` Bolt listener and per-connection state machine.
//!
//! This is the final M4 increment: the piece that ties
//! `generation` + `cache` + `bolt` + `acl` + `parser` + `exec` together into a
//! live server. It accepts a socket (optionally TLS-wrapped via `rustls`), runs
//! the Bolt handshake ([`bolt::handshake`]), and drives the message loop
//! (`HELLO`→`LOGON`→`RUN`→`PULL`…): it authenticates at `LOGON` against the
//! [`acl`](crate::acl), selects a graph (enforcing the per-graph `read` grant),
//! parses with [`parser`](crate::parser), executes with [`exec::Engine`] over an
//! `Arc<Generation>` + a shared `Arc<BlockCache>`, and PackStream-encodes the rows
//! back as `RECORD`/`SUCCESS` (mapping [`exec::Val`] → [`bolt::packstream::PsValue`]
//! including `Node`/`Relationship`/`Map` structures).
//!
//! Design points:
//! - **One shared [`BlockCache`]** across every graph and connection. Its key is the
//!   generation UUID (D18), which is globally unique, so a single byte-budgeted pool
//!   correctly isolates graphs and orphans a swapped generation's blocks for free.
//! - **Execution runs on `spawn_blocking`** (the planner/executor and its `pread`s
//!   are synchronous), and the rows are encoded to `PsValue` *inside* that blocking
//!   task so all storage IO — including resolving a returned node's labels and
//!   properties — stays off the async reactor.
//! - **Buffered streaming.** `RUN` executes and buffers the (already `max_rows`-
//!   bounded) result, replying `SUCCESS {fields}`; the following `PULL` drains the
//!   buffer as `RECORD`s then a final `SUCCESS {has_more}`. Pull-time `n` is honoured.
//! - **Bolt failure semantics.** A `FAILURE` puts the connection into a failed state
//!   where every message except `RESET` is answered `IGNORED`, matching the drivers.

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use graph_format::ids::Generation as GenId;
use graph_format::store::fs::FsObjectStore;
use graph_format::store::{join_key, ObjectStore};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::{timeout, timeout_at, Instant as TokioInstant};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn, Level};

use crate::acl::AclHandle;
use crate::bolt::chunk;
use crate::bolt::handshake;
use crate::bolt::message;
use crate::bolt::packstream::PsValue;
use crate::cache::{BlockCache, ResultCache, ResultKey, VectorIndexCache};
use crate::config::{AppConfig, ReloadStrategy, TlsConfig};
use crate::exec::{Engine, GlobalIntermediateBudget, QueryResult, Val};
use crate::generation::Generation;
use crate::introspect;
use crate::parser;

/// PackStream structure tags for the graph types (Bolt `Node`/`Relationship`).
const TAG_NODE: u8 = 0x4E;
const TAG_RELATIONSHIP: u8 = 0x52;
/// Bolt `Path` (`0x50`) and the `UnboundRelationship` (`0x72`) it carries — a
/// relationship without endpoint ids, since the path's node list supplies them.
const TAG_PATH: u8 = 0x50;
const TAG_UNBOUND_REL: u8 = 0x72;
/// Bolt `Point2D` (`0x58`): `[srid::Int, x::Float, y::Float]`. FalkorDB points are
/// always WGS-84, so `srid` is fixed at 4326 with `x = longitude`, `y = latitude`
/// (see `resultset_replybolt.c`).
const TAG_POINT2D: u8 = 0x58;
/// Bolt v2 temporal structures (Neo4j PackStream spec). FalkorDB never encodes
/// temporals over Bolt (its formatter asserts on them), so these follow the
/// published Neo4j spec — what an official driver decodes. Slater's `localtime`/
/// `localdatetime` are timezone-free, so they map to the *Local* structs:
/// - `Date` (`0x44`): `[days::Int]` — days since the Unix epoch.
/// - `LocalTime` (`0x74`): `[nanoOfDay::Int]`.
/// - `LocalDateTime` (`0x64`): `[seconds::Int, nanoseconds::Int]`.
/// - `Duration` (`0x45`): `[months::Int, days::Int, seconds::Int, nanoseconds::Int]`.
const TAG_DATE: u8 = 0x44;
const TAG_LOCAL_TIME: u8 = 0x74;
const TAG_LOCAL_DATETIME: u8 = 0x64;
const TAG_DURATION: u8 = 0x45;

/// The `server` agent string returned in the `HELLO` reply. Kept with a `Neo4j/`
/// prefix so the official drivers' feature-gating (which sniffs this string) treats
/// us as a modern Bolt server, while still naming Slater honestly.
const SERVER_AGENT: &str = concat!("Neo4j/5.4.0 (Slater ", env!("CARGO_PKG_VERSION"), ")");

// Bolt/Neo4j status codes used in `FAILURE` responses.
const CODE_UNAUTHORIZED: &str = "Neo.ClientError.Security.Unauthorized";
const CODE_FORBIDDEN: &str = "Neo.ClientError.Security.Forbidden";
const CODE_NOT_FOUND: &str = "Neo.ClientError.Database.DatabaseNotFound";
const CODE_SYNTAX: &str = "Neo.ClientError.Statement.SyntaxError";
const CODE_ACCESS_MODE: &str = "Neo.ClientError.Statement.AccessMode";
const CODE_EXECUTION: &str = "Neo.ClientError.Statement.ExecutionFailed";
const CODE_REQUEST: &str = "Neo.ClientError.Request.Invalid";

// ── Graph registry ──────────────────────────────────────────────────────────

/// Every graph served by this process: `graph name → opened generation`.
///
/// Each `<data_dir>/<name>/` directory that carries a `current` pointer is opened
/// and validated at startup; a corrupt/incomplete generation fails the whole boot
/// (the copy-completeness guard), matching the plan's fail-fast stance.
///
/// Each entry is held behind a `RwLock<Arc<Generation>>` so the generation guard
/// can **atomically swap** in a new generation under the `swap` reload strategy
/// (M8). A query takes a cheap `get()` snapshot — an `Arc<Generation>` it holds
/// for its whole life — so a swap never mixes two generations within one query;
/// the `data_dir` + `master_key` are retained so the guard can re-open a graph.
pub struct Graphs {
    /// Storage backend the generations are read from (local filesystem by
    /// default, or an object store). Retained so the guard can re-open a graph.
    store: Arc<dyn ObjectStore>,
    master_key: Option<Vec<u8>>,
    /// Run the copy-completeness re-hash when opening (and swapping in) a
    /// generation. Default for the filesystem backend; usually off for S3.
    verify_integrity: bool,
    graphs: HashMap<String, RwLock<Arc<Generation>>>,
    /// Live `acl.json` path used to verify per-generation `aclBlake3` stamps at
    /// open/swap time. `None` ⇒ no ACL stamp checking (e.g. unit-test fixtures).
    acl_path: Option<PathBuf>,
    /// Refuse to serve a generation whose manifest carries no ACL stamp.
    require_acl_stamp: bool,
}

impl Graphs {
    /// Discover and open every graph under `data_dir` on the local filesystem,
    /// deriving each generation's block cipher from `master_key` (required iff a
    /// generation is encrypted). Convenience over [`open_all_with_store`] for the
    /// filesystem backend.
    ///
    /// [`open_all_with_store`]: Graphs::open_all_with_store
    pub fn open_all(data_dir: &Path, master_key: Option<&[u8]>) -> Result<Self> {
        let store: Arc<dyn ObjectStore> = Arc::new(FsObjectStore::new(data_dir));
        Self::open_all_with_store(store, master_key, true)
    }

    /// Discover and open every graph in `store`, deriving each generation's block
    /// cipher from `master_key`. A graph is a top-level name that carries a
    /// published `current` pointer; anything else (scratch dirs, half-written
    /// `.tmp-*` generations) is skipped. `verify_integrity` runs the
    /// copy-completeness re-hash at open (and on later swaps).
    pub fn open_all_with_store(
        store: Arc<dyn ObjectStore>,
        master_key: Option<&[u8]>,
        verify_integrity: bool,
    ) -> Result<Self> {
        let mut graphs = HashMap::new();
        let names = store.list("").context("list graphs in data store")?;
        for name in names {
            // A graph is one with a published `current` pointer.
            if !store.exists(&join_key(&name, "current")).unwrap_or(false) {
                continue;
            }
            let gen = Generation::open_with_store_opts(
                store.as_ref(),
                &name,
                master_key,
                verify_integrity,
            )
            .with_context(|| format!("open graph {name}"))?;
            graphs.insert(name, RwLock::new(Arc::new(gen)));
        }
        Ok(Self {
            store,
            master_key: master_key.map(<[u8]>::to_vec),
            verify_integrity,
            graphs,
            acl_path: None,
            require_acl_stamp: false,
        })
    }

    /// Install the manifest-authentication policy before the graphs go live. The
    /// server calls this between `open_all` and `verify_manifest_policy`; the MAC
    /// itself is verified inside `Generation::open_with_key`, while the ACL stamp
    /// and the require-presence downgrade guards are enforced here so the config
    /// flags stay co-located with the acl path. (MAC presence is not a policy
    /// knob: a keyed server unconditionally refuses a MAC-less generation — see
    /// `check_manifest_policy`.)
    pub fn set_manifest_policy(&mut self, acl_path: Option<PathBuf>, require_acl_stamp: bool) {
        self.acl_path = acl_path;
        self.require_acl_stamp = require_acl_stamp;
    }

    /// Hash the configured live `acl.json` once, or `None` when no `acl_path` is set.
    fn live_acl_digest(&self) -> Result<Option<String>> {
        match &self.acl_path {
            Some(p) => Ok(Some(
                graph_format::integrity::hash_file(p)
                    .with_context(|| format!("hash live acl {}", p.display()))?,
            )),
            None => Ok(None),
        }
    }

    /// Enforce the ACL stamp + require-presence policy for one generation's
    /// manifest. `live_acl` is the digest of the live `acl.json` (`None` when no
    /// acl path is configured). Bails — refusing to serve — on a stamp mismatch or
    /// a violated require-presence flag. (The MAC value itself is verified at open.)
    fn check_manifest_policy(
        &self,
        name: &str,
        m: &graph_format::manifest::Manifest,
        live_acl: Option<&str>,
    ) -> Result<()> {
        match (&m.acl_blake3, live_acl) {
            (Some(stamp), Some(live)) if stamp != live => bail!(
                "graph '{name}' was built against an acl.json with digest {stamp} but the live \
                 acl hashes to {live} — refusing to serve; rebuild the graph against the current \
                 acl.json"
            ),
            (Some(_), None) => warn!(
                graph = name,
                "generation carries an ACL stamp but no aclPath is configured to verify it"
            ),
            (None, _) if self.require_acl_stamp => {
                bail!("graph '{name}' manifest has no ACL stamp but requireAclStamp is set")
            }
            _ => {}
        }
        // Not configurable by design: a MAC-less generation on a keyed server is
        // either a strip attack or a plaintext image that doesn't need the key —
        // there is no legitimate keyed-but-unauthenticated deployment, so there is
        // no flag an attacker (or a mistaken operator) could flip to reopen the
        // strip downgrade. Plaintext deployments simply configure no key.
        if self.master_key.is_some() && m.mac.is_none() {
            bail!(
                "graph '{name}' manifest has no MAC but a master key is configured — \
                 refusing to serve an unauthenticated generation; rebuild with --encrypt \
                 (or remove the key for an all-plaintext deployment)"
            );
        }
        Ok(())
    }

    /// Is a candidate `acl.json` (identified by its BLAKE3 `digest`) acceptable for
    /// every served generation? Used to gate ACL hot-reload: a generation that
    /// carries an `aclBlake3` stamp only accepts an ACL whose digest equals that
    /// stamp; an unstamped generation imposes no constraint (legacy/plaintext
    /// images hot-reload as before). With several served graphs the live ACL must
    /// satisfy them all — the same operational contract as the open/swap check.
    ///
    /// This is the runtime enforcement of the `aclBlake3` stamp: between generation
    /// swaps it refuses a post-build edit to `acl.json`, closing the window where a
    /// hot-reload would otherwise adopt a tampered ACL unverified.
    pub fn acl_digest_acceptable(&self, digest: &str) -> bool {
        self.graphs.values().all(|slot| {
            match slot.read().unwrap().manifest().acl_blake3.as_deref() {
                Some(stamp) => stamp == digest,
                None => true,
            }
        })
    }

    /// Verify the manifest-authentication policy for every served generation. Called
    /// once at boot after `open_all` + `set_manifest_policy`. The live acl is hashed
    /// a single time and reused across all graphs.
    pub fn verify_manifest_policy(&self) -> Result<()> {
        let live = self.live_acl_digest()?;
        for (name, slot) in &self.graphs {
            let gen = slot.read().unwrap().clone();
            self.check_manifest_policy(name, gen.manifest(), live.as_deref())?;
        }
        Ok(())
    }

    /// A snapshot of the live generation for `name`. A query holds this `Arc` for
    /// its whole life, so a concurrent swap (which only replaces the slot's `Arc`)
    /// never changes the generation a running query sees.
    fn get(&self, name: &str) -> Option<Arc<Generation>> {
        self.graphs
            .get(name)
            .map(|slot| slot.read().unwrap().clone())
    }

    /// Every live generation — used to pin resident PQ codes into the
    /// vector-index pool at startup.
    fn current_generations(&self) -> Vec<Arc<Generation>> {
        self.graphs
            .values()
            .map(|slot| slot.read().unwrap().clone())
            .collect()
    }

    /// The names of all served graphs.
    pub fn names(&self) -> Vec<String> {
        self.graphs.keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.graphs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.graphs.is_empty()
    }

    /// If `<data_dir>/<name>/current` now names a different generation, open and
    /// **validate** it (the content-hash copy-completeness guard), pin its resident
    /// PQ into `vector_cache`, atomically swap the live `Arc`, and unpin the old
    /// generation's PQ. Returns:
    /// - `Ok(None)` — unchanged (the pointer still names the live generation).
    /// - `Ok(Some(uuid))` — swapped to the new generation `uuid`.
    /// - `Err(_)` — the pointer changed but the new generation is incomplete /
    ///   corrupt; the live one is left untouched and kept serving.
    ///
    /// Ordering matters: in-flight queries hold their own `Arc<Generation>` (with
    /// its own resident-PQ `Arc`) to completion, so they are unaffected by the
    /// unpin; the block/result/PQ caches key on the generation UUID, so the old
    /// generation's entries orphan for free (D18/D27/D32).
    fn swap_if_changed(
        &self,
        name: &str,
        vector_cache: &VectorIndexCache,
    ) -> Result<Option<GenId>> {
        let slot = self
            .graphs
            .get(name)
            .ok_or_else(|| anyhow!("graph '{name}' is not served"))?;
        let live = slot.read().unwrap().clone();
        let on_disk = Generation::current_uuid_in(self.store.as_ref(), name)?;
        if on_disk == live.uuid().0 {
            return Ok(None);
        }

        // Open + validate the new generation. A half-rsynced/truncated copy fails
        // its content-hash check here and the caller keeps the old one serving.
        let new_gen = Arc::new(
            Generation::open_with_store_opts(
                self.store.as_ref(),
                name,
                self.master_key.as_deref(),
                self.verify_integrity,
            )
            .with_context(|| format!("open swapped-in generation {on_disk} of graph '{name}'"))?,
        );

        // The swapped-in generation may carry a different ACL stamp / MAC presence
        // than the one it replaces. Re-apply the same policy before publishing it;
        // a violation returns Err and the caller keeps the old generation serving.
        let live_acl = self.live_acl_digest()?;
        self.check_manifest_policy(name, new_gen.manifest(), live_acl.as_deref())
            .with_context(|| format!("manifest policy for swapped-in generation of '{name}'"))?;

        // Pin the new generation's resident PQ *before* publishing it, then swap,
        // then unpin the old — so the pool never under-counts the resident set.
        for vi in new_gen.vamana_indexes() {
            vector_cache.pin(new_gen.uuid(), vi.ord, vi.pq.clone());
        }
        *slot.write().unwrap() = new_gen.clone();
        // Free the retired generation's whole resident set — pinned PQ codes *and*
        // lazily-built brute-force matrices — so it does not linger past the swap.
        vector_cache.unpin_generation(live.uuid());
        Ok(Some(new_gen.uuid()))
    }
}

// ── Generation guard (poll → exit / swap) ─────────────────────────────────────

/// The outcome of one guard sweep over every graph.
enum SweepAction {
    /// Nothing changed, or a `swap` was applied / refused; keep serving.
    Continue,
    /// The `exit` strategy detected a changed `current` for the named graph; the
    /// process must exit non-zero so the orchestrator restarts it.
    Shutdown(String),
}

/// One synchronous sweep of the generation guard over every graph. For each graph
/// whose on-disk `current` differs from the live generation, apply `strategy`:
/// `Exit` returns [`SweepAction::Shutdown`] at the first change; `Swap` opens +
/// validates the new generation and swaps it in (a corrupt/incomplete one is
/// logged and the old kept). Per-graph errors never abort the sweep.
///
/// Pure and synchronous (the swap does blocking IO) so it is unit-testable; the
/// async [`spawn_generation_guard`] wraps it with the poll timer + shutdown wiring.
fn guard_sweep(
    graphs: &Graphs,
    vector_cache: &VectorIndexCache,
    strategy: ReloadStrategy,
    acl: Option<&AclHandle>,
) -> SweepAction {
    for name in graphs.names() {
        let Some(live) = graphs.get(&name) else {
            continue;
        };
        let on_disk = match Generation::current_uuid_in(graphs.store.as_ref(), &name) {
            Ok(u) => u,
            Err(e) => {
                warn!(graph = %name, error = %e, "generation guard could not read the current pointer");
                continue;
            }
        };
        if on_disk == live.uuid().0 {
            continue;
        }
        match strategy {
            ReloadStrategy::Exit => return SweepAction::Shutdown(name),
            ReloadStrategy::Swap => match graphs.swap_if_changed(&name, vector_cache) {
                Ok(Some(new)) => {
                    info!(graph = %name, generation = %new, "swapped to a new generation (reloadStrategy=swap)");
                    // The swap's policy check already verified the live acl.json
                    // hashes to the new generation's stamp, so adopt it now: this is
                    // the legitimate channel for an ACL change (rebuild + publish),
                    // and it keeps the in-memory ACL in step with the new stamp so a
                    // later stamp-enforced poll does not reject the matching file.
                    if let Some(acl) = acl {
                        acl.reload();
                    }
                }
                Ok(None) => {} // raced back to the live generation
                Err(e) => {
                    warn!(graph = %name, error = %format!("{e:#}"), "refused an incomplete/corrupt new generation; keeping the current one")
                }
            },
        }
    }
    SweepAction::Continue
}

/// Spawn the background generation guard: a task that polls each graph's `current`
/// pointer every `poll_interval` and applies `strategy` on a change. For `Swap` it
/// swaps in a validated new generation (logging — never crashing — on a corrupt
/// one). For `Exit` it sends the changed graph's name down `shutdown` and stops, so
/// `serve` returns an error and the process exits non-zero for the orchestrator to
/// restart. **Poll, not inotify**: the data dir may be remote/network storage
/// (e.g. NFS), where filesystem change events are unreliable (D14/D16).
fn spawn_generation_guard(
    graphs: Arc<Graphs>,
    vector_cache: Arc<VectorIndexCache>,
    strategy: ReloadStrategy,
    poll_interval: Duration,
    shutdown: tokio::sync::oneshot::Sender<String>,
    acl: Option<Arc<AclHandle>>,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // `interval`'s first tick fires immediately; consume it so the first real
        // sweep waits a full interval (we have only just opened every generation).
        ticker.tick().await;
        let mut shutdown = Some(shutdown);
        loop {
            ticker.tick().await;
            let graphs = graphs.clone();
            let vector_cache = vector_cache.clone();
            let acl = acl.clone();
            // The sweep does blocking IO (re-hash + open on a swap), so it runs on
            // the blocking pool, off the async reactor.
            let action = match tokio::task::spawn_blocking(move || {
                guard_sweep(&graphs, &vector_cache, strategy, acl.as_deref())
            })
            .await
            {
                Ok(a) => a,
                Err(e) => {
                    warn!(error = %e, "generation guard sweep task failed");
                    continue;
                }
            };
            if let SweepAction::Shutdown(name) = action {
                error!(graph = %name, "generation changed on disk; exiting for orchestrator restart (reloadStrategy=exit)");
                if let Some(tx) = shutdown.take() {
                    let _ = tx.send(name);
                }
                return;
            }
        }
    });
}

/// Spawn the background cache-maintenance task: every so often it reclaims cache
/// entries that have been idle (untouched) for at least `ttl`, freeing memory
/// below the byte budgets when the working set goes quiet. Pinned PQ codes are
/// exempt. Only spawned when an idle TTL is configured (`cache.cacheTtlMs > 0`).
fn spawn_cache_maintenance(
    cache: Arc<BlockCache>,
    vector_cache: Arc<VectorIndexCache>,
    result_cache: Arc<ResultCache<QueryResult>>,
    ttl: Duration,
) {
    // Check ~4x per TTL window so an entry is reclaimed within ~TTL+25% of going
    // idle, clamped so we neither spin nor let a long TTL drift far past its mark.
    let sweep_every = (ttl / 4).clamp(Duration::from_secs(1), Duration::from_secs(30));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(sweep_every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume the immediate first tick — nothing is idle yet at startup.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let (c, v, r) = (cache.clone(), vector_cache.clone(), result_cache.clone());
            // Each sweep briefly takes the cache mutexes; run off the async reactor.
            if let Err(e) = tokio::task::spawn_blocking(move || {
                let now = Instant::now();
                c.evict_expired(now, ttl);
                v.evict_expired(now, ttl);
                r.evict_expired(now, ttl);
            })
            .await
            {
                warn!(error = %e, "cache maintenance sweep task failed");
            }
        }
    });
}

/// Build the shared worker pool for per-query parallelism (shortestPath frontier
/// expansion, multi-hop expansion, brute-force kNN, anchor scans, …), or `None`
/// when `fanout <= 1` (sequential). Sized to `min(fanout, cores)`; created once and
/// shared so per-query fanout never churns OS threads.
fn build_fanout_pool(fanout: usize) -> Option<Arc<rayon::ThreadPool>> {
    if fanout <= 1 {
        return None;
    }
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    rayon::ThreadPoolBuilder::new()
        .num_threads(fanout.min(cores))
        .thread_name(|i| format!("slater-q-{i}"))
        .build()
        .map(Arc::new)
        .ok()
}

// ── Shared connection context ─────────────────────────────────────────────────

/// Immutable state shared by every connection task.
struct ConnCtx {
    acl: Arc<AclHandle>,
    graphs: Arc<Graphs>,
    cache: Arc<BlockCache>,
    /// The vector-index pool (second cache pool): resident PQ codes (pinned) + the
    /// Vamana block LRU, with its own `vector_cache_bytes` budget. Shared across
    /// connections; the `AnnMode::Vamana` arm of the executor reads through it.
    vector_cache: Arc<VectorIndexCache>,
    /// The result LRU (third cache pool): caches whole executed results keyed by
    /// `(generation, normalised query + params)`, shared across connections.
    result_cache: Arc<ResultCache<QueryResult>>,
    max_rows: usize,
    timeout_ms: u64,
    /// Per-query intermediate-element budget (`query.maxIntermediate`); 0 disables.
    max_intermediate: u64,
    /// Per-query transient walk-work budget for count-pushdown traversals
    /// (`query.maxScan`); memory-flat, so set high. 0 disables.
    max_scan: u64,
    /// Server-wide ceiling on the sum of all in-flight queries' intermediate
    /// charges (`query.maxIntermediateGlobal`); shared by every per-query engine so
    /// concurrency cannot multiply the per-query budget into an OOM. 0 disables.
    intermediate_budget: Arc<GlobalIntermediateBudget>,
    /// Per-query `shortestPath()` BFS discovery cap (`query.maxShortestPathExplore`);
    /// 0 = unlimited.
    max_shortest_path_explore: u64,
    /// Shared worker pool for per-query parallelism (shortestPath frontier expansion,
    /// multi-hop expansion, brute-force kNN, anchor scans, …), sized to
    /// `query.maxFanout`. `None` when the fanout is ≤ 1 (sequential). Built once per
    /// process and shared across connections (per-query pool creation would churn OS
    /// threads).
    fanout_pool: Option<Arc<rayon::ThreadPool>>,
    /// Beam-search list size for the Vamana arm (`vectorQuery.beamWidth`).
    beam_width: usize,
    /// `bind:port`, reported as the address in `SHOW DATABASES` rows.
    bind_addr: String,
    /// Graph flagged as the home database in `SHOW DATABASES` (`config.defaultGraph`);
    /// `None` ⇒ the sole readable graph is home, or none is marked. This is display
    /// metadata only — it is never used to auto-select a graph for a query, so an
    /// ambiguous session always errors rather than silently serving this graph.
    default_graph: Option<String>,
    /// The graph each user last selected with `USE <graph>`, keyed by principal. Kept
    /// per-user rather than per-connection because pooled drivers (e.g. Memgraph Lab's
    /// neo4j-javascript driver) spread a logical session's queries across several Bolt
    /// connections — a per-connection selection would survive only until the pool
    /// rotated. Consulted when a db-less query needs to resolve its graph.
    use_selection: RwLock<HashMap<String, String>>,
    /// Principals observed issuing Memgraph-only statements (`SHOW STORAGE INFO`,
    /// `CALL mg.*`, …). Memgraph Lab and Neo4j Browser expect different shapes for
    /// some answers (notably `SHOW DATABASES`); this lets us reply in the right
    /// dialect. Tracked per-user, not per-connection, because a pooled driver may ask
    /// `SHOW DATABASES` on a connection that has not yet shown its dialect.
    memgraph_users: RwLock<HashSet<String>>,

    // ── Connection-security limits (see `config::ServerConfig`) ──────────────
    /// Reassembly-body cap for an authenticated reader.
    max_message_bytes: usize,
    /// Reassembly-body cap before `LOGON` (tight; ratchets up on auth).
    max_pre_auth_bytes: usize,
    /// Handshake→`LOGON` deadline (ms); 0 = none.
    login_timeout_ms: u64,
    /// Idle read timeout (ms) for an authenticated connection; 0 = none.
    idle_timeout_ms: u64,
    /// Budget for connections that have not yet completed `LOGON`. A connection holds
    /// one permit from its first byte until authentication succeeds, then releases it,
    /// so a flood of anonymous sockets cannot starve authenticated readers.
    pre_auth_limit: Arc<Semaphore>,
    /// Live per-source connection counts (key: /32 for IPv4, /64 for IPv6).
    per_ip: Arc<Mutex<HashMap<IpAddr, usize>>>,
    /// Per-source concurrent-connection cap; 0 = unlimited.
    max_per_ip: usize,

    // ── Load-test diagnostics (see `config::AppConfig::load_test_diagnostics`) ─
    /// Gated diagnostics registry. Inert (no atomics touched) when disabled, so
    /// the hot path is unchanged; answers `CALL slater.diagnostics()` when on.
    diag: Arc<crate::diag::Diagnostics>,
    /// The same global connection semaphore the accept loop holds, kept here so
    /// the diagnostics snapshot can report live occupancy and headroom.
    conn_limit: Arc<Semaphore>,
    /// Configured global / pre-auth connection caps (0 = unlimited), echoed by the
    /// diagnostics snapshot next to live occupancy.
    max_connections: usize,
    max_pre_auth_connections: usize,
}

impl ConnCtx {
    /// The graph `user` last selected via `USE`, if any.
    fn current_selection(&self, user: &str) -> Option<String> {
        self.use_selection.read().unwrap().get(user).cloned()
    }

    /// Record `user`'s `USE <graph>` selection for subsequent db-less queries.
    fn set_selection(&self, user: &str, graph: &str) {
        self.use_selection
            .write()
            .unwrap()
            .insert(user.to_string(), graph.to_string());
    }

    /// Has `user` issued a Memgraph-dialect statement on any connection?
    fn is_memgraph(&self, user: &str) -> bool {
        self.memgraph_users.read().unwrap().contains(user)
    }

    /// Flag `user` as a Memgraph-dialect client (idempotent).
    fn mark_memgraph(&self, user: &str) {
        self.memgraph_users
            .write()
            .unwrap()
            .insert(user.to_string());
    }
}

impl ConnCtx {
    /// Resolve the graph a `RUN` targets and check the user may read it. An explicit
    /// `db` in the message metadata wins; otherwise the user's single readable graph
    /// is used, and ambiguity (or none) is an error.
    fn select_graph(
        &self,
        extra: &PsValue,
        user: &str,
        sticky: Option<&str>,
    ) -> std::result::Result<String, Failure> {
        let acl = self.acl.snapshot();
        if let Some(db) = extra
            .get("db")
            .and_then(PsValue::as_str)
            .filter(|s| !s.is_empty())
        {
            if self.graphs.get(db).is_none() {
                let mut served: Vec<String> = self
                    .graphs
                    .names()
                    .into_iter()
                    .filter(|g| acl.can_read(user, g))
                    .collect();
                served.sort();
                return Err(Failure::new(
                    CODE_NOT_FOUND,
                    format!(
                        "graph '{db}' is not served (available: {})",
                        served.join(", ")
                    ),
                ));
            }
            if !acl.can_read(user, db) {
                return Err(Failure::new(
                    CODE_FORBIDDEN,
                    format!("user '{user}' has no read grant on graph '{db}'"),
                ));
            }
            return Ok(db.to_string());
        }
        // No explicit `db`: honour a sticky `USE <graph>` selection if it is still
        // served and readable, before falling back to the single-graph / ambiguous
        // resolution.
        if let Some(g) = sticky {
            if self.graphs.get(g).is_some() && acl.can_read(user, g) {
                return Ok(g.to_string());
            }
        }
        let mut readable: Vec<String> = self
            .graphs
            .names()
            .into_iter()
            .filter(|g| acl.can_read(user, g))
            .collect();
        match readable.len() {
            1 => Ok(readable.pop().unwrap()),
            0 => Err(Failure::new(
                CODE_FORBIDDEN,
                format!("user '{user}' has no readable graph"),
            )),
            // Ambiguous: the session named no graph but can read several. We do NOT
            // silently fall back to a default — that masks a mistyped or unset graph
            // name by serving an unrelated graph. Require an exact name and tell the
            // client which graphs are on offer.
            _ => {
                readable.sort();
                Err(Failure::new(
                    CODE_NOT_FOUND,
                    format!(
                        "no graph selected: name an exact graph in the connection's \
                         database field (one of: {})",
                        readable.join(", ")
                    ),
                ))
            }
        }
    }

    /// The graphs `user` may read, each flagged whether it is the default/home
    /// graph (the configured `defaultGraph`, or the sole graph when there is one).
    /// Used to answer `SHOW DATABASES`.
    fn readable_databases(&self, user: &str) -> Vec<(String, bool)> {
        let acl = self.acl.snapshot();
        let mut names: Vec<String> = self
            .graphs
            .names()
            .into_iter()
            .filter(|g| acl.can_read(user, g))
            .collect();
        names.sort();
        let default = self
            .default_graph
            .clone()
            .filter(|dg| names.iter().any(|g| g == dg))
            .or_else(|| (names.len() == 1).then(|| names[0].clone()));
        names
            .into_iter()
            .map(|n| {
                let is_default = default.as_deref() == Some(n.as_str());
                (n, is_default)
            })
            .collect()
    }

    /// Intercept the read-only introspection / metadata statements a browser GUI
    /// fires on connect (which the strict read-only Cypher grammar would reject),
    /// answering them from the in-memory manifest. Returns `Ok(None)` for anything
    /// that is not such a statement, so the caller falls through to the query path.
    /// Assemble the live (non-counter) state the diagnostics snapshot needs:
    /// connection-semaphore occupancy, configured caps, and the three cache pools.
    /// Read on demand from `CALL slater.diagnostics()` only, never on the hot path.
    fn live_gauges(&self) -> crate::diag::LiveGauges {
        use crate::diag::{CachePoolSnapshot, LiveGauges};
        // In-use = configured permits − currently available. `semaphore_permits`
        // maps the "0 = unlimited" config to the sentinel the semaphore was built
        // with, so the subtraction matches the live permit count either way.
        let in_use = |configured: usize, sem: &Semaphore| -> u64 {
            (semaphore_permits(configured) as u64).saturating_sub(sem.available_permits() as u64)
        };
        let (bm, vm, rm) = (
            self.cache.metrics(),
            self.vector_cache.metrics(),
            self.result_cache.metrics(),
        );
        LiveGauges {
            conn_in_use: in_use(self.max_connections, &self.conn_limit),
            conn_limit: self.max_connections as u64,
            pre_auth_in_use: in_use(self.max_pre_auth_connections, &self.pre_auth_limit),
            pre_auth_limit: self.max_pre_auth_connections as u64,
            max_per_ip: self.max_per_ip as u64,
            max_rows: self.max_rows as u64,
            timeout_ms: self.timeout_ms,
            max_intermediate: self.max_intermediate,
            max_scan: self.max_scan,
            max_intermediate_global: self.intermediate_budget.limit(),
            intermediate_global_in_use: self.intermediate_budget.in_use(),
            intermediate_global_peak: self.intermediate_budget.peak(),
            max_shortest_path_explore: self.max_shortest_path_explore,
            // The effective fanout = the pool's thread count (1 when sequential).
            max_fanout: self
                .fanout_pool
                .as_ref()
                .map_or(1, |p| p.current_num_threads()) as u64,
            max_message_bytes: self.max_message_bytes as u64,
            block_cache: CachePoolSnapshot {
                bytes: self.cache.bytes() as u64,
                entries: self.cache.len() as u64,
                hits: bm.hits,
                misses: bm.misses,
                evictions: bm.evictions,
            },
            vector_cache: CachePoolSnapshot {
                bytes: self.vector_cache.bytes() as u64,
                entries: self.vector_cache.block_count() as u64,
                hits: vm.hits,
                misses: vm.misses,
                evictions: vm.evictions,
            },
            result_cache: CachePoolSnapshot {
                bytes: self.result_cache.bytes() as u64,
                entries: self.result_cache.len() as u64,
                hits: rm.hits,
                misses: rm.misses,
                evictions: rm.evictions,
            },
        }
    }

    fn introspect(
        &self,
        user: &str,
        extra: &PsValue,
        query: &str,
        sticky: Option<&str>,
        memgraph: bool,
    ) -> std::result::Result<Option<introspect::Rows>, Failure> {
        let q = normalize_query(query);

        // Graph-agnostic (server-level) statements — answerable without a graph.
        let agnostic = match q.as_str() {
            _ if q.starts_with("call dbms.components") => Some(introspect::dbms_components()),
            // Memgraph Lab and Neo4j Browser want different `SHOW DATABASES` shapes.
            _ if q.starts_with("show databases") => Some(if memgraph {
                introspect::show_databases_memgraph(&self.readable_databases(user))
            } else {
                introspect::show_databases(&self.readable_databases(user), &self.bind_addr)
            }),
            _ if q.starts_with("show default database") => Some(introspect::show_databases(
                &self.readable_databases(user),
                &self.bind_addr,
            )),
            _ if q.starts_with("show version") => Some(introspect::show_version()),
            _ if q.starts_with("show license info") => Some(introspect::show_license_info()),
            _ if q.starts_with("show replication role") => {
                Some(introspect::show_replication_role())
            }
            _ if q == "show database" => Some(introspect::show_database(sticky)),
            _ if q.starts_with("show procedures") => Some(introspect::show_procedures()),
            _ if q.starts_with("show functions") => {
                Some(introspect::empty(&["name", "category", "description"]))
            }
            _ if q.starts_with("show constraints") => Some(introspect::empty(&[
                "id",
                "name",
                "type",
                "entityType",
                "labelsOrTypes",
                "properties",
                "ownedIndex",
            ])),
            _ if q.starts_with("show constraint info") => Some(introspect::empty(&[
                "constraint type",
                "label",
                "properties",
            ])),
            _ if q.starts_with("show triggers") => Some(introspect::empty(&[
                "trigger name",
                "statement",
                "event type",
                "phase",
                "owner",
            ])),
            _ if q.starts_with("show transactions") => Some(introspect::empty(&[
                "transactionId",
                "username",
                "currentQuery",
            ])),
            _ => None,
        };
        if let Some(rows) = agnostic {
            return Ok(Some(rows));
        }

        // `CALL slater.diagnostics()` / `SHOW SERVER DIAGNOSTICS` — the gated
        // load-test health snapshot: process RSS/CPU, the cgroup memory & CPU
        // limits, connection-cap headroom, per-reason query-failure tallies, and
        // latency percentiles. Server-level (no graph needed). Errors unless
        // `loadTestDiagnostics` is on, so the surface stays dark by default and the
        // hot path keeps no extra state.
        if q.starts_with("call slater.diagnostics") || q.starts_with("show server diagnostics") {
            if !self.diag.enabled {
                return Err(Failure::new(
                    CODE_REQUEST,
                    "load-test diagnostics are disabled; set loadTestDiagnostics=true to \
                     enable CALL slater.diagnostics()"
                        .to_string(),
                ));
            }
            let live = self.live_gauges();
            let rows = self.diag.snapshot(&live);
            return Ok(Some(introspect::server_diagnostics(&rows)));
        }

        // `SHOW STORAGE INFO` is graph-scoped *and* carries the live per-pool cache
        // metrics (block / vector / result) so an operator can watch residency, hit
        // rate, and eviction pressure — the evidence for tuning the budget split.
        if q.starts_with("show storage info") {
            let graph = self.select_graph(extra, user, sticky)?;
            let gen = self.graphs.get(&graph).ok_or_else(|| {
                Failure::new(CODE_NOT_FOUND, format!("graph '{graph}' is not served"))
            })?;
            let (bm, vm, rm) = (
                self.cache.metrics(),
                self.vector_cache.metrics(),
                self.result_cache.metrics(),
            );
            let pools = [
                introspect::CachePoolStat {
                    name: "block",
                    bytes: self.cache.bytes(),
                    entries: self.cache.len(),
                    hits: bm.hits,
                    misses: bm.misses,
                    evictions: bm.evictions,
                },
                introspect::CachePoolStat {
                    name: "vector",
                    bytes: self.vector_cache.bytes(),
                    entries: self.vector_cache.block_count(),
                    hits: vm.hits,
                    misses: vm.misses,
                    evictions: vm.evictions,
                },
                introspect::CachePoolStat {
                    name: "result",
                    bytes: self.result_cache.bytes(),
                    entries: self.result_cache.len(),
                    hits: rm.hits,
                    misses: rm.misses,
                    evictions: rm.evictions,
                },
            ];
            return Ok(Some(introspect::show_storage_info_with_caches(
                gen.manifest(),
                &pools,
            )));
        }

        // Graph-scoped statements — resolve the graph (honouring an explicit `db`
        // or the default) and read its manifest.
        let scoped: Option<fn(&graph_format::manifest::Manifest) -> introspect::Rows> =
            if q.starts_with("call db.labels") {
                Some(introspect::db_labels)
            } else if q.starts_with("call db.relationshiptypes") {
                Some(introspect::db_relationship_types)
            } else if q.starts_with("call db.propertykeys") {
                Some(introspect::db_property_keys)
            } else if q.starts_with("show indexes") {
                Some(introspect::show_indexes)
            } else if q.starts_with("call db.indexes") {
                Some(introspect::db_indexes)
            } else if q.starts_with("show index info") {
                Some(introspect::show_index_info)
            } else if q.starts_with("call db.schema.visualization") {
                Some(|_| introspect::schema_visualization())
            } else {
                None
            };
        if let Some(build) = scoped {
            let graph = self.select_graph(extra, user, sticky)?;
            let gen = self.graphs.get(&graph).ok_or_else(|| {
                Failure::new(CODE_NOT_FOUND, format!("graph '{graph}' is not served"))
            })?;
            return Ok(Some(build(gen.manifest())));
        }

        Ok(None)
    }
}

/// Normalise a query for introspection matching: collapse whitespace, lowercase,
/// strip a leading `EXPLAIN`/`PROFILE`, and drop a trailing `;`.
fn normalize_query(query: &str) -> String {
    let mut q = query.split_whitespace().collect::<Vec<_>>().join(" ");
    q.make_ascii_lowercase();
    while let Some(rest) = q
        .strip_prefix("explain ")
        .or_else(|| q.strip_prefix("profile "))
    {
        q = rest.to_string();
    }
    q.trim_end_matches(';').trim().to_string()
}

/// Strip an optional leading `GQL` / `CYPHER` dialect selector from a statement,
/// returning the remainder to parse. This mirrors Neo4j's `CYPHER 5` / `CYPHER 25`
/// dialect prefix: the language is chosen by a query-string token, never a protocol
/// negotiation. Routing is a deliberate no-op today — one parser serves both Cypher
/// and the GQL subset (DECISIONS D40) — so we record nothing and simply hand the
/// rest to `parser::parse`, keeping `parser.rs` language-agnostic.
///
/// Only a leading `GQL` / `CYPHER` keyword (case-insensitive, at a token boundary),
/// optionally followed by a single bare numeric version token (`5`, `25`, `5.0`), is
/// consumed. A following query keyword (`CYPHER MATCH …`) is preserved untouched, and
/// anything that is not such a prefix is returned unchanged — so a bare query, and an
/// identifier such as `cypher_score`, are never disturbed. The prefix is recognised
/// only at the very start of the statement.
fn strip_dialect_prefix(query: &str) -> &str {
    let trimmed = query.trim_start();
    for kw in ["gql", "cypher"] {
        // The keyword must be followed by whitespace, so a longer identifier sharing
        // the prefix (e.g. `cypher_x`) is never mistaken for a dialect selector.
        let Some(after_kw) = trimmed.get(..kw.len()).and_then(|head| {
            head.eq_ignore_ascii_case(kw)
                .then(|| &trimmed[kw.len()..])
                .filter(|rest| rest.starts_with(char::is_whitespace))
        }) else {
            continue;
        };
        // Optionally swallow a single numeric version token (`CYPHER 25`); a query
        // keyword in that slot (`CYPHER MATCH`) is left in place.
        let rest = after_kw.trim_start();
        return match rest.split_once(char::is_whitespace) {
            Some((first, tail)) if is_version_token(first) => tail.trim_start(),
            _ => rest,
        };
    }
    query
}

/// A bare dialect-version token: digits and dots only (`5`, `25`, `5.0`), nothing
/// like a query keyword. Used by [`strip_dialect_prefix`] to tell `CYPHER 25 MATCH`
/// (version) from `CYPHER MATCH` (no version).
fn is_version_token(t: &str) -> bool {
    !t.is_empty() && t.chars().all(|c| c.is_ascii_digit() || c == '.')
}

/// Recognise a `USE <graph>` / `USE DATABASE <graph>` statement and extract the graph
/// name (preserving its original case — graph names are case-sensitive). Returns
/// `None` for anything that is not a bare `USE`. A backtick- or quote-wrapped name is
/// unwrapped. Memgraph's database-switch statement; not part of slater's read-only
/// Cypher grammar, so it is handled out-of-band in the `RUN` path.
fn parse_use_statement(query: &str) -> Option<String> {
    let trimmed = query.trim().trim_end_matches(';').trim();
    let mut words = trimmed.split_whitespace();
    let first = words.next()?;
    if !first.eq_ignore_ascii_case("use") {
        return None;
    }
    let mut rest: Vec<&str> = words.collect();
    // Optional `DATABASE` keyword: `USE DATABASE <name>`.
    if rest
        .first()
        .is_some_and(|w| w.eq_ignore_ascii_case("database"))
    {
        rest.remove(0);
    }
    if rest.len() != 1 {
        return None;
    }
    let name = rest[0]
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'');
    (!name.is_empty()).then(|| name.to_string())
}

/// Does the (already normalised) query belong to Memgraph's SQL dialect — a
/// statement only a Memgraph client (e.g. Memgraph Lab) issues? Seeing one lets us
/// answer dialect-sensitive statements (notably `SHOW DATABASES`) in Memgraph's shape
/// for that user rather than Neo4j's.
fn is_memgraph_dialect_query(q: &str) -> bool {
    const MARKERS: &[&str] = &[
        "show storage info",
        "show index info",
        "show constraint info",
        "show replication role",
        "show replicas",
        "show streams",
        "show metrics info",
        "show triggers",
        "show version",
        "call mg.",
    ];
    MARKERS.iter().any(|m| q.starts_with(m))
}

// ── Failure ────────────────────────────────────────────────────────────────

/// A Bolt `FAILURE` to send: a status code and a human message.
struct Failure {
    code: &'static str,
    message: String,
}

impl Failure {
    fn new(code: &'static str, message: String) -> Self {
        Self { code, message }
    }

    fn unauthorized(message: &str) -> Self {
        Self::new(CODE_UNAUTHORIZED, message.into())
    }

    /// Classify a parser/executor `anyhow` error into a Bolt status code.
    fn from_query_error(e: &anyhow::Error) -> Self {
        let m = e.to_string();
        let code = if m.contains("read-only") {
            CODE_ACCESS_MODE
        } else if m.contains("syntax error") {
            CODE_SYNTAX
        } else {
            CODE_EXECUTION
        };
        Self::new(code, m)
    }

    /// Map the Neo4j status code to an ISO-GQL GQLSTATUS code + description. The
    /// codes follow GQL's SQLSTATE-style classes: `42000` syntax error or access
    /// rule violation (a malformed or read-only-rejected statement), `50000` general
    /// processing exception (everything else). Additive — the legacy `code`/`message`
    /// still ship verbatim (DECISIONS D41). Description follows GQL house style:
    /// `error: <condition>. <message>`.
    fn gqlstatus(&self) -> (&'static str, String) {
        let (status, condition) = match self.code {
            // A bad statement or a write/procedure rejected by read-only mode is, in
            // GQL terms, a syntax-or-access-rule violation (class 42).
            CODE_SYNTAX | CODE_ACCESS_MODE | CODE_UNAUTHORIZED | CODE_FORBIDDEN => {
                ("42000", "syntax error or access rule violation")
            }
            // Missing graph, bad request, execution failure: general processing.
            _ => ("50000", "general processing exception"),
        };
        (status, format!("error: {condition}. {}", self.message))
    }

    fn to_message(&self) -> PsValue {
        let (status, description) = self.gqlstatus();
        message::failure_gqlstatus(self.code, &self.message, status, &description)
    }
}

/// The ISO-GQL completion status for a result that has drained `row_count` rows,
/// as the two additive metadata pairs appended to the final PULL/DISCARD SUCCESS:
/// `00000` (successful completion) normally, or `02000` (no data) when the result
/// was empty (DECISIONS D41). Purely additive — `has_more` and any other summary
/// keys are untouched, so existing drivers ignore these and GQL-aware ones read
/// the standard status.
fn gqlstatus_completion(row_count: usize) -> Vec<(String, PsValue)> {
    let (status, description) = if row_count == 0 {
        ("02000", "note: no data")
    } else {
        ("00000", "note: successful completion")
    };
    vec![
        ("gql_status".into(), PsValue::str(status)),
        ("status_description".into(), PsValue::str(description)),
    ]
}

// ── Per-connection session ────────────────────────────────────────────────────

/// Result rows already encoded to PackStream, awaiting `PULL`/`DISCARD`.
struct Pending {
    rows: Vec<Vec<PsValue>>,
    sent: usize,
}

/// Mutable per-connection state.
struct Session {
    /// The authenticated user (set at `LOGON`), if any.
    user: Option<String>,
    /// In the Bolt FAILED state: every message but `RESET` is answered `IGNORED`.
    failed: bool,
    /// A buffered result a `PULL` will drain.
    pending: Option<Pending>,
    /// The graph resolved and validated at `BEGIN`, held for the life of an explicit
    /// transaction (Bolt sends the `db` only on `BEGIN`, not on the `RUN`s inside it).
    /// `None` outside a transaction, where each auto-commit `RUN` resolves its own.
    tx_graph: Option<String>,
    /// Negotiated Bolt version `(major, minor)`; gates element-id struct fields.
    version: (u8, u8),
}

// ── Framing over an async stream ──────────────────────────────────────────────

/// A Bolt message framer over any async byte stream (plain TCP or TLS).
struct Framed<S> {
    stream: S,
    buf: Vec<u8>,
    /// Largest reassembled message body this connection will currently accept. The
    /// framer is deliberately auth-blind — it owns a budget number, not the reason
    /// it changed. `handle_connection` starts it at the tight pre-auth cap and
    /// bumps it to the generous post-auth cap once `LOGON` succeeds (ratcheting it
    /// back down on `LOGOFF`).
    max_body: usize,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Framed<S> {
    fn new(stream: S, max_body: usize) -> Self {
        Self {
            stream,
            buf: Vec::with_capacity(8192),
            max_body,
        }
    }

    /// Read the next complete (de-chunked) message body, or `None` at a clean EOF.
    async fn read_message(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            if let Some((body, consumed)) = chunk::decode_message_capped(&self.buf, self.max_body)?
            {
                self.buf.drain(..consumed);
                return Ok(Some(body));
            }
            let mut tmp = [0u8; 8192];
            let n = self.stream.read(&mut tmp).await?;
            if n == 0 {
                if self.buf.is_empty() {
                    return Ok(None);
                }
                bail!("connection closed mid-message");
            }
            self.buf.extend_from_slice(&tmp[..n]);
            // Bound the unparsed buffer too: `decode_message_capped` only caps a
            // *complete* body, but a peer that streams chunks and never sends the
            // terminating `00 00` would grow `buf` without bound. Allow a little
            // slack over the body cap for chunk headers. With the pre-auth cap in
            // force this keeps an unauthenticated peer's footprint tiny.
            if self.buf.len() > self.max_body + (1 << 20) {
                bail!(
                    "Bolt message framing exceeded {} bytes without completing; closing connection",
                    self.max_body
                );
            }
        }
    }

    async fn write_message(&mut self, msg: &PsValue) -> Result<()> {
        self.stream.write_all(&message::to_wire(msg)).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await?;
        Ok(())
    }
}

// ── Listener ─────────────────────────────────────────────────────────────────

/// Bind the configured address and serve Bolt connections until the process exits.
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
    let mut graphs = Graphs::open_all_with_store(store, master_key.as_deref(), verify_integrity)?;
    graphs.set_manifest_policy(Some(PathBuf::from(&cfg.acl_path)), cfg.require_acl_stamp);
    graphs
        .verify_manifest_policy()
        .context("manifest authentication policy")?;
    let graphs = Arc::new(graphs);
    if graphs.is_empty() {
        warn!(data_dir = %cfg.data_dir(), "no graphs found to serve");
    }
    let cache = Arc::new(BlockCache::new(cfg.cache.block_cache_bytes));
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

    let ctx = Arc::new(ConnCtx {
        acl,
        graphs,
        cache,
        vector_cache,
        result_cache,
        max_rows: cfg.query.max_rows as usize,
        timeout_ms: cfg.query.timeout_ms,
        max_intermediate: cfg.query.max_intermediate,
        max_scan: cfg.query.max_scan,
        intermediate_budget: Arc::new(GlobalIntermediateBudget::new(
            cfg.query.max_intermediate_global,
        )),
        max_shortest_path_explore: cfg.query.max_shortest_path_explore,
        fanout_pool: build_fanout_pool(cfg.query.max_fanout),
        beam_width: cfg.vector_query.beam_width as usize,
        bind_addr: format!("{}:{}", cfg.server.bind, cfg.server.port),
        default_graph: Some(cfg.default_graph.clone()).filter(|g| !g.is_empty()),
        use_selection: RwLock::new(HashMap::new()),
        memgraph_users: RwLock::new(HashSet::new()),
        max_message_bytes: cfg.server.max_message_bytes,
        max_pre_auth_bytes: cfg.server.max_pre_auth_bytes,
        login_timeout_ms: cfg.server.login_timeout_ms,
        idle_timeout_ms: cfg.server.idle_timeout_ms,
        pre_auth_limit: Arc::new(Semaphore::new(semaphore_permits(
            cfg.server.max_pre_auth_connections,
        ))),
        per_ip: Arc::new(Mutex::new(HashMap::new())),
        max_per_ip: cfg.server.max_connections_per_ip,
        diag: Arc::new(crate::diag::Diagnostics::new(cfg.load_test_diagnostics)),
        conn_limit: conn_limit.clone(),
        max_connections: cfg.server.max_connections,
        max_pre_auth_connections: cfg.server.max_pre_auth_connections,
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
async fn warm_cache(warming_query: &str, ctx: &Arc<ConnCtx>) {
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
    let max_rows = ctx.max_rows;
    let max_intermediate = ctx.max_intermediate;
    let max_scan = ctx.max_scan;
    let intermediate_budget = ctx.intermediate_budget.clone();
    let max_shortest_path_explore = ctx.max_shortest_path_explore;
    let fanout_pool = ctx.fanout_pool.clone();
    let timeout_ms = ctx.timeout_ms;

    // Execution is blocking and CPU-bound — keep it off the async runtime.
    let started = Instant::now();
    let _ = tokio::task::spawn_blocking(move || {
        for gen in generations {
            let g_start = Instant::now();
            let mut engine = Engine::new(gen.as_ref(), cache.as_ref())
                .with_vector_cache(vector_cache.as_ref(), beam_width)
                .with_max_rows(max_rows)
                .with_max_intermediate(max_intermediate)
                .with_max_scan(max_scan)
                .with_global_budget(intermediate_budget.as_ref())
                .with_max_shortest_path_explore(max_shortest_path_explore)
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
async fn accept_loop(
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
                tokio::spawn(async move {
                    let _permit = permit; // global slot, released on connection end
                    let _per_ip_guard = per_ip_guard; // per-source count, released on end
                    if let Err(e) = serve_conn(sock, tls, ctx).await {
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
fn semaphore_permits(limit: usize) -> usize {
    if limit == 0 {
        Semaphore::MAX_PERMITS
    } else {
        limit
    }
}

/// The per-source counting key: the full address for IPv4 (/32), the /64 prefix
/// for IPv6. An attacker controls an entire /64, so keying on the full v6 address
/// would let them sidestep the cap by varying the low bits.
fn per_ip_key(addr: IpAddr) -> IpAddr {
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
struct PerIpGuard {
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
fn try_acquire_per_ip(
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

/// (Optionally) wrap the socket in TLS, then run the Bolt connection.
async fn serve_conn(sock: TcpStream, tls: Option<TlsAcceptor>, ctx: Arc<ConnCtx>) -> Result<()> {
    match tls {
        Some(acceptor) => {
            let stream = acceptor.accept(sock).await.context("TLS handshake")?;
            handle_connection(stream, ctx).await
        }
        None => handle_connection(sock, ctx).await,
    }
}

/// Build a `rustls` acceptor from the configured cert/key, or `None` when TLS is
/// disabled (plaintext, for loopback dev).
fn build_tls_acceptor(tls: &TlsConfig) -> Result<Option<TlsAcceptor>> {
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

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader = std::io::BufReader::new(
        std::fs::File::open(path).with_context(|| format!("open TLS certificate {path}"))?,
    );
    let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        bail!("no certificates found in {path}");
    }
    Ok(certs)
}

fn load_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    let mut reader = std::io::BufReader::new(
        std::fs::File::open(path).with_context(|| format!("open TLS private key {path}"))?,
    );
    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| anyhow!("no private key found in {path}"))
}

// ── Connection state machine ──────────────────────────────────────────────────

/// Run one Bolt connection from handshake to close.
async fn handle_connection<S>(stream: S, ctx: Arc<ConnCtx>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    // Hold a pre-auth budget slot from the first byte until LOGON succeeds. If the
    // antechamber is full, reject immediately — queuing anonymous sockets would just
    // hold file descriptors, the very exhaustion this defends against.
    let mut pre_auth_permit = match ctx.pre_auth_limit.clone().try_acquire_owned() {
        Ok(p) => Some(p),
        Err(_) => {
            debug!("pre-auth connection budget reached; rejecting connection");
            ctx.diag.record_rejected_pre_auth();
            return Ok(());
        }
    };

    // The whole pre-auth phase (handshake → HELLO → LOGON) must finish within the
    // login deadline — the slow-loris guard a byte cap alone leaves open.
    let login_deadline = (ctx.login_timeout_ms > 0)
        .then(|| TokioInstant::now() + Duration::from_millis(ctx.login_timeout_ms));

    // Start under the tight pre-auth body cap; it ratchets up once LOGON succeeds.
    let mut framed = Framed::new(stream, ctx.max_pre_auth_bytes);

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
    framed.stream.write_all(&reply).await?;
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
    };

    loop {
        // Sync the per-connection budgets to the current auth state before each read.
        // The framer is auth-blind, so its cap is set here; the pre-auth permit is
        // released on the transition to authenticated and reclaimed on LOGOFF.
        if sess.user.is_some() {
            framed.max_body = ctx.max_message_bytes;
            pre_auth_permit = None; // free the antechamber slot for the next anon peer
        } else {
            framed.max_body = ctx.max_pre_auth_bytes;
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
                sess.pending = None;
                sess.tx_graph = None;
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
    }
    Ok(())
}

/// Verify `basic`-scheme credentials from a `LOGON` (or 4.4 `HELLO`) metadata map
/// against the ACL, recording the user on the session on success.
fn authenticate(
    sess: &mut Session,
    ctx: &Arc<ConnCtx>,
    meta: &PsValue,
) -> std::result::Result<(), Failure> {
    let scheme = meta.get("scheme").and_then(PsValue::as_str).unwrap_or("");
    if scheme != "basic" {
        ctx.diag.record_auth_failure();
        return Err(Failure::unauthorized(
            "only the 'basic' authentication scheme is supported",
        ));
    }
    let principal = meta
        .get("principal")
        .and_then(PsValue::as_str)
        .unwrap_or("");
    let credentials = meta
        .get("credentials")
        .and_then(PsValue::as_str)
        .unwrap_or("");
    // Pick up any out-of-band ACL edit before authenticating — but only adopt one
    // whose digest still matches the served generation's `aclBlake3` stamp. A
    // post-generation edit to `acl.json` (e.g. self-granting a read) is refused and
    // the last-good ACL kept; the legitimate way to change access control is to
    // rebuild and publish a generation stamped against the new file.
    let graphs = ctx.graphs.clone();
    ctx.acl
        .poll_checked(|digest| graphs.acl_digest_acceptable(digest));
    if ctx.acl.snapshot().verify(principal, credentials) {
        sess.user = Some(principal.to_string());
        Ok(())
    } else {
        ctx.diag.record_auth_failure();
        Err(Failure::unauthorized("invalid principal or credentials"))
    }
}

/// Handle one decoded request, returning the messages to send back (in order) or a
/// `Failure` (which the caller writes and then enters the FAILED state).
async fn handle_request(
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
                authenticate(sess, ctx, &meta)?;
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
            authenticate(sess, ctx, &meta)?;
            Ok(vec![message::success(vec![])])
        }

        Request::Logoff => {
            sess.user = None;
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
                Some(g) => g.clone(),
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
            // Parse synchronously so a syntax / read-only error is classified
            // cleanly; only the (blocking) execution moves to the blocking pool.
            let ast = parser::parse(&query).map_err(|e| Failure::from_query_error(&e))?;
            let param_vals = params_to_vals(&params)?;

            let (columns, rows) =
                run_query(ctx, gen, &query, ast, param_vals, sess.version).await?;
            sess.pending = Some(Pending { rows, sent: 0 });
            Ok(vec![message::success(vec![(
                "fields".into(),
                PsValue::List(columns.into_iter().map(PsValue::String).collect()),
            )])])
        }

        Request::Pull(meta) => {
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

        Request::Discard(_) => {
            // The discarded result still completes the statement, so carry the same
            // additive GQLSTATUS completion status as the final PULL.
            let row_count = sess.pending.as_ref().map_or(0, |p| p.rows.len());
            sess.pending = None;
            let mut meta = vec![("has_more".into(), PsValue::Bool(false))];
            meta.extend(gqlstatus_completion(row_count));
            Ok(vec![message::success(meta)])
        }

        // Handled before dispatch.
        Request::Reset | Request::Goodbye => Ok(vec![message::success(vec![])]),
    }
}

/// Execute the parsed query on the blocking pool and return its rows already
/// encoded to PackStream (node/relationship resolution reads through the same
/// block cache, so it stays off the async reactor).
///
/// The result LRU is consulted first: a hit skips execution entirely and re-encodes
/// the cached rows for this connection's Bolt version (the cache stores the
/// version-independent `QueryResult`, so encoding — which still resolves node/rel
/// records through the block cache — is the only per-connection work). A miss
/// executes, caches the result, then encodes.
async fn run_query(
    ctx: &Arc<ConnCtx>,
    gen: Arc<Generation>,
    query: &str,
    ast: parser::ast::Query,
    params: HashMap<String, Val>,
    version: (u8, u8),
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    let cache = ctx.cache.clone();
    let vector_cache = ctx.vector_cache.clone();
    let result_cache = ctx.result_cache.clone();
    let key = ResultKey::new(gen.uuid(), result_query_key(query, &params));
    // Queries calling `rand()`/`randomUUID()`/`timestamp()` must re-run every
    // time, so they bypass the result cache (both lookup and store).
    let cacheable = !parser::is_nondeterministic(&ast);
    let max_rows = ctx.max_rows;
    let timeout_ms = ctx.timeout_ms;
    let max_intermediate = ctx.max_intermediate;
    let max_scan = ctx.max_scan;
    let intermediate_budget = ctx.intermediate_budget.clone();
    let max_shortest_path_explore = ctx.max_shortest_path_explore;
    let fanout_pool = ctx.fanout_pool.clone();
    let beam_width = ctx.beam_width;
    let graph_name = gen.graph().to_string();
    // Gate all per-query instrumentation on the debug level being active OR
    // load-test diagnostics being enabled: when both are off, we take no
    // timestamps and no cache snapshots, and build no QueryTiming — the hot path
    // is exactly what it was before instrumentation. The default log level is
    // `debug`, so every query emits its `query executed` summary out of the box;
    // raising the level to `info`/`warn` restores the zero-overhead hot path.
    // Diagnostics needs the same `total_ms` for its latency histogram, so it
    // shares this gate.
    let instrument = tracing::enabled!(Level::DEBUG) || ctx.diag.enabled;

    ctx.diag.on_query_start();
    let join =
        tokio::task::spawn_blocking(move || -> Result<(EncodedRows, Option<QueryTiming>)> {
            // Per-query instrumentation (only when `instrument`): wall-clock split into
            // execute vs encode, and the block-cache hit/miss/eviction delta this query
            // caused (the counters are process-wide, so we snapshot before/after). A
            // result-cache hit skips execution, which shows up as exec_ms ≈ 0.
            let t_start = instrument.then(Instant::now);
            let blk_before = instrument.then(|| cache.metrics());

            // Result-cache lookup (skipped for non-deterministic queries), then
            // execute-and-cache on a miss.
            let cached = if cacheable {
                result_cache.get(&key)
            } else {
                None
            };
            // `cost` is the elements charged against the query budget; it is only
            // meaningful when the query actually executed, so a result-cache hit
            // (no engine) reports `None` and the summary omits the field.
            let (result, result_cache_hit, cost) = match cached {
                Some(r) => (r, true, None),
                None => {
                    let mut engine = Engine::new(gen.as_ref(), cache.as_ref())
                        .with_vector_cache(vector_cache.as_ref(), beam_width)
                        .with_params(params)
                        .with_max_rows(max_rows)
                        .with_max_intermediate(max_intermediate)
                        .with_max_scan(max_scan)
                        .with_global_budget(intermediate_budget.as_ref())
                        .with_max_shortest_path_explore(max_shortest_path_explore)
                        .with_fanout_pool(fanout_pool.clone());
                    if timeout_ms > 0 {
                        engine = engine
                            .with_deadline(Instant::now() + Duration::from_millis(timeout_ms));
                    }
                    let r = Arc::new(engine.run(&ast)?);
                    let cost = engine.cost();
                    if cacheable {
                        let bytes = estimate_result_bytes(&r);
                        result_cache.insert(key.clone(), r.clone(), bytes);
                    }
                    (r, false, Some(cost))
                }
            };
            let t_after_exec = instrument.then(Instant::now);

            // Encode for this connection's version. A plain engine (no params/limits
            // needed) resolves Node/Relationship records through the shared block cache.
            let engine = Engine::new(gen.as_ref(), cache.as_ref());
            let mut rows = Vec::with_capacity(result.rows.len());
            for row in &result.rows {
                let mut encoded = Vec::with_capacity(row.len());
                for v in row {
                    encoded.push(encode_val(&engine, version, v)?);
                }
                rows.push(encoded);
            }

            let timing = if instrument {
                let t_end = Instant::now();
                let blk_after = cache.metrics();
                let blk_before = blk_before.unwrap();
                let t_start = t_start.unwrap();
                let t_after_exec = t_after_exec.unwrap();
                Some(QueryTiming {
                    result_cache_hit,
                    cost,
                    exec_ms: (t_after_exec - t_start).as_secs_f64() * 1e3,
                    encode_ms: (t_end - t_after_exec).as_secs_f64() * 1e3,
                    total_ms: (t_end - t_start).as_secs_f64() * 1e3,
                    rows: rows.len(),
                    blk_hits: blk_after.hits.saturating_sub(blk_before.hits),
                    blk_misses: blk_after.misses.saturating_sub(blk_before.misses),
                    blk_evictions: blk_after.evictions.saturating_sub(blk_before.evictions),
                })
            } else {
                None
            };
            Ok(((result.columns.clone(), rows), timing))
        })
        .await;

    match join {
        Ok(Ok((out, timing))) => {
            // Only ever `Some` when the debug level was active (see `instrument`).
            // A block-cache miss is a cold block read (pread + decompress); many
            // misses on a small query is the signature of an unindexed scan. A high
            // total_ms with result_cache=miss and many blk_misses points at exactly
            // that.
            // Feed the diagnostics latency histogram (no-op when disabled). When
            // diagnostics are on, `instrument` is true so `timing` is always Some.
            let total_ms = timing.as_ref().map(|t| t.total_ms);
            if let Some(t) = timing {
                debug!(
                    graph = %graph_name,
                    // A result-cache hit ran no engine, so it charges no budget:
                    // `cost = 0` alongside `result_cache = "hit"`.
                    cost = t.cost.unwrap_or(0),
                    rows = t.rows,
                    result_cache = if t.result_cache_hit { "hit" } else { "miss" },
                    exec_ms = format_args!("{:.1}", t.exec_ms),
                    encode_ms = format_args!("{:.1}", t.encode_ms),
                    total_ms = format_args!("{:.1}", t.total_ms),
                    blk_hits = t.blk_hits,
                    blk_misses = t.blk_misses,
                    blk_hit_ratio = format_args!("{:.2}", hit_ratio(t.blk_hits, t.blk_misses)),
                    blk_evicted = t.blk_evictions,
                    query = %log_query(query),
                    "query executed"
                );
            }
            ctx.diag.on_query_ok(total_ms.unwrap_or(0.0));
            Ok(out)
        }
        Ok(Err(e)) => {
            ctx.diag.on_query_err(&e);
            Err(Failure::from_query_error(&e))
        }
        Err(e) => {
            ctx.diag.on_query_task_failed();
            Err(Failure::new(
                CODE_EXECUTION,
                format!("query task failed: {e}"),
            ))
        }
    }
}

/// Column names plus the PackStream-encoded rows — the shape `run_query`'s
/// blocking task produces.
type EncodedRows = (Vec<String>, Vec<Vec<PsValue>>);

/// Per-query timing + cache-delta, captured inside the blocking task and logged
/// once the result returns (see [`run_query`]).
struct QueryTiming {
    result_cache_hit: bool,
    /// Elements charged against the query budget (`Engine::cost`); `None` on a
    /// result-cache hit, where no engine ran.
    cost: Option<u64>,
    exec_ms: f64,
    encode_ms: f64,
    total_ms: f64,
    rows: usize,
    blk_hits: u64,
    blk_misses: u64,
    blk_evictions: u64,
}

/// Block-cache hit ratio `hits / (hits + misses)` for a single query, as a
/// fraction in `[0.0, 1.0]`. A query that touched no blocks (e.g. a pure
/// `RETURN 1`, or a result-cache hit) has no accesses and reports `0.0`.
pub(crate) fn hit_ratio(hits: u64, misses: u64) -> f64 {
    let total = hits + misses;
    if total == 0 {
        0.0
    } else {
        hits as f64 / total as f64
    }
}

/// Collapse a query's whitespace and truncate it for a single-line log field.
fn log_query(query: &str) -> String {
    let one_line = query.split_whitespace().collect::<Vec<_>>().join(" ");
    if one_line.chars().count() > 160 {
        let truncated: String = one_line.chars().take(160).collect();
        format!("{truncated}…")
    } else {
        one_line
    }
}

/// Build the normalised query portion of a [`ResultKey`]: the query text with
/// runs of whitespace collapsed, followed by the parameters serialised in a
/// deterministic (name-sorted) order. Two textually-different-but-equivalent
/// whitespace variants share a cache entry; differing params do not.
fn result_query_key(query: &str, params: &HashMap<String, Val>) -> String {
    let mut s = query.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut names: Vec<&String> = params.keys().collect();
    names.sort();
    for name in names {
        // \u{1} is not valid in a query, so it cannot collide with query content.
        s.push('\u{1}');
        s.push_str(name);
        s.push('=');
        s.push_str(&format!("{:?}", params[name]));
    }
    s
}

/// A coarse estimate of a result's resident footprint, used to charge it against
/// the result-cache budget. Exactness is not required — it only needs to grow with
/// the result so the byte budget bounds memory.
fn estimate_result_bytes(r: &QueryResult) -> usize {
    let cols: usize = r.columns.iter().map(|c| c.len() + 16).sum();
    let rows: usize = r
        .rows
        .iter()
        .map(|row| row.iter().map(val_bytes).sum::<usize>())
        .sum();
    cols + rows + 64
}

fn val_bytes(v: &Val) -> usize {
    match v {
        Val::Null | Val::Bool(_) | Val::Int(_) | Val::Float(_) => 16,
        Val::Str(s) => s.len() + 16,
        Val::List(xs) => 16 + xs.iter().map(val_bytes).sum::<usize>(),
        Val::Vector(xs) => 16 + xs.len() * 4,
        Val::Map(m) => {
            16 + m
                .iter()
                .map(|(k, x)| k.len() + 16 + val_bytes(x))
                .sum::<usize>()
        }
        Val::Node(_) => 24,
        Val::Rel { .. } => 40,
        Val::Path { nodes, rels } => {
            16 + nodes.len() * 24 + rels.iter().map(val_bytes).sum::<usize>()
        }
        Val::Point { .. } => 32,
        Val::Date(_) | Val::Time(_) | Val::DateTime(_) | Val::Duration(_) => 24,
    }
}

// ── Value encoding (exec::Val → PackStream) ───────────────────────────────────

/// Encode a runtime [`Val`] as a Bolt [`PsValue`]. `Node`/`Relationship` are
/// resolved against the engine (labels, type, properties); element-id fields are
/// emitted only for Bolt ≥ 5 (`version.0 >= 5`), matching the drivers' decoders.
fn encode_val(engine: &Engine, version: (u8, u8), v: &Val) -> Result<PsValue> {
    Ok(match v {
        Val::Null => PsValue::Null,
        Val::Bool(b) => PsValue::Bool(*b),
        Val::Int(i) => PsValue::Int(*i),
        Val::Float(f) => PsValue::Float(*f),
        Val::Str(s) => PsValue::String(s.clone()),
        Val::List(xs) => PsValue::List(
            xs.iter()
                .map(|x| encode_val(engine, version, x))
                .collect::<Result<_>>()?,
        ),
        // Bolt has no native vector type; a stored embedding returns as a list of floats.
        Val::Vector(xs) => PsValue::List(xs.iter().map(|f| PsValue::Float(*f as f64)).collect()),
        Val::Map(m) => PsValue::Map(encode_pairs(engine, version, m)?),
        Val::Node(id) => {
            let (labels, props) = engine.node_record(*id)?;
            let mut fields = vec![
                PsValue::Int(*id as i64),
                PsValue::List(labels.into_iter().map(PsValue::String).collect()),
                PsValue::Map(encode_pairs(engine, version, &props)?),
            ];
            if version.0 >= 5 {
                fields.push(PsValue::String(id.to_string())); // element_id
            }
            PsValue::Struct {
                tag: TAG_NODE,
                fields,
            }
        }
        Val::Rel {
            id,
            start,
            end,
            reltype,
        } => {
            let (type_name, props) = engine.rel_record(*id, *reltype)?;
            let mut fields = vec![
                PsValue::Int(*id as i64),
                PsValue::Int(*start as i64),
                PsValue::Int(*end as i64),
                PsValue::String(type_name),
                PsValue::Map(encode_pairs(engine, version, &props)?),
            ];
            if version.0 >= 5 {
                fields.push(PsValue::String(id.to_string())); // element_id
                fields.push(PsValue::String(start.to_string())); // start element_id
                fields.push(PsValue::String(end.to_string())); // end element_id
            }
            PsValue::Struct {
                tag: TAG_RELATIONSHIP,
                fields,
            }
        }
        // Bolt `Path` (0x50): a list of the distinct nodes (start first), a list of
        // the distinct relationships as `UnboundRelationship` (0x72) structures, and
        // an `indices` list weaving them into walk order. Each segment contributes a
        // pair `[rel_index, node_index]`: `rel_index` is 1-based into the rel list,
        // signed by traversal direction (+ when the edge's stored src→dst matches the
        // walk, − when reversed); `node_index` is 0-based into the node list of the
        // node reached. The walk starts at node 0. Validated against the Neo4j driver
        // decoder semantics, not FalkorDB's RESP path.
        Val::Path { nodes, rels } => {
            // Distinct nodes, preserving first-appearance order (start at index 0).
            let mut node_ids: Vec<u64> = Vec::new();
            let mut node_pos: HashMap<u64, usize> = HashMap::new();
            for &nid in nodes {
                node_pos.entry(nid).or_insert_with(|| {
                    node_ids.push(nid);
                    node_ids.len() - 1
                });
            }
            // Distinct relationships by id (a bidirectional walk may reuse an edge).
            let mut rel_pos: HashMap<u64, usize> = HashMap::new();
            let mut rel_order: Vec<&Val> = Vec::new();
            for r in rels {
                if let Val::Rel { id, .. } = r {
                    rel_pos.entry(*id).or_insert_with(|| {
                        rel_order.push(r);
                        rel_order.len() - 1
                    });
                }
            }
            let node_structs = node_ids
                .iter()
                .map(|id| encode_val(engine, version, &Val::Node(*id)))
                .collect::<Result<Vec<_>>>()?;
            let rel_structs = rel_order
                .iter()
                .map(|r| encode_unbound_rel(engine, version, r))
                .collect::<Result<Vec<_>>>()?;
            let mut indices = Vec::with_capacity(rels.len() * 2);
            for (k, r) in rels.iter().enumerate() {
                if let Val::Rel { id, start, end, .. } = r {
                    let from = nodes[k];
                    let to = nodes[k + 1];
                    let idx = (rel_pos[id] + 1) as i64;
                    let signed = if *start == from && *end == to {
                        idx
                    } else {
                        -idx
                    };
                    indices.push(PsValue::Int(signed));
                    indices.push(PsValue::Int(node_pos[&to] as i64));
                }
            }
            PsValue::Struct {
                tag: TAG_PATH,
                fields: vec![
                    PsValue::List(node_structs),
                    PsValue::List(rel_structs),
                    PsValue::List(indices),
                ],
            }
        }
        // Bolt `Point2D` (0x58): `[srid, x, y]`. FalkorDB always uses WGS-84, so
        // srid = 4326, x = longitude, y = latitude (resultset_replybolt.c). Not
        // yet byte-validated against a live Neo4j driver in this env (none
        // available); follows the published Point2D spec.
        Val::Point {
            latitude,
            longitude,
        } => PsValue::Struct {
            tag: TAG_POINT2D,
            fields: vec![
                PsValue::Int(4326),
                PsValue::Float(*longitude),
                PsValue::Float(*latitude),
            ],
        },
        // Bolt v2 temporals. Whole-second storage ⇒ `nanoseconds` is always 0.
        // Not byte-validated against a live driver here (same caveat as Path /
        // Point2D); follows the published Neo4j PackStream spec.
        Val::Date(secs) => PsValue::Struct {
            tag: TAG_DATE,
            fields: vec![PsValue::Int(secs.div_euclid(86_400))],
        },
        Val::Time(secs) => PsValue::Struct {
            tag: TAG_LOCAL_TIME,
            fields: vec![PsValue::Int(secs.rem_euclid(86_400) * 1_000_000_000)],
        },
        Val::DateTime(secs) => PsValue::Struct {
            tag: TAG_LOCAL_DATETIME,
            fields: vec![PsValue::Int(*secs), PsValue::Int(0)],
        },
        Val::Duration(secs) => {
            let d = crate::temporal::duration_components(*secs);
            PsValue::Struct {
                tag: TAG_DURATION,
                fields: vec![
                    PsValue::Int(d.years * 12 + d.months),
                    PsValue::Int(d.days),
                    PsValue::Int(d.hours * 3_600 + d.minutes * 60 + d.seconds),
                    PsValue::Int(0),
                ],
            }
        }
    })
}

/// Encode a `Val::Rel` as a Bolt `UnboundRelationship` (0x72): `[id, type, props]`
/// (plus the element-id field for Bolt ≥ 5). Endpoints are omitted — a path's node
/// list supplies them.
fn encode_unbound_rel(engine: &Engine, version: (u8, u8), r: &Val) -> Result<PsValue> {
    let Val::Rel { id, reltype, .. } = r else {
        bail!("encode_unbound_rel expects a relationship value");
    };
    let (type_name, props) = engine.rel_record(*id, *reltype)?;
    let mut fields = vec![
        PsValue::Int(*id as i64),
        PsValue::String(type_name),
        PsValue::Map(encode_pairs(engine, version, &props)?),
    ];
    if version.0 >= 5 {
        fields.push(PsValue::String(id.to_string())); // element_id
    }
    Ok(PsValue::Struct {
        tag: TAG_UNBOUND_REL,
        fields,
    })
}

fn encode_pairs(
    engine: &Engine,
    version: (u8, u8),
    pairs: &[(String, Val)],
) -> Result<Vec<(String, PsValue)>> {
    pairs
        .iter()
        .map(|(k, v)| Ok((k.clone(), encode_val(engine, version, v)?)))
        .collect()
}

/// Map Bolt `RUN` parameters (a PackStream map) into executor [`Val`]s.
fn params_to_vals(params: &PsValue) -> std::result::Result<HashMap<String, Val>, Failure> {
    let mut out = HashMap::new();
    if let PsValue::Map(entries) = params {
        for (k, v) in entries {
            let val = ps_to_val(v).map_err(|e| Failure::new(CODE_REQUEST, e.to_string()))?;
            out.insert(k.clone(), val);
        }
    }
    Ok(out)
}

fn ps_to_val(v: &PsValue) -> Result<Val> {
    Ok(match v {
        PsValue::Null => Val::Null,
        PsValue::Bool(b) => Val::Bool(*b),
        PsValue::Int(i) => Val::Int(*i),
        PsValue::Float(f) => Val::Float(*f),
        PsValue::String(s) => Val::Str(s.clone()),
        PsValue::Bytes(b) => Val::List(b.iter().map(|x| Val::Int(*x as i64)).collect()),
        PsValue::List(xs) => Val::List(xs.iter().map(ps_to_val).collect::<Result<_>>()?),
        PsValue::Map(m) => Val::Map(
            m.iter()
                .map(|(k, x)| Ok((k.clone(), ps_to_val(x)?)))
                .collect::<Result<_>>()?,
        ),
        PsValue::Struct { .. } => bail!("a structure cannot be used as a query parameter"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::hash_password;
    use crate::testgen;
    use tokio::net::TcpStream;

    /// Write a temp ACL granting `reporting`/`pw` read on `people`, return its path.
    fn write_acl(root: &Path) -> std::path::PathBuf {
        let path = root.join("acl.json");
        let json = serde_json::json!({
            "users": {
                "reporting": {
                    "passwordArgon2id": hash_password("pw").unwrap(),
                    "grants": { "people": ["read"] }
                }
            }
        });
        std::fs::write(&path, json.to_string()).unwrap();
        path
    }

    /// Patch one top-level field in every generation manifest of `graph` under
    /// `root`. Safe for fields outside the data-file inventory (e.g. `aclBlake3`),
    /// which `content_hash` excludes, so `open_all` still validates afterwards.
    fn patch_manifest(root: &Path, graph: &str, key: &str, value: serde_json::Value) {
        for entry in std::fs::read_dir(root.join(graph)).unwrap() {
            let man = entry.unwrap().path().join("MANIFEST.json");
            if man.exists() {
                let mut v: serde_json::Value =
                    serde_json::from_str(&std::fs::read_to_string(&man).unwrap()).unwrap();
                v[key] = value.clone();
                std::fs::write(&man, serde_json::to_string_pretty(&v).unwrap()).unwrap();
            }
        }
    }

    #[test]
    fn in_flight_gauge_tracks_without_diagnostics() {
        // The idle gate depends on `queries_in_flight` being maintained even when
        // load-test diagnostics are OFF (the default).
        let d = crate::diag::Diagnostics::new(false);
        assert_eq!(d.in_flight(), 0);
        d.on_query_start();
        d.on_query_start();
        assert_eq!(d.in_flight(), 2);
        d.on_query_ok(1.0);
        assert_eq!(d.in_flight(), 1);
        d.on_query_err(&anyhow::anyhow!("boom"));
        assert_eq!(d.in_flight(), 0);
    }

    #[test]
    fn acl_stamp_matches_serves_and_mismatch_refuses() {
        let (root, _g, _) = testgen::write_basic("aclstamp_match");
        let acl_path = write_acl(&root);
        let live = graph_format::integrity::hash_file(&acl_path).unwrap();

        // Stamped with the live digest → serves.
        patch_manifest(&root, "people", "aclBlake3", serde_json::json!(live));
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs.set_manifest_policy(Some(acl_path.clone()), false);
        assert!(graphs.verify_manifest_policy().is_ok());

        // Stamped with a stale digest → refuses to serve.
        patch_manifest(&root, "people", "aclBlake3", serde_json::json!("deadbeef"));
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs.set_manifest_policy(Some(acl_path), false);
        assert!(graphs.verify_manifest_policy().is_err());
    }

    #[test]
    fn acl_digest_acceptable_matches_served_stamp() {
        let (root, _g, _) = testgen::write_basic("acl_digest_ok");
        let acl_path = write_acl(&root);
        let live = graph_format::integrity::hash_file(&acl_path).unwrap();
        patch_manifest(&root, "people", "aclBlake3", serde_json::json!(live));

        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs.set_manifest_policy(Some(acl_path), false);

        assert!(
            graphs.acl_digest_acceptable(&live),
            "matching digest accepted"
        );
        assert!(
            !graphs.acl_digest_acceptable("deadbeef"),
            "a digest other than the stamp is refused"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unstamped_generation_accepts_any_acl_digest() {
        // A legacy/plaintext image with no aclBlake3 stamp imposes no hot-reload
        // constraint, so the ACL keeps hot-reloading as before.
        let (root, _g, _) = testgen::write_basic("acl_digest_unstamped");
        let acl_path = write_acl(&root);
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs.set_manifest_policy(Some(acl_path), false);
        assert!(graphs.acl_digest_acceptable("anything"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn hot_reload_refuses_tamper_then_adopts_matching_rebuild() {
        let (root, _g, _) = testgen::write_basic("acl_hotreload_e2e");
        let acl_path = write_acl(&root);
        let live = graph_format::integrity::hash_file(&acl_path).unwrap();
        patch_manifest(&root, "people", "aclBlake3", serde_json::json!(live));

        let acl = AclHandle::load(&acl_path).unwrap();
        assert!(acl.snapshot().can_read("reporting", "people"));
        assert!(!acl.snapshot().can_read("reporting", "secret"));

        // ── Tamper: edit acl.json at runtime to self-grant a new read. The served
        // generation still carries the *old* stamp, so the enforced reload refuses it.
        let tampered = serde_json::json!({
            "users": { "reporting": { "passwordArgon2id": hash_password("pw").unwrap(),
                "grants": { "people": ["read"], "secret": ["read"] } } }
        });
        std::fs::write(&acl_path, tampered.to_string()).unwrap();

        let graphs = {
            let mut g = Graphs::open_all(&root, None).unwrap();
            g.set_manifest_policy(Some(acl_path.clone()), false);
            Arc::new(g)
        };
        let g1 = graphs.clone();
        assert!(!acl.reload_checked(move |d| g1.acl_digest_acceptable(d)));
        assert!(
            !acl.snapshot().can_read("reporting", "secret"),
            "tampered grant must not take effect"
        );

        // ── Legitimate change: a generation rebuilt against the new acl.json carries a
        // matching stamp. Re-open to model the swapped-in generation; the enforced
        // reload now accepts the same file.
        let newdigest = graph_format::integrity::hash_file(&acl_path).unwrap();
        patch_manifest(&root, "people", "aclBlake3", serde_json::json!(newdigest));
        let graphs2 = {
            let mut g = Graphs::open_all(&root, None).unwrap();
            g.set_manifest_policy(Some(acl_path), false);
            Arc::new(g)
        };
        let g2 = graphs2.clone();
        assert!(acl.reload_checked(move |d| g2.acl_digest_acceptable(d)));
        assert!(
            acl.snapshot().can_read("reporting", "secret"),
            "ACL matching the new stamp is adopted"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unstamped_generation_ignored_unless_required() {
        let (root, _g, _) = testgen::write_basic("aclstamp_absent");
        let acl_path = write_acl(&root);

        // Legacy image with no aclBlake3 serves when not required.
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs.set_manifest_policy(Some(acl_path.clone()), false);
        assert!(graphs.verify_manifest_policy().is_ok());

        // requireAclStamp turns the absence into a refusal.
        let mut graphs = Graphs::open_all(&root, None).unwrap();
        graphs.set_manifest_policy(Some(acl_path), true);
        assert!(graphs.verify_manifest_policy().is_err());
    }

    /// Re-seal every generation manifest of `graph` with a MAC under `key`, as an
    /// encrypted build would. (The fixture data stays plaintext; the MAC path is
    /// independent of whether blocks are encrypted.)
    fn reseal_manifest_with_mac(root: &Path, graph: &str, key: &[u8]) {
        for entry in std::fs::read_dir(root.join(graph)).unwrap() {
            let man = entry.unwrap().path().join("MANIFEST.json");
            if man.exists() {
                let mut m: graph_format::manifest::Manifest =
                    serde_json::from_str(&std::fs::read_to_string(&man).unwrap()).unwrap();
                m.seal_mac(key).unwrap();
                std::fs::write(&man, m.to_json().unwrap()).unwrap();
            }
        }
    }

    #[test]
    fn manifest_mac_catches_tamper_through_open() {
        let (root, _g, _) = testgen::write_basic("mac_e2e");
        let key: &[u8] = b"operator master key";
        reseal_manifest_with_mac(&root, "people", key);

        // Sealed manifest opens cleanly with the key (MAC verifies; data plaintext).
        assert!(Generation::open_with_key(&root, "people", Some(key)).is_ok());

        // Tamper a MAC-covered field the content-hash does NOT cover (nodeCount)
        // without resealing: the MAC check refuses before anything else. A plaintext
        // image (no MAC) would happily serve this forged count.
        patch_manifest(&root, "people", "nodeCount", serde_json::json!(999_999));
        let err = Generation::open_with_key(&root, "people", Some(key))
            .err()
            .expect("tampered manifest must fail to open");
        assert!(
            format!("{err:#}").contains("MAC"),
            "expected a MAC error, got: {err:#}"
        );
    }

    #[test]
    fn keyed_server_refuses_macless_generation_unconditionally() {
        let (root, _g, _) = testgen::write_basic("require_mac");
        let acl_path = write_acl(&root);
        // The plaintext fixture carries no MAC; a server configured with a master
        // key must refuse it (the MAC-strip downgrade guard). This is deliberately
        // not a policy flag — there is no legitimate keyed-but-unauthenticated
        // deployment, so there is nothing to configure.
        let mut graphs = Graphs::open_all(&root, Some(b"master")).unwrap();
        graphs.set_manifest_policy(Some(acl_path), false);
        assert!(graphs.verify_manifest_policy().is_err());
    }

    /// Stand up a ConnCtx over the shared fixture graph + a temp ACL.
    /// Per-connection security limits for the test ConnCtx builders. Defaults are
    /// generous/on so existing tests are unaffected; the connection-security tests
    /// pass tight values to exercise a specific gate.
    #[derive(Clone)]
    struct TestLimits {
        max_message_bytes: usize,
        max_pre_auth_bytes: usize,
        login_timeout_ms: u64,
        idle_timeout_ms: u64,
        max_pre_auth_connections: usize,
        max_per_ip: usize,
        load_test_diagnostics: bool,
    }

    impl Default for TestLimits {
        fn default() -> Self {
            Self {
                max_message_bytes: 64 * 1024 * 1024,
                max_pre_auth_bytes: 64 * 1024,
                login_timeout_ms: 0, // off by default so unrelated tests never time out
                idle_timeout_ms: 0,
                max_pre_auth_connections: 4_096,
                max_per_ip: 0,                // unlimited by default
                load_test_diagnostics: false, // diagnostics off by default, as in prod
            }
        }
    }

    fn build_ctx(tag: &str) -> (std::path::PathBuf, Arc<ConnCtx>) {
        build_ctx_limited(tag, TestLimits::default())
    }

    fn build_ctx_limited(tag: &str, limits: TestLimits) -> (std::path::PathBuf, Arc<ConnCtx>) {
        let (root, _graph, _) = testgen::write_basic(tag);
        let acl_path = write_acl(&root);
        let acl = Arc::new(AclHandle::load(&acl_path).unwrap());
        let graphs = Arc::new(Graphs::open_all(&root, None).unwrap());
        let cache = Arc::new(BlockCache::new(1 << 20));
        let vector_cache = Arc::new(VectorIndexCache::new(1 << 20));
        for gen in graphs.current_generations() {
            for vi in gen.vamana_indexes() {
                vector_cache.pin(gen.uuid(), vi.ord, vi.pq.clone());
            }
        }
        let result_cache = Arc::new(ResultCache::new(1 << 20));
        let ctx = Arc::new(ConnCtx {
            acl,
            graphs,
            cache,
            vector_cache,
            result_cache,
            max_rows: 100_000,
            timeout_ms: 0,
            max_intermediate: 1_000_000,
            max_scan: 500_000_000,
            intermediate_budget: Arc::new(GlobalIntermediateBudget::new(8_000_000)),
            max_shortest_path_explore: 0,
            fanout_pool: None,
            beam_width: 64,
            bind_addr: "127.0.0.1:7687".to_string(),
            default_graph: None,
            use_selection: RwLock::new(HashMap::new()),
            memgraph_users: RwLock::new(HashSet::new()),
            max_message_bytes: limits.max_message_bytes,
            max_pre_auth_bytes: limits.max_pre_auth_bytes,
            login_timeout_ms: limits.login_timeout_ms,
            idle_timeout_ms: limits.idle_timeout_ms,
            pre_auth_limit: Arc::new(Semaphore::new(semaphore_permits(
                limits.max_pre_auth_connections,
            ))),
            per_ip: Arc::new(Mutex::new(HashMap::new())),
            max_per_ip: limits.max_per_ip,
            diag: Arc::new(crate::diag::Diagnostics::new(limits.load_test_diagnostics)),
            conn_limit: Arc::new(Semaphore::new(semaphore_permits(16_384))),
            max_connections: 16_384,
            max_pre_auth_connections: limits.max_pre_auth_connections,
        });
        (root, ctx)
    }

    /// Recursively copy a (small fixture) directory tree.
    fn copy_dir(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).unwrap();
        for entry in std::fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let to = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &to);
            } else {
                std::fs::copy(entry.path(), &to).unwrap();
            }
        }
    }

    /// A ConnCtx serving two graphs (`people` + a copy `places`), with `reporting`
    /// granted read on both — exercises the ambiguous (multi-graph) selection path.
    fn build_multi_ctx(tag: &str) -> Arc<ConnCtx> {
        let (root, _graph, _) = testgen::write_basic(tag);
        let places = root.join("places");
        copy_dir(&root.join("people"), &places);
        // The manifest records its own graph name (and open_all rejects a mismatch);
        // the data-file content hash excludes MANIFEST.json, so renaming the copied
        // graph to "places" only requires patching that one field.
        for entry in std::fs::read_dir(&places).unwrap() {
            let gen_dir = entry.unwrap().path();
            let man = gen_dir.join("MANIFEST.json");
            if man.exists() {
                let mut v: serde_json::Value =
                    serde_json::from_str(&std::fs::read_to_string(&man).unwrap()).unwrap();
                v["graph"] = serde_json::json!("places");
                std::fs::write(&man, serde_json::to_string_pretty(&v).unwrap()).unwrap();
            }
        }
        let acl_path = root.join("acl.json");
        let json = serde_json::json!({
            "users": { "reporting": {
                "passwordArgon2id": hash_password("pw").unwrap(),
                "grants": { "people": ["read"], "places": ["read"] }
            }}
        });
        std::fs::write(&acl_path, json.to_string()).unwrap();
        let acl = Arc::new(AclHandle::load(&acl_path).unwrap());
        let graphs = Arc::new(Graphs::open_all(&root, None).unwrap());
        Arc::new(ConnCtx {
            acl,
            graphs,
            cache: Arc::new(BlockCache::new(1 << 20)),
            vector_cache: Arc::new(VectorIndexCache::new(1 << 20)),
            result_cache: Arc::new(ResultCache::new(1 << 20)),
            max_rows: 100_000,
            timeout_ms: 0,
            max_intermediate: 1_000_000,
            max_scan: 500_000_000,
            intermediate_budget: Arc::new(GlobalIntermediateBudget::new(8_000_000)),
            max_shortest_path_explore: 0,
            fanout_pool: None,
            beam_width: 64,
            bind_addr: "127.0.0.1:7687".to_string(),
            // A default is configured but must NOT be silently served for queries.
            default_graph: Some("people".to_string()),
            use_selection: RwLock::new(HashMap::new()),
            memgraph_users: RwLock::new(HashSet::new()),
            max_message_bytes: 64 * 1024 * 1024,
            max_pre_auth_bytes: 64 * 1024,
            login_timeout_ms: 0,
            idle_timeout_ms: 0,
            pre_auth_limit: Arc::new(Semaphore::new(semaphore_permits(4_096))),
            per_ip: Arc::new(Mutex::new(HashMap::new())),
            max_per_ip: 0,
            diag: Arc::new(crate::diag::Diagnostics::new(false)),
            conn_limit: Arc::new(Semaphore::new(semaphore_permits(16_384))),
            max_connections: 16_384,
            max_pre_auth_connections: 4_096,
        })
    }

    #[test]
    fn unknown_db_name_errors_and_lists_the_served_graphs() {
        let (_root, ctx) = build_ctx("select_unknown_db");
        let extra = PsValue::Map(vec![("db".into(), PsValue::str("eu-ai-act"))]);
        let err = ctx.select_graph(&extra, "reporting", None).unwrap_err();
        assert_eq!(err.code, CODE_NOT_FOUND);
        assert!(
            err.message.contains("'eu-ai-act' is not served"),
            "{}",
            err.message
        );
        // The real name is offered so a typo is self-correcting.
        assert!(err.message.contains("people"), "{}", err.message);
    }

    #[test]
    fn ambiguous_session_errors_instead_of_silently_serving_the_default() {
        let ctx = build_multi_ctx("select_ambiguous");
        // No `db` field, and `reporting` can read two graphs: must error, not fall
        // back to `default_graph` ("people").
        let empty = PsValue::Map(vec![]);
        let err = ctx.select_graph(&empty, "reporting", None).unwrap_err();
        assert_eq!(err.code, CODE_NOT_FOUND);
        assert!(err.message.contains("no graph selected"), "{}", err.message);
        assert!(
            err.message.contains("people") && err.message.contains("places"),
            "{}",
            err.message
        );
        // An empty (not just absent) db string is treated the same.
        let blank = PsValue::Map(vec![("db".into(), PsValue::str(""))]);
        assert!(ctx.select_graph(&blank, "reporting", None).is_err());
        // Naming an exact, served graph still works.
        let named = PsValue::Map(vec![("db".into(), PsValue::str("places"))]);
        assert_eq!(
            ctx.select_graph(&named, "reporting", None).ok(),
            Some("places".to_string())
        );
    }

    #[tokio::test]
    async fn begin_validates_the_graph_and_remembers_it_for_the_transaction() {
        let ctx = build_multi_ctx("begin_validate");
        let mut sess = Session {
            user: Some("reporting".into()),
            failed: false,
            pending: None,
            tx_graph: None,
            version: (5, 4),
        };
        // BEGIN naming an unserved graph fails at BEGIN, before any RUN.
        let bad =
            message::Request::Begin(PsValue::Map(vec![("db".into(), PsValue::str("eu-ai-act"))]));
        let err = handle_request(&mut sess, &ctx, bad).await.unwrap_err();
        assert_eq!(err.code, CODE_NOT_FOUND);
        assert!(sess.tx_graph.is_none());
        // BEGIN with no db does NOT bind the transaction — the graph is deferred to
        // the RUN (clients like Memgraph Lab put `db` on the RUN, not the BEGIN). The
        // BEGIN itself succeeds; an unnamed graph only errors if the RUN omits it too.
        let unbound = message::Request::Begin(PsValue::Map(vec![]));
        assert!(handle_request(&mut sess, &ctx, unbound).await.is_ok());
        assert!(sess.tx_graph.is_none());
        // BEGIN naming a served graph is remembered for the transaction's RUNs.
        let good =
            message::Request::Begin(PsValue::Map(vec![("db".into(), PsValue::str("places"))]));
        assert!(handle_request(&mut sess, &ctx, good).await.is_ok());
        assert_eq!(sess.tx_graph.as_deref(), Some("places"));
        // COMMIT ends the transaction and clears the held graph.
        assert!(handle_request(&mut sess, &ctx, message::Request::Commit)
            .await
            .is_ok());
        assert!(sess.tx_graph.is_none());
    }

    #[tokio::test]
    async fn warm_cache_pulls_blocks_into_a_cold_cache() {
        let (root, ctx) = build_ctx("warm_cache_warms");
        // A fresh block cache holds nothing until something reads.
        assert_eq!(ctx.cache.bytes(), 0, "cache should start cold");
        warm_cache("MATCH (n:Person) RETURN n.name", &ctx).await;
        assert!(
            ctx.cache.bytes() > 0,
            "warming query should have faulted blocks into the cache"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn warm_cache_is_a_noop_when_unset() {
        let (root, ctx) = build_ctx("warm_cache_noop");
        // Empty and whitespace-only both mean "disabled" — neither touches the cache.
        warm_cache("", &ctx).await;
        warm_cache("   \n  ", &ctx).await;
        assert_eq!(
            ctx.cache.bytes(),
            0,
            "an unset warming query must not read anything"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn warm_cache_survives_a_bad_query() {
        let (root, ctx) = build_ctx("warm_cache_bad");
        // A parse error must not panic or abort — it logs and leaves the cache cold.
        warm_cache("THIS IS NOT CYPHER", &ctx).await;
        assert_eq!(ctx.cache.bytes(), 0, "a bad warming query warms nothing");
        // A syntactically valid query against a label that does not exist executes
        // (and warms whatever it scans) without taking the server down.
        warm_cache("MATCH (n:NoSuchLabel) RETURN n", &ctx).await;
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Spawn the connection handler over a fresh loopback listener, returning the
    /// bound address so a client can connect.
    async fn spawn_server(ctx: Arc<ConnCtx>) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (sock, _) = listener.accept().await.unwrap();
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(sock, ctx).await;
                });
            }
        });
        addr
    }

    /// A minimal async Bolt client for the tests.
    struct Client {
        stream: TcpStream,
        buf: Vec<u8>,
    }

    impl Client {
        async fn connect(addr: std::net::SocketAddr) -> Self {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            // Handshake: preamble + offer 5.4 then 4.4.
            let mut hs = Vec::new();
            hs.extend_from_slice(&handshake::PREAMBLE);
            hs.extend_from_slice(&[0, 0, 4, 5]);
            hs.extend_from_slice(&[0, 0, 4, 4]);
            hs.extend_from_slice(&[0, 0, 0, 0]);
            hs.extend_from_slice(&[0, 0, 0, 0]);
            stream.write_all(&hs).await.unwrap();
            let mut reply = [0u8; 4];
            stream.read_exact(&mut reply).await.unwrap();
            assert_eq!(reply, [0, 0, 4, 5], "should negotiate Bolt 5.4");
            Self {
                stream,
                buf: Vec::new(),
            }
        }

        async fn send(&mut self, msg: PsValue) {
            self.stream
                .write_all(&message::to_wire(&msg))
                .await
                .unwrap();
        }

        /// Read the next response message as a decoded struct `(tag, fields)`.
        async fn recv(&mut self) -> (u8, Vec<PsValue>) {
            loop {
                if let Some((body, consumed)) = chunk::decode_message(&self.buf).unwrap() {
                    self.buf.drain(..consumed);
                    match crate::bolt::packstream::from_slice(&body).unwrap() {
                        PsValue::Struct { tag, fields } => return (tag, fields),
                        other => panic!("expected a struct, got {other:?}"),
                    }
                }
                let mut tmp = [0u8; 4096];
                let n = self.stream.read(&mut tmp).await.unwrap();
                assert!(n > 0, "server closed unexpectedly");
                self.buf.extend_from_slice(&tmp[..n]);
            }
        }

        fn hello() -> PsValue {
            PsValue::Struct {
                tag: message::tag::HELLO,
                fields: vec![PsValue::Map(vec![(
                    "user_agent".into(),
                    PsValue::str("slater-test/1.0"),
                )])],
            }
        }

        fn logon(user: &str, pw: &str) -> PsValue {
            PsValue::Struct {
                tag: message::tag::LOGON,
                fields: vec![PsValue::Map(vec![
                    ("scheme".into(), PsValue::str("basic")),
                    ("principal".into(), PsValue::str(user)),
                    ("credentials".into(), PsValue::str(pw)),
                ])],
            }
        }

        /// A 4.4-style HELLO carrying auth inline (no separate LOGON).
        fn hello_with_auth(user: &str, pw: &str) -> PsValue {
            PsValue::Struct {
                tag: message::tag::HELLO,
                fields: vec![PsValue::Map(vec![
                    ("user_agent".into(), PsValue::str("slater-test/1.0")),
                    ("scheme".into(), PsValue::str("basic")),
                    ("principal".into(), PsValue::str(user)),
                    ("credentials".into(), PsValue::str(pw)),
                ])],
            }
        }

        fn run(query: &str) -> PsValue {
            PsValue::Struct {
                tag: message::tag::RUN,
                fields: vec![
                    PsValue::str(query),
                    PsValue::Map(vec![]),
                    PsValue::Map(vec![("db".into(), PsValue::str("people"))]),
                ],
            }
        }

        fn pull_all() -> PsValue {
            PsValue::Struct {
                tag: message::tag::PULL,
                fields: vec![PsValue::Map(vec![("n".into(), PsValue::Int(-1))])],
            }
        }
    }

    #[tokio::test]
    async fn full_handshake_logon_run_pull_returns_records() {
        let (root, ctx) = build_ctx("server_e2e");
        let addr = spawn_server(ctx).await;
        let mut c = Client::connect(addr).await;

        c.send(Client::hello()).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::logon("reporting", "pw")).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);

        c.send(Client::run(
            "MATCH (n:Person) RETURN n.name AS name ORDER BY name",
        ))
        .await;
        let (tag, fields) = c.recv().await;
        assert_eq!(tag, message::tag::SUCCESS);
        // SUCCESS {fields: ["name"]}.
        assert_eq!(
            fields[0].get("fields"),
            Some(&PsValue::List(vec![PsValue::str("name")]))
        );

        c.send(Client::pull_all()).await;
        let mut names = Vec::new();
        loop {
            let (tag, fields) = c.recv().await;
            if tag == message::tag::RECORD {
                if let PsValue::List(vals) = &fields[0] {
                    names.push(vals[0].as_str().unwrap().to_string());
                }
            } else {
                assert_eq!(tag, message::tag::SUCCESS);
                assert_eq!(fields[0].get("has_more"), Some(&PsValue::Bool(false)));
                break;
            }
        }
        assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn show_storage_info_includes_per_pool_cache_metrics() {
        let (root, ctx) = build_ctx("server_storage_info");
        let addr = spawn_server(ctx).await;
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::logon("reporting", "pw")).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);

        // Touch the block cache first so its counters are non-trivial.
        c.send(Client::run("MATCH (n:Person) RETURN n.name AS name"))
            .await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::pull_all()).await;
        while c.recv().await.0 != message::tag::SUCCESS {}

        c.send(Client::run("SHOW STORAGE INFO")).await;
        let (tag, fields) = c.recv().await;
        assert_eq!(tag, message::tag::SUCCESS);
        assert_eq!(
            fields[0].get("fields"),
            Some(&PsValue::List(vec![
                PsValue::str("storage info"),
                PsValue::str("value")
            ]))
        );

        c.send(Client::pull_all()).await;
        let mut kv: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
        loop {
            let (tag, fields) = c.recv().await;
            if tag == message::tag::RECORD {
                if let PsValue::List(vals) = &fields[0] {
                    if let (Some(key), PsValue::Int(v)) = (vals[0].as_str(), &vals[1]) {
                        kv.insert(key.to_string(), *v);
                    }
                }
            } else {
                assert_eq!(tag, message::tag::SUCCESS);
                break;
            }
        }

        // The manifest stats are still there…
        assert!(kv.contains_key("vertex_count"), "manifest rows must remain");
        // …and every pool now reports its full metric set.
        for pool in ["block", "vector", "result"] {
            for metric in ["bytes", "entries", "hits", "misses", "evictions"] {
                let key = format!("{pool}_cache_{metric}");
                assert!(kv.contains_key(&key), "SHOW STORAGE INFO missing `{key}`");
            }
        }
        // The MATCH above went through the block cache, so it recorded an access.
        assert!(
            kv["block_cache_hits"] + kv["block_cache_misses"] >= 1,
            "block cache should show at least one access after the MATCH"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn diagnostics_disabled_by_default_errors() {
        // With `loadTestDiagnostics` off (the default), the statement must fail
        // rather than leak a surface — and no diagnostics state is maintained.
        let (root, ctx) = build_ctx("server_diag_off");
        let addr = spawn_server(ctx).await;
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::logon("reporting", "pw")).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);

        c.send(Client::run("CALL slater.diagnostics()")).await;
        let (tag, fields) = c.recv().await;
        assert_eq!(tag, message::tag::FAILURE, "disabled diagnostics must fail");
        // The message should point the operator at the flag.
        let msg = fields[0]
            .get("message")
            .and_then(PsValue::as_str)
            .unwrap_or_default();
        assert!(
            msg.contains("loadTestDiagnostics"),
            "failure should name the flag, got: {msg}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn diagnostics_enabled_returns_health_metrics() {
        // Stand up a server with diagnostics enabled, drive one query so the
        // query counters are non-trivial, then read the snapshot.
        let (root, ctx) = build_ctx_limited(
            "server_diag_on",
            TestLimits {
                load_test_diagnostics: true,
                ..Default::default()
            },
        );
        let addr = spawn_server(ctx).await;
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::logon("reporting", "pw")).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);

        // A successful query so `queries_ok_total` and a latency sample are recorded.
        c.send(Client::run("MATCH (n:Person) RETURN n.name AS name"))
            .await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::pull_all()).await;
        while c.recv().await.0 != message::tag::SUCCESS {}

        c.send(Client::run("CALL slater.diagnostics()")).await;
        let (tag, fields) = c.recv().await;
        assert_eq!(
            tag,
            message::tag::SUCCESS,
            "enabled diagnostics must succeed"
        );
        assert_eq!(
            fields[0].get("fields"),
            Some(&PsValue::List(vec![
                PsValue::str("metric"),
                PsValue::str("value")
            ]))
        );

        c.send(Client::pull_all()).await;
        let mut metrics: std::collections::HashMap<String, PsValue> =
            std::collections::HashMap::new();
        loop {
            let (tag, fields) = c.recv().await;
            if tag == message::tag::RECORD {
                if let PsValue::List(vals) = &fields[0] {
                    if let Some(key) = vals[0].as_str() {
                        metrics.insert(key.to_string(), vals[1].clone());
                    }
                }
            } else {
                assert_eq!(tag, message::tag::SUCCESS);
                break;
            }
        }

        // Headline rows are present: process RSS, the cgroup limit (may be -1 when
        // unconstrained), and the echoed connection cap.
        assert!(
            metrics.contains_key("rss_bytes"),
            "snapshot missing rss_bytes"
        );
        assert!(
            metrics.contains_key("cgroup_mem_limit_bytes"),
            "snapshot missing cgroup_mem_limit_bytes"
        );
        assert_eq!(
            metrics.get("conn_limit"),
            Some(&PsValue::Int(16_384)),
            "echoed connection cap should match the configured maxConnections"
        );
        // The MATCH was counted as a completed query.
        match metrics.get("queries_ok_total") {
            Some(PsValue::Int(n)) => assert!(*n >= 1, "expected >=1 ok query, got {n}"),
            other => panic!("queries_ok_total missing or not an int: {other:?}"),
        }
        // A latency percentile was recorded (>= 0; -1 would mean no samples).
        match metrics.get("latency_p50_ms") {
            Some(PsValue::Float(v)) => assert!(*v >= 0.0, "expected a latency sample, got {v}"),
            other => panic!("latency_p50_ms missing or not a float: {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn parse_use_statement_recognises_the_database_switch_forms() {
        assert_eq!(
            parse_use_statement("USE eu_ai_act").as_deref(),
            Some("eu_ai_act")
        );
        assert_eq!(
            parse_use_statement("use database eu_ai_act;").as_deref(),
            Some("eu_ai_act")
        );
        assert_eq!(
            parse_use_statement("  USE   `eu_ai_act` ").as_deref(),
            Some("eu_ai_act")
        );
        assert_eq!(
            parse_use_statement("USE DATABASE \"eu_ai_act\"").as_deref(),
            Some("eu_ai_act")
        );
        // Not a bare USE / malformed → ignored (falls through to the query path).
        assert_eq!(parse_use_statement("MATCH (n) RETURN n"), None);
        assert_eq!(parse_use_statement("USE"), None);
        assert_eq!(parse_use_statement("USE a b"), None);
        assert_eq!(parse_use_statement("USEFUL eu_ai_act"), None);
    }

    // ── GQL PR 5 — optional `GQL` / `CYPHER` dialect prefix ───────────────────

    #[test]
    fn strip_dialect_prefix_removes_the_selector_only() {
        // The keyword (any case), with or without a numeric version token, is dropped.
        assert_eq!(
            strip_dialect_prefix("GQL MATCH (n) RETURN n"),
            "MATCH (n) RETURN n"
        );
        assert_eq!(
            strip_dialect_prefix("cypher MATCH (n) RETURN n"),
            "MATCH (n) RETURN n"
        );
        assert_eq!(
            strip_dialect_prefix("CYPHER 25 MATCH (n) RETURN n"),
            "MATCH (n) RETURN n"
        );
        assert_eq!(
            strip_dialect_prefix("  cypher 5.0\n MATCH (n) RETURN n"),
            "MATCH (n) RETURN n"
        );

        // A bare query is returned untouched, and an identifier merely sharing the
        // prefix (`cypher_score`) is never mistaken for a selector.
        assert_eq!(
            strip_dialect_prefix("MATCH (n) RETURN n"),
            "MATCH (n) RETURN n"
        );
        assert_eq!(
            strip_dialect_prefix("RETURN cypher_score"),
            "RETURN cypher_score"
        );
        // `CYPHER` immediately followed by a query keyword (no version) keeps the
        // keyword — only the selector is consumed.
        assert_eq!(strip_dialect_prefix("GQL RETURN 1"), "RETURN 1");
    }

    #[test]
    fn dialect_prefix_parses_to_the_same_ast_as_the_bare_query() {
        // GQL / CYPHER prefixes are pure dialect selectors: after stripping, the
        // remainder parses to the identical AST as the unprefixed query.
        let bare = parser::parse("MATCH (n) RETURN n").unwrap();
        for q in ["GQL MATCH (n) RETURN n", "CYPHER MATCH (n) RETURN n"] {
            let stripped = strip_dialect_prefix(q);
            assert_eq!(parser::parse(stripped).unwrap(), bare, "for {q:?}");
        }
        // A bare query is byte-for-byte unaffected by the strip.
        assert_eq!(
            strip_dialect_prefix("MATCH (n) RETURN n"),
            "MATCH (n) RETURN n"
        );
    }

    // ── GQL PR 5 — additive GQLSTATUS metadata ────────────────────────────────

    #[test]
    fn gqlstatus_completion_distinguishes_empty_from_nonempty() {
        // A non-empty result completes `00000`; an empty one is GQL `02000` (no data).
        let nonempty = gqlstatus_completion(3);
        let status = |pairs: &[(String, PsValue)], k: &str| {
            pairs
                .iter()
                .find(|(kk, _)| kk == k)
                .and_then(|(_, v)| v.as_str().map(str::to_string))
        };
        assert_eq!(status(&nonempty, "gql_status").as_deref(), Some("00000"));
        let empty = gqlstatus_completion(0);
        assert_eq!(status(&empty, "gql_status").as_deref(), Some("02000"));
    }

    #[test]
    fn failure_message_keeps_legacy_keys_and_adds_gqlstatus() {
        // Syntax / access-mode errors map to GQL class 42; everything else to 50000.
        assert_eq!(Failure::new(CODE_SYNTAX, "x".into()).gqlstatus().0, "42000");
        assert_eq!(
            Failure::new(CODE_ACCESS_MODE, "x".into()).gqlstatus().0,
            "42000"
        );
        assert_eq!(
            Failure::new(CODE_EXECUTION, "x".into()).gqlstatus().0,
            "50000"
        );

        // The wire FAILURE keeps `code`/`message` and gains the GQLSTATUS pair.
        let PsValue::Struct { tag, fields } = Failure::new(CODE_SYNTAX, "bad".into()).to_message()
        else {
            panic!("expected a Struct");
        };
        assert_eq!(tag, message::tag::FAILURE);
        let PsValue::Map(m) = &fields[0] else {
            panic!("expected a Map");
        };
        for key in ["code", "message", "gql_status", "status_description"] {
            assert!(
                m.iter().any(|(k, _)| k == key),
                "missing metadata key {key}"
            );
        }
    }

    #[tokio::test]
    async fn begin_without_db_defers_to_the_run_graph() {
        // Memgraph Lab's wire shape: an explicit transaction whose BEGIN names no
        // graph, with `db` riding on the RUN inside it. A multi-graph user must still
        // succeed — the unbound BEGIN defers, and the RUN resolves the graph.
        let ctx = build_multi_ctx("begin_defer_run");
        let addr = spawn_server(ctx).await;
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::logon("reporting", "pw")).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);

        // BEGIN with empty metadata (no `db`).
        c.send(PsValue::Struct {
            tag: message::tag::BEGIN,
            fields: vec![PsValue::Map(vec![])],
        })
        .await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);

        // RUN carrying the graph in its `db` field.
        c.send(PsValue::Struct {
            tag: message::tag::RUN,
            fields: vec![
                PsValue::str("MATCH (n:Person) RETURN n.name AS name ORDER BY name"),
                PsValue::Map(vec![]),
                PsValue::Map(vec![("db".into(), PsValue::str("places"))]),
            ],
        })
        .await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);

        c.send(Client::pull_all()).await;
        let mut names = Vec::new();
        loop {
            let (tag, fields) = c.recv().await;
            if tag == message::tag::RECORD {
                if let PsValue::List(vals) = &fields[0] {
                    names.push(vals[0].as_str().unwrap().to_string());
                }
            } else {
                assert_eq!(tag, message::tag::SUCCESS);
                break;
            }
        }
        assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
    }

    #[tokio::test]
    async fn returns_node_and_relationship_structures() {
        let (root, ctx) = build_ctx("server_structs");
        let addr = spawn_server(ctx).await;
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        c.recv().await;
        c.send(Client::logon("reporting", "pw")).await;
        c.recv().await;

        c.send(Client::run(
            "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, r",
        ))
        .await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::pull_all()).await;

        let (tag, fields) = c.recv().await;
        assert_eq!(tag, message::tag::RECORD);
        let row = match &fields[0] {
            PsValue::List(vals) => vals,
            other => panic!("expected a record list, got {other:?}"),
        };
        // Node a: struct 'N' with [id, labels, props, element_id] (Bolt 5).
        match &row[0] {
            PsValue::Struct { tag, fields } => {
                assert_eq!(*tag, TAG_NODE);
                assert_eq!(fields.len(), 4);
                assert_eq!(
                    fields[1],
                    PsValue::List(vec![PsValue::str("Person")]),
                    "labels"
                );
                assert_eq!(fields[2].get("name"), Some(&PsValue::str("Alice")));
            }
            other => panic!("expected a Node struct, got {other:?}"),
        }
        // Relationship r: struct 'R' with [id, start, end, type, props, +3 element ids].
        match &row[1] {
            PsValue::Struct { tag, fields } => {
                assert_eq!(*tag, TAG_RELATIONSHIP);
                assert_eq!(fields.len(), 8);
                assert_eq!(fields[1], PsValue::Int(0), "start node id (Alice)");
                assert_eq!(fields[2], PsValue::Int(1), "end node id (Bob)");
                assert_eq!(fields[3], PsValue::str("KNOWS"), "type");
                assert_eq!(fields[4].get("since"), Some(&PsValue::Int(2020)));
            }
            other => panic!("expected a Relationship struct, got {other:?}"),
        }
        // Drain the trailing SUCCESS.
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn returns_path_structure() {
        let (root, ctx) = build_ctx("server_path");
        let addr = spawn_server(ctx).await;
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        c.recv().await;
        c.send(Client::logon("reporting", "pw")).await;
        c.recv().await;

        c.send(Client::run(
            "MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) RETURN p",
        ))
        .await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::pull_all()).await;

        let (tag, fields) = c.recv().await;
        assert_eq!(tag, message::tag::RECORD);
        let row = match &fields[0] {
            PsValue::List(vals) => vals,
            other => panic!("expected a record list, got {other:?}"),
        };
        // Path p: struct 'P' (0x50) with [nodes, rels, indices].
        let (path_tag, path_fields) = match &row[0] {
            PsValue::Struct { tag, fields } => (*tag, fields),
            other => panic!("expected a Path struct, got {other:?}"),
        };
        assert_eq!(path_tag, TAG_PATH);
        assert_eq!(path_fields.len(), 3);

        // Field 0: the two nodes (Alice at index 0, Bob at index 1).
        let nodes = match &path_fields[0] {
            PsValue::List(ns) => ns,
            other => panic!("expected a node list, got {other:?}"),
        };
        assert_eq!(nodes.len(), 2);
        for (n, name) in nodes.iter().zip(["Alice", "Bob"]) {
            match n {
                PsValue::Struct { tag, fields } => {
                    assert_eq!(*tag, TAG_NODE);
                    assert_eq!(fields[2].get("name"), Some(&PsValue::str(name)));
                }
                other => panic!("expected a Node struct, got {other:?}"),
            }
        }

        // Field 1: one UnboundRelationship (0x72) — [id, type, props, element_id],
        // no endpoint ids (the node list supplies them).
        let rels = match &path_fields[1] {
            PsValue::List(rs) => rs,
            other => panic!("expected a rel list, got {other:?}"),
        };
        assert_eq!(rels.len(), 1);
        match &rels[0] {
            PsValue::Struct { tag, fields } => {
                assert_eq!(*tag, TAG_UNBOUND_REL);
                assert_eq!(fields.len(), 4); // Bolt 5: id, type, props, element_id
                assert_eq!(fields[0], PsValue::Int(0), "edge id");
                assert_eq!(fields[1], PsValue::str("KNOWS"), "type");
                assert_eq!(fields[2].get("since"), Some(&PsValue::Int(2020)));
            }
            other => panic!("expected an UnboundRelationship struct, got {other:?}"),
        }

        // Field 2: indices weaving the single forward segment — rel 1 (+, forward)
        // into node index 1 (Bob).
        assert_eq!(
            path_fields[2],
            PsValue::List(vec![PsValue::Int(1), PsValue::Int(1)]),
            "path indices"
        );

        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn returns_point2d_structure() {
        let (root, ctx) = build_ctx("server_point");
        let addr = spawn_server(ctx).await;
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        c.recv().await;
        c.send(Client::logon("reporting", "pw")).await;
        c.recv().await;

        c.send(Client::run(
            "RETURN point({latitude: 32.5, longitude: 34.25}) AS p",
        ))
        .await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::pull_all()).await;

        let (tag, fields) = c.recv().await;
        assert_eq!(tag, message::tag::RECORD);
        let row = match &fields[0] {
            PsValue::List(vals) => vals,
            other => panic!("expected a record list, got {other:?}"),
        };
        // Point2D struct (0x58): [srid::Int=4326, x::Float=longitude, y::Float=latitude].
        match &row[0] {
            PsValue::Struct { tag, fields } => {
                assert_eq!(*tag, TAG_POINT2D);
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0], PsValue::Int(4326), "srid");
                assert_eq!(fields[1], PsValue::Float(34.25), "x = longitude");
                assert_eq!(fields[2], PsValue::Float(32.5), "y = latitude");
            }
            other => panic!("expected a Point2D struct, got {other:?}"),
        }

        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        let _ = std::fs::remove_dir_all(&root);
    }

    // Bolt v2 temporal structs (Date 0x44, LocalTime 0x74, LocalDateTime 0x64,
    // Duration 0x45). FalkorDB never wires temporals over Bolt, so this validates
    // the published Neo4j PackStream encoding an official driver would decode.
    #[tokio::test]
    async fn returns_temporal_structures() {
        let (root, ctx) = build_ctx("server_temporal");
        let addr = spawn_server(ctx).await;
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        c.recv().await;
        c.send(Client::logon("reporting", "pw")).await;
        c.recv().await;

        c.send(Client::run(
            "RETURN date('1970-01-02') AS d, localtime({hour:1, minute:0, second:1}) AS t, \
                    localdatetime('1970-01-01T00:00:05') AS dt, \
                    duration({months:2, days:3, hours:1, minutes:0, seconds:4}) AS u",
        ))
        .await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::pull_all()).await;

        let (tag, fields) = c.recv().await;
        assert_eq!(tag, message::tag::RECORD);
        let row = match &fields[0] {
            PsValue::List(vals) => vals,
            other => panic!("expected a record list, got {other:?}"),
        };

        // Date 0x44: [days] — 1970-01-02 is 1 day past the epoch.
        match &row[0] {
            PsValue::Struct { tag, fields } => {
                assert_eq!(*tag, TAG_DATE);
                assert_eq!(fields, &vec![PsValue::Int(1)]);
            }
            other => panic!("expected a Date struct, got {other:?}"),
        }
        // LocalTime 0x74: [nanoOfDay] — 01:00:01 = 3601 s.
        match &row[1] {
            PsValue::Struct { tag, fields } => {
                assert_eq!(*tag, TAG_LOCAL_TIME);
                assert_eq!(fields, &vec![PsValue::Int(3601 * 1_000_000_000)]);
            }
            other => panic!("expected a LocalTime struct, got {other:?}"),
        }
        // LocalDateTime 0x64: [seconds, nanoseconds] — epoch + 5 s.
        match &row[2] {
            PsValue::Struct { tag, fields } => {
                assert_eq!(*tag, TAG_LOCAL_DATETIME);
                assert_eq!(fields, &vec![PsValue::Int(5), PsValue::Int(0)]);
            }
            other => panic!("expected a LocalDateTime struct, got {other:?}"),
        }
        // Duration 0x45: [months, days, seconds, nanoseconds] — 2mo 3d 1h4s.
        match &row[3] {
            PsValue::Struct { tag, fields } => {
                assert_eq!(*tag, TAG_DURATION);
                assert_eq!(
                    fields,
                    &vec![
                        PsValue::Int(2),
                        PsValue::Int(3),
                        PsValue::Int(3604),
                        PsValue::Int(0),
                    ]
                );
            }
            other => panic!("expected a Duration struct, got {other:?}"),
        }

        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn hello_embedded_auth_authenticates_the_4_4_fallback() {
        let (root, ctx) = build_ctx("server_hello_auth");
        let addr = spawn_server(ctx).await;
        let mut c = Client::connect(addr).await;

        // 4.4-style: credentials ride in HELLO, no separate LOGON.
        c.send(Client::hello_with_auth("reporting", "pw")).await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);

        // The connection is authenticated, so RUN/PULL proceed.
        c.send(Client::run("MATCH (n:Person) RETURN count(*) AS c"))
            .await;
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        c.send(Client::pull_all()).await;
        let (tag, fields) = c.recv().await;
        assert_eq!(tag, message::tag::RECORD);
        assert_eq!(fields[0], PsValue::List(vec![PsValue::Int(3)]));
        assert_eq!(c.recv().await.0, message::tag::SUCCESS);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn bad_password_fails_and_run_before_logon_fails() {
        let (root, ctx) = build_ctx("server_auth");
        let addr = spawn_server(ctx).await;

        // Wrong password → FAILURE on LOGON.
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        c.recv().await;
        c.send(Client::logon("reporting", "wrong")).await;
        let (tag, fields) = c.recv().await;
        assert_eq!(tag, message::tag::FAILURE);
        assert_eq!(
            fields[0].get("code").and_then(PsValue::as_str),
            Some(CODE_UNAUTHORIZED)
        );

        // RUN before LOGON → FAILURE (unauthenticated).
        let mut c = Client::connect(addr).await;
        c.send(Client::hello()).await;
        c.recv().await;
        c.send(Client::run("MATCH (n) RETURN n")).await;
        assert_eq!(c.recv().await.0, message::tag::FAILURE);
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

    // ── Generation guard (M8) ──────────────────────────────────────────────

    /// Recursively copy `src` to `dst` (files + subdirectories).
    fn copy_dir_all(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).unwrap();
        for entry in std::fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let from = entry.path();
            let to = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir_all(&from, &to);
            } else {
                std::fs::copy(&from, &to).unwrap();
            }
        }
    }

    /// Copy `graph`'s live generation directory to a fresh UUID, optionally
    /// truncating `corrupt` (a path relative to the generation dir) in the copy to
    /// simulate a half-rsynced generation, then republish `current` to name the new
    /// UUID. Returns the new UUID. A generation's identity is its `current` pointer
    /// (the recorded MANIFEST `build_uuid` is not re-checked on open), so a
    /// byte-identical copy republished under a new UUID validates and opens cleanly.
    fn publish_copy_as_new_generation(
        root: &Path,
        graph: &str,
        corrupt: Option<&str>,
    ) -> uuid::Uuid {
        let graph_dir = root.join(graph);
        let old = std::fs::read_to_string(graph_dir.join("current")).unwrap();
        let new_uuid = uuid::Uuid::from_u128(0x5_1a7e_0000_0000_0000_0000_0000_00ff);
        let src = graph_dir.join(old.trim());
        let dst = graph_dir.join(new_uuid.to_string());
        copy_dir_all(&src, &dst);
        if let Some(rel) = corrupt {
            let victim = dst.join(rel);
            let mut bytes = std::fs::read(&victim).unwrap();
            bytes.truncate(bytes.len().saturating_sub(16));
            std::fs::write(&victim, bytes).unwrap();
        }
        std::fs::write(
            graph_dir.join("current"),
            format!("{}\n", new_uuid.hyphenated()),
        )
        .unwrap();
        new_uuid
    }

    #[test]
    fn swap_refuses_a_truncated_new_generation() {
        let (root, _g, old) = testgen::write_basic("guard_swap_refuse");
        let graphs = Graphs::open_all(&root, None).unwrap();
        let vc = VectorIndexCache::new(1 << 20);

        // A half-copied (truncated) new generation is published under `current`.
        publish_copy_as_new_generation(&root, "people", Some("node_props.blk"));
        let err = graphs.swap_if_changed("people", &vc).err().unwrap();
        assert!(
            err.chain().any(|e| e.to_string().contains("integrity")),
            "unexpected error: {err:#}"
        );
        // The live generation is untouched — the corrupt copy never took over.
        assert_eq!(graphs.get("people").unwrap().uuid().0, old);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn swap_applies_a_valid_new_generation_while_in_flight_reads_the_old() {
        let (root, _g, old) = testgen::write_basic("guard_swap_apply");
        let graphs = Graphs::open_all(&root, None).unwrap();
        let vc = VectorIndexCache::new(1 << 20);

        // An in-flight query's snapshot, taken before the swap.
        let in_flight = graphs.get("people").unwrap();

        let new = publish_copy_as_new_generation(&root, "people", None);
        let swapped = graphs.swap_if_changed("people", &vc).unwrap();
        assert_eq!(swapped.map(|g| g.0), Some(new));

        // New queries see the new generation; the in-flight handle still reads old.
        assert_eq!(graphs.get("people").unwrap().uuid().0, new);
        assert_eq!(in_flight.uuid().0, old);

        // A second swap with no further change on disk is a clean no-op.
        assert!(graphs.swap_if_changed("people", &vc).unwrap().is_none());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn exit_strategy_guard_sweep_signals_shutdown_on_change() {
        let (root, _g, _) = testgen::write_basic("guard_exit_sweep");
        let graphs = Graphs::open_all(&root, None).unwrap();
        let vc = VectorIndexCache::new(1 << 20);

        // No change yet → keep serving.
        assert!(matches!(
            guard_sweep(&graphs, &vc, ReloadStrategy::Exit, None),
            SweepAction::Continue
        ));

        // A changed `current` → shutdown signal naming the graph. Exit does not even
        // open the new generation — the orchestrator restart re-opens it cleanly.
        publish_copy_as_new_generation(&root, "people", None);
        match guard_sweep(&graphs, &vc, ReloadStrategy::Exit, None) {
            SweepAction::Shutdown(name) => assert_eq!(name, "people"),
            SweepAction::Continue => panic!("expected a shutdown signal on a changed current"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn swap_strategy_guard_sweep_swaps_in_place() {
        let (root, _g, old) = testgen::write_basic("guard_swap_sweep");
        let graphs = Graphs::open_all(&root, None).unwrap();
        let vc = VectorIndexCache::new(1 << 20);

        let new = publish_copy_as_new_generation(&root, "people", None);
        assert!(matches!(
            guard_sweep(&graphs, &vc, ReloadStrategy::Swap, None),
            SweepAction::Continue
        ));
        assert_ne!(new, old);
        assert_eq!(graphs.get("people").unwrap().uuid().0, new);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn swap_moves_pinned_pq_from_the_old_generation_to_the_new() {
        let f = testgen::VamanaFixture {
            n: 64,
            dim: 8,
            r: 16,
            alpha: 1.2,
            pq_subspaces: 4,
            pq_bits: 6,
            vector_block_size: 1024,
        };
        let (root, _g, _) = testgen::write_vamana("guard_swap_pq", &f);
        let graphs = Graphs::open_all(&root, None).unwrap();
        let vc = VectorIndexCache::new(1 << 20);

        // Pin the live generation's resident PQ, as `serve` does at startup.
        let old = graphs.get("docs").unwrap();
        for vi in old.vamana_indexes() {
            vc.pin(old.uuid(), vi.ord, vi.pq.clone());
        }
        assert!(vc.resident_pq(old.uuid(), 0).is_some());

        let new = publish_copy_as_new_generation(&root, "docs", None);
        graphs.swap_if_changed("docs", &vc).unwrap();

        // The new generation's PQ is now pinned and the old generation's released —
        // so the pool's resident set tracks the live generation (D32).
        assert!(
            vc.resident_pq(GenId(new), 0).is_some(),
            "new generation PQ should be pinned"
        );
        assert!(
            vc.resident_pq(old.uuid(), 0).is_none(),
            "old generation PQ should be unpinned after swap"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn exit_strategy_guard_task_signals_shutdown_over_oneshot() {
        let (root, _g, _) = testgen::write_basic("guard_exit_task");
        let graphs = Arc::new(Graphs::open_all(&root, None).unwrap());
        let vc = Arc::new(VectorIndexCache::new(1 << 20));
        let (tx, rx) = tokio::sync::oneshot::channel();
        // A tight poll interval so the test does not wait the production default.
        spawn_generation_guard(
            graphs.clone(),
            vc,
            ReloadStrategy::Exit,
            Duration::from_millis(20),
            tx,
            None,
        );

        publish_copy_as_new_generation(&root, "people", None);
        let reason = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("guard should fire within the timeout")
            .expect("the shutdown sender should not be dropped");
        assert_eq!(reason, "people");
        let _ = std::fs::remove_dir_all(&root);
    }

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
}
