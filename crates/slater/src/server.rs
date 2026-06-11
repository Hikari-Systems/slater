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
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use graph_format::ids::Generation as GenId;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn, Level};

use crate::acl::AclHandle;
use crate::bolt::chunk;
use crate::bolt::handshake;
use crate::bolt::message;
use crate::bolt::packstream::PsValue;
use crate::cache::{BlockCache, ResultCache, ResultKey, VectorIndexCache};
use crate::config::{AppConfig, ReloadStrategy, TlsConfig};
use crate::exec::{Engine, QueryResult, Val};
use crate::generation::Generation;
use crate::introspect;
use crate::parser;

/// PackStream structure tags for the graph types (Bolt `Node`/`Relationship`).
const TAG_NODE: u8 = 0x4E;
const TAG_RELATIONSHIP: u8 = 0x52;

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
    data_dir: PathBuf,
    master_key: Option<Vec<u8>>,
    graphs: HashMap<String, RwLock<Arc<Generation>>>,
}

impl Graphs {
    /// Discover and open every graph under `data_dir`, deriving each generation's
    /// block cipher from `master_key` (required iff a generation is encrypted).
    pub fn open_all(data_dir: &Path, master_key: Option<&[u8]>) -> Result<Self> {
        let mut graphs = HashMap::new();
        let entries = std::fs::read_dir(data_dir)
            .with_context(|| format!("read data directory {}", data_dir.display()))?;
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            // A graph directory is one with a published `current` pointer; skip
            // anything else (scratch dirs, half-written `.tmp-*` generations).
            if !entry.path().join("current").exists() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let gen = Generation::open_with_key(data_dir, &name, master_key)
                .with_context(|| format!("open graph {name}"))?;
            graphs.insert(name, RwLock::new(Arc::new(gen)));
        }
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            master_key: master_key.map(<[u8]>::to_vec),
            graphs,
        })
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
        let on_disk = Generation::current_uuid(&self.data_dir, name)?;
        if on_disk == live.uuid().0 {
            return Ok(None);
        }

        // Open + validate the new generation. A half-rsynced/truncated copy fails
        // its content-hash check here and the caller keeps the old one serving.
        let new_gen = Arc::new(
            Generation::open_with_key(&self.data_dir, name, self.master_key.as_deref())
                .with_context(|| {
                    format!("open swapped-in generation {on_disk} of graph '{name}'")
                })?,
        );

        // Pin the new generation's resident PQ *before* publishing it, then swap,
        // then unpin the old — so the pool never under-counts the resident set.
        for vi in new_gen.vamana_indexes() {
            vector_cache.pin(new_gen.uuid(), vi.ord, vi.pq.clone());
        }
        *slot.write().unwrap() = new_gen.clone();
        for vi in live.vamana_indexes() {
            vector_cache.unpin(live.uuid(), vi.ord);
        }
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
) -> SweepAction {
    for name in graphs.names() {
        let Some(live) = graphs.get(&name) else {
            continue;
        };
        let on_disk = match Generation::current_uuid(&graphs.data_dir, &name) {
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
                    info!(graph = %name, generation = %new, "swapped to a new generation (reloadStrategy=swap)")
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
            // The sweep does blocking IO (re-hash + open on a swap), so it runs on
            // the blocking pool, off the async reactor.
            let action = match tokio::task::spawn_blocking(move || {
                guard_sweep(&graphs, &vector_cache, strategy)
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
        self.memgraph_users.write().unwrap().insert(user.to_string());
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
    if rest.first().is_some_and(|w| w.eq_ignore_ascii_case("database")) {
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

    fn to_message(&self) -> PsValue {
        message::failure(self.code, &self.message)
    }
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
}

impl<S: AsyncRead + AsyncWrite + Unpin> Framed<S> {
    fn new(stream: S) -> Self {
        Self {
            stream,
            buf: Vec::with_capacity(8192),
        }
    }

    /// Read the next complete (de-chunked) message body, or `None` at a clean EOF.
    async fn read_message(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            if let Some((body, consumed)) = chunk::decode_message(&self.buf)? {
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
    let master_key = cfg
        .encryption
        .load_key()
        .context("load at-rest encryption key")?;
    let graphs = Arc::new(Graphs::open_all(
        Path::new(&cfg.data_dir),
        master_key.as_deref(),
    )?);
    if graphs.is_empty() {
        warn!(data_dir = %cfg.data_dir, "no graphs found to serve");
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

    let ctx = Arc::new(ConnCtx {
        acl,
        graphs,
        cache,
        vector_cache,
        result_cache,
        max_rows: cfg.query.max_rows as usize,
        timeout_ms: cfg.query.timeout_ms,
        beam_width: cfg.vector_query.beam_width as usize,
        bind_addr: format!("{}:{}", cfg.server.bind, cfg.server.port),
        default_graph: Some(cfg.default_graph.clone()).filter(|g| !g.is_empty()),
        use_selection: RwLock::new(HashMap::new()),
        memgraph_users: RwLock::new(HashSet::new()),
    });

    info!(
        bind = %cfg.server.bind,
        port = cfg.server.port,
        tls = tls.is_some(),
        graphs = ctx.graphs.len(),
        poll_ms = cfg.generation_poll_ms,
        reload_strategy = %cfg.reload_strategy,
        "slater Bolt listener ready"
    );

    // The in-flight generation guard: poll each graph's `current` and, on a change,
    // either swap the validated new generation in place or signal a clean exit.
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<String>();
    spawn_generation_guard(
        ctx.graphs.clone(),
        ctx.vector_cache.clone(),
        reload_strategy,
        cfg.generation_poll_interval(),
        shutdown_tx,
    );

    // The cache-maintenance task: reclaim idle entries past the configured TTL.
    // Disabled (not spawned) when `cache.cacheTtlMs` is 0 — caches then evict on
    // budget pressure alone, as before.
    if let Some(ttl) = cfg.cache.cache_ttl() {
        info!(ttl_ms = cfg.cache.cache_ttl_ms, "cache idle-TTL eviction enabled");
        spawn_cache_maintenance(
            ctx.cache.clone(),
            ctx.vector_cache.clone(),
            ctx.result_cache.clone(),
            ttl,
        );
    }

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (sock, peer) = accepted.context("accept")?;
                sock.set_nodelay(true).ok();
                let ctx = ctx.clone();
                let tls = tls.clone();
                tokio::spawn(async move {
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
    let mut framed = Framed::new(stream);

    // Handshake: 20 bytes (preamble + four proposals), reply with the agreed
    // 4-byte version, or four zero bytes if we share none (the client disconnects).
    let mut hello = [0u8; 20];
    framed
        .stream
        .read_exact(&mut hello)
        .await
        .context("read handshake")?;
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

    while let Some(body) = framed.read_message().await? {
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
    // Pick up any out-of-band ACL edit before authenticating.
    ctx.acl.poll();
    if ctx.acl.snapshot().verify(principal, credentials) {
        sess.user = Some(principal.to_string());
        Ok(())
    } else {
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
            warn!(db = ?extra.get("db"), selected = ?sticky, query = %query, "WIRE-DIAG: RUN");
            // `USE <graph>` / `USE DATABASE <graph>` selects the user's graph in-band
            // (clients that never send the Bolt `db` field, e.g. Memgraph Lab, rely on
            // this). Validate the target and remember it per-user for later db-less
            // statements; answer with an empty result like a Memgraph database switch.
            if let Some(target) = parse_use_statement(&query) {
                if ctx.graphs.get(&target).is_none() || !ctx.acl.snapshot().can_read(&user, &target) {
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
                warn!(graph = %target, "WIRE-DIAG: USE selected graph");
                ctx.set_selection(&user, &target);
                sess.pending = Some(Pending { rows: vec![], sent: 0 });
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
            if let Some((columns, rows)) =
                ctx.introspect(&user, &extra, &query, sticky.as_deref(), ctx.is_memgraph(&user))?
            {
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
            msgs.push(message::success(vec![(
                "has_more".into(),
                PsValue::Bool(has_more),
            )]));
            if !has_more {
                sess.pending = None;
            }
            Ok(msgs)
        }

        Request::Discard(_) => {
            sess.pending = None;
            Ok(vec![message::success(vec![(
                "has_more".into(),
                PsValue::Bool(false),
            )])])
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
    let max_rows = ctx.max_rows;
    let timeout_ms = ctx.timeout_ms;
    let beam_width = ctx.beam_width;
    let graph_name = gen.graph().to_string();
    // Gate all per-query instrumentation on the debug level being active: when it
    // is off, we take no timestamps and no cache snapshots, and build no
    // QueryTiming — the hot path is exactly what it was before instrumentation.
    let instrument = tracing::enabled!(Level::DEBUG);

    let join =
        tokio::task::spawn_blocking(move || -> Result<(EncodedRows, Option<QueryTiming>)> {
            // Per-query instrumentation (only when `instrument`): wall-clock split into
            // execute vs encode, and the block-cache hit/miss/eviction delta this query
            // caused (the counters are process-wide, so we snapshot before/after). A
            // result-cache hit skips execution, which shows up as exec_ms ≈ 0.
            let t_start = instrument.then(Instant::now);
            let blk_before = instrument.then(|| cache.metrics());

            // Result-cache lookup, then execute-and-cache on a miss.
            let (result, result_cache_hit) = match result_cache.get(&key) {
                Some(r) => (r, true),
                None => {
                    let mut engine = Engine::new(gen.as_ref(), cache.as_ref())
                        .with_vector_cache(vector_cache.as_ref(), beam_width)
                        .with_params(params)
                        .with_max_rows(max_rows);
                    if timeout_ms > 0 {
                        engine = engine
                            .with_deadline(Instant::now() + Duration::from_millis(timeout_ms));
                    }
                    let r = Arc::new(engine.run(&ast)?);
                    let bytes = estimate_result_bytes(&r);
                    result_cache.insert(key.clone(), r.clone(), bytes);
                    (r, false)
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
            if let Some(t) = timing {
                debug!(
                    graph = %graph_name,
                    rows = t.rows,
                    result_cache = if t.result_cache_hit { "hit" } else { "miss" },
                    exec_ms = format_args!("{:.1}", t.exec_ms),
                    encode_ms = format_args!("{:.1}", t.encode_ms),
                    total_ms = format_args!("{:.1}", t.total_ms),
                    blk_hits = t.blk_hits,
                    blk_misses = t.blk_misses,
                    blk_evicted = t.blk_evictions,
                    query = %log_query(query),
                    "query executed"
                );
            }
            Ok(out)
        }
        Ok(Err(e)) => Err(Failure::from_query_error(&e)),
        Err(e) => Err(Failure::new(
            CODE_EXECUTION,
            format!("query task failed: {e}"),
        )),
    }
}

/// Column names plus the PackStream-encoded rows — the shape `run_query`'s
/// blocking task produces.
type EncodedRows = (Vec<String>, Vec<Vec<PsValue>>);

/// Per-query timing + cache-delta, captured inside the blocking task and logged
/// once the result returns (see [`run_query`]).
struct QueryTiming {
    result_cache_hit: bool,
    exec_ms: f64,
    encode_ms: f64,
    total_ms: f64,
    rows: usize,
    blk_hits: u64,
    blk_misses: u64,
    blk_evictions: u64,
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

    /// Stand up a ConnCtx over the shared fixture graph + a temp ACL.
    fn build_ctx(tag: &str) -> (std::path::PathBuf, Arc<ConnCtx>) {
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
            beam_width: 64,
            bind_addr: "127.0.0.1:7687".to_string(),
            default_graph: None,
            use_selection: RwLock::new(HashMap::new()),
        memgraph_users: RwLock::new(HashSet::new()),
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
            beam_width: 64,
            bind_addr: "127.0.0.1:7687".to_string(),
            // A default is configured but must NOT be silently served for queries.
            default_graph: Some("people".to_string()),
            use_selection: RwLock::new(HashMap::new()),
        memgraph_users: RwLock::new(HashSet::new()),
        })
    }

    #[test]
    fn unknown_db_name_errors_and_lists_the_served_graphs() {
        let (_root, ctx) = build_ctx("select_unknown_db");
        let extra = PsValue::Map(vec![("db".into(), PsValue::str("eu-ai-act"))]);
        let err = ctx.select_graph(&extra, "reporting", None).unwrap_err();
        assert_eq!(err.code, CODE_NOT_FOUND);
        assert!(err.message.contains("'eu-ai-act' is not served"), "{}", err.message);
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
        assert!(err.message.contains("people") && err.message.contains("places"), "{}", err.message);
        // An empty (not just absent) db string is treated the same.
        let blank = PsValue::Map(vec![("db".into(), PsValue::str(""))]);
        assert!(ctx.select_graph(&blank, "reporting", None).is_err());
        // Naming an exact, served graph still works.
        let named = PsValue::Map(vec![("db".into(), PsValue::str("places"))]);
        assert_eq!(ctx.select_graph(&named, "reporting", None).ok(), Some("places".to_string()));
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
        let bad = message::Request::Begin(PsValue::Map(vec![(
            "db".into(),
            PsValue::str("eu-ai-act"),
        )]));
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
        let good = message::Request::Begin(PsValue::Map(vec![(
            "db".into(),
            PsValue::str("places"),
        )]));
        assert!(handle_request(&mut sess, &ctx, good).await.is_ok());
        assert_eq!(sess.tx_graph.as_deref(), Some("places"));
        // COMMIT ends the transaction and clears the held graph.
        assert!(handle_request(&mut sess, &ctx, message::Request::Commit)
            .await
            .is_ok());
        assert!(sess.tx_graph.is_none());
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
        c.send(Client::run("MATCH (n:Person) RETURN n.name AS name")).await;
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

    #[test]
    fn parse_use_statement_recognises_the_database_switch_forms() {
        assert_eq!(parse_use_statement("USE eu_ai_act").as_deref(), Some("eu_ai_act"));
        assert_eq!(parse_use_statement("use database eu_ai_act;").as_deref(), Some("eu_ai_act"));
        assert_eq!(parse_use_statement("  USE   `eu_ai_act` ").as_deref(), Some("eu_ai_act"));
        assert_eq!(parse_use_statement("USE DATABASE \"eu_ai_act\"").as_deref(), Some("eu_ai_act"));
        // Not a bare USE / malformed → ignored (falls through to the query path).
        assert_eq!(parse_use_statement("MATCH (n) RETURN n"), None);
        assert_eq!(parse_use_statement("USE"), None);
        assert_eq!(parse_use_statement("USE a b"), None);
        assert_eq!(parse_use_statement("USEFUL eu_ai_act"), None);
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
            guard_sweep(&graphs, &vc, ReloadStrategy::Exit),
            SweepAction::Continue
        ));

        // A changed `current` → shutdown signal naming the graph. Exit does not even
        // open the new generation — the orchestrator restart re-opens it cleanly.
        publish_copy_as_new_generation(&root, "people", None);
        match guard_sweep(&graphs, &vc, ReloadStrategy::Exit) {
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
            guard_sweep(&graphs, &vc, ReloadStrategy::Swap),
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
        );

        publish_copy_as_new_generation(&root, "people", None);
        let reason = tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .expect("guard should fire within the timeout")
            .expect("the shutdown sender should not be dropped");
        assert_eq!(reason, "people");
        let _ = std::fs::remove_dir_all(&root);
    }
}
