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
//!   properties — stays off the async reactor. **Writes** are no exception (their key
//!   resolution, adjacency materialisation and WAL append+fsync are just as blocking):
//!   they go to the same pool via [`execute_write_off_reactor`], under the
//!   `server.maxConcurrentWrites` cap that keeps a write flood — serialised behind one
//!   writer lock per graph — from swallowing the pool that queries share.
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
use graph_format::ids::Value;
use graph_format::store::fs::FsObjectStore;
use graph_format::store::{join_key, ObjectStore};
use rayon::prelude::*;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{timeout, timeout_at, Instant as TokioInstant};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn, Level};

use crate::acl::{Acl, AclHandle};
use crate::bolt::chunk;
use crate::bolt::handshake;
use crate::bolt::message;
use crate::bolt::packstream::PsValue;
use crate::cache::{BlockCache, ResultCache, ResultKey, VectorIndexCache};
use crate::config::{AppConfig, DeltaConfig, ReloadStrategy, TlsConfig};
use crate::delta_writer::DeltaWriter;
use crate::exec::{Engine, GlobalIntermediateBudget, QueryResult, Val};
use crate::generation::Generation;
use crate::introspect;
use crate::parser;
use crate::read_view::{MergedView, ReadView};
use crate::rwindex::{RwIndexCache, TouchedJournal};
use slater_delta::{DeltaSnapshot, Memtable, OpResolution, WalOp};

mod conn;
mod consolidate;
mod handle;
mod listen;
mod query;
/// PackStream structure tags for the graph types (Bolt `Node`/`Relationship`).
// The `Graphs` registry and `ConnCtx` impls live in these child modules.
mod registry;
mod write;

// Re-export each child module's items at the `server` scope so sibling modules
// (via `use super::*`) and `crate::query` can call them by name.
pub(crate) use consolidate::*;
pub(crate) use handle::*;
pub(crate) use listen::*;
pub use listen::{serve, serve_with_listener};
pub(crate) use query::*;
pub(crate) use write::*;

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
/// A lost `begin_consolidation` single-flight race — another flush/consolidation already
/// holds the exclusive claim. A `TransientError` (not a `ClientError`): the operation is
/// benign and retriable, and Neo4j-aware drivers auto-retry that class. Carried as the
/// `Failure::code` so callers branch on this *typed discriminant*, not on message text —
/// see [`is_already_in_progress`] and [`spawn_auto_consolidation`].
const CODE_CONSOLIDATION_IN_PROGRESS: &str = "Neo.TransientError.General.ConsolidationInProgress";

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
    /// At-rest master key, retained for the whole process so the guard can
    /// re-open a graph. `Zeroizing` wipes it when the registry drops (HIK-139).
    master_key: Option<zeroize::Zeroizing<Vec<u8>>>,
    /// Run the copy-completeness re-hash when opening (and swapping in) a
    /// generation. Default for the filesystem backend; usually off for S3.
    verify_integrity: bool,
    graphs: HashMap<String, RwLock<Arc<Generation>>>,
    /// Live `acl.json` path used to verify per-generation `aclBlake3` stamps at
    /// open/swap time. `None` ⇒ no ACL stamp checking (e.g. unit-test fixtures).
    acl_path: Option<PathBuf>,
    /// Refuse to serve a generation whose manifest carries no ACL stamp.
    require_acl_stamp: bool,
    /// Per-generation range-index block-cache budget (`config.cache.rangeIndexCacheBytes`),
    /// applied when opening a generation here and on every hot-reload swap. `None` (the
    /// non-server openers) leaves range readers uncached.
    range_index_cache_bytes: Option<usize>,
    /// Residency policy for each generation's dense degree column
    /// (`config.cache.degreeColumn`), applied at open and on every hot-reload swap. The
    /// non-server openers default to `Lazy`.
    degree_residency: crate::degree_column::DegreeResidency,
    /// Byte budget for each lazy dense degree column (`config.cache.degreeColumnBytes`), applied
    /// at open and on every hot-reload swap. `None` (the non-server openers) uses
    /// [`DEFAULT_BUDGET_BYTES`](crate::degree_column::DEFAULT_BUDGET_BYTES).
    degree_column_bytes: Option<usize>,
    /// Per-graph writable-layer writers, populated only when the delta layer is
    /// enabled (`config.delta.enabled`). Empty otherwise — the read-only server is
    /// exactly what it was. Each writer is bound to the generation it resolved its
    /// dense ids against (`DeltaWriter::core_uuid`).
    writers: HashMap<String, Arc<DeltaWriter>>,
    /// Serialises generation swaps per graph. Two independent actors publish a new
    /// `current` and then swap the served slot onto it: the background generation
    /// guard ([`guard_sweep`], polling on a timer) and the writer-side stack mutations
    /// (consolidate / flush / compact). Without this lock they interleave — the guard
    /// swaps the op's freshly published generation in first, the op's own swap then
    /// reports "unchanged", and the op skips the post-swap work that retires the delta
    /// it just folded in, orphaning it against the old core.
    ///
    /// Keyed exactly like `graphs` (both are fixed at construction). A **leaf** lock:
    /// only the per-graph slot `RwLock` is taken beneath it, never the reverse.
    swap_locks: HashMap<String, Mutex<()>>,
}

/// What a [`Graphs::gc_orphan_segments`] sweep reclaimed (or newly observed).
#[derive(Debug, Default, Clone)]
pub struct SegmentGcReport {
    /// Segment directories deleted this sweep (aged past the grace).
    pub deleted_segments: Vec<GenId>,
    /// Set manifest files deleted this sweep.
    pub deleted_sets: Vec<GenId>,
    /// Orphans newly observed this sweep — marked, awaiting the grace before deletion.
    pub marked: usize,
}

/// Seconds since the Unix epoch (0 if the clock is before it — never in practice).
fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Publish a flush's set manifest then flip `current`, both via a local tmp-then-rename so a
/// crash never exposes a half-written set or a pointer to one. Mirrors the builder's
/// local-publish barrier (`slater-build::common`): `sets/<uuid>.json` (fsynced) *before*
/// `current` (fsynced). Local-fs only for now (the object-store upload path is a later
/// slice); the segment directory is already durable when this is called.
fn publish_set_and_current(
    data_dir: &Path,
    graph: &str,
    set_uuid: GenId,
    set: &graph_format::setmanifest::SetManifest,
) -> Result<()> {
    let graph_dir = data_dir.join(graph);
    let sets_dir = graph_dir.join("sets");
    std::fs::create_dir_all(&sets_dir).with_context(|| format!("create {}", sets_dir.display()))?;
    let set_path = sets_dir.join(format!("{}.json", set_uuid.0));
    let set_tmp = sets_dir.join(format!(".{}.json.tmp", set_uuid.0));
    std::fs::write(&set_tmp, set.to_bytes()?)
        .with_context(|| format!("write {}", set_tmp.display()))?;
    std::fs::rename(&set_tmp, &set_path)
        .with_context(|| format!("publish {}", set_path.display()))?;
    fsync_dir(&sets_dir)?;

    let current = graph_dir.join("current");
    let current_tmp = graph_dir.join(".current.tmp");
    std::fs::write(&current_tmp, format!("{}\n", set_uuid.0))
        .with_context(|| format!("write {}", current_tmp.display()))?;
    std::fs::rename(&current_tmp, &current)
        .with_context(|| format!("swap {}", current.display()))?;
    fsync_dir(&graph_dir)?;
    Ok(())
}

/// Upload a locally-staged flush to a remote object store: every segment file (with its
/// SHA-256 so S3 validates the body and stores the object checksum), then `SEGMENT.json`,
/// then the set manifest, then the `current` pointer **last** — the copy-completeness
/// barrier (`current` only ever names a fully-uploaded set, which only names fully-uploaded
/// segments). Mirrors the builder's `upload_generation` (`slater-build::common`).
fn upload_flush_to_store(
    store: &dyn ObjectStore,
    graph: &str,
    seg_dir: &Path,
    seg_manifest: &graph_format::segmanifest::SegmentManifest,
    set_uuid: GenId,
    set: &graph_format::setmanifest::SetManifest,
) -> Result<()> {
    let seg_prefix = crate::segstack::segment_prefix(graph, seg_manifest.segment_uuid);
    for fe in &seg_manifest.files {
        let bytes = std::fs::read(seg_dir.join(&fe.name))
            .with_context(|| format!("read {} for upload", fe.name))?;
        store
            .put(
                &join_key(&seg_prefix, &fe.name),
                &bytes,
                fe.sha256.as_deref(),
            )
            .with_context(|| format!("upload segment file {}", fe.name))?;
    }
    // SEGMENT.json (authenticated by its own MAC, no inventory checksum) after its sections,
    // so a lister never sees a manifest before the files it names.
    let seg_json =
        std::fs::read(seg_dir.join("SEGMENT.json")).context("read SEGMENT.json for upload")?;
    store
        .put(
            &graph_format::segmanifest::SegmentManifest::key(graph, seg_manifest.segment_uuid),
            &seg_json,
            None,
        )
        .context("upload SEGMENT.json")?;
    // The set manifest, then the `current` pointer last.
    store
        .put(
            &graph_format::setmanifest::SetManifest::key(graph, set_uuid),
            &set.to_bytes()?,
            None,
        )
        .context("upload flush set manifest")?;
    store
        .put(
            &join_key(graph, "current"),
            format!("{}\n", set_uuid.0).as_bytes(),
            None,
        )
        .context("write remote current pointer")?;
    Ok(())
}

/// fsync a directory so a rename into it is durable before the next publish step.
fn fsync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .with_context(|| format!("fsync {}", dir.display()))
}

/// Spawn the configured `slater-build` binary to rebuild `graph` from the binary
/// consolidation `dump` directory into `data_dir`, publishing a fresh generation —
/// the production `build` seam for [`Graphs::consolidate_graph`]. A bare
/// `builder_bin` resolves on `PATH`. A non-zero exit is an error, so the caller
/// keeps the old core serving. The dump carries dense ids and global symbol ids, so
/// the builder ingests it directly (`--input-format slater-dump`), skipping parse,
/// node dedup, and endpoint resolution.
pub fn run_builder(builder_bin: &str, dump: &Path, graph: &str, data_dir: &Path) -> Result<()> {
    let status = std::process::Command::new(builder_bin)
        .arg("--input")
        .arg(dump)
        .arg("--input-format")
        .arg("slater-dump")
        .arg("--graph")
        .arg(graph)
        .arg("--data-dir")
        .arg(data_dir)
        .status()
        .with_context(|| format!("spawn builder '{builder_bin}'"))?;
    if !status.success() {
        bail!("builder '{builder_bin}' exited with {status} while consolidating '{graph}'");
    }
    Ok(())
}

/// RAII release of the per-writer consolidation claim ([`DeltaWriter::begin_consolidation`]),
/// so every exit path of [`Graphs::consolidate_graph`] — success, a dump/build/swap/retire
/// error, or an early return — clears the in-flight flag and re-enables auto flush/compaction.
struct ConsolidationGuard(Arc<DeltaWriter>);

impl Drop for ConsolidationGuard {
    fn drop(&mut self) {
        self.0.end_consolidation();
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
/// A graph with a writer-side stack mutation in flight — consolidate / flush /
/// compact — is **left alone**: those ops publish `current` themselves and own the
/// swap that follows it (see [`Graphs::adopt_published_generation`]). The guard is
/// here to notice generations published *behind the server's back* (an external
/// rebuild + rsync), not the server's own.
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
        // Hold the graph's swap mutex across the whole decision: the read of the served
        // generation, the read of `current`, and the swap that may follow. It serialises
        // this sweep against the writer-side ops, which take the same lock to adopt what
        // they publish.
        let Ok(_swap) = graphs.swap_lock(&name) else {
            continue; // not served (the map is fixed at construction, so: unreachable)
        };
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
        // The pointer moved — but is this change the server's own? A consolidation, flush
        // or compaction publishes `current` and then swaps the served slot onto it itself,
        // because only it can run the retire/rebind that follows. Stealing that swap makes
        // its own report "unchanged" and silently skips the retire, orphaning the delta.
        //
        // Deferring on the claim is exact, not a narrowing of the window: an op that has
        // published `current` still holds it (`ConsolidationGuard` releases it only after
        // retire, which runs after the op's own swap — and that swap must first take the
        // mutex we are holding right now). So an op cannot have published *and* finished
        // while we hold this lock: a pointer change seen with the claim set is always the
        // op's own, still pending its swap. Leave it. It is picked up on a later poll if
        // the op somehow does not adopt it.
        if graphs.writer(&name).is_some_and(|w| w.is_consolidating()) {
            debug!(graph = %name, "generation guard: deferring to an in-flight consolidation/flush/compaction");
            continue;
        }
        match strategy {
            ReloadStrategy::Exit => return SweepAction::Shutdown(name),
            // We already hold the swap mutex, so call the locked body directly (a std
            // `Mutex` is not reentrant).
            ReloadStrategy::Swap => match graphs.swap_locked(&name, vector_cache) {
                Ok(Some(new)) => {
                    info!(graph = %name, generation = %new, "swapped to a new generation (reloadStrategy=swap)");
                    // Adopt the ACL published alongside the new generation — this is the
                    // legitimate channel for an ACL change (rebuild + publish) — but adopt
                    // it *stamp-enforced*, exactly as the hot-reload poll does. The swap's
                    // policy check hashed the live acl.json and verified it against the new
                    // stamp, but that is a *separate read*; re-using its verdict to justify
                    // an unconditional `reload()` is a check-then-load TOCTOU — the file can
                    // change between the two reads, so the bytes actually loaded need not be
                    // the bytes that were verified. `reload_checked` closes that: it reads
                    // acl.json once, hashes *those* bytes, and installs the parsed ACL only
                    // when that digest still matches every served generation's stamp — so the
                    // bytes loaded are the bytes checked, and the stamp is enforced on this
                    // reload just like on `poll_checked`. A file that no longer matches (an
                    // in-window tamper) is refused and the last-good ACL keeps serving.
                    if let Some(acl) = acl {
                        acl.reload_checked(|d| graphs.acl_digest_acceptable(d));
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
    graphs: Arc<Graphs>,
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
            let (c, v, r, g) = (
                cache.clone(),
                vector_cache.clone(),
                result_cache.clone(),
                graphs.clone(),
            );
            // Each sweep briefly takes the cache mutexes; run off the async reactor.
            if let Err(e) = tokio::task::spawn_blocking(move || {
                let now = Instant::now();
                c.evict_expired(now, ttl);
                v.evict_expired(now, ttl);
                r.evict_expired(now, ttl);
                // Free dense-degree chunks idle past the TTL on every live generation — the
                // chunk-lazy column's elastic tier, swept like the block cache (pinned columns
                // and generations without a column are no-ops).
                for gen in g.current_generations() {
                    gen.evict_cold_degree_chunks(now, ttl);
                }
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
pub(crate) struct ConnCtx {
    acl: Arc<AclHandle>,
    graphs: Arc<Graphs>,
    cache: Arc<BlockCache>,
    /// The vector-index pool (second cache pool): resident PQ codes (pinned) + the
    /// Vamana block LRU, with its own `vector_cache_bytes` budget. Shared across
    /// connections; the `AnnMode::Vamana` arm of the executor reads through it.
    vector_cache: Arc<VectorIndexCache>,
    /// The FreshDiskANN RW-indexes over the write delta, one per `(generation, label,
    /// property)` (`crate::rwindex`). **Not** part of `vector_cache`'s budget: it is derived
    /// state bounded by the delta, with its own `vectorQuery.rwIndex.maxVectors` valve.
    rw_indexes: Arc<RwIndexCache>,
    /// Safety valves for the above (`vectorQuery.rwIndex.*`).
    rw_index_cfg: crate::rwindex::RwIndexConfig,
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
    /// Effective-degree at/above which a node's adjacency is streamed rather than
    /// materialised (`query.adjStreamThreshold`), and the edges-per-chunk of that stream
    /// (`query.adjStreamChunk`). Applied to every per-query engine — the fan-out hub guard.
    adj_stream_threshold: u64,
    adj_stream_chunk: usize,
    /// Shared worker pool for per-query parallelism (shortestPath frontier expansion,
    /// multi-hop expansion, brute-force kNN, anchor scans, …), sized to
    /// `query.maxFanout`. `None` when the fanout is ≤ 1 (sequential). Built once per
    /// process and shared across connections (per-query pool creation would churn OS
    /// threads).
    fanout_pool: Option<Arc<rayon::ThreadPool>>,
    /// Beam-search list size for the Vamana arm (`vectorQuery.beamWidth`).
    beam_width: usize,
    /// Beam-search list size for the per-segment read-only temp indexes
    /// (`vectorQuery.tempBeamWidth`, HIK-113).
    temp_beam_width: usize,
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
    /// TLS handshake→Bolt handshake→`LOGON` deadline (ms); 0 = none. Armed at `accept()`
    /// (see [`PreAuth::admit`]), so it bounds the whole pre-auth window as one budget.
    login_timeout_ms: u64,
    /// Deadline (ms) for the TLS handshake alone, on top of `login_timeout_ms` —
    /// whichever expires first wins (`server.tlsHandshakeTimeoutMs`); 0 = none.
    tls_handshake_timeout_ms: u64,
    /// Idle read timeout (ms) for an authenticated connection; 0 = none.
    idle_timeout_ms: u64,
    /// Budget for connections that have not yet completed `LOGON`. A connection holds
    /// one permit from the TCP `accept()` — *before* the TLS handshake, not after it —
    /// until authentication succeeds, then releases it, so a flood of anonymous sockets
    /// cannot starve authenticated readers. See [`PreAuth`].
    pre_auth_limit: Arc<Semaphore>,
    /// Budget for argon2id password verifies running **at once**
    /// (`server.maxConcurrentAuth`). A `LOGON` takes a permit before it hands the verify
    /// to a blocking thread and holds it until the hash actually finishes, so a flood of
    /// auth attempts can neither wedge a reactor worker nor swamp the blocking pool that
    /// query execution runs on. See [`verify_off_reactor`].
    auth_limit: Arc<Semaphore>,
    /// Failed `LOGON`s one connection may make before it is closed
    /// (`server.maxAuthFailures`); 0 = unlimited.
    max_auth_failures: usize,
    /// Budget for write statements **executing at once** (`server.maxConcurrentWrites`).
    /// A RUN takes a permit before it hands the write to a blocking thread and holds it
    /// until the write actually finishes, so a write flood can neither wedge a reactor
    /// worker nor swamp the blocking pool that query execution runs on. See
    /// [`execute_write_off_reactor`].
    write_limit: Arc<Semaphore>,
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

    // ── Writable-layer consolidation (`CALL slater.consolidate()`) ────────────
    /// The data directory holding each graph's generations + scratch consolidation
    /// dump. Passed to [`Graphs::consolidate_graph`].
    data_dir: PathBuf,
    /// The `slater-build` binary spawned to rebuild a consolidated generation
    /// (`config.delta.builder_bin`). A bare name resolves on `PATH`.
    builder_bin: String,
    /// Active-memtable byte budget: a write that pushes it past this flushes the
    /// memtable to an L0 segment (`config.delta.memtable_bytes`, Phase 4d-ii).
    memtable_bytes: usize,
    /// L0 segment count that triggers an L0→L0 compaction after a write
    /// (`config.delta.l0_compaction_trigger`; 0 disables — Phase 4d-ii).
    l0_compaction_trigger: usize,
    /// Whole-delta byte budget (active memtable + every L0 level) that triggers a T2
    /// delta→segment flush after a write (`config.delta.segment_flush_bytes`; 0 disables
    /// — Phase 6). Distinct from `memtable_bytes` (memtable→L0); this folds the entire
    /// delta into a core segment, resident or off-heap L0 alike (Phase 7.5).
    segment_flush_bytes: usize,
    /// Upper core-segment count that admits a T3 segment→segment compaction after a write
    /// (`config.delta.max_upper_segments`; 0 disables — Phase 5.3 policy, Phase 6 auto-fire).
    max_upper_segments: usize,
    /// Grace (seconds) before the orphan segment/set GC sweep reclaims a dir the served set no
    /// longer references (`config.delta.segment_gc_grace_secs`; 0 disables — Phase 7 slice 7.2).
    /// The sweep fires after the orphan-creating events (a T3 compaction, a consolidation).
    segment_gc_grace_secs: u64,
    /// Auto-consolidation threshold as a percent of the served core's entity count
    /// (`config.delta.delta_core_percent`; 0 disables — Phase 4d-ii-b).
    delta_core_percent: usize,
    /// Hard cap on total resident delta bytes before a write throttles
    /// (`config.delta.delta_hard_bytes`; 0 disables — Phase 4d-ii-b).
    delta_hard_bytes: usize,
    /// Off-peak window (server-local, cron-style) gating the fraction-of-core
    /// auto-consolidation (`config.delta.consolidate_window`). `None` = no gating: a
    /// due consolidation fires whenever. The hard-cap throttle ignores this.
    consolidate_window: Option<crate::cron_window::CronWindow>,
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
#[derive(Debug)]
pub(crate) struct Failure {
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
        let code = if e.downcast_ref::<parser::WriteClauseRejected>().is_some() {
            CODE_ACCESS_MODE
        } else if e.downcast_ref::<parser::QueryTooDeep>().is_some() {
            // A query rejected for nesting past the parser's depth bound is malformed
            // input, not a failed execution — same class as a syntax error (GQL 42000).
            // Classified by *type*: the depth guards run before the text ever reaches
            // pest, so there is no "syntax error" prefix on the message to match.
            CODE_SYNTAX
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
pub(crate) struct Session {
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
    /// Failed authentication attempts on this connection. Once it reaches
    /// `ConnCtx::max_auth_failures` the connection is closed — one socket does not get
    /// to queue password verifies for its whole login window. Reset on a success.
    auth_failures: usize,
    /// The connection's handshake→`LOGON` deadline, if one is configured. Held here so
    /// the wait for an argon2 permit is bounded by the same deadline that bounds the
    /// pre-auth reads: a queued verify must not outlive the login window.
    login_deadline: Option<TokioInstant>,
}

impl Session {
    /// Drop every piece of session state that belongs to **the authenticated user**,
    /// as opposed to the connection (HIK-123).
    ///
    /// A Bolt connection outlives the identity on it: `LOGOFF` and a bare re-`LOGON`
    /// both hand the same socket to a new principal. Anything scoped to the *old*
    /// principal — rows their grants let them read, a graph their grants resolved — is
    /// their data, and must not survive into the next principal's session. `RESET`
    /// wants the identical clear for its own reason (abandon the stream).
    ///
    /// **Invariant: this is the only place that clears user-scoped state, and every
    /// identity transition calls it.** The bug this fixes was three transitions
    /// (`RESET` / `LOGOFF` / re-`LOGON`) agreeing on what to clear in only one of
    /// them: `LOGOFF` zeroed `user` and left `pending` (the prior user's rows, drained
    /// by the next `PULL`) and `tx_graph` (the prior user's graph, reused by the next
    /// `RUN` without an ACL check). A field added to [`Session`] and cleared on only
    /// some of those paths reintroduces exactly that leak — so if it is scoped to the
    /// user's identity rather than to the connection, clear it **here**.
    fn clear_user_state(&mut self) {
        self.pending = None;
        self.tx_graph = None;
    }
}

// ── Framing over an async stream ──────────────────────────────────────────────

/// A write on the pre-auth path ran past the login deadline. Typed (HIK-103) so the
/// accept-loop error sink branches on the cause's *type* rather than matching its message
/// text, and can count it against the same login-timeout counter as a stalled pre-auth read.
#[derive(Debug, thiserror::Error)]
#[error("pre-auth socket write exceeded the login deadline")]
struct WriteDeadlineExceeded;

/// A Bolt message framer over any async byte stream (plain TCP or TLS).
struct Framed<S> {
    stream: S,
    buf: Vec<u8>,
    /// Resumable de-chunker: keeps a cursor into `buf` and the partial body across reads so
    /// a message that arrives over many reads is reassembled in O(n), not O(n²) (it does not
    /// re-scan the whole buffer on every partial read).
    reassembler: chunk::ChunkReassembler,
    /// Largest reassembled message body this connection will currently accept. The
    /// framer is deliberately auth-blind — it owns a budget number, not the reason
    /// it changed. `handle_connection` starts it at the tight pre-auth cap and
    /// bumps it to the generous post-auth cap once `LOGON` succeeds (ratcheting it
    /// back down on `LOGOFF`).
    max_body: usize,
    /// Deadline for a single write, when one applies. The write-side counterpart of the
    /// bounded pre-auth reads (HIK-103): a zero-window peer that completes the handshake and
    /// then stops reading would otherwise park the server in `write_all`/`flush` for as long
    /// as it keeps the socket open, while holding an antechamber permit. Like `max_body`, the
    /// framer is auth-blind here — it owns a deadline, not the reason it is set.
    /// `handle_connection` sets it to the login deadline while unauthenticated and clears it
    /// once `LOGON` succeeds, so an authenticated client is left free to read its own results
    /// at its own pace (a slow post-auth reader is back-pressure, not an attack).
    write_deadline: Option<TokioInstant>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Framed<S> {
    fn new(stream: S, max_body: usize) -> Self {
        Self {
            stream,
            buf: Vec::with_capacity(8192),
            reassembler: chunk::ChunkReassembler::new(),
            max_body,
            write_deadline: None,
        }
    }

    /// Read the next complete (de-chunked) message body, or `None` at a clean EOF.
    async fn read_message(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            if let Some((body, consumed)) = self.reassembler.feed(&self.buf, self.max_body)? {
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

    /// Write raw bytes, bounded by `write_deadline` when it is set. A stalled write in the
    /// pre-auth window is torn down at the deadline (surfaced as [`WriteDeadlineExceeded`])
    /// so the connection is dropped and its antechamber permit released, rather than parked
    /// while a zero-window peer holds the socket open (HIK-103). When no deadline is set the
    /// path is the bare `await` it always was.
    async fn write_all_bounded(&mut self, bytes: &[u8]) -> Result<()> {
        match self.write_deadline {
            Some(dl) => timeout_at(dl, self.stream.write_all(bytes))
                .await
                .map_err(|_| anyhow!(WriteDeadlineExceeded))??,
            None => self.stream.write_all(bytes).await?,
        }
        Ok(())
    }

    async fn write_message(&mut self, msg: &PsValue) -> Result<()> {
        self.write_all_bounded(&message::to_wire(msg)).await
    }

    async fn flush(&mut self) -> Result<()> {
        match self.write_deadline {
            Some(dl) => timeout_at(dl, self.stream.flush())
                .await
                .map_err(|_| anyhow!(WriteDeadlineExceeded))??,
            None => self.stream.flush().await?,
        }
        Ok(())
    }
}

// ── Listener ─────────────────────────────────────────────────────────────────

/// Bind the configured address and serve Bolt connections until the process exits.
/// global `conn_limit` permit the accept loop had already reserved for it, while the
/// antechamber cap (`maxPreAuthConnections`, deliberately a fraction of `maxConnections`
/// so authenticated readers always have headroom) counted it as nothing. Enough such
/// peers took every global slot; the accept loop then parked on `conn_limit` and stopped
/// draining the kernel queue. The plaintext path was never exposed — `handle_connection`
/// runs immediately there — so the hole was TLS-only, and TLS is what production runs.
pub(crate) struct PreAuth {
    /// Antechamber slot, held from `accept()` until `LOGON` succeeds. `Option` because
    /// `handle_connection` releases it on the transition to authenticated (and reclaims
    /// one on `LOGOFF`), so an authenticated reader does not squat the anonymous budget.
    permit: Option<OwnedSemaphorePermit>,
    /// Deadline for the *whole* pre-auth window — TLS handshake, Bolt handshake, `HELLO`,
    /// `LOGON` — as a single budget. Armed once, so advancing a stage does not refresh
    /// the allowance and no stage boundary leaves a gap for a slow peer to sit in.
    deadline: Option<TokioInstant>,
}

impl PreAuth {
    /// Admit a freshly accepted socket to the antechamber, or `None` when it is full.
    /// Rejecting rather than queueing is the point: parking anonymous sockets would just
    /// hold file descriptors, the very exhaustion this defends against.
    fn admit(ctx: &Arc<ConnCtx>) -> Option<Self> {
        let permit = match ctx.pre_auth_limit.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                debug!("pre-auth connection budget reached; rejecting connection");
                ctx.diag.record_rejected_pre_auth();
                return None;
            }
        };
        Some(Self {
            permit: Some(permit),
            deadline: (ctx.login_timeout_ms > 0)
                .then(|| TokioInstant::now() + Duration::from_millis(ctx.login_timeout_ms)),
        })
    }

    /// The deadline the TLS handshake must finish by: the sooner of the pre-auth window
    /// and `server.tlsHandshakeTimeoutMs`. The dedicated bound is not redundant — a TLS
    /// handshake is a 2-RTT machine exchange and deserves a far tighter leash than a
    /// login window sized for a driver's `HELLO`/`LOGON` round trips, and `loginTimeoutMs`
    /// may legitimately be set to 0, which must not silently un-bound the handshake.
    fn tls_deadline(&self, ctx: &Arc<ConnCtx>) -> Option<TokioInstant> {
        let handshake = (ctx.tls_handshake_timeout_ms > 0)
            .then(|| TokioInstant::now() + Duration::from_millis(ctx.tls_handshake_timeout_ms));
        match (handshake, self.deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
}

/// A flush / compaction / consolidation found the exclusive per-graph consolidation
/// claim already held by another in-flight operation. Benign — the other op is doing
/// the work — so the segment-tier auto-triggers log it at debug, not warn. Typed so the
/// classifier branches on the error *type* rather than matching its message text.
#[derive(Debug, thiserror::Error)]
#[error("a {op} for '{graph}' is already in progress")]
struct ConsolidationInProgress {
    /// The operation phrase, so the rendered message reads naturally at each site
    /// ("consolidation" vs "consolidation or flush").
    op: &'static str,
    graph: String,
}

/// Whether an error is a lost `begin_consolidation` single-flight race — a flush or a
/// compaction that found another flush/consolidation already holding the exclusive
/// claim. Branches on the typed [`ConsolidationInProgress`] cause.
fn is_already_in_progress(e: &anyhow::Error) -> bool {
    e.downcast_ref::<ConsolidationInProgress>().is_some()
}

/// The writable-layer overlay a read runs under: the delta snapshot pinned for the
/// query's whole life, the epoch that keys its cached result **and cuts the RW-index**, and
/// the writer's touched-id journal the index advances from. `empty()` is the read-only path
/// (no delta, epoch 0, no journal), behaviourally identical to reading the core.
///
/// The delta and the epoch are taken in **one** atomic read (`DeltaWriter::delta_snapshot_at`).
/// Two separate loads let a commit land between them, handing the query a snapshot from before
/// the write and an epoch from after it — and an RW-index cut at that epoch then describes a
/// delta the query is not reading. See `crate::rwindex`.
pub(crate) struct ReadOverlay {
    delta: DeltaSnapshot,
    epoch: u64,
    /// `None` on the read-only path — there is no writer, hence no journal, hence no index.
    journal: Option<Arc<TouchedJournal>>,
}

impl ReadOverlay {
    fn empty() -> Self {
        Self {
            delta: DeltaSnapshot::empty(),
            epoch: 0,
            journal: None,
        }
    }
}

#[cfg(test)]
mod tests;
