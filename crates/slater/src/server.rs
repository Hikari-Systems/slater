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

impl Graphs {
    /// Discover and open every graph under `data_dir` on the local filesystem,
    /// deriving each generation's block cipher from `master_key` (required iff a
    /// generation is encrypted). Convenience over [`open_all_with_store`] for the
    /// filesystem backend.
    ///
    /// [`open_all_with_store`]: Graphs::open_all_with_store
    pub fn open_all(data_dir: &Path, master_key: Option<&[u8]>) -> Result<Self> {
        let store: Arc<dyn ObjectStore> = Arc::new(FsObjectStore::new(data_dir));
        Self::open_all_with_store(
            store,
            master_key,
            true,
            None,
            crate::degree_column::DegreeResidency::Lazy,
            None,
        )
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
        range_index_cache_bytes: Option<usize>,
        degree_residency: crate::degree_column::DegreeResidency,
        degree_column_bytes: Option<usize>,
    ) -> Result<Self> {
        let names = store.list("").context("list graphs in data store")?;
        // Open every graph concurrently. Each open is dominated by serial S3
        // round-trips (a HEAD per inventory file, a footer read per range index),
        // so overlapping the graphs — and, inside each, the per-file work (see
        // `Generation::open_with_store_opts`) — turns a sum-of-graphs cold start
        // into roughly the slowest single graph. rayon's work-stealing pool bounds
        // the fan-out to the core count; `ObjectStore` is `Send + Sync`. First
        // error wins.
        let graphs = names
            .into_par_iter()
            // A graph is one with a published `current` pointer.
            .filter(|name| store.exists(&join_key(name, "current")).unwrap_or(false))
            .map(|name| -> Result<(String, RwLock<Arc<Generation>>)> {
                let gen = Generation::open_with_store_opts_cached(
                    store.as_ref(),
                    &name,
                    master_key,
                    verify_integrity,
                    range_index_cache_bytes,
                    degree_residency,
                    degree_column_bytes,
                )
                .with_context(|| format!("open graph {name}"))?;
                Ok((name, RwLock::new(Arc::new(gen))))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let swap_locks = graphs.keys().map(|n| (n.clone(), Mutex::new(()))).collect();
        Ok(Self {
            store,
            master_key: master_key.map(<[u8]>::to_vec),
            verify_integrity,
            graphs,
            acl_path: None,
            require_acl_stamp: false,
            range_index_cache_bytes,
            degree_residency,
            degree_column_bytes,
            writers: HashMap::new(),
            swap_locks,
        })
    }

    /// Bring the writable layer online: open a [`DeltaWriter`] per served graph,
    /// replaying each graph's WAL against its current generation. A relative
    /// `cfg.wal_dir` is resolved under `data_dir`; one graph's segments live under
    /// `<wal_dir>/<graph>/`. Called once at boot only when `cfg.enabled`. Idempotent
    /// per graph — a graph that fails to open its writer aborts boot (a durable
    /// write layer that silently isn't there is worse than a hard failure).
    pub fn enable_writable_layer(
        &mut self,
        cfg: &DeltaConfig,
        data_dir: &Path,
        block_cache: Option<Arc<graph_format::blockcache::BlockCache>>,
    ) -> Result<()> {
        let base = {
            let p = Path::new(&cfg.wal_dir);
            if p.is_absolute() {
                p.to_path_buf()
            } else {
                data_dir.join(p)
            }
        };
        for (name, slot) in &self.graphs {
            let gen = slot.read().unwrap().clone();
            let dir = base.join(name);
            let writer = DeltaWriter::open_with_cache(
                &dir,
                name,
                gen.uuid(),
                gen.node_count(),
                gen.edge_count(),
                cfg.off_heap_l0,
                block_cache.clone(),
                |op| resolve_op(&gen, op),
            )
            .with_context(|| format!("open writable layer for graph '{name}'"))?;
            let n = writer.node_delta_count();
            if n > 0 {
                info!(graph = %name, node_deltas = n, "writable layer replayed WAL");
            }
            self.writers.insert(name.clone(), Arc::new(writer));
        }
        Ok(())
    }

    /// The writable-layer writer for `name`, or `None` when the layer is disabled
    /// or the graph has none.
    pub(crate) fn writer(&self, name: &str) -> Option<Arc<DeltaWriter>> {
        self.writers.get(name).cloned()
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
    pub(crate) fn get(&self, name: &str) -> Option<Arc<Generation>> {
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

    /// The per-graph swap mutex (see [`Graphs::swap_locks`]).
    ///
    /// Poison is recovered from rather than propagated: the lock guards `()`, and a
    /// panic mid-swap leaves the slot holding either the old or the new
    /// `Arc<Generation>` — both complete, neither torn — so there is no broken
    /// invariant to protect a later swapper from. Propagating poison would instead
    /// wedge every future hot-reload *and* every future consolidation of the graph on
    /// one unrelated panic. Mirrors the writable layer's poison-tolerant locks.
    fn swap_lock(&self, name: &str) -> Result<std::sync::MutexGuard<'_, ()>> {
        let lock = self
            .swap_locks
            .get(name)
            .ok_or_else(|| anyhow!("graph '{name}' is not served"))?;
        Ok(lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
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
    ///
    /// **The caller must hold the graph's [swap mutex](Graphs::swap_lock)** — both
    /// callers have to make a decision *atomically with* the swap ([`guard_sweep`]: is
    /// this pointer change mine to apply? [`Self::adopt_published_generation`]: which
    /// generation is served now?), so they take the lock themselves and call this body
    /// rather than a self-locking wrapper (a std `Mutex` is not reentrant).
    fn swap_locked(&self, name: &str, vector_cache: &VectorIndexCache) -> Result<Option<GenId>> {
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
            Generation::open_with_store_opts_cached(
                self.store.as_ref(),
                name,
                self.master_key.as_deref(),
                self.verify_integrity,
                self.range_index_cache_bytes,
                self.degree_residency,
                self.degree_column_bytes,
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
        // Sealed segment indexes (HIK-113): pin the new generation's before the swap.
        pin_segment_pqs(new_gen.as_ref(), vector_cache);
        *slot.write().unwrap() = new_gen.clone();
        // Free the retired generation's whole resident set — pinned PQ codes *and*
        // lazily-built brute-force matrices — so it does not linger past the swap.
        vector_cache.unpin_generation(live.uuid());
        // The base `unpin_generation` is keyed by the *generation* uuid; a segment's PQ is
        // keyed by the *segment* uuid, so unpin the retired segments explicitly or every T3
        // merge leaks their pinned bytes (the pinning trap — segment retirement funnels
        // through this swap on both the merge and the set-swap paths).
        unpin_retired_segment_pqs(&live, &new_gen, vector_cache);
        Ok(Some(new_gen.uuid()))
    }

    /// The publish step of a writer-side stack mutation (consolidate / flush / compact):
    /// adopt the generation the caller has just published as `<name>/current`, and return
    /// **the generation now served** — whether this call swapped it in or found it already
    /// swapped in.
    ///
    /// # Why not a bare [`Self::swap_locked`]
    /// A bare swap answers *"did **I** perform the swap?"* (`Ok(Some)` vs `Ok(None)`).
    /// That is the wrong question here, and using its answer to gate the op's work is a lost
    /// update. The background generation guard polls the very same `current` pointer, so a
    /// poll landing between the op's publish and the op's swap applies the new generation
    /// **first**; the op's swap then reports `Ok(None)`, the op treats a successful build
    /// as a failure, and its cleanup — `retire`, the only thing that drops the folded
    /// delta's WAL and rebinds the writer to the new core — is skipped. The delta is left
    /// bound to a generation that is no longer served, which every later consolidation
    /// refuses as orphaned: one unlucky poll wedges the writable layer permanently.
    ///
    /// Cleanup ownership does not belong to whoever wins a race for the swap; it belongs
    /// to the op that froze the delta, and to nobody else (the guard does not know a delta
    /// exists). So the op asks the question whose answer is the same whoever won — *which
    /// generation is served now?* — and the [swap mutex](Graphs::swap_lock) makes the read
    /// of the served slot atomic against the guard, so the answer is never a torn
    /// intermediate.
    ///
    /// # The caller still checks *what* it adopted
    /// An unchanged pointer is not an error here — it is the answer. It means the served
    /// generation already equals the on-disk `current`, which is either the caller's own
    /// published generation (the guard got there first) or the pre-existing one (nothing
    /// was published at all — e.g. a builder that exited 0 without writing a generation).
    /// The caller distinguishes the two, because only the caller knows which id it
    /// expected, and bails on the latter *before* running any cleanup.
    ///
    /// Returns the served `Arc<Generation>` itself, not just its id: the caller's cleanup
    /// needs both (the id to re-bind the writer to, the generation to re-resolve the WAL
    /// tail against and to take the new node/edge extents from), and reading them together
    /// under the swap mutex is what makes them consistent — a later `get()` could observe a
    /// *different* generation and rebind the writer to one id while rebasing it on another's
    /// extents.
    fn adopt_published_generation(
        &self,
        name: &str,
        vector_cache: &VectorIndexCache,
    ) -> Result<Arc<Generation>> {
        let _swap = self.swap_lock(name)?;
        // Swap if the pointer moved; *ignore whether it was us who moved it* — that is the
        // whole point. Errors (a corrupt/incomplete new generation) still propagate.
        self.swap_locked(name, vector_cache)?;
        self.get(name)
            .ok_or_else(|| anyhow!("graph '{name}' is not served"))
    }

    /// Consolidate `name`'s writable delta into a fresh immutable generation
    /// (Phase 1d, *dump-and-rebuild*): freeze the delta, dump the merged
    /// (core ⊕ delta) view as a business-key `MERGE` script, hand that to `build` to
    /// rebuild a fresh generation, then swap to the new generation and retire the
    /// consumed WAL segments. Returns the new generation's UUID.
    ///
    /// # Failure is non-destructive
    /// If `build` fails (a non-zero builder exit, an unwritable dump, …) nothing is
    /// mutated in place: the old core keeps serving, the frozen delta stays live in
    /// the memtable (the freeze only *sealed* its WAL segments, it did not clear
    /// them), and a crash before the `current` swap replays every write on reopen.
    /// Retirement — the only step that discards the delta — runs solely after the
    /// new generation is published and swapped in.
    ///
    /// # The `build` seam
    /// `build(dump, graph, data_dir)` rebuilds `graph` from the `dump` file and
    /// publishes a fresh generation under `data_dir` (updating its `current`
    /// pointer). Production passes [`run_builder`] (spawns the configured
    /// `slater-build`); tests inject a closure that publishes a known-correct
    /// generation, so the orchestration is exercised without a subprocess.
    ///
    /// Phase 1 runs on the single-writer path — the caller must not admit concurrent
    /// writes for the duration; the manual trigger and the Bolt surface are Phase 4.
    pub fn consolidate_graph(
        &self,
        name: &str,
        cache: &BlockCache,
        vector_cache: &VectorIndexCache,
        data_dir: &Path,
        build: impl Fn(&Path, &str, &Path) -> Result<()>,
    ) -> Result<GenId> {
        let writer = self
            .writer(name)
            .ok_or_else(|| anyhow!("graph '{name}' has no writable layer to consolidate"))?;
        // Claim exclusive consolidation rights: refuse to overlap two consolidations,
        // and suppress auto flush/compaction for the whole freeze→retire window (a new
        // L0 segment there would be dropped by `retire`, losing its writes — Phase
        // 4d-ii). The guard releases the claim on every exit path (RAII).
        if !writer.begin_consolidation() {
            return Err(ConsolidationInProgress {
                op: "consolidation",
                graph: name.to_string(),
            }
            .into());
        }
        let _consolidation_guard = ConsolidationGuard(writer.clone());
        let core = self
            .get(name)
            .ok_or_else(|| anyhow!("graph '{name}' is not served"))?;
        // The delta's dense ids only line up with the core it was resolved against.
        // A mismatch means the served generation already moved on (a prior swap) and
        // the delta is orphaned — refuse rather than dump a mis-resolved view.
        if writer.core_uuid() != core.uuid() {
            bail!(
                "cannot consolidate '{name}': the writable delta was resolved against generation \
                 {} but the served core is {} — the delta is orphaned",
                writer.core_uuid(),
                core.uuid()
            );
        }

        // Freeze first: seal the WAL, capture the merged snapshot. Everything after
        // this is either fully applied (swap + retire) or fully reverted (the sealed
        // segments still replay the writes).
        let frozen = writer
            .freeze()
            .with_context(|| format!("freeze writable delta for '{name}'"))?;

        // Dump the merged (core ⊕ delta) view to a scratch *binary* dump directory
        // beside the graph. The builder ingests it directly (no re-parse / re-resolve).
        let dump_path = data_dir.join(name).join(".consolidate.dump");
        let dump_res: Result<()> = {
            let view = MergedView::new(
                core.as_ref(),
                DeltaSnapshot::with_levels(frozen.snapshot.clone(), frozen.l0.clone()),
            );
            let engine = Engine::new(&view, cache);
            crate::consolidate::serialise_binary_dump(&engine, &view, &dump_path)
        };
        if let Err(e) = dump_res {
            let _ = std::fs::remove_dir_all(&dump_path);
            return Err(e).with_context(|| format!("serialise consolidation dump for '{name}'"));
        }

        // Rebuild. A builder failure leaves the delta live (no retire) and the old
        // core serving; propagate the error after removing the scratch dump.
        if let Err(e) = build(&dump_path, name, data_dir) {
            let _ = std::fs::remove_dir_all(&dump_path);
            return Err(e).with_context(|| format!("rebuild consolidated generation for '{name}'"));
        }
        let _ = std::fs::remove_dir_all(&dump_path);

        // Publish: adopt the freshly built generation (validated + PQ-pinned) into the
        // served slot. The background guard polls the same `current` pointer and may have
        // swapped it in already; `adopt_published_generation` reports what is served
        // either way, so the retire below runs whoever won the swap — it is *this* call's
        // to run, and skipping it would orphan the delta we just folded in.
        let new_gen = self
            .adopt_published_generation(name, vector_cache)
            .with_context(|| format!("swap in consolidated generation for '{name}'"))?;
        let new_uuid = new_gen.uuid();
        // Still the pre-consolidation core ⇒ the builder exited 0 without publishing
        // anything. Nothing was folded in, so bail *before* retire: the delta stays live.
        if new_uuid == core.uuid() {
            bail!("builder for '{name}' did not publish a new generation (current unchanged)");
        }

        // Retire: the delta now lives in the new core, so drop the consumed WAL
        // segments and re-bind the writer to the new generation (re-basing the
        // synthetic node/edge id spaces on the new core's node/edge counts). Any
        // post-freeze write is replayed onto the new core via `resolve_op` bound to the
        // freshly-swapped generation — a business key that was delta-born pre-freeze
        // re-resolves to its now-real dense id (Phase 4a).
        writer
            .retire(
                &frozen.consumed,
                &frozen.consumed_l0,
                new_uuid,
                new_gen.node_count(),
                new_gen.edge_count(),
                |op| resolve_op(new_gen.as_ref(), op),
            )
            .with_context(|| format!("retire consolidated delta for '{name}'"))?;

        info!(graph = %name, generation = %new_uuid, "consolidated writable delta into a fresh generation");
        Ok(new_uuid)
    }

    /// **T2 flush** (`docs/SEGMENTED-CORE-PLAN.md`, Phase 4): fold `name`'s writable delta
    /// into a single immutable **upper core segment** stacked over the unchanged base —
    /// the O(delta) alternative to [`consolidate_graph`](Self::consolidate_graph), which
    /// reads the whole core back out and rebuilds. The base is *preserved*: no id moves and
    /// no business key is re-resolved against a new core (only the surviving post-freeze WAL
    /// tail is re-resolved by retire, exactly as consolidation does).
    ///
    /// Skeleton mirrors consolidation — freeze → publish → swap → retire — with the segment
    /// write standing in for the dump+rebuild:
    /// 1. Freeze the delta (seal its WAL, capture an immutable snapshot).
    /// 2. Materialise the snapshot into a new `segments/<uuid>/` core segment.
    /// 3. Publish a fresh **set** (same base, one more segment) and flip `current` — the
    ///    crash barrier: `current` only ever names a set whose segment + manifest are fully
    ///    written, and a crash before the flip leaves an orphan segment (no `current`
    ///    change), harmless until GC.
    /// 4. Swap the served generation to the new set (its stack now carries the segment).
    /// 5. Retire: drop the consumed WAL, rebase the memtable past the new stack top, re-bind
    ///    the writer to the new set uuid.
    ///
    /// Returns `Ok(Some(set_uuid))` on a flush, `Ok(None)` when the delta is empty.
    ///
    /// # Scope (slice 4.1)
    /// Births-only, plaintext, fs-backed, no prior L0 level. A delta carrying a core patch
    /// or tombstone, a stacked L0 level, or an encrypted-at-rest core is refused (later
    /// slices). Not yet wired to an auto-trigger — invoked explicitly.
    pub fn flush_graph_to_segment(
        &self,
        name: &str,
        vector_cache: &VectorIndexCache,
        data_dir: &Path,
    ) -> Result<Option<GenId>> {
        let writer = self
            .writer(name)
            .ok_or_else(|| anyhow!("graph '{name}' has no writable layer to flush"))?;
        // A flush and a consolidation both mutate the set/stack — share the exclusive claim
        // (and suppress auto flush/compaction over the freeze→retire window). RAII release.
        if !writer.begin_consolidation() {
            return Err(ConsolidationInProgress {
                op: "consolidation or flush",
                graph: name.to_string(),
            }
            .into());
        }
        let _guard = ConsolidationGuard(writer.clone());

        let core = self
            .get(name)
            .ok_or_else(|| anyhow!("graph '{name}' is not served"))?;
        if writer.core_uuid() != core.uuid() {
            bail!(
                "cannot flush '{name}': the writable delta was resolved against generation {} \
                 but the served core is {} — the delta is orphaned",
                writer.core_uuid(),
                core.uuid()
            );
        }

        // Freeze: seal the WAL, capture the immutable snapshot. Non-destructive — a failure
        // before the `current` flip leaves the old set serving and the delta live.
        let frozen = writer
            .freeze()
            .with_context(|| format!("freeze writable delta for '{name}'"))?;
        if frozen.snapshot.is_empty() && frozen.l0.iter().all(|l| l.is_empty()) {
            return Ok(None); // nothing to flush; freeze's fresh WAL segment keeps taking writes
        }

        // Fold the frozen snapshot with any spilled L0 levels into ONE newest-wins
        // `SegmentData` — the flush writer's input (Phase 4c). The active memtable is newest;
        // `frozen.l0` is newest-first. Every level was resolved against the same served core, so
        // they share a `synthetic_base`; the fold keeps it (= `prior_node_total`), holding the
        // writer's Phase-3.2 band assertion. Three cases: **no L0** flushes the snapshot
        // directly; **resident L0** folds in RAM via `merge_levels` (the tested path); **off-heap
        // L0** (a block image, not a memtable) folds at the `SegmentData` level via
        // `flush_segment_data` — working in dense-id space without reconstructing a memtable (an
        // off-heap image drops the edge endpoint identities a memtable rebuild would need).
        let flush_data: slater_delta::l0_offheap::SegmentData = if frozen.l0.is_empty() {
            frozen.snapshot.to_segment_data()
        } else if frozen.l0.iter().all(|l| l.as_memtable().is_some()) {
            let mut levels: Vec<&Memtable> = Vec::with_capacity(1 + frozen.l0.len());
            levels.push(frozen.snapshot.as_ref());
            for lvl in &frozen.l0 {
                levels.push(lvl.as_memtable().expect("all levels checked resident"));
            }
            Memtable::merge_levels(&levels).to_segment_data()
        } else {
            slater_delta::flush_segment_data(&frozen.snapshot, &frozen.l0)
        };

        // The appended band starts at the current stack top (base + every existing segment).
        let prior_node_total = core.stack().extents().nodes.total();
        let prior_edge_total = core.stack().extents().edges.total();

        let seg_uuid = GenId(uuid::Uuid::new_v4());
        let set_uuid = GenId(uuid::Uuid::new_v4());
        let created_unix = now_unix();
        let seg_dir = data_dir
            .join(name)
            .join("segments")
            .join(seg_uuid.0.to_string());

        // Encryption parity: when the served core is encrypted at rest, the flush segment must
        // be too. Derive a *fresh* per-segment cipher + manifest header (KDF salt only, never
        // the key) mirroring the builder's `derive_cipher`; the read side re-derives the same
        // cipher from `manifest.encryption` + the master key (`segstack::derive_segment_cipher`).
        let (cipher, encryption_header): (
            Option<std::sync::Arc<graph_format::crypto::BlockCipher>>,
            Option<graph_format::manifest::EncryptionHeader>,
        ) = match self.master_key.as_deref() {
            Some(key) => {
                let salt = graph_format::crypto::random_salt();
                let header = graph_format::manifest::EncryptionHeader {
                    aead: graph_format::crypto::AEAD_NAME.to_string(),
                    kdf: graph_format::crypto::KDF_NAME.to_string(),
                    salt_hex: graph_format::crypto::hex_encode(&salt),
                };
                let cipher =
                    std::sync::Arc::new(graph_format::crypto::BlockCipher::from_master(key, &salt));
                (Some(cipher), Some(header))
            }
            None => (None, None),
        };

        let manifest = {
            let inp = crate::flush_segment::FlushInputs {
                seg_dir: &seg_dir,
                seg_uuid,
                base_uuid: core.base_uuid(),
                core: core.as_ref(),
                prior_node_total,
                prior_edge_total,
                cipher,
                master_key: self.master_key.as_deref(),
                encryption_header,
                created_unix,
            };
            match crate::flush_segment::write_flush_segment(&flush_data, &inp) {
                Ok(m) => m,
                Err(e) => {
                    let _ = std::fs::remove_dir_all(&seg_dir);
                    return Err(e)
                        .with_context(|| format!("materialise flush segment for '{name}'"));
                }
            }
        };

        // Publish a fresh set (same base, existing segments + the new one), then flip
        // `current` — set before pointer so `current` only ever names a complete set.
        let mut set =
            graph_format::setmanifest::SetManifest::singleton(core.base_uuid(), created_unix);
        set.set_uuid = set_uuid;
        set.segments = core
            .stack()
            .segments()
            .iter()
            .map(|s| graph_format::setmanifest::SegmentRef::from_manifest(&s.manifest))
            .chain(std::iter::once(
                graph_format::setmanifest::SegmentRef::from_manifest(&manifest),
            ))
            .collect();
        publish_set_and_current(data_dir, name, set_uuid, &set)
            .with_context(|| format!("publish flush set for '{name}'"))?;

        // When the served store is not the local filesystem the segment was staged to
        // (S3/GCS/in-memory), publish the segment + set + `current` through it so a reader
        // that opens via the store finds them — `current` last, the copy-completeness
        // barrier. This must precede the swap below, which reads `current` from `self.store`.
        if !self.store.is_local_fs() {
            upload_flush_to_store(
                self.store.as_ref(),
                name,
                &seg_dir,
                &manifest,
                set_uuid,
                &set,
            )
            .with_context(|| format!("upload flush segment for '{name}' to the object store"))?;
        }

        // Swap the served generation to the new set (its stack now carries the segment) —
        // or find that the background guard, polling the same `current`, already did. The
        // retire below is ours to run either way (see `adopt_published_generation`).
        let new_gen = self
            .adopt_published_generation(name, vector_cache)
            .with_context(|| format!("swap in flushed set for '{name}'"))?;
        let new_uuid = new_gen.uuid();
        if new_uuid != set_uuid {
            bail!(
                "flush for '{name}' published set {set_uuid} but the served generation is \
                 {new_uuid} — refusing to retire the delta"
            );
        }

        // Retire: the flushed writes now live in the segment. Drop the consumed WAL and
        // rebase the memtable past the new stack top (base + every segment band, incl. the
        // new one). resolve_op is bound to the new generation, so a post-freeze re-MERGE of
        // a flushed born key re-resolves through the segment's index.
        writer
            .retire(
                &frozen.consumed,
                &frozen.consumed_l0,
                set_uuid,
                new_gen.stack().extents().nodes.total(),
                new_gen.stack().extents().edges.total(),
                |op| resolve_op(new_gen.as_ref(), op),
            )
            .with_context(|| format!("retire flushed delta for '{name}'"))?;

        info!(graph = %name, set = %set_uuid, segment = %seg_uuid, "flushed writable delta into a core segment");
        Ok(Some(set_uuid))
    }

    /// **T3 segment compaction** (Phase 5): fold the contiguous run of upper segments at
    /// ordinal `[start, end)` (oldest→newest) into a single merged segment, publish a new set
    /// that splices it in place of the run, and swap the served generation onto it.
    ///
    /// Unlike a flush, compaction touches **only** the immutable segment stack — it reads the
    /// run's segments, never the base or the delta, and writes an O(inputs) merged segment
    /// that reads identically to the run. The merged band is the union of the run's bands, so
    /// the dense id space (`extents().total()`) is invariant: the write-delta's resolved ids
    /// stay valid and the writer is simply **rebound** to the new set (no freeze, no WAL
    /// replay, no rebase — see [`DeltaWriter::rebind_core_uuid`]). The run's old segment
    /// directories are left on disk for a later GC pass (Phase 7); the new set no longer
    /// references them.
    ///
    /// Shares the [`DeltaWriter::begin_consolidation`] exclusion with flush/consolidation so
    /// no two stack mutations overlap. Returns the new set uuid, or an error if the run is not
    /// a valid contiguous run of ≥ 2 segments.
    pub fn compact_graph_segments(
        &self,
        name: &str,
        vector_cache: &VectorIndexCache,
        data_dir: &Path,
        start: usize,
        end: usize,
    ) -> Result<GenId> {
        let writer = self
            .writer(name)
            .ok_or_else(|| anyhow!("graph '{name}' has no writable layer to compact"))?;
        if !writer.begin_consolidation() {
            return Err(ConsolidationInProgress {
                op: "consolidation or flush",
                graph: name.to_string(),
            }
            .into());
        }
        let _guard = ConsolidationGuard(writer.clone());

        let core = self
            .get(name)
            .ok_or_else(|| anyhow!("graph '{name}' is not served"))?;
        if writer.core_uuid() != core.uuid() {
            bail!(
                "cannot compact '{name}': the writable delta was resolved against generation \
                 {} but the served core is {} — the delta is orphaned",
                writer.core_uuid(),
                core.uuid()
            );
        }

        let segments = core.stack().segments();
        if start >= end || end > segments.len() || end - start < 2 {
            bail!(
                "compact '{name}': invalid run [{start}, {end}) over {} segment(s) — need a \
                 contiguous run of at least two",
                segments.len()
            );
        }
        let run: Vec<&crate::segstack::LoadedSegment> = segments[start..end].iter().collect();

        // The id-space totals must be preserved by the merge (the merged band unions the
        // run's), else the delta's resolved ids would no longer line up — assert it below.
        let old_node_total = core.stack().extents().nodes.total();
        let old_edge_total = core.stack().extents().edges.total();

        let seg_uuid = GenId(uuid::Uuid::new_v4());
        let set_uuid = GenId(uuid::Uuid::new_v4());
        let created_unix = now_unix();
        let seg_dir = data_dir
            .join(name)
            .join("segments")
            .join(seg_uuid.0.to_string());

        // Encryption parity: a merge over an encrypted stack writes a fresh per-segment cipher
        // + header (KDF salt only) — mirroring the flush path.
        let (cipher, encryption_header): (
            Option<std::sync::Arc<graph_format::crypto::BlockCipher>>,
            Option<graph_format::manifest::EncryptionHeader>,
        ) = match self.master_key.as_deref() {
            Some(key) => {
                let salt = graph_format::crypto::random_salt();
                let header = graph_format::manifest::EncryptionHeader {
                    aead: graph_format::crypto::AEAD_NAME.to_string(),
                    kdf: graph_format::crypto::KDF_NAME.to_string(),
                    salt_hex: graph_format::crypto::hex_encode(&salt),
                };
                let cipher =
                    std::sync::Arc::new(graph_format::crypto::BlockCipher::from_master(key, &salt));
                (Some(cipher), Some(header))
            }
            None => (None, None),
        };

        let manifest = {
            let inp = crate::merge_segment::MergeInputs {
                seg_dir: &seg_dir,
                seg_uuid,
                base_uuid: core.base_uuid(),
                base: core.as_ref(),
                cipher,
                master_key: self.master_key.as_deref(),
                encryption_header,
                created_unix,
            };
            match crate::merge_segment::write_merge_segment(&run, &inp) {
                Ok(m) => m,
                Err(e) => {
                    let _ = std::fs::remove_dir_all(&seg_dir);
                    return Err(e)
                        .with_context(|| format!("materialise merged segment for '{name}'"));
                }
            }
        };

        // Publish a fresh set: the segments below the run, the merged segment in the run's
        // ordinal slot, then the segments above the run — precedence preserved.
        let mut set =
            graph_format::setmanifest::SetManifest::singleton(core.base_uuid(), created_unix);
        set.set_uuid = set_uuid;
        let mut refs: Vec<graph_format::setmanifest::SegmentRef> =
            Vec::with_capacity(segments.len() - (end - start) + 1);
        for s in &segments[..start] {
            refs.push(graph_format::setmanifest::SegmentRef::from_manifest(
                &s.manifest,
            ));
        }
        refs.push(graph_format::setmanifest::SegmentRef::from_manifest(
            &manifest,
        ));
        for s in &segments[end..] {
            refs.push(graph_format::setmanifest::SegmentRef::from_manifest(
                &s.manifest,
            ));
        }
        set.segments = refs;
        publish_set_and_current(data_dir, name, set_uuid, &set)
            .with_context(|| format!("publish compacted set for '{name}'"))?;

        // Upload the merged segment + set + `current` (current last) when the store is remote;
        // the run's old segments stay in the store for a later GC. Shares the flush uploader.
        if !self.store.is_local_fs() {
            upload_flush_to_store(
                self.store.as_ref(),
                name,
                &seg_dir,
                &manifest,
                set_uuid,
                &set,
            )
            .with_context(|| format!("upload merged segment for '{name}' to the object store"))?;
        }

        // As in flush: adopt the published set, whether we swap it in or the background
        // guard already has. The rebind below is ours to run either way.
        let new_gen = self
            .adopt_published_generation(name, vector_cache)
            .with_context(|| format!("swap in compacted set for '{name}'"))?;
        let new_uuid = new_gen.uuid();
        if new_uuid != set_uuid {
            bail!(
                "compaction for '{name}' published set {set_uuid} but the served generation is \
                 {new_uuid} — refusing to rebind the delta"
            );
        }

        // The merged band unions the run's, so the id space is unchanged — the delta's ids
        // stay valid. Verify before rebinding (fail safe rather than corrupt the overlay).
        let new_node_total = new_gen.stack().extents().nodes.total();
        let new_edge_total = new_gen.stack().extents().edges.total();
        if new_node_total != old_node_total || new_edge_total != old_edge_total {
            bail!(
                "compaction for '{name}' changed the id space (nodes {old_node_total}→\
                 {new_node_total}, edges {old_edge_total}→{new_edge_total}) — refusing to \
                 rebind the delta"
            );
        }
        // Rebind the delta to the new set: ids unchanged, so no re-resolution or rebase.
        writer.rebind_core_uuid(set_uuid);

        info!(graph = %name, set = %set_uuid, segment = %seg_uuid, run_start = start, run_end = end, "compacted a run of upper segments into one");
        Ok(set_uuid)
    }

    /// Size-tiered auto-compaction (Phase 5 slice 5.3): consult the admission policy
    /// ([`crate::merge_segment::select_compaction_run`]) against the served stack's segment
    /// sizes and, when a run is admissible, fold it via [`Self::compact_graph_segments`].
    /// Returns the new set id, or `None` when the stack is within its `max_upper_segments`
    /// fan-out budget (a no-op — nothing is published or swapped).
    ///
    /// This is the *policy* entry point; **auto-firing it from the write path is Phase-6-gated**
    /// (it needs a segment-aware write resolve), exactly as the flush auto-trigger is. Until
    /// then it is driven explicitly (a future `CALL slater.compact()`, a schedule, or a test),
    /// mirroring how [`Self::compact_graph_segments`] takes an explicit run.
    pub fn compact_graph_segments_auto(
        &self,
        name: &str,
        vector_cache: &VectorIndexCache,
        data_dir: &Path,
        max_upper_segments: usize,
    ) -> Result<Option<GenId>> {
        // Per-segment on-disk size (the write-amplification proxy the selector tiers on).
        let sizes: Vec<u64> = {
            let core = self
                .get(name)
                .ok_or_else(|| anyhow!("graph '{name}' is not served"))?;
            core.stack()
                .segments()
                .iter()
                .map(|s| s.manifest.files.iter().map(|f| f.bytes).sum())
                .collect()
        };
        let Some((start, end)) =
            crate::merge_segment::select_compaction_run(&sizes, max_upper_segments)
        else {
            return Ok(None);
        };
        self.compact_graph_segments(name, vector_cache, data_dir, start, end)
            .map(Some)
    }

    /// **T4 GC** (Phase 7 slice 7.2): reclaim the orphaned `segments/<uuid>/` directories and
    /// stale `sets/<uuid>.json` files under `<data_dir>/<name>/` that the **currently served
    /// set** no longer references — the disk-reclamation the flush (4.4-d) and compaction (5.1)
    /// slices deferred, plus everything a retarget (7.1) orphans when it collapses a stacked set
    /// to a singleton (the whole prior set + all its segments).
    ///
    /// # Grace period (reader safety)
    /// A just-swapped-out set/segment may still be held by an in-flight reader that opened its
    /// `Generation` before the swap. On the local filesystem that reader holds the segment's
    /// files open, so `remove_dir_all` is safe for it (the inode outlives the unlink until the
    /// reader drops) — but a reader mid-open, or a remote backend that re-fetches a block lazily
    /// after the object is gone, is not. So an orphan is deleted only after it has been
    /// **observed unreferenced for at least `grace_secs`**: the first sweep stamps a marker under
    /// `<graph>/.gc/` (its mtime is the retirement observation), and a later sweep deletes once
    /// the marker has aged past the grace. `grace_secs == 0` deletes on first sight (immediate
    /// mode — safe here because the sweep holds the single-flight claim below, so no in-flight
    /// flush/compaction is publishing a segment `current` does not yet name).
    ///
    /// # Single-flight
    /// Takes the [`DeltaWriter::begin_consolidation`] claim (when a writable layer exists) so it
    /// never races a flush / compaction / consolidation mutating the set/stack — a lost race
    /// bails "already in progress" (benign; the caller retries on a later write). A read-only
    /// graph has no writer and nothing mutates its stack, so the claim is skipped.
    ///
    /// # Backends (slice 7.4)
    /// Works on **any** backend: an orphan is discovered by listing the store, its objects
    /// removed via [`ObjectStore::delete`] (on a remote store) and its local staged directory
    /// removed via `std::fs` (the staged copy a flush left under `data_dir` before uploading; on
    /// a local-fs store the staged files *are* the objects, so `remove_dir_all` alone suffices
    /// and the redundant object-delete is skipped). The grace marker is always a small local
    /// file under `<graph>/.gc/` — the server's own bookkeeping of when it first saw the orphan,
    /// independent of whether the segment was ever staged locally on this instance.
    pub fn gc_orphan_segments(
        &self,
        name: &str,
        data_dir: &Path,
        grace_secs: u64,
    ) -> Result<SegmentGcReport> {
        // Never race a stack mutation (flush / compaction / consolidation). A read-only graph
        // has no writable layer, so nothing mutates its stack and the claim is unnecessary.
        let _guard = match self.writer(name) {
            Some(w) => {
                if !w.begin_consolidation() {
                    return Err(ConsolidationInProgress {
                        op: "consolidation or flush",
                        graph: name.to_string(),
                    }
                    .into());
                }
                Some(ConsolidationGuard(w))
            }
            None => None,
        };
        let remote = !self.store.is_local_fs();

        // The live reference set: the set `current` names, plus its segments. A `current` that
        // names a bare generation uuid (a singleton — e.g. just after a retarget) has no set
        // file, so nothing under `sets/` or `segments/` is live and every entry is an orphan.
        let current = GenId(Generation::current_uuid_in(self.store.as_ref(), name)?);
        let (live_set, live_segments): (Option<GenId>, std::collections::HashSet<GenId>) =
            if graph_format::setmanifest::SetManifest::exists_via(
                self.store.as_ref(),
                name,
                current,
            ) {
                let set = graph_format::setmanifest::SetManifest::read_via(
                    self.store.as_ref(),
                    name,
                    current,
                )
                .with_context(|| format!("read current set for GC of '{name}'"))?;
                let segs = set.segments.iter().map(|s| s.uuid).collect();
                (Some(current), segs)
            } else {
                (None, std::collections::HashSet::new())
            };

        let graph_dir = data_dir.join(name);
        // Grace markers live here — always local (server-side bookkeeping), never in the segment
        // dir (which may not exist on this instance for a store-backed segment).
        let gc_dir = graph_dir.join(".gc");
        let now = now_unix();
        let mut report = SegmentGcReport::default();

        // Whether an orphan marked at `marker` has aged past the grace; on the first sighting
        // (no marker) it stamps one and reports "not yet". `grace_secs == 0` is immediate.
        let eligible = |marker: &Path| -> Result<bool> {
            if grace_secs == 0 {
                return Ok(true);
            }
            match std::fs::metadata(marker) {
                Ok(md) => {
                    let mtime = md
                        .modified()
                        .ok()
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    Ok(now - mtime >= grace_secs as i64)
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    std::fs::create_dir_all(&gc_dir)
                        .with_context(|| format!("create gc dir {}", gc_dir.display()))?;
                    std::fs::File::create(marker)
                        .with_context(|| format!("stamp gc marker {}", marker.display()))?;
                    Ok(false)
                }
                Err(e) => Err(e).with_context(|| format!("stat gc marker {}", marker.display())),
            }
        };

        // Remove the local staged directory, tolerating an absent one (a store-backed segment
        // this instance never staged) — the local counterpart of the object delete below.
        let remove_local_dir = |dir: &Path| -> Result<()> {
            match std::fs::remove_dir_all(dir) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e).with_context(|| format!("gc local dir {}", dir.display())),
            }
        };
        let remove_local_file = |path: &Path| -> Result<()> {
            match std::fs::remove_file(path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(e).with_context(|| format!("gc local file {}", path.display())),
            }
        };

        // Orphaned segments. `segments/<uuid>/` the current set does not reference.
        let segments_dir = graph_dir.join("segments");
        for child in self.store.list(&join_key(name, "segments"))? {
            let Ok(uuid) = uuid::Uuid::parse_str(&child) else {
                continue; // skip anything not a bare uuid dir
            };
            if live_segments.contains(&GenId(uuid)) {
                continue;
            }
            let marker = gc_dir.join(format!("seg-{uuid}"));
            if !eligible(&marker)? {
                report.marked += 1;
                continue;
            }
            // Remote: delete every object under the segment prefix (node.blk … SEGMENT.json).
            if remote {
                let prefix = join_key(name, &format!("segments/{child}"));
                for f in self.store.list(&prefix)? {
                    self.store
                        .delete(&join_key(&prefix, &f))
                        .with_context(|| format!("gc remote segment object {prefix}/{f}"))?;
                }
            }
            // Local: the objects themselves on a local-fs store, else a staged copy.
            remove_local_dir(&segments_dir.join(&child))?;
            let _ = remove_local_file(&marker);
            report.deleted_segments.push(GenId(uuid));
        }

        // Stale set manifests. `sets/<uuid>.json` other than the current set.
        let sets_dir = graph_dir.join("sets");
        for child in self.store.list(&join_key(name, "sets"))? {
            let Some(stem) = child.strip_suffix(".json") else {
                continue; // skip *.tmp
            };
            let Ok(uuid) = uuid::Uuid::parse_str(stem) else {
                continue;
            };
            if live_set == Some(GenId(uuid)) {
                continue;
            }
            let marker = gc_dir.join(format!("set-{uuid}"));
            if !eligible(&marker)? {
                report.marked += 1;
                continue;
            }
            if remote {
                self.store
                    .delete(&graph_format::setmanifest::SetManifest::key(
                        name,
                        GenId(uuid),
                    ))
                    .with_context(|| format!("gc remote set manifest {uuid}"))?;
            }
            remove_local_file(&sets_dir.join(&child))?;
            let _ = remove_local_file(&marker);
            report.deleted_sets.push(GenId(uuid));
        }

        if !report.deleted_segments.is_empty() || !report.deleted_sets.is_empty() {
            info!(
                graph = %name,
                segments = report.deleted_segments.len(),
                sets = report.deleted_sets.len(),
                "reclaimed orphaned segment/set artifacts"
            );
        }
        Ok(report)
    }
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
struct ConnCtx {
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
                introspect::show_databases(&self.readable_databases(user), &self.bind_addr, |g| {
                    self.graphs.writer(g).is_some()
                })
            }),
            _ if q.starts_with("show default database") => Some(introspect::show_databases(
                &self.readable_databases(user),
                &self.bind_addr,
                |g| self.graphs.writer(g).is_some(),
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
fn pin_segment_pqs(gen: &Generation, cache: &VectorIndexCache) -> usize {
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
fn unpin_retired_segment_pqs(old: &Generation, new: &Generation, cache: &VectorIndexCache) {
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
        master_key.as_deref(),
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

/// The two things that bound an *anonymous* connection: its antechamber slot and its
/// deadline to stop being anonymous. Both are taken at the TCP `accept()`, in
/// [`PreAuth::admit`], and handed down through the TLS handshake into the Bolt state
/// machine — **not** created inside `handle_connection`, which is one stage too late.
///
/// That ordering is the fix for HIK-72. When the permit and the deadline were armed
/// behind the TLS handshake, a peer that completed TCP and then simply never sent a
/// ClientHello was invisible to both: it sat in `acceptor.accept()` forever, holding the
/// global `conn_limit` permit the accept loop had already reserved for it, while the
/// antechamber cap (`maxPreAuthConnections`, deliberately a fraction of `maxConnections`
/// so authenticated readers always have headroom) counted it as nothing. Enough such
/// peers took every global slot; the accept loop then parked on `conn_limit` and stopped
/// draining the kernel queue. The plaintext path was never exposed — `handle_connection`
/// runs immediately there — so the hole was TLS-only, and TLS is what production runs.
struct PreAuth {
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

/// (Optionally) wrap the socket in TLS, then run the Bolt connection.
///
/// The antechamber slot and the login deadline are taken *here*, before the TLS
/// handshake, and the handshake itself is bounded — see [`PreAuth`].
async fn serve_conn(sock: TcpStream, tls: Option<TlsAcceptor>, ctx: Arc<ConnCtx>) -> Result<()> {
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
///
/// `pre_auth` carries the antechamber slot and the login deadline, both already armed at
/// the TCP `accept()` by [`serve_conn`] — the deadline therefore covers the TLS handshake
/// that has just happened as well as the Bolt handshake about to happen, as one budget.
async fn handle_connection<S>(stream: S, ctx: Arc<ConnCtx>, pre_auth: PreAuth) -> Result<()>
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
async fn authenticate(
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
async fn verify_off_reactor(
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

/// Execute the parsed query on the blocking pool and return its rows already
/// encoded to PackStream (node/relationship resolution reads through the same
/// block cache, so it stays off the async reactor).
///
/// The result LRU is consulted first: a hit skips execution entirely and re-encodes
/// the cached rows for this connection's Bolt version (the cache stores the
/// version-independent `QueryResult`, so encoding — which still resolves node/rel
/// records through the block cache — is the only per-connection work). A miss
/// executes, caches the result, then encodes.
/// Resolve a write's business key `(label, key, value)` to its unique current-core
/// dense id via the label/property range index (an ISAM equality probe). `None`
/// when the property is not range-indexed for that label, the key is absent, or —
/// Phase 1 assumes a unique business key — the probe is ambiguous. The overlay's
/// dense-id read index ([`slater_delta::Memtable::by_dense`]) is built from this.
fn resolve_op(gen: &Generation, op: &WalOp) -> OpResolution {
    // Resolve one business key to a unique current-core dense id, or `None` when it is
    // absent (a delta-born node / endpoint, whose synthetic id the memtable allocates
    // in replay order) or non-unique/unindexed.
    let one = |(label, key, value): (&str, &str, &Value)| match resolve_business_key(
        gen, label, key, value,
    ) {
        KeyResolution::Unique(id) => Some(id),
        _ => None,
    };
    if let Some(node) = op.node_key() {
        return OpResolution::Node(one(node));
    }
    let (src, reltype, dst) = op.edge_keys().expect("node_key None ⇒ edge op");
    let src_id = one(src);
    let dst_id = one(dst);
    // An `UpsertEdge` whose endpoints are both core *and* whose edge already exists in
    // the core is an in-place property patch — resolve its core edge id so `apply`
    // routes it to `patch_core_edge` (rather than allocating a duplicate born edge).
    // A born-edge create (no core edge) or any delete resolves to `None`. Re-scanned
    // against the *current* core on every replay, so a born edge folded into a fresh
    // core (post-consolidation) correctly becomes a core-edge patch. An I/O error while
    // scanning collapses to `None` (a replay-time read failure is catastrophic anyway,
    // and this matches how endpoint resolution swallows a failed probe).
    let edge_id = match op {
        WalOp::UpsertEdge { .. } => match (src_id, dst_id, gen.reltype_id(reltype)) {
            (Some(s), Some(d), Some(rt)) => find_core_edge_id(gen, s, rt, d).unwrap_or(None),
            _ => None,
        },
        _ => None,
    };
    OpResolution::Edge {
        src: src_id,
        dst: dst_id,
        edge_id,
    }
}

/// The outcome of probing a write's business key against the current-core range
/// index. Distinguishing *absent* from *ambiguous*/*unindexed* is what lets a
/// `MERGE` create a delta-born node only when the key is genuinely new (Phase 2c).
#[derive(Clone, Copy)]
enum KeyResolution {
    /// Exactly one existing core node — its dense id.
    Unique(u64),
    /// The key is range-indexed but matches no core node (a `MERGE` create candidate).
    Absent,
    /// More than one core node carries the key (Phase 1 assumes a unique business key).
    Ambiguous,
    /// The `(label, key)` pair has no range index, so the write cannot be resolved.
    Unindexed,
}

/// Probe `(label, key, value)` against the label/property range index (an ISAM
/// equality probe), then **fold the core stack** over it so the write path resolves the
/// key the same way a read does (Phase 6, closing the 4.1 note (e) gap). The base
/// generation carries the index descriptor (`index_for` reads its manifest); the segment
/// fragments carry the born/patched/deleted contributions, folded oldest→newest by
/// [`CoreStack::fold_index_eq`] (each segment's `removals` sidecar suppresses the
/// base/older ids it supersedes, then its own matching ids union in — newest-wins). So a
/// `MERGE` of a business key **flushed into a segment** resolves to the segment's id (no
/// duplicate born node), a base key **deleted into a segment** resolves `Absent` (its
/// index entry is in the segment's `removals`, so a re-`MERGE` reborns it), and a key
/// **relocated by a segment patch** resolves under its new value only. The singleton
/// (no-segment) set short-circuits to the base ids, so a non-flushed graph is unchanged.
/// The overlay's dense-id read index is built from a `Unique` hit.
fn resolve_business_key(gen: &Generation, label: &str, key: &str, value: &Value) -> KeyResolution {
    let labels = [label.to_string()];
    let Some(idx) = crate::plan::index_for(gen, &labels, key) else {
        return KeyResolution::Unindexed;
    };
    let Some(reader) = gen.range_index(&idx) else {
        return KeyResolution::Unindexed;
    };
    let Ok(mut ids) = reader.lookup_eq(value) else {
        return KeyResolution::Unindexed;
    };
    let stack = gen.stack();
    if !stack.is_singleton() {
        // A fold read failure collapses to `Unindexed` — the write cannot resolve the key,
        // matching how the base probe's `Err` above is handled (a resolve-time read failure
        // is treated as "cannot resolve", never as "absent" — an `Absent` would risk a
        // duplicate born node).
        if stack.fold_index_eq(&mut ids, label, key, value).is_err() {
            return KeyResolution::Unindexed;
        }
        // The fold neither sorts nor dedups (base ids + per-segment unions), so a value
        // carried by both the base and a segment fragment would appear twice.
        ids.sort_unstable();
        ids.dedup();
    }
    match ids.as_slice() {
        [] => KeyResolution::Absent,
        [only] => KeyResolution::Unique(*only),
        _ => KeyResolution::Ambiguous,
    }
}

/// Resolve a whole batch of business-key `values` for a **fixed** `(label, key)` in one
/// merge-join sweep, returning a `KeyResolution` per input value (aligned to `values`). This
/// is the bulk-write floor from memory `bulk-delete-isam-resolve-floor`: resolving each of a
/// write batch's rows one-at-a-time re-decompresses the same ISAM leaf blocks per row (the
/// fence only skips a *segment* that cannot hold a given key — a batch of many distinct keys
/// still touches many blocks). Here the distinct values are sorted once and streamed against
/// the sorted base ISAM ([`IsamReader::lookup_eq_sorted`]) and each segment fragment
/// ([`CoreStack::fold_index_eq_batch`], carrying the oldest→newest suppress-then-union
/// semantics and the fence), so each touched block decompresses once for the whole batch.
///
/// Each value's verdict is **byte-identical** to [`resolve_business_key`] for that value: the
/// per-value base sweep equals its point `lookup_eq`, the batch fold equals the point fold,
/// and the singleton set short-circuits to the base sweep exactly as the single path does. A
/// probe of the same `(label, key)` that is unindexed, or any read failure in the sweep,
/// collapses every value to `Unindexed` (never `Absent`, so a read failure cannot manufacture
/// a duplicate born node — matching the single path).
fn resolve_business_keys_batch(
    gen: &Generation,
    label: &str,
    key: &str,
    values: &[&Value],
) -> Vec<KeyResolution> {
    let unindexed = || vec![KeyResolution::Unindexed; values.len()];
    let labels = [label.to_string()];
    let Some(idx) = crate::plan::index_for(gen, &labels, key) else {
        return unindexed();
    };
    let Some(reader) = gen.range_index(&idx) else {
        return unindexed();
    };
    // Base equality sweep: `ids[i]` is the base ids whose value equals `values[i]` (sorted,
    // unique — one entry per (value, id) in the base ISAM).
    let Ok(mut ids) = reader.lookup_eq_sorted(values) else {
        return unindexed();
    };
    let stack = gen.stack();
    if !stack.is_singleton() {
        if stack
            .fold_index_eq_batch(&mut ids, label, key, values)
            .is_err()
        {
            return unindexed();
        }
        // The fold unions base ids + per-segment fragment ids, so a value carried by both the
        // base and a fragment can appear twice — sort+dedup before the verdict.
        for v in &mut ids {
            v.sort_unstable();
            v.dedup();
        }
    }
    ids.iter()
        .map(|v| match v.as_slice() {
            [] => KeyResolution::Absent,
            [only] => KeyResolution::Unique(*only),
            _ => KeyResolution::Ambiguous,
        })
        .collect()
}

/// The delta snapshot + epoch to overlay when reading `gen`. The writer's delta
/// is only valid against the core generation it resolved its dense ids against;
/// after a generation swap the dense ids no longer line up, so we fail safe to the
/// pure core (empty delta) rather than mis-overlay. Phase 1c runs with
/// `reloadStrategy = exit` in practice, so this guard is defence in depth.
fn delta_for_read(writer: &Arc<DeltaWriter>, gen: &Arc<Generation>) -> ReadOverlay {
    if writer.core_uuid() == gen.uuid() {
        // ONE atomic read of the (snapshot, epoch) pair — see `ReadOverlay`.
        let published = writer.delta_snapshot_at();
        ReadOverlay {
            delta: published.delta,
            epoch: published.epoch,
            journal: Some(writer.touched_journal()),
        }
    } else {
        warn!(
            graph = %gen.graph(),
            "writable-layer delta resolved against a superseded generation — serving pure core"
        );
        ReadOverlay::empty()
    }
}

/// Coerce a bound value to a dense `f32` vector — the `vecf32($p)` write spelling.
///
/// Bolt has no vector type, so a driver sends an embedding as a list of numbers and it
/// arrives as a [`Val::List`] (`ps_to_val` is type-blind — it cannot know the target
/// property is vector-indexed). This is the write-side twin of the KNN path's
/// `eval_query_vector` coercion, and it keeps the two directions symmetric: a vector
/// returned to a driver is likewise rendered as a float list.
fn coerce_vecf32(v: Value, what: &str) -> std::result::Result<Value, Failure> {
    let items = match v {
        Value::Vector(_) => return Ok(v),
        Value::List(items) => items,
        other => {
            return Err(Failure::new(
                CODE_REQUEST,
                format!(
                    "{what}: vecf32() needs a list of numbers, got {}",
                    other.type_name()
                ),
            ))
        }
    };
    let mut out = Vec::with_capacity(items.len());
    for (i, x) in items.into_iter().enumerate() {
        let n = match x {
            Value::Float(f) => f,
            Value::Int(i) => i as f64,
            other => {
                return Err(Failure::new(
                    CODE_REQUEST,
                    format!(
                        "{what}: vecf32() elements must be numbers, got {}",
                        other.type_name()
                    ),
                ))
            }
        };
        // The Bolt front door: a driver can send a `NaN`/`±inf` `Float64` directly (no
        // `log()` needed). Reject it here through the one shared finiteness gate so a
        // non-finite component never enters an embedding via the wire (HIK-134).
        let c = graph_format::pq::finite_f32(i, n as f32)
            .map_err(|e| Failure::new(CODE_REQUEST, format!("{what}: {e}")))?;
        out.push(c);
    }
    Ok(Value::Vector(out))
}

/// Reject an embedding whose dimension disagrees with the index it is written to.
///
/// Both KNN arms hard-error on a dim mismatch, and a bad row would otherwise ride the T2
/// flush into a segment and the rebuild into the next generation before anyone noticed.
/// The write is the one place to catch it cheaply, and the one place that can still
/// report it to the client. A vector on an *unindexed* `(label, property)` is unconstrained
/// — it is an ordinary inline value, and the core admits those at any width.
fn validate_vector_dims(ops: &[WalOp], gen: &Generation) -> std::result::Result<(), Failure> {
    let indexes = &gen.manifest().vector_indexes;
    if indexes.is_empty() {
        return Ok(());
    }
    let check = |label: &str, prop: &str, v: &Value| -> std::result::Result<(), Failure> {
        let Value::Vector(xs) = v else {
            return Ok(());
        };
        let Some(d) = indexes
            .iter()
            .find(|d| d.label == label && d.property == prop)
        else {
            return Ok(());
        };
        if xs.len() != d.dim as usize {
            return Err(Failure::new(
                CODE_REQUEST,
                format!(
                    "the vector index on (:{label} {{{prop}}}) is {}-dimensional, but the value \
                     assigned to {prop} has {} dimensions",
                    d.dim,
                    xs.len()
                ),
            ));
        }
        Ok(())
    };
    for op in ops {
        match op {
            WalOp::UpsertNode { label, patches, .. }
            | WalOp::ReplaceNode { label, patches, .. } => {
                for (prop, v) in patches {
                    check(label, prop, v)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Evaluate a Phase 1c write's constant expression (a literal or a parameter) to a
/// storable [`Value`] against the query's parameters.
fn write_value(
    e: &parser::ast::Expr,
    params: &HashMap<String, Val>,
    what: &str,
) -> std::result::Result<Value, Failure> {
    use parser::ast::Expr;
    // `vecf32($p)` is the one call `ensure_constant` admits: its value is knowable only
    // once the parameter is bound, so unlike the all-literal form it cannot be folded at
    // lowering. Anything else non-constant was rejected there.
    if let Some(arg) = parser::as_vecf32_arg(e) {
        let val = match arg {
            Expr::Param(name) => params.get(name).ok_or_else(|| {
                Failure::new(CODE_REQUEST, format!("parameter ${name} was not supplied"))
            })?,
            _ => unreachable!("lower_write_statement folds or rejects vecf32 over {what}"),
        };
        let v = crate::exec::val_to_value(val).ok_or_else(|| {
            Failure::new(
                CODE_REQUEST,
                format!("{what} is not a storable scalar value"),
            )
        })?;
        return coerce_vecf32(v, what);
    }
    let val = match e {
        Expr::Literal(v) => return Ok(v.clone()),
        Expr::Param(name) => params.get(name).ok_or_else(|| {
            Failure::new(CODE_REQUEST, format!("parameter ${name} was not supplied"))
        })?,
        _ => unreachable!("lower_write_statement rejects non-constant {what}"),
    };
    crate::exec::val_to_value(val).ok_or_else(|| {
        Failure::new(
            CODE_REQUEST,
            format!("{what} is not a storable scalar value"),
        )
    })
}

/// Build the durable node WAL op sequence for a write statement, evaluating each value
/// through `eval` (constant/parameter for a plain write, per-row for a write-`UNWIND`).
/// Shared by the plain and batch paths so they cannot diverge. `SET n.p = v` /
/// `SET n += {map}` fold into `UpsertNode` patches (source order, LWW); `SET n = {map}`
/// emits a `ReplaceNode`; `REMOVE n.p` a `RemoveNodeProps`; `DELETE` a `DeleteNode`.
/// A statement that mixes a replace with further items yields several ops that
/// group-commit atomically. Label mutations (Stage 5) are still rejected by name.
/// Fold a list of `SET` items (identity `label`/`key`/`value` fixed) into a WAL op
/// sequence: `var.prop = v` / `var += {map}` accumulate into `UpsertNode` patches (source
/// order, LWW); `var = {map}` emits a `ReplaceNode`; `var:Label` a `SetNodeLabels`. When
/// `ensure_nonempty` and the items produce nothing, a no-op upsert is emitted (so a MERGE
/// still create-if-absent's its node). Shared by the main `SET`, the `ON CREATE`/`ON MATCH`
/// blocks, and `CREATE`.
fn fold_set_items(
    items: &[parser::ast::SetItem],
    label: &str,
    key: &str,
    value: &Value,
    ensure_nonempty: bool,
    eval: impl Fn(&parser::ast::Expr, &str) -> std::result::Result<Value, Failure>,
) -> std::result::Result<Vec<WalOp>, Failure> {
    use parser::ast::SetItem;
    let upsert = |patches: Vec<(String, Value)>| WalOp::UpsertNode {
        label: label.to_string(),
        key: key.to_string(),
        value: value.clone(),
        patches,
    };
    let mut ops: Vec<WalOp> = Vec::new();
    let mut pending: Vec<(String, Value)> = Vec::new();
    let mut added_labels: Vec<String> = Vec::new();
    for item in items {
        match item {
            // Patching the anchor key's value is allowed — it relocates the node in the
            // index (the "moved indexed value" overlay); the delta identity stays fixed.
            SetItem::Prop { prop, value: expr } => {
                pending.push((prop.clone(), eval(expr, "a SET value")?));
            }
            SetItem::MergeMap(map) => {
                for (k, expr) in map {
                    pending.push((k.clone(), eval(expr, "a merge-map value")?));
                }
            }
            SetItem::ReplaceMap(map) => {
                let patches = replace_map_patches(map, &eval)?;
                pending.clear();
                ops.push(WalOp::ReplaceNode {
                    label: label.to_string(),
                    key: key.to_string(),
                    value: value.clone(),
                    patches,
                });
            }
            // Label additions are independent of the property patches; collect them into
            // one SetNodeLabels op emitted after the patch flush.
            SetItem::AddLabels(labels) => added_labels.extend(labels.iter().cloned()),
        }
    }
    if !pending.is_empty() {
        ops.push(upsert(pending));
    }
    if !added_labels.is_empty() {
        ops.push(WalOp::SetNodeLabels {
            label: label.to_string(),
            key: key.to_string(),
            value: value.clone(),
            added: added_labels,
            removed: Vec::new(),
        });
    }
    if ops.is_empty() && ensure_nonempty {
        ops.push(upsert(Vec::new()));
    }
    Ok(ops)
}

fn build_node_wal_ops(
    stmt: &parser::ast::WriteStmt,
    key_value: &Value,
    eval: impl Fn(&parser::ast::Expr, &str) -> std::result::Result<Value, Failure>,
) -> std::result::Result<Vec<WalOp>, Failure> {
    use parser::ast::{RemoveItem, WriteOp};
    let label = stmt.label.clone();
    let key = stmt.key.clone();
    let value = key_value.clone();
    match &stmt.op {
        // The main SET fold emits at least one op (a no-op upsert when empty) so a MERGE
        // create-if-absent's its node and the write acks.
        WriteOp::Set(items) => fold_set_items(items, &label, &key, &value, true, eval),
        WriteOp::Remove(items) => {
            let mut props = Vec::new();
            let mut removed_labels = Vec::new();
            for item in items {
                match item {
                    RemoveItem::Prop(p) => {
                        if p == &stmt.key {
                            return Err(Failure::new(
                                CODE_REQUEST,
                                format!(
                                    "cannot REMOVE the business-key property '{p}' — it is the \
                                     node's identity"
                                ),
                            ));
                        }
                        props.push(p.clone());
                    }
                    RemoveItem::Labels(labels) => removed_labels.extend(labels.iter().cloned()),
                }
            }
            let mut ops = Vec::new();
            if !props.is_empty() {
                ops.push(WalOp::RemoveNodeProps {
                    label: label.clone(),
                    key: key.clone(),
                    value: value.clone(),
                    props,
                });
            }
            if !removed_labels.is_empty() {
                ops.push(WalOp::SetNodeLabels {
                    label,
                    key,
                    value,
                    added: Vec::new(),
                    removed: removed_labels,
                });
            }
            debug_assert!(!ops.is_empty(), "REMOVE names at least one prop or label");
            Ok(ops)
        }
        // A node DELETE tombstones the node; the topology overlay then suppresses its
        // incident edges. DELETE conformance (Stage 2) — a plain DELETE of a connected
        // node — is enforced by the caller after resolution.
        WriteOp::Delete { .. } => Ok(vec![WalOp::DeleteNode { label, key, value }]),
    }
}

/// Validate the label mutations in a write's op sequence against the graph and the
/// resolved node:
///  - a `SET n:Label` naming a label absent from the core symbol table is rejected (a
///    brand-new label has no core id, so the read overlay could not honour it — the
///    pre-existing-label subset ships first);
///  - `REMOVE n:<identity-label>` on a **delta-born** node is rejected (Decision C): its
///    label comes from its identity, so dropping it would leave the node label-less. On
///    an existing **core** node the drop is allowed (it still resolves by dense id).
///
/// `resolved` is the node's dense id; a born id is at or above the core node count.
fn validate_label_ops(
    ops: &[WalOp],
    resolved: Option<u64>,
    gen: &Generation,
    stmt: &parser::ast::WriteStmt,
) -> std::result::Result<(), Failure> {
    let is_born = resolved.is_some_and(|id| id >= gen.node_count());
    for op in ops {
        let WalOp::SetNodeLabels { added, removed, .. } = op else {
            continue;
        };
        for l in added {
            if gen.label_id(l).is_none() {
                return Err(Failure::new(
                    CODE_REQUEST,
                    format!(
                        "cannot add label ':{l}' — it is not defined in the graph (only \
                         pre-existing labels can be set)"
                    ),
                ));
            }
        }
        if is_born && removed.iter().any(|l| l == &stmt.label) {
            return Err(Failure::new(
                CODE_REQUEST,
                format!(
                    "cannot REMOVE the identity label ':{}' from a newly-created node",
                    stmt.label
                ),
            ));
        }
    }
    Ok(())
}

/// Evaluate a `SET n = {map}` replace map into storable patches. The map may re-set the
/// anchor key (which relocates the node in the index, like any indexed-value patch); a
/// map that omits the key keeps it — the reader re-seeds it from the delta identity.
fn replace_map_patches(
    map: &[(String, parser::ast::Expr)],
    eval: impl Fn(&parser::ast::Expr, &str) -> std::result::Result<Value, Failure>,
) -> std::result::Result<Vec<(String, Value)>, Failure> {
    let mut patches = Vec::with_capacity(map.len());
    for (k, expr) in map {
        patches.push((k.clone(), eval(expr, "a replace-map value")?));
    }
    Ok(patches)
}

/// Whether node `id` still has any relationship over the merged view (core + the
/// writer's current delta). Used to enforce openCypher DELETE conformance: a plain
/// `DELETE` of a node that still has relationships is an error — only `DETACH DELETE`
/// removes them. Both `outgoing_adj` and `incoming_adj` are overlay-aware, so an edge
/// a prior write already tombstoned (or an edge to an already-deleted node) is not
/// counted; the check therefore sees the *live* incident set at write time.
fn node_has_relationships(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    id: u64,
) -> std::result::Result<bool, Failure> {
    let delta = DeltaSnapshot::from_memtable(writer.snapshot());
    let view = MergedView::new(gen, delta);
    let cache = BlockCache::new(1 << 16);
    let engine = crate::exec::Engine::new(&view, &cache);
    node_has_relationships_via(&engine, id)
}

/// The engine-driven core of [`node_has_relationships`]: does `id` have any incident
/// relationship in the engine's overlaid view? Uses the short-circuit existence probe
/// ([`Engine::has_incident_edge`]) so a high-degree hub stops at its first live edge instead
/// of materialising the whole adjacency `Vec`. Factored out so the batched DELETE path can
/// hoist **one** engine (over the shared per-batch cache) out of its per-row loop rather than
/// rebuild a throwaway 64 KiB `BlockCache` + engine — and fully re-decode a hub — every row.
fn node_has_relationships_via<V: ReadView>(
    engine: &Engine<'_, V>,
    id: u64,
) -> std::result::Result<bool, Failure> {
    engine.has_incident_edge(id).map_err(|e: anyhow::Error| {
        Failure::new(
            CODE_EXECUTION,
            format!("check the node's incident relationships: {e:#}"),
        )
    })
}

/// The error a plain (non-`DETACH`) `DELETE` raises when its node still has
/// relationships — openCypher requires the edges be removed first.
fn delete_has_relationships_error() -> Failure {
    Failure::new(
        CODE_EXECUTION,
        "Cannot delete node, because it still has relationships. To delete it and its \
         relationships, use DETACH DELETE."
            .into(),
    )
}

/// One parsed write statement, ready to execute. The three write shapes differ only in
/// which `execute_*` they dispatch to; carrying them in one owned enum lets a single
/// helper move any of them onto a blocking thread.
enum WriteJob {
    /// Boxed: a `WriteStmt` is much the biggest of the three, and every `WriteJob` is
    /// moved into a blocking task.
    Node(Box<parser::ast::WriteStmt>),
    Create(parser::ast::CreateStmt),
    Edge(parser::ast::EdgeWriteStmt),
}

impl WriteJob {
    /// Run the write. Called on a blocking thread, never on the reactor.
    fn run(
        self,
        writer: &Arc<DeltaWriter>,
        gen: &Generation,
        params: &HashMap<String, Val>,
    ) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
        match self {
            WriteJob::Node(stmt) => execute_write(writer, gen, &stmt, params),
            WriteJob::Create(stmt) => execute_create(writer, gen, &stmt, params),
            WriteJob::Edge(stmt) => execute_edge_write(writer, gen, &stmt, params),
        }
    }
}

/// Execute one write statement **off the reactor**, under a concurrency cap.
///
/// A write is not cheap and it is not pure CPU: it resolves business keys against the
/// core (ISAM `pread` + zstd decompress — a network round trip on an S3/GCS backend),
/// materialises adjacency, then appends to the WAL and **fsyncs**. Running that inline in
/// the async `handle_request` — as the three RUN write arms did — parks a tokio *reactor*
/// worker for the whole of it, so one slow write stalls every other connection that
/// worker is driving and a handful of concurrent writes deafen the server entirely. Read
/// execution ([`run_query`]), consolidation and the delta-maintenance rungs have always
/// been on `spawn_blocking`; the write arms were the odd ones out (module doc, top of
/// file). So:
///
/// * the whole statement — resolve, adjacency, WAL append, fsync — moves to a blocking
///   thread, leaving the reactor free to keep driving every other connection's IO;
/// * `write_limit` caps how many writes execute **at once**. The cap is the point, not a
///   detail: every mutation of one graph is serialised behind that graph's single
///   [`DeltaWriter`] lock, so a bare `spawn_blocking` would merely relocate the problem —
///   a write flood would hand tokio's 512-thread blocking pool an unbounded queue of
///   tasks that immediately park on a mutex they cannot get, and *read queries, which run
///   on that same pool*, would starve behind them. A small cap keeps the pool free.
///   Permits above a handful buy nothing anyway (they only queue on the writer lock); the
///   handful that is there pays for itself because key resolution — the expensive,
///   IO-bound part — happens *outside* the lock, and separate graphs have separate
///   writers. Waiters park asynchronously: no thread, no reactor worker, and their number
///   is bounded by `server.maxConnections` (a writer is authenticated by construction);
/// * the permit is moved **into** the blocking closure, so it is released when the write
///   actually finishes rather than when a hung-up client cancels the await (a cancelled
///   `spawn_blocking` still runs to completion — releasing the permit early would let a
///   client who disconnects mid-write overrun the cap).
///
/// Write ordering is unaffected: one connection's requests are handled strictly in
/// sequence, and the order in which concurrent connections' writes land was — and still
/// is — decided by the single writer lock, not by this gate.
///
/// A panicked or aborted write task **fails closed**: the caller reports a failure, never
/// a SUCCESS. Durability is unchanged — the ack is still written only after
/// `DeltaWriter`'s fsync has returned.
async fn execute_write_off_reactor(
    ctx: &Arc<ConnCtx>,
    writer: &Arc<DeltaWriter>,
    gen: &Arc<Generation>,
    job: WriteJob,
    params: HashMap<String, Val>,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    let permit = ctx
        .write_limit
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| Failure::new(CODE_EXECUTION, "server is shutting down".into()))?;

    let writer = writer.clone();
    let gen = gen.clone();
    tokio::task::spawn_blocking(move || {
        // Held until the write is done, not until the caller stops waiting for it.
        let _permit = permit;
        job.run(&writer, gen.as_ref(), &params)
    })
    .await
    .map_err(|e| {
        // A panicked/aborted write is not acknowledged: the client is told it failed.
        warn!(error = %e, "write task did not complete");
        Failure::new(CODE_EXECUTION, "the write did not complete".into())
    })?
}

/// Execute one durable write: build the WAL op sequence from the parsed statement +
/// parameters, resolve the anchor's business key to a current-core dense id, and hand
/// the ops to the writer (WAL append + fsync commit + memtable apply + publish) as one
/// group commit. A statement lowers to several ops only when it mixes a replace-all with
/// further SET items; they commit atomically. Returns an empty result — read-back is a
/// separate `MATCH … RETURN` over the overlaid view.
pub(crate) fn execute_write(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    stmt: &parser::ast::WriteStmt,
    params: &HashMap<String, Val>,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    use parser::ast::WriteOp;
    if stmt.ret.is_some() {
        return Err(Failure::new(
            CODE_REQUEST,
            "RETURN after a write is not yet supported; issue a separate MATCH … RETURN to read \
             back the written values"
                .into(),
        ));
    }
    // A leading `UNWIND <list> AS r` is a batched (group-committed) write.
    if stmt.unwind.is_some() {
        return execute_write_batch(writer, gen, stmt, params);
    }
    let key_value = write_value(&stmt.key_value, params, "the anchor business-key value")?;
    let ops = build_node_wal_ops(stmt, &key_value, |e, what| write_value(e, params, what))?;
    // Every op in a statement shares the anchor key, so one resolution serves them all.
    // A non-DELETE op is "set-like" (addresses an existing node, or MERGE-creates one).
    let mut ops = ops;
    // MERGE `ON CREATE SET` / `ON MATCH SET`: whether the MERGE creates or matches is
    // decided by the pre-write state, so compute it before appending the conditional ops.
    if stmt.upsert && (!stmt.on_create.is_empty() || !stmt.on_match.is_empty()) {
        let created = merge_creates_node(writer, gen, &stmt.label, &stmt.key, &key_value);
        let items = if created {
            &stmt.on_create
        } else {
            &stmt.on_match
        };
        ops.extend(fold_set_items(
            items,
            &stmt.label,
            &stmt.key,
            &key_value,
            false,
            |e, what| write_value(e, params, what),
        )?);
    }
    // After the conditional fold, so an `ON CREATE SET` embedding is checked too.
    validate_vector_dims(&ops, gen)?;
    let is_set = !matches!(stmt.op, WriteOp::Delete { .. });
    let first = ops.first().expect("a node write yields at least one op");
    let resolved = resolve_node_op(writer, gen, first, is_set, stmt.upsert)?;
    validate_label_ops(&ops, resolved, gen, stmt)?;
    // DELETE conformance: a plain (non-DETACH) DELETE errors if the node still has any
    // relationship. `resolved` is the node's dense id (a delete never returns `None`).
    if let WriteOp::Delete { detach: false, .. } = &stmt.op {
        if let Some(id) = resolved {
            if node_has_relationships(writer, gen, id)? {
                return Err(delete_has_relationships_error());
            }
        }
    }
    let batch: Vec<(WalOp, OpResolution)> = ops
        .into_iter()
        .map(|op| (op, OpResolution::Node(resolved)))
        .collect();
    writer
        .write_batch(&batch)
        .map_err(|e| Failure::new(CODE_EXECUTION, format!("durable write failed: {e:#}")))?;
    Ok((Vec::new(), Vec::new()))
}

/// Whether a MERGE on `(label, key, value)` **creates** a new node (vs matching an
/// existing one). The node exists if the current core carries the key uniquely, or a
/// prior write already made it a delta-born node; otherwise this MERGE creates it.
/// Computed against the pre-write state (so it must be called before the op is applied).
fn merge_creates_node(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    label: &str,
    key: &str,
    value: &Value,
) -> bool {
    merge_creates_node_from(
        writer,
        resolve_business_key(gen, label, key, value),
        label,
        key,
        value,
    )
}

/// The core half of [`merge_creates_node`] over an already-resolved `KeyResolution` — so the
/// batch path can resolve every row's key in one merge-join sweep and decide create-vs-match
/// per row without a second per-row core probe.
fn merge_creates_node_from(
    writer: &Arc<DeltaWriter>,
    resolution: KeyResolution,
    label: &str,
    key: &str,
    value: &Value,
) -> bool {
    match resolution {
        KeyResolution::Unique(_) => false, // an existing core node — matched
        KeyResolution::Absent => writer.born_synthetic_in_delta(label, key, value).is_none(),
        // Ambiguous / unindexed will error at `resolve_node_op`; treat as "not created".
        _ => false,
    }
}

/// Execute a `CREATE (n:Label {props})`: designate the range-indexed property as the
/// business key and unconditionally create (born-upsert) the node with the remaining
/// properties. Errors if no inline property is the label's range-indexed identity.
fn execute_create(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    stmt: &parser::ast::CreateStmt,
    params: &HashMap<String, Val>,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    if stmt.ret.is_some() {
        return Err(Failure::new(
            CODE_REQUEST,
            "RETURN after a write is not yet supported; issue a separate MATCH … RETURN".into(),
        ));
    }
    // Evaluate every inline property, then pick the business key: the label's
    // range-indexed property (lowest core label id breaks a tie among indexes).
    let mut props: Vec<(String, Value)> = Vec::with_capacity(stmt.props.len());
    for (name, expr) in &stmt.props {
        props.push((
            name.clone(),
            write_value(expr, params, "a CREATE property")?,
        ));
    }
    let key = gen
        .manifest()
        .range_indexes
        .iter()
        .find(|ri| {
            ri.entity == graph_format::manifest::EntityKind::Node
                && ri.label_or_type == stmt.label
                && props.iter().any(|(p, _)| p == &ri.property)
        })
        .map(|ri| ri.property.clone())
        .ok_or_else(|| {
            Failure::new(
                CODE_REQUEST,
                format!(
                    "cannot CREATE (:{}): none of its properties is the label's range-indexed \
                     business key — add a range index, or use MERGE with an inline key",
                    stmt.label
                ),
            )
        })?;
    let key_pos = props
        .iter()
        .position(|(p, _)| p == &key)
        .expect("key present");
    let (_, key_value) = props.remove(key_pos);
    let op = WalOp::UpsertNode {
        label: stmt.label.clone(),
        key: key.clone(),
        value: key_value,
        patches: props,
    };
    // Born-create (upsert semantics): resolve as a set-like op with create-on-absent.
    let resolved = resolve_node_op(writer, gen, &op, true, true)?;
    writer
        .write(op, OpResolution::Node(resolved))
        .map_err(|e| Failure::new(CODE_EXECUTION, format!("durable CREATE failed: {e:#}")))?;
    Ok((Vec::new(), Vec::new()))
}

/// Does this statement mutate the graph, and so require a `write` grant?
///
/// Every arm of the write grammar must be listed here: node writes (`MERGE` /
/// `MATCH … SET` / `MATCH … DELETE`, plain or under a write-`UNWIND`), relationship writes
/// (`MERGE (a)-[r:R]->(b) [SET …]` / `MATCH (a)-[r:R]->(b) DELETE r`), and the
/// `CALL slater.consolidate()` admin trigger, which rewrites the served generation.
/// Matching on the enum rather than sniffing the query text means a new write statement
/// cannot be added without the compiler forcing a decision here.
fn statement_mutates(stmt: &parser::ast::Statement) -> bool {
    match stmt {
        parser::ast::Statement::Write(_)
        | parser::ast::Statement::Create(_)
        | parser::ast::Statement::WriteEdge(_)
        | parser::ast::Statement::Consolidate => true,
        parser::ast::Statement::Read(_) => false,
    }
}

/// Gate a parsed statement on the caller's grants for `graph`.
///
/// Reads are already gated at graph selection (`Acl::can_read`); this adds the write gate.
/// A `read` grant does **not** imply the right to mutate, so switching on `delta.enabled`
/// cannot silently promote every existing reader into a writer.
fn authorize_statement(
    acl: &Acl,
    user: &str,
    graph: &str,
    stmt: &parser::ast::Statement,
) -> std::result::Result<(), Failure> {
    if statement_mutates(stmt) && !acl.can_write(user, graph) {
        return Err(Failure::new(
            CODE_FORBIDDEN,
            format!("write access to graph '{graph}' is not granted to this user"),
        ));
    }
    Ok(())
}

/// Resolve a node write's business key to its dense-id context for the WAL op. `Unique`
/// → the core id; a MERGE-create (`is_set && upsert`) on an `Absent` key → a born
/// synthetic id (reusing one already flushed to L0, else `None` to allocate); a DELETE
/// or a `MATCH … SET` of a born node → its synthetic id resolved across the whole delta;
/// every other absent / ambiguous / unindexed case is a clear error. Shared by the single
/// and batched (write-UNWIND) node write paths so their semantics cannot drift.
///
/// Every `Absent` arm consults the delta, not just the core: a delta-born node is a real,
/// readable node, so `MERGE`, `DELETE` and `SET` must all be able to name it.
fn resolve_node_op(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    op: &WalOp,
    is_set: bool,
    upsert: bool,
) -> std::result::Result<Option<u64>, Failure> {
    let (label, key, value) = op.node_key().expect("resolve_node_op is for node ops only");
    resolve_node_op_from(
        writer,
        resolve_business_key(gen, label, key, value),
        label,
        key,
        value,
        is_set,
        upsert,
    )
}

/// The core half of [`resolve_node_op`] over an already-resolved `KeyResolution` — the
/// delta/born-id decision that does not touch the ISAM. The batch path resolves every row's
/// business key in one merge-join sweep (the bulk-write floor) and then routes each row's
/// `KeyResolution` through here, so the batched and single write paths still share one set of
/// create/update/delete semantics (they cannot drift).
fn resolve_node_op_from(
    writer: &Arc<DeltaWriter>,
    resolution: KeyResolution,
    label: &str,
    key: &str,
    value: &Value,
    is_set: bool,
    upsert: bool,
) -> std::result::Result<Option<u64>, Failure> {
    Ok(match resolution {
        KeyResolution::Unique(id) => Some(id),
        // MERGE create: a key absent from the core is a delta-born node — reuse an id
        // already flushed to L0, else `None` allocates a fresh one.
        KeyResolution::Absent if is_set && upsert => {
            writer.born_synthetic_for_identity(label, key, value)
        }
        // DELETE of a delta-born node: resolve its synthetic id across the whole delta so
        // the tombstone suppresses it (even if flushed to L0). Absent everywhere → error.
        KeyResolution::Absent if !is_set => {
            match writer.born_synthetic_in_delta(label, key, value) {
                Some(id) => Some(id),
                None => {
                    return Err(Failure::new(
                        CODE_EXECUTION,
                        format!(
                            "no {label}({key} = …) node to delete: the business key matches no \
                             existing node"
                        ),
                    ))
                }
            }
        }
        // A `MATCH … SET` (update-only) whose key matches no core node may still name a
        // **delta-born** node: it exists and reads back like any other, so an update has
        // to resolve it across the whole delta exactly as the DELETE arm above does.
        // Absent from the core *and* the delta → the key names nothing.
        KeyResolution::Absent => match writer.born_synthetic_in_delta(label, key, value) {
            Some(id) => Some(id),
            None => {
                return Err(Failure::new(
                    CODE_EXECUTION,
                    format!(
                        "no {label}({key} = …) node to update: the business key matches no \
                         existing node (use MERGE to create it)"
                    ),
                ))
            }
        },
        KeyResolution::Ambiguous => {
            return Err(Failure::new(
                CODE_EXECUTION,
                format!(
                    "the business key {label}({key} = …) matches more than one node — writes \
                     require a unique business key"
                ),
            ))
        }
        KeyResolution::Unindexed => {
            return Err(Failure::new(
                CODE_EXECUTION,
                format!(
                    "cannot write {label}({key} = …): the business key must be range-indexed to \
                     resolve it"
                ),
            ))
        }
    })
}

/// Evaluate a write-UNWIND per-row value expression: a literal, a parameter, the row
/// variable `var` itself, or `var.field` (a field of the current row map). Anything else
/// is rejected — a batched write's values are the bulk-import subset, not arbitrary
/// expressions. Returns the storable [`Value`].
fn eval_row_value(
    e: &parser::ast::Expr,
    var: &str,
    row: &Val,
    params: &HashMap<String, Val>,
    what: &str,
) -> std::result::Result<Value, Failure> {
    use parser::ast::Expr;
    // `SET n.embedding = vecf32(r.emb)` — the batched spelling. The argument is itself a
    // row reference, so evaluate it through this same restricted grammar and coerce.
    if let Some(arg) = parser::as_vecf32_arg(e) {
        let v = eval_row_value(arg, var, row, params, what)?;
        return coerce_vecf32(v, what);
    }
    let val: Val = match e {
        Expr::Literal(v) => return Ok(v.clone()),
        Expr::Param(name) => params.get(name).cloned().ok_or_else(|| {
            Failure::new(CODE_REQUEST, format!("parameter ${name} was not supplied"))
        })?,
        Expr::Var(v) if v == var => row.clone(),
        Expr::Property(base, field) => match base.as_ref() {
            Expr::Var(v) if v == var => match row {
                Val::Map(m) => m
                    .iter()
                    .find(|(k, _)| k == field)
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Val::Null),
                _ => {
                    return Err(Failure::new(
                        CODE_REQUEST,
                        format!(
                            "the UNWIND row is not a map, so {var}.{field} cannot supply {what}"
                        ),
                    ))
                }
            },
            _ => {
                return Err(Failure::new(
                    CODE_REQUEST,
                    format!(
                        "{what} may reference only the UNWIND variable '{var}' (as {var}.field)"
                    ),
                ))
            }
        },
        _ => {
            return Err(Failure::new(
                CODE_REQUEST,
                format!("{what} must be a literal, a parameter, or {var}.field in a batched write"),
            ))
        }
    };
    crate::exec::val_to_value(&val).ok_or_else(|| {
        Failure::new(
            CODE_REQUEST,
            format!("{what} is not a storable scalar value"),
        )
    })
}

/// Execute a **batched** node write (`UNWIND <list> AS r MATCH|MERGE (n:L {k: …}) …`):
/// evaluate the source list, build one WAL op per row (its business key + SET values
/// evaluated against that row), resolve each against the core, and apply the whole batch
/// under a single group commit ([`DeltaWriter::write_batch`] — one fsync, one publish).
/// Atomic: if any row fails to evaluate or resolve, the batch is rejected before it is
/// committed. NB: resolution is against the core ⊕ the delta *as of the batch start*, so
/// a within-batch create-then-delete of the same new key is not resolved (independent
/// rows — the bulk-import case — are unaffected).
fn execute_write_batch(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    stmt: &parser::ast::WriteStmt,
    params: &HashMap<String, Val>,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    use parser::ast::{Expr, WriteOp};
    let (source, var) = stmt
        .unwind
        .as_ref()
        .expect("execute_write_batch requires an UNWIND");
    // The UNWIND source is a parameter list (the bulk-import shape, `UNWIND $rows AS r`).
    let list: Val = match source {
        Expr::Param(name) => params.get(name).cloned().ok_or_else(|| {
            Failure::new(CODE_REQUEST, format!("parameter ${name} was not supplied"))
        })?,
        _ => {
            return Err(Failure::new(
                CODE_REQUEST,
                "the UNWIND source of a batched write must be a parameter list (e.g. \
                 `UNWIND $rows AS r`)"
                    .into(),
            ))
        }
    };
    let rows = match list {
        Val::List(items) => items,
        _ => {
            return Err(Failure::new(
                CODE_REQUEST,
                "the UNWIND source of a batched write is not a list".into(),
            ))
        }
    };
    // Evaluate every row's anchor business-key value up front, then resolve the whole batch's
    // keys against the core in **one merge-join sweep** (`resolve_business_keys_batch`) rather
    // than a per-row ISAM point probe — the bulk-write floor (memory
    // `bulk-delete-isam-resolve-floor`). The `(label, key)` is fixed across the batch, so only
    // the value varies: dedup the values, sweep the distinct set once, then fan each row's
    // resolution back out. The per-row `KeyResolution` is byte-identical to what the single
    // path would compute (the core probe reads `gen` only — the accumulating delta cannot
    // change it), so the born-id / create-vs-match decisions below are unchanged.
    let key_values: Vec<Value> = rows
        .iter()
        .map(|row| {
            eval_row_value(
                &stmt.key_value,
                var,
                row,
                params,
                "the anchor business-key value",
            )
        })
        .collect::<std::result::Result<_, _>>()?;
    let row_res: Vec<KeyResolution> = {
        // Distinct values in `cmp_key` order (the ISAM order the sweep needs), with each row
        // mapped to its distinct slot.
        let mut order: Vec<usize> = (0..key_values.len()).collect();
        order.sort_by(|&a, &b| key_values[a].cmp_key(&key_values[b]));
        let mut distinct: Vec<&Value> = Vec::new();
        let mut row_to_distinct = vec![0usize; key_values.len()];
        for &ri in &order {
            if distinct
                .last()
                .is_none_or(|last| !last.cmp_key(&key_values[ri]).is_eq())
            {
                distinct.push(&key_values[ri]);
            }
            row_to_distinct[ri] = distinct.len() - 1;
        }
        let resolved = resolve_business_keys_batch(gen, &stmt.label, &stmt.key, &distinct);
        row_to_distinct.iter().map(|&d| resolved[d]).collect()
    };

    // Hoist a single overlaid view + block cache + engine for the whole batch so the plain-DELETE
    // conformance probe below reuses them across every row, instead of allocating a throwaway
    // 64 KiB `BlockCache` (and re-decoding a hub's adjacency) per row. No op mutates the memtable
    // inside this loop — the ops accumulate and commit in the single `write_batch` after it — so
    // the pre-loop snapshot is the pre-batch state every per-row check would otherwise re-snapshot,
    // byte-identical. Built unconditionally (cheap: `Engine::new` allocates nothing material and
    // the cache stays empty until the first probe reads a block); the probe fires only for a plain
    // DELETE batch.
    let batch_delta = DeltaSnapshot::from_memtable(writer.snapshot());
    let batch_view = MergedView::new(gen, batch_delta);
    let batch_cache = BlockCache::new(1 << 16);
    let batch_engine = crate::exec::Engine::new(&batch_view, &batch_cache);

    let mut ops: Vec<(WalOp, OpResolution)> = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        let key_value = &key_values[i];
        let resolution = row_res[i];
        let mut row_ops = build_node_wal_ops(stmt, key_value, |e, what| {
            eval_row_value(e, var, row, params, what)
        })?;
        // MERGE ON CREATE / ON MATCH, per row (create-vs-match against the pre-batch state).
        if stmt.upsert && (!stmt.on_create.is_empty() || !stmt.on_match.is_empty()) {
            let created =
                merge_creates_node_from(writer, resolution, &stmt.label, &stmt.key, key_value);
            let items = if created {
                &stmt.on_create
            } else {
                &stmt.on_match
            };
            row_ops.extend(fold_set_items(
                items,
                &stmt.label,
                &stmt.key,
                key_value,
                false,
                |e, what| eval_row_value(e, var, row, params, what),
            )?);
        }
        validate_vector_dims(&row_ops, gen)?;
        let is_set = !matches!(stmt.op, WriteOp::Delete { .. });
        debug_assert!(!row_ops.is_empty(), "a node write yields at least one op");
        let resolved = resolve_node_op_from(
            writer,
            resolution,
            &stmt.label,
            &stmt.key,
            key_value,
            is_set,
            stmt.upsert,
        )?;
        validate_label_ops(&row_ops, resolved, gen, stmt)?;
        // DELETE conformance, per row: a plain DELETE errors if the row's node still
        // has a relationship (the batch is all-DELETE or all-SET, so no edge this batch
        // creates precedes the check).
        if let WriteOp::Delete { detach: false, .. } = &stmt.op {
            if let Some(id) = resolved {
                if node_has_relationships_via(&batch_engine, id)? {
                    return Err(delete_has_relationships_error());
                }
            }
        }
        for op in row_ops {
            ops.push((op, OpResolution::Node(resolved)));
        }
    }
    writer
        .write_batch(&ops)
        .map_err(|e| Failure::new(CODE_EXECUTION, format!("durable batch write failed: {e:#}")))?;
    Ok((Vec::new(), Vec::new()))
}

/// Resolve an edge-write endpoint's business key to a current-core dense id.
/// `Unique` → the id; `Absent` → `None` (a `MERGE` auto-creates a delta-born endpoint,
/// a `DELETE` no-ops if it is also not a born node); ambiguous / unindexed is a clear
/// error, exactly as the node write path.
fn resolve_endpoint(
    gen: &Generation,
    ep: &parser::ast::EndpointPat,
    value: &Value,
) -> std::result::Result<Option<u64>, Failure> {
    match resolve_business_key(gen, &ep.label, &ep.key, value) {
        KeyResolution::Unique(id) => Ok(Some(id)),
        KeyResolution::Absent => Ok(None),
        KeyResolution::Ambiguous => Err(Failure::new(
            CODE_EXECUTION,
            format!(
                "the business key {}({} = …) matches more than one node — writes require a \
                 unique business key",
                ep.label, ep.key
            ),
        )),
        KeyResolution::Unindexed => Err(Failure::new(
            CODE_EXECUTION,
            format!(
                "cannot write a relationship to {}({} = …): the business key must be range-indexed \
                 to resolve it",
                ep.label, ep.key
            ),
        )),
    }
}

/// The core edge id of `src -[reltype]-> dst` if the core already carries that edge,
/// else `None`. This is both the `MERGE` idempotency check (a re-`MERGE` of an existing
/// core edge must not add a duplicate delta-born edge) and the resolver for an in-place
/// core-edge property patch (the id keys the patch overlay). Scans only the source's
/// core outgoing adjacency (bounded by its out-degree) over an **empty-delta** view, so
/// it sees core edges only — a born duplicate is prevented by the memtable's identity
/// idempotency, and a patch must land on the genuine core edge id, never a synthetic one.
fn find_core_edge_id(
    gen: &Generation,
    src: u64,
    reltype: u32,
    dst: u64,
) -> std::result::Result<Option<u64>, Failure> {
    let cache = BlockCache::new(1 << 16);
    let view = MergedView::read_only(gen);
    let engine = crate::exec::Engine::new(&view, &cache);
    // Short-circuit at the first matching out-edge instead of materialising the source's whole
    // out-adjacency `Vec` to `find()` one edge — a hub source is never fully decoded. The
    // empty-delta (`read_only`) view keeps this core-only, so it returns the genuine core edge id.
    engine.find_outgoing_edge(src, reltype, dst).map_err(|e| {
        Failure::new(
            CODE_EXECUTION,
            format!("check for an existing relationship: {e:#}"),
        )
    })
}

/// Execute one durable relationship write (Phase 3c): resolve both endpoints, build
/// the WAL edge op, and hand it to the writer. A `MERGE` of an edge that already
/// exists in the core is an idempotent no-op; the relationship type must already
/// exist (the traversal overlay maps it to a core reltype id).
pub(crate) fn execute_edge_write(
    writer: &Arc<DeltaWriter>,
    gen: &Generation,
    stmt: &parser::ast::EdgeWriteStmt,
    params: &HashMap<String, Val>,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    use parser::ast::EdgeWriteOp;
    // The reltype must pre-exist: the read overlay resolves a born edge's type through
    // the core symbol table, so a brand-new type would be invisible to traversal.
    let Some(reltype_id) = gen.reltype_id(&stmt.reltype) else {
        return Err(Failure::new(
            CODE_EXECUTION,
            format!(
                "cannot write a :{} relationship: the relationship type must already exist in the \
                 graph",
                stmt.reltype
            ),
        ));
    };
    let src_value = write_value(&stmt.src.key_value, params, "the source business-key value")?;
    let dst_value = write_value(
        &stmt.dst.key_value,
        params,
        "the destination business-key value",
    )?;
    // Evaluate the optional `SET r.p = …` property patches (empty for a bare MERGE or a
    // DELETE). They are carried on the WAL op and stored on the delta-born edge.
    let mut patches = Vec::with_capacity(stmt.sets.len());
    for (prop, expr) in &stmt.sets {
        patches.push((
            prop.clone(),
            write_value(expr, params, "a relationship SET value")?,
        ));
    }
    // Core-only resolution first (`None` = absent from the core): the duplicate check
    // below must run against genuine core dense ids, never a delta-born synthetic id.
    let src_core = resolve_endpoint(gen, &stmt.src, &src_value)?;
    let dst_core = resolve_endpoint(gen, &stmt.dst, &dst_value)?;

    // A MERGE of an edge whose endpoints are both existing core nodes may already exist
    // in the core. If it does, a bare re-MERGE is an idempotent no-op, and a
    // `SET r.p = …` is an **in-place property patch** of that core edge (resolved to its
    // core edge id, which routes the write to `patch_core_edge`). If either endpoint is
    // delta-born there can be no matching core edge, so no check is needed.
    let mut core_edge_id = None;
    if stmt.op == EdgeWriteOp::Create {
        if let (Some(s), Some(d)) = (src_core, dst_core) {
            core_edge_id = find_core_edge_id(gen, s, reltype_id, d)?;
            if core_edge_id.is_some() && patches.is_empty() {
                // Bare re-MERGE of an existing core edge: nothing to write.
                return Ok((Vec::new(), Vec::new()));
            }
        }
    }

    // Resolve the WAL op's endpoints: an endpoint absent from the core but already born
    // and flushed to an L0 level reuses its synthetic id (Phase 4c-B) rather than
    // allocating a duplicate born endpoint; a still-`None` endpoint is a fresh born node
    // (MERGE) or a no-op (DELETE), exactly as before.
    let src = src_core
        .or_else(|| writer.born_synthetic_for_identity(&stmt.src.label, &stmt.src.key, &src_value));
    let dst = dst_core
        .or_else(|| writer.born_synthetic_for_identity(&stmt.dst.label, &stmt.dst.key, &dst_value));

    let op = match stmt.op {
        EdgeWriteOp::Create => WalOp::UpsertEdge {
            src_label: stmt.src.label.clone(),
            src_key: stmt.src.key.clone(),
            src_value,
            reltype: stmt.reltype.clone(),
            dst_label: stmt.dst.label.clone(),
            dst_key: stmt.dst.key.clone(),
            dst_value,
            patches,
        },
        EdgeWriteOp::Delete => WalOp::DeleteEdge {
            src_label: stmt.src.label.clone(),
            src_key: stmt.src.key.clone(),
            src_value,
            reltype: stmt.reltype.clone(),
            dst_label: stmt.dst.label.clone(),
            dst_key: stmt.dst.key.clone(),
            dst_value,
        },
    };
    // `edge_id` is `Some` only for a core-edge patch (a Create whose edge exists in the
    // core); a born-edge create and every delete leave it `None`.
    writer
        .write(
            op,
            OpResolution::Edge {
                src,
                dst,
                edge_id: core_edge_id,
            },
        )
        .map_err(|e| Failure::new(CODE_EXECUTION, format!("durable write failed: {e:#}")))?;
    Ok((Vec::new(), Vec::new()))
}

/// Post-write delta maintenance — the write path's self-tuning (Phase 4d-ii). Three
/// tiers, cheapest first:
///
/// 1. **Flush** the active memtable to an L0 segment when it exceeds `memtableBytes`.
/// 2. **Compact** the L0 stack when it exceeds `l0CompactionTrigger` levels (4d-i).
/// 3. **Consolidate** — fire a *background* full rebuild when the delta reaches
///    `deltaCorePercent`% of the core's entity count (4d-ii-b): rare because it is
///    O(core), triggered as a fraction of core so write amplification stays bounded.
///
/// Flush + compaction are cheap (O(delta), fsync only) and run on a blocking thread;
/// neither can fail the write (it already acked durably), so an error is logged and
/// swallowed. Both are skipped while a consolidation owns the L0 stack. The
/// consolidation is spawned detached — it must not block the ack. Finally, if the delta
/// has blown past the `deltaHardBytes` **hard cap**, the write **throttles**: it ensures
/// a drain is running and waits for headroom before returning (the OOM backstop).
async fn maybe_maintain_delta(ctx: &Arc<ConnCtx>, graph: &str, writer: &Arc<DeltaWriter>) {
    if !writer.is_consolidating() {
        if ctx.memtable_bytes > 0 && writer.bytes() >= ctx.memtable_bytes {
            let w = writer.clone();
            match tokio::task::spawn_blocking(move || w.flush_to_l0()).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    warn!(graph = %graph, error = %format!("{e:#}"), "delta flush_to_l0 failed")
                }
                Err(e) => warn!(graph = %graph, error = %e, "delta flush task panicked"),
            }
        }
        if ctx.l0_compaction_trigger > 0 && writer.l0_len() >= ctx.l0_compaction_trigger {
            let w = writer.clone();
            match tokio::task::spawn_blocking(move || w.compact_l0()).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    warn!(graph = %graph, error = %format!("{e:#}"), "delta compact_l0 failed")
                }
                Err(e) => warn!(graph = %graph, error = %e, "delta compaction task panicked"),
            }
        }

        // ── Segment-tier maintenance (Phase 6 closing slice): the two upper rungs of
        // the D50 ladder, beside the L0-internal rungs above. Safe to auto-fire now the
        // 6.1 segment-aware write resolve is in — a concurrent re-MERGE of a just-flushed
        // key resolves through the new segment instead of duplicating. Both take the
        // `begin_consolidation` claim inside `Graphs` (so they never overlap each other or
        // a consolidation) and run on a blocking pool; a lost single-flight race bails as
        // "already in progress" (logged at debug, not warn). The L0 rungs above still run
        // regardless, so the memtable always drains even if a flush bails — the T2 flush's
        // extra L0 write before it folds the whole delta is the cheap price of that.

        // T2: once the WHOLE delta (memtable + every L0 level) reaches `segmentFlushBytes`,
        // fold it into one durable core segment — the O(delta) drain that keeps the delta
        // small without an O(core) consolidation. Off by default (0). Fires for a resident or
        // an off-heap L0 stack alike (the off-heap fold is `flush_segment_data`, Phase 7.5).
        if ctx.segment_flush_bytes > 0 && writer.total_bytes() >= ctx.segment_flush_bytes {
            let (g, c) = (graph.to_string(), ctx.clone());
            match tokio::task::spawn_blocking(move || {
                c.graphs
                    .flush_graph_to_segment(&g, &c.vector_cache, &c.data_dir)
            })
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) if is_already_in_progress(&e) => {
                    debug!(graph = %graph, "segment flush skipped: a flush/consolidation is already running")
                }
                Ok(Err(e)) => {
                    warn!(graph = %graph, error = %format!("{e:#}"), "delta flush_to_segment failed")
                }
                Err(e) => warn!(graph = %graph, error = %e, "delta flush_to_segment task panicked"),
            }
        }

        // T3: once the served set carries more than `maxUpperSegments` upper segments,
        // fold a contiguous run (the size-tiered selector picks it — self-gating, so the
        // auto entry point is a true no-op when within budget). Pre-checked on the resident
        // segment count (the selector's own admission predicate) so no blocking task is
        // spawned per write. Runs after the T2 flush so a freshly appended segment that
        // tips the stack over budget folds in the same pass.
        let over_segment_budget = ctx.max_upper_segments > 0
            && ctx
                .graphs
                .get(graph)
                .map(|gen| gen.stack().segments().len() > ctx.max_upper_segments)
                .unwrap_or(false);
        let mut compacted = false;
        if over_segment_budget {
            let (g, c) = (graph.to_string(), ctx.clone());
            let max_upper = ctx.max_upper_segments;
            match tokio::task::spawn_blocking(move || {
                c.graphs
                    .compact_graph_segments_auto(&g, &c.vector_cache, &c.data_dir, max_upper)
            })
            .await
            {
                // `Some` means a run actually folded — its old segment dirs + the superseded
                // set are now orphaned, so a GC sweep below has something to reclaim.
                Ok(Ok(folded)) => compacted = folded.is_some(),
                Ok(Err(e)) if is_already_in_progress(&e) => {
                    debug!(graph = %graph, "segment compaction skipped: a flush/consolidation is already running")
                }
                Ok(Err(e)) => {
                    warn!(graph = %graph, error = %format!("{e:#}"), "segment compaction failed")
                }
                Err(e) => warn!(graph = %graph, error = %e, "segment compaction task panicked"),
            }
        }

        // T4 GC (Phase 7 slice 7.2): after a compaction folds a run, reclaim its now-orphaned
        // segment dirs + the superseded set — only when a fold happened (so it is not paid per
        // write) and GC is enabled (`segmentGcGraceSecs > 0`). The sweep takes its own
        // `begin_consolidation` claim (the compaction already released it); a lost race is
        // benign (another op holds it — it will re-observe the orphans on a later write).
        if compacted && ctx.segment_gc_grace_secs > 0 {
            let (g, c) = (graph.to_string(), ctx.clone());
            let grace = ctx.segment_gc_grace_secs;
            match tokio::task::spawn_blocking(move || {
                c.graphs.gc_orphan_segments(&g, &c.data_dir, grace)
            })
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) if is_already_in_progress(&e) => {
                    debug!(graph = %graph, "segment GC skipped: a flush/consolidation is already running")
                }
                Ok(Err(e)) => {
                    warn!(graph = %graph, error = %format!("{e:#}"), "segment GC after compaction failed")
                }
                Err(e) => warn!(graph = %graph, error = %e, "segment GC task panicked"),
            }
        }
    }

    // Background consolidation at a fraction of the core's size (4d-ii-b). Spawned
    // detached so the ack never waits on the O(core) rebuild; 4a keeps writes that
    // arrive during it safe. `begin_consolidation` inside `consolidate_graph` is the
    // real single-flight guard — the pre-check only avoids a spurious spawn.
    if ctx.delta_core_percent > 0 && !writer.is_consolidating() {
        if let Some(gen) = ctx.graphs.get(graph) {
            let core_entities = gen.node_count() + gen.edge_count();
            if consolidation_due(
                core_entities,
                writer.delta_entity_count() as u64,
                ctx.delta_core_percent,
            ) {
                // Defer to the off-peak window if one is configured (the hard-cap
                // throttle below still fires anytime as the OOM backstop).
                if window_permits(&ctx.consolidate_window, crate::cron_window::local_now_hms()) {
                    spawn_auto_consolidation(ctx.clone(), graph.to_string());
                } else {
                    debug!(
                        graph = %graph,
                        "auto-consolidation is due but deferred — outside the configured off-peak window"
                    );
                }
            }
        }
    }

    // Hard-cap throttle (runs even during a consolidation — waiting for it is the
    // point). The OOM backstop: block this write until the delta drains below the cap.
    if ctx.delta_hard_bytes > 0 && writer.total_bytes() >= ctx.delta_hard_bytes {
        throttle_until_drained(ctx, graph, writer).await;
    }
}

/// Whether the delta has grown to `percent`% of the core's entity count — the
/// fraction-of-core auto-consolidation predicate (Phase 4d-ii-b). `false` when
/// disabled (`percent == 0`), the core is empty, or the rounded threshold is 0 (a
/// core too small for this percent to mean one whole entity). `u128` maths avoids
/// overflow on a large core.
fn consolidation_due(core_entities: u64, delta_entities: u64, percent: usize) -> bool {
    if percent == 0 || core_entities == 0 {
        return false;
    }
    let threshold = (core_entities as u128 * percent as u128 / 100) as u64;
    threshold > 0 && delta_entities >= threshold
}

/// Whether the off-peak window (if any) permits a fraction-triggered consolidation at
/// the given server-local time `(hour, day-of-month, month, day-of-week)`. `None` window
/// ⇒ always permitted. Pure over the supplied time so it is testable without a clock —
/// the caller reads the real clock via [`crate::cron_window::local_now_hms`]. The
/// hard-cap throttle never consults this (it is the OOM backstop, fires anytime).
fn window_permits(
    window: &Option<crate::cron_window::CronWindow>,
    (hour, dom, month, dow): (u32, u32, u32, u32),
) -> bool {
    match window {
        None => true,
        Some(w) => w.contains(hour, dom, month, dow),
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

/// Fire a background consolidation for `graph`, detached from the write that triggered
/// it. Reuses the `execute_consolidate` path (dump → builder → swap → retire on a
/// blocking thread). A lost single-flight race (another consolidation already claimed
/// the writer) surfaces as a benign "already in progress" and is logged at debug, not
/// warn.
fn spawn_auto_consolidation(ctx: Arc<ConnCtx>, graph: String) {
    tokio::spawn(async move {
        match execute_consolidate(&ctx, &graph).await {
            Ok(_) => info!(graph = %graph, "auto-consolidation folded the delta into a fresh core"),
            Err(e) if e.code == CODE_CONSOLIDATION_IN_PROGRESS => {
                debug!(graph = %graph, "auto-consolidation skipped: one is already running")
            }
            Err(e) => warn!(graph = %graph, error = %e.message, "auto-consolidation failed"),
        }
    });
}

/// Block the calling write until the delta drains below the `deltaHardBytes` hard cap
/// (Phase 4d-ii-b). Ensures a consolidation is draining (kicking one if none is), then
/// awaits headroom. The await yields the reactor thread, so other connections proceed;
/// a client whose write blocks too long times out — the correct "server saturated"
/// signal. Re-kicks if a drain finishes/fails without clearing the cap, and bails after
/// a generous bound so a wedged consolidation cannot hang a writer forever (logged
/// loudly — for a very large core whose rebuild exceeds the window, the hard cap is
/// advisory; the fraction-of-core trigger is what keeps the delta from getting there).
async fn throttle_until_drained(ctx: &Arc<ConnCtx>, graph: &str, writer: &Arc<DeltaWriter>) {
    use std::time::Duration;
    const STEP_MS: u64 = 50;
    const MAX_WAIT_MS: u64 = 10 * 60 * 1000;
    warn!(
        graph = %graph,
        delta_bytes = writer.total_bytes(),
        hard_cap = ctx.delta_hard_bytes,
        "delta hard cap reached — throttling the writer until a consolidation drains it"
    );
    let mut waited_ms = 0u64;
    while writer.total_bytes() >= ctx.delta_hard_bytes {
        if !writer.is_consolidating() {
            spawn_auto_consolidation(ctx.clone(), graph.to_string());
        }
        if waited_ms >= MAX_WAIT_MS {
            warn!(graph = %graph, "delta hard-cap throttle timed out; proceeding over cap");
            return;
        }
        tokio::time::sleep(Duration::from_millis(STEP_MS)).await;
        waited_ms += STEP_MS;
    }
}

/// Execute `CALL slater.consolidate()` (Phase 5): fold the graph's writable delta
/// into a fresh generation and swap it in, returning the new generation's id as a
/// single `generation` column. The heavy work — dumping the merged view, spawning the
/// builder subprocess, validating and swapping the new generation — runs on a blocking
/// thread so the Bolt reactor is never parked on it. Only reached when the writable
/// layer is enabled for this graph (the caller already resolved a `writer`).
async fn execute_consolidate(
    ctx: &Arc<ConnCtx>,
    graph: &str,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    let graphs = ctx.graphs.clone();
    let cache = ctx.cache.clone();
    let vector_cache = ctx.vector_cache.clone();
    let data_dir = ctx.data_dir.clone();
    let builder_bin = ctx.builder_bin.clone();
    let graph = graph.to_string();
    let gc_graph = graph.clone(); // retained for the post-consolidation GC sweep below
    let new_uuid = tokio::task::spawn_blocking(move || {
        graphs.consolidate_graph(&graph, &cache, &vector_cache, &data_dir, |dump, g, dd| {
            run_builder(&builder_bin, dump, g, dd)
        })
    })
    .await
    .map_err(|e| Failure::new(CODE_EXECUTION, format!("consolidation task failed: {e}")))?
    .map_err(|e| {
        // Classify a lost single-flight race here, where the typed `ConsolidationInProgress`
        // cause is still intact — `{e:#}` below flattens it to a string. Callers then branch
        // on the resulting `code`, never on the message text.
        let code = if is_already_in_progress(&e) {
            CODE_CONSOLIDATION_IN_PROGRESS
        } else {
            CODE_EXECUTION
        };
        Failure::new(code, format!("consolidation failed: {e:#}"))
    })?;

    // T4 GC (Phase 7 slice 7.2): a retarget collapses the served set to a singleton, orphaning
    // the whole prior set + every one of its segments. Reclaim them when GC is enabled — a
    // best-effort sweep whose failure never fails the (already-published) consolidation.
    if ctx.segment_gc_grace_secs > 0 {
        let (g, graphs, data_dir) = (gc_graph.clone(), ctx.graphs.clone(), ctx.data_dir.clone());
        let grace = ctx.segment_gc_grace_secs;
        match tokio::task::spawn_blocking(move || graphs.gc_orphan_segments(&g, &data_dir, grace))
            .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) if is_already_in_progress(&e) => {
                debug!(graph = %gc_graph, "segment GC skipped: a flush/consolidation is already running")
            }
            Ok(Err(e)) => {
                warn!(graph = %gc_graph, error = %format!("{e:#}"), "segment GC after consolidation failed")
            }
            Err(e) => warn!(graph = %gc_graph, error = %e, "segment GC task panicked"),
        }
    }
    Ok((
        vec!["generation".to_string()],
        vec![vec![PsValue::String(new_uuid.to_string())]],
    ))
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
struct ReadOverlay {
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

async fn run_query(
    ctx: &Arc<ConnCtx>,
    gen: Arc<Generation>,
    query: &str,
    ast: parser::ast::Query,
    params: HashMap<String, Val>,
    version: (u8, u8),
    overlay: ReadOverlay,
) -> std::result::Result<(Vec<String>, Vec<Vec<PsValue>>), Failure> {
    let ReadOverlay {
        delta,
        epoch: delta_epoch,
        journal: rw_journal,
    } = overlay;
    let cache = ctx.cache.clone();
    let vector_cache = ctx.vector_cache.clone();
    let rw_indexes = ctx.rw_indexes.clone();
    let rw_cfg = ctx.rw_index_cfg;
    let result_cache = ctx.result_cache.clone();
    let key =
        ResultKey::with_delta_epoch(gen.uuid(), delta_epoch, result_query_key(query, &params));
    // Queries calling `rand()`/`randomUUID()`/`timestamp()` must re-run every
    // time, so they bypass the result cache (both lookup and store).
    let cacheable = !parser::is_nondeterministic(&ast);
    let max_rows = ctx.max_rows;
    let timeout_ms = ctx.timeout_ms;
    let max_intermediate = ctx.max_intermediate;
    let max_scan = ctx.max_scan;
    let intermediate_budget = ctx.intermediate_budget.clone();
    let max_shortest_path_explore = ctx.max_shortest_path_explore;
    let adj_stream_threshold = ctx.adj_stream_threshold;
    let adj_stream_chunk = ctx.adj_stream_chunk;
    let fanout_pool = ctx.fanout_pool.clone();
    let beam_width = ctx.beam_width;
    let temp_beam_width = ctx.temp_beam_width;
    let graph_name = gen.graph().to_string();
    // Gate all per-query instrumentation on the info level being active OR
    // load-test diagnostics being enabled: when both are off, we take no
    // timestamps and no cache snapshots, and build no QueryTiming — the hot path
    // is exactly what it was before instrumentation. The default log level is
    // `info`, so every query emits its `query executed` summary out of the box
    // (without the chatty `debug` SDK/wire tracing); raising the level to `warn`
    // restores the zero-overhead hot path. Diagnostics needs the same `total_ms`
    // for its latency histogram, so it shares this gate.
    let instrument = tracing::enabled!(Level::INFO) || ctx.diag.enabled;

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
            // Overlay the writable-layer delta on the pinned core for this query's
            // whole life (`MergedView`). The empty-delta fast path makes the
            // read-only case behaviourally identical to reading the bare core.
            let view = MergedView::new(gen.as_ref(), delta);
            let (result, result_cache_hit, cost) = match cached {
                Some(r) => (r, true, None),
                None => {
                    let mut engine = Engine::new(&view, cache.as_ref())
                        .with_vector_cache(vector_cache.as_ref(), beam_width)
                        .with_temp_beam_width(temp_beam_width)
                        .with_params(params)
                        .with_max_rows(max_rows)
                        .with_max_intermediate(max_intermediate)
                        .with_max_scan(max_scan)
                        .with_global_budget(intermediate_budget.as_ref())
                        .with_max_shortest_path_explore(max_shortest_path_explore)
                        .with_adj_stream(adj_stream_threshold, adj_stream_chunk)
                        .with_fanout_pool(fanout_pool.clone());
                    // The RW-index arm of the delta's KNN. The epoch is the one taken with the
                    // snapshot above, in the same atomic read — the index is cut at exactly it.
                    if let Some(journal) = rw_journal {
                        engine =
                            engine.with_rw_index(rw_indexes.as_ref(), journal, delta_epoch, rw_cfg);
                    }
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
            // needed) resolves Node/Relationship records through the shared block
            // cache — over the same merged view, so a returned node carries its
            // overlaid (patched) properties.
            let engine = Engine::new(&view, cache.as_ref());
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
            // Only ever `Some` when the info level was active (see `instrument`).
            // A block-cache miss is a cold block read (pread + decompress); many
            // misses on a small query is the signature of an unindexed scan. A high
            // total_ms with result_cache=miss and many blk_misses points at exactly
            // that.
            // Feed the diagnostics latency histogram (no-op when disabled). When
            // diagnostics are on, `instrument` is true so `timing` is always Some.
            let total_ms = timing.as_ref().map(|t| t.total_ms);
            if let Some(t) = timing {
                info!(
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
            // A failed query emits no `query executed` summary (that only fires on
            // success), so without this line a budget trip / timeout is invisible in
            // the logs. Log at warn with the graph, reason and (truncated) query so
            // the next such failure is diagnosable.
            warn!(
                graph = %graph_name,
                error = %format!("{e:#}"),
                query = %log_query(query),
                "query failed"
            );
            ctx.diag.on_query_err(&e);
            Err(Failure::from_query_error(&e))
        }
        Err(e) => {
            warn!(
                graph = %graph_name,
                error = %e,
                query = %log_query(query),
                "query task failed"
            );
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
fn encode_val<V: ReadView>(engine: &Engine<'_, V>, version: (u8, u8), v: &Val) -> Result<PsValue> {
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
fn encode_unbound_rel<V: ReadView>(
    engine: &Engine<'_, V>,
    version: (u8, u8),
    r: &Val,
) -> Result<PsValue> {
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

fn encode_pairs<V: ReadView>(
    engine: &Engine<'_, V>,
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
mod tests;
